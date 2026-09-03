package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/example/autostream-discord-bot/internal/audioforward"
	"github.com/example/autostream-discord-bot/internal/control"
	discordclient "github.com/example/autostream-discord-bot/internal/discord"
	"github.com/example/autostream-discord-bot/internal/httpapi"
	"github.com/example/autostream-discord-bot/internal/jobs"
	"github.com/example/autostream-discord-bot/internal/version"
	"github.com/example/autostream-discord-bot/internal/worker"
)

func main() {
	if len(os.Args) > 1 && (os.Args[1] == "--version" || os.Args[1] == "version") {
		fmt.Printf("autostream-discord-bot %s\ncommit: %s\nbuild_date: %s\n", version.Current(), version.Commit, version.BuildDate)
		return
	}
	if len(os.Args) > 1 && os.Args[1] == "configure" {
		if err := control.RunConfigureCommand(os.Args[2:], control.ServiceType, os.Stdout); err != nil {
			fmt.Fprintf(os.Stderr, "configure failed: %v\n", err)
			os.Exit(2)
		}
		return
	}

	controlCfg := control.ConfigFromEnv()
	addr, err := discordBotStartupAddr(controlCfg.BindAddress)
	if err != nil {
		log.Fatal(err)
	}
	updaterIdentity := httpapi.NewUpdaterIdentityLatch(control.ServiceType)
	if _, err := updaterIdentity.ResolveFromEnv(); err != nil && !errors.Is(err, httpapi.ErrUpdaterIdentityPending) {
		log.Fatalf("invalid updater identity: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := requireMatchingUpdaterIdentity(updaterIdentity, controlCfg.ServiceID); err != nil && !errors.Is(err, httpapi.ErrUpdaterIdentityPending) {
		log.Fatalf("invalid updater identity: %v", err)
	}
	controlClient := control.Client{Config: controlCfg}
	var runtimeConfigProvider httpapi.RuntimeConfigProvider
	var runtimeCfg control.RuntimeConfig
	hasRuntimeConfig := false
	requireRuntimeConfig := requireControlPanelRuntimeConfig()
	configPending := control.NodeConfigPendingFromEnv()
	startPendingRegistrationLoop := false
	if shouldUseControlPanelRuntimeConfig(controlCfg) {
		runtimeConfigProvider = controlRuntimeConfigFromEnv
	}
	if controlClient.Config.ControlPanelURL != "" && controlClient.Config.Token != "" {
		if err := controlClient.Register(ctx); err != nil {
			if requireRuntimeConfig {
				log.Fatalf("control panel registration is required in this environment: %v", err)
			}
			log.Printf("control panel registration failed: %v", err)
		} else {
			log.Printf("registered with control panel as %s", controlClient.Config.ServiceID)
			if cfg, ok := logRuntimeConfig(ctx, controlClient); ok {
				runtimeCfg = cfg
				hasRuntimeConfig = true
			} else if requireRuntimeConfig {
				log.Fatal("control panel runtime config is required in this environment")
			}
		}
	} else if configPending {
		startPendingRegistrationLoop = true
	} else if requireRuntimeConfig {
		if strings.TrimSpace(controlClient.Config.ConfigError) != "" {
			log.Fatalf("node config invalid: %v", controlClient.Config.ConfigError)
		} else {
			log.Fatal("panel-managed node config is required in this environment")
		}
	}

	discordCfg := discordclient.Config{}
	tokenSource := "Control Panel runtime secret"
	if hasRuntimeConfig {
		secretName := discordBotTokenSecretNameFromRuntimeConfig(runtimeCfg)
		if secretName != "" {
			secret, err := controlClient.ResolveRuntimeSecret(ctx, secretName)
			if errors.Is(err, control.ErrRuntimeSecretLeaseActive) {
				if requireRuntimeConfig {
					log.Fatalf("control panel Discord bot token runtime secret is required but lease is active: %v", err)
				}
				log.Printf("control panel Discord bot token runtime secret lease is active; Discord remains disabled: %v", err)
			} else if err != nil {
				if requireRuntimeConfig {
					log.Fatalf("control panel Discord bot token runtime secret resolve is required in this environment: %v", err)
				}
				log.Printf("control panel Discord bot token resolve failed; Discord remains disabled: %v", err)
			} else if secret.Value != "" {
				discordCfg.BotToken = secret.Value
				log.Printf("loaded Discord bot token from Control Panel runtime secret %s", secret.SecretName)
			} else if requireRuntimeConfig {
				log.Fatal("control panel Discord bot token runtime secret resolved to an empty value")
			}
		} else if requireRuntimeConfig {
			log.Fatal("control panel Discord bot token secret reference is required in this environment")
		}
	}

	voiceClient, err := buildDiscordClient(discordCfg, tokenSource, !requireRuntimeConfig || configPending)
	if err != nil {
		log.Fatal(err)
	}
	if source, ok := voiceClient.(discordclient.AudioForwardSource); ok {
		cfg := audioforward.ConfigFromEnv()
		source.SetAudioForwarder(audioforward.Client{Config: cfg}, controlCfg.ServiceID)
	}
	manager := jobs.NewManagerWithReporter(voiceClient, buildWorkerReporter())
	if controlClient.Config.ControlPanelURL != "" && controlClient.Config.Token != "" {
		streamController := controlStreamStarter{client: controlClient}
		manager.SetStreamStarter(streamController)
		manager.SetStreamStopper(streamController)
	} else {
		log.Printf("Discord VC auto-start unavailable: Control Panel URL/token is not configured")
	}
	if eventSource, ok := voiceClient.(discordclient.EventSource); ok {
		eventSource.SetEventSink(manager)
	}
	reconnectPolicy := reconnectPolicyFromEnv()
	if hasRuntimeConfig {
		reconnectPolicy = applyRuntimeConfigToManager(manager, runtimeCfg, reconnectPolicy)
	}
	manager.SetReconnectPolicy(reconnectPolicy)
	if controlClient.Config.ControlPanelURL != "" && controlClient.Config.Token != "" && runtimeConfigProvider != nil {
		manager.SetAutoStartRefresher(func() error {
			refreshCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			cfg, err := controlClient.RuntimeConfig(refreshCtx)
			if err != nil {
				log.Printf("control panel runtime config refresh for VC auto-start failed: %v", err)
				return err
			}
			applyRuntimeConfigToManager(manager, cfg, reconnectPolicy)
			return nil
		})
	}
	// Auto-start is driven by Discord VoiceStateUpdate events while no stream
	// job is active.  RealClient used to open the Gateway only from JoinVoice,
	// which made VC-join auto-start impossible: the event needed to trigger the
	// start could never arrive before the start itself.  Keep dry-run mode
	// disconnected, but establish the real Gateway session during startup so
	// the bot can observe waiting stream channels.
	if _, dryRun := voiceClient.(*discordclient.NoopClient); !dryRun {
		if err := voiceClient.Connect(); err != nil {
			log.Fatalf("Discord Gateway connection failed: %v", err)
		}
		log.Printf("Discord Gateway connected; waiting for VC auto-start events")
	}
	if controlClient.Config.ControlPanelURL != "" && controlClient.Config.Token != "" && runtimeConfigProvider != nil {
		go runRuntimeConfigRefreshLoop(ctx, controlClient, manager, reconnectPolicyFromEnv(), envDurationDefault("CONTROL_PANEL_RUNTIME_CONFIG_REFRESH_INTERVAL", 30*time.Second))
	}

	if controlClient.Config.ControlPanelURL != "" && controlClient.Config.Token != "" {
		go controlClient.RunHeartbeatLoopWithMetrics(ctx, manager.CurrentStreamID, manager.Metrics, func(err error) {
			log.Printf("control panel heartbeat failed: %v", err)
		})
	} else if startPendingRegistrationLoop {
		go runPendingControlPanelRegistrationLoop(ctx, manager, reconnectPolicyFromEnv(), requireRuntimeConfig, updaterIdentity)
	}

	server := &http.Server{
		Addr:              addr,
		Handler:           httpapi.NewServerWithRuntimeConfigAndUpdaterIdentity(control.ServiceType, manager, httpapi.TokenVerifierFromEnv(), runtimeConfigProvider, updaterIdentity),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}
	errCh := make(chan error, 1)
	go func() {
		log.Printf("autostream-discord-bot listening on %s", addr)
		err := server.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		errCh <- err
	}()
	select {
	case err := <-errCh:
		if err != nil {
			log.Fatal(err)
		}
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Printf("discord bot shutdown failed: %v", err)
			if closeErr := server.Close(); closeErr != nil {
				log.Printf("discord bot close failed: %v", closeErr)
			}
		}
	}
}

func discordBotStartupAddr(configured string) (string, error) {
	addr, err := discordBotBindAddr(configured)
	if err != nil {
		return "", fmt.Errorf("invalid node listener credential bind_address: %w", err)
	}
	return addr, nil
}

func discordBotBindAddr(configured string) (string, error) {
	addr := strings.TrimSpace(configured)
	if addr == "" {
		return "", errors.New("node listener credential bind_address is required")
	}
	_, portText, err := net.SplitHostPort(addr)
	if err != nil {
		return "", fmt.Errorf("must be host:port: %w", err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		return "", fmt.Errorf("port must be an integer: %w", err)
	}
	if port < 1024 || port > 65535 {
		return "", fmt.Errorf("port %d is outside the supported range 1024-65535", port)
	}
	return addr, nil
}

func shouldUseControlPanelRuntimeConfig(cfg control.Config) bool {
	return requireControlPanelRuntimeConfig() ||
		control.NodeConfigPendingFromEnv() ||
		(strings.TrimSpace(cfg.ControlPanelURL) != "" && strings.TrimSpace(cfg.Token) != "")
}

func controlRuntimeConfigFromEnv(ctx context.Context) (control.RuntimeConfig, error) {
	return control.Client{Config: control.ConfigFromEnv()}.RuntimeConfig(ctx)
}

func runPendingControlPanelRegistrationLoop(ctx context.Context, manager *jobs.Manager, fallbackReconnectPolicy jobs.ReconnectPolicy, requireRuntimeConfig bool, updaterIdentity *httpapi.UpdaterIdentityLatch) {
	lastState := ""
	registeredServiceID := ""
	for {
		cfg := control.ConfigFromEnv()
		client := control.Client{Config: cfg}
		wait := controlPanelRegistrationInterval(cfg)
		state := ""
		if err := requireMatchingUpdaterIdentity(updaterIdentity, cfg.ServiceID); err != nil {
			if errors.Is(err, httpapi.ErrUpdaterIdentityPending) {
				state = "pending:" + control.NodeConfigPathFromEnv()
				logRegistrationStateChange(&lastState, state, "node config pending: waiting for %s", control.NodeConfigPathFromEnv())
				registeredServiceID = ""
			} else {
				log.Fatalf("updater identity invalid: %v", err)
			}
			timer := time.NewTimer(wait)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
			continue
		}
		switch {
		case strings.TrimSpace(cfg.ConfigError) != "":
			state = "invalid:" + cfg.ConfigError
			if requireRuntimeConfig {
				log.Fatalf("node config invalid: %v", cfg.ConfigError)
			}
			logRegistrationStateChange(&lastState, state, "node config invalid: %v", cfg.ConfigError)
			registeredServiceID = ""
		case strings.TrimSpace(cfg.ControlPanelURL) == "" || strings.TrimSpace(cfg.Token) == "":
			state = "pending:" + control.NodeConfigPathFromEnv()
			logRegistrationStateChange(&lastState, state, "node config pending: waiting for %s", control.NodeConfigPathFromEnv())
			registeredServiceID = ""
		default:
			if registeredServiceID != cfg.ServiceID {
				if err := client.Register(ctx); err != nil {
					state = "register-failed:" + err.Error()
					if requireRuntimeConfig {
						log.Fatalf("control panel registration is required in this environment: %v", err)
					}
					logRegistrationStateChange(&lastState, state, "control panel registration failed: %v", err)
					registeredServiceID = ""
					break
				}
				registeredServiceID = cfg.ServiceID
				state = "registered:" + cfg.ServiceID
				logRegistrationStateChange(&lastState, state, "registered with control panel as %s", cfg.ServiceID)
				streamController := controlStreamStarter{client: client}
				manager.SetStreamStarter(streamController)
				manager.SetStreamStopper(streamController)
				if runtimeCfg, ok := logRuntimeConfig(ctx, client); ok {
					applyRuntimeConfigToManager(manager, runtimeCfg, fallbackReconnectPolicy)
				} else if requireRuntimeConfig {
					log.Fatal("control panel runtime config is required in this environment")
				}
			}
			if registeredServiceID == cfg.ServiceID {
				if err := client.HeartbeatWithMetrics(ctx, "online", manager.CurrentStreamID(), manager.Metrics()); err != nil {
					state = "heartbeat-failed:" + err.Error()
					logRegistrationStateChange(&lastState, state, "control panel heartbeat failed: %v", err)
				} else {
					lastState = "online:" + cfg.ServiceID
				}
			}
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func requireMatchingUpdaterIdentity(latch *httpapi.UpdaterIdentityLatch, serviceID string) error {
	identity, err := latch.ResolveFromEnv()
	if err != nil {
		return err
	}
	if strings.TrimSpace(serviceID) != identity.ServiceID {
		return fmt.Errorf("%w: control client service id does not match the updater identity", httpapi.ErrUpdaterIdentityDrift)
	}
	return nil
}

func controlPanelRegistrationInterval(cfg control.Config) time.Duration {
	if strings.TrimSpace(cfg.ControlPanelURL) != "" && strings.TrimSpace(cfg.Token) != "" && cfg.HeartbeatEvery > 0 {
		return cfg.HeartbeatEvery
	}
	return 10 * time.Second
}

func logRegistrationStateChange(lastState *string, state, format string, args ...any) {
	if state == *lastState {
		return
	}
	log.Printf(format, args...)
	*lastState = state
}

type controlStreamStarter struct {
	client control.Client
}

func (s controlStreamStarter) StartStream(streamID string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	err := s.client.StartStream(ctx, streamID)
	if err != nil {
		log.Printf("control panel auto-start failed for stream %s: %v", streamID, err)
	} else {
		log.Printf("control panel auto-start requested for stream %s", streamID)
	}
	return err
}

func (s controlStreamStarter) StopStream(streamID string) error {
	return s.StopStreamContext(context.Background(), streamID)
}

func (s controlStreamStarter) StopStreamContext(ctx context.Context, streamID string) error {
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	err := s.client.StopStream(ctx, streamID)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return err
		}
		log.Printf("control panel auto-stop failed for stream %s: %v", streamID, err)
	} else {
		log.Printf("control panel auto-stop requested for stream %s", streamID)
	}
	return err
}

func logRuntimeConfig(ctx context.Context, client control.Client) (control.RuntimeConfig, bool) {
	cfg, err := client.RuntimeConfig(ctx)
	if err != nil {
		log.Printf("control panel runtime config fetch failed: %v", err)
		return control.RuntimeConfig{}, false
	}
	profileCount := 0
	for _, profiles := range cfg.Profiles {
		profileCount += len(profiles)
	}
	log.Printf("loaded control panel runtime config for %s: assignments=%d profiles=%d stream_discord_configs=%d", cfg.Service.ServiceID, len(cfg.Assignments), profileCount, len(cfg.StreamDiscordConfigs))
	return cfg, true
}

func runRuntimeConfigRefreshLoop(ctx context.Context, client control.Client, manager *jobs.Manager, baseReconnectPolicy jobs.ReconnectPolicy, interval time.Duration) {
	if manager == nil {
		return
	}
	if interval <= 0 {
		interval = 30 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		cfg, err := client.RuntimeConfig(ctx)
		if err != nil {
			log.Printf("control panel runtime config refresh failed: %v", err)
			continue
		}
		applyRuntimeConfigToManager(manager, cfg, baseReconnectPolicy)
		log.Printf("refreshed control panel runtime config for %s: stream_discord_configs=%d", cfg.Service.ServiceID, len(cfg.StreamDiscordConfigs))
	}
}

func applyRuntimeConfigToManager(manager *jobs.Manager, cfg control.RuntimeConfig, baseReconnectPolicy jobs.ReconnectPolicy) jobs.ReconnectPolicy {
	manager.SetStreamVoiceDefaults(streamDiscordDefaultsFromRuntimeConfig(cfg))
	reconnectPolicy := reconnectPolicyFromRuntimeConfig(cfg, baseReconnectPolicy)
	manager.SetReconnectPolicy(reconnectPolicy)
	return reconnectPolicy
}

func reconnectPolicyFromEnv() jobs.ReconnectPolicy {
	return jobs.ReconnectPolicy{
		Enabled:     envBoolDefault("DISCORD_RECONNECT_ENABLED", true),
		MaxAttempts: envIntDefault("DISCORD_RECONNECT_MAX_ATTEMPTS", 5),
		BaseDelay:   envDurationDefault("DISCORD_RECONNECT_BASE_DELAY", 2*time.Second),
		MaxDelay:    envDurationDefault("DISCORD_RECONNECT_MAX_DELAY", 30*time.Second),
	}
}

func reconnectPolicyFromRuntimeConfig(cfg control.RuntimeConfig, fallback jobs.ReconnectPolicy) jobs.ReconnectPolicy {
	if profile, ok := firstRuntimeProfileForService(cfg.Profiles["discord_config"], cfg.Service.ServiceID); ok {
		if value, ok := boolConfig(profile.Config, "reconnect_enabled"); ok {
			fallback.Enabled = value
		}
		if value, ok := intConfig(profile.Config, "reconnect_max_attempts"); ok {
			fallback.MaxAttempts = value
		}
		if value, ok := durationConfig(profile.Config, "reconnect_base_delay"); ok {
			fallback.BaseDelay = value
		}
		if value, ok := durationConfig(profile.Config, "reconnect_max_delay"); ok {
			fallback.MaxDelay = value
		}
	}
	return fallback
}

func streamDiscordDefaultsFromRuntimeConfig(cfg control.RuntimeConfig) map[string]jobs.VoiceDefaults {
	defaults := map[string]jobs.VoiceDefaults{}
	assignedPrimaryStreams, hasDiscordAssignment := primaryDiscordAssignmentStreams(cfg)
	for _, item := range cfg.StreamDiscordConfigs {
		if item.StreamID == "" {
			continue
		}
		if item.AssignmentRole != "primary" {
			continue
		}
		// Older Panel payloads can include historical streams with an implicit
		// primary role. Once this Bot has an explicit assignment, accept only
		// that authoritative primary stream so a completed source cannot mask
		// its rearmed successor.
		if hasDiscordAssignment && !assignedPrimaryStreams[item.StreamID] {
			continue
		}
		defaults[item.StreamID] = jobs.VoiceDefaults{
			GuildID:          item.GuildID,
			VoiceChannelID:   item.VoiceChannelID,
			TextChannelID:    item.TextChannelID,
			AutoStartEnabled: strings.TrimSpace(item.AutoStartTrigger) == "discord_voice_join",
		}
	}
	return defaults
}

func primaryDiscordAssignmentStreams(cfg control.RuntimeConfig) (map[string]bool, bool) {
	streams := map[string]bool{}
	serviceID := strings.TrimSpace(cfg.Service.ServiceID)
	if serviceID == "" {
		return streams, false
	}
	hasDiscordAssignment := false
	for _, assignment := range cfg.Assignments {
		if strings.TrimSpace(assignment.ServiceID) != serviceID {
			continue
		}
		if serviceType := strings.TrimSpace(assignment.ServiceType); serviceType != "" && serviceType != control.ServiceType {
			continue
		}
		hasDiscordAssignment = true
		if role := strings.TrimSpace(assignment.AssignmentRole); role == "" || role == "primary" {
			streams[strings.TrimSpace(assignment.StreamID)] = true
		}
	}
	return streams, hasDiscordAssignment
}

func discordBotTokenSecretNameFromRuntimeConfig(cfg control.RuntimeConfig) string {
	if profile, ok := firstRuntimeProfileForService(cfg.Profiles["discord_config"], cfg.Service.ServiceID); ok {
		if value := stringConfig(profile.Config, "bot_token_secret_name"); value != "" {
			return value
		}
	}
	return ""
}

func firstRuntimeProfileForService(profiles []control.RuntimeProfile, serviceID string) (control.RuntimeProfile, bool) {
	for _, profile := range profiles {
		if profileBelongsToService(profile, serviceID) {
			return profile, true
		}
	}
	return control.RuntimeProfile{}, false
}

func profileBelongsToService(profile control.RuntimeProfile, serviceID string) bool {
	rawServiceID, ok := profile.Config["service_id"]
	if !ok {
		return true
	}
	profileServiceID, ok := rawServiceID.(string)
	if !ok {
		return false
	}
	profileServiceID = strings.TrimSpace(profileServiceID)
	return profileServiceID == "" || profileServiceID == strings.TrimSpace(serviceID)
}

func stringConfig(config map[string]any, key string) string {
	if value, ok := config[key].(string); ok {
		return value
	}
	return ""
}

func boolConfig(config map[string]any, key string) (bool, bool) {
	value, ok := config[key]
	if !ok {
		return false, false
	}
	switch typed := value.(type) {
	case bool:
		return typed, true
	case string:
		switch strings.ToLower(strings.TrimSpace(typed)) {
		case "1", "true", "yes", "on":
			return true, true
		case "0", "false", "no", "off":
			return false, true
		}
	}
	return false, false
}

func intConfig(config map[string]any, key string) (int, bool) {
	value, ok := config[key]
	if !ok {
		return 0, false
	}
	switch typed := value.(type) {
	case int:
		return typed, true
	case float64:
		return int(typed), true
	case string:
		parsed, err := strconv.Atoi(strings.TrimSpace(typed))
		return parsed, err == nil
	}
	return 0, false
}

func durationConfig(config map[string]any, key string) (time.Duration, bool) {
	if value := strings.TrimSpace(stringConfig(config, key)); value != "" {
		parsed, err := time.ParseDuration(strings.TrimSpace(value))
		return parsed, err == nil
	}
	if seconds, ok := intConfig(config, key+"_sec"); ok {
		return time.Duration(seconds) * time.Second, true
	}
	return 0, false
}

func envBoolDefault(name string, fallback bool) bool {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback
	}
	switch strings.ToLower(raw) {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		log.Printf("invalid %s=%q; using %v", name, raw, fallback)
		return fallback
	}
}

func envIntDefault(name string, fallback int) int {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		log.Printf("invalid %s=%q; using %d", name, raw, fallback)
		return fallback
	}
	return value
}

func envDurationDefault(name string, fallback time.Duration) time.Duration {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback
	}
	value, err := time.ParseDuration(raw)
	if err != nil || value < 0 {
		log.Printf("invalid %s=%q; using %s", name, raw, fallback)
		return fallback
	}
	return value
}

func requireControlPanelRuntimeConfig() bool {
	if envBoolDefault("AUTOSTREAM_REQUIRE_CONTROL_PANEL_RUNTIME_CONFIG", false) {
		return true
	}
	return strings.EqualFold(strings.TrimSpace(os.Getenv("AUTOSTREAM_ENV")), "production")
}

func buildWorkerReporter() jobs.EventReporter {
	return worker.Reporter{Config: worker.ConfigFromEnv()}
}

func buildDiscordClient(cfg discordclient.Config, tokenSource string, allowDryRun bool) (discordclient.Client, error) {
	if cfg.BotToken == "" {
		if !allowDryRun {
			return nil, errors.New("Discord bot token is required in this environment")
		}
		log.Printf("Discord bot token is not configured by Control Panel; running in dry-run Discord mode")
		return &discordclient.NoopClient{}, nil
	}
	client, err := discordclient.NewRealClient(cfg)
	if err != nil {
		if !allowDryRun {
			return nil, err
		}
		log.Printf("Discord client initialization failed; running in dry-run mode: %v", err)
		return &discordclient.NoopClient{}, nil
	}
	log.Printf("Discord client initialized using %s token", tokenSource)
	return client, nil
}
