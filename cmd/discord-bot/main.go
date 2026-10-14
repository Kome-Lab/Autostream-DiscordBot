package main

import (
	"context"
	"crypto/sha256"
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

	// Managed nodes begin disconnected. A selected primary assignment is the
	// authority for loading credentials and opening the Gateway.
	voiceClient := discordclient.Client(discordclient.NewDormantRealClient())
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
			return manager.RefreshRuntimeConfig(func() error {
				refreshCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				cfg, err := controlClient.RuntimeConfig(refreshCtx)
				if err != nil {
					log.Printf("control panel runtime config refresh for VC auto-start failed: %v", err)
					return err
				}
				if err := reconcileRuntimeGateway(refreshCtx, controlClient, manager, cfg); err != nil {
					return err
				}
				applyRuntimeConfigToManager(manager, cfg, reconnectPolicy)
				return nil
			})
		})
	}
	if hasRuntimeConfig {
		if err := reconcileRuntimeGateway(ctx, controlClient, manager, runtimeCfg); err != nil {
			log.Printf("selected Discord Gateway unavailable: %v", err)
		}
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
	registeredAuthority := ""
	var cancelRefresh context.CancelFunc
	stopRefresh := func() {
		if cancelRefresh != nil {
			cancelRefresh()
			cancelRefresh = nil
		}
		registeredAuthority = ""
		if manager != nil {
			_ = manager.RefreshRuntimeConfig(func() error {
				manager.SetStreamVoiceDefaults(nil)
				manager.SetAutoStartRefresher(nil)
				manager.SetStreamStarter(nil)
				manager.SetStreamStopper(nil)
				return manager.ReconcileGatewayCredential("", false, nil)
			})
		}
	}
	defer stopRefresh()
	for {
		cfg := control.ConfigFromEnv()
		authority := fmt.Sprintf("%x", sha256.Sum256([]byte(cfg.ServiceID+"\x00"+cfg.ControlPanelURL+"\x00"+cfg.Token)))
		if registeredAuthority != "" && registeredAuthority != authority {
			stopRefresh()
			registeredServiceID = ""
		}
		client := control.Client{Config: cfg}
		wait := controlPanelRegistrationInterval(cfg)
		state := ""
		if err := requireMatchingUpdaterIdentity(updaterIdentity, cfg.ServiceID); err != nil {
			if errors.Is(err, httpapi.ErrUpdaterIdentityPending) {
				state = "pending:" + control.NodeConfigPathFromEnv()
				logRegistrationStateChange(&lastState, state, "node config pending: waiting for %s", control.NodeConfigPathFromEnv())
				registeredServiceID = ""
				stopRefresh()
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
			stopRefresh()
		case strings.TrimSpace(cfg.ControlPanelURL) == "" || strings.TrimSpace(cfg.Token) == "":
			state = "pending:" + control.NodeConfigPathFromEnv()
			logRegistrationStateChange(&lastState, state, "node config pending: waiting for %s", control.NodeConfigPathFromEnv())
			registeredServiceID = ""
			stopRefresh()
		default:
			if registeredServiceID != cfg.ServiceID {
				if err := client.Register(ctx); err != nil {
					state = "register-failed:" + err.Error()
					if requireRuntimeConfig {
						log.Fatalf("control panel registration is required in this environment: %v", err)
					}
					logRegistrationStateChange(&lastState, state, "control panel registration failed: %v", err)
					registeredServiceID = ""
					stopRefresh()
					break
				}
				registeredServiceID = cfg.ServiceID
				state = "registered:" + cfg.ServiceID
				logRegistrationStateChange(&lastState, state, "registered with control panel as %s", cfg.ServiceID)
				stopRefresh()
				streamController := controlStreamStarter{client: client}
				manager.SetStreamStarter(streamController)
				manager.SetStreamStopper(streamController)

				refreshLifetime, refreshCancel := context.WithCancel(ctx)
				cancelRefresh = refreshCancel
				registeredAuthority = authority
				manager.SetAutoStartRefresher(func() error {
					return manager.RefreshRuntimeConfig(func() error {
						refreshCtx, cancel := context.WithTimeout(refreshLifetime, 10*time.Second)
						defer cancel()
						if err := refreshCtx.Err(); err != nil {
							return err
						}
						cfg, err := client.RuntimeConfig(refreshCtx)
						if err != nil {
							return err
						}
						if err := refreshCtx.Err(); err != nil {
							return err
						}
						if err = reconcileRuntimeGateway(refreshCtx, client, manager, cfg); err != nil {
							return err
						}
						if err := refreshCtx.Err(); err != nil {
							return err
						}
						applyRuntimeConfigToManager(manager, cfg, fallbackReconnectPolicy)
						return nil
					})
				})
				if err := manager.RefreshRuntimeConfig(func() error {
					runtimeCfg, ok := logRuntimeConfig(refreshLifetime, client)
					if !ok {
						return errors.New("control panel runtime config unavailable")
					}
					if err := refreshLifetime.Err(); err != nil {
						return err
					}
					if err := reconcileRuntimeGateway(refreshLifetime, client, manager, runtimeCfg); err != nil {
						return err
					}
					if err := refreshLifetime.Err(); err != nil {
						return err
					}
					applyRuntimeConfigToManager(manager, runtimeCfg, fallbackReconnectPolicy)
					return nil
				}); err != nil {
					log.Printf("pending registration Gateway reconciliation unavailable: %v", err)
					if requireRuntimeConfig {
						log.Fatal("control panel runtime config is required in this environment")
					}
				}
				go runRuntimeConfigRefreshLoop(refreshLifetime, client, manager, fallbackReconnectPolicy, envDurationDefault("CONTROL_PANEL_RUNTIME_CONFIG_REFRESH_INTERVAL", 30*time.Second))

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
		_ = manager.RefreshRuntimeConfig(func() error {
			if err := ctx.Err(); err != nil {
				return err
			}
			cfg, err := client.RuntimeConfig(ctx)
			if err != nil {
				log.Printf("control panel runtime config refresh failed: %v", err)
				return err
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := reconcileRuntimeGateway(ctx, client, manager, cfg); err != nil {
				manager.SetStreamVoiceDefaults(nil)
				log.Printf("selected Discord Gateway refresh unavailable: %v", err)
				return err
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			applyRuntimeConfigToManager(manager, cfg, baseReconnectPolicy)
			log.Printf("refreshed control panel runtime config for %s: stream_discord_configs=%d", cfg.Service.ServiceID, len(cfg.StreamDiscordConfigs))
			return nil
		})
	}
}

func reconcileRuntimeGateway(ctx context.Context, client control.Client, manager *jobs.Manager, cfg control.RuntimeConfig) error {
	streams, _ := primaryDiscordAssignmentStreams(cfg)
	current := map[string]string{}
	mapped := map[string]string{}
	for _, assignment := range cfg.Assignments {
		if streams[assignment.StreamID] && assignment.ServiceID == cfg.Service.ServiceID && assignment.ServiceType == control.ServiceType && assignment.AssignmentRole == "primary" {
			current[assignment.StreamID] = assignment.AssignedAt.UTC().Format(time.RFC3339Nano)
		}
	}
	for streamID := range streams {
		if mapping, ok := cfg.DiscordConfigForStream(streamID); ok {
			mapped[streamID] = strings.TrimSpace(mapping.DiscordConfigID)
		}
	}
	profileID, selected := manager.GatewayProfileForAssignments(current, mapped, len(cfg.StreamDiscordConfigs) != 0)
	if len(streams) == 0 {
		return manager.ReconcileGatewayCredential("", false, nil)
	}
	var profile control.RuntimeProfile
	matches := 0
	for _, candidate := range cfg.Profiles["discord_config"] {
		if selected && candidate.ID == profileID {
			profile = candidate
			matches++
		}
	}
	secretName := stringConfig(profile.Config, "bot_token_secret_name")
	fingerprint := stringConfig(profile.Config, "bot_token_fingerprint")
	if !selected || matches != 1 || !profileBelongsToService(profile, cfg.Service.ServiceID) || secretName == "" || fingerprint == "" {
		manager.SetStreamVoiceDefaults(nil)
		_ = manager.ReconcileGateway(discordclient.Config{}, false)
		return errors.New("selected Bot credential reference unavailable")
	}
	return manager.ReconcileGatewayCredential(cfg.Service.ServiceID+"/"+secretName+"/"+fingerprint, true, func() (string, error) {
		secret, err := client.ResolveRuntimeSecret(ctx, secretName)
		return secret.Value, err
	})
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
	assignedPrimaryStreams, _ := primaryDiscordAssignmentStreams(cfg)
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
		if !assignedPrimaryStreams[item.StreamID] {
			continue
		}
		epoch := ""
		for _, assignment := range cfg.Assignments {
			if assignment.StreamID == item.StreamID && assignment.ServiceID == cfg.Service.ServiceID && assignment.ServiceType == control.ServiceType && assignment.AssignmentRole == "primary" && !assignment.AssignedAt.IsZero() {
				epoch = assignment.AssignedAt.UTC().Format(time.RFC3339Nano)
			}
		}
		if epoch == "" {
			continue
		}
		defaults[item.StreamID] = jobs.VoiceDefaults{
			AssignmentEpoch:  epoch,
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
		if strings.TrimSpace(assignment.ServiceType) != control.ServiceType {
			continue
		}
		hasDiscordAssignment = true
		if strings.TrimSpace(assignment.AssignmentRole) == "primary" && strings.TrimSpace(assignment.StreamID) != "" {
			streams[strings.TrimSpace(assignment.StreamID)] = true
		}
	}
	return streams, hasDiscordAssignment
}

func discordBotTokenFingerprintFromRuntimeConfig(cfg control.RuntimeConfig) string {
	if profile, ok := discordGatewayRuntimeProfile(cfg); ok {
		return stringConfig(profile.Config, "bot_token_fingerprint")
	}
	return ""
}

func discordBotTokenSecretNameFromRuntimeConfig(cfg control.RuntimeConfig) string {
	if profile, ok := discordGatewayRuntimeProfile(cfg); ok {
		if value := stringConfig(profile.Config, "bot_token_secret_name"); value != "" {
			return value
		}
	}
	return ""
}

// Current per-stream mappings are authority for the selected Gateway credential.
// A stale profile belonging to this same service must not win by list order.
func discordGatewayRuntimeProfile(cfg control.RuntimeConfig) (control.RuntimeProfile, bool) {
	if len(cfg.StreamDiscordConfigs) == 0 {
		return firstRuntimeProfileForService(cfg.Profiles["discord_config"], cfg.Service.ServiceID)
	}
	streams, _ := primaryDiscordAssignmentStreams(cfg)
	selected := ""
	for streamID := range streams {
		mapping, ok := cfg.DiscordConfigForStream(streamID)
		id := strings.TrimSpace(mapping.DiscordConfigID)
		if !ok || id == "" || (selected != "" && selected != id) {
			return control.RuntimeProfile{}, false
		}
		selected = id
	}
	if selected == "" {
		return control.RuntimeProfile{}, false
	}
	var match control.RuntimeProfile
	found := false
	for _, profile := range cfg.Profiles["discord_config"] {
		if strings.TrimSpace(profile.ID) != selected || !profileBelongsToService(profile, cfg.Service.ServiceID) {
			continue
		}
		if found { // Ambiguous duplicate IDs must not select a secret by list order.
			return control.RuntimeProfile{}, false
		}
		match, found = profile, true
	}
	return match, found
}

func firstRuntimeProfileForService(profiles []control.RuntimeProfile, serviceID string) (control.RuntimeProfile, bool) {
	// An explicitly scoped profile always wins over a legacy unscoped row.
	for _, profile := range profiles {
		if value, ok := profile.Config["service_id"].(string); ok && strings.TrimSpace(value) != "" && strings.TrimSpace(value) == strings.TrimSpace(serviceID) {
			return profile, true
		}
	}
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
