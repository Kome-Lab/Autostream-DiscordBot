package main

import (
	"context"
	"errors"
	"log"
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
	"github.com/example/autostream-discord-bot/internal/worker"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	addr := os.Getenv("AUTOSTREAM_BIND_ADDR")
	if addr == "" {
		addr = "127.0.0.1:8080"
	}

	controlCfg := control.ConfigFromEnv()
	controlClient := control.Client{Config: controlCfg}
	var runtimeConfigProvider httpapi.RuntimeConfigProvider
	var runtimeCfg control.RuntimeConfig
	hasRuntimeConfig := false
	requireRuntimeConfig := requireControlPanelRuntimeConfig()
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
		runtimeConfigProvider = controlClient.RuntimeConfig
	} else if requireRuntimeConfig {
		log.Fatal("CONTROL_PANEL_URL and CONTROL_PANEL_TOKEN are required in this environment")
	}

	discordCfg := discordclient.ConfigFromEnv()
	tokenSource := "environment fallback"
	if hasRuntimeConfig {
		secretName := discordBotTokenSecretNameFromRuntimeConfig(runtimeCfg)
		if secretName != "" {
			secret, err := controlClient.ResolveRuntimeSecret(ctx, secretName)
			if errors.Is(err, control.ErrRuntimeSecretLeaseActive) {
				if requireRuntimeConfig {
					log.Fatalf("control panel Discord bot token runtime secret is required but lease is active: %v", err)
				}
				log.Printf("control panel Discord bot token runtime secret lease is active; using existing environment fallback only if configured: %v", err)
			} else if err != nil {
				if requireRuntimeConfig {
					log.Fatalf("control panel Discord bot token runtime secret resolve is required in this environment: %v", err)
				}
				log.Printf("control panel Discord bot token resolve failed; falling back to environment token if configured: %v", err)
			} else if secret.Value != "" {
				discordCfg.BotToken = secret.Value
				tokenSource = "Control Panel Discord config"
				log.Printf("loaded Discord bot token from Control Panel runtime secret %s", secret.SecretName)
			} else if requireRuntimeConfig {
				log.Fatal("control panel Discord bot token runtime secret resolved to an empty value")
			}
		} else if requireRuntimeConfig {
			log.Fatal("control panel Discord bot token secret reference is required in this environment")
		}
	}

	voiceClient, err := buildDiscordClient(discordCfg, tokenSource, !requireRuntimeConfig)
	if err != nil {
		log.Fatal(err)
	}
	if source, ok := voiceClient.(discordclient.AudioForwardSource); ok {
		cfg := audioforward.ConfigFromEnv()
		source.SetAudioForwarder(audioforward.Client{Config: cfg}, controlCfg.ServiceID)
		if !cfg.Enabled() {
			log.Printf("static encoder audio token is not configured; job-scoped signed ingest tokens will be used")
		}
	}
	manager := jobs.NewManagerWithReporter(voiceClient, buildWorkerReporter())
	if controlClient.Config.ControlPanelURL != "" && controlClient.Config.Token != "" {
		manager.SetStreamStarter(controlStreamStarter{client: controlClient})
	}
	if eventSource, ok := voiceClient.(discordclient.EventSource); ok {
		eventSource.SetEventSink(manager)
	}
	reconnectPolicy := reconnectPolicyFromEnv()
	if hasRuntimeConfig {
		manager.SetVoiceDefaults(discordDefaultsFromRuntimeConfig(runtimeCfg))
		manager.SetStreamVoiceDefaults(streamDiscordDefaultsFromRuntimeConfig(runtimeCfg))
		reconnectPolicy = reconnectPolicyFromRuntimeConfig(runtimeCfg, reconnectPolicy)
	}
	manager.SetReconnectPolicy(reconnectPolicy)

	if controlClient.Config.ControlPanelURL != "" && controlClient.Config.Token != "" {
		go controlClient.RunHeartbeatLoopWithMetrics(ctx, manager.CurrentStreamID, manager.Metrics, func(err error) {
			log.Printf("control panel heartbeat failed: %v", err)
		})
	}

	server := &http.Server{
		Addr:              addr,
		Handler:           httpapi.NewServerWithRuntimeConfig(control.ServiceType, manager, httpapi.TokenVerifierFromEnv(), runtimeConfigProvider),
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

type controlStreamStarter struct {
	client control.Client
}

func (s controlStreamStarter) StartStream(streamID string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	err := s.client.StartStream(ctx, streamID)
	if err != nil {
		log.Printf("control panel auto-start failed for stream %s: %v", streamID, err)
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
	log.Printf("loaded control panel runtime config for %s: assignments=%d profiles=%d", cfg.Service.ServiceID, len(cfg.Assignments), profileCount)
	return cfg, true
}

func discordDefaultsFromRuntimeConfig(cfg control.RuntimeConfig) jobs.VoiceDefaults {
	if profile, ok := firstRuntimeProfileForService(cfg.Profiles["discord_config"], cfg.Service.ServiceID); ok {
		return jobs.VoiceDefaults{
			GuildID:         stringConfig(profile.Config, "guild_id"),
			VoiceChannelID:  stringConfig(profile.Config, "voice_channel_id"),
			TextChannelID:   stringConfig(profile.Config, "text_channel_id"),
			CaptionAudioURL: stringConfig(profile.Config, "caption_audio_url"),
		}
	}
	return jobs.VoiceDefaults{}
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
	for _, item := range cfg.StreamDiscordConfigs {
		if item.StreamID == "" {
			continue
		}
		if item.AssignmentRole != "primary" {
			continue
		}
		defaults[item.StreamID] = jobs.VoiceDefaults{
			GuildID:         item.GuildID,
			VoiceChannelID:  item.VoiceChannelID,
			TextChannelID:   item.TextChannelID,
			CaptionAudioURL: item.CaptionAudioURL,
		}
	}
	return defaults
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
	cfg := worker.ConfigFromEnv()
	if !cfg.Enabled() {
		log.Printf("WORKER_URL or WORKER_TOKEN is not configured; Discord participant events will stay local")
		return nil
	}
	return worker.Reporter{Config: cfg}
}

func buildDiscordClient(cfg discordclient.Config, tokenSource string, allowDryRun bool) (discordclient.Client, error) {
	if cfg.BotToken == "" {
		if !allowDryRun {
			return nil, errors.New("Discord bot token is required in this environment")
		}
		log.Printf("Discord bot token is not configured by Control Panel or environment; running in dry-run Discord mode")
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
