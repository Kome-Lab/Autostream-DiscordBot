package main

import (
	"testing"
	"time"

	"github.com/example/autostream-discord-bot/internal/control"
	discordclient "github.com/example/autostream-discord-bot/internal/discord"
	"github.com/example/autostream-discord-bot/internal/jobs"
)

type fakeStreamStarter struct {
	started chan string
}

func (f fakeStreamStarter) StartStream(streamID string) error {
	f.started <- streamID
	return nil
}

func TestDiscordBotTokenSecretNameUsesOnlyOwnServiceProfile(t *testing.T) {
	cfg := control.RuntimeConfig{
		Service: control.RegisteredService{ServiceID: "discord-bot-01"},
		Profiles: map[string][]control.RuntimeProfile{
			"discord_config": {
				{
					ID:   "discord-other",
					Kind: "discord_config",
					Config: map[string]any{
						"service_id":            "discord-bot-02",
						"guild_id":              "guild-other",
						"voice_channel_id":      "voice-other",
						"bot_token_secret_name": "discord-bot-other",
						"text_channel_id":       "text-other",
					},
					CreatedAt: testProfileTime(),
					UpdatedAt: testProfileTime(),
				},
				{
					ID:   "discord-own",
					Kind: "discord_config",
					Config: map[string]any{
						"service_id":            "discord-bot-01",
						"guild_id":              "guild-own",
						"voice_channel_id":      "voice-own",
						"bot_token_secret_name": "discord-bot-own",
						"text_channel_id":       "text-own",
					},
					CreatedAt: testProfileTime(),
					UpdatedAt: testProfileTime(),
				},
			},
		},
	}

	if got := discordBotTokenSecretNameFromRuntimeConfig(cfg); got != "discord-bot-own" {
		t.Fatalf("expected own bot token secret name, got %q", got)
	}
}

func TestDiscordBotTokenSecretNameAllowsUnscopedFallback(t *testing.T) {
	cfg := control.RuntimeConfig{
		Service: control.RegisteredService{ServiceID: "discord-bot-01"},
		Profiles: map[string][]control.RuntimeProfile{
			"discord_config": {
				{
					ID:   "discord-other",
					Kind: "discord_config",
					Config: map[string]any{
						"service_id":            "discord-bot-02",
						"guild_id":              "guild-other",
						"voice_channel_id":      "voice-other",
						"bot_token_secret_name": "discord-bot-other",
					},
					CreatedAt: testProfileTime(),
					UpdatedAt: testProfileTime(),
				},
				{
					ID:   "discord-global",
					Kind: "discord_config",
					Config: map[string]any{
						"guild_id":              "guild-global",
						"voice_channel_id":      "voice-global",
						"bot_token_secret_name": "discord-bot-global",
					},
					CreatedAt: testProfileTime(),
					UpdatedAt: testProfileTime(),
				},
			},
		},
	}

	if got := discordBotTokenSecretNameFromRuntimeConfig(cfg); got != "discord-bot-global" {
		t.Fatalf("expected unscoped bot token secret fallback, got %q", got)
	}
}

func TestDiscordBotTokenSecretNameRejectsMalformedServiceID(t *testing.T) {
	cfg := control.RuntimeConfig{
		Service: control.RegisteredService{ServiceID: "discord-bot-01"},
		Profiles: map[string][]control.RuntimeProfile{
			"discord_config": {
				{
					ID:   "discord-malformed",
					Kind: "discord_config",
					Config: map[string]any{
						"service_id":            []string{"discord-bot-01"},
						"guild_id":              "guild-malformed",
						"voice_channel_id":      "voice-malformed",
						"bot_token_secret_name": "discord-bot-malformed",
					},
					CreatedAt: testProfileTime(),
					UpdatedAt: testProfileTime(),
				},
			},
		},
	}

	if got := discordBotTokenSecretNameFromRuntimeConfig(cfg); got != "" {
		t.Fatalf("malformed service-scoped profile should not provide a bot token secret: %q", got)
	}
}

func TestApplyRuntimeConfigToManagerRefreshesAutoStartDefaults(t *testing.T) {
	manager := jobs.NewManager(&discordclient.NoopClient{})
	starter := fakeStreamStarter{started: make(chan string, 1)}
	manager.SetStreamStarter(starter)

	cfg := control.RuntimeConfig{
		Service: control.RegisteredService{ServiceID: "discord-bot-01"},
		StreamDiscordConfigs: []control.StreamDiscordConfig{
			{
				StreamID:         "stream-new",
				AssignmentRole:   "primary",
				GuildID:          "guild-new",
				VoiceChannelID:   "voice-new",
				TextChannelID:    "text-new",
				AutoStartTrigger: "discord_voice_join",
			},
		},
	}
	applyRuntimeConfigToManager(manager, cfg, reconnectPolicyFromEnv())

	manager.VoiceUserJoined(discordclient.VoiceJoinEvent{GuildID: "guild-new", VoiceChannelID: "voice-new", UserID: "user-01"})
	select {
	case got := <-starter.started:
		if got != "stream-new" {
			t.Fatalf("unexpected auto-start stream: %q", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for refreshed auto-start defaults")
	}
}

func TestApplyRuntimeConfigToManagerDoesNotAutoStartWithoutTrigger(t *testing.T) {
	manager := jobs.NewManager(&discordclient.NoopClient{})
	starter := fakeStreamStarter{started: make(chan string, 1)}
	manager.SetStreamStarter(starter)

	cfg := control.RuntimeConfig{
		Service: control.RegisteredService{ServiceID: "discord-bot-01"},
		StreamDiscordConfigs: []control.StreamDiscordConfig{
			{
				StreamID:       "stream-new",
				AssignmentRole: "primary",
				GuildID:        "guild-new",
				VoiceChannelID: "voice-new",
			},
		},
	}
	applyRuntimeConfigToManager(manager, cfg, reconnectPolicyFromEnv())

	manager.VoiceUserJoined(discordclient.VoiceJoinEvent{GuildID: "guild-new", VoiceChannelID: "voice-new", UserID: "user-01"})
	select {
	case got := <-starter.started:
		t.Fatalf("stream without auto-start trigger should not start, got %q", got)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestReconnectPolicyFromRuntimeConfigOverridesFallback(t *testing.T) {
	cfg := control.RuntimeConfig{
		Service: control.RegisteredService{ServiceID: "discord-bot-01"},
		Profiles: map[string][]control.RuntimeProfile{
			"discord_config": {
				{
					ID:   "discord-own",
					Kind: "discord_config",
					Config: map[string]any{
						"service_id":             "discord-bot-01",
						"reconnect_enabled":      false,
						"reconnect_max_attempts": float64(7),
						"reconnect_base_delay":   "3s",
						"reconnect_max_delay":    "45s",
					},
					CreatedAt: testProfileTime(),
					UpdatedAt: testProfileTime(),
				},
			},
		},
	}
	policy := reconnectPolicyFromRuntimeConfig(cfg, jobs.ReconnectPolicy{Enabled: true, MaxAttempts: 2, BaseDelay: time.Second, MaxDelay: 5 * time.Second})
	if policy.Enabled || policy.MaxAttempts != 7 || policy.BaseDelay != 3*time.Second || policy.MaxDelay != 45*time.Second {
		t.Fatalf("unexpected reconnect policy: %#v", policy)
	}
}

func TestRequireControlPanelRuntimeConfigInProduction(t *testing.T) {
	t.Setenv("AUTOSTREAM_ENV", "production")
	if !requireControlPanelRuntimeConfig() {
		t.Fatal("expected production Discord Bot to require Control Panel runtime config")
	}
}

func TestRequireControlPanelRuntimeConfigExplicitEnv(t *testing.T) {
	t.Setenv("AUTOSTREAM_REQUIRE_CONTROL_PANEL_RUNTIME_CONFIG", "true")
	if !requireControlPanelRuntimeConfig() {
		t.Fatal("expected explicit runtime config requirement")
	}
}

func TestBuildDiscordClientRejectsDryRunWhenRuntimeConfigRequired(t *testing.T) {
	client, err := buildDiscordClient(discordclient.Config{}, "environment fallback", false)
	if err == nil {
		t.Fatal("expected missing Discord token to fail when dry-run fallback is disabled")
	}
	if client != nil {
		t.Fatalf("expected no client when dry-run fallback is disabled, got %#v", client)
	}
}

func TestBuildDiscordClientAllowsDryRunOutsideProduction(t *testing.T) {
	client, err := buildDiscordClient(discordclient.Config{}, "environment fallback", true)
	if err != nil {
		t.Fatalf("expected dry-run fallback outside production: %v", err)
	}
	if _, ok := client.(*discordclient.NoopClient); !ok {
		t.Fatalf("expected dry-run noop client, got %#v", client)
	}
}

func testProfileTime() time.Time {
	return time.Date(2026, 6, 10, 0, 0, 0, 0, time.UTC)
}
