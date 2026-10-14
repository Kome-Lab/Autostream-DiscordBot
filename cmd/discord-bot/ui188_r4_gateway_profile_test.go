package main

import (
	"github.com/example/autostream-discord-bot/internal/control"
	"testing"
)

func TestUI188R4GatewayCredentialUsesAssignedConfig(t *testing.T) {
	profile := func(id, owner string) control.RuntimeProfile {
		return control.RuntimeProfile{ID: id, Config: map[string]any{"service_id": owner, "bot_token_secret_name": "secret-" + id, "bot_token_fingerprint": "fingerprint-" + id}}
	}
	base := func() control.RuntimeConfig {
		return control.RuntimeConfig{
			Service:              control.RegisteredService{ServiceID: "own"},
			Assignments:          []control.StreamServiceAssignment{{StreamID: "current", ServiceID: "own", ServiceType: control.ServiceType, AssignmentRole: "primary"}},
			StreamDiscordConfigs: []control.StreamDiscordConfig{{StreamID: "current", AssignmentRole: "primary", DiscordConfigID: "new", GuildID: "guild", VoiceChannelID: "vc"}},
			Profiles:             map[string][]control.RuntimeProfile{"discord_config": {profile("old", "own"), profile("new", "own")}},
		}
	}
	t.Run("current assignment wins over old first row", func(t *testing.T) {
		cfg := base()
		if got := discordBotTokenSecretNameFromRuntimeConfig(cfg); got != "secret-new" {
			t.Fatalf("actual selected reference %q", got)
		}
		if got := discordBotTokenFingerprintFromRuntimeConfig(cfg); got != "fingerprint-new" {
			t.Fatalf("actual fingerprint %q", got)
		}
	})
	for _, tc := range []struct {
		name   string
		mutate func(*control.RuntimeConfig)
	}{
		{"missing current profile", func(c *control.RuntimeConfig) { c.Profiles["discord_config"] = c.Profiles["discord_config"][:1] }},
		{"foreign profile owner", func(c *control.RuntimeConfig) { c.Profiles["discord_config"][1] = profile("new", "other") }},
		{"missing current mapping", func(c *control.RuntimeConfig) { c.StreamDiscordConfigs[0].StreamID = "old-stream" }},
		{"foreign assignment", func(c *control.RuntimeConfig) { c.Assignments[0].ServiceID = "other" }},
		{"ambiguous primary credentials", func(c *control.RuntimeConfig) {
			c.Assignments = append(c.Assignments, control.StreamServiceAssignment{StreamID: "second", ServiceID: "own", ServiceType: control.ServiceType, AssignmentRole: "primary"})
			c.StreamDiscordConfigs = append(c.StreamDiscordConfigs, control.StreamDiscordConfig{StreamID: "second", AssignmentRole: "primary", DiscordConfigID: "old", GuildID: "guild", VoiceChannelID: "vc"})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := base()
			tc.mutate(&cfg)
			if got := discordBotTokenSecretNameFromRuntimeConfig(cfg); got != "" {
				t.Fatalf("unsafe fallback %q", got)
			}
		})
	}
}
