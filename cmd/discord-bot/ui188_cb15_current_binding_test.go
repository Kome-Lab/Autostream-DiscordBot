package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/example/autostream-discord-bot/internal/control"
	"github.com/example/autostream-discord-bot/internal/discord"
	"github.com/example/autostream-discord-bot/internal/jobs"
)

type cb15ObservedGateway struct {
	ui188R5Gateway
	targets    []discord.AutoStartVoiceTarget
	maxTargets int
	onDeselect func()
}

func (v *cb15ObservedGateway) SetAutoStartVoiceTargets(targets []discord.AutoStartVoiceTarget) {
	v.targets = append([]discord.AutoStartVoiceTarget(nil), targets...)
	if len(targets) > v.maxTargets {
		v.maxTargets = len(targets)
	}
}

func (v *cb15ObservedGateway) ReconcileGateway(c discord.Config, selected bool) error {
	err := v.ui188R5Gateway.ReconcileGateway(c, selected)
	if !selected && v.onDeselect != nil {
		v.onDeselect()
	}
	return err
}

func TestUI188CB15NormalLoopRevocationAndMalformedAuthorityClearEligibility(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		body   string
	}{
		{"401", 401, `{"code":"service_token_invalid"}`},
		{"unknown 403", 403, `{"code":"unrecognized_forbidden"}`},
		{"malformed authority", 200, `{"current_discord_bindings":{"version":"UNIT_ONLY_SECRET_NEVER_LOG"}}`},
		{"malformed epoch", 200, `{"current_discord_bindings":{"version":1,"bindings":[{"assignment_epoch":"UNIT_ONLY_SECRET_NEVER_LOG"}]}}`},
		{"duplicate readiness alias", 200, `{"current_discord_bindings":{"version":1,"bindings":[{"ready":false,"Ready":true}]}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			var reads, resolves, starts atomic.Int32
			cfg := cb15BoundConfig(t)
			cfg.StreamDiscordConfigs[0].AutoStartTrigger = "discord_voice_join"
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPost {
					if r.URL.Path == "/services/runtime-secrets/resolve" {
						resolves.Add(1)
						_ = json.NewEncoder(w).Encode(map[string]any{"value": "normal-loop-token"})
					} else {
						starts.Add(1)
						w.WriteHeader(403)
					}
					return
				}
				if reads.Add(1) == 1 {
					_ = json.NewEncoder(w).Encode(cfg)
					return
				}
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer server.Close()
			client := control.Client{Config: control.Config{ControlPanelURL: server.URL, Token: "test-only", ServiceID: "own", ServiceName: "own", ServicePublicURL: "https://bot.example.com"}}
			voice := &cb15ObservedGateway{onDeselect: cancel}
			manager := jobs.NewManager(voice)
			done := make(chan struct{})
			go func() {
				runRuntimeConfigRefreshLoop(ctx, client, manager, jobs.ReconnectPolicy{}, 5*time.Millisecond)
				close(done)
			}()
			<-done
			if reads.Load() != 2 || resolves.Load() != 1 || starts.Load() != 0 || voice.selected || voice.token != "" || voice.maxTargets != 1 || len(voice.targets) != 0 {
				t.Fatal("normal product loop kept revoked/ambiguous waiting authority or retried mutation")
			}
		})
	}
}

// Build the wire body independently so this oracle compiles and reaches the
// original selector before the new DTO exists.
func cb15BoundConfig(t *testing.T) control.RuntimeConfig {
	t.Helper()
	epoch := time.Unix(100, 0).UTC().Format(time.RFC3339Nano)
	body := map[string]any{
		"service":                  map[string]any{"service_id": "own", "service_type": "discord_bot", "current_stream_id": "s1"},
		"assignments":              []any{map[string]any{"stream_id": "s1", "service_id": "own", "service_type": "discord_bot", "assignment_role": "primary", "assigned_at": epoch}},
		"profiles":                 map[string]any{"discord_config": []any{map[string]any{"id": "current", "kind": "discord_config", "config": map[string]any{"service_id": "own", "bot_token_secret_name": "secret-current", "bot_token_fingerprint": "fp-current"}}}},
		"stream_discord_configs":   []any{map[string]any{"stream_id": "s1", "assignment_role": "primary", "discord_config_id": "current", "guild_id": "guild", "voice_channel_id": "voice"}},
		"current_discord_bindings": map[string]any{"version": 1, "bindings": []any{map[string]any{"stream_id": "s1", "service_id": "own", "service_type": "discord_bot", "assignment_role": "primary", "assignment_epoch": epoch, "profile_id": "current", "profile_owner_service_id": "own", "secret_name": "secret-current", "fingerprint": "fp-current", "ready": true}}},
	}
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	var result control.RuntimeConfig
	if err := json.Unmarshal(raw, &result); err != nil {
		t.Fatal(err)
	}
	return result
}

func cb15Clone(t *testing.T, cfg control.RuntimeConfig) control.RuntimeConfig {
	t.Helper()
	raw, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var result control.RuntimeConfig
	if err = json.Unmarshal(raw, &result); err != nil {
		t.Fatal(err)
	}
	return result
}

func TestUI188CB15CurrentBindingNegativeMatrix(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*control.RuntimeConfig)
	}{
		{"old Panel omitted", func(c *control.RuntimeConfig) {
			c.CurrentDiscordBindings = nil
			c.CurrentDiscordBindingsPresent = false
		}},
		{"null authority", func(c *control.RuntimeConfig) { c.CurrentDiscordBindings = nil; c.CurrentDiscordBindingsPresent = true }},
		{"empty authority with current", func(c *control.RuntimeConfig) {
			c.CurrentDiscordBindings.Bindings = []control.CurrentDiscordCredentialBinding{}
		}},
		{"null list", func(c *control.RuntimeConfig) { c.CurrentDiscordBindings.Bindings = nil }},
		{"unknown version", func(c *control.RuntimeConfig) { c.CurrentDiscordBindings.Version = 2 }},
		{"old epoch", func(c *control.RuntimeConfig) {
			c.CurrentDiscordBindings.Bindings[0].AssignmentEpoch = time.Unix(99, 0)
		}},
		{"assignment lost", func(c *control.RuntimeConfig) { c.Assignments = nil; c.Service.CurrentStreamID = "" }},
		{"foreign service", func(c *control.RuntimeConfig) { c.Service.ServiceID = "other" }},
		{"foreign assignment", func(c *control.RuntimeConfig) { c.Assignments[0].ServiceID = "other" }},
		{"wrong role", func(c *control.RuntimeConfig) { c.CurrentDiscordBindings.Bindings[0].AssignmentRole = "standby" }},
		{"foreign owner", func(c *control.RuntimeConfig) { c.CurrentDiscordBindings.Bindings[0].ProfileOwnerServiceID = "other" }},
		{"missing profile", func(c *control.RuntimeConfig) { c.Profiles["discord_config"] = nil }},
		{"duplicate profile", func(c *control.RuntimeConfig) {
			c.Profiles["discord_config"] = append(c.Profiles["discord_config"], c.Profiles["discord_config"][0])
		}},
		{"duplicate binding", func(c *control.RuntimeConfig) {
			c.CurrentDiscordBindings.Bindings = append(c.CurrentDiscordBindings.Bindings, c.CurrentDiscordBindings.Bindings[0])
		}},
		{"duplicate assignment", func(c *control.RuntimeConfig) { c.Assignments = append(c.Assignments, c.Assignments[0]) }},
		{"wrong kind", func(c *control.RuntimeConfig) { c.Profiles["discord_config"][0].Kind = "encoder_profile" }},
		{"missing owner", func(c *control.RuntimeConfig) { delete(c.Profiles["discord_config"][0].Config, "service_id") }},
		{"profile owner changed", func(c *control.RuntimeConfig) { c.Profiles["discord_config"][0].Config["service_id"] = "other" }},
		{"secret reference changed", func(c *control.RuntimeConfig) {
			c.Profiles["discord_config"][0].Config["bot_token_secret_name"] = "another-secret"
		}},
		{"fingerprint changed", func(c *control.RuntimeConfig) {
			c.Profiles["discord_config"][0].Config["bot_token_fingerprint"] = "new-fp"
		}},
		{"revoked", func(c *control.RuntimeConfig) {
			c.CurrentDiscordBindings.Bindings[0].Ready = false
			c.CurrentDiscordBindings.Bindings[0].ReadinessCode = "current_profile_unavailable"
		}},
		{"zero epoch", func(c *control.RuntimeConfig) {
			c.Assignments[0].AssignedAt = time.Time{}
			c.CurrentDiscordBindings.Bindings[0].AssignmentEpoch = time.Time{}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				_ = json.NewEncoder(w).Encode(map[string]any{"value": "synthetic-test-token"})
			}))
			defer server.Close()
			client := control.Client{Config: control.Config{ControlPanelURL: server.URL, Token: "test-only", ServiceID: "own", ServiceName: "own", ServicePublicURL: "https://bot.example.com"}}
			v := &ui188R5Gateway{}
			m := jobs.NewManager(v)
			cfg := cb15BoundConfig(t)
			cold := cb15Clone(t, cfg)
			tc.mutate(&cold)
			if err := reconcileRuntimeGateway(t.Context(), client, jobs.NewManager(&ui188R5Gateway{}), cold); err == nil || calls.Load() != 0 {
				t.Fatal("cold invalid authority reached resolver")
			}
			if err := reconcileRuntimeGateway(t.Context(), client, m, cfg); err != nil {
				t.Fatal(err)
			}
			tc.mutate(&cfg)
			if err := reconcileRuntimeGateway(t.Context(), client, m, cfg); err == nil || v.selected || v.token != "" || calls.Load() != 1 {
				t.Fatal("invalid current authority accepted or resolver retried")
			}
		})
	}
}

func TestUI188CB15CurrentBindingColdCacheAndHTTPRejections(t *testing.T) {
	for _, status := range []int{200, 401, 403, 409} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.WriteHeader(status)
				if status == 200 {
					_ = json.NewEncoder(w).Encode(map[string]any{"value": "cold-current-token"})
				} else {
					code := "unknown_forbidden"
					if status == 409 {
						code = "runtime_secret_lease_active"
					}
					_ = json.NewEncoder(w).Encode(map[string]any{"code": code})
				}
			}))
			defer server.Close()
			client := control.Client{Config: control.Config{ControlPanelURL: server.URL, Token: "test-only", ServiceID: "own", ServiceName: "own", ServicePublicURL: "https://bot.example.com"}}
			cfg := cb15BoundConfig(t)
			cfg.StreamDiscordConfigs = cfg.StreamDiscordConfigs[:0]
			v := &ui188R5Gateway{}
			m := jobs.NewManager(v)
			err := reconcileRuntimeGateway(t.Context(), client, m, cfg)
			if calls.Load() != 1 || ((status == 200) != (err == nil)) || v.selected != (status == 200) {
				t.Fatal("cold cache or rejection handling changed")
			}
		})
	}
}

func TestUI188CB15MultipleCurrentAndExplicitUnassigned(t *testing.T) {
	cfg := cb15BoundConfig(t)
	a := cfg.Assignments[0]
	a.StreamID = "s2"
	a.AssignedAt = time.Unix(101, 0)
	b := cfg.CurrentDiscordBindings.Bindings[0]
	b.StreamID = "s2"
	b.AssignmentEpoch = a.AssignedAt
	cfg.Assignments = append(cfg.Assignments, a)
	cfg.CurrentDiscordBindings.Bindings = append(cfg.CurrentDiscordBindings.Bindings, b)
	if _, err := currentDiscordGatewayAuthority(cfg, "own"); err != nil {
		t.Fatal("same-profile explicit currents rejected")
	}
	different := cb15Clone(t, cfg)
	different.CurrentDiscordBindings.Bindings[1].ProfileID = "other-profile"
	other := control.RuntimeProfile{ID: "other-profile", Kind: "discord_config", Config: map[string]any{"service_id": "own", "bot_token_secret_name": "other-secret", "bot_token_fingerprint": "other-fingerprint"}}
	different.Profiles["discord_config"] = append(different.Profiles["discord_config"], other)
	different.CurrentDiscordBindings.Bindings[1].SecretName = "other-secret"
	different.CurrentDiscordBindings.Bindings[1].Fingerprint = "other-fingerprint"
	if _, err := currentDiscordGatewayAuthority(different, "own"); err == nil {
		t.Fatal("ambiguous profiles accepted")
	}
	cfg.Assignments = nil
	cfg.Service.CurrentStreamID = ""
	cfg.CurrentDiscordBindings.Bindings = []control.CurrentDiscordCredentialBinding{}
	if result, err := currentDiscordGatewayAuthority(cfg, "own"); err != nil || len(result.current) != 0 {
		t.Fatal("explicit unassigned rejected")
	}
}

func TestUI188CB15NormalRefreshLoopKeepsCurrentAcrossStarting(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	var reads, resolves atomic.Int32
	cfg := cb15BoundConfig(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			resolves.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{"value": "normal-loop-token"})
			return
		}
		n := reads.Add(1)
		body := cb15Clone(t, cfg)
		if n > 1 {
			body.StreamDiscordConfigs = []control.StreamDiscordConfig{{StreamID: "other-created", AssignmentRole: "primary", DiscordConfigID: "current", GuildID: "guild", VoiceChannelID: "voice"}}
		}
		_ = json.NewEncoder(w).Encode(body)
	}))
	defer server.Close()
	client := control.Client{Config: control.Config{ControlPanelURL: server.URL, Token: "test-only", ServiceID: "own", ServiceName: "own", ServicePublicURL: "https://bot.example.com"}}
	v := &ui188R5Gateway{}
	m := jobs.NewManager(v)
	done := make(chan struct{})
	go func() {
		runRuntimeConfigRefreshLoop(ctx, client, m, jobs.ReconnectPolicy{}, 5*time.Millisecond)
		close(done)
	}()
	for reads.Load() < 4 {
		select {
		case <-ctx.Done():
			t.Fatal("normal refresh did not advance")
		case <-time.After(time.Millisecond):
		}
	}
	cancel()
	<-done
	if resolves.Load() != 1 || !v.selected || v.token != "normal-loop-token" {
		t.Fatal("normal product refresh lost current credential")
	}
}

func TestUI188CB15CurrentBindingKeepsGatewayWithOtherWaiting(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		_ = json.NewEncoder(w).Encode(map[string]any{"value": "synthetic-cb15-token", "expires_in_sec": 60})
	}))
	defer server.Close()
	client := control.Client{Config: control.Config{ControlPanelURL: server.URL, Token: "test-only-control", ServiceID: "own", ServiceName: "own", ServicePublicURL: "https://bot.example.com"}}
	voice := &ui188R5Gateway{}
	manager := jobs.NewManager(voice)
	cfg := cb15BoundConfig(t)
	if err := reconcileRuntimeGateway(t.Context(), client, manager, cfg); err != nil {
		t.Fatal(err)
	}
	cfg.StreamDiscordConfigs = []control.StreamDiscordConfig{{StreamID: "other-created", AssignmentRole: "primary", DiscordConfigID: "current", GuildID: "guild", VoiceChannelID: "voice"}}
	if err := reconcileRuntimeGateway(t.Context(), client, manager, cfg); err != nil {
		t.Fatalf("current binding rejected because another waiting candidate remains: %v", err)
	}
	if !voice.selected || voice.token != "synthetic-cb15-token" || calls.Load() != 1 {
		t.Fatal("current authority lost or resolver automatically reacquired lease")
	}
}
