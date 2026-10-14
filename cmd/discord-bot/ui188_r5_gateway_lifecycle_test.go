package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/example/autostream-discord-bot/internal/control"
	"github.com/example/autostream-discord-bot/internal/discord"
	"github.com/example/autostream-discord-bot/internal/jobs"
)

type ui188R5Gateway struct {
	discord.NoopClient
	selected bool
	token    string
}

func (v *ui188R5Gateway) ReconcileGateway(c discord.Config, s bool) error {
	v.selected = s
	v.token = c.BotToken
	return nil
}
func TestUI188R5RuntimeGatewayActiveProjectionKeepsExactAuthority(t *testing.T) {
	calls := []string{}
	var callsMu sync.Mutex
	resolved := func() []string { callsMu.Lock(); defer callsMu.Unlock(); return append([]string(nil), calls...) }
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]string
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		callsMu.Lock()
		defer callsMu.Unlock()
		calls = append(calls, body["secret_name"])
		if len(calls) > 1 {
			w.WriteHeader(409)
			_, _ = w.Write([]byte(`{"code":"runtime_secret_lease_active"}`))
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"value": "new-test-token", "expires_in_sec": 60})
	}))
	defer server.Close()
	client := control.Client{Config: control.Config{ControlPanelURL: server.URL, Token: "test-only-control", ServiceID: "own", ServiceName: "own", ServicePublicURL: "https://bot.example.com"}}
	profile := func(id, owner string) control.RuntimeProfile {
		return control.RuntimeProfile{ID: id, Config: map[string]any{"service_id": owner, "bot_token_secret_name": "secret-" + id, "bot_token_fingerprint": "fp-" + id}}
	}
	base := control.RuntimeConfig{Service: control.RegisteredService{ServiceID: "own"}, Assignments: []control.StreamServiceAssignment{{StreamID: "s", ServiceID: "own", ServiceType: control.ServiceType, AssignmentRole: "primary", AssignedAt: time.Unix(100, 0)}}, StreamDiscordConfigs: []control.StreamDiscordConfig{{StreamID: "s", AssignmentRole: "primary", DiscordConfigID: "new", GuildID: "guild", VoiceChannelID: "voice"}}, Profiles: map[string][]control.RuntimeProfile{"discord_config": {profile("old", "own"), profile("new", "own")}}}
	voice := &ui188R5Gateway{}
	m := jobs.NewManager(voice)
	if err := reconcileRuntimeGateway(t.Context(), client, m, base); err != nil {
		t.Fatal(err)
	}
	active := base
	active.StreamDiscordConfigs = nil
	for range 3 {
		if err := reconcileRuntimeGateway(t.Context(), client, m, active); err != nil {
			t.Fatal(err)
		}
	}
	if got := resolved(); len(got) != 1 || got[0] != "secret-new" || !voice.selected || voice.token != "new-test-token" {
		t.Fatal("active projection switched to old profile or reacquired lease")
	}
	for _, tc := range []struct {
		name   string
		mutate func(*control.RuntimeConfig)
	}{
		{"old epoch", func(c *control.RuntimeConfig) {
			c.Assignments = append([]control.StreamServiceAssignment(nil), c.Assignments...)
			c.Assignments[0].AssignedAt = time.Unix(101, 0)
		}},
		{"foreign owner", func(c *control.RuntimeConfig) {
			c.Profiles = map[string][]control.RuntimeProfile{"discord_config": {profile("old", "own"), profile("new", "other")}}
		}},
		{"duplicate profile", func(c *control.RuntimeConfig) {
			c.Profiles = map[string][]control.RuntimeProfile{"discord_config": {profile("new", "own"), profile("new", "own")}}
		}},
		{"missing profile", func(c *control.RuntimeConfig) {
			c.Profiles = map[string][]control.RuntimeProfile{"discord_config": {profile("old", "own")}}
		}},
		{"missing fingerprint", func(c *control.RuntimeConfig) {
			c.Profiles = map[string][]control.RuntimeProfile{"discord_config": {profile("old", "own"), profile("new", "own")}}
			delete(c.Profiles["discord_config"][1].Config, "bot_token_fingerprint")
		}},
		{"wrong mapping", func(c *control.RuntimeConfig) {
			c.StreamDiscordConfigs = []control.StreamDiscordConfig{{StreamID: "other", AssignmentRole: "primary", DiscordConfigID: "old"}}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := &ui188R5Gateway{}
			manager := jobs.NewManager(v)
			cfg := base
			// Each negative receives current verified authority before corruption.
			// Give it an independent HTTP responder; do not borrow another session's token.
			okServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_ = json.NewEncoder(w).Encode(map[string]any{"value": "test-token"})
			}))
			defer okServer.Close()
			c := client
			c.Config.ControlPanelURL = okServer.URL
			if err := reconcileRuntimeGateway(t.Context(), c, manager, cfg); err != nil {
				t.Fatal(err)
			}
			cfg.StreamDiscordConfigs = nil
			tc.mutate(&cfg)
			if err := reconcileRuntimeGateway(t.Context(), c, manager, cfg); err == nil || v.selected || v.token != "" {
				t.Fatal("invalid authority remained selected")
			}
		})
	}
	rotated := base
	rotated.Profiles = map[string][]control.RuntimeProfile{"discord_config": {profile("old", "own"), profile("new", "own")}}
	rotated.Profiles["discord_config"][1].Config["bot_token_fingerprint"] = "fp-rotated"
	if err := reconcileRuntimeGateway(t.Context(), client, m, rotated); err == nil || voice.selected || voice.token != "" || len(resolved()) != 2 {
		t.Fatal("actual HTTP409 rotation reused cached token")
	}
}
