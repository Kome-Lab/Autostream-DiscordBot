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

type waitingDecodeRefreshVoice struct {
	discord.NoopClient
	disconnects atomic.Int32
}

func (v *waitingDecodeRefreshVoice) ReconcileGateway(_ discord.Config, selected bool) error {
	if !selected {
		v.disconnects.Add(1)
	}
	return nil
}

func TestWaitingMonitorDecodeFailurePreservesActiveGatewayOnPeriodicRefresh(t *testing.T) {
	for _, tc := range []struct {
		name       string
		waiting    string
		current    string
		mutate     func(*control.RuntimeConfig)
		disconnect bool
	}{
		{name: "missing execution flag", waiting: `{"version":1,"bindings":[]}`},
		{name: "invalid execution flag", waiting: `{"version":1,"execution_active":"invalid","bindings":[]}`},
		{name: "invalid waiting epoch", waiting: `{"version":1,"execution_active":true,"bindings":[{"selection_epoch":"invalid"}]}`},
		{name: "invalid current epoch", current: `{"version":1,"bindings":[{"assignment_epoch":"invalid"}]}`, disconnect: true},
		{name: "null current", current: "null", disconnect: true},
		{name: "unsupported current version", mutate: func(cfg *control.RuntimeConfig) { cfg.CurrentDiscordBindings.Version = 2 }, disconnect: true},
		{name: "current no longer ready", mutate: func(cfg *control.RuntimeConfig) { cfg.CurrentDiscordBindings.Bindings[0].Ready = false }, disconnect: true},
		{name: "current owner changed", mutate: func(cfg *control.RuntimeConfig) { cfg.CurrentDiscordBindings.Bindings[0].ProfileOwnerServiceID = "another" }, disconnect: true},
		{name: "current profile mismatch", mutate: func(cfg *control.RuntimeConfig) { cfg.Profiles["discord_config"][0].Config["bot_token_fingerprint"] = "another" }, disconnect: true},
		{name: "local owner removed", mutate: func(cfg *control.RuntimeConfig) {
			cfg.Assignments = []control.StreamServiceAssignment{}
			cfg.CurrentDiscordBindings.Bindings = []control.CurrentDiscordCredentialBinding{}
			cfg.Service.CurrentStreamID = ""
		}, disconnect: true},
	} {
		for _, waitingFirst := range []bool{false, true} {
			order := "current first"
			if waitingFirst {
				order = "waiting first"
			}
			t.Run(tc.name+"/"+order, func(t *testing.T) {
				cfg := currentWaitingMonitorFixture()
				if tc.mutate != nil {
					tc.mutate(&cfg)
				}
				encoded, err := json.Marshal(cfg)
				if err != nil {
					t.Fatal(err)
				}
				var fields map[string]json.RawMessage
				if err := json.Unmarshal(encoded, &fields); err != nil {
					t.Fatal(err)
				}
				waiting := tc.waiting
				if waiting == "" {
					waiting = `{"version":1,"bindings":[]}`
				}
				fields["discord_waiting_monitor"] = json.RawMessage(waiting)
				if tc.current != "" {
					fields["current_discord_bindings"] = json.RawMessage(tc.current)
				}
				if waitingFirst {
					delete(fields, "discord_waiting_monitor")
				}
				body, err := json.Marshal(fields)
				if err != nil {
					t.Fatal(err)
				}
				if waitingFirst {
					body = append([]byte(`{"discord_waiting_monitor":`+waiting+","), body[1:]...)
				}
				runWaitingDecodeRefreshCase(t, body, tc.disconnect)
			})
		}
	}
}

func runWaitingDecodeRefreshCase(t *testing.T, body []byte, wantDisconnect bool) {
	t.Helper()
	var calls atomic.Int32
	processed := make(chan struct{})
	panel := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/services/runtime-config" || r.Header.Get("Authorization") != "Bearer test-only" {
			t.Error("unexpected unauthenticated runtime refresh")
		}
		// A second request proves the first serialized refresh completed.
		if calls.Add(1) == 2 {
			close(processed)
		}
		_, _ = w.Write(body)
	}))
	defer panel.Close()
	voice := &waitingDecodeRefreshVoice{}
	manager := jobs.NewManager(voice)
	if err := manager.ReconcileGatewayCredential("active-reference", true, func() (string, error) { return "synthetic-active-token", nil }); err != nil {
		t.Fatal(err)
	}
	if err := manager.Start(discord.VoiceJob{StreamID: "running-A", GuildID: "100000000000000001", VoiceChannelID: "100000000000000002", JobGeneration: 1}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = manager.Stop("running-A") })
	client := control.Client{Config: control.Config{
		ControlPanelURL: panel.URL, Token: "test-only", ServiceID: "own", ServiceName: "own", ServicePublicURL: "https://bot.example.com",
	}}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		runRuntimeConfigRefreshLoop(ctx, client, manager, jobs.ReconnectPolicy{}, time.Millisecond)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("runtime refresh did not stop")
		}
	})
	select {
	case <-processed:
	case <-time.After(2 * time.Second):
		t.Fatal("runtime refresh did not complete its first response")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("runtime refresh loop did not stop")
	}
	if got := voice.disconnects.Load() > 0; got != wantDisconnect {
		t.Fatalf("active Gateway disconnected=%t, expected=%t", got, wantDisconnect)
	}
	if manager.CurrentStreamID() != "running-A" {
		t.Fatal("decode failure replaced the active execution")
	}
}
