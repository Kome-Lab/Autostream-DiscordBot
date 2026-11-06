package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/example/autostream-discord-bot/internal/control"
	"github.com/example/autostream-discord-bot/internal/discord"
	"github.com/example/autostream-discord-bot/internal/jobs"
)

type ownerRevocationGateway struct {
	discord.NoopClient
	selected    atomic.Bool
	joined      atomic.Bool
	disconnects atomic.Int32
	joins       atomic.Int32
}

func (v *ownerRevocationGateway) ReconcileGateway(_ discord.Config, selected bool) error {
	v.selected.Store(selected)
	if !selected {
		v.joined.Store(false)
		v.disconnects.Add(1)
	}
	return nil
}

func (v *ownerRevocationGateway) JoinVoice(_ discord.VoiceJob) error {
	if !v.selected.Load() {
		return errors.New("test Gateway has no selected credential")
	}
	v.joined.Store(true)
	v.joins.Add(1)
	return nil
}

func TestSuccessfulPeriodicRefreshRevokesGatewayWhenLocalExecutionOwnerRemoved(t *testing.T) {
	for _, tc := range []struct {
		name       string
		mutate     func(*control.RuntimeConfig)
		disconnect bool
	}{
		{"valid empty current", func(cfg *control.RuntimeConfig) {
			cfg.Service.CurrentStreamID = ""
			cfg.Assignments = []control.StreamServiceAssignment{}
			cfg.CurrentDiscordBindings.Bindings = []control.CurrentDiscordCredentialBinding{}
			cfg.DiscordWaitingMonitor.ExecutionActive = false
			cfg.DiscordWaitingMonitor.Bindings = cfg.DiscordWaitingMonitor.Bindings[1:]
			cfg.DiscordWaitingMonitor.Bindings[0].SecretName = "discord_bot_token_waiting_B"
			cfg.DiscordWaitingMonitor.Bindings[0].Fingerprint = "waiting-B-fingerprint"
		}, true},
		{"valid replacement current", func(cfg *control.RuntimeConfig) {
			cfg.Service.CurrentStreamID = "replacement-B"
			cfg.Assignments[0].StreamID = "replacement-B"
			binding := &cfg.CurrentDiscordBindings.Bindings[0]
			binding.StreamID = "replacement-B"
			binding.ProfileID = "replacement-profile"
			binding.SecretName = "discord_bot_token_replacement"
			binding.Fingerprint = "replacement-fingerprint"
			profile := &cfg.Profiles["discord_config"][0]
			profile.ID = binding.ProfileID
			profile.Config["bot_token_secret_name"] = binding.SecretName
			profile.Config["bot_token_fingerprint"] = binding.Fingerprint
		}, true},
		{"A remains owner while B target changes", func(cfg *control.RuntimeConfig) {
			cfg.DiscordWaitingMonitor.Bindings[1].VoiceChannelID = "100000000000000004"
			cfg.DiscordWaitingMonitor.Bindings[1].TargetRevision++
		}, false},
		{"A remains owner while B credential changes", func(cfg *control.RuntimeConfig) {
			cfg.DiscordWaitingMonitor.Bindings[1].SecretName = "discord_bot_token_waiting_B"
			cfg.DiscordWaitingMonitor.Bindings[1].Fingerprint = "waiting-B-fingerprint"
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			initial := currentWaitingMonitorFixture()
			next := currentWaitingMonitorFixture()
			tc.mutate(&next)
			body, err := json.Marshal(next)
			if err != nil {
				t.Fatal(err)
			}
			var decoded control.RuntimeConfig
			if err := json.Unmarshal(body, &decoded); err != nil {
				t.Fatalf("test response must decode successfully: %v", err)
			}
			if _, err := currentDiscordGatewayAuthority(decoded, "own"); err != nil {
				t.Fatalf("test response must contain valid current authority: %v", err)
			}
			var reads, resolves atomic.Int32
			processed := make(chan struct{})
			panel := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Authorization") != "Bearer test-only" {
					t.Error("unauthenticated runtime request")
				}
				switch r.URL.Path {
				case "/services/runtime-config":
					// The second request proves the first serialized refresh completed.
					if reads.Add(1) == 2 {
						close(processed)
					}
					_, _ = w.Write(body)
				case "/services/runtime-secrets/resolve":
					resolves.Add(1)
					var request struct {
						Binding control.CurrentDiscordCredentialBinding `json:"current_discord_binding"`
					}
					if err := json.NewDecoder(r.Body).Decode(&request); err != nil || request.Binding.StreamID != "running-A" {
						t.Error("local execution resolved an unrelated credential")
					}
					_ = json.NewEncoder(w).Encode(map[string]any{"value": "synthetic-active-token"})
				default:
					t.Error("unexpected runtime endpoint", r.URL.Path)
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			defer panel.Close()
			client := control.Client{Config: control.Config{
				ControlPanelURL: panel.URL, Token: "test-only", ServiceID: "own", ServiceName: "own", ServicePublicURL: "https://bot.example.com",
			}}
			voice := &ownerRevocationGateway{}
			manager := jobs.NewManager(voice)
			if err := reconcileRuntimeGateway(t.Context(), client, manager, initial); err != nil {
				t.Fatal(err)
			}
			if err := manager.Start(discord.VoiceJob{StreamID: "running-A", GuildID: "100000000000000001", VoiceChannelID: "100000000000000002", JobGeneration: 1}); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = manager.Stop("running-A") })
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
				t.Fatal("runtime refresh did not process the successful response")
			}
			cancel()
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Fatal("runtime refresh loop did not stop")
			}
			if got := voice.disconnects.Load() > 0; got != tc.disconnect {
				t.Fatalf("Gateway disconnected=%t, expected=%t", got, tc.disconnect)
			}
			if voice.selected.Load() == tc.disconnect || voice.joined.Load() == tc.disconnect {
				t.Fatal("Gateway or VC retained the wrong execution authority")
			}
			if resolves.Load() != 1 || voice.joins.Load() != 1 || manager.CurrentStreamID() != "running-A" {
				t.Fatal("refresh replaced A or acquired another execution credential")
			}
			epoch := initial.Assignments[0].AssignedAt.UTC().Format(time.RFC3339Nano)
			profile, cached := manager.GatewayProfileForAssignments(map[string]string{"running-A": epoch}, nil, false)
			if tc.disconnect {
				if cached || profile != "" {
					t.Fatal("revoked execution retained its cached profile authority")
				}
			} else if !cached || profile != initial.CurrentDiscordBindings.Bindings[0].ProfileID {
				t.Fatal("waiting B changes removed A's current profile authority")
			}
		})
	}
}
