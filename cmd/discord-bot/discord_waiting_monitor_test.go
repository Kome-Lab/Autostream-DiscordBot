package main

import (
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

type waitingMonitorVoice struct {
	discord.NoopClient
	selected bool
	token    string
	sink     *jobs.Manager
	present  map[string]bool
}

func (v *waitingMonitorVoice) ReconcileGateway(cfg discord.Config, selected bool) error {
	v.selected, v.token = selected, cfg.BotToken
	return nil
}

func (v *waitingMonitorVoice) SetAutoStartVoiceTargets(targets []discord.AutoStartVoiceTarget) {
	if v.sink == nil {
		return
	}
	for _, target := range targets {
		if v.present[target.VoiceChannelID] {
			v.sink.VoiceUserJoined(discord.VoiceJoinEvent{
				ExistingPresence: true, GuildID: target.GuildID, VoiceChannelID: target.VoiceChannelID, UserID: "human",
				Authority: &discord.AutoStartAuthority{StreamID: target.StreamID, AssignmentEpoch: target.AssignmentEpoch},
			})
		}
	}
}

func (v *waitingMonitorVoice) AutoStartVoicePresence(target discord.AutoStartVoiceTarget) (bool, bool) {
	return v.present[target.VoiceChannelID], true
}

func waitingMonitorFixture() control.RuntimeConfig {
	binding := func(stream, profile, voice string) control.DiscordWaitingMonitorBinding {
		return control.DiscordWaitingMonitorBinding{
			DiscordWaitingCredentialBinding: control.DiscordWaitingCredentialBinding{
				StreamID: stream, ServiceID: "own", ServiceType: control.ServiceType, SelectionEpoch: time.Unix(100, 0).UTC(),
				ProfileID: profile, ProfileRevision: "profile-revision", ProfileOwnerServiceID: "own",
				SecretName: "discord_bot_token_shared", Fingerprint: "fingerprint", Ready: true,
			},
			GuildID: "100000000000000001", VoiceChannelID: voice, TargetRevision: 7, AutoStartTrigger: "discord_voice_join",
		}
	}
	return control.RuntimeConfig{
		Service: control.RegisteredService{ServiceID: "own", ServiceType: control.ServiceType},
		Assignments: []control.StreamServiceAssignment{},
		CurrentDiscordBindings: &control.CurrentDiscordBindings{Version: 1, Bindings: []control.CurrentDiscordCredentialBinding{}},
		DiscordWaitingMonitor: &control.DiscordWaitingMonitor{Version: 1, Bindings: []control.DiscordWaitingMonitorBinding{
			binding("waiting-A", "profile-A", "100000000000000002"),
			binding("waiting-B", "profile-B", "100000000000000003"),
		}},
	}
}

func currentWaitingMonitorFixture() control.RuntimeConfig {
	cfg := waitingMonitorFixture()
	epoch := time.Unix(200, 0).UTC()
	cfg.Service.CurrentStreamID = "running-A"
	cfg.DiscordWaitingMonitor.ExecutionActive = true
	cfg.Assignments = []control.StreamServiceAssignment{{StreamID: "running-A", ServiceID: "own", ServiceType: control.ServiceType, AssignmentRole: "primary", AssignedAt: epoch}}
	cfg.CurrentDiscordBindings = &control.CurrentDiscordBindings{Version: 1, Bindings: []control.CurrentDiscordCredentialBinding{{
		StreamID: "running-A", ServiceID: "own", ServiceType: control.ServiceType, AssignmentRole: "primary", AssignmentEpoch: epoch,
		ProfileID: "active-profile", ProfileOwnerServiceID: "own", SecretName: "discord_bot_token_shared", Fingerprint: "fingerprint", Ready: true,
	}}}
	cfg.Profiles = map[string][]control.RuntimeProfile{"discord_config": {{
		ID: "active-profile", Kind: "discord_config",
		Config: map[string]any{"service_id": "own", "bot_token_secret_name": "discord_bot_token_shared", "bot_token_fingerprint": "fingerprint"},
	}}}
	return cfg
}

func TestDiscordWaitingMonitorSameCredentialMultipleVCsAndCachedGateway(t *testing.T) {
	cfg := waitingMonitorFixture()
	defaults := streamDiscordDefaultsFromRuntimeConfig(cfg)
	if len(defaults) != 2 || defaults["waiting-A"].VoiceChannelID == defaults["waiting-B"].VoiceChannelID {
		t.Fatal("same credential across different profiles/VCs did not monitor both frames")
	}
	if len(cfg.Assignments) != 0 || cfg.Service.CurrentStreamID != "" {
		t.Fatal("waiting monitoring fabricated execution authority")
	}
	var calls atomic.Int32
	panel := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		var body map[string]json.RawMessage
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
			return
		}
		if string(body["discord_waiting_binding_version"]) != "1" || len(body["current_discord_binding"]) != 0 || len(body["stream_id"]) != 0 {
			t.Error("Gateway monitoring borrowed execution authority")
		}
		if calls.Load() > 1 {
			w.WriteHeader(409)
			_, _ = w.Write([]byte(`{"code":"runtime_secret_lease_active"}`))
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"value": "synthetic-monitor-token", "expires_in_sec": 60})
	}))
	defer panel.Close()
	client := control.Client{Config: control.Config{ControlPanelURL: panel.URL, Token: "test-only", ServiceID: "own", ServiceName: "own", ServicePublicURL: "https://bot.example.com"}}
	voice := &waitingMonitorVoice{}
	manager := jobs.NewManager(voice)
	for range 3 {
		if err := reconcileRuntimeGateway(t.Context(), client, manager, cfg); err != nil {
			t.Fatal(err)
		}
	}
	if calls.Load() != 1 || !voice.selected || voice.token != "synthetic-monitor-token" {
		t.Fatal("unchanged waiting profiles reacquired lease or lost Gateway")
	}
	active := currentWaitingMonitorFixture()
	active.DiscordWaitingMonitor.Bindings[1].SecretName = "discord_bot_token_different"
	active.DiscordWaitingMonitor.Bindings[1].Fingerprint = "different-fingerprint"
	for range 3 {
		if err := reconcileRuntimeGateway(t.Context(), client, manager, active); err != nil {
			t.Fatal(err)
		}
	}
	if calls.Load() != 1 || !voice.selected || voice.token != "synthetic-monitor-token" {
		t.Fatal("adding B with another credential displaced current A")
	}
	if defaults := streamDiscordDefaultsFromRuntimeConfig(active); len(defaults) != 1 || defaults["waiting-A"].VoiceChannelID == "" {
		t.Fatal("waiting candidates did not preserve active credential grouping")
	}
}

func TestDiscordWaitingMonitorRejectsUnsupportedAndAmbiguousAuthority(t *testing.T) {
	tests := []struct {
		name string
		mutate func(*control.RuntimeConfig)
	}{
		{"version", func(cfg *control.RuntimeConfig) { cfg.DiscordWaitingMonitor.Version = 2 }},
		{"null bindings", func(cfg *control.RuntimeConfig) { cfg.DiscordWaitingMonitor.Bindings = nil }},
		{"same VC", func(cfg *control.RuntimeConfig) { cfg.DiscordWaitingMonitor.Bindings[1].VoiceChannelID = cfg.DiscordWaitingMonitor.Bindings[0].VoiceChannelID }},
		{"different credentials", func(cfg *control.RuntimeConfig) { cfg.DiscordWaitingMonitor.Bindings[1].SecretName = "discord_bot_token_other" }},
		{"foreign owner", func(cfg *control.RuntimeConfig) { cfg.DiscordWaitingMonitor.Bindings[0].ProfileOwnerServiceID = "foreign" }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := waitingMonitorFixture()
			tc.mutate(&cfg)
			voice := &waitingMonitorVoice{}
			manager := jobs.NewManager(voice)
			if err := reconcileRuntimeGateway(t.Context(), control.Client{}, manager, cfg); err == nil || voice.selected || voice.token != "" {
				t.Fatal("invalid or ambiguous monitoring authority selected a Gateway")
			}
			if len(streamDiscordDefaultsFromRuntimeConfig(cfg)) != 0 {
				t.Fatal("invalid monitoring authority exposed an auto-start target")
			}
		})
	}
}

func TestDiscordWaitingMonitorRechecksExistingPresenceAfterActiveJobStops(t *testing.T) {
	voice := &waitingMonitorVoice{present: map[string]bool{"100000000000000003": true}}
	manager := jobs.NewManager(voice)
	voice.sink = manager
	starter := fakeStreamStarter{started: make(chan string, 1)}
	manager.SetStreamStarter(starter)
	cfg := waitingMonitorFixture()
	if err := manager.Start(discord.VoiceJob{StreamID: "running-A", GuildID: "100000000000000001", VoiceChannelID: "100000000000000002", JobGeneration: 1}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if id := manager.CurrentStreamID(); id != "" {
			_ = manager.Stop(id)
		}
	})
	applyRuntimeConfigToManager(manager, cfg, reconnectPolicyFromEnv())
	select {
	case started := <-starter.started:
		t.Fatal("B consumed an auto-start attempt while A was running", started)
	default:
	}
	if err := manager.Stop("running-A"); err != nil {
		t.Fatal(err)
	}
	// The same target/epoch is refreshed; there is no artificial leave/join.
	applyRuntimeConfigToManager(manager, cfg, reconnectPolicyFromEnv())
	select {
	case started := <-starter.started:
		if started != "waiting-B" {
			t.Fatal("wrong waiting frame started", started)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("capacity release did not recheck the already present human")
	}
}

func TestDiscordWaitingMonitorInactiveRetainedAssignmentCanUseAnotherCredential(t *testing.T) {
	cfg := currentWaitingMonitorFixture()
	cfg.DiscordWaitingMonitor.ExecutionActive = false
	cfg.DiscordWaitingMonitor.Bindings = cfg.DiscordWaitingMonitor.Bindings[1:]
	cfg.DiscordWaitingMonitor.Bindings[0].SecretName = "discord_bot_token_new"
	cfg.DiscordWaitingMonitor.Bindings[0].Fingerprint = "new-fingerprint"
	var calls atomic.Int32
	panel := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		var body struct {
			Binding control.DiscordWaitingCredentialBinding `json:"discord_waiting_binding"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
			return
		}
		if body.Binding.StreamID != "waiting-B" || body.Binding.SecretName != "discord_bot_token_new" {
			t.Error("inactive retained owner won over the sole waiting credential")
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"value": "synthetic-new-idle-token"})
	}))
	defer panel.Close()
	voice := &waitingMonitorVoice{}
	manager := jobs.NewManager(voice)
	client := control.Client{Config: control.Config{ControlPanelURL: panel.URL, Token: "test-only", ServiceID: "own", ServiceName: "own", ServicePublicURL: "https://bot.example.com"}}
	if err := reconcileRuntimeGateway(t.Context(), client, manager, cfg); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 || !voice.selected || voice.token != "synthetic-new-idle-token" || len(streamDiscordDefaultsFromRuntimeConfig(cfg)) != 1 {
		t.Fatal("inactive retained assignment pinned the old credential")
	}
}
