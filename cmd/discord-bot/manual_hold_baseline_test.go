package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/example/autostream-discord-bot/internal/control"
	"github.com/example/autostream-discord-bot/internal/discord"
	"github.com/example/autostream-discord-bot/internal/jobs"
)

// This test uses only APIs present before IU-WS2-10, so it also demonstrates
// the baseline defect: the old waiting DTO discarded the CP hold fields.
func TestMSRBotWaitingMonitorHoldPublicPath(t *testing.T) {
	raw, err := json.Marshal(waitingMonitorFixture())
	if err != nil {
		t.Fatal(err)
	}
	var wire map[string]any
	if err = json.Unmarshal(raw, &wire); err != nil {
		t.Fatal(err)
	}
	monitor := wire["discord_waiting_monitor"].(map[string]any)
	bindings := monitor["bindings"].([]any)
	for _, value := range bindings {
		binding := value.(map[string]any)
		binding["auto_start_hold_required"], binding["auto_start_hold_known"], binding["auto_start_hold"] = true, true, true
		binding["auto_start_hold_revision"] = 8
		binding["expected_stream_revision"], binding["expected_settings_revision"], binding["expected_run_id"] = "stream-rev", "settings-rev", "run"
	}
	raw, _ = json.Marshal(wire)
	var cfg control.RuntimeConfig
	if err = json.Unmarshal(raw, &cfg); err != nil {
		t.Fatal(err)
	}
	voice := &waitingMonitorVoice{present: map[string]bool{"100000000000000002": true}}
	manager := jobs.NewManager(voice)
	sent := make(chan string, 2)
	panel := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sent <- r.URL.Path
		w.WriteHeader(http.StatusAccepted)
	}))
	defer panel.Close()
	manager.SetStreamStarter(controlStreamStarter{client: control.Client{Config: control.Config{ControlPanelURL: panel.URL, Token: "synthetic-test-only", ServiceID: "own", ServiceName: "own", ServicePublicURL: "https://bot.example.invalid"}}})
	defaults := streamDiscordDefaultsFromRuntimeConfig(cfg)
	manager.SetStreamVoiceDefaults(defaults)
	selected := defaults["waiting-A"]
	manager.VoiceUserJoined(discord.VoiceJoinEvent{GuildID: selected.GuildID, VoiceChannelID: selected.VoiceChannelID, UserID: "human",
		Authority: &discord.AutoStartAuthority{StreamID: "waiting-A", AssignmentEpoch: selected.AssignmentEpoch}})
	select {
	case <-sent:
		t.Fatal("CP-owned durable hold was discarded by the waiting-monitor product path")
	case <-time.After(50 * time.Millisecond):
	}
}
