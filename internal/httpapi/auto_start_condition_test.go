package httpapi

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/example/autostream-discord-bot/internal/discord"
	"github.com/example/autostream-discord-bot/internal/jobs"
)

type conditionVoice struct {
	httpFakeVoice
	known, present bool
}

func (v *conditionVoice) AutoStartVoicePresence(discord.AutoStartVoiceTarget) (bool, bool) {
	return v.present, v.known
}

func TestMSRConditionHTTPIsAuthenticatedBoundedAndPure(t *testing.T) {
	voice := &conditionVoice{known: true, present: true}
	manager := jobs.NewManager(voice)
	manager.SetStreamVoiceDefaults(map[string]jobs.VoiceDefaults{"stream": {
		AssignmentEpoch: "epoch", GuildID: "guild", VoiceChannelID: "voice", AutoStartEnabled: true,
		AutoStartHoldRequired: true, AutoStartHoldKnown: true, AutoStartHold: true, AutoStartHoldRevision: 4,
		ExpectedStreamRevision: "stream-r", ExpectedSettingsRevision: "settings-r", ExpectedRunID: "run",
	}})
	server := httptest.NewServer(NewServer("discord_bot", manager, TokenVerifier{PlainToken: "synthetic-test-only"}))
	defer server.Close()
	for _, test := range []struct {
		name, query, token string
		status             int
	}{
		{"current", "hold_revision=4&assignment_epoch=epoch", "synthetic-test-only", http.StatusOK},
		{"unauthorized", "hold_revision=4&assignment_epoch=epoch", "wrong", http.StatusUnauthorized},
		{"old revision", "hold_revision=3&assignment_epoch=epoch", "synthetic-test-only", http.StatusConflict},
		{"old epoch", "hold_revision=4&assignment_epoch=old", "synthetic-test-only", http.StatusConflict},
		{"missing", "hold_revision=4", "synthetic-test-only", http.StatusBadRequest},
		{"duplicate", "hold_revision=4&hold_revision=4&assignment_epoch=epoch", "synthetic-test-only", http.StatusBadRequest},
		{"extra", "hold_revision=4&assignment_epoch=epoch&current=1", "synthetic-test-only", http.StatusBadRequest},
	} {
		t.Run(test.name, func(t *testing.T) {
			req, _ := http.NewRequest(http.MethodGet, server.URL+"/streams/stream/auto-start-condition?"+test.query, nil)
			req.Header.Set("Authorization", "Bearer "+test.token)
			response, err := server.Client().Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			if response.StatusCode != test.status {
				t.Fatalf("status=%d want=%d", response.StatusCode, test.status)
			}
			if test.status == http.StatusOK {
				var condition jobs.AutoStartCondition
				if err := json.NewDecoder(response.Body).Decode(&condition); err != nil {
					t.Fatal(err)
				}
				if !condition.Known || !condition.Present || condition.HoldRevision != 4 || condition.AssignmentEpoch != "epoch" {
					t.Fatal("current held target condition was substituted or guessed")
				}
			} else {
				_, _ = io.Copy(io.Discard, response.Body)
			}
		})
	}
	manager.SetStreamVoiceDefaults(map[string]jobs.VoiceDefaults{"stream": {
		AssignmentEpoch: "epoch", GuildID: "guild", VoiceChannelID: "voice", AutoStartEnabled: true,
		AutoStartHoldRequired: true, AutoStartHoldKnown: false, AutoStartHold: true, AutoStartHoldRevision: 5,
		ExpectedStreamRevision: "stream-r", ExpectedSettingsRevision: "settings-r", ExpectedRunID: "run",
	}})
	for _, test := range []struct {
		query, token string
		status       int
	}{
		{"assignment_epoch=epoch", "synthetic-test-only", http.StatusOK},
		{"assignment_epoch=epoch", "wrong", http.StatusUnauthorized},
		{"assignment_epoch=old", "synthetic-test-only", http.StatusConflict},
		{"", "synthetic-test-only", http.StatusBadRequest},
		{"assignment_epoch=epoch&assignment_epoch=epoch", "synthetic-test-only", http.StatusBadRequest},
		{"assignment_epoch=epoch&hold_revision=5", "synthetic-test-only", http.StatusBadRequest},
	} {
		req, _ := http.NewRequest(http.MethodGet, server.URL+"/streams/stream/auto-start-presence?"+test.query, nil)
		req.Header.Set("Authorization", "Bearer "+test.token)
		response, err := server.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		if response.StatusCode != test.status {
			t.Fatalf("pure presence status=%d want=%d", response.StatusCode, test.status)
		}
		if response.StatusCode == http.StatusOK {
			var presence jobs.AutoStartCondition
			if json.NewDecoder(response.Body).Decode(&presence) != nil || !presence.Known || !presence.Present || presence.HoldRevision != 5 {
				t.Fatal("pending CP Hold was mistaken for unavailable SDK presence or unheld authority")
			}
		}
		response.Body.Close()
	}
	manager.SetAutoStartSourceAvailable(false)
	req, _ := http.NewRequest(http.MethodGet, server.URL+"/streams/stream/auto-start-presence?assignment_epoch=epoch", nil)
	req.Header.Set("Authorization", "Bearer synthetic-test-only")
	response, err := server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	var unavailable jobs.AutoStartCondition
	if json.NewDecoder(response.Body).Decode(&unavailable) != nil || unavailable.Known || unavailable.Present {
		t.Fatal("source failure restored stale cached SDK presence")
	}
	response.Body.Close()
	if voice.joinCalls != 0 || manager.CurrentStreamID() != "" {
		t.Fatal("observation caused a Start or a hold-release effect")
	}
}
