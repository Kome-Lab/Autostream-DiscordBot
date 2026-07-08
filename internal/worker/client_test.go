package worker

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/example/autostream-discord-bot/internal/jobs"
)

func TestReporterPublishesParticipants(t *testing.T) {
	var gotAuth string
	var got struct {
		Participants []participantPayload `json:"participants"`
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/streams/stream-01/events/participants" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		gotAuth = r.Header.Get("Authorization")
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Fatal(err)
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	reporter := Reporter{Config: Config{URL: server.URL, Token: "secret-token", Timeout: time.Second}}
	if err := reporter.ParticipantsChanged("stream-01", []jobs.Participant{{UserID: "user-01", Username: "alice"}}); err != nil {
		t.Fatal(err)
	}

	if gotAuth != "Bearer secret-token" || len(got.Participants) != 1 || got.Participants[0].DisplayName != "alice" {
		t.Fatalf("unexpected publish request: auth=%q body=%#v", gotAuth, got)
	}
}

func TestReporterPublishesActiveSpeaker(t *testing.T) {
	var got map[string]string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/streams/stream-01/events/active-speaker" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Fatal(err)
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	reporter := Reporter{Config: Config{URL: server.URL, Token: "secret-token", Timeout: time.Second}}
	if err := reporter.ActiveSpeakerChanged("stream-01", "user-01", "alice"); err != nil {
		t.Fatal(err)
	}

	if got["user_id"] != "user-01" || got["display_name"] != "alice" {
		t.Fatalf("unexpected active speaker payload: %#v", got)
	}
}

func TestReporterPublishesDiscordChatOverlay(t *testing.T) {
	var got struct {
		Type    string         `json:"type"`
		Payload map[string]any `json:"payload"`
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/streams/stream-01/events/overlay" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Fatal(err)
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	reporter := Reporter{Config: Config{URL: server.URL, Token: "secret-token", Timeout: time.Second}}
	err := reporter.ChatMessageReceived("stream-01", jobs.ChatMessage{
		MessageID:     "msg-01",
		UserID:        "user-01",
		Username:      "alice",
		Content:       "こんにちは",
		TextChannelID: "text-01",
		CreatedAt:     time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatal(err)
	}

	if got.Type != "overlay.discord_chat" || got.Payload["message_id"] != "msg-01" || got.Payload["user_id"] != "user-01" || got.Payload["display_name"] != "alice" || got.Payload["text"] != "こんにちは" || got.Payload["text_channel_id"] != "text-01" {
		t.Fatalf("unexpected discord chat payload: %#v", got)
	}
}

func TestReporterErrorDoesNotLeakToken(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "secret-token", http.StatusForbidden)
	}))
	defer server.Close()

	reporter := Reporter{Config: Config{URL: server.URL, Token: "secret-token", Timeout: time.Second}}
	err := reporter.post(t.Context(), "/streams/stream-01/events/participants", map[string]any{})
	if err == nil {
		t.Fatal("expected publish error")
	}
	if strings.Contains(err.Error(), "secret-token") {
		t.Fatalf("token leaked in error: %v", err)
	}
}
