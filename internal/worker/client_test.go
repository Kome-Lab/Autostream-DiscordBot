package worker

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/example/autostream-discord-bot/internal/discord"
	"github.com/example/autostream-discord-bot/internal/jobs"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func TestReporterPublishesParticipantsWithJobGeneration(t *testing.T) {
	var gotAuth string
	var got struct {
		Participants  []participantPayload `json:"participants"`
		JobGeneration uint64               `json:"job_generation"`
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

	reporter := Reporter{Config: Config{Timeout: time.Second}}
	job := discord.VoiceJob{StreamID: "stream-01", JobGeneration: 17, WorkerEventsURL: server.URL, WorkerEventsToken: "secret-token"}
	if err := reporter.ParticipantsChanged(job, []jobs.Participant{{UserID: "user-01", Username: "alice", AvatarURL: "https://cdn.discordapp.com/avatars/user-01/a.png", IsBot: true, Speaking: true}}); err != nil {
		t.Fatal(err)
	}

	if gotAuth != "Bearer secret-token" || got.JobGeneration != 17 || len(got.Participants) != 1 || got.Participants[0].DisplayName != "alice" || got.Participants[0].AvatarURL == "" || !got.Participants[0].IsBot || !got.Participants[0].Speaking {
		t.Fatalf("unexpected publish request: auth=%q body=%#v", gotAuth, got)
	}
}

func TestReporterPublishesActiveSpeaker(t *testing.T) {
	var got struct {
		UserID      string `json:"user_id"`
		DisplayName string `json:"display_name"`
		Speaking    bool   `json:"speaking"`
	}
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

	reporter := Reporter{Config: Config{Timeout: time.Second}}
	job := discord.VoiceJob{StreamID: "stream-01", WorkerEventsURL: server.URL, WorkerEventsToken: "secret-token"}
	if err := reporter.ActiveSpeakerChanged(job, "user-01", "alice"); err != nil {
		t.Fatal(err)
	}

	if got.UserID != "user-01" || got.DisplayName != "alice" || !got.Speaking {
		t.Fatalf("unexpected active speaker payload: %#v", got)
	}
}

func TestReporterPublishesSpeakerStop(t *testing.T) {
	var got struct {
		UserID   string `json:"user_id"`
		Speaking bool   `json:"speaking"`
	}
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

	reporter := Reporter{Config: Config{Timeout: time.Second}}
	job := discord.VoiceJob{StreamID: "stream-01", WorkerEventsURL: server.URL, WorkerEventsToken: "secret-token"}
	if err := reporter.ActiveSpeakerStateChanged(job, "user-01", "alice", false); err != nil {
		t.Fatal(err)
	}
	if got.UserID != "user-01" || got.Speaking {
		t.Fatalf("unexpected speaker stop payload: %#v", got)
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

	reporter := Reporter{Config: Config{Timeout: time.Second}}
	job := discord.VoiceJob{StreamID: "stream-01", WorkerEventsURL: server.URL, WorkerEventsToken: "secret-token"}
	err := reporter.ChatMessageReceived(job, jobs.ChatMessage{
		MessageID:     "msg-01",
		UserID:        "user-01",
		Username:      "alice",
		AvatarURL:     "https://cdn.discordapp.com/avatars/user-01/avatar.png",
		IsBot:         true,
		Content:       "こんにちは",
		TextChannelID: "text-01",
		CreatedAt:     time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatal(err)
	}

	if got.Type != "overlay.discord_chat" || got.Payload["message_id"] != "msg-01" || got.Payload["author_id"] != "user-01" || got.Payload["display_name"] != "alice" || got.Payload["avatar_url"] == "" || got.Payload["is_bot"] != true || got.Payload["content"] != "こんにちは" || got.Payload["text_channel_id"] != "text-01" {
		t.Fatalf("unexpected discord chat payload: %#v", got)
	}
}

func TestReporterErrorDoesNotLeakToken(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "secret-token", http.StatusForbidden)
	}))
	defer server.Close()

	reporter := Reporter{Config: Config{Timeout: time.Second}}
	job := discord.VoiceJob{StreamID: "stream-01", WorkerEventsURL: server.URL, WorkerEventsToken: "secret-token"}
	err := reporter.post(t.Context(), job, "/streams/stream-01/events/participants", map[string]any{})
	if err == nil {
		t.Fatal("expected publish error")
	}
	if strings.Contains(err.Error(), "secret-token") {
		t.Fatalf("token leaked in error: %v", err)
	}
}

func TestReporterClassifiesRetryableHTTPStatusWithoutSecrets(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "secret-token https://worker.example.invalid/private", http.StatusConflict)
	}))
	defer server.Close()

	reporter := Reporter{Config: Config{Timeout: time.Second, RetryDelays: []time.Duration{}}}
	job := discord.VoiceJob{StreamID: "stream-01", WorkerEventsURL: server.URL, WorkerEventsToken: "secret-token"}
	err := reporter.post(context.Background(), job, "/streams/stream-01/events/participants", map[string]any{})
	if err == nil {
		t.Fatal("expected publish error")
	}
	var classified *PublishError
	if !errors.As(err, &classified) {
		t.Fatalf("publish error type = %T, want *PublishError", err)
	}
	if classified.HTTPStatusCode() != http.StatusConflict || classified.ErrorClass() != "http_status" || !classified.RetryablePublish() {
		t.Fatalf("unexpected publish classification: status=%d class=%q retryable=%t", classified.HTTPStatusCode(), classified.ErrorClass(), classified.RetryablePublish())
	}
	for _, secret := range []string{"secret-token", "worker.example.invalid", "private"} {
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("publish error leaked %q: %v", secret, err)
		}
	}
}

func TestReporterRetriesTransientWorkerFailure(t *testing.T) {
	for _, statusCode := range []int{http.StatusRequestTimeout, http.StatusConflict, http.StatusTooManyRequests, http.StatusServiceUnavailable} {
		t.Run(http.StatusText(statusCode), func(t *testing.T) {
			attempts := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				attempts++
				if attempts == 1 {
					http.Error(w, "temporarily unavailable", statusCode)
					return
				}
				w.WriteHeader(http.StatusAccepted)
			}))
			defer server.Close()

			reporter := Reporter{Config: Config{Timeout: time.Second, RetryDelays: []time.Duration{0}}}
			job := discord.VoiceJob{StreamID: "stream-01", WorkerEventsURL: server.URL, WorkerEventsToken: "secret-token"}
			if err := reporter.ParticipantsChanged(job, []jobs.Participant{{UserID: "user-01"}}); err != nil {
				t.Fatal(err)
			}
			if attempts != 2 {
				t.Fatalf("transient publish attempts = %d, want 2", attempts)
			}
		})
	}
}

func TestReporterDoesNotRetryPermanentWorkerFailure(t *testing.T) {
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		http.Error(w, "invalid event", http.StatusBadRequest)
	}))
	defer server.Close()

	reporter := Reporter{Config: Config{Timeout: time.Second, RetryDelays: []time.Duration{0, 0}}}
	job := discord.VoiceJob{StreamID: "stream-01", WorkerEventsURL: server.URL, WorkerEventsToken: "secret-token"}
	err := reporter.post(context.Background(), job, "/streams/stream-01/events/participants", map[string]any{})
	if err == nil {
		t.Fatal("expected permanent publish failure")
	}
	if attempts != 1 {
		t.Fatalf("permanent publish attempts = %d, want 1", attempts)
	}
}

func TestReporterRetriesTransientTransportFailure(t *testing.T) {
	attempts := 0
	reporter := Reporter{
		Config: Config{Timeout: time.Second, RetryDelays: []time.Duration{0}},
		HTTP: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			attempts++
			if attempts == 1 {
				return nil, errors.New("temporary transport failure")
			}
			return &http.Response{StatusCode: http.StatusAccepted, Body: io.NopCloser(strings.NewReader(""))}, nil
		})},
	}
	job := discord.VoiceJob{StreamID: "stream-01", WorkerEventsURL: "https://worker.example.com", WorkerEventsToken: "secret-token"}
	if err := reporter.ParticipantsChanged(job, []jobs.Participant{{UserID: "user-01"}}); err != nil {
		t.Fatal(err)
	}
	if attempts != 2 {
		t.Fatalf("transport publish attempts = %d, want 2", attempts)
	}
}

func TestReporterStopsAfterConfiguredRetryBudget(t *testing.T) {
	attempts := 0
	reporter := Reporter{
		Config: Config{Timeout: time.Second, RetryDelays: []time.Duration{0, 0}},
		HTTP: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			attempts++
			return nil, errors.New("temporary transport failure")
		})},
	}
	job := discord.VoiceJob{StreamID: "stream-01", WorkerEventsURL: "https://worker.example.com", WorkerEventsToken: "secret-token"}
	if err := reporter.ParticipantsChanged(job, []jobs.Participant{{UserID: "user-01"}}); err == nil {
		t.Fatal("expected bounded publish failure")
	}
	if attempts != 3 {
		t.Fatalf("publish attempts = %d, want initial attempt plus two retries", attempts)
	}
}

func TestReporterHonorsCanceledParentContext(t *testing.T) {
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	reporter := Reporter{Config: Config{Timeout: time.Second, RetryDelays: []time.Duration{0, 0}}}
	job := discord.VoiceJob{StreamID: "stream-01", WorkerEventsURL: server.URL, WorkerEventsToken: "secret-token"}
	if err := reporter.post(ctx, job, "/streams/stream-01/events/participants", map[string]any{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("publish error = %v, want context canceled", err)
	}
	if attempts != 0 {
		t.Fatalf("canceled publish reached worker %d times", attempts)
	}
}

func TestReporterRetryStaysWithinConfiguredTotalTimeout(t *testing.T) {
	attempts := 0
	reporter := Reporter{
		Config: Config{Timeout: 40 * time.Millisecond, RetryDelays: []time.Duration{time.Second, time.Second}},
		HTTP: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			attempts++
			return nil, errors.New("temporary transport failure")
		})},
	}
	job := discord.VoiceJob{StreamID: "stream-01", WorkerEventsURL: "https://worker.example.com", WorkerEventsToken: "secret-token"}
	startedAt := time.Now()
	err := reporter.ParticipantsChanged(job, []jobs.Participant{{UserID: "user-01"}})
	if err == nil {
		t.Fatal("expected publish failure")
	}
	if elapsed := time.Since(startedAt); elapsed >= 200*time.Millisecond {
		t.Fatalf("retry exceeded configured total timeout: %s", elapsed)
	}
	if attempts != 1 {
		t.Fatalf("publish attempts = %d, want 1 before total timeout", attempts)
	}
}

func TestReporterNoopsWithoutJobWorkerEventsEndpoint(t *testing.T) {
	reporter := Reporter{Config: Config{Timeout: time.Second}}
	if err := reporter.ParticipantsChanged(discord.VoiceJob{StreamID: "stream-01"}, []jobs.Participant{{UserID: "user-01"}}); err != nil {
		t.Fatal(err)
	}
}
