package audioforward

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestClientForwardsOpusPackets(t *testing.T) {
	var gotAuth string
	var gotPath string
	var gotBody ingestRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotPath = r.URL.Path
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Fatal(err)
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	client := Client{Config: Config{Token: "token"}}
	err := client.ForwardOpus(context.Background(), server.URL, "stream-01", "discord-bot-01", "", []OpusPacket{{SSRC: 10, Sequence: 2, Timestamp: 960, Opus: []byte{1, 2, 3}}})
	if err != nil {
		t.Fatal(err)
	}
	if gotAuth != "Bearer token" || gotPath != "/streams/stream-01/audio/opus" {
		t.Fatalf("unexpected request auth=%q path=%q", gotAuth, gotPath)
	}
	if gotBody.StreamID != "stream-01" || len(gotBody.Packets) != 1 || gotBody.Packets[0].OpusBase64 == "" {
		t.Fatalf("unexpected body: %#v", gotBody)
	}
}

func TestClientRejectsUnsafeURL(t *testing.T) {
	client := Client{Config: Config{Token: "token"}}
	err := client.ForwardOpus(context.Background(), "file:///tmp/audio", "stream-01", "bot", "", []OpusPacket{{Opus: []byte{1}}})
	if err == nil {
		t.Fatal("expected unsafe URL to be rejected")
	}
}

func TestClientDoesNotFollowRedirectsWithBearerToken(t *testing.T) {
	var redirectedAuth string
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		redirectedAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL+"/capture", http.StatusFound)
	}))
	defer server.Close()

	client := Client{Config: Config{Token: "token"}}
	err := client.ForwardOpus(context.Background(), server.URL, "stream-01", "bot", "", []OpusPacket{{Opus: []byte{1}}})
	if err == nil {
		t.Fatal("expected redirect response to fail")
	}
	if redirectedAuth != "" {
		t.Fatalf("authorization header followed redirect: %q", redirectedAuth)
	}
}

func TestClientRejectsRemoteHTTPURL(t *testing.T) {
	client := Client{Config: Config{Token: "token"}}
	err := client.ForwardOpus(context.Background(), "http://encoder.example.com", "stream-01", "bot", "", []OpusPacket{{Opus: []byte{1}}})
	if err == nil {
		t.Fatal("expected remote http URL to be rejected")
	}
	if !strings.Contains(err.Error(), "https for remote hosts") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestClientAllowsLocalHTTPURL(t *testing.T) {
	endpoint, err := audioEndpoint("http://host.docker.internal:18084", "stream-01")
	if err != nil {
		t.Fatalf("expected local http URL to be allowed: %v", err)
	}
	if !strings.Contains(endpoint, "/streams/stream-01/audio/opus") {
		t.Fatalf("unexpected endpoint: %s", endpoint)
	}
}

func TestClientRejectsURLQueryOrFragment(t *testing.T) {
	client := Client{Config: Config{Token: "token"}}
	err := client.ForwardOpus(context.Background(), "https://encoder.example.com?token=bad", "stream-01", "bot", "", []OpusPacket{{Opus: []byte{1}}})
	if err == nil {
		t.Fatal("expected URL with query to be rejected")
	}
	if !strings.Contains(err.Error(), "query or fragment") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestClientUsesTokenOverride(t *testing.T) {
	var gotAuth string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	client := Client{Config: Config{Token: "env-token"}}
	err := client.ForwardOpus(context.Background(), server.URL, "stream-01", "discord-bot-01", "job-token", []OpusPacket{{SSRC: 10, Sequence: 2, Timestamp: 960, Opus: []byte{1, 2, 3}}})
	if err != nil {
		t.Fatal(err)
	}
	if gotAuth != "Bearer job-token" {
		t.Fatalf("expected override token, got %q", gotAuth)
	}
}

func TestClientRetriesTransientForwardFailure(t *testing.T) {
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts == 1 {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	client := Client{
		Config: Config{Token: "token", RetryMax: 2, RetryBaseDelay: time.Second},
		Sleep:  func(context.Context, time.Duration) error { return nil },
	}
	err := client.ForwardOpus(context.Background(), server.URL, "stream-01", "discord-bot-01", "", []OpusPacket{{SSRC: 10, Sequence: 2, Timestamp: 960, Opus: []byte{1, 2, 3}}})
	if err != nil {
		t.Fatal(err)
	}
	if attempts != 2 {
		t.Fatalf("expected retry, got %d attempts", attempts)
	}
}

func TestClientDoesNotRetryUnauthorizedForwardFailure(t *testing.T) {
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer server.Close()

	client := Client{
		Config: Config{Token: "token", RetryMax: 3, RetryBaseDelay: time.Second},
		Sleep:  func(context.Context, time.Duration) error { return nil },
	}
	err := client.ForwardOpus(context.Background(), server.URL, "stream-01", "discord-bot-01", "", []OpusPacket{{SSRC: 10, Sequence: 2, Timestamp: 960, Opus: []byte{1, 2, 3}}})
	if err == nil {
		t.Fatal("expected unauthorized error")
	}
	if attempts != 1 {
		t.Fatalf("expected no retry for 401, got %d attempts", attempts)
	}
}
