package httpapi

import (
	"github.com/example/autostream-discord-bot/internal/discord"
	"github.com/example/autostream-discord-bot/internal/jobs"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestStopJobTreatsNoActiveTargetAsAlreadyStopped(t *testing.T) {
	handler := NewServer("discord_bot", jobs.NewManager(&discord.NoopClient{}), TokenVerifier{PlainToken: "service-token"})
	req := httptest.NewRequest(http.MethodPost, "/jobs/stream-01/stop", nil)
	req.Header.Set("Authorization", "Bearer service-token")
	res := httptest.NewRecorder()
	handler.ServeHTTP(res, req)
	if res.Code != http.StatusAccepted || !strings.Contains(res.Body.String(), "already_stopped") {
		t.Fatalf("idempotent stop status=%d body=%s", res.Code, res.Body.String())
	}
}

func TestStopJobCallbackDoesNotCancelOutboundAutoStopParentContext(t *testing.T) {
	const token = "service-token"
	voice := &httpFakeVoice{}
	manager := jobs.NewManager(voice)
	stopper := &httpPanelStopCallbackStopper{
		token:       token,
		started:     make(chan string, 1),
		callback:    make(chan string, 1),
		ready:       make(chan string, 1),
		release:     make(chan struct{}),
		returned:    make(chan string, 1),
		ctxCanceled: make(chan string, 1),
	}
	manager.SetStreamStopper(stopper)
	handler := NewServer("discord_bot", manager, TokenVerifier{PlainToken: token})
	stopper.handler = handler
	if err := manager.Start(discord.VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01"}); err != nil {
		t.Fatal(err)
	}
	manager.ParticipantChanged(discord.ParticipantEvent{StreamID: "stream-01", UserID: "user-01", Present: true})
	manager.ParticipantChanged(discord.ParticipantEvent{StreamID: "stream-01", UserID: "user-01", Present: false})

	select {
	case got := <-stopper.started:
		if got != "stream-01" {
			t.Fatalf("outbound auto-stop stream = %q, want stream-01", got)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("timed out waiting for outbound auto-stop request")
	}
	select {
	case got := <-stopper.callback:
		if got != "stream-01" {
			t.Fatalf("nested HTTP stop stream = %q, want stream-01", got)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for authenticated /jobs/{id}/stop callback")
	}
	if got := manager.CurrentStreamID(); got != "" {
		t.Fatalf("HTTP stop callback did not clear current stream: %q", got)
	}
	select {
	case got := <-stopper.ready:
		if got != "stream-01" {
			t.Fatalf("outbound auto-stop readiness stream = %q, want stream-01", got)
		}
	case got := <-stopper.ctxCanceled:
		t.Fatalf("HTTP stop callback canceled its own parent auto-stop context for %q", got)
	case <-time.After(time.Second):
		t.Fatal("outbound auto-stop parent context did not remain live after HTTP callback")
	}

	close(stopper.release)
	select {
	case got := <-stopper.returned:
		if got != "stream-01" {
			t.Fatalf("outbound auto-stop return stream = %q, want stream-01", got)
		}
	case <-time.After(time.Second):
		t.Fatal("outbound auto-stop did not finish after nested HTTP callback")
	}
	select {
	case got := <-stopper.ctxCanceled:
		t.Fatalf("HTTP stop callback canceled its own parent auto-stop context for %q", got)
	case <-time.After(50 * time.Millisecond):
	}
}
