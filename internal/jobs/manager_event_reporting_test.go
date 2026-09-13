package jobs

import (
	"bytes"
	"errors"
	"github.com/example/autostream-discord-bot/internal/discord"
	"log"
	"strings"
	"testing"
	"time"
)

func TestAutoStopDiagnosticRecordsEmptyParticipantObservation(t *testing.T) {
	previousWriter := log.Writer()
	previousFlags := log.Flags()
	var output bytes.Buffer
	log.SetOutput(&output)
	log.SetFlags(0)
	defer func() {
		log.SetOutput(previousWriter)
		log.SetFlags(previousFlags)
	}()

	manager := NewManager(&fakeVoice{})
	manager.autoStopDelay = 0
	stopper := &fakeStreamStopper{ch: make(chan string, 1)}
	manager.SetStreamStopper(stopper)
	job := discord.VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01"}
	if err := manager.Start(job); err != nil {
		t.Fatal(err)
	}
	manager.ParticipantChanged(discord.ParticipantEvent{StreamID: job.StreamID, UserID: "user-01", Present: true})
	manager.ParticipantChanged(discord.ParticipantEvent{StreamID: job.StreamID, UserID: "user-01", Present: false})

	select {
	case got := <-stopper.ch:
		if got != job.StreamID {
			t.Fatalf("auto-stop stream = %q, want %q", got, job.StreamID)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for auto-stop request")
	}

	got := output.String()
	for _, expected := range []string{
		"event=scheduled",
		"event=requested",
		"reason=empty_participants",
		"source=voice_event",
		"participant_count=0",
		"snapshot_revision=0",
		"participant_state_revision=2",
		"reconnect_generation=1",
		"auto_stop_generation=3",
		"attempt=1",
		"delay_ms=0",
	} {
		if !strings.Contains(got, expected) {
			t.Fatalf("auto-stop diagnostic missing %q: %q", expected, got)
		}
	}
	if strings.Contains(got, "user-01") {
		t.Fatalf("auto-stop diagnostic leaked a participant ID: %q", got)
	}
}

func TestManagerRecordsWorkerEventPublishFailures(t *testing.T) {
	reporter := &fakeReporter{err: errors.New("worker unavailable")}
	manager := NewManagerWithReporter(&fakeVoice{}, reporter)
	if err := manager.Start(discord.VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01"}); err != nil {
		t.Fatal(err)
	}
	manager.ParticipantChanged(discord.ParticipantEvent{StreamID: "stream-01", UserID: "user-01", Username: "alice", Present: true})
	if err := manager.SetActiveSpeaker("stream-01", "user-01"); err != nil {
		t.Fatal(err)
	}
	status := manager.Status()
	if status.Metrics["discord.worker_event_publish_failures_total"] != 3 {
		t.Fatalf("expected worker publish failures to be counted, got %#v", status.Metrics)
	}
}

func TestActiveSpeakerMustBeParticipant(t *testing.T) {
	manager := NewManager(&fakeVoice{})
	if err := manager.Start(discord.VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01"}); err != nil {
		t.Fatal(err)
	}
	if err := manager.SetActiveSpeaker("stream-01", "missing"); err == nil {
		t.Fatal("expected missing participant to be rejected")
	}
}

func TestChatMessageReceivedPublishesOnlyCurrentTextChannel(t *testing.T) {
	reporter := &fakeReporter{}
	manager := NewManagerWithReporter(&fakeVoice{}, reporter)
	if err := manager.Start(discord.VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01", TextChannelID: "text-01"}); err != nil {
		t.Fatal(err)
	}

	manager.ChatMessageReceived(discord.ChatMessageEvent{
		StreamID:      "stream-01",
		GuildID:       "guild-01",
		TextChannelID: "text-other",
		MessageID:     "msg-ignored",
		UserID:        "user-01",
		Content:       "wrong channel",
	})
	if reporter.chatMessage.MessageID != "" {
		t.Fatalf("wrong text channel message should be ignored: %#v", reporter.chatMessage)
	}

	manager.ChatMessageReceived(discord.ChatMessageEvent{
		StreamID:      "stream-01",
		GuildID:       "guild-01",
		TextChannelID: "text-01",
		MessageID:     "msg-01",
		UserID:        "user-01",
		Username:      "alice",
		AvatarURL:     "https://cdn.discordapp.com/avatars/user-01/avatar.png",
		IsBot:         true,
		Content:       " こんにちは ",
		CreatedAt:     time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC),
	})
	if reporter.chatStreamID != "stream-01" || reporter.chatMessage.MessageID != "msg-01" || reporter.chatMessage.Content != "こんにちは" || reporter.chatMessage.Username != "alice" || reporter.chatMessage.AvatarURL == "" || !reporter.chatMessage.IsBot {
		t.Fatalf("chat message was not published: stream=%q message=%#v", reporter.chatStreamID, reporter.chatMessage)
	}
}

func TestChatMessageReceivedBoundsOverlayPayload(t *testing.T) {
	reporter := &fakeReporter{}
	manager := NewManagerWithReporter(&fakeVoice{}, reporter)
	if err := manager.Start(discord.VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01", TextChannelID: "text-01"}); err != nil {
		t.Fatal(err)
	}

	manager.ChatMessageReceived(discord.ChatMessageEvent{
		StreamID:      "stream-01",
		GuildID:       "guild-01",
		TextChannelID: "text-01",
		MessageID:     strings.Repeat("m", 200),
		UserID:        strings.Repeat("u", 200),
		Username:      strings.Repeat("n", 150),
		AvatarURL:     "https://cdn.discordapp.com/" + strings.Repeat("a", 2100),
		Content:       strings.Repeat("文", 1100),
	})

	message := reporter.chatMessage
	if len([]rune(message.MessageID)) != 128 || len([]rune(message.UserID)) != 128 || len([]rune(message.Username)) != 100 || len([]rune(message.AvatarURL)) != 2048 || len([]rune(message.Content)) != 1000 {
		t.Fatalf("chat payload bounds were not applied: message=%#v", message)
	}
}

func TestWorkerPublishFailureLogIsRateLimitedAndSecretSafe(t *testing.T) {
	previousWriter := log.Writer()
	previousFlags := log.Flags()
	var output bytes.Buffer
	log.SetOutput(&output)
	log.SetFlags(0)
	defer func() {
		log.SetOutput(previousWriter)
		log.SetFlags(previousFlags)
	}()

	reporter := &fakeReporter{err: errors.New("https://worker.example.com secret-token hidden-content")}
	manager := NewManagerWithReporter(&fakeVoice{}, reporter)
	manager.workerFailureLogInterval = time.Minute
	job := discord.VoiceJob{
		StreamID:          "stream-01",
		GuildID:           "guild-01",
		VoiceChannelID:    "voice-01",
		TextChannelID:     "text-01",
		WorkerEventsURL:   "https://worker.example.com",
		WorkerEventsToken: "secret-token",
	}
	if err := manager.Start(job); err != nil {
		t.Fatal(err)
	}
	event := discord.ChatMessageEvent{StreamID: job.StreamID, GuildID: job.GuildID, TextChannelID: job.TextChannelID, MessageID: "message-01", UserID: "user-01", Content: "hidden-content"}
	manager.ChatMessageReceived(event)
	manager.ChatMessageReceived(event)

	got := output.String()
	if strings.Count(got, "event_type=overlay.discord_chat") != 1 || !strings.Contains(got, "stream_id=stream-01") || !strings.Contains(got, "error_class=") {
		t.Fatalf("unexpected worker failure warning: %q", got)
	}
	for _, secret := range []string{"secret-token", "worker.example.com", "hidden-content"} {
		if strings.Contains(got, secret) {
			t.Fatalf("worker failure warning leaked %q: %q", secret, got)
		}
	}
}

func TestWorkerPublishFailureLogIncludesSafeHTTPClassification(t *testing.T) {
	previousWriter := log.Writer()
	previousFlags := log.Flags()
	var output bytes.Buffer
	log.SetOutput(&output)
	log.SetFlags(0)
	defer func() {
		log.SetOutput(previousWriter)
		log.SetFlags(previousFlags)
	}()

	reporter := &fakeReporter{err: retryableWorkerPublishError{status: 409, class: "http_status"}}
	manager := NewManagerWithReporter(&fakeVoice{}, reporter)
	job := discord.VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01", TextChannelID: "text-01"}
	if err := manager.Start(job); err != nil {
		t.Fatal(err)
	}
	manager.ChatMessageReceived(discord.ChatMessageEvent{StreamID: job.StreamID, GuildID: job.GuildID, TextChannelID: job.TextChannelID, MessageID: "message-01", UserID: "user-01", Content: "hello"})
	if err := manager.Stop(job.StreamID); err != nil {
		t.Fatal(err)
	}
	got := output.String()
	for _, expected := range []string{"error_class=http_status", "http_status=409", "retryable=true", "retry_count=1"} {
		if !strings.Contains(got, expected) {
			t.Fatalf("worker failure log missing %q: %q", expected, got)
		}
	}
}
