package jobs

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/example/autostream-discord-bot/internal/discord"
)

type fakeVoice struct {
	mu        sync.Mutex
	status    discord.Status
	joined    discord.VoiceJob
	leftFor   string
	err       error
	joinCount int
	joinCh    chan discord.VoiceJob
}

type fakeReporter struct {
	participantStreamID string
	participants        []Participant
	speakerStreamID     string
	speakerUserID       string
	speakerDisplayName  string
	speakerCallCount    int
	chatStreamID        string
	chatMessage         ChatMessage
	err                 error
}

type fakeStreamStarter struct {
	mu      sync.Mutex
	started []string
	ch      chan string
	err     error
}

func (f *fakeVoice) Connect() error { return f.err }
func (f *fakeVoice) JoinVoice(job discord.VoiceJob) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.joined = job
	f.joinCount++
	f.status.Connected = true
	f.status.VoiceConnected = true
	if f.joinCh != nil {
		select {
		case f.joinCh <- job:
		default:
		}
	}
	return nil
}
func (f *fakeVoice) LeaveVoice(streamID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.leftFor = streamID
	f.status.VoiceConnected = false
	return nil
}
func (f *fakeVoice) Status() discord.Status {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.status
}

func (f *fakeReporter) ParticipantsChanged(streamID string, participants []Participant) error {
	f.participantStreamID = streamID
	f.participants = participants
	return f.err
}

func (f *fakeReporter) ActiveSpeakerChanged(streamID, userID, displayName string) error {
	f.speakerStreamID = streamID
	f.speakerUserID = userID
	f.speakerDisplayName = displayName
	f.speakerCallCount++
	return f.err
}

func (f *fakeReporter) ChatMessageReceived(streamID string, message ChatMessage) error {
	f.chatStreamID = streamID
	f.chatMessage = message
	return f.err
}

func (f *fakeStreamStarter) StartStream(streamID string) error {
	f.mu.Lock()
	f.started = append(f.started, streamID)
	f.mu.Unlock()
	if f.ch != nil {
		select {
		case f.ch <- streamID:
		default:
		}
	}
	return f.err
}

func TestManagerStartsAndStopsVoiceJob(t *testing.T) {
	voice := &fakeVoice{}
	manager := NewManager(voice)
	job := discord.VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01", EncoderAudioURL: "https://encoder.example.com", StreamIngestToken: "job-token"}
	if err := manager.Start(job); err != nil {
		t.Fatal(err)
	}
	if voice.joined.StreamID != "stream-01" || manager.CurrentStreamID() != "stream-01" {
		t.Fatalf("job was not started: %#v", voice.joined)
	}
	if voice.joined.EncoderAudioURL != "https://encoder.example.com" {
		t.Fatalf("encoder audio URL was not passed to voice client: %#v", voice.joined)
	}
	if status := manager.Status(); status.CurrentJob == nil || status.CurrentJob.EncoderAudioURL != "" || status.CurrentJob.StreamIngestToken != "" {
		t.Fatalf("status leaked job secrets: %#v", status.CurrentJob)
	}
	if err := manager.Stop("stream-01"); err != nil {
		t.Fatal(err)
	}
	if voice.leftFor != "stream-01" || manager.CurrentStreamID() != "" {
		t.Fatalf("job was not stopped: left=%q current=%q", voice.leftFor, manager.CurrentStreamID())
	}
}

func TestManagerStartAppliesVoiceDefaults(t *testing.T) {
	voice := &fakeVoice{}
	manager := NewManager(voice)
	manager.SetVoiceDefaults(VoiceDefaults{GuildID: "guild-default", VoiceChannelID: "voice-default", TextChannelID: "text-default", CaptionAudioURL: "https://caption.example.com/audio"})
	if err := manager.Start(discord.VoiceJob{StreamID: "stream-01"}); err != nil {
		t.Fatal(err)
	}
	if voice.joined.GuildID != "guild-default" || voice.joined.VoiceChannelID != "voice-default" || voice.joined.TextChannelID != "text-default" || voice.joined.CaptionAudioURL != "https://caption.example.com/audio" {
		t.Fatalf("voice defaults were not applied: %#v", voice.joined)
	}
}

func TestManagerStartAppliesStreamVoiceDefaults(t *testing.T) {
	voice := &fakeVoice{}
	manager := NewManager(voice)
	manager.SetVoiceDefaults(VoiceDefaults{GuildID: "guild-default", VoiceChannelID: "voice-default", TextChannelID: "text-default", CaptionAudioURL: "https://caption.example.com/default"})
	manager.SetStreamVoiceDefaults(map[string]VoiceDefaults{
		"stream-01": {
			GuildID:         "guild-stream",
			VoiceChannelID:  "voice-stream",
			TextChannelID:   "text-stream",
			CaptionAudioURL: "https://caption.example.com/stream",
		},
	})
	if err := manager.Start(discord.VoiceJob{StreamID: "stream-01"}); err != nil {
		t.Fatal(err)
	}
	if voice.joined.GuildID != "guild-stream" || voice.joined.VoiceChannelID != "voice-stream" || voice.joined.TextChannelID != "text-stream" || voice.joined.CaptionAudioURL != "https://caption.example.com/stream" {
		t.Fatalf("stream voice defaults were not applied: %#v", voice.joined)
	}
}

func TestVoiceUserJoinedStartsMatchingConfiguredStream(t *testing.T) {
	manager := NewManager(&fakeVoice{})
	manager.SetStreamVoiceDefaults(map[string]VoiceDefaults{
		"stream-01": {GuildID: "guild-01", VoiceChannelID: "voice-01", AutoStartEnabled: true},
	})
	starter := &fakeStreamStarter{ch: make(chan string, 2)}
	manager.SetStreamStarter(starter)

	manager.VoiceUserJoined(discord.VoiceJoinEvent{GuildID: "guild-01", VoiceChannelID: "voice-01", UserID: "user-01"})

	select {
	case got := <-starter.ch:
		if got != "stream-01" {
			t.Fatalf("unexpected auto-start stream: %q", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for auto-start")
	}

	manager.VoiceUserJoined(discord.VoiceJoinEvent{GuildID: "guild-01", VoiceChannelID: "voice-01", UserID: "user-02"})
	select {
	case got := <-starter.ch:
		t.Fatalf("duplicate join should be throttled, got %q", got)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestVoiceUserJoinedRequiresAutoStartEnabled(t *testing.T) {
	manager := NewManager(&fakeVoice{})
	manager.SetStreamVoiceDefaults(map[string]VoiceDefaults{
		"stream-01": {GuildID: "guild-01", VoiceChannelID: "voice-01"},
	})
	starter := &fakeStreamStarter{ch: make(chan string, 1)}
	manager.SetStreamStarter(starter)

	manager.VoiceUserJoined(discord.VoiceJoinEvent{GuildID: "guild-01", VoiceChannelID: "voice-01", UserID: "user-01"})
	select {
	case got := <-starter.ch:
		t.Fatalf("stream without auto-start trigger should not start, got %q", got)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestVoiceUserJoinedDoesNotStartAmbiguousOrActiveStream(t *testing.T) {
	manager := NewManager(&fakeVoice{})
	manager.SetStreamVoiceDefaults(map[string]VoiceDefaults{
		"stream-01": {GuildID: "guild-01", VoiceChannelID: "voice-01", AutoStartEnabled: true},
		"stream-02": {GuildID: "guild-01", VoiceChannelID: "voice-01", AutoStartEnabled: true},
	})
	starter := &fakeStreamStarter{ch: make(chan string, 1)}
	manager.SetStreamStarter(starter)

	manager.VoiceUserJoined(discord.VoiceJoinEvent{GuildID: "guild-01", VoiceChannelID: "voice-01", UserID: "user-01"})
	select {
	case got := <-starter.ch:
		t.Fatalf("ambiguous voice channel should not start a stream, got %q", got)
	case <-time.After(100 * time.Millisecond):
	}

	manager.SetStreamVoiceDefaults(map[string]VoiceDefaults{
		"stream-01": {GuildID: "guild-01", VoiceChannelID: "voice-01", AutoStartEnabled: true},
	})
	if err := manager.Start(discord.VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01"}); err != nil {
		t.Fatal(err)
	}
	manager.VoiceUserJoined(discord.VoiceJoinEvent{GuildID: "guild-01", VoiceChannelID: "voice-01", UserID: "user-02"})
	select {
	case got := <-starter.ch:
		t.Fatalf("active stream should suppress auto-start, got %q", got)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestManagerRejectsSecondActiveJob(t *testing.T) {
	manager := NewManager(&fakeVoice{})
	if err := manager.Start(discord.VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01"}); err != nil {
		t.Fatal(err)
	}
	err := manager.Start(discord.VoiceJob{StreamID: "stream-02", GuildID: "guild-01", VoiceChannelID: "voice-01"})
	if err == nil {
		t.Fatal("expected second job to be rejected")
	}
}

func TestParticipantAndActiveSpeakerState(t *testing.T) {
	reporter := &fakeReporter{}
	voice := &fakeVoice{status: discord.Status{Connected: true, VoiceConnected: true, AudioForwardEnabled: true, AudioForwardActive: true, AudioReceiving: true, AudioPacketsReceived: 12, AudioPacketsForwarded: 10, AudioForwardErrors: 1, GatewayReconnectCount: 2, VoiceDisconnectCount: 1}}
	manager := NewManagerWithReporter(voice, reporter)
	if err := manager.Start(discord.VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01"}); err != nil {
		t.Fatal(err)
	}
	manager.ParticipantChanged(discord.ParticipantEvent{StreamID: "stream-01", UserID: "user-01", Username: "alice", Present: true})
	if reporter.participantStreamID != "stream-01" || len(reporter.participants) != 1 {
		t.Fatalf("participant event was not reported: %#v", reporter)
	}
	if err := manager.SetActiveSpeaker("stream-01", "user-01"); err != nil {
		t.Fatal(err)
	}
	if reporter.speakerStreamID != "stream-01" || reporter.speakerUserID != "user-01" || reporter.speakerDisplayName != "alice" {
		t.Fatalf("active speaker event was not reported: %#v", reporter)
	}
	status := manager.Status()
	if status.ParticipantCount != 1 || status.ActiveSpeakerID != "user-01" {
		t.Fatalf("unexpected status: %#v", status)
	}
	if status.Metrics["discord.gateway_connected"] != 1 ||
		status.Metrics["discord.audio_forward_enabled"] != 1 ||
		status.Metrics["discord.audio_forward_active"] != 1 ||
		status.Metrics["discord.audio_packets_total"] != 12 ||
		status.Metrics["discord.audio_forward_errors_total"] != 1 ||
		status.Metrics["discord.worker_event_publish_failures_total"] != 0 ||
		status.Metrics["discord.reconnect_count"] != 2 ||
		status.Metrics["discord.voice_disconnect_count"] != 1 {
		t.Fatalf("unexpected metrics: %#v", status.Metrics)
	}
	manager.ParticipantChanged(discord.ParticipantEvent{StreamID: "stream-01", UserID: "user-01", Present: false})
	status = manager.Status()
	if status.ParticipantCount != 0 || status.ActiveSpeakerID != "" {
		t.Fatalf("participant removal did not clear state: %#v", status)
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
	if status.Metrics["discord.worker_event_publish_failures_total"] != 2 {
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
		Content:       " こんにちは ",
		CreatedAt:     time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC),
	})
	if reporter.chatStreamID != "stream-01" || reporter.chatMessage.MessageID != "msg-01" || reporter.chatMessage.Content != "こんにちは" || reporter.chatMessage.Username != "alice" {
		t.Fatalf("chat message was not published: stream=%q message=%#v", reporter.chatStreamID, reporter.chatMessage)
	}
}

func TestActiveSpeakerDetectedPublishesWorkerEvent(t *testing.T) {
	reporter := &fakeReporter{}
	manager := NewManagerWithReporter(&fakeVoice{}, reporter)
	if err := manager.Start(discord.VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01"}); err != nil {
		t.Fatal(err)
	}
	manager.ParticipantChanged(discord.ParticipantEvent{StreamID: "stream-01", UserID: "user-01", Username: "alice", Present: true})
	manager.ActiveSpeakerDetected("stream-01", "user-01")

	if reporter.speakerStreamID != "stream-01" || reporter.speakerUserID != "user-01" || reporter.speakerDisplayName != "alice" {
		t.Fatalf("active speaker detected event was not reported: %#v", reporter)
	}
	status := manager.Status()
	if status.ActiveSpeakerID != "user-01" {
		t.Fatalf("active speaker state was not updated: %#v", status)
	}
}

func TestDuplicateActiveSpeakerDetectedIsNoop(t *testing.T) {
	reporter := &fakeReporter{}
	manager := NewManagerWithReporter(&fakeVoice{}, reporter)
	if err := manager.Start(discord.VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01"}); err != nil {
		t.Fatal(err)
	}
	manager.ParticipantChanged(discord.ParticipantEvent{StreamID: "stream-01", UserID: "user-01", Username: "alice", Present: true})
	manager.ActiveSpeakerDetected("stream-01", "user-01")
	manager.ActiveSpeakerDetected("stream-01", "user-01")

	if reporter.speakerCallCount != 1 {
		t.Fatalf("duplicate active speaker should not be reported repeatedly, got %d", reporter.speakerCallCount)
	}
}

func TestManagerRejoinsVoiceAfterVoiceDisconnect(t *testing.T) {
	voice := &fakeVoice{joinCh: make(chan discord.VoiceJob, 2)}
	manager := NewManager(voice)
	manager.SetReconnectPolicy(ReconnectPolicy{Enabled: true, MaxAttempts: 1})
	job := discord.VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01", EncoderAudioURL: "https://encoder.example.com/audio", StreamIngestToken: "job-token"}
	if err := manager.Start(job); err != nil {
		t.Fatal(err)
	}
	<-voice.joinCh

	manager.DiscordDisconnected("voice_state_disconnected")

	var rejoined discord.VoiceJob
	select {
	case rejoined = <-voice.joinCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for voice rejoin")
	}
	if rejoined.StreamID != job.StreamID || rejoined.GuildID != job.GuildID || rejoined.VoiceChannelID != job.VoiceChannelID || rejoined.StreamIngestToken != job.StreamIngestToken {
		t.Fatalf("unexpected rejoin job: %#v", rejoined)
	}
	status := manager.Status()
	if status.Metrics["discord.voice_rejoin_attempts_total"] != 1 || status.Metrics["discord.voice_rejoin_failures_total"] != 0 {
		t.Fatalf("unexpected rejoin metrics: %#v", status.Metrics)
	}
}

func TestManagerDoesNotRejoinOnGatewayDisconnect(t *testing.T) {
	voice := &fakeVoice{joinCh: make(chan discord.VoiceJob, 2)}
	manager := NewManager(voice)
	manager.SetReconnectPolicy(ReconnectPolicy{Enabled: true, MaxAttempts: 1})
	if err := manager.Start(discord.VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01"}); err != nil {
		t.Fatal(err)
	}
	<-voice.joinCh

	manager.DiscordDisconnected("gateway_disconnect")

	select {
	case job := <-voice.joinCh:
		t.Fatalf("gateway disconnect should not force voice rejoin: %#v", job)
	case <-time.After(50 * time.Millisecond):
	}
}
