package jobs

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/example/autostream-discord-bot/internal/control"
	"github.com/example/autostream-discord-bot/internal/discord"
)

type fakeVoice struct {
	mu            sync.Mutex
	status        discord.Status
	joined        discord.VoiceJob
	leftFor       string
	err           error
	joinCount     int
	joinCh        chan discord.VoiceJob
	sentMessages  []discord.OutboundMessage
	sendErr       error
	sendMessageID string
	sendStarted   chan struct{}
	sendRelease   chan struct{}
}

type snapshotVoice struct {
	fakeVoice
	snapshot discord.ParticipantSnapshot
	known    bool
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
	errs    []error
}

type fakeStreamStopper struct {
	mu      sync.Mutex
	stopped []string
	ch      chan string
	err     error
	errs    []error
}

type blockingContextStreamStopper struct {
	started   chan string
	canceled  chan string
	committed chan string
}

type commitAfterCancelStreamStopper struct {
	started   chan string
	canceled  chan string
	release   chan struct{}
	committed chan string
}

// panelStopCallbackStreamStopper models the nested lifecycle call made by the
// Control Panel: the Bot initiates auto-stop, then the Panel calls this Bot's
// stop endpoint before it has dispatched the Encoder/Worker stops and rearmed
// the next waiting stream.
type panelStopCallbackStreamStopper struct {
	manager  *Manager
	started  chan string
	callback chan string
	ready    chan string
	release  chan struct{}
	returned chan string
	canceled chan string
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
func (f *fakeVoice) SendMessage(ctx context.Context, message discord.OutboundMessage) (discord.SentMessage, error) {
	f.mu.Lock()
	f.sentMessages = append(f.sentMessages, message)
	err := f.sendErr
	messageID := f.sendMessageID
	started := f.sendStarted
	release := f.sendRelease
	f.mu.Unlock()
	if started != nil {
		select {
		case started <- struct{}{}:
		default:
		}
	}
	if release != nil {
		select {
		case <-release:
		case <-ctx.Done():
			return discord.SentMessage{}, ctx.Err()
		}
	}
	if err != nil {
		return discord.SentMessage{}, err
	}
	if messageID == "" {
		messageID = "message-01"
	}
	return discord.SentMessage{MessageID: messageID}, nil
}
func (f *fakeVoice) Status() discord.Status {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.status
}

func (f *snapshotVoice) SnapshotVoiceParticipants(job discord.VoiceJob) (discord.ParticipantSnapshot, bool) {
	if !f.known {
		return discord.ParticipantSnapshot{}, false
	}
	snapshot := f.snapshot
	if snapshot.StreamID == "" {
		snapshot.StreamID = job.StreamID
	}
	if snapshot.GuildID == "" {
		snapshot.GuildID = job.GuildID
	}
	if snapshot.VoiceChannelID == "" {
		snapshot.VoiceChannelID = job.VoiceChannelID
	}
	return snapshot, true
}

func (f *fakeReporter) ParticipantsChanged(job discord.VoiceJob, participants []Participant) error {
	f.participantStreamID = job.StreamID
	f.participants = participants
	return f.err
}

func (f *fakeReporter) ActiveSpeakerChanged(job discord.VoiceJob, userID, displayName string) error {
	f.speakerStreamID = job.StreamID
	f.speakerUserID = userID
	f.speakerDisplayName = displayName
	f.speakerCallCount++
	return f.err
}

func (f *fakeReporter) ChatMessageReceived(job discord.VoiceJob, message ChatMessage) error {
	f.chatStreamID = job.StreamID
	f.chatMessage = message
	return f.err
}

func (f *fakeStreamStarter) StartStream(streamID string) error {
	f.mu.Lock()
	f.started = append(f.started, streamID)
	err := f.err
	if len(f.errs) > 0 {
		err = f.errs[0]
		f.errs = f.errs[1:]
	}
	f.mu.Unlock()
	if f.ch != nil {
		select {
		case f.ch <- streamID:
		default:
		}
	}
	return err
}

func (f *fakeStreamStopper) StopStream(streamID string) error {
	f.mu.Lock()
	f.stopped = append(f.stopped, streamID)
	err := f.err
	if len(f.errs) > 0 {
		err = f.errs[0]
		f.errs = f.errs[1:]
	}
	f.mu.Unlock()
	if f.ch != nil {
		select {
		case f.ch <- streamID:
		default:
		}
	}
	return err
}

func (f *blockingContextStreamStopper) StopStream(streamID string) error {
	return errors.New("context-aware auto-stop was not used")
}

func (f *blockingContextStreamStopper) StopStreamContext(ctx context.Context, streamID string) error {
	select {
	case f.started <- streamID:
	default:
	}
	select {
	case <-ctx.Done():
		select {
		case f.canceled <- streamID:
		default:
		}
		return ctx.Err()
	case <-time.After(time.Second):
		select {
		case f.committed <- streamID:
		default:
		}
		return nil
	}
}

func (f *commitAfterCancelStreamStopper) StopStream(streamID string) error {
	return errors.New("context-aware auto-stop was not used")
}

func (f *commitAfterCancelStreamStopper) StopStreamContext(ctx context.Context, streamID string) error {
	select {
	case f.started <- streamID:
	default:
	}
	<-ctx.Done()
	select {
	case f.canceled <- streamID:
	default:
	}
	<-f.release
	select {
	case f.committed <- streamID:
	default:
	}
	return nil
}

func (f *panelStopCallbackStreamStopper) StopStream(streamID string) error {
	return errors.New("context-aware auto-stop was not used")
}

func (f *panelStopCallbackStreamStopper) StopStreamContext(ctx context.Context, streamID string) error {
	select {
	case f.started <- streamID:
	default:
	}
	if err := f.manager.Stop(streamID); err != nil {
		return err
	}
	select {
	case f.callback <- streamID:
	default:
	}
	select {
	case <-ctx.Done():
		select {
		case f.canceled <- streamID:
		default:
		}
		return ctx.Err()
	default:
	}
	select {
	case f.ready <- streamID:
	default:
	}
	select {
	case <-ctx.Done():
		select {
		case f.canceled <- streamID:
		default:
		}
		return ctx.Err()
	case <-f.release:
		select {
		case f.returned <- streamID:
		default:
		}
		return nil
	}
}

func TestManagerStartsAndStopsVoiceJob(t *testing.T) {
	voice := &fakeVoice{}
	manager := NewManager(voice)
	job := discord.VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01", EncoderAudioURL: "https://encoder.example.com", CaptionAudioURL: "https://caption.example.com", StreamIngestToken: "job-token", CaptionAudioToken: "caption-token", WorkerEventsURL: "https://worker.example.com", WorkerEventsToken: "worker-events-token"}
	if err := manager.Start(job); err != nil {
		t.Fatal(err)
	}
	if voice.joined.StreamID != "stream-01" || manager.CurrentStreamID() != "stream-01" {
		t.Fatalf("job was not started: %#v", voice.joined)
	}
	if voice.joined.EncoderAudioURL != "https://encoder.example.com" {
		t.Fatalf("encoder audio URL was not passed to voice client: %#v", voice.joined)
	}
	if status := manager.Status(); status.CurrentJob == nil || status.CurrentJob.EncoderAudioURL != "" || status.CurrentJob.CaptionAudioURL != "" || status.CurrentJob.StreamIngestToken != "" || status.CurrentJob.CaptionAudioToken != "" || status.CurrentJob.WorkerEventsURL != "" || status.CurrentJob.WorkerEventsToken != "" {
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
	manager.SetVoiceDefaults(VoiceDefaults{GuildID: "guild-default", VoiceChannelID: "voice-default", TextChannelID: "text-default"})
	if err := manager.Start(discord.VoiceJob{StreamID: "stream-01"}); err != nil {
		t.Fatal(err)
	}
	if voice.joined.GuildID != "guild-default" || voice.joined.VoiceChannelID != "voice-default" || voice.joined.TextChannelID != "text-default" || voice.joined.CaptionAudioURL != "" {
		t.Fatalf("voice defaults were not applied: %#v", voice.joined)
	}
}

func TestManagerStartAppliesStreamVoiceDefaults(t *testing.T) {
	voice := &fakeVoice{}
	manager := NewManager(voice)
	manager.SetVoiceDefaults(VoiceDefaults{GuildID: "guild-default", VoiceChannelID: "voice-default", TextChannelID: "text-default"})
	manager.SetStreamVoiceDefaults(map[string]VoiceDefaults{
		"stream-01": {
			GuildID:        "guild-stream",
			VoiceChannelID: "voice-stream",
			TextChannelID:  "text-stream",
		},
	})
	if err := manager.Start(discord.VoiceJob{StreamID: "stream-01"}); err != nil {
		t.Fatal(err)
	}
	if voice.joined.GuildID != "guild-stream" || voice.joined.VoiceChannelID != "voice-stream" || voice.joined.TextChannelID != "text-stream" || voice.joined.CaptionAudioURL != "" {
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

func TestVoiceUserJoinedRefreshesDefaultsBeforeMatching(t *testing.T) {
	manager := NewManager(&fakeVoice{})
	starter := &fakeStreamStarter{ch: make(chan string, 1)}
	manager.SetStreamStarter(starter)
	manager.SetAutoStartRefresher(func() error {
		manager.SetStreamVoiceDefaults(map[string]VoiceDefaults{
			"stream-new": {GuildID: "guild-new", VoiceChannelID: "voice-new", AutoStartEnabled: true},
		})
		return nil
	})

	manager.VoiceUserJoined(discord.VoiceJoinEvent{GuildID: "guild-new", VoiceChannelID: "voice-new", UserID: "user-01"})
	select {
	case got := <-starter.ch:
		if got != "stream-new" {
			t.Fatalf("unexpected auto-start stream after refresh: %q", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for refreshed auto-start")
	}
}

func TestVoiceUserJoinedRefreshesAndRetriesARearmedSuccessorAfterNotFound(t *testing.T) {
	manager := NewManager(&fakeVoice{})
	manager.SetStreamVoiceDefaults(map[string]VoiceDefaults{
		"stream-old": {GuildID: "guild-01", VoiceChannelID: "voice-01", AutoStartEnabled: true},
	})
	starter := &fakeStreamStarter{
		ch:   make(chan string, 2),
		errs: []error{control.ControlPanelError{StatusCode: 404, Code: "not_found"}, nil},
	}
	manager.SetStreamStarter(starter)
	manager.SetAutoStartRefresher(func() error {
		manager.SetStreamVoiceDefaults(map[string]VoiceDefaults{
			"stream-new": {GuildID: "guild-01", VoiceChannelID: "voice-01", AutoStartEnabled: true},
		})
		return nil
	})

	manager.VoiceUserJoined(discord.VoiceJoinEvent{GuildID: "guild-01", VoiceChannelID: "voice-01", UserID: "user-01"})
	select {
	case got := <-starter.ch:
		if got != "stream-old" {
			t.Fatalf("first auto-start stream = %q, want stream-old", got)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for stale auto-start")
	}
	select {
	case got := <-starter.ch:
		if got != "stream-new" {
			t.Fatalf("successor auto-start stream = %q, want stream-new", got)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for refreshed successor auto-start")
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
	voice := &fakeVoice{status: discord.Status{Connected: true, VoiceConnected: true, AudioForwardEnabled: true, AudioForwardActive: true, CaptionAudioForwardActive: true, AudioReceiving: true, AudioPacketsReceived: 12, AudioPacketsForwarded: 10, AudioForwardErrors: 1, CaptionPacketsForwarded: 9, CaptionForwardErrors: 2, GatewayReconnectCount: 2, VoiceDisconnectCount: 1}}
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
		status.Metrics["discord.caption_audio_forward_active"] != 1 ||
		status.Metrics["discord.audio_packets_total"] != 12 ||
		status.Metrics["discord.audio_forward_errors_total"] != 1 ||
		status.Metrics["discord.caption_packets_forwarded_total"] != 9 ||
		status.Metrics["discord.caption_forward_errors_total"] != 2 ||
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

func TestParticipantLeavingEmptyVCRequestsStreamStopOnce(t *testing.T) {
	manager := NewManager(&fakeVoice{})
	manager.autoStopDelay = 10 * time.Millisecond
	manager.autoStopCooldown = time.Minute
	stopper := &fakeStreamStopper{ch: make(chan string, 2)}
	manager.SetStreamStopper(stopper)
	if err := manager.Start(discord.VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01"}); err != nil {
		t.Fatal(err)
	}
	manager.ParticipantChanged(discord.ParticipantEvent{StreamID: "stream-01", UserID: "user-01", Present: true})
	manager.ParticipantChanged(discord.ParticipantEvent{StreamID: "stream-01", UserID: "user-01", Present: false})
	manager.ParticipantChanged(discord.ParticipantEvent{StreamID: "stream-01", UserID: "user-01", Present: false})

	select {
	case got := <-stopper.ch:
		if got != "stream-01" {
			t.Fatalf("unexpected auto-stop stream: %q", got)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for auto-stop")
	}
	select {
	case got := <-stopper.ch:
		t.Fatalf("duplicate participant leave requested another stop: %q", got)
	case <-time.After(40 * time.Millisecond):
	}
}

func TestStartHydratesExistingVoiceParticipantsBeforeLeave(t *testing.T) {
	voice := &snapshotVoice{
		known: true,
		snapshot: discord.ParticipantSnapshot{
			Revision: 1,
			Participants: []discord.VoiceParticipant{
				{UserID: "user-a"},
				{UserID: "user-b"},
			},
		},
	}
	manager := NewManager(voice)
	manager.autoStopDelay = 5 * time.Millisecond
	stopper := &fakeStreamStopper{ch: make(chan string, 1)}
	manager.SetStreamStopper(stopper)
	job := discord.VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01"}
	if err := manager.Start(job); err != nil {
		t.Fatal(err)
	}
	if status := manager.Status(); status.ParticipantCount != 2 {
		t.Fatalf("start must hydrate both already-present users, got %#v", status)
	}

	// B leaving must not stop the stream while A was already in the VC before
	// the job became active.
	manager.ParticipantChanged(discord.ParticipantEvent{StreamID: job.StreamID, UserID: "user-b", Present: false})
	select {
	case got := <-stopper.ch:
		t.Fatalf("remaining hydrated participant should block auto-stop, got %q", got)
	case <-time.After(30 * time.Millisecond):
	}

	manager.ParticipantChanged(discord.ParticipantEvent{StreamID: job.StreamID, UserID: "user-a", Present: false})
	select {
	case got := <-stopper.ch:
		if got != job.StreamID {
			t.Fatalf("auto-stop stream = %q, want %q", got, job.StreamID)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for empty-VC auto-stop")
	}
}

func TestStartDoesNotAutoStopOnTransientEmptyInitialSnapshot(t *testing.T) {
	voice := &snapshotVoice{
		known: true,
		snapshot: discord.ParticipantSnapshot{
			Revision:       1,
			StreamID:       "stream-01",
			GuildID:        "guild-01",
			VoiceChannelID: "voice-01",
			Participants:   nil,
		},
	}
	manager := NewManager(voice)
	manager.autoStopDelay = 5 * time.Millisecond
	stopper := &fakeStreamStopper{ch: make(chan string, 1)}
	manager.SetStreamStopper(stopper)
	job := discord.VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01"}
	if err := manager.Start(job); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-stopper.ch:
		t.Fatalf("empty initial hydration must not immediately stop a newly joined job: %q", got)
	case <-time.After(50 * time.Millisecond):
	}

	// Once a later authoritative snapshot arrives, the normal empty-VC policy
	// is still allowed to stop the job.
	manager.ParticipantsSynced(discord.ParticipantSnapshot{
		StreamID:       job.StreamID,
		GuildID:        job.GuildID,
		VoiceChannelID: job.VoiceChannelID,
		Revision:       2,
	})
	select {
	case got := <-stopper.ch:
		if got != job.StreamID {
			t.Fatalf("authoritative empty snapshot stopped %q, want %q", got, job.StreamID)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for authoritative empty-VC auto-stop")
	}
}

func TestParticipantsSyncedReplacesStaleMemberAfterGatewayRecovery(t *testing.T) {
	manager := NewManager(&fakeVoice{})
	manager.autoStopDelay = 5 * time.Millisecond
	stopper := &fakeStreamStopper{ch: make(chan string, 1)}
	manager.SetStreamStopper(stopper)
	job := discord.VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01"}
	if err := manager.Start(job); err != nil {
		t.Fatal(err)
	}
	manager.ParticipantChanged(discord.ParticipantEvent{StreamID: job.StreamID, UserID: "user-stale", Present: true})

	// The gateway reconnect can miss the old leave dispatch. A complete,
	// authoritative State view of the target VC must replace—not merge—the
	// stale member so the empty-channel stop path resumes.
	manager.ParticipantsSynced(discord.ParticipantSnapshot{
		StreamID:       job.StreamID,
		GuildID:        job.GuildID,
		VoiceChannelID: job.VoiceChannelID,
		Revision:       2,
	})
	select {
	case got := <-stopper.ch:
		if got != job.StreamID {
			t.Fatalf("auto-stop stream = %q, want %q", got, job.StreamID)
		}
	case <-time.After(time.Second):
		t.Fatal("authoritative empty snapshot did not resume auto-stop")
	}
}

func TestParticipantsSyncedIgnoresDelayedOlderSnapshot(t *testing.T) {
	manager := NewManager(&fakeVoice{})
	manager.autoStopDelay = 20 * time.Millisecond
	stopper := &fakeStreamStopper{ch: make(chan string, 1)}
	manager.SetStreamStopper(stopper)
	job := discord.VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01"}
	if err := manager.Start(job); err != nil {
		t.Fatal(err)
	}
	manager.ParticipantsSynced(discord.ParticipantSnapshot{
		StreamID:       job.StreamID,
		GuildID:        job.GuildID,
		VoiceChannelID: job.VoiceChannelID,
		Participants:   []discord.VoiceParticipant{{UserID: "user-01"}},
		Revision:       3,
	})
	manager.ParticipantsSynced(discord.ParticipantSnapshot{
		StreamID:       job.StreamID,
		GuildID:        job.GuildID,
		VoiceChannelID: job.VoiceChannelID,
		Revision:       4,
	})
	// An older asynchronous handler must not restore a departed participant
	// and cancel the valid empty-VC stop.
	manager.ParticipantsSynced(discord.ParticipantSnapshot{
		StreamID:       job.StreamID,
		GuildID:        job.GuildID,
		VoiceChannelID: job.VoiceChannelID,
		Participants:   []discord.VoiceParticipant{{UserID: "user-01"}},
		Revision:       3,
	})
	select {
	case got := <-stopper.ch:
		if got != job.StreamID {
			t.Fatalf("auto-stop stream = %q, want %q", got, job.StreamID)
		}
	case <-time.After(time.Second):
		t.Fatal("older snapshot incorrectly canceled empty-VC auto-stop")
	}
}

func TestVoiceRejoinHydratesCurrentParticipants(t *testing.T) {
	voice := &snapshotVoice{
		known: true,
		snapshot: discord.ParticipantSnapshot{
			Revision:     1,
			Participants: []discord.VoiceParticipant{{UserID: "user-stale"}},
		},
	}
	manager := NewManager(voice)
	manager.autoStopDelay = 5 * time.Millisecond
	stopper := &fakeStreamStopper{ch: make(chan string, 1)}
	manager.SetStreamStopper(stopper)
	job := discord.VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01"}
	if err := manager.Start(job); err != nil {
		t.Fatal(err)
	}
	if status := manager.Status(); status.ParticipantCount != 1 {
		t.Fatalf("start snapshot = %#v, want one participant", status)
	}

	// A voice-only reconnect does not emit Gateway RESUMED. Its successful
	// rejoin must still replace stale membership before the empty-VC stop is
	// evaluated.
	voice.snapshot = discord.ParticipantSnapshot{Revision: 2}
	manager.mu.Lock()
	generation := manager.reconnectGeneration
	manager.mu.Unlock()
	manager.rejoinVoiceWithBackoff(job, ReconnectPolicy{Enabled: true, MaxAttempts: 1}, generation)

	select {
	case got := <-stopper.ch:
		if got != job.StreamID {
			t.Fatalf("auto-stop stream = %q, want %q", got, job.StreamID)
		}
	case <-time.After(time.Second):
		t.Fatal("voice rejoin did not hydrate its authoritative empty VC snapshot")
	}
}

func TestParticipantReturningBeforeAutoStopCancelsRequest(t *testing.T) {
	manager := NewManager(&fakeVoice{})
	manager.autoStopDelay = 30 * time.Millisecond
	stopper := &fakeStreamStopper{ch: make(chan string, 1)}
	manager.SetStreamStopper(stopper)
	if err := manager.Start(discord.VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01"}); err != nil {
		t.Fatal(err)
	}
	manager.ParticipantChanged(discord.ParticipantEvent{StreamID: "stream-01", UserID: "user-01", Present: true})
	manager.ParticipantChanged(discord.ParticipantEvent{StreamID: "stream-01", UserID: "user-01", Present: false})
	time.Sleep(5 * time.Millisecond)
	manager.ParticipantChanged(discord.ParticipantEvent{StreamID: "stream-01", UserID: "user-01", Present: true})
	select {
	case got := <-stopper.ch:
		t.Fatalf("participant return should cancel auto-stop, got %q", got)
	case <-time.After(70 * time.Millisecond):
	}
}

func TestParticipantLeavingEmptyVCRetriesTransientStopFailures(t *testing.T) {
	manager := NewManager(&fakeVoice{})
	manager.autoStopDelay = 5 * time.Millisecond
	manager.autoStopRetryDelays = []time.Duration{5 * time.Millisecond, 10 * time.Millisecond, 15 * time.Millisecond}
	stopper := &fakeStreamStopper{
		ch: make(chan string, 5),
		errs: []error{
			control.AutoStopTransportError{Err: errors.New("transport connection reset")},
			control.ControlPanelError{StatusCode: 429},
			control.ControlPanelError{StatusCode: 502},
			nil,
		},
	}
	manager.SetStreamStopper(stopper)
	if err := manager.Start(discord.VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01"}); err != nil {
		t.Fatal(err)
	}
	manager.ParticipantChanged(discord.ParticipantEvent{StreamID: "stream-01", UserID: "user-01", Present: true})
	manager.ParticipantChanged(discord.ParticipantEvent{StreamID: "stream-01", UserID: "user-01", Present: false})

	for attempt := 1; attempt <= 4; attempt++ {
		select {
		case got := <-stopper.ch:
			if got != "stream-01" {
				t.Fatalf("attempt %d stopped %q, want stream-01", attempt, got)
			}
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for auto-stop attempt %d", attempt)
		}
	}
	select {
	case got := <-stopper.ch:
		t.Fatalf("successful retry should finish auto-stop, got another request for %q", got)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestAutoStopRetryPolicyUsesBoundedExpectedDelays(t *testing.T) {
	manager := NewManager(&fakeVoice{})
	want := []time.Duration{5 * time.Second, 15 * time.Second, 45 * time.Second}
	if len(manager.autoStopRetryDelays) != len(want) {
		t.Fatalf("auto-stop retry delays = %#v, want %#v", manager.autoStopRetryDelays, want)
	}
	for index, delay := range want {
		if got := manager.autoStopRetryDelays[index]; got != delay {
			t.Fatalf("auto-stop retry delay %d = %s, want %s", index, got, delay)
		}
	}
}

func TestParticipantLeavingEmptyVCDoesNotRetryRejectedStop(t *testing.T) {
	manager := NewManager(&fakeVoice{})
	manager.autoStopDelay = 5 * time.Millisecond
	manager.autoStopRetryDelays = []time.Duration{5 * time.Millisecond}
	stopper := &fakeStreamStopper{ch: make(chan string, 2), errs: []error{control.ControlPanelError{StatusCode: 409}}}
	manager.SetStreamStopper(stopper)
	if err := manager.Start(discord.VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01"}); err != nil {
		t.Fatal(err)
	}
	manager.ParticipantChanged(discord.ParticipantEvent{StreamID: "stream-01", UserID: "user-01", Present: true})
	manager.ParticipantChanged(discord.ParticipantEvent{StreamID: "stream-01", UserID: "user-01", Present: false})
	select {
	case <-stopper.ch:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for rejected auto-stop request")
	}
	select {
	case got := <-stopper.ch:
		t.Fatalf("rejected 4xx auto-stop should not retry, got %q", got)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestParticipantReturningCancelsScheduledAutoStopRetry(t *testing.T) {
	manager := NewManager(&fakeVoice{})
	manager.autoStopDelay = 5 * time.Millisecond
	manager.autoStopRetryDelays = []time.Duration{40 * time.Millisecond}
	stopper := &fakeStreamStopper{ch: make(chan string, 2), errs: []error{control.AutoStopTransportError{Err: errors.New("transport unavailable")}}}
	manager.SetStreamStopper(stopper)
	if err := manager.Start(discord.VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01"}); err != nil {
		t.Fatal(err)
	}
	manager.ParticipantChanged(discord.ParticipantEvent{StreamID: "stream-01", UserID: "user-01", Present: true})
	manager.ParticipantChanged(discord.ParticipantEvent{StreamID: "stream-01", UserID: "user-01", Present: false})
	select {
	case <-stopper.ch:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for initial auto-stop request")
	}
	manager.ParticipantChanged(discord.ParticipantEvent{StreamID: "stream-01", UserID: "user-01", Present: true})
	select {
	case got := <-stopper.ch:
		t.Fatalf("participant return should cancel retry, got %q", got)
	case <-time.After(80 * time.Millisecond):
	}
}

func TestParticipantReturningCancelsInFlightAutoStopRequest(t *testing.T) {
	manager := NewManager(&fakeVoice{})
	manager.autoStopDelay = 0
	stopper := &blockingContextStreamStopper{
		started:   make(chan string, 1),
		canceled:  make(chan string, 1),
		committed: make(chan string, 1),
	}
	manager.SetStreamStopper(stopper)
	if err := manager.Start(discord.VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01"}); err != nil {
		t.Fatal(err)
	}
	manager.ParticipantChanged(discord.ParticipantEvent{StreamID: "stream-01", UserID: "user-01", Present: true})
	manager.ParticipantChanged(discord.ParticipantEvent{StreamID: "stream-01", UserID: "user-01", Present: false})
	select {
	case got := <-stopper.started:
		if got != "stream-01" {
			t.Fatalf("in-flight stop stream = %q, want stream-01", got)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for in-flight auto-stop request")
	}

	manager.ParticipantChanged(discord.ParticipantEvent{StreamID: "stream-01", UserID: "user-01", Present: true})
	select {
	case got := <-stopper.canceled:
		if got != "stream-01" {
			t.Fatalf("canceled stream = %q, want stream-01", got)
		}
	case <-time.After(time.Second):
		t.Fatal("participant return did not cancel in-flight auto-stop request")
	}
	select {
	case got := <-stopper.committed:
		t.Fatalf("stale auto-stop committed after participant return for %q", got)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestControlPanelStopCallbackDoesNotCancelOwnAutoStopRequest(t *testing.T) {
	manager := NewManager(&fakeVoice{})
	manager.autoStopDelay = 0
	stopper := &panelStopCallbackStreamStopper{
		manager:  manager,
		started:  make(chan string, 1),
		callback: make(chan string, 1),
		ready:    make(chan string, 1),
		release:  make(chan struct{}),
		returned: make(chan string, 1),
		canceled: make(chan string, 1),
	}
	manager.SetStreamStopper(stopper)
	if err := manager.Start(discord.VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01"}); err != nil {
		t.Fatal(err)
	}
	manager.ParticipantChanged(discord.ParticipantEvent{StreamID: "stream-01", UserID: "user-01", Present: true})
	manager.ParticipantChanged(discord.ParticipantEvent{StreamID: "stream-01", UserID: "user-01", Present: false})
	select {
	case got := <-stopper.started:
		if got != "stream-01" {
			t.Fatalf("auto-stop stream = %q, want stream-01", got)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for auto-stop request")
	}
	select {
	case got := <-stopper.callback:
		if got != "stream-01" {
			t.Fatalf("Panel callback stream = %q, want stream-01", got)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for nested Control Panel stop callback")
	}
	if got := manager.CurrentStreamID(); got != "" {
		t.Fatalf("nested Panel stop did not clear current stream: %q", got)
	}
	select {
	case got := <-stopper.ready:
		if got != "stream-01" {
			t.Fatalf("parent auto-stop readiness stream = %q, want stream-01", got)
		}
	case got := <-stopper.canceled:
		t.Fatalf("Panel callback canceled its own parent auto-stop request for %q", got)
	case <-time.After(time.Second):
		t.Fatal("auto-stop request did not remain live after nested Panel callback")
	}

	close(stopper.release)
	select {
	case got := <-stopper.returned:
		if got != "stream-01" {
			t.Fatalf("returned auto-stop stream = %q, want stream-01", got)
		}
	case <-time.After(time.Second):
		t.Fatal("auto-stop request did not finish after the Panel dispatch completed")
	}
	select {
	case got := <-stopper.canceled:
		t.Fatalf("Panel callback canceled its own parent auto-stop request for %q", got)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestVoiceJoinAfterPanelStopCallbackReconcilesRearmedStreamAfterRuntimeRefresh(t *testing.T) {
	manager := NewManager(&fakeVoice{})
	manager.autoStopDelay = 0
	// The production policy retains the intent for more than one minute. Use
	// millisecond-scale service windows here while requiring the rearmed source
	// row to remain absent for two sequential refreshes.
	manager.rejoinReconcileDelay = []time.Duration{0, 5 * time.Millisecond, 5 * time.Millisecond, 5 * time.Millisecond}
	stopper := &panelStopCallbackStreamStopper{
		manager:  manager,
		started:  make(chan string, 1),
		callback: make(chan string, 1),
		ready:    make(chan string, 1),
		release:  make(chan struct{}),
		returned: make(chan string, 1),
		canceled: make(chan string, 1),
	}
	starter := &fakeStreamStarter{ch: make(chan string, 2)}
	manager.SetStreamStopper(stopper)
	manager.SetStreamStarter(starter)
	manager.SetStreamVoiceDefaults(map[string]VoiceDefaults{
		"stream-01": {GuildID: "guild-01", VoiceChannelID: "voice-01", AutoStartEnabled: true},
	})
	var refreshMu sync.Mutex
	refreshes := 0
	manager.SetAutoStartRefresher(func() error {
		refreshMu.Lock()
		refreshes++
		attempt := refreshes
		refreshMu.Unlock()
		defaults := map[string]VoiceDefaults{}
		if attempt >= 3 {
			defaults["stream-01"] = VoiceDefaults{GuildID: "guild-01", VoiceChannelID: "voice-01", AutoStartEnabled: true}
		}
		manager.SetStreamVoiceDefaults(defaults)
		return nil
	})
	if err := manager.Start(discord.VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01"}); err != nil {
		t.Fatal(err)
	}
	manager.ParticipantChanged(discord.ParticipantEvent{StreamID: "stream-01", UserID: "user-left", Present: true})
	manager.ParticipantChanged(discord.ParticipantEvent{StreamID: "stream-01", UserID: "user-left", Present: false})
	select {
	case <-stopper.callback:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for nested Control Panel stop callback")
	}
	select {
	case <-stopper.ready:
	case got := <-stopper.canceled:
		t.Fatalf("Panel callback canceled its own parent auto-stop request for %q", got)
	case <-time.After(time.Second):
		t.Fatal("auto-stop request did not remain live after nested Panel callback")
	}

	// The Bot's local stop callback has already cleared current, but the parent
	// request is still waiting for the Panel's remaining service stops. This VC
	// join must become a durable in-memory rejoin intent, not an attempt to
	// restart the source stream or a cancellation of the parent request.
	manager.VoiceUserJoined(discord.VoiceJoinEvent{GuildID: "guild-01", VoiceChannelID: "voice-01", UserID: "user-returned"})
	manager.mu.Lock()
	_, _, recorded := manager.autoStopRejoinIntentForStreamLocked("stream-01")
	manager.mu.Unlock()
	if !recorded {
		t.Fatal("VC join after Panel callback did not retain a rejoin intent")
	}
	select {
	case got := <-stopper.canceled:
		t.Fatalf("post-callback VC join canceled the parent request for %q", got)
	default:
	}

	close(stopper.release)
	select {
	case got := <-stopper.returned:
		if got != "stream-01" {
			t.Fatalf("returned auto-stop stream = %q, want stream-01", got)
		}
	case <-time.After(time.Second):
		t.Fatal("auto-stop request did not finish after the Panel dispatch completed")
	}
	select {
	case got := <-starter.ch:
		if got != "stream-01" {
			t.Fatalf("delayed rejoin started %q, want stream-01", got)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for delayed rearmed auto-start")
	}
	select {
	case got := <-starter.ch:
		t.Fatalf("delayed rejoin issued duplicate start for %q", got)
	case <-time.After(50 * time.Millisecond):
	}
	refreshMu.Lock()
	gotRefreshes := refreshes
	refreshMu.Unlock()
	if gotRefreshes < 3 {
		t.Fatalf("refresh attempts = %d, want rearmed source visibility after at least two service windows", gotRefreshes)
	}
}

func TestAutoStopRejoinPolicyCoversSequentialPanelStops(t *testing.T) {
	manager := NewManager(&fakeVoice{})
	var total time.Duration
	for _, delay := range manager.rejoinReconcileDelay {
		total += delay
	}
	// The Control Panel may wait for three five-second service calls and a
	// rearm write. Keep enough budget for more than two sequential timeouts.
	if total < 30*time.Second {
		t.Fatalf("rejoin reconciliation window = %s, want at least 30s", total)
	}
}

func TestVoiceJoinDuringCommittedAutoStopReconcilesRearmedStream(t *testing.T) {
	manager := NewManager(&fakeVoice{})
	manager.autoStopDelay = 0
	manager.rejoinReconcileDelay = []time.Duration{0, 5 * time.Millisecond, 5 * time.Millisecond}
	stopper := &commitAfterCancelStreamStopper{
		started:   make(chan string, 1),
		canceled:  make(chan string, 1),
		release:   make(chan struct{}),
		committed: make(chan string, 1),
	}
	starter := &fakeStreamStarter{ch: make(chan string, 2)}
	manager.SetStreamStopper(stopper)
	manager.SetStreamStarter(starter)
	manager.SetStreamVoiceDefaults(map[string]VoiceDefaults{
		"stream-01": {GuildID: "guild-01", VoiceChannelID: "voice-01", AutoStartEnabled: true},
	})
	refreshed := make(chan struct{}, 3)
	manager.SetAutoStartRefresher(func() error {
		manager.SetStreamVoiceDefaults(map[string]VoiceDefaults{
			// The Control Panel re-arms the completed source row. Reconciliation
			// must start that same durable stream ID after the stop commits.
			"stream-01": {GuildID: "guild-01", VoiceChannelID: "voice-01", AutoStartEnabled: true},
		})
		select {
		case refreshed <- struct{}{}:
		default:
		}
		return nil
	})
	if err := manager.Start(discord.VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01"}); err != nil {
		t.Fatal(err)
	}
	manager.ParticipantChanged(discord.ParticipantEvent{StreamID: "stream-01", UserID: "user-left", Present: true})
	manager.ParticipantChanged(discord.ParticipantEvent{StreamID: "stream-01", UserID: "user-left", Present: false})
	select {
	case got := <-stopper.started:
		if got != "stream-01" {
			t.Fatalf("in-flight stop stream = %q, want stream-01", got)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for in-flight auto-stop request")
	}

	// VoiceUserJoined has no stream ID. It must still retain a rejoin intent
	// while the old stream is current and cancel the outbound stop request.
	manager.VoiceUserJoined(discord.VoiceJoinEvent{GuildID: "guild-01", VoiceChannelID: "voice-01", UserID: "user-returned"})
	select {
	case got := <-stopper.canceled:
		if got != "stream-01" {
			t.Fatalf("canceled stream = %q, want stream-01", got)
		}
	case <-time.After(time.Second):
		t.Fatal("voice join did not cancel in-flight auto-stop request")
	}

	// Simulate a Control Panel commit that won the cancellation race: the stop
	// response returns while the Bot still has the old job, then the downstream
	// /stop callback clears it. The first reconcile must keep the intent alive.
	close(stopper.release)
	select {
	case got := <-stopper.committed:
		if got != "stream-01" {
			t.Fatalf("committed stream = %q, want stream-01", got)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for committed stop response")
	}
	select {
	case <-refreshed:
	case <-time.After(time.Second):
		t.Fatal("expected stale-stop reconciliation runtime refresh")
	}
	if err := manager.Stop("stream-01"); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-starter.ch:
		if got != "stream-01" {
			t.Fatalf("rejoined VC started %q, want rearmed stream-01", got)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for rearmed auto-start")
	}
	select {
	case got := <-starter.ch:
		t.Fatalf("rejoin reconciliation issued duplicate start for %q", got)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestVoiceJoinDuringAutoStopDoesNotRestartWhenActiveOrNoSuccessor(t *testing.T) {
	for _, tt := range []struct {
		name          string
		stopOldStream bool
		defaults      map[string]VoiceDefaults
	}{
		{
			name: "original stream remains active",
			defaults: map[string]VoiceDefaults{
				"stream-01": {GuildID: "guild-01", VoiceChannelID: "voice-01", AutoStartEnabled: true},
				"stream-02": {GuildID: "guild-01", VoiceChannelID: "voice-01", AutoStartEnabled: true},
			},
		},
		{
			name:          "no successor in refreshed config",
			stopOldStream: true,
			defaults:      map[string]VoiceDefaults{},
		},
		{
			name:          "ambiguous successors are rejected",
			stopOldStream: true,
			defaults: map[string]VoiceDefaults{
				"stream-02": {GuildID: "guild-01", VoiceChannelID: "voice-01", AutoStartEnabled: true},
				"stream-03": {GuildID: "guild-01", VoiceChannelID: "voice-01", AutoStartEnabled: true},
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			manager := NewManager(&fakeVoice{})
			manager.autoStopDelay = 0
			manager.rejoinReconcileDelay = []time.Duration{0}
			stopper := &commitAfterCancelStreamStopper{
				started:   make(chan string, 1),
				canceled:  make(chan string, 1),
				release:   make(chan struct{}),
				committed: make(chan string, 1),
			}
			starter := &fakeStreamStarter{ch: make(chan string, 1)}
			manager.SetStreamStopper(stopper)
			manager.SetStreamStarter(starter)
			manager.SetStreamVoiceDefaults(map[string]VoiceDefaults{
				"stream-01": {GuildID: "guild-01", VoiceChannelID: "voice-01", AutoStartEnabled: true},
			})
			manager.SetAutoStartRefresher(func() error {
				manager.SetStreamVoiceDefaults(tt.defaults)
				return nil
			})
			if err := manager.Start(discord.VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01"}); err != nil {
				t.Fatal(err)
			}
			manager.ParticipantChanged(discord.ParticipantEvent{StreamID: "stream-01", UserID: "user-left", Present: true})
			manager.ParticipantChanged(discord.ParticipantEvent{StreamID: "stream-01", UserID: "user-left", Present: false})
			select {
			case <-stopper.started:
			case <-time.After(time.Second):
				t.Fatal("timed out waiting for in-flight auto-stop request")
			}
			manager.VoiceUserJoined(discord.VoiceJoinEvent{GuildID: "guild-01", VoiceChannelID: "voice-01", UserID: "user-returned"})
			select {
			case <-stopper.canceled:
			case <-time.After(time.Second):
				t.Fatal("voice join did not cancel in-flight auto-stop request")
			}
			close(stopper.release)
			select {
			case <-stopper.committed:
			case <-time.After(time.Second):
				t.Fatal("timed out waiting for committed stop response")
			}
			if tt.stopOldStream {
				if err := manager.Stop("stream-01"); err != nil {
					t.Fatal(err)
				}
			}
			select {
			case got := <-starter.ch:
				t.Fatalf("unexpected reconciliation start for %q", got)
			case <-time.After(80 * time.Millisecond):
			}
		})
	}
}

func TestStartAndStopCancelScheduledAutoStop(t *testing.T) {
	t.Run("start", func(t *testing.T) {
		manager := NewManager(&fakeVoice{})
		manager.autoStopDelay = 40 * time.Millisecond
		stopper := &fakeStreamStopper{ch: make(chan string, 1)}
		manager.SetStreamStopper(stopper)
		job := discord.VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01"}
		if err := manager.Start(job); err != nil {
			t.Fatal(err)
		}
		manager.ParticipantChanged(discord.ParticipantEvent{StreamID: "stream-01", UserID: "user-01", Present: false})
		if err := manager.Start(job); err != nil {
			t.Fatal(err)
		}
		select {
		case got := <-stopper.ch:
			t.Fatalf("start should cancel stale auto-stop timer, got %q", got)
		case <-time.After(70 * time.Millisecond):
		}
	})

	t.Run("stop", func(t *testing.T) {
		manager := NewManager(&fakeVoice{})
		manager.autoStopDelay = 40 * time.Millisecond
		stopper := &fakeStreamStopper{ch: make(chan string, 1)}
		manager.SetStreamStopper(stopper)
		if err := manager.Start(discord.VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01"}); err != nil {
			t.Fatal(err)
		}
		manager.ParticipantChanged(discord.ParticipantEvent{StreamID: "stream-01", UserID: "user-01", Present: false})
		if err := manager.Stop("stream-01"); err != nil {
			t.Fatal(err)
		}
		select {
		case got := <-stopper.ch:
			t.Fatalf("stop should cancel stale auto-stop timer, got %q", got)
		case <-time.After(70 * time.Millisecond):
		}
	})
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
