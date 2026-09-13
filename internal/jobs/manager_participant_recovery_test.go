package jobs

import (
	"github.com/example/autostream-discord-bot/internal/discord"
	"testing"
	"time"
)

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

func TestPeriodicParticipantReplayCannotOverwriteNewerGatewaySnapshot(t *testing.T) {
	reporter := &fakeReporter{}
	manager := NewManagerWithReporter(&fakeVoice{}, reporter)
	job := discord.VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01"}
	if err := manager.Start(job); err != nil {
		t.Fatal(err)
	}
	manager.ParticipantsSynced(discord.ParticipantSnapshot{
		StreamID:       job.StreamID,
		GuildID:        job.GuildID,
		VoiceChannelID: job.VoiceChannelID,
		Revision:       2,
		Participants:   []discord.VoiceParticipant{{UserID: "user-new", Username: "new"}},
	})
	manager.mu.Lock()
	generation := manager.reconnectGeneration
	manager.mu.Unlock()

	manager.participantsSynced(discord.ParticipantSnapshot{
		StreamID:       job.StreamID,
		GuildID:        job.GuildID,
		VoiceChannelID: job.VoiceChannelID,
		Revision:       1,
		Participants:   []discord.VoiceParticipant{{UserID: "user-old", Username: "old"}},
	}, participantSnapshotApplyOptions{expectedGeneration: generation, requireGeneration: true, authoritativeReplay: true})
	participants, err := manager.Participants(job.StreamID)
	if err != nil {
		t.Fatal(err)
	}
	if len(participants) != 1 || participants[0].UserID != "user-new" {
		t.Fatalf("older periodic snapshot replaced newer gateway state: %#v", participants)
	}

	reporter.participants = nil
	manager.participantsSynced(discord.ParticipantSnapshot{
		StreamID:       job.StreamID,
		GuildID:        job.GuildID,
		VoiceChannelID: job.VoiceChannelID,
		Revision:       2,
		Participants:   []discord.VoiceParticipant{{UserID: "user-old", Username: "old"}},
	}, participantSnapshotApplyOptions{expectedGeneration: generation, requireGeneration: true, authoritativeReplay: true})
	if len(reporter.participants) != 1 || reporter.participants[0].UserID != "user-new" {
		t.Fatalf("same-revision replay did not publish current manager state: %#v", reporter.participants)
	}
}

func TestPeriodicSnapshotReadCannotOverwriteConcurrentParticipantEvent(t *testing.T) {
	voice := &blockingSnapshotVoice{
		snapshot: discord.ParticipantSnapshot{Revision: 1, Participants: []discord.VoiceParticipant{{UserID: "user-old", Username: "old"}}},
		started:  make(chan struct{}),
		release:  make(chan struct{}),
	}
	reporter := &recordingParticipantReporter{calls: make(chan []Participant, 8)}
	manager := NewManagerWithReporter(voice, reporter)
	manager.participantSyncDelays = []time.Duration{0}
	manager.participantSyncInterval = 0
	job := discord.VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01"}
	if err := manager.Start(job); err != nil {
		t.Fatal(err)
	}
	<-reporter.calls

	<-voice.started
	manager.ParticipantChanged(discord.ParticipantEvent{StreamID: job.StreamID, UserID: "user-new", Username: "new", Present: true})
	close(voice.release)
	time.Sleep(20 * time.Millisecond)

	participants, err := manager.Participants(job.StreamID)
	if err != nil {
		t.Fatal(err)
	}
	if len(participants) != 2 {
		t.Fatalf("stale periodic read overwrote concurrent participant event: %#v", participants)
	}
}

func TestStartRehydratesParticipantsAfterTransientInitialEmpty(t *testing.T) {
	voice := &sequenceSnapshotVoice{snapshots: []discord.ParticipantSnapshot{
		{Revision: 1},
		{Revision: 2, Participants: []discord.VoiceParticipant{{UserID: "user-01", Username: "alice"}}},
	}}
	reporter := &recordingParticipantReporter{calls: make(chan []Participant, 4)}
	manager := NewManagerWithReporter(voice, reporter)

	if err := manager.Start(discord.VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01"}); err != nil {
		t.Fatal(err)
	}

	select {
	case participants := <-reporter.calls:
		if len(participants) != 1 || participants[0].UserID != "user-01" {
			t.Fatalf("delayed authoritative snapshot was not published: %#v", participants)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for delayed participant hydration")
	}
}

func TestStartReplaysParticipantsAfterInitialWorkerPublishFailure(t *testing.T) {
	voice := &sequenceSnapshotVoice{snapshots: []discord.ParticipantSnapshot{
		{Revision: 1, Participants: []discord.VoiceParticipant{{UserID: "user-01", Username: "alice"}}},
		{Revision: 2, Participants: []discord.VoiceParticipant{{UserID: "user-01", Username: "alice"}}},
	}}
	reporter := &recordingParticipantReporter{calls: make(chan []Participant, 4), failuresRemaining: 1}
	manager := NewManagerWithReporter(voice, reporter)

	if err := manager.Start(discord.VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01"}); err != nil {
		t.Fatal(err)
	}
	<-reporter.calls

	select {
	case participants := <-reporter.calls:
		if len(participants) != 1 || participants[0].UserID != "user-01" {
			t.Fatalf("replayed snapshot was not published: %#v", participants)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for participant snapshot replay")
	}
}

func TestParticipantSnapshotRetriesHTTP409UntilWorkerAccepts(t *testing.T) {
	reporter := &recordingParticipantReporter{
		calls:             make(chan []Participant, 8),
		failuresRemaining: 2,
		failureErr:        retryableWorkerPublishError{status: 409, class: "http_status"},
	}
	manager := NewManagerWithReporter(&fakeVoice{}, reporter)
	job := discord.VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01", JobGeneration: 17}
	if err := manager.Start(job); err != nil {
		t.Fatal(err)
	}
	manager.ParticipantChanged(discord.ParticipantEvent{StreamID: job.StreamID, UserID: "user-01", Username: "alice", Present: true})

	for attempt := 0; attempt < 3; attempt++ {
		select {
		case participants := <-reporter.calls:
			if attempt == 2 && (len(participants) != 1 || participants[0].UserID != "user-01") {
				t.Fatalf("accepted participant snapshot = %#v", participants)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("timed out waiting for participant retry %d", attempt+1)
		}
	}
	if got := reporter.count(); got != 3 {
		t.Fatalf("participant publish attempts = %d, want 3", got)
	}
}

func TestWorkerEventRetryCoalescesSpeakerStopAndRestoresChat(t *testing.T) {
	reporter := &retryingOverlayReporter{speakerFails: 2, chatFails: 1}
	manager := NewManagerWithReporter(&fakeVoice{}, reporter)
	job := discord.VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01", TextChannelID: "text-01", JobGeneration: 23}
	if err := manager.Start(job); err != nil {
		t.Fatal(err)
	}
	manager.ParticipantChanged(discord.ParticipantEvent{StreamID: job.StreamID, UserID: "user-01", Username: "alice", Present: true})
	manager.ActiveSpeakerStateChanged(job.StreamID, "user-01", true)
	manager.ActiveSpeakerStateChanged(job.StreamID, "user-01", false)
	manager.ChatMessageReceived(discord.ChatMessageEvent{StreamID: job.StreamID, GuildID: job.GuildID, TextChannelID: job.TextChannelID, MessageID: "message-01", UserID: "user-01", Content: "hello"})

	deadline := time.After(3 * time.Second)
	for {
		reporter.mu.Lock()
		speakerCount := len(reporter.speaking)
		chatCount := len(reporter.chatMessages)
		lastSpeaking := false
		if speakerCount > 0 {
			lastSpeaking = reporter.speaking[speakerCount-1]
		}
		reporter.mu.Unlock()
		if speakerCount == 1 && !lastSpeaking && chatCount == 1 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("retry did not converge: speaker=%d last_speaking=%t chat=%d", speakerCount, lastSpeaking, chatCount)
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestWorkerEventRetryIsCanceledByStopAndDoesNotReachRearmedGeneration(t *testing.T) {
	reporter := &recordingParticipantReporter{
		calls:             make(chan []Participant, 8),
		failuresRemaining: 1,
		failureErr:        retryableWorkerPublishError{status: 503, class: "http_status"},
	}
	voice := &fakeVoice{}
	manager := NewManagerWithReporter(voice, reporter)
	first := discord.VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01", JobGeneration: 31}
	if err := manager.Start(first); err != nil {
		t.Fatal(err)
	}
	manager.ParticipantChanged(discord.ParticipantEvent{StreamID: first.StreamID, UserID: "old-user", Present: true})
	select {
	case <-reporter.calls:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for initial failed publish")
	}
	if err := manager.Stop(first.StreamID); err != nil {
		t.Fatal(err)
	}
	second := first
	second.JobGeneration = 32
	if err := manager.Start(second); err != nil {
		t.Fatal(err)
	}
	time.Sleep(400 * time.Millisecond)
	if got := reporter.count(); got != 1 {
		t.Fatalf("old generation retry reached rearmed job: attempts=%d", got)
	}
	if err := manager.Stop(second.StreamID); err != nil {
		t.Fatal(err)
	}
}

func TestParticipantSnapshotSyncRunsPeriodicallyAndStopsWithJobGeneration(t *testing.T) {
	voice := &sequenceSnapshotVoice{snapshots: []discord.ParticipantSnapshot{
		{Revision: 1, Participants: []discord.VoiceParticipant{{UserID: "user-01", Username: "alice"}}},
	}}
	reporter := &recordingParticipantReporter{calls: make(chan []Participant, 8)}
	manager := NewManagerWithReporter(voice, reporter)
	manager.participantSyncDelays = nil
	manager.participantSyncInterval = 10 * time.Millisecond

	if err := manager.Start(discord.VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01"}); err != nil {
		t.Fatal(err)
	}
	<-reporter.calls
	select {
	case <-reporter.calls:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for periodic participant snapshot")
	}

	if err := manager.Stop("stream-01"); err != nil {
		t.Fatal(err)
	}
	countAfterStop := reporter.count()
	time.Sleep(40 * time.Millisecond)
	if got := reporter.count(); got != countAfterStop {
		t.Fatalf("participant snapshot was published after stop: before=%d after=%d", countAfterStop, got)
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
	manager.autoStopEmptyConfirmDelay = 5 * time.Millisecond
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
