package jobs

import (
	"github.com/example/autostream-discord-bot/internal/discord"
	"sync"
	"testing"
	"time"
)

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

func TestActiveSpeakerStateChangedClearsOnlyTheStoppedSpeaker(t *testing.T) {
	reporter := &activeSpeakerStateReporter{}
	manager := NewManagerWithReporter(&fakeVoice{}, reporter)
	if err := manager.Start(discord.VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01"}); err != nil {
		t.Fatal(err)
	}
	manager.ParticipantChanged(discord.ParticipantEvent{StreamID: "stream-01", UserID: "user-01", Username: "alice", Present: true})
	manager.ParticipantChanged(discord.ParticipantEvent{StreamID: "stream-01", UserID: "user-02", Username: "bob", Present: true})
	manager.ActiveSpeakerStateChanged("stream-01", "user-01", true)
	manager.ActiveSpeakerStateChanged("stream-01", "user-02", false)
	if got := manager.Status().ActiveSpeakerID; got != "user-01" {
		t.Fatalf("stopping a different participant cleared active speaker: %q", got)
	}
	manager.ActiveSpeakerStateChanged("stream-01", "user-01", false)
	if got := manager.Status().ActiveSpeakerID; got != "" {
		t.Fatalf("stopping active participant did not clear speaker: %q", got)
	}
	if len(reporter.speaking) != 2 || reporter.speaking[0] != true || reporter.speaking[1] != false {
		t.Fatalf("unexpected speaker state reports: %#v", reporter.speaking)
	}
}

func TestActiveSpeakerStateChangedKeepsMultipleSpeakersHighlighted(t *testing.T) {
	manager := NewManagerWithReporter(&fakeVoice{}, &fakeReporter{})
	job := discord.VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01"}
	if err := manager.Start(job); err != nil {
		t.Fatal(err)
	}
	manager.ParticipantChanged(discord.ParticipantEvent{StreamID: job.StreamID, UserID: "user-01", Username: "Alice", Present: true})
	manager.ParticipantChanged(discord.ParticipantEvent{StreamID: job.StreamID, UserID: "user-02", Username: "Bob", Present: true})

	manager.ActiveSpeakerStateChanged(job.StreamID, "user-01", true)
	manager.ActiveSpeakerStateChanged(job.StreamID, "user-02", true)
	participants, err := manager.Participants(job.StreamID)
	if err != nil {
		t.Fatal(err)
	}
	speaking := map[string]bool{}
	for _, participant := range participants {
		speaking[participant.UserID] = participant.Speaking
	}
	if !speaking["user-01"] || !speaking["user-02"] {
		t.Fatalf("simultaneous speakers = %#v, want both highlighted", speaking)
	}

	manager.ActiveSpeakerStateChanged(job.StreamID, "user-01", false)
	participants, err = manager.Participants(job.StreamID)
	if err != nil {
		t.Fatal(err)
	}
	speaking = map[string]bool{}
	for _, participant := range participants {
		speaking[participant.UserID] = participant.Speaking
	}
	if speaking["user-01"] || !speaking["user-02"] {
		t.Fatalf("speaker stop state = %#v, want only user-02 highlighted", speaking)
	}
}

func TestConcurrentSpeakerEdgesPublishAuthoritativeParticipantState(t *testing.T) {
	reporter := &recordingParticipantReporter{calls: make(chan []Participant, 8)}
	manager := NewManagerWithReporter(&fakeVoice{}, reporter)
	job := discord.VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01"}
	if err := manager.Start(job); err != nil {
		t.Fatal(err)
	}
	manager.ParticipantChanged(discord.ParticipantEvent{StreamID: job.StreamID, UserID: "user-01", Username: "Alice", Present: true})
	manager.ParticipantChanged(discord.ParticipantEvent{StreamID: job.StreamID, UserID: "user-02", Username: "Bob", Present: true})
	for len(reporter.calls) > 0 {
		<-reporter.calls
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		manager.ActiveSpeakerStateChanged(job.StreamID, "user-01", true)
	}()
	go func() {
		defer wg.Done()
		manager.ActiveSpeakerStateChanged(job.StreamID, "user-02", true)
	}()
	wg.Wait()

	var latest []Participant
	for len(reporter.calls) > 0 {
		latest = <-reporter.calls
	}
	speaking := map[string]bool{}
	for _, participant := range latest {
		speaking[participant.UserID] = participant.Speaking
	}
	if !speaking["user-01"] || !speaking["user-02"] {
		t.Fatalf("published participant state = %#v, want both speakers highlighted", speaking)
	}
}

func TestClearActiveSpeakerPublishesAuthoritativeClear(t *testing.T) {
	reporter := &activeSpeakerStateReporter{}
	manager := NewManagerWithReporter(&fakeVoice{}, reporter)
	job := discord.VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01"}
	if err := manager.Start(job); err != nil {
		t.Fatal(err)
	}
	manager.ParticipantChanged(discord.ParticipantEvent{StreamID: job.StreamID, UserID: "user-01", Username: "Alice", Present: true})
	manager.ActiveSpeakerStateChanged(job.StreamID, "user-01", true)

	if err := manager.SetActiveSpeaker(job.StreamID, ""); err != nil {
		t.Fatal(err)
	}
	if len(reporter.participants) != 1 || reporter.participants[0].Speaking {
		t.Fatalf("clear did not publish authoritative participant state: %#v", reporter.participants)
	}
	if got := reporter.speaking; len(got) != 2 || !got[0] || got[1] {
		t.Fatalf("clear speaking edges = %#v, want [true false]", got)
	}
}

func TestParticipantSnapshotPublishCannotArriveAfterNewSpeakerEvent(t *testing.T) {
	reporter := &orderedOverlayReporter{
		participantStarted: make(chan struct{}),
		participantRelease: make(chan struct{}),
	}
	manager := NewManagerWithReporter(&fakeVoice{}, reporter)
	job := discord.VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01"}
	if err := manager.Start(job); err != nil {
		t.Fatal(err)
	}
	participantDone := make(chan struct{})
	go func() {
		manager.ParticipantChanged(discord.ParticipantEvent{StreamID: job.StreamID, UserID: "user-01", Username: "alice", Present: true})
		close(participantDone)
	}()
	<-reporter.participantStarted
	speakerDone := make(chan struct{})
	go func() {
		manager.ActiveSpeakerStateChanged(job.StreamID, "user-01", true)
		close(speakerDone)
	}()
	close(reporter.participantRelease)
	<-participantDone
	<-speakerDone

	reporter.mu.Lock()
	order := append([]string(nil), reporter.order...)
	reporter.mu.Unlock()
	if len(order) != 3 || order[0] != "participants" || order[1] != "participants" || order[2] != "speaker" {
		t.Fatalf("overlay event order = %#v, want initial participants then authoritative speaker snapshot and edge", order)
	}
}

func TestOvertakenParticipantReportPublishesLatestSpeakerState(t *testing.T) {
	reporter := &fakeReporter{}
	manager := NewManagerWithReporter(&fakeVoice{}, reporter)
	job := discord.VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01"}
	if err := manager.Start(job); err != nil {
		t.Fatal(err)
	}
	manager.ParticipantChanged(discord.ParticipantEvent{StreamID: job.StreamID, UserID: "user-01", Username: "alice", Present: true})
	manager.mu.Lock()
	staleParticipants := manager.participantsSnapshotLocked()
	staleRevision := manager.participantStateRevision
	manager.mu.Unlock()
	reporter.participants = nil

	manager.ActiveSpeakerStateChanged(job.StreamID, "user-01", true)
	manager.mu.Lock()
	generation := manager.reconnectGeneration
	manager.mu.Unlock()
	manager.reportParticipantsIfCurrent(job, staleParticipants, staleRevision, generation, true)

	if len(reporter.participants) != 1 || reporter.participants[0].UserID != "user-01" || !reporter.participants[0].Speaking {
		t.Fatalf("overtaken participant report did not publish latest speaking state: %#v", reporter.participants)
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

func TestGatewayReconnectRestartsPeriodicParticipantSnapshotSync(t *testing.T) {
	voice := &sequenceSnapshotVoice{snapshots: []discord.ParticipantSnapshot{
		{Revision: 1, Participants: []discord.VoiceParticipant{{UserID: "user-01", Username: "alice"}}},
	}}
	reporter := &recordingParticipantReporter{calls: make(chan []Participant, 16)}
	manager := NewManagerWithReporter(voice, reporter)
	manager.participantSyncDelays = nil
	manager.participantSyncInterval = 10 * time.Millisecond
	if err := manager.Start(discord.VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01"}); err != nil {
		t.Fatal(err)
	}
	<-reporter.calls

	manager.DiscordDisconnected("gateway_disconnect")
	time.Sleep(30 * time.Millisecond)
	for {
		select {
		case <-reporter.calls:
			continue
		default:
			goto drained
		}
	}

drained:
	countBeforeReconnect := reporter.count()
	manager.DiscordConnected()
	select {
	case <-reporter.calls:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for participant snapshot after gateway reconnect")
	}
	if got := reporter.count(); got <= countBeforeReconnect {
		t.Fatalf("gateway reconnect did not restart participant sync: before=%d after=%d", countBeforeReconnect, got)
	}
	if err := manager.Stop("stream-01"); err != nil {
		t.Fatal(err)
	}
}
