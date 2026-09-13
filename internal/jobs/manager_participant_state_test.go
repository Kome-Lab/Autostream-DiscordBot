package jobs

import (
	"github.com/example/autostream-discord-bot/internal/discord"
	"testing"
	"time"
)

func TestParticipantAndActiveSpeakerState(t *testing.T) {
	reporter := &fakeReporter{}
	voice := &fakeVoice{status: discord.Status{Connected: true, VoiceConnected: true, AudioForwardEnabled: true, AudioForwardActive: true, CaptionAudioForwardActive: true, AudioReceiving: true, AudioPacketsReceived: 12, AudioPacketsForwarded: 10, AudioForwardErrors: 1, AudioForwardQueueDrops: 3, CaptionPacketsForwarded: 9, CaptionForwardErrors: 2, CaptionForwardQueueDrops: 4, GatewayReconnectCount: 2, VoiceDisconnectCount: 1, DAVEInitialized: true, DAVEReady: true, DAVEWelcomeReceived: true, DAVERosterSize: 2, DAVERatchetsMissing: 1, DAVEKeyPackageResends: 2, DAVESoftResets: 3, DAVERecoveryErrors: 1}}
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
		status.Metrics["discord.audio_forward_queue_drops_total"] != 3 ||
		status.Metrics["discord.caption_packets_forwarded_total"] != 9 ||
		status.Metrics["discord.caption_forward_errors_total"] != 2 ||
		status.Metrics["discord.caption_forward_queue_drops_total"] != 4 ||
		status.Metrics["discord.worker_event_publish_failures_total"] != 0 ||
		status.Metrics["discord.reconnect_count"] != 2 ||
		status.Metrics["discord.voice_disconnect_count"] != 1 ||
		status.Metrics["discord.dave_initialized"] != 1 ||
		status.Metrics["discord.dave_ready"] != 1 ||
		status.Metrics["discord.dave_welcome_received"] != 1 ||
		status.Metrics["discord.dave_roster_size"] != 2 ||
		status.Metrics["discord.dave_ratchets_missing"] != 1 ||
		status.Metrics["discord.dave_key_package_resends_total"] != 2 ||
		status.Metrics["discord.dave_soft_resets_total"] != 3 ||
		status.Metrics["discord.dave_recovery_errors_total"] != 1 {
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
	manager.autoStopEmptyConfirmDelay = 5 * time.Millisecond
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

	voice.snapshot = discord.ParticipantSnapshot{Revision: 2}
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
	manager.autoStopEmptyConfirmDelay = 5 * time.Millisecond
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

func TestAuthoritativeEmptyAutoStopRevalidatesBeforeStop(t *testing.T) {
	voice := &sequenceSnapshotVoice{snapshots: []discord.ParticipantSnapshot{
		{Revision: 1, Participants: []discord.VoiceParticipant{{UserID: "user-01", Username: "alice"}}},
		{Revision: 3, Participants: []discord.VoiceParticipant{{UserID: "user-01", Username: "alice"}}},
	}}
	manager := NewManager(voice)
	manager.autoStopDelay = 5 * time.Millisecond
	manager.participantSyncDelays = nil
	manager.participantSyncInterval = 0
	stopper := &fakeStreamStopper{ch: make(chan string, 1)}
	manager.SetStreamStopper(stopper)
	job := discord.VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01"}
	if err := manager.Start(job); err != nil {
		t.Fatal(err)
	}

	// The first authoritative empty snapshot is a stale cache view. The
	// revalidation read returns the participant before the stop request is
	// created, so the stream must remain alive.
	manager.ParticipantsSynced(discord.ParticipantSnapshot{
		StreamID:       job.StreamID,
		GuildID:        job.GuildID,
		VoiceChannelID: job.VoiceChannelID,
		Revision:       2,
	})
	select {
	case got := <-stopper.ch:
		t.Fatalf("stale authoritative empty snapshot stopped %q", got)
	case <-time.After(50 * time.Millisecond):
	}
	participants, err := manager.Participants(job.StreamID)
	if err != nil {
		t.Fatal(err)
	}
	if len(participants) != 1 || participants[0].UserID != "user-01" {
		t.Fatalf("revalidation did not restore the participant: %#v", participants)
	}
}

func TestVoiceEventEmptyAutoStopRevalidatesBeforeStop(t *testing.T) {
	voice := &sequenceSnapshotVoice{snapshots: []discord.ParticipantSnapshot{
		{Revision: 1, Participants: []discord.VoiceParticipant{{UserID: "user-01", Username: "alice"}}},
		{Revision: 2, Participants: []discord.VoiceParticipant{{UserID: "user-01", Username: "alice"}}},
	}}
	manager := NewManager(voice)
	manager.autoStopDelay = 5 * time.Millisecond
	manager.participantSyncDelays = nil
	manager.participantSyncInterval = 0
	stopper := &fakeStreamStopper{ch: make(chan string, 1)}
	manager.SetStreamStopper(stopper)
	job := discord.VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01"}
	if err := manager.Start(job); err != nil {
		t.Fatal(err)
	}

	// A transient leave delta may race a newer gateway state. The stop path
	// must re-read the authoritative participant view even when the empty state
	// originated from a VoiceStateUpdate rather than a full snapshot.
	manager.ParticipantChanged(discord.ParticipantEvent{
		StreamID:       job.StreamID,
		GuildID:        job.GuildID,
		VoiceChannelID: job.VoiceChannelID,
		UserID:         "user-01",
		Present:        false,
	})
	select {
	case got := <-stopper.ch:
		t.Fatalf("transient voice-state leave stopped %q", got)
	case <-time.After(50 * time.Millisecond):
	}
	participants, err := manager.Participants(job.StreamID)
	if err != nil {
		t.Fatal(err)
	}
	if len(participants) != 1 || participants[0].UserID != "user-01" {
		t.Fatalf("voice-event revalidation did not restore the participant: %#v", participants)
	}
}

func TestVoiceEventEmptyAutoStopDoesNotTrustOneEmptyRevalidation(t *testing.T) {
	voice := &sequenceSnapshotVoice{snapshots: []discord.ParticipantSnapshot{
		{Revision: 1, Participants: []discord.VoiceParticipant{{UserID: "user-01", Username: "alice"}}},
		{Revision: 2},
		{Revision: 3, Participants: []discord.VoiceParticipant{{UserID: "user-01", Username: "alice"}}},
	}}
	manager := NewManager(voice)
	manager.autoStopDelay = 5 * time.Millisecond
	manager.autoStopEmptyConfirmDelay = 5 * time.Millisecond
	manager.participantSyncDelays = nil
	manager.participantSyncInterval = 0
	stopper := &fakeStreamStopper{ch: make(chan string, 1)}
	manager.SetStreamStopper(stopper)
	job := discord.VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01"}
	if err := manager.Start(job); err != nil {
		t.Fatal(err)
	}

	manager.ParticipantChanged(discord.ParticipantEvent{
		StreamID:       job.StreamID,
		GuildID:        job.GuildID,
		VoiceChannelID: job.VoiceChannelID,
		UserID:         "user-01",
		Present:        false,
	})
	waitForSequenceSnapshotReads(t, voice, 3)
	select {
	case got := <-stopper.ch:
		t.Fatalf("one transiently empty revalidation stopped %q", got)
	case <-time.After(20 * time.Millisecond):
	}
	participants, err := manager.Participants(job.StreamID)
	if err != nil {
		t.Fatal(err)
	}
	if len(participants) != 1 || participants[0].UserID != "user-01" {
		t.Fatalf("second revalidation did not restore the participant: %#v", participants)
	}
}

func TestVoiceEventPersistentEmptyAutoStopsAfterSecondRevalidation(t *testing.T) {
	voice := &sequenceSnapshotVoice{snapshots: []discord.ParticipantSnapshot{
		{Revision: 1, Participants: []discord.VoiceParticipant{{UserID: "user-01", Username: "alice"}}},
		{Revision: 2},
		{Revision: 3},
	}}
	manager := NewManager(voice)
	manager.autoStopDelay = 5 * time.Millisecond
	manager.autoStopEmptyConfirmDelay = 40 * time.Millisecond
	manager.participantSyncDelays = nil
	manager.participantSyncInterval = 0
	stopper := &fakeStreamStopper{ch: make(chan string, 1)}
	manager.SetStreamStopper(stopper)
	job := discord.VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01"}
	if err := manager.Start(job); err != nil {
		t.Fatal(err)
	}

	manager.ParticipantChanged(discord.ParticipantEvent{
		StreamID:       job.StreamID,
		GuildID:        job.GuildID,
		VoiceChannelID: job.VoiceChannelID,
		UserID:         "user-01",
		Present:        false,
	})
	waitForSequenceSnapshotReads(t, voice, 2)
	select {
	case got := <-stopper.ch:
		t.Fatalf("first empty revalidation stopped %q before the stability window", got)
	default:
	}
	select {
	case got := <-stopper.ch:
		if got != job.StreamID {
			t.Fatalf("persistent empty state stopped %q, want %q", got, job.StreamID)
		}
	case <-time.After(time.Second):
		t.Fatal("persistent empty state did not stop after the second revalidation")
	}
	if got := voice.snapshotReadCount(); got < 3 {
		t.Fatalf("auto-stop used %d snapshot reads, want at least 3", got)
	}
}
