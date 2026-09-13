package discord

import (
	"testing"
)

type fakeEventSink struct {
	activeStreamID string
	activeUserID   string
	voiceJoin      VoiceJoinEvent
	voiceJoins     []VoiceJoinEvent
	participants   []ParticipantEvent
	chatMessage    ChatMessageEvent
	connectedCount int
}

func (f *fakeEventSink) VoiceUserJoined(event VoiceJoinEvent) {
	f.voiceJoin = event
	f.voiceJoins = append(f.voiceJoins, event)
}
func (f *fakeEventSink) ParticipantChanged(event ParticipantEvent) {
	f.participants = append(f.participants, event)
}
func (f *fakeEventSink) ChatMessageReceived(event ChatMessageEvent) {
	f.chatMessage = event
}
func (f *fakeEventSink) ActiveSpeakerDetected(streamID, userID string) {
	f.activeStreamID = streamID
	f.activeUserID = userID
}
func (f *fakeEventSink) DiscordConnected()          { f.connectedCount++ }
func (f *fakeEventSink) DiscordDisconnected(string) {}

type snapshotEventSink struct {
	fakeEventSink
	snapshots []ParticipantSnapshot
}

type participantStateSink struct {
	fakeEventSink
	snapshots        []ParticipantSnapshot
	participantsByID map[string]bool
}

type activeSpeakerStateSink struct {
	fakeEventSink
	speaking []bool
}

type reentrantStatusSpeakerSink struct {
	fakeEventSink
	client *RealClient
	called chan struct{}
}

type fakeVoiceSSRCResolver struct {
	users map[uint32]string
}

func (f *fakeVoiceSSRCResolver) SSRCUserID(ssrc uint32) string {
	return f.users[ssrc]
}

func (f *activeSpeakerStateSink) ActiveSpeakerStateChanged(streamID, userID string, speaking bool) {
	f.activeStreamID = streamID
	f.activeUserID = userID
	f.speaking = append(f.speaking, speaking)
}

func (f *reentrantStatusSpeakerSink) ActiveSpeakerStateChanged(streamID, userID string, speaking bool) {
	_ = f.client.Status()
	f.activeStreamID = streamID
	f.activeUserID = userID
	select {
	case f.called <- struct{}{}:
	default:
	}
}

func TestResolveSSRCUserIDFallsBackToVoiceConnectionCache(t *testing.T) {
	resolver := &fakeVoiceSSRCResolver{users: map[uint32]string{
		42: "user-01",
		84: "user-02",
	}}

	for ssrc, want := range map[uint32]string{42: "user-01", 84: "user-02"} {
		if got := resolveSSRCUserID(resolver, ssrc); got != want {
			t.Fatalf("durable voice SSRC mapping was ignored for SSRC %d: got %q, want %q", ssrc, got, want)
		}
	}
}

func (f *snapshotEventSink) ParticipantsSynced(snapshot ParticipantSnapshot) {
	f.snapshots = append(f.snapshots, snapshot)
}

func (f *participantStateSink) ParticipantChanged(event ParticipantEvent) {
	f.fakeEventSink.ParticipantChanged(event)
	if event.Present {
		f.participantsByID[event.UserID] = true
		return
	}
	delete(f.participantsByID, event.UserID)
}

func (f *participantStateSink) ParticipantsSynced(snapshot ParticipantSnapshot) {
	f.snapshots = append(f.snapshots, snapshot)
	f.participantsByID = make(map[string]bool, len(snapshot.Participants))
	for _, participant := range snapshot.Participants {
		f.participantsByID[participant.UserID] = true
	}
}
