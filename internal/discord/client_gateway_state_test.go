package discord

import (
	"bytes"
	"github.com/cartridge-gg/discordgo"
	"log"
	"strings"
	"testing"
)

func TestGatewayReconnectStatus(t *testing.T) {
	client := &RealClient{status: Status{Connected: true}}
	client.onGatewayDisconnect(nil, &discordgo.Disconnect{})
	status := client.Status()
	if status.Connected {
		t.Fatalf("expected disconnected status after gateway disconnect: %#v", status)
	}

	client.onGatewayResumed(nil, &discordgo.Resumed{})
	status = client.Status()
	if !status.Connected || status.GatewayReconnectCount != 1 {
		t.Fatalf("expected resumed status and reconnect count: %#v", status)
	}
}

func TestLogVoiceStateDiagnosticRecordsSafeTransitionFields(t *testing.T) {
	var output bytes.Buffer
	previousWriter := log.Writer()
	previousFlags := log.Flags()
	log.SetOutput(&output)
	log.SetFlags(0)
	defer func() {
		log.SetOutput(previousWriter)
		log.SetFlags(previousFlags)
	}()

	logVoiceStateDiagnostic(
		VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01", JobGeneration: 7},
		&discordgo.VoiceStateUpdate{
			VoiceState:   &discordgo.VoiceState{GuildID: "guild-01", UserID: "user-01", ChannelID: ""},
			BeforeUpdate: &discordgo.VoiceState{GuildID: "guild-01", UserID: "user-01", ChannelID: "voice-01"},
		},
		"",
		true,
		3,
		"participant_leave_published",
	)

	got := output.String()
	for _, marker := range []string{
		"event=voice_state_update",
		"stream_id=stream-01",
		"job_generation=7",
		"voice_generation=3",
		"before_channel_id=voice-01",
		"event_channel_id=",
		"current_state_known=true",
		"decision=participant_leave_published",
	} {
		if !strings.Contains(got, marker) {
			t.Fatalf("diagnostic log missing %q: %q", marker, got)
		}
	}
	for _, forbidden := range []string{"authorization", "bearer", "token", "avatar_url", "https://"} {
		if strings.Contains(strings.ToLower(got), forbidden) {
			t.Fatalf("diagnostic log contains forbidden sensitive marker %q: %q", forbidden, got)
		}
	}
}

func TestOwnVoiceStateDisconnectUpdatesStatus(t *testing.T) {
	stop := make(chan struct{})
	client := &RealClient{
		audioStop: stop,
		status: Status{
			VoiceConnected:            true,
			AudioForwardActive:        true,
			CaptionAudioForwardActive: true,
			AudioReceiving:            true,
			CurrentGuildID:            "guild-01",
			CurrentVoiceID:            "voice-01",
		},
		job: VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01"},
	}
	session := &discordgo.Session{State: discordgo.NewState()}
	session.State.User = &discordgo.User{ID: "bot-01"}

	client.onVoiceStateUpdate(session, &discordgo.VoiceStateUpdate{
		VoiceState:   &discordgo.VoiceState{UserID: "bot-01", GuildID: "guild-01", ChannelID: ""},
		BeforeUpdate: &discordgo.VoiceState{UserID: "bot-01", GuildID: "guild-01", ChannelID: "voice-01"},
	})

	select {
	case <-stop:
	default:
		t.Fatal("expected audio forward stop channel to be closed")
	}
	status := client.Status()
	if status.VoiceConnected || status.AudioForwardActive || status.CaptionAudioForwardActive || status.AudioReceiving {
		t.Fatalf("expected voice/audio status to be inactive after own voice disconnect: %#v", status)
	}
	if status.CurrentGuildID != "" || status.CurrentVoiceID != "" {
		t.Fatalf("expected current voice IDs to be cleared: %#v", status)
	}
	if status.VoiceDisconnectCount != 1 {
		t.Fatalf("expected one voice disconnect, got %#v", status)
	}
	if client.job.StreamID != "stream-01" {
		t.Fatalf("voice disconnect should preserve current job for control-plane stop handling: %#v", client.job)
	}
}

func TestVoiceStateJoinTriggersAutoStartEventWithoutActiveJob(t *testing.T) {
	sink := &fakeEventSink{}
	client := &RealClient{sink: sink}
	session := &discordgo.Session{State: discordgo.NewState()}
	session.State.User = &discordgo.User{ID: "bot-01"}

	client.onVoiceStateUpdate(session, &discordgo.VoiceStateUpdate{
		VoiceState: &discordgo.VoiceState{UserID: "user-01", GuildID: "guild-01", ChannelID: "voice-01"},
	})

	if sink.voiceJoin.GuildID != "guild-01" || sink.voiceJoin.VoiceChannelID != "voice-01" || sink.voiceJoin.UserID != "user-01" {
		t.Fatalf("voice join did not trigger auto-start event: %#v", sink.voiceJoin)
	}

	client.onVoiceStateUpdate(session, &discordgo.VoiceStateUpdate{
		VoiceState: &discordgo.VoiceState{UserID: "bot-01", GuildID: "guild-01", ChannelID: "voice-02"},
	})
	if sink.voiceJoin.VoiceChannelID != "voice-01" {
		t.Fatalf("bot's own voice join should not trigger auto-start event: %#v", sink.voiceJoin)
	}

	// A real gateway session already knows the guild even when the joining user
	// is not in its cached VoiceStates yet (and this can also happen briefly
	// during a guild rebuild). Waiting for the cache to agree would drop the
	// only event that can trigger the waiting stream.
	knownGuildSink := &fakeEventSink{}
	knownGuildClient := &RealClient{sink: knownGuildSink}
	knownGuildSession := &discordgo.Session{State: discordgo.NewState()}
	knownGuildSession.State.User = &discordgo.User{ID: "bot-01"}
	if err := knownGuildSession.State.GuildAdd(&discordgo.Guild{ID: "guild-01"}); err != nil {
		t.Fatal(err)
	}
	knownGuildClient.onVoiceStateUpdate(knownGuildSession, &discordgo.VoiceStateUpdate{
		VoiceState: &discordgo.VoiceState{UserID: "user-01", GuildID: "guild-01", ChannelID: "voice-01"},
	})
	if knownGuildSink.voiceJoin.UserID != "user-01" {
		t.Fatalf("join in a known guild with a cold VoiceStates cache must trigger auto-start: %#v", knownGuildSink.voiceJoin)
	}
}

func TestVoiceStateUpdateIgnoresHandlersThatLostGatewayOrder(t *testing.T) {
	newSession := func(t *testing.T, initialVoiceStates []*discordgo.VoiceState) *discordgo.Session {
		t.Helper()
		session := &discordgo.Session{State: discordgo.NewState(), StateEnabled: true}
		session.State.User = &discordgo.User{ID: "bot-01"}
		if err := session.State.GuildAdd(&discordgo.Guild{ID: "guild-01", VoiceStates: initialVoiceStates}); err != nil {
			t.Fatalf("add guild state: %v", err)
		}
		return session
	}
	apply := func(t *testing.T, session *discordgo.Session, event *discordgo.VoiceStateUpdate) {
		t.Helper()
		if err := session.State.OnInterface(session, event); err != nil {
			t.Fatalf("apply gateway voice state: %v", err)
		}
	}

	t.Run("ultimately empty ignores delayed join", func(t *testing.T) {
		sink := &fakeEventSink{}
		client := &RealClient{
			sink: sink,
			job:  VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01"},
		}
		session := newSession(t, nil)
		joined := &discordgo.VoiceStateUpdate{VoiceState: &discordgo.VoiceState{UserID: "user-01", GuildID: "guild-01", ChannelID: "voice-01"}}
		left := &discordgo.VoiceStateUpdate{VoiceState: &discordgo.VoiceState{UserID: "user-01", GuildID: "guild-01", ChannelID: ""}}
		apply(t, session, joined)
		apply(t, session, left)

		// discordgo updates State in gateway order before it runs handlers, but
		// handlers are asynchronous by default. Model the leave handler running
		// first and the stale join handler running later.
		client.onVoiceStateUpdate(session, left)
		client.onVoiceStateUpdate(session, joined)

		if len(sink.participants) != 1 || sink.participants[0].Present || sink.participants[0].UserID != "user-01" {
			t.Fatalf("empty final state must publish only the leave, got %#v", sink.participants)
		}
		if sink.voiceJoin.UserID != "" {
			t.Fatalf("delayed join must not cancel empty-channel auto-stop: %#v", sink.voiceJoin)
		}
	})

	t.Run("ultimately occupied ignores delayed leave", func(t *testing.T) {
		sink := &fakeEventSink{}
		client := &RealClient{
			sink: sink,
			job:  VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01"},
		}
		session := newSession(t, []*discordgo.VoiceState{{UserID: "user-01", GuildID: "guild-01", ChannelID: "voice-01"}})
		left := &discordgo.VoiceStateUpdate{VoiceState: &discordgo.VoiceState{UserID: "user-01", GuildID: "guild-01", ChannelID: ""}}
		joined := &discordgo.VoiceStateUpdate{VoiceState: &discordgo.VoiceState{UserID: "user-01", GuildID: "guild-01", ChannelID: "voice-01"}}
		apply(t, session, left)
		apply(t, session, joined)

		client.onVoiceStateUpdate(session, joined)
		client.onVoiceStateUpdate(session, left)

		if len(sink.participants) != 1 || !sink.participants[0].Present || sink.participants[0].UserID != "user-01" {
			t.Fatalf("occupied final state must publish only the join, got %#v", sink.participants)
		}
		if sink.voiceJoin.UserID != "user-01" {
			t.Fatalf("current join must still cancel empty-channel auto-stop: %#v", sink.voiceJoin)
		}
	})

	t.Run("move away then leave elsewhere still removes target participant", func(t *testing.T) {
		sink := &fakeEventSink{}
		client := &RealClient{
			sink: sink,
			job:  VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01"},
		}
		session := newSession(t, []*discordgo.VoiceState{{UserID: "user-01", GuildID: "guild-01", ChannelID: "voice-01"}})
		movedAway := &discordgo.VoiceStateUpdate{VoiceState: &discordgo.VoiceState{UserID: "user-01", GuildID: "guild-01", ChannelID: "voice-02"}}
		leftElsewhere := &discordgo.VoiceStateUpdate{VoiceState: &discordgo.VoiceState{UserID: "user-01", GuildID: "guild-01", ChannelID: ""}}
		apply(t, session, movedAway)
		apply(t, session, leftElsewhere)

		// The later leave from voice-02 is not itself a target-VC leave. The
		// delayed move is the only event that can remove this user from voice-01.
		client.onVoiceStateUpdate(session, leftElsewhere)
		client.onVoiceStateUpdate(session, movedAway)

		if len(sink.participants) != 1 || sink.participants[0].Present || sink.participants[0].UserID != "user-01" {
			t.Fatalf("move away must still remove the target participant, got %#v", sink.participants)
		}
		if sink.voiceJoin.UserID != "" {
			t.Fatalf("stale move must not create a new join signal: %#v", sink.voiceJoin)
		}
	})
}
