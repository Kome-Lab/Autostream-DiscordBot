package discord

import (
	"github.com/cartridge-gg/discordgo"
	"testing"
)

func TestVoiceParticipantSnapshotsHydrateAndRecoverAuthoritatively(t *testing.T) {
	session := &discordgo.Session{State: discordgo.NewState(), StateEnabled: true}
	session.State.User = &discordgo.User{ID: "bot-01"}
	if err := session.State.GuildAdd(&discordgo.Guild{
		ID: "guild-01",
		VoiceStates: []*discordgo.VoiceState{
			{UserID: "bot-01", GuildID: "guild-01", ChannelID: "voice-01"},
			{UserID: "user-a", GuildID: "guild-01", ChannelID: "voice-01", Member: &discordgo.Member{User: &discordgo.User{ID: "user-a", Username: "Alice"}}},
			{UserID: "user-b", GuildID: "guild-01", ChannelID: "voice-01", Member: &discordgo.Member{User: &discordgo.User{ID: "user-b", Username: "Bob"}}},
		},
	}); err != nil {
		t.Fatal(err)
	}
	job := VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01", JobGeneration: 7}
	sink := &snapshotEventSink{}
	client := &RealClient{session: session, sink: sink, job: job}

	staleJob := job
	staleJob.JobGeneration = 6
	if snapshot, known := client.SnapshotVoiceParticipants(staleJob); known {
		t.Fatalf("old job generation received current participant snapshot: %#v", snapshot)
	}

	initial, known := client.SnapshotVoiceParticipants(job)
	if !known || initial.Revision != 1 || len(initial.Participants) != 2 || initial.Participants[0].UserID != "user-a" || initial.Participants[1].UserID != "user-b" {
		t.Fatalf("start snapshot must include only existing humans, got %#v known=%t", initial, known)
	}

	left := &discordgo.VoiceStateUpdate{VoiceState: &discordgo.VoiceState{UserID: "user-b", GuildID: "guild-01", ChannelID: ""}}
	if err := session.State.OnInterface(session, left); err != nil {
		t.Fatal(err)
	}
	client.onGatewayResumed(session, &discordgo.Resumed{})
	if len(sink.snapshots) != 1 || sink.snapshots[0].Revision != 2 || len(sink.snapshots[0].Participants) != 1 || sink.snapshots[0].Participants[0].UserID != "user-a" {
		t.Fatalf("resume must replace state with the current authoritative VC members: %#v", sink.snapshots)
	}

	left = &discordgo.VoiceStateUpdate{VoiceState: &discordgo.VoiceState{UserID: "user-a", GuildID: "guild-01", ChannelID: ""}}
	if err := session.State.OnInterface(session, left); err != nil {
		t.Fatal(err)
	}
	client.onGuildCreate(session, &discordgo.GuildCreate{Guild: &discordgo.Guild{ID: "guild-01"}})
	if len(sink.snapshots) != 2 || sink.snapshots[1].Revision != 3 || len(sink.snapshots[1].Participants) != 0 {
		t.Fatalf("full reconnect guild sync must publish an authoritative empty VC: %#v", sink.snapshots)
	}
}

func TestAutoStartPresenceSyncEmitsExistingHumanParticipant(t *testing.T) {
	session := &discordgo.Session{State: discordgo.NewState(), StateEnabled: true}
	session.State.TrackVoice = true
	session.State.User = &discordgo.User{ID: "bot-01", Bot: true}
	if err := session.State.GuildAdd(&discordgo.Guild{
		ID: "guild-01",
		VoiceStates: []*discordgo.VoiceState{
			{UserID: "bot-01", GuildID: "guild-01", ChannelID: "voice-01", Member: &discordgo.Member{User: &discordgo.User{ID: "bot-01", Bot: true}}},
			{UserID: "bot-other", GuildID: "guild-01", ChannelID: "voice-01", Member: &discordgo.Member{User: &discordgo.User{ID: "bot-other", Bot: true}}},
			{UserID: "user-01", GuildID: "guild-01", ChannelID: "voice-01", Member: &discordgo.Member{Nick: "Alice", User: &discordgo.User{ID: "user-01", Username: "alice"}}},
		},
	}); err != nil {
		t.Fatal(err)
	}
	sink := &fakeEventSink{}
	client := &RealClient{session: session, sink: sink}

	client.SetAutoStartVoiceTargets([]AutoStartVoiceTarget{{
		StreamID:       "stream-01",
		GuildID:        "guild-01",
		VoiceChannelID: "voice-01",
	}})

	if len(sink.voiceJoins) != 1 {
		t.Fatalf("expected one synthetic auto-start join, got %#v", sink.voiceJoins)
	}
	if got := sink.voiceJoins[0]; got.GuildID != "guild-01" || got.VoiceChannelID != "voice-01" || got.UserID != "user-01" || got.Username != "Alice" {
		t.Fatalf("unexpected existing participant join event: %#v", got)
	}
}

func TestVoiceParticipantSnapshotUsesDiscordDisplayNameAndGuildAvatar(t *testing.T) {
	session := &discordgo.Session{State: discordgo.NewState(), StateEnabled: true}
	session.State.User = &discordgo.User{ID: "bot-self"}
	guild := &discordgo.Guild{
		ID: "guild-01",
		VoiceStates: []*discordgo.VoiceState{
			{UserID: "bot-self", GuildID: "guild-01", ChannelID: "voice-01", Member: &discordgo.Member{GuildID: "guild-01", User: &discordgo.User{ID: "bot-self", Username: "self", Bot: true}}},
			{UserID: "user-nick", GuildID: "guild-01", ChannelID: "voice-01", Member: &discordgo.Member{Nick: "Server Alice", Avatar: "guild-avatar", User: &discordgo.User{ID: "user-nick", Username: "alice", GlobalName: "Global Alice", Avatar: "user-avatar"}}},
			{UserID: "user-global", GuildID: "guild-01", ChannelID: "voice-01", Member: &discordgo.Member{GuildID: "guild-01", User: &discordgo.User{ID: "user-global", Username: "bob", GlobalName: "Global Bob", Avatar: "bob-avatar"}}},
			{UserID: "bot-other", GuildID: "guild-01", ChannelID: "voice-01", Member: &discordgo.Member{GuildID: "guild-01", User: &discordgo.User{ID: "bot-other", Username: "Helper", Bot: true}}},
		},
	}
	if err := session.State.GuildAdd(guild); err != nil {
		t.Fatal(err)
	}
	client := &RealClient{session: session, job: VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01"}}

	snapshot, known := client.SnapshotVoiceParticipants(client.job)
	if !known {
		t.Fatal("participant snapshot was not available")
	}
	participants := make(map[string]VoiceParticipant, len(snapshot.Participants))
	for _, participant := range snapshot.Participants {
		participants[participant.UserID] = participant
	}
	if _, present := participants["bot-self"]; present {
		t.Fatal("the connected bot itself must not be included")
	}
	if other := participants["bot-other"]; !other.IsBot || other.Username != "Helper" {
		t.Fatalf("another bot must remain visible, got %#v", other)
	}
	expectedGuildMember := *guild.VoiceStates[1].Member
	expectedGuildMember.GuildID = guild.ID
	if nick := participants["user-nick"]; nick.Username != "Server Alice" || nick.AvatarURL != expectedGuildMember.AvatarURL("128") {
		t.Fatalf("guild nickname/avatar must win, got %#v", nick)
	}
	if global := participants["user-global"]; global.Username != "Global Bob" || global.AvatarURL != guild.VoiceStates[2].Member.User.AvatarURL("128") {
		t.Fatalf("global name and user avatar must be fallbacks, got %#v", global)
	}
}

func TestUniqueHumanVoiceParticipantProvidesCaptionIdentityFallback(t *testing.T) {
	session := &discordgo.Session{State: discordgo.NewState(), StateEnabled: true}
	session.State.User = &discordgo.User{ID: "bot-self"}
	if err := session.State.GuildAdd(&discordgo.Guild{
		ID: "guild-01",
		VoiceStates: []*discordgo.VoiceState{
			{UserID: "bot-self", GuildID: "guild-01", ChannelID: "voice-01"},
			{UserID: "user-01", GuildID: "guild-01", ChannelID: "voice-01"},
			{UserID: "bot-other", GuildID: "guild-01", ChannelID: "voice-01", Member: &discordgo.Member{User: &discordgo.User{ID: "bot-other", Bot: true}}},
		},
	}); err != nil {
		t.Fatal(err)
	}
	job := VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01"}
	client := &RealClient{session: session, job: job}
	if got := client.uniqueHumanVoiceParticipant(job); got != "user-01" {
		t.Fatalf("expected the only human participant as fallback, got %q", got)
	}

	if err := session.State.OnInterface(session, &discordgo.VoiceStateUpdate{VoiceState: &discordgo.VoiceState{UserID: "user-02", GuildID: "guild-01", ChannelID: "voice-01"}}); err != nil {
		t.Fatal(err)
	}
	if got := client.uniqueHumanVoiceParticipant(job); got != "" {
		t.Fatalf("fallback must remain empty when multiple humans are present, got %q", got)
	}
}

func TestReadyRestartsConnectedStateAndParticipantSnapshot(t *testing.T) {
	session := &discordgo.Session{State: discordgo.NewState(), StateEnabled: true}
	session.State.User = &discordgo.User{ID: "bot-01"}
	if err := session.State.GuildAdd(&discordgo.Guild{
		ID: "guild-01",
		VoiceStates: []*discordgo.VoiceState{
			{UserID: "bot-01", GuildID: "guild-01", ChannelID: "voice-01"},
			{UserID: "user-01", GuildID: "guild-01", ChannelID: "voice-01"},
		},
	}); err != nil {
		t.Fatal(err)
	}
	sink := &snapshotEventSink{}
	client := &RealClient{
		session: session,
		sink:    sink,
		job:     VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01"},
	}

	client.onReady(session, &discordgo.Ready{})
	if sink.connectedCount != 1 {
		t.Fatalf("READY connected notifications = %d, want 1", sink.connectedCount)
	}
	if len(sink.snapshots) != 1 || len(sink.snapshots[0].Participants) != 1 || sink.snapshots[0].Participants[0].UserID != "user-01" {
		t.Fatalf("READY participant snapshot = %#v", sink.snapshots)
	}
}

func TestVoiceStateUpdateUsesDeltaForSnapshotAwareSink(t *testing.T) {
	session := &discordgo.Session{State: discordgo.NewState(), StateEnabled: true}
	session.State.User = &discordgo.User{ID: "bot-01"}
	if err := session.State.GuildAdd(&discordgo.Guild{ID: "guild-01", VoiceStates: []*discordgo.VoiceState{
		{UserID: "user-01", GuildID: "guild-01", ChannelID: "voice-01"},
	}}); err != nil {
		t.Fatal(err)
	}
	job := VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01"}
	sink := &snapshotEventSink{}
	client := &RealClient{session: session, sink: sink, job: job}
	left := &discordgo.VoiceStateUpdate{VoiceState: &discordgo.VoiceState{UserID: "user-01", GuildID: "guild-01", ChannelID: ""}}
	if err := session.State.OnInterface(session, left); err != nil {
		t.Fatal(err)
	}
	client.onVoiceStateUpdate(session, left)

	if len(sink.snapshots) != 0 {
		t.Fatalf("target VC leave must not replace the full participant set: %#v", sink.snapshots)
	}
	if len(sink.participants) != 1 || sink.participants[0].UserID != "user-01" || sink.participants[0].Present {
		t.Fatalf("snapshot-aware sink must receive the departed user's delta: %#v", sink.participants)
	}
}

func TestDelayedTargetLeaveDeltaPreservesCurrentParticipant(t *testing.T) {
	session := &discordgo.Session{State: discordgo.NewState(), StateEnabled: true}
	session.State.User = &discordgo.User{ID: "bot-01"}
	if err := session.State.GuildAdd(&discordgo.Guild{ID: "guild-01", VoiceStates: []*discordgo.VoiceState{
		{UserID: "bot-01", GuildID: "guild-01", ChannelID: "voice-01"},
		{UserID: "user-current", GuildID: "guild-01", ChannelID: "voice-01"},
	}}); err != nil {
		t.Fatal(err)
	}
	job := VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01"}
	sink := &participantStateSink{participantsByID: map[string]bool{"user-current": true}}
	client := &RealClient{session: session, sink: sink, job: job}

	// DiscordGo applies State before it asynchronously invokes typed handlers.
	// This leave belongs to a prior job/state epoch: the departed user is no
	// longer in State, while the current job's participant remains in the VC.
	delayedLeave := &discordgo.VoiceStateUpdate{
		VoiceState:   &discordgo.VoiceState{UserID: "user-old", GuildID: "guild-01", ChannelID: ""},
		BeforeUpdate: &discordgo.VoiceState{UserID: "user-old", GuildID: "guild-01", ChannelID: "voice-01"},
	}
	client.onVoiceStateUpdate(session, delayedLeave)

	if !sink.participantsByID["user-current"] {
		t.Fatalf("delayed leave erased the current participant: %#v", sink.participantsByID)
	}
	if len(sink.snapshots) != 0 {
		t.Fatalf("delayed leave must not replace the full participant set: %#v", sink.snapshots)
	}
	if len(sink.participants) != 1 || sink.participants[0].UserID != "user-old" || sink.participants[0].Present {
		t.Fatalf("delayed leave must emit only the departed user's delta: %#v", sink.participants)
	}
}

func TestVoiceStateUpdateDoesNotClearOtherParticipantsWhenStateCacheIsIncomplete(t *testing.T) {
	session := &discordgo.Session{State: discordgo.NewState(), StateEnabled: true}
	session.State.TrackVoice = true
	session.State.User = &discordgo.User{ID: "bot-01"}
	if err := session.State.GuildAdd(&discordgo.Guild{ID: "guild-01"}); err != nil {
		t.Fatal(err)
	}
	job := VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01"}
	sink := &participantStateSink{participantsByID: map[string]bool{"user-current": true}}
	client := &RealClient{session: session, sink: sink, job: job}

	// During a guild-state rebuild, Discord can dispatch one user's target-VC
	// transition while the cached VoiceStates slice temporarily omits another
	// participant. The event is authoritative only for user-old; replacing the
	// whole participant set from that incomplete cache would erase user-current.
	client.onVoiceStateUpdate(session, &discordgo.VoiceStateUpdate{
		VoiceState:   &discordgo.VoiceState{UserID: "user-old", GuildID: "guild-01", ChannelID: ""},
		BeforeUpdate: &discordgo.VoiceState{UserID: "user-old", GuildID: "guild-01", ChannelID: "voice-01"},
	})

	if !sink.participantsByID["user-current"] {
		t.Fatalf("single-user transition erased another participant: %#v", sink.participantsByID)
	}
	if len(sink.snapshots) != 0 {
		t.Fatalf("single-user transition must not publish a full cache snapshot: %#v", sink.snapshots)
	}
	if len(sink.participants) != 1 || sink.participants[0].UserID != "user-old" || sink.participants[0].Present {
		t.Fatalf("expected only the departed user's delta, got %#v", sink.participants)
	}
}

func TestOwnVoiceStateJoinDoesNotBecomeParticipant(t *testing.T) {
	sink := &fakeEventSink{}
	client := &RealClient{
		sink: sink,
		job:  VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01"},
	}
	session := &discordgo.Session{State: discordgo.NewState()}
	session.State.User = &discordgo.User{ID: "bot-01"}

	client.onVoiceStateUpdate(session, &discordgo.VoiceStateUpdate{
		VoiceState: &discordgo.VoiceState{UserID: "bot-01", GuildID: "guild-01", ChannelID: "voice-01"},
	})
	if len(sink.participants) != 0 {
		t.Fatalf("bot voice join must not be reported as a participant: %#v", sink.participants)
	}

	client.onVoiceStateUpdate(session, &discordgo.VoiceStateUpdate{
		VoiceState: &discordgo.VoiceState{UserID: "user-01", GuildID: "guild-01", ChannelID: "voice-01"},
	})
	if len(sink.participants) != 1 || sink.participants[0].UserID != "user-01" || !sink.participants[0].Present {
		t.Fatalf("human voice join was not reported: %#v", sink.participants)
	}
}

func TestMessageCreatePublishesOnlyActiveTextChannelMessages(t *testing.T) {
	sink := &fakeEventSink{}
	client := &RealClient{
		sink: sink,
		job: VoiceJob{
			StreamID:       "stream-01",
			GuildID:        "guild-01",
			VoiceChannelID: "voice-01",
			TextChannelID:  "text-01",
		},
	}
	session := &discordgo.Session{State: discordgo.NewState()}
	session.State.User = &discordgo.User{ID: "bot-01"}

	client.onMessageCreate(session, &discordgo.MessageCreate{Message: &discordgo.Message{
		ID:        "msg-ignored",
		GuildID:   "guild-01",
		ChannelID: "text-other",
		Author:    &discordgo.User{ID: "user-01", Username: "alice"},
		Content:   "wrong channel",
	}})
	if sink.chatMessage.MessageID != "" {
		t.Fatalf("message from another channel should be ignored: %#v", sink.chatMessage)
	}

	client.onMessageCreate(session, &discordgo.MessageCreate{Message: &discordgo.Message{
		ID:        "msg-01",
		GuildID:   "guild-01",
		ChannelID: "text-01",
		Author:    &discordgo.User{ID: "user-01", Username: "alice"},
		Content:   " 本番開始します ",
	}})
	if sink.chatMessage.StreamID != "stream-01" || sink.chatMessage.TextChannelID != "text-01" || sink.chatMessage.UserID != "user-01" || sink.chatMessage.Username != "alice" || sink.chatMessage.Content != "本番開始します" {
		t.Fatalf("active text channel message was not published: %#v", sink.chatMessage)
	}

	client.onMessageCreate(session, &discordgo.MessageCreate{Message: &discordgo.Message{
		ID:        "msg-bot",
		GuildID:   "guild-01",
		ChannelID: "text-01",
		Author:    &discordgo.User{ID: "bot-other", Username: "helper", GlobalName: "Helper Bot", Avatar: "avatar-hash", Bot: true},
		Content:   "bot message",
	}})
	if sink.chatMessage.MessageID != "msg-bot" || sink.chatMessage.UserID != "bot-other" || sink.chatMessage.Username != "Helper Bot" || sink.chatMessage.AvatarURL == "" || !sink.chatMessage.IsBot {
		t.Fatalf("another bot's message should be published: %#v", sink.chatMessage)
	}

	client.onMessageCreate(session, &discordgo.MessageCreate{Message: &discordgo.Message{
		ID:        "msg-self",
		GuildID:   "guild-01",
		ChannelID: "text-01",
		Author:    &discordgo.User{ID: "bot-01", Username: "self", Bot: true},
		Content:   "self message",
	}})
	if sink.chatMessage.MessageID != "msg-bot" {
		t.Fatalf("bot's own message should be ignored: %#v", sink.chatMessage)
	}
}
