package discord

import (
	"testing"

	"github.com/cartridge-gg/discordgo"
)

func r4Member(guild, user string, bot bool) *discordgo.Member {
	return &discordgo.Member{GuildID: guild, User: &discordgo.User{ID: user, Bot: bot}}
}

func r4IdentityClient(t *testing.T, members []*discordgo.Member, states []*discordgo.VoiceState) (*RealClient, *discordgo.Session, *fakeEventSink) {
	t.Helper()
	session := &discordgo.Session{State: discordgo.NewState(), StateEnabled: true}
	session.State.User = &discordgo.User{ID: "self"}
	if err := session.State.GuildAdd(&discordgo.Guild{ID: "guild", Members: members, VoiceStates: states}); err != nil {
		t.Fatal(err)
	}
	sink := &fakeEventSink{}
	client := &RealClient{session: session, sink: sink, status: Status{Connected: true}, autoStartTargets: []AutoStartVoiceTarget{{StreamID: "stream", GuildID: "guild", VoiceChannelID: "vc", AssignmentEpoch: "epoch"}}}
	return client, session, sink
}

func TestUI188R4GatewayRequiresVerifiedHuman(t *testing.T) {
	for _, tc := range []struct {
		name                          string
		event, cached                 *discordgo.Member
		user, channel, currentChannel string
		stale                         bool
		want                          int
	}{
		{name: "other bot event", event: r4Member("guild", "user", true)},
		{name: "event bot overrides cached human", event: r4Member("guild", "user", true), cached: r4Member("guild", "user", false)},
		{name: "cold cache genuine human", event: r4Member("guild", "user", false), want: 1},
		{name: "event human overrides cached bot", event: r4Member("guild", "user", false), cached: r4Member("guild", "user", true), want: 1},
		{name: "wire member omits guild field", event: r4Member("", "user", false), want: 1},
		{name: "missing member cached bot", cached: r4Member("guild", "user", true)},
		{name: "missing member cached human", cached: r4Member("guild", "user", false), want: 1},
		{name: "unknown member"},
		{name: "wrong event user", event: r4Member("guild", "someone-else", false), cached: r4Member("guild", "user", false)},
		{name: "wrong event guild", event: r4Member("another-guild", "user", false), cached: r4Member("guild", "user", false)},
		{name: "wrong cached guild", cached: r4Member("another-guild", "user", false)},
		{name: "self", user: "self", event: r4Member("guild", "self", false)},
		{name: "other VC", channel: "other", event: r4Member("guild", "user", false)},
		{name: "already moved", currentChannel: "other", event: r4Member("guild", "user", false)},
		{name: "old session", stale: true, event: r4Member("guild", "user", false)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			user, channel := tc.user, tc.channel
			if user == "" {
				user = "user"
			}
			if channel == "" {
				channel = "vc"
			}
			var members []*discordgo.Member
			if tc.cached != nil {
				members = append(members, tc.cached)
			}
			var states []*discordgo.VoiceState
			if tc.currentChannel != "" {
				states = append(states, &discordgo.VoiceState{GuildID: "guild", UserID: user, ChannelID: tc.currentChannel})
			}
			client, session, sink := r4IdentityClient(t, members, states)
			if tc.stale {
				client.session = &discordgo.Session{State: discordgo.NewState()}
			}
			client.onVoiceStateUpdate(session, &discordgo.VoiceStateUpdate{VoiceState: &discordgo.VoiceState{GuildID: "guild", UserID: user, ChannelID: channel, Member: tc.event}})
			if len(sink.voiceJoins) != tc.want {
				t.Fatalf("auto-start joins=%d want=%d", len(sink.voiceJoins), tc.want)
			}
			if tc.want == 1 && (sink.voiceJoin.Authority == nil || sink.voiceJoin.Authority.AssignmentEpoch != "epoch" || sink.voiceJoin.Authority.StreamID != "stream") {
				t.Fatal("current assignment authority lost")
			}
		})
	}
}

func TestUI188R4UnknownIdentityConvergesWithoutInventingHuman(t *testing.T) {
	client, session, sink := r4IdentityClient(t, nil, nil)
	event := &discordgo.VoiceStateUpdate{VoiceState: &discordgo.VoiceState{GuildID: "guild", UserID: "user", ChannelID: "vc"}}
	if err := session.State.OnInterface(session, event); err != nil {
		t.Fatal(err)
	}
	client.onVoiceStateUpdate(session, event)
	if len(sink.voiceJoins) != 0 {
		t.Fatal("unknown identity started before human information arrived")
	}
	if err := session.State.MemberAdd(r4Member("guild", "user", false)); err != nil {
		t.Fatal(err)
	}
	guild, err := session.State.Guild("guild")
	if err != nil {
		t.Fatal(err)
	}
	client.onGuildCreate(session, &discordgo.GuildCreate{Guild: guild})
	if len(sink.voiceJoins) != 1 || sink.voiceJoin.ExistingPresence {
		t.Fatal("verified delayed identity must retain the genuine new join authority")
	}
}

func TestUI188R4SnapshotUsesCurrentMemberIdentity(t *testing.T) {
	for _, tc := range []struct {
		name   string
		member *discordgo.Member
		want   int
	}{
		{"unknown", nil, 0}, {"bot", r4Member("guild", "user", true), 0},
		{"human", r4Member("guild", "user", false), 1}, {"wrong guild", r4Member("other", "user", false), 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var members []*discordgo.Member
			if tc.member != nil {
				members = append(members, tc.member)
			}
			client, session, sink := r4IdentityClient(t, members, []*discordgo.VoiceState{{GuildID: "guild", UserID: "user", ChannelID: "vc"}})
			client.onReady(session, &discordgo.Ready{})
			if len(sink.voiceJoins) != tc.want {
				t.Fatalf("snapshot joins=%d want=%d", len(sink.voiceJoins), tc.want)
			}
			if tc.want == 1 && !sink.voiceJoin.ExistingPresence {
				t.Fatal("snapshot must preserve existing-presence stop fence")
			}
		})
	}
}

func TestUI188R4DelayedMemberInformationRetainsCurrentTransitionOnly(t *testing.T) {
	for _, kind := range []string{"member add", "member update", "moved VC", "disconnected", "old assignment"} {
		t.Run(kind, func(t *testing.T) {
			client, session, sink := r4IdentityClient(t, nil, nil)
			event := &discordgo.VoiceStateUpdate{VoiceState: &discordgo.VoiceState{GuildID: "guild", UserID: "user", ChannelID: "vc"}}
			if err := session.State.OnInterface(session, event); err != nil {
				t.Fatal(err)
			}
			client.onVoiceStateUpdate(session, event)
			if len(sink.voiceJoins) != 0 {
				t.Fatal("unknown started")
			}
			if kind == "moved VC" {
				move := &discordgo.VoiceStateUpdate{VoiceState: &discordgo.VoiceState{GuildID: "guild", UserID: "user", ChannelID: "other"}, BeforeUpdate: event.VoiceState}
				if err := session.State.OnInterface(session, move); err != nil {
					t.Fatal(err)
				}
				client.onVoiceStateUpdate(session, move)
			}
			if kind == "disconnected" {
				client.onGatewayDisconnect(session, &discordgo.Disconnect{})
			}
			if kind == "old assignment" {
				client.SetAutoStartVoiceTargets([]AutoStartVoiceTarget{{StreamID: "successor", GuildID: "guild", VoiceChannelID: "vc", AssignmentEpoch: "new epoch"}})
			}
			member := r4Member("guild", "user", false)
			if kind == "member add" {
				add := &discordgo.GuildMemberAdd{Member: member}
				if err := session.State.OnInterface(session, add); err != nil {
					t.Fatal(err)
				}
				client.onAutoStartMemberAdd(session, add)
			} else {
				update := &discordgo.GuildMemberUpdate{Member: member}
				if err := session.State.OnInterface(session, update); err != nil {
					t.Fatal(err)
				}
				client.onAutoStartMemberUpdate(session, update)
			}
			if kind == "moved VC" || kind == "disconnected" {
				if len(sink.voiceJoins) != 0 {
					t.Fatal("obsolete transition started")
				}
				return
			}
			if len(sink.voiceJoins) != 1 {
				t.Fatalf("verified information joins=%d", len(sink.voiceJoins))
			}
			if kind == "old assignment" {
				if sink.voiceJoin.Authority.StreamID != "successor" || sink.voiceJoin.Authority.AssignmentEpoch != "new epoch" || !sink.voiceJoin.ExistingPresence {
					t.Fatal("old join was reused as a new-assignment transition")
				}
			} else if sink.voiceJoin.ExistingPresence {
				t.Fatal("new join was downgraded to stopped existing presence")
			}
		})
	}
}

func TestUI188R4UnknownJoinCannotCaptureReplacementEpoch(t *testing.T) {
	client, session, _ := r4IdentityClient(t, nil, nil)
	observed := append([]AutoStartVoiceTarget(nil), client.autoStartTargets...)
	client.SetAutoStartVoiceTargets([]AutoStartVoiceTarget{{StreamID: "stream", GuildID: "guild", VoiceChannelID: "vc", AssignmentEpoch: "replacement"}})
	client.rememberUnknownAutoStartJoin(session, &discordgo.VoiceState{GuildID: "guild", UserID: "user", ChannelID: "vc"}, observed)
	if len(client.unknownAutoStartJoins) != 0 {
		t.Fatal("obsolete input captured a replacement assignment epoch")
	}
}
