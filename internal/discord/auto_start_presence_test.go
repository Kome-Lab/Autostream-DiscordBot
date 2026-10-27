package discord

import (
	"testing"

	"github.com/cartridge-gg/discordgo"
)

func TestAutoStartVoicePresenceUsesCurrentSelectedHuman(t *testing.T) {
	for _, tc := range []struct {
		name    string
		members []*discordgo.Member
		states  []*discordgo.VoiceState
		present bool
	}{
		{name: "empty"},
		{name: "self", states: []*discordgo.VoiceState{{UserID: "self", ChannelID: "vc", Member: r4Member("guild", "self", false)}}},
		{name: "other bot", states: []*discordgo.VoiceState{{UserID: "bot", ChannelID: "vc", Member: r4Member("guild", "bot", true)}}},
		{name: "unknown identity", states: []*discordgo.VoiceState{{UserID: "unknown", ChannelID: "vc"}}},
		{name: "empty identity", states: []*discordgo.VoiceState{{UserID: " ", ChannelID: "vc", Member: r4Member("guild", " ", false)}}},
		{name: "wrong member", states: []*discordgo.VoiceState{{UserID: "human", ChannelID: "vc", Member: r4Member("guild", "other", false)}}},
		{name: "different channel", states: []*discordgo.VoiceState{{UserID: "human", ChannelID: "other", Member: r4Member("guild", "human", false)}}},
		{name: "human", states: []*discordgo.VoiceState{{UserID: "human", ChannelID: "vc", Member: r4Member("guild", "human", false)}}, present: true},
		{name: "cached human", members: []*discordgo.Member{r4Member("guild", "human", false)}, states: []*discordgo.VoiceState{{UserID: "human", ChannelID: "vc"}}, present: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, _, _ := r4IdentityClient(t, tc.members, tc.states)
			target := client.autoStartTargets[0]
			present, known := client.AutoStartVoicePresence(target)
			if !known || present != tc.present {
				t.Fatalf("presence=%t known=%t want present=%t known=true", present, known, tc.present)
			}
		})
	}
}

func TestAutoStartVoicePresenceRejectsUnavailableOrStaleAuthority(t *testing.T) {
	for _, change := range []string{"epoch", "channel", "stream", "unselected", "disconnected", "active job", "voice tracking", "guild", "self identity"} {
		t.Run(change, func(t *testing.T) {
			client, session, _ := r4IdentityClient(t, nil, []*discordgo.VoiceState{{UserID: "human", ChannelID: "vc", Member: r4Member("guild", "human", false)}})
			target := client.autoStartTargets[0]
			switch change {
			case "epoch":
				target.AssignmentEpoch = "stale"
			case "channel":
				target.VoiceChannelID = "other"
			case "stream":
				target.StreamID = "other"
			case "unselected":
				client.autoStartTargets = nil
			case "disconnected":
				client.status.Connected = false
			case "active job":
				client.job.StreamID = "active"
			case "voice tracking":
				session.State.TrackVoice = false
			case "guild":
				session.State.Guilds = nil
			case "self identity":
				session.State.User = nil
			}
			if present, known := client.AutoStartVoicePresence(target); present || known {
				t.Fatalf("%s accepted presence=%t known=%t", change, present, known)
			}
		})
	}
}

func TestAutoStartVoicePresenceFollowsSDKLeaveAndOtherHumans(t *testing.T) {
	client, session, _ := r4IdentityClient(t, nil, []*discordgo.VoiceState{
		{UserID: "first", ChannelID: "vc", Member: r4Member("guild", "first", false)},
		{UserID: "second", ChannelID: "vc", Member: r4Member("guild", "second", false)},
	})
	target := client.autoStartTargets[0]
	for index, user := range []string{"first", "second"} {
		leave := &discordgo.VoiceStateUpdate{VoiceState: &discordgo.VoiceState{GuildID: "guild", UserID: user}}
		if err := session.State.OnInterface(session, leave); err != nil {
			t.Fatal(err)
		}
		present, known := client.AutoStartVoicePresence(target)
		if !known || present != (index == 0) {
			t.Fatalf("after leave %s: present=%t known=%t", user, present, known)
		}
	}
}
