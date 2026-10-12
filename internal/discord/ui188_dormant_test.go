package discord

import (
	"github.com/cartridge-gg/discordgo"
	"testing"
)

func TestUI188UnselectedBotHasNoGatewaySession(t *testing.T) {
	client := NewDormantRealClient()
	for range 3 {
		if err := client.ReconcileGateway(Config{}, false); err != nil {
			t.Fatal(err)
		}
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.session != nil || client.voice != nil || client.cfg.BotToken != "" {
		t.Fatal("unselected idle Bot retained Discord connection")
	}
}

func TestUI188WaitingJoinColdCacheRequiresCurrentSelectedAuthority(t *testing.T) {
	for _, tc := range []struct {
		name            string
		selected, stale bool
		otherChannel    bool
		want            bool
	}{
		{"selected current session", true, false, false, true},
		{"unselected bot", false, false, false, false},
		{"replaced gateway session", true, true, false, false},
		{"already moved to another channel", true, false, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sink := &fakeEventSink{}
			session := &discordgo.Session{State: discordgo.NewState()}
			session.State.User = &discordgo.User{ID: "bot-01"}
			if err := session.State.GuildAdd(&discordgo.Guild{ID: "guild-01"}); err != nil {
				t.Fatal(err)
			}
			if tc.otherChannel {
				if err := session.State.GuildAdd(&discordgo.Guild{ID: "guild-01", VoiceStates: []*discordgo.VoiceState{{UserID: "user-01", GuildID: "guild-01", ChannelID: "voice-other"}}}); err != nil {
					t.Fatal(err)
				}
			}
			client := &RealClient{sink: sink, session: session}
			if tc.selected {
				client.autoStartTargets = []AutoStartVoiceTarget{{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01", AssignmentEpoch: "epoch-01"}}
			}
			if tc.stale {
				client.session = &discordgo.Session{State: discordgo.NewState()}
			}
			client.onVoiceStateUpdate(session, &discordgo.VoiceStateUpdate{VoiceState: &discordgo.VoiceState{UserID: "user-01", GuildID: "guild-01", ChannelID: "voice-01"}})
			if got := sink.voiceJoin.UserID != ""; got != tc.want {
				t.Fatalf("join emitted=%v, want=%v", got, tc.want)
			}
			if tc.want && (sink.voiceJoin.Authority == nil || sink.voiceJoin.Authority.StreamID != "stream-01" || sink.voiceJoin.Authority.AssignmentEpoch != "epoch-01") {
				t.Fatal("selected authority lost")
			}
		})
	}
}
