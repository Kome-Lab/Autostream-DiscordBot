package jobs

import (
	"testing"
	"time"

	"github.com/example/autostream-discord-bot/internal/discord"
)

func TestUI188R3StoppedPresenceSurvivesWaitingSnapshotGap(t *testing.T) {
	for _, change := range []string{"same authority", "new epoch", "new channel"} {
		t.Run(change, func(t *testing.T) {
			manager := NewManager(&fakeVoice{})
			starter := &fakeStreamStarter{ch: make(chan string, 10)}
			manager.SetStreamStarter(starter)
			defaults := VoiceDefaults{AssignmentEpoch: "epoch-B", GuildID: "guild", VoiceChannelID: "voice", AutoStartEnabled: true}
			manager.SetStreamVoiceDefaults(map[string]VoiceDefaults{"selected": defaults})
			manager.SetStreamVoiceDefaults(nil) // Starting is omitted before BOT dispatch too.
			if err := manager.Start(discord.VoiceJob{StreamID: "selected", GuildID: "guild", VoiceChannelID: "voice"}); err != nil {
				t.Fatal(err)
			}
			// Live/stopping streams are omitted from Panel's waiting target list.
			manager.SetStreamVoiceDefaults(nil)
			if err := manager.Stop("selected"); err != nil {
				t.Fatal(err)
			}
			manager.SetStreamVoiceDefaults(nil)
			if stopped, ok := manager.autoStartStopped["selected"]; !ok || stopped.AssignmentEpoch != "epoch-B" || stopped.VoiceChannelID != "voice" {
				t.Fatalf("waiting snapshot gap lost stopped execution authority: %+v, exists=%v", stopped, ok)
			}
			if change == "new epoch" {
				defaults.AssignmentEpoch = "epoch-C"
			}
			if change == "new channel" {
				defaults.VoiceChannelID = "other-voice"
			}
			manager.SetStreamVoiceDefaults(map[string]VoiceDefaults{"selected": defaults})
			event := discord.VoiceJoinEvent{GuildID: "guild", VoiceChannelID: defaults.VoiceChannelID, UserID: "human", ExistingPresence: true, Authority: &discord.AutoStartAuthority{StreamID: "selected", AssignmentEpoch: defaults.AssignmentEpoch}}
			manager.VoiceUserJoined(event)
			if change == "same authority" {
				select {
				case <-starter.ch:
					t.Fatal("existing presence restarted explicitly stopped execution")
				case <-time.After(20 * time.Millisecond):
				}
				event.ExistingPresence = false
				manager.VoiceUserJoined(event)
			}
			select {
			case id := <-starter.ch:
				if id != "selected" {
					t.Fatal(id)
				}
			case <-time.After(time.Second):
				t.Fatal("current changed authority or genuine new join did not start")
			}
		})
	}
}
