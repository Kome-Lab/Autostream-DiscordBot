package jobs

import (
	"testing"
	"time"

	"github.com/example/autostream-discord-bot/internal/control"
	"github.com/example/autostream-discord-bot/internal/discord"
)

type waitingCapacityStarter struct {
	err   error
	calls chan string
}

func (s *waitingCapacityStarter) StartStream(id string) error {
	s.calls <- id
	return s.err
}

func TestNodeCapacityRetryExhaustionDoesNotConsumeFuturePresence(t *testing.T) {
	for _, tc := range []struct {
		name         string
		err          error
		wantCooldown bool
		wantCalls    int
	}{
		{"capacity", control.ControlPanelError{StatusCode: 409, Code: "node_execution_busy"}, false, 2},
		{"terminal conflict", control.ControlPanelError{StatusCode: 409, Code: "node_selection_changed"}, true, 1},
		{"accepted intent", nil, true, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			manager := NewManager(&discord.NoopClient{})
			manager.autoStartRetryDelays = []time.Duration{0}
			manager.autoStartCooldown = time.Hour
			starter := &waitingCapacityStarter{err: tc.err, calls: make(chan string, 8)}
			manager.SetStreamStarter(starter)
			manager.SetStreamVoiceDefaults(map[string]VoiceDefaults{
				"waiting": {AssignmentEpoch: "epoch", GuildID: "guild", VoiceChannelID: "voice", AutoStartEnabled: true},
			})
			t.Cleanup(func() {
				manager.mu.Lock()
				manager.cancelAllAutoStartRetriesLocked()
				manager.mu.Unlock()
			})
			event := discord.VoiceJoinEvent{
				ExistingPresence: true, GuildID: "guild", VoiceChannelID: "voice", UserID: "human",
				Authority: &discord.AutoStartAuthority{StreamID: "waiting", AssignmentEpoch: "epoch"},
			}
			manager.VoiceUserJoined(event)
			deadline := time.NewTimer(2 * time.Second)
			defer deadline.Stop()
			tick := time.NewTicker(time.Millisecond)
			defer tick.Stop()
			for {
				manager.mu.Lock()
				pending := manager.autoStartRetries["waiting"] != nil
				_, cooldown := manager.autoStartPending["waiting"]
				manager.mu.Unlock()
				if !pending {
					if cooldown != tc.wantCooldown || len(starter.calls) != tc.wantCalls {
						t.Fatalf("cooldown=%t calls=%d", cooldown, len(starter.calls))
					}
					break
				}
				select {
				case <-tick.C:
				case <-deadline.C:
					t.Fatal("bounded start retry did not finish")
				}
			}
			if !tc.wantCooldown {
				for range tc.wantCalls {
					<-starter.calls
				}
				manager.VoiceUserJoined(event)
				select {
				case id := <-starter.calls:
					if id != "waiting" {
						t.Fatal("capacity replay selected another stream")
					}
				case <-time.After(2 * time.Second):
					t.Fatal("old busy attempt suppressed existing presence after capacity became available")
				}
			}
		})
	}
}

func TestWaitingEpochCacheRetainsOnlyReferencedStreams(t *testing.T) {
	manager := NewManager(&discord.NoopClient{})
	manager.SetStreamVoiceDefaults(map[string]VoiceDefaults{
		"A": {AssignmentEpoch: "epoch-A", GuildID: "guild", VoiceChannelID: "voice-A", AutoStartEnabled: true},
	})
	manager.SetExecutionAuthorityStreams(map[string]bool{"A": true})
	manager.SetStreamVoiceDefaults(map[string]VoiceDefaults{
		"B": {AssignmentEpoch: "epoch-B", GuildID: "guild", VoiceChannelID: "voice-B", AutoStartEnabled: true},
	})
	if manager.lastStreamDefaults["A"].AssignmentEpoch != "epoch-A" || manager.streamDefaults["A"].AssignmentEpoch != "" {
		t.Fatal("execution epoch was lost or was promoted to a waiting candidate")
	}
	manager.SetExecutionAuthorityStreams(nil)
	manager.SetStreamVoiceDefaults(map[string]VoiceDefaults{
		"B": {AssignmentEpoch: "epoch-B", GuildID: "guild", VoiceChannelID: "voice-B", AutoStartEnabled: true},
	})
	if len(manager.lastStreamDefaults) != 1 || manager.lastStreamDefaults["B"].AssignmentEpoch != "epoch-B" {
		t.Fatal("unreferenced epochs accumulated in the retention cache")
	}
}
