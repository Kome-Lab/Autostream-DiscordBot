package jobs

import (
	"github.com/example/autostream-discord-bot/internal/discord"
	"sync"
	"testing"
	"time"
)

func TestVoiceJoinAfterPanelStopCallbackReconcilesRearmedStreamAfterRuntimeRefresh(t *testing.T) {
	manager := NewManager(&fakeVoice{})
	manager.autoStopDelay = 0
	// The production policy retains the intent for more than one minute. Use
	// millisecond-scale service windows here while requiring the rearmed source
	// row to remain absent for two sequential refreshes.
	manager.rejoinReconcileDelay = []time.Duration{0, 5 * time.Millisecond, 5 * time.Millisecond, 5 * time.Millisecond}
	stopper := &panelStopCallbackStreamStopper{
		manager:  manager,
		started:  make(chan string, 1),
		callback: make(chan string, 1),
		ready:    make(chan string, 1),
		release:  make(chan struct{}),
		returned: make(chan string, 1),
		canceled: make(chan string, 1),
	}
	starter := &fakeStreamStarter{ch: make(chan string, 2)}
	manager.SetStreamStopper(stopper)
	manager.SetStreamStarter(starter)
	manager.SetStreamVoiceDefaults(map[string]VoiceDefaults{
		"stream-01": {GuildID: "guild-01", VoiceChannelID: "voice-01", AutoStartEnabled: true},
	})
	var refreshMu sync.Mutex
	refreshes := 0
	manager.SetAutoStartRefresher(func() error {
		refreshMu.Lock()
		refreshes++
		attempt := refreshes
		refreshMu.Unlock()
		defaults := map[string]VoiceDefaults{}
		if attempt >= 3 {
			defaults["stream-01"] = VoiceDefaults{GuildID: "guild-01", VoiceChannelID: "voice-01", AutoStartEnabled: true}
		}
		manager.SetStreamVoiceDefaults(defaults)
		return nil
	})
	if err := manager.Start(discord.VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01"}); err != nil {
		t.Fatal(err)
	}
	manager.ParticipantChanged(discord.ParticipantEvent{StreamID: "stream-01", UserID: "user-left", Present: true})
	manager.ParticipantChanged(discord.ParticipantEvent{StreamID: "stream-01", UserID: "user-left", Present: false})
	select {
	case <-stopper.callback:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for nested Control Panel stop callback")
	}
	select {
	case <-stopper.ready:
	case got := <-stopper.canceled:
		t.Fatalf("Panel callback canceled its own parent auto-stop request for %q", got)
	case <-time.After(time.Second):
		t.Fatal("auto-stop request did not remain live after nested Panel callback")
	}

	// The Bot's local stop callback has already cleared current, but the parent
	// request is still waiting for the Panel's remaining service stops. This VC
	// join must become a durable in-memory rejoin intent, not an attempt to
	// restart the source stream or a cancellation of the parent request.
	manager.VoiceUserJoined(discord.VoiceJoinEvent{GuildID: "guild-01", VoiceChannelID: "voice-01", UserID: "user-returned"})
	manager.mu.Lock()
	_, _, recorded := manager.autoStopRejoinIntentForStreamLocked("stream-01")
	manager.mu.Unlock()
	if !recorded {
		t.Fatal("VC join after Panel callback did not retain a rejoin intent")
	}
	select {
	case got := <-stopper.canceled:
		t.Fatalf("post-callback VC join canceled the parent request for %q", got)
	default:
	}

	close(stopper.release)
	select {
	case got := <-stopper.returned:
		if got != "stream-01" {
			t.Fatalf("returned auto-stop stream = %q, want stream-01", got)
		}
	case <-time.After(time.Second):
		t.Fatal("auto-stop request did not finish after the Panel dispatch completed")
	}
	select {
	case got := <-starter.ch:
		if got != "stream-01" {
			t.Fatalf("delayed rejoin started %q, want stream-01", got)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for delayed rearmed auto-start")
	}
	select {
	case got := <-starter.ch:
		t.Fatalf("delayed rejoin issued duplicate start for %q", got)
	case <-time.After(50 * time.Millisecond):
	}
	refreshMu.Lock()
	gotRefreshes := refreshes
	refreshMu.Unlock()
	if gotRefreshes < 3 {
		t.Fatalf("refresh attempts = %d, want rearmed source visibility after at least two service windows", gotRefreshes)
	}
}

func TestAutoStopRejoinPolicyCoversSequentialPanelStops(t *testing.T) {
	manager := NewManager(&fakeVoice{})
	var total time.Duration
	for _, delay := range manager.rejoinReconcileDelay {
		total += delay
	}
	// The Control Panel may wait for three five-second service calls and a
	// rearm write. Keep enough budget for more than two sequential timeouts.
	if total < 30*time.Second {
		t.Fatalf("rejoin reconciliation window = %s, want at least 30s", total)
	}
}

func TestVoiceJoinDuringCommittedAutoStopReconcilesRearmedStream(t *testing.T) {
	manager := NewManager(&fakeVoice{})
	manager.autoStopDelay = 0
	manager.rejoinReconcileDelay = []time.Duration{0, 5 * time.Millisecond, 5 * time.Millisecond}
	stopper := &commitAfterCancelStreamStopper{
		started:   make(chan string, 1),
		canceled:  make(chan string, 1),
		release:   make(chan struct{}),
		committed: make(chan string, 1),
	}
	starter := &fakeStreamStarter{ch: make(chan string, 2)}
	manager.SetStreamStopper(stopper)
	manager.SetStreamStarter(starter)
	manager.SetStreamVoiceDefaults(map[string]VoiceDefaults{
		"stream-01": {GuildID: "guild-01", VoiceChannelID: "voice-01", AutoStartEnabled: true},
	})
	refreshed := make(chan struct{}, 3)
	manager.SetAutoStartRefresher(func() error {
		manager.SetStreamVoiceDefaults(map[string]VoiceDefaults{
			// The Control Panel re-arms the completed source row. Reconciliation
			// must start that same durable stream ID after the stop commits.
			"stream-01": {GuildID: "guild-01", VoiceChannelID: "voice-01", AutoStartEnabled: true},
		})
		select {
		case refreshed <- struct{}{}:
		default:
		}
		return nil
	})
	if err := manager.Start(discord.VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01"}); err != nil {
		t.Fatal(err)
	}
	manager.ParticipantChanged(discord.ParticipantEvent{StreamID: "stream-01", UserID: "user-left", Present: true})
	manager.ParticipantChanged(discord.ParticipantEvent{StreamID: "stream-01", UserID: "user-left", Present: false})
	select {
	case got := <-stopper.started:
		if got != "stream-01" {
			t.Fatalf("in-flight stop stream = %q, want stream-01", got)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for in-flight auto-stop request")
	}

	// VoiceUserJoined has no stream ID. It must still retain a rejoin intent
	// while the old stream is current and cancel the outbound stop request.
	manager.VoiceUserJoined(discord.VoiceJoinEvent{GuildID: "guild-01", VoiceChannelID: "voice-01", UserID: "user-returned"})
	select {
	case got := <-stopper.canceled:
		if got != "stream-01" {
			t.Fatalf("canceled stream = %q, want stream-01", got)
		}
	case <-time.After(time.Second):
		t.Fatal("voice join did not cancel in-flight auto-stop request")
	}

	// Simulate a Control Panel commit that won the cancellation race: the stop
	// response returns while the Bot still has the old job, then the downstream
	// /stop callback clears it. The first reconcile must keep the intent alive.
	close(stopper.release)
	select {
	case got := <-stopper.committed:
		if got != "stream-01" {
			t.Fatalf("committed stream = %q, want stream-01", got)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for committed stop response")
	}
	select {
	case <-refreshed:
	case <-time.After(time.Second):
		t.Fatal("expected stale-stop reconciliation runtime refresh")
	}
	if err := manager.Stop("stream-01"); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-starter.ch:
		if got != "stream-01" {
			t.Fatalf("rejoined VC started %q, want rearmed stream-01", got)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for rearmed auto-start")
	}
	select {
	case got := <-starter.ch:
		t.Fatalf("rejoin reconciliation issued duplicate start for %q", got)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestVoiceJoinDuringAutoStopDoesNotRestartWhenActiveOrNoSuccessor(t *testing.T) {
	for _, tt := range []struct {
		name          string
		stopOldStream bool
		defaults      map[string]VoiceDefaults
	}{
		{
			name: "original stream remains active",
			defaults: map[string]VoiceDefaults{
				"stream-01": {GuildID: "guild-01", VoiceChannelID: "voice-01", AutoStartEnabled: true},
				"stream-02": {GuildID: "guild-01", VoiceChannelID: "voice-01", AutoStartEnabled: true},
			},
		},
		{
			name:          "no successor in refreshed config",
			stopOldStream: true,
			defaults:      map[string]VoiceDefaults{},
		},
		{
			name:          "ambiguous successors are rejected",
			stopOldStream: true,
			defaults: map[string]VoiceDefaults{
				"stream-02": {GuildID: "guild-01", VoiceChannelID: "voice-01", AutoStartEnabled: true},
				"stream-03": {GuildID: "guild-01", VoiceChannelID: "voice-01", AutoStartEnabled: true},
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			manager := NewManager(&fakeVoice{})
			manager.autoStopDelay = 0
			manager.rejoinReconcileDelay = []time.Duration{0}
			stopper := &commitAfterCancelStreamStopper{
				started:   make(chan string, 1),
				canceled:  make(chan string, 1),
				release:   make(chan struct{}),
				committed: make(chan string, 1),
			}
			starter := &fakeStreamStarter{ch: make(chan string, 1)}
			manager.SetStreamStopper(stopper)
			manager.SetStreamStarter(starter)
			manager.SetStreamVoiceDefaults(map[string]VoiceDefaults{
				"stream-01": {GuildID: "guild-01", VoiceChannelID: "voice-01", AutoStartEnabled: true},
			})
			manager.SetAutoStartRefresher(func() error {
				manager.SetStreamVoiceDefaults(tt.defaults)
				return nil
			})
			if err := manager.Start(discord.VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01"}); err != nil {
				t.Fatal(err)
			}
			manager.ParticipantChanged(discord.ParticipantEvent{StreamID: "stream-01", UserID: "user-left", Present: true})
			manager.ParticipantChanged(discord.ParticipantEvent{StreamID: "stream-01", UserID: "user-left", Present: false})
			select {
			case <-stopper.started:
			case <-time.After(time.Second):
				t.Fatal("timed out waiting for in-flight auto-stop request")
			}
			manager.VoiceUserJoined(discord.VoiceJoinEvent{GuildID: "guild-01", VoiceChannelID: "voice-01", UserID: "user-returned"})
			select {
			case <-stopper.canceled:
			case <-time.After(time.Second):
				t.Fatal("voice join did not cancel in-flight auto-stop request")
			}
			close(stopper.release)
			select {
			case <-stopper.committed:
			case <-time.After(time.Second):
				t.Fatal("timed out waiting for committed stop response")
			}
			if tt.stopOldStream {
				if err := manager.Stop("stream-01"); err != nil {
					t.Fatal(err)
				}
			}
			select {
			case got := <-starter.ch:
				t.Fatalf("unexpected reconciliation start for %q", got)
			case <-time.After(80 * time.Millisecond):
			}
		})
	}
}

func TestStartAndStopCancelScheduledAutoStop(t *testing.T) {
	t.Run("start", func(t *testing.T) {
		manager := NewManager(&fakeVoice{})
		manager.autoStopDelay = 40 * time.Millisecond
		stopper := &fakeStreamStopper{ch: make(chan string, 1)}
		manager.SetStreamStopper(stopper)
		job := discord.VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01"}
		if err := manager.Start(job); err != nil {
			t.Fatal(err)
		}
		manager.ParticipantChanged(discord.ParticipantEvent{StreamID: "stream-01", UserID: "user-01", Present: false})
		if err := manager.Start(job); err != nil {
			t.Fatal(err)
		}
		select {
		case got := <-stopper.ch:
			t.Fatalf("start should cancel stale auto-stop timer, got %q", got)
		case <-time.After(70 * time.Millisecond):
		}
	})

	t.Run("stop", func(t *testing.T) {
		manager := NewManager(&fakeVoice{})
		manager.autoStopDelay = 40 * time.Millisecond
		stopper := &fakeStreamStopper{ch: make(chan string, 1)}
		manager.SetStreamStopper(stopper)
		if err := manager.Start(discord.VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01"}); err != nil {
			t.Fatal(err)
		}
		manager.ParticipantChanged(discord.ParticipantEvent{StreamID: "stream-01", UserID: "user-01", Present: false})
		if err := manager.Stop("stream-01"); err != nil {
			t.Fatal(err)
		}
		select {
		case got := <-stopper.ch:
			t.Fatalf("stop should cancel stale auto-stop timer, got %q", got)
		case <-time.After(70 * time.Millisecond):
		}
	})
}
