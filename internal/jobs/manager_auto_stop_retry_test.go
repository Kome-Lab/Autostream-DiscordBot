package jobs

import (
	"errors"
	"github.com/example/autostream-discord-bot/internal/control"
	"github.com/example/autostream-discord-bot/internal/discord"
	"testing"
	"time"
)

func TestParticipantReturningBeforeAutoStopCancelsRequest(t *testing.T) {
	manager := NewManager(&fakeVoice{})
	manager.autoStopDelay = 30 * time.Millisecond
	stopper := &fakeStreamStopper{ch: make(chan string, 1)}
	manager.SetStreamStopper(stopper)
	if err := manager.Start(discord.VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01"}); err != nil {
		t.Fatal(err)
	}
	manager.ParticipantChanged(discord.ParticipantEvent{StreamID: "stream-01", UserID: "user-01", Present: true})
	manager.ParticipantChanged(discord.ParticipantEvent{StreamID: "stream-01", UserID: "user-01", Present: false})
	time.Sleep(5 * time.Millisecond)
	manager.ParticipantChanged(discord.ParticipantEvent{StreamID: "stream-01", UserID: "user-01", Present: true})
	select {
	case got := <-stopper.ch:
		t.Fatalf("participant return should cancel auto-stop, got %q", got)
	case <-time.After(70 * time.Millisecond):
	}
}

func TestParticipantLeavingEmptyVCRetriesTransientStopFailures(t *testing.T) {
	manager := NewManager(&fakeVoice{})
	manager.autoStopDelay = 5 * time.Millisecond
	manager.autoStopRetryDelays = []time.Duration{5 * time.Millisecond, 10 * time.Millisecond, 15 * time.Millisecond}
	stopper := &fakeStreamStopper{
		ch: make(chan string, 5),
		errs: []error{
			control.AutoStopTransportError{Err: errors.New("transport connection reset")},
			control.ControlPanelError{StatusCode: 429},
			control.ControlPanelError{StatusCode: 502},
			nil,
		},
	}
	manager.SetStreamStopper(stopper)
	if err := manager.Start(discord.VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01"}); err != nil {
		t.Fatal(err)
	}
	manager.ParticipantChanged(discord.ParticipantEvent{StreamID: "stream-01", UserID: "user-01", Present: true})
	manager.ParticipantChanged(discord.ParticipantEvent{StreamID: "stream-01", UserID: "user-01", Present: false})

	for attempt := 1; attempt <= 4; attempt++ {
		select {
		case got := <-stopper.ch:
			if got != "stream-01" {
				t.Fatalf("attempt %d stopped %q, want stream-01", attempt, got)
			}
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for auto-stop attempt %d", attempt)
		}
	}
	select {
	case got := <-stopper.ch:
		t.Fatalf("successful retry should finish auto-stop, got another request for %q", got)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestAutoStopRetryPolicyUsesBoundedExpectedDelays(t *testing.T) {
	manager := NewManager(&fakeVoice{})
	want := []time.Duration{5 * time.Second, 15 * time.Second, 45 * time.Second}
	if len(manager.autoStopRetryDelays) != len(want) {
		t.Fatalf("auto-stop retry delays = %#v, want %#v", manager.autoStopRetryDelays, want)
	}
	for index, delay := range want {
		if got := manager.autoStopRetryDelays[index]; got != delay {
			t.Fatalf("auto-stop retry delay %d = %s, want %s", index, got, delay)
		}
	}
}

func TestParticipantLeavingEmptyVCDoesNotRetryRejectedStop(t *testing.T) {
	manager := NewManager(&fakeVoice{})
	manager.autoStopDelay = 5 * time.Millisecond
	manager.autoStopRetryDelays = []time.Duration{5 * time.Millisecond}
	stopper := &fakeStreamStopper{ch: make(chan string, 2), errs: []error{control.ControlPanelError{StatusCode: 409}}}
	manager.SetStreamStopper(stopper)
	if err := manager.Start(discord.VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01"}); err != nil {
		t.Fatal(err)
	}
	manager.ParticipantChanged(discord.ParticipantEvent{StreamID: "stream-01", UserID: "user-01", Present: true})
	manager.ParticipantChanged(discord.ParticipantEvent{StreamID: "stream-01", UserID: "user-01", Present: false})
	select {
	case <-stopper.ch:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for rejected auto-stop request")
	}
	select {
	case got := <-stopper.ch:
		t.Fatalf("rejected 4xx auto-stop should not retry, got %q", got)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestParticipantReturningCancelsScheduledAutoStopRetry(t *testing.T) {
	manager := NewManager(&fakeVoice{})
	manager.autoStopDelay = 5 * time.Millisecond
	manager.autoStopRetryDelays = []time.Duration{40 * time.Millisecond}
	stopper := &fakeStreamStopper{ch: make(chan string, 2), errs: []error{control.AutoStopTransportError{Err: errors.New("transport unavailable")}}}
	manager.SetStreamStopper(stopper)
	if err := manager.Start(discord.VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01"}); err != nil {
		t.Fatal(err)
	}
	manager.ParticipantChanged(discord.ParticipantEvent{StreamID: "stream-01", UserID: "user-01", Present: true})
	manager.ParticipantChanged(discord.ParticipantEvent{StreamID: "stream-01", UserID: "user-01", Present: false})
	select {
	case <-stopper.ch:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for initial auto-stop request")
	}
	manager.ParticipantChanged(discord.ParticipantEvent{StreamID: "stream-01", UserID: "user-01", Present: true})
	select {
	case got := <-stopper.ch:
		t.Fatalf("participant return should cancel retry, got %q", got)
	case <-time.After(80 * time.Millisecond):
	}
}

func TestParticipantReturningCancelsInFlightAutoStopRequest(t *testing.T) {
	manager := NewManager(&fakeVoice{})
	manager.autoStopDelay = 0
	stopper := &blockingContextStreamStopper{
		started:   make(chan string, 1),
		canceled:  make(chan string, 1),
		committed: make(chan string, 1),
	}
	manager.SetStreamStopper(stopper)
	if err := manager.Start(discord.VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01"}); err != nil {
		t.Fatal(err)
	}
	manager.ParticipantChanged(discord.ParticipantEvent{StreamID: "stream-01", UserID: "user-01", Present: true})
	manager.ParticipantChanged(discord.ParticipantEvent{StreamID: "stream-01", UserID: "user-01", Present: false})
	select {
	case got := <-stopper.started:
		if got != "stream-01" {
			t.Fatalf("in-flight stop stream = %q, want stream-01", got)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for in-flight auto-stop request")
	}

	manager.ParticipantChanged(discord.ParticipantEvent{StreamID: "stream-01", UserID: "user-01", Present: true})
	select {
	case got := <-stopper.canceled:
		if got != "stream-01" {
			t.Fatalf("canceled stream = %q, want stream-01", got)
		}
	case <-time.After(time.Second):
		t.Fatal("participant return did not cancel in-flight auto-stop request")
	}
	select {
	case got := <-stopper.committed:
		t.Fatalf("stale auto-stop committed after participant return for %q", got)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestControlPanelStopCallbackDoesNotCancelOwnAutoStopRequest(t *testing.T) {
	manager := NewManager(&fakeVoice{})
	manager.autoStopDelay = 0
	stopper := &panelStopCallbackStreamStopper{
		manager:  manager,
		started:  make(chan string, 1),
		callback: make(chan string, 1),
		ready:    make(chan string, 1),
		release:  make(chan struct{}),
		returned: make(chan string, 1),
		canceled: make(chan string, 1),
	}
	manager.SetStreamStopper(stopper)
	if err := manager.Start(discord.VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01"}); err != nil {
		t.Fatal(err)
	}
	manager.ParticipantChanged(discord.ParticipantEvent{StreamID: "stream-01", UserID: "user-01", Present: true})
	manager.ParticipantChanged(discord.ParticipantEvent{StreamID: "stream-01", UserID: "user-01", Present: false})
	select {
	case got := <-stopper.started:
		if got != "stream-01" {
			t.Fatalf("auto-stop stream = %q, want stream-01", got)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for auto-stop request")
	}
	select {
	case got := <-stopper.callback:
		if got != "stream-01" {
			t.Fatalf("Panel callback stream = %q, want stream-01", got)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for nested Control Panel stop callback")
	}
	if got := manager.CurrentStreamID(); got != "" {
		t.Fatalf("nested Panel stop did not clear current stream: %q", got)
	}
	select {
	case got := <-stopper.ready:
		if got != "stream-01" {
			t.Fatalf("parent auto-stop readiness stream = %q, want stream-01", got)
		}
	case got := <-stopper.canceled:
		t.Fatalf("Panel callback canceled its own parent auto-stop request for %q", got)
	case <-time.After(time.Second):
		t.Fatal("auto-stop request did not remain live after nested Panel callback")
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
	case got := <-stopper.canceled:
		t.Fatalf("Panel callback canceled its own parent auto-stop request for %q", got)
	case <-time.After(50 * time.Millisecond):
	}
}
