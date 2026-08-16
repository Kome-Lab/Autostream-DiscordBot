package davewatch

import (
	"context"
	"testing"
	"time"
)

type fakeRecovery struct {
	health  Health
	resends int
	resets  int
}

func (f *fakeRecovery) Health() Health          { return f.health }
func (f *fakeRecovery) ResendKeyPackage() error { f.resends++; return nil }
func (f *fakeRecovery) SoftReset() error        { f.resets++; return nil }

func TestWatchdogRecoversWelcomeTimeoutWithBoundedResends(t *testing.T) {
	now := time.Date(2026, 8, 16, 0, 0, 0, 0, time.UTC)
	recovery := &fakeRecovery{health: Health{Initialized: true, OP26SentAt: now.Add(-11 * time.Second)}}
	var events []Event
	watchdog := New(recovery, func() bool { return true }, func(event Event) { events = append(events, event) }, Config{})

	for index := 0; index < 5; index++ {
		watchdog.Tick(now.Add(time.Duration(index) * time.Second))
	}
	if recovery.resends != 3 {
		t.Fatalf("resends = %d, want bounded limit 3", recovery.resends)
	}
	if len(events) != 3 || events[0].Reason != "welcome_timeout" || events[2].Attempt != 3 {
		t.Fatalf("unexpected recovery events: %#v", events)
	}
}

func TestWatchdogPrioritizesEpochDivergenceReset(t *testing.T) {
	now := time.Date(2026, 8, 16, 0, 0, 0, 0, time.UTC)
	recovery := &fakeRecovery{health: Health{
		Initialized:         true,
		OP26SentAt:          now.Add(-30 * time.Second),
		ProposalFailedSince: now.Add(-6 * time.Second),
	}}
	var event Event
	watchdog := New(recovery, func() bool { return true }, func(value Event) { event = value }, Config{})

	watchdog.Tick(now)
	if recovery.resets != 1 || recovery.resends != 0 {
		t.Fatalf("reset/resend = %d/%d, want 1/0", recovery.resets, recovery.resends)
	}
	if event.Action != "soft_reset" || event.Reason != "epoch_diverged" {
		t.Fatalf("unexpected event: %#v", event)
	}
}

func TestWatchdogRecoversPersistentMissingRatchetsWithBoundedResets(t *testing.T) {
	now := time.Date(2026, 8, 16, 0, 0, 0, 0, time.UTC)
	recovery := &fakeRecovery{health: Health{
		Initialized:      true,
		OP26SentAt:       now.Add(-time.Minute),
		OP30Received:     true,
		LastMissing:      1,
		MissingFirstSeen: now.Add(-16 * time.Second),
	}}
	watchdog := New(recovery, func() bool { return true }, nil, Config{})

	for index := 0; index < 5; index++ {
		watchdog.Tick(now.Add(time.Duration(index) * time.Second))
	}
	if recovery.resets != 3 {
		t.Fatalf("resets = %d, want bounded limit 3", recovery.resets)
	}
}

func TestWatchdogDoesNothingWithoutRemoteParticipants(t *testing.T) {
	now := time.Date(2026, 8, 16, 0, 0, 0, 0, time.UTC)
	recovery := &fakeRecovery{health: Health{Initialized: true, OP26SentAt: now.Add(-time.Minute)}}
	watchdog := New(recovery, func() bool { return false }, nil, Config{})

	watchdog.Tick(now)
	if recovery.resends != 0 || recovery.resets != 0 {
		t.Fatalf("recovery ran without participants: resend=%d reset=%d", recovery.resends, recovery.resets)
	}
}

func TestWatchdogRunStopsWhenContextIsCanceled(t *testing.T) {
	recovery := &fakeRecovery{}
	watchdog := New(recovery, func() bool { return true }, nil, Config{TickEvery: time.Millisecond})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		watchdog.Run(ctx)
		close(done)
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("watchdog did not stop after context cancellation")
	}
}
