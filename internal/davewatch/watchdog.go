package davewatch

import (
	"context"
	"time"
)

// Health is the non-secret subset of the Discord DAVE/MLS state needed by
// the recovery policy.
type Health struct {
	Initialized         bool
	OP26SentAt          time.Time
	OP30Received        bool
	LastMissing         int
	MissingFirstSeen    time.Time
	ProposalFailedSince time.Time
}

// Recovery is implemented by the active Discord voice connection.
type Recovery interface {
	Health() Health
	ResendKeyPackage() error
	SoftReset() error
}

type Event struct {
	Action  string
	Reason  string
	Result  string
	Attempt int
	Limit   int
}

type Config struct {
	TickEvery       time.Duration
	StuckTimeout    time.Duration
	MissingTimeout  time.Duration
	DivergedTimeout time.Duration
	ResendLimit     int
	ResendWindow    time.Duration
	ResetLimit      int
	ResetWindow     time.Duration
}

func DefaultConfig() Config {
	return Config{
		TickEvery:       2 * time.Second,
		StuckTimeout:    10 * time.Second,
		MissingTimeout:  15 * time.Second,
		DivergedTimeout: 5 * time.Second,
		ResendLimit:     3,
		ResendWindow:    time.Minute,
		ResetLimit:      3,
		ResetWindow:     2 * time.Minute,
	}
}

type Watchdog struct {
	recovery            Recovery
	participantsPresent func() bool
	report              func(Event)
	config              Config
	resends             []time.Time
	resets              []time.Time
}

func New(recovery Recovery, participantsPresent func() bool, report func(Event), config Config) *Watchdog {
	defaults := DefaultConfig()
	if config.TickEvery <= 0 {
		config.TickEvery = defaults.TickEvery
	}
	if config.StuckTimeout <= 0 {
		config.StuckTimeout = defaults.StuckTimeout
	}
	if config.MissingTimeout <= 0 {
		config.MissingTimeout = defaults.MissingTimeout
	}
	if config.DivergedTimeout <= 0 {
		config.DivergedTimeout = defaults.DivergedTimeout
	}
	if config.ResendLimit <= 0 {
		config.ResendLimit = defaults.ResendLimit
	}
	if config.ResendWindow <= 0 {
		config.ResendWindow = defaults.ResendWindow
	}
	if config.ResetLimit <= 0 {
		config.ResetLimit = defaults.ResetLimit
	}
	if config.ResetWindow <= 0 {
		config.ResetWindow = defaults.ResetWindow
	}
	return &Watchdog{recovery: recovery, participantsPresent: participantsPresent, report: report, config: config}
}

// Run watches one voice generation until its context is canceled. Recovery is
// rate-bounded independently for key-package resends and MLS soft resets.
func (w *Watchdog) Run(ctx context.Context) {
	if w == nil || w.recovery == nil || w.participantsPresent == nil {
		return
	}
	ticker := time.NewTicker(w.config.TickEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			w.Tick(now)
		}
	}
}

// Tick is exported so the bounded policy can be tested without real time or a
// live Discord voice connection.
func (w *Watchdog) Tick(now time.Time) {
	if w == nil || w.recovery == nil || w.participantsPresent == nil || !w.participantsPresent() {
		return
	}
	health := w.recovery.Health()
	if !health.Initialized || health.OP26SentAt.IsZero() {
		return
	}
	switch {
	case !health.ProposalFailedSince.IsZero() && now.Sub(health.ProposalFailedSince) > w.config.DivergedTimeout:
		w.tryReset(now, "epoch_diverged")
	case !health.OP30Received && now.Sub(health.OP26SentAt) > w.config.StuckTimeout:
		w.tryResend(now, "welcome_timeout")
	case health.OP30Received && health.LastMissing > 0 && !health.MissingFirstSeen.IsZero() && now.Sub(health.MissingFirstSeen) > w.config.MissingTimeout:
		w.tryReset(now, "missing_ratchets")
	}
}

func (w *Watchdog) tryResend(now time.Time, reason string) {
	w.resends = pruneOlderThan(w.resends, now.Add(-w.config.ResendWindow))
	if len(w.resends) >= w.config.ResendLimit {
		return
	}
	w.resends = append(w.resends, now)
	event := Event{Action: "resend_key_package", Reason: reason, Result: "success", Attempt: len(w.resends), Limit: w.config.ResendLimit}
	if err := w.recovery.ResendKeyPackage(); err != nil {
		event.Result = "failure"
	}
	w.emit(event)
}

func (w *Watchdog) tryReset(now time.Time, reason string) {
	w.resets = pruneOlderThan(w.resets, now.Add(-w.config.ResetWindow))
	if len(w.resets) >= w.config.ResetLimit {
		return
	}
	w.resets = append(w.resets, now)
	event := Event{Action: "soft_reset", Reason: reason, Result: "success", Attempt: len(w.resets), Limit: w.config.ResetLimit}
	if err := w.recovery.SoftReset(); err != nil {
		event.Result = "failure"
	}
	w.emit(event)
}

func (w *Watchdog) emit(event Event) {
	if w.report != nil {
		w.report(event)
	}
}

func pruneOlderThan(stamps []time.Time, cutoff time.Time) []time.Time {
	out := stamps[:0]
	for _, stamp := range stamps {
		if stamp.After(cutoff) {
			out = append(out, stamp)
		}
	}
	return out
}
