//go:build linux && ui188_chain

package jobs

import "time"

// UI188AutoStartInFlight lets the isolated Gateway fixture wait for a pending
// callback/retry to finish before asserting external request counts. It exposes
// no credential and is absent from ordinary product builds.
func (m *Manager) UI188AutoStartInFlight(streamID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.autoStartRetries[streamID] != nil
}

// Only shorten the fixture's idle schedule. The production snapshot warmup,
// initial-empty suppression and two-step empty confirmation remain unchanged.
func (m *Manager) UI188UseFocusedEmptyVoiceTiming() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.current.StreamID != "" {
		return false
	}
	m.participantSyncDelays = []time.Duration{5 * time.Millisecond, 5 * time.Millisecond}
	m.participantSyncInterval = 0
	m.autoStopDelay = 5 * time.Millisecond
	m.autoStopEmptyConfirmDelay = 5 * time.Millisecond
	return true
}

func (m *Manager) UI188VoiceStopState(streamID string) (snapshotRevision uint64, pending, inFlight bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, inFlight = m.autoStopInFlight[streamID]
	return m.participantSnapshotRevision, m.autoStopPending[streamID], inFlight
}
