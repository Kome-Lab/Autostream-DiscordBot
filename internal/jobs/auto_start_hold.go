package jobs

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/example/autostream-discord-bot/internal/control"
	"github.com/example/autostream-discord-bot/internal/discord"
)

// ManagedStreamStarter preserves the original automatic request and its exact
// owner observation. Managed UNKNOWN never enters the legacy retry interface.
type ManagedStreamStarter interface {
	StartManagedStream(string, control.AutoStartRequest) error
}

type ManagedStreamInspector interface {
	InspectManagedStream(string, string) (control.ManagedStartInspection, error)
}

type managedHoldObservation struct {
	revision uint64
	known    bool
	held     bool
}

func (m *Manager) observeManagedHoldLocked(streamID string, item VoiceDefaults) VoiceDefaults {
	if !item.AutoStartHoldRequired || item.AutoStartHoldRevision == 0 {
		return item
	}
	if m.managedHoldObservations == nil {
		m.managedHoldObservations = map[string]managedHoldObservation{}
	}
	previous, seen := m.managedHoldObservations[streamID]
	if seen && (item.AutoStartHoldRevision < previous.revision ||
		(item.AutoStartHoldRevision == previous.revision && previous.known && item.AutoStartHoldKnown && item.AutoStartHold != previous.held)) {
		item.AutoStartHoldKnown = false
		return item
	}
	if !seen || item.AutoStartHoldRevision > previous.revision || item.AutoStartHoldKnown {
		m.managedHoldObservations[streamID] = managedHoldObservation{item.AutoStartHoldRevision, item.AutoStartHoldKnown, item.AutoStartHold}
	}
	return item
}

// RequireAutoStartHoldObservations is selected by normal managed Bot startup.
// Older test/embedded v2 callers retain their existing explicit legacy seam;
// production never interprets a missing hold field as an unheld observation.
func (m *Manager) RequireAutoStartHoldObservations() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.requireAutoStartHold = true
	if m.managedAutoStartStreams == nil {
		m.managedAutoStartStreams = map[string]bool{}
	}
	m.autoStartSourceUnavailable = true
	for id, current := range m.streamDefaults {
		current.AutoStartHoldRequired, current.AutoStartHoldKnown = true, false
		m.streamDefaults[id] = current
		m.managedAutoStartStreams[id] = true
		m.cancelAutoStartRetryLocked(id)
	}
}

// SetAutoStartSourceAvailable invalidates managed observations when a current
// CP refresh fails. Cache contents alone cannot restore source availability.
func (m *Manager) SetAutoStartSourceAvailable(available bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.autoStartSourceUnavailable = !available
	if !available {
		for streamID, retry := range m.autoStartRetries {
			if retry.managed || m.streamDefaults[streamID].AutoStartHoldRequired {
				m.cancelAutoStartRetryLocked(streamID)
			}
		}
	}
}

func (m *Manager) autoStartHoldAllowsLocked(streamID string, current VoiceDefaults) bool {
	if !current.AutoStartHoldRequired && !m.managedAutoStartStreams[streamID] {
		return true // Original v2 targets have no managed-hold observation.
	}
	return !m.autoStartSourceUnavailable && current.AutoStartHoldRequired &&
		current.AutoStartHoldKnown && !current.AutoStartHold &&
		current.AutoStartHoldRevision > 0 && current.AssignmentEpoch != "" &&
		current.ExpectedStreamRevision != "" && current.ExpectedSettingsRevision != ""
}

func managedAutoStartRequest(streamID string, current VoiceDefaults) control.AutoStartRequest {
	// These fields are an allowlisted, non-secret source identity. This digest is
	// request correlation only: no raw credential/payload, Generation, authority,
	// or D4 secret commitment is created. Identical source observations retain
	// the same request ID after a Bot restart, so CP can Inspect the old attempt.
	identity := struct {
		StreamID, AssignmentEpoch, StreamRevision, SettingsRevision, RunID string
		HoldRevision                                                       uint64
	}{streamID, current.AssignmentEpoch, current.ExpectedStreamRevision, current.ExpectedSettingsRevision, current.ExpectedRunID, current.AutoStartHoldRevision}
	raw, _ := json.Marshal(identity)
	digest := sha256.Sum256(raw)
	return control.AutoStartRequest{
		RequestID:                "msr-auto-" + hex.EncodeToString(digest[:]),
		ExpectedHoldRevision:     current.AutoStartHoldRevision,
		ExpectedStreamRevision:   current.ExpectedStreamRevision,
		ExpectedSettingsRevision: current.ExpectedSettingsRevision,
		ExpectedRunID:            current.ExpectedRunID,
	}
}

func (m *Manager) claimManagedAutoStartDispatch(streamID string, retry *autoStartRetry) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.autoStartRetryStillValidLocked(streamID, retry) || !retry.managed || !retry.request.Valid() {
		return false
	}
	if m.managedAutoStartAttempts == nil {
		m.managedAutoStartAttempts = map[string]bool{}
	}
	if m.managedAutoStartAttempts[retry.request.RequestID] {
		return false
	}
	// Retain both known and unknown dispatches. Only a different observed owner
	// frame may identify new independent work. Presence/VC events cannot retry it.
	m.managedAutoStartAttempts[retry.request.RequestID] = true
	return true
}

// AutoStartCondition reports a current observation only. It does not release a
// hold or start a job. The caller must compare its original revision/epoch.
type AutoStartCondition struct {
	StreamID         string `json:"stream_id"`
	AssignmentEpoch  string `json:"assignment_epoch"`
	HoldRevision     uint64 `json:"hold_revision"`
	StreamRevision   string `json:"stream_revision"`
	SettingsRevision string `json:"settings_revision"`
	RunID            string `json:"run_id"`
	Known            bool   `json:"known"`
	Present          bool   `json:"present"`
}

func (m *Manager) ObserveAutoStartCondition(streamID string) AutoStartCondition {
	return m.observeAutoStartPresence(streamID, true)
}

// ObserveAutoStartPresence reads only the current selected Gateway presence.
// The cached Hold is reported for diagnostics, never used as authority: CP
// independently owns the pending Resume and its Hold CAS. The strict condition
// getter requires known Hold metadata; automatic dispatch also requires unheld.
func (m *Manager) ObserveAutoStartPresence(streamID string) AutoStartCondition {
	return m.observeAutoStartPresence(streamID, false)
}

func sameAutoStartPresenceFrame(a, b VoiceDefaults) bool {
	return a.AssignmentEpoch == b.AssignmentEpoch && a.GuildID == b.GuildID &&
		a.VoiceChannelID == b.VoiceChannelID && a.TextChannelID == b.TextChannelID &&
		a.AutoStartEnabled == b.AutoStartEnabled && a.ExpectedStreamRevision == b.ExpectedStreamRevision &&
		a.ExpectedSettingsRevision == b.ExpectedSettingsRevision && a.ExpectedRunID == b.ExpectedRunID
}

func (m *Manager) observeAutoStartPresence(streamID string, requireHold bool) AutoStartCondition {
	m.mu.Lock()
	current, exists := m.streamDefaults[streamID]
	unavailable := m.autoStartSourceUnavailable
	result := AutoStartCondition{StreamID: streamID, AssignmentEpoch: current.AssignmentEpoch,
		HoldRevision: current.AutoStartHoldRevision, StreamRevision: current.ExpectedStreamRevision,
		SettingsRevision: current.ExpectedSettingsRevision, RunID: current.ExpectedRunID}
	voice := m.voice
	m.mu.Unlock()
	if !exists || unavailable || current.AssignmentEpoch == "" ||
		current.ExpectedStreamRevision == "" || current.ExpectedSettingsRevision == "" {
		return result
	}
	if requireHold && (!current.AutoStartHoldRequired || !current.AutoStartHoldKnown || current.AutoStartHoldRevision == 0) {
		return result
	}
	source, ok := voice.(discord.AutoStartVoicePresenceSource)
	if !ok {
		return result
	}
	present, known := source.AutoStartVoicePresence(discord.AutoStartVoiceTarget{
		StreamID: streamID, AssignmentEpoch: current.AssignmentEpoch,
		GuildID: current.GuildID, VoiceChannelID: current.VoiceChannelID,
	})
	m.mu.Lock()
	defer m.mu.Unlock()
	latest, stillExists := m.streamDefaults[streamID]
	if m.autoStartSourceUnavailable || !stillExists || !sameAutoStartPresenceFrame(latest, current) || (requireHold && latest != current) {
		return result
	}
	// A Hold-only refresh does not change the actual SDK condition. Report its
	// latest metadata without promoting its unknown state into an unheld value.
	if !requireHold {
		result.HoldRevision = latest.AutoStartHoldRevision
	}
	result.Known, result.Present = known, known && present
	return result
}
