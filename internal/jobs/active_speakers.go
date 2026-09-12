package jobs

import (
	"context"
	"errors"
	"time"

	"github.com/example/autostream-discord-bot/internal/discord"
)

func (m *Manager) ActiveSpeakerDetected(streamID, userID string) {
	_ = m.SetActiveSpeaker(streamID, userID)
}

// ActiveSpeakerStateChanged tracks both edges of Discord's speaking signal.
// A stop from a non-active participant must not clear another participant's
// currently highlighted speaker.
func (m *Manager) ActiveSpeakerStateChanged(streamID, userID string, speaking bool) {
	_ = m.setSpeakerState(streamID, userID, speaking, true)
}

func (m *Manager) SetActiveSpeaker(streamID, userID string) error {
	if userID == "" {
		return m.clearSpeakerStates(streamID)
	}
	return m.setSpeakerState(streamID, userID, true, false)
}

func (m *Manager) setSpeakerState(streamID, userID string, speaking, preserveOthers bool) error {
	m.mu.Lock()
	if m.eventsPausedLocked() || m.current.StreamID == "" {
		m.mu.Unlock()
		return errors.New("no active stream job")
	}
	if streamID != "" && streamID != m.current.StreamID {
		m.mu.Unlock()
		return errors.New("stream_id does not match current job")
	}
	if _, ok := m.participants[userID]; !ok {
		m.mu.Unlock()
		return errors.New("active speaker must be an active participant")
	}
	if m.activeSpeakers == nil {
		m.activeSpeakers = map[string]bool{}
	}
	currentlySpeaking := m.activeSpeakers[userID]
	if preserveOthers && currentlySpeaking == speaking {
		m.mu.Unlock()
		return nil
	}
	if speaking && !preserveOthers && currentlySpeaking && len(m.activeSpeakers) == 1 {
		m.mu.Unlock()
		return nil
	}
	if speaking {
		if !preserveOthers {
			m.activeSpeakers = map[string]bool{}
		}
		m.activeSpeakers[userID] = true
		m.activeSpeaker = userID
	} else {
		delete(m.activeSpeakers, userID)
		if m.activeSpeaker == userID {
			m.activeSpeaker = anyActiveSpeaker(m.activeSpeakers)
		}
	}
	m.participantStateRevision++
	stateRevision := m.participantStateRevision
	reconnectGeneration := m.reconnectGeneration
	m.lastEventAt = time.Now().UTC()
	job := m.current
	displayName := ""
	if participant, ok := m.participants[userID]; ok {
		displayName = participant.Username
	}
	participants := m.participantsSnapshotLocked()
	m.mu.Unlock()
	m.reportParticipantsIfCurrent(job, participants, stateRevision, reconnectGeneration, true)
	m.reportActiveSpeakerIfCurrent(job, userID, displayName, speaking, stateRevision, reconnectGeneration)
	return nil
}

func (m *Manager) clearSpeakerStates(streamID string) error {
	m.mu.Lock()
	if m.current.StreamID == "" {
		m.mu.Unlock()
		return errors.New("no active stream job")
	}
	if streamID != "" && streamID != m.current.StreamID {
		m.mu.Unlock()
		return errors.New("stream_id does not match current job")
	}
	if len(m.activeSpeakers) == 0 {
		m.mu.Unlock()
		return nil
	}
	m.activeSpeakers = map[string]bool{}
	m.activeSpeaker = ""
	m.participantStateRevision++
	stateRevision := m.participantStateRevision
	reconnectGeneration := m.reconnectGeneration
	m.lastEventAt = time.Now().UTC()
	job := m.current
	participants := m.participantsSnapshotLocked()
	m.mu.Unlock()
	m.reportParticipantsIfCurrent(job, participants, stateRevision, reconnectGeneration, true)
	m.reportActiveSpeakerIfCurrent(job, "", "", false, stateRevision, reconnectGeneration)
	return nil
}

func (m *Manager) reportActiveSpeakerIfCurrent(job discord.VoiceJob, userID, displayName string, speaking bool, stateRevision uint64, generation int64) {
	m.participantReportMu.Lock()
	defer m.participantReportMu.Unlock()

	m.mu.Lock()
	current := m.current
	if m.eventsPausedLocked() || !sameWorkerEventJob(current, job) || m.reconnectGeneration != generation || m.participantStateRevision != stateRevision {
		m.mu.Unlock()
		return
	}
	reporter := m.reporter
	queue := m.workerRetry
	m.mu.Unlock()
	if reporter == nil {
		return
	}
	ctx := context.Background()
	if queue != nil {
		ctx = queue.ctx
	}
	err := publishActiveSpeaker(ctx, reporter, current, userID, displayName, speaking)
	if err != nil {
		m.recordWorkerPublishFailure("overlay.active_speaker", job.StreamID, err)
		m.enqueueWorkerEventRetry(workerEventRetryReport{
			key:                 workerSpeakerRetryKey(current),
			eventType:           "overlay.active_speaker",
			job:                 current,
			reconnectGeneration: generation,
			speakerUserID:       userID,
			speakerDisplayName:  displayName,
			speaking:            speaking,
			stateRevision:       stateRevision,
			err:                 err,
		})
	} else {
		m.supersedeWorkerEventRetry(current, generation, workerSpeakerRetryKey(current))
	}
}

func anyActiveSpeaker(active map[string]bool) string {
	for userID, speaking := range active {
		if speaking {
			return userID
		}
	}
	return ""
}
