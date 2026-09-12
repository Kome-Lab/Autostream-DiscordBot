package jobs

import (
	"log"
	"strings"
	"time"
)

// autoStopRejoinIntent records a Discord VC join that happened while the
// empty-channel stop request was pending. A Panel stop can race with its
// cancellation, so this intent is retained until the freshly rearmed waiting
// stream can be reconciled or a bounded retry window proves it is not needed.
type autoStopRejoinIntent struct {
	GuildID        string
	VoiceChannelID string
	SourceStreamID string
	Sequence       uint64
}

// inFlightAutoStopStreamForVoiceLocked finds the still-unanswered outbound
// auto-stop request that owns a VC. The current job may already be empty
// because the Panel has called this Bot's /stop endpoint while it continues
// dispatching Encoder/Worker and rearming the successor. Do not guess when
// more than one stale request claims the same VC.
func (m *Manager) inFlightAutoStopStreamForVoiceLocked(guildID, voiceChannelID string) string {
	guildID = strings.TrimSpace(guildID)
	voiceChannelID = strings.TrimSpace(voiceChannelID)
	if guildID == "" || voiceChannelID == "" {
		return ""
	}
	matched := ""
	for streamID, request := range m.autoStopInFlight {
		if request.job.GuildID != guildID || request.job.VoiceChannelID != voiceChannelID {
			continue
		}
		if matched != "" && matched != streamID {
			return ""
		}
		matched = streamID
	}
	return matched
}

func (m *Manager) recordAutoStopRejoinIntentLocked(sourceStreamID, guildID, voiceChannelID string) {
	sourceStreamID = strings.TrimSpace(sourceStreamID)
	guildID = strings.TrimSpace(guildID)
	voiceChannelID = strings.TrimSpace(voiceChannelID)
	if sourceStreamID == "" || guildID == "" || voiceChannelID == "" {
		return
	}
	key := autoStopRejoinIntentKey(guildID, voiceChannelID)
	if existing, ok := m.rejoinIntents[key]; ok && existing.SourceStreamID == sourceStreamID {
		return
	}
	m.rejoinIntentSequence++
	m.rejoinIntents[key] = autoStopRejoinIntent{
		GuildID:        guildID,
		VoiceChannelID: voiceChannelID,
		SourceStreamID: sourceStreamID,
		Sequence:       m.rejoinIntentSequence,
	}
}

func autoStopRejoinIntentKey(guildID, voiceChannelID string) string {
	return strings.TrimSpace(guildID) + "\x00" + strings.TrimSpace(voiceChannelID)
}

func firstNonEmptyTrimmed(values ...string) string {
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}

func (m *Manager) autoStopRejoinIntentForStreamLocked(streamID string) (string, autoStopRejoinIntent, bool) {
	streamID = strings.TrimSpace(streamID)
	for key, intent := range m.rejoinIntents {
		if intent.SourceStreamID == streamID {
			return key, intent, true
		}
	}
	return "", autoStopRejoinIntent{}, false
}

func (m *Manager) hasAutoStopRejoinIntentForStreamLocked(streamID string) bool {
	_, _, ok := m.autoStopRejoinIntentForStreamLocked(streamID)
	return ok
}

func (m *Manager) scheduleAutoStopRejoinReconciliation(streamID string) {
	streamID = strings.TrimSpace(streamID)
	if streamID == "" {
		return
	}
	m.mu.Lock()
	if !m.hasAutoStopRejoinIntentForStreamLocked(streamID) || m.rejoinReconcileRun[streamID] {
		m.mu.Unlock()
		return
	}
	m.rejoinReconcileRun[streamID] = true
	m.mu.Unlock()
	go m.reconcileAutoStopRejoin(streamID)
}

func (m *Manager) reconcileAutoStopRejoin(sourceStreamID string) {
	defer func() {
		m.mu.Lock()
		delete(m.rejoinReconcileRun, sourceStreamID)
		m.mu.Unlock()
	}()

	m.mu.Lock()
	delays := append([]time.Duration(nil), m.rejoinReconcileDelay...)
	m.mu.Unlock()
	for _, delay := range delays {
		if delay > 0 {
			timer := time.NewTimer(delay)
			<-timer.C
		}

		m.mu.Lock()
		key, intent, ok := m.autoStopRejoinIntentForStreamLocked(sourceStreamID)
		refresher := m.autoStartRefresher
		m.mu.Unlock()
		if !ok {
			return
		}
		if refresher != nil {
			if err := refresher(); err != nil {
				log.Printf("Discord VC auto-stop rejoin runtime config refresh failed for stream=%s: %v", sourceStreamID, err)
				continue
			}
		}

		m.mu.Lock()
		currentKey, currentIntent, stillCurrent := m.autoStopRejoinIntentForStreamLocked(sourceStreamID)
		if !stillCurrent || currentKey != key || currentIntent.Sequence != intent.Sequence {
			m.mu.Unlock()
			return
		}
		if m.current.StreamID != "" {
			if m.current.StreamID != sourceStreamID {
				delete(m.rejoinIntents, key)
				m.mu.Unlock()
				return
			}
			m.mu.Unlock()
			continue
		}
		rearmed := m.matchingAutoStartStreamLocked(intent.GuildID, intent.VoiceChannelID)
		if rearmed == "" || rearmed != sourceStreamID || m.streamStarter == nil {
			m.mu.Unlock()
			continue
		}
		now := time.Now().UTC()
		if last, pending := m.autoStartPending[rearmed]; pending && now.Sub(last) < m.autoStartCooldown {
			delete(m.rejoinIntents, key)
			m.mu.Unlock()
			return
		}
		m.autoStartPending[rearmed] = now
		m.lastEventAt = now
		starter := m.streamStarter
		delete(m.rejoinIntents, key)
		m.mu.Unlock()

		go func() {
			if err := starter.StartStream(rearmed); err != nil {
				log.Printf("Discord VC auto-stop rejoin request failed for stream=%s: %v", rearmed, err)
			}
		}()
		return
	}

	m.mu.Lock()
	if key, intent, ok := m.autoStopRejoinIntentForStreamLocked(sourceStreamID); ok {
		delete(m.rejoinIntents, key)
		log.Printf("Discord VC auto-stop rejoin reconciliation exhausted for stream=%s guild=%s voice=%s", sourceStreamID, intent.GuildID, intent.VoiceChannelID)
	}
	m.mu.Unlock()
}
