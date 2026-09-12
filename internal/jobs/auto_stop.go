package jobs

import (
	"context"
	"errors"
	"log"
	"strings"
	"time"

	"github.com/example/autostream-discord-bot/internal/discord"
)

type StreamStopper interface {
	StopStream(streamID string) error
}

// ContextStreamStopper lets VC-empty auto-stop cancel an in-flight external
// request when a participant returns. StreamStopper remains supported for
// callers that do not make a cancelable request.
type ContextStreamStopper interface {
	StopStreamContext(ctx context.Context, streamID string) error
}

type autoStopInFlight struct {
	generation uint64
	cancel     context.CancelFunc
	job        discord.VoiceJob
}

type autoStopObservation struct {
	reason                   string
	source                   string
	participantCount         int
	snapshotRevision         uint64
	participantStateRevision uint64
	reconnectGeneration      int64
	autoStopGeneration       uint64
	emptyRevalidationPassed  bool
}

func (m *Manager) SetStreamStopper(stopper StreamStopper) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.streamStopper = stopper
}

// updateAutoStopForParticipantSetLocked applies the single empty/non-empty
// policy to both transition events and authoritative snapshot replacement.
// The caller must hold m.mu.
func (m *Manager) updateAutoStopForParticipantSetLocked(job discord.VoiceJob, now time.Time, source string, snapshotRevision uint64) (StreamStopper, bool, autoStopObservation) {
	stopper := m.streamStopper
	if len(m.participants) > 0 {
		if m.hasAutoStopInFlightLocked(job.StreamID) {
			m.recordAutoStopRejoinIntentLocked(job.StreamID, job.GuildID, job.VoiceChannelID)
		}
		m.cancelAutoStopLocked(job.StreamID)
		return stopper, false, autoStopObservation{}
	}
	if stopper == nil {
		return nil, false, autoStopObservation{}
	}
	lastAttempt := m.autoStopLastAttempt[job.StreamID]
	if m.autoStopPending[job.StreamID] || (!lastAttempt.IsZero() && now.Sub(lastAttempt) < m.autoStopCooldown) {
		return stopper, false, autoStopObservation{}
	}
	m.autoStopPending[job.StreamID] = true
	m.autoStopGeneration[job.StreamID]++
	autoStopGeneration := m.autoStopGeneration[job.StreamID]
	m.autoStopAttempts[job.StreamID] = 0
	return stopper, true, autoStopObservation{
		reason:                   "empty_participants",
		source:                   source,
		participantCount:         len(m.participants),
		snapshotRevision:         snapshotRevision,
		participantStateRevision: m.participantStateRevision,
		reconnectGeneration:      m.reconnectGeneration,
		autoStopGeneration:       autoStopGeneration,
	}
}

func (m *Manager) autoStopWhenEmpty(streamID string, observation autoStopObservation, stopper StreamStopper, delay time.Duration) {
	if delay < 0 {
		delay = 0
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	<-timer.C

	m.mu.Lock()
	if !m.autoStopStillValidLocked(streamID, observation.autoStopGeneration) {
		m.mu.Unlock()
		return
	}
	job := m.current
	reconnectGeneration := m.reconnectGeneration
	participantStateRevision := m.participantStateRevision
	emptyConfirmDelay := m.autoStopEmptyConfirmDelay
	m.mu.Unlock()

	// Discord's gateway State can briefly omit a participant during reconnects
	// or voice-state churn. Re-read the current full view after the normal
	// empty-VC grace period for both snapshot- and event-derived emptiness. A
	// missing snapshot is non-authoritative and must never be treated as proof
	// that the channel is empty.
	if source, ok := m.voice.(discord.ParticipantSnapshotSource); ok {
		snapshot, known := source.SnapshotVoiceParticipants(job)
		if !known {
			m.mu.Lock()
			if m.autoStopStillValidForJobLocked(streamID, observation.autoStopGeneration, job, reconnectGeneration) {
				m.cancelAutoStopLocked(streamID)
			}
			m.mu.Unlock()
			log.Printf("Discord VC auto-stop canceled: event=revalidation_unavailable stream_id=%s reason=participant_snapshot_unavailable source=%s reconnect_generation=%d auto_stop_generation=%d", streamID, observation.source, reconnectGeneration, observation.autoStopGeneration)
			return
		}
		if len(snapshot.Participants) > 0 {
			m.participantsSynced(snapshot, participantSnapshotApplyOptions{
				expectedGeneration:    reconnectGeneration,
				requireGeneration:     true,
				authoritativeReplay:   true,
				expectedStateRevision: participantStateRevision,
				requireStateRevision:  true,
			})
			m.mu.Lock()
			if m.autoStopStillValidForJobLocked(streamID, observation.autoStopGeneration, job, reconnectGeneration) {
				m.cancelAutoStopLocked(streamID)
			}
			m.mu.Unlock()
			log.Printf("Discord VC auto-stop canceled: event=revalidation_nonempty stream_id=%s source=%s participant_count=%d snapshot_revision=%d reconnect_generation=%d auto_stop_generation=%d", streamID, observation.source, len(snapshot.Participants), snapshot.Revision, reconnectGeneration, observation.autoStopGeneration)
			return
		}
		observation.snapshotRevision = snapshot.Revision
		observation.participantStateRevision = participantStateRevision
		m.mu.Lock()
		stillValid := m.autoStopStillValidForJobLocked(streamID, observation.autoStopGeneration, job, reconnectGeneration)
		m.mu.Unlock()
		if !stillValid {
			return
		}
		if !observation.emptyRevalidationPassed {
			observation.emptyRevalidationPassed = true
			log.Printf("Discord VC auto-stop deferred: event=revalidation_pending stream_id=%s source=%s participant_count=0 snapshot_revision=%d reconnect_generation=%d auto_stop_generation=%d next_delay_ms=%d", streamID, observation.source, snapshot.Revision, reconnectGeneration, observation.autoStopGeneration, emptyConfirmDelay.Milliseconds())
			go m.autoStopWhenEmpty(streamID, observation, stopper, emptyConfirmDelay)
			return
		}
		log.Printf("Discord VC auto-stop confirmed: event=revalidation_empty stream_id=%s source=%s participant_count=0 snapshot_revision=%d reconnect_generation=%d auto_stop_generation=%d", streamID, observation.source, snapshot.Revision, reconnectGeneration, observation.autoStopGeneration)
	}

	m.mu.Lock()
	if !m.autoStopStillValidForJobLocked(streamID, observation.autoStopGeneration, job, reconnectGeneration) {
		m.mu.Unlock()
		return
	}
	m.autoStopLastAttempt[streamID] = time.Now().UTC()
	attempt := m.autoStopAttempts[streamID] + 1
	ctx, cancel := context.WithCancel(context.Background())
	m.autoStopInFlight[streamID] = autoStopInFlight{generation: observation.autoStopGeneration, cancel: cancel, job: m.current}
	m.mu.Unlock()
	logAutoStopObservation("requested", streamID, observation, delay, attempt)

	err := stopStreamWithContext(ctx, stopper, streamID)
	m.mu.Lock()
	m.clearAutoStopInFlightLocked(streamID, observation.autoStopGeneration)
	stillValid := m.autoStopStillValidLocked(streamID, observation.autoStopGeneration)
	reconcileRejoin := !stillValid && m.hasAutoStopRejoinIntentForStreamLocked(streamID)
	m.mu.Unlock()
	if reconcileRejoin {
		m.scheduleAutoStopRejoinReconciliation(streamID)
	}
	if err != nil {
		if !stillValid {
			return
		}
		log.Printf("Discord VC auto-stop request failed for stream=%s: %v", streamID, err)
		m.scheduleAutoStopRetry(streamID, observation.autoStopGeneration, stopper, err, observation)
		return
	}
	if !stillValid {
		return
	}
	m.mu.Lock()
	m.finishAutoStopLocked(streamID, observation.autoStopGeneration)
	m.mu.Unlock()
}

func stopStreamWithContext(ctx context.Context, stopper StreamStopper, streamID string) error {
	if contextual, ok := stopper.(ContextStreamStopper); ok {
		return contextual.StopStreamContext(ctx, streamID)
	}
	return stopper.StopStream(streamID)
}

func (m *Manager) scheduleAutoStopRetry(streamID string, generation uint64, stopper StreamStopper, err error, observation autoStopObservation) {
	if !shouldRetryAutoStop(err) {
		m.mu.Lock()
		m.finishAutoStopLocked(streamID, generation)
		m.mu.Unlock()
		return
	}

	m.mu.Lock()
	if !m.autoStopStillValidLocked(streamID, generation) {
		m.mu.Unlock()
		return
	}
	attempt := m.autoStopAttempts[streamID]
	if attempt >= len(m.autoStopRetryDelays) {
		m.finishAutoStopLocked(streamID, generation)
		m.mu.Unlock()
		log.Printf("Discord VC auto-stop exhausted retries for stream=%s", streamID)
		return
	}
	delay := m.autoStopRetryDelays[attempt]
	m.autoStopAttempts[streamID] = attempt + 1
	m.mu.Unlock()

	log.Printf("Discord VC auto-stop retry scheduled for stream=%s attempt=%d delay=%s", streamID, attempt+1, delay)
	go m.autoStopWhenEmpty(streamID, observation, stopper, delay)
}

func logAutoStopObservation(event, streamID string, observation autoStopObservation, delay time.Duration, attempt int) {
	log.Printf(
		"Discord VC auto-stop: event=%s stream_id=%s reason=%s source=%s participant_count=%d snapshot_revision=%d participant_state_revision=%d reconnect_generation=%d auto_stop_generation=%d attempt=%d delay_ms=%d",
		event,
		streamID,
		observation.reason,
		observation.source,
		observation.participantCount,
		observation.snapshotRevision,
		observation.participantStateRevision,
		observation.reconnectGeneration,
		observation.autoStopGeneration,
		attempt,
		delay.Milliseconds(),
	)
}

func (m *Manager) autoStopStillValidLocked(streamID string, generation uint64) bool {
	return m.autoStopGeneration[streamID] == generation &&
		m.autoStopPending[streamID] &&
		m.current.StreamID == streamID &&
		len(m.participants) == 0 &&
		m.streamStopper != nil
}

func (m *Manager) autoStopStillValidForJobLocked(streamID string, generation uint64, job discord.VoiceJob, reconnectGeneration int64) bool {
	return m.autoStopStillValidLocked(streamID, generation) &&
		m.reconnectGeneration == reconnectGeneration &&
		sameWorkerEventJob(m.current, job)
}

func (m *Manager) finishAutoStopLocked(streamID string, generation uint64) {
	m.clearAutoStopInFlightLocked(streamID, generation)
	if m.autoStopGeneration[streamID] != generation {
		return
	}
	delete(m.autoStopPending, streamID)
	delete(m.autoStopAttempts, streamID)
}

func (m *Manager) cancelAutoStopLocked(streamID string) {
	streamID = strings.TrimSpace(streamID)
	if streamID == "" {
		return
	}
	if request, ok := m.autoStopInFlight[streamID]; ok {
		request.cancel()
		delete(m.autoStopInFlight, streamID)
	}
	m.invalidateAutoStopLocked(streamID)
}

// invalidateAutoStopLocked prevents timers and retries from remaining valid
// without canceling a possibly active Control Panel request. This is used by
// the inbound Panel stop callback so it cannot tear down its own parent
// request before the rest of the service stop/rearm transaction completes.
func (m *Manager) invalidateAutoStopLocked(streamID string) {
	streamID = strings.TrimSpace(streamID)
	if streamID == "" {
		return
	}
	m.autoStopGeneration[streamID]++
	delete(m.autoStopPending, streamID)
	delete(m.autoStopAttempts, streamID)
	delete(m.autoStopLastAttempt, streamID)
}

func (m *Manager) hasPendingAutoStopLocked(streamID string) bool {
	if m.autoStopPending[streamID] {
		return true
	}
	return m.hasAutoStopInFlightLocked(streamID)
}

func (m *Manager) hasAutoStopInFlightLocked(streamID string) bool {
	_, inFlight := m.autoStopInFlight[streamID]
	return inFlight
}

func (m *Manager) clearAutoStopInFlightLocked(streamID string, generation uint64) {
	request, ok := m.autoStopInFlight[streamID]
	if !ok || request.generation != generation {
		return
	}
	request.cancel()
	delete(m.autoStopInFlight, streamID)
}

type autoStopRetryability interface {
	RetryableAutoStop() bool
}

func shouldRetryAutoStop(err error) bool {
	var classified autoStopRetryability
	if errors.As(err, &classified) {
		return classified.RetryableAutoStop()
	}
	return false
}
