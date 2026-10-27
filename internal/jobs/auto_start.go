package jobs

import (
	"context"
	"errors"
	"log"
	"strings"
	"time"

	"github.com/example/autostream-discord-bot/internal/discord"
)

type StreamStarter interface {
	StartStream(streamID string) error
}

type staleAutoStartError interface {
	ControlPanelCode() string
	HTTPStatusCode() int
}

type retryableAutoStartError interface {
	RetryableAutoStart() bool
}

type autoStartRetry struct {
	assignmentEpoch     string
	cancel              context.CancelFunc
	reconnectGeneration int64
	guildID             string
	voiceChannelID      string
}

func (m *Manager) SetStreamStarter(starter StreamStarter) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.streamStarter = starter
}

// SetAutoStartRefresher supplies a best-effort runtime-config refresh for a
// VC join that arrives before a newly-created stream is visible in the
// manager's cached defaults, including the bounded reconciliation after a
// canceled auto-stop. Ordinary no-candidate joins rate limit this refresh;
// rejoin reconciliation intentionally polls within its own bounded window.
func (m *Manager) SetAutoStartRefresher(refresher func() error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.autoStartRefresher = refresher
	m.autoStartRefreshAt = time.Time{}
}

func (m *Manager) VoiceUserJoined(event discord.VoiceJoinEvent) {
	event.GuildID = strings.TrimSpace(event.GuildID)
	event.VoiceChannelID = strings.TrimSpace(event.VoiceChannelID)
	event.UserID = strings.TrimSpace(event.UserID)
	if event.GuildID == "" || event.VoiceChannelID == "" || event.UserID == "" {
		return
	}
	now := time.Now().UTC()
	m.mu.Lock()
	if event.ExistingPresence && m.current.StreamID != "" {
		m.mu.Unlock()
		return
	}
	if m.current.StreamID != "" {
		current := m.current
		if !autoStartAuthorityMatches(current.StreamID, m.currentAssignmentEpoch, event) {
			m.mu.Unlock()
			return
		}
		if current.GuildID == event.GuildID && current.VoiceChannelID == event.VoiceChannelID && m.hasAutoStopInFlightLocked(current.StreamID) {
			m.recordAutoStopRejoinIntentLocked(current.StreamID, event.GuildID, event.VoiceChannelID)
			m.cancelAutoStopLocked(current.StreamID)
		}
		m.mu.Unlock()
		return
	}
	if event.ExistingPresence && m.inFlightAutoStopStreamForVoiceLocked(event.GuildID, event.VoiceChannelID) != "" {
		m.mu.Unlock()
		return
	}
	if sourceStreamID := m.inFlightAutoStopStreamForVoiceLocked(event.GuildID, event.VoiceChannelID); sourceStreamID != "" {
		if !autoStartAuthorityMatches(sourceStreamID, m.autoStopInFlight[sourceStreamID].assignmentEpoch, event) {
			m.mu.Unlock()
			return
		}
		// The Panel has already called our local /stop endpoint, so canceling
		// the parent request at this point can interrupt its remaining service
		// dispatch. Retain the rejoin intent and wait for that request to return
		// after the successor has been rearmed instead.
		m.recordAutoStopRejoinIntentLocked(sourceStreamID, event.GuildID, event.VoiceChannelID)
		m.mu.Unlock()
		return
	}
	streamID := m.matchingAutoStartStreamLocked(event.GuildID, event.VoiceChannelID)
	refresher := m.autoStartRefresher
	shouldRefresh := streamID == "" && refresher != nil && (m.autoStartRefreshWait <= 0 || m.autoStartRefreshAt.IsZero() || now.Sub(m.autoStartRefreshAt) >= m.autoStartRefreshWait)
	if shouldRefresh {
		m.autoStartRefreshAt = now
	}
	m.mu.Unlock()

	if shouldRefresh {
		if err := refresher(); err != nil {
			log.Printf("Discord VC auto-start runtime config refresh failed for guild=%s voice=%s: %v", event.GuildID, event.VoiceChannelID, err)
			return
		}
		m.mu.Lock()
		if m.current.StreamID != "" {
			m.mu.Unlock()
			return
		}
		streamID = m.matchingAutoStartStreamLocked(event.GuildID, event.VoiceChannelID)
	} else {
		m.mu.Lock()
	}
	if streamID != "" && !m.autoStartEventAuthorizedLocked(streamID, event) {
		m.mu.Unlock()
		return
	}
	if streamID == "" {
		if m.shouldLogAutoStartLocked("no-candidate", now) {
			log.Printf("Discord VC auto-start ignored: no matching waiting stream for guild=%s voice=%s configured_streams=%d", event.GuildID, event.VoiceChannelID, len(m.streamDefaults))
		}
		m.mu.Unlock()
		return
	}
	if m.streamStarter == nil {
		if m.shouldLogAutoStartLocked("starter-missing:"+streamID, now) {
			log.Printf("Discord VC auto-start unavailable: Control Panel stream starter is not configured for stream=%s (check panel-managed node config)", streamID)
		}
		m.mu.Unlock()
		return
	}
	if _, stopped := m.autoStartStopped[streamID]; stopped {
		if event.ExistingPresence {
			m.mu.Unlock()
			return
		}
		delete(m.autoStartStopped, streamID)
		delete(m.autoStartPending, streamID)
	}
	if m.autoStartRetries[streamID] != nil {
		m.mu.Unlock()
		return
	}
	if last, ok := m.autoStartPending[streamID]; ok && now.Sub(last) < m.autoStartCooldown {
		m.mu.Unlock()
		return
	}
	m.autoStartPending[streamID] = now
	starter := m.streamStarter
	retryCtx, retryCancel := context.WithCancel(context.Background())
	retry := &autoStartRetry{
		assignmentEpoch:     m.streamDefaults[streamID].AssignmentEpoch,
		cancel:              retryCancel,
		reconnectGeneration: m.reconnectGeneration,
		guildID:             event.GuildID,
		voiceChannelID:      event.VoiceChannelID,
	}
	m.autoStartRetries[streamID] = retry
	m.lastEventAt = now
	log.Printf("Discord auto-start diagnostic: stage=reserved stream_id=%s assignment_epoch=%s existing_presence=%t", streamID, retry.assignmentEpoch, event.ExistingPresence)
	m.mu.Unlock()

	go func() {
		defer m.finishAutoStartRetry(streamID, retry)
		if retryCtx.Err() != nil || !m.autoStartRetryStillValid(streamID, retry) {
			return
		}
		if err := starter.StartStream(streamID); err != nil {
			if isRetryableAutoStartError(err) {
				logAutoStartRetryFailure(streamID, 1, err, true)
				m.retryAutoStart(retryCtx, retry, starter, streamID)
				return
			}
			logAutoStartRetryFailure(streamID, 1, err, false)
			if !isStaleAutoStartError(err) || refresher == nil {
				return
			}
			if refreshErr := refresher(); refreshErr != nil {
				log.Printf("Discord VC auto-start stale-stream refresh failed: stream=%s error_class=refresh_failed", streamID)
				return
			}
			m.mu.Lock()
			if m.current.StreamID != "" {
				m.mu.Unlock()
				return
			}
			successor := m.matchingAutoStartStreamLocked(event.GuildID, event.VoiceChannelID)
			m.mu.Unlock()
			if successor == "" || successor == streamID {
				log.Printf("Discord VC auto-start stale stream has no refreshed successor: old_stream=%s", streamID)
				return
			}
			// Refresh can synchronously evaluate existing occupants. Re-enter the
			// same reservation path as Gateway events instead of dispatching twice.
			m.VoiceUserJoined(event)
			return
		}
		// This receipt means only that the Control Panel accepted the request;
		// Encoder/YouTube lifecycle readiness is verified separately.
		log.Printf("Discord VC auto-start accepted by control panel for stream=%s", streamID)
	}()
}

func isStaleAutoStartError(err error) bool {
	var classified staleAutoStartError
	if !errors.As(err, &classified) {
		return false
	}
	return classified.HTTPStatusCode() == 404 && classified.ControlPanelCode() == "not_found"
}

func isRetryableAutoStartError(err error) bool {
	var classified retryableAutoStartError
	if !errors.As(err, &classified) {
		return false
	}
	return classified.RetryableAutoStart()
}

func logAutoStartRetryFailure(streamID string, retryCount int, err error, retryable bool) {
	statusCode := 0
	code := ""
	var classified staleAutoStartError
	if errors.As(err, &classified) {
		statusCode = classified.HTTPStatusCode()
		code = classified.ControlPanelCode()
	}
	if statusCode > 0 {
		if code != "" {
			log.Printf("Discord VC auto-start request failed: stream=%s error_class=http_status http_status=%d code=%s retryable=%t retry_count=%d", streamID, statusCode, code, retryable, retryCount)
			return
		}
		log.Printf("Discord VC auto-start request failed: stream=%s error_class=http_status http_status=%d retryable=%t retry_count=%d", streamID, statusCode, retryable, retryCount)
		return
	}
	log.Printf("Discord VC auto-start request failed: stream=%s error_class=transport_or_unknown retryable=%t retry_count=%d", streamID, retryable, retryCount)
}

func (m *Manager) retryAutoStart(ctx context.Context, retry *autoStartRetry, starter StreamStarter, streamID string) {
	m.mu.Lock()
	delays := append([]time.Duration(nil), m.autoStartRetryDelays...)
	m.mu.Unlock()
	for index, delay := range delays {
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return
		case <-timer.C:
		}
		if !m.autoStartRetryStillValid(streamID, retry) {
			return
		}
		attempt := index + 2
		err := starter.StartStream(streamID)
		if err == nil {
			log.Printf("Discord VC auto-start retry accepted by control panel: stream=%s retry_count=%d", streamID, attempt)
			return
		}
		retryable := isRetryableAutoStartError(err)
		logAutoStartRetryFailure(streamID, attempt, err, retryable)
		if !retryable {
			return
		}
	}
	log.Printf("Discord VC auto-start retry exhausted: stream=%s retry_count=%d", streamID, len(delays)+1)
}

func (m *Manager) autoStartRetryStillValid(streamID string, retry *autoStartRetry) bool {
	m.mu.Lock()
	valid := m.autoStartRetryStillValidLocked(streamID, retry)
	m.mu.Unlock()
	if !valid {
		return false
	}
	source, ok := m.voice.(discord.AutoStartVoicePresenceSource)
	if !ok {
		return true
	}
	// The initial Gateway callback may have been queued before the last human
	// left. Re-read current presence for the first request and every bounded
	// retry, without holding the manager lock across a client callback.
	present, known := source.AutoStartVoicePresence(discord.AutoStartVoiceTarget{
		StreamID: streamID, AssignmentEpoch: retry.assignmentEpoch,
		GuildID: retry.guildID, VoiceChannelID: retry.voiceChannelID,
	})
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.autoStartRetryStillValidLocked(streamID, retry) {
		return false
	}
	reason := "human_present"
	if !known {
		reason = "gateway_view_unavailable"
	} else if !present {
		reason = "human_absent"
	}
	log.Printf("Discord auto-start diagnostic: stage=presence stream_id=%s assignment_epoch=%s reason=%s", streamID, retry.assignmentEpoch, reason)
	if !known || !present {
		m.cancelAutoStartRetryLocked(streamID)
		// No start was accepted. A genuine next join must not inherit this
		// canceled attempt's cooldown.
		delete(m.autoStartPending, streamID)
		return false
	}
	return true
}

func (m *Manager) autoStartRetryStillValidLocked(streamID string, retry *autoStartRetry) bool {
	if m.autoStartRetries[streamID] != retry || m.current.StreamID != "" || m.reconnectGeneration != retry.reconnectGeneration {
		return false
	}
	return m.streamDefaults[streamID].AssignmentEpoch == retry.assignmentEpoch && m.matchingAutoStartStreamLocked(retry.guildID, retry.voiceChannelID) == streamID
}

func (m *Manager) finishAutoStartRetry(streamID string, retry *autoStartRetry) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.autoStartRetries[streamID] == retry {
		delete(m.autoStartRetries, streamID)
	}
	retry.cancel()
}

func (m *Manager) cancelAutoStartRetryLocked(streamID string) {
	if retry := m.autoStartRetries[streamID]; retry != nil {
		retry.cancel()
		delete(m.autoStartRetries, streamID)
	}
}

func (m *Manager) cancelAllAutoStartRetriesLocked() {
	for streamID := range m.autoStartRetries {
		m.cancelAutoStartRetryLocked(streamID)
	}
}

func (m *Manager) shouldLogAutoStartLocked(key string, now time.Time) bool {
	if key == m.lastAutoStartLogKey && !m.lastAutoStartLogAt.IsZero() && now.Sub(m.lastAutoStartLogAt) < 10*time.Second {
		return false
	}
	m.lastAutoStartLogKey = key
	m.lastAutoStartLogAt = now
	return true
}

func (m *Manager) matchingAutoStartStreamLocked(guildID, voiceChannelID string) string {
	guildID = strings.TrimSpace(guildID)
	voiceChannelID = strings.TrimSpace(voiceChannelID)
	if guildID == "" || voiceChannelID == "" {
		return ""
	}
	matched := ""
	for streamID, defaults := range m.streamDefaults {
		if !defaults.AutoStartEnabled {
			continue
		}
		if defaults.GuildID != guildID || defaults.VoiceChannelID != voiceChannelID {
			continue
		}
		if matched != "" {
			return ""
		}
		matched = streamID
	}
	return matched
}

func (m *Manager) autoStartEventAuthorizedLocked(streamID string, event discord.VoiceJoinEvent) bool {
	return autoStartAuthorityMatches(streamID, m.streamDefaults[streamID].AssignmentEpoch, event)
}

func autoStartAuthorityMatches(streamID, epoch string, event discord.VoiceJoinEvent) bool {
	if event.Authority == nil {
		return epoch == ""
	}
	return event.Authority.StreamID == streamID && event.Authority.AssignmentEpoch == epoch
}
