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
	if m.current.StreamID != "" {
		current := m.current
		if current.GuildID == event.GuildID && current.VoiceChannelID == event.VoiceChannelID && m.hasAutoStopInFlightLocked(current.StreamID) {
			m.recordAutoStopRejoinIntentLocked(current.StreamID, event.GuildID, event.VoiceChannelID)
			m.cancelAutoStopLocked(current.StreamID)
		}
		m.mu.Unlock()
		return
	}
	if sourceStreamID := m.inFlightAutoStopStreamForVoiceLocked(event.GuildID, event.VoiceChannelID); sourceStreamID != "" {
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
	if last, ok := m.autoStartPending[streamID]; ok && now.Sub(last) < m.autoStartCooldown {
		m.mu.Unlock()
		return
	}
	m.autoStartPending[streamID] = now
	starter := m.streamStarter
	retryCtx, retryCancel := context.WithCancel(context.Background())
	retry := &autoStartRetry{
		cancel:              retryCancel,
		reconnectGeneration: m.reconnectGeneration,
		guildID:             event.GuildID,
		voiceChannelID:      event.VoiceChannelID,
	}
	m.autoStartRetries[streamID] = retry
	m.lastEventAt = now
	m.mu.Unlock()

	go func() {
		defer m.finishAutoStartRetry(streamID, retry)
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
			if successor != "" && successor != streamID {
				m.autoStartPending[successor] = time.Now().UTC()
			}
			m.mu.Unlock()
			if successor == "" || successor == streamID {
				log.Printf("Discord VC auto-start stale stream has no refreshed successor: old_stream=%s", streamID)
				return
			}
			if retryErr := starter.StartStream(successor); retryErr != nil {
				logAutoStartRetryFailure(successor, 1, retryErr, false)
			} else {
				log.Printf("Discord VC auto-start successor accepted by control panel for stream=%s (old_stream=%s)", successor, streamID)
			}
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
	defer m.mu.Unlock()
	if m.autoStartRetries[streamID] != retry || m.current.StreamID != "" || m.reconnectGeneration != retry.reconnectGeneration {
		return false
	}
	return m.matchingAutoStartStreamLocked(retry.guildID, retry.voiceChannelID) == streamID
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
