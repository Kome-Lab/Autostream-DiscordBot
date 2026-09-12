package jobs

import (
	"context"
	"time"

	"github.com/example/autostream-discord-bot/internal/discord"
)

type ReconnectPolicy struct {
	Enabled     bool
	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
}

func (m *Manager) SetReconnectPolicy(policy ReconnectPolicy) {
	if policy.MaxAttempts <= 0 {
		policy.MaxAttempts = 3
	}
	if policy.BaseDelay < 0 {
		policy.BaseDelay = 0
	}
	if policy.MaxDelay <= 0 {
		policy.MaxDelay = 30 * time.Second
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.reconnectPolicy = policy
}

func (m *Manager) DiscordConnected() {
	m.mu.Lock()
	m.lastEventAt = time.Now().UTC()
	job := m.current
	generation := m.reconnectGeneration
	previousWorkerRetry := m.workerRetry
	var workerRetry *workerEventRetryQueue
	var participantSyncContext context.Context
	m.disconnected = false
	if job.StreamID != "" && !m.stopping {
		workerRetry = newWorkerEventRetryQueue()
		m.workerRetry = workerRetry
		participantSyncContext = m.restartParticipantSyncLocked()
	} else {
		m.workerRetry = nil
	}
	m.mu.Unlock()
	if previousWorkerRetry != nil {
		previousWorkerRetry.stopAndWait()
	}
	if workerRetry != nil {
		workerRetry.start(m)
	}
	if participantSyncContext != nil {
		go m.keepVoiceParticipantsSynced(participantSyncContext, job, generation)
	}
}

func (m *Manager) DiscordDisconnected(reason string) {
	m.mu.Lock()
	m.cancelAllAutoStartRetriesLocked()
	m.lastEventAt = time.Now().UTC()
	job := m.current
	policy := m.reconnectPolicy
	stopInProgress := m.stopping
	if m.participantSyncCancel != nil {
		m.participantSyncCancel()
		m.participantSyncCancel = nil
	}
	m.disconnected = true
	previousWorkerRetry := m.workerRetry
	m.workerRetry = nil
	m.reconnectGeneration++
	generation := m.reconnectGeneration
	m.mu.Unlock()
	if previousWorkerRetry != nil {
		previousWorkerRetry.stopAndWait()
	}
	if !stopInProgress && shouldRejoinVoice(reason, job, policy) {
		go m.rejoinVoiceWithBackoff(job, policy, generation)
	}
}

func shouldRejoinVoice(reason string, job discord.VoiceJob, policy ReconnectPolicy) bool {
	if !policy.Enabled || job.StreamID == "" || reason == "gateway_disconnect" {
		return false
	}
	return true
}

func (m *Manager) rejoinVoiceWithBackoff(job discord.VoiceJob, policy ReconnectPolicy, generation int64) {
	for attempt := 1; attempt <= policy.MaxAttempts; attempt++ {
		if delay := reconnectDelay(policy, attempt); delay > 0 {
			time.Sleep(delay)
		}
		m.mu.Lock()
		current := m.current
		if generation != m.reconnectGeneration || !sameWorkerEventJob(current, job) || m.stopping {
			m.mu.Unlock()
			return
		}
		m.voiceRejoinAttempts++
		m.mu.Unlock()
		if err := m.voice.JoinVoice(job); err == nil {
			m.mu.Lock()
			stillCurrent := generation == m.reconnectGeneration && sameWorkerEventJob(m.current, job) && !m.stopping
			previousWorkerRetry := m.workerRetry
			var workerRetry *workerEventRetryQueue
			var participantSyncContext context.Context
			if stillCurrent {
				m.lastEventAt = time.Now().UTC()
				m.disconnected = false
				workerRetry = newWorkerEventRetryQueue()
				m.workerRetry = workerRetry
				participantSyncContext = m.restartParticipantSyncLocked()
			}
			m.mu.Unlock()
			if previousWorkerRetry != nil {
				previousWorkerRetry.stopAndWait()
			}
			if stillCurrent {
				workerRetry.start(m)
				m.hydrateVoiceParticipants(job, false)
				go m.keepVoiceParticipantsSynced(participantSyncContext, job, generation)
			}
			return
		}
		m.mu.Lock()
		if generation != m.reconnectGeneration || !sameWorkerEventJob(m.current, job) {
			m.mu.Unlock()
			return
		}
		m.voiceRejoinFailures++
		m.lastEventAt = time.Now().UTC()
		m.mu.Unlock()
	}
}

func reconnectDelay(policy ReconnectPolicy, attempt int) time.Duration {
	delay := policy.BaseDelay
	if delay <= 0 || attempt <= 1 {
		return delay
	}
	for i := 1; i < attempt; i++ {
		delay *= 2
		if policy.MaxDelay > 0 && delay > policy.MaxDelay {
			return policy.MaxDelay
		}
	}
	return delay
}
