package jobs

import (
	"time"

	"github.com/example/autostream-discord-bot/internal/discord"
)

type Status struct {
	CurrentJob       *discord.VoiceJob  `json:"current_job,omitempty"`
	CurrentStreamID  string             `json:"current_stream_id,omitempty"`
	StartedAt        *time.Time         `json:"started_at,omitempty"`
	Discord          discord.Status     `json:"discord"`
	Metrics          map[string]float64 `json:"metrics"`
	ParticipantCount int                `json:"participant_count"`
	ActiveSpeakerID  string             `json:"active_speaker_id,omitempty"`
	LastEventAt      *time.Time         `json:"last_event_at,omitempty"`
}

func (m *Manager) Status() Status {
	m.mu.Lock()
	defer m.mu.Unlock()
	discordStatus := m.voice.Status()
	status := Status{
		Discord:          discordStatus,
		Metrics:          metricsFromStatus(discordStatus, len(m.participants)),
		ParticipantCount: len(m.participants),
		ActiveSpeakerID:  m.activeSpeaker,
	}
	status.Metrics["discord.worker_event_publish_failures_total"] = float64(m.workerFailures)
	status.Metrics["discord.voice_rejoin_attempts_total"] = float64(m.voiceRejoinAttempts)
	status.Metrics["discord.voice_rejoin_failures_total"] = float64(m.voiceRejoinFailures)
	if m.current.StreamID != "" {
		job := m.current
		job.EncoderAudioURL = ""
		job.CaptionAudioURL = ""
		job.CaptionAudioToken = ""
		job.StreamIngestToken = ""
		job.WorkerEventsURL = ""
		job.WorkerEventsToken = ""
		status.CurrentJob = &job
		status.CurrentStreamID = job.StreamID
		startedAt := m.startedAt
		status.StartedAt = &startedAt
	}
	if !m.lastEventAt.IsZero() {
		lastEventAt := m.lastEventAt
		status.LastEventAt = &lastEventAt
	}
	return status
}

func (m *Manager) Metrics() map[string]float64 {
	status := m.Status()
	return status.Metrics
}

func metricsFromStatus(status discord.Status, participantCount int) map[string]float64 {
	metrics := map[string]float64{
		"discord.gateway_connected":                 boolMetric(status.Connected),
		"discord.voice_connected":                   boolMetric(status.VoiceConnected),
		"discord.audio_forward_enabled":             boolMetric(status.AudioForwardEnabled),
		"discord.audio_forward_active":              boolMetric(status.AudioForwardActive),
		"discord.caption_audio_forward_active":      boolMetric(status.CaptionAudioForwardActive),
		"discord.audio_receiving":                   boolMetric(status.AudioReceiving),
		"discord.participant_count":                 float64(participantCount),
		"discord.audio_packets_total":               float64(status.AudioPacketsReceived),
		"discord.audio_forwarded_total":             float64(status.AudioPacketsForwarded),
		"discord.audio_forward_errors_total":        float64(status.AudioForwardErrors),
		"discord.audio_forward_queue_drops_total":   float64(status.AudioForwardQueueDrops),
		"discord.caption_packets_forwarded_total":   float64(status.CaptionPacketsForwarded),
		"discord.caption_forward_errors_total":      float64(status.CaptionForwardErrors),
		"discord.caption_forward_queue_drops_total": float64(status.CaptionForwardQueueDrops),
		"discord.reconnect_count":                   float64(status.GatewayReconnectCount),
		"discord.voice_disconnect_count":            float64(status.VoiceDisconnectCount),
		"discord.dave_initialized":                  boolMetric(status.DAVEInitialized),
		"discord.dave_ready":                        boolMetric(status.DAVEReady),
		"discord.dave_welcome_received":             boolMetric(status.DAVEWelcomeReceived),
		"discord.dave_roster_size":                  float64(status.DAVERosterSize),
		"discord.dave_ratchets_missing":             float64(status.DAVERatchetsMissing),
		"discord.dave_key_package_resends_total":    float64(status.DAVEKeyPackageResends),
		"discord.dave_soft_resets_total":            float64(status.DAVESoftResets),
		"discord.dave_recovery_errors_total":        float64(status.DAVERecoveryErrors),
	}
	if status.LastAudioAgeSec > 0 {
		metrics["discord.audio_last_packet_age_sec"] = status.LastAudioAgeSec
	}
	if status.LastForwardAgeSec > 0 {
		metrics["discord.audio_last_forward_age_sec"] = status.LastForwardAgeSec
	}
	return metrics
}

func boolMetric(value bool) float64 {
	if value {
		return 1
	}
	return 0
}
