package discord

import (
	"time"

	"github.com/example/autostream-discord-bot/internal/secrets"
)

type Status struct {
	Connected                 bool    `json:"connected"`
	VoiceConnected            bool    `json:"voice_connected"`
	CurrentGuildID            string  `json:"current_guild_id,omitempty"`
	CurrentVoiceID            string  `json:"current_voice_channel_id,omitempty"`
	AudioForwardEnabled       bool    `json:"audio_forward_enabled"`
	AudioForwardActive        bool    `json:"audio_forward_active"`
	CaptionAudioForwardActive bool    `json:"caption_audio_forward_active"`
	AudioReceiving            bool    `json:"audio_receiving"`
	LastAudioAt               string  `json:"last_audio_at,omitempty"`
	LastAudioAgeSec           float64 `json:"last_audio_age_sec,omitempty"`
	AudioPacketsReceived      int64   `json:"audio_packets_received"`
	AudioPacketsForwarded     int64   `json:"audio_packets_forwarded"`
	AudioForwardErrors        int64   `json:"audio_forward_errors"`
	AudioForwardQueueDrops    int64   `json:"audio_forward_queue_drops"`
	CaptionPacketsForwarded   int64   `json:"caption_packets_forwarded"`
	CaptionForwardErrors      int64   `json:"caption_forward_errors"`
	CaptionForwardQueueDrops  int64   `json:"caption_forward_queue_drops"`
	GatewayReconnectCount     int64   `json:"gateway_reconnect_count"`
	VoiceDisconnectCount      int64   `json:"voice_disconnect_count"`
	DAVEInitialized           bool    `json:"dave_initialized"`
	DAVEReady                 bool    `json:"dave_ready"`
	DAVEWelcomeReceived       bool    `json:"dave_welcome_received"`
	DAVERosterSize            int     `json:"dave_roster_size"`
	DAVERatchetsMissing       int     `json:"dave_ratchets_missing"`
	DAVEKeyPackageResends     int64   `json:"dave_key_package_resends"`
	DAVESoftResets            int64   `json:"dave_soft_resets"`
	DAVERecoveryErrors        int64   `json:"dave_recovery_errors"`
	DAVELastRecoveryReason    string  `json:"dave_last_recovery_reason,omitempty"`
	LastForwardAt             string  `json:"last_forward_at,omitempty"`
	LastForwardAgeSec         float64 `json:"last_forward_age_sec,omitempty"`
	LastForwardError          string  `json:"last_forward_error,omitempty"`
	LastCaptionForwardError   string  `json:"last_caption_forward_error,omitempty"`
	LastError                 string  `json:"last_error,omitempty"`
}

func (c *RealClient) Status() Status {
	c.mu.Lock()
	status := c.status
	voice := c.voice
	c.mu.Unlock()
	if voice != nil {
		health := voice.DAVEHealth()
		status.DAVEInitialized = health.Initialized
		status.DAVEReady = health.EpochEstablished
		status.DAVEWelcomeReceived = health.OP30Received
		status.DAVERosterSize = health.LastRosterSize
		status.DAVERatchetsMissing = health.LastMissing
	}
	return normalizeStatus(status, time.Now().UTC())
}

func (c *RealClient) setLastError(value string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.status.LastError = secrets.SanitizeOperationalError(value, "discord operation failed")
}

func normalizeStatus(status Status, now time.Time) Status {
	if status.LastAudioAt != "" {
		if last, err := time.Parse(time.RFC3339Nano, status.LastAudioAt); err == nil {
			status.LastAudioAgeSec = maxFloat(now.Sub(last).Seconds(), 0)
			status.AudioReceiving = status.AudioReceiving && status.LastAudioAgeSec <= 5
		}
	}
	if status.LastForwardAt != "" {
		if last, err := time.Parse(time.RFC3339Nano, status.LastForwardAt); err == nil {
			status.LastForwardAgeSec = maxFloat(now.Sub(last).Seconds(), 0)
		}
	}
	return status
}

func maxFloat(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}
