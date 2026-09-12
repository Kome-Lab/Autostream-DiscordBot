package jobs

import (
	"strings"

	"github.com/example/autostream-discord-bot/internal/discord"
)

type VoiceDefaults struct {
	GuildID          string
	VoiceChannelID   string
	TextChannelID    string
	AutoStartEnabled bool
}

func (m *Manager) SetVoiceDefaults(defaults VoiceDefaults) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.defaults = VoiceDefaults{
		GuildID:          strings.TrimSpace(defaults.GuildID),
		VoiceChannelID:   strings.TrimSpace(defaults.VoiceChannelID),
		TextChannelID:    strings.TrimSpace(defaults.TextChannelID),
		AutoStartEnabled: defaults.AutoStartEnabled,
	}
}

func (m *Manager) SetStreamVoiceDefaults(defaults map[string]VoiceDefaults) {
	m.mu.Lock()
	m.streamDefaults = map[string]VoiceDefaults{}
	targets := make([]discord.AutoStartVoiceTarget, 0, len(defaults))
	for streamID, item := range defaults {
		streamID = strings.TrimSpace(streamID)
		if streamID == "" {
			continue
		}
		m.streamDefaults[streamID] = VoiceDefaults{
			GuildID:          strings.TrimSpace(item.GuildID),
			VoiceChannelID:   strings.TrimSpace(item.VoiceChannelID),
			TextChannelID:    strings.TrimSpace(item.TextChannelID),
			AutoStartEnabled: item.AutoStartEnabled,
		}
		if item.AutoStartEnabled {
			targets = append(targets, discord.AutoStartVoiceTarget{
				StreamID:       streamID,
				GuildID:        strings.TrimSpace(item.GuildID),
				VoiceChannelID: strings.TrimSpace(item.VoiceChannelID),
			})
		}
	}
	for streamID := range m.autoStartRetries {
		item, ok := m.streamDefaults[streamID]
		if !ok || !item.AutoStartEnabled {
			m.cancelAutoStartRetryLocked(streamID)
		}
	}
	voice := m.voice
	m.mu.Unlock()
	if setter, ok := voice.(discord.AutoStartVoiceTargetSetter); ok {
		setter.SetAutoStartVoiceTargets(targets)
	}
}

func (m *Manager) ApplyVoiceDefaults(job discord.VoiceJob) discord.VoiceJob {
	m.mu.Lock()
	defaults := m.defaults
	if streamDefaults, ok := m.streamDefaults[strings.TrimSpace(job.StreamID)]; ok {
		defaults = mergeVoiceDefaults(defaults, streamDefaults)
	}
	m.mu.Unlock()
	if strings.TrimSpace(job.GuildID) == "" {
		job.GuildID = defaults.GuildID
	}
	if strings.TrimSpace(job.VoiceChannelID) == "" {
		job.VoiceChannelID = defaults.VoiceChannelID
	}
	if strings.TrimSpace(job.TextChannelID) == "" {
		job.TextChannelID = defaults.TextChannelID
	}
	return job
}

func mergeVoiceDefaults(base, override VoiceDefaults) VoiceDefaults {
	if override.GuildID != "" {
		base.GuildID = override.GuildID
	}
	if override.VoiceChannelID != "" {
		base.VoiceChannelID = override.VoiceChannelID
	}
	if override.TextChannelID != "" {
		base.TextChannelID = override.TextChannelID
	}
	if override.AutoStartEnabled {
		base.AutoStartEnabled = true
	}
	return base
}
