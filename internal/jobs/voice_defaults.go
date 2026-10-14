package jobs

import (
	"errors"
	"strings"

	"github.com/example/autostream-discord-bot/internal/discord"
)

func (m *Manager) RefreshRuntimeConfig(refresh func() error) error {
	m.runtimeRefreshMu.Lock()
	defer m.runtimeRefreshMu.Unlock()
	return refresh()
}

func (m *Manager) ReconcileGateway(cfg discord.Config, selected bool) error {
	if client, ok := m.voice.(interface {
		ReconcileGateway(discord.Config, bool) error
	}); ok {
		return client.ReconcileGateway(cfg, selected)
	}
	if selected {
		return errors.New("runtime Gateway reconciliation unavailable")
	}
	return nil
}

// Resolve each selected credential reference once. Runtime secrets are leased;
// polling the same reference must not reacquire its lease or disconnect a VC.
func (m *Manager) ReconcileGatewayCredential(reference string, selected bool, resolve func() (string, error)) error {
	m.gatewayCredentialMu.Lock()
	defer m.gatewayCredentialMu.Unlock()
	if !selected {
		// Disconnect immediately, but keep the last reference-scoped credential.
		// A rapid reassignment of that same reference cannot reacquire its active
		// Panel lease. Current assignment authority is still required before use;
		// a changed reference/fingerprint always resolves a new credential.
		return m.ReconcileGateway(discord.Config{}, false)
	}
	if strings.TrimSpace(reference) == "" {
		return errors.New("selected Bot credential reference unavailable")
	}
	if reference != m.gatewayCredentialReference || m.gatewayCredentialToken == "" {
		token, err := resolve()
		if err != nil || strings.TrimSpace(token) == "" {
			_ = m.ReconcileGateway(discord.Config{}, false)
			return errors.New("selected Bot credential unavailable")
		}
		m.gatewayCredentialReference, m.gatewayCredentialToken = reference, token
	}
	return m.ReconcileGateway(discord.Config{BotToken: m.gatewayCredentialToken}, true)
}

type VoiceDefaults struct {
	AssignmentEpoch  string
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
	previousDefaults := m.streamDefaults
	m.streamDefaults = map[string]VoiceDefaults{}
	targets := make([]discord.AutoStartVoiceTarget, 0, len(defaults))
	for streamID, item := range defaults {
		streamID = strings.TrimSpace(streamID)
		if streamID == "" {
			continue
		}
		if old, ok := previousDefaults[streamID]; ok && old.AssignmentEpoch != item.AssignmentEpoch {
			delete(m.autoStartPending, streamID)
		}
		m.streamDefaults[streamID] = VoiceDefaults{
			AssignmentEpoch:  item.AssignmentEpoch,
			GuildID:          strings.TrimSpace(item.GuildID),
			VoiceChannelID:   strings.TrimSpace(item.VoiceChannelID),
			TextChannelID:    strings.TrimSpace(item.TextChannelID),
			AutoStartEnabled: item.AutoStartEnabled,
		}
		if item.AutoStartEnabled {
			targets = append(targets, discord.AutoStartVoiceTarget{
				StreamID:        streamID,
				AssignmentEpoch: item.AssignmentEpoch,
				GuildID:         strings.TrimSpace(item.GuildID),
				VoiceChannelID:  strings.TrimSpace(item.VoiceChannelID),
			})
		}
	}
	if len(m.streamDefaults) > 0 {
		// A start can outlast the waiting projection. Retain only its last
		// nonempty authority snapshot, not an additional auto-start candidate.
		m.lastStreamDefaults = m.streamDefaults
	}
	for streamID := range m.autoStartPending {
		if _, ok := m.streamDefaults[streamID]; !ok {
			delete(m.autoStartPending, streamID)
		}
	}
	for streamID := range m.autoStartRetries {
		item, ok := m.streamDefaults[streamID]
		if !ok || !item.AutoStartEnabled || item.AssignmentEpoch != m.autoStartRetries[streamID].assignmentEpoch {
			m.cancelAutoStartRetryLocked(streamID)
		}
	}
	for streamID, stopped := range m.autoStartStopped {
		// Absence means the waiting projection has not rearmed yet. It is not
		// new authority and must not restart an explicitly stopped VC by presence.
		if next, ok := m.streamDefaults[streamID]; ok && (next.AssignmentEpoch != stopped.AssignmentEpoch || next.GuildID != stopped.GuildID || next.VoiceChannelID != stopped.VoiceChannelID) {
			delete(m.autoStartStopped, streamID)
			delete(m.autoStartPending, streamID)
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
