package main

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/example/autostream-discord-bot/internal/control"
	"github.com/example/autostream-discord-bot/internal/jobs"
)

type discordWaitingGatewaySelection struct {
	binding  control.DiscordWaitingCredentialBinding
	defaults map[string]jobs.VoiceDefaults
	selected bool
}

func discordWaitingBindingEpoch(binding control.DiscordWaitingMonitorBinding) string {
	return fmt.Sprintf("%s/%d/%s", binding.SelectionEpoch.UTC().Format(time.RFC3339Nano), binding.TargetRevision, binding.ProfileRevision)
}

func selectDiscordWaitingGateway(cfg control.RuntimeConfig, active currentGatewayAuthority) (discordWaitingGatewaySelection, error) {
	result := discordWaitingGatewaySelection{defaults: map[string]jobs.VoiceDefaults{}}
	monitor := cfg.DiscordWaitingMonitor
	if monitor == nil || monitor.Version != 1 || monitor.Bindings == nil {
		return result, errors.New("discord_waiting_monitor_version_unsupported")
	}
	serviceID := strings.TrimSpace(cfg.Service.ServiceID)
	activeCredential := ""
	if monitor.ExecutionActive && len(active.current) == 0 {
		return result, errors.New("discord_waiting_execution_authority_unavailable")
	}
	if monitor.ExecutionActive && len(active.current) != 0 {
		activeCredential = active.binding.SecretName + "\x00" + active.binding.Fingerprint
	}
	if monitor.AvailabilityCode != "" && activeCredential == "" {
		return result, errors.New(monitor.AvailabilityCode)
	}
	canonical := func(s string) bool { return s != "" && s == strings.TrimSpace(s) }
	seen := map[string]bool{}
	credentials := map[string]bool{}
	targetCounts := map[string]int{}
	var candidates []control.DiscordWaitingMonitorBinding
	for _, binding := range monitor.Bindings {
		if !binding.Ready {
			continue
		}
		if !canonical(binding.StreamID) || seen[binding.StreamID] || binding.ServiceID != serviceID ||
			binding.ServiceType != control.ServiceType || binding.ProfileOwnerServiceID != serviceID ||
			!canonical(binding.ProfileID) || !canonical(binding.ProfileRevision) || binding.SelectionEpoch.IsZero() ||
			!canonical(binding.SecretName) || !canonical(binding.Fingerprint) || binding.ReadinessCode != "" ||
			!canonical(binding.GuildID) || !canonical(binding.VoiceChannelID) || binding.TargetRevision == 0 ||
			binding.AutoStartTrigger != "discord_voice_join" {
			return result, errors.New("discord_waiting_monitor_binding_invalid")
		}
		seen[binding.StreamID] = true
		if binding.SecretName != "discord_bot_token" && !strings.HasPrefix(binding.SecretName, "discord_bot_token_") {
			return result, errors.New("discord_waiting_monitor_binding_invalid")
		}
		credential := binding.SecretName + "\x00" + binding.Fingerprint
		if activeCredential != "" && credential != activeCredential {
			// Current execution authority keeps the running Gateway. A waiting
			// frame with another credential does not invalidate that authority.
			continue
		}
		credentials[credential] = true
		key := binding.GuildID + "\x00" + binding.VoiceChannelID
		targetCounts[key]++
		candidates = append(candidates, binding)
	}
	if len(credentials) > 1 {
		return result, errors.New("discord_waiting_credentials_ambiguous")
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].StreamID < candidates[j].StreamID })
	for _, binding := range candidates {
		if targetCounts[binding.GuildID+"\x00"+binding.VoiceChannelID] != 1 {
			continue
		}
		result.defaults[binding.StreamID] = jobs.VoiceDefaults{
			AssignmentEpoch: discordWaitingBindingEpoch(binding),
			GuildID:         binding.GuildID, VoiceChannelID: binding.VoiceChannelID,
			TextChannelID: binding.TextChannelID, AutoStartEnabled: true,
		}
		if !result.selected {
			result.binding, result.selected = binding.DiscordWaitingCredentialBinding, true
		}
	}
	if len(candidates) != 0 && !result.selected {
		return result, errors.New("discord_waiting_target_ambiguous")
	}
	return result, nil
}

func reconcileWaitingDiscordGateway(ctx context.Context, client control.Client, manager *jobs.Manager, cfg control.RuntimeConfig, active currentGatewayAuthority) error {
	selection, err := selectDiscordWaitingGateway(cfg, active)
	if err != nil {
		manager.SetStreamVoiceDefaults(nil)
		_ = manager.ReconcileGatewayCredential("", false, nil)
		return err
	}
	if !selection.selected {
		return manager.ReconcileGatewayCredential("", false, nil)
	}
	binding := selection.binding
	return manager.ReconcileGatewayCredential(cfg.Service.ServiceID+"/"+binding.SecretName+"/"+binding.Fingerprint, true, func() (string, error) {
		secret, err := client.ResolveRuntimeSecretForWaitingBinding(ctx, binding)
		return secret.Value, err
	})
}
