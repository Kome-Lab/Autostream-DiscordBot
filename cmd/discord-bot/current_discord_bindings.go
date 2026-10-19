package main

import (
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/example/autostream-discord-bot/internal/control"
)

type currentGatewayAuthority struct {
	current map[string]string
	mapped  map[string]string
	profile control.RuntimeProfile
	binding control.CurrentDiscordCredentialBinding
}

func currentDiscordGatewayAuthority(cfg control.RuntimeConfig, authenticatedServiceID string) (currentGatewayAuthority, error) {
	result := currentGatewayAuthority{current: map[string]string{}, mapped: map[string]string{}}
	invalid := func() (currentGatewayAuthority, error) {
		return result, errors.New("current Discord credential binding unavailable")
	}
	canonical := func(s string) bool { return s != "" && s == strings.TrimSpace(s) }
	serviceID := cfg.Service.ServiceID
	if !canonical(serviceID) || serviceID != authenticatedServiceID || cfg.Service.ServiceType != control.ServiceType {
		return invalid()
	}
	envelope := cfg.CurrentDiscordBindings
	if envelope == nil && !cfg.CurrentDiscordBindingsPresent {
		return result, errors.New("Panel current Discord credential binding unsupported")
	}
	if envelope == nil {
		return result, errors.New("Panel current Discord credential binding is null")
	}
	if envelope.Version != 1 {
		return result, errors.New("Panel current Discord credential binding version unsupported")
	}
	if envelope.Bindings == nil {
		return invalid()
	}
	assignments := map[string]control.StreamServiceAssignment{}
	allAssigned := map[string]bool{}
	for _, a := range cfg.Assignments {
		if !canonical(a.StreamID) || a.ServiceID != serviceID || a.ServiceType != control.ServiceType ||
			(a.AssignmentRole != "primary" && a.AssignmentRole != "standby") || a.AssignedAt.IsZero() || allAssigned[a.StreamID] {
			return invalid()
		}
		allAssigned[a.StreamID] = true
		if a.AssignmentRole == "primary" {
			assignments[a.StreamID] = a
			result.current[a.StreamID] = a.AssignedAt.UTC().Format(time.RFC3339Nano)
		}
	}
	if (len(allAssigned) == 0 && cfg.Service.CurrentStreamID != "") ||
		(len(allAssigned) != 0 && !allAssigned[cfg.Service.CurrentStreamID]) || len(assignments) != len(envelope.Bindings) {
		return invalid()
	}
	seen := map[string]bool{}
	selected := ""
	for _, b := range envelope.Bindings {
		a, ok := assignments[b.StreamID]
		if !ok || seen[b.StreamID] || b.ServiceID != serviceID || b.ServiceType != control.ServiceType || b.AssignmentRole != "primary" ||
			!b.AssignmentEpoch.Equal(a.AssignedAt) || b.AssignmentEpoch.IsZero() || !canonical(b.ProfileID) ||
			b.ProfileOwnerServiceID != serviceID || !canonical(b.SecretName) || !canonical(b.Fingerprint) || !b.Ready || b.ReadinessCode != "" ||
			(selected != "" && selected != b.ProfileID) {
			return invalid()
		}
		seen[b.StreamID] = true
		selected = b.ProfileID
		result.mapped[b.StreamID] = b.ProfileID
	}
	if len(assignments) == 0 {
		return result, nil
	}
	matches := 0
	for _, p := range cfg.Profiles["discord_config"] {
		if p.ID != selected {
			continue
		}
		matches++
		result.profile = p
	}
	if matches != 1 || result.profile.Kind != "discord_config" || stringConfig(result.profile.Config, "service_id") != serviceID {
		return invalid()
	}
	ids := make([]string, 0, len(assignments))
	for id := range assignments {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, b := range envelope.Bindings {
		if stringConfig(result.profile.Config, "bot_token_secret_name") != b.SecretName ||
			stringConfig(result.profile.Config, "bot_token_fingerprint") != b.Fingerprint {
			return invalid()
		}
		if b.StreamID == ids[0] {
			result.binding = b
		}
	}
	return result, nil
}
