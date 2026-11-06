package control

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"
)

const CapabilityDiscordWaitingMonitorV1 = "discord_waiting_monitor_v1"

var ErrInvalidDiscordWaitingMonitor = errors.New("invalid waiting Discord monitoring authority")

type DiscordWaitingMonitor struct {
	Version            int                            `json:"version"`
	ExecutionActive    bool                           `json:"execution_active"`
	RequiredCapability string                         `json:"required_capability,omitempty"`
	AvailabilityCode   string                         `json:"availability_code,omitempty"`
	Bindings           []DiscordWaitingMonitorBinding `json:"bindings"`
}

type DiscordWaitingCredentialBinding struct {
	StreamID              string    `json:"stream_id"`
	ServiceID             string    `json:"service_id"`
	ServiceType           string    `json:"service_type"`
	SelectionEpoch        time.Time `json:"selection_epoch"`
	ProfileID             string    `json:"profile_id"`
	ProfileRevision       string    `json:"profile_revision"`
	ProfileOwnerServiceID string    `json:"profile_owner_service_id,omitempty"`
	SecretName            string    `json:"secret_name,omitempty"`
	Fingerprint           string    `json:"fingerprint,omitempty"`
	Ready                 bool      `json:"ready"`
	ReadinessCode         string    `json:"readiness_code,omitempty"`
}

type DiscordWaitingMonitorBinding struct {
	DiscordWaitingCredentialBinding
	GuildID          string `json:"guild_id"`
	VoiceChannelID   string `json:"voice_channel_id"`
	TextChannelID    string `json:"text_channel_id,omitempty"`
	TargetRevision   uint64 `json:"target_revision"`
	AutoStartTrigger string `json:"auto_start_trigger"`
}

// Monitoring uses the existing service-level secret lease. No execution,
// stream, archive, or stop scope is added by a saved waiting selection.
func (c Client) ResolveRuntimeSecretForWaitingBinding(ctx context.Context, binding DiscordWaitingCredentialBinding) (RuntimeSecret, error) {
	var out RuntimeSecret
	err := c.postDecode(ctx, "/services/runtime-secrets/resolve", map[string]any{
		"service_id":                      c.Config.ServiceID,
		"secret_name":                     binding.SecretName,
		"discord_waiting_binding_version": 1,
		"discord_waiting_binding":         binding,
	}, &out)
	return out, err
}

func validateDiscordWaitingMonitorJSONKeys(raw []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	if token != json.Delim('{') {
		return errors.New("runtime config must be an object")
	}
	found := false
	for decoder.More() {
		key, err := decoder.Token()
		if err != nil {
			return err
		}
		if !strings.EqualFold(key.(string), "discord_waiting_monitor") {
			var ignored json.RawMessage
			if err := decoder.Decode(&ignored); err != nil {
				return err
			}
			continue
		}
		if found {
			return errors.New("duplicate waiting monitoring envelope")
		}
		found = true
		if err := rejectCurrentBindingDuplicateKeys(decoder, 0); err != nil {
			return err
		}
	}
	_, err = decoder.Token()
	return err
}

func (monitor *DiscordWaitingMonitor) UnmarshalJSON(raw []byte) error {
	type plain DiscordWaitingMonitor
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return ErrInvalidDiscordWaitingMonitor
	}
	active, exists := fields["execution_active"]
	if !exists || bytes.Equal(bytes.TrimSpace(active), []byte("null")) {
		return ErrInvalidDiscordWaitingMonitor
	}
	var executionActive bool
	if err := json.Unmarshal(active, &executionActive); err != nil {
		return ErrInvalidDiscordWaitingMonitor
	}
	var value plain
	if err := json.Unmarshal(raw, &value); err != nil {
		return ErrInvalidDiscordWaitingMonitor
	}
	*monitor = DiscordWaitingMonitor(value)
	return nil
}
