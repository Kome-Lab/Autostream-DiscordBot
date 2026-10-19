package control

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

var ErrInvalidCurrentDiscordBinding = errors.New("invalid current Discord credential binding")

// CurrentDiscordBindings is local Panel/Bot authority for an existing primary
// assignment. Waiting auto-start candidates have a separate lifecycle.
type CurrentDiscordBindings struct {
	Version  int                               `json:"version"`
	Bindings []CurrentDiscordCredentialBinding `json:"bindings"`
}

type CurrentDiscordCredentialBinding struct {
	StreamID              string    `json:"stream_id"`
	ServiceID             string    `json:"service_id"`
	ServiceType           string    `json:"service_type"`
	AssignmentRole        string    `json:"assignment_role"`
	AssignmentEpoch       time.Time `json:"assignment_epoch"`
	ProfileID             string    `json:"profile_id"`
	ProfileOwnerServiceID string    `json:"profile_owner_service_id,omitempty"`
	SecretName            string    `json:"secret_name,omitempty"`
	Fingerprint           string    `json:"fingerprint,omitempty"`
	Ready                 bool      `json:"ready"`
	ReadinessCode         string    `json:"readiness_code,omitempty"`
}

// Keep omitted (an old Panel) distinct from an explicitly null authority. A
// versioned empty list is the only supported declaration of no current binding.
func (cfg *RuntimeConfig) UnmarshalJSON(raw []byte) error {
	type plain RuntimeConfig
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return err
	}
	present := false
	for key := range fields {
		if strings.EqualFold(key, "current_discord_bindings") {
			present = true
		}
	}
	if err := validateCurrentBindingJSONKeys(raw); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidCurrentDiscordBinding, err)
	}
	var value plain
	if err := json.Unmarshal(raw, &value); err != nil {
		if present {
			return fmt.Errorf("%w: malformed authority fields", ErrInvalidCurrentDiscordBinding)
		}
		return err
	}
	value.CurrentDiscordBindingsPresent = present
	*cfg = RuntimeConfig(value)
	return nil
}

// Apply duplicate-key rejection only to this additive authority field. Legacy
// fields keep their existing decoding behavior; no last-key-wins grant is made
// for a duplicated envelope or a false/true readiness pair.
func validateCurrentBindingJSONKeys(raw []byte) error {
	d := json.NewDecoder(bytes.NewReader(raw))
	token, err := d.Token()
	if err != nil {
		return err
	}
	if token != json.Delim('{') {
		return nil
	}
	found := false
	for d.More() {
		key, err := d.Token()
		if err != nil {
			return err
		}
		var value json.RawMessage
		if err = d.Decode(&value); err != nil {
			return err
		}
		if !strings.EqualFold(key.(string), "current_discord_bindings") {
			continue
		}
		if found {
			return errors.New("duplicate current authority envelope")
		}
		found = true
		if err = rejectCurrentBindingDuplicateKeys(json.NewDecoder(bytes.NewReader(value)), 0); err != nil {
			return err
		}
	}
	_, err = d.Token()
	return err
}

func rejectCurrentBindingDuplicateKeys(d *json.Decoder, depth int) error {
	if depth > 16 {
		return errors.New("current authority nesting exceeds wire contract")
	}
	token, err := d.Token()
	if err != nil {
		return err
	}
	switch token {
	case json.Delim('{'):
		seen := map[string]bool{}
		for d.More() {
			key, err := d.Token()
			if err != nil {
				return err
			}
			name := strings.ToLower(key.(string))
			if seen[name] {
				return errors.New("duplicate current authority key")
			}
			seen[name] = true
			if err = rejectCurrentBindingDuplicateKeys(d, depth+1); err != nil {
				return err
			}
		}
		_, err = d.Token()
		return err
	case json.Delim('['):
		for d.More() {
			if err = rejectCurrentBindingDuplicateKeys(d, depth+1); err != nil {
				return err
			}
		}
		_, err = d.Token()
		return err
	default:
		return nil
	}
}

// The expected tuple is revalidated by the Panel in the same store boundary as
// the existing service-level lease and secret read. Do not send stream_id as a
// lease scope or fall back to the legacy resolver when this request is rejected.
func (c Client) ResolveRuntimeSecretForCurrentBinding(ctx context.Context, binding CurrentDiscordCredentialBinding) (RuntimeSecret, error) {
	var out RuntimeSecret
	err := c.postDecode(ctx, "/services/runtime-secrets/resolve", map[string]any{
		"service_id":                      c.Config.ServiceID,
		"secret_name":                     binding.SecretName,
		"current_discord_binding_version": 1,
		"current_discord_binding":         binding,
	}, &out)
	if err != nil {
		return RuntimeSecret{}, err
	}
	return out, nil
}
