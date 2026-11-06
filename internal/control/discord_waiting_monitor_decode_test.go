package control

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestDiscordWaitingMonitorDecodeErrorsDoNotInvalidateCurrentBinding(t *testing.T) {
	const current = `{"version":1,"bindings":[{"stream_id":"active-A","service_id":"own","service_type":"discord_bot","assignment_role":"primary","assignment_epoch":"2026-11-06T00:00:00Z","profile_id":"active-profile","secret_name":"discord_bot_token_active","fingerprint":"active-fingerprint","ready":true}]}`
	for _, waiting := range []string{
		`{"version":1,"bindings":[]}`,
		`{"version":1,"execution_active":null,"bindings":[]}`,
		`{"version":1,"execution_active":"false","bindings":[]}`,
		`{"version":1,"execution_active":{},"bindings":[]}`,
		`{"version":"invalid","execution_active":true,"bindings":[]}`,
		`{"version":1,"execution_active":true,"bindings":{}}`,
		`{"version":1,"execution_active":true,"bindings":[{"selection_epoch":"UNIT_ONLY_WAITING_NEVER_LOG"}]}`,
		`[]`,
		`"invalid"`,
	} {
		var direct DiscordWaitingMonitor
		if err := json.Unmarshal([]byte(waiting), &direct); !errors.Is(err, ErrInvalidDiscordWaitingMonitor) {
			t.Fatalf("monitor-only parse error lost its classification: %v", err)
		}
		var cfg RuntimeConfig
		err := json.Unmarshal([]byte(`{"current_discord_bindings":`+current+`,"discord_waiting_monitor":`+waiting+`}`), &cfg)
		if !errors.Is(err, ErrInvalidDiscordWaitingMonitor) || errors.Is(err, ErrInvalidCurrentDiscordBinding) {
			t.Fatalf("waiting-only parse error invalidated current authority: %v", err)
		}
		if strings.Contains(err.Error(), "UNIT_ONLY_WAITING_NEVER_LOG") {
			t.Fatal("waiting parse error disclosed a raw authority value")
		}
	}
}

func TestDiscordWaitingMonitorDecodePreservesMalformedCurrentRejection(t *testing.T) {
	const waiting = `{"version":1,"execution_active":true,"bindings":[]}`
	for _, current := range []string{
		`[]`,
		`{"version":"invalid","bindings":[]}`,
		`{"version":1,"bindings":[{"assignment_epoch":"invalid"}]}`,
		`{"version":1,"bindings":[{"ready":false,"Ready":true}]}`,
	} {
		var cfg RuntimeConfig
		err := json.Unmarshal([]byte(`{"current_discord_bindings":`+current+`,"discord_waiting_monitor":`+waiting+`}`), &cfg)
		if !errors.Is(err, ErrInvalidCurrentDiscordBinding) || errors.Is(err, ErrInvalidDiscordWaitingMonitor) {
			t.Fatalf("malformed current binding lost its revocation classification: %v", err)
		}
	}
}
