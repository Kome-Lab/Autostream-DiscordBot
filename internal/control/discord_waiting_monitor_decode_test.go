package control

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
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

func TestDiscordWaitingMonitorCannotMaskMalformedCurrentDecode(t *testing.T) {
	const waiting = `{"version":1,"bindings":[]}`
	for _, current := range []string{
		`[]`,
		`{"version":"invalid","bindings":[]}`,
		`{"version":1,"bindings":[{"assignment_epoch":"invalid"}]}`,
		`{"version":1,"bindings":[{"ready":false,"Ready":true}]}`,
	} {
		for _, raw := range []string{
			`{"discord_waiting_monitor":` + waiting + `,"current_discord_bindings":` + current + `}`,
			`{"current_discord_bindings":` + current + `,"discord_waiting_monitor":` + waiting + `}`,
		} {
			var cfg RuntimeConfig
			err := json.Unmarshal([]byte(raw), &cfg)
			if !errors.Is(err, ErrInvalidCurrentDiscordBinding) || errors.Is(err, ErrInvalidDiscordWaitingMonitor) {
				t.Fatalf("waiting decode masked current rejection through field order: %v", err)
			}
		}
	}
}

func TestRuntimeConfigRetainsExecutionSnapshotOnlyForWaitingDecodeFailure(t *testing.T) {
	for _, tc := range []struct {
		name    string
		current string
		partial bool
	}{
		{"waiting only", `{"version":1,"bindings":[]}`, true},
		{"current and waiting", `{"version":1,"bindings":[{"assignment_epoch":"invalid"}]}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := `{"service":{"service_id":"own","service_type":"discord_bot"},"assignments":[],"profiles":{"discord_config":[]},"current_discord_bindings":` +
				tc.current + `,"discord_waiting_monitor":{"version":1,"bindings":[]}}`
			panel := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = w.Write([]byte(body))
			}))
			defer panel.Close()
			client := Client{Config: Config{ControlPanelURL: panel.URL, Token: "test-only", ServiceID: "own", ServiceName: "own", ServicePublicURL: "https://bot.example.com"}}
			cfg, err := client.RuntimeConfig(t.Context())
			if tc.partial {
				if !errors.Is(err, ErrInvalidDiscordWaitingMonitor) || errors.Is(err, ErrInvalidCurrentDiscordBinding) ||
					cfg.Service.ServiceID != "own" || cfg.CurrentDiscordBindings == nil || cfg.Assignments == nil ||
					!cfg.CurrentDiscordBindingsPresent || !cfg.DiscordWaitingMonitorPresent || cfg.DiscordWaitingMonitor != nil {
					t.Fatalf("waiting-only error lost or trusted its partial snapshot: cfg=%+v err=%v", cfg, err)
				}
			} else if !errors.Is(err, ErrInvalidCurrentDiscordBinding) || cfg.Service.ServiceID != "" || cfg.CurrentDiscordBindings != nil || cfg.DiscordWaitingMonitorPresent {
				t.Fatalf("other error leaked a usable partial runtime snapshot: cfg=%+v err=%v", cfg, err)
			}
		})
	}
}
