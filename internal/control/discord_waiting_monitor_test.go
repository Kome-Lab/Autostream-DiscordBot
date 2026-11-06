package control

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestDiscordWaitingMonitorPresenceAndResolverScope(t *testing.T) {
	for _, tc := range []struct {
		raw     string
		present bool
		nilBody bool
	}{
		{`{}`, false, true},
		{`{"discord_waiting_monitor":null}`, true, true},
		{`{"discord_waiting_monitor":{"version":1,"execution_active":false,"bindings":[]}}`, true, false},
	} {
		var cfg RuntimeConfig
		if err := json.Unmarshal([]byte(tc.raw), &cfg); err != nil {
			t.Fatal(err)
		}
		if cfg.DiscordWaitingMonitorPresent != tc.present || (cfg.DiscordWaitingMonitor == nil) != tc.nilBody {
			t.Fatal("waiting authority absence and explicit null were conflated")
		}
	}
	for _, raw := range []string{
		`{"discord_waiting_monitor":null,"discord_waiting_monitor":{"version":1,"execution_active":false,"bindings":[]}}`,
		`{"discord_waiting_monitor":null,"DISCORD_WAITING_MONITOR":{"version":1,"bindings":[]}}`,
		`{"discord_waiting_monitor":{"version":1,"bindings":[{"ready":false,"Ready":true}]}}`,
	} {
		var cfg RuntimeConfig
		if err := json.Unmarshal([]byte(raw), &cfg); !errors.Is(err, ErrInvalidDiscordWaitingMonitor) {
			t.Fatal("ambiguous waiting authority accepted", err)
		}
	}
	binding := DiscordWaitingCredentialBinding{
		StreamID: "waiting", ServiceID: "own", ServiceType: ServiceType, SelectionEpoch: time.Unix(100, 0).UTC(),
		ProfileID: "profile", ProfileRevision: "revision", ProfileOwnerServiceID: "own",
		SecretName: "discord_bot_token_waiting", Fingerprint: "fingerprint", Ready: true,
	}
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Method != http.MethodPost || r.URL.Path != "/services/runtime-secrets/resolve" || r.Header.Get("Authorization") != "Bearer test-only" {
			t.Error("unexpected unauthenticated waiting resolver")
		}
		var body map[string]json.RawMessage
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
			return
		}
		if len(body) != 4 || len(body["stream_id"]) != 0 || len(body["archive_profile_id"]) != 0 ||
			len(body["current_discord_binding"]) != 0 || string(body["discord_waiting_binding_version"]) != "1" {
			t.Error("waiting binding widened execution/current/lease authority")
		}
		var got DiscordWaitingCredentialBinding
		if err := json.Unmarshal(body["discord_waiting_binding"], &got); err != nil || got != binding {
			t.Error("waiting expected tuple changed")
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"value": "synthetic-monitor-only", "expires_in_sec": 60})
	}))
	defer server.Close()
	client := Client{Config: Config{ControlPanelURL: server.URL, Token: "test-only", ServiceID: "own", ServiceName: "own", ServicePublicURL: "https://bot.example.com"}}
	secret, err := client.ResolveRuntimeSecretForWaitingBinding(t.Context(), binding)
	if err != nil || secret.Value != "synthetic-monitor-only" || calls.Load() != 1 {
		t.Fatal("fresh monitoring resolver failed", err)
	}
}

func TestNodeExecutionBusyIsRetryableWithoutRetryingOtherConflicts(t *testing.T) {
	if !(ControlPanelError{StatusCode: 409, Code: "node_execution_busy"}).RetryableAutoStart() {
		t.Fatal("temporary execution capacity was treated as a permanent candidate failure")
	}
	for _, code := range []string{"node_selection_changed", "service_not_primary_assignment", "service_assignment_conflict", "stream_not_waiting"} {
		if (ControlPanelError{StatusCode: 409, Code: code}).RetryableAutoStart() {
			t.Fatal("non-capacity conflict became retryable", code)
		}
	}
}
