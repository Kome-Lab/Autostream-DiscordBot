package control

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestUI188CB15BindingPresenceAndResolverWire(t *testing.T) {
	for _, tc := range []struct {
		raw         string
		present     bool
		nilEnvelope bool
		nilList     bool
	}{
		{`{}`, false, true, true},
		{`{"current_discord_bindings":null}`, true, true, true},
		{`{"current_discord_bindings":{"version":1,"bindings":[]}}`, true, false, false},
		{`{"current_discord_bindings":{"version":1,"bindings":null}}`, true, false, true},
	} {
		var cfg RuntimeConfig
		if err := json.Unmarshal([]byte(tc.raw), &cfg); err != nil {
			t.Fatal(err)
		}
		if cfg.CurrentDiscordBindingsPresent != tc.present || (cfg.CurrentDiscordBindings == nil) != tc.nilEnvelope {
			t.Fatal("presence state conflated")
		}
		if cfg.CurrentDiscordBindings != nil && (cfg.CurrentDiscordBindings.Bindings == nil) != tc.nilList {
			t.Fatal("empty and null list conflated")
		}
	}
	for _, raw := range []string{`{"current_discord_bindings":[]}`, `{"current_discord_bindings":{"version":"1"}}`, `{"current_discord_bindings":{"version":1,"bindings":[{"assignment_epoch":"bad"}]}}`} {
		var cfg RuntimeConfig
		if json.Unmarshal([]byte(raw), &cfg) == nil {
			t.Fatal("invalid binding DTO accepted")
		}
	}
	for _, raw := range []string{
		`{"current_discord_bindings":null,"current_discord_bindings":{"version":1,"bindings":[]}}`,
		`{"current_discord_bindings":null,"CURRENT_DISCORD_BINDINGS":{"version":1,"bindings":[]}}`,
		`{"current_discord_bindings":{"version":1,"bindings":[{"ready":false,"ready":true}]}}`,
		`{"current_discord_bindings":{"version":1,"bindings":[{"ready":false,"Ready":true}]}}`,
		`{"current_discord_bindings":{"version":1,"bindings":[{"assignment_epoch":"UNIT_ONLY_SECRET_NEVER_LOG"}]}}`,
	} {
		var cfg RuntimeConfig
		err := json.Unmarshal([]byte(raw), &cfg)
		if !errors.Is(err, ErrInvalidCurrentDiscordBinding) || strings.Contains(err.Error(), "UNIT_ONLY_SECRET_NEVER_LOG") {
			t.Fatal("ambiguous/malformed authority accepted or leaked")
		}
	}
	binding := CurrentDiscordCredentialBinding{StreamID: "s", ServiceID: "own", ServiceType: ServiceType, AssignmentRole: "primary", AssignmentEpoch: time.Unix(100, 0).UTC(), ProfileID: "p", ProfileOwnerServiceID: "own", SecretName: "discord_bot_token_test", Fingerprint: "fp", Ready: true}
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Method != http.MethodPost || r.URL.Path != "/services/runtime-secrets/resolve" || r.Header.Get("Authorization") != "Bearer test-only" {
			t.Error("wrong authenticated resolver")
		}
		var body map[string]json.RawMessage
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
			return
		}
		if len(body) != 4 || len(body["stream_id"]) != 0 || string(body["current_discord_binding_version"]) != "1" {
			t.Error("lease scope changed or current authority missing")
		}
		var got CurrentDiscordCredentialBinding
		if err := json.Unmarshal(body["current_discord_binding"], &got); err != nil || got != binding {
			t.Error("expected binding changed")
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"value": "synthetic-current", "expires_in_sec": 60})
	}))
	defer server.Close()
	client := Client{Config: Config{ControlPanelURL: server.URL, Token: "test-only", ServiceID: "own", ServiceName: "own", ServicePublicURL: "https://bot.example.com"}}
	value, err := client.ResolveRuntimeSecretForCurrentBinding(t.Context(), binding)
	if err != nil || value.Value != "synthetic-current" || calls != 1 {
		t.Fatal("normal current resolver failed")
	}
}
