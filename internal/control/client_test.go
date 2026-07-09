package control

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestRegisterPostsServiceRegistration(t *testing.T) {
	var gotAuth string
	var got Registration
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/services/register" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		gotAuth = r.Header.Get("Authorization")
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Fatal(err)
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	client := Client{Config: Config{ControlPanelURL: server.URL, Token: "secret-token", ServiceID: "bot-01", ServiceName: "Discord 01", ServicePublicURL: "https://bot.example.com", Version: "0.1.0"}}
	if err := client.Register(t.Context()); err != nil {
		t.Fatal(err)
	}
	if gotAuth != "Bearer secret-token" {
		t.Fatalf("unexpected auth header: %s", gotAuth)
	}
	if got.ServiceType != ServiceType || got.ServiceID != "bot-01" || got.Capabilities["voice_connect"] != true {
		t.Fatalf("unexpected registration: %#v", got)
	}
	if got.OS != runtime.GOOS || got.Arch != runtime.GOARCH {
		t.Fatalf("registration did not include runtime platform: %#v", got)
	}
	if got.Capabilities["active_speaker_state"] != true || got.Capabilities["audio_capture"] != true || got.Capabilities["audio_stream_forward"] != true {
		t.Fatalf("capabilities must advertise runtime-config audio support explicitly: %#v", got.Capabilities)
	}
	if got.Capabilities["audio_capture_runtime_secret"] != true || got.Capabilities["audio_stream_forward_runtime_token"] != true {
		t.Fatalf("capabilities must advertise Control Panel managed audio auth support: %#v", got.Capabilities)
	}
	for name, value := range got.Capabilities {
		if value == "placeholder" || value == "todo" {
			t.Fatalf("capability %s has non-contract value %q", name, value)
		}
	}
}

func TestHeartbeatPostsStatus(t *testing.T) {
	var got Heartbeat
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/services/heartbeat" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Fatal(err)
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	client := Client{Config: Config{ControlPanelURL: server.URL, Token: "secret-token", ServiceID: "bot-01", ServiceName: "Discord 01", ServicePublicURL: "https://discord.example.com"}}
	if err := client.HeartbeatWithMetrics(t.Context(), "", "stream-01", map[string]float64{"discord.audio_receiving": 1}); err != nil {
		t.Fatal(err)
	}
	if got.Status != "online" || got.CurrentStreamID != "stream-01" || got.Metrics["discord.audio_receiving"] != 1 {
		t.Fatalf("unexpected heartbeat: %#v", got)
	}
	if got.Metrics["node.cpu_count"] <= 0 || got.Metrics["process.heap_alloc_bytes"] <= 0 || got.Metrics["process.uptime_seconds"] < 0 {
		t.Fatalf("heartbeat did not include host/process metrics: %#v", got.Metrics)
	}
	if got.OS != runtime.GOOS || got.Arch != runtime.GOARCH || got.Capabilities["job_endpoint"] != true {
		t.Fatalf("heartbeat did not include platform/capabilities: %#v", got)
	}
}

func TestRuntimeConfigFetchesScopedServiceConfig(t *testing.T) {
	var gotAuth string
	var gotQuery string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/services/runtime-config" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		gotAuth = r.Header.Get("Authorization")
		gotQuery = r.URL.Query().Get("service_id")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"service":{"service_id":"bot-01","service_type":"discord_bot","service_name":"Discord 01","status":"assigned"},
			"assignments":[{"stream_id":"stream-01","service_id":"bot-01","service_type":"discord_bot","assignment_role":"primary","assigned_at":"2026-06-10T00:00:00Z"}],
			"profiles":{"discord_config":[{"id":"profile-01","kind":"discord_config","name":"Main VC","config":{"service_id":"bot-01","guild_id":"guild-01","voice_channel_id":"voice-01","bot_token_secret_name":"discord-bot-main"},"created_at":"2026-06-10T00:00:00Z","updated_at":"2026-06-10T00:00:00Z"}]},
			"stream_discord_configs":[{"stream_id":"stream-01","assignment_role":"primary","discord_config_id":"profile-01","guild_id":"guild-stream","voice_channel_id":"voice-stream","text_channel_id":"text-stream","caption_audio_url":"https://caption.example.com/audio","auto_start_trigger":"discord_voice_join"}]
		}`))
	}))
	defer server.Close()

	client := Client{Config: Config{ControlPanelURL: server.URL, Token: "secret-token", ServiceID: "bot-01", ServiceName: "Discord 01", ServicePublicURL: "https://discord.example.com"}}
	cfg, err := client.RuntimeConfig(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if gotAuth != "Bearer secret-token" || gotQuery != "bot-01" {
		t.Fatalf("unexpected request auth=%q query=%q", gotAuth, gotQuery)
	}
	if cfg.Service.ServiceID != "bot-01" || len(cfg.Assignments) != 1 {
		t.Fatalf("unexpected runtime config: %#v", cfg)
	}
	profiles := cfg.Profiles["discord_config"]
	if len(profiles) != 1 || profiles[0].Config["guild_id"] != "guild-01" || profiles[0].Config["bot_token_secret_name"] != "discord-bot-main" {
		t.Fatalf("unexpected runtime profiles: %#v", profiles)
	}
	if len(cfg.StreamDiscordConfigs) != 1 || cfg.StreamDiscordConfigs[0].StreamID != "stream-01" || cfg.StreamDiscordConfigs[0].GuildID != "guild-stream" || cfg.StreamDiscordConfigs[0].VoiceChannelID != "voice-stream" || cfg.StreamDiscordConfigs[0].AutoStartTrigger != "discord_voice_join" {
		t.Fatalf("unexpected stream discord configs: %#v", cfg.StreamDiscordConfigs)
	}
	streamConfig, ok := cfg.DiscordConfigForStream("stream-01")
	if !ok {
		t.Fatalf("expected stream discord config lookup to succeed: %#v", cfg.StreamDiscordConfigs)
	}
	if streamConfig.GuildID != "guild-stream" || streamConfig.VoiceChannelID != "voice-stream" || streamConfig.TextChannelID != "text-stream" || streamConfig.AutoStartTrigger != "discord_voice_join" {
		t.Fatalf("unexpected stream discord config lookup result: %#v", streamConfig)
	}
}

func TestDiscordConfigForStreamUsesPrimaryOnly(t *testing.T) {
	cfg := RuntimeConfig{StreamDiscordConfigs: []StreamDiscordConfig{
		{StreamID: "stream-01", AssignmentRole: "standby", GuildID: "guild-standby", VoiceChannelID: "voice-standby"},
		{StreamID: "stream-01", AssignmentRole: "primary", GuildID: "guild-primary", VoiceChannelID: "voice-primary", TextChannelID: "text-primary"},
	}}
	item, ok := cfg.DiscordConfigForStream("stream-01")
	if !ok {
		t.Fatal("expected primary stream discord config")
	}
	if item.GuildID != "guild-primary" || item.VoiceChannelID != "voice-primary" || item.TextChannelID != "text-primary" {
		t.Fatalf("unexpected primary stream discord config: %#v", item)
	}
	standbyOnly := RuntimeConfig{StreamDiscordConfigs: []StreamDiscordConfig{
		{StreamID: "stream-02", AssignmentRole: "standby", GuildID: "guild-standby", VoiceChannelID: "voice-standby"},
	}}
	if _, ok := standbyOnly.DiscordConfigForStream("stream-02"); ok {
		t.Fatal("standby-only stream config must not be used for job start defaults")
	}
}

func TestResolveRuntimeSecretPostsScopedServiceRequest(t *testing.T) {
	var gotAuth string
	var gotBody map[string]string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/services/runtime-secrets/resolve" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		gotAuth = r.Header.Get("Authorization")
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Fatal(err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"secret_name":"discord_bot_token_profile-01","value":"Bot <RAW_DISCORD_TOKEN>","expires_in_sec":60}`))
	}))
	defer server.Close()

	client := Client{Config: Config{ControlPanelURL: server.URL, Token: "secret-token", ServiceID: "bot-01", ServiceName: "Discord 01", ServicePublicURL: "https://discord.example.com"}}
	secret, err := client.ResolveRuntimeSecret(t.Context(), "discord_bot_token_profile-01")
	if err != nil {
		t.Fatal(err)
	}
	if gotAuth != "Bearer secret-token" {
		t.Fatalf("unexpected auth header: %q", gotAuth)
	}
	if gotBody["service_id"] != "bot-01" || gotBody["secret_name"] != "discord_bot_token_profile-01" {
		t.Fatalf("unexpected request body: %#v", gotBody)
	}
	if secret.Value != "Bot <RAW_DISCORD_TOKEN>" || secret.ExpiresInSec != 60 {
		t.Fatalf("unexpected runtime secret: %#v", secret)
	}
}

func TestStartStreamPostsServiceStartRequest(t *testing.T) {
	var gotAuth string
	var gotBody map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/services/streams/stream-01/start" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		gotAuth = r.Header.Get("Authorization")
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Fatal(err)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := Client{Config: Config{ControlPanelURL: server.URL, Token: "secret-token", ServiceID: "bot-01", ServiceName: "Discord 01", ServicePublicURL: "https://discord.example.com"}}
	if err := client.StartStream(t.Context(), "stream-01"); err != nil {
		t.Fatal(err)
	}
	if gotAuth != "Bearer secret-token" {
		t.Fatalf("unexpected auth header: %q", gotAuth)
	}
	if len(gotBody) != 0 {
		t.Fatalf("auto-start request should not send stream overrides: %#v", gotBody)
	}
}

func TestResolveRuntimeSecretLeaseActiveIsTypedAndRedacted(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/services/runtime-secrets/resolve" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"code":"runtime_secret_lease_active","value":"Bot <RAW_DISCORD_TOKEN>","detail":"secret-token"}`))
	}))
	defer server.Close()

	client := Client{Config: Config{ControlPanelURL: server.URL, Token: "secret-token", ServiceID: "bot-01", ServiceName: "Discord 01", ServicePublicURL: "https://discord.example.com"}}
	_, err := client.ResolveRuntimeSecret(t.Context(), "discord_bot_token_profile-01")
	if err == nil {
		t.Fatal("expected lease-active error")
	}
	if !errors.Is(err, ErrRuntimeSecretLeaseActive) {
		t.Fatalf("expected ErrRuntimeSecretLeaseActive, got %v", err)
	}
	for _, forbidden := range []string{"<RAW_DISCORD_TOKEN>", "secret-token", "detail"} {
		if strings.Contains(err.Error(), forbidden) {
			t.Fatalf("runtime secret error leaked response body: %v", err)
		}
	}
}

func TestControlPanelErrorsDoNotLeakTokenOrBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "secret-token", http.StatusForbidden)
	}))
	defer server.Close()

	client := Client{Config: Config{ControlPanelURL: server.URL, Token: "secret-token", ServiceID: "bot-01", ServiceName: "Discord 01", ServicePublicURL: "https://discord.example.com"}}
	err := client.Register(t.Context())
	if err == nil {
		t.Fatal("expected error")
	}
	if strings.Contains(err.Error(), "secret-token") {
		t.Fatalf("token leaked in error: %v", err)
	}
}

func TestControlPanelClientDoesNotFollowRedirectsWithBearerToken(t *testing.T) {
	var redirectedAuth string
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		redirectedAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL+"/capture", http.StatusFound)
	}))
	defer server.Close()

	client := Client{Config: Config{ControlPanelURL: server.URL, Token: "secret-token", ServiceID: "bot-01", ServiceName: "Discord 01", ServicePublicURL: "https://discord.example.com"}}
	err := client.Register(t.Context())
	if err == nil {
		t.Fatal("expected redirect response to fail")
	}
	if redirectedAuth != "" {
		t.Fatalf("authorization header followed redirect: %q", redirectedAuth)
	}
}

func TestValidateRejectsNonHTTPControlPanelURL(t *testing.T) {
	cfg := Config{ControlPanelURL: "ftp://control.example.com/api", Token: "<SERVICE_TOKEN>", ServiceID: "bot-01", ServiceName: "Discord 01", ServicePublicURL: "https://discord.example.com"}
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected validation error")
	}
	if !strings.Contains(err.Error(), "http or https") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidateRejectsNonHTTPServicePublicURL(t *testing.T) {
	cfg := Config{ControlPanelURL: "https://control.example.com", Token: "<SERVICE_TOKEN>", ServiceID: "bot-01", ServiceName: "Discord 01", ServicePublicURL: "ftp://discord.example.com"}
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected validation error")
	}
	if !strings.Contains(err.Error(), "SERVICE_PUBLIC_URL") || !strings.Contains(err.Error(), "http or https") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidateRejectsRemoteHTTPControlPanelURL(t *testing.T) {
	cfg := Config{ControlPanelURL: "http://control.example.com", Token: "<SERVICE_TOKEN>", ServiceID: "bot-01", ServiceName: "Discord 01", ServicePublicURL: "https://discord.example.com"}
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected validation error")
	}
	if !strings.Contains(err.Error(), "https for remote hosts") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidateAllowsLocalHTTPControlPanelURL(t *testing.T) {
	cfg := Config{ControlPanelURL: "http://127.0.0.1:8080", Token: "<SERVICE_TOKEN>", ServiceID: "bot-01", ServiceName: "Discord 01", ServicePublicURL: "http://host.docker.internal:18082"}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected local http URLs to be allowed: %v", err)
	}
}

func TestValidateRejectsRemoteHTTPServicePublicURL(t *testing.T) {
	cfg := Config{ControlPanelURL: "https://control.example.com", Token: "<SERVICE_TOKEN>", ServiceID: "bot-01", ServiceName: "Discord 01", ServicePublicURL: "http://discord.example.com"}
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected validation error")
	}
	if !strings.Contains(err.Error(), "SERVICE_PUBLIC_URL") || !strings.Contains(err.Error(), "https for remote hosts") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidateRejectsControlPanelURLQueryOrFragment(t *testing.T) {
	cfg := Config{ControlPanelURL: "https://control.example.com#bearer", Token: "<SERVICE_TOKEN>", ServiceID: "bot-01", ServiceName: "Discord 01", ServicePublicURL: "https://discord.example.com"}
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected validation error")
	}
	if !strings.Contains(err.Error(), "query or fragment") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestConfigFromEnv(t *testing.T) {
	t.Setenv("CONTROL_PANEL_URL", "https://control.example.com")
	t.Setenv("CONTROL_PANEL_TOKEN", "<SERVICE_TOKEN>")
	t.Setenv("SERVICE_ID", "bot-01")
	t.Setenv("SERVICE_NAME", "Discord 01")
	t.Setenv("SERVICE_PUBLIC_URL", "https://discord.example.com")
	t.Setenv("SERVICE_VERSION", "0.1.0")
	t.Setenv("CONTROL_PANEL_HEARTBEAT_INTERVAL_SEC", "5")

	cfg := ConfigFromEnv()
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	if cfg.HeartbeatEvery != 5*time.Second || cfg.ServicePublicURL == "" {
		t.Fatalf("unexpected config: %#v", cfg)
	}
}
