package httpapi

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/example/autostream-discord-bot/internal/control"
	"github.com/example/autostream-discord-bot/internal/discord"
	"github.com/example/autostream-discord-bot/internal/jobs"
	"github.com/example/autostream-discord-bot/internal/version"
)

type httpFakeVoice struct {
	joined        discord.VoiceJob
	sentMessages  []discord.OutboundMessage
	sendErr       error
	sendMessageID string
}

func (f *httpFakeVoice) Connect() error { return nil }

func (f *httpFakeVoice) JoinVoice(job discord.VoiceJob) error {
	f.joined = job
	return nil
}

func (f *httpFakeVoice) LeaveVoice(streamID string) error { return nil }

func (f *httpFakeVoice) SendMessage(ctx context.Context, message discord.OutboundMessage) (discord.SentMessage, error) {
	f.sentMessages = append(f.sentMessages, message)
	if f.sendErr != nil {
		return discord.SentMessage{}, f.sendErr
	}
	messageID := f.sendMessageID
	if messageID == "" {
		messageID = "message-01"
	}
	return discord.SentMessage{MessageID: messageID}, nil
}

func (f *httpFakeVoice) Status() discord.Status {
	return discord.Status{Connected: f.joined.StreamID != "", VoiceConnected: f.joined.StreamID != ""}
}

func TestUpdaterVersionDoesNotRequireAuthorization(t *testing.T) {
	previousVersion := version.Version
	version.Version = "v1.1.1"
	t.Setenv("SERVICE_VERSION", "v9.9.9")
	t.Cleanup(func() {
		version.Version = previousVersion
	})

	handler := NewServer("discord_bot", jobs.NewManager(&discord.NoopClient{}), TokenVerifier{PlainToken: "expected"})
	req := httptest.NewRequest(http.MethodGet, "/updater/version", nil)
	res := httptest.NewRecorder()

	handler.ServeHTTP(res, req)

	if res.Code != http.StatusOK {
		t.Fatalf("expected unauthenticated updater version request to return 200, got %d body=%s", res.Code, res.Body.String())
	}
	var payload map[string]string
	if err := json.NewDecoder(res.Body).Decode(&payload); err != nil {
		t.Fatalf("decode updater version response: %v", err)
	}
	if len(payload) != 1 || payload["version"] != version.Current() {
		t.Fatalf("expected only embedded version %q, got %#v", version.Current(), payload)
	}
}

func TestProtectedEndpointsRejectMissingToken(t *testing.T) {
	server := httptest.NewServer(NewServer("discord_bot", jobs.NewManager(&discord.NoopClient{}), TokenVerifier{PlainToken: "expected"}))
	defer server.Close()

	res, err := http.Post(server.URL+"/jobs/start", "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", res.StatusCode)
	}
}

func TestStartJobRequiresValidTokenAndUpdatesStatus(t *testing.T) {
	server := httptest.NewServer(NewServer("discord_bot", jobs.NewManager(&discord.NoopClient{}), TokenVerifier{PlainToken: "expected"}))
	defer server.Close()

	body := []byte(`{"stream_id":"stream-01","guild_id":"guild-01","voice_channel_id":"voice-01","text_channel_id":"text-01","encoder_audio_url":"` + "https://" + "user:" + "secret" + "@encoder.example.com" + `","caption_audio_url":"https://caption.example.com","stream_ingest_token":"ingest-secret","caption_audio_token":"caption-job-token","worker_events_url":"https://worker.example.com","worker_events_token":"worker-events-secret"}`)
	req, err := http.NewRequest(http.MethodPost, server.URL+"/jobs/start", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer expected")
	req.Header.Set("Content-Type", "application/json")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", res.StatusCode)
	}

	statusRes, err := http.Get(server.URL + "/status")
	if err != nil {
		t.Fatal(err)
	}
	defer statusRes.Body.Close()
	if statusRes.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", statusRes.StatusCode)
	}
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(statusRes.Body); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), "stream-01") {
		t.Fatalf("status should include current stream id: %s", buf.String())
	}
	for _, raw := range []string{"secret", "encoder_audio_url", "caption_audio_url", "caption.example.com", "caption_audio_token", "caption-job-token", "guild-01", "voice-01", "text-01", "stream_ingest_token", "worker.example.com", "worker_events_url", "worker_events_token"} {
		if strings.Contains(buf.String(), raw) {
			t.Fatalf("status leaked sensitive job field %q: %s", raw, buf.String())
		}
	}
}

func TestStartJobAppliesRuntimeStreamDiscordConfig(t *testing.T) {
	voice := &httpFakeVoice{}
	handler := NewServerWithRuntimeConfig("discord_bot", jobs.NewManager(voice), TokenVerifier{PlainToken: "expected"}, func(ctx context.Context) (control.RuntimeConfig, error) {
		return control.RuntimeConfig{
			Service: control.RegisteredService{ServiceID: "discord-bot-01"},
			Assignments: []control.StreamServiceAssignment{{
				StreamID:       "stream-01",
				ServiceID:      "discord-bot-01",
				ServiceType:    "discord_bot",
				AssignmentRole: "primary",
			}},
			StreamDiscordConfigs: []control.StreamDiscordConfig{{
				StreamID:        "stream-01",
				AssignmentRole:  "primary",
				DiscordConfigID: "discord-config-01",
				GuildID:         "guild-stream",
				VoiceChannelID:  "voice-stream",
				TextChannelID:   "text-stream",
			}},
		}, nil
	})

	body := []byte(`{"stream_id":"stream-01","caption_audio_url":"https://worker.example.com/streams/stream-01/audio/opus","caption_audio_token":"caption-job-token"}`)
	req := httptest.NewRequest(http.MethodPost, "/jobs/start", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer expected")
	req.Header.Set("Content-Type", "application/json")
	res := httptest.NewRecorder()
	handler.ServeHTTP(res, req)
	if res.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d body=%s", res.Code, res.Body.String())
	}
	if voice.joined.GuildID != "guild-stream" || voice.joined.VoiceChannelID != "voice-stream" || voice.joined.TextChannelID != "text-stream" || voice.joined.CaptionAudioURL != "https://worker.example.com/streams/stream-01/audio/opus" || voice.joined.CaptionAudioToken != "caption-job-token" {
		t.Fatalf("runtime stream discord config was not applied: %#v", voice.joined)
	}
	if strings.Contains(res.Body.String(), "guild-stream") || strings.Contains(res.Body.String(), "voice-stream") || strings.Contains(res.Body.String(), "text-stream") || strings.Contains(res.Body.String(), "caption.example.com") || strings.Contains(res.Body.String(), "caption-job-token") || strings.Contains(res.Body.String(), "caption_audio_token") {
		t.Fatalf("start response leaked stream channel config: %s", res.Body.String())
	}
}

func TestStartJobDoesNotUseStandbyRuntimeStreamDiscordConfig(t *testing.T) {
	voice := &httpFakeVoice{}
	handler := NewServerWithRuntimeConfig("discord_bot", jobs.NewManager(voice), TokenVerifier{PlainToken: "expected"}, func(ctx context.Context) (control.RuntimeConfig, error) {
		return control.RuntimeConfig{
			Service: control.RegisteredService{ServiceID: "discord-bot-01"},
			Assignments: []control.StreamServiceAssignment{{
				StreamID:       "stream-01",
				ServiceID:      "discord-bot-01",
				ServiceType:    "discord_bot",
				AssignmentRole: "standby",
			}},
			StreamDiscordConfigs: []control.StreamDiscordConfig{{
				StreamID:        "stream-01",
				AssignmentRole:  "standby",
				DiscordConfigID: "discord-config-standby",
				GuildID:         "guild-standby",
				VoiceChannelID:  "voice-standby",
			}},
		}, nil
	})

	req := httptest.NewRequest(http.MethodPost, "/jobs/start", strings.NewReader(`{"stream_id":"stream-01"}`))
	req.Header.Set("Authorization", "Bearer expected")
	req.Header.Set("Content-Type", "application/json")
	res := httptest.NewRecorder()
	handler.ServeHTTP(res, req)
	if res.Code != http.StatusForbidden {
		t.Fatalf("expected standby-only runtime config to fail assignment policy, got %d body=%s", res.Code, res.Body.String())
	}
	if voice.joined.StreamID != "" {
		t.Fatalf("standby config must not start a job: %#v", voice.joined)
	}
	if strings.Contains(res.Body.String(), "guild-standby") || strings.Contains(res.Body.String(), "voice-standby") {
		t.Fatalf("standby channel config leaked in error response: %s", res.Body.String())
	}
}

func TestStartJobRejectsUnassignedRuntimeStreamEvenWithRequestChannels(t *testing.T) {
	voice := &httpFakeVoice{}
	handler := NewServerWithRuntimeConfig("discord_bot", jobs.NewManager(voice), TokenVerifier{PlainToken: "expected"}, func(ctx context.Context) (control.RuntimeConfig, error) {
		return control.RuntimeConfig{
			Service: control.RegisteredService{ServiceID: "discord-bot-01"},
			Assignments: []control.StreamServiceAssignment{{
				StreamID:       "stream-01",
				ServiceID:      "discord-bot-02",
				ServiceType:    "discord_bot",
				AssignmentRole: "primary",
			}},
			StreamDiscordConfigs: []control.StreamDiscordConfig{{
				StreamID:        "stream-01",
				AssignmentRole:  "primary",
				DiscordConfigID: "discord-config-01",
				GuildID:         "guild-runtime",
				VoiceChannelID:  "voice-runtime",
			}},
		}, nil
	})

	req := httptest.NewRequest(http.MethodPost, "/jobs/start", strings.NewReader(`{"stream_id":"stream-01","guild_id":"guild-request","voice_channel_id":"voice-request"}`))
	req.Header.Set("Authorization", "Bearer expected")
	req.Header.Set("Content-Type", "application/json")
	res := httptest.NewRecorder()
	handler.ServeHTTP(res, req)
	if res.Code != http.StatusForbidden {
		t.Fatalf("expected unassigned stream to be rejected, got %d body=%s", res.Code, res.Body.String())
	}
	if !strings.Contains(res.Body.String(), `"code":"stream_not_assigned_to_service"`) {
		t.Fatalf("expected stream assignment error code, got %s", res.Body.String())
	}
	if voice.joined.StreamID != "" {
		t.Fatalf("unassigned stream must not start a job: %#v", voice.joined)
	}
	for _, leaked := range []string{"guild-request", "voice-request", "guild-runtime", "voice-runtime"} {
		if strings.Contains(res.Body.String(), leaked) {
			t.Fatalf("assignment rejection leaked channel config %q: %s", leaked, res.Body.String())
		}
	}
}

func TestStartJobFailsClosedWhenRuntimeConfigFetchFailsEvenWithRequestChannels(t *testing.T) {
	voice := &httpFakeVoice{}
	handler := NewServerWithRuntimeConfig("discord_bot", jobs.NewManager(voice), TokenVerifier{PlainToken: "expected"}, func(ctx context.Context) (control.RuntimeConfig, error) {
		return control.RuntimeConfig{}, errors.New("control panel unavailable")
	})

	req := httptest.NewRequest(http.MethodPost, "/jobs/start", strings.NewReader(`{"stream_id":"stream-01","guild_id":"guild-request","voice_channel_id":"voice-request"}`))
	req.Header.Set("Authorization", "Bearer expected")
	req.Header.Set("Content-Type", "application/json")
	res := httptest.NewRecorder()
	handler.ServeHTTP(res, req)
	if res.Code != http.StatusBadGateway {
		t.Fatalf("expected runtime config fetch failure to fail closed, got %d body=%s", res.Code, res.Body.String())
	}
	if !strings.Contains(res.Body.String(), `"code":"runtime_config_fetch_failed"`) {
		t.Fatalf("expected runtime config fetch error code, got %s", res.Body.String())
	}
	if voice.joined.StreamID != "" {
		t.Fatalf("runtime config fetch failure must not start a job: %#v", voice.joined)
	}
	for _, leaked := range []string{"guild-request", "voice-request", "control panel unavailable"} {
		if strings.Contains(res.Body.String(), leaked) {
			t.Fatalf("runtime config fetch rejection leaked %q: %s", leaked, res.Body.String())
		}
	}
}

func TestSHA256TokenVerifier(t *testing.T) {
	sum := sha256.Sum256([]byte("expected"))
	verifier := TokenVerifier{SHA256Hex: hex.EncodeToString(sum[:])}
	if !verifier.Verify("Bearer expected") {
		t.Fatal("expected token to verify")
	}
	if verifier.Verify("Bearer wrong") {
		t.Fatal("wrong token verified")
	}
}

func TestTokenVerifierFromEnvRejectsControlPanelTokenFallbackInProduction(t *testing.T) {
	t.Setenv("CONTROL_PANEL_TOKEN", "control-panel-token")
	t.Setenv("AUTOSTREAM_ENV", "production")
	verifier := TokenVerifierFromEnv()
	if verifier.Verify("Bearer control-panel-token") {
		t.Fatal("CONTROL_PANEL_TOKEN must not authorize inbound Discord Bot control requests in production")
	}
}

func TestTokenVerifierFromEnvRejectsControlPanelTokenFallbackWhenRuntimeConfigRequired(t *testing.T) {
	t.Setenv("CONTROL_PANEL_TOKEN", "control-panel-token")
	t.Setenv("AUTOSTREAM_REQUIRE_CONTROL_PANEL_RUNTIME_CONFIG", "true")
	verifier := TokenVerifierFromEnv()
	if verifier.Verify("Bearer control-panel-token") {
		t.Fatal("CONTROL_PANEL_TOKEN must not authorize inbound Discord Bot control requests when runtime config is required")
	}
}

func TestTokenVerifierFromEnvAllowsControlPanelTokenFallbackOutsideProduction(t *testing.T) {
	t.Setenv("CONTROL_PANEL_TOKEN", "control-panel-token")
	verifier := TokenVerifierFromEnv()
	if !verifier.Verify("Bearer control-panel-token") {
		t.Fatal("expected local compatibility CONTROL_PANEL_TOKEN fallback outside production")
	}
}

func TestTokenVerifierReadsNodeRuntimeTokenAfterStartup(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yml")
	t.Setenv("AUTOSTREAM_NODE_CONFIG", path)
	t.Setenv("CONTROL_PANEL_TOKEN", "")
	verifier := TokenVerifierFromEnv()
	if verifier.Verify("Bearer runtime-secret") {
		t.Fatal("runtime token should not verify before config exists")
	}
	writeNodeConfigForVerifierTest(t, path, "discord_bot")
	if !verifier.Verify("Bearer runtime-secret") {
		t.Fatal("runtime token should verify after config is written")
	}
}

func TestErrorDoesNotEchoBearerToken(t *testing.T) {
	server := httptest.NewServer(NewServer("discord_bot", jobs.NewManager(&discord.NoopClient{}), TokenVerifier{PlainToken: "secret-token"}))
	defer server.Close()

	req, err := http.NewRequest(http.MethodPost, server.URL+"/jobs/start", strings.NewReader(`{"stream_id":"","guild_id":"","voice_channel_id":""}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer secret-token")
	req.Header.Set("Content-Type", "application/json")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(res.Body); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(buf.String(), "secret-token") {
		t.Fatalf("token leaked in response: %s", buf.String())
	}
}

func writeNodeConfigForVerifierTest(t *testing.T, path, nodeType string) {
	t.Helper()
	body := `panel:
  url: "https://panel.example.jp"
node:
  id: "discord-bot-01"
  name: "Discord Bot 01"
  type: "` + nodeType + `"
api:
  host: "discord.example.jp"
  port: 8443
  ssl_enabled: true
auth:
  token_id: "token-id"
  token: "runtime-secret"
`
	if err := os.WriteFile(path, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
}
