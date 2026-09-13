package httpapi

import (
	"bytes"
	"encoding/json"
	"github.com/example/autostream-discord-bot/internal/control"
	"github.com/example/autostream-discord-bot/internal/discord"
	"github.com/example/autostream-discord-bot/internal/jobs"
	"github.com/example/autostream-discord-bot/internal/version"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
)

func TestUpdaterVersionDoesNotRequireAuthorization(t *testing.T) {
	previousVersion := version.Version
	version.Version = "v1.1.1"
	configPath := filepath.Join(t.TempDir(), "config.yml")
	writeNodeConfigForVerifierTest(t, configPath, control.ServiceType)
	t.Setenv("AUTOSTREAM_NODE_CONFIG", configPath)
	t.Setenv("SERVICE_ID", "legacy-env-service-id")
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
	if got := res.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("updater version cache control = %q", got)
	}
	var payload map[string]any
	if err := json.NewDecoder(res.Body).Decode(&payload); err != nil {
		t.Fatalf("decode updater version response: %v", err)
	}
	if len(payload) != 4 ||
		payload["version"] != version.Current() ||
		payload["service_id"] != "discord-bot-01" ||
		payload["service_type"] != control.ServiceType ||
		payload["config_revision"] != float64(11) {
		t.Fatalf("expected embedded version and configured service identity, got %#v", payload)
	}

	methodReq := httptest.NewRequest(http.MethodPost, "/updater/version", nil)
	methodRes := httptest.NewRecorder()
	handler.ServeHTTP(methodRes, methodReq)
	if methodRes.Code != http.StatusMethodNotAllowed {
		t.Fatalf("updater version POST status = %d body = %s", methodRes.Code, methodRes.Body.String())
	}
	if got := methodRes.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("updater version POST cache control = %q", got)
	}
	if got := methodRes.Header().Get("Allow"); !strings.Contains(got, http.MethodGet) {
		t.Fatalf("updater version POST Allow = %q", got)
	}
}

func TestNewServerFailsClosedForInvalidListenerConfigRevision(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.yml")
	writeNodeConfigForVerifierTest(t, configPath, control.ServiceType)
	writeNodeListenerCredentialForVerifierTest(t, configPath, control.ServiceType, "0")
	t.Setenv("AUTOSTREAM_NODE_CONFIG", configPath)
	defer func() {
		if recover() == nil {
			t.Fatal("NewServer must reject an invalid listener config_revision")
		}
	}()
	_ = NewServer(control.ServiceType, jobs.NewManager(&discord.NoopClient{}), TokenVerifier{})
}

func TestNewServerFailsClosedForInvalidNodeIdentity(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.yml")
	writeNodeConfigForVerifierTest(t, configPath, "worker")
	t.Setenv("AUTOSTREAM_NODE_CONFIG", configPath)
	defer func() {
		if recover() == nil {
			t.Fatal("NewServer must reject a node config for a different service type")
		}
	}()
	_ = NewServer(control.ServiceType, jobs.NewManager(&discord.NoopClient{}), TokenVerifier{})
}

func TestUpdaterVersionFailsClosedWhenIdentityDriftsAfterConstruction(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.yml")
	writeNodeConfigForVerifierTest(t, configPath, control.ServiceType)
	t.Setenv("AUTOSTREAM_NODE_CONFIG", configPath)
	handler := NewServer(control.ServiceType, jobs.NewManager(&discord.NoopClient{}), TokenVerifier{})
	writeNodeListenerCredentialForVerifierTest(t, configPath, control.ServiceType, "12")

	req := httptest.NewRequest(http.MethodGet, "/updater/version", nil)
	res := httptest.NewRecorder()
	handler.ServeHTTP(res, req)

	if res.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d body = %s, want 503", res.Code, res.Body.String())
	}
	if strings.Contains(res.Body.String(), "service_id") {
		t.Fatalf("drift response leaked service identity: %s", res.Body.String())
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
	voice := &httpFakeVoice{}
	server := httptest.NewServer(NewServer("discord_bot", jobs.NewManager(voice), TokenVerifier{PlainToken: "expected"}))
	defer server.Close()

	body := []byte(`{"stream_id":"stream-01","job_generation":17,"guild_id":"guild-01","voice_channel_id":"voice-01","text_channel_id":"text-01","encoder_audio_url":"` + "https://" + "user:" + "secret" + "@encoder.example.com" + `","caption_audio_url":"https://caption.example.com","stream_ingest_token":"ingest-secret","caption_audio_token":"caption-job-token","worker_events_url":"https://worker.example.com","worker_events_token":"worker-events-secret"}`)
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
	var startBody bytes.Buffer
	if _, err := startBody.ReadFrom(res.Body); err != nil {
		t.Fatal(err)
	}
	if voice.joined.JobGeneration != 17 {
		t.Fatalf("manager received job_generation=%d, want 17", voice.joined.JobGeneration)
	}
	assertPublicStatus := func(label, raw string) {
		t.Helper()
		if strings.Contains(raw, `"job_generation"`) {
			t.Fatalf("%s exposed internal job_generation: %s", label, raw)
		}
		for _, expected := range []string{`"current_job"`, `"stream_id":"stream-01"`, `"current_stream_id":"stream-01"`, `"discord"`, `"metrics"`, `"participant_count"`} {
			if !strings.Contains(raw, expected) {
				t.Fatalf("%s omitted public status field %q: %s", label, expected, raw)
			}
		}
		for _, sensitive := range []string{"secret", "encoder_audio_url", "caption_audio_url", "caption.example.com", "caption_audio_token", "caption-job-token", "guild-01", "voice-01", "text-01", "stream_ingest_token", "worker.example.com", "worker_events_url", "worker_events_token"} {
			if strings.Contains(raw, sensitive) {
				t.Fatalf("%s leaked sensitive job field %q: %s", label, sensitive, raw)
			}
		}
	}
	assertPublicStatus("start response", startBody.String())

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
	assertPublicStatus("GET /status response", buf.String())
}
