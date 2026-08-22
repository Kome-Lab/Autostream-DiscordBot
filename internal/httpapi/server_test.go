package httpapi

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/example/autostream-discord-bot/internal/control"
	"github.com/example/autostream-discord-bot/internal/discord"
	"github.com/example/autostream-discord-bot/internal/jobs"
	"github.com/example/autostream-discord-bot/internal/version"
)

const expectedStartJobRequestLimitBytes = 1_048_576
const expectedStartJobRequestLimitPlusOneBytes = 1_048_577

func TestStartJobRequestLimitIsOneMiB(t *testing.T) {
	if maxStartJobRequestBytes != expectedStartJobRequestLimitBytes {
		t.Fatalf(
			"maxStartJobRequestBytes = %d, want %d",
			maxStartJobRequestBytes,
			expectedStartJobRequestLimitBytes,
		)
	}
}

type httpFakeVoice struct {
	joined        discord.VoiceJob
	joinCalls     int
	sentMessages  []discord.OutboundMessage
	sendErr       error
	sendMessageID string
}

type httpReadCountingBody struct {
	reader io.Reader
	reads  int
}

func (b *httpReadCountingBody) Read(p []byte) (int, error) {
	b.reads++
	return b.reader.Read(p)
}

func (b *httpReadCountingBody) Close() error { return nil }

// httpPanelStopCallbackStopper models the Control Panel's nested callback to
// this Bot's authenticated /jobs/{id}/stop endpoint while the Bot's outbound
// auto-stop request is still awaiting the rest of the Panel dispatch.
type httpPanelStopCallbackStopper struct {
	handler     http.Handler
	token       string
	started     chan string
	callback    chan string
	ready       chan string
	release     chan struct{}
	returned    chan string
	ctxCanceled chan string
}

func (f *httpPanelStopCallbackStopper) StopStream(streamID string) error {
	return errors.New("context-aware auto-stop was not used")
}

func (f *httpPanelStopCallbackStopper) StopStreamContext(ctx context.Context, streamID string) error {
	select {
	case f.started <- streamID:
	default:
	}
	req := httptest.NewRequest(http.MethodPost, "/jobs/"+streamID+"/stop", nil)
	req.Header.Set("Authorization", "Bearer "+f.token)
	res := httptest.NewRecorder()
	f.handler.ServeHTTP(res, req)
	if res.Code != http.StatusAccepted {
		return errors.New("nested Control Panel stop callback was not accepted")
	}
	select {
	case f.callback <- streamID:
	default:
	}
	select {
	case <-ctx.Done():
		select {
		case f.ctxCanceled <- streamID:
		default:
		}
		return ctx.Err()
	default:
	}
	select {
	case f.ready <- streamID:
	default:
	}
	select {
	case <-ctx.Done():
		select {
		case f.ctxCanceled <- streamID:
		default:
		}
		return ctx.Err()
	case <-f.release:
		select {
		case f.returned <- streamID:
		default:
		}
		return nil
	}
}

func (f *httpFakeVoice) Connect() error { return nil }

func (f *httpFakeVoice) JoinVoice(job discord.VoiceJob) error {
	f.joinCalls++
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
	configPath := filepath.Join(t.TempDir(), "config.yml")
	writeNodeConfigForVerifierTest(t, configPath, control.ServiceType)
	t.Setenv("AUTOSTREAM_NODE_CONFIG", configPath)
	t.Setenv("SERVICE_ID", "legacy-env-service-id")
	t.Setenv("SERVICE_VERSION", "v9.9.9")
	t.Setenv("AUTOSTREAM_CONFIG_REVISION", "11")
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
}

func TestNewServerFailsClosedForInvalidConfigRevision(t *testing.T) {
	t.Setenv("AUTOSTREAM_CONFIG_REVISION", "0")
	defer func() {
		if recover() == nil {
			t.Fatal("NewServer must reject an invalid AUTOSTREAM_CONFIG_REVISION")
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
	t.Setenv("AUTOSTREAM_CONFIG_REVISION", "11")
	handler := NewServer(control.ServiceType, jobs.NewManager(&discord.NoopClient{}), TokenVerifier{})
	t.Setenv("AUTOSTREAM_NODE_CONFIG", "")
	t.Setenv("SERVICE_ID", "changed-after-start")
	t.Setenv("AUTOSTREAM_CONFIG_REVISION", "12")

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

func TestStartJobRejectsInvalidJobGenerationBeforeRuntimeConfig(t *testing.T) {
	for _, test := range []struct {
		name     string
		body     string
		wantCode string
	}{
		{
			name:     "missing",
			body:     `{"stream_id":"stream-01","guild_id":"guild-01","voice_channel_id":"voice-01"}`,
			wantCode: "job_generation_required",
		},
		{
			name:     "zero",
			body:     `{"stream_id":"stream-01","job_generation":0,"guild_id":"guild-01","voice_channel_id":"voice-01"}`,
			wantCode: "job_generation_required",
		},
		{
			name:     "negative",
			body:     `{"stream_id":"stream-01","job_generation":-1,"guild_id":"guild-01","voice_channel_id":"voice-01"}`,
			wantCode: "invalid_json",
		},
		{
			name:     "fractional",
			body:     `{"stream_id":"stream-01","job_generation":1.5,"guild_id":"guild-01","voice_channel_id":"voice-01"}`,
			wantCode: "invalid_json",
		},
		{
			name:     "string",
			body:     `{"stream_id":"stream-01","job_generation":"17","guild_id":"guild-01","voice_channel_id":"voice-01"}`,
			wantCode: "invalid_json",
		},
		{
			name:     "uint64 overflow",
			body:     `{"stream_id":"stream-01","job_generation":18446744073709551616,"guild_id":"guild-01","voice_channel_id":"voice-01"}`,
			wantCode: "invalid_json",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			voice := &httpFakeVoice{}
			runtimeConfigCalls := 0
			handler := NewServerWithRuntimeConfig("discord_bot", jobs.NewManager(voice), TokenVerifier{PlainToken: "expected"}, func(context.Context) (control.RuntimeConfig, error) {
				runtimeConfigCalls++
				return control.RuntimeConfig{}, nil
			})
			req := httptest.NewRequest(http.MethodPost, "/jobs/start", strings.NewReader(test.body))
			req.Header.Set("Authorization", "Bearer expected")
			req.Header.Set("Content-Type", "application/json")
			res := httptest.NewRecorder()
			handler.ServeHTTP(res, req)

			if res.Code != http.StatusBadRequest {
				t.Fatalf("status=%d body=%s, want 400", res.Code, res.Body.String())
			}
			var response map[string]string
			if err := json.Unmarshal(res.Body.Bytes(), &response); err != nil {
				t.Fatal(err)
			}
			if response["code"] != test.wantCode {
				t.Fatalf("code=%q body=%s, want %q", response["code"], res.Body.String(), test.wantCode)
			}
			if runtimeConfigCalls != 0 || voice.joined.StreamID != "" {
				t.Fatalf("invalid generation reached side effects: runtime_config_calls=%d joined=%#v", runtimeConfigCalls, voice.joined)
			}
		})
	}
}

func TestStartJobRequiresExactlyOneJSONValue(t *testing.T) {
	const (
		validBody = `{"stream_id":"stream-01","job_generation":17,"guild_id":"guild-01","voice_channel_id":"voice-01"}`
	)
	padRequest := func(totalLength int, suffix string) string {
		t.Helper()
		paddingLength := totalLength - len(validBody) - len(suffix)
		if paddingLength < 0 {
			t.Fatalf("cannot build %d-byte request with %d-byte suffix", totalLength, len(suffix))
		}
		body := validBody + strings.Repeat(" ", paddingLength) + suffix
		if len(body) != totalLength {
			t.Fatalf("request length=%d, want %d", len(body), totalLength)
		}
		return body
	}
	secondObject := `{"job_generation":18}`

	for _, test := range []struct {
		name             string
		body             string
		wantBodyLen      int
		authorization    string
		wantStatus       int
		wantCode         string
		wantRuntimeCalls int
		wantJoinCalls    int
		wantBodyUnread   bool
	}{
		{
			name:             "single JSON object",
			body:             validBody,
			wantBodyLen:      len(validBody),
			authorization:    "Bearer expected",
			wantStatus:       http.StatusAccepted,
			wantRuntimeCalls: 1,
			wantJoinCalls:    1,
		},
		{
			name:          "second JSON object",
			body:          validBody + secondObject,
			wantBodyLen:   len(validBody) + len(secondObject),
			authorization: "Bearer expected",
			wantStatus:    http.StatusBadRequest,
			wantCode:      "invalid_json",
		},
		{
			name:          "trailing garbage",
			body:          validBody + "garbage",
			wantBodyLen:   len(validBody) + len("garbage"),
			authorization: "Bearer expected",
			wantStatus:    http.StatusBadRequest,
			wantCode:      "invalid_json",
		},
		{
			name:             "trailing whitespace",
			body:             validBody + "   \r\n",
			wantBodyLen:      len(validBody) + len("   \r\n"),
			authorization:    "Bearer expected",
			wantStatus:       http.StatusAccepted,
			wantRuntimeCalls: 1,
			wantJoinCalls:    1,
		},
		{
			name:             "exact limit with trailing whitespace",
			body:             padRequest(expectedStartJobRequestLimitBytes, ""),
			wantBodyLen:      expectedStartJobRequestLimitBytes,
			authorization:    "Bearer expected",
			wantStatus:       http.StatusAccepted,
			wantRuntimeCalls: 1,
			wantJoinCalls:    1,
		},
		{
			name:          "one byte over limit with trailing whitespace",
			body:          padRequest(expectedStartJobRequestLimitPlusOneBytes, ""),
			wantBodyLen:   expectedStartJobRequestLimitPlusOneBytes,
			authorization: "Bearer expected",
			wantStatus:    http.StatusBadRequest,
			wantCode:      "invalid_json",
		},
		{
			name:          "second JSON object hidden beyond limit",
			body:          padRequest(expectedStartJobRequestLimitBytes+len(secondObject), secondObject),
			wantBodyLen:   expectedStartJobRequestLimitBytes + len(secondObject),
			authorization: "Bearer expected",
			wantStatus:    http.StatusBadRequest,
			wantCode:      "invalid_json",
		},
		{
			name:          "garbage hidden beyond limit",
			body:          padRequest(expectedStartJobRequestLimitPlusOneBytes, "g"),
			wantBodyLen:   expectedStartJobRequestLimitPlusOneBytes,
			authorization: "Bearer expected",
			wantStatus:    http.StatusBadRequest,
			wantCode:      "invalid_json",
		},
		{
			name:           "invalid token precedes malformed JSON",
			body:           validBody + secondObject,
			wantBodyLen:    len(validBody) + len(secondObject),
			authorization:  "Bearer wrong",
			wantStatus:     http.StatusUnauthorized,
			wantCode:       "missing_or_invalid_service_token",
			wantBodyUnread: true,
		},
		{
			name:           "invalid token precedes oversized JSON",
			body:           padRequest(expectedStartJobRequestLimitPlusOneBytes, ""),
			wantBodyLen:    expectedStartJobRequestLimitPlusOneBytes,
			authorization:  "Bearer wrong",
			wantStatus:     http.StatusUnauthorized,
			wantCode:       "missing_or_invalid_service_token",
			wantBodyUnread: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if len(test.body) != test.wantBodyLen {
				t.Fatalf("test request length=%d, want %d", len(test.body), test.wantBodyLen)
			}
			voice := &httpFakeVoice{}
			manager := jobs.NewManager(voice)
			runtimeConfigCalls := 0
			handler := NewServerWithRuntimeConfig("discord_bot", manager, TokenVerifier{PlainToken: "expected"}, func(context.Context) (control.RuntimeConfig, error) {
				runtimeConfigCalls++
				return control.RuntimeConfig{
					Service: control.RegisteredService{ServiceID: "discord-bot-01"},
					Assignments: []control.StreamServiceAssignment{{
						StreamID:       "stream-01",
						ServiceID:      "discord-bot-01",
						ServiceType:    "discord_bot",
						AssignmentRole: "primary",
					}},
					StreamDiscordConfigs: []control.StreamDiscordConfig{{
						StreamID:       "stream-01",
						AssignmentRole: "primary",
						GuildID:        "guild-01",
						VoiceChannelID: "voice-01",
					}},
				}, nil
			})
			requestBody := &httpReadCountingBody{reader: strings.NewReader(test.body)}
			req := httptest.NewRequest(http.MethodPost, "/jobs/start", requestBody)
			req.Header.Set("Authorization", test.authorization)
			req.Header.Set("Content-Type", "application/json")
			res := httptest.NewRecorder()
			handler.ServeHTTP(res, req)

			if res.Code != test.wantStatus {
				t.Fatalf("status=%d body=%s, want %d", res.Code, res.Body.String(), test.wantStatus)
			}
			if test.wantCode != "" && !strings.Contains(res.Body.String(), `"code":"`+test.wantCode+`"`) {
				t.Fatalf("body=%s, want code %q", res.Body.String(), test.wantCode)
			}
			if runtimeConfigCalls != test.wantRuntimeCalls || voice.joinCalls != test.wantJoinCalls {
				t.Fatalf("side effects: runtime_config_calls=%d join_calls=%d, want %d/%d", runtimeConfigCalls, voice.joinCalls, test.wantRuntimeCalls, test.wantJoinCalls)
			}
			if test.wantBodyUnread && requestBody.reads != 0 {
				t.Fatalf("unauthorized request body was read %d times before rejection", requestBody.reads)
			}
			if !test.wantBodyUnread && requestBody.reads == 0 {
				t.Fatal("authorized request body was not read")
			}
			status := manager.Status()
			if test.wantJoinCalls == 0 && status.CurrentStreamID != "" {
				t.Fatalf("invalid request reached Manager.Start: %#v", status)
			}
			if test.wantJoinCalls == 1 && (status.CurrentStreamID != "stream-01" || voice.joined.JobGeneration != 17) {
				t.Fatalf("valid request did not start expected job: status=%#v joined=%#v", status, voice.joined)
			}
		})
	}
}

func TestStopJobTreatsNoActiveTargetAsAlreadyStopped(t *testing.T) {
	handler := NewServer("discord_bot", jobs.NewManager(&discord.NoopClient{}), TokenVerifier{PlainToken: "service-token"})
	req := httptest.NewRequest(http.MethodPost, "/jobs/stream-01/stop", nil)
	req.Header.Set("Authorization", "Bearer service-token")
	res := httptest.NewRecorder()
	handler.ServeHTTP(res, req)
	if res.Code != http.StatusAccepted || !strings.Contains(res.Body.String(), "already_stopped") {
		t.Fatalf("idempotent stop status=%d body=%s", res.Code, res.Body.String())
	}
}

func TestStopJobCallbackDoesNotCancelOutboundAutoStopParentContext(t *testing.T) {
	const token = "service-token"
	voice := &httpFakeVoice{}
	manager := jobs.NewManager(voice)
	stopper := &httpPanelStopCallbackStopper{
		token:       token,
		started:     make(chan string, 1),
		callback:    make(chan string, 1),
		ready:       make(chan string, 1),
		release:     make(chan struct{}),
		returned:    make(chan string, 1),
		ctxCanceled: make(chan string, 1),
	}
	manager.SetStreamStopper(stopper)
	handler := NewServer("discord_bot", manager, TokenVerifier{PlainToken: token})
	stopper.handler = handler
	if err := manager.Start(discord.VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01"}); err != nil {
		t.Fatal(err)
	}
	manager.ParticipantChanged(discord.ParticipantEvent{StreamID: "stream-01", UserID: "user-01", Present: true})
	manager.ParticipantChanged(discord.ParticipantEvent{StreamID: "stream-01", UserID: "user-01", Present: false})

	select {
	case got := <-stopper.started:
		if got != "stream-01" {
			t.Fatalf("outbound auto-stop stream = %q, want stream-01", got)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("timed out waiting for outbound auto-stop request")
	}
	select {
	case got := <-stopper.callback:
		if got != "stream-01" {
			t.Fatalf("nested HTTP stop stream = %q, want stream-01", got)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for authenticated /jobs/{id}/stop callback")
	}
	if got := manager.CurrentStreamID(); got != "" {
		t.Fatalf("HTTP stop callback did not clear current stream: %q", got)
	}
	select {
	case got := <-stopper.ready:
		if got != "stream-01" {
			t.Fatalf("outbound auto-stop readiness stream = %q, want stream-01", got)
		}
	case got := <-stopper.ctxCanceled:
		t.Fatalf("HTTP stop callback canceled its own parent auto-stop context for %q", got)
	case <-time.After(time.Second):
		t.Fatal("outbound auto-stop parent context did not remain live after HTTP callback")
	}

	close(stopper.release)
	select {
	case got := <-stopper.returned:
		if got != "stream-01" {
			t.Fatalf("outbound auto-stop return stream = %q, want stream-01", got)
		}
	case <-time.After(time.Second):
		t.Fatal("outbound auto-stop did not finish after nested HTTP callback")
	}
	select {
	case got := <-stopper.ctxCanceled:
		t.Fatalf("HTTP stop callback canceled its own parent auto-stop context for %q", got)
	case <-time.After(50 * time.Millisecond):
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

	body := []byte(`{"stream_id":"stream-01","job_generation":17,"caption_audio_url":"https://worker.example.com/streams/stream-01/audio/opus","caption_audio_token":"caption-job-token"}`)
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

	req := httptest.NewRequest(http.MethodPost, "/jobs/start", strings.NewReader(`{"stream_id":"stream-01","job_generation":17}`))
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

	req := httptest.NewRequest(http.MethodPost, "/jobs/start", strings.NewReader(`{"stream_id":"stream-01","job_generation":17,"guild_id":"guild-request","voice_channel_id":"voice-request"}`))
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

	req := httptest.NewRequest(http.MethodPost, "/jobs/start", strings.NewReader(`{"stream_id":"stream-01","job_generation":17,"guild_id":"guild-request","voice_channel_id":"voice-request"}`))
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

	req, err := http.NewRequest(http.MethodPost, server.URL+"/jobs/start", strings.NewReader(`{"stream_id":"","job_generation":17,"guild_id":"","voice_channel_id":""}`))
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
