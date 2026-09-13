package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/example/autostream-discord-bot/internal/control"
	"github.com/example/autostream-discord-bot/internal/jobs"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

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

	body := []byte(`{"stream_id":"stream-01","job_generation":17,"caption_audio_url":"https://worker.example.com/streams/stream-01/audio/opus","caption_audio_token":"caption-job-token","stream_ingest_token":"encoder-job-token","worker_events_url":"https://worker.example.com/events","worker_events_token":"worker-job-token","caption_audio_flush_ms":125,"caption_audio_max_batch_packets":32,"unresolved_ssrc_buffer_ms":900}`)
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
	if voice.joined.StreamIngestToken != "encoder-job-token" || voice.joined.WorkerEventsURL != "https://worker.example.com/events" || voice.joined.WorkerEventsToken != "worker-job-token" || voice.joined.CaptionAudioFlushMS != 125 || voice.joined.CaptionAudioMaxBatchPackets != 32 || voice.joined.UnresolvedSSRCBufferMS != 900 {
		t.Fatalf("legacy Control Panel job compatibility changed: %#v", voice.joined)
	}
	for _, sensitive := range []string{"guild-stream", "voice-stream", "text-stream", "caption.example.com", "caption-job-token", "caption_audio_token", "encoder-job-token", "worker.example.com", "worker-job-token", "stream_ingest_token", "worker_events_token"} {
		if strings.Contains(res.Body.String(), sensitive) {
			t.Fatalf("start response leaked legacy runtime field %q: %s", sensitive, res.Body.String())
		}
	}
}

func TestStartJobV2UsesResolvedTargetSnapshotWithoutRuntimeOverride(t *testing.T) {
	voice := &httpFakeVoice{}
	runtimeConfigCalls := 0
	handler := NewServerWithRuntimeConfig("discord_bot", jobs.NewManager(voice), TokenVerifier{PlainToken: "expected"}, func(ctx context.Context) (control.RuntimeConfig, error) {
		runtimeConfigCalls++
		return control.RuntimeConfig{
			Service: control.RegisteredService{ServiceID: "discord-bot-01"},
			Assignments: []control.StreamServiceAssignment{{
				StreamID:       "stream-01",
				ServiceID:      "discord-bot-01",
				ServiceType:    "discord_bot",
				AssignmentRole: "primary",
			}},
			// These legacy runtime target values deliberately differ from the
			// server-resolved v2 snapshot. They remain compatibility input for
			// versionless requests, but must not overwrite a v2 target.
			StreamDiscordConfigs: []control.StreamDiscordConfig{{
				StreamID:        "stream-01",
				AssignmentRole:  "primary",
				DiscordConfigID: "legacy-config-01",
				GuildID:         "900000000000000001",
				TextChannelID:   "900000000000000002",
				VoiceChannelID:  "900000000000000003",
			}},
		}, nil
	})

	body := `{
		"schema_version":2,
		"stream_id":"stream-01",
		"job_generation":17,
		"discord_target":{
			"revision":29,
			"resolved":{
				"guild_id":"100000000000000001",
				"text_channel_id":"100000000000000002",
				"voice_channel_id":"100000000000000003"
			}
		},
		"encoder_audio_url":"https://encoder.example.com/audio",
		"caption_audio_url":"https://worker.example.com/audio",
		"stream_ingest_token":"encoder-job-token",
		"caption_audio_token":"caption-job-token",
		"worker_events_url":"https://worker.example.com/events",
		"worker_events_token":"worker-job-token",
		"caption_audio_flush_ms":125,
		"caption_audio_max_batch_packets":32,
		"unresolved_ssrc_buffer_ms":900
	}`
	req := httptest.NewRequest(http.MethodPost, "/jobs/start", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer expected")
	req.Header.Set("Content-Type", "application/json")
	res := httptest.NewRecorder()
	handler.ServeHTTP(res, req)

	if res.Code != http.StatusAccepted {
		t.Fatalf("status=%d body=%s, want 202", res.Code, res.Body.String())
	}
	if runtimeConfigCalls != 1 || voice.joinCalls != 1 {
		t.Fatalf("side effects: runtime_config_calls=%d join_calls=%d, want 1/1", runtimeConfigCalls, voice.joinCalls)
	}
	if voice.joined.GuildID != "100000000000000001" || voice.joined.TextChannelID != "100000000000000002" || voice.joined.VoiceChannelID != "100000000000000003" {
		t.Fatalf("v2 resolved target was not preserved: %#v", voice.joined)
	}
	if voice.joined.DiscordTargetRevision != 29 {
		t.Fatalf("DiscordTargetRevision=%d, want 29", voice.joined.DiscordTargetRevision)
	}
	if voice.joined.StreamIngestToken != "encoder-job-token" || voice.joined.CaptionAudioToken != "caption-job-token" || voice.joined.WorkerEventsToken != "worker-job-token" {
		t.Fatalf("v2 audio/event token mapping changed: %#v", voice.joined)
	}
	if voice.joined.CaptionAudioFlushMS != 125 || voice.joined.CaptionAudioMaxBatchPackets != 32 || voice.joined.UnresolvedSSRCBufferMS != 900 {
		t.Fatalf("v2 caption tuning normalization changed: %#v", voice.joined)
	}
	for _, sensitive := range []string{
		"100000000000000001", "100000000000000002", "100000000000000003",
		"900000000000000001", "900000000000000002", "900000000000000003",
		"encoder-job-token", "caption-job-token", "worker-job-token", "discord_target", "target_revision",
	} {
		if strings.Contains(res.Body.String(), sensitive) {
			t.Fatalf("start response leaked v2 internal field %q: %s", sensitive, res.Body.String())
		}
	}
}

func TestDecodeStartJobRequestV2AcceptsCaptionTuningBounds(t *testing.T) {
	const target = `"discord_target":{"revision":29,"resolved":{"guild_id":"100000000000000001","text_channel_id":"100000000000000002","voice_channel_id":"100000000000000003"}}`
	tests := []struct {
		name      string
		tuning    string
		wantFlush int
		wantBatch int
		wantSSRC  int
		wantSet   bool
	}{
		{name: "omitted"},
		{name: "minimums", tuning: `"caption_audio_flush_ms":10,"caption_audio_max_batch_packets":1,"unresolved_ssrc_buffer_ms":0`, wantFlush: 10, wantBatch: 1, wantSSRC: 0, wantSet: true},
		{name: "maximums", tuning: `"caption_audio_flush_ms":1000,"caption_audio_max_batch_packets":100,"unresolved_ssrc_buffer_ms":5000`, wantFlush: 1000, wantBatch: 100, wantSSRC: 5000, wantSet: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tuning := ""
			if test.tuning != "" {
				tuning = "," + test.tuning
			}
			body := `{"schema_version":2,"stream_id":"stream-01","job_generation":17,` + target + tuning + `}`
			req := httptest.NewRequest(http.MethodPost, "/jobs/start", strings.NewReader(body))
			job, mode, err := decodeStartJobRequest(req)
			if err != nil {
				t.Fatalf("decodeStartJobRequest() error = %v", err)
			}
			if mode != startJobRequestResolvedTargetV2 {
				t.Fatalf("mode=%d, want v2", mode)
			}
			if job.CaptionAudioFlushMS != test.wantFlush || job.CaptionAudioMaxBatchPackets != test.wantBatch || job.UnresolvedSSRCBufferMS != test.wantSSRC {
				t.Fatalf("normalized tuning=%d/%d/%d, want %d/%d/%d", job.CaptionAudioFlushMS, job.CaptionAudioMaxBatchPackets, job.UnresolvedSSRCBufferMS, test.wantFlush, test.wantBatch, test.wantSSRC)
			}
			if job.UnresolvedSSRCBufferMSSet != test.wantSet {
				t.Fatalf("UnresolvedSSRCBufferMSSet=%t, want %t", job.UnresolvedSSRCBufferMSSet, test.wantSet)
			}
		})
	}
}

func TestStartJobV2RejectsInvalidResolvedTargetContractsBeforeSideEffects(t *testing.T) {
	const valid = `{"schema_version":2,"stream_id":"stream-01","job_generation":17,"discord_target":{"revision":29,"resolved":{"guild_id":"100000000000000001","text_channel_id":"100000000000000002","voice_channel_id":"100000000000000003"}}}`
	tests := []struct {
		name     string
		body     string
		wantCode string
	}{
		{name: "unknown version", body: strings.Replace(valid, `"schema_version":2`, `"schema_version":3`, 1), wantCode: "unsupported_schema_version"},
		{name: "null version", body: strings.Replace(valid, `"schema_version":2`, `"schema_version":null`, 1), wantCode: "invalid_json"},
		{name: "mixed legacy guild", body: strings.TrimSuffix(valid, "}") + `,"guild_id":"100000000000000001"}`, wantCode: "invalid_json"},
		{name: "missing target revision", body: strings.Replace(valid, `"revision":29,`, "", 1), wantCode: "discord_target_invalid"},
		{name: "zero target revision", body: strings.Replace(valid, `"revision":29`, `"revision":0`, 1), wantCode: "discord_target_invalid"},
		{name: "missing resolved target", body: strings.Replace(valid, `,"resolved":{"guild_id":"100000000000000001","text_channel_id":"100000000000000002","voice_channel_id":"100000000000000003"}`, "", 1), wantCode: "discord_target_invalid"},
		{name: "preset reference", body: strings.Replace(valid, `"revision":29`, `"revision":29,"preset_id":"preset-01"`, 1), wantCode: "invalid_json"},
		{name: "legacy preset reference", body: strings.TrimSuffix(valid, "}") + `,"preset_id":"preset-01"}`, wantCode: "invalid_json"},
		{name: "missing text channel", body: strings.Replace(valid, `"text_channel_id":"100000000000000002",`, "", 1), wantCode: "discord_target_invalid"},
		{name: "non numeric guild", body: strings.Replace(valid, `"100000000000000001"`, `"guild-01"`, 1), wantCode: "discord_target_invalid"},
		{name: "oversized voice id", body: strings.Replace(valid, `"100000000000000003"`, `"123456789012345678901234567890123"`, 1), wantCode: "discord_target_invalid"},
		{name: "unknown resolved field", body: strings.Replace(valid, `"voice_channel_id":"100000000000000003"`, `"voice_channel_id":"100000000000000003","unknown":"value"`, 1), wantCode: "invalid_json"},
		{name: "caption flush below minimum", body: strings.TrimSuffix(valid, "}") + `,"caption_audio_flush_ms":9}`, wantCode: "invalid_json"},
		{name: "caption flush above maximum", body: strings.TrimSuffix(valid, "}") + `,"caption_audio_flush_ms":1001}`, wantCode: "invalid_json"},
		{name: "caption batch below minimum", body: strings.TrimSuffix(valid, "}") + `,"caption_audio_max_batch_packets":0}`, wantCode: "invalid_json"},
		{name: "caption batch above maximum", body: strings.TrimSuffix(valid, "}") + `,"caption_audio_max_batch_packets":101}`, wantCode: "invalid_json"},
		{name: "SSRC buffer below minimum", body: strings.TrimSuffix(valid, "}") + `,"unresolved_ssrc_buffer_ms":-1}`, wantCode: "invalid_json"},
		{name: "SSRC buffer above maximum", body: strings.TrimSuffix(valid, "}") + `,"unresolved_ssrc_buffer_ms":5001}`, wantCode: "invalid_json"},
		{name: "caption flush null", body: strings.TrimSuffix(valid, "}") + `,"caption_audio_flush_ms":null}`, wantCode: "invalid_json"},
		{name: "caption batch string", body: strings.TrimSuffix(valid, "}") + `,"caption_audio_max_batch_packets":"1"}`, wantCode: "invalid_json"},
		{name: "SSRC buffer fractional", body: strings.TrimSuffix(valid, "}") + `,"unresolved_ssrc_buffer_ms":0.5}`, wantCode: "invalid_json"},
		{name: "non contract flush alias", body: strings.TrimSuffix(valid, "}") + `,"caption_flush_interval_ms":10}`, wantCode: "invalid_json"},
		{name: "non contract batch alias", body: strings.TrimSuffix(valid, "}") + `,"caption_max_batch_messages":1}`, wantCode: "invalid_json"},
		{name: "non contract delete alias", body: strings.TrimSuffix(valid, "}") + `,"caption_delete_delay_ms":0}`, wantCode: "invalid_json"},
		{name: "second JSON value", body: valid + `{}`, wantCode: "invalid_json"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			voice := &httpFakeVoice{}
			runtimeConfigCalls := 0
			handler := NewServerWithRuntimeConfig("discord_bot", jobs.NewManager(voice), TokenVerifier{PlainToken: "expected"}, func(context.Context) (control.RuntimeConfig, error) {
				runtimeConfigCalls++
				return control.RuntimeConfig{}, nil
			})
			req := httptest.NewRequest(http.MethodPost, "/jobs/start", strings.NewReader(test.body))
			req.Header.Set("Authorization", "Bearer expected")
			res := httptest.NewRecorder()
			handler.ServeHTTP(res, req)

			if res.Code != http.StatusBadRequest {
				t.Fatalf("status=%d body=%s, want 400", res.Code, res.Body.String())
			}
			var response map[string]string
			if err := json.Unmarshal(res.Body.Bytes(), &response); err != nil {
				t.Fatal(err)
			}
			if response["code"] != test.wantCode || len(response) != 1 {
				t.Fatalf("response=%#v, want bounded code %q only", response, test.wantCode)
			}
			if runtimeConfigCalls != 0 || voice.joinCalls != 0 || voice.joined.StreamID != "" {
				t.Fatalf("invalid v2 payload reached side effects: runtime_config_calls=%d joined=%#v", runtimeConfigCalls, voice.joined)
			}
			for _, forbidden := range []string{"preset-01", "guild-01", "100000000000000001", "100000000000000002", "100000000000000003"} {
				if strings.Contains(res.Body.String(), forbidden) {
					t.Fatalf("bounded error leaked request field %q: %s", forbidden, res.Body.String())
				}
			}
		})
	}
}

func TestStartJobV2RejectsUnassignedStreamWithoutResolvingTargetAgain(t *testing.T) {
	voice := &httpFakeVoice{}
	runtimeConfigCalls := 0
	handler := NewServerWithRuntimeConfig("discord_bot", jobs.NewManager(voice), TokenVerifier{PlainToken: "expected"}, func(context.Context) (control.RuntimeConfig, error) {
		runtimeConfigCalls++
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
				DiscordConfigID: "legacy-config-01",
				GuildID:         "900000000000000001",
				TextChannelID:   "900000000000000002",
				VoiceChannelID:  "900000000000000003",
			}},
		}, nil
	})
	body := `{"schema_version":2,"stream_id":"stream-01","job_generation":17,"discord_target":{"revision":29,"resolved":{"guild_id":"100000000000000001","text_channel_id":"100000000000000002","voice_channel_id":"100000000000000003"}}}`
	req := httptest.NewRequest(http.MethodPost, "/jobs/start", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer expected")
	res := httptest.NewRecorder()
	handler.ServeHTTP(res, req)

	if res.Code != http.StatusForbidden || !strings.Contains(res.Body.String(), `"code":"stream_not_assigned_to_service"`) {
		t.Fatalf("status=%d body=%s, want bounded assignment rejection", res.Code, res.Body.String())
	}
	if runtimeConfigCalls != 1 || voice.joinCalls != 0 {
		t.Fatalf("unassigned v2 side effects: runtime_config_calls=%d join_calls=%d, want 1/0", runtimeConfigCalls, voice.joinCalls)
	}
	for _, target := range []string{"100000000000000001", "100000000000000002", "100000000000000003", "900000000000000001", "900000000000000002", "900000000000000003"} {
		if strings.Contains(res.Body.String(), target) {
			t.Fatalf("assignment rejection leaked resolved target %q: %s", target, res.Body.String())
		}
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
