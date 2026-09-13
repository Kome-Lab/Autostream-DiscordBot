package httpapi

import (
	"context"
	"encoding/json"
	"github.com/example/autostream-discord-bot/internal/control"
	"github.com/example/autostream-discord-bot/internal/jobs"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

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
