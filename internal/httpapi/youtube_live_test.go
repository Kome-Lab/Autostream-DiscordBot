package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/example/autostream-discord-bot/internal/control"
	"github.com/example/autostream-discord-bot/internal/discord"
	"github.com/example/autostream-discord-bot/internal/jobs"
)

func TestYouTubeLiveNotificationRequiresServiceToken(t *testing.T) {
	voice := &httpFakeVoice{}
	handler := NewServerWithRuntimeConfig(control.ServiceType, jobs.NewManager(voice), TokenVerifier{PlainToken: "expected"}, func(context.Context) (control.RuntimeConfig, error) {
		t.Fatal("runtime config must not be fetched before authentication")
		return control.RuntimeConfig{}, nil
	})

	res := performYouTubeNotificationRequest(t, handler, "", `{"event_id":"event-01","watch_url":"https://youtu.be/abc123"}`)
	if res.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d body=%s", res.Code, res.Body.String())
	}
	if len(voice.sentMessages) != 0 {
		t.Fatal("unauthorized notification must not send to Discord")
	}
}

func TestYouTubeLiveNotificationUsesRuntimeChannelAndIsIdempotent(t *testing.T) {
	voice := &httpFakeVoice{sendMessageID: "message-123"}
	manager := jobs.NewManager(voice)
	if err := manager.Start(discord.VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01", TextChannelID: "text-runtime"}); err != nil {
		t.Fatal(err)
	}
	runtimeFetches := 0
	handler := NewServerWithRuntimeConfig(control.ServiceType, manager, TokenVerifier{PlainToken: "expected"}, func(context.Context) (control.RuntimeConfig, error) {
		runtimeFetches++
		return notificationRuntimeConfig("stream-01", "primary", "text-runtime"), nil
	})
	body := `{"event_id":"event-01","watch_url":"https://www.youtube.com/watch?v=abc123"}`

	first := performYouTubeNotificationRequest(t, handler, "expected", body)
	if first.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", first.Code, first.Body.String())
	}
	var firstResponse youtubeLiveNotificationResponse
	if err := json.Unmarshal(first.Body.Bytes(), &firstResponse); err != nil {
		t.Fatal(err)
	}
	if firstResponse.Status != "sent" || firstResponse.MessageID != "message-123" || firstResponse.AlreadySent {
		t.Fatalf("unexpected first response: %#v", firstResponse)
	}

	second := performYouTubeNotificationRequest(t, handler, "expected", body)
	if second.Code != http.StatusOK {
		t.Fatalf("expected duplicate 200, got %d body=%s", second.Code, second.Body.String())
	}
	var secondResponse youtubeLiveNotificationResponse
	if err := json.Unmarshal(second.Body.Bytes(), &secondResponse); err != nil {
		t.Fatal(err)
	}
	if secondResponse.MessageID != firstResponse.MessageID || !secondResponse.AlreadySent {
		t.Fatalf("unexpected duplicate response: %#v", secondResponse)
	}
	if runtimeFetches != 2 {
		t.Fatalf("runtime config must be revalidated on every request, got %d fetches", runtimeFetches)
	}
	if len(voice.sentMessages) != 1 {
		t.Fatalf("duplicate event must send once, got %d sends", len(voice.sentMessages))
	}
	if voice.sentMessages[0].ChannelID != "text-runtime" {
		t.Fatalf("notification did not use runtime text channel: %#v", voice.sentMessages[0])
	}
}

func TestYouTubeLiveNotificationRejectsPayloadChannelID(t *testing.T) {
	voice := &httpFakeVoice{}
	manager := jobs.NewManager(voice)
	if err := manager.Start(discord.VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01", TextChannelID: "text-runtime"}); err != nil {
		t.Fatal(err)
	}
	handler := NewServerWithRuntimeConfig(control.ServiceType, manager, TokenVerifier{PlainToken: "expected"}, func(context.Context) (control.RuntimeConfig, error) {
		return notificationRuntimeConfig("stream-01", "primary", "text-runtime"), nil
	})

	res := performYouTubeNotificationRequest(t, handler, "expected", `{"event_id":"event-01","watch_url":"https://youtu.be/abc123","text_channel_id":"text-attacker"}`)
	if res.Code != http.StatusBadRequest || !strings.Contains(res.Body.String(), `"code":"invalid_json"`) {
		t.Fatalf("expected payload channel id to be rejected, got %d body=%s", res.Code, res.Body.String())
	}
	if len(voice.sentMessages) != 0 {
		t.Fatal("request-supplied channel id must never be used")
	}
}

func TestValidateYouTubeWatchURL(t *testing.T) {
	valid := []struct {
		raw  string
		want string
	}{
		{raw: "https://youtube.com/watch?v=abc123", want: "https://www.youtube.com/watch?v=abc123"},
		{raw: "https://www.youtube.com/watch?v=abc123", want: "https://www.youtube.com/watch?v=abc123"},
		{raw: "https://m.youtube.com/watch?v=abc123&feature=share", want: "https://www.youtube.com/watch?v=abc123"},
		{raw: "https://youtu.be/abc123", want: "https://www.youtube.com/watch?v=abc123"},
		{raw: "https://WWW.YouTube.com/watch?v=abc123", want: "https://www.youtube.com/watch?v=abc123"},
	}
	for _, test := range valid {
		if got, err := validateYouTubeWatchURL(test.raw); err != nil || got != test.want {
			t.Errorf("expected valid URL %q to normalize to %q, got %q err=%v", test.raw, test.want, got, err)
		}
	}

	invalid := []string{
		"http://youtube.com/watch?v=abc123",
		"https://youtube.example/watch?v=abc123",
		"https://youtube.com.evil.example/watch?v=abc123",
		"https://youtu.be.evil.example/abc123",
		"https://user:password@youtube.com/watch?v=abc123",
		"https://youtube.com@evil.example/watch?v=abc123",
		"https://youtube.com:443/watch?v=abc123",
		"https://youtube.com/watch?v=abc123#fragment",
		"https://youtube.com/watch?v=abc123#",
		"//youtube.com/watch?v=abc123",
		"https://youtube.com./watch?v=abc123",
		"https://youtube.com/channel/abc123",
		"https://youtube.com/watch",
		"https://youtube.com/watch?v=abc",
		"https://youtu.be/abc123/extra",
	}
	for _, raw := range invalid {
		if got, err := validateYouTubeWatchURL(raw); err == nil {
			t.Errorf("expected invalid URL %q, got %q", raw, got)
		}
	}
}

func TestYouTubeLiveNotificationRejectsInvalidURLBeforeRuntimeFetch(t *testing.T) {
	voice := &httpFakeVoice{}
	runtimeFetched := false
	handler := NewServerWithRuntimeConfig(control.ServiceType, jobs.NewManager(voice), TokenVerifier{PlainToken: "expected"}, func(context.Context) (control.RuntimeConfig, error) {
		runtimeFetched = true
		return control.RuntimeConfig{}, nil
	})

	res := performYouTubeNotificationRequest(t, handler, "expected", `{"event_id":"event-01","watch_url":"https://youtube.com.evil.example/watch?v=abc123"}`)
	if res.Code != http.StatusBadRequest || !strings.Contains(res.Body.String(), `"code":"invalid_watch_url"`) {
		t.Fatalf("expected invalid URL error, got %d body=%s", res.Code, res.Body.String())
	}
	if runtimeFetched || len(voice.sentMessages) != 0 {
		t.Fatal("invalid URL must be rejected before runtime lookup or Discord send")
	}
}

func TestYouTubeLiveNotificationRejectsInvalidRuntimeAndLiveState(t *testing.T) {
	tests := []struct {
		name       string
		job        *discord.VoiceJob
		provider   RuntimeConfigProvider
		wantStatus int
		wantCode   string
	}{
		{name: "runtime unavailable", wantStatus: http.StatusServiceUnavailable, wantCode: "runtime_config_unavailable"},
		{name: "runtime fetch failure", provider: func(context.Context) (control.RuntimeConfig, error) {
			return control.RuntimeConfig{}, errors.New("token=runtime-secret")
		}, wantStatus: http.StatusBadGateway, wantCode: "runtime_config_fetch_failed"},
		{name: "not primary", provider: func(context.Context) (control.RuntimeConfig, error) {
			return notificationRuntimeConfig("stream-01", "standby", "text-01"), nil
		}, wantStatus: http.StatusForbidden, wantCode: "stream_not_assigned_to_service"},
		{name: "runtime text channel missing", provider: func(context.Context) (control.RuntimeConfig, error) {
			return notificationRuntimeConfig("stream-01", "primary", ""), nil
		}, wantStatus: http.StatusConflict, wantCode: "text_channel_not_configured"},
		{name: "live job stopped", provider: func(context.Context) (control.RuntimeConfig, error) {
			return notificationRuntimeConfig("stream-01", "primary", "text-01"), nil
		}, wantStatus: http.StatusConflict, wantCode: "live_job_not_active"},
		{name: "live job is another stream", job: &discord.VoiceJob{StreamID: "stream-02", GuildID: "guild-01", VoiceChannelID: "voice-01", TextChannelID: "text-01"}, provider: func(context.Context) (control.RuntimeConfig, error) {
			return notificationRuntimeConfig("stream-01", "primary", "text-01"), nil
		}, wantStatus: http.StatusConflict, wantCode: "live_job_stream_mismatch"},
		{name: "live job channel mismatch", job: &discord.VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01", TextChannelID: "text-old"}, provider: func(context.Context) (control.RuntimeConfig, error) {
			return notificationRuntimeConfig("stream-01", "primary", "text-new"), nil
		}, wantStatus: http.StatusConflict, wantCode: "text_channel_mismatch"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			voice := &httpFakeVoice{}
			manager := jobs.NewManager(voice)
			if tt.job != nil {
				if err := manager.Start(*tt.job); err != nil {
					t.Fatal(err)
				}
			}
			handler := NewServerWithRuntimeConfig(control.ServiceType, manager, TokenVerifier{PlainToken: "expected"}, tt.provider)
			res := performYouTubeNotificationRequest(t, handler, "expected", `{"event_id":"event-01","watch_url":"https://youtu.be/abc123"}`)
			if res.Code != tt.wantStatus || !strings.Contains(res.Body.String(), `"code":"`+tt.wantCode+`"`) {
				t.Fatalf("expected %d %s, got %d body=%s", tt.wantStatus, tt.wantCode, res.Code, res.Body.String())
			}
			if strings.Contains(res.Body.String(), "runtime-secret") {
				t.Fatalf("runtime error leaked a secret: %s", res.Body.String())
			}
			if len(voice.sentMessages) != 0 {
				t.Fatalf("invalid state sent %d Discord messages", len(voice.sentMessages))
			}
		})
	}
}

func TestYouTubeLiveNotificationMapsDiscordFailures(t *testing.T) {
	tests := []struct {
		name           string
		sendErr        error
		wantStatus     int
		wantCode       string
		wantRetryable  bool
		wantRetryAfter string
	}{
		{name: "rate limited", sendErr: &discord.SendMessageError{Code: discord.SendMessageCodeRateLimited, Retryable: true, RetryAfter: 1500 * time.Millisecond}, wantStatus: http.StatusTooManyRequests, wantCode: discord.SendMessageCodeRateLimited, wantRetryable: true, wantRetryAfter: "2"},
		{name: "Discord unavailable", sendErr: &discord.SendMessageError{Code: discord.SendMessageCodeUnavailable, Retryable: true}, wantStatus: http.StatusBadGateway, wantCode: discord.SendMessageCodeUnavailable, wantRetryable: true},
		{name: "missing permission", sendErr: &discord.SendMessageError{Code: discord.SendMessageCodeMissingPermissions}, wantStatus: http.StatusForbidden, wantCode: discord.SendMessageCodeMissingPermissions},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			voice := &httpFakeVoice{sendErr: tt.sendErr}
			manager := jobs.NewManager(voice)
			if err := manager.Start(discord.VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01", TextChannelID: "text-01"}); err != nil {
				t.Fatal(err)
			}
			handler := NewServerWithRuntimeConfig(control.ServiceType, manager, TokenVerifier{PlainToken: "expected"}, func(context.Context) (control.RuntimeConfig, error) {
				return notificationRuntimeConfig("stream-01", "primary", "text-01"), nil
			})
			res := performYouTubeNotificationRequest(t, handler, "expected", `{"event_id":"event-01","watch_url":"https://youtu.be/abc123"}`)
			if res.Code != tt.wantStatus {
				t.Fatalf("expected %d, got %d body=%s", tt.wantStatus, res.Code, res.Body.String())
			}
			var response notificationErrorResponse
			if err := json.Unmarshal(res.Body.Bytes(), &response); err != nil {
				t.Fatal(err)
			}
			if response.Code != tt.wantCode || response.Retryable != tt.wantRetryable {
				t.Fatalf("unexpected error response: %#v", response)
			}
			if res.Header().Get("Retry-After") != tt.wantRetryAfter {
				t.Fatalf("unexpected Retry-After: %q", res.Header().Get("Retry-After"))
			}
		})
	}
}

func performYouTubeNotificationRequest(t *testing.T, handler http.Handler, token, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/streams/stream-01/notifications/youtube-live", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	res := httptest.NewRecorder()
	handler.ServeHTTP(res, req)
	return res
}

func notificationRuntimeConfig(streamID, assignmentRole, textChannelID string) control.RuntimeConfig {
	return control.RuntimeConfig{
		Service: control.RegisteredService{ServiceID: "discord-bot-01"},
		Assignments: []control.StreamServiceAssignment{{
			StreamID:       streamID,
			ServiceID:      "discord-bot-01",
			ServiceType:    control.ServiceType,
			AssignmentRole: assignmentRole,
		}},
		StreamDiscordConfigs: []control.StreamDiscordConfig{{
			StreamID:        streamID,
			AssignmentRole:  "primary",
			DiscordConfigID: "discord-config-01",
			GuildID:         "guild-01",
			VoiceChannelID:  "voice-01",
			TextChannelID:   textChannelID,
		}},
	}
}
