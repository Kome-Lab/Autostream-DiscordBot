package discord

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
)

func TestDiscordMessageSendDisablesAllAllowedMentions(t *testing.T) {
	payload := newDiscordMessageSend("YouTube Live is now available:\nhttps://www.youtube.com/watch?v=test@everyone")
	encoded, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	var body map[string]any
	if err := json.Unmarshal(encoded, &body); err != nil {
		t.Fatal(err)
	}
	allowedMentions, ok := body["allowed_mentions"].(map[string]any)
	if !ok {
		t.Fatalf("allowed_mentions is missing: %s", encoded)
	}
	parse, ok := allowedMentions["parse"].([]any)
	if !ok || len(parse) != 0 {
		t.Fatalf("allowed mention parsing must be empty: %s", encoded)
	}
	if repliedUser, ok := allowedMentions["replied_user"].(bool); !ok || repliedUser {
		t.Fatalf("reply mentions must be disabled: %s", encoded)
	}
	if _, ok := allowedMentions["users"]; ok {
		t.Fatalf("explicit user mentions must not be present: %s", encoded)
	}
	if _, ok := allowedMentions["roles"]; ok {
		t.Fatalf("explicit role mentions must not be present: %s", encoded)
	}
}

func TestClassifyMessageSendErrorReturnsSafeRetryableErrors(t *testing.T) {
	rateLimit := classifyMessageSendError(&discordgo.RateLimitError{RateLimit: &discordgo.RateLimit{
		TooManyRequests: &discordgo.TooManyRequests{RetryAfter: 1500 * time.Millisecond},
		URL:             "https://discord.com/api/channels/secret/messages",
	}})
	if rateLimit.Code != SendMessageCodeRateLimited || !rateLimit.Retryable || rateLimit.RetryAfter != 1500*time.Millisecond {
		t.Fatalf("unexpected rate limit classification: %#v", rateLimit)
	}
	if strings.Contains(rateLimit.Error(), "secret") {
		t.Fatalf("rate limit error leaked endpoint details: %q", rateLimit.Error())
	}
	incompleteRateLimit := classifyMessageSendError(&discordgo.RateLimitError{})
	if incompleteRateLimit.Code != SendMessageCodeRateLimited || !incompleteRateLimit.Retryable {
		t.Fatalf("unexpected incomplete rate limit classification: %#v", incompleteRateLimit)
	}

	unavailable := classifyMessageSendError(&discordgo.RESTError{
		Response:     &http.Response{StatusCode: http.StatusBadGateway},
		ResponseBody: []byte(`{"message":"Bot secret-token failed"}`),
	})
	if unavailable.Code != SendMessageCodeUnavailable || !unavailable.Retryable {
		t.Fatalf("unexpected upstream failure classification: %#v", unavailable)
	}
	if strings.Contains(unavailable.Error(), "secret-token") {
		t.Fatalf("upstream error leaked response details: %q", unavailable.Error())
	}
}

func TestClassifyMessageSendErrorDistinguishesPermissionFailures(t *testing.T) {
	tests := []struct {
		name     string
		apiCode  int
		wantCode string
	}{
		{name: "missing permissions", apiCode: 50013, wantCode: SendMessageCodeMissingPermissions},
		{name: "missing access", apiCode: 50001, wantCode: SendMessageCodeMissingAccess},
		{name: "unknown channel", apiCode: 10003, wantCode: SendMessageCodeChannelNotFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := classifyMessageSendError(&discordgo.RESTError{
				Response: &http.Response{StatusCode: http.StatusForbidden},
				Message:  &discordgo.APIErrorMessage{Code: tt.apiCode, Message: "sensitive upstream detail"},
			})
			if err.Code != tt.wantCode || err.Retryable {
				t.Fatalf("unexpected classification: %#v", err)
			}
			if strings.Contains(err.Error(), "sensitive") {
				t.Fatalf("classified error leaked upstream detail: %q", err.Error())
			}
		})
	}
}

func TestNoopClientSupportsMessageSend(t *testing.T) {
	client := &NoopClient{}
	sent, err := client.SendMessage(t.Context(), OutboundMessage{ChannelID: "text-01", Content: "message"})
	if err != nil {
		t.Fatal(err)
	}
	if sent.MessageID == "" {
		t.Fatal("noop message send must return a receipt id")
	}
	_, err = client.SendMessage(t.Context(), OutboundMessage{})
	var sendErr *SendMessageError
	if !errors.As(err, &sendErr) || sendErr.Code != SendMessageCodeInvalidRequest {
		t.Fatalf("expected a typed validation error, got %v", err)
	}
}
