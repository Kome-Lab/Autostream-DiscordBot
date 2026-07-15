package discord

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/bwmarrin/discordgo"
)

const (
	SendMessageCodeInvalidRequest       = "discord_message_invalid"
	SendMessageCodeRateLimited          = "discord_rate_limited"
	SendMessageCodeUnavailable          = "discord_unavailable"
	SendMessageCodeMissingPermissions   = "discord_missing_permissions"
	SendMessageCodeMissingAccess        = "discord_missing_access"
	SendMessageCodeAuthenticationFailed = "discord_authentication_failed"
	SendMessageCodeChannelNotFound      = "discord_channel_not_found"
	SendMessageCodeRequestRejected      = "discord_request_rejected"
)

type OutboundMessage struct {
	ChannelID string
	Content   string
}

type SentMessage struct {
	MessageID string
}

type SendMessageError struct {
	Code       string
	Retryable  bool
	RetryAfter time.Duration
}

func (e *SendMessageError) Error() string {
	if e == nil {
		return "Discord message send failed"
	}
	switch e.Code {
	case SendMessageCodeInvalidRequest:
		return "Discord message request is invalid"
	case SendMessageCodeRateLimited:
		return "Discord rate limit reached"
	case SendMessageCodeUnavailable:
		return "Discord is temporarily unavailable"
	case SendMessageCodeMissingPermissions:
		return "Discord bot lacks permission to post in the text channel"
	case SendMessageCodeMissingAccess:
		return "Discord bot cannot access the text channel"
	case SendMessageCodeAuthenticationFailed:
		return "Discord bot authentication failed"
	case SendMessageCodeChannelNotFound:
		return "Discord text channel was not found"
	case SendMessageCodeRequestRejected:
		return "Discord rejected the message request"
	default:
		return "Discord message send failed"
	}
}

func (c *RealClient) SendMessage(ctx context.Context, message OutboundMessage) (SentMessage, error) {
	message.ChannelID = strings.TrimSpace(message.ChannelID)
	if message.ChannelID == "" || strings.TrimSpace(message.Content) == "" {
		return SentMessage{}, &SendMessageError{Code: SendMessageCodeInvalidRequest}
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if c.session == nil {
		return SentMessage{}, &SendMessageError{Code: SendMessageCodeUnavailable, Retryable: true}
	}
	sent, err := c.session.ChannelMessageSendComplex(
		message.ChannelID,
		newDiscordMessageSend(message.Content),
		discordgo.WithContext(ctx),
		discordgo.WithRetryOnRatelimit(false),
		discordgo.WithRestRetries(0),
	)
	if err != nil {
		c.setLastError("discord message send failed")
		return SentMessage{}, classifyMessageSendError(err)
	}
	if sent == nil || strings.TrimSpace(sent.ID) == "" {
		c.setLastError("discord message response was invalid")
		return SentMessage{}, &SendMessageError{Code: SendMessageCodeUnavailable, Retryable: true}
	}
	return SentMessage{MessageID: sent.ID}, nil
}

func (c *NoopClient) SendMessage(ctx context.Context, message OutboundMessage) (SentMessage, error) {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return SentMessage{}, &SendMessageError{Code: SendMessageCodeUnavailable, Retryable: true}
		}
	}
	if strings.TrimSpace(message.ChannelID) == "" || strings.TrimSpace(message.Content) == "" {
		return SentMessage{}, &SendMessageError{Code: SendMessageCodeInvalidRequest}
	}
	return SentMessage{MessageID: "noop-message"}, nil
}

func newDiscordMessageSend(content string) *discordgo.MessageSend {
	return &discordgo.MessageSend{
		Content: content,
		AllowedMentions: &discordgo.MessageAllowedMentions{
			Parse:       []discordgo.AllowedMentionType{},
			RepliedUser: false,
		},
	}
}

func classifyMessageSendError(err error) *SendMessageError {
	var rateLimitErr *discordgo.RateLimitError
	if errors.As(err, &rateLimitErr) {
		retryAfter := time.Duration(0)
		if rateLimitErr.RateLimit != nil && rateLimitErr.TooManyRequests != nil {
			retryAfter = rateLimitErr.RetryAfter
		}
		return &SendMessageError{Code: SendMessageCodeRateLimited, Retryable: true, RetryAfter: retryAfter}
	}

	var restErr *discordgo.RESTError
	if errors.As(err, &restErr) {
		status := 0
		if restErr.Response != nil {
			status = restErr.Response.StatusCode
		}
		apiCode := 0
		if restErr.Message != nil {
			apiCode = restErr.Message.Code
		}
		switch apiCode {
		case 50013:
			return &SendMessageError{Code: SendMessageCodeMissingPermissions}
		case 50001:
			return &SendMessageError{Code: SendMessageCodeMissingAccess}
		case 10003:
			return &SendMessageError{Code: SendMessageCodeChannelNotFound}
		}
		switch {
		case status == http.StatusTooManyRequests:
			return &SendMessageError{Code: SendMessageCodeRateLimited, Retryable: true, RetryAfter: retryAfterFromRESTError(restErr)}
		case status >= 500:
			return &SendMessageError{Code: SendMessageCodeUnavailable, Retryable: true}
		case status == http.StatusForbidden:
			return &SendMessageError{Code: SendMessageCodeMissingPermissions}
		case status == http.StatusUnauthorized:
			return &SendMessageError{Code: SendMessageCodeAuthenticationFailed}
		case status == http.StatusNotFound:
			return &SendMessageError{Code: SendMessageCodeChannelNotFound}
		default:
			return &SendMessageError{Code: SendMessageCodeRequestRejected}
		}
	}

	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return &SendMessageError{Code: SendMessageCodeUnavailable, Retryable: true}
	}
	return &SendMessageError{Code: SendMessageCodeUnavailable, Retryable: true}
}

func retryAfterFromRESTError(err *discordgo.RESTError) time.Duration {
	if err == nil || err.Response == nil {
		return 0
	}
	raw := strings.TrimSpace(err.Response.Header.Get("Retry-After"))
	seconds, parseErr := strconv.ParseFloat(raw, 64)
	if parseErr != nil || seconds <= 0 {
		return 0
	}
	return time.Duration(seconds * float64(time.Second))
}
