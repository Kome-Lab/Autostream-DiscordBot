package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/example/autostream-discord-bot/internal/discord"
	"github.com/example/autostream-discord-bot/internal/jobs"
)

type Config struct {
	URL         string
	Token       string
	Timeout     time.Duration
	RetryDelays []time.Duration
}

type Reporter struct {
	Config Config
	HTTP   *http.Client
}

type participantPayload struct {
	UserID      string `json:"user_id"`
	DisplayName string `json:"display_name,omitempty"`
	AvatarURL   string `json:"avatar_url,omitempty"`
	IsBot       bool   `json:"is_bot,omitempty"`
	Speaking    bool   `json:"speaking"`
}

func ConfigFromEnv() Config {
	return Config{
		Timeout:     envDuration("DISCORD_WORKER_EVENT_TIMEOUT_SEC", envDuration("WORKER_EVENT_TIMEOUT_SEC", 3*time.Second)),
		RetryDelays: defaultRetryDelays(),
	}
}

func (c Config) Enabled() bool {
	return strings.TrimSpace(c.URL) != "" && strings.TrimSpace(c.Token) != ""
}

func (c Config) Validate() error {
	if strings.TrimSpace(c.URL) == "" {
		return errors.New("worker_events_url is required")
	}
	if strings.TrimSpace(c.Token) == "" {
		return errors.New("worker_events_token is required")
	}
	parsed, err := url.Parse(c.URL)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return errors.New("worker_events_url must be an absolute URL")
	}
	if parsed.Scheme != "https" && parsed.Scheme != "http" {
		return errors.New("worker_events_url scheme is not allowed")
	}
	return nil
}

func (r Reporter) ParticipantsChanged(job discord.VoiceJob, participants []jobs.Participant) error {
	payload := struct {
		Participants []participantPayload `json:"participants"`
	}{Participants: make([]participantPayload, 0, len(participants))}
	for _, participant := range participants {
		payload.Participants = append(payload.Participants, participantPayload{
			UserID:      participant.UserID,
			DisplayName: participant.Username,
			AvatarURL:   participant.AvatarURL,
			IsBot:       participant.IsBot,
			Speaking:    participant.Speaking,
		})
	}
	return r.post(context.Background(), job, "/streams/"+url.PathEscape(job.StreamID)+"/events/participants", payload)
}

func (r Reporter) ActiveSpeakerChanged(job discord.VoiceJob, userID, displayName string) error {
	return r.ActiveSpeakerStateChanged(job, userID, displayName, true)
}

func (r Reporter) ActiveSpeakerStateChanged(job discord.VoiceJob, userID, displayName string, speaking bool) error {
	payload := map[string]any{"user_id": userID, "display_name": displayName, "speaking": speaking}
	return r.post(context.Background(), job, "/streams/"+url.PathEscape(job.StreamID)+"/events/active-speaker", payload)
}

func (r Reporter) ChatMessageReceived(job discord.VoiceJob, message jobs.ChatMessage) error {
	payload := map[string]any{
		"type": "overlay.discord_chat",
		"payload": map[string]any{
			"message_id":      message.MessageID,
			"author_id":       message.UserID,
			"user_id":         message.UserID,
			"display_name":    message.Username,
			"avatar_url":      message.AvatarURL,
			"is_bot":          message.IsBot,
			"content":         message.Content,
			"text":            message.Content,
			"text_channel_id": message.TextChannelID,
			"created_at":      message.CreatedAt.UTC().Format(time.RFC3339Nano),
		},
	}
	return r.post(context.Background(), job, "/streams/"+url.PathEscape(job.StreamID)+"/events/overlay", payload)
}

func (r Reporter) post(ctx context.Context, job discord.VoiceJob, endpoint string, payload any) error {
	cfg := r.Config.withJob(job)
	if strings.TrimSpace(cfg.URL) == "" && strings.TrimSpace(cfg.Token) == "" {
		return nil
	}
	if err := cfg.Validate(); err != nil {
		return err
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	retryDelays := cfg.RetryDelays
	if retryDelays == nil {
		retryDelays = defaultRetryDelays()
	}
	publishCtx := ctx
	cancel := func() {}
	if cfg.Timeout > 0 {
		publishCtx, cancel = context.WithTimeout(ctx, cfg.Timeout)
	}
	defer cancel()
	for attempt := 0; ; attempt++ {
		retryable, err := r.postOnce(publishCtx, cfg, endpoint, body)
		if err == nil {
			return nil
		}
		if !retryable || attempt >= len(retryDelays) {
			return err
		}
		if err := waitForRetry(publishCtx, retryDelays[attempt]); err != nil {
			return err
		}
	}
}

func (r Reporter) postOnce(ctx context.Context, cfg Config, endpoint string, body []byte) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, joinURL(cfg.URL, endpoint), bytes.NewReader(body))
	if err != nil {
		return false, err
	}
	req.Header.Set("Authorization", "Bearer "+cfg.Token)
	req.Header.Set("Content-Type", "application/json")
	client := r.HTTP
	if client == nil {
		client = http.DefaultClient
	}
	res, err := client.Do(req)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return false, ctxErr
		}
		return true, errors.New("worker event publish request failed")
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(res.Body, 4<<10))
	_ = res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		retryable := res.StatusCode == http.StatusRequestTimeout || res.StatusCode == http.StatusTooManyRequests || res.StatusCode >= 500
		return retryable, fmt.Errorf("worker event publish failed with status %d", res.StatusCode)
	}
	return false, nil
}

func defaultRetryDelays() []time.Duration {
	return []time.Duration{100 * time.Millisecond, 300 * time.Millisecond}
}

func waitForRetry(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (c Config) withJob(job discord.VoiceJob) Config {
	c.URL = job.WorkerEventsURL
	c.Token = job.WorkerEventsToken
	return c
}

func joinURL(baseURL, endpoint string) string {
	return strings.TrimRight(baseURL, "/") + "/" + strings.TrimLeft(endpoint, "/")
}

func envDuration(key string, fallback time.Duration) time.Duration {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	duration, err := time.ParseDuration(value)
	if err != nil {
		duration, err = time.ParseDuration(value + "s")
	}
	if err != nil || duration <= 0 {
		return fallback
	}
	return duration
}
