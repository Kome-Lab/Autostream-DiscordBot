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

// PublishError is a safe, bounded classification of a Worker event publish
// failure. It deliberately does not retain the response body, URL, token, or
// request payload so callers can record its metadata without leaking secrets.
type PublishError struct {
	statusCode int
	class      string
	retryable  bool
	cause      error
}

func (e *PublishError) Error() string {
	if e == nil {
		return "worker event publish failed"
	}
	if e.statusCode > 0 {
		return fmt.Sprintf("worker event publish failed with status %d", e.statusCode)
	}
	return "worker event publish request failed"
}

func (e *PublishError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

// ErrorClass returns a low-cardinality, secret-safe failure class.
func (e *PublishError) ErrorClass() string {
	if e == nil || e.class == "" {
		return "unknown"
	}
	return e.class
}

// HTTPStatusCode returns the response status, or zero for transport failures.
func (e *PublishError) HTTPStatusCode() int {
	if e == nil {
		return 0
	}
	return e.statusCode
}

// RetryablePublish reports whether the failure is eligible for a bounded
// retry by the caller.
func (e *PublishError) RetryablePublish() bool {
	return e != nil && e.retryable
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
	return r.ParticipantsChangedContext(context.Background(), job, participants)
}

// ParticipantsChangedContext is the cancellation-aware form used by the job
// manager's bounded latest-state retry queue.
func (r Reporter) ParticipantsChangedContext(ctx context.Context, job discord.VoiceJob, participants []jobs.Participant) error {
	payload := struct {
		Participants  []participantPayload `json:"participants"`
		JobGeneration uint64               `json:"job_generation"`
	}{Participants: make([]participantPayload, 0, len(participants)), JobGeneration: job.JobGeneration}
	for _, participant := range participants {
		payload.Participants = append(payload.Participants, participantPayload{
			UserID:      participant.UserID,
			DisplayName: participant.Username,
			AvatarURL:   participant.AvatarURL,
			IsBot:       participant.IsBot,
			Speaking:    participant.Speaking,
		})
	}
	return r.post(ctx, job, "/streams/"+url.PathEscape(job.StreamID)+"/events/participants", payload)
}

func (r Reporter) ActiveSpeakerChanged(job discord.VoiceJob, userID, displayName string) error {
	return r.ActiveSpeakerStateChanged(job, userID, displayName, true)
}

func (r Reporter) ActiveSpeakerStateChanged(job discord.VoiceJob, userID, displayName string, speaking bool) error {
	return r.ActiveSpeakerStateChangedContext(context.Background(), job, userID, displayName, speaking)
}

// ActiveSpeakerStateChangedContext is the cancellation-aware form used by the
// job manager's bounded latest-state retry queue.
func (r Reporter) ActiveSpeakerStateChangedContext(ctx context.Context, job discord.VoiceJob, userID, displayName string, speaking bool) error {
	payload := map[string]any{"user_id": userID, "display_name": displayName, "speaking": speaking, "job_generation": job.JobGeneration}
	return r.post(ctx, job, "/streams/"+url.PathEscape(job.StreamID)+"/events/active-speaker", payload)
}

func (r Reporter) ChatMessageReceived(job discord.VoiceJob, message jobs.ChatMessage) error {
	return r.ChatMessageReceivedContext(context.Background(), job, message)
}

// ChatMessageReceivedContext is the cancellation-aware form used by the job
// manager's bounded retry queue.
func (r Reporter) ChatMessageReceivedContext(ctx context.Context, job discord.VoiceJob, message jobs.ChatMessage) error {
	payload := map[string]any{
		"type":           "overlay.discord_chat",
		"job_generation": job.JobGeneration,
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
	return r.post(ctx, job, "/streams/"+url.PathEscape(job.StreamID)+"/events/overlay", payload)
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
			return classifyContextError(err)
		}
	}
}

func (r Reporter) postOnce(ctx context.Context, cfg Config, endpoint string, body []byte) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, classifyContextError(err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, joinURL(cfg.URL, endpoint), bytes.NewReader(body))
	if err != nil {
		return false, errors.New("worker event publish request could not be created")
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
			return false, classifyContextError(ctxErr)
		}
		return true, &PublishError{class: "transport", retryable: true, cause: err}
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(res.Body, 4<<10))
	_ = res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		retryable := res.StatusCode == http.StatusRequestTimeout || res.StatusCode == http.StatusConflict || res.StatusCode == http.StatusTooManyRequests || res.StatusCode >= 500
		return retryable, &PublishError{statusCode: res.StatusCode, class: "http_status", retryable: retryable}
	}
	return false, nil
}

func classifyContextError(err error) error {
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return &PublishError{class: "timeout", retryable: true, cause: context.DeadlineExceeded}
	}
	return err
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
