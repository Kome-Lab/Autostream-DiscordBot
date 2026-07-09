package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/example/autostream-discord-bot/internal/discord"
	"github.com/example/autostream-discord-bot/internal/jobs"
)

type Config struct {
	URL     string
	Token   string
	Timeout time.Duration
}

type Reporter struct {
	Config Config
	HTTP   *http.Client
}

type participantPayload struct {
	UserID      string `json:"user_id"`
	DisplayName string `json:"display_name,omitempty"`
	Speaking    bool   `json:"speaking"`
}

func ConfigFromEnv() Config {
	return Config{
		Timeout: envDuration("DISCORD_WORKER_EVENT_TIMEOUT_SEC", envDuration("WORKER_EVENT_TIMEOUT_SEC", 3*time.Second)),
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
		})
	}
	return r.post(context.Background(), job, "/streams/"+url.PathEscape(job.StreamID)+"/events/participants", payload)
}

func (r Reporter) ActiveSpeakerChanged(job discord.VoiceJob, userID, displayName string) error {
	payload := map[string]string{"user_id": userID, "display_name": displayName}
	return r.post(context.Background(), job, "/streams/"+url.PathEscape(job.StreamID)+"/events/active-speaker", payload)
}

func (r Reporter) ChatMessageReceived(job discord.VoiceJob, message jobs.ChatMessage) error {
	payload := map[string]any{
		"type": "overlay.discord_chat",
		"payload": map[string]any{
			"message_id":      message.MessageID,
			"user_id":         message.UserID,
			"display_name":    message.Username,
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
	reqCtx := ctx
	if cfg.Timeout > 0 {
		var cancel context.CancelFunc
		reqCtx, cancel = context.WithTimeout(ctx, cfg.Timeout)
		defer cancel()
	}
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, joinURL(cfg.URL, endpoint), bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+cfg.Token)
	req.Header.Set("Content-Type", "application/json")
	client := r.HTTP
	if client == nil {
		client = http.DefaultClient
	}
	res, err := client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return fmt.Errorf("worker event publish failed with status %d", res.StatusCode)
	}
	return nil
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
