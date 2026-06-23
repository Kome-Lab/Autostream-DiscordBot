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
		URL:     os.Getenv("WORKER_URL"),
		Token:   os.Getenv("WORKER_TOKEN"),
		Timeout: envDuration("WORKER_EVENT_TIMEOUT_SEC", 3*time.Second),
	}
}

func (c Config) Enabled() bool {
	return strings.TrimSpace(c.URL) != "" && strings.TrimSpace(c.Token) != ""
}

func (c Config) Validate() error {
	if strings.TrimSpace(c.URL) == "" {
		return errors.New("WORKER_URL is required")
	}
	if strings.TrimSpace(c.Token) == "" {
		return errors.New("WORKER_TOKEN is required")
	}
	parsed, err := url.Parse(c.URL)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return errors.New("WORKER_URL must be an absolute URL")
	}
	if parsed.Scheme != "https" && parsed.Scheme != "http" {
		return errors.New("WORKER_URL scheme is not allowed")
	}
	return nil
}

func (r Reporter) ParticipantsChanged(streamID string, participants []jobs.Participant) error {
	payload := struct {
		Participants []participantPayload `json:"participants"`
	}{Participants: make([]participantPayload, 0, len(participants))}
	for _, participant := range participants {
		payload.Participants = append(payload.Participants, participantPayload{
			UserID:      participant.UserID,
			DisplayName: participant.Username,
		})
	}
	return r.post(context.Background(), "/streams/"+url.PathEscape(streamID)+"/events/participants", payload)
}

func (r Reporter) ActiveSpeakerChanged(streamID, userID, displayName string) error {
	payload := map[string]string{"user_id": userID, "display_name": displayName}
	return r.post(context.Background(), "/streams/"+url.PathEscape(streamID)+"/events/active-speaker", payload)
}

func (r Reporter) post(ctx context.Context, endpoint string, payload any) error {
	if err := r.Config.Validate(); err != nil {
		return err
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	reqCtx := ctx
	if r.Config.Timeout > 0 {
		var cancel context.CancelFunc
		reqCtx, cancel = context.WithTimeout(ctx, r.Config.Timeout)
		defer cancel()
	}
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, joinURL(r.Config.URL, endpoint), bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+r.Config.Token)
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

func joinURL(baseURL, endpoint string) string {
	return strings.TrimRight(baseURL, "/") + "/" + strings.TrimLeft(endpoint, "/")
}

func envDuration(key string, fallback time.Duration) time.Duration {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	duration, err := time.ParseDuration(value + "s")
	if err != nil || duration <= 0 {
		return fallback
	}
	return duration
}
