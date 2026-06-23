package audioforward

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

type Config struct {
	Token          string
	Timeout        time.Duration
	RetryMax       int
	RetryBaseDelay time.Duration
}

type Client struct {
	Config Config
	HTTP   *http.Client
	Sleep  func(context.Context, time.Duration) error
}

type OpusPacket struct {
	SSRC       uint32
	UserID     string
	Sequence   uint16
	Timestamp  uint32
	ReceivedAt time.Time
	Opus       []byte
}

type packetWire struct {
	SSRC       uint32    `json:"ssrc"`
	UserID     string    `json:"user_id,omitempty"`
	Sequence   uint16    `json:"sequence"`
	Timestamp  uint32    `json:"timestamp"`
	ReceivedAt time.Time `json:"received_at"`
	OpusBase64 string    `json:"opus_base64"`
}

type ingestRequest struct {
	StreamID string       `json:"stream_id"`
	Source   string       `json:"source"`
	Packets  []packetWire `json:"packets"`
}

func ConfigFromEnv() Config {
	return Config{
		Token:          firstNonEmpty(os.Getenv("ENCODER_AUDIO_TOKEN"), os.Getenv("ENCODER_RECORDER_TOKEN")),
		Timeout:        envDuration("ENCODER_AUDIO_TIMEOUT_SEC", 5*time.Second),
		RetryMax:       envInt("ENCODER_AUDIO_RETRY_MAX", 3),
		RetryBaseDelay: envDuration("ENCODER_AUDIO_RETRY_BASE_DELAY_SEC", time.Second),
	}
}

func (c Config) Enabled() bool {
	return strings.TrimSpace(c.Token) != ""
}

func (c Client) ForwardOpus(ctx context.Context, encoderAudioURL, streamID, source, tokenOverride string, packets []OpusPacket) error {
	if strings.TrimSpace(encoderAudioURL) == "" {
		return errors.New("encoder_audio_url is required")
	}
	if strings.TrimSpace(streamID) == "" {
		return errors.New("stream_id is required")
	}
	if strings.TrimSpace(source) == "" {
		source = "discord-bot"
	}
	if len(packets) == 0 {
		return nil
	}
	token := strings.TrimSpace(c.Config.Token)
	if strings.TrimSpace(tokenOverride) != "" {
		token = strings.TrimSpace(tokenOverride)
	}
	if token == "" {
		return errors.New("ENCODER_AUDIO_TOKEN is required")
	}
	endpoint, err := audioEndpoint(encoderAudioURL, streamID)
	if err != nil {
		return err
	}
	wirePackets := make([]packetWire, 0, len(packets))
	for _, packet := range packets {
		if len(packet.Opus) == 0 {
			continue
		}
		receivedAt := packet.ReceivedAt
		if receivedAt.IsZero() {
			receivedAt = time.Now().UTC()
		}
		wirePackets = append(wirePackets, packetWire{
			SSRC:       packet.SSRC,
			UserID:     packet.UserID,
			Sequence:   packet.Sequence,
			Timestamp:  packet.Timestamp,
			ReceivedAt: receivedAt,
			OpusBase64: base64.StdEncoding.EncodeToString(packet.Opus),
		})
	}
	if len(wirePackets) == 0 {
		return nil
	}
	body, err := json.Marshal(ingestRequest{StreamID: streamID, Source: source, Packets: wirePackets})
	if err != nil {
		return err
	}
	client := c.HTTP
	if client == nil {
		client = noRedirectClient()
	}
	attempts := c.Config.RetryMax
	if attempts <= 0 {
		attempts = 1
	}
	var lastErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		reqCtx := ctx
		cancel := func() {}
		if c.Config.Timeout > 0 {
			reqCtx, cancel = context.WithTimeout(ctx, c.Config.Timeout)
		}
		req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, endpoint, bytes.NewReader(body))
		if err != nil {
			cancel()
			return err
		}
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")
		res, err := client.Do(req)
		cancel()
		if err != nil {
			lastErr = err
		} else {
			statusCode := res.StatusCode
			res.Body.Close()
			if statusCode >= 200 && statusCode < 300 {
				return nil
			}
			lastErr = fmt.Errorf("encoder audio ingest failed with status %d", statusCode)
			if !isRetryableStatus(statusCode) {
				return lastErr
			}
		}
		if attempt < attempts {
			if err := c.sleep(ctx, retryDelay(c.Config.RetryBaseDelay, attempt)); err != nil {
				return err
			}
		}
	}
	return lastErr
}

func noRedirectClient() *http.Client {
	return &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

func audioEndpoint(baseURL, streamID string) (string, error) {
	parsed, err := url.Parse(baseURL)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return "", errors.New("encoder_audio_url must be an absolute URL")
	}
	if parsed.Scheme != "https" && parsed.Scheme != "http" {
		return "", errors.New("encoder_audio_url scheme is not allowed")
	}
	if parsed.User != nil {
		return "", errors.New("encoder_audio_url must not include credentials")
	}
	if parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", errors.New("encoder_audio_url must not include query or fragment")
	}
	if parsed.Scheme == "http" && !isLocalDevHost(parsed.Hostname()) {
		return "", errors.New("encoder_audio_url must use https for remote hosts")
	}
	pathPrefix := strings.TrimRight(parsed.EscapedPath(), "/")
	parsed.RawPath = pathPrefix + "/streams/" + url.PathEscape(streamID) + "/audio/opus"
	parsed.Path = strings.TrimRight(parsed.Path, "/") + "/streams/" + streamID + "/audio/opus"
	return parsed.String(), nil
}

func isLocalDevHost(host string) bool {
	normalized := strings.Trim(strings.ToLower(strings.TrimSpace(host)), "[]")
	return normalized == "localhost" || normalized == "127.0.0.1" || normalized == "host.docker.internal"
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
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

func envInt(key string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	var parsed int
	if _, err := fmt.Sscanf(value, "%d", &parsed); err != nil || parsed <= 0 {
		return fallback
	}
	return parsed
}

func isRetryableStatus(status int) bool {
	return status == http.StatusRequestTimeout || status == http.StatusTooManyRequests || status >= 500
}

func retryDelay(base time.Duration, attempt int) time.Duration {
	if base <= 0 {
		return 0
	}
	delay := base
	for i := 1; i < attempt; i++ {
		delay *= 2
	}
	if delay > 30*time.Second {
		return 30 * time.Second
	}
	return delay
}

func (c Client) sleep(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	if c.Sleep != nil {
		return c.Sleep(ctx, delay)
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
