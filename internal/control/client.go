package control

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
	"runtime"
	"strings"
	"time"

	"github.com/example/autostream-discord-bot/internal/version"
)

const ServiceType = "discord_bot"
const RuntimeSecretLeaseActiveCode = "runtime_secret_lease_active"

var ErrRuntimeSecretLeaseActive = errors.New(RuntimeSecretLeaseActiveCode)

type ControlPanelError struct {
	Endpoint   string
	StatusCode int
	Code       string
}

func (e ControlPanelError) Error() string {
	if e.Code != "" {
		return fmt.Sprintf("control panel %s failed with status %d code %s", e.Endpoint, e.StatusCode, e.Code)
	}
	return fmt.Sprintf("control panel %s failed with status %d", e.Endpoint, e.StatusCode)
}

func (e ControlPanelError) Is(target error) bool {
	return target == ErrRuntimeSecretLeaseActive && e.Code == RuntimeSecretLeaseActiveCode
}

func (e ControlPanelError) ControlPanelCode() string {
	return e.Code
}

type Config struct {
	ControlPanelURL  string
	Token            string
	ServiceID        string
	ServiceName      string
	ServicePublicURL string
	Version          string
	HeartbeatEvery   time.Duration
	ConfigError      string
}

type Client struct {
	Config Config
	HTTP   *http.Client
}

type Registration struct {
	ServiceID    string         `json:"service_id"`
	ServiceType  string         `json:"service_type"`
	ServiceName  string         `json:"service_name"`
	PublicURL    string         `json:"public_url"`
	Version      string         `json:"version"`
	Capabilities map[string]any `json:"capabilities"`
	Hostname     string         `json:"hostname,omitempty"`
	OS           string         `json:"os,omitempty"`
	Arch         string         `json:"arch,omitempty"`
}

type Heartbeat struct {
	ServiceID       string             `json:"service_id"`
	Status          string             `json:"status"`
	CurrentStreamID string             `json:"current_stream_id,omitempty"`
	Version         string             `json:"version,omitempty"`
	Capabilities    map[string]any     `json:"capabilities,omitempty"`
	Hostname        string             `json:"hostname,omitempty"`
	OS              string             `json:"os,omitempty"`
	Arch            string             `json:"arch,omitempty"`
	Metrics         map[string]float64 `json:"metrics,omitempty"`
}

type RuntimeConfig struct {
	Service              RegisteredService           `json:"service"`
	Assignments          []StreamServiceAssignment   `json:"assignments"`
	Profiles             map[string][]RuntimeProfile `json:"profiles"`
	StreamDiscordConfigs []StreamDiscordConfig       `json:"stream_discord_configs,omitempty"`
}

type RegisteredService struct {
	ServiceID       string         `json:"service_id"`
	ServiceType     string         `json:"service_type"`
	ServiceName     string         `json:"service_name"`
	PublicURL       string         `json:"public_url"`
	Version         string         `json:"version"`
	Status          string         `json:"status"`
	AssignmentRole  string         `json:"assignment_role,omitempty"`
	CurrentStreamID string         `json:"current_stream_id,omitempty"`
	Capabilities    map[string]any `json:"capabilities,omitempty"`
}

type StreamServiceAssignment struct {
	StreamID       string    `json:"stream_id"`
	ServiceID      string    `json:"service_id"`
	ServiceType    string    `json:"service_type"`
	AssignmentRole string    `json:"assignment_role"`
	AssignedAt     time.Time `json:"assigned_at"`
}

type RuntimeProfile struct {
	ID        string         `json:"id"`
	Kind      string         `json:"kind"`
	Name      string         `json:"name"`
	Config    map[string]any `json:"config"`
	CreatedAt time.Time      `json:"created_at"`
	UpdatedAt time.Time      `json:"updated_at"`
}

type StreamDiscordConfig struct {
	StreamID         string `json:"stream_id"`
	AssignmentRole   string `json:"assignment_role"`
	DiscordConfigID  string `json:"discord_config_id"`
	GuildID          string `json:"guild_id"`
	VoiceChannelID   string `json:"voice_channel_id"`
	TextChannelID    string `json:"text_channel_id,omitempty"`
	CaptionAudioURL  string `json:"caption_audio_url,omitempty"`
	AutoStartTrigger string `json:"auto_start_trigger,omitempty"`
}

func (cfg RuntimeConfig) DiscordConfigForStream(streamID string) (StreamDiscordConfig, bool) {
	streamID = strings.TrimSpace(streamID)
	if streamID == "" {
		return StreamDiscordConfig{}, false
	}
	for _, item := range cfg.StreamDiscordConfigs {
		if strings.TrimSpace(item.StreamID) != streamID {
			continue
		}
		if strings.TrimSpace(item.AssignmentRole) != "primary" {
			continue
		}
		if strings.TrimSpace(item.GuildID) == "" || strings.TrimSpace(item.VoiceChannelID) == "" {
			continue
		}
		return item, true
	}
	return StreamDiscordConfig{}, false
}

type RuntimeSecret struct {
	SecretName   string `json:"secret_name"`
	Value        string `json:"value"`
	ExpiresInSec int    `json:"expires_in_sec"`
}

func ConfigFromEnv() Config {
	cfg := Config{
		ControlPanelURL:  os.Getenv("CONTROL_PANEL_URL"),
		Token:            os.Getenv("CONTROL_PANEL_TOKEN"),
		ServiceID:        envDefault("SERVICE_ID", "discord-bot-01"),
		ServiceName:      envDefault("SERVICE_NAME", "Discord BOT"),
		ServicePublicURL: os.Getenv("SERVICE_PUBLIC_URL"),
		Version:          envDefault("SERVICE_VERSION", version.Current()),
		HeartbeatEvery:   envDuration("CONTROL_PANEL_HEARTBEAT_INTERVAL_SEC", 30*time.Second),
	}
	applyNodeConfigFromEnv(&cfg, ServiceType)
	return cfg
}

func (c Config) Validate() error {
	if strings.TrimSpace(c.ConfigError) != "" {
		return errors.New(c.ConfigError)
	}
	if strings.TrimSpace(c.ControlPanelURL) == "" {
		return errors.New("CONTROL_PANEL_URL is required")
	}
	if strings.TrimSpace(c.Token) == "" {
		return errors.New("CONTROL_PANEL_TOKEN is required")
	}
	if strings.TrimSpace(c.ServiceID) == "" {
		return errors.New("SERVICE_ID is required")
	}
	if strings.TrimSpace(c.ServiceName) == "" {
		return errors.New("SERVICE_NAME is required")
	}
	if err := validateHTTPURL(c.ControlPanelURL, "CONTROL_PANEL_URL"); err != nil {
		return err
	}
	if err := validateHTTPURL(c.ServicePublicURL, "SERVICE_PUBLIC_URL"); err != nil {
		return err
	}
	return nil
}

func validateHTTPURL(raw, name string) error {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return errors.New(name + " must be an absolute URL")
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return errors.New(name + " must use http or https")
	}
	if parsed.User != nil {
		return errors.New(name + " must not include credentials")
	}
	if parsed.RawQuery != "" || parsed.Fragment != "" {
		return errors.New(name + " must not include query or fragment")
	}
	if parsed.Scheme == "http" && !isLocalDevHost(parsed.Hostname()) {
		return errors.New(name + " must use https for remote hosts")
	}
	return nil
}

func isLocalDevHost(host string) bool {
	normalized := strings.Trim(strings.ToLower(strings.TrimSpace(host)), "[]")
	return normalized == "localhost" || normalized == "127.0.0.1" || normalized == "host.docker.internal"
}

func serviceCapabilities() map[string]any {
	return map[string]any{
		"discord_gateway":                    true,
		"voice_connect":                      true,
		"participant_events":                 true,
		"active_speaker_state":               true,
		"audio_capture":                      true,
		"audio_capture_runtime_secret":       true,
		"audio_stream_forward":               true,
		"audio_stream_forward_runtime_token": true,
		"audio_forward_retry":                true,
		"chat_overlay_events":                true,
		"caption_audio_forward":              false,
		"health_endpoint":                    true,
		"job_endpoint":                       true,
	}
}

func reportHostname() string {
	hostname, err := os.Hostname()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(hostname)
}

func (c Client) Register(ctx context.Context) error {
	body := Registration{
		ServiceID:    c.Config.ServiceID,
		ServiceType:  ServiceType,
		ServiceName:  c.Config.ServiceName,
		PublicURL:    c.Config.ServicePublicURL,
		Version:      c.Config.Version,
		Capabilities: serviceCapabilities(),
		Hostname:     reportHostname(),
		OS:           runtime.GOOS,
		Arch:         runtime.GOARCH,
	}
	return c.post(ctx, "/services/register", body)
}

func (c Client) Heartbeat(ctx context.Context, status, currentStreamID string) error {
	return c.HeartbeatWithMetrics(ctx, status, currentStreamID, nil)
}

func (c Client) HeartbeatWithMetrics(ctx context.Context, status, currentStreamID string, metrics map[string]float64) error {
	if status == "" {
		status = "online"
	}
	body := Heartbeat{
		ServiceID:       c.Config.ServiceID,
		Status:          status,
		CurrentStreamID: currentStreamID,
		Version:         c.Config.Version,
		Capabilities:    serviceCapabilities(),
		Hostname:        reportHostname(),
		OS:              runtime.GOOS,
		Arch:            runtime.GOARCH,
		Metrics:         mergeFloatMetrics(NodeHostMetrics(), metrics),
	}
	return c.post(ctx, "/services/heartbeat", body)
}

func (c Client) RuntimeConfig(ctx context.Context) (RuntimeConfig, error) {
	endpoint := "/services/runtime-config?service_id=" + url.QueryEscape(c.Config.ServiceID)
	var cfg RuntimeConfig
	if err := c.get(ctx, endpoint, &cfg); err != nil {
		return RuntimeConfig{}, err
	}
	return cfg, nil
}

func (c Client) ResolveRuntimeSecret(ctx context.Context, secretName string) (RuntimeSecret, error) {
	secretName = strings.TrimSpace(secretName)
	if secretName == "" {
		return RuntimeSecret{}, errors.New("secret name is required")
	}
	var out RuntimeSecret
	err := c.postDecode(ctx, "/services/runtime-secrets/resolve", map[string]string{
		"service_id":  c.Config.ServiceID,
		"secret_name": secretName,
	}, &out)
	if err != nil {
		return RuntimeSecret{}, err
	}
	return out, nil
}

func (c Client) StartStream(ctx context.Context, streamID string) error {
	streamID = strings.TrimSpace(streamID)
	if streamID == "" {
		return errors.New("stream_id is required")
	}
	return c.post(ctx, "/services/streams/"+url.PathEscape(streamID)+"/start", map[string]any{})
}

func (c Client) RunHeartbeatLoop(ctx context.Context, currentStreamID func() string, onError func(error)) {
	c.RunHeartbeatLoopWithMetrics(ctx, currentStreamID, nil, onError)
}

func (c Client) RunHeartbeatLoopWithMetrics(ctx context.Context, currentStreamID func() string, metrics func() map[string]float64, onError func(error)) {
	if currentStreamID == nil {
		currentStreamID = func() string { return "" }
	}
	if metrics == nil {
		metrics = func() map[string]float64 { return nil }
	}
	interval := c.Config.HeartbeatEvery
	if interval <= 0 {
		interval = 30 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if err := c.HeartbeatWithMetrics(ctx, "online", currentStreamID(), metrics()); err != nil && onError != nil {
			onError(err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (c Client) get(ctx context.Context, endpoint string, out any) error {
	if err := c.Config.Validate(); err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, joinURL(c.Config.ControlPanelURL, endpoint), nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.Config.Token)
	client := c.HTTP
	if client == nil {
		client = noRedirectClient()
	}
	res, err := client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return controlPanelErrorFromResponse(endpoint, res)
	}
	return json.NewDecoder(res.Body).Decode(out)
}

func (c Client) post(ctx context.Context, endpoint string, payload any) error {
	return c.postDecode(ctx, endpoint, payload, nil)
}

func (c Client) postDecode(ctx context.Context, endpoint string, payload any, out any) error {
	if err := c.Config.Validate(); err != nil {
		return err
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, joinURL(c.Config.ControlPanelURL, endpoint), bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.Config.Token)
	req.Header.Set("Content-Type", "application/json")
	client := c.HTTP
	if client == nil {
		client = noRedirectClient()
	}
	res, err := client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return controlPanelErrorFromResponse(endpoint, res)
	}
	if out != nil {
		return json.NewDecoder(res.Body).Decode(out)
	}
	return nil
}

func controlPanelErrorFromResponse(endpoint string, res *http.Response) error {
	out := ControlPanelError{Endpoint: endpoint, StatusCode: res.StatusCode}
	var body struct {
		Code string `json:"code"`
	}
	if err := json.NewDecoder(io.LimitReader(res.Body, 4096)).Decode(&body); err == nil {
		out.Code = strings.TrimSpace(body.Code)
	}
	return out
}

func noRedirectClient() *http.Client {
	return &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

func joinURL(baseURL, endpoint string) string {
	base := strings.TrimRight(baseURL, "/")
	path := "/" + strings.TrimLeft(endpoint, "/")
	return base + path
}

func envDefault(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func envDuration(key string, fallback time.Duration) time.Duration {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	seconds, err := time.ParseDuration(value + "s")
	if err != nil || seconds <= 0 {
		return fallback
	}
	return seconds
}
