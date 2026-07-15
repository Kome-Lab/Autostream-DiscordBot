package httpapi

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/example/autostream-discord-bot/internal/control"
	"github.com/example/autostream-discord-bot/internal/discord"
	"github.com/example/autostream-discord-bot/internal/jobs"
)

type Status struct {
	ServiceType string      `json:"service_type"`
	ServiceID   string      `json:"service_id"`
	Status      string      `json:"status"`
	CheckedAt   time.Time   `json:"checked_at"`
	Job         jobs.Status `json:"job"`
}

type Server struct {
	serviceType   string
	manager       *jobs.Manager
	verifier      TokenVerifier
	runtimeConfig RuntimeConfigProvider
}

type RuntimeConfigProvider func(ctx context.Context) (control.RuntimeConfig, error)

type TokenVerifier struct {
	PlainToken string
	SHA256Hex  string
}

var errStreamNotAssignedToService = errors.New("stream is not assigned to this discord bot as primary")

func TokenVerifierFromEnv() TokenVerifier {
	verifier := TokenVerifier{
		PlainToken: os.Getenv("SERVICE_CONTROL_TOKEN"),
		SHA256Hex:  os.Getenv("SERVICE_CONTROL_TOKEN_SHA256"),
	}
	if verifier.PlainToken == "" && verifier.SHA256Hex == "" {
		if token := control.NodeRuntimeTokenFromEnv(); token != "" {
			sum := sha256.Sum256([]byte(token))
			verifier.SHA256Hex = hex.EncodeToString(sum[:])
		}
	}
	return verifier
}

func (v TokenVerifier) Verify(header string) bool {
	if !strings.HasPrefix(header, "Bearer ") {
		return false
	}
	token := strings.TrimSpace(strings.TrimPrefix(header, "Bearer "))
	if token == "" {
		return false
	}
	if v.SHA256Hex != "" {
		sum := sha256.Sum256([]byte(token))
		got := hex.EncodeToString(sum[:])
		return subtle.ConstantTimeCompare([]byte(got), []byte(strings.ToLower(v.SHA256Hex))) == 1
	}
	if v.PlainToken != "" {
		return subtle.ConstantTimeCompare([]byte(token), []byte(v.PlainToken)) == 1
	}
	if runtimeToken := control.NodeRuntimeTokenFromEnv(); runtimeToken != "" {
		return subtle.ConstantTimeCompare([]byte(token), []byte(runtimeToken)) == 1
	}
	if !allowControlPanelTokenFallback() {
		return false
	}
	controlPanelToken := os.Getenv("CONTROL_PANEL_TOKEN")
	if controlPanelToken == "" {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(token), []byte(controlPanelToken)) == 1
}

func allowControlPanelTokenFallback() bool {
	if envBool("AUTOSTREAM_REQUIRE_CONTROL_PANEL_RUNTIME_CONFIG", false) {
		return false
	}
	return !strings.EqualFold(strings.TrimSpace(os.Getenv("AUTOSTREAM_ENV")), "production")
}

func envBool(key string, fallback bool) bool {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	switch strings.ToLower(raw) {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return fallback
	}
}

func NewServer(serviceType string, manager *jobs.Manager, verifier TokenVerifier) http.Handler {
	return NewServerWithRuntimeConfig(serviceType, manager, verifier, nil)
}

func NewServerWithRuntimeConfig(serviceType string, manager *jobs.Manager, verifier TokenVerifier, runtimeConfig RuntimeConfigProvider) http.Handler {
	if manager == nil {
		manager = jobs.NewManager(&discord.NoopClient{})
	}
	server := Server{serviceType: serviceType, manager: manager, verifier: verifier, runtimeConfig: runtimeConfig}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", server.health)
	mux.HandleFunc("GET /status", server.status)
	mux.HandleFunc("POST /heartbeat", server.heartbeat)
	mux.HandleFunc("POST /jobs/start", server.startJob)
	mux.HandleFunc("POST /jobs/{id}/stop", server.stopJob)
	mux.HandleFunc("GET /streams/{id}/participants", server.participants)
	mux.HandleFunc("POST /streams/{id}/active-speaker", server.activeSpeaker)
	mux.HandleFunc("POST /streams/{id}/notifications/youtube-live", server.youtubeLiveNotification)
	return securityHeaders(mux)
}

func (s Server) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s Server) status(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, Status{ServiceType: s.serviceType, ServiceID: os.Getenv("SERVICE_ID"), Status: "ready", CheckedAt: time.Now().UTC(), Job: publicJobStatus(s.manager.Status())})
}

func (s Server) heartbeat(w http.ResponseWriter, r *http.Request) {
	if !s.authorized(r) {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"code": "missing_or_invalid_service_token"})
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"status": "accepted"})
}

func (s Server) startJob(w http.ResponseWriter, r *http.Request) {
	if !s.authorized(r) {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"code": "missing_or_invalid_service_token"})
		return
	}
	var req discord.VoiceJob
	if err := decodeJSON(r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"code": "invalid_json"})
		return
	}
	if err := s.applyRuntimeDiscordConfig(r.Context(), &req); err != nil {
		if errors.Is(err, errStreamNotAssignedToService) {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusBadGateway, map[string]string{"code": "runtime_config_fetch_failed"})
		return
	}
	if err := s.manager.Start(req); err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, publicJobStatus(s.manager.Status()))
}

func (s Server) applyRuntimeDiscordConfig(ctx context.Context, job *discord.VoiceJob) error {
	if job == nil || s.runtimeConfig == nil || strings.TrimSpace(job.StreamID) == "" {
		return nil
	}
	cfg, err := s.runtimeConfig(ctx)
	if err != nil {
		return err
	}
	if !isPrimaryDiscordAssignment(cfg, job.StreamID) {
		return errStreamNotAssignedToService
	}
	streamConfig, ok := cfg.DiscordConfigForStream(job.StreamID)
	if !ok {
		return nil
	}
	if strings.TrimSpace(job.GuildID) == "" {
		job.GuildID = streamConfig.GuildID
	}
	if strings.TrimSpace(job.VoiceChannelID) == "" {
		job.VoiceChannelID = streamConfig.VoiceChannelID
	}
	if strings.TrimSpace(job.TextChannelID) == "" {
		job.TextChannelID = streamConfig.TextChannelID
	}
	return nil
}

func isPrimaryDiscordAssignment(cfg control.RuntimeConfig, streamID string) bool {
	streamID = strings.TrimSpace(streamID)
	serviceID := strings.TrimSpace(cfg.Service.ServiceID)
	if streamID == "" || serviceID == "" {
		return false
	}
	for _, assignment := range cfg.Assignments {
		if strings.TrimSpace(assignment.StreamID) != streamID {
			continue
		}
		if strings.TrimSpace(assignment.ServiceID) != serviceID {
			continue
		}
		if strings.TrimSpace(assignment.ServiceType) != control.ServiceType {
			continue
		}
		if strings.TrimSpace(assignment.AssignmentRole) != "primary" {
			continue
		}
		return true
	}
	return false
}

func (s Server) stopJob(w http.ResponseWriter, r *http.Request) {
	if !s.authorized(r) {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"code": "missing_or_invalid_service_token"})
		return
	}
	if err := s.manager.Stop(r.PathValue("id")); err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"status": "stopped"})
}

func (s Server) participants(w http.ResponseWriter, r *http.Request) {
	if !s.authorized(r) {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"code": "missing_or_invalid_service_token"})
		return
	}
	participants, err := s.manager.Participants(r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"participants": participants})
}

func (s Server) activeSpeaker(w http.ResponseWriter, r *http.Request) {
	if !s.authorized(r) {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"code": "missing_or_invalid_service_token"})
		return
	}
	var req struct {
		UserID string `json:"user_id"`
	}
	if err := decodeJSON(r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"code": "invalid_json"})
		return
	}
	if err := s.manager.SetActiveSpeaker(r.PathValue("id"), req.UserID); err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, publicJobStatus(s.manager.Status()))
}

func (s Server) authorized(r *http.Request) bool {
	return s.verifier.Verify(r.Header.Get("Authorization"))
}

func securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("Referrer-Policy", "no-referrer")
		next.ServeHTTP(w, r)
	})
}

func decodeJSON(r *http.Request, value any) error {
	defer r.Body.Close()
	decoder := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	return decoder.Decode(value)
}

func writeError(w http.ResponseWriter, err error) {
	code := "request_failed"
	status := http.StatusConflict
	if errors.Is(err, http.ErrBodyReadAfterClose) {
		status = http.StatusBadRequest
	}
	if strings.Contains(err.Error(), "required") {
		status = http.StatusBadRequest
		code = "validation_failed"
	}
	if strings.Contains(err.Error(), "does not match") || strings.Contains(err.Error(), "already active") || strings.Contains(err.Error(), "no active") {
		status = http.StatusConflict
		code = "invalid_stream_state"
	}
	if strings.Contains(err.Error(), "not assigned") {
		status = http.StatusForbidden
		code = "stream_not_assigned_to_service"
	}
	writeJSON(w, status, map[string]string{"code": code, "message": err.Error()})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func publicJobStatus(status jobs.Status) jobs.Status {
	status.Discord.CurrentGuildID = ""
	status.Discord.CurrentVoiceID = ""
	if status.CurrentJob == nil {
		return status
	}
	status.CurrentJob = &discord.VoiceJob{StreamID: status.CurrentJob.StreamID}
	return status
}
