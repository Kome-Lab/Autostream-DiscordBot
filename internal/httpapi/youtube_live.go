package httpapi

import (
	"errors"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/example/autostream-discord-bot/internal/discord"
	"github.com/example/autostream-discord-bot/internal/jobs"
)

const (
	maxNotificationEventIDLength = 256
	maxYouTubeWatchURLLength     = 1900
)

type youtubeLiveNotificationRequest struct {
	EventID  string `json:"event_id"`
	WatchURL string `json:"watch_url"`
}

type youtubeLiveNotificationResponse struct {
	Status      string `json:"status"`
	MessageID   string `json:"message_id"`
	AlreadySent bool   `json:"already_sent"`
}

type notificationErrorResponse struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable"`
}

func (s Server) youtubeLiveNotification(w http.ResponseWriter, r *http.Request) {
	if !s.authorized(r) {
		writeNotificationError(w, http.StatusUnauthorized, "missing_or_invalid_service_token", "A valid service token is required", false)
		return
	}
	streamID := strings.TrimSpace(r.PathValue("id"))
	if streamID == "" {
		writeNotificationError(w, http.StatusBadRequest, "invalid_stream_id", "stream id is required", false)
		return
	}

	var req youtubeLiveNotificationRequest
	if err := decodeJSON(r, &req); err != nil {
		writeNotificationError(w, http.StatusBadRequest, "invalid_json", "request body must be valid JSON", false)
		return
	}
	req.EventID = strings.TrimSpace(req.EventID)
	if req.EventID == "" || len(req.EventID) > maxNotificationEventIDLength {
		writeNotificationError(w, http.StatusBadRequest, "invalid_event_id", "event_id is required and must be at most 256 bytes", false)
		return
	}
	watchURL, err := validateYouTubeWatchURL(req.WatchURL)
	if err != nil {
		writeNotificationError(w, http.StatusBadRequest, "invalid_watch_url", "watch_url must be an approved HTTPS YouTube URL without credentials or a fragment", false)
		return
	}

	if s.runtimeConfig == nil {
		writeNotificationError(w, http.StatusServiceUnavailable, "runtime_config_unavailable", "runtime config is unavailable", true)
		return
	}
	cfg, err := s.runtimeConfig(r.Context())
	if err != nil {
		writeNotificationError(w, http.StatusBadGateway, "runtime_config_fetch_failed", "runtime config fetch failed", true)
		return
	}
	if !isPrimaryDiscordAssignment(cfg, streamID) {
		writeNotificationError(w, http.StatusForbidden, "stream_not_assigned_to_service", "stream is not assigned to this Discord bot as primary", false)
		return
	}
	streamConfig, ok := cfg.DiscordConfigForStream(streamID)
	if !ok {
		writeNotificationError(w, http.StatusConflict, "discord_config_not_found", "primary Discord config is not available for the stream", false)
		return
	}
	textChannelID := strings.TrimSpace(streamConfig.TextChannelID)
	if textChannelID == "" {
		writeNotificationError(w, http.StatusConflict, "text_channel_not_configured", "primary Discord config has no text channel", false)
		return
	}

	result, err := s.manager.NotifyYouTubeLive(r.Context(), streamID, req.EventID, textChannelID, watchURL)
	if err != nil {
		writeYouTubeLiveNotificationError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, youtubeLiveNotificationResponse{
		Status:      "sent",
		MessageID:   result.MessageID,
		AlreadySent: result.AlreadySent,
	})
}

func validateYouTubeWatchURL(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" || len(raw) > maxYouTubeWatchURLLength || strings.Contains(raw, "#") {
		return "", errors.New("invalid YouTube watch URL")
	}
	parsed, err := url.Parse(raw)
	if err != nil || !strings.EqualFold(parsed.Scheme, "https") || parsed.Host == "" || parsed.User != nil || parsed.Fragment != "" || parsed.Port() != "" {
		return "", errors.New("invalid YouTube watch URL")
	}
	videoID := ""
	switch strings.ToLower(parsed.Hostname()) {
	case "youtube.com", "www.youtube.com", "m.youtube.com":
		if parsed.Path != "/watch" {
			return "", errors.New("invalid YouTube watch URL")
		}
		videoID = parsed.Query().Get("v")
	case "youtu.be":
		videoID = strings.Trim(parsed.Path, "/")
		if strings.Contains(videoID, "/") {
			return "", errors.New("invalid YouTube watch URL")
		}
	default:
		return "", errors.New("invalid YouTube watch URL")
	}
	if !validYouTubeVideoID(videoID) {
		return "", errors.New("invalid YouTube watch URL")
	}
	return "https://www.youtube.com/watch?" + url.Values{"v": []string{videoID}}.Encode(), nil
}

func validYouTubeVideoID(value string) bool {
	value = strings.TrimSpace(value)
	if len(value) < 6 || len(value) > 32 {
		return false
	}
	for _, char := range value {
		if char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' || char == '-' || char == '_' {
			continue
		}
		return false
	}
	return true
}

func writeYouTubeLiveNotificationError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, jobs.ErrNoActiveLiveJob):
		writeNotificationError(w, http.StatusConflict, "live_job_not_active", "live job is not active", false)
	case errors.Is(err, jobs.ErrLiveJobStreamMismatch):
		writeNotificationError(w, http.StatusConflict, "live_job_stream_mismatch", "active live job belongs to another stream", false)
	case errors.Is(err, jobs.ErrLiveJobTextChannelMissing):
		writeNotificationError(w, http.StatusConflict, "text_channel_not_configured", "active live job has no text channel", false)
	case errors.Is(err, jobs.ErrLiveJobTextChannelMismatch):
		writeNotificationError(w, http.StatusConflict, "text_channel_mismatch", "runtime text channel does not match the active live job", false)
	case errors.Is(err, jobs.ErrNotificationEventIDConflict):
		writeNotificationError(w, http.StatusConflict, "event_id_conflict", "event_id was already used for another notification payload", false)
	case errors.Is(err, jobs.ErrNotificationReceiptCapacity):
		writeNotificationError(w, http.StatusServiceUnavailable, "notification_capacity_reached", "notification processing capacity was reached", true)
	default:
		var sendErr *discord.SendMessageError
		if errors.As(err, &sendErr) {
			writeDiscordSendError(w, sendErr)
			return
		}
		writeNotificationError(w, http.StatusBadGateway, discord.SendMessageCodeUnavailable, "Discord is temporarily unavailable", true)
	}
}

func writeDiscordSendError(w http.ResponseWriter, err *discord.SendMessageError) {
	status := http.StatusBadGateway
	switch err.Code {
	case discord.SendMessageCodeRateLimited:
		status = http.StatusTooManyRequests
		retryAfterSeconds := int(math.Ceil(err.RetryAfter.Seconds()))
		if retryAfterSeconds < 1 {
			retryAfterSeconds = 1
		}
		w.Header().Set("Retry-After", strconv.Itoa(retryAfterSeconds))
	case discord.SendMessageCodeMissingPermissions, discord.SendMessageCodeMissingAccess:
		status = http.StatusForbidden
	case discord.SendMessageCodeChannelNotFound:
		status = http.StatusUnprocessableEntity
	case discord.SendMessageCodeInvalidRequest:
		status = http.StatusInternalServerError
	}
	writeNotificationError(w, status, err.Code, err.Error(), err.Retryable)
}

func writeNotificationError(w http.ResponseWriter, status int, code, message string, retryable bool) {
	writeJSON(w, status, notificationErrorResponse{Code: code, Message: message, Retryable: retryable})
}
