package jobs

import (
	"context"
	"log"
	"strings"
	"time"

	"github.com/example/autostream-discord-bot/internal/discord"
)

type EventReporter interface {
	ParticipantsChanged(job discord.VoiceJob, participants []Participant) error
	ActiveSpeakerChanged(job discord.VoiceJob, userID, displayName string) error
	ChatMessageReceived(job discord.VoiceJob, message ChatMessage) error
}

// The context-aware extensions are optional so existing reporters and focused
// test doubles remain source-compatible. The production Worker reporter uses
// them so Stop/Disconnect can cancel an in-flight HTTP request.
type ParticipantsContextReporter interface {
	ParticipantsChangedContext(ctx context.Context, job discord.VoiceJob, participants []Participant) error
}

type ActiveSpeakerContextReporter interface {
	ActiveSpeakerStateChangedContext(ctx context.Context, job discord.VoiceJob, userID, displayName string, speaking bool) error
}

type ChatContextReporter interface {
	ChatMessageReceivedContext(ctx context.Context, job discord.VoiceJob, message ChatMessage) error
}

// ActiveSpeakerStateReporter is an optional EventReporter extension. It lets
// the worker distinguish a speaker stopping from a new speaker being detected
// while keeping the legacy reporter contract source-compatible.
type ActiveSpeakerStateReporter interface {
	ActiveSpeakerStateChanged(job discord.VoiceJob, userID, displayName string, speaking bool) error
}

type ChatMessage struct {
	MessageID     string    `json:"message_id"`
	UserID        string    `json:"user_id"`
	Username      string    `json:"username,omitempty"`
	AvatarURL     string    `json:"avatar_url,omitempty"`
	IsBot         bool      `json:"is_bot,omitempty"`
	Content       string    `json:"content"`
	TextChannelID string    `json:"text_channel_id"`
	CreatedAt     time.Time `json:"created_at"`
}

func (m *Manager) recordWorkerPublishFailure(eventType, streamID string, err error) {
	m.recordWorkerPublishFailureAttempt(eventType, streamID, 1, err)
}

func (m *Manager) recordWorkerPublishFailureAttempt(eventType, streamID string, retryCount int, err error) {
	eventType = strings.TrimSpace(eventType)
	streamID = strings.TrimSpace(streamID)
	if eventType == "" {
		eventType = "unknown"
	}
	if retryCount < 1 {
		retryCount = 1
	}
	errorClass, httpStatus, retryable := workerPublishMetadata(err)
	now := time.Now().UTC()
	m.mu.Lock()
	m.workerFailures += 1
	if m.workerFailureLogAt == nil {
		m.workerFailureLogAt = map[string]time.Time{}
	}
	logKey := eventType + "\x00" + streamID
	lastLogAt := m.workerFailureLogAt[logKey]
	shouldLog := lastLogAt.IsZero() || m.workerFailureLogInterval <= 0 || now.Sub(lastLogAt) >= m.workerFailureLogInterval
	if shouldLog {
		m.workerFailureLogAt[logKey] = now
	}
	m.mu.Unlock()
	if shouldLog {
		log.Printf("Discord worker event publish failed: event_type=%s stream_id=%s error_class=%s http_status=%d retryable=%t retry_count=%d", eventType, streamID, errorClass, httpStatus, retryable, retryCount)
	}
}

func (m *Manager) ChatMessageReceived(event discord.ChatMessageEvent) {
	content := trimDiscordChatContent(event.Content)
	messageID := trimDiscordChatField(event.MessageID, 128)
	userID := trimDiscordChatField(event.UserID, 128)
	if content == "" || messageID == "" || userID == "" {
		return
	}
	m.mu.Lock()
	if m.eventsPausedLocked() || m.current.StreamID == "" || event.StreamID != m.current.StreamID || event.GuildID != m.current.GuildID || event.TextChannelID != m.current.TextChannelID {
		m.mu.Unlock()
		return
	}
	job := m.current
	reporter := m.reporter
	reconnectGeneration := m.reconnectGeneration
	m.lastEventAt = time.Now().UTC()
	message := ChatMessage{
		MessageID:     messageID,
		UserID:        userID,
		Username:      trimDiscordChatField(event.Username, 100),
		AvatarURL:     trimDiscordChatField(event.AvatarURL, 2048),
		IsBot:         event.IsBot,
		Content:       content,
		TextChannelID: trimDiscordChatField(event.TextChannelID, 128),
		CreatedAt:     event.CreatedAt,
	}
	if message.CreatedAt.IsZero() {
		message.CreatedAt = time.Now().UTC()
	}
	m.mu.Unlock()
	if reporter == nil {
		return
	}
	m.participantReportMu.Lock()
	defer m.participantReportMu.Unlock()
	m.mu.Lock()
	if m.eventsPausedLocked() || !sameWorkerEventJob(m.current, job) || m.reconnectGeneration != reconnectGeneration {
		m.mu.Unlock()
		return
	}
	reporter = m.reporter
	queue := m.workerRetry
	m.mu.Unlock()
	ctx := context.Background()
	if queue != nil {
		ctx = queue.ctx
	}
	if err := publishChatMessage(ctx, reporter, job, message); err != nil {
		m.recordWorkerPublishFailure("overlay.discord_chat", job.StreamID, err)
		m.enqueueWorkerEventRetry(workerEventRetryReport{
			key:                 workerChatRetryKey(job, message),
			eventType:           "overlay.discord_chat",
			job:                 job,
			reconnectGeneration: reconnectGeneration,
			chat:                message,
			err:                 err,
		})
	} else {
		m.supersedeWorkerEventRetry(job, reconnectGeneration, workerChatRetryKey(job, message))
	}
}

func trimDiscordChatContent(content string) string {
	return trimDiscordChatField(content, 1000)
}

func trimDiscordChatField(value string, maxRunes int) string {
	value = strings.TrimSpace(value)
	if maxRunes <= 0 {
		return ""
	}
	runes := []rune(value)
	if len(runes) <= maxRunes {
		return value
	}
	return strings.TrimSpace(string(runes[:maxRunes]))
}
