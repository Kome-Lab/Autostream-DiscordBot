package jobs

import (
	"context"
	"crypto/sha256"
	"errors"
	"strings"
	"time"

	"github.com/example/autostream-discord-bot/internal/discord"
)

const (
	youtubeLiveNotificationPrefix = "YouTube配信を開始しました。\n"
	notificationReceiptTTL        = 24 * time.Hour
	maxNotificationReceipts       = 2048
)

var (
	ErrNotificationStreamIDRequired = errors.New("notification stream_id is required")
	ErrNotificationEventIDRequired  = errors.New("notification event_id is required")
	ErrNotificationWatchURLRequired = errors.New("notification watch_url is required")
	ErrNoActiveLiveJob              = errors.New("no active live job")
	ErrLiveJobStreamMismatch        = errors.New("notification stream_id does not match active live job")
	ErrLiveJobTextChannelMissing    = errors.New("active live job text channel is required")
	ErrLiveJobTextChannelMismatch   = errors.New("runtime text channel does not match active live job")
	ErrNotificationEventIDConflict  = errors.New("notification event_id was already used for another payload")
	ErrNotificationReceiptCapacity  = errors.New("notification receipt capacity reached")
)

type NotificationResult struct {
	MessageID   string
	AlreadySent bool
}

type notificationEventKey [sha256.Size]byte

type notificationReceipt struct {
	streamID    string
	payloadHash [sha256.Size]byte
	messageID   string
	err         error
	completedAt time.Time
	done        chan struct{}
}

func (m *Manager) NotifyYouTubeLive(ctx context.Context, streamID, eventID, runtimeTextChannelID, watchURL string) (NotificationResult, error) {
	streamID = strings.TrimSpace(streamID)
	eventID = strings.TrimSpace(eventID)
	runtimeTextChannelID = strings.TrimSpace(runtimeTextChannelID)
	watchURL = strings.TrimSpace(watchURL)
	if streamID == "" {
		return NotificationResult{}, ErrNotificationStreamIDRequired
	}
	if eventID == "" {
		return NotificationResult{}, ErrNotificationEventIDRequired
	}
	if watchURL == "" {
		return NotificationResult{}, ErrNotificationWatchURLRequired
	}
	if ctx == nil {
		ctx = context.Background()
	}

	key := notificationEventKey(sha256.Sum256([]byte(eventID)))
	payloadHash := sha256.Sum256([]byte(streamID + "\x00" + runtimeTextChannelID + "\x00" + watchURL))
	m.mu.Lock()
	if err := m.validateLiveNotificationStateLocked(streamID, runtimeTextChannelID); err != nil {
		m.mu.Unlock()
		return NotificationResult{}, err
	}
	if m.notificationReceipts == nil {
		m.notificationReceipts = map[notificationEventKey]*notificationReceipt{}
	}
	m.pruneNotificationReceiptsLocked(time.Now().UTC())
	if receipt, ok := m.notificationReceipts[key]; ok {
		if receipt.streamID != streamID || receipt.payloadHash != payloadHash {
			m.mu.Unlock()
			return NotificationResult{}, ErrNotificationEventIDConflict
		}
		done := receipt.done
		m.mu.Unlock()
		select {
		case <-done:
			if receipt.err != nil {
				return NotificationResult{}, receipt.err
			}
			return NotificationResult{MessageID: receipt.messageID, AlreadySent: true}, nil
		case <-ctx.Done():
			return NotificationResult{}, &discord.SendMessageError{Code: discord.SendMessageCodeUnavailable, Retryable: true}
		}
	}
	if len(m.notificationReceipts) >= maxNotificationReceipts {
		m.mu.Unlock()
		return NotificationResult{}, ErrNotificationReceiptCapacity
	}

	receipt := &notificationReceipt{streamID: streamID, payloadHash: payloadHash, done: make(chan struct{})}
	m.notificationReceipts[key] = receipt
	client := m.voice
	m.mu.Unlock()

	var sent discord.SentMessage
	var err error
	if client == nil {
		err = &discord.SendMessageError{Code: discord.SendMessageCodeUnavailable, Retryable: true}
	} else {
		sent, err = client.SendMessage(ctx, discord.OutboundMessage{
			ChannelID: runtimeTextChannelID,
			Content:   youtubeLiveNotificationPrefix + watchURL,
		})
		if err == nil && strings.TrimSpace(sent.MessageID) == "" {
			err = &discord.SendMessageError{Code: discord.SendMessageCodeUnavailable, Retryable: true}
		}
	}

	m.mu.Lock()
	receipt.err = err
	if err != nil {
		delete(m.notificationReceipts, key)
	} else {
		receipt.messageID = strings.TrimSpace(sent.MessageID)
		receipt.completedAt = time.Now().UTC()
		m.lastEventAt = receipt.completedAt
	}
	close(receipt.done)
	m.mu.Unlock()

	if err != nil {
		return NotificationResult{}, err
	}
	return NotificationResult{MessageID: receipt.messageID}, nil
}

func (m *Manager) pruneNotificationReceiptsLocked(now time.Time) {
	for key, receipt := range m.notificationReceipts {
		if receipt == nil || (!receipt.completedAt.IsZero() && now.Sub(receipt.completedAt) >= notificationReceiptTTL) {
			delete(m.notificationReceipts, key)
		}
	}
	for len(m.notificationReceipts) >= maxNotificationReceipts {
		var oldestKey notificationEventKey
		var oldestAt time.Time
		found := false
		for key, receipt := range m.notificationReceipts {
			if receipt == nil || receipt.completedAt.IsZero() {
				continue
			}
			if !found || receipt.completedAt.Before(oldestAt) {
				oldestKey = key
				oldestAt = receipt.completedAt
				found = true
			}
		}
		if !found {
			return
		}
		delete(m.notificationReceipts, oldestKey)
	}
}

func (m *Manager) validateLiveNotificationStateLocked(streamID, runtimeTextChannelID string) error {
	if m.current.StreamID == "" {
		return ErrNoActiveLiveJob
	}
	if m.current.StreamID != streamID {
		return ErrLiveJobStreamMismatch
	}
	currentTextChannelID := strings.TrimSpace(m.current.TextChannelID)
	if currentTextChannelID == "" || runtimeTextChannelID == "" {
		return ErrLiveJobTextChannelMissing
	}
	if currentTextChannelID != runtimeTextChannelID {
		return ErrLiveJobTextChannelMismatch
	}
	return nil
}
