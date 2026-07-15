package jobs

import (
	"errors"
	"testing"
	"time"

	"github.com/example/autostream-discord-bot/internal/discord"
)

func TestNotifyYouTubeLiveSendsFixedMessageAndReturnsDuplicateReceipt(t *testing.T) {
	voice := &fakeVoice{sendMessageID: "message-123"}
	manager := NewManager(voice)
	if err := manager.Start(discord.VoiceJob{
		StreamID:       "stream-01",
		GuildID:        "guild-01",
		VoiceChannelID: "voice-01",
		TextChannelID:  "text-01",
	}); err != nil {
		t.Fatal(err)
	}

	first, err := manager.NotifyYouTubeLive(t.Context(), "stream-01", "event-01", "text-01", "https://www.youtube.com/watch?v=abc123")
	if err != nil {
		t.Fatal(err)
	}
	second, err := manager.NotifyYouTubeLive(t.Context(), "stream-01", "event-01", "text-01", "https://www.youtube.com/watch?v=abc123")
	if err != nil {
		t.Fatal(err)
	}
	if first.MessageID != "message-123" || first.AlreadySent {
		t.Fatalf("unexpected first result: %#v", first)
	}
	if second.MessageID != first.MessageID || !second.AlreadySent {
		t.Fatalf("unexpected duplicate result: %#v", second)
	}

	voice.mu.Lock()
	sentMessages := append([]discord.OutboundMessage(nil), voice.sentMessages...)
	voice.mu.Unlock()
	if len(sentMessages) != 1 {
		t.Fatalf("duplicate event must send once, got %d sends", len(sentMessages))
	}
	if sentMessages[0].ChannelID != "text-01" {
		t.Fatalf("unexpected target channel: %#v", sentMessages[0])
	}
	wantContent := "YouTube配信を開始しました。\nhttps://www.youtube.com/watch?v=abc123"
	if sentMessages[0].Content != wantContent {
		t.Fatalf("notification content must contain only the fixed text and URL: %q", sentMessages[0].Content)
	}
}

func TestNotifyYouTubeLiveCoalescesConcurrentDuplicate(t *testing.T) {
	voice := &fakeVoice{
		sendMessageID: "message-123",
		sendStarted:   make(chan struct{}, 1),
		sendRelease:   make(chan struct{}),
	}
	manager := NewManager(voice)
	if err := manager.Start(discord.VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01", TextChannelID: "text-01"}); err != nil {
		t.Fatal(err)
	}

	type callResult struct {
		result NotificationResult
		err    error
	}
	firstCh := make(chan callResult, 1)
	secondCh := make(chan callResult, 1)
	go func() {
		result, err := manager.NotifyYouTubeLive(t.Context(), "stream-01", "event-01", "text-01", "https://www.youtube.com/watch?v=abc123")
		firstCh <- callResult{result: result, err: err}
	}()
	select {
	case <-voice.sendStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for first Discord send")
	}
	go func() {
		result, err := manager.NotifyYouTubeLive(t.Context(), "stream-01", "event-01", "text-01", "https://www.youtube.com/watch?v=abc123")
		secondCh <- callResult{result: result, err: err}
	}()
	time.Sleep(25 * time.Millisecond)
	voice.mu.Lock()
	sendCount := len(voice.sentMessages)
	voice.mu.Unlock()
	if sendCount != 1 {
		t.Fatalf("concurrent duplicate started %d Discord sends", sendCount)
	}
	close(voice.sendRelease)

	first := <-firstCh
	second := <-secondCh
	if first.err != nil || second.err != nil {
		t.Fatalf("concurrent duplicate failed: first=%v second=%v", first.err, second.err)
	}
	if first.result.AlreadySent || !second.result.AlreadySent || first.result.MessageID != second.result.MessageID {
		t.Fatalf("unexpected concurrent receipts: first=%#v second=%#v", first.result, second.result)
	}
}

func TestNotifyYouTubeLiveFailureCanBeRetried(t *testing.T) {
	voice := &fakeVoice{sendErr: &discord.SendMessageError{Code: discord.SendMessageCodeUnavailable, Retryable: true}}
	manager := NewManager(voice)
	if err := manager.Start(discord.VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01", TextChannelID: "text-01"}); err != nil {
		t.Fatal(err)
	}

	if _, err := manager.NotifyYouTubeLive(t.Context(), "stream-01", "event-01", "text-01", "https://www.youtube.com/watch?v=abc123"); err == nil {
		t.Fatal("expected first send to fail")
	}
	voice.mu.Lock()
	voice.sendErr = nil
	voice.mu.Unlock()
	result, err := manager.NotifyYouTubeLive(t.Context(), "stream-01", "event-01", "text-01", "https://www.youtube.com/watch?v=abc123")
	if err != nil {
		t.Fatal(err)
	}
	if result.AlreadySent {
		t.Fatalf("retry after a failed send must be a fresh send: %#v", result)
	}
	voice.mu.Lock()
	sendCount := len(voice.sentMessages)
	voice.mu.Unlock()
	if sendCount != 2 {
		t.Fatalf("failed receipt must not suppress retry, got %d sends", sendCount)
	}
}

func TestNotifyYouTubeLiveRejectsInvalidLiveJobState(t *testing.T) {
	tests := []struct {
		name           string
		job            *discord.VoiceJob
		requestStream  string
		runtimeChannel string
		wantErr        error
	}{
		{name: "stopped", wantErr: ErrNoActiveLiveJob, requestStream: "stream-01", runtimeChannel: "text-01"},
		{name: "another stream", job: &discord.VoiceJob{StreamID: "stream-02", GuildID: "guild-01", VoiceChannelID: "voice-01", TextChannelID: "text-01"}, requestStream: "stream-01", runtimeChannel: "text-01", wantErr: ErrLiveJobStreamMismatch},
		{name: "empty active channel", job: &discord.VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01"}, requestStream: "stream-01", runtimeChannel: "text-01", wantErr: ErrLiveJobTextChannelMissing},
		{name: "runtime channel mismatch", job: &discord.VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01", TextChannelID: "text-old"}, requestStream: "stream-01", runtimeChannel: "text-new", wantErr: ErrLiveJobTextChannelMismatch},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			voice := &fakeVoice{}
			manager := NewManager(voice)
			if tt.job != nil {
				if err := manager.Start(*tt.job); err != nil {
					t.Fatal(err)
				}
			}
			_, err := manager.NotifyYouTubeLive(t.Context(), tt.requestStream, "event-01", tt.runtimeChannel, "https://www.youtube.com/watch?v=abc123")
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("expected %v, got %v", tt.wantErr, err)
			}
			voice.mu.Lock()
			sendCount := len(voice.sentMessages)
			voice.mu.Unlock()
			if sendCount != 0 {
				t.Fatalf("invalid live state must not send, got %d sends", sendCount)
			}
		})
	}
}

func TestNotifyYouTubeLiveRejectsEventIDReusedForAnotherStream(t *testing.T) {
	voice := &fakeVoice{}
	manager := NewManager(voice)
	if err := manager.Start(discord.VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01", TextChannelID: "text-01"}); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.NotifyYouTubeLive(t.Context(), "stream-01", "event-shared", "text-01", "https://www.youtube.com/watch?v=abc123"); err != nil {
		t.Fatal(err)
	}
	if err := manager.Stop("stream-01"); err != nil {
		t.Fatal(err)
	}
	if err := manager.Start(discord.VoiceJob{StreamID: "stream-02", GuildID: "guild-01", VoiceChannelID: "voice-01", TextChannelID: "text-02"}); err != nil {
		t.Fatal(err)
	}
	_, err := manager.NotifyYouTubeLive(t.Context(), "stream-02", "event-shared", "text-02", "https://www.youtube.com/watch?v=def456")
	if !errors.Is(err, ErrNotificationEventIDConflict) {
		t.Fatalf("expected event id conflict, got %v", err)
	}
}

func TestNotifyYouTubeLiveRejectsEventIDReusedForDifferentURL(t *testing.T) {
	voice := &fakeVoice{sendMessageID: "message-123"}
	manager := NewManager(voice)
	if err := manager.Start(discord.VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01", TextChannelID: "text-01"}); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.NotifyYouTubeLive(t.Context(), "stream-01", "event-shared", "text-01", "https://www.youtube.com/watch?v=abc123"); err != nil {
		t.Fatal(err)
	}
	_, err := manager.NotifyYouTubeLive(t.Context(), "stream-01", "event-shared", "text-01", "https://www.youtube.com/watch?v=def456")
	if !errors.Is(err, ErrNotificationEventIDConflict) {
		t.Fatalf("expected event id conflict, got %v", err)
	}
}

func TestPruneNotificationReceiptsRemovesExpiredEntriesAndBoundsCapacity(t *testing.T) {
	manager := NewManager(&fakeVoice{})
	now := time.Now().UTC()
	expiredKey := notificationEventKey([32]byte{1})
	recentKey := notificationEventKey([32]byte{2})
	manager.notificationReceipts[expiredKey] = &notificationReceipt{completedAt: now.Add(-notificationReceiptTTL), done: closedNotificationReceiptChannel()}
	manager.notificationReceipts[recentKey] = &notificationReceipt{completedAt: now.Add(-time.Minute), done: closedNotificationReceiptChannel()}
	manager.mu.Lock()
	manager.pruneNotificationReceiptsLocked(now)
	_, expiredExists := manager.notificationReceipts[expiredKey]
	_, recentExists := manager.notificationReceipts[recentKey]
	manager.mu.Unlock()
	if expiredExists || !recentExists {
		t.Fatalf("unexpected receipt TTL pruning result: expired=%t recent=%t", expiredExists, recentExists)
	}

	bounded := NewManager(&fakeVoice{})
	for i := 0; i <= maxNotificationReceipts; i++ {
		key := notificationEventKey([32]byte{byte(i >> 8), byte(i), 3})
		bounded.notificationReceipts[key] = &notificationReceipt{completedAt: now.Add(-time.Duration(i) * time.Nanosecond), done: closedNotificationReceiptChannel()}
	}
	bounded.mu.Lock()
	bounded.pruneNotificationReceiptsLocked(now)
	count := len(bounded.notificationReceipts)
	bounded.mu.Unlock()
	if count >= maxNotificationReceipts {
		t.Fatalf("receipt capacity was not bounded: count=%d", count)
	}
}

func closedNotificationReceiptChannel() chan struct{} {
	done := make(chan struct{})
	close(done)
	return done
}
