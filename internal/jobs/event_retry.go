package jobs

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/example/autostream-discord-bot/internal/discord"
)

const (
	maxWorkerEventRetryPending  = 256
	maxWorkerEventRetryAttempts = 4
	workerEventRetryBase        = 250 * time.Millisecond
	workerEventRetryMax         = 3 * time.Second
)

type workerEventRetryReport struct {
	key                 string
	eventType           string
	job                 discord.VoiceJob
	reconnectGeneration int64
	participants        []Participant
	stateRevision       uint64
	speakerUserID       string
	speakerDisplayName  string
	speaking            bool
	chat                ChatMessage
	sequence            uint64
	attempts            int
	nextTryAt           time.Time
	queuedAt            time.Time
	err                 error
}

// workerEventRetryQueue keeps one bounded, cancellable retry loop for the
// current Discord job. Participants and active-speaker state are coalesced to
// their latest value; chat messages are keyed by message ID so distinct
// messages are retained until the bounded queue is full.
type workerEventRetryQueue struct {
	ctx    context.Context
	cancel context.CancelFunc
	wake   chan struct{}

	mu       sync.Mutex
	pending  map[string]workerEventRetryReport
	latest   map[string]uint64
	sequence uint64
	stopped  bool
	wg       sync.WaitGroup
}

func newWorkerEventRetryQueue() *workerEventRetryQueue {
	ctx, cancel := context.WithCancel(context.Background())
	return &workerEventRetryQueue{
		ctx:     ctx,
		cancel:  cancel,
		wake:    make(chan struct{}, 1),
		pending: map[string]workerEventRetryReport{},
		latest:  map[string]uint64{},
	}
}

func (q *workerEventRetryQueue) start(manager *Manager) {
	q.wg.Add(1)
	go q.loop(manager)
}

func (q *workerEventRetryQueue) stopAndWait() {
	q.mu.Lock()
	if !q.stopped {
		q.stopped = true
		q.pending = map[string]workerEventRetryReport{}
		q.latest = map[string]uint64{}
	}
	q.mu.Unlock()
	q.cancel()
	q.signal()
	q.wg.Wait()
}

func (q *workerEventRetryQueue) enqueue(report workerEventRetryReport) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.stopped {
		return false
	}
	if _, exists := q.pending[report.key]; !exists && len(q.pending) >= maxWorkerEventRetryPending {
		if !q.dropOldestChatLocked() {
			return false
		}
	}
	q.sequence++
	report.sequence = q.sequence
	report.attempts = 0
	report.queuedAt = time.Now().UTC()
	report.nextTryAt = report.queuedAt.Add(workerEventRetryBase)
	q.latest[report.key] = report.sequence
	q.pending[report.key] = report
	q.signalLocked()
	return true
}

func (q *workerEventRetryQueue) dropOldestChatLocked() bool {
	oldestKey := ""
	var oldest time.Time
	for key, report := range q.pending {
		if report.eventType != "overlay.discord_chat" {
			continue
		}
		if oldestKey == "" || report.queuedAt.Before(oldest) {
			oldestKey = key
			oldest = report.queuedAt
		}
	}
	if oldestKey == "" {
		return false
	}
	delete(q.pending, oldestKey)
	delete(q.latest, oldestKey)
	return true
}

func (q *workerEventRetryQueue) next() (workerEventRetryReport, bool, time.Duration) {
	now := time.Now().UTC()
	q.mu.Lock()
	defer q.mu.Unlock()
	var selected workerEventRetryReport
	var next time.Time
	for _, report := range q.pending {
		if report.nextTryAt.IsZero() || !report.nextTryAt.After(now) {
			if selected.key == "" || report.queuedAt.Before(selected.queuedAt) {
				selected = report
			}
			continue
		}
		if next.IsZero() || report.nextTryAt.Before(next) {
			next = report.nextTryAt
		}
	}
	if selected.key != "" {
		delete(q.pending, selected.key)
		return selected, true, 0
	}
	if !next.IsZero() {
		return workerEventRetryReport{}, false, time.Until(next)
	}
	return workerEventRetryReport{}, false, 0
}

func (q *workerEventRetryQueue) isLatest(report workerEventRetryReport) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return !q.stopped && q.latest[report.key] == report.sequence
}

func (q *workerEventRetryQueue) markDelivered(report workerEventRetryReport) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.latest[report.key] != report.sequence {
		return
	}
	delete(q.pending, report.key)
	delete(q.latest, report.key)
}

func (q *workerEventRetryQueue) markDropped(report workerEventRetryReport) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.latest[report.key] == report.sequence {
		delete(q.pending, report.key)
		delete(q.latest, report.key)
	}
}

func (q *workerEventRetryQueue) requeue(report workerEventRetryReport) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.stopped || q.latest[report.key] != report.sequence {
		return
	}
	if _, newer := q.pending[report.key]; newer {
		return
	}
	if report.attempts >= maxWorkerEventRetryAttempts {
		delete(q.latest, report.key)
		return
	}
	delay := workerEventRetryBase
	for attempt := 1; attempt < report.attempts; attempt++ {
		delay *= 2
		if delay >= workerEventRetryMax {
			delay = workerEventRetryMax
			break
		}
	}
	report.nextTryAt = time.Now().UTC().Add(delay)
	report.queuedAt = time.Now().UTC()
	q.pending[report.key] = report
	q.signalLocked()
}

func (q *workerEventRetryQueue) signal() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.signalLocked()
}

func (q *workerEventRetryQueue) signalLocked() {
	select {
	case q.wake <- struct{}{}:
	default:
	}
}

func (q *workerEventRetryQueue) loop(manager *Manager) {
	defer q.wg.Done()
	for {
		report, ready, wait := q.next()
		if ready {
			manager.deliverWorkerEventRetry(q, report)
			continue
		}
		if wait > 0 {
			timer := time.NewTimer(wait)
			select {
			case <-q.ctx.Done():
				timer.Stop()
				return
			case <-q.wake:
				timer.Stop()
			case <-timer.C:
			}
			continue
		}
		select {
		case <-q.ctx.Done():
			return
		case <-q.wake:
		}
	}
}

type workerPublishErrorClassification interface {
	ErrorClass() string
	HTTPStatusCode() int
	RetryablePublish() bool
}

func workerPublishMetadata(err error) (class string, status int, retryable bool) {
	class = "unknown"
	if err == nil {
		return class, 0, false
	}
	if errors.Is(err, context.Canceled) {
		return "canceled", 0, false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout", 0, true
	}
	var classified workerPublishErrorClassification
	if errors.As(err, &classified) {
		class = safeWorkerErrorClass(classified.ErrorClass())
		status = classified.HTTPStatusCode()
		if status < 100 || status > 599 {
			status = 0
		}
		return class, status, classified.RetryablePublish()
	}
	return class, 0, false
}

func safeWorkerErrorClass(class string) string {
	switch strings.TrimSpace(class) {
	case "http_status", "transport", "timeout", "canceled", "configuration", "unknown":
		return strings.TrimSpace(class)
	default:
		return "unknown"
	}
}

func shouldRetryWorkerPublish(err error) bool {
	_, _, retryable := workerPublishMetadata(err)
	return retryable
}

func workerParticipantsRetryKey(job discord.VoiceJob) string {
	return strings.TrimSpace(job.StreamID) + "\x00participants"
}

func workerSpeakerRetryKey(job discord.VoiceJob) string {
	return strings.TrimSpace(job.StreamID) + "\x00active_speaker"
}

func workerChatRetryKey(job discord.VoiceJob, message ChatMessage) string {
	return strings.TrimSpace(job.StreamID) + "\x00chat\x00" + strings.TrimSpace(message.MessageID)
}

func sameWorkerEventJob(left, right discord.VoiceJob) bool {
	return strings.TrimSpace(left.StreamID) == strings.TrimSpace(right.StreamID) &&
		strings.TrimSpace(left.GuildID) == strings.TrimSpace(right.GuildID) &&
		strings.TrimSpace(left.VoiceChannelID) == strings.TrimSpace(right.VoiceChannelID) &&
		left.JobGeneration == right.JobGeneration
}

func (m *Manager) enqueueWorkerEventRetry(report workerEventRetryReport) bool {
	if !shouldRetryWorkerPublish(report.err) {
		return false
	}
	// Do not retain the original error in the pending queue. A transport error
	// may wrap implementation-specific data even though only its safe class is
	// needed for the decision above.
	report.err = nil
	m.mu.Lock()
	queue := m.workerRetry
	current := m.current
	currentGeneration := m.reconnectGeneration
	if queue == nil || !sameWorkerEventJob(current, report.job) || currentGeneration != report.reconnectGeneration {
		m.mu.Unlock()
		return false
	}
	m.mu.Unlock()
	return queue.enqueue(report)
}

func (m *Manager) supersedeWorkerEventRetry(job discord.VoiceJob, generation int64, key string) {
	m.mu.Lock()
	queue := m.workerRetry
	valid := queue != nil && sameWorkerEventJob(m.current, job) && m.reconnectGeneration == generation
	m.mu.Unlock()
	if valid {
		queue.mu.Lock()
		if !queue.stopped {
			delete(queue.pending, key)
			delete(queue.latest, key)
		}
		queue.mu.Unlock()
		queue.signal()
	}
}

func (m *Manager) isCurrentWorkerRetryReport(queue *workerEventRetryQueue, report workerEventRetryReport) bool {
	if !queue.isLatest(report) {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.workerRetry == queue && sameWorkerEventJob(m.current, report.job) && m.reconnectGeneration == report.reconnectGeneration
}

func (m *Manager) refreshWorkerRetryReport(queue *workerEventRetryQueue, report workerEventRetryReport) (workerEventRetryReport, EventReporter, context.Context, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.workerRetry != queue || !sameWorkerEventJob(m.current, report.job) || m.reconnectGeneration != report.reconnectGeneration {
		return workerEventRetryReport{}, nil, nil, false
	}
	report.job = m.current
	switch report.eventType {
	case "overlay.participants":
		report.participants = m.participantsSnapshotLocked()
		report.stateRevision = m.participantStateRevision
	case "overlay.active_speaker":
		report.speaking = report.speakerUserID != "" && m.activeSpeakers[report.speakerUserID]
		if participant, ok := m.participants[report.speakerUserID]; ok {
			report.speakerDisplayName = participant.Username
		}
	}
	return report, m.reporter, queue.ctx, true
}

func (m *Manager) deliverWorkerEventRetry(queue *workerEventRetryQueue, report workerEventRetryReport) {
	if queue.ctx.Err() != nil {
		queue.markDropped(report)
		return
	}
	if !m.isCurrentWorkerRetryReport(queue, report) {
		queue.markDropped(report)
		return
	}
	m.participantReportMu.Lock()
	defer m.participantReportMu.Unlock()
	if !m.isCurrentWorkerRetryReport(queue, report) {
		queue.markDropped(report)
		return
	}
	var reporter EventReporter
	var ctx context.Context
	var ok bool
	report, reporter, ctx, ok = m.refreshWorkerRetryReport(queue, report)
	if !ok || reporter == nil {
		queue.markDropped(report)
		return
	}
	report.attempts++
	err := m.publishWorkerRetryReport(ctx, reporter, report)
	if err == nil {
		queue.markDelivered(report)
		return
	}
	m.recordWorkerPublishFailureAttempt(report.eventType, report.job.StreamID, report.attempts, err)
	if !shouldRetryWorkerPublish(err) || ctx.Err() != nil || report.attempts >= maxWorkerEventRetryAttempts {
		queue.markDropped(report)
		return
	}
	queue.requeue(report)
}

func (m *Manager) publishWorkerRetryReport(ctx context.Context, reporter EventReporter, report workerEventRetryReport) error {
	switch report.eventType {
	case "overlay.participants":
		return publishParticipants(ctx, reporter, report.job, report.participants)
	case "overlay.active_speaker":
		return publishActiveSpeaker(ctx, reporter, report.job, report.speakerUserID, report.speakerDisplayName, report.speaking)
	case "overlay.discord_chat":
		return publishChatMessage(ctx, reporter, report.job, report.chat)
	default:
		return nil
	}
}

func publishParticipants(ctx context.Context, reporter EventReporter, job discord.VoiceJob, participants []Participant) error {
	if contextReporter, ok := reporter.(ParticipantsContextReporter); ok {
		return contextReporter.ParticipantsChangedContext(ctx, job, participants)
	}
	return reporter.ParticipantsChanged(job, participants)
}

func publishActiveSpeaker(ctx context.Context, reporter EventReporter, job discord.VoiceJob, userID, displayName string, speaking bool) error {
	if contextReporter, ok := reporter.(ActiveSpeakerContextReporter); ok {
		return contextReporter.ActiveSpeakerStateChangedContext(ctx, job, userID, displayName, speaking)
	}
	if stateReporter, ok := reporter.(ActiveSpeakerStateReporter); ok {
		return stateReporter.ActiveSpeakerStateChanged(job, userID, displayName, speaking)
	}
	if speaking {
		return reporter.ActiveSpeakerChanged(job, userID, displayName)
	}
	return nil
}

func publishChatMessage(ctx context.Context, reporter EventReporter, job discord.VoiceJob, message ChatMessage) error {
	if contextReporter, ok := reporter.(ChatContextReporter); ok {
		return contextReporter.ChatMessageReceivedContext(ctx, job, message)
	}
	return reporter.ChatMessageReceived(job, message)
}
