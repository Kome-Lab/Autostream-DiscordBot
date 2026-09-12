package jobs

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/example/autostream-discord-bot/internal/discord"
)

type Manager struct {
	voice                       discord.Client
	reporter                    EventReporter
	streamStarter               StreamStarter
	streamStopper               StreamStopper
	mu                          sync.Mutex
	current                     discord.VoiceJob
	stopping                    bool
	disconnected                bool
	defaults                    VoiceDefaults
	streamDefaults              map[string]VoiceDefaults
	autoStartPending            map[string]time.Time
	autoStartRetries            map[string]*autoStartRetry
	autoStopPending             map[string]bool
	autoStopGeneration          map[string]uint64
	autoStopAttempts            map[string]int
	autoStopInFlight            map[string]autoStopInFlight
	autoStopLastAttempt         map[string]time.Time
	rejoinIntents               map[string]autoStopRejoinIntent
	rejoinIntentSequence        uint64
	rejoinReconcileRun          map[string]bool
	rejoinReconcileDelay        []time.Duration
	autoStartCooldown           time.Duration
	autoStartRetryDelays        []time.Duration
	autoStopDelay               time.Duration
	autoStopEmptyConfirmDelay   time.Duration
	autoStopRetryDelays         []time.Duration
	autoStopCooldown            time.Duration
	autoStartRefresher          func() error
	autoStartRefreshAt          time.Time
	autoStartRefreshWait        time.Duration
	lastAutoStartLogAt          time.Time
	lastAutoStartLogKey         string
	reconnectPolicy             ReconnectPolicy
	reconnectGeneration         int64
	startedAt                   time.Time
	participants                map[string]Participant
	participantSnapshotRevision uint64
	participantStateRevision    uint64
	participantReportMu         sync.Mutex
	participantSyncCancel       context.CancelFunc
	participantSyncDelays       []time.Duration
	participantSyncInterval     time.Duration
	workerRetry                 *workerEventRetryQueue
	activeSpeaker               string
	activeSpeakers              map[string]bool
	lastEventAt                 time.Time
	workerFailures              int64
	workerFailureLogAt          map[string]time.Time
	workerFailureLogInterval    time.Duration
	voiceRejoinAttempts         int64
	voiceRejoinFailures         int64
	notificationReceipts        map[notificationEventKey]*notificationReceipt
}

func NewManager(voice discord.Client) *Manager {
	return NewManagerWithReporter(voice, nil)
}

func NewManagerWithReporter(voice discord.Client, reporter EventReporter) *Manager {
	if voice == nil {
		voice = &discord.NoopClient{}
	}
	return &Manager{
		voice:               voice,
		reporter:            reporter,
		participants:        map[string]Participant{},
		activeSpeakers:      map[string]bool{},
		streamDefaults:      map[string]VoiceDefaults{},
		autoStartPending:    map[string]time.Time{},
		autoStartRetries:    map[string]*autoStartRetry{},
		autoStopPending:     map[string]bool{},
		autoStopGeneration:  map[string]uint64{},
		autoStopAttempts:    map[string]int{},
		autoStopInFlight:    map[string]autoStopInFlight{},
		autoStopLastAttempt: map[string]time.Time{},
		rejoinIntents:       map[string]autoStopRejoinIntent{},
		rejoinReconcileRun:  map[string]bool{},
		// The Panel stops Bot, Encoder, and Worker in sequence (each service
		// call may take its bounded timeout) before it persists a newly rearmed
		// waiting stream. Keep a deliberately bounded, but operationally wider,
		// reconciliation window so a VC rejoin is not lost in that gap.
		rejoinReconcileDelay:      []time.Duration{0, time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second, 30 * time.Second},
		autoStartCooldown:         30 * time.Second,
		autoStartRetryDelays:      []time.Duration{500 * time.Millisecond, 2 * time.Second, 5 * time.Second},
		autoStopDelay:             2 * time.Second,
		autoStopEmptyConfirmDelay: 15 * time.Second,
		autoStopRetryDelays:       []time.Duration{5 * time.Second, 15 * time.Second, 45 * time.Second},
		autoStopCooldown:          15 * time.Second,
		autoStartRefreshWait:      5 * time.Second,
		// Discord's state cache may lag immediately after JoinVoice, while the
		// Bot -> Worker -> Encoder event path can fail transiently during service
		// restarts. Re-publish one authoritative full snapshot on a bounded warmup
		// schedule, then periodically while this exact job generation is active.
		participantSyncDelays:    []time.Duration{250 * time.Millisecond, time.Second, 3 * time.Second},
		participantSyncInterval:  15 * time.Second,
		workerFailureLogAt:       map[string]time.Time{},
		workerFailureLogInterval: 30 * time.Second,
		notificationReceipts:     map[notificationEventKey]*notificationReceipt{},
	}
}

func (m *Manager) Start(job discord.VoiceJob) error {
	job = m.ApplyVoiceDefaults(job)
	if strings.TrimSpace(job.StreamID) == "" {
		return errors.New("stream_id is required")
	}
	if strings.TrimSpace(job.GuildID) == "" {
		return errors.New("guild_id is required")
	}
	if strings.TrimSpace(job.VoiceChannelID) == "" {
		return errors.New("voice_channel_id is required")
	}
	m.mu.Lock()
	if m.stopping {
		m.mu.Unlock()
		return errors.New("stream job is stopping")
	}
	if m.current.StreamID != "" && m.current.StreamID != job.StreamID {
		m.mu.Unlock()
		return errors.New("another stream job is already active")
	}
	m.cancelAutoStopLocked(job.StreamID)
	m.mu.Unlock()

	if err := m.voice.JoinVoice(job); err != nil {
		return err
	}

	m.mu.Lock()
	m.reconnectGeneration++
	participantSyncGeneration := m.reconnectGeneration
	participantSyncContext := m.restartParticipantSyncLocked()
	previousWorkerRetry := m.workerRetry
	workerRetry := newWorkerEventRetryQueue()
	m.workerRetry = workerRetry
	m.current = job
	m.stopping = false
	m.disconnected = false
	now := time.Now().UTC()
	m.startedAt = now
	m.lastEventAt = now
	m.participants = map[string]Participant{}
	m.participantSnapshotRevision = 0
	m.participantStateRevision = 0
	m.activeSpeaker = ""
	m.activeSpeakers = map[string]bool{}
	delete(m.autoStartPending, job.StreamID)
	m.cancelAllAutoStartRetriesLocked()
	m.mu.Unlock()
	if previousWorkerRetry != nil {
		previousWorkerRetry.stopAndWait()
	}
	workerRetry.start(m)

	// Discord's gateway cache can briefly report the target channel as empty
	// while JoinVoice is establishing the bot's session. Do not turn that
	// transient initial snapshot into an immediate auto-stop; a later voice
	// event or reconnect snapshot remains authoritative. Rejoin hydration keeps
	// accepting an explicitly empty snapshot because the job was already live.
	m.hydrateVoiceParticipants(job, true)
	go m.keepVoiceParticipantsSynced(participantSyncContext, job, participantSyncGeneration)
	return nil
}

func (m *Manager) Stop(streamID string) error {
	m.mu.Lock()
	m.cancelAllAutoStartRetriesLocked()
	currentStreamID := m.current.StreamID
	if currentStreamID == "" {
		m.mu.Unlock()
		return errors.New("no active stream job")
	}
	if m.stopping {
		m.mu.Unlock()
		return errors.New("stream job is stopping")
	}
	if streamID != "" && streamID != currentStreamID {
		m.mu.Unlock()
		return errors.New("stream_id does not match current job")
	}
	// This method is called by the Control Panel while it is servicing the
	// outbound auto-stop request. Do not cancel that request here: canceling its
	// client context can cancel the Panel handler before it dispatches the
	// Encoder/Worker stops and creates the successor waiting stream. Participant
	// rejoin paths still use cancelAutoStopLocked to abort a stale request.
	m.invalidateAutoStopLocked(currentStreamID)
	m.stopping = true
	remainingWorkerRetry := m.workerRetry
	m.workerRetry = nil
	m.reconnectGeneration++
	m.mu.Unlock()
	if remainingWorkerRetry != nil {
		remainingWorkerRetry.stopAndWait()
	}

	if err := m.voice.LeaveVoice(currentStreamID); err != nil {
		m.mu.Lock()
		if m.current.StreamID == currentStreamID && m.stopping {
			m.stopping = false
			jobAfterFailure := m.current
			participantSyncGeneration := m.reconnectGeneration
			participantSyncContext := m.restartParticipantSyncLocked()
			workerRetry := newWorkerEventRetryQueue()
			m.workerRetry = workerRetry
			m.mu.Unlock()
			workerRetry.start(m)
			go m.keepVoiceParticipantsSynced(participantSyncContext, jobAfterFailure, participantSyncGeneration)
		} else {
			m.mu.Unlock()
		}
		return err
	}

	m.mu.Lock()
	if m.participantSyncCancel != nil {
		m.participantSyncCancel()
		m.participantSyncCancel = nil
	}
	previousWorkerRetry := m.workerRetry
	m.workerRetry = nil
	m.current = discord.VoiceJob{}
	m.stopping = false
	m.startedAt = time.Time{}
	m.lastEventAt = time.Now().UTC()
	m.participants = map[string]Participant{}
	m.activeSpeaker = ""
	m.activeSpeakers = map[string]bool{}
	m.mu.Unlock()
	if previousWorkerRetry != nil {
		previousWorkerRetry.stopAndWait()
	}
	return nil
}

func (m *Manager) CurrentStreamID() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.current.StreamID
}
