package jobs

import (
	"context"
	"errors"
	"log"
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
	defaults                    VoiceDefaults
	streamDefaults              map[string]VoiceDefaults
	autoStartPending            map[string]time.Time
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
	autoStopDelay               time.Duration
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

type VoiceDefaults struct {
	GuildID          string
	VoiceChannelID   string
	TextChannelID    string
	AutoStartEnabled bool
}

type ReconnectPolicy struct {
	Enabled     bool
	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
}

type EventReporter interface {
	ParticipantsChanged(job discord.VoiceJob, participants []Participant) error
	ActiveSpeakerChanged(job discord.VoiceJob, userID, displayName string) error
	ChatMessageReceived(job discord.VoiceJob, message ChatMessage) error
}

// ActiveSpeakerStateReporter is an optional EventReporter extension. It lets
// the worker distinguish a speaker stopping from a new speaker being detected
// while keeping the legacy reporter contract source-compatible.
type ActiveSpeakerStateReporter interface {
	ActiveSpeakerStateChanged(job discord.VoiceJob, userID, displayName string, speaking bool) error
}

type StreamStarter interface {
	StartStream(streamID string) error
}

type staleAutoStartError interface {
	ControlPanelCode() string
	HTTPStatusCode() int
}

type StreamStopper interface {
	StopStream(streamID string) error
}

// ContextStreamStopper lets VC-empty auto-stop cancel an in-flight external
// request when a participant returns. StreamStopper remains supported for
// callers that do not make a cancelable request.
type ContextStreamStopper interface {
	StopStreamContext(ctx context.Context, streamID string) error
}

type autoStopInFlight struct {
	generation uint64
	cancel     context.CancelFunc
	job        discord.VoiceJob
}

// autoStopRejoinIntent records a Discord VC join that happened while the
// empty-channel stop request was pending. A Panel stop can race with its
// cancellation, so this intent is retained until the freshly rearmed waiting
// stream can be reconciled or a bounded retry window proves it is not needed.
type autoStopRejoinIntent struct {
	GuildID        string
	VoiceChannelID string
	SourceStreamID string
	Sequence       uint64
}

type Participant struct {
	UserID    string    `json:"user_id"`
	Username  string    `json:"username,omitempty"`
	AvatarURL string    `json:"avatar_url,omitempty"`
	IsBot     bool      `json:"is_bot,omitempty"`
	Speaking  bool      `json:"speaking,omitempty"`
	JoinedAt  time.Time `json:"joined_at"`
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

type Status struct {
	CurrentJob       *discord.VoiceJob  `json:"current_job,omitempty"`
	CurrentStreamID  string             `json:"current_stream_id,omitempty"`
	StartedAt        *time.Time         `json:"started_at,omitempty"`
	Discord          discord.Status     `json:"discord"`
	Metrics          map[string]float64 `json:"metrics"`
	ParticipantCount int                `json:"participant_count"`
	ActiveSpeakerID  string             `json:"active_speaker_id,omitempty"`
	LastEventAt      *time.Time         `json:"last_event_at,omitempty"`
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
		rejoinReconcileDelay: []time.Duration{0, time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second, 30 * time.Second},
		autoStartCooldown:    30 * time.Second,
		autoStopDelay:        2 * time.Second,
		autoStopRetryDelays:  []time.Duration{5 * time.Second, 15 * time.Second, 45 * time.Second},
		autoStopCooldown:     15 * time.Second,
		autoStartRefreshWait: 5 * time.Second,
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
	m.current = job
	now := time.Now().UTC()
	m.startedAt = now
	m.lastEventAt = now
	m.participants = map[string]Participant{}
	m.participantSnapshotRevision = 0
	m.participantStateRevision = 0
	m.activeSpeaker = ""
	m.activeSpeakers = map[string]bool{}
	delete(m.autoStartPending, job.StreamID)
	m.mu.Unlock()

	// Discord's gateway cache can briefly report the target channel as empty
	// while JoinVoice is establishing the bot's session. Do not turn that
	// transient initial snapshot into an immediate auto-stop; a later voice
	// event or reconnect snapshot remains authoritative. Rejoin hydration keeps
	// accepting an explicitly empty snapshot because the job was already live.
	m.hydrateVoiceParticipants(job, true)
	go m.keepVoiceParticipantsSynced(participantSyncContext, job, participantSyncGeneration)
	return nil
}

func (m *Manager) SetStreamStarter(starter StreamStarter) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.streamStarter = starter
}

func (m *Manager) SetStreamStopper(stopper StreamStopper) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.streamStopper = stopper
}

// SetAutoStartRefresher supplies a best-effort runtime-config refresh for a
// VC join that arrives before a newly-created stream is visible in the
// manager's cached defaults, including the bounded reconciliation after a
// canceled auto-stop. Ordinary no-candidate joins rate limit this refresh;
// rejoin reconciliation intentionally polls within its own bounded window.
func (m *Manager) SetAutoStartRefresher(refresher func() error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.autoStartRefresher = refresher
	m.autoStartRefreshAt = time.Time{}
}

func (m *Manager) SetReconnectPolicy(policy ReconnectPolicy) {
	if policy.MaxAttempts <= 0 {
		policy.MaxAttempts = 3
	}
	if policy.BaseDelay < 0 {
		policy.BaseDelay = 0
	}
	if policy.MaxDelay <= 0 {
		policy.MaxDelay = 30 * time.Second
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.reconnectPolicy = policy
}

func (m *Manager) SetVoiceDefaults(defaults VoiceDefaults) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.defaults = VoiceDefaults{
		GuildID:          strings.TrimSpace(defaults.GuildID),
		VoiceChannelID:   strings.TrimSpace(defaults.VoiceChannelID),
		TextChannelID:    strings.TrimSpace(defaults.TextChannelID),
		AutoStartEnabled: defaults.AutoStartEnabled,
	}
}

func (m *Manager) SetStreamVoiceDefaults(defaults map[string]VoiceDefaults) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.streamDefaults = map[string]VoiceDefaults{}
	for streamID, item := range defaults {
		streamID = strings.TrimSpace(streamID)
		if streamID == "" {
			continue
		}
		m.streamDefaults[streamID] = VoiceDefaults{
			GuildID:          strings.TrimSpace(item.GuildID),
			VoiceChannelID:   strings.TrimSpace(item.VoiceChannelID),
			TextChannelID:    strings.TrimSpace(item.TextChannelID),
			AutoStartEnabled: item.AutoStartEnabled,
		}
	}
}

func (m *Manager) ApplyVoiceDefaults(job discord.VoiceJob) discord.VoiceJob {
	m.mu.Lock()
	defaults := m.defaults
	if streamDefaults, ok := m.streamDefaults[strings.TrimSpace(job.StreamID)]; ok {
		defaults = mergeVoiceDefaults(defaults, streamDefaults)
	}
	m.mu.Unlock()
	if strings.TrimSpace(job.GuildID) == "" {
		job.GuildID = defaults.GuildID
	}
	if strings.TrimSpace(job.VoiceChannelID) == "" {
		job.VoiceChannelID = defaults.VoiceChannelID
	}
	if strings.TrimSpace(job.TextChannelID) == "" {
		job.TextChannelID = defaults.TextChannelID
	}
	return job
}

func mergeVoiceDefaults(base, override VoiceDefaults) VoiceDefaults {
	if override.GuildID != "" {
		base.GuildID = override.GuildID
	}
	if override.VoiceChannelID != "" {
		base.VoiceChannelID = override.VoiceChannelID
	}
	if override.TextChannelID != "" {
		base.TextChannelID = override.TextChannelID
	}
	if override.AutoStartEnabled {
		base.AutoStartEnabled = true
	}
	return base
}

func (m *Manager) Stop(streamID string) error {
	m.mu.Lock()
	currentStreamID := m.current.StreamID
	if currentStreamID == "" {
		m.mu.Unlock()
		return errors.New("no active stream job")
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
	m.mu.Unlock()

	if err := m.voice.LeaveVoice(currentStreamID); err != nil {
		return err
	}

	m.mu.Lock()
	if m.participantSyncCancel != nil {
		m.participantSyncCancel()
		m.participantSyncCancel = nil
	}
	m.reconnectGeneration++
	defer m.mu.Unlock()
	m.current = discord.VoiceJob{}
	m.startedAt = time.Time{}
	m.lastEventAt = time.Now().UTC()
	m.participants = map[string]Participant{}
	m.activeSpeaker = ""
	m.activeSpeakers = map[string]bool{}
	return nil
}

func (m *Manager) CurrentStreamID() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.current.StreamID
}

func (m *Manager) Status() Status {
	m.mu.Lock()
	defer m.mu.Unlock()
	discordStatus := m.voice.Status()
	status := Status{
		Discord:          discordStatus,
		Metrics:          metricsFromStatus(discordStatus, len(m.participants)),
		ParticipantCount: len(m.participants),
		ActiveSpeakerID:  m.activeSpeaker,
	}
	status.Metrics["discord.worker_event_publish_failures_total"] = float64(m.workerFailures)
	status.Metrics["discord.voice_rejoin_attempts_total"] = float64(m.voiceRejoinAttempts)
	status.Metrics["discord.voice_rejoin_failures_total"] = float64(m.voiceRejoinFailures)
	if m.current.StreamID != "" {
		job := m.current
		job.EncoderAudioURL = ""
		job.CaptionAudioURL = ""
		job.CaptionAudioToken = ""
		job.StreamIngestToken = ""
		job.WorkerEventsURL = ""
		job.WorkerEventsToken = ""
		status.CurrentJob = &job
		status.CurrentStreamID = job.StreamID
		startedAt := m.startedAt
		status.StartedAt = &startedAt
	}
	if !m.lastEventAt.IsZero() {
		lastEventAt := m.lastEventAt
		status.LastEventAt = &lastEventAt
	}
	return status
}

func (m *Manager) Metrics() map[string]float64 {
	status := m.Status()
	return status.Metrics
}

func (m *Manager) recordWorkerPublishFailure(eventType, streamID string, err error) {
	eventType = strings.TrimSpace(eventType)
	streamID = strings.TrimSpace(streamID)
	if eventType == "" {
		eventType = "unknown"
	}
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
		log.Printf("Discord worker event publish failed: event_type=%s stream_id=%s error_class=%T", eventType, streamID, err)
	}
}

func (m *Manager) Participants(streamID string) ([]Participant, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.current.StreamID == "" {
		return nil, errors.New("no active stream job")
	}
	if streamID != "" && streamID != m.current.StreamID {
		return nil, errors.New("stream_id does not match current job")
	}
	out := make([]Participant, 0, len(m.participants))
	for _, participant := range m.participants {
		out = append(out, participant)
	}
	return out, nil
}

func (m *Manager) ParticipantChanged(event discord.ParticipantEvent) {
	if strings.TrimSpace(event.UserID) == "" {
		return
	}
	m.mu.Lock()
	if m.current.StreamID == "" || event.StreamID != m.current.StreamID {
		m.mu.Unlock()
		return
	}
	now := time.Now().UTC()
	if event.Present {
		joinedAt := now
		if existing, ok := m.participants[event.UserID]; ok && !existing.JoinedAt.IsZero() {
			joinedAt = existing.JoinedAt
		}
		participant := Participant{UserID: event.UserID, Username: event.Username, AvatarURL: event.AvatarURL, IsBot: event.IsBot, JoinedAt: joinedAt}
		if existing, ok := m.participants[event.UserID]; ok {
			if participant.Username == "" {
				participant.Username = existing.Username
			}
			if participant.AvatarURL == "" {
				participant.AvatarURL = existing.AvatarURL
			}
			participant.IsBot = participant.IsBot || existing.IsBot
		}
		m.participants[event.UserID] = participant
	} else {
		delete(m.participants, event.UserID)
		delete(m.activeSpeakers, event.UserID)
		if m.activeSpeaker == event.UserID {
			m.activeSpeaker = anyActiveSpeaker(m.activeSpeakers)
		}
	}
	m.lastEventAt = now
	m.participantStateRevision++
	participantStateRevision := m.participantStateRevision
	job := m.current
	participants := m.participantsSnapshotLocked()
	stopper, shouldAutoStop, autoStopGeneration := m.updateAutoStopForParticipantSetLocked(job, now)
	m.mu.Unlock()
	m.reportParticipantsIfCurrent(job, participants, participantStateRevision, 0, false)
	if shouldAutoStop {
		go m.autoStopWhenEmpty(job.StreamID, autoStopGeneration, stopper, m.autoStopDelay)
	}
}

type participantSnapshotApplyOptions struct {
	expectedGeneration    int64
	requireGeneration     bool
	authoritativeReplay   bool
	expectedStateRevision uint64
	requireStateRevision  bool
}

// ParticipantsSynced replaces the locally inferred participant set with a
// current Discord State snapshot. This closes both startup and reconnect gaps:
// an already-present member cannot be missed, while a leave lost during a
// gateway reconnect cannot keep an empty VC alive indefinitely.
func (m *Manager) ParticipantsSynced(snapshot discord.ParticipantSnapshot) {
	m.participantsSynced(snapshot, participantSnapshotApplyOptions{})
}

func (m *Manager) participantsSynced(snapshot discord.ParticipantSnapshot, options participantSnapshotApplyOptions) bool {
	snapshot.StreamID = strings.TrimSpace(snapshot.StreamID)
	snapshot.GuildID = strings.TrimSpace(snapshot.GuildID)
	snapshot.VoiceChannelID = strings.TrimSpace(snapshot.VoiceChannelID)
	if snapshot.StreamID == "" || snapshot.GuildID == "" || snapshot.VoiceChannelID == "" {
		return false
	}

	m.mu.Lock()
	job := m.current
	if job.StreamID != snapshot.StreamID || job.GuildID != snapshot.GuildID || job.VoiceChannelID != snapshot.VoiceChannelID || (options.requireGeneration && m.reconnectGeneration != options.expectedGeneration) {
		m.mu.Unlock()
		return false
	}
	if options.requireStateRevision && m.participantStateRevision != options.expectedStateRevision {
		participants := m.participantsSnapshotLocked()
		stateRevision := m.participantStateRevision
		m.mu.Unlock()
		m.reportParticipantsIfCurrent(job, participants, stateRevision, options.expectedGeneration, options.requireGeneration)
		return true
	}
	if snapshot.Revision != 0 && snapshot.Revision <= m.participantSnapshotRevision {
		if options.authoritativeReplay && snapshot.Revision == m.participantSnapshotRevision {
			participants := m.participantsSnapshotLocked()
			stateRevision := m.participantStateRevision
			m.mu.Unlock()
			m.reportParticipantsIfCurrent(job, participants, stateRevision, options.expectedGeneration, options.requireGeneration)
			return true
		}
		// A periodic snapshot can race a newer gateway snapshot between reading
		// Discord state and taking the manager lock. Never let that older view
		// replace the newer participant set.
		m.mu.Unlock()
		return true
	}
	if snapshot.Revision > m.participantSnapshotRevision {
		m.participantSnapshotRevision = snapshot.Revision
	}
	now := time.Now().UTC()
	participantsByID := make(map[string]Participant, len(snapshot.Participants))
	for _, item := range snapshot.Participants {
		userID := strings.TrimSpace(item.UserID)
		if userID == "" {
			continue
		}
		participant := Participant{UserID: userID, Username: strings.TrimSpace(item.Username), AvatarURL: strings.TrimSpace(item.AvatarURL), IsBot: item.IsBot, JoinedAt: now}
		if existing, ok := m.participants[userID]; ok {
			if !existing.JoinedAt.IsZero() {
				participant.JoinedAt = existing.JoinedAt
			}
			if participant.Username == "" {
				participant.Username = existing.Username
			}
			if participant.AvatarURL == "" {
				participant.AvatarURL = existing.AvatarURL
			}
			participant.IsBot = participant.IsBot || existing.IsBot
		}
		participantsByID[userID] = participant
	}
	m.participants = participantsByID
	for userID := range m.activeSpeakers {
		if _, present := m.participants[userID]; !present {
			delete(m.activeSpeakers, userID)
		}
	}
	if !m.activeSpeakers[m.activeSpeaker] {
		m.activeSpeaker = anyActiveSpeaker(m.activeSpeakers)
	}
	m.lastEventAt = now
	m.participantStateRevision++
	stateRevision := m.participantStateRevision
	participants := m.participantsSnapshotLocked()
	stopper, shouldAutoStop, autoStopGeneration := m.updateAutoStopForParticipantSetLocked(job, now)
	m.mu.Unlock()

	m.reportParticipantsIfCurrent(job, participants, stateRevision, options.expectedGeneration, options.requireGeneration)
	if shouldAutoStop {
		go m.autoStopWhenEmpty(job.StreamID, autoStopGeneration, stopper, m.autoStopDelay)
	}
	return true
}

func (m *Manager) hydrateVoiceParticipants(job discord.VoiceJob, suppressInitialEmpty bool) {
	source, ok := m.voice.(discord.ParticipantSnapshotSource)
	if !ok {
		return
	}
	snapshot, known := source.SnapshotVoiceParticipants(job)
	if !known {
		return
	}
	if suppressInitialEmpty && len(snapshot.Participants) == 0 {
		return
	}
	m.ParticipantsSynced(snapshot)
}

func (m *Manager) keepVoiceParticipantsSynced(ctx context.Context, job discord.VoiceJob, generation int64) {
	if _, ok := m.voice.(discord.ParticipantSnapshotSource); !ok {
		return
	}
	m.mu.Lock()
	delays := append([]time.Duration(nil), m.participantSyncDelays...)
	interval := m.participantSyncInterval
	m.mu.Unlock()

	for index, delay := range delays {
		if !waitForParticipantSync(ctx, delay) || !m.participantJobCurrent(job, generation, true) {
			return
		}
		// Keep the earliest cache-warmup snapshots from turning Discord's
		// transient empty state into an auto-stop. The final delayed snapshot is
		// authoritative even when empty.
		suppressEmpty := index < len(delays)-1
		if !m.hydrateVoiceParticipantsForGeneration(job, suppressEmpty, generation) {
			return
		}
	}

	if interval <= 0 {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !m.hydrateVoiceParticipantsForGeneration(job, false, generation) {
				return
			}
		}
	}
}

// restartParticipantSyncLocked replaces the one sync loop owned by the active
// job. The caller must hold m.mu and start keepVoiceParticipantsSynced only
// after releasing it.
func (m *Manager) restartParticipantSyncLocked() context.Context {
	if m.participantSyncCancel != nil {
		m.participantSyncCancel()
	}
	ctx, cancel := context.WithCancel(context.Background())
	m.participantSyncCancel = cancel
	return ctx
}

func (m *Manager) hydrateVoiceParticipantsForGeneration(job discord.VoiceJob, suppressEmpty bool, generation int64) bool {
	m.mu.Lock()
	if m.reconnectGeneration != generation || m.current.StreamID != job.StreamID || m.current.GuildID != job.GuildID || m.current.VoiceChannelID != job.VoiceChannelID {
		m.mu.Unlock()
		return false
	}
	stateRevision := m.participantStateRevision
	m.mu.Unlock()
	source, ok := m.voice.(discord.ParticipantSnapshotSource)
	if !ok {
		return false
	}
	snapshot, known := source.SnapshotVoiceParticipants(job)
	if !known {
		return m.participantJobCurrent(job, generation, true)
	}
	if suppressEmpty && len(snapshot.Participants) == 0 {
		return m.participantJobCurrent(job, generation, true)
	}
	return m.participantsSynced(snapshot, participantSnapshotApplyOptions{
		expectedGeneration:    generation,
		requireGeneration:     true,
		authoritativeReplay:   true,
		expectedStateRevision: stateRevision,
		requireStateRevision:  true,
	})
}

func (m *Manager) participantJobCurrent(job discord.VoiceJob, generation int64, requireGeneration bool) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if requireGeneration && m.reconnectGeneration != generation {
		return false
	}
	return m.current.StreamID == job.StreamID && m.current.GuildID == job.GuildID && m.current.VoiceChannelID == job.VoiceChannelID
}

func (m *Manager) reportParticipantsIfCurrent(job discord.VoiceJob, participants []Participant, stateRevision uint64, generation int64, requireGeneration bool) {
	m.participantReportMu.Lock()
	defer m.participantReportMu.Unlock()

	m.mu.Lock()
	current := m.current
	if current.StreamID != job.StreamID || current.GuildID != job.GuildID || current.VoiceChannelID != job.VoiceChannelID || (requireGeneration && m.reconnectGeneration != generation) {
		m.mu.Unlock()
		return
	}
	if m.participantStateRevision != stateRevision {
		// Participant and speaking callbacks update state before waiting for the
		// shared publish lock. If one overtakes this report, publish the newest
		// full snapshot instead of dropping the participant card until the next
		// periodic sync. Any newer event is then serialized after this snapshot.
		participants = m.participantsSnapshotLocked()
	}
	reporter := m.reporter
	m.mu.Unlock()
	if reporter != nil {
		if err := reporter.ParticipantsChanged(job, participants); err != nil {
			m.recordWorkerPublishFailure("overlay.participants", job.StreamID, err)
		}
	}
}

func waitForParticipantSync(ctx context.Context, delay time.Duration) bool {
	if delay <= 0 {
		select {
		case <-ctx.Done():
			return false
		default:
			return true
		}
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// updateAutoStopForParticipantSetLocked applies the single empty/non-empty
// policy to both transition events and authoritative snapshot replacement.
// The caller must hold m.mu.
func (m *Manager) updateAutoStopForParticipantSetLocked(job discord.VoiceJob, now time.Time) (StreamStopper, bool, uint64) {
	stopper := m.streamStopper
	if len(m.participants) > 0 {
		if m.hasAutoStopInFlightLocked(job.StreamID) {
			m.recordAutoStopRejoinIntentLocked(job.StreamID, job.GuildID, job.VoiceChannelID)
		}
		m.cancelAutoStopLocked(job.StreamID)
		return stopper, false, 0
	}
	if stopper == nil {
		return nil, false, 0
	}
	lastAttempt := m.autoStopLastAttempt[job.StreamID]
	if m.autoStopPending[job.StreamID] || (!lastAttempt.IsZero() && now.Sub(lastAttempt) < m.autoStopCooldown) {
		return stopper, false, 0
	}
	m.autoStopPending[job.StreamID] = true
	m.autoStopGeneration[job.StreamID]++
	autoStopGeneration := m.autoStopGeneration[job.StreamID]
	m.autoStopAttempts[job.StreamID] = 0
	return stopper, true, autoStopGeneration
}

func (m *Manager) autoStopWhenEmpty(streamID string, generation uint64, stopper StreamStopper, delay time.Duration) {
	if delay < 0 {
		delay = 0
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	<-timer.C

	m.mu.Lock()
	if !m.autoStopStillValidLocked(streamID, generation) {
		m.mu.Unlock()
		return
	}
	m.autoStopLastAttempt[streamID] = time.Now().UTC()
	ctx, cancel := context.WithCancel(context.Background())
	m.autoStopInFlight[streamID] = autoStopInFlight{generation: generation, cancel: cancel, job: m.current}
	m.mu.Unlock()

	err := stopStreamWithContext(ctx, stopper, streamID)
	m.mu.Lock()
	m.clearAutoStopInFlightLocked(streamID, generation)
	stillValid := m.autoStopStillValidLocked(streamID, generation)
	reconcileRejoin := !stillValid && m.hasAutoStopRejoinIntentForStreamLocked(streamID)
	m.mu.Unlock()
	if reconcileRejoin {
		m.scheduleAutoStopRejoinReconciliation(streamID)
	}
	if err != nil {
		if !stillValid {
			return
		}
		log.Printf("Discord VC auto-stop request failed for stream=%s: %v", streamID, err)
		m.scheduleAutoStopRetry(streamID, generation, stopper, err)
		return
	}
	if !stillValid {
		return
	}
	m.mu.Lock()
	m.finishAutoStopLocked(streamID, generation)
	m.mu.Unlock()
}

func stopStreamWithContext(ctx context.Context, stopper StreamStopper, streamID string) error {
	if contextual, ok := stopper.(ContextStreamStopper); ok {
		return contextual.StopStreamContext(ctx, streamID)
	}
	return stopper.StopStream(streamID)
}

func (m *Manager) scheduleAutoStopRetry(streamID string, generation uint64, stopper StreamStopper, err error) {
	if !shouldRetryAutoStop(err) {
		m.mu.Lock()
		m.finishAutoStopLocked(streamID, generation)
		m.mu.Unlock()
		return
	}

	m.mu.Lock()
	if !m.autoStopStillValidLocked(streamID, generation) {
		m.mu.Unlock()
		return
	}
	attempt := m.autoStopAttempts[streamID]
	if attempt >= len(m.autoStopRetryDelays) {
		m.finishAutoStopLocked(streamID, generation)
		m.mu.Unlock()
		log.Printf("Discord VC auto-stop exhausted retries for stream=%s", streamID)
		return
	}
	delay := m.autoStopRetryDelays[attempt]
	m.autoStopAttempts[streamID] = attempt + 1
	m.mu.Unlock()

	log.Printf("Discord VC auto-stop retry scheduled for stream=%s attempt=%d delay=%s", streamID, attempt+1, delay)
	go m.autoStopWhenEmpty(streamID, generation, stopper, delay)
}

func (m *Manager) autoStopStillValidLocked(streamID string, generation uint64) bool {
	return m.autoStopGeneration[streamID] == generation &&
		m.autoStopPending[streamID] &&
		m.current.StreamID == streamID &&
		len(m.participants) == 0 &&
		m.streamStopper != nil
}

func (m *Manager) finishAutoStopLocked(streamID string, generation uint64) {
	m.clearAutoStopInFlightLocked(streamID, generation)
	if m.autoStopGeneration[streamID] != generation {
		return
	}
	delete(m.autoStopPending, streamID)
	delete(m.autoStopAttempts, streamID)
}

func (m *Manager) cancelAutoStopLocked(streamID string) {
	streamID = strings.TrimSpace(streamID)
	if streamID == "" {
		return
	}
	if request, ok := m.autoStopInFlight[streamID]; ok {
		request.cancel()
		delete(m.autoStopInFlight, streamID)
	}
	m.invalidateAutoStopLocked(streamID)
}

// invalidateAutoStopLocked prevents timers and retries from remaining valid
// without canceling a possibly active Control Panel request. This is used by
// the inbound Panel stop callback so it cannot tear down its own parent
// request before the rest of the service stop/rearm transaction completes.
func (m *Manager) invalidateAutoStopLocked(streamID string) {
	streamID = strings.TrimSpace(streamID)
	if streamID == "" {
		return
	}
	m.autoStopGeneration[streamID]++
	delete(m.autoStopPending, streamID)
	delete(m.autoStopAttempts, streamID)
	delete(m.autoStopLastAttempt, streamID)
}

func (m *Manager) hasPendingAutoStopLocked(streamID string) bool {
	if m.autoStopPending[streamID] {
		return true
	}
	return m.hasAutoStopInFlightLocked(streamID)
}

func (m *Manager) hasAutoStopInFlightLocked(streamID string) bool {
	_, inFlight := m.autoStopInFlight[streamID]
	return inFlight
}

// inFlightAutoStopStreamForVoiceLocked finds the still-unanswered outbound
// auto-stop request that owns a VC. The current job may already be empty
// because the Panel has called this Bot's /stop endpoint while it continues
// dispatching Encoder/Worker and rearming the successor. Do not guess when
// more than one stale request claims the same VC.
func (m *Manager) inFlightAutoStopStreamForVoiceLocked(guildID, voiceChannelID string) string {
	guildID = strings.TrimSpace(guildID)
	voiceChannelID = strings.TrimSpace(voiceChannelID)
	if guildID == "" || voiceChannelID == "" {
		return ""
	}
	matched := ""
	for streamID, request := range m.autoStopInFlight {
		if request.job.GuildID != guildID || request.job.VoiceChannelID != voiceChannelID {
			continue
		}
		if matched != "" && matched != streamID {
			return ""
		}
		matched = streamID
	}
	return matched
}

func (m *Manager) recordAutoStopRejoinIntentLocked(sourceStreamID, guildID, voiceChannelID string) {
	sourceStreamID = strings.TrimSpace(sourceStreamID)
	guildID = strings.TrimSpace(guildID)
	voiceChannelID = strings.TrimSpace(voiceChannelID)
	if sourceStreamID == "" || guildID == "" || voiceChannelID == "" {
		return
	}
	key := autoStopRejoinIntentKey(guildID, voiceChannelID)
	if existing, ok := m.rejoinIntents[key]; ok && existing.SourceStreamID == sourceStreamID {
		return
	}
	m.rejoinIntentSequence++
	m.rejoinIntents[key] = autoStopRejoinIntent{
		GuildID:        guildID,
		VoiceChannelID: voiceChannelID,
		SourceStreamID: sourceStreamID,
		Sequence:       m.rejoinIntentSequence,
	}
}

func autoStopRejoinIntentKey(guildID, voiceChannelID string) string {
	return strings.TrimSpace(guildID) + "\x00" + strings.TrimSpace(voiceChannelID)
}

func firstNonEmptyTrimmed(values ...string) string {
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}

func (m *Manager) autoStopRejoinIntentForStreamLocked(streamID string) (string, autoStopRejoinIntent, bool) {
	streamID = strings.TrimSpace(streamID)
	for key, intent := range m.rejoinIntents {
		if intent.SourceStreamID == streamID {
			return key, intent, true
		}
	}
	return "", autoStopRejoinIntent{}, false
}

func (m *Manager) hasAutoStopRejoinIntentForStreamLocked(streamID string) bool {
	_, _, ok := m.autoStopRejoinIntentForStreamLocked(streamID)
	return ok
}

func (m *Manager) scheduleAutoStopRejoinReconciliation(streamID string) {
	streamID = strings.TrimSpace(streamID)
	if streamID == "" {
		return
	}
	m.mu.Lock()
	if !m.hasAutoStopRejoinIntentForStreamLocked(streamID) || m.rejoinReconcileRun[streamID] {
		m.mu.Unlock()
		return
	}
	m.rejoinReconcileRun[streamID] = true
	m.mu.Unlock()
	go m.reconcileAutoStopRejoin(streamID)
}

func (m *Manager) reconcileAutoStopRejoin(sourceStreamID string) {
	defer func() {
		m.mu.Lock()
		delete(m.rejoinReconcileRun, sourceStreamID)
		m.mu.Unlock()
	}()

	m.mu.Lock()
	delays := append([]time.Duration(nil), m.rejoinReconcileDelay...)
	m.mu.Unlock()
	for _, delay := range delays {
		if delay > 0 {
			timer := time.NewTimer(delay)
			<-timer.C
		}

		m.mu.Lock()
		key, intent, ok := m.autoStopRejoinIntentForStreamLocked(sourceStreamID)
		refresher := m.autoStartRefresher
		m.mu.Unlock()
		if !ok {
			return
		}
		if refresher != nil {
			if err := refresher(); err != nil {
				log.Printf("Discord VC auto-stop rejoin runtime config refresh failed for stream=%s: %v", sourceStreamID, err)
				continue
			}
		}

		m.mu.Lock()
		currentKey, currentIntent, stillCurrent := m.autoStopRejoinIntentForStreamLocked(sourceStreamID)
		if !stillCurrent || currentKey != key || currentIntent.Sequence != intent.Sequence {
			m.mu.Unlock()
			return
		}
		if m.current.StreamID != "" {
			if m.current.StreamID != sourceStreamID {
				delete(m.rejoinIntents, key)
				m.mu.Unlock()
				return
			}
			m.mu.Unlock()
			continue
		}
		rearmed := m.matchingAutoStartStreamLocked(intent.GuildID, intent.VoiceChannelID)
		if rearmed == "" || rearmed != sourceStreamID || m.streamStarter == nil {
			m.mu.Unlock()
			continue
		}
		now := time.Now().UTC()
		if last, pending := m.autoStartPending[rearmed]; pending && now.Sub(last) < m.autoStartCooldown {
			delete(m.rejoinIntents, key)
			m.mu.Unlock()
			return
		}
		m.autoStartPending[rearmed] = now
		m.lastEventAt = now
		starter := m.streamStarter
		delete(m.rejoinIntents, key)
		m.mu.Unlock()

		go func() {
			if err := starter.StartStream(rearmed); err != nil {
				log.Printf("Discord VC auto-stop rejoin request failed for stream=%s: %v", rearmed, err)
			}
		}()
		return
	}

	m.mu.Lock()
	if key, intent, ok := m.autoStopRejoinIntentForStreamLocked(sourceStreamID); ok {
		delete(m.rejoinIntents, key)
		log.Printf("Discord VC auto-stop rejoin reconciliation exhausted for stream=%s guild=%s voice=%s", sourceStreamID, intent.GuildID, intent.VoiceChannelID)
	}
	m.mu.Unlock()
}

func (m *Manager) clearAutoStopInFlightLocked(streamID string, generation uint64) {
	request, ok := m.autoStopInFlight[streamID]
	if !ok || request.generation != generation {
		return
	}
	request.cancel()
	delete(m.autoStopInFlight, streamID)
}

type autoStopRetryability interface {
	RetryableAutoStop() bool
}

func shouldRetryAutoStop(err error) bool {
	var classified autoStopRetryability
	if errors.As(err, &classified) {
		return classified.RetryableAutoStop()
	}
	return false
}

func (m *Manager) VoiceUserJoined(event discord.VoiceJoinEvent) {
	event.GuildID = strings.TrimSpace(event.GuildID)
	event.VoiceChannelID = strings.TrimSpace(event.VoiceChannelID)
	event.UserID = strings.TrimSpace(event.UserID)
	if event.GuildID == "" || event.VoiceChannelID == "" || event.UserID == "" {
		return
	}
	now := time.Now().UTC()
	m.mu.Lock()
	if m.current.StreamID != "" {
		current := m.current
		if current.GuildID == event.GuildID && current.VoiceChannelID == event.VoiceChannelID && m.hasAutoStopInFlightLocked(current.StreamID) {
			m.recordAutoStopRejoinIntentLocked(current.StreamID, event.GuildID, event.VoiceChannelID)
			m.cancelAutoStopLocked(current.StreamID)
		}
		m.mu.Unlock()
		return
	}
	if sourceStreamID := m.inFlightAutoStopStreamForVoiceLocked(event.GuildID, event.VoiceChannelID); sourceStreamID != "" {
		// The Panel has already called our local /stop endpoint, so canceling
		// the parent request at this point can interrupt its remaining service
		// dispatch. Retain the rejoin intent and wait for that request to return
		// after the successor has been rearmed instead.
		m.recordAutoStopRejoinIntentLocked(sourceStreamID, event.GuildID, event.VoiceChannelID)
		m.mu.Unlock()
		return
	}
	streamID := m.matchingAutoStartStreamLocked(event.GuildID, event.VoiceChannelID)
	refresher := m.autoStartRefresher
	shouldRefresh := streamID == "" && refresher != nil && (m.autoStartRefreshWait <= 0 || m.autoStartRefreshAt.IsZero() || now.Sub(m.autoStartRefreshAt) >= m.autoStartRefreshWait)
	if shouldRefresh {
		m.autoStartRefreshAt = now
	}
	m.mu.Unlock()

	if shouldRefresh {
		if err := refresher(); err != nil {
			log.Printf("Discord VC auto-start runtime config refresh failed for guild=%s voice=%s: %v", event.GuildID, event.VoiceChannelID, err)
			return
		}
		m.mu.Lock()
		if m.current.StreamID != "" {
			m.mu.Unlock()
			return
		}
		streamID = m.matchingAutoStartStreamLocked(event.GuildID, event.VoiceChannelID)
	} else {
		m.mu.Lock()
	}
	if streamID == "" {
		if m.shouldLogAutoStartLocked("no-candidate", now) {
			log.Printf("Discord VC auto-start ignored: no matching waiting stream for guild=%s voice=%s configured_streams=%d", event.GuildID, event.VoiceChannelID, len(m.streamDefaults))
		}
		m.mu.Unlock()
		return
	}
	if m.streamStarter == nil {
		if m.shouldLogAutoStartLocked("starter-missing:"+streamID, now) {
			log.Printf("Discord VC auto-start unavailable: Control Panel stream starter is not configured for stream=%s (check CONTROL_PANEL_URL and CONTROL_PANEL_TOKEN)", streamID)
		}
		m.mu.Unlock()
		return
	}
	if last, ok := m.autoStartPending[streamID]; ok && now.Sub(last) < m.autoStartCooldown {
		m.mu.Unlock()
		return
	}
	m.autoStartPending[streamID] = now
	starter := m.streamStarter
	m.lastEventAt = now
	m.mu.Unlock()

	go func() {
		if err := starter.StartStream(streamID); err != nil {
			log.Printf("Discord VC auto-start request failed for stream=%s: %v", streamID, err)
			if !isStaleAutoStartError(err) || refresher == nil {
				return
			}
			if refreshErr := refresher(); refreshErr != nil {
				log.Printf("Discord VC auto-start stale-stream refresh failed for stream=%s: %v", streamID, refreshErr)
				return
			}
			m.mu.Lock()
			if m.current.StreamID != "" {
				m.mu.Unlock()
				return
			}
			successor := m.matchingAutoStartStreamLocked(event.GuildID, event.VoiceChannelID)
			if successor != "" && successor != streamID {
				m.autoStartPending[successor] = time.Now().UTC()
			}
			m.mu.Unlock()
			if successor == "" || successor == streamID {
				log.Printf("Discord VC auto-start stale stream has no refreshed successor: old_stream=%s", streamID)
				return
			}
			if retryErr := starter.StartStream(successor); retryErr != nil {
				log.Printf("Discord VC auto-start successor request failed for stream=%s (old_stream=%s): %v", successor, streamID, retryErr)
			} else {
				log.Printf("Discord VC auto-start successor accepted by control panel for stream=%s (old_stream=%s)", successor, streamID)
			}
			return
		}
		// This receipt means only that the Control Panel accepted the request;
		// Encoder/YouTube lifecycle readiness is verified separately.
		log.Printf("Discord VC auto-start accepted by control panel for stream=%s", streamID)
	}()
}

func isStaleAutoStartError(err error) bool {
	var classified staleAutoStartError
	if !errors.As(err, &classified) {
		return false
	}
	return classified.HTTPStatusCode() == 404 && classified.ControlPanelCode() == "not_found"
}

func (m *Manager) shouldLogAutoStartLocked(key string, now time.Time) bool {
	if key == m.lastAutoStartLogKey && !m.lastAutoStartLogAt.IsZero() && now.Sub(m.lastAutoStartLogAt) < 10*time.Second {
		return false
	}
	m.lastAutoStartLogKey = key
	m.lastAutoStartLogAt = now
	return true
}

func (m *Manager) ChatMessageReceived(event discord.ChatMessageEvent) {
	content := trimDiscordChatContent(event.Content)
	messageID := trimDiscordChatField(event.MessageID, 128)
	userID := trimDiscordChatField(event.UserID, 128)
	if content == "" || messageID == "" || userID == "" {
		return
	}
	m.mu.Lock()
	if m.current.StreamID == "" || event.StreamID != m.current.StreamID || event.GuildID != m.current.GuildID || event.TextChannelID != m.current.TextChannelID {
		m.mu.Unlock()
		return
	}
	job := m.current
	reporter := m.reporter
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
	if reporter != nil {
		if err := reporter.ChatMessageReceived(job, message); err != nil {
			m.recordWorkerPublishFailure("overlay.discord_chat", job.StreamID, err)
		}
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

func (m *Manager) matchingAutoStartStreamLocked(guildID, voiceChannelID string) string {
	guildID = strings.TrimSpace(guildID)
	voiceChannelID = strings.TrimSpace(voiceChannelID)
	if guildID == "" || voiceChannelID == "" {
		return ""
	}
	matched := ""
	for streamID, defaults := range m.streamDefaults {
		if !defaults.AutoStartEnabled {
			continue
		}
		if defaults.GuildID != guildID || defaults.VoiceChannelID != voiceChannelID {
			continue
		}
		if matched != "" {
			return ""
		}
		matched = streamID
	}
	return matched
}

func (m *Manager) DiscordConnected() {
	m.mu.Lock()
	m.lastEventAt = time.Now().UTC()
	job := m.current
	generation := m.reconnectGeneration
	var participantSyncContext context.Context
	if job.StreamID != "" {
		participantSyncContext = m.restartParticipantSyncLocked()
	}
	m.mu.Unlock()
	if participantSyncContext != nil {
		go m.keepVoiceParticipantsSynced(participantSyncContext, job, generation)
	}
}

func (m *Manager) DiscordDisconnected(reason string) {
	m.mu.Lock()
	m.lastEventAt = time.Now().UTC()
	job := m.current
	policy := m.reconnectPolicy
	if m.participantSyncCancel != nil {
		m.participantSyncCancel()
		m.participantSyncCancel = nil
	}
	m.reconnectGeneration++
	generation := m.reconnectGeneration
	m.mu.Unlock()
	if shouldRejoinVoice(reason, job, policy) {
		go m.rejoinVoiceWithBackoff(job, policy, generation)
	}
}

func (m *Manager) ActiveSpeakerDetected(streamID, userID string) {
	_ = m.SetActiveSpeaker(streamID, userID)
}

// ActiveSpeakerStateChanged tracks both edges of Discord's speaking signal.
// A stop from a non-active participant must not clear another participant's
// currently highlighted speaker.
func (m *Manager) ActiveSpeakerStateChanged(streamID, userID string, speaking bool) {
	_ = m.setSpeakerState(streamID, userID, speaking, true)
}

func (m *Manager) SetActiveSpeaker(streamID, userID string) error {
	if userID == "" {
		return m.clearSpeakerStates(streamID)
	}
	return m.setSpeakerState(streamID, userID, true, false)
}

func (m *Manager) setSpeakerState(streamID, userID string, speaking, preserveOthers bool) error {
	m.mu.Lock()
	if m.current.StreamID == "" {
		m.mu.Unlock()
		return errors.New("no active stream job")
	}
	if streamID != "" && streamID != m.current.StreamID {
		m.mu.Unlock()
		return errors.New("stream_id does not match current job")
	}
	if _, ok := m.participants[userID]; !ok {
		m.mu.Unlock()
		return errors.New("active speaker must be an active participant")
	}
	if m.activeSpeakers == nil {
		m.activeSpeakers = map[string]bool{}
	}
	currentlySpeaking := m.activeSpeakers[userID]
	if preserveOthers && currentlySpeaking == speaking {
		m.mu.Unlock()
		return nil
	}
	if speaking && !preserveOthers && currentlySpeaking && len(m.activeSpeakers) == 1 {
		m.mu.Unlock()
		return nil
	}
	if speaking {
		if !preserveOthers {
			m.activeSpeakers = map[string]bool{}
		}
		m.activeSpeakers[userID] = true
		m.activeSpeaker = userID
	} else {
		delete(m.activeSpeakers, userID)
		if m.activeSpeaker == userID {
			m.activeSpeaker = anyActiveSpeaker(m.activeSpeakers)
		}
	}
	m.participantStateRevision++
	stateRevision := m.participantStateRevision
	m.lastEventAt = time.Now().UTC()
	job := m.current
	displayName := ""
	if participant, ok := m.participants[userID]; ok {
		displayName = participant.Username
	}
	participants := m.participantsSnapshotLocked()
	m.mu.Unlock()
	m.reportParticipantsIfCurrent(job, participants, stateRevision, 0, false)
	m.reportActiveSpeakerIfCurrent(job, userID, displayName, speaking, stateRevision)
	return nil
}

func (m *Manager) clearSpeakerStates(streamID string) error {
	m.mu.Lock()
	if m.current.StreamID == "" {
		m.mu.Unlock()
		return errors.New("no active stream job")
	}
	if streamID != "" && streamID != m.current.StreamID {
		m.mu.Unlock()
		return errors.New("stream_id does not match current job")
	}
	if len(m.activeSpeakers) == 0 {
		m.mu.Unlock()
		return nil
	}
	m.activeSpeakers = map[string]bool{}
	m.activeSpeaker = ""
	m.participantStateRevision++
	stateRevision := m.participantStateRevision
	m.lastEventAt = time.Now().UTC()
	job := m.current
	participants := m.participantsSnapshotLocked()
	m.mu.Unlock()
	m.reportParticipantsIfCurrent(job, participants, stateRevision, 0, false)
	m.reportActiveSpeakerIfCurrent(job, "", "", false, stateRevision)
	return nil
}

func (m *Manager) reportActiveSpeakerIfCurrent(job discord.VoiceJob, userID, displayName string, speaking bool, stateRevision uint64) {
	m.participantReportMu.Lock()
	defer m.participantReportMu.Unlock()

	m.mu.Lock()
	current := m.current
	if current.StreamID != job.StreamID || current.GuildID != job.GuildID || current.VoiceChannelID != job.VoiceChannelID || m.participantStateRevision != stateRevision {
		m.mu.Unlock()
		return
	}
	reporter := m.reporter
	m.mu.Unlock()
	if reporter == nil {
		return
	}
	var err error
	if stateReporter, ok := reporter.(ActiveSpeakerStateReporter); ok {
		err = stateReporter.ActiveSpeakerStateChanged(job, userID, displayName, speaking)
	} else if speaking {
		err = reporter.ActiveSpeakerChanged(job, userID, displayName)
	}
	if err != nil {
		m.recordWorkerPublishFailure("overlay.active_speaker", job.StreamID, err)
	}
}

func (m *Manager) participantsSnapshotLocked() []Participant {
	out := make([]Participant, 0, len(m.participants))
	for _, participant := range m.participants {
		participant.Speaking = m.activeSpeakers[participant.UserID]
		out = append(out, participant)
	}
	return out
}

func anyActiveSpeaker(active map[string]bool) string {
	for userID, speaking := range active {
		if speaking {
			return userID
		}
	}
	return ""
}

func metricsFromStatus(status discord.Status, participantCount int) map[string]float64 {
	metrics := map[string]float64{
		"discord.gateway_connected":               boolMetric(status.Connected),
		"discord.voice_connected":                 boolMetric(status.VoiceConnected),
		"discord.audio_forward_enabled":           boolMetric(status.AudioForwardEnabled),
		"discord.audio_forward_active":            boolMetric(status.AudioForwardActive),
		"discord.caption_audio_forward_active":    boolMetric(status.CaptionAudioForwardActive),
		"discord.audio_receiving":                 boolMetric(status.AudioReceiving),
		"discord.participant_count":               float64(participantCount),
		"discord.audio_packets_total":             float64(status.AudioPacketsReceived),
		"discord.audio_forwarded_total":           float64(status.AudioPacketsForwarded),
		"discord.audio_forward_errors_total":      float64(status.AudioForwardErrors),
		"discord.caption_packets_forwarded_total": float64(status.CaptionPacketsForwarded),
		"discord.caption_forward_errors_total":    float64(status.CaptionForwardErrors),
		"discord.reconnect_count":                 float64(status.GatewayReconnectCount),
		"discord.voice_disconnect_count":          float64(status.VoiceDisconnectCount),
	}
	if status.LastAudioAgeSec > 0 {
		metrics["discord.audio_last_packet_age_sec"] = status.LastAudioAgeSec
	}
	if status.LastForwardAgeSec > 0 {
		metrics["discord.audio_last_forward_age_sec"] = status.LastForwardAgeSec
	}
	return metrics
}

func shouldRejoinVoice(reason string, job discord.VoiceJob, policy ReconnectPolicy) bool {
	if !policy.Enabled || job.StreamID == "" || reason == "gateway_disconnect" {
		return false
	}
	return true
}

func (m *Manager) rejoinVoiceWithBackoff(job discord.VoiceJob, policy ReconnectPolicy, generation int64) {
	for attempt := 1; attempt <= policy.MaxAttempts; attempt++ {
		if delay := reconnectDelay(policy, attempt); delay > 0 {
			time.Sleep(delay)
		}
		m.mu.Lock()
		current := m.current
		if generation != m.reconnectGeneration || current.StreamID != job.StreamID {
			m.mu.Unlock()
			return
		}
		m.voiceRejoinAttempts++
		m.mu.Unlock()
		if err := m.voice.JoinVoice(job); err == nil {
			m.mu.Lock()
			stillCurrent := generation == m.reconnectGeneration && m.current.StreamID == job.StreamID
			var participantSyncContext context.Context
			if stillCurrent {
				m.lastEventAt = time.Now().UTC()
				participantSyncContext = m.restartParticipantSyncLocked()
			}
			m.mu.Unlock()
			if stillCurrent {
				m.hydrateVoiceParticipants(job, false)
				go m.keepVoiceParticipantsSynced(participantSyncContext, job, generation)
			}
			return
		}
		m.mu.Lock()
		if generation != m.reconnectGeneration || m.current.StreamID != job.StreamID {
			m.mu.Unlock()
			return
		}
		m.voiceRejoinFailures++
		m.lastEventAt = time.Now().UTC()
		m.mu.Unlock()
	}
}

func reconnectDelay(policy ReconnectPolicy, attempt int) time.Duration {
	delay := policy.BaseDelay
	if delay <= 0 || attempt <= 1 {
		return delay
	}
	for i := 1; i < attempt; i++ {
		delay *= 2
		if policy.MaxDelay > 0 && delay > policy.MaxDelay {
			return policy.MaxDelay
		}
	}
	return delay
}

func boolMetric(value bool) float64 {
	if value {
		return 1
	}
	return 0
}
