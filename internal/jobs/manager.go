package jobs

import (
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/example/autostream-discord-bot/internal/discord"
)

type Manager struct {
	voice               discord.Client
	reporter            EventReporter
	streamStarter       StreamStarter
	mu                  sync.Mutex
	current             discord.VoiceJob
	defaults            VoiceDefaults
	streamDefaults      map[string]VoiceDefaults
	autoStartPending    map[string]time.Time
	autoStartCooldown   time.Duration
	reconnectPolicy     ReconnectPolicy
	reconnectGeneration int64
	startedAt           time.Time
	participants        map[string]Participant
	activeSpeaker       string
	lastEventAt         time.Time
	workerFailures      int64
	voiceRejoinAttempts int64
	voiceRejoinFailures int64
}

type VoiceDefaults struct {
	GuildID          string
	VoiceChannelID   string
	TextChannelID    string
	CaptionAudioURL  string
	AutoStartEnabled bool
}

type ReconnectPolicy struct {
	Enabled     bool
	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
}

type EventReporter interface {
	ParticipantsChanged(streamID string, participants []Participant) error
	ActiveSpeakerChanged(streamID, userID, displayName string) error
	ChatMessageReceived(streamID string, message ChatMessage) error
}

type StreamStarter interface {
	StartStream(streamID string) error
}

type Participant struct {
	UserID   string    `json:"user_id"`
	Username string    `json:"username,omitempty"`
	JoinedAt time.Time `json:"joined_at"`
}

type ChatMessage struct {
	MessageID     string    `json:"message_id"`
	UserID        string    `json:"user_id"`
	Username      string    `json:"username,omitempty"`
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
	return &Manager{voice: voice, reporter: reporter, participants: map[string]Participant{}, streamDefaults: map[string]VoiceDefaults{}, autoStartPending: map[string]time.Time{}, autoStartCooldown: 30 * time.Second}
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
	m.mu.Unlock()

	if err := m.voice.JoinVoice(job); err != nil {
		return err
	}

	m.mu.Lock()
	m.reconnectGeneration++
	defer m.mu.Unlock()
	m.current = job
	now := time.Now().UTC()
	m.startedAt = now
	m.lastEventAt = now
	m.participants = map[string]Participant{}
	m.activeSpeaker = ""
	delete(m.autoStartPending, job.StreamID)
	return nil
}

func (m *Manager) SetStreamStarter(starter StreamStarter) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.streamStarter = starter
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
		CaptionAudioURL:  strings.TrimSpace(defaults.CaptionAudioURL),
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
			CaptionAudioURL:  strings.TrimSpace(item.CaptionAudioURL),
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
	if strings.TrimSpace(job.CaptionAudioURL) == "" {
		job.CaptionAudioURL = defaults.CaptionAudioURL
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
	if override.CaptionAudioURL != "" {
		base.CaptionAudioURL = override.CaptionAudioURL
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
	m.mu.Unlock()

	if err := m.voice.LeaveVoice(currentStreamID); err != nil {
		return err
	}

	m.mu.Lock()
	m.reconnectGeneration++
	defer m.mu.Unlock()
	m.current = discord.VoiceJob{}
	m.startedAt = time.Time{}
	m.lastEventAt = time.Now().UTC()
	m.participants = map[string]Participant{}
	m.activeSpeaker = ""
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
		job.StreamIngestToken = ""
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

func (m *Manager) recordWorkerPublishFailure() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.workerFailures += 1
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
		m.participants[event.UserID] = Participant{UserID: event.UserID, Username: event.Username, JoinedAt: joinedAt}
	} else {
		delete(m.participants, event.UserID)
		if m.activeSpeaker == event.UserID {
			m.activeSpeaker = ""
		}
	}
	m.lastEventAt = now
	streamID := m.current.StreamID
	participants := m.participantsSnapshotLocked()
	reporter := m.reporter
	m.mu.Unlock()
	if reporter != nil {
		if err := reporter.ParticipantsChanged(streamID, participants); err != nil {
			m.recordWorkerPublishFailure()
		}
	}
}

func (m *Manager) VoiceUserJoined(event discord.VoiceJoinEvent) {
	if strings.TrimSpace(event.GuildID) == "" || strings.TrimSpace(event.VoiceChannelID) == "" || strings.TrimSpace(event.UserID) == "" {
		return
	}
	now := time.Now().UTC()
	m.mu.Lock()
	if m.current.StreamID != "" {
		m.mu.Unlock()
		return
	}
	streamID := m.matchingAutoStartStreamLocked(event.GuildID, event.VoiceChannelID)
	if streamID == "" || m.streamStarter == nil {
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
		_ = starter.StartStream(streamID)
	}()
}

func (m *Manager) ChatMessageReceived(event discord.ChatMessageEvent) {
	content := trimDiscordChatContent(event.Content)
	if content == "" {
		return
	}
	m.mu.Lock()
	if m.current.StreamID == "" || event.StreamID != m.current.StreamID || event.GuildID != m.current.GuildID || event.TextChannelID != m.current.TextChannelID {
		m.mu.Unlock()
		return
	}
	streamID := m.current.StreamID
	reporter := m.reporter
	m.lastEventAt = time.Now().UTC()
	message := ChatMessage{
		MessageID:     strings.TrimSpace(event.MessageID),
		UserID:        strings.TrimSpace(event.UserID),
		Username:      strings.TrimSpace(event.Username),
		Content:       content,
		TextChannelID: strings.TrimSpace(event.TextChannelID),
		CreatedAt:     event.CreatedAt,
	}
	if message.CreatedAt.IsZero() {
		message.CreatedAt = time.Now().UTC()
	}
	m.mu.Unlock()
	if reporter != nil {
		if err := reporter.ChatMessageReceived(streamID, message); err != nil {
			m.recordWorkerPublishFailure()
		}
	}
}

func trimDiscordChatContent(content string) string {
	content = strings.TrimSpace(content)
	runes := []rune(content)
	if len(runes) <= 1000 {
		return content
	}
	return strings.TrimSpace(string(runes[:1000]))
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
	defer m.mu.Unlock()
	m.lastEventAt = time.Now().UTC()
}

func (m *Manager) DiscordDisconnected(reason string) {
	m.mu.Lock()
	m.lastEventAt = time.Now().UTC()
	job := m.current
	policy := m.reconnectPolicy
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

func (m *Manager) SetActiveSpeaker(streamID, userID string) error {
	m.mu.Lock()
	if m.current.StreamID == "" {
		m.mu.Unlock()
		return errors.New("no active stream job")
	}
	if streamID != "" && streamID != m.current.StreamID {
		m.mu.Unlock()
		return errors.New("stream_id does not match current job")
	}
	if userID != "" {
		if _, ok := m.participants[userID]; !ok {
			m.mu.Unlock()
			return errors.New("active speaker must be an active participant")
		}
	}
	if m.activeSpeaker == userID {
		m.mu.Unlock()
		return nil
	}
	m.activeSpeaker = userID
	m.lastEventAt = time.Now().UTC()
	currentStreamID := m.current.StreamID
	displayName := ""
	if participant, ok := m.participants[userID]; ok {
		displayName = participant.Username
	}
	reporter := m.reporter
	m.mu.Unlock()
	if reporter != nil {
		if err := reporter.ActiveSpeakerChanged(currentStreamID, userID, displayName); err != nil {
			m.recordWorkerPublishFailure()
		}
	}
	return nil
}

func (m *Manager) participantsSnapshotLocked() []Participant {
	out := make([]Participant, 0, len(m.participants))
	for _, participant := range m.participants {
		out = append(out, participant)
	}
	return out
}

func metricsFromStatus(status discord.Status, participantCount int) map[string]float64 {
	metrics := map[string]float64{
		"discord.gateway_connected":          boolMetric(status.Connected),
		"discord.voice_connected":            boolMetric(status.VoiceConnected),
		"discord.audio_forward_enabled":      boolMetric(status.AudioForwardEnabled),
		"discord.audio_forward_active":       boolMetric(status.AudioForwardActive),
		"discord.audio_receiving":            boolMetric(status.AudioReceiving),
		"discord.participant_count":          float64(participantCount),
		"discord.audio_packets_total":        float64(status.AudioPacketsReceived),
		"discord.audio_forwarded_total":      float64(status.AudioPacketsForwarded),
		"discord.audio_forward_errors_total": float64(status.AudioForwardErrors),
		"discord.reconnect_count":            float64(status.GatewayReconnectCount),
		"discord.voice_disconnect_count":     float64(status.VoiceDisconnectCount),
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
			if generation == m.reconnectGeneration && m.current.StreamID == job.StreamID {
				m.lastEventAt = time.Now().UTC()
			}
			m.mu.Unlock()
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
