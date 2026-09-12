package jobs

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/example/autostream-discord-bot/internal/discord"
)

type Participant struct {
	UserID    string    `json:"user_id"`
	Username  string    `json:"username,omitempty"`
	AvatarURL string    `json:"avatar_url,omitempty"`
	IsBot     bool      `json:"is_bot,omitempty"`
	Speaking  bool      `json:"speaking,omitempty"`
	JoinedAt  time.Time `json:"joined_at"`
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
	out = append(out, m.participantsSnapshotLocked()...)
	return out, nil
}

func (m *Manager) ParticipantChanged(event discord.ParticipantEvent) {
	if strings.TrimSpace(event.UserID) == "" {
		return
	}
	m.mu.Lock()
	if m.eventsPausedLocked() || m.current.StreamID == "" || event.StreamID != m.current.StreamID {
		m.mu.Unlock()
		return
	}
	now := time.Now().UTC()
	speakerStopped := false
	speakerDisplayName := ""
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
		if participant, ok := m.participants[event.UserID]; ok {
			speakerDisplayName = participant.Username
		}
		speakerStopped = m.activeSpeakers[event.UserID]
		delete(m.participants, event.UserID)
		delete(m.activeSpeakers, event.UserID)
		if m.activeSpeaker == event.UserID {
			m.activeSpeaker = anyActiveSpeaker(m.activeSpeakers)
		}
	}
	m.lastEventAt = now
	m.participantStateRevision++
	participantStateRevision := m.participantStateRevision
	reconnectGeneration := m.reconnectGeneration
	job := m.current
	participants := m.participantsSnapshotLocked()
	stopper, shouldAutoStop, autoStopObservation := m.updateAutoStopForParticipantSetLocked(job, now, "voice_event", m.participantSnapshotRevision)
	autoStopDelay := m.autoStopDelay
	m.mu.Unlock()
	m.reportParticipantsIfCurrent(job, participants, participantStateRevision, reconnectGeneration, true)
	if speakerStopped {
		m.reportActiveSpeakerIfCurrent(job, event.UserID, speakerDisplayName, false, participantStateRevision, reconnectGeneration)
	}
	if shouldAutoStop {
		logAutoStopObservation("scheduled", job.StreamID, autoStopObservation, autoStopDelay, 0)
		go m.autoStopWhenEmpty(job.StreamID, autoStopObservation, stopper, autoStopDelay)
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
	if m.eventsPausedLocked() || job.StreamID != snapshot.StreamID || job.GuildID != snapshot.GuildID || job.VoiceChannelID != snapshot.VoiceChannelID || (options.requireGeneration && m.reconnectGeneration != options.expectedGeneration) {
		m.mu.Unlock()
		return false
	}
	if options.requireStateRevision && m.participantStateRevision != options.expectedStateRevision {
		participants := m.participantsSnapshotLocked()
		stateRevision := m.participantStateRevision
		reportGeneration := m.reconnectGeneration
		m.mu.Unlock()
		m.reportParticipantsIfCurrent(job, participants, stateRevision, reportGeneration, true)
		return true
	}
	if snapshot.Revision != 0 && snapshot.Revision <= m.participantSnapshotRevision {
		if options.authoritativeReplay && snapshot.Revision == m.participantSnapshotRevision {
			participants := m.participantsSnapshotLocked()
			stateRevision := m.participantStateRevision
			reportGeneration := m.reconnectGeneration
			m.mu.Unlock()
			m.reportParticipantsIfCurrent(job, participants, stateRevision, reportGeneration, true)
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
	reportGeneration := m.reconnectGeneration
	stopper, shouldAutoStop, autoStopObservation := m.updateAutoStopForParticipantSetLocked(job, now, "authoritative_snapshot", snapshot.Revision)
	autoStopDelay := m.autoStopDelay
	m.mu.Unlock()

	m.reportParticipantsIfCurrent(job, participants, stateRevision, reportGeneration, true)
	if shouldAutoStop {
		logAutoStopObservation("scheduled", job.StreamID, autoStopObservation, autoStopDelay, 0)
		go m.autoStopWhenEmpty(job.StreamID, autoStopObservation, stopper, autoStopDelay)
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
	if m.reconnectGeneration != generation || !sameWorkerEventJob(m.current, job) {
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
	return !m.eventsPausedLocked() && sameWorkerEventJob(m.current, job)
}

// eventsPausedLocked is the generation fence for Stop and Discord reconnect
// windows. The caller must hold m.mu.
func (m *Manager) eventsPausedLocked() bool {
	return m.stopping || m.disconnected
}

func (m *Manager) reportParticipantsIfCurrent(job discord.VoiceJob, participants []Participant, stateRevision uint64, generation int64, requireGeneration bool) {
	m.participantReportMu.Lock()
	defer m.participantReportMu.Unlock()

	m.mu.Lock()
	current := m.current
	if m.eventsPausedLocked() || !sameWorkerEventJob(current, job) || (requireGeneration && m.reconnectGeneration != generation) {
		m.mu.Unlock()
		return
	}
	if m.participantStateRevision != stateRevision {
		// Participant and speaking callbacks update state before waiting for the
		// shared publish lock. If one overtakes this report, publish the newest
		// full snapshot instead of dropping the participant card until the next
		// periodic sync. Any newer event is then serialized after this snapshot.
		participants = m.participantsSnapshotLocked()
		stateRevision = m.participantStateRevision
	}
	reporter := m.reporter
	reportGeneration := m.reconnectGeneration
	queue := m.workerRetry
	m.mu.Unlock()
	if reporter != nil {
		ctx := context.Background()
		if queue != nil {
			ctx = queue.ctx
		}
		if err := publishParticipants(ctx, reporter, current, participants); err != nil {
			m.recordWorkerPublishFailure("overlay.participants", job.StreamID, err)
			m.enqueueWorkerEventRetry(workerEventRetryReport{
				key:                 workerParticipantsRetryKey(current),
				eventType:           "overlay.participants",
				job:                 current,
				reconnectGeneration: reportGeneration,
				participants:        append([]Participant(nil), participants...),
				stateRevision:       stateRevision,
				err:                 err,
			})
		} else {
			m.supersedeWorkerEventRetry(current, reportGeneration, workerParticipantsRetryKey(current))
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

func (m *Manager) participantsSnapshotLocked() []Participant {
	out := make([]Participant, 0, len(m.participants))
	for _, participant := range m.participants {
		participant.Speaking = m.activeSpeakers[participant.UserID]
		out = append(out, participant)
	}
	return out
}
