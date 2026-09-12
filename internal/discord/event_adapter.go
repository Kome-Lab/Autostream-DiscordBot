package discord

import (
	"sort"
	"strings"
	"time"

	"github.com/cartridge-gg/discordgo"
)

func (c *RealClient) onMessageCreate(session *discordgo.Session, event *discordgo.MessageCreate) {
	if event == nil || event.Message == nil || event.Author == nil {
		return
	}
	if event.Author.ID == sessionUserID(session) {
		return
	}
	c.mu.Lock()
	job := c.job
	sink := c.sink
	c.mu.Unlock()
	if sink == nil || strings.TrimSpace(job.StreamID) == "" || strings.TrimSpace(job.TextChannelID) == "" {
		return
	}
	if event.ChannelID != job.TextChannelID || event.GuildID != job.GuildID {
		return
	}
	content := strings.TrimSpace(event.Content)
	if content == "" {
		return
	}
	createdAt := time.Now().UTC()
	if !event.Timestamp.IsZero() {
		createdAt = time.Time(event.Timestamp).UTC()
	}
	sink.ChatMessageReceived(ChatMessageEvent{
		StreamID:      job.StreamID,
		GuildID:       job.GuildID,
		TextChannelID: job.TextChannelID,
		MessageID:     event.ID,
		UserID:        event.Author.ID,
		Username:      discordMessageDisplayName(event.Message),
		AvatarURL:     strings.TrimSpace(event.Author.AvatarURL("128")),
		IsBot:         event.Author.Bot,
		Content:       content,
		CreatedAt:     createdAt,
	})
}

func discordMessageDisplayName(message *discordgo.Message) string {
	if message == nil || message.Author == nil {
		return ""
	}
	if message.Member != nil {
		if nickname := strings.TrimSpace(message.Member.Nick); nickname != "" {
			return nickname
		}
	}
	if globalName := strings.TrimSpace(message.Author.GlobalName); globalName != "" {
		return globalName
	}
	return strings.TrimSpace(message.Author.Username)
}

// SnapshotVoiceParticipants returns one current, authoritative view of a
// joined job's target VC. A missing guild or disabled voice State is explicitly
// non-authoritative: callers must retain their previous view rather than
// mistaking a cache gap for an empty channel.
func (c *RealClient) SnapshotVoiceParticipants(job VoiceJob) (ParticipantSnapshot, bool) {
	c.participantSyncMu.Lock()
	defer c.participantSyncMu.Unlock()

	c.mu.Lock()
	current := c.job
	session := c.session
	c.mu.Unlock()
	if !sameVoiceJob(current, job) {
		return ParticipantSnapshot{}, false
	}
	return c.snapshotVoiceParticipantsLocked(session, current)
}

func (c *RealClient) syncCurrentVoiceParticipants(session *discordgo.Session) {
	c.mu.Lock()
	job := c.job
	c.mu.Unlock()
	if strings.TrimSpace(job.StreamID) == "" {
		return
	}
	c.syncVoiceParticipants(session, job)
}

func (c *RealClient) syncVoiceParticipants(session *discordgo.Session, expected VoiceJob) {
	c.participantSyncMu.Lock()
	defer c.participantSyncMu.Unlock()

	c.mu.Lock()
	job := c.job
	sink := c.sink
	c.mu.Unlock()
	if !sameVoiceJob(job, expected) {
		return
	}
	snapshotSink, ok := sink.(ParticipantSnapshotSink)
	if !ok {
		return
	}
	if session == nil {
		session = c.session
	}
	snapshot, ok := c.snapshotVoiceParticipantsLocked(session, job)
	if !ok {
		return
	}
	snapshotSink.ParticipantsSynced(snapshot)
}

// snapshotVoiceParticipantsLocked must be called while participantSyncMu is
// held. It takes DiscordGo's State read lock only while copying the small,
// target-VC subset, then invokes no sink while that State lock is held.
func (c *RealClient) snapshotVoiceParticipantsLocked(session *discordgo.Session, job VoiceJob) (ParticipantSnapshot, bool) {
	if session == nil || session.State == nil || !session.State.TrackVoice {
		return ParticipantSnapshot{}, false
	}
	job.StreamID = strings.TrimSpace(job.StreamID)
	job.GuildID = strings.TrimSpace(job.GuildID)
	job.VoiceChannelID = strings.TrimSpace(job.VoiceChannelID)
	if job.StreamID == "" || job.GuildID == "" || job.VoiceChannelID == "" {
		return ParticipantSnapshot{}, false
	}

	state := session.State
	state.RLock()
	defer state.RUnlock()
	selfUserID := ""
	if state.User != nil {
		selfUserID = strings.TrimSpace(state.User.ID)
	}
	if selfUserID == "" {
		return ParticipantSnapshot{}, false
	}
	for _, guild := range state.Guilds {
		if guild == nil || guild.ID != job.GuildID {
			continue
		}
		participants := make([]VoiceParticipant, 0, len(guild.VoiceStates))
		for _, voiceState := range guild.VoiceStates {
			if voiceState == nil || voiceState.ChannelID != job.VoiceChannelID {
				continue
			}
			participant := voiceParticipantFromState(guild.ID, voiceState)
			userID := participant.UserID
			if userID == "" || userID == selfUserID {
				continue
			}
			participants = append(participants, participant)
		}
		sort.Slice(participants, func(i, j int) bool {
			return participants[i].UserID < participants[j].UserID
		})
		c.participantSnapshotNext++
		return ParticipantSnapshot{
			StreamID:       job.StreamID,
			GuildID:        job.GuildID,
			VoiceChannelID: job.VoiceChannelID,
			Participants:   participants,
			Revision:       c.participantSnapshotNext,
		}, true
	}
	return ParticipantSnapshot{}, false
}

func voiceParticipantFromState(guildID string, voiceState *discordgo.VoiceState) VoiceParticipant {
	if voiceState == nil {
		return VoiceParticipant{}
	}
	participant := VoiceParticipant{UserID: strings.TrimSpace(voiceState.UserID)}
	if voiceState.Member == nil || voiceState.Member.User == nil {
		return participant
	}
	member := voiceState.Member
	user := member.User
	participant.Username = strings.TrimSpace(member.Nick)
	if participant.Username == "" {
		participant.Username = strings.TrimSpace(user.GlobalName)
	}
	if participant.Username == "" {
		participant.Username = strings.TrimSpace(user.Username)
	}
	memberForAvatar := *member
	if strings.TrimSpace(memberForAvatar.GuildID) == "" {
		memberForAvatar.GuildID = strings.TrimSpace(guildID)
	}
	participant.AvatarURL = strings.TrimSpace(memberForAvatar.AvatarURL("128"))
	if participant.AvatarURL == "" {
		participant.AvatarURL = strings.TrimSpace(user.AvatarURL("128"))
	}
	participant.IsBot = user.Bot
	return participant
}
