package discord

import (
	"strings"

	"github.com/cartridge-gg/discordgo"
)

// A present member payload is authoritative for this event: malformed or bot
// identities must never fall back to a cached human with the same voice ID.
func verifiedVoiceMember(member *discordgo.Member, guildID, userID string) (*discordgo.Member, bool) {
	if member == nil {
		return nil, false
	}
	if member.User == nil || member.User.ID != userID || (member.GuildID != "" && member.GuildID != guildID) {
		return nil, true
	}
	return member, true
}

// The caller holds State's read lock; only this session's matching guild is used.
func currentVoiceMemberLocked(guild *discordgo.Guild, userID string, member *discordgo.Member) (*discordgo.Member, bool) {
	if member != nil {
		return verifiedVoiceMember(member, guild.ID, userID)
	}
	for _, cached := range guild.Members {
		if cached != nil && cached.User != nil && cached.User.ID == userID {
			return verifiedVoiceMember(cached, guild.ID, userID)
		}
	}
	return nil, false
}

func currentAutoStartMember(session *discordgo.Session, event *discordgo.VoiceState) (*discordgo.Member, bool) {
	if event.Member != nil {
		return verifiedVoiceMember(event.Member, event.GuildID, event.UserID)
	}
	if session == nil || session.State == nil {
		return nil, false
	}
	state := session.State
	state.RLock()
	defer state.RUnlock()
	for _, guild := range state.Guilds {
		if guild == nil || guild.ID != event.GuildID {
			continue
		}
		for _, voice := range guild.VoiceStates {
			if voice != nil && voice.UserID == event.UserID && voice.Member != nil {
				return currentVoiceMemberLocked(guild, event.UserID, voice.Member)
			}
		}
		return currentVoiceMemberLocked(guild, event.UserID, nil)
	}
	return nil, false
}

type pendingAutoStartJoin struct{ StreamID, AssignmentEpoch, GuildID, VoiceChannelID, UserID string }

func (c *RealClient) rememberUnknownAutoStartJoin(session *discordgo.Session, event *discordgo.VoiceState, observedTargets []AutoStartVoiceTarget) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if session == nil || session != c.session {
		return
	}
	if c.unknownAutoStartJoins == nil {
		c.unknownAutoStartJoins = make(map[pendingAutoStartJoin]*discordgo.Session)
	}
	for _, target := range observedTargets {
		current := false
		for _, selected := range c.autoStartTargets {
			if selected == target {
				current = true
				break
			}
		}
		if current && target.GuildID == event.GuildID && target.VoiceChannelID == event.ChannelID && len(c.unknownAutoStartJoins) < 256 {
			key := pendingAutoStartJoin{target.StreamID, target.AssignmentEpoch, target.GuildID, target.VoiceChannelID, event.UserID}
			c.unknownAutoStartJoins[key] = session
		}
	}
}

func (c *RealClient) discardObsoleteAutoStartJoins(session *discordgo.Session, event *discordgo.VoiceState, rejectedIdentity bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if session != c.session {
		return
	}
	for key := range c.unknownAutoStartJoins {
		if key.GuildID == event.GuildID && key.UserID == event.UserID && (rejectedIdentity || key.VoiceChannelID != event.ChannelID) {
			delete(c.unknownAutoStartJoins, key)
		}
	}
}

func (c *RealClient) resolveAutoStartPresence(session *discordgo.Session, event *VoiceJoinEvent) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if session != c.session || event.Authority == nil {
		return
	}
	key := pendingAutoStartJoin{event.Authority.StreamID, event.Authority.AssignmentEpoch, event.GuildID, event.VoiceChannelID, event.UserID}
	if c.unknownAutoStartJoins[key] == session {
		delete(c.unknownAutoStartJoins, key)
		event.ExistingPresence = false
	}
}

func (c *RealClient) onAutoStartMemberAdd(session *discordgo.Session, event *discordgo.GuildMemberAdd) {
	if event != nil {
		c.onAutoStartMemberIdentity(session, event.Member)
	}
}
func (c *RealClient) onAutoStartMemberUpdate(session *discordgo.Session, event *discordgo.GuildMemberUpdate) {
	if event != nil {
		c.onAutoStartMemberIdentity(session, event.Member)
	}
}
func (c *RealClient) onAutoStartMemberIdentity(session *discordgo.Session, member *discordgo.Member) {
	if member == nil || member.User == nil || strings.TrimSpace(member.GuildID) == "" || strings.TrimSpace(member.User.ID) == "" {
		return
	}
	if member.User.Bot {
		c.discardObsoleteAutoStartJoins(session, &discordgo.VoiceState{GuildID: member.GuildID, UserID: member.User.ID}, true)
	}
	c.syncAutoStartVoiceMembers(session)
}
