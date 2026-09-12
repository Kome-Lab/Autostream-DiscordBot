package discord

import (
	"strings"

	"github.com/cartridge-gg/discordgo"
)

func (c *RealClient) SetAutoStartVoiceTargets(targets []AutoStartVoiceTarget) {
	normalized := make([]AutoStartVoiceTarget, 0, len(targets))
	seen := make(map[string]struct{}, len(targets))
	for _, target := range targets {
		target.StreamID = strings.TrimSpace(target.StreamID)
		target.GuildID = strings.TrimSpace(target.GuildID)
		target.VoiceChannelID = strings.TrimSpace(target.VoiceChannelID)
		if target.StreamID == "" || target.GuildID == "" || target.VoiceChannelID == "" {
			continue
		}
		key := target.StreamID + "\x00" + target.GuildID + "\x00" + target.VoiceChannelID
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		normalized = append(normalized, target)
	}

	c.mu.Lock()
	c.autoStartTargets = normalized
	session := c.session
	c.mu.Unlock()
	c.syncAutoStartVoiceMembers(session)
}

// syncAutoStartVoiceMembers emits at most one synthetic join event per
// configured target when a human is already present in that VC. It never calls
// the event sink while DiscordGo's State lock is held, and the manager's
// existing pending/start fences make a repeated Gateway or runtime refresh
// harmless.
func (c *RealClient) syncAutoStartVoiceMembers(session *discordgo.Session) {
	c.mu.Lock()
	targets := append([]AutoStartVoiceTarget(nil), c.autoStartTargets...)
	sink := c.sink
	if session == nil {
		session = c.session
	}
	c.mu.Unlock()
	if sink == nil || session == nil || session.State == nil || !session.State.TrackVoice || len(targets) == 0 {
		return
	}

	selfUserID := sessionUserID(session)
	if selfUserID == "" {
		return
	}
	events := make([]VoiceJoinEvent, 0, len(targets))
	state := session.State
	state.RLock()
	for _, target := range targets {
		for _, guild := range state.Guilds {
			if guild == nil || guild.ID != target.GuildID {
				continue
			}
			for _, voiceState := range guild.VoiceStates {
				if voiceState == nil || voiceState.ChannelID != target.VoiceChannelID {
					continue
				}
				userID := strings.TrimSpace(voiceState.UserID)
				if userID == "" || userID == selfUserID {
					continue
				}
				if voiceState.Member != nil && voiceState.Member.User != nil && voiceState.Member.User.Bot {
					continue
				}
				username := ""
				if voiceState.Member != nil && voiceState.Member.User != nil {
					username = strings.TrimSpace(voiceState.Member.Nick)
					if username == "" {
						username = strings.TrimSpace(voiceState.Member.User.GlobalName)
					}
					if username == "" {
						username = strings.TrimSpace(voiceState.Member.User.Username)
					}
				}
				events = append(events, VoiceJoinEvent{
					GuildID:        target.GuildID,
					VoiceChannelID: target.VoiceChannelID,
					UserID:         userID,
					Username:       username,
				})
				break
			}
			break
		}
	}
	state.RUnlock()

	for _, event := range events {
		sink.VoiceUserJoined(event)
	}
}

func sessionUserID(session *discordgo.Session) string {
	if session == nil || session.State == nil || session.State.User == nil {
		return ""
	}
	return session.State.User.ID
}

// currentTrackedVoiceChannel snapshots a user's latest voice channel while
// holding discordgo's State lock. discordgo updates State in Gateway order
// before starting typed handlers, which run asynchronously by default. The
// snapshot therefore lets each target-VC transition reject only an obsolete
// enter or leave without dropping a still-needed leave from that target.
//
// State.VoiceState is intentionally not used here: it unlocks after resolving
// the guild and then scans VoiceStates, which can race a later Gateway update.
// An unavailable snapshot is non-authoritative and callers retain the event to
// avoid falsely treating a present participant as absent.
func currentTrackedVoiceChannel(session *discordgo.Session, guildID, userID string) (string, bool) {
	if session == nil || session.State == nil || !session.State.TrackVoice {
		return "", false
	}
	guildID = strings.TrimSpace(guildID)
	userID = strings.TrimSpace(userID)
	if guildID == "" || userID == "" {
		return "", false
	}
	state := session.State
	state.RLock()
	defer state.RUnlock()
	for _, guild := range state.Guilds {
		if guild.ID != guildID {
			continue
		}
		for _, voiceState := range guild.VoiceStates {
			if voiceState.UserID == userID {
				return voiceState.ChannelID, true
			}
		}
		return "", true
	}
	return "", false
}
