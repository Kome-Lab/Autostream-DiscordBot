package discord

import (
	"log"
	"strings"

	"github.com/cartridge-gg/discordgo"
)

func (c *RealClient) Connect() error {
	c.mu.Lock()
	alreadyConnected := c.status.Connected
	c.mu.Unlock()
	if alreadyConnected {
		return nil
	}
	if err := c.session.Open(); err != nil {
		c.setLastError(err.Error())
		return err
	}
	c.mu.Lock()
	c.status.Connected = true
	c.status.LastError = ""
	sink := c.sink
	c.mu.Unlock()
	if sink != nil {
		sink.DiscordConnected()
	}
	return nil
}

func (c *RealClient) onGatewayDisconnect(_ *discordgo.Session, _ *discordgo.Disconnect) {
	c.mu.Lock()
	c.status.Connected = false
	sink := c.sink
	job := c.job
	voiceGeneration := c.voiceGeneration
	c.mu.Unlock()
	logGatewayDiagnostic("disconnect", job, voiceGeneration)
	if sink != nil {
		sink.DiscordDisconnected("gateway_disconnect")
	}
}

func (c *RealClient) onReady(session *discordgo.Session, _ *discordgo.Ready) {
	c.mu.Lock()
	c.status.Connected = true
	c.status.LastError = ""
	sink := c.sink
	job := c.job
	voiceGeneration := c.voiceGeneration
	c.mu.Unlock()
	logGatewayDiagnostic("ready", job, voiceGeneration)
	if sink != nil {
		sink.DiscordConnected()
	}
	// READY is the non-resumable reconnect counterpart to RESUMED. Its guild
	// cache may still be filling, so this best-effort snapshot is followed by
	// the authoritative GUILD_CREATE path below.
	c.syncCurrentVoiceParticipants(session)
	c.syncAutoStartVoiceMembers(session)
}

func (c *RealClient) onGatewayResumed(session *discordgo.Session, _ *discordgo.Resumed) {
	c.mu.Lock()
	c.status.Connected = true
	c.status.GatewayReconnectCount++
	c.status.LastError = ""
	sink := c.sink
	job := c.job
	voiceGeneration := c.voiceGeneration
	c.mu.Unlock()
	logGatewayDiagnostic("resumed", job, voiceGeneration)
	if sink != nil {
		sink.DiscordConnected()
	}
	c.syncCurrentVoiceParticipants(session)
	c.syncAutoStartVoiceMembers(session)
}

// onGuildCreate is also emitted while DiscordGo is rebuilding State after a
// non-resumable gateway reconnect. Unlike Ready, this event carries a complete
// guild snapshot, so an empty target channel is authoritative rather than a
// transient cache gap.
func (c *RealClient) onGuildCreate(session *discordgo.Session, event *discordgo.GuildCreate) {
	if event == nil || event.Guild == nil {
		return
	}
	c.syncAutoStartVoiceMembers(session)
	c.mu.Lock()
	job := c.job
	c.mu.Unlock()
	if strings.TrimSpace(job.StreamID) == "" || event.Guild.ID != job.GuildID {
		return
	}
	c.syncVoiceParticipants(session, job)
}

func (c *RealClient) onVoiceStateUpdate(session *discordgo.Session, event *discordgo.VoiceStateUpdate) {
	if event == nil || event.VoiceState == nil {
		return
	}
	currentVoiceChannelID, currentVoiceStateKnown := currentTrackedVoiceChannel(session, event.GuildID, event.UserID)
	c.mu.Lock()
	job := c.job
	sink := c.sink
	voiceGeneration := c.voiceGeneration
	c.mu.Unlock()
	decision := "ignored"
	defer func() {
		logVoiceStateDiagnostic(job, event, currentVoiceChannelID, currentVoiceStateKnown, voiceGeneration, decision)
	}()
	selfUserID := sessionUserID(session)
	// While no job is active, this event is the auto-start trigger itself. The
	// DiscordGo state cache can still be one update behind (or briefly omit the
	// user while a guild is being rebuilt), so do not discard a real join just
	// because the snapshot is non-authoritative at this point. Once a job is
	// active, retain the current-state fence so delayed joins cannot resurrect a
	// participant that has already left.
	currentStateAcceptsJoin := job.StreamID == "" || !currentVoiceStateKnown || currentVoiceChannelID == event.ChannelID
	if sink != nil && event.ChannelID != "" && event.UserID != "" && event.UserID != selfUserID && (event.BeforeUpdate == nil || event.BeforeUpdate.ChannelID != event.ChannelID) && currentStateAcceptsJoin {
		if job.StreamID != "" {
			decision = "auto_start_join_candidate"
		}
		sink.VoiceUserJoined(VoiceJoinEvent{
			GuildID:        event.GuildID,
			VoiceChannelID: event.ChannelID,
			UserID:         event.UserID,
		})
	}
	if job.StreamID == "" || event.GuildID != job.GuildID {
		return
	}
	if event.UserID == selfUserID && event.BeforeUpdate != nil && event.BeforeUpdate.ChannelID == job.VoiceChannelID && event.ChannelID != job.VoiceChannelID && (!currentVoiceStateKnown || currentVoiceChannelID != job.VoiceChannelID) {
		decision = "self_voice_disconnect"
		c.markVoiceDisconnected("voice_state_disconnected", true)
		return
	}
	if sink == nil {
		decision = "ignored_no_sink"
		return
	}
	// The bot itself is present in its voice channel, but must never count as
	// a participant. Counting it prevents the empty-channel auto-stop path.
	if event.UserID == selfUserID {
		decision = "ignored_self"
		return
	}
	// A VoiceStateUpdate is authoritative only for the user carried by that
	// event. DiscordGo's guild VoiceStates cache can be temporarily incomplete
	// while a guild is rebuilt, so replacing the entire participant set here can
	// erase unrelated users and incorrectly trigger empty-VC auto-stop. Full
	// snapshots remain the convergence path for startup, reconnect, and the
	// bounded periodic sync loop.
	if event.ChannelID == job.VoiceChannelID {
		if currentVoiceStateKnown && currentVoiceChannelID != job.VoiceChannelID {
			decision = "ignored_current_state_fence"
			return
		}
		participant := voiceParticipantFromState(job.GuildID, event.VoiceState)
		decision = "participant_join_published"
		sink.ParticipantChanged(ParticipantEvent{
			StreamID:       job.StreamID,
			GuildID:        job.GuildID,
			VoiceChannelID: job.VoiceChannelID,
			UserID:         event.UserID,
			Username:       participant.Username,
			AvatarURL:      participant.AvatarURL,
			IsBot:          participant.IsBot,
			Present:        true,
		})
		return
	}
	if event.BeforeUpdate != nil && event.BeforeUpdate.ChannelID == job.VoiceChannelID {
		if currentVoiceStateKnown && currentVoiceChannelID == job.VoiceChannelID {
			decision = "ignored_current_state_fence"
			return
		}
		decision = "participant_leave_published"
		sink.ParticipantChanged(ParticipantEvent{
			StreamID:       job.StreamID,
			GuildID:        job.GuildID,
			VoiceChannelID: job.VoiceChannelID,
			UserID:         event.UserID,
			Present:        false,
		})
		return
	}
	decision = "ignored_not_target_transition"
}

// logVoiceStateDiagnostic records only Discord identifiers and state fences.
// It intentionally excludes member payloads, URLs, tokens, and authorization
// data so a false empty-participant transition can be reconstructed safely.
func logVoiceStateDiagnostic(job VoiceJob, event *discordgo.VoiceStateUpdate, currentChannelID string, currentStateKnown bool, voiceGeneration uint64, decision string) {
	if event == nil || strings.TrimSpace(job.StreamID) == "" || strings.TrimSpace(job.GuildID) == "" || event.GuildID != job.GuildID {
		return
	}
	beforeChannelID := ""
	if event.BeforeUpdate != nil {
		beforeChannelID = strings.TrimSpace(event.BeforeUpdate.ChannelID)
	}
	if beforeChannelID != strings.TrimSpace(job.VoiceChannelID) && event.ChannelID != strings.TrimSpace(job.VoiceChannelID) {
		return
	}
	if strings.TrimSpace(decision) == "" {
		decision = "unknown"
	}
	log.Printf("Discord voice state diagnostic: event=voice_state_update stream_id=%s job_generation=%d voice_generation=%d guild_id=%s user_id=%s before_channel_id=%s event_channel_id=%s current_channel_id=%s current_state_known=%t target_channel_id=%s decision=%s", strings.TrimSpace(job.StreamID), job.JobGeneration, voiceGeneration, strings.TrimSpace(event.GuildID), strings.TrimSpace(event.UserID), beforeChannelID, strings.TrimSpace(event.ChannelID), strings.TrimSpace(currentChannelID), currentStateKnown, strings.TrimSpace(job.VoiceChannelID), decision)
}

func logGatewayDiagnostic(event string, job VoiceJob, voiceGeneration uint64) {
	log.Printf("Discord gateway diagnostic: event=%s stream_id=%s job_generation=%d voice_generation=%d", strings.TrimSpace(event), strings.TrimSpace(job.StreamID), job.JobGeneration, voiceGeneration)
}
