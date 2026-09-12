package discord

import (
	"context"
	"errors"
	"log"
	"strings"

	"github.com/cartridge-gg/discordgo"
	"github.com/example/autostream-discord-bot/internal/davewatch"
)

type discordDAVERecovery struct {
	voice *discordgo.VoiceConnection
}

func (r discordDAVERecovery) Health() davewatch.Health {
	if r.voice == nil {
		return davewatch.Health{}
	}
	health := r.voice.DAVEHealth()
	return davewatch.Health{
		Initialized:         health.Initialized,
		OP26SentAt:          health.OP26SentAt,
		EpochEstablished:    health.EpochEstablished,
		OP30Received:        health.OP30Received,
		LastMissing:         health.LastMissing,
		MissingFirstSeen:    health.MissingFirstSeen,
		ProposalFailedSince: health.ProposalFailedSince,
	}
}

func (r discordDAVERecovery) ResendKeyPackage() error {
	if r.voice == nil {
		return errors.New("DAVE voice connection is unavailable")
	}
	return r.voice.ResendDAVEKeyPackage()
}

func (r discordDAVERecovery) SoftReset() error {
	if r.voice == nil {
		return errors.New("DAVE voice connection is unavailable")
	}
	return r.voice.SoftResetDAVE()
}

func (c *RealClient) watchDAVE(ctx context.Context, job VoiceJob, voice *discordgo.VoiceConnection, voiceGeneration uint64) {
	watchdog := davewatch.New(
		discordDAVERecovery{voice: voice},
		func() bool { return c.remoteVoiceParticipantPresent(job, voice, voiceGeneration) },
		func(event davewatch.Event) { c.recordDAVERecovery(job, voice, voiceGeneration, event) },
		davewatch.Config{},
	)
	watchdog.Run(ctx)
}

func (c *RealClient) remoteVoiceParticipantPresent(job VoiceJob, voice *discordgo.VoiceConnection, voiceGeneration uint64) bool {
	c.mu.Lock()
	if c.voice != voice || c.voiceGeneration != voiceGeneration || !sameVoiceJob(c.job, job) {
		c.mu.Unlock()
		return false
	}
	session := c.session
	c.mu.Unlock()
	if session == nil || session.State == nil || !session.State.TrackVoice {
		return false
	}
	state := session.State
	state.RLock()
	defer state.RUnlock()
	selfUserID := ""
	if state.User != nil {
		selfUserID = strings.TrimSpace(state.User.ID)
	}
	for _, guild := range state.Guilds {
		if guild == nil || guild.ID != job.GuildID {
			continue
		}
		for _, voiceState := range guild.VoiceStates {
			if voiceState == nil || voiceState.ChannelID != job.VoiceChannelID {
				continue
			}
			userID := strings.TrimSpace(voiceState.UserID)
			if userID != "" && userID != selfUserID {
				return true
			}
		}
		return false
	}
	return false
}

func (c *RealClient) recordDAVERecovery(job VoiceJob, voice *discordgo.VoiceConnection, voiceGeneration uint64, event davewatch.Event) {
	c.mu.Lock()
	if c.voice != voice || c.voiceGeneration != voiceGeneration || !sameVoiceJob(c.job, job) {
		c.mu.Unlock()
		return
	}
	switch event.Action {
	case "resend_key_package":
		c.status.DAVEKeyPackageResends++
	case "soft_reset":
		c.status.DAVESoftResets++
	}
	c.status.DAVELastRecoveryReason = event.Reason
	errorClass := event.ErrorClass
	if event.Result != "success" {
		c.status.DAVERecoveryErrors++
		if errorClass == "" {
			errorClass = "dave_recovery_failed"
		}
	}
	c.mu.Unlock()
	log.Printf("Discord DAVE recovery: event=%s stream_id=%s job_generation=%d voice_generation=%d reason=%s result=%s attempt=%d limit=%d error_class=%s", event.Action, strings.TrimSpace(job.StreamID), job.JobGeneration, voiceGeneration, event.Reason, event.Result, event.Attempt, event.Limit, errorClass)
}
