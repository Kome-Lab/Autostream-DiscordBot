package discord

import (
	"strings"
	"time"

	"github.com/cartridge-gg/discordgo"
)

const audioSpeakerIdleTimeout = 1250 * time.Millisecond

func (c *RealClient) onVoiceSpeakingUpdate(voice *discordgo.VoiceConnection, event *discordgo.VoiceSpeakingUpdate) {
	if event == nil {
		return
	}
	c.mu.Lock()
	if c.voice != voice || !c.status.VoiceConnected {
		c.mu.Unlock()
		return
	}
	if c.ssrcUsers == nil {
		c.ssrcUsers = map[uint32]string{}
	}
	if event.SSRC != 0 && event.UserID != "" {
		c.ssrcUsers[uint32(event.SSRC)] = event.UserID
	}
	job := c.job
	voiceGeneration := c.voiceGeneration
	userID := event.UserID
	speaking := event.Speaking
	if userID != "" {
		if speaking {
			if c.audioSpeakers == nil {
				c.audioSpeakers = map[string]time.Time{}
			}
			c.audioSpeakers[userID] = time.Now().UTC()
		} else {
			delete(c.audioSpeakers, userID)
		}
	}
	c.mu.Unlock()
	c.dispatchActiveSpeakerState(job, voiceGeneration, userID, speaking)
}

func (c *RealClient) recordAudioSpeakerActivity(job VoiceJob, voiceGeneration uint64, userID string, now time.Time) {
	userID = strings.TrimSpace(userID)
	if userID == "" {
		return
	}
	c.mu.Lock()
	if c.voiceGeneration != voiceGeneration || !sameVoiceJob(c.job, job) || !c.status.VoiceConnected {
		c.mu.Unlock()
		return
	}
	if c.audioSpeakers == nil {
		c.audioSpeakers = map[string]time.Time{}
	}
	_, alreadyActive := c.audioSpeakers[userID]
	c.audioSpeakers[userID] = now
	c.mu.Unlock()
	if !alreadyActive {
		c.dispatchActiveSpeakerState(job, voiceGeneration, userID, true)
	}
}

func (c *RealClient) expireIdleAudioSpeakers(job VoiceJob, voiceGeneration uint64, now time.Time) {
	c.mu.Lock()
	if c.voiceGeneration != voiceGeneration || !sameVoiceJob(c.job, job) || !c.status.VoiceConnected {
		c.mu.Unlock()
		return
	}
	expired := make([]string, 0)
	for userID, lastAudioAt := range c.audioSpeakers {
		if !lastAudioAt.Add(audioSpeakerIdleTimeout).After(now) {
			delete(c.audioSpeakers, userID)
			expired = append(expired, userID)
		}
	}
	c.mu.Unlock()
	for _, userID := range expired {
		c.dispatchActiveSpeakerState(job, voiceGeneration, userID, false)
	}
}

func (c *RealClient) dispatchActiveSpeakerState(job VoiceJob, voiceGeneration uint64, userID string, speaking bool) {
	userID = strings.TrimSpace(userID)
	if userID == "" {
		return
	}
	c.speakerDispatchMu.Lock()
	defer c.speakerDispatchMu.Unlock()

	c.mu.Lock()
	if c.voiceGeneration != voiceGeneration || !sameVoiceJob(c.job, job) || !c.status.VoiceConnected {
		c.mu.Unlock()
		return
	}
	_, active := c.audioSpeakers[userID]
	if active != speaking {
		c.mu.Unlock()
		return
	}
	streamID := c.job.StreamID
	sink := c.sink
	c.mu.Unlock()
	if streamID == "" || sink == nil {
		return
	}
	if stateSink, ok := sink.(ActiveSpeakerStateSink); ok {
		stateSink.ActiveSpeakerStateChanged(streamID, userID, speaking)
		return
	}
	if speaking {
		sink.ActiveSpeakerDetected(streamID, userID)
	}
}

type voiceSSRCUserResolver interface {
	SSRCUserID(uint32) string
}

func resolveSSRCUserID(resolver voiceSSRCUserResolver, ssrc uint32) string {
	if resolver == nil || ssrc == 0 {
		return ""
	}
	return strings.TrimSpace(resolver.SSRCUserID(ssrc))
}

func (c *RealClient) userForSSRC(ssrc uint32, voiceGeneration uint64) string {
	c.mu.Lock()
	if c.voiceGeneration != voiceGeneration || !c.status.VoiceConnected {
		c.mu.Unlock()
		return ""
	}
	userID := strings.TrimSpace(c.ssrcUsers[ssrc])
	voice := c.voice
	c.mu.Unlock()
	if userID != "" || voice == nil {
		return userID
	}

	// DiscordGo records OP5 SPEAKING mappings before userspace handlers run.
	// Consult that durable map when JoinVoice registered our callback after a
	// participant's first speaking event; otherwise only the first observed
	// speaker receives captions and active-speaker state in a multi-user VC.
	userID = resolveSSRCUserID(voice, ssrc)
	if userID == "" {
		return ""
	}

	c.mu.Lock()
	if c.voice != voice || c.voiceGeneration != voiceGeneration || !c.status.VoiceConnected {
		c.mu.Unlock()
		return ""
	}
	if c.ssrcUsers == nil {
		c.ssrcUsers = map[uint32]string{}
	}
	if current := strings.TrimSpace(c.ssrcUsers[ssrc]); current != "" {
		userID = current
	} else {
		c.ssrcUsers[ssrc] = userID
	}
	c.mu.Unlock()
	return userID
}

func (c *RealClient) uniqueHumanVoiceParticipant(job VoiceJob) string {
	snapshot, ok := c.SnapshotVoiceParticipants(job)
	if !ok {
		return ""
	}
	userID := ""
	for _, participant := range snapshot.Participants {
		if participant.IsBot {
			continue
		}
		candidate := strings.TrimSpace(participant.UserID)
		if candidate == "" {
			continue
		}
		if userID != "" {
			return ""
		}
		userID = candidate
	}
	return userID
}
