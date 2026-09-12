package discord

import (
	"context"
	"errors"
	"log"
	"strings"
	"time"

	"github.com/cartridge-gg/discordgo"
)

func (c *RealClient) JoinVoice(job VoiceJob) error {
	if strings.TrimSpace(job.GuildID) == "" || strings.TrimSpace(job.VoiceChannelID) == "" || strings.TrimSpace(job.StreamID) == "" {
		return errors.New("stream_id, guild_id and voice_channel_id are required")
	}
	if err := c.Connect(); err != nil {
		return err
	}
	voice, err := c.session.ChannelVoiceJoin(job.GuildID, job.VoiceChannelID, false, false)
	if err != nil {
		c.setLastError(err.Error())
		return err
	}
	if voice.OpusRecv == nil {
		voice.OpusRecv = make(chan *discordgo.Packet, 32)
	}
	voice.AddHandler(c.onVoiceSpeakingUpdate)
	c.speakerDispatchMu.Lock()
	c.mu.Lock()
	oldAudioDone := c.audioDone
	oldVoice := c.voice
	if c.audioStop != nil {
		close(c.audioStop)
	}
	if c.daveCancel != nil {
		c.daveCancel()
	}
	audioStop := make(chan struct{})
	daveCtx, daveCancel := context.WithCancel(context.Background())
	c.voice = voice
	c.job = job
	c.voiceGeneration++
	voiceGeneration := c.voiceGeneration
	c.audioStop = audioStop
	c.audioDone = nil
	c.daveCancel = daveCancel
	c.ssrcUsers = map[uint32]string{}
	c.audioSpeakers = map[string]time.Time{}
	forwarder := c.forward
	source := c.source
	c.status.VoiceConnected = true
	c.status.CurrentGuildID = job.GuildID
	c.status.CurrentVoiceID = job.VoiceChannelID
	encoderForwardActive := forwarder != nil && strings.TrimSpace(job.EncoderAudioURL) != "" && voice.OpusRecv != nil
	captionForwardActive := forwarder != nil && strings.TrimSpace(job.CaptionAudioURL) != "" && voice.OpusRecv != nil
	c.status.AudioForwardActive = encoderForwardActive
	c.status.CaptionAudioForwardActive = captionForwardActive
	c.status.DAVEInitialized = false
	c.status.DAVEReady = false
	c.status.DAVEWelcomeReceived = false
	c.status.DAVERosterSize = 0
	c.status.DAVERatchetsMissing = 0
	c.status.DAVELastRecoveryReason = ""
	c.status.LastError = ""
	var audioDone chan struct{}
	if encoderForwardActive || captionForwardActive {
		audioDone = make(chan struct{})
		c.audioDone = audioDone
	}
	c.mu.Unlock()
	c.speakerDispatchMu.Unlock()
	if oldVoice != nil && oldVoice != voice {
		_ = oldVoice.Disconnect()
	}
	waitForOpusForwardStop(oldAudioDone)
	go c.watchDAVE(daveCtx, job, voice, voiceGeneration)
	if encoderForwardActive || captionForwardActive {
		go func() {
			defer close(audioDone)
			c.forwardOpus(job, voiceGeneration, voice.OpusRecv, audioStop, forwarder, source)
		}()
	}
	return nil
}

func (c *RealClient) LeaveVoice(streamID string) error {
	c.speakerDispatchMu.Lock()
	c.mu.Lock()
	if c.job.StreamID != "" && streamID != "" && c.job.StreamID != streamID {
		c.mu.Unlock()
		c.speakerDispatchMu.Unlock()
		return errors.New("stream_id does not match current voice job")
	}
	voice := c.voice
	audioDone := c.audioDone
	if c.audioStop != nil {
		close(c.audioStop)
		c.audioStop = nil
	}
	c.audioDone = nil
	if c.daveCancel != nil {
		c.daveCancel()
		c.daveCancel = nil
	}
	c.voice = nil
	c.voiceGeneration++
	c.job = VoiceJob{}
	c.ssrcUsers = nil
	c.audioSpeakers = nil
	c.status.VoiceConnected = false
	c.status.AudioReceiving = false
	c.status.AudioForwardActive = false
	c.status.CaptionAudioForwardActive = false
	c.status.CurrentGuildID = ""
	c.status.CurrentVoiceID = ""
	c.status.DAVEInitialized = false
	c.status.DAVEReady = false
	c.status.DAVEWelcomeReceived = false
	c.status.DAVERosterSize = 0
	c.status.DAVERatchetsMissing = 0
	c.mu.Unlock()
	c.speakerDispatchMu.Unlock()
	var disconnectErr error
	if voice != nil {
		disconnectErr = voice.Disconnect()
	}
	waitForOpusForwardStop(audioDone)
	if disconnectErr != nil {
		c.setLastError(disconnectErr.Error())
		return disconnectErr
	}
	return nil
}

func waitForOpusForwardStop(done <-chan struct{}) {
	if done == nil {
		return
	}
	timer := time.NewTimer(opusForwardStopWait)
	defer timer.Stop()
	select {
	case <-done:
	case <-timer.C:
	}
}

func (c *RealClient) connectionGenerationForJob(job VoiceJob, voiceGeneration uint64) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.voiceGeneration != voiceGeneration || !sameVoiceJob(c.job, job) || !c.status.VoiceConnected {
		return 0
	}
	return voiceGeneration
}

func (c *RealClient) isCurrentVoiceJob(job VoiceJob, voiceGeneration uint64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.voiceGeneration == voiceGeneration && sameVoiceJob(c.job, job) && c.status.VoiceConnected
}

func sameVoiceJob(left, right VoiceJob) bool {
	return strings.TrimSpace(left.StreamID) == strings.TrimSpace(right.StreamID) &&
		strings.TrimSpace(left.GuildID) == strings.TrimSpace(right.GuildID) &&
		strings.TrimSpace(left.VoiceChannelID) == strings.TrimSpace(right.VoiceChannelID) &&
		strings.TrimSpace(left.TextChannelID) == strings.TrimSpace(right.TextChannelID) &&
		left.DiscordTargetRevision == right.DiscordTargetRevision &&
		left.JobGeneration == right.JobGeneration
}

func (c *RealClient) markVoiceDisconnected(reason string, closeAudioStop bool) {
	c.speakerDispatchMu.Lock()
	defer c.speakerDispatchMu.Unlock()
	c.mu.Lock()
	c.markVoiceDisconnectedLocked(reason, closeAudioStop)
}

func (c *RealClient) markVoiceDisconnectedForJob(reason string, closeAudioStop bool, job VoiceJob, voiceGeneration uint64) {
	c.speakerDispatchMu.Lock()
	defer c.speakerDispatchMu.Unlock()
	c.mu.Lock()
	if c.voiceGeneration != voiceGeneration || !sameVoiceJob(c.job, job) || !c.status.VoiceConnected {
		c.mu.Unlock()
		return
	}
	c.markVoiceDisconnectedLocked(reason, closeAudioStop)
}

func (c *RealClient) markVoiceDisconnectedLocked(reason string, closeAudioStop bool) {
	wasConnected := c.status.VoiceConnected
	job := c.job
	voiceGeneration := c.voiceGeneration
	if closeAudioStop && c.audioStop != nil {
		close(c.audioStop)
		c.audioStop = nil
	}
	if c.daveCancel != nil {
		c.daveCancel()
		c.daveCancel = nil
	}
	c.voice = nil
	c.ssrcUsers = nil
	c.audioSpeakers = nil
	c.status.VoiceConnected = false
	c.status.AudioReceiving = false
	c.status.AudioForwardActive = false
	c.status.CaptionAudioForwardActive = false
	c.status.CurrentGuildID = ""
	c.status.CurrentVoiceID = ""
	if wasConnected {
		c.status.VoiceDisconnectCount++
	}
	sink := c.sink
	c.mu.Unlock()
	if wasConnected {
		log.Printf("Discord voice diagnostic: event=voice_disconnected reason=%s stream_id=%s job_generation=%d voice_generation=%d", strings.TrimSpace(reason), strings.TrimSpace(job.StreamID), job.JobGeneration, voiceGeneration)
	}
	if wasConnected && sink != nil {
		sink.DiscordDisconnected(reason)
	}
}
