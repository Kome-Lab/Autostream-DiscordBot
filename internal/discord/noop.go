package discord

import (
	"errors"
	"strings"
	"sync"
	"time"
)

type NoopClient struct {
	mu     sync.Mutex
	status Status
	job    VoiceJob
	sink   EventSink
}

func (c *NoopClient) SetEventSink(sink EventSink) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sink = sink
}

func (c *NoopClient) Connect() error {
	c.mu.Lock()
	c.status.Connected = true
	sink := c.sink
	c.mu.Unlock()
	if sink != nil {
		sink.DiscordConnected()
	}
	return nil
}

func (c *NoopClient) JoinVoice(job VoiceJob) error {
	if strings.TrimSpace(job.GuildID) == "" || strings.TrimSpace(job.VoiceChannelID) == "" || strings.TrimSpace(job.StreamID) == "" {
		return errors.New("stream_id, guild_id and voice_channel_id are required")
	}
	if err := c.Connect(); err != nil {
		return err
	}
	c.mu.Lock()
	c.job = job
	c.status.VoiceConnected = true
	c.status.CurrentGuildID = job.GuildID
	c.status.CurrentVoiceID = job.VoiceChannelID
	c.mu.Unlock()
	return nil
}

func (c *NoopClient) LeaveVoice(streamID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.job.StreamID != "" && streamID != "" && c.job.StreamID != streamID {
		return errors.New("stream_id does not match current voice job")
	}
	c.job = VoiceJob{}
	c.status.VoiceConnected = false
	c.status.CurrentGuildID = ""
	c.status.CurrentVoiceID = ""
	return nil
}

func (c *NoopClient) Status() Status {
	c.mu.Lock()
	defer c.mu.Unlock()
	return normalizeStatus(c.status, time.Now().UTC())
}
