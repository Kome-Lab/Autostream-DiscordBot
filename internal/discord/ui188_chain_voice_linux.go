//go:build linux && ui188_chain

package discord

import (
	"errors"
	"strings"

	"github.com/cartridge-gg/discordgo"
)

// UI188ControlledVoice replaces only the external Voice transport. It feeds
// actual SDK packets through the production generation, batching, queue, and
// forwarder path. This helper is absent from ordinary product builds.
func (c *RealClient) UI188ControlledVoice(job VoiceJob) (chan<- *discordgo.Packet, error) {
	if strings.TrimSpace(job.StreamID) == "" || job.GuildID == "" || job.VoiceChannelID == "" {
		return nil, errors.New("controlled Voice job identity missing")
	}
	c.speakerDispatchMu.Lock()
	c.mu.Lock()
	if c.job.StreamID != "" || c.session == nil || !c.status.Connected || c.forward == nil {
		c.mu.Unlock()
		c.speakerDispatchMu.Unlock()
		return nil, errors.New("controlled Voice requires current Gateway, idle job, and actual forwarder")
	}
	c.job = job
	c.voiceGeneration++
	generation := c.voiceGeneration
	stop, done := make(chan struct{}), make(chan struct{})
	c.audioStop, c.audioDone = stop, done
	c.ssrcUsers = map[uint32]string{}
	c.audioSpeakers = nil
	c.status.VoiceConnected = true
	c.status.CurrentGuildID, c.status.CurrentVoiceID = job.GuildID, job.VoiceChannelID
	c.status.AudioForwardActive = true
	forwarder, source := c.forward, c.source
	packets := make(chan *discordgo.Packet, 64)
	c.mu.Unlock()
	c.speakerDispatchMu.Unlock()
	go func() { defer close(done); c.forwardOpus(job, generation, packets, stop, forwarder, source) }()
	return packets, nil
}
