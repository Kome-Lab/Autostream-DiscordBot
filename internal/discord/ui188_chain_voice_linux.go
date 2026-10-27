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

type UI188GatewayRef struct {
	owner   *RealClient
	session *discordgo.Session
	target  AutoStartVoiceTarget
}

// UI188SynchronousGatewayBarrier selects deterministic SDK dispatch before a
// fixture connects. An ordered member-update marker then proves all earlier
// production typed handlers completed; SDK cache mutation alone cannot do so.
func (c *RealClient) UI188SynchronousGatewayBarrier(userID string) (<-chan struct{}, error) {
	c.gatewayMu.Lock()
	defer c.gatewayMu.Unlock()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.session == nil || c.status.Connected || c.job.StreamID != "" || strings.TrimSpace(userID) == "" {
		return nil, errors.New("ui188 synchronous barrier requires an idle unconnected Gateway")
	}
	barrier := make(chan struct{}, 16)
	c.session.SyncEvents = true
	c.session.AddHandler(func(_ *discordgo.Session, event *discordgo.GuildMemberUpdate) {
		if event != nil && event.Member != nil && event.User != nil && event.User.ID == userID {
			select {
			case barrier <- struct{}{}:
			default:
			}
		}
	})
	return barrier, nil
}

func (c *RealClient) UI188CaptureGatewayRef(streamID string) (*UI188GatewayRef, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.session == nil || !c.status.Connected || c.job.StreamID != "" || c.sink == nil {
		return nil, errors.New("ui188 capture requires connected waiting Gateway")
	}
	var result *UI188GatewayRef
	for _, target := range c.autoStartTargets {
		if target.StreamID != streamID {
			continue
		}
		if result != nil || target.AssignmentEpoch == "" || target.GuildID == "" || target.VoiceChannelID == "" {
			return nil, errors.New("ui188 capture requires one authoritative target")
		}
		result = &UI188GatewayRef{owner: c, session: c.session, target: target}
	}
	if result == nil {
		return nil, errors.New("ui188 waiting target missing")
	}
	return result, nil
}

func (c *RealClient) ui188RefCurrentLocked(ref *UI188GatewayRef) bool {
	if ref == nil || ref.owner != c || ref.session == nil || ref.session != c.session ||
		!c.status.Connected || c.job.StreamID != "" || c.sink == nil {
		return false
	}
	for _, target := range c.autoStartTargets {
		if target == ref.target {
			return true
		}
	}
	return false
}

func (c *RealClient) UI188GatewayRefCurrent(ref *UI188GatewayRef) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ui188RefCurrentLocked(ref)
}

func (c *RealClient) UI188GatewayVoiceLocation(ref *UI188GatewayRef, userID string) (string, error) {
	if !c.UI188GatewayRefCurrent(ref) {
		return "", errors.New("ui188 current Gateway missing")
	}
	if ref.session.State == nil {
		return "", errors.New("ui188 SDK State missing")
	}
	ref.session.State.RLock()
	tracked := ref.session.State.TrackVoice
	ref.session.State.RUnlock()
	if !tracked {
		return "", errors.New("ui188 SDK voice tracking disabled")
	}
	channel, _ := currentTrackedVoiceChannel(ref.session, ref.target.GuildID, userID)
	if channel == "" {
		return "absent", nil
	}
	if channel == ref.target.VoiceChannelID {
		return "target", nil
	}
	return "other", nil
}

func (ref *UI188GatewayRef) UI188MatchesCapturedJoin(event VoiceJoinEvent) bool {
	return ref != nil && event.Authority != nil &&
		event.Authority.StreamID == ref.target.StreamID &&
		event.Authority.AssignmentEpoch == ref.target.AssignmentEpoch &&
		event.GuildID == ref.target.GuildID && event.VoiceChannelID == ref.target.VoiceChannelID
}

func (c *RealClient) ui188ReplayContext(old, current *UI188GatewayRef) (EventSink, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if old == nil || old.owner != c || old.session == nil || !c.ui188RefCurrentLocked(current) ||
		old.session == current.session || old.target.StreamID != current.target.StreamID ||
		old.target.GuildID != current.target.GuildID || old.target.VoiceChannelID != current.target.VoiceChannelID ||
		old.target.AssignmentEpoch == current.target.AssignmentEpoch {
		return nil, errors.New("ui188 replay requires real replacement session and epoch")
	}
	return c.sink, nil
}

func (c *RealClient) UI188ReplayReady(old, current *UI188GatewayRef) bool {
	_, err := c.ui188ReplayContext(old, current)
	return err == nil // Test preconditions only; not a product rejection verdict.
}

func (c *RealClient) UI188CurrentHumanKnown(current *UI188GatewayRef, userID string) bool {
	if !c.UI188GatewayRefCurrent(current) {
		return false
	}
	_, err := ui188CurrentHuman(current, userID)
	return err == nil
}

// Reads the actual current SDK cache; it never inserts a member or voice state.
func ui188CurrentHuman(current *UI188GatewayRef, userID string) (*discordgo.VoiceStateUpdate, error) {
	state := &discordgo.VoiceState{GuildID: current.target.GuildID,
		ChannelID: current.target.VoiceChannelID, UserID: userID}
	member, known := currentAutoStartMember(current.session, state)
	if !known || member == nil || member.User == nil || member.User.Bot ||
		userID == "" || userID == sessionUserID(current.session) {
		return nil, errors.New("ui188 replay requires verified current human")
	}
	// Event has no BeforeUpdate: a genuine new-join-shaped input. No SDK cache write.
	return &discordgo.VoiceStateUpdate{VoiceState: state}, nil
}

func (c *RealClient) UI188ReplayOldSession(old, current *UI188GatewayRef, userID string) error {
	if _, err := c.ui188ReplayContext(old, current); err != nil {
		return err
	}
	event, err := ui188CurrentHuman(current, userID)
	if err != nil {
		return err
	}
	// Do not pre-reject old.session here: production handler must receive it.
	c.onVoiceStateUpdate(old.session, event)
	return nil // Only means handler returned; external counters determine the verdict.
}

func (c *RealClient) UI188ReplayOldEpoch(old, current *UI188GatewayRef, held VoiceJoinEvent) error {
	sink, err := c.ui188ReplayContext(old, current)
	if err != nil {
		return err
	}
	if !old.UI188MatchesCapturedJoin(held) {
		return errors.New("ui188 captured join mismatch")
	}
	if _, err := ui188CurrentHuman(current, held.UserID); err != nil {
		return err
	}
	// held.Authority is the immutable event observed BEFORE reassignment.
	// Do NOT replace it with current.target, and do NOT pre-reject it in this hook.
	// Real Manager.VoiceUserJoined must perform the epoch rejection.
	sink.VoiceUserJoined(held) // c.mu is not held.
	return nil
}
