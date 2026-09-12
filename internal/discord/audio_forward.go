package discord

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/cartridge-gg/discordgo"
	"github.com/example/autostream-discord-bot/internal/audioforward"
	"github.com/example/autostream-discord-bot/internal/secrets"
)

type AudioForwarder interface {
	ForwardOpus(ctx context.Context, encoderAudioURL, streamID, source, tokenOverride string, packets []audioforward.OpusPacket) error
}

type AudioForwardSource interface {
	SetAudioForwarder(forwarder AudioForwarder, source string)
}

func (c *RealClient) SetAudioForwarder(forwarder AudioForwarder, source string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.forward = forwarder
	c.source = source
	c.status.AudioForwardEnabled = forwarder != nil
}

const (
	opusForwardQueueBatches  = 64
	opusForwardStopWait      = 2 * time.Second
	opusForwardRequestLimit  = 5 * time.Second
	opusEncoderBatchPackets  = 50
	opusEncoderFlushInterval = 100 * time.Millisecond
)

type opusForwardTarget struct {
	caption bool
	url     string
	token   string
	batches *audioforward.BatchQueue
}

type opusForwardRequestTimeoutProvider interface {
	ForwardOpusRequestTimeout() time.Duration
}

func opusForwardRequestTimeout(forwarder AudioForwarder) time.Duration {
	if provider, ok := forwarder.(opusForwardRequestTimeoutProvider); ok {
		if timeout := provider.ForwardOpusRequestTimeout(); timeout > 0 {
			return timeout
		}
	}
	return opusForwardRequestLimit
}

func newOpusForwardTarget(caption bool, targetURL, token string) *opusForwardTarget {
	if strings.TrimSpace(targetURL) == "" {
		return nil
	}
	return &opusForwardTarget{
		caption: caption,
		url:     targetURL,
		token:   token,
		batches: audioforward.NewBatchQueue(opusForwardQueueBatches),
	}
}

func (c *RealClient) runOpusForwardTarget(ctx context.Context, job VoiceJob, voiceGeneration uint64, forwarder AudioForwarder, source string, target *opusForwardTarget, done *sync.WaitGroup) {
	defer done.Done()
	target.batches.Run(ctx, opusForwardRequestTimeout(forwarder), func(requestCtx context.Context, batch []audioforward.OpusPacket) error {
		return forwarder.ForwardOpus(requestCtx, target.url, job.StreamID, source, target.token, batch)
	}, func(packetCount int, err error) {
		if err != nil {
			c.setForwardErrorForJob(job, voiceGeneration, target.caption, err.Error())
			return
		}
		now := time.Now().UTC()
		c.mu.Lock()
		if c.voiceGeneration != voiceGeneration || !sameVoiceJob(c.job, job) || !c.status.VoiceConnected {
			c.mu.Unlock()
			return
		}
		if target.caption {
			c.status.CaptionPacketsForwarded += int64(packetCount)
			c.status.LastCaptionForwardError = ""
		} else {
			c.status.AudioPacketsForwarded += int64(packetCount)
			c.status.LastForwardAt = now.Format(time.RFC3339Nano)
			c.status.LastForwardError = ""
		}
		c.mu.Unlock()
	})
}

func (c *RealClient) enqueueOpusForwardBatch(job VoiceJob, voiceGeneration uint64, target *opusForwardTarget, batch []audioforward.OpusPacket) {
	if target == nil || len(batch) == 0 {
		return
	}
	if !c.isCurrentVoiceJob(job, voiceGeneration) {
		return
	}
	if !target.batches.Enqueue(batch) {
		c.recordForwardQueueDropForJob(job, voiceGeneration, target.caption, len(batch))
	}
}

func (c *RealClient) forwardOpus(job VoiceJob, voiceGeneration uint64, packets <-chan *discordgo.Packet, stop <-chan struct{}, forwarder AudioForwarder, source string) {
	encoderBatchMax := opusEncoderBatchPackets
	captionBatchMax := job.CaptionAudioMaxBatchPackets
	if captionBatchMax <= 0 {
		captionBatchMax = 5
	}
	if captionBatchMax > 100 {
		captionBatchMax = 100
	}
	captionFlush := time.Duration(job.CaptionAudioFlushMS) * time.Millisecond
	if captionFlush <= 0 {
		captionFlush = 100 * time.Millisecond
	}
	if captionFlush > time.Second {
		captionFlush = time.Second
	}
	unresolvedWindow := unresolvedSSRCBufferWindow(job)

	runCtx, cancel := context.WithCancel(context.Background())
	monitorDone := make(chan struct{})
	go func() {
		select {
		case <-stop:
			cancel()
		case <-monitorDone:
		}
	}()
	encoderTarget := newOpusForwardTarget(false, job.EncoderAudioURL, job.StreamIngestToken)
	captionTarget := newOpusForwardTarget(true, job.CaptionAudioURL, job.CaptionAudioToken)
	var forwarders sync.WaitGroup
	for _, target := range []*opusForwardTarget{encoderTarget, captionTarget} {
		if target == nil {
			continue
		}
		forwarders.Add(1)
		go c.runOpusForwardTarget(runCtx, job, voiceGeneration, forwarder, source, target, &forwarders)
	}
	defer func() {
		close(monitorDone)
		cancel()
		forwarders.Wait()
	}()

	encoderBatch := make([]audioforward.OpusPacket, 0, encoderBatchMax)
	captionBatch := make([]audioforward.OpusPacket, 0, captionBatchMax)
	lastEncoderFlush := time.Now().UTC()
	lastCaptionFlush := lastEncoderFlush
	var fallbackUserID string
	var fallbackUserCheckedAt time.Time
	unresolved := map[uint32][]audioforward.OpusPacket{}
	unresolvedSince := map[uint32]time.Time{}
	resolveFallbackUser := func(now time.Time) string {
		if !fallbackUserCheckedAt.IsZero() && now.Sub(fallbackUserCheckedAt) < 250*time.Millisecond {
			return fallbackUserID
		}
		fallbackUserCheckedAt = now
		fallbackUserID = c.uniqueHumanVoiceParticipant(job)
		return fallbackUserID
	}
	flush := func(isCaption bool) {
		var batch []audioforward.OpusPacket
		if isCaption {
			batch, captionBatch = captionBatch, nil
		} else {
			batch, encoderBatch = encoderBatch, nil
		}
		if len(batch) == 0 {
			return
		}
		if isCaption {
			if strings.TrimSpace(job.CaptionAudioToken) == "" {
				c.setForwardErrorForJob(job, voiceGeneration, true, errors.New("caption_audio_token is required").Error())
				return
			}
			c.enqueueOpusForwardBatch(job, voiceGeneration, captionTarget, batch)
			return
		}
		c.enqueueOpusForwardBatch(job, voiceGeneration, encoderTarget, batch)
	}
	appendEncoderPacket := func(packet audioforward.OpusPacket) {
		if strings.TrimSpace(job.EncoderAudioURL) != "" {
			encoderBatch = append(encoderBatch, packet)
		}
	}
	appendCaptionPacket := func(packet audioforward.OpusPacket) {
		if strings.TrimSpace(job.CaptionAudioURL) != "" {
			captionBatch = append(captionBatch, packet)
		}
	}
	flushExpiredUnresolved := func(now time.Time, force bool) {
		for ssrc, buffered := range unresolved {
			userID := c.userForSSRC(ssrc, voiceGeneration)
			if userID == "" {
				userID = resolveFallbackUser(now)
			}
			if userID == "" && !force && now.Sub(unresolvedSince[ssrc]) < unresolvedWindow {
				continue
			}
			if userID != "" {
				c.recordAudioSpeakerActivity(job, voiceGeneration, userID, now)
			}
			for _, packet := range buffered {
				packet.UserID = userID
				appendCaptionPacket(packet)
			}
			delete(unresolved, ssrc)
			delete(unresolvedSince, ssrc)
		}
	}
	flushTicker := time.NewTicker(25 * time.Millisecond)
	defer flushTicker.Stop()
	for {
		select {
		case <-stop:
			return
		case now := <-flushTicker.C:
			if !c.isCurrentVoiceJob(job, voiceGeneration) {
				return
			}
			now = now.UTC()
			flushExpiredUnresolved(now, false)
			if now.Sub(lastCaptionFlush) >= captionFlush {
				flush(true)
				lastCaptionFlush = now
			}
			if now.Sub(lastEncoderFlush) >= opusEncoderFlushInterval {
				flush(false)
				lastEncoderFlush = now
			}
			c.expireIdleAudioSpeakers(job, voiceGeneration, now)
		case packet, ok := <-packets:
			if !ok {
				c.markVoiceDisconnectedForJob("opus_recv_closed", false, job, voiceGeneration)
				return
			}
			if packet == nil || len(packet.Opus) == 0 {
				continue
			}
			if !c.isCurrentVoiceJob(job, voiceGeneration) {
				return
			}
			now := time.Now().UTC()
			userID := c.userForSSRC(packet.SSRC, voiceGeneration)
			if userID == "" {
				// Discord can deliver Opus before the first SSRC speaking update.
				// When the target VC has exactly one human participant, use that
				// authoritative snapshot so Deepgram and the scene do not fall back
				// to the synthetic MIC speaker.
				userID = resolveFallbackUser(now)
			}
			c.recordAudioSpeakerActivity(job, voiceGeneration, userID, now)
			if !c.recordAudioPacketReceived(job, voiceGeneration, now) {
				return
			}
			forwardedPacket := audioforward.OpusPacket{
				SSRC:                 packet.SSRC,
				UserID:               userID,
				Sequence:             packet.Sequence,
				Timestamp:            packet.Timestamp,
				ReceivedAt:           now,
				Opus:                 append([]byte(nil), packet.Opus...),
				JobGeneration:        job.JobGeneration,
				ConnectionGeneration: voiceGeneration,
			}
			// Encoder mixing is keyed by SSRC and must not wait for Discord's
			// eventually-consistent SSRC-to-user mapping. Delaying both targets
			// here turns the identity window into a burst that can overflow the
			// real-time mixer. Only captions need the resolved user identity.
			appendEncoderPacket(forwardedPacket)
			if strings.TrimSpace(job.CaptionAudioURL) != "" && userID == "" && unresolvedWindow > 0 {
				if len(unresolved[packet.SSRC]) < 50 {
					unresolved[packet.SSRC] = append(unresolved[packet.SSRC], forwardedPacket)
					if unresolvedSince[packet.SSRC].IsZero() {
						unresolvedSince[packet.SSRC] = now
					}
				}
			} else if strings.TrimSpace(job.CaptionAudioURL) != "" {
				flushExpiredUnresolved(now, false)
				appendCaptionPacket(forwardedPacket)
			}
			if len(captionBatch) >= captionBatchMax {
				flush(true)
				lastCaptionFlush = now
			}
			if len(encoderBatch) >= encoderBatchMax {
				flush(false)
				lastEncoderFlush = now
			}
		}
	}
}

func unresolvedSSRCBufferWindow(job VoiceJob) time.Duration {
	window := time.Duration(job.UnresolvedSSRCBufferMS) * time.Millisecond
	if window < 0 || (window == 0 && !job.UnresolvedSSRCBufferMSSet) {
		return time.Second
	}
	return window
}

func (c *RealClient) recordAudioPacketReceived(job VoiceJob, voiceGeneration uint64, now time.Time) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.voiceGeneration != voiceGeneration || !sameVoiceJob(c.job, job) || !c.status.VoiceConnected {
		return false
	}
	c.status.AudioReceiving = true
	c.status.LastAudioAt = now.Format(time.RFC3339Nano)
	c.status.AudioPacketsReceived++
	return true
}

func (c *RealClient) setForwardError(value string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.status.AudioForwardErrors++
	safeValue := secrets.SanitizeOperationalError(value, "discord audio forward failed")
	c.status.LastForwardError = safeValue
	c.status.LastError = safeValue
}

func (c *RealClient) setCaptionForwardError(value string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.status.CaptionForwardErrors++
	safeValue := secrets.SanitizeOperationalError(value, "discord caption audio forward failed")
	c.status.LastCaptionForwardError = safeValue
	c.status.LastError = safeValue
}

func (c *RealClient) setForwardErrorForJob(job VoiceJob, voiceGeneration uint64, caption bool, value string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.voiceGeneration != voiceGeneration || !sameVoiceJob(c.job, job) || !c.status.VoiceConnected {
		return false
	}
	if caption {
		c.status.CaptionForwardErrors++
		safeValue := secrets.SanitizeOperationalError(value, "discord caption audio forward failed")
		c.status.LastCaptionForwardError = safeValue
		c.status.LastError = safeValue
		return true
	}
	c.status.AudioForwardErrors++
	safeValue := secrets.SanitizeOperationalError(value, "discord audio forward failed")
	c.status.LastForwardError = safeValue
	c.status.LastError = safeValue
	return true
}

func (c *RealClient) recordForwardQueueDropForJob(job VoiceJob, voiceGeneration uint64, caption bool, packetCount int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.voiceGeneration != voiceGeneration || !sameVoiceJob(c.job, job) || !c.status.VoiceConnected {
		return false
	}
	if caption {
		c.status.CaptionForwardQueueDrops += int64(packetCount)
		c.status.CaptionForwardErrors++
		safeValue := secrets.SanitizeOperationalError("discord caption audio forward queue full", "discord caption audio forward failed")
		c.status.LastCaptionForwardError = safeValue
		c.status.LastError = safeValue
		return true
	}
	c.status.AudioForwardQueueDrops += int64(packetCount)
	c.status.AudioForwardErrors++
	safeValue := secrets.SanitizeOperationalError("discord audio forward queue full", "discord audio forward failed")
	c.status.LastForwardError = safeValue
	c.status.LastError = safeValue
	return true
}
