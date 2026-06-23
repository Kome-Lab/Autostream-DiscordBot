package discord

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/example/autostream-discord-bot/internal/audioforward"
	"github.com/example/autostream-discord-bot/internal/secrets"
)

type VoiceJob struct {
	GuildID           string `json:"guild_id"`
	VoiceChannelID    string `json:"voice_channel_id"`
	TextChannelID     string `json:"text_channel_id,omitempty"`
	StreamID          string `json:"stream_id"`
	EncoderAudioURL   string `json:"encoder_audio_url,omitempty"`
	CaptionAudioURL   string `json:"caption_audio_url,omitempty"`
	StreamIngestToken string `json:"stream_ingest_token,omitempty"`
}

type ParticipantEvent struct {
	StreamID       string `json:"stream_id"`
	GuildID        string `json:"guild_id"`
	VoiceChannelID string `json:"voice_channel_id"`
	UserID         string `json:"user_id"`
	Username       string `json:"username,omitempty"`
	Present        bool   `json:"present"`
}

type EventSink interface {
	ParticipantChanged(event ParticipantEvent)
	ActiveSpeakerDetected(streamID, userID string)
	DiscordConnected()
	DiscordDisconnected(reason string)
}

type EventSource interface {
	SetEventSink(sink EventSink)
}

type AudioForwarder interface {
	ForwardOpus(ctx context.Context, encoderAudioURL, streamID, source, tokenOverride string, packets []audioforward.OpusPacket) error
}

type AudioForwardSource interface {
	SetAudioForwarder(forwarder AudioForwarder, source string)
}

type Client interface {
	Connect() error
	JoinVoice(job VoiceJob) error
	LeaveVoice(streamID string) error
	Status() Status
}

type Status struct {
	Connected             bool    `json:"connected"`
	VoiceConnected        bool    `json:"voice_connected"`
	CurrentGuildID        string  `json:"current_guild_id,omitempty"`
	CurrentVoiceID        string  `json:"current_voice_channel_id,omitempty"`
	AudioForwardEnabled   bool    `json:"audio_forward_enabled"`
	AudioForwardActive    bool    `json:"audio_forward_active"`
	AudioReceiving        bool    `json:"audio_receiving"`
	LastAudioAt           string  `json:"last_audio_at,omitempty"`
	LastAudioAgeSec       float64 `json:"last_audio_age_sec,omitempty"`
	AudioPacketsReceived  int64   `json:"audio_packets_received"`
	AudioPacketsForwarded int64   `json:"audio_packets_forwarded"`
	AudioForwardErrors    int64   `json:"audio_forward_errors"`
	GatewayReconnectCount int64   `json:"gateway_reconnect_count"`
	VoiceDisconnectCount  int64   `json:"voice_disconnect_count"`
	LastForwardAt         string  `json:"last_forward_at,omitempty"`
	LastForwardAgeSec     float64 `json:"last_forward_age_sec,omitempty"`
	LastForwardError      string  `json:"last_forward_error,omitempty"`
	LastError             string  `json:"last_error,omitempty"`
}

type Config struct {
	BotToken string
}

func ConfigFromEnv() Config {
	return Config{BotToken: os.Getenv("DISCORD_BOT_TOKEN")}
}

func (c Config) Validate() error {
	if strings.TrimSpace(c.BotToken) == "" {
		return errors.New("DISCORD_BOT_TOKEN is required")
	}
	return nil
}

type RealClient struct {
	cfg       Config
	session   *discordgo.Session
	voice     *discordgo.VoiceConnection
	sink      EventSink
	forward   AudioForwarder
	source    string
	audioStop chan struct{}
	ssrcUsers map[uint32]string
	mu        sync.Mutex
	status    Status
	job       VoiceJob
}

func NewRealClient(cfg Config) (*RealClient, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	session, err := discordgo.New("Bot " + cfg.BotToken)
	if err != nil {
		return nil, err
	}
	session.Identify.Intents = discordgo.IntentsGuilds | discordgo.IntentsGuildVoiceStates
	client := &RealClient{cfg: cfg, session: session}
	session.AddHandler(client.onGatewayDisconnect)
	session.AddHandler(client.onGatewayResumed)
	session.AddHandler(client.onVoiceStateUpdate)
	return client, nil
}

func (c *RealClient) SetEventSink(sink EventSink) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sink = sink
}

func (c *RealClient) SetAudioForwarder(forwarder AudioForwarder, source string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.forward = forwarder
	c.source = source
	c.status.AudioForwardEnabled = forwarder != nil
}

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
	c.mu.Lock()
	if c.voice != nil && c.voice != voice {
		_ = c.voice.Disconnect()
	}
	if c.audioStop != nil {
		close(c.audioStop)
	}
	audioStop := make(chan struct{})
	c.voice = voice
	c.job = job
	c.audioStop = audioStop
	c.ssrcUsers = map[uint32]string{}
	forwarder := c.forward
	source := c.source
	c.status.VoiceConnected = true
	c.status.CurrentGuildID = job.GuildID
	c.status.CurrentVoiceID = job.VoiceChannelID
	c.status.AudioForwardActive = forwarder != nil && job.EncoderAudioURL != "" && voice.OpusRecv != nil
	c.status.LastError = ""
	c.mu.Unlock()
	if forwarder != nil && job.EncoderAudioURL != "" && voice.OpusRecv != nil {
		go c.forwardOpus(job, voice.OpusRecv, audioStop, forwarder, source)
	}
	return nil
}

func (c *RealClient) LeaveVoice(streamID string) error {
	c.mu.Lock()
	if c.job.StreamID != "" && streamID != "" && c.job.StreamID != streamID {
		c.mu.Unlock()
		return errors.New("stream_id does not match current voice job")
	}
	voice := c.voice
	if c.audioStop != nil {
		close(c.audioStop)
		c.audioStop = nil
	}
	c.voice = nil
	c.job = VoiceJob{}
	c.ssrcUsers = nil
	c.status.VoiceConnected = false
	c.status.AudioReceiving = false
	c.status.AudioForwardActive = false
	c.status.CurrentGuildID = ""
	c.status.CurrentVoiceID = ""
	c.mu.Unlock()
	if voice != nil {
		if err := voice.Disconnect(); err != nil {
			c.setLastError(err.Error())
			return err
		}
	}
	return nil
}

func (c *RealClient) onVoiceSpeakingUpdate(_ *discordgo.VoiceConnection, event *discordgo.VoiceSpeakingUpdate) {
	if event == nil {
		return
	}
	c.mu.Lock()
	if c.ssrcUsers == nil {
		c.ssrcUsers = map[uint32]string{}
	}
	if event.SSRC != 0 && event.UserID != "" {
		c.ssrcUsers[uint32(event.SSRC)] = event.UserID
	}
	streamID := c.job.StreamID
	sink := c.sink
	userID := event.UserID
	speaking := event.Speaking
	c.mu.Unlock()
	if speaking && streamID != "" && userID != "" && sink != nil {
		sink.ActiveSpeakerDetected(streamID, userID)
	}
}

func (c *RealClient) forwardOpus(job VoiceJob, packets <-chan *discordgo.Packet, stop <-chan struct{}, forwarder AudioForwarder, source string) {
	const maxBatch = 20
	flushEvery := time.NewTicker(500 * time.Millisecond)
	defer flushEvery.Stop()
	batch := make([]audioforward.OpusPacket, 0, maxBatch)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		count := len(batch)
		err := forwarder.ForwardOpus(ctx, job.EncoderAudioURL, job.StreamID, source, job.StreamIngestToken, batch)
		cancel()
		now := time.Now().UTC()
		if err != nil {
			c.setForwardError(err.Error())
		} else {
			c.mu.Lock()
			c.status.AudioPacketsForwarded += int64(count)
			c.status.LastForwardAt = now.Format(time.RFC3339Nano)
			c.status.LastForwardError = ""
			c.mu.Unlock()
		}
		batch = batch[:0]
	}
	for {
		select {
		case <-stop:
			flush()
			return
		case <-flushEvery.C:
			flush()
		case packet, ok := <-packets:
			if !ok {
				flush()
				c.markVoiceDisconnected("opus_recv_closed", false)
				return
			}
			if packet == nil || len(packet.Opus) == 0 {
				continue
			}
			userID := c.userForSSRC(packet.SSRC)
			now := time.Now().UTC()
			c.mu.Lock()
			c.status.AudioReceiving = true
			c.status.LastAudioAt = now.Format(time.RFC3339Nano)
			c.status.AudioPacketsReceived++
			c.mu.Unlock()
			batch = append(batch, audioforward.OpusPacket{
				SSRC:       packet.SSRC,
				UserID:     userID,
				Sequence:   packet.Sequence,
				Timestamp:  packet.Timestamp,
				ReceivedAt: now,
				Opus:       append([]byte(nil), packet.Opus...),
			})
			if len(batch) >= maxBatch {
				flush()
			}
		}
	}
}

func (c *RealClient) userForSSRC(ssrc uint32) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ssrcUsers[ssrc]
}

func (c *RealClient) Status() Status {
	c.mu.Lock()
	defer c.mu.Unlock()
	return normalizeStatus(c.status, time.Now().UTC())
}

func (c *RealClient) onGatewayDisconnect(_ *discordgo.Session, _ *discordgo.Disconnect) {
	c.mu.Lock()
	c.status.Connected = false
	sink := c.sink
	c.mu.Unlock()
	if sink != nil {
		sink.DiscordDisconnected("gateway_disconnect")
	}
}

func (c *RealClient) onGatewayResumed(_ *discordgo.Session, _ *discordgo.Resumed) {
	c.mu.Lock()
	c.status.Connected = true
	c.status.GatewayReconnectCount++
	c.status.LastError = ""
	sink := c.sink
	c.mu.Unlock()
	if sink != nil {
		sink.DiscordConnected()
	}
}

func (c *RealClient) onVoiceStateUpdate(session *discordgo.Session, event *discordgo.VoiceStateUpdate) {
	if event == nil || event.VoiceState == nil {
		return
	}
	c.mu.Lock()
	job := c.job
	sink := c.sink
	c.mu.Unlock()
	if job.StreamID == "" || event.GuildID != job.GuildID {
		return
	}
	if event.UserID == sessionUserID(session) && event.BeforeUpdate != nil && event.BeforeUpdate.ChannelID == job.VoiceChannelID && event.ChannelID != job.VoiceChannelID {
		c.markVoiceDisconnected("voice_state_disconnected", true)
		return
	}
	if sink == nil {
		return
	}
	if event.ChannelID == job.VoiceChannelID {
		sink.ParticipantChanged(ParticipantEvent{
			StreamID:       job.StreamID,
			GuildID:        job.GuildID,
			VoiceChannelID: job.VoiceChannelID,
			UserID:         event.UserID,
			Present:        true,
		})
		return
	}
	if event.BeforeUpdate != nil && event.BeforeUpdate.ChannelID == job.VoiceChannelID {
		sink.ParticipantChanged(ParticipantEvent{
			StreamID:       job.StreamID,
			GuildID:        job.GuildID,
			VoiceChannelID: job.VoiceChannelID,
			UserID:         event.UserID,
			Present:        false,
		})
	}
}

func sessionUserID(session *discordgo.Session) string {
	if session == nil || session.State == nil || session.State.User == nil {
		return ""
	}
	return session.State.User.ID
}

func (c *RealClient) markVoiceDisconnected(reason string, closeAudioStop bool) {
	c.mu.Lock()
	wasConnected := c.status.VoiceConnected
	if closeAudioStop && c.audioStop != nil {
		close(c.audioStop)
		c.audioStop = nil
	}
	c.voice = nil
	c.ssrcUsers = nil
	c.status.VoiceConnected = false
	c.status.AudioReceiving = false
	c.status.AudioForwardActive = false
	c.status.CurrentGuildID = ""
	c.status.CurrentVoiceID = ""
	if wasConnected {
		c.status.VoiceDisconnectCount++
	}
	sink := c.sink
	c.mu.Unlock()
	if wasConnected && sink != nil {
		sink.DiscordDisconnected(reason)
	}
}

func (c *RealClient) setLastError(value string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.status.LastError = secrets.SanitizeOperationalError(value, "discord operation failed")
}

func (c *RealClient) setForwardError(value string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.status.AudioForwardErrors++
	safeValue := secrets.SanitizeOperationalError(value, "discord audio forward failed")
	c.status.LastForwardError = safeValue
	c.status.LastError = safeValue
}

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

func normalizeStatus(status Status, now time.Time) Status {
	if status.LastAudioAt != "" {
		if last, err := time.Parse(time.RFC3339Nano, status.LastAudioAt); err == nil {
			status.LastAudioAgeSec = maxFloat(now.Sub(last).Seconds(), 0)
			status.AudioReceiving = status.AudioReceiving && status.LastAudioAgeSec <= 5
		}
	}
	if status.LastForwardAt != "" {
		if last, err := time.Parse(time.RFC3339Nano, status.LastForwardAt); err == nil {
			status.LastForwardAgeSec = maxFloat(now.Sub(last).Seconds(), 0)
		}
	}
	return status
}

func maxFloat(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}
