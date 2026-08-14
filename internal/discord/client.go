package discord

import (
	"context"
	"errors"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cartridge-gg/discordgo"
	"github.com/example/autostream-discord-bot/internal/audioforward"
	"github.com/example/autostream-discord-bot/internal/secrets"
)

type VoiceJob struct {
	GuildID                     string `json:"guild_id"`
	VoiceChannelID              string `json:"voice_channel_id"`
	TextChannelID               string `json:"text_channel_id,omitempty"`
	StreamID                    string `json:"stream_id"`
	EncoderAudioURL             string `json:"encoder_audio_url,omitempty"`
	CaptionAudioURL             string `json:"caption_audio_url,omitempty"`
	CaptionAudioToken           string `json:"caption_audio_token,omitempty"`
	StreamIngestToken           string `json:"stream_ingest_token,omitempty"`
	WorkerEventsURL             string `json:"worker_events_url,omitempty"`
	WorkerEventsToken           string `json:"worker_events_token,omitempty"`
	CaptionAudioFlushMS         int    `json:"caption_audio_flush_ms,omitempty"`
	CaptionAudioMaxBatchPackets int    `json:"caption_audio_max_batch_packets,omitempty"`
	UnresolvedSSRCBufferMS      int    `json:"unresolved_ssrc_buffer_ms,omitempty"`
	JobGeneration               uint64 `json:"job_generation,omitempty"`
}

type ParticipantEvent struct {
	StreamID       string `json:"stream_id"`
	GuildID        string `json:"guild_id"`
	VoiceChannelID string `json:"voice_channel_id"`
	UserID         string `json:"user_id"`
	Username       string `json:"username,omitempty"`
	AvatarURL      string `json:"avatar_url,omitempty"`
	IsBot          bool   `json:"is_bot,omitempty"`
	Present        bool   `json:"present"`
}

// VoiceParticipant is a non-secret, current member of a tracked Discord voice
// channel. It deliberately carries no gateway session or voice-server fields.
// The bot's own user is filtered before this value is produced; other Discord
// bots remain visible so the preview matches the channel membership.
type VoiceParticipant struct {
	UserID    string
	Username  string
	AvatarURL string
	IsBot     bool
}

// ParticipantSnapshot is an authoritative view of the human members currently
// in one active stream's configured voice channel. Revision increases for each
// snapshot produced by a client so a delayed snapshot cannot overwrite a newer
// view in the job manager.
type ParticipantSnapshot struct {
	StreamID       string
	GuildID        string
	VoiceChannelID string
	Participants   []VoiceParticipant
	Revision       uint64
}

type VoiceJoinEvent struct {
	GuildID        string `json:"guild_id"`
	VoiceChannelID string `json:"voice_channel_id"`
	UserID         string `json:"user_id"`
	Username       string `json:"username,omitempty"`
}

type ChatMessageEvent struct {
	StreamID      string    `json:"stream_id"`
	GuildID       string    `json:"guild_id"`
	TextChannelID string    `json:"text_channel_id"`
	MessageID     string    `json:"message_id"`
	UserID        string    `json:"user_id"`
	Username      string    `json:"username,omitempty"`
	AvatarURL     string    `json:"avatar_url,omitempty"`
	IsBot         bool      `json:"is_bot,omitempty"`
	Content       string    `json:"content"`
	CreatedAt     time.Time `json:"created_at"`
}

type EventSink interface {
	VoiceUserJoined(event VoiceJoinEvent)
	ParticipantChanged(event ParticipantEvent)
	ChatMessageReceived(event ChatMessageEvent)
	ActiveSpeakerDetected(streamID, userID string)
	DiscordConnected()
	DiscordDisconnected(reason string)
}

// ActiveSpeakerStateSink is an optional extension for consumers that need both
// speaking-start and speaking-stop notifications. EventSink remains stable for
// lightweight consumers that only care about the currently detected speaker.
type ActiveSpeakerStateSink interface {
	ActiveSpeakerStateChanged(streamID, userID string, speaking bool)
}

type EventSource interface {
	SetEventSink(sink EventSink)
}

// ParticipantSnapshotSink is implemented by consumers that can replace their
// participant state from an authoritative Discord State snapshot. EventSink is
// intentionally kept backward-compatible for lightweight consumers that only
// need transition notifications.
type ParticipantSnapshotSink interface {
	ParticipantsSynced(snapshot ParticipantSnapshot)
}

// ParticipantSnapshotSource is implemented by Discord clients that can read a
// current voice-channel membership snapshot after a job is joined.
type ParticipantSnapshotSource interface {
	SnapshotVoiceParticipants(job VoiceJob) (ParticipantSnapshot, bool)
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
	SendMessage(ctx context.Context, message OutboundMessage) (SentMessage, error)
	Status() Status
}

type Status struct {
	Connected                 bool    `json:"connected"`
	VoiceConnected            bool    `json:"voice_connected"`
	CurrentGuildID            string  `json:"current_guild_id,omitempty"`
	CurrentVoiceID            string  `json:"current_voice_channel_id,omitempty"`
	AudioForwardEnabled       bool    `json:"audio_forward_enabled"`
	AudioForwardActive        bool    `json:"audio_forward_active"`
	CaptionAudioForwardActive bool    `json:"caption_audio_forward_active"`
	AudioReceiving            bool    `json:"audio_receiving"`
	LastAudioAt               string  `json:"last_audio_at,omitempty"`
	LastAudioAgeSec           float64 `json:"last_audio_age_sec,omitempty"`
	AudioPacketsReceived      int64   `json:"audio_packets_received"`
	AudioPacketsForwarded     int64   `json:"audio_packets_forwarded"`
	AudioForwardErrors        int64   `json:"audio_forward_errors"`
	CaptionPacketsForwarded   int64   `json:"caption_packets_forwarded"`
	CaptionForwardErrors      int64   `json:"caption_forward_errors"`
	GatewayReconnectCount     int64   `json:"gateway_reconnect_count"`
	VoiceDisconnectCount      int64   `json:"voice_disconnect_count"`
	LastForwardAt             string  `json:"last_forward_at,omitempty"`
	LastForwardAgeSec         float64 `json:"last_forward_age_sec,omitempty"`
	LastForwardError          string  `json:"last_forward_error,omitempty"`
	LastCaptionForwardError   string  `json:"last_caption_forward_error,omitempty"`
	LastError                 string  `json:"last_error,omitempty"`
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
	cfg           Config
	session       *discordgo.Session
	voice         *discordgo.VoiceConnection
	sink          EventSink
	forward       AudioForwarder
	source        string
	audioStop     chan struct{}
	ssrcUsers     map[uint32]string
	audioSpeakers map[string]time.Time
	mu            sync.Mutex
	// participantSyncMu serializes snapshot creation and delivery. DiscordGo
	// invokes typed handlers asynchronously, so an older handler must always
	// re-read the latest State while holding this gate before it can publish.
	participantSyncMu       sync.Mutex
	participantSnapshotNext uint64
	status                  Status
	job                     VoiceJob
	voiceGeneration         uint64
}

const audioSpeakerIdleTimeout = 1250 * time.Millisecond

func NewRealClient(cfg Config) (*RealClient, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	installSafeDAVELogSink()
	session, err := discordgo.New("Bot " + cfg.BotToken)
	if err != nil {
		return nil, err
	}
	session.Identify.Intents = discordgo.IntentsGuilds | discordgo.IntentsGuildVoiceStates | discordgo.IntentsGuildMessages | discordgo.IntentsMessageContent
	client := &RealClient{cfg: cfg, session: session}
	session.AddHandler(client.onGatewayDisconnect)
	session.AddHandler(client.onReady)
	session.AddHandler(client.onGatewayResumed)
	session.AddHandler(client.onGuildCreate)
	session.AddHandler(client.onVoiceStateUpdate)
	session.AddHandler(client.onMessageCreate)
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
	c.voiceGeneration++
	c.audioStop = audioStop
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
	c.status.LastError = ""
	c.mu.Unlock()
	if encoderForwardActive || captionForwardActive {
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
	if streamID == "" || userID == "" || sink == nil {
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

func (c *RealClient) forwardOpus(job VoiceJob, packets <-chan *discordgo.Packet, stop <-chan struct{}, forwarder AudioForwarder, source string) {
	encoderBatchMax := 20
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
	unresolvedWindow := time.Duration(job.UnresolvedSSRCBufferMS) * time.Millisecond
	if unresolvedWindow <= 0 {
		unresolvedWindow = time.Second
	}

	runCtx, cancel := context.WithCancel(context.Background())
	monitorDone := make(chan struct{})
	go func() {
		select {
		case <-stop:
			cancel()
		case <-monitorDone:
		}
	}()
	defer func() {
		close(monitorDone)
		cancel()
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
		url := job.EncoderAudioURL
		token := job.StreamIngestToken
		if isCaption {
			url = job.CaptionAudioURL
			token = job.CaptionAudioToken
			if strings.TrimSpace(token) == "" {
				c.setCaptionForwardError(errors.New("caption_audio_token is required").Error())
				return
			}
		}
		ctx, cancel := context.WithTimeout(runCtx, 5*time.Second)
		err := forwarder.ForwardOpus(ctx, url, job.StreamID, source, token, batch)
		cancel()
		if err != nil {
			if isCaption {
				c.setCaptionForwardError(err.Error())
			} else {
				c.setForwardError(err.Error())
			}
			return
		}
		now := time.Now().UTC()
		c.mu.Lock()
		if isCaption {
			c.status.CaptionPacketsForwarded += int64(len(batch))
			c.status.LastCaptionForwardError = ""
		} else {
			c.status.AudioPacketsForwarded += int64(len(batch))
			c.status.LastForwardAt = now.Format(time.RFC3339Nano)
			c.status.LastForwardError = ""
		}
		c.mu.Unlock()
	}
	appendPacket := func(packet audioforward.OpusPacket) {
		if strings.TrimSpace(job.EncoderAudioURL) != "" {
			encoderBatch = append(encoderBatch, packet)
		}
		if strings.TrimSpace(job.CaptionAudioURL) != "" {
			captionBatch = append(captionBatch, packet)
		}
	}
	flushExpiredUnresolved := func(now time.Time, force bool) {
		for ssrc, buffered := range unresolved {
			userID := c.userForSSRC(ssrc)
			if userID == "" {
				userID = resolveFallbackUser(now)
			}
			if userID == "" && !force && now.Sub(unresolvedSince[ssrc]) < unresolvedWindow {
				continue
			}
			if userID != "" {
				c.recordAudioSpeakerActivity(job, userID, now)
			}
			for _, packet := range buffered {
				packet.UserID = userID
				appendPacket(packet)
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
			now = now.UTC()
			flushExpiredUnresolved(now, false)
			if now.Sub(lastCaptionFlush) >= captionFlush {
				flush(true)
				lastCaptionFlush = now
			}
			if now.Sub(lastEncoderFlush) >= 500*time.Millisecond {
				flush(false)
				lastEncoderFlush = now
			}
			c.expireIdleAudioSpeakers(job, now)
		case packet, ok := <-packets:
			if !ok {
				c.markVoiceDisconnected("opus_recv_closed", false)
				return
			}
			if packet == nil || len(packet.Opus) == 0 {
				continue
			}
			now := time.Now().UTC()
			userID := c.userForSSRC(packet.SSRC)
			if userID == "" {
				// Discord can deliver Opus before the first SSRC speaking update.
				// When the target VC has exactly one human participant, use that
				// authoritative snapshot so Deepgram and the scene do not fall back
				// to the synthetic MIC speaker.
				userID = resolveFallbackUser(now)
			}
			c.recordAudioSpeakerActivity(job, userID, now)
			c.mu.Lock()
			c.status.AudioReceiving = true
			c.status.LastAudioAt = now.Format(time.RFC3339Nano)
			c.status.AudioPacketsReceived++
			c.mu.Unlock()
			forwardedPacket := audioforward.OpusPacket{
				SSRC:                 packet.SSRC,
				UserID:               userID,
				Sequence:             packet.Sequence,
				Timestamp:            packet.Timestamp,
				ReceivedAt:           now,
				Opus:                 append([]byte(nil), packet.Opus...),
				JobGeneration:        job.JobGeneration,
				ConnectionGeneration: c.connectionGenerationForJob(job),
			}
			if userID == "" && unresolvedWindow > 0 {
				if len(unresolved[packet.SSRC]) < 50 {
					unresolved[packet.SSRC] = append(unresolved[packet.SSRC], forwardedPacket)
					if unresolvedSince[packet.SSRC].IsZero() {
						unresolvedSince[packet.SSRC] = now
					}
				}
			} else {
				flushExpiredUnresolved(now, false)
				appendPacket(forwardedPacket)
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

func (c *RealClient) connectionGenerationForJob(job VoiceJob) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.job.StreamID != job.StreamID || c.job.GuildID != job.GuildID || c.job.VoiceChannelID != job.VoiceChannelID {
		return 0
	}
	return c.voiceGeneration
}

func (c *RealClient) recordAudioSpeakerActivity(job VoiceJob, userID string, now time.Time) {
	userID = strings.TrimSpace(userID)
	if userID == "" {
		return
	}
	c.mu.Lock()
	if c.job.StreamID != job.StreamID || c.job.GuildID != job.GuildID || c.job.VoiceChannelID != job.VoiceChannelID {
		c.mu.Unlock()
		return
	}
	if c.audioSpeakers == nil {
		c.audioSpeakers = map[string]time.Time{}
	}
	_, alreadyActive := c.audioSpeakers[userID]
	c.audioSpeakers[userID] = now
	streamID := c.job.StreamID
	sink := c.sink
	c.mu.Unlock()
	if alreadyActive || sink == nil {
		return
	}
	if stateSink, ok := sink.(ActiveSpeakerStateSink); ok {
		stateSink.ActiveSpeakerStateChanged(streamID, userID, true)
	} else {
		sink.ActiveSpeakerDetected(streamID, userID)
	}
}

func (c *RealClient) expireIdleAudioSpeakers(job VoiceJob, now time.Time) {
	c.mu.Lock()
	if c.job.StreamID != job.StreamID || c.job.GuildID != job.GuildID || c.job.VoiceChannelID != job.VoiceChannelID {
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
	streamID := c.job.StreamID
	sink := c.sink
	c.mu.Unlock()
	stateSink, ok := sink.(ActiveSpeakerStateSink)
	if !ok {
		return
	}
	for _, userID := range expired {
		stateSink.ActiveSpeakerStateChanged(streamID, userID, false)
	}
}

func (c *RealClient) userForSSRC(ssrc uint32) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ssrcUsers[ssrc]
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

func (c *RealClient) onReady(session *discordgo.Session, _ *discordgo.Ready) {
	c.mu.Lock()
	c.status.Connected = true
	c.status.LastError = ""
	sink := c.sink
	c.mu.Unlock()
	if sink != nil {
		sink.DiscordConnected()
	}
	// READY is the non-resumable reconnect counterpart to RESUMED. Its guild
	// cache may still be filling, so this best-effort snapshot is followed by
	// the authoritative GUILD_CREATE path below.
	c.syncCurrentVoiceParticipants(session)
}

func (c *RealClient) onGatewayResumed(session *discordgo.Session, _ *discordgo.Resumed) {
	c.mu.Lock()
	c.status.Connected = true
	c.status.GatewayReconnectCount++
	c.status.LastError = ""
	sink := c.sink
	c.mu.Unlock()
	if sink != nil {
		sink.DiscordConnected()
	}
	c.syncCurrentVoiceParticipants(session)
}

// onGuildCreate is also emitted while DiscordGo is rebuilding State after a
// non-resumable gateway reconnect. Unlike Ready, this event carries a complete
// guild snapshot, so an empty target channel is authoritative rather than a
// transient cache gap.
func (c *RealClient) onGuildCreate(session *discordgo.Session, event *discordgo.GuildCreate) {
	if event == nil || event.Guild == nil {
		return
	}
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
	c.mu.Unlock()
	selfUserID := sessionUserID(session)
	// While no job is active, this event is the auto-start trigger itself. The
	// DiscordGo state cache can still be one update behind (or briefly omit the
	// user while a guild is being rebuilt), so do not discard a real join just
	// because the snapshot is non-authoritative at this point. Once a job is
	// active, retain the current-state fence so delayed joins cannot resurrect a
	// participant that has already left.
	currentStateAcceptsJoin := job.StreamID == "" || !currentVoiceStateKnown || currentVoiceChannelID == event.ChannelID
	if sink != nil && event.ChannelID != "" && event.UserID != "" && event.UserID != selfUserID && (event.BeforeUpdate == nil || event.BeforeUpdate.ChannelID != event.ChannelID) && currentStateAcceptsJoin {
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
		c.markVoiceDisconnected("voice_state_disconnected", true)
		return
	}
	if sink == nil {
		return
	}
	// The bot itself is present in its voice channel, but must never count as
	// a participant. Counting it prevents the empty-channel auto-stop path.
	if event.UserID == selfUserID {
		return
	}
	if _, supportsSnapshots := sink.(ParticipantSnapshotSink); supportsSnapshots {
		if event.ChannelID == job.VoiceChannelID || (event.BeforeUpdate != nil && event.BeforeUpdate.ChannelID == job.VoiceChannelID) {
			c.syncVoiceParticipants(session, job)
		}
		return
	}
	if event.ChannelID == job.VoiceChannelID {
		if currentVoiceStateKnown && currentVoiceChannelID != job.VoiceChannelID {
			return
		}
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
		if currentVoiceStateKnown && currentVoiceChannelID == job.VoiceChannelID {
			return
		}
		sink.ParticipantChanged(ParticipantEvent{
			StreamID:       job.StreamID,
			GuildID:        job.GuildID,
			VoiceChannelID: job.VoiceChannelID,
			UserID:         event.UserID,
			Present:        false,
		})
	}
}

func (c *RealClient) onMessageCreate(session *discordgo.Session, event *discordgo.MessageCreate) {
	if event == nil || event.Message == nil || event.Author == nil {
		return
	}
	if event.Author.ID == sessionUserID(session) {
		return
	}
	c.mu.Lock()
	job := c.job
	sink := c.sink
	c.mu.Unlock()
	if sink == nil || strings.TrimSpace(job.StreamID) == "" || strings.TrimSpace(job.TextChannelID) == "" {
		return
	}
	if event.ChannelID != job.TextChannelID || event.GuildID != job.GuildID {
		return
	}
	content := strings.TrimSpace(event.Content)
	if content == "" {
		return
	}
	createdAt := time.Now().UTC()
	if !event.Timestamp.IsZero() {
		createdAt = time.Time(event.Timestamp).UTC()
	}
	sink.ChatMessageReceived(ChatMessageEvent{
		StreamID:      job.StreamID,
		GuildID:       job.GuildID,
		TextChannelID: job.TextChannelID,
		MessageID:     event.ID,
		UserID:        event.Author.ID,
		Username:      discordMessageDisplayName(event.Message),
		AvatarURL:     strings.TrimSpace(event.Author.AvatarURL("128")),
		IsBot:         event.Author.Bot,
		Content:       content,
		CreatedAt:     createdAt,
	})
}

func discordMessageDisplayName(message *discordgo.Message) string {
	if message == nil || message.Author == nil {
		return ""
	}
	if message.Member != nil {
		if nickname := strings.TrimSpace(message.Member.Nick); nickname != "" {
			return nickname
		}
	}
	if globalName := strings.TrimSpace(message.Author.GlobalName); globalName != "" {
		return globalName
	}
	return strings.TrimSpace(message.Author.Username)
}

// SnapshotVoiceParticipants returns one current, authoritative view of a
// joined job's target VC. A missing guild or disabled voice State is explicitly
// non-authoritative: callers must retain their previous view rather than
// mistaking a cache gap for an empty channel.
func (c *RealClient) SnapshotVoiceParticipants(job VoiceJob) (ParticipantSnapshot, bool) {
	c.participantSyncMu.Lock()
	defer c.participantSyncMu.Unlock()

	c.mu.Lock()
	current := c.job
	session := c.session
	c.mu.Unlock()
	if !sameVoiceJob(current, job) {
		return ParticipantSnapshot{}, false
	}
	return c.snapshotVoiceParticipantsLocked(session, current)
}

func (c *RealClient) syncCurrentVoiceParticipants(session *discordgo.Session) {
	c.mu.Lock()
	job := c.job
	c.mu.Unlock()
	if strings.TrimSpace(job.StreamID) == "" {
		return
	}
	c.syncVoiceParticipants(session, job)
}

func (c *RealClient) syncVoiceParticipants(session *discordgo.Session, expected VoiceJob) {
	c.participantSyncMu.Lock()
	defer c.participantSyncMu.Unlock()

	c.mu.Lock()
	job := c.job
	sink := c.sink
	c.mu.Unlock()
	if !sameVoiceJob(job, expected) {
		return
	}
	snapshotSink, ok := sink.(ParticipantSnapshotSink)
	if !ok {
		return
	}
	if session == nil {
		session = c.session
	}
	snapshot, ok := c.snapshotVoiceParticipantsLocked(session, job)
	if !ok {
		return
	}
	snapshotSink.ParticipantsSynced(snapshot)
}

// snapshotVoiceParticipantsLocked must be called while participantSyncMu is
// held. It takes DiscordGo's State read lock only while copying the small,
// target-VC subset, then invokes no sink while that State lock is held.
func (c *RealClient) snapshotVoiceParticipantsLocked(session *discordgo.Session, job VoiceJob) (ParticipantSnapshot, bool) {
	if session == nil || session.State == nil || !session.State.TrackVoice {
		return ParticipantSnapshot{}, false
	}
	job.StreamID = strings.TrimSpace(job.StreamID)
	job.GuildID = strings.TrimSpace(job.GuildID)
	job.VoiceChannelID = strings.TrimSpace(job.VoiceChannelID)
	if job.StreamID == "" || job.GuildID == "" || job.VoiceChannelID == "" {
		return ParticipantSnapshot{}, false
	}

	state := session.State
	state.RLock()
	defer state.RUnlock()
	selfUserID := ""
	if state.User != nil {
		selfUserID = strings.TrimSpace(state.User.ID)
	}
	if selfUserID == "" {
		return ParticipantSnapshot{}, false
	}
	for _, guild := range state.Guilds {
		if guild == nil || guild.ID != job.GuildID {
			continue
		}
		participants := make([]VoiceParticipant, 0, len(guild.VoiceStates))
		for _, voiceState := range guild.VoiceStates {
			if voiceState == nil || voiceState.ChannelID != job.VoiceChannelID {
				continue
			}
			userID := strings.TrimSpace(voiceState.UserID)
			if userID == "" || userID == selfUserID {
				continue
			}
			username := ""
			avatarURL := ""
			isBot := false
			if voiceState.Member != nil && voiceState.Member.User != nil {
				member := voiceState.Member
				user := member.User
				username = strings.TrimSpace(member.Nick)
				if username == "" {
					username = strings.TrimSpace(user.GlobalName)
				}
				if username == "" {
					username = strings.TrimSpace(user.Username)
				}
				memberForAvatar := *member
				if strings.TrimSpace(memberForAvatar.GuildID) == "" {
					memberForAvatar.GuildID = guild.ID
				}
				avatarURL = strings.TrimSpace(memberForAvatar.AvatarURL("128"))
				if avatarURL == "" {
					avatarURL = strings.TrimSpace(user.AvatarURL("128"))
				}
				isBot = user.Bot
			}
			participants = append(participants, VoiceParticipant{UserID: userID, Username: username, AvatarURL: avatarURL, IsBot: isBot})
		}
		sort.Slice(participants, func(i, j int) bool {
			return participants[i].UserID < participants[j].UserID
		})
		c.participantSnapshotNext++
		return ParticipantSnapshot{
			StreamID:       job.StreamID,
			GuildID:        job.GuildID,
			VoiceChannelID: job.VoiceChannelID,
			Participants:   participants,
			Revision:       c.participantSnapshotNext,
		}, true
	}
	return ParticipantSnapshot{}, false
}

func sameVoiceJob(left, right VoiceJob) bool {
	return strings.TrimSpace(left.StreamID) == strings.TrimSpace(right.StreamID) &&
		strings.TrimSpace(left.GuildID) == strings.TrimSpace(right.GuildID) &&
		strings.TrimSpace(left.VoiceChannelID) == strings.TrimSpace(right.VoiceChannelID)
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

func (c *RealClient) markVoiceDisconnected(reason string, closeAudioStop bool) {
	c.mu.Lock()
	wasConnected := c.status.VoiceConnected
	if closeAudioStop && c.audioStop != nil {
		close(c.audioStop)
		c.audioStop = nil
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

func (c *RealClient) setCaptionForwardError(value string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.status.CaptionForwardErrors++
	safeValue := secrets.SanitizeOperationalError(value, "discord caption audio forward failed")
	c.status.LastCaptionForwardError = safeValue
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
