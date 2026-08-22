package discord

import (
	"context"
	"errors"
	"log"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cartridge-gg/discordgo"
	"github.com/example/autostream-discord-bot/internal/audioforward"
	"github.com/example/autostream-discord-bot/internal/davewatch"
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
	JobGeneration               uint64 `json:"job_generation"`
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

// AutoStartVoiceTarget identifies a waiting stream whose Discord VC should be
// checked when the Gateway state becomes available. The normal VoiceStateUpdate
// event is not emitted for users who were already in the VC before the stream
// configuration was loaded, so the client must also be able to inspect the
// cached guild state.
type AutoStartVoiceTarget struct {
	StreamID       string
	GuildID        string
	VoiceChannelID string
}

// AutoStartVoiceTargetSetter is an optional extension used by the job manager
// when Control Panel runtime configuration is refreshed.
type AutoStartVoiceTargetSetter interface {
	SetAutoStartVoiceTargets(targets []AutoStartVoiceTarget)
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
	AudioForwardQueueDrops    int64   `json:"audio_forward_queue_drops"`
	CaptionPacketsForwarded   int64   `json:"caption_packets_forwarded"`
	CaptionForwardErrors      int64   `json:"caption_forward_errors"`
	CaptionForwardQueueDrops  int64   `json:"caption_forward_queue_drops"`
	GatewayReconnectCount     int64   `json:"gateway_reconnect_count"`
	VoiceDisconnectCount      int64   `json:"voice_disconnect_count"`
	DAVEInitialized           bool    `json:"dave_initialized"`
	DAVEReady                 bool    `json:"dave_ready"`
	DAVEWelcomeReceived       bool    `json:"dave_welcome_received"`
	DAVERosterSize            int     `json:"dave_roster_size"`
	DAVERatchetsMissing       int     `json:"dave_ratchets_missing"`
	DAVEKeyPackageResends     int64   `json:"dave_key_package_resends"`
	DAVESoftResets            int64   `json:"dave_soft_resets"`
	DAVERecoveryErrors        int64   `json:"dave_recovery_errors"`
	DAVELastRecoveryReason    string  `json:"dave_last_recovery_reason,omitempty"`
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
	cfg              Config
	session          *discordgo.Session
	voice            *discordgo.VoiceConnection
	sink             EventSink
	autoStartTargets []AutoStartVoiceTarget
	forward          AudioForwarder
	source           string
	audioStop        chan struct{}
	audioDone        chan struct{}
	daveCancel       context.CancelFunc
	ssrcUsers        map[uint32]string
	audioSpeakers    map[string]time.Time
	mu               sync.Mutex
	// speakerDispatchMu serializes active-speaker callbacks with voice job
	// transitions. Callers must never hold mu while acquiring this gate or
	// invoking the external sink; Manager.Status takes the inverse Manager ->
	// RealClient lock path.
	speakerDispatchMu sync.Mutex
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

func (c *RealClient) SetAutoStartVoiceTargets(targets []AutoStartVoiceTarget) {
	normalized := make([]AutoStartVoiceTarget, 0, len(targets))
	seen := make(map[string]struct{}, len(targets))
	for _, target := range targets {
		target.StreamID = strings.TrimSpace(target.StreamID)
		target.GuildID = strings.TrimSpace(target.GuildID)
		target.VoiceChannelID = strings.TrimSpace(target.VoiceChannelID)
		if target.StreamID == "" || target.GuildID == "" || target.VoiceChannelID == "" {
			continue
		}
		key := target.StreamID + "\x00" + target.GuildID + "\x00" + target.VoiceChannelID
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		normalized = append(normalized, target)
	}

	c.mu.Lock()
	c.autoStartTargets = normalized
	session := c.session
	c.mu.Unlock()
	c.syncAutoStartVoiceMembers(session)
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

const (
	opusForwardQueueBatches  = 64
	opusForwardStopWait      = 2 * time.Second
	opusForwardRequestLimit  = 5 * time.Second
	opusEncoderBatchPackets  = 50
	opusEncoderFlushInterval = 100 * time.Millisecond
)

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

func (c *RealClient) Status() Status {
	c.mu.Lock()
	status := c.status
	voice := c.voice
	c.mu.Unlock()
	if voice != nil {
		health := voice.DAVEHealth()
		status.DAVEInitialized = health.Initialized
		status.DAVEReady = health.EpochEstablished
		status.DAVEWelcomeReceived = health.OP30Received
		status.DAVERosterSize = health.LastRosterSize
		status.DAVERatchetsMissing = health.LastMissing
	}
	return normalizeStatus(status, time.Now().UTC())
}

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

// syncAutoStartVoiceMembers emits at most one synthetic join event per
// configured target when a human is already present in that VC. It never calls
// the event sink while DiscordGo's State lock is held, and the manager's
// existing pending/start fences make a repeated Gateway or runtime refresh
// harmless.
func (c *RealClient) syncAutoStartVoiceMembers(session *discordgo.Session) {
	c.mu.Lock()
	targets := append([]AutoStartVoiceTarget(nil), c.autoStartTargets...)
	sink := c.sink
	if session == nil {
		session = c.session
	}
	c.mu.Unlock()
	if sink == nil || session == nil || session.State == nil || !session.State.TrackVoice || len(targets) == 0 {
		return
	}

	selfUserID := sessionUserID(session)
	if selfUserID == "" {
		return
	}
	events := make([]VoiceJoinEvent, 0, len(targets))
	state := session.State
	state.RLock()
	for _, target := range targets {
		for _, guild := range state.Guilds {
			if guild == nil || guild.ID != target.GuildID {
				continue
			}
			for _, voiceState := range guild.VoiceStates {
				if voiceState == nil || voiceState.ChannelID != target.VoiceChannelID {
					continue
				}
				userID := strings.TrimSpace(voiceState.UserID)
				if userID == "" || userID == selfUserID {
					continue
				}
				if voiceState.Member != nil && voiceState.Member.User != nil && voiceState.Member.User.Bot {
					continue
				}
				username := ""
				if voiceState.Member != nil && voiceState.Member.User != nil {
					username = strings.TrimSpace(voiceState.Member.Nick)
					if username == "" {
						username = strings.TrimSpace(voiceState.Member.User.GlobalName)
					}
					if username == "" {
						username = strings.TrimSpace(voiceState.Member.User.Username)
					}
				}
				events = append(events, VoiceJoinEvent{
					GuildID:        target.GuildID,
					VoiceChannelID: target.VoiceChannelID,
					UserID:         userID,
					Username:       username,
				})
				break
			}
			break
		}
	}
	state.RUnlock()

	for _, event := range events {
		sink.VoiceUserJoined(event)
	}
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
			participant := voiceParticipantFromState(guild.ID, voiceState)
			userID := participant.UserID
			if userID == "" || userID == selfUserID {
				continue
			}
			participants = append(participants, participant)
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

func voiceParticipantFromState(guildID string, voiceState *discordgo.VoiceState) VoiceParticipant {
	if voiceState == nil {
		return VoiceParticipant{}
	}
	participant := VoiceParticipant{UserID: strings.TrimSpace(voiceState.UserID)}
	if voiceState.Member == nil || voiceState.Member.User == nil {
		return participant
	}
	member := voiceState.Member
	user := member.User
	participant.Username = strings.TrimSpace(member.Nick)
	if participant.Username == "" {
		participant.Username = strings.TrimSpace(user.GlobalName)
	}
	if participant.Username == "" {
		participant.Username = strings.TrimSpace(user.Username)
	}
	memberForAvatar := *member
	if strings.TrimSpace(memberForAvatar.GuildID) == "" {
		memberForAvatar.GuildID = strings.TrimSpace(guildID)
	}
	participant.AvatarURL = strings.TrimSpace(memberForAvatar.AvatarURL("128"))
	if participant.AvatarURL == "" {
		participant.AvatarURL = strings.TrimSpace(user.AvatarURL("128"))
	}
	participant.IsBot = user.Bot
	return participant
}

func sameVoiceJob(left, right VoiceJob) bool {
	return strings.TrimSpace(left.StreamID) == strings.TrimSpace(right.StreamID) &&
		strings.TrimSpace(left.GuildID) == strings.TrimSpace(right.GuildID) &&
		strings.TrimSpace(left.VoiceChannelID) == strings.TrimSpace(right.VoiceChannelID) &&
		left.JobGeneration == right.JobGeneration
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
