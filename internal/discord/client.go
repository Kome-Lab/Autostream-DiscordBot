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
	GuildID           string `json:"guild_id"`
	VoiceChannelID    string `json:"voice_channel_id"`
	TextChannelID     string `json:"text_channel_id,omitempty"`
	StreamID          string `json:"stream_id"`
	EncoderAudioURL   string `json:"encoder_audio_url,omitempty"`
	CaptionAudioURL   string `json:"caption_audio_url,omitempty"`
	CaptionAudioToken string `json:"caption_audio_token,omitempty"`
	StreamIngestToken string `json:"stream_ingest_token,omitempty"`
	WorkerEventsURL   string `json:"worker_events_url,omitempty"`
	WorkerEventsToken string `json:"worker_events_token,omitempty"`
}

type ParticipantEvent struct {
	StreamID       string `json:"stream_id"`
	GuildID        string `json:"guild_id"`
	VoiceChannelID string `json:"voice_channel_id"`
	UserID         string `json:"user_id"`
	Username       string `json:"username,omitempty"`
	Present        bool   `json:"present"`
}

// VoiceParticipant is a non-secret, current member of a tracked Discord voice
// channel. It deliberately carries no gateway session or voice-server fields.
type VoiceParticipant struct {
	UserID   string
	Username string
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
	cfg       Config
	session   *discordgo.Session
	voice     *discordgo.VoiceConnection
	sink      EventSink
	forward   AudioForwarder
	source    string
	audioStop chan struct{}
	ssrcUsers map[uint32]string
	mu        sync.Mutex
	// participantSyncMu serializes snapshot creation and delivery. DiscordGo
	// invokes typed handlers asynchronously, so an older handler must always
	// re-read the latest State while holding this gate before it can publish.
	participantSyncMu       sync.Mutex
	participantSnapshotNext uint64
	status                  Status
	job                     VoiceJob
}

func NewRealClient(cfg Config) (*RealClient, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	session, err := discordgo.New("Bot " + cfg.BotToken)
	if err != nil {
		return nil, err
	}
	session.Identify.Intents = discordgo.IntentsGuilds | discordgo.IntentsGuildVoiceStates | discordgo.IntentsGuildMessages | discordgo.IntentsMessageContent
	client := &RealClient{cfg: cfg, session: session}
	session.AddHandler(client.onGatewayDisconnect)
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
	c.audioStop = audioStop
	c.ssrcUsers = map[uint32]string{}
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
	c.job = VoiceJob{}
	c.ssrcUsers = nil
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
	c.mu.Unlock()
	if speaking && streamID != "" && userID != "" && sink != nil {
		sink.ActiveSpeakerDetected(streamID, userID)
	}
}

func (c *RealClient) forwardOpus(job VoiceJob, packets <-chan *discordgo.Packet, stop <-chan struct{}, forwarder AudioForwarder, source string) {
	const maxBatch = 20
	type forwardTarget struct {
		url       string
		token     string
		isCaption bool
	}
	type forwardResult struct {
		isCaption bool
		err       error
	}
	targets := make([]forwardTarget, 0, 2)
	if strings.TrimSpace(job.EncoderAudioURL) != "" {
		targets = append(targets, forwardTarget{url: job.EncoderAudioURL, token: job.StreamIngestToken})
	}
	if strings.TrimSpace(job.CaptionAudioURL) != "" {
		targets = append(targets, forwardTarget{url: job.CaptionAudioURL, token: job.CaptionAudioToken, isCaption: true})
	}
	flushEvery := time.NewTicker(500 * time.Millisecond)
	defer flushEvery.Stop()
	batch := make([]audioforward.OpusPacket, 0, maxBatch)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		count := len(batch)
		results := make(chan forwardResult, len(targets))
		for _, target := range targets {
			target := target
			go func() {
				if target.isCaption && strings.TrimSpace(target.token) == "" {
					results <- forwardResult{isCaption: true, err: errors.New("caption_audio_token is required")}
					return
				}
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				err := forwarder.ForwardOpus(ctx, target.url, job.StreamID, source, target.token, batch)
				cancel()
				results <- forwardResult{isCaption: target.isCaption, err: err}
			}()
		}
		for range targets {
			result := <-results
			if result.err != nil {
				if result.isCaption {
					c.setCaptionForwardError(result.err.Error())
				} else {
					c.setForwardError(result.err.Error())
				}
				continue
			}
			now := time.Now().UTC()
			c.mu.Lock()
			if result.isCaption {
				c.status.CaptionPacketsForwarded += int64(count)
				c.status.LastCaptionForwardError = ""
			} else {
				c.status.AudioPacketsForwarded += int64(count)
				c.status.LastForwardAt = now.Format(time.RFC3339Nano)
				c.status.LastForwardError = ""
			}
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
	if event.Author.Bot || event.Author.ID == sessionUserID(session) {
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
		Username:      strings.TrimSpace(event.Author.Username),
		Content:       content,
		CreatedAt:     createdAt,
	})
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
			if voiceState.Member != nil && voiceState.Member.User != nil {
				username = strings.TrimSpace(voiceState.Member.User.Username)
			}
			participants = append(participants, VoiceParticipant{UserID: userID, Username: username})
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
