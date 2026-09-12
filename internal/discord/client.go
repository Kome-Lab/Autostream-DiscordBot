package discord

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/cartridge-gg/discordgo"
)

// VoiceJob is normalized runtime state, not an HTTP wire DTO. Every field is
// excluded from JSON so status or diagnostics cannot accidentally expose
// resolved channel IDs, endpoints, or job-scoped credentials.
type VoiceJob struct {
	GuildID                     string `json:"-"`
	VoiceChannelID              string `json:"-"`
	TextChannelID               string `json:"-"`
	DiscordTargetRevision       uint64 `json:"-"`
	StreamID                    string `json:"-"`
	EncoderAudioURL             string `json:"-"`
	CaptionAudioURL             string `json:"-"`
	CaptionAudioToken           string `json:"-"`
	StreamIngestToken           string `json:"-"`
	WorkerEventsURL             string `json:"-"`
	WorkerEventsToken           string `json:"-"`
	CaptionAudioFlushMS         int    `json:"-"`
	CaptionAudioMaxBatchPackets int    `json:"-"`
	UnresolvedSSRCBufferMS      int    `json:"-"`
	UnresolvedSSRCBufferMSSet   bool   `json:"-"`
	JobGeneration               uint64 `json:"-"`
}

type Client interface {
	Connect() error
	JoinVoice(job VoiceJob) error
	LeaveVoice(streamID string) error
	SendMessage(ctx context.Context, message OutboundMessage) (SentMessage, error)
	Status() Status
}

type Config struct {
	BotToken string
}

func (c Config) Validate() error {
	if strings.TrimSpace(c.BotToken) == "" {
		return errors.New("Discord bot runtime secret is required")
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
