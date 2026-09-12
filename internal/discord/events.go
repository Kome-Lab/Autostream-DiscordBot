package discord

import (
	"time"
)

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
