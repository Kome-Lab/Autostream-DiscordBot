package httpapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/example/autostream-discord-bot/internal/discord"
)

type startJobRequestMode uint8

const (
	startJobRequestLegacy startJobRequestMode = iota
	startJobRequestResolvedTargetV2
)

const (
	startJobCodeInvalidJSON              = "invalid_json"
	startJobCodeUnsupportedSchemaVersion = "unsupported_schema_version"
	startJobCodeDiscordTargetInvalid     = "discord_target_invalid"
)

// startJobRequestError deliberately carries only one bounded public code. JSON
// decoder details can include attacker-controlled field names or values and are
// never reflected by /jobs/start.
type startJobRequestError struct {
	code string
}

func (e startJobRequestError) Error() string { return e.code }

type legacyStartJobRequest struct {
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

type resolvedDiscordTarget struct {
	GuildID        string `json:"guild_id"`
	TextChannelID  string `json:"text_channel_id"`
	VoiceChannelID string `json:"voice_channel_id"`
}

type discordTargetSnapshot struct {
	Revision uint64                `json:"revision"`
	Resolved resolvedDiscordTarget `json:"resolved"`
}

type v2StartJobRequest struct {
	SchemaVersion               uint64                `json:"schema_version"`
	StreamID                    string                `json:"stream_id"`
	JobGeneration               uint64                `json:"job_generation"`
	DiscordTarget               discordTargetSnapshot `json:"discord_target"`
	EncoderAudioURL             string                `json:"encoder_audio_url,omitempty"`
	CaptionAudioURL             string                `json:"caption_audio_url,omitempty"`
	CaptionAudioToken           string                `json:"caption_audio_token,omitempty"`
	StreamIngestToken           string                `json:"stream_ingest_token,omitempty"`
	WorkerEventsURL             string                `json:"worker_events_url,omitempty"`
	WorkerEventsToken           string                `json:"worker_events_token,omitempty"`
	CaptionAudioFlushMS         optionalInt           `json:"caption_audio_flush_ms,omitempty"`
	CaptionAudioMaxBatchPackets optionalInt           `json:"caption_audio_max_batch_packets,omitempty"`
	UnresolvedSSRCBufferMS      optionalInt           `json:"unresolved_ssrc_buffer_ms,omitempty"`
}

// optionalInt distinguishes an omitted optional integer from an explicit zero.
// That keeps legacy zero-value compatibility outside the v2 DTO while allowing
// v2 to reject null and enforce the canonical Caption Profile bounds.
type optionalInt struct {
	present bool
	value   int
}

func (value *optionalInt) UnmarshalJSON(data []byte) error {
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		return errors.New("optional integer must not be null")
	}
	var parsed int
	if err := json.Unmarshal(data, &parsed); err != nil {
		return err
	}
	value.present = true
	value.value = parsed
	return nil
}

func (value optionalInt) within(minimum, maximum int) bool {
	return !value.present || (value.value >= minimum && value.value <= maximum)
}

func (value optionalInt) orZero() int {
	if !value.present {
		return 0
	}
	return value.value
}

func decodeStartJobRequest(r *http.Request) (discord.VoiceJob, startJobRequestMode, error) {
	body, err := readStartJobRequestBody(r)
	if err != nil {
		return discord.VoiceJob{}, startJobRequestLegacy, startJobRequestError{code: startJobCodeInvalidJSON}
	}

	// The envelope pass chooses the versioned DTO only. The selected DTO is
	// decoded again with unknown-field rejection, so this pass never normalizes
	// or resolves Discord targets.
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(body, &envelope); err != nil || envelope == nil {
		return discord.VoiceJob{}, startJobRequestLegacy, startJobRequestError{code: startJobCodeInvalidJSON}
	}
	rawVersion, versioned := envelope["schema_version"]
	if !versioned {
		var req legacyStartJobRequest
		if err := decodeStrictJSON(body, &req); err != nil {
			return discord.VoiceJob{}, startJobRequestLegacy, startJobRequestError{code: startJobCodeInvalidJSON}
		}
		return req.voiceJob(), startJobRequestLegacy, nil
	}

	var version uint64
	if bytes.Equal(bytes.TrimSpace(rawVersion), []byte("null")) {
		return discord.VoiceJob{}, startJobRequestLegacy, startJobRequestError{code: startJobCodeInvalidJSON}
	}
	if err := json.Unmarshal(rawVersion, &version); err != nil {
		return discord.VoiceJob{}, startJobRequestLegacy, startJobRequestError{code: startJobCodeInvalidJSON}
	}
	if version != 2 {
		return discord.VoiceJob{}, startJobRequestLegacy, startJobRequestError{code: startJobCodeUnsupportedSchemaVersion}
	}

	var req v2StartJobRequest
	if err := decodeStrictJSON(body, &req); err != nil {
		return discord.VoiceJob{}, startJobRequestResolvedTargetV2, startJobRequestError{code: startJobCodeInvalidJSON}
	}
	if req.SchemaVersion != 2 || !req.DiscordTarget.valid() {
		return discord.VoiceJob{}, startJobRequestResolvedTargetV2, startJobRequestError{code: startJobCodeDiscordTargetInvalid}
	}
	if !req.captionTuningValid() {
		return discord.VoiceJob{}, startJobRequestResolvedTargetV2, startJobRequestError{code: startJobCodeInvalidJSON}
	}
	return req.voiceJob(), startJobRequestResolvedTargetV2, nil
}

func readStartJobRequestBody(r *http.Request) ([]byte, error) {
	defer r.Body.Close()
	body, err := io.ReadAll(io.LimitReader(r.Body, maxStartJobRequestBytes+1))
	if err != nil {
		return nil, err
	}
	if len(body) > maxStartJobRequestBytes {
		return nil, errors.New("request body exceeds size limit")
	}
	return body, nil
}

func decodeStrictJSON(body []byte, value any) error {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return errors.New("request body must contain exactly one JSON value")
		}
		return err
	}
	return nil
}

func (req legacyStartJobRequest) voiceJob() discord.VoiceJob {
	return discord.VoiceJob{
		GuildID:                     req.GuildID,
		VoiceChannelID:              req.VoiceChannelID,
		TextChannelID:               req.TextChannelID,
		StreamID:                    req.StreamID,
		EncoderAudioURL:             req.EncoderAudioURL,
		CaptionAudioURL:             req.CaptionAudioURL,
		CaptionAudioToken:           req.CaptionAudioToken,
		StreamIngestToken:           req.StreamIngestToken,
		WorkerEventsURL:             req.WorkerEventsURL,
		WorkerEventsToken:           req.WorkerEventsToken,
		CaptionAudioFlushMS:         req.CaptionAudioFlushMS,
		CaptionAudioMaxBatchPackets: req.CaptionAudioMaxBatchPackets,
		UnresolvedSSRCBufferMS:      req.UnresolvedSSRCBufferMS,
		JobGeneration:               req.JobGeneration,
	}
}

func (req v2StartJobRequest) voiceJob() discord.VoiceJob {
	// This is the single v2 wire-to-runtime normalization point. The Discord Bot
	// consumes only the server-resolved IDs and snapshot revision; preset or
	// inheritance metadata is intentionally unrepresentable in this DTO.
	return discord.VoiceJob{
		GuildID:                     req.DiscordTarget.Resolved.GuildID,
		VoiceChannelID:              req.DiscordTarget.Resolved.VoiceChannelID,
		TextChannelID:               req.DiscordTarget.Resolved.TextChannelID,
		DiscordTargetRevision:       req.DiscordTarget.Revision,
		StreamID:                    req.StreamID,
		EncoderAudioURL:             req.EncoderAudioURL,
		CaptionAudioURL:             req.CaptionAudioURL,
		CaptionAudioToken:           req.CaptionAudioToken,
		StreamIngestToken:           req.StreamIngestToken,
		WorkerEventsURL:             req.WorkerEventsURL,
		WorkerEventsToken:           req.WorkerEventsToken,
		CaptionAudioFlushMS:         req.CaptionAudioFlushMS.orZero(),
		CaptionAudioMaxBatchPackets: req.CaptionAudioMaxBatchPackets.orZero(),
		UnresolvedSSRCBufferMS:      req.UnresolvedSSRCBufferMS.orZero(),
		UnresolvedSSRCBufferMSSet:   req.UnresolvedSSRCBufferMS.present,
		JobGeneration:               req.JobGeneration,
	}
}

func (req v2StartJobRequest) captionTuningValid() bool {
	return req.CaptionAudioFlushMS.within(10, 1000) &&
		req.CaptionAudioMaxBatchPackets.within(1, 100) &&
		req.UnresolvedSSRCBufferMS.within(0, 5000)
}

func (target discordTargetSnapshot) valid() bool {
	return target.Revision > 0 &&
		validDiscordID(target.Resolved.GuildID) &&
		validDiscordID(target.Resolved.TextChannelID) &&
		validDiscordID(target.Resolved.VoiceChannelID)
}

func validDiscordID(value string) bool {
	if len(value) < 1 || len(value) > 32 {
		return false
	}
	for _, char := range value {
		if char < '0' || char > '9' {
			return false
		}
	}
	return true
}
