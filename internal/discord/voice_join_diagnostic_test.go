package discord

import (
	"bytes"
	"log"
	"strings"
	"testing"
)

func TestVoiceJoinDiagnosticContainsOnlySafeStageFields(t *testing.T) {
	var output bytes.Buffer
	previousWriter, previousFlags := log.Writer(), log.Flags()
	log.SetOutput(&output)
	log.SetFlags(0)
	defer func() { log.SetOutput(previousWriter); log.SetFlags(previousFlags) }()
	job := VoiceJob{StreamID: "selected-stream", JobGeneration: 17, DiscordTargetRevision: 29, GuildID: "private-guild", VoiceChannelID: "private-voice", EncoderAudioURL: "https://encoder.invalid/audio", CaptionAudioURL: "https://caption.invalid/audio", WorkerEventsURL: "https://worker.invalid/events", StreamIngestToken: "private-ingest-capability", CaptionAudioToken: "private-caption-capability", WorkerEventsToken: "private-worker-capability"}
	logVoiceJoinDiagnostic(job, "voice_transport_failed")
	message := output.String()
	for _, expected := range []string{"stream_id=selected-stream", "job_generation=17", "target_revision=29", "reason=voice_transport_failed"} {
		if !strings.Contains(message, expected) {
			t.Fatalf("diagnostic missing stage field %q", expected)
		}
	}
	for _, forbidden := range []string{"https://", "encoder.invalid", "caption.invalid", "worker.invalid", "private-", "token", "authorization"} {
		if strings.Contains(message, forbidden) {
			t.Fatalf("diagnostic included a forbidden private field %q", forbidden)
		}
	}
}
