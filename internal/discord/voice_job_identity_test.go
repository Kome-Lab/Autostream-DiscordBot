package discord

import (
	"encoding/json"
	"testing"
	"time"
)

func TestSameVoiceJobIncludesDiscordTargetRevision(t *testing.T) {
	base := VoiceJob{
		StreamID:              "stream-01",
		JobGeneration:         17,
		DiscordTargetRevision: 29,
		GuildID:               "100000000000000001",
		VoiceChannelID:        "100000000000000003",
		TextChannelID:         "100000000000000002",
		StreamIngestToken:     "job-secret",
	}
	if !sameVoiceJob(base, base) {
		t.Fatal("identical v2 voice job must match itself")
	}
	differentRevision := base
	differentRevision.DiscordTargetRevision++
	if sameVoiceJob(base, differentRevision) {
		t.Fatal("different Discord target revisions must be different voice job identities")
	}
	differentTextChannel := base
	differentTextChannel.TextChannelID = "200000000000000002"
	if sameVoiceJob(base, differentTextChannel) {
		t.Fatal("different resolved text channels must be different voice job identities")
	}

	encoded, err := json.Marshal(base)
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) != "{}" {
		t.Fatalf("internal voice job must not be JSON-serializable: %s", encoded)
	}
}

func TestUnresolvedSSRCBufferWindowPreservesExplicitZero(t *testing.T) {
	tests := []struct {
		name string
		job  VoiceJob
		want time.Duration
	}{
		{name: "legacy or omitted default", job: VoiceJob{}, want: time.Second},
		{name: "v2 explicit zero", job: VoiceJob{UnresolvedSSRCBufferMSSet: true}, want: 0},
		{name: "v2 positive", job: VoiceJob{UnresolvedSSRCBufferMS: 5000, UnresolvedSSRCBufferMSSet: true}, want: 5 * time.Second},
		{name: "defensive negative", job: VoiceJob{UnresolvedSSRCBufferMS: -1, UnresolvedSSRCBufferMSSet: true}, want: time.Second},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := unresolvedSSRCBufferWindow(test.job); got != test.want {
				t.Fatalf("unresolvedSSRCBufferWindow()=%s, want %s", got, test.want)
			}
		})
	}
}
