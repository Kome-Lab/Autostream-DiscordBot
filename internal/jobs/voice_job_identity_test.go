package jobs

import (
	"testing"

	"github.com/example/autostream-discord-bot/internal/discord"
)

func TestSameWorkerEventJobIncludesDiscordTargetRevision(t *testing.T) {
	base := discord.VoiceJob{
		StreamID:              "stream-01",
		JobGeneration:         17,
		DiscordTargetRevision: 29,
		GuildID:               "100000000000000001",
		VoiceChannelID:        "100000000000000003",
		TextChannelID:         "100000000000000002",
	}
	if !sameWorkerEventJob(base, base) {
		t.Fatal("identical v2 worker-event job must match itself")
	}
	differentRevision := base
	differentRevision.DiscordTargetRevision++
	if sameWorkerEventJob(base, differentRevision) {
		t.Fatal("different Discord target revisions must fence worker event retries")
	}
	differentTextChannel := base
	differentTextChannel.TextChannelID = "200000000000000002"
	if sameWorkerEventJob(base, differentTextChannel) {
		t.Fatal("different resolved text channels must fence worker event retries")
	}
}
