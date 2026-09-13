package jobs

import (
	"context"
	"github.com/example/autostream-discord-bot/internal/discord"
	"testing"
	"time"
)

func (f *fakeVoice) Connect() error { return f.err }
func (f *fakeVoice) JoinVoice(job discord.VoiceJob) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.joined = job
	f.joinCount++
	f.status.Connected = true
	f.status.VoiceConnected = true
	if f.joinCh != nil {
		select {
		case f.joinCh <- job:
		default:
		}
	}
	return nil
}
func (f *fakeVoice) LeaveVoice(streamID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.leftFor = streamID
	f.status.VoiceConnected = false
	return nil
}
func (f *fakeVoice) SendMessage(ctx context.Context, message discord.OutboundMessage) (discord.SentMessage, error) {
	f.mu.Lock()
	f.sentMessages = append(f.sentMessages, message)
	err := f.sendErr
	messageID := f.sendMessageID
	started := f.sendStarted
	release := f.sendRelease
	f.mu.Unlock()
	if started != nil {
		select {
		case started <- struct{}{}:
		default:
		}
	}
	if release != nil {
		select {
		case <-release:
		case <-ctx.Done():
			return discord.SentMessage{}, ctx.Err()
		}
	}
	if err != nil {
		return discord.SentMessage{}, err
	}
	if messageID == "" {
		messageID = "message-01"
	}
	return discord.SentMessage{MessageID: messageID}, nil
}
func (f *fakeVoice) Status() discord.Status {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.status
}

func (f *snapshotVoice) SnapshotVoiceParticipants(job discord.VoiceJob) (discord.ParticipantSnapshot, bool) {
	if !f.known {
		return discord.ParticipantSnapshot{}, false
	}
	snapshot := f.snapshot
	if snapshot.StreamID == "" {
		snapshot.StreamID = job.StreamID
	}
	if snapshot.GuildID == "" {
		snapshot.GuildID = job.GuildID
	}
	if snapshot.VoiceChannelID == "" {
		snapshot.VoiceChannelID = job.VoiceChannelID
	}
	return snapshot, true
}

func (f *sequenceSnapshotVoice) SnapshotVoiceParticipants(job discord.VoiceJob) (discord.ParticipantSnapshot, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.snapshots) == 0 {
		return discord.ParticipantSnapshot{}, false
	}
	index := f.next
	if index >= len(f.snapshots) {
		index = len(f.snapshots) - 1
	} else {
		f.next++
	}
	snapshot := f.snapshots[index]
	if snapshot.StreamID == "" {
		snapshot.StreamID = job.StreamID
	}
	if snapshot.GuildID == "" {
		snapshot.GuildID = job.GuildID
	}
	if snapshot.VoiceChannelID == "" {
		snapshot.VoiceChannelID = job.VoiceChannelID
	}
	if snapshot.Revision == 0 {
		snapshot.Revision = uint64(f.next)
	}
	return snapshot, true
}

func (f *sequenceSnapshotVoice) snapshotReadCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.next
}

func waitForSequenceSnapshotReads(t *testing.T, voice *sequenceSnapshotVoice, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for voice.snapshotReadCount() < want {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %d participant snapshot reads; got %d", want, voice.snapshotReadCount())
		}
		time.Sleep(time.Millisecond)
	}
}

func (f *blockingSnapshotVoice) SnapshotVoiceParticipants(job discord.VoiceJob) (discord.ParticipantSnapshot, bool) {
	f.mu.Lock()
	f.calls++
	call := f.calls
	snapshot := f.snapshot
	f.mu.Unlock()
	if call == 2 {
		close(f.started)
		<-f.release
	}
	if snapshot.StreamID == "" {
		snapshot.StreamID = job.StreamID
	}
	if snapshot.GuildID == "" {
		snapshot.GuildID = job.GuildID
	}
	if snapshot.VoiceChannelID == "" {
		snapshot.VoiceChannelID = job.VoiceChannelID
	}
	return snapshot, true
}
