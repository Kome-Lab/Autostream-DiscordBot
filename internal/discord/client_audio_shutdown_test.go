package discord

import (
	"github.com/cartridge-gg/discordgo"
	"reflect"
	"testing"
	"time"
)

func TestForwardOpusOldReceiveClosureDoesNotDisconnectRearmedJob(t *testing.T) {
	oldJob := VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01", JobGeneration: 7}
	newJob := oldJob
	newJob.JobGeneration = 8
	client := &RealClient{
		job:             newJob,
		voiceGeneration: 4,
		status: Status{
			VoiceConnected: true,
			CurrentGuildID: newJob.GuildID,
			CurrentVoiceID: newJob.VoiceChannelID,
		},
	}
	packets := make(chan *discordgo.Packet)
	close(packets)

	client.forwardOpus(oldJob, 3, packets, make(chan struct{}), nil, "discord-bot-01")

	status := client.Status()
	if !status.VoiceConnected || status.CurrentGuildID != newJob.GuildID || status.CurrentVoiceID != newJob.VoiceChannelID || status.VoiceDisconnectCount != 0 {
		t.Fatalf("old receive loop changed the rearmed voice state: %#v", status)
	}
	if got := client.connectionGenerationForJob(oldJob, 3); got != 0 {
		t.Fatalf("old job observed the new connection generation: %d", got)
	}
	if got := client.connectionGenerationForJob(newJob, 4); got != 4 {
		t.Fatalf("current job did not observe its connection generation: %d", got)
	}
}

func TestForwardOpusStaleReconnectPacketCannotMutateCurrentState(t *testing.T) {
	job := VoiceJob{
		StreamID:       "stream-01",
		GuildID:        "guild-01",
		VoiceChannelID: "voice-01",
		JobGeneration:  7,
	}
	sink := &activeSpeakerStateSink{}
	client := &RealClient{
		sink:            sink,
		job:             job,
		voiceGeneration: 4,
		ssrcUsers:       map[uint32]string{42: "user-01"},
		audioSpeakers:   map[string]time.Time{},
		status:          Status{VoiceConnected: true},
	}
	packets := make(chan *discordgo.Packet, 1)
	packets <- &discordgo.Packet{SSRC: 42, Sequence: 1, Opus: []byte{0x01}}
	close(packets)

	client.forwardOpus(job, 3, packets, make(chan struct{}), nil, "discord-bot-01")

	status := client.Status()
	if status.AudioReceiving || status.AudioPacketsReceived != 0 || status.VoiceDisconnectCount != 0 {
		t.Fatalf("stale reconnect loop mutated current status: %#v", status)
	}
	if len(sink.speaking) != 0 || sink.activeUserID != "" {
		t.Fatalf("stale reconnect loop mutated current speaker state: %#v", sink)
	}
}

func TestLeaveVoiceWaitsForAudioForwardLoopCancellation(t *testing.T) {
	stop := make(chan struct{})
	done := make(chan struct{})
	stopObserved := make(chan struct{})
	go func() {
		<-stop
		close(stopObserved)
	}()
	client := &RealClient{
		job:       VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01", JobGeneration: 3},
		audioStop: stop,
		audioDone: done,
		status:    Status{VoiceConnected: true},
	}
	result := make(chan error, 1)
	go func() {
		result <- client.LeaveVoice("stream-01")
	}()

	select {
	case <-stopObserved:
	case <-time.After(time.Second):
		t.Fatal("LeaveVoice did not cancel the audio loop")
	}
	select {
	case err := <-result:
		t.Fatalf("LeaveVoice returned before the audio loop stopped: %v", err)
	default:
	}
	close(done)
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("LeaveVoice did not return after the audio loop stopped")
	}
}

func TestForwardOpusCaptionDoesNotUseEncoderTokenFallback(t *testing.T) {
	forwarder := newFakeAudioForwarder()
	client := &RealClient{}
	job := VoiceJob{
		StreamID:        "stream-01",
		GuildID:         "guild-01",
		VoiceChannelID:  "voice-01",
		CaptionAudioURL: "https://worker.example.com/captions",
	}
	activateVoiceJobForTest(client, job, 1)
	packets := make(chan *discordgo.Packet, 20)
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		client.forwardOpus(job, 1, packets, stop, forwarder, "discord-bot-01")
	}()
	for i := 0; i < 20; i++ {
		packets <- &discordgo.Packet{SSRC: 42, Sequence: uint16(i), Opus: []byte{0x01}}
	}
	deadline := time.Now().Add(2 * time.Second)
	for client.Status().CaptionForwardErrors == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	close(stop)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for caption forward loop to stop")
	}
	if calls := forwarder.callsSnapshot(); len(calls) != 0 {
		t.Fatalf("caption without a job token must not call the shared client with its encoder fallback: %#v", calls)
	}
	status := client.Status()
	if status.CaptionForwardErrors != 1 || status.CaptionPacketsForwarded != 0 || status.LastCaptionForwardError != "discord caption audio forward failed" {
		t.Fatalf("unexpected missing caption token status: %#v", status)
	}
}

func TestVoiceSpeakingUpdateIgnoresStopSpeaking(t *testing.T) {
	sink := &fakeEventSink{}
	client := &RealClient{sink: sink}
	activateVoiceJobForTest(client, VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01"}, 1)

	client.onVoiceSpeakingUpdate(nil, &discordgo.VoiceSpeakingUpdate{UserID: "user-01", SSRC: 42, Speaking: false})

	if sink.activeStreamID != "" || sink.activeUserID != "" {
		t.Fatalf("stop-speaking update should not publish active speaker: %#v", sink)
	}
}

func TestVoiceSpeakingUpdateReportsStopToStateSink(t *testing.T) {
	sink := &activeSpeakerStateSink{}
	client := &RealClient{sink: sink}
	activateVoiceJobForTest(client, VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01"}, 1)

	client.onVoiceSpeakingUpdate(nil, &discordgo.VoiceSpeakingUpdate{UserID: "user-01", SSRC: 42, Speaking: true})
	client.onVoiceSpeakingUpdate(nil, &discordgo.VoiceSpeakingUpdate{UserID: "user-01", SSRC: 42, Speaking: false})

	if !reflect.DeepEqual(sink.speaking, []bool{true, false}) || sink.activeUserID != "user-01" {
		t.Fatalf("speaking state edges were not reported: %#v", sink)
	}
}

func TestAudioActivityExpiresMissingSpeakerStop(t *testing.T) {
	sink := &activeSpeakerStateSink{}
	job := VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01"}
	client := &RealClient{sink: sink}
	activateVoiceJobForTest(client, job, 0)
	now := time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)

	client.recordAudioSpeakerActivity(job, 0, "user-01", now)
	client.expireIdleAudioSpeakers(job, 0, now.Add(audioSpeakerIdleTimeout))

	if !reflect.DeepEqual(sink.speaking, []bool{true, false}) || sink.activeUserID != "user-01" {
		t.Fatalf("audio activity did not converge a missing Discord speaking stop: %#v", sink)
	}
}

func TestAudioActivityRefreshKeepsCurrentSpeakerHighlighted(t *testing.T) {
	sink := &activeSpeakerStateSink{}
	job := VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01"}
	client := &RealClient{sink: sink}
	activateVoiceJobForTest(client, job, 0)
	now := time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)

	client.recordAudioSpeakerActivity(job, 0, "user-01", now)
	client.recordAudioSpeakerActivity(job, 0, "user-01", now.Add(audioSpeakerIdleTimeout/2))
	client.expireIdleAudioSpeakers(job, 0, now.Add(audioSpeakerIdleTimeout))

	if !reflect.DeepEqual(sink.speaking, []bool{true}) {
		t.Fatalf("active audio was cleared before the idle timeout: %#v", sink.speaking)
	}
}
