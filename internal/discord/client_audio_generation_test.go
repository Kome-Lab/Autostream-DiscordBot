package discord

import (
	"encoding/json"
	"fmt"
	"github.com/cartridge-gg/discordgo"
	"strings"
	"testing"
	"time"
)

func TestForwardErrorIsSanitizedInStatus(t *testing.T) {
	client := &RealClient{}
	input := `Post "` + "https://" + "user:" + "secret" + "@encoder.example.com/streams/stream-01/audio/opus" + `": Authorization Bearer secret-token rejected`
	client.setForwardError(input)

	status := client.Status()
	if status.AudioForwardErrors != 1 {
		t.Fatalf("expected one forward error, got %d", status.AudioForwardErrors)
	}
	if status.LastForwardError != "discord audio forward failed" || status.LastError != "discord audio forward failed" {
		t.Fatalf("unexpected sanitized errors: %#v", status)
	}
}

func TestCaptionForwardErrorIsSanitizedInStatus(t *testing.T) {
	client := &RealClient{}
	client.setCaptionForwardError(`Post "https://caption.example.com/audio": Authorization Bearer caption-secret rejected`)

	status := client.Status()
	if status.CaptionForwardErrors != 1 {
		t.Fatalf("expected one caption forward error, got %d", status.CaptionForwardErrors)
	}
	if status.LastCaptionForwardError != "discord caption audio forward failed" || status.LastError != "discord caption audio forward failed" {
		t.Fatalf("unexpected sanitized caption errors: %#v", status)
	}
}

func TestLastErrorIsSanitizedInStatus(t *testing.T) {
	client := &RealClient{}
	client.setLastError("Discord token rejected by upstream")

	status := client.Status()
	if status.LastError != "discord operation failed" {
		t.Fatalf("unexpected last error: %q", status.LastError)
	}
	if strings.Contains(strings.ToLower(status.LastError), "token") {
		t.Fatalf("last error leaked sensitive word: %q", status.LastError)
	}
}

func TestVoiceSpeakingUpdateReportsActiveSpeaker(t *testing.T) {
	sink := &fakeEventSink{}
	client := &RealClient{
		sink: sink,
	}
	activateVoiceJobForTest(client, VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01"}, 1)

	client.onVoiceSpeakingUpdate(nil, &discordgo.VoiceSpeakingUpdate{UserID: "user-01", SSRC: 42, Speaking: true})

	if sink.activeStreamID != "stream-01" || sink.activeUserID != "user-01" {
		t.Fatalf("active speaker was not reported from speaking update: %#v", sink)
	}
	if got := client.userForSSRC(42, 1); got != "user-01" {
		t.Fatalf("speaking update did not populate SSRC user map, got %q", got)
	}
}

func TestUserForSSRCRejectsStaleVoiceGeneration(t *testing.T) {
	client := &RealClient{
		voiceGeneration: 2,
		ssrcUsers:       map[uint32]string{42: "new-user"},
		status:          Status{VoiceConnected: true},
	}

	if got := client.userForSSRC(42, 1); got != "" {
		t.Fatalf("stale voice generation resolved current SSRC mapping: %q", got)
	}
}

func TestActiveSpeakerCallbackMayReadClientStatusWithoutDeadlock(t *testing.T) {
	job := VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01", JobGeneration: 7}
	client := &RealClient{}
	sink := &reentrantStatusSpeakerSink{client: client, called: make(chan struct{}, 1)}
	client.sink = sink
	activateVoiceJobForTest(client, job, 3)

	done := make(chan struct{})
	go func() {
		client.recordAudioSpeakerActivity(job, 3, "user-01", time.Now().UTC())
		close(done)
	}()
	select {
	case <-sink.called:
	case <-time.After(time.Second):
		t.Fatal("active speaker callback deadlocked while reading Discord status")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("active speaker dispatch did not return")
	}
}

func TestForwardOpusPreservesJobGenerationAndUpdatesStatus(t *testing.T) {
	forwarder := newFakeAudioForwarder()
	client := &RealClient{}
	packets := make(chan *discordgo.Packet, 20)
	stop := make(chan struct{})
	done := make(chan struct{})
	job := VoiceJob{
		StreamID:          "stream-01",
		JobGeneration:     17,
		GuildID:           "guild-01",
		VoiceChannelID:    "voice-01",
		EncoderAudioURL:   "https://encoder.example.com/streams/stream-01/audio/opus",
		StreamIngestToken: "job-ingest-token",
	}
	activateVoiceJobForTest(client, job, 1)
	client.onVoiceSpeakingUpdate(nil, &discordgo.VoiceSpeakingUpdate{UserID: "user-01", SSRC: 42, Speaking: true})
	go func() {
		defer close(done)
		client.forwardOpus(job, 1, packets, stop, forwarder, "discord-bot-01")
	}()
	for i := 0; i < 20; i++ {
		packets <- &discordgo.Packet{
			SSRC:      42,
			Sequence:  uint16(100 + i),
			Timestamp: uint32(960 * i),
			Opus:      []byte{byte(i), 0xaa, 0xbb},
		}
	}
	select {
	case <-forwarder.called:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for opus batch to be forwarded")
	}
	close(stop)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for forward loop to stop")
	}
	encoderAudioURL, streamID, source, tokenOverride, forwarded := forwarder.snapshot()
	if encoderAudioURL != job.EncoderAudioURL || streamID != "stream-01" || source != "discord-bot-01" || tokenOverride != "job-ingest-token" {
		t.Fatalf("unexpected forward context url=%q stream=%q source=%q token=%q", encoderAudioURL, streamID, source, tokenOverride)
	}
	if len(forwarded) != 20 {
		t.Fatalf("expected 20 forwarded packets, got %d", len(forwarded))
	}
	if forwarded[0].UserID != "user-01" || forwarded[0].JobGeneration != 17 || forwarded[0].ConnectionGeneration != 1 || forwarded[0].SSRC != 42 || forwarded[0].Sequence != 100 || string(forwarded[0].Opus) != string([]byte{0, 0xaa, 0xbb}) {
		t.Fatalf("unexpected forwarded packet: %#v", forwarded[0])
	}
	status := client.Status()
	if !status.AudioReceiving || status.AudioPacketsReceived != 20 || status.AudioPacketsForwarded != 20 || status.AudioForwardErrors != 0 {
		t.Fatalf("unexpected audio status after forwarding: %#v", status)
	}
	statusJSON, err := json.Marshal(status)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"job-ingest-token", "encoder.example.com", job.EncoderAudioURL} {
		if strings.Contains(string(statusJSON), secret) {
			t.Fatalf("status leaked forward secret/context %q: %s", secret, string(statusJSON))
		}
	}
}

func TestForwardOpusBatchesFifteenSpeakerLoadWithoutRequestBacklog(t *testing.T) {
	forwarder := newFakeAudioForwarder()
	client := &RealClient{}
	packets := make(chan *discordgo.Packet, 75)
	stop := make(chan struct{})
	done := make(chan struct{})
	job := VoiceJob{
		StreamID:          "stream-15-speakers",
		GuildID:           "guild-01",
		VoiceChannelID:    "voice-01",
		EncoderAudioURL:   "https://encoder.example.com/streams/stream-15-speakers/audio/opus",
		StreamIngestToken: "job-ingest-token",
	}
	activateVoiceJobForTest(client, job, 1)
	for speaker := 0; speaker < 15; speaker++ {
		client.onVoiceSpeakingUpdate(nil, &discordgo.VoiceSpeakingUpdate{
			UserID:   fmt.Sprintf("user-%02d", speaker),
			SSRC:     1000 + speaker,
			Speaking: true,
		})
	}
	go func() {
		defer close(done)
		client.forwardOpus(job, 1, packets, stop, forwarder, "discord-bot-01")
	}()
	for frame := 0; frame < 5; frame++ {
		for speaker := 0; speaker < 15; speaker++ {
			packets <- &discordgo.Packet{
				SSRC:      uint32(1000 + speaker),
				Sequence:  uint16(frame + 1),
				Timestamp: uint32((frame + 1) * 960),
				Opus:      []byte{byte(speaker), byte(frame)},
			}
		}
	}

	deadline := time.Now().Add(400 * time.Millisecond)
	for client.Status().AudioPacketsForwarded < 75 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	forwardedBeforeStop := client.Status().AudioPacketsForwarded
	close(stop)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("15-speaker forward loop did not stop")
	}
	if forwardedBeforeStop != 75 {
		t.Fatalf("forwarded packets before stop = %d, want 75 within 400 ms", forwardedBeforeStop)
	}
	calls := forwarder.callsSnapshot()
	if len(calls) != 2 {
		t.Fatalf("15-speaker load used %d encoder requests, want 2 bounded batches", len(calls))
	}
	if len(calls[0].packets) != 50 || len(calls[1].packets) != 25 {
		t.Fatalf("unexpected 15-speaker batch sizes: %d, %d", len(calls[0].packets), len(calls[1].packets))
	}
}

func TestForwardOpusDoesNotDelayEncoderWhileSSRCIdentityIsUnresolved(t *testing.T) {
	forwarder := newFakeAudioForwarder()
	client := &RealClient{}
	packets := make(chan *discordgo.Packet, 5)
	stop := make(chan struct{})
	done := make(chan struct{})
	job := VoiceJob{
		StreamID:                    "stream-unresolved-ssrc",
		GuildID:                     "guild-01",
		VoiceChannelID:              "voice-01",
		EncoderAudioURL:             "https://encoder.example.com/audio",
		CaptionAudioURL:             "https://worker.example.com/captions",
		StreamIngestToken:           "encoder-token",
		CaptionAudioToken:           "caption-token",
		CaptionAudioFlushMS:         100,
		CaptionAudioMaxBatchPackets: 5,
		UnresolvedSSRCBufferMS:      300,
	}
	activateVoiceJobForTest(client, job, 1)
	go func() {
		defer close(done)
		client.forwardOpus(job, 1, packets, stop, forwarder, "discord-bot-01")
	}()
	for sequence := 1; sequence <= 5; sequence++ {
		packets <- &discordgo.Packet{
			SSRC:      42,
			Sequence:  uint16(sequence),
			Timestamp: uint32(sequence * 960),
			Opus:      []byte{byte(sequence)},
		}
	}

	encoderDeadline := time.Now().Add(250 * time.Millisecond)
	for client.Status().AudioPacketsForwarded < 5 && time.Now().Before(encoderDeadline) {
		time.Sleep(time.Millisecond)
	}
	if got := client.Status().AudioPacketsForwarded; got != 5 {
		close(stop)
		<-done
		t.Fatalf("encoder packets forwarded before identity timeout = %d, want 5", got)
	}
	for _, call := range forwarder.callsSnapshot() {
		if call.url == job.CaptionAudioURL {
			close(stop)
			<-done
			t.Fatal("caption packets were forwarded before the SSRC identity timeout")
		}
	}

	captionDeadline := time.Now().Add(time.Second)
	for client.Status().CaptionPacketsForwarded < 5 && time.Now().Before(captionDeadline) {
		time.Sleep(time.Millisecond)
	}
	close(stop)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("unresolved SSRC forward loop did not stop")
	}
	status := client.Status()
	if status.AudioPacketsForwarded != 5 || status.CaptionPacketsForwarded != 5 {
		t.Fatalf("unexpected unresolved SSRC forwarding status: %#v", status)
	}
}
