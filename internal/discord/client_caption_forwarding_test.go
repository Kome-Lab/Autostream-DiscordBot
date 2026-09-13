package discord

import (
	"encoding/json"
	"errors"
	"github.com/cartridge-gg/discordgo"
	"github.com/example/autostream-discord-bot/internal/audioforward"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestForwardOpusForwardsCaptionOnly(t *testing.T) {
	forwarder := newFakeAudioForwarder()
	client := &RealClient{}
	job := VoiceJob{
		StreamID:          "stream-01",
		GuildID:           "guild-01",
		VoiceChannelID:    "voice-01",
		CaptionAudioURL:   "https://worker.example.com/captions",
		CaptionAudioToken: "caption-token",
	}
	activateVoiceJobForTest(client, job, 1)
	client.onVoiceSpeakingUpdate(nil, &discordgo.VoiceSpeakingUpdate{UserID: "user-01", SSRC: 42, Speaking: true})

	runSyntheticForwardBatch(t, client, forwarder, job, 4)

	calls := forwarder.callsSnapshot()
	if len(calls) != 4 {
		t.Fatalf("expected four caption forward calls, got %d", len(calls))
	}
	totalPackets := 0
	for i, call := range calls {
		if call.url != job.CaptionAudioURL || call.streamID != job.StreamID || call.source != "discord-bot-01" || call.tokenOverride != job.CaptionAudioToken {
			t.Fatalf("unexpected caption forward context at call %d: %#v", i, call)
		}
		if len(call.packets) == 0 || len(call.packets) > 5 || call.packets[0].UserID != "user-01" {
			t.Fatalf("unexpected caption packet batch at call %d: %#v", i, call.packets)
		}
		totalPackets += len(call.packets)
	}
	if totalPackets != 20 {
		t.Fatalf("expected 20 caption packets across batches, got %d", totalPackets)
	}
	status := client.Status()
	if status.AudioPacketsForwarded != 0 || status.AudioForwardErrors != 0 || status.CaptionPacketsForwarded != 20 || status.CaptionForwardErrors != 0 {
		t.Fatalf("unexpected caption-only status: %#v", status)
	}
	statusJSON, err := json.Marshal(status)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{job.CaptionAudioToken, "worker.example.com", job.CaptionAudioURL} {
		if strings.Contains(string(statusJSON), secret) {
			t.Fatalf("status leaked caption secret/context %q: %s", secret, string(statusJSON))
		}
	}
}

func TestForwardOpusForwardsSameBatchToBothTargetsWithSeparateTokens(t *testing.T) {
	forwarder := newFakeAudioForwarder()
	client := &RealClient{}
	job := VoiceJob{
		StreamID:            "stream-01",
		GuildID:             "guild-01",
		VoiceChannelID:      "voice-01",
		EncoderAudioURL:     "https://encoder.example.com/audio",
		CaptionAudioURL:     "https://worker.example.com/captions",
		StreamIngestToken:   "encoder-token",
		CaptionAudioToken:   "caption-token",
		CaptionAudioFlushMS: 500, CaptionAudioMaxBatchPackets: 20,
	}

	runSyntheticForwardBatch(t, client, forwarder, job, 2)

	calls := forwarder.callsSnapshot()
	if len(calls) != 2 {
		t.Fatalf("expected two target calls, got %d", len(calls))
	}
	byURL := make(map[string]fakeAudioForwardCall, len(calls))
	for _, call := range calls {
		byURL[call.url] = call
	}
	encoderCall := byURL[job.EncoderAudioURL]
	captionCall := byURL[job.CaptionAudioURL]
	if encoderCall.tokenOverride != job.StreamIngestToken {
		t.Fatalf("encoder target received wrong token: %#v", encoderCall)
	}
	if captionCall.tokenOverride != job.CaptionAudioToken {
		t.Fatalf("caption target received wrong token: %#v", captionCall)
	}
	if !reflect.DeepEqual(encoderCall.packets, captionCall.packets) || len(encoderCall.packets) != 20 {
		t.Fatalf("targets did not receive the same packet batch: encoder=%#v caption=%#v", encoderCall.packets, captionCall.packets)
	}
	status := client.Status()
	if status.AudioPacketsReceived != 20 || status.AudioPacketsForwarded != 20 || status.CaptionPacketsForwarded != 20 || status.AudioForwardErrors != 0 || status.CaptionForwardErrors != 0 {
		t.Fatalf("unexpected dual-target status: %#v", status)
	}
}

func TestForwardOpusTargetFailuresAreIndependent(t *testing.T) {
	encoderURL := "https://encoder.example.com/audio"
	captionURL := "https://worker.example.com/captions"
	tests := []struct {
		name                 string
		failedURL            string
		wantEncoderForwarded int64
		wantEncoderErrors    int64
		wantCaptionForwarded int64
		wantCaptionErrors    int64
	}{
		{name: "encoder failure does not stop captions", failedURL: encoderURL, wantEncoderErrors: 1, wantCaptionForwarded: 20},
		{name: "caption failure does not stop encoder", failedURL: captionURL, wantEncoderForwarded: 20, wantCaptionErrors: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			forwarder := newFakeAudioForwarder()
			forwarder.failURL(tt.failedURL, errors.New("target unavailable"))
			client := &RealClient{}
			job := VoiceJob{
				StreamID:            "stream-01",
				GuildID:             "guild-01",
				VoiceChannelID:      "voice-01",
				EncoderAudioURL:     encoderURL,
				CaptionAudioURL:     captionURL,
				StreamIngestToken:   "encoder-token",
				CaptionAudioToken:   "caption-token",
				CaptionAudioFlushMS: 500, CaptionAudioMaxBatchPackets: 20,
			}

			runSyntheticForwardBatch(t, client, forwarder, job, 2)

			if calls := forwarder.callsSnapshot(); len(calls) != 2 {
				t.Fatalf("both targets must be attempted, got %d calls", len(calls))
			}
			status := client.Status()
			if status.AudioPacketsForwarded != tt.wantEncoderForwarded || status.AudioForwardErrors != tt.wantEncoderErrors || status.CaptionPacketsForwarded != tt.wantCaptionForwarded || status.CaptionForwardErrors != tt.wantCaptionErrors {
				t.Fatalf("unexpected status after one target failed: %#v", status)
			}
		})
	}
}

func TestForwardOpusCaptionBackpressureDoesNotBlockEncoder(t *testing.T) {
	captionURL := "https://worker.example.com/captions"
	forwarder := &blockingCaptionForwarder{
		captionURL:     captionURL,
		captionStarted: make(chan struct{}),
		captionRelease: make(chan struct{}),
		encoderCalls:   make(chan []audioforward.OpusPacket, 1),
	}
	client := &RealClient{}
	job := VoiceJob{
		StreamID:                    "stream-01",
		GuildID:                     "guild-01",
		VoiceChannelID:              "voice-01",
		EncoderAudioURL:             "https://encoder.example.com/audio",
		CaptionAudioURL:             captionURL,
		StreamIngestToken:           "encoder-token",
		CaptionAudioToken:           "caption-token",
		CaptionAudioFlushMS:         100,
		CaptionAudioMaxBatchPackets: 5,
	}
	activateVoiceJobForTest(client, job, 1)
	client.onVoiceSpeakingUpdate(nil, &discordgo.VoiceSpeakingUpdate{UserID: "user-01", SSRC: 42, Speaking: true})
	packets := make(chan *discordgo.Packet, 64)
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		client.forwardOpus(job, 1, packets, stop, forwarder, "discord-bot-01")
	}()
	t.Cleanup(func() {
		select {
		case <-stop:
		default:
			close(stop)
		}
		select {
		case <-forwarder.captionRelease:
		default:
			close(forwarder.captionRelease)
		}
		select {
		case <-done:
		case <-time.After(time.Second):
		}
	})

	for i := 0; i < 5; i++ {
		packets <- &discordgo.Packet{SSRC: 42, Sequence: uint16(i), Timestamp: uint32(960 * i), Opus: []byte{0x01}}
	}
	select {
	case <-forwarder.captionStarted:
	case <-time.After(time.Second):
		t.Fatal("caption forward did not enter the blocking request")
	}
	for i := 5; i < 25; i++ {
		packets <- &discordgo.Packet{SSRC: 42, Sequence: uint16(i), Timestamp: uint32(960 * i), Opus: []byte{0x01}}
	}

	select {
	case batch := <-forwarder.encoderCalls:
		if len(batch) == 0 {
			t.Fatal("encoder received an empty packet batch")
		}
	case <-time.After(time.Second):
		t.Fatal("caption backpressure blocked encoder audio forwarding")
	}
	close(stop)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("stopping the stream did not cancel the blocked caption request")
	}
}

func TestForwardOpusCaptionQueueIsBoundedAndCanceledOnStop(t *testing.T) {
	captionURL := "https://worker.example.com/captions"
	forwarder := &blockingCaptionForwarder{
		captionURL:     captionURL,
		captionStarted: make(chan struct{}),
		captionRelease: make(chan struct{}),
		encoderCalls:   make(chan []audioforward.OpusPacket, 1),
	}
	client := &RealClient{}
	job := VoiceJob{
		StreamID:                    "stream-01",
		GuildID:                     "guild-01",
		VoiceChannelID:              "voice-01",
		CaptionAudioURL:             captionURL,
		CaptionAudioToken:           "caption-token",
		CaptionAudioFlushMS:         1000,
		CaptionAudioMaxBatchPackets: 1,
	}
	activateVoiceJobForTest(client, job, 1)
	client.onVoiceSpeakingUpdate(nil, &discordgo.VoiceSpeakingUpdate{UserID: "user-01", SSRC: 42, Speaking: true})
	packets := make(chan *discordgo.Packet, opusForwardQueueBatches+2)
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		client.forwardOpus(job, 1, packets, stop, forwarder, "discord-bot-01")
	}()
	t.Cleanup(func() {
		select {
		case <-stop:
		default:
			close(stop)
		}
		select {
		case <-forwarder.captionRelease:
		default:
			close(forwarder.captionRelease)
		}
	})

	packets <- &discordgo.Packet{SSRC: 42, Sequence: 1, Opus: []byte{0x01}}
	select {
	case <-forwarder.captionStarted:
	case <-time.After(time.Second):
		t.Fatal("caption forward did not enter the blocking request")
	}
	for i := 0; i < opusForwardQueueBatches+1; i++ {
		packets <- &discordgo.Packet{SSRC: 42, Sequence: uint16(i + 2), Opus: []byte{0x01}}
	}
	deadline := time.Now().Add(time.Second)
	for client.Status().CaptionForwardErrors == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if status := client.Status(); status.CaptionForwardErrors == 0 || status.CaptionForwardQueueDrops == 0 {
		t.Fatalf("caption queue overflow was not bounded: %#v", status)
	}
	close(stop)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("stop did not cancel the blocked bounded caption queue")
	}
}
