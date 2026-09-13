package discord

import (
	"context"
	"github.com/cartridge-gg/discordgo"
	"github.com/example/autostream-discord-bot/internal/audioforward"
	"strings"
	"sync"
	"testing"
	"time"
)

type fakeAudioForwarder struct {
	mu              sync.Mutex
	called          chan struct{}
	encoderAudioURL string
	streamID        string
	source          string
	tokenOverride   string
	packets         []audioforward.OpusPacket
	calls           []fakeAudioForwardCall
	errorsByURL     map[string]error
}

type fakeAudioForwardCall struct {
	url           string
	streamID      string
	source        string
	tokenOverride string
	packets       []audioforward.OpusPacket
}

type blockingCaptionForwarder struct {
	captionURL     string
	captionStarted chan struct{}
	captionRelease chan struct{}
	encoderCalls   chan []audioforward.OpusPacket
	startOnce      sync.Once
}

func (f *blockingCaptionForwarder) ForwardOpus(ctx context.Context, targetURL, _ string, _ string, _ string, packets []audioforward.OpusPacket) error {
	if targetURL == f.captionURL {
		f.startOnce.Do(func() { close(f.captionStarted) })
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-f.captionRelease:
			return nil
		}
	}
	select {
	case f.encoderCalls <- append([]audioforward.OpusPacket(nil), packets...):
	default:
	}
	return nil
}

func newFakeAudioForwarder() *fakeAudioForwarder {
	return &fakeAudioForwarder{called: make(chan struct{}, 8), errorsByURL: map[string]error{}}
}

func (f *fakeAudioForwarder) ForwardOpus(ctx context.Context, encoderAudioURL, streamID, source, tokenOverride string, packets []audioforward.OpusPacket) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f.mu.Lock()
	f.encoderAudioURL = encoderAudioURL
	f.streamID = streamID
	f.source = source
	f.tokenOverride = tokenOverride
	f.packets = append([]audioforward.OpusPacket(nil), packets...)
	f.calls = append(f.calls, fakeAudioForwardCall{
		url:           encoderAudioURL,
		streamID:      streamID,
		source:        source,
		tokenOverride: tokenOverride,
		packets:       append([]audioforward.OpusPacket(nil), packets...),
	})
	err := f.errorsByURL[encoderAudioURL]
	f.mu.Unlock()
	select {
	case f.called <- struct{}{}:
	default:
	}
	return err
}

func (f *fakeAudioForwarder) snapshot() (string, string, string, string, []audioforward.OpusPacket) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.encoderAudioURL, f.streamID, f.source, f.tokenOverride, append([]audioforward.OpusPacket(nil), f.packets...)
}

func (f *fakeAudioForwarder) callsSnapshot() []fakeAudioForwardCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	calls := make([]fakeAudioForwardCall, len(f.calls))
	for i, call := range f.calls {
		calls[i] = call
		calls[i].packets = append([]audioforward.OpusPacket(nil), call.packets...)
	}
	return calls
}

func (f *fakeAudioForwarder) failURL(targetURL string, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.errorsByURL[targetURL] = err
}

func (f *fakeAudioForwarder) failsURL(targetURL string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.errorsByURL[targetURL] != nil
}

func forwardResultsSettled(client *RealClient, forwarder *fakeAudioForwarder, job VoiceJob) bool {
	status := client.Status()
	if strings.TrimSpace(job.EncoderAudioURL) != "" {
		if forwarder.failsURL(job.EncoderAudioURL) {
			if status.AudioForwardErrors < 1 {
				return false
			}
		} else if status.AudioPacketsForwarded < 20 {
			return false
		}
	}
	if strings.TrimSpace(job.CaptionAudioURL) != "" {
		batchSize := job.CaptionAudioMaxBatchPackets
		if batchSize <= 0 {
			batchSize = 5
		}
		captionCalls := int64((20 + batchSize - 1) / batchSize)
		if forwarder.failsURL(job.CaptionAudioURL) {
			if status.CaptionForwardErrors < captionCalls {
				return false
			}
		} else if status.CaptionPacketsForwarded < 20 {
			return false
		}
	}
	return true
}

func activateVoiceJobForTest(client *RealClient, job VoiceJob, voiceGeneration uint64) {
	client.mu.Lock()
	client.job = job
	client.voiceGeneration = voiceGeneration
	client.status.VoiceConnected = true
	if client.ssrcUsers == nil {
		client.ssrcUsers = map[uint32]string{}
	}
	client.mu.Unlock()
}

func runSyntheticForwardBatch(t *testing.T, client *RealClient, forwarder *fakeAudioForwarder, job VoiceJob, expectedCalls int) {
	t.Helper()
	activateVoiceJobForTest(client, job, 1)
	packets := make(chan *discordgo.Packet, 20)
	stop := make(chan struct{})
	done := make(chan struct{})
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
	for i := 0; i < expectedCalls; i++ {
		select {
		case <-forwarder.called:
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for opus forward call %d of %d", i+1, expectedCalls)
		}
	}
	deadline := time.Now().Add(2 * time.Second)
	for !forwardResultsSettled(client, forwarder, job) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !forwardResultsSettled(client, forwarder, job) {
		t.Fatalf("forward result callback did not settle before Stop: %#v", client.Status())
	}
	close(stop)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for forward loop to stop")
	}
}
