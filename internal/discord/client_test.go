package discord

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cartridge-gg/discordgo"
	"github.com/example/autostream-discord-bot/internal/audioforward"
)

type fakeEventSink struct {
	activeStreamID string
	activeUserID   string
	voiceJoin      VoiceJoinEvent
	voiceJoins     []VoiceJoinEvent
	participants   []ParticipantEvent
	chatMessage    ChatMessageEvent
	connectedCount int
}

func (f *fakeEventSink) VoiceUserJoined(event VoiceJoinEvent) {
	f.voiceJoin = event
	f.voiceJoins = append(f.voiceJoins, event)
}
func (f *fakeEventSink) ParticipantChanged(event ParticipantEvent) {
	f.participants = append(f.participants, event)
}
func (f *fakeEventSink) ChatMessageReceived(event ChatMessageEvent) {
	f.chatMessage = event
}
func (f *fakeEventSink) ActiveSpeakerDetected(streamID, userID string) {
	f.activeStreamID = streamID
	f.activeUserID = userID
}
func (f *fakeEventSink) DiscordConnected()          { f.connectedCount++ }
func (f *fakeEventSink) DiscordDisconnected(string) {}

type snapshotEventSink struct {
	fakeEventSink
	snapshots []ParticipantSnapshot
}

type participantStateSink struct {
	fakeEventSink
	snapshots        []ParticipantSnapshot
	participantsByID map[string]bool
}

type activeSpeakerStateSink struct {
	fakeEventSink
	speaking []bool
}

type reentrantStatusSpeakerSink struct {
	fakeEventSink
	client *RealClient
	called chan struct{}
}

type fakeVoiceSSRCResolver struct {
	users map[uint32]string
}

func (f *fakeVoiceSSRCResolver) SSRCUserID(ssrc uint32) string {
	return f.users[ssrc]
}

func (f *activeSpeakerStateSink) ActiveSpeakerStateChanged(streamID, userID string, speaking bool) {
	f.activeStreamID = streamID
	f.activeUserID = userID
	f.speaking = append(f.speaking, speaking)
}

func (f *reentrantStatusSpeakerSink) ActiveSpeakerStateChanged(streamID, userID string, speaking bool) {
	_ = f.client.Status()
	f.activeStreamID = streamID
	f.activeUserID = userID
	select {
	case f.called <- struct{}{}:
	default:
	}
}

func TestResolveSSRCUserIDFallsBackToVoiceConnectionCache(t *testing.T) {
	resolver := &fakeVoiceSSRCResolver{users: map[uint32]string{
		42: "user-01",
		84: "user-02",
	}}

	for ssrc, want := range map[uint32]string{42: "user-01", 84: "user-02"} {
		if got := resolveSSRCUserID(resolver, ssrc); got != want {
			t.Fatalf("durable voice SSRC mapping was ignored for SSRC %d: got %q, want %q", ssrc, got, want)
		}
	}
}

func (f *snapshotEventSink) ParticipantsSynced(snapshot ParticipantSnapshot) {
	f.snapshots = append(f.snapshots, snapshot)
}

func (f *participantStateSink) ParticipantChanged(event ParticipantEvent) {
	f.fakeEventSink.ParticipantChanged(event)
	if event.Present {
		f.participantsByID[event.UserID] = true
		return
	}
	delete(f.participantsByID, event.UserID)
}

func (f *participantStateSink) ParticipantsSynced(snapshot ParticipantSnapshot) {
	f.snapshots = append(f.snapshots, snapshot)
	f.participantsByID = make(map[string]bool, len(snapshot.Participants))
	for _, participant := range snapshot.Participants {
		f.participantsByID[participant.UserID] = true
	}
}

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

func TestForwardOpusForwardsSyntheticPacketsAndUpdatesStatus(t *testing.T) {
	forwarder := newFakeAudioForwarder()
	client := &RealClient{}
	packets := make(chan *discordgo.Packet, 20)
	stop := make(chan struct{})
	done := make(chan struct{})
	job := VoiceJob{
		StreamID:          "stream-01",
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
	if forwarded[0].UserID != "user-01" || forwarded[0].SSRC != 42 || forwarded[0].Sequence != 100 || string(forwarded[0].Opus) != string([]byte{0, 0xaa, 0xbb}) {
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

func TestGatewayReconnectStatus(t *testing.T) {
	client := &RealClient{status: Status{Connected: true}}
	client.onGatewayDisconnect(nil, &discordgo.Disconnect{})
	status := client.Status()
	if status.Connected {
		t.Fatalf("expected disconnected status after gateway disconnect: %#v", status)
	}

	client.onGatewayResumed(nil, &discordgo.Resumed{})
	status = client.Status()
	if !status.Connected || status.GatewayReconnectCount != 1 {
		t.Fatalf("expected resumed status and reconnect count: %#v", status)
	}
}

func TestLogVoiceStateDiagnosticRecordsSafeTransitionFields(t *testing.T) {
	var output bytes.Buffer
	previousWriter := log.Writer()
	previousFlags := log.Flags()
	log.SetOutput(&output)
	log.SetFlags(0)
	defer func() {
		log.SetOutput(previousWriter)
		log.SetFlags(previousFlags)
	}()

	logVoiceStateDiagnostic(
		VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01", JobGeneration: 7},
		&discordgo.VoiceStateUpdate{
			VoiceState:   &discordgo.VoiceState{GuildID: "guild-01", UserID: "user-01", ChannelID: ""},
			BeforeUpdate: &discordgo.VoiceState{GuildID: "guild-01", UserID: "user-01", ChannelID: "voice-01"},
		},
		"",
		true,
		3,
		"participant_leave_published",
	)

	got := output.String()
	for _, marker := range []string{
		"event=voice_state_update",
		"stream_id=stream-01",
		"job_generation=7",
		"voice_generation=3",
		"before_channel_id=voice-01",
		"event_channel_id=",
		"current_state_known=true",
		"decision=participant_leave_published",
	} {
		if !strings.Contains(got, marker) {
			t.Fatalf("diagnostic log missing %q: %q", marker, got)
		}
	}
	for _, forbidden := range []string{"authorization", "bearer", "token", "avatar_url", "https://"} {
		if strings.Contains(strings.ToLower(got), forbidden) {
			t.Fatalf("diagnostic log contains forbidden sensitive marker %q: %q", forbidden, got)
		}
	}
}

func TestOwnVoiceStateDisconnectUpdatesStatus(t *testing.T) {
	stop := make(chan struct{})
	client := &RealClient{
		audioStop: stop,
		status: Status{
			VoiceConnected:            true,
			AudioForwardActive:        true,
			CaptionAudioForwardActive: true,
			AudioReceiving:            true,
			CurrentGuildID:            "guild-01",
			CurrentVoiceID:            "voice-01",
		},
		job: VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01"},
	}
	session := &discordgo.Session{State: discordgo.NewState()}
	session.State.User = &discordgo.User{ID: "bot-01"}

	client.onVoiceStateUpdate(session, &discordgo.VoiceStateUpdate{
		VoiceState:   &discordgo.VoiceState{UserID: "bot-01", GuildID: "guild-01", ChannelID: ""},
		BeforeUpdate: &discordgo.VoiceState{UserID: "bot-01", GuildID: "guild-01", ChannelID: "voice-01"},
	})

	select {
	case <-stop:
	default:
		t.Fatal("expected audio forward stop channel to be closed")
	}
	status := client.Status()
	if status.VoiceConnected || status.AudioForwardActive || status.CaptionAudioForwardActive || status.AudioReceiving {
		t.Fatalf("expected voice/audio status to be inactive after own voice disconnect: %#v", status)
	}
	if status.CurrentGuildID != "" || status.CurrentVoiceID != "" {
		t.Fatalf("expected current voice IDs to be cleared: %#v", status)
	}
	if status.VoiceDisconnectCount != 1 {
		t.Fatalf("expected one voice disconnect, got %#v", status)
	}
	if client.job.StreamID != "stream-01" {
		t.Fatalf("voice disconnect should preserve current job for control-plane stop handling: %#v", client.job)
	}
}

func TestVoiceStateJoinTriggersAutoStartEventWithoutActiveJob(t *testing.T) {
	sink := &fakeEventSink{}
	client := &RealClient{sink: sink}
	session := &discordgo.Session{State: discordgo.NewState()}
	session.State.User = &discordgo.User{ID: "bot-01"}

	client.onVoiceStateUpdate(session, &discordgo.VoiceStateUpdate{
		VoiceState: &discordgo.VoiceState{UserID: "user-01", GuildID: "guild-01", ChannelID: "voice-01"},
	})

	if sink.voiceJoin.GuildID != "guild-01" || sink.voiceJoin.VoiceChannelID != "voice-01" || sink.voiceJoin.UserID != "user-01" {
		t.Fatalf("voice join did not trigger auto-start event: %#v", sink.voiceJoin)
	}

	client.onVoiceStateUpdate(session, &discordgo.VoiceStateUpdate{
		VoiceState: &discordgo.VoiceState{UserID: "bot-01", GuildID: "guild-01", ChannelID: "voice-02"},
	})
	if sink.voiceJoin.VoiceChannelID != "voice-01" {
		t.Fatalf("bot's own voice join should not trigger auto-start event: %#v", sink.voiceJoin)
	}

	// A real gateway session already knows the guild even when the joining user
	// is not in its cached VoiceStates yet (and this can also happen briefly
	// during a guild rebuild). Waiting for the cache to agree would drop the
	// only event that can trigger the waiting stream.
	knownGuildSink := &fakeEventSink{}
	knownGuildClient := &RealClient{sink: knownGuildSink}
	knownGuildSession := &discordgo.Session{State: discordgo.NewState()}
	knownGuildSession.State.User = &discordgo.User{ID: "bot-01"}
	if err := knownGuildSession.State.GuildAdd(&discordgo.Guild{ID: "guild-01"}); err != nil {
		t.Fatal(err)
	}
	knownGuildClient.onVoiceStateUpdate(knownGuildSession, &discordgo.VoiceStateUpdate{
		VoiceState: &discordgo.VoiceState{UserID: "user-01", GuildID: "guild-01", ChannelID: "voice-01"},
	})
	if knownGuildSink.voiceJoin.UserID != "user-01" {
		t.Fatalf("join in a known guild with a cold VoiceStates cache must trigger auto-start: %#v", knownGuildSink.voiceJoin)
	}
}

func TestVoiceStateUpdateIgnoresHandlersThatLostGatewayOrder(t *testing.T) {
	newSession := func(t *testing.T, initialVoiceStates []*discordgo.VoiceState) *discordgo.Session {
		t.Helper()
		session := &discordgo.Session{State: discordgo.NewState(), StateEnabled: true}
		session.State.User = &discordgo.User{ID: "bot-01"}
		if err := session.State.GuildAdd(&discordgo.Guild{ID: "guild-01", VoiceStates: initialVoiceStates}); err != nil {
			t.Fatalf("add guild state: %v", err)
		}
		return session
	}
	apply := func(t *testing.T, session *discordgo.Session, event *discordgo.VoiceStateUpdate) {
		t.Helper()
		if err := session.State.OnInterface(session, event); err != nil {
			t.Fatalf("apply gateway voice state: %v", err)
		}
	}

	t.Run("ultimately empty ignores delayed join", func(t *testing.T) {
		sink := &fakeEventSink{}
		client := &RealClient{
			sink: sink,
			job:  VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01"},
		}
		session := newSession(t, nil)
		joined := &discordgo.VoiceStateUpdate{VoiceState: &discordgo.VoiceState{UserID: "user-01", GuildID: "guild-01", ChannelID: "voice-01"}}
		left := &discordgo.VoiceStateUpdate{VoiceState: &discordgo.VoiceState{UserID: "user-01", GuildID: "guild-01", ChannelID: ""}}
		apply(t, session, joined)
		apply(t, session, left)

		// discordgo updates State in gateway order before it runs handlers, but
		// handlers are asynchronous by default. Model the leave handler running
		// first and the stale join handler running later.
		client.onVoiceStateUpdate(session, left)
		client.onVoiceStateUpdate(session, joined)

		if len(sink.participants) != 1 || sink.participants[0].Present || sink.participants[0].UserID != "user-01" {
			t.Fatalf("empty final state must publish only the leave, got %#v", sink.participants)
		}
		if sink.voiceJoin.UserID != "" {
			t.Fatalf("delayed join must not cancel empty-channel auto-stop: %#v", sink.voiceJoin)
		}
	})

	t.Run("ultimately occupied ignores delayed leave", func(t *testing.T) {
		sink := &fakeEventSink{}
		client := &RealClient{
			sink: sink,
			job:  VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01"},
		}
		session := newSession(t, []*discordgo.VoiceState{{UserID: "user-01", GuildID: "guild-01", ChannelID: "voice-01"}})
		left := &discordgo.VoiceStateUpdate{VoiceState: &discordgo.VoiceState{UserID: "user-01", GuildID: "guild-01", ChannelID: ""}}
		joined := &discordgo.VoiceStateUpdate{VoiceState: &discordgo.VoiceState{UserID: "user-01", GuildID: "guild-01", ChannelID: "voice-01"}}
		apply(t, session, left)
		apply(t, session, joined)

		client.onVoiceStateUpdate(session, joined)
		client.onVoiceStateUpdate(session, left)

		if len(sink.participants) != 1 || !sink.participants[0].Present || sink.participants[0].UserID != "user-01" {
			t.Fatalf("occupied final state must publish only the join, got %#v", sink.participants)
		}
		if sink.voiceJoin.UserID != "user-01" {
			t.Fatalf("current join must still cancel empty-channel auto-stop: %#v", sink.voiceJoin)
		}
	})

	t.Run("move away then leave elsewhere still removes target participant", func(t *testing.T) {
		sink := &fakeEventSink{}
		client := &RealClient{
			sink: sink,
			job:  VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01"},
		}
		session := newSession(t, []*discordgo.VoiceState{{UserID: "user-01", GuildID: "guild-01", ChannelID: "voice-01"}})
		movedAway := &discordgo.VoiceStateUpdate{VoiceState: &discordgo.VoiceState{UserID: "user-01", GuildID: "guild-01", ChannelID: "voice-02"}}
		leftElsewhere := &discordgo.VoiceStateUpdate{VoiceState: &discordgo.VoiceState{UserID: "user-01", GuildID: "guild-01", ChannelID: ""}}
		apply(t, session, movedAway)
		apply(t, session, leftElsewhere)

		// The later leave from voice-02 is not itself a target-VC leave. The
		// delayed move is the only event that can remove this user from voice-01.
		client.onVoiceStateUpdate(session, leftElsewhere)
		client.onVoiceStateUpdate(session, movedAway)

		if len(sink.participants) != 1 || sink.participants[0].Present || sink.participants[0].UserID != "user-01" {
			t.Fatalf("move away must still remove the target participant, got %#v", sink.participants)
		}
		if sink.voiceJoin.UserID != "" {
			t.Fatalf("stale move must not create a new join signal: %#v", sink.voiceJoin)
		}
	})
}

func TestVoiceParticipantSnapshotsHydrateAndRecoverAuthoritatively(t *testing.T) {
	session := &discordgo.Session{State: discordgo.NewState(), StateEnabled: true}
	session.State.User = &discordgo.User{ID: "bot-01"}
	if err := session.State.GuildAdd(&discordgo.Guild{
		ID: "guild-01",
		VoiceStates: []*discordgo.VoiceState{
			{UserID: "bot-01", GuildID: "guild-01", ChannelID: "voice-01"},
			{UserID: "user-a", GuildID: "guild-01", ChannelID: "voice-01", Member: &discordgo.Member{User: &discordgo.User{ID: "user-a", Username: "Alice"}}},
			{UserID: "user-b", GuildID: "guild-01", ChannelID: "voice-01", Member: &discordgo.Member{User: &discordgo.User{ID: "user-b", Username: "Bob"}}},
		},
	}); err != nil {
		t.Fatal(err)
	}
	job := VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01", JobGeneration: 7}
	sink := &snapshotEventSink{}
	client := &RealClient{session: session, sink: sink, job: job}

	staleJob := job
	staleJob.JobGeneration = 6
	if snapshot, known := client.SnapshotVoiceParticipants(staleJob); known {
		t.Fatalf("old job generation received current participant snapshot: %#v", snapshot)
	}

	initial, known := client.SnapshotVoiceParticipants(job)
	if !known || initial.Revision != 1 || len(initial.Participants) != 2 || initial.Participants[0].UserID != "user-a" || initial.Participants[1].UserID != "user-b" {
		t.Fatalf("start snapshot must include only existing humans, got %#v known=%t", initial, known)
	}

	left := &discordgo.VoiceStateUpdate{VoiceState: &discordgo.VoiceState{UserID: "user-b", GuildID: "guild-01", ChannelID: ""}}
	if err := session.State.OnInterface(session, left); err != nil {
		t.Fatal(err)
	}
	client.onGatewayResumed(session, &discordgo.Resumed{})
	if len(sink.snapshots) != 1 || sink.snapshots[0].Revision != 2 || len(sink.snapshots[0].Participants) != 1 || sink.snapshots[0].Participants[0].UserID != "user-a" {
		t.Fatalf("resume must replace state with the current authoritative VC members: %#v", sink.snapshots)
	}

	left = &discordgo.VoiceStateUpdate{VoiceState: &discordgo.VoiceState{UserID: "user-a", GuildID: "guild-01", ChannelID: ""}}
	if err := session.State.OnInterface(session, left); err != nil {
		t.Fatal(err)
	}
	client.onGuildCreate(session, &discordgo.GuildCreate{Guild: &discordgo.Guild{ID: "guild-01"}})
	if len(sink.snapshots) != 2 || sink.snapshots[1].Revision != 3 || len(sink.snapshots[1].Participants) != 0 {
		t.Fatalf("full reconnect guild sync must publish an authoritative empty VC: %#v", sink.snapshots)
	}
}

func TestAutoStartPresenceSyncEmitsExistingHumanParticipant(t *testing.T) {
	session := &discordgo.Session{State: discordgo.NewState(), StateEnabled: true}
	session.State.TrackVoice = true
	session.State.User = &discordgo.User{ID: "bot-01", Bot: true}
	if err := session.State.GuildAdd(&discordgo.Guild{
		ID: "guild-01",
		VoiceStates: []*discordgo.VoiceState{
			{UserID: "bot-01", GuildID: "guild-01", ChannelID: "voice-01", Member: &discordgo.Member{User: &discordgo.User{ID: "bot-01", Bot: true}}},
			{UserID: "bot-other", GuildID: "guild-01", ChannelID: "voice-01", Member: &discordgo.Member{User: &discordgo.User{ID: "bot-other", Bot: true}}},
			{UserID: "user-01", GuildID: "guild-01", ChannelID: "voice-01", Member: &discordgo.Member{Nick: "Alice", User: &discordgo.User{ID: "user-01", Username: "alice"}}},
		},
	}); err != nil {
		t.Fatal(err)
	}
	sink := &fakeEventSink{}
	client := &RealClient{session: session, sink: sink}

	client.SetAutoStartVoiceTargets([]AutoStartVoiceTarget{{
		StreamID:       "stream-01",
		GuildID:        "guild-01",
		VoiceChannelID: "voice-01",
	}})

	if len(sink.voiceJoins) != 1 {
		t.Fatalf("expected one synthetic auto-start join, got %#v", sink.voiceJoins)
	}
	if got := sink.voiceJoins[0]; got.GuildID != "guild-01" || got.VoiceChannelID != "voice-01" || got.UserID != "user-01" || got.Username != "Alice" {
		t.Fatalf("unexpected existing participant join event: %#v", got)
	}
}

func TestVoiceParticipantSnapshotUsesDiscordDisplayNameAndGuildAvatar(t *testing.T) {
	session := &discordgo.Session{State: discordgo.NewState(), StateEnabled: true}
	session.State.User = &discordgo.User{ID: "bot-self"}
	guild := &discordgo.Guild{
		ID: "guild-01",
		VoiceStates: []*discordgo.VoiceState{
			{UserID: "bot-self", GuildID: "guild-01", ChannelID: "voice-01", Member: &discordgo.Member{GuildID: "guild-01", User: &discordgo.User{ID: "bot-self", Username: "self", Bot: true}}},
			{UserID: "user-nick", GuildID: "guild-01", ChannelID: "voice-01", Member: &discordgo.Member{Nick: "Server Alice", Avatar: "guild-avatar", User: &discordgo.User{ID: "user-nick", Username: "alice", GlobalName: "Global Alice", Avatar: "user-avatar"}}},
			{UserID: "user-global", GuildID: "guild-01", ChannelID: "voice-01", Member: &discordgo.Member{GuildID: "guild-01", User: &discordgo.User{ID: "user-global", Username: "bob", GlobalName: "Global Bob", Avatar: "bob-avatar"}}},
			{UserID: "bot-other", GuildID: "guild-01", ChannelID: "voice-01", Member: &discordgo.Member{GuildID: "guild-01", User: &discordgo.User{ID: "bot-other", Username: "Helper", Bot: true}}},
		},
	}
	if err := session.State.GuildAdd(guild); err != nil {
		t.Fatal(err)
	}
	client := &RealClient{session: session, job: VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01"}}

	snapshot, known := client.SnapshotVoiceParticipants(client.job)
	if !known {
		t.Fatal("participant snapshot was not available")
	}
	participants := make(map[string]VoiceParticipant, len(snapshot.Participants))
	for _, participant := range snapshot.Participants {
		participants[participant.UserID] = participant
	}
	if _, present := participants["bot-self"]; present {
		t.Fatal("the connected bot itself must not be included")
	}
	if other := participants["bot-other"]; !other.IsBot || other.Username != "Helper" {
		t.Fatalf("another bot must remain visible, got %#v", other)
	}
	expectedGuildMember := *guild.VoiceStates[1].Member
	expectedGuildMember.GuildID = guild.ID
	if nick := participants["user-nick"]; nick.Username != "Server Alice" || nick.AvatarURL != expectedGuildMember.AvatarURL("128") {
		t.Fatalf("guild nickname/avatar must win, got %#v", nick)
	}
	if global := participants["user-global"]; global.Username != "Global Bob" || global.AvatarURL != guild.VoiceStates[2].Member.User.AvatarURL("128") {
		t.Fatalf("global name and user avatar must be fallbacks, got %#v", global)
	}
}

func TestUniqueHumanVoiceParticipantProvidesCaptionIdentityFallback(t *testing.T) {
	session := &discordgo.Session{State: discordgo.NewState(), StateEnabled: true}
	session.State.User = &discordgo.User{ID: "bot-self"}
	if err := session.State.GuildAdd(&discordgo.Guild{
		ID: "guild-01",
		VoiceStates: []*discordgo.VoiceState{
			{UserID: "bot-self", GuildID: "guild-01", ChannelID: "voice-01"},
			{UserID: "user-01", GuildID: "guild-01", ChannelID: "voice-01"},
			{UserID: "bot-other", GuildID: "guild-01", ChannelID: "voice-01", Member: &discordgo.Member{User: &discordgo.User{ID: "bot-other", Bot: true}}},
		},
	}); err != nil {
		t.Fatal(err)
	}
	job := VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01"}
	client := &RealClient{session: session, job: job}
	if got := client.uniqueHumanVoiceParticipant(job); got != "user-01" {
		t.Fatalf("expected the only human participant as fallback, got %q", got)
	}

	if err := session.State.OnInterface(session, &discordgo.VoiceStateUpdate{VoiceState: &discordgo.VoiceState{UserID: "user-02", GuildID: "guild-01", ChannelID: "voice-01"}}); err != nil {
		t.Fatal(err)
	}
	if got := client.uniqueHumanVoiceParticipant(job); got != "" {
		t.Fatalf("fallback must remain empty when multiple humans are present, got %q", got)
	}
}

func TestReadyRestartsConnectedStateAndParticipantSnapshot(t *testing.T) {
	session := &discordgo.Session{State: discordgo.NewState(), StateEnabled: true}
	session.State.User = &discordgo.User{ID: "bot-01"}
	if err := session.State.GuildAdd(&discordgo.Guild{
		ID: "guild-01",
		VoiceStates: []*discordgo.VoiceState{
			{UserID: "bot-01", GuildID: "guild-01", ChannelID: "voice-01"},
			{UserID: "user-01", GuildID: "guild-01", ChannelID: "voice-01"},
		},
	}); err != nil {
		t.Fatal(err)
	}
	sink := &snapshotEventSink{}
	client := &RealClient{
		session: session,
		sink:    sink,
		job:     VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01"},
	}

	client.onReady(session, &discordgo.Ready{})
	if sink.connectedCount != 1 {
		t.Fatalf("READY connected notifications = %d, want 1", sink.connectedCount)
	}
	if len(sink.snapshots) != 1 || len(sink.snapshots[0].Participants) != 1 || sink.snapshots[0].Participants[0].UserID != "user-01" {
		t.Fatalf("READY participant snapshot = %#v", sink.snapshots)
	}
}

func TestVoiceStateUpdateUsesDeltaForSnapshotAwareSink(t *testing.T) {
	session := &discordgo.Session{State: discordgo.NewState(), StateEnabled: true}
	session.State.User = &discordgo.User{ID: "bot-01"}
	if err := session.State.GuildAdd(&discordgo.Guild{ID: "guild-01", VoiceStates: []*discordgo.VoiceState{
		{UserID: "user-01", GuildID: "guild-01", ChannelID: "voice-01"},
	}}); err != nil {
		t.Fatal(err)
	}
	job := VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01"}
	sink := &snapshotEventSink{}
	client := &RealClient{session: session, sink: sink, job: job}
	left := &discordgo.VoiceStateUpdate{VoiceState: &discordgo.VoiceState{UserID: "user-01", GuildID: "guild-01", ChannelID: ""}}
	if err := session.State.OnInterface(session, left); err != nil {
		t.Fatal(err)
	}
	client.onVoiceStateUpdate(session, left)

	if len(sink.snapshots) != 0 {
		t.Fatalf("target VC leave must not replace the full participant set: %#v", sink.snapshots)
	}
	if len(sink.participants) != 1 || sink.participants[0].UserID != "user-01" || sink.participants[0].Present {
		t.Fatalf("snapshot-aware sink must receive the departed user's delta: %#v", sink.participants)
	}
}

func TestDelayedTargetLeaveDeltaPreservesCurrentParticipant(t *testing.T) {
	session := &discordgo.Session{State: discordgo.NewState(), StateEnabled: true}
	session.State.User = &discordgo.User{ID: "bot-01"}
	if err := session.State.GuildAdd(&discordgo.Guild{ID: "guild-01", VoiceStates: []*discordgo.VoiceState{
		{UserID: "bot-01", GuildID: "guild-01", ChannelID: "voice-01"},
		{UserID: "user-current", GuildID: "guild-01", ChannelID: "voice-01"},
	}}); err != nil {
		t.Fatal(err)
	}
	job := VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01"}
	sink := &participantStateSink{participantsByID: map[string]bool{"user-current": true}}
	client := &RealClient{session: session, sink: sink, job: job}

	// DiscordGo applies State before it asynchronously invokes typed handlers.
	// This leave belongs to a prior job/state epoch: the departed user is no
	// longer in State, while the current job's participant remains in the VC.
	delayedLeave := &discordgo.VoiceStateUpdate{
		VoiceState:   &discordgo.VoiceState{UserID: "user-old", GuildID: "guild-01", ChannelID: ""},
		BeforeUpdate: &discordgo.VoiceState{UserID: "user-old", GuildID: "guild-01", ChannelID: "voice-01"},
	}
	client.onVoiceStateUpdate(session, delayedLeave)

	if !sink.participantsByID["user-current"] {
		t.Fatalf("delayed leave erased the current participant: %#v", sink.participantsByID)
	}
	if len(sink.snapshots) != 0 {
		t.Fatalf("delayed leave must not replace the full participant set: %#v", sink.snapshots)
	}
	if len(sink.participants) != 1 || sink.participants[0].UserID != "user-old" || sink.participants[0].Present {
		t.Fatalf("delayed leave must emit only the departed user's delta: %#v", sink.participants)
	}
}

func TestVoiceStateUpdateDoesNotClearOtherParticipantsWhenStateCacheIsIncomplete(t *testing.T) {
	session := &discordgo.Session{State: discordgo.NewState(), StateEnabled: true}
	session.State.TrackVoice = true
	session.State.User = &discordgo.User{ID: "bot-01"}
	if err := session.State.GuildAdd(&discordgo.Guild{ID: "guild-01"}); err != nil {
		t.Fatal(err)
	}
	job := VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01"}
	sink := &participantStateSink{participantsByID: map[string]bool{"user-current": true}}
	client := &RealClient{session: session, sink: sink, job: job}

	// During a guild-state rebuild, Discord can dispatch one user's target-VC
	// transition while the cached VoiceStates slice temporarily omits another
	// participant. The event is authoritative only for user-old; replacing the
	// whole participant set from that incomplete cache would erase user-current.
	client.onVoiceStateUpdate(session, &discordgo.VoiceStateUpdate{
		VoiceState:   &discordgo.VoiceState{UserID: "user-old", GuildID: "guild-01", ChannelID: ""},
		BeforeUpdate: &discordgo.VoiceState{UserID: "user-old", GuildID: "guild-01", ChannelID: "voice-01"},
	})

	if !sink.participantsByID["user-current"] {
		t.Fatalf("single-user transition erased another participant: %#v", sink.participantsByID)
	}
	if len(sink.snapshots) != 0 {
		t.Fatalf("single-user transition must not publish a full cache snapshot: %#v", sink.snapshots)
	}
	if len(sink.participants) != 1 || sink.participants[0].UserID != "user-old" || sink.participants[0].Present {
		t.Fatalf("expected only the departed user's delta, got %#v", sink.participants)
	}
}

func TestOwnVoiceStateJoinDoesNotBecomeParticipant(t *testing.T) {
	sink := &fakeEventSink{}
	client := &RealClient{
		sink: sink,
		job:  VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01"},
	}
	session := &discordgo.Session{State: discordgo.NewState()}
	session.State.User = &discordgo.User{ID: "bot-01"}

	client.onVoiceStateUpdate(session, &discordgo.VoiceStateUpdate{
		VoiceState: &discordgo.VoiceState{UserID: "bot-01", GuildID: "guild-01", ChannelID: "voice-01"},
	})
	if len(sink.participants) != 0 {
		t.Fatalf("bot voice join must not be reported as a participant: %#v", sink.participants)
	}

	client.onVoiceStateUpdate(session, &discordgo.VoiceStateUpdate{
		VoiceState: &discordgo.VoiceState{UserID: "user-01", GuildID: "guild-01", ChannelID: "voice-01"},
	})
	if len(sink.participants) != 1 || sink.participants[0].UserID != "user-01" || !sink.participants[0].Present {
		t.Fatalf("human voice join was not reported: %#v", sink.participants)
	}
}

func TestMessageCreatePublishesOnlyActiveTextChannelMessages(t *testing.T) {
	sink := &fakeEventSink{}
	client := &RealClient{
		sink: sink,
		job: VoiceJob{
			StreamID:       "stream-01",
			GuildID:        "guild-01",
			VoiceChannelID: "voice-01",
			TextChannelID:  "text-01",
		},
	}
	session := &discordgo.Session{State: discordgo.NewState()}
	session.State.User = &discordgo.User{ID: "bot-01"}

	client.onMessageCreate(session, &discordgo.MessageCreate{Message: &discordgo.Message{
		ID:        "msg-ignored",
		GuildID:   "guild-01",
		ChannelID: "text-other",
		Author:    &discordgo.User{ID: "user-01", Username: "alice"},
		Content:   "wrong channel",
	}})
	if sink.chatMessage.MessageID != "" {
		t.Fatalf("message from another channel should be ignored: %#v", sink.chatMessage)
	}

	client.onMessageCreate(session, &discordgo.MessageCreate{Message: &discordgo.Message{
		ID:        "msg-01",
		GuildID:   "guild-01",
		ChannelID: "text-01",
		Author:    &discordgo.User{ID: "user-01", Username: "alice"},
		Content:   " 本番開始します ",
	}})
	if sink.chatMessage.StreamID != "stream-01" || sink.chatMessage.TextChannelID != "text-01" || sink.chatMessage.UserID != "user-01" || sink.chatMessage.Username != "alice" || sink.chatMessage.Content != "本番開始します" {
		t.Fatalf("active text channel message was not published: %#v", sink.chatMessage)
	}

	client.onMessageCreate(session, &discordgo.MessageCreate{Message: &discordgo.Message{
		ID:        "msg-bot",
		GuildID:   "guild-01",
		ChannelID: "text-01",
		Author:    &discordgo.User{ID: "bot-other", Username: "helper", GlobalName: "Helper Bot", Avatar: "avatar-hash", Bot: true},
		Content:   "bot message",
	}})
	if sink.chatMessage.MessageID != "msg-bot" || sink.chatMessage.UserID != "bot-other" || sink.chatMessage.Username != "Helper Bot" || sink.chatMessage.AvatarURL == "" || !sink.chatMessage.IsBot {
		t.Fatalf("another bot's message should be published: %#v", sink.chatMessage)
	}

	client.onMessageCreate(session, &discordgo.MessageCreate{Message: &discordgo.Message{
		ID:        "msg-self",
		GuildID:   "guild-01",
		ChannelID: "text-01",
		Author:    &discordgo.User{ID: "bot-01", Username: "self", Bot: true},
		Content:   "self message",
	}})
	if sink.chatMessage.MessageID != "msg-bot" {
		t.Fatalf("bot's own message should be ignored: %#v", sink.chatMessage)
	}
}
