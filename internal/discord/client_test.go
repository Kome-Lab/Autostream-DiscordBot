package discord

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/example/autostream-discord-bot/internal/audioforward"
)

type fakeEventSink struct {
	activeStreamID string
	activeUserID   string
	voiceJoin      VoiceJoinEvent
	chatMessage    ChatMessageEvent
}

func (f *fakeEventSink) VoiceUserJoined(event VoiceJoinEvent) {
	f.voiceJoin = event
}
func (f *fakeEventSink) ParticipantChanged(ParticipantEvent) {}
func (f *fakeEventSink) ChatMessageReceived(event ChatMessageEvent) {
	f.chatMessage = event
}
func (f *fakeEventSink) ActiveSpeakerDetected(streamID, userID string) {
	f.activeStreamID = streamID
	f.activeUserID = userID
}
func (f *fakeEventSink) DiscordConnected()          {}
func (f *fakeEventSink) DiscordDisconnected(string) {}

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

func runSyntheticForwardBatch(t *testing.T, client *RealClient, forwarder *fakeAudioForwarder, job VoiceJob, expectedCalls int) {
	t.Helper()
	packets := make(chan *discordgo.Packet, 20)
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		client.forwardOpus(job, packets, stop, forwarder, "discord-bot-01")
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
		job:  VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01"},
	}

	client.onVoiceSpeakingUpdate(nil, &discordgo.VoiceSpeakingUpdate{UserID: "user-01", SSRC: 42, Speaking: true})

	if sink.activeStreamID != "stream-01" || sink.activeUserID != "user-01" {
		t.Fatalf("active speaker was not reported from speaking update: %#v", sink)
	}
	if got := client.userForSSRC(42); got != "user-01" {
		t.Fatalf("speaking update did not populate SSRC user map, got %q", got)
	}
}

func TestForwardOpusForwardsSyntheticPacketsAndUpdatesStatus(t *testing.T) {
	forwarder := newFakeAudioForwarder()
	client := &RealClient{}
	client.onVoiceSpeakingUpdate(nil, &discordgo.VoiceSpeakingUpdate{UserID: "user-01", SSRC: 42, Speaking: true})
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
	go func() {
		defer close(done)
		client.forwardOpus(job, packets, stop, forwarder, "discord-bot-01")
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

func TestForwardOpusForwardsCaptionOnly(t *testing.T) {
	forwarder := newFakeAudioForwarder()
	client := &RealClient{}
	client.onVoiceSpeakingUpdate(nil, &discordgo.VoiceSpeakingUpdate{UserID: "user-01", SSRC: 42, Speaking: true})
	job := VoiceJob{
		StreamID:          "stream-01",
		GuildID:           "guild-01",
		VoiceChannelID:    "voice-01",
		CaptionAudioURL:   "https://worker.example.com/captions",
		CaptionAudioToken: "caption-token",
	}

	runSyntheticForwardBatch(t, client, forwarder, job, 1)

	calls := forwarder.callsSnapshot()
	if len(calls) != 1 {
		t.Fatalf("expected one caption forward call, got %d", len(calls))
	}
	call := calls[0]
	if call.url != job.CaptionAudioURL || call.streamID != job.StreamID || call.source != "discord-bot-01" || call.tokenOverride != job.CaptionAudioToken {
		t.Fatalf("unexpected caption forward context: %#v", call)
	}
	if len(call.packets) != 20 || call.packets[0].UserID != "user-01" {
		t.Fatalf("unexpected caption packet batch: %#v", call.packets)
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
		StreamID:          "stream-01",
		GuildID:           "guild-01",
		VoiceChannelID:    "voice-01",
		EncoderAudioURL:   "https://encoder.example.com/audio",
		CaptionAudioURL:   "https://worker.example.com/captions",
		StreamIngestToken: "encoder-token",
		CaptionAudioToken: "caption-token",
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
				StreamID:          "stream-01",
				GuildID:           "guild-01",
				VoiceChannelID:    "voice-01",
				EncoderAudioURL:   encoderURL,
				CaptionAudioURL:   captionURL,
				StreamIngestToken: "encoder-token",
				CaptionAudioToken: "caption-token",
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

func TestForwardOpusCaptionDoesNotUseEncoderTokenFallback(t *testing.T) {
	forwarder := newFakeAudioForwarder()
	client := &RealClient{}
	job := VoiceJob{
		StreamID:        "stream-01",
		GuildID:         "guild-01",
		VoiceChannelID:  "voice-01",
		CaptionAudioURL: "https://worker.example.com/captions",
	}
	packets := make(chan *discordgo.Packet, 20)
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		client.forwardOpus(job, packets, stop, forwarder, "discord-bot-01")
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
	client := &RealClient{
		sink: sink,
		job:  VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01"},
	}

	client.onVoiceSpeakingUpdate(nil, &discordgo.VoiceSpeakingUpdate{UserID: "user-01", SSRC: 42, Speaking: false})

	if sink.activeStreamID != "" || sink.activeUserID != "" {
		t.Fatalf("stop-speaking update should not publish active speaker: %#v", sink)
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
		Author:    &discordgo.User{ID: "bot-01", Username: "bot", Bot: true},
		Content:   "bot message",
	}})
	if sink.chatMessage.MessageID != "msg-01" {
		t.Fatalf("bot's own message should be ignored: %#v", sink.chatMessage)
	}
}
