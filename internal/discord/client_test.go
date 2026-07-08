package discord

import (
	"context"
	"encoding/json"
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
}

func newFakeAudioForwarder() *fakeAudioForwarder {
	return &fakeAudioForwarder{called: make(chan struct{}, 1)}
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
	f.mu.Unlock()
	select {
	case f.called <- struct{}{}:
	default:
	}
	return nil
}

func (f *fakeAudioForwarder) snapshot() (string, string, string, string, []audioforward.OpusPacket) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.encoderAudioURL, f.streamID, f.source, f.tokenOverride, append([]audioforward.OpusPacket(nil), f.packets...)
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
			VoiceConnected:     true,
			AudioForwardActive: true,
			AudioReceiving:     true,
			CurrentGuildID:     "guild-01",
			CurrentVoiceID:     "voice-01",
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
	if status.VoiceConnected || status.AudioForwardActive || status.AudioReceiving {
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
