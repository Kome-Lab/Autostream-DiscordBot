package discord

import (
	"context"
	"testing"
	"time"

	"github.com/cartridge-gg/discordgo"
)

// Exercise the production reconciler with a real, unopened Gateway session.
// No Discord connection or provider credential is needed for these checks.
func activeGatewayAuthorityFixture(t *testing.T) (*RealClient, *discordgo.Session, <-chan struct{}, <-chan struct{}) {
	t.Helper()
	cfg := Config{BotToken: "synthetic-active-credential"}
	client := NewDormantRealClient()
	session, err := client.newGatewaySession(cfg)
	if err != nil {
		t.Fatal(err)
	}
	session.SyncEvents = true
	session.DataReady = true
	stop, done := make(chan struct{}), make(chan struct{})
	daveCtx, daveCancel := context.WithCancel(context.Background())
	client.cfg, client.session = cfg, session
	client.job = VoiceJob{StreamID: "running-A", GuildID: "guild-A", VoiceChannelID: "voice-A", JobGeneration: 3}
	client.voiceGeneration = 7
	client.audioStop, client.audioDone, client.daveCancel = stop, done, daveCancel
	client.status = Status{
		Connected: true, VoiceConnected: true, AudioReceiving: true,
		AudioForwardActive: true, CaptionAudioForwardActive: true,
		CurrentGuildID: "guild-A", CurrentVoiceID: "voice-A",
		DAVEInitialized: true, DAVEReady: true,
	}
	go func() {
		<-stop
		close(done)
	}()
	t.Cleanup(func() {
		_ = client.LeaveVoice("")
		_ = session.Close()
	})
	return client, session, done, daveCtx.Done()
}

func TestReconcileGatewayRevokesActiveVoiceAuthority(t *testing.T) {
	client, session, audioDone, daveDone := activeGatewayAuthorityFixture(t)
	if err := client.ReconcileGateway(Config{}, false); err != nil {
		t.Fatal(err)
	}
	status := client.Status()
	if status.Connected || status.VoiceConnected || status.AudioReceiving ||
		status.AudioForwardActive || status.CaptionAudioForwardActive || status.DAVEReady {
		t.Fatal("revoked real client retained Gateway, voice, or media activity")
	}
	if session.DataReady || client.session != nil || client.cfg.BotToken != "" || client.job.StreamID != "" {
		t.Fatal("revoked real client retained its session, credential, or voice job")
	}
	if client.voiceGeneration <= 7 || status.CurrentGuildID != "" || status.CurrentVoiceID != "" {
		t.Fatal("revocation did not fence the old voice generation and target")
	}
	for _, done := range []<-chan struct{}{audioDone, daveDone} {
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("revocation did not stop a media worker")
		}
	}
	// A delayed handler from the detached Gateway cannot restore authority.
	client.onGatewayResumed(session, &discordgo.Resumed{})
	if client.Status().Connected {
		t.Fatal("a stale Gateway event reactivated revoked authority")
	}
	if err := client.ReconcileGateway(Config{}, false); err != nil {
		t.Fatal("repeated revocation must be safe")
	}
}

func TestReconcileGatewayRefreshPreservesActiveVoiceAuthority(t *testing.T) {
	for _, token := range []string{"synthetic-active-credential", "synthetic-waiting-B-credential"} {
		t.Run(token, func(t *testing.T) {
			client, session, audioDone, daveDone := activeGatewayAuthorityFixture(t)
			if err := client.ReconcileGateway(Config{BotToken: token}, true); err != nil {
				t.Fatal(err)
			}
			status := client.Status()
			if client.session != session || !session.DataReady || client.cfg.BotToken != "synthetic-active-credential" ||
				client.job.StreamID != "running-A" || client.voiceGeneration != 7 ||
				!status.Connected || !status.VoiceConnected || !status.AudioForwardActive || !status.CaptionAudioForwardActive {
				t.Fatal("a selected credential refresh disrupted the active real client")
			}
			for _, done := range []<-chan struct{}{audioDone, daveDone} {
				select {
				case <-done:
					t.Fatal("a selected credential refresh stopped an active media worker")
				default:
				}
			}
		})
	}
}
