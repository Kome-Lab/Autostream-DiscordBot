package jobs

import (
	"github.com/example/autostream-discord-bot/internal/discord"
	"testing"
	"time"
)

func TestUI188AssignmentEpochPresenceAndJoinBurstStartOnce(t *testing.T) {
	manager := NewManager(&fakeVoice{})
	starter := &fakeStreamStarter{ch: make(chan string, 10)}
	manager.SetStreamStarter(starter)
	defaults := map[string]VoiceDefaults{"selected": {AssignmentEpoch: "epoch-B", GuildID: "guild", VoiceChannelID: "voice", AutoStartEnabled: true}}
	manager.SetStreamVoiceDefaults(defaults)
	event := discord.VoiceJoinEvent{GuildID: "guild", VoiceChannelID: "voice", UserID: "human", ExistingPresence: true}
	for _, authority := range []*discord.AutoStartAuthority{nil, {StreamID: "selected", AssignmentEpoch: "epoch-A"}, {StreamID: "other", AssignmentEpoch: "epoch-B"}} {
		event.Authority = authority
		manager.VoiceUserJoined(event)
	}
	select {
	case <-starter.ch:
		t.Fatal("unselected or stale epoch started")
	case <-time.After(20 * time.Millisecond):
	}
	event.Authority = &discord.AutoStartAuthority{StreamID: "selected", AssignmentEpoch: "epoch-B"}
	for range 20 {
		manager.VoiceUserJoined(event)
	}
	select {
	case id := <-starter.ch:
		if id != "selected" {
			t.Fatal(id)
		}
	case <-time.After(time.Second):
		t.Fatal("existing presence did not start selected assignment")
	}
	select {
	case <-starter.ch:
		t.Fatal("presence burst issued duplicate start")
	case <-time.After(30 * time.Millisecond):
	}
	manager.SetStreamVoiceDefaults(nil)
	defaults["selected"] = VoiceDefaults{AssignmentEpoch: "epoch-C", GuildID: "guild", VoiceChannelID: "voice", AutoStartEnabled: true}
	manager.SetStreamVoiceDefaults(defaults)
	event.Authority = &discord.AutoStartAuthority{StreamID: "selected", AssignmentEpoch: "epoch-C"}
	manager.VoiceUserJoined(event)
	select {
	case <-starter.ch:
	case <-time.After(time.Second):
		t.Fatal("removed assignment retained previous epoch cooldown")
	}
}

func TestUI188CredentialReferenceResolvedOnceWhileSelected(t *testing.T) {
	voice := &ui188GatewayVoice{}
	manager := NewManager(voice)
	calls := 0
	resolve := func() (string, error) { calls++; return "test-only-token", nil }
	for range 3 {
		if err := manager.ReconcileGatewayCredential("selected/ref/rev1", true, resolve); err != nil {
			t.Fatal(err)
		}
	}
	if calls != 1 {
		t.Fatalf("credential resolves=%d", calls)
	}
	if err := manager.ReconcileGatewayCredential("", false, nil); err != nil {
		t.Fatal(err)
	}
	if err := manager.ReconcileGatewayCredential("selected/ref/rev2", true, resolve); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatal("new selection did not resolve current credential")
	}
	if voice.reconciles != 5 || !voice.selected || voice.token != "test-only-token" {
		t.Fatalf("Gateway did not receive each selection: %+v", voice)
	}
}

type ui188GatewayVoice struct {
	fakeVoice
	reconciles int
	selected   bool
	token      string
}

func (v *ui188GatewayVoice) ReconcileGateway(cfg discord.Config, selected bool) error {
	v.reconciles++
	v.selected, v.token = selected, cfg.BotToken
	return nil
}
