package httpapi

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/example/autostream-discord-bot/internal/control"
	"github.com/example/autostream-discord-bot/internal/discord"
	"github.com/example/autostream-discord-bot/internal/jobs"
)

type waitingStartVoice struct {
	httpFakeVoice
	credentialReady bool
	joinEntered     chan struct{}
	releaseJoin     chan struct{}
}

func (v *waitingStartVoice) JoinVoice(job discord.VoiceJob) error {
	if !v.credentialReady {
		return errors.New("Gateway credential was not reconciled before JoinVoice")
	}
	if v.joinEntered != nil {
		close(v.joinEntered)
		<-v.releaseJoin
	}
	return v.httpFakeVoice.JoinVoice(job)
}

func waitingStartRequest() *http.Request {
	request := httptest.NewRequest(http.MethodPost, "/jobs/start", strings.NewReader(`{
		"schema_version":2,"stream_id":"selected","job_generation":7,
		"discord_target":{"revision":11,"resolved":{
			"guild_id":"100000000000000001","text_channel_id":"100000000000000002","voice_channel_id":"100000000000000003"
		}}
	}`))
	request.Header.Set("Authorization", "Bearer expected")
	request.Header.Set("Content-Type", "application/json")
	return request
}

func TestStartRefreshReconcilesCurrentCredentialBeforeVoiceJoin(t *testing.T) {
	voice := &waitingStartVoice{joinEntered: make(chan struct{}), releaseJoin: make(chan struct{})}
	manager := jobs.NewManager(voice)
	var release sync.Once
	t.Cleanup(func() {
		release.Do(func() { close(voice.releaseJoin) })
		if id := manager.CurrentStreamID(); id != "" {
			_ = manager.Stop(id)
		}
	})
	callbacks := 0
	manager.SetRuntimeGatewayReconciler(func(ctx context.Context, cfg control.RuntimeConfig) error {
		callbacks++
		if cfg.Service.CurrentStreamID != "selected" || len(cfg.Assignments) != 1 || cfg.Assignments[0].StreamID != "selected" {
			return errors.New("fresh current execution authority absent")
		}
		voice.credentialReady = true
		return nil
	})
	reads := 0
	handler := NewServerWithRuntimeConfig("discord_bot", manager, TokenVerifier{PlainToken: "expected"}, func(context.Context) (control.RuntimeConfig, error) {
		reads++
		return control.RuntimeConfig{
			Service: control.RegisteredService{ServiceID: "own", ServiceType: control.ServiceType, CurrentStreamID: "selected"},
			Assignments: []control.StreamServiceAssignment{{StreamID: "selected", ServiceID: "own", ServiceType: control.ServiceType, AssignmentRole: "primary", AssignedAt: time.Unix(200, 0).UTC()}},
			// Resolved v2 target IDs must survive runtime refresh unchanged.
			StreamDiscordConfigs: []control.StreamDiscordConfig{{StreamID: "selected", GuildID: "other", VoiceChannelID: "other"}},
		}, nil
	})
	response := httptest.NewRecorder()
	startDone := make(chan struct{})
	go func() {
		handler.ServeHTTP(response, waitingStartRequest())
		close(startDone)
	}()
	select {
	case <-voice.joinEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("start did not reach credentialed voice join")
	}
	periodicQueued, periodicEntered := make(chan struct{}), make(chan struct{})
	go func() {
		close(periodicQueued)
		_ = manager.RefreshRuntimeConfig(func() error {
			close(periodicEntered)
			return nil
		})
	}()
	<-periodicQueued
	select {
	case <-periodicEntered:
		t.Fatal("periodic refresh reconfigured Gateway during JoinVoice")
	case <-time.After(100 * time.Millisecond):
	}
	release.Do(func() { close(voice.releaseJoin) })
	select {
	case <-startDone:
	case <-time.After(2 * time.Second):
		t.Fatal("start did not finish")
	}
	select {
	case <-periodicEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("serialized periodic refresh never resumed")
	}
	if response.Code != http.StatusAccepted || callbacks != 1 || reads != 1 || voice.joinCalls != 1 ||
		voice.joined.GuildID != "100000000000000001" || voice.joined.VoiceChannelID != "100000000000000003" ||
		voice.joined.DiscordTargetRevision != 11 {
		t.Fatal("start refresh/join order or immutable target changed", response.Code, response.Body.String())
	}
}

func TestWaitingMonitoringCannotAuthorizeVoiceExecution(t *testing.T) {
	voice := &waitingStartVoice{}
	manager := jobs.NewManager(voice)
	reconciled := false
	manager.SetRuntimeGatewayReconciler(func(context.Context, control.RuntimeConfig) error {
		reconciled = true
		voice.credentialReady = true
		return nil
	})
	handler := NewServerWithRuntimeConfig("discord_bot", manager, TokenVerifier{PlainToken: "expected"}, func(context.Context) (control.RuntimeConfig, error) {
		return control.RuntimeConfig{
			Service: control.RegisteredService{ServiceID: "own", ServiceType: control.ServiceType},
			Assignments: []control.StreamServiceAssignment{},
			DiscordWaitingMonitor: &control.DiscordWaitingMonitor{Version: 1, Bindings: []control.DiscordWaitingMonitorBinding{{
				DiscordWaitingCredentialBinding: control.DiscordWaitingCredentialBinding{StreamID: "selected", ServiceID: "own", Ready: true},
				GuildID: "100000000000000001", VoiceChannelID: "100000000000000003", TargetRevision: 11,
			}}},
		}, nil
	})
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, waitingStartRequest())
	if response.Code != http.StatusForbidden || reconciled || voice.joinCalls != 0 || manager.CurrentStreamID() != "" {
		t.Fatal("monitor selection was mistaken for an execution assignment", response.Code, response.Body.String())
	}
}
