package jobs

import (
	"bytes"
	"github.com/example/autostream-discord-bot/internal/control"
	"github.com/example/autostream-discord-bot/internal/discord"
	"log"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestManagerStartsAndStopsVoiceJob(t *testing.T) {
	voice := &fakeVoice{}
	manager := NewManager(voice)
	job := discord.VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01", EncoderAudioURL: "https://encoder.example.com", CaptionAudioURL: "https://caption.example.com", StreamIngestToken: "job-token", CaptionAudioToken: "caption-token", WorkerEventsURL: "https://worker.example.com", WorkerEventsToken: "worker-events-token"}
	if err := manager.Start(job); err != nil {
		t.Fatal(err)
	}
	if voice.joined.StreamID != "stream-01" || manager.CurrentStreamID() != "stream-01" {
		t.Fatalf("job was not started: %#v", voice.joined)
	}
	if voice.joined.EncoderAudioURL != "https://encoder.example.com" {
		t.Fatalf("encoder audio URL was not passed to voice client: %#v", voice.joined)
	}
	if status := manager.Status(); status.CurrentJob == nil || status.CurrentJob.EncoderAudioURL != "" || status.CurrentJob.CaptionAudioURL != "" || status.CurrentJob.StreamIngestToken != "" || status.CurrentJob.CaptionAudioToken != "" || status.CurrentJob.WorkerEventsURL != "" || status.CurrentJob.WorkerEventsToken != "" {
		t.Fatalf("status leaked job secrets: %#v", status.CurrentJob)
	}
	if err := manager.Stop("stream-01"); err != nil {
		t.Fatal(err)
	}
	if voice.leftFor != "stream-01" || manager.CurrentStreamID() != "" {
		t.Fatalf("job was not stopped: left=%q current=%q", voice.leftFor, manager.CurrentStreamID())
	}
}

func TestManagerStartAppliesVoiceDefaults(t *testing.T) {
	voice := &fakeVoice{}
	manager := NewManager(voice)
	manager.SetVoiceDefaults(VoiceDefaults{GuildID: "guild-default", VoiceChannelID: "voice-default", TextChannelID: "text-default"})
	if err := manager.Start(discord.VoiceJob{StreamID: "stream-01"}); err != nil {
		t.Fatal(err)
	}
	if voice.joined.GuildID != "guild-default" || voice.joined.VoiceChannelID != "voice-default" || voice.joined.TextChannelID != "text-default" || voice.joined.CaptionAudioURL != "" {
		t.Fatalf("voice defaults were not applied: %#v", voice.joined)
	}
}

func TestManagerStartAppliesStreamVoiceDefaults(t *testing.T) {
	voice := &fakeVoice{}
	manager := NewManager(voice)
	manager.SetVoiceDefaults(VoiceDefaults{GuildID: "guild-default", VoiceChannelID: "voice-default", TextChannelID: "text-default"})
	manager.SetStreamVoiceDefaults(map[string]VoiceDefaults{
		"stream-01": {
			GuildID:        "guild-stream",
			VoiceChannelID: "voice-stream",
			TextChannelID:  "text-stream",
		},
	})
	if err := manager.Start(discord.VoiceJob{StreamID: "stream-01"}); err != nil {
		t.Fatal(err)
	}
	if voice.joined.GuildID != "guild-stream" || voice.joined.VoiceChannelID != "voice-stream" || voice.joined.TextChannelID != "text-stream" || voice.joined.CaptionAudioURL != "" {
		t.Fatalf("stream voice defaults were not applied: %#v", voice.joined)
	}
}

func TestSetStreamVoiceDefaultsPublishesAutoStartTargets(t *testing.T) {
	voice := &autoStartTargetVoice{}
	manager := NewManager(voice)

	manager.SetStreamVoiceDefaults(map[string]VoiceDefaults{
		"stream-auto":   {GuildID: "guild-auto", VoiceChannelID: "voice-auto", AutoStartEnabled: true},
		"stream-manual": {GuildID: "guild-manual", VoiceChannelID: "voice-manual", AutoStartEnabled: false},
	})

	voice.mu.Lock()
	targets := append([]discord.AutoStartVoiceTarget(nil), voice.targets...)
	voice.mu.Unlock()
	if len(targets) != 1 || targets[0].StreamID != "stream-auto" || targets[0].GuildID != "guild-auto" || targets[0].VoiceChannelID != "voice-auto" {
		t.Fatalf("unexpected auto-start targets: %#v", targets)
	}
}

func TestVoiceUserJoinedStartsMatchingConfiguredStream(t *testing.T) {
	manager := NewManager(&fakeVoice{})
	manager.SetStreamVoiceDefaults(map[string]VoiceDefaults{
		"stream-01": {GuildID: "guild-01", VoiceChannelID: "voice-01", AutoStartEnabled: true},
	})
	starter := &fakeStreamStarter{ch: make(chan string, 2)}
	manager.SetStreamStarter(starter)

	manager.VoiceUserJoined(discord.VoiceJoinEvent{GuildID: "guild-01", VoiceChannelID: "voice-01", UserID: "user-01"})

	select {
	case got := <-starter.ch:
		if got != "stream-01" {
			t.Fatalf("unexpected auto-start stream: %q", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for auto-start")
	}

	manager.VoiceUserJoined(discord.VoiceJoinEvent{GuildID: "guild-01", VoiceChannelID: "voice-01", UserID: "user-02"})
	select {
	case got := <-starter.ch:
		t.Fatalf("duplicate join should be throttled, got %q", got)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestVoiceUserJoinedRetriesServiceUpdateInProgress(t *testing.T) {
	manager := NewManager(&fakeVoice{})
	manager.autoStartRetryDelays = []time.Duration{10 * time.Millisecond, 10 * time.Millisecond}
	manager.SetStreamVoiceDefaults(map[string]VoiceDefaults{
		"stream-01": {GuildID: "guild-01", VoiceChannelID: "voice-01", AutoStartEnabled: true},
	})
	starter := &fakeStreamStarter{
		ch:   make(chan string, 3),
		errs: []error{control.ControlPanelError{StatusCode: 409, Code: "service_update_in_progress"}, control.ControlPanelError{StatusCode: 409, Code: "service_update_in_progress"}, nil},
	}
	manager.SetStreamStarter(starter)

	manager.VoiceUserJoined(discord.VoiceJoinEvent{GuildID: "guild-01", VoiceChannelID: "voice-01", UserID: "user-01"})
	for attempt := 1; attempt <= 3; attempt++ {
		select {
		case got := <-starter.ch:
			if got != "stream-01" {
				t.Fatalf("attempt %d started unexpected stream %q", attempt, got)
			}
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for bounded start retry %d", attempt)
		}
	}
	select {
	case got := <-starter.ch:
		t.Fatalf("retry continued past success: %q", got)
	case <-time.After(80 * time.Millisecond):
	}
}

func TestVoiceUserJoinedRetriesTransientControlPanelFailure(t *testing.T) {
	manager := NewManager(&fakeVoice{})
	manager.autoStartRetryDelays = []time.Duration{10 * time.Millisecond}
	manager.SetStreamVoiceDefaults(map[string]VoiceDefaults{
		"stream-01": {GuildID: "guild-01", VoiceChannelID: "voice-01", AutoStartEnabled: true},
	})
	starter := &fakeStreamStarter{
		ch:   make(chan string, 2),
		errs: []error{control.ControlPanelError{StatusCode: http.StatusBadGateway}, nil},
	}
	manager.SetStreamStarter(starter)

	manager.VoiceUserJoined(discord.VoiceJoinEvent{GuildID: "guild-01", VoiceChannelID: "voice-01", UserID: "user-01"})
	for attempt := 1; attempt <= 2; attempt++ {
		select {
		case got := <-starter.ch:
			if got != "stream-01" {
				t.Fatalf("attempt %d started unexpected stream %q", attempt, got)
			}
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for transient start retry %d", attempt)
		}
	}
	select {
	case got := <-starter.ch:
		t.Fatalf("retry continued past success: %q", got)
	case <-time.After(80 * time.Millisecond):
	}
}

func TestAutoStartFailureLogPreservesHTTPStatusWithoutProviderCode(t *testing.T) {
	previousWriter := log.Writer()
	previousFlags := log.Flags()
	var output bytes.Buffer
	log.SetOutput(&output)
	log.SetFlags(0)
	defer func() {
		log.SetOutput(previousWriter)
		log.SetFlags(previousFlags)
	}()

	err := control.ControlPanelError{StatusCode: http.StatusBadGateway}
	retryable := isRetryableAutoStartError(err)
	logAutoStartRetryFailure("stream-01", 1, err, retryable)

	got := output.String()
	for _, expected := range []string{"error_class=http_status", "http_status=502", "retryable=true", "retry_count=1"} {
		if !strings.Contains(got, expected) {
			t.Fatalf("auto-start failure log missing %q: %q", expected, got)
		}
	}
	if strings.Contains(got, "transport_or_unknown") {
		t.Fatalf("HTTP failure was mislabeled as transport_or_unknown: %q", got)
	}
}

func TestVoiceUserJoinedCancelsStartRetryOnDisconnect(t *testing.T) {
	manager := NewManager(&fakeVoice{})
	manager.autoStartRetryDelays = []time.Duration{100 * time.Millisecond}
	manager.SetStreamVoiceDefaults(map[string]VoiceDefaults{
		"stream-01": {GuildID: "guild-01", VoiceChannelID: "voice-01", AutoStartEnabled: true},
	})
	starter := &fakeStreamStarter{
		ch:   make(chan string, 2),
		errs: []error{control.ControlPanelError{StatusCode: 409, Code: "service_update_in_progress"}, nil},
	}
	manager.SetStreamStarter(starter)
	manager.VoiceUserJoined(discord.VoiceJoinEvent{GuildID: "guild-01", VoiceChannelID: "voice-01", UserID: "user-01"})
	select {
	case <-starter.ch:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for initial start attempt")
	}
	manager.DiscordDisconnected("gateway_disconnect")
	select {
	case got := <-starter.ch:
		t.Fatalf("disconnected retry reached Control Panel: %q", got)
	case <-time.After(180 * time.Millisecond):
	}
}

func TestVoiceUserJoinedRefreshesDefaultsBeforeMatching(t *testing.T) {
	manager := NewManager(&fakeVoice{})
	starter := &fakeStreamStarter{ch: make(chan string, 1)}
	manager.SetStreamStarter(starter)
	manager.SetAutoStartRefresher(func() error {
		manager.SetStreamVoiceDefaults(map[string]VoiceDefaults{
			"stream-new": {GuildID: "guild-new", VoiceChannelID: "voice-new", AutoStartEnabled: true},
		})
		return nil
	})

	manager.VoiceUserJoined(discord.VoiceJoinEvent{GuildID: "guild-new", VoiceChannelID: "voice-new", UserID: "user-01"})
	select {
	case got := <-starter.ch:
		if got != "stream-new" {
			t.Fatalf("unexpected auto-start stream after refresh: %q", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for refreshed auto-start")
	}
}

func TestVoiceUserJoinedRefreshesAndRetriesARearmedSuccessorAfterNotFound(t *testing.T) {
	manager := NewManager(&fakeVoice{})
	manager.SetStreamVoiceDefaults(map[string]VoiceDefaults{
		"stream-old": {GuildID: "guild-01", VoiceChannelID: "voice-01", AutoStartEnabled: true},
	})
	starter := &fakeStreamStarter{
		ch:   make(chan string, 2),
		errs: []error{control.ControlPanelError{StatusCode: 404, Code: "not_found"}, nil},
	}
	manager.SetStreamStarter(starter)
	manager.SetAutoStartRefresher(func() error {
		manager.SetStreamVoiceDefaults(map[string]VoiceDefaults{
			"stream-new": {GuildID: "guild-01", VoiceChannelID: "voice-01", AutoStartEnabled: true},
		})
		return nil
	})

	manager.VoiceUserJoined(discord.VoiceJoinEvent{GuildID: "guild-01", VoiceChannelID: "voice-01", UserID: "user-01"})
	select {
	case got := <-starter.ch:
		if got != "stream-old" {
			t.Fatalf("first auto-start stream = %q, want stream-old", got)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for stale auto-start")
	}
	select {
	case got := <-starter.ch:
		if got != "stream-new" {
			t.Fatalf("successor auto-start stream = %q, want stream-new", got)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for refreshed successor auto-start")
	}
}

func TestVoiceUserJoinedRequiresAutoStartEnabled(t *testing.T) {
	manager := NewManager(&fakeVoice{})
	manager.SetStreamVoiceDefaults(map[string]VoiceDefaults{
		"stream-01": {GuildID: "guild-01", VoiceChannelID: "voice-01"},
	})
	starter := &fakeStreamStarter{ch: make(chan string, 1)}
	manager.SetStreamStarter(starter)

	manager.VoiceUserJoined(discord.VoiceJoinEvent{GuildID: "guild-01", VoiceChannelID: "voice-01", UserID: "user-01"})
	select {
	case got := <-starter.ch:
		t.Fatalf("stream without auto-start trigger should not start, got %q", got)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestVoiceUserJoinedDoesNotStartAmbiguousOrActiveStream(t *testing.T) {
	manager := NewManager(&fakeVoice{})
	manager.SetStreamVoiceDefaults(map[string]VoiceDefaults{
		"stream-01": {GuildID: "guild-01", VoiceChannelID: "voice-01", AutoStartEnabled: true},
		"stream-02": {GuildID: "guild-01", VoiceChannelID: "voice-01", AutoStartEnabled: true},
	})
	starter := &fakeStreamStarter{ch: make(chan string, 1)}
	manager.SetStreamStarter(starter)

	manager.VoiceUserJoined(discord.VoiceJoinEvent{GuildID: "guild-01", VoiceChannelID: "voice-01", UserID: "user-01"})
	select {
	case got := <-starter.ch:
		t.Fatalf("ambiguous voice channel should not start a stream, got %q", got)
	case <-time.After(100 * time.Millisecond):
	}

	manager.SetStreamVoiceDefaults(map[string]VoiceDefaults{
		"stream-01": {GuildID: "guild-01", VoiceChannelID: "voice-01", AutoStartEnabled: true},
	})
	if err := manager.Start(discord.VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01"}); err != nil {
		t.Fatal(err)
	}
	manager.VoiceUserJoined(discord.VoiceJoinEvent{GuildID: "guild-01", VoiceChannelID: "voice-01", UserID: "user-02"})
	select {
	case got := <-starter.ch:
		t.Fatalf("active stream should suppress auto-start, got %q", got)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestManagerRejectsSecondActiveJob(t *testing.T) {
	manager := NewManager(&fakeVoice{})
	if err := manager.Start(discord.VoiceJob{StreamID: "stream-01", GuildID: "guild-01", VoiceChannelID: "voice-01"}); err != nil {
		t.Fatal(err)
	}
	err := manager.Start(discord.VoiceJob{StreamID: "stream-02", GuildID: "guild-01", VoiceChannelID: "voice-01"})
	if err == nil {
		t.Fatal("expected second job to be rejected")
	}
}
