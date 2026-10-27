package jobs

import (
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/example/autostream-discord-bot/internal/control"
	"github.com/example/autostream-discord-bot/internal/discord"
)

type autoStartPresenceVoice struct {
	fakeVoice
	presenceMu sync.Mutex
	present    bool
	known      bool
	targets    []discord.AutoStartVoiceTarget
	read       chan struct{}
	release    chan struct{}
}

func (v *autoStartPresenceVoice) AutoStartVoicePresence(target discord.AutoStartVoiceTarget) (bool, bool) {
	v.presenceMu.Lock()
	v.targets = append(v.targets, target)
	present, known := v.present, v.known
	read, release := v.read, v.release
	v.presenceMu.Unlock()
	if read != nil {
		read <- struct{}{}
	}
	if release != nil {
		<-release
	}
	return present, known
}

type gatedAutoStartStarter struct {
	*fakeStreamStarter
	release <-chan struct{}
}

func (s gatedAutoStartStarter) StartStream(streamID string) error {
	err := s.fakeStreamStarter.StartStream(streamID)
	<-s.release
	return err
}

func waitAutoStartIdle(t *testing.T, manager *Manager, streamID string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		manager.mu.Lock()
		idle := manager.autoStartRetries[streamID] == nil
		manager.mu.Unlock()
		if idle {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("auto-start reservation did not finish")
}

func TestAutoStartRevalidatesHumanPresenceBeforeRequest(t *testing.T) {
	for _, known := range []bool{true, false} {
		name := "empty"
		if !known {
			name = "unavailable"
		}
		t.Run(name, func(t *testing.T) {
			voice := &autoStartPresenceVoice{known: known}
			manager := NewManager(voice)
			manager.SetStreamVoiceDefaults(map[string]VoiceDefaults{"waiting": {AssignmentEpoch: "epoch", GuildID: "guild", VoiceChannelID: "voice", AutoStartEnabled: true}})
			starter := &fakeStreamStarter{ch: make(chan string, 2)}
			manager.SetStreamStarter(starter)
			event := discord.VoiceJoinEvent{GuildID: "guild", VoiceChannelID: "voice", UserID: "human", Authority: &discord.AutoStartAuthority{StreamID: "waiting", AssignmentEpoch: "epoch"}}
			manager.VoiceUserJoined(event)
			waitAutoStartIdle(t, manager, "waiting")
			select {
			case <-starter.ch:
				t.Fatal("queued join started an empty or unknown target")
			default:
			}
			voice.presenceMu.Lock()
			if len(voice.targets) != 1 || voice.targets[0].StreamID != "waiting" || voice.targets[0].AssignmentEpoch != "epoch" || voice.targets[0].VoiceChannelID != "voice" {
				t.Fatal("current waiting target authority was not checked")
			}
			voice.present, voice.known = true, true
			voice.presenceMu.Unlock()
			// A real next join must start immediately, without inheriting the
			// canceled empty attempt's thirty-second cooldown.
			manager.VoiceUserJoined(event)
			select {
			case <-starter.ch:
			case <-time.After(time.Second):
				t.Fatal("next present human join did not start")
			}
			waitAutoStartIdle(t, manager, "waiting")
		})
	}
}

func TestAutoStartRetryStopsWhenLastHumanLeaves(t *testing.T) {
	voice := &autoStartPresenceVoice{present: true, known: true}
	manager := NewManager(voice)
	manager.autoStartRetryDelays = []time.Duration{30 * time.Millisecond}
	manager.SetStreamVoiceDefaults(map[string]VoiceDefaults{"waiting": {GuildID: "guild", VoiceChannelID: "voice", AutoStartEnabled: true}})
	starter := &fakeStreamStarter{ch: make(chan string, 2), errs: []error{control.ControlPanelError{StatusCode: http.StatusServiceUnavailable}, nil}}
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	manager.SetStreamStarter(gatedAutoStartStarter{fakeStreamStarter: starter, release: release})
	manager.VoiceUserJoined(discord.VoiceJoinEvent{GuildID: "guild", VoiceChannelID: "voice", UserID: "human"})
	select {
	case <-starter.ch:
	case <-time.After(time.Second):
		t.Fatal("initial present-human attempt did not run")
	}
	voice.presenceMu.Lock()
	voice.present = false
	voice.presenceMu.Unlock()
	releaseOnce.Do(func() { close(release) })
	waitAutoStartIdle(t, manager, "waiting")
	select {
	case <-starter.ch:
		t.Fatal("retry started after the last human left")
	default:
	}
}

func TestManualStartDoesNotRequireAutoStartHumanPresence(t *testing.T) {
	voice := &autoStartPresenceVoice{known: true}
	manager := NewManager(voice)
	if err := manager.Start(discord.VoiceJob{StreamID: "manual", GuildID: "guild", VoiceChannelID: "voice", JobGeneration: 1}); err != nil {
		t.Fatal(err)
	}
	defer manager.Stop("manual")
	voice.presenceMu.Lock()
	defer voice.presenceMu.Unlock()
	if len(voice.targets) != 0 || manager.CurrentStreamID() != "manual" {
		t.Fatal("manual start was gated on automatic-start presence")
	}
}

func TestAutoStartPresenceReadKeepsAssignmentFence(t *testing.T) {
	voice := &autoStartPresenceVoice{present: true, known: true, read: make(chan struct{}, 1), release: make(chan struct{})}
	manager := NewManager(voice)
	defaults := VoiceDefaults{AssignmentEpoch: "old", GuildID: "guild", VoiceChannelID: "voice", AutoStartEnabled: true}
	manager.SetStreamVoiceDefaults(map[string]VoiceDefaults{"waiting": defaults})
	retry := &autoStartRetry{assignmentEpoch: "old", guildID: "guild", voiceChannelID: "voice", cancel: func() {}}
	manager.autoStartRetries["waiting"] = retry
	result := make(chan bool, 1)
	go func() { result <- manager.autoStartRetryStillValid("waiting", retry) }()
	select {
	case <-voice.read:
	case <-time.After(time.Second):
		t.Fatal("presence read did not run")
	}
	defaults.AssignmentEpoch = "new"
	manager.SetStreamVoiceDefaults(map[string]VoiceDefaults{"waiting": defaults})
	close(voice.release)
	select {
	case valid := <-result:
		if valid {
			t.Fatal("assignment changed during presence read but old request remained valid")
		}
	case <-time.After(time.Second):
		t.Fatal("presence reader did not finish after release")
	}
}
