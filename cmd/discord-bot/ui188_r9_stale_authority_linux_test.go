//go:build linux && ui188_chain

package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cartridge-gg/discordgo"
	discord "github.com/example/autostream-discord-bot/internal/discord"
	"github.com/example/autostream-discord-bot/internal/jobs"
)

// Only the tagged external-peer test binary owns this finite delay/control.
// Authority is captured from real Gateway callbacks, never accepted from JSON.
type ui188R9HeldSink struct {
	*jobs.Manager
	mu                                                    sync.Mutex
	old, current                                          *discord.UI188GatewayRef
	voice                                                 *discord.RealClient
	armed, consumed, overflow, oldSessionReserved         bool
	held                                                  *discord.VoiceJoinEvent
	extra                                                 []discord.VoiceJoinEvent
	controllerEntries, oldSessionEntries, oldEpochEntries uint64
	starts                                                map[string]uint64
	streamID                                              string
}

type ui188R9UnitStarter struct{}

func (ui188R9UnitStarter) StartStream(string) error { return nil }

func TestUI188R9StaleAuthoritySeam(t *testing.T) {
	peer := newUI188Peer()
	mux := http.NewServeMux()
	mux.HandleFunc("/ws/", peer.gateway)
	server := httptest.NewServer(mux)
	defer server.Close()
	mux.HandleFunc("/gateway", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{"url": "ws" + strings.TrimPrefix(server.URL, "http") + "/ws"})
	})
	previous := discordgo.EndpointGateway
	discordgo.EndpointGateway = server.URL + "/gateway"
	defer func() { discordgo.EndpointGateway = previous }()
	voice, err := discord.NewRealClient(discord.Config{BotToken: "UI188-EXTERNAL-TEST-ONLY"})
	if err != nil {
		t.Fatal(err)
	}
	manager := jobs.NewManager(voice)
	sink := &ui188R9HeldSink{Manager: manager, voice: voice, starts: map[string]uint64{}}
	voice.SetEventSink(sink)
	manager.SetStreamStarter(ui188R9CountedStarter{sink: sink, next: ui188R9UnitStarter{}})
	if err := voice.Connect(); err != nil {
		t.Fatal(err)
	}
	defer voice.ReconcileGateway(discord.Config{}, false)
	defaults := func(epoch string) {
		manager.SetStreamVoiceDefaults(map[string]jobs.VoiceDefaults{"r9-stream": {AssignmentEpoch: epoch, GuildID: "1001", VoiceChannelID: "1003", AutoStartEnabled: true}})
	}
	defaults("old-real-assignment")
	if err := sink.execute("capture-old", "r9-stream"); err != nil {
		t.Fatal(err)
	}
	input := func(body string) {
		r := httptest.NewRequest(http.MethodPost, "/input", strings.NewReader(body))
		w := httptest.NewRecorder()
		peer.input(w, r)
		if w.Code != 202 {
			t.Fatalf("external input=%d", w.Code)
		}
	}
	wait := func(condition func() bool) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			if condition() {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatal("real Gateway observation deadline")
	}
	input(`{"user_id":"1010","channel_id":"1003"}`)
	wait(func() bool { sink.mu.Lock(); defer sink.mu.Unlock(); return sink.held != nil })
	input(`{"user_id":"1010","channel_id":""}`)
	wait(func() bool {
		location, err := voice.UI188GatewayVoiceLocation(sink.old, "1010")
		return err == nil && location == "absent"
	})
	if err := voice.ReconcileGateway(discord.Config{}, false); err != nil {
		t.Fatal(err)
	}
	if err := voice.ReconcileGateway(discord.Config{BotToken: "UI188-EXTERNAL-TEST-ONLY"}, true); err != nil {
		t.Fatal(err)
	}
	defaults("new-real-assignment")
	if err := sink.execute("capture-current", "r9-stream"); err != nil {
		t.Fatal(err)
	}
	wait(func() bool { return voice.UI188CurrentHumanKnown(sink.current, "1010") })
	before := sink.snapshot()
	if err := sink.execute("old-session", ""); err != nil {
		t.Fatal(err)
	}
	afterSession := sink.snapshot()
	if afterSession["controller_entries"] != before["controller_entries"] || afterSession["starter_calls"].(uint64) != 0 {
		t.Fatal("old Session reached start controller")
	}
	if err := sink.execute("old-epoch", ""); err != nil {
		t.Fatal(err)
	}
	afterEpoch := sink.snapshot()
	if afterEpoch["controller_entries"].(uint64)-afterSession["controller_entries"].(uint64) != afterEpoch["old_epoch_entries"].(uint64) || afterEpoch["starter_calls"].(uint64) != 0 {
		t.Fatal("old actual callback did not reach/reject in Manager")
	}
	if err := sink.execute("old-epoch", ""); err == nil {
		t.Fatal("repeated consume accepted")
	}
	input(`{"user_id":"1010","channel_id":"1003"}`)
	wait(func() bool { return sink.snapshot()["starter_calls"].(uint64) == 1 })
	input(`{"user_id":"1010","channel_id":"1003"}`)
	time.Sleep(100 * time.Millisecond)
	if sink.snapshot()["starter_calls"].(uint64) != 1 {
		t.Fatal("current duplicate started twice")
	}
}

func cloneR9Join(e discord.VoiceJoinEvent) discord.VoiceJoinEvent {
	if e.Authority != nil {
		a := *e.Authority
		e.Authority = &a
	}
	return e
}

func (s *ui188R9HeldSink) VoiceUserJoined(e discord.VoiceJoinEvent) {
	s.mu.Lock()
	if s.armed && e.UserID == "1010" && s.old.UI188MatchesCapturedJoin(e) {
		copy := cloneR9Join(e)
		if s.held == nil && !e.ExistingPresence {
			s.held = &copy
		} else if len(s.extra) < 8 {
			s.extra = append(s.extra, copy)
		} else {
			s.overflow = true
		}
		s.mu.Unlock()
		return
	}
	s.controllerEntries++
	s.mu.Unlock()
	s.Manager.VoiceUserJoined(e)
}

var _ discord.EventSink = (*ui188R9HeldSink)(nil)
var _ discord.ParticipantSnapshotSink = (*ui188R9HeldSink)(nil)
var _ discord.ActiveSpeakerStateSink = (*ui188R9HeldSink)(nil)

type ui188R9CountedStarter struct {
	sink *ui188R9HeldSink
	next jobs.StreamStarter
}

func (c ui188R9CountedStarter) StartStream(id string) error {
	c.sink.mu.Lock()
	c.sink.starts[id]++
	c.sink.mu.Unlock()
	return c.next.StartStream(id)
}

func (s *ui188R9HeldSink) snapshot() map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	location := "unavailable"
	humanKnown := s.voice.UI188CurrentHumanKnown(s.current, "1010")
	locationRef := s.current
	if locationRef == nil {
		locationRef = s.old
	}
	if locationRef != nil {
		if value, err := s.voice.UI188GatewayVoiceLocation(locationRef, "1010"); err == nil {
			location = value
		}
	}
	return map[string]any{"old_ref_current": s.voice.UI188GatewayRefCurrent(s.old), "current_ref_current": s.voice.UI188GatewayRefCurrent(s.current), "replacement_ready": s.voice.UI188ReplayReady(s.old, s.current), "held": s.held != nil, "held_matches_old": s.old != nil && s.held != nil && s.old.UI188MatchesCapturedJoin(*s.held), "held_matches_current": s.current != nil && s.held != nil && s.current.UI188MatchesCapturedJoin(*s.held), "extra_held": len(s.extra), "overflow": s.overflow, "armed": s.armed, "consumed": s.consumed, "controller_entries": s.controllerEntries, "old_session_entries": s.oldSessionEntries, "old_epoch_entries": s.oldEpochEntries, "starter_calls": s.starts[s.streamID], "capture_user_location": location, "current_human_known": humanKnown}
}

func (s *ui188R9HeldSink) control(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		value := s.snapshot()
		if streamID := r.URL.Query().Get("stream_id"); streamID != "" {
			_, err := s.voice.UI188CaptureGatewayRef(streamID)
			value["waiting_target_ready"] = err == nil
		}
		_ = json.NewEncoder(w).Encode(value)
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method", 405)
		return
	}
	var input struct {
		Operation string `json:"operation"`
		StreamID  string `json:"stream_id"`
	}
	d := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1024))
	d.DisallowUnknownFields()
	if d.Decode(&input) != nil {
		http.Error(w, "input", 400)
		return
	}
	err := s.execute(input.Operation, input.StreamID)
	if err != nil {
		http.Error(w, err.Error(), 409)
		return
	}
	_ = json.NewEncoder(w).Encode(s.snapshot())
}

func (s *ui188R9HeldSink) execute(operation, streamID string) error {
	s.mu.Lock()
	switch operation {
	case "capture-old":
		if s.old != nil || streamID == "" {
			s.mu.Unlock()
			return errors.New("capture-old precondition")
		}
		ref, err := s.voice.UI188CaptureGatewayRef(streamID)
		if err == nil {
			s.old = ref
			s.streamID = streamID
			s.armed = true
		}
		s.mu.Unlock()
		return err
	case "capture-current":
		if s.old == nil || s.current != nil || streamID != s.streamID || s.held == nil || s.overflow {
			s.mu.Unlock()
			return errors.New("capture-current precondition")
		}
		ref, err := s.voice.UI188CaptureGatewayRef(streamID)
		if err == nil && !s.voice.UI188ReplayReady(s.old, ref) {
			err = errors.New("real Session/epoch replacement missing")
		}
		if err == nil {
			s.current = ref
			s.armed = false
		}
		s.mu.Unlock()
		return err
	case "old-session":
		if s.oldSessionReserved || s.current == nil || s.armed {
			s.mu.Unlock()
			return errors.New("old-session precondition")
		}
		old, current := s.old, s.current
		s.oldSessionReserved = true
		s.mu.Unlock()
		if err := s.voice.UI188ReplayOldSession(old, current, "1010"); err != nil {
			return err
		}
		s.mu.Lock()
		s.oldSessionEntries++
		s.mu.Unlock()
		return nil
	case "old-epoch":
		if s.oldSessionEntries != 1 || s.consumed || s.current == nil || s.held == nil || s.overflow || s.armed {
			s.mu.Unlock()
			return errors.New("old-epoch precondition")
		}
		old, current, canonical := s.old, s.current, cloneR9Join(*s.held)
		extra := append([]discord.VoiceJoinEvent(nil), s.extra...)
		s.consumed = true
		s.mu.Unlock()
		for _, e := range append([]discord.VoiceJoinEvent{canonical}, extra...) {
			if err := s.voice.UI188ReplayOldEpoch(old, current, e); err != nil {
				return err
			}
			s.mu.Lock()
			s.oldEpochEntries++
			s.mu.Unlock()
		}
		return nil
	default:
		s.mu.Unlock()
		return errors.New("unknown finite operation")
	}
}
