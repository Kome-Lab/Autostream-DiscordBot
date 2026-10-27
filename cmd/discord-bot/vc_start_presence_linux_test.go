//go:build linux && ui188_chain

package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cartridge-gg/discordgo"
	"github.com/example/autostream-discord-bot/internal/audioforward"
	"github.com/example/autostream-discord-bot/internal/control"
	"github.com/example/autostream-discord-bot/internal/discord"
	"github.com/example/autostream-discord-bot/internal/httpapi"
	"github.com/example/autostream-discord-bot/internal/jobs"
)

type vcPresenceObservation struct{ present, known bool }

type vcSnapshotObservation struct {
	snapshot discord.ParticipantSnapshot
	known    bool
	release  chan struct{}
}

type vcLeaveObservation struct {
	streamID string
	err      error
}

type vcPresenceVoice struct {
	*discord.RealClient
	reads         chan vcPresenceObservation
	joins         chan discord.VoiceJob
	leaves        chan vcLeaveObservation
	snapshots     chan vcSnapshotObservation
	snapshotAbort chan struct{}
}

func (v *vcPresenceVoice) AutoStartVoicePresence(target discord.AutoStartVoiceTarget) (bool, bool) {
	present, known := v.RealClient.AutoStartVoicePresence(target)
	v.reads <- vcPresenceObservation{present, known}
	return present, known
}

func (v *vcPresenceVoice) JoinVoice(job discord.VoiceJob) error {
	// Only the external encrypted Voice transport is controlled. The SDK
	// Gateway, real job handler, Manager.Start, and production Voice job/
	// forwarding setup remain in the path. No audio delivery is claimed here.
	if _, err := v.RealClient.UI188ControlledVoice(job); err != nil {
		return err
	}
	v.joins <- job
	return nil
}

func (v *vcPresenceVoice) SnapshotVoiceParticipants(job discord.VoiceJob) (discord.ParticipantSnapshot, bool) {
	snapshot, known := v.RealClient.SnapshotVoiceParticipants(job)
	observation := vcSnapshotObservation{snapshot: snapshot, known: known, release: make(chan struct{})}
	select {
	case v.snapshots <- observation:
	case <-v.snapshotAbort:
		return discord.ParticipantSnapshot{}, false
	}
	select {
	case <-observation.release:
		return snapshot, known
	case <-v.snapshotAbort:
		return discord.ParticipantSnapshot{}, false
	}
}

func (v *vcPresenceVoice) LeaveVoice(streamID string) error {
	err := v.RealClient.LeaveVoice(streamID)
	v.leaves <- vcLeaveObservation{streamID: streamID, err: err}
	return err
}

type vcObservedStopper struct {
	next    controlStreamStarter
	results chan error
}

func (s vcObservedStopper) StopStream(string) error {
	err := errors.New("fixture requires the production context-aware stopper")
	s.results <- err
	return err
}

func (s vcObservedStopper) StopStreamContext(ctx context.Context, streamID string) error {
	err := s.next.StopStreamContext(ctx, streamID)
	s.results <- err
	return err
}

type vcHeldJoinSink struct {
	*jobs.Manager
	mu   sync.Mutex
	hold bool
	held chan discord.VoiceJoinEvent
}

func (s *vcHeldJoinSink) VoiceUserJoined(event discord.VoiceJoinEvent) {
	s.mu.Lock()
	hold := s.hold
	s.mu.Unlock()
	if hold {
		s.held <- cloneR9Join(event)
		return
	}
	s.Manager.VoiceUserJoined(event)
}

func TestVCStartGatewayPresenceThroughJobHandler(t *testing.T) {
	peer := newUI188Peer()
	mux := http.NewServeMux()
	mux.HandleFunc("/ws/", peer.gateway)
	external := httptest.NewServer(mux)
	defer external.Close()
	mux.HandleFunc("/gateway", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{"url": "ws" + strings.TrimPrefix(external.URL, "http") + "/ws"})
	})
	previousGateway := discordgo.EndpointGateway
	discordgo.EndpointGateway = external.URL + "/gateway"
	defer func() { discordgo.EndpointGateway = previousGateway }()
	realVoice, err := discord.NewRealClient(discord.Config{BotToken: "VC-PRESENCE-ISOLATED-TEST-ONLY"})
	if err != nil {
		t.Fatal(err)
	}
	voice := &vcPresenceVoice{RealClient: realVoice, reads: make(chan vcPresenceObservation, 16), joins: make(chan discord.VoiceJob, 4), leaves: make(chan vcLeaveObservation, 4), snapshots: make(chan vcSnapshotObservation, 8), snapshotAbort: make(chan struct{})}
	gatewayBarrier, err := voice.UI188SynchronousGatewayBarrier("1099")
	if err != nil {
		t.Fatal(err)
	}
	voice.SetAudioForwarder(audioforward.Client{}, "selected-bot")
	manager := jobs.NewManager(voice)
	if !manager.UI188UseFocusedEmptyVoiceTiming() {
		t.Fatal("focused timing requires an idle manager")
	}
	sink := &vcHeldJoinSink{Manager: manager, hold: true, held: make(chan discord.VoiceJoinEvent, 16)}
	voice.SetEventSink(sink)
	if err := voice.Connect(); err != nil {
		t.Fatal(err)
	}
	defer voice.ReconcileGateway(discord.Config{}, false)
	defer manager.Stop("waiting")
	manager.SetStreamVoiceDefaults(map[string]jobs.VoiceDefaults{"waiting": {AssignmentEpoch: "current-epoch", GuildID: "1001", VoiceChannelID: "1003", AutoStartEnabled: true}})
	ref, err := voice.UI188CaptureGatewayRef("waiting")
	if err != nil {
		t.Fatal(err)
	}
	// The controlled Panel endpoint below represents only the remote CP
	// boundary. This test does not prove CP's own claim/authorization/store.
	// Its authenticated outbound client and the receiving Bot handler are real.
	runtimeConfig := func(context.Context) (control.RuntimeConfig, error) {
		return control.RuntimeConfig{
			Service:              control.RegisteredService{ServiceID: "selected-bot"},
			Assignments:          []control.StreamServiceAssignment{{StreamID: "waiting", ServiceID: "selected-bot", ServiceType: control.ServiceType, AssignmentRole: "primary"}},
			StreamDiscordConfigs: []control.StreamDiscordConfig{{StreamID: "waiting", AssignmentRole: "primary", GuildID: "9001", TextChannelID: "9002", VoiceChannelID: "9003"}},
		}, nil
	}
	service := httptest.NewServer(httpapi.NewServerWithRuntimeConfig(control.ServiceType, manager, httpapi.TokenVerifier{PlainToken: "controlled-service-token"}, runtimeConfig))
	defer service.Close()
	attempts := make(chan int, 4)
	dispatchReceipts := make(chan int, 4)
	stopReceipts := make(chan int, 4)
	stopResults := make(chan error, 4)
	releaseFirstResponse := make(chan struct{})
	var releaseFirstOnce sync.Once
	successfulStartArrived := make(chan struct{}, 1)
	releaseSuccessfulStart := make(chan struct{})
	var releaseSuccessfulOnce sync.Once
	var attemptMu sync.Mutex
	requestCount := 0
	stopRequestCount := 0
	panel := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || (r.URL.Path != "/services/streams/waiting/start" && r.URL.Path != "/services/streams/waiting/stop") || r.Header.Get("Authorization") != "Bearer controlled-panel-token" {
			http.Error(w, "controlled Panel authentication rejected", http.StatusForbidden)
			return
		}
		if r.URL.Path == "/services/streams/waiting/stop" {
			attemptMu.Lock()
			stopRequestCount++
			attemptMu.Unlock()
			req, err := http.NewRequestWithContext(r.Context(), http.MethodPost, service.URL+"/jobs/waiting/stop", strings.NewReader(`{}`))
			if err != nil {
				http.Error(w, "stop dispatch request", 500)
				return
			}
			req.Header.Set("Authorization", "Bearer controlled-service-token")
			response, err := service.Client().Do(req)
			if err != nil {
				http.Error(w, "stop dispatch transport", 502)
				return
			}
			defer response.Body.Close()
			var receipt struct {
				Status string `json:"status"`
			}
			if json.NewDecoder(io.LimitReader(response.Body, 4096)).Decode(&receipt) != nil || response.StatusCode != http.StatusAccepted || receipt.Status != "stopped" || r.Context().Err() != nil {
				http.Error(w, "actual stop receipt unavailable or parent canceled", 502)
				return
			}
			stopReceipts <- response.StatusCode
			// CP terminal proof is distinct from the Bot's accepted callback.
			// The real control.Client must consume its canonical terminal receipt.
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]bool{"already_stopped": true})
			return
		}
		attemptMu.Lock()
		requestCount++
		attempt := requestCount
		attemptMu.Unlock()
		attempts <- attempt
		if attempt == 1 {
			select {
			case <-releaseFirstResponse:
			case <-r.Context().Done():
				return
			}
			http.Error(w, "controlled transient outage", http.StatusServiceUnavailable)
			return
		}
		if attempt == 2 {
			successfulStartArrived <- struct{}{}
			select {
			case <-releaseSuccessfulStart:
			case <-r.Context().Done():
				return
			}
		}
		payload := `{"schema_version":2,"stream_id":"waiting","job_generation":17,"discord_target":{"revision":29,"resolved":{"guild_id":"1001","text_channel_id":"1002","voice_channel_id":"1003"}}}`
		req, err := http.NewRequestWithContext(r.Context(), http.MethodPost, service.URL+"/jobs/start", strings.NewReader(payload))
		if err != nil {
			http.Error(w, "dispatch request", 500)
			return
		}
		req.Header.Set("Authorization", "Bearer controlled-service-token")
		req.Header.Set("Content-Type", "application/json")
		response, err := service.Client().Do(req)
		if err != nil {
			http.Error(w, "dispatch transport", 502)
			return
		}
		defer response.Body.Close()
		w.WriteHeader(response.StatusCode)
		_, _ = io.Copy(w, response.Body)
		dispatchReceipts <- response.StatusCode
	}))
	defer panel.Close()
	defer releaseFirstOnce.Do(func() { close(releaseFirstResponse) })
	defer releaseSuccessfulOnce.Do(func() { close(releaseSuccessfulStart) })
	defer close(voice.snapshotAbort)
	controller := controlStreamStarter{client: control.Client{Config: control.Config{ControlPanelURL: panel.URL, Token: "controlled-panel-token", ServiceID: "selected-bot", ServiceName: "selected", ServicePublicURL: service.URL}, HTTP: panel.Client()}}
	manager.SetStreamStarter(controller)
	manager.SetStreamStopper(vcObservedStopper{next: controller, results: stopResults})
	input := func(channel string) {
		t.Helper()
		r := httptest.NewRequest(http.MethodPost, "/input", strings.NewReader(`{"user_id":"1010","channel_id":"`+channel+`"}`))
		w := httptest.NewRecorder()
		peer.input(w, r)
		if w.Code != http.StatusAccepted {
			t.Fatal("controlled external input rejected")
		}
	}
	waitAbsent := func() {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			location, err := voice.UI188GatewayVoiceLocation(ref, "1010")
			if err == nil && location == "absent" {
				// Cache absence precedes asynchronous typed callbacks in the normal
				// SDK. This fixture serializes dispatch before Connect and follows
				// the real leave with an ordered marker over the same WebSocket.
				request := httptest.NewRequest(http.MethodPost, "/input", strings.NewReader(`{"user_id":"1099","member_only":true}`))
				response := httptest.NewRecorder()
				peer.input(response, request)
				if response.Code != http.StatusAccepted {
					t.Fatal("external ordered Gateway marker rejected")
				}
				select {
				case <-gatewayBarrier:
					return
				case <-time.After(5 * time.Second):
					t.Fatal("prior production Gateway handlers did not finish")
				}
			}
			time.Sleep(5 * time.Millisecond)
		}
		t.Fatal("actual SDK cache did not observe external leave")
	}
	waitEmptyRead := func() {
		t.Helper()
		deadline := time.After(5 * time.Second)
		for {
			select {
			case observation := <-voice.reads:
				if observation.known && !observation.present {
					return
				}
			case <-deadline:
				t.Fatal("automatic start did not re-read actual empty SDK State")
			}
		}
	}
	waitIdle := func() {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			if !manager.UI188AutoStartInFlight("waiting") {
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
		t.Fatal("automatic start callback/retry did not finish")
	}
	assertCounts := func(wantRequests int, wantVoice bool) {
		t.Helper()
		attemptMu.Lock()
		gotRequests := requestCount
		attemptMu.Unlock()
		if gotRequests != wantRequests || voice.Status().VoiceConnected != wantVoice {
			t.Fatalf("Panel requests=%d want=%d Voice connected=%t want=%t", gotRequests, wantRequests, voice.Status().VoiceConnected, wantVoice)
		}
	}
	input("1003")
	var held discord.VoiceJoinEvent
	select {
	case held = <-sink.held:
	case <-time.After(5 * time.Second):
		t.Fatal("real Gateway join callback not captured")
	}
	input("")
	waitAbsent()
	sink.mu.Lock()
	sink.hold = false
	sink.mu.Unlock()
	manager.VoiceUserJoined(held)
	waitEmptyRead()
	waitIdle()
	assertCounts(0, false)
	input("1003")
	select {
	case attempt := <-attempts:
		if attempt != 1 {
			t.Fatal("unexpected first controlled Panel request")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("present human did not reach real outbound control.Client")
	}
	input("")
	waitAbsent()
	releaseFirstOnce.Do(func() { close(releaseFirstResponse) })
	waitEmptyRead()
	waitIdle()
	assertCounts(1, false)
	input("1003")
	select {
	case <-successfulStartArrived:
	case <-time.After(5 * time.Second):
		t.Fatal("second start request did not reach the controlled Panel gate")
	}
	// The request has left the Bot, but the remote claim/callback has not run.
	// Preserve that real distributed race: start an empty Voice job, then prove
	// the existing warmup and two confirmations converge through normal stop.
	input("")
	waitAbsent()
	releaseSuccessfulOnce.Do(func() { close(releaseSuccessfulStart) })
	select {
	case job := <-voice.joins:
		if job.StreamID != "waiting" || job.JobGeneration != 17 || job.DiscordTargetRevision != 29 || job.GuildID != "1001" || job.TextChannelID != "1002" || job.VoiceChannelID != "1003" {
			t.Fatal("real jobs/start lost the server-resolved target or generation")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("rejoin did not pass authenticated jobs/start and real Manager.Start to Voice")
	}
	lastRevision := uint64(0)
	nextEmptySnapshot := func() vcSnapshotObservation {
		t.Helper()
		select {
		case observation := <-voice.snapshots:
			if !observation.known || observation.snapshot.StreamID != "waiting" || observation.snapshot.GuildID != "1001" || observation.snapshot.VoiceChannelID != "1003" || len(observation.snapshot.Participants) != 0 || observation.snapshot.Revision <= lastRevision {
				t.Fatal("expected a fresh actual SDK snapshot with no humans or other bots")
			}
			lastRevision = observation.snapshot.Revision
			return observation
		case <-time.After(5 * time.Second):
			t.Fatal("actual empty snapshot phase did not arrive")
			return vcSnapshotObservation{}
		}
	}
	initial := nextEmptySnapshot()
	close(initial.release)
	select {
	case status := <-dispatchReceipts:
		if status != http.StatusAccepted {
			t.Fatalf("actual jobs/start returned %d, want 202", status)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("actual jobs/start did not return its accepted receipt")
	}
	waitIdle()
	assertCounts(2, true)
	assertBeforeStop := func(wantRevision uint64, wantPending bool) {
		t.Helper()
		revision, pending, inFlight := manager.UI188VoiceStopState("waiting")
		attemptMu.Lock()
		stops := stopRequestCount
		attemptMu.Unlock()
		if revision != wantRevision || pending != wantPending || inFlight || stops != 0 {
			t.Fatalf("empty-stop stage revision=%d pending=%t in_flight=%t external_stops=%d", revision, pending, inFlight, stops)
		}
	}
	earlyWarmup := nextEmptySnapshot()
	assertBeforeStop(0, false) // The initial empty snapshot was suppressed.
	close(earlyWarmup.release)
	finalWarmup := nextEmptySnapshot()
	assertBeforeStop(0, false) // Early warmup emptiness was suppressed too.
	close(finalWarmup.release)
	firstConfirmation := nextEmptySnapshot()
	assertBeforeStop(finalWarmup.snapshot.Revision, true)
	close(firstConfirmation.release)
	secondConfirmation := nextEmptySnapshot()
	assertBeforeStop(finalWarmup.snapshot.Revision, true)
	close(secondConfirmation.release)
	select {
	case err := <-stopResults:
		if err != nil {
			t.Fatalf("production context-aware stop caller did not consume terminal success: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("production context-aware stop caller did not finish")
	}
	select {
	case status := <-stopReceipts:
		if status != http.StatusAccepted {
			t.Fatal("actual Bot stop callback was not accepted")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("actual Bot stop callback did not return")
	}
	select {
	case leave := <-voice.leaves:
		if leave.err != nil || leave.streamID != "waiting" {
			t.Fatal("actual LeaveVoice did not finish for the started job")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("actual LeaveVoice was not called")
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, _, inFlight := manager.UI188VoiceStopState("waiting")
		if !inFlight {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("finished stop caller retained its in-flight request")
		}
		time.Sleep(5 * time.Millisecond)
	}
	assertCounts(2, false)
	attemptMu.Lock()
	stops := stopRequestCount
	attemptMu.Unlock()
	if stops != 1 || manager.CurrentStreamID() != "" || len(voice.leaves) != 0 || len(voice.joins) != 0 {
		t.Fatal("empty Voice start did not converge exactly once through normal stop")
	}
}
