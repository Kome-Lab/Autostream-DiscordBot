package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/example/autostream-discord-bot/internal/control"
	"github.com/example/autostream-discord-bot/internal/discord"
	botapi "github.com/example/autostream-discord-bot/internal/httpapi"
	"github.com/example/autostream-discord-bot/internal/jobs"
)

// TestMSRBotProcessHelper is an explicitly selected, synthetic test-binary
// driver. Its fake Gateway supplies observations only; the normal main DTO
// mapping, Manager, and ControlPanel HTTP adapter execute without a Provider.
func TestMSRBotProcessHelper(t *testing.T) {
	if os.Getenv("AUTOSTREAM_MSR_BOT_HELPER") != "1" {
		return
	}
	endpoint, err := url.Parse(os.Getenv("AUTOSTREAM_MSR_BOT_CP_URL"))
	ready := os.Getenv("AUTOSTREAM_MSR_BOT_READY")
	serviceID := os.Getenv("AUTOSTREAM_MSR_BOT_SERVICE_ID")
	if err != nil || endpoint.Scheme != "http" || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" || endpoint.Port() == "" ||
		(endpoint.Hostname() != "127.0.0.1" && endpoint.Hostname() != "::1") || !filepath.IsAbs(ready) || serviceID == "" {
		t.Fatal("explicit loopback synthetic process configuration is required")
	}
	voice := &msrProcessVoice{known: true, present: true}
	manager := jobs.NewManager(voice)
	manager.RequireAutoStartHoldObservations()
	voice.sink = manager
	starter := &msrProcessStarter{adapter: controlStreamStarter{client: control.Client{Config: control.Config{
		ControlPanelURL: endpoint.String(), Token: "synthetic-msr-bot-process", ServiceID: serviceID,
		ServiceName: "synthetic-msr-bot-process", ServicePublicURL: "https://synthetic.example.invalid",
	}}}}
	manager.SetStreamStarter(starter)
	product := botapi.NewServer("discord_bot", manager, botapi.TokenVerifier{PlainToken: "synthetic-msr-bot-process-read"})
	var config control.RuntimeConfig
	var stateMu sync.Mutex
	var shutdown sync.Once
	done := make(chan struct{})
	mux := http.NewServeMux()
	mux.HandleFunc("POST /snapshot", func(w http.ResponseWriter, r *http.Request) {
		var next control.RuntimeConfig
		if !msrProcessDecode(w, r, &next) {
			return
		}
		stateMu.Lock()
		config = next
		manager.SetStreamVoiceDefaults(streamDiscordDefaultsFromRuntimeConfig(config))
		stateMu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST /presence", func(w http.ResponseWriter, r *http.Request) {
		var next struct{ Known, Present bool }
		if !msrProcessDecode(w, r, &next) {
			return
		}
		voice.mu.Lock()
		voice.known, voice.present = next.Known, next.Present
		voice.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST /join", func(w http.ResponseWriter, r *http.Request) {
		var event struct {
			StreamID         string `json:"stream_id"`
			GuildID          string `json:"guild_id"`
			VoiceChannelID   string `json:"voice_channel_id"`
			UserID           string `json:"user_id"`
			AssignmentEpoch  string `json:"assignment_epoch"`
			ExistingPresence bool   `json:"existing_presence"`
		}
		if !msrProcessDecode(w, r, &event) {
			return
		}
		manager.VoiceUserJoined(discord.VoiceJoinEvent{GuildID: event.GuildID, VoiceChannelID: event.VoiceChannelID, UserID: event.UserID,
			ExistingPresence: event.ExistingPresence, Authority: &discord.AutoStartAuthority{StreamID: event.StreamID, AssignmentEpoch: event.AssignmentEpoch}})
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST /reconnect", func(w http.ResponseWriter, r *http.Request) {
		manager.DiscordDisconnected("synthetic-test-reconnect")
		stateMu.Lock()
		manager.SetStreamVoiceDefaults(streamDiscordDefaultsFromRuntimeConfig(config))
		stateMu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST /source-unavailable", func(w http.ResponseWriter, r *http.Request) {
		manager.SetAutoStartSourceAvailable(false)
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST /dispatch-block", func(w http.ResponseWriter, r *http.Request) {
		starter.mu.Lock()
		if starter.barrier != nil {
			starter.mu.Unlock()
			w.WriteHeader(http.StatusConflict)
			return
		}
		starter.barrier = make(chan struct{})
		starter.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST /dispatch-release", func(w http.ResponseWriter, r *http.Request) {
		starter.release()
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("GET /state", func(w http.ResponseWriter, r *http.Request) {
		starter.mu.Lock()
		last := starter.last
		starter.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"managed_calls": starter.managed.Load(), "legacy_calls": starter.legacy.Load(), "blocked_calls": starter.blocked.Load(), "http_dispatches": starter.dispatched.Load(), "inspect_calls": starter.inspected.Load(),
			"last_request": last, "condition": manager.ObserveAutoStartCondition(r.URL.Query().Get("stream_id")), "presence_condition": manager.ObserveAutoStartPresence(r.URL.Query().Get("stream_id")),
		})
	})
	mux.HandleFunc("POST /shutdown", func(w http.ResponseWriter, r *http.Request) {
		shutdown.Do(func() { close(done) })
		w.WriteHeader(http.StatusNoContent)
	})
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/streams/") &&
			(strings.HasSuffix(r.URL.Path, "/auto-start-presence") || strings.HasSuffix(r.URL.Path, "/auto-start-condition")) {
			product.ServeHTTP(w, r)
			return
		}
		if r.Header.Get("Authorization") != "Bearer synthetic-msr-helper-control" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		mux.ServeHTTP(w, r)
	}))
	server.Config.ReadHeaderTimeout = 3 * time.Second
	server.Config.ReadTimeout = 5 * time.Second
	server.Config.WriteTimeout = 5 * time.Second
	server.Start()
	defer server.Close()
	defer starter.release()
	data, err := json.Marshal(map[string]any{"url": server.URL, "pid": os.Getpid()})
	if err != nil || os.WriteFile(ready, data, 0o600) != nil {
		t.Fatal("synthetic process readiness could not be recorded")
	}
	select {
	case <-done:
	case <-time.After(45 * time.Second):
		t.Fatal("synthetic process driver timed out")
	}
}

func msrProcessDecode(w http.ResponseWriter, r *http.Request, out any) bool {
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if decoder.Decode(out) != nil {
		w.WriteHeader(http.StatusBadRequest)
		return false
	}
	var trailing any
	if decoder.Decode(&trailing) != io.EOF {
		w.WriteHeader(http.StatusBadRequest)
		return false
	}
	return true
}

type msrProcessVoice struct {
	discord.NoopClient
	mu      sync.RWMutex
	known   bool
	present bool
	targets []discord.AutoStartVoiceTarget
	sink    *jobs.Manager
}

func (v *msrProcessVoice) SetAutoStartVoiceTargets(targets []discord.AutoStartVoiceTarget) {
	v.mu.Lock()
	v.targets = append([]discord.AutoStartVoiceTarget(nil), targets...)
	present, known, sink := v.present, v.known, v.sink
	v.mu.Unlock()
	if present && known && sink != nil {
		for _, target := range targets {
			sink.VoiceUserJoined(discord.VoiceJoinEvent{GuildID: target.GuildID, VoiceChannelID: target.VoiceChannelID, UserID: "synthetic-human", ExistingPresence: true,
				Authority: &discord.AutoStartAuthority{StreamID: target.StreamID, AssignmentEpoch: target.AssignmentEpoch}})
		}
	}
}

func (v *msrProcessVoice) AutoStartVoicePresence(target discord.AutoStartVoiceTarget) (bool, bool) {
	v.mu.RLock()
	defer v.mu.RUnlock()
	for _, current := range v.targets {
		if current == target {
			return v.known && v.present, v.known
		}
	}
	return false, false
}

type msrProcessStarter struct {
	adapter    controlStreamStarter
	managed    atomic.Int32
	legacy     atomic.Int32
	blocked    atomic.Int32
	dispatched atomic.Int32
	inspected  atomic.Int32
	mu         sync.Mutex
	barrier    chan struct{}
	last       control.AutoStartRequest
}

func (s *msrProcessStarter) StartStream(streamID string) error {
	s.legacy.Add(1)
	s.dispatched.Add(1)
	return s.adapter.StartStream(streamID)
}

func (s *msrProcessStarter) StartManagedStream(streamID string, original control.AutoStartRequest) error {
	s.managed.Add(1)
	s.mu.Lock()
	s.last = original
	barrier := s.barrier
	s.mu.Unlock()
	if barrier != nil {
		s.blocked.Add(1)
		select {
		case <-barrier:
		case <-time.After(15 * time.Second):
			return control.ControlPanelError{StatusCode: http.StatusServiceUnavailable, Code: "synthetic_dispatch_barrier_timeout"}
		}
	}
	s.dispatched.Add(1)
	return s.adapter.StartManagedStream(streamID, original)
}

func (s *msrProcessStarter) InspectManagedStream(streamID, requestID string) (control.ManagedStartInspection, error) {
	s.inspected.Add(1)
	return s.adapter.InspectManagedStream(streamID, requestID)
}

func (s *msrProcessStarter) release() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.barrier != nil {
		close(s.barrier)
		s.barrier = nil
	}
}
