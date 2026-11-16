package jobs

import (
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/example/autostream-discord-bot/internal/control"
	"github.com/example/autostream-discord-bot/internal/discord"
)

type managedHoldStarter struct {
	mu        sync.Mutex
	legacy    int
	requests  []control.AutoStartRequest
	sent      chan control.AutoStartRequest
	err       error
	inspected []string
}

func (s *managedHoldStarter) StartStream(string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.legacy++
	return errors.New("managed work used legacy start")
}

func (s *managedHoldStarter) StartManagedStream(_ string, request control.AutoStartRequest) error {
	s.mu.Lock()
	s.requests = append(s.requests, request)
	err := s.err
	s.mu.Unlock()
	if s.sent != nil {
		s.sent <- request
	}
	return err
}

func (s *managedHoldStarter) InspectManagedStream(_ string, requestID string) (control.ManagedStartInspection, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.inspected = append(s.inspected, requestID)
	return control.ManagedStartInspection{}, errors.New("synthetic original attempt remains unknown")
}

func managedHoldDefaults(held bool) VoiceDefaults {
	return VoiceDefaults{AssignmentEpoch: "assignment-1", GuildID: "guild", VoiceChannelID: "voice", AutoStartEnabled: true,
		AutoStartHoldRequired: true, AutoStartHoldKnown: true, AutoStartHold: held, AutoStartHoldRevision: 4,
		ExpectedStreamRevision: "stream-revision-7", ExpectedSettingsRevision: "settings-revision-3", ExpectedRunID: "run-2"}
}

func managedHoldJoin() discord.VoiceJoinEvent {
	return discord.VoiceJoinEvent{GuildID: "guild", VoiceChannelID: "voice", UserID: "human",
		Authority: &discord.AutoStartAuthority{StreamID: "stream", AssignmentEpoch: "assignment-1"}}
}

func TestMSRBotHoldSurvivesJoinRestartAndAuthorityChanges(t *testing.T) {
	for _, change := range []string{"same", "restart", "new epoch", "new channel", "unknown", "unavailable", "downgrade"} {
		t.Run(change, func(t *testing.T) {
			voice := &autoStartPresenceVoice{present: true, known: true}
			manager := NewManager(voice)
			starter := &managedHoldStarter{sent: make(chan control.AutoStartRequest, 2)}
			manager.SetStreamStarter(starter)
			defaults := managedHoldDefaults(true)
			manager.SetStreamVoiceDefaults(map[string]VoiceDefaults{"stream": defaults})
			event := managedHoldJoin()
			switch change {
			case "restart":
				manager = NewManager(voice)
				manager.SetStreamStarter(starter)
			case "new epoch":
				defaults.AssignmentEpoch = "assignment-2"
				event.Authority.AssignmentEpoch = defaults.AssignmentEpoch
			case "new channel":
				defaults.VoiceChannelID = "other"
				event.VoiceChannelID = "other"
			case "unknown":
				defaults.AutoStartHoldKnown = false
				defaults.AutoStartHold = false
			case "downgrade":
				defaults.AutoStartHoldRequired, defaults.AutoStartHoldKnown, defaults.AutoStartHold = false, false, false
			}
			manager.SetStreamVoiceDefaults(map[string]VoiceDefaults{"stream": defaults})
			if change == "unavailable" {
				manager.SetAutoStartSourceAvailable(false)
			}
			for _, existing := range []bool{true, false} {
				event.ExistingPresence = existing
				manager.VoiceUserJoined(event)
			}
			waitAutoStartIdle(t, manager, "stream")
			starter.mu.Lock()
			defer starter.mu.Unlock()
			if starter.legacy != 0 || len(starter.requests) != 0 {
				t.Fatal("join, restart or changed authority bypassed CP-owned hold")
			}
		})
	}
}

func TestMSRBotManagedUnknownNeverRetriesOrReissues(t *testing.T) {
	manager := NewManager(&autoStartPresenceVoice{present: true, known: true})
	manager.autoStartCooldown = 0
	manager.autoStartRetryDelays = []time.Duration{time.Millisecond}
	manager.SetStreamVoiceDefaults(map[string]VoiceDefaults{"stream": managedHoldDefaults(false)})
	starter := &managedHoldStarter{sent: make(chan control.AutoStartRequest, 4), err: control.ControlPanelError{StatusCode: http.StatusServiceUnavailable}}
	manager.SetStreamStarter(starter)
	manager.VoiceUserJoined(managedHoldJoin())
	select {
	case request := <-starter.sent:
		if !request.Valid() || request.ExpectedHoldRevision != 4 || request.ExpectedRunID != "run-2" {
			t.Fatal("original owner observation was not transmitted")
		}
	case <-time.After(time.Second):
		t.Fatal("managed request did not reach explicit sender")
	}
	waitAutoStartIdle(t, manager, "stream")
	manager.VoiceUserJoined(managedHoldJoin())
	waitAutoStartIdle(t, manager, "stream")
	starter.mu.Lock()
	defer starter.mu.Unlock()
	if starter.legacy != 0 || len(starter.requests) != 1 {
		t.Fatalf("UNKNOWN reissued work: legacy=%d managed=%d", starter.legacy, len(starter.requests))
	}
	if len(starter.inspected) != 1 || starter.inspected[0] != starter.requests[0].RequestID {
		t.Fatal("ACK loss did not inspect exactly the same original attempt")
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if !manager.autoStartSourceUnavailable {
		t.Fatal("pure Inspect promoted an unknown source into current success")
	}
}

func TestMSRBotRequestCorrelationStableAcrossRestart(t *testing.T) {
	first := managedAutoStartRequest("stream", managedHoldDefaults(false))
	second := managedAutoStartRequest("stream", managedHoldDefaults(false))
	if first != second || !first.Valid() {
		t.Fatal("same immutable source frame acquired a different attempt after restart")
	}
	changed := managedHoldDefaults(false)
	changed.AutoStartHoldRevision++
	if managedAutoStartRequest("stream", changed).RequestID == first.RequestID {
		t.Fatal("new hold revision reused the old original request")
	}
	changed = managedHoldDefaults(false)
	changed.ExpectedSettingsRevision = "different-settings"
	if managedAutoStartRequest("stream", changed).RequestID == first.RequestID {
		t.Fatal("different immutable content reused request identity")
	}
}

func TestMSRBotConditionReadIsPureAndRejectsChangedFrame(t *testing.T) {
	voice := &autoStartPresenceVoice{present: true, known: true}
	manager := NewManager(voice)
	defaults := managedHoldDefaults(true)
	manager.SetStreamVoiceDefaults(map[string]VoiceDefaults{"stream": defaults})
	condition := manager.ObserveAutoStartCondition("stream")
	if !condition.Known || !condition.Present || condition.HoldRevision != 4 {
		t.Fatal("explicit condition lookup could not observe held target presence")
	}
	manager.mu.Lock()
	if manager.streamDefaults["stream"] != defaults || len(manager.autoStartPending) != 0 || len(manager.managedAutoStartAttempts) != 0 {
		t.Fatal("pure condition read mutated hold/start state")
	}
	manager.mu.Unlock()
	defaults.AutoStartHoldKnown = false
	defaults.AutoStartHoldRevision++
	manager.SetStreamVoiceDefaults(map[string]VoiceDefaults{"stream": defaults})
	if strict := manager.ObserveAutoStartCondition("stream"); strict.Known {
		t.Fatal("pending Hold was promoted into a known condition observation")
	}
	if presence := manager.ObserveAutoStartPresence("stream"); !presence.Known || !presence.Present || presence.HoldRevision != defaults.AutoStartHoldRevision {
		t.Fatal("known current SDK presence was confused with pending Hold authority")
	}
	voice.presenceMu.Lock()
	voice.read, voice.release = make(chan struct{}, 1), make(chan struct{})
	voice.presenceMu.Unlock()
	pureResult := make(chan AutoStartCondition, 1)
	go func() { pureResult <- manager.ObserveAutoStartPresence("stream") }()
	<-voice.read
	defaults.AutoStartHoldRevision++
	manager.SetStreamVoiceDefaults(map[string]VoiceDefaults{"stream": defaults})
	close(voice.release)
	if presence := <-pureResult; !presence.Known || !presence.Present || presence.HoldRevision != defaults.AutoStartHoldRevision {
		t.Fatal("Hold-only refresh invalidated or mutated the independent presence frame")
	}
	defaults.AutoStartHoldKnown = true
	manager.SetStreamVoiceDefaults(map[string]VoiceDefaults{"stream": defaults})
	voice.presenceMu.Lock()
	voice.read, voice.release = nil, nil
	voice.present = false
	voice.presenceMu.Unlock()
	if empty := manager.ObserveAutoStartCondition("stream"); !empty.Known || empty.Present {
		t.Fatal("known absent presence was reported as unknown or present")
	}
	voice.presenceMu.Lock()
	voice.known = false
	voice.presenceMu.Unlock()
	if missing := manager.ObserveAutoStartCondition("stream"); missing.Known || missing.Present {
		t.Fatal("unavailable Gateway view was converted into known absence")
	}
	if missing := manager.ObserveAutoStartPresence("stream"); missing.Known || missing.Present {
		t.Fatal("pure presence reused an unavailable SDK observation")
	}
	voice.presenceMu.Lock()
	voice.present, voice.known = true, true
	voice.read, voice.release = make(chan struct{}, 2), make(chan struct{})
	voice.presenceMu.Unlock()
	result := make(chan AutoStartCondition, 1)
	changedPresence := make(chan AutoStartCondition, 1)
	go func() { result <- manager.ObserveAutoStartCondition("stream") }()
	go func() { changedPresence <- manager.ObserveAutoStartPresence("stream") }()
	<-voice.read
	<-voice.read
	defaults.ExpectedSettingsRevision = "changed-before-confirmation"
	manager.SetStreamVoiceDefaults(map[string]VoiceDefaults{"stream": defaults})
	close(voice.release)
	if (<-result).Known {
		t.Fatal("source changed during presence read but old frame was returned as known")
	}
	if (<-changedPresence).Known {
		t.Fatal("pure presence accepted changed Stream/settings/Run source")
	}
}

func TestMSRBotManualStartDoesNotReleaseHold(t *testing.T) {
	manager := NewManager(&fakeVoice{})
	defaults := managedHoldDefaults(true)
	manager.SetStreamVoiceDefaults(map[string]VoiceDefaults{"stream": defaults})
	if err := manager.Start(discord.VoiceJob{StreamID: "stream", GuildID: "guild", VoiceChannelID: "voice", JobGeneration: 1}); err != nil {
		t.Fatal(err)
	}
	defer manager.Stop("stream")
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if !manager.streamDefaults["stream"].AutoStartHold || manager.streamDefaults["stream"].AutoStartHoldRevision != 4 {
		t.Fatal("manual Start cleared or changed human hold")
	}
}

func TestMSRBotMissingAndRolledBackHoldCannotEnableAutoStart(t *testing.T) {
	for _, change := range []string{"missing", "older revision", "same revision different state"} {
		t.Run(change, func(t *testing.T) {
			manager := NewManager(&autoStartPresenceVoice{present: true, known: true})
			manager.RequireAutoStartHoldObservations()
			starter := &managedHoldStarter{sent: make(chan control.AutoStartRequest, 1)}
			manager.SetStreamStarter(starter)
			current := managedHoldDefaults(true)
			manager.SetStreamVoiceDefaults(map[string]VoiceDefaults{"stream": current})
			switch change {
			case "missing":
				current.AutoStartHoldRequired, current.AutoStartHoldKnown, current.AutoStartHold = false, false, false
			case "older revision":
				current.AutoStartHoldRevision--
				current.AutoStartHold = false
			case "same revision different state":
				current.AutoStartHold = false
			}
			manager.SetStreamVoiceDefaults(map[string]VoiceDefaults{"stream": current})
			manager.VoiceUserJoined(managedHoldJoin())
			waitAutoStartIdle(t, manager, "stream")
			starter.mu.Lock()
			defer starter.mu.Unlock()
			if len(starter.requests) != 0 || starter.legacy != 0 {
				t.Fatal("unobserved/rolled-back hold was treated as unheld")
			}
		})
	}
}
