package audioforward

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	lifecycle "github.com/Kome-Lab/Autostream-Contracts/v3lifecycle"
)

type fixtureFrames struct {
	record lifecycle.Record
	calls  atomic.Int64
}

func (f *fixtureFrames) Prepare(context.Context, string, []OpusPacket) (lifecycle.Record, error) {
	f.calls.Add(1)
	return f.record, nil
}

// Explicit fixture bounds and witness names are test inputs, never runtime
// defaults or deployment qualification. Expected results below are literal.
func botLifecycleFixture() (LifecycleClientConfig, lifecycle.Record, []OpusPacket) {
	bot := lifecycle.ProducerRef{ComponentID: "C07", ServiceID: "bot-fixture", HostID: "bot-host", BootID: "bot-boot", ProcessIncarnation: "bot-process", WriterID: "opus-writer"}
	er := lifecycle.ProducerRef{ComponentID: "C04", ServiceID: "er-fixture", HostID: "er-host", BootID: "er-boot", ProcessIncarnation: "er-process", WriterID: "raw-writer"}
	botRun := lifecycle.ResourceRef{Kind: lifecycle.Run, OwnerID: bot.ServiceID, ResourceID: "stream-fixture"}
	raw := lifecycle.ResourceRef{Kind: lifecycle.Run, OwnerID: er.ServiceID, ResourceID: "stream-fixture"}
	queue := lifecycle.ResourceRef{Kind: lifecycle.Partition, OwnerID: er.ServiceID, ResourceID: "queue-fixture"}
	p := lifecycle.ComparisonProfile{Ref: lifecycle.ProfileRef{ProfileID: lifecycle.MediaIngress, Revision: 1}, ResourceKinds: []lifecycle.ResourceKind{lifecycle.Run, lifecycle.Partition}, Purposes: []lifecycle.Purpose{lifecycle.PurposeIngress}, TerminalPolicyRef: "fixture-terminal", LimitsRef: "fixture-limits", Limits: lifecycle.Limits{MaxIDBytes: 192, MaxMembers: 16, MaxAxes: 32, MaxRefs: 64, MaxObligations: 16, MaxPartitions: 8, MaxRanges: 16, MaxDepth: 16}}
	var vector lifecycle.RevisionVector
	for _, axis := range []lifecycle.Axis{lifecycle.Membership, lifecycle.Admission, lifecycle.Boot, lifecycle.Process, lifecycle.RunAxis, lifecycle.JobAxis, lifecycle.ConnectionAxis, lifecycle.Writer, lifecycle.Cover} {
		key := lifecycle.AxisKey{Axis: axis, OwnerID: er.ServiceID, Resource: raw}
		value := lifecycle.VersionedValue{Kind: lifecycle.PresentLiteral}
		switch axis {
		case lifecycle.Membership, lifecycle.JobAxis, lifecycle.ConnectionAxis, lifecycle.Writer, lifecycle.Cover:
			n := uint64(1)
			value = lifecycle.VersionedValue{Kind: lifecycle.PresentUint64, Uint64: &n}
		default:
			id := lifecycle.ID("fixture-" + string(axis))
			value.Literal = &id
		}
		p.AxisRules = append(p.AxisRules, lifecycle.AxisRule{Key: key, OwnerRole: "C04", ValueKind: value.Kind, Required: true, Comparison: "EXACT"})
		vector = append(vector, lifecycle.RevisionEntry{Key: key, Incarnation: "fixture-incarnation", Value: value, SourceFrameRef: "bot-frame"})
	}
	for _, sub := range []lifecycle.SubBoundary{lifecycle.RawWrite, lifecycle.Durability, lifecycle.QueueAdmission, lifecycle.QueueTerminal} {
		r := raw
		if sub == lifecycle.QueueAdmission || sub == lifecycle.QueueTerminal {
			r = queue
		}
		p.RequiredBoundaries = append(p.RequiredBoundaries, lifecycle.BoundaryAddress{Kind: lifecycle.LocalPersistence, OwnerID: er.ServiceID, Resource: r, SubBoundary: sub})
	}
	ref := lifecycle.ContractRef{ContractID: lifecycle.CC02, LogicalRevision: 1, SpecRef: lifecycle.SpecRef, ModuleCommitRef: lifecycleModuleCommit}
	record := lifecycle.Record{Header: lifecycle.Header{ContractRef: ref, RecordKey: lifecycle.RecordKey{ContractRef: ref, OwnerID: bot.ServiceID, RecordID: "request-vector"}, Correlation: lifecycle.Correlation{RequestID: "request-fixture", OperationID: "operation-fixture", AttemptID: "attempt-fixture", StepID: "step-fixture", ExecutionKey: "execution-fixture"}, DomainRef: lifecycle.DomainRef{DeploymentID: "fixture-deployment", Realm: "fixture-realm", DomainID: "fixture-domain", MembershipRevision: 1, SnapshotRef: "fixture-membership", Resources: []lifecycle.ResourceRef{botRun, raw, queue}}, ProducerRef: bot, RecordRevision: 1, ProfileRef: p.Ref}, Vector: &lifecycle.VectorRecord{Vector: vector}}
	frames := &fixtureFrames{record: record}
	config := LifecycleClientConfig{HTTP: &http.Client{}, Frames: frames, RequestProfile: p, InspectionProfile: p, Consumer: er, RawTarget: raw, QueueTarget: queue, QueueAdmissionRef: "admission-fixture", MaxResponseBytes: 65536}
	config.JobAxisKey = lifecycle.AxisKey{Axis: lifecycle.JobAxis, OwnerID: er.ServiceID, Resource: raw}
	config.ConnectionAxisKey = lifecycle.AxisKey{Axis: lifecycle.ConnectionAxis, OwnerID: er.ServiceID, Resource: raw}
	config.QueueWorkIDs = func(lifecycle.Correlation) (lifecycle.ID, lifecycle.ID) { return "fixture-work", "fixture-parent-work" }
	packets := []OpusPacket{{SSRC: 1, JobGeneration: 1, ConnectionGeneration: 1, Sequence: 1, Timestamp: 480, ReceivedAt: time.Date(2026, 11, 15, 0, 0, 0, 0, time.UTC), Opus: []byte{1, 2}}}
	return config, record, packets
}

func botInspection(config LifecycleClientConfig, request lifecycle.Record) lifecycle.Record {
	h := request.Header
	h.ContractRef.ContractID = lifecycle.CC08
	h.RecordKey = lifecycle.RecordKey{ContractRef: h.ContractRef, OwnerID: config.Consumer.ServiceID, RecordID: "response-record"}
	h.ProducerRef = config.Consumer
	ctx := &lifecycle.ObservationContext{OwnerID: config.Consumer.ServiceID, FrameRef: "er-frame", Scope: h.DomainRef.DomainID, SourceRevision: 1}
	coverage := lifecycle.ID("fixture-coverage")
	result := lifecycle.Record{Header: h, Inspection: &lifecycle.InspectionRecord{Target: lifecycle.QueryTarget{Correlation: request.Header.Correlation, BindingRef: request.Header.RecordKey.RecordID, ExecutionKey: request.Header.Correlation.ExecutionKey, TargetRecordRef: request.Header.RecordKey.RecordID, Scope: h.DomainRef.DomainID, Purpose: lifecycle.PurposeLookup}, Observations: lifecycle.ObservationSet{Gate: lifecycle.Held}, ObservedScopes: []lifecycle.ID{h.DomainRef.DomainID}, SourceFrames: []lifecycle.SourceFrame{{FrameRef: "er-frame", OwnerID: config.Consumer.ServiceID, Scope: h.DomainRef.DomainID, SourceRevision: 1}}, Coverage: lifecycle.Fact[lifecycle.ID]{State: lifecycle.Observed, Value: &coverage, Observation: ctx}}}
	for _, address := range config.InspectionProfile.RequiredBoundaries {
		outcome := lifecycle.SuccessAtBoundary
		observation := lifecycle.BoundaryObservation{Address: address, Knowledge: lifecycle.Known, Presence: lifecycle.Present, Outcome: lifecycle.Fact[lifecycle.Outcome]{State: lifecycle.Observed, Value: &outcome, Observation: ctx}}
		switch address.SubBoundary {
		case lifecycle.RawWrite:
			offset, length := uint64(0), uint64(2)
			observation.Details = &lifecycle.BoundaryDetails{RawExtent: &lifecycle.WriteExtent{ExtentRef: "fixture-extent", Offset: &offset, Length: &length, RequestedLength: &length}}
		case lifecycle.Durability:
			observation.Details = &lifecycle.BoundaryDetails{Durability: &lifecycle.DurabilityDetail{ProfileRef: config.InspectionProfile.Ref, Level: "SYNC_OBSERVED", WitnessRef: "fixture-sync"}}
		case lifecycle.QueueAdmission:
			observation.Details = &lifecycle.BoundaryDetails{Queue: &lifecycle.QueueDetail{Work: lifecycle.WorkRef{WorkID: "fixture-work", ParentWorkID: "fixture-parent-work", Namespace: lifecycle.Namespace{OwnerID: config.Consumer.ServiceID, Resource: config.QueueTarget, WorkKind: "MEDIA", ProducerIncarnation: config.Consumer.ProcessIncarnation, SequenceEpoch: "queue-epoch"}, Sequence: 1, AdmissionRecordRef: "admission-fixture"}, ReceiptRef: "fixture-queue-receipt"}}
		case lifecycle.QueueTerminal:
			observation.Knowledge = lifecycle.KnowledgeUnknown
			observation.Presence = lifecycle.NotObserved
			observation.Outcome = lifecycle.Fact[lifecycle.Outcome]{State: lifecycle.Unavailable, Unavailable: &lifecycle.UnavailableReason{Code: "TERMINAL_NOT_OBSERVED"}}
		}
		result.Inspection.Observations.Observations = append(result.Inspection.Observations.Observations, observation)
	}
	return result
}

func TestIUWS3BotTypedAdmission(t *testing.T) {
	config, frame, packets := botLifecycleFixture()
	var posts atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		posts.Add(1)
		if r.URL.Path != "/v3/streams/stream-fixture/audio/opus" || r.Header.Get("Authorization") != "Bearer fixture-token" {
			t.Errorf("wrong request target")
		}
		metadata, err := base64.RawURLEncoding.DecodeString(r.Header.Get(lifecycleHeader))
		if err != nil {
			t.Error(err)
		}
		var got lifecycle.Record
		if json.Unmarshal(metadata, &got) != nil || got.Header.Correlation != frame.Header.Correlation {
			t.Error("original tuple missing")
		}
		var body lifecycleIngestRequest
		decoder := json.NewDecoder(r.Body)
		decoder.DisallowUnknownFields()
		if decoder.Decode(&body) != nil || len(body.Packets) != 1 || body.Packets[0].OpusBase64 != "AQI=" || body.Packets[0].Sequence != 1 {
			t.Error("actual Opus payload missing")
		}
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(botInspection(config, frame))
	}))
	defer server.Close()
	client, err := NewLifecycleClient(config)
	if err != nil {
		t.Fatal(err)
	}
	out, err := client.Forward(t.Context(), server.URL, "stream-fixture", "bot-fixture", "fixture-token", packets)
	if err != nil || !out.QueueAdmitted || out.Assessment.Kind != lifecycle.Unknown || out.Assessment.ProductionQualified() || posts.Load() != 1 {
		t.Fatalf("local admission / terminal separation: %+v %v posts=%d", out.Assessment, err, posts.Load())
	}
}

func TestIUWS3BotRejectsMismatched2XX(t *testing.T) {
	for _, mutation := range []string{"body", "attempt", "owner", "incarnation", "raw", "queue", "missing-observation", "missing-frame", "wrong-revision", "wrong-admission", "unknown-coverage", "empty-write", "wrong-work"} {
		t.Run(mutation, func(t *testing.T) {
			config, frame, packets := botLifecycleFixture()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusAccepted)
				if mutation == "body" {
					_, _ = w.Write([]byte(`{"accepted":true}`))
					return
				}
				response := botInspection(config, frame)
				switch mutation {
				case "attempt":
					response.Inspection.Target.Correlation.AttemptID = "other-attempt"
				case "owner":
					response.Header.RecordKey.OwnerID = "other-owner"
				case "incarnation":
					response.Header.ProducerRef.ProcessIncarnation = "old-process"
				case "raw":
					response.Inspection.Observations.Observations[0].Address.Resource.ResourceID = "other-stream"
				case "queue":
					response.Inspection.Observations.Observations[2].Address.Resource.ResourceID = "other-queue"
				case "missing-observation":
					response.Inspection.Observations.Observations[0].Outcome.Observation = nil
				case "missing-frame":
					response.Inspection.SourceFrames = nil
				case "wrong-revision":
					response.Inspection.SourceFrames[0].SourceRevision = 2
				case "wrong-admission":
					response.Inspection.Observations.Observations[2].Details.Queue.Work.AdmissionRecordRef = "other-admission"
				case "wrong-work":
					response.Inspection.Observations.Observations[2].Details.Queue.Work.WorkID = "other-work"
				case "unknown-coverage":
					response.Inspection.Coverage = lifecycle.Fact[lifecycle.ID]{State: lifecycle.Unavailable, Unavailable: &lifecycle.UnavailableReason{Code: "COVERAGE_UNKNOWN"}}
				case "empty-write":
					zero := uint64(0)
					response.Inspection.Observations.Observations[0].Details.RawExtent.Length = &zero
					response.Inspection.Observations.Observations[0].Details.RawExtent.RequestedLength = &zero
				}
				_ = json.NewEncoder(w).Encode(response)
			}))
			defer server.Close()
			client, err := NewLifecycleClient(config)
			if err != nil {
				t.Fatal(err)
			}
			out, err := client.Forward(t.Context(), server.URL, "stream-fixture", "bot-fixture", "fixture-token", packets)
			if err == nil || out.QueueAdmitted {
				t.Fatalf("mismatched 2xx became success: %+v", out)
			}
		})
	}
}

func TestIUWS3BotACKLossInspectOriginalAttempt(t *testing.T) {
	t.Run("immutable_frame_snapshot", testBotImmutableFrameSnapshot)
	config, frame, packets := botLifecycleFixture()
	var dispatch, inspect atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v3/streams/stream-fixture/audio/opus/inspect" {
			inspect.Add(1)
			if r.ContentLength > 0 {
				t.Error("inspect replays payload")
			}
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(botInspection(config, frame))
			return
		}
		dispatch.Add(1)
		h := w.(http.Hijacker)
		connection, _, err := h.Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		_ = connection.Close()
	}))
	defer server.Close()
	client, err := NewLifecycleClient(config)
	if err != nil {
		t.Fatal(err)
	}
	out, err := client.Forward(t.Context(), server.URL, "stream-fixture", "bot-fixture", "fixture-token", packets)
	if err == nil || out.Assessment.Kind != lifecycle.Unknown || dispatch.Load() != 1 {
		t.Fatalf("ACK loss must be UNKNOWN, no retry: %+v %v", out.Assessment, err)
	}
	out, err = client.Inspect(t.Context(), server.URL, "stream-fixture", "fixture-token", out.Request)
	if err != nil || !out.QueueAdmitted || dispatch.Load() != 1 || inspect.Load() != 1 || config.Frames.(*fixtureFrames).calls.Load() != 1 {
		t.Fatalf("inspect reissued attempt: %+v %v", out.Assessment, err)
	}
}

func testBotImmutableFrameSnapshot(t *testing.T) {
	config, frame, packets := botLifecycleFixture()
	// Freeze the fixed test input independently of the candidate's clone helper.
	data, err := json.Marshal(frame)
	if err != nil {
		t.Fatal(err)
	}
	var baseline lifecycle.Record
	if err = json.Unmarshal(data, &baseline); err != nil {
		t.Fatal(err)
	}
	forwardEntered, inspectEntered := make(chan struct{}), make(chan struct{})
	forwardRelease, inspectRelease := make(chan struct{}), make(chan struct{})
	var forwardOnce, inspectOnce sync.Once
	releaseForward := func() { forwardOnce.Do(func() { close(forwardRelease) }) }
	releaseInspect := func() { inspectOnce.Do(func() { close(inspectRelease) }) }
	metadata := make(chan lifecycle.Record, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, decodeErr := base64.RawURLEncoding.DecodeString(r.Header.Get(lifecycleHeader))
		var received lifecycle.Record
		if decodeErr != nil || json.Unmarshal(raw, &received) != nil {
			t.Error("shared request metadata missing")
		}
		metadata <- received
		if r.URL.Path == "/v3/streams/stream-fixture/audio/opus/inspect" {
			close(inspectEntered)
			<-inspectRelease
		} else {
			close(forwardEntered)
			<-forwardRelease
		}
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(botInspection(config, baseline))
	}))
	defer server.Close()
	defer releaseForward()
	defer releaseInspect()
	client, err := NewLifecycleClient(config)
	if err != nil {
		t.Fatal(err)
	}
	type answer struct {
		observation ForwardObservation
		err         error
	}
	answers := make(chan answer, 1)
	go func() {
		out, callErr := client.Forward(t.Context(), server.URL, "stream-fixture", "bot-fixture", "fixture-token", packets)
		answers <- answer{out, callErr}
	}()
	select {
	case <-forwardEntered:
	case <-time.After(3 * time.Second):
		t.Fatal("forward did not reach HTTP barrier")
	}
	assertOriginal := func(record lifecycle.Record) {
		t.Helper()
		if record.Vector == nil || len(record.Header.DomainRef.Resources) == 0 || record.Header.DomainRef.Resources[0].ResourceID != "stream-fixture" {
			t.Fatal("retained attempt changed its original domain")
		}
		jobFound := false
		for _, entry := range record.Vector.Vector {
			if entry.Key == config.JobAxisKey && entry.Value.Uint64 != nil && *entry.Value.Uint64 == 1 {
				jobFound = true
			}
		}
		if !jobFound {
			t.Fatal("retained attempt changed its original generation")
		}
	}
	assertOriginal(<-metadata)
	mutate := func(record *lifecycle.Record) {
		record.Header.DomainRef.Resources[0].ResourceID = "mutated-source-resource"
		for i := range record.Vector.Vector {
			if record.Vector.Vector[i].Key == config.JobAxisKey {
				*record.Vector.Vector[i].Value.Uint64 = 99
			}
		}
	}
	// The owner changes reused slices only after the request is on the wire.
	// This must not change the returned original-attempt snapshot or its ACK.
	mutate(&config.Frames.(*fixtureFrames).record)
	releaseForward()
	var forwarded answer
	select {
	case forwarded = <-answers:
	case <-time.After(3 * time.Second):
		t.Fatal("forward did not finish")
	}
	if forwarded.err != nil || !forwarded.observation.QueueAdmitted {
		t.Fatalf("source alias changed ACK binding: %v", forwarded.err)
	}
	assertOriginal(forwarded.observation.Request)
	original := forwarded.observation.Request
	go func(input lifecycle.Record) {
		out, callErr := client.Inspect(t.Context(), server.URL, "stream-fixture", "fixture-token", input)
		answers <- answer{out, callErr}
	}(original)
	select {
	case <-inspectEntered:
	case <-time.After(3 * time.Second):
		t.Fatal("Inspect did not reach HTTP barrier")
	}
	assertOriginal(<-metadata)
	mutate(&original)
	releaseInspect()
	select {
	case inspected := <-answers:
		if inspected.err != nil || !inspected.observation.QueueAdmitted {
			t.Fatalf("caller alias changed Inspect binding: %v", inspected.err)
		}
		assertOriginal(inspected.observation.Request)
	case <-time.After(3 * time.Second):
		t.Fatal("Inspect did not finish")
	}
	if config.Frames.(*fixtureFrames).calls.Load() != 1 {
		t.Fatal("Inspect prepared a new attempt")
	}
}

func TestIUWS3BotStaleGenerationHasNoDispatch(t *testing.T) {
	config, _, packets := botLifecycleFixture()
	packets[0].ConnectionGeneration = 2
	var dispatch atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { dispatch.Add(1) }))
	defer server.Close()
	client, err := NewLifecycleClient(config)
	if err != nil {
		t.Fatal(err)
	}
	out, err := client.Forward(t.Context(), server.URL, "stream-fixture", "bot-fixture", "fixture-token", packets)
	if err == nil || out.Assessment.Kind != lifecycle.Stale || dispatch.Load() != 0 {
		t.Fatalf("stale packet dispatched: %+v %v", out.Assessment, err)
	}
}

func TestIUWS3BotRequiresExplicitProfile(t *testing.T) {
	config, _, _ := botLifecycleFixture()
	config.RequestProfile.Limits = lifecycle.Limits{}
	if _, err := NewLifecycleClient(config); err == nil {
		t.Fatal("missing limits silently defaulted")
	}
}

func TestIUWS3BotNoRedispatch(t *testing.T) {
	config, frame, packets := botLifecycleFixture()
	var count atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count.Add(1)
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(botInspection(config, frame))
	}))
	defer server.Close()
	client, err := NewLifecycleClient(config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = client.Forward(t.Context(), server.URL, "stream-fixture", "bot-fixture", "fixture-token", packets); err != nil {
		t.Fatal(err)
	}
	out, err := client.Forward(t.Context(), server.URL, "stream-fixture", "bot-fixture", "fixture-token", packets)
	if err == nil || out.Assessment.Kind != lifecycle.Unknown || count.Load() != 1 {
		t.Fatal("same attempt was dispatched again")
	}
}
