package audioforward

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"time"

	lifecycle "github.com/Kome-Lab/Autostream-Contracts/v3lifecycle"
)

const lifecycleModuleCommit lifecycle.ID = "d32ea3514b1c1010391caae0348b0ff9e751f447"
const lifecycleHeader = "X-Autostream-Lifecycle"
const lifecycleHeaderBytes = 32768

// The v3 opt-in route retains ER's existing raw packet payload. Generation
// metadata belongs in the shared CC02 header and is checked before dispatch;
// it is not added as an unknown field to the strict legacy packet decoder.
type lifecyclePacketWire struct {
	SSRC       uint32    `json:"ssrc"`
	UserID     string    `json:"user_id,omitempty"`
	Sequence   uint16    `json:"sequence"`
	Timestamp  uint32    `json:"timestamp"`
	ReceivedAt time.Time `json:"received_at"`
	OpusBase64 string    `json:"opus_base64"`
}
type lifecycleIngestRequest struct {
	StreamID string                `json:"stream_id"`
	Source   string                `json:"source"`
	Packets  []lifecyclePacketWire `json:"packets"`
}

// LifecycleFrameSource supplies a previously bound attempt from the product's
// trusted participant. It does not issue a permit or qualify a deployment.
// A caller must retain the returned frame to inspect an uncertain dispatch.
type LifecycleFrameSource interface {
	Prepare(context.Context, string, []OpusPacket) (lifecycle.Record, error)
}

type LifecycleClientConfig struct {
	HTTP              *http.Client
	Frames            LifecycleFrameSource
	RequestProfile    lifecycle.ComparisonProfile
	InspectionProfile lifecycle.ComparisonProfile
	Consumer          lifecycle.ProducerRef
	RawTarget         lifecycle.ResourceRef
	QueueTarget       lifecycle.ResourceRef
	QueueAdmissionRef lifecycle.ID
	JobAxisKey        lifecycle.AxisKey
	ConnectionAxisKey lifecycle.AxisKey
	QueueWorkIDs      func(lifecycle.Correlation) (lifecycle.ID, lifecycle.ID)
	MaxResponseBytes  int64
}

// LifecycleClient is an explicit, opt-in internal conformance transport. The
// ordinary v2 client and startup configuration remain separate. A coherent
// record is never an execution authority, and this client never retries POSTs.
type LifecycleClient struct {
	config     LifecycleClientConfig
	mu         sync.Mutex
	dispatched map[lifecycle.RecordKey]lifecycle.Correlation
	attempts   map[lifecycle.Correlation]bool
}

type ForwardObservation struct {
	Request            lifecycle.Record
	Inspection         lifecycle.Record
	Assessment         lifecycle.Result
	ResponseValidation lifecycle.Result
	QueueAdmitted      bool
}

type LifecycleError struct{ Result lifecycle.Result }

func (e *LifecycleError) Error() string { return "lifecycle audio outcome: " + string(e.Result.Kind) }

func clientResult(kind lifecycle.ResultKind, code string) lifecycle.Result {
	return lifecycle.Result{Kind: kind, Issues: []lifecycle.Issue{{Code: code, Path: "AudioForward"}}}
}

func NewLifecycleClient(config LifecycleClientConfig) (*LifecycleClient, error) {
	if config.HTTP == nil || config.Frames == nil || config.QueueWorkIDs == nil || config.MaxResponseBytes <= 0 || config.MaxResponseBytes > 1<<20 || config.Consumer.ComponentID != "C04" || config.Consumer.ServiceID == "" || config.RawTarget.OwnerID != config.Consumer.ServiceID || config.RawTarget.Kind != lifecycle.Run || config.RawTarget.ResourceID == "" || config.QueueTarget.OwnerID != config.Consumer.ServiceID || config.QueueTarget.Kind != lifecycle.Partition || config.QueueTarget.ResourceID == "" || config.QueueAdmissionRef == "" {
		return nil, &LifecycleError{clientResult(lifecycle.NeedsEvidence, "EXPLICIT_CLIENT_CONFIGURATION_REQUIRED")}
	}
	if r := lifecycle.ValidateProfile(config.RequestProfile); r.Kind != lifecycle.StructurallyCoherent {
		return nil, &LifecycleError{r}
	}
	if r := lifecycle.ValidateProfile(config.InspectionProfile); r.Kind != lifecycle.StructurallyCoherent {
		return nil, &LifecycleError{r}
	}
	if config.RequestProfile.Ref.ProfileID != lifecycle.MediaIngress {
		return nil, &LifecycleError{clientResult(lifecycle.NeedsDecision, "MEDIA_INGRESS_PROFILE_REQUIRED")}
	}
	jobBound, connectionBound := false, false
	for _, rule := range config.RequestProfile.AxisRules {
		if rule.Key == config.JobAxisKey && rule.Key.Axis == lifecycle.JobAxis && rule.Required && rule.ValueKind == lifecycle.PresentUint64 {
			jobBound = true
		}
		if rule.Key == config.ConnectionAxisKey && rule.Key.Axis == lifecycle.ConnectionAxis && rule.Required && rule.ValueKind == lifecycle.PresentUint64 {
			connectionBound = true
		}
	}
	if !jobBound || !connectionBound {
		return nil, &LifecycleError{clientResult(lifecycle.NeedsDecision, "EXPLICIT_RESOURCE_BOUND_GENERATION_KEYS_REQUIRED")}
	}
	// Freeze the explicit profile slices; callers cannot mutate an installed
	// comparison policy concurrently with a dispatch.
	config.RequestProfile = cloneProfile(config.RequestProfile)
	config.InspectionProfile = cloneProfile(config.InspectionProfile)
	client := *config.HTTP
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	config.HTTP = &client
	return &LifecycleClient{config: config, dispatched: map[lifecycle.RecordKey]lifecycle.Correlation{}, attempts: map[lifecycle.Correlation]bool{}}, nil
}

func cloneProfile(profile lifecycle.ComparisonProfile) lifecycle.ComparisonProfile {
	profile.AxisRules = append([]lifecycle.AxisRule(nil), profile.AxisRules...)
	profile.ResourceKinds = append([]lifecycle.ResourceKind(nil), profile.ResourceKinds...)
	profile.Purposes = append([]lifecycle.Purpose(nil), profile.Purposes...)
	profile.RequiredBoundaries = append([]lifecycle.BoundaryAddress(nil), profile.RequiredBoundaries...)
	return profile
}

// ForwardOpus connects the lifecycle client to Discord's existing AudioForwarder
// interface. nil means the named local queue admission was observed, not that
// a provider, codec, archive, or remote terminal operation succeeded.
func (c *LifecycleClient) ForwardOpus(ctx context.Context, endpoint, streamID, source, token string, packets []OpusPacket) error {
	_, err := c.Forward(ctx, endpoint, streamID, source, token, packets)
	return err
}

func (c *LifecycleClient) Forward(ctx context.Context, endpoint, streamID, source, token string, packets []OpusPacket) (ForwardObservation, error) {
	if c == nil || c.config.Frames == nil {
		return ForwardObservation{}, &LifecycleError{clientResult(lifecycle.NeedsEvidence, "PARTICIPANT_REQUIRED")}
	}
	if err := ctx.Err(); err != nil {
		return ForwardObservation{}, err
	}
	frame, err := c.config.Frames.Prepare(ctx, streamID, packets)
	if err != nil {
		return ForwardObservation{}, &LifecycleError{clientResult(lifecycle.NeedsEvidence, "BOUND_ATTEMPT_UNAVAILABLE")}
	}
	if r := c.validateFrame(frame, streamID, packets); r.Kind != lifecycle.StructurallyCoherent {
		return ForwardObservation{Request: frame, Assessment: r}, &LifecycleError{r}
	}
	if source != string(frame.Header.ProducerRef.ServiceID) || strings.TrimSpace(token) == "" || len(packets) == 0 {
		r := clientResult(lifecycle.RefusedUnchanged, "EXPLICIT_PAYLOAD_FIELDS_REQUIRED")
		return ForwardObservation{Request: frame, Assessment: r}, &LifecycleError{r}
	}
	wire := lifecycleIngestRequest{StreamID: streamID, Source: source}
	for _, packet := range packets {
		if len(packet.Opus) == 0 || packet.ReceivedAt.IsZero() {
			r := clientResult(lifecycle.RefusedUnchanged, "PACKET_CONTENT_OR_TIME_MISSING")
			return ForwardObservation{Request: frame, Assessment: r}, &LifecycleError{r}
		}
		wire.Packets = append(wire.Packets, lifecyclePacketWire{SSRC: packet.SSRC, UserID: packet.UserID, Sequence: packet.Sequence, Timestamp: packet.Timestamp, ReceivedAt: packet.ReceivedAt, OpusBase64: base64.StdEncoding.EncodeToString(packet.Opus)})
	}
	body, err := json.Marshal(wire)
	if err != nil {
		return ForwardObservation{}, err
	}
	return c.exchange(ctx, endpoint, streamID, token, frame, body, false)
}

// Inspect uses the original immutable request, without preparing a new attempt,
// replaying Opus, or releasing a HELD state.
func (c *LifecycleClient) Inspect(ctx context.Context, endpoint, streamID, token string, original lifecycle.Record) (ForwardObservation, error) {
	if c == nil {
		return ForwardObservation{}, &LifecycleError{clientResult(lifecycle.NeedsEvidence, "PARTICIPANT_REQUIRED")}
	}
	if r := c.validateFrame(original, streamID, nil); r.Kind != lifecycle.StructurallyCoherent {
		return ForwardObservation{Request: original, Assessment: r}, &LifecycleError{r}
	}
	return c.exchange(ctx, endpoint, streamID, token, original, nil, true)
}

func (c *LifecycleClient) validateFrame(frame lifecycle.Record, streamID string, packets []OpusPacket) lifecycle.Result {
	if frame.Header.ContractRef.ContractID != lifecycle.CC02 || frame.Header.ContractRef.ModuleCommitRef != lifecycleModuleCommit || frame.Vector == nil || frame.Header.ProducerRef.ComponentID != "C07" {
		return clientResult(lifecycle.RefusedUnchanged, "PINNED_PRODUCER_VECTOR_REQUIRED")
	}
	if r := lifecycle.ValidateRecord(frame, c.config.RequestProfile); r.Kind != lifecycle.StructurallyCoherent {
		return r
	}
	child, parent := c.config.QueueWorkIDs(frame.Header.Correlation)
	if lifecycle.ValidateID(child, c.config.RequestProfile.Limits).Kind != lifecycle.StructurallyCoherent || lifecycle.ValidateID(parent, c.config.RequestProfile.Limits).Kind != lifecycle.StructurallyCoherent || child == parent {
		return clientResult(lifecycle.NeedsEvidence, "TRUSTED_QUEUE_WORK_MAPPING_REQUIRED")
	}
	streamBound := false
	var job, connection *uint64
	for _, entry := range frame.Vector.Vector {
		if entry.Key.Resource == c.config.RawTarget && entry.Key.Resource.ResourceID == lifecycle.ID(streamID) {
			streamBound = true
		}
		if entry.Key == c.config.JobAxisKey {
			job = entry.Value.Uint64
		}
		if entry.Key == c.config.ConnectionAxisKey {
			connection = entry.Value.Uint64
		}
	}
	if !streamBound || job == nil || connection == nil {
		return clientResult(lifecycle.NeedsEvidence, "STREAM_JOB_CONNECTION_BINDING_REQUIRED")
	}
	for _, packet := range packets {
		if packet.JobGeneration != *job || packet.ConnectionGeneration != *connection {
			return clientResult(lifecycle.Stale, "PACKET_GENERATION_MISMATCH")
		}
	}
	return lifecycle.Result{Kind: lifecycle.StructurallyCoherent}
}

func (c *LifecycleClient) exchange(ctx context.Context, base, streamID, token string, frame lifecycle.Record, body []byte, inspect bool) (ForwardObservation, error) {
	out := ForwardObservation{Request: frame}
	endpoint, err := lifecycleEndpoint(base, streamID, inspect)
	if err != nil || strings.TrimSpace(token) == "" {
		return out, &LifecycleError{clientResult(lifecycle.RefusedUnchanged, "INVALID_TARGET_OR_TOKEN")}
	}
	metadata, err := json.Marshal(frame)
	if err != nil {
		return out, err
	}
	encoded := base64.RawURLEncoding.EncodeToString(metadata)
	if len(encoded) > lifecycleHeaderBytes {
		return out, &LifecycleError{clientResult(lifecycle.RefusedUnchanged, "METADATA_LIMIT_EXCEEDED")}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return out, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(lifecycleHeader, encoded)
	if !inspect {
		c.mu.Lock()
		_, seenRecord := c.dispatched[frame.Header.RecordKey]
		seenAttempt := c.attempts[frame.Header.Correlation]
		if seenRecord || seenAttempt {
			c.mu.Unlock()
			out.Assessment = clientResult(lifecycle.Unknown, "ATTEMPT_ALREADY_DISPATCHED_INSPECT_ORIGINAL_ATTEMPT")
			return out, &LifecycleError{out.Assessment}
		}
		if len(c.dispatched) >= c.config.RequestProfile.Limits.MaxRefs {
			c.mu.Unlock()
			out.Assessment = clientResult(lifecycle.RefusedUnchanged, "LOCAL_ATTEMPT_RETENTION_LIMIT_REACHED")
			return out, &LifecycleError{out.Assessment}
		}
		c.dispatched[frame.Header.RecordKey] = frame.Header.Correlation
		c.attempts[frame.Header.Correlation] = true
		c.mu.Unlock()
	}
	response, err := c.config.HTTP.Do(req)
	if err != nil {
		out.Assessment = clientResult(lifecycle.Unknown, "DISPATCH_RESULT_UNKNOWN_INSPECT_ORIGINAL_ATTEMPT")
		return out, &LifecycleError{out.Assessment}
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, c.config.MaxResponseBytes+1))
	if err != nil || int64(len(data)) > c.config.MaxResponseBytes {
		out.Assessment = clientResult(lifecycle.Unknown, "RESPONSE_UNAVAILABLE_INSPECT_ORIGINAL_ATTEMPT")
		return out, &LifecycleError{out.Assessment}
	}
	if response.StatusCode != http.StatusAccepted {
		// A bare error Result carries no original-attempt binding. Its status
		// cannot prove that the remote side made no change before responding.
		out.Assessment = clientResult(lifecycle.Unknown, "UNBOUND_ERROR_RESPONSE_INSPECT_ORIGINAL_ATTEMPT")
		return out, &LifecycleError{out.Assessment}
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&out.Inspection) != nil || decoder.Decode(new(any)) != io.EOF {
		out.Assessment = clientResult(lifecycle.Unknown, "UNVERIFIED_2XX_RESPONSE")
		return out, &LifecycleError{out.Assessment}
	}
	out.ResponseValidation = c.validateInspection(frame, out.Inspection)
	out.Assessment = out.ResponseValidation
	if out.ResponseValidation.Kind == lifecycle.RefusedUnchanged || out.ResponseValidation.Kind == lifecycle.Conflict || out.ResponseValidation.Kind == lifecycle.NeedsDecision || out.ResponseValidation.Kind == lifecycle.Stale {
		out.Assessment = clientResult(lifecycle.Unknown, "INVALID_REMOTE_RECORD_INSPECT_ORIGINAL_ATTEMPT")
		return out, &LifecycleError{out.Assessment}
	}
	out.QueueAdmitted = c.knownLocalAdmission(out.Inspection)
	if !out.QueueAdmitted {
		out.Assessment = clientResult(lifecycle.Unknown, "LOCAL_ADMISSION_NOT_PROVEN_INSPECT_ORIGINAL_ATTEMPT")
		return out, &LifecycleError{out.Assessment}
	}
	return out, nil
}

func (c *LifecycleClient) validateInspection(request, response lifecycle.Record) lifecycle.Result {
	if response.Header.ContractRef.ContractID != lifecycle.CC08 || response.Header.ContractRef.ModuleCommitRef != lifecycleModuleCommit || response.Inspection == nil || response.Header.Correlation != request.Header.Correlation || response.Header.ProducerRef != c.config.Consumer || response.Header.RecordKey.OwnerID != c.config.Consumer.ServiceID || !reflect.DeepEqual(response.Header.DomainRef, request.Header.DomainRef) {
		return clientResult(lifecycle.Conflict, "RESPONSE_BINDING_MISMATCH")
	}
	target := response.Inspection.Target
	if target.Correlation != request.Header.Correlation || target.ExecutionKey != request.Header.Correlation.ExecutionKey || target.TargetRecordRef != request.Header.RecordKey.RecordID || target.BindingRef != request.Header.RecordKey.RecordID || target.Scope != request.Header.DomainRef.DomainID || target.Purpose != lifecycle.PurposeLookup {
		return clientResult(lifecycle.Conflict, "ORIGINAL_ATTEMPT_MISMATCH")
	}
	return lifecycle.ValidateRecord(response, c.config.InspectionProfile)
}

func (c *LifecycleClient) knownLocalAdmission(record lifecycle.Record) bool {
	if record.Inspection == nil {
		return false
	}
	child, parent := c.config.QueueWorkIDs(record.Inspection.Target.Correlation)
	coverage := record.Inspection.Coverage
	if lifecycle.ValidateFact(coverage, c.config.InspectionProfile.Limits).Kind != lifecycle.StructurallyCoherent || !c.observedFrame(record, coverage.Observation) {
		return false
	}
	seen := map[lifecycle.SubBoundary]bool{}
	for _, observation := range record.Inspection.Observations.Observations {
		if observation.Address.Kind != lifecycle.LocalPersistence || observation.Address.OwnerID != c.config.Consumer.ServiceID {
			continue
		}
		want := c.config.RawTarget
		if observation.Address.SubBoundary == lifecycle.QueueAdmission || observation.Address.SubBoundary == lifecycle.QueueTerminal {
			want = c.config.QueueTarget
		}
		if observation.Address.Resource != want {
			continue
		}
		if result := lifecycle.ValidateBoundaryObservation(observation, c.config.InspectionProfile.Limits); result.Kind != lifecycle.StructurallyCoherent {
			continue
		}
		if !c.observedFrame(record, observation.Outcome.Observation) {
			continue
		}
		switch observation.Address.SubBoundary {
		case lifecycle.RawWrite:
			if observation.Details == nil || observation.Details.RawExtent == nil || *observation.Details.RawExtent.Length != *observation.Details.RawExtent.RequestedLength || *observation.Details.RawExtent.RequestedLength == 0 {
				continue
			}
		case lifecycle.Durability:
			if observation.Details == nil || observation.Details.Durability == nil || observation.Details.Durability.ProfileRef != c.config.InspectionProfile.Ref || observation.Details.Durability.Level != "SYNC_OBSERVED" {
				continue
			}
		case lifecycle.QueueAdmission:
			if observation.Details == nil || observation.Details.Queue == nil || observation.Details.Queue.Work.WorkID != child || observation.Details.Queue.Work.ParentWorkID != parent || observation.Details.Queue.Work.AdmissionRecordRef != c.config.QueueAdmissionRef || observation.Details.Queue.Work.Namespace.OwnerID != c.config.Consumer.ServiceID || observation.Details.Queue.Work.Namespace.Resource != c.config.QueueTarget || observation.Details.Queue.Work.Namespace.ProducerIncarnation != c.config.Consumer.ProcessIncarnation {
				continue
			}
		}
		if observation.Knowledge == lifecycle.Known && observation.Presence == lifecycle.Present && observation.Outcome.State == lifecycle.Observed && observation.Outcome.Value != nil && *observation.Outcome.Value == lifecycle.SuccessAtBoundary {
			seen[observation.Address.SubBoundary] = true
		}
	}
	return seen[lifecycle.RawWrite] && seen[lifecycle.Durability] && seen[lifecycle.QueueAdmission]
}

func (c *LifecycleClient) observedFrame(record lifecycle.Record, context *lifecycle.ObservationContext) bool {
	if context == nil || context.OwnerID != c.config.Consumer.ServiceID || context.Scope != record.Inspection.Target.Scope {
		return false
	}
	for _, frame := range record.Inspection.SourceFrames {
		if frame.FrameRef == context.FrameRef && frame.OwnerID == context.OwnerID && frame.Scope == context.Scope && frame.SourceRevision == context.SourceRevision {
			return true
		}
	}
	return false
}

func lifecycleEndpoint(base, streamID string, inspect bool) (string, error) {
	if _, err := audioEndpoint(base, streamID); err != nil {
		return "", err
	}
	u, err := url.Parse(base)
	if err != nil || streamID == "" || strings.ContainsAny(streamID, "/\\") {
		return "", errors.New("invalid lifecycle target")
	}
	u.Path = strings.TrimRight(u.Path, "/") + "/v3/streams/" + streamID + "/audio/opus"
	u.RawPath = ""
	if inspect {
		u.Path += "/inspect"
	}
	return u.String(), nil
}
