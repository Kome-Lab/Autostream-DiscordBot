package control

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"

	lifecycle "github.com/Kome-Lab/Autostream-Contracts/v3lifecycle"
	"github.com/example/autostream-discord-bot/internal/manualhold"
)

// AutoStartRequest identifies one automatic-start attempt against the exact
// non-secret CP observation. It is neither an execution grant nor a Generation.
// An empty ExpectedRunID is an explicitly observed absence, not a current-value
// substitution; the field is always transmitted.
type AutoStartRequest struct {
	RequestID                string `json:"request_id"`
	ExpectedHoldRevision     uint64 `json:"expected_hold_revision"`
	ExpectedStreamRevision   string `json:"expected_stream_revision"`
	ExpectedSettingsRevision string `json:"expected_settings_revision"`
	ExpectedRunID            string `json:"expected_run_id"`
}

func (r AutoStartRequest) Valid() bool {
	for _, value := range []string{r.RequestID, r.ExpectedStreamRevision, r.ExpectedSettingsRevision} {
		if value == "" || value != strings.TrimSpace(value) || len(value) > 96 || strings.ContainsAny(value, "\r\n\x00") {
			return false
		}
	}
	if !validAutoStartRequestID(r.RequestID) {
		return false
	}
	return r.ExpectedHoldRevision > 0 && r.ExpectedRunID == strings.TrimSpace(r.ExpectedRunID) &&
		len(r.ExpectedRunID) <= 96 && !strings.ContainsAny(r.ExpectedRunID, "\r\n\x00")
}

func validAutoStartRequestID(value string) bool {
	if value == "" || len(value) > 96 {
		return false
	}
	for _, char := range value {
		if !(char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' || strings.ContainsRune("-_.:", char)) {
			return false
		}
	}
	return true
}

// StartManagedStream performs exactly one HTTP attempt. In particular, an ACK
// loss must not fall through Client.StartStream's legacy empty-body path.
func (c Client) StartManagedStream(ctx context.Context, streamID string, request AutoStartRequest) error {
	streamID = strings.TrimSpace(streamID)
	if streamID == "" || !request.Valid() {
		return errors.New("explicit automatic-start observation is required")
	}
	return c.post(ctx, "/services/streams/"+url.PathEscape(streamID)+"/start", request)
}

// ManagedStartInspection contains the CP-owned original and independently
// observed target boundaries. It never asserts aggregate Start success.
type ManagedStartInspection struct {
	Admission struct {
		RequestID string             `json:"request_id"`
		StreamID  string             `json:"stream_id"`
		Records   []lifecycle.Record `json:"records"`
	} `json:"admission"`
	StartObservations []lifecycle.BoundaryObservation `json:"start_observations"`
	Gate              lifecycle.Gate                  `json:"gate"`
}

// InspectManagedStream performs one bounded pure result read for the original
// request. ACK loss is never converted into a second POST, mutation or fallback.
func (c Client) InspectManagedStream(ctx context.Context, streamID, requestID string) (ManagedStartInspection, error) {
	var result ManagedStartInspection
	if strings.TrimSpace(streamID) != streamID || streamID == "" || len(streamID) > 96 ||
		!validAutoStartRequestID(requestID) {
		return result, errors.New("original managed Start request is required")
	}
	if err := c.Config.Validate(); err != nil {
		return result, err
	}
	endpoint := "/services/streams/" + url.PathEscape(streamID) + "/auto-start/attempts/" + url.PathEscape(requestID)
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, joinURL(c.Config.ControlPanelURL, endpoint), nil)
	if err != nil {
		return result, err
	}
	request.Header.Set("Authorization", "Bearer "+c.Config.Token)
	client := c.HTTP
	if client == nil {
		client = noRedirectClient()
	}
	response, err := client.Do(request)
	if err != nil {
		return result, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return result, controlPanelErrorFromResponse(endpoint, response)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, 1<<20+1))
	if err != nil || len(body) > 1<<20 {
		return result, errors.New("bounded managed Start inspection is unavailable")
	}
	var wire struct {
		Admission struct {
			RequestID string            `json:"request_id"`
			StreamID  string            `json:"stream_id"`
			Records   []json.RawMessage `json:"records"`
		} `json:"admission"`
		StartObservations []lifecycle.BoundaryObservation `json:"start_observations"`
		Gate              lifecycle.Gate                  `json:"gate"`
	}
	if json.Unmarshal(body, &wire) != nil || wire.Admission.RequestID != requestID || wire.Admission.StreamID != streamID ||
		wire.Gate != lifecycle.NotQualified || len(wire.Admission.Records) == 0 || len(wire.Admission.Records) > 16 || len(wire.StartObservations) > 64 {
		return result, errors.New("original managed Start inspection does not match")
	}
	result.Admission.RequestID, result.Admission.StreamID, result.Gate = requestID, streamID, wire.Gate
	targets := map[lifecycle.ResourceRef]bool{}
	for _, raw := range wire.Admission.Records {
		record, err := manualhold.DecodeJSON(raw)
		if err != nil || manualhold.Validate(record, streamID) != nil || string(record.Header.Correlation.RequestID) != requestID {
			return ManagedStartInspection{}, errors.New("managed Start source record does not match")
		}
		resource, _ := manualhold.Resource(record)
		if targets[resource] {
			return ManagedStartInspection{}, errors.New("managed Start source target is duplicated")
		}
		targets[resource] = true
		result.Admission.Records = append(result.Admission.Records, record)
	}
	for _, observation := range wire.StartObservations {
		validation := lifecycle.ValidateBoundaryObservation(observation, manualhold.Limits())
		if (validation.Kind != lifecycle.StructurallyCoherent && validation.Kind != lifecycle.Unknown && validation.Kind != lifecycle.PendingObligation) ||
			!targets[observation.Address.Resource] || observation.Address.OwnerID != observation.Address.Resource.OwnerID {
			return ManagedStartInspection{}, errors.New("managed Start boundary observation does not match")
		}
		if observation.Knowledge == lifecycle.KnowledgeUnknown &&
			(observation.Presence != lifecycle.NotObserved || observation.Outcome.State != lifecycle.Unavailable || observation.Outcome.Value != nil || observation.Outcome.Observation != nil || observation.Outcome.Unavailable == nil) {
			return ManagedStartInspection{}, errors.New("managed Start UNKNOWN boundary has conflicting knowledge")
		}
	}
	result.StartObservations = wire.StartObservations
	return result, nil
}
