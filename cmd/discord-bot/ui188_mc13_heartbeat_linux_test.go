//go:build linux && ui188_chain

package main

// Test-only observation and bounded recovery of one precisely classified
// heartbeat rejection. Production Client, Manager and CP guards are unchanged.
import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/example/autostream-discord-bot/internal/control"
)

type ui188HBClock interface{ Now() time.Time }
type ui188HBRealClock struct{}

func (ui188HBRealClock) Now() time.Time { return time.Now() }

type ui188HBExchange struct {
	Sequence             uint64  `json:"sequence"`
	Method               string  `json:"method"`
	Path                 string  `json:"path"`
	Status               int     `json:"status"`
	Code                 string  `json:"code"`
	ReportedStreamID     string  `json:"reported_stream_id"`
	ServiceID            string  `json:"service_id"`
	AtUTC                string  `json:"at_utc"`
	EndedAtUTC           string  `json:"ended_at_utc"`
	ElapsedMonoNS        int64   `json:"elapsed_mono_ns"`
	DurationNS           int64   `json:"duration_ns"`
	BodyValid            bool    `json:"body_valid"`
	TransportError       bool    `json:"transport_error"`
	ResponseBodyBytes    int     `json:"response_body_bytes,omitempty"`
	ResponseSHA256       string  `json:"response_sha256,omitempty"`
	OriginalResponseBody *string `json:"original_response_body,omitempty"`
}

type ui188HBCallKey struct{}
type ui188HBCall struct{ exchange *ui188HBExchange }

type ui188HBTransport struct {
	next   http.RoundTripper
	clock  ui188HBClock
	origin time.Time
	mu     sync.Mutex
	seq    uint64
	write  func(any) error
}

func ui188HBHTTPClient(transport http.RoundTripper) *http.Client {
	return &http.Client{Transport: transport, Timeout: 30 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
}

func (h *ui188HBTransport) record(value ui188HBExchange, slot *ui188HBCall) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if slot != nil {
		copy := value
		slot.exchange = &copy
	}
	return h.write(value)
}

func ui188HBCode(raw []byte) (string, bool) {
	if len(raw) > 4096 {
		return "", false
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	opening, err := decoder.Token()
	if err != nil || opening != json.Delim('{') {
		return "", false
	}
	key, err := decoder.Token()
	if err != nil || key != "code" {
		return "", false
	}
	var code string
	if decoder.Decode(&code) != nil {
		return "", false
	}
	closing, err := decoder.Token()
	if err != nil || closing != json.Delim('}') || decoder.Decode(new(any)) != io.EOF {
		return "", false
	}
	return code, code != ""
}

func (h *ui188HBTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	isHeartbeat := request.Method == http.MethodPost && request.URL.Path == "/services/heartbeat"
	isRuntime := request.Method == http.MethodGet && request.URL.Path == "/services/runtime-config"
	if !isHeartbeat && !isRuntime {
		return h.next.RoundTrip(request)
	}
	started := h.clock.Now()
	h.mu.Lock()
	h.seq++
	seq := h.seq
	h.mu.Unlock()
	event := ui188HBExchange{Sequence: seq, Method: request.Method, Path: request.URL.Path,
		AtUTC: started.UTC().Format(time.RFC3339Nano), ElapsedMonoNS: started.Sub(h.origin).Nanoseconds()}
	if isHeartbeat {
		raw, err := io.ReadAll(io.LimitReader(request.Body, 8193))
		request.Body.Close()
		if err != nil || len(raw) > 8192 {
			return nil, errors.New("heartbeat trace request framing unavailable")
		}
		request.Body = io.NopCloser(bytes.NewReader(raw))
		var body struct {
			ServiceID string `json:"service_id"`
			StreamID  string `json:"current_stream_id"`
		}
		if json.Unmarshal(raw, &body) != nil || body.ServiceID == "" {
			return nil, errors.New("heartbeat trace request identity unavailable")
		}
		event.ServiceID, event.ReportedStreamID = body.ServiceID, body.StreamID
	} else {
		event.ServiceID = request.URL.Query().Get("service_id")
	}
	response, transportError := h.next.RoundTrip(request)
	event.TransportError = transportError != nil
	if response != nil {
		event.Status = response.StatusCode
		raw, err := io.ReadAll(io.LimitReader(response.Body, 131073))
		response.Body.Close()
		response.Body = io.NopCloser(bytes.NewReader(raw))
		if err == nil && len(raw) <= 131072 {
			event.BodyValid = json.Valid(raw)
			if isHeartbeat && response.StatusCode != http.StatusAccepted {
				event.Code, event.BodyValid = ui188HBCode(raw)
				event.ResponseBodyBytes = len(raw)
				digest := sha256.Sum256(raw)
				event.ResponseSHA256 = hex.EncodeToString(digest[:])
				if response.StatusCode == http.StatusForbidden && event.BodyValid && event.Code == "service_not_assigned_to_token" {
					original := string(raw) // Exact bounded code-only body; no arbitrary body is printed.
					event.OriginalResponseBody = &original
				}
			}
		}
	}
	ended := h.clock.Now()
	event.EndedAtUTC = ended.UTC().Format(time.RFC3339Nano)
	event.DurationNS = ended.Sub(started).Nanoseconds()
	slot, _ := request.Context().Value(ui188HBCallKey{}).(*ui188HBCall)
	if err := h.record(event, slot); err != nil {
		if response != nil {
			response.Body.Close()
		}
		return nil, errors.New("heartbeat trace write failed")
	}
	return response, transportError
}

type ui188HBAssignment struct {
	StreamID    string `json:"stream_id"`
	ServiceID   string `json:"service_id"`
	ServiceType string `json:"service_type"`
	Role        string `json:"assignment_role"`
	AssignedAt  string `json:"assigned_at"`
}

type ui188HBSnapshot struct {
	Sequence        uint64              `json:"sequence"`
	Status          int                 `json:"status"`
	AtUTC           string              `json:"at_utc"`
	EndedAtUTC      string              `json:"ended_at_utc"`
	ServiceID       string              `json:"service_id"`
	ServiceType     string              `json:"service_type"`
	CurrentStreamID string              `json:"current_stream_id"`
	Assignments     []ui188HBAssignment `json:"assignments"`
}

type ui188HBCorroborationRequest struct {
	Version    int             `json:"version"`
	Case       string          `json:"case"`
	RunUUID    string          `json:"run_uuid"`
	Schema     string          `json:"DB_schema"`
	ServiceID  string          `json:"service_id"`
	RecoveryID uint64          `json:"recovery_id"`
	Phase      string          `json:"phase"`
	Heartbeat  ui188HBExchange `json:"heartbeat"`
	Readback   ui188HBSnapshot `json:"readback"`
}

type ui188HBCorroborationProof struct {
	Version                  int    `json:"version"`
	Case                     string `json:"case"`
	RunUUID                  string `json:"run_uuid"`
	Schema                   string `json:"DB_schema"`
	ServiceID                string `json:"service_id"`
	RecoveryID               uint64 `json:"recovery_id"`
	Phase                    string `json:"phase"`
	RequestSHA256            string `json:"request_sha256"`
	OriginalExit             int    `json:"original_exit"`
	ObservedAtUTC            string `json:"observed_at_utc"`
	TokenIdentityCurrent     bool   `json:"token_identity_current"`
	TokenActive              bool   `json:"token_active"`
	CanonicalReceiptsMatched bool   `json:"canonical_receipts_matched"`
	CanonicalReceipt         struct {
		Path   string `json:"path"`
		SHA256 string `json:"sha256"`
	} `json:"canonical_receipt"`
	DBSource      string `json:"dbsource"`
	DBTransaction struct {
		StartedAtUTC string `json:"started_at_utc"`
		EndedAtUTC   string `json:"ended_at_utc"`
		OriginalExit int    `json:"original_exit"`
		Isolation    string `json:"isolation"`
	} `json:"db_transaction"`
	DB ui188HBSnapshot `json:"db"`
}

type ui188HBCorroborator func(context.Context, ui188HBCorroborationRequest) error

func ui188HBReadback(config control.RuntimeConfig, event *ui188HBExchange, service, reported string) (ui188HBSnapshot, error) {
	bad := func() (ui188HBSnapshot, error) {
		return ui188HBSnapshot{}, errors.New("heartbeat runtime identity or assignment mismatch")
	}
	if event == nil || event.TransportError || event.Status != http.StatusOK || !event.BodyValid ||
		event.ServiceID != service || event.Method != http.MethodGet || event.Path != "/services/runtime-config" ||
		config.Service.ServiceID != service || config.Service.ServiceType != control.ServiceType {
		return bad()
	}
	out := ui188HBSnapshot{Sequence: event.Sequence, Status: event.Status, AtUTC: event.AtUTC, EndedAtUTC: event.EndedAtUTC, ServiceID: service,
		ServiceType: config.Service.ServiceType, CurrentStreamID: config.Service.CurrentStreamID,
		Assignments: []ui188HBAssignment{}}
	if config.Assignments == nil || len(config.Assignments) > 1 {
		return bad()
	}
	for _, assignment := range config.Assignments {
		if assignment.ServiceID != service || assignment.ServiceType != control.ServiceType ||
			assignment.AssignmentRole != "primary" || !ui188HBUUID(assignment.StreamID) || assignment.AssignedAt.IsZero() ||
			assignment.StreamID != config.Service.CurrentStreamID {
			return bad()
		}
		out.Assignments = append(out.Assignments, ui188HBAssignment{StreamID: assignment.StreamID,
			ServiceID: service, ServiceType: control.ServiceType, Role: "primary",
			AssignedAt: assignment.AssignedAt.UTC().Format(time.RFC3339Nano)})
	}
	if (len(out.Assignments) == 0 && out.CurrentStreamID != "") ||
		(reported != "" && reported != out.CurrentStreamID) {
		return bad()
	}
	return out, nil
}

func ui188HBUUID(value string) bool {
	if len(value) != 36 || value[8] != '-' || value[13] != '-' || value[18] != '-' || value[23] != '-' {
		return false
	}
	compact := strings.ReplaceAll(value, "-", "")
	if len(compact) != 32 || strings.ToLower(compact) != compact {
		return false
	}
	_, err := hex.DecodeString(compact)
	return err == nil
}

func ui188HBSameOwner(a, b ui188HBSnapshot) bool {
	if a.ServiceID != b.ServiceID || a.ServiceType != b.ServiceType || a.CurrentStreamID != b.CurrentStreamID ||
		a.Assignments == nil || b.Assignments == nil || len(a.Assignments) != len(b.Assignments) {
		return false
	}
	for i := range a.Assignments {
		x, y := a.Assignments[i], b.Assignments[i]
		xTime, xErr := time.Parse(time.RFC3339Nano, x.AssignedAt)
		yTime, yErr := time.Parse(time.RFC3339Nano, y.AssignedAt)
		if xErr != nil || yErr != nil || !xTime.Equal(yTime) || x.StreamID != y.StreamID ||
			x.ServiceID != y.ServiceID || x.ServiceType != y.ServiceType || x.Role != y.Role {
			return false
		}
	}
	return true
}

type ui188HBPending struct {
	id       uint64
	started  time.Time
	ticks    int
	snapshot ui188HBSnapshot
}

type ui188HBRecovery struct {
	service, caseID, runUUID, schema string
	token                            string // In-memory equality only; never serialized, printed or hashed.
	clock                            ui188HBClock
	corroborate                      ui188HBCorroborator
	record                           func(any) error
	pending                          *ui188HBPending
}

func (p *ui188HBRecovery) boundedContext(ctx context.Context) (context.Context, context.CancelFunc, error) {
	remaining := 5*time.Second - p.clock.Now().Sub(p.pending.started)
	if remaining <= 0 || p.pending.ticks > 3 {
		return nil, nil, errors.New("heartbeat recovery bound exhausted")
	}
	bound, cancel := context.WithTimeout(ctx, remaining)
	return bound, cancel, nil
}

func (p *ui188HBRecovery) readAndCorroborate(ctx context.Context, client control.Client, heartbeat ui188HBExchange, phase string) (ui188HBSnapshot, error) {
	bound, cancel, err := p.boundedContext(ctx)
	if err != nil {
		return ui188HBSnapshot{}, err
	}
	defer cancel()
	slot := &ui188HBCall{}
	config, err := client.RuntimeConfig(context.WithValue(bound, ui188HBCallKey{}, slot))
	if err != nil {
		return ui188HBSnapshot{}, errors.New("heartbeat recovery authenticated runtime read failed")
	}
	snapshot, err := ui188HBReadback(config, slot.exchange, p.service, heartbeat.ReportedStreamID)
	if err != nil {
		return ui188HBSnapshot{}, err
	}
	request := ui188HBCorroborationRequest{Version: 1, Case: p.caseID, RunUUID: p.runUUID, Schema: p.schema,
		ServiceID: p.service, RecoveryID: p.pending.id, Phase: phase, Heartbeat: heartbeat, Readback: snapshot}
	corroborationContext, cancelCorroboration, boundErr := p.boundedContext(bound)
	if boundErr != nil {
		return ui188HBSnapshot{}, boundErr
	}
	defer cancelCorroboration()
	if err = p.corroborate(corroborationContext, request); err != nil {
		return ui188HBSnapshot{}, err
	}
	if p.clock.Now().Sub(p.pending.started) >= 5*time.Second {
		return ui188HBSnapshot{}, errors.New("heartbeat recovery bound exhausted")
	}
	return snapshot, nil
}

// Tick is called only by the existing one-second ticker. It sends exactly one
// new real heartbeat. It never retries a rejected request or any control action.
func (p *ui188HBRecovery) Tick(ctx context.Context, client control.Client, reported string, metrics map[string]float64) error {
	if client.Config.ServiceID != p.service || client.Config.Token == "" {
		return errors.New("heartbeat current client identity unavailable")
	}
	if p.token == "" {
		p.token = client.Config.Token
	}
	if p.token != client.Config.Token {
		return errors.New("heartbeat current client credential changed")
	}
	started := p.clock.Now()
	if p.pending != nil {
		p.pending.ticks++
		if started.Sub(p.pending.started) >= 5*time.Second || p.pending.ticks > 3 {
			return errors.New("heartbeat recovery bound exhausted")
		}
	}
	callContext := ctx
	cancel := func() {}
	if p.pending != nil {
		var err error
		callContext, cancel, err = p.boundedContext(ctx)
		if err != nil {
			return err
		}
	}
	defer cancel()
	slot := &ui188HBCall{}
	err := client.HeartbeatWithMetrics(context.WithValue(callContext, ui188HBCallKey{}, slot), "online", reported, metrics)
	heartbeat := slot.exchange
	if heartbeat == nil || heartbeat.TransportError || heartbeat.ServiceID != p.service || heartbeat.ReportedStreamID != reported ||
		heartbeat.Path != "/services/heartbeat" || heartbeat.Method != http.MethodPost || !heartbeat.BodyValid {
		return errors.New("heartbeat actual exchange unavailable or invalid")
	}
	if err != nil {
		var panel control.ControlPanelError
		recognized := errors.As(err, &panel) && panel.Endpoint == "/services/heartbeat" &&
			panel.StatusCode == http.StatusForbidden && panel.Code == "service_not_assigned_to_token" &&
			heartbeat.Status == http.StatusForbidden && heartbeat.Code == panel.Code
		if !recognized {
			return errors.New("heartbeat unrecognized original error preserved")
		}
		if p.pending != nil {
			return errors.New("heartbeat repeated assignment rejection")
		}
		p.pending = &ui188HBPending{id: heartbeat.Sequence, started: started, ticks: 1}
		if p.record(map[string]any{"event": "heartbeat403_unresolved", "heartbeat": heartbeat, "case": p.caseID,
			"run_uuid": p.runUUID, "DB_schema": p.schema, "automatic_heartbeat_retries": 0}) != nil {
			return errors.New("heartbeat recovery event write failed")
		}
		snapshot, readErr := p.readAndCorroborate(ctx, client, *heartbeat, "rejected")
		if readErr != nil {
			return readErr
		}
		p.pending.snapshot = snapshot
		return nil
	}
	if heartbeat.Status != http.StatusAccepted {
		return errors.New("heartbeat actual success status is not 202")
	}
	if p.pending == nil {
		return nil
	}
	snapshot, readErr := p.readAndCorroborate(ctx, client, *heartbeat, "resolved")
	if readErr != nil {
		return readErr
	}
	if !ui188HBSameOwner(p.pending.snapshot, snapshot) {
		return errors.New("heartbeat assignment or epoch not stable")
	}
	if p.record(map[string]any{"event": "heartbeat_recovery_resolved", "heartbeat": heartbeat,
		"runtime_readback": snapshot, "case": p.caseID, "run_uuid": p.runUUID, "DB_schema": p.schema,
		"recovery_id": p.pending.id, "scheduled_ticks": p.pending.ticks,
		"elapsed_ns": p.clock.Now().Sub(p.pending.started).Nanoseconds(), "automatic_heartbeat_retries": 0}) != nil {
		return errors.New("heartbeat recovery resolution write failed")
	}
	p.pending = nil
	return nil
}

func ui188HBProofEqual(proof ui188HBCorroborationProof, request ui188HBCorroborationRequest, raw []byte, now time.Time) error {
	digest := sha256.Sum256(raw)
	observed, err := time.Parse(time.RFC3339Nano, proof.ObservedAtUTC)
	dbStart, startErr := time.Parse(time.RFC3339Nano, proof.DBTransaction.StartedAtUTC)
	dbEnd, endErr := time.Parse(time.RFC3339Nano, proof.DBTransaction.EndedAtUTC)
	heartbeatAt, heartbeatErr := time.Parse(time.RFC3339Nano, request.Heartbeat.AtUTC)
	heartbeatEnd, heartbeatEndErr := time.Parse(time.RFC3339Nano, request.Heartbeat.EndedAtUTC)
	readAt, readErr := time.Parse(time.RFC3339Nano, request.Readback.AtUTC)
	readEnd, readEndErr := time.Parse(time.RFC3339Nano, request.Readback.EndedAtUTC)
	if err != nil || now.Sub(observed) < 0 || now.Sub(observed) > time.Second ||
		heartbeatErr != nil || heartbeatEndErr != nil || readErr != nil || readEndErr != nil ||
		heartbeatEnd.Before(heartbeatAt) || readAt.Before(heartbeatEnd) || readEnd.Before(readAt) ||
		startErr != nil || endErr != nil || dbEnd.Before(dbStart) || dbEnd.After(observed) ||
		dbStart.Before(readEnd) || now.Sub(dbStart) < 0 || now.Sub(dbStart) > 5*time.Second ||
		proof.DBTransaction.OriginalExit != 0 || proof.DBTransaction.Isolation != "REPEATABLE_READ_READ_ONLY" ||
		proof.DBSource != "REAL_MARIADB_READ_ONLY" ||
		proof.Version != 1 || proof.Case != request.Case || proof.RunUUID != request.RunUUID ||
		proof.Schema != request.Schema || proof.ServiceID != request.ServiceID || proof.RecoveryID != request.RecoveryID ||
		proof.Phase != request.Phase || proof.RequestSHA256 != hex.EncodeToString(digest[:]) || proof.OriginalExit != 0 ||
		!proof.TokenIdentityCurrent || !proof.TokenActive || !proof.CanonicalReceiptsMatched ||
		!ui188HBSameOwner(proof.DB, request.Readback) {
		return errors.New("heartbeat independent receipt or DB proof mismatch")
	}
	return nil
}

func ui188HBReceiptEqual(root string, proof ui188HBCorroborationProof, request ui188HBCorroborationRequest) error {
	name := proof.CanonicalReceipt.Path
	prefix := "heartbeat-recovery/assignment-receipt-" + request.Case + "-" + request.ServiceID + "-"
	if !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, ".json") ||
		filepath.IsAbs(name) || filepath.ToSlash(filepath.Clean(name)) != name || strings.Contains(name, "..") {
		return errors.New("heartbeat canonical receipt path invalid")
	}
	path := filepath.Join(root, filepath.FromSlash(name))
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("heartbeat canonical receipt unavailable")
	}
	raw, err := os.ReadFile(path)
	if err != nil || len(raw) > 32768 {
		return errors.New("heartbeat canonical receipt framing failed")
	}
	digest := sha256.Sum256(raw)
	if proof.CanonicalReceipt.SHA256 != hex.EncodeToString(digest[:]) {
		return errors.New("heartbeat canonical receipt source hash mismatch")
	}
	var receipt struct {
		Case    string          `json:"case"`
		RunUUID string          `json:"run_uuid"`
		Schema  string          `json:"DB_schema"`
		Service string          `json:"service_id"`
		Status  int             `json:"HTTP_status"`
		Source  string          `json:"source"`
		Retries int             `json:"mutation_auto_retries"`
		After   ui188HBSnapshot `json:"after"`
		Method  string          `json:"method"`
		Path    string          `json:"path"`
	}
	if json.Unmarshal(raw, &receipt) != nil || receipt.Case != request.Case || receipt.RunUUID != request.RunUUID ||
		receipt.Schema != request.Schema || receipt.Service != request.ServiceID || receipt.Status != 200 ||
		receipt.Retries != 0 || receipt.Source != "REGULAR_CP_MUTATION_THEN_AUTHENTICATED_ADMIN_GET" ||
		!ui188HBSameOwner(receipt.After, request.Readback) ||
		!((receipt.Method == "POST" && receipt.Path == "/services/"+request.ServiceID+"/assign") ||
			(receipt.Method == "DELETE" && receipt.Path == "/services/"+request.ServiceID+"/assignment")) {
		return errors.New("heartbeat canonical receipt identity mismatch")
	}
	return nil
}

func ui188HBAtomicCreate(path string, raw []byte) error {
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		return errors.New("heartbeat evidence already exists or unavailable")
	}
	temporary := path + "." + strconv.Itoa(os.Getpid()) + ".tmp"
	file, err := os.OpenFile(temporary, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	_, writeErr := file.Write(raw)
	closeErr := file.Close()
	if writeErr != nil {
		return writeErr
	}
	if closeErr != nil {
		return closeErr
	}
	return os.Rename(temporary, path)
}

func ui188HBDecodeProof(raw []byte) (ui188HBCorroborationProof, error) {
	var proof ui188HBCorroborationProof
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil || fields == nil {
		return proof, errors.New("heartbeat proof object missing")
	}
	for _, key := range []string{"version", "case", "run_uuid", "DB_schema", "service_id", "recovery_id", "phase", "request_sha256", "original_exit", "observed_at_utc", "token_identity_current", "token_active", "canonical_receipts_matched", "canonical_receipt", "dbsource", "db_transaction", "db"} {
		value, ok := fields[key]
		if !ok || bytes.Equal(value, []byte("null")) {
			return proof, errors.New("heartbeat proof required field missing")
		}
	}
	for key, required := range map[string][]string{"db": {"service_id", "service_type", "current_stream_id", "assignments"}, "db_transaction": {"started_at_utc", "ended_at_utc", "original_exit", "isolation"}, "canonical_receipt": {"path", "sha256"}} {
		var child map[string]json.RawMessage
		if json.Unmarshal(fields[key], &child) != nil || child == nil {
			return proof, errors.New("heartbeat nested proof object missing")
		}
		for _, name := range required {
			value, ok := child[name]
			if !ok || bytes.Equal(value, []byte("null")) {
				return proof, errors.New("heartbeat nested proof required field missing")
			}
		}
	}
	if json.Unmarshal(raw, &proof) != nil {
		return proof, errors.New("heartbeat proof JSON invalid")
	}
	return proof, nil
}

func ui188HBFileCorroborator(directory string, clock ui188HBClock) ui188HBCorroborator {
	return func(ctx context.Context, request ui188HBCorroborationRequest) error {
		name := request.Case + "-" + request.ServiceID + "-" + strconv.FormatUint(request.RecoveryID, 10) + "-" + request.Phase + ".json"
		raw, err := json.Marshal(request)
		if err != nil {
			return err
		}
		raw = append(raw, '\n')
		if err = ui188HBAtomicCreate(filepath.Join(directory, "request-"+name), raw); err != nil {
			return err
		}
		proofPath := filepath.Join(directory, "proof-"+name)
		limit, cancel := context.WithTimeout(ctx, 700*time.Millisecond)
		defer cancel()
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			info, statErr := os.Lstat(proofPath)
			if statErr == nil {
				if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
					return errors.New("heartbeat proof is not a regular owned file")
				}
				proofRaw, readErr := os.ReadFile(proofPath)
				if readErr != nil || len(proofRaw) > 32768 {
					return errors.New("heartbeat proof framing failed")
				}
				proof, decodeErr := ui188HBDecodeProof(proofRaw)
				if decodeErr != nil {
					return decodeErr
				}
				if err := ui188HBProofEqual(proof, request, raw, clock.Now()); err != nil {
					return err
				}
				return ui188HBReceiptEqual(filepath.Dir(directory), proof, request)
			}
			if !os.IsNotExist(statErr) {
				return errors.New("heartbeat proof lookup failed")
			}
			select {
			case <-limit.Done():
				return errors.New("heartbeat independent DB proof missing within bound")
			case <-ticker.C:
			}
		}
	}
}

func ui188HBContext(root, caseID, schema, runUUID string) bool {
	if caseID != "cb15" || schema != "ui188_cb15_20261018" || runUUID != "03e37a89-9a1b-4e39-aab3-49152b74d0a4" {
		return false
	}
	path := filepath.Join(root, "cb15-diagnostic-identity.json")
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return false
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return false
	}
	digest := sha256.Sum256(raw)
	if hex.EncodeToString(digest[:]) != "5e3b75f0d82f56dd8a634c92324b5cea3c34e333e0c864ef25ed8b9c77594a61" {
		return false
	}
	var authority struct {
		Scope        string `json:"scope"`
		Case         string `json:"case"`
		RunUUID      string `json:"run_uuid"`
		Schema       string `json:"DB_schema"`
		T0           string `json:"T0_UTC"`
		NoAcceptance bool   `json:"not_formal_acceptance"`
	}
	return json.Unmarshal(raw, &authority) == nil && authority.Scope == "UI188_CURRENT_CREDENTIAL_BINDING_AND_WAIT_DIAGNOSIS" && authority.Case == caseID && authority.RunUUID == runUUID && authority.Schema == schema && authority.T0 == "2026-10-18T13:56:43+00:00" && authority.NoAcceptance
}

func newUI188MC12Heartbeat(service string, next http.RoundTripper) (*ui188HBTransport, *ui188HBRecovery, func(), error) {
	caseID, schema, root := os.Getenv("UI188_PROBE_ATTEMPT"), os.Getenv("UI188_DB_SCHEMA"), os.Getenv("UI188_EVIDENCE_DIRECTORY")
	runUUID := os.Getenv("UI188_WAIT_CALLER_RUN_ID")
	valid := ui188HBContext(root, caseID, schema, runUUID)
	if !valid || (service != "bot-a" && service != "bot-b") || !filepath.IsAbs(root) {
		return nil, nil, nil, errors.New("MC12 heartbeat scope identity invalid")
	}
	directory := filepath.Join(root, "heartbeat-recovery")
	if err := os.Mkdir(directory, 0700); err != nil && !os.IsExist(err) {
		return nil, nil, nil, err
	}
	info, err := os.Lstat(directory)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, nil, nil, errors.New("heartbeat proof directory invalid")
	}
	file, err := os.OpenFile(filepath.Join(directory, "events-"+caseID+"-"+service+".jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return nil, nil, nil, err
	}
	clock := ui188HBRealClock{}
	var lock sync.Mutex
	record := func(value any) error { lock.Lock(); defer lock.Unlock(); return json.NewEncoder(file).Encode(value) }
	transport := &ui188HBTransport{next: next, clock: clock, origin: clock.Now(), write: record}
	policy := &ui188HBRecovery{service: service, caseID: caseID, runUUID: runUUID, schema: schema, clock: clock,
		corroborate: ui188HBFileCorroborator(directory, clock), record: record}
	return transport, policy, func() { _ = file.Close() }, nil
}
