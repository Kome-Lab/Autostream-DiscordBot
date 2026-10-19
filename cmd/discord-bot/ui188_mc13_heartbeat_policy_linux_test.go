//go:build linux && ui188_chain

package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/example/autostream-discord-bot/internal/control"
)

type ui188HBFakeClock struct{ now time.Time }

func (c *ui188HBFakeClock) Now() time.Time { return c.now }

type ui188HBUnitStep struct {
	method, path string
	status       int
	body         string
	err          error
	bodyClock    *ui188HBFakeClock
	bodyAdvance  time.Duration
}
type ui188HBUnitDelayedBody struct {
	reader   io.Reader
	clock    *ui188HBFakeClock
	advance  time.Duration
	advanced bool
}

func (b *ui188HBUnitDelayedBody) Read(out []byte) (int, error) {
	if !b.advanced {
		b.clock.now = b.clock.now.Add(b.advance)
		b.advanced = true
	}
	return b.reader.Read(out)
}

type ui188HBUnitTransport struct {
	t     *testing.T
	steps []ui188HBUnitStep
	calls []string
}

func (u *ui188HBUnitTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	u.t.Helper()
	if len(u.steps) == 0 {
		u.t.Fatal("unexpected HTTP request")
	}
	step := u.steps[0]
	u.steps = u.steps[1:]
	if request.Method != step.method || request.URL.Path != step.path {
		u.t.Fatalf("unexpected HTTP method/path: %s %s", request.Method, request.URL.Path)
	}
	if request.Header.Get("Authorization") != "Bearer UNIT_ONLY_SECRET_NEVER_LOG" {
		u.t.Fatal("actual control.Client authentication missing")
	}
	u.calls = append(u.calls, request.Method+" "+request.URL.Path)
	if step.err != nil {
		return nil, step.err
	}
	var body io.Reader = strings.NewReader(step.body)
	if step.bodyClock != nil {
		body = &ui188HBUnitDelayedBody{reader: body, clock: step.bodyClock, advance: step.bodyAdvance}
	}
	return &http.Response{StatusCode: step.status, Header: make(http.Header), Body: io.NopCloser(body), Request: request}, nil
}

type ui188HBUnit struct {
	t                  *testing.T
	clock              *ui188HBFakeClock
	wire               *ui188HBUnitTransport
	client             control.Client
	policy             *ui188HBRecovery
	exchanges          []ui188HBExchange
	requests           []ui188HBCorroborationRequest
	records            []any
	corroborationError error
}

func ui188HBUnitRuntime(service, kind, current string, assignments []control.StreamServiceAssignment) string {
	raw, _ := json.Marshal(control.RuntimeConfig{Service: control.RegisteredService{ServiceID: service, ServiceType: kind, CurrentStreamID: current}, Assignments: assignments})
	return string(raw)
}
func ui188HBUnitNew(t *testing.T, steps ...ui188HBUnitStep) *ui188HBUnit {
	t.Helper()
	u := &ui188HBUnit{t: t, clock: &ui188HBFakeClock{now: time.Date(2026, 10, 17, 16, 0, 0, 0, time.UTC)}}
	u.wire = &ui188HBUnitTransport{t: t, steps: steps}
	transport := &ui188HBTransport{next: u.wire, clock: u.clock, origin: u.clock.Now(), write: func(value any) error { u.exchanges = append(u.exchanges, value.(ui188HBExchange)); return nil }}
	u.client = control.Client{Config: control.Config{ControlPanelURL: "http://127.0.0.1:18841", Token: "UNIT_ONLY_SECRET_NEVER_LOG", ServiceID: "bot-a", ServiceName: "unit-bot", ServicePublicURL: "http://127.0.0.1:18083"}, HTTP: ui188HBHTTPClient(transport)}
	u.policy = &ui188HBRecovery{service: "bot-a", caseID: "mc13", runUUID: "4e7ac320-a685-4d00-b4a7-de0d36720f73", schema: "ui188_mc13_recovery_20261018", clock: u.clock,
		corroborate: func(_ context.Context, r ui188HBCorroborationRequest) error {
			u.requests = append(u.requests, r)
			return u.corroborationError
		}, record: func(v any) error { u.records = append(u.records, v); return nil }}
	return u
}
func ui188HBUnitHB(status int, body string) ui188HBUnitStep {
	return ui188HBUnitStep{method: "POST", path: "/services/heartbeat", status: status, body: body}
}
func ui188HBUnitGET(body string) ui188HBUnitStep {
	return ui188HBUnitStep{method: "GET", path: "/services/runtime-config", status: 200, body: body}
}
func ui188HBUnitReject() ui188HBUnitStep {
	return ui188HBUnitHB(403, "{\"code\":\"service_not_assigned_to_token\"}\n")
}
func ui188HBUnitEmpty() string {
	return ui188HBUnitRuntime("bot-a", "discord_bot", "", []control.StreamServiceAssignment{})
}
func (u *ui188HBUnit) tick(reported string) error {
	return u.policy.Tick(context.Background(), u.client, reported, nil)
}

func TestUI188MC12HeartbeatActual403Scheduled202AndGET200(t *testing.T) {
	u := ui188HBUnitNew(t, ui188HBUnitReject(), ui188HBUnitGET(ui188HBUnitEmpty()), ui188HBUnitHB(202, "{}"), ui188HBUnitGET(ui188HBUnitEmpty()))
	if err := u.tick(""); err != nil {
		t.Fatal(err)
	}
	if u.policy.pending == nil || len(u.wire.calls) != 2 || len(u.requests) != 1 {
		t.Fatal("first actual 403 must remain pending, without resend")
	}
	if u.exchanges[0].OriginalResponseBody == nil || *u.exchanges[0].OriginalResponseBody != "{\"code\":\"service_not_assigned_to_token\"}\n" {
		t.Fatal("raw safe original 403 body was not retained")
	}
	u.clock.now = u.clock.now.Add(time.Second)
	if err := u.tick(""); err != nil {
		t.Fatal(err)
	}
	if u.policy.pending != nil || len(u.wire.calls) != 4 || len(u.requests) != 2 || len(u.records) != 2 {
		t.Fatal("bounded independently corroborated recovery incomplete")
	}
	if u.requests[0].Phase != "rejected" || u.requests[1].Phase != "resolved" || u.requests[0].RecoveryID != u.requests[1].RecoveryID {
		t.Fatal("wrong event binding")
	}
	if u.requests[1].Heartbeat.Status != 202 || u.requests[1].Readback.Status != 200 || u.requests[1].Readback.Sequence <= u.requests[1].Heartbeat.Sequence {
		t.Fatal("actual producer and consumer sequence mismatch")
	}
	raw, _ := json.Marshal(struct {
		Exchanges []ui188HBExchange
		Records   []any
		Requests  []ui188HBCorroborationRequest
	}{u.exchanges, u.records, u.requests})
	if strings.Contains(string(raw), "UNIT_ONLY_SECRET_NEVER_LOG") || strings.Contains(string(raw), "Authorization") {
		t.Fatal("secret in trace")
	}
	for _, call := range u.wire.calls {
		if call != "POST /services/heartbeat" && call != "GET /services/runtime-config" {
			t.Fatal("control mutation/retry was introduced")
		}
	}
}

func TestUI188MC12HeartbeatUnknownErrorsAndMalformed403FailClosed(t *testing.T) {
	cases := map[string]ui188HBUnitStep{
		"401":           ui188HBUnitHB(401, `{"code":"invalid_service_token"}`),
		"unknown403":    ui188HBUnitHB(403, `{"code":"other"}`),
		"invalidJSON":   ui188HBUnitHB(403, `not-json-UNIT_ONLY_SECRET_NEVER_LOG`),
		"missingCode":   ui188HBUnitHB(403, `{}`),
		"nullCode":      ui188HBUnitHB(403, `{"code":null}`),
		"trailing":      ui188HBUnitHB(403, `{"code":"service_not_assigned_to_token"}{}`),
		"duplicateCode": ui188HBUnitHB(403, `{"code":"service_not_assigned_to_token","code":"service_not_assigned_to_token"}`),
		"extraSecret":   ui188HBUnitHB(403, `{"code":"service_not_assigned_to_token","token":"UNIT_ONLY_SECRET_NEVER_LOG"}`),
		"trimmedCode":   ui188HBUnitHB(403, `{"code":" service_not_assigned_to_token "}`),
		"500":           ui188HBUnitHB(500, `{"code":"internal"}`),
		"HB200":         ui188HBUnitHB(200, `{}`),
		"HB204":         ui188HBUnitHB(204, ``),
		"transport":     {method: "POST", path: "/services/heartbeat", err: errors.New("UNIT_ONLY_SECRET_NEVER_LOG")},
	}
	for name, step := range cases {
		t.Run(name, func(t *testing.T) {
			u := ui188HBUnitNew(t, step)
			if u.tick("") == nil {
				t.Fatal("original failure was ignored")
			}
			if len(u.wire.calls) != 1 || len(u.requests) != 0 {
				t.Fatal("unknown failure triggered GET or automatic retry")
			}
			raw, _ := json.Marshal(u.exchanges)
			if strings.Contains(string(raw), "UNIT_ONLY_SECRET_NEVER_LOG") {
				t.Fatal("arbitrary error body or secret leaked")
			}
			if len(u.exchanges) > 0 && u.exchanges[0].OriginalResponseBody != nil {
				t.Fatal("unknown body retained as safe original")
			}
		})
	}
	for _, redirectStatus := range []int{302, 307, 308} {
		t.Run("UNIT_HTTP_redirect"+strconv.Itoa(redirectStatus), func(t *testing.T) {
			var targetHits, heartbeatPosts atomic.Int32
			target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				targetHits.Add(1)
				if r.Method == http.MethodPost {
					w.WriteHeader(202)
					_, _ = io.WriteString(w, "{}")
				} else {
					_, _ = io.WriteString(w, ui188HBUnitEmpty())
				}
			}))
			defer target.Close()
			origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPost {
					heartbeatPosts.Add(1)
					if redirectStatus == 302 {
						w.WriteHeader(403)
						_, _ = io.WriteString(w, `{"code":"service_not_assigned_to_token"}`)
						return
					}
				}
				w.Header().Set("Location", target.URL+r.URL.Path)
				w.WriteHeader(redirectStatus)
				_, _ = io.WriteString(w, `{"code":"redirect"}`)
			}))
			defer origin.Close()
			u := ui188HBUnitNew(t)
			transport := u.client.HTTP.Transport.(*ui188HBTransport)
			transport.next = http.DefaultTransport
			u.client.Config.ControlPanelURL = origin.URL
			if u.tick("") == nil {
				t.Fatal("redirect was accepted as a real heartbeat/runtime result")
			}
			if targetHits.Load() != 0 || heartbeatPosts.Load() != 1 || len(u.requests) != 0 {
				t.Fatal("redirect target reached or heartbeat automatically reposted")
			}
			if len(u.exchanges) == 0 || u.exchanges[len(u.exchanges)-1].Status != redirectStatus {
				t.Fatal("original redirect status was overwritten")
			}
		})
	}
}

func TestUI188MC12HeartbeatReadbackAndManagerNegatives(t *testing.T) {
	epoch := time.Date(2026, 10, 17, 15, 0, 0, 0, time.UTC)
	owner := control.StreamServiceAssignment{StreamID: "11111111-1111-4111-8111-111111111111", ServiceID: "bot-a", ServiceType: "discord_bot", AssignmentRole: "primary", AssignedAt: epoch}
	cases := map[string]string{
		"missingAssignments": `{"service":{"service_id":"bot-a","service_type":"discord_bot","current_stream_id":""}}`,
		"nullAssignments":    ui188HBUnitRuntime("bot-a", "discord_bot", "", nil),
		"wrongService":       ui188HBUnitRuntime("bot-b", "discord_bot", "", []control.StreamServiceAssignment{}),
		"wrongType":          ui188HBUnitRuntime("bot-a", "worker", "", []control.StreamServiceAssignment{}),
		"missingOwner":       ui188HBUnitRuntime("bot-a", "discord_bot", owner.StreamID, []control.StreamServiceAssignment{}),
		"duplicateOwner":     ui188HBUnitRuntime("bot-a", "discord_bot", owner.StreamID, []control.StreamServiceAssignment{owner, owner}),
		"wrongStream":        ui188HBUnitRuntime("bot-a", "discord_bot", "22222222-2222-4222-8222-222222222222", []control.StreamServiceAssignment{owner}),
	}
	wrongService := owner
	wrongService.ServiceID = "bot-b"
	cases["assignmentService"] = ui188HBUnitRuntime("bot-a", "discord_bot", owner.StreamID, []control.StreamServiceAssignment{wrongService})
	wrongRole := owner
	wrongRole.AssignmentRole = "standby"
	cases["standby"] = ui188HBUnitRuntime("bot-a", "discord_bot", owner.StreamID, []control.StreamServiceAssignment{wrongRole})
	wrongEpoch := owner
	wrongEpoch.AssignedAt = time.Time{}
	cases["missingEpoch"] = ui188HBUnitRuntime("bot-a", "discord_bot", owner.StreamID, []control.StreamServiceAssignment{wrongEpoch})
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			u := ui188HBUnitNew(t, ui188HBUnitReject(), ui188HBUnitGET(body))
			if u.tick("") == nil {
				t.Fatal("invalid readback accepted")
			}
			if len(u.requests) != 0 {
				t.Fatal("invalid readback reached independent success proof")
			}
		})
	}
	t.Run("staleManager", func(t *testing.T) {
		u := ui188HBUnitNew(t, ui188HBUnitReject(), ui188HBUnitGET(ui188HBUnitEmpty()))
		if u.tick(owner.StreamID) == nil {
			t.Fatal("stale Manager was hidden or cleared")
		}
	})
	for _, status := range []int{202, 401, 403} {
		t.Run("GETstatus"+strconv.Itoa(status), func(t *testing.T) {
			step := ui188HBUnitGET(ui188HBUnitEmpty())
			step.status = status
			u := ui188HBUnitNew(t, ui188HBUnitReject(), step)
			if u.tick("") == nil {
				t.Fatal("non-200 runtime accepted")
			}
		})
	}
}

func TestUI188MC12HeartbeatRepeatedRejectionAndBounds(t *testing.T) {
	t.Run("repeated403", func(t *testing.T) {
		u := ui188HBUnitNew(t, ui188HBUnitReject(), ui188HBUnitGET(ui188HBUnitEmpty()), ui188HBUnitReject())
		if err := u.tick(""); err != nil {
			t.Fatal(err)
		}
		u.clock.now = u.clock.now.Add(time.Second)
		if u.tick("") == nil {
			t.Fatal("repeated403 ignored")
		}
		if len(u.wire.calls) != 3 || len(u.requests) != 1 {
			t.Fatal("repeated rejection was retried or requalified")
		}
	})
	t.Run("UNIT_COUNTER_BOUNDARY_ONLY_thirdScheduledTick202", func(t *testing.T) {
		u := ui188HBUnitNew(t, ui188HBUnitReject(), ui188HBUnitGET(ui188HBUnitEmpty()), ui188HBUnitHB(202, "{}"), ui188HBUnitGET(ui188HBUnitEmpty()))
		if err := u.tick(""); err != nil {
			t.Fatal(err)
		}
		u.policy.pending.ticks = 2
		u.clock.now = u.clock.now.Add(2 * time.Second)
		if err := u.tick(""); err != nil {
			t.Fatal(err)
		}
		if u.policy.pending != nil || len(u.wire.calls) != 4 || u.records[len(u.records)-1].(map[string]any)["scheduled_ticks"] != 3 {
			t.Fatal("third scheduled tick boundary failed")
		}
	})
	for _, mode := range []string{"5seconds", "3ticks"} {
		t.Run(mode, func(t *testing.T) {
			u := ui188HBUnitNew(t, ui188HBUnitReject(), ui188HBUnitGET(ui188HBUnitEmpty()))
			if err := u.tick(""); err != nil {
				t.Fatal(err)
			}
			if mode == "5seconds" {
				u.clock.now = u.clock.now.Add(5 * time.Second)
			} else {
				u.policy.pending.ticks = 3
			}
			if u.tick("") == nil {
				t.Fatal("bound exhausted but accepted")
			}
			if len(u.wire.calls) != 2 {
				t.Fatal("request sent after bound")
			}
		})
	}
	t.Run("proofMissing", func(t *testing.T) {
		u := ui188HBUnitNew(t, ui188HBUnitReject(), ui188HBUnitGET(ui188HBUnitEmpty()))
		u.corroborationError = errors.New("UNIT_ONLY missing independent DB proof")
		if u.tick("") == nil {
			t.Fatal("GET alone accepted")
		}
	})
	t.Run("slowProof", func(t *testing.T) {
		u := ui188HBUnitNew(t, ui188HBUnitReject(), ui188HBUnitGET(ui188HBUnitEmpty()))
		u.policy.corroborate = func(context.Context, ui188HBCorroborationRequest) error {
			u.clock.now = u.clock.now.Add(5 * time.Second)
			return nil
		}
		if u.tick("") == nil {
			t.Fatal("late independent proof accepted")
		}
	})
	t.Run("remainingContextAfterSlowGET", func(t *testing.T) {
		u := ui188HBUnitNew(t, ui188HBUnitReject(), ui188HBUnitGET(ui188HBUnitEmpty()))
		u.wire.steps[1].bodyClock = u.clock
		u.wire.steps[1].bodyAdvance = 3 * time.Second
		called := false
		u.policy.corroborate = func(ctx context.Context, _ ui188HBCorroborationRequest) error {
			called = true
			deadline, ok := ctx.Deadline()
			if !ok || time.Until(deadline) > 2050*time.Millisecond {
				t.Fatal("DB proof inherited a full client30sec/five-second budget after slow GET")
			}
			return nil
		}
		if err := u.tick(""); err != nil {
			t.Fatal(err)
		}
		if !called {
			t.Fatal("bounded independent proof callback not reached")
		}
	})
}

func TestUI188MC12HeartbeatTokenInvalidationAndEpochInstability(t *testing.T) {
	t.Run("tokenChanged", func(t *testing.T) {
		u := ui188HBUnitNew(t, ui188HBUnitReject(), ui188HBUnitGET(ui188HBUnitEmpty()))
		if err := u.tick(""); err != nil {
			t.Fatal(err)
		}
		u.client.Config.Token = "UNIT_ONLY_DIFFERENT_SECRET"
		if u.tick("") == nil || len(u.wire.calls) != 2 {
			t.Fatal("client credential changed or request was sent")
		}
	})
	t.Run("finalEpochChanged", func(t *testing.T) {
		a := control.StreamServiceAssignment{StreamID: "11111111-1111-4111-8111-111111111111", ServiceID: "bot-a", ServiceType: "discord_bot", AssignmentRole: "primary", AssignedAt: time.Date(2026, 10, 17, 15, 0, 0, 0, time.UTC)}
		before := ui188HBUnitRuntime("bot-a", "discord_bot", a.StreamID, []control.StreamServiceAssignment{a})
		a.AssignedAt = a.AssignedAt.Add(time.Second)
		after := ui188HBUnitRuntime("bot-a", "discord_bot", a.StreamID, []control.StreamServiceAssignment{a})
		u := ui188HBUnitNew(t, ui188HBUnitReject(), ui188HBUnitGET(before), ui188HBUnitHB(202, "{}"), ui188HBUnitGET(after))
		if err := u.tick(a.StreamID); err != nil {
			t.Fatal(err)
		}
		u.clock.now = u.clock.now.Add(time.Second)
		if u.tick(a.StreamID) == nil {
			t.Fatal("new epoch silently treated as same recovery")
		}
	})
}

func ui188HBUnitProof(request ui188HBCorroborationRequest, raw []byte, now time.Time) ui188HBCorroborationProof {
	digest := sha256.Sum256(raw)
	p := ui188HBCorroborationProof{Version: 1, Case: request.Case, RunUUID: request.RunUUID, Schema: request.Schema, ServiceID: request.ServiceID, RecoveryID: request.RecoveryID, Phase: request.Phase, RequestSHA256: hex.EncodeToString(digest[:]), ObservedAtUTC: now.UTC().Format(time.RFC3339Nano), TokenIdentityCurrent: true, TokenActive: true, CanonicalReceiptsMatched: true, DB: request.Readback, DBSource: "REAL_MARIADB_READ_ONLY"}
	p.DBTransaction.StartedAtUTC = now.Format(time.RFC3339Nano)
	p.DBTransaction.EndedAtUTC = now.Format(time.RFC3339Nano)
	p.DBTransaction.Isolation = "REPEATABLE_READ_READ_ONLY"
	return p
}

func TestUI188MC12HeartbeatIndependentProofAllNegatives(t *testing.T) {
	u := ui188HBUnitNew(t, ui188HBUnitReject(), ui188HBUnitGET(ui188HBUnitEmpty()))
	if err := u.tick(""); err != nil {
		t.Fatal(err)
	}
	request := u.requests[0]
	raw, _ := json.Marshal(request)
	raw = append(raw, '\n')
	valid := ui188HBUnitProof(request, raw, u.clock.Now())
	if err := ui188HBProofEqual(valid, request, raw, u.clock.Now()); err != nil {
		t.Fatal(err)
	}
	cases := map[string]func(*ui188HBCorroborationProof){
		"oldCase": func(p *ui188HBCorroborationProof) { p.Case = "mc11" }, "wrongUUID": func(p *ui188HBCorroborationProof) { p.RunUUID = "old" }, "wrongSchema": func(p *ui188HBCorroborationProof) { p.Schema = "old" }, "wrongService": func(p *ui188HBCorroborationProof) { p.ServiceID = "bot-b" }, "wrongRecovery": func(p *ui188HBCorroborationProof) { p.RecoveryID++ }, "wrongPhase": func(p *ui188HBCorroborationProof) { p.Phase = "resolved" }, "wrongRequestHash": func(p *ui188HBCorroborationProof) { p.RequestSHA256 = strings.Repeat("0", 64) }, "originalNonzero": func(p *ui188HBCorroborationProof) { p.OriginalExit = 1 }, "tokenInactive": func(p *ui188HBCorroborationProof) { p.TokenActive = false }, "tokenNotCurrent": func(p *ui188HBCorroborationProof) { p.TokenIdentityCurrent = false }, "noReceipt": func(p *ui188HBCorroborationProof) { p.CanonicalReceiptsMatched = false }, "wrongDBsource": func(p *ui188HBCorroborationProof) { p.DBSource = "fixture" }, "wrongDBowner": func(p *ui188HBCorroborationProof) { p.DB.CurrentStreamID = "wrong" }, "missingDBAssignments": func(p *ui188HBCorroborationProof) { p.DB.Assignments = nil }, "staleProof": func(p *ui188HBCorroborationProof) {
			p.ObservedAtUTC = u.clock.Now().Add(-2 * time.Second).Format(time.RFC3339Nano)
		}, "futureProof": func(p *ui188HBCorroborationProof) {
			p.ObservedAtUTC = u.clock.Now().Add(time.Second).Format(time.RFC3339Nano)
		}, "DBnonzero": func(p *ui188HBCorroborationProof) { p.DBTransaction.OriginalExit = 1 }, "DBnotReadOnly": func(p *ui188HBCorroborationProof) { p.DBTransaction.Isolation = "write" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			copy := valid
			copy.DB.Assignments = append([]ui188HBAssignment{}, valid.DB.Assignments...)
			mutate(&copy)
			if ui188HBProofEqual(copy, request, raw, u.clock.Now()) == nil {
				t.Fatal("independent proof negative accepted")
			}
		})
	}
	for _, mode := range []string{"oldDBFreshObserved", "DBBeforeHeartbeat", "DBBeforeGETEnd", "DBFuture"} {
		t.Run(mode, func(t *testing.T) {
			copy := valid
			switch mode {
			case "oldDBFreshObserved":
				copy.DBTransaction.StartedAtUTC = "2020-01-01T00:00:00Z"
				copy.DBTransaction.EndedAtUTC = "2020-01-01T00:00:01Z"
			case "DBBeforeHeartbeat":
				copy.DBTransaction.StartedAtUTC = u.clock.Now().Add(-time.Second).Format(time.RFC3339Nano)
			case "DBBeforeGETEnd":
				copy.DBTransaction.StartedAtUTC = u.clock.Now().Add(-time.Nanosecond).Format(time.RFC3339Nano)
			case "DBFuture":
				copy.DBTransaction.StartedAtUTC = u.clock.Now().Add(time.Second).Format(time.RFC3339Nano)
				copy.DBTransaction.EndedAtUTC = copy.DBTransaction.StartedAtUTC
			}
			if ui188HBProofEqual(copy, request, raw, u.clock.Now()) == nil {
				t.Fatal("fresh observed timestamp hid stale or future DB transaction")
			}
		})
	}
	t.Run("DBAfterHeartbeatButBeforeGETCompletion", func(t *testing.T) {
		r := request
		r.Readback.AtUTC = u.clock.Now().Add(time.Second).Format(time.RFC3339Nano)
		r.Readback.EndedAtUTC = u.clock.Now().Add(2 * time.Second).Format(time.RFC3339Nano)
		b, _ := json.Marshal(r)
		now := u.clock.Now().Add(3 * time.Second)
		p := ui188HBUnitProof(r, b, now)
		p.DBTransaction.StartedAtUTC = u.clock.Now().Add(1500 * time.Millisecond).Format(time.RFC3339Nano)
		p.DBTransaction.EndedAtUTC = now.Format(time.RFC3339Nano)
		if ui188HBProofEqual(p, r, b, now) == nil {
			t.Fatal("DB observation before actual GET completion accepted")
		}
	})
}

func TestUI188MC12HeartbeatFileReceiptAndCorroborator(t *testing.T) {
	u := ui188HBUnitNew(t, ui188HBUnitReject(), ui188HBUnitGET(ui188HBUnitEmpty()))
	if err := u.tick(""); err != nil {
		t.Fatal(err)
	}
	request := u.requests[0]
	root := t.TempDir()
	directory := filepath.Join(root, "heartbeat-recovery")
	if err := os.Mkdir(directory, 0700); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(request)
	raw = append(raw, '\n')
	proof := ui188HBUnitProof(request, raw, u.clock.Now())
	receiptName := "heartbeat-recovery/assignment-receipt-mc13-bot-a-UNIT_ONLY.json"
	receipt := map[string]any{"case": request.Case, "run_uuid": request.RunUUID, "DB_schema": request.Schema, "service_id": "bot-a", "HTTP_status": 200, "source": "REGULAR_CP_MUTATION_THEN_AUTHENTICATED_ADMIN_GET", "mutation_auto_retries": 0, "after": request.Readback, "method": "DELETE", "path": "/services/bot-a/assignment"}
	receiptRaw, _ := json.Marshal(receipt)
	if err := os.WriteFile(filepath.Join(root, filepath.FromSlash(receiptName)), receiptRaw, 0600); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(receiptRaw)
	proof.CanonicalReceipt.Path = receiptName
	proof.CanonicalReceipt.SHA256 = hex.EncodeToString(digest[:])
	if err := ui188HBReceiptEqual(root, proof, request); err != nil {
		t.Fatal(err)
	}
	proofRaw, _ := json.Marshal(proof)
	name := "proof-mc13-bot-a-1-rejected.json"
	if err := os.WriteFile(filepath.Join(directory, name), proofRaw, 0600); err != nil {
		t.Fatal(err)
	}
	if err := ui188HBFileCorroborator(directory, u.clock)(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if ui188HBFileCorroborator(directory, u.clock)(context.Background(), request) == nil {
		t.Fatal("request evidence overwritten")
	}
	for _, mutation := range []string{"path", "hash", "contents"} {
		t.Run(mutation, func(t *testing.T) {
			p := proof
			if mutation == "path" {
				p.CanonicalReceipt.Path = "../assignment.json"
			} else if mutation == "hash" {
				p.CanonicalReceipt.SHA256 = strings.Repeat("0", 64)
			} else {
				changed := map[string]any{}
				for k, v := range receipt {
					changed[k] = v
				}
				changed["run_uuid"] = "old"
				b, _ := json.Marshal(changed)
				if err := os.WriteFile(filepath.Join(root, filepath.FromSlash(receiptName)), b, 0600); err != nil {
					t.Fatal(err)
				}
				d := sha256.Sum256(b)
				p.CanonicalReceipt.SHA256 = hex.EncodeToString(d[:])
			}
			if ui188HBReceiptEqual(root, p, request) == nil {
				t.Fatal("receipt negative accepted")
			}
		})
	}
}

func TestUI188MC12HeartbeatIndependentEpochAndMalformedStream(t *testing.T) {
	assignment := control.StreamServiceAssignment{StreamID: "11111111-1111-4111-8111-111111111111", ServiceID: "bot-a", ServiceType: "discord_bot", AssignmentRole: "primary", AssignedAt: time.Date(2026, 10, 17, 15, 0, 0, 0, time.UTC)}
	body := ui188HBUnitRuntime("bot-a", "discord_bot", assignment.StreamID, []control.StreamServiceAssignment{assignment})
	u := ui188HBUnitNew(t, ui188HBUnitReject(), ui188HBUnitGET(body))
	if err := u.tick(assignment.StreamID); err != nil {
		t.Fatal(err)
	}
	request := u.requests[0]
	raw, _ := json.Marshal(request)
	proof := ui188HBUnitProof(request, raw, u.clock.Now())
	proof.DB.Assignments = append([]ui188HBAssignment{}, request.Readback.Assignments...)
	proof.DB.Assignments[0].AssignedAt = "2026-10-17T15:00:00.000000Z"
	if err := ui188HBProofEqual(proof, request, raw, u.clock.Now()); err != nil {
		t.Fatal("same instant precision must match", err)
	}
	proof.DB.Assignments[0].AssignedAt = "2026-10-17T15:00:01Z"
	if ui188HBProofEqual(proof, request, raw, u.clock.Now()) == nil {
		t.Fatal("wrong independent DB epoch accepted")
	}
	assignment.StreamID = "not-a-uuid"
	bad := ui188HBUnitRuntime("bot-a", "discord_bot", assignment.StreamID, []control.StreamServiceAssignment{assignment})
	v := ui188HBUnitNew(t, ui188HBUnitReject(), ui188HBUnitGET(bad))
	if v.tick(assignment.StreamID) == nil {
		t.Fatal("malformed stream authority accepted")
	}
}

func TestUI188MC12HeartbeatProofRequiredFields(t *testing.T) {
	u := ui188HBUnitNew(t, ui188HBUnitReject(), ui188HBUnitGET(ui188HBUnitEmpty()))
	if err := u.tick(""); err != nil {
		t.Fatal(err)
	}
	request := u.requests[0]
	raw, _ := json.Marshal(request)
	proof := ui188HBUnitProof(request, raw, u.clock.Now())
	valid, _ := json.Marshal(proof)
	if _, err := ui188HBDecodeProof(valid); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"original_exit", "token_active", "canonical_receipt", "db_transaction", "db", "case", "run_uuid"} {
		t.Run(key, func(t *testing.T) {
			var fields map[string]json.RawMessage
			if err := json.Unmarshal(valid, &fields); err != nil {
				t.Fatal(err)
			}
			delete(fields, key)
			changed, _ := json.Marshal(fields)
			if _, err := ui188HBDecodeProof(changed); err == nil {
				t.Fatal("missing proof field defaulted to success")
			}
		})
	}
}

func TestUI188MC13ScopeBindingsRejectClosedScopes(t *testing.T) {
	root := t.TempDir()
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "cb15-diagnostic-identity.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(root, "cb15-diagnostic-identity.json"), raw, 0600); err != nil {
		t.Fatal(err)
	}
	if !ui188HBContext(root, "cb15", "ui188_cb15_20261018", "03e37a89-9a1b-4e39-aab3-49152b74d0a4") {
		t.Fatal("exact new diagnostic identity rejected")
	}
	for _, value := range [][3]string{
		{"wc14", "ui188_wc14_diagnostic_20261018", "7006d7d2-eb2c-4d4a-ad61-47927db9b9ee"},
		{"mc13", "ui188_mc13_recovery_20261018", "4e7ac320-a685-4d00-b4a7-de0d36720f73"},
		{"md13", "ui188_md13_diagnostic_20261018", "beff4f0f-b1a5-43ca-9f54-ab779befda56"},
		{"hb13", "ui188_hb13_canary_20261018", "37c1f2b9-47fb-42db-8794-043a7302bb59"},
		{"mc12", "ui188_mc12_recovery_20261017", "f50e5cb4-ca18-41ce-8e2e-f200c8ddff22"},
		{"cb15", "ui188_mc13_recovery_20261018", "03e37a89-9a1b-4e39-aab3-49152b74d0a4"},
		{"cb15", "ui188_cb15_20261018", "4e7ac320-a685-4d00-b4a7-de0d36720f73"},
	} {
		if ui188HBContext(root, value[0], value[1], value[2]) {
			t.Fatal("closed or mixed scope accepted")
		}
	}
}

func TestUI188MC12HeartbeatCanaryContextIsFiniteAndSourceBound(t *testing.T) {
	root := t.TempDir()
	valid := func() bool {
		return ui188HBContext(root, "cb15", "ui188_cb15_20261018", "03e37a89-9a1b-4e39-aab3-49152b74d0a4")
	}
	if valid() {
		t.Fatal("missing sealed identity accepted")
	}
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "cb15-diagnostic-identity.json"))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "cb15-diagnostic-identity.json")
	if err = os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	if !valid() {
		t.Fatal("exact parent-authorized diagnostic context rejected")
	}
	if err = os.WriteFile(path, append(raw, byte(' ')), 0600); err != nil {
		t.Fatal(err)
	}
	if valid() {
		t.Fatal("changed sealed identity accepted")
	}
	if err = os.Remove(path); err != nil {
		t.Fatal(err)
	}
	source := filepath.Join(root, "source.json")
	if err = os.WriteFile(source, raw, 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.Symlink(source, path); err != nil {
		t.Fatal(err)
	}
	if valid() {
		t.Fatal("symlink identity accepted")
	}
}

func TestUI188MC12HeartbeatBodyCompletionPrecedesIndependentDB(t *testing.T) {
	u := ui188HBUnitNew(t, ui188HBUnitReject(), ui188HBUnitGET(ui188HBUnitEmpty()))
	start := u.clock.Now()
	u.wire.steps[1].bodyClock = u.clock
	u.wire.steps[1].bodyAdvance = 1500 * time.Millisecond
	if err := u.tick(""); err != nil {
		t.Fatal(err)
	}
	request := u.requests[0]
	ended, err := time.Parse(time.RFC3339Nano, request.Readback.EndedAtUTC)
	if err != nil {
		t.Fatal(err)
	}
	if !ended.Equal(start.Add(1500*time.Millisecond)) || u.exchanges[1].DurationNS != int64(1500*time.Millisecond) {
		t.Fatal("actual GET completion was captured at response headers")
	}
	raw, _ := json.Marshal(request)
	proof := ui188HBUnitProof(request, raw, u.clock.Now())
	proof.DBTransaction.StartedAtUTC = start.Add(500 * time.Millisecond).Format(time.RFC3339Nano)
	if ui188HBProofEqual(proof, request, raw, u.clock.Now()) == nil {
		t.Fatal("DB between headers and actual body completion was accepted")
	}
}
