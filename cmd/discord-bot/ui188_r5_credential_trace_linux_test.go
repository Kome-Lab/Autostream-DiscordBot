//go:build linux && ui188_chain

package main

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/example/autostream-discord-bot/internal/control"
	"github.com/example/autostream-discord-bot/internal/jobs"
)

type ui188CredentialTrace struct {
	mu   sync.Mutex
	key  []byte
	file *os.File
}

func newUI188CredentialTrace(service string) *ui188CredentialTrace {
	key, err := os.ReadFile(filepath.Join(os.Getenv("UI188_EVIDENCE_DIRECTORY"), "scope-equality-key"))
	if err != nil {
		panic("scope equality key unavailable")
	}
	f, err := os.OpenFile(filepath.Join(os.Getenv("UI188_EVIDENCE_DIRECTORY"), "credential-"+service+"-"+os.Getenv("UI188_PROBE_ATTEMPT")+".jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		panic("credential trace unavailable")
	}
	return &ui188CredentialTrace{key: key, file: f}
}
func (d *ui188CredentialTrace) tag(v string) string {
	if v == "" {
		return "empty"
	}
	h := hmac.New(sha256.New, d.key)
	h.Write([]byte(v))
	return hex.EncodeToString(h.Sum(nil))[:16]
}
func (d *ui188CredentialTrace) record(v map[string]any) {
	d.mu.Lock()
	defer d.mu.Unlock()
	v["at"] = time.Now().UTC().Format(time.RFC3339Nano)
	_ = json.NewEncoder(d.file).Encode(v)
}
func (d *ui188CredentialTrace) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.URL.Path != "/services/runtime-secrets/resolve" {
		return http.DefaultTransport.RoundTrip(r)
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}
	r.Body = io.NopCloser(bytes.NewReader(body))
	var fields map[string]string
	_ = json.Unmarshal(body, &fields)
	e := map[string]any{"event": "resolve", "service": d.tag(fields["service_id"]), "secret": d.tag(fields["secret_name"]), "stream": d.tag(fields["stream_id"]), "archive": d.tag(fields["archive_profile_id"])}
	res, err := http.DefaultTransport.RoundTrip(r)
	if err != nil {
		e["transport_error"] = true
		d.record(e)
		return res, err
	}
	e["status"] = res.StatusCode
	if res.StatusCode != 200 {
		b, readErr := io.ReadAll(res.Body)
		if readErr != nil {
			return nil, readErr
		}
		res.Body = io.NopCloser(bytes.NewReader(b))
		var failure struct {
			Code string `json:"code"`
		}
		_ = json.Unmarshal(b, &failure)
		switch failure.Code {
		case "runtime_secret_lease_active", "runtime_secret_not_allowed", "runtime_secret_not_configured", "runtime_secret_unknown", "runtime_secret_transport_insecure", "service_not_assigned_to_token":
			e["code"] = failure.Code
		default:
			e["code"] = "other"
		}
	}
	d.record(e)
	return res, nil
}
func (d *ui188CredentialTrace) snapshot(cfg control.RuntimeConfig, m *jobs.Manager, phase string, failed bool) {
	streams, _ := primaryDiscordAssignmentStreams(cfg)
	p, ok := discordGatewayRuntimeProfile(cfg)
	reason := "explicit_mapping"
	if len(cfg.StreamDiscordConfigs) == 0 && len(streams) > 0 {
		ok = false
		reason = "missing_current_authority"
		bound := m.UI188ProfileDiagnostic()
		id := ""
		for _, a := range cfg.Assignments {
			if streams[a.StreamID] && a.ServiceID == cfg.Service.ServiceID && a.AssignmentRole == "primary" {
				v, found := bound[a.StreamID]
				if !found || v.Epoch != a.AssignedAt.UTC().Format(time.RFC3339Nano) || (id != "" && id != v.ProfileID) {
					id = ""
					break
				}
				id = v.ProfileID
			}
		}
		for _, v := range cfg.Profiles["discord_config"] {
			if id != "" && v.ID == id && profileBelongsToService(v, cfg.Service.ServiceID) {
				p = v
				ok = true
				reason = "retained_same_assignment_epoch"
			}
		}
	}
	name, fp := "", ""
	if ok {
		name = stringConfig(p.Config, "bot_token_secret_name")
		fp = stringConfig(p.Config, "bot_token_fingerprint")
	}
	ref := ""
	if name != "" {
		ref = cfg.Service.ServiceID + "/" + name + "/" + fp
	}
	cached, present := m.UI188CredentialDiagnostic()
	assignments := []map[string]any{}
	for _, a := range cfg.Assignments {
		assignments = append(assignments, map[string]any{"stream_id": a.StreamID, "service_id": a.ServiceID, "role": a.AssignmentRole, "epoch": a.AssignedAt})
	}
	profiles := []map[string]any{}
	for _, v := range cfg.Profiles["discord_config"] {
		profiles = append(profiles, map[string]any{"id": v.ID, "owner_matches": profileBelongsToService(v, cfg.Service.ServiceID), "secret": d.tag(stringConfig(v.Config, "bot_token_secret_name")), "fingerprint": d.tag(stringConfig(v.Config, "bot_token_fingerprint"))})
	}
	d.record(map[string]any{"event": "runtime", "phase": phase, "failed": failed, "assignments": assignments, "mappings": cfg.StreamDiscordConfigs, "selected": len(streams) > 0, "profile_ok": ok, "profile_id": p.ID, "selection_reason": reason, "profiles": profiles, "secret": d.tag(name), "fingerprint": d.tag(fp), "reference": d.tag(ref), "cache_reference": d.tag(cached), "cache_present": present, "cache_hit": ref != "" && ref == cached && present, "current_stream": m.CurrentStreamID()})
}
