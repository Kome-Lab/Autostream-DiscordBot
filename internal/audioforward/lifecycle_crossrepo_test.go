//go:build iu_crossrepo

package audioforward

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	lifecycle "github.com/Kome-Lab/Autostream-Contracts/v3lifecycle"
)

type crossRepositoryFrames struct{ record lifecycle.Record }

func (f *crossRepositoryFrames) Prepare(context.Context, string, []OpusPacket) (lifecycle.Record, error) {
	data, err := json.Marshal(f.record)
	var out lifecycle.Record
	if err == nil {
		err = json.Unmarshal(data, &out)
	}
	return out, err
}

type crossRepositoryState struct {
	Posts, Inspections, RawLines int64
	QueuePending                 int
	ProductionQualified          bool
}

// The assertions below are literal consequences of the frozen G0 boundary
// contract: one append/queue admission, no remote completion, and no replay on
// ACK loss or Inspect. Expected results are not generated from the implementation.
func TestIUWS3CrossRepositoryBotEncoder(t *testing.T) {
	binary, source := os.Getenv("IU_WS3_ER_BINARY"), os.Getenv("IU_WS3_ER_SOURCE")
	if !filepath.IsAbs(binary) || !filepath.IsAbs(source) {
		t.Fatal("explicit fixed-source Encoder test binary required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 75*time.Second)
	t.Cleanup(cancel)
	dir := t.TempDir()
	command := exec.CommandContext(ctx, binary, "-test.run=^TestIUWS3CrossRepositoryEncoderProcess$", "-test.v")
	command.Dir = source
	command.Env = []string{"PATH=/usr/bin:/bin", "IU_WS3_FIXTURE_DIR=" + dir, "IU_WS3_ER_SOURCE=" + source}
	var logs bytes.Buffer
	command.Stdout, command.Stderr = &logs, &logs
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	exited := make(chan error, 1)
	go func() { exited <- command.Wait() }()
	var endpoint string
	for endpoint == "" {
		data, err := os.ReadFile(filepath.Join(dir, "server.url"))
		if err == nil && len(data) != 0 {
			endpoint = string(data)
			break
		}
		select {
		case err = <-exited:
			t.Fatalf("Encoder fixture exited: %v; %s", err, logs.String())
		case <-ctx.Done():
			t.Fatal("Encoder fixture readiness unavailable")
		case <-time.After(20 * time.Millisecond):
		}
	}
	httpClient := &http.Client{Timeout: 5 * time.Second}
	t.Cleanup(func() {
		request, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, endpoint+"/fixture/finish", nil)
		response, err := httpClient.Do(request)
		if err == nil {
			_ = response.Body.Close()
		}
		select {
		case err = <-exited:
			if err != nil {
				t.Errorf("Encoder fixture failed: %v; %s", err, logs.String())
			}
		case <-time.After(10 * time.Second):
			cancel()
			t.Error("Encoder fixture did not exit")
		}
	})
	read := func(path string, out any) {
		t.Helper()
		data, err := os.ReadFile(filepath.Join(source, "internal/lifecycleparticipant/testdata", path))
		if err != nil || json.Unmarshal(data, out) != nil {
			t.Fatalf("fixed input %s unavailable: %v", path, err)
		}
	}
	var owner struct {
		IngressProfile             lifecycle.ComparisonProfile
		OwnerProducer              lifecycle.ProducerRef
		RawResource, QueueResource lifecycle.ResourceRef
		Admission                  lifecycle.Record
	}
	read("owner.json", &owner)
	var frame lifecycle.Record
	read("request.json", &frame)
	if frame.Header.ContractRef.ModuleCommitRef != lifecycleModuleCommit || owner.IngressProfile.Ref.ProfileID != lifecycle.MediaIngress || owner.OwnerProducer.ComponentID != "C04" {
		t.Fatal("fixed Pin/Profile/consumer mismatch")
	}
	frames := &crossRepositoryFrames{record: frame}
	var jobKey, connectionKey lifecycle.AxisKey
	for _, rule := range owner.IngressProfile.AxisRules {
		if rule.Key.Resource == owner.RawResource && rule.Key.Axis == lifecycle.JobAxis {
			jobKey = rule.Key
		}
		if rule.Key.Resource == owner.RawResource && rule.Key.Axis == lifecycle.ConnectionAxis {
			connectionKey = rule.Key
		}
	}
	// Independent, literal expectations for this isolated owner's documented
	// nonsecret correlation-to-work mapping. No response supplies these IDs and
	// no such mapping is copied into the Bot's product implementation.
	queueIDs := map[lifecycle.ID][2]lifecycle.ID{
		"attempt-1":         {"queue-579c5cf3ab1631eaa4fae45ebb0b7456bf197de064841ed82d2a18cbe1e9d6ef", "ingress-579c5cf3ab1631eaa4fae45ebb0b7456bf197de064841ed82d2a18cbe1e9d6ef"},
		"attempt-stale":     {"queue-543a0bd22346f06c0fbf627b4653eaa9e051e2e19e7328d60dcd6f69c6893fbc", "ingress-543a0bd22346f06c0fbf627b4653eaa9e051e2e19e7328d60dcd6f69c6893fbc"},
		"attempt-ack-loss":  {"queue-018529b7425f669a9a6d364aaaaab22f9450b4da6ff5815358a62f49f48ace3a", "ingress-018529b7425f669a9a6d364aaaaab22f9450b4da6ff5815358a62f49f48ace3a"},
		"attempt-false-2xx": {"queue-eab5373fea9b92d72d74d1f2f61e50ab02b6d326daa4f204a983b97ea9cb671b", "ingress-eab5373fea9b92d72d74d1f2f61e50ab02b6d326daa4f204a983b97ea9cb671b"},
	}
	client, err := NewLifecycleClient(LifecycleClientConfig{
		HTTP: httpClient, Frames: frames, RequestProfile: owner.IngressProfile, InspectionProfile: owner.IngressProfile,
		Consumer: owner.OwnerProducer, RawTarget: owner.RawResource, QueueTarget: owner.QueueResource, MaxResponseBytes: 1 << 20,
		QueueAdmissionRef: owner.Admission.Header.RecordKey.RecordID, JobAxisKey: jobKey, ConnectionAxisKey: connectionKey,
		QueueWorkIDs: func(c lifecycle.Correlation) (lifecycle.ID, lifecycle.ID) {
			pair := queueIDs[c.AttemptID]
			return pair[0], pair[1]
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	state := func(t *testing.T) crossRepositoryState {
		t.Helper()
		response, err := httpClient.Get(endpoint + "/fixture/state")
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		var out crossRepositoryState
		if err = json.NewDecoder(io.LimitReader(response.Body, 4096)).Decode(&out); err != nil || out.ProductionQualified {
			t.Fatalf("invalid fixture state: %+v %v", out, err)
		}
		return out
	}
	packet := OpusPacket{SSRC: 1, Sequence: 1, Timestamp: 960, JobGeneration: 1, ConnectionGeneration: 1,
		ReceivedAt: time.Date(2026, 11, 15, 0, 0, 0, 0, time.UTC), Opus: []byte{1, 2, 3}}
	const token = "synthetic-only-not-a-credential"
	if before := state(t); before.Posts != 0 || before.RawLines != 0 || before.QueuePending != 0 {
		t.Fatalf("fixture not empty: %+v", before)
	}
	t.Run("actual_raw_and_queue", func(t *testing.T) {
		out, err := client.Forward(ctx, endpoint+"/normal", "stream-1", "bot-1", token, []OpusPacket{packet})
		if err != nil || !out.QueueAdmitted || out.Assessment.ProductionQualified() || out.Assessment.AuthorizesMutation() {
			t.Fatalf("real local admission not observed: %+v %v", out, err)
		}
		if got := state(t); got.Posts != 1 || got.RawLines != 1 || got.QueuePending != 1 {
			t.Fatalf("expected exactly one product effect: %+v", got)
		}
		before := state(t)
		if _, err = client.Inspect(ctx, endpoint+"/normal", "stream-1", token, out.Request); err != nil {
			t.Fatal(err)
		}
		if got := state(t); got.Posts != before.Posts || got.RawLines != before.RawLines || got.QueuePending != before.QueuePending || got.Inspections != before.Inspections+1 {
			t.Fatalf("Inspect replayed work: %+v -> %+v", before, got)
		}
	})
	t.Run("stale_vector_has_no_product_effect", func(t *testing.T) {
		stale := frames.record
		data, _ := json.Marshal(frame)
		_ = json.Unmarshal(data, &stale)
		stale.Header.Correlation.AttemptID = "attempt-stale"
		stale.Header.RecordKey.RecordID = "vector-stale"
		for i := range stale.Vector.Vector {
			if stale.Vector.Vector[i].Key.Axis == lifecycle.RunAxis {
				other := lifecycle.ID("run-stale")
				stale.Vector.Vector[i].Value.Literal = &other
			}
		}
		frames.record = stale
		before := state(t)
		out, err := client.Forward(ctx, endpoint+"/normal", "stream-1", "bot-1", token, []OpusPacket{packet})
		if err == nil || out.QueueAdmitted {
			t.Fatal("stale vector admitted")
		}
		if got := state(t); got.RawLines != before.RawLines || got.QueuePending != before.QueuePending {
			t.Fatalf("stale input changed product state: %+v -> %+v", before, got)
		}
	})
	t.Run("ack_loss_inspects_same_attempt_without_resend", func(t *testing.T) {
		frames.record = frame
		frames.record.Header.Correlation.AttemptID = "attempt-ack-loss"
		frames.record.Header.RecordKey.RecordID = "vector-ack-loss"
		packet.Sequence = 2
		before := state(t)
		out, err := client.Forward(ctx, endpoint+"/ack-loss", "stream-1", "bot-1", token, []OpusPacket{packet})
		if err == nil || out.Assessment.Kind != lifecycle.Unknown || out.QueueAdmitted {
			t.Fatalf("ACK loss became success: %+v %v", out, err)
		}
		after := state(t)
		if after.Posts != before.Posts+1 || after.RawLines != before.RawLines+1 || after.QueuePending != before.QueuePending+1 {
			t.Fatalf("unexpected dispatch count/effects: %+v -> %+v", before, after)
		}
		if _, err = client.Inspect(ctx, endpoint+"/normal", "stream-1", token, out.Request); err != nil {
			t.Fatal(err)
		}
		if got := state(t); got.Posts != after.Posts || got.RawLines != after.RawLines || got.QueuePending != after.QueuePending || got.Inspections != after.Inspections+1 {
			t.Fatalf("ACK Inspect resent work: %+v -> %+v", after, got)
		}
	})
	t.Run("http_2xx_without_typed_body_is_unknown", func(t *testing.T) {
		frames.record = frame
		frames.record.Header.Correlation.AttemptID = "attempt-false-2xx"
		frames.record.Header.RecordKey.RecordID = "vector-false-2xx"
		packet.Sequence = 3
		before := state(t)
		out, err := client.Forward(ctx, endpoint+"/false-2xx", "stream-1", "bot-1", token, []OpusPacket{packet})
		if err == nil || out.QueueAdmitted {
			t.Fatal("2xx alone became successful admission")
		}
		if got := state(t); got.RawLines != before.RawLines || got.QueuePending != before.QueuePending {
			t.Fatal("false 2xx changed real product state")
		}
	})
}
