package jobs

import (
	"errors"
	"testing"
)

func TestUI188R5GatewayAuthorityFencesWaitingOmission(t *testing.T) {
	m := NewManager(&fakeVoice{})
	assert := func(current, mapped map[string]string, present bool, want string, valid bool) {
		t.Helper()
		id, ok := m.GatewayProfileForAssignments(current, mapped, present)
		if id != want || ok != valid {
			t.Fatalf("selection=%q valid=%v", id, ok)
		}
	}
	assert(map[string]string{"s": "e1"}, nil, false, "", false)
	assert(map[string]string{"s": "e1"}, map[string]string{"s": "new"}, true, "new", true)
	assert(map[string]string{"s": "e1"}, nil, false, "new", true)
	assert(map[string]string{"s": "e2"}, nil, false, "", false)
	assert(map[string]string{"s": "e2"}, map[string]string{"s": "new"}, true, "new", true)
	assert(map[string]string{"s": "e2"}, map[string]string{"other": "old"}, true, "", false)
	assert(map[string]string{"s": "e2"}, nil, false, "", false)
	assert(map[string]string{"s": "e2", "other": "e3"}, map[string]string{"s": "new", "other": "old"}, true, "", false)
	assert(map[string]string{"s": "e2", "other": "e3"}, nil, false, "", false)
	assert(map[string]string{"s": "e2", "other": "e3"}, map[string]string{"s": "new", "other": "new"}, true, "new", true)
	assert(map[string]string{"s": "e2", "other": "e3"}, map[string]string{"s": "new"}, true, "", false)
	assert(map[string]string{"s": "e2", "other": "e3"}, nil, false, "", false)
	assert(nil, nil, false, "", false)
	assert(map[string]string{"s": "e2"}, nil, false, "", false)
}

func TestUI188R5CredentialChangesNeverSubstituteCachedToken(t *testing.T) {
	for _, ref := range []string{"service/same-name/new-fingerprint", "service/new-name/same-fingerprint"} {
		t.Run(ref, func(t *testing.T) {
			voice := &ui188GatewayVoice{}
			m := NewManager(voice)
			calls := 0
			if err := m.ReconcileGatewayCredential("service/same-name/old-fingerprint", true, func() (string, error) { calls++; return "old-test-token", nil }); err != nil {
				t.Fatal(err)
			}
			if err := m.ReconcileGatewayCredential(ref, true, func() (string, error) { calls++; return "", errors.New("synthetic active lease conflict") }); err == nil {
				t.Fatal("active lease conflict accepted")
			}
			if voice.selected || voice.token != "" || calls != 2 {
				t.Fatal("failed reference reused old token or stayed connected")
			}
			if err := m.ReconcileGatewayCredential(ref, true, func() (string, error) { calls++; return "new-test-token", nil }); err != nil {
				t.Fatal(err)
			}
			if !voice.selected || voice.token != "new-test-token" || calls != 3 {
				t.Fatal("new credential not applied")
			}
		})
	}
}
