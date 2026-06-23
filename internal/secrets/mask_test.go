package secrets

import (
	"strings"
	"testing"
)

func TestMaskURL(t *testing.T) {
	got := MaskURL("rtsp://" + "user:" + "secret" + "@example.com/live")
	expected := "rtsp://" + "user:%2A%2A%2A%2A" + "@example.com/live"
	if got != expected {
		t.Fatalf("unexpected masked URL: %s", got)
	}
}

func TestSanitizeOperationalErrorMasksURLCredentials(t *testing.T) {
	input := `Post "` + "https://" + "user:" + "secret" + "@encoder.example.com/streams/stream-01/audio/opus" + `": connection refused`
	got := SanitizeOperationalError(input, "fallback")
	if got == "fallback" {
		t.Fatalf("expected masked URL rather than fallback: %s", got)
	}
	if got == "" || containsAny(got, []string{"secret@", "user:secret", "encoder_audio_url"}) {
		t.Fatalf("error was not sanitized: %s", got)
	}
}

func TestSanitizeOperationalErrorFallsBackForTokensAndWebhooks(t *testing.T) {
	cases := []string{
		"Authorization Bearer secret-token was rejected",
		"https://discord.com/api/webhooks/123456789012345678/" + "abcdefghijklmnopqrstuvwxyz",
		"https://hooks.slack.com/services/T000/B000/" + "XXXXXXXXXXXXXXXXXXXXXXXX",
	}
	for _, input := range cases {
		if got := SanitizeOperationalError(input, "safe failure"); got != "safe failure" {
			t.Fatalf("expected fallback for %q, got %q", input, got)
		}
	}
}

func containsAny(value string, needles []string) bool {
	for _, needle := range needles {
		if strings.Contains(value, needle) {
			return true
		}
	}
	return false
}
