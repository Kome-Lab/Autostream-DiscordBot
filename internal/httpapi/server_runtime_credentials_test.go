package httpapi

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"github.com/example/autostream-discord-bot/internal/discord"
	"github.com/example/autostream-discord-bot/internal/jobs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSHA256TokenVerifier(t *testing.T) {
	sum := sha256.Sum256([]byte("expected"))
	verifier := TokenVerifier{SHA256Hex: hex.EncodeToString(sum[:])}
	if !verifier.Verify("Bearer expected") {
		t.Fatal("expected token to verify")
	}
	if verifier.Verify("Bearer wrong") {
		t.Fatal("wrong token verified")
	}
}

func TestTokenVerifierFromEnvRejectsControlPanelTokenFallbackInProduction(t *testing.T) {
	t.Setenv("CONTROL_PANEL_TOKEN", "control-panel-token")
	t.Setenv("AUTOSTREAM_ENV", "production")
	verifier := TokenVerifierFromEnv()
	if verifier.Verify("Bearer control-panel-token") {
		t.Fatal("CONTROL_PANEL_TOKEN must not authorize inbound Discord Bot control requests in production")
	}
}

func TestTokenVerifierFromEnvRejectsControlPanelTokenFallbackWhenRuntimeConfigRequired(t *testing.T) {
	t.Setenv("CONTROL_PANEL_TOKEN", "control-panel-token")
	t.Setenv("AUTOSTREAM_REQUIRE_CONTROL_PANEL_RUNTIME_CONFIG", "true")
	verifier := TokenVerifierFromEnv()
	if verifier.Verify("Bearer control-panel-token") {
		t.Fatal("CONTROL_PANEL_TOKEN must not authorize inbound Discord Bot control requests when runtime config is required")
	}
}

func TestTokenVerifierFromEnvRejectsRemovedControlPanelTokenFallbackOutsideProduction(t *testing.T) {
	t.Setenv("CONTROL_PANEL_TOKEN", "control-panel-token")
	verifier := TokenVerifierFromEnv()
	if verifier.Verify("Bearer control-panel-token") {
		t.Fatal("removed CONTROL_PANEL_TOKEN fallback authorized an inbound request")
	}
}

func TestTokenVerifierReadsNodeRuntimeTokenAfterStartup(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yml")
	t.Setenv("AUTOSTREAM_NODE_CONFIG", path)
	t.Setenv("CONTROL_PANEL_TOKEN", "")
	verifier := TokenVerifierFromEnv()
	if verifier.Verify("Bearer runtime-secret") {
		t.Fatal("runtime token should not verify before config exists")
	}
	writeNodeConfigForVerifierTest(t, path, "discord_bot")
	if !verifier.Verify("Bearer runtime-secret") {
		t.Fatal("runtime token should verify after config is written")
	}
}

func TestErrorDoesNotEchoBearerToken(t *testing.T) {
	server := httptest.NewServer(NewServer("discord_bot", jobs.NewManager(&discord.NoopClient{}), TokenVerifier{PlainToken: "secret-token"}))
	defer server.Close()

	req, err := http.NewRequest(http.MethodPost, server.URL+"/jobs/start", strings.NewReader(`{"stream_id":"","job_generation":17,"guild_id":"","voice_channel_id":""}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer secret-token")
	req.Header.Set("Content-Type", "application/json")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(res.Body); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(buf.String(), "secret-token") {
		t.Fatalf("token leaked in response: %s", buf.String())
	}
}

func writeNodeConfigForVerifierTest(t *testing.T, path, nodeType string) {
	t.Helper()
	writeNodeListenerCredentialForVerifierTest(t, path, nodeType, "11")
	body := `panel:
  url: "https://panel.example.jp"
node:
  id: "discord-bot-01"
  name: "Discord Bot 01"
  type: "` + nodeType + `"
listener:
  credential: "node-listener.json"
api:
  host: "discord.example.jp"
  port: 8443
  ssl_enabled: true
auth:
  token_id: "token-id"
  token: "runtime-secret"
`
	if err := os.WriteFile(path, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
}

func writeNodeListenerCredentialForVerifierTest(t *testing.T, configPath, serviceType, revision string) {
	t.Helper()
	credentialDir := filepath.Join(filepath.Dir(configPath), "credentials")
	if err := os.MkdirAll(credentialDir, 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CREDENTIALS_DIRECTORY", credentialDir)
	body := `{"schema_version":2,"service_type":"` + serviceType + `","bind_address":"127.0.0.1:18083","config_revision":` + revision + `}`
	if err := os.WriteFile(filepath.Join(credentialDir, "node-listener.json"), []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
}
