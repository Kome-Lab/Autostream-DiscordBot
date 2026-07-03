package control

import (
	"os"
	"path/filepath"
	"testing"
)

func TestConfigFromEnvUsesNodeConfig(t *testing.T) {
	path := writeNodeConfigForTest(t, "discord_bot")
	t.Setenv("AUTOSTREAM_NODE_CONFIG", path)
	cfg := ConfigFromEnv()
	if cfg.ControlPanelURL != "https://panel.example.jp" || cfg.Token != "runtime-secret" || cfg.ServiceID != "discord-01" || cfg.ServiceName != "Discord 01" || cfg.ServicePublicURL != "https://discord.example.jp:8443" {
		t.Fatalf("unexpected config from node file: %#v", cfg)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("node config should validate: %v", err)
	}
	if got := NodeRuntimeTokenFromEnv(); got != "runtime-secret" {
		t.Fatalf("runtime token = %q", got)
	}
}

func TestConfigFromEnvRejectsWrongNodeType(t *testing.T) {
	path := writeNodeConfigForTest(t, "worker")
	t.Setenv("AUTOSTREAM_NODE_CONFIG", path)
	cfg := ConfigFromEnv()
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected wrong node type to fail validation")
	}
}

func writeNodeConfigForTest(t *testing.T, nodeType string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yml")
	body := `panel:
  url: "https://panel.example.jp"
node:
  id: "discord-01"
  name: "Discord 01"
  type: "` + nodeType + `"
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
	return path
}
