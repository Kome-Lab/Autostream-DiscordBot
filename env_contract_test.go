package discordbot_test

import (
	"os"
	"strings"
	"testing"
)

func TestEnvExampleUsesCanonicalHostBindAddress(t *testing.T) {
	body, err := os.ReadFile(".env.example")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), "AUTOSTREAM_BIND_ADDR=127.0.0.1:8083") {
		t.Fatal(".env.example must bind the host service to 127.0.0.1:8083")
	}
}

func TestBaseComposePublishesCanonicalDiscordBotPort(t *testing.T) {
	assertFileContains(t, "docker-compose.yml",
		"AUTOSTREAM_BIND_ADDR: 0.0.0.0:8080",
		`- "8083:8080"`,
	)
}

func TestProductionComposeReplacesBasePortWithLoopbackPublish(t *testing.T) {
	assertFileContains(t, "docker-compose.prod.yml",
		"AUTOSTREAM_BIND_ADDR: 0.0.0.0:8080",
		"ports: !override",
		`- "127.0.0.1:8083:8080"`,
	)
}

func TestLocalComposeKeepsCanonicalContainerAndHostPorts(t *testing.T) {
	assertFileContains(t, "docker-compose.local.yml",
		"AUTOSTREAM_BIND_ADDR: 0.0.0.0:8080",
		`- "8083:8080"`,
	)
}

func assertFileContains(t *testing.T, path string, required ...string) {
	t.Helper()
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	content := string(body)
	for _, value := range required {
		if !strings.Contains(content, value) {
			t.Errorf("%s is missing %q", path, value)
		}
	}
}
