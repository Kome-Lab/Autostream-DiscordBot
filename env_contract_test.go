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
	for _, required := range []string{
		"AUTOSTREAM_DISCORD_BOT_PORT=8083",
		"AUTOSTREAM_DISCORD_BOT_CONTAINER_PORT=8080",
	} {
		if !strings.Contains(string(body), required) {
			t.Errorf(".env.example is missing Docker port default %q", required)
		}
	}
	if !strings.Contains(string(body), "AUTOSTREAM_CONFIG_REVISION=1") {
		t.Fatal(".env.example must retain configuration revision 1 as the compatibility default")
	}
	if !strings.Contains(strings.ToLower(string(body)), "root-owned") {
		t.Fatal(".env.example must document the root-owned updater probe config revision")
	}
	if !strings.Contains(string(body), "1024") || !strings.Contains(string(body), "65535") {
		t.Fatal(".env.example must document the supported unprivileged port range")
	}
	if !strings.Contains(string(body), "legacy 127.0.0.1:8080 fallback") {
		t.Fatal(".env.example must document the env-unset legacy port fallback")
	}
}

func TestBaseComposePublishesCanonicalDiscordBotPort(t *testing.T) {
	assertFileContains(t, "docker-compose.yml",
		"AUTOSTREAM_CONFIG_REVISION: ${AUTOSTREAM_CONFIG_REVISION:-1}",
		"AUTOSTREAM_BIND_ADDR: 0.0.0.0:${AUTOSTREAM_DISCORD_BOT_CONTAINER_PORT:-8080}",
		`127.0.0.1:${AUTOSTREAM_DISCORD_BOT_PORT:-8083}:${AUTOSTREAM_DISCORD_BOT_CONTAINER_PORT:-8080}`,
	)
}

func TestDiscordGatewayIntentDocumentationContract(t *testing.T) {
	assertFileContains(t, "internal/discord/client.go",
		"discordgo.IntentsGuildVoiceStates",
		"discordgo.IntentsGuildMessages",
		"discordgo.IntentsMessageContent",
	)
	assertFileContains(t, "README.md",
		"`Guild Voice States`",
		"`Message Content Intent`",
		"VC参加者",
		"チャットだけが空",
	)
	assertFileContains(t, "release/README.install.md",
		"`Guild Voice States`",
		"`Message Content Intent`",
		"VC participant",
		"chat overlay remains empty",
	)
}

func TestProductionComposeReplacesBasePortWithLoopbackPublish(t *testing.T) {
	assertFileContains(t, "docker-compose.prod.yml",
		"AUTOSTREAM_CONFIG_REVISION: ${AUTOSTREAM_CONFIG_REVISION:-1}",
		"AUTOSTREAM_BIND_ADDR: 0.0.0.0:${AUTOSTREAM_DISCORD_BOT_CONTAINER_PORT:-8080}",
		"ports: !override",
		`127.0.0.1:${AUTOSTREAM_DISCORD_BOT_PORT:-8083}:${AUTOSTREAM_DISCORD_BOT_CONTAINER_PORT:-8080}`,
	)
}

func TestLocalComposeKeepsCanonicalContainerAndHostPorts(t *testing.T) {
	assertFileContains(t, "docker-compose.local.yml",
		"AUTOSTREAM_CONFIG_REVISION: ${AUTOSTREAM_CONFIG_REVISION:-1}",
		"AUTOSTREAM_BIND_ADDR: 0.0.0.0:${AUTOSTREAM_DISCORD_BOT_CONTAINER_PORT:-8080}",
		`127.0.0.1:${AUTOSTREAM_DISCORD_BOT_PORT:-8083}:${AUTOSTREAM_DISCORD_BOT_CONTAINER_PORT:-8080}`,
	)
}

func TestSystemdLoadsConfigurableHostBindAddress(t *testing.T) {
	body, err := os.ReadFile("systemd/autostream-discord-bot.service.example")
	if err != nil {
		t.Fatal(err)
	}
	unit := string(body)
	primaryEnv := "EnvironmentFile=/etc/autostream/discord-bot.env"
	managedEnv := "EnvironmentFile=-/opt/autostream/local-executor/ports/discord-bot.env"
	if !strings.Contains(unit, primaryEnv) {
		t.Error("systemd unit must load the configurable bind address from discord-bot.env")
	}
	if !strings.Contains(unit, managedEnv) {
		t.Error("systemd unit must optionally load the Control Panel managed port sidecar")
	}
	if strings.Index(unit, managedEnv) <= strings.Index(unit, primaryEnv) {
		t.Error("managed port sidecar must load after discord-bot.env so its bind address and revision win")
	}
	if !strings.Contains(unit, "AUTOSTREAM_CONFIG_REVISION") {
		t.Error("systemd unit must document the required configuration revision environment value")
	}
	if !strings.Contains(unit, "root-owned") {
		t.Error("systemd unit must document root ownership of the revision environment file")
	}
	if strings.Contains(unit, "8083") {
		t.Error("systemd unit must not hard-code the Discord Bot port")
	}

	assertFileContains(t, "release/README.install.md",
		"AUTOSTREAM_CONFIG_REVISION=1",
		"version, service_id, service_type, and config_revision",
		`PROBE_HOST="${PROBE_HOST:-127.0.0.1}"`,
		"PROBE_HOST='[::1]'",
	)
	assertFileContains(t, "README.md",
		"AUTOSTREAM_CONFIG_REVISION=1",
		"increment it after a configuration change",
		"host/reverse-proxy responsibility",
		"`1024` through `65535`",
		"The production health authority is the host Local Executor.",
		"intentionally omit an in-container `healthcheck`",
		"does not add or repurpose `curl`, `wget`, or another unrelated executable",
		"probes the loopback published port for both `/health` and `/updater/version`",
		"the published port is the health port",
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
