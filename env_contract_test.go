package discordbot_test

import (
	"os"
	"strings"
	"testing"
)

func TestEnvExampleUsesV2NodeConfigContract(t *testing.T) {
	body, err := os.ReadFile(".env.example")
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{
		"AUTOSTREAM_DISCORD_BOT_PORT=8083",
		"AUTOSTREAM_DISCORD_BOT_CONTAINER_PORT=8080",
		"listener.credential: node-listener.json",
		"bind_address and config_revision",
	} {
		if !strings.Contains(string(body), required) {
			t.Errorf(".env.example is missing Docker port default %q", required)
		}
	}
	if !strings.Contains(string(body), "1024") || !strings.Contains(string(body), "65535") {
		t.Fatal(".env.example must document the supported unprivileged port range")
	}
	for _, removed := range []string{"AUTOSTREAM_BIND_ADDR", "AUTOSTREAM_CONFIG_REVISION", "api.bind_host", "DISCORD_BOT_TOKEN", "ENCODER_AUDIO_TOKEN", "ENCODER_RECORDER_TOKEN", "CONTROL_PANEL_TOKEN", "SERVICE_ID"} {
		if strings.Contains(string(body), removed) {
			t.Fatalf(".env.example retains removed runtime key %q", removed)
		}
	}
}

func TestBaseComposePublishesCanonicalDiscordBotPort(t *testing.T) {
	assertFileContains(t, "docker-compose.yml",
		"CREDENTIALS_DIRECTORY: /run/autostream-credentials",
		"source: node-listener",
		"target: /run/autostream-credentials/node-listener.json",
		`"service_type":"discord_bot"`,
		`"config_revision":${AUTOSTREAM_CONFIG_REVISION:?AUTOSTREAM_CONFIG_REVISION is required}`,
		`127.0.0.1:${AUTOSTREAM_DISCORD_BOT_PORT:-8083}:${AUTOSTREAM_DISCORD_BOT_CONTAINER_PORT:-8080}`,
	)
	body, err := os.ReadFile("docker-compose.yml")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(string(body), "${AUTOSTREAM_CONFIG_REVISION:") != 1 || strings.Contains(string(body), "\n      AUTOSTREAM_CONFIG_REVISION:") {
		t.Error("base compose must use the revision only as the node-listener JSON generation input")
	}
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
		"ports: !override",
		`127.0.0.1:${AUTOSTREAM_DISCORD_BOT_PORT:-8083}:${AUTOSTREAM_DISCORD_BOT_CONTAINER_PORT:-8080}`,
	)
}

func TestLocalComposeKeepsCanonicalContainerAndHostPorts(t *testing.T) {
	assertFileContains(t, "docker-compose.local.yml",
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
	listenerCredential := "LoadCredential=node-listener.json:/opt/autostream/local-executor/ports/discord-bot.json"
	if !strings.Contains(unit, primaryEnv) {
		t.Error("systemd unit must load operational settings from discord-bot.env")
	}
	if !strings.Contains(unit, listenerCredential) {
		t.Error("systemd unit must load the Panel-issued listener credential")
	}
	if strings.Contains(unit, "8083") {
		t.Error("systemd unit must not hard-code the Discord Bot port")
	}
	if strings.Contains(unit, "AUTOSTREAM_BIND_ADDR") {
		t.Error("systemd unit retains removed bind environment key")
	}
	for _, path := range []string{"docker-compose.yml", "docker-compose.local.yml", "docker-compose.prod.yml"} {
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		for _, removed := range []string{"AUTOSTREAM_BIND_ADDR", "DISCORD_BOT_TOKEN", "ENCODER_AUDIO_TOKEN", "ENCODER_RECORDER_TOKEN"} {
			if strings.Contains(string(body), removed) {
				t.Errorf("%s retains removed runtime key %q", path, removed)
			}
		}
		if strings.Contains(string(body), "\n      AUTOSTREAM_CONFIG_REVISION:") || (path != "docker-compose.yml" && strings.Contains(string(body), "AUTOSTREAM_CONFIG_REVISION")) {
			t.Errorf("%s injects the removed runtime revision environment key", path)
		}
	}

	assertFileContains(t, "release/README.install.md",
		"node-listener.json",
		"listener.credential",
		"bind_address",
		"config_revision",
		"version, service_id, service_type, and config_revision",
		`PROBE_HOST="${PROBE_HOST:-127.0.0.1}"`,
		"PROBE_HOST='[::1]'",
	)
	assertFileContains(t, "README.md",
		"node-listener.json",
		"listener.credential",
		"bind_address",
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
