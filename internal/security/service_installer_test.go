package security

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDiscordBotReleaseShipsManagedServiceInstaller(t *testing.T) {
	root := filepath.Join("..", "..")
	installerPath := filepath.Join(root, "release", "install-autostream-discord-bot")
	installerBytes, err := os.ReadFile(installerPath)
	if err != nil {
		t.Fatal(err)
	}
	installer := string(installerBytes)

	for _, marker := range []string{
		"set -euo pipefail",
		`readonly SERVICE_NAME="discord-bot"`,
		`readonly MANAGED_ROOT="/opt/autostream/discord-bot"`,
		`readonly PUBLIC_BINARY="/usr/local/bin/autostream-discord-bot"`,
		`readonly PUBLIC_ALIAS="/usr/local/bin/discord-bot"`,
		`readonly ENV_DEST="/etc/autostream/discord-bot.env"`,
		`readonly UNIT_DEST="/etc/systemd/system/autostream-discord-bot.service"`,
		`readonly BACKUP_ROOT="${BACKUP_BASE}/install-migrations"`,
		`readonly BACKUP_DIR="${BACKUP_ROOT}/discord-bot"`,
		"sha256sum --check --strict",
		"release-manifest.json",
		".artifact-sha256",
		".version",
		`flock -n 9`,
		"root anchor directory has unsafe write or special mode bits",
		`[[ ${version_first_line} == "autostream-discord-bot ${VERSION}" ]]`,
		`[[ ${managed_version_first_line} == "autostream-discord-bot ${VERSION}" ]]`,
		"root-only recovery evidence preserved at",
		"systemctl daemon-reload",
	} {
		if !strings.Contains(installer, marker) {
			t.Fatalf("service installer is missing %q", marker)
		}
	}

	workflowBytes, err := os.ReadFile(filepath.Join(root, ".github", "workflows", "release-host.yml"))
	if err != nil {
		t.Fatal(err)
	}
	workflow := string(workflowBytes)
	for _, marker := range []string{
		`run: bash -n release/install-autostream-discord-bot`,
		`run: sudo bash release/test-install-autostream-discord-bot-integration.sh`,
		`cp release/install-autostream-discord-bot "${root}/install-autostream-discord-bot"`,
		`chmod 0755 "${root}/install-autostream-discord-bot"`,
		`artifacts/autostream-discord-bot_${{ needs.release-host.outputs.version }}_linux_amd64.tar.gz`,
		`artifacts/autostream-discord-bot_${{ needs.release-host.outputs.version }}_linux_arm64.tar.gz`,
	} {
		if !strings.Contains(workflow, marker) {
			t.Fatalf("host release workflow is missing installer packaging marker %q", marker)
		}
	}

	ciBytes, err := os.ReadFile(filepath.Join(root, ".github", "workflows", "ci.yml"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(ciBytes), "run: bash -n release/install-autostream-discord-bot") {
		t.Fatal("CI must reject a syntactically invalid managed installer")
	}
	if !strings.Contains(string(ciBytes), "run: sudo bash release/test-install-autostream-discord-bot-integration.sh") {
		t.Fatal("CI must execute the production installer integration scenarios")
	}

	integrationBytes, err := os.ReadFile(filepath.Join(root, "release", "test-install-autostream-discord-bot-integration.sh"))
	if err != nil {
		t.Fatal(err)
	}
	integration := string(integrationBytes)
	for _, marker := range []string{
		"unshare --mount --propagation private",
		"root-only recovery evidence preserved at",
		"systemctl show --property MainPID",
		"config.yml",
		"idempotent reinstall",
		"managed current link must be owned by root:root",
		"another privileged update is already active",
		"AUTOSTREAM_DISCORD_BOT_INSTALLER_TEST_MOUNT_NS",
		"autostream-discord-bot-installer-test-scratch /mnt",
		"mount --rbind /usr /mnt/usr-lower",
		"mount --make-rprivate /mnt/usr-lower",
		"install -d -o root -g root -m 0755 /mnt/usr-upper",
		"install -d -o root -g root -m 0755 /mnt/usr-upper/local",
		"install -d -o root -g root -m 0700 /mnt/usr-work",
		"-o nodev,nosuid,lowerdir=/mnt/usr-lower,upperdir=/mnt/usr-upper,workdir=/mnt/usr-work",
		"autostream-discord-bot-installer-test-usr-overlay /usr",
		"autostream-discord-bot-installer-test-bin /usr/local/bin",
		"autostream-discord-bot-installer-test-opt /opt",
		"isolated /mnt scratch mount is missing",
		"isolated /usr overlay mount is missing",
		"isolated /usr/local/bin mount is missing",
		"isolated /opt mount is missing",
		"could not create an isolated safe /mnt fixture",
		"could not create an isolated safe /usr fixture",
		"could not create an isolated safe /usr/local fixture",
		"could not create an isolated safe /usr/local/bin fixture",
		"could not create an isolated safe /opt fixture",
		`legacy_unit_file_state="$(systemctl is-enabled "${UNIT}" 2>/dev/null || true)"`,
		"legacy fixture must begin disabled",
	} {
		if !strings.Contains(integration, marker) {
			t.Fatalf("installer integration fixture is missing scenario marker %q", marker)
		}
	}
	namespaceIndex := strings.Index(
		integration,
		`if [[ ${AUTOSTREAM_DISCORD_BOT_INSTALLER_TEST_MOUNT_NS:-} != "1" ]]; then`,
	)
	outerStrictIndex := strings.Index(
		integration,
		"exec unshare --mount --propagation private bash -c '\n    set -euo pipefail",
	)
	workDirIndex := strings.Index(integration, `WORK_DIR="$(mktemp`)
	if namespaceIndex < 0 ||
		outerStrictIndex <= namespaceIndex ||
		workDirIndex <= outerStrictIndex {
		t.Fatal("installer integration fixture must enter its isolated mount namespace before creating mutable state")
	}
	scratchIndex := strings.Index(integration, "autostream-discord-bot-installer-test-scratch /mnt")
	lowerIndex := strings.Index(integration, "mount --rbind /usr /mnt/usr-lower")
	privateIndex := strings.Index(integration, "mount --make-rprivate /mnt/usr-lower")
	upperIndex := strings.Index(integration, "install -d -o root -g root -m 0755 /mnt/usr-upper")
	upperLocalIndex := strings.Index(integration, "install -d -o root -g root -m 0755 /mnt/usr-upper/local")
	workIndex := strings.Index(integration, "install -d -o root -g root -m 0700 /mnt/usr-work")
	overlayIndex := strings.Index(integration, "autostream-discord-bot-installer-test-usr-overlay /usr")
	binIndex := strings.Index(integration, "autostream-discord-bot-installer-test-bin /usr/local/bin")
	optIndex := strings.Index(integration, "autostream-discord-bot-installer-test-opt /opt")
	if scratchIndex <= outerStrictIndex ||
		lowerIndex <= scratchIndex ||
		privateIndex <= lowerIndex ||
		upperIndex <= privateIndex ||
		upperLocalIndex <= upperIndex ||
		workIndex <= upperLocalIndex ||
		overlayIndex <= workIndex ||
		binIndex <= overlayIndex ||
		optIndex <= overlayIndex {
		t.Fatal("installer integration fixture must overlay an isolated /usr before mounting child fixture filesystems")
	}
	if count := strings.Count(integration, "[Install]\nWantedBy=multi-user.target"); count != 2 {
		t.Fatalf("integration fixture must define two enable-capable but disabled units, got %d", count)
	}

	unitBytes, err := os.ReadFile(filepath.Join(root, "systemd", "autostream-discord-bot.service.example"))
	if err != nil {
		t.Fatal(err)
	}
	unit := string(unitBytes)
	if !strings.Contains(unit, "ExecStart=/usr/local/bin/autostream-discord-bot") {
		t.Fatal("Discord Bot systemd unit must use the stable public binary path")
	}
	if strings.Contains(unit, "ExecStart=/opt/autostream/discord-bot/current/") {
		t.Fatal("Discord Bot systemd unit exposes installer-owned release internals")
	}

	guideBytes, err := os.ReadFile(filepath.Join(root, "release", "README.install.md"))
	if err != nil {
		t.Fatal(err)
	}
	guide := string(guideBytes)
	for _, marker := range []string{
		"sudo install -d -o root -g root -m 0755 /opt/autostream/releases/artifacts",
		"gh attestation verify autostream-discord-bot_vX.Y.Z_linux_amd64.tar.gz",
		"sudo tar --no-same-owner --no-same-permissions -xzf autostream-discord-bot_vX.Y.Z_linux_amd64.tar.gz",
		"sudo ./install-autostream-discord-bot",
		"installer-owned",
	} {
		if !strings.Contains(guide, marker) {
			t.Fatalf("install guide is missing simple installer marker %q", marker)
		}
	}
}
