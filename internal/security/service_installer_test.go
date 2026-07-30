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
		"mount --rbind /etc /mnt/etc-lower",
		"mount --make-rprivate /mnt/etc-lower",
		"mount --rbind /var /mnt/var-lower",
		"mount --make-rprivate /mnt/var-lower",
		"mount --rbind /run /mnt/run-lower",
		"mount --make-rprivate /mnt/run-lower",
		"install -d -o root -g root -m 0755 /mnt/usr-upper",
		"install -d -o root -g root -m 0755 /mnt/usr-upper/local",
		"/mnt/var-upper/lib",
		"/mnt/var-upper/backups",
		"install -d -o root -g root -m 1777 /mnt/var-upper/tmp",
		"install -d -o root -g root -m 0700 /mnt/usr-work",
		"-o nodev,nosuid,lowerdir=/mnt/usr-lower,upperdir=/mnt/usr-upper,workdir=/mnt/usr-work",
		"-o nodev,nosuid,lowerdir=/mnt/etc-lower,upperdir=/mnt/etc-upper,workdir=/mnt/etc-work",
		"-o nodev,nosuid,lowerdir=/mnt/var-lower,upperdir=/mnt/var-upper,workdir=/mnt/var-work",
		"-o nodev,nosuid,lowerdir=/mnt/run-lower,upperdir=/mnt/run-upper,workdir=/mnt/run-work",
		"autostream-discord-bot-installer-test-usr-overlay /usr",
		"autostream-discord-bot-installer-test-etc-overlay /etc",
		"autostream-discord-bot-installer-test-var-overlay /var",
		"autostream-discord-bot-installer-test-run-overlay /run",
		`host_run_systemd_identity="$(stat -c "%d:%i" -- /mnt/run-lower/systemd)"`,
		"mount --rbind /mnt/run-lower/systemd /run/systemd",
		"mount --make-rprivate /run/systemd",
		"host-backed /run/systemd bind mount is not writable",
		"autostream-discord-bot-installer-test-bin /usr/local/bin",
		"autostream-discord-bot-installer-test-opt /opt",
		"mount -t tmpfs -o ro,nodev,nosuid,noexec,mode=0555,uid=0,gid=0",
		"autostream-discord-bot-installer-test-sealed-mnt /mnt",
		`AUTOSTREAM_DISCORD_BOT_INSTALLER_TEST_RUN_SYSTEMD_ID="${host_run_systemd_identity}"`,
		"assert_sealed_scratch_mount()",
		"effective /mnt is not the read-only sealed fixture mount",
		"effective /mnt seal has unsafe metadata",
		"effective /mnt seal unexpectedly accepted a write",
		"isolated /usr overlay mount is missing",
		"isolated /etc overlay mount is missing",
		"isolated /var overlay mount is missing",
		"isolated /run overlay mount is missing",
		"host-backed /run/systemd bind mount is missing",
		"isolated /usr/local/bin mount is missing",
		"isolated /opt mount is missing",
		"could not create an isolated sealed /mnt fixture",
		"could not create an isolated safe /usr fixture",
		"could not create an isolated safe /etc fixture",
		"could not create an isolated safe /etc/systemd fixture",
		"could not create an isolated safe /etc/systemd/system fixture",
		"could not create an isolated safe /usr/local fixture",
		"could not create an isolated safe /usr/local/bin fixture",
		"could not create an isolated safe /opt fixture",
		"could not create an isolated safe /var fixture",
		"could not create an isolated safe /var/lib fixture",
		"could not create an isolated safe /var/backups fixture",
		"could not create an isolated safe /var/tmp fixture",
		"could not create an isolated safe /run fixture",
		"host-backed /run/systemd mount does not match its lower source",
		`legacy_unit_file_state="$(systemctl is-enabled "${UNIT}" 2>/dev/null || true)"`,
		"legacy fixture must begin disabled",
		`readonly RUNTIME_UNIT_PATH="/run/systemd/system/${UNIT}"`,
		"systemd runtime unit directory is unsafe",
		"fixture_paths_owned=false",
		"runtime_unit_owned=false",
		`runtime_unit_identity=""`,
		"runtime_unit_identity_matches=false",
		"fixture_service_start_attempted=false",
		`old_pid_start_time=""`,
		"read_proc_pid_start_time()",
		`stat_tail="${stat_line##*) }"`,
		`start_time="${20}"`,
		`if [[ ${fixture_paths_owned} == true ]]; then`,
		`if [[ ${runtime_unit_owned} == true &&`,
		`if [[ ${fixture_service_start_attempted} == true &&`,
		`"${INSTALL_BACKUP_ROOT}" \`,
		`"${TARGET_LOCK}"; do`,
		"create_runtime_unit_no_clobber()",
		`mktemp "/run/systemd/system/.${UNIT}.legacy.XXXXXXXX"`,
		`ln -- "${runtime_unit_stage}" "${RUNTIME_UNIT_PATH}"`,
		"assert_owned_runtime_unit_identity()",
		"sync_managed_runtime_unit()",
		`mktemp "/run/systemd/system/.${UNIT}.managed.XXXXXXXX"`,
		`mv -Tf -- "${runtime_unit_stage}" "${RUNTIME_UNIT_PATH}"`,
		"assert_loaded_legacy_runtime_unit()",
		"assert_loaded_managed_runtime_unit()",
		`systemctl show --property FragmentPath --value "${UNIT}"`,
		`systemctl show --property ExecStart --value "${UNIT}"`,
		`systemctl show --property User --value "${UNIT}"`,
		"AUTOSTREAM_DISCORD_BOT_INSTALLER_TEST_PREFLIGHT_PROBE",
		"preflight ownership probe unexpectedly passed",
		"preflight conflict probe unexpectedly succeeded",
		"preflight conflict changed the runtime sentinel inode",
		"preflight conflict changed the runtime sentinel hash",
		"preflight conflict changed the runtime sentinel PID",
		"preflight conflict changed the runtime sentinel enabled state",
		`preflight_load_state="$(systemctl show --property LoadState --value "${UNIT}" 2>/dev/null || true)"`,
		"runner already has a loaded ${UNIT}",
		`systemctl is-active --quiet "${UNIT}"`,
		`preflight_enabled_state="$(systemctl is-enabled "${UNIT}" 2>/dev/null || true)"`,
		"runner already has an enabled ${UNIT}",
		`old_pid_start_time="$(read_proc_pid_start_time "${old_pid}")"`,
		`current_pid_start_time="$(read_proc_pid_start_time "${old_pid}" 2>/dev/null || true)"`,
		"cleanup fallback refused a reused PID",
		"local cleanup_failed=false",
		"local cleanup_expected_unit_absent=false",
		"cleanup could not prove runtime unit ownership",
		"cleanup failed to stop ${UNIT}",
		"cleanup failed to remove ${RUNTIME_UNIT_PATH}",
		"cleanup daemon-reload failed",
		"cleanup left ${UNIT} active",
		"cleanup left ${UNIT} loaded",
		`cleanup_load_state="$(systemctl show --property LoadState --value "${UNIT}" 2>/dev/null || true)"`,
		`cleanup_fragment_path="$(systemctl show --property FragmentPath --value "${UNIT}" 2>/dev/null || true)"`,
		`if [[ ${cleanup_failed} == true && ${exit_code} -eq 0 ]]; then`,
		`runtime_sync_precommit_hook=""`,
		`cleanup_runtime_pre_remove_hook=""`,
		"runtime_unit_identity_is_owned()",
		"replace_runtime_unit_for_precommit_probe()",
		"replace_runtime_unit_for_cleanup_probe()",
		"restore_runtime_sync_race()",
		"runtime_race_active=false",
		`"${runtime_sync_precommit_hook}"`,
		"return 75",
		"runtime precommit race unexpectedly committed",
		"precommit race changed the foreign runtime unit inode",
		"precommit race changed the foreign runtime unit hash",
		"precommit race changed PID1 FragmentPath",
		"precommit race changed PID1 ExecStart",
		"precommit race changed PID1 User",
		"precommit race changed PID1 MainPID",
		"AUTOSTREAM_DISCORD_BOT_INSTALLER_TEST_CLEANUP_RACE_PROBE",
		"cleanup runtime race hook failed",
		"cleanup runtime race did not promote a successful exit to failure",
		"cleanup runtime race removed or replaced the foreign inode",
		"cleanup runtime race changed the foreign runtime unit hash",
		"cleanup runtime race recovery did not restore the owned inode",
		"Keep cleanup race enablement semantics equivalent to the foreign probe unit.",
		"preflight conflict changed the runtime sentinel FragmentPath",
		"preflight conflict changed the runtime sentinel ExecStart",
		"preflight conflict changed the runtime sentinel User",
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
	etcLowerIndex := strings.Index(integration, "mount --rbind /etc /mnt/etc-lower")
	etcPrivateIndex := strings.Index(integration, "mount --make-rprivate /mnt/etc-lower")
	varLowerIndex := strings.Index(integration, "mount --rbind /var /mnt/var-lower")
	varPrivateIndex := strings.Index(integration, "mount --make-rprivate /mnt/var-lower")
	runLowerIndex := strings.Index(integration, "mount --rbind /run /mnt/run-lower")
	runPrivateIndex := strings.Index(integration, "mount --make-rprivate /mnt/run-lower")
	upperIndex := strings.Index(integration, "install -d -o root -g root -m 0755 /mnt/usr-upper")
	upperLocalIndex := strings.Index(integration, "install -d -o root -g root -m 0755 /mnt/usr-upper/local")
	workIndex := strings.Index(integration, "install -d -o root -g root -m 0700 /mnt/usr-work")
	varWorkIndex := strings.Index(integration, "install -d -o root -g root -m 0700 /mnt/var-work")
	overlayIndex := strings.Index(integration, "autostream-discord-bot-installer-test-usr-overlay /usr")
	etcOverlayIndex := strings.Index(integration, "autostream-discord-bot-installer-test-etc-overlay /etc")
	varOverlayIndex := strings.Index(integration, "autostream-discord-bot-installer-test-var-overlay /var")
	runOverlayIndex := strings.Index(integration, "autostream-discord-bot-installer-test-run-overlay /run")
	hostSystemdIdentityIndex := strings.Index(
		integration,
		`host_run_systemd_identity="$(stat -c "%d:%i" -- /mnt/run-lower/systemd)"`,
	)
	systemdBindIndex := strings.Index(integration, "mount --rbind /mnt/run-lower/systemd /run/systemd")
	binIndex := strings.Index(integration, "autostream-discord-bot-installer-test-bin /usr/local/bin")
	optIndex := strings.Index(integration, "autostream-discord-bot-installer-test-opt /opt")
	sealIndex := strings.Index(integration, "autostream-discord-bot-installer-test-sealed-mnt /mnt")
	if scratchIndex <= outerStrictIndex ||
		lowerIndex <= scratchIndex ||
		privateIndex <= lowerIndex ||
		etcLowerIndex <= privateIndex ||
		etcPrivateIndex <= etcLowerIndex ||
		varLowerIndex <= etcPrivateIndex ||
		varPrivateIndex <= varLowerIndex ||
		runLowerIndex <= varPrivateIndex ||
		runPrivateIndex <= runLowerIndex ||
		upperIndex <= runPrivateIndex ||
		upperLocalIndex <= upperIndex ||
		workIndex <= upperLocalIndex ||
		varWorkIndex <= workIndex ||
		overlayIndex <= varWorkIndex ||
		etcOverlayIndex <= overlayIndex ||
		varOverlayIndex <= etcOverlayIndex ||
		runOverlayIndex <= varOverlayIndex ||
		hostSystemdIdentityIndex <= runOverlayIndex ||
		systemdBindIndex <= hostSystemdIdentityIndex ||
		binIndex <= systemdBindIndex ||
		optIndex <= systemdBindIndex ||
		sealIndex <= binIndex ||
		sealIndex <= optIndex {
		t.Fatal("installer integration fixture must isolate all mutable roots before mounting child fixture filesystems")
	}
	childExecIndex := strings.Index(
		integration,
		`AUTOSTREAM_DISCORD_BOT_INSTALLER_TEST_RUN_SYSTEMD_ID="${host_run_systemd_identity}"`,
	)
	sealAssertionIndex := strings.Index(integration, "\nassert_sealed_scratch_mount\n")
	runtimeSafetyIndex := strings.Index(integration, "systemd runtime unit directory is unsafe")
	if childExecIndex <= sealIndex ||
		sealAssertionIndex <= childExecIndex ||
		runtimeSafetyIndex <= sealAssertionIndex {
		t.Fatal("fixture must seal scratch aliases before restoring only the host-backed /run/systemd subtree")
	}
	if count := strings.Count(integration, "[Install]\nWantedBy=multi-user.target"); count != 2 {
		t.Fatalf("integration fixture must define two enable-capable but disabled units, got %d", count)
	}
	for _, forbidden := range []string{
		`install -o root -g root -m 0644 "${UNIT_PATH}" "${RUNTIME_UNIT_PATH}"`,
		`rm -f -- "${UNIT_PATH}" "${RUNTIME_UNIT_PATH}"`,
	} {
		if strings.Contains(integration, forbidden) {
			t.Fatalf("installer integration fixture contains unsafe runtime-unit operation %q", forbidden)
		}
	}

	cleanupIndex := strings.Index(integration, "cleanup() {")
	trapIndex := strings.Index(integration, "trap cleanup EXIT")
	if cleanupIndex < 0 || trapIndex <= cleanupIndex {
		t.Fatal("installer integration fixture must define cleanup before installing its EXIT trap")
	}
	cleanup := integration[cleanupIndex:trapIndex]
	serviceGateIndex := strings.Index(cleanup, `if [[ ${fixture_service_start_attempted} == true &&`)
	serviceStopIndex := strings.Index(cleanup, `systemctl stop "${UNIT}"`)
	fallbackIdentityIndex := strings.Index(
		cleanup,
		`current_pid_start_time="$(read_proc_pid_start_time "${old_pid}" 2>/dev/null || true)"`,
	)
	fallbackKillIndex := strings.Index(cleanup, `kill "${old_pid}"`)
	cleanupRaceHookIndex := strings.Index(
		cleanup,
		`"${cleanup_runtime_pre_remove_hook}"`,
	)
	runtimeGateIndex := strings.Index(cleanup, `if [[ ${runtime_unit_owned} == true &&`)
	runtimeRemoveIndex := strings.Index(cleanup, `rm -f -- "${RUNTIME_UNIT_PATH}"`)
	preRemoveIdentityIndex := -1
	if runtimeRemoveIndex >= 0 {
		preRemoveIdentityIndex = strings.LastIndex(
			cleanup[:runtimeRemoveIndex],
			"if ! runtime_unit_identity_is_owned; then",
		)
	}
	pathsGateIndex := strings.Index(cleanup, `if [[ ${fixture_paths_owned} == true ]]; then`)
	unitRemoveIndex := strings.Index(cleanup, `rm -f -- "${UNIT_PATH}"`)
	cleanupReloadIndex := strings.Index(cleanup, `if ! systemctl daemon-reload`)
	finalInactiveIndex := strings.Index(cleanup, `systemctl is-active --quiet "${UNIT}"`)
	finalLoadIndex := strings.Index(
		cleanup,
		`cleanup_load_state="$(systemctl show --property LoadState --value "${UNIT}" 2>/dev/null || true)"`,
	)
	exitPromotionIndex := strings.Index(
		cleanup,
		`if [[ ${cleanup_failed} == true && ${exit_code} -eq 0 ]]; then`,
	)
	cleanupExitIndex := strings.LastIndex(cleanup, `exit "${exit_code}"`)
	if serviceGateIndex < 0 || serviceStopIndex <= serviceGateIndex ||
		fallbackIdentityIndex <= serviceStopIndex || fallbackKillIndex <= fallbackIdentityIndex ||
		runtimeGateIndex < 0 || runtimeRemoveIndex <= runtimeGateIndex ||
		cleanupRaceHookIndex <= fallbackKillIndex ||
		preRemoveIdentityIndex <= cleanupRaceHookIndex ||
		pathsGateIndex < 0 || unitRemoveIndex <= pathsGateIndex ||
		cleanupReloadIndex <= runtimeRemoveIndex ||
		finalInactiveIndex <= cleanupReloadIndex || finalLoadIndex <= finalInactiveIndex ||
		exitPromotionIndex <= finalLoadIndex || cleanupExitIndex <= exitPromotionIndex {
		t.Fatal("fixture cleanup must gate service and path removal on strict fixture ownership")
	}

	cleanupFlagIndex := strings.Index(integration, "fixture_paths_owned=false")
	runtimeFlagIndex := strings.Index(integration, "runtime_unit_owned=false")
	serviceFlagIndex := strings.Index(integration, "fixture_service_start_attempted=false")
	preflightTargetIndex := strings.Index(
		integration,
		`"${INSTALL_BACKUP_ROOT}" \`+"\n"+`  "${TARGET_LOCK}"; do`,
	)
	pathsOwnedIndex := strings.Index(integration, "fixture_paths_owned=true")
	if cleanupFlagIndex < 0 || runtimeFlagIndex < cleanupFlagIndex ||
		serviceFlagIndex < runtimeFlagIndex || cleanupIndex <= serviceFlagIndex ||
		preflightTargetIndex <= trapIndex || pathsOwnedIndex <= preflightTargetIndex {
		t.Fatal("fixture ownership must remain false until every preflight conflict check passes")
	}
	probeGateIndex := strings.Index(
		integration,
		`if [[ ${AUTOSTREAM_DISCORD_BOT_INSTALLER_TEST_PREFLIGHT_PROBE:-} == "1" ]]; then`,
	)
	loadedPreflightIndex := strings.Index(integration, "preflight_load_state=")
	sentinelIndex := strings.Index(integration, "runtime_sentinel_inode_before=")
	probeInvocationIndex := strings.Index(
		integration,
		"AUTOSTREAM_DISCORD_BOT_INSTALLER_TEST_PREFLIGHT_PROBE=1 bash",
	)
	cleanupRaceProbeBranchIndex := strings.Index(
		integration,
		`if [[ ${AUTOSTREAM_DISCORD_BOT_INSTALLER_TEST_CLEANUP_RACE_PROBE:-} == "1" ]]; then`,
	)
	cleanupRaceInvocationIndex := strings.Index(
		integration,
		"AUTOSTREAM_DISCORD_BOT_INSTALLER_TEST_CLEANUP_RACE_PROBE=1",
	)
	sentinelCleanupIndex := strings.Index(
		integration,
		"runtime_unit_owned=false\nruntime_unit_identity=\"\"\nold_pid=\"\"\nold_pid_start_time=\"\"",
	)
	if cleanupRaceProbeBranchIndex <= trapIndex ||
		cleanupRaceProbeBranchIndex >= preflightTargetIndex ||
		loadedPreflightIndex <= preflightTargetIndex ||
		probeGateIndex <= loadedPreflightIndex || pathsOwnedIndex <= probeGateIndex ||
		sentinelIndex <= pathsOwnedIndex ||
		cleanupRaceInvocationIndex <= sentinelIndex ||
		probeInvocationIndex <= cleanupRaceInvocationIndex ||
		sentinelCleanupIndex <= probeInvocationIndex {
		t.Fatal("fixture must prove a nested preflight conflict cannot mutate its running runtime sentinel")
	}

	createShadowIndex := strings.Index(integration, "create_runtime_unit_no_clobber")
	noClobberIndex := strings.Index(integration, `ln -- "${runtime_unit_stage}" "${RUNTIME_UNIT_PATH}"`)
	startIndex := strings.Index(integration, `systemctl start "${UNIT}"`)
	if createShadowIndex < 0 || noClobberIndex <= createShadowIndex || startIndex <= noClobberIndex {
		t.Fatal("legacy runtime shadow must be created atomically without clobbering before service start")
	}
	if count := strings.Count(integration, "assert_loaded_legacy_runtime_unit"); count < 4 {
		t.Fatalf("fixture must verify the loaded legacy unit initially and after both rollback paths, got %d assertions", count-1)
	}
	if count := strings.Count(integration, "assert_loaded_managed_runtime_unit"); count < 3 {
		t.Fatalf("fixture must verify the loaded managed unit after migration and idempotent reinstall, got %d assertions", count-1)
	}
	if count := strings.Count(
		integration,
		`old_pid_start_time="$(read_proc_pid_start_time "${old_pid}")"`,
	); count != 2 {
		t.Fatalf("fixture must record PID start time after both fixture service starts, got %d", count)
	}
	runtimeSyncStart := strings.Index(integration, "sync_managed_runtime_unit() {")
	runtimeSyncEnd := -1
	if runtimeSyncStart >= 0 {
		runtimeSyncEnd = strings.Index(
			integration[runtimeSyncStart:],
			"\n}\n\nassert_loaded_legacy_runtime_unit()",
		)
	}
	if runtimeSyncStart < 0 || runtimeSyncEnd < 0 {
		t.Fatal("managed runtime synchronization helper is missing")
	}
	runtimeSync := integration[runtimeSyncStart : runtimeSyncStart+runtimeSyncEnd]
	runtimeMoveIndex := strings.Index(runtimeSync, `mv -Tf -- "${runtime_unit_stage}" "${RUNTIME_UNIT_PATH}"`)
	precommitIdentityIndex := -1
	if runtimeMoveIndex >= 0 {
		precommitIdentityIndex = strings.LastIndex(
			runtimeSync[:runtimeMoveIndex],
			"runtime_unit_identity_is_owned",
		)
	}
	stageSyncIndex := strings.Index(runtimeSync, `sync -f "${runtime_unit_stage}"`)
	if count := strings.Count(runtimeSync, "assert_owned_runtime_unit_identity"); count != 2 ||
		stageSyncIndex < 0 || precommitIdentityIndex <= stageSyncIndex ||
		runtimeMoveIndex <= precommitIdentityIndex {
		t.Fatal("managed runtime synchronization must revalidate the owned inode immediately before commit")
	}

	migrationIndex := strings.Index(
		integration,
		`"${EXTRACTED_ROOT}/install-autostream-discord-bot" > "${WORK_DIR}/migration.out"`,
	)
	idempotentIndex := strings.Index(
		integration,
		`"${EXTRACTED_ROOT}/install-autostream-discord-bot" > "${WORK_DIR}/idempotent.out"`,
	)
	raceBaselineIndex := strings.Index(integration, "runtime_race_fragment_before=")
	raceRestoreIndex := strings.Index(integration, "restore_runtime_sync_race ||")
	migrationSyncOffset := -1
	if migrationIndex >= 0 {
		migrationSyncOffset = strings.Index(integration[migrationIndex:], "\nsync_managed_runtime_unit\n")
	}
	idempotentAssertionOffset := -1
	if idempotentIndex >= 0 {
		idempotentAssertionOffset = strings.Index(integration[idempotentIndex:], "\nassert_loaded_managed_runtime_unit\n")
	}
	if migrationIndex < 0 || migrationSyncOffset < 0 ||
		raceBaselineIndex <= migrationIndex || raceRestoreIndex <= raceBaselineIndex ||
		idempotentIndex <= migrationIndex+migrationSyncOffset ||
		idempotentAssertionOffset < 0 {
		t.Fatal("fixture must sync and verify the runtime unit after migration and re-verify it after idempotent reinstall")
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
