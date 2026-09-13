package security

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func discordPublicSyncScenarioHasRequiredAssertions(integration string) bool {
	const startMarker = "public_sync_status=$?"
	startIndex := strings.Index(integration, startMarker)
	if startIndex < 0 {
		return false
	}
	tail := integration[startIndex:]
	endOffset := strings.Index(tail, "\nset +e\n")
	if endOffset < 0 {
		return false
	}
	scenario := tail[:endOffset]
	cursor := 0
	for _, marker := range []string{
		startMarker,
		`if [[ ${public_sync_status} -ne 74 ]]; then`,
		`public-link sync failure actual status=${public_sync_status}`,
		`public-link sync shim marker=${public_sync_marker_state}`,
		"public-link sync shim argv follows:",
		"public-link sync installer output follows:",
		`die "public-link sync failure injection returned an unexpected status"`,
		`[[ -f ${WORK_DIR}/public-sync-failed ]] || die "public-link sync failure injection did not reach its shim"`,
		`[[ ! -e ${MANAGED_ROOT}/current && ! -L ${MANAGED_ROOT}/current ]]`,
		"assert_legacy_public_paths_unchanged",
		"assert_preexisting_backups_unchanged",
		"assert_shared_managed_parent_unchanged",
		"assert_loaded_legacy_runtime_unit",
		"assert_not_enabled",
	} {
		markerOffset := strings.Index(scenario[cursor:], marker)
		if markerOffset < 0 {
			return false
		}
		cursor += markerOffset + len(marker)
	}
	return true
}

func TestDiscordPublicSyncScenarioGuardRejectsMarkersOutsideScenario(t *testing.T) {
	fixture := `public_sync_status=$?
if [[ ${public_sync_status} -ne 74 ]]; then
	printf '%s\n' "public-link sync failure actual status=${public_sync_status}"
	printf '%s\n' "public-link sync shim marker=${public_sync_marker_state}"
	printf '%s\n' "public-link sync shim argv follows:"
	printf '%s\n' "public-link sync installer output follows:"
  die "public-link sync failure injection returned an unexpected status"
fi
[[ -f ${WORK_DIR}/public-sync-failed ]] || die "public-link sync failure injection did not reach its shim"
[[ ! -e ${MANAGED_ROOT}/current && ! -L ${MANAGED_ROOT}/current ]]
set +e
assert_legacy_public_paths_unchanged
assert_preexisting_backups_unchanged
assert_shared_managed_parent_unchanged
assert_loaded_legacy_runtime_unit
assert_not_enabled
`
	if discordPublicSyncScenarioHasRequiredAssertions(fixture) {
		t.Fatal("public-link sync guard accepted rollback assertions outside its scenario boundary")
	}
}

func TestDiscordBotInstallerPreservesLegacyPublicInodesOnRollback(t *testing.T) {
	root := filepath.Join("..", "..")
	installerBytes, err := os.ReadFile(filepath.Join(root, "release", "install-autostream-discord-bot"))
	if err != nil {
		t.Fatal(err)
	}
	integrationBytes, err := readInstallerScenarioSource(filepath.Join(root, "release", "test-install-autostream-discord-bot-integration.sh"))
	if err != nil {
		t.Fatal(err)
	}
	installer := string(installerBytes)
	integration := string(integrationBytes)

	for _, marker := range []string{
		"declare -A previous_public_identity=()",
		"declare -A previous_public_nlink=()",
		"declare -A public_rollback_anchor=()",
		"declare -A published_public_identity=()",
		"declare -A published_public_target=()",
		"declare -A protected_temporary_path=()",
		"prepare_public_rollback_anchor()",
		`ln -T -- "${path}" "${anchor}"`,
		`temporary_path_identity["${anchor}"]} != "${original_identity}"`,
		`protected_temporary_path["${anchor}"]=true`,
		`published_public_identity["${link_path}"]="${staged_identity}"`,
		`published_public_target["${link_path}"]="${target}"`,
		`$(stat -c '%d:%i' -- "${path}") == "${published_identity}"`,
		`mv -Tf -- "${anchor}" "${path}"`,
		`$(stat -c '%d:%i' -- "${path}") == "${previous_public_identity["${path}"]}"`,
		`$(stat -c '%h' -- "${path}") == "${previous_public_nlink["${path}"]}"`,
		`remove_unchanged_public_rollback_anchor "${path}"`,
		"release_completed_public_rollback_anchors()",
		"public_binary_anchor_observed_identity=",
		"public_alias_anchor_observed_identity=",
		`prepare_public_rollback_anchor "${PUBLIC_BINARY}"`,
		`prepare_public_rollback_anchor "${PUBLIC_ALIAS}"`,
		`legacy canonical public binary and alias must not be hard links to the same inode`,
		"reject_stale_public_rollback_anchors()",
		`stale_anchors=("${path}.rollback-anchor."*)`,
		`reject_stale_public_rollback_anchors "${PUBLIC_BINARY}"`,
		`reject_stale_public_rollback_anchors "${PUBLIC_ALIAS}"`,
		`local published_target="${published_public_target["${path}"]-}"`,
		`$(readlink -- "${path}") == "${published_target}"`,
	} {
		if !strings.Contains(installer, marker) {
			t.Fatalf("installer is missing inode-preserving public rollback marker %q", marker)
		}
	}

	backupBinary := strings.Index(installer, `backup_legacy_binary "${PUBLIC_BINARY}"`)
	backupAlias := strings.Index(installer, `backup_legacy_binary "${PUBLIC_ALIAS}"`)
	anchorBinary := strings.Index(installer, `prepare_public_rollback_anchor "${PUBLIC_BINARY}"`)
	anchorAlias := strings.Index(installer, `prepare_public_rollback_anchor "${PUBLIC_ALIAS}"`)
	anchorSync := strings.Index(installer, "sync -f /usr/local/bin")
	firstPublicCommit := strings.Index(installer, `install_public_link "${PUBLIC_BINARY}" "${PUBLIC_ALIAS}"`)
	if backupBinary < 0 || backupAlias <= backupBinary || anchorBinary <= backupAlias ||
		anchorAlias <= anchorBinary || anchorSync <= anchorAlias || firstPublicCommit <= anchorSync {
		t.Fatal("installer must durably back up both legacy binaries, hard-link both rollback anchors, and sync their directory before public link publication")
	}
	staleBinary := strings.Index(installer, `reject_stale_public_rollback_anchors "${PUBLIC_BINARY}"`)
	staleAlias := strings.Index(installer, `reject_stale_public_rollback_anchors "${PUBLIC_ALIAS}"`)
	if staleBinary < 0 || staleAlias <= staleBinary || backupBinary <= staleAlias {
		t.Fatal("installer must reject stale rollback anchors before creating migration backups")
	}

	removeStart := strings.Index(installer, "remove_unchanged_public_rollback_anchor() {")
	removeEnd := strings.Index(installer, "\n}\n\ncompleted_public_anchor_is_safe_to_remove() {")
	if removeStart < 0 || removeEnd <= removeStart {
		t.Fatal("installer rollback-anchor removal helper is missing")
	}
	removeFunction := installer[removeStart:removeEnd]
	removeRM := strings.Index(removeFunction, `rm -f -- "${anchor}"`)
	removeSync := strings.Index(removeFunction, `sync -f "${sync_parent}"`)
	removeForget := strings.Index(removeFunction, `forget_temporary_path "${anchor}"`)
	if removeRM < 0 || removeSync <= removeRM || removeForget <= removeSync {
		t.Fatal("installer must retain rollback-anchor tracking until its directory unlink is durable")
	}

	releaseStart := strings.Index(installer, "release_completed_public_rollback_anchors() {")
	releaseEndOffset := -1
	if releaseStart >= 0 {
		releaseEndOffset = strings.Index(installer[releaseStart:], "\n}\n\nrestore_public_state() {")
	}
	if releaseStart < 0 || releaseEndOffset < 0 {
		t.Fatal("installer completed rollback-anchor finalizer is missing")
	}
	releaseFunction := installer[releaseStart : releaseStart+releaseEndOffset]
	releaseRM := strings.Index(releaseFunction, `rm -f -- "${anchor}"`)
	releaseSync := strings.Index(releaseFunction, `sync -f "${sync_parent}"`)
	releaseForget := strings.Index(releaseFunction, `forget_temporary_path "${anchor}"`)
	if releaseRM < 0 || releaseSync <= releaseRM || releaseForget <= releaseSync {
		t.Fatal("installer must retain completed rollback-anchor tracking until its unlink is durable")
	}

	restoreStart := strings.Index(installer, "restore_public_state() {")
	restoreEndOffset := -1
	if restoreStart >= 0 {
		restoreEndOffset = strings.Index(installer[restoreStart:], "\n}\n\nrestore_unit_state() {")
	}
	if restoreStart < 0 || restoreEndOffset < 0 {
		t.Fatal("installer public rollback helper is missing")
	}
	restoreFunction := installer[restoreStart : restoreStart+restoreEndOffset]
	anchorMove := strings.Index(restoreFunction, `mv -Tf -- "${anchor}" "${path}"`)
	restoreSync := -1
	restoreForget := -1
	if anchorMove >= 0 {
		restoreSync = strings.Index(restoreFunction[anchorMove:], `sync -f "$(dirname -- "${path}")"`)
		restoreForget = strings.Index(restoreFunction[anchorMove:], `forget_temporary_path "${anchor}"`)
	}
	if anchorMove < 0 || restoreSync < 0 || restoreForget <= restoreSync {
		t.Fatal("installer must retain restored rollback-anchor tracking until its rename is durable")
	}

	assertStart := strings.Index(integration, "assert_legacy_public_paths_unchanged() {")
	if assertStart < 0 {
		t.Fatal("could not locate legacy public path rollback assertions")
	}
	assertEnd := strings.Index(integration[assertStart:], "\n}")
	if assertEnd < 0 {
		t.Fatal("could not locate legacy public path rollback assertions")
	}
	assertions := integration[assertStart : assertStart+assertEnd]
	for _, marker := range []string{
		`stat -c '%d:%i:%u:%g:%a:%h' -- "${PUBLIC_BINARY}"`,
		`stat -c '%d:%i:%u:%g:%a:%h' -- "${PUBLIC_ALIAS}"`,
		`compgen -G "${PUBLIC_BINARY}.rollback-anchor.*"`,
		`compgen -G "${PUBLIC_ALIAS}.rollback-anchor.*"`,
	} {
		if !strings.Contains(assertions, marker) {
			t.Fatalf("integration fixture is missing exact inode or rollback-anchor assertion %q", marker)
		}
	}
	if !strings.Contains(integration, `if [ -L '${PUBLIC_ALIAS}' ] && [ \"\$(readlink -- '${PUBLIC_ALIAS}')\" = '${PUBLIC_BINARY}' ]; then`) {
		t.Fatal("public sync failure injection must fire only after the compatibility symlink is published")
	}
}
