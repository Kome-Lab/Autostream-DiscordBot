package security

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDiscordBotInstallerTransactionsPrivilegedHostSetup(t *testing.T) {
	root := filepath.Join("..", "..")
	installerBytes, err := os.ReadFile(filepath.Join(root, "release", "install-autostream-discord-bot"))
	if err != nil {
		t.Fatal(err)
	}
	installer := string(installerBytes)

	groupValidation := strings.Index(installer, `autostream_group_gid="$(getent group autostream | awk -F: 'NR == 1 { print $3 }')"`)
	userCreation := strings.Index(installer, `useradd --system --gid "${autostream_group_gid}"`)
	if groupValidation < 0 || userCreation < 0 || groupValidation > userCreation {
		t.Fatal("installer must validate the named autostream group numeric GID before user creation")
	}
	if !strings.Contains(installer, `[[ $(id -g autostream) == "${autostream_group_gid}" ]]`) {
		t.Fatal("installer must verify the service user's numeric primary GID")
	}

	for _, marker := range []string{
		"rollback_created_autostream_account()",
		"rollback_journaled_directories()",
		"rollback_created_release()",
		"restore_existing_state_directory()",
		"register_temporary_path()",
		"create_registered_temporary_path()",
		"create_registered_symlink_path()",
		"INPUT_STAGE is the single temporary-path journal exception",
		"input_stage_is_owned()",
		"INPUT_STAGE_IDENTITY",
		"restore_legacy_backup_state()",
		"created_autostream_user=false",
		"created_autostream_group=false",
		"preexisting_autostream_group_record",
		`readonly AUTOSTREAM_USER_ROLLBACK_LOGIN="autostream-install-rollback"`,
		"prepare_autostream_user_rollback_login()",
		"local_account_member_fields_are_clear()",
		"local_account_database_matches_digests()",
		"restore_created_autostream_user_login()",
		"remove_created_autostream_user_preserving_group()",
		`user_record_prefix="${BASH_REMATCH[1]}"`,
		`user_record_suffix="${BASH_REMATCH[2]}"`,
		`usermod --login "${AUTOSTREAM_USER_ROLLBACK_LOGIN}" autostream`,
		`userdel "${AUTOSTREAM_USER_ROLLBACK_LOGIN}"`,
		`usermod --login autostream "${AUTOSTREAM_USER_ROLLBACK_LOGIN}"`,
		`$(getent group autostream 2>/dev/null || true) == "${expected_group_record}"`,
		"release_created=false",
		"state_directory_mutation_started=false",
		"backup_previous_kind",
		"backup_created_identity",
		"ensure_permanent_lock_path_atomically()",
		`readonly SHARED_HOST_SETUP_LOCK="/run/autostream-updater/.autostream-runtime-host-setup.lock"`,
		`ensure_permanent_lock_path_atomically "${SHARED_HOST_SETUP_LOCK}"`,
		`exec 8<>"${SHARED_HOST_SETUP_LOCK}"`,
		`flock -n 8`,
		"another AutoStream installer is provisioning shared host state",
		"shared host-setup lock identity changed after acquisition",
		`ln -- "${lock_create_stage}" "${path}"`,
		`ensure_permanent_lock_path_atomically "${TARGET_LOCK}"`,
		`exec 9<>"${TARGET_LOCK}"`,
		`-f /proc/self/fd/9`,
		`$(stat -Lc '%U:%G:%a' -- /proc/self/fd/9) == "root:root:600"`,
		"updater target lock identity changed",
		"permanent updater lock",
		"durable recovery backup",
	} {
		if !strings.Contains(installer, marker) {
			t.Fatalf("installer is missing privileged transaction marker %q", marker)
		}
	}
	if strings.Contains(installer, `exec 9>"${TARGET_LOCK}"`) {
		t.Fatal("installer must not truncate the production updater lock")
	}
	if strings.Contains(installer, `rm -f -- "${TARGET_LOCK}"`) {
		t.Fatal("installer must never unlink the permanent production updater lock")
	}
	if strings.Contains(installer, `stat -Lc '%F:%U:%G:%a' -- /proc/self/fd/`) {
		t.Fatal("installer must not compare locale- and size-dependent stat file-type labels")
	}
	if strings.Contains(installer, "usermod --gid") ||
		strings.Contains(installer, "usermod --home") {
		t.Fatal("service-account rollback must not mutate the created user's GID or home")
	}
	if strings.Contains(installer, "for (index =") {
		t.Fatal("installer awk must not use the reserved index function name as a loop variable")
	}
	if strings.Count(
		installer,
		`members[member_index] == service_login || members[member_index] == rollback_login`,
	) < 2 ||
		!strings.Contains(
			installer,
			`admins[admin_index] == service_login || admins[admin_index] == rollback_login`,
		) {
		t.Fatal("service-account transaction must reject both login names in local group and gshadow member fields")
	}
	restoreStart := strings.Index(installer, "restore_created_autostream_user_login() {")
	restoreEnd := strings.Index(installer[restoreStart:], "\n}\n\nremove_created_autostream_user_preserving_group()")
	if restoreStart < 0 || restoreEnd < 0 {
		t.Fatal("could not locate service-account login restoration")
	}
	restore := installer[restoreStart : restoreStart+restoreEnd]
	restoreRename := strings.Index(
		restore,
		`usermod --login autostream "${AUTOSTREAM_USER_ROLLBACK_LOGIN}"`,
	)
	restoreDigestCheck := strings.Index(
		restore,
		"local_account_database_matches_digests",
	)
	if restoreRename < 0 || restoreDigestCheck <= restoreRename {
		t.Fatal("service-account restoration must rename the login back before checking group database digests")
	}
	removeStart := strings.Index(installer, "remove_created_autostream_user_preserving_group() {")
	removeEnd := strings.Index(installer[removeStart:], "\n}\n\nrollback_created_autostream_account()")
	if removeStart < 0 || removeEnd < 0 {
		t.Fatal("could not locate invocation-created service-account removal")
	}
	remove := installer[removeStart : removeStart+removeEnd]
	groupDigestSnapshot := strings.Index(remove, "sha256sum -- /etc/group")
	gshadowDigestSnapshot := strings.Index(remove, "sha256sum -- /etc/gshadow")
	memberFieldCheck := strings.Index(remove, "local_account_member_fields_are_clear || return 1")
	renameLogin := strings.Index(
		remove,
		`usermod --login "${AUTOSTREAM_USER_ROLLBACK_LOGIN}" autostream`,
	)
	if groupDigestSnapshot < 0 || gshadowDigestSnapshot < 0 ||
		memberFieldCheck < 0 || renameLogin < 0 ||
		groupDigestSnapshot > memberFieldCheck ||
		gshadowDigestSnapshot > memberFieldCheck ||
		memberFieldCheck > renameLogin {
		t.Fatal("service-account removal must snapshot local group databases and reject member references before renaming")
	}
	if strings.Count(remove, "local_account_database_matches_digests") < 3 {
		t.Fatal("service-account removal must verify local group database digests before rename, after rename, and after userdel")
	}
	postRenameDigestCheck := strings.Index(
		remove[renameLogin:],
		"local_account_database_matches_digests",
	)
	userDelete := strings.Index(
		remove,
		`userdel "${AUTOSTREAM_USER_ROLLBACK_LOGIN}"`,
	)
	postDeleteDigestCheck := strings.Index(
		remove[userDelete:],
		"local_account_database_matches_digests",
	)
	if postRenameDigestCheck < 0 || userDelete <= renameLogin || postDeleteDigestCheck < 0 {
		t.Fatal("service-account removal must preserve group and gshadow contents across rename and deletion")
	}
	sharedLockIndex := strings.Index(installer, "flock -n 8")
	firstJournaledAnchorIndex := strings.Index(installer, "ensure_root_anchor_directory /usr\n")
	if sharedLockIndex < 0 || firstJournaledAnchorIndex <= sharedLockIndex {
		t.Fatal("installer must acquire the shared host-setup lock before journaled host mutations")
	}
	backupTypeValidationIndex := strings.Index(
		installer,
		`[[ -f ${backup_path} && ! -L ${backup_path} &&`,
	)
	backupDigestIndex := strings.Index(
		installer,
		`backup_digest="$(sha256sum -- "${backup_path}" | awk 'NR == 1 { print $1 }')"`,
	)
	if backupTypeValidationIndex < 0 ||
		backupDigestIndex <= backupTypeValidationIndex {
		t.Fatal("installer must validate a pre-existing legacy backup before reading it")
	}
	managedStart := strings.Index(installer, "ensure_managed_directory() {")
	privateStart := strings.Index(installer, "ensure_private_root_directory() {")
	if managedStart < 0 || privateStart <= managedStart {
		t.Fatal("installer managed-directory helper is missing")
	}
	managedHelper := installer[managedStart:privateStart]
	if !strings.Contains(managedHelper, `install_journaled_directory "${path}" root root 0755`) ||
		strings.Contains(managedHelper, "\n    return\n") {
		t.Fatal("installer must normalize safe pre-existing managed directories to mode 0755")
	}
}

func TestDiscordBotInstallerClosesSignalJournalWindows(t *testing.T) {
	root := filepath.Join("..", "..")
	installerBytes, err := os.ReadFile(filepath.Join(root, "release", "install-autostream-discord-bot"))
	if err != nil {
		t.Fatal(err)
	}
	installer := string(installerBytes)

	for _, marker := range []string{
		"cleanup_in_progress=false",
		"signal_transaction_active=false",
		"deferred_termination_status=0",
		"handle_installer_signal()",
		"begin_installer_signal_transaction()",
		"finish_installer_signal_transaction()",
		`if [[ ${cleanup_in_progress} == true ]]; then`,
		`useradd --system --gid "${autostream_group_gid}"`,
		`created_autostream_group_record="$(getent group autostream)"`,
		`created_autostream_user_record="$(getent passwd autostream)"`,
		`local created_identity_variable="${5-}"`,
		`printf -v "${created_identity_variable}" '%s'`,
		`if [[ ${rollback_incomplete} == true && ${status} -eq 0 ]]; then`,
		`create_registered_symlink_path "${target}" "${public_link_next}"`,
		`create_registered_symlink_path "${RELEASE_DIR}" "${current_next}"`,
	} {
		if !strings.Contains(installer, marker) {
			t.Fatalf("installer is missing signal-safe journal marker %q", marker)
		}
	}
	if count := strings.Count(installer, `trap '' HUP INT TERM`); count != 3 {
		t.Fatalf("installer must ignore termination only in the cleanup handler paths; got %d sites", count)
	}
	if strings.Count(installer, "begin_installer_signal_transaction") < 12 ||
		strings.Count(installer, "finish_installer_signal_transaction") < 12 {
		t.Fatal("installer is missing deferred-signal transactions around privileged mutations")
	}

	cleanupStart := strings.Index(installer, "cleanup() {")
	cleanupEnd := strings.Index(installer[cleanupStart:], "\n}\ntrap cleanup EXIT")
	if cleanupStart < 0 || cleanupEnd < 0 {
		t.Fatal("could not locate installer cleanup")
	}
	cleanup := installer[cleanupStart : cleanupStart+cleanupEnd]
	cleanupIgnore := strings.Index(cleanup, `trap '' HUP INT TERM`)
	cleanupRollback := strings.Index(cleanup, "rollback_activation")
	if cleanupIgnore < 0 || cleanupRollback <= cleanupIgnore {
		t.Fatal("cleanup must ignore a second terminating signal before rollback begins")
	}
	enclosingIfConditions := func(marker string) []string {
		t.Helper()
		var conditions []string
		pendingCondition := ""
		for _, line := range strings.Split(cleanup, "\n") {
			trimmed := strings.TrimSpace(line)
			if strings.Contains(trimmed, marker) {
				return append([]string(nil), conditions...)
			}
			if pendingCondition != "" {
				pendingCondition += " " + trimmed
				if strings.HasSuffix(trimmed, "then") {
					conditions = append(conditions, pendingCondition)
					pendingCondition = ""
				}
				continue
			}
			if strings.HasPrefix(trimmed, "elif ") {
				if len(conditions) == 0 {
					t.Fatalf("cleanup has an unmatched elif before %q", marker)
				}
				conditions = conditions[:len(conditions)-1]
				pendingCondition = trimmed
				if strings.HasSuffix(trimmed, "then") {
					conditions = append(conditions, pendingCondition)
					pendingCondition = ""
				}
				continue
			}
			if strings.HasPrefix(trimmed, "if ") {
				pendingCondition = trimmed
				if strings.HasSuffix(trimmed, "then") {
					conditions = append(conditions, pendingCondition)
					pendingCondition = ""
				}
				continue
			}
			if trimmed == "fi" {
				if len(conditions) == 0 {
					t.Fatalf("cleanup has an unmatched fi before %q", marker)
				}
				conditions = conditions[:len(conditions)-1]
			}
		}
		t.Fatalf("cleanup operation is missing %q", marker)
		return nil
	}
	directoryRollbackConditions := strings.Join(enclosingIfConditions("rollback_journaled_directories"), "\n")
	if !strings.Contains(directoryRollbackConditions, "${status} -ne 0") ||
		!strings.Contains(directoryRollbackConditions, "${installation_complete} != true") ||
		strings.Contains(directoryRollbackConditions, "setup_rollback_safe") {
		t.Fatal("cleanup must attempt inode-guarded journaled-directory restoration after every failed incomplete install")
	}
	assertSetupRollbackGate := func(operation string) {
		t.Helper()
		conditions := strings.Join(enclosingIfConditions(operation), "\n")
		if !strings.Contains(conditions, "setup_rollback_safe") {
			t.Fatalf("cleanup operation %q must retain its shared setup-safety gate", operation)
		}
	}
	assertSetupRollbackGate("rollback_created_release")
	assertSetupRollbackGate("rollback_created_autostream_account")

	assertOrdered := func(name, scope string, markers ...string) {
		t.Helper()
		offset := 0
		for _, marker := range markers {
			index := strings.Index(scope[offset:], marker)
			if index < 0 {
				t.Fatalf("%s is missing ordered marker %q", name, marker)
			}
			offset += index + len(marker)
		}
	}

	journalStart := strings.Index(installer, "journal_directory_before_mutation() {")
	journalEnd := strings.Index(installer[journalStart:], "\n}\n\nrecord_journaled_directory_creation()")
	if journalStart < 0 || journalEnd < 0 {
		t.Fatal("could not locate directory journal publication")
	}
	directoryJournal := installer[journalStart : journalStart+journalEnd]
	assertOrdered(
		"directory journal",
		directoryJournal,
		`previous_identity="$(stat -c '%d:%i' -- "${path}")"`,
		`previous_uid="$(stat -c '%u' -- "${path}")"`,
		`previous_gid="$(stat -c '%g' -- "${path}")"`,
		`previous_mode="$(stat -c '%a' -- "${path}")"`,
		"begin_installer_signal_transaction",
		`journaled_directory_recorded["${path}"]=true`,
		`journaled_directory_order+=("${path}")`,
		`journaled_directory_previous_kind["${path}"]="${previous_kind}"`,
		`journaled_directory_previous_identity["${path}"]="${previous_identity}"`,
		`journaled_directory_previous_uid["${path}"]="${previous_uid}"`,
		`journaled_directory_previous_gid["${path}"]="${previous_gid}"`,
		`journaled_directory_previous_mode["${path}"]="${previous_mode}"`,
		"finish_installer_signal_transaction",
	)

	groupStart := strings.Index(installer, "\nif ! getent group autostream")
	groupEnd := strings.Index(installer[groupStart:], "\nautostream_group_gid=")
	if groupStart < 0 || groupEnd < 0 {
		t.Fatal("could not locate autostream group provisioning")
	}
	rollbackLoginPreflight := strings.Index(
		installer,
		"\nprepare_autostream_user_rollback_login ||",
	)
	if rollbackLoginPreflight < 0 || rollbackLoginPreflight > groupStart {
		t.Fatal("installer must reserve the rollback login before account mutation")
	}
	groupProvision := installer[groupStart : groupStart+groupEnd]
	assertOrdered(
		"group provisioning",
		groupProvision,
		"begin_installer_signal_transaction",
		"groupadd --system autostream",
		"created_autostream_group=true",
		`created_autostream_group_record="$(getent group autostream)"`,
		"finish_installer_signal_transaction",
	)

	userStart := strings.Index(installer, "\nif ! id autostream")
	userEnd := strings.Index(installer[userStart:], "\n[[ $(id -u autostream)")
	if userStart < 0 || userEnd < 0 {
		t.Fatal("could not locate autostream user provisioning")
	}
	userProvision := installer[userStart : userStart+userEnd]
	assertOrdered(
		"user provisioning",
		userProvision,
		"local_account_member_fields_are_clear",
		"begin_installer_signal_transaction",
		`useradd --system --gid "${autostream_group_gid}"`,
		"created_autostream_user=true",
		`created_autostream_user_record="$(getent passwd autostream)"`,
		"finish_installer_signal_transaction",
	)

	integrationBytes, err := readInstallerScenarioSource(filepath.Join(
		root,
		"release",
		"test-install-autostream-discord-bot-integration.sh",
	))
	if err != nil {
		t.Fatal(err)
	}
	integration := string(integrationBytes)
	for _, marker := range []string{
		"groupadd signal-window probe did not run",
		"groupadd signal-window probe did not exit with 143",
		"useradd signal-window probe did not run",
		"useradd signal-window probe did not exit with 143",
		"groupadd signal-window rollback left the invocation-created service account",
		"useradd signal-window rollback left the invocation-created service account",
		"useradd signal-window rollback left the reserved rollback login",
		"useradd signal-window rollback changed the pre-existing service group",
		"useradd signal-window rollback changed the pre-existing /etc/gshadow",
	} {
		if !strings.Contains(integration, marker) {
			t.Fatalf("integration fixture is missing signal-window marker %q", marker)
		}
	}
}
