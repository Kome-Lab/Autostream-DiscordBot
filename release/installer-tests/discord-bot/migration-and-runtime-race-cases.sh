
retry_backup_dir="${preexisting_backup_dir}"
assert_preexisting_backups_unchanged

"${EXTRACTED_ROOT}/install-autostream-discord-bot" > "${WORK_DIR}/migration.out"
[[ $(stat -c '%U:%G:%a' -- /opt/autostream) == "root:root:755" ]] || \
  die "successful migration did not normalize the shared managed parent"
runtime_race_fragment_before="$(systemctl show --property FragmentPath --value "${UNIT}")"
runtime_race_exec_start_before="$(systemctl show --property ExecStart --value "${UNIT}")"
runtime_race_user_before="$(systemctl show --property User --value "${UNIT}")"
runtime_race_pid_before="$(systemctl show --property MainPID --value "${UNIT}")"
runtime_race_enabled_before="$(systemctl is-enabled "${UNIT}" 2>/dev/null || true)"
runtime_sync_precommit_hook=replace_runtime_unit_for_precommit_probe
set +e
sync_managed_runtime_unit
runtime_race_status=$?
set -e
runtime_sync_precommit_hook=""
[[ ${runtime_race_status} -eq 75 ]] || \
  die "runtime precommit race unexpectedly committed"
[[ ${runtime_race_active} == true ]] || \
  die "runtime precommit race did not retain recovery ownership"
[[ $(stat -c '%d:%i' -- "${RUNTIME_UNIT_PATH}") == \
  "${runtime_race_foreign_identity}" ]] || \
  die "precommit race changed the foreign runtime unit inode"
[[ $(sha256sum "${RUNTIME_UNIT_PATH}" | awk 'NR == 1 { print $1 }') == \
  "${runtime_race_foreign_hash}" ]] || \
  die "precommit race changed the foreign runtime unit hash"
[[ $(systemctl show --property FragmentPath --value "${UNIT}") == \
  "${runtime_race_fragment_before}" ]] || \
  die "precommit race changed PID1 FragmentPath"
[[ $(systemctl show --property ExecStart --value "${UNIT}") == \
  "${runtime_race_exec_start_before}" ]] || \
  die "precommit race changed PID1 ExecStart"
[[ $(systemctl show --property User --value "${UNIT}") == \
  "${runtime_race_user_before}" ]] || \
  die "precommit race changed PID1 User"
[[ $(systemctl show --property MainPID --value "${UNIT}") == \
  "${runtime_race_pid_before}" ]] || \
  die "precommit race changed PID1 MainPID"
[[ $(systemctl is-enabled "${UNIT}" 2>/dev/null || true) == \
  "${runtime_race_enabled_before}" ]] || \
  die "precommit race changed the enabled state"
kill -0 "${old_pid}" || die "precommit race stopped the legacy process"
restore_runtime_sync_race || die "could not restore the owned runtime unit after the race probe"
[[ $(sha256sum "${RUNTIME_UNIT_PATH}" | awk 'NR == 1 { print $1 }') == \
  "${legacy_unit_sha256}" ]] || \
  die "race probe did not restore the legacy runtime unit"
sync_managed_runtime_unit
assert_loaded_managed_runtime_unit
readonly RELEASE_DIR="${MANAGED_ROOT}/releases/${VERSION}-${archive_sha256:0:12}"
readonly INSTALL_BACKUP_DIR="${INSTALL_BACKUP_ROOT}/${VERSION}-${archive_sha256:0:12}"
[[ $(readlink -f -- "${MANAGED_ROOT}/current") == "${RELEASE_DIR}" ]] || \
  die "successful migration did not activate the verified release"
[[ $(readlink -f -- "${PUBLIC_BINARY}") == "${RELEASE_DIR}/bin/autostream-discord-bot" ]] || \
  die "canonical public link does not resolve to the verified release"
[[ $(readlink -f -- "${PUBLIC_ALIAS}") == "${RELEASE_DIR}/bin/autostream-discord-bot" ]] || \
  die "public alias does not resolve to the verified release"
[[ -z $(compgen -G "${PUBLIC_BINARY}.rollback-anchor.*" || true) ]] || \
  die "successful migration left a canonical public rollback anchor"
[[ -z $(compgen -G "${PUBLIC_ALIAS}.rollback-anchor.*" || true) ]] || \
  die "successful migration left a compatibility public rollback anchor"
[[ $(sha256sum "${ENV_PATH}" | awk 'NR == 1 { print $1 }') == "${env_before}" ]] || \
  die "successful migration changed the existing environment"
[[ $(sha256sum "${CONFIG_PATH}" | awk 'NR == 1 { print $1 }') == "${config_before}" ]] || \
  die "successful migration changed config.yml"
grep -Fx -- "${LEGACY_BINARY_CONTENT}" \
  "${INSTALL_BACKUP_DIR}/autostream-discord-bot" >/dev/null || \
  die "successful migration did not retain the legacy canonical binary"
grep -Fx -- "${LEGACY_ALIAS_CONTENT}" \
  "${INSTALL_BACKUP_DIR}/discord-bot" >/dev/null || \
  die "successful migration did not retain the legacy alias"
[[ $(stat -c '%U:%G:%a' -- /var/backups/autostream) == "root:root:700" ]] || \
  die "installer backup parent is not root-only"
grep -F -- "sudo systemctl restart ${UNIT}" "${WORK_DIR}/migration.out" >/dev/null || \
  die "active migration did not print the explicit restart command"
[[ $(systemctl show --property MainPID --value "${UNIT}") == "${old_pid}" ]] || \
  die "successful migration replaced the running legacy process"
kill -0 "${old_pid}" || die "successful migration stopped the legacy process"
assert_not_enabled

printf '%s\n' 'intentionally corrupt archive sidecar' > "${ARCHIVE}.sha256"
printf '%s\n' '{"intentionally":"stale"}' > "${ARTIFACTS_DIR}/release-manifest.json"
printf '%s\n' 'intentionally corrupt manifest sidecar' \
  > "${ARTIFACTS_DIR}/release-manifest.json.sha256"
chmod 0644 \
  "${ARCHIVE}.sha256" \
  "${ARTIFACTS_DIR}/release-manifest.json" \
  "${ARTIFACTS_DIR}/release-manifest.json.sha256"
"${EXTRACTED_ROOT}/install-autostream-discord-bot" > "${WORK_DIR}/idempotent.out"
assert_loaded_managed_runtime_unit
[[ $(systemctl show --property MainPID --value "${UNIT}") == "${old_pid}" ]] || \
  die "idempotent reinstall replaced the running legacy process"
[[ $(sha256sum "${ENV_PATH}" | awk 'NR == 1 { print $1 }') == "${env_before}" ]] || \
  die "idempotent reinstall changed the existing environment"
[[ $(sha256sum "${CONFIG_PATH}" | awk 'NR == 1 { print $1 }') == "${config_before}" ]] || \
  die "idempotent reinstall changed config.yml"
assert_not_enabled

chown -h autostream:autostream "${MANAGED_ROOT}/current"
set +e
"${EXTRACTED_ROOT}/install-autostream-discord-bot" \
  > "${WORK_DIR}/malformed-current.out" 2>&1
malformed_current_status=$?
set -e
[[ ${malformed_current_status} -ne 0 ]] || \
  die "installer accepted a non-root-owned managed current link"
grep -F -- "managed current link must be owned by root:root" \
  "${WORK_DIR}/malformed-current.out" >/dev/null || \
  die "malformed current link did not fail closed with the expected message"
chown -h root:root "${MANAGED_ROOT}/current"
[[ $(systemctl show --property MainPID --value "${UNIT}") == "${old_pid}" ]] || \
  die "malformed current validation changed the running legacy process"

(
  exec 7<>"${SHARED_HOST_SETUP_LOCK}"
  flock -n 7 || die "test could not acquire the shared host-setup lock"
  set +e
  "${EXTRACTED_ROOT}/install-autostream-discord-bot" \
    > "${WORK_DIR}/shared-contention.out" 2>&1
  shared_contention_status=$?
  set -e
  [[ ${shared_contention_status} -ne 0 ]] || \
    die "installer ignored shared host-setup lock contention"
)
grep -F -- "another AutoStream installer is provisioning shared host state" \
  "${WORK_DIR}/shared-contention.out" >/dev/null || \
  die "shared host-setup lock contention did not fail with the expected message"
[[ $(systemctl show --property MainPID --value "${UNIT}") == "${old_pid}" ]] || \
  die "shared host-setup lock contention changed the running legacy process"

(
  exec 8>"${TARGET_LOCK}"
  flock -n 8 || die "test could not acquire the updater target lock"
  set +e
  "${EXTRACTED_ROOT}/install-autostream-discord-bot" \
    > "${WORK_DIR}/contention.out" 2>&1
  contention_status=$?
  set -e
  [[ ${contention_status} -ne 0 ]] || die "installer ignored updater lock contention"
)
grep -F -- "another privileged update is already active for ${UNIT}" \
  "${WORK_DIR}/contention.out" >/dev/null || \
  die "lock contention did not fail with the expected message"
[[ $(systemctl show --property MainPID --value "${UNIT}") == "${old_pid}" ]] || \
  die "lock contention changed the running legacy process"

printf '%s\n' "Discord Bot installer integration scenarios passed."
