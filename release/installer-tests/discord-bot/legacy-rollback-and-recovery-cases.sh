
install -o root -g root -m 0755 /usr/bin/sync "${WORK_DIR}/real-sync"
printf '%s\n' \
  '#!/bin/sh' \
  "printf 'argc=%s' \"\$#\" >> '${WORK_DIR}/public-sync.argv'" \
  'for argument in "$@"; do' \
  "  printf '\\t%s' \"\${argument}\" >> '${WORK_DIR}/public-sync.argv'" \
  'done' \
  "printf '\\n' >> '${WORK_DIR}/public-sync.argv'" \
  'if [ "${1:-}" = "-f" ] && [ "${2:-}" = "/usr/local/bin" ]; then' \
  "  if [ -L '${PUBLIC_ALIAS}' ] && [ \"\$(readlink -- '${PUBLIC_ALIAS}')\" = '${PUBLIC_BINARY}' ]; then" \
  "    if [ ! -e '${WORK_DIR}/public-sync-failed' ]; then" \
  "      : > '${WORK_DIR}/public-sync-failed'" \
  '      exit 74' \
  '    fi' \
  '  fi' \
  'fi' \
  "exec '${WORK_DIR}/real-sync' \"\$@\"" \
  > "${WORK_DIR}/fail-public-sync"
chmod 0755 "${WORK_DIR}/fail-public-sync"
set +e
unshare --mount --propagation private bash -c \
  "mount --bind '${WORK_DIR}/fail-public-sync' /usr/bin/sync && '${EXTRACTED_ROOT}/install-autostream-discord-bot'" \
  > "${WORK_DIR}/public-sync-failure.out" 2>&1
public_sync_status=$?
set -e
if [[ ${public_sync_status} -ne 74 ]]; then
  public_sync_marker_state=absent
  if [[ -f ${WORK_DIR}/public-sync-failed &&
    ! -L ${WORK_DIR}/public-sync-failed ]]; then
    public_sync_marker_state=present
  elif [[ -e ${WORK_DIR}/public-sync-failed ||
    -L ${WORK_DIR}/public-sync-failed ]]; then
    public_sync_marker_state=unsafe
  fi
  printf '%s\n' \
    "discord-bot installer integration test: public-link sync failure actual status=${public_sync_status}" >&2
  printf '%s\n' \
    "discord-bot installer integration test: public-link sync shim marker=${public_sync_marker_state}" >&2
  printf '%s\n' \
    'discord-bot installer integration test: public-link sync shim argv follows:' >&2
  if [[ -f ${WORK_DIR}/public-sync.argv && ! -L ${WORK_DIR}/public-sync.argv ]]; then
    while IFS= read -r public_sync_argv_line; do
      printf '  %s\n' "${public_sync_argv_line}" >&2
    done < "${WORK_DIR}/public-sync.argv"
  else
    printf '  %s\n' '<missing or unsafe>' >&2
  fi
  printf '%s\n' \
    'discord-bot installer integration test: public-link sync installer output follows:' >&2
  if [[ -f ${WORK_DIR}/public-sync-failure.out &&
    ! -L ${WORK_DIR}/public-sync-failure.out ]]; then
    while IFS= read -r public_sync_output_line; do
      printf '  %s\n' "${public_sync_output_line}" >&2
    done < "${WORK_DIR}/public-sync-failure.out"
  else
    printf '  %s\n' '<missing or unsafe>' >&2
  fi
  die "public-link sync failure injection returned an unexpected status"
fi
[[ -f ${WORK_DIR}/public-sync-failed ]] || die "public-link sync failure injection did not reach its shim"
[[ ! -e ${MANAGED_ROOT}/current && ! -L ${MANAGED_ROOT}/current ]] || \
  die "public-link sync failure left current activated"
grep -Fx -- "${LEGACY_BINARY_CONTENT}" "${PUBLIC_BINARY}" >/dev/null || \
  die "public-link sync failure changed the legacy canonical binary"
grep -Fx -- "${LEGACY_ALIAS_CONTENT}" "${PUBLIC_ALIAS}" >/dev/null || \
  die "public-link sync failure changed the legacy alias"
[[ $(sha256sum "${ENV_PATH}" | awk 'NR == 1 { print $1 }') == "${env_before}" ]] || \
  die "public-link sync failure changed the existing environment"
[[ $(sha256sum "${CONFIG_PATH}" | awk 'NR == 1 { print $1 }') == "${config_before}" ]] || \
  die "public-link sync failure changed config.yml"
[[ $(sha256sum "${UNIT_PATH}" | awk 'NR == 1 { print $1 }') == "${unit_before}" ]] || \
  die "public-link sync failure did not restore the systemd unit"
assert_legacy_public_paths_unchanged
assert_preexisting_backups_unchanged
assert_shared_managed_parent_unchanged
assert_loaded_legacy_runtime_unit
assert_not_enabled

set +e
unshare --mount --propagation private bash -c \
  "mount -t tmpfs tmpfs /run/systemd && '${EXTRACTED_ROOT}/install-autostream-discord-bot'" \
  > "${WORK_DIR}/failed-install.out" 2>&1
failed_status=$?
set -e
[[ ${failed_status} -ne 0 ]] || die "daemon-reload failure injection unexpectedly succeeded"
[[ ! -e ${MANAGED_ROOT}/current && ! -L ${MANAGED_ROOT}/current ]] || \
  die "failed migration left current activated"
[[ -f ${PUBLIC_BINARY} && ! -L ${PUBLIC_BINARY} ]] || \
  die "failed migration did not restore the legacy canonical binary"
[[ -f ${PUBLIC_ALIAS} && ! -L ${PUBLIC_ALIAS} ]] || \
  die "failed migration did not restore the legacy alias"
grep -Fx -- "${LEGACY_BINARY_CONTENT}" "${PUBLIC_BINARY}" >/dev/null || \
  die "failed migration changed the legacy canonical binary"
grep -Fx -- "${LEGACY_ALIAS_CONTENT}" "${PUBLIC_ALIAS}" >/dev/null || \
  die "failed migration changed the legacy alias"
[[ $(sha256sum "${ENV_PATH}" | awk 'NR == 1 { print $1 }') == "${env_before}" ]] || \
  die "failed migration changed the existing environment"
[[ $(sha256sum "${CONFIG_PATH}" | awk 'NR == 1 { print $1 }') == "${config_before}" ]] || \
  die "failed migration changed config.yml"
[[ $(sha256sum "${UNIT_PATH}" | awk 'NR == 1 { print $1 }') == "${unit_before}" ]] || \
  die "failed migration did not restore the systemd unit"
assert_legacy_public_paths_unchanged
assert_preexisting_backups_unchanged
assert_shared_managed_parent_unchanged
assert_loaded_legacy_runtime_unit
assert_not_enabled

recovery_path="$(
  sed -n \
    's/^install-autostream-discord-bot: root-only recovery evidence preserved at //p' \
    "${WORK_DIR}/failed-install.out" |
    tail -n 1
)"
[[ ${recovery_path} == /var/tmp/autostream-discord-bot-install.* ]] || \
  die "failed rollback did not report a bounded recovery path"
[[ -d ${recovery_path} && ! -L ${recovery_path} ]] || \
  die "reported recovery path is missing or unsafe"
[[ $(stat -c '%U:%G:%a' -- "${recovery_path}") == "root:root:700" ]] || \
  die "recovery path is not root-only"
[[ -f ${recovery_path}/unit.previous && -f ${recovery_path}/recovery-state.txt ]] || \
  die "recovery evidence does not retain the previous unit and baseline metadata"
rm -rf -- "${recovery_path}"
