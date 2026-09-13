
readonly VALID_ARTIFACT_MANIFEST="${WORK_DIR}/artifact-manifest.valid.json"
install -o root -g root -m 0600 \
  "${EXTRACTED_ROOT}/artifact-manifest.json" \
  "${VALID_ARTIFACT_MANIFEST}"

assert_preflight_rejection_did_not_mutate_host() {
  if id autostream >/dev/null 2>&1 || getent group autostream >/dev/null 2>&1; then
    die "preflight rejection mutated the service account"
  fi
  for path in \
    "${UNIT_PATH}" \
    "${PUBLIC_BINARY}" \
    "${PUBLIC_ALIAS}" \
    "${ENV_PATH}" \
    "${CONFIG_DIR}" \
    "${STATE_DIR}" \
    "${MANAGED_ROOT}" \
    "${INSTALL_BACKUP_ROOT}" \
    "${TARGET_LOCK}" \
    "${SHARED_HOST_SETUP_LOCK}"; do
    [[ ! -e ${path} && ! -L ${path} ]] || \
      die "preflight rejection mutated ${path}"
  done
}

rm -f -- "${EXTRACTED_ROOT}/artifact-manifest.json"
package_fixture_archive
set +e
"${EXTRACTED_ROOT}/install-autostream-discord-bot" \
  > "${WORK_DIR}/missing-artifact-manifest.out" 2>&1
missing_manifest_status=$?
set -e
[[ ${missing_manifest_status} -ne 0 ]] || \
  die "installer accepted an archive without artifact-manifest.json"
grep -F -- "required release file is missing or unsafe: artifact-manifest.json" \
  "${WORK_DIR}/missing-artifact-manifest.out" >/dev/null || \
  die "missing artifact-manifest.json did not fail with the expected message"
assert_preflight_rejection_did_not_mutate_host

install -o root -g root -m 0644 \
  "${VALID_ARTIFACT_MANIFEST}" \
  "${EXTRACTED_ROOT}/artifact-manifest.json"
jq '.platform.arch = "arm64"' \
  "${VALID_ARTIFACT_MANIFEST}" > "${EXTRACTED_ROOT}/artifact-manifest.json"
package_fixture_archive
set +e
"${EXTRACTED_ROOT}/install-autostream-discord-bot" \
  > "${WORK_DIR}/wrong-artifact-arch.out" 2>&1
wrong_arch_status=$?
set -e
[[ ${wrong_arch_status} -ne 0 ]] || \
  die "installer accepted artifact-manifest.json for the wrong architecture"
grep -F -- "artifact-manifest.json does not describe this exact artifact" \
  "${WORK_DIR}/wrong-artifact-arch.out" >/dev/null || \
  die "wrong artifact architecture did not fail with the expected message"
assert_preflight_rejection_did_not_mutate_host

jq '.commit = ("f" * 40)' \
  "${VALID_ARTIFACT_MANIFEST}" > "${EXTRACTED_ROOT}/artifact-manifest.json"
package_fixture_archive
set +e
"${EXTRACTED_ROOT}/install-autostream-discord-bot" \
  > "${WORK_DIR}/wrong-artifact-commit.out" 2>&1
wrong_commit_status=$?
set -e
[[ ${wrong_commit_status} -ne 0 ]] || \
  die "installer accepted a binary commit that differs from artifact-manifest.json"
grep -F -- "Discord Bot binary commit does not match artifact-manifest.json" \
  "${WORK_DIR}/wrong-artifact-commit.out" >/dev/null || \
  die "binary commit mismatch did not fail with the expected message"
assert_preflight_rejection_did_not_mutate_host

jq '.build_date = "2026-01-02T00:00:00Z"' \
  "${VALID_ARTIFACT_MANIFEST}" > "${EXTRACTED_ROOT}/artifact-manifest.json"
package_fixture_archive
set +e
"${EXTRACTED_ROOT}/install-autostream-discord-bot" \
  > "${WORK_DIR}/wrong-artifact-build-date.out" 2>&1
wrong_build_date_status=$?
set -e
[[ ${wrong_build_date_status} -ne 0 ]] || \
  die "installer accepted a binary build date that differs from artifact-manifest.json"
grep -F -- "Discord Bot binary build date does not match artifact-manifest.json" \
  "${WORK_DIR}/wrong-artifact-build-date.out" >/dev/null || \
  die "binary build date mismatch did not fail with the expected message"
assert_preflight_rejection_did_not_mutate_host

install -o root -g root -m 0644 \
  "${VALID_ARTIFACT_MANIFEST}" \
  "${EXTRACTED_ROOT}/artifact-manifest.json"
package_fixture_archive

printf '%s\n' 'canonical archive alias probe' \
  > "${ARTIFACTS_DIR}/discord-bot-canonical-alias-file"
tar -C "${ARTIFACTS_DIR}" -czf "${ARCHIVE}" \
  "${ARTIFACT_ID}" \
  --transform="s#^discord-bot-canonical-alias-file\$#${ARTIFACT_ID}#" \
  discord-bot-canonical-alias-file
rm -f -- "${ARTIFACTS_DIR}/discord-bot-canonical-alias-file"
set +e
"${EXTRACTED_ROOT}/install-autostream-discord-bot" \
  > "${WORK_DIR}/duplicate-archive-entry.out" 2>&1
duplicate_archive_status=$?
set -e
[[ ${duplicate_archive_status} -ne 0 ]] || \
  die "installer accepted an archive with a duplicate canonical path"
grep -F -- "release archive contains duplicate paths" \
  "${WORK_DIR}/duplicate-archive-entry.out" >/dev/null || \
  die "duplicate archive path did not fail at the archive layout boundary"
assert_preflight_rejection_did_not_mutate_host
package_fixture_archive

archive_sha256="$(sha256sum "${ARCHIVE}" | awk 'NR == 1 { print $1 }')"
[[ ${archive_sha256} =~ ^[0-9a-f]{64}$ ]] || \
  die "fixture archive digest is invalid"

printf '%s\n' \
  '#!/bin/sh' \
  "printf '%s\n' reached > '${WORK_DIR}/mktemp-shim.reached'" \
  'exit 73' > "${WORK_DIR}/failing-mktemp"
chmod 0755 "${WORK_DIR}/failing-mktemp"
set +e
unshare --mount --propagation private bash -c \
  "mount --bind '${WORK_DIR}/failing-mktemp' /usr/bin/mktemp && '${EXTRACTED_ROOT}/install-autostream-discord-bot'" \
  > "${WORK_DIR}/mktemp-failure.out" 2>&1
mktemp_failure_status=$?
set -e
[[ ${mktemp_failure_status} -eq 73 ]] || die "installer did not preserve the INPUT_STAGE mktemp failure status"
[[ $(< "${WORK_DIR}/mktemp-shim.reached") == "reached" ]] || \
  die "mktemp failure injection did not reach the mounted shim"
[[ ! -e /unpack && ! -L /unpack ]] || die "mktemp failure created a root-level /unpack path"
if id autostream >/dev/null 2>&1 || getent group autostream >/dev/null 2>&1; then
  die "mktemp failure mutated the service account"
fi

unsafe_backup_dir="${INSTALL_BACKUP_ROOT}/${VERSION}-${archive_sha256:0:12}"
unsafe_backup_fifo="${WORK_DIR}/unsafe-backup-fifo"
printf '%s\n' "${LEGACY_BINARY_CONTENT}" > "${PUBLIC_BINARY}"
chmod 0755 "${PUBLIC_BINARY}"
install -d -o root -g root -m 0700 "${unsafe_backup_dir}"
mkfifo -m 0600 "${unsafe_backup_fifo}"
ln -s -- "${unsafe_backup_fifo}" \
  "${unsafe_backup_dir}/autostream-discord-bot"
install -o root -g root -m 0755 /usr/bin/sha256sum \
  "${WORK_DIR}/real-sha256sum"
printf '%s\n' \
  '#!/bin/sh' \
  'for argument in "$@"; do' \
  "  if [ \"\${argument}\" = '${unsafe_backup_dir}/autostream-discord-bot' ]; then" \
  "    : > '${WORK_DIR}/unsafe-backup-was-read'" \
  '    exit 99' \
  '  fi' \
  'done' \
  "exec '${WORK_DIR}/real-sha256sum' \"\$@\"" \
  > "${WORK_DIR}/reject-unsafe-backup-read"
chmod 0755 "${WORK_DIR}/reject-unsafe-backup-read"
set +e
unshare --mount --propagation private bash -c \
  "mount --bind '${WORK_DIR}/reject-unsafe-backup-read' /usr/bin/sha256sum && '${EXTRACTED_ROOT}/install-autostream-discord-bot'" \
  > "${WORK_DIR}/unsafe-backup-type.out" 2>&1
unsafe_backup_status=$?
set -e
[[ ${unsafe_backup_status} -ne 0 ]] || \
  die "unsafe legacy backup symlink unexpectedly passed preflight"
grep -F -- "legacy backup destination conflicts with ${PUBLIC_BINARY}" \
  "${WORK_DIR}/unsafe-backup-type.out" >/dev/null || {
    printf '%s\n' \
      "unsafe legacy backup status=${unsafe_backup_status}" \
      "unsafe legacy backup dir=${unsafe_backup_dir}" \
      "unsafe legacy backup installer output follows:" >&2
    while IFS= read -r unsafe_backup_output_line; do
      printf '  %s\n' "${unsafe_backup_output_line}" >&2
    done < "${WORK_DIR}/unsafe-backup-type.out"
    die "unsafe legacy backup did not fail at its type boundary"
  }
[[ ! -e ${WORK_DIR}/unsafe-backup-was-read ]] || \
  die "unsafe legacy backup was read before type validation"
rm -f -- \
  "${unsafe_backup_dir}/autostream-discord-bot" \
  "${unsafe_backup_fifo}" \
  "${PUBLIC_BINARY}"
rm -rf -- /var/backups/autostream

install -o root -g root -m 0755 /usr/sbin/groupadd \
  "${WORK_DIR}/real-groupadd"
install -o root -g root -m 0755 /usr/sbin/useradd \
  "${WORK_DIR}/real-useradd"
printf '%s\n' \
  '#!/bin/sh' \
  "'${WORK_DIR}/real-groupadd' \"\$@\"" \
  'status=$?' \
  'if [ "${status}" -eq 0 ]; then' \
  "  : > '${WORK_DIR}/groupadd-signal-window-reached'" \
  '  kill -TERM "$PPID"' \
  'fi' \
  'exit "${status}"' \
  > "${WORK_DIR}/signal-after-groupadd"
printf '%s\n' \
  '#!/bin/sh' \
  "'${WORK_DIR}/real-useradd' \"\$@\"" \
  'status=$?' \
  'if [ "${status}" -eq 0 ]; then' \
  "  : > '${WORK_DIR}/useradd-signal-window-reached'" \
  '  kill -TERM "$PPID"' \
  'fi' \
  'exit "${status}"' \
  > "${WORK_DIR}/signal-after-useradd"
chmod 0755 \
  "${WORK_DIR}/signal-after-groupadd" \
  "${WORK_DIR}/signal-after-useradd"
set +e
unshare --mount --propagation private bash -c \
  "mount --bind '${WORK_DIR}/signal-after-groupadd' /usr/sbin/groupadd && \
   '${EXTRACTED_ROOT}/install-autostream-discord-bot'" \
  > "${WORK_DIR}/groupadd-signal-window.out" 2>&1
groupadd_signal_window_status=$?
set -e
[[ ${groupadd_signal_window_status} -eq 143 ]] || \
  die "groupadd signal-window probe did not exit with 143"
[[ -f ${WORK_DIR}/groupadd-signal-window-reached ]] || \
  die "groupadd signal-window probe did not run"
[[ ! -e ${WORK_DIR}/useradd-signal-window-reached ]] || \
  die "groupadd signal-window probe unexpectedly reached useradd"
if id autostream >/dev/null 2>&1 || getent group autostream >/dev/null 2>&1; then
  die "groupadd signal-window rollback left the invocation-created service account"
fi
for path in \
  /opt/autostream \
  /var/lib/autostream \
  /var/backups/autostream \
  /etc/autostream \
  "${MANAGED_ROOT}" \
  "${STATE_DIR}" \
  "${INSTALL_BACKUP_ROOT}" \
  "${PUBLIC_BINARY}" \
  "${PUBLIC_ALIAS}" \
  "${ENV_PATH}" \
  "${UNIT_PATH}"; do
  [[ ! -e ${path} && ! -L ${path} ]] || \
    die "groupadd signal-window rollback left persistent mutation ${path}"
done

"${WORK_DIR}/real-groupadd" --system autostream
useradd_signal_group_record_before="$(getent group autostream)"
[[ -n ${useradd_signal_group_record_before} ]] || \
  die "useradd signal-window fixture could not capture its pre-existing service group"
cp -- /etc/gshadow "${WORK_DIR}/useradd-signal-gshadow.before"
set +e
unshare --mount --propagation private bash -c \
  "mount --bind '${WORK_DIR}/signal-after-useradd' /usr/sbin/useradd && \
   '${EXTRACTED_ROOT}/install-autostream-discord-bot'" \
  > "${WORK_DIR}/useradd-signal-window.out" 2>&1
useradd_signal_window_status=$?
set -e
[[ ${useradd_signal_window_status} -eq 143 ]] || \
  die "useradd signal-window probe did not exit with 143"
[[ -f ${WORK_DIR}/useradd-signal-window-reached ]] || \
  die "useradd signal-window probe did not run"
if id autostream >/dev/null 2>&1; then
  die "useradd signal-window rollback left the invocation-created service account"
fi
if getent passwd autostream-install-rollback >/dev/null 2>&1 ||
  getent group autostream-install-rollback >/dev/null 2>&1; then
  die "useradd signal-window rollback left the reserved rollback login"
fi
getent group autostream >/dev/null || \
  die "useradd signal-window rollback removed the pre-existing service group"
[[ $(getent group autostream) == "${useradd_signal_group_record_before}" ]] || \
  die "useradd signal-window rollback changed the pre-existing service group"
cmp -s -- /etc/gshadow "${WORK_DIR}/useradd-signal-gshadow.before" || \
  die "useradd signal-window rollback changed the pre-existing /etc/gshadow"
for path in \
  /opt/autostream \
  /var/lib/autostream \
  /var/backups/autostream \
  /etc/autostream \
  "${MANAGED_ROOT}" \
  "${STATE_DIR}" \
  "${INSTALL_BACKUP_ROOT}" \
  "${PUBLIC_BINARY}" \
  "${PUBLIC_ALIAS}" \
  "${ENV_PATH}" \
  "${UNIT_PATH}"; do
  [[ ! -e ${path} && ! -L ${path} ]] || \
    die "useradd signal-window rollback left persistent mutation ${path}"
done
groupdel autostream

install -o root -g root -m 0755 /usr/bin/systemctl "${WORK_DIR}/real-systemctl"
printf '%s\n' \
  '#!/bin/sh' \
  'if [ "${1:-}" = "daemon-reload" ] && [ ! -e "'"${WORK_DIR}"'/fresh-late-failure-reached" ]; then' \
  "  : > '${WORK_DIR}/fresh-late-failure-reached'" \
  '  exit 74' \
  'fi' \
  "exec '${WORK_DIR}/real-systemctl' \"\$@\"" \
  > "${WORK_DIR}/fail-first-daemon-reload"
chmod 0755 "${WORK_DIR}/fail-first-daemon-reload"
set +e
unshare --mount --propagation private bash -c \
  "mount --bind '${WORK_DIR}/fail-first-daemon-reload' /usr/bin/systemctl && '${EXTRACTED_ROOT}/install-autostream-discord-bot'" \
  > "${WORK_DIR}/fresh-late-failure.out" 2>&1
fresh_late_failure_status=$?
set -e
[[ ${fresh_late_failure_status} -eq 74 ]] || \
  die "fresh late-failure rollback probe returned an unexpected status"
[[ -f ${WORK_DIR}/fresh-late-failure-reached ]] || \
  die "fresh late-failure rollback probe did not reach daemon-reload"
if id autostream >/dev/null 2>&1 || getent group autostream >/dev/null 2>&1; then
  die "fresh late-failure rollback left the invocation-created service account"
fi
for path in \
  /opt/autostream \
  /var/lib/autostream \
  /var/backups/autostream \
  /etc/autostream \
  "${MANAGED_ROOT}" \
  "${STATE_DIR}" \
  "${INSTALL_BACKUP_ROOT}" \
  "${PUBLIC_BINARY}" \
  "${PUBLIC_ALIAS}" \
  "${ENV_PATH}" \
  "${UNIT_PATH}"; do
  [[ ! -e ${path} && ! -L ${path} ]] || \
    die "fresh late-failure rollback left persistent mutation ${path}"
done
[[ -f ${TARGET_LOCK} && ! -L ${TARGET_LOCK} &&
  $(stat -c '%U:%G:%a' -- "${TARGET_LOCK}") == "root:root:600" ]] || \
  die "fresh late-failure rollback did not retain the safe permanent updater lock"
[[ -f ${SHARED_HOST_SETUP_LOCK} && ! -L ${SHARED_HOST_SETUP_LOCK} &&
  $(stat -c '%U:%G:%a' -- "${SHARED_HOST_SETUP_LOCK}") == "root:root:600" ]] || \
  die "fresh late-failure rollback did not retain the safe shared host-setup lock"
[[ $(stat -c '%U:%G:%a' -- /run/autostream-updater) == "root:root:700" ]] || \
  die "permanent updater lock directory is not root-only after rollback"
