#!/bin/bash
set -euo pipefail

umask 077
export PATH=/usr/sbin:/usr/bin:/sbin:/bin
export LC_ALL=C

die() {
  printf 'discord-bot installer integration test: %s\n' "$*" >&2
  exit 1
}

assert_not_enabled() {
  systemctl is-enabled --quiet "${UNIT}" &&
    die "installer unexpectedly enabled ${UNIT}"
  return 0
}

[[ ${EUID} -eq 0 ]] || die "must run as root"
[[ $(uname -m) == "x86_64" ]] || die "this integration fixture requires an amd64 Linux runner"
command -v mkfifo >/dev/null 2>&1 || die "mkfifo is required"

if [[ ${AUTOSTREAM_DISCORD_BOT_INSTALLER_TEST_MOUNT_NS:-} != "1" ]]; then
  exec unshare --mount --propagation private bash -c '
    set -euo pipefail
    mount -t tmpfs -o nodev,nosuid,mode=0755,uid=0,gid=0 \
      autostream-discord-bot-installer-test-scratch /mnt
    install -d -o root -g root -m 0755 /mnt/usr-lower
    mount --rbind /usr /mnt/usr-lower
    mount --make-rprivate /mnt/usr-lower
    install -d -o root -g root -m 0755 /mnt/etc-lower
    mount --rbind /etc /mnt/etc-lower
    mount --make-rprivate /mnt/etc-lower
    install -d -o root -g root -m 0755 /mnt/var-lower
    mount --rbind /var /mnt/var-lower
    mount --make-rprivate /mnt/var-lower
    install -d -o root -g root -m 0755 /mnt/run-lower
    mount --rbind /run /mnt/run-lower
    mount --make-rprivate /mnt/run-lower
    install -d -o root -g root -m 0755 /mnt/usr-upper
    install -d -o root -g root -m 0755 /mnt/usr-upper/local
    install -d -o root -g root -m 0755 \
      /mnt/etc-upper \
      /mnt/etc-upper/systemd \
      /mnt/etc-upper/systemd/system \
      /mnt/var-upper \
      /mnt/var-upper/lib \
      /mnt/var-upper/backups \
      /mnt/run-upper
    install -d -o root -g root -m 1777 /mnt/var-upper/tmp
    install -d -o root -g root -m 0700 /mnt/usr-work /mnt/etc-work
    install -d -o root -g root -m 0700 /mnt/var-work /mnt/run-work
    mount -t overlay \
      -o nodev,nosuid,lowerdir=/mnt/usr-lower,upperdir=/mnt/usr-upper,workdir=/mnt/usr-work \
      autostream-discord-bot-installer-test-usr-overlay /usr
    mount -t overlay \
      -o nodev,nosuid,lowerdir=/mnt/etc-lower,upperdir=/mnt/etc-upper,workdir=/mnt/etc-work \
      autostream-discord-bot-installer-test-etc-overlay /etc
    mount -t overlay \
      -o nodev,nosuid,lowerdir=/mnt/var-lower,upperdir=/mnt/var-upper,workdir=/mnt/var-work \
      autostream-discord-bot-installer-test-var-overlay /var
    mount -t overlay \
      -o nodev,nosuid,lowerdir=/mnt/run-lower,upperdir=/mnt/run-upper,workdir=/mnt/run-work \
      autostream-discord-bot-installer-test-run-overlay /run
    host_run_systemd_identity="$(stat -c "%d:%i" -- /mnt/run-lower/systemd)"
    mount --rbind /mnt/run-lower/systemd /run/systemd
    mount --make-rprivate /run/systemd
    [[ $(stat -c "%d:%i" -- /run/systemd) == "${host_run_systemd_identity}" ]]
    mount -t tmpfs -o nodev,nosuid,mode=0755,uid=0,gid=0 \
      autostream-discord-bot-installer-test-bin /usr/local/bin
    mount -t tmpfs -o nodev,nosuid,mode=0755,uid=0,gid=0 \
      autostream-discord-bot-installer-test-opt /opt
    mount -t tmpfs -o ro,nodev,nosuid,noexec,mode=0555,uid=0,gid=0 \
      autostream-discord-bot-installer-test-sealed-mnt /mnt
    exec env \
      AUTOSTREAM_DISCORD_BOT_INSTALLER_TEST_MOUNT_NS=1 \
      AUTOSTREAM_DISCORD_BOT_INSTALLER_TEST_RUN_SYSTEMD_ID="${host_run_systemd_identity}" \
      bash "$1"
  ' autostream-discord-bot-installer-test-mount "$0"
fi

assert_sealed_scratch_mount() {
  local probe="/mnt/.autostream-installer-write-probe"

  awk '
    $5 == "/mnt" {
      has_ro = 0
      has_nodev = 0
      has_nosuid = 0
      has_noexec = 0
      option_count = split($6, options, ",")
      for (option = 1; option <= option_count; option++) {
        has_ro = has_ro || options[option] == "ro"
        has_nodev = has_nodev || options[option] == "nodev"
        has_nosuid = has_nosuid || options[option] == "nosuid"
        has_noexec = has_noexec || options[option] == "noexec"
      }
      if (!has_ro || !has_nodev || !has_nosuid || !has_noexec) {
        next
      }
      for (field = 7; field <= NF; field++) {
        if ($field == "-" &&
            $(field + 1) == "tmpfs" &&
            $(field + 2) == "autostream-discord-bot-installer-test-sealed-mnt") {
          found = 1
        }
      }
    }
    END { exit found ? 0 : 1 }
  ' /proc/self/mountinfo || die "effective /mnt is not the read-only sealed fixture mount"
  [[ $(stat -f -c '%T' -- /mnt) == "tmpfs" ]] || \
    die "effective /mnt is not backed by the sealed tmpfs"
  [[ $(stat -c '%U:%G:%a' -- /mnt) == "root:root:555" ]] || \
    die "effective /mnt seal has unsafe metadata"
  if touch -- "${probe}" 2>/dev/null; then
    rm -f -- "${probe}"
    die "effective /mnt seal unexpectedly accepted a write"
  fi
}

assert_sealed_scratch_mount
grep -Eq ' /usr .* - overlay autostream-discord-bot-installer-test-usr-overlay ' \
  /proc/self/mountinfo || die "isolated /usr overlay mount is missing"
grep -Eq ' /etc .* - overlay autostream-discord-bot-installer-test-etc-overlay ' \
  /proc/self/mountinfo || die "isolated /etc overlay mount is missing"
grep -Eq ' /var .* - overlay autostream-discord-bot-installer-test-var-overlay ' \
  /proc/self/mountinfo || die "isolated /var overlay mount is missing"
grep -Eq ' /run .* - overlay autostream-discord-bot-installer-test-run-overlay ' \
  /proc/self/mountinfo || die "isolated /run overlay mount is missing"
awk '$5 == "/run/systemd" { found=1 } END { exit found ? 0 : 1 }' \
  /proc/self/mountinfo || die "host-backed /run/systemd bind mount is missing"
awk '$5 == "/run/systemd" && $6 ~ /^rw(,|$)/ { found=1 } END { exit found ? 0 : 1 }' \
  /proc/self/mountinfo || die "host-backed /run/systemd bind mount is not writable"
grep -Eq ' /usr/local/bin .* - tmpfs autostream-discord-bot-installer-test-bin ' \
  /proc/self/mountinfo || die "isolated /usr/local/bin mount is missing"
grep -Eq ' /opt .* - tmpfs autostream-discord-bot-installer-test-opt ' \
  /proc/self/mountinfo || die "isolated /opt mount is missing"
[[ $(stat -c '%U:%G:%a' -- /mnt) == "root:root:555" ]] || \
  die "could not create an isolated sealed /mnt fixture"
[[ $(stat -c '%U:%G:%a' -- /usr) == "root:root:755" ]] || \
  die "could not create an isolated safe /usr fixture"
[[ $(stat -c '%U:%G:%a' -- /etc) == "root:root:755" ]] || \
  die "could not create an isolated safe /etc fixture"
[[ $(stat -c '%U:%G:%a' -- /etc/systemd) == "root:root:755" ]] || \
  die "could not create an isolated safe /etc/systemd fixture"
[[ $(stat -c '%U:%G:%a' -- /etc/systemd/system) == "root:root:755" ]] || \
  die "could not create an isolated safe /etc/systemd/system fixture"
[[ $(stat -c '%U:%G:%a' -- /usr/local) == "root:root:755" ]] || \
  die "could not create an isolated safe /usr/local fixture"
[[ $(stat -c '%U:%G:%a' -- /usr/local/bin) == "root:root:755" ]] || \
  die "could not create an isolated safe /usr/local/bin fixture"
[[ $(stat -c '%U:%G:%a' -- /opt) == "root:root:755" ]] || \
  die "could not create an isolated safe /opt fixture"
[[ $(stat -c '%U:%G:%a' -- /var) == "root:root:755" ]] || \
  die "could not create an isolated safe /var fixture"
[[ $(stat -c '%U:%G:%a' -- /var/lib) == "root:root:755" ]] || \
  die "could not create an isolated safe /var/lib fixture"
[[ $(stat -c '%U:%G:%a' -- /var/backups) == "root:root:755" ]] || \
  die "could not create an isolated safe /var/backups fixture"
[[ $(stat -c '%U:%G:%a' -- /var/tmp) == "root:root:1777" ]] || \
  die "could not create an isolated safe /var/tmp fixture"
[[ $(stat -c '%U:%G:%a' -- /run) == "root:root:755" ]] || \
  die "could not create an isolated safe /run fixture"
[[ $(stat -c '%d:%i' -- /run/systemd) == \
  "${AUTOSTREAM_DISCORD_BOT_INSTALLER_TEST_RUN_SYSTEMD_ID:-}" ]] || \
  die "host-backed /run/systemd mount does not match its lower source"

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
[[ ${SCRIPT_DIR} == /* && -d ${SCRIPT_DIR} ]] || die "could not resolve the fixture directory"
readonly SCRIPT_DIR
readonly INSTALLER_SOURCE="${SCRIPT_DIR}/install-autostream-discord-bot"
readonly VERSION="v9.9.9"
readonly ARTIFACT_COMMIT="0123456789abcdef0123456789abcdef01234567"
readonly ARTIFACT_BUILD_DATE="2026-01-01T00:00:00Z"
readonly ARTIFACT_ID="autostream-discord-bot_${VERSION}_linux_amd64"
WORK_DIR="$(mktemp -d /var/tmp/autostream-discord-bot-installer-test.XXXXXXXX)"
[[ ${WORK_DIR} == /var/tmp/autostream-discord-bot-installer-test.* &&
  -d ${WORK_DIR} && ! -L ${WORK_DIR} &&
  $(readlink -f -- "${WORK_DIR}") == "${WORK_DIR}" &&
  $(stat -c '%U:%G:%a' -- "${WORK_DIR}") == "root:root:700" ]] || \
  die "could not create a safe fixture work directory"
readonly WORK_DIR
readonly ARTIFACTS_DIR="${WORK_DIR}/artifacts"
readonly EXTRACTED_ROOT="${ARTIFACTS_DIR}/${ARTIFACT_ID}"
readonly ARCHIVE="${ARTIFACTS_DIR}/${ARTIFACT_ID}.tar.gz"
readonly UNIT="autostream-discord-bot.service"
readonly UNIT_PATH="/etc/systemd/system/${UNIT}"
readonly RUNTIME_UNIT_PATH="/run/systemd/system/${UNIT}"
[[ -d /run/systemd/system && ! -L /run/systemd/system &&
  $(readlink -f -- /run/systemd/system) == "/run/systemd/system" &&
  $(stat -c '%U:%G:%a' -- /run/systemd/system) == "root:root:755" ]] || \
  die "systemd runtime unit directory is unsafe"
readonly PUBLIC_BINARY="/usr/local/bin/autostream-discord-bot"
readonly PUBLIC_ALIAS="/usr/local/bin/discord-bot"
readonly ENV_PATH="/etc/autostream/discord-bot.env"
readonly CONFIG_DIR="/etc/autostream-discord-bot"
readonly CONFIG_PATH="${CONFIG_DIR}/config.yml"
readonly STATE_DIR="/var/lib/autostream/discord-bot"
readonly MANAGED_ROOT="/opt/autostream/discord-bot"
readonly INSTALL_BACKUP_ROOT="/var/backups/autostream/install-migrations/discord-bot"
TARGET_LOCK_ID="$(printf '%s' "${UNIT}" | sha256sum | awk 'NR == 1 { print substr($1, 1, 12) }')"
[[ ${TARGET_LOCK_ID} =~ ^[0-9a-f]{12}$ ]] || die "could not derive the updater target lock ID"
readonly TARGET_LOCK_ID
readonly TARGET_LOCK="/run/autostream-updater/.autostream-updater-${TARGET_LOCK_ID}.lock"
readonly SHARED_HOST_SETUP_LOCK="/run/autostream-updater/.autostream-runtime-host-setup.lock"
readonly LEGACY_UNIT_CONTENT="discord-bot-installer-integration-legacy-unit"
readonly LEGACY_BINARY_CONTENT="discord-bot-installer-integration-legacy-binary"
readonly LEGACY_ALIAS_CONTENT="discord-bot-installer-integration-legacy-alias"
readonly LEGACY_ENV_CONTENT="DISCORD_BOT_INSTALLER_INTEGRATION_ENV=preserve-exactly"
readonly LEGACY_CONFIG_CONTENT="discord-bot-installer-integration-config-preserve-exactly"

fixture_paths_owned=false
runtime_unit_owned=false
runtime_unit_identity=""
fixture_service_start_attempted=false
created_autostream_user=false
old_pid=""
old_pid_start_time=""
runtime_unit_stage=""
legacy_unit_sha256=""
runtime_sync_precommit_hook=""
cleanup_runtime_pre_remove_hook=""
cleanup_runtime_race_report=""
runtime_race_active=false
runtime_race_backup=""
runtime_race_foreign_stage=""
runtime_race_foreign_identity=""
runtime_race_foreign_hash=""

read_proc_pid_start_time() {
  local pid=$1
  local start_time
  local stat_line
  local stat_tail

  [[ ${pid} =~ ^[1-9][0-9]*$ && -r /proc/${pid}/stat ]] || return 1
  IFS= read -r stat_line < "/proc/${pid}/stat" || return 1
  [[ ${stat_line} == *") "* ]] || return 1
  stat_tail="${stat_line##*) }"
  set -- ${stat_tail}
  [[ $# -ge 20 ]] || return 1
  start_time="${20}"
  [[ ${start_time} =~ ^[0-9]+$ ]] || return 1
  printf '%s\n' "${start_time}"
}

runtime_unit_identity_is_owned() {
  [[ ${runtime_unit_owned} == true &&
    -n ${runtime_unit_identity} &&
    -f ${RUNTIME_UNIT_PATH} &&
    ! -L ${RUNTIME_UNIT_PATH} &&
    $(stat -c '%d:%i' -- "${RUNTIME_UNIT_PATH}") == "${runtime_unit_identity}" ]]
}

restore_runtime_sync_race() {
  local current_identity=""

  [[ ${runtime_race_active} == true ]] || return 0
  [[ -n ${runtime_race_backup} &&
    -f ${runtime_race_backup} &&
    ! -L ${runtime_race_backup} &&
    $(stat -c '%d:%i' -- "${runtime_race_backup}") == "${runtime_unit_identity}" ]] || \
    return 1
  if [[ -f ${RUNTIME_UNIT_PATH} && ! -L ${RUNTIME_UNIT_PATH} ]]; then
    current_identity="$(stat -c '%d:%i' -- "${RUNTIME_UNIT_PATH}")"
  fi
  if [[ ${current_identity} == "${runtime_race_foreign_identity}" ]]; then
    mv -Tf -- "${runtime_race_backup}" "${RUNTIME_UNIT_PATH}" || return 1
    runtime_race_backup=""
  elif [[ ${current_identity} == "${runtime_unit_identity}" ]]; then
    rm -f -- "${runtime_race_backup}" || return 1
    runtime_race_backup=""
  else
    return 1
  fi
  if [[ -n ${runtime_race_foreign_stage} ]]; then
    [[ -f ${runtime_race_foreign_stage} &&
      ! -L ${runtime_race_foreign_stage} &&
      $(stat -c '%d:%i' -- "${runtime_race_foreign_stage}") == \
        "${runtime_race_foreign_identity}" ]] || return 1
    rm -f -- "${runtime_race_foreign_stage}" || return 1
    runtime_race_foreign_stage=""
  fi
  sync -f /run/systemd/system || return 1
  runtime_unit_identity_is_owned || return 1
  runtime_race_active=false
  runtime_race_foreign_identity=""
  runtime_race_foreign_hash=""
}

replace_runtime_unit_for_precommit_probe() {
  runtime_unit_identity_is_owned || return 1
  runtime_race_backup="$(
    mktemp "/run/systemd/system/.${UNIT}.race-backup.XXXXXXXX"
  )" || return 1
  rm -f -- "${runtime_race_backup}" || return 1
  ln -- "${RUNTIME_UNIT_PATH}" "${runtime_race_backup}" || return 1
  [[ $(stat -c '%d:%i' -- "${runtime_race_backup}") == \
    "${runtime_unit_identity}" ]] || return 1
  runtime_race_active=true

  runtime_race_foreign_stage="$(
    mktemp "/run/systemd/system/.${UNIT}.race-foreign.XXXXXXXX"
  )" || return 1
  runtime_race_foreign_identity="$(
    stat -c '%d:%i' -- "${runtime_race_foreign_stage}"
  )" || return 1
  cat > "${runtime_race_foreign_stage}" <<EOF
[Unit]
Description=discord-bot-installer-integration-foreign-runtime-unit

[Service]
Type=simple
User=nobody
ExecStart=/usr/bin/false

[Install]
# Keep enablement semantics equivalent while the foreign inode is present.
WantedBy=multi-user.target
EOF
  chmod 0644 "${runtime_race_foreign_stage}" || return 1
  runtime_race_foreign_hash="$(
    sha256sum "${runtime_race_foreign_stage}" | awk 'NR == 1 { print $1 }'
  )" || return 1
  sync -f "${runtime_race_foreign_stage}" || return 1
  mv -Tf -- "${runtime_race_foreign_stage}" "${RUNTIME_UNIT_PATH}" || return 1
  runtime_race_foreign_stage=""
  sync -f /run/systemd/system || return 1
  [[ $(stat -c '%d:%i' -- "${RUNTIME_UNIT_PATH}") == \
    "${runtime_race_foreign_identity}" ]]
}

replace_runtime_unit_for_cleanup_probe() {
  local report_parent=""

  [[ -n ${cleanup_runtime_race_report} &&
    ${cleanup_runtime_race_report} == \
      /var/tmp/autostream-discord-bot-installer-test.*/* &&
    ! -e ${cleanup_runtime_race_report} &&
    ! -L ${cleanup_runtime_race_report} ]] || return 1
  report_parent="$(dirname -- "${cleanup_runtime_race_report}")" || return 1
  [[ -d ${report_parent} &&
    ! -L ${report_parent} &&
    $(stat -c '%U:%G:%a' -- "${report_parent}") == "root:root:700" ]] || \
    return 1
  replace_runtime_unit_for_precommit_probe || return 1
  if ! install -o root -g root -m 0600 /dev/null \
    "${cleanup_runtime_race_report}" ||
    ! printf '%s\t%s\t%s\n' \
      "${runtime_race_backup}" \
      "${runtime_race_foreign_identity}" \
      "${runtime_race_foreign_hash}" > "${cleanup_runtime_race_report}"; then
    restore_runtime_sync_race
    return 1
  fi
}

cleanup() {
  local exit_code=$?
  local cleanup_expected_unit_absent=false
  local cleanup_failed=false
  local cleanup_fragment_path=""
  local cleanup_load_state=""
  local current_pid_start_time=""
  local runtime_unit_identity_matches=false
  set +e
  if [[ ${runtime_unit_owned} == true ||
    ${fixture_service_start_attempted} == true ]]; then
    cleanup_expected_unit_absent=true
  fi
  if [[ ${runtime_race_active} == true ]] &&
    ! restore_runtime_sync_race; then
    cleanup_failed=true
  fi
  if [[ ${runtime_unit_owned} == true &&
    -n ${runtime_unit_identity} &&
    -f ${RUNTIME_UNIT_PATH} &&
    ! -L ${RUNTIME_UNIT_PATH} &&
    $(stat -c '%d:%i' -- "${RUNTIME_UNIT_PATH}") == "${runtime_unit_identity}" ]]; then
    runtime_unit_identity_matches=true
  fi
  if [[ ${runtime_unit_owned} == true &&
    ${runtime_unit_identity_matches} == false ]]; then
    cleanup_failed=true
    printf '%s\n' \
      "discord-bot installer integration test: cleanup could not prove runtime unit ownership" >&2
  fi
  if [[ ${fixture_service_start_attempted} == true &&
    ${runtime_unit_identity_matches} == true ]]; then
    if systemctl stop "${UNIT}" >/dev/null 2>&1; then
      old_pid=""
      old_pid_start_time=""
    else
      cleanup_failed=true
      printf '%s\n' \
        "discord-bot installer integration test: cleanup failed to stop ${UNIT}" >&2
    fi
  fi
  if [[ ${fixture_service_start_attempted} == true &&
    -n ${old_pid} && -n ${old_pid_start_time} ]]; then
    current_pid_start_time="$(read_proc_pid_start_time "${old_pid}" 2>/dev/null || true)"
    if [[ -n ${current_pid_start_time} &&
      ${current_pid_start_time} == "${old_pid_start_time}" ]]; then
      if kill "${old_pid}" >/dev/null 2>&1; then
        old_pid=""
        old_pid_start_time=""
      else
        cleanup_failed=true
      fi
    elif [[ -n ${current_pid_start_time} ]]; then
      cleanup_failed=true
      printf '%s\n' \
        "discord-bot installer integration test: cleanup fallback refused a reused PID ${old_pid}" >&2
    fi
  fi
  if [[ -n ${cleanup_runtime_pre_remove_hook} ]]; then
    if ! "${cleanup_runtime_pre_remove_hook}"; then
      cleanup_failed=true
      printf '%s\n' \
        "discord-bot installer integration test: cleanup runtime race hook failed" >&2
    fi
    cleanup_runtime_pre_remove_hook=""
  fi
  if [[ ${runtime_unit_owned} == true &&
    ${runtime_unit_identity_matches} == true ]]; then
    if ! runtime_unit_identity_is_owned; then
      cleanup_failed=true
      runtime_unit_identity_matches=false
      printf '%s\n' \
        "discord-bot installer integration test: cleanup could not prove runtime unit ownership before removal" >&2
    else
      if ! rm -f -- "${RUNTIME_UNIT_PATH}"; then
        cleanup_failed=true
        printf '%s\n' \
          "discord-bot installer integration test: cleanup failed to remove ${RUNTIME_UNIT_PATH}" >&2
      fi
      if ! systemctl daemon-reload >/dev/null 2>&1; then
        cleanup_failed=true
        printf '%s\n' \
          "discord-bot installer integration test: cleanup daemon-reload failed" >&2
      fi
    fi
  fi
  if [[ ${fixture_paths_owned} == true ]]; then
    rm -f -- "${UNIT_PATH}"
    rm -f -- \
      "${PUBLIC_BINARY}" \
      "${PUBLIC_ALIAS}" \
      "${ENV_PATH}" \
      "${TARGET_LOCK}" \
      "${SHARED_HOST_SETUP_LOCK}"
    rm -rf -- \
      "${CONFIG_DIR}" \
      "${STATE_DIR}" \
      "${MANAGED_ROOT}" \
      "${INSTALL_BACKUP_ROOT}"
    rmdir /unpack >/dev/null 2>&1
  fi
  if [[ -n ${runtime_unit_stage} ]]; then
    if ! rm -f -- "${runtime_unit_stage}"; then
      cleanup_failed=true
    fi
  fi
  rm -rf -- "${WORK_DIR}"
  if [[ ${fixture_paths_owned} == true ]]; then
    userdel autostream-install-rollback >/dev/null 2>&1
  fi
  if [[ ${fixture_paths_owned} == true && ${created_autostream_user} == true ]]; then
    userdel autostream >/dev/null 2>&1
    groupdel autostream >/dev/null 2>&1
  fi
  if [[ ${cleanup_expected_unit_absent} == true ]]; then
    if systemctl is-active --quiet "${UNIT}" >/dev/null 2>&1; then
      cleanup_failed=true
      printf '%s\n' \
        "discord-bot installer integration test: cleanup left ${UNIT} active" >&2
    fi
    cleanup_load_state="$(systemctl show --property LoadState --value "${UNIT}" 2>/dev/null || true)"
    cleanup_fragment_path="$(systemctl show --property FragmentPath --value "${UNIT}" 2>/dev/null || true)"
    if [[ ${cleanup_load_state} != "not-found" || -n ${cleanup_fragment_path} ]]; then
      cleanup_failed=true
      printf '%s\n' \
        "discord-bot installer integration test: cleanup left ${UNIT} loaded" >&2
    fi
  fi
  if [[ ${cleanup_failed} == true && ${exit_code} -eq 0 ]]; then
    exit_code=1
  fi
  exit "${exit_code}"
}
trap cleanup EXIT

if [[ ${AUTOSTREAM_DISCORD_BOT_INSTALLER_TEST_CLEANUP_RACE_PROBE:-} == "1" ]]; then
  [[ -n ${AUTOSTREAM_DISCORD_BOT_INSTALLER_TEST_CLEANUP_RACE_ID:-} &&
    -f ${RUNTIME_UNIT_PATH} &&
    ! -L ${RUNTIME_UNIT_PATH} &&
    $(stat -c '%d:%i' -- "${RUNTIME_UNIT_PATH}") == \
      "${AUTOSTREAM_DISCORD_BOT_INSTALLER_TEST_CLEANUP_RACE_ID}" ]] || \
    die "cleanup race probe could not adopt the expected runtime unit"
  runtime_unit_owned=true
  runtime_unit_identity="${AUTOSTREAM_DISCORD_BOT_INSTALLER_TEST_CLEANUP_RACE_ID}"
  cleanup_runtime_race_report="${AUTOSTREAM_DISCORD_BOT_INSTALLER_TEST_CLEANUP_RACE_REPORT:-}"
  cleanup_runtime_pre_remove_hook=replace_runtime_unit_for_cleanup_probe
  exit 0
fi

assert_runtime_unit_file() {
  [[ -f ${RUNTIME_UNIT_PATH} && ! -L ${RUNTIME_UNIT_PATH} ]] || \
    die "runtime unit path is missing or unsafe"
  [[ $(stat -c '%U:%G:%a' -- "${RUNTIME_UNIT_PATH}") == "root:root:644" ]] || \
    die "runtime unit path has unsafe ownership or mode"
}

assert_owned_runtime_unit_identity() {
  runtime_unit_identity_is_owned || die "runtime unit is not strictly fixture-owned"
  assert_runtime_unit_file
}

create_runtime_unit_no_clobber() {
  [[ ${runtime_unit_owned} == false ]] || die "runtime unit is already fixture-owned"
  [[ ! -e ${RUNTIME_UNIT_PATH} && ! -L ${RUNTIME_UNIT_PATH} ]] || \
    die "runtime unit path appeared after preflight"

  runtime_unit_stage="$(mktemp "/run/systemd/system/.${UNIT}.legacy.XXXXXXXX")"
  install -o root -g root -m 0644 "${UNIT_PATH}" "${runtime_unit_stage}"
  sync -f "${runtime_unit_stage}"
  if ! ln -- "${runtime_unit_stage}" "${RUNTIME_UNIT_PATH}"; then
    rm -f -- "${runtime_unit_stage}"
    runtime_unit_stage=""
    die "runtime unit path appeared during atomic creation"
  fi
  runtime_unit_owned=true
  runtime_unit_identity="$(stat -c '%d:%i' -- "${RUNTIME_UNIT_PATH}")"
  rm -f -- "${runtime_unit_stage}"
  runtime_unit_stage=""
  sync -f /run/systemd/system
  assert_owned_runtime_unit_identity
  cmp -s -- "${UNIT_PATH}" "${RUNTIME_UNIT_PATH}" || \
    die "atomic runtime unit creation changed the legacy unit"
}

sync_managed_runtime_unit() {
  assert_owned_runtime_unit_identity
  [[ -f ${UNIT_PATH} && ! -L ${UNIT_PATH} ]] || \
    die "managed private unit path is missing or unsafe"
  [[ $(stat -c '%U:%G:%a' -- "${UNIT_PATH}") == "root:root:644" ]] || \
    die "managed private unit path has unsafe ownership or mode"

  runtime_unit_stage="$(mktemp "/run/systemd/system/.${UNIT}.managed.XXXXXXXX")"
  install -o root -g root -m 0644 "${UNIT_PATH}" "${runtime_unit_stage}"
  cmp -s -- "${UNIT_PATH}" "${runtime_unit_stage}" || \
    die "managed runtime unit staging changed the private unit"
  sync -f "${runtime_unit_stage}"
  if [[ -n ${runtime_sync_precommit_hook} ]] &&
    ! "${runtime_sync_precommit_hook}"; then
    rm -f -- "${runtime_unit_stage}"
    runtime_unit_stage=""
    return 76
  fi
  if ! runtime_unit_identity_is_owned; then
    rm -f -- "${runtime_unit_stage}"
    runtime_unit_stage=""
    return 75
  fi
  mv -Tf -- "${runtime_unit_stage}" "${RUNTIME_UNIT_PATH}"
  runtime_unit_stage=""
  runtime_unit_identity="$(stat -c '%d:%i' -- "${RUNTIME_UNIT_PATH}")"
  sync -f /run/systemd/system
  assert_owned_runtime_unit_identity
  cmp -s -- "${UNIT_PATH}" "${RUNTIME_UNIT_PATH}" || \
    die "managed runtime unit does not match the private unit"
  systemctl daemon-reload
}

assert_loaded_legacy_runtime_unit() {
  local loaded_exec
  local loaded_fragment
  local loaded_pid
  local loaded_user

  assert_owned_runtime_unit_identity
  [[ $(sha256sum "${UNIT_PATH}" | awk 'NR == 1 { print $1 }') == \
    "${legacy_unit_sha256}" ]] || die "legacy private unit hash changed"
  [[ $(sha256sum "${RUNTIME_UNIT_PATH}" | awk 'NR == 1 { print $1 }') == \
    "${legacy_unit_sha256}" ]] || die "legacy runtime shadow hash changed"
  loaded_fragment="$(systemctl show --property FragmentPath --value "${UNIT}")"
  [[ ${loaded_fragment} == "${RUNTIME_UNIT_PATH}" ]] || \
    die "PID 1 loaded the legacy unit from ${loaded_fragment:-unknown}"
  loaded_exec="$(systemctl show --property ExecStart --value "${UNIT}")"
  [[ ${loaded_exec} == *"path=/usr/bin/sleep ;"* ]] || \
    die "PID 1 did not retain the legacy ExecStart"
  loaded_user="$(systemctl show --property User --value "${UNIT}")"
  [[ ${loaded_user} == "root" ]] || die "PID 1 did not retain the legacy User"
  loaded_pid="$(systemctl show --property MainPID --value "${UNIT}")"
  [[ ${loaded_pid} == "${old_pid}" ]] || die "legacy process PID changed"
  kill -0 "${old_pid}" || die "legacy process is not alive"
}

assert_loaded_managed_runtime_unit() {
  local loaded_exec
  local loaded_fragment
  local loaded_pid
  local loaded_user
  local managed_binary

  assert_owned_runtime_unit_identity
  cmp -s -- "${UNIT_PATH}" "${RUNTIME_UNIT_PATH}" || \
    die "loaded managed runtime unit differs from the private unit"
  loaded_fragment="$(systemctl show --property FragmentPath --value "${UNIT}")"
  [[ ${loaded_fragment} == "${RUNTIME_UNIT_PATH}" ]] || \
    die "PID 1 loaded the managed unit from ${loaded_fragment:-unknown}"
  loaded_exec="$(systemctl show --property ExecStart --value "${UNIT}")"
  [[ ${loaded_exec} == *"path=${PUBLIC_BINARY} ;"* ]] || \
    die "PID 1 did not load the managed public ExecStart"
  managed_binary="$(readlink -f -- "${PUBLIC_BINARY}")"
  [[ ${managed_binary} == \
    "${MANAGED_ROOT}"/releases/*/bin/autostream-discord-bot ]] || \
    die "managed public ExecStart does not resolve into a verified release"
  loaded_user="$(systemctl show --property User --value "${UNIT}")"
  [[ ${loaded_user} == "autostream" ]] || die "PID 1 did not load the managed User"
  loaded_pid="$(systemctl show --property MainPID --value "${UNIT}")"
  [[ ${loaded_pid} == "${old_pid}" ]] || \
    die "managed daemon-reload replaced the running legacy process"
  kill -0 "${old_pid}" || die "managed daemon-reload stopped the legacy process"
}

for path in \
  "${UNIT_PATH}" \
  "${RUNTIME_UNIT_PATH}" \
  "${PUBLIC_BINARY}" \
  "${PUBLIC_ALIAS}" \
  "${ENV_PATH}" \
  "${CONFIG_DIR}" \
  "${STATE_DIR}" \
  "${MANAGED_ROOT}" \
  "${INSTALL_BACKUP_ROOT}" \
  "${TARGET_LOCK}" \
  "${SHARED_HOST_SETUP_LOCK}"; do
  [[ ! -e ${path} && ! -L ${path} ]] || die "runner is not clean at ${path}"
done
preflight_load_state="$(systemctl show --property LoadState --value "${UNIT}" 2>/dev/null || true)"
preflight_fragment_path="$(systemctl show --property FragmentPath --value "${UNIT}" 2>/dev/null || true)"
[[ ${preflight_load_state} == "not-found" && -z ${preflight_fragment_path} ]] || \
  die "runner already has a loaded ${UNIT}"
systemctl is-active --quiet "${UNIT}" &&
  die "runner already has an active ${UNIT}"
preflight_enabled_state="$(systemctl is-enabled "${UNIT}" 2>/dev/null || true)"
[[ -z ${preflight_enabled_state} ||
  ${preflight_enabled_state} == "disabled" ||
  ${preflight_enabled_state} == "not-found" ]] || \
  die "runner already has an enabled ${UNIT}"
if id autostream >/dev/null 2>&1 || getent group autostream >/dev/null 2>&1; then
  die "runner already has an autostream account"
fi
if getent passwd autostream-install-rollback >/dev/null 2>&1 ||
  getent group autostream-install-rollback >/dev/null 2>&1; then
  die "runner already has the reserved service-account rollback login"
fi
[[ ! -e /unpack && ! -L /unpack ]] || die "runner is not clean at /unpack"
if [[ ${AUTOSTREAM_DISCORD_BOT_INSTALLER_TEST_PREFLIGHT_PROBE:-} == "1" ]]; then
  die "preflight ownership probe unexpectedly passed"
fi
fixture_paths_owned=true

cat > "${UNIT_PATH}" <<EOF
[Unit]
Description=discord-bot-installer-integration-runtime-sentinel

[Service]
Type=simple
User=root
ExecStart=/usr/bin/sleep infinity
Restart=no

[Install]
# Keep cleanup race enablement semantics equivalent to the foreign probe unit.
WantedBy=multi-user.target
EOF
chmod 0644 "${UNIT_PATH}"
create_runtime_unit_no_clobber
systemctl daemon-reload
fixture_service_start_attempted=true
systemctl start "${UNIT}"
old_pid="$(systemctl show --property MainPID --value "${UNIT}")"
[[ ${old_pid} =~ ^[1-9][0-9]*$ ]] || die "runtime sentinel service did not start"
old_pid_start_time="$(read_proc_pid_start_time "${old_pid}")"
[[ ${old_pid_start_time} =~ ^[0-9]+$ ]] || \
  die "runtime sentinel PID start time is unavailable"
runtime_sentinel_inode_before="$(stat -c '%d:%i' -- "${RUNTIME_UNIT_PATH}")"
runtime_sentinel_hash_before="$(sha256sum "${RUNTIME_UNIT_PATH}" | awk 'NR == 1 { print $1 }')"
runtime_sentinel_fragment_before="$(systemctl show --property FragmentPath --value "${UNIT}")"
runtime_sentinel_exec_start_before="$(systemctl show --property ExecStart --value "${UNIT}")"
runtime_sentinel_user_before="$(systemctl show --property User --value "${UNIT}")"
runtime_sentinel_enabled_before="$(systemctl is-enabled "${UNIT}" 2>/dev/null || true)"
rm -f -- "${UNIT_PATH}"

cleanup_race_report="${WORK_DIR}/cleanup-runtime-race.report"
set +e
AUTOSTREAM_DISCORD_BOT_INSTALLER_TEST_MOUNT_NS=1 \
  AUTOSTREAM_DISCORD_BOT_INSTALLER_TEST_CLEANUP_RACE_PROBE=1 \
  AUTOSTREAM_DISCORD_BOT_INSTALLER_TEST_CLEANUP_RACE_ID="${runtime_sentinel_inode_before}" \
  AUTOSTREAM_DISCORD_BOT_INSTALLER_TEST_CLEANUP_RACE_REPORT="${cleanup_race_report}" \
  bash "${SCRIPT_DIR}/test-install-autostream-discord-bot-integration.sh" \
  > "${WORK_DIR}/cleanup-runtime-race.out" 2>&1
cleanup_race_status=$?
set -e
[[ ${cleanup_race_status} -eq 1 ]] || \
  die "cleanup runtime race did not promote a successful exit to failure"
[[ -f ${cleanup_race_report} && ! -L ${cleanup_race_report} &&
  $(stat -c '%U:%G:%a' -- "${cleanup_race_report}") == "root:root:600" ]] || \
  die "cleanup runtime race report is missing or unsafe"
IFS=$'\t' read -r \
  cleanup_race_backup \
  cleanup_race_foreign_identity \
  cleanup_race_foreign_hash < "${cleanup_race_report}"
[[ ${cleanup_race_backup} == \
    /run/systemd/system/.${UNIT}.race-backup.* &&
  -f ${cleanup_race_backup} &&
  ! -L ${cleanup_race_backup} &&
  $(stat -c '%d:%i' -- "${cleanup_race_backup}") == \
    "${runtime_sentinel_inode_before}" ]] || \
  die "cleanup runtime race did not preserve the owned inode for recovery"
[[ $(stat -c '%d:%i' -- "${RUNTIME_UNIT_PATH}") == \
  "${cleanup_race_foreign_identity}" ]] || \
  die "cleanup runtime race removed or replaced the foreign inode"
[[ $(sha256sum "${RUNTIME_UNIT_PATH}" | awk 'NR == 1 { print $1 }') == \
  "${cleanup_race_foreign_hash}" ]] || \
  die "cleanup runtime race changed the foreign runtime unit hash"
[[ $(systemctl show --property FragmentPath --value "${UNIT}") == \
  "${runtime_sentinel_fragment_before}" ]] || \
  die "cleanup runtime race changed PID1 FragmentPath"
[[ $(systemctl show --property ExecStart --value "${UNIT}") == \
  "${runtime_sentinel_exec_start_before}" ]] || \
  die "cleanup runtime race changed PID1 ExecStart"
[[ $(systemctl show --property User --value "${UNIT}") == \
  "${runtime_sentinel_user_before}" ]] || \
  die "cleanup runtime race changed PID1 User"
[[ $(systemctl show --property MainPID --value "${UNIT}") == "${old_pid}" ]] || \
  die "cleanup runtime race changed the runtime sentinel PID"
[[ $(systemctl is-enabled "${UNIT}" 2>/dev/null || true) == \
  "${runtime_sentinel_enabled_before}" ]] || \
  die "cleanup runtime race changed the runtime sentinel enabled state"
kill -0 "${old_pid}" || die "cleanup runtime race stopped the runtime sentinel"
mv -Tf -- "${cleanup_race_backup}" "${RUNTIME_UNIT_PATH}"
sync -f /run/systemd/system
[[ $(stat -c '%d:%i' -- "${RUNTIME_UNIT_PATH}") == \
  "${runtime_sentinel_inode_before}" ]] || \
  die "cleanup runtime race recovery did not restore the owned inode"
[[ $(sha256sum "${RUNTIME_UNIT_PATH}" | awk 'NR == 1 { print $1 }') == \
  "${runtime_sentinel_hash_before}" ]] || \
  die "cleanup runtime race recovery changed the runtime sentinel hash"
rm -f -- "${cleanup_race_report}"

set +e
AUTOSTREAM_DISCORD_BOT_INSTALLER_TEST_MOUNT_NS=1 \
  AUTOSTREAM_DISCORD_BOT_INSTALLER_TEST_PREFLIGHT_PROBE=1 bash \
  "${SCRIPT_DIR}/test-install-autostream-discord-bot-integration.sh" \
  > "${WORK_DIR}/preflight-conflict.out" 2>&1
preflight_probe_status=$?
set -e
[[ ${preflight_probe_status} -ne 0 ]] || \
  die "preflight conflict probe unexpectedly succeeded"
grep -F -- "runner is not clean at ${RUNTIME_UNIT_PATH}" \
  "${WORK_DIR}/preflight-conflict.out" >/dev/null || \
  die "preflight conflict probe did not reject the runtime sentinel"
[[ ! -e ${UNIT_PATH} && ! -L ${UNIT_PATH} ]] || \
  die "preflight conflict recreated the private unit"
[[ $(stat -c '%d:%i' -- "${RUNTIME_UNIT_PATH}") == \
  "${runtime_sentinel_inode_before}" ]] || \
  die "preflight conflict changed the runtime sentinel inode"
[[ $(sha256sum "${RUNTIME_UNIT_PATH}" | awk 'NR == 1 { print $1 }') == \
  "${runtime_sentinel_hash_before}" ]] || \
  die "preflight conflict changed the runtime sentinel hash"
[[ $(systemctl show --property FragmentPath --value "${UNIT}") == \
  "${runtime_sentinel_fragment_before}" ]] || \
  die "preflight conflict changed the runtime sentinel FragmentPath"
[[ $(systemctl show --property ExecStart --value "${UNIT}") == \
  "${runtime_sentinel_exec_start_before}" ]] || \
  die "preflight conflict changed the runtime sentinel ExecStart"
[[ $(systemctl show --property User --value "${UNIT}") == \
  "${runtime_sentinel_user_before}" ]] || \
  die "preflight conflict changed the runtime sentinel User"
[[ $(systemctl show --property MainPID --value "${UNIT}") == "${old_pid}" ]] || \
  die "preflight conflict changed the runtime sentinel PID"
[[ $(systemctl is-enabled "${UNIT}" 2>/dev/null || true) == \
  "${runtime_sentinel_enabled_before}" ]] || \
  die "preflight conflict changed the runtime sentinel enabled state"
kill -0 "${old_pid}" || die "preflight conflict stopped the runtime sentinel"

systemctl stop "${UNIT}"
fixture_service_start_attempted=false
assert_owned_runtime_unit_identity
rm -f -- "${RUNTIME_UNIT_PATH}"
runtime_unit_owned=false
runtime_unit_identity=""
old_pid=""
old_pid_start_time=""
systemctl daemon-reload

install -d -o root -g root -m 0755 \
  "${ARTIFACTS_DIR}" \
  "${EXTRACTED_ROOT}/bin" \
  "${EXTRACTED_ROOT}/systemd"
install -o root -g root -m 0755 "${INSTALLER_SOURCE}" \
  "${EXTRACTED_ROOT}/install-autostream-discord-bot"

cat > "${EXTRACTED_ROOT}/bin/autostream-discord-bot" <<'EOF'
#!/bin/sh
if [ "${1:-}" = "--version" ]; then
  printf '%s\n' 'autostream-discord-bot v9.9.9'
  printf '%s\n' 'commit: 0123456789abcdef0123456789abcdef01234567'
  printf '%s\n' 'build_date: 2026-01-01T00:00:00Z'
  exit 0
fi
exec /usr/bin/sleep infinity
EOF
chmod 0755 "${EXTRACTED_ROOT}/bin/autostream-discord-bot"
cp "${EXTRACTED_ROOT}/bin/autostream-discord-bot" \
  "${EXTRACTED_ROOT}/bin/discord-bot"
chmod 0755 "${EXTRACTED_ROOT}/bin/discord-bot"

cat > "${EXTRACTED_ROOT}/systemd/autostream-discord-bot.service.example" <<'EOF'
[Unit]
Description=AutoStream Discord Bot integration fixture

[Service]
Type=simple
User=autostream
Group=autostream
EnvironmentFile=-/etc/autostream/discord-bot.env
ExecStart=/usr/local/bin/autostream-discord-bot
Restart=on-failure

[Install]
WantedBy=multi-user.target
EOF
printf '%s\n' 'AUTOSTREAM_BIND_ADDR=127.0.0.1:18083' \
  > "${EXTRACTED_ROOT}/.env.example"

jq -n \
  --arg component "discord-bot" \
  --arg source_version "${VERSION}" \
  --arg commit "${ARTIFACT_COMMIT}" \
  --arg build_date "${ARTIFACT_BUILD_DATE}" \
  --arg archive_name "${ARTIFACT_ID}.tar.gz" \
  --arg archive_root "${ARTIFACT_ID}" \
  '{
    schema_version: 1,
    component: $component,
    source_version: $source_version,
    commit: $commit,
    build_date: $build_date,
    platform: {
      os: "linux",
      arch: "amd64"
    },
    archive: {
      name: $archive_name,
      root: $archive_root
    },
    compatibility: {
      minimum_agent_version: "v1.0.0",
      minimum_panel_version: null,
      rollback_compatible: true,
      database_schema: "none"
    }
  }' > "${EXTRACTED_ROOT}/artifact-manifest.json"

package_fixture_archive() {
  (
    cd -- "${EXTRACTED_ROOT}"
    find . -type f ! -path './checksums.txt' -print0 |
      sort -z |
      xargs -0 sha256sum > checksums.txt
  )
  tar -C "${ARTIFACTS_DIR}" -czf "${ARCHIVE}" "${ARTIFACT_ID}"
}

package_fixture_archive
[[ ! -e ${ARCHIVE}.sha256 && ! -L ${ARCHIVE}.sha256 ]] || \
  die "archive-only fixture unexpectedly contains an archive checksum sidecar"
[[ ! -e ${ARTIFACTS_DIR}/release-manifest.json &&
  ! -L ${ARTIFACTS_DIR}/release-manifest.json ]] || \
  die "archive-only fixture unexpectedly contains an external release manifest"
[[ ! -e ${ARTIFACTS_DIR}/release-manifest.json.sha256 &&
  ! -L ${ARTIFACTS_DIR}/release-manifest.json.sha256 ]] || \
  die "archive-only fixture unexpectedly contains an external manifest checksum sidecar"

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

created_autostream_user=true
"${EXTRACTED_ROOT}/install-autostream-discord-bot" > "${WORK_DIR}/fresh.out"
[[ -L ${MANAGED_ROOT}/current ]] || die "fresh install did not activate current"
[[ -L ${PUBLIC_BINARY} && -L ${PUBLIC_ALIAS} ]] || \
  die "fresh install did not create stable public links"
[[ -f ${ENV_PATH} && ! -L ${ENV_PATH} ]] || die "fresh install did not seed the environment"
[[ $(stat -c '%U:%G:%a' -- "${ENV_PATH}") == "root:root:640" ]] || \
  die "fresh environment ownership or mode is invalid"
[[ $(stat -c '%U:%G:%a' -- "${STATE_DIR}") == "autostream:autostream:750" ]] || \
  die "fresh state directory ownership or mode is invalid"
systemctl is-active --quiet "${UNIT}" && die "fresh installer unexpectedly started the service"
assert_not_enabled
grep -F -- "sudo systemctl enable --now ${UNIT}" "${WORK_DIR}/fresh.out" >/dev/null || \
  die "fresh install did not print the explicit first-start command"

rm -f -- "${PUBLIC_BINARY}" "${PUBLIC_ALIAS}" "${ENV_PATH}" "${UNIT_PATH}"
rm -rf -- "${STATE_DIR}" "${MANAGED_ROOT}" "${INSTALL_BACKUP_ROOT}"
systemctl daemon-reload

install -d -o autostream -g autostream -m 0700 "${STATE_DIR}"
printf '%s\n' 'discord-bot-state-preflight-sentinel' > "${STATE_DIR}/sentinel.txt"
chown autostream:autostream "${STATE_DIR}/sentinel.txt"
chmod 0600 "${STATE_DIR}/sentinel.txt"
install -d -o root -g root -m 0700 "${ENV_PATH}"
state_preflight_identity_before="$(stat -c '%d:%i' -- "${STATE_DIR}")"
state_preflight_metadata_before="$(stat -c '%U:%G:%a' -- "${STATE_DIR}")"
state_preflight_sentinel_metadata_before="$(
  stat -c '%d:%i:%U:%G:%a' -- "${STATE_DIR}/sentinel.txt"
)"
state_preflight_sentinel_sha_before="$(
  sha256sum "${STATE_DIR}/sentinel.txt" | awk 'NR == 1 { print $1 }'
)"
state_preflight_account_before="$(id -u autostream):$(id -g autostream)"
[[ ! -e ${MANAGED_ROOT} && ! -L ${MANAGED_ROOT} ]] || \
  die "state preflight fixture unexpectedly has a managed root"
[[ ! -e ${INSTALL_BACKUP_ROOT} && ! -L ${INSTALL_BACKUP_ROOT} ]] || \
  die "state preflight fixture unexpectedly has an install backup root"
set +e
"${EXTRACTED_ROOT}/install-autostream-discord-bot" \
  > "${WORK_DIR}/state-preflight-failure.out" 2>&1
state_preflight_status=$?
set -e
[[ ${state_preflight_status} -ne 0 ]] || \
  die "installer accepted an unsafe existing environment path"
grep -F -- "existing environment file is not a regular file" \
  "${WORK_DIR}/state-preflight-failure.out" >/dev/null || \
  die "state preservation fixture did not reach the later environment preflight"
[[ $(stat -c '%d:%i' -- "${STATE_DIR}") == "${state_preflight_identity_before}" ]] || \
  die "later preflight failure replaced the existing state directory"
[[ $(stat -c '%U:%G:%a' -- "${STATE_DIR}") == "${state_preflight_metadata_before}" ]] || \
  die "later preflight failure changed existing state ownership or mode"
[[ "$(stat -c '%d:%i:%U:%G:%a' -- "${STATE_DIR}/sentinel.txt")" == \
  "${state_preflight_sentinel_metadata_before}" ]] || \
  die "later preflight failure changed the state sentinel metadata"
[[ "$(sha256sum "${STATE_DIR}/sentinel.txt" | awk 'NR == 1 { print $1 }')" == \
  "${state_preflight_sentinel_sha_before}" ]] || \
  die "later preflight failure changed the state sentinel content"
[[ "$(id -u autostream):$(id -g autostream)" == "${state_preflight_account_before}" ]] || \
  die "later preflight failure changed the preexisting service account"
for path in \
  "${MANAGED_ROOT}" \
  "${INSTALL_BACKUP_ROOT}" \
  "${PUBLIC_BINARY}" \
  "${PUBLIC_ALIAS}" \
  "${UNIT_PATH}"; do
  [[ ! -e ${path} && ! -L ${path} ]] || \
    die "later preflight failure created persistent boundary ${path}"
done
rm -rf -- "${ENV_PATH}" "${STATE_DIR}"

install -d -o autostream -g autostream -m 0750 "${STATE_DIR}"
install -d -o root -g root -m 0750 /etc/autostream
printf '%s\n' "${LEGACY_ENV_CONTENT}" > "${ENV_PATH}"
chmod 0640 "${ENV_PATH}"
install -d -o root -g root -m 0700 "${CONFIG_DIR}"
printf '%s\n' "${LEGACY_CONFIG_CONTENT}" > "${CONFIG_PATH}"
chmod 0600 "${CONFIG_PATH}"
printf '%s\n' "${LEGACY_BINARY_CONTENT}" > "${PUBLIC_BINARY}"
chmod 0755 "${PUBLIC_BINARY}"
printf '%s\n' "${LEGACY_ALIAS_CONTENT}" > "${PUBLIC_ALIAS}"
chmod 0755 "${PUBLIC_ALIAS}"
cat > "${UNIT_PATH}" <<EOF
[Unit]
Description=${LEGACY_UNIT_CONTENT}

[Service]
Type=simple
User=root
ExecStart=/usr/bin/sleep infinity
Restart=on-failure

[Install]
WantedBy=multi-user.target
EOF
chmod 0644 "${UNIT_PATH}"
legacy_unit_sha256="$(sha256sum "${UNIT_PATH}" | awk 'NR == 1 { print $1 }')"
create_runtime_unit_no_clobber
systemctl daemon-reload
fixture_service_start_attempted=true
systemctl start "${UNIT}"
old_pid="$(systemctl show --property MainPID --value "${UNIT}")"
[[ ${old_pid} =~ ^[1-9][0-9]*$ ]] || die "legacy service did not start"
old_pid_start_time="$(read_proc_pid_start_time "${old_pid}")"
[[ ${old_pid_start_time} =~ ^[0-9]+$ ]] || \
  die "legacy service PID start time is unavailable"
assert_loaded_legacy_runtime_unit
legacy_unit_file_state="$(systemctl is-enabled "${UNIT}" 2>/dev/null || true)"
[[ ${legacy_unit_file_state} == "disabled" ]] || \
  die "legacy fixture must begin disabled, got ${legacy_unit_file_state:-unknown}"

env_before="$(sha256sum "${ENV_PATH}" | awk 'NR == 1 { print $1 }')"
config_before="$(sha256sum "${CONFIG_PATH}" | awk 'NR == 1 { print $1 }')"
unit_before="${legacy_unit_sha256}"
install -d -o root -g root -m 0700 /opt/autostream
shared_managed_parent_before="$(stat -c '%d:%i:%u:%g:%a' -- /opt/autostream)"
legacy_public_binary_metadata_before="$(
  stat -c '%d:%i:%u:%g:%a:%h' -- "${PUBLIC_BINARY}"
)"
legacy_public_binary_sha_before="$(
  sha256sum "${PUBLIC_BINARY}" | awk 'NR == 1 { print $1 }'
)"
legacy_public_alias_metadata_before="$(
  stat -c '%d:%i:%u:%g:%a:%h' -- "${PUBLIC_ALIAS}"
)"
legacy_public_alias_sha_before="$(
  sha256sum "${PUBLIC_ALIAS}" | awk 'NR == 1 { print $1 }'
)"

preexisting_backup_dir="${INSTALL_BACKUP_ROOT}/${VERSION}-${archive_sha256:0:12}"
install -d -o root -g root -m 0700 "${INSTALL_BACKUP_ROOT}"
install -d -o root -g root -m 0700 "${preexisting_backup_dir}"
install -o root -g root -m 0500 "${PUBLIC_BINARY}" \
  "${preexisting_backup_dir}/autostream-discord-bot"
install -o root -g root -m 0500 "${PUBLIC_ALIAS}" \
  "${preexisting_backup_dir}/discord-bot"
[[ $(stat -c '%u:%g:%a' -- "${INSTALL_BACKUP_ROOT}") == "0:0:700" ]] || \
  die "pre-existing backup root fixture is not root-only"
[[ $(stat -c '%u:%g:%a' -- "${preexisting_backup_dir}") == "0:0:700" ]] || \
  die "pre-existing backup directory fixture is not root-only"
[[ $(stat -c '%u:%g:%a' -- \
  "${preexisting_backup_dir}/autostream-discord-bot") == "0:0:500" ]] || \
  die "pre-existing canonical backup fixture is not root-only"
[[ $(stat -c '%u:%g:%a' -- \
  "${preexisting_backup_dir}/discord-bot") == "0:0:500" ]] || \
  die "pre-existing alias backup fixture is not root-only"
preexisting_backup_dir_metadata_before="$(
  stat -c '%d:%i:%u:%g:%a' -- "${preexisting_backup_dir}"
)"
preexisting_backup_binary_metadata_before="$(
  stat -c '%d:%i:%u:%g:%a' -- \
    "${preexisting_backup_dir}/autostream-discord-bot"
)"
preexisting_backup_binary_sha_before="$(
  sha256sum "${preexisting_backup_dir}/autostream-discord-bot" |
    awk 'NR == 1 { print $1 }'
)"
preexisting_backup_alias_metadata_before="$(
  stat -c '%d:%i:%u:%g:%a' -- "${preexisting_backup_dir}/discord-bot"
)"
preexisting_backup_alias_sha_before="$(
  sha256sum "${preexisting_backup_dir}/discord-bot" |
    awk 'NR == 1 { print $1 }'
)"

assert_preexisting_backups_unchanged() {
  [[ "$(stat -c '%d:%i:%u:%g:%a' -- "${preexisting_backup_dir}")" == \
    "${preexisting_backup_dir_metadata_before}" ]] || \
    die "pre-existing backup directory metadata changed"
  [[ "$(stat -c '%d:%i:%u:%g:%a' -- \
    "${preexisting_backup_dir}/autostream-discord-bot")" == \
    "${preexisting_backup_binary_metadata_before}" ]] || \
    die "pre-existing canonical backup inode or metadata changed"
  [[ "$(sha256sum "${preexisting_backup_dir}/autostream-discord-bot" |
    awk 'NR == 1 { print $1 }')" == \
    "${preexisting_backup_binary_sha_before}" ]] || \
    die "pre-existing canonical backup content changed"
  [[ "$(stat -c '%d:%i:%u:%g:%a' -- \
    "${preexisting_backup_dir}/discord-bot")" == \
    "${preexisting_backup_alias_metadata_before}" ]] || \
    die "pre-existing alias backup inode or metadata changed"
  [[ "$(sha256sum "${preexisting_backup_dir}/discord-bot" |
    awk 'NR == 1 { print $1 }')" == \
    "${preexisting_backup_alias_sha_before}" ]] || \
    die "pre-existing alias backup content changed"
}

assert_legacy_public_paths_unchanged() {
  [[ "$(stat -c '%d:%i:%u:%g:%a:%h' -- "${PUBLIC_BINARY}")" == \
    "${legacy_public_binary_metadata_before}" ]] || \
    die "failed migration changed the legacy canonical binary inode, metadata, or link count"
  [[ "$(sha256sum "${PUBLIC_BINARY}" | awk 'NR == 1 { print $1 }')" == \
    "${legacy_public_binary_sha_before}" ]] || \
    die "failed migration changed the legacy canonical binary content"
  [[ "$(stat -c '%d:%i:%u:%g:%a:%h' -- "${PUBLIC_ALIAS}")" == \
    "${legacy_public_alias_metadata_before}" ]] || \
    die "failed migration changed the legacy alias inode, metadata, or link count"
  [[ "$(sha256sum "${PUBLIC_ALIAS}" | awk 'NR == 1 { print $1 }')" == \
    "${legacy_public_alias_sha_before}" ]] || \
    die "failed migration changed the legacy alias content"
  [[ "${legacy_public_binary_sha_before}" == \
    "${preexisting_backup_binary_sha_before}" ]] || \
    die "pre-existing canonical backup was not bound to the live legacy binary"
  [[ "${legacy_public_alias_sha_before}" == \
    "${preexisting_backup_alias_sha_before}" ]] || \
    die "pre-existing alias backup was not bound to the live legacy binary"
  [[ -z $(compgen -G "${PUBLIC_BINARY}.rollback-anchor.*" || true) ]] || \
    die "failed migration left a canonical public rollback anchor"
  [[ -z $(compgen -G "${PUBLIC_ALIAS}.rollback-anchor.*" || true) ]] || \
    die "failed migration left a compatibility public rollback anchor"
}

assert_shared_managed_parent_unchanged() {
  [[ "$(stat -c '%d:%i:%u:%g:%a' -- /opt/autostream)" == \
    "${shared_managed_parent_before}" ]] || \
    die "failed migration did not restore the shared managed parent exactly"
}

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
