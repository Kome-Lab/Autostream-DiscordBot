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
