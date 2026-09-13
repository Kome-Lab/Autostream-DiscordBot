
preflight_existing_host_paths() {
  local backup_digest
  local backup_name
  local backup_path
  local current_release
  local env_mode
  local marker_digest
  local marker_version
  local mode
  local path
  local source_digest
  local unit_mode

  preflight_state_directory

  if [[ -e ${ENV_DEST} || -L ${ENV_DEST} ]]; then
    [[ -f ${ENV_DEST} && ! -L ${ENV_DEST} ]] || \
      die "existing environment file is not a regular file"
    [[ $(stat -c '%U' -- "${ENV_DEST}") == "root" ]] || \
      die "existing environment file must be owned by root"
    env_mode="$(stat -c '%a' -- "${ENV_DEST}")"
    [[ ${env_mode} == "600" || ${env_mode} == "640" ]] || \
      die "existing environment file must use mode 0600 or 0640"
  fi

  if [[ -e ${UNIT_DEST} || -L ${UNIT_DEST} ]]; then
    [[ -f ${UNIT_DEST} && ! -L ${UNIT_DEST} ]] || \
      die "existing systemd unit is not a regular file"
    [[ $(stat -c '%U:%G' -- "${UNIT_DEST}") == "root:root" ]] || \
      die "existing systemd unit must be owned by root:root"
    unit_mode="$(stat -c '%a' -- "${UNIT_DEST}")"
    (( (8#${unit_mode} & 0022) == 0 )) || \
      die "existing systemd unit must not be group/other writable"
  fi

  if [[ -e ${CURRENT_LINK} || -L ${CURRENT_LINK} ]]; then
    [[ -L ${CURRENT_LINK} ]] || die "managed current path must be a symlink"
    [[ $(stat -c '%U:%G' -- "${CURRENT_LINK}") == "root:root" ]] || \
      die "managed current link must be owned by root:root"
    current_release="$(readlink -f -- "${CURRENT_LINK}")" || \
      die "managed current link is dangling"
    [[ -d ${current_release} && ! -L ${current_release} &&
      $(dirname -- "${current_release}") == "${RELEASES_DIR}" ]] || \
      die "managed current target is missing, unsafe, or outside the releases directory"
    [[ -f ${current_release}/.artifact-sha256 &&
      ! -L ${current_release}/.artifact-sha256 &&
      -f ${current_release}/.version &&
      ! -L ${current_release}/.version &&
      -f ${current_release}/checksums.txt &&
      ! -L ${current_release}/checksums.txt ]] || \
      die "managed current release markers are missing or unsafe"
    marker_digest="$(< "${current_release}/.artifact-sha256")"
    marker_version="$(< "${current_release}/.version")"
    [[ ${marker_digest} =~ ^[0-9a-f]{64}$ ]] || \
      die "managed current digest marker is invalid"
    [[ ${marker_version} =~ ^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z]+(\.[0-9A-Za-z]+)*)?$ ]] || \
      die "managed current version marker is invalid"
    [[ $(basename -- "${current_release}") == "${marker_version}-${marker_digest:0:12}" ]] || \
      die "managed current release name does not match its markers"
  fi

  for path in "${PUBLIC_BINARY}" "${PUBLIC_ALIAS}"; do
    if [[ -L ${path} ]]; then
      backup_previous_kind["${path}"]="not-applicable"
      if [[ ${path} == "${PUBLIC_BINARY}" ]]; then
        [[ $(readlink -- "${path}") == "${CURRENT_LINK}/bin/autostream-discord-bot" ]] || \
          die "existing public symlink has an unexpected target: ${path}"
      else
        [[ $(readlink -- "${path}") == "${PUBLIC_BINARY}" ]] || \
          die "existing public symlink has an unexpected target: ${path}"
      fi
    elif [[ -e ${path} ]]; then
      [[ -f ${path} ]] || die "existing public path is not a regular file: ${path}"
      [[ $(stat -c '%U:%G' -- "${path}") == "root:root" ]] || \
        die "legacy public binary must be owned by root:root: ${path}"
      [[ -x ${path} ]] || die "legacy public binary is not executable: ${path}"
      mode="$(stat -c '%a' -- "${path}")"
      (( (8#${mode} & 07022) == 0 )) || \
        die "legacy public binary has unsafe write or special mode bits: ${path}"
      backup_name="$(basename -- "${path}")"
      backup_path="${LEGACY_BACKUP_DIR}/${backup_name}"
      if [[ -e ${backup_path} || -L ${backup_path} ]]; then
        [[ -f ${backup_path} && ! -L ${backup_path} &&
          $(stat -c '%U:%G:%a' -- "${backup_path}") == "root:root:500" ]] || \
          die "legacy backup destination conflicts with ${path}"
        backup_digest="$(sha256sum -- "${backup_path}" | awk 'NR == 1 { print $1 }')"
        source_digest="$(sha256sum -- "${path}" | awk 'NR == 1 { print $1 }')"
        [[ ${backup_digest} == "${source_digest}" ]] || \
          die "legacy backup destination conflicts with ${path}"
        backup_previous_kind["${path}"]="regular"
        backup_previous_identity["${path}"]="$(stat -c '%d:%i' -- "${backup_path}")"
        backup_previous_uid["${path}"]="$(stat -c '%u' -- "${backup_path}")"
        backup_previous_gid["${path}"]="$(stat -c '%g' -- "${backup_path}")"
        backup_previous_mode["${path}"]="$(stat -c '%a' -- "${backup_path}")"
        backup_previous_sha256["${path}"]="${backup_digest}"
      else
        backup_previous_kind["${path}"]="absent"
      fi
    else
      backup_previous_kind["${path}"]="not-applicable"
    fi
  done

  if [[ ! -e ${PUBLIC_BINARY} && ! -L ${PUBLIC_BINARY} ]] &&
    systemctl is-active --quiet "${UNIT_NAME}"; then
    die "active service has no canonical public binary; stop it before installing"
  fi
}

[[ -d /run && ! -L /run && $(readlink -f -- /run) == "/run" &&
  $(stat -c '%U:%G' -- /run) == "root:root" ]] || \
  die "permanent updater lock root is unsafe"
run_mode="$(stat -c '%a' -- /run)"
(( (8#${run_mode} & 07022) == 0 )) || \
  die "permanent updater lock root has unsafe write or special mode bits"
ensure_permanent_lock_directory /run/autostream-updater

# Service installers share account and parent-directory mutations, so this
# permanent host-setup lock is held through success or rollback. Like the
# target lock below, it is never unlinked by an installer.
readonly SHARED_HOST_SETUP_LOCK="/run/autostream-updater/.autostream-runtime-host-setup.lock"
ensure_permanent_lock_path_atomically "${SHARED_HOST_SETUP_LOCK}"
exec 8<>"${SHARED_HOST_SETUP_LOCK}"
chown root:root /proc/self/fd/8
chmod 0600 /proc/self/fd/8
shared_lock_fd_identity="$(stat -Lc '%d:%i' -- /proc/self/fd/8)"
[[ -f ${SHARED_HOST_SETUP_LOCK} && ! -L ${SHARED_HOST_SETUP_LOCK} &&
  -f /proc/self/fd/8 &&
  $(stat -Lc '%U:%G:%a' -- /proc/self/fd/8) == "root:root:600" &&
  $(stat -c '%U:%G:%a' -- "${SHARED_HOST_SETUP_LOCK}") == "root:root:600" &&
  ${shared_lock_fd_identity} == "$(stat -c '%d:%i' -- "${SHARED_HOST_SETUP_LOCK}")" ]] || \
  die "shared host-setup lock identity changed while being opened"
flock -n 8 || die "another AutoStream installer is provisioning shared host state"
[[ -f ${SHARED_HOST_SETUP_LOCK} && ! -L ${SHARED_HOST_SETUP_LOCK} &&
  -f /proc/self/fd/8 &&
  $(stat -Lc '%U:%G:%a' -- /proc/self/fd/8) == "root:root:600" &&
  $(stat -c '%U:%G:%a' -- "${SHARED_HOST_SETUP_LOCK}") == "root:root:600" &&
  ${shared_lock_fd_identity} == "$(stat -c '%d:%i' -- "${SHARED_HOST_SETUP_LOCK}")" ]] || \
  die "shared host-setup lock identity changed after acquisition"

ensure_root_anchor_directory /usr
ensure_root_anchor_directory /usr/local
ensure_root_anchor_directory /usr/local/bin
ensure_root_anchor_directory /etc
ensure_root_anchor_directory /etc/systemd
ensure_root_anchor_directory /etc/systemd/system
ensure_root_anchor_directory /opt
ensure_root_anchor_directory /var
ensure_root_anchor_directory /var/lib
ensure_root_anchor_directory /var/backups
ensure_root_anchor_directory /run
TARGET_LOCK_ID="$(printf '%s' "${UNIT_NAME}" | sha256sum | awk 'NR == 1 { print substr($1, 1, 12) }')"
[[ ${TARGET_LOCK_ID} =~ ^[0-9a-f]{12}$ ]] || die "could not derive the updater target lock identifier"
readonly TARGET_LOCK_ID
readonly TARGET_LOCK="/run/autostream-updater/.autostream-updater-${TARGET_LOCK_ID}.lock"
ensure_permanent_lock_path_atomically "${TARGET_LOCK}"
exec 9<>"${TARGET_LOCK}"
chown root:root /proc/self/fd/9
chmod 0600 /proc/self/fd/9
target_lock_fd_identity="$(stat -Lc '%d:%i' -- /proc/self/fd/9)"
[[ -f ${TARGET_LOCK} && ! -L ${TARGET_LOCK} &&
  -f /proc/self/fd/9 &&
  $(stat -Lc '%U:%G:%a' -- /proc/self/fd/9) == "root:root:600" &&
  $(stat -c '%U:%G:%a' -- "${TARGET_LOCK}") == "root:root:600" &&
  ${target_lock_fd_identity} == "$(stat -c '%d:%i' -- "${TARGET_LOCK}")" ]] || \
  die "updater target lock identity changed while being opened"
flock -n 9 || die "another privileged update is already active for ${UNIT_NAME}"
[[ -f ${TARGET_LOCK} && ! -L ${TARGET_LOCK} &&
  -f /proc/self/fd/9 &&
  $(stat -Lc '%U:%G:%a' -- /proc/self/fd/9) == "root:root:600" &&
  $(stat -c '%U:%G:%a' -- "${TARGET_LOCK}") == "root:root:600" &&
  ${target_lock_fd_identity} == "$(stat -c '%d:%i' -- "${TARGET_LOCK}")" ]] || \
  die "updater target lock identity changed after acquisition"

preflight_existing_host_paths
prepare_autostream_user_rollback_login || \
  die "could not reserve a safe service-account rollback login"

if ! getent group autostream >/dev/null; then
  group_creation_failed=false
  begin_installer_signal_transaction
  if ! groupadd --system autostream; then
    group_creation_failed=true
  else
    created_autostream_group=true
    if ! created_autostream_group_record="$(getent group autostream)"; then
      group_creation_failed=true
    fi
  fi
  finish_installer_signal_transaction
  [[ ${group_creation_failed} == false ]] || \
    die "could not create and journal the autostream service group"
  [[ -n ${created_autostream_group_record} ]] || \
    die "could not capture the created autostream service group"
else
  if ! preexisting_autostream_group_record="$(getent group autostream)"; then
    die "could not capture the pre-existing autostream service group"
  fi
  [[ -n ${preexisting_autostream_group_record} ]] || \
    die "could not capture the pre-existing autostream service group"
fi
autostream_group_gid="$(getent group autostream | awk -F: 'NR == 1 { print $3 }')"
[[ ${autostream_group_gid} =~ ^[0-9]+$ && ${autostream_group_gid} -ne 0 ]] || \
  die "autostream service group must have a non-root numeric gid"
if [[ ${created_autostream_group} == true ]]; then
  [[ $(getent group autostream) == "${created_autostream_group_record}" ]] || \
    die "created autostream service group changed before user provisioning"
else
  [[ $(getent group autostream) == "${preexisting_autostream_group_record}" ]] || \
    die "pre-existing autostream service group changed before user provisioning"
fi
if ! id autostream >/dev/null 2>&1; then
  [[ ${autostream_user_rollback_login_ready} == true &&
    -z $(getent passwd "${AUTOSTREAM_USER_ROLLBACK_LOGIN}" 2>/dev/null || true) &&
    -z $(getent group "${AUTOSTREAM_USER_ROLLBACK_LOGIN}" 2>/dev/null || true) ]] || \
    die "service-account rollback login changed before user provisioning"
  local_account_member_fields_are_clear || \
    die "service-account names are referenced by local group membership state"
  user_creation_failed=false
  begin_installer_signal_transaction
  if ! useradd --system --gid "${autostream_group_gid}" --home-dir /var/lib/autostream \
    --no-create-home --shell /usr/sbin/nologin autostream; then
    user_creation_failed=true
  else
    created_autostream_user=true
    if ! created_autostream_user_record="$(getent passwd autostream)"; then
      user_creation_failed=true
    fi
  fi
  finish_installer_signal_transaction
  [[ ${user_creation_failed} == false ]] || \
    die "could not create and journal the autostream service account"
  [[ -n ${created_autostream_user_record} ]] || \
    die "could not capture the created autostream service account"
fi
[[ $(id -u autostream) -ne 0 ]] || die "autostream service account must not be root"
[[ $(id -g autostream) == "${autostream_group_gid}" ]] || \
  die "autostream service account must use the verified autostream group"
[[ $(id -gn autostream) == "autostream" ]] || die "autostream service account must use the autostream primary group"

ensure_managed_directory /opt/autostream
ensure_managed_directory "${MANAGED_ROOT}"
ensure_managed_directory "${RELEASES_DIR}"
ensure_directory /var/lib/autostream root root 0755
state_directory_mutation_started=true
ensure_state_directory
ensure_private_root_directory "${BACKUP_BASE}"
ensure_private_root_directory "${BACKUP_ROOT}"
ensure_private_root_directory "${BACKUP_DIR}"
ensure_directory /etc/autostream root root 0750
