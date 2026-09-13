
ensure_directory() {
  local path=$1
  local owner=$2
  local group=$3
  local mode=$4
  local created_identity_variable="${5-}"
  local existing_mode
  local existing_owner_group
  if [[ -e ${path} || -L ${path} ]]; then
    [[ -d ${path} && ! -L ${path} ]] || die "required directory path is unsafe: ${path}"
    [[ $(readlink -f -- "${path}") == "${path}" ]] || die "required directory path is not canonical: ${path}"
    existing_owner_group="$(stat -c '%U:%G' -- "${path}")"
    [[ ${existing_owner_group} == "${owner}:${group}" ]] || \
      die "existing directory must be owned by ${owner}:${group}: ${path}"
    existing_mode="$(stat -c '%a' -- "${path}")"
    (( (8#${existing_mode} & 07022) == 0 )) || \
      die "existing directory has unsafe write or special mode bits: ${path}"
  fi
  journal_directory_before_mutation "${path}" || die "could not journal directory before mutation: ${path}"
  install_journaled_directory \
    "${path}" "${owner}" "${group}" "${mode}" "${created_identity_variable}" || \
    die "could not create, normalize, or journal directory: ${path}"
  [[ $(readlink -f -- "${path}") == "${path}" ]] || die "required directory path is not canonical: ${path}"
  [[ $(stat -c '%U:%G:%a' -- "${path}") == "${owner}:${group}:${mode#0}" ]] || \
    die "directory ownership or mode normalization failed: ${path}"
}

ensure_managed_directory() {
  local path=$1
  local mode
  if [[ -e ${path} || -L ${path} ]]; then
    [[ -d ${path} && ! -L ${path} ]] || die "managed directory path is unsafe: ${path}"
    [[ $(readlink -f -- "${path}") == "${path}" ]] || die "managed directory path is not canonical: ${path}"
    [[ $(stat -c '%U:%G' -- "${path}") == "root:root" ]] || \
      die "managed directory must be owned by root:root: ${path}"
    mode="$(stat -c '%a' -- "${path}")"
    (( (8#${mode} & 07022) == 0 )) || \
      die "managed directory has unsafe write or special mode bits: ${path}"
  fi
  journal_directory_before_mutation "${path}" || die "could not journal managed directory: ${path}"
  install_journaled_directory "${path}" root root 0755 || \
    die "could not create, normalize, or journal managed directory: ${path}"
  [[ $(readlink -f -- "${path}") == "${path}" ]] || die "managed directory path is not canonical: ${path}"
  [[ $(stat -c '%U:%G:%a' -- "${path}") == "root:root:755" ]] || \
    die "managed directory ownership or mode normalization failed: ${path}"
}

ensure_private_root_directory() {
  local path=$1
  local mode
  if [[ -e ${path} || -L ${path} ]]; then
    [[ -d ${path} && ! -L ${path} ]] || die "private root directory path is unsafe: ${path}"
    [[ $(stat -c '%U:%G' -- "${path}") == "root:root" ]] || \
      die "private root directory must be owned by root:root: ${path}"
    mode="$(stat -c '%a' -- "${path}")"
    (( (8#${mode} & 07077) == 0 )) || \
      die "private root directory has unsafe group/other or special mode bits: ${path}"
  fi
  journal_directory_before_mutation "${path}" || die "could not journal private root directory: ${path}"
  install_journaled_directory "${path}" root root 0700 || \
    die "could not create, normalize, or journal private root directory: ${path}"
  [[ $(readlink -f -- "${path}") == "${path}" ]] || die "private root directory path is not canonical: ${path}"
  [[ $(stat -c '%U:%G:%a' -- "${path}") == "root:root:700" ]] || \
    die "private root directory ownership or mode normalization failed: ${path}"
}

ensure_root_anchor_directory() {
  local path=$1
  local mode
  if [[ -e ${path} || -L ${path} ]]; then
    [[ -d ${path} && ! -L ${path} ]] || die "root anchor directory path is unsafe: ${path}"
    [[ $(readlink -f -- "${path}") == "${path}" ]] || die "root anchor directory path is not canonical: ${path}"
    [[ $(stat -c '%U:%G' -- "${path}") == "root:root" ]] || \
      die "root anchor directory must be owned by root:root: ${path}"
    mode="$(stat -c '%a' -- "${path}")"
    (( (8#${mode} & 07022) == 0 )) || \
      die "root anchor directory has unsafe write or special mode bits: ${path}"
    journal_directory_before_mutation "${path}" || die "could not journal root anchor directory: ${path}"
    return
  fi
  journal_directory_before_mutation "${path}" || die "could not journal root anchor directory: ${path}"
  install_journaled_directory "${path}" root root 0755 || \
    die "could not create or journal root anchor directory: ${path}"
  [[ $(readlink -f -- "${path}") == "${path}" ]] || die "root anchor directory path is not canonical: ${path}"
  [[ $(stat -c '%U:%G:%a' -- "${path}") == "root:root:755" ]] || \
    die "root anchor directory ownership or mode normalization failed: ${path}"
}

ensure_permanent_lock_directory() {
  local path=$1
  local mode
  if [[ -e ${path} || -L ${path} ]]; then
    [[ -d ${path} && ! -L ${path} &&
      $(readlink -f -- "${path}") == "${path}" &&
      $(stat -c '%U:%G' -- "${path}") == "root:root" ]] || \
      die "permanent updater lock directory is unsafe: ${path}"
    mode="$(stat -c '%a' -- "${path}")"
    (( (8#${mode} & 07077) == 0 )) || \
      die "permanent updater lock directory grants unsafe permissions: ${path}"
  fi
  # This permanent root-only directory and its lock files intentionally survive
  # failed installs so every future invocation serializes on the same inode.
  install -d -o root -g root -m 0700 "${path}"
  [[ -d ${path} && ! -L ${path} &&
    $(readlink -f -- "${path}") == "${path}" &&
    $(stat -c '%U:%G:%a' -- "${path}") == "root:root:700" ]] || \
    die "permanent updater lock directory normalization failed: ${path}"
}

ensure_permanent_lock_path_atomically() {
  local path=$1
  local lock_create_stage=""
  local lock_mode

  if [[ ! -e ${path} && ! -L ${path} ]]; then
    create_registered_temporary_path lock_create_stage file \
      /run/autostream-updater/.lock-create.XXXXXXXX || \
      die "could not create updater lock candidate"
    [[ -f ${lock_create_stage} &&
      ! -L ${lock_create_stage} &&
      $(stat -c '%U:%G:%a' -- "${lock_create_stage}") == "root:root:600" ]] || \
      die "could not create a safe updater lock candidate"
    if ln -- "${lock_create_stage}" "${path}" 2>/dev/null; then
      sync -f /run/autostream-updater
    elif [[ ! -e ${path} && ! -L ${path} ]]; then
      die "could not atomically create a permanent updater lock"
    fi
    rm -f -- "${lock_create_stage}"
    forget_temporary_path "${lock_create_stage}"
  fi
  [[ -f ${path} && ! -L ${path} ]] || \
    die "updater lock path is unsafe: ${path}"
  [[ $(stat -c '%U:%G' -- "${path}") == "root:root" ]] || \
    die "updater lock must be owned by root:root: ${path}"
  lock_mode="$(stat -c '%a' -- "${path}")"
  (( (8#${lock_mode} & 0077) == 0 )) || \
    die "updater lock must not grant group/other permissions: ${path}"
}

readonly RELEASE_DIR="${RELEASES_DIR}/${VERSION}-${ARTIFACT_SHA256:0:12}"
readonly LEGACY_BACKUP_DIR="${BACKUP_DIR}/${VERSION}-${ARTIFACT_SHA256:0:12}"

preflight_state_directory() {
  local mode
  if [[ ! -e ${STATE_DIR} && ! -L ${STATE_DIR} ]]; then
    return
  fi
  [[ -d ${STATE_DIR} && ! -L ${STATE_DIR} ]] || \
    die "existing state directory path is unsafe: ${STATE_DIR}"
  [[ $(readlink -f -- "${STATE_DIR}") == "${STATE_DIR}" ]] || \
    die "existing state directory path is not canonical: ${STATE_DIR}"
  [[ $(stat -c '%U:%G' -- "${STATE_DIR}") == "autostream:autostream" ]] || \
    die "existing state directory must be owned by autostream:autostream"
  mode="$(stat -c '%a' -- "${STATE_DIR}")"
  [[ ${mode} == "700" || ${mode} == "750" ]] || \
    die "existing state directory must use mode 0700 or 0750"
  state_dir_preexisting=true
  state_dir_identity="$(stat -c '%d:%i' -- "${STATE_DIR}")"
  state_dir_metadata="$(stat -c '%U:%G:%a' -- "${STATE_DIR}")"
}

ensure_state_directory() {
  if [[ ${state_dir_preexisting} == true ]]; then
    [[ -d ${STATE_DIR} && ! -L ${STATE_DIR} &&
      $(readlink -f -- "${STATE_DIR}") == "${STATE_DIR}" &&
      $(stat -c '%d:%i' -- "${STATE_DIR}") == "${state_dir_identity}" &&
      $(stat -c '%U:%G:%a' -- "${STATE_DIR}") == "${state_dir_metadata}" ]] || \
      die "existing state directory changed after preflight"
    return
  fi
  ensure_directory \
    "${STATE_DIR}" \
    autostream \
    autostream \
    0750 \
    state_directory_created_identity
}
