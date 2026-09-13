reject_stale_public_rollback_anchors() {
  local path=$1
  local restore_nullglob=false
  local -a stale_anchors=()
  if ! shopt -q nullglob; then
    shopt -s nullglob
    restore_nullglob=true
  fi
  stale_anchors=("${path}.rollback-anchor."*)
  if [[ ${restore_nullglob} == true ]]; then
    shopt -u nullglob
  fi
  ((${#stale_anchors[@]} == 0)) || \
    die "stale public rollback anchor requires manual recovery: ${path}"
}

preflight_public_path() {
  local link_path=$1
  local expected_target=$2
  local parent
  local mode
  local source_digest
  local source_identity
  local backup_name
  local backup_path
  backup_name="$(basename -- "${link_path}")" || die "could not resolve the legacy public binary name"
  [[ -n ${backup_name} && ${backup_name} != */* ]] || die "legacy public binary name is unsafe"
  backup_path="${LEGACY_BACKUP_DIR}/${backup_name}"
  parent="$(dirname -- "${link_path}")"
  [[ -d ${parent} && ! -L ${parent} ]] || die "public path parent is missing or unsafe: ${parent}"
  if [[ -L ${link_path} ]]; then
    [[ $(readlink -- "${link_path}") == "${expected_target}" ]] || \
      die "existing public symlink has an unexpected target: ${link_path}"
    previous_public_kind["${link_path}"]="symlink"
    previous_public_target["${link_path}"]="$(readlink -- "${link_path}")"
    previous_public_identity["${link_path}"]="$(stat -c '%d:%i' -- "${link_path}")"
  elif [[ -e ${link_path} ]]; then
    [[ -f ${link_path} ]] || die "existing public path is not a regular file: ${link_path}"
    [[ $(stat -c '%U:%G' -- "${link_path}") == "root:root" ]] || \
      die "legacy public binary must be owned by root:root: ${link_path}"
    [[ -x ${link_path} ]] || die "legacy public binary is not executable: ${link_path}"
    mode="$(stat -c '%a' -- "${link_path}")"
    (( (8#${mode} & 07022) == 0 )) || \
      die "legacy public binary has unsafe write or special mode bits: ${link_path}"
    source_identity="$(stat -c '%d:%i' -- "${link_path}")"
    source_digest="$(sha256sum -- "${link_path}" | awk 'NR == 1 { print $1 }')"
    [[ $(stat -c '%d:%i' -- "${link_path}") == "${source_identity}" ]] || \
      die "legacy public binary changed during preflight: ${link_path}"
    case "${backup_previous_kind["${link_path}"]-unobserved}" in
      regular)
        [[ -f ${backup_path} && ! -L ${backup_path} &&
          $(stat -c '%d:%i' -- "${backup_path}") == "${backup_previous_identity["${link_path}"]}" &&
          $(stat -c '%u:%g:%a' -- "${backup_path}") == \
            "${backup_previous_uid["${link_path}"]}:${backup_previous_gid["${link_path}"]}:${backup_previous_mode["${link_path}"]}" &&
          $(sha256sum -- "${backup_path}" | awk 'NR == 1 { print $1 }') == \
            "${backup_previous_sha256["${link_path}"]}" &&
          ${backup_previous_sha256["${link_path}"]} == "${source_digest}" ]] || \
          die "legacy backup destination changed after preflight: ${link_path}"
        ;;
      absent)
        [[ ! -e ${backup_path} && ! -L ${backup_path} ]] || \
          die "legacy backup destination appeared after preflight: ${link_path}"
        ;;
      *)
        die "legacy backup state was not captured before mutation: ${link_path}"
        ;;
    esac
    previous_public_kind["${link_path}"]="regular"
    previous_public_mode["${link_path}"]="${mode}"
    previous_public_sha256["${link_path}"]="${source_digest}"
    previous_public_identity["${link_path}"]="${source_identity}"
    previous_public_nlink["${link_path}"]="$(stat -c '%h' -- "${link_path}")"
  else
    previous_public_kind["${link_path}"]="absent"
  fi
}

preflight_public_path "${PUBLIC_BINARY}" "${CURRENT_LINK}/bin/autostream-discord-bot"
preflight_public_path "${PUBLIC_ALIAS}" "${PUBLIC_BINARY}"
if [[ ${previous_public_kind["${PUBLIC_BINARY}"]} == "regular" &&
  ${previous_public_kind["${PUBLIC_ALIAS}"]} == "regular" &&
  ${previous_public_identity["${PUBLIC_BINARY}"]} == \
    "${previous_public_identity["${PUBLIC_ALIAS}"]}" ]]; then
  die "legacy canonical public binary and alias must not be hard links to the same inode"
fi
reject_stale_public_rollback_anchors "${PUBLIC_BINARY}"
reject_stale_public_rollback_anchors "${PUBLIC_ALIAS}"
if [[ ${previous_public_kind["${PUBLIC_BINARY}"]} == "absent" ]] &&
  systemctl is-active --quiet "${UNIT_NAME}"; then
  die "active service has no canonical public binary; stop it before installing"
fi
activation_started=true

backup_legacy_binary() {
  local link_path=$1
  local backup_name
  local backup_path
  local backup_stage
  local committed_backup_identity
  local source_digest
  backup_name="$(basename -- "${link_path}")" || die "could not resolve the legacy public binary name"
  [[ -n ${backup_name} && ${backup_name} != */* ]] || die "legacy public binary name is unsafe"
  backup_path="${LEGACY_BACKUP_DIR}/${backup_name}"
  if [[ ! -e ${link_path} || -L ${link_path} ]]; then
    return
  fi
  ensure_private_root_directory "${LEGACY_BACKUP_DIR}"
  source_digest="${previous_public_sha256["${link_path}"]}"
  [[ $(sha256sum -- "${link_path}" | awk 'NR == 1 { print $1 }') == "${source_digest}" ]] || \
    die "legacy public binary changed after preflight: ${link_path}"
  case "${backup_previous_kind["${link_path}"]-unobserved}" in
    regular)
      [[ -f ${backup_path} && ! -L ${backup_path} &&
        $(stat -c '%d:%i' -- "${backup_path}") == "${backup_previous_identity["${link_path}"]}" &&
        $(stat -c '%u:%g:%a' -- "${backup_path}") == \
          "${backup_previous_uid["${link_path}"]}:${backup_previous_gid["${link_path}"]}:${backup_previous_mode["${link_path}"]}" &&
        $(sha256sum -- "${backup_path}" | awk 'NR == 1 { print $1 }') == \
          "${backup_previous_sha256["${link_path}"]}" ]] || \
        die "legacy public binary backup changed after preflight: ${link_path}"
      return
      ;;
    absent)
      [[ ! -e ${backup_path} && ! -L ${backup_path} ]] || \
        die "legacy public binary backup appeared after preflight: ${link_path}"
      ;;
    *)
      die "legacy public binary backup state was not captured: ${link_path}"
      ;;
  esac
  create_registered_temporary_path backup_stage file \
    "${LEGACY_BACKUP_DIR}/.${backup_name}.XXXXXXXX" || \
    die "could not create and journal legacy backup staging"
  install -o root -g root -m 0500 "${link_path}" "${backup_stage}"
  [[ $(sha256sum -- "${link_path}" | awk 'NR == 1 { print $1 }') == "${source_digest}" ]] || \
    die "legacy public binary changed while being backed up: ${link_path}"
  [[ $(sha256sum -- "${backup_stage}" | awk 'NR == 1 { print $1 }') == "${source_digest}" ]] || \
    die "legacy public binary backup differs from its source: ${link_path}"
  backup_commit_failed=false
  begin_installer_signal_transaction
  if ! mv -T -- "${backup_stage}" "${backup_path}"; then
    backup_commit_failed=true
  elif ! committed_backup_identity="$(stat -c '%d:%i' -- "${backup_path}")"; then
    backup_commit_failed=true
  else
    backup_created_identity["${link_path}"]="${committed_backup_identity}"
    forget_temporary_path "${backup_stage}"
  fi
  finish_installer_signal_transaction
  [[ ${backup_commit_failed} == false ]] || \
    die "could not commit and journal the legacy public binary backup: ${link_path}"
  sync -f "${LEGACY_BACKUP_DIR}"
}

prepare_public_rollback_anchor() {
  local path=$1
  local anchor="${path}.rollback-anchor.$$"
  local original_identity="${previous_public_identity["${path}"]-}"
  local original_nlink="${previous_public_nlink["${path}"]-}"
  local expected_nlink
  local transaction_failed=false

  [[ ${previous_public_kind["${path}"]-unknown} == "regular" ]] || return 0
  [[ -n ${original_identity} && ${original_nlink} =~ ^[1-9][0-9]*$ ]] || return 1
  expected_nlink=$((original_nlink + 1))
  [[ -f ${path} && ! -L ${path} &&
    $(stat -c '%d:%i' -- "${path}") == "${original_identity}" &&
    $(stat -c '%h' -- "${path}") == "${original_nlink}" &&
    $(stat -c '%U:%G:%a' -- "${path}") == \
      "root:root:${previous_public_mode["${path}"]}" &&
    $(sha256sum -- "${path}" | awk 'NR == 1 { print $1 }') == \
      "${previous_public_sha256["${path}"]}" ]] || return 1
  [[ ! -e ${anchor} && ! -L ${anchor} ]] || return 1

  # The registration, hard-link creation, inode claim, and public-path mapping
  # form one deferred-signal transaction. Cleanup can therefore remove only an
  # invocation-owned anchor, or deliberately retain it as recovery evidence.
  begin_installer_signal_transaction
  if ! register_temporary_path "${anchor}"; then
    transaction_failed=true
  fi
  if [[ ${transaction_failed} == false ]]; then
    public_rollback_anchor["${path}"]="${anchor}"
    temporary_path_sync_parent["${anchor}"]="$(dirname -- "${path}")"
    protected_temporary_path["${anchor}"]=true
  fi
  if [[ ${transaction_failed} == false ]] && ! ln -T -- "${path}" "${anchor}"; then
    if [[ ! -e ${anchor} && ! -L ${anchor} ]]; then
      forget_temporary_path "${anchor}"
      unset 'public_rollback_anchor['"${path}"']'
    fi
    transaction_failed=true
  fi
  if [[ ${transaction_failed} == false ]] && ! claim_temporary_path "${anchor}"; then
    transaction_failed=true
  fi
  if [[ ${transaction_failed} == false &&
    ${temporary_path_identity["${anchor}"]} != "${original_identity}" ]]; then
    transaction_failed=true
  fi
  finish_installer_signal_transaction
  [[ ${transaction_failed} == false ]] || return 1

  [[ -f ${anchor} && ! -L ${anchor} &&
    $(stat -c '%d:%i' -- "${anchor}") == "${original_identity}" &&
    $(stat -c '%h' -- "${anchor}") == "${expected_nlink}" &&
    $(stat -c '%d:%i' -- "${path}") == "${original_identity}" &&
    $(stat -c '%h' -- "${path}") == "${expected_nlink}" &&
    $(stat -c '%U:%G:%a' -- "${anchor}") == \
      "root:root:${previous_public_mode["${path}"]}" &&
    $(sha256sum -- "${anchor}" | awk 'NR == 1 { print $1 }') == \
      "${previous_public_sha256["${path}"]}" ]] || return 1
}

backup_legacy_binary "${PUBLIC_BINARY}"
backup_legacy_binary "${PUBLIC_ALIAS}"

install_public_link() {
  local target=$1
  local link_path=$2
  local staged_identity
  public_link_next="${link_path}.next.$$"
  [[ ! -e ${public_link_next} && ! -L ${public_link_next} ]] || \
    die "public link staging path already exists: ${public_link_next}"
  create_registered_symlink_path "${target}" "${public_link_next}" || \
    die "could not create and journal public link staging"
  staged_identity="${temporary_path_identity["${public_link_next}"]-pending}"
  [[ ${staged_identity} != "pending" ]] || \
    die "public link staging identity was not journaled: ${link_path}"
  public_link_commit_failed=false
  begin_installer_signal_transaction
  published_public_identity["${link_path}"]="${staged_identity}"
  published_public_target["${link_path}"]="${target}"
  if ! mv -Tf -- "${public_link_next}" "${link_path}"; then
    public_link_commit_failed=true
  elif [[ ! -L ${link_path} ||
    $(stat -c '%d:%i' -- "${link_path}") != "${staged_identity}" ||
    $(readlink -- "${link_path}") != "${target}" ]]; then
    public_link_commit_failed=true
  else
    forget_temporary_path "${public_link_next}"
    public_link_next=""
  fi
  finish_installer_signal_transaction
  [[ ${public_link_commit_failed} == false ]] || \
    die "could not commit the public link: ${link_path}"
  sync -f "$(dirname -- "${link_path}")"
}

if [[ -n ${env_stage} ]]; then
  env_commit_failed=false
  begin_installer_signal_transaction
  if ! mv -Tf -- "${env_stage}" "${ENV_DEST}"; then
    env_commit_failed=true
  else
    env_created=true
    if ! env_created_identity="$(stat -c '%d:%i' -- "${ENV_DEST}")"; then
      env_commit_failed=true
    else
      forget_temporary_path "${env_stage}"
      env_stage=""
    fi
  fi
  finish_installer_signal_transaction
  [[ ${env_commit_failed} == false ]] || \
    die "could not commit and journal the seeded environment file"
  sync -f "$(dirname -- "${ENV_DEST}")"
fi
if [[ -n ${existing_env_sha256} ]]; then
  [[ $(sha256sum -- "${ENV_DEST}" | awk 'NR == 1 { print $1 }') == "${existing_env_sha256}" ]] || \
    die "existing environment file changed during installation"
fi

unit_commit_failed=false
begin_installer_signal_transaction
unit_replaced=true
if ! mv -Tf -- "${unit_stage}" "${UNIT_DEST}"; then
  unit_commit_failed=true
else
  forget_temporary_path "${unit_stage}"
  unit_stage=""
fi
finish_installer_signal_transaction
[[ ${unit_commit_failed} == false ]] || die "could not commit the systemd unit"
sync -f /etc/systemd/system
[[ $(stat -c '%U:%G:%a' -- "${UNIT_DEST}") == "root:root:644" ]] || \
  die "installed systemd unit failed ownership or mode verification"
systemctl daemon-reload

# Prepare current while a legacy active unit still resolves its unchanged
# regular public binary. Public links are the final activation mutation.
if [[ ! -L ${CURRENT_LINK} ]] || [[ $(readlink -f -- "${CURRENT_LINK}") != "${RELEASE_DIR}" ]]; then
  current_next="${MANAGED_ROOT}/.current.next.$$"
  [[ ! -e ${current_next} && ! -L ${current_next} ]] || die "managed current staging path already exists"
  create_registered_symlink_path "${RELEASE_DIR}" "${current_next}" || \
    die "could not create and journal managed current staging"
  current_link_commit_failed=false
  begin_installer_signal_transaction
  if ! mv -Tf -- "${current_next}" "${CURRENT_LINK}"; then
    current_link_commit_failed=true
  else
    forget_temporary_path "${current_next}"
    current_next=""
  fi
  finish_installer_signal_transaction
  [[ ${current_link_commit_failed} == false ]] || \
    die "could not commit the managed current link"
  sync -f "${MANAGED_ROOT}"
fi
[[ $(readlink -f -- "${CURRENT_LINK}") == "${RELEASE_DIR}" ]] || die "managed current link verification failed"

# The alias is not used by ExecStart, so switch it first and commit the
# canonical systemd path last.
prepare_public_rollback_anchor "${PUBLIC_BINARY}" || \
  die "could not create and journal the canonical public rollback anchor"
prepare_public_rollback_anchor "${PUBLIC_ALIAS}" || \
  die "could not create and journal the compatibility public rollback anchor"
# Make both original-inode anchors durable before either public name changes.
sync -f /usr/local/bin
install_public_link "${PUBLIC_BINARY}" "${PUBLIC_ALIAS}"
[[ $(readlink -- "${PUBLIC_ALIAS}") == "${PUBLIC_BINARY}" ]] || \
  die "public compatibility symlink target is incorrect"
install_public_link "${CURRENT_LINK}/bin/autostream-discord-bot" "${PUBLIC_BINARY}"
[[ $(readlink -- "${PUBLIC_BINARY}") == "${CURRENT_LINK}/bin/autostream-discord-bot" ]] || \
  die "public canonical binary symlink target is incorrect"
[[ $(readlink -f -- "${PUBLIC_BINARY}") == "${RELEASE_DIR}/bin/autostream-discord-bot" ]] || \
  die "public canonical binary link verification failed"
[[ $(readlink -f -- "${PUBLIC_ALIAS}") == "${RELEASE_DIR}/bin/autostream-discord-bot" ]] || \
  die "public compatibility binary link verification failed"
installation_complete=true

printf 'Installed AutoStream Discord Bot %s.\n' "${VERSION}"
printf 'Existing settings were preserved. Review %s and run the Panel-generated configure command.\n' "${ENV_DEST}"
if systemctl is-active --quiet "${UNIT_NAME}"; then
  printf 'The service is already active; load the installed release with:\n'
  printf '  sudo systemctl restart %s\n' "${UNIT_NAME}"
else
  printf 'Start this service for the first time with:\n'
  printf '  sudo systemctl enable --now %s\n' "${UNIT_NAME}"
fi
printf '  sudo systemctl status %s\n' "${UNIT_NAME}"
