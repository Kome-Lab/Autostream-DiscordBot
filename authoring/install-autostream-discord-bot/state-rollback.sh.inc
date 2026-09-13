
restore_existing_state_directory() {
  local parent
  [[ ${state_directory_mutation_started} == true ]] || return 0
  if [[ ${state_dir_preexisting} == true ]]; then
    [[ -d ${STATE_DIR} && ! -L ${STATE_DIR} &&
      $(stat -c '%d:%i' -- "${STATE_DIR}") == "${state_dir_identity}" &&
      $(stat -c '%U:%G:%a' -- "${STATE_DIR}") == "${state_dir_metadata}" ]] || return 1
    return 0
  fi
  if [[ -e ${STATE_DIR} || -L ${STATE_DIR} ]]; then
    [[ -d ${STATE_DIR} && ! -L ${STATE_DIR} &&
      -n ${state_directory_created_identity} &&
      $(stat -c '%d:%i' -- "${STATE_DIR}") == "${state_directory_created_identity}" &&
      -z $(find "${STATE_DIR}" -mindepth 1 -print -quit) ]] || return 1
    rmdir -- "${STATE_DIR}" || return 1
  fi
  [[ ! -e ${STATE_DIR} && ! -L ${STATE_DIR} ]] || return 1
  parent="$(dirname -- "${STATE_DIR}")"
  sync -f "${parent}" || return 1
}

restore_current_state() {
  local restore_next="${MANAGED_ROOT}/.current.rollback.$$"
  [[ ! -e ${restore_next} && ! -L ${restore_next} ]] || return 1
  case "${previous_current_kind}" in
    absent)
      if [[ -e ${CURRENT_LINK} && ! -L ${CURRENT_LINK} ]]; then
        return 1
      fi
      rm -f -- "${CURRENT_LINK}" || return 1
      [[ ! -e ${CURRENT_LINK} && ! -L ${CURRENT_LINK} ]] || return 1
      ;;
    symlink)
      if [[ -e ${CURRENT_LINK} && ! -L ${CURRENT_LINK} ]]; then
        return 1
      fi
      create_registered_symlink_path \
        "${previous_current_target}" \
        "${restore_next}" || return 1
      mv -Tf -- "${restore_next}" "${CURRENT_LINK}" || return 1
      forget_temporary_path "${restore_next}"
      [[ -L ${CURRENT_LINK} &&
        $(readlink -- "${CURRENT_LINK}") == "${previous_current_target}" ]] || return 1
      ;;
    *)
      return 1
      ;;
  esac
  sync -f "${MANAGED_ROOT}" || return 1
}

restore_legacy_backup_state() {
  local path=$1
  local backup_name
  local backup_path
  backup_name="$(basename -- "${path}")" || return 1
  backup_path="${LEGACY_BACKUP_DIR}/${backup_name}"
  case "${backup_previous_kind["${path}"]-not-applicable}" in
    not-applicable)
      return 0
      ;;
    regular)
      [[ -f ${backup_path} && ! -L ${backup_path} &&
        $(stat -c '%d:%i' -- "${backup_path}") == "${backup_previous_identity["${path}"]}" &&
        $(stat -c '%u:%g:%a' -- "${backup_path}") == \
          "${backup_previous_uid["${path}"]}:${backup_previous_gid["${path}"]}:${backup_previous_mode["${path}"]}" &&
        $(sha256sum -- "${backup_path}" | awk 'NR == 1 { print $1 }') == \
          "${backup_previous_sha256["${path}"]}" ]] || return 1
      ;;
    absent)
      if [[ -z ${backup_created_identity["${path}"]-} ]]; then
        [[ ! -e ${backup_path} && ! -L ${backup_path} ]] || return 1
        return 0
      fi
      [[ -f ${backup_path} && ! -L ${backup_path} &&
        $(stat -c '%d:%i' -- "${backup_path}") == "${backup_created_identity["${path}"]}" ]] || return 1
      rm -f -- "${backup_path}" || return 1
      [[ ! -e ${backup_path} && ! -L ${backup_path} ]] || return 1
      sync -f "${LEGACY_BACKUP_DIR}" || return 1
      backup_created_identity["${path}"]=""
      ;;
    *)
      return 1
      ;;
  esac
}

observed_path_identity() {
  local path=$1
  if [[ -z ${path} || ( ! -e ${path} && ! -L ${path} ) ]]; then
    printf '%s' "absent"
  elif ! stat -c '%d:%i' -- "${path}"; then
    printf '%s' "unreadable"
  fi
}

observed_path_nlink() {
  local path=$1
  if [[ -z ${path} || ( ! -e ${path} && ! -L ${path} ) ]]; then
    printf '%s' "absent"
  elif ! stat -c '%h' -- "${path}"; then
    printf '%s' "unreadable"
  fi
}

remove_unchanged_public_rollback_anchor() {
  local path=$1
  local anchor="${public_rollback_anchor["${path}"]-}"
  local original_identity="${previous_public_identity["${path}"]-}"
  local original_nlink="${previous_public_nlink["${path}"]-}"
  local anchored_nlink
  local sync_parent
  [[ -n ${anchor} ]] || return 0
  [[ ${original_nlink} =~ ^[1-9][0-9]*$ ]] || return 1
  anchored_nlink=$((original_nlink + 1))
  sync_parent="$(dirname -- "${path}")" || return 1
  [[ ${protected_temporary_path["${anchor}"]-false} == true &&
    ${temporary_path_identity["${anchor}"]-pending} == "${original_identity}" &&
    -f ${anchor} && ! -L ${anchor} &&
    $(stat -c '%d:%i' -- "${anchor}") == "${original_identity}" &&
    $(stat -c '%h' -- "${anchor}") == "${anchored_nlink}" &&
    $(stat -c '%U:%G:%a' -- "${anchor}") == \
      "root:root:${previous_public_mode["${path}"]}" &&
    $(sha256sum -- "${anchor}" | awk 'NR == 1 { print $1 }') == \
      "${previous_public_sha256["${path}"]}" ]] || return 1
  rm -f -- "${anchor}" || return 1
  [[ ! -e ${anchor} && ! -L ${anchor} ]] || return 1
  sync -f "${sync_parent}" || return 1
  forget_temporary_path "${anchor}"
  unset 'public_rollback_anchor['"${path}"']'
}

completed_public_anchor_is_safe_to_remove() {
  local path=$1
  local anchor="${public_rollback_anchor["${path}"]-}"
  local original_identity="${previous_public_identity["${path}"]-}"
  local original_nlink="${previous_public_nlink["${path}"]-}"
  local published_identity="${published_public_identity["${path}"]-}"
  local published_target="${published_public_target["${path}"]-}"
  [[ ${previous_public_kind["${path}"]-unknown} == "regular" ]] || return 0
  [[ -n ${anchor} && -n ${original_identity} && -n ${published_identity} &&
    -n ${published_target} && ${original_nlink} =~ ^[1-9][0-9]*$ ]] || return 1
  [[ ${protected_temporary_path["${anchor}"]-false} == true &&
    ${temporary_path_identity["${anchor}"]-pending} == "${original_identity}" &&
    -f ${anchor} && ! -L ${anchor} &&
    $(stat -c '%d:%i' -- "${anchor}") == "${original_identity}" &&
    $(stat -c '%h' -- "${anchor}") == "${original_nlink}" &&
    $(stat -c '%U:%G:%a' -- "${anchor}") == \
      "root:root:${previous_public_mode["${path}"]}" &&
    $(sha256sum -- "${anchor}" | awk 'NR == 1 { print $1 }') == \
      "${previous_public_sha256["${path}"]}" &&
    -L ${path} && $(stat -c '%d:%i' -- "${path}") == "${published_identity}" &&
    $(readlink -- "${path}") == "${published_target}" ]]
}

release_completed_public_rollback_anchors() {
  local path
  local anchor
  local sync_parent
  for path in "${PUBLIC_BINARY}" "${PUBLIC_ALIAS}"; do
    completed_public_anchor_is_safe_to_remove "${path}" || return 1
  done
  for path in "${PUBLIC_BINARY}" "${PUBLIC_ALIAS}"; do
    if [[ ${previous_public_kind["${path}"]-unknown} == "regular" ]]; then
      completed_public_anchor_is_safe_to_remove "${path}" || return 1
      anchor="${public_rollback_anchor["${path}"]}"
      sync_parent="$(dirname -- "${anchor}")" || return 1
      rm -f -- "${anchor}" || return 1
      [[ ! -e ${anchor} && ! -L ${anchor} ]] || return 1
      sync -f "${sync_parent}" || return 1
      forget_temporary_path "${anchor}"
      unset 'public_rollback_anchor['"${path}"']'
    fi
  done
}

restore_public_state() {
  local path=$1
  local kind="${previous_public_kind["${path}"]}"
  local restore_next="${path}.rollback.$$"
  local anchor
  local original_identity="${previous_public_identity["${path}"]-}"
  local published_identity="${published_public_identity["${path}"]-}"
  local published_target="${published_public_target["${path}"]-}"
  local anchor_restored=false
  if [[ -e ${path} && ! -L ${path} && ! -f ${path} ]]; then
    return 1
  fi
  case "${kind}" in
    absent)
      if [[ -e ${path} || -L ${path} ]]; then
        [[ -n ${published_identity} && -n ${published_target} && -L ${path} &&
          $(stat -c '%d:%i' -- "${path}") == "${published_identity}" &&
          $(readlink -- "${path}") == "${published_target}" ]] || return 1
        rm -f -- "${path}" || return 1
      fi
      [[ ! -e ${path} && ! -L ${path} ]] || return 1
      ;;
    symlink)
      if [[ -L ${path} && -n ${original_identity} &&
        $(stat -c '%d:%i' -- "${path}") == "${original_identity}" &&
        $(readlink -- "${path}") == "${previous_public_target["${path}"]}" ]]; then
        :
      else
        [[ ! -e ${restore_next} && ! -L ${restore_next} ]] || return 1
        if [[ -e ${path} || -L ${path} ]]; then
          [[ -n ${published_identity} && -n ${published_target} && -L ${path} &&
            $(stat -c '%d:%i' -- "${path}") == "${published_identity}" &&
            $(readlink -- "${path}") == "${published_target}" ]] || return 1
        fi
        create_registered_symlink_path \
          "${previous_public_target["${path}"]}" \
          "${restore_next}" || return 1
        mv -Tf -- "${restore_next}" "${path}" || return 1
        forget_temporary_path "${restore_next}"
      fi
      [[ -L ${path} && -n ${original_identity} &&
        $(readlink -- "${path}") == "${previous_public_target["${path}"]}" ]] || return 1
      ;;
    regular)
      [[ -n ${original_identity} ]] || return 1
      if [[ -f ${path} && ! -L ${path} &&
        $(stat -c '%d:%i' -- "${path}") == "${original_identity}" ]]; then
        :
      else
        anchor="${public_rollback_anchor["${path}"]-}"
        [[ -n ${anchor} && -f ${anchor} && ! -L ${anchor} &&
          ${temporary_path_identity["${anchor}"]-pending} == "${original_identity}" &&
          $(stat -c '%d:%i' -- "${anchor}") == "${original_identity}" ]] || return 1
        # Once the public name no longer identifies the original inode, keep the
        # proven anchor out of generic temporary cleanup until it is moved back.
        # If any later ownership check refuses rollback, the original inode stays
        # available as root-only recovery evidence.
        if [[ -e ${path} || -L ${path} ]]; then
          [[ -n ${published_identity} && -n ${published_target} && -L ${path} &&
            $(stat -c '%d:%i' -- "${path}") == "${published_identity}" &&
            $(readlink -- "${path}") == "${published_target}" ]] || return 1
        fi
        [[ $(stat -c '%U:%G:%a' -- "${anchor}") == \
          "root:root:${previous_public_mode["${path}"]}" &&
          $(sha256sum -- "${anchor}" | awk 'NR == 1 { print $1 }') == \
            "${previous_public_sha256["${path}"]}" ]] || return 1
        mv -Tf -- "${anchor}" "${path}" || return 1
        anchor_restored=true
      fi
      if [[ ${anchor_restored} == false ]]; then
        remove_unchanged_public_rollback_anchor "${path}" || return 1
      fi
      [[ -f ${path} && ! -L ${path} &&
        $(stat -c '%d:%i' -- "${path}") == "${previous_public_identity["${path}"]}" &&
        $(stat -c '%h' -- "${path}") == "${previous_public_nlink["${path}"]}" &&
        $(stat -c '%U:%G:%a' -- "${path}") == "root:root:${previous_public_mode["${path}"]}" &&
        $(sha256sum -- "${path}" | awk 'NR == 1 { print $1 }') == \
          "${previous_public_sha256["${path}"]}" ]] || return 1
      sync -f "$(dirname -- "${path}")" || return 1
      if [[ ${anchor_restored} == true ]]; then
        forget_temporary_path "${anchor}"
        unset 'public_rollback_anchor['"${path}"']'
      fi
      # A backup is removed only after the public binary is proven restored.
      # On incomplete rollback it remains as a durable recovery backup.
      restore_legacy_backup_state "${path}" || return 1
      ;;
    *)
      return 1
      ;;
  esac
  sync -f "$(dirname -- "${path}")" || return 1
}

restore_unit_state() {
  local restore_next=""
  [[ ${unit_replaced} == true ]] || return 0
  case "${unit_previous_kind}" in
    absent)
      [[ ! -L ${UNIT_DEST} ]] || return 1
      [[ ! -e ${UNIT_DEST} || -f ${UNIT_DEST} ]] || return 1
      rm -f -- "${UNIT_DEST}" || return 1
      [[ ! -e ${UNIT_DEST} && ! -L ${UNIT_DEST} ]] || return 1
      ;;
    regular)
      [[ -f ${unit_previous_backup} && ! -L ${unit_previous_backup} ]] || return 1
      create_registered_temporary_path restore_next file \
        /etc/systemd/system/.autostream-discord-bot.service.rollback.XXXXXXXX || return 1
      install -o root -g root -m "${unit_previous_mode}" \
        "${unit_previous_backup}" "${restore_next}" || return 1
      [[ $(sha256sum -- "${restore_next}" | awk 'NR == 1 { print $1 }') == \
        "${unit_previous_sha256}" ]] || return 1
      mv -Tf -- "${restore_next}" "${UNIT_DEST}" || return 1
      forget_temporary_path "${restore_next}"
      [[ -f ${UNIT_DEST} && ! -L ${UNIT_DEST} &&
        $(stat -c '%U:%G:%a' -- "${UNIT_DEST}") == "root:root:${unit_previous_mode}" &&
        $(sha256sum -- "${UNIT_DEST}" | awk 'NR == 1 { print $1 }') == \
          "${unit_previous_sha256}" ]] || return 1
      ;;
    *)
      return 1
      ;;
  esac
  sync -f /etc/systemd/system || return 1
}
