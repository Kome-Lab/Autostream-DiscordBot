
journal_directory_before_mutation() {
  local path=$1
  local previous_kind="absent"
  local previous_identity=""
  local previous_uid=""
  local previous_gid=""
  local previous_mode=""
  [[ -z ${journaled_directory_recorded["${path}"]+present} ]] || return 0
  if [[ -e ${path} || -L ${path} ]]; then
    [[ -d ${path} && ! -L ${path} ]] || return 1
    previous_kind="directory"
    previous_identity="$(stat -c '%d:%i' -- "${path}")" || return 1
    previous_uid="$(stat -c '%u' -- "${path}")" || return 1
    previous_gid="$(stat -c '%g' -- "${path}")" || return 1
    previous_mode="$(stat -c '%a' -- "${path}")" || return 1
  fi

  begin_installer_signal_transaction
  journaled_directory_recorded["${path}"]=true
  journaled_directory_order+=("${path}")
  journaled_directory_previous_kind["${path}"]="${previous_kind}"
  if [[ ${previous_kind} == "directory" ]]; then
    journaled_directory_previous_identity["${path}"]="${previous_identity}"
    journaled_directory_previous_uid["${path}"]="${previous_uid}"
    journaled_directory_previous_gid["${path}"]="${previous_gid}"
    journaled_directory_previous_mode["${path}"]="${previous_mode}"
  fi
  finish_installer_signal_transaction
}

record_journaled_directory_creation() {
  local path=$1
  local created_identity=""
  [[ ${signal_transaction_active} == true ]] || return 1
  if [[ ${journaled_directory_previous_kind["${path}"]} == "absent" ]]; then
    created_identity="$(stat -c '%d:%i' -- "${path}")" || return 1
    journaled_directory_created_identity["${path}"]="${created_identity}"
  fi
}

install_journaled_directory() {
  local path=$1
  local owner=$2
  local group=$3
  local mode=$4
  local created_identity_variable="${5-}"
  local transaction_failed=false

  begin_installer_signal_transaction
  if ! install -d -o "${owner}" -g "${group}" -m "${mode}" "${path}"; then
    transaction_failed=true
  elif ! record_journaled_directory_creation "${path}"; then
    transaction_failed=true
  elif [[ -n ${created_identity_variable} ]]; then
    printf -v "${created_identity_variable}" '%s' \
      "${journaled_directory_created_identity["${path}"]-}"
  fi
  finish_installer_signal_transaction
  [[ ${transaction_failed} == false ]]
}

rollback_journaled_directories() {
  local index
  local path
  local parent
  local rollback_failed=false
  for ((index=${#journaled_directory_order[@]} - 1; index >= 0; index--)); do
    path="${journaled_directory_order[index]}"
    case "${journaled_directory_previous_kind["${path}"]}" in
      directory)
        if [[ ! -d ${path} || -L ${path} ||
          $(stat -c '%d:%i' -- "${path}") != "${journaled_directory_previous_identity["${path}"]}" ]]; then
          rollback_failed=true
          continue
        fi
        chown "${journaled_directory_previous_uid["${path}"]}:${journaled_directory_previous_gid["${path}"]}" \
          "${path}" || rollback_failed=true
        chmod "${journaled_directory_previous_mode["${path}"]}" "${path}" || rollback_failed=true
        [[ $(stat -c '%d:%i:%u:%g:%a' -- "${path}") == \
          "${journaled_directory_previous_identity["${path}"]}:${journaled_directory_previous_uid["${path}"]}:${journaled_directory_previous_gid["${path}"]}:${journaled_directory_previous_mode["${path}"]}" ]] || \
          rollback_failed=true
        ;;
      absent)
        if [[ ! -e ${path} && ! -L ${path} ]]; then
          continue
        fi
        if [[ ! -d ${path} || -L ${path} ||
          -z ${journaled_directory_created_identity["${path}"]-} ||
          $(stat -c '%d:%i' -- "${path}") != "${journaled_directory_created_identity["${path}"]}" ||
          -n $(find "${path}" -mindepth 1 -print -quit) ]]; then
          rollback_failed=true
          continue
        fi
        rmdir -- "${path}" || rollback_failed=true
        parent="$(dirname -- "${path}")"
        sync -f "${parent}" || rollback_failed=true
        ;;
      *)
        rollback_failed=true
        ;;
    esac
  done
  [[ ${rollback_failed} == false ]]
}

rollback_created_release() {
  [[ ${release_created} == true ]] || return 0
  [[ -d ${RELEASE_DIR} && ! -L ${RELEASE_DIR} &&
    $(stat -c '%d:%i' -- "${RELEASE_DIR}") == "${release_created_identity}" &&
    $(dirname -- "${RELEASE_DIR}") == "${RELEASES_DIR}" ]] || return 1
  rm -rf -- "${RELEASE_DIR}" || return 1
  [[ ! -e ${RELEASE_DIR} && ! -L ${RELEASE_DIR} ]] || return 1
  sync -f "${RELEASES_DIR}" || return 1
  release_created=false
  release_created_identity=""
}
