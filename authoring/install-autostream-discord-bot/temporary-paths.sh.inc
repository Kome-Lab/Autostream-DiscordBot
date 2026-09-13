
register_temporary_path() {
  local path=$1
  [[ -z ${temporary_path_identity["${path}"]+present} ]] || return 1
  temporary_path_identity["${path}"]="pending"
}

claim_temporary_path() {
  local path=$1
  local expected_identity="${2-}"
  local actual_identity
  [[ -n ${temporary_path_identity["${path}"]+present} &&
    ${temporary_path_identity["${path}"]} == "pending" &&
    ( -e ${path} || -L ${path} ) ]] || return 1
  actual_identity="$(stat -c '%d:%i' -- "${path}")" || return 1
  [[ -z ${expected_identity} || ${actual_identity} == "${expected_identity}" ]] || \
    return 1
  temporary_path_identity["${path}"]="${actual_identity}"
}

create_registered_temporary_path() {
  local output_variable=$1
  local path_kind=$2
  local template=$3
  local path=""
  local created_identity=""
  local transaction_failed=false

  # mktemp and journal claim form one deferred-signal ownership transaction.
  begin_installer_signal_transaction
  if [[ ${path_kind} == "directory" ]]; then
    if ! path="$(mktemp -d "${template}")"; then
      transaction_failed=true
    fi
  elif [[ ${path_kind} == "file" ]]; then
    if ! path="$(mktemp "${template}")"; then
      transaction_failed=true
    fi
  else
    transaction_failed=true
  fi
  if [[ ${transaction_failed} == false ]] &&
    ! register_temporary_path "${path}"; then
    transaction_failed=true
  fi
  if [[ ${transaction_failed} == false ]] &&
    ! created_identity="$(stat -c '%d:%i' -- "${path}")"; then
    transaction_failed=true
  fi
  if [[ ${transaction_failed} == false ]] &&
    ! claim_temporary_path "${path}" "${created_identity}"; then
    transaction_failed=true
  fi
  if [[ ${transaction_failed} == true ]]; then
    if [[ -n ${created_identity} && ( -e ${path} || -L ${path} ) &&
      $(stat -c '%d:%i' -- "${path}") == "${created_identity}" ]]; then
      if [[ ${path_kind} == "directory" ]]; then
        rm -rf -- "${path}"
      else
        rm -f -- "${path}"
      fi
    fi
    if [[ -n ${path} ]]; then
      forget_temporary_path "${path}"
    fi
  else
    printf -v "${output_variable}" '%s' "${path}"
  fi
  finish_installer_signal_transaction
  [[ ${transaction_failed} == false ]]
}

create_registered_symlink_path() {
  local target=$1
  local path=$2
  local transaction_failed=false

  # Registration, symlink creation, and inode claim are one deferred-signal journal
  # transaction. A path that races this claim remains pending and is never
  # removed without a proven invocation-owned identity.
  begin_installer_signal_transaction
  if ! register_temporary_path "${path}"; then
    transaction_failed=true
  fi
  if [[ ${transaction_failed} == false ]] &&
    ! ln -s -- "${target}" "${path}"; then
    if [[ ! -e ${path} && ! -L ${path} ]]; then
      forget_temporary_path "${path}"
    fi
    transaction_failed=true
  fi
  if [[ ${transaction_failed} == false ]] &&
    ! claim_temporary_path "${path}"; then
    transaction_failed=true
  fi
  finish_installer_signal_transaction
  [[ ${transaction_failed} == false ]]
}

forget_temporary_path() {
  local path=$1
  unset 'temporary_path_identity['"${path}"']'
  unset 'temporary_path_sync_parent['"${path}"']'
  unset 'protected_temporary_path['"${path}"']'
}

cleanup_temporary_paths() {
  local path
  local identity
  local sync_parent
  local cleanup_failed=false
  for path in "${!temporary_path_identity[@]}"; do
    identity="${temporary_path_identity["${path}"]}"
    sync_parent="${temporary_path_sync_parent["${path}"]-}"
    if [[ ${protected_temporary_path["${path}"]-false} == true ]]; then
      cleanup_failed=true
      continue
    fi
    if [[ ! -e ${path} && ! -L ${path} ]]; then
      unset 'temporary_path_identity['"${path}"']'
      unset 'temporary_path_sync_parent['"${path}"']'
      unset 'protected_temporary_path['"${path}"']'
      continue
    fi
    if [[ ${identity} == "pending" ||
      $(stat -c '%d:%i' -- "${path}") != "${identity}" ]]; then
      cleanup_failed=true
      continue
    fi
    rm -f -- "${path}" || cleanup_failed=true
    if [[ -e ${path} || -L ${path} ]]; then
      cleanup_failed=true
    else
      unset 'temporary_path_identity['"${path}"']'
      unset 'temporary_path_sync_parent['"${path}"']'
      unset 'protected_temporary_path['"${path}"']'
      if [[ -n ${sync_parent} ]] && ! sync -f "${sync_parent}"; then
        cleanup_failed=true
      fi
    fi
  done
  [[ ${cleanup_failed} == false ]]
}
