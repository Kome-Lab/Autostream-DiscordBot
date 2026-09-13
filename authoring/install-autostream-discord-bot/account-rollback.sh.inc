
prepare_autostream_user_rollback_login() {
  if id autostream >/dev/null 2>&1; then
    return 0
  fi
  [[ -z $(getent passwd "${AUTOSTREAM_USER_ROLLBACK_LOGIN}" 2>/dev/null || true) &&
    -z $(getent group "${AUTOSTREAM_USER_ROLLBACK_LOGIN}" 2>/dev/null || true) ]] || \
    return 1
  local_account_member_fields_are_clear || return 1
  autostream_user_rollback_login_ready=true
}

local_account_member_fields_are_clear() {
  [[ -f /etc/group && ! -L /etc/group &&
    -f /etc/gshadow && ! -L /etc/gshadow ]] || return 1
  awk -F: \
    -v service_login="autostream" \
    -v rollback_login="${AUTOSTREAM_USER_ROLLBACK_LOGIN}" \
    '{
      count = split($4, members, ",")
      for (member_index = 1; member_index <= count; member_index++) {
        if (members[member_index] == service_login || members[member_index] == rollback_login) {
          exit 1
        }
      }
    }' /etc/group || return 1
  awk -F: \
    -v service_login="autostream" \
    -v rollback_login="${AUTOSTREAM_USER_ROLLBACK_LOGIN}" \
    '{
      admin_count = split($3, admins, ",")
      for (admin_index = 1; admin_index <= admin_count; admin_index++) {
        if (admins[admin_index] == service_login || admins[admin_index] == rollback_login) {
          exit 1
        }
      }
      member_count = split($4, members, ",")
      for (member_index = 1; member_index <= member_count; member_index++) {
        if (members[member_index] == service_login || members[member_index] == rollback_login) {
          exit 1
        }
      }
    }' /etc/gshadow
}

local_account_database_matches_digests() {
  local expected_group_digest=$1
  local expected_gshadow_digest=$2
  local group_digest
  local gshadow_digest

  group_digest="$(sha256sum -- /etc/group | awk 'NR == 1 { print $1 }')" || return 1
  gshadow_digest="$(sha256sum -- /etc/gshadow | awk 'NR == 1 { print $1 }')" || return 1
  [[ ${group_digest} =~ ^[[:xdigit:]]{64}$ &&
    ${gshadow_digest} =~ ^[[:xdigit:]]{64}$ &&
    ${group_digest} == "${expected_group_digest}" &&
    ${gshadow_digest} == "${expected_gshadow_digest}" ]]
}

restore_created_autostream_user_login() {
  local expected_group_record=$1
  local renamed_user_record=$2
  local expected_group_digest=$3
  local expected_gshadow_digest=$4

  [[ -z $(getent passwd autostream 2>/dev/null || true) &&
    $(getent passwd "${AUTOSTREAM_USER_ROLLBACK_LOGIN}" 2>/dev/null || true) == \
      "${renamed_user_record}" &&
    -z $(getent group "${AUTOSTREAM_USER_ROLLBACK_LOGIN}" 2>/dev/null || true) ]] || \
    return 1
  usermod --login autostream "${AUTOSTREAM_USER_ROLLBACK_LOGIN}" || return 1
  [[ $(getent passwd autostream 2>/dev/null || true) == \
      "${created_autostream_user_record}" &&
    -z $(getent passwd "${AUTOSTREAM_USER_ROLLBACK_LOGIN}" 2>/dev/null || true) &&
    -z $(getent group "${AUTOSTREAM_USER_ROLLBACK_LOGIN}" 2>/dev/null || true) ]] || \
    return 1
  local_account_database_matches_digests \
    "${expected_group_digest}" \
    "${expected_gshadow_digest}" || return 1
  [[ $(getent group autostream 2>/dev/null || true) == "${expected_group_record}" ]]
}

remove_created_autostream_user_preserving_group() {
  local delete_status=0
  local expected_group_gid
  local expected_group_record
  local group_database_digest_before
  local gshadow_database_digest_before
  local rename_status=0
  local renamed_user_record
  local user_record_prefix
  local user_record_suffix

  [[ ${created_autostream_user} == true &&
    ${autostream_user_rollback_login_ready} == true &&
    ${created_autostream_user_record} =~ ^(autostream)(:[^:]*:[0-9]+:[1-9][0-9]*:[^:]*:/var/lib/autostream:/usr/sbin/nologin)$ ]] || \
    return 1
  user_record_prefix="${BASH_REMATCH[1]}"
  user_record_suffix="${BASH_REMATCH[2]}"
  [[ ${user_record_prefix} == "autostream" &&
    $(getent passwd autostream 2>/dev/null || true) == \
      "${created_autostream_user_record}" ]] || return 1

  if [[ ${created_autostream_group} == true ]]; then
    expected_group_record="${created_autostream_group_record}"
  else
    expected_group_record="${preexisting_autostream_group_record}"
  fi
  [[ -n ${expected_group_record} &&
    $(getent group autostream 2>/dev/null || true) == "${expected_group_record}" ]] || \
    return 1
  expected_group_gid="$(printf '%s\n' "${expected_group_record}" |
    awk -F: 'NR == 1 && $1 == "autostream" { print $3 }')"
  [[ ${expected_group_gid} =~ ^[1-9][0-9]*$ &&
    $(printf '%s\n' "${created_autostream_user_record}" |
      awk -F: 'NR == 1 { print $4 }') == "${expected_group_gid}" &&
    -z $(getent passwd "${AUTOSTREAM_USER_ROLLBACK_LOGIN}" 2>/dev/null || true) &&
    -z $(getent group "${AUTOSTREAM_USER_ROLLBACK_LOGIN}" 2>/dev/null || true) ]] || \
    return 1

  group_database_digest_before="$(
    sha256sum -- /etc/group | awk 'NR == 1 { print $1 }'
  )" || return 1
  gshadow_database_digest_before="$(
    sha256sum -- /etc/gshadow | awk 'NR == 1 { print $1 }'
  )" || return 1
  [[ ${group_database_digest_before} =~ ^[[:xdigit:]]{64}$ &&
    ${gshadow_database_digest_before} =~ ^[[:xdigit:]]{64}$ ]] || return 1
  local_account_member_fields_are_clear || return 1
  local_account_database_matches_digests \
    "${group_database_digest_before}" \
    "${gshadow_database_digest_before}" || return 1

  # Ubuntu's USERGROUPS_ENAB makes userdel consider deleting a group whose name
  # matches the login being deleted. Rename only the invocation-created login
  # to a preflight-verified unused name; this keeps its primary GID and home
  # untouched while making the protected autostream group ineligible.
  renamed_user_record="${AUTOSTREAM_USER_ROLLBACK_LOGIN}${user_record_suffix}"
  if usermod --login "${AUTOSTREAM_USER_ROLLBACK_LOGIN}" autostream; then
    :
  else
    rename_status=$?
  fi
  if [[ ${rename_status} -ne 0 ||
    -n $(getent passwd autostream 2>/dev/null || true) ||
    $(getent passwd "${AUTOSTREAM_USER_ROLLBACK_LOGIN}" 2>/dev/null || true) != \
      "${renamed_user_record}" ||
    -n $(getent group "${AUTOSTREAM_USER_ROLLBACK_LOGIN}" 2>/dev/null || true) ]]; then
    restore_created_autostream_user_login \
      "${expected_group_record}" \
      "${renamed_user_record}" \
      "${group_database_digest_before}" \
      "${gshadow_database_digest_before}" || true
    return 1
  fi
  if ! local_account_database_matches_digests \
    "${group_database_digest_before}" \
    "${gshadow_database_digest_before}"; then
    restore_created_autostream_user_login \
      "${expected_group_record}" \
      "${renamed_user_record}" \
      "${group_database_digest_before}" \
      "${gshadow_database_digest_before}" || true
    return 1
  fi

  if userdel "${AUTOSTREAM_USER_ROLLBACK_LOGIN}"; then
    delete_status=0
  else
    delete_status=$?
  fi
  if [[ ${delete_status} -ne 0 ]]; then
    restore_created_autostream_user_login \
      "${expected_group_record}" \
      "${renamed_user_record}" \
      "${group_database_digest_before}" \
      "${gshadow_database_digest_before}" || true
    return 1
  fi
  [[ -z $(getent passwd autostream 2>/dev/null || true) &&
    -z $(getent passwd "${AUTOSTREAM_USER_ROLLBACK_LOGIN}" 2>/dev/null || true) &&
    $(getent group autostream 2>/dev/null || true) == "${expected_group_record}" &&
    -z $(getent group "${AUTOSTREAM_USER_ROLLBACK_LOGIN}" 2>/dev/null || true) ]] || \
    return 1
  local_account_database_matches_digests \
    "${group_database_digest_before}" \
    "${gshadow_database_digest_before}" || return 1
  created_autostream_user=false
  created_autostream_user_record=""
}

rollback_created_autostream_account() {
  local current_group
  local rollback_failed=false
  if [[ ${created_autostream_user} == true ]]; then
    remove_created_autostream_user_preserving_group || rollback_failed=true
  fi
  if [[ ${created_autostream_group} == true &&
    ${created_autostream_user} == true ]]; then
    rollback_failed=true
  elif [[ ${created_autostream_group} == true ]]; then
    current_group="$(getent group autostream 2>/dev/null || true)"
    if [[ ${current_group} != "${created_autostream_group_record}" ]]; then
      rollback_failed=true
    elif ! groupdel autostream; then
      rollback_failed=true
    elif getent group autostream >/dev/null; then
      rollback_failed=true
    else
      created_autostream_group=false
      created_autostream_group_record=""
    fi
  fi
  [[ ${rollback_failed} == false ]]
}
