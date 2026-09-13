
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
