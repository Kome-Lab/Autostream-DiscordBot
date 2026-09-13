
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
LoadCredential=node-listener.json:/opt/autostream/local-executor/ports/discord-bot.json
ExecStart=/usr/local/bin/autostream-discord-bot
Restart=on-failure

[Install]
WantedBy=multi-user.target
EOF
printf '%s\n' 'AUTOSTREAM_NODE_CONFIG=/etc/autostream-discord-bot/config.yml' \
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
