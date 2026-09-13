#!/bin/bash
set -euo pipefail

umask 077
export PATH=/usr/sbin:/usr/bin:/sbin:/bin
export LC_ALL=C

readonly SERVICE_NAME="discord-bot"
readonly MANAGED_ROOT="/opt/autostream/discord-bot"
readonly RELEASES_DIR="${MANAGED_ROOT}/releases"
readonly CURRENT_LINK="${MANAGED_ROOT}/current"
readonly PUBLIC_BINARY="/usr/local/bin/autostream-discord-bot"
readonly PUBLIC_ALIAS="/usr/local/bin/discord-bot"
readonly ENV_DEST="/etc/autostream/discord-bot.env"
readonly UNIT_DEST="/etc/systemd/system/autostream-discord-bot.service"
readonly UNIT_NAME="autostream-discord-bot.service"
readonly STATE_DIR="/var/lib/autostream/discord-bot"
readonly BACKUP_BASE="/var/backups/autostream"
readonly BACKUP_ROOT="${BACKUP_BASE}/install-migrations"
readonly BACKUP_DIR="${BACKUP_ROOT}/discord-bot"

die() {
  printf 'install-autostream-discord-bot: %s\n' "$*" >&2
  exit 1
}

cleanup_in_progress=false
signal_transaction_active=false
deferred_termination_status=0

handle_installer_signal() {
  local requested_status=$1
  if [[ ${signal_transaction_active} == true ]]; then
    deferred_termination_status=${requested_status}
    return 0
  fi
  exit "${requested_status}"
}

restore_installer_signal_traps() {
  if [[ ${cleanup_in_progress} == true ]]; then
    trap '' HUP INT TERM
    return
  fi
  trap 'handle_installer_signal 129' HUP
  trap 'handle_installer_signal 130' INT
  trap 'handle_installer_signal 143' TERM
}

begin_installer_signal_transaction() {
  [[ ${signal_transaction_active} == false ]] || \
    die "nested installer signal transaction is not allowed"
  signal_transaction_active=true
}

finish_installer_signal_transaction() {
  local requested_status
  signal_transaction_active=false
  requested_status=${deferred_termination_status}
  deferred_termination_status=0
  if (( requested_status != 0 )); then
    exit "${requested_status}"
  fi
}

[[ ${EUID} -eq 0 ]] || die "must run as root (use sudo)"
[[ $# -eq 0 ]] || die "this installer does not accept arguments"

for command in \
  awk basename chmod chown cmp cp dirname find flock getent grep groupadd groupdel id install jq \
  ln mktemp mv readlink rm rmdir runuser sha256sum sort stat sync systemctl tar uname uniq useradd userdel usermod wc; do
  command -v "${command}" >/dev/null 2>&1 || die "required command is unavailable: ${command}"
done

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
[[ ${SCRIPT_DIR} == /* && -d ${SCRIPT_DIR} ]] || die "could not resolve the installer directory"
readonly SCRIPT_DIR
ARTIFACT_ID="$(basename -- "${SCRIPT_DIR}")"
[[ -n ${ARTIFACT_ID} ]] || die "could not resolve the artifact directory name"
readonly ARTIFACT_ID
if [[ ! ${ARTIFACT_ID} =~ ^autostream-discord-bot_(v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z]+(\.[0-9A-Za-z]+)*)?)_linux_(amd64|arm64)$ ]]; then
  die "the extracted directory name is invalid: ${ARTIFACT_ID}"
fi
readonly VERSION="${BASH_REMATCH[1]}"
readonly ARCH="${BASH_REMATCH[4]}"

case "$(uname -m)" in
  x86_64) readonly HOST_ARCH="amd64" ;;
  aarch64|arm64) readonly HOST_ARCH="arm64" ;;
  *) die "unsupported host architecture: $(uname -m)" ;;
esac
[[ ${ARCH} == "${HOST_ARCH}" ]] || die "artifact architecture ${ARCH} does not match host ${HOST_ARCH}"

ARTIFACT_PARENT="$(dirname -- "${SCRIPT_DIR}")"
[[ ${ARTIFACT_PARENT} == /* && -d ${ARTIFACT_PARENT} ]] || die "could not resolve the artifact parent directory"
readonly ARTIFACT_PARENT
readonly ARCHIVE_NAME="${ARTIFACT_ID}.tar.gz"
readonly ARCHIVE_SOURCE="${ARTIFACT_PARENT}/${ARCHIVE_NAME}"
readonly ARTIFACT_MANIFEST_NAME="artifact-manifest.json"
readonly MAX_ARCHIVE_SIZE=268435456
[[ -f ${ARCHIVE_SOURCE} && ! -L ${ARCHIVE_SOURCE} ]] || \
  die "required release archive is missing or unsafe: ${ARCHIVE_SOURCE}"
ARTIFACT_SIZE="$(stat -c %s -- "${ARCHIVE_SOURCE}")"
[[ ${ARTIFACT_SIZE} =~ ^[1-9][0-9]*$ &&
  ${ARTIFACT_SIZE} -le ${MAX_ARCHIVE_SIZE} ]] || \
  die "release archive size must be between 1 and ${MAX_ARCHIVE_SIZE} bytes"
readonly ARTIFACT_SIZE

INPUT_STAGE=""
INPUT_STAGE_IDENTITY=""

input_stage_is_owned() {
  [[ -n ${INPUT_STAGE} && -n ${INPUT_STAGE_IDENTITY} &&
    ${INPUT_STAGE} == /var/tmp/autostream-discord-bot-install.* &&
    -d ${INPUT_STAGE} && ! -L ${INPUT_STAGE} &&
    $(readlink -f -- "${INPUT_STAGE}") == "${INPUT_STAGE}" &&
    $(stat -c '%d:%i' -- "${INPUT_STAGE}") == "${INPUT_STAGE_IDENTITY}" ]]
}

cleanup_early_input_stage() {
  local status=$?
  trap '' HUP INT TERM
  trap - EXIT
  if [[ -z ${INPUT_STAGE} ]]; then
    :
  elif input_stage_is_owned; then
    rm -rf -- "${INPUT_STAGE}"
  else
    printf '%s\n' "install-autostream-discord-bot: refusing to remove replaced installer staging" >&2
    status=1
    fi
  exit "${status}"
}
trap cleanup_early_input_stage EXIT
restore_installer_signal_traps

# INPUT_STAGE is the single temporary-path journal exception: it is the
# root-only durable recovery container for every artifact kept after an
# incomplete rollback. Creation and identity capture are one deferred-signal
# transaction so a termination request cannot strand an unjournaled directory.
input_stage_failed=false
input_stage_failure_status=1
begin_installer_signal_transaction
if INPUT_STAGE="$(mktemp -d /var/tmp/autostream-discord-bot-install.XXXXXXXX)"; then
  :
else
  input_stage_failure_status=$?
  input_stage_failed=true
fi
if [[ ${input_stage_failed} == false &&
  ! ( ${INPUT_STAGE} == /var/tmp/autostream-discord-bot-install.* &&
  -d ${INPUT_STAGE} && ! -L ${INPUT_STAGE} &&
  $(readlink -f -- "${INPUT_STAGE}") == "${INPUT_STAGE}" &&
  $(stat -c '%U:%G:%a' -- "${INPUT_STAGE}") == "root:root:700" ) ]]; then
  input_stage_failed=true
fi
if [[ ${input_stage_failed} == false ]] &&
  ! INPUT_STAGE_IDENTITY="$(stat -c '%d:%i' -- "${INPUT_STAGE}")"; then
  input_stage_failed=true
fi
finish_installer_signal_transaction
if [[ ${input_stage_failed} == true ]]; then
  if [[ ${INPUT_STAGE} == /var/tmp/autostream-discord-bot-install.* &&
    -d ${INPUT_STAGE} && ! -L ${INPUT_STAGE} &&
    $(readlink -f -- "${INPUT_STAGE}") == "${INPUT_STAGE}" &&
    $(stat -c '%U' -- "${INPUT_STAGE}") == "root" ]]; then
    unsafe_input_stage_identity="$(stat -c '%d:%i' -- "${INPUT_STAGE}")"
    if [[ $(stat -c '%d:%i' -- "${INPUT_STAGE}") == "${unsafe_input_stage_identity}" ]]; then
      rm -rf -- "${INPUT_STAGE}"
    fi
  elif [[ -n ${INPUT_STAGE} ]]; then
    printf '%s\n' \
      "install-autostream-discord-bot: refusing to remove an unsafe installer staging path" >&2
  fi
  if [[ -z ${INPUT_STAGE} && ${input_stage_failure_status} -ne 1 ]]; then
    printf '%s\n' \
      "install-autostream-discord-bot: could not create the installer staging directory" >&2
    exit "${input_stage_failure_status}"
  fi
  die "could not create a safe installer staging directory"
fi
readonly INPUT_STAGE
readonly INPUT_STAGE_IDENTITY
readonly UNPACK_DIR="${INPUT_STAGE}/unpack"

managed_candidate=""
managed_candidate_identity=""
current_next=""
unit_stage=""
env_stage=""
public_link_next=""
activation_started=false
installation_complete=false
previous_current_kind="absent"
previous_current_target=""
declare -A previous_public_kind=()
declare -A previous_public_target=()
declare -A previous_public_mode=()
declare -A previous_public_sha256=()
declare -A previous_public_identity=()
declare -A previous_public_nlink=()
declare -A public_rollback_anchor=()
declare -A published_public_identity=()
declare -A published_public_target=()
unit_previous_kind="absent"
unit_previous_mode=""
unit_previous_sha256=""
unit_previous_backup="${INPUT_STAGE}/unit.previous"
unit_replaced=false
env_created=false
env_created_identity=""
state_dir_preexisting=false
state_dir_identity=""
state_dir_metadata=""
state_directory_mutation_started=false
state_directory_created_identity=""
created_autostream_user=false
created_autostream_user_record=""
created_autostream_group=false
created_autostream_group_record=""
preexisting_autostream_group_record=""
readonly AUTOSTREAM_USER_ROLLBACK_LOGIN="autostream-install-rollback"
autostream_user_rollback_login_ready=false
release_created=false
release_created_identity=""
rollback_incomplete=false
declare -a journaled_directory_order=()
declare -A journaled_directory_recorded=()
declare -A journaled_directory_previous_kind=()
declare -A journaled_directory_previous_identity=()
declare -A journaled_directory_previous_uid=()
declare -A journaled_directory_previous_gid=()
declare -A journaled_directory_previous_mode=()
declare -A journaled_directory_created_identity=()
declare -A temporary_path_identity=()
declare -A temporary_path_sync_parent=()
declare -A protected_temporary_path=()
declare -A backup_previous_kind=()
declare -A backup_previous_identity=()
declare -A backup_previous_uid=()
declare -A backup_previous_gid=()
declare -A backup_previous_mode=()
declare -A backup_previous_sha256=()
declare -A backup_created_identity=()
