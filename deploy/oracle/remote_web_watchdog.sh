#!/usr/bin/env bash
set -euo pipefail

WATCHDOG_NAME="${WATCHDOG_NAME:-remote-web-watchdog}"
REMOTE_LABEL="${REMOTE_LABEL:-remote}"
HEALTHCHECK_URL="${HEALTHCHECK_URL:-}"
STATUS_URL="${STATUS_URL:-}"
CONNECT_TIMEOUT_SECONDS="${CONNECT_TIMEOUT_SECONDS:-3}"
MAX_TIME_SECONDS="${MAX_TIME_SECONDS:-12}"
FAILURE_THRESHOLD="${FAILURE_THRESHOLD:-2}"
REMEDIATION_COOLDOWN_SECONDS="${REMEDIATION_COOLDOWN_SECONDS:-600}"
REMEDIATION_COMMAND="${REMEDIATION_COMMAND:-}"
AUTH_USERNAME="${AUTH_USERNAME:-}"
AUTH_PASSWORD="${AUTH_PASSWORD:-}"
STATE_DIR="${STATE_DIR:-/var/tmp/grid-remote-web-watchdog}"

mkdir -p "${STATE_DIR}"
STATE_FILE="${STATE_DIR}/${WATCHDOG_NAME}.state"
LOG_FILE="${STATE_DIR}/${WATCHDOG_NAME}.log"

log_line() {
  local message="$1"
  printf '%s [%s] %s\n' "$(date -Is)" "${REMOTE_LABEL}" "${message}" | tee -a "${LOG_FILE}"
  logger -t "grid-remote-web-watchdog[${WATCHDOG_NAME}]" "[${REMOTE_LABEL}] ${message}" || true
}

read_state_value() {
  local key="$1"
  if [ -f "${STATE_FILE}" ]; then
    sed -n "s/^${key}=//p" "${STATE_FILE}" | tail -n 1
  fi
}

write_state() {
  local fail_count="$1"
  local last_remediation="$2"
  {
    printf 'fail_count=%s\n' "${fail_count}"
    printf 'last_remediation=%s\n' "${last_remediation}"
    printf 'last_check=%s\n' "$(date -Is)"
  } >"${STATE_FILE}"
}

run_probe() {
  local url="$1"
  [ -n "${url}" ] || return 0
  local auth_args=()
  if [ -n "${AUTH_USERNAME}" ] || [ -n "${AUTH_PASSWORD}" ]; then
    auth_args=(-u "${AUTH_USERNAME}:${AUTH_PASSWORD}")
  fi
  curl -fsS \
    --connect-timeout "${CONNECT_TIMEOUT_SECONDS}" \
    --max-time "${MAX_TIME_SECONDS}" \
    "${auth_args[@]}" \
    "${url}" >/dev/null
}

if [ -z "${HEALTHCHECK_URL}" ] && [ -z "${STATUS_URL}" ]; then
  log_line "no probe URLs configured"
  exit 2
fi

last_remediation="$(read_state_value last_remediation || true)"
if ! [[ "${last_remediation}" =~ ^[0-9]+$ ]]; then
  last_remediation=0
fi

if run_probe "${HEALTHCHECK_URL}" && run_probe "${STATUS_URL}"; then
  previous_fail_count="$(read_state_value fail_count || true)"
  write_state 0 "${last_remediation}"
  if [ "${previous_fail_count:-0}" != "0" ]; then
    log_line "probe recovered after ${previous_fail_count} consecutive failures"
  fi
  exit 0
fi

fail_count="$(read_state_value fail_count || true)"
if ! [[ "${fail_count}" =~ ^[0-9]+$ ]]; then
  fail_count=0
fi
fail_count="$((fail_count + 1))"
write_state "${fail_count}" "${last_remediation}"
log_line "probe failed (${fail_count}/${FAILURE_THRESHOLD}) health=${HEALTHCHECK_URL:-disabled} status=${STATUS_URL:-disabled}"

if [ "${fail_count}" -lt "${FAILURE_THRESHOLD}" ]; then
  exit 0
fi

now="$(date +%s)"
cooldown_until="$((last_remediation + REMEDIATION_COOLDOWN_SECONDS))"
if [ "${now}" -lt "${cooldown_until}" ]; then
  log_line "threshold reached, but remediation is in cooldown until ${cooldown_until}"
  exit 1
fi

if [ -z "${REMEDIATION_COMMAND}" ]; then
  log_line "threshold reached, but REMEDIATION_COMMAND is not configured"
  exit 1
fi

log_line "threshold reached; running remediation command"
if bash -lc "${REMEDIATION_COMMAND}"; then
  write_state 0 "${now}"
  log_line "remediation command succeeded"
  exit 0
fi

write_state "${fail_count}" "${now}"
log_line "remediation command failed"
exit 1
