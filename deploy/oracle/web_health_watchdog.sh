#!/usr/bin/env bash
set -euo pipefail

SERVICE_NAME="${SERVICE_NAME:-grid-web}"
HEALTHCHECK_URL="${HEALTHCHECK_URL:-http://127.0.0.1:8787/api/health}"
STATUS_URL="${STATUS_URL:-}"
CONNECT_TIMEOUT_SECONDS="${CONNECT_TIMEOUT_SECONDS:-2}"
MAX_TIME_SECONDS="${MAX_TIME_SECONDS:-8}"
STATE_DIR="${STATE_DIR:-/var/tmp/grid-web-watchdog}"
FAILURE_THRESHOLD="${FAILURE_THRESHOLD:-3}"
ALERT_EMAIL_TO="${ALERT_EMAIL_TO:-}"
AUTH_USERNAME="${AUTH_USERNAME:-}"
AUTH_PASSWORD="${AUTH_PASSWORD:-}"

mkdir -p "${STATE_DIR}"
STATE_FILE="${STATE_DIR}/${SERVICE_NAME}.state"
LOG_FILE="${STATE_DIR}/${SERVICE_NAME}.log"

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

read_fail_count() {
  if [ -f "${STATE_FILE}" ]; then
    sed -n 's/^fail_count=//p' "${STATE_FILE}" | tail -n 1
  else
    echo "0"
  fi
}

write_state() {
  local fail_count="$1"
  printf 'fail_count=%s\nlast_check=%s\n' "${fail_count}" "$(date -Is)" >"${STATE_FILE}"
}

send_alert_if_configured() {
  local subject="$1"
  local body="$2"
  [ -n "${ALERT_EMAIL_TO}" ] || return 0
  GRID_ALERT_EMAIL_TO="${ALERT_EMAIL_TO}" \
  GRID_ALERT_SOURCE_LABEL="${SERVICE_NAME}" \
  WATCHDOG_ALERT_SUBJECT="${subject}" \
  WATCHDOG_ALERT_BODY="${body}" \
  PYTHONPATH="${PYTHONPATH:-${APP_DIR:-$PWD}/src}" \
  python3 - <<'PY'
from grid_optimizer.notifications import send_alert_email
import os

subject = os.environ["WATCHDOG_ALERT_SUBJECT"]
body = os.environ["WATCHDOG_ALERT_BODY"]
send_alert_email(subject=subject, body=body)
PY
}

log_line() {
  local message="$1"
  printf '%s %s\n' "$(date -Is)" "${message}" | tee -a "${LOG_FILE}"
  logger -t "grid-web-watchdog[${SERVICE_NAME}]" "${message}" || true
}

if run_probe "${HEALTHCHECK_URL}" && run_probe "${STATUS_URL}"; then
  previous_fail_count="$(read_fail_count)"
  write_state 0
  if [ "${previous_fail_count}" != "0" ]; then
    log_line "probe recovered after ${previous_fail_count} consecutive failures"
  fi
  exit 0
fi

fail_count="$(read_fail_count)"
if ! [[ "${fail_count}" =~ ^[0-9]+$ ]]; then
  fail_count=0
fi
fail_count="$((fail_count + 1))"
write_state "${fail_count}"
log_line "probe failed (${fail_count}/${FAILURE_THRESHOLD}) health=${HEALTHCHECK_URL} status=${STATUS_URL:-disabled}"

if [ "${fail_count}" -lt "${FAILURE_THRESHOLD}" ]; then
  exit 1
fi

log_line "threshold reached; restarting ${SERVICE_NAME}.service"
sudo systemctl reset-failed "${SERVICE_NAME}.service" || true
sudo systemctl restart "${SERVICE_NAME}.service"
write_state 0

if [ -n "${ALERT_EMAIL_TO}" ]; then
  send_alert_if_configured \
    "[grid][${SERVICE_NAME}] web watchdog restarted service" \
    "$(cat <<EOF
Service: ${SERVICE_NAME}.service
Host: $(hostname)
Time: $(date -Is)
Health URL: ${HEALTHCHECK_URL}
Status URL: ${STATUS_URL:-disabled}
Failure threshold: ${FAILURE_THRESHOLD}

The web watchdog observed repeated local probe failures and restarted the service automatically.
EOF
)"
fi
