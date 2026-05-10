#!/usr/bin/env bash
set -euo pipefail

SERVICE_NAME="${SERVICE_NAME:-grid-web}"
PROBE_URL="${PROBE_URL:-}"
AUTH_USERNAME="${AUTH_USERNAME:-}"
AUTH_PASSWORD="${AUTH_PASSWORD:-}"
STATE_DIR="${STATE_DIR:-/var/tmp/grid-host-pressure-watchdog}"
FAILURE_THRESHOLD="${FAILURE_THRESHOLD:-2}"
MIN_MEM_AVAILABLE_MB="${MIN_MEM_AVAILABLE_MB:-160}"
MIN_SWAP_FREE_MB="${MIN_SWAP_FREE_MB:-128}"
MAX_LOAD_PER_CPU="${MAX_LOAD_PER_CPU:-2.5}"
MAX_SERVICE_RSS_MB="${MAX_SERVICE_RSS_MB:-0}"
MAX_PROBE_SECONDS="${MAX_PROBE_SECONDS:-5}"
ALERT_EMAIL_TO="${ALERT_EMAIL_TO:-}"

mkdir -p "${STATE_DIR}"
STATE_FILE="${STATE_DIR}/${SERVICE_NAME}.state"
LOG_FILE="${STATE_DIR}/${SERVICE_NAME}.log"

metric_mb() {
  local key="$1"
  awk -v key="${key}:" '$1 == key { printf "%d\n", $2 / 1024 }' /proc/meminfo
}

cpu_count() {
  local count
  count="$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 1)"
  if ! [[ "${count}" =~ ^[0-9]+$ ]] || [ "${count}" -lt 1 ]; then
    count=1
  fi
  echo "${count}"
}

load_1m() {
  awk '{print $1}' /proc/loadavg
}

service_rss_mb() {
  local pid rss_kb
  pid="$(systemctl show -p MainPID --value "${SERVICE_NAME}.service" 2>/dev/null || echo 0)"
  if ! [[ "${pid}" =~ ^[0-9]+$ ]] || [ "${pid}" -le 0 ]; then
    echo 0
    return
  fi
  rss_kb="$(ps -p "${pid}" -o rss= 2>/dev/null | awk '{print $1}')"
  if ! [[ "${rss_kb}" =~ ^[0-9]+$ ]]; then
    echo 0
    return
  fi
  echo $((rss_kb / 1024))
}

probe_elapsed_seconds() {
  local url="$1"
  [ -n "${url}" ] || {
    echo 0
    return
  }
  local auth_args=()
  if [ -n "${AUTH_USERNAME}" ] || [ -n "${AUTH_PASSWORD}" ]; then
    auth_args=(-u "${AUTH_USERNAME}:${AUTH_PASSWORD}")
  fi
  curl -fsS -o /dev/null \
    --connect-timeout 2 \
    --max-time "${MAX_PROBE_SECONDS}" \
    -w '%{time_total}' \
    "${auth_args[@]}" \
    "${url}" 2>/dev/null || echo "${MAX_PROBE_SECONDS}"
}

read_fail_count() {
  if [ -f "${STATE_FILE}" ]; then
    sed -n 's/^fail_count=//p' "${STATE_FILE}" | tail -n 1
  else
    echo 0
  fi
}

write_state() {
  local fail_count="$1"
  printf 'fail_count=%s\nlast_check=%s\n' "${fail_count}" "$(date -Is)" >"${STATE_FILE}"
}

log_line() {
  local message="$1"
  printf '%s %s\n' "$(date -Is)" "${message}" | tee -a "${LOG_FILE}"
  logger -t "grid-host-pressure[${SERVICE_NAME}]" "${message}" || true
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

send_alert_email(subject=os.environ["WATCHDOG_ALERT_SUBJECT"], body=os.environ["WATCHDOG_ALERT_BODY"])
PY
}

mem_available_mb="$(metric_mb MemAvailable)"
swap_free_mb="$(metric_mb SwapFree)"
rss_mb="$(service_rss_mb)"
load_value="$(load_1m)"
cpus="$(cpu_count)"
probe_seconds="$(probe_elapsed_seconds "${PROBE_URL}")"

reasons=()
if [ "${mem_available_mb:-0}" -lt "${MIN_MEM_AVAILABLE_MB}" ]; then
  reasons+=("MemAvailable ${mem_available_mb}MB < ${MIN_MEM_AVAILABLE_MB}MB")
fi
if [ "${swap_free_mb:-0}" -lt "${MIN_SWAP_FREE_MB}" ]; then
  reasons+=("SwapFree ${swap_free_mb}MB < ${MIN_SWAP_FREE_MB}MB")
fi
if [ "${MAX_SERVICE_RSS_MB}" -gt 0 ] && [ "${rss_mb:-0}" -gt "${MAX_SERVICE_RSS_MB}" ]; then
  reasons+=("${SERVICE_NAME} RSS ${rss_mb}MB > ${MAX_SERVICE_RSS_MB}MB")
fi
if awk "BEGIN { exit !(${load_value} > (${MAX_LOAD_PER_CPU} * ${cpus})) }"; then
  reasons+=("load1 ${load_value} > ${MAX_LOAD_PER_CPU} per CPU x ${cpus}")
fi
if [ -n "${PROBE_URL}" ] && awk "BEGIN { exit !(${probe_seconds} >= ${MAX_PROBE_SECONDS}) }"; then
  reasons+=("probe ${PROBE_URL} took ${probe_seconds}s >= ${MAX_PROBE_SECONDS}s")
fi

if [ "${#reasons[@]}" -eq 0 ]; then
  previous_fail_count="$(read_fail_count)"
  write_state 0
  if [ "${previous_fail_count}" != "0" ]; then
    log_line "pressure recovered: mem=${mem_available_mb}MB swap=${swap_free_mb}MB rss=${rss_mb}MB load=${load_value} probe=${probe_seconds}s"
  fi
  exit 0
fi

fail_count="$(read_fail_count)"
if ! [[ "${fail_count}" =~ ^[0-9]+$ ]]; then
  fail_count=0
fi
fail_count="$((fail_count + 1))"
write_state "${fail_count}"
reason_text="$(IFS='; '; echo "${reasons[*]}")"
log_line "pressure detected (${fail_count}/${FAILURE_THRESHOLD}): ${reason_text}"

if [ "${fail_count}" -lt "${FAILURE_THRESHOLD}" ]; then
  exit 1
fi

log_line "threshold reached; restarting ${SERVICE_NAME}.service"
sudo systemctl reset-failed "${SERVICE_NAME}.service" || true
sudo systemctl restart "${SERVICE_NAME}.service"
write_state 0

send_alert_if_configured \
  "[grid][${SERVICE_NAME}] host pressure restart" \
  "$(cat <<EOF
Service: ${SERVICE_NAME}.service
Host: $(hostname)
Time: $(date -Is)

Host pressure triggered an automatic service restart.

Reasons:
${reason_text}

Metrics:
- MemAvailable: ${mem_available_mb} MB
- SwapFree: ${swap_free_mb} MB
- Service RSS: ${rss_mb} MB
- Load1: ${load_value}
- CPUs: ${cpus}
- Probe: ${probe_seconds}s (${PROBE_URL:-disabled})
EOF
)"
