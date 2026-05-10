#!/usr/bin/env bash
set -euo pipefail

SERVICE_NAME="${SERVICE_NAME:-grid-web}"
APP_DIR="${APP_DIR:-$PWD}"
STATE_DIR="${STATE_DIR:-/var/tmp/grid-disk-pressure-watchdog}"
WATCH_PATHS="${WATCH_PATHS:-/}"
OUTPUT_DIR="${OUTPUT_DIR:-${APP_DIR}/output}"
MAX_ROOT_USE_PCT="${MAX_ROOT_USE_PCT:-88}"
CRITICAL_ROOT_USE_PCT="${CRITICAL_ROOT_USE_PCT:-94}"
MAX_OUTPUT_SIZE_MB="${MAX_OUTPUT_SIZE_MB:-10240}"
OUTPUT_ARCHIVE_DAYS="${OUTPUT_ARCHIVE_DAYS:-3}"
OUTPUT_DELETE_DAYS="${OUTPUT_DELETE_DAYS:-14}"
JOURNAL_VACUUM_SIZE="${JOURNAL_VACUUM_SIZE:-512M}"
JOURNAL_VACUUM_TIME="${JOURNAL_VACUUM_TIME:-7d}"
DOCKER_PRUNE="${DOCKER_PRUNE:-0}"
DOCKER_PRUNE_UNTIL_HOURS="${DOCKER_PRUNE_UNTIL_HOURS:-168}"
ALERT_EMAIL_TO="${ALERT_EMAIL_TO:-}"
FAILURE_THRESHOLD="${FAILURE_THRESHOLD:-1}"

mkdir -p "${STATE_DIR}"
STATE_FILE="${STATE_DIR}/${SERVICE_NAME}.state"
LOG_FILE="${STATE_DIR}/${SERVICE_NAME}.log"

log_line() {
  local message="$1"
  printf '%s %s\n' "$(date -Is)" "${message}" | tee -a "${LOG_FILE}"
  logger -t "grid-disk-pressure[${SERVICE_NAME}]" "${message}" || true
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

path_use_pct() {
  local path="$1"
  df -P "${path}" | awk 'NR == 2 { gsub("%", "", $5); print $5 }'
}

dir_size_mb() {
  local path="$1"
  if [ ! -d "${path}" ]; then
    echo 0
    return
  fi
  du -sm "${path}" 2>/dev/null | awk '{print $1}'
}

top_disk_report() {
  {
    echo "Disk:"
    df -h ${WATCH_PATHS} 2>/dev/null || true
    echo
    echo "Largest under ${OUTPUT_DIR}:"
    du -xhd1 "${OUTPUT_DIR}" 2>/dev/null | sort -h | tail -20 || true
    echo
    echo "Largest files under ${OUTPUT_DIR}:"
    find "${OUTPUT_DIR}" -xdev -type f -printf '%s %TY-%Tm-%Td %TH:%TM %p\n' 2>/dev/null \
      | sort -nr | head -30 \
      | awk '{printf "%.1fM %s %s %s\n", $1/1024/1024, $2, $3, $4}' || true
    echo
    echo "Journald:"
    journalctl --disk-usage 2>/dev/null || true
    echo
    if command -v docker >/dev/null 2>&1; then
      echo "Docker:"
      docker system df 2>/dev/null || true
    fi
  } | sed -n '1,160p'
}

send_alert_if_configured() {
  local subject="$1"
  local body="$2"
  [ -n "${ALERT_EMAIL_TO}" ] || return 0
  GRID_ALERT_EMAIL_TO="${ALERT_EMAIL_TO}" \
  GRID_ALERT_SOURCE_LABEL="${SERVICE_NAME}" \
  WATCHDOG_ALERT_SUBJECT="${subject}" \
  WATCHDOG_ALERT_BODY="${body}" \
  PYTHONPATH="${PYTHONPATH:-${APP_DIR}/src}" \
  python3 - <<'PY'
from grid_optimizer.notifications import send_alert_email
import os

send_alert_email(subject=os.environ["WATCHDOG_ALERT_SUBJECT"], body=os.environ["WATCHDOG_ALERT_BODY"])
PY
}

cleanup_output_jsonl() {
  [ -d "${OUTPUT_DIR}" ] || return 0

  find "${OUTPUT_DIR}" -maxdepth 1 -type f \
    \( -name '*_loop_plan_audit.jsonl' -o -name '*_loop_events.jsonl' -o -name '*_loop_submit_audit.jsonl' -o -name '*_loop_order_audit.jsonl' \) \
    -mtime +"${OUTPUT_ARCHIVE_DAYS}" -size +20M -print0 2>/dev/null \
    | while IFS= read -r -d '' file; do
        gzip -f "${file}" || true
      done

  find "${OUTPUT_DIR}" -maxdepth 1 -type f -name '*.jsonl.gz' -mtime +"${OUTPUT_DELETE_DAYS}" -print -delete 2>/dev/null || true
  find "${OUTPUT_DIR}" -maxdepth 1 -type f -name '*.bak_*' -mtime +"${OUTPUT_DELETE_DAYS}" -print -delete 2>/dev/null || true
}

cleanup_journal() {
  journalctl --vacuum-size="${JOURNAL_VACUUM_SIZE}" --vacuum-time="${JOURNAL_VACUUM_TIME}" >/dev/null 2>&1 || true
}

cleanup_docker() {
  [ "${DOCKER_PRUNE}" = "1" ] || return 0
  command -v docker >/dev/null 2>&1 || return 0
  docker image prune -af --filter "until=${DOCKER_PRUNE_UNTIL_HOURS}h" >/dev/null 2>&1 || true
  docker container prune -f --filter "until=${DOCKER_PRUNE_UNTIL_HOURS}h" >/dev/null 2>&1 || true
  docker builder prune -af --filter "until=${DOCKER_PRUNE_UNTIL_HOURS}h" >/dev/null 2>&1 || true
}

reasons=()
for path in ${WATCH_PATHS}; do
  use_pct="$(path_use_pct "${path}" || echo 0)"
  if [ "${use_pct:-0}" -ge "${MAX_ROOT_USE_PCT}" ]; then
    reasons+=("${path} use ${use_pct}% >= ${MAX_ROOT_USE_PCT}%")
  fi
done

output_size_mb="$(dir_size_mb "${OUTPUT_DIR}")"
if [ "${output_size_mb:-0}" -ge "${MAX_OUTPUT_SIZE_MB}" ]; then
  reasons+=("${OUTPUT_DIR} size ${output_size_mb}MB >= ${MAX_OUTPUT_SIZE_MB}MB")
fi

if [ "${#reasons[@]}" -eq 0 ]; then
  previous_fail_count="$(read_fail_count)"
  write_state 0
  if [ "${previous_fail_count}" != "0" ]; then
    log_line "disk pressure recovered: root=$(path_use_pct /)% output=${output_size_mb}MB"
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
before_report="$(top_disk_report)"
log_line "disk pressure detected (${fail_count}/${FAILURE_THRESHOLD}): ${reason_text}"

if [ "${fail_count}" -lt "${FAILURE_THRESHOLD}" ]; then
  exit 1
fi

log_line "threshold reached; running disk cleanup"
cleanup_output_jsonl
cleanup_journal
cleanup_docker

after_report="$(top_disk_report)"
write_state 0

critical_reasons=()
for path in ${WATCH_PATHS}; do
  use_pct="$(path_use_pct "${path}" || echo 0)"
  if [ "${use_pct:-0}" -ge "${CRITICAL_ROOT_USE_PCT}" ]; then
    critical_reasons+=("${path} still ${use_pct}% >= ${CRITICAL_ROOT_USE_PCT}%")
  fi
done

subject="[grid][${SERVICE_NAME}] disk pressure cleanup"
if [ "${#critical_reasons[@]}" -gt 0 ]; then
  subject="[grid][${SERVICE_NAME}] CRITICAL disk pressure"
fi

send_alert_if_configured \
  "${subject}" \
  "$(cat <<EOF
Service: ${SERVICE_NAME}
Host: $(hostname)
Time: $(date -Is)

Reasons:
${reason_text}

Before cleanup:
${before_report}

After cleanup:
${after_report}

Critical after cleanup:
$(IFS='; '; echo "${critical_reasons[*]:-none}")
EOF
)"
