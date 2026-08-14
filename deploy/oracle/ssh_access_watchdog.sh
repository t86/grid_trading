#!/usr/bin/env bash
set -euo pipefail

STATE_DIR="${STATE_DIR:-/run/wangge-ssh-watchdog}"
PROBE_KEY="${PROBE_KEY:-/etc/wangge-ssh-watchdog/id_ed25519}"
KNOWN_HOSTS_FILE="${KNOWN_HOSTS_FILE:-/etc/wangge-ssh-watchdog/known_hosts}"
PROBE_USER="${PROBE_USER:-ubuntu}"
SSH_SERVICE="${SSH_SERVICE:-ssh}"
FAILURE_THRESHOLD="${FAILURE_THRESHOLD:-3}"
COOLDOWN_SECONDS="${COOLDOWN_SECONDS:-600}"
RECOVERY_SETTLE_SECONDS="${RECOVERY_SETTLE_SECONDS:-2}"
SSH_BIN="${SSH_BIN:-/usr/bin/ssh}"
SSHD_BIN="${SSHD_BIN:-/usr/sbin/sshd}"
SYSTEMCTL_BIN="${SYSTEMCTL_BIN:-/usr/bin/systemctl}"
NOW_EPOCH="${NOW_EPOCH:-$(date +%s)}"
STATE_FILE="${STATE_DIR}/state"
LAST_RECOVERY_FILE="${STATE_DIR}/last_recovery"

mkdir -p "${STATE_DIR}"

probe_ssh() {
  "${SSH_BIN}" -q -T -i "${PROBE_KEY}" -o IdentitiesOnly=yes -o BatchMode=yes \
    -o StrictHostKeyChecking=yes -o UserKnownHostsFile="${KNOWN_HOSTS_FILE}" \
    -o ConnectTimeout=5 -o ConnectionAttempts=1 "${PROBE_USER}@127.0.0.1" true
}

read_number() {
  local path="$1"
  local value=0
  if [ -f "${path}" ]; then
    value="$(cat "${path}" 2>/dev/null || echo 0)"
  fi
  if ! [[ "${value}" =~ ^[0-9]+$ ]]; then
    value=0
  fi
  printf '%s\n' "${value}"
}

if probe_ssh; then
  printf '0\n' >"${STATE_FILE}"
  exit 0
fi

fail_count="$(( $(read_number "${STATE_FILE}") + 1 ))"
printf '%s\n' "${fail_count}" >"${STATE_FILE}"
logger -t wangge-ssh-watchdog "localhost SSH probe failed (${fail_count}/${FAILURE_THRESHOLD})" || true

if [ "${fail_count}" -lt "${FAILURE_THRESHOLD}" ]; then
  exit 0
fi

last_recovery="$(read_number "${LAST_RECOVERY_FILE}")"
if [ "$((NOW_EPOCH - last_recovery))" -lt "${COOLDOWN_SECONDS}" ]; then
  logger -t wangge-ssh-watchdog "recovery suppressed by ${COOLDOWN_SECONDS}s cooldown" || true
  exit 0
fi

if ! "${SSHD_BIN}" -t; then
  logger -t wangge-ssh-watchdog "sshd configuration invalid; refusing restart" || true
  exit 1
fi

"${SYSTEMCTL_BIN}" restart "${SSH_SERVICE}"
printf '%s\n' "${NOW_EPOCH}" >"${LAST_RECOVERY_FILE}"
sleep "${RECOVERY_SETTLE_SECONDS}"

if probe_ssh; then
  printf '0\n' >"${STATE_FILE}"
  logger -t wangge-ssh-watchdog "SSH listener restarted and localhost probe recovered" || true
  exit 0
fi

logger -t wangge-ssh-watchdog "SSH listener restart completed but localhost probe still fails" || true
exit 1
