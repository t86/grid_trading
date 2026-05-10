#!/usr/bin/env bash
set -euo pipefail

SERVICE_NAME="${SERVICE_NAME:-grid-web}"
SERVICE_USER="${SERVICE_USER:-$(id -un)}"
APP_DIR="${APP_DIR:-$PWD}"
RUNNER_CODE_DIR="${RUNNER_CODE_DIR:-$APP_DIR}"
WATCHDOG_NAME="${WATCHDOG_NAME:-${SERVICE_NAME}-disk-pressure-watchdog}"
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
FAILURE_THRESHOLD="${FAILURE_THRESHOLD:-1}"
ON_BOOT_SEC="${ON_BOOT_SEC:-3min}"
ON_UNIT_ACTIVE_SEC="${ON_UNIT_ACTIVE_SEC:-15min}"
ALERT_EMAIL_TO="${ALERT_EMAIL_TO:-}"

if ! command -v sudo >/dev/null 2>&1; then
  echo "sudo is required for systemd installation." >&2
  exit 1
fi

if [ ! -f "${RUNNER_CODE_DIR}/deploy/oracle/disk_pressure_watchdog.sh" ]; then
  echo "watchdog script missing under ${RUNNER_CODE_DIR}/deploy/oracle" >&2
  exit 1
fi

WATCHDOG_SERVICE_FILE="/etc/systemd/system/${WATCHDOG_NAME}.service"
WATCHDOG_TIMER_FILE="/etc/systemd/system/${WATCHDOG_NAME}.timer"

sudo tee "${WATCHDOG_SERVICE_FILE}" >/dev/null <<EOF
[Unit]
Description=Grid disk pressure watchdog for ${SERVICE_NAME}
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
User=${SERVICE_USER}
Group=${SERVICE_USER}
WorkingDirectory=${APP_DIR}
Environment=APP_DIR=${APP_DIR}
Environment=SERVICE_NAME=${SERVICE_NAME}
Environment=WATCH_PATHS=${WATCH_PATHS}
Environment=OUTPUT_DIR=${OUTPUT_DIR}
Environment=MAX_ROOT_USE_PCT=${MAX_ROOT_USE_PCT}
Environment=CRITICAL_ROOT_USE_PCT=${CRITICAL_ROOT_USE_PCT}
Environment=MAX_OUTPUT_SIZE_MB=${MAX_OUTPUT_SIZE_MB}
Environment=OUTPUT_ARCHIVE_DAYS=${OUTPUT_ARCHIVE_DAYS}
Environment=OUTPUT_DELETE_DAYS=${OUTPUT_DELETE_DAYS}
Environment=JOURNAL_VACUUM_SIZE=${JOURNAL_VACUUM_SIZE}
Environment=JOURNAL_VACUUM_TIME=${JOURNAL_VACUUM_TIME}
Environment=DOCKER_PRUNE=${DOCKER_PRUNE}
Environment=DOCKER_PRUNE_UNTIL_HOURS=${DOCKER_PRUNE_UNTIL_HOURS}
Environment=FAILURE_THRESHOLD=${FAILURE_THRESHOLD}
Environment=ALERT_EMAIL_TO=${ALERT_EMAIL_TO}
Environment=PYTHONPATH=${RUNNER_CODE_DIR}/src
ExecStart=${RUNNER_CODE_DIR}/deploy/oracle/disk_pressure_watchdog.sh
EOF

sudo tee "${WATCHDOG_TIMER_FILE}" >/dev/null <<EOF
[Unit]
Description=Run Grid disk pressure watchdog for ${SERVICE_NAME}

[Timer]
OnBootSec=${ON_BOOT_SEC}
OnUnitActiveSec=${ON_UNIT_ACTIVE_SEC}
AccuracySec=30s
Unit=${WATCHDOG_NAME}.service

[Install]
WantedBy=timers.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable "${WATCHDOG_NAME}.timer"
sudo systemctl restart "${WATCHDOG_NAME}.timer"

echo "Disk pressure watchdog installed: ${WATCHDOG_NAME}.timer"
sudo systemctl --no-pager --full status "${WATCHDOG_NAME}.timer" | sed -n '1,20p'
