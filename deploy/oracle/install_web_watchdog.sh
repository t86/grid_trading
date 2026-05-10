#!/usr/bin/env bash
set -euo pipefail

SERVICE_NAME="${SERVICE_NAME:-grid-web}"
SERVICE_USER="${SERVICE_USER:-$(id -un)}"
APP_DIR="${APP_DIR:-$PWD}"
RUNNER_CODE_DIR="${RUNNER_CODE_DIR:-$APP_DIR}"
WATCHDOG_NAME="${WATCHDOG_NAME:-${SERVICE_NAME}-health-watchdog}"
HEALTHCHECK_URL="${HEALTHCHECK_URL:-http://127.0.0.1:8787/api/health}"
STATUS_URL="${STATUS_URL:-}"
CONNECT_TIMEOUT_SECONDS="${CONNECT_TIMEOUT_SECONDS:-2}"
MAX_TIME_SECONDS="${MAX_TIME_SECONDS:-8}"
FAILURE_THRESHOLD="${FAILURE_THRESHOLD:-3}"
ON_BOOT_SEC="${ON_BOOT_SEC:-2min}"
ON_UNIT_ACTIVE_SEC="${ON_UNIT_ACTIVE_SEC:-1min}"
ALERT_EMAIL_TO="${ALERT_EMAIL_TO:-}"

if ! command -v sudo >/dev/null 2>&1; then
  echo "sudo is required for systemd installation." >&2
  exit 1
fi

if [ ! -d "${APP_DIR}" ]; then
  echo "APP_DIR does not exist: ${APP_DIR}" >&2
  exit 1
fi

if [ ! -f "${RUNNER_CODE_DIR}/deploy/oracle/web_health_watchdog.sh" ]; then
  echo "watchdog script missing under ${RUNNER_CODE_DIR}/deploy/oracle" >&2
  exit 1
fi

WATCHDOG_SERVICE_FILE="/etc/systemd/system/${WATCHDOG_NAME}.service"
WATCHDOG_TIMER_FILE="/etc/systemd/system/${WATCHDOG_NAME}.timer"

sudo tee "${WATCHDOG_SERVICE_FILE}" >/dev/null <<EOF
[Unit]
Description=Grid web health watchdog for ${SERVICE_NAME}
After=network-online.target ${SERVICE_NAME}.service
Wants=network-online.target

[Service]
Type=oneshot
User=${SERVICE_USER}
Group=${SERVICE_USER}
WorkingDirectory=${APP_DIR}
Environment=APP_DIR=${APP_DIR}
Environment=SERVICE_NAME=${SERVICE_NAME}
Environment=HEALTHCHECK_URL=${HEALTHCHECK_URL}
Environment=STATUS_URL=${STATUS_URL}
Environment=CONNECT_TIMEOUT_SECONDS=${CONNECT_TIMEOUT_SECONDS}
Environment=MAX_TIME_SECONDS=${MAX_TIME_SECONDS}
Environment=FAILURE_THRESHOLD=${FAILURE_THRESHOLD}
Environment=ALERT_EMAIL_TO=${ALERT_EMAIL_TO}
Environment=PYTHONPATH=${RUNNER_CODE_DIR}/src
ExecStart=${RUNNER_CODE_DIR}/deploy/oracle/web_health_watchdog.sh
EOF

sudo tee "${WATCHDOG_TIMER_FILE}" >/dev/null <<EOF
[Unit]
Description=Run Grid web health watchdog for ${SERVICE_NAME}

[Timer]
OnBootSec=${ON_BOOT_SEC}
OnUnitActiveSec=${ON_UNIT_ACTIVE_SEC}
AccuracySec=15s
Unit=${WATCHDOG_NAME}.service

[Install]
WantedBy=timers.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable "${WATCHDOG_NAME}.timer"
sudo systemctl restart "${WATCHDOG_NAME}.timer"

echo "Web watchdog installed: ${WATCHDOG_NAME}.timer"
sudo systemctl --no-pager --full status "${WATCHDOG_NAME}.timer" | sed -n '1,20p'
