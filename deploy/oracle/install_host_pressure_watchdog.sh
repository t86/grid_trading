#!/usr/bin/env bash
set -euo pipefail

SERVICE_NAME="${SERVICE_NAME:-grid-web}"
SERVICE_USER="${SERVICE_USER:-$(id -un)}"
APP_DIR="${APP_DIR:-$PWD}"
RUNNER_CODE_DIR="${RUNNER_CODE_DIR:-$APP_DIR}"
WATCHDOG_NAME="${WATCHDOG_NAME:-${SERVICE_NAME}-host-pressure-watchdog}"
PROBE_URL="${PROBE_URL:-}"
AUTH_USERNAME="${AUTH_USERNAME:-}"
AUTH_PASSWORD="${AUTH_PASSWORD:-}"
FAILURE_THRESHOLD="${FAILURE_THRESHOLD:-2}"
MIN_MEM_AVAILABLE_MB="${MIN_MEM_AVAILABLE_MB:-160}"
MIN_SWAP_FREE_MB="${MIN_SWAP_FREE_MB:-128}"
MAX_LOAD_PER_CPU="${MAX_LOAD_PER_CPU:-2.5}"
MAX_SERVICE_RSS_MB="${MAX_SERVICE_RSS_MB:-0}"
MAX_PROBE_SECONDS="${MAX_PROBE_SECONDS:-5}"
ON_BOOT_SEC="${ON_BOOT_SEC:-2min}"
ON_UNIT_ACTIVE_SEC="${ON_UNIT_ACTIVE_SEC:-1min}"
ALERT_EMAIL_TO="${ALERT_EMAIL_TO:-}"

if ! command -v sudo >/dev/null 2>&1; then
  echo "sudo is required for systemd installation." >&2
  exit 1
fi

if [ ! -f "${RUNNER_CODE_DIR}/deploy/oracle/host_pressure_watchdog.sh" ]; then
  echo "watchdog script missing under ${RUNNER_CODE_DIR}/deploy/oracle" >&2
  exit 1
fi

WATCHDOG_SERVICE_FILE="/etc/systemd/system/${WATCHDOG_NAME}.service"
WATCHDOG_TIMER_FILE="/etc/systemd/system/${WATCHDOG_NAME}.timer"

sudo tee "${WATCHDOG_SERVICE_FILE}" >/dev/null <<EOF
[Unit]
Description=Grid host pressure watchdog for ${SERVICE_NAME}
After=network-online.target ${SERVICE_NAME}.service
Wants=network-online.target

[Service]
Type=oneshot
User=${SERVICE_USER}
Group=${SERVICE_USER}
WorkingDirectory=${APP_DIR}
Environment=APP_DIR=${APP_DIR}
Environment=SERVICE_NAME=${SERVICE_NAME}
Environment=PROBE_URL=${PROBE_URL}
Environment=AUTH_USERNAME=${AUTH_USERNAME}
Environment=AUTH_PASSWORD=${AUTH_PASSWORD}
Environment=FAILURE_THRESHOLD=${FAILURE_THRESHOLD}
Environment=MIN_MEM_AVAILABLE_MB=${MIN_MEM_AVAILABLE_MB}
Environment=MIN_SWAP_FREE_MB=${MIN_SWAP_FREE_MB}
Environment=MAX_LOAD_PER_CPU=${MAX_LOAD_PER_CPU}
Environment=MAX_SERVICE_RSS_MB=${MAX_SERVICE_RSS_MB}
Environment=MAX_PROBE_SECONDS=${MAX_PROBE_SECONDS}
Environment=ALERT_EMAIL_TO=${ALERT_EMAIL_TO}
Environment=PYTHONPATH=${RUNNER_CODE_DIR}/src
ExecStart=/usr/bin/env bash ${RUNNER_CODE_DIR}/deploy/oracle/host_pressure_watchdog.sh
EOF

sudo tee "${WATCHDOG_TIMER_FILE}" >/dev/null <<EOF
[Unit]
Description=Run Grid host pressure watchdog for ${SERVICE_NAME}

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

echo "Host pressure watchdog installed: ${WATCHDOG_NAME}.timer"
sudo systemctl --no-pager --full status "${WATCHDOG_NAME}.timer" | sed -n '1,20p'
