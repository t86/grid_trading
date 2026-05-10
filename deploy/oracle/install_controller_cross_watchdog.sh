#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${APP_DIR:-$PWD}"
RUNNER_CODE_DIR="${RUNNER_CODE_DIR:-$APP_DIR}"
SERVICE_USER="${SERVICE_USER:-$(id -un)}"
WATCHDOG_NAME="${WATCHDOG_NAME:-grid-controller-cross-watchdog}"
CONTROLLER_CROSS_STATUS_URL="${CONTROLLER_CROSS_STATUS_URL:-http://127.0.0.1:8787/api/running_status?scope=cross}"
AUTH_USERNAME="${AUTH_USERNAME:-}"
AUTH_PASSWORD="${AUTH_PASSWORD:-}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-10}"
FAILURE_THRESHOLD="${FAILURE_THRESHOLD:-2}"
HOST_LABEL="${HOST_LABEL:-110}"
ON_BOOT_SEC="${ON_BOOT_SEC:-2min}"
ON_UNIT_ACTIVE_SEC="${ON_UNIT_ACTIVE_SEC:-1min}"
PYTHON_BIN="${PYTHON_BIN:-${RUNNER_CODE_DIR}/.venv/bin/python}"

if ! command -v sudo >/dev/null 2>&1; then
  echo "sudo is required for systemd installation." >&2
  exit 1
fi

if [ ! -x "${PYTHON_BIN}" ]; then
  echo "python binary not found or not executable: ${PYTHON_BIN}" >&2
  exit 1
fi

WATCHDOG_SERVICE_FILE="/etc/systemd/system/${WATCHDOG_NAME}.service"
WATCHDOG_TIMER_FILE="/etc/systemd/system/${WATCHDOG_NAME}.timer"

sudo tee "${WATCHDOG_SERVICE_FILE}" >/dev/null <<EOF
[Unit]
Description=Grid controller cross watchdog
After=network-online.target grid-web-controller.service
Wants=network-online.target

[Service]
Type=oneshot
User=${SERVICE_USER}
Group=${SERVICE_USER}
WorkingDirectory=${APP_DIR}
Environment=PYTHONPATH=${RUNNER_CODE_DIR}/src
Environment=CONTROLLER_CROSS_STATUS_URL=${CONTROLLER_CROSS_STATUS_URL}
Environment=AUTH_USERNAME=${AUTH_USERNAME}
Environment=AUTH_PASSWORD=${AUTH_PASSWORD}
Environment=TIMEOUT_SECONDS=${TIMEOUT_SECONDS}
Environment=FAILURE_THRESHOLD=${FAILURE_THRESHOLD}
Environment=HOST_LABEL=${HOST_LABEL}
ExecStart=${PYTHON_BIN} ${RUNNER_CODE_DIR}/deploy/oracle/controller_cross_watchdog.py
EOF

sudo tee "${WATCHDOG_TIMER_FILE}" >/dev/null <<EOF
[Unit]
Description=Run Grid controller cross watchdog

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

echo "Controller cross watchdog installed: ${WATCHDOG_NAME}.timer"
sudo systemctl --no-pager --full status "${WATCHDOG_NAME}.timer" | sed -n '1,20p'
