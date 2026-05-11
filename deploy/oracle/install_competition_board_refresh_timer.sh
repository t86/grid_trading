#!/usr/bin/env bash
set -euo pipefail

UNIT_NAME="${UNIT_NAME:-grid-competition-board-refresh}"
SERVICE_USER="${SERVICE_USER:-$(id -un)}"
APP_DIR="${APP_DIR:-$PWD}"
RUNNER_CODE_DIR="${RUNNER_CODE_DIR:-$APP_DIR}"
ON_CALENDAR="${ON_CALENDAR:-*-*-* 15:00:00 Asia/Shanghai}"
SLUGS="${SLUGS:-futures_chip futures_bill}"

if ! command -v sudo >/dev/null 2>&1; then
  echo "sudo is required for systemd installation." >&2
  exit 1
fi

SERVICE_FILE="/etc/systemd/system/${UNIT_NAME}.service"
TIMER_FILE="/etc/systemd/system/${UNIT_NAME}.timer"

sudo tee "${SERVICE_FILE}" >/dev/null <<EOF
[Unit]
Description=Refresh targeted Binance competition board cache
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
User=${SERVICE_USER}
Group=${SERVICE_USER}
WorkingDirectory=${APP_DIR}
Environment=PYTHONPATH=${RUNNER_CODE_DIR}/src
ExecStart=${APP_DIR}/.venv/bin/grid-competition-board-refresh $(for slug in ${SLUGS}; do printf -- "--slug %s " "$slug"; done)
EOF

sudo tee "${TIMER_FILE}" >/dev/null <<EOF
[Unit]
Description=Run targeted Binance competition board cache refresh daily

[Timer]
OnCalendar=${ON_CALENDAR}
Persistent=true
Unit=${UNIT_NAME}.service

[Install]
WantedBy=timers.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable "${UNIT_NAME}.timer"
sudo systemctl restart "${UNIT_NAME}.timer"
echo "Installed ${UNIT_NAME}.timer"
sudo systemctl --no-pager --full status "${UNIT_NAME}.timer" | sed -n '1,20p'
