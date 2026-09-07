#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${APP_DIR:-$PWD}"
SERVICE_USER="${SERVICE_USER:-$(id -un)}"
SERVICE_GROUP="${SERVICE_GROUP:-$SERVICE_USER}"
PYTHON_BIN="${PYTHON_BIN:-${APP_DIR}/.venv/bin/python}"
PYTHONPATH_VALUE="${PYTHONPATH_VALUE:-${APP_DIR}/src}"
TIMER_UNIT_NAME="${TIMER_UNIT_NAME:-grid-carry-watchlist-monitor}"
WATCHLIST_PATH="${WATCHLIST_PATH:-${APP_DIR}/output/carry_watchlist.json}"
STATE_PATH="${STATE_PATH:-${APP_DIR}/output/carry_watchlist_monitor_state.json}"
ENV_FILE="${ENV_FILE:-/etc/grid-carry-watchlist.env}"

if ! command -v sudo >/dev/null 2>&1; then
  echo "sudo is required for systemd installation." >&2
  exit 1
fi
if [ ! -d "$APP_DIR" ] || [ ! -x "$PYTHON_BIN" ]; then
  echo "APP_DIR or python binary is unavailable" >&2
  exit 1
fi

SERVICE_FILE="/etc/systemd/system/${TIMER_UNIT_NAME}.service"
TIMER_FILE="/etc/systemd/system/${TIMER_UNIT_NAME}.timer"

sudo tee "$SERVICE_FILE" >/dev/null <<EOF
[Unit]
Description=Monitor selected Binance carry funding rates for positive-to-negative changes
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
User=${SERVICE_USER}
Group=${SERVICE_GROUP}
WorkingDirectory=${APP_DIR}
Environment=PYTHONUNBUFFERED=1
Environment=PYTHONPATH=${PYTHONPATH_VALUE}
EnvironmentFile=-${ENV_FILE}
ExecStart=${PYTHON_BIN} -m grid_optimizer.carry_watchlist --watchlist-path ${WATCHLIST_PATH} --state-path ${STATE_PATH}
EOF

sudo tee "$TIMER_FILE" >/dev/null <<EOF
[Unit]
Description=Run selected carry funding-rate monitor every five minutes

[Timer]
OnBootSec=2min
OnUnitActiveSec=5min
AccuracySec=30s
Persistent=true
Unit=${TIMER_UNIT_NAME}.service

[Install]
WantedBy=timers.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable "${TIMER_UNIT_NAME}.timer"
sudo systemctl restart "${TIMER_UNIT_NAME}.timer"
sudo systemctl start "${TIMER_UNIT_NAME}.service"

echo "Installed ${TIMER_UNIT_NAME}.service and ${TIMER_UNIT_NAME}.timer"
sudo systemctl --no-pager --full status "${TIMER_UNIT_NAME}.service" | sed -n '1,20p' || true
sudo systemctl --no-pager --full status "${TIMER_UNIT_NAME}.timer" | sed -n '1,20p'
