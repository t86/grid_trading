#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${APP_DIR:-$PWD}"
SERVICE_USER="${SERVICE_USER:-$(id -un)}"
SERVICE_GROUP="${SERVICE_GROUP:-$SERVICE_USER}"
PYTHON_BIN="${PYTHON_BIN:-${APP_DIR}/.venv/bin/python}"
PYTHONPATH_VALUE="${PYTHONPATH_VALUE:-${APP_DIR}/src}"
TIMER_UNIT_NAME="${TIMER_UNIT_NAME:-grid-alpha-airdrop-monitor}"
ALERT_CONFIG_PATH="${ALERT_CONFIG_PATH:-${APP_DIR}/output/alert_notifier_config.json}"
STATE_PATH="${STATE_PATH:-${APP_DIR}/output/alpha_airdrop_monitor_state.json}"
ACCOUNTS="${ACCOUNTS:-binancezh,BinanceWallet}"
TZ_OFFSET_HOURS="${TZ_OFFSET_HOURS:-8}"

if ! command -v sudo >/dev/null 2>&1; then
  echo "sudo is required for systemd installation." >&2
  exit 1
fi

if [ ! -d "$APP_DIR" ]; then
  echo "APP_DIR does not exist: $APP_DIR" >&2
  exit 1
fi

if [ ! -x "$PYTHON_BIN" ]; then
  echo "python binary not found or not executable: $PYTHON_BIN" >&2
  exit 1
fi

SERVICE_FILE="/etc/systemd/system/${TIMER_UNIT_NAME}.service"
TIMER_FILE="/etc/systemd/system/${TIMER_UNIT_NAME}.timer"

sudo tee "$SERVICE_FILE" >/dev/null <<EOF
[Unit]
Description=Monitor Binance Alpha airdrop posts on X
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
User=${SERVICE_USER}
Group=${SERVICE_GROUP}
WorkingDirectory=${APP_DIR}
Environment=PYTHONUNBUFFERED=1
Environment=PYTHONPATH=${PYTHONPATH_VALUE}
ExecStart=${PYTHON_BIN} -m grid_optimizer.alpha_airdrop_monitor --accounts ${ACCOUNTS} --state-path ${STATE_PATH} --alert-config-path ${ALERT_CONFIG_PATH}
EOF

sudo tee "$TIMER_FILE" >/dev/null <<EOF
[Unit]
Description=Run Binance Alpha airdrop X monitor every 10 minutes from 14:00 to 22:50

[Timer]
OnCalendar=*-*-* 14..22:0/10:00
Persistent=false
AccuracySec=30s
Unit=${TIMER_UNIT_NAME}.service

[Install]
WantedBy=timers.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable "${TIMER_UNIT_NAME}.timer"
sudo systemctl restart "${TIMER_UNIT_NAME}.timer"
sudo systemctl start "${TIMER_UNIT_NAME}.service"

echo "Installed ${TIMER_UNIT_NAME}.service and ${TIMER_UNIT_NAME}.timer"
sudo systemctl --no-pager --full status "${TIMER_UNIT_NAME}.service" | sed -n '1,20p'
sudo systemctl --no-pager --full status "${TIMER_UNIT_NAME}.timer" | sed -n '1,20p'
