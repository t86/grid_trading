#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${APP_DIR:-$PWD}"
SERVICE_USER="${SERVICE_USER:-$(id -un)}"
SERVICE_GROUP="${SERVICE_GROUP:-$SERVICE_USER}"
PYTHON_BIN="${PYTHON_BIN:-${APP_DIR}/.venv/bin/python}"
PYTHONPATH_VALUE="${PYTHONPATH_VALUE:-${APP_DIR}/src}"
TIMER_UNIT_NAME="${TIMER_UNIT_NAME:-grid-low-volume-monitor}"
SYMBOLS="${SYMBOLS:-BILLUSDT}"
OUTPUT_DIR="${OUTPUT_DIR:-${APP_DIR}/output}"
STATE_PATH="${STATE_PATH:-${APP_DIR}/output/low_volume_monitor_state.json}"
ALERT_CONFIG_PATH="${ALERT_CONFIG_PATH:-${APP_DIR}/output/low_volume_alert_config.json}"
WINDOW_SECONDS="${WINDOW_SECONDS:-600}"
MIN_VOLUME_NOTIONAL="${MIN_VOLUME_NOTIONAL:-1000}"
THRESHOLD_SECONDS="${THRESHOLD_SECONDS:-600}"
ON_UNIT_ACTIVE_SEC="${ON_UNIT_ACTIVE_SEC:-10min}"

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
Description=Grid low volume monitor
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
User=${SERVICE_USER}
Group=${SERVICE_GROUP}
WorkingDirectory=${APP_DIR}
Environment=PYTHONUNBUFFERED=1
Environment=PYTHONPATH=${PYTHONPATH_VALUE}
ExecStart=${PYTHON_BIN} -m grid_optimizer.low_volume_monitor --symbols ${SYMBOLS} --output-dir ${OUTPUT_DIR} --state-path ${STATE_PATH} --alert-config-path ${ALERT_CONFIG_PATH} --window-seconds ${WINDOW_SECONDS} --min-volume-notional ${MIN_VOLUME_NOTIONAL} --threshold-seconds ${THRESHOLD_SECONDS}
EOF

sudo tee "$TIMER_FILE" >/dev/null <<EOF
[Unit]
Description=Run Grid low volume monitor

[Timer]
OnBootSec=2min
OnUnitActiveSec=${ON_UNIT_ACTIVE_SEC}
AccuracySec=15s
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
sudo systemctl --no-pager --full status "${TIMER_UNIT_NAME}.timer" | sed -n '1,20p' || true
