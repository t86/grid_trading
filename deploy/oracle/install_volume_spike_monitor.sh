#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${APP_DIR:-/home/ubuntu/wangge}"
CODE_DIR="${CODE_DIR:-${APP_DIR}}"
PYTHON_BIN="${PYTHON_BIN:-${CODE_DIR}/.venv/bin/python}"
OUTPUT_DIR="${OUTPUT_DIR:-${APP_DIR}/output}"
UNIT_NAME="${UNIT_NAME:-grid-volume-spike-monitor}"
SYMBOLS="${SYMBOLS:-STRAXUSDT,OPGUSDT}"
STATE_PATH="${STATE_PATH:-${OUTPUT_DIR}/volume_spike_monitor_state.json}"
ALERT_CONFIG_PATH="${ALERT_CONFIG_PATH:-${OUTPUT_DIR}/alert_notifier_config.json}"
BARK_CONFIG_PATH="${BARK_CONFIG_PATH:-}"
CURRENT_WINDOW_SECONDS="${CURRENT_WINDOW_SECONDS:-60}"
BASELINE_WINDOW_SECONDS="${BASELINE_WINDOW_SECONDS:-600}"
MIN_SPIKE_NOTIONAL="${MIN_SPIKE_NOTIONAL:-1000}"
MULTIPLIER="${MULTIPLIER:-3}"
COOLDOWN_SECONDS="${COOLDOWN_SECONDS:-900}"

extra_args=()
if [[ -n "${BARK_CONFIG_PATH}" ]]; then
  extra_args+=(--bark-config-path "${BARK_CONFIG_PATH}")
fi

sudo tee "/etc/systemd/system/${UNIT_NAME}.service" >/dev/null <<EOF
[Unit]
Description=Grid volume spike monitor

[Service]
Type=oneshot
WorkingDirectory=${APP_DIR}
Environment=PYTHONPATH=${CODE_DIR}/src
ExecStart=${PYTHON_BIN} -m grid_optimizer.volume_spike_monitor --symbols ${SYMBOLS} --output-dir ${OUTPUT_DIR} --state-path ${STATE_PATH} --alert-config-path ${ALERT_CONFIG_PATH} --current-window-seconds ${CURRENT_WINDOW_SECONDS} --baseline-window-seconds ${BASELINE_WINDOW_SECONDS} --min-spike-notional ${MIN_SPIKE_NOTIONAL} --multiplier ${MULTIPLIER} --cooldown-seconds ${COOLDOWN_SECONDS} ${extra_args[*]:-}
EOF

sudo tee "/etc/systemd/system/${UNIT_NAME}.timer" >/dev/null <<EOF
[Unit]
Description=Run Grid volume spike monitor every minute

[Timer]
OnBootSec=30s
OnUnitActiveSec=60s
AccuracySec=5s
Unit=${UNIT_NAME}.service

[Install]
WantedBy=timers.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable --now "${UNIT_NAME}.timer"
sudo systemctl start "${UNIT_NAME}.service"
sudo systemctl status "${UNIT_NAME}.timer" --no-pager -l
