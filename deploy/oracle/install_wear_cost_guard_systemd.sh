#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${APP_DIR:-/home/ubuntu/wangge}"
RUNNER_CODE_DIR="${RUNNER_CODE_DIR:-$APP_DIR}"
PYTHON_BIN="${PYTHON_BIN:-$RUNNER_CODE_DIR/.venv/bin/python}"
RUNNER_WRAPPER="${RUNNER_WRAPPER:-/usr/local/bin/grid-saved-runner}"
SYMBOLS="${SYMBOLS:-BZUSDT CLUSDT}"
UNIT_NAME="${UNIT_NAME:-grid-wear-cost-guard}"
SERVICE_USER="${SERVICE_USER:-ubuntu}"
SERVICE_GROUP="${SERVICE_GROUP:-$SERVICE_USER}"
ON_BOOT_SEC="${ON_BOOT_SEC:-3min}"
ON_UNIT_ACTIVE_SEC="${ON_UNIT_ACTIVE_SEC:-10min}"

if ! command -v sudo >/dev/null 2>&1; then
  echo "sudo is required for systemd installation." >&2
  exit 1
fi

if [ ! -x "$PYTHON_BIN" ]; then
  echo "python binary not found: $PYTHON_BIN" >&2
  exit 1
fi

if [ ! -x "$RUNNER_WRAPPER" ]; then
  echo "runner wrapper not found: $RUNNER_WRAPPER" >&2
  exit 1
fi

SERVICE_FILE="/etc/systemd/system/${UNIT_NAME}.service"
TIMER_FILE="/etc/systemd/system/${UNIT_NAME}.timer"

sudo tee "$SERVICE_FILE" >/dev/null <<EOF
[Unit]
Description=Grid wear cost guard for BZ/CL
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
User=${SERVICE_USER}
Group=${SERVICE_GROUP}
WorkingDirectory=${RUNNER_CODE_DIR}
ExecStart=${PYTHON_BIN} ${RUNNER_CODE_DIR}/deploy/oracle/wear_cost_guard.py --app-dir ${APP_DIR} --runner-wrapper ${RUNNER_WRAPPER} --symbols ${SYMBOLS}
EOF

sudo tee "$TIMER_FILE" >/dev/null <<EOF
[Unit]
Description=Run grid wear cost guard every 10 minutes

[Timer]
OnBootSec=${ON_BOOT_SEC}
OnUnitActiveSec=${ON_UNIT_ACTIVE_SEC}
AccuracySec=30s
Persistent=true
Unit=${UNIT_NAME}.service

[Install]
WantedBy=timers.target
EOF

chmod +x "${RUNNER_CODE_DIR}/deploy/oracle/wear_cost_guard.py"
sudo systemctl daemon-reload
sudo systemctl enable --now "${UNIT_NAME}.timer"
systemctl --no-pager --full status "${UNIT_NAME}.timer" | sed -n '1,20p'
