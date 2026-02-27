#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${APP_DIR:-$HOME/grid_trading}"
SERVICE_NAME="${SERVICE_NAME:-grid-web}"
SERVICE_USER="${SERVICE_USER:-$(id -un)}"
GRID_WEB_HOST="${GRID_WEB_HOST:-0.0.0.0}"
GRID_WEB_PORT="${GRID_WEB_PORT:-8787}"
PYTHON_BIN="${PYTHON_BIN:-python3}"

if ! command -v "${PYTHON_BIN}" >/dev/null 2>&1; then
  echo "Python binary not found: ${PYTHON_BIN}" >&2
  exit 1
fi

if ! command -v sudo >/dev/null 2>&1; then
  echo "sudo is required for systemd service installation." >&2
  exit 1
fi

if [ ! -d "${APP_DIR}" ]; then
  echo "APP_DIR does not exist: ${APP_DIR}" >&2
  exit 1
fi

cd "${APP_DIR}"

"${PYTHON_BIN}" -m venv .venv
. .venv/bin/activate
python -m pip install --upgrade pip setuptools wheel
python -m pip install -e .

SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"
sudo tee "${SERVICE_FILE}" >/dev/null <<EOF
[Unit]
Description=Grid Optimizer Web Service
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=${SERVICE_USER}
Group=${SERVICE_USER}
WorkingDirectory=${APP_DIR}
Environment=PYTHONUNBUFFERED=1
ExecStart=${APP_DIR}/.venv/bin/python -m grid_optimizer.web --host ${GRID_WEB_HOST} --port ${GRID_WEB_PORT}
Restart=always
RestartSec=3
TimeoutStartSec=30
TimeoutStopSec=20
KillMode=process
LimitNOFILE=65535

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable "${SERVICE_NAME}.service"
sudo systemctl restart "${SERVICE_NAME}.service"

echo "Service status:"
sudo systemctl --no-pager --full status "${SERVICE_NAME}.service" | sed -n '1,25p'

echo "Deployment complete. Listening on ${GRID_WEB_HOST}:${GRID_WEB_PORT}"
