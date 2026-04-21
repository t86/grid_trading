#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${APP_DIR:-$HOME/grid_trading}"
SERVICE_WORKING_DIR="${SERVICE_WORKING_DIR:-${APP_DIR}}"
SERVICE_NAME="${SERVICE_NAME:-grid-web}"
SERVICE_DESCRIPTION="${SERVICE_DESCRIPTION:-Grid Optimizer Web Service}"
SERVICE_USER="${SERVICE_USER:-$(id -un)}"
GRID_WEB_HOST="${GRID_WEB_HOST:-0.0.0.0}"
GRID_WEB_PORT="${GRID_WEB_PORT:-8787}"
PYTHON_BIN="${PYTHON_BIN:-python3}"
EXEC_PYTHON_BIN="${EXEC_PYTHON_BIN:-${APP_DIR}/.venv/bin/python}"
SYSTEMD_ENV_FILE="${SYSTEMD_ENV_FILE:-}"
PYTHONPATH_VALUE="${PYTHONPATH_VALUE:-${APP_DIR}/src}"
GRID_SYMBOL_LISTS_PATH="${GRID_SYMBOL_LISTS_PATH:-}"
INSTALL_UPDATE_WRAPPER="${INSTALL_UPDATE_WRAPPER:-1}"
UPDATE_WRAPPER_NAME="${UPDATE_WRAPPER_NAME:-${SERVICE_NAME}-update}"
INSTALL_RUNNER_WRAPPER="${INSTALL_RUNNER_WRAPPER:-0}"
RUNNER_ENV_FILE="${RUNNER_ENV_FILE:-}"
RUNNER_WRAPPER_NAME="${RUNNER_WRAPPER_NAME:-${SERVICE_NAME}-saved-runner}"

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

if [ ! -d "${SERVICE_WORKING_DIR}" ]; then
  echo "SERVICE_WORKING_DIR does not exist: ${SERVICE_WORKING_DIR}" >&2
  exit 1
fi

if [ -n "${SYSTEMD_ENV_FILE}" ] && [ ! -f "${SYSTEMD_ENV_FILE}" ]; then
  echo "SYSTEMD_ENV_FILE does not exist: ${SYSTEMD_ENV_FILE}" >&2
  exit 1
fi

if [ -n "${RUNNER_ENV_FILE}" ] && [ ! -f "${RUNNER_ENV_FILE}" ]; then
  echo "RUNNER_ENV_FILE does not exist: ${RUNNER_ENV_FILE}" >&2
  exit 1
fi

if [ "${INSTALL_RUNNER_WRAPPER}" != "0" ] && [ -z "${RUNNER_ENV_FILE}" ]; then
  echo "RUNNER_ENV_FILE is required when INSTALL_RUNNER_WRAPPER=1" >&2
  exit 1
fi

cd "${APP_DIR}"

"${PYTHON_BIN}" -m venv .venv
. .venv/bin/activate
python -m pip install --upgrade pip setuptools wheel
python -m pip install -e .

ENV_FILE_LINE=""
if [ -n "${SYSTEMD_ENV_FILE}" ]; then
  ENV_FILE_LINE="EnvironmentFile=${SYSTEMD_ENV_FILE}"
fi

PYTHONPATH_LINE=""
if [ -n "${PYTHONPATH_VALUE}" ]; then
  PYTHONPATH_LINE="Environment=PYTHONPATH=${PYTHONPATH_VALUE}"
fi

GRID_SYMBOL_LISTS_LINE=""
if [ -n "${GRID_SYMBOL_LISTS_PATH}" ]; then
  GRID_SYMBOL_LISTS_LINE="Environment=GRID_SYMBOL_LISTS_PATH=${GRID_SYMBOL_LISTS_PATH}"
fi

SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"
sudo tee "${SERVICE_FILE}" >/dev/null <<EOF
[Unit]
Description=${SERVICE_DESCRIPTION}
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=${SERVICE_USER}
Group=${SERVICE_USER}
WorkingDirectory=${SERVICE_WORKING_DIR}
${ENV_FILE_LINE}
Environment=PYTHONUNBUFFERED=1
${PYTHONPATH_LINE}
${GRID_SYMBOL_LISTS_LINE}
ExecStart=${EXEC_PYTHON_BIN} -m grid_optimizer.web --host ${GRID_WEB_HOST} --port ${GRID_WEB_PORT}
Restart=always
RestartSec=3
TimeoutStartSec=30
TimeoutStopSec=20
KillMode=process
LimitNOFILE=65535

[Install]
WantedBy=multi-user.target
EOF

if [ "${INSTALL_UPDATE_WRAPPER}" != "0" ]; then
  UPDATE_WRAPPER_PATH="/usr/local/bin/${UPDATE_WRAPPER_NAME}"
  sudo tee "${UPDATE_WRAPPER_PATH}" >/dev/null <<EOF
#!/usr/bin/env bash
set -euo pipefail
export SERVICE_NAME='${SERVICE_NAME}'
export APP_DIR='${APP_DIR}'
exec /usr/local/bin/grid-web-update "\$@"
EOF
  sudo chmod 755 "${UPDATE_WRAPPER_PATH}"
fi

if [ "${INSTALL_RUNNER_WRAPPER}" != "0" ]; then
  RUNNER_WRAPPER_PATH="/usr/local/bin/${RUNNER_WRAPPER_NAME}"
  sudo tee "${RUNNER_WRAPPER_PATH}" >/dev/null <<EOF
#!/usr/bin/env bash
set -euo pipefail
export APP_DIR='${SERVICE_WORKING_DIR}'
export PYTHON_BIN='${EXEC_PYTHON_BIN}'
export PYTHONPATH_VALUE='${PYTHONPATH_VALUE}'
export GRID_API_ENV_FILE='${RUNNER_ENV_FILE}'
exec '${APP_DIR}/deploy/oracle/manage_saved_runner.sh' "\$@"
EOF
  sudo chmod 755 "${RUNNER_WRAPPER_PATH}"
fi

sudo systemctl daemon-reload
sudo systemctl enable "${SERVICE_NAME}.service"
sudo systemctl restart "${SERVICE_NAME}.service"

echo "Service status:"
sudo systemctl --no-pager --full status "${SERVICE_NAME}.service" | sed -n '1,25p'

echo "Deployment complete. Listening on ${GRID_WEB_HOST}:${GRID_WEB_PORT}"
if [ "${INSTALL_UPDATE_WRAPPER}" != "0" ]; then
  echo "Update wrapper installed at /usr/local/bin/${UPDATE_WRAPPER_NAME}"
fi
if [ "${INSTALL_RUNNER_WRAPPER}" != "0" ]; then
  echo "Saved runner wrapper installed at /usr/local/bin/${RUNNER_WRAPPER_NAME}"
fi
