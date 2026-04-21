#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${APP_DIR:-$PWD}"
RUNNER_CODE_DIR="${RUNNER_CODE_DIR:-$APP_DIR}"
SERVICE_USER="${SERVICE_USER:-$(id -un)}"
SERVICE_GROUP="${SERVICE_GROUP:-$SERVICE_USER}"
PYTHON_BIN="${PYTHON_BIN:-${RUNNER_CODE_DIR}/.venv/bin/python}"
PYTHONPATH_VALUE="${PYTHONPATH_VALUE:-${RUNNER_CODE_DIR}/src}"
GRID_API_ENV_FILE="${GRID_API_ENV_FILE:-}"
RUNNER_UNIT_NAME="${RUNNER_UNIT_NAME:-grid-loop}"
WATCHDOG_UNIT_NAME="${WATCHDOG_UNIT_NAME:-${RUNNER_UNIT_NAME}-watchdog}"
if [ -z "${GRID_RUNNER_SERVICE_TEMPLATE:-}" ]; then
  GRID_RUNNER_SERVICE_TEMPLATE="${RUNNER_UNIT_NAME}"'@{symbol}.service'
fi
RUNNER_WATCHDOG_STALE_SECONDS="${RUNNER_WATCHDOG_STALE_SECONDS:-180}"
INSTALL_WATCHDOG="${INSTALL_WATCHDOG:-1}"
INSTALL_RUNNER_WRAPPER="${INSTALL_RUNNER_WRAPPER:-1}"
RUNNER_WRAPPER_NAME="${RUNNER_WRAPPER_NAME:-grid-saved-runner}"
WEB_SERVICE_NAME="${WEB_SERVICE_NAME:-}"
RESTART_WEB_SERVICE="${RESTART_WEB_SERVICE:-1}"
SYMBOLS="${SYMBOLS:-}"
START_NOW="${START_NOW:-0}"

if ! command -v sudo >/dev/null 2>&1; then
  echo "sudo is required for systemd installation." >&2
  exit 1
fi

if [ ! -d "$APP_DIR" ]; then
  echo "APP_DIR does not exist: $APP_DIR" >&2
  exit 1
fi

if [ ! -d "$RUNNER_CODE_DIR" ]; then
  echo "RUNNER_CODE_DIR does not exist: $RUNNER_CODE_DIR" >&2
  exit 1
fi

if [ ! -x "$PYTHON_BIN" ]; then
  echo "python binary not found or not executable: $PYTHON_BIN" >&2
  exit 1
fi

if [ -z "$GRID_API_ENV_FILE" ] || [ ! -f "$GRID_API_ENV_FILE" ]; then
  echo "GRID_API_ENV_FILE is required and must exist." >&2
  exit 1
fi

RUNNER_SERVICE_FILE="/etc/systemd/system/${RUNNER_UNIT_NAME}@.service"
sudo tee "$RUNNER_SERVICE_FILE" >/dev/null <<EOF
[Unit]
Description=Grid saved runner for %i
After=network-online.target
Wants=network-online.target
StartLimitIntervalSec=300
StartLimitBurst=20

[Service]
Type=simple
User=${SERVICE_USER}
Group=${SERVICE_GROUP}
WorkingDirectory=${RUNNER_CODE_DIR}
EnvironmentFile=${GRID_API_ENV_FILE}
Environment=PYTHONUNBUFFERED=1
Environment=PYTHONPATH=${PYTHONPATH_VALUE}
Environment=GRID_RUNNER_WORK_DIR=${APP_DIR}
Environment=GRID_RUNNER_SYMBOL=%i
Environment=GRID_RUNNER_SERVICE_TEMPLATE=${GRID_RUNNER_SERVICE_TEMPLATE}
ExecStart=${PYTHON_BIN} -u -m grid_optimizer.run_saved_runner --symbol %i
Restart=always
RestartSec=5
TimeoutStartSec=30
TimeoutStopSec=20
KillMode=control-group
LimitNOFILE=65535

[Install]
WantedBy=multi-user.target
EOF

if [ "$INSTALL_WATCHDOG" != "0" ]; then
  WATCHDOG_SERVICE_FILE="/etc/systemd/system/${WATCHDOG_UNIT_NAME}@.service"
  WATCHDOG_TIMER_FILE="/etc/systemd/system/${WATCHDOG_UNIT_NAME}@.timer"
  sudo tee "$WATCHDOG_SERVICE_FILE" >/dev/null <<EOF
[Unit]
Description=Grid saved runner watchdog for %i
After=network-online.target ${RUNNER_UNIT_NAME}@%i.service

[Service]
Type=oneshot
Environment=APP_DIR=${APP_DIR}
Environment=GRID_RUNNER_SERVICE_TEMPLATE=${GRID_RUNNER_SERVICE_TEMPLATE}
Environment=RUNNER_WATCHDOG_STALE_SECONDS=${RUNNER_WATCHDOG_STALE_SECONDS}
ExecStart=${RUNNER_CODE_DIR}/deploy/oracle/runner_watchdog.sh %i
EOF
  sudo tee "$WATCHDOG_TIMER_FILE" >/dev/null <<EOF
[Unit]
Description=Run Grid saved runner watchdog for %i

[Timer]
OnBootSec=2min
OnUnitActiveSec=1min
AccuracySec=15s
Unit=${WATCHDOG_UNIT_NAME}@%i.service

[Install]
WantedBy=timers.target
EOF
fi

if [ "$INSTALL_RUNNER_WRAPPER" != "0" ]; then
  RUNNER_WRAPPER_PATH="/usr/local/bin/${RUNNER_WRAPPER_NAME}"
  sudo tee "$RUNNER_WRAPPER_PATH" >/dev/null <<EOF
#!/usr/bin/env bash
set -euo pipefail
export APP_DIR='${APP_DIR}'
export RUNNER_CODE_DIR='${RUNNER_CODE_DIR}'
export PYTHON_BIN='${PYTHON_BIN}'
export PYTHONPATH_VALUE='${PYTHONPATH_VALUE}'
export GRID_API_ENV_FILE='${GRID_API_ENV_FILE}'
export GRID_RUNNER_SERVICE_TEMPLATE='${GRID_RUNNER_SERVICE_TEMPLATE}'
exec '${RUNNER_CODE_DIR}/deploy/oracle/manage_saved_runner.sh' "\$@"
EOF
  sudo chmod 755 "$RUNNER_WRAPPER_PATH"
fi

if [ -n "$WEB_SERVICE_NAME" ]; then
  sudo mkdir -p "/etc/systemd/system/${WEB_SERVICE_NAME}.service.d"
  sudo tee "/etc/systemd/system/${WEB_SERVICE_NAME}.service.d/20-runner-systemd.conf" >/dev/null <<EOF
[Service]
Environment=GRID_RUNNER_SERVICE_TEMPLATE=${GRID_RUNNER_SERVICE_TEMPLATE}
EOF
fi

sudo systemctl daemon-reload

if [ -n "$WEB_SERVICE_NAME" ] && [ "$RESTART_WEB_SERVICE" != "0" ]; then
  sudo systemctl restart "${WEB_SERVICE_NAME}.service"
fi

if [ -n "$SYMBOLS" ]; then
  for raw_symbol in ${SYMBOLS//,/ }; do
    symbol="$(printf '%s' "$raw_symbol" | tr '[:lower:]' '[:upper:]')"
    [ -n "$symbol" ] || continue
    sudo systemctl enable "${RUNNER_UNIT_NAME}@${symbol}.service"
    if [ "$INSTALL_WATCHDOG" != "0" ]; then
      sudo systemctl enable "${WATCHDOG_UNIT_NAME}@${symbol}.timer"
      sudo systemctl start "${WATCHDOG_UNIT_NAME}@${symbol}.timer"
    fi
    if [ "$START_NOW" != "0" ]; then
      sudo systemctl restart "${RUNNER_UNIT_NAME}@${symbol}.service"
    fi
  done
fi

echo "Runner systemd template installed: ${RUNNER_UNIT_NAME}@.service"
if [ "$INSTALL_WATCHDOG" != "0" ]; then
  echo "Runner watchdog installed: ${WATCHDOG_UNIT_NAME}@.timer"
fi
if [ "$INSTALL_RUNNER_WRAPPER" != "0" ]; then
  echo "Saved runner wrapper installed: /usr/local/bin/${RUNNER_WRAPPER_NAME}"
fi
if [ -n "$WEB_SERVICE_NAME" ]; then
  echo "Web service runner template configured: ${WEB_SERVICE_NAME}.service -> ${GRID_RUNNER_SERVICE_TEMPLATE}"
fi
