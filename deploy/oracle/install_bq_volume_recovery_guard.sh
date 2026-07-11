#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${APP_DIR:-$PWD}"
SERVICE_USER="${SERVICE_USER:-$(id -un)}"
SERVICE_GROUP="${SERVICE_GROUP:-$SERVICE_USER}"
PYTHON_BIN="${PYTHON_BIN:-${APP_DIR}/.venv/bin/python}"
PYTHONPATH_VALUE="${PYTHONPATH_VALUE:-${APP_DIR}/src}"
ENV_FILE="${ENV_FILE:-/home/ubuntu/.config/wangge/binance_api_env.env}"
TIMER_UNIT_NAME="${TIMER_UNIT_NAME:-grid-bq-volume-recovery-guard}"
SYMBOLS="${SYMBOLS:-ARXUSDT,OUSDT}"
OUTPUT_DIR="${OUTPUT_DIR:-${APP_DIR}/output}"
STATE_PATH="${STATE_PATH:-${APP_DIR}/output/bq_volume_recovery_guard_state.json}"
RUNNER_WRAPPER="${RUNNER_WRAPPER:-/usr/local/bin/grid-saved-runner}"
WINDOW_SECONDS="${WINDOW_SECONDS:-180}"
MIN_VOLUME_NOTIONAL="${MIN_VOLUME_NOTIONAL:-125}"
TRIGGER_SECONDS="${TRIGGER_SECONDS:-120}"
RECOVER_MIN_VOLUME_NOTIONAL="${RECOVER_MIN_VOLUME_NOTIONAL:-${MIN_VOLUME_NOTIONAL}}"
DAILY_TARGETS="${DAILY_TARGETS:-ARXUSDT=180000,OUSDT=60000}"
TARGET_PACE_FRACTION="${TARGET_PACE_FRACTION:-1.05}"
TARGET_PACE_MAX_MULTIPLIER="${TARGET_PACE_MAX_MULTIPLIER:-5.0}"
TARGET_COMPLETION_BUFFER_SECONDS="${TARGET_COMPLETION_BUFFER_SECONDS:-10800}"
NEAR_CAP_RATIO="${NEAR_CAP_RATIO:-0.95}"
RECOVER_CAP_RATIO="${RECOVER_CAP_RATIO:-0.96}"
FAR_TICKS="${FAR_TICKS:-8}"
PLAN_STALE_SECONDS="${PLAN_STALE_SECONDS:-300}"
ALLOW_INVENTORY_COST_GATE_DISABLE="${ALLOW_INVENTORY_COST_GATE_DISABLE:-true}"
MAX_RECOVERY_SECONDS="${MAX_RECOVERY_SECONDS:-300}"
COOLDOWN_SECONDS="${COOLDOWN_SECONDS:-600}"
INVENTORY_BIAS_RELIEF_NOTIONAL_MARGIN="${INVENTORY_BIAS_RELIEF_NOTIONAL_MARGIN:-24}"
VOLUME_RECOVERY_CYCLE_BUDGET_INCREMENT="${VOLUME_RECOVERY_CYCLE_BUDGET_INCREMENT:-12}"
ON_UNIT_ACTIVE_SEC="${ON_UNIT_ACTIVE_SEC:-1min}"

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

if [ ! -r "$ENV_FILE" ]; then
  echo "Binance environment file is not readable: $ENV_FILE" >&2
  exit 1
fi

if [ ! -x "$RUNNER_WRAPPER" ]; then
  echo "runner wrapper not found or not executable: $RUNNER_WRAPPER" >&2
  exit 1
fi

if [ "$ALLOW_INVENTORY_COST_GATE_DISABLE" = "true" ]; then
  COST_GATE_FLAG="--allow-inventory-cost-gate-disable"
else
  COST_GATE_FLAG="--no-allow-inventory-cost-gate-disable"
fi

SERVICE_FILE="/etc/systemd/system/${TIMER_UNIT_NAME}.service"
TIMER_FILE="/etc/systemd/system/${TIMER_UNIT_NAME}.timer"

sudo tee "$SERVICE_FILE" >/dev/null <<EOF
[Unit]
Description=Grid BQ volume recovery guard
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
User=${SERVICE_USER}
Group=${SERVICE_GROUP}
WorkingDirectory=${APP_DIR}
Environment=PYTHONUNBUFFERED=1
Environment=PYTHONPATH=${PYTHONPATH_VALUE}
EnvironmentFile=${ENV_FILE}
ExecStart=${PYTHON_BIN} -m grid_optimizer.bq_volume_recovery_guard --symbols ${SYMBOLS} --output-dir ${OUTPUT_DIR} --state-path ${STATE_PATH} --runner-wrapper ${RUNNER_WRAPPER} --window-seconds ${WINDOW_SECONDS} --min-volume-notional ${MIN_VOLUME_NOTIONAL} --trigger-seconds ${TRIGGER_SECONDS} --recover-min-volume-notional ${RECOVER_MIN_VOLUME_NOTIONAL} --daily-targets ${DAILY_TARGETS} --target-pace-fraction ${TARGET_PACE_FRACTION} --target-pace-max-multiplier ${TARGET_PACE_MAX_MULTIPLIER} --target-completion-buffer-seconds ${TARGET_COMPLETION_BUFFER_SECONDS} --near-cap-ratio ${NEAR_CAP_RATIO} --recover-cap-ratio ${RECOVER_CAP_RATIO} --far-ticks ${FAR_TICKS} --plan-stale-seconds ${PLAN_STALE_SECONDS} --max-recovery-seconds ${MAX_RECOVERY_SECONDS} --cooldown-seconds ${COOLDOWN_SECONDS} --inventory-bias-relief-notional-margin ${INVENTORY_BIAS_RELIEF_NOTIONAL_MARGIN} --volume-recovery-cycle-budget-increment ${VOLUME_RECOVERY_CYCLE_BUDGET_INCREMENT} ${COST_GATE_FLAG}
EOF

sudo tee "$TIMER_FILE" >/dev/null <<EOF
[Unit]
Description=Run Grid BQ volume recovery guard

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
