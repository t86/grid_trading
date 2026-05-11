#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${APP_DIR:-$PWD}"
RUNNER_CODE_DIR="${RUNNER_CODE_DIR:-$APP_DIR}"
SERVICE_USER="${SERVICE_USER:-$(id -un)}"
WATCHDOG_NAME="${WATCHDOG_NAME:-remote-web-watchdog}"
REMOTE_LABEL="${REMOTE_LABEL:-remote}"
HEALTHCHECK_URL="${HEALTHCHECK_URL:-}"
STATUS_URL="${STATUS_URL:-}"
CONNECT_TIMEOUT_SECONDS="${CONNECT_TIMEOUT_SECONDS:-3}"
MAX_TIME_SECONDS="${MAX_TIME_SECONDS:-12}"
FAILURE_THRESHOLD="${FAILURE_THRESHOLD:-2}"
REMEDIATION_COOLDOWN_SECONDS="${REMEDIATION_COOLDOWN_SECONDS:-600}"
REMEDIATION_COMMAND="${REMEDIATION_COMMAND:-}"
AUTH_USERNAME="${AUTH_USERNAME:-}"
AUTH_PASSWORD="${AUTH_PASSWORD:-}"
ON_BOOT_SEC="${ON_BOOT_SEC:-2min}"
ON_UNIT_ACTIVE_SEC="${ON_UNIT_ACTIVE_SEC:-1min}"

if ! command -v sudo >/dev/null 2>&1; then
  echo "sudo is required for systemd installation." >&2
  exit 1
fi

if [ ! -d "${APP_DIR}" ]; then
  echo "APP_DIR does not exist: ${APP_DIR}" >&2
  exit 1
fi

if [ ! -f "${RUNNER_CODE_DIR}/deploy/oracle/remote_web_watchdog.sh" ]; then
  echo "watchdog script missing under ${RUNNER_CODE_DIR}/deploy/oracle" >&2
  exit 1
fi

WATCHDOG_SERVICE_FILE="/etc/systemd/system/${WATCHDOG_NAME}.service"
WATCHDOG_TIMER_FILE="/etc/systemd/system/${WATCHDOG_NAME}.timer"

systemd_escape_env() {
  local value="$1"
  value="${value//\\/\\\\}"
  value="${value//\"/\\\"}"
  printf '%s' "${value}"
}

REMEDIATION_COMMAND_ESCAPED="$(systemd_escape_env "${REMEDIATION_COMMAND}")"

sudo tee "${WATCHDOG_SERVICE_FILE}" >/dev/null <<EOF
[Unit]
Description=Grid remote web watchdog for ${REMOTE_LABEL}
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
User=${SERVICE_USER}
Group=${SERVICE_USER}
WorkingDirectory=${APP_DIR}
Environment=WATCHDOG_NAME=${WATCHDOG_NAME}
Environment=REMOTE_LABEL=${REMOTE_LABEL}
Environment=HEALTHCHECK_URL=${HEALTHCHECK_URL}
Environment=STATUS_URL=${STATUS_URL}
Environment=CONNECT_TIMEOUT_SECONDS=${CONNECT_TIMEOUT_SECONDS}
Environment=MAX_TIME_SECONDS=${MAX_TIME_SECONDS}
Environment=FAILURE_THRESHOLD=${FAILURE_THRESHOLD}
Environment=REMEDIATION_COOLDOWN_SECONDS=${REMEDIATION_COOLDOWN_SECONDS}
Environment=AUTH_USERNAME=${AUTH_USERNAME}
Environment=AUTH_PASSWORD=${AUTH_PASSWORD}
Environment="REMEDIATION_COMMAND=${REMEDIATION_COMMAND_ESCAPED}"
ExecStart=/usr/bin/env bash ${RUNNER_CODE_DIR}/deploy/oracle/remote_web_watchdog.sh
EOF

sudo tee "${WATCHDOG_TIMER_FILE}" >/dev/null <<EOF
[Unit]
Description=Run Grid remote web watchdog for ${REMOTE_LABEL}

[Timer]
OnBootSec=${ON_BOOT_SEC}
OnUnitActiveSec=${ON_UNIT_ACTIVE_SEC}
AccuracySec=15s
Unit=${WATCHDOG_NAME}.service

[Install]
WantedBy=timers.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable "${WATCHDOG_NAME}.timer"
sudo systemctl restart "${WATCHDOG_NAME}.timer"

echo "Remote web watchdog installed: ${WATCHDOG_NAME}.timer"
sudo systemctl --no-pager --full status "${WATCHDOG_NAME}.timer" | sed -n '1,20p'
