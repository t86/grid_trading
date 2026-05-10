#!/usr/bin/env bash
set -euo pipefail

SERVICE_NAME="${SERVICE_NAME:-grid-web}"
SERVICE_USER="${SERVICE_USER:-$(id -un)}"
OUTPUT_DIR="${OUTPUT_DIR:-$PWD/output}"
ROTATE_NAME="${ROTATE_NAME:-${SERVICE_NAME}-output-jsonl}"
MAX_SIZE="${MAX_SIZE:-200M}"
ROTATE_COUNT="${ROTATE_COUNT:-14}"
MAX_AGE_DAYS="${MAX_AGE_DAYS:-14}"
ON_BOOT_SEC="${ON_BOOT_SEC:-5min}"
ON_UNIT_ACTIVE_SEC="${ON_UNIT_ACTIVE_SEC:-15min}"
RUN_NOW="${RUN_NOW:-1}"

if ! command -v sudo >/dev/null 2>&1; then
  echo "sudo is required for logrotate installation." >&2
  exit 1
fi

if ! command -v logrotate >/dev/null 2>&1; then
  echo "logrotate is required." >&2
  exit 1
fi

if [ ! -d "${OUTPUT_DIR}" ]; then
  echo "OUTPUT_DIR does not exist: ${OUTPUT_DIR}" >&2
  exit 1
fi

LOGROTATE_CONF="/etc/logrotate.d/${ROTATE_NAME}"
LOGROTATE_STATE="/var/lib/logrotate/${ROTATE_NAME}.status"
SERVICE_FILE="/etc/systemd/system/${ROTATE_NAME}.service"
TIMER_FILE="/etc/systemd/system/${ROTATE_NAME}.timer"

sudo tee "${LOGROTATE_CONF}" >/dev/null <<EOF
${OUTPUT_DIR}/*_loop_events.jsonl
${OUTPUT_DIR}/*_loop_plan_audit.jsonl
${OUTPUT_DIR}/*_loop_submit_audit.jsonl
${OUTPUT_DIR}/*_loop_order_audit.jsonl
${OUTPUT_DIR}/*_spot_events.jsonl
${OUTPUT_DIR}/*_spot_flatten_events.jsonl
{
    su ${SERVICE_USER} ${SERVICE_USER}
    size ${MAX_SIZE}
    rotate ${ROTATE_COUNT}
    maxage ${MAX_AGE_DAYS}
    missingok
    notifempty
    copytruncate
    compress
    delaycompress
    dateext
    dateformat -%Y%m%d-%s
    create 0644 ${SERVICE_USER} ${SERVICE_USER}
}
EOF

sudo tee "${SERVICE_FILE}" >/dev/null <<EOF
[Unit]
Description=Rotate Grid output JSONL files for ${SERVICE_NAME}

[Service]
Type=oneshot
ExecStart=/usr/sbin/logrotate -s ${LOGROTATE_STATE} ${LOGROTATE_CONF}
EOF

sudo tee "${TIMER_FILE}" >/dev/null <<EOF
[Unit]
Description=Run Grid output JSONL rotation for ${SERVICE_NAME}

[Timer]
OnBootSec=${ON_BOOT_SEC}
OnUnitActiveSec=${ON_UNIT_ACTIVE_SEC}
AccuracySec=30s
Unit=${ROTATE_NAME}.service

[Install]
WantedBy=timers.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable "${ROTATE_NAME}.timer"
sudo systemctl restart "${ROTATE_NAME}.timer"

if [ "${RUN_NOW}" = "1" ]; then
  sudo systemctl start "${ROTATE_NAME}.service"
fi

echo "Output JSONL logrotate installed: ${ROTATE_NAME}.timer"
sudo systemctl --no-pager --full status "${ROTATE_NAME}.timer" | sed -n '1,20p'
