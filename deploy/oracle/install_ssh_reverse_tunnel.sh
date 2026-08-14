#!/usr/bin/env bash
set -euo pipefail

if [ "$(id -u)" -ne 0 ]; then
  echo "run as root" >&2
  exit 1
fi

REMOTE_HOST="${REMOTE_HOST:-}"
REMOTE_PORT="${REMOTE_PORT:-22}"
REMOTE_USER="${REMOTE_USER:-ubuntu}"
REMOTE_LISTEN_PORT="${REMOTE_LISTEN_PORT:-22150}"
REMOTE_HOST_KEY_LINE="${REMOTE_HOST_KEY_LINE:-}"
KEY_DIR="${KEY_DIR:-/etc/wangge-ssh-rescue}"
SERVICE_NAME="${SERVICE_NAME:-wangge-ssh-reverse-tunnel}"
START_SERVICE="${START_SERVICE:-0}"

if [ -z "${REMOTE_HOST}" ]; then
  echo "REMOTE_HOST must be supplied" >&2
  exit 1
fi
if [ -z "${REMOTE_HOST_KEY_LINE}" ]; then
  echo "REMOTE_HOST_KEY_LINE must be supplied from a separately verified host key" >&2
  exit 1
fi
if ! [[ "${REMOTE_HOST}" =~ ^[A-Za-z0-9.-]+$ && "${REMOTE_USER}" =~ ^[A-Za-z0-9._-]+$ ]]; then
  echo "invalid remote host or user" >&2
  exit 1
fi
if ! [[ "${REMOTE_PORT}" =~ ^[0-9]+$ && "${REMOTE_LISTEN_PORT}" =~ ^[0-9]+$ ]]; then
  echo "ports must be numeric" >&2
  exit 1
fi
if [[ "${REMOTE_HOST_KEY_LINE}" == *$'\n'* ]]; then
  echo "REMOTE_HOST_KEY_LINE must contain exactly one line" >&2
  exit 1
fi
read -r known_host key_type key_data extra <<<"${REMOTE_HOST_KEY_LINE}"
expected_known_host="${REMOTE_HOST}"
if [ "${REMOTE_PORT}" != "22" ]; then
  expected_known_host="[${REMOTE_HOST}]:${REMOTE_PORT}"
fi
if [ "${known_host}" != "${expected_known_host}" ] || [ "${key_type}" != "ssh-ed25519" ] || \
   [ -z "${key_data}" ] || [ -n "${extra:-}" ]; then
  echo "REMOTE_HOST_KEY_LINE must be a single pinned ED25519 key for ${expected_known_host}" >&2
  exit 1
fi

install -d -o root -g root -m 0700 "${KEY_DIR}"
if [ ! -f "${KEY_DIR}/id_ed25519" ]; then
  ssh-keygen -q -t ed25519 -N '' -C 'wangge-150-reverse-rescue' -f "${KEY_DIR}/id_ed25519"
fi
printf '%s\n' "${REMOTE_HOST_KEY_LINE}" >"${KEY_DIR}/known_hosts"
chmod 0600 "${KEY_DIR}/id_ed25519" "${KEY_DIR}/known_hosts"
chmod 0644 "${KEY_DIR}/id_ed25519.pub"

cat >"/etc/systemd/system/${SERVICE_NAME}.service" <<EOF
[Unit]
Description=Wangge SSH reverse rescue tunnel to ${REMOTE_HOST}
After=network-online.target ssh.service
Wants=network-online.target
StartLimitIntervalSec=0

[Service]
Type=simple
User=root
ExecStart=/usr/bin/ssh -NT -i ${KEY_DIR}/id_ed25519 -o IdentitiesOnly=yes -o BatchMode=yes -o StrictHostKeyChecking=yes -o UserKnownHostsFile=${KEY_DIR}/known_hosts -o ExitOnForwardFailure=yes -o ServerAliveInterval=15 -o ServerAliveCountMax=3 -p ${REMOTE_PORT} -R 127.0.0.1:${REMOTE_LISTEN_PORT}:127.0.0.1:22 ${REMOTE_USER}@${REMOTE_HOST}
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable "${SERVICE_NAME}.service"
if [ "${START_SERVICE}" = "1" ]; then
  systemctl restart "${SERVICE_NAME}.service"
fi

printf 'AUTHORIZED_KEY_OPTIONS=no-agent-forwarding,no-X11-forwarding,no-pty,no-user-rc,permitlisten="127.0.0.1:%s" %s\n' \
  "${REMOTE_LISTEN_PORT}" "$(cat "${KEY_DIR}/id_ed25519.pub")"
