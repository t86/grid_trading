#!/usr/bin/env bash
set -euo pipefail

if [ "$(id -u)" -ne 0 ]; then
  echo "run as root" >&2
  exit 1
fi

RUNNER_CODE_DIR="${RUNNER_CODE_DIR:-$PWD}"
PROBE_USER="${PROBE_USER:-ubuntu}"
KEY_DIR="${KEY_DIR:-/etc/wangge-ssh-watchdog}"
SERVICE_NAME="${SERVICE_NAME:-wangge-ssh-watchdog}"
FAILURE_THRESHOLD="${FAILURE_THRESHOLD:-3}"
COOLDOWN_SECONDS="${COOLDOWN_SECONDS:-600}"
ON_BOOT_SEC="${ON_BOOT_SEC:-2min}"
ON_UNIT_ACTIVE_SEC="${ON_UNIT_ACTIVE_SEC:-30s}"

if [ ! -f "${RUNNER_CODE_DIR}/deploy/oracle/ssh_access_watchdog.sh" ]; then
  echo "watchdog script missing under ${RUNNER_CODE_DIR}/deploy/oracle" >&2
  exit 1
fi

user_home="$(getent passwd "${PROBE_USER}" | cut -d: -f6)"
user_group="$(id -gn "${PROBE_USER}")"
if [ -z "${user_home}" ]; then
  echo "probe user does not exist: ${PROBE_USER}" >&2
  exit 1
fi

install -d -o root -g root -m 0700 "${KEY_DIR}"
if [ ! -f "${KEY_DIR}/id_ed25519" ]; then
  ssh-keygen -q -t ed25519 -N '' -C 'wangge-local-ssh-watchdog' -f "${KEY_DIR}/id_ed25519"
fi
chmod 0600 "${KEY_DIR}/id_ed25519"
chmod 0644 "${KEY_DIR}/id_ed25519.pub"
if [ ! -f /etc/ssh/ssh_host_ed25519_key.pub ]; then
  echo "local ED25519 SSH host key is missing" >&2
  exit 1
fi
awk '{print "127.0.0.1 " $1 " " $2}' /etc/ssh/ssh_host_ed25519_key.pub >"${KEY_DIR}/known_hosts"
chmod 0600 "${KEY_DIR}/known_hosts"

install -d -o "${PROBE_USER}" -g "${user_group}" -m 0700 "${user_home}/.ssh"
touch "${user_home}/.ssh/authorized_keys"
chown "${PROBE_USER}:${user_group}" "${user_home}/.ssh/authorized_keys"
chmod 0600 "${user_home}/.ssh/authorized_keys"
public_key="$(cat "${KEY_DIR}/id_ed25519.pub")"
key_line="from=\"127.0.0.1,::1\",command=\"/bin/true\",no-agent-forwarding,no-port-forwarding,no-pty,no-user-rc,no-X11-forwarding ${public_key}"
if ! grep -qF 'wangge-local-ssh-watchdog' "${user_home}/.ssh/authorized_keys"; then
  printf '%s\n' "${key_line}" >>"${user_home}/.ssh/authorized_keys"
fi

cat >"/etc/systemd/system/${SERVICE_NAME}.service" <<EOF
[Unit]
Description=Wangge localhost SSH access watchdog
After=ssh.service

[Service]
Type=oneshot
User=root
Environment=PROBE_KEY=${KEY_DIR}/id_ed25519
Environment=KNOWN_HOSTS_FILE=${KEY_DIR}/known_hosts
Environment=PROBE_USER=${PROBE_USER}
Environment=FAILURE_THRESHOLD=${FAILURE_THRESHOLD}
Environment=COOLDOWN_SECONDS=${COOLDOWN_SECONDS}
ExecStart=/usr/bin/env bash ${RUNNER_CODE_DIR}/deploy/oracle/ssh_access_watchdog.sh
EOF

cat >"/etc/systemd/system/${SERVICE_NAME}.timer" <<EOF
[Unit]
Description=Run Wangge SSH access watchdog

[Timer]
OnBootSec=${ON_BOOT_SEC}
OnUnitActiveSec=${ON_UNIT_ACTIVE_SEC}
AccuracySec=5s
Unit=${SERVICE_NAME}.service

[Install]
WantedBy=timers.target
EOF

systemctl daemon-reload
systemctl enable "${SERVICE_NAME}.timer"
systemctl restart "${SERVICE_NAME}.timer"
systemctl start "${SERVICE_NAME}.service"
