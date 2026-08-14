#!/usr/bin/env bash
set -euo pipefail

if [ "$(id -u)" -ne 0 ]; then
  echo "run as root" >&2
  exit 1
fi

SSHD_BIN="${SSHD_BIN:-/usr/sbin/sshd}"
SSH_SERVICE="${SSH_SERVICE:-ssh}"
CONFIG_PATH="${CONFIG_PATH:-/etc/ssh/sshd_config.d/01-wangge-ssh-resilience.conf}"
backup_path="${CONFIG_PATH}.bak.$(date +%Y%m%d%H%M%S)"
tmp_path="$(mktemp)"
trap 'rm -f "${tmp_path}"' EXIT

cat >"${tmp_path}" <<'EOF'
# Bound unauthenticated SSH sessions so one scanner cannot exhaust MaxStartups.
LoginGraceTime 20
MaxAuthTries 3
MaxStartups 20:50:40
PerSourceMaxStartups 3
EOF

if [ -f "${CONFIG_PATH}" ]; then
  cp -a "${CONFIG_PATH}" "${backup_path}"
fi
install -o root -g root -m 0644 "${tmp_path}" "${CONFIG_PATH}"

if ! "${SSHD_BIN}" -t; then
  if [ -f "${backup_path}" ]; then
    cp -a "${backup_path}" "${CONFIG_PATH}"
  else
    rm -f "${CONFIG_PATH}"
  fi
  "${SSHD_BIN}" -t
  echo "new sshd configuration rejected and rolled back" >&2
  exit 1
fi

systemctl reload "${SSH_SERVICE}"
"${SSHD_BIN}" -T | grep -E '^(logingracetime|maxauthtries|maxstartups|persourcemaxstartups) '
