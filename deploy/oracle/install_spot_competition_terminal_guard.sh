#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 3 ]]; then
  echo "usage: $0 SYMBOL TARGET_VOLUME START_MS" >&2
  exit 64
fi

symbol="${1^^}"
target_volume="$2"
start_ms="$3"
app_dir="${APP_DIR:-/home/ubuntu/wangge}"
python_bin="${PYTHON_BIN:-${app_dir}/.venv/bin/python}"
runner_wrapper="${RUNNER_WRAPPER:-/usr/local/bin/grid-saved-runner}"
config_dir="/etc/grid-spot-terminal"
service_path="/etc/systemd/system/grid-spot-terminal@.service"
env_path="${config_dir}/${symbol}.env"
runner_dropin_dir="/etc/systemd/system/grid-loop@${symbol}.service.d"

sudo install -d -m 0755 "${config_dir}"
tmp_env="$(mktemp)"
trap 'rm -f "${tmp_env}"' EXIT
printf '%s\n' \
  "TARGET_VOLUME=${target_volume}" \
  "START_MS=${start_ms}" \
  "APP_DIR=${app_dir}" \
  "PYTHON_BIN=${python_bin}" \
  "RUNNER_WRAPPER=${runner_wrapper}" >"${tmp_env}"
sudo install -m 0644 "${tmp_env}" "${env_path}"

tmp_service="$(mktemp)"
cat >"${tmp_service}" <<'UNIT'
[Unit]
Description=Spot competition terminal guard for %i
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=ubuntu
Group=ubuntu
WorkingDirectory=/home/ubuntu/wangge
EnvironmentFile=/home/ubuntu/.config/wangge/binance_api_env.env
EnvironmentFile=/etc/grid-spot-terminal/%i.env
Environment=PYTHONUNBUFFERED=1
Environment=PYTHONPATH=/home/ubuntu/wangge/src
ExecStart=/home/ubuntu/wangge/.venv/bin/python -m grid_optimizer.spot_competition_terminal_guard --symbol %i --target-volume ${TARGET_VOLUME} --start-ms ${START_MS} --wrapper ${RUNNER_WRAPPER} --service grid-loop@%i.service --python-bin ${PYTHON_BIN} --state output/%i_spot_terminal_state.json --events output/%i_spot_terminal_events.jsonl --spot-flatten-events output/%i_spot_terminal_spot_flatten_events.jsonl --futures-flatten-events output/%i_spot_terminal_futures_flatten_events.jsonl
Restart=on-failure
RestartSec=10
TimeoutStopSec=30
KillMode=control-group

[Install]
WantedBy=multi-user.target
UNIT
sudo install -m 0644 "${tmp_service}" "${service_path}"
rm -f "${tmp_service}"

sudo install -d -m 0755 "${runner_dropin_dir}"
tmp_dropin="$(mktemp)"
cat >"${tmp_dropin}" <<'UNIT'
[Service]
Restart=no
UNIT
sudo install -m 0644 "${tmp_dropin}" "${runner_dropin_dir}/90-spot-terminal-no-restart.conf"
rm -f "${tmp_dropin}"

sudo systemctl daemon-reload
sudo systemctl enable "grid-spot-terminal@${symbol}.service"
echo "installed grid-spot-terminal@${symbol}.service target=${target_volume} start_ms=${start_ms}"
