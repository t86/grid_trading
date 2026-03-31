#!/usr/bin/env bash
set -euo pipefail

SERVICE_NAME="${SERVICE_NAME:-grid-web}"
BRANCH="${BRANCH:-main}"
APP_DIR="${APP_DIR:-$(systemctl show --property=WorkingDirectory --value "${SERVICE_NAME}.service" 2>/dev/null)}"

if [ -z "${APP_DIR}" ]; then
  echo "Unable to determine WorkingDirectory for ${SERVICE_NAME}.service" >&2
  exit 1
fi

PORT="${GRID_WEB_PORT:-$(systemctl cat "${SERVICE_NAME}.service" | sed -n 's/.*--port \([0-9][0-9]*\).*/\1/p' | tail -n 1)}"
if [ -z "${PORT}" ]; then
  PORT=8787
fi

cleanup_stale_web_processes() {
  local pids pid args
  mapfile -t pids < <(sudo ss -ltnp "( sport = :${PORT} )" 2>/dev/null | sed -n 's/.*pid=\([0-9]\+\).*/\1/p' | sort -u)
  if [ "${#pids[@]}" -eq 0 ]; then
    return 0
  fi
  for pid in "${pids[@]}"; do
    args="$(ps -p "${pid}" -o args= 2>/dev/null || true)"
    if [[ "${args}" == *"${APP_DIR}/.venv/bin/python -m grid_optimizer.web"* ]] || [[ "${args}" == *"python -m grid_optimizer.web"* ]]; then
      sudo kill "${pid}" 2>/dev/null || true
    fi
  done
  sleep 1
  for pid in "${pids[@]}"; do
    if kill -0 "${pid}" 2>/dev/null; then
      sudo kill -9 "${pid}" 2>/dev/null || true
    fi
  done
}

cd "${APP_DIR}"
git fetch origin "${BRANCH}"
git checkout "${BRANCH}"
git reset --hard "origin/${BRANCH}"

sudo systemctl stop "${SERVICE_NAME}.service" || true
cleanup_stale_web_processes
sudo systemctl reset-failed "${SERVICE_NAME}.service" || true

python3 -m venv .venv
. .venv/bin/activate
python -m pip install --upgrade pip setuptools wheel
python -m pip install -e .

sudo systemctl restart "${SERVICE_NAME}.service"
git rev-parse HEAD
systemctl --no-pager --full status "${SERVICE_NAME}.service" | sed -n "1,20p"

for _ in $(seq 1 15); do
  if curl -fsS "http://127.0.0.1:${PORT}/api/health"; then
    echo
    exit 0
  fi
  sleep 1
done

exit 1
