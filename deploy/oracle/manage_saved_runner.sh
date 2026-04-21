#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  manage_saved_runner.sh <start|restart|stop|status> <SYMBOL>

Environment overrides:
  APP_DIR            Working directory that contains output/ and saved runner config.
  PYTHON_BIN         Python binary used to launch grid_optimizer.run_saved_runner.
  GRID_API_ENV_FILE  Shell env file that exports BINANCE_API_KEY / BINANCE_API_SECRET.
  PYTHONPATH_VALUE   Explicit PYTHONPATH passed to the runner process.
  LOG_DIR            Directory for runner nohup logs and pid files.
EOF
}

if [ "${1:-}" = "" ] || [ "${2:-}" = "" ]; then
  usage >&2
  exit 1
fi

ACTION="$(printf '%s' "$1" | tr '[:upper:]' '[:lower:]')"
SYMBOL="$(printf '%s' "$2" | tr '[:lower:]' '[:upper:]')"
APP_DIR="${APP_DIR:-$PWD}"
RUNNER_CODE_DIR="${RUNNER_CODE_DIR:-$APP_DIR}"
PYTHON_BIN="${PYTHON_BIN:-$APP_DIR/.venv/bin/python}"
GRID_API_ENV_FILE="${GRID_API_ENV_FILE:-$APP_DIR/.env.runner}"
PYTHONPATH_VALUE="${PYTHONPATH_VALUE:-$APP_DIR/src}"
LOG_DIR="${LOG_DIR:-$APP_DIR/output}"
SYMBOL_SLUG="$(printf '%s' "$SYMBOL" | tr '[:upper:]' '[:lower:]')"
PID_PATH="$LOG_DIR/${SYMBOL_SLUG}_loop_runner.pid"
LOG_PATH="$LOG_DIR/${SYMBOL_SLUG}_loop_runner.nohup.log"

runner_patterns=(
  "grid_optimizer.run_saved_runner --symbol ${SYMBOL}"
  "grid_optimizer.loop_runner --symbol ${SYMBOL}"
  "grid_optimizer.spot_loop_runner --symbol ${SYMBOL}"
)

ensure_paths() {
  if [ ! -d "$APP_DIR" ]; then
    echo "APP_DIR does not exist: $APP_DIR" >&2
    exit 1
  fi
  if [ ! -d "$RUNNER_CODE_DIR" ]; then
    echo "RUNNER_CODE_DIR does not exist: $RUNNER_CODE_DIR" >&2
    exit 1
  fi
  if [ ! -x "$PYTHON_BIN" ]; then
    echo "python binary not found: $PYTHON_BIN" >&2
    exit 1
  fi
  mkdir -p "$LOG_DIR"
}

load_exchange_env() {
  if [ -f "$GRID_API_ENV_FILE" ]; then
    set -a
    # shellcheck disable=SC1090
    . "$GRID_API_ENV_FILE"
    set +a
  fi
  if [ -z "${BINANCE_API_KEY:-}" ] || [ -z "${BINANCE_API_SECRET:-}" ]; then
    echo "BINANCE_API_KEY / BINANCE_API_SECRET are missing. Set them in env or $GRID_API_ENV_FILE" >&2
    exit 1
  fi
}

find_matching_pids() {
  local pattern pid
  for pattern in "${runner_patterns[@]}"; do
    while IFS= read -r pid; do
      [ -n "$pid" ] && printf '%s\n' "$pid"
    done < <(pgrep -f "$pattern" || true)
  done | awk '!seen[$0]++'
}

stop_runner() {
  local pids
  pids="$(find_matching_pids || true)"
  if [ -n "$pids" ]; then
    while IFS= read -r pid; do
      [ -n "$pid" ] || continue
      kill "$pid" 2>/dev/null || true
    done <<< "$pids"
    sleep 1
    while IFS= read -r pid; do
      [ -n "$pid" ] || continue
      kill -0 "$pid" 2>/dev/null && kill -9 "$pid" 2>/dev/null || true
    done <<< "$pids"
  fi
  if [ -f "$PID_PATH" ]; then
    rm -f "$PID_PATH"
  fi
}

print_status() {
  local pids
  pids="$(find_matching_pids || true)"
  if [ -n "$pids" ]; then
    echo "runner_status=running"
    ps -fp "$(printf '%s\n' "$pids" | paste -sd, -)"
  else
    echo "runner_status=stopped"
  fi
  if [ -f "$PID_PATH" ]; then
    echo "pid_file=$(cat "$PID_PATH" 2>/dev/null || true)"
  fi
  if [ -f "$LOG_PATH" ]; then
    echo "--- tail ${LOG_PATH} ---"
    tail -n 10 "$LOG_PATH" || true
  fi
}

start_runner() {
  ensure_paths
  load_exchange_env
  cd "$RUNNER_CODE_DIR"
  if [ -f "$LOG_PATH" ]; then
    mv "$LOG_PATH" "${LOG_PATH}.$(date +%Y%m%d_%H%M%S)"
  fi
  nohup env PYTHONPATH="$PYTHONPATH_VALUE" GRID_RUNNER_WORK_DIR="$APP_DIR" "$PYTHON_BIN" -u -m grid_optimizer.run_saved_runner --symbol "$SYMBOL" >"$LOG_PATH" 2>&1 < /dev/null &
  sleep 2
  print_status
}

case "$ACTION" in
  start)
    ensure_paths
    if [ -n "$(find_matching_pids || true)" ]; then
      echo "runner already running for $SYMBOL"
      print_status
      exit 0
    fi
    start_runner
    ;;
  restart)
    ensure_paths
    stop_runner
    start_runner
    ;;
  stop)
    ensure_paths
    stop_runner
    print_status
    ;;
  status)
    ensure_paths
    print_status
    ;;
  *)
    usage >&2
    exit 1
    ;;
esac
