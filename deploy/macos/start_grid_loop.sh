#!/bin/zsh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-/opt/anaconda3/bin/python3}"
GRID_API_ENV_FILE="${GRID_API_ENV_FILE:-/tmp/binance_api_env.sh}"

if [[ ! -x "$PYTHON_BIN" ]]; then
  echo "python binary not found: $PYTHON_BIN" >&2
  exit 1
fi

cd "$ROOT_DIR"
export PYTHONPATH="$ROOT_DIR/src"

if [[ -f "$GRID_API_ENV_FILE" ]]; then
  source "$GRID_API_ENV_FILE"
fi

exec "$PYTHON_BIN" -m grid_optimizer.run_saved_runner
