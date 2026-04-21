#!/usr/bin/env bash
set -euo pipefail

SYMBOL="$(printf '%s' "${1:-}" | tr '[:lower:]' '[:upper:]')"
if [ -z "$SYMBOL" ]; then
  echo "Usage: runner_watchdog.sh <SYMBOL>" >&2
  exit 2
fi

APP_DIR="${APP_DIR:-$PWD}"
RUNNER_SERVICE_TEMPLATE="${GRID_RUNNER_SERVICE_TEMPLATE:-${RUNNER_SERVICE_TEMPLATE:-grid-loop@{symbol}.service}}"
RUNNER_WATCHDOG_STALE_SECONDS="${RUNNER_WATCHDOG_STALE_SECONDS:-180}"

SYMBOL_LOWER="$(printf '%s' "$SYMBOL" | tr '[:upper:]' '[:lower:]')"
SYMBOL_SLUG="$(printf '%s' "$SYMBOL" | sed -E 's/[^A-Za-z0-9]+/_/g; s/^_+//; s/_+$//' | tr '[:upper:]' '[:lower:]')"
SERVICE_NAME="${RUNNER_SERVICE_TEMPLATE//\{symbol\}/${SYMBOL}}"
SERVICE_NAME="${SERVICE_NAME//\{symbol_upper\}/${SYMBOL}}"
SERVICE_NAME="${SERVICE_NAME//\{symbol_lower\}/${SYMBOL_LOWER}}"
SERVICE_NAME="${SERVICE_NAME//\{slug\}/${SYMBOL_SLUG}}"

CONTROL_PATH="${APP_DIR}/output/${SYMBOL_SLUG}_loop_runner_control.json"
EVENTS_PATH="${APP_DIR}/output/${SYMBOL_SLUG}_loop_events.jsonl"

if [ ! -f "$CONTROL_PATH" ]; then
  echo "runner_watchdog: no control config for $SYMBOL at $CONTROL_PATH; skip"
  exit 0
fi

if ! systemctl is-active --quiet "$SERVICE_NAME"; then
  echo "runner_watchdog: $SERVICE_NAME is not active; skip"
  exit 0
fi

if [ ! -f "$EVENTS_PATH" ]; then
  echo "runner_watchdog: $EVENTS_PATH does not exist; restart $SERVICE_NAME"
  systemctl restart "$SERVICE_NAME"
  exit 0
fi

now="$(date +%s)"
mtime="$(stat -c %Y "$EVENTS_PATH")"
age="$((now - mtime))"
if [ "$age" -le "$RUNNER_WATCHDOG_STALE_SECONDS" ]; then
  echo "runner_watchdog: $SERVICE_NAME healthy; events age ${age}s"
  exit 0
fi

echo "runner_watchdog: $SERVICE_NAME stale; events age ${age}s > ${RUNNER_WATCHDOG_STALE_SECONDS}s; restart"
systemctl restart "$SERVICE_NAME"
