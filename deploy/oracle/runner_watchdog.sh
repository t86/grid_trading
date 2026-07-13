#!/usr/bin/env bash
set -euo pipefail

SYMBOL="$(printf '%s' "${1:-}" | tr '[:lower:]' '[:upper:]')"
if [ -z "$SYMBOL" ]; then
  echo "Usage: runner_watchdog.sh <SYMBOL>" >&2
  exit 2
fi

APP_DIR="${APP_DIR:-$PWD}"
if [ -n "${GRID_RUNNER_SERVICE_TEMPLATE:-}" ]; then
  RUNNER_SERVICE_TEMPLATE="$GRID_RUNNER_SERVICE_TEMPLATE"
elif [ -n "${RUNNER_SERVICE_TEMPLATE:-}" ]; then
  RUNNER_SERVICE_TEMPLATE="$RUNNER_SERVICE_TEMPLATE"
else
  RUNNER_SERVICE_TEMPLATE='grid-loop@{symbol}.service'
fi
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

# ARXUSDT/OUSDT are recovery-managed during the single-writer rollout.  The
# watchdog remains an observer: it must not start/restart a runner that the
# recovery actuator is reconciling.  The owner marker extends this rule to
# later managed symbols without another shell change.
if python3 - "$SYMBOL" "$CONTROL_PATH" <<'PY'
import json
import sys

symbol, control_path = sys.argv[1:]
try:
    with open(control_path, encoding="utf-8") as handle:
        control = json.load(handle)
except (OSError, ValueError):
    control = {}
managed = symbol in {"ARXUSDT", "OUSDT"} or control.get("recovery_control_owner") == "bq_volume_recovery_guard"
raise SystemExit(0 if managed else 1)
PY
then
  echo "runner_watchdog: $SYMBOL is recovery-managed; skip actuator"
  exit 0
fi

# Echoes a reason and returns 0 when the runner's stop is INTENDED (target gate done
# today, or the last event carries a stop_reason such as a runtime-guard risk stop).
# EVERY restart-capable path below must consult this — a guard-stopped runner can be
# inactive OR still process-alive with stale events, and blind revival of either is
# how the 2026-07-04 ARX net blowout compounded.
intended_stop_reason() {
  local gate_done_flag="${APP_DIR}/output/${SYMBOL_SLUG}_target_gate_done_$(date -u +%Y%m%d).flag"
  if [ -f "$gate_done_flag" ]; then
    echo "target_gate_done_today"
    return 0
  fi
  if [ -f "$EVENTS_PATH" ] && tail -n 1 "$EVENTS_PATH" | grep -Eq '"stop_reason": *"[^"]+"'; then
    echo "last_event_stop_reason"
    return 0
  fi
  return 1
}

if ! systemctl is-active --quiet "$SERVICE_NAME"; then
  if [ "${RUNNER_WATCHDOG_START_INACTIVE:-1}" != "1" ]; then
    echo "runner_watchdog: $SERVICE_NAME is not active; start-inactive disabled; skip"
    exit 0
  fi
  if stop_reason="$(intended_stop_reason)"; then
    echo "runner_watchdog: $SERVICE_NAME inactive with intended stop (${stop_reason}); skip start"
    exit 0
  fi
  echo "runner_watchdog: $SERVICE_NAME is not active; start"
  systemctl reset-failed "$SERVICE_NAME" || true
  systemctl start "$SERVICE_NAME"
  exit 0
fi

if [ ! -f "$EVENTS_PATH" ]; then
  if stop_reason="$(intended_stop_reason)"; then
    echo "runner_watchdog: $SERVICE_NAME missing events but intended stop (${stop_reason}); skip restart"
    exit 0
  fi
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

# Stale events do NOT necessarily mean a hang: a runner held in a runtime-guard stop
# keeps the service active but stops writing events, and restarting it would revive
# a risk-stopped runner (and re-latch on the stale plan snapshot).
if stop_reason="$(intended_stop_reason)"; then
  echo "runner_watchdog: $SERVICE_NAME events stale (${age}s) but intended stop (${stop_reason}); skip restart"
  exit 0
fi

echo "runner_watchdog: $SERVICE_NAME stale; events age ${age}s > ${RUNNER_WATCHDOG_STALE_SECONDS}s; restart"
systemctl restart "$SERVICE_NAME"
