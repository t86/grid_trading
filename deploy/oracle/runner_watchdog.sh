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
SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
RUNNER_SRC_DIR="$(dirname "$(dirname "$SCRIPT_DIR")")/src"
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

has_recovery_state_registration() {
  python3 - "$CONTROL_PATH" <<'PY'
import json
import sys

try:
    with open(sys.argv[1], encoding="utf-8") as handle:
        control = json.load(handle)
except (OSError, ValueError):
    control = None
registered = isinstance(control, dict) and (
    "_futures_recovery_state" in control
    or "_futures_recovery_state_mirror" in control
)
raise SystemExit(0 if registered else 1)
PY
}

# Coordinator registration is a presence-based ownership fence for ordinary
# watchdog actions.  It does not suppress a strictly validated active terminal
# owner: that owner must be resumed or the symbol can remain locked forever.
RECOVERY_REGISTERED=0
if has_recovery_state_registration; then
  RECOVERY_REGISTERED=1
fi

recovery_state_slots_consistent() {
  python3 - "$SYMBOL" "$CONTROL_PATH" "$RUNNER_SRC_DIR" <<'PY'
import json
import sys

symbol, control_path, src_dir = sys.argv[1:]
sys.path.insert(0, src_dir)
from grid_optimizer.futures_recovery_store import (
    RecoveryStateStoreError,
    decode_recovery_control_state,
)

try:
    with open(control_path, encoding="utf-8") as handle:
        control = json.load(handle)
    if not isinstance(control, dict):
        raise ValueError("control must be a JSON object")
    decode_recovery_control_state(control, expected_symbol=symbol)
except (OSError, TypeError, ValueError, RecoveryStateStoreError) as exc:
    print(f"recovery state mirror repair required: {exc}", file=sys.stderr)
    raise SystemExit(1)
PY
}

# A single valid slot is enough for the coordinator to repair ownership, but it
# is not authority for the watchdog to replay a terminal start/restart in the
# same cycle.  The guard repairs locally first; the next watchdog cycle may act.
if [ "$RECOVERY_REGISTERED" = "1" ] && ! recovery_state_slots_consistent; then
  echo "runner_watchdog: $SYMBOL recovery state repair pending; fail closed" >&2
  exit 1
fi

# Classify the durable terminal owner before every other actuator gate.  An
# active intent is recovery work, not an intended stop: if its runner dies the
# watchdog must resume the same frozen contract or the symbol can lock forever.
TERMINAL_INTENT_PATH="${APP_DIR}/output/${SYMBOL_SLUG}_terminal_intent.json"
classify_terminal_intent() {
  python3 - "$SYMBOL" "$TERMINAL_INTENT_PATH" "$CONTROL_PATH" "$APP_DIR" "$RUNNER_SRC_DIR" <<'PY'
import json
import os
import sys

symbol, intent_path, control_path, app_dir, src_dir = sys.argv[1:]
sys.path.insert(0, src_dir)
from grid_optimizer.futures_run_lifecycle import (
    resolve_authoritative_run_contract,
    run_contract_identity_from_config,
    run_contract_snapshot_from_config,
)
from grid_optimizer.futures_terminal_ownership import (
    TERMINAL_INTENT_ACTIVE_STATUSES,
    TERMINAL_INTENT_COMPLETED_STATUSES,
    terminal_drain_runtime_owner_is_integral,
    validate_terminal_intent,
)

try:
    with open(control_path, encoding="utf-8") as handle:
        control = json.load(handle)
    if not isinstance(control, dict):
        raise ValueError("control must be a JSON object")
    _current_snapshot, current_contract_id = resolve_authoritative_run_contract(
        control,
        expected_symbol=symbol,
    )
except (OSError, TypeError, ValueError) as exc:
    print(f"invalid current run contract: {exc}", file=sys.stderr)
    raise SystemExit(2)

active_statuses = TERMINAL_INTENT_ACTIVE_STATUSES
completed_statuses = TERMINAL_INTENT_COMPLETED_STATUSES
owner_states = []
handoff_pending = False

def verify_snapshot(owner, *, label):
    snapshot = owner.get("run_contract_snapshot")
    owner_contract_id = str(owner.get("run_contract_id") or "").strip()
    try:
        canonical = run_contract_snapshot_from_config(snapshot)
        snapshot_contract_id = run_contract_identity_from_config(canonical)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"invalid {label} snapshot: {exc}") from exc
    if canonical != snapshot or snapshot_contract_id != owner_contract_id:
        raise ValueError(f"{label} snapshot digest mismatch")
    if canonical.get("symbol") != symbol:
        raise ValueError(f"{label} snapshot symbol mismatch")
    return owner_contract_id

if os.path.exists(intent_path):
    try:
        with open(intent_path, encoding="utf-8") as handle:
            intent = json.load(handle)
        validated_intent = validate_terminal_intent(
            intent,
            expected_symbol=symbol,
        )
        intent_contract_id = validated_intent.run_contract_id
        status = validated_intent.status
        if status in active_statuses:
            owner_states.append(("active", intent_contract_id))
        elif status in completed_statuses:
            owner_states.append(("completed", intent_contract_id))
        else:
            raise ValueError("invalid terminal intent status")
    except (OSError, TypeError, ValueError) as exc:
        print(str(exc), file=sys.stderr)
        raise SystemExit(2)

try:
    configured_state_path = str(control.get("state_path") or "").strip()
    if configured_state_path:
        state_path = (
            configured_state_path
            if os.path.isabs(configured_state_path)
            else os.path.join(app_dir, configured_state_path)
        )
    else:
        state_path = os.path.join(
            app_dir,
            "output",
            f"{symbol.lower()}_loop_state.json",
        )
    if os.path.exists(state_path):
        with open(state_path, encoding="utf-8") as handle:
            state = json.load(handle)
        if not isinstance(state, dict):
            raise ValueError("loop state must be a JSON object")
        runtime_owner = state.get("futures_terminal_drain")
        if runtime_owner is not None:
            if not (
                isinstance(runtime_owner, dict)
                and runtime_owner.get("schema") == "futures_terminal_drain_runtime_v1"
            ):
                raise ValueError("invalid terminal runtime owner envelope")
            runtime_contract_id = verify_snapshot(
                runtime_owner,
                label="terminal runtime owner",
            )
            expected_decision_id = f"{symbol}|{runtime_contract_id}"
            if runtime_owner.get("decision_id") != expected_decision_id:
                raise ValueError("terminal runtime owner decision mismatch")
            if not terminal_drain_runtime_owner_is_integral(
                runtime_owner,
                symbol=symbol,
                loop_state=state,
            ):
                raise ValueError("terminal runtime owner integrity mismatch")
            exit_status = str(runtime_owner.get("exit_status") or "exiting")
            if exit_status in {"exiting", "exit_blocked"}:
                owner_states.append(("active", runtime_contract_id))
            elif exit_status in {"stopped_clean", "stopped_preserved"}:
                owner_states.append(("completed", runtime_contract_id))
            else:
                raise ValueError("invalid terminal runtime owner status")
        handoff = state.get("futures_terminal_handoff")
        if handoff is not None:
            if not (
                isinstance(handoff, dict)
                and handoff.get("schema") == "futures_terminal_handoff_v1"
                and handoff.get("symbol") == symbol
            ):
                raise ValueError("invalid terminal handoff envelope")
            handoff_status = str(handoff.get("status") or "")
            if handoff_status == "pending":
                source_contract_id = verify_snapshot(
                    handoff,
                    label="terminal handoff source",
                )
                if handoff.get("from_decision_id") != f"{symbol}|{source_contract_id}":
                    raise ValueError("terminal handoff source decision mismatch")
                if handoff.get("to_decision_id") != f"{symbol}|{current_contract_id}":
                    raise ValueError("terminal handoff target mismatch")
                handoff_pending = True
            elif handoff_status == "acknowledged":
                if not str(handoff.get("acknowledged_at") or "").strip():
                    raise ValueError("terminal handoff acknowledgement is missing")
            else:
                raise ValueError("invalid terminal handoff status")
except (OSError, TypeError, ValueError) as exc:
    print(str(exc), file=sys.stderr)
    raise SystemExit(2)

# An incomplete owner always wins, even if a stale completed receipt also
# exists.  It must be resumed until it reaches an explicit terminal outcome.
if any(state == "active" for state, _ in owner_states):
    print("active")
elif any(
    state == "completed" and contract_id == current_contract_id
    for state, contract_id in owner_states
):
    print("completed_current")
elif handoff_pending:
    print("handoff_pending")
elif owner_states:
    print("completed_old")
else:
    print("none")
PY
}

if ! TERMINAL_STATE="$(classify_terminal_intent)"; then
  echo "runner_watchdog: $SYMBOL terminal intent is invalid; fail closed" >&2
  exit 1
fi

is_recovery_managed() {
  python3 - "$SYMBOL" "$CONTROL_PATH" <<'PY'
import json
import sys

symbol, control_path = sys.argv[1:]
try:
    with open(control_path, encoding="utf-8") as handle:
        control = json.load(handle)
except (OSError, ValueError):
    control = {}
managed = isinstance(control, dict) and (
    symbol in {"ARXUSDT", "OUSDT"}
    or "_futures_recovery_state" in control
    or "_futures_recovery_state_mirror" in control
    or control.get("recovery_control_owner") == "bq_volume_recovery_guard"
)
raise SystemExit(0 if managed else 1)
PY
}

RECOVERY_MANAGED="$RECOVERY_REGISTERED"
if [ "$RECOVERY_MANAGED" = "0" ] && is_recovery_managed; then
  RECOVERY_MANAGED=1
fi

# Echoes a reason and returns 0 when the current runner's stop is INTENDED.
# EVERY restart-capable path below must consult this — a guard-stopped runner can be
# inactive OR still process-alive with stale events, and blind revival of either is
# how the 2026-07-04 ARX net blowout compounded.
intended_stop_reason() {
  if [ "$TERMINAL_STATE" = "completed_current" ]; then
    echo "terminal_lifecycle_intent"
    return 0
  fi
  if [ -f "$EVENTS_PATH" ] && tail -n 1 "$EVENTS_PATH" | grep -Eq '"stop_reason": *"[^"]+"'; then
    echo "last_event_stop_reason"
    return 0
  fi
  return 1
}

if ! systemctl is-active --quiet "$SERVICE_NAME"; then
  if [ "$TERMINAL_STATE" = "active" ]; then
    echo "runner_watchdog: $SERVICE_NAME inactive with active terminal owner; start/resume"
    systemctl reset-failed "$SERVICE_NAME" || true
    systemctl start "$SERVICE_NAME"
    exit 0
  fi
  if [ "$TERMINAL_STATE" = "handoff_pending" ]; then
    echo "runner_watchdog: $SERVICE_NAME inactive with terminal handoff pending; start/resume"
    systemctl reset-failed "$SERVICE_NAME" || true
    systemctl start "$SERVICE_NAME"
    exit 0
  fi
  if [ "$TERMINAL_STATE" = "completed_current" ]; then
    echo "runner_watchdog: $SERVICE_NAME inactive with intended stop (terminal_lifecycle_intent); skip start"
    exit 0
  fi
  if [ "$RECOVERY_MANAGED" = "1" ]; then
    echo "runner_watchdog: $SYMBOL is recovery-managed; skip actuator"
    exit 0
  fi
  if [ "${RUNNER_WATCHDOG_START_INACTIVE:-1}" != "1" ]; then
    echo "runner_watchdog: $SERVICE_NAME is not active; start-inactive disabled; skip"
    exit 0
  fi
  if [ "$TERMINAL_STATE" != "completed_old" ] && stop_reason="$(intended_stop_reason)"; then
    echo "runner_watchdog: $SERVICE_NAME inactive with intended stop (${stop_reason}); skip start"
    exit 0
  fi
  echo "runner_watchdog: $SERVICE_NAME is not active; start"
  systemctl reset-failed "$SERVICE_NAME" || true
  systemctl start "$SERVICE_NAME"
  exit 0
fi

if [ "$RECOVERY_MANAGED" = "1" ] && [ "$TERMINAL_STATE" != "active" ] && [ "$TERMINAL_STATE" != "handoff_pending" ]; then
  echo "runner_watchdog: $SYMBOL is recovery-managed; skip actuator"
  exit 0
fi

if [ "$TERMINAL_STATE" = "completed_current" ]; then
  echo "runner_watchdog: $SERVICE_NAME has completed current terminal owner; skip actuator"
  exit 0
fi

if [ ! -f "$EVENTS_PATH" ]; then
  if [ "$TERMINAL_STATE" = "active" ]; then
    echo "runner_watchdog: $SERVICE_NAME active terminal owner missing events; restart/resume"
    systemctl restart "$SERVICE_NAME"
    exit 0
  fi
  if [ "$TERMINAL_STATE" = "handoff_pending" ]; then
    echo "runner_watchdog: $SERVICE_NAME terminal handoff pending and events missing; restart/resume"
    systemctl restart "$SERVICE_NAME"
    exit 0
  fi
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

if [ "$TERMINAL_STATE" = "active" ]; then
  echo "runner_watchdog: $SERVICE_NAME active terminal owner stale; restart/resume"
  systemctl restart "$SERVICE_NAME"
  exit 0
fi

if [ "$TERMINAL_STATE" = "handoff_pending" ]; then
  echo "runner_watchdog: $SERVICE_NAME terminal handoff pending and events stale; restart/resume"
  systemctl restart "$SERVICE_NAME"
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
