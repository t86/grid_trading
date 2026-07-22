#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: configure_recovery_managed_runner.sh <enable|retire|disable|verify> <SYMBOL>

Enable writes a per-symbol systemd drop-in with Restart=no.  It refuses to
  operate unless the control document is already registered to the futures
  recovery coordinator, the coordinator timer is active, and a successful
  coordinator heartbeat for this symbol is fresh. Disable refuses to remove the
  drop-in while that registration remains, so ownership cannot fall back to
  systemd by accident.
EOF
}

ACTION="${1:-}"
SYMBOL="$(printf '%s' "${2:-}" | tr '[:lower:]' '[:upper:]')"
if [[ "$ACTION" != "enable" && "$ACTION" != "retire" && "$ACTION" != "disable" && "$ACTION" != "verify" ]] || [[ ! "$SYMBOL" =~ ^[A-Z0-9]{3,20}$ ]]; then
  usage >&2
  exit 2
fi

APP_DIR="${APP_DIR:-$PWD}"
PYTHON_BIN="${PYTHON_BIN:-${APP_DIR}/.venv/bin/python}"
RUNNER_SRC_DIR="${RUNNER_SRC_DIR:-${APP_DIR}/src}"
OUTPUT_DIR="${OUTPUT_DIR:-${APP_DIR}/output}"
COORDINATOR_TIMER_UNIT="${COORDINATOR_TIMER_UNIT:-grid-bq-volume-recovery-guard}"
COORDINATOR_WATCHDOG_TIMER_UNIT="${COORDINATOR_WATCHDOG_TIMER_UNIT:-${COORDINATOR_TIMER_UNIT}-watchdog}"
COORDINATOR_STATE_PATH="${COORDINATOR_STATE_PATH:-${OUTPUT_DIR}/bq_volume_recovery_guard_state.json}"
COORDINATOR_WATCHDOG_STATE_PATH="${COORDINATOR_WATCHDOG_STATE_PATH:-${OUTPUT_DIR}/recovery_coordinator_watchdog_state.json}"
COORDINATOR_ALERT_CONFIG_PATH="${COORDINATOR_ALERT_CONFIG_PATH:-${OUTPUT_DIR}/alert_notifier_config.json}"
COORDINATOR_HEARTBEAT_MAX_AGE_SECONDS="${COORDINATOR_HEARTBEAT_MAX_AGE_SECONDS:-150}"
RUNNER_SERVICE_TEMPLATE="${RUNNER_SERVICE_TEMPLATE:-}"
if [[ -z "$RUNNER_SERVICE_TEMPLATE" ]]; then
  RUNNER_SERVICE_TEMPLATE='grid-loop@{symbol}.service'
fi
SYMBOL_LOWER="$(printf '%s' "$SYMBOL" | tr '[:upper:]' '[:lower:]')"
CONTROL_PATH="${CONTROL_PATH:-${OUTPUT_DIR}/${SYMBOL_LOWER}_loop_runner_control.json}"
SERVICE_NAME="$(printf '%s' "$RUNNER_SERVICE_TEMPLATE" | sed \
  -e "s/{symbol}/${SYMBOL}/g" \
  -e "s/{symbol_upper}/${SYMBOL}/g" \
  -e "s/{symbol_lower}/${SYMBOL_LOWER}/g")"
SYSTEMD_DIR="${SYSTEMD_DIR:-/etc/systemd/system}"
DROPIN_DIR="${SYSTEMD_DIR}/${SERVICE_NAME}.d"
DROPIN_PATH="${DROPIN_DIR}/90-futures-recovery-managed.conf"

if [[ ! -x "$PYTHON_BIN" ]]; then
  echo "python binary is not executable: $PYTHON_BIN" >&2
  exit 2
fi
if [[ ! -r "$CONTROL_PATH" ]]; then
  echo "control document is not readable: $CONTROL_PATH" >&2
  exit 2
fi

registered_control() {
  "$PYTHON_BIN" - "$SYMBOL" "$CONTROL_PATH" "$RUNNER_SRC_DIR" <<'PY'
import json
import sys

symbol, control_path, src_dir = sys.argv[1:]
sys.path.insert(0, src_dir)
from grid_optimizer.futures_recovery_store import (
    RecoveryStateStoreError,
    decode_recovery_control_state,
    recovery_coordinator_registered,
)

try:
    with open(control_path, encoding="utf-8") as handle:
        control = json.load(handle)
    if not isinstance(control, dict):
        raise ValueError("control must be a JSON object")
    if not recovery_coordinator_registered(control):
        raise SystemExit(1)
    decode_recovery_control_state(control, expected_symbol=symbol)
except SystemExit:
    raise
except (OSError, TypeError, ValueError, RecoveryStateStoreError) as exc:
    print(f"invalid registered recovery control: {exc}", file=sys.stderr)
    raise SystemExit(2)
PY
}

require_coordinator_timer() {
  if ! systemctl is-active --quiet "${COORDINATOR_TIMER_UNIT}.timer"; then
    echo "recovery coordinator timer is not active: ${COORDINATOR_TIMER_UNIT}.timer" >&2
    exit 1
  fi
}

require_coordinator_watchdog_timer() {
  if ! systemctl is-active --quiet "${COORDINATOR_WATCHDOG_TIMER_UNIT}.timer"; then
    echo "recovery coordinator watchdog timer is not active: ${COORDINATOR_WATCHDOG_TIMER_UNIT}.timer" >&2
    exit 1
  fi
}

require_coordinator_watchdog_heartbeat() {
  "$PYTHON_BIN" - "$SYMBOL" "$COORDINATOR_WATCHDOG_STATE_PATH" "$COORDINATOR_HEARTBEAT_MAX_AGE_SECONDS" <<'PY'
import json
import sys
from datetime import datetime, timezone

symbol, state_path, max_age_text = sys.argv[1:]
try:
    max_age = float(max_age_text)
    if max_age <= 0:
        raise ValueError("maximum age must be positive")
    with open(state_path, encoding="utf-8") as handle:
        state = json.load(handle)
    heartbeat = state["recovery_coordinator_watchdog_heartbeat"]
    if heartbeat.get("schema") != "recovery_coordinator_watchdog_heartbeat_v1":
        raise ValueError("unexpected watchdog heartbeat schema")
    checked_at = datetime.fromisoformat(str(heartbeat["checked_at"]).replace("Z", "+00:00"))
    if checked_at.tzinfo is None:
        raise ValueError("watchdog heartbeat timestamp has no timezone")
    age = (datetime.now(timezone.utc) - checked_at.astimezone(timezone.utc)).total_seconds()
    if age < -60 or age > max_age:
        raise ValueError(f"watchdog heartbeat age {age:.1f}s is outside 0..{max_age:.1f}s")
    healthy = heartbeat.get("symbols", {}).get(symbol)
    if heartbeat.get("ok") is not True or healthy is not True:
        raise ValueError("watchdog does not confirm a healthy observation for symbol")
except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
    print(f"recovery coordinator watchdog heartbeat is not fresh for {symbol}: {exc}", file=sys.stderr)
    raise SystemExit(1)
PY
}

require_coordinator_alert_delivery() {
  "$PYTHON_BIN" - "$COORDINATOR_ALERT_CONFIG_PATH" "$RUNNER_SRC_DIR" <<'PY'
import sys

config_path, src_dir = sys.argv[1:]
sys.path.insert(0, src_dir)
from grid_optimizer.notifications import load_alert_notifier_config

config = load_alert_notifier_config(config_path)
if not config.get("enabled"):
    print(f"recovery coordinator alert delivery is not configured: {config_path}", file=sys.stderr)
    raise SystemExit(1)
PY
}

require_coordinator_heartbeat() {
  "$PYTHON_BIN" - "$SYMBOL" "$COORDINATOR_STATE_PATH" "$COORDINATOR_HEARTBEAT_MAX_AGE_SECONDS" <<'PY'
import json
import sys
from datetime import datetime, timezone

symbol, state_path, max_age_text = sys.argv[1:]
try:
    max_age = float(max_age_text)
    if max_age <= 0:
        raise ValueError("maximum age must be positive")
    with open(state_path, encoding="utf-8") as handle:
        state = json.load(handle)
    heartbeat = state["futures_recovery_guard_heartbeat"]
    if heartbeat.get("schema") != "futures_recovery_guard_heartbeat_v1":
        raise ValueError("unexpected heartbeat schema")
    checked_at = datetime.fromisoformat(str(heartbeat["checked_at"]).replace("Z", "+00:00"))
    if checked_at.tzinfo is None:
        raise ValueError("heartbeat timestamp has no timezone")
    age = (datetime.now(timezone.utc) - checked_at.astimezone(timezone.utc)).total_seconds()
    if age < -60 or age > max_age:
        raise ValueError(f"heartbeat age {age:.1f}s is outside 0..{max_age:.1f}s")
    symbol_status = heartbeat.get("symbols", {}).get(symbol)
    if heartbeat.get("ok") is not True or not isinstance(symbol_status, dict) or symbol_status.get("healthy") is not True:
        raise ValueError("heartbeat does not confirm a successful coordinator round for symbol")
except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
    print(f"recovery coordinator heartbeat is not fresh for {symbol}: {exc}", file=sys.stderr)
    raise SystemExit(1)
PY
}

restart_policy() {
  systemctl show -p Restart --value "$SERVICE_NAME" | tr -d '[:space:]'
}

require_runner_inactive_without_terminal_artifacts() {
  if systemctl is-active --quiet "$SERVICE_NAME"; then
    echo "runner must be inactive before retiring recovery ownership: $SERVICE_NAME" >&2
    exit 1
  fi
  if [[ -e "${OUTPUT_DIR}/${SYMBOL_LOWER}_terminal_intent.json" ]]; then
    echo "terminal intent must be absent before retiring recovery ownership" >&2
    exit 1
  fi
  if "$PYTHON_BIN" - "${OUTPUT_DIR}/${SYMBOL_LOWER}_loop_state.json" <<'PY'
import json
import sys

path = sys.argv[1]
try:
    with open(path, encoding="utf-8") as handle:
        state = json.load(handle)
except FileNotFoundError:
    raise SystemExit(0)
except (OSError, json.JSONDecodeError):
    raise SystemExit(1)
raise SystemExit(1 if isinstance(state, dict) and isinstance(state.get("futures_terminal_drain"), dict) else 0)
PY
  then
    :
  else
    echo "terminal drain owner must be absent before retiring recovery ownership" >&2
    exit 1
  fi
}

retire_control() {
  "$PYTHON_BIN" - "$SYMBOL" "$CONTROL_PATH" "$RUNNER_SRC_DIR" <<'PY'
import sys

symbol, control_path, src_dir = sys.argv[1:]
sys.path.insert(0, src_dir)
from grid_optimizer.futures_recovery_store import JsonRecoveryStore, RecoveryStateStoreError

try:
    JsonRecoveryStore(control_path).retire_symbol(symbol)
except (ValueError, RecoveryStateStoreError) as exc:
    print(f"cannot retire recovery ownership: {exc}", file=sys.stderr)
    raise SystemExit(1)
PY
}

case "$ACTION" in
  verify)
    if ! registered_control; then
      echo "recovery registration is missing or invalid for $SYMBOL" >&2
      exit 1
    fi
    require_coordinator_timer
    require_coordinator_watchdog_timer
    require_coordinator_heartbeat
    require_coordinator_watchdog_heartbeat
    require_coordinator_alert_delivery
    if [[ "$(restart_policy)" != "no" ]]; then
      echo "recovery-managed runner must use Restart=no: $SERVICE_NAME" >&2
      exit 1
    fi
    echo "recovery-managed runner verified: $SYMBOL"
    ;;
  enable)
    if ! registered_control; then
      echo "recovery registration is missing or invalid for $SYMBOL" >&2
      exit 1
    fi
    require_coordinator_timer
    require_coordinator_watchdog_timer
    require_coordinator_heartbeat
    require_coordinator_watchdog_heartbeat
    require_coordinator_alert_delivery
    temporary_path="$(mktemp)"
    trap 'rm -f "$temporary_path"' EXIT
    printf '[Service]\nRestart=no\n' >"$temporary_path"
    sudo install -d -m 0755 "$DROPIN_DIR"
    sudo install -m 0644 "$temporary_path" "$DROPIN_PATH"
    sudo systemctl daemon-reload
    if [[ "$(restart_policy)" != "no" ]]; then
      sudo rm -f "$DROPIN_PATH"
      sudo systemctl daemon-reload
      echo "failed to apply Restart=no for $SERVICE_NAME; reverted drop-in" >&2
      exit 1
    fi
    echo "recovery-managed runner enabled: $SYMBOL"
    ;;
  retire)
    if ! registered_control; then
      echo "recovery registration is missing or invalid for $SYMBOL" >&2
      exit 1
    fi
    require_runner_inactive_without_terminal_artifacts
    retire_control
    echo "recovery coordinator ownership retired: $SYMBOL"
    ;;
  disable)
    if registered_control; then
      echo "registered control must be retired before disabling recovery-managed restart policy" >&2
      exit 1
    else
      registration_status=$?
    fi
    if [[ "$registration_status" -ne 1 ]]; then
      echo "cannot disable while recovery control is invalid" >&2
      exit 1
    fi
    require_runner_inactive_without_terminal_artifacts
    sudo rm -f "$DROPIN_PATH"
    sudo rmdir "$DROPIN_DIR" 2>/dev/null || true
    sudo systemctl daemon-reload
    echo "recovery-managed runner policy removed: $SYMBOL"
    ;;
esac
