from __future__ import annotations

import argparse
import json
import runpy
import sys
from datetime import datetime, timezone
from pathlib import Path

from .recovery_control_ownership import is_recovery_managed


def target_done_marker(*, workdir: Path, symbol: str, now: datetime) -> Path:
    day = now.astimezone(timezone.utc).strftime("%Y%m%d")
    return workdir / "output" / f"{symbol.lower()}_target_gate_done_{day}.flag"


def invalid_state_reason(*, workdir: Path, symbol: str) -> dict[str, str] | None:
    state_path = workdir / "output" / f"{symbol.lower()}_loop_state.json"
    if not state_path.exists():
        return None
    try:
        payload = json.loads(state_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        return {"state_path": str(state_path), "error": str(exc)}
    if not isinstance(payload, dict):
        return {"state_path": str(state_path), "error": "state payload is not an object"}
    return None


def clear_stale_drift_flag_if_reconciled(
    *,
    workdir: Path,
    symbol: str,
    now: datetime,
    max_flag_age_seconds: float = 300.0,
    max_state_age_seconds: float = 120.0,
) -> Path | None:
    flag_path = workdir / "output" / f"{symbol.upper()}_DRIFT_ALERT.flag"
    state_path = workdir / "output" / f"{symbol.lower()}_loop_state.json"
    if not flag_path.exists() or not state_path.exists():
        return None
    try:
        if now.timestamp() - flag_path.stat().st_mtime <= max(float(max_flag_age_seconds), 0.0):
            return None
        payload = json.loads(state_path.read_text(encoding="utf-8"))
        updated_at = datetime.fromisoformat(str(payload.get("updated_at") or ""))
        if updated_at.tzinfo is None:
            updated_at = updated_at.replace(tzinfo=timezone.utc)
        if (now - updated_at.astimezone(timezone.utc)).total_seconds() > max(
            float(max_state_age_seconds), 0.0
        ):
            return None
        reconcile = payload.get("last_reconcile")
        if not isinstance(reconcile, dict):
            return None
        if abs(float(reconcile.get("open_order_diff") or 0.0)) > 10.0:
            return None
        if abs(float(reconcile.get("actual_net_qty_diff") or 0.0)) > 1e-9:
            return None
        if bool(reconcile.get("protective_stop_required")):
            return None
        flag_path.unlink()
    except (OSError, UnicodeError, json.JSONDecodeError, TypeError, ValueError):
        return None
    return flag_path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--workdir", required=True)
    args, _ = parser.parse_known_args(argv)
    workdir = Path(args.workdir).resolve()
    symbol = str(args.symbol).upper().strip()
    marker = target_done_marker(
        workdir=workdir,
        symbol=symbol,
        now=datetime.now(timezone.utc),
    )
    if marker.exists():
        print(json.dumps({"action": "skip_target_gate_done_terminal", "marker": str(marker)}))
        return 0

    invalid_state = invalid_state_reason(workdir=workdir, symbol=symbol)
    if invalid_state is not None:
        print(json.dumps({"action": "skip_invalid_state_safety_gate", **invalid_state}))
        return 0

    control_path = workdir / "output" / f"{symbol.lower()}_loop_runner_control.json"
    try:
        control = json.loads(control_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        control = {}
    if is_recovery_managed(symbol, control if isinstance(control, dict) else {}):
        print(json.dumps({"action": "observe_only_recovery_managed_symbol", "symbol": symbol}))
        return 0


    clear_stale_drift_flag_if_reconciled(
        workdir=workdir,
        symbol=symbol,
        now=datetime.now(timezone.utc),
    )

    legacy = workdir / "output" / "ops" / "bq_liveness_watchdog.py"
    if not legacy.exists():
        raise FileNotFoundError(f"Legacy liveness watchdog not found: {legacy}")
    original_argv = sys.argv
    try:
        sys.argv = [str(legacy), *(argv if argv is not None else original_argv[1:])]
        runpy.run_path(str(legacy), run_name="__main__")
    finally:
        sys.argv = original_argv
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
