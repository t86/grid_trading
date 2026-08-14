"""Create the next UTC daily futures run contract and restart its saved runner.

This is deliberately contract-only: it never touches exchange orders, positions,
or frozen inventory.  A prior terminal owner is handed off by the runner after
it verifies the owner is complete.
"""

from __future__ import annotations

import argparse
import json
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .futures_run_lifecycle import bind_run_contract_owner
from .recovery_control_ownership import exclusive_control_lock, write_control_json_atomically


def utc_daily_window(now: datetime) -> tuple[datetime, datetime]:
    current = now.astimezone(timezone.utc)
    start = current.replace(hour=0, minute=0, second=0, microsecond=0)
    return start, start + timedelta(days=1)


def rollover_symbol(*, workdir: Path, symbol: str, now: datetime) -> dict[str, Any]:
    normalized = symbol.upper().strip()
    control_path = workdir / "output" / f"{normalized.lower()}_loop_runner_control.json"
    start, end = utc_daily_window(now)
    with exclusive_control_lock(control_path):
        control = json.loads(control_path.read_text(encoding="utf-8"))
        if not isinstance(control, dict):
            raise ValueError(f"{normalized}: control is not an object")
        prior_owner = dict(control.get("futures_run_contract_owner") or {})
        prior_id = str(prior_owner.get("run_contract_id") or "")
        control.update(
            {
                "run_start_time": start.isoformat(),
                "runtime_guard_stats_start_time": start.isoformat(),
                "run_end_time": end.isoformat(),
            }
        )
        prepared, changed = bind_run_contract_owner(
            control,
            activated_at=now.astimezone(timezone.utc),
            handoff_reason="daily_utc_window_rollover",
        )
        write_control_json_atomically(control_path, prepared)
    return {
        "symbol": normalized,
        "window_start": start.isoformat(),
        "window_end": end.isoformat(),
        "target": prepared.get("max_cumulative_notional"),
        "previous_contract_id": prior_id or None,
        "run_contract_id": (prepared.get("futures_run_contract_owner") or {}).get("run_contract_id"),
        "owner_changed": changed,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workdir", required=True)
    parser.add_argument("--symbols", required=True, help="comma-separated futures symbols")
    parser.add_argument("--runner-wrapper", default="/usr/local/bin/grid-saved-runner")
    parser.add_argument("--now", help="ISO timestamp for a controlled run/test")
    args = parser.parse_args()
    now = (
        datetime.fromisoformat(args.now.replace("Z", "+00:00"))
        if args.now
        else datetime.now(timezone.utc)
    )
    if now.tzinfo is None:
        raise SystemExit("--now must include a timezone")
    for raw_symbol in args.symbols.split(","):
        symbol = raw_symbol.upper().strip()
        if not symbol:
            continue
        record = rollover_symbol(workdir=Path(args.workdir), symbol=symbol, now=now)
        restart = subprocess.run(
            [args.runner_wrapper, "restart", symbol],
            text=True,
            capture_output=True,
            check=False,
        )
        record["restart_rc"] = restart.returncode
        record["restart_output"] = (restart.stdout or restart.stderr).strip()[-500:]
        print(json.dumps(record, ensure_ascii=False, sort_keys=True))
        if restart.returncode:
            raise SystemExit(restart.returncode)


if __name__ == "__main__":
    main()
