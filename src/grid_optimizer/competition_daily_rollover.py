"""Create the next UTC daily futures run contract for its saved runner.

This is deliberately contract-only: it never touches exchange orders, positions,
or frozen inventory.  A prior terminal owner is handed off by the runner after
it verifies the owner is complete. Registered recovery submits a durable baseline
change; the legacy path retains its direct restart contract.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import uuid
from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .futures_recovery_coordinator import (
    BaselineChange,
    FuturesRecoveryCoordinator,
)
from .futures_recovery_store import (
    JsonRecoveryStore,
    recovery_coordinator_registered,
)
from .futures_run_lifecycle import bind_run_contract_owner, validate_run_contract
from .recovery_control_ownership import exclusive_control_lock, write_control_json_atomically


def utc_daily_window(now: datetime) -> tuple[datetime, datetime]:
    current = now.astimezone(timezone.utc)
    start = current.replace(hour=0, minute=0, second=0, microsecond=0)
    return start, start + timedelta(days=1)


def _validate_candidate(candidate: dict[str, Any]) -> None:
    validate_run_contract(
        run_start_time=candidate.get("run_start_time"),
        runtime_guard_stats_start_time=candidate.get("runtime_guard_stats_start_time"),
        run_end_time=candidate.get("run_end_time"),
        target_value=candidate.get("max_cumulative_notional"),
        exit_policy=candidate.get("terminal_drain_exit_policy"),
        loss_budget=candidate.get("terminal_drain_absolute_loss_budget"),
        max_wait_seconds=candidate.get("terminal_drain_max_wait_seconds"),
        preserve_reason=candidate.get("terminal_drain_stop_preserve_reason"),
    )


def _thaw_profile(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _thaw_profile(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_thaw_profile(item) for item in value]
    return value


def _submit_registered_rollover(
    *,
    control_path: Path,
    symbol: str,
    now: datetime,
    start: datetime,
    end: datetime,
) -> dict[str, Any]:
    store = JsonRecoveryStore(control_path)
    state = store.read(symbol)
    candidate = _thaw_profile(state.baseline_profile.fields)
    prior_owner = dict(candidate.get("futures_run_contract_owner") or {})
    prior_id = str(prior_owner.get("run_contract_id") or "")
    candidate.update(
        {
            "run_start_time": start.isoformat(),
            "runtime_guard_stats_start_time": start.isoformat(),
            "run_end_time": end.isoformat(),
        }
    )
    _validate_candidate(candidate)
    prepared, owner_changed = bind_run_contract_owner(
        candidate,
        activated_at=start,
        handoff_reason="daily_utc_window_rollover",
    )
    request = BaselineChange.create(
        symbol=symbol,
        operation_id=f"competition-daily-roll:{symbol}:{start.isoformat()}",
        attempt_id=f"competition-daily-roll-attempt:{uuid.uuid4().hex}",
        source="competition_daily_rollover",
        requested_at=now.astimezone(timezone.utc),
        expected_baseline_digest=state.baseline_profile.digest,
        candidate_baseline=prepared,
    )
    coordinator = FuturesRecoveryCoordinator(
        store=store,
        snapshot_provider=lambda *_args: (_ for _ in ()).throw(
            RuntimeError("daily roll submission cannot collect a recovery snapshot")
        ),
    )
    outcome = coordinator.change_baseline(request)
    return {
        "symbol": symbol,
        "window_start": start.isoformat(),
        "window_end": end.isoformat(),
        "target": prepared.get("max_cumulative_notional"),
        "previous_contract_id": prior_id or None,
        "run_contract_id": (prepared.get("futures_run_contract_owner") or {}).get(
            "run_contract_id"
        ),
        "owner_changed": owner_changed,
        "recovery_coordinator_registered": True,
        "request_status": outcome.status.value,
        "operation_id": outcome.operation_id,
        "attempt_id": outcome.attempt_id,
        "restart_skipped": True,
    }


def rollover_symbol(*, workdir: Path, symbol: str, now: datetime) -> dict[str, Any]:
    normalized = symbol.upper().strip()
    control_path = workdir / "output" / f"{normalized.lower()}_loop_runner_control.json"
    start, end = utc_daily_window(now)
    with exclusive_control_lock(control_path):
        control = json.loads(control_path.read_text(encoding="utf-8"))
        if not isinstance(control, dict):
            raise ValueError(f"{normalized}: control is not an object")
        registered = recovery_coordinator_registered(control)
        if not registered:
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
    if registered:
        return _submit_registered_rollover(
            control_path=control_path,
            symbol=normalized,
            now=now,
            start=start,
            end=end,
        )
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
        if record.get("recovery_coordinator_registered"):
            print(json.dumps(record, ensure_ascii=False, sort_keys=True))
            continue
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
