from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from deploy.oracle import wear_cost_guard


def _write_guard_inputs(
    app_dir: Path,
    *,
    symbol: str,
    recovery_registered: bool,
) -> Path:
    output_dir = app_dir / "output"
    output_dir.mkdir(parents=True)
    control = {
        "symbol": symbol,
        "per_order_notional": 100.0,
        "max_total_notional": 1_000.0,
        "max_new_orders": 10,
        "sleep_seconds": 0.0,
    }
    if recovery_registered:
        control["_futures_recovery_state"] = {"schema_version": 1}
    control_path = output_dir / f"{symbol.lower()}_loop_runner_control.json"
    control_path.write_text(json.dumps(control), encoding="utf-8")
    (output_dir / f"{symbol.lower()}_loop_events.jsonl").write_text(
        json.dumps(
            {
                "rolling_hourly_loss": 1.0,
                "cumulative_gross_notional": 10_000.0,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    return control_path


def _args(app_dir: Path, symbol: str) -> SimpleNamespace:
    return SimpleNamespace(
        app_dir=str(app_dir),
        runner_wrapper="runner-wrapper",
        symbols=[symbol],
        dry_run=False,
    )


def test_registered_recovery_wear_guard_reports_tier_without_write_or_restart(
    tmp_path: Path, monkeypatch
) -> None:
    control_path = _write_guard_inputs(
        tmp_path,
        symbol="ARXUSDT",
        recovery_registered=True,
    )
    original_control = json.loads(control_path.read_text(encoding="utf-8"))
    restarts: list[str] = []
    monkeypatch.setattr(
        wear_cost_guard,
        "_restart_symbol",
        lambda symbol, **_kwargs: restarts.append(symbol),
    )

    assert wear_cost_guard.run_guard(_args(tmp_path, "ARXUSDT")) == 0

    assert restarts == []
    assert json.loads(control_path.read_text(encoding="utf-8")) == original_control
    events = [
        json.loads(line)
        for line in (tmp_path / "output" / "wear_cost_guard_events.jsonl")
        .read_text(encoding="utf-8")
        .splitlines()
    ]
    assert events[-1]["status"] == "deferred"
    assert events[-1]["reason"] == "futures_recovery_coordinator_registered"
    assert events[-1]["requested_tier"] == "emergency"
    assert events[-1]["restart"] is False


def test_unregistered_wear_guard_keeps_existing_write_and_restart_behavior(
    tmp_path: Path, monkeypatch
) -> None:
    control_path = _write_guard_inputs(
        tmp_path,
        symbol="BZUSDT",
        recovery_registered=False,
    )
    restarts: list[str] = []
    monkeypatch.setattr(
        wear_cost_guard,
        "_restart_symbol",
        lambda symbol, **_kwargs: restarts.append(symbol),
    )

    assert wear_cost_guard.run_guard(_args(tmp_path, "BZUSDT")) == 0

    assert restarts == ["BZUSDT"]
    updated = json.loads(control_path.read_text(encoding="utf-8"))
    assert updated["per_order_notional"] == 40.0
