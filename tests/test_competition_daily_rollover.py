from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

from grid_optimizer.futures_recovery_coordinator import (
    ActionId,
    BaselineChangeStatus,
    EffectStage,
    RecoveryPhase,
)
from grid_optimizer.futures_recovery_store import JsonRecoveryStore
from grid_optimizer.futures_recovery_store import (
    RECOVERY_STATE_KEY,
    RecoveryStateCorruptError,
)
from grid_optimizer.futures_run_lifecycle import bind_run_contract_owner


NOW = datetime(2026, 8, 17, 3, 0, tzinfo=timezone.utc)


def _registered_control(tmp_path: Path) -> Path:
    output = tmp_path / "output"
    output.mkdir()
    path = output / "arxusdt_loop_runner_control.json"
    baseline, _changed = bind_run_contract_owner(
        {
            "symbol": "ARXUSDT",
            "strategy_profile": "volume-long-v4",
            "strategy_mode": "one_way_long",
            "step_price": 0.0005,
            "per_order_notional": 20.0,
            "run_start_time": "2026-08-16T00:00:00+00:00",
            "runtime_guard_stats_start_time": "2026-08-16T00:00:00+00:00",
            "run_end_time": "2026-08-17T00:00:00+00:00",
            "max_cumulative_notional": 20_000.0,
            "terminal_drain_exit_policy": "drain_then_preserve",
            "terminal_drain_absolute_loss_budget": 5.0,
            "terminal_drain_max_wait_seconds": 900.0,
            "volatility_entry_pause_enabled": True,
        },
        activated_at=datetime(2026, 8, 16, tzinfo=timezone.utc),
    )
    path.write_text(json.dumps(baseline), encoding="utf-8")
    JsonRecoveryStore(path).register_symbol("ARXUSDT", baseline, now=NOW)
    return path


def test_registered_daily_roll_submits_durable_request_without_direct_write_or_restart(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    control_path = _registered_control(tmp_path)
    before = JsonRecoveryStore(control_path).read("ARXUSDT")

    from grid_optimizer import competition_daily_rollover as rollover

    monkeypatch.setattr(
        rollover,
        "write_control_json_atomically",
        lambda *_args, **_kwargs: pytest.fail("registered roll directly wrote control"),
    )
    monkeypatch.setattr(
        rollover.subprocess,
        "run",
        lambda *_args, **_kwargs: pytest.fail("registered roll restarted runner"),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "competition_daily_rollover.py",
            "--workdir",
            str(tmp_path),
            "--symbols",
            "ARXUSDT",
            "--now",
            NOW.isoformat(),
        ],
    )

    rollover.main()

    result = json.loads(capsys.readouterr().out)
    fresh = JsonRecoveryStore(control_path).read("ARXUSDT")
    assert result["request_status"] == "deferred"
    assert result["restart_skipped"] is True
    assert fresh.generation == before.generation
    assert fresh.phase is RecoveryPhase.STABLE
    assert fresh.active_action is ActionId.NOOP
    assert fresh.pending_effect_stage is EffectStage.NONE
    assert fresh.desired_profile == before.desired_profile
    assert fresh.baseline_change is not None
    assert fresh.baseline_change.status is BaselineChangeStatus.DEFERRED
    assert fresh.baseline_change.request.expected_baseline_digest == (
        before.baseline_profile.digest
    )
    assert fresh.baseline_change.request.operation_id == (
        "competition-daily-roll:ARXUSDT:2026-08-17T00:00:00+00:00"
    )


def test_unregistered_daily_roll_keeps_legacy_write_and_restart(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    control_path = _registered_control(tmp_path)
    document = json.loads(control_path.read_text(encoding="utf-8"))
    document.pop(RECOVERY_STATE_KEY)
    document.pop("_futures_recovery_state_mirror")
    control_path.write_text(json.dumps(document), encoding="utf-8")

    from grid_optimizer import competition_daily_rollover as rollover

    restarts: list[list[str]] = []

    class Result:
        returncode = 0
        stdout = "restarted"
        stderr = ""

    monkeypatch.setattr(
        rollover.subprocess,
        "run",
        lambda command, **_kwargs: (restarts.append(command) or Result()),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "competition_daily_rollover.py",
            "--workdir",
            str(tmp_path),
            "--symbols",
            "ARXUSDT",
            "--runner-wrapper",
            "/runner-wrapper",
            "--now",
            NOW.isoformat(),
        ],
    )

    rollover.main()

    result = json.loads(capsys.readouterr().out)
    saved = json.loads(control_path.read_text(encoding="utf-8"))
    assert restarts == [["/runner-wrapper", "restart", "ARXUSDT"]]
    assert result["restart_rc"] == 0
    assert saved["run_start_time"] == "2026-08-17T00:00:00+00:00"
    assert saved["run_end_time"] == "2026-08-18T00:00:00+00:00"


def test_corrupt_registered_daily_control_is_byte_preserving(tmp_path: Path) -> None:
    control_path = _registered_control(tmp_path)
    document = json.loads(control_path.read_text(encoding="utf-8"))
    document[RECOVERY_STATE_KEY] = {"schema_version": 1, "state": {}}
    control_path.write_text(json.dumps(document), encoding="utf-8")
    before = control_path.read_bytes()

    from grid_optimizer.competition_daily_rollover import rollover_symbol

    with pytest.raises(RecoveryStateCorruptError):
        rollover_symbol(workdir=tmp_path, symbol="ARXUSDT", now=NOW)

    assert control_path.read_bytes() == before
