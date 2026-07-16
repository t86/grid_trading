from dataclasses import replace
from datetime import datetime, timedelta
import stat
import sys
from zoneinfo import ZoneInfo

from deploy.oracle.roll_competition_window import (
    apply_target_notional,
    bind_rolled_run_contract_owner,
    clear_recovery_overlay,
    load_usable_control,
    reset_runtime_guard_baseline,
    roll_control_window,
    validate_rolled_control_contract,
    write_json_atomically,
)
from grid_optimizer.futures_run_lifecycle import (
    RUN_CONTRACT_OWNER_KEY,
    bind_run_contract_owner,
)
from grid_optimizer.futures_recovery_coordinator import (
    ActionId,
    ActionMode,
    EffectStage,
    FlowBlockerAssessment,
    FuturesRecoveryDecisionEngine,
    RecoveryPhase,
    SymbolSnapshot,
)
from grid_optimizer.futures_recovery_store import (
    RECOVERY_STATE_MIRROR_KEY,
    RECOVERY_STATE_KEY,
    JsonRecoveryStore,
)
from pathlib import Path
from tempfile import TemporaryDirectory
import json

import pytest


BEIJING = ZoneInfo("Asia/Shanghai")


def _write_registered_roll_fixture(
    temp_dir: Path,
    *,
    phase: RecoveryPhase = RecoveryPhase.STABLE,
    extra_managed_baseline: dict[str, object] | None = None,
) -> tuple[Path, Path, Path, Path]:
    control_path = temp_dir / "arxusdt_loop_runner_control.json"
    runtime_profile_path = temp_dir / "arx-runtime-profile.json"
    guard_state_path = temp_dir / "bq-volume-recovery-state.json"
    loop_state_path = temp_dir / "arxusdt_loop_state.json"
    control, _owner_changed = bind_run_contract_owner(
        {
            "symbol": "ARXUSDT",
            "strategy_profile": "arx-volume-v2",
            "strategy_mode": "hedge_best_quote_maker_volume_v1",
            "step_price": 0.0005,
            "max_actual_net_notional": 1_000.0,
            "run_start_time": "2026-07-14T08:00:00+08:00",
            "runtime_guard_stats_start_time": "2026-07-14T08:00:00+08:00",
            "run_end_time": "2026-07-15T08:00:00+08:00",
            "max_cumulative_notional": 20_000.0,
            "per_order_notional": 20.0,
            "terminal_drain_exit_policy": "drain_then_preserve",
            "terminal_drain_absolute_loss_budget": 5.0,
            "terminal_drain_max_wait_seconds": 900.0,
        },
        activated_at=datetime(2026, 7, 14, 8, 0, tzinfo=BEIJING),
    )
    control_path.write_text(json.dumps(control), encoding="utf-8")
    store = JsonRecoveryStore(control_path)
    baseline: dict[str, object] = {
        "step_price": 0.0005,
        "max_actual_net_notional": 1_000.0,
        "best_quote_maker_volume_cycle_budget_notional": 360.0,
        "pause_buy_position_notional": 400.0,
        "pause_short_position_notional": 400.0,
        "volatility_entry_pause_enabled": True,
    }
    baseline.update(extra_managed_baseline or {})
    registered = store.register_symbol(
        "ARXUSDT",
        baseline,
        now=datetime(2026, 7, 14, 8, 0, tzinfo=BEIJING),
    )
    if phase is RecoveryPhase.COOLDOWN:
        store.compare_and_swap(
            "ARXUSDT",
            expected_revision=0,
            next_state=replace(
                registered,
                document_revision=1,
                generation=1,
                phase=RecoveryPhase.COOLDOWN,
                cooldown_until=datetime(2026, 7, 16, 8, 5, tzinfo=BEIJING),
            ),
        )
    runtime_profile_path.write_text(
        json.dumps(
            {
                "step_price": 0.0008,
                "max_actual_net_notional": 2_000.0,
                "best_quote_maker_volume_cycle_budget_notional": 720.0,
                "pause_buy_position_notional": 800.0,
                "pause_short_position_notional": 800.0,
                "best_quote_maker_volume_allow_loss_reduce_only": True,
                "best_quote_maker_volume_net_loss_reduce_enabled": True,
                "hard_loss_forced_reduce_enabled": True,
            }
        ),
        encoding="utf-8",
    )
    guard_state_path.write_text(
        json.dumps(
            {
                "symbols": {
                    "ARXUSDT": {
                        "recovery_owned": True,
                        "guard_original_controls": {"step_price": 0.0005},
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    loop_state_path.write_text(
        json.dumps(
            {
                "runtime_guard_loss_recovery": {"stopped_at": "2026-07-16T07:59:00+08:00"},
                "best_quote_frozen_inventory": {"long_qty": 25.0},
            }
        ),
        encoding="utf-8",
    )
    return control_path, runtime_profile_path, guard_state_path, loop_state_path


def _registered_roll_argv(
    *,
    control_path: Path,
    runtime_profile_path: Path,
    guard_state_path: Path,
    loop_state_path: Path,
) -> list[str]:
    return [
        "roll_competition_window.py",
        "--control-path",
        str(control_path),
        "--runtime-profile",
        str(runtime_profile_path),
        "--guard-state-path",
        str(guard_state_path),
        "--loop-state-path",
        str(loop_state_path),
        "--symbol",
        "ARXUSDT",
        "--force-profile-rebase",
        "--target-notional",
        "40_000",
        "--reset-runtime-guard-baseline",
        "--run-contract-handoff-reason",
        "daily_competition_window_rollover",
    ]


def _invoke_registered_roll_deferred(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    *,
    control_path: Path,
    runtime_profile_path: Path,
    guard_state_path: Path,
    loop_state_path: Path,
) -> dict[str, object]:
    paths = (control_path, guard_state_path, loop_state_path)
    before = {path: path.read_bytes() for path in paths}
    monkeypatch.setattr(
        sys,
        "argv",
        _registered_roll_argv(
            control_path=control_path,
            runtime_profile_path=runtime_profile_path,
            guard_state_path=guard_state_path,
            loop_state_path=loop_state_path,
        ),
    )

    with pytest.raises(SystemExit) as raised:
        from deploy.oracle import roll_competition_window

        roll_competition_window.main()

    assert raised.value.code == 3
    for path in paths:
        assert path.read_bytes() == before[path]
    return json.loads(capsys.readouterr().out)


def test_atomic_control_write_keeps_runner_read_permission() -> None:
    with TemporaryDirectory() as temp_dir:
        path = Path(temp_dir) / "arxusdt_loop_runner_control.json"
        write_json_atomically(path, {"symbol": "ARXUSDT"})

        assert stat.S_IMODE(path.stat().st_mode) == 0o644


def test_registered_stable_roll_updates_only_lifecycle_and_defers_managed_profile(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    paths = _write_registered_roll_fixture(tmp_path)
    original_control = json.loads(paths[0].read_text(encoding="utf-8"))
    original_recovery_state = JsonRecoveryStore(paths[0]).read("ARXUSDT")
    original_guard_state = paths[2].read_bytes()
    original_loop_state = paths[3].read_bytes()
    original_owner = original_control[RUN_CONTRACT_OWNER_KEY]
    monkeypatch.setattr(
        sys,
        "argv",
        _registered_roll_argv(
            control_path=paths[0],
            runtime_profile_path=paths[1],
            guard_state_path=paths[2],
            loop_state_path=paths[3],
        ),
    )

    from deploy.oracle import roll_competition_window

    writes: list[Path] = []
    original_write = roll_competition_window.write_json_atomically

    def capture_write(path: Path, payload: dict[str, object]) -> None:
        writes.append(path)
        original_write(path, payload)

    monkeypatch.setattr(
        roll_competition_window,
        "write_json_atomically",
        capture_write,
    )
    roll_competition_window.main()
    status = json.loads(capsys.readouterr().out)
    saved = json.loads(paths[0].read_text(encoding="utf-8"))
    saved_owner = saved[RUN_CONTRACT_OWNER_KEY]
    saved_recovery_state = JsonRecoveryStore(paths[0]).read("ARXUSDT")

    assert status["status"] == "applied"
    assert status["registered_recovery_contract_roll"] is True
    assert status["recovery_restart_scheduled"] is True
    assert status["recovery_managed_profile_preserved"] is True
    assert status["recovery_phase"] == "active"
    assert status["recovery_effect_stage"] == "runner_restart"
    assert status["recovery_overlay_cleared"] is False
    assert status["runtime_guard_loss_recovery_cleared"] is False
    assert status["profile_rebased"] is False
    assert status["profile_rebase_partial"] is True
    assert status["managed_profile_changes_deferred"] == [
        "best_quote_maker_volume_allow_loss_reduce_only",
        "best_quote_maker_volume_cycle_budget_notional",
        "best_quote_maker_volume_net_loss_reduce_enabled",
        "hard_loss_forced_reduce_enabled",
        "max_actual_net_notional",
        "pause_buy_position_notional",
        "pause_short_position_notional",
        "step_price",
    ]
    assert saved[RECOVERY_STATE_KEY] != original_control[RECOVERY_STATE_KEY]
    assert saved[RECOVERY_STATE_MIRROR_KEY] == saved[RECOVERY_STATE_KEY]
    assert saved_recovery_state.document_revision == (
        original_recovery_state.document_revision + 1
    )
    assert saved_recovery_state.generation == original_recovery_state.generation + 1
    assert saved_recovery_state.phase is RecoveryPhase.ACTIVE
    assert saved_recovery_state.active_action is ActionId.BASELINE_REBASE
    assert saved_recovery_state.reasons == ("baseline_change_requested",)
    assert saved_recovery_state.pending_effect_stage is EffectStage.RUNNER_RESTART
    assert saved_recovery_state.pending_effect_epoch == saved_recovery_state.effect_epoch
    assert saved_recovery_state.baseline_profile == original_recovery_state.baseline_profile
    assert saved_recovery_state.desired_profile == original_recovery_state.desired_profile
    assert saved_recovery_state.issued_at is not None
    retry_at = saved_recovery_state.issued_at + timedelta(seconds=1)
    retry = FuturesRecoveryDecisionEngine().plan_round(
        snapshot=SymbolSnapshot(
            symbol="ARXUSDT",
            captured_at=retry_at,
            assessment=FlowBlockerAssessment(),
        ),
        state=saved_recovery_state,
        now=retry_at,
        round_id="guard-retries-rollover-restart-without-admission",
    )
    assert retry.action_id is ActionId.BASELINE_REBASE
    assert retry.mode is ActionMode.ADVANCE
    assert retry.effect_stage is EffectStage.RUNNER_RESTART
    assert retry.effect_epoch == saved_recovery_state.pending_effect_epoch
    assert saved["step_price"] == 0.0005
    assert saved["max_actual_net_notional"] == 1_000.0
    assert saved["best_quote_maker_volume_cycle_budget_notional"] == 360.0
    assert saved["pause_buy_position_notional"] == 400.0
    assert saved["pause_short_position_notional"] == 400.0
    assert saved["best_quote_maker_volume_allow_loss_reduce_only"] is False
    assert saved["best_quote_maker_volume_net_loss_reduce_enabled"] is False
    assert saved["hard_loss_forced_reduce_enabled"] is False
    assert saved["volatility_entry_pause_enabled"] is True
    assert saved["run_start_time"] != original_control["run_start_time"]
    assert saved["run_end_time"] != original_control["run_end_time"]
    assert saved["max_cumulative_notional"] == 40_000.0
    assert saved["best_quote_maker_volume_target_remaining_notional"] == 40_000.0
    assert saved_owner["generation"] == original_owner["generation"] + 1
    assert saved_owner["handoff_from_contract_id"] == original_owner["run_contract_id"]
    assert saved_owner["handoff_reason"] == "daily_competition_window_rollover"
    assert writes == [paths[0]]
    assert paths[2].read_bytes() == original_guard_state
    assert paths[3].read_bytes() == original_loop_state


@pytest.mark.parametrize(
    ("managed_key", "managed_value"),
    (
        ("run_start_time", "2026-07-14T08:00:00+08:00"),
        ("runtime_guard_stats_start_time", "2026-07-14T08:00:00+08:00"),
        ("run_end_time", "2026-07-15T08:00:00+08:00"),
        ("max_cumulative_notional", 20_000.0),
        ("best_quote_maker_volume_target_remaining_notional", 20_000.0),
        (RUN_CONTRACT_OWNER_KEY, {"managed": True}),
        ("terminal_drain_max_order_notional", 20.0),
    ),
)
def test_registered_stable_roll_rejects_recovery_managed_lifecycle_fields(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    managed_key: str,
    managed_value: object,
) -> None:
    paths = _write_registered_roll_fixture(
        tmp_path,
        extra_managed_baseline={managed_key: managed_value},
    )

    status = _invoke_registered_roll_deferred(
        monkeypatch,
        capsys,
        control_path=paths[0],
        runtime_profile_path=paths[1],
        guard_state_path=paths[2],
        loop_state_path=paths[3],
    )

    assert status["status"] == "deferred"
    assert status["reason"] == "registered_recovery_manages_lifecycle_fields"
    assert status["managed_lifecycle_fields"] == [managed_key]
    assert status["phase"] == "stable"
    assert status["changed"] is False


def test_registered_nonstable_roll_defers_without_any_control_or_state_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    paths = _write_registered_roll_fixture(
        tmp_path,
        phase=RecoveryPhase.COOLDOWN,
    )

    status = _invoke_registered_roll_deferred(
        monkeypatch,
        capsys,
        control_path=paths[0],
        runtime_profile_path=paths[1],
        guard_state_path=paths[2],
        loop_state_path=paths[3],
    )

    assert status["status"] == "deferred"
    assert status["reason"] == "registered_recovery_state_not_safe_to_rebase"
    assert status["phase"] == "cooldown"
    assert status["stable_rebase_eligible"] is False
    assert status["changed"] is False


def test_malformed_registered_recovery_envelope_fails_closed_without_writes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    paths = _write_registered_roll_fixture(tmp_path)
    control = json.loads(paths[0].read_text(encoding="utf-8"))
    control[RECOVERY_STATE_KEY] = {"schema_version": 1, "state": {}}
    paths[0].write_text(json.dumps(control), encoding="utf-8")

    status = _invoke_registered_roll_deferred(
        monkeypatch,
        capsys,
        control_path=paths[0],
        runtime_profile_path=paths[1],
        guard_state_path=paths[2],
        loop_state_path=paths[3],
    )

    assert status["status"] == "deferred"
    assert status["reason"] == "registered_recovery_state_invalid"
    assert status["phase"] is None
    assert status["stable_rebase_eligible"] is False
    assert status["changed"] is False


def test_incomplete_registered_control_cannot_fall_back_to_unregistered_backup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    paths = _write_registered_roll_fixture(tmp_path)
    registered = json.loads(paths[0].read_text(encoding="utf-8"))
    legacy_backup = dict(registered)
    legacy_backup.pop(RECOVERY_STATE_KEY)
    legacy_backup.pop(RECOVERY_STATE_MIRROR_KEY)
    backup_path = paths[0].with_name(paths[0].name + ".bak_legacy")
    backup_path.write_text(json.dumps(legacy_backup), encoding="utf-8")
    registered.pop("step_price")
    paths[0].write_text(json.dumps(registered), encoding="utf-8")

    status = _invoke_registered_roll_deferred(
        monkeypatch,
        capsys,
        control_path=paths[0],
        runtime_profile_path=paths[1],
        guard_state_path=paths[2],
        loop_state_path=paths[3],
    )

    assert status["status"] == "deferred"
    assert status["reason"] == "registered_recovery_state_invalid"
    assert status["phase"] is None
    assert status["stable_rebase_eligible"] is False
    assert status["changed"] is False


def test_rolled_target_rejects_missing_predictable_exit_contract() -> None:
    control: dict[str, object] = {
        "symbol": "ARXUSDT",
        "run_start_time": "2026-07-15T08:00:00+08:00",
        "runtime_guard_stats_start_time": "2026-07-15T08:00:00+08:00",
        "run_end_time": "2026-07-16T08:00:00+08:00",
        "max_cumulative_notional": 200_000.0,
    }

    with pytest.raises(ValueError, match="bounded run requires exit_policy"):
        validate_rolled_control_contract(control)


def test_rolled_target_accepts_bounded_drain_then_preserve_contract() -> None:
    control: dict[str, object] = {
        "symbol": "ARXUSDT",
        "run_start_time": "2026-07-15T08:00:00+08:00",
        "runtime_guard_stats_start_time": "2026-07-15T08:00:00+08:00",
        "run_end_time": "2026-07-16T08:00:00+08:00",
        "max_cumulative_notional": 200_000.0,
        "terminal_drain_exit_policy": "drain_then_preserve",
        "terminal_drain_absolute_loss_budget": 8.0,
        "terminal_drain_max_wait_seconds": 900.0,
    }

    validate_rolled_control_contract(control)


def test_rolled_control_binds_first_owner_and_requires_explicit_next_run_handoff() -> None:
    now = datetime(2026, 7, 15, 8, 0, tzinfo=BEIJING)
    control: dict[str, object] = {
        "symbol": "ARXUSDT",
        "strategy_profile": "arx-volume-v2",
        "strategy_mode": "best_quote_maker_volume",
        "run_start_time": now.isoformat(),
        "runtime_guard_stats_start_time": now.isoformat(),
        "run_end_time": datetime(2026, 7, 16, 8, 0, tzinfo=BEIJING).isoformat(),
        "max_cumulative_notional": 200_000.0,
        "per_order_notional": 8.0,
        "terminal_drain_exit_policy": "drain_then_preserve",
        "terminal_drain_absolute_loss_budget": 8.0,
        "terminal_drain_max_wait_seconds": 900.0,
    }

    owned, changed = bind_rolled_run_contract_owner(
        control,
        now=now,
        handoff_reason=None,
    )

    assert changed is True
    previous = owned[RUN_CONTRACT_OWNER_KEY]
    candidate = dict(owned)
    candidate["run_start_time"] = datetime(2026, 7, 16, 8, 0, tzinfo=BEIJING).isoformat()
    candidate["runtime_guard_stats_start_time"] = candidate["run_start_time"]
    candidate["run_end_time"] = datetime(2026, 7, 17, 8, 0, tzinfo=BEIJING).isoformat()

    with pytest.raises(ValueError, match="explicit run contract handoff is required"):
        bind_rolled_run_contract_owner(candidate, now=now, handoff_reason=None)

    handed_off, changed = bind_rolled_run_contract_owner(
        candidate,
        now=now,
        handoff_reason="daily_competition_window_rollover",
    )
    assert changed is True
    current = handed_off[RUN_CONTRACT_OWNER_KEY]
    assert current["generation"] == 2
    assert current["handoff_from_contract_id"] == previous["run_contract_id"]
    assert current["handoff_reason"] == "daily_competition_window_rollover"


def test_rolls_an_expired_window_to_the_current_trade_day() -> None:
    control = {
        "run_start_time": "2026-07-13T08:00:00+08:00",
        "run_end_time": "2026-07-14T08:00:00+08:00",
        "runtime_guard_stats_start_time": "2026-07-13T08:00:00+08:00",
    }

    result = roll_control_window(
        control,
        now=datetime(2026, 7, 14, 8, 28, tzinfo=BEIJING),
        reset_hour=8,
    )

    assert result["run_start_time"] == "2026-07-14T08:00:00+08:00"
    assert result["run_end_time"] == "2026-07-15T08:00:00+08:00"
    assert result["runtime_guard_stats_start_time"] == "2026-07-14T08:00:00+08:00"


def test_preserves_an_active_window() -> None:
    control = {
        "run_start_time": "2026-07-14T08:00:00+08:00",
        "run_end_time": "2026-07-15T08:00:00+08:00",
    }

    result = roll_control_window(
        control,
        now=datetime(2026, 7, 14, 8, 28, tzinfo=BEIJING),
        reset_hour=8,
    )

    assert result == control


def test_force_profile_rebase_refreshes_an_active_window_without_moving_it() -> None:
    control = {
        "run_start_time": "2026-07-14T08:00:00+08:00",
        "run_end_time": "2026-07-15T08:00:00+08:00",
        "max_actual_net_notional": 180.0,
        "best_quote_maker_volume_allow_loss_reduce_only": True,
    }
    profile = {
        "max_actual_net_notional": 1000.0,
        "best_quote_maker_volume_allow_loss_reduce_only": False,
    }

    result = roll_control_window(
        control,
        now=datetime(2026, 7, 14, 10, 30, tzinfo=BEIJING),
        reset_hour=8,
        runtime_profile=profile,
        force_profile_rebase=True,
    )

    assert result["run_start_time"] == control["run_start_time"]
    assert result["run_end_time"] == control["run_end_time"]
    assert result["max_actual_net_notional"] == 1000.0
    assert result["best_quote_maker_volume_allow_loss_reduce_only"] is False


def test_clears_only_temporary_recovery_overlay() -> None:
    state: dict[str, object] = {
        "symbols": {
            "ARXUSDT": {
                "guard_original_controls": {"step_price": 0.0005},
                "guard_recovery_controls": {"step_price": 0.0006},
                "recovery_owned": True,
                "last_volume_summary": {"gross_quote_qty": 123.0},
            }
        }
    }

    assert clear_recovery_overlay(state, symbol="ARXUSDT") is True
    item = state["symbols"]["ARXUSDT"]  # type: ignore[index]
    assert item == {"last_volume_summary": {"gross_quote_qty": 123.0}}


def test_apply_target_notional_updates_both_runner_target_fields() -> None:
    control: dict[str, object] = {
        "best_quote_maker_volume_target_remaining_notional": 100_000.0,
        "max_cumulative_notional": 180_000.0,
    }

    assert apply_target_notional(control, target_notional=200_000.0) is True
    assert control["best_quote_maker_volume_target_remaining_notional"] == 200_000.0
    assert control["max_cumulative_notional"] == 200_000.0
    assert apply_target_notional(control, target_notional=200_000.0) is False


def test_reset_runtime_guard_baseline_preserves_inventory_and_audit_state() -> None:
    now = datetime(2026, 7, 14, 18, 20, tzinfo=BEIJING)
    control: dict[str, object] = {"runtime_guard_stats_start_time": "2026-07-14T08:00:00+08:00"}
    state: dict[str, object] = {
        "runtime_guard_loss_recovery": {"stopped_at": "2026-07-14T18:10:00+08:00"},
        "best_quote_frozen_inventory": {"long_qty": 100.0},
        "best_quote_volume_ledger": {"realized_pnl": -12.0},
    }

    control_changed, recovery_cleared = reset_runtime_guard_baseline(control, state, now=now)

    assert control_changed is True
    assert recovery_cleared is True
    assert control["runtime_guard_stats_start_time"] == now.isoformat()
    assert "runtime_guard_loss_recovery" not in state
    assert state["best_quote_frozen_inventory"] == {"long_qty": 100.0}
    assert state["best_quote_volume_ledger"] == {"realized_pnl": -12.0}


def test_recovers_incomplete_control_from_latest_complete_backup() -> None:
    with TemporaryDirectory() as tmpdir:
        control_path = Path(tmpdir) / "arxusdt_loop_runner_control.json"
        control_path.write_text('{"hard_loss_forced_reduce_enabled": false}', encoding="utf-8")
        backup = control_path.with_name(control_path.name + ".bak_bq_volume_recovery_20260714T030000Z")
        backup.write_text(
            json.dumps(
                {
                    "symbol": "ARXUSDT",
                    "strategy_profile": "arxusdt_best_quote_maker_volume_114_v2",
                    "step_price": 0.0005,
                    "max_actual_net_notional": 1000.0,
                }
            ),
            encoding="utf-8",
        )

        control, recovered_from = load_usable_control(control_path)

    assert control["max_actual_net_notional"] == 1000.0
    assert recovered_from == str(backup)
