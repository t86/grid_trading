from datetime import datetime
from zoneinfo import ZoneInfo

from deploy.oracle.roll_competition_window import (
    apply_target_notional,
    clear_recovery_overlay,
    load_usable_control,
    roll_control_window,
)
from pathlib import Path
from tempfile import TemporaryDirectory
import json


BEIJING = ZoneInfo("Asia/Shanghai")


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
