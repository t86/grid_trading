from datetime import datetime
from zoneinfo import ZoneInfo

from deploy.oracle.roll_competition_window import clear_recovery_overlay, roll_control_window


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
