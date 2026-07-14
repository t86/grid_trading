from datetime import datetime
from zoneinfo import ZoneInfo

from deploy.oracle.roll_competition_window import roll_control_window


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
