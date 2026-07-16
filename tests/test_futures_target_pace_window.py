from datetime import datetime, timedelta, timezone

import pytest

from grid_optimizer.bq_volume_recovery_guard import (
    _configured_target_pace_window,
    _target_trade_fetch_window_seconds,
    apply_daily_target_pace_floor,
    apply_target_pace_cycle_budget_floor,
)
from grid_optimizer.futures_run_lifecycle import (
    run_contract_identity_from_config,
    run_contract_snapshot_from_config,
    validate_run_contract,
)


NOW = datetime(2026, 7, 16, 10, 30, tzinfo=timezone.utc)
START = NOW - timedelta(minutes=30)
END = NOW + timedelta(minutes=30)


def _exit_terms() -> dict[str, object]:
    return {
        "run_end_time": END.isoformat(),
        "max_cumulative_notional": 20_000.0,
        "runtime_guard_stats_start_time": START.isoformat(),
        "terminal_drain_exit_policy": "drain_then_preserve",
        "terminal_drain_absolute_loss_budget": 2.0,
        "terminal_drain_max_wait_seconds": 600.0,
    }


def test_target_requires_an_explicit_statistics_start() -> None:
    terms = _exit_terms()
    terms["runtime_guard_stats_start_time"] = None

    with pytest.raises(ValueError, match="runtime_guard_stats_start_time"):
        validate_run_contract(
            run_end_time=terms["run_end_time"],
            target_value=terms["max_cumulative_notional"],
            runtime_guard_stats_start_time=terms["runtime_guard_stats_start_time"],
            run_start_time=None,
            exit_policy=terms["terminal_drain_exit_policy"],
            loss_budget=terms["terminal_drain_absolute_loss_budget"],
            max_wait_seconds=terms["terminal_drain_max_wait_seconds"],
            preserve_reason=None,
        )


def test_target_statistics_start_must_precede_deadline() -> None:
    terms = _exit_terms()

    with pytest.raises(ValueError, match="must be earlier than run_end_time"):
        validate_run_contract(
            run_end_time=terms["run_end_time"],
            target_value=terms["max_cumulative_notional"],
            runtime_guard_stats_start_time=terms["run_end_time"],
            run_start_time=None,
            exit_policy=terms["terminal_drain_exit_policy"],
            loss_budget=terms["terminal_drain_absolute_loss_budget"],
            max_wait_seconds=terms["terminal_drain_max_wait_seconds"],
            preserve_reason=None,
        )


def test_statistics_start_is_part_of_immutable_run_identity() -> None:
    first = _exit_terms()
    second = {**first, "runtime_guard_stats_start_time": (START + timedelta(seconds=1)).isoformat()}

    assert run_contract_identity_from_config(first) != run_contract_identity_from_config(second)


def test_wear_exit_thresholds_are_part_of_immutable_run_identity() -> None:
    first = {
        **_exit_terms(),
        "lifecycle_wear_stop_per_10k": 2.0,
        "lifecycle_wear_stop_min_gross_notional": 75_000.0,
    }
    changed_stop = {**first, "lifecycle_wear_stop_per_10k": 2.5}
    changed_arm = {
        **first,
        "lifecycle_wear_stop_min_gross_notional": 80_000.0,
    }

    assert run_contract_identity_from_config(first) != run_contract_identity_from_config(changed_stop)
    assert run_contract_identity_from_config(first) != run_contract_identity_from_config(changed_arm)


def test_terminal_execution_fallback_and_strategy_mode_are_in_run_identity() -> None:
    first = {
        **_exit_terms(),
        "symbol": "BCHUSDT",
        "strategy_profile": "probe_v1",
        "strategy_mode": "hedge_best_quote_maker_volume_v1",
        "per_order_notional": 20.0,
    }
    changed_size = {**first, "per_order_notional": 21.0}
    changed_mode = {**first, "strategy_mode": "synthetic_neutral"}

    assert run_contract_identity_from_config(first) != run_contract_identity_from_config(changed_size)
    assert run_contract_identity_from_config(first) != run_contract_identity_from_config(changed_mode)


def test_run_contract_snapshot_is_canonical_and_rehashable() -> None:
    config = {
        **_exit_terms(),
        "symbol": "bchusdt",
        "strategy_profile": "probe_v1",
        "strategy_mode": "hedge_best_quote_maker_volume_v1",
        "per_order_notional": 20.0,
    }

    snapshot = run_contract_snapshot_from_config(config)

    assert snapshot["symbol"] == "BCHUSDT"
    assert snapshot["terminal_drain_max_order_notional"] == pytest.approx(20.0)
    assert run_contract_identity_from_config(snapshot) == run_contract_identity_from_config(config)


def test_configured_target_window_uses_explicit_statistics_start() -> None:
    assert _configured_target_pace_window(_exit_terms()) == (START, END)


def test_exchange_trade_fetch_keeps_contract_window_after_deadline() -> None:
    now = END + timedelta(minutes=30)

    fetch_window = _target_trade_fetch_window_seconds(
        control=_exit_terms(),
        now=now,
        window_seconds=180.0,
        has_cli_target=False,
    )

    assert fetch_window == pytest.approx((now - START).total_seconds())


def test_one_hour_target_pace_uses_run_window_instead_of_rest_of_day() -> None:
    summary: dict[str, object] = {}

    floor = apply_daily_target_pace_floor(
        volume_summary=summary,
        rows=[],
        now=NOW,
        window_seconds=180.0,
        min_volume_notional=100.0,
        daily_target_notional=20_000.0,
        target_pace_fraction=0.9,
        target_pace_max_multiplier=2.0,
        target_completion_buffer_seconds=0.0,
        target_window_start=START,
        target_window_end=END,
    )

    # Half of a one-hour contract remains, so the required pace is 40k/hour.
    # The old day-based calculation incorrectly spread the same target over the
    # remaining 13.5 hours.
    assert summary["required_hourly_notional"] == pytest.approx(40_000.0)
    assert summary["target_window_start"] == START.isoformat()
    assert summary["target_deadline"] == END.isoformat()
    assert summary["target_pace_window_source"] == "run_contract"
    assert floor == pytest.approx(200.0)


def test_target_pace_counts_only_trades_inside_statistics_window() -> None:
    rows = [
        {
            "id": 1,
            "time": int((START - timedelta(seconds=1)).timestamp() * 1000),
            "quoteQty": "9000",
        },
        {
            "id": 2,
            "time": int((START + timedelta(minutes=5)).timestamp() * 1000),
            "quoteQty": "1000",
        },
    ]
    summary: dict[str, object] = {}

    apply_daily_target_pace_floor(
        volume_summary=summary,
        rows=rows,
        now=NOW,
        window_seconds=180.0,
        min_volume_notional=100.0,
        daily_target_notional=20_000.0,
        target_pace_fraction=0.9,
        target_pace_max_multiplier=2.0,
        target_window_start=START,
        target_window_end=END,
    )

    assert summary["target_window_gross_notional"] == pytest.approx(1000.0)
    assert summary["remaining_target_notional"] == pytest.approx(19_000.0)
    assert summary["required_hourly_notional"] == pytest.approx(38_000.0)


def test_cycle_budget_pace_annualizes_only_elapsed_run_window() -> None:
    summary: dict[str, object] = {"required_hourly_notional": 2_000.0}
    rows = [
        {
            "id": 1,
            "time": int((START + timedelta(minutes=5)).timestamp() * 1000),
            "quoteQty": "1000",
            "realizedPnl": "0",
        }
    ]

    floor = apply_target_pace_cycle_budget_floor(
        volume_summary=summary,
        rows=rows,
        now=NOW,
        target_window_start=START,
        control={
            "best_quote_maker_volume_cycle_budget_notional": 128.0,
            "per_order_notional": 32.0,
            "buy_levels": 4,
            "sell_levels": 4,
        },
        assessment={
            "actual_long_notional": 400.0,
            "actual_short_notional": 350.0,
            "frozen_total_notional": 0.0,
            "max_long_notional": 700.0,
            "max_short_notional": 700.0,
        },
        static_floor_notional=108.0,
        target_pace_fraction=0.9,
        cycle_budget_increment=12.0,
    )

    assert summary["trailing_window_seconds"] == pytest.approx(1800.0)
    assert summary["trailing_60m_hourly_notional"] == pytest.approx(2_000.0)
    assert floor == pytest.approx(108.0)
