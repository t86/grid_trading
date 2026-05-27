import pytest

from grid_optimizer.inventory_grid_state import (
    apply_inventory_grid_fill,
    apply_synthetic_frozen_pair_release,
    build_forced_reduce_lot_plan,
    freeze_inventory_grid_reduce_lots,
    new_inventory_grid_runtime,
    synthetic_frozen_position_report,
)


def test_apply_fill_updates_anchor_only_for_grid_roles() -> None:
    runtime = new_inventory_grid_runtime(market_type="futures")

    apply_inventory_grid_fill(
        runtime=runtime,
        role="bootstrap_entry",
        side="BUY",
        price=0.10,
        qty=400.0,
        fill_time_ms=1,
    )
    assert runtime["direction_state"] == "long_active"
    assert runtime["grid_anchor_price"] == 0.10

    apply_inventory_grid_fill(
        runtime=runtime,
        role="forced_reduce",
        side="SELL",
        price=0.09,
        qty=100.0,
        fill_time_ms=2,
    )
    assert runtime["grid_anchor_price"] == 0.10


def test_apply_fill_rejects_impossible_exit_on_flat_futures_runtime() -> None:
    runtime = new_inventory_grid_runtime(market_type="futures")

    with pytest.raises(ValueError):
        apply_inventory_grid_fill(
            runtime=runtime,
            role="grid_exit",
            side="SELL",
            price=0.09,
            qty=100.0,
            fill_time_ms=2,
        )

    assert runtime["direction_state"] == "flat"
    assert runtime["grid_anchor_price"] == 0.0
    assert runtime["position_lots"] == []


def test_apply_fill_raises_when_exit_exceeds_available_qty() -> None:
    runtime = new_inventory_grid_runtime(market_type="futures")
    runtime["direction_state"] = "long_active"
    runtime["position_lots"] = [
        {"lot_id": "lot_1", "side": "long", "qty": 50.0, "entry_price": 0.10, "opened_at_ms": 1, "source_role": "grid_entry"},
    ]

    with pytest.raises(ValueError):
        apply_inventory_grid_fill(
            runtime=runtime,
            role="grid_exit",
            side="SELL",
            price=0.09,
            qty=100.0,
            fill_time_ms=2,
        )

    assert runtime["direction_state"] == "long_active"
    assert runtime["position_lots"][0]["qty"] == 50.0
    assert runtime["pair_credit_steps"] == 0


def test_apply_fill_accumulates_pair_credit_steps_for_completed_pairs() -> None:
    runtime = new_inventory_grid_runtime(market_type="spot")

    apply_inventory_grid_fill(
        runtime=runtime,
        role="bootstrap_entry",
        side="BUY",
        price=0.10,
        qty=400.0,
        fill_time_ms=1,
    )
    apply_inventory_grid_fill(
        runtime=runtime,
        role="grid_exit",
        side="SELL",
        price=0.11,
        qty=100.0,
        fill_time_ms=2,
    )

    assert runtime["pair_credit_steps"] == 1
    assert runtime["position_lots"][0]["qty"] == 300.0


def test_grid_exit_fill_prefers_most_profitable_long_lots_first() -> None:
    runtime = new_inventory_grid_runtime(market_type="spot")
    runtime["direction_state"] = "long_active"
    runtime["position_lots"] = [
        {"lot_id": "expensive", "side": "long", "qty": 100.0, "entry_price": 0.10, "opened_at_ms": 1, "source_role": "grid_entry"},
        {"lot_id": "cheap", "side": "long", "qty": 100.0, "entry_price": 0.09, "opened_at_ms": 2, "source_role": "grid_entry"},
    ]

    apply_inventory_grid_fill(
        runtime=runtime,
        role="grid_exit",
        side="SELL",
        price=0.105,
        qty=100.0,
        fill_time_ms=3,
        step_price=0.01,
    )

    assert runtime["pair_credit_steps"] == 1
    assert [item["lot_id"] for item in runtime["position_lots"]] == ["expensive"]


def test_apply_fill_handles_short_side_entries_and_exits_with_anchor_updates() -> None:
    runtime = new_inventory_grid_runtime(market_type="futures")

    apply_inventory_grid_fill(
        runtime=runtime,
        role="grid_entry",
        side="SELL",
        price=0.12,
        qty=200.0,
        fill_time_ms=1,
    )
    assert runtime["direction_state"] == "short_active"
    assert runtime["grid_anchor_price"] == 0.12
    assert runtime["position_lots"][0]["side"] == "short"
    assert runtime["position_lots"][0]["qty"] == 200.0

    apply_inventory_grid_fill(
        runtime=runtime,
        role="grid_exit",
        side="BUY",
        price=0.11,
        qty=75.0,
        fill_time_ms=2,
    )

    assert runtime["grid_anchor_price"] == 0.11
    assert runtime["direction_state"] == "short_active"
    assert runtime["pair_credit_steps"] == 1
    assert runtime["position_lots"][0]["qty"] == 125.0


def test_forced_reduce_fill_debits_pair_credit_steps_for_long_side() -> None:
    runtime = new_inventory_grid_runtime(market_type="futures")
    runtime["direction_state"] = "long_active"
    runtime["pair_credit_steps"] = 5
    runtime["position_lots"] = [
        {
            "lot_id": "l1",
            "side": "long",
            "qty": 100.0,
            "entry_price": 0.10,
            "opened_at_ms": 1,
            "source_role": "grid_entry",
        }
    ]

    apply_inventory_grid_fill(
        runtime=runtime,
        role="forced_reduce",
        side="SELL",
        price=0.0701,
        qty=100.0,
        fill_time_ms=2,
        step_price=0.01,
    )

    assert runtime["pair_credit_steps"] == 3
    assert runtime["position_lots"] == []


def test_forced_reduce_fill_floors_pair_credit_steps_at_zero_for_short_side() -> None:
    runtime = new_inventory_grid_runtime(market_type="futures")
    runtime["direction_state"] = "short_active"
    runtime["pair_credit_steps"] = 1
    runtime["position_lots"] = [
        {
            "lot_id": "s1",
            "side": "short",
            "qty": 100.0,
            "entry_price": 0.10,
            "opened_at_ms": 1,
            "source_role": "grid_entry",
        }
    ]

    apply_inventory_grid_fill(
        runtime=runtime,
        role="forced_reduce",
        side="BUY",
        price=0.1299,
        qty=100.0,
        fill_time_ms=2,
        step_price=0.01,
    )

    assert runtime["pair_credit_steps"] == 0
    assert runtime["position_lots"] == []


def test_forced_reduce_fill_debit_matches_planner_cost_for_favorable_price() -> None:
    runtime = new_inventory_grid_runtime(market_type="futures")
    runtime["direction_state"] = "long_active"
    runtime["pair_credit_steps"] = 3
    runtime["position_lots"] = [
        {
            "lot_id": "l1",
            "side": "long",
            "qty": 100.0,
            "entry_price": 0.10,
            "opened_at_ms": 1,
            "source_role": "grid_entry",
        }
    ]

    lot_plan = build_forced_reduce_lot_plan(
        runtime=runtime,
        reduce_price=0.12,
        reduce_qty=100.0,
        step_price=0.01,
    )

    apply_inventory_grid_fill(
        runtime=runtime,
        role="forced_reduce",
        side="SELL",
        price=0.12,
        qty=100.0,
        fill_time_ms=2,
        step_price=0.01,
    )

    assert lot_plan["forced_reduce_cost_steps"] == 0
    assert runtime["pair_credit_steps"] == 3
    assert runtime["position_lots"] == []


def test_forced_reduce_fill_debit_matches_planner_cost_for_favorable_short_price() -> None:
    runtime = new_inventory_grid_runtime(market_type="futures")
    runtime["direction_state"] = "short_active"
    runtime["pair_credit_steps"] = 3
    runtime["position_lots"] = [
        {
            "lot_id": "s1",
            "side": "short",
            "qty": 100.0,
            "entry_price": 0.10,
            "opened_at_ms": 1,
            "source_role": "grid_entry",
        }
    ]

    lot_plan = build_forced_reduce_lot_plan(
        runtime=runtime,
        reduce_price=0.08,
        reduce_qty=100.0,
        step_price=0.01,
    )

    apply_inventory_grid_fill(
        runtime=runtime,
        role="forced_reduce",
        side="BUY",
        price=0.08,
        qty=100.0,
        fill_time_ms=2,
        step_price=0.01,
    )

    assert lot_plan["forced_reduce_cost_steps"] == 0
    assert runtime["pair_credit_steps"] == 3
    assert runtime["position_lots"] == []


def test_build_forced_reduce_lot_plan_prefers_most_adverse_longs() -> None:
    runtime = new_inventory_grid_runtime(market_type="futures")
    runtime["direction_state"] = "long_active"
    runtime["position_lots"] = [
        {"lot_id": "cheap", "side": "long", "qty": 100.0, "entry_price": 0.09, "opened_at_ms": 1, "source_role": "grid_entry"},
        {"lot_id": "expensive", "side": "long", "qty": 100.0, "entry_price": 0.10, "opened_at_ms": 2, "source_role": "grid_entry"},
    ]

    lot_plan = build_forced_reduce_lot_plan(
        runtime=runtime,
        reduce_price=0.095,
        reduce_qty=100.0,
        step_price=0.01,
    )

    assert [item["lot_id"] for item in lot_plan["lots"]] == ["expensive"]
    assert lot_plan["forced_reduce_cost_steps"] == 0


def test_forced_reduce_fill_matches_planner_cost_for_adverse_price() -> None:
    runtime = new_inventory_grid_runtime(market_type="futures")
    runtime["direction_state"] = "long_active"
    runtime["pair_credit_steps"] = 5
    runtime["position_lots"] = [
        {
            "lot_id": "l1",
            "side": "long",
            "qty": 100.0,
            "entry_price": 0.10,
            "opened_at_ms": 1,
            "source_role": "grid_entry",
        }
    ]

    lot_plan = build_forced_reduce_lot_plan(
        runtime=runtime,
        reduce_price=0.0701,
        reduce_qty=100.0,
        step_price=0.01,
    )

    apply_inventory_grid_fill(
        runtime=runtime,
        role="forced_reduce",
        side="SELL",
        price=0.0701,
        qty=100.0,
        fill_time_ms=2,
        step_price=0.01,
    )

    assert runtime["pair_credit_steps"] == 5 - lot_plan["forced_reduce_cost_steps"]
    assert lot_plan["forced_reduce_cost_steps"] == 2
    assert runtime["position_lots"] == []


def test_forced_reduce_fill_matches_planner_cost_for_adverse_short_price() -> None:
    runtime = new_inventory_grid_runtime(market_type="futures")
    runtime["direction_state"] = "short_active"
    runtime["pair_credit_steps"] = 5
    runtime["position_lots"] = [
        {
            "lot_id": "s1",
            "side": "short",
            "qty": 100.0,
            "entry_price": 0.10,
            "opened_at_ms": 1,
            "source_role": "grid_entry",
        }
    ]

    lot_plan = build_forced_reduce_lot_plan(
        runtime=runtime,
        reduce_price=0.1299,
        reduce_qty=100.0,
        step_price=0.01,
    )

    apply_inventory_grid_fill(
        runtime=runtime,
        role="forced_reduce",
        side="BUY",
        price=0.1299,
        qty=100.0,
        fill_time_ms=2,
        step_price=0.01,
    )

    assert runtime["pair_credit_steps"] == 5 - lot_plan["forced_reduce_cost_steps"]
    assert lot_plan["forced_reduce_cost_steps"] == 2
    assert runtime["position_lots"] == []


def test_forced_reduce_fill_consumes_same_most_adverse_lots_as_plan() -> None:
    runtime = new_inventory_grid_runtime(market_type="futures")
    runtime["direction_state"] = "long_active"
    runtime["position_lots"] = [
        {"lot_id": "cheap", "side": "long", "qty": 100.0, "entry_price": 0.09, "opened_at_ms": 1, "source_role": "grid_entry"},
        {"lot_id": "expensive", "side": "long", "qty": 100.0, "entry_price": 0.10, "opened_at_ms": 2, "source_role": "grid_entry"},
    ]

    plan = build_forced_reduce_lot_plan(
        runtime=runtime,
        reduce_price=0.095,
        reduce_qty=100.0,
        step_price=0.01,
    )

    apply_inventory_grid_fill(
        runtime=runtime,
        role="forced_reduce",
        side="SELL",
        price=0.095,
        qty=100.0,
        fill_time_ms=3,
    )

    assert [item["lot_id"] for item in plan["lots"]] == ["expensive"]
    assert runtime["position_lots"][0]["lot_id"] == "cheap"
    assert runtime["position_lots"][0]["qty"] == 100.0


def test_synthetic_freeze_moves_adverse_reduce_lots_to_frozen_inventory() -> None:
    runtime = new_inventory_grid_runtime(market_type="futures")
    runtime["synthetic_neutral"] = True
    runtime["direction_state"] = "long_active"
    runtime["position_lots"] = [
        {"lot_id": "cheap", "side": "long", "qty": 100.0, "entry_price": 0.09, "opened_at_ms": 1, "source_role": "grid_entry"},
        {"lot_id": "expensive", "side": "long", "qty": 100.0, "entry_price": 0.10, "opened_at_ms": 2, "source_role": "grid_entry"},
    ]

    result = freeze_inventory_grid_reduce_lots(
        runtime=runtime,
        reduce_price=0.08,
        reduce_qty=100.0,
        step_price=0.01,
        freeze_time_ms=3000,
        reason="threshold_reduce_loss",
    )

    assert result["applied"] is True
    assert result["freeze_qty"] == 100.0
    assert [item["lot_id"] for item in runtime["position_lots"]] == ["cheap"]
    assert len(runtime["frozen_position_lots"]) == 1
    frozen = runtime["frozen_position_lots"][0]
    assert frozen["side"] == "long"
    assert frozen["source_lot_id"] == "expensive"
    assert frozen["qty"] == 100.0
    assert frozen["entry_price"] == 0.10
    assert frozen["frozen_at_ms"] == 3000

    report = synthetic_frozen_position_report(runtime=runtime, mid_price=0.08)
    assert report["long_qty"] == 100.0
    assert report["long_notional"] == 8.0


def test_synthetic_freeze_can_create_opposite_frozen_lots_for_pair_release() -> None:
    runtime = new_inventory_grid_runtime(market_type="futures")
    runtime["synthetic_neutral"] = True
    runtime["direction_state"] = "short_active"
    runtime["frozen_position_lots"] = [
        {"lot_id": "frozen_long", "source_lot_id": "long", "side": "long", "qty": 50.0, "entry_price": 0.10, "frozen_at_ms": 1, "freeze_reason": "threshold_reduce_loss"},
    ]
    runtime["position_lots"] = [
        {"lot_id": "short", "side": "short", "qty": 80.0, "entry_price": 0.09, "opened_at_ms": 2, "source_role": "grid_entry"},
    ]

    result = freeze_inventory_grid_reduce_lots(
        runtime=runtime,
        reduce_price=0.11,
        reduce_qty=40.0,
        step_price=0.01,
        freeze_time_ms=3000,
        reason="threshold_reduce_loss",
    )

    assert result["applied"] is True
    report = synthetic_frozen_position_report(runtime=runtime, mid_price=0.10)
    assert report["long_qty"] == 50.0
    assert report["short_qty"] == 40.0
    assert report["pair_eligible_qty"] == 40.0


def test_synthetic_frozen_pair_release_offsets_long_and_short_lots_without_orders() -> None:
    runtime = new_inventory_grid_runtime(market_type="futures")
    runtime["synthetic_neutral"] = True
    runtime["frozen_position_lots"] = [
        {"lot_id": "frozen_long", "source_lot_id": "long", "side": "long", "qty": 60.0, "entry_price": 0.10, "frozen_at_ms": 1, "freeze_reason": "threshold_reduce_loss"},
        {"lot_id": "frozen_short", "source_lot_id": "short", "side": "short", "qty": 40.0, "entry_price": 0.12, "frozen_at_ms": 2, "freeze_reason": "threshold_reduce_loss"},
    ]

    result = apply_synthetic_frozen_pair_release(
        runtime=runtime,
        release_qty=None,
        release_time_ms=4000,
        reason="pair_release",
    )

    assert result["applied"] is True
    assert result["release_qty"] == 40.0
    assert result["pair_release_pnl_quote"] == pytest.approx(0.8)
    assert runtime["frozen_position_lots"] == [
        {"lot_id": "frozen_long", "source_lot_id": "long", "side": "long", "qty": 20.0, "entry_price": 0.10, "frozen_at_ms": 1, "freeze_reason": "threshold_reduce_loss"},
    ]
    assert runtime["last_strategy_trade_time_ms"] == 4000


def test_synthetic_frozen_release_fill_consumes_only_frozen_lots() -> None:
    runtime = new_inventory_grid_runtime(market_type="futures")
    runtime["synthetic_neutral"] = True
    runtime["direction_state"] = "long_active"
    runtime["position_lots"] = [
        {"lot_id": "active", "side": "long", "qty": 50.0, "entry_price": 0.09, "opened_at_ms": 1, "source_role": "grid_entry"},
    ]
    runtime["frozen_position_lots"] = [
        {"lot_id": "frozen_1", "source_lot_id": "frozen_source", "side": "long", "qty": 100.0, "entry_price": 0.10, "frozen_at_ms": 2, "freeze_reason": "threshold_reduce_loss"},
    ]

    apply_inventory_grid_fill(
        runtime=runtime,
        role="synthetic_frozen_release",
        side="SELL",
        price=0.101,
        qty=100.0,
        fill_time_ms=4000,
        step_price=0.01,
    )

    assert runtime["position_lots"][0]["lot_id"] == "active"
    assert runtime["position_lots"][0]["qty"] == 50.0
    assert runtime["frozen_position_lots"] == []
    assert runtime["direction_state"] == "long_active"


def test_synthetic_frozen_manual_release_fill_consumes_frozen_lots() -> None:
    runtime = new_inventory_grid_runtime(market_type="futures")
    runtime["synthetic_neutral"] = True
    runtime["direction_state"] = "flat"
    runtime["frozen_position_lots"] = [
        {"lot_id": "frozen_1", "source_lot_id": "held", "side": "long", "qty": 100.0, "entry_price": 0.10, "frozen_at_ms": 2, "freeze_reason": "threshold_reduce_loss"},
    ]

    apply_inventory_grid_fill(
        runtime=runtime,
        role="synthetic_frozen_manual_release",
        side="SELL",
        price=0.09,
        qty=60.0,
        fill_time_ms=4000,
        step_price=0.01,
    )

    assert runtime["frozen_position_lots"][0]["qty"] == 40.0
    assert runtime["last_strategy_trade_time_ms"] == 4000


def test_synthetic_frozen_short_release_consumes_quote_backed_liability() -> None:
    runtime = new_inventory_grid_runtime(market_type="futures")
    runtime["synthetic_neutral"] = True
    runtime["direction_state"] = "flat"
    runtime["frozen_position_lots"] = [
        {"lot_id": "frozen_short", "source_lot_id": "sold_neutral_base", "side": "short", "qty": 100.0, "entry_price": 0.10, "frozen_at_ms": 2, "freeze_reason": "threshold_reduce_loss"},
    ]

    apply_inventory_grid_fill(
        runtime=runtime,
        role="synthetic_frozen_release",
        side="BUY",
        price=0.099,
        qty=100.0,
        fill_time_ms=4000,
        step_price=0.01,
    )

    assert runtime["position_lots"] == []
    assert runtime["frozen_position_lots"] == []
    assert runtime["direction_state"] == "flat"


def test_tail_cleanup_closes_long_and_short_inventory_without_anchor_updates() -> None:
    runtime = new_inventory_grid_runtime(market_type="futures")

    apply_inventory_grid_fill(
        runtime=runtime,
        role="bootstrap_entry",
        side="BUY",
        price=0.10,
        qty=100.0,
        fill_time_ms=1,
    )
    runtime["grid_anchor_price"] = 0.10
    apply_inventory_grid_fill(
        runtime=runtime,
        role="tail_cleanup",
        side="SELL",
        price=0.09,
        qty=100.0,
        fill_time_ms=2,
    )
    assert runtime["direction_state"] == "flat"
    assert runtime["position_lots"] == []
    assert runtime["grid_anchor_price"] == 0.10

    apply_inventory_grid_fill(
        runtime=runtime,
        role="grid_entry",
        side="SELL",
        price=0.12,
        qty=50.0,
        fill_time_ms=3,
    )
    runtime["grid_anchor_price"] = 0.12
    apply_inventory_grid_fill(
        runtime=runtime,
        role="tail_cleanup",
        side="BUY",
        price=0.11,
        qty=50.0,
        fill_time_ms=4,
    )
    assert runtime["direction_state"] == "flat"
    assert runtime["position_lots"] == []
    assert runtime["grid_anchor_price"] == 0.12


def test_build_forced_reduce_lot_plan_rejects_invalid_inputs() -> None:
    runtime = new_inventory_grid_runtime(market_type="futures")
    runtime["direction_state"] = "bogus"
    runtime["position_lots"] = [
        {"lot_id": "lot_1", "side": "long", "qty": 100.0, "entry_price": 0.10, "opened_at_ms": 1, "source_role": "grid_entry"},
    ]

    with pytest.raises(ValueError):
        build_forced_reduce_lot_plan(
            runtime=runtime,
            reduce_price=0.095,
            reduce_qty=100.0,
            step_price=0.01,
        )

    runtime["direction_state"] = "long_active"
    with pytest.raises(ValueError):
        build_forced_reduce_lot_plan(
            runtime=runtime,
            reduce_price=0.095,
            reduce_qty=100.0,
            step_price=0.0,
        )
