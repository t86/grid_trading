import pytest

from grid_optimizer.inventory_grid_state import (
    apply_inventory_grid_fill,
    build_forced_reduce_lot_plan,
    new_inventory_grid_runtime,
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
