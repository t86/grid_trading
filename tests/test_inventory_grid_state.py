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
