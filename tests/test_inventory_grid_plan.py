from grid_optimizer.inventory_grid_plan import build_inventory_grid_orders
from grid_optimizer.inventory_grid_state import new_inventory_grid_runtime


def test_futures_flat_starts_with_two_bootstrap_orders() -> None:
    runtime = new_inventory_grid_runtime(market_type="futures")

    plan = build_inventory_grid_orders(
        runtime=runtime,
        bid_price=0.0999,
        ask_price=0.1001,
        step_price=0.01,
        per_order_notional=10.0,
        first_order_multiplier=4.0,
        threshold_position_notional=50.0,
        max_order_position_notional=80.0,
        max_position_notional=120.0,
        tick_size=0.0001,
        step_size=1.0,
        min_qty=1.0,
        min_notional=5.0,
    )

    assert {item["side"] for item in plan["bootstrap_orders"]} == {"BUY", "SELL"}
    assert plan["buy_orders"] == []
    assert plan["sell_orders"] == []
    assert plan["forced_reduce_orders"] == []
    assert plan["tail_cleanup_active"] is False


def test_spot_flat_starts_with_single_bootstrap_buy() -> None:
    runtime = new_inventory_grid_runtime(market_type="spot")

    plan = build_inventory_grid_orders(
        runtime=runtime,
        bid_price=0.0999,
        ask_price=0.1001,
        step_price=0.01,
        per_order_notional=10.0,
        first_order_multiplier=4.0,
        threshold_position_notional=50.0,
        max_order_position_notional=80.0,
        max_position_notional=120.0,
        tick_size=0.0001,
        step_size=1.0,
        min_qty=1.0,
        min_notional=5.0,
    )

    assert len(plan["bootstrap_orders"]) == 1
    assert plan["bootstrap_orders"][0]["side"] == "BUY"
    assert plan["bootstrap_orders"][0]["role"] == "bootstrap_entry"
    assert plan["sell_orders"] == []


def test_threshold_reduce_only_blocks_same_side_entries_until_pair_credit_covers_cost() -> None:
    runtime = new_inventory_grid_runtime(market_type="futures")
    runtime["direction_state"] = "long_active"
    runtime["grid_anchor_price"] = 0.10
    runtime["position_lots"] = [
        {
            "lot_id": "l1",
            "side": "long",
            "qty": 100.0,
            "entry_price": 0.10,
            "opened_at_ms": 1,
            "source_role": "bootstrap_entry",
        },
        {
            "lot_id": "l2",
            "side": "long",
            "qty": 100.0,
            "entry_price": 0.09,
            "opened_at_ms": 2,
            "source_role": "grid_entry",
        },
    ]

    plan = build_inventory_grid_orders(
        runtime=runtime,
        bid_price=0.0799,
        ask_price=0.0801,
        step_price=0.01,
        per_order_notional=10.0,
        first_order_multiplier=4.0,
        threshold_position_notional=12.0,
        max_order_position_notional=30.0,
        max_position_notional=40.0,
        tick_size=0.0001,
        step_size=1.0,
        min_qty=1.0,
        min_notional=5.0,
    )

    assert plan["risk_state"] == "threshold_reduce_only"
    assert plan["buy_orders"] == []
    assert all(item["role"] != "forced_reduce" for item in plan["sell_orders"])


def test_pair_credit_unlocks_forced_reduce_order() -> None:
    runtime = new_inventory_grid_runtime(market_type="futures")
    runtime["direction_state"] = "long_active"
    runtime["grid_anchor_price"] = 0.10
    runtime["pair_credit_steps"] = 5
    runtime["position_lots"] = [
        {
            "lot_id": "l1",
            "side": "long",
            "qty": 200.0,
            "entry_price": 0.10,
            "opened_at_ms": 1,
            "source_role": "bootstrap_entry",
        }
    ]

    plan = build_inventory_grid_orders(
        runtime=runtime,
        bid_price=0.0899,
        ask_price=0.0901,
        step_price=0.01,
        per_order_notional=10.0,
        first_order_multiplier=4.0,
        threshold_position_notional=5.0,
        max_order_position_notional=30.0,
        max_position_notional=40.0,
        tick_size=0.0001,
        step_size=1.0,
        min_qty=1.0,
        min_notional=5.0,
    )

    assert any(item["role"] == "forced_reduce" for item in plan["sell_orders"])
    assert any(item["role"] == "forced_reduce" for item in plan["forced_reduce_orders"])


def test_threshold_reduce_only_uses_raw_best_price_for_forced_reduce_credit() -> None:
    runtime = new_inventory_grid_runtime(market_type="futures")
    runtime["direction_state"] = "long_active"
    runtime["grid_anchor_price"] = 0.10
    runtime["pair_credit_steps"] = 0
    runtime["position_lots"] = [
        {
            "lot_id": "l1",
            "side": "long",
            "qty": 200.0,
            "entry_price": 0.10,
            "opened_at_ms": 1,
            "source_role": "bootstrap_entry",
        }
    ]

    plan = build_inventory_grid_orders(
        runtime=runtime,
        bid_price=0.0748,
        ask_price=0.0749,
        step_price=0.025,
        per_order_notional=10.0,
        first_order_multiplier=4.0,
        threshold_position_notional=5.0,
        max_order_position_notional=30.0,
        max_position_notional=40.0,
        tick_size=0.01,
        step_size=1.0,
        min_qty=1.0,
        min_notional=5.0,
    )

    assert plan["risk_state"] == "threshold_reduce_only"
    assert all(item["role"] != "forced_reduce" for item in plan["sell_orders"])
    assert plan["forced_reduce_orders"] == []


def test_tail_cleanup_emits_single_cleanup_order() -> None:
    runtime = new_inventory_grid_runtime(market_type="spot")
    runtime["direction_state"] = "long_active"
    runtime["grid_anchor_price"] = 0.10
    runtime["position_lots"] = [
        {
            "lot_id": "tail",
            "side": "long",
            "qty": 5.0,
            "entry_price": 0.10,
            "opened_at_ms": 1,
            "source_role": "grid_entry",
        }
    ]

    plan = build_inventory_grid_orders(
        runtime=runtime,
        bid_price=0.0999,
        ask_price=0.1001,
        step_price=0.01,
        per_order_notional=10.0,
        first_order_multiplier=4.0,
        threshold_position_notional=50.0,
        max_order_position_notional=80.0,
        max_position_notional=120.0,
        tick_size=0.0001,
        step_size=1.0,
        min_qty=1.0,
        min_notional=5.0,
    )

    assert plan["tail_cleanup_active"] is True
    assert [item["role"] for item in plan["sell_orders"]] == ["tail_cleanup"]
    assert plan["buy_orders"] == []


def test_tail_cleanup_overrides_threshold_reduce_only() -> None:
    runtime = new_inventory_grid_runtime(market_type="spot")
    runtime["direction_state"] = "long_active"
    runtime["grid_anchor_price"] = 0.10
    runtime["position_lots"] = [
        {
            "lot_id": "tail",
            "side": "long",
            "qty": 5.0,
            "entry_price": 0.10,
            "opened_at_ms": 1,
            "source_role": "grid_entry",
        }
    ]

    plan = build_inventory_grid_orders(
        runtime=runtime,
        bid_price=0.0999,
        ask_price=0.1001,
        step_price=0.01,
        per_order_notional=10.0,
        first_order_multiplier=4.0,
        threshold_position_notional=0.1,
        max_order_position_notional=80.0,
        max_position_notional=120.0,
        tick_size=0.0001,
        step_size=1.0,
        min_qty=1.0,
        min_notional=5.0,
    )

    assert plan["risk_state"] == "threshold_reduce_only"
    assert plan["tail_cleanup_active"] is True
    assert [item["role"] for item in plan["sell_orders"]] == ["tail_cleanup"]
    assert plan["buy_orders"] == []
    assert plan["forced_reduce_orders"] == []


def test_max_order_position_notional_blocks_new_same_side_entries() -> None:
    runtime = new_inventory_grid_runtime(market_type="futures")
    runtime["direction_state"] = "long_active"
    runtime["grid_anchor_price"] = 0.10
    runtime["position_lots"] = [
        {
            "lot_id": "l1",
            "side": "long",
            "qty": 200.0,
            "entry_price": 0.10,
            "opened_at_ms": 1,
            "source_role": "bootstrap_entry",
        }
    ]

    plan = build_inventory_grid_orders(
        runtime=runtime,
        bid_price=0.0999,
        ask_price=0.1001,
        step_price=0.01,
        per_order_notional=10.0,
        first_order_multiplier=4.0,
        threshold_position_notional=1000.0,
        max_order_position_notional=15.0,
        max_position_notional=120.0,
        tick_size=0.0001,
        step_size=1.0,
        min_qty=1.0,
        min_notional=5.0,
    )

    assert plan["risk_state"] == "normal"
    assert plan["buy_orders"] == []
    assert plan["forced_reduce_orders"] == []


def test_long_active_grid_orders_are_anchored_to_grid_price() -> None:
    runtime = new_inventory_grid_runtime(market_type="futures")
    runtime["direction_state"] = "long_active"
    runtime["grid_anchor_price"] = 0.10
    runtime["position_lots"] = [
        {
            "lot_id": "l1",
            "side": "long",
            "qty": 100.0,
            "entry_price": 0.10,
            "opened_at_ms": 1,
            "source_role": "bootstrap_entry",
        }
    ]

    plan = build_inventory_grid_orders(
        runtime=runtime,
        bid_price=0.1010,
        ask_price=0.1020,
        step_price=0.01,
        per_order_notional=10.0,
        first_order_multiplier=4.0,
        threshold_position_notional=50.0,
        max_order_position_notional=80.0,
        max_position_notional=120.0,
        tick_size=0.0001,
        step_size=1.0,
        min_qty=1.0,
        min_notional=5.0,
    )

    assert [item["price"] for item in plan["buy_orders"]] == [0.09]
    assert [item["price"] for item in plan["sell_orders"]] == [0.11]


def test_short_active_grid_orders_are_anchored_to_grid_price() -> None:
    runtime = new_inventory_grid_runtime(market_type="futures")
    runtime["direction_state"] = "short_active"
    runtime["grid_anchor_price"] = 0.10
    runtime["position_lots"] = [
        {
            "lot_id": "s1",
            "side": "short",
            "qty": 100.0,
            "entry_price": 0.10,
            "opened_at_ms": 1,
            "source_role": "bootstrap_entry",
        }
    ]

    plan = build_inventory_grid_orders(
        runtime=runtime,
        bid_price=0.0990,
        ask_price=0.1000,
        step_price=0.01,
        per_order_notional=10.0,
        first_order_multiplier=4.0,
        threshold_position_notional=50.0,
        max_order_position_notional=80.0,
        max_position_notional=120.0,
        tick_size=0.0001,
        step_size=1.0,
        min_qty=1.0,
        min_notional=5.0,
    )

    assert [item["price"] for item in plan["buy_orders"]] == [0.09]
    assert [item["price"] for item in plan["sell_orders"]] == [0.11]
