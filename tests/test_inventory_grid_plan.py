from grid_optimizer.inventory_grid_plan import build_inventory_grid_orders
from grid_optimizer.inventory_grid_state import apply_inventory_grid_fill, new_inventory_grid_runtime


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
        min_notional=0.1,
    )

    assert {item["side"] for item in plan["bootstrap_orders"]} == {"BUY", "SELL"}
    assert {item.get("position_side", "BOTH") for item in plan["bootstrap_orders"]} == {"BOTH"}
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
        min_notional=0.1,
    )

    assert len(plan["bootstrap_orders"]) == 1
    assert plan["bootstrap_orders"][0]["side"] == "BUY"
    assert plan["bootstrap_orders"][0]["role"] == "bootstrap_entry"
    assert plan["sell_orders"] == []


def test_spot_flat_seeds_multiple_buy_levels() -> None:
    runtime = new_inventory_grid_runtime(market_type="spot")

    plan = build_inventory_grid_orders(
        runtime=runtime,
        bid_price=100.0,
        ask_price=100.2,
        step_price=0.7,
        per_order_notional=20.0,
        first_order_multiplier=1.0,
        threshold_position_notional=50.0,
        max_order_position_notional=80.0,
        max_position_notional=120.0,
        buy_levels=3,
        tick_size=0.1,
        step_size=0.001,
        min_qty=0.001,
        min_notional=5.0,
    )

    assert [item["side"] for item in plan["bootstrap_orders"]] == ["BUY", "BUY", "BUY"]
    assert [item["role"] for item in plan["bootstrap_orders"]] == ["bootstrap_entry", "grid_entry", "grid_entry"]
    assert [item["price"] for item in plan["bootstrap_orders"]] == [100.0, 99.3, 98.6]
    assert plan["buy_orders"] == []
    assert plan["sell_orders"] == []


def test_conservative_flat_runtime_does_not_bootstrap() -> None:
    runtime = new_inventory_grid_runtime(market_type="futures")
    runtime["recovery_mode"] = "conservative_reduce_only"
    runtime["risk_state"] = "hard_reduce_only"

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
        min_notional=0.1,
    )

    assert plan["risk_state"] == "hard_reduce_only"
    assert plan["bootstrap_orders"] == []
    assert plan["buy_orders"] == []
    assert plan["sell_orders"] == []
    assert plan["forced_reduce_orders"] == []


def test_conservative_active_runtime_preserves_risk_state_and_blocks_entries() -> None:
    runtime = new_inventory_grid_runtime(market_type="futures")
    runtime["recovery_mode"] = "conservative_reduce_only"
    runtime["risk_state"] = "hard_reduce_only"
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
        max_order_position_notional=80.0,
        max_position_notional=120.0,
        tick_size=0.0001,
        step_size=1.0,
        min_qty=1.0,
        min_notional=0.1,
    )

    assert plan["risk_state"] == "hard_reduce_only"
    assert plan["buy_orders"] == []


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


def test_grid_exit_is_capped_to_held_qty_for_long_side() -> None:
    runtime = new_inventory_grid_runtime(market_type="futures")
    runtime["direction_state"] = "long_active"
    runtime["grid_anchor_price"] = 0.01
    runtime["position_lots"] = [
        {
            "lot_id": "l1",
            "side": "long",
            "qty": 100.0,
            "entry_price": 0.01,
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
        threshold_position_notional=5.0,
        max_order_position_notional=30.0,
        max_position_notional=40.0,
        tick_size=0.0001,
        step_size=1.0,
        min_qty=1.0,
        min_notional=0.1,
    )

    assert [item["role"] for item in plan["sell_orders"]] == ["grid_exit"]
    assert [item["qty"] for item in plan["sell_orders"]] == [100.0]

    replay_runtime = new_inventory_grid_runtime(market_type="futures")
    replay_runtime["direction_state"] = "long_active"
    replay_runtime["grid_anchor_price"] = 0.01
    replay_runtime["position_lots"] = [
        {
            "lot_id": "l1",
            "side": "long",
            "qty": 100.0,
            "entry_price": 0.01,
            "opened_at_ms": 1,
            "source_role": "bootstrap_entry",
        }
    ]

    order = plan["sell_orders"][0]
    apply_inventory_grid_fill(
        runtime=replay_runtime,
        role=order["role"],
        side=order["side"],
        price=order["price"],
        qty=order["qty"],
        fill_time_ms=1,
    )

    assert replay_runtime["direction_state"] == "flat"
    assert replay_runtime["position_lots"] == []


def test_spot_long_active_grid_exit_uses_best_ask_when_anchor_exit_is_stale() -> None:
    runtime = new_inventory_grid_runtime(market_type="spot")
    runtime["direction_state"] = "long_active"
    runtime["grid_anchor_price"] = 100.0
    runtime["position_lots"] = [
        {
            "lot_id": "l1",
            "side": "long",
            "qty": 1.0,
            "entry_price": 100.0,
            "opened_at_ms": 1,
            "source_role": "bootstrap_entry",
        }
    ]

    plan = build_inventory_grid_orders(
        runtime=runtime,
        bid_price=109.9,
        ask_price=110.1,
        step_price=0.7,
        per_order_notional=20.0,
        first_order_multiplier=1.0,
        threshold_position_notional=500.0,
        max_order_position_notional=800.0,
        max_position_notional=1200.0,
        buy_levels=3,
        tick_size=0.1,
        step_size=0.001,
        min_qty=0.001,
        min_notional=5.0,
    )

    assert [item["role"] for item in plan["sell_orders"]] == ["grid_exit"]
    assert [item["price"] for item in plan["sell_orders"]] == [110.1]
    assert [item["role"] for item in plan["buy_orders"]] == ["grid_entry", "grid_entry", "grid_entry"]


def test_spot_long_active_supports_multiple_sell_levels() -> None:
    runtime = new_inventory_grid_runtime(market_type="spot")
    runtime["direction_state"] = "long_active"
    runtime["grid_anchor_price"] = 100.0
    runtime["position_lots"] = [
        {
            "lot_id": "l1",
            "side": "long",
            "qty": 1.0,
            "entry_price": 100.0,
            "opened_at_ms": 1,
            "source_role": "bootstrap_entry",
        }
    ]

    plan = build_inventory_grid_orders(
        runtime=runtime,
        bid_price=99.9,
        ask_price=100.1,
        step_price=0.7,
        per_order_notional=20.0,
        first_order_multiplier=1.0,
        threshold_position_notional=500.0,
        max_order_position_notional=800.0,
        max_position_notional=1200.0,
        buy_levels=3,
        tick_size=0.1,
        step_size=0.001,
        min_qty=0.001,
        min_notional=5.0,
        sell_levels=3,
    )

    assert [item["price"] for item in plan["sell_orders"]] == [100.7, 101.4, 102.1]
    assert all(item["role"] == "grid_exit" for item in plan["sell_orders"])


def test_spot_long_active_sell_levels_are_capped_by_held_qty() -> None:
    runtime = new_inventory_grid_runtime(market_type="spot")
    runtime["direction_state"] = "long_active"
    runtime["grid_anchor_price"] = 100.0
    runtime["position_lots"] = [
        {
            "lot_id": "l1",
            "side": "long",
            "qty": 0.25,
            "entry_price": 100.0,
            "opened_at_ms": 1,
            "source_role": "bootstrap_entry",
        }
    ]

    plan = build_inventory_grid_orders(
        runtime=runtime,
        bid_price=99.9,
        ask_price=100.1,
        step_price=0.7,
        per_order_notional=20.0,
        first_order_multiplier=1.0,
        threshold_position_notional=500.0,
        max_order_position_notional=800.0,
        max_position_notional=1200.0,
        buy_levels=3,
        tick_size=0.1,
        step_size=0.001,
        min_qty=0.001,
        min_notional=5.0,
        sell_levels=3,
    )

    assert [item["price"] for item in plan["sell_orders"]] == [100.7, 101.4]
    assert [item["qty"] for item in plan["sell_orders"]] == [0.198, 0.051]
    assert round(sum(item["qty"] for item in plan["sell_orders"]), 8) <= 0.25


def test_grid_exit_is_capped_to_held_qty_for_short_side() -> None:
    runtime = new_inventory_grid_runtime(market_type="futures")
    runtime["direction_state"] = "short_active"
    runtime["grid_anchor_price"] = 0.02
    runtime["position_lots"] = [
        {
            "lot_id": "s1",
            "side": "short",
            "qty": 100.0,
            "entry_price": 0.02,
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
        threshold_position_notional=5.0,
        max_order_position_notional=30.0,
        max_position_notional=40.0,
        tick_size=0.0001,
        step_size=1.0,
        min_qty=1.0,
        min_notional=0.1,
    )

    assert [item["role"] for item in plan["buy_orders"]] == ["grid_exit"]
    assert [item["qty"] for item in plan["buy_orders"]] == [100.0]


def test_threshold_reduce_only_caps_forced_reduce_after_grid_exit() -> None:
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

    assert [item["role"] for item in plan["sell_orders"]] == ["grid_exit", "forced_reduce"]
    assert [item["qty"] for item in plan["sell_orders"]] == [90.0, 110.0]

    replay_runtime = new_inventory_grid_runtime(market_type="futures")
    replay_runtime["direction_state"] = "long_active"
    replay_runtime["grid_anchor_price"] = 0.10
    replay_runtime["pair_credit_steps"] = 5
    replay_runtime["position_lots"] = [
        {
            "lot_id": "l1",
            "side": "long",
            "qty": 200.0,
            "entry_price": 0.10,
            "opened_at_ms": 1,
            "source_role": "bootstrap_entry",
        }
    ]

    for fill_time_ms, order in enumerate(plan["sell_orders"], start=1):
        apply_inventory_grid_fill(
            runtime=replay_runtime,
            role=order["role"],
            side=order["side"],
            price=order["price"],
            qty=order["qty"],
            fill_time_ms=fill_time_ms,
        )

    assert replay_runtime["direction_state"] == "flat"
    assert replay_runtime["position_lots"] == []


def test_threshold_reduce_only_skips_forced_reduce_below_exchange_mins() -> None:
    runtime = new_inventory_grid_runtime(market_type="futures")
    runtime["direction_state"] = "long_active"
    runtime["grid_anchor_price"] = 0.10
    runtime["position_lots"] = [
        {
            "lot_id": "l1",
            "side": "long",
            "qty": 101.0,
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
        threshold_position_notional=9.99,
        max_order_position_notional=30.0,
        max_position_notional=40.0,
        tick_size=0.0001,
        step_size=1.0,
        min_qty=1.0,
        min_notional=5.0,
    )

    assert all(item["role"] != "forced_reduce" for item in plan["sell_orders"])
    assert plan["forced_reduce_orders"] == []


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
        min_notional=0.1,
    )

    assert plan["tail_cleanup_active"] is True
    assert [item["role"] for item in plan["sell_orders"]] == ["tail_cleanup"]
    assert plan["buy_orders"] == []


def test_tail_cleanup_below_exchange_mins_is_skipped() -> None:
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
        min_notional=1.0,
    )

    assert plan["tail_cleanup_active"] is True
    assert plan["sell_orders"] == []
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
        min_notional=0.1,
    )

    assert plan["risk_state"] == "threshold_reduce_only"
    assert plan["tail_cleanup_active"] is True
    assert [item["role"] for item in plan["sell_orders"]] == ["tail_cleanup"]
    assert plan["buy_orders"] == []
    assert plan["forced_reduce_orders"] == []


def test_spot_long_active_keeps_multiple_buy_levels_below_anchor() -> None:
    runtime = new_inventory_grid_runtime(market_type="spot")
    runtime["direction_state"] = "long_active"
    runtime["grid_anchor_price"] = 100.0
    runtime["position_lots"] = [
        {
            "lot_id": "l1",
            "side": "long",
            "qty": 0.2,
            "entry_price": 100.0,
            "opened_at_ms": 1,
            "source_role": "bootstrap_entry",
        }
    ]

    plan = build_inventory_grid_orders(
        runtime=runtime,
        bid_price=99.9,
        ask_price=100.1,
        step_price=0.7,
        per_order_notional=20.0,
        first_order_multiplier=1.0,
        threshold_position_notional=1800.0,
        max_order_position_notional=200.0,
        max_position_notional=400.0,
        buy_levels=3,
        tick_size=0.1,
        step_size=0.001,
        min_qty=0.001,
        min_notional=5.0,
    )

    assert [item["role"] for item in plan["sell_orders"]] == ["grid_exit"]
    assert [item["price"] for item in plan["sell_orders"]] == [100.7]
    assert [item["role"] for item in plan["buy_orders"]] == ["grid_entry", "grid_entry", "grid_entry"]
    assert [item["price"] for item in plan["buy_orders"]] == [99.3, 98.6, 97.9]


def test_spot_flat_buy_levels_respect_opening_notional_cap() -> None:
    runtime = new_inventory_grid_runtime(market_type="spot")

    plan = build_inventory_grid_orders(
        runtime=runtime,
        bid_price=100.0,
        ask_price=100.2,
        step_price=0.7,
        per_order_notional=20.0,
        first_order_multiplier=1.0,
        threshold_position_notional=50.0,
        max_order_position_notional=45.0,
        max_position_notional=120.0,
        buy_levels=3,
        tick_size=0.1,
        step_size=0.001,
        min_qty=0.001,
        min_notional=5.0,
    )

    assert [item["price"] for item in plan["bootstrap_orders"]] == [100.0, 99.3]


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
