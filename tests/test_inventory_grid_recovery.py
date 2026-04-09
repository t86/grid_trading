from grid_optimizer.inventory_grid_recovery import rebuild_inventory_grid_runtime


def test_rebuild_runtime_recovers_anchor_and_lots_from_strategy_trades() -> None:
    runtime = rebuild_inventory_grid_runtime(
        market_type="futures",
        trades=[
            {"id": 1, "time": 1000, "price": "0.1000", "qty": "400", "side": "BUY", "orderId": 11},
            {"id": 2, "time": 2000, "price": "0.1100", "qty": "100", "side": "SELL", "orderId": 12},
        ],
        order_refs={
            "11": {"role": "bootstrap_entry", "side": "BUY"},
            "12": {"role": "grid_exit", "side": "SELL"},
        },
        step_price=0.01,
    )

    assert runtime["direction_state"] == "long_active"
    assert runtime["grid_anchor_price"] == 0.11
    assert runtime["pair_credit_steps"] == 0
    assert runtime["position_lots"][0]["qty"] == 300.0


def test_rebuild_runtime_marks_conservative_mode_when_position_cannot_be_explained() -> None:
    runtime = rebuild_inventory_grid_runtime(
        market_type="spot",
        trades=[],
        order_refs={},
        step_price=0.01,
        current_position_qty=50.0,
    )

    assert runtime["recovery_mode"] == "conservative_reduce_only"
    assert "missing_strategy_trade_history" in runtime["recovery_errors"]


def test_rebuild_runtime_marks_conservative_mode_when_both_bootstrap_sides_fill() -> None:
    runtime = rebuild_inventory_grid_runtime(
        market_type="futures",
        trades=[
            {"id": 1, "time": 1000, "price": "0.1000", "qty": "100", "side": "BUY", "orderId": 11},
            {"id": 2, "time": 1001, "price": "0.1001", "qty": "100", "side": "SELL", "orderId": 12},
        ],
        order_refs={
            "11": {"role": "bootstrap_entry", "side": "BUY"},
            "12": {"role": "bootstrap_entry", "side": "SELL"},
        },
        step_price=0.01,
    )

    assert runtime["recovery_mode"] == "conservative_reduce_only"
    assert "conflicting_bootstrap_fills" in runtime["recovery_errors"]


def test_rebuild_runtime_marks_conservative_mode_when_order_ref_is_missing() -> None:
    runtime = rebuild_inventory_grid_runtime(
        market_type="futures",
        trades=[
            {"id": 1, "time": 1000, "price": "0.1000", "qty": "100", "side": "BUY", "orderId": 11},
        ],
        order_refs={},
        step_price=0.01,
        current_position_qty=100.0,
    )

    assert runtime["recovery_mode"] == "conservative_reduce_only"
    assert "missing_order_ref" in runtime["recovery_errors"]


def test_rebuild_runtime_marks_conservative_mode_when_order_ref_is_unusable() -> None:
    runtime = rebuild_inventory_grid_runtime(
        market_type="futures",
        trades=[
            {"id": 1, "time": 1000, "price": "0.1000", "qty": "100", "side": "BUY", "orderId": 11},
        ],
        order_refs={
            "11": {"role": "bootstrap_entry"},
        },
        step_price=0.01,
        current_position_qty=100.0,
    )

    assert runtime["recovery_mode"] == "conservative_reduce_only"
    assert "unusable_order_ref" in runtime["recovery_errors"]


def test_rebuild_runtime_marks_conservative_mode_when_recovered_qty_does_not_match_position() -> None:
    runtime = rebuild_inventory_grid_runtime(
        market_type="futures",
        trades=[
            {"id": 1, "time": 1000, "price": "0.1000", "qty": "100", "side": "BUY", "orderId": 11},
        ],
        order_refs={
            "11": {"role": "bootstrap_entry", "side": "BUY"},
        },
        step_price=0.01,
        current_position_qty=0.0,
    )

    assert runtime["recovery_mode"] == "conservative_reduce_only"
    assert "position_qty_mismatch" in runtime["recovery_errors"]


def test_rebuild_runtime_preserves_replayed_state_before_conflicting_bootstrap_fill() -> None:
    runtime = rebuild_inventory_grid_runtime(
        market_type="futures",
        trades=[
            {"id": 1, "time": 1000, "price": "0.1000", "qty": "100", "side": "BUY", "orderId": 11},
            {"id": 2, "time": 2000, "price": "0.1100", "qty": "50", "side": "SELL", "orderId": 12},
        ],
        order_refs={
            "11": {"role": "bootstrap_entry", "side": "BUY"},
            "12": {"role": "bootstrap_entry", "side": "SELL"},
        },
        step_price=0.01,
    )

    assert runtime["recovery_mode"] == "conservative_reduce_only"
    assert runtime["grid_anchor_price"] == 0.1
    assert runtime["last_strategy_trade_time_ms"] == 2000
    assert runtime["direction_state"] == "long_active"
    assert runtime["position_lots"][0]["qty"] == 50.0
    assert "conflicting_bootstrap_fills" in runtime["recovery_errors"]


def test_rebuild_runtime_preserves_flipped_state_when_conflicting_bootstrap_exceeds_inventory() -> None:
    runtime = rebuild_inventory_grid_runtime(
        market_type="futures",
        trades=[
            {"id": 1, "time": 1000, "price": "0.1000", "qty": "100", "side": "BUY", "orderId": 11},
            {"id": 2, "time": 2000, "price": "0.1100", "qty": "150", "side": "SELL", "orderId": 12},
        ],
        order_refs={
            "11": {"role": "bootstrap_entry", "side": "BUY"},
            "12": {"role": "bootstrap_entry", "side": "SELL"},
        },
        step_price=0.01,
        current_position_qty=50.0,
    )

    assert runtime["recovery_mode"] == "conservative_reduce_only"
    assert runtime["direction_state"] == "short_active"
    assert runtime["grid_anchor_price"] == 0.11
    assert runtime["last_strategy_trade_time_ms"] == 2000
    assert runtime["position_lots"][0]["side"] == "short"
    assert runtime["position_lots"][0]["qty"] == 50.0
    assert "conflicting_bootstrap_fills" in runtime["recovery_errors"]


def test_rebuild_runtime_keeps_live_state_for_valid_two_cycle_futures_sequence() -> None:
    runtime = rebuild_inventory_grid_runtime(
        market_type="futures",
        trades=[
            {"id": 1, "time": 1000, "price": "0.1000", "qty": "100", "side": "BUY", "orderId": 11},
            {"id": 2, "time": 2000, "price": "0.0900", "qty": "100", "side": "SELL", "orderId": 12},
            {"id": 3, "time": 3000, "price": "0.1200", "qty": "50", "side": "SELL", "orderId": 13},
        ],
        order_refs={
            "11": {"role": "bootstrap_entry", "side": "BUY"},
            "12": {"role": "tail_cleanup", "side": "SELL"},
            "13": {"role": "bootstrap_entry", "side": "SELL"},
        },
        step_price=0.01,
        current_position_qty=50.0,
    )

    assert runtime["recovery_mode"] == "live"
    assert runtime["direction_state"] == "short_active"
    assert runtime["grid_anchor_price"] == 0.12
    assert runtime["last_strategy_trade_time_ms"] == 3000
    assert runtime["position_lots"][0]["side"] == "short"
    assert runtime["position_lots"][0]["qty"] == 50.0
