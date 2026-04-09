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
        current_position_qty=150.0,
    )

    assert runtime["recovery_mode"] == "conservative_reduce_only"
    assert "position_qty_mismatch" in runtime["recovery_errors"]
