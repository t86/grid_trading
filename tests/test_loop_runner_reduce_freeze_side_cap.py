from grid_optimizer.loop_runner import (
    _apply_best_quote_reduce_freeze,
    _cap_best_quote_reduce_orders_to_managed_inventory,
)


def test_reduce_freeze_blocks_side_when_cap_remaining_below_min_notional() -> None:
    state = {
        "best_quote_volume_ledger": {
            "initialized": True,
            "sync_ok": True,
            "long_lots": [
                {
                    "qty": 100.0,
                    "price": 1.0,
                    "opened_at": "2026-06-24T00:00:00+00:00",
                    "order_id": 1,
                    "client_order_id": "long-1",
                }
            ],
            "short_lots": [
                {
                    "qty": 100.0,
                    "price": 0.8,
                    "opened_at": "2026-06-24T00:00:00+00:00",
                    "order_id": 2,
                    "client_order_id": "short-1",
                }
            ],
        }
    }
    plan = {
        "sell_orders": [{"role": "best_quote_reduce_long"}],
        "buy_orders": [{"role": "best_quote_reduce_short"}],
    }
    report = {
        "frozen_long_notional": 0.0,
        "frozen_short_notional": 0.0,
        "frozen_long_qty": 0.0,
        "frozen_short_qty": 0.0,
    }

    result = _apply_best_quote_reduce_freeze(
        state=state,
        plan=plan,
        report=report,
        enabled=True,
        threshold_loss_ratio=0.01,
        min_notional=6.0,
        confirm_cycles=1,
        frozen_total_cap_notional=2400.0,
        frozen_long_cap_notional=0.01,
        frozen_short_cap_notional=2400.0,
        current_long_qty=100.0,
        current_short_qty=100.0,
        current_long_avg_price=1.0,
        current_short_avg_price=0.8,
        mid_price=0.9,
        bq_ledger_report={
            "initialized": True,
            "long_qty": 100.0,
            "short_qty": 100.0,
            "long_avg_price": 1.0,
            "short_avg_price": 0.8,
        },
    )

    side_cap = result["frozen_side_cap"]
    assert side_cap["blocks_new_freeze"] is True
    assert "LONG" in side_cap["blocked_sides"]
    assert "SHORT" not in side_cap["blocked_sides"]
    assert state.get("best_quote_frozen_inventory", {}).get("long_qty", 0.0) == 0.0
    assert not any(event.get("side") == "LONG" for event in result.get("events", []))


def test_active_pair_reduce_has_priority_when_managed_cap_is_tight() -> None:
    plan = {
        "sell_orders": [
            {
                "side": "SELL",
                "position_side": "LONG",
                "role": "best_quote_reduce_long",
                "price": 1.0,
                "qty": 20.0,
                "notional": 20.0,
                "force_reduce_only": True,
            },
            {
                "side": "SELL",
                "position_side": "LONG",
                "role": "best_quote_active_pair_reduce_long",
                "price": 1.0,
                "qty": 50.0,
                "notional": 50.0,
                "force_reduce_only": True,
            },
        ],
        "buy_orders": [
            {
                "side": "BUY",
                "position_side": "SHORT",
                "role": "best_quote_reduce_short",
                "price": 1.0,
                "qty": 20.0,
                "notional": 20.0,
                "force_reduce_only": True,
            },
            {
                "side": "BUY",
                "position_side": "SHORT",
                "role": "best_quote_active_pair_reduce_short",
                "price": 1.0,
                "qty": 50.0,
                "notional": 50.0,
                "force_reduce_only": True,
            },
        ],
    }
    report = {
        "managed_long_qty": 50.0,
        "managed_short_qty": 50.0,
        "frozen_long_qty": 100.0,
        "frozen_short_qty": 100.0,
        "position_long_qty": 150.0,
        "position_short_qty": 150.0,
    }

    cap = _cap_best_quote_reduce_orders_to_managed_inventory(
        plan=plan,
        report=report,
        step_size=1.0,
        min_qty=1.0,
        min_notional=5.0,
    )

    assert [order["role"] for order in plan["sell_orders"]] == ["best_quote_active_pair_reduce_long"]
    assert [order["role"] for order in plan["buy_orders"]] == ["best_quote_active_pair_reduce_short"]
    assert cap["dropped_long_orders"] == 1
    assert cap["dropped_short_orders"] == 1
