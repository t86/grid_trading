from grid_optimizer.loop_runner import _apply_best_quote_reduce_freeze


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
