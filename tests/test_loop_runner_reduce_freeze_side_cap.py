from grid_optimizer.loop_runner import (
    _apply_best_quote_reduce_freeze,
    _cap_best_quote_reduce_orders_to_managed_inventory,
    _transfer_best_quote_volume_to_frozen,
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


def test_reduce_freeze_clamps_transfer_to_total_cap_remaining_notional() -> None:
    state = {
        "best_quote_frozen_inventory": {
            "long_lots": [{"qty": 735.0, "entry_price": 1.0}],
            "short_lots": [],
        },
        "best_quote_volume_ledger": {
            "initialized": True,
            "sync_ok": True,
            "long_lots": [],
            "short_lots": [{"qty": 100.0, "price": 0.8}],
        },
    }
    report = {
        "frozen_long_notional": 735.0,
        "frozen_short_notional": 0.0,
        "frozen_long_qty": 735.0,
        "frozen_short_qty": 0.0,
    }

    result = _apply_best_quote_reduce_freeze(
        state=state,
        plan={"sell_orders": [], "buy_orders": [{"role": "best_quote_reduce_short"}]},
        report=report,
        enabled=True,
        threshold_loss_ratio=0.01,
        min_notional=6.0,
        confirm_cycles=1,
        frozen_total_cap_notional=800.0,
        frozen_long_cap_notional=800.0,
        frozen_short_cap_notional=800.0,
        current_long_qty=735.0,
        current_short_qty=100.0,
        current_long_avg_price=1.0,
        current_short_avg_price=0.8,
        mid_price=1.0,
        profitable_pair_gate_enabled=False,
        bq_ledger_report={
            "initialized": True,
            "long_qty": 0.0,
            "short_qty": 100.0,
            "long_avg_price": 0.0,
            "short_avg_price": 0.8,
        },
    )

    frozen = state["best_quote_frozen_inventory"]
    frozen_total_qty = sum(lot["qty"] for lot in frozen["long_lots"] + frozen["short_lots"])
    assert frozen_total_qty == 800.0
    assert result["events"][0]["qty"] == 65.0
    assert result["frozen_total_cap"]["frozen_total_notional"] == 800.0
    assert result["frozen_total_cap"]["remaining_notional"] == 0.0
    assert result["frozen_total_cap"]["blocks_new_freeze"] is True


def test_reduce_freeze_rechecks_total_cap_between_two_sides() -> None:
    state = {
        "best_quote_frozen_inventory": {
            "long_lots": [{"qty": 700.0, "entry_price": 1.0}],
            "short_lots": [],
        },
        "best_quote_volume_ledger": {
            "initialized": True,
            "sync_ok": True,
            "long_lots": [{"qty": 100.0, "price": 1.2}],
            "short_lots": [{"qty": 100.0, "price": 0.8}],
        },
    }

    result = _apply_best_quote_reduce_freeze(
        state=state,
        plan={
            "sell_orders": [{"role": "best_quote_reduce_long"}],
            "buy_orders": [{"role": "best_quote_reduce_short"}],
        },
        report={
            "frozen_long_notional": 700.0,
            "frozen_short_notional": 0.0,
            "frozen_long_qty": 700.0,
            "frozen_short_qty": 0.0,
        },
        enabled=True,
        threshold_loss_ratio=0.01,
        min_notional=6.0,
        confirm_cycles=1,
        frozen_total_cap_notional=800.0,
        frozen_long_cap_notional=800.0,
        frozen_short_cap_notional=800.0,
        current_long_qty=800.0,
        current_short_qty=100.0,
        current_long_avg_price=1.2,
        current_short_avg_price=0.8,
        mid_price=1.0,
        profitable_pair_gate_enabled=False,
        bq_ledger_report={
            "initialized": True,
            "long_qty": 100.0,
            "short_qty": 100.0,
            "long_avg_price": 1.2,
            "short_avg_price": 0.8,
        },
    )

    frozen = state["best_quote_frozen_inventory"]
    frozen_total_qty = sum(lot["qty"] for lot in frozen["long_lots"] + frozen["short_lots"])
    assert frozen_total_qty == 800.0
    assert [event["side"] for event in result["events"]] == ["LONG"]


def test_transfer_migrates_legacy_aggregate_before_enforcing_total_cap() -> None:
    state = {
        "best_quote_frozen_inventory": {
            "short_qty": 735.0,
            "short_entry_price": 0.8,
        },
        "best_quote_volume_ledger": {
            "short_lots": [{"qty": 100.0, "price": 0.8}],
        },
    }

    first = _transfer_best_quote_volume_to_frozen(
        state=state,
        side="short",
        qty=100.0,
        reason="reduce_loss_ratio_threshold",
        loss_ratio=0.05,
        mid_price=1.0,
        frozen_total_cap_notional=800.0,
    )
    second = _transfer_best_quote_volume_to_frozen(
        state=state,
        side="short",
        qty=35.0,
        reason="reduce_loss_ratio_threshold",
        loss_ratio=0.05,
        mid_price=1.0,
        frozen_total_cap_notional=800.0,
    )

    frozen = state["best_quote_frozen_inventory"]
    assert sum(lot["qty"] for lot in frozen["short_lots"]) == 800.0
    assert frozen["short_lots"][0]["entry_price"] == 0.8
    assert first["transferred_qty"] == 65.0
    assert second["transferred_qty"] == 0.0


def test_transfer_does_not_blame_total_cap_when_band_budget_blocks() -> None:
    state = {
        "best_quote_volume_ledger": {
            "long_lots": [{"qty": 10.0, "price": 1.0}],
        },
    }

    result = _transfer_best_quote_volume_to_frozen(
        state=state,
        side="long",
        qty=10.0,
        reason="reduce_loss_ratio_threshold",
        loss_ratio=0.05,
        mid_price=1.0,
        band_budget_enabled=True,
        band_budget_price_ratio=0.1,
        band_budget_base_notional=0.0,
        frozen_total_cap_notional=800.0,
    )

    assert result["transferred_qty"] == 0.0
    assert result["band_budget"]["blocked_reason"] == "invalid_band_budget_config"
    assert result["total_cap"]["blocked_reason"] is None


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
