from grid_optimizer.spot_freeze_manager import (
    compute_deviation_loss,
    expected_short_position_now,
    ledger_totals,
    new_ledger,
    pending_signed_short_delta,
    reconcile_short_position,
)


def test_new_ledger_contains_pending_actions_and_zero_totals() -> None:
    ledger = new_ledger()

    assert ledger["schema"] == "spot_frozen_ledger_v1"
    assert ledger["long_lots"] == []
    assert ledger["short_lots"] == []
    assert ledger["pending_contract_actions"] == []
    assert ledger["frozen_long_qty"] == 0.0
    assert ledger["frozen_short_qty"] == 0.0


def test_ledger_totals_sums_lots_and_refreshes_summary_fields() -> None:
    ledger = new_ledger()
    ledger["long_lots"] = [{"qty": 10.0}, {"qty": "2.5"}]
    ledger["short_lots"] = [{"qty": 3.0}]

    assert ledger_totals(ledger) == (12.5, 3.0)
    assert ledger["frozen_long_qty"] == 12.5
    assert ledger["frozen_short_qty"] == 3.0


def test_pending_signed_short_delta_buy_reduces_short_sell_increases_short() -> None:
    ledger = new_ledger()
    ledger["pending_contract_actions"] = [
        {"side": "BUY", "position_side": "SHORT", "qty": 2.0, "reason": "freeze_long_hedge", "lot_id": "L1"},
        {"side": "SELL", "position_side": "SHORT", "qty": 1.0, "reason": "freeze_short_hedge", "lot_id": "S1"},
        {"side": "BUY", "position_side": "LONG", "qty": 9.0, "reason": "ignore", "lot_id": "bad"},
    ]

    assert pending_signed_short_delta(ledger) == -1.0


def test_expected_short_position_now_removes_pending_effect_from_ledger_expectation() -> None:
    ledger = new_ledger()
    ledger["long_lots"] = [{"qty": 10.0}]
    ledger["short_lots"] = [{"qty": 3.0}]
    ledger["pending_contract_actions"] = [
        {"side": "BUY", "position_side": "SHORT", "qty": 2.0, "reason": "freeze_long_hedge", "lot_id": "L1"},
        {"side": "SELL", "position_side": "SHORT", "qty": 1.0, "reason": "freeze_short_hedge", "lot_id": "S1"},
    ]

    assert expected_short_position_now(100.0, ledger) == 94.0


def test_reconcile_short_position_accepts_within_tolerance_and_rejects_drift() -> None:
    ledger = new_ledger()
    ledger["long_lots"] = [{"qty": 10.0}]
    ledger["short_lots"] = [{"qty": 3.0}]
    ledger["pending_contract_actions"] = [
        {"side": "BUY", "position_side": "SHORT", "qty": 2.0, "reason": "freeze_long_hedge", "lot_id": "L1"},
        {"side": "SELL", "position_side": "SHORT", "qty": 1.0, "reason": "freeze_short_hedge", "lot_id": "S1"},
    ]

    assert reconcile_short_position(100.0, 94.0, ledger, 0.01) == (True, 94.0)
    assert reconcile_short_position(100.0, 95.5, ledger, 0.01) == (False, 94.0)


def test_compute_deviation_loss_long_uses_highest_entry_price_lots_first() -> None:
    runtime = {
        "recovery_mode": "live",
        "synthetic_cost_unknown": False,
        "position_lots": [
            {"lot_id": "a", "side": "long", "qty": 3.0, "entry_price": 10.0},
            {"lot_id": "b", "side": "long", "qty": 2.0, "entry_price": 12.0},
            {"lot_id": "c", "side": "long", "qty": 5.0, "entry_price": 9.0},
        ],
    }

    result = compute_deviation_loss(runtime, 100.0, mid_price=10.0, deviation_side="long", deviation_qty=4.0)

    assert result.usable is True
    assert round(result.cost_avg, 6) == 11.0
    assert round(result.loss_ratio, 6) == round((11.0 - 10.0) / 11.0, 6)
    assert result.reason == "ok"


def test_compute_deviation_loss_short_uses_lowest_entry_price_lots_first() -> None:
    runtime = {
        "recovery_mode": "live",
        "synthetic_cost_unknown": False,
        "position_lots": [
            {"lot_id": "a", "side": "short", "qty": 2.0, "entry_price": 8.0},
            {"lot_id": "b", "side": "short", "qty": 4.0, "entry_price": 9.0},
        ],
    }

    result = compute_deviation_loss(runtime, 100.0, mid_price=10.0, deviation_side="short", deviation_qty=3.0)

    assert result.usable is True
    assert round(result.cost_avg, 6) == round((2.0 * 8.0 + 1.0 * 9.0) / 3.0, 6)
    assert result.loss_ratio > 0.0
    assert result.reason == "ok"


def test_compute_deviation_loss_rejects_unknown_cost_or_recovery_mode() -> None:
    runtime = {
        "recovery_mode": "live",
        "synthetic_cost_unknown": True,
        "position_lots": [{"lot_id": "a", "side": "long", "qty": 3.0, "entry_price": 10.0}],
    }

    result = compute_deviation_loss(runtime, 100.0, mid_price=9.0, deviation_side="long", deviation_qty=1.0)

    assert result.usable is False
    assert result.loss_ratio == 0.0
    assert result.cost_avg == 0.0
    assert result.reason == "synthetic_cost_unknown"

    runtime["synthetic_cost_unknown"] = False
    runtime["recovery_mode"] = "conservative_reduce_only"

    result = compute_deviation_loss(runtime, 100.0, mid_price=9.0, deviation_side="long", deviation_qty=1.0)

    assert result.usable is False
    assert result.loss_ratio == 0.0
    assert result.cost_avg == 0.0
    assert result.reason == "recovery_mode_not_live"


def test_compute_deviation_loss_rejects_synthetic_cost_unknown_lot() -> None:
    runtime = {
        "recovery_mode": "live",
        "synthetic_cost_unknown": False,
        "position_lots": [
            {"lot_id": "a", "side": "long", "qty": 3.0, "entry_price": 10.0, "freeze_reason": "synthetic_cost_unknown"}
        ],
    }

    result = compute_deviation_loss(runtime, 100.0, mid_price=9.0, deviation_side="long", deviation_qty=1.0)

    assert result.usable is False
    assert result.loss_ratio == 0.0
    assert result.cost_avg == 0.0
    assert result.reason == "lot_cost_unknown"


def test_compute_deviation_loss_rejects_missing_lots_or_zero_cost() -> None:
    missing = {
        "recovery_mode": "live",
        "synthetic_cost_unknown": False,
        "position_lots": [{"lot_id": "a", "side": "long", "qty": 0.5, "entry_price": 10.0}],
    }

    result = compute_deviation_loss(missing, 100.0, mid_price=9.0, deviation_side="long", deviation_qty=1.0)

    assert result.usable is False
    assert result.reason == "insufficient_lots"

    zero_cost = {
        "recovery_mode": "live",
        "synthetic_cost_unknown": False,
        "position_lots": [{"lot_id": "a", "side": "long", "qty": 1.0, "entry_price": 0.0}],
    }

    result = compute_deviation_loss(zero_cost, 100.0, mid_price=9.0, deviation_side="long", deviation_qty=1.0)

    assert result.usable is False
    assert result.loss_ratio == 0.0
    assert result.cost_avg == 0.0
    assert result.reason == "invalid_cost"
