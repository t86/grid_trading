from grid_optimizer.spot_freeze_manager import (
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
