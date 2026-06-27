from grid_optimizer.spot_freeze_manager import (
    _round_down_qty,
    compute_deviation_loss,
    expected_short_position_now,
    freeze_cycle,
    FreezeConfig,
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


def test_round_down_qty_avoids_float_precision_tail() -> None:
    assert str(_round_down_qty(0.0262, 0.0001)) == "0.0262"
    assert str(_round_down_qty(0.0262, 0.001)) == "0.026"


def test_ledger_totals_sums_lots_and_refreshes_summary_fields() -> None:
    ledger = new_ledger()
    ledger["long_lots"] = [{"qty": 10.0}, {"qty": "2.5"}]
    ledger["short_lots"] = [{"qty": 3.0}]

    assert ledger_totals(ledger) == (12.5, 3.0)
    assert ledger["frozen_long_qty"] == 12.5
    assert ledger["frozen_short_qty"] == 3.0


def test_pending_signed_short_delta_tracks_short_side_actions() -> None:
    ledger = new_ledger()
    ledger["pending_contract_actions"] = [
        {"side": "SELL", "position_side": "SHORT", "qty": 2.0, "reason": "freeze_long_hedge", "lot_id": "L1"},
        {"side": "SELL", "position_side": "SHORT", "qty": 1.0, "reason": "freeze_short_hedge", "lot_id": "S1"},
        {"side": "BUY", "position_side": "LONG", "qty": 9.0, "reason": "ignore", "lot_id": "bad"},
    ]

    assert pending_signed_short_delta(ledger) == 3.0


def test_expected_short_position_now_removes_pending_effect_from_ledger_expectation() -> None:
    ledger = new_ledger()
    ledger["long_lots"] = [{"qty": 10.0}]
    ledger["short_lots"] = [{"qty": 3.0}]
    ledger["pending_contract_actions"] = [
        {"side": "SELL", "position_side": "SHORT", "qty": 2.0, "reason": "freeze_long_hedge", "lot_id": "L1"},
        {"side": "SELL", "position_side": "SHORT", "qty": 1.0, "reason": "freeze_short_hedge", "lot_id": "S1"},
    ]

    assert expected_short_position_now(100.0, ledger) == 100.0


def test_reconcile_short_position_accepts_within_tolerance_and_rejects_drift() -> None:
    ledger = new_ledger()
    ledger["long_lots"] = [{"qty": 10.0}]
    ledger["short_lots"] = [{"qty": 3.0}]
    ledger["pending_contract_actions"] = [
        {"side": "SELL", "position_side": "SHORT", "qty": 2.0, "reason": "freeze_long_hedge", "lot_id": "L1"},
        {"side": "SELL", "position_side": "SHORT", "qty": 1.0, "reason": "freeze_short_hedge", "lot_id": "S1"},
    ]

    assert reconcile_short_position(100.0, 100.0, ledger, 0.01) == (True, 100.0)
    assert reconcile_short_position(100.0, 101.5, ledger, 0.01) == (False, 100.0)


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


class OrderRecorder:
    def __init__(self, *, fail_contract: bool = False) -> None:
        self.fail_contract = fail_contract
        self.spot_orders: list[dict] = []
        self.contract_orders: list[dict] = []

    def spot(self, **kwargs: object) -> dict[str, str]:
        self.spot_orders.append(dict(kwargs))
        qty = float(kwargs["qty"])
        return {"executedQty": str(qty), "cummulativeQuoteQty": str(qty * 10.0)}

    def contract(self, **kwargs: object) -> dict[str, str]:
        self.contract_orders.append(dict(kwargs))
        if self.fail_contract:
            raise RuntimeError("contract rejected")
        return {"executedQty": str(kwargs["qty"])}


def enabled_config(**overrides: object) -> FreezeConfig:
    values = {
        "enabled": True,
        "deviation_notional": 50.0,
        "min_loss_ratio": 0.05,
        "max_per_cycle_notional": 100.0,
        "total_cap_notional": 1000.0,
    }
    values.update(overrides)
    return FreezeConfig(**values)


def run_cycle(
    *,
    ledger: dict | None = None,
    spot_inventory_qty: float = 120.0,
    mark_price: float = 10.0,
    contract_short_qty: float = 100.0,
    base_hedge_qty: float = 100.0,
    loss_ratio_usable: bool = True,
    deviation_loss_ratio: float = 0.10,
    config: FreezeConfig | None = None,
    recorder: OrderRecorder | None = None,
    dry_run: bool = False,
) -> tuple[dict, OrderRecorder]:
    rec = recorder or OrderRecorder()
    result = freeze_cycle(
        symbol="WLDUSDT",
        neutral_base_qty=100.0,
        spot_inventory_qty=spot_inventory_qty,
        mid_price=10.0,
        mark_price=mark_price,
        ledger=ledger or new_ledger(),
        contract_short_qty=contract_short_qty,
        base_hedge_qty=base_hedge_qty,
        deviation_loss_ratio=deviation_loss_ratio,
        loss_ratio_usable=loss_ratio_usable,
        config=config or enabled_config(),
        qty_step=0.001,
        tolerance_qty=0.01,
        now="2026-06-23T12:00:00Z",
        place_spot=rec.spot,
        place_contract=rec.contract,
        dry_run=dry_run,
    )
    return result, rec


def test_freeze_long_deviation_freezes_spot_only_without_contract_order() -> None:
    result, rec = run_cycle()

    assert rec.spot_orders == []
    assert rec.contract_orders == []
    assert result["actions"][0] == {"type": "freeze", "side": "long", "qty": 10.0, "mode": "spot_only"}
    assert result["ledger"]["long_lots"][0]["qty"] == 10.0
    assert result["ledger"]["long_lots"][0]["cost_price"] == 10.0
    assert result["ledger"]["long_lots"][0]["hedge_pending"] is False
    assert result["ledger"]["pending_contract_actions"] == []


def test_freeze_long_deviation_does_not_require_existing_short_hedge() -> None:
    result, rec = run_cycle(
        contract_short_qty=0.0,
        base_hedge_qty=0.0,
        config=enabled_config(deviation_notional=1.0, min_loss_ratio=0.0),
    )

    assert rec.spot_orders == []
    assert rec.contract_orders == []
    assert result["ledger"]["long_lots"][0]["qty"] == 10.0
    assert result["ledger"]["pending_contract_actions"] == []
    assert result["alerts"] == []


def test_freeze_short_deviation_buys_spot_and_sells_short_position_side() -> None:
    result, rec = run_cycle(spot_inventory_qty=80.0)

    assert rec.spot_orders[0]["side"] == "BUY"
    assert rec.contract_orders[0]["side"] == "SELL"
    assert rec.contract_orders[0]["position_side"] == "SHORT"
    assert result["ledger"]["short_lots"][0]["qty"] == 10.0


def test_freeze_short_deviation_respects_contract_short_limit() -> None:
    ledger = new_ledger()
    ledger["short_lots"] = [{"lot_id": "S1", "qty": 9.0, "cost_price": 10.0, "frozen_at": "t", "frozen_mid": 10.0, "hedge_pending": False}]
    ledger_totals(ledger)

    result, rec = run_cycle(
        ledger=ledger,
        spot_inventory_qty=80.0,
        contract_short_qty=9.0,
        base_hedge_qty=0.0,
        config=enabled_config(max_contract_short_notional=90.0),
    )

    assert rec.spot_orders == []
    assert rec.contract_orders == []
    assert result["ledger"]["short_lots"] == ledger["short_lots"]
    assert "short_hedge_capacity_exhausted" in result["alerts"]


def test_freeze_short_deviation_contract_cap_counts_frozen_short_not_base_hedge() -> None:
    rec = OrderRecorder()

    result = freeze_cycle(
        symbol="XPLUSDT",
        neutral_base_qty=4800.0,
        spot_inventory_qty=4121.8,
        mid_price=0.08875,
        mark_price=0.08875,
        ledger=new_ledger(),
        contract_short_qty=4800.0,
        base_hedge_qty=4800.0,
        deviation_loss_ratio=0.10,
        loss_ratio_usable=True,
        config=enabled_config(
            deviation_notional=50.0,
            min_loss_ratio=0.0,
            max_per_cycle_notional=60.0,
            total_cap_notional=240.0,
            max_contract_short_notional=240.0,
        ),
        qty_step=0.1,
        tolerance_qty=0.01,
        now="2026-06-24T12:00:00Z",
        place_spot=rec.spot,
        place_contract=rec.contract,
        dry_run=False,
    )

    assert rec.spot_orders == [{"symbol": "XPLUSDT", "side": "BUY", "qty": 676.0}]
    assert rec.contract_orders == [
        {
            "symbol": "XPLUSDT",
            "side": "SELL",
            "position_side": "SHORT",
            "qty": 676.0,
        }
    ]
    assert result["ledger"]["short_lots"][0]["qty"] == 676.0
    assert result["alerts"] == []


def test_freeze_skips_when_loss_ratio_unusable() -> None:
    result, rec = run_cycle(loss_ratio_usable=False)

    assert rec.spot_orders == []
    assert rec.contract_orders == []
    assert result["ledger"]["long_lots"] == []
    assert "cost_not_usable" in result["alerts"]


def test_freeze_skips_when_deviation_or_loss_below_threshold() -> None:
    result, rec = run_cycle(spot_inventory_qty=102.0)

    assert rec.spot_orders == []
    assert rec.contract_orders == []
    assert result["actions"] == []

    result, rec = run_cycle(deviation_loss_ratio=0.01)

    assert rec.spot_orders == []
    assert rec.contract_orders == []
    assert result["actions"] == []


def test_freeze_respects_total_cap_across_long_and_short_lots() -> None:
    ledger = new_ledger()
    ledger["long_lots"] = [{"lot_id": "L1", "qty": 40.0, "cost_price": 10.0, "frozen_at": "t", "frozen_mid": 10.0, "hedge_pending": False}]
    ledger["short_lots"] = [{"lot_id": "S1", "qty": 60.0, "cost_price": 10.0, "frozen_at": "t", "frozen_mid": 10.0, "hedge_pending": False}]
    ledger_totals(ledger)

    result, rec = run_cycle(ledger=ledger, contract_short_qty=120.0, config=enabled_config(total_cap_notional=1000.0))

    assert rec.spot_orders == []
    assert rec.contract_orders == []
    assert "total_cap_reached" in result["alerts"]


def test_freeze_long_respects_side_total_cap() -> None:
    ledger = new_ledger()
    ledger["long_lots"] = [{"lot_id": "L1", "qty": 4.0, "cost_price": 10.0, "frozen_at": "t", "frozen_mid": 10.0, "hedge_pending": False}]
    ledger_totals(ledger)

    result, rec = run_cycle(
        ledger=ledger,
        contract_short_qty=96.0,
        config=enabled_config(
            total_cap_notional=1000.0,
            long_total_cap_notional=30.0,
            short_total_cap_notional=500.0,
        ),
    )

    assert rec.spot_orders == []
    assert rec.contract_orders == []
    assert result["ledger"]["long_lots"] == ledger["long_lots"]
    assert "long_total_cap_reached" in result["alerts"]


def test_freeze_short_respects_side_total_cap_without_blocking_long_cap() -> None:
    ledger = new_ledger()
    ledger["long_lots"] = [{"lot_id": "L1", "qty": 4.0, "cost_price": 10.0, "frozen_at": "t", "frozen_mid": 10.0, "hedge_pending": False}]
    ledger["short_lots"] = [{"lot_id": "S1", "qty": 50.0, "cost_price": 10.0, "frozen_at": "t", "frozen_mid": 10.0, "hedge_pending": False}]
    ledger_totals(ledger)

    result, rec = run_cycle(
        ledger=ledger,
        spot_inventory_qty=80.0,
        contract_short_qty=146.0,
        config=enabled_config(
            total_cap_notional=1000.0,
            long_total_cap_notional=30.0,
            short_total_cap_notional=500.0,
            max_contract_short_notional=1000.0,
        ),
    )

    assert rec.spot_orders == []
    assert rec.contract_orders == []
    assert result["ledger"]["short_lots"] == ledger["short_lots"]
    assert "short_total_cap_reached" in result["alerts"]


def test_freeze_contract_failure_records_pending_and_marks_lot_pending() -> None:
    result, rec = run_cycle(spot_inventory_qty=80.0, recorder=OrderRecorder(fail_contract=True))

    assert rec.spot_orders[0]["side"] == "BUY"
    assert rec.contract_orders[0]["side"] == "SELL"
    assert result["ledger"]["pending_contract_actions"][0]["side"] == "SELL"
    assert result["ledger"]["pending_contract_actions"][0]["position_side"] == "SHORT"
    assert result["ledger"]["short_lots"][0]["hedge_pending"] is True
    assert "contract_failed" in result["alerts"]


def test_pending_actions_are_repaired_before_new_freeze() -> None:
    ledger = new_ledger()
    ledger["long_lots"] = [{"lot_id": "L1", "qty": 10.0, "cost_price": 10.0, "frozen_at": "t", "frozen_mid": 10.0, "hedge_pending": True}]
    ledger["pending_contract_actions"] = [
        {"side": "BUY", "position_side": "SHORT", "qty": 10.0, "reason": "freeze_long_hedge", "lot_id": "L1"}
    ]
    ledger_totals(ledger)

    result, rec = run_cycle(ledger=ledger, contract_short_qty=100.0, spot_inventory_qty=100.0)

    assert rec.contract_orders == [{"symbol": "WLDUSDT", "side": "BUY", "qty": 10.0, "position_side": "SHORT"}]
    assert rec.spot_orders == []
    assert result["ledger"]["pending_contract_actions"] == []
    assert result["ledger"]["long_lots"][0]["hedge_pending"] is False


def test_pending_repair_notifies_ledger_change_after_success() -> None:
    ledger = new_ledger()
    ledger["long_lots"] = [{"lot_id": "L1", "qty": 10.0, "cost_price": 10.0, "frozen_at": "t", "frozen_mid": 10.0, "hedge_pending": True}]
    ledger["pending_contract_actions"] = [
        {"side": "BUY", "position_side": "SHORT", "qty": 10.0, "reason": "freeze_long_hedge", "lot_id": "L1"}
    ]
    ledger_totals(ledger)
    snapshots: list[dict] = []

    freeze_cycle(
        symbol="WLDUSDT",
        neutral_base_qty=100.0,
        spot_inventory_qty=100.0,
        mid_price=10.0,
        mark_price=10.0,
        ledger=ledger,
        contract_short_qty=100.0,
        base_hedge_qty=100.0,
        deviation_loss_ratio=0.10,
        loss_ratio_usable=True,
        config=enabled_config(),
        qty_step=0.001,
        tolerance_qty=0.01,
        now="2026-06-23T12:00:00Z",
        place_spot=OrderRecorder().spot,
        place_contract=OrderRecorder().contract,
        dry_run=False,
        on_ledger_change=lambda changed: snapshots.append(
            {
                "pending_contract_actions": list(changed["pending_contract_actions"]),
                "hedge_pending": changed["long_lots"][0]["hedge_pending"],
                "frozen_long_qty": changed["frozen_long_qty"],
            }
        ),
    )

    assert snapshots == [{"pending_contract_actions": [], "hedge_pending": False, "frozen_long_qty": 10.0}]


def test_pending_repair_failure_keeps_pending_and_blocks_new_freeze() -> None:
    ledger = new_ledger()
    ledger["long_lots"] = [{"lot_id": "L1", "qty": 10.0, "cost_price": 10.0, "frozen_at": "t", "frozen_mid": 10.0, "hedge_pending": True}]
    ledger["pending_contract_actions"] = [
        {"side": "BUY", "position_side": "SHORT", "qty": 10.0, "reason": "freeze_long_hedge", "lot_id": "L1"}
    ]
    ledger_totals(ledger)

    result, rec = run_cycle(ledger=ledger, contract_short_qty=100.0, recorder=OrderRecorder(fail_contract=True))

    assert len(rec.contract_orders) == 1
    assert rec.spot_orders == []
    assert result["ledger"]["pending_contract_actions"]
    assert result["ledger"]["long_lots"][0]["hedge_pending"] is True
    assert "pending_contract_failed" in result["alerts"]


def test_reconcile_drift_is_recorded_without_blocking_freeze_cycle() -> None:
    result, rec = run_cycle(contract_short_qty=99.0)

    assert result["reconcile_ok"] is False
    assert rec.spot_orders == []
    assert rec.contract_orders == []
    assert "reconcile_drift" in result["alerts"][0]
    assert result["ledger"]["last_contract_short_qty"] == 99.0
    assert result["ledger"]["last_expected_short_qty"] == 100.0
    assert result["ledger"]["last_short_drift_qty"] == -1.0


def test_dry_run_records_actions_without_calling_callbacks() -> None:
    result, rec = run_cycle(dry_run=True)

    assert rec.spot_orders == []
    assert rec.contract_orders == []
    assert result["actions"][0]["type"] == "freeze_dry_run"
    assert result["ledger"]["long_lots"] == []


def test_contract_callback_kwargs_have_short_position_side_and_no_reduce_kwargs() -> None:
    _result, rec = run_cycle()

    for kwargs in rec.contract_orders:
        assert kwargs["position_side"] == "SHORT"
        assert "reduce_only" not in kwargs
        assert "reduceOnly" not in kwargs


def test_pair_release_removes_equal_long_and_short_lots_without_contract_order() -> None:
    ledger = new_ledger()
    ledger["long_lots"] = [{"lot_id": "L1", "qty": 10.0, "cost_price": 8.0, "frozen_at": "t", "frozen_mid": 9.0, "hedge_pending": False}]
    ledger["short_lots"] = [{"lot_id": "S1", "qty": 6.0, "cost_price": 11.0, "frozen_at": "t", "frozen_mid": 10.0, "hedge_pending": False}]
    ledger_totals(ledger)

    result, rec = run_cycle(
        ledger=ledger,
        spot_inventory_qty=100.0,
        contract_short_qty=96.0,
        config=enabled_config(pair_release_enabled=True),
    )

    assert rec.spot_orders == []
    assert rec.contract_orders == []
    assert result["ledger"]["frozen_long_qty"] == 4.0
    assert result["ledger"]["frozen_short_qty"] == 0.0
    assert result["actions"][0]["type"] == "pair_release"


def test_pair_release_fifo_partial_lot_accounting() -> None:
    ledger = new_ledger()
    ledger["long_lots"] = [
        {"lot_id": "L1", "qty": 5.0, "cost_price": 8.0, "frozen_at": "t", "frozen_mid": 9.0, "hedge_pending": False},
        {"lot_id": "L2", "qty": 7.0, "cost_price": 9.0, "frozen_at": "t", "frozen_mid": 9.0, "hedge_pending": False},
    ]
    ledger["short_lots"] = [{"lot_id": "S1", "qty": 8.0, "cost_price": 11.0, "frozen_at": "t", "frozen_mid": 10.0, "hedge_pending": False}]
    ledger_totals(ledger)

    result, rec = run_cycle(
        ledger=ledger,
        spot_inventory_qty=100.0,
        contract_short_qty=96.0,
        config=enabled_config(pair_release_enabled=True),
    )

    assert rec.contract_orders == []
    assert result["ledger"]["long_lots"] == [
        {"lot_id": "L2", "qty": 4.0, "cost_price": 9.0, "frozen_at": "t", "frozen_mid": 9.0, "hedge_pending": False}
    ]
    assert result["ledger"]["short_lots"] == []


def test_profit_release_long_removes_spot_only_lot_without_contract_order() -> None:
    ledger = new_ledger()
    ledger["long_lots"] = [{"lot_id": "L1", "qty": 10.0, "cost_price": 10.0, "frozen_at": "t", "frozen_mid": 9.0, "hedge_pending": False}]
    ledger_totals(ledger)

    result, rec = run_cycle(
        ledger=ledger,
        spot_inventory_qty=100.0,
        contract_short_qty=90.0,
        mark_price=11.0,
        config=enabled_config(profit_release_enabled=True, release_profit_ratio=0.05),
    )

    assert rec.contract_orders == []
    assert result["ledger"]["long_lots"] == []
    assert result["ledger"]["pending_contract_actions"] == []


def test_profit_release_short_buys_short_position_then_removes_lot() -> None:
    ledger = new_ledger()
    ledger["short_lots"] = [{"lot_id": "S1", "qty": 10.0, "cost_price": 10.0, "frozen_at": "t", "frozen_mid": 11.0, "hedge_pending": False}]
    ledger_totals(ledger)

    result, rec = run_cycle(
        ledger=ledger,
        spot_inventory_qty=100.0,
        contract_short_qty=110.0,
        mark_price=9.0,
        config=enabled_config(profit_release_enabled=True, release_profit_ratio=0.05),
    )

    assert rec.contract_orders == [{"symbol": "WLDUSDT", "side": "BUY", "qty": 10.0, "position_side": "SHORT"}]
    assert result["ledger"]["short_lots"] == []
    assert result["ledger"]["pending_contract_actions"] == []


def test_profit_release_short_then_long_freeze_keeps_new_long_spot_only() -> None:
    ledger = new_ledger()
    ledger["short_lots"] = [{"lot_id": "S1", "qty": 10.0, "cost_price": 10.0, "frozen_at": "t", "frozen_mid": 11.0, "hedge_pending": False}]
    ledger_totals(ledger)

    result, rec = run_cycle(
        ledger=ledger,
        spot_inventory_qty=120.0,
        contract_short_qty=10.0,
        base_hedge_qty=0.0,
        mark_price=9.0,
        config=enabled_config(profit_release_enabled=True, release_profit_ratio=0.05),
    )

    assert rec.spot_orders == []
    assert rec.contract_orders == [
        {"symbol": "WLDUSDT", "side": "BUY", "qty": 10.0, "position_side": "SHORT"},
    ]
    assert result["ledger"]["long_lots"][0]["qty"] == 10.0
    assert result["ledger"]["short_lots"] == []
    assert result["ledger"]["pending_contract_actions"] == []
    assert result["alerts"] == []


def test_profit_release_short_contract_failure_keeps_lot_and_does_not_create_pending() -> None:
    ledger = new_ledger()
    ledger["short_lots"] = [{"lot_id": "S1", "qty": 10.0, "cost_price": 10.0, "frozen_at": "t", "frozen_mid": 11.0, "hedge_pending": False}]
    ledger_totals(ledger)

    result, rec = run_cycle(
        ledger=ledger,
        spot_inventory_qty=100.0,
        contract_short_qty=110.0,
        mark_price=9.0,
        config=enabled_config(profit_release_enabled=True, release_profit_ratio=0.05),
        recorder=OrderRecorder(fail_contract=True),
    )

    assert rec.contract_orders == [{"symbol": "WLDUSDT", "side": "BUY", "qty": 10.0, "position_side": "SHORT"}]
    assert result["ledger"]["short_lots"][0]["lot_id"] == "S1"
    assert result["ledger"]["pending_contract_actions"] == []
    assert "profit_release_contract_failed" in result["alerts"]
