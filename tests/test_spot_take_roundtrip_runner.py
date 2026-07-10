from grid_optimizer.spot_take_roundtrip_runner import (
    execute_take_roundtrip,
    minimum_executable_leg_notional,
    next_leg_notional,
)


def test_execute_take_roundtrip_counts_exchange_quotes_and_sells_filled_buy_qty() -> None:
    calls: list[tuple[str, float]] = []

    def submit(side: str, qty: float) -> dict[str, str]:
        calls.append((side, qty))
        if side == "BUY":
            return {"executedQty": "60.0", "cummulativeQuoteQty": "100.2"}
        return {"executedQty": "60.0", "cummulativeQuoteQty": "99.9"}

    result = execute_take_roundtrip(
        leg_notional=100.0,
        bid_price=1.665,
        ask_price=1.666,
        step_size=0.01,
        min_qty=0.01,
        min_notional=5.0,
        submit_market=submit,
    )

    assert calls == [("BUY", 60.02), ("SELL", 60.0)]
    assert result["ok"] is True
    assert result["gross_notional"] == 200.1
    assert result["price_wear"] == 0.3
    assert result["residual_base_qty"] == 0.0


def test_execute_take_roundtrip_reports_partial_sell_residual() -> None:
    def submit(side: str, qty: float) -> dict[str, str]:
        if side == "BUY":
            return {"executedQty": "50.0", "cummulativeQuoteQty": "83.3"}
        return {"executedQty": "49.5", "cummulativeQuoteQty": "82.4"}

    result = execute_take_roundtrip(
        leg_notional=83.4,
        bid_price=1.665,
        ask_price=1.666,
        step_size=0.01,
        min_qty=0.01,
        min_notional=5.0,
        submit_market=submit,
    )

    assert result["ok"] is True
    assert result["residual_base_qty"] == 0.5


def test_next_leg_notional_uses_half_of_remaining_volume() -> None:
    assert next_leg_notional(remaining_volume=150.0, per_leg_notional=100.0, min_notional=5.0) == 75.0
    assert next_leg_notional(remaining_volume=250.0, per_leg_notional=100.0, min_notional=5.0) == 100.0
    assert next_leg_notional(remaining_volume=4.0, per_leg_notional=100.0, min_notional=5.0) == 5.0


def test_minimum_executable_leg_notional_allows_for_qty_rounding() -> None:
    assert minimum_executable_leg_notional(min_notional=5.0, ask_price=1.666, step_size=0.01) == 5.01666
