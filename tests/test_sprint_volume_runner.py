from __future__ import annotations

import pytest

from grid_optimizer import sprint_volume_runner as runner


def test_execute_roundtrip_preserves_residual_when_sell_fails(monkeypatch):
    def fake_order(**kwargs):
        if kwargs["side"] == "BUY":
            return {"executedQty": "0.001", "cummulativeQuoteQty": "100"}
        raise RuntimeError("sell unavailable")

    monkeypatch.setattr(runner, "post_spot_order", fake_order)

    result = runner.execute_roundtrip(
        symbol="BTCUSDT",
        notional=100.0,
        book={
            "bid": 99_999.0,
            "ask": 100_000.0,
            "mid": 99_999.5,
            "spread_bps": 0.1,
            "top_bid_notional": 1_000.0,
            "top_ask_notional": 1_000.0,
        },
        qty_step=0.00001,
        tick_size=0.01,
        api_key="key",
        api_secret="secret",
        dry_run=False,
    )

    assert result["ok"] is True
    assert result["reason"] == "sell_error"
    assert result["base_bought"] == pytest.approx(0.001)
    assert result["base_sold"] == 0.0
    assert result["residual_base"] == pytest.approx(0.001)


def test_flatten_all_base_cancels_orders_and_sells_free_base(monkeypatch):
    signed_calls = []

    def fake_signed(url, params, api_key, api_secret, *, method):
        signed_calls.append((url, method))
        if url.endswith("/account"):
            return {"balances": [{"asset": "BTC", "free": "0.001"}]}
        return {}

    monkeypatch.setattr(runner, "_http_signed_request_json", fake_signed)
    monkeypatch.setattr(
        runner,
        "fetch_book",
        lambda symbol: {"mid": 100_000.0},
    )
    monkeypatch.setattr(
        runner,
        "post_spot_order",
        lambda **kwargs: {
            "executedQty": str(kwargs["quantity"]),
            "cummulativeQuoteQty": "100",
        },
    )

    result = runner.flatten_all_base(
        symbol="BTCUSDT",
        qty_step=0.00001,
        min_order_notional=5.0,
        api_key="key",
        api_secret="secret",
    )

    assert signed_calls[0][0].endswith("/openOrders")
    assert signed_calls[0][1] == "DELETE"
    assert result["canceled"] is True
    assert result["flattened_base"] == pytest.approx(0.001)
    assert result["recovered_quote"] == pytest.approx(100.0)
