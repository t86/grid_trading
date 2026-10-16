from __future__ import annotations

import pytest

from grid_optimizer import sprint_volume_runner as runner


@pytest.mark.parametrize("bid,ask,tick,expected", [
    (1227.78, 1227.79, 0.01, True),
    (600.01, 600.02, 0.01, True),
    (2400.0, 2400.02, 0.01, False),
    (1227.78, 1227.78, 0.01, False),
    (1227.79, 1227.78, 0.01, False),
    (0, 0.01, 0.01, False),
    (1227.78, 1227.79, 0, False),
    (float("nan"), 1227.79, 0.01, False),
    (1227.78, float("inf"), 0.01, False),
    (None, 1227.79, 0.01, False),
])
def test_one_tick_spread_is_exact_and_fails_closed(bid, ask, tick, expected):
    assert runner.is_one_tick_spread({"bid": bid, "ask": ask}, tick) is expected


@pytest.mark.parametrize("ask,enabled,reason", [
    (2400.02, True, "spread_not_one_tick"),
    (2400.01, True, "burst"),
    (2400.02, False, "burst"),
])
def test_main_one_tick_guard_only_blocks_wide_spread_when_enabled(tmp_path, monkeypatch, ask, enabled, reason):
    monkeypatch.setattr(runner, "load_binance_api_credentials", lambda: ("key", "secret"))
    monkeypatch.setattr(runner, "fetch_spot_symbol_config", lambda symbol: {"tick_size": 0.01, "step_size": 0.001})
    monkeypatch.setattr(runner, "fetch_book", lambda symbol: {
        "bid": 2400.0, "ask": ask, "mid": (2400.0 + ask) / 2, "spread_bps": 0.083333,
        "top_bid_notional": 10000, "top_ask_notional": 10000,
    })
    if reason == "spread_not_one_tick":
        monkeypatch.setattr(runner, "execute_roundtrip", lambda **kwargs: pytest.fail("wide spread must not trade"))

    class EndWait(Exception):
        pass

    def end_wait(seconds):
        raise EndWait

    monkeypatch.setattr(runner.time, "sleep", end_wait)
    status = tmp_path / "status.json"
    argv = ["--symbol", "ZECUSDC", "--burst-volume", "100", "--status-json", str(status)]
    if enabled:
        argv.append("--require-one-tick-spread")
    with pytest.raises(EndWait):
        runner.main(argv)
    import json
    result = json.loads(status.read_text())
    assert result["reason"] == reason
    assert (result["done_volume"] > 0) is (reason == "burst")


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
