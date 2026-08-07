from __future__ import annotations

import pytest

from grid_optimizer.alpha_market import AlphaMarketClient


def test_parse_trading_pairs_prefers_usdt_and_falls_back_to_usdc() -> None:
    payload = {
        "code": "000000",
        "data": {
            "symbols": [
                {"status": "TRADING", "baseAsset": "ALPHA_1", "quoteAsset": "USDC", "symbol": "ALPHA_1USDC"},
                {"status": "TRADING", "baseAsset": "ALPHA_1", "quoteAsset": "USDT", "symbol": "ALPHA_1USDT"},
                {"status": "TRADING", "baseAsset": "ALPHA_2", "quoteAsset": "USDC", "symbol": "ALPHA_2USDC"},
                {"status": "BREAK", "baseAsset": "ALPHA_3", "quoteAsset": "USDT", "symbol": "ALPHA_3USDT"},
            ]
        },
    }

    assert AlphaMarketClient.parse_trading_pairs(payload) == {
        "ALPHA_1": "ALPHA_1USDT",
        "ALPHA_2": "ALPHA_2USDC",
    }


def test_fetch_klines_uses_start_time_and_end_time_params(monkeypatch: pytest.MonkeyPatch) -> None:
    client = AlphaMarketClient()
    received: dict[str, object] = {}

    def fake_get_json(path: str, params: dict[str, object] | None = None) -> dict[str, object]:
        received["path"] = path
        received["params"] = params
        return {"code": "000000", "data": []}

    monkeypatch.setattr(client, "_get_json", fake_get_json)

    assert client.fetch_klines("ALPHA_1USDC", interval="1m", limit=63, start_time_ms=10, end_time_ms=20) == []
    assert received == {
        "path": "/bapi/defi/v1/public/alpha-trade/klines",
        "params": {"symbol": "ALPHA_1USDC", "interval": "1m", "limit": 63, "startTime": 10, "endTime": 20},
    }


def test_fetch_tokens_keeps_actual_usdc_pair_and_highest_volume_duplicate(monkeypatch: pytest.MonkeyPatch) -> None:
    client = AlphaMarketClient()
    token_payload = {
        "code": "000000",
        "data": [
            {"symbol": "QUID", "alphaId": "ALPHA_1", "name": "QUID", "volume24h": "10"},
            {"symbol": "QUID", "alphaId": "ALPHA_2", "name": "QUID v2", "volume24h": "99"},
            {"symbol": "SKIP", "alphaId": "ALPHA_3", "volume24h": "100"},
        ],
    }
    pairs_payload = {
        "code": "000000",
        "data": {
            "symbols": [
                {"status": "TRADING", "baseAsset": "ALPHA_1", "quoteAsset": "USDT", "symbol": "ALPHA_1USDT"},
                {"status": "TRADING", "baseAsset": "ALPHA_2", "quoteAsset": "USDC", "symbol": "ALPHA_2USDC"},
            ]
        },
    }

    def fake_get_json(path: str, params: dict[str, object] | None = None) -> dict[str, object]:
        if path.endswith("token/list"):
            return token_payload
        if path.endswith("get-exchange-info"):
            return pairs_payload
        raise AssertionError(path)

    monkeypatch.setattr(client, "_get_json", fake_get_json)

    tokens = client.fetch_tokens()

    assert set(tokens) == {"QUID"}
    assert tokens["QUID"].pair == "ALPHA_2USDC"
    assert tokens["QUID"].volume_24h == 99.0


@pytest.mark.parametrize("payload", [{"code": "123", "data": {}}, {"code": "000000", "data": []}])
def test_malformed_exchange_info_raises_without_payload_echo(payload: object) -> None:
    with pytest.raises(RuntimeError) as exc_info:
        AlphaMarketClient.parse_trading_pairs(payload)

    assert "Unexpected exchange-info response" in str(exc_info.value)
    assert repr(payload) not in str(exc_info.value)


@pytest.mark.parametrize(
    "payload",
    [
        {"code": "000000", "data": {}},
        {"code": "000000", "data": {"symbols": None}},
        {"code": "000000", "data": {"symbols": "ALPHA_1USDT"}},
        {"code": "000000", "data": {"symbols": [None]}},
        {"code": "000000", "data": {"symbols": ["ALPHA_1USDT"]}},
    ],
)
def test_parse_trading_pairs_rejects_malformed_symbol_schema(payload: object) -> None:
    with pytest.raises(RuntimeError) as exc_info:
        AlphaMarketClient.parse_trading_pairs(payload)

    assert str(exc_info.value) == "Unexpected exchange-info response"
