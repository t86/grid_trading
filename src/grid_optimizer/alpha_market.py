from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

import requests


BASE_URL = "https://www.binance.com"
TOKEN_LIST_PATH = "/bapi/defi/v1/public/wallet-direct/buw/wallet/cex/alpha/all/token/list"
EXCHANGE_INFO_PATH = "/bapi/defi/v1/public/alpha-trade/get-exchange-info"
TICKER_PATH = "/bapi/defi/v1/public/alpha-trade/ticker"
KLINES_PATH = "/bapi/defi/v1/public/alpha-trade/klines"
_RETRY_STATUS_CODES = {429, 502, 503, 504}


@dataclass(frozen=True)
class AlphaToken:
    symbol: str
    alpha_id: str
    name: str
    chain_name: str
    price: float
    volume_24h: float
    count_24h: int
    pair: str


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value in (None, ""):
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


class AlphaMarketClient:
    def __init__(self, timeout_seconds: float = 20.0, session: requests.Session | None = None) -> None:
        self.timeout_seconds = timeout_seconds
        self.session = session or requests.Session()

    def _get_json(self, path: str, params: dict[str, Any] | None = None) -> Any:
        url = f"{BASE_URL}{path}"
        for attempt in range(5):
            response = self.session.get(
                url,
                params=params,
                timeout=self.timeout_seconds,
                headers={"Accept": "application/json", "User-Agent": "wangge-alpha-dashboard/1.0"},
            )
            if response.status_code in _RETRY_STATUS_CODES and attempt < 4:
                time.sleep(2**attempt)
                continue
            response.raise_for_status()
            try:
                return response.json()
            except ValueError as exc:
                raise RuntimeError("Invalid JSON response from Binance Alpha API") from exc
        raise RuntimeError("Binance Alpha API retry loop exhausted")

    @staticmethod
    def parse_trading_pairs(payload: Any) -> dict[str, str]:
        if not isinstance(payload, dict) or payload.get("code") != "000000" or not isinstance(payload.get("data"), dict):
            raise RuntimeError("Unexpected exchange-info response")

        symbols = payload["data"].get("symbols")
        if not isinstance(symbols, list):
            raise RuntimeError("Unexpected exchange-info response")

        candidates: dict[str, tuple[int, str]] = {}
        for item in symbols:
            if not isinstance(item, dict):
                raise RuntimeError("Unexpected exchange-info response")
            if item.get("status") != "TRADING":
                continue
            base_asset = str(item.get("baseAsset", "")).upper().strip()
            quote_asset = str(item.get("quoteAsset", "")).upper().strip()
            pair = str(item.get("symbol", "")).upper().strip()
            if not base_asset or not pair or quote_asset not in ("USDT", "USDC"):
                continue
            candidate = (0 if quote_asset == "USDT" else 1, pair)
            if base_asset not in candidates or candidate < candidates[base_asset]:
                candidates[base_asset] = candidate
        return {base_asset: candidate[1] for base_asset, candidate in candidates.items()}

    def fetch_trading_pairs(self) -> dict[str, str]:
        return self.parse_trading_pairs(self._get_json(EXCHANGE_INFO_PATH))

    def fetch_tokens(self) -> dict[str, AlphaToken]:
        payload = self._get_json(TOKEN_LIST_PATH)
        if not isinstance(payload, dict) or payload.get("code") != "000000" or not isinstance(payload.get("data"), list):
            raise RuntimeError("Unexpected token-list response")

        trading_pairs = self.fetch_trading_pairs()
        tokens: dict[str, AlphaToken] = {}
        for item in payload["data"]:
            if not isinstance(item, dict):
                continue
            symbol = str(item.get("symbol", "")).upper().strip()
            alpha_id = str(item.get("alphaId", "")).upper().strip()
            pair = trading_pairs.get(alpha_id)
            if not symbol or not alpha_id or not pair:
                continue
            token = AlphaToken(
                symbol=symbol,
                alpha_id=alpha_id,
                name=str(item.get("name") or symbol),
                chain_name=str(item.get("chainName") or ""),
                price=_safe_float(item.get("price")),
                volume_24h=_safe_float(item.get("volume24h")),
                count_24h=_safe_int(item.get("count24h")),
                pair=pair,
            )
            current = tokens.get(symbol)
            if current is None or token.volume_24h > current.volume_24h:
                tokens[symbol] = token
        return tokens

    def fetch_ticker(self, pair: str) -> dict[str, Any]:
        payload = self._get_json(TICKER_PATH, {"symbol": pair})
        if not isinstance(payload, dict) or payload.get("code") != "000000" or not isinstance(payload.get("data"), dict):
            raise RuntimeError(f"Unexpected ticker response for {pair}")
        return payload["data"]

    def fetch_klines(
        self,
        pair: str,
        *,
        interval: str,
        limit: int,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
    ) -> list[list[Any]]:
        params: dict[str, Any] = {"symbol": pair, "interval": interval, "limit": limit}
        if start_time_ms is not None:
            params["startTime"] = start_time_ms
        if end_time_ms is not None:
            params["endTime"] = end_time_ms
        payload = self._get_json(KLINES_PATH, params)
        if not isinstance(payload, dict) or payload.get("code") != "000000" or not isinstance(payload.get("data"), list):
            raise RuntimeError(f"Unexpected klines response for {pair}")
        return payload["data"]
