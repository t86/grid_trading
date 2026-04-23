from __future__ import annotations

import csv
import hashlib
import hmac
import json
import os
import re
import socket
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .types import Candle, FundingRate

_USDM_URLS = {
    "kline": "https://fapi.binance.com/fapi/v1/klines",
    "exchange_info": "https://fapi.binance.com/fapi/v1/exchangeInfo",
    "funding_rate": "https://fapi.binance.com/fapi/v1/fundingRate",
    "ticker_price": "https://fapi.binance.com/fapi/v1/ticker/price",
    "book_ticker": "https://fapi.binance.com/fapi/v1/ticker/bookTicker",
    "premium_index": "https://fapi.binance.com/fapi/v1/premiumIndex",
}
_COINM_URLS = {
    "kline": "https://dapi.binance.com/dapi/v1/klines",
    "exchange_info": "https://dapi.binance.com/dapi/v1/exchangeInfo",
    "funding_rate": "https://dapi.binance.com/dapi/v1/fundingRate",
    "ticker_price": "https://dapi.binance.com/dapi/v1/ticker/price",
    "book_ticker": "https://dapi.binance.com/dapi/v1/ticker/bookTicker",
    "premium_index": "https://dapi.binance.com/dapi/v1/premiumIndex",
}
_SPOT_URLS = {
    "exchange_info": "https://api.binance.com/api/v3/exchangeInfo",
    "book_ticker": "https://api.binance.com/api/v3/ticker/bookTicker",
    "ticker_price": "https://api.binance.com/api/v3/ticker/price",
    "kline": "https://api.binance.com/api/v3/klines",
}
_SUPPORTED_MARKET_TYPES = ("futures", "spot")
_MARKET_TYPE_ALIASES = {
    "futures": "futures",
    "future": "futures",
    "perp": "futures",
    "perpetual": "futures",
    "contract": "futures",
    "contracts": "futures",
    "spot": "spot",
}
_SUPPORTED_CONTRACT_TYPES = ("usdm", "coinm")
_CONTRACT_TYPE_ALIASES = {
    "u": "usdm",
    "um": "usdm",
    "usdm": "usdm",
    "umfutures": "usdm",
    "coin": "coinm",
    "cm": "coinm",
    "coinm": "coinm",
    "delivery": "coinm",
}
DEFAULT_TIMEOUT_SECONDS = 30
MAX_HTTP_RESPONSE_BYTES = 8 * 1024 * 1024
HTTP_READ_CHUNK_BYTES = 64 * 1024
SYMBOL_CACHE_TTL_SECONDS = 6 * 3600
COINM_MAX_KLINE_WINDOW_MS = 200 * 24 * 60 * 60 * 1000
FUNDING_DEFAULT_STEP_MS = 8 * 60 * 60 * 1000
FUNDING_MISSING_GAP_THRESHOLD_MS = 10 * 60 * 60 * 1000
SECOND_INTERVAL_MAX_SPAN_MS = 31 * 24 * 60 * 60 * 1000
_ORIGINAL_GETADDRINFO = socket.getaddrinfo
_GETADDRINFO_PATCH_LOCK = threading.RLock()
_GETADDRINFO_PATCH_DEPTH = 0


def _ipv4_first_getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):
    infos = _ORIGINAL_GETADDRINFO(host, port, family, type, proto, flags)
    ipv4_infos = [item for item in infos if item[0] == socket.AF_INET]
    return ipv4_infos or infos


@contextmanager
def _prefer_ipv4() -> Any:
    global _GETADDRINFO_PATCH_DEPTH
    with _GETADDRINFO_PATCH_LOCK:
        if _GETADDRINFO_PATCH_DEPTH == 0:
            socket.getaddrinfo = _ipv4_first_getaddrinfo
        _GETADDRINFO_PATCH_DEPTH += 1
    try:
        yield
    finally:
        with _GETADDRINFO_PATCH_LOCK:
            _GETADDRINFO_PATCH_DEPTH = max(_GETADDRINFO_PATCH_DEPTH - 1, 0)
            if _GETADDRINFO_PATCH_DEPTH == 0:
                socket.getaddrinfo = _ORIGINAL_GETADDRINFO


def parse_interval_ms(interval: str) -> int:
    match = re.fullmatch(r"(\d+)([smhdw])", interval.strip())
    if not match:
        raise ValueError(f"Unsupported interval: {interval}")
    value = int(match.group(1))
    unit = match.group(2)
    if unit == "M":
        # Month has variable length; use 30d approximation for cursor stepping.
        return value * 2_592_000_000
    factor = {
        "s": 1_000,
        "m": 60_000,
        "h": 3_600_000,
        "d": 86_400_000,
        "w": 604_800_000,
    }[unit]
    return value * factor


def normalize_market_type(market_type: str | None) -> str:
    raw = str(market_type or "futures").strip().lower()
    normalized = _MARKET_TYPE_ALIASES.get(raw, raw)
    if normalized not in _SUPPORTED_MARKET_TYPES:
        raise ValueError(
            f"Unsupported market_type: {market_type}. "
            f"Supported: {', '.join(_SUPPORTED_MARKET_TYPES)}"
        )
    return normalized


def supported_market_types() -> list[str]:
    return list(_SUPPORTED_MARKET_TYPES)


def normalize_contract_type(contract_type: str | None) -> str:
    raw = str(contract_type or "usdm").strip().lower()
    normalized = _CONTRACT_TYPE_ALIASES.get(raw, raw)
    if normalized not in _SUPPORTED_CONTRACT_TYPES:
        raise ValueError(
            f"Unsupported contract_type: {contract_type}. "
            f"Supported: {', '.join(_SUPPORTED_CONTRACT_TYPES)}"
        )
    return normalized


def supported_contract_types() -> list[str]:
    return list(_SUPPORTED_CONTRACT_TYPES)


def _market_api_urls(contract_type: str | None) -> dict[str, str]:
    normalized = normalize_contract_type(contract_type)
    if normalized == "usdm":
        return _USDM_URLS
    return _COINM_URLS


def _response_header_value(response: Any, name: str) -> str | None:
    headers = getattr(response, "headers", None)
    if headers is None:
        return None
    if hasattr(headers, "get"):
        value = headers.get(name)
    elif isinstance(headers, dict):
        value = headers.get(name)
    else:
        value = None
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _read_http_response_bytes(response: Any, *, url: str) -> bytes:
    content_length = _response_header_value(response, "Content-Length")
    if content_length is not None:
        try:
            expected_size = int(content_length)
        except ValueError:
            expected_size = None
        if expected_size is not None and expected_size > MAX_HTTP_RESPONSE_BYTES:
            raise RuntimeError(
                f"HTTP response from {url} is too large: "
                f"{expected_size} bytes exceeds {MAX_HTTP_RESPONSE_BYTES}"
            )

    payload = bytearray()
    while True:
        chunk = response.read(HTTP_READ_CHUNK_BYTES)
        if not chunk:
            break
        if len(payload) + len(chunk) > MAX_HTTP_RESPONSE_BYTES:
            raise RuntimeError(
                f"HTTP response from {url} is too large: "
                f"exceeds {MAX_HTTP_RESPONSE_BYTES} bytes"
            )
        payload.extend(chunk)
    return bytes(payload)


def _http_request_json(
    url: str,
    params: dict[str, str | int],
    headers: dict[str, str] | None = None,
    method: str = "GET",
) -> Any:
    query = urlencode(params)
    full_url = f"{url}?{query}" if query else url
    request_headers = {
        "User-Agent": "grid-optimizer/0.1",
        "Accept": "application/json",
    }
    if headers:
        request_headers.update(headers)
    request = Request(
        full_url,
        headers=request_headers,
        method=method.upper(),
    )
    try:
        with _prefer_ipv4():
            with urlopen(request, timeout=DEFAULT_TIMEOUT_SECONDS) as response:
                payload = _read_http_response_bytes(response, url=full_url).decode("utf-8")
    except HTTPError as exc:
        payload = _read_http_response_bytes(exc, url=full_url).decode("utf-8", errors="replace")
        try:
            data = json.loads(payload)
        except json.JSONDecodeError:
            raise
        if isinstance(data, dict) and "code" in data and "msg" in data:
            code = data.get("code", exc.code)
            msg = data.get("msg", exc.reason)
            raise RuntimeError(f"Binance API error {code}: {msg}") from exc
        raise
    data = json.loads(payload)
    # Binance error payloads are dicts containing both "code" and "msg".
    # Successful endpoints like exchangeInfo also return dict, so only raise
    # when an explicit error code/message is present.
    if isinstance(data, dict) and "code" in data and "msg" in data:
        code = data.get("code", "unknown")
        msg = data.get("msg", "unknown error")
        if code not in (0, "0"):
            raise RuntimeError(f"Binance API error {code}: {msg}")
    return data


def _http_get_json(url: str, params: dict[str, str | int]) -> Any:
    return _http_request_json(url, params)


def _http_api_key_request_json(
    url: str,
    params: dict[str, str | int],
    api_key: str,
    method: str = "GET",
) -> Any:
    if not api_key.strip():
        raise RuntimeError("Binance API key is empty")
    return _http_request_json(url, params, headers={"X-MBX-APIKEY": api_key.strip()}, method=method)


def _http_api_key_get_json(url: str, params: dict[str, str | int], api_key: str) -> Any:
    return _http_api_key_request_json(url, params, api_key, method="GET")


def _http_signed_request_json(
    url: str,
    params: dict[str, str | int],
    api_key: str,
    api_secret: str,
    method: str = "GET",
) -> Any:
    if not api_key.strip() or not api_secret.strip():
        raise RuntimeError("Binance API credentials are empty")
    query_params = dict(params)
    query_params.setdefault("timestamp", int(time.time() * 1000))
    query = urlencode(query_params)
    signature = hmac.new(
        api_secret.strip().encode("utf-8"),
        query.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    signed_params = dict(query_params)
    signed_params["signature"] = signature
    return _http_request_json(
        url,
        signed_params,
        headers={"X-MBX-APIKEY": api_key.strip()},
        method=method,
    )


def _http_signed_get_json(
    url: str,
    params: dict[str, str | int],
    api_key: str,
    api_secret: str,
) -> Any:
    return _http_signed_request_json(url, params, api_key, api_secret, method="GET")


def load_binance_api_credentials() -> tuple[str, str] | None:
    api_key = load_binance_api_key() or ""
    api_secret = (
        os.getenv("BINANCE_API_SECRET")
        or os.getenv("BINANCE_SAPI_SECRET")
        or os.getenv("BINANCE_SECRET")
        or ""
    ).strip()
    if api_key and api_secret:
        return api_key, api_secret
    return None


def load_binance_api_key() -> str | None:
    api_key = (
        os.getenv("BINANCE_API_KEY")
        or os.getenv("BINANCE_SAPI_KEY")
        or os.getenv("BINANCE_KEY")
        or ""
    ).strip()
    return api_key or None


def load_binance_borrow_lookup_mode() -> str:
    raw = str(os.getenv("BINANCE_BORROW_LOOKUP_MODE", "safe")).strip().lower()
    return "full" if raw == "full" else "safe"


def _as_list_payload(data: Any, name: str) -> list[dict[str, Any]]:
    if isinstance(data, dict):
        items = [data]
    elif isinstance(data, list):
        items = data
    else:
        raise RuntimeError(f"Unexpected {name} response")
    return [item for item in items if isinstance(item, dict)]


def _safe_positive_float(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number <= 0:
        return None
    return number


def _safe_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _format_request_number(value: float) -> str:
    text = format(Decimal(str(value)), "f").rstrip("0").rstrip(".")
    return text or "0"


def fetch_spot_markets() -> list[dict[str, str]]:
    data = _http_get_json(_SPOT_URLS["exchange_info"], {})
    if not isinstance(data, dict):
        raise RuntimeError("Unexpected spot exchangeInfo response")
    symbols_raw = data.get("symbols", [])
    if not isinstance(symbols_raw, list):
        raise RuntimeError("Unexpected spot exchangeInfo symbols payload")

    markets: dict[str, dict[str, str]] = {}
    for item in symbols_raw:
        if not isinstance(item, dict):
            continue
        symbol = str(item.get("symbol", "")).upper().strip()
        status = str(item.get("status", "")).upper().strip()
        base_asset = str(item.get("baseAsset", "")).upper().strip()
        quote_asset = str(item.get("quoteAsset", "")).upper().strip()
        if not symbol or not base_asset or not quote_asset:
            continue
        if status != "TRADING":
            continue
        if not bool(item.get("isSpotTradingAllowed", True)):
            continue
        markets[symbol] = {
            "symbol": symbol,
            "base_asset": base_asset,
            "quote_asset": quote_asset,
        }
    rows = [markets[key] for key in sorted(markets)]
    if not rows:
        raise RuntimeError("No spot markets returned from Binance")
    return rows


def fetch_spot_symbols() -> list[str]:
    return [item["symbol"] for item in fetch_spot_markets()]


def fetch_spot_symbol_config(symbol: str) -> dict[str, Any]:
    normalized_symbol = str(symbol).upper().strip()
    if not normalized_symbol:
        raise ValueError("symbol is required")
    data = _http_get_json(_SPOT_URLS["exchange_info"], {"symbol": normalized_symbol})
    if not isinstance(data, dict):
        raise RuntimeError("Unexpected spot exchangeInfo response")
    symbols_raw = data.get("symbols", [])
    if not isinstance(symbols_raw, list) or not symbols_raw:
        raise RuntimeError(f"Symbol not found in spot exchangeInfo: {normalized_symbol}")
    item = None
    for candidate in symbols_raw:
        if not isinstance(candidate, dict):
            continue
        if str(candidate.get("symbol", "")).upper().strip() == normalized_symbol:
            item = candidate
            break
    if item is None:
        raise RuntimeError(f"Symbol not found in spot exchangeInfo payload: {normalized_symbol}")

    filters_raw = item.get("filters", [])
    if not isinstance(filters_raw, list):
        filters_raw = []
    filters: dict[str, dict[str, Any]] = {}
    for entry in filters_raw:
        if not isinstance(entry, dict):
            continue
        filter_type = str(entry.get("filterType", "")).upper().strip()
        if filter_type:
            filters[filter_type] = entry

    price_filter = filters.get("PRICE_FILTER", {})
    lot_size = filters.get("LOT_SIZE", {})
    market_lot_size = filters.get("MARKET_LOT_SIZE", {})
    min_notional_filter = filters.get("MIN_NOTIONAL", {})
    notional_filter = filters.get("NOTIONAL", {})
    min_notional_value = (
        min_notional_filter.get("minNotional")
        or min_notional_filter.get("notional")
        or notional_filter.get("minNotional")
        or notional_filter.get("notional")
    )

    return {
        "symbol": normalized_symbol,
        "status": str(item.get("status", "")).upper().strip(),
        "base_asset": str(item.get("baseAsset", "")).upper().strip(),
        "quote_asset": str(item.get("quoteAsset", "")).upper().strip(),
        "price_precision": (
            int(item["quotePrecision"])
            if str(item.get("quotePrecision", "")).strip()
            else None
        ),
        "quantity_precision": (
            int(item["baseAssetPrecision"])
            if str(item.get("baseAssetPrecision", "")).strip()
            else None
        ),
        "tick_size": _safe_positive_float(price_filter.get("tickSize")),
        "min_price": _safe_positive_float(price_filter.get("minPrice")),
        "max_price": _safe_positive_float(price_filter.get("maxPrice")),
        "step_size": _safe_positive_float(lot_size.get("stepSize")),
        "min_qty": _safe_positive_float(lot_size.get("minQty")),
        "max_qty": _safe_positive_float(lot_size.get("maxQty")),
        "market_step_size": _safe_positive_float(market_lot_size.get("stepSize")),
        "market_min_qty": _safe_positive_float(market_lot_size.get("minQty")),
        "market_max_qty": _safe_positive_float(market_lot_size.get("maxQty")),
        "min_notional": _safe_positive_float(min_notional_value),
    }


def _spot_markets_cache_path(cache_dir: str | Path = "data") -> Path:
    return Path(cache_dir) / "spot_markets.json"


def load_or_fetch_spot_markets(
    cache_dir: str | Path = "data",
    refresh: bool = False,
) -> list[dict[str, str]]:
    path = _spot_markets_cache_path(cache_dir)
    now_ts = int(time.time())
    stale_markets: list[dict[str, str]] = []
    if not refresh and path.exists():
        try:
            with path.open("r", encoding="utf-8") as f:
                payload = json.load(f)
            fetched_at = int(payload.get("fetched_at", 0))
            markets = payload.get("markets", [])
            normalized: list[dict[str, str]] = []
            for item in markets:
                if not isinstance(item, dict):
                    continue
                symbol = str(item.get("symbol", "")).upper().strip()
                base_asset = str(item.get("base_asset", "")).upper().strip()
                quote_asset = str(item.get("quote_asset", "")).upper().strip()
                if not symbol or not base_asset or not quote_asset:
                    continue
                normalized.append(
                    {
                        "symbol": symbol,
                        "base_asset": base_asset,
                        "quote_asset": quote_asset,
                    }
                )
            if normalized:
                stale_markets = normalized
            if stale_markets and now_ts - fetched_at <= SYMBOL_CACHE_TTL_SECONDS:
                return stale_markets
        except (ValueError, OSError, json.JSONDecodeError):
            pass

    try:
        markets = fetch_spot_markets()
    except Exception:
        if stale_markets:
            return stale_markets
        raise
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump({"fetched_at": now_ts, "markets": markets}, f, ensure_ascii=False)
    return markets


def load_or_fetch_spot_symbols(
    cache_dir: str | Path = "data",
    refresh: bool = False,
) -> list[str]:
    return [item["symbol"] for item in load_or_fetch_spot_markets(cache_dir=cache_dir, refresh=refresh)]


def fetch_spot_book_tickers(symbol: str | None = None) -> list[dict[str, Any]]:
    params: dict[str, str] = {}
    if symbol:
        params["symbol"] = str(symbol).upper().strip()
    data = _http_get_json(_SPOT_URLS["book_ticker"], params)
    items = _as_list_payload(data, "spot bookTicker")
    rows: list[dict[str, Any]] = []
    for item in items:
        ticker_symbol = str(item.get("symbol", "")).upper().strip()
        bid_price = _safe_positive_float(item.get("bidPrice"))
        ask_price = _safe_positive_float(item.get("askPrice"))
        if not ticker_symbol or bid_price is None or ask_price is None:
            continue
        rows.append(
            {
                "symbol": ticker_symbol,
                "bid_price": bid_price,
                "bid_qty": _safe_positive_float(item.get("bidQty")),
                "ask_price": ask_price,
                "ask_qty": _safe_positive_float(item.get("askQty")),
            }
        )
    return rows


def fetch_spot_latest_price(symbol: str) -> float:
    data = _http_get_json(_SPOT_URLS["ticker_price"], {"symbol": symbol.upper()})
    if not isinstance(data, dict):
        raise RuntimeError("Unexpected spot ticker price response")
    price_raw = data.get("price")
    try:
        price = float(price_raw)
    except (TypeError, ValueError) as exc:
        raise RuntimeError("Invalid spot ticker price payload") from exc
    if price <= 0:
        raise RuntimeError("Invalid spot ticker price value")
    return price


def fetch_spot_klines(
    symbol: str,
    interval: str,
    start_ms: int,
    end_ms: int,
    limit: int = 1000,
) -> list[Candle]:
    if start_ms >= end_ms:
        raise ValueError("start_ms must be < end_ms")
    interval_ms = parse_interval_ms(interval)
    cursor = start_ms
    rows: list[list] = []
    max_candle_span_ms = max(interval_ms * max(limit - 1, 1), interval_ms)

    while cursor < end_ms:
        request_end_ms = min(end_ms - 1, cursor + max_candle_span_ms)
        data = _http_get_json(
            _SPOT_URLS["kline"],
            {
                "symbol": symbol.upper(),
                "interval": interval,
                "startTime": cursor,
                "endTime": request_end_ms,
                "limit": limit,
            },
        )
        if not data:
            next_cursor = request_end_ms + interval_ms
            if next_cursor <= cursor:
                break
            cursor = next_cursor
            continue
        rows.extend(data)
        last_open = max(int(row[0]) for row in data)
        next_cursor = last_open + interval_ms
        if next_cursor <= cursor:
            next_cursor = request_end_ms + interval_ms
            if next_cursor <= cursor:
                break
        cursor = next_cursor
        if len(data) >= limit:
            time.sleep(0.03)

    dedup: dict[int, Candle] = {}
    for row in rows:
        open_ms = int(row[0])
        close_ms = int(row[6])
        if open_ms < start_ms or open_ms >= end_ms:
            continue
        dedup[open_ms] = Candle(
            open_time=datetime.fromtimestamp(open_ms / 1000, tz=timezone.utc),
            close_time=datetime.fromtimestamp(close_ms / 1000, tz=timezone.utc),
            open=float(row[1]),
            high=float(row[2]),
            low=float(row[3]),
            close=float(row[4]),
        )
    return [dedup[key] for key in sorted(dedup)]


def _filter_futures_symbols(
    *,
    exchange_info: dict[str, Any],
    quote_asset: str | None = None,
    contract_type: str | None = "PERPETUAL",
    only_trading: bool = True,
) -> list[str]:
    symbols_raw = exchange_info.get("symbols", [])
    if not isinstance(symbols_raw, list):
        raise RuntimeError("Unexpected exchangeInfo symbols payload")
    normalized_quote = str(quote_asset or "").upper().strip()
    normalized_contract = str(contract_type or "").upper().strip()
    symbols: list[str] = []
    for item in symbols_raw:
        if not isinstance(item, dict):
            continue
        symbol = str(item.get("symbol", "")).upper().strip()
        status = str(item.get("status") or item.get("contractStatus") or "").upper().strip()
        item_quote_asset = str(item.get("quoteAsset", "")).upper().strip()
        item_contract_type = str(item.get("contractType", "")).upper().strip()
        if not symbol:
            continue
        if only_trading and status != "TRADING":
            continue
        if normalized_quote and item_quote_asset != normalized_quote:
            continue
        if normalized_contract and item_contract_type != normalized_contract:
            continue
        symbols.append(symbol)
    return sorted(set(symbols))


def fetch_futures_symbols(contract_type: str = "usdm") -> list[str]:
    data = _http_get_json(_market_api_urls(contract_type)["exchange_info"], {})
    if not isinstance(data, dict):
        raise RuntimeError("Unexpected exchangeInfo response")
    symbols = _filter_futures_symbols(exchange_info=data, contract_type="PERPETUAL", only_trading=True)
    if not symbols:
        raise RuntimeError("No futures symbols returned from Binance")
    return symbols


def fetch_futures_symbol_config(symbol: str, contract_type: str = "usdm") -> dict[str, Any]:
    normalized_symbol = str(symbol).upper().strip()
    if not normalized_symbol:
        raise ValueError("symbol is required")
    data = _http_get_json(_market_api_urls(contract_type)["exchange_info"], {"symbol": normalized_symbol})
    if not isinstance(data, dict):
        raise RuntimeError("Unexpected exchangeInfo response")
    symbols_raw = data.get("symbols", [])
    if not isinstance(symbols_raw, list) or not symbols_raw:
        raise RuntimeError(f"Symbol not found in exchangeInfo: {normalized_symbol}")
    item = None
    for candidate in symbols_raw:
        if not isinstance(candidate, dict):
            continue
        if str(candidate.get("symbol", "")).upper().strip() == normalized_symbol:
            item = candidate
            break
    if item is None:
        raise RuntimeError(f"Symbol not found in exchangeInfo payload: {normalized_symbol}")

    filters_raw = item.get("filters", [])
    if not isinstance(filters_raw, list):
        filters_raw = []
    filters: dict[str, dict[str, Any]] = {}
    for entry in filters_raw:
        if not isinstance(entry, dict):
            continue
        filter_type = str(entry.get("filterType", "")).upper().strip()
        if filter_type:
            filters[filter_type] = entry

    price_filter = filters.get("PRICE_FILTER", {})
    lot_size = filters.get("LOT_SIZE", {})
    market_lot_size = filters.get("MARKET_LOT_SIZE", {})
    min_notional_filter = filters.get("MIN_NOTIONAL", {})

    return {
        "symbol": normalized_symbol,
        "status": str(item.get("status") or item.get("contractStatus") or "").upper().strip(),
        "contract_type": str(item.get("contractType", "")).upper().strip(),
        "price_precision": int(item["pricePrecision"]) if str(item.get("pricePrecision", "")).strip() else None,
        "quantity_precision": int(item["quantityPrecision"]) if str(item.get("quantityPrecision", "")).strip() else None,
        "tick_size": _safe_positive_float(price_filter.get("tickSize")),
        "min_price": _safe_positive_float(price_filter.get("minPrice")),
        "max_price": _safe_positive_float(price_filter.get("maxPrice")),
        "step_size": _safe_positive_float(lot_size.get("stepSize")),
        "min_qty": _safe_positive_float(lot_size.get("minQty")),
        "max_qty": _safe_positive_float(lot_size.get("maxQty")),
        "market_step_size": _safe_positive_float(market_lot_size.get("stepSize")),
        "market_min_qty": _safe_positive_float(market_lot_size.get("minQty")),
        "market_max_qty": _safe_positive_float(market_lot_size.get("maxQty")),
        "min_notional": _safe_positive_float(min_notional_filter.get("notional")),
        "trigger_protect": _safe_float(item.get("triggerProtect")),
        "market_take_bound": _safe_float(item.get("marketTakeBound")),
    }


def fetch_futures_latest_price(symbol: str, contract_type: str = "usdm") -> float:
    data = _http_get_json(_market_api_urls(contract_type)["ticker_price"], {"symbol": symbol.upper()})
    if not isinstance(data, dict):
        raise RuntimeError("Unexpected ticker price response")
    price_raw = data.get("price")
    try:
        price = float(price_raw)
    except (TypeError, ValueError) as exc:
        raise RuntimeError("Invalid ticker price payload") from exc
    if price <= 0:
        raise RuntimeError("Invalid ticker price value")
    return price


def fetch_futures_book_tickers(
    contract_type: str = "usdm",
    symbol: str | None = None,
) -> list[dict[str, Any]]:
    params: dict[str, str] = {}
    if symbol:
        params["symbol"] = str(symbol).upper().strip()
    data = _http_get_json(_market_api_urls(contract_type)["book_ticker"], params)
    items = _as_list_payload(data, "futures bookTicker")
    rows: list[dict[str, Any]] = []
    for item in items:
        ticker_symbol = str(item.get("symbol", "")).upper().strip()
        bid_price = _safe_positive_float(item.get("bidPrice"))
        ask_price = _safe_positive_float(item.get("askPrice"))
        if not ticker_symbol or bid_price is None or ask_price is None:
            continue
        rows.append(
            {
                "symbol": ticker_symbol,
                "pair": str(item.get("pair", "")).upper().strip(),
                "bid_price": bid_price,
                "bid_qty": _safe_positive_float(item.get("bidQty")),
                "ask_price": ask_price,
                "ask_qty": _safe_positive_float(item.get("askQty")),
                "time": int(item["time"]) if str(item.get("time", "")).strip() else None,
            }
        )
    return rows


def fetch_futures_premium_index(
    contract_type: str = "usdm",
    symbol: str | None = None,
) -> list[dict[str, Any]]:
    params: dict[str, str] = {}
    if symbol:
        params["symbol"] = str(symbol).upper().strip()
    data = _http_get_json(_market_api_urls(contract_type)["premium_index"], params)
    items = _as_list_payload(data, "premiumIndex")
    rows: list[dict[str, Any]] = []
    for item in items:
        ticker_symbol = str(item.get("symbol", "")).upper().strip()
        if not ticker_symbol:
            continue
        next_funding_time_raw = str(item.get("nextFundingTime", "")).strip()
        time_raw = str(item.get("time", "")).strip()
        rows.append(
            {
                "symbol": ticker_symbol,
                "mark_price": _safe_positive_float(item.get("markPrice")),
                "index_price": _safe_positive_float(item.get("indexPrice")),
                "estimated_settle_price": _safe_positive_float(item.get("estimatedSettlePrice")),
                "funding_rate": _safe_float(item.get("lastFundingRate")),
                "interest_rate": _safe_float(item.get("interestRate")),
                "next_funding_time": int(next_funding_time_raw) if next_funding_time_raw else None,
                "time": int(time_raw) if time_raw else None,
            }
        )
    return rows


def _futures_trade_base_url(contract_type: str = "usdm") -> str:
    normalized = normalize_contract_type(contract_type)
    if normalized != "usdm":
        raise ValueError("Authenticated futures helpers currently support usdm only")
    return "https://fapi.binance.com"


def _spot_trade_base_url() -> str:
    return "https://api.binance.com"


def fetch_spot_account_info(
    api_key: str,
    api_secret: str,
    recv_window: int = 5000,
) -> dict[str, Any]:
    data = _http_signed_request_json(
        f"{_spot_trade_base_url()}/api/v3/account",
        {"recvWindow": recv_window},
        api_key,
        api_secret,
        method="GET",
    )
    if not isinstance(data, dict):
        raise RuntimeError("Unexpected spot account response")
    return data


def fetch_spot_open_orders(
    symbol: str,
    api_key: str,
    api_secret: str,
    recv_window: int = 5000,
) -> list[dict[str, Any]]:
    data = _http_signed_request_json(
        f"{_spot_trade_base_url()}/api/v3/openOrders",
        {"symbol": symbol.upper(), "recvWindow": recv_window},
        api_key,
        api_secret,
        method="GET",
    )
    return _as_list_payload(data, "spot openOrders")


def fetch_spot_all_orders(
    *,
    symbol: str,
    api_key: str,
    api_secret: str,
    start_time_ms: int | None = None,
    end_time_ms: int | None = None,
    limit: int = 1000,
    recv_window: int = 5000,
) -> list[dict[str, Any]]:
    params: dict[str, str | int] = {
        "symbol": symbol.upper(),
        "limit": int(limit),
        "recvWindow": recv_window,
    }
    if start_time_ms is not None:
        params["startTime"] = int(start_time_ms)
    if end_time_ms is not None:
        params["endTime"] = int(end_time_ms)
    data = _http_signed_request_json(
        f"{_spot_trade_base_url()}/api/v3/allOrders",
        params,
        api_key,
        api_secret,
        method="GET",
    )
    return _as_list_payload(data, "spot allOrders")


def fetch_spot_user_trades(
    *,
    symbol: str,
    api_key: str,
    api_secret: str,
    start_time_ms: int | None = None,
    end_time_ms: int | None = None,
    limit: int = 1000,
    recv_window: int = 5000,
) -> list[dict[str, Any]]:
    params: dict[str, str | int] = {
        "symbol": symbol.upper(),
        "limit": int(limit),
        "recvWindow": recv_window,
    }
    if start_time_ms is not None:
        params["startTime"] = int(start_time_ms)
    if end_time_ms is not None:
        params["endTime"] = int(end_time_ms)
    data = _http_signed_request_json(
        f"{_spot_trade_base_url()}/api/v3/myTrades",
        params,
        api_key,
        api_secret,
        method="GET",
    )
    return _as_list_payload(data, "spot myTrades")


def post_spot_order(
    *,
    symbol: str,
    side: str,
    quantity: float,
    price: float,
    api_key: str,
    api_secret: str,
    order_type: str = "LIMIT_MAKER",
    time_in_force: str = "GTC",
    recv_window: int = 5000,
    new_client_order_id: str | None = None,
) -> dict[str, Any]:
    normalized_type = str(order_type or "LIMIT_MAKER").upper().strip() or "LIMIT_MAKER"
    params: dict[str, str | int] = {
        "symbol": symbol.upper(),
        "side": side.upper(),
        "type": normalized_type,
        "quantity": _format_request_number(quantity),
        "price": _format_request_number(price),
        "recvWindow": recv_window,
    }
    if normalized_type == "LIMIT":
        params["timeInForce"] = str(time_in_force or "GTC").upper().strip() or "GTC"
    if new_client_order_id:
        params["newClientOrderId"] = str(new_client_order_id).strip()
    data = _http_signed_request_json(
        f"{_spot_trade_base_url()}/api/v3/order",
        params,
        api_key,
        api_secret,
        method="POST",
    )
    if not isinstance(data, dict):
        raise RuntimeError("Unexpected spot order response")
    return data


def delete_spot_order(
    *,
    symbol: str,
    api_key: str,
    api_secret: str,
    order_id: int | None = None,
    orig_client_order_id: str | None = None,
    recv_window: int = 5000,
) -> dict[str, Any]:
    if order_id is None and not str(orig_client_order_id or "").strip():
        raise ValueError("order_id or orig_client_order_id is required")
    params: dict[str, str | int] = {
        "symbol": symbol.upper(),
        "recvWindow": recv_window,
    }
    if order_id is not None:
        params["orderId"] = int(order_id)
    if orig_client_order_id:
        params["origClientOrderId"] = str(orig_client_order_id).strip()
    data = _http_signed_request_json(
        f"{_spot_trade_base_url()}/api/v3/order",
        params,
        api_key,
        api_secret,
        method="DELETE",
    )
    if not isinstance(data, dict):
        raise RuntimeError("Unexpected spot cancel order response")
    return data


def fetch_futures_position_risk_v3(
    api_key: str,
    api_secret: str,
    symbol: str | None = None,
    contract_type: str = "usdm",
    recv_window: int = 5000,
) -> list[dict[str, Any]]:
    params: dict[str, str | int] = {"recvWindow": recv_window}
    normalized_symbol = str(symbol or "").upper().strip()
    if normalized_symbol:
        params["symbol"] = normalized_symbol
    data = _http_signed_request_json(
        f"{_futures_trade_base_url(contract_type)}/fapi/v3/positionRisk",
        params,
        api_key,
        api_secret,
        method="GET",
    )
    if not isinstance(data, list):
        raise RuntimeError("Unexpected futures positionRisk response")
    return [dict(item) for item in data if isinstance(item, dict)]


def _merge_futures_position_risk_into_account_info(
    account_info: dict[str, Any],
    position_risk: list[dict[str, Any]],
) -> dict[str, Any]:
    positions = account_info.get("positions", [])
    if not isinstance(positions, list):
        return account_info
    risk_by_key: dict[tuple[str, str], dict[str, Any]] = {}
    risk_by_symbol: dict[str, list[dict[str, Any]]] = {}
    for item in position_risk:
        symbol = str(item.get("symbol", "")).upper().strip()
        if not symbol:
            continue
        side = str(item.get("positionSide") or "BOTH").upper().strip() or "BOTH"
        risk_by_key[(symbol, side)] = item
        risk_by_symbol.setdefault(symbol, []).append(item)

    enriched_positions: list[Any] = []
    risk_fields = (
        "entryPrice",
        "breakEvenPrice",
        "markPrice",
        "liquidationPrice",
        "unRealizedProfit",
        "notional",
    )
    for raw_position in positions:
        if not isinstance(raw_position, dict):
            enriched_positions.append(raw_position)
            continue
        position = dict(raw_position)
        symbol = str(position.get("symbol", "")).upper().strip()
        side = str(position.get("positionSide") or "BOTH").upper().strip() or "BOTH"
        risk = risk_by_key.get((symbol, side)) or risk_by_key.get((symbol, "BOTH"))
        if risk is None:
            symbol_risks = risk_by_symbol.get(symbol, [])
            if len(symbol_risks) == 1:
                risk = symbol_risks[0]
        if risk:
            for field in risk_fields:
                value = risk.get(field)
                if value is not None and str(value).strip():
                    position[field] = value
        enriched_positions.append(position)
    enriched = dict(account_info)
    enriched["positions"] = enriched_positions
    return enriched


def fetch_futures_account_info_v3(
    api_key: str,
    api_secret: str,
    contract_type: str = "usdm",
    recv_window: int = 5000,
) -> dict[str, Any]:
    data = _http_signed_request_json(
        f"{_futures_trade_base_url(contract_type)}/fapi/v3/account",
        {"recvWindow": recv_window},
        api_key,
        api_secret,
        method="GET",
    )
    if not isinstance(data, dict):
        raise RuntimeError("Unexpected futures account response")
    try:
        position_risk = fetch_futures_position_risk_v3(
            api_key,
            api_secret,
            contract_type=contract_type,
            recv_window=recv_window,
        )
    except Exception:
        return data
    data = _merge_futures_position_risk_into_account_info(data, position_risk)
    return data


def fetch_futures_position_mode(
    api_key: str,
    api_secret: str,
    contract_type: str = "usdm",
    recv_window: int = 5000,
) -> dict[str, Any]:
    data = _http_signed_request_json(
        f"{_futures_trade_base_url(contract_type)}/fapi/v1/positionSide/dual",
        {"recvWindow": recv_window},
        api_key,
        api_secret,
        method="GET",
    )
    if not isinstance(data, dict):
        raise RuntimeError("Unexpected futures position mode response")
    return data


def fetch_futures_open_orders(
    symbol: str,
    api_key: str,
    api_secret: str,
    contract_type: str = "usdm",
    recv_window: int = 5000,
) -> list[dict[str, Any]]:
    data = _http_signed_request_json(
        f"{_futures_trade_base_url(contract_type)}/fapi/v1/openOrders",
        {"symbol": symbol.upper(), "recvWindow": recv_window},
        api_key,
        api_secret,
        method="GET",
    )
    return _as_list_payload(data, "futures openOrders")


def fetch_futures_user_trades(
    *,
    symbol: str,
    api_key: str,
    api_secret: str,
    contract_type: str = "usdm",
    start_time_ms: int | None = None,
    end_time_ms: int | None = None,
    limit: int = 1000,
    recv_window: int = 5000,
) -> list[dict[str, Any]]:
    params: dict[str, str | int] = {
        "symbol": symbol.upper(),
        "limit": int(limit),
        "recvWindow": recv_window,
    }
    if start_time_ms is not None:
        params["startTime"] = int(start_time_ms)
    if end_time_ms is not None:
        params["endTime"] = int(end_time_ms)
    data = _http_signed_request_json(
        f"{_futures_trade_base_url(contract_type)}/fapi/v1/userTrades",
        params,
        api_key,
        api_secret,
        method="GET",
    )
    return _as_list_payload(data, "futures userTrades")


def fetch_futures_income_history(
    *,
    api_key: str,
    api_secret: str,
    symbol: str | None = None,
    income_type: str | None = None,
    contract_type: str = "usdm",
    start_time_ms: int | None = None,
    end_time_ms: int | None = None,
    limit: int = 1000,
    recv_window: int = 5000,
) -> list[dict[str, Any]]:
    params: dict[str, str | int] = {
        "limit": int(limit),
        "recvWindow": recv_window,
    }
    if symbol:
        params["symbol"] = symbol.upper().strip()
    if income_type:
        params["incomeType"] = str(income_type).upper().strip()
    if start_time_ms is not None:
        params["startTime"] = int(start_time_ms)
    if end_time_ms is not None:
        params["endTime"] = int(end_time_ms)
    data = _http_signed_request_json(
        f"{_futures_trade_base_url(contract_type)}/fapi/v1/income",
        params,
        api_key,
        api_secret,
        method="GET",
    )
    return _as_list_payload(data, "futures income history")


def post_futures_test_order(
    *,
    symbol: str,
    side: str,
    quantity: float,
    price: float,
    api_key: str,
    api_secret: str,
    contract_type: str = "usdm",
    time_in_force: str = "GTX",
    recv_window: int = 5000,
    new_client_order_id: str | None = None,
    position_side: str | None = None,
) -> dict[str, Any]:
    params: dict[str, str | int] = {
        "symbol": symbol.upper(),
        "side": side.upper(),
        "type": "LIMIT",
        "timeInForce": time_in_force,
        "quantity": _format_request_number(quantity),
        "price": _format_request_number(price),
        "recvWindow": recv_window,
    }
    if new_client_order_id:
        params["newClientOrderId"] = str(new_client_order_id).strip()
    if position_side:
        params["positionSide"] = str(position_side).upper().strip()
    data = _http_signed_request_json(
        f"{_futures_trade_base_url(contract_type)}/fapi/v1/order/test",
        params,
        api_key,
        api_secret,
        method="POST",
    )
    if not isinstance(data, dict):
        raise RuntimeError("Unexpected futures test order response")
    return data


def post_futures_order(
    *,
    symbol: str,
    side: str,
    quantity: float,
    price: float,
    api_key: str,
    api_secret: str,
    contract_type: str = "usdm",
    time_in_force: str = "GTX",
    recv_window: int = 5000,
    new_client_order_id: str | None = None,
    reduce_only: bool | None = None,
    position_side: str | None = None,
) -> dict[str, Any]:
    params: dict[str, str | int] = {
        "symbol": symbol.upper(),
        "side": side.upper(),
        "type": "LIMIT",
        "timeInForce": time_in_force,
        "quantity": _format_request_number(quantity),
        "price": _format_request_number(price),
        "recvWindow": recv_window,
    }
    if new_client_order_id:
        params["newClientOrderId"] = str(new_client_order_id).strip()
    if position_side:
        params["positionSide"] = str(position_side).upper().strip()
    if reduce_only is not None:
        params["reduceOnly"] = "true" if reduce_only else "false"
    data = _http_signed_request_json(
        f"{_futures_trade_base_url(contract_type)}/fapi/v1/order",
        params,
        api_key,
        api_secret,
        method="POST",
    )
    if not isinstance(data, dict):
        raise RuntimeError("Unexpected futures order response")
    return data


def delete_futures_order(
    *,
    symbol: str,
    api_key: str,
    api_secret: str,
    order_id: int | None = None,
    orig_client_order_id: str | None = None,
    contract_type: str = "usdm",
    recv_window: int = 5000,
) -> dict[str, Any]:
    if order_id is None and not str(orig_client_order_id or "").strip():
        raise ValueError("order_id or orig_client_order_id is required")
    params: dict[str, str | int] = {
        "symbol": symbol.upper(),
        "recvWindow": recv_window,
    }
    if order_id is not None:
        params["orderId"] = int(order_id)
    if orig_client_order_id:
        params["origClientOrderId"] = str(orig_client_order_id).strip()
    data = _http_signed_request_json(
        f"{_futures_trade_base_url(contract_type)}/fapi/v1/order",
        params,
        api_key,
        api_secret,
        method="DELETE",
    )
    if not isinstance(data, dict):
        raise RuntimeError("Unexpected futures cancel order response")
    return data


def post_futures_change_margin_type(
    *,
    symbol: str,
    margin_type: str,
    api_key: str,
    api_secret: str,
    contract_type: str = "usdm",
    recv_window: int = 5000,
) -> dict[str, Any]:
    data = _http_signed_request_json(
        f"{_futures_trade_base_url(contract_type)}/fapi/v1/marginType",
        {
            "symbol": symbol.upper(),
            "marginType": str(margin_type).upper().strip(),
            "recvWindow": recv_window,
        },
        api_key,
        api_secret,
        method="POST",
    )
    if not isinstance(data, dict):
        raise RuntimeError("Unexpected futures margin type response")
    return data


def post_futures_change_initial_leverage(
    *,
    symbol: str,
    leverage: int,
    api_key: str,
    api_secret: str,
    contract_type: str = "usdm",
    recv_window: int = 5000,
) -> dict[str, Any]:
    data = _http_signed_request_json(
        f"{_futures_trade_base_url(contract_type)}/fapi/v1/leverage",
        {
            "symbol": symbol.upper(),
            "leverage": int(leverage),
            "recvWindow": recv_window,
        },
        api_key,
        api_secret,
        method="POST",
    )
    if not isinstance(data, dict):
        raise RuntimeError("Unexpected futures leverage response")
    return data


def _symbols_cache_path(cache_dir: str | Path = "data", contract_type: str = "usdm") -> Path:
    normalized = normalize_contract_type(contract_type)
    if normalized == "usdm":
        return Path(cache_dir) / "futures_symbols.json"
    return Path(cache_dir) / f"futures_symbols_{normalized}.json"


def load_or_fetch_futures_symbols(
    contract_type: str = "usdm",
    cache_dir: str | Path = "data",
    refresh: bool = False,
) -> list[str]:
    normalized = normalize_contract_type(contract_type)
    path = _symbols_cache_path(cache_dir, normalized)
    now_ts = int(time.time())
    stale_symbols: list[str] = []
    if not refresh and path.exists():
        try:
            with path.open("r", encoding="utf-8") as f:
                payload = json.load(f)
            fetched_at = int(payload.get("fetched_at", 0))
            symbols = payload.get("symbols", [])
            normalized = [str(x).upper().strip() for x in symbols if str(x).strip()]
            if normalized:
                stale_symbols = normalized
            if stale_symbols and now_ts - fetched_at <= SYMBOL_CACHE_TTL_SECONDS:
                return stale_symbols
        except (ValueError, OSError, json.JSONDecodeError):
            pass

    try:
        symbols = fetch_futures_symbols(contract_type=normalized)
    except Exception:
        if stale_symbols:
            return stale_symbols
        raise
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump({"fetched_at": now_ts, "symbols": symbols}, f, ensure_ascii=False)
    return symbols


def fetch_futures_klines(
    symbol: str,
    interval: str,
    start_ms: int,
    end_ms: int,
    contract_type: str = "usdm",
    limit: int = 1500,
) -> list[Candle]:
    if start_ms >= end_ms:
        raise ValueError("start_ms must be < end_ms")
    interval_ms = parse_interval_ms(interval)
    normalized_contract_type = normalize_contract_type(contract_type)
    cursor = start_ms
    rows: list[list] = []
    # Binance endpoints may truncate to `limit` bars when the requested
    # time-span is too wide (even if startTime is set). To keep pagination
    # stable and lossless, each request window is capped to <= limit bars.
    max_candle_span_ms = max(interval_ms * max(limit - 1, 1), interval_ms)
    contract_window_cap_ms = (
        COINM_MAX_KLINE_WINDOW_MS - 1
        if normalized_contract_type == "coinm"
        else max_candle_span_ms
    )
    request_window_ms = min(max_candle_span_ms, contract_window_cap_ms)
    if request_window_ms <= 0:
        request_window_ms = interval_ms

    while cursor < end_ms:
        request_end_ms = min(end_ms - 1, cursor + request_window_ms)
        data = _http_get_json(
            _market_api_urls(normalized_contract_type)["kline"],
            {
                "symbol": symbol.upper(),
                "interval": interval,
                "startTime": cursor,
                "endTime": request_end_ms,
                "limit": limit,
            },
        )
        if not data:
            next_cursor = request_end_ms + interval_ms
            if next_cursor <= cursor:
                break
            cursor = next_cursor
            continue
        rows.extend(data)
        last_open = max(int(row[0]) for row in data)
        next_cursor = last_open + interval_ms
        if next_cursor <= cursor:
            next_cursor = request_end_ms + interval_ms
            if next_cursor <= cursor:
                break
        cursor = next_cursor
        if len(data) >= limit:
            time.sleep(0.03)

    dedup: dict[int, Candle] = {}
    for row in rows:
        open_ms = int(row[0])
        close_ms = int(row[6])
        if open_ms < start_ms or open_ms >= end_ms:
            continue
        dedup[open_ms] = Candle(
            open_time=datetime.fromtimestamp(open_ms / 1000, tz=timezone.utc),
            close_time=datetime.fromtimestamp(close_ms / 1000, tz=timezone.utc),
            open=float(row[1]),
            high=float(row[2]),
            low=float(row[3]),
            close=float(row[4]),
        )
    candles = [dedup[key] for key in sorted(dedup)]
    return candles


def fetch_futures_quote_volume_sum(
    symbol: str,
    *,
    window_minutes: int,
    contract_type: str = "usdm",
    end_time_ms: int | None = None,
    interval: str = "1m",
    limit: int = 1500,
) -> float:
    if window_minutes <= 0:
        raise ValueError("window_minutes must be > 0")
    if interval != "1m":
        raise ValueError("only 1m interval is supported for quote volume aggregation")
    if limit <= 0:
        raise ValueError("limit must be > 0")

    interval_ms = parse_interval_ms(interval)
    window_ms = int(window_minutes) * 60 * 1000
    end_ms = int(end_time_ms if end_time_ms is not None else time.time() * 1000)
    start_ms = end_ms - window_ms
    if start_ms >= end_ms:
        return 0.0

    normalized_contract_type = normalize_contract_type(contract_type)
    cursor = start_ms
    request_span_ms = max(interval_ms * max(limit - 1, 1), interval_ms)
    quote_volume_sum = 0.0
    seen_open_times: set[int] = set()

    while cursor < end_ms:
        request_end_ms = min(end_ms - 1, cursor + request_span_ms)
        data = _http_get_json(
            _market_api_urls(normalized_contract_type)["kline"],
            {
                "symbol": symbol.upper(),
                "interval": interval,
                "startTime": cursor,
                "endTime": request_end_ms,
                "limit": limit,
            },
        )
        if not data:
            next_cursor = request_end_ms + interval_ms
            if next_cursor <= cursor:
                break
            cursor = next_cursor
            continue

        last_open = cursor
        for row in data:
            open_ms = int(row[0])
            if open_ms < start_ms or open_ms >= end_ms or open_ms in seen_open_times:
                continue
            seen_open_times.add(open_ms)
            quote_volume_sum += float(row[7])
            last_open = max(last_open, open_ms)

        next_cursor = last_open + interval_ms
        if next_cursor <= cursor:
            next_cursor = request_end_ms + interval_ms
            if next_cursor <= cursor:
                break
        cursor = next_cursor
        if len(data) >= limit:
            time.sleep(0.03)

    return quote_volume_sum


def fetch_futures_window_price_stats(
    symbol: str,
    *,
    window_minutes: int,
    contract_type: str = "usdm",
    end_time_ms: int | None = None,
    interval: str = "1m",
    limit: int = 1500,
) -> dict[str, float | int | None]:
    if window_minutes <= 0:
        raise ValueError("window_minutes must be > 0")
    if interval != "1m":
        raise ValueError("only 1m interval is supported for window price stats")
    if limit <= 0:
        raise ValueError("limit must be > 0")

    end_ms = int(end_time_ms if end_time_ms is not None else time.time() * 1000)
    start_ms = end_ms - int(window_minutes) * 60 * 1000
    if start_ms >= end_ms:
        return {
            "window_minutes": int(window_minutes),
            "candle_count": 0,
            "open_price": None,
            "close_price": None,
            "high_price": None,
            "low_price": None,
            "return_ratio": 0.0,
            "amplitude_ratio": 0.0,
        }

    candles = fetch_futures_klines(
        symbol=symbol,
        interval=interval,
        start_ms=start_ms,
        end_ms=end_ms,
        contract_type=contract_type,
        limit=limit,
    )
    if not candles:
        return {
            "window_minutes": int(window_minutes),
            "candle_count": 0,
            "open_price": None,
            "close_price": None,
            "high_price": None,
            "low_price": None,
            "return_ratio": 0.0,
            "amplitude_ratio": 0.0,
        }

    open_price = float(candles[0].open)
    close_price = float(candles[-1].close)
    high_price = max(float(candle.high) for candle in candles)
    low_price = min(float(candle.low) for candle in candles)
    return_ratio = close_price / open_price - 1.0 if open_price > 0 else 0.0
    amplitude_ratio = high_price / low_price - 1.0 if low_price > 0 else 0.0
    return {
        "window_minutes": int(window_minutes),
        "candle_count": len(candles),
        "open_price": open_price,
        "close_price": close_price,
        "high_price": high_price,
        "low_price": low_price,
        "return_ratio": return_ratio,
        "amplitude_ratio": amplitude_ratio,
    }


def _cache_file_path(
    cache_dir: Path,
    symbol: str,
    interval: str,
    contract_type: str = "usdm",
    market_type: str = "futures",
) -> Path:
    # Unified cache per symbol+interval so different lookback windows can reuse the same local data.
    normalized_market_type = normalize_market_type(market_type)
    filename = f"{symbol.upper()}_{interval}.csv"
    if normalized_market_type == "spot":
        return cache_dir / f"spot_{filename}"
    normalized = normalize_contract_type(contract_type)
    if normalized == "usdm":
        return cache_dir / filename
    return cache_dir / f"{normalized}_{filename}"


def _funding_cache_file_path(
    cache_dir: Path,
    symbol: str,
    contract_type: str = "usdm",
    market_type: str = "futures",
) -> Path:
    normalized_market_type = normalize_market_type(market_type)
    if normalized_market_type == "spot":
        return cache_dir / f"spot_{symbol.upper()}_funding.csv"
    normalized = normalize_contract_type(contract_type)
    filename = f"{symbol.upper()}_funding.csv"
    if normalized == "usdm":
        return cache_dir / filename
    return cache_dir / f"{normalized}_{filename}"


def _legacy_cache_file_path(
    cache_dir: Path,
    symbol: str,
    interval: str,
    lookback_days: int,
    contract_type: str = "usdm",
    market_type: str = "futures",
) -> Path:
    normalized_market_type = normalize_market_type(market_type)
    if normalized_market_type == "spot":
        return cache_dir / f"spot_{symbol.upper()}_{interval}_{lookback_days}d.csv"
    normalized = normalize_contract_type(contract_type)
    filename = f"{symbol.upper()}_{interval}_{lookback_days}d.csv"
    if normalized == "usdm":
        return cache_dir / filename
    return cache_dir / f"{normalized}_{filename}"


def cache_file_path(
    symbol: str,
    interval: str,
    cache_dir: str | Path = "data",
    contract_type: str = "usdm",
    market_type: str = "futures",
) -> Path:
    return _cache_file_path(Path(cache_dir), symbol, interval, contract_type, market_type)


def read_latest_cached_close(
    symbol: str,
    intervals: tuple[str, ...] = ("1m", "1h", "4h"),
    cache_dir: str | Path = "data",
    contract_type: str = "usdm",
    market_type: str = "futures",
) -> float | None:
    normalized_market_type = normalize_market_type(market_type)
    normalized = normalize_contract_type(contract_type) if normalized_market_type == "futures" else "usdm"
    base = Path(cache_dir)
    for interval in intervals:
        paths = [_cache_file_path(base, symbol, interval, normalized, normalized_market_type)]
        if normalized_market_type == "futures" and normalized == "usdm":
            # Backward compatibility with legacy cache naming.
            paths.append(base / f"{symbol.upper()}_{interval}.csv")
        path = next((p for p in paths if p.exists()), None)
        if path is None:
            continue
        try:
            last_line: str | None = None
            with path.open("r", encoding="utf-8") as f:
                _ = f.readline()
                for line in f:
                    row = line.strip()
                    if row:
                        last_line = row
            if not last_line:
                continue
            parts = last_line.split(",")
            if len(parts) < 6:
                continue
            return float(parts[5])
        except (OSError, ValueError):
            continue
    return None


def funding_cache_file_path(
    symbol: str,
    cache_dir: str | Path = "data",
    contract_type: str = "usdm",
    market_type: str = "futures",
) -> Path:
    return _funding_cache_file_path(Path(cache_dir), symbol, contract_type, market_type)


def _cache_has_ohlc_columns(path: Path) -> bool:
    with path.open("r", encoding="utf-8") as f:
        header = f.readline().strip().split(",")
    needed = {"open", "high", "low", "close"}
    return needed.issubset(set(header))


def save_candles_to_csv(path: Path, candles: list[Candle]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["open_time_ms", "close_time_ms", "open", "high", "low", "close"])
        for candle in candles:
            writer.writerow(
                [
                    int(candle.open_time.timestamp() * 1000),
                    int(candle.close_time.timestamp() * 1000),
                    f"{candle.open:.8f}",
                    f"{candle.high:.8f}",
                    f"{candle.low:.8f}",
                    f"{candle.close:.8f}",
                ]
            )


def load_candles_from_csv(path: Path) -> list[Candle]:
    candles: list[Candle] = []
    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            open_ms = int(row["open_time_ms"])
            close_ms = int(row["close_time_ms"])
            # Backward compatible with old cache files that only contain close.
            close_price = float(row["close"])
            open_price = float(row["open"]) if row.get("open") else close_price
            high_price = float(row["high"]) if row.get("high") else close_price
            low_price = float(row["low"]) if row.get("low") else close_price
            candles.append(
                Candle(
                    open_time=datetime.fromtimestamp(open_ms / 1000, tz=timezone.utc),
                    close_time=datetime.fromtimestamp(close_ms / 1000, tz=timezone.utc),
                    open=open_price,
                    high=high_price,
                    low=low_price,
                    close=close_price,
                )
            )
    return candles


def load_or_fetch_candles(
    symbol: str,
    interval: str,
    lookback_days: int | None = None,
    start_time: datetime | None = None,
    end_time: datetime | None = None,
    cache_dir: str | Path = "data",
    contract_type: str = "usdm",
    market_type: str = "futures",
    refresh: bool = False,
) -> list[Candle]:
    normalized_market_type = normalize_market_type(market_type)
    normalized_contract_type = normalize_contract_type(contract_type) if normalized_market_type == "futures" else "usdm"
    if (start_time is None) != (end_time is None):
        raise ValueError("start_time and end_time must be provided together")
    if start_time is None and end_time is None:
        if lookback_days is None:
            lookback_days = 365
        if lookback_days <= 0:
            raise ValueError("lookback_days must be > 0")

    def _dedup_sorted(candles: list[Candle]) -> list[Candle]:
        dedup: dict[int, Candle] = {}
        for candle in candles:
            dedup[int(candle.open_time.timestamp() * 1000)] = candle
        return [dedup[key] for key in sorted(dedup)]

    def _slice_range(candles: list[Candle], start_ms: int, end_ms: int) -> list[Candle]:
        if not candles:
            return []
        sliced = []
        for item in candles:
            open_ms = int(item.open_time.timestamp() * 1000)
            if start_ms <= open_ms < end_ms:
                sliced.append(item)
        return sliced

    def _find_missing_ranges(
        candles_in_range: list[Candle],
        start_ms: int,
        end_ms: int,
        step_ms: int,
    ) -> list[tuple[int, int]]:
        if start_ms >= end_ms:
            return []
        if not candles_in_range:
            return [(start_ms, end_ms)]

        missing: list[tuple[int, int]] = []
        cursor = start_ms
        for item in candles_in_range:
            open_ms = int(item.open_time.timestamp() * 1000)
            if open_ms > cursor:
                missing.append((cursor, min(open_ms, end_ms)))
            next_cursor = open_ms + step_ms
            if next_cursor > cursor:
                cursor = next_cursor
        if cursor < end_ms:
            missing.append((cursor, end_ms))
        return [(a, b) for a, b in missing if a < b]

    def _slice_range(candles: list[Candle], start_ms: int, end_ms: int) -> list[Candle]:
        if not candles:
            return []
        sliced: list[Candle] = []
        for candle in candles:
            open_ms = int(candle.open_time.timestamp() * 1000)
            if start_ms <= open_ms <= end_ms:
                sliced.append(candle)
        return sliced

    def _as_utc(dt: datetime) -> datetime:
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)

    cache_dir_path = Path(cache_dir)
    now = datetime.now(tz=timezone.utc)
    if start_time is not None and end_time is not None:
        st = start_time.astimezone(timezone.utc)
        et = end_time.astimezone(timezone.utc)
        if st >= et:
            raise ValueError("start_time must be before end_time")
        req_start_ms = int(st.timestamp() * 1000)
        req_end_ms = int(et.timestamp() * 1000)
    else:
        req_end_ms = int(now.timestamp() * 1000)
        req_start_ms = int((now - timedelta(days=int(lookback_days))).timestamp() * 1000)
    interval_ms = parse_interval_ms(interval)
    if interval_ms < 60_000 and (req_end_ms - req_start_ms) > SECOND_INTERVAL_MAX_SPAN_MS:
        raise ValueError(
            "second-level kline range cannot exceed 31 days per request"
        )

    def _fetch_range(fetch_start_ms: int, fetch_end_ms: int) -> list[Candle]:
        if normalized_market_type == "spot":
            return fetch_spot_klines(
                symbol=symbol,
                interval=interval,
                start_ms=fetch_start_ms,
                end_ms=fetch_end_ms,
            )
        return fetch_futures_klines(
            symbol=symbol,
            interval=interval,
            start_ms=fetch_start_ms,
            end_ms=fetch_end_ms,
            contract_type=normalized_contract_type,
        )

    cache_path = _cache_file_path(
        cache_dir_path,
        symbol,
        interval,
        normalized_contract_type,
        normalized_market_type,
    )
    legacy_path = None
    if lookback_days is not None and normalized_market_type == "futures":
        legacy_path = _legacy_cache_file_path(
            cache_dir_path,
            symbol,
            interval,
            int(lookback_days),
            normalized_contract_type,
            normalized_market_type,
        )

    cache_candles: list[Candle] = []
    cache_changed = False

    if refresh and cache_path.exists() and _cache_has_ohlc_columns(cache_path):
        cache_candles = load_candles_from_csv(cache_path)

    if not refresh:
        source_path: Path | None = None
        if cache_path.exists():
            source_path = cache_path
        elif legacy_path is not None and legacy_path.exists():
            source_path = legacy_path
        if source_path and _cache_has_ohlc_columns(source_path):
            cache_candles = load_candles_from_csv(source_path)
            if legacy_path is not None and source_path == legacy_path and cache_candles:
                cache_changed = True

    if refresh:
        fetched = _fetch_range(req_start_ms, req_end_ms)
        if not fetched:
            raise RuntimeError("No kline data fetched from Binance.")
        cache_candles = _dedup_sorted(cache_candles + fetched)
        cache_changed = True
    else:
        if cache_candles:
            first_open_ms = int(cache_candles[0].open_time.timestamp() * 1000)
            last_open_ms = int(cache_candles[-1].open_time.timestamp() * 1000)
            # Only fetch missing data inside the requested window; avoid
            # backfilling unrelated historical gaps between request and cache.
            if req_end_ms <= first_open_ms:
                older = _fetch_range(req_start_ms, req_end_ms)
                if older:
                    cache_candles = _dedup_sorted(older + cache_candles)
                    cache_changed = True
            elif req_start_ms >= last_open_ms + interval_ms:
                newer = _fetch_range(req_start_ms, req_end_ms)
                if newer:
                    cache_candles = _dedup_sorted(cache_candles + newer)
                    cache_changed = True
            else:
                if req_start_ms < first_open_ms:
                    older_end_ms = min(first_open_ms, req_end_ms)
                    if req_start_ms < older_end_ms:
                        older = _fetch_range(req_start_ms, older_end_ms)
                        if older:
                            cache_candles = _dedup_sorted(older + cache_candles)
                            cache_changed = True

                next_start_ms = last_open_ms + interval_ms
                newer_start_ms = max(next_start_ms, req_start_ms)
                if newer_start_ms < req_end_ms:
                    newer = _fetch_range(newer_start_ms, req_end_ms)
                    if newer:
                        cache_candles = _dedup_sorted(cache_candles + newer)
                        cache_changed = True
        else:
            fetched = _fetch_range(req_start_ms, req_end_ms)
            if not fetched:
                raise RuntimeError("No kline data fetched from Binance.")
            cache_candles = fetched
            cache_changed = True

    if cache_changed:
        save_candles_to_csv(cache_path, cache_candles)

    sliced = _slice_range(cache_candles, start_ms=req_start_ms, end_ms=req_end_ms)

    # Repair internal gaps inside the requested window (can happen if old cache
    # was built by interrupted runs or legacy buggy versions).
    missing_ranges = _find_missing_ranges(
        candles_in_range=sliced,
        start_ms=req_start_ms,
        end_ms=req_end_ms,
        step_ms=interval_ms,
    )
    if missing_ranges:
        repaired = False
        for miss_start_ms, miss_end_ms in missing_ranges:
            fetched_missing = _fetch_range(miss_start_ms, miss_end_ms)
            if fetched_missing:
                cache_candles = _dedup_sorted(cache_candles + fetched_missing)
                repaired = True
        if repaired:
            save_candles_to_csv(cache_path, cache_candles)
            sliced = _slice_range(cache_candles, start_ms=req_start_ms, end_ms=req_end_ms)

    if not sliced:
        raise RuntimeError("No candle data available after loading cache.")
    return sliced


def fetch_funding_rates(
    symbol: str,
    start_ms: int,
    end_ms: int,
    contract_type: str = "usdm",
    limit: int = 1000,
) -> list[FundingRate]:
    if start_ms >= end_ms:
        raise ValueError("start_ms must be < end_ms")
    cursor = start_ms
    rows: list[dict[str, Any]] = []
    max_window_ms = max(FUNDING_DEFAULT_STEP_MS * max(limit - 1, 1), FUNDING_DEFAULT_STEP_MS)

    while cursor < end_ms:
        request_end_ms = min(end_ms, cursor + max_window_ms)
        data = _http_get_json(
            _market_api_urls(contract_type)["funding_rate"],
            {
                "symbol": symbol.upper(),
                "startTime": cursor,
                "endTime": request_end_ms,
                "limit": limit,
            },
        )
        if not isinstance(data, list):
            raise RuntimeError("Unexpected fundingRate response payload")
        if not data:
            next_cursor = request_end_ms + 1
            if next_cursor <= cursor:
                break
            cursor = next_cursor
            continue
        rows.extend(data)
        last_ms = int(data[-1]["fundingTime"])
        next_cursor = last_ms + 1
        if next_cursor <= cursor:
            next_cursor = request_end_ms + 1
            if next_cursor <= cursor:
                break
        cursor = next_cursor
        if len(data) < limit:
            if request_end_ms >= end_ms:
                break
            if cursor <= request_end_ms:
                cursor = request_end_ms + 1
            continue
        time.sleep(0.05)

    dedup: dict[int, FundingRate] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        ts_ms = int(row["fundingTime"])
        if ts_ms < start_ms or ts_ms >= end_ms:
            continue
        dedup[ts_ms] = FundingRate(
            ts=datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc),
            rate=float(row["fundingRate"]),
        )
    return [dedup[key] for key in sorted(dedup)]


def fetch_recent_funding_records(
    contract_type: str = "usdm",
    symbol: str | None = None,
    limit: int = 1000,
) -> list[dict[str, Any]]:
    params: dict[str, str | int] = {"limit": limit}
    if symbol:
        params["symbol"] = str(symbol).upper().strip()
    data = _http_get_json(_market_api_urls(contract_type)["funding_rate"], params)
    if not isinstance(data, list):
        raise RuntimeError("Unexpected fundingRate response payload")
    rows: list[dict[str, Any]] = []
    for item in data:
        if not isinstance(item, dict):
            continue
        funding_symbol = str(item.get("symbol", symbol or "")).upper().strip()
        funding_time_raw = str(item.get("fundingTime", "")).strip()
        funding_rate = _safe_float(item.get("fundingRate"))
        mark_price = _safe_positive_float(item.get("markPrice"))
        if not funding_symbol or not funding_time_raw or funding_rate is None:
            continue
        rows.append(
            {
                "symbol": funding_symbol,
                "funding_time": int(funding_time_raw),
                "funding_rate": funding_rate,
                "mark_price": mark_price,
            }
        )
    return rows


def save_funding_rates_to_csv(path: Path, rates: list[FundingRate]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["funding_time_ms", "funding_rate"])
        for item in rates:
            writer.writerow([int(item.ts.timestamp() * 1000), f"{item.rate:.12f}"])


def load_funding_rates_from_csv(path: Path) -> list[FundingRate]:
    rates: list[FundingRate] = []
    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ts_ms = int(row["funding_time_ms"])
            rates.append(
                FundingRate(
                    ts=datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc),
                    rate=float(row["funding_rate"]),
                )
            )
    return rates


def load_or_fetch_funding_rates(
    symbol: str,
    start_time: datetime,
    end_time: datetime,
    cache_dir: str | Path = "data",
    contract_type: str = "usdm",
    market_type: str = "futures",
    refresh: bool = False,
) -> list[FundingRate]:
    normalized_market_type = normalize_market_type(market_type)
    if normalized_market_type == "spot":
        return []
    normalized_contract_type = normalize_contract_type(contract_type)
    st = start_time.astimezone(timezone.utc)
    et = end_time.astimezone(timezone.utc)
    if st >= et:
        raise ValueError("start_time must be before end_time")

    req_start_ms = int(st.timestamp() * 1000)
    req_end_ms = int(et.timestamp() * 1000)

    def _dedup_sorted(items: list[FundingRate]) -> list[FundingRate]:
        dedup: dict[int, FundingRate] = {}
        for item in items:
            dedup[int(item.ts.timestamp() * 1000)] = item
        return [dedup[key] for key in sorted(dedup)]

    def _slice(items: list[FundingRate], start_ms: int, end_ms: int) -> list[FundingRate]:
        out: list[FundingRate] = []
        for item in items:
            ts_ms = int(item.ts.timestamp() * 1000)
            if start_ms <= ts_ms < end_ms:
                out.append(item)
        return out

    def _find_missing_ranges(
        items_in_range: list[FundingRate],
        start_ms: int,
        end_ms: int,
        gap_threshold_ms: int = FUNDING_MISSING_GAP_THRESHOLD_MS,
    ) -> list[tuple[int, int]]:
        if start_ms >= end_ms:
            return []
        if not items_in_range:
            return [(start_ms, end_ms)]

        missing: list[tuple[int, int]] = []
        cursor_ms = start_ms
        for item in items_in_range:
            ts_ms = int(item.ts.timestamp() * 1000)
            if ts_ms - cursor_ms > gap_threshold_ms:
                missing.append((cursor_ms, min(ts_ms, end_ms)))
            if ts_ms + 1 > cursor_ms:
                cursor_ms = ts_ms + 1
        if end_ms - cursor_ms > gap_threshold_ms:
            missing.append((cursor_ms, end_ms))
        return [(a, b) for a, b in missing if a < b]

    cache_dir_path = Path(cache_dir)
    cache_path = _funding_cache_file_path(
        cache_dir_path,
        symbol,
        normalized_contract_type,
        normalized_market_type,
    )
    cache_rates: list[FundingRate] = []
    cache_changed = False

    if cache_path.exists():
        try:
            cache_rates = load_funding_rates_from_csv(cache_path)
        except (ValueError, OSError, KeyError):
            cache_rates = []

    if refresh:
        fetched = fetch_funding_rates(
            symbol=symbol,
            start_ms=req_start_ms,
            end_ms=req_end_ms,
            contract_type=normalized_contract_type,
        )
        cache_rates = _dedup_sorted(cache_rates + fetched)
        cache_changed = bool(fetched) or not cache_path.exists()
    else:
        if cache_rates:
            first_ms = int(cache_rates[0].ts.timestamp() * 1000)
            last_ms = int(cache_rates[-1].ts.timestamp() * 1000)
            # Same policy as kline cache: fetch only requested-window gaps.
            if req_end_ms <= first_ms:
                older = fetch_funding_rates(
                    symbol=symbol,
                    start_ms=req_start_ms,
                    end_ms=req_end_ms,
                    contract_type=normalized_contract_type,
                )
                if older:
                    cache_rates = _dedup_sorted(older + cache_rates)
                    cache_changed = True
            elif req_start_ms > last_ms:
                newer = fetch_funding_rates(
                    symbol=symbol,
                    start_ms=req_start_ms,
                    end_ms=req_end_ms,
                    contract_type=normalized_contract_type,
                )
                if newer:
                    cache_rates = _dedup_sorted(cache_rates + newer)
                    cache_changed = True
            else:
                if req_start_ms < first_ms:
                    older_end_ms = min(first_ms, req_end_ms)
                    if req_start_ms < older_end_ms:
                        older = fetch_funding_rates(
                            symbol=symbol,
                            start_ms=req_start_ms,
                            end_ms=older_end_ms,
                            contract_type=normalized_contract_type,
                        )
                        if older:
                            cache_rates = _dedup_sorted(older + cache_rates)
                            cache_changed = True
                newer_start_ms = max(last_ms + 1, req_start_ms)
                if newer_start_ms < req_end_ms:
                    newer = fetch_funding_rates(
                        symbol=symbol,
                        start_ms=newer_start_ms,
                        end_ms=req_end_ms,
                        contract_type=normalized_contract_type,
                    )
                    if newer:
                        cache_rates = _dedup_sorted(cache_rates + newer)
                        cache_changed = True
        else:
            fetched = fetch_funding_rates(
                symbol=symbol,
                start_ms=req_start_ms,
                end_ms=req_end_ms,
                contract_type=normalized_contract_type,
            )
            if fetched:
                cache_rates = fetched
                cache_changed = True
            elif not cache_path.exists():
                # Ensure we keep a valid empty cache when symbol has no historical funding records.
                cache_changed = True

    if cache_changed:
        save_funding_rates_to_csv(cache_path, cache_rates)
    sliced = _slice(cache_rates, req_start_ms, req_end_ms)

    # Repair internal funding gaps inside requested range. This is especially
    # important for legacy/partial caches generated by interrupted runs.
    missing_ranges = _find_missing_ranges(
        items_in_range=sliced,
        start_ms=req_start_ms,
        end_ms=req_end_ms,
    )
    if missing_ranges:
        repaired = False
        for miss_start_ms, miss_end_ms in missing_ranges:
            fetched_missing = fetch_funding_rates(
                symbol=symbol,
                start_ms=miss_start_ms,
                end_ms=miss_end_ms,
                contract_type=normalized_contract_type,
            )
            if fetched_missing:
                cache_rates = _dedup_sorted(cache_rates + fetched_missing)
                repaired = True
        if repaired:
            save_funding_rates_to_csv(cache_path, cache_rates)
            sliced = _slice(cache_rates, req_start_ms, req_end_ms)

    return sliced


def fetch_margin_all_assets(api_key: str, asset: str | None = None) -> list[dict[str, Any]]:
    params: dict[str, str] = {}
    if asset:
        params["asset"] = str(asset).upper().strip()
    data = _http_api_key_get_json("https://api.binance.com/sapi/v1/margin/allAssets", params, api_key)
    items = _as_list_payload(data, "margin allAssets")
    rows: list[dict[str, Any]] = []
    for item in items:
        asset_name = str(item.get("assetName", "")).upper().strip()
        if not asset_name:
            continue
        rows.append(
            {
                "asset": asset_name,
                "asset_full_name": str(item.get("assetFullName", "")).strip(),
                "is_borrowable": bool(item.get("isBorrowable", False)),
                "is_mortgageable": bool(item.get("isMortgageable", False)),
                "user_min_borrow": _safe_float(item.get("userMinBorrow")),
                "user_min_repay": _safe_float(item.get("userMinRepay")),
                "delist_time": int(item["delistTime"]) if str(item.get("delistTime", "")).strip() else None,
            }
        )
    return rows


def fetch_margin_restricted_assets(api_key: str) -> dict[str, list[str]]:
    data = _http_api_key_get_json("https://api.binance.com/sapi/v1/margin/restricted-asset", {}, api_key)
    if not isinstance(data, dict):
        raise RuntimeError("Unexpected restricted-asset response")
    return {
        "open_long_restricted": [str(x).upper().strip() for x in data.get("openLongRestrictedAsset", [])],
        "max_collateral_exceeded": [str(x).upper().strip() for x in data.get("maxCollateralExceededAsset", [])],
    }


def fetch_margin_available_inventory(api_key: str, margin_type: str = "MARGIN") -> dict[str, Any]:
    normalized_type = str(margin_type or "MARGIN").strip().upper()
    if normalized_type not in {"MARGIN", "ISOLATED"}:
        raise ValueError("margin_type must be MARGIN or ISOLATED")
    data = _http_api_key_get_json(
        "https://api.binance.com/sapi/v1/margin/available-inventory",
        {"type": normalized_type},
        api_key,
    )
    if not isinstance(data, dict):
        raise RuntimeError("Unexpected available-inventory response")
    assets_raw = data.get("assets", {})
    assets: dict[str, float] = {}
    if isinstance(assets_raw, dict):
        for asset_name, value in assets_raw.items():
            amount = _safe_float(value)
            if amount is None:
                continue
            assets[str(asset_name).upper().strip()] = amount
    update_time = _safe_float(data.get("updateTime"))
    return {
        "type": normalized_type,
        "assets": assets,
        "update_time": int(update_time) if update_time is not None else None,
    }


def fetch_margin_max_borrowable(
    asset: str,
    api_key: str,
    api_secret: str,
    isolated_symbol: str | None = None,
) -> dict[str, Any]:
    params: dict[str, str | int] = {"asset": str(asset).upper().strip()}
    if isolated_symbol:
        params["isolatedSymbol"] = str(isolated_symbol).upper().strip()
    data = _http_signed_get_json(
        "https://api.binance.com/sapi/v1/margin/maxBorrowable",
        params,
        api_key,
        api_secret,
    )
    if not isinstance(data, dict):
        raise RuntimeError("Unexpected maxBorrowable response")
    return {
        "asset": str(asset).upper().strip(),
        "isolated_symbol": str(isolated_symbol or "").upper().strip() or None,
        "amount": _safe_float(data.get("amount")),
        "borrow_limit": _safe_float(data.get("borrowLimit")),
    }


def fetch_margin_next_hourly_interest_rates(
    assets: list[str],
    is_isolated: bool,
    api_key: str,
) -> list[dict[str, Any]]:
    normalized_assets = [str(x).upper().strip() for x in assets if str(x).strip()]
    if not normalized_assets:
        return []
    data = _http_api_key_get_json(
        "https://api.binance.com/sapi/v1/margin/next-hourly-interest-rate",
        {
            "assets": ",".join(normalized_assets[:20]),
            "isIsolated": "TRUE" if is_isolated else "FALSE",
        },
        api_key,
    )
    items = _as_list_payload(data, "margin next-hourly-interest-rate")
    rows: list[dict[str, Any]] = []
    for item in items:
        asset_name = str(item.get("asset", "")).upper().strip()
        next_rate = _safe_float(item.get("nextHourlyInterestRate"))
        if not asset_name or next_rate is None:
            continue
        rows.append({"asset": asset_name, "next_hourly_interest_rate": next_rate})
    return rows


def fetch_margin_isolated_all_pairs(
    api_key: str,
    api_secret: str,
    symbol: str | None = None,
) -> list[dict[str, Any]]:
    params: dict[str, str | int] = {}
    if symbol:
        params["symbol"] = str(symbol).upper().strip()
    data = _http_signed_get_json(
        "https://api.binance.com/sapi/v1/margin/isolated/allPairs",
        params,
        api_key,
        api_secret,
    )
    items = _as_list_payload(data, "margin isolated allPairs")
    rows: list[dict[str, Any]] = []
    for item in items:
        pair_symbol = str(item.get("symbol", "")).upper().strip()
        if not pair_symbol:
            continue
        rows.append(
            {
                "symbol": pair_symbol,
                "base": str(item.get("base", "")).upper().strip(),
                "quote": str(item.get("quote", "")).upper().strip(),
                "is_margin_trade": bool(item.get("isMarginTrade", False)),
                "is_buy_allowed": bool(item.get("isBuyAllowed", False)),
                "is_sell_allowed": bool(item.get("isSellAllowed", False)),
            }
        )
    return rows


def fetch_vip_loanable_assets_data(
    api_key: str,
    api_secret: str,
    loan_coin: str | None = None,
) -> list[dict[str, Any]]:
    params: dict[str, str | int] = {}
    if loan_coin:
        params["loanCoin"] = str(loan_coin).upper().strip()
    data = _http_signed_get_json(
        "https://api.binance.com/sapi/v1/loan/vip/loanable/data",
        params,
        api_key,
        api_secret,
    )
    if not isinstance(data, dict):
        raise RuntimeError("Unexpected vip loanable/data response")
    rows_raw = data.get("rows", [])
    if not isinstance(rows_raw, list):
        raise RuntimeError("Unexpected vip loanable/data rows payload")
    rows: list[dict[str, Any]] = []
    for item in rows_raw:
        if not isinstance(item, dict):
            continue
        asset_name = str(item.get("loanCoin", "")).upper().strip()
        if not asset_name:
            continue
        rows.append(
            {
                "asset": asset_name,
                "flexible_daily_interest_rate": _safe_float(item.get("_flexibleDailyInterestRate")),
                "flexible_yearly_interest_rate": _safe_float(item.get("_flexibleYearlyInterestRate")),
                "min_limit": _safe_float(item.get("minLimit")),
                "max_limit": _safe_float(item.get("maxLimit")),
                "vip_level": int(item["vipLevel"]) if str(item.get("vipLevel", "")).strip() else None,
            }
        )
    return rows


def fetch_vip_borrow_interest_rate(
    api_key: str,
    api_secret: str,
    loan_coins: list[str],
) -> list[dict[str, Any]]:
    normalized_assets = [str(x).upper().strip() for x in loan_coins if str(x).strip()]
    if not normalized_assets:
        return []
    data = _http_signed_get_json(
        "https://api.binance.com/sapi/v1/loan/vip/request/interestRate",
        {"loanCoin": ",".join(normalized_assets[:10])},
        api_key,
        api_secret,
    )
    items = _as_list_payload(data, "vip request/interestRate")
    rows: list[dict[str, Any]] = []
    for item in items:
        asset_name = str(item.get("asset", "")).upper().strip()
        if not asset_name:
            continue
        rows.append(
            {
                "asset": asset_name,
                "flexible_daily_interest_rate": _safe_float(item.get("flexibleDailyInterestRate")),
                "flexible_yearly_interest_rate": _safe_float(item.get("flexibleYearlyInterestRate")),
                "time": int(item["time"]) if str(item.get("time", "")).strip() else None,
            }
        )
    return rows
