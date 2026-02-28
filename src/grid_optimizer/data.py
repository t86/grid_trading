from __future__ import annotations

import csv
import json
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .types import Candle

FUTURES_KLINE_URL = "https://fapi.binance.com/fapi/v1/klines"
FUTURES_AGG_TRADES_URL = "https://fapi.binance.com/fapi/v1/aggTrades"
FUTURES_EXCHANGE_INFO_URL = "https://fapi.binance.com/fapi/v1/exchangeInfo"
DEFAULT_TIMEOUT_SECONDS = 30


def parse_interval_ms(interval: str) -> int:
    match = re.fullmatch(r"(\d+)([smhdwM])", interval.strip())
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


def _aggregate_trades_to_1s_candles(
    rows: list[dict],
    start_ms: int,
    end_ms: int,
    last_close: float | None = None,
) -> tuple[list[Candle], float | None]:
    buckets: dict[int, list[float]] = {}
    sorted_rows = sorted(rows, key=lambda x: (int(x.get("T", 0)), int(x.get("a", 0))))
    for row in sorted_rows:
        ts_ms = int(row.get("T", 0))
        if ts_ms < start_ms or ts_ms > end_ms:
            continue
        sec_ms = (ts_ms // 1000) * 1000
        price = float(row["p"])
        bucket = buckets.get(sec_ms)
        if bucket is None:
            buckets[sec_ms] = [price, price, price, price]
        else:
            bucket[1] = max(bucket[1], price)
            bucket[2] = min(bucket[2], price)
            bucket[3] = price

    candles: list[Candle] = []
    sec_start = (start_ms // 1000) * 1000
    sec_end = (end_ms // 1000) * 1000
    for sec_ms in range(sec_start, sec_end + 1, 1000):
        bucket = buckets.get(sec_ms)
        if bucket is not None:
            open_price, high_price, low_price, close_price = bucket
            last_close = close_price
        elif last_close is not None:
            open_price = last_close
            high_price = last_close
            low_price = last_close
            close_price = last_close
        else:
            continue

        candles.append(
            Candle(
                open_time=datetime.fromtimestamp(sec_ms / 1000, tz=timezone.utc),
                close_time=datetime.fromtimestamp((sec_ms + 999) / 1000, tz=timezone.utc),
                open=open_price,
                high=high_price,
                low=low_price,
                close=close_price,
            )
        )

    return candles, last_close


def _fetch_agg_trades_segment(
    symbol: str,
    start_ms: int,
    end_ms: int,
    limit: int = 1000,
) -> list[dict]:
    if start_ms > end_ms:
        return []

    seed = _http_get_json(
        FUTURES_AGG_TRADES_URL,
        {
            "symbol": symbol.upper(),
            "startTime": start_ms,
            "endTime": end_ms,
            "limit": 1,
        },
    )
    if not seed:
        return []

    next_from_id = int(seed[0]["a"])
    rows: list[dict] = []
    while True:
        data = _http_get_json(
            FUTURES_AGG_TRADES_URL,
            {
                "symbol": symbol.upper(),
                "fromId": next_from_id,
                "limit": limit,
            },
        )
        if not data:
            break

        stop = False
        for row in data:
            ts_ms = int(row["T"])
            if ts_ms < start_ms:
                continue
            if ts_ms > end_ms:
                stop = True
                break
            rows.append(row)

        last_id = int(data[-1]["a"])
        if stop or len(data) < limit or last_id < next_from_id:
            break
        next_from_id = last_id + 1
        time.sleep(0.02)

    return rows


def fetch_futures_1s_candles(
    symbol: str,
    start_ms: int,
    end_ms: int,
    last_close_seed: float | None = None,
) -> list[Candle]:
    if start_ms >= end_ms:
        raise ValueError("start_ms must be < end_ms")

    segment_ms = 55 * 60_000  # Keep segment window below 1h constraint.
    cursor = start_ms
    candles: list[Candle] = []
    last_close = last_close_seed
    while cursor <= end_ms:
        seg_end = min(end_ms, cursor + segment_ms - 1)
        rows = _fetch_agg_trades_segment(symbol=symbol, start_ms=cursor, end_ms=seg_end)
        seg_candles, last_close = _aggregate_trades_to_1s_candles(
            rows=rows,
            start_ms=cursor,
            end_ms=seg_end,
            last_close=last_close,
        )
        candles.extend(seg_candles)
        if seg_candles:
            last_close = seg_candles[-1].close
        cursor = seg_end + 1
        time.sleep(0.02)

    return candles


def _http_get_payload(url: str, params: dict[str, str | int]) -> Any:
    query = urlencode(params)
    request = Request(
        f"{url}?{query}",
        headers={
            "User-Agent": "grid-optimizer/0.1",
            "Accept": "application/json",
        },
    )
    with urlopen(request, timeout=DEFAULT_TIMEOUT_SECONDS) as response:
        payload = response.read().decode("utf-8")
    data = json.loads(payload)
    if (
        isinstance(data, dict)
        and "code" in data
        and "msg" in data
        and "symbols" not in data
    ):
        code = data.get("code", "unknown")
        msg = data.get("msg", "unknown error")
        raise RuntimeError(f"Binance API error {code}: {msg}")
    return data


def _http_get_json(url: str, params: dict[str, str | int]) -> list:
    data = _http_get_payload(url, params)
    if not isinstance(data, list):
        raise RuntimeError(f"Unexpected response type from Binance: {type(data).__name__}")
    return data


def fetch_futures_exchange_info() -> dict[str, Any]:
    data = _http_get_payload(FUTURES_EXCHANGE_INFO_URL, {})
    if not isinstance(data, dict):
        raise RuntimeError(f"Unexpected exchangeInfo type: {type(data).__name__}")
    return data


def _filter_futures_symbols(
    exchange_info: dict[str, Any],
    quote_asset: str | None = "USDT",
    contract_type: str | None = "PERPETUAL",
    only_trading: bool = True,
) -> list[str]:
    raw_symbols = exchange_info.get("symbols", [])
    if not isinstance(raw_symbols, list):
        raise ValueError("exchange_info.symbols must be a list")

    quote = quote_asset.upper().strip() if quote_asset else ""
    ctype = contract_type.upper().strip() if contract_type else ""
    symbols: list[str] = []
    for item in raw_symbols:
        if not isinstance(item, dict):
            continue
        symbol = str(item.get("symbol", "")).upper().strip()
        if not symbol:
            continue
        status = str(item.get("status", "")).upper().strip()
        current_quote = str(item.get("quoteAsset", "")).upper().strip()
        current_contract = str(item.get("contractType", "")).upper().strip()

        if only_trading and status != "TRADING":
            continue
        if quote and current_quote != quote:
            continue
        if ctype and current_contract != ctype:
            continue
        symbols.append(symbol)

    return sorted(set(symbols))


def list_futures_symbols(
    quote_asset: str | None = "USDT",
    contract_type: str | None = "PERPETUAL",
    only_trading: bool = True,
) -> list[str]:
    exchange_info = fetch_futures_exchange_info()
    return _filter_futures_symbols(
        exchange_info=exchange_info,
        quote_asset=quote_asset,
        contract_type=contract_type,
        only_trading=only_trading,
    )


def fetch_futures_klines(
    symbol: str,
    interval: str,
    start_ms: int,
    end_ms: int,
    limit: int = 1500,
) -> list[Candle]:
    if start_ms >= end_ms:
        raise ValueError("start_ms must be < end_ms")
    interval_ms = parse_interval_ms(interval)
    cursor = start_ms
    rows: list[list] = []

    while cursor < end_ms:
        data = _http_get_json(
            FUTURES_KLINE_URL,
            {
                "symbol": symbol.upper(),
                "interval": interval,
                "startTime": cursor,
                "endTime": end_ms,
                "limit": limit,
            },
        )
        if not data:
            break
        rows.extend(data)
        last_open = int(data[-1][0])
        next_cursor = last_open + interval_ms
        if next_cursor <= cursor:
            break
        cursor = next_cursor
        if len(data) < limit:
            break
        time.sleep(0.05)

    dedup: dict[int, Candle] = {}
    for row in rows:
        open_ms = int(row[0])
        close_ms = int(row[6])
        if open_ms < start_ms or open_ms > end_ms:
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


def _cache_file_path(cache_dir: Path, symbol: str, interval: str) -> Path:
    # Unified cache per symbol+interval so different lookback windows can reuse the same local data.
    return cache_dir / f"{symbol.upper()}_{interval}.csv"


def _legacy_cache_file_path(cache_dir: Path, symbol: str, interval: str, lookback_days: int) -> Path:
    return cache_dir / f"{symbol.upper()}_{interval}_{lookback_days}d.csv"


def cache_file_path(symbol: str, interval: str, cache_dir: str | Path = "data") -> Path:
    return _cache_file_path(Path(cache_dir), symbol, interval)


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
    lookback_days: int | None = 365,
    cache_dir: str | Path = "data",
    refresh: bool = False,
    start_time: datetime | None = None,
    end_time: datetime | None = None,
) -> list[Candle]:
    def _dedup_sorted(candles: list[Candle]) -> list[Candle]:
        dedup: dict[int, Candle] = {}
        for candle in candles:
            dedup[int(candle.open_time.timestamp() * 1000)] = candle
        return [dedup[key] for key in sorted(dedup)]

    def _slice_lookback(candles: list[Candle], now_dt: datetime, days: int) -> list[Candle]:
        if not candles:
            return []
        start_dt = now_dt - timedelta(days=days)
        sliced = [x for x in candles if x.open_time >= start_dt]
        return sliced if sliced else candles

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

    use_explicit_range = start_time is not None or end_time is not None
    if use_explicit_range:
        end_dt = _as_utc(end_time) if end_time is not None else now
        if start_time is not None:
            start_dt = _as_utc(start_time)
        else:
            fallback_days = lookback_days if lookback_days is not None else 365
            if fallback_days <= 0:
                raise ValueError("lookback_days must be > 0")
            start_dt = end_dt - timedelta(days=fallback_days)
    else:
        days = lookback_days if lookback_days is not None else 365
        if days <= 0:
            raise ValueError("lookback_days must be > 0")
        end_dt = now
        start_dt = now - timedelta(days=days)

    if start_dt >= end_dt:
        raise ValueError("start_time must be earlier than end_time")

    end_ms = int(end_dt.timestamp() * 1000)
    req_start_ms = int(start_dt.timestamp() * 1000)
    interval_ms = parse_interval_ms(interval)

    cache_path = _cache_file_path(cache_dir_path, symbol, interval)
    legacy_days = lookback_days if lookback_days is not None else 365
    legacy_path = _legacy_cache_file_path(cache_dir_path, symbol, interval, legacy_days)

    cache_candles: list[Candle] = []
    cache_changed = False

    if interval == "1s":
        if not refresh and cache_path.exists() and _cache_has_ohlc_columns(cache_path):
            cache_candles = load_candles_from_csv(cache_path)

        fetch_start_ms = (req_start_ms // 1000) * 1000
        fetch_end_ms = (end_ms // 1000) * 1000

        if refresh:
            fetched = fetch_futures_1s_candles(
                symbol=symbol,
                start_ms=fetch_start_ms,
                end_ms=fetch_end_ms,
            )
            if not fetched:
                raise RuntimeError("No 1s trade data fetched from Binance Futures aggTrades.")
            cache_candles = fetched
            cache_changed = True
        else:
            if cache_candles:
                first_open_ms = int(cache_candles[0].open_time.timestamp() * 1000)
                last_open_ms = int(cache_candles[-1].open_time.timestamp() * 1000)

                if fetch_start_ms < first_open_ms:
                    older_end_ms = first_open_ms - 1000
                    if fetch_start_ms <= older_end_ms:
                        older = fetch_futures_1s_candles(
                            symbol=symbol,
                            start_ms=fetch_start_ms,
                            end_ms=older_end_ms,
                        )
                        if older:
                            cache_candles = _dedup_sorted(older + cache_candles)
                            cache_changed = True

                next_start_ms = last_open_ms + 1000
                if next_start_ms <= fetch_end_ms:
                    newer = fetch_futures_1s_candles(
                        symbol=symbol,
                        start_ms=next_start_ms,
                        end_ms=fetch_end_ms,
                        last_close_seed=cache_candles[-1].close,
                    )
                    if newer:
                        cache_candles = _dedup_sorted(cache_candles + newer)
                        cache_changed = True
            else:
                fetched = fetch_futures_1s_candles(
                    symbol=symbol,
                    start_ms=fetch_start_ms,
                    end_ms=fetch_end_ms,
                )
                if not fetched:
                    raise RuntimeError("No 1s trade data fetched from Binance Futures aggTrades.")
                cache_candles = fetched
                cache_changed = True

        if cache_changed:
            save_candles_to_csv(cache_path, cache_candles)

        if use_explicit_range:
            sliced = _slice_range(cache_candles, start_ms=req_start_ms, end_ms=end_ms)
        else:
            days = lookback_days if lookback_days is not None else 365
            sliced = _slice_lookback(cache_candles, now_dt=now, days=days)
        if not sliced:
            raise RuntimeError("No candle data available after loading 1s cache.")
        return sliced

    if not refresh:
        source_path: Path | None = None
        if cache_path.exists():
            source_path = cache_path
        elif legacy_path.exists():
            source_path = legacy_path
        if source_path and _cache_has_ohlc_columns(source_path):
            cache_candles = load_candles_from_csv(source_path)
            if source_path == legacy_path and cache_candles:
                cache_changed = True

    if refresh:
        fetched = fetch_futures_klines(
            symbol=symbol,
            interval=interval,
            start_ms=req_start_ms,
            end_ms=end_ms,
        )
        if not fetched:
            raise RuntimeError("No kline data fetched from Binance Futures.")
        cache_candles = fetched
        cache_changed = True
    else:
        if cache_candles:
            first_open_ms = int(cache_candles[0].open_time.timestamp() * 1000)
            last_open_ms = int(cache_candles[-1].open_time.timestamp() * 1000)

            if req_start_ms < first_open_ms:
                older = fetch_futures_klines(
                    symbol=symbol,
                    interval=interval,
                    start_ms=req_start_ms,
                    end_ms=first_open_ms - 1,
                )
                if older:
                    cache_candles = _dedup_sorted(older + cache_candles)
                    cache_changed = True

            next_start_ms = last_open_ms + interval_ms
            if next_start_ms < end_ms:
                newer = fetch_futures_klines(
                    symbol=symbol,
                    interval=interval,
                    start_ms=next_start_ms,
                    end_ms=end_ms,
                )
                if newer:
                    cache_candles = _dedup_sorted(cache_candles + newer)
                    cache_changed = True
        else:
            fetched = fetch_futures_klines(
                symbol=symbol,
                interval=interval,
                start_ms=req_start_ms,
                end_ms=end_ms,
            )
            if not fetched:
                raise RuntimeError("No kline data fetched from Binance Futures.")
            cache_candles = fetched
            cache_changed = True

    if cache_changed:
        save_candles_to_csv(cache_path, cache_candles)

    if use_explicit_range:
        sliced = _slice_range(cache_candles, start_ms=req_start_ms, end_ms=end_ms)
    else:
        days = lookback_days if lookback_days is not None else 365
        sliced = _slice_lookback(cache_candles, now_dt=now, days=days)
    if not sliced:
        raise RuntimeError("No candle data available after loading cache.")
    return sliced
