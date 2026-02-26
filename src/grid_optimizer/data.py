from __future__ import annotations

import csv
import json
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .types import Candle

FUTURES_KLINE_URL = "https://fapi.binance.com/fapi/v1/klines"
DEFAULT_TIMEOUT_SECONDS = 30


def parse_interval_ms(interval: str) -> int:
    match = re.fullmatch(r"(\d+)([mhdw])", interval.strip())
    if not match:
        raise ValueError(f"Unsupported interval: {interval}")
    value = int(match.group(1))
    unit = match.group(2)
    factor = {
        "m": 60_000,
        "h": 3_600_000,
        "d": 86_400_000,
        "w": 604_800_000,
    }[unit]
    return value * factor


def _http_get_json(url: str, params: dict[str, str | int]) -> list:
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
    if isinstance(data, dict):
        code = data.get("code", "unknown")
        msg = data.get("msg", "unknown error")
        raise RuntimeError(f"Binance API error {code}: {msg}")
    return data


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
    lookback_days: int,
    cache_dir: str | Path = "data",
    refresh: bool = False,
) -> list[Candle]:
    if lookback_days <= 0:
        raise ValueError("lookback_days must be > 0")

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

    cache_dir_path = Path(cache_dir)
    now = datetime.now(tz=timezone.utc)
    end_ms = int(now.timestamp() * 1000)
    req_start_ms = int((now - timedelta(days=lookback_days)).timestamp() * 1000)
    interval_ms = parse_interval_ms(interval)

    cache_path = _cache_file_path(cache_dir_path, symbol, interval)
    legacy_path = _legacy_cache_file_path(cache_dir_path, symbol, interval, lookback_days)

    cache_candles: list[Candle] = []
    cache_changed = False

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

    sliced = _slice_lookback(cache_candles, now_dt=now, days=lookback_days)
    if not sliced:
        raise RuntimeError("No candle data available after loading cache.")
    return sliced
