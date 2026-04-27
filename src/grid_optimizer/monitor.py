from __future__ import annotations

import json
import os
import re
import shlex
import subprocess
import threading
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .audit import (
    DEFAULT_AUDIT_LOOKBACK_DAYS,
    build_audit_paths,
    count_jsonl_lines,
    fetch_time_paged,
    income_row_key,
    income_row_time_ms,
    iter_jsonl,
    parse_iso_ts as _parse_iso_ts,
    read_json as _read_json,
    read_jsonl_filtered,
    trade_row_key,
    trade_row_time_ms,
)
from .data import (
    fetch_futures_account_info_v3,
    fetch_futures_book_tickers,
    fetch_futures_income_history,
    fetch_futures_klines,
    fetch_futures_open_orders,
    fetch_futures_premium_index,
    fetch_futures_position_mode,
    fetch_futures_user_trades,
    fetch_spot_klines,
    load_binance_api_credentials,
)
from .live_check import extract_symbol_position
from .competition_board import build_reward_volume_targets, resolve_active_competition_board


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"true", "1", "yes"}


RUNNER_PID_PATH = Path("output/night_loop_runner.pid")
RUNNER_CONTROL_PATH = Path("output/night_loop_runner_control.json")
RUNNER_LOG_PATH = Path("output/night_loop_runner.log")
STABLE_QUOTE_ASSETS = {"USDT", "USDC", "FDUSD", "BUSD"}
MONITOR_CACHE_TTL_SECONDS = 2.0
LOCAL_RUNTIME_SNAPSHOT_MAX_AGE_SECONDS = 180.0
_MONITOR_CACHE_LOCK = threading.Lock()
_MONITOR_CACHE: dict[tuple[str, str, str, str, int], tuple[float, dict[str, Any]]] = {}
_MONITOR_INFLIGHT: dict[tuple[str, str, str, str, int], "_InflightMonitorSnapshot"] = {}


class _InflightMonitorSnapshot:
    def __init__(self) -> None:
        self.event = threading.Event()
        self.snapshot: dict[str, Any] | None = None
        self.error: Exception | None = None


def _read_json_dict(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _runner_service_template() -> str:
    return str(os.environ.get("GRID_RUNNER_SERVICE_TEMPLATE", "")).strip()


def _symbol_runner_systemd_enabled() -> bool:
    return bool(_runner_service_template())


def _symbol_output_slug(symbol: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9]+", "_", str(symbol or "").strip()).strip("_").lower()
    return normalized or "symbol"


def runner_pid_path_for_symbol(symbol: str) -> Path:
    slug = _symbol_output_slug(symbol)
    return Path(f"output/{slug}_loop_runner.pid")


def runner_control_path_for_symbol(symbol: str) -> Path:
    slug = _symbol_output_slug(symbol)
    return Path(f"output/{slug}_loop_runner_control.json")


def runner_log_path_for_symbol(symbol: str) -> Path:
    slug = _symbol_output_slug(symbol)
    return Path(f"output/{slug}_loop_runner.log")


def _read_runner_process(
    pid_path: Path = RUNNER_PID_PATH,
    control_path: Path = RUNNER_CONTROL_PATH,
) -> dict[str, Any]:
    runner = {
        "configured": False,
        "pid": None,
        "is_running": False,
        "elapsed": None,
        "args": None,
        "config": {},
    }
    control_payload = _read_json_dict(control_path)
    if control_payload:
        runner["configured"] = True
        runner["config"] = control_payload
    if not pid_path.exists():
        return runner
    try:
        raw_pid = pid_path.read_text(encoding="utf-8").strip()
        pid = int(raw_pid)
    except (OSError, ValueError):
        try:
            pid_path.unlink(missing_ok=True)
        except OSError:
            pass
        return runner

    runner["configured"] = True
    runner["pid"] = pid
    try:
        proc = subprocess.run(
            ["ps", "-ww", "-p", str(pid), "-o", "pid=", "-o", "etime=", "-o", "args="],
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError:
        return runner

    line = proc.stdout.strip().splitlines()
    if proc.returncode != 0 or not line:
        try:
            pid_path.unlink(missing_ok=True)
        except OSError:
            pass
        return runner

    parts = line[-1].strip().split(None, 2)
    if len(parts) < 3:
        try:
            pid_path.unlink(missing_ok=True)
        except OSError:
            pass
        return runner

    _, elapsed, args_text = parts
    runner["is_running"] = True
    runner["elapsed"] = elapsed
    runner["args"] = args_text
    parsed_config = _parse_runner_args(args_text)
    if runner["config"]:
        merged_config = dict(runner["config"])
        merged_config.update(parsed_config)
        runner["config"] = merged_config
    else:
        runner["config"] = parsed_config
    runner["configured"] = True
    return runner


def _legacy_runner_symbol() -> str:
    if _symbol_runner_systemd_enabled():
        return ""
    control_payload = _read_json_dict(RUNNER_CONTROL_PATH) or {}
    symbol = str(control_payload.get("symbol", "")).upper().strip()
    if symbol:
        return symbol
    runner = _read_runner_process()
    return str((runner.get("config") or {}).get("symbol", "")).upper().strip()


def read_symbol_runner_process(symbol: str) -> dict[str, Any]:
    normalized_symbol = str(symbol or "").upper().strip()
    symbol_runner = _read_runner_process(
        pid_path=runner_pid_path_for_symbol(normalized_symbol),
        control_path=runner_control_path_for_symbol(normalized_symbol),
    )
    symbol_runner["scope"] = "symbol"
    symbol_runner["symbol"] = normalized_symbol

    symbol_config_symbol = str((symbol_runner.get("config") or {}).get("symbol", "")).upper().strip()
    if symbol_runner.get("configured") or symbol_runner.get("pid") is not None:
        if not symbol_config_symbol or symbol_config_symbol == normalized_symbol:
            return symbol_runner

    if _symbol_runner_systemd_enabled():
        return symbol_runner

    legacy_symbol = _legacy_runner_symbol()
    if normalized_symbol and legacy_symbol == normalized_symbol:
        legacy_runner = _read_runner_process()
        legacy_runner["scope"] = "legacy"
        legacy_runner["symbol"] = normalized_symbol
        return legacy_runner

    return symbol_runner


def _parse_runner_args(args_text: str) -> dict[str, Any]:
    try:
        argv = shlex.split(args_text)
    except ValueError:
        return {}

    config: dict[str, Any] = {}
    i = 0
    while i < len(argv):
        token = argv[i]
        next_token = argv[i + 1] if i + 1 < len(argv) else None
        key = token.lstrip("-").replace("-", "_")

        if token in {"--apply", "--cancel-stale", "--reset-state", "--fixed-center-enabled"}:
            config[key] = True
            i += 1
            continue
        if token.startswith("--no-"):
            config[token[5:].replace("-", "_")] = False
            i += 1
            continue
        if not token.startswith("--") or next_token is None or str(next_token).startswith("--"):
            i += 1
            continue

        if token in {
            "--center-price",
            "--step-price",
            "--per-order-notional",
            "--base-position-notional",
            "--pause-buy-position-notional",
            "--pause-short-position-notional",
            "--inventory-pause-long-probe-scale",
            "--inventory-pause-short-probe-scale",
            "--max-position-notional",
            "--max-short-position-notional",
            "--min-mid-price-for-buys",
            "--buy-pause-amp-trigger-ratio",
            "--buy-pause-down-return-trigger-ratio",
            "--freeze-shift-abs-return-trigger-ratio",
            "--inventory-tier-start-notional",
            "--inventory-tier-end-notional",
            "--inventory-tier-per-order-notional",
            "--inventory-tier-base-position-notional",
            "--max-total-notional",
            "--rolling-hourly-loss-limit",
            "--max-cumulative-notional",
            "--max-mid-drift-steps",
            "--sleep-seconds",
        }:
            config[key] = _safe_float(next_token)
            i += 2
            continue
        if token in {
            "--buy-levels",
            "--sell-levels",
            "--up-trigger-steps",
            "--down-trigger-steps",
            "--shift-steps",
            "--leverage",
            "--max-new-orders",
            "--inventory-tier-buy-levels",
            "--inventory-tier-sell-levels",
        }:
            try:
                config[key] = int(next_token)
            except ValueError:
                pass
            i += 2
            continue
        if token in {
            "--symbol",
            "--strategy-mode",
            "--margin-type",
            "--state-path",
            "--plan-json",
            "--submit-report-json",
            "--summary-jsonl",
            "--run-start-time",
            "--run-end-time",
        }:
            config[key] = next_token
            i += 2
            continue
        i += 1
    return config


def _build_session_window(
    events: list[dict[str, Any]],
    submit_report: dict[str, Any] | None,
) -> tuple[datetime | None, datetime | None]:
    first_ts: datetime | None = None
    last_ts: datetime | None = None
    for item in events:
        ts = _parse_iso_ts(item.get("ts"))
        if ts is None:
            continue
        if first_ts is None or ts < first_ts:
            first_ts = ts
        if last_ts is None or ts > last_ts:
            last_ts = ts
    if first_ts is None and submit_report:
        first_ts = _parse_iso_ts(str(submit_report.get("generated_at", "")))
        last_ts = first_ts
    return first_ts, last_ts


def _build_commission_converter(trades: list[dict[str, Any]]) -> Any:
    by_asset_times: dict[str, list[int]] = {}
    for item in trades:
        asset = str(item.get("commissionAsset", "")).upper().strip()
        ts_ms = trade_row_time_ms(item)
        if not asset or ts_ms <= 0 or asset in STABLE_QUOTE_ASSETS:
            continue
        by_asset_times.setdefault(asset, []).append(ts_ms)

    price_maps: dict[str, dict[int, float]] = {}
    fallback_prices: dict[str, float] = {}
    for asset, ts_list in by_asset_times.items():
        start_ms = min(ts_list)
        end_ms = max(ts_list) + 60_000
        symbol = f"{asset}USDT"
        candles = fetch_spot_klines(symbol=symbol, interval="1m", start_ms=start_ms, end_ms=end_ms, limit=1000)
        minute_map = {int(candle.open_time.timestamp() * 1000): float(candle.close) for candle in candles}
        if minute_map:
            price_maps[asset] = minute_map
            fallback_prices[asset] = float(candles[-1].close)

    def _converter(item: dict[str, Any]) -> float:
        raw_commission = abs(_safe_float(item.get("commission")))
        asset = str(item.get("commissionAsset", "")).upper().strip()
        if raw_commission <= 0 or not asset:
            return 0.0
        if asset in STABLE_QUOTE_ASSETS:
            return raw_commission
        minute_map = price_maps.get(asset)
        if minute_map:
            ts_ms = trade_row_time_ms(item)
            minute_key = (ts_ms // 60_000) * 60_000 if ts_ms > 0 else 0
            price = minute_map.get(minute_key)
            if price is None:
                price = fallback_prices.get(asset, 0.0)
            if price > 0:
                return raw_commission * price
        fallback = fallback_prices.get(asset, 0.0)
        return raw_commission * fallback if fallback > 0 else raw_commission

    return _converter


def _min_dt(*values: datetime | None) -> datetime | None:
    rows = [item for item in values if item is not None]
    return min(rows) if rows else None


def _max_dt(*values: datetime | None) -> datetime | None:
    rows = [item for item in values if item is not None]
    return max(rows) if rows else None


def _epoch_bounds(rows: list[dict[str, Any]], row_time_ms: Any) -> tuple[datetime | None, datetime | None]:
    first_ts: datetime | None = None
    last_ts: datetime | None = None
    for item in rows:
        ts_ms = int(row_time_ms(item) or 0)
        if ts_ms <= 0:
            continue
        ts = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
        if first_ts is None or ts < first_ts:
            first_ts = ts
        if last_ts is None or ts > last_ts:
            last_ts = ts
    return first_ts, last_ts


def _filter_rows_since(
    rows: list[dict[str, Any]],
    *,
    since: datetime | None,
    row_time_ms: Any,
) -> list[dict[str, Any]]:
    if since is None:
        return list(rows)
    floor_ms = int(since.astimezone(timezone.utc).timestamp() * 1000)
    return [item for item in rows if int(row_time_ms(item) or 0) >= floor_ms]


def _filter_events_since(events: list[dict[str, Any]], since: datetime | None) -> list[dict[str, Any]]:
    if since is None:
        return list(events)
    floor = since.astimezone(timezone.utc)
    filtered: list[dict[str, Any]] = []
    for item in events:
        ts = _parse_iso_ts(item.get("ts"))
        if ts is None or ts < floor:
            continue
        filtered.append(item)
    return filtered


def _read_event_window(
    path: Path,
    *,
    since: datetime | None,
    limit: int,
) -> tuple[list[dict[str, Any]], datetime | None, datetime | None]:
    floor = since.astimezone(timezone.utc) if since is not None else None
    rows: deque[dict[str, Any]] = deque(maxlen=limit if limit > 0 else None)
    first_ts: datetime | None = None
    last_ts: datetime | None = None
    for item in iter_jsonl(path):
        ts = _parse_iso_ts(item.get("ts"))
        if ts is None:
            continue
        if floor is not None and ts < floor:
            continue
        if first_ts is None:
            first_ts = ts
        last_ts = ts
        rows.append(item)
    return list(rows), first_ts, last_ts


def _load_or_fetch_trade_rows(
    *,
    audit_path: Path,
    symbol: str,
    api_key: str,
    api_secret: str,
    session_start: datetime | None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    effective_start = session_start or (datetime.now(timezone.utc) - timedelta(days=DEFAULT_AUDIT_LOOKBACK_DAYS))
    floor_ms = int(effective_start.timestamp() * 1000)
    audit_rows = read_jsonl_filtered(
        audit_path,
        limit=0,
        predicate=lambda item: int(trade_row_time_ms(item) or 0) >= floor_ms,
    )
    if audit_rows:
        return audit_rows, {
            "source": "audit",
            "path": str(audit_path),
            "row_count": len(audit_rows),
            "start_time": effective_start.isoformat(),
        }

    start_time_ms = int(effective_start.timestamp() * 1000)
    end_time_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    rows = fetch_time_paged(
        fetch_page=lambda **params: fetch_futures_user_trades(
            symbol=symbol,
            api_key=api_key,
            api_secret=api_secret,
            **params,
        ),
        start_time_ms=start_time_ms,
        end_time_ms=end_time_ms,
        limit=1000,
        row_time_ms=trade_row_time_ms,
        row_key=trade_row_key,
    )
    return rows, {
        "source": "api",
        "path": None,
        "row_count": len(rows),
        "start_time": effective_start.isoformat(),
    }


def _load_or_fetch_income_rows(
    *,
    audit_path: Path,
    symbol: str,
    api_key: str,
    api_secret: str,
    session_start: datetime | None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    effective_start = session_start or (datetime.now(timezone.utc) - timedelta(days=DEFAULT_AUDIT_LOOKBACK_DAYS))
    floor_ms = int(effective_start.timestamp() * 1000)
    audit_rows = read_jsonl_filtered(
        audit_path,
        limit=0,
        predicate=lambda item: int(income_row_time_ms(item) or 0) >= floor_ms,
    )
    if audit_rows:
        return audit_rows, {
            "source": "audit",
            "path": str(audit_path),
            "row_count": len(audit_rows),
            "start_time": effective_start.isoformat(),
        }

    start_time_ms = int(effective_start.timestamp() * 1000)
    end_time_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    rows = fetch_time_paged(
        fetch_page=lambda **params: fetch_futures_income_history(
            symbol=symbol,
            income_type="FUNDING_FEE",
            api_key=api_key,
            api_secret=api_secret,
            **params,
        ),
        start_time_ms=start_time_ms,
        end_time_ms=end_time_ms,
        limit=1000,
        row_time_ms=income_row_time_ms,
        row_key=income_row_key,
    )
    return rows, {
        "source": "api",
        "path": None,
        "row_count": len(rows),
        "start_time": effective_start.isoformat(),
    }


def summarize_user_trades(trades: list[dict[str, Any]], commission_converter: Any | None = None) -> dict[str, Any]:
    sorted_trades = sorted(trades, key=lambda item: (int(item.get("time", 0) or 0), int(item.get("id", 0) or 0)))
    gross_notional = 0.0
    buy_notional = 0.0
    sell_notional = 0.0
    realized_pnl = 0.0
    commission = 0.0
    commission_raw_by_asset: dict[str, float] = {}
    buy_count = 0
    sell_count = 0
    maker_count = 0
    series: list[dict[str, Any]] = []
    cumulative_notional = 0.0
    cumulative_realized = 0.0
    cumulative_net = 0.0

    for item in sorted_trades:
        price = _safe_float(item.get("price"))
        qty = _safe_float(item.get("qty"))
        trade_notional = price * qty
        side = str(item.get("side", "")).upper().strip()
        trade_realized = _safe_float(item.get("realizedPnl"))
        trade_commission_raw = abs(_safe_float(item.get("commission")))
        trade_commission = (
            float(commission_converter(item))
            if commission_converter is not None
            else trade_commission_raw
        )
        commission_asset = str(item.get("commissionAsset", "")).upper().strip()
        gross_notional += trade_notional
        realized_pnl += trade_realized
        commission += trade_commission
        cumulative_notional += trade_notional
        cumulative_realized += trade_realized
        cumulative_net += trade_realized - trade_commission
        if commission_asset:
            commission_raw_by_asset[commission_asset] = commission_raw_by_asset.get(commission_asset, 0.0) + trade_commission_raw
        if side == "BUY":
            buy_count += 1
            buy_notional += trade_notional
        elif side == "SELL":
            sell_count += 1
            sell_notional += trade_notional
        if _truthy(item.get("maker")):
            maker_count += 1
        ts_ms = int(item.get("time", 0) or 0)
        series.append(
            {
                "ts": datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).isoformat() if ts_ms else None,
                "cumulative_notional": cumulative_notional,
                "cumulative_realized_pnl": cumulative_realized,
                "cumulative_net_realized": cumulative_net,
            }
        )

    return {
        "trade_count": len(sorted_trades),
        "buy_count": buy_count,
        "sell_count": sell_count,
        "maker_count": maker_count,
        "gross_notional": gross_notional,
        "buy_notional": buy_notional,
        "sell_notional": sell_notional,
        "realized_pnl": realized_pnl,
        "commission": commission,
        "commission_raw_by_asset": commission_raw_by_asset,
        "series": series[-240:],
        "recent_trades": [
            {
                **item,
                "commission_usdt": (
                    float(commission_converter(item))
                    if commission_converter is not None
                    else abs(_safe_float(item.get("commission")))
                ),
            }
            for item in sorted_trades[-50:]
        ],
    }


def summarize_income(rows: list[dict[str, Any]]) -> dict[str, Any]:
    funding_fee = 0.0
    records: list[dict[str, Any]] = []
    for item in sorted(rows, key=lambda row: int(row.get("time", 0) or 0)):
        income = _safe_float(item.get("income"))
        funding_fee += income
        records.append(
            {
                "ts": datetime.fromtimestamp(int(item.get("time", 0) or 0) / 1000, tz=timezone.utc).isoformat()
                if str(item.get("time", "")).strip()
                else None,
                "income": income,
                "asset": str(item.get("asset", "")).upper().strip(),
                "income_type": str(item.get("incomeType", "")).upper().strip(),
                "info": str(item.get("info", "")),
            }
        )
    return {
        "funding_fee": funding_fee,
        "recent_income": records[-50:],
    }


def _floor_hour(dt: datetime) -> datetime:
    normalized = dt.astimezone(timezone.utc)
    return normalized.replace(minute=0, second=0, microsecond=0)


def summarize_hourly_metrics(
    trades: list[dict[str, Any]],
    income_rows: list[dict[str, Any]],
    candles: list[Any],
    *,
    commission_converter: Any | None = None,
    limit: int = 24,
) -> dict[str, Any]:
    buckets: dict[datetime, dict[str, Any]] = {}

    def ensure_bucket(hour_start: datetime) -> dict[str, Any]:
        bucket = buckets.get(hour_start)
        if bucket is None:
            bucket = {
                "hour_start": hour_start.isoformat(),
                "gross_notional": 0.0,
                "buy_notional": 0.0,
                "sell_notional": 0.0,
                "trade_count": 0,
                "buy_count": 0,
                "sell_count": 0,
                "realized_pnl": 0.0,
                "commission": 0.0,
                "funding_fee": 0.0,
                "net_after_fees_and_funding": 0.0,
                "loss_per_10k": 0.0,
                "open_price": 0.0,
                "close_price": 0.0,
                "high_price": 0.0,
                "low_price": 0.0,
                "return_ratio": 0.0,
                "amplitude_ratio": 0.0,
            }
            buckets[hour_start] = bucket
        return bucket

    for candle in sorted(candles, key=lambda item: item.open_time):
        hour_start = _floor_hour(candle.open_time)
        bucket = ensure_bucket(hour_start)
        bucket["open_price"] = float(candle.open)
        bucket["close_price"] = float(candle.close)
        bucket["high_price"] = float(candle.high)
        bucket["low_price"] = float(candle.low)
        bucket["return_ratio"] = (float(candle.close) / float(candle.open) - 1.0) if float(candle.open) > 0 else 0.0
        bucket["amplitude_ratio"] = (float(candle.high) / float(candle.low) - 1.0) if float(candle.low) > 0 else 0.0

    for item in trades:
        ts_ms = int(item.get("time", 0) or 0)
        if ts_ms <= 0:
            continue
        hour_start = _floor_hour(datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc))
        bucket = ensure_bucket(hour_start)
        price = _safe_float(item.get("price"))
        qty = _safe_float(item.get("qty"))
        trade_notional = price * qty
        side = str(item.get("side", "")).upper().strip()
        trade_realized = _safe_float(item.get("realizedPnl"))
        trade_commission_raw = abs(_safe_float(item.get("commission")))
        trade_commission = (
            float(commission_converter(item))
            if commission_converter is not None
            else trade_commission_raw
        )

        bucket["gross_notional"] += trade_notional
        bucket["trade_count"] += 1
        bucket["realized_pnl"] += trade_realized
        bucket["commission"] += trade_commission
        if side == "BUY":
            bucket["buy_notional"] += trade_notional
            bucket["buy_count"] += 1
        elif side == "SELL":
            bucket["sell_notional"] += trade_notional
            bucket["sell_count"] += 1

    for item in income_rows:
        ts_ms = int(item.get("time", 0) or 0)
        if ts_ms <= 0:
            continue
        hour_start = _floor_hour(datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc))
        bucket = ensure_bucket(hour_start)
        income_type = str(item.get("incomeType", item.get("income_type", ""))).upper().strip()
        income = _safe_float(item.get("income"))
        if income_type == "FUNDING_FEE":
            bucket["funding_fee"] += income

    rows = []
    for hour_start in sorted(buckets.keys(), reverse=True):
        bucket = dict(buckets[hour_start])
        bucket["net_after_fees_and_funding"] = (
            float(bucket["realized_pnl"]) + float(bucket["funding_fee"]) - float(bucket["commission"])
        )
        gross_notional = float(bucket["gross_notional"])
        bucket["loss_per_10k"] = (
            bucket["net_after_fees_and_funding"] / gross_notional * 10000.0
            if gross_notional > 0
            else 0.0
        )
        rows.append(bucket)

    limited_rows = rows[: max(int(limit), 1)]
    return {
        "rows": limited_rows,
        "row_count": len(limited_rows),
        "available_hours": len(rows),
    }


def summarize_loop_events(events: list[dict[str, Any]]) -> dict[str, Any]:
    cycle_rows = [item for item in events if "cycle" in item]
    error_rows = [item for item in cycle_rows if item.get("error_message")]
    success_rows = [item for item in cycle_rows if not item.get("error_message")]
    latest = cycle_rows[-1] if cycle_rows else None

    interval_seconds: list[float] = []
    previous_ts: datetime | None = None
    for item in success_rows[-20:]:
        ts = _parse_iso_ts(item.get("ts"))
        if ts is None:
            continue
        if previous_ts is not None:
            interval_seconds.append((ts - previous_ts).total_seconds())
        previous_ts = ts
    avg_interval = sum(interval_seconds) / len(interval_seconds) if interval_seconds else None
    now = datetime.now(timezone.utc)
    last_ts = _parse_iso_ts(latest.get("ts")) if latest else None
    last_age = (now - last_ts).total_seconds() if last_ts else None
    heartbeat = max(90.0, (avg_interval or 20.0) * 3.0)
    is_alive = last_age is not None and last_age <= heartbeat

    return {
        "cycle_count": len(cycle_rows),
        "success_count": len(success_rows),
        "error_count": len(error_rows),
        "latest": latest,
        "recent": cycle_rows[-30:],
        "avg_interval_seconds": avg_interval,
        "last_age_seconds": last_age,
        "is_alive": bool(is_alive),
    }


def _latest_recent_error(loop_summary: dict[str, Any]) -> dict[str, Any] | None:
    rows = loop_summary.get("recent") or []
    for item in reversed(rows):
        if str(item.get("error_message", "")).strip():
            return item
    return None


def _push_alert(
    alerts: list[dict[str, Any]],
    seen_codes: set[str],
    *,
    code: str,
    severity: str,
    title: str,
    detail: str,
    action: str | None = None,
    ts: str | None = None,
) -> None:
    if code in seen_codes:
        return
    seen_codes.add(code)
    alerts.append(
        {
            "code": code,
            "severity": severity,
            "title": title,
            "detail": detail,
            "action": action,
            "ts": ts,
        }
    )


def _extract_futures_asset_snapshot(account_info: dict[str, Any], asset: str) -> dict[str, Any] | None:
    normalized_asset = str(asset or "").upper().strip()
    if not normalized_asset:
        return None
    assets = account_info.get("assets")
    asset_row: dict[str, Any] | None = None
    if isinstance(assets, list):
        for item in assets:
            if not isinstance(item, dict):
                continue
            if str(item.get("asset", "")).upper().strip() == normalized_asset:
                asset_row = item
                break
    if asset_row is None and normalized_asset != "USDT":
        return None

    wallet_balance = _safe_float((asset_row or {}).get("walletBalance"))
    available_balance = _safe_float((asset_row or {}).get("availableBalance"))
    cross_un_pnl = _safe_float((asset_row or {}).get("crossUnPnl"))
    max_withdraw_amount = _safe_float((asset_row or {}).get("maxWithdrawAmount"))
    margin_balance = _safe_float((asset_row or {}).get("marginBalance"))
    if normalized_asset == "USDT":
        wallet_balance = wallet_balance or _safe_float(account_info.get("totalWalletBalance"))
        available_balance = available_balance or _safe_float(account_info.get("availableBalance"))
        margin_balance = margin_balance or _safe_float(account_info.get("totalMarginBalance"))
        max_withdraw_amount = max_withdraw_amount or _safe_float(account_info.get("maxWithdrawAmount"))
    return {
        "asset": normalized_asset,
        "wallet_balance": wallet_balance,
        "available_balance": available_balance,
        "margin_balance": margin_balance,
        "unrealized_pnl": cross_un_pnl,
        "max_withdraw_amount": max_withdraw_amount,
    }


def build_monitor_alerts(
    *,
    loop_summary: dict[str, Any],
    warnings: list[str],
    risk_controls: dict[str, Any],
    runner: dict[str, Any],
) -> list[dict[str, Any]]:
    alerts: list[dict[str, Any]] = []
    seen_codes: set[str] = set()

    latest_error = _latest_recent_error(loop_summary)
    latest_error_message = str((latest_error or {}).get("error_message", "")).strip()
    latest_error_ts = str((latest_error or {}).get("ts", "")).strip() or None

    if runner.get("configured") and not runner.get("is_running"):
        _push_alert(
            alerts,
            seen_codes,
            code="runner_not_running",
            severity="critical",
            title="策略进程已退出",
            detail="监控检测到本地控制文件存在，但 loop_runner 进程当前不在运行。",
            action="先看最近循环日志和错误事件，再决定是否直接重启策略。",
        )

    if latest_error_message:
        if "-2019" in latest_error_message or "Margin is insufficient" in latest_error_message:
            _push_alert(
                alerts,
                seen_codes,
                code="margin_insufficient",
                severity="critical",
                title="保证金不足，交易所已拒单",
                detail=latest_error_message,
                action="降低 base_position_notional、per_order_notional、挂单层数和仓位上限，或释放其他币种占用的保证金。",
                ts=latest_error_ts,
            )
        elif "-1003" in latest_error_message or "Too many requests" in latest_error_message:
            _push_alert(
                alerts,
                seen_codes,
                code="rate_limited",
                severity="critical",
                title="Binance API 频率超限",
                detail=latest_error_message,
                action="降低监控页自动刷新频率，减少同时运行币种，或继续压低 runner 的 REST 轮询频率。",
                ts=latest_error_ts,
            )
        elif "-5022" in latest_error_message or "post only rejected" in latest_error_message.lower():
            _push_alert(
                alerts,
                seen_codes,
                code="post_only_rejected",
                severity="warning",
                title="Post only 挂单被拒",
                detail=latest_error_message,
                action="说明挂单价格过于贴近会立即成交。应适当放大 step_price，或降低 maker_retries，避免反复无效重挂。",
                ts=latest_error_ts,
            )
        elif "validation failed" in latest_error_message.lower():
            _push_alert(
                alerts,
                seen_codes,
                code="validation_failed",
                severity="critical",
                title="下单前校验失败",
                detail=latest_error_message,
                action="优先检查账户持仓模式、策略模式和提交参数是否匹配，再决定是否重启。",
                ts=latest_error_ts,
            )

    for warning in warnings:
        if warning == "runner_pid_present_but_process_not_running":
            _push_alert(
                alerts,
                seen_codes,
                code="stale_runner_pid",
                severity="warning",
                title="状态文件里有旧 PID",
                detail="监控发现控制文件记录了 runner PID，但对应进程已经不存在。",
                action="这通常说明策略异常退出过；先看最近一轮错误，再决定是否清理状态并重启。",
            )
        elif warning.startswith("account_read_failed:"):
            _push_alert(
                alerts,
                seen_codes,
                code="account_read_failed",
                severity="warning",
                title="账户读取失败",
                detail=warning.removeprefix("account_read_failed:").strip(),
                action="账户读不到时，页面里的持仓、挂单和损益会不完整，应先恢复 API 可用性。",
            )
        elif warning.startswith("market_read_failed:"):
            _push_alert(
                alerts,
                seen_codes,
                code="market_read_failed",
                severity="warning",
                title="行情读取失败",
                detail=warning.removeprefix("market_read_failed:").strip(),
                action="行情读不到时，中心价和挂单判断都会失真。先确认网络和 Binance 行情接口可用。",
            )
        elif warning == "missing_binance_api_credentials":
            _push_alert(
                alerts,
                seen_codes,
                code="missing_credentials",
                severity="critical",
                title="缺少 Binance API 凭证",
                detail="当前 Web 进程没有读到 BINANCE_API_KEY / BINANCE_API_SECRET。",
                action="先补齐环境变量，否则页面只能看到本地文件，不能拿到账户真实状态。",
            )

    pause_reasons = [str(item).strip() for item in (risk_controls.get("pause_reasons") or []) if str(item).strip()]
    if risk_controls.get("buy_paused") and pause_reasons:
        _push_alert(
            alerts,
            seen_codes,
            code="buy_paused",
            severity="warning",
            title="当前已暂停继续买入",
            detail="；".join(pause_reasons[:2]),
            action="这通常表示多仓已经偏重，或触发了价格/振幅保护。若想恢复成交密度，需要先降低库存或上调阈值。",
        )

    short_pause_reasons = [str(item).strip() for item in (risk_controls.get("short_pause_reasons") or []) if str(item).strip()]
    if risk_controls.get("short_paused") and short_pause_reasons:
        _push_alert(
            alerts,
            seen_codes,
            code="short_paused",
            severity="warning",
            title="当前已暂停继续开空",
            detail="；".join(short_pause_reasons[:2]),
            action="这通常表示空仓已经偏重。若想继续放量，需要先回补一部分空仓或上调空仓阈值。",
        )

    if risk_controls.get("buy_cap_applied"):
        _push_alert(
            alerts,
            seen_codes,
            code="buy_cap_applied",
            severity="info",
            title="买单已触发硬上限裁剪",
            detail="本轮计划中的买单总量超过了 max_position_notional，系统已经自动裁掉超出的部分。",
            action="如果成交量不够，应该先确认保证金充足，再考虑适度提高 max_position_notional。",
        )

    if risk_controls.get("short_cap_applied"):
        _push_alert(
            alerts,
            seen_codes,
            code="short_cap_applied",
            severity="info",
            title="卖空单已触发硬上限裁剪",
            detail="本轮计划中的卖空总量超过了 max_short_position_notional，系统已经自动裁掉超出的部分。",
            action="如果想继续提升做空成交量，需要先确认保证金余量，再考虑提高空仓上限。",
        )

    if risk_controls.get("volatility_buy_pause"):
        _push_alert(
            alerts,
            seen_codes,
            code="volatility_buy_pause",
            severity="info",
            title="分钟级风控已暂停做多开仓",
            detail="最近 1 分钟的振幅和跌幅触发了 buy_pause_amp_trigger_ratio / buy_pause_down_return_trigger_ratio。",
            action="这是策略自带的短时熔断，不是异常。恢复后会自动重新挂单。",
        )

    if risk_controls.get("shift_frozen"):
        _push_alert(
            alerts,
            seen_codes,
            code="shift_frozen",
            severity="info",
            title="中心迁移已临时冻结",
            detail="最近 1 分钟波动超过 freeze_shift_abs_return_trigger_ratio，本轮不会继续追价移动中心。",
            action="这是为了防止急拉急杀里频繁追价，不代表策略停止运行。",
        )

    return alerts


def _fresh_local_runtime_snapshot_available(
    *,
    runner: dict[str, Any],
    loop_summary: dict[str, Any],
    plan_report: dict[str, Any] | None,
    submit_report: dict[str, Any] | None,
) -> bool:
    latest_runtime_ts = _max_dt(
        _parse_iso_ts(((loop_summary.get("latest") or {}) if isinstance(loop_summary, dict) else {}).get("ts")),
        _parse_iso_ts(str((plan_report or {}).get("generated_at", ""))),
        _parse_iso_ts(str((submit_report or {}).get("generated_at", ""))),
    )
    if latest_runtime_ts is None:
        return False
    if not (bool(runner.get("is_running")) or bool((loop_summary or {}).get("is_alive"))):
        return False
    avg_interval = _safe_float((loop_summary or {}).get("avg_interval_seconds"))
    heartbeat = max(90.0, (avg_interval or 20.0) * 3.0)
    max_age_seconds = max(LOCAL_RUNTIME_SNAPSHOT_MAX_AGE_SECONDS, heartbeat)
    age_seconds = (datetime.now(timezone.utc) - latest_runtime_ts).total_seconds()
    return age_seconds <= max_age_seconds


def _normalize_runtime_open_order(payload: dict[str, Any]) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    response = payload.get("response") if isinstance(payload.get("response"), dict) else None
    request = payload.get("request") if isinstance(payload.get("request"), dict) else None
    order = response if response is not None else payload

    side = str(order.get("side") or (request or {}).get("side", "")).upper().strip()
    price = _safe_float(order.get("price"))
    if price <= 0:
        price = _safe_float((request or {}).get("submitted_price"))
    if price <= 0:
        price = _safe_float((request or {}).get("desired_price"))
    orig_qty = _safe_float(order.get("origQty", order.get("qty", order.get("quantity"))))
    if orig_qty <= 0:
        orig_qty = _safe_float((request or {}).get("qty"))
    if side not in {"BUY", "SELL"} or price <= 0 or orig_qty <= 0:
        return None

    reduce_only = order.get("reduceOnly")
    if reduce_only is None:
        reduce_only = (request or {}).get("reduce_only")
    position_side = str(
        order.get("positionSide")
        or (request or {}).get("position_side")
        or payload.get("position_side")
        or payload.get("positionSide")
        or "BOTH"
    ).upper().strip() or "BOTH"

    return {
        "order_id": order.get("orderId"),
        "client_order_id": order.get("clientOrderId") or (request or {}).get("client_order_id"),
        "side": side,
        "price": price,
        "orig_qty": orig_qty,
        "executed_qty": _safe_float(order.get("executedQty")),
        "reduce_only": _truthy(reduce_only),
        "position_side": position_side,
        "time": int(order.get("time") or order.get("updateTime") or 0),
    }


def _build_local_runtime_snapshot(
    *,
    runner: dict[str, Any],
    loop_summary: dict[str, Any],
    plan_report: dict[str, Any] | None,
    submit_report: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if not _fresh_local_runtime_snapshot_available(
        runner=runner,
        loop_summary=loop_summary,
        plan_report=plan_report,
        submit_report=submit_report,
    ):
        return None

    plan_report = plan_report if isinstance(plan_report, dict) else {}
    submit_report = submit_report if isinstance(submit_report, dict) else {}
    runner_config = runner.get("config") if isinstance(runner.get("config"), dict) else {}
    live_book = submit_report.get("live_book") if isinstance(submit_report.get("live_book"), dict) else {}

    bid_price = _safe_float(plan_report.get("bid_price"))
    if bid_price <= 0:
        bid_price = _safe_float(live_book.get("bid_price"))
    ask_price = _safe_float(plan_report.get("ask_price"))
    if ask_price <= 0:
        ask_price = _safe_float(live_book.get("ask_price"))
    mid_price = _safe_float(plan_report.get("mid_price"))
    if mid_price <= 0 and bid_price > 0 and ask_price > 0:
        mid_price = (bid_price + ask_price) / 2.0
    mark_price = _safe_float(plan_report.get("mark_price"))
    if mark_price <= 0:
        mark_price = mid_price

    dual_side_position = _truthy(plan_report.get("dual_side_position"))
    actual_net_qty = _safe_float(plan_report.get("actual_net_qty"))
    long_qty = max(_safe_float(plan_report.get("current_long_qty")), 0.0)
    short_qty = max(_safe_float(plan_report.get("current_short_qty")), 0.0)
    if not dual_side_position:
        long_qty = max(actual_net_qty, 0.0)
        short_qty = max(-actual_net_qty, 0.0)
    long_avg_price = _safe_float(plan_report.get("current_long_avg_price"))
    short_avg_price = _safe_float(plan_report.get("current_short_avg_price"))
    if dual_side_position:
        entry_price = long_avg_price if long_qty >= short_qty else short_avg_price
    elif actual_net_qty < -1e-12:
        entry_price = short_avg_price
    else:
        entry_price = long_avg_price
    break_even_price = entry_price

    open_orders: list[dict[str, Any]] = []
    seen_keys: set[tuple[Any, ...]] = set()
    for item in list(plan_report.get("kept_orders") or []) + list(submit_report.get("placed_orders") or []):
        normalized = _normalize_runtime_open_order(item)
        if normalized is None:
            continue
        dedupe_key = (
            normalized.get("order_id"),
            normalized.get("client_order_id"),
            normalized.get("side"),
            normalized.get("price"),
            normalized.get("orig_qty"),
            normalized.get("position_side"),
        )
        if dedupe_key in seen_keys:
            continue
        seen_keys.add(dedupe_key)
        open_orders.append(normalized)

    leverage_raw = runner_config.get("leverage")
    try:
        leverage = int(float(leverage_raw)) if str(leverage_raw).strip() else None
    except (TypeError, ValueError):
        leverage = None
    account_mode = submit_report.get("account_mode") if isinstance(submit_report.get("account_mode"), dict) else {}

    return {
        "market": {
            "bid_price": bid_price,
            "ask_price": ask_price,
            "mid_price": mid_price,
            "funding_rate": _safe_float(plan_report.get("funding_rate")),
            "mark_price": mark_price,
        },
        "position": {
            "position_amt": actual_net_qty,
            "long_qty": long_qty,
            "short_qty": short_qty,
            "entry_price": entry_price,
            "break_even_price": break_even_price,
            "unrealized_pnl": _safe_float(plan_report.get("unrealized_pnl")),
            "isolated": str(runner_config.get("margin_type", "")).upper().strip() == "ISOLATED",
            "leverage": leverage,
            "one_way_mode": not dual_side_position,
            "available_balance": _safe_float(plan_report.get("available_balance")),
            "wallet_balance": _safe_float(plan_report.get("wallet_balance")),
            "multi_assets_margin": _truthy(account_mode.get("multi_assets_margin")),
        },
        "open_orders": open_orders,
    }


def build_monitor_snapshot(
    *,
    symbol: str,
    events_path: str | Path = "output/night_loop_events.jsonl",
    plan_path: str | Path = "output/night_loop_latest_plan.json",
    submit_report_path: str | Path = "output/night_loop_latest_submit.json",
    summary_limit: int = 500,
    runner_process: dict[str, Any] | None = None,
) -> dict[str, Any]:
    normalized_symbol = str(symbol).upper().strip()
    cache_key = (
        normalized_symbol,
        str(Path(events_path)),
        str(Path(plan_path)),
        str(Path(submit_report_path)),
        int(summary_limit),
    )
    now = time.time()
    with _MONITOR_CACHE_LOCK:
        cached = _MONITOR_CACHE.get(cache_key)
        if cached is not None and (now - cached[0]) <= MONITOR_CACHE_TTL_SECONDS:
            return cached[1]
        inflight = _MONITOR_INFLIGHT.get(cache_key)
        if inflight is None:
            inflight = _InflightMonitorSnapshot()
            _MONITOR_INFLIGHT[cache_key] = inflight
            owner = True
        else:
            owner = False

    if not owner:
        inflight.event.wait()
        if inflight.snapshot is not None:
            return inflight.snapshot
        if inflight.error is not None:
            raise RuntimeError("monitor snapshot build failed") from inflight.error
        raise RuntimeError("monitor snapshot build failed without result")

    try:
        snapshot = _build_monitor_snapshot_uncached(
            symbol=normalized_symbol,
            events_path=events_path,
            plan_path=plan_path,
            submit_report_path=submit_report_path,
            summary_limit=summary_limit,
            runner_process=runner_process,
        )
    except Exception as exc:
        with _MONITOR_CACHE_LOCK:
            inflight.error = exc
            _MONITOR_INFLIGHT.pop(cache_key, None)
        inflight.event.set()
        raise

    with _MONITOR_CACHE_LOCK:
        _MONITOR_CACHE[cache_key] = (time.time(), snapshot)
        inflight.snapshot = snapshot
        _MONITOR_INFLIGHT.pop(cache_key, None)
    inflight.event.set()
    return snapshot


def _build_monitor_snapshot_uncached(
    *,
    symbol: str,
    events_path: str | Path = "output/night_loop_events.jsonl",
    plan_path: str | Path = "output/night_loop_latest_plan.json",
    submit_report_path: str | Path = "output/night_loop_latest_submit.json",
    summary_limit: int = 500,
    runner_process: dict[str, Any] | None = None,
) -> dict[str, Any]:
    normalized_symbol = str(symbol).upper().strip()
    event_path = Path(events_path)
    audit_paths = build_audit_paths(event_path)
    competition_board = resolve_active_competition_board(normalized_symbol, "futures")
    competition_start = _parse_iso_ts((competition_board or {}).get("activity_start_at"))
    competition_end = _parse_iso_ts((competition_board or {}).get("activity_end_at"))
    events, event_start, event_last = _read_event_window(
        event_path,
        since=competition_start,
        limit=summary_limit,
    )
    plan_report = _read_json(Path(plan_path))
    submit_report = _read_json(Path(submit_report_path))
    session_start, session_last = _build_session_window(events, submit_report)
    session_start = _min_dt(session_start, event_start)
    stats_start = competition_start or session_start
    session_last = _max_dt(session_last, event_last)
    loop_summary = summarize_loop_events(events)
    runner = runner_process or read_symbol_runner_process(normalized_symbol)

    snapshot: dict[str, Any] = {
        "ok": True,
        "symbol": normalized_symbol,
        "ts": datetime.now(timezone.utc).isoformat(),
        "session": {
            "start": stats_start.isoformat() if stats_start else None,
            "last_event": session_last.isoformat() if session_last else None,
        },
        "competition_window": {
            "label": str((competition_board or {}).get("label", "")).strip() or None,
            "activity_start_at": competition_start.isoformat() if competition_start else None,
            "activity_end_at": competition_end.isoformat() if competition_end else None,
            "stats_start_at": stats_start.isoformat() if stats_start else None,
        },
        "local": {
            "loop_summary": loop_summary,
            "plan_report": plan_report,
            "submit_report": submit_report,
        },
        "competition_reward_targets": build_reward_volume_targets(competition_board),
        "runner": runner,
        "audit": {
            "paths": {key: str(path) for key, path in audit_paths.items()},
            "trade_source": None,
            "income_source": None,
            "trade_row_count": 0,
            "income_row_count": 0,
            "order_event_count": count_jsonl_lines(audit_paths["order_audit"]),
            "submit_event_count": count_jsonl_lines(audit_paths["submit_audit"]),
            "plan_event_count": count_jsonl_lines(audit_paths["plan_audit"]),
        },
        "account_connected": False,
        "market": None,
        "position": None,
        "account_assets": {},
        "open_orders": [],
        "trade_summary": None,
        "income_summary": None,
        "hourly_summary": None,
        "risk_controls": None,
        "warnings": [],
        "alerts": [],
    }
    if runner["configured"] and not runner["is_running"]:
        snapshot["warnings"].append("runner_pid_present_but_process_not_running")

    local_runtime_snapshot = _build_local_runtime_snapshot(
        runner=runner,
        loop_summary=loop_summary,
        plan_report=plan_report if isinstance(plan_report, dict) else {},
        submit_report=submit_report if isinstance(submit_report, dict) else {},
    )
    if local_runtime_snapshot is not None:
        snapshot["market"] = local_runtime_snapshot["market"]
        snapshot["position"] = local_runtime_snapshot["position"]
        snapshot["open_orders"] = local_runtime_snapshot["open_orders"]
    else:
        try:
            book_rows = fetch_futures_book_tickers(symbol=normalized_symbol)
            premium_rows = fetch_futures_premium_index(symbol=normalized_symbol)
            book = book_rows[0] if book_rows else {}
            premium = premium_rows[0] if premium_rows else {}
            bid = _safe_float(book.get("bid_price"))
            ask = _safe_float(book.get("ask_price"))
            snapshot["market"] = {
                "bid_price": bid,
                "ask_price": ask,
                "mid_price": (bid + ask) / 2.0 if bid > 0 and ask > 0 else 0.0,
                "funding_rate": _safe_float(premium.get("funding_rate")),
                "mark_price": _safe_float(premium.get("mark_price")),
            }
        except Exception as exc:
            snapshot["warnings"].append(f"market_read_failed: {type(exc).__name__}: {exc}")

    credentials = load_binance_api_credentials()
    if credentials is None:
        snapshot["warnings"].append("missing_binance_api_credentials")
    else:
        api_key, api_secret = credentials
        snapshot["account_connected"] = True
        try:
            unrealized_pnl = _safe_float((snapshot.get("position") or {}).get("unrealized_pnl"))
            if local_runtime_snapshot is None:
                account_info = fetch_futures_account_info_v3(api_key, api_secret)
                position_mode = fetch_futures_position_mode(api_key, api_secret)
                open_orders = fetch_futures_open_orders(normalized_symbol, api_key, api_secret)
                dual_side_position = _truthy(position_mode.get("dualSidePosition"))
                if dual_side_position:
                    long_position = extract_symbol_position(account_info, normalized_symbol, "LONG")
                    short_position = extract_symbol_position(account_info, normalized_symbol, "SHORT")
                    long_qty = abs(_safe_float(long_position.get("positionAmt")))
                    short_qty = abs(_safe_float(short_position.get("positionAmt")))
                    long_unrealized = _safe_float(long_position.get("unRealizedProfit", long_position.get("unrealizedProfit")))
                    short_unrealized = _safe_float(short_position.get("unRealizedProfit", short_position.get("unrealizedProfit")))
                    unrealized_pnl = long_unrealized + short_unrealized
                    position_amt = long_qty - short_qty
                    position = long_position if long_qty >= short_qty else short_position
                    entry_price = _safe_float(position.get("entryPrice"))
                    break_even_price = _safe_float(position.get("breakEvenPrice"))
                else:
                    position = extract_symbol_position(account_info, normalized_symbol)
                    unrealized_pnl = _safe_float(position.get("unRealizedProfit", position.get("unrealizedProfit")))
                    entry_price = _safe_float(position.get("entryPrice"))
                    break_even_price = _safe_float(position.get("breakEvenPrice"))
                    position_amt = _safe_float(position.get("positionAmt"))
                snapshot["position"] = {
                    "position_amt": position_amt,
                    "long_qty": long_qty if dual_side_position else max(position_amt, 0.0),
                    "short_qty": short_qty if dual_side_position else 0.0,
                    "entry_price": entry_price,
                    "break_even_price": break_even_price,
                    "unrealized_pnl": unrealized_pnl,
                    "isolated": _truthy(position.get("isolated")),
                    "leverage": int(float(position.get("leverage", 0) or 0)) if str(position.get("leverage", "")).strip() else None,
                    "one_way_mode": not dual_side_position,
                    "available_balance": _safe_float(account_info.get("availableBalance")),
                    "wallet_balance": _safe_float(account_info.get("totalWalletBalance")),
                    "multi_assets_margin": _truthy(account_info.get("multiAssetsMargin")),
                }
                snapshot["account_assets"] = {
                    "USDT": _extract_futures_asset_snapshot(account_info, "USDT"),
                    "BNB": _extract_futures_asset_snapshot(account_info, "BNB"),
                }
                snapshot["open_orders"] = [
                    {
                        "order_id": item.get("orderId"),
                        "client_order_id": item.get("clientOrderId"),
                        "side": str(item.get("side", "")).upper().strip(),
                        "price": _safe_float(item.get("price")),
                        "orig_qty": _safe_float(item.get("origQty")),
                        "executed_qty": _safe_float(item.get("executedQty")),
                        "reduce_only": _truthy(item.get("reduceOnly")),
                        "position_side": str(item.get("positionSide", "BOTH")).upper().strip() or "BOTH",
                        "time": int(item.get("time", 0) or 0),
                    }
                    for item in open_orders
                ]
            user_trades, trade_meta = _load_or_fetch_trade_rows(
                audit_path=audit_paths["trade_audit"],
                symbol=normalized_symbol,
                api_key=api_key,
                api_secret=api_secret,
                session_start=stats_start,
            )
            income_rows, income_meta = _load_or_fetch_income_rows(
                audit_path=audit_paths["income_audit"],
                symbol=normalized_symbol,
                api_key=api_key,
                api_secret=api_secret,
                session_start=stats_start,
            )
            trade_start, trade_last = _epoch_bounds(user_trades, trade_row_time_ms)
            income_start, income_last = _epoch_bounds(income_rows, income_row_time_ms)
            session_start = _min_dt(stats_start, trade_start, income_start)
            stats_start = competition_start or session_start
            session_last = _max_dt(session_last, trade_last, income_last)
            snapshot["session"]["start"] = stats_start.isoformat() if stats_start else None
            snapshot["session"]["last_event"] = session_last.isoformat() if session_last else None
            snapshot["competition_window"]["stats_start_at"] = stats_start.isoformat() if stats_start else None
            commission_converter = _build_commission_converter(user_trades)
            trade_summary = summarize_user_trades(user_trades, commission_converter=commission_converter)
            income_summary = summarize_income(income_rows)
            hourly_summary = None
            try:
                hourly_window_start = max(
                    stats_start or (datetime.now(timezone.utc) - timedelta(hours=48)),
                    datetime.now(timezone.utc) - timedelta(hours=48),
                )
                hourly_window_start = _floor_hour(hourly_window_start)
                hourly_window_end = datetime.now(timezone.utc) + timedelta(minutes=1)
                hourly_candles = fetch_futures_klines(
                    symbol=normalized_symbol,
                    interval="1h",
                    start_ms=int(hourly_window_start.timestamp() * 1000),
                    end_ms=int(hourly_window_end.timestamp() * 1000),
                )
                hourly_summary = summarize_hourly_metrics(
                    user_trades,
                    income_rows,
                    hourly_candles,
                    commission_converter=commission_converter,
                    limit=24,
                )
            except Exception as exc:
                snapshot["warnings"].append(f"hourly_summary_failed: {type(exc).__name__}: {exc}")
            net_pnl_est = (
                trade_summary["realized_pnl"]
                + unrealized_pnl
                + income_summary["funding_fee"]
                - trade_summary["commission"]
            )
            trade_summary["net_pnl_estimate"] = net_pnl_est
            snapshot["trade_summary"] = trade_summary
            snapshot["income_summary"] = income_summary
            snapshot["hourly_summary"] = hourly_summary
            snapshot["audit"]["trade_source"] = trade_meta
            snapshot["audit"]["income_source"] = income_meta
            snapshot["audit"]["trade_row_count"] = len(user_trades)
            snapshot["audit"]["income_row_count"] = len(income_rows)
        except Exception as exc:
            snapshot["warnings"].append(f"account_read_failed: {type(exc).__name__}: {exc}")

    runner_config = runner.get("config", {})
    latest_loop = (loop_summary.get("latest") or {}) if isinstance(loop_summary, dict) else {}
    position = snapshot.get("position") or {}
    market = snapshot.get("market") or {}
    plan_snapshot = (submit_report or {}).get("plan_snapshot", {}) if isinstance(submit_report, dict) else {}
    strategy_mode = str(
        (plan_report or {}).get("strategy_mode")
        or runner_config.get("strategy_mode")
        or latest_loop.get("strategy_mode")
        or "one_way_long"
    ).strip() or "one_way_long"
    synthetic_mode = strategy_mode == "synthetic_neutral"
    target_neutral_mode = strategy_mode == "inventory_target_neutral"
    runner_is_running = bool(runner.get("is_running")) if isinstance(runner, dict) else False
    use_live_synthetic_position = synthetic_mode and not runner_is_running and isinstance(position, dict)

    synthetic_long_qty = _safe_float((plan_report or {}).get("current_long_qty"))
    if synthetic_long_qty <= 0:
        synthetic_long_qty = _safe_float(plan_snapshot.get("current_long_qty"))
    if synthetic_long_qty <= 0:
        synthetic_long_qty = _safe_float(latest_loop.get("current_long_qty"))

    synthetic_short_qty = _safe_float((plan_report or {}).get("current_short_qty"))
    if synthetic_short_qty <= 0:
        synthetic_short_qty = _safe_float(plan_snapshot.get("current_short_qty"))
    if synthetic_short_qty <= 0:
        synthetic_short_qty = _safe_float(latest_loop.get("current_short_qty"))

    synthetic_net_qty = _safe_float((plan_report or {}).get("synthetic_net_qty"))
    if abs(synthetic_net_qty) <= 0:
        synthetic_net_qty = _safe_float(plan_snapshot.get("synthetic_net_qty"))
    if abs(synthetic_net_qty) <= 0:
        synthetic_net_qty = _safe_float(latest_loop.get("synthetic_net_qty"))
    if abs(synthetic_net_qty) <= 0 and synthetic_mode:
        synthetic_net_qty = synthetic_long_qty - synthetic_short_qty

    synthetic_drift_qty = _safe_float((plan_report or {}).get("synthetic_drift_qty"))
    if abs(synthetic_drift_qty) <= 0:
        synthetic_drift_qty = _safe_float(plan_snapshot.get("synthetic_drift_qty"))
    if abs(synthetic_drift_qty) <= 0:
        synthetic_drift_qty = _safe_float(latest_loop.get("synthetic_drift_qty"))
    if use_live_synthetic_position:
        live_net_qty = _safe_float(position.get("position_amt"))
        if abs(live_net_qty) <= 1e-12:
            live_net_qty = 0.0
        synthetic_long_qty = live_net_qty if live_net_qty > 0 else 0.0
        synthetic_short_qty = -live_net_qty if live_net_qty < 0 else 0.0
        synthetic_net_qty = live_net_qty
        synthetic_drift_qty = 0.0

    current_long_notional = 0.0
    current_short_notional = 0.0
    if use_live_synthetic_position:
        mid_price = max(_safe_float(market.get("mid_price")), 0.0)
        current_long_notional = synthetic_long_qty * mid_price
        current_short_notional = synthetic_short_qty * mid_price
    elif synthetic_mode or target_neutral_mode:
        current_long_notional = _safe_float((plan_report or {}).get("current_long_notional"))
        if current_long_notional <= 0:
            current_long_notional = _safe_float(plan_snapshot.get("current_long_notional"))
        if current_long_notional <= 0:
            current_long_notional = _safe_float(latest_loop.get("current_long_notional"))
        current_short_notional = _safe_float((plan_report or {}).get("current_short_notional"))
        if current_short_notional <= 0:
            current_short_notional = _safe_float(plan_snapshot.get("current_short_notional"))
        if current_short_notional <= 0:
            current_short_notional = _safe_float(latest_loop.get("current_short_notional"))
    else:
        current_long_notional = (
            max(_safe_float(position.get("long_qty", position.get("position_amt"))), 0.0)
            * max(_safe_float(market.get("mid_price")), 0.0)
        )
        current_short_notional = (
            max(_safe_float(position.get("short_qty")), 0.0)
            * max(_safe_float(market.get("mid_price")), 0.0)
        )
    if current_long_notional <= 0 and not use_live_synthetic_position:
        current_long_notional = _safe_float(plan_report.get("current_long_notional")) if plan_report else 0.0
    if current_long_notional <= 0 and not use_live_synthetic_position:
        current_long_notional = _safe_float(latest_loop.get("current_long_notional"))
    if current_short_notional <= 0 and not use_live_synthetic_position:
        current_short_notional = _safe_float(plan_report.get("current_short_notional")) if plan_report else 0.0
    if current_short_notional <= 0 and not use_live_synthetic_position:
        current_short_notional = _safe_float(latest_loop.get("current_short_notional"))

    pause_buy_position_notional = _safe_float(runner_config.get("pause_buy_position_notional"))
    if pause_buy_position_notional <= 0:
        pause_buy_position_notional = None
    pause_short_position_notional = _safe_float(runner_config.get("pause_short_position_notional"))
    if pause_short_position_notional <= 0:
        pause_short_position_notional = None
    min_mid_price_for_buys = _safe_float(runner_config.get("min_mid_price_for_buys"))
    if min_mid_price_for_buys <= 0:
        min_mid_price_for_buys = None

    max_position_notional = _safe_float((plan_report or {}).get("max_position_notional"))
    if max_position_notional <= 0:
        max_position_notional = _safe_float(plan_snapshot.get("max_position_notional"))
    if max_position_notional <= 0:
        max_position_notional = _safe_float(runner_config.get("max_position_notional"))
    if max_position_notional <= 0:
        max_position_notional = None
    max_short_position_notional = _safe_float((plan_report or {}).get("max_short_position_notional"))
    if max_short_position_notional <= 0:
        max_short_position_notional = _safe_float(plan_snapshot.get("max_short_position_notional"))
    if max_short_position_notional <= 0:
        max_short_position_notional = _safe_float(runner_config.get("max_short_position_notional"))
    if max_short_position_notional <= 0:
        max_short_position_notional = None

    buy_budget_notional = _safe_float((plan_report or {}).get("buy_budget_notional"))
    if buy_budget_notional <= 0:
        buy_budget_notional = _safe_float(plan_snapshot.get("buy_budget_notional"))
    if buy_budget_notional <= 0:
        buy_budget_notional = None
    short_budget_notional = _safe_float((plan_report or {}).get("short_budget_notional"))
    if short_budget_notional <= 0:
        short_budget_notional = _safe_float(plan_snapshot.get("short_budget_notional"))
    if short_budget_notional <= 0:
        short_budget_notional = None

    planned_buy_notional = _safe_float((plan_report or {}).get("planned_buy_notional"))
    if planned_buy_notional <= 0:
        planned_buy_notional = _safe_float(plan_snapshot.get("planned_buy_notional"))
    if planned_buy_notional <= 0:
        planned_buy_notional = None
    planned_short_notional = _safe_float((plan_report or {}).get("planned_short_notional"))
    if planned_short_notional <= 0:
        planned_short_notional = _safe_float(plan_snapshot.get("planned_short_notional"))
    if planned_short_notional <= 0:
        planned_short_notional = None

    computed_pause_reasons: list[str] = []
    if pause_buy_position_notional is not None and current_long_notional >= pause_buy_position_notional:
        computed_pause_reasons.append(
            f"current_long_notional={current_long_notional:.4f} >= pause_buy_position_notional={pause_buy_position_notional:.4f}"
        )
    live_mid_price = _safe_float(market.get("mid_price"))
    if min_mid_price_for_buys is not None and live_mid_price > 0 and live_mid_price <= min_mid_price_for_buys:
        computed_pause_reasons.append(
            f"mid_price={live_mid_price:.7f} <= min_mid_price_for_buys={min_mid_price_for_buys:.7f}"
        )
    if computed_pause_reasons:
        buy_paused = True
        pause_reasons = computed_pause_reasons
    else:
        buy_paused = False
        pause_reasons = []
    short_pause_reasons: list[str] = []
    if pause_short_position_notional is not None and current_short_notional >= pause_short_position_notional:
        short_pause_reasons.append(
            f"current_short_notional={current_short_notional:.4f} >= pause_short_position_notional={pause_short_position_notional:.4f}"
        )
    short_paused = bool(short_pause_reasons)
    buy_cap_applied = bool((plan_report or {}).get("buy_cap_applied"))
    if not buy_cap_applied:
        buy_cap_applied = bool(plan_snapshot.get("buy_cap_applied"))
    if not buy_cap_applied:
        buy_cap_applied = bool(latest_loop.get("buy_cap_applied"))
    short_cap_applied = bool((plan_report or {}).get("short_cap_applied"))
    if not short_cap_applied:
        short_cap_applied = bool(plan_snapshot.get("short_cap_applied"))
    if not short_cap_applied:
        short_cap_applied = bool(latest_loop.get("short_cap_applied"))

    inventory_tier = (plan_report or {}).get("inventory_tier")
    if not isinstance(inventory_tier, dict) or not inventory_tier:
        inventory_tier = plan_snapshot.get("inventory_tier") if isinstance(plan_snapshot.get("inventory_tier"), dict) else {}

    market_guard = (plan_report or {}).get("market_guard")
    if not isinstance(market_guard, dict) or not market_guard:
        market_guard = {}
    volatility_buy_pause = bool(market_guard.get("buy_pause_active"))
    if not volatility_buy_pause:
        volatility_buy_pause = bool(latest_loop.get("volatility_buy_pause"))
    shift_frozen = bool(market_guard.get("shift_frozen"))
    if not shift_frozen:
        shift_frozen = bool(latest_loop.get("shift_frozen"))
    market_guard_return_ratio = _safe_float(market_guard.get("return_ratio"))
    if market_guard_return_ratio == 0:
        market_guard_return_ratio = _safe_float(latest_loop.get("market_guard_return_ratio"))
    market_guard_amplitude_ratio = _safe_float(market_guard.get("amplitude_ratio"))
    if market_guard_amplitude_ratio == 0:
        market_guard_amplitude_ratio = _safe_float(latest_loop.get("market_guard_amplitude_ratio"))

    auto_regime = (plan_report or {}).get("auto_regime")
    if not isinstance(auto_regime, dict) or not auto_regime:
        auto_regime = {}
    auto_regime_enabled = bool(auto_regime) or bool(runner_config.get("auto_regime_enabled"))
    auto_regime_regime = str(auto_regime.get("regime", "")).strip() or str(latest_loop.get("auto_regime_regime", "")).strip()
    auto_regime_reason = auto_regime.get("reason") or latest_loop.get("auto_regime_reason")
    auto_regime_pending_count = int(auto_regime.get("pending_count", 0) or latest_loop.get("auto_regime_pending_count", 0) or 0)
    auto_regime_confirm_cycles = int(auto_regime.get("confirm_cycles", 0) or latest_loop.get("auto_regime_confirm_cycles", 0) or 0)
    effective_strategy_profile = str(
        (plan_report or {}).get("effective_strategy_profile")
        or latest_loop.get("effective_strategy_profile")
        or runner_config.get("strategy_profile", "")
    ).strip()
    effective_strategy_label = str((plan_report or {}).get("effective_strategy_label") or "").strip()
    if not effective_strategy_label:
        effective_strategy_label = effective_strategy_profile
    xaut_adaptive = (plan_report or {}).get("xaut_adaptive")
    if not isinstance(xaut_adaptive, dict) or not xaut_adaptive:
        xaut_adaptive = {}
    xaut_adaptive_enabled = bool(xaut_adaptive) or bool(latest_loop.get("xaut_adaptive_enabled"))
    xaut_adaptive_state = str(xaut_adaptive.get("active_state", "")).strip() or str(latest_loop.get("xaut_adaptive_state", "")).strip()
    xaut_adaptive_candidate_state = (
        str(xaut_adaptive.get("candidate_state", "")).strip()
        or str(latest_loop.get("xaut_adaptive_candidate_state", "")).strip()
    )
    xaut_adaptive_pending_count = int(
        xaut_adaptive.get("pending_count", 0)
        or latest_loop.get("xaut_adaptive_pending_count", 0)
        or 0
    )
    xaut_adaptive_reason = xaut_adaptive.get("reason") or latest_loop.get("xaut_adaptive_reason")

    custom_grid_roll = (plan_report or {}).get("custom_grid_roll")
    if not isinstance(custom_grid_roll, dict) or not custom_grid_roll:
        custom_grid_roll = {}
    custom_grid_runtime_min_price = _safe_float(custom_grid_roll.get("current_min_price"))
    if custom_grid_runtime_min_price <= 0:
        custom_grid_runtime_min_price = _safe_float((plan_report or {}).get("custom_grid_runtime_min_price"))
    if custom_grid_runtime_min_price <= 0:
        custom_grid_runtime_min_price = _safe_float(latest_loop.get("custom_grid_runtime_min_price"))
    if custom_grid_runtime_min_price <= 0:
        custom_grid_runtime_min_price = _safe_float(runner_config.get("custom_grid_min_price"))
    custom_grid_runtime_max_price = _safe_float(custom_grid_roll.get("current_max_price"))
    if custom_grid_runtime_max_price <= 0:
        custom_grid_runtime_max_price = _safe_float((plan_report or {}).get("custom_grid_runtime_max_price"))
    if custom_grid_runtime_max_price <= 0:
        custom_grid_runtime_max_price = _safe_float(latest_loop.get("custom_grid_runtime_max_price"))
    if custom_grid_runtime_max_price <= 0:
        custom_grid_runtime_max_price = _safe_float(runner_config.get("custom_grid_max_price"))
    custom_grid_roll_enabled = bool(custom_grid_roll.get("enabled"))
    if not custom_grid_roll_enabled:
        custom_grid_roll_enabled = bool(runner_config.get("custom_grid_roll_enabled"))
    custom_grid_roll_checked = bool(custom_grid_roll.get("checked"))
    if not custom_grid_roll_checked:
        custom_grid_roll_checked = bool(latest_loop.get("custom_grid_roll_checked"))
    custom_grid_roll_triggered = bool(custom_grid_roll.get("triggered"))
    if not custom_grid_roll_triggered:
        custom_grid_roll_triggered = bool(latest_loop.get("custom_grid_roll_triggered"))
    custom_grid_roll_reason = str(custom_grid_roll.get("reason", "") or latest_loop.get("custom_grid_roll_reason", "") or "")
    custom_grid_roll_interval_minutes = int(
        custom_grid_roll.get("interval_minutes", 0)
        or latest_loop.get("custom_grid_roll_interval_minutes", 0)
        or runner_config.get("custom_grid_roll_interval_minutes", 0)
        or 0
    )
    custom_grid_roll_trade_threshold = int(
        custom_grid_roll.get("trade_threshold", 0)
        or latest_loop.get("custom_grid_roll_trade_threshold", 0)
        or runner_config.get("custom_grid_roll_trade_threshold", 0)
        or 0
    )
    custom_grid_roll_upper_distance_ratio = _safe_float(
        custom_grid_roll.get("upper_distance_ratio")
        if custom_grid_roll.get("upper_distance_ratio") is not None
        else latest_loop.get("custom_grid_roll_upper_distance_ratio")
        if latest_loop.get("custom_grid_roll_upper_distance_ratio") is not None
        else runner_config.get("custom_grid_roll_upper_distance_ratio")
    )
    custom_grid_roll_shift_levels = int(
        custom_grid_roll.get("shift_levels", 0)
        or latest_loop.get("custom_grid_roll_shift_levels", 0)
        or runner_config.get("custom_grid_roll_shift_levels", 0)
        or 0
    )
    custom_grid_roll_current_trade_count = int(
        custom_grid_roll.get("current_trade_count", 0)
        or latest_loop.get("custom_grid_roll_current_trade_count", 0)
        or 0
    )
    custom_grid_roll_trade_baseline = int(
        custom_grid_roll.get("trade_baseline", 0)
        or latest_loop.get("custom_grid_roll_trade_baseline", 0)
        or 0
    )
    custom_grid_roll_trades_since_last_roll = int(
        custom_grid_roll.get("trades_since_last_roll", 0)
        or latest_loop.get("custom_grid_roll_trades_since_last_roll", 0)
        or 0
    )
    custom_grid_roll_levels_above_current = int(
        custom_grid_roll.get("levels_above_current", 0)
        or latest_loop.get("custom_grid_roll_levels_above_current", 0)
        or 0
    )
    custom_grid_roll_required_levels_above = int(
        custom_grid_roll.get("required_levels_above", 0)
        or latest_loop.get("custom_grid_roll_required_levels_above", 0)
        or 0
    )
    custom_grid_roll_last_check_bucket = (
        custom_grid_roll.get("last_check_bucket")
        or latest_loop.get("custom_grid_roll_last_check_bucket")
        or None
    )
    custom_grid_roll_last_applied_at = (
        custom_grid_roll.get("last_applied_at")
        or latest_loop.get("custom_grid_roll_last_applied_at")
        or None
    )
    custom_grid_roll_last_applied_price = _safe_float(
        custom_grid_roll.get("last_applied_price")
        if custom_grid_roll.get("last_applied_price") is not None
        else latest_loop.get("custom_grid_roll_last_applied_price")
    )
    custom_grid_roll_last_old_min_price = _safe_float(
        custom_grid_roll.get("last_old_min_price")
        if custom_grid_roll.get("last_old_min_price") is not None
        else latest_loop.get("custom_grid_roll_last_old_min_price")
    )
    custom_grid_roll_last_old_max_price = _safe_float(
        custom_grid_roll.get("last_old_max_price")
        if custom_grid_roll.get("last_old_max_price") is not None
        else latest_loop.get("custom_grid_roll_last_old_max_price")
    )
    custom_grid_roll_last_new_min_price = _safe_float(
        custom_grid_roll.get("last_new_min_price")
        if custom_grid_roll.get("last_new_min_price") is not None
        else latest_loop.get("custom_grid_roll_last_new_min_price")
    )
    custom_grid_roll_last_new_max_price = _safe_float(
        custom_grid_roll.get("last_new_max_price")
        if custom_grid_roll.get("last_new_max_price") is not None
        else latest_loop.get("custom_grid_roll_last_new_max_price")
    )
    threshold_target_reduce = (plan_report or {}).get("threshold_target_reduce")
    if not isinstance(threshold_target_reduce, dict):
        threshold_target_reduce = {}
    adverse_inventory_reduce = (plan_report or {}).get("adverse_inventory_reduce")
    if not isinstance(adverse_inventory_reduce, dict):
        adverse_inventory_reduce = {}
    hard_loss_forced_reduce = (plan_report or {}).get("hard_loss_forced_reduce")
    if not isinstance(hard_loss_forced_reduce, dict):
        hard_loss_forced_reduce = {}

    if synthetic_mode:
        snapshot["position"]["virtual_long_qty"] = synthetic_long_qty
        snapshot["position"]["virtual_short_qty"] = synthetic_short_qty
        snapshot["position"]["virtual_net_qty"] = synthetic_net_qty
        snapshot["position"]["ledger_drift_qty"] = synthetic_drift_qty
    if target_neutral_mode:
        snapshot["position"]["long_qty"] = max(_safe_float(position.get("position_amt")), 0.0)
        snapshot["position"]["short_qty"] = max(-_safe_float(position.get("position_amt")), 0.0)

    snapshot["risk_controls"] = {
        "strategy_mode": strategy_mode,
        "pause_buy_position_notional": pause_buy_position_notional,
        "pause_short_position_notional": pause_short_position_notional,
        "max_position_notional": max_position_notional,
        "max_short_position_notional": max_short_position_notional,
        "current_long_notional": current_long_notional,
        "current_short_notional": current_short_notional,
        "buy_budget_notional": buy_budget_notional,
        "short_budget_notional": short_budget_notional,
        "planned_buy_notional": planned_buy_notional,
        "planned_short_notional": planned_short_notional,
        "remaining_headroom": (max_position_notional - current_long_notional) if max_position_notional is not None else None,
        "remaining_short_headroom": (max_short_position_notional - current_short_notional) if max_short_position_notional is not None else None,
        "remaining_buy_budget": (
            buy_budget_notional - planned_buy_notional
            if buy_budget_notional is not None and planned_buy_notional is not None
            else None
        ),
        "buy_paused": buy_paused,
        "short_paused": short_paused,
        "buy_cap_applied": buy_cap_applied,
        "short_cap_applied": short_cap_applied,
        "pause_reasons": pause_reasons,
        "short_pause_reasons": short_pause_reasons,
        "inventory_tier": inventory_tier,
        "inventory_tier_active": bool(inventory_tier.get("active")),
        "inventory_tier_ratio": _safe_float(inventory_tier.get("ratio")),
        "volatility_buy_pause": volatility_buy_pause,
        "shift_frozen": shift_frozen,
        "market_guard_return_ratio": market_guard_return_ratio,
        "market_guard_amplitude_ratio": market_guard_amplitude_ratio,
        "market_guard_warning": market_guard.get("warning"),
        "auto_regime_enabled": auto_regime_enabled,
        "auto_regime_regime": auto_regime_regime,
        "auto_regime_reason": auto_regime_reason,
        "auto_regime_pending_count": auto_regime_pending_count,
        "auto_regime_confirm_cycles": auto_regime_confirm_cycles,
        "effective_strategy_profile": effective_strategy_profile,
        "effective_strategy_label": effective_strategy_label,
        "xaut_adaptive_enabled": xaut_adaptive_enabled,
        "xaut_adaptive_state": xaut_adaptive_state,
        "xaut_adaptive_candidate_state": xaut_adaptive_candidate_state,
        "xaut_adaptive_pending_count": xaut_adaptive_pending_count,
        "xaut_adaptive_reason": xaut_adaptive_reason,
        "inventory_target_bands": (plan_report or {}).get("inventory_target_bands"),
        "neutral_hourly_scale": (plan_report or {}).get("neutral_hourly_scale"),
        "neutral_hourly_scale_enabled": bool(((plan_report or {}).get("neutral_hourly_scale") or {}).get("enabled")),
        "neutral_hourly_scale_ratio": _safe_float(((plan_report or {}).get("neutral_hourly_scale") or {}).get("scale")),
        "neutral_hourly_regime": str((((plan_report or {}).get("neutral_hourly_scale") or {}).get("regime", "") or "")),
        "neutral_hourly_reason": ((plan_report or {}).get("neutral_hourly_scale") or {}).get("reason"),
        "center_source": (plan_report or {}).get("center_source"),
        "threshold_reduce_enabled": bool(
            threshold_target_reduce.get("enabled") if threshold_target_reduce else latest_loop.get("threshold_reduce_enabled")
        ),
        "threshold_reduce_active": bool(
            threshold_target_reduce.get("active") if threshold_target_reduce else latest_loop.get("threshold_reduce_active")
        ),
        "threshold_reduce_long_active": bool(
            threshold_target_reduce.get("long_active") if threshold_target_reduce else latest_loop.get("threshold_reduce_long_active")
        ),
        "threshold_reduce_short_active": bool(
            threshold_target_reduce.get("short_active") if threshold_target_reduce else latest_loop.get("threshold_reduce_short_active")
        ),
        "threshold_reduce_long_taker_timeout_active": bool(
            threshold_target_reduce.get("long_taker_timeout_active")
            if threshold_target_reduce
            else latest_loop.get("threshold_reduce_long_taker_timeout_active")
        ),
        "threshold_reduce_short_taker_timeout_active": bool(
            threshold_target_reduce.get("short_taker_timeout_active")
            if threshold_target_reduce
            else latest_loop.get("threshold_reduce_short_taker_timeout_active")
        ),
        "adverse_reduce_enabled": bool(
            adverse_inventory_reduce.get("enabled") if adverse_inventory_reduce else latest_loop.get("adverse_reduce_enabled")
        ),
        "adverse_reduce_active": bool(
            adverse_inventory_reduce.get("active") if adverse_inventory_reduce else latest_loop.get("adverse_reduce_active")
        ),
        "adverse_reduce_direction": str(
            (
                adverse_inventory_reduce.get("direction")
                if adverse_inventory_reduce
                else latest_loop.get("adverse_reduce_direction", "")
            )
            or ""
        ),
        "adverse_reduce_long_aggressive": bool(
            adverse_inventory_reduce.get("long_aggressive")
            if adverse_inventory_reduce
            else latest_loop.get("adverse_reduce_long_aggressive")
        ),
        "adverse_reduce_short_aggressive": bool(
            adverse_inventory_reduce.get("short_aggressive")
            if adverse_inventory_reduce
            else latest_loop.get("adverse_reduce_short_aggressive")
        ),
        "hard_loss_forced_reduce_enabled": bool(
            hard_loss_forced_reduce.get("enabled")
            if hard_loss_forced_reduce
            else latest_loop.get("hard_loss_forced_reduce_enabled")
        ),
        "hard_loss_forced_reduce_active": bool(
            hard_loss_forced_reduce.get("active")
            if hard_loss_forced_reduce
            else latest_loop.get("hard_loss_forced_reduce_active")
        ),
        "hard_loss_forced_reduce_reason": (
            hard_loss_forced_reduce.get("reason")
            if hard_loss_forced_reduce
            else latest_loop.get("hard_loss_forced_reduce_reason")
        ),
        "hard_loss_forced_reduce_blocked_reason": (
            hard_loss_forced_reduce.get("blocked_reason")
            if hard_loss_forced_reduce
            else latest_loop.get("hard_loss_forced_reduce_blocked_reason")
        ),
        "hard_loss_forced_reduce_side": str(
            (
                hard_loss_forced_reduce.get("side")
                if hard_loss_forced_reduce
                else latest_loop.get("hard_loss_forced_reduce_side", "")
            )
            or ""
        ),
        "hard_loss_forced_reduce_unrealized_pnl": _safe_float(
            hard_loss_forced_reduce.get("unrealized_pnl")
            if hard_loss_forced_reduce
            else latest_loop.get("hard_loss_forced_reduce_unrealized_pnl")
        ),
        "effective_buy_levels": int((plan_report or {}).get("effective_buy_levels", 0) or 0),
        "effective_sell_levels": int((plan_report or {}).get("effective_sell_levels", 0) or 0),
        "effective_per_order_notional": _safe_float((plan_report or {}).get("effective_per_order_notional")),
        "effective_base_position_notional": _safe_float((plan_report or {}).get("effective_base_position_notional")),
        "runtime_status": str(latest_loop.get("runtime_status", "") or "running"),
        "stop_triggered": bool(latest_loop.get("stop_triggered")),
        "stop_reason": latest_loop.get("stop_reason"),
        "stop_reasons": list(latest_loop.get("stop_reasons") or []),
        "stop_triggered_at": latest_loop.get("stop_triggered_at"),
        "run_start_time": runner_config.get("run_start_time"),
        "run_end_time": runner_config.get("run_end_time"),
        "runtime_guard_stats_start_time": latest_loop.get("runtime_guard_stats_start_time"),
        "rolling_hourly_loss": _safe_float(latest_loop.get("rolling_hourly_loss")),
        "rolling_hourly_loss_limit": _safe_float(runner_config.get("rolling_hourly_loss_limit")),
        "cumulative_gross_notional": _safe_float(latest_loop.get("cumulative_gross_notional")),
        "max_cumulative_notional": _safe_float(runner_config.get("max_cumulative_notional")),
        "custom_grid_runtime_min_price": custom_grid_runtime_min_price,
        "custom_grid_runtime_max_price": custom_grid_runtime_max_price,
        "custom_grid_roll_enabled": custom_grid_roll_enabled,
        "custom_grid_roll_checked": custom_grid_roll_checked,
        "custom_grid_roll_triggered": custom_grid_roll_triggered,
        "custom_grid_roll_reason": custom_grid_roll_reason or None,
        "custom_grid_roll_interval_minutes": custom_grid_roll_interval_minutes,
        "custom_grid_roll_trade_threshold": custom_grid_roll_trade_threshold,
        "custom_grid_roll_upper_distance_ratio": custom_grid_roll_upper_distance_ratio,
        "custom_grid_roll_shift_levels": custom_grid_roll_shift_levels,
        "custom_grid_roll_current_trade_count": custom_grid_roll_current_trade_count,
        "custom_grid_roll_trade_baseline": custom_grid_roll_trade_baseline,
        "custom_grid_roll_trades_since_last_roll": custom_grid_roll_trades_since_last_roll,
        "custom_grid_roll_levels_above_current": custom_grid_roll_levels_above_current,
        "custom_grid_roll_required_levels_above": custom_grid_roll_required_levels_above,
        "custom_grid_roll_last_check_bucket": custom_grid_roll_last_check_bucket,
        "custom_grid_roll_last_applied_at": custom_grid_roll_last_applied_at,
        "custom_grid_roll_last_applied_price": custom_grid_roll_last_applied_price if custom_grid_roll_last_applied_price > 0 else None,
        "custom_grid_roll_last_old_min_price": custom_grid_roll_last_old_min_price if custom_grid_roll_last_old_min_price > 0 else None,
        "custom_grid_roll_last_old_max_price": custom_grid_roll_last_old_max_price if custom_grid_roll_last_old_max_price > 0 else None,
        "custom_grid_roll_last_new_min_price": custom_grid_roll_last_new_min_price if custom_grid_roll_last_new_min_price > 0 else None,
        "custom_grid_roll_last_new_max_price": custom_grid_roll_last_new_max_price if custom_grid_roll_last_new_max_price > 0 else None,
    }
    snapshot["alerts"] = build_monitor_alerts(
        loop_summary=loop_summary,
        warnings=snapshot["warnings"],
        risk_controls=snapshot["risk_controls"],
        runner=runner,
    )

    return snapshot
