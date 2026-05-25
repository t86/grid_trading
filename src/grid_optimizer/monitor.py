from __future__ import annotations

import json
import math
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
    append_jsonl,
    build_audit_paths,
    count_jsonl_lines,
    fetch_time_paged,
    income_row_key,
    income_row_time_ms,
    iter_jsonl,
    parse_iso_ts as _parse_iso_ts,
    read_json as _read_json,
    read_jsonl_filtered,
    read_trade_audit_rows,
    trade_row_key,
    trade_row_time_ms,
    write_json,
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
from .competition_board import (
    build_competition_displacement_volume,
    build_competition_entry_volume_targets,
    build_reward_volume_targets,
    resolve_active_competition_board,
)

def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _trade_sort_key(value: Any) -> tuple[int, int | str]:
    text = str(value or "").strip()
    if not text:
        return (0, 0)
    try:
        return (0, int(text))
    except (TypeError, ValueError):
        return (1, text)


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"true", "1", "yes"}


def _truncate_two_decimals(value: float) -> float:
    return math.floor(max(float(value), 0.0) * 100.0) / 100.0


def _competition_uses_daily_sqrt_score(board: dict[str, Any] | None) -> bool:
    if not isinstance(board, dict):
        return False
    source_slug = str(board.get("source_slug", "")).strip().lower()
    symbol = str(board.get("symbol", "")).strip().upper()
    metric_field = str(board.get("metric_field", "")).strip().lower()
    return source_slug == "futures_pharos" or (symbol == "PHAROS" and metric_field == "grade")


def _competition_daily_sqrt_score(trade_summary: dict[str, Any] | None) -> float:
    if not isinstance(trade_summary, dict):
        return 0.0
    score = 0.0
    rows = trade_summary.get("daily_volume_rows")
    if isinstance(rows, list) and rows:
        for row in rows:
            if not isinstance(row, dict):
                continue
            gross = max(_safe_float(row.get("gross_notional")), 0.0)
            if gross <= 0:
                continue
            score += _truncate_two_decimals(math.sqrt(gross))
        return float(score)
    gross = max(_safe_float(trade_summary.get("gross_notional")), 0.0)
    return _truncate_two_decimals(math.sqrt(gross)) if gross > 0 else 0.0


def _competition_current_metric_value(
    board: dict[str, Any] | None,
    trade_summary: dict[str, Any] | None,
) -> float:
    if _competition_uses_daily_sqrt_score(board):
        return _competition_daily_sqrt_score(trade_summary)
    if not isinstance(trade_summary, dict):
        return 0.0
    return _safe_float(trade_summary.get("gross_notional"))


def _sort_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


RUNNER_PID_PATH = Path("output/night_loop_runner.pid")
RUNNER_CONTROL_PATH = Path("output/night_loop_runner_control.json")
RUNNER_LOG_PATH = Path("output/night_loop_runner.log")
STABLE_QUOTE_ASSETS = {"USDT", "USDC", "FDUSD", "BUSD"}
MONITOR_CACHE_TTL_SECONDS = 2.0
LOCAL_RUNTIME_SNAPSHOT_MAX_AGE_SECONDS = 180.0
LIVE_ACCOUNT_SNAPSHOT_TTL_SECONDS = float(os.environ.get("GRID_MONITOR_LIVE_ACCOUNT_TTL_SECONDS", "60.0"))
_MONITOR_CACHE_LOCK = threading.Lock()
_MONITOR_CACHE: dict[tuple[str, str, str, str, int], tuple[float, dict[str, Any]]] = {}
_MONITOR_INFLIGHT: dict[tuple[str, str, str, str, int], "_InflightMonitorSnapshot"] = {}
_LIVE_ACCOUNT_CACHE: dict[tuple[str, str], tuple[float, dict[str, Any]]] = {}


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

        if token in {
            "--apply",
            "--cancel-stale",
            "--reset-state",
            "--fixed-center-enabled",
            "--execution-regime-enabled",
        }:
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
            "--execution-regime-vol-p50-ratio",
            "--execution-regime-vol-p95-ratio",
            "--execution-regime-spread-p50-bps",
            "--execution-regime-spread-p95-bps",
            "--execution-regime-trend-p50-ratio",
            "--execution-regime-trend-p95-ratio",
            "--execution-regime-depth-value",
            "--execution-regime-depth-p10-notional",
            "--execution-regime-depth-p50-notional",
            "--execution-regime-impact-q",
            "--execution-regime-anomaly-q",
            "--execution-regime-safe-score-upper",
            "--execution-regime-normal-score-upper",
            "--execution-regime-caution-score-upper",
            "--execution-regime-recover-exit-to-caution-score",
            "--execution-regime-recover-caution-to-normal-score",
            "--execution-regime-recover-normal-to-safe-score",
            "--execution-regime-vol-exit-q",
            "--execution-regime-spread-exit-q",
            "--execution-regime-depth-exit-q",
            "--execution-regime-latency-ms",
            "--execution-regime-latency-ms-exit",
            "--execution-regime-order-failure-rate",
            "--execution-regime-order-failure-rate-exit",
            "--execution-regime-inventory-notional-limit",
            "--execution-regime-rolling-loss-abs",
            "--execution-regime-rolling-loss-limit",
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
            "--execution-regime-confirm-exit-to-caution",
            "--execution-regime-confirm-caution-to-normal",
            "--execution-regime-confirm-normal-to-safe",
            "--execution-regime-confirm-normal-to-caution",
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


def _monitor_anchor_path(audit_paths: dict[str, Path]) -> Path:
    audit_state_path = audit_paths["audit_state"]
    return audit_state_path.with_name(audit_state_path.name.replace("_audit_state.json", "_monitor_anchor.json"))


def _jsonl_time_bounds(path: Path, row_time_ms: Any) -> tuple[datetime | None, datetime | None]:
    first_ts: datetime | None = None
    last_ts: datetime | None = None
    for item in iter_jsonl(path):
        ts_ms = int(row_time_ms(item) or 0)
        if ts_ms <= 0:
            continue
        ts = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
        if first_ts is None or ts < first_ts:
            first_ts = ts
        if last_ts is None or ts > last_ts:
            last_ts = ts
    return first_ts, last_ts


def _resolve_monitor_stats_anchor(
    *,
    anchor_path: Path,
    symbol: str,
    candidates: list[tuple[str, datetime | None]],
) -> tuple[datetime | None, dict[str, Any]]:
    normalized_symbol = str(symbol).upper().strip()
    persisted_payload = _read_json(anchor_path)
    persisted_start = _parse_iso_ts((persisted_payload or {}).get("stats_start_at"))
    selected_start = persisted_start
    selected_source = "persisted" if persisted_start is not None else None
    for source, value in candidates:
        if value is None:
            continue
        candidate = value.astimezone(timezone.utc)
        if selected_start is None or candidate < selected_start:
            selected_start = candidate
            selected_source = source
    if selected_start is None:
        return None, {
            "path": str(anchor_path),
            "source": None,
            "persisted": False,
            "updated": False,
        }
    updated = (
        persisted_start is None
        or selected_start != persisted_start
        or str((persisted_payload or {}).get("symbol", "")).upper().strip() != normalized_symbol
    )
    if updated:
        write_json(
            anchor_path,
            {
                "symbol": normalized_symbol,
                "stats_start_at": selected_start.isoformat(),
                "source": selected_source,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            },
        )
    return selected_start, {
        "path": str(anchor_path),
        "source": selected_source,
        "persisted": persisted_start is not None,
        "updated": updated,
        "stats_start_at": selected_start.isoformat(),
    }


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
    audit_rows = read_trade_audit_rows(
        audit_path,
        limit=0,
        predicate=lambda item: int(trade_row_time_ms(item) or 0) >= floor_ms,
    )
    start_time_ms = int(effective_start.timestamp() * 1000)
    end_time_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    try:
        api_rows = fetch_time_paged(
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
    except Exception as exc:
        if audit_rows:
            return audit_rows, {
                "source": "audit",
                "path": str(audit_path),
                "row_count": len(audit_rows),
                "start_time": effective_start.isoformat(),
                "api_error": f"{type(exc).__name__}: {exc}",
            }
        raise
    def _trade_merge_key(row: dict[str, Any]) -> tuple[Any, ...]:
        order_id = row.get("orderId")
        if order_id is not None:
            return (
                "order_fill",
                str(order_id).strip(),
                str(row.get("side", "")).upper().strip(),
                round(_safe_float(row.get("price")), 12),
                round(_safe_float(row.get("qty")), 12),
            )
        return (
            "trade_id",
            int(trade_row_time_ms(row) or 0),
            trade_row_key(row),
        )

    merged: list[dict[str, Any]] = []
    seen: set[tuple[Any, ...]] = set()
    audit_keys: set[tuple[Any, ...]] = set()
    duplicate_count = 0
    for row in audit_rows:
        key = _trade_merge_key(row)
        audit_keys.add(key)
        if key in seen:
            duplicate_count += 1
            continue
        seen.add(key)
        merged.append(row)
    missing_api_rows: list[dict[str, Any]] = []
    for row in api_rows:
        key = _trade_merge_key(row)
        if key in seen:
            duplicate_count += 1
            continue
        if key not in audit_keys:
            missing_api_rows.append(row)
        seen.add(key)
        merged.append(row)
    rows = sorted(merged, key=lambda item: (int(trade_row_time_ms(item) or 0), trade_row_key(item)))
    for row in missing_api_rows:
        append_jsonl(audit_path, row)
    source = "audit+api" if audit_rows and api_rows else ("audit" if audit_rows else "api")
    return rows, {
        "source": source,
        "path": str(audit_path) if audit_rows or missing_api_rows else None,
        "row_count": len(rows),
        "audit_row_count": len(audit_rows),
        "api_row_count": len(api_rows),
        "merged_row_count": len(rows),
        "deduped_row_count": duplicate_count,
        "missing_api_row_count": len(missing_api_rows),
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
    start_time_ms = int(effective_start.timestamp() * 1000)
    end_time_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    try:
        api_rows = fetch_time_paged(
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
    except Exception as exc:
        if audit_rows:
            return audit_rows, {
                "source": "audit",
                "path": str(audit_path),
                "row_count": len(audit_rows),
                "start_time": effective_start.isoformat(),
                "api_error": f"{type(exc).__name__}: {exc}",
            }
        raise
    merged: list[dict[str, Any]] = []
    seen: set[tuple[int, str]] = set()
    audit_keys: set[tuple[int, str]] = set()
    duplicate_count = 0
    for row in audit_rows:
        key = (int(income_row_time_ms(row) or 0), income_row_key(row))
        audit_keys.add(key)
        if key in seen:
            duplicate_count += 1
            continue
        seen.add(key)
        merged.append(row)
    missing_api_rows: list[dict[str, Any]] = []
    for row in api_rows:
        key = (int(income_row_time_ms(row) or 0), income_row_key(row))
        if key in seen:
            duplicate_count += 1
            continue
        if key not in audit_keys:
            missing_api_rows.append(row)
        seen.add(key)
        merged.append(row)
    rows = sorted(merged, key=lambda item: (int(income_row_time_ms(item) or 0), income_row_key(item)))
    for row in missing_api_rows:
        append_jsonl(audit_path, row)
    source = "audit+api" if audit_rows and api_rows else ("audit" if audit_rows else "api")
    return rows, {
        "source": source,
        "path": str(audit_path) if audit_rows or missing_api_rows else None,
        "row_count": len(rows),
        "audit_row_count": len(audit_rows),
        "api_row_count": len(api_rows),
        "merged_row_count": len(rows),
        "deduped_row_count": duplicate_count,
        "missing_api_row_count": len(missing_api_rows),
        "start_time": effective_start.isoformat(),
    }


def _merge_time_keyed_rows(
    *,
    audit_rows: list[dict[str, Any]],
    api_rows: list[dict[str, Any]],
    row_time_ms: Any,
    row_key: Any,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rows_by_identity: dict[tuple[int, str], dict[str, Any]] = {}
    audit_identities: set[tuple[int, str]] = set()
    for item in audit_rows:
        ts_ms = int(row_time_ms(item) or 0)
        key = str(row_key(item) or "").strip()
        if ts_ms <= 0 or not key:
            continue
        identity = (ts_ms, key)
        audit_identities.add(identity)
        rows_by_identity[identity] = item

    missing_api_rows: list[dict[str, Any]] = []
    for item in api_rows:
        ts_ms = int(row_time_ms(item) or 0)
        key = str(row_key(item) or "").strip()
        if ts_ms <= 0 or not key:
            continue
        identity = (ts_ms, key)
        if identity not in audit_identities:
            missing_api_rows.append(item)
        rows_by_identity.setdefault(identity, item)

    rows = list(rows_by_identity.items())
    rows.sort(key=lambda pair: (pair[0][0], pair[0][1]))
    missing_api_rows.sort(key=lambda item: (int(row_time_ms(item) or 0), str(row_key(item) or "")))
    return [item for _, item in rows], missing_api_rows


def summarize_user_trades(trades: list[dict[str, Any]], commission_converter: Any | None = None) -> dict[str, Any]:
    sorted_trades = sorted(
        trades,
        key=lambda item: (int(item.get("time", 0) or 0), _trade_sort_key(item.get("id"))),
    )
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
    daily_rows: dict[str, dict[str, Any]] = {}

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
        trade_day = (
            datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
            if ts_ms
            else ""
        )
        if trade_day:
            daily = daily_rows.setdefault(
                trade_day,
                {
                    "date": trade_day,
                    "timezone": "UTC+0",
                    "trade_count": 0,
                    "buy_count": 0,
                    "sell_count": 0,
                    "gross_notional": 0.0,
                    "buy_notional": 0.0,
                    "sell_notional": 0.0,
                    "realized_pnl": 0.0,
                    "commission": 0.0,
                    "net_realized": 0.0,
                },
            )
            daily["trade_count"] += 1
            daily["gross_notional"] += trade_notional
            daily["realized_pnl"] += trade_realized
            daily["commission"] += trade_commission
            daily["net_realized"] += trade_realized - trade_commission
            if side == "BUY":
                daily["buy_count"] += 1
                daily["buy_notional"] += trade_notional
            elif side == "SELL":
                daily["sell_count"] += 1
                daily["sell_notional"] += trade_notional
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
        "daily_volume_rows": [daily_rows[key] for key in sorted(daily_rows, reverse=True)],
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


TRADE_QUALITY_BUCKETS = (
    "normal_grid",
    "take_profit_recycle",
    "active_delever",
    "adverse_reduce",
    "hard_loss_forced_reduce",
    "protective_flatten",
    "manual_unknown",
)


def _empty_trade_quality_bucket() -> dict[str, Any]:
    return {
        "count": 0,
        "gross_notional": 0.0,
        "realized_pnl": 0.0,
        "commission": 0.0,
    }


def _empty_trade_quality_buckets() -> dict[str, dict[str, Any]]:
    return {key: _empty_trade_quality_bucket() for key in TRADE_QUALITY_BUCKETS}


def _normalize_trade_role_text(value: Any) -> str:
    return str(value or "").strip().lower().replace("-", "_")


def _trade_role_from_client_order_id(value: Any) -> str:
    text = _normalize_trade_role_text(value)
    if not text:
        return ""
    if "takeprof" in text or "take_profit" in text:
        return "take_profit"
    if "entrylon" in text or "entry_long" in text or "grid_long" in text:
        return "entry_long"
    if "entrysho" in text or "entry_short" in text or "grid_short" in text:
        return "entry_short"
    if "activede" in text or "active_delever" in text:
        return "active_delever"
    if "adverser" in text or "adverse_reduce" in text:
        return "adverse_reduce"
    if "harddele" in text or "hard_loss" in text or "forced_reduce" in text:
        return "hard_loss_forced_reduce"
    if "softdele" in text or "protect" in text or "flatten" in text or "flowslee" in text:
        return "protective_flatten"
    return ""


def _trade_quality_bucket_for_role(role: Any) -> str:
    normalized = _normalize_trade_role_text(role)
    if not normalized:
        return "manual_unknown"
    if normalized in {
        "entry",
        "bootstrap",
        "bootstrap_entry",
        "grid_entry",
        "entry_long",
        "entry_short",
        "bootstrap_long",
        "bootstrap_short",
    }:
        return "normal_grid"
    if normalized in {"take_profit", "take_profit_long", "take_profit_short", "grid_exit"}:
        return "take_profit_recycle"
    if "active_delever" in normalized or "activede" in normalized:
        return "active_delever"
    if "adverse_reduce" in normalized or "adverser" in normalized:
        return "adverse_reduce"
    if "hard_loss_forced_reduce" in normalized or "hard_delever" in normalized or "harddele" in normalized:
        return "hard_loss_forced_reduce"
    if (
        "protect" in normalized
        or "flatten" in normalized
        or "soft_delever" in normalized
        or "softdele" in normalized
        or "flow_sleeve" in normalized
        or "flowslee" in normalized
        or normalized == "forced_reduce"
        or normalized == "tail_cleanup"
    ):
        return "protective_flatten"
    return "manual_unknown"


def _trade_role_from_lookup(item: dict[str, Any], order_role_lookup: dict[str, dict[str, Any]] | None) -> str:
    for key in ("role", "order_role", "strategy_role", "source_role"):
        role = _normalize_trade_role_text(item.get(key))
        if role:
            return role
    role = _trade_role_from_client_order_id(
        item.get("clientOrderId")
        or item.get("client_order_id")
        or item.get("origClientOrderId")
        or item.get("orig_client_order_id")
    )
    if role:
        return role
    if not order_role_lookup:
        return ""
    order_id = str(item.get("orderId") or item.get("order_id") or "").strip()
    if not order_id:
        return ""
    ref = order_role_lookup.get(order_id)
    if not isinstance(ref, dict):
        return ""
    return _normalize_trade_role_text(ref.get("role")) or _trade_role_from_client_order_id(ref.get("client_order_id"))


def _finalize_trade_quality(bucket: dict[str, Any]) -> dict[str, Any]:
    gross = float(bucket.get("gross_notional") or 0.0)
    buy = float(bucket.get("buy_notional") or 0.0)
    sell = float(bucket.get("sell_notional") or 0.0)
    imbalance_ratio = abs(buy - sell) / gross if gross > 0 else 0.0
    buckets = bucket.get("quality_buckets") if isinstance(bucket.get("quality_buckets"), dict) else {}
    healthy_gross = sum(
        float((buckets.get(key) or {}).get("gross_notional") or 0.0)
        for key in ("normal_grid", "take_profit_recycle")
    )
    protective_gross = sum(
        float((buckets.get(key) or {}).get("gross_notional") or 0.0)
        for key in ("active_delever", "adverse_reduce", "hard_loss_forced_reduce", "protective_flatten")
    )
    unknown_gross = float((buckets.get("manual_unknown") or {}).get("gross_notional") or 0.0)
    protective_share = protective_gross / gross if gross > 0 else 0.0
    unknown_share = unknown_gross / gross if gross > 0 else 0.0
    reasons: list[str] = []
    if gross > 0 and protective_share >= 0.30:
        reasons.append(f"保护/减仓成交占比 {protective_share:.1%}")
    if gross > 0 and unknown_share >= 0.25:
        reasons.append(f"未知成交占比 {unknown_share:.1%}")
    if gross >= 1000.0 and imbalance_ratio >= 0.35:
        reasons.append(f"买卖失衡 {imbalance_ratio:.1%}")
    verdict = "abnormal" if reasons else ("healthy" if gross > 0 else "empty")
    return {
        "buy_count": int(bucket.get("buy_count") or 0),
        "buy_notional": buy,
        "sell_count": int(bucket.get("sell_count") or 0),
        "sell_notional": sell,
        "imbalance_ratio": imbalance_ratio,
        "buckets": buckets,
        "healthy_gross_notional": healthy_gross,
        "protective_gross_notional": protective_gross,
        "unknown_gross_notional": unknown_gross,
        "protective_share": protective_share,
        "unknown_share": unknown_share,
        "verdict": verdict,
        "reasons": reasons,
    }


def summarize_hourly_metrics(
    trades: list[dict[str, Any]],
    income_rows: list[dict[str, Any]],
    candles: list[Any],
    *,
    commission_converter: Any | None = None,
    order_role_lookup: dict[str, dict[str, Any]] | None = None,
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
                "buy_sell_imbalance_ratio": 0.0,
                "quality_buckets": _empty_trade_quality_buckets(),
                "trade_quality": None,
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
        role = _trade_role_from_lookup(item, order_role_lookup)
        bucket_key = _trade_quality_bucket_for_role(role)
        quality_bucket = bucket["quality_buckets"].setdefault(bucket_key, _empty_trade_quality_bucket())
        quality_bucket["count"] += 1
        quality_bucket["gross_notional"] += trade_notional
        quality_bucket["realized_pnl"] += trade_realized
        quality_bucket["commission"] += trade_commission

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
        bucket["loss_per_10k_abs"] = (
            max(-float(bucket["net_after_fees_and_funding"]), 0.0) / gross_notional * 10000.0
            if gross_notional > 0
            else 0.0
        )
        bucket["buy_sell_imbalance_ratio"] = (
            abs(float(bucket["buy_notional"]) - float(bucket["sell_notional"])) / gross_notional
            if gross_notional > 0
            else 0.0
        )
        bucket["trade_quality"] = _finalize_trade_quality(bucket)
        rows.append(bucket)

    limited_rows = rows[: max(int(limit), 1)]
    return {
        "rows": limited_rows,
        "row_count": len(limited_rows),
        "available_hours": len(rows),
    }


def build_hourly_diagnostics(
    events: list[dict[str, Any]],
    hourly_summary: dict[str, Any] | None,
    *,
    limit: int = 24,
) -> dict[str, Any]:
    hourly_rows = list((hourly_summary or {}).get("rows") or [])
    rows_by_hour = {str(row.get("hour_start") or ""): dict(row) for row in hourly_rows}
    event_buckets: dict[datetime, dict[str, Any]] = {}

    for item in events:
        ts = _parse_iso_ts(item.get("ts"))
        if ts is None:
            continue
        hour_start = _floor_hour(ts)
        bucket = event_buckets.setdefault(
            hour_start,
            {
                "count": 0,
                "effective_step_sum": 0.0,
                "effective_step_count": 0,
                "min_effective_step": None,
                "max_effective_step": None,
                "base_step_price": None,
                "adaptive_scale_sum": 0.0,
                "adaptive_scale_count": 0,
                "max_rolling_hourly_loss": 0.0,
                "buy_pause_count": 0,
                "volatility_entry_pause_count": 0,
                "adverse_reduce_count": 0,
            },
        )
        bucket["count"] += 1
        adaptive_step = item.get("adaptive_step") if isinstance(item.get("adaptive_step"), dict) else {}
        effective_step = _safe_float(
            adaptive_step.get("effective_step_price")
            or item.get("adaptive_step_effective_step_price")
        )
        if effective_step > 0:
            bucket["effective_step_sum"] += effective_step
            bucket["effective_step_count"] += 1
            current_min = bucket["min_effective_step"]
            current_max = bucket["max_effective_step"]
            bucket["min_effective_step"] = (
                effective_step if current_min is None else min(float(current_min), effective_step)
            )
            bucket["max_effective_step"] = (
                effective_step if current_max is None else max(float(current_max), effective_step)
            )
        base_step = _safe_float(adaptive_step.get("base_step_price") or item.get("adaptive_step_base_step_price"))
        if base_step > 0:
            bucket["base_step_price"] = base_step
        scale = _safe_float(adaptive_step.get("scale") or item.get("adaptive_step_scale"))
        if scale > 0:
            bucket["adaptive_scale_sum"] += scale
            bucket["adaptive_scale_count"] += 1
        bucket["max_rolling_hourly_loss"] = max(
            float(bucket["max_rolling_hourly_loss"]),
            _safe_float(item.get("rolling_hourly_loss")),
        )
        if item.get("buy_paused"):
            bucket["buy_pause_count"] += 1
        volatility_entry_pause = item.get("volatility_entry_pause")
        if (
            item.get("volatility_entry_pause_active")
            or (isinstance(volatility_entry_pause, dict) and volatility_entry_pause.get("active"))
        ):
            bucket["volatility_entry_pause_count"] += 1
        adverse_reduce = item.get("adverse_inventory_reduce")
        if item.get("adverse_reduce_active") or (isinstance(adverse_reduce, dict) and adverse_reduce.get("active")):
            bucket["adverse_reduce_count"] += 1

    for hour_start, bucket in event_buckets.items():
        key = hour_start.isoformat()
        row = rows_by_hour.setdefault(key, {"hour_start": key})
        step_count = int(bucket["effective_step_count"])
        scale_count = int(bucket["adaptive_scale_count"])
        row["event_count"] = int(bucket["count"])
        row["effective_step_price"] = (
            float(bucket["effective_step_sum"]) / step_count if step_count > 0 else 0.0
        )
        row["min_effective_step_price"] = bucket["min_effective_step"] or 0.0
        row["max_effective_step_price"] = bucket["max_effective_step"] or 0.0
        row["base_step_price"] = bucket["base_step_price"] or 0.0
        row["adaptive_scale"] = float(bucket["adaptive_scale_sum"]) / scale_count if scale_count > 0 else 0.0
        row["max_rolling_hourly_loss"] = float(bucket["max_rolling_hourly_loss"])
        row["buy_pause_count"] = int(bucket["buy_pause_count"])
        row["volatility_entry_pause_count"] = int(bucket["volatility_entry_pause_count"])
        row["adverse_reduce_count"] = int(bucket["adverse_reduce_count"])

    rows = []
    for hour_key in sorted(rows_by_hour.keys(), reverse=True):
        row = rows_by_hour[hour_key]
        gross_notional = _safe_float(row.get("gross_notional"))
        net_after_fees = _safe_float(row.get("net_after_fees_and_funding"))
        row["loss_per_10k_abs"] = (
            max(-net_after_fees, 0.0) / gross_notional * 10000.0
            if gross_notional > 0
            else 0.0
        )
        row["return_ratio_abs"] = abs(_safe_float(row.get("return_ratio")))
        rows.append(row)

    limited_rows = rows[: max(int(limit), 1)]
    return {
        "rows": limited_rows,
        "row_count": len(limited_rows),
        "available_hours": len(rows),
    }


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _string_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, tuple):
        return [str(item).strip() for item in value if str(item).strip()]
    text = str(value or "").strip()
    return [text] if text else []


def _execution_regime_from_sources(
    *,
    plan_report: dict[str, Any] | None,
    latest_loop: dict[str, Any],
    runner_config: dict[str, Any],
) -> dict[str, Any]:
    plan_regime = _as_dict((plan_report or {}).get("execution_regime"))
    loop_regime = _as_dict(latest_loop.get("execution_regime"))
    source = plan_regime if plan_regime else loop_regime
    params = dict(_as_dict(source.get("params")))
    if not params:
        for row_key, param_key in (
            ("execution_regime_quote_offset_bps", "quote_offset_bps"),
            ("execution_regime_refresh_interval_ms", "refresh_interval_ms"),
            ("execution_regime_order_size_pct", "order_size_pct"),
        ):
            if latest_loop.get(row_key) is not None:
                params[param_key] = latest_loop.get(row_key)
    enabled = bool(
        source.get("enabled")
        if "enabled" in source
        else latest_loop.get("execution_regime_enabled") or runner_config.get("execution_regime_enabled")
    )
    state = str(source.get("state") or latest_loop.get("execution_regime_state") or "").strip()
    raw_state = str(source.get("raw_state") or latest_loop.get("execution_regime_raw_state") or "").strip()
    if not state:
        state = "DISABLED" if not enabled else "UNKNOWN"
    if not raw_state:
        raw_state = state
    config = {
        key: value
        for key, value in (runner_config or {}).items()
        if str(key).startswith("execution_regime_")
    }
    return {
        "enabled": enabled,
        "mode": str(source.get("mode") or "shadow").strip() or "shadow",
        "state": state,
        "raw_state": raw_state,
        "risk_score": _safe_float(source.get("risk_score") if source else latest_loop.get("execution_regime_risk_score")),
        "hard_risk": bool(source.get("hard_risk") if source else latest_loop.get("execution_regime_hard_risk")),
        "reason_codes": _string_list(source.get("reason_codes") if source else latest_loop.get("execution_regime_reason_codes")),
        "missing_features": _string_list(source.get("missing_features")),
        "params": params,
        "features": dict(_as_dict(source.get("features"))),
        "memory": dict(_as_dict(source.get("memory"))),
        "applied": bool(source.get("applied")),
        "config": config,
    }


def _execution_regime_from_event(item: dict[str, Any]) -> dict[str, Any]:
    source = dict(_as_dict(item.get("execution_regime")))
    params = dict(_as_dict(source.get("params")))
    if not params:
        for row_key, param_key in (
            ("execution_regime_quote_offset_bps", "quote_offset_bps"),
            ("execution_regime_refresh_interval_ms", "refresh_interval_ms"),
            ("execution_regime_order_size_pct", "order_size_pct"),
        ):
            if item.get(row_key) is not None:
                params[param_key] = item.get(row_key)
    enabled = bool(
        source.get("enabled")
        if "enabled" in source
        else item.get("execution_regime_enabled")
    )
    state = str(source.get("state") or item.get("execution_regime_state") or "").strip()
    raw_state = str(source.get("raw_state") or item.get("execution_regime_raw_state") or "").strip()
    if not state:
        state = "DISABLED" if not enabled else "UNKNOWN"
    if not raw_state:
        raw_state = state
    return {
        "enabled": enabled,
        "mode": str(source.get("mode") or "shadow").strip() or "shadow",
        "state": state,
        "raw_state": raw_state,
        "risk_score": _safe_float(source.get("risk_score") if source else item.get("execution_regime_risk_score")),
        "hard_risk": bool(source.get("hard_risk") if source else item.get("execution_regime_hard_risk")),
        "reason_codes": _string_list(source.get("reason_codes") if source else item.get("execution_regime_reason_codes")),
        "missing_features": _string_list(source.get("missing_features")),
        "params": params,
        "features": dict(_as_dict(source.get("features"))),
        "memory": dict(_as_dict(source.get("memory"))),
        "applied": bool(source.get("applied")),
    }


def _adaptive_step_from_event(item: dict[str, Any]) -> dict[str, Any]:
    adaptive_step = _as_dict(item.get("adaptive_step"))
    return {
        "enabled": bool(adaptive_step.get("enabled") if adaptive_step else item.get("adaptive_step_enabled")),
        "active": bool(adaptive_step.get("active") if adaptive_step else item.get("adaptive_step_active")),
        "controls_active": bool(
            adaptive_step.get("controls_active") if adaptive_step else item.get("adaptive_step_controls_active")
        ),
        "base_step_price": _safe_float(
            adaptive_step.get("base_step_price") if adaptive_step else item.get("adaptive_step_base_step_price")
        ),
        "effective_step_price": _safe_float(
            adaptive_step.get("effective_step_price") if adaptive_step else item.get("adaptive_step_effective_step_price")
        ),
        "scale": _safe_float(adaptive_step.get("scale") if adaptive_step else item.get("adaptive_step_scale")),
        "raw_scale": _safe_float(adaptive_step.get("raw_scale") if adaptive_step else item.get("adaptive_step_raw_scale")),
        "per_order_scale": _safe_float(
            adaptive_step.get("per_order_scale") if adaptive_step else item.get("adaptive_step_per_order_scale")
        ),
        "position_limit_scale": _safe_float(
            adaptive_step.get("position_limit_scale")
            if adaptive_step
            else item.get("adaptive_step_position_limit_scale")
        ),
        "dominant_window": adaptive_step.get("dominant_window") or item.get("adaptive_step_dominant_window"),
        "dominant_metric": adaptive_step.get("dominant_metric") or item.get("adaptive_step_dominant_metric"),
        "dominant_value": _safe_float(
            adaptive_step.get("dominant_value") if adaptive_step else item.get("adaptive_step_dominant_value")
        ),
        "dominant_threshold": _safe_float(
            adaptive_step.get("dominant_threshold")
            if adaptive_step
            else item.get("adaptive_step_dominant_threshold")
        ),
        "reason": adaptive_step.get("reason") or item.get("adaptive_step_reason"),
    }


def _timeline_context(item: dict[str, Any]) -> dict[str, Any]:
    adaptive = _adaptive_step_from_event(item)
    regime = _execution_regime_from_event(item)
    return {
        "market": {
            "mid_price": _safe_float(item.get("mid_price")),
            "return_ratio": _safe_float(item.get("market_guard_return_ratio")),
            "amplitude_ratio": _safe_float(item.get("market_guard_amplitude_ratio")),
            "spread_bps": _safe_float(regime.get("features", {}).get("spread_bps")),
        },
        "position": {
            "current_long_qty": _safe_float(item.get("current_long_qty")),
            "current_short_qty": _safe_float(item.get("current_short_qty")),
            "current_long_notional": _safe_float(item.get("current_long_notional")),
            "current_short_notional": _safe_float(item.get("current_short_notional")),
            "actual_net_notional": _safe_float(item.get("actual_net_notional")),
            "synthetic_net_notional": _safe_float(item.get("synthetic_net_notional")),
        },
        "thresholds": {
            "pause_buy_position_notional": _safe_float(item.get("pause_buy_position_notional")),
            "pause_short_position_notional": _safe_float(item.get("pause_short_position_notional")),
            "max_position_notional": _safe_float(item.get("max_position_notional")),
            "max_short_position_notional": _safe_float(item.get("max_short_position_notional")),
            "rolling_hourly_loss": _safe_float(item.get("rolling_hourly_loss")),
            "rolling_hourly_loss_limit": _safe_float(item.get("rolling_hourly_loss_limit")),
            "cumulative_gross_notional": _safe_float(item.get("cumulative_gross_notional")),
            "max_cumulative_notional": _safe_float(item.get("max_cumulative_notional")),
        },
        "step": adaptive,
        "execution_regime": regime,
    }


def _push_timeline_row(
    rows: list[dict[str, Any]],
    item: dict[str, Any],
    *,
    row_type: str,
    label: str,
    state: str,
    severity: str,
    detail: str,
    reason_codes: list[str] | None = None,
) -> None:
    rows.append(
        {
            "ts": item.get("ts"),
            "cycle": item.get("cycle"),
            "type": row_type,
            "label": label,
            "state": state,
            "severity": severity,
            "detail": detail,
            "reason_codes": reason_codes or [],
            **_timeline_context(item),
        }
    )


def build_risk_activation_timeline(events: list[dict[str, Any]], *, limit: int = 120) -> list[dict[str, Any]]:
    """Build a compact timeline for parameter activations and risk-state changes."""
    rows: list[dict[str, Any]] = []
    last_signatures: dict[str, Any] = {}
    cycle_rows = [item for item in events if isinstance(item, dict) and "cycle" in item]

    for item in cycle_rows:
        regime = _execution_regime_from_event(item)
        if regime["enabled"]:
            state = str(regime.get("state") or "UNKNOWN").upper()
            raw_state = str(regime.get("raw_state") or state).upper()
            signature = (
                state,
                raw_state,
                bool(regime.get("hard_risk")),
                tuple(regime.get("reason_codes") or []),
            )
            if signature != last_signatures.get("execution_regime") or state in {"CAUTION", "EXIT"}:
                params = regime.get("params") or {}
                severity = "critical" if state == "EXIT" or regime.get("hard_risk") else ("warning" if state == "CAUTION" else "info")
                _push_timeline_row(
                    rows,
                    item,
                    row_type="execution_regime",
                    label="执行区间判断",
                    state=state,
                    severity=severity,
                    detail=(
                        f"score={_safe_float(regime.get('risk_score')):.3f} "
                        f"quote={_safe_float(params.get('quote_offset_bps')):.2f}bps "
                        f"refresh={int(_safe_float(params.get('refresh_interval_ms')))}ms "
                        f"size={_safe_float(params.get('order_size_pct')):.2f}% "
                        f"mode={regime.get('mode') or 'shadow'}"
                    ),
                    reason_codes=list(regime.get("reason_codes") or []),
                )
            last_signatures["execution_regime"] = signature

        adaptive = _adaptive_step_from_event(item)
        adaptive_signature = (
            bool(adaptive.get("controls_active")),
            round(_safe_float(adaptive.get("effective_step_price")), 10),
            round(_safe_float(adaptive.get("scale")), 4),
            round(_safe_float(adaptive.get("per_order_scale")), 4),
            round(_safe_float(adaptive.get("position_limit_scale")), 4),
        )
        previous_adaptive = last_signatures.get("adaptive_step")
        if adaptive_signature != previous_adaptive and (
            adaptive.get("controls_active")
            or (previous_adaptive and previous_adaptive[0])
        ):
            active = bool(adaptive.get("controls_active"))
            _push_timeline_row(
                rows,
                item,
                row_type="adaptive_step",
                label="adaptive step 缩放",
                state="ACTIVE" if active else "BASE",
                severity="warning" if active else "info",
                detail=(
                    f"base={_safe_float(adaptive.get('base_step_price')):.10f} "
                    f"effective={_safe_float(adaptive.get('effective_step_price')):.10f} "
                    f"scale={_safe_float(adaptive.get('scale')):.2f} "
                    f"per_order={_safe_float(adaptive.get('per_order_scale')):.2f} "
                    f"position_limit={_safe_float(adaptive.get('position_limit_scale')):.2f} "
                    f"reason={adaptive.get('reason') or '--'}"
                ),
            )
        last_signatures["adaptive_step"] = adaptive_signature

        pause_signature = (
            bool(item.get("buy_paused")),
            tuple(_string_list(item.get("pause_reasons"))),
            bool(item.get("short_paused")),
            tuple(_string_list(item.get("short_pause_reasons"))),
        )
        previous_pause = last_signatures.get("soft_threshold")
        if pause_signature != previous_pause and (any(pause_signature) or (previous_pause and any(previous_pause))):
            parts = []
            if item.get("buy_paused"):
                parts.append("停买")
            if item.get("short_paused"):
                parts.append("停空")
            _push_timeline_row(
                rows,
                item,
                row_type="soft_threshold",
                label="软阈值生效",
                state="/".join(parts) or "RECOVERED",
                severity="warning" if parts else "info",
                detail="；".join(_string_list(item.get("pause_reasons")) + _string_list(item.get("short_pause_reasons"))) or "软阈值恢复",
            )
        last_signatures["soft_threshold"] = pause_signature

        hard_signature = (bool(item.get("buy_cap_applied")), bool(item.get("short_cap_applied")))
        previous_hard = last_signatures.get("hard_threshold")
        if hard_signature != previous_hard and (any(hard_signature) or (previous_hard and any(previous_hard))):
            parts = []
            if item.get("buy_cap_applied"):
                parts.append("多仓硬裁单")
            if item.get("short_cap_applied"):
                parts.append("空仓硬裁单")
            _push_timeline_row(
                rows,
                item,
                row_type="hard_threshold",
                label="硬阈值裁剪",
                state="/".join(parts) or "RECOVERED",
                severity="warning" if parts else "info",
                detail="计划订单超过硬上限，本轮已裁剪超出部分。" if parts else "硬阈值裁剪解除，本轮计划未再被硬上限裁剪。",
            )
        last_signatures["hard_threshold"] = hard_signature

        market_guard_signature = (bool(item.get("volatility_buy_pause")), bool(item.get("shift_frozen")))
        previous_market_guard = last_signatures.get("market_guard")
        if market_guard_signature != previous_market_guard and (
            any(market_guard_signature) or (previous_market_guard and any(previous_market_guard))
        ):
            parts = []
            if item.get("volatility_buy_pause"):
                parts.append("分钟停买")
            if item.get("shift_frozen"):
                parts.append("冻结迁移")
            _push_timeline_row(
                rows,
                item,
                row_type="market_guard",
                label="分钟行情保护",
                state="/".join(parts) or "RECOVERED",
                severity="info",
                detail=(
                    f"return={_safe_float(item.get('market_guard_return_ratio')):.4%} "
                    f"amplitude={_safe_float(item.get('market_guard_amplitude_ratio')):.4%}"
                ),
            )
        last_signatures["market_guard"] = market_guard_signature

        reduce_signature = (
            bool(item.get("threshold_reduce_active")),
            bool(item.get("adverse_reduce_active")),
            bool(item.get("hard_loss_forced_reduce_active")),
        )
        previous_reduce = last_signatures.get("reduce_guard")
        if reduce_signature != previous_reduce and (any(reduce_signature) or (previous_reduce and any(previous_reduce))):
            parts = []
            if item.get("threshold_reduce_active"):
                parts.append("阈值减仓")
            if item.get("adverse_reduce_active"):
                parts.append("逆向减仓")
            if item.get("hard_loss_forced_reduce_active"):
                parts.append("硬损强减")
            _push_timeline_row(
                rows,
                item,
                row_type="reduce_guard",
                label="减仓保护",
                state="/".join(parts) or "RECOVERED",
                severity="critical" if item.get("hard_loss_forced_reduce_active") else ("warning" if parts else "info"),
                detail=str(
                    item.get("hard_loss_forced_reduce_reason")
                    or item.get("adverse_reduce_direction")
                    or item.get("threshold_reduce_direction")
                    or ("减仓保护已激活" if parts else "减仓保护解除")
                ),
            )
        last_signatures["reduce_guard"] = reduce_signature

        runtime_signature = (
            str(item.get("runtime_status") or "running"),
            bool(item.get("stop_triggered")),
            str(item.get("stop_reason") or ""),
        )
        if runtime_signature != last_signatures.get("runtime_guard") and (
            item.get("stop_triggered") or str(item.get("runtime_status") or "running") != "running"
        ):
            _push_timeline_row(
                rows,
                item,
                row_type="runtime_guard",
                label="运行停止保护",
                state=str(item.get("runtime_status") or "stopped"),
                severity="critical",
                detail=str(item.get("stop_reason") or "runtime guard stopped the runner"),
                reason_codes=_string_list(item.get("stop_reasons")),
            )
        last_signatures["runtime_guard"] = runtime_signature

    return rows[-max(int(limit), 1):]


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

    if risk_controls.get("execution_regime_enabled"):
        regime_state = str(risk_controls.get("execution_regime_state") or "").upper().strip()
        regime_score = _safe_float(risk_controls.get("execution_regime_risk_score"))
        regime_reasons = [
            str(item).strip()
            for item in (risk_controls.get("execution_regime_reason_codes") or [])
            if str(item).strip()
        ]
        regime_detail = f"state={regime_state or '--'} score={regime_score:.3f}"
        if regime_reasons:
            regime_detail = f"{regime_detail} reason={', '.join(regime_reasons[:5])}"
        if risk_controls.get("execution_regime_hard_risk") or regime_state == "EXIT":
            _push_alert(
                alerts,
                seen_codes,
                code="execution_regime_exit",
                severity="critical",
                title="执行区间已进入退出/硬风险状态",
                detail=regime_detail,
                action="这是影子判断层；当前不会直接改下单，但应检查同一时间线里的市场振幅、价差、仓位和硬阈值。",
            )
        elif regime_state == "CAUTION":
            _push_alert(
                alerts,
                seen_codes,
                code="execution_regime_caution",
                severity="warning",
                title="执行区间进入谨慎状态",
                detail=regime_detail,
                action="建议观察时间线中 quote offset、刷新间隔和 adaptive step 是否同步放大，再决定是否收紧真实参数。",
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


def _summarize_frozen_position_qty(open_orders: list[dict[str, Any]]) -> dict[str, float]:
    frozen_long_qty = 0.0
    frozen_short_qty = 0.0
    for order in open_orders:
        if not isinstance(order, dict):
            continue
        side = str(order.get("side", "")).upper().strip()
        position_side = str(order.get("position_side", order.get("positionSide", "")) or "").upper().strip() or "BOTH"
        remaining_qty = max(
            _safe_float(order.get("orig_qty", order.get("origQty")))
            - _safe_float(order.get("executed_qty", order.get("executedQty"))),
            0.0,
        )
        if remaining_qty <= 0:
            continue
        reduce_only = _truthy(order.get("reduce_only", order.get("reduceOnly")))
        if position_side == "LONG" and side == "SELL":
            frozen_long_qty += remaining_qty
        elif position_side == "SHORT" and side == "BUY":
            frozen_short_qty += remaining_qty
        elif reduce_only and position_side == "BOTH" and side == "SELL":
            frozen_long_qty += remaining_qty
        elif reduce_only and position_side == "BOTH" and side == "BUY":
            frozen_short_qty += remaining_qty
    return {"frozen_long_qty": frozen_long_qty, "frozen_short_qty": frozen_short_qty}


def _derive_inventory_frozen_position_qty(
    plan_report: dict[str, Any],
    *,
    active_long_qty: float,
    active_short_qty: float,
) -> dict[str, float]:
    frozen_long_qty = 0.0
    frozen_short_qty = 0.0

    raw_inventory = plan_report.get("frozen_inventory")
    if isinstance(raw_inventory, dict):
        frozen_long_qty = max(
            _safe_float(
                raw_inventory.get("long_qty")
                or raw_inventory.get("frozen_long_qty")
                or raw_inventory.get("long")
            ),
            0.0,
        )
        frozen_short_qty = max(
            _safe_float(
                raw_inventory.get("short_qty")
                or raw_inventory.get("frozen_short_qty")
                or raw_inventory.get("short")
            ),
            0.0,
        )
        if frozen_long_qty > 0 or frozen_short_qty > 0:
            return {"frozen_long_qty": frozen_long_qty, "frozen_short_qty": frozen_short_qty}

    actual_net_qty = _safe_float(plan_report.get("actual_net_qty"))
    strategy_net_qty = _safe_float(plan_report.get("strategy_actual_net_qty"))
    if abs(strategy_net_qty) <= 1e-12:
        strategy_net_qty = active_long_qty - active_short_qty
    frozen_net_qty = actual_net_qty - strategy_net_qty
    if frozen_net_qty > 1e-12:
        frozen_long_qty = frozen_net_qty
    elif frozen_net_qty < -1e-12:
        frozen_short_qty = abs(frozen_net_qty)
    return {"frozen_long_qty": frozen_long_qty, "frozen_short_qty": frozen_short_qty}


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
    active_long_qty = max(_safe_float(plan_report.get("current_long_qty")), 0.0)
    active_short_qty = max(_safe_float(plan_report.get("current_short_qty")), 0.0)
    long_qty = active_long_qty
    short_qty = active_short_qty
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
    reduce_order_frozen_position = _summarize_frozen_position_qty(open_orders)
    inventory_frozen_position = (
        _derive_inventory_frozen_position_qty(
            plan_report,
            active_long_qty=active_long_qty,
            active_short_qty=active_short_qty,
        )
        if dual_side_position
        else {"frozen_long_qty": 0.0, "frozen_short_qty": 0.0}
    )
    if inventory_frozen_position["frozen_long_qty"] > 0 or inventory_frozen_position["frozen_short_qty"] > 0:
        long_qty = active_long_qty + inventory_frozen_position["frozen_long_qty"]
        short_qty = active_short_qty + inventory_frozen_position["frozen_short_qty"]
    frozen_position = {
        "frozen_long_qty": reduce_order_frozen_position["frozen_long_qty"]
        + inventory_frozen_position["frozen_long_qty"],
        "frozen_short_qty": reduce_order_frozen_position["frozen_short_qty"]
        + inventory_frozen_position["frozen_short_qty"],
    }

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
            "active_long_qty": active_long_qty,
            "active_short_qty": active_short_qty,
            "entry_price": entry_price,
            "break_even_price": break_even_price,
            "long_entry_price": long_avg_price,
            "short_entry_price": short_avg_price,
            "long_break_even_price": long_avg_price,
            "short_break_even_price": short_avg_price,
            "long_unrealized_pnl": 0.0,
            "short_unrealized_pnl": 0.0,
            "frozen_long_qty": frozen_position["frozen_long_qty"],
            "frozen_short_qty": frozen_position["frozen_short_qty"],
            "inventory_frozen_long_qty": inventory_frozen_position["frozen_long_qty"],
            "inventory_frozen_short_qty": inventory_frozen_position["frozen_short_qty"],
            "reduce_order_frozen_long_qty": reduce_order_frozen_position["frozen_long_qty"],
            "reduce_order_frozen_short_qty": reduce_order_frozen_position["frozen_short_qty"],
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


def _infer_state_path_from_events_path(events_path: str | Path) -> Path | None:
    path = Path(events_path)
    name = path.name
    if name.endswith("_loop_events.jsonl"):
        return path.with_name(f"{name.removesuffix('_loop_events.jsonl')}_loop_state.json")
    if name == "night_loop_events.jsonl":
        return path.with_name("night_loop_state.json")
    return None


def _build_order_role_lookup(
    *,
    state_path: Path | None,
    plan_report: dict[str, Any] | None,
    submit_report: dict[str, Any] | None,
) -> dict[str, dict[str, Any]]:
    lookup: dict[str, dict[str, Any]] = {}
    state = _read_json(state_path) if state_path is not None else None
    if isinstance(state, dict):
        for refs_key in ("synthetic_order_refs", "inventory_grid_order_refs"):
            refs = state.get(refs_key)
            if not isinstance(refs, dict):
                continue
            for order_id, ref in refs.items():
                if isinstance(ref, dict) and str(order_id).strip():
                    lookup[str(order_id).strip()] = dict(ref)

    for container in (plan_report if isinstance(plan_report, dict) else {}, submit_report if isinstance(submit_report, dict) else {}):
        for key in ("kept_orders", "placed_orders", "open_orders"):
            rows = container.get(key)
            if not isinstance(rows, list):
                continue
            for item in rows:
                if not isinstance(item, dict):
                    continue
                request = item.get("request") if isinstance(item.get("request"), dict) else {}
                response = item.get("response") if isinstance(item.get("response"), dict) else item
                order_id = response.get("orderId") or response.get("order_id") or item.get("orderId") or item.get("order_id")
                if order_id in {"", None}:
                    continue
                role = (
                    request.get("role")
                    or response.get("role")
                    or item.get("role")
                    or _trade_role_from_client_order_id(
                        response.get("clientOrderId")
                        or response.get("client_order_id")
                        or request.get("client_order_id")
                        or item.get("client_order_id")
                    )
                )
                lookup[str(order_id).strip()] = {
                    "role": str(role or "").strip(),
                    "side": str(request.get("side") or response.get("side") or item.get("side") or "").upper().strip(),
                    "position_side": str(
                        request.get("position_side")
                        or response.get("positionSide")
                        or item.get("position_side")
                        or "BOTH"
                    ).upper().strip()
                    or "BOTH",
                    "client_order_id": str(
                        response.get("clientOrderId")
                        or response.get("client_order_id")
                        or request.get("client_order_id")
                        or item.get("client_order_id")
                        or ""
                    ).strip(),
                }
    return lookup


def _build_live_account_snapshot(
    *,
    symbol: str,
    api_key: str,
    api_secret: str,
) -> dict[str, Any]:
    normalized_symbol = str(symbol).upper().strip()
    cache_key = (api_key.strip(), normalized_symbol)
    now = time.time()
    ttl = max(float(LIVE_ACCOUNT_SNAPSHOT_TTL_SECONDS), 0.0)
    with _MONITOR_CACHE_LOCK:
        cached = _LIVE_ACCOUNT_CACHE.get(cache_key)
        if cached is not None and (now - cached[0]) <= ttl:
            snapshot = dict(cached[1])
            snapshot["cache_hit"] = True
            return snapshot

    account_info = fetch_futures_account_info_v3(api_key, api_secret)
    position_mode = fetch_futures_position_mode(api_key, api_secret)
    open_orders = fetch_futures_open_orders(normalized_symbol, api_key, api_secret)
    dual_side_position = _truthy(position_mode.get("dualSidePosition"))
    if dual_side_position:
        long_position = extract_symbol_position(account_info, normalized_symbol, "LONG")
        short_position = extract_symbol_position(account_info, normalized_symbol, "SHORT")
        long_qty = abs(_safe_float(long_position.get("positionAmt")))
        short_qty = abs(_safe_float(short_position.get("positionAmt")))
        long_entry_price = _safe_float(long_position.get("entryPrice"))
        short_entry_price = _safe_float(short_position.get("entryPrice"))
        long_break_even_price = _safe_float(long_position.get("breakEvenPrice"))
        short_break_even_price = _safe_float(short_position.get("breakEvenPrice"))
        long_unrealized = _safe_float(long_position.get("unRealizedProfit", long_position.get("unrealizedProfit")))
        short_unrealized = _safe_float(short_position.get("unRealizedProfit", short_position.get("unrealizedProfit")))
        unrealized_pnl = long_unrealized + short_unrealized
        position_amt = long_qty - short_qty
        position = long_position if long_qty >= short_qty else short_position
        entry_price = long_entry_price if long_qty >= short_qty else short_entry_price
        break_even_price = long_break_even_price if long_qty >= short_qty else short_break_even_price
    else:
        position = extract_symbol_position(account_info, normalized_symbol)
        position_amt = _safe_float(position.get("positionAmt"))
        long_qty = max(position_amt, 0.0)
        short_qty = max(-position_amt, 0.0)
        unrealized_pnl = _safe_float(position.get("unRealizedProfit", position.get("unrealizedProfit")))
        entry_price = _safe_float(position.get("entryPrice"))
        break_even_price = _safe_float(position.get("breakEvenPrice"))
        long_entry_price = entry_price if position_amt > 0 else 0.0
        short_entry_price = entry_price if position_amt < 0 else 0.0
        long_break_even_price = break_even_price if position_amt > 0 else 0.0
        short_break_even_price = break_even_price if position_amt < 0 else 0.0
        long_unrealized = unrealized_pnl if position_amt > 0 else 0.0
        short_unrealized = unrealized_pnl if position_amt < 0 else 0.0

    normalized_open_orders = [
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
    frozen_position = _summarize_frozen_position_qty(normalized_open_orders)

    snapshot = {
        "source": "binance_futures_account",
        "cache_hit": False,
        "ttl_seconds": ttl,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "position": {
            "source": "binance_futures_account",
            "position_amt": position_amt,
            "long_qty": long_qty,
            "short_qty": short_qty,
            "entry_price": entry_price,
            "break_even_price": break_even_price,
            "long_entry_price": long_entry_price,
            "short_entry_price": short_entry_price,
            "long_break_even_price": long_break_even_price,
            "short_break_even_price": short_break_even_price,
            "long_unrealized_pnl": long_unrealized,
            "short_unrealized_pnl": short_unrealized,
            "frozen_long_qty": frozen_position["frozen_long_qty"],
            "frozen_short_qty": frozen_position["frozen_short_qty"],
            "unrealized_pnl": unrealized_pnl,
            "isolated": _truthy(position.get("isolated")),
            "leverage": int(float(position.get("leverage", 0) or 0)) if str(position.get("leverage", "")).strip() else None,
            "one_way_mode": not dual_side_position,
            "available_balance": _safe_float(account_info.get("availableBalance")),
            "wallet_balance": _safe_float(account_info.get("totalWalletBalance")),
            "multi_assets_margin": _truthy(account_info.get("multiAssetsMargin")),
        },
        "account_assets": {
            "USDT": _extract_futures_asset_snapshot(account_info, "USDT"),
            "BNB": _extract_futures_asset_snapshot(account_info, "BNB"),
        },
        "open_orders": normalized_open_orders,
    }
    with _MONITOR_CACHE_LOCK:
        _LIVE_ACCOUNT_CACHE[cache_key] = (time.time(), snapshot)
    return snapshot


def _merge_live_account_position_with_local_runtime(
    local_position: dict[str, Any],
    live_position: dict[str, Any],
) -> dict[str, Any]:
    active_long_qty = max(
        _safe_float(local_position.get("active_long_qty", local_position.get("long_qty"))),
        0.0,
    )
    active_short_qty = max(
        _safe_float(local_position.get("active_short_qty", local_position.get("short_qty"))),
        0.0,
    )
    live_long_qty = max(_safe_float(live_position.get("long_qty")), 0.0)
    live_short_qty = max(_safe_float(live_position.get("short_qty")), 0.0)
    inventory_frozen_long_qty = max(live_long_qty - active_long_qty, 0.0)
    inventory_frozen_short_qty = max(live_short_qty - active_short_qty, 0.0)
    reduce_order_frozen_long_qty = max(_safe_float(live_position.get("frozen_long_qty")), 0.0)
    reduce_order_frozen_short_qty = max(_safe_float(live_position.get("frozen_short_qty")), 0.0)

    merged = dict(live_position)
    merged.update(
        {
            "source": "local_runtime_with_live_account_position",
            "active_long_qty": active_long_qty,
            "active_short_qty": active_short_qty,
            "inventory_frozen_long_qty": inventory_frozen_long_qty,
            "inventory_frozen_short_qty": inventory_frozen_short_qty,
            "reduce_order_frozen_long_qty": reduce_order_frozen_long_qty,
            "reduce_order_frozen_short_qty": reduce_order_frozen_short_qty,
            "frozen_long_qty": inventory_frozen_long_qty + reduce_order_frozen_long_qty,
            "frozen_short_qty": inventory_frozen_short_qty + reduce_order_frozen_short_qty,
        }
    )
    return merged


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


def _monitor_audit_count_max_scan_bytes() -> int:
    raw = str(os.environ.get("GRID_MONITOR_AUDIT_COUNT_MAX_SCAN_BYTES") or "50000000").strip()
    try:
        value = int(raw)
    except ValueError:
        return 50_000_000
    return max(0, value)


def _count_monitor_audit_lines(path: Path) -> int | None:
    return count_jsonl_lines(path, max_scan_bytes=_monitor_audit_count_max_scan_bytes())


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
    order_role_lookup = _build_order_role_lookup(
        state_path=_infer_state_path_from_events_path(event_path),
        plan_report=plan_report if isinstance(plan_report, dict) else {},
        submit_report=submit_report if isinstance(submit_report, dict) else {},
    )
    session_start, session_last = _build_session_window(events, submit_report)
    session_start = _min_dt(session_start, event_start)
    audit_trade_start, _audit_trade_last = _jsonl_time_bounds(audit_paths["trade_audit"], trade_row_time_ms)
    audit_income_start, _audit_income_last = _jsonl_time_bounds(audit_paths["income_audit"], income_row_time_ms)
    stats_start, stats_anchor = _resolve_monitor_stats_anchor(
        anchor_path=_monitor_anchor_path(audit_paths),
        symbol=normalized_symbol,
        candidates=[
            ("competition_start", competition_start),
            ("audit_trade_start", audit_trade_start),
            ("audit_income_start", audit_income_start),
            ("session_start", session_start),
        ],
    )
    stats_start = stats_start or competition_start or session_start
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
        "competition_displacement_volume": None,
        "competition_entry_volume_targets": None,
        "runner": runner,
        "audit": {
            "paths": {key: str(path) for key, path in audit_paths.items()},
            "trade_source": None,
            "income_source": None,
            "stats_anchor": stats_anchor,
            "trade_row_count": 0,
            "income_row_count": 0,
            "order_event_count": _count_monitor_audit_lines(audit_paths["order_audit"]),
            "submit_event_count": _count_monitor_audit_lines(audit_paths["submit_audit"]),
            "plan_event_count": _count_monitor_audit_lines(audit_paths["plan_audit"]),
        },
        "account_connected": False,
        "market": None,
        "position": None,
        "account_assets": {},
        "open_orders": [],
        "live_account": None,
        "trade_summary": None,
        "income_summary": None,
        "hourly_summary": None,
        "diagnostics": {
            "hourly": build_hourly_diagnostics(events, None, limit=24),
        },
        "risk_timeline": build_risk_activation_timeline(events, limit=120),
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
        unrealized_pnl = _safe_float((snapshot.get("position") or {}).get("unrealized_pnl"))
        try:
            live_account = _build_live_account_snapshot(
                symbol=normalized_symbol,
                api_key=api_key,
                api_secret=api_secret,
            )
            snapshot["live_account"] = live_account
            if local_runtime_snapshot is not None and bool(runner.get("is_running")):
                local_position = snapshot.get("position") if isinstance(snapshot.get("position"), dict) else {}
                live_position = live_account.get("position") if isinstance(live_account.get("position"), dict) else {}
                if local_position and live_position:
                    snapshot["position"] = _merge_live_account_position_with_local_runtime(local_position, live_position)
                snapshot["account_assets"] = live_account["account_assets"]
                snapshot["open_orders"] = live_account["open_orders"]
                unrealized_pnl = _safe_float((snapshot.get("position") or {}).get("unrealized_pnl"))
            else:
                snapshot["position"] = live_account["position"]
                snapshot["account_assets"] = live_account["account_assets"]
                snapshot["open_orders"] = live_account["open_orders"]
                unrealized_pnl = _safe_float(live_account["position"].get("unrealized_pnl"))
        except Exception as exc:
            snapshot["warnings"].append(f"account_read_failed: {type(exc).__name__}: {exc}")
        try:
            user_trades, trade_meta = _load_or_fetch_trade_rows(
                audit_path=audit_paths["trade_audit"],
                symbol=normalized_symbol,
                api_key=api_key,
                api_secret=api_secret,
                session_start=stats_start,
            )
        except Exception as exc:
            user_trades = []
            trade_meta = {"source": "unavailable", "error": f"{type(exc).__name__}: {exc}"}
            snapshot["warnings"].append(f"trade_audit_refresh_failed: {type(exc).__name__}: {exc}")
        try:
            income_rows, income_meta = _load_or_fetch_income_rows(
                audit_path=audit_paths["income_audit"],
                symbol=normalized_symbol,
                api_key=api_key,
                api_secret=api_secret,
                session_start=stats_start,
            )
        except Exception as exc:
            income_rows = []
            income_meta = {"source": "unavailable", "error": f"{type(exc).__name__}: {exc}"}
            snapshot["warnings"].append(f"income_refresh_failed: {type(exc).__name__}: {exc}")
        trade_start, trade_last = _epoch_bounds(user_trades, trade_row_time_ms)
        income_start, income_last = _epoch_bounds(income_rows, income_row_time_ms)
        stats_start, stats_anchor = _resolve_monitor_stats_anchor(
            anchor_path=_monitor_anchor_path(audit_paths),
            symbol=normalized_symbol,
            candidates=[
                ("competition_start", competition_start),
                ("trade_start", trade_start),
                ("income_start", income_start),
                ("session_start", session_start),
            ],
        )
        session_start = _min_dt(stats_start, trade_start, income_start)
        session_last = _max_dt(session_last, trade_last, income_last)
        snapshot["session"]["start"] = stats_start.isoformat() if stats_start else None
        snapshot["session"]["last_event"] = session_last.isoformat() if session_last else None
        snapshot["competition_window"]["stats_start_at"] = stats_start.isoformat() if stats_start else None
        snapshot["audit"]["stats_anchor"] = stats_anchor
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
                order_role_lookup=order_role_lookup,
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
        competition_current_metric = _competition_current_metric_value(competition_board, trade_summary)
        trade_summary["competition_current_metric"] = competition_current_metric
        if _competition_uses_daily_sqrt_score(competition_board):
            trade_summary["competition_current_metric_label"] = "每日成交量开根号积分"
        snapshot["trade_summary"] = trade_summary
        snapshot["competition_reward_targets"] = build_reward_volume_targets(
            competition_board,
            current_volume=competition_current_metric,
        )
        snapshot["competition_displacement_volume"] = build_competition_displacement_volume(
            competition_board,
            current_volume=competition_current_metric,
        )
        snapshot["competition_entry_volume_targets"] = build_competition_entry_volume_targets(
            competition_board,
            current_volume=competition_current_metric,
        )
        snapshot["income_summary"] = income_summary
        snapshot["hourly_summary"] = hourly_summary
        snapshot["diagnostics"] = {
            "hourly": build_hourly_diagnostics(events, hourly_summary, limit=24),
        }
        snapshot["audit"]["trade_source"] = trade_meta
        snapshot["audit"]["income_source"] = income_meta
        snapshot["audit"]["trade_row_count"] = len(user_trades)
        snapshot["audit"]["income_row_count"] = len(income_rows)

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
            max(_safe_float(position.get("active_long_qty", position.get("long_qty", position.get("position_amt")))), 0.0)
            * max(_safe_float(market.get("mid_price")), 0.0)
        )
        current_short_notional = (
            max(_safe_float(position.get("active_short_qty", position.get("short_qty"))), 0.0)
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
    adaptive_step = (plan_report or {}).get("adaptive_step")
    if not isinstance(adaptive_step, dict):
        adaptive_step = {}
    elastic_volume = (plan_report or {}).get("elastic_volume")
    if not isinstance(elastic_volume, dict):
        elastic_volume = {}
    regime_entry_budget = (plan_report or {}).get("regime_entry_budget")
    if not isinstance(regime_entry_budget, dict):
        regime_entry_budget = {}
    entry_permission_gate = (plan_report or {}).get("entry_permission_gate")
    if not isinstance(entry_permission_gate, dict):
        entry_permission_gate = {}
    execution_regime = _execution_regime_from_sources(
        plan_report=plan_report if isinstance(plan_report, dict) else {},
        latest_loop=latest_loop,
        runner_config=runner_config if isinstance(runner_config, dict) else {},
    )

    if not isinstance(snapshot.get("position"), dict):
        snapshot["position"] = {}
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
        "execution_regime": execution_regime,
        "execution_regime_enabled": bool(execution_regime.get("enabled")),
        "execution_regime_mode": execution_regime.get("mode"),
        "execution_regime_state": execution_regime.get("state"),
        "execution_regime_raw_state": execution_regime.get("raw_state"),
        "execution_regime_risk_score": _safe_float(execution_regime.get("risk_score")),
        "execution_regime_hard_risk": bool(execution_regime.get("hard_risk")),
        "execution_regime_reason_codes": list(execution_regime.get("reason_codes") or []),
        "execution_regime_missing_features": list(execution_regime.get("missing_features") or []),
        "execution_regime_params": dict(execution_regime.get("params") or {}),
        "execution_regime_features": dict(execution_regime.get("features") or {}),
        "execution_regime_applied": bool(execution_regime.get("applied")),
        "auto_regime_enabled": auto_regime_enabled,
        "auto_regime_regime": auto_regime_regime,
        "auto_regime_reason": auto_regime_reason,
        "auto_regime_pending_count": auto_regime_pending_count,
        "auto_regime_confirm_cycles": auto_regime_confirm_cycles,
        "effective_strategy_profile": effective_strategy_profile,
        "effective_strategy_label": effective_strategy_label,
        "base_step_price": _safe_float(adaptive_step.get("base_step_price") or runner_config.get("step_price")),
        "plan_step_price": _safe_float((plan_report or {}).get("step_price")),
        "effective_step_price": _safe_float(
            regime_entry_budget.get("effective_step_price")
            or adaptive_step.get("effective_step_price")
            or (plan_report or {}).get("step_price")
        ),
        "base_per_order_notional": _safe_float(
            regime_entry_budget.get("base_per_order_notional")
            or runner_config.get("regime_entry_budget_base_per_order_notional")
            or runner_config.get("per_order_notional")
        ),
        "regime_entry_budget": dict(regime_entry_budget),
        "regime_entry_budget_enabled": bool(regime_entry_budget.get("enabled")),
        "regime_entry_budget_report_only": bool(regime_entry_budget.get("report_only")),
        "regime_entry_budget_state": str(regime_entry_budget.get("state") or "").strip(),
        "regime_entry_budget_candidate_regime": str(regime_entry_budget.get("candidate_regime") or "").strip(),
        "regime_entry_budget_target_regime": str(regime_entry_budget.get("target_regime") or "").strip(),
        "regime_entry_budget_switching": bool(regime_entry_budget.get("switching")),
        "regime_entry_budget_shock_guard_active": bool(regime_entry_budget.get("shock_guard_active")),
        "regime_entry_budget_cancel_entry_required": bool(regime_entry_budget.get("cancel_entry_required")),
        "regime_entry_budget_allow_entry_long": bool(regime_entry_budget.get("allow_entry_long", True)),
        "regime_entry_budget_allow_entry_short": bool(regime_entry_budget.get("allow_entry_short", True)),
        "regime_entry_budget_long_side_budget": _safe_float(regime_entry_budget.get("long_side_budget")),
        "regime_entry_budget_short_side_budget": _safe_float(regime_entry_budget.get("short_side_budget")),
        "regime_entry_budget_long_capacity": int(_safe_float(regime_entry_budget.get("long_entry_capacity"))),
        "regime_entry_budget_short_capacity": int(_safe_float(regime_entry_budget.get("short_entry_capacity"))),
        "regime_entry_budget_effective_step_ratio": _safe_float(regime_entry_budget.get("effective_step_ratio")),
        "regime_entry_budget_effective_step_price": _safe_float(regime_entry_budget.get("effective_step_price")),
        "regime_entry_budget_effective_per_order_notional": _safe_float(
            regime_entry_budget.get("effective_per_order_notional")
        ),
        "regime_entry_budget_base_step_ratio": _safe_float(runner_config.get("regime_entry_budget_base_step_ratio")),
        "regime_entry_budget_base_per_order_notional": _safe_float(
            runner_config.get("regime_entry_budget_base_per_order_notional")
        ),
        "regime_entry_budget_entry_reuse_tolerance": _safe_float(regime_entry_budget.get("entry_reuse_tolerance")),
        "regime_entry_budget_entry_reuse_reason": regime_entry_budget.get("entry_reuse_reason"),
        "regime_entry_budget_reasons": list(regime_entry_budget.get("reasons") or []),
        "regime_entry_budget_metrics": dict(regime_entry_budget.get("metrics") or {}),
        "entry_permission_gate": dict(entry_permission_gate),
        "elastic_volume": dict(elastic_volume),
        "elastic_volume_enabled": bool(elastic_volume.get("enabled")),
        "elastic_volume_regime": str(elastic_volume.get("regime") or "").strip(),
        "elastic_volume_step_scale": _safe_float(elastic_volume.get("step_scale")),
        "elastic_volume_per_order_scale": _safe_float(elastic_volume.get("per_order_scale")),
        "elastic_volume_levels_scale": _safe_float(elastic_volume.get("levels_scale")),
        "elastic_volume_reasons": list(elastic_volume.get("reasons") or []),
        "elastic_volume_metrics": dict(elastic_volume.get("metrics") or {}),
        "adaptive_step": dict(adaptive_step),
        "adaptive_step_enabled": bool(adaptive_step.get("enabled")),
        "adaptive_step_active": bool(adaptive_step.get("active")),
        "adaptive_step_controls_active": bool(adaptive_step.get("controls_active")),
        "adaptive_step_base_step_price": _safe_float(adaptive_step.get("base_step_price")),
        "adaptive_step_effective_step_price": _safe_float(adaptive_step.get("effective_step_price")),
        "adaptive_step_scale": _safe_float(adaptive_step.get("scale")),
        "adaptive_step_raw_scale": _safe_float(adaptive_step.get("raw_scale")),
        "adaptive_step_per_order_scale": _safe_float(adaptive_step.get("per_order_scale")),
        "adaptive_step_dominant_window": adaptive_step.get("dominant_window"),
        "adaptive_step_dominant_metric": adaptive_step.get("dominant_metric"),
        "adaptive_step_dominant_value": _safe_float(adaptive_step.get("dominant_value")),
        "adaptive_step_dominant_threshold": _safe_float(adaptive_step.get("dominant_threshold")),
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
        "cumulative_gross_notional": _safe_float(
            (snapshot.get("trade_summary") or {}).get("gross_notional")
            if isinstance(snapshot.get("trade_summary"), dict)
            else latest_loop.get("cumulative_gross_notional")
        ),
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
