from __future__ import annotations

import json
import os
import re
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlparse

from .audit import build_audit_paths, income_row_time_ms, read_trade_audit_rows, trade_row_time_ms
from .console_overview import _fetch_remote_json
from .console_registry import load_console_registry
from .data import fetch_spot_latest_price
from .monitor import (
    RUNNER_CONTROL_PATH,
    _build_session_window,
    _epoch_bounds,
    _filter_rows_since,
    read_symbol_runner_process,
    summarize_income,
    summarize_user_trades,
)
from .notifications import alert_source_label


RUNNING_STATUS_PAYLOAD_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
RUNNING_STATUS_PAYLOAD_CACHE_LOCK = threading.Lock()
STABLE_FEE_ASSETS = {"USDT", "USDC", "FDUSD", "BUSD"}
FEE_ASSET_PRICE_CACHE: dict[str, tuple[float, float]] = {}
FEE_ASSET_PRICE_CACHE_TTL_SECONDS = 60.0


def _running_status_cache_seconds(scope: str) -> float:
    raw = str(
        os.environ.get(f"GRID_RUNNING_STATUS_{scope.upper()}_CACHE_SECONDS")
        or os.environ.get("GRID_RUNNING_STATUS_CACHE_SECONDS")
        or "10"
    ).strip()
    try:
        value = float(raw)
    except ValueError:
        return 10.0
    return max(0.0, value)


def _copy_status_payload(payload: dict[str, Any]) -> dict[str, Any]:
    return json.loads(json.dumps(payload, ensure_ascii=False))


def _get_cached_status_payload(key: str, ttl_seconds: float) -> dict[str, Any] | None:
    if ttl_seconds <= 0:
        return None
    now = time.monotonic()
    with RUNNING_STATUS_PAYLOAD_CACHE_LOCK:
        cached = RUNNING_STATUS_PAYLOAD_CACHE.get(key)
        if cached is None:
            return None
        cached_at, payload = cached
        if now - cached_at > ttl_seconds:
            RUNNING_STATUS_PAYLOAD_CACHE.pop(key, None)
            return None
        return _copy_status_payload(payload)


def _set_cached_status_payload(key: str, payload: dict[str, Any]) -> None:
    with RUNNING_STATUS_PAYLOAD_CACHE_LOCK:
        RUNNING_STATUS_PAYLOAD_CACHE[key] = (time.monotonic(), _copy_status_payload(payload))


def _read_json_dict(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _default_runtime_paths_for_symbol(symbol: str) -> dict[str, str]:
    slug = re.sub(r"[^A-Za-z0-9]+", "_", str(symbol or "").strip()).strip("_").lower() or "symbol"
    return {
        "state_path": f"output/{slug}_loop_state.json",
        "plan_json": f"output/{slug}_loop_latest_plan.json",
        "submit_report_json": f"output/{slug}_loop_latest_submit.json",
        "summary_jsonl": f"output/{slug}_loop_events.jsonl",
    }


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _runtime_jsonl_limit() -> int:
    raw = str(os.environ.get("GRID_RUNNING_STATUS_JSONL_LIMIT") or "5000").strip()
    try:
        value = int(raw)
    except ValueError:
        return 5000
    return max(100, value)


def _read_jsonl_tail(path: Path, *, limit: int) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    chunk_size = 64 * 1024
    max_bytes = max(1, limit // 1000) * 1024 * 1024
    data = bytearray()
    try:
        with path.open("rb") as fh:
            fh.seek(0, 2)
            pos = fh.tell()
            while pos > 0 and data.count(b"\n") <= limit and len(data) < max_bytes:
                read_size = min(chunk_size, pos)
                pos -= read_size
                fh.seek(pos)
                data[:0] = fh.read(read_size)
    except OSError:
        return []

    rows: list[dict[str, Any]] = []
    for raw_line in data.splitlines()[-limit:]:
        raw_line = raw_line.strip()
        if not raw_line:
            continue
        try:
            item = json.loads(raw_line.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            continue
        if isinstance(item, dict):
            rows.append(item)
    return rows


def _runner_status_runtime_paths(symbol: str, runner: dict[str, Any]) -> dict[str, str]:
    runner_config = runner.get("config") if isinstance(runner.get("config"), dict) else {}
    runtime_paths = _default_runtime_paths_for_symbol(symbol)
    return {
        "events_path": str(runner_config.get("summary_jsonl") or runtime_paths["summary_jsonl"]),
        "plan_path": str(runner_config.get("plan_json") or runtime_paths["plan_json"]),
        "submit_report_path": str(runner_config.get("submit_report_json") or runtime_paths["submit_report_json"]),
    }


def _read_runtime_rows(path: Path) -> list[dict[str, Any]]:
    return _read_jsonl_tail(path, limit=_runtime_jsonl_limit())


def _parse_iso_datetime(value: Any) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None


def _safe_int(value: Any) -> int | None:
    if value is None or str(value).strip() == "":
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _safe_float_or_none(value: Any) -> float | None:
    if value is None or str(value).strip() == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _fee_asset_price_usdt(asset: str) -> float:
    normalized_asset = str(asset or "").upper().strip()
    if not normalized_asset:
        return 0.0
    now = time.time()
    cached = FEE_ASSET_PRICE_CACHE.get(normalized_asset)
    if cached is not None and now - cached[0] <= FEE_ASSET_PRICE_CACHE_TTL_SECONDS:
        return cached[1]
    try:
        price = float(fetch_spot_latest_price(f"{normalized_asset}USDT"))
    except Exception:
        price = 0.0
    if price > 0:
        FEE_ASSET_PRICE_CACHE[normalized_asset] = (now, price)
    return price


def _commission_usdt(trade_summary: dict[str, Any]) -> float:
    raw_by_asset = trade_summary.get("commission_raw_by_asset")
    if not isinstance(raw_by_asset, dict):
        return _safe_float(trade_summary.get("commission"))
    total = 0.0
    for asset, raw_amount in raw_by_asset.items():
        amount = _safe_float(raw_amount)
        if amount <= 0:
            continue
        normalized_asset = str(asset or "").upper().strip()
        if normalized_asset in STABLE_FEE_ASSETS:
            total += amount
            continue
        price = _fee_asset_price_usdt(normalized_asset)
        total += amount * price if price > 0 else amount
    return total


def _safe_int_count(value: Any, fallback: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return fallback


def _resolve_last_run_start(
    events: list[dict[str, Any]],
    submit_report: dict[str, Any],
    runner: dict[str, Any],
) -> datetime | None:
    runner_config = runner.get("config") if isinstance(runner.get("config"), dict) else {}
    stats_start = _parse_iso_datetime(runner_config.get("runtime_guard_stats_start_time"))
    if stats_start is not None:
        return stats_start
    for item in reversed(events):
        run_start = _parse_iso_datetime(
            item.get("run_start_time") or item.get("runtime_guard_stats_start_time")
        )
        if run_start is not None:
            return run_start
    runner_start = _parse_iso_datetime(runner_config.get("run_start_time"))
    if runner_start is not None:
        return runner_start
    session_start, _ = _build_session_window(events, submit_report)
    return session_start


def _runtime_order_identity(item: Any) -> tuple[Any, ...] | None:
    if not isinstance(item, dict):
        return None
    order_id = str(item.get("orderId") or item.get("order_id") or "").strip()
    if order_id:
        return ("order_id", order_id)
    client_order_id = str(item.get("clientOrderId") or item.get("client_order_id") or "").strip()
    if client_order_id:
        return ("client_order_id", client_order_id)
    side = str(item.get("side", "")).upper().strip()
    position_side = str(item.get("positionSide", item.get("position_side", "")) or "").upper().strip()
    price = str(item.get("price", "")).strip()
    qty = str(item.get("origQty", item.get("orig_qty", "")) or "").strip()
    if side or position_side or price or qty:
        return ("fallback", side, position_side, price, qty)
    return None


def _deduped_runtime_order_count(plan_report: dict[str, Any], submit_report: dict[str, Any]) -> int:
    seen: set[tuple[Any, ...]] = set()
    count = 0
    for item in list(plan_report.get("kept_orders") or []) + list(submit_report.get("placed_orders") or []):
        identity = _runtime_order_identity(item)
        if identity is None:
            continue
        if identity in seen:
            continue
        seen.add(identity)
        count += 1
    return count


def _format_current_position(strategy_mode: str, position_amt: float, long_qty: float, short_qty: float) -> str:
    normalized_mode = str(strategy_mode or "").strip()
    if normalized_mode == "one_way_long":
        return f"多 {long_qty:g}"
    if normalized_mode == "one_way_short":
        return f"空 {short_qty:g}"
    return f"净 {position_amt:g} · 多 {long_qty:g} / 空 {short_qty:g}"


def _build_local_stat_snapshot(symbol: str, config: dict[str, Any], runner: dict[str, Any]) -> dict[str, Any]:
    runtime_paths = _runner_status_runtime_paths(symbol, runner)
    plan_report = _read_json_dict(Path(runtime_paths["plan_path"])) or {}
    submit_report = _read_json_dict(Path(runtime_paths["submit_report_path"])) or {}
    submit_plan_snapshot = (
        submit_report.get("plan_snapshot") if isinstance(submit_report.get("plan_snapshot"), dict) else {}
    )
    events = _read_runtime_rows(Path(runtime_paths["events_path"]))
    session_start = _resolve_last_run_start(events, submit_report, runner)
    audit_paths = build_audit_paths(Path(runtime_paths["events_path"]))
    trade_rows = read_trade_audit_rows(audit_paths["trade_audit"], limit=0)
    income_rows = _read_runtime_rows(audit_paths["income_audit"])
    trade_rows = _filter_rows_since(trade_rows, since=session_start, row_time_ms=trade_row_time_ms)
    income_rows = _filter_rows_since(income_rows, since=session_start, row_time_ms=income_row_time_ms)
    trade_summary = summarize_user_trades(trade_rows)
    income_summary = summarize_income(income_rows)

    recent_hour_floor_ms = int((datetime.now(timezone.utc) - timedelta(hours=1)).timestamp() * 1000)
    recent_hour_trades = [item for item in trade_rows if trade_row_time_ms(item) >= recent_hour_floor_ms]
    recent_hour_income = [item for item in income_rows if income_row_time_ms(item) >= recent_hour_floor_ms]
    recent_trade_summary = summarize_user_trades(recent_hour_trades)
    recent_income_summary = summarize_income(recent_hour_income)

    strategy_mode = str(
        plan_report.get("strategy_mode")
        or submit_plan_snapshot.get("strategy_mode")
        or config.get("strategy_mode")
        or ""
    ).strip()
    long_qty = max(
        _safe_float(
            plan_report.get("current_long_qty")
            if plan_report.get("current_long_qty") is not None
            else submit_plan_snapshot.get("current_long_qty")
        ),
        0.0,
    )
    short_qty = max(
        _safe_float(
            plan_report.get("current_short_qty")
            if plan_report.get("current_short_qty") is not None
            else submit_plan_snapshot.get("current_short_qty")
        ),
        0.0,
    )
    raw_position_amt = (
        plan_report.get("actual_net_qty")
        if plan_report.get("actual_net_qty") is not None
        else submit_plan_snapshot.get("actual_net_qty")
    )
    position_amt = _safe_float(raw_position_amt)
    if raw_position_amt is None:
        if strategy_mode == "one_way_short" and short_qty > 0:
            position_amt = -short_qty
        elif strategy_mode == "one_way_long" and long_qty > 0:
            position_amt = long_qty
        else:
            position_amt = long_qty - short_qty
    unrealized_pnl = _safe_float(
        plan_report.get("unrealized_pnl")
        if plan_report.get("unrealized_pnl") is not None
        else submit_plan_snapshot.get("unrealized_pnl")
    )
    total_fees = _commission_usdt(trade_summary)
    total_pnl = (
        _safe_float(trade_summary.get("realized_pnl"))
        + unrealized_pnl
        + _safe_float(income_summary.get("funding_fee"))
        - total_fees
    )
    recent_hour_pnl = (
        _safe_float(recent_trade_summary.get("realized_pnl"))
        + _safe_float(recent_income_summary.get("funding_fee"))
        - _commission_usdt(recent_trade_summary)
    )

    open_order_count = _deduped_runtime_order_count(plan_report, submit_report)
    trade_start, trade_last = _epoch_bounds(trade_rows, trade_row_time_ms)
    income_start, income_last = _epoch_bounds(income_rows, income_row_time_ms)
    updated_at_dt = max(
        [item for item in [trade_last, income_last, _parse_iso_datetime(submit_report.get("generated_at"))] if item is not None],
        default=None,
    )
    updated_at = updated_at_dt.isoformat() if updated_at_dt is not None else None
    has_last_run = bool(plan_report or submit_report or trade_rows or income_rows or events)
    adaptive_step = plan_report.get("adaptive_step") if isinstance(plan_report.get("adaptive_step"), dict) else {}

    return {
        "has_last_run": has_last_run,
        "strategy_mode": strategy_mode or config.get("strategy_mode"),
        "session_start": session_start.isoformat() if session_start is not None else None,
        "session_trade_start": trade_start.isoformat() if trade_start is not None else None,
        "updated_at": updated_at,
        "open_order_count": open_order_count if has_last_run else 0,
        "effective_step_price": _safe_float_or_none(adaptive_step.get("effective_step_price")),
        "effective_buy_levels": _safe_int(plan_report.get("effective_buy_levels")),
        "effective_sell_levels": _safe_int(plan_report.get("effective_sell_levels")),
        "effective_base_position_notional": _safe_float_or_none(plan_report.get("effective_base_position_notional")),
        "effective_pause_buy_position_notional": _safe_float_or_none(plan_report.get("effective_pause_buy_position_notional")),
        "effective_pause_short_position_notional": _safe_float_or_none(plan_report.get("effective_pause_short_position_notional")),
        "effective_max_position_notional": _safe_float_or_none(plan_report.get("effective_max_position_notional")),
        "effective_max_short_position_notional": _safe_float_or_none(plan_report.get("effective_max_short_position_notional")),
        "current_position_display": (
            _format_current_position(strategy_mode, position_amt, long_qty, short_qty) if has_last_run else "--"
        ),
        "position_amt": position_amt if has_last_run else None,
        "long_qty": long_qty if has_last_run else None,
        "short_qty": short_qty if has_last_run else None,
        "unrealized_pnl": unrealized_pnl if has_last_run else None,
        "total_volume": _safe_float(trade_summary.get("gross_notional")) if has_last_run else None,
        "recent_hour_volume": _safe_float(recent_trade_summary.get("gross_notional")) if has_last_run else None,
        "total_pnl": total_pnl if has_last_run else None,
        "recent_hour_pnl": recent_hour_pnl if has_last_run else None,
        "total_fees": total_fees if has_last_run else None,
        "trade_pnl": _safe_float(trade_summary.get("realized_pnl")) if has_last_run else None,
        "fees": total_fees if has_last_run else None,
        "funding_fee": _safe_float(income_summary.get("funding_fee")) if has_last_run else None,
    }


def iter_running_status_saved_control_configs(output_dir: Path | None = None) -> list[dict[str, Any]]:
    base_output_dir = Path(output_dir) if output_dir is not None else Path("output")
    paths: list[Path] = []
    if base_output_dir.exists():
        paths.extend(sorted(base_output_dir.glob("*_loop_runner_control.json")))
    legacy_control_path = base_output_dir / RUNNER_CONTROL_PATH.name
    if legacy_control_path.exists() and legacy_control_path not in paths:
        paths.append(legacy_control_path)

    seen_symbols: set[str] = set()
    configs: list[dict[str, Any]] = []
    for path in paths:
        stored = _read_json_dict(path)
        if not stored:
            continue
        symbol = str(stored.get("symbol", "")).upper().strip()
        if not symbol or symbol in seen_symbols:
            continue
        candidate = dict(stored)
        candidate["symbol"] = symbol
        for key, value in _default_runtime_paths_for_symbol(symbol).items():
            candidate.setdefault(key, value)
        configs.append(candidate)
        seen_symbols.add(symbol)
    return configs


def running_status_target_url(symbol: str, base_url: str = "") -> str:
    normalized_symbol = str(symbol or "").upper().strip()
    query = quote(normalized_symbol) if normalized_symbol else ""
    if not query:
        return f"{str(base_url or '').rstrip('/')}/running_status" if base_url else "/running_status"
    suffix = f"/running_status?symbol={query}"
    normalized_base_url = str(base_url or "").strip().rstrip("/")
    return f"{normalized_base_url}{suffix}" if normalized_base_url else suffix


def build_running_status_card(
    symbol: str,
    config: dict[str, Any],
    runner: dict[str, Any],
    *,
    server_id: str | None = None,
    server_label: str | None = None,
    server_base_url: str = "",
    focused_symbol: str | None = None,
) -> dict[str, Any]:
    normalized_symbol = str(symbol or "").upper().strip()
    runner_payload = dict(runner or {})
    merged_config = dict(config or {})
    if isinstance(runner_payload.get("config"), dict):
        merged_config.update(runner_payload["config"])
    runner_payload["config"] = merged_config
    normalized_focused_symbol = str(focused_symbol or "").upper().strip()
    stats = _build_local_stat_snapshot(normalized_symbol, merged_config, runner_payload)

    def runtime_or_config(runtime_key: str, config_key: str) -> Any:
        runtime_value = stats.get(runtime_key)
        return merged_config.get(config_key) if runtime_value is None else runtime_value

    card = {
        "symbol": normalized_symbol,
        "server_id": str(server_id or "").strip() or None,
        "server_label": str(server_label or "").strip() or None,
        "server_base_url": str(server_base_url or "").strip(),
        "target_url": running_status_target_url(normalized_symbol, server_base_url),
        "focused": bool(normalized_focused_symbol and normalized_symbol == normalized_focused_symbol),
        "configured": bool(runner_payload.get("configured")),
        "is_running": bool(runner_payload.get("is_running")),
        "pid": runner_payload.get("pid"),
        "updated_at": stats.get("updated_at"),
        "session_start": stats.get("session_start"),
        "session_trade_start": stats.get("session_trade_start"),
        "strategy_name": str(merged_config.get("strategy_profile") or normalized_symbol).strip(),
        "strategy_profile": merged_config.get("strategy_profile"),
        "strategy_mode": stats.get("strategy_mode") or merged_config.get("strategy_mode"),
        "step_price": runtime_or_config("effective_step_price", "step_price"),
        "buy_levels": runtime_or_config("effective_buy_levels", "buy_levels"),
        "sell_levels": runtime_or_config("effective_sell_levels", "sell_levels"),
        "base_position_notional": runtime_or_config("effective_base_position_notional", "base_position_notional"),
        "pause_buy_position_notional": runtime_or_config(
            "effective_pause_buy_position_notional",
            "pause_buy_position_notional",
        ),
        "pause_short_position_notional": runtime_or_config(
            "effective_pause_short_position_notional",
            "pause_short_position_notional",
        ),
        "threshold_position_notional": merged_config.get("threshold_position_notional"),
        "max_position_notional": runtime_or_config("effective_max_position_notional", "max_position_notional"),
        "max_short_position_notional": runtime_or_config(
            "effective_max_short_position_notional",
            "max_short_position_notional",
        ),
        "max_total_notional": merged_config.get("max_total_notional"),
        "soft_reduce_summary": {
            "adverse_reduce_enabled": bool(merged_config.get("adverse_reduce_enabled", False)),
            "adverse_reduce_target_ratio": merged_config.get("adverse_reduce_target_ratio"),
            "adverse_reduce_max_order_notional": merged_config.get("adverse_reduce_max_order_notional"),
        },
        "hard_reduce_summary": {
            "hard_loss_forced_reduce_enabled": bool(merged_config.get("hard_loss_forced_reduce_enabled", False)),
            "hard_loss_forced_reduce_target_notional": merged_config.get("hard_loss_forced_reduce_target_notional"),
            "hard_loss_forced_reduce_max_order_notional": merged_config.get("hard_loss_forced_reduce_max_order_notional"),
        },
        "timeout_reduce_summary": {
            "threshold_reduce_target_ratio": merged_config.get("threshold_reduce_target_ratio"),
            "threshold_reduce_taker_timeout_seconds": merged_config.get("threshold_reduce_taker_timeout_seconds"),
            "short_threshold_timeout_seconds": merged_config.get("short_threshold_timeout_seconds"),
            "adverse_reduce_maker_timeout_seconds": merged_config.get("adverse_reduce_maker_timeout_seconds"),
        },
        "open_order_count": stats.get("open_order_count", 0),
        "current_position_display": stats.get("current_position_display", "--"),
        "position_amt": stats.get("position_amt"),
        "long_qty": stats.get("long_qty"),
        "short_qty": stats.get("short_qty"),
        "unrealized_pnl": stats.get("unrealized_pnl"),
        "total_volume": stats.get("total_volume"),
        "recent_hour_volume": stats.get("recent_hour_volume"),
        "total_pnl": stats.get("total_pnl"),
        "recent_hour_pnl": stats.get("recent_hour_pnl"),
        "total_fees": stats.get("total_fees"),
        "trade_pnl": stats.get("trade_pnl"),
        "fees": stats.get("fees"),
        "funding_fee": stats.get("funding_fee"),
        "config": merged_config,
        "snapshot": None,
    }
    card["last_run_state"] = "running" if card["is_running"] else ("last_run" if stats["has_last_run"] else "saved")
    return card


def build_running_status_local_payload(
    symbol: str | None = None,
    *,
    server_id: str | None = None,
    server_label: str | None = None,
    server_base_url: str = "",
) -> dict[str, Any]:
    normalized_focused_symbol = str(symbol or "").upper().strip() or None
    cards: list[dict[str, Any]] = []
    for config in iter_running_status_saved_control_configs():
        card_symbol = str(config.get("symbol", "")).upper().strip()
        if not card_symbol:
            continue
        runner = read_symbol_runner_process(card_symbol)
        cards.append(
            build_running_status_card(
                card_symbol,
                config,
                runner,
                server_id=server_id,
                server_label=server_label,
                server_base_url=server_base_url,
                focused_symbol=normalized_focused_symbol,
            )
        )

    cards.sort(key=lambda item: str(item.get("symbol") or ""))
    running = [item for item in cards if item.get("is_running")]
    saved_idle = [item for item in cards if not item.get("is_running")]
    running_summary = _summarize_running_cards(running)

    return {
        "ok": True,
        "ts": datetime.now(timezone.utc).isoformat(),
        "scope": "local",
        "view_mode": "local",
        "server_id": str(server_id or "").strip() or None,
        "server_label": str(server_label or "").strip() or None,
        "server_base_url": str(server_base_url or "").strip(),
        "focused_symbol": normalized_focused_symbol,
        "groups": {
            "running": running,
            "saved_idle": saved_idle,
        },
        "summary": {
            "running_symbol_count": len(running),
            "saved_idle_symbol_count": len(saved_idle),
            **running_summary,
        },
    }


def _running_cards(server_payloads: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for payload in server_payloads:
        groups = payload.get("groups") if isinstance(payload.get("groups"), dict) else {}
        for item in list(groups.get("running") or []):
            if isinstance(item, dict):
                rows.append(item)
    return rows


def _summarize_running_cards(running_rows: list[dict[str, Any]]) -> dict[str, float]:
    return {
        "running_total_volume": sum(_safe_float(item.get("total_volume")) for item in running_rows),
        "recent_hour_volume": sum(_safe_float(item.get("recent_hour_volume")) for item in running_rows),
        "total_pnl": sum(_safe_float(item.get("total_pnl")) for item in running_rows),
        "recent_hour_pnl": sum(_safe_float(item.get("recent_hour_pnl")) for item in running_rows),
        "total_fees": sum(_safe_float(item.get("total_fees")) for item in running_rows),
    }


def local_running_status_server_id(registry: dict[str, Any] | None = None) -> str | None:
    explicit = str(os.environ.get("GRID_CONSOLE_SERVER_ID") or os.environ.get("GRID_SERVER_ID") or "").strip()
    if explicit:
        return explicit

    try:
        registry_data = registry or load_console_registry()
    except Exception:
        return None

    normalized_source_label = str(alert_source_label() or "").strip().lower()
    if not normalized_source_label:
        return None

    for server in list(registry_data.get("servers") or []):
        server_id = str(server.get("id") or "").strip()
        server_label = str(server.get("label") or "").strip().lower()
        base_url = str(server.get("base_url") or "").strip().rstrip("/")
        hostname = (urlparse(base_url).hostname or "").strip().lower()
        if normalized_source_label == server_id.lower():
            return server_id
        if normalized_source_label == server_label:
            return server_id
        if hostname and (
            hostname == normalized_source_label
            or hostname.endswith(f".{normalized_source_label}")
            or hostname.endswith(normalized_source_label)
        ):
            return server_id
    return None


def normalize_running_status_server_payload(payload: dict[str, Any], *, server: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(payload or {})
    groups = normalized.get("groups") if isinstance(normalized.get("groups"), dict) else {}
    running = list(groups.get("running") or [])
    saved_idle = list(groups.get("saved_idle") or [])
    summary = normalized.get("summary") if isinstance(normalized.get("summary"), dict) else {}
    raw_server_base_url = normalized["server_base_url"] if "server_base_url" in normalized else None
    normalized["ok"] = bool(normalized.get("ok", True))
    normalized["scope"] = "local"
    normalized["view_mode"] = "local"
    normalized["server_id"] = str(normalized.get("server_id") or server.get("id") or "").strip() or None
    normalized["server_label"] = str(normalized.get("server_label") or server.get("label") or "").strip() or None
    normalized["server_base_url"] = (
        str(raw_server_base_url).strip().rstrip("/")
        if raw_server_base_url is not None
        else str(server.get("base_url") or "").strip().rstrip("/")
    )
    normalized["groups"] = {
        "running": running,
        "saved_idle": saved_idle,
    }
    normalized["summary"] = {
        "running_symbol_count": _safe_int_count(summary.get("running_symbol_count"), len(running)),
        "saved_idle_symbol_count": _safe_int_count(summary.get("saved_idle_symbol_count"), len(saved_idle)),
        **_summarize_running_cards([item for item in running if isinstance(item, dict)]),
    }
    return normalized


def fetch_remote_running_status_payload(server: dict[str, Any]) -> dict[str, Any]:
    payload = _fetch_remote_json(server, "/api/running_status", params={"scope": "local"})
    return normalize_running_status_server_payload(payload, server=server)


def build_running_status_cross_payload() -> dict[str, Any]:
    registry = load_console_registry()
    servers = [server for server in list(registry.get("servers") or []) if bool(server.get("enabled", True))]
    local_server_id = local_running_status_server_id(registry)
    server_key = ",".join(
        f"{str(server.get('id') or '').strip()}={str(server.get('base_url') or '').strip().rstrip('/')}"
        for server in servers
    )
    cache_key = (
        f"cross:{local_server_id or ''}:{server_key}:"
        f"{id(build_running_status_local_payload)}:{id(fetch_remote_running_status_payload)}"
    )
    cached = _get_cached_status_payload(cache_key, _running_status_cache_seconds("cross"))
    if cached is not None:
        return cached

    server_payloads: list[dict[str, Any]] = []
    warnings: list[str] = []

    for server in servers:
        server_id = str(server.get("id") or "").strip()
        try:
            if local_server_id and server_id == local_server_id:
                payload = build_running_status_local_payload(
                    server_id=server_id,
                    server_label=str(server.get("label") or "").strip() or None,
                    server_base_url="",
                )
                payload = normalize_running_status_server_payload(payload, server=server)
            else:
                payload = fetch_remote_running_status_payload(server)
        except Exception as exc:
            warnings.append(f"{server_id or 'unknown'} unavailable: {type(exc).__name__}: {exc}")
            payload = {
                "ok": False,
                "scope": "local",
                "view_mode": "local",
                "server_id": server_id or None,
                "server_label": str(server.get("label") or "").strip() or None,
                "server_base_url": str(server.get("base_url") or "").strip().rstrip("/"),
                "groups": {"running": [], "saved_idle": []},
                "summary": {
                    "running_symbol_count": 0,
                    "saved_idle_symbol_count": 0,
                    **_summarize_running_cards([]),
                },
                "error": f"{type(exc).__name__}: {exc}",
            }
        server_payloads.append(payload)

    running_rows = _running_cards(server_payloads)
    payload = {
        "ok": True,
        "ts": datetime.now(timezone.utc).isoformat(),
        "scope": "cross",
        "view_mode": "cross",
        "servers": server_payloads,
        "summary": {
            "server_count": len(server_payloads),
            "running_symbol_count": sum(
                int(((item.get("summary") or {}).get("running_symbol_count") or 0))
                for item in server_payloads
            ),
            "saved_idle_symbol_count": sum(
                int(((item.get("summary") or {}).get("saved_idle_symbol_count") or 0))
                for item in server_payloads
            ),
            **_summarize_running_cards(running_rows),
        },
        "warnings": warnings,
    }
    _set_cached_status_payload(cache_key, payload)
    return payload


def run_running_status_query(query: dict[str, list[str]]) -> dict[str, Any]:
    scope = str((query.get("scope") or [""])[0] or "").strip().lower()
    normalized_symbol = str((query.get("symbol") or [""])[0] or "").upper().strip()
    if scope == "cross" and not normalized_symbol:
        return build_running_status_cross_payload()
    if scope not in {"", "local", "cross"}:
        raise ValueError("scope must be local or cross")
    if not normalized_symbol:
        cache_key = "local"
        cached = _get_cached_status_payload(cache_key, _running_status_cache_seconds("local"))
        if cached is not None:
            return cached
        payload = build_running_status_local_payload(symbol=None)
        _set_cached_status_payload(cache_key, payload)
        return payload
    return build_running_status_local_payload(symbol=normalized_symbol)
