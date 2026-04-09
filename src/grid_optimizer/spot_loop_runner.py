from __future__ import annotations

import argparse
import json
import math
import os
import subprocess
import sys
import time
import traceback
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .backtest import build_grid_levels
from .data import (
    delete_spot_order,
    fetch_spot_account_info,
    fetch_spot_book_tickers,
    fetch_spot_klines,
    fetch_spot_latest_price,
    fetch_spot_open_orders,
    fetch_spot_symbol_config,
    fetch_spot_user_trades,
    load_binance_api_credentials,
    post_spot_order,
)
from .dry_run import _round_order_price, _round_order_qty
from .runtime_guards import evaluate_runtime_guards, normalize_runtime_guard_config
from .inventory_grid_plan import build_inventory_grid_orders
from .inventory_grid_recovery import rebuild_inventory_grid_runtime
from .semi_auto_plan import diff_open_orders
from .spot_flatten_runner import load_live_spot_flatten_snapshot, spot_flatten_client_order_prefix

STATE_VERSION = 2
EPSILON = 1e-12
PRICE_CACHE_TTL_SECONDS = 30.0
_LATEST_PRICE_CACHE: dict[str, tuple[float, float]] = {}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _safe_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"true", "1", "yes"}


def _append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False) + "\n")


def _new_metrics() -> dict[str, Any]:
    return {
        "gross_notional": 0.0,
        "buy_notional": 0.0,
        "sell_notional": 0.0,
        "commission_quote": 0.0,
        "commission_raw_by_asset": {},
        "realized_pnl": 0.0,
        "recycle_realized_pnl": 0.0,
        "recycle_loss_abs": 0.0,
        "trade_count": 0,
        "maker_count": 0,
        "buy_count": 0,
        "sell_count": 0,
        "recent_trades": [],
        "first_trade_time_ms": 0,
        "last_trade_time_ms": 0,
    }


def _load_state(path: Path, symbol: str, strategy_mode: str, cell_count: int) -> dict[str, Any]:
    state: dict[str, Any]
    if path.exists():
        try:
            state = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(state, dict):
                state = {}
        except (OSError, json.JSONDecodeError):
            state = {}
    else:
        state = {}
    state.setdefault("version", STATE_VERSION)
    state.setdefault("symbol", symbol)
    state.setdefault("strategy_mode", strategy_mode)
    state.setdefault("last_trade_time_ms", 0)
    state.setdefault("seen_trade_ids", [])
    state.setdefault("known_orders", {})
    state.setdefault("cells", {})
    state.setdefault("cycle", 0)
    state.setdefault("inventory_lots", [])
    state.setdefault("metrics", _new_metrics())
    state.setdefault("center_price", 0.0)
    state.setdefault("center_shift_down_cycles", 0)
    state.setdefault("center_shift_up_cycles", 0)
    state.setdefault("center_shift_count", 0)
    state.setdefault("last_mode", "attack")

    metrics = state.get("metrics")
    if not isinstance(metrics, dict):
        metrics = _new_metrics()
        state["metrics"] = metrics
    defaults = _new_metrics()
    for key, value in defaults.items():
        metrics.setdefault(key, value)
    if not isinstance(metrics.get("commission_raw_by_asset"), dict):
        metrics["commission_raw_by_asset"] = {}
    if not isinstance(metrics.get("recent_trades"), list):
        metrics["recent_trades"] = []

    cells = state["cells"]
    if not isinstance(cells, dict):
        cells = {}
        state["cells"] = cells
    for idx in range(1, cell_count + 1):
        key = str(idx)
        cell_state = cells.get(key)
        if not isinstance(cell_state, dict):
            cell_state = {}
        cell_state["position_qty"] = max(_safe_float(cell_state.get("position_qty")), 0.0)
        cells[key] = cell_state
    for key in list(cells.keys()):
        if not str(key).isdigit() or int(key) < 1 or int(key) > cell_count:
            cells.pop(key, None)

    lots = state.get("inventory_lots")
    if not isinstance(lots, list):
        lots = []
    normalized_lots: list[dict[str, Any]] = []
    for lot in lots:
        if not isinstance(lot, dict):
            continue
        qty = max(_safe_float(lot.get("qty")), 0.0)
        cost_quote = max(_safe_float(lot.get("cost_quote")), 0.0)
        if qty <= EPSILON:
            continue
        normalized_lots.append(
            {
                "qty": qty,
                "cost_quote": cost_quote,
                "buy_time_ms": _safe_int(lot.get("buy_time_ms")),
                "tag": str(lot.get("tag", "") or ""),
            }
        )
    state["inventory_lots"] = normalized_lots
    return state


def _save_state(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def _extract_balance(account_info: dict[str, Any], asset: str) -> tuple[float, float]:
    wanted = str(asset or "").upper().strip()
    balances = account_info.get("balances", [])
    if not isinstance(balances, list):
        return 0.0, 0.0
    for item in balances:
        if not isinstance(item, dict):
            continue
        if str(item.get("asset", "")).upper().strip() != wanted:
            continue
        return _safe_float(item.get("free")), _safe_float(item.get("locked"))
    return 0.0, 0.0


def _build_client_order_id(prefix: str, tag: str, side: str) -> str:
    side_text = "b" if str(side).upper().strip() == "BUY" else "s"
    nonce = int(time.time() * 1000) % 1_000_000_000
    return f"{prefix}_{tag[:20]}_{side_text}_{nonce}"


def _sum_inventory_qty(lots: list[dict[str, Any]]) -> float:
    return sum(max(_safe_float(item.get("qty")), 0.0) for item in lots if isinstance(item, dict))


def _sum_inventory_cost(lots: list[dict[str, Any]]) -> float:
    return sum(max(_safe_float(item.get("cost_quote")), 0.0) for item in lots if isinstance(item, dict))


def _inventory_avg_cost(lots: list[dict[str, Any]]) -> float:
    total_qty = _sum_inventory_qty(lots)
    if total_qty <= EPSILON:
        return 0.0
    return _sum_inventory_cost(lots) / total_qty


def _oldest_inventory_age_minutes(lots: list[dict[str, Any]]) -> float:
    buy_times = [_safe_int(item.get("buy_time_ms")) for item in lots if isinstance(item, dict) and _safe_int(item.get("buy_time_ms")) > 0]
    if not buy_times:
        return 0.0
    oldest_ms = min(buy_times)
    return max((time.time() * 1000 - oldest_ms) / 60_000.0, 0.0)


def _consume_inventory_lots(lots: list[dict[str, Any]], qty_to_consume: float) -> tuple[list[dict[str, Any]], float]:
    remaining = max(float(qty_to_consume), 0.0)
    new_lots: list[dict[str, Any]] = []
    consumed_cost = 0.0
    for lot in lots:
        if not isinstance(lot, dict):
            continue
        qty = max(_safe_float(lot.get("qty")), 0.0)
        cost_quote = max(_safe_float(lot.get("cost_quote")), 0.0)
        if qty <= EPSILON:
            continue
        if remaining <= EPSILON:
            new_lots.append({"qty": qty, "cost_quote": cost_quote, "buy_time_ms": _safe_int(lot.get("buy_time_ms")), "tag": str(lot.get("tag", "") or "")})
            continue
        take_qty = min(qty, remaining)
        unit_cost = cost_quote / qty if qty > EPSILON else 0.0
        consumed_cost += unit_cost * take_qty
        left_qty = qty - take_qty
        if left_qty > EPSILON:
            new_lots.append(
                {
                    "qty": left_qty,
                    "cost_quote": max(cost_quote - unit_cost * take_qty, 0.0),
                    "buy_time_ms": _safe_int(lot.get("buy_time_ms")),
                    "tag": str(lot.get("tag", "") or ""),
                }
            )
        remaining -= take_qty
    return new_lots, consumed_cost


def _build_grid_cells(config: dict[str, Any], symbol_info: dict[str, Any]) -> list[dict[str, Any]]:
    levels = build_grid_levels(
        float(config["min_price"]),
        float(config["max_price"]),
        int(config["n"]),
        str(config.get("grid_level_mode", "arithmetic")),
    )
    total_quote_budget = max(float(config["total_quote_budget"]), 0.0)
    per_cell_notional = total_quote_budget / max(int(config["n"]), 1)
    tick_size = symbol_info.get("tick_size")
    step_size = symbol_info.get("step_size")
    min_qty = symbol_info.get("min_qty")
    min_notional = symbol_info.get("min_notional")

    cells: list[dict[str, Any]] = []
    for idx in range(int(config["n"])):
        lower_price = _round_order_price(float(levels[idx]), tick_size, "BUY")
        upper_price = _round_order_price(float(levels[idx + 1]), tick_size, "SELL")
        if lower_price <= 0 or upper_price <= lower_price:
            continue
        qty = _round_order_qty(per_cell_notional / lower_price, step_size)
        notional = lower_price * qty
        if qty <= 0:
            continue
        if min_qty is not None and qty < min_qty:
            continue
        if min_notional is not None and notional < min_notional:
            continue
        cells.append(
            {
                "idx": idx + 1,
                "buy_price": lower_price,
                "sell_price": upper_price,
                "target_qty": qty,
                "entry_notional": notional,
                "step_size": step_size,
            }
        )
    return cells


def _is_strategy_order(order: dict[str, Any], prefix: str) -> bool:
    client_order_id = str(order.get("clientOrderId", "") or "")
    return client_order_id.startswith(prefix)


def _normalize_commission_quote(*, commission: float, commission_asset: str, price: float, base_asset: str, quote_asset: str) -> float:
    if commission <= 0:
        return 0.0
    if commission_asset == quote_asset:
        return commission
    if commission_asset == base_asset:
        return commission * price
    rate = _conversion_rate_to_quote(commission_asset=commission_asset, quote_asset=quote_asset)
    if rate > 0:
        return commission * rate
    return 0.0


def _latest_spot_price_cached(symbol: str) -> float:
    normalized = str(symbol or "").upper().strip()
    if not normalized:
        return 0.0
    now = time.time()
    cached = _LATEST_PRICE_CACHE.get(normalized)
    if cached and now - cached[1] <= PRICE_CACHE_TTL_SECONDS:
        return cached[0]
    try:
        latest = fetch_spot_latest_price(normalized)
    except Exception:
        return 0.0
    if latest > 0:
        _LATEST_PRICE_CACHE[normalized] = (latest, now)
    return latest


def _conversion_rate_to_quote(*, commission_asset: str, quote_asset: str) -> float:
    asset = str(commission_asset or "").upper().strip()
    quote = str(quote_asset or "").upper().strip()
    if not asset or not quote:
        return 0.0
    if asset == quote:
        return 1.0

    direct = _latest_spot_price_cached(f"{asset}{quote}")
    if direct > 0:
        return direct

    inverse = _latest_spot_price_cached(f"{quote}{asset}")
    if inverse > 0:
        return 1.0 / inverse

    if quote != "USDT":
        asset_usdt = _latest_spot_price_cached(f"{asset}USDT")
        quote_usdt = _latest_spot_price_cached(f"{quote}USDT")
        if asset_usdt > 0 and quote_usdt > 0:
            return asset_usdt / quote_usdt
    return 0.0


def _append_recent_trade(metrics: dict[str, Any], payload: dict[str, Any]) -> None:
    rows = metrics.get("recent_trades")
    if not isinstance(rows, list):
        rows = []
    rows.append(payload)
    metrics["recent_trades"] = rows[-2000:]


def _symbol_output_slug(symbol: str) -> str:
    normalized = "".join(ch if ch.isalnum() else "_" for ch in str(symbol or "").strip().lower()).strip("_")
    return normalized or "symbol"


def _spot_flatten_pid_path(symbol: str) -> Path:
    return Path(f"output/{_symbol_output_slug(symbol)}_spot_flatten.pid")


def _spot_flatten_log_path(symbol: str) -> Path:
    return Path(f"output/{_symbol_output_slug(symbol)}_spot_flatten.log")


def _spot_flatten_events_path(symbol: str) -> Path:
    return Path(f"output/{_symbol_output_slug(symbol)}_spot_flatten_events.jsonl")


def _pid_is_running(pid_path: Path) -> bool:
    if not pid_path.exists():
        return False
    try:
        pid = int(pid_path.read_text(encoding="utf-8").strip())
    except (OSError, ValueError):
        pid_path.unlink(missing_ok=True)
        return False
    try:
        os.kill(pid, 0)
    except OSError:
        pid_path.unlink(missing_ok=True)
        return False
    return True


def _start_spot_flatten_process(symbol: str) -> dict[str, Any]:
    pid_path = _spot_flatten_pid_path(symbol)
    if _pid_is_running(pid_path):
        return {"started": False, "already_running": True}
    log_path = _spot_flatten_log_path(symbol)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    src_path = str((Path.cwd() / "src").resolve())
    current_pythonpath = env.get("PYTHONPATH", "").strip()
    env["PYTHONPATH"] = src_path if not current_pythonpath else f"{src_path}{os.pathsep}{current_pythonpath}"
    command = [
        sys.executable,
        "-m",
        "grid_optimizer.spot_flatten_runner",
        "--symbol",
        str(symbol),
        "--client-order-prefix",
        spot_flatten_client_order_prefix(symbol),
        "--sleep-seconds",
        "2.0",
        "--recv-window",
        "5000",
        "--max-consecutive-errors",
        "20",
        "--events-jsonl",
        str(_spot_flatten_events_path(symbol)),
    ]
    with log_path.open("ab") as log_file:
        proc = subprocess.Popen(
            command,
            cwd=str(Path.cwd()),
            env=env,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    pid_path.write_text(str(proc.pid), encoding="utf-8")
    return {"started": True, "already_running": False, "pid": proc.pid}


def _runtime_guard_events_from_metrics(metrics: dict[str, Any]) -> list[dict[str, Any]]:
    rows = metrics.get("recent_trades")
    if not isinstance(rows, list):
        return []
    events: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        trade_time_ms = _safe_int(row.get("time"))
        if trade_time_ms <= 0:
            continue
        commission_quote = _safe_float(row.get("commission_quote"))
        realized_pnl = _safe_float(row.get("realized_pnl"))
        recycle_loss_abs = abs(realized_pnl) if "recycle" in str(row.get("role", "")).lower() and realized_pnl < 0 else 0.0
        events.append(
            {
                "ts": datetime.fromtimestamp(trade_time_ms / 1000.0, tz=timezone.utc).isoformat(),
                "realized_pnl": realized_pnl,
                "commission_quote": commission_quote,
                "recycle_loss_abs": recycle_loss_abs,
            }
        )
    return events


def _record_trade_metrics(
    *,
    metrics: dict[str, Any],
    trade: dict[str, Any],
    side: str,
    price: float,
    qty: float,
    commission_quote: float,
    commission_asset: str,
    commission_raw: float,
    realized_pnl: float,
    role: str,
) -> None:
    notional = price * qty
    metrics["gross_notional"] = _safe_float(metrics.get("gross_notional")) + notional
    metrics["commission_quote"] = _safe_float(metrics.get("commission_quote")) + commission_quote
    metrics["trade_count"] = _safe_int(metrics.get("trade_count")) + 1
    if _truthy(trade.get("isMaker")):
        metrics["maker_count"] = _safe_int(metrics.get("maker_count")) + 1
    if side == "BUY":
        metrics["buy_notional"] = _safe_float(metrics.get("buy_notional")) + notional
        metrics["buy_count"] = _safe_int(metrics.get("buy_count")) + 1
    else:
        metrics["sell_notional"] = _safe_float(metrics.get("sell_notional")) + notional
        metrics["sell_count"] = _safe_int(metrics.get("sell_count")) + 1
    metrics["realized_pnl"] = _safe_float(metrics.get("realized_pnl")) + realized_pnl
    if "recycle" in role:
        metrics["recycle_realized_pnl"] = _safe_float(metrics.get("recycle_realized_pnl")) + realized_pnl
        if realized_pnl < 0:
            metrics["recycle_loss_abs"] = _safe_float(metrics.get("recycle_loss_abs")) + abs(realized_pnl)
    raw = metrics.get("commission_raw_by_asset")
    if not isinstance(raw, dict):
        raw = {}
    if commission_raw > 0 and commission_asset:
        raw[commission_asset] = _safe_float(raw.get(commission_asset)) + commission_raw
    metrics["commission_raw_by_asset"] = raw
    trade_time_ms = _safe_int(trade.get("time"))
    if trade_time_ms > 0:
        if _safe_int(metrics.get("first_trade_time_ms")) <= 0:
            metrics["first_trade_time_ms"] = trade_time_ms
        metrics["last_trade_time_ms"] = max(_safe_int(metrics.get("last_trade_time_ms")), trade_time_ms)
    _append_recent_trade(
        metrics,
        {
            "id": _safe_int(trade.get("id")),
            "time": trade_time_ms,
            "side": side,
            "price": price,
            "qty": qty,
            "notional": notional,
            "commission_quote": commission_quote,
            "commission": commission_raw,
            "commission_asset": commission_asset,
            "maker": bool(trade.get("isMaker")),
            "realized_pnl": realized_pnl,
            "role": role,
        },
    )


def _sync_static_trades(
    *,
    state: dict[str, Any],
    cells_by_index: dict[int, dict[str, Any]],
    trades: list[dict[str, Any]],
    base_asset: str,
    quote_asset: str,
) -> int:
    seen_ids = {int(x) for x in state.get("seen_trade_ids", []) if str(x).strip().isdigit()}
    known_orders = state.get("known_orders", {})
    if not isinstance(known_orders, dict):
        known_orders = {}
        state["known_orders"] = known_orders
    metrics = state.get("metrics")
    if not isinstance(metrics, dict):
        metrics = _new_metrics()
        state["metrics"] = metrics
    last_trade_time_ms = int(state.get("last_trade_time_ms") or 0)
    applied = 0
    for trade in sorted(trades, key=lambda row: (int(row.get("time", 0) or 0), int(row.get("id", 0) or 0))):
        trade_id = int(trade.get("id", 0) or 0)
        if trade_id <= 0 or trade_id in seen_ids:
            continue
        order_id = str(int(trade.get("orderId", 0) or 0))
        meta = known_orders.get(order_id)
        if not isinstance(meta, dict):
            continue
        cell_idx = int(meta.get("cell_idx", 0) or 0)
        cell = cells_by_index.get(cell_idx)
        if cell is None:
            continue
        cell_state = state["cells"].setdefault(str(cell_idx), {"position_qty": 0.0})
        current_qty = max(_safe_float(cell_state.get("position_qty")), 0.0)
        side = str(meta.get("side", "")).upper().strip()
        trade_qty = max(_safe_float(trade.get("qty")), 0.0)
        commission = max(_safe_float(trade.get("commission")), 0.0)
        commission_asset = str(trade.get("commissionAsset", "")).upper().strip()
        commission_quote = _normalize_commission_quote(
            commission=commission,
            commission_asset=commission_asset,
            price=max(_safe_float(trade.get("price")), 0.0),
            base_asset=base_asset,
            quote_asset=quote_asset,
        )
        delta_qty = trade_qty
        if commission_asset == base_asset:
            if side == "BUY":
                delta_qty = max(trade_qty - commission, 0.0)
            elif side == "SELL":
                delta_qty = trade_qty + commission
        if side == "BUY":
            current_qty = min(float(cell["target_qty"]), current_qty + delta_qty)
            realized_pnl = -commission_quote
        elif side == "SELL":
            sold_qty = min(current_qty, delta_qty)
            current_qty = max(0.0, current_qty - delta_qty)
            realized_pnl = sold_qty * max(_safe_float(trade.get("price")) - float(cell["buy_price"]), 0.0) - commission_quote
        else:
            continue
        cell_state["position_qty"] = current_qty
        _record_trade_metrics(
            metrics=metrics,
            trade=trade,
            side=side,
            price=max(_safe_float(trade.get("price")), 0.0),
            qty=trade_qty,
            commission_quote=commission_quote,
            commission_asset=commission_asset,
            commission_raw=commission,
            realized_pnl=realized_pnl,
            role=str(meta.get("role", "") or "static"),
        )
        seen_ids.add(trade_id)
        trade_time_ms = int(trade.get("time", 0) or 0)
        if trade_time_ms > last_trade_time_ms:
            last_trade_time_ms = trade_time_ms
        applied += 1

    trimmed_seen = sorted(seen_ids)[-5000:]
    state["seen_trade_ids"] = trimmed_seen
    state["last_trade_time_ms"] = last_trade_time_ms
    now_ms = int(time.time() * 1000)
    cutoff_ms = now_ms - 7 * 24 * 3600 * 1000
    state["known_orders"] = {
        key: value
        for key, value in known_orders.items()
        if int((value or {}).get("created_at_ms", now_ms) or now_ms) >= cutoff_ms
    }
    return applied


def _sync_volume_shift_trades(
    *,
    state: dict[str, Any],
    trades: list[dict[str, Any]],
    base_asset: str,
    quote_asset: str,
) -> int:
    seen_ids = {int(x) for x in state.get("seen_trade_ids", []) if str(x).strip().isdigit()}
    known_orders = state.get("known_orders", {})
    if not isinstance(known_orders, dict):
        known_orders = {}
        state["known_orders"] = known_orders
    metrics = state.get("metrics")
    if not isinstance(metrics, dict):
        metrics = _new_metrics()
        state["metrics"] = metrics
    lots = state.get("inventory_lots")
    if not isinstance(lots, list):
        lots = []
    last_trade_time_ms = int(state.get("last_trade_time_ms") or 0)
    applied = 0
    for trade in sorted(trades, key=lambda row: (int(row.get("time", 0) or 0), int(row.get("id", 0) or 0))):
        trade_id = int(trade.get("id", 0) or 0)
        if trade_id <= 0 or trade_id in seen_ids:
            continue
        order_id = str(int(trade.get("orderId", 0) or 0))
        meta = known_orders.get(order_id)
        if not isinstance(meta, dict):
            continue
        side = str(meta.get("side", trade.get("isBuyer") and "BUY" or "SELL")).upper().strip()
        trade_qty = max(_safe_float(trade.get("qty")), 0.0)
        price = max(_safe_float(trade.get("price")), 0.0)
        commission = max(_safe_float(trade.get("commission")), 0.0)
        commission_asset = str(trade.get("commissionAsset", "")).upper().strip()
        commission_quote = _normalize_commission_quote(
            commission=commission,
            commission_asset=commission_asset,
            price=price,
            base_asset=base_asset,
            quote_asset=quote_asset,
        )
        role = str(meta.get("role", "") or "")
        realized_pnl = -commission_quote
        if side == "BUY":
            received_qty = trade_qty
            if commission_asset == base_asset:
                received_qty = max(trade_qty - commission, 0.0)
            cost_quote = price * trade_qty + commission_quote
            if received_qty > EPSILON:
                lots.append(
                    {
                        "qty": received_qty,
                        "cost_quote": cost_quote,
                        "buy_time_ms": _safe_int(trade.get("time")),
                        "tag": role or "buy",
                    }
                )
        elif side == "SELL":
            consume_qty = trade_qty + (commission if commission_asset == base_asset else 0.0)
            lots, consumed_cost = _consume_inventory_lots(lots, consume_qty)
            proceeds_quote = price * trade_qty - commission_quote
            realized_pnl = proceeds_quote - consumed_cost
        else:
            continue
        _record_trade_metrics(
            metrics=metrics,
            trade=trade,
            side=side,
            price=price,
            qty=trade_qty,
            commission_quote=commission_quote,
            commission_asset=commission_asset,
            commission_raw=commission,
            realized_pnl=realized_pnl,
            role=role,
        )
        seen_ids.add(trade_id)
        trade_time_ms = int(trade.get("time", 0) or 0)
        if trade_time_ms > last_trade_time_ms:
            last_trade_time_ms = trade_time_ms
        applied += 1
    state["inventory_lots"] = lots
    state["seen_trade_ids"] = sorted(seen_ids)[-5000:]
    state["last_trade_time_ms"] = last_trade_time_ms
    now_ms = int(time.time() * 1000)
    cutoff_ms = now_ms - 7 * 24 * 3600 * 1000
    state["known_orders"] = {
        key: value
        for key, value in known_orders.items()
        if int((value or {}).get("created_at_ms", now_ms) or now_ms) >= cutoff_ms
    }
    return applied


def _build_static_desired_orders(
    *,
    cells: list[dict[str, Any]],
    state: dict[str, Any],
    bid_price: float,
    ask_price: float,
) -> list[dict[str, Any]]:
    desired: list[dict[str, Any]] = []
    for cell in cells:
        cell_idx = int(cell["idx"])
        cell_state = state["cells"].setdefault(str(cell_idx), {"position_qty": 0.0})
        position_qty = max(_safe_float(cell_state.get("position_qty")), 0.0)
        if position_qty <= EPSILON:
            if float(cell["buy_price"]) >= ask_price:
                continue
            desired.append(
                {
                    "side": "BUY",
                    "price": float(cell["buy_price"]),
                    "qty": float(cell["target_qty"]),
                    "cell_idx": cell_idx,
                    "role": "entry",
                }
            )
            continue
        sell_qty = _round_order_qty(min(position_qty, float(cell["target_qty"])), cell.get("step_size"))
        if sell_qty <= EPSILON:
            continue
        if float(cell["sell_price"]) <= bid_price:
            continue
        desired.append(
            {
                "side": "SELL",
                "price": float(cell["sell_price"]),
                "qty": sell_qty,
                "cell_idx": cell_idx,
                "role": "take_profit",
            }
        )
    return desired


def _assess_spot_market_guard(
    *,
    symbol: str,
    buy_pause_amp_trigger_ratio: float | None,
    buy_pause_down_return_trigger_ratio: float | None,
    freeze_shift_abs_return_trigger_ratio: float | None,
    now: datetime | None = None,
) -> dict[str, Any]:
    guard = {
        "enabled": any(
            threshold is not None and threshold != 0
            for threshold in (
                buy_pause_amp_trigger_ratio,
                buy_pause_down_return_trigger_ratio,
                freeze_shift_abs_return_trigger_ratio,
            )
        ),
        "available": False,
        "warning": None,
        "buy_pause_active": False,
        "shift_frozen": False,
        "buy_pause_reasons": [],
        "shift_freeze_reasons": [],
        "candle": None,
        "return_ratio": 0.0,
        "amplitude_ratio": 0.0,
    }
    if not guard["enabled"]:
        return guard
    current_time = now or _utc_now()
    end_ms = int(current_time.timestamp() * 1000)
    start_ms = end_ms - int(timedelta(minutes=3).total_seconds() * 1000)
    try:
        candles = fetch_spot_klines(symbol=symbol, interval="1m", start_ms=start_ms, end_ms=end_ms, limit=10)
    except Exception as exc:
        guard["warning"] = f"{exc.__class__.__name__}: {exc}"
        return guard
    closed_candles = [item for item in candles if item.close_time <= current_time]
    if not closed_candles:
        guard["warning"] = "no_closed_1m_candle"
        return guard
    candle = closed_candles[-1]
    open_price = max(float(candle.open), 0.0)
    high_price = max(float(candle.high), 0.0)
    low_price = max(float(candle.low), 0.0)
    close_price = max(float(candle.close), 0.0)
    return_ratio = ((close_price / open_price) - 1.0) if open_price > 0 else 0.0
    amplitude_ratio = ((high_price / low_price) - 1.0) if low_price > 0 else 0.0

    buy_pause_reasons: list[str] = []
    shift_freeze_reasons: list[str] = []
    if (
        buy_pause_amp_trigger_ratio is not None
        and buy_pause_amp_trigger_ratio > 0
        and buy_pause_down_return_trigger_ratio is not None
        and amplitude_ratio >= buy_pause_amp_trigger_ratio
        and return_ratio <= buy_pause_down_return_trigger_ratio
    ):
        buy_pause_reasons.append(f"1m_extreme_down_candle amp={amplitude_ratio * 100:.2f}% ret={return_ratio * 100:.2f}%")
    if (
        freeze_shift_abs_return_trigger_ratio is not None
        and freeze_shift_abs_return_trigger_ratio > 0
        and abs(return_ratio) >= freeze_shift_abs_return_trigger_ratio
    ):
        shift_freeze_reasons.append(f"1m_shift_freeze abs_ret={abs(return_ratio) * 100:.2f}%")
    guard.update(
        {
            "available": True,
            "buy_pause_active": bool(buy_pause_reasons),
            "shift_frozen": bool(shift_freeze_reasons),
            "buy_pause_reasons": buy_pause_reasons,
            "shift_freeze_reasons": shift_freeze_reasons,
            "candle": {
                "open_time": candle.open_time.isoformat(),
                "close_time": candle.close_time.isoformat(),
                "open": open_price,
                "high": high_price,
                "low": low_price,
                "close": close_price,
            },
            "return_ratio": return_ratio,
            "amplitude_ratio": amplitude_ratio,
        }
    )
    return guard


def _band_prices(*, center_price: float, levels: int, band_ratio: float, tick_size: float | None, side: str) -> list[float]:
    if center_price <= 0 or levels <= 0 or band_ratio <= 0:
        return []
    step_ratio = band_ratio / levels
    prices: list[float] = []
    for idx in range(1, levels + 1):
        raw = center_price * (1.0 - step_ratio * idx if side == "BUY" else 1.0 + step_ratio * idx)
        prices.append(_round_order_price(raw, tick_size, side))
    return prices


def _normalize_order(
    *,
    side: str,
    price: float,
    qty: float,
    step_size: float | None,
    min_qty: float | None,
    min_notional: float | None,
    tag: str,
    role: str,
) -> dict[str, Any] | None:
    rounded_qty = _round_order_qty(qty, step_size)
    notional = rounded_qty * price
    if rounded_qty <= EPSILON or price <= 0:
        return None
    if min_qty is not None and rounded_qty < min_qty:
        return None
    if min_notional is not None and notional < min_notional:
        return None
    return {
        "side": side,
        "price": price,
        "qty": rounded_qty,
        "tag": tag,
        "role": role,
    }


def _update_center_price(
    *,
    state: dict[str, Any],
    mid_price: float,
    tick_size: float | None,
    center_shift_trigger_ratio: float,
    center_shift_confirm_cycles: int,
    center_shift_step_ratio: float,
    shift_frozen: bool,
) -> tuple[float, list[dict[str, Any]]]:
    current_center = max(_safe_float(state.get("center_price")), 0.0)
    if current_center <= 0:
        current_center = _round_order_price(mid_price, tick_size, "BUY")
        state["center_price"] = current_center
        state["center_shift_down_cycles"] = 0
        state["center_shift_up_cycles"] = 0
        return current_center, []
    if shift_frozen:
        state["center_shift_down_cycles"] = 0
        state["center_shift_up_cycles"] = 0
        return current_center, []

    down_threshold = current_center * (1.0 - center_shift_trigger_ratio)
    up_threshold = current_center * (1.0 + center_shift_trigger_ratio)
    moves: list[dict[str, Any]] = []

    if mid_price <= down_threshold:
        state["center_shift_down_cycles"] = _safe_int(state.get("center_shift_down_cycles")) + 1
    else:
        state["center_shift_down_cycles"] = 0
    if mid_price >= up_threshold:
        state["center_shift_up_cycles"] = _safe_int(state.get("center_shift_up_cycles")) + 1
    else:
        state["center_shift_up_cycles"] = 0

    if _safe_int(state.get("center_shift_down_cycles")) >= max(center_shift_confirm_cycles, 1):
        shifted = _round_order_price(current_center * (1.0 - center_shift_step_ratio), tick_size, "BUY")
        if shifted > 0 and abs(shifted - current_center) > EPSILON:
            current_center = shifted
            moves.append({"direction": "down", "new_center_price": current_center})
            state["center_shift_count"] = _safe_int(state.get("center_shift_count")) + 1
        state["center_shift_down_cycles"] = 0
    elif _safe_int(state.get("center_shift_up_cycles")) >= max(center_shift_confirm_cycles * 2, 1):
        shifted = _round_order_price(current_center * (1.0 + center_shift_step_ratio), tick_size, "SELL")
        if shifted > 0 and abs(shifted - current_center) > EPSILON:
            current_center = shifted
            moves.append({"direction": "up", "new_center_price": current_center})
            state["center_shift_count"] = _safe_int(state.get("center_shift_count")) + 1
        state["center_shift_up_cycles"] = 0

    state["center_price"] = current_center
    return current_center, moves


def _resolve_volume_shift_mode(
    *,
    inventory_notional: float,
    soft_limit: float,
    hard_limit: float,
    oldest_age_minutes: float,
    recycle_age_minutes: float,
    market_guard: dict[str, Any],
) -> tuple[str, bool, list[str]]:
    reasons: list[str] = []
    buy_paused = False
    mode = "attack"
    if inventory_notional >= hard_limit > 0:
        mode = "recycle"
        buy_paused = True
        reasons.append("inventory_hard_limit")
    elif recycle_age_minutes > 0 and oldest_age_minutes >= recycle_age_minutes and inventory_notional > 0:
        mode = "recycle"
        buy_paused = True
        reasons.append("inventory_age_limit")
    elif inventory_notional >= soft_limit > 0:
        mode = "defense"
        reasons.append("inventory_soft_limit")
    if market_guard.get("buy_pause_active"):
        buy_paused = True
        if mode == "attack":
            mode = "defense"
        reasons.extend(list(market_guard.get("buy_pause_reasons") or []))
    return mode, buy_paused, reasons


def _build_volume_shift_desired_orders(
    *,
    state: dict[str, Any],
    config: dict[str, Any],
    symbol_info: dict[str, Any],
    bid_price: float,
    ask_price: float,
    mid_price: float,
    market_guard: dict[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    lots = state.get("inventory_lots")
    if not isinstance(lots, list):
        lots = []
    inventory_qty = _sum_inventory_qty(lots)
    inventory_cost_quote = _sum_inventory_cost(lots)
    inventory_avg_cost = (inventory_cost_quote / inventory_qty) if inventory_qty > EPSILON else 0.0
    inventory_notional = inventory_qty * mid_price
    oldest_age_minutes = _oldest_inventory_age_minutes(lots)

    soft_limit = max(_safe_float(config.get("inventory_soft_limit_notional")), 0.0)
    hard_limit = max(_safe_float(config.get("inventory_hard_limit_notional")), soft_limit)
    recycle_age_minutes = max(_safe_float(config.get("inventory_recycle_age_minutes")), 0.0)
    mode, buy_paused, pause_reasons = _resolve_volume_shift_mode(
        inventory_notional=inventory_notional,
        soft_limit=soft_limit,
        hard_limit=hard_limit,
        oldest_age_minutes=oldest_age_minutes,
        recycle_age_minutes=recycle_age_minutes,
        market_guard=market_guard,
    )
    state["last_mode"] = mode

    if mode == "attack":
        buy_levels = max(_safe_int(config.get("attack_buy_levels")), 1)
        sell_levels = max(_safe_int(config.get("attack_sell_levels")), 1)
        per_order_notional = max(_safe_float(config.get("attack_per_order_notional")), 0.0)
    else:
        buy_levels = max(_safe_int(config.get("defense_buy_levels")), 1)
        sell_levels = max(_safe_int(config.get("defense_sell_levels")), 1)
        per_order_notional = max(_safe_float(config.get("defense_per_order_notional")), 0.0)

    center_price, shift_moves = _update_center_price(
        state=state,
        mid_price=mid_price,
        tick_size=symbol_info.get("tick_size"),
        center_shift_trigger_ratio=max(_safe_float(config.get("center_shift_trigger_ratio")), 0.0),
        center_shift_confirm_cycles=max(_safe_int(config.get("center_shift_confirm_cycles")), 1),
        center_shift_step_ratio=max(_safe_float(config.get("center_shift_step_ratio")), 0.0),
        shift_frozen=bool(market_guard.get("shift_frozen")),
    )
    center_price = max(center_price, mid_price)
    band_ratio = max(_safe_float(config.get("grid_band_ratio")), 0.0)
    tick_size = symbol_info.get("tick_size")
    step_size = symbol_info.get("step_size")
    min_qty = symbol_info.get("min_qty")
    min_notional = symbol_info.get("min_notional")

    buy_prices = _band_prices(center_price=center_price, levels=buy_levels, band_ratio=band_ratio, tick_size=tick_size, side="BUY")
    sell_prices = _band_prices(center_price=max(center_price, mid_price), levels=sell_levels, band_ratio=band_ratio * (0.5 if mode == "recycle" else 1.0), tick_size=tick_size, side="SELL")

    desired: list[dict[str, Any]] = []
    target_cap = max(hard_limit, per_order_notional)
    if target_cap <= 0:
        target_cap = max(inventory_notional, 0.0)
    running_inventory_notional = inventory_notional
    for idx, price in enumerate(buy_prices, start=1):
        if buy_paused:
            break
        if price <= 0 or price >= ask_price:
            continue
        target_notional = target_cap * (idx / max(len(buy_prices), 1))
        gap_notional = max(target_notional - running_inventory_notional, 0.0)
        order_notional = min(gap_notional, per_order_notional)
        if order_notional <= 0:
            continue
        order = _normalize_order(
            side="BUY",
            price=price,
            qty=order_notional / price,
            step_size=step_size,
            min_qty=min_qty,
            min_notional=min_notional,
            tag=f"atkbuy{idx}" if mode == "attack" else f"dfbuy{idx}",
            role="inventory_build" if mode == "attack" else "defense_buy",
        )
        if order:
            desired.append(order)
            running_inventory_notional += order["qty"] * price

    available_sell_qty = inventory_qty
    recycle_loss_tolerance_ratio = max(_safe_float(config.get("inventory_recycle_loss_tolerance_ratio")), 0.0)
    recycle_min_profit_ratio = max(_safe_float(config.get("inventory_recycle_min_profit_ratio")), 0.0)
    if available_sell_qty > EPSILON and sell_prices:
        per_sell_qty = available_sell_qty / len(sell_prices)
        min_recycle_price = inventory_avg_cost * (1.0 - recycle_loss_tolerance_ratio) if inventory_avg_cost > 0 else 0.0
        normal_profit_floor = inventory_avg_cost * (1.0 + recycle_min_profit_ratio) if inventory_avg_cost > 0 else 0.0
        for idx, raw_price in enumerate(sell_prices, start=1):
            if available_sell_qty <= EPSILON:
                break
            price = raw_price
            role = "take_profit"
            if mode == "recycle":
                role = "recycle_sell"
                price = max(ask_price, min_recycle_price)
            elif mode == "defense":
                role = "defense_sell"
                price = max(ask_price, min(raw_price, max(normal_profit_floor, ask_price)))
            else:
                if normal_profit_floor > 0:
                    price = max(raw_price, normal_profit_floor)
                price = max(price, ask_price)
            order_qty = min(available_sell_qty, per_sell_qty if idx < len(sell_prices) else available_sell_qty)
            order = _normalize_order(
                side="SELL",
                price=_round_order_price(price, tick_size, "SELL"),
                qty=order_qty,
                step_size=step_size,
                min_qty=min_qty,
                min_notional=min_notional,
                tag=f"sell{idx}",
                role=role,
            )
            if order and order["price"] > bid_price:
                desired.append(order)
                available_sell_qty -= order["qty"]

    controls = {
        "mode": mode,
        "buy_paused": buy_paused,
        "pause_reasons": pause_reasons,
        "shift_frozen": bool(market_guard.get("shift_frozen")),
        "market_guard_return_ratio": _safe_float(market_guard.get("return_ratio")),
        "market_guard_amplitude_ratio": _safe_float(market_guard.get("amplitude_ratio")),
        "center_price": center_price,
        "center_shift_count": _safe_int(state.get("center_shift_count")),
        "shift_moves": shift_moves,
        "inventory_qty": inventory_qty,
        "inventory_notional": inventory_notional,
        "inventory_avg_cost": inventory_avg_cost,
        "oldest_inventory_age_minutes": oldest_age_minutes,
        "inventory_soft_limit_notional": soft_limit,
        "inventory_hard_limit_notional": hard_limit,
        "effective_buy_levels": buy_levels,
        "effective_sell_levels": sell_levels,
        "effective_per_order_notional": per_order_notional,
    }
    return desired, controls


def _build_spot_competition_inventory_grid_orders(
    *,
    state: dict[str, Any],
    trades: list[dict[str, Any]],
    bid_price: float,
    ask_price: float,
    step_price: float,
    first_order_multiplier: float,
    per_order_notional: float,
    threshold_position_notional: float,
    max_order_position_notional: float,
    max_position_notional: float,
    tick_size: float | None,
    step_size: float | None,
    min_qty: float | None,
    min_notional: float | None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    known_orders = state.get("known_orders")
    order_refs = known_orders if isinstance(known_orders, dict) else {}
    strategy_trades = [
        trade
        for trade in list(trades or [])
        if isinstance(trade, dict) and str(int(trade.get("orderId", 0) or 0)) in order_refs
    ]
    inventory_lots = state.get("inventory_lots")
    current_position_qty = _sum_inventory_qty(inventory_lots if isinstance(inventory_lots, list) else [])
    runtime = rebuild_inventory_grid_runtime(
        market_type="spot",
        trades=strategy_trades,
        order_refs=order_refs,
        step_price=step_price,
        current_position_qty=current_position_qty,
    )
    if _safe_float(runtime.get("grid_anchor_price")) <= 0:
        runtime["grid_anchor_price"] = (max(float(bid_price), 0.0) + max(float(ask_price), 0.0)) / 2.0

    plan = build_inventory_grid_orders(
        runtime=runtime,
        bid_price=bid_price,
        ask_price=ask_price,
        step_price=step_price,
        per_order_notional=per_order_notional,
        first_order_multiplier=first_order_multiplier,
        threshold_position_notional=threshold_position_notional,
        max_order_position_notional=max_order_position_notional,
        max_position_notional=max_position_notional,
        tick_size=tick_size,
        step_size=step_size,
        min_qty=min_qty,
        min_notional=min_notional,
    )
    desired_orders = list(plan.get("bootstrap_orders") or []) + list(plan.get("buy_orders") or []) + list(plan.get("sell_orders") or [])
    controls = {
        "mode": "competition_inventory_grid",
        "buy_paused": False,
        "pause_reasons": [],
        "shift_frozen": False,
        "market_guard_return_ratio": 0.0,
        "market_guard_amplitude_ratio": 0.0,
        "center_price": _safe_float(runtime.get("grid_anchor_price")),
        "center_shift_count": 0,
        "inventory_soft_limit_notional": _safe_float(threshold_position_notional),
        "inventory_hard_limit_notional": _safe_float(max_position_notional),
        "effective_buy_levels": sum(1 for order in desired_orders if str(order.get("side", "")).upper() == "BUY"),
        "effective_sell_levels": sum(1 for order in desired_orders if str(order.get("side", "")).upper() == "SELL"),
        "effective_per_order_notional": _safe_float(per_order_notional),
        "direction_state": str(runtime.get("direction_state", "flat") or "flat"),
        "risk_state": str(plan.get("risk_state", "normal") or "normal"),
        "grid_anchor_price": _safe_float(runtime.get("grid_anchor_price")),
        "pair_credit_steps": _safe_int(runtime.get("pair_credit_steps")),
    }
    return desired_orders, controls


def _strategy_open_orders(open_orders: list[dict[str, Any]], prefix: str) -> list[dict[str, Any]]:
    return [order for order in open_orders if isinstance(order, dict) and _is_strategy_order(order, prefix)]


def _available_new_funds(
    account_info: dict[str, Any],
    base_asset: str,
    quote_asset: str,
) -> tuple[float, float]:
    quote_free, _ = _extract_balance(account_info, quote_asset)
    base_free, _ = _extract_balance(account_info, base_asset)
    return quote_free, base_free


def _cancel_orders(
    *,
    symbol: str,
    api_key: str,
    api_secret: str,
    orders: list[dict[str, Any]],
) -> int:
    count = 0
    for order in orders:
        order_id_raw = order.get("orderId")
        if order_id_raw is None:
            continue
        delete_spot_order(
            symbol=symbol,
            api_key=api_key,
            api_secret=api_secret,
            order_id=int(order_id_raw),
        )
        count += 1
    return count


def _build_runtime_guard_summary(
    *,
    args: argparse.Namespace,
    state: dict[str, Any],
    metrics: dict[str, Any],
    symbol: str,
    strategy_mode: str,
    base_asset: str,
    quote_asset: str,
    bid_price: float,
    ask_price: float,
    mid_price: float,
    quote_free: float,
    base_free: float,
    runtime_guard_result: Any,
    runtime_guard_config: Any,
    canceled_count: int,
    flatten_result: dict[str, Any],
) -> dict[str, Any]:
    inventory_qty = _sum_inventory_qty(state.get("inventory_lots") if isinstance(state.get("inventory_lots"), list) else [])
    inventory_cost_quote = _sum_inventory_cost(state.get("inventory_lots") if isinstance(state.get("inventory_lots"), list) else [])
    inventory_avg_cost = (inventory_cost_quote / inventory_qty) if inventory_qty > EPSILON else 0.0
    unrealized_pnl = (mid_price - inventory_avg_cost) * inventory_qty if inventory_qty > EPSILON else 0.0
    return {
        "ts": _utc_now().isoformat(),
        "cycle": int(state.get("cycle", 0) or 0),
        "symbol": symbol,
        "strategy_mode": strategy_mode,
        "mid_price": mid_price,
        "bid_price": bid_price,
        "ask_price": ask_price,
        "grid_count": 0,
        "active_buy_orders": 0,
        "active_sell_orders": 0,
        "managed_base_qty": inventory_qty,
        "quote_free": quote_free,
        "base_free": base_free,
        "quote_asset": quote_asset,
        "base_asset": base_asset,
        "applied_trades": 0,
        "placed_count": 0,
        "canceled_count": canceled_count,
        "open_strategy_orders": 0,
        "placement_errors": [],
        "gross_notional": _safe_float(metrics.get("gross_notional")),
        "buy_notional": _safe_float(metrics.get("buy_notional")),
        "sell_notional": _safe_float(metrics.get("sell_notional")),
        "commission_quote": _safe_float(metrics.get("commission_quote")),
        "realized_pnl": _safe_float(metrics.get("realized_pnl")),
        "recycle_realized_pnl": _safe_float(metrics.get("recycle_realized_pnl")),
        "recycle_loss_abs": _safe_float(metrics.get("recycle_loss_abs")),
        "unrealized_pnl": unrealized_pnl,
        "net_pnl_estimate": _safe_float(metrics.get("realized_pnl")) + unrealized_pnl,
        "trade_count": _safe_int(metrics.get("trade_count")),
        "maker_count": _safe_int(metrics.get("maker_count")),
        "buy_count": _safe_int(metrics.get("buy_count")),
        "sell_count": _safe_int(metrics.get("sell_count")),
        "inventory_qty": inventory_qty,
        "inventory_notional": inventory_qty * mid_price,
        "inventory_avg_cost": inventory_avg_cost,
        "oldest_inventory_age_minutes": _oldest_inventory_age_minutes(state.get("inventory_lots") if isinstance(state.get("inventory_lots"), list) else []),
        "mode": "runtime_guard",
        "buy_paused": False,
        "pause_reasons": [],
        "shift_frozen": False,
        "center_price": _safe_float(state.get("center_price")),
        "center_shift_count": _safe_int(state.get("center_shift_count")),
        "market_guard_return_ratio": 0.0,
        "market_guard_amplitude_ratio": 0.0,
        "inventory_soft_limit_notional": _safe_float(getattr(args, "inventory_soft_limit_notional", 0.0)),
        "inventory_hard_limit_notional": _safe_float(getattr(args, "inventory_hard_limit_notional", 0.0)),
        "effective_buy_levels": 0,
        "effective_sell_levels": 0,
        "effective_per_order_notional": 0.0,
        "shift_moves": [],
        "runtime_status": runtime_guard_result.runtime_status,
        "stop_triggered": runtime_guard_result.stop_triggered,
        "stop_reason": runtime_guard_result.primary_reason,
        "stop_reasons": runtime_guard_result.matched_reasons,
        "stop_triggered_at": runtime_guard_result.triggered_at,
        "run_start_time": runtime_guard_config.run_start_time.isoformat() if runtime_guard_config.run_start_time else None,
        "run_end_time": runtime_guard_config.run_end_time.isoformat() if runtime_guard_config.run_end_time else None,
        "rolling_hourly_loss": runtime_guard_result.rolling_hourly_loss,
        "rolling_hourly_loss_limit": runtime_guard_config.rolling_hourly_loss_limit,
        "cumulative_gross_notional": runtime_guard_result.cumulative_gross_notional,
        "max_cumulative_notional": runtime_guard_config.max_cumulative_notional,
        "flatten_started": bool(flatten_result.get("started")),
        "flatten_already_running": bool(flatten_result.get("already_running")),
    }


def _run_cycle(args: argparse.Namespace, symbol_info: dict[str, Any], api_key: str, api_secret: str) -> dict[str, Any]:
    symbol = str(args.symbol).upper().strip()
    strategy_mode = str(args.strategy_mode or "spot_one_way_long").strip()
    base_asset = str(symbol_info.get("base_asset") or "").upper().strip()
    quote_asset = str(symbol_info.get("quote_asset") or "").upper().strip()
    if not base_asset or not quote_asset:
        raise RuntimeError("spot symbol config missing base/quote asset")

    book_rows = fetch_spot_book_tickers(symbol=symbol)
    if not book_rows:
        raise RuntimeError("spot bookTicker returned empty")
    book = book_rows[0]
    bid_price = float(book["bid_price"])
    ask_price = float(book["ask_price"])
    if bid_price <= 0 or ask_price <= 0:
        raise RuntimeError("invalid spot bid/ask price")
    mid_price = (bid_price + ask_price) / 2.0

    cells: list[dict[str, Any]] = []
    cells_by_index: dict[int, dict[str, Any]] = {}
    if strategy_mode == "spot_one_way_long":
        cells = _build_grid_cells(vars(args), symbol_info)
        cells_by_index = {int(cell["idx"]): cell for cell in cells}
    state_path = Path(args.state_path)
    state = _load_state(state_path, symbol, strategy_mode, len(cells))
    state["cycle"] = int(state.get("cycle", 0) or 0) + 1
    state["symbol"] = symbol
    state["strategy_mode"] = strategy_mode

    trade_start_ms = int(state.get("last_trade_time_ms") or 0)
    if trade_start_ms > 0:
        trade_start_ms = max(trade_start_ms - 60_000, 0)
    else:
        trade_start_ms = max(int(time.time() * 1000) - 48 * 3600 * 1000, 0)

    trades = fetch_spot_user_trades(
        symbol=symbol,
        api_key=api_key,
        api_secret=api_secret,
        start_time_ms=trade_start_ms,
        limit=1000,
    )
    if strategy_mode == "spot_one_way_long":
        applied_trades = _sync_static_trades(
            state=state,
            cells_by_index=cells_by_index,
            trades=trades,
            base_asset=base_asset,
            quote_asset=quote_asset,
        )
        controls = {
            "mode": "static",
            "buy_paused": False,
            "pause_reasons": [],
            "shift_frozen": False,
            "market_guard_return_ratio": 0.0,
            "market_guard_amplitude_ratio": 0.0,
            "center_price": 0.0,
            "center_shift_count": 0,
            "shift_moves": [],
            "inventory_qty": sum(max(_safe_float((state["cells"].get(str(cell["idx"])) or {}).get("position_qty")), 0.0) for cell in cells),
            "inventory_notional": 0.0,
            "inventory_avg_cost": 0.0,
            "oldest_inventory_age_minutes": 0.0,
            "inventory_soft_limit_notional": 0.0,
            "inventory_hard_limit_notional": 0.0,
            "effective_buy_levels": len(cells),
            "effective_sell_levels": len(cells),
            "effective_per_order_notional": max(float(args.total_quote_budget), 0.0) / max(int(args.n), 1),
        }
    elif strategy_mode == "spot_volume_shift_long":
        applied_trades = _sync_volume_shift_trades(
            state=state,
            trades=trades,
            base_asset=base_asset,
            quote_asset=quote_asset,
        )
        market_guard = _assess_spot_market_guard(
            symbol=symbol,
            buy_pause_amp_trigger_ratio=args.buy_pause_amp_trigger_ratio,
            buy_pause_down_return_trigger_ratio=args.buy_pause_down_return_trigger_ratio,
            freeze_shift_abs_return_trigger_ratio=args.freeze_shift_abs_return_trigger_ratio,
        )
        controls = {}
    else:
        applied_trades = _sync_volume_shift_trades(
            state=state,
            trades=trades,
            base_asset=base_asset,
            quote_asset=quote_asset,
        )
        controls = {}

    account_info = fetch_spot_account_info(api_key, api_secret)
    open_orders = fetch_spot_open_orders(symbol, api_key, api_secret)
    strategy_open_orders = _strategy_open_orders(open_orders, str(args.client_order_prefix))
    metrics = state.get("metrics") if isinstance(state.get("metrics"), dict) else _new_metrics()
    runtime_guard_config = normalize_runtime_guard_config(vars(args))
    runtime_guard_result = evaluate_runtime_guards(
        config=runtime_guard_config,
        now=_utc_now(),
        cumulative_gross_notional=_safe_float(metrics.get("gross_notional")),
        pnl_events=_runtime_guard_events_from_metrics(metrics),
    )
    if not runtime_guard_result.tradable:
        canceled_count = 0
        if strategy_open_orders:
            canceled_count = _cancel_orders(
                symbol=symbol,
                api_key=api_key,
                api_secret=api_secret,
                orders=strategy_open_orders,
            )
        flatten_result = {"started": False, "already_running": False}
        flatten_snapshot = load_live_spot_flatten_snapshot(symbol, api_key, api_secret)
        if list(flatten_snapshot.get("orders", [])):
            flatten_result = _start_spot_flatten_process(symbol)
        quote_free, base_free = _available_new_funds(account_info, base_asset, quote_asset)
        summary = _build_runtime_guard_summary(
            args=args,
            state=state,
            metrics=metrics,
            symbol=symbol,
            strategy_mode=strategy_mode,
            base_asset=base_asset,
            quote_asset=quote_asset,
            bid_price=bid_price,
            ask_price=ask_price,
            mid_price=mid_price,
            quote_free=quote_free,
            base_free=base_free,
            runtime_guard_result=runtime_guard_result,
            runtime_guard_config=runtime_guard_config,
            canceled_count=canceled_count,
            flatten_result=flatten_result,
        )
        _save_state(state_path, state)
        _append_jsonl(Path(args.summary_jsonl), summary)
        return summary

    if strategy_mode == "spot_one_way_long":
        desired_orders = _build_static_desired_orders(
            cells=cells,
            state=state,
            bid_price=bid_price,
            ask_price=ask_price,
        )
    elif strategy_mode == "spot_volume_shift_long":
        desired_orders, controls = _build_volume_shift_desired_orders(
            state=state,
            config=vars(args),
            symbol_info=symbol_info,
            bid_price=bid_price,
            ask_price=ask_price,
            mid_price=mid_price,
            market_guard=market_guard,
        )
    else:
        desired_orders, controls = _build_spot_competition_inventory_grid_orders(
            state=state,
            trades=trades,
            bid_price=bid_price,
            ask_price=ask_price,
            step_price=float(args.step_price),
            first_order_multiplier=float(args.first_order_multiplier),
            per_order_notional=float(args.per_order_notional),
            threshold_position_notional=float(args.threshold_position_notional),
            max_order_position_notional=float(args.max_order_position_notional),
            max_position_notional=float(args.max_position_notional),
            tick_size=symbol_info.get("tick_size"),
            step_size=symbol_info.get("step_size"),
            min_qty=symbol_info.get("min_qty"),
            min_notional=symbol_info.get("min_notional"),
        )
    diff = diff_open_orders(existing_orders=strategy_open_orders, desired_orders=desired_orders)

    canceled_count = 0
    if _truthy(args.cancel_stale) and diff["stale_orders"]:
        canceled_count = _cancel_orders(
            symbol=symbol,
            api_key=api_key,
            api_secret=api_secret,
            orders=diff["stale_orders"],
        )

    placed_count = 0
    placement_errors: list[str] = []
    quote_free, base_free = _available_new_funds(account_info, base_asset, quote_asset)
    remaining_quote_free = quote_free
    remaining_base_free = base_free
    max_single_cycle_new_orders = max(_safe_int(getattr(args, "max_single_cycle_new_orders", 0)), 0)
    for order in diff["missing_orders"]:
        if max_single_cycle_new_orders > 0 and placed_count >= max_single_cycle_new_orders:
            break
        side = str(order["side"]).upper().strip()
        qty = float(order["qty"])
        price = float(order["price"])
        notional = qty * price
        if side == "BUY":
            if notional > remaining_quote_free + 1e-8:
                continue
        else:
            if qty > remaining_base_free + 1e-8:
                continue
        if not _truthy(args.apply):
            placed_count += 1
            if side == "BUY":
                remaining_quote_free = max(0.0, remaining_quote_free - notional)
            else:
                remaining_base_free = max(0.0, remaining_base_free - qty)
            continue
        client_order_id = _build_client_order_id(str(args.client_order_prefix), str(order.get("tag", "ord")), side)
        try:
            placed = post_spot_order(
                symbol=symbol,
                side=side,
                quantity=qty,
                price=price,
                api_key=api_key,
                api_secret=api_secret,
                order_type="LIMIT_MAKER",
                new_client_order_id=client_order_id,
            )
            order_id = int(placed.get("orderId", 0) or 0)
            if order_id > 0:
                state["known_orders"][str(order_id)] = {
                    "cell_idx": int(order.get("cell_idx", 0) or 0),
                    "side": side,
                    "client_order_id": client_order_id,
                    "created_at_ms": int(time.time() * 1000),
                    "role": str(order.get("role", "") or ""),
                    "tag": str(order.get("tag", "") or ""),
                }
            placed_count += 1
            if side == "BUY":
                remaining_quote_free = max(0.0, remaining_quote_free - notional)
            else:
                remaining_base_free = max(0.0, remaining_base_free - qty)
        except Exception as exc:  # pragma: no cover - network/runtime specific
            placement_errors.append(f"{side} {price:.8f} x {qty:.8f}: {type(exc).__name__}: {exc}")

    inventory_qty = _sum_inventory_qty(state.get("inventory_lots") if isinstance(state.get("inventory_lots"), list) else [])
    inventory_cost_quote = _sum_inventory_cost(state.get("inventory_lots") if isinstance(state.get("inventory_lots"), list) else [])
    inventory_avg_cost = (inventory_cost_quote / inventory_qty) if inventory_qty > EPSILON else 0.0
    managed_base_qty = (
        sum(max(_safe_float((state["cells"].get(str(cell["idx"])) or {}).get("position_qty")), 0.0) for cell in cells)
        if strategy_mode == "spot_one_way_long"
        else inventory_qty
    )
    unrealized_pnl = (mid_price - inventory_avg_cost) * inventory_qty if inventory_qty > EPSILON else 0.0
    net_pnl_estimate = _safe_float(metrics.get("realized_pnl")) + unrealized_pnl

    summary = {
        "ts": _utc_now().isoformat(),
        "cycle": int(state.get("cycle", 0) or 0),
        "symbol": symbol,
        "strategy_mode": strategy_mode,
        "mid_price": mid_price,
        "bid_price": bid_price,
        "ask_price": ask_price,
        "grid_count": len(cells),
        "active_buy_orders": sum(1 for item in desired_orders if str(item["side"]).upper() == "BUY"),
        "active_sell_orders": sum(1 for item in desired_orders if str(item["side"]).upper() == "SELL"),
        "managed_base_qty": managed_base_qty,
        "quote_free": quote_free,
        "base_free": base_free,
        "quote_asset": quote_asset,
        "base_asset": base_asset,
        "applied_trades": applied_trades,
        "placed_count": placed_count,
        "canceled_count": canceled_count,
        "open_strategy_orders": len(strategy_open_orders),
        "placement_errors": placement_errors,
        "gross_notional": _safe_float(metrics.get("gross_notional")),
        "buy_notional": _safe_float(metrics.get("buy_notional")),
        "sell_notional": _safe_float(metrics.get("sell_notional")),
        "commission_quote": _safe_float(metrics.get("commission_quote")),
        "realized_pnl": _safe_float(metrics.get("realized_pnl")),
        "recycle_realized_pnl": _safe_float(metrics.get("recycle_realized_pnl")),
        "recycle_loss_abs": _safe_float(metrics.get("recycle_loss_abs")),
        "unrealized_pnl": unrealized_pnl,
        "net_pnl_estimate": net_pnl_estimate,
        "trade_count": _safe_int(metrics.get("trade_count")),
        "maker_count": _safe_int(metrics.get("maker_count")),
        "buy_count": _safe_int(metrics.get("buy_count")),
        "sell_count": _safe_int(metrics.get("sell_count")),
        "inventory_qty": inventory_qty,
        "inventory_notional": inventory_qty * mid_price,
        "inventory_avg_cost": inventory_avg_cost,
        "oldest_inventory_age_minutes": _oldest_inventory_age_minutes(state.get("inventory_lots") if isinstance(state.get("inventory_lots"), list) else []),
        "mode": controls.get("mode", "static"),
        "buy_paused": bool(controls.get("buy_paused")),
        "pause_reasons": list(controls.get("pause_reasons") or []),
        "shift_frozen": bool(controls.get("shift_frozen")),
        "center_price": _safe_float(controls.get("center_price")),
        "center_shift_count": _safe_int(controls.get("center_shift_count")),
        "market_guard_return_ratio": _safe_float(controls.get("market_guard_return_ratio")),
        "market_guard_amplitude_ratio": _safe_float(controls.get("market_guard_amplitude_ratio")),
        "inventory_soft_limit_notional": _safe_float(controls.get("inventory_soft_limit_notional")),
        "inventory_hard_limit_notional": _safe_float(controls.get("inventory_hard_limit_notional")),
        "effective_buy_levels": _safe_int(controls.get("effective_buy_levels")),
        "effective_sell_levels": _safe_int(controls.get("effective_sell_levels")),
        "effective_per_order_notional": _safe_float(controls.get("effective_per_order_notional")),
        "direction_state": str(controls.get("direction_state", "") or ""),
        "risk_state": str(controls.get("risk_state", "") or ""),
        "grid_anchor_price": _safe_float(controls.get("grid_anchor_price")),
        "pair_credit_steps": _safe_int(controls.get("pair_credit_steps")),
        "shift_moves": controls.get("shift_moves", []),
        "runtime_status": runtime_guard_result.runtime_status,
        "stop_triggered": runtime_guard_result.stop_triggered,
        "stop_reason": runtime_guard_result.primary_reason,
        "stop_reasons": runtime_guard_result.matched_reasons,
        "stop_triggered_at": runtime_guard_result.triggered_at,
        "run_start_time": runtime_guard_config.run_start_time.isoformat() if runtime_guard_config.run_start_time else None,
        "run_end_time": runtime_guard_config.run_end_time.isoformat() if runtime_guard_config.run_end_time else None,
        "rolling_hourly_loss": runtime_guard_result.rolling_hourly_loss,
        "rolling_hourly_loss_limit": runtime_guard_config.rolling_hourly_loss_limit,
        "cumulative_gross_notional": runtime_guard_result.cumulative_gross_notional,
        "max_cumulative_notional": runtime_guard_config.max_cumulative_notional,
    }
    _save_state(state_path, state)
    _append_jsonl(Path(args.summary_jsonl), summary)
    return summary


def run_once(args: argparse.Namespace) -> dict[str, Any]:
    creds = load_binance_api_credentials()
    if not creds:
        raise RuntimeError("Binance API credentials are required for spot runner")
    strategy_mode = str(args.strategy_mode or "spot_one_way_long").strip()
    if strategy_mode == "spot_one_way_long":
        if args.min_price <= 0 or args.max_price <= 0 or args.min_price >= args.max_price:
            raise RuntimeError("static spot grid requires valid min/max price")
        if args.n <= 0 or args.total_quote_budget <= 0:
            raise RuntimeError("static spot grid requires n > 0 and total_quote_budget > 0")
    elif strategy_mode == "spot_volume_shift_long":
        if args.total_quote_budget <= 0:
            raise RuntimeError("spot volume-shift runner requires total_quote_budget > 0")
        if args.inventory_hard_limit_notional <= 0:
            raise RuntimeError("inventory_hard_limit_notional must be > 0")
    else:
        if args.step_price <= 0:
            raise RuntimeError("spot competition inventory grid requires step_price > 0")
        if args.per_order_notional <= 0:
            raise RuntimeError("spot competition inventory grid requires per_order_notional > 0")
        if args.max_position_notional <= 0:
            raise RuntimeError("max_position_notional must be > 0")
    api_key, api_secret = creds
    symbol_info = fetch_spot_symbol_config(args.symbol)
    return _run_cycle(args, symbol_info, api_key, api_secret)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Spot runner with static, volume-shift, and competition inventory-grid long-only modes.")
    parser.add_argument("--symbol", type=str, required=True)
    parser.add_argument(
        "--strategy-mode",
        type=str,
        default="spot_one_way_long",
        choices=("spot_one_way_long", "spot_volume_shift_long", "spot_competition_inventory_grid"),
    )
    parser.add_argument("--min-price", type=float, default=0.0)
    parser.add_argument("--max-price", type=float, default=0.0)
    parser.add_argument("--n", type=int, default=0)
    parser.add_argument("--total-quote-budget", type=float, required=True)
    parser.add_argument("--step-price", type=float, default=0.0)
    parser.add_argument("--per-order-notional", type=float, default=0.0)
    parser.add_argument("--grid-level-mode", type=str, default="arithmetic", choices=("arithmetic", "geometric"))
    parser.add_argument("--sleep-seconds", type=float, default=10.0)
    parser.add_argument("--client-order-prefix", type=str, required=True)
    parser.add_argument("--state-path", type=str, required=True)
    parser.add_argument("--summary-jsonl", type=str, required=True)
    parser.add_argument("--cancel-stale", dest="cancel_stale", action="store_true")
    parser.add_argument("--no-cancel-stale", dest="cancel_stale", action="store_false")
    parser.set_defaults(cancel_stale=True)
    parser.add_argument("--apply", dest="apply", action="store_true")
    parser.add_argument("--no-apply", dest="apply", action="store_false")
    parser.set_defaults(apply=True)

    parser.add_argument("--grid-band-ratio", type=float, default=0.045)
    parser.add_argument("--attack-buy-levels", type=int, default=14)
    parser.add_argument("--attack-sell-levels", type=int, default=22)
    parser.add_argument("--attack-per-order-notional", type=float, default=20.0)
    parser.add_argument("--defense-buy-levels", type=int, default=8)
    parser.add_argument("--defense-sell-levels", type=int, default=20)
    parser.add_argument("--defense-per-order-notional", type=float, default=12.0)
    parser.add_argument("--inventory-soft-limit-notional", type=float, default=350.0)
    parser.add_argument("--inventory-hard-limit-notional", type=float, default=500.0)
    parser.add_argument("--center-shift-trigger-ratio", type=float, default=0.012)
    parser.add_argument("--center-shift-confirm-cycles", type=int, default=3)
    parser.add_argument("--center-shift-step-ratio", type=float, default=0.006)
    parser.add_argument("--buy-pause-amp-trigger-ratio", type=float, default=0.045)
    parser.add_argument("--buy-pause-down-return-trigger-ratio", type=float, default=-0.022)
    parser.add_argument("--freeze-shift-abs-return-trigger-ratio", type=float, default=0.03)
    parser.add_argument("--inventory-recycle-age-minutes", type=float, default=40.0)
    parser.add_argument("--inventory-recycle-loss-tolerance-ratio", type=float, default=0.006)
    parser.add_argument("--inventory-recycle-min-profit-ratio", type=float, default=0.001)
    parser.add_argument("--first-order-multiplier", type=float, default=1.0)
    parser.add_argument("--threshold-position-notional", type=float, default=0.0)
    parser.add_argument("--max-order-position-notional", type=float, default=0.0)
    parser.add_argument("--max-position-notional", type=float, default=0.0)
    parser.add_argument("--max-single-cycle-new-orders", type=int, default=8)
    parser.add_argument("--run-start-time", type=str, default=None)
    parser.add_argument("--run-end-time", type=str, default=None)
    parser.add_argument("--rolling-hourly-loss-limit", type=float, default=None)
    parser.add_argument("--max-cumulative-notional", type=float, default=None)
    return parser


def main() -> None:
    parser = _build_parser()

    args = parser.parse_args()
    normalize_runtime_guard_config(vars(args))

    while True:
        cycle_started_at = _utc_now()
        try:
            summary = run_once(args)
            print(
                f"[{cycle_started_at.isoformat()}] "
                f"mode={summary.get('mode')} "
                f"cycle={summary.get('cycle')} "
                f"mid={summary.get('mid_price'):.8f} "
                f"volume={summary.get('gross_notional', 0.0):.4f} "
                f"inventory={summary.get('inventory_qty', summary.get('managed_base_qty', 0.0)):.8f} "
                f"placed={summary.get('placed_count')} "
                f"canceled={summary.get('canceled_count')} "
                f"runtime={summary.get('runtime_status', 'running')}"
            )
            if summary.get("stop_reason"):
                print(f"  stop_reason: {summary['stop_reason']}")
        except KeyboardInterrupt:
            raise
        except Exception as exc:  # pragma: no cover - runtime specific
            error_payload = {
                "ts": cycle_started_at.isoformat(),
                "symbol": str(args.symbol).upper().strip(),
                "strategy_mode": str(args.strategy_mode or "spot_one_way_long"),
                "error": f"{type(exc).__name__}: {exc}",
                "traceback": traceback.format_exc(limit=8),
            }
            _append_jsonl(Path(args.summary_jsonl), error_payload)
            print(f"[{cycle_started_at.isoformat()}] error: {type(exc).__name__}: {exc}")
        time.sleep(max(float(args.sleep_seconds), 1.0))


if __name__ == "__main__":
    main()
