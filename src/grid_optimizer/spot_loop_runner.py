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
from .semi_auto_plan import diff_open_orders
from .spot_flatten_runner import load_live_spot_flatten_snapshot, spot_flatten_client_order_prefix

STATE_VERSION = 2
EPSILON = 1e-12
PRICE_CACHE_TTL_SECONDS = 30.0
_LATEST_PRICE_CACHE: dict[str, tuple[float, float]] = {}
DEFAULT_LOT_TAKE_PROFIT_OFFSET = 1.25
DEFAULT_FORCE_REDUCE_TRIGGER_RATIO = 0.5
DEFAULT_FORCE_REDUCE_PENDING_MS = 20_000
DEFAULT_ATTACK_MARKET_ENTRY_TRIGGER_MULTIPLIER = 1.0
DEFAULT_ATTACK_MARKET_ENTRY_TARGET_MULTIPLIER = 4.0
DEFAULT_ATTACK_MARKET_ENTRY_COOLDOWN_SECONDS = 60.0
DEFAULT_BEST_QUOTE_MAX_INVENTORY_MULTIPLIER = 2.0
DEFAULT_REDUCE_ONLY_TIMEOUT_SECONDS = 60.0
DEFAULT_REDUCE_ONLY_TARGET_MULTIPLIER = 1.4


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
    state.setdefault("force_reduce_pending_until_ms", 0)
    state.setdefault("inventory_bootstrap_completed", False)
    state.setdefault("attack_market_entry_last_time_ms", 0)
    state.setdefault("reduce_only_entered_at_ms", 0)

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
    for idx, lot in enumerate(lots, start=1):
        if not isinstance(lot, dict):
            continue
        qty = max(_safe_float(lot.get("qty")), 0.0)
        cost_quote = max(_safe_float(lot.get("cost_quote")), 0.0)
        if qty <= EPSILON:
            continue
        lot_id = str(lot.get("lot_id") or f"legacy_{_safe_int(lot.get('buy_time_ms'))}_{idx}").strip()
        normalized_lots.append(
            {
                "lot_id": lot_id,
                "qty": qty,
                "cost_quote": cost_quote,
                "buy_time_ms": _safe_int(lot.get("buy_time_ms")),
                "tag": str(lot.get("tag", "") or ""),
            }
        )
    state["inventory_lots"] = normalized_lots
    if normalized_lots:
        state["inventory_bootstrap_completed"] = True
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
    allowed = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_")
    prefix_clean = "".join(ch for ch in str(prefix or "").strip() if ch in allowed) or "sg"
    tag_clean = "".join(ch for ch in str(tag or "").strip() if ch in allowed) or "ord"
    side_text = "b" if str(side).upper().strip() == "BUY" else "s"
    nonce_value = time.time_ns() % (36**6)
    alphabet = "0123456789abcdefghijklmnopqrstuvwxyz"
    nonce_chars = []
    current = nonce_value
    for _ in range(6):
        current, rem = divmod(current, 36)
        nonce_chars.append(alphabet[rem])
    nonce = "".join(reversed(nonce_chars))

    prefix_part = prefix_clean[:12]
    fixed_width = len(prefix_part) + len(side_text) + len(nonce) + 3
    max_tag_len = max(1, 36 - fixed_width)
    client_order_id = f"{prefix_part}_{tag_clean[:max_tag_len]}_{side_text}_{nonce}"
    return client_order_id[:36]


def _sum_inventory_qty(lots: list[dict[str, Any]]) -> float:
    return sum(max(_safe_float(item.get("qty")), 0.0) for item in lots if isinstance(item, dict))


def _sum_inventory_cost(lots: list[dict[str, Any]]) -> float:
    return sum(max(_safe_float(item.get("cost_quote")), 0.0) for item in lots if isinstance(item, dict))


def _buy_capacity_inventory_notional(lots: list[dict[str, Any]], mid_price: float) -> float:
    total = 0.0
    for lot in lots:
        if not isinstance(lot, dict):
            continue
        if str(lot.get("tag", "") or "") == "bootstrap_inventory":
            continue
        qty = max(_safe_float(lot.get("qty")), 0.0)
        if qty <= EPSILON:
            continue
        total += qty * max(_safe_float(mid_price), 0.0)
    return total


def _maybe_bootstrap_inventory_from_balance(
    *,
    state: dict[str, Any],
    base_free: float,
    mid_price: float,
    now_ms: int,
    step_size: float | None,
    min_qty: float | None,
) -> int:
    lots = state.get("inventory_lots")
    if not isinstance(lots, list):
        lots = []
    if lots:
        state["inventory_bootstrap_completed"] = True
        return 0
    if _truthy(state.get("inventory_bootstrap_completed")):
        return 0

    metrics = state.get("metrics")
    if isinstance(metrics, dict) and _safe_int(metrics.get("trade_count")) > 0:
        state["inventory_bootstrap_completed"] = True
        return 0

    bootstrap_qty = _round_order_qty(max(_safe_float(base_free), 0.0), step_size)
    min_qty_value = max(_safe_float(min_qty), 0.0)
    if bootstrap_qty <= EPSILON or (min_qty_value > 0 and bootstrap_qty + EPSILON < min_qty_value):
        return 0
    bootstrap_price = max(_safe_float(mid_price), 0.0)
    if bootstrap_price <= 0:
        return 0

    state["inventory_lots"] = [
        {
            "lot_id": f"bootstrap_{now_ms}",
            "qty": bootstrap_qty,
            "cost_quote": bootstrap_qty * bootstrap_price,
            "buy_time_ms": int(now_ms),
            "tag": "bootstrap_inventory",
        }
    ]
    state["inventory_bootstrap_completed"] = True
    state["inventory_bootstrap_time_ms"] = int(now_ms)
    return 1


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
            new_lots.append(
                {
                    "lot_id": str(lot.get("lot_id") or ""),
                    "qty": qty,
                    "cost_quote": cost_quote,
                    "buy_time_ms": _safe_int(lot.get("buy_time_ms")),
                    "tag": str(lot.get("tag", "") or ""),
                }
            )
            continue
        take_qty = min(qty, remaining)
        unit_cost = cost_quote / qty if qty > EPSILON else 0.0
        consumed_cost += unit_cost * take_qty
        left_qty = qty - take_qty
        if left_qty > EPSILON:
            new_lots.append(
                {
                    "lot_id": str(lot.get("lot_id") or ""),
                    "qty": left_qty,
                    "cost_quote": max(cost_quote - unit_cost * take_qty, 0.0),
                    "buy_time_ms": _safe_int(lot.get("buy_time_ms")),
                    "tag": str(lot.get("tag", "") or ""),
                }
            )
        remaining -= take_qty
    return new_lots, consumed_cost


def _lot_unit_cost(lot: dict[str, Any]) -> float:
    qty = max(_safe_float(lot.get("qty")), 0.0)
    if qty <= EPSILON:
        return 0.0
    return max(_safe_float(lot.get("cost_quote")), 0.0) / qty


def _aggregate_desired_orders(desired_orders: list[dict[str, Any]]) -> list[dict[str, Any]]:
    aggregated: dict[tuple[str, float], dict[str, Any]] = {}
    order_keys: list[tuple[str, float]] = []
    for order in desired_orders:
        side = str(order.get("side", "")).upper().strip()
        price = float(order.get("price", 0.0) or 0.0)
        qty = float(order.get("qty", 0.0) or 0.0)
        if side not in {"BUY", "SELL"} or price <= 0 or qty <= 0:
            continue
        key = (side, price)
        current = aggregated.get(key)
        if current is None:
            current = dict(order)
            if "lot_plan" in current:
                current["lot_plan"] = list(current.get("lot_plan") or [])
            aggregated[key] = current
            order_keys.append(key)
            continue
        current["qty"] = float(current.get("qty", 0.0) or 0.0) + qty
        existing_plan = list(current.get("lot_plan") or [])
        existing_plan.extend(list(order.get("lot_plan") or []))
        if existing_plan:
            current["lot_plan"] = existing_plan
    return [aggregated[key] for key in order_keys]


def _sort_orders_for_placement(
    orders: list[dict[str, Any]],
    *,
    bid_price: float,
    ask_price: float,
) -> list[dict[str, Any]]:
    def _sort_key(order: dict[str, Any]) -> tuple[int, float, str]:
        side = str(order.get("side", "")).upper().strip()
        price = max(_safe_float(order.get("price")), 0.0)
        tag = str(order.get("tag", "") or "")
        if side == "BUY":
            return (0, max(ask_price - price, 0.0), tag)
        if side == "SELL":
            return (1, max(price - bid_price, 0.0), tag)
        return (9, price, tag)

    return sorted(list(orders or []), key=_sort_key)


def _consume_specific_lot(
    lots: list[dict[str, Any]],
    *,
    lot_id: str,
    qty_to_consume: float,
) -> tuple[list[dict[str, Any]], float]:
    wanted = str(lot_id or "").strip()
    remaining = max(float(qty_to_consume), 0.0)
    consumed_cost = 0.0
    new_lots: list[dict[str, Any]] = []
    for lot in lots:
        if not isinstance(lot, dict):
            continue
        current_id = str(lot.get("lot_id") or "").strip()
        qty = max(_safe_float(lot.get("qty")), 0.0)
        cost_quote = max(_safe_float(lot.get("cost_quote")), 0.0)
        if qty <= EPSILON:
            continue
        if current_id != wanted or remaining <= EPSILON:
            new_lots.append(dict(lot))
            continue
        take_qty = min(qty, remaining)
        unit_cost = cost_quote / qty if qty > EPSILON else 0.0
        consumed_cost += unit_cost * take_qty
        left_qty = qty - take_qty
        if left_qty > EPSILON:
            updated = dict(lot)
            updated["qty"] = left_qty
            updated["cost_quote"] = max(cost_quote - unit_cost * take_qty, 0.0)
            new_lots.append(updated)
        remaining -= take_qty
    return new_lots, consumed_cost


def _consume_lot_plan(
    lots: list[dict[str, Any]],
    *,
    lot_plan: list[dict[str, Any]],
    qty_to_consume: float,
) -> tuple[list[dict[str, Any]], float]:
    remaining = max(float(qty_to_consume), 0.0)
    working_lots = list(lots)
    consumed_cost = 0.0
    for item in lot_plan:
        if remaining <= EPSILON:
            break
        if not isinstance(item, dict):
            continue
        plan_qty = max(_safe_float(item.get("qty")), 0.0)
        if plan_qty <= EPSILON:
            continue
        lot_id = str(item.get("lot_id") or "").strip()
        take_qty = min(plan_qty, remaining)
        working_lots, lot_cost = _consume_specific_lot(working_lots, lot_id=lot_id, qty_to_consume=take_qty)
        consumed_cost += lot_cost
        remaining -= take_qty
    if remaining > EPSILON:
        working_lots, extra_cost = _consume_inventory_lots(working_lots, remaining)
        consumed_cost += extra_cost
    return working_lots, consumed_cost


def _build_force_reduce_orders(
    *,
    lots: list[dict[str, Any]],
    bid_price: float,
    mid_price: float,
    hard_limit_notional: float,
    trigger_ratio: float = DEFAULT_FORCE_REDUCE_TRIGGER_RATIO,
    step_size: float | None,
    min_qty: float | None,
    min_notional: float | None,
) -> list[dict[str, Any]]:
    inventory_qty = _sum_inventory_qty(lots)
    inventory_notional = inventory_qty * max(mid_price, 0.0)
    trigger_notional = max(float(hard_limit_notional), 0.0) * max(float(trigger_ratio), 0.0)
    if inventory_qty <= EPSILON or inventory_notional <= trigger_notional or bid_price <= 0:
        return []
    excess_notional = inventory_notional - trigger_notional
    remaining_qty = excess_notional / max(mid_price, EPSILON)
    plan: list[dict[str, Any]] = []
    for lot in sorted((dict(item) for item in lots if isinstance(item, dict)), key=_lot_unit_cost, reverse=True):
        if remaining_qty <= EPSILON:
            break
        lot_qty = max(_safe_float(lot.get("qty")), 0.0)
        if lot_qty <= EPSILON:
            continue
        order_qty = _round_order_qty(min(lot_qty, remaining_qty), step_size)
        if order_qty <= EPSILON:
            continue
        if min_qty is not None and order_qty < min_qty:
            continue
        if min_notional is not None and order_qty * bid_price < min_notional:
            continue
        plan.append(
            {
                "side": "SELL",
                "order_type": "MARKET",
                "price": bid_price,
                "qty": order_qty,
                "tag": f"mktreduce_{lot.get('lot_id')}",
                "role": "forced_reduce",
                "lot_plan": [{"lot_id": str(lot.get("lot_id") or ""), "qty": order_qty}],
            }
        )
        remaining_qty -= order_qty
    return plan


def _build_attack_market_entry_order(
    *,
    state: dict[str, Any],
    config: dict[str, Any],
    controls: dict[str, Any],
    ask_price: float,
    quote_free: float,
    step_size: float | None,
    min_qty: float | None,
    min_notional: float | None,
    now_ms: int,
) -> dict[str, Any] | None:
    if not _truthy(config.get("attack_market_entry_enabled", False)):
        return None
    if str(controls.get("mode", "") or "").strip() != "attack":
        return None
    if _truthy(controls.get("buy_paused", False)):
        return None
    if ask_price <= 0 or quote_free <= 0:
        return None

    per_order_notional = max(
        _safe_float(controls.get("effective_per_order_notional")),
        _safe_float(config.get("attack_per_order_notional")),
    )
    if per_order_notional <= 0:
        return None

    trigger_multiplier = max(
        _safe_float(
            config.get(
                "attack_market_entry_trigger_multiplier",
                DEFAULT_ATTACK_MARKET_ENTRY_TRIGGER_MULTIPLIER,
            )
        ),
        0.0,
    ) or DEFAULT_ATTACK_MARKET_ENTRY_TRIGGER_MULTIPLIER
    target_multiplier = max(
        _safe_float(
            config.get(
                "attack_market_entry_target_multiplier",
                DEFAULT_ATTACK_MARKET_ENTRY_TARGET_MULTIPLIER,
            )
        ),
        0.0,
    ) or DEFAULT_ATTACK_MARKET_ENTRY_TARGET_MULTIPLIER
    if target_multiplier < trigger_multiplier:
        target_multiplier = trigger_multiplier

    inventory_notional = max(_safe_float(controls.get("inventory_notional")), 0.0)
    if inventory_notional + EPSILON >= per_order_notional * trigger_multiplier:
        return None

    cooldown_ms = int(
        max(
            _safe_float(
                config.get(
                    "attack_market_entry_cooldown_seconds",
                    DEFAULT_ATTACK_MARKET_ENTRY_COOLDOWN_SECONDS,
                )
            ),
            0.0,
        )
        * 1000
    )
    last_time_ms = _safe_int(state.get("attack_market_entry_last_time_ms"))
    if cooldown_ms > 0 and now_ms < last_time_ms + cooldown_ms:
        return None

    target_notional = per_order_notional * target_multiplier
    order_notional = min(max(target_notional - inventory_notional, 0.0), quote_free)
    if order_notional <= 0:
        return None

    qty = _round_order_qty(order_notional / ask_price, step_size)
    if qty <= EPSILON:
        return None
    if min_qty is not None and qty + EPSILON < float(min_qty):
        return None
    estimated_notional = qty * ask_price
    if min_notional is not None and estimated_notional + EPSILON < float(min_notional):
        return None

    return {
        "side": "BUY",
        "order_type": "MARKET",
        "price": 0.0,
        "qty": qty,
        "tag": "warmentry",
        "role": "warm_start_entry",
        "estimated_notional": estimated_notional,
    }


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
    prefix_text = str(prefix or "").strip()
    if not prefix_text:
        return False
    if client_order_id.startswith(prefix_text):
        return True
    legacy_prefix = prefix_text[:8]
    return bool(legacy_prefix) and client_order_id.startswith(legacy_prefix)


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
                lot_id = str(meta.get("lot_id") or f"lot_{trade_id}").strip()
                lots.append(
                    {
                        "lot_id": lot_id,
                        "qty": received_qty,
                        "cost_quote": cost_quote,
                        "buy_time_ms": _safe_int(trade.get("time")),
                        "tag": role or "buy",
                    }
                )
        elif side == "SELL":
            consume_qty = trade_qty + (commission if commission_asset == base_asset else 0.0)
            lot_plan = meta.get("lot_plan")
            lot_id = str(meta.get("lot_id") or "").strip()
            if isinstance(lot_plan, list) and lot_plan:
                lots, consumed_cost = _consume_lot_plan(lots, lot_plan=lot_plan, qty_to_consume=consume_qty)
            elif lot_id:
                lots, consumed_cost = _consume_specific_lot(lots, lot_id=lot_id, qty_to_consume=consume_qty)
            else:
                lots, consumed_cost = _consume_inventory_lots(lots, consume_qty)
            proceeds_quote = price * trade_qty - commission_quote
            realized_pnl = proceeds_quote - consumed_cost
            if role == "forced_reduce":
                state["force_reduce_pending_until_ms"] = 0
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
    order_type: str = "LIMIT_MAKER",
    time_in_force: str = "GTC",
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
        "order_type": str(order_type or "LIMIT_MAKER").upper().strip() or "LIMIT_MAKER",
        "time_in_force": str(time_in_force or "GTC").upper().strip() or "GTC",
    }


def _best_quote_inventory_sell_price(
    *,
    ask_price: float,
    inventory_avg_cost: float,
    min_profit_offset: float,
    tick_size: float | None,
) -> float:
    profit_floor = inventory_avg_cost + max(float(min_profit_offset), 0.0)
    raw_price = max(float(ask_price), profit_floor)
    return _round_order_price(raw_price, tick_size, "SELL")


def _resolve_best_quote_inventory_mode(
    *,
    inventory_notional: float,
    base_position_notional: float,
    max_inventory_multiplier: float = DEFAULT_BEST_QUOTE_MAX_INVENTORY_MULTIPLIER,
) -> str:
    base_notional = max(float(base_position_notional), 0.0)
    max_multiplier = max(float(max_inventory_multiplier), 1.0)
    if base_notional <= 0:
        return "quote"
    if inventory_notional + EPSILON < base_notional:
        return "bootstrap"
    if inventory_notional + EPSILON >= base_notional * max_multiplier:
        return "reduce_only"
    return "quote"


def _build_best_quote_inventory_desired_orders(
    *,
    state: dict[str, Any],
    config: dict[str, Any],
    symbol_info: dict[str, Any],
    bid_price: float,
    ask_price: float,
    mid_price: float,
    inventory_lots: list[dict[str, Any]],
    base_free: float,
    now_ms: int,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    tick_size = symbol_info.get("tick_size")
    step_size = symbol_info.get("step_size")
    min_qty = symbol_info.get("min_qty")
    min_notional = symbol_info.get("min_notional")

    inventory_qty = _sum_inventory_qty(inventory_lots)
    inventory_cost_quote = _sum_inventory_cost(inventory_lots)
    inventory_avg_cost = inventory_cost_quote / inventory_qty if inventory_qty > EPSILON else 0.0
    inventory_notional = inventory_qty * max(mid_price, 0.0)

    base_position_notional = max(_safe_float(config.get("base_position_notional")), 0.0)
    max_inventory_multiplier = max(
        _safe_float(config.get("max_inventory_multiplier", DEFAULT_BEST_QUOTE_MAX_INVENTORY_MULTIPLIER)),
        1.0,
    ) or DEFAULT_BEST_QUOTE_MAX_INVENTORY_MULTIPLIER
    mode = _resolve_best_quote_inventory_mode(
        inventory_notional=inventory_notional,
        base_position_notional=base_position_notional,
        max_inventory_multiplier=max_inventory_multiplier,
    )

    if mode == "reduce_only":
        entered_at_ms = _safe_int(state.get("reduce_only_entered_at_ms"))
        if entered_at_ms <= 0:
            state["reduce_only_entered_at_ms"] = int(now_ms)
    else:
        state["reduce_only_entered_at_ms"] = 0

    quote_buy_order_notional = max(_safe_float(config.get("quote_buy_order_notional")), 0.0)
    quote_sell_order_notional = max(
        _safe_float(config.get("quote_sell_order_notional", quote_buy_order_notional)),
        0.0,
    )
    min_profit_offset = max(_safe_float(config.get("min_profit_offset")), 0.0)
    reduce_target_multiplier = max(
        _safe_float(config.get("reduce_only_taker_target_multiplier", DEFAULT_REDUCE_ONLY_TARGET_MULTIPLIER)),
        1.0,
    ) or DEFAULT_REDUCE_ONLY_TARGET_MULTIPLIER
    desired: list[dict[str, Any]] = []

    if mode in {"bootstrap", "quote"} and quote_buy_order_notional > 0 and bid_price > 0:
        buy_notional = quote_buy_order_notional
        if mode == "bootstrap" and base_position_notional > 0:
            buy_notional = min(quote_buy_order_notional, max(base_position_notional - inventory_notional, 0.0))
        if buy_notional > 0:
            buy_order = _normalize_order(
                side="BUY",
                price=_round_order_price(bid_price, tick_size, "BUY"),
                qty=buy_notional / bid_price,
                step_size=step_size,
                min_qty=min_qty,
                min_notional=min_notional,
                tag="bestbuy",
                role="inventory_build" if mode == "bootstrap" else "best_quote_buy",
            )
            if buy_order is not None:
                desired.append(buy_order)

    sell_qty_cap = max(_round_order_qty(max(base_free, 0.0), step_size), 0.0)
    if sell_qty_cap > EPSILON and ask_price > 0 and quote_sell_order_notional > 0:
        if mode == "reduce_only" and base_position_notional > 0:
            target_qty = (base_position_notional * reduce_target_multiplier) / max(mid_price, EPSILON)
            sell_qty = max(inventory_qty - target_qty, 0.0)
        else:
            sell_qty = quote_sell_order_notional / ask_price
        sell_qty = min(sell_qty, sell_qty_cap)
        sell_price = _best_quote_inventory_sell_price(
            ask_price=ask_price,
            inventory_avg_cost=inventory_avg_cost,
            min_profit_offset=min_profit_offset,
            tick_size=tick_size,
        )
        if sell_price > 0 and sell_qty > 0:
            sell_order = _normalize_order(
                side="SELL",
                price=sell_price,
                qty=sell_qty,
                step_size=step_size,
                min_qty=min_qty,
                min_notional=min_notional,
                tag="bestsell" if mode != "reduce_only" else "reducesell",
                role="best_quote_sell" if mode != "reduce_only" else "reduce_only_sell",
                order_type="LIMIT_MAKER",
                time_in_force="GTC",
            )
            if sell_order is not None:
                desired.append(sell_order)

    controls = {
        "mode": mode,
        "buy_paused": mode == "reduce_only",
        "pause_reasons": ["inventory_max_limit"] if mode == "reduce_only" else [],
        "shift_frozen": False,
        "market_guard_return_ratio": 0.0,
        "market_guard_amplitude_ratio": 0.0,
        "center_price": 0.0,
        "center_shift_count": 0,
        "shift_moves": [],
        "inventory_qty": inventory_qty,
        "inventory_notional": inventory_notional,
        "inventory_avg_cost": inventory_avg_cost,
        "oldest_inventory_age_minutes": _oldest_inventory_age_minutes(inventory_lots),
        "inventory_soft_limit_notional": base_position_notional,
        "inventory_hard_limit_notional": base_position_notional * max_inventory_multiplier,
        "effective_buy_levels": 1 if any(order["side"] == "BUY" for order in desired) else 0,
        "effective_sell_levels": 1 if any(order["side"] == "SELL" for order in desired) else 0,
        "effective_per_order_notional": quote_buy_order_notional,
        "reduce_only_entered_at_ms": _safe_int(state.get("reduce_only_entered_at_ms")),
    }
    state["last_mode"] = mode
    return desired, controls


def _build_reduce_only_market_exit_order(
    *,
    state: dict[str, Any],
    config: dict[str, Any],
    symbol_info: dict[str, Any],
    bid_price: float,
    mid_price: float,
    inventory_qty: float,
    inventory_notional: float,
    now_ms: int,
) -> dict[str, Any] | None:
    base_position_notional = max(_safe_float(config.get("base_position_notional")), 0.0)
    if base_position_notional <= 0:
        return None
    mode = _resolve_best_quote_inventory_mode(
        inventory_notional=inventory_notional,
        base_position_notional=base_position_notional,
        max_inventory_multiplier=max(
            _safe_float(config.get("max_inventory_multiplier", DEFAULT_BEST_QUOTE_MAX_INVENTORY_MULTIPLIER)),
            1.0,
        ),
    )
    if mode != "reduce_only":
        return None
    entered_at_ms = _safe_int(state.get("reduce_only_entered_at_ms"))
    if entered_at_ms <= 0:
        return None
    timeout_ms = int(
        max(
            _safe_float(config.get("reduce_only_timeout_seconds", DEFAULT_REDUCE_ONLY_TIMEOUT_SECONDS)),
            0.0,
        )
        * 1000
    )
    if timeout_ms > 0 and now_ms < entered_at_ms + timeout_ms:
        return None

    target_multiplier = max(
        _safe_float(config.get("reduce_only_taker_target_multiplier", DEFAULT_REDUCE_ONLY_TARGET_MULTIPLIER)),
        1.0,
    ) or DEFAULT_REDUCE_ONLY_TARGET_MULTIPLIER
    target_qty = (base_position_notional * target_multiplier) / max(mid_price, EPSILON)
    reduce_qty = max(inventory_qty - target_qty, 0.0)
    qty = _round_order_qty(reduce_qty, symbol_info.get("step_size"))
    if qty <= EPSILON:
        return None
    min_qty = symbol_info.get("min_qty")
    min_notional = symbol_info.get("min_notional")
    if min_qty is not None and qty + EPSILON < float(min_qty):
        return None
    if min_notional is not None and qty * max(bid_price, 0.0) + EPSILON < float(min_notional):
        return None
    return {
        "side": "SELL",
        "order_type": "MARKET",
        "price": bid_price,
        "qty": qty,
        "tag": "reduceonly_taker",
        "role": "reduce_only_market_exit",
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
    elif (
        recycle_age_minutes > 0
        and oldest_age_minutes >= recycle_age_minutes
        and inventory_notional > 0
        and (soft_limit <= 0 or inventory_notional >= soft_limit * 0.5)
    ):
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
    buy_capacity_inventory_notional = _buy_capacity_inventory_notional(lots, mid_price)
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
        per_order_notional = max(_safe_float(config.get("attack_per_order_notional")), 0.0)
    else:
        buy_levels = max(_safe_int(config.get("defense_buy_levels")), 1)
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

    desired: list[dict[str, Any]] = []
    target_cap = max(hard_limit, per_order_notional)
    if target_cap <= 0:
        target_cap = max(inventory_notional, 0.0)
    running_inventory_notional = buy_capacity_inventory_notional
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

    lot_take_profit_offset = max(_safe_float(config.get("lot_take_profit_offset", DEFAULT_LOT_TAKE_PROFIT_OFFSET)), 0.0)
    sell_role = "take_profit"
    if mode == "defense":
        sell_role = "defense_sell"
    elif mode == "recycle":
        sell_role = "recycle_sell"
    sell_order_count = 0
    for lot in lots:
        if not isinstance(lot, dict):
            continue
        lot_qty = max(_safe_float(lot.get("qty")), 0.0)
        if lot_qty <= EPSILON:
            continue
        lot_cost = _lot_unit_cost(lot)
        raw_price = lot_cost + lot_take_profit_offset
        sell_price = max(_round_order_price(raw_price, tick_size, "SELL"), ask_price)
        order = _normalize_order(
            side="SELL",
            price=sell_price,
            qty=lot_qty,
            step_size=step_size,
            min_qty=min_qty,
            min_notional=min_notional,
            tag=f"sell_{lot.get('lot_id')}",
            role=sell_role,
        )
        if order and order["price"] > bid_price:
            order["lot_id"] = str(lot.get("lot_id") or "")
            order["lot_plan"] = [{"lot_id": str(lot.get("lot_id") or ""), "qty": order["qty"]}]
            desired.append(order)
            sell_order_count += 1

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
        "effective_sell_levels": sell_order_count,
        "effective_per_order_notional": per_order_notional,
        "lot_take_profit_offset": lot_take_profit_offset,
    }
    return desired, controls


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
        "cumulative_loss": runtime_guard_result.cumulative_loss,
        "max_cumulative_loss_limit": runtime_guard_config.max_cumulative_loss_limit,
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
    else:
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

    account_info = fetch_spot_account_info(api_key, api_secret)
    if strategy_mode != "spot_one_way_long":
        _quote_free, bootstrap_base_free = _available_new_funds(account_info, base_asset, quote_asset)
        _maybe_bootstrap_inventory_from_balance(
            state=state,
            base_free=bootstrap_base_free,
            mid_price=mid_price,
            now_ms=int(time.time() * 1000),
            step_size=symbol_info.get("step_size"),
            min_qty=symbol_info.get("min_qty"),
        )
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
        now_ms = int(time.time() * 1000)
        force_reduce_orders = []
        pending_until_ms = _safe_int(state.get("force_reduce_pending_until_ms"))
        if now_ms >= pending_until_ms:
            force_reduce_orders = _build_force_reduce_orders(
                lots=state.get("inventory_lots") if isinstance(state.get("inventory_lots"), list) else [],
                bid_price=bid_price,
                mid_price=mid_price,
                hard_limit_notional=_safe_float(getattr(args, "inventory_hard_limit_notional", 0.0)),
                trigger_ratio=max(_safe_float(getattr(args, "inventory_market_reduce_trigger_ratio", DEFAULT_FORCE_REDUCE_TRIGGER_RATIO)), 0.0)
                or DEFAULT_FORCE_REDUCE_TRIGGER_RATIO,
                step_size=symbol_info.get("step_size"),
                min_qty=symbol_info.get("min_qty"),
                min_notional=symbol_info.get("min_notional"),
            )
        if force_reduce_orders:
            stale_sell_orders = [order for order in strategy_open_orders if str(order.get("side", "")).upper().strip() == "SELL"]
            if _truthy(args.cancel_stale) and stale_sell_orders:
                _cancel_orders(
                    symbol=symbol,
                    api_key=api_key,
                    api_secret=api_secret,
                    orders=stale_sell_orders,
                )
                account_info = fetch_spot_account_info(api_key, api_secret)
                open_orders = fetch_spot_open_orders(symbol, api_key, api_secret)
                strategy_open_orders = _strategy_open_orders(open_orders, str(args.client_order_prefix))
            quote_free, base_free = _available_new_funds(account_info, base_asset, quote_asset)
            remaining_base_free = base_free
            forced_reduce_count = 0
            for idx, order in enumerate(force_reduce_orders, start=1):
                qty = min(float(order["qty"]), remaining_base_free)
                qty = _round_order_qty(qty, symbol_info.get("step_size"))
                if qty <= EPSILON:
                    continue
                client_order_id = _build_client_order_id(str(args.client_order_prefix), f"mktreduce{idx}", "SELL")
                order_lot_plan = []
                for item in list(order.get("lot_plan") or []):
                    if not isinstance(item, dict):
                        continue
                    plan_qty = min(max(_safe_float(item.get("qty")), 0.0), qty)
                    if plan_qty <= EPSILON:
                        continue
                    order_lot_plan.append({"lot_id": str(item.get("lot_id") or ""), "qty": plan_qty})
                if not _truthy(args.apply):
                    forced_reduce_count += 1
                    remaining_base_free = max(0.0, remaining_base_free - qty)
                    continue
                placed = post_spot_order(
                    symbol=symbol,
                    side="SELL",
                    quantity=qty,
                    price=0.0,
                    api_key=api_key,
                    api_secret=api_secret,
                    order_type="MARKET",
                    new_client_order_id=client_order_id,
                )
                order_id = int(placed.get("orderId", 0) or 0)
                if order_id > 0:
                    state["known_orders"][str(order_id)] = {
                        "cell_idx": 0,
                        "side": "SELL",
                        "client_order_id": client_order_id,
                        "created_at_ms": int(time.time() * 1000),
                        "role": "forced_reduce",
                        "tag": str(order.get("tag", "") or ""),
                        "lot_plan": order_lot_plan,
                    }
                forced_reduce_count += 1
                remaining_base_free = max(0.0, remaining_base_free - qty)
            if forced_reduce_count > 0:
                state["force_reduce_pending_until_ms"] = int(time.time() * 1000) + DEFAULT_FORCE_REDUCE_PENDING_MS
                if _truthy(args.apply):
                    time.sleep(0.5)
                    refreshed_trades = fetch_spot_user_trades(
                        symbol=symbol,
                        api_key=api_key,
                        api_secret=api_secret,
                        start_time_ms=max(int(state.get("last_trade_time_ms") or 0) - 60_000, 0),
                        limit=1000,
                    )
                    applied_trades += _sync_volume_shift_trades(
                        state=state,
                        trades=refreshed_trades,
                        base_asset=base_asset,
                        quote_asset=quote_asset,
                    )
                    account_info = fetch_spot_account_info(api_key, api_secret)
                    open_orders = fetch_spot_open_orders(symbol, api_key, api_secret)
                    strategy_open_orders = _strategy_open_orders(open_orders, str(args.client_order_prefix))
        desired_orders, controls = _build_volume_shift_desired_orders(
            state=state,
            config=vars(args),
            symbol_info=symbol_info,
            bid_price=bid_price,
            ask_price=ask_price,
            mid_price=mid_price,
            market_guard=market_guard,
        )
        quote_free, base_free = _available_new_funds(account_info, base_asset, quote_asset)
        warm_entry_order = _build_attack_market_entry_order(
            state=state,
            config=vars(args),
            controls=controls,
            ask_price=ask_price,
            quote_free=quote_free,
            step_size=symbol_info.get("step_size"),
            min_qty=symbol_info.get("min_qty"),
            min_notional=symbol_info.get("min_notional"),
            now_ms=now_ms,
        )
        if warm_entry_order is not None:
            stale_buy_orders = [order for order in strategy_open_orders if str(order.get("side", "")).upper().strip() == "BUY"]
            if _truthy(args.cancel_stale) and stale_buy_orders:
                _cancel_orders(
                    symbol=symbol,
                    api_key=api_key,
                    api_secret=api_secret,
                    orders=stale_buy_orders,
                )
                account_info = fetch_spot_account_info(api_key, api_secret)
                open_orders = fetch_spot_open_orders(symbol, api_key, api_secret)
                strategy_open_orders = _strategy_open_orders(open_orders, str(args.client_order_prefix))
                quote_free, base_free = _available_new_funds(account_info, base_asset, quote_asset)
                warm_entry_order = _build_attack_market_entry_order(
                    state=state,
                    config=vars(args),
                    controls=controls,
                    ask_price=ask_price,
                    quote_free=quote_free,
                    step_size=symbol_info.get("step_size"),
                    min_qty=symbol_info.get("min_qty"),
                    min_notional=symbol_info.get("min_notional"),
                    now_ms=now_ms,
                )
            if warm_entry_order is not None:
                client_order_id = _build_client_order_id(str(args.client_order_prefix), str(warm_entry_order["tag"]), "BUY")
                if _truthy(args.apply):
                    placed = post_spot_order(
                        symbol=symbol,
                        side="BUY",
                        quantity=float(warm_entry_order["qty"]),
                        price=0.0,
                        api_key=api_key,
                        api_secret=api_secret,
                        order_type="MARKET",
                        new_client_order_id=client_order_id,
                    )
                    order_id = int(placed.get("orderId", 0) or 0)
                    if order_id > 0:
                        state["known_orders"][str(order_id)] = {
                            "cell_idx": 0,
                            "side": "BUY",
                            "client_order_id": client_order_id,
                            "created_at_ms": now_ms,
                            "role": str(warm_entry_order.get("role", "") or ""),
                            "tag": str(warm_entry_order.get("tag", "") or ""),
                            "lot_id": f"warmentry_{now_ms}",
                            "lot_plan": [],
                        }
                    state["attack_market_entry_last_time_ms"] = now_ms
                    time.sleep(0.5)
                    refreshed_trades = fetch_spot_user_trades(
                        symbol=symbol,
                        api_key=api_key,
                        api_secret=api_secret,
                        start_time_ms=max(int(state.get("last_trade_time_ms") or 0) - 60_000, 0),
                        limit=1000,
                    )
                    applied_trades += _sync_volume_shift_trades(
                        state=state,
                        trades=refreshed_trades,
                        base_asset=base_asset,
                        quote_asset=quote_asset,
                    )
                    account_info = fetch_spot_account_info(api_key, api_secret)
                    open_orders = fetch_spot_open_orders(symbol, api_key, api_secret)
                    strategy_open_orders = _strategy_open_orders(open_orders, str(args.client_order_prefix))
                else:
                    state["attack_market_entry_last_time_ms"] = now_ms
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
        now_ms = int(time.time() * 1000)
        quote_free, base_free = _available_new_funds(account_info, base_asset, quote_asset)
        inventory_lots = state.get("inventory_lots") if isinstance(state.get("inventory_lots"), list) else []
        desired_orders, controls = _build_best_quote_inventory_desired_orders(
            state=state,
            config=vars(args),
            symbol_info=symbol_info,
            bid_price=bid_price,
            ask_price=ask_price,
            mid_price=mid_price,
            inventory_lots=inventory_lots,
            base_free=base_free,
            now_ms=now_ms,
        )
        market_exit_order = _build_reduce_only_market_exit_order(
            state=state,
            config=vars(args),
            symbol_info=symbol_info,
            bid_price=bid_price,
            mid_price=mid_price,
            inventory_qty=_sum_inventory_qty(inventory_lots),
            inventory_notional=_sum_inventory_qty(inventory_lots) * mid_price,
            now_ms=now_ms,
        )
        if market_exit_order is not None:
            if _truthy(args.cancel_stale) and strategy_open_orders:
                _cancel_orders(
                    symbol=symbol,
                    api_key=api_key,
                    api_secret=api_secret,
                    orders=strategy_open_orders,
                )
                account_info = fetch_spot_account_info(api_key, api_secret)
                open_orders = fetch_spot_open_orders(symbol, api_key, api_secret)
                strategy_open_orders = _strategy_open_orders(open_orders, str(args.client_order_prefix))
                quote_free, base_free = _available_new_funds(account_info, base_asset, quote_asset)
            exit_qty = min(float(market_exit_order["qty"]), max(base_free, 0.0))
            exit_qty = _round_order_qty(exit_qty, symbol_info.get("step_size"))
            if exit_qty > EPSILON:
                if _truthy(args.apply):
                    client_order_id = _build_client_order_id(str(args.client_order_prefix), str(market_exit_order["tag"]), "SELL")
                    placed = post_spot_order(
                        symbol=symbol,
                        side="SELL",
                        quantity=exit_qty,
                        price=0.0,
                        api_key=api_key,
                        api_secret=api_secret,
                        order_type="MARKET",
                        new_client_order_id=client_order_id,
                    )
                    order_id = int(placed.get("orderId", 0) or 0)
                    if order_id > 0:
                        state["known_orders"][str(order_id)] = {
                            "cell_idx": 0,
                            "side": "SELL",
                            "client_order_id": client_order_id,
                            "created_at_ms": now_ms,
                            "role": str(market_exit_order.get("role", "") or ""),
                            "tag": str(market_exit_order.get("tag", "") or ""),
                            "lot_plan": [],
                        }
                    time.sleep(0.5)
                    refreshed_trades = fetch_spot_user_trades(
                        symbol=symbol,
                        api_key=api_key,
                        api_secret=api_secret,
                        start_time_ms=max(int(state.get("last_trade_time_ms") or 0) - 60_000, 0),
                        limit=1000,
                    )
                    applied_trades += _sync_volume_shift_trades(
                        state=state,
                        trades=refreshed_trades,
                        base_asset=base_asset,
                        quote_asset=quote_asset,
                    )
                    account_info = fetch_spot_account_info(api_key, api_secret)
                    open_orders = fetch_spot_open_orders(symbol, api_key, api_secret)
                    strategy_open_orders = _strategy_open_orders(open_orders, str(args.client_order_prefix))
                    inventory_lots = state.get("inventory_lots") if isinstance(state.get("inventory_lots"), list) else []
                    quote_free, base_free = _available_new_funds(account_info, base_asset, quote_asset)
                desired_orders, controls = _build_best_quote_inventory_desired_orders(
                    state=state,
                    config=vars(args),
                    symbol_info=symbol_info,
                    bid_price=bid_price,
                    ask_price=ask_price,
                    mid_price=mid_price,
                    inventory_lots=inventory_lots,
                    base_free=base_free,
                    now_ms=now_ms,
                )
    desired_orders = _aggregate_desired_orders(desired_orders)
    diff = diff_open_orders(existing_orders=strategy_open_orders, desired_orders=desired_orders)
    diff["missing_orders"] = _sort_orders_for_placement(
        diff.get("missing_orders", []),
        bid_price=bid_price,
        ask_price=ask_price,
    )

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
                order_type=str(order.get("order_type") or "LIMIT_MAKER"),
                time_in_force=str(order.get("time_in_force") or "GTC"),
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
                    "lot_id": str(order.get("lot_id", "") or ""),
                    "lot_plan": list(order.get("lot_plan") or []),
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
        "cumulative_loss": runtime_guard_result.cumulative_loss,
        "max_cumulative_loss_limit": runtime_guard_config.max_cumulative_loss_limit,
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
    if str(args.strategy_mode or "spot_one_way_long").strip() == "spot_one_way_long":
        if args.min_price <= 0 or args.max_price <= 0 or args.min_price >= args.max_price:
            raise RuntimeError("static spot grid requires valid min/max price")
        if args.n <= 0 or args.total_quote_budget <= 0:
            raise RuntimeError("static spot grid requires n > 0 and total_quote_budget > 0")
    else:
        if args.total_quote_budget <= 0:
            raise RuntimeError("spot volume-shift runner requires total_quote_budget > 0")
        if args.inventory_hard_limit_notional <= 0:
            raise RuntimeError("inventory_hard_limit_notional must be > 0")
    api_key, api_secret = creds
    symbol_info = fetch_spot_symbol_config(args.symbol)
    return _run_cycle(args, symbol_info, api_key, api_secret)


def main() -> None:
    parser = argparse.ArgumentParser(description="Spot runner with static and volume-shift long-only modes.")
    parser.add_argument("--symbol", type=str, required=True)
    parser.add_argument(
        "--strategy-mode",
        type=str,
        default="spot_one_way_long",
        choices=("spot_one_way_long", "spot_volume_shift_long", "spot_best_quote_inventory"),
    )
    parser.add_argument("--min-price", type=float, default=0.0)
    parser.add_argument("--max-price", type=float, default=0.0)
    parser.add_argument("--n", type=int, default=0)
    parser.add_argument("--total-quote-budget", type=float, required=True)
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
    parser.add_argument("--attack-market-entry-enabled", dest="attack_market_entry_enabled", action="store_true")
    parser.add_argument("--no-attack-market-entry-enabled", dest="attack_market_entry_enabled", action="store_false")
    parser.set_defaults(attack_market_entry_enabled=False)
    parser.add_argument(
        "--attack-market-entry-trigger-multiplier",
        type=float,
        default=DEFAULT_ATTACK_MARKET_ENTRY_TRIGGER_MULTIPLIER,
    )
    parser.add_argument(
        "--attack-market-entry-target-multiplier",
        type=float,
        default=DEFAULT_ATTACK_MARKET_ENTRY_TARGET_MULTIPLIER,
    )
    parser.add_argument(
        "--attack-market-entry-cooldown-seconds",
        type=float,
        default=DEFAULT_ATTACK_MARKET_ENTRY_COOLDOWN_SECONDS,
    )
    parser.add_argument("--lot-take-profit-offset", type=float, default=DEFAULT_LOT_TAKE_PROFIT_OFFSET)
    parser.add_argument("--max-single-cycle-new-orders", type=int, default=8)
    parser.add_argument("--base-position-notional", type=float, default=0.0)
    parser.add_argument("--max-inventory-multiplier", type=float, default=DEFAULT_BEST_QUOTE_MAX_INVENTORY_MULTIPLIER)
    parser.add_argument("--quote-buy-order-notional", type=float, default=20.0)
    parser.add_argument("--quote-sell-order-notional", type=float, default=20.0)
    parser.add_argument("--min-profit-offset", type=float, default=0.0)
    parser.add_argument("--reduce-only-timeout-seconds", type=float, default=DEFAULT_REDUCE_ONLY_TIMEOUT_SECONDS)
    parser.add_argument(
        "--reduce-only-taker-target-multiplier",
        type=float,
        default=DEFAULT_REDUCE_ONLY_TARGET_MULTIPLIER,
    )
    parser.add_argument("--run-start-time", type=str, default=None)
    parser.add_argument("--run-end-time", type=str, default=None)
    parser.add_argument("--rolling-hourly-loss-limit", type=float, default=None)
    parser.add_argument("--max-cumulative-notional", type=float, default=None)
    parser.add_argument("--max-cumulative-loss-limit", type=float, default=None)

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
