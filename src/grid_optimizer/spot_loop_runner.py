from __future__ import annotations

import argparse
import copy
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
    fetch_futures_position_mode,
    fetch_futures_position_risk_v3,
    fetch_futures_symbol_config,
    fetch_spot_account_info,
    fetch_spot_agg_trades,
    fetch_spot_book_tickers,
    fetch_spot_klines,
    fetch_spot_latest_price,
    fetch_spot_open_orders,
    fetch_spot_symbol_config,
    fetch_spot_user_trades,
    load_binance_api_credentials,
    post_futures_market_order,
    post_spot_order,
)
from .dry_run import _round_order_price, _round_order_qty
from .runtime_guards import evaluate_runtime_guards, normalize_runtime_guard_config
from .inventory_grid_plan import build_inventory_grid_orders
from .inventory_grid_recovery import rebuild_inventory_grid_runtime
from .inventory_grid_state import (
    apply_inventory_grid_fill,
    apply_synthetic_frozen_pair_release,
    freeze_inventory_grid_reduce_lots,
    new_inventory_grid_runtime,
    synthetic_frozen_position_report,
)
from .semi_auto_plan import diff_open_orders
from .submit_plan import _ignore_noop_error
from .spot_flatten_runner import load_live_spot_flatten_snapshot, spot_flatten_client_order_prefix
from .spot_freeze_manager import (
    DeviationLossResult,
    FreezeConfig,
    compute_deviation_loss,
    expected_short_position_now,
    freeze_cycle,
    ledger_totals,
    new_ledger,
)
from .trade_database import ensure_trade_database_schema, persist_cycle_snapshot, persist_trade_rows, trade_database_enabled

STATE_VERSION = 2
EPSILON = 1e-12
PRICE_CACHE_TTL_SECONDS = 30.0
_LATEST_PRICE_CACHE: dict[str, tuple[float, float]] = {}
SPOT_COMPETITION_RUNTIME_CACHE_KEY = "spot_competition_inventory_grid_runtime_cache"
SPOT_SYNTHETIC_NEUTRAL_RUNTIME_CACHE_KEY = "spot_competition_synthetic_neutral_grid_runtime_cache"
SPOT_FREEZE_MAKER_ROLE = "spot_freeze_maker"
SPOT_COMPETITION_MODES = {
    "spot_competition_inventory_grid",
    "spot_competition_synthetic_neutral_grid",
}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _qty_zero_tolerance(*quantities: float) -> float:
    total_qty = sum(abs(_safe_float(value)) for value in quantities)
    return max(EPSILON, total_qty * 1e-12)


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
        "buy_qty": 0.0,
        "sell_qty": 0.0,
        "app_loss_window_aligned": True,
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
    loaded_strategy_mode = str(state.get("strategy_mode") or "").strip()
    loaded_symbol = str(state.get("symbol") or "").upper().strip()
    requested_symbol = str(symbol or "").upper().strip()
    if strategy_mode in SPOT_COMPETITION_MODES and (
        (loaded_strategy_mode and loaded_strategy_mode != strategy_mode)
        or (loaded_symbol and loaded_symbol != requested_symbol)
    ):
        state["known_orders"] = {}
        state["inventory_lots"] = []
        state["seen_trade_ids"] = []
        state["last_trade_time_ms"] = 0
        state["metrics"] = _new_metrics()
        state.pop(SPOT_COMPETITION_RUNTIME_CACHE_KEY, None)
        state.pop(SPOT_SYNTHETIC_NEUTRAL_RUNTIME_CACHE_KEY, None)
    elif strategy_mode not in SPOT_COMPETITION_MODES:
        state.pop(SPOT_COMPETITION_RUNTIME_CACHE_KEY, None)
        state.pop(SPOT_SYNTHETIC_NEUTRAL_RUNTIME_CACHE_KEY, None)
    state["version"] = STATE_VERSION
    state["symbol"] = symbol
    state["strategy_mode"] = strategy_mode
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
    legacy_app_loss_window = (
        ("buy_qty" not in metrics or "sell_qty" not in metrics)
        and _safe_float(metrics.get("buy_notional")) + _safe_float(metrics.get("sell_notional")) > EPSILON
    )
    if legacy_app_loss_window:
        metrics["app_loss_window_aligned"] = False
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


def _competition_runtime_position_qty(runtime: dict[str, Any]) -> float:
    total = 0.0
    long_qty = 0.0
    short_qty = 0.0
    for key in ("position_lots", "frozen_position_lots"):
        lots = runtime.get(key)
        if not isinstance(lots, list):
            continue
        for lot in lots:
            if not isinstance(lot, dict):
                continue
            qty = max(_safe_float(lot.get("qty")), 0.0)
            total += qty
            side = str(lot.get("side", "") or "").strip().lower()
            if side == "short":
                short_qty += qty
            elif side == "long":
                long_qty += qty
    if bool(runtime.get("synthetic_neutral")) or short_qty > EPSILON:
        return abs(long_qty - short_qty)
    return total


def _competition_runtime_frozen_net_qty(runtime: dict[str, Any]) -> float:
    long_qty = 0.0
    short_qty = 0.0
    lots = runtime.get("frozen_position_lots")
    if not isinstance(lots, list):
        return 0.0
    for lot in lots:
        if not isinstance(lot, dict):
            continue
        qty = max(_safe_float(lot.get("qty")), 0.0)
        side = str(lot.get("side", "") or "").strip().lower()
        if side == "short":
            short_qty += qty
        elif side == "long":
            long_qty += qty
    return long_qty - short_qty


def _competition_position_qty_matches(actual: float, expected: float, *, extra_tolerance: float = 0.0) -> bool:
    tolerance = max(EPSILON, max(float(extra_tolerance), 0.0), max(abs(float(actual)), abs(float(expected)), 1.0) * 1e-9)
    return abs(float(actual) - float(expected)) <= tolerance


def _synthetic_position_qty_tolerance(
    *,
    price: float,
    min_qty: float | None,
    min_notional: float | None,
) -> float:
    tolerance = 0.0
    if min_qty is not None:
        tolerance = max(tolerance, max(float(min_qty), 0.0))
    safe_price = max(_safe_float(price), 0.0)
    if min_notional is not None and safe_price > EPSILON:
        tolerance = max(tolerance, max(float(min_notional), 0.0) / safe_price)
    return tolerance


def _competition_runtime_matches_expected_position(
    runtime: dict[str, Any],
    *,
    expected_position_qty: float,
    synthetic_neutral: bool,
    position_qty_tolerance: float = 0.0,
) -> bool:
    expected = max(float(expected_position_qty), 0.0)
    runtime_qty = _competition_runtime_position_qty(runtime)
    if _competition_position_qty_matches(runtime_qty, expected, extra_tolerance=position_qty_tolerance):
        return True
    if not synthetic_neutral:
        return False
    frozen_net_qty = _competition_runtime_frozen_net_qty(runtime)
    return _competition_position_qty_matches(abs(frozen_net_qty), expected, extra_tolerance=position_qty_tolerance)


def _competition_runtime_trade_key(trade: dict[str, Any]) -> str:
    return ":".join(
        (
            str(int(trade.get("time", 0) or 0)),
            str(int(trade.get("id", 0) or 0)),
            str(int(trade.get("orderId", 0) or 0)),
        )
    )


def _spot_competition_runtime_cache_key(*, synthetic_neutral: bool) -> str:
    return SPOT_SYNTHETIC_NEUTRAL_RUNTIME_CACHE_KEY if synthetic_neutral else SPOT_COMPETITION_RUNTIME_CACHE_KEY


def _load_cached_spot_competition_runtime(
    state: dict[str, Any],
    *,
    synthetic_neutral: bool = False,
) -> tuple[dict[str, Any] | None, list[str]]:
    cache_key = _spot_competition_runtime_cache_key(synthetic_neutral=synthetic_neutral)
    expected_market_type = "futures" if synthetic_neutral else "spot"
    expected_strategy_mode = (
        "spot_competition_synthetic_neutral_grid"
        if synthetic_neutral
        else "spot_competition_inventory_grid"
    )
    payload = state.get(cache_key)
    if not isinstance(payload, dict):
        return None, []
    if str(payload.get("strategy_mode", "")).strip() != expected_strategy_mode:
        state.pop(cache_key, None)
        return None, []
    if str(payload.get("market_type", "")).strip().lower() != expected_market_type:
        state.pop(cache_key, None)
        return None, []

    runtime = payload.get("runtime")
    if not isinstance(runtime, dict) or str(runtime.get("market_type", "")).strip().lower() != expected_market_type:
        state.pop(cache_key, None)
        return None, []

    applied_trade_keys = [str(item).strip() for item in list(payload.get("applied_trade_keys") or []) if str(item).strip()]
    return copy.deepcopy(runtime), applied_trade_keys


def _store_cached_spot_competition_runtime(
    *,
    state: dict[str, Any],
    runtime: dict[str, Any],
    applied_trade_keys: list[str],
    synthetic_neutral: bool = False,
) -> None:
    cached_runtime = copy.deepcopy(runtime)
    cached_runtime["pair_credit_steps"] = 0
    if str(cached_runtime.get("recovery_mode", "live") or "live").strip().lower() == "live":
        cached_runtime["recovery_mode"] = "live"
        cached_runtime["recovery_errors"] = []
    cache_key = _spot_competition_runtime_cache_key(synthetic_neutral=synthetic_neutral)
    state[cache_key] = {
        "strategy_mode": (
            "spot_competition_synthetic_neutral_grid"
            if synthetic_neutral
            else "spot_competition_inventory_grid"
        ),
        "market_type": "futures" if synthetic_neutral else "spot",
        "runtime": cached_runtime,
        "applied_trade_keys": list(applied_trade_keys),
    }


def _cached_spot_competition_applied_trade_keys(
    *,
    state: dict[str, Any],
    synthetic_neutral: bool,
) -> list[str]:
    payload = state.get(_spot_competition_runtime_cache_key(synthetic_neutral=synthetic_neutral))
    if not isinstance(payload, dict):
        return []
    return [str(item).strip() for item in list(payload.get("applied_trade_keys") or []) if str(item).strip()]


def _apply_cached_conflicting_bootstrap_fill(
    *,
    runtime: dict[str, Any],
    side: str,
    price: float,
    qty: float,
    fill_time_ms: int,
    step_price: float,
) -> bool:
    if str(runtime.get("market_type", "")).strip().lower() != "futures":
        return False
    current_state = str(runtime.get("direction_state", "flat") or "flat").strip().lower()
    if current_state == "long_active":
        close_side = "SELL"
    elif current_state == "short_active":
        close_side = "BUY"
    else:
        return False
    if str(side).upper().strip() != close_side:
        return False
    available_qty = _sum_inventory_qty(list(runtime.get("position_lots") or []))
    fill_qty = max(float(qty), 0.0)
    close_qty = min(fill_qty, available_qty)
    if close_qty > EPSILON:
        apply_inventory_grid_fill(
            runtime=runtime,
            role="tail_cleanup",
            side=close_side,
            price=price,
            qty=close_qty,
            fill_time_ms=fill_time_ms,
            step_price=step_price,
        )
    remainder_qty = fill_qty - close_qty
    if remainder_qty > EPSILON:
        apply_inventory_grid_fill(
            runtime=runtime,
            role="bootstrap_entry",
            side=side,
            price=price,
            qty=remainder_qty,
            fill_time_ms=fill_time_ms,
            step_price=step_price,
        )
    return True


def _apply_spot_competition_runtime_trade_delta(
    *,
    runtime: dict[str, Any],
    trade: dict[str, Any],
    order_refs: dict[str, dict[str, Any]],
    step_price: float,
) -> bool:
    order_id = str(int(trade.get("orderId", 0) or 0))
    order_ref = order_refs.get(order_id)
    if not isinstance(order_ref, dict):
        return False

    role = str(order_ref.get("role", "") or "").strip()
    side = str(order_ref.get("side", "") or "").upper().strip()
    if not role or not side:
        return False

    price = _safe_float(trade.get("price"))
    qty = abs(_safe_float(trade.get("qty")))
    fill_time_ms = int(trade.get("time", 0) or 0)
    try:
        apply_inventory_grid_fill(
            runtime=runtime,
            role=role,
            side=side,
            price=price,
            qty=qty,
            fill_time_ms=fill_time_ms,
            step_price=step_price,
        )
    except ValueError:
        if role == "bootstrap_entry" and _apply_cached_conflicting_bootstrap_fill(
            runtime=runtime,
            side=side,
            price=price,
            qty=qty,
            fill_time_ms=fill_time_ms,
            step_price=step_price,
        ):
            return True
        return False
    return True


def _resolve_spot_competition_runtime(
    *,
    state: dict[str, Any],
    trades: list[dict[str, Any]],
    order_refs: dict[str, dict[str, Any]],
    step_price: float,
    current_position_qty: float,
    synthetic_neutral: bool = False,
    position_qty_tolerance: float = 0.0,
) -> dict[str, Any]:
    expected_position_qty = max(float(current_position_qty), 0.0)
    runtime_market_type = "futures" if synthetic_neutral else "spot"
    strategy_trades = sorted(
        [
            trade
            for trade in list(trades or [])
            if isinstance(trade, dict) and str(int(trade.get("orderId", 0) or 0)) in order_refs
        ],
        key=lambda row: (
            int(row.get("time", 0) or 0),
            int(row.get("id", 0) or 0),
        ),
    )

    cached_runtime, applied_trade_keys = _load_cached_spot_competition_runtime(
        state,
        synthetic_neutral=synthetic_neutral,
    )
    if cached_runtime is not None:
        applied_trade_key_set = set(applied_trade_keys)
        cache_valid = True
        for trade in strategy_trades:
            trade_key = _competition_runtime_trade_key(trade)
            if trade_key in applied_trade_key_set:
                continue
            if not _apply_spot_competition_runtime_trade_delta(
                runtime=cached_runtime,
                trade=trade,
                order_refs=order_refs,
                step_price=step_price,
            ):
                cache_valid = False
                break
            applied_trade_keys.append(trade_key)
            applied_trade_key_set.add(trade_key)

        cached_runtime["pair_credit_steps"] = 0
        cached_runtime["recovery_mode"] = "live"
        cached_runtime["recovery_errors"] = []
        if cache_valid and _competition_runtime_matches_expected_position(
            cached_runtime,
            expected_position_qty=expected_position_qty,
            synthetic_neutral=synthetic_neutral,
            position_qty_tolerance=position_qty_tolerance,
        ):
            _store_cached_spot_competition_runtime(
                state=state,
                runtime=cached_runtime,
                applied_trade_keys=applied_trade_keys,
                synthetic_neutral=synthetic_neutral,
            )
            return cached_runtime

    runtime = rebuild_inventory_grid_runtime(
        market_type=runtime_market_type,
        trades=strategy_trades,
        order_refs=order_refs,
        step_price=step_price,
        current_position_qty=expected_position_qty,
        allow_conflicting_bootstrap_fills=synthetic_neutral,
    )
    if str(runtime.get("recovery_mode", "live") or "live") == "live":
        _store_cached_spot_competition_runtime(
            state=state,
            runtime=runtime,
            applied_trade_keys=[_competition_runtime_trade_key(trade) for trade in strategy_trades],
            synthetic_neutral=synthetic_neutral,
        )
    elif expected_position_qty <= EPSILON:
        flat_runtime = new_inventory_grid_runtime(market_type=runtime_market_type)
        replayed_trade_keys = [_competition_runtime_trade_key(trade) for trade in strategy_trades]
        _store_cached_spot_competition_runtime(
            state=state,
            runtime=flat_runtime,
            applied_trade_keys=replayed_trade_keys,
            synthetic_neutral=synthetic_neutral,
        )
        return flat_runtime
    return runtime


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
                "net_pnl": realized_pnl,
                "realized_pnl": realized_pnl,
                "commission_quote": commission_quote,
                "gross_notional": _safe_float(row.get("notional")),
                "recycle_loss_abs": recycle_loss_abs,
            }
        )
    return events


def _spot_app_loss_metrics_with_trade_fallback(*, metrics: dict[str, Any], trades: list[dict[str, Any]]) -> dict[str, Any]:
    if (
        _safe_float(metrics.get("buy_notional")) + _safe_float(metrics.get("sell_notional")) > EPSILON
        or _safe_float(metrics.get("buy_qty")) + _safe_float(metrics.get("sell_qty")) > EPSILON
        or not isinstance(trades, list)
    ):
        return metrics
    recent: list[dict[str, Any]] = []
    for trade in trades:
        if not isinstance(trade, dict):
            continue
        qty = max(_safe_float(trade.get("qty")), 0.0)
        price = max(_safe_float(trade.get("price")), 0.0)
        notional = max(_safe_float(trade.get("quoteQty")), 0.0)
        if notional <= EPSILON:
            notional = qty * price
        side = "BUY" if _truthy(trade.get("isBuyer")) else "SELL"
        if qty <= EPSILON or notional <= EPSILON:
            continue
        recent.append(
            {
                "id": _safe_int(trade.get("id")),
                "orderId": _safe_int(trade.get("orderId")),
                "time": _safe_int(trade.get("time")),
                "side": side,
                "price": price,
                "qty": qty,
                "notional": notional,
                "maker": bool(trade.get("isMaker")),
                "role": "app_loss_trade_fallback",
            }
        )
    if not recent:
        return metrics
    fallback = dict(metrics)
    fallback["app_loss_window_aligned"] = False
    fallback["recent_trades"] = recent[-2000:]
    return fallback


def _build_spot_app_loss_guard(
    *,
    enabled: bool,
    metrics: dict[str, Any],
    position_qty: float,
    latest_price: float,
    min_notional: float,
    soft_per_10k: float,
    hard_per_10k: float,
    recovery_reduce_only_enabled: bool = False,
    tick_size: float | None = None,
    min_bid_break_even_buffer_ticks: float = 0.0,
) -> dict[str, Any]:
    buy_notional = max(_safe_float(metrics.get("buy_notional")), 0.0)
    sell_notional = max(_safe_float(metrics.get("sell_notional")), 0.0)
    buy_qty = max(_safe_float(metrics.get("buy_qty")), 0.0)
    sell_qty = max(_safe_float(metrics.get("sell_qty")), 0.0)
    window_source = "metrics"
    window_aligned = bool(metrics.get("app_loss_window_aligned", True))
    need_recent_window = not window_aligned or (buy_notional + sell_notional > EPSILON and buy_qty <= EPSILON and sell_qty <= EPSILON)
    if need_recent_window and isinstance(metrics.get("recent_trades"), list) and metrics.get("recent_trades"):
        recent_buy_notional = 0.0
        recent_sell_notional = 0.0
        recent_buy_qty = 0.0
        recent_sell_qty = 0.0
        for row in metrics.get("recent_trades") or []:
            if not isinstance(row, dict):
                continue
            qty = max(_safe_float(row.get("qty")), 0.0)
            notional = max(_safe_float(row.get("notional")), 0.0)
            if notional <= EPSILON:
                notional = qty * max(_safe_float(row.get("price")), 0.0)
            side = str(row.get("side", "") or "").upper().strip()
            if side == "BUY":
                recent_buy_qty += qty
                recent_buy_notional += notional
            elif side == "SELL":
                recent_sell_qty += qty
                recent_sell_notional += notional
        buy_notional = recent_buy_notional
        sell_notional = recent_sell_notional
        buy_qty = recent_buy_qty
        sell_qty = recent_sell_qty
        window_source = "recent_trades"
        window_aligned = buy_notional + sell_notional > EPSILON
    elif need_recent_window:
        window_source = "position_fallback"
        window_aligned = False

    gross_notional = buy_notional + sell_notional
    if window_aligned and (buy_qty > EPSILON or sell_qty > EPSILON):
        window_net_qty = buy_qty - sell_qty
        if abs(window_net_qty) <= _qty_zero_tolerance(buy_qty, sell_qty):
            window_net_qty = 0.0
        holding_qty = max(window_net_qty, 0.0)
    else:
        holding_qty = max(_safe_float(position_qty), 0.0)
    mark_price = max(_safe_float(latest_price), 0.0)
    position_value = holding_qty * mark_price
    raw_loss = buy_notional - sell_notional - position_value
    app_loss = max(raw_loss, 0.0)
    app_loss_per_10k = app_loss / gross_notional * 10000.0 if gross_notional > EPSILON else 0.0
    tick = max(_safe_float(tick_size), 0.0)
    break_even_price = (
        max((buy_notional - sell_notional) / holding_qty, 0.0)
        if window_aligned and holding_qty > EPSILON
        else 0.0
    )
    bid_break_even_buffer_ticks = (
        (mark_price - break_even_price) / tick
        if tick > EPSILON and mark_price > EPSILON and break_even_price > EPSILON
        else 0.0
    )
    safe_min_notional = max(_safe_float(min_notional), 0.0)
    hard = max(_safe_float(hard_per_10k), 0.0)
    soft = max(_safe_float(soft_per_10k), 0.0)
    volume_active = gross_notional + EPSILON >= safe_min_notional
    min_bid_buffer = max(_safe_float(min_bid_break_even_buffer_ticks), 0.0)
    bid_buffer_below_min = (
        bool(enabled)
        and bool(window_aligned)
        and volume_active
        and min_bid_buffer > EPSILON
        and holding_qty > EPSILON
        and tick > EPSILON
        and break_even_price > EPSILON
        and bid_break_even_buffer_ticks + EPSILON < min_bid_buffer
    )
    preactive_loss_cap = hard * safe_min_notional / 10000.0 if hard > EPSILON and safe_min_notional > EPSILON else 0.0
    preactive_loss_cap_hit = (
        bool(enabled)
        and bool(window_aligned)
        and not volume_active
        and preactive_loss_cap > EPSILON
        and app_loss + EPSILON >= preactive_loss_cap
    )
    active = bool(enabled) and bool(window_aligned) and (volume_active or preactive_loss_cap_hit)
    recovery_reduce_only = (
        bool(enabled)
        and bool(recovery_reduce_only_enabled)
        and bool(window_aligned)
        and gross_notional > EPSILON
        and holding_qty > EPSILON
        and app_loss_per_10k <= hard + EPSILON
    )
    if not enabled:
        state = "disabled"
    elif not window_aligned:
        state = "insufficient_window"
    elif preactive_loss_cap_hit:
        state = "blocked"
    elif bid_buffer_below_min:
        state = "blocked"
    elif not active:
        state = "warming_up"
    elif hard > EPSILON and app_loss_per_10k + EPSILON >= hard:
        state = "blocked"
    elif soft > EPSILON and app_loss_per_10k + EPSILON >= soft:
        state = "defensive"
    elif recovery_reduce_only:
        state = "recovery_reduce_only"
    else:
        state = "cruise"
    return {
        "enabled": bool(enabled),
        "formula": "buy_notional-sell_notional-position_qty*latest_price",
        "window_source": window_source,
        "window_aligned": bool(window_aligned),
        "buy_notional": buy_notional,
        "sell_notional": sell_notional,
        "buy_qty": buy_qty,
        "sell_qty": sell_qty,
        "gross_notional": gross_notional,
        "position_qty": holding_qty,
        "latest_price": mark_price,
        "position_value": position_value,
        "raw_app_loss": raw_loss,
        "app_loss": app_loss,
        "app_loss_per_10k": app_loss_per_10k,
        "break_even_price": break_even_price,
        "bid_break_even_buffer_ticks": bid_break_even_buffer_ticks,
        "min_bid_break_even_buffer_ticks": min_bid_buffer,
        "bid_break_even_buffer_below_min": bid_buffer_below_min,
        "min_notional": safe_min_notional,
        "soft_per_10k": soft,
        "hard_per_10k": hard,
        "preactive_loss_cap": preactive_loss_cap,
        "preactive_loss_cap_hit": preactive_loss_cap_hit,
        "recovery_reduce_only_enabled": bool(recovery_reduce_only_enabled),
        "active": active,
        "state": state,
        "action": "observe",
        "reduce_side": "",
        "dropped_order_count": 0,
    }


def _spot_app_loss_guard_stop_reason(guard: dict[str, Any]) -> str:
    if not isinstance(guard, dict):
        return ""
    if str(guard.get("state", "") or "").strip().lower() != "blocked":
        return ""
    if bool(guard.get("preactive_loss_cap_hit")):
        return "spot_app_loss_preactive_cap_hit"
    if bool(guard.get("bid_break_even_buffer_below_min")):
        return "spot_app_loss_bid_buffer_below_min"
    hard = max(_safe_float(guard.get("hard_per_10k")), 0.0)
    if hard > EPSILON and _safe_float(guard.get("app_loss_per_10k")) + EPSILON >= hard:
        return "spot_app_loss_hard_limit_hit"
    return ""


def _spot_app_loss_guard_should_stop_runner(guard: dict[str, Any]) -> bool:
    return bool(_spot_app_loss_guard_stop_reason(guard))


def _spot_app_loss_mark_price(*, metrics: dict[str, Any], bid_price: float, ask_price: float) -> float:
    bid = max(_safe_float(bid_price), 0.0)
    ask = max(_safe_float(ask_price), 0.0)
    if bid <= EPSILON:
        return ask
    if ask <= EPSILON:
        return bid
    buy_qty = max(_safe_float(metrics.get("buy_qty")), 0.0)
    sell_qty = max(_safe_float(metrics.get("sell_qty")), 0.0)
    if buy_qty <= EPSILON and sell_qty <= EPSILON and isinstance(metrics.get("recent_trades"), list):
        for row in metrics.get("recent_trades") or []:
            if not isinstance(row, dict):
                continue
            qty = max(_safe_float(row.get("qty")), 0.0)
            side = str(row.get("side", "") or "").upper().strip()
            if side == "BUY":
                buy_qty += qty
            elif side == "SELL":
                sell_qty += qty
    if buy_qty > sell_qty + EPSILON:
        return bid
    return (bid + ask) / 2.0


def _spot_app_loss_reduce_side(controls: dict[str, Any], position_qty: float, app_position_qty: float | None = None) -> str:
    neutral_base_qty = max(_safe_float(controls.get("neutral_base_qty")), 0.0)
    if neutral_base_qty > EPSILON or "synthetic_net_qty" in controls:
        deviation_qty = _safe_float(controls.get("actual_base_qty")) - neutral_base_qty
        if deviation_qty > EPSILON:
            return "SELL"
        if deviation_qty < -EPSILON:
            return "BUY"
        return ""
    if app_position_qty is not None and _safe_float(app_position_qty) > EPSILON:
        return "SELL"
    if _safe_float(position_qty) > EPSILON:
        return "SELL"
    return ""


def _spot_app_loss_deviation_qty(controls: dict[str, Any], position_qty: float) -> float:
    neutral_base_qty = max(_safe_float(controls.get("neutral_base_qty")), 0.0)
    if neutral_base_qty > EPSILON or "synthetic_net_qty" in controls:
        return abs(_safe_float(controls.get("actual_base_qty")) - neutral_base_qty)
    guard = controls.get("spot_app_loss_guard")
    if isinstance(guard, dict):
        app_position_qty = max(_safe_float(guard.get("position_qty")), 0.0)
        if app_position_qty > EPSILON:
            return app_position_qty
    return max(_safe_float(position_qty), 0.0)


def _spot_app_loss_break_even_price(controls: dict[str, Any]) -> float:
    guard = controls.get("spot_app_loss_guard")
    if not isinstance(guard, dict):
        return 0.0
    app_position_qty = max(_safe_float(guard.get("position_qty")), 0.0)
    if app_position_qty <= EPSILON:
        return 0.0
    return max((_safe_float(guard.get("buy_notional")) - _safe_float(guard.get("sell_notional"))) / app_position_qty, 0.0)


def _cap_spot_app_loss_reduce_orders(
    *,
    orders: list[dict[str, Any]],
    controls: dict[str, Any],
    reduce_side: str,
    position_qty: float,
    maker_reference_price: float | None,
    step_size: float | None,
    min_qty: float | None,
    min_notional: float | None,
    available_quote_free: float | None,
    available_base_free: float | None,
) -> list[dict[str, Any]]:
    normalized_side = str(reduce_side or "").upper().strip()
    if normalized_side not in {"BUY", "SELL"}:
        return []
    remaining_qty = _spot_app_loss_deviation_qty(controls, position_qty)
    min_sell_price = 0.0
    max_buy_price = 0.0
    if normalized_side == "SELL":
        min_sell_price = max(_spot_app_loss_break_even_price(controls), _safe_float(maker_reference_price))
    elif normalized_side == "BUY":
        max_buy_price = _safe_float(maker_reference_price)
    if normalized_side == "SELL" and available_base_free is not None:
        remaining_qty = min(remaining_qty, max(_safe_float(available_base_free), 0.0))
    remaining_quote = None
    if normalized_side == "BUY" and available_quote_free is not None:
        remaining_quote = max(_safe_float(available_quote_free), 0.0)
    capped_orders: list[dict[str, Any]] = []
    for order in orders:
        if str(order.get("side", "") or "").upper().strip() != normalized_side:
            continue
        if remaining_qty <= EPSILON:
            break
        price = max(_safe_float(order.get("price")), 0.0)
        if price <= EPSILON:
            continue
        if normalized_side == "SELL" and min_sell_price > EPSILON and price + EPSILON < min_sell_price:
            continue
        if normalized_side == "BUY" and max_buy_price > EPSILON and price > max_buy_price + EPSILON:
            continue
        raw_qty = min(max(_safe_float(order.get("qty")), 0.0), remaining_qty)
        if remaining_quote is not None:
            raw_qty = min(raw_qty, remaining_quote / price if price > EPSILON else 0.0)
        qty = _round_order_qty(raw_qty, step_size)
        if not _spot_order_meets_exchange_mins(qty=qty, price=price, min_qty=min_qty, min_notional=min_notional):
            break
        capped = dict(order)
        capped["qty"] = qty
        capped["notional"] = qty * price
        capped.setdefault("source_role", str(order.get("role", "") or ""))
        capped["role"] = "spot_app_loss_reduce"
        capped["tag"] = "app_loss_reduce"
        capped_orders.append(capped)
        remaining_qty = max(remaining_qty - qty, 0.0)
        if remaining_quote is not None:
            remaining_quote = max(remaining_quote - qty * price, 0.0)
    return capped_orders


def _drop_spot_app_loss_projected_bid_buffer_buy_orders(
    *,
    orders: list[dict[str, Any]],
    controls: dict[str, Any],
    guard: dict[str, Any],
    tick_size: float | None,
) -> list[dict[str, Any]]:
    min_bid_buffer = max(_safe_float(guard.get("min_bid_break_even_buffer_ticks")), 0.0)
    tick = max(_safe_float(tick_size), 0.0)
    mark_price = max(_safe_float(guard.get("latest_price")), 0.0)
    if (
        min_bid_buffer <= EPSILON
        or tick <= EPSILON
        or mark_price <= EPSILON
        or not bool(guard.get("enabled"))
        or not bool(guard.get("window_aligned"))
        or _safe_float(guard.get("gross_notional")) + EPSILON < _safe_float(guard.get("min_notional"))
        or bool(guard.get("bid_break_even_buffer_below_min"))
        or str(guard.get("state", "") or "").strip().lower() in {"defensive", "blocked", "recovery_reduce_only"}
    ):
        guard["projected_bid_buffer_dropped_buy_count"] = 0
        return orders
    neutral_base_qty = max(_safe_float(controls.get("neutral_base_qty")), 0.0)
    recovery_budget_qty = 0.0
    if neutral_base_qty > EPSILON or "synthetic_net_qty" in controls:
        recovery_budget_qty = max(neutral_base_qty - _safe_float(controls.get("actual_base_qty")), 0.0)

    buy_notional = max(_safe_float(guard.get("buy_notional")), 0.0)
    sell_notional = max(_safe_float(guard.get("sell_notional")), 0.0)
    buy_qty = max(_safe_float(guard.get("buy_qty")), 0.0)
    sell_qty = max(_safe_float(guard.get("sell_qty")), 0.0)
    kept: list[dict[str, Any]] = []
    dropped = 0
    for order in orders:
        if str(order.get("side", "") or "").upper().strip() != "BUY":
            kept.append(order)
            continue
        price = max(_safe_float(order.get("price")), 0.0)
        qty = max(_safe_float(order.get("qty")), 0.0)
        notional = max(_safe_float(order.get("notional")), 0.0)
        if notional <= EPSILON:
            notional = price * qty
        projected_qty = buy_qty + qty - sell_qty
        projected_notional = buy_notional + notional - sell_notional
        projected_break_even = projected_notional / projected_qty if projected_qty > EPSILON else 0.0
        projected_buffer = (mark_price - projected_break_even) / tick if projected_break_even > EPSILON else min_bid_buffer
        if recovery_budget_qty > EPSILON:
            if qty <= recovery_budget_qty + EPSILON:
                kept.append(order)
                recovery_budget_qty = max(recovery_budget_qty - qty, 0.0)
                buy_qty += qty
                buy_notional += notional
                continue
            if projected_buffer + EPSILON >= min_bid_buffer:
                kept.append(order)
                recovery_budget_qty = 0.0
                buy_qty += qty
                buy_notional += notional
                continue
            recovery_budget_qty = 0.0
            dropped += 1
            continue
        if projected_buffer + EPSILON < min_bid_buffer:
            dropped += 1
            continue
        kept.append(order)
        buy_qty += qty
        buy_notional += notional
    guard["projected_bid_buffer_dropped_buy_count"] = dropped
    if dropped > 0:
        pause_reasons = list(controls.get("pause_reasons") or [])
        reason = "spot_app_loss_guard_projected_bid_buffer_below_min"
        if reason not in pause_reasons:
            pause_reasons.append(reason)
        controls["pause_reasons"] = pause_reasons
        controls["buy_paused"] = True
    return kept


def _build_spot_app_loss_maker_reduce_order(
    *,
    controls: dict[str, Any],
    latest_price: float,
    maker_reference_price: float | None = None,
    reduce_side: str,
    maker_reduce_notional: float,
    tick_size: float | None,
    step_size: float | None,
    min_qty: float | None,
    min_notional: float | None,
    available_quote_free: float | None,
    available_base_free: float | None,
) -> dict[str, Any] | None:
    normalized_side = str(reduce_side or "").upper().strip()
    price = max(_safe_float(maker_reference_price if maker_reference_price is not None else latest_price), 0.0)
    if normalized_side == "SELL":
        price = max(price, _spot_app_loss_break_even_price(controls))
    price = _round_order_price(price, tick_size, normalized_side)
    notional_cap = max(_safe_float(maker_reduce_notional), 0.0)
    if normalized_side not in {"BUY", "SELL"} or price <= EPSILON or notional_cap <= EPSILON:
        return None

    if "neutral_base_qty" not in controls and "synthetic_net_qty" not in controls:
        return None
    deviation_qty = _spot_app_loss_deviation_qty(controls, 0.0)
    if normalized_side == "BUY":
        if available_quote_free is not None:
            notional_cap = min(notional_cap, max(_safe_float(available_quote_free), 0.0))
        raw_qty = min(deviation_qty, notional_cap / price)
    else:
        raw_qty = min(deviation_qty, notional_cap / price)
        if available_base_free is not None:
            raw_qty = min(raw_qty, max(_safe_float(available_base_free), 0.0))
    qty = _round_order_qty(raw_qty, step_size)
    if not _spot_order_meets_exchange_mins(qty=qty, price=price, min_qty=min_qty, min_notional=min_notional):
        return None
    return {
        "side": normalized_side,
        "price": price,
        "qty": qty,
        "role": "spot_app_loss_reduce",
        "tag": "app_loss_reduce",
    }


def _apply_spot_app_loss_guard_to_orders(
    *,
    desired_orders: list[dict[str, Any]],
    controls: dict[str, Any],
    metrics: dict[str, Any],
    position_qty: float,
    latest_price: float,
    enabled: bool,
    recovery_reduce_only_enabled: bool = False,
    min_notional: float,
    soft_per_10k: float,
    hard_per_10k: float,
    maker_reduce_notional: float = 0.0,
    maker_reference_price: float | None = None,
    maker_buy_reference_price: float | None = None,
    maker_sell_reference_price: float | None = None,
    tick_size: float | None = None,
    step_size: float | None = None,
    min_qty: float | None = None,
    exchange_min_notional: float | None = None,
    available_quote_free: float | None = None,
    available_base_free: float | None = None,
    min_bid_break_even_buffer_ticks: float = 0.0,
) -> list[dict[str, Any]]:
    guard = _build_spot_app_loss_guard(
        enabled=enabled,
        metrics=metrics,
        position_qty=position_qty,
        latest_price=latest_price,
        min_notional=min_notional,
        soft_per_10k=soft_per_10k,
        hard_per_10k=hard_per_10k,
        recovery_reduce_only_enabled=recovery_reduce_only_enabled,
        tick_size=tick_size,
        min_bid_break_even_buffer_ticks=min_bid_break_even_buffer_ticks,
    )
    controls["spot_app_loss_guard"] = guard
    reduce_side = ""
    if guard["state"] == "recovery_reduce_only":
        reduce_side = _spot_app_loss_reduce_side(controls, position_qty, app_position_qty=guard.get("position_qty"))
        if not reduce_side:
            guard["state"] = "cruise"
            guard["action"] = "observe"
            guard["reduce_side"] = ""
            guard["dropped_order_count"] = 0
            guard["injected_order_count"] = 0
            guard["capped_order_count"] = 0
            guard["reduce_deviation_qty"] = 0.0
    desired_orders = _drop_spot_app_loss_projected_bid_buffer_buy_orders(
        orders=desired_orders,
        controls=controls,
        guard=guard,
        tick_size=tick_size,
    )
    if guard["state"] not in {"defensive", "blocked", "recovery_reduce_only"}:
        return desired_orders

    if not reduce_side:
        reduce_side = _spot_app_loss_reduce_side(controls, position_qty, app_position_qty=guard.get("position_qty"))
    if reduce_side == "BUY":
        reduce_reference_price = maker_buy_reference_price if maker_buy_reference_price is not None else maker_reference_price
    else:
        reduce_reference_price = maker_sell_reference_price if maker_sell_reference_price is not None else maker_reference_price
    kept_orders = _cap_spot_app_loss_reduce_orders(
        orders=desired_orders,
        controls=controls,
        reduce_side=reduce_side,
        position_qty=position_qty,
        maker_reference_price=reduce_reference_price,
        step_size=step_size,
        min_qty=min_qty,
        min_notional=exchange_min_notional,
        available_quote_free=available_quote_free,
        available_base_free=available_base_free,
    )
    dropped_count = len(desired_orders) - len(kept_orders)
    guard["action"] = "reduce_only"
    guard["reduce_side"] = reduce_side
    guard["dropped_order_count"] = dropped_count
    guard["injected_order_count"] = 0
    guard["capped_order_count"] = len(kept_orders)
    guard["reduce_deviation_qty"] = _spot_app_loss_deviation_qty(controls, position_qty) if reduce_side else 0.0
    if not kept_orders and reduce_side:
        reduce_order = _build_spot_app_loss_maker_reduce_order(
            controls=controls,
            latest_price=latest_price,
            maker_reference_price=reduce_reference_price,
            reduce_side=reduce_side,
            maker_reduce_notional=maker_reduce_notional,
            tick_size=tick_size,
            step_size=step_size,
            min_qty=min_qty,
            min_notional=exchange_min_notional,
            available_quote_free=available_quote_free,
            available_base_free=available_base_free,
        )
        if reduce_order is not None:
            kept_orders = [reduce_order]
            guard["injected_order_count"] = 1
    if dropped_count > 0:
        pause_reasons = list(controls.get("pause_reasons") or [])
        if guard["state"] == "blocked":
            if bool(guard.get("bid_break_even_buffer_below_min")):
                reason = "spot_app_loss_guard_bid_buffer_below_min"
            else:
                reason = "spot_app_loss_guard_blocked"
        elif guard["state"] == "recovery_reduce_only":
            reason = "spot_app_loss_guard_recovery_reduce_only"
        else:
            reason = "spot_app_loss_guard_defensive"
        if reason not in pause_reasons:
            pause_reasons.append(reason)
        controls["pause_reasons"] = pause_reasons
        desired_buy_count = sum(1 for order in desired_orders if str(order.get("side", "") or "").upper().strip() == "BUY")
        kept_buy_count = sum(1 for order in kept_orders if str(order.get("side", "") or "").upper().strip() == "BUY")
        if desired_buy_count > kept_buy_count:
            controls["buy_paused"] = True
    return kept_orders


def _apply_spot_freeze_maker_orders_to_desired_orders(
    *,
    desired_orders: list[dict[str, Any]],
    maker_orders: list[dict[str, Any]],
    controls: dict[str, Any],
) -> list[dict[str, Any]]:
    normalized_maker_orders = [dict(order) for order in list(maker_orders or []) if isinstance(order, dict)]
    if not normalized_maker_orders:
        controls["spot_freeze_suppressed_app_loss_reduce_orders"] = 0
        return desired_orders
    maker_sides = {str(order.get("side", "") or "").upper().strip() for order in normalized_maker_orders}
    filtered: list[dict[str, Any]] = []
    suppressed = 0
    for order in list(desired_orders or []):
        if (
            str(order.get("role", "") or "").strip() == "spot_app_loss_reduce"
            and str(order.get("side", "") or "").upper().strip() in maker_sides
        ):
            suppressed += 1
            continue
        filtered.append(order)
    controls["spot_freeze_suppressed_app_loss_reduce_orders"] = suppressed
    return filtered + normalized_maker_orders


def _apply_spot_freeze_runtime_hedge_block(
    *,
    strategy_mode: str,
    desired_orders: list[dict[str, Any]],
    controls: dict[str, Any],
) -> list[dict[str, Any]]:
    controls["spot_freeze_runtime_blocked"] = False
    if strategy_mode != "spot_competition_synthetic_neutral_grid":
        return desired_orders
    if not bool(controls.get("spot_freeze_enabled")):
        return desired_orders
    skip_reason = str(controls.get("spot_freeze_skip_reason", "") or "").strip()
    blocking_reasons = {"hedge_gate_failed", "total_cap_reached", "short_hedge_capacity_exhausted"}
    if skip_reason not in blocking_reasons:
        return desired_orders

    controls["spot_freeze_runtime_blocked"] = True
    controls["buy_paused"] = True
    pause_reasons = list(controls.get("pause_reasons") or [])
    reason = f"spot_freeze_{skip_reason}"
    if reason not in pause_reasons:
        pause_reasons.append(reason)
    controls["pause_reasons"] = pause_reasons
    controls["risk_state"] = reason
    return []


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
        metrics["buy_qty"] = _safe_float(metrics.get("buy_qty")) + qty
        metrics["buy_count"] = _safe_int(metrics.get("buy_count")) + 1
    else:
        metrics["sell_notional"] = _safe_float(metrics.get("sell_notional")) + notional
        metrics["sell_qty"] = _safe_float(metrics.get("sell_qty")) + qty
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


def _spot_trade_audit_rows_from_metrics(metrics: dict[str, Any], *, symbol: str) -> list[dict[str, Any]]:
    recent = metrics.get("recent_trades")
    if not isinstance(recent, list):
        return []
    rows: list[dict[str, Any]] = []
    for row in recent:
        if not isinstance(row, dict):
            continue
        trade_id = _safe_int(row.get("id"))
        rows.append(
            {
                "id": trade_id,
                "orderId": row.get("orderId") or row.get("order_id") or trade_id,
                "time": row.get("time"),
                "symbol": symbol,
                "side": row.get("side"),
                "price": row.get("price"),
                "qty": row.get("qty"),
                "quoteQty": row.get("notional"),
                "commission": row.get("commission"),
                "commissionAsset": row.get("commission_asset"),
                "commission_quote": row.get("commission_quote"),
                "realizedPnl": row.get("realized_pnl"),
                "maker": row.get("maker"),
                "role": row.get("role"),
                "source": "spot_runner_metrics",
            }
        )
    return rows


def _persist_spot_trade_database(
    *,
    symbol: str,
    strategy_mode: str,
    args: argparse.Namespace,
    metrics: dict[str, Any],
) -> dict[str, Any]:
    status: dict[str, Any] = {"enabled": trade_database_enabled(), "trade_inserted": 0, "income_inserted": 0}
    if not trade_database_enabled():
        return status
    rows = _spot_trade_audit_rows_from_metrics(metrics, symbol=symbol)
    if not rows:
        return status
    try:
        ensure_trade_database_schema()
        return persist_trade_rows(
            symbol=symbol,
            market_type="spot",
            strategy_mode=strategy_mode,
            config=vars(args),
            trade_rows=rows,
            income_rows=[],
        )
    except Exception as exc:
        return {
            "enabled": True,
            "trade_inserted": 0,
            "income_inserted": 0,
            "error": f"{type(exc).__name__}: {exc}",
        }


def _persist_spot_cycle_database(
    *,
    symbol: str,
    strategy_mode: str,
    args: argparse.Namespace,
    summary: dict[str, Any],
) -> dict[str, Any]:
    status: dict[str, Any] = {"enabled": trade_database_enabled(), "cycle_inserted": 0}
    if not trade_database_enabled():
        return status
    try:
        ensure_trade_database_schema()
        return persist_cycle_snapshot(
            symbol=symbol,
            market_type="spot",
            strategy_mode=strategy_mode,
            config=vars(args),
            summary=summary,
        )
    except Exception as exc:
        return {
            "enabled": True,
            "cycle_inserted": 0,
            "error": f"{type(exc).__name__}: {exc}",
        }


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
    if str(state.get("strategy_mode") or "").strip() in SPOT_COMPETITION_MODES:
        state["known_orders"] = known_orders
    else:
        now_ms = int(time.time() * 1000)
        cutoff_ms = now_ms - 7 * 24 * 3600 * 1000
        state["known_orders"] = {
            key: value
            for key, value in known_orders.items()
            if int((value or {}).get("created_at_ms", now_ms) or now_ms) >= cutoff_ms
        }
    return applied


def _record_spot_freeze_maker_fill(
    *,
    state: dict[str, Any],
    trade: dict[str, Any],
    side: str,
    price: float,
    qty: float,
) -> None:
    normalized_side = str(side or "").upper().strip()
    freeze_qty = max(_safe_float(qty), 0.0)
    fill_price = max(_safe_float(price), 0.0)
    trade_id = _safe_int(trade.get("id"))
    if normalized_side not in {"BUY", "SELL"} or freeze_qty <= EPSILON or fill_price <= EPSILON or trade_id <= 0:
        return
    ledger = state.get("spot_frozen_ledger") if isinstance(state.get("spot_frozen_ledger"), dict) else new_ledger()
    ledger.setdefault("long_lots", [])
    ledger.setdefault("short_lots", [])
    ledger.setdefault("pending_contract_actions", [])
    lot_side = "long" if normalized_side == "SELL" else "short"
    lot_key = "long_lots" if lot_side == "long" else "short_lots"
    contract_side = "BUY" if lot_side == "long" else "SELL"
    reason = "freeze_long_hedge" if lot_side == "long" else "freeze_short_hedge"
    lot_id = f"spot_freeze_maker_{trade_id}"
    if any(str(lot.get("lot_id") or "") == lot_id for lot in list(ledger.get("long_lots") or []) + list(ledger.get("short_lots") or [])):
        return
    if any(str(action.get("lot_id") or "") == lot_id for action in list(ledger.get("pending_contract_actions") or [])):
        return
    ledger[lot_key].append(
        {
            "lot_id": lot_id,
            "qty": freeze_qty,
            "cost_price": fill_price,
            "frozen_at": _safe_int(trade.get("time")),
            "frozen_mid": fill_price,
            "hedge_pending": True,
            "spot_order_id": _safe_int(trade.get("orderId")),
            "spot_trade_id": trade_id,
        }
    )
    ledger["pending_contract_actions"].append(
        {"side": contract_side, "position_side": "SHORT", "qty": freeze_qty, "reason": reason, "lot_id": lot_id}
    )
    ledger_totals(ledger)
    state["spot_frozen_ledger"] = ledger


def _spot_freeze_open_maker_orders(open_orders: list[dict[str, Any]], state: dict[str, Any]) -> list[dict[str, Any]]:
    if not isinstance(state.get("known_orders"), dict):
        state["known_orders"] = {}
    known_orders = state["known_orders"]
    matched: list[dict[str, Any]] = []
    for order in list(open_orders or []):
        if not isinstance(order, dict):
            continue
        order_id = str(_safe_int(order.get("orderId", order.get("order_id"))))
        meta = known_orders.get(order_id)
        client_order_id = str(order.get("clientOrderId", order.get("client_order_id", "")) or "")
        matched_by_meta = isinstance(meta, dict) and str(meta.get("role", "") or "").strip() == SPOT_FREEZE_MAKER_ROLE
        matched_by_client_id = SPOT_FREEZE_MAKER_ROLE in client_order_id or "spot_freeze_" in client_order_id
        if matched_by_meta or matched_by_client_id:
            side = str(order.get("side", "") or "").upper().strip()
            if "spot_freeze_short" in client_order_id:
                tag = "spot_freeze_short"
            elif "spot_freeze_long" in client_order_id:
                tag = "spot_freeze_long"
            elif side == "BUY":
                tag = "spot_freeze_short"
            elif side == "SELL":
                tag = "spot_freeze_long"
            else:
                tag = ""
            if matched_by_client_id and not isinstance(meta, dict) and order_id != "0" and tag:
                known_orders[order_id] = {
                    "cell_idx": 0,
                    "side": side,
                    "client_order_id": client_order_id,
                    "created_at_ms": _safe_int(
                        order.get("time", order.get("workingTime", order.get("transactTime", order.get("updateTime", 0))))
                    ),
                    "role": SPOT_FREEZE_MAKER_ROLE,
                    "tag": tag,
                }
            matched.append(order)
    return matched


def _sync_synthetic_neutral_trades(
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
        if side not in {"BUY", "SELL"}:
            continue
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
        effective_qty = trade_qty
        if commission_asset == base_asset:
            if side == "BUY":
                effective_qty = max(trade_qty - commission, 0.0)
            elif side == "SELL":
                effective_qty = trade_qty + commission
        _record_trade_metrics(
            metrics=metrics,
            trade=trade,
            side=side,
            price=price,
            qty=trade_qty,
            commission_quote=commission_quote,
            commission_asset=commission_asset,
            commission_raw=commission,
            realized_pnl=-commission_quote,
            role=str(meta.get("role", "") or ""),
        )
        if str(meta.get("role", "") or "").strip() == SPOT_FREEZE_MAKER_ROLE:
            _record_spot_freeze_maker_fill(
                state=state,
                trade=trade,
                side=side,
                price=price,
                qty=effective_qty,
            )
        seen_ids.add(trade_id)
        trade_time_ms = int(trade.get("time", 0) or 0)
        if trade_time_ms > last_trade_time_ms:
            last_trade_time_ms = trade_time_ms
        applied += 1
    state["seen_trade_ids"] = sorted(seen_ids)[-5000:]
    state["last_trade_time_ms"] = last_trade_time_ms
    state["known_orders"] = known_orders
    return applied


def _refresh_spot_trades_after_account_snapshot(
    *,
    state: dict[str, Any],
    trades: list[dict[str, Any]],
    symbol: str,
    api_key: str,
    api_secret: str,
    trade_start_ms: int,
    base_asset: str,
    quote_asset: str,
) -> tuple[list[dict[str, Any]], int]:
    refreshed_trades = fetch_spot_user_trades(
        symbol=symbol,
        api_key=api_key,
        api_secret=api_secret,
        start_time_ms=trade_start_ms,
        limit=1000,
    )
    current_keys = {_competition_runtime_trade_key(trade) for trade in list(trades or []) if isinstance(trade, dict)}
    refreshed_keys = {_competition_runtime_trade_key(trade) for trade in list(refreshed_trades or []) if isinstance(trade, dict)}
    if refreshed_keys == current_keys or not current_keys.issubset(refreshed_keys):
        return trades, 0
    applied = _sync_synthetic_neutral_trades(
        state=state,
        trades=refreshed_trades,
        base_asset=base_asset,
        quote_asset=quote_asset,
    )
    return refreshed_trades, applied


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


def _spot_trade_window_move(symbol: str, *, seconds: int, now: datetime | None = None) -> dict[str, Any]:
    window_seconds = max(int(seconds), 1)
    current_time = now or _utc_now()
    end_ms = int(current_time.timestamp() * 1000)
    start_ms = end_ms - window_seconds * 1000
    result: dict[str, Any] = {
        "seconds": window_seconds,
        "available": False,
        "warning": None,
        "open": 0.0,
        "high": 0.0,
        "low": 0.0,
        "close": 0.0,
        "return_ratio": 0.0,
        "amplitude_ratio": 0.0,
        "trade_count": 0,
    }
    try:
        trades = fetch_spot_agg_trades(symbol=symbol, start_ms=start_ms, end_ms=end_ms, limit=1000)
    except Exception as exc:
        result["warning"] = f"{exc.__class__.__name__}: {exc}"
        return result
    prices: list[float] = []
    for trade in trades:
        price = _safe_float(trade.get("p"))
        if price > 0:
            prices.append(price)
    if len(prices) < 2:
        result["warning"] = "insufficient_trades"
        result["trade_count"] = len(prices)
        return result
    open_price = prices[0]
    close_price = prices[-1]
    high_price = max(prices)
    low_price = min(prices)
    result.update(
        {
            "available": True,
            "open": open_price,
            "high": high_price,
            "low": low_price,
            "close": close_price,
            "return_ratio": (close_price / open_price - 1.0) if open_price > 0 else 0.0,
            "amplitude_ratio": (high_price / low_price - 1.0) if low_price > 0 else 0.0,
            "trade_count": len(prices),
        }
    )
    return result


def _assess_spot_fast_stop_guard(
    *,
    symbol: str,
    current_long_notional: float,
    freeze_position_notional: float,
    exit_position_notional: float,
    trigger_10s_abs_return_ratio: float,
    trigger_10s_amplitude_ratio: float,
    trigger_30s_abs_return_ratio: float,
    trigger_30s_amplitude_ratio: float,
    down_only: bool = True,
    now: datetime | None = None,
) -> dict[str, Any]:
    enabled = any(
        threshold > 0
        for threshold in (
            freeze_position_notional,
            exit_position_notional,
            trigger_10s_abs_return_ratio,
            trigger_10s_amplitude_ratio,
            trigger_30s_abs_return_ratio,
            trigger_30s_amplitude_ratio,
        )
    )
    result: dict[str, Any] = {
        "enabled": enabled,
        "available": False,
        "freeze_buy": False,
        "force_exit": False,
        "reasons": [],
        "windows": {},
        "current_long_notional": max(_safe_float(current_long_notional), 0.0),
        "freeze_position_notional": max(_safe_float(freeze_position_notional), 0.0),
        "exit_position_notional": max(_safe_float(exit_position_notional), 0.0),
    }
    if not enabled:
        return result
    windows = {
        "10s": _spot_trade_window_move(symbol, seconds=10, now=now),
        "30s": _spot_trade_window_move(symbol, seconds=30, now=now),
    }
    result["windows"] = windows
    result["available"] = any(bool(item.get("available")) for item in windows.values())
    move_reasons: list[str] = []
    for label, window in windows.items():
        if not window.get("available"):
            continue
        ret = _safe_float(window.get("return_ratio"))
        amp = _safe_float(window.get("amplitude_ratio"))
        if down_only and ret >= 0:
            continue
        abs_ret_threshold = trigger_10s_abs_return_ratio if label == "10s" else trigger_30s_abs_return_ratio
        amp_threshold = trigger_10s_amplitude_ratio if label == "10s" else trigger_30s_amplitude_ratio
        if abs_ret_threshold > 0 and abs(ret) >= abs_ret_threshold:
            move_reasons.append(f"{label}_fast_return ret={ret * 100:.3f}%")
        if amp_threshold > 0 and amp >= amp_threshold:
            move_reasons.append(f"{label}_fast_amplitude amp={amp * 100:.3f}%")
    if not move_reasons:
        return result
    current_notional = max(_safe_float(current_long_notional), 0.0)
    freeze_notional = max(_safe_float(freeze_position_notional), 0.0)
    exit_notional = max(_safe_float(exit_position_notional), 0.0)
    if freeze_notional <= 0 or current_notional >= freeze_notional:
        result["freeze_buy"] = True
    if exit_notional > 0 and current_notional >= exit_notional:
        result["force_exit"] = True
    result["reasons"] = move_reasons
    return result


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
    buy_levels: int,
    sell_levels: int,
    first_order_multiplier: float,
    per_order_notional: float,
    threshold_position_notional: float,
    threshold_reduce_target_notional: float = 0.0,
    warmup_position_notional: float = 0.0,
    require_non_loss_exit: bool = False,
    max_order_position_notional: float,
    max_position_notional: float,
    tick_size: float | None,
    step_size: float | None,
    min_qty: float | None,
    min_notional: float | None,
    synthetic_neutral: bool = False,
    neutral_base_qty: float = 0.0,
    max_short_position_notional: float = 0.0,
    actual_base_qty: float | None = None,
    symbol: str = "",
    slow_trend_step_enabled: bool = False,
    slow_trend_step_5m_return_ratio: float = 0.0,
    slow_trend_step_15m_return_ratio: float = 0.0,
    slow_trend_step_5m_amplitude_ratio: float = 0.0,
    slow_trend_step_15m_amplitude_ratio: float = 0.0,
    slow_trend_step_scale: float = 1.0,
    synthetic_freeze_enabled: bool = False,
    synthetic_freeze_loss_ratio: float = 0.0,
    synthetic_freeze_min_notional: float = 0.0,
    synthetic_freeze_max_side_notional: float = 0.0,
    synthetic_freeze_release_profit_ratio: float = 0.0,
    synthetic_freeze_pair_release_enabled: bool = True,
    synthetic_freeze_manual_release_side: str | None = None,
    synthetic_freeze_manual_release_qty: float = 0.0,
    synthetic_freeze_manual_release_price: float = 0.0,
    now: datetime | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    synthetic_freeze_requested = bool(synthetic_freeze_enabled)
    synthetic_freeze_disabled_reason = ""
    if synthetic_neutral:
        if synthetic_freeze_requested:
            synthetic_freeze_disabled_reason = "spot_synthetic_neutral_uses_spot_freeze"
        synthetic_freeze_enabled = False
        synthetic_freeze_pair_release_enabled = False
        synthetic_freeze_manual_release_side = None
        synthetic_freeze_manual_release_qty = 0.0
        synthetic_freeze_manual_release_price = 0.0

    known_orders = state.get("known_orders")
    order_refs = known_orders if isinstance(known_orders, dict) else {}
    inventory_lots = state.get("inventory_lots")
    tracked_base_qty = _sum_inventory_qty(inventory_lots if isinstance(inventory_lots, list) else [])
    resolved_actual_base_qty = (
        max(_safe_float(actual_base_qty), 0.0)
        if actual_base_qty is not None
        else tracked_base_qty
    )
    neutral_qty = max(_safe_float(neutral_base_qty), 0.0) if synthetic_neutral else 0.0
    synthetic_net_qty = resolved_actual_base_qty - neutral_qty if synthetic_neutral else resolved_actual_base_qty
    mid_price = (max(float(bid_price), 0.0) + max(float(ask_price), 0.0)) / 2.0
    synthetic_dust_qty = 0.0
    synthetic_dust_notional = 0.0
    if synthetic_neutral and _synthetic_residual_is_dust(
        qty=abs(synthetic_net_qty),
        price=mid_price,
        min_qty=min_qty,
        min_notional=min_notional,
    ):
        synthetic_dust_qty = synthetic_net_qty
        synthetic_dust_notional = abs(synthetic_net_qty) * mid_price
        synthetic_net_qty = 0.0
    raw_synthetic_net_qty = synthetic_net_qty
    current_position_qty = abs(raw_synthetic_net_qty) if synthetic_neutral else resolved_actual_base_qty
    position_qty_tolerance = (
        _synthetic_position_qty_tolerance(price=mid_price, min_qty=min_qty, min_notional=min_notional)
        if synthetic_neutral
        else 0.0
    )
    runtime = _resolve_spot_competition_runtime(
        state=state,
        trades=trades,
        order_refs=order_refs,
        step_price=step_price,
        current_position_qty=current_position_qty,
        synthetic_neutral=synthetic_neutral,
        position_qty_tolerance=position_qty_tolerance,
    )
    if synthetic_neutral:
        runtime["synthetic_neutral"] = True
        runtime["neutral_base_qty"] = neutral_qty
        runtime["frozen_position_lots"] = []

    def _isolate_synthetic_frozen_exposure() -> float:
        if not synthetic_neutral:
            return raw_synthetic_net_qty
        frozen_report = synthetic_frozen_position_report(runtime=runtime, mid_price=mid_price)
        frozen_net_qty = _safe_float(frozen_report.get("long_qty")) - _safe_float(frozen_report.get("short_qty"))
        active_net_qty = raw_synthetic_net_qty - frozen_net_qty
        if _synthetic_residual_is_dust(
            qty=abs(active_net_qty),
            price=mid_price,
            min_qty=min_qty,
            min_notional=min_notional,
        ):
            active_net_qty = 0.0
        expected_direction_state = (
            "long_active" if active_net_qty > EPSILON else "short_active" if active_net_qty < -EPSILON else "flat"
        )
        runtime_direction_state = str(runtime.get("direction_state", "flat") or "flat").strip().lower()
        recovery_mode = str(runtime.get("recovery_mode", "live") or "live").strip().lower()
        active_position_qty = abs(active_net_qty)
        position_lots = [dict(item) for item in list(runtime.get("position_lots") or []) if isinstance(item, dict)]
        active_lot_qty = _sum_inventory_qty(position_lots)
        if active_position_qty <= EPSILON:
            runtime["direction_state"] = "flat"
            runtime["position_lots"] = []
            runtime["recovery_mode"] = "live"
            runtime["risk_state"] = "normal"
            runtime["recovery_errors"] = []
            runtime.pop("synthetic_cost_unknown", None)
            return 0.0
        resume_target_qty = 0.0
        resume_target_notional = max(_safe_float(threshold_reduce_target_notional), 0.0)
        if resume_target_notional > EPSILON and mid_price > EPSILON:
            resume_target_qty = resume_target_notional / mid_price
        near_flat_qty = max(position_qty_tolerance, resume_target_qty)
        stale_active_without_lots = (
            active_lot_qty <= EPSILON
            and (
                runtime_direction_state in {"long_active", "short_active"}
                or recovery_mode != "live"
                or bool(runtime.get("synthetic_cost_unknown"))
            )
        )
        if stale_active_without_lots and active_position_qty <= near_flat_qty:
            runtime["direction_state"] = "flat"
            runtime["position_lots"] = []
            runtime["recovery_mode"] = "live"
            runtime["risk_state"] = "normal"
            runtime["recovery_errors"] = []
            runtime.pop("synthetic_cost_unknown", None)
            return 0.0
        has_synthetic_recovered_lot = any(
            str(item.get("lot_id") or "") == "synthetic_recovered"
            or str(item.get("source_role") or "") == "synthetic_recovery"
            for item in position_lots
        )

        def _unknown_cost_entry_price() -> float:
            candidates = [
                max(_safe_float(item.get("entry_price")), 0.0)
                for item in position_lots
                if isinstance(item, dict) and _safe_float(item.get("entry_price")) > EPSILON
            ]
            anchor_price = max(_safe_float(runtime.get("grid_anchor_price")), 0.0)
            if anchor_price > EPSILON:
                candidates.append(anchor_price)
            candidates.append(mid_price)
            if expected_direction_state == "short_active":
                positives = [price for price in candidates if price > EPSILON]
                return min(positives) if positives else mid_price
            return max(candidates)

        def _prepare_unknown_cost_runtime(*, reason: str) -> None:
            runtime["direction_state"] = expected_direction_state
            runtime["risk_state"] = "normal"
            runtime["recovery_mode"] = "live"
            runtime["recovery_errors"] = []
            runtime["synthetic_cost_unknown"] = True
            runtime["position_lots"] = [
                {
                    "lot_id": "synthetic_recovered",
                    "side": "long" if expected_direction_state == "long_active" else "short",
                    "qty": active_position_qty,
                    "entry_price": _unknown_cost_entry_price(),
                    "opened_at_ms": 0,
                    "source_role": "synthetic_recovery",
                    "recovery_reason": reason,
                }
            ]

        def _prepare_conservative_reduce_runtime(*, reason: str) -> None:
            runtime["direction_state"] = expected_direction_state
            runtime["risk_state"] = "hard_reduce_only"
            runtime["recovery_mode"] = "conservative_reduce_only"
            runtime["recovery_errors"] = [reason]
            runtime["synthetic_cost_unknown"] = True
            runtime["position_lots"] = [
                {
                    "lot_id": "synthetic_recovered",
                    "side": "long" if expected_direction_state == "long_active" else "short",
                    "qty": active_position_qty,
                    "entry_price": _unknown_cost_entry_price(),
                    "opened_at_ms": 0,
                    "source_role": "synthetic_recovery",
                    "recovery_reason": reason,
                }
            ]

        def _handle_unknown_cost_runtime(*, reason: str) -> float:
            threshold_notional = max(_safe_float(threshold_position_notional), 0.0)
            reduce_target_notional = max(_safe_float(threshold_reduce_target_notional), 0.0)
            should_handle = (
                threshold_notional > EPSILON
                and (bool(synthetic_freeze_enabled) or reduce_target_notional > EPSILON)
            )
            if should_handle:
                _prepare_unknown_cost_runtime(reason=reason)
                return active_net_qty
            _prepare_conservative_reduce_runtime(reason=reason)
            return active_net_qty

        if has_synthetic_recovered_lot:
            return _handle_unknown_cost_runtime(reason="synthetic_recovered_cost_unknown")
        if recovery_mode != "live":
            recovery_errors = [str(item) for item in list(runtime.get("recovery_errors") or []) if str(item)]
            return _handle_unknown_cost_runtime(reason=recovery_errors[0] if recovery_errors else "synthetic_cost_unknown")
        if (
            runtime_direction_state != expected_direction_state
            or not _competition_position_qty_matches(
                active_lot_qty,
                active_position_qty,
                extra_tolerance=position_qty_tolerance,
            )
        ):
            runtime["direction_state"] = expected_direction_state
            runtime["recovery_mode"] = "live"
            runtime["position_lots"] = [
                {
                    "lot_id": "synthetic_recovered",
                    "side": "long" if expected_direction_state == "long_active" else "short",
                    "qty": active_position_qty,
                    "entry_price": mid_price,
                    "opened_at_ms": 0,
                    "source_role": "synthetic_recovery",
                }
            ]
            runtime["synthetic_cost_unknown"] = True
        else:
            runtime.pop("synthetic_cost_unknown", None)
        return active_net_qty

    synthetic_net_qty = _isolate_synthetic_frozen_exposure()
    if synthetic_neutral:
        _store_cached_spot_competition_runtime(
            state=state,
            runtime=runtime,
            applied_trade_keys=_cached_spot_competition_applied_trade_keys(
                state=state,
                synthetic_neutral=True,
            ),
            synthetic_neutral=True,
        )
    if _safe_float(runtime.get("grid_anchor_price")) <= 0:
        runtime["grid_anchor_price"] = (max(float(bid_price), 0.0) + max(float(ask_price), 0.0)) / 2.0
    synthetic_pair_release_last: dict[str, Any] = {}
    if synthetic_neutral and synthetic_freeze_enabled and synthetic_freeze_pair_release_enabled:
        synthetic_pair_release_last = apply_synthetic_frozen_pair_release(
            runtime=runtime,
            release_qty=None,
            release_time_ms=int((now or _utc_now()).timestamp() * 1000),
            reason="pair_release",
        )
        if bool(synthetic_pair_release_last.get("applied")):
            _store_cached_spot_competition_runtime(
                state=state,
                runtime=runtime,
                applied_trade_keys=_cached_spot_competition_applied_trade_keys(
                    state=state,
                    synthetic_neutral=True,
                ),
                synthetic_neutral=True,
            )
        synthetic_net_qty = _isolate_synthetic_frozen_exposure()
        _store_cached_spot_competition_runtime(
            state=state,
            runtime=runtime,
            applied_trade_keys=_cached_spot_competition_applied_trade_keys(
                state=state,
                synthetic_neutral=True,
            ),
            synthetic_neutral=True,
        )
    slow_trend_step = _resolve_spot_slow_trend_step(
        symbol=str(symbol or state.get("symbol") or "").upper().strip(),
        base_step_price=step_price,
        enabled=bool(slow_trend_step_enabled),
        trigger_5m_return_ratio=slow_trend_step_5m_return_ratio,
        trigger_15m_return_ratio=slow_trend_step_15m_return_ratio,
        trigger_5m_amplitude_ratio=slow_trend_step_5m_amplitude_ratio,
        trigger_15m_amplitude_ratio=slow_trend_step_15m_amplitude_ratio,
        scale=slow_trend_step_scale,
        tick_size=tick_size,
        now=now,
    )
    effective_step_price = max(_safe_float(slow_trend_step.get("effective_step_price")), _safe_float(step_price))

    plan_kwargs = {
        "runtime": runtime,
        "bid_price": bid_price,
        "ask_price": ask_price,
        "step_price": effective_step_price,
        "per_order_notional": per_order_notional,
        "first_order_multiplier": first_order_multiplier,
        "threshold_position_notional": threshold_position_notional,
        "threshold_reduce_target_notional": threshold_reduce_target_notional,
        "warmup_position_notional": warmup_position_notional,
        "require_non_loss_exit": require_non_loss_exit,
        "max_order_position_notional": max_order_position_notional,
        "max_position_notional": max_position_notional,
        "max_short_order_position_notional": max_short_position_notional,
        "max_short_position_notional": max_short_position_notional,
        "buy_levels": buy_levels,
        "sell_levels": sell_levels,
        "tick_size": tick_size,
        "step_size": step_size,
        "min_qty": min_qty,
        "min_notional": min_notional,
        "synthetic_freeze_enabled": bool(synthetic_neutral and synthetic_freeze_enabled),
        "synthetic_freeze_loss_ratio": synthetic_freeze_loss_ratio,
        "synthetic_freeze_min_notional": synthetic_freeze_min_notional,
        "synthetic_freeze_max_side_notional": synthetic_freeze_max_side_notional,
        "synthetic_freeze_release_profit_ratio": synthetic_freeze_release_profit_ratio,
        "synthetic_freeze_manual_release_side": synthetic_freeze_manual_release_side,
        "synthetic_freeze_manual_release_qty": synthetic_freeze_manual_release_qty,
        "synthetic_freeze_manual_release_price": synthetic_freeze_manual_release_price,
    }
    plan = build_inventory_grid_orders(**plan_kwargs)
    if synthetic_neutral and bool(runtime.get("synthetic_cost_unknown")) and str(runtime.get("recovery_mode", "live") or "live") != "live":
        plan = dict(plan)
        plan["bootstrap_orders"] = []
        plan["buy_orders"] = [order for order in list(plan.get("buy_orders") or []) if order.get("role") == "forced_reduce"]
        plan["sell_orders"] = [order for order in list(plan.get("sell_orders") or []) if order.get("role") == "forced_reduce"]
        plan["forced_reduce_orders"] = list(plan["buy_orders"]) + list(plan["sell_orders"])
    if synthetic_neutral and not (
        list(plan.get("bootstrap_orders") or [])
        or list(plan.get("buy_orders") or [])
        or list(plan.get("sell_orders") or [])
    ):
        stale_recovery_mode = str(runtime.get("recovery_mode", "live") or "live").strip().lower() != "live"
        stale_hard_reduce = str(plan.get("risk_state", "") or "").strip().lower() == "hard_reduce_only"
        reduce_target_notional = max(_safe_float(threshold_reduce_target_notional), 0.0)
        resume_notional = max(_safe_float(threshold_position_notional), reduce_target_notional, 0.0)
        active_notional = abs(_safe_float(synthetic_net_qty)) * mid_price
        if reduce_target_notional > EPSILON and (
            stale_recovery_mode or stale_hard_reduce or bool(runtime.get("synthetic_cost_unknown"))
        ) and (
            resume_notional <= EPSILON or active_notional <= resume_notional + EPSILON
        ):
            runtime["direction_state"] = "flat"
            runtime["position_lots"] = []
            runtime["recovery_mode"] = "live"
            runtime["risk_state"] = "normal"
            runtime["recovery_errors"] = []
            runtime.pop("synthetic_cost_unknown", None)
            synthetic_net_qty = 0.0
            _store_cached_spot_competition_runtime(
                state=state,
                runtime=runtime,
                applied_trade_keys=_cached_spot_competition_applied_trade_keys(
                    state=state,
                    synthetic_neutral=True,
                ),
                synthetic_neutral=True,
            )
            plan = build_inventory_grid_orders(**plan_kwargs)
    synthetic_freeze_last: dict[str, Any] = {}
    freeze_candidate = plan.get("synthetic_freeze_candidate") if isinstance(plan.get("synthetic_freeze_candidate"), dict) else {}
    if synthetic_neutral and bool(freeze_candidate.get("should_freeze")):
        freeze_time = int((now or _utc_now()).timestamp() * 1000)
        synthetic_freeze_last = freeze_inventory_grid_reduce_lots(
            runtime=runtime,
            reduce_price=_safe_float(freeze_candidate.get("reduce_price")),
            reduce_qty=_safe_float(freeze_candidate.get("qty")),
            step_price=effective_step_price,
            freeze_time_ms=freeze_time,
            reason=str(freeze_candidate.get("reason") or "threshold_reduce_loss"),
        )
        if synthetic_freeze_pair_release_enabled:
            synthetic_pair_release_last = apply_synthetic_frozen_pair_release(
                runtime=runtime,
                release_qty=None,
                release_time_ms=freeze_time,
                reason="pair_release",
            )
        _store_cached_spot_competition_runtime(
            state=state,
            runtime=runtime,
            applied_trade_keys=_cached_spot_competition_applied_trade_keys(
                state=state,
                synthetic_neutral=True,
            ),
            synthetic_neutral=True,
        )
        synthetic_net_qty = _isolate_synthetic_frozen_exposure()
        plan = build_inventory_grid_orders(**plan_kwargs)
    desired_orders = list(plan.get("bootstrap_orders") or []) + list(plan.get("buy_orders") or []) + list(plan.get("sell_orders") or [])
    reduce_target_notional = max(_safe_float(threshold_reduce_target_notional), 0.0)
    display_soft_limit = reduce_target_notional if reduce_target_notional > 0 else _safe_float(threshold_position_notional)
    current_long_notional = max(synthetic_net_qty, 0.0) * mid_price
    current_short_notional = max(-synthetic_net_qty, 0.0) * mid_price
    synthetic_frozen_position = plan.get("synthetic_frozen_position") if isinstance(plan.get("synthetic_frozen_position"), dict) else {}
    controls = {
        "mode": "competition_synthetic_neutral_grid" if synthetic_neutral else "competition_inventory_grid",
        "buy_paused": False,
        "pause_reasons": [],
        "shift_frozen": False,
        "market_guard_return_ratio": 0.0,
        "market_guard_amplitude_ratio": 0.0,
        "center_price": _safe_float(runtime.get("grid_anchor_price")),
        "center_shift_count": 0,
        "inventory_soft_limit_notional": display_soft_limit,
        "inventory_hard_limit_notional": _safe_float(max_position_notional),
        "effective_buy_levels": sum(1 for order in desired_orders if str(order.get("side", "")).upper() == "BUY"),
        "effective_sell_levels": sum(1 for order in desired_orders if str(order.get("side", "")).upper() == "SELL"),
        "effective_per_order_notional": _safe_float(per_order_notional),
        "base_step_price": _safe_float(step_price),
        "effective_step_price": effective_step_price,
        "slow_trend_step": slow_trend_step,
        "direction_state": str(runtime.get("direction_state", "flat") or "flat"),
        "risk_state": str(plan.get("risk_state", "normal") or "normal"),
        "recovery_mode": str(runtime.get("recovery_mode", "live") or "live"),
        "recovery_errors": list(runtime.get("recovery_errors") or []),
        "synthetic_cost_unknown": bool(runtime.get("synthetic_cost_unknown")),
        "_runtime": copy.deepcopy(runtime),
        "grid_anchor_price": _safe_float(runtime.get("grid_anchor_price")),
        "pair_credit_steps": _safe_int(runtime.get("pair_credit_steps")),
        "threshold_position_notional": _safe_float(threshold_position_notional),
        "threshold_reduce_target_notional": reduce_target_notional,
        "warmup_position_notional": max(_safe_float(warmup_position_notional), 0.0),
        "require_non_loss_exit": bool(require_non_loss_exit),
        "neutral_base_qty": neutral_qty,
        "actual_base_qty": resolved_actual_base_qty,
        "tracked_base_qty": tracked_base_qty,
        "synthetic_net_qty": synthetic_net_qty,
        "synthetic_dust_qty": synthetic_dust_qty,
        "synthetic_dust_notional": synthetic_dust_notional,
        "current_long_notional": current_long_notional,
        "current_short_notional": current_short_notional,
        "synthetic_freeze_enabled": bool(synthetic_neutral and synthetic_freeze_enabled),
        "synthetic_freeze_requested": synthetic_freeze_requested,
        "synthetic_freeze_disabled_reason": synthetic_freeze_disabled_reason,
        "synthetic_freeze_max_side_notional": max(_safe_float(synthetic_freeze_max_side_notional), 0.0),
        "synthetic_freeze_pair_release_enabled": bool(synthetic_neutral and synthetic_freeze_enabled and synthetic_freeze_pair_release_enabled),
        "synthetic_freeze_candidate": plan.get("synthetic_freeze_candidate", {}),
        "synthetic_freeze_last": synthetic_freeze_last,
        "synthetic_pair_release_last": synthetic_pair_release_last,
        "synthetic_frozen_position": synthetic_frozen_position,
        "synthetic_frozen_long_qty": _safe_float(synthetic_frozen_position.get("long_qty")),
        "synthetic_frozen_short_qty": _safe_float(synthetic_frozen_position.get("short_qty")),
        "synthetic_frozen_long_notional": _safe_float(synthetic_frozen_position.get("long_notional")),
        "synthetic_frozen_short_notional": _safe_float(synthetic_frozen_position.get("short_notional")),
        "synthetic_frozen_short_quote_reserve": _safe_float(synthetic_frozen_position.get("short_notional")),
        "max_short_position_notional": max(_safe_float(max_short_position_notional), 0.0),
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


def _total_base_balance(account_info: dict[str, Any], base_asset: str) -> float:
    base_free, base_locked = _extract_balance(account_info, base_asset)
    return max(base_free, 0.0) + max(base_locked, 0.0)


def _spot_freeze_position_side_qty(position_risk: list[dict[str, Any]], symbol: str, position_side: str) -> float:
    wanted_symbol = str(symbol or "").upper().strip()
    wanted_side = str(position_side or "").upper().strip()
    for row in position_risk:
        if str(row.get("symbol", "") or "").upper().strip() != wanted_symbol:
            continue
        if str(row.get("positionSide", row.get("position_side", "")) or "").upper().strip() != wanted_side:
            continue
        return abs(_safe_float(row.get("positionAmt", row.get("position_amt"))))
    return 0.0


def _check_spot_freeze_hedge_gate(
    *,
    symbol: str,
    position_mode: dict[str, Any],
    position_risk: list[dict[str, Any]],
    base_hedge_qty: float,
    tolerance_qty: float,
    expected_short_qty: float | None = None,
) -> dict[str, Any]:
    if not _truthy(position_mode.get("dualSidePosition")):
        return {"ok": False, "reason": "not_hedge_mode", "contract_short_qty": 0.0, "contract_long_qty": 0.0}
    tolerance = max(_safe_float(tolerance_qty), 0.0)
    long_qty = _spot_freeze_position_side_qty(position_risk, symbol, "LONG")
    short_qty = _spot_freeze_position_side_qty(position_risk, symbol, "SHORT")
    if long_qty > tolerance + EPSILON:
        return {"ok": False, "reason": "long_position_not_zero", "contract_short_qty": short_qty, "contract_long_qty": long_qty}
    base_qty = max(_safe_float(base_hedge_qty), 0.0)
    expected_short = base_qty if expected_short_qty is None else max(_safe_float(expected_short_qty), 0.0)
    if abs(short_qty - expected_short) > tolerance + EPSILON:
        return {
            "ok": False,
            "reason": "short_position_base_mismatch",
            "contract_short_qty": short_qty,
            "contract_long_qty": long_qty,
            "expected_short_qty": expected_short,
        }
    return {"ok": True, "reason": "ok", "contract_short_qty": short_qty, "contract_long_qty": long_qty, "expected_short_qty": expected_short}


def _make_spot_freeze_spot_market_callback(*, symbol: str, api_key: str, api_secret: str):
    def _place_spot(*, side: str, qty: float, **_kwargs: Any) -> dict[str, Any]:
        return post_spot_order(
            symbol=symbol,
            side=side,
            quantity=qty,
            price=0.0,
            api_key=api_key,
            api_secret=api_secret,
            order_type="MARKET",
        )

    return _place_spot


def _make_spot_freeze_contract_callback(*, symbol: str, api_key: str, api_secret: str):
    def _place_contract(*, side: str, qty: float, position_side: str = "SHORT", **_kwargs: Any) -> dict[str, Any]:
        return post_futures_market_order(
            symbol=symbol,
            side=side,
            quantity=qty,
            api_key=api_key,
            api_secret=api_secret,
            position_side=position_side,
        )

    return _place_contract


def _spot_freeze_default_controls(enabled: bool) -> dict[str, Any]:
    return {
        "spot_freeze_enabled": bool(enabled),
        "spot_freeze_effective_enabled": bool(enabled),
        "spot_freeze_config_reason": "enabled" if enabled else "disabled",
        "spot_freeze_dry_run": False,
        "spot_freeze_market_execution_enabled": False,
        "spot_freeze_maker_execution_enabled": False,
        "spot_freeze_runtime_blocked": False,
        "spot_freeze_reconcile_ok": None,
        "spot_freeze_alerts": [],
        "spot_freeze_actions": [],
        "spot_freeze_maker_orders": [],
        "spot_freeze_existing_maker_orders": [],
        "spot_freeze_suppressed_app_loss_reduce_orders": 0,
        "spot_freeze_frozen_long_qty": 0.0,
        "spot_freeze_frozen_short_qty": 0.0,
        "spot_freeze_pending_contract_actions": [],
        "spot_freeze_loss_ratio_usable": False,
        "spot_freeze_deviation_qty": 0.0,
        "spot_freeze_deviation_side": "",
        "spot_freeze_deviation_notional": 0.0,
        "spot_freeze_deviation_threshold_notional": 0.0,
        "spot_freeze_deviation_loss_ratio": 0.0,
        "spot_freeze_deviation_cost_avg": 0.0,
        "spot_freeze_deviation_loss_reason": "",
        "spot_freeze_skip_reason": "disabled" if not enabled else "",
    }


def _spot_freeze_common_qty_step(symbol: str, symbol_info: dict[str, Any]) -> float:
    spot_step = max(_safe_float(symbol_info.get("step_size")), 0.0)
    futures_info = fetch_futures_symbol_config(symbol)
    futures_step = max(_safe_float(futures_info.get("step_size")), 0.0) if isinstance(futures_info, dict) else 0.0
    return max(spot_step, futures_step)


def _spot_freeze_threshold_effectively_disables(args: argparse.Namespace, deviation_threshold: float) -> bool:
    threshold = max(_safe_float(deviation_threshold), 0.0)
    if threshold <= EPSILON:
        return False
    capacity = max(
        max(_safe_float(getattr(args, "spot_freeze_total_cap_notional", 0.0)), 0.0),
        max(_safe_float(getattr(args, "spot_freeze_max_per_cycle_notional", 0.0)), 0.0),
        max(_safe_float(getattr(args, "max_position_notional", 0.0)), 0.0),
        max(_safe_float(getattr(args, "max_short_position_notional", 0.0)), 0.0),
        max(_safe_float(getattr(args, "threshold_position_notional", 0.0)), 0.0),
        max(_safe_float(getattr(args, "max_order_position_notional", 0.0)), 0.0),
        max(_safe_float(getattr(args, "per_order_notional", 0.0)), 0.0),
    )
    return capacity > EPSILON and threshold >= capacity * 1000.0


def _spot_freeze_app_loss_deviation_loss(
    *,
    controls: dict[str, Any],
    mid_price: float,
    deviation_side: str,
    deviation_qty: float,
) -> DeviationLossResult | None:
    if str(deviation_side or "").strip().lower() != "long":
        return None
    guard = controls.get("spot_app_loss_guard")
    if not isinstance(guard, dict):
        return None
    if str(guard.get("window_source", "") or "").strip() != "metrics":
        return None
    if not bool(guard.get("window_aligned")):
        return None
    buy_qty = max(_safe_float(guard.get("buy_qty")), 0.0)
    sell_qty = max(_safe_float(guard.get("sell_qty")), 0.0)
    app_position_qty = max(_safe_float(guard.get("position_qty")), 0.0)
    required_qty = max(_safe_float(deviation_qty), 0.0)
    if buy_qty <= sell_qty + EPSILON or app_position_qty + EPSILON < required_qty or required_qty <= EPSILON:
        return None
    cost_basis = max(_safe_float(guard.get("buy_notional")) - _safe_float(guard.get("sell_notional")), 0.0)
    if cost_basis <= EPSILON:
        return None
    cost_avg = cost_basis / app_position_qty
    if cost_avg <= EPSILON:
        return None
    loss_ratio = max(cost_avg - max(_safe_float(mid_price), 0.0), 0.0) / cost_avg
    return DeviationLossResult(usable=True, loss_ratio=loss_ratio, cost_avg=cost_avg, reason="app_loss_guard")


def _build_spot_freeze_maker_order(
    *,
    args: argparse.Namespace,
    ledger: dict[str, Any],
    base_hedge_qty: float,
    deviation_side: str,
    deviation_qty: float,
    mid_price: float,
    bid_price: float,
    ask_price: float,
    qty_step: float,
    tick_size: float | None,
    min_qty: float | None,
    min_notional: float | None,
) -> tuple[dict[str, Any] | None, str]:
    side_name = str(deviation_side or "").strip().lower()
    mid = max(_safe_float(mid_price), 0.0)
    if side_name not in {"long", "short"} or mid <= EPSILON:
        return None, "invalid_maker_candidate"
    long_qty, short_qty = ledger_totals(ledger)
    cap = max(_safe_float(getattr(args, "spot_freeze_total_cap_notional", 0.0)), 0.0)
    frozen_notional = (long_qty + short_qty) * mid
    if cap > EPSILON and frozen_notional + EPSILON >= cap:
        return None, "total_cap_reached"

    max_qty = max(_safe_float(deviation_qty), 0.0)
    available_short_qty = expected_short_position_now(base_hedge_qty, ledger)
    if side_name == "long":
        max_qty = min(max_qty, max(_safe_float(available_short_qty), 0.0))
    else:
        max_contract_short_notional = max(_safe_float(getattr(args, "max_short_position_notional", 0.0)), 0.0)
        if max_contract_short_notional > EPSILON:
            max_contract_short_qty = max_contract_short_notional / mid
            short_capacity_qty = max(max_contract_short_qty - max(_safe_float(short_qty), 0.0), 0.0)
            if short_capacity_qty <= EPSILON:
                return None, "short_hedge_capacity_exhausted"
            max_qty = min(max_qty, short_capacity_qty)
    per_cycle_cap = max(_safe_float(getattr(args, "spot_freeze_max_per_cycle_notional", 0.0)), 0.0)
    if per_cycle_cap > EPSILON:
        max_qty = min(max_qty, per_cycle_cap / mid)
    if cap > EPSILON:
        max_qty = min(max_qty, max((cap - frozen_notional) / mid, 0.0))
    qty = _round_order_qty(max_qty, qty_step)
    side = "SELL" if side_name == "long" else "BUY"
    reference_price = ask_price if side == "SELL" else bid_price
    price = _round_order_price(max(_safe_float(reference_price), 0.0), tick_size, side)
    if not _spot_order_meets_exchange_mins(qty=qty, price=price, min_qty=min_qty, min_notional=min_notional):
        return None, "qty_too_small"
    return {
        "side": side,
        "price": price,
        "qty": qty,
        "role": SPOT_FREEZE_MAKER_ROLE,
        "tag": "spot_freeze_long" if side_name == "long" else "spot_freeze_short",
    }, ""


def _maybe_run_spot_freeze(
    *,
    args: argparse.Namespace,
    state: dict[str, Any],
    controls: dict[str, Any],
    symbol: str,
    mid_price: float,
    bid_price: float = 0.0,
    ask_price: float = 0.0,
    symbol_info: dict[str, Any],
    api_key: str,
    api_secret: str,
    now: datetime,
    persist_ledger: Any | None = None,
    existing_maker_orders: list[dict[str, Any]] | None = None,
) -> None:
    enabled = bool(getattr(args, "spot_freeze_enabled", False))
    controls.update(_spot_freeze_default_controls(enabled))
    controls["spot_freeze_dry_run"] = bool(getattr(args, "spot_freeze_dry_run", False))
    market_execution_enabled = bool(getattr(args, "spot_freeze_market_execution_enabled", False))
    controls["spot_freeze_market_execution_enabled"] = market_execution_enabled
    maker_execution_enabled = bool(getattr(args, "spot_freeze_maker_execution_enabled", False))
    controls["spot_freeze_maker_execution_enabled"] = maker_execution_enabled
    neutral_base_qty = _safe_float(controls.get("neutral_base_qty", getattr(args, "neutral_base_qty", 0.0)))
    spot_inventory_qty = _safe_float(controls.get("actual_base_qty", neutral_base_qty))
    deviation_qty = abs(spot_inventory_qty - neutral_base_qty)
    deviation_side = "long" if spot_inventory_qty >= neutral_base_qty else "short"
    deviation_notional = deviation_qty * max(_safe_float(mid_price), 0.0)
    deviation_threshold = max(_safe_float(getattr(args, "spot_freeze_deviation_notional", 0.0)), 0.0)
    controls["spot_freeze_deviation_qty"] = deviation_qty
    controls["spot_freeze_deviation_side"] = deviation_side
    controls["spot_freeze_deviation_notional"] = deviation_notional
    controls["spot_freeze_deviation_threshold_notional"] = deviation_threshold
    threshold_disables = enabled and _spot_freeze_threshold_effectively_disables(args, deviation_threshold)
    controls["spot_freeze_effective_enabled"] = bool(enabled and not threshold_disables)
    controls["spot_freeze_config_reason"] = (
        "deviation_threshold_disables" if threshold_disables else "enabled" if enabled else "disabled"
    )
    if not enabled:
        return

    base_hedge_qty = max(_safe_float(getattr(args, "spot_freeze_base_hedge_qty", 0.0)), 0.0)
    tolerance_qty = max(_safe_float(getattr(args, "spot_freeze_tolerance_qty", 0.0)), 0.0)
    ledger = state.get("spot_frozen_ledger") if isinstance(state.get("spot_frozen_ledger"), dict) else new_ledger()
    expected_short_qty = expected_short_position_now(base_hedge_qty, ledger)
    try:
        position_mode = fetch_futures_position_mode(api_key, api_secret)
        position_risk = fetch_futures_position_risk_v3(api_key, api_secret, symbol=symbol)
    except Exception as exc:
        controls["spot_freeze_gate"] = {
            "ok": False,
            "reason": "futures_position_fetch_failed",
            "error": f"{type(exc).__name__}: {exc}",
        }
        controls["spot_freeze_alerts"] = ["futures_position_fetch_failed"]
        controls["spot_freeze_skip_reason"] = "hedge_gate_failed"
        return
    gate = _check_spot_freeze_hedge_gate(
        symbol=symbol,
        position_mode=position_mode if isinstance(position_mode, dict) else {},
        position_risk=position_risk if isinstance(position_risk, list) else [],
        base_hedge_qty=base_hedge_qty,
        tolerance_qty=tolerance_qty,
        expected_short_qty=expected_short_qty,
    )
    controls["spot_freeze_gate"] = gate
    if not bool(gate.get("ok")):
        controls["spot_freeze_alerts"] = [str(gate.get("reason") or "gate_failed")]
        controls["spot_freeze_skip_reason"] = "hedge_gate_failed"
        return
    try:
        qty_step = _spot_freeze_common_qty_step(symbol, symbol_info)
    except Exception:
        controls["spot_freeze_alerts"] = ["futures_symbol_config_failed"]
        controls["spot_freeze_skip_reason"] = "futures_symbol_config_failed"
        return
    if ledger.get("pending_contract_actions"):
        pending_dry_run = bool(getattr(args, "spot_freeze_dry_run", False)) or not bool(getattr(args, "apply", False))
        result = freeze_cycle(
            symbol=symbol,
            neutral_base_qty=neutral_base_qty,
            spot_inventory_qty=spot_inventory_qty,
            mid_price=mid_price,
            mark_price=mid_price,
            ledger=ledger,
            contract_short_qty=_safe_float(gate.get("contract_short_qty")),
            base_hedge_qty=base_hedge_qty,
            deviation_loss_ratio=0.0,
            loss_ratio_usable=False,
            config=FreezeConfig(enabled=False),
            qty_step=qty_step,
            tolerance_qty=tolerance_qty,
            now=now.isoformat(),
            place_spot=_make_spot_freeze_spot_market_callback(symbol=symbol, api_key=api_key, api_secret=api_secret),
            place_contract=_make_spot_freeze_contract_callback(symbol=symbol, api_key=api_key, api_secret=api_secret),
            dry_run=pending_dry_run,
            on_ledger_change=persist_ledger,
        )
        ledger_result = result.get("ledger") if isinstance(result, dict) else new_ledger()
        state["spot_frozen_ledger"] = ledger_result
        controls["spot_freeze_reconcile_ok"] = bool(result.get("reconcile_ok")) if isinstance(result, dict) else False
        controls["spot_freeze_alerts"] = list(result.get("alerts") or []) if isinstance(result, dict) else ["invalid_freeze_result"]
        controls["spot_freeze_actions"] = list(result.get("actions") or []) if isinstance(result, dict) else []
        controls["spot_freeze_frozen_long_qty"] = _safe_float(ledger_result.get("frozen_long_qty")) if isinstance(ledger_result, dict) else 0.0
        controls["spot_freeze_frozen_short_qty"] = _safe_float(ledger_result.get("frozen_short_qty")) if isinstance(ledger_result, dict) else 0.0
        controls["spot_freeze_pending_contract_actions"] = list(ledger_result.get("pending_contract_actions") or []) if isinstance(ledger_result, dict) else []
        controls["spot_freeze_skip_reason"] = "pending_contract_repair"
        return

    runtime = controls.get("_runtime") if isinstance(controls.get("_runtime"), dict) else {}
    loss = compute_deviation_loss(runtime, neutral_base_qty, mid_price, deviation_side, deviation_qty)
    loss_source = "runtime"
    if not loss.usable:
        fallback_loss = _spot_freeze_app_loss_deviation_loss(
            controls=controls,
            mid_price=mid_price,
            deviation_side=deviation_side,
            deviation_qty=deviation_qty,
        )
        if fallback_loss is not None:
            loss = fallback_loss
            loss_source = "app_loss_guard"
    controls["spot_freeze_loss_ratio_usable"] = bool(loss.usable)
    controls["spot_freeze_deviation_loss_ratio"] = _safe_float(loss.loss_ratio)
    controls["spot_freeze_deviation_cost_avg"] = _safe_float(loss.cost_avg)
    controls["spot_freeze_deviation_loss_reason"] = str(loss.reason or "")
    controls["spot_freeze_deviation_loss_source"] = loss_source
    min_loss_ratio = max(_safe_float(getattr(args, "spot_freeze_min_loss_ratio", 0.0)), 0.0)
    if deviation_notional <= deviation_threshold + EPSILON:
        controls["spot_freeze_skip_reason"] = "below_deviation_threshold"
    elif not bool(loss.usable):
        controls["spot_freeze_skip_reason"] = "cost_not_usable"
    elif _safe_float(loss.loss_ratio) + EPSILON < min_loss_ratio:
        controls["spot_freeze_skip_reason"] = "loss_ratio_below_min"
    else:
        controls["spot_freeze_skip_reason"] = "eligible"
    dry_run = bool(getattr(args, "spot_freeze_dry_run", False)) or not bool(getattr(args, "apply", False))
    if not dry_run and not market_execution_enabled:
        if maker_execution_enabled and controls["spot_freeze_skip_reason"] == "eligible":
            open_maker_orders = [dict(order) for order in list(existing_maker_orders or []) if isinstance(order, dict)]
            if open_maker_orders:
                controls["spot_freeze_existing_maker_orders"] = open_maker_orders
                controls["spot_freeze_maker_orders"] = []
                controls["spot_freeze_actions"] = []
                controls["spot_freeze_skip_reason"] = "maker_order_open"
                return
            maker_order, maker_reason = _build_spot_freeze_maker_order(
                args=args,
                ledger=ledger,
                base_hedge_qty=base_hedge_qty,
                deviation_side=deviation_side,
                deviation_qty=deviation_qty,
                mid_price=mid_price,
                bid_price=bid_price,
                ask_price=ask_price,
                qty_step=qty_step,
                tick_size=symbol_info.get("tick_size"),
                min_qty=symbol_info.get("min_qty"),
                min_notional=symbol_info.get("min_notional"),
            )
            if maker_order is not None:
                controls["spot_freeze_maker_orders"] = [maker_order]
                controls["spot_freeze_actions"] = [
                    {"type": "freeze_maker_order", "side": deviation_side, "qty": _safe_float(maker_order.get("qty"))}
                ]
                controls["spot_freeze_skip_reason"] = "maker_order_pending"
            else:
                controls["spot_freeze_alerts"] = [maker_reason or "maker_order_unavailable"]
                controls["spot_freeze_actions"] = []
                controls["spot_freeze_skip_reason"] = maker_reason or "maker_order_unavailable"
            return
        controls["spot_freeze_alerts"] = ["market_execution_disabled"]
        controls["spot_freeze_actions"] = []
        if controls["spot_freeze_skip_reason"] == "eligible":
            controls["spot_freeze_skip_reason"] = "market_execution_disabled"
        return
    result = freeze_cycle(
        symbol=symbol,
        neutral_base_qty=neutral_base_qty,
        spot_inventory_qty=spot_inventory_qty,
        mid_price=mid_price,
        mark_price=mid_price,
        ledger=ledger,
        contract_short_qty=_safe_float(gate.get("contract_short_qty")),
        base_hedge_qty=base_hedge_qty,
        deviation_loss_ratio=loss.loss_ratio,
        loss_ratio_usable=loss.usable,
        config=FreezeConfig(
            enabled=True,
            deviation_notional=deviation_threshold,
            min_loss_ratio=min_loss_ratio,
            max_per_cycle_notional=max(_safe_float(getattr(args, "spot_freeze_max_per_cycle_notional", 0.0)), 0.0),
            total_cap_notional=max(_safe_float(getattr(args, "spot_freeze_total_cap_notional", 0.0)), 0.0),
            max_contract_short_notional=max(_safe_float(getattr(args, "max_short_position_notional", 0.0)), 0.0),
            pair_release_enabled=bool(getattr(args, "spot_freeze_pair_release_enabled", False)),
            profit_release_enabled=bool(getattr(args, "spot_freeze_profit_release_enabled", False)),
            release_profit_ratio=max(_safe_float(getattr(args, "spot_freeze_release_profit_ratio", 0.05)), 0.0),
        ),
        qty_step=qty_step,
        tolerance_qty=tolerance_qty,
        now=now.isoformat(),
        place_spot=_make_spot_freeze_spot_market_callback(symbol=symbol, api_key=api_key, api_secret=api_secret),
        place_contract=_make_spot_freeze_contract_callback(symbol=symbol, api_key=api_key, api_secret=api_secret),
        dry_run=dry_run,
        on_ledger_change=persist_ledger,
    )
    ledger_result = result.get("ledger") if isinstance(result, dict) else new_ledger()
    state["spot_frozen_ledger"] = ledger_result
    controls["spot_freeze_reconcile_ok"] = bool(result.get("reconcile_ok")) if isinstance(result, dict) else False
    controls["spot_freeze_alerts"] = list(result.get("alerts") or []) if isinstance(result, dict) else ["invalid_freeze_result"]
    controls["spot_freeze_actions"] = list(result.get("actions") or []) if isinstance(result, dict) else []
    controls["spot_freeze_frozen_long_qty"] = _safe_float(ledger_result.get("frozen_long_qty")) if isinstance(ledger_result, dict) else 0.0
    controls["spot_freeze_frozen_short_qty"] = _safe_float(ledger_result.get("frozen_short_qty")) if isinstance(ledger_result, dict) else 0.0
    controls["spot_freeze_pending_contract_actions"] = list(ledger_result.get("pending_contract_actions") or []) if isinstance(ledger_result, dict) else []
    if controls["spot_freeze_actions"]:
        controls["spot_freeze_skip_reason"] = ""
    elif controls["spot_freeze_alerts"] and controls["spot_freeze_skip_reason"] == "eligible":
        controls["spot_freeze_skip_reason"] = str(controls["spot_freeze_alerts"][0])


def _auto_flatten_on_runtime_guard(strategy_mode: str) -> bool:
    return str(strategy_mode or "").strip() not in SPOT_COMPETITION_MODES


def _spot_order_meets_exchange_mins(
    *,
    qty: float,
    price: float,
    min_qty: float | None,
    min_notional: float | None,
) -> bool:
    if qty <= EPSILON or price <= EPSILON:
        return False
    if min_qty is not None and qty + EPSILON < float(min_qty):
        return False
    if min_notional is not None and qty * price + EPSILON < float(min_notional):
        return False
    return True


def _clamp_limit_maker_price_to_book(
    *,
    side: str,
    price: float,
    bid_price: float,
    ask_price: float,
    tick_size: float | None,
) -> float:
    normalized_side = str(side or "").upper().strip()
    safe_price = max(_safe_float(price), 0.0)
    safe_bid = max(_safe_float(bid_price), 0.0)
    safe_ask = max(_safe_float(ask_price), 0.0)
    if normalized_side == "BUY" and safe_ask > EPSILON and safe_price + EPSILON >= safe_ask:
        return _round_order_price(safe_bid, tick_size, "BUY")
    if normalized_side == "SELL" and safe_bid > EPSILON and safe_price <= safe_bid + EPSILON:
        return _round_order_price(safe_ask, tick_size, "SELL")
    return _round_order_price(safe_price, tick_size, normalized_side)


def _synthetic_residual_is_dust(
    *,
    qty: float,
    price: float,
    min_qty: float | None,
    min_notional: float | None,
) -> bool:
    residual_qty = max(_safe_float(qty), 0.0)
    if residual_qty <= EPSILON:
        return False
    if min_qty is not None and residual_qty + EPSILON < float(min_qty):
        return True
    if min_notional is not None and residual_qty * max(_safe_float(price), 0.0) + EPSILON < float(min_notional):
        return True
    return False


def _round_up_to_step(value: float, step: float | None) -> float:
    if step is None or step <= 0:
        return float(value)
    units = max(int(math.ceil(max(float(value), 0.0) / float(step))), 0)
    return units * float(step)


def _empty_spot_slow_trend_step(
    *,
    enabled: bool,
    base_step_price: float,
    warning: str | None = None,
) -> dict[str, Any]:
    safe_base_step = max(_safe_float(base_step_price), 0.0)
    return {
        "enabled": bool(enabled),
        "active": False,
        "warning": warning,
        "base_step_price": safe_base_step,
        "effective_step_price": safe_base_step,
        "scale": 1.0,
        "configured_scale": 1.0,
        "reasons": [],
        "window_5m": {
            "available": False,
            "return_ratio": 0.0,
            "amplitude_ratio": 0.0,
        },
        "window_15m": {
            "available": False,
            "return_ratio": 0.0,
            "amplitude_ratio": 0.0,
        },
    }


def _spot_slow_trend_window_stats(candles: list[Any], minutes: int) -> dict[str, Any]:
    safe_minutes = max(_safe_int(minutes), 1)
    window = list(candles[-safe_minutes:])
    if len(window) < safe_minutes:
        return {
            "available": False,
            "minutes": safe_minutes,
            "return_ratio": 0.0,
            "amplitude_ratio": 0.0,
        }
    open_price = max(_safe_float(getattr(window[0], "open", 0.0)), 0.0)
    close_price = max(_safe_float(getattr(window[-1], "close", 0.0)), 0.0)
    high_price = max((_safe_float(getattr(item, "high", 0.0)) for item in window), default=0.0)
    low_price = min((_safe_float(getattr(item, "low", 0.0)) for item in window), default=0.0)
    return_ratio = ((close_price / open_price) - 1.0) if open_price > EPSILON and close_price > EPSILON else 0.0
    amplitude_ratio = ((high_price / low_price) - 1.0) if low_price > EPSILON and high_price > EPSILON else 0.0
    return {
        "available": open_price > EPSILON and close_price > EPSILON,
        "minutes": safe_minutes,
        "return_ratio": return_ratio,
        "amplitude_ratio": amplitude_ratio,
        "open": open_price,
        "close": close_price,
        "high": high_price,
        "low": low_price,
    }


def _resolve_spot_slow_trend_step(
    *,
    symbol: str,
    base_step_price: float,
    enabled: bool,
    trigger_5m_return_ratio: float,
    trigger_15m_return_ratio: float,
    trigger_5m_amplitude_ratio: float,
    trigger_15m_amplitude_ratio: float,
    scale: float,
    tick_size: float | None,
    now: datetime | None = None,
) -> dict[str, Any]:
    safe_base_step = max(_safe_float(base_step_price), 0.0)
    configured_scale = max(_safe_float(scale), 1.0)
    result = _empty_spot_slow_trend_step(enabled=enabled, base_step_price=safe_base_step)
    result["configured_scale"] = configured_scale
    if not enabled:
        return result
    if safe_base_step <= EPSILON:
        result["warning"] = "base_step_price_not_positive"
        return result
    if configured_scale <= 1.0 + EPSILON:
        result["warning"] = "scale_not_above_one"
        return result
    current_time = now or _utc_now()
    end_ms = int(current_time.timestamp() * 1000)
    start_ms = end_ms - int(timedelta(minutes=16).total_seconds() * 1000)
    try:
        candles = fetch_spot_klines(symbol=symbol, interval="1m", start_ms=start_ms, end_ms=end_ms, limit=20)
    except Exception as exc:
        result["warning"] = f"{type(exc).__name__}: {exc}"
        return result
    closed_candles = [item for item in candles if getattr(item, "close_time", current_time) <= current_time]
    if len(closed_candles) < 5:
        result["warning"] = "insufficient_closed_1m_candles"
        return result
    window_5m = _spot_slow_trend_window_stats(closed_candles, 5)
    window_15m = _spot_slow_trend_window_stats(closed_candles, 15)
    result["window_5m"] = window_5m
    result["window_15m"] = window_15m

    reasons: list[str] = []
    for name, stats, metric, threshold in (
        ("5m_return", window_5m, "return_ratio", trigger_5m_return_ratio),
        ("15m_return", window_15m, "return_ratio", trigger_15m_return_ratio),
        ("5m_amplitude", window_5m, "amplitude_ratio", trigger_5m_amplitude_ratio),
        ("15m_amplitude", window_15m, "amplitude_ratio", trigger_15m_amplitude_ratio),
    ):
        safe_threshold = max(_safe_float(threshold), 0.0)
        if safe_threshold <= EPSILON or not bool(stats.get("available")):
            continue
        value = abs(_safe_float(stats.get(metric))) if metric == "return_ratio" else _safe_float(stats.get(metric))
        if value >= safe_threshold:
            reasons.append(f"{name}={value * 100:.3f}%>=threshold={safe_threshold * 100:.3f}%")
    if not reasons:
        return result
    desired_step = safe_base_step * configured_scale
    safe_tick = _safe_float(tick_size)
    effective_step = _round_up_to_step(desired_step, safe_tick if safe_tick > 0 else None)
    if effective_step <= safe_base_step + EPSILON:
        return result
    result.update(
        {
            "active": True,
            "effective_step_price": effective_step,
            "scale": effective_step / safe_base_step,
            "reasons": reasons,
        }
    )
    return result


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
        try:
            delete_spot_order(
                symbol=symbol,
                api_key=api_key,
                api_secret=api_secret,
                order_id=int(order_id_raw),
            )
        except RuntimeError as exc:
            if not _ignore_noop_error(exc, ("-2011", "unknown order sent")):
                raise
        count += 1
    return count


def _apply_taker_sell_response_to_state(
    *,
    state: dict[str, Any],
    response: dict[str, Any],
    base_asset: str,
    quote_asset: str,
) -> dict[str, Any]:
    metrics = state.get("metrics")
    if not isinstance(metrics, dict):
        metrics = _new_metrics()
        state["metrics"] = metrics
    lots = state.get("inventory_lots")
    if not isinstance(lots, list):
        lots = []
    transact_time_ms = _safe_int(response.get("transactTime")) or int(time.time() * 1000)
    order_id = _safe_int(response.get("orderId"))
    applied_qty = 0.0
    applied_notional = 0.0
    applied_realized_pnl = 0.0
    applied_commission_quote = 0.0
    fills = response.get("fills")
    if not isinstance(fills, list):
        fills = []
    for idx, fill in enumerate(fills):
        if not isinstance(fill, dict):
            continue
        price = max(_safe_float(fill.get("price")), 0.0)
        qty = max(_safe_float(fill.get("qty")), 0.0)
        if price <= 0 or qty <= 0:
            continue
        commission = max(_safe_float(fill.get("commission")), 0.0)
        commission_asset = str(fill.get("commissionAsset", "")).upper().strip()
        commission_quote = _normalize_commission_quote(
            commission=commission,
            commission_asset=commission_asset,
            price=price,
            base_asset=base_asset,
            quote_asset=quote_asset,
        )
        consume_qty = qty + (commission if commission_asset == base_asset else 0.0)
        lots, consumed_cost = _consume_inventory_lots(lots, consume_qty)
        proceeds_quote = price * qty - commission_quote
        realized_pnl = proceeds_quote - consumed_cost
        trade = {
            "id": _safe_int(fill.get("tradeId")) or order_id * 1000 + idx,
            "orderId": order_id,
            "time": transact_time_ms,
            "isMaker": False,
        }
        _record_trade_metrics(
            metrics=metrics,
            trade=trade,
            side="SELL",
            price=price,
            qty=qty,
            commission_quote=commission_quote,
            commission_asset=commission_asset,
            commission_raw=commission,
            realized_pnl=realized_pnl,
            role="taker_exit",
        )
        applied_qty += qty
        applied_notional += price * qty
        applied_realized_pnl += realized_pnl
        applied_commission_quote += commission_quote
    state["inventory_lots"] = lots
    state["last_trade_time_ms"] = max(_safe_int(state.get("last_trade_time_ms")), transact_time_ms)
    state.pop(SPOT_COMPETITION_RUNTIME_CACHE_KEY, None)
    return {
        "executed_qty": applied_qty,
        "executed_notional": applied_notional,
        "realized_pnl": applied_realized_pnl,
        "commission_quote": applied_commission_quote,
        "order_id": order_id,
    }


def _maybe_execute_spot_taker_exit(
    *,
    args: argparse.Namespace,
    state: dict[str, Any],
    symbol: str,
    api_key: str,
    api_secret: str,
    strategy_open_orders: list[dict[str, Any]],
    bid_price: float,
    base_asset: str,
    quote_asset: str,
    step_size: float | None,
    min_qty: float | None,
    min_notional: float | None,
) -> dict[str, Any]:
    result = {
        "enabled": bool(getattr(args, "spot_taker_exit_enabled", False)),
        "executed": False,
        "reason": "",
        "canceled_count": 0,
        "order": None,
        "execution": None,
        "break_even_price": 0.0,
        "effective_bid_after_fee": 0.0,
    }
    if not result["enabled"]:
        return result
    lots = state.get("inventory_lots")
    if not isinstance(lots, list):
        lots = []
    inventory_qty = _sum_inventory_qty(lots)
    inventory_cost = _sum_inventory_cost(lots)
    if inventory_qty <= EPSILON or inventory_cost <= EPSILON:
        result["reason"] = "flat"
        return result
    fee_ratio = max(_safe_float(getattr(args, "spot_taker_exit_fee_ratio", 0.001)), 0.0)
    min_profit_ratio = max(_safe_float(getattr(args, "spot_taker_exit_min_profit_ratio", 0.0)), 0.0)
    avg_cost = inventory_cost / inventory_qty
    effective_bid = max(bid_price, 0.0) * max(1.0 - fee_ratio, 0.0)
    break_even_price = avg_cost * (1.0 + min_profit_ratio)
    result["break_even_price"] = break_even_price
    result["effective_bid_after_fee"] = effective_bid
    if effective_bid + EPSILON < break_even_price:
        result["reason"] = "below_break_even"
        return result
    if strategy_open_orders:
        result["canceled_count"] = _cancel_orders(
            symbol=symbol,
            api_key=api_key,
            api_secret=api_secret,
            orders=strategy_open_orders,
        )
    account_info = fetch_spot_account_info(api_key, api_secret)
    base_free, _ = _extract_balance(account_info, base_asset)
    sell_qty = _round_order_qty(min(base_free, inventory_qty), step_size)
    if sell_qty <= EPSILON:
        result["reason"] = "no_free_base"
        return result
    if min_qty is not None and sell_qty < min_qty:
        result["reason"] = "below_min_qty"
        return result
    if min_notional is not None and sell_qty * max(bid_price, 0.0) < min_notional:
        result["reason"] = "below_min_notional"
        return result
    if not _truthy(args.apply):
        result["executed"] = True
        result["reason"] = "dry_run"
        result["execution"] = {"executed_qty": sell_qty, "executed_notional": sell_qty * max(bid_price, 0.0)}
        return result
    client_order_id = _build_client_order_id(str(args.client_order_prefix), "takerexit", "SELL")
    placed = post_spot_order(
        symbol=symbol,
        side="SELL",
        quantity=sell_qty,
        price=bid_price,
        api_key=api_key,
        api_secret=api_secret,
        order_type="LIMIT",
        time_in_force="IOC",
        new_client_order_id=client_order_id,
    )
    execution = _apply_taker_sell_response_to_state(
        state=state,
        response=placed,
        base_asset=base_asset,
        quote_asset=quote_asset,
    )
    result.update(
        {
            "executed": True,
            "reason": "executed",
            "order": placed,
            "execution": execution,
        }
    )
    return result


def _apply_synthetic_fast_stop_sell_to_state(
    *,
    state: dict[str, Any],
    response: dict[str, Any],
    base_asset: str,
    quote_asset: str,
    role: str,
) -> dict[str, Any]:
    metrics = state.get("metrics")
    if not isinstance(metrics, dict):
        metrics = _new_metrics()
        state["metrics"] = metrics
    transact_time_ms = _safe_int(response.get("transactTime")) or int(time.time() * 1000)
    order_id = _safe_int(response.get("orderId"))
    applied_qty = 0.0
    applied_notional = 0.0
    applied_commission_quote = 0.0
    fills = response.get("fills")
    if not isinstance(fills, list):
        fills = []
    for idx, fill in enumerate(fills):
        if not isinstance(fill, dict):
            continue
        price = max(_safe_float(fill.get("price")), 0.0)
        qty = max(_safe_float(fill.get("qty")), 0.0)
        if price <= 0 or qty <= 0:
            continue
        commission = max(_safe_float(fill.get("commission")), 0.0)
        commission_asset = str(fill.get("commissionAsset", "")).upper().strip()
        commission_quote = _normalize_commission_quote(
            commission=commission,
            commission_asset=commission_asset,
            price=price,
            base_asset=base_asset,
            quote_asset=quote_asset,
        )
        trade = {
            "id": _safe_int(fill.get("tradeId")) or order_id * 1000 + idx,
            "orderId": order_id,
            "time": transact_time_ms,
            "isMaker": False,
        }
        _record_trade_metrics(
            metrics=metrics,
            trade=trade,
            side="SELL",
            price=price,
            qty=qty,
            commission_quote=commission_quote,
            commission_asset=commission_asset,
            commission_raw=commission,
            realized_pnl=-commission_quote,
            role=role,
        )
        applied_qty += qty
        applied_notional += price * qty
        applied_commission_quote += commission_quote
    state["last_trade_time_ms"] = max(_safe_int(state.get("last_trade_time_ms")), transact_time_ms)
    state.pop(SPOT_COMPETITION_RUNTIME_CACHE_KEY, None)
    state.pop(SPOT_SYNTHETIC_NEUTRAL_RUNTIME_CACHE_KEY, None)
    return {
        "executed_qty": applied_qty,
        "executed_notional": applied_notional,
        "realized_pnl": -applied_commission_quote,
        "commission_quote": applied_commission_quote,
        "order_id": order_id,
    }


def _maybe_execute_spot_fast_stop_exit(
    *,
    args: argparse.Namespace,
    state: dict[str, Any],
    symbol: str,
    api_key: str,
    api_secret: str,
    strategy_open_orders: list[dict[str, Any]],
    bid_price: float,
    base_asset: str,
    quote_asset: str,
    step_size: float | None,
    min_qty: float | None,
    min_notional: float | None,
    fast_stop_guard: dict[str, Any],
    controls: dict[str, Any],
) -> dict[str, Any]:
    result = {
        "enabled": bool(getattr(args, "spot_fast_stop_enabled", False)),
        "executed": False,
        "reason": "",
        "canceled_count": 0,
        "order": None,
        "execution": None,
        "guard": fast_stop_guard,
    }
    if not result["enabled"]:
        return result
    if not fast_stop_guard.get("force_exit"):
        result["reason"] = "not_triggered"
        return result
    neutral_qty = max(_safe_float(getattr(args, "neutral_base_qty", 0.0)), 0.0)
    actual_base_qty = max(_safe_float(controls.get("actual_base_qty")), 0.0)
    mid_price = (max(_safe_float(bid_price), 0.0) + max(_safe_float(controls.get("center_price")), 0.0)) / 2.0
    if mid_price <= 0:
        mid_price = max(_safe_float(bid_price), 0.0)
    reduce_target_notional = max(_safe_float(getattr(args, "spot_fast_stop_reduce_target_notional", 0.0)), 0.0)
    target_excess_qty = (reduce_target_notional / mid_price) if mid_price > 0 else 0.0
    excess_qty = max(actual_base_qty - neutral_qty, 0.0)
    desired_sell_qty = max(excess_qty - target_excess_qty, 0.0)
    if desired_sell_qty <= EPSILON:
        result["reason"] = "at_or_below_target"
        return result
    account_info = fetch_spot_account_info(api_key, api_secret)
    base_free, _ = _extract_balance(account_info, base_asset)
    protected_base_qty = neutral_qty + max(_safe_float(getattr(args, "spot_fast_stop_min_base_buffer_qty", 0.0)), 0.0)
    free_excess_qty = max(base_free - protected_base_qty, 0.0)
    sell_qty = _round_order_qty(min(desired_sell_qty, free_excess_qty), step_size)
    if sell_qty <= EPSILON:
        result["reason"] = "no_free_excess_base"
        return result
    if min_qty is not None and sell_qty < min_qty:
        result["reason"] = "below_min_qty"
        return result
    if min_notional is not None and sell_qty * max(bid_price, 0.0) < min_notional:
        result["reason"] = "below_min_notional"
        return result
    if strategy_open_orders:
        result["canceled_count"] = _cancel_orders(
            symbol=symbol,
            api_key=api_key,
            api_secret=api_secret,
            orders=strategy_open_orders,
        )
    if not _truthy(args.apply):
        result["executed"] = True
        result["reason"] = "dry_run"
        result["execution"] = {"executed_qty": sell_qty, "executed_notional": sell_qty * max(bid_price, 0.0)}
        return result
    client_order_id = _build_client_order_id(str(args.client_order_prefix), "faststop", "SELL")
    placed = post_spot_order(
        symbol=symbol,
        side="SELL",
        quantity=sell_qty,
        price=bid_price,
        api_key=api_key,
        api_secret=api_secret,
        order_type="MARKET",
        new_client_order_id=client_order_id,
    )
    result["order"] = placed
    result["execution"] = _apply_synthetic_fast_stop_sell_to_state(
        state=state,
        response=placed,
        base_asset=base_asset,
        quote_asset=quote_asset,
        role="fast_stop_exit",
    )
    result["executed"] = True
    result["reason"] = "executed"
    return result


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
        "buy_qty": _safe_float(metrics.get("buy_qty")),
        "sell_qty": _safe_float(metrics.get("sell_qty")),
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
        "rolling_hourly_gross_notional": runtime_guard_result.rolling_hourly_gross_notional,
        "rolling_hourly_loss_per_10k": runtime_guard_result.rolling_hourly_loss_per_10k,
        "rolling_hourly_loss_per_10k_active": runtime_guard_result.rolling_hourly_loss_per_10k_active,
        "rolling_hourly_loss_per_10k_limit": runtime_guard_config.rolling_hourly_loss_per_10k_limit,
        "rolling_hourly_loss_per_10k_min_notional": runtime_guard_config.rolling_hourly_loss_per_10k_min_notional,
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
    open_orders: list[dict[str, Any]] | None = None
    strategy_open_orders: list[dict[str, Any]] = []
    if strategy_mode == "spot_competition_synthetic_neutral_grid":
        open_orders = fetch_spot_open_orders(symbol, api_key, api_secret)
        strategy_open_orders = _strategy_open_orders(open_orders, str(args.client_order_prefix))
        _spot_freeze_open_maker_orders(strategy_open_orders, state)
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
    elif strategy_mode == "spot_competition_synthetic_neutral_grid":
        applied_trades = _sync_synthetic_neutral_trades(
            state=state,
            trades=trades,
            base_asset=base_asset,
            quote_asset=quote_asset,
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
    if open_orders is None:
        open_orders = fetch_spot_open_orders(symbol, api_key, api_secret)
        strategy_open_orders = _strategy_open_orders(open_orders, str(args.client_order_prefix))
    if strategy_mode == "spot_competition_synthetic_neutral_grid":
        trades, refreshed_applied_trades = _refresh_spot_trades_after_account_snapshot(
            state=state,
            trades=trades,
            symbol=symbol,
            api_key=api_key,
            api_secret=api_secret,
            trade_start_ms=trade_start_ms,
            base_asset=base_asset,
            quote_asset=quote_asset,
        )
        applied_trades += refreshed_applied_trades
    metrics = state.get("metrics") if isinstance(state.get("metrics"), dict) else _new_metrics()
    app_loss_metrics = _spot_app_loss_metrics_with_trade_fallback(metrics=metrics, trades=trades)
    trade_database_status: dict[str, Any] = {"enabled": trade_database_enabled(), "trade_inserted": 0, "income_inserted": 0}
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
        if _auto_flatten_on_runtime_guard(strategy_mode):
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
        summary["trade_database"] = _persist_spot_trade_database(
            symbol=symbol,
            strategy_mode=strategy_mode,
            args=args,
            metrics=metrics,
        )
        summary["cycle_database"] = _persist_spot_cycle_database(
            symbol=symbol,
            strategy_mode=strategy_mode,
            args=args,
            summary=summary,
        )
        _save_state(state_path, state)
        _append_jsonl(Path(args.summary_jsonl), summary)
        return summary

    taker_exit_result = _maybe_execute_spot_taker_exit(
        args=args,
        state=state,
        symbol=symbol,
        api_key=api_key,
        api_secret=api_secret,
        strategy_open_orders=strategy_open_orders,
        bid_price=bid_price,
        base_asset=base_asset,
        quote_asset=quote_asset,
        step_size=symbol_info.get("step_size"),
        min_qty=symbol_info.get("min_qty"),
        min_notional=symbol_info.get("min_notional"),
    )
    if taker_exit_result.get("executed"):
        account_info = fetch_spot_account_info(api_key, api_secret)
        open_orders = fetch_spot_open_orders(symbol, api_key, api_secret)
        strategy_open_orders = _strategy_open_orders(open_orders, str(args.client_order_prefix))
    fast_stop_exit_result = {
        "enabled": bool(getattr(args, "spot_fast_stop_enabled", False)),
        "executed": False,
        "reason": "not_evaluated",
        "canceled_count": 0,
        "order": None,
        "execution": None,
        "guard": {},
    }

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
        synthetic_neutral_mode = strategy_mode == "spot_competition_synthetic_neutral_grid"
        desired_orders, controls = _build_spot_competition_inventory_grid_orders(
            state=state,
            trades=trades,
            bid_price=bid_price,
            ask_price=ask_price,
            step_price=float(args.step_price),
            buy_levels=max(_safe_int(getattr(args, "attack_buy_levels", 1)), 0),
            sell_levels=max(_safe_int(getattr(args, "attack_sell_levels", 1)), 1),
            first_order_multiplier=float(args.first_order_multiplier),
            per_order_notional=float(args.per_order_notional),
            threshold_position_notional=float(args.threshold_position_notional),
            threshold_reduce_target_notional=float(getattr(args, "threshold_reduce_target_notional", 0.0)),
            warmup_position_notional=float(getattr(args, "warmup_position_notional", 0.0)),
            require_non_loss_exit=bool(getattr(args, "require_non_loss_exit", False)),
            max_order_position_notional=float(args.max_order_position_notional),
            max_position_notional=float(args.max_position_notional),
            tick_size=symbol_info.get("tick_size"),
            step_size=symbol_info.get("step_size"),
            min_qty=symbol_info.get("min_qty"),
            min_notional=symbol_info.get("min_notional"),
            synthetic_neutral=synthetic_neutral_mode,
            neutral_base_qty=float(getattr(args, "neutral_base_qty", 0.0)),
            max_short_position_notional=float(getattr(args, "max_short_position_notional", 0.0)),
            actual_base_qty=_total_base_balance(account_info, base_asset) if synthetic_neutral_mode else None,
            symbol=symbol,
            slow_trend_step_enabled=bool(getattr(args, "spot_slow_trend_step_enabled", False)),
            slow_trend_step_5m_return_ratio=float(getattr(args, "spot_slow_trend_step_5m_return_ratio", 0.0)),
            slow_trend_step_15m_return_ratio=float(getattr(args, "spot_slow_trend_step_15m_return_ratio", 0.0)),
            slow_trend_step_5m_amplitude_ratio=float(getattr(args, "spot_slow_trend_step_5m_amplitude_ratio", 0.0)),
            slow_trend_step_15m_amplitude_ratio=float(getattr(args, "spot_slow_trend_step_15m_amplitude_ratio", 0.0)),
            slow_trend_step_scale=float(getattr(args, "spot_slow_trend_step_scale", 1.0)),
            synthetic_freeze_enabled=bool(getattr(args, "synthetic_freeze_enabled", False)),
            synthetic_freeze_loss_ratio=float(getattr(args, "synthetic_freeze_loss_ratio", 0.0)),
            synthetic_freeze_min_notional=float(getattr(args, "synthetic_freeze_min_notional", 0.0)),
            synthetic_freeze_max_side_notional=float(getattr(args, "synthetic_freeze_max_side_notional", 0.0)),
            synthetic_freeze_release_profit_ratio=float(getattr(args, "synthetic_freeze_release_profit_ratio", 0.0)),
            synthetic_freeze_pair_release_enabled=bool(getattr(args, "synthetic_freeze_pair_release_enabled", True)),
            synthetic_freeze_manual_release_side=getattr(args, "synthetic_freeze_manual_release_side", None),
            synthetic_freeze_manual_release_qty=float(getattr(args, "synthetic_freeze_manual_release_qty", 0.0)),
            synthetic_freeze_manual_release_price=float(getattr(args, "synthetic_freeze_manual_release_price", 0.0)),
        )
        current_long_notional_for_guard = _safe_float(controls.get("current_long_notional"))
        fast_stop_guard = _assess_spot_fast_stop_guard(
            symbol=symbol,
            current_long_notional=current_long_notional_for_guard,
            freeze_position_notional=float(getattr(args, "spot_fast_stop_freeze_position_notional", 0.0)),
            exit_position_notional=float(getattr(args, "spot_fast_stop_exit_position_notional", 0.0)),
            trigger_10s_abs_return_ratio=float(getattr(args, "spot_fast_stop_10s_abs_return_ratio", 0.0)),
            trigger_10s_amplitude_ratio=float(getattr(args, "spot_fast_stop_10s_amplitude_ratio", 0.0)),
            trigger_30s_abs_return_ratio=float(getattr(args, "spot_fast_stop_30s_abs_return_ratio", 0.0)),
            trigger_30s_amplitude_ratio=float(getattr(args, "spot_fast_stop_30s_amplitude_ratio", 0.0)),
            down_only=bool(getattr(args, "spot_fast_stop_down_only", True)),
        )
        fast_stop_exit_result = _maybe_execute_spot_fast_stop_exit(
            args=args,
            state=state,
            symbol=symbol,
            api_key=api_key,
            api_secret=api_secret,
            strategy_open_orders=strategy_open_orders,
            bid_price=bid_price,
            base_asset=base_asset,
            quote_asset=quote_asset,
            step_size=symbol_info.get("step_size"),
            min_qty=symbol_info.get("min_qty"),
            min_notional=symbol_info.get("min_notional"),
            fast_stop_guard=fast_stop_guard,
            controls=controls,
        )
        if fast_stop_exit_result.get("executed"):
            account_info = fetch_spot_account_info(api_key, api_secret)
            open_orders = fetch_spot_open_orders(symbol, api_key, api_secret)
            strategy_open_orders = _strategy_open_orders(open_orders, str(args.client_order_prefix))
            desired_orders = []
            controls["buy_paused"] = True
            controls["pause_reasons"] = list(controls.get("pause_reasons") or []) + list(fast_stop_guard.get("reasons") or [])
            controls["risk_state"] = "fast_stop_exit"
        elif fast_stop_guard.get("freeze_buy"):
            desired_orders = [order for order in desired_orders if str(order.get("side", "")).upper() != "BUY"]
            controls["buy_paused"] = True
            controls["pause_reasons"] = list(controls.get("pause_reasons") or []) + list(fast_stop_guard.get("reasons") or [])
            controls["risk_state"] = "fast_stop_freeze"
        controls["fast_stop_guard"] = fast_stop_guard
    quote_free, base_free = _available_new_funds(account_info, base_asset, quote_asset)
    app_loss_position_qty = _total_base_balance(account_info, base_asset)
    app_loss_mark_price = _spot_app_loss_mark_price(metrics=app_loss_metrics, bid_price=bid_price, ask_price=ask_price)
    desired_orders = _apply_spot_app_loss_guard_to_orders(
        desired_orders=desired_orders,
        controls=controls,
        metrics=app_loss_metrics,
        position_qty=app_loss_position_qty,
        latest_price=app_loss_mark_price,
        enabled=bool(getattr(args, "spot_app_loss_guard_enabled", False)),
        recovery_reduce_only_enabled=bool(getattr(args, "spot_app_loss_recovery_reduce_only_enabled", False)),
        min_notional=float(getattr(args, "spot_app_loss_min_notional", 10000.0)),
        soft_per_10k=float(getattr(args, "spot_app_loss_per_10k_soft", 0.0)),
        hard_per_10k=float(getattr(args, "spot_app_loss_per_10k_hard", 0.0)),
        maker_reduce_notional=float(getattr(args, "per_order_notional", 0.0)),
        maker_buy_reference_price=bid_price,
        maker_sell_reference_price=ask_price,
        tick_size=symbol_info.get("tick_size"),
        step_size=symbol_info.get("step_size"),
        min_qty=symbol_info.get("min_qty"),
        exchange_min_notional=symbol_info.get("min_notional"),
        available_quote_free=quote_free,
        available_base_free=base_free,
        min_bid_break_even_buffer_ticks=float(getattr(args, "spot_app_loss_min_bid_break_even_buffer_ticks", 0.0)),
    )
    spot_app_loss_stop_reason = _spot_app_loss_guard_stop_reason(
        controls.get("spot_app_loss_guard") if isinstance(controls.get("spot_app_loss_guard"), dict) else {}
    )
    if spot_app_loss_stop_reason:
        desired_orders = []
        controls["risk_state"] = spot_app_loss_stop_reason
        controls["spot_app_loss_runner_stop_reason"] = spot_app_loss_stop_reason
        controls["buy_paused"] = True
        pause_reasons = list(controls.get("pause_reasons") or [])
        if spot_app_loss_stop_reason not in pause_reasons:
            pause_reasons.append(spot_app_loss_stop_reason)
        controls["pause_reasons"] = pause_reasons
    if strategy_mode == "spot_competition_synthetic_neutral_grid":
        def _persist_spot_freeze_ledger(ledger_result: dict[str, Any]) -> None:
            state["spot_frozen_ledger"] = ledger_result
            _save_state(state_path, state)

        if spot_app_loss_stop_reason:
            controls.update(_spot_freeze_default_controls(bool(getattr(args, "spot_freeze_enabled", False))))
            controls["spot_freeze_skip_reason"] = "spot_app_loss_runner_stop"
        else:
            _maybe_run_spot_freeze(
                args=args,
                state=state,
                controls=controls,
                symbol=symbol,
                mid_price=mid_price,
                bid_price=bid_price,
                ask_price=ask_price,
                symbol_info=symbol_info,
                api_key=api_key,
                api_secret=api_secret,
                now=_utc_now(),
                persist_ledger=_persist_spot_freeze_ledger,
                existing_maker_orders=_spot_freeze_open_maker_orders(strategy_open_orders, state),
            )
            desired_orders = _apply_spot_freeze_runtime_hedge_block(
                strategy_mode=strategy_mode,
                desired_orders=desired_orders,
                controls=controls,
            )
            desired_orders = _apply_spot_freeze_maker_orders_to_desired_orders(
                desired_orders=desired_orders,
                maker_orders=list(controls.get("spot_freeze_maker_orders") or []),
                controls=controls,
            )
            if controls.get("spot_freeze_actions") or controls.get("spot_freeze_pending_contract_actions"):
                # Spot freeze may have already placed market orders; persist the ledger
                # before later stale-order reconciliation can abort the cycle.
                _save_state(state_path, state)
    elif bool(getattr(args, "spot_freeze_enabled", False)):
        controls.update(_spot_freeze_default_controls(True))
        controls["spot_freeze_alerts"] = ["unsupported_strategy_mode"]
    else:
        controls.update(_spot_freeze_default_controls(False))
    diff = diff_open_orders(existing_orders=strategy_open_orders, desired_orders=desired_orders)

    canceled_count = 0
    if (_truthy(args.cancel_stale) or spot_app_loss_stop_reason) and diff["stale_orders"]:
        canceled_count = _cancel_orders(
            symbol=symbol,
            api_key=api_key,
            api_secret=api_secret,
            orders=diff["stale_orders"],
        )

    placed_count = 0
    placement_errors: list[str] = []
    skipped_below_exchange_mins: list[str] = []
    remaining_quote_free = quote_free
    remaining_base_free = base_free
    max_single_cycle_new_orders = max(_safe_int(getattr(args, "max_single_cycle_new_orders", 0)), 0)
    min_qty = symbol_info.get("min_qty")
    min_notional = symbol_info.get("min_notional")
    tick_size = symbol_info.get("tick_size")
    for order in diff["missing_orders"]:
        if max_single_cycle_new_orders > 0 and placed_count >= max_single_cycle_new_orders:
            break
        side = str(order["side"]).upper().strip()
        qty = float(order["qty"])
        price = float(order["price"])
        try:
            latest_book_rows = fetch_spot_book_tickers(symbol=symbol)
            if latest_book_rows:
                latest_book = latest_book_rows[0]
                price = _clamp_limit_maker_price_to_book(
                    side=side,
                    price=price,
                    bid_price=_safe_float(latest_book.get("bid_price")),
                    ask_price=_safe_float(latest_book.get("ask_price")),
                    tick_size=tick_size,
                )
        except Exception:
            price = _clamp_limit_maker_price_to_book(
                side=side,
                price=price,
                bid_price=bid_price,
                ask_price=ask_price,
                tick_size=tick_size,
            )
        notional = qty * price
        if not _spot_order_meets_exchange_mins(qty=qty, price=price, min_qty=min_qty, min_notional=min_notional):
            skipped_below_exchange_mins.append(f"{side} {price:.8f} x {qty:.8f}")
            continue
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
    synthetic_net_qty = _safe_float(controls.get("synthetic_net_qty")) if isinstance(controls, dict) else 0.0
    managed_base_qty = (
        sum(max(_safe_float((state["cells"].get(str(cell["idx"])) or {}).get("position_qty")), 0.0) for cell in cells)
        if strategy_mode == "spot_one_way_long"
        else abs(synthetic_net_qty) if strategy_mode == "spot_competition_synthetic_neutral_grid" else inventory_qty
    )
    unrealized_pnl = (mid_price - inventory_avg_cost) * inventory_qty if inventory_qty > EPSILON else 0.0
    net_pnl_estimate = _safe_float(metrics.get("realized_pnl")) + unrealized_pnl
    trade_database_status = _persist_spot_trade_database(
        symbol=symbol,
        strategy_mode=strategy_mode,
        args=args,
        metrics=metrics,
    )

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
        "trade_database": trade_database_status,
        "placed_count": placed_count,
        "canceled_count": canceled_count,
        "open_strategy_orders": len(strategy_open_orders),
        "placement_errors": placement_errors,
        "skipped_below_exchange_mins": skipped_below_exchange_mins,
        "gross_notional": _safe_float(metrics.get("gross_notional")),
        "buy_notional": _safe_float(metrics.get("buy_notional")),
        "sell_notional": _safe_float(metrics.get("sell_notional")),
        "buy_qty": _safe_float(metrics.get("buy_qty")),
        "sell_qty": _safe_float(metrics.get("sell_qty")),
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
        "neutral_base_qty": _safe_float(controls.get("neutral_base_qty")),
        "actual_base_qty": _safe_float(controls.get("actual_base_qty")),
        "synthetic_net_qty": _safe_float(controls.get("synthetic_net_qty")),
        "synthetic_dust_qty": _safe_float(controls.get("synthetic_dust_qty")),
        "synthetic_dust_notional": _safe_float(controls.get("synthetic_dust_notional")),
        "current_long_notional": _safe_float(controls.get("current_long_notional")),
        "current_short_notional": _safe_float(controls.get("current_short_notional")),
        "synthetic_freeze_enabled": bool(controls.get("synthetic_freeze_enabled")),
        "synthetic_freeze_requested": bool(controls.get("synthetic_freeze_requested")),
        "synthetic_freeze_disabled_reason": str(controls.get("synthetic_freeze_disabled_reason", "") or ""),
        "synthetic_freeze_max_side_notional": _safe_float(controls.get("synthetic_freeze_max_side_notional")),
        "synthetic_freeze_pair_release_enabled": bool(controls.get("synthetic_freeze_pair_release_enabled")),
        "synthetic_freeze_candidate": controls.get("synthetic_freeze_candidate", {}),
        "synthetic_freeze_last": controls.get("synthetic_freeze_last", {}),
        "synthetic_pair_release_last": controls.get("synthetic_pair_release_last", {}),
        "synthetic_frozen_position": controls.get("synthetic_frozen_position", {}),
        "synthetic_frozen_long_qty": _safe_float(controls.get("synthetic_frozen_long_qty")),
        "synthetic_frozen_short_qty": _safe_float(controls.get("synthetic_frozen_short_qty")),
        "synthetic_frozen_long_notional": _safe_float(controls.get("synthetic_frozen_long_notional")),
        "synthetic_frozen_short_notional": _safe_float(controls.get("synthetic_frozen_short_notional")),
        "synthetic_frozen_short_quote_reserve": _safe_float(controls.get("synthetic_frozen_short_quote_reserve")),
        "max_short_position_notional": _safe_float(controls.get("max_short_position_notional")),
        "spot_freeze_enabled": bool(controls.get("spot_freeze_enabled")),
        "spot_freeze_effective_enabled": bool(controls.get("spot_freeze_effective_enabled")),
        "spot_freeze_config_reason": str(controls.get("spot_freeze_config_reason", "") or ""),
        "spot_freeze_dry_run": bool(controls.get("spot_freeze_dry_run")),
        "spot_freeze_market_execution_enabled": bool(controls.get("spot_freeze_market_execution_enabled")),
        "spot_freeze_maker_execution_enabled": bool(controls.get("spot_freeze_maker_execution_enabled")),
        "spot_freeze_runtime_blocked": bool(controls.get("spot_freeze_runtime_blocked")),
        "spot_freeze_reconcile_ok": controls.get("spot_freeze_reconcile_ok"),
        "spot_freeze_alerts": list(controls.get("spot_freeze_alerts") or []),
        "spot_freeze_actions": list(controls.get("spot_freeze_actions") or []),
        "spot_freeze_maker_orders": list(controls.get("spot_freeze_maker_orders") or []),
        "spot_freeze_existing_maker_orders": list(controls.get("spot_freeze_existing_maker_orders") or []),
        "spot_freeze_suppressed_app_loss_reduce_orders": _safe_int(
            controls.get("spot_freeze_suppressed_app_loss_reduce_orders")
        ),
        "spot_freeze_frozen_long_qty": _safe_float(controls.get("spot_freeze_frozen_long_qty")),
        "spot_freeze_frozen_short_qty": _safe_float(controls.get("spot_freeze_frozen_short_qty")),
        "spot_freeze_pending_contract_actions": list(controls.get("spot_freeze_pending_contract_actions") or []),
        "spot_freeze_loss_ratio_usable": bool(controls.get("spot_freeze_loss_ratio_usable")),
        "spot_freeze_deviation_qty": _safe_float(controls.get("spot_freeze_deviation_qty")),
        "spot_freeze_deviation_side": str(controls.get("spot_freeze_deviation_side", "") or ""),
        "spot_freeze_deviation_notional": _safe_float(controls.get("spot_freeze_deviation_notional")),
        "spot_freeze_deviation_threshold_notional": _safe_float(controls.get("spot_freeze_deviation_threshold_notional")),
        "spot_freeze_deviation_loss_ratio": _safe_float(controls.get("spot_freeze_deviation_loss_ratio")),
        "spot_freeze_deviation_cost_avg": _safe_float(controls.get("spot_freeze_deviation_cost_avg")),
        "spot_freeze_deviation_loss_reason": str(controls.get("spot_freeze_deviation_loss_reason", "") or ""),
        "spot_freeze_deviation_loss_source": str(controls.get("spot_freeze_deviation_loss_source", "") or ""),
        "spot_freeze_skip_reason": str(controls.get("spot_freeze_skip_reason", "") or ""),
        "spot_freeze_gate": controls.get("spot_freeze_gate", {}),
        "spot_app_loss_guard": controls.get("spot_app_loss_guard", {}),
        "spot_app_loss": _safe_float((controls.get("spot_app_loss_guard") or {}).get("app_loss") if isinstance(controls.get("spot_app_loss_guard"), dict) else 0.0),
        "spot_app_loss_per_10k": _safe_float((controls.get("spot_app_loss_guard") or {}).get("app_loss_per_10k") if isinstance(controls.get("spot_app_loss_guard"), dict) else 0.0),
        "spot_app_loss_guard_state": str((controls.get("spot_app_loss_guard") or {}).get("state", "") if isinstance(controls.get("spot_app_loss_guard"), dict) else ""),
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
        "base_step_price": _safe_float(controls.get("base_step_price")),
        "effective_step_price": _safe_float(controls.get("effective_step_price")),
        "slow_trend_step": controls.get("slow_trend_step", {}),
        "direction_state": str(controls.get("direction_state", "") or ""),
        "risk_state": str(controls.get("risk_state", "") or ""),
        "grid_anchor_price": _safe_float(controls.get("grid_anchor_price")),
        "pair_credit_steps": _safe_int(controls.get("pair_credit_steps")),
        "threshold_position_notional": _safe_float(controls.get("threshold_position_notional")),
        "threshold_reduce_target_notional": _safe_float(controls.get("threshold_reduce_target_notional")),
        "warmup_position_notional": _safe_float(controls.get("warmup_position_notional")),
        "require_non_loss_exit": bool(controls.get("require_non_loss_exit")),
        "spot_taker_exit": taker_exit_result,
        "spot_fast_stop_exit": fast_stop_exit_result,
        "spot_fast_stop_guard": controls.get("fast_stop_guard", {}),
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
        "rolling_hourly_gross_notional": runtime_guard_result.rolling_hourly_gross_notional,
        "rolling_hourly_loss_per_10k": runtime_guard_result.rolling_hourly_loss_per_10k,
        "rolling_hourly_loss_per_10k_active": runtime_guard_result.rolling_hourly_loss_per_10k_active,
        "rolling_hourly_loss_per_10k_limit": runtime_guard_config.rolling_hourly_loss_per_10k_limit,
        "rolling_hourly_loss_per_10k_min_notional": runtime_guard_config.rolling_hourly_loss_per_10k_min_notional,
        "cumulative_gross_notional": runtime_guard_result.cumulative_gross_notional,
        "max_cumulative_notional": runtime_guard_config.max_cumulative_notional,
    }
    if spot_app_loss_stop_reason:
        summary["runtime_status"] = "stopped"
        summary["stop_triggered"] = True
        summary["stop_reason"] = spot_app_loss_stop_reason
        summary["stop_reasons"] = [spot_app_loss_stop_reason]
        summary["stop_triggered_at"] = _utc_now().isoformat()
    summary["cycle_database"] = _persist_spot_cycle_database(
        symbol=symbol,
        strategy_mode=strategy_mode,
        args=args,
        summary=summary,
    )
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
        if strategy_mode == "spot_competition_synthetic_neutral_grid":
            if args.neutral_base_qty <= 0:
                raise RuntimeError("spot synthetic-neutral grid requires neutral_base_qty > 0")
            if args.max_short_position_notional <= 0:
                raise RuntimeError("spot synthetic-neutral grid requires max_short_position_notional > 0")
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
        choices=(
            "spot_one_way_long",
            "spot_volume_shift_long",
            "spot_competition_inventory_grid",
            "spot_competition_synthetic_neutral_grid",
        ),
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
    parser.add_argument("--threshold-reduce-target-notional", type=float, default=0.0)
    parser.add_argument("--warmup-position-notional", type=float, default=0.0)
    parser.add_argument("--require-non-loss-exit", action="store_true")
    parser.add_argument("--spot-taker-exit-enabled", action="store_true")
    parser.add_argument("--spot-taker-exit-fee-ratio", type=float, default=0.001)
    parser.add_argument("--spot-taker-exit-min-profit-ratio", type=float, default=0.0)
    parser.add_argument("--spot-fast-stop-enabled", action="store_true")
    parser.add_argument("--spot-fast-stop-down-only", dest="spot_fast_stop_down_only", action="store_true")
    parser.add_argument("--spot-fast-stop-any-direction", dest="spot_fast_stop_down_only", action="store_false")
    parser.set_defaults(spot_fast_stop_down_only=True)
    parser.add_argument("--spot-fast-stop-10s-abs-return-ratio", type=float, default=0.0)
    parser.add_argument("--spot-fast-stop-10s-amplitude-ratio", type=float, default=0.0)
    parser.add_argument("--spot-fast-stop-30s-abs-return-ratio", type=float, default=0.0)
    parser.add_argument("--spot-fast-stop-30s-amplitude-ratio", type=float, default=0.0)
    parser.add_argument("--spot-fast-stop-freeze-position-notional", type=float, default=0.0)
    parser.add_argument("--spot-fast-stop-exit-position-notional", type=float, default=0.0)
    parser.add_argument("--spot-fast-stop-reduce-target-notional", type=float, default=0.0)
    parser.add_argument("--spot-fast-stop-min-base-buffer-qty", type=float, default=0.0)
    parser.add_argument("--spot-app-loss-guard-enabled", action="store_true")
    parser.add_argument("--spot-app-loss-recovery-reduce-only-enabled", action="store_true")
    parser.add_argument("--spot-app-loss-min-notional", type=float, default=10000.0)
    parser.add_argument("--spot-app-loss-per-10k-soft", type=float, default=0.0)
    parser.add_argument("--spot-app-loss-per-10k-hard", type=float, default=0.0)
    parser.add_argument("--spot-app-loss-min-bid-break-even-buffer-ticks", type=float, default=0.0)
    parser.add_argument("--spot-slow-trend-step-enabled", action="store_true")
    parser.add_argument("--spot-slow-trend-step-5m-return-ratio", type=float, default=0.0)
    parser.add_argument("--spot-slow-trend-step-15m-return-ratio", type=float, default=0.0)
    parser.add_argument("--spot-slow-trend-step-5m-amplitude-ratio", type=float, default=0.0)
    parser.add_argument("--spot-slow-trend-step-15m-amplitude-ratio", type=float, default=0.0)
    parser.add_argument("--spot-slow-trend-step-scale", type=float, default=1.0)
    parser.add_argument("--max-order-position-notional", type=float, default=0.0)
    parser.add_argument("--max-position-notional", type=float, default=0.0)
    parser.add_argument("--neutral-base-qty", type=float, default=0.0)
    parser.add_argument("--max-short-position-notional", type=float, default=0.0)
    parser.add_argument("--synthetic-freeze-enabled", action="store_true")
    parser.add_argument("--synthetic-freeze-loss-ratio", type=float, default=0.0)
    parser.add_argument("--synthetic-freeze-min-notional", type=float, default=0.0)
    parser.add_argument("--synthetic-freeze-max-side-notional", type=float, default=0.0)
    parser.add_argument("--synthetic-freeze-release-profit-ratio", type=float, default=0.0)
    parser.add_argument("--synthetic-freeze-pair-release-enabled", dest="synthetic_freeze_pair_release_enabled", action="store_true")
    parser.add_argument("--no-synthetic-freeze-pair-release-enabled", dest="synthetic_freeze_pair_release_enabled", action="store_false")
    parser.set_defaults(synthetic_freeze_pair_release_enabled=True)
    parser.add_argument("--synthetic-freeze-manual-release-side", type=str, default="")
    parser.add_argument("--synthetic-freeze-manual-release-qty", type=float, default=0.0)
    parser.add_argument("--synthetic-freeze-manual-release-price", type=float, default=0.0)
    parser.add_argument("--spot-freeze-enabled", action="store_true")
    parser.add_argument("--spot-freeze-dry-run", action="store_true")
    parser.add_argument("--spot-freeze-market-execution-enabled", action="store_true")
    parser.add_argument("--spot-freeze-maker-execution-enabled", action="store_true")
    parser.add_argument("--spot-freeze-base-hedge-qty", type=float, default=0.0)
    parser.add_argument("--spot-freeze-tolerance-qty", type=float, default=0.0)
    parser.add_argument("--spot-freeze-deviation-notional", type=float, default=0.0)
    parser.add_argument("--spot-freeze-min-loss-ratio", type=float, default=0.0)
    parser.add_argument("--spot-freeze-max-per-cycle-notional", type=float, default=0.0)
    parser.add_argument("--spot-freeze-total-cap-notional", type=float, default=0.0)
    parser.add_argument("--spot-freeze-pair-release-enabled", action="store_true")
    parser.add_argument("--spot-freeze-profit-release-enabled", action="store_true")
    parser.add_argument("--spot-freeze-release-profit-ratio", type=float, default=0.05)
    parser.add_argument("--elastic-volume-enabled", dest="elastic_volume_enabled", action="store_true")
    parser.add_argument("--no-elastic-volume-enabled", dest="elastic_volume_enabled", action="store_false")
    parser.set_defaults(elastic_volume_enabled=False)
    parser.add_argument("--max-single-cycle-new-orders", type=int, default=8)
    parser.add_argument("--run-start-time", type=str, default=None)
    parser.add_argument("--run-end-time", type=str, default=None)
    parser.add_argument("--runtime-guard-stats-start-time", type=str, default=None)
    parser.add_argument("--rolling-hourly-loss-limit", type=float, default=None)
    parser.add_argument("--rolling-hourly-loss-per-10k-limit", type=float, default=None)
    parser.add_argument("--rolling-hourly-loss-per-10k-min-notional", type=float, default=None)
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
            if summary.get("stop_triggered"):
                raise SystemExit(2)
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
