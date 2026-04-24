from __future__ import annotations

import argparse
import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP, ROUND_UP
from pathlib import Path
from typing import Any

from .backtest import run_backtest
from .data import (
    fetch_futures_account_info_v3,
    fetch_futures_book_tickers,
    fetch_futures_open_orders,
    fetch_futures_premium_index,
    fetch_futures_position_mode,
    fetch_futures_symbol_config,
    load_binance_api_credentials,
)
from .dry_run import _round_order_price, _round_order_qty
from .live_check import extract_symbol_position
from .types import Candle

STATE_VERSION = 1


@dataclass(frozen=True)
class PlanOrder:
    side: str
    price: float
    qty: float
    notional: float
    level: int
    role: str
    position_side: str = "BOTH"
    lot_tracked: bool = False


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _isoformat(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    return dt.astimezone(timezone.utc).isoformat()


def _float(value: float) -> str:
    return f"{value:,.4f}"


def _pct(value: float) -> str:
    return f"{value * 100:.2f}%"


def _price(value: float) -> str:
    return f"{value:.7f}"


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"true", "1", "yes"}


def _round_to_nearest_step(value: float, step: float | None) -> float:
    if step is None or step <= 0:
        return float(value)
    step_dec = Decimal(str(step))
    value_dec = Decimal(str(value))
    units = (value_dec / step_dec).to_integral_value(rounding=ROUND_HALF_UP)
    return float(units * step_dec)


def _shift_center_by_minimum_increment(
    *,
    center_price: float,
    shift_size: float,
    tick_size: float | None,
    direction: str,
) -> float:
    delta = abs(float(shift_size))
    if direction == "down":
        candidate = _round_to_nearest_step(center_price - delta, tick_size)
        if abs(candidate - center_price) <= 1e-12 and tick_size is not None and tick_size > 0:
            candidate = _round_to_nearest_step(center_price - float(tick_size), tick_size)
    elif direction == "up":
        candidate = _round_to_nearest_step(center_price + delta, tick_size)
        if abs(candidate - center_price) <= 1e-12 and tick_size is not None and tick_size > 0:
            candidate = _round_to_nearest_step(center_price + float(tick_size), tick_size)
    else:
        raise ValueError(f"Unsupported direction: {direction}")
    if abs(candidate - center_price) <= 1e-12:
        raise ValueError("effective center shift is zero after tick rounding")
    return candidate


def _order_key(side: str, price: float, qty: float, position_side: str | None = None) -> str:
    normalized_position_side = str(position_side or "BOTH").upper().strip() or "BOTH"
    return f"{side.upper()}:{normalized_position_side}:{price:.10f}:{qty:.10f}"


def _order_bucket_key(side: str, price: float, position_side: str | None = None) -> str:
    normalized_position_side = str(position_side or "BOTH").upper().strip() or "BOTH"
    return f"{side.upper()}:{normalized_position_side}:{price:.10f}"


def _quantity_decimal(value: Any) -> Decimal:
    try:
        return Decimal(str(value))
    except Exception:
        return Decimal("0")


def _clone_order_with_qty(order: dict[str, Any], qty: Decimal) -> dict[str, Any]:
    cloned = dict(order)
    qty_float = float(qty)
    cloned["qty"] = qty_float
    cloned["quantity"] = qty_float
    price = _safe_float(order.get("price"))
    if price > 0:
        cloned["notional"] = price * qty_float
    return cloned


def shift_center_price(
    *,
    center_price: float,
    mid_price: float,
    step_price: float,
    tick_size: float | None,
    down_trigger_steps: int,
    up_trigger_steps: int,
    shift_steps: int,
) -> tuple[float, list[dict[str, Any]]]:
    if step_price <= 0:
        raise ValueError("step_price must be > 0")
    if down_trigger_steps <= 0 or up_trigger_steps <= 0 or shift_steps <= 0:
        raise ValueError("trigger and shift steps must be > 0")

    center = float(center_price)
    moves: list[dict[str, Any]] = []
    down_trigger = step_price * down_trigger_steps
    up_trigger = step_price * up_trigger_steps

    def _proportional_shift_steps(drift_steps: float) -> int:
        abs_drift_steps = abs(float(drift_steps))
        scaled = int(math.floor((abs_drift_steps / 2.0) + 0.5))
        max_without_overshoot = max(int(math.floor(abs_drift_steps)), 1)
        return min(max(scaled, 1), max_without_overshoot)

    if mid_price <= center - down_trigger:
        drift_steps = (center - float(mid_price)) / float(step_price)
        effective_shift_steps = _proportional_shift_steps(drift_steps)
        center = _shift_center_by_minimum_increment(
            center_price=center,
            shift_size=step_price * effective_shift_steps,
            tick_size=tick_size,
            direction="down",
        )
        moves.append(
            {
                "direction": "down",
                "new_center_price": center,
                "drift_steps": drift_steps,
                "shift_steps": effective_shift_steps,
                "shift_ratio": 1.0 / 2.0,
            }
        )
    elif mid_price >= center + up_trigger:
        drift_steps = (float(mid_price) - center) / float(step_price)
        effective_shift_steps = _proportional_shift_steps(drift_steps)
        center = _shift_center_by_minimum_increment(
            center_price=center,
            shift_size=step_price * effective_shift_steps,
            tick_size=tick_size,
            direction="up",
        )
        moves.append(
            {
                "direction": "up",
                "new_center_price": center,
                "drift_steps": drift_steps,
                "shift_steps": effective_shift_steps,
                "shift_ratio": 1.0 / 2.0,
            }
        )
    return center, moves


def _build_order(
    *,
    side: str,
    price: float,
    qty: float,
    level: int,
    role: str,
    position_side: str = "BOTH",
    lot_tracked: bool = False,
) -> PlanOrder:
    return PlanOrder(
        side=side.upper(),
        price=float(price),
        qty=float(qty),
        notional=float(price) * float(qty),
        level=level,
        role=role,
        position_side=str(position_side or "BOTH").upper().strip() or "BOTH",
        lot_tracked=bool(lot_tracked),
    )


def _normalize_lots(lots: list[dict[str, Any]] | None) -> list[dict[str, float]]:
    normalized: list[dict[str, float]] = []
    for item in lots or []:
        if not isinstance(item, dict):
            continue
        qty = max(_safe_float(item.get("qty")), 0.0)
        price = max(_safe_float(item.get("price")), 0.0)
        if qty <= 1e-12 or price <= 0:
            continue
        normalized.append({"qty": qty, "price": price})
    return normalized


def _lot_price_extremes(lots: list[dict[str, float]]) -> tuple[float | None, float | None]:
    lowest_price: float | None = None
    highest_price: float | None = None
    for item in lots:
        price = max(_safe_float(item.get("price")), 0.0)
        if price <= 0:
            continue
        if lowest_price is None or price < lowest_price:
            lowest_price = price
        if highest_price is None or price > highest_price:
            highest_price = price
    return lowest_price, highest_price


def _build_lot_take_profit_orders(
    *,
    lots: list[dict[str, float]],
    side: str,
    profit_step: float,
    min_profit_ratio: float | None = None,
    tick_size: float | None,
    step_size: float | None,
    min_qty: float | None,
    min_notional: float | None,
    role: str,
    position_side: str,
) -> tuple[list[PlanOrder], float]:
    aggregated_qty_by_price: dict[float, float] = {}
    total_qty = 0.0
    safe_side = str(side).upper().strip()
    if safe_side not in {"BUY", "SELL"}:
        raise ValueError(f"unsupported take-profit side: {side}")

    for lot in lots:
        lot_price = max(_safe_float(lot.get("price")), 0.0)
        lot_qty = _round_order_qty(max(_safe_float(lot.get("qty")), 0.0), step_size)
        if lot_price <= 0 or lot_qty <= 0:
            continue
        target_raw = lot_price + profit_step if safe_side == "SELL" else max(lot_price - profit_step, 0.0)
        target_price = _round_order_price(target_raw, tick_size, safe_side)
        min_profit_price = _nearest_profitable_exit_price(
            reference_price=lot_price,
            side=safe_side,
            min_profit_ratio=min_profit_ratio,
            tick_size=tick_size,
        )
        if min_profit_price is not None:
            if safe_side == "SELL":
                target_price = max(target_price, min_profit_price)
            else:
                target_price = min(target_price, min_profit_price)
        notional = target_price * lot_qty
        if min_qty is not None and lot_qty < min_qty:
            continue
        if min_notional is not None and notional < min_notional:
            continue
        aggregated_qty_by_price[target_price] = aggregated_qty_by_price.get(target_price, 0.0) + lot_qty
        total_qty += lot_qty

    sorted_prices = sorted(aggregated_qty_by_price, reverse=(safe_side == "BUY"))
    orders: list[PlanOrder] = []
    for level, price in enumerate(sorted_prices, start=1):
        qty = _round_order_qty(aggregated_qty_by_price[price], step_size)
        if qty <= 0:
            continue
        orders.append(
            _build_order(
                side=safe_side,
                price=price,
                qty=qty,
                level=level,
                role=role,
                position_side=position_side,
                lot_tracked=True,
            )
        )
    return orders, total_qty


def _nearest_profitable_exit_price(
    *,
    reference_price: float,
    side: str,
    min_profit_ratio: float | None,
    tick_size: float | None,
) -> float | None:
    safe_reference = max(float(reference_price), 0.0)
    if safe_reference <= 0:
        return None
    safe_side = str(side).upper().strip()
    if safe_side not in {"BUY", "SELL"}:
        raise ValueError(f"unsupported take-profit side: {side}")

    safe_ratio = 0.0 if min_profit_ratio is None else max(float(min_profit_ratio), 0.0)
    if tick_size is not None and tick_size > 0:
        tick_dec = Decimal(str(tick_size))
        reference_dec = Decimal(str(safe_reference))
        if safe_side == "BUY":
            target_dec = reference_dec * (Decimal("1") - Decimal(str(safe_ratio)))
            units = (target_dec / tick_dec).to_integral_value(rounding=ROUND_DOWN)
            candidate = units * tick_dec
            if candidate >= reference_dec:
                candidate = max(units - 1, Decimal("0")) * tick_dec
        else:
            target_dec = reference_dec * (Decimal("1") + Decimal(str(safe_ratio)))
            units = (target_dec / tick_dec).to_integral_value(rounding=ROUND_UP)
            candidate = units * tick_dec
            if candidate <= reference_dec:
                candidate = (units + 1) * tick_dec
        return max(float(candidate), 0.0)

    epsilon = max(safe_reference * 1e-9, 1e-12)
    if safe_side == "BUY":
        return max((safe_reference * (1.0 - safe_ratio)) - epsilon, 0.0)
    return safe_reference * (1.0 + safe_ratio) + epsilon


def build_static_binance_grid_plan(
    *,
    strategy_direction: str,
    grid_level_mode: str,
    min_price: float,
    max_price: float,
    n: int,
    total_grid_notional: float,
    neutral_anchor_price: float | None,
    bid_price: float,
    ask_price: float,
    tick_size: float | None,
    step_size: float | None,
    min_qty: float | None,
    min_notional: float | None,
    current_long_qty: float,
    current_short_qty: float,
    bootstrap_positions: bool = True,
) -> dict[str, Any]:
    if min_price <= 0 or max_price <= 0 or min_price >= max_price:
        raise ValueError("invalid grid bounds")
    if n <= 0:
        raise ValueError("n must be > 0")
    if total_grid_notional <= 0:
        raise ValueError("total_grid_notional must be > 0")

    mid_price = (float(bid_price) + float(ask_price)) / 2.0
    preview_time = _utc_now()
    preview_candle = Candle(
        open_time=preview_time,
        close_time=preview_time,
        open=mid_price,
        high=mid_price,
        low=mid_price,
        close=mid_price,
    )
    result = run_backtest(
        candles=[preview_candle],
        min_price=min_price,
        max_price=max_price,
        n=n,
        total_buy_notional=total_grid_notional,
        grid_level_mode=grid_level_mode,
        allocation_mode="equal",
        strategy_direction=strategy_direction,
        fee_rate=0.0,
        slippage=0.0,
        funding_rates=[],
        neutral_anchor_price=neutral_anchor_price,
        bootstrap_positions=bootstrap_positions,
        capture_funding_events=False,
        capture_trades=False,
        capture_curves=False,
    )

    def _active_grid_rows() -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for idx in range(result.n):
            grid_side = str(result.grid_sides[idx]).strip().lower()
            lower_raw = float(result.grid_levels[idx])
            upper_raw = float(result.grid_levels[idx + 1])
            lower_price = _round_order_price(lower_raw, tick_size, "BUY")
            upper_price = _round_order_price(upper_raw, tick_size, "SELL")
            raw_qty = float(result.per_grid_qty[idx])
            if grid_side == "long":
                qty = _round_order_qty(raw_qty, step_size)
                if qty <= 0:
                    continue
                bootstrapped = upper_raw > mid_price
                rows.append(
                    {
                        "grid_side": "long",
                        "lower_price": lower_price,
                        "upper_price": upper_price,
                        "qty": qty,
                        "bootstrapped": bootstrapped,
                        "active_side": "SELL" if bootstrapped else "BUY",
                    }
                )
            else:
                qty = _round_order_qty(raw_qty, step_size)
                if qty <= 0:
                    continue
                bootstrapped = lower_raw < mid_price
                rows.append(
                    {
                        "grid_side": "short",
                        "lower_price": lower_price,
                        "upper_price": upper_price,
                        "qty": qty,
                        "bootstrapped": bootstrapped,
                        "active_side": "BUY" if bootstrapped else "SELL",
                    }
                )
        return rows

    rows = _active_grid_rows()
    buy_orders: list[PlanOrder] = []
    sell_orders: list[PlanOrder] = []
    bootstrap_orders: list[PlanOrder] = []

    startup_long_qty = sum(item["qty"] for item in rows if item["grid_side"] == "long" and item["bootstrapped"])
    startup_short_qty = sum(item["qty"] for item in rows if item["grid_side"] == "short" and item["bootstrapped"])

    current_long_qty = max(float(current_long_qty), 0.0)
    current_short_qty = max(float(current_short_qty), 0.0)

    def _append_entry_buy(price: float, qty: float, level: int, role: str) -> None:
        notional = price * qty
        if price >= ask_price or qty <= 0:
            return
        if min_qty is not None and qty < min_qty:
            return
        if min_notional is not None and notional < min_notional:
            return
        buy_orders.append(_build_order(side="BUY", price=price, qty=qty, level=level, role=role))

    def _append_entry_sell(price: float, qty: float, level: int, role: str) -> None:
        notional = price * qty
        if price <= bid_price or qty <= 0:
            return
        if min_qty is not None and qty < min_qty:
            return
        if min_notional is not None and notional < min_notional:
            return
        sell_orders.append(_build_order(side="SELL", price=price, qty=qty, level=level, role=role))

    long_sell_rows = [item for item in rows if item["grid_side"] == "long" and item["bootstrapped"]]
    short_buy_rows = [item for item in rows if item["grid_side"] == "short" and item["bootstrapped"]]
    long_buy_rows = [item for item in rows if item["grid_side"] == "long" and not item["bootstrapped"]]
    short_sell_rows = [item for item in rows if item["grid_side"] == "short" and not item["bootstrapped"]]

    for level, item in enumerate(long_buy_rows, start=1):
        _append_entry_buy(item["lower_price"], item["qty"], level, "entry_long" if strategy_direction == "neutral" else "entry")
    for level, item in enumerate(short_sell_rows, start=1):
        _append_entry_sell(item["upper_price"], item["qty"], level, "entry_short")

    if startup_long_qty > 0 and current_long_qty > 0:
        remaining = _round_order_qty(current_long_qty, step_size)
        scale = max(current_long_qty / startup_long_qty, 1.0)
        for level, item in enumerate(long_sell_rows, start=1):
            desired_qty = _round_order_qty(item["qty"] * scale, step_size)
            qty = _round_order_qty(min(desired_qty, remaining), step_size)
            if qty <= 0:
                continue
            _append_entry_sell(item["upper_price"], qty, level, "take_profit_long" if strategy_direction == "neutral" else "take_profit")
            remaining = max(remaining - qty, 0.0)
            if remaining <= 0:
                break

    if startup_short_qty > 0 and current_short_qty > 0:
        remaining = _round_order_qty(current_short_qty, step_size)
        scale = max(current_short_qty / startup_short_qty, 1.0)
        for level, item in enumerate(short_buy_rows, start=1):
            desired_qty = _round_order_qty(item["qty"] * scale, step_size)
            qty = _round_order_qty(min(desired_qty, remaining), step_size)
            if qty <= 0:
                continue
            _append_entry_buy(item["lower_price"], qty, level, "take_profit_short")
            remaining = max(remaining - qty, 0.0)
            if remaining <= 0:
                break

    bootstrap_long_qty = _round_order_qty(max(startup_long_qty - current_long_qty, 0.0), step_size)
    if bootstrap_positions and bootstrap_long_qty > 0:
        bootstrap_price = _round_order_price(bid_price, tick_size, "BUY")
        bootstrap_notional = bootstrap_price * bootstrap_long_qty
        if (min_qty is None or bootstrap_long_qty >= min_qty) and (
            min_notional is None or bootstrap_notional >= min_notional
        ):
            bootstrap_orders.append(
                _build_order(
                    side="BUY",
                    price=bootstrap_price,
                    qty=bootstrap_long_qty,
                    level=0,
                    role="bootstrap_long" if strategy_direction == "neutral" else "bootstrap",
                )
            )

    bootstrap_short_qty = _round_order_qty(max(startup_short_qty - current_short_qty, 0.0), step_size)
    if bootstrap_positions and bootstrap_short_qty > 0:
        bootstrap_price = _round_order_price(ask_price, tick_size, "SELL")
        bootstrap_notional = bootstrap_price * bootstrap_short_qty
        if (min_qty is None or bootstrap_short_qty >= min_qty) and (
            min_notional is None or bootstrap_notional >= min_notional
        ):
            bootstrap_orders.append(
                _build_order(
                    side="SELL",
                    price=bootstrap_price,
                    qty=bootstrap_short_qty,
                    level=0,
                    role="bootstrap_short",
                )
            )

    target_base_qty = startup_long_qty if strategy_direction == "long" else startup_short_qty if strategy_direction == "short" else 0.0
    bootstrap_qty = bootstrap_long_qty if strategy_direction == "long" else bootstrap_short_qty if strategy_direction == "short" else 0.0
    target_long_base_qty = startup_long_qty
    target_short_base_qty = startup_short_qty
    if not bootstrap_positions:
        bootstrap_long_qty = 0.0
        bootstrap_short_qty = 0.0
        target_base_qty = 0.0
        bootstrap_qty = 0.0
        target_long_base_qty = 0.0
        target_short_base_qty = 0.0

    return {
        "buy_orders": [order.__dict__ for order in buy_orders],
        "sell_orders": [order.__dict__ for order in sell_orders],
        "bootstrap_orders": [order.__dict__ for order in bootstrap_orders],
        "target_base_qty": target_base_qty,
        "bootstrap_qty": bootstrap_qty,
        "target_long_base_qty": target_long_base_qty,
        "target_short_base_qty": target_short_base_qty,
        "bootstrap_long_qty": bootstrap_long_qty,
        "bootstrap_short_qty": bootstrap_short_qty,
        "active_buy_order_count": len(buy_orders),
        "active_sell_order_count": len(sell_orders),
        "static_grid_rows": rows,
    }


def build_micro_grid_plan(
    *,
    center_price: float,
    step_price: float,
    buy_levels: int,
    sell_levels: int,
    per_order_notional: float,
    base_position_notional: float,
    bid_price: float,
    ask_price: float,
    tick_size: float | None,
    step_size: float | None,
    min_qty: float | None,
    min_notional: float | None,
    current_long_qty: float,
) -> dict[str, Any]:
    if center_price <= 0 or step_price <= 0:
        raise ValueError("center_price and step_price must be > 0")
    if buy_levels < 0 or sell_levels < 0:
        raise ValueError("buy_levels and sell_levels must be >= 0")
    if per_order_notional <= 0 or base_position_notional < 0:
        raise ValueError("per_order_notional must be > 0 and base_position_notional >= 0")

    buy_orders: list[PlanOrder] = []
    sell_orders: list[PlanOrder] = []

    for level in range(1, buy_levels + 1):
        price_raw = center_price - (level * step_price)
        price = _round_order_price(price_raw, tick_size, "BUY")
        qty = _round_order_qty(per_order_notional / price, step_size)
        notional = price * qty
        if price >= ask_price:
            continue
        if min_qty is not None and qty < min_qty:
            continue
        if min_notional is not None and notional < min_notional:
            continue
        if qty <= 0:
            continue
        buy_orders.append(_build_order(side="BUY", price=price, qty=qty, level=level, role="entry"))

    target_base_qty = _round_order_qty(base_position_notional / center_price, step_size)
    current_long_qty = max(float(current_long_qty), 0.0)
    bootstrap_qty = max(target_base_qty - current_long_qty, 0.0)
    remaining_sell_qty = _round_order_qty(current_long_qty, step_size)

    for level in range(1, sell_levels + 1):
        if remaining_sell_qty <= 0:
            break
        price_raw = center_price + (level * step_price)
        price = _round_order_price(price_raw, tick_size, "SELL")
        desired_qty = _round_order_qty(per_order_notional / price, step_size)
        qty = min(desired_qty, remaining_sell_qty)
        qty = _round_order_qty(qty, step_size)
        notional = price * qty
        if price <= bid_price:
            continue
        if min_qty is not None and qty < min_qty:
            continue
        if min_notional is not None and notional < min_notional:
            continue
        if qty <= 0:
            continue
        sell_orders.append(_build_order(side="SELL", price=price, qty=qty, level=level, role="take_profit"))
        remaining_sell_qty = max(remaining_sell_qty - qty, 0.0)

    bootstrap_orders: list[PlanOrder] = []
    if bootstrap_qty > 0:
        bootstrap_price = _round_order_price(bid_price, tick_size, "BUY")
        bootstrap_qty = _round_order_qty(bootstrap_qty, step_size)
        bootstrap_notional = bootstrap_price * bootstrap_qty
        if bootstrap_qty > 0 and (
            (min_qty is None or bootstrap_qty >= min_qty)
            and (min_notional is None or bootstrap_notional >= min_notional)
        ):
            bootstrap_orders.append(
                _build_order(side="BUY", price=bootstrap_price, qty=bootstrap_qty, level=0, role="bootstrap")
            )

    return {
        "buy_orders": [order.__dict__ for order in buy_orders],
        "sell_orders": [order.__dict__ for order in sell_orders],
        "bootstrap_orders": [order.__dict__ for order in bootstrap_orders],
        "target_base_qty": target_base_qty,
        "bootstrap_qty": bootstrap_qty,
    }


def build_short_micro_grid_plan(
    *,
    center_price: float,
    step_price: float,
    buy_levels: int,
    sell_levels: int,
    per_order_notional: float,
    base_position_notional: float,
    bid_price: float,
    ask_price: float,
    tick_size: float | None,
    step_size: float | None,
    min_qty: float | None,
    min_notional: float | None,
    current_short_qty: float,
) -> dict[str, Any]:
    if center_price <= 0 or step_price <= 0:
        raise ValueError("center_price and step_price must be > 0")
    if buy_levels < 0 or sell_levels < 0:
        raise ValueError("buy_levels and sell_levels must be >= 0")
    if per_order_notional <= 0 or base_position_notional < 0:
        raise ValueError("per_order_notional must be > 0 and base_position_notional >= 0")

    buy_orders: list[PlanOrder] = []
    sell_orders: list[PlanOrder] = []

    remaining_buyback_qty = _round_order_qty(max(float(current_short_qty), 0.0), step_size)
    for level in range(1, buy_levels + 1):
        if remaining_buyback_qty <= 0:
            break
        price_raw = center_price - (level * step_price)
        price = _round_order_price(price_raw, tick_size, "BUY")
        desired_qty = _round_order_qty(per_order_notional / price, step_size)
        qty = _round_order_qty(min(desired_qty, remaining_buyback_qty), step_size)
        notional = price * qty
        if price >= ask_price:
            continue
        if min_qty is not None and qty < min_qty:
            continue
        if min_notional is not None and notional < min_notional:
            continue
        if qty <= 0:
            continue
        buy_orders.append(_build_order(side="BUY", price=price, qty=qty, level=level, role="take_profit_short"))
        remaining_buyback_qty = max(remaining_buyback_qty - qty, 0.0)

    for level in range(1, sell_levels + 1):
        price_raw = center_price + (level * step_price)
        price = _round_order_price(price_raw, tick_size, "SELL")
        qty = _round_order_qty(per_order_notional / price, step_size)
        notional = price * qty
        if price <= bid_price:
            continue
        if min_qty is not None and qty < min_qty:
            continue
        if min_notional is not None and notional < min_notional:
            continue
        if qty <= 0:
            continue
        sell_orders.append(_build_order(side="SELL", price=price, qty=qty, level=level, role="entry_short"))

    target_base_qty = _round_order_qty(base_position_notional / center_price, step_size)
    current_short_qty = max(float(current_short_qty), 0.0)
    bootstrap_qty = max(target_base_qty - current_short_qty, 0.0)

    bootstrap_orders: list[PlanOrder] = []
    if bootstrap_qty > 0:
        bootstrap_price = _round_order_price(ask_price, tick_size, "SELL")
        bootstrap_qty = _round_order_qty(bootstrap_qty, step_size)
        bootstrap_notional = bootstrap_price * bootstrap_qty
        if bootstrap_qty > 0 and (
            (min_qty is None or bootstrap_qty >= min_qty)
            and (min_notional is None or bootstrap_notional >= min_notional)
        ):
            bootstrap_orders.append(
                _build_order(side="SELL", price=bootstrap_price, qty=bootstrap_qty, level=0, role="bootstrap_short")
            )

    return {
        "buy_orders": [order.__dict__ for order in buy_orders],
        "sell_orders": [order.__dict__ for order in sell_orders],
        "bootstrap_orders": [order.__dict__ for order in bootstrap_orders],
        "target_base_qty": target_base_qty,
        "bootstrap_qty": bootstrap_qty,
    }


def build_best_quote_short_flip_plan(
    *,
    bid_price: float,
    ask_price: float,
    per_order_notional: float,
    max_short_position_notional: float,
    max_entry_orders: int,
    current_short_qty: float,
    current_short_notional: float,
    tick_size: float | None,
    step_size: float | None,
    min_qty: float | None,
    min_notional: float | None,
) -> dict[str, Any]:
    if bid_price <= 0 or ask_price <= 0:
        raise ValueError("bid_price and ask_price must be > 0")
    if per_order_notional <= 0:
        raise ValueError("per_order_notional must be > 0")
    if max_short_position_notional <= 0:
        raise ValueError("max_short_position_notional must be > 0")
    if max_entry_orders < 0:
        raise ValueError("max_entry_orders must be >= 0")

    buy_orders: list[PlanOrder] = []
    sell_orders: list[PlanOrder] = []

    buy_price = _round_order_price(bid_price, tick_size, "BUY")
    sell_price = _round_order_price(ask_price, tick_size, "SELL")
    current_short_qty = max(float(current_short_qty), 0.0)
    current_short_notional = max(float(current_short_notional), 0.0)

    cover_qty = _round_order_qty(current_short_qty, step_size)
    cover_notional = buy_price * cover_qty
    if (
        cover_qty > 0
        and buy_price < ask_price
        and (min_qty is None or cover_qty >= min_qty)
        and (min_notional is None or cover_notional >= min_notional)
    ):
        buy_orders.append(
            _build_order(
                side="BUY",
                price=buy_price,
                qty=cover_qty,
                level=1,
                role="take_profit_short",
            )
        )

    remaining_short_budget = max(max_short_position_notional - current_short_notional, 0.0)
    entry_slots = min(int((remaining_short_budget + 1e-9) / per_order_notional), max_entry_orders)
    entry_qty = _round_order_qty(per_order_notional / sell_price, step_size)
    entry_notional = sell_price * entry_qty
    if (
        entry_qty > 0
        and sell_price > bid_price
        and (min_qty is None or entry_qty >= min_qty)
        and (min_notional is None or entry_notional >= min_notional)
    ):
        for level in range(1, entry_slots + 1):
            sell_orders.append(
                _build_order(
                    side="SELL",
                    price=sell_price,
                    qty=entry_qty,
                    level=level,
                    role="entry_short",
                )
            )

    return {
        "buy_orders": [order.__dict__ for order in buy_orders],
        "sell_orders": [order.__dict__ for order in sell_orders],
        "bootstrap_orders": [],
        "target_base_qty": 0.0,
        "bootstrap_qty": 0.0,
        "active_buy_order_count": len(buy_orders),
        "active_sell_order_count": len(sell_orders),
    }


def build_best_quote_long_flip_plan(
    *,
    bid_price: float,
    ask_price: float,
    per_order_notional: float,
    max_position_notional: float,
    max_entry_orders: int,
    max_exit_orders: int,
    current_long_qty: float,
    current_long_notional: float,
    tick_size: float | None,
    step_size: float | None,
    min_qty: float | None,
    min_notional: float | None,
) -> dict[str, Any]:
    if bid_price <= 0 or ask_price <= 0:
        raise ValueError("bid_price and ask_price must be > 0")
    if per_order_notional <= 0:
        raise ValueError("per_order_notional must be > 0")
    if max_position_notional <= 0:
        raise ValueError("max_position_notional must be > 0")
    if max_entry_orders < 0:
        raise ValueError("max_entry_orders must be >= 0")
    if max_exit_orders < 0:
        raise ValueError("max_exit_orders must be >= 0")

    buy_orders: list[PlanOrder] = []
    sell_orders: list[PlanOrder] = []

    buy_price = _round_order_price(bid_price, tick_size, "BUY")
    sell_price = _round_order_price(ask_price, tick_size, "SELL")
    current_long_qty = max(float(current_long_qty), 0.0)
    current_long_notional = max(float(current_long_notional), 0.0)

    remaining_reduce_qty = _round_order_qty(current_long_qty, step_size)
    reduce_order_qty = _round_order_qty(per_order_notional / sell_price, step_size)
    for level in range(1, max_exit_orders + 1):
        if remaining_reduce_qty <= 0:
            break
        reduce_qty = min(reduce_order_qty, remaining_reduce_qty)
        reduce_qty = _round_order_qty(reduce_qty, step_size)
        reduce_notional = sell_price * reduce_qty
        if (
            reduce_qty > 0
            and sell_price > bid_price
            and (min_qty is None or reduce_qty >= min_qty)
            and (min_notional is None or reduce_notional >= min_notional)
        ):
            sell_orders.append(
                _build_order(
                    side="SELL",
                    price=sell_price,
                    qty=reduce_qty,
                    level=level,
                    role="take_profit",
                )
            )
            remaining_reduce_qty = max(remaining_reduce_qty - reduce_qty, 0.0)

    remaining_long_budget = max(max_position_notional - current_long_notional, 0.0)
    entry_slots = min(int((remaining_long_budget + 1e-9) / per_order_notional), max_entry_orders)
    entry_qty = _round_order_qty(per_order_notional / buy_price, step_size)
    entry_notional = buy_price * entry_qty
    if (
        entry_qty > 0
        and buy_price < ask_price
        and (min_qty is None or entry_qty >= min_qty)
        and (min_notional is None or entry_notional >= min_notional)
    ):
        for level in range(1, entry_slots + 1):
            buy_orders.append(
                _build_order(
                    side="BUY",
                    price=buy_price,
                    qty=entry_qty,
                    level=level,
                    role="entry",
                )
            )

    return {
        "buy_orders": [order.__dict__ for order in buy_orders],
        "sell_orders": [order.__dict__ for order in sell_orders],
        "bootstrap_orders": [],
        "target_base_qty": 0.0,
        "bootstrap_qty": 0.0,
        "active_buy_order_count": len(buy_orders),
        "active_sell_order_count": len(sell_orders),
    }


def build_hedge_micro_grid_plan(
    *,
    center_price: float,
    step_price: float,
    buy_levels: int,
    sell_levels: int,
    per_order_notional: float,
    base_position_notional: float,
    bid_price: float,
    ask_price: float,
    tick_size: float | None,
    step_size: float | None,
    min_qty: float | None,
    min_notional: float | None,
    current_long_qty: float,
    current_short_qty: float,
    current_long_avg_price: float = 0.0,
    current_short_avg_price: float = 0.0,
    current_long_lots: list[dict[str, Any]] | None = None,
    current_short_lots: list[dict[str, Any]] | None = None,
    startup_entry_multiplier: float = 1.0,
    startup_large_entry_active: bool = False,
    near_market_entry_max_center_distance_steps: float = 2.0,
    near_market_entries_allowed: bool | None = None,
    buy_offset_steps: float = 0.0,
    sell_offset_steps: float = 0.0,
    entry_long_cost_guard_release_notional: float = 0.0,
    entry_short_cost_guard_release_notional: float = 0.0,
    take_profit_min_profit_ratio: float | None = None,
    residual_long_flat_notional: float = 0.0,
    residual_short_flat_notional: float = 0.0,
    synthetic_tiny_long_residual_notional: float | None = None,
    synthetic_tiny_short_residual_notional: float | None = None,
    entry_long_paused: bool = False,
    entry_short_paused: bool = False,
    paused_entry_short_scale: float = 0.0,
) -> dict[str, Any]:
    if center_price <= 0 or step_price <= 0:
        raise ValueError("center_price and step_price must be > 0")
    if buy_levels < 0 or sell_levels < 0:
        raise ValueError("buy_levels and sell_levels must be >= 0")
    if per_order_notional <= 0 or base_position_notional < 0:
        raise ValueError("per_order_notional must be > 0 and base_position_notional >= 0")

    buy_orders: list[PlanOrder] = []
    sell_orders: list[PlanOrder] = []
    current_long_qty = max(float(current_long_qty), 0.0)
    current_short_qty = max(float(current_short_qty), 0.0)
    current_long_avg_price = max(float(current_long_avg_price), 0.0)
    current_short_avg_price = max(float(current_short_avg_price), 0.0)
    current_long_lots = _normalize_lots(current_long_lots)
    current_short_lots = _normalize_lots(current_short_lots)
    startup_multiplier = max(float(startup_entry_multiplier), 1.0)
    near_market_entry_max_center_distance_steps = max(float(near_market_entry_max_center_distance_steps), 0.0)
    buy_offset_steps = float(buy_offset_steps)
    sell_offset_steps = float(sell_offset_steps)
    paused_entry_short_scale = min(max(float(paused_entry_short_scale), 0.0), 1.0)
    mid_price = (
        (float(bid_price) + float(ask_price)) / 2.0
        if bid_price is not None and ask_price is not None and bid_price > 0 and ask_price > 0
        else float(center_price)
    )
    center_distance_steps = abs(float(mid_price) - float(center_price)) / float(step_price)
    if near_market_entries_allowed is None:
        near_market_entries_allowed = center_distance_steps <= (near_market_entry_max_center_distance_steps + 1e-12)
    else:
        near_market_entries_allowed = bool(near_market_entries_allowed)
    long_inventory_dust = (
        current_long_qty > 0
        and min_notional is not None
        and min_notional > 0
        and (current_long_qty * mid_price) < float(min_notional)
    )
    short_inventory_dust = (
        current_short_qty > 0
        and min_notional is not None
        and min_notional > 0
        and (current_short_qty * mid_price) < float(min_notional)
    )
    effective_long_qty = 0.0 if long_inventory_dust else current_long_qty
    effective_short_qty = 0.0 if short_inventory_dust else current_short_qty
    residual_long_flat_notional = max(float(residual_long_flat_notional), 0.0)
    residual_short_flat_notional = max(float(residual_short_flat_notional), 0.0)
    tiny_long_residual_threshold_notional = (
        max(float(synthetic_tiny_long_residual_notional), 0.0)
        if synthetic_tiny_long_residual_notional is not None
        else max(float(per_order_notional), 0.0)
    )
    tiny_short_residual_threshold_notional = (
        max(float(synthetic_tiny_short_residual_notional), 0.0)
        if synthetic_tiny_short_residual_notional is not None
        else max(float(per_order_notional), 0.0)
    )
    residual_long_flattened_qty = 0.0
    residual_long_flattened_notional = 0.0
    residual_long_flattened = False
    if (
        residual_long_flat_notional > 0
        and effective_long_qty > 0
        and effective_short_qty <= 0
        and (effective_long_qty * mid_price) <= residual_long_flat_notional
    ):
        residual_long_flattened = True
        residual_long_flattened_qty = effective_long_qty
        residual_long_flattened_notional = effective_long_qty * mid_price
        effective_long_qty = 0.0
    residual_short_flattened_qty = 0.0
    residual_short_flattened_notional = 0.0
    residual_short_flattened = False
    if (
        residual_short_flat_notional > 0
        and effective_short_qty > 0
        and effective_long_qty <= 0
        and (effective_short_qty * mid_price) <= residual_short_flat_notional
    ):
        residual_short_flattened = True
        residual_short_flattened_qty = effective_short_qty
        residual_short_flattened_notional = effective_short_qty * mid_price
        effective_short_qty = 0.0
    effective_long_lots = current_long_lots if effective_long_qty > 1e-12 else []
    effective_short_lots = current_short_lots if effective_short_qty > 1e-12 else []
    flat_inventory = effective_long_qty <= 1e-12 and effective_short_qty <= 1e-12
    effective_long_notional = effective_long_qty * mid_price
    effective_short_notional = effective_short_qty * mid_price
    dominant_long_with_tiny_short_residual = (
        effective_long_qty > 0
        and effective_short_qty > 0
        and effective_long_notional > effective_short_notional
        and effective_short_notional <= tiny_short_residual_threshold_notional
    )
    dominant_short_with_tiny_long_residual = (
        effective_short_qty > 0
        and effective_long_qty > 0
        and effective_short_notional > effective_long_notional
        and effective_long_notional <= tiny_long_residual_threshold_notional
    )
    long_entry_cost_guard_active = (
        max(float(entry_long_cost_guard_release_notional), 0.0) > 0
        and effective_long_qty > 0
        and (effective_short_qty <= 0 or dominant_long_with_tiny_short_residual)
        and effective_long_qty * mid_price < float(entry_long_cost_guard_release_notional)
    )
    short_entry_cost_guard_active = (
        max(float(entry_short_cost_guard_release_notional), 0.0) > 0
        and effective_short_qty > 0
        and (effective_long_qty <= 0 or dominant_short_with_tiny_long_residual)
        and effective_short_qty * mid_price < float(entry_short_cost_guard_release_notional)
    )
    long_exit_profit_guard_active = (
        max(float(entry_long_cost_guard_release_notional), 0.0) > 0
        and effective_long_qty > 0
        and (effective_short_qty <= 0 or dominant_long_with_tiny_short_residual)
    )
    short_exit_profit_guard_active = (
        max(float(entry_short_cost_guard_release_notional), 0.0) > 0
        and effective_short_qty > 0
        and (effective_long_qty <= 0 or dominant_short_with_tiny_long_residual)
    )
    lowest_long_lot_price, highest_long_lot_price = _lot_price_extremes(effective_long_lots)
    lowest_short_lot_price, highest_short_lot_price = _lot_price_extremes(effective_short_lots)
    held_long_same_side_entry = effective_long_qty > 0 and (
        effective_short_qty <= 0 or dominant_long_with_tiny_short_residual
    )
    held_short_same_side_entry = effective_short_qty > 0 and (
        effective_long_qty <= 0 or dominant_short_with_tiny_long_residual
    )
    light_long_entry_max_price = (
        _round_order_price(max(float(bid_price) - float(step_price), 0.0), tick_size, "BUY")
        if long_entry_cost_guard_active and bid_price is not None and bid_price > 0
        else None
    )
    light_short_entry_min_price = (
        _round_order_price(float(ask_price) + float(step_price), tick_size, "SELL")
        if short_entry_cost_guard_active and ask_price is not None and ask_price > 0
        else None
    )
    long_entry_reference_price = (
        lowest_long_lot_price
        if lowest_long_lot_price is not None
        else (current_long_avg_price if current_long_avg_price > 0 else None)
    )
    short_entry_reference_price = (
        highest_short_lot_price
        if highest_short_lot_price is not None
        else (current_short_avg_price if current_short_avg_price > 0 else None)
    )
    long_exit_reference_price = (
        highest_long_lot_price
        if highest_long_lot_price is not None
        else (current_long_avg_price if current_long_avg_price > 0 else None)
    )
    short_exit_reference_price = (
        lowest_short_lot_price
        if lowest_short_lot_price is not None
        else (current_short_avg_price if current_short_avg_price > 0 else None)
    )
    long_non_loss_exit_price = (
        _nearest_profitable_exit_price(
            reference_price=long_exit_reference_price,
            side="SELL",
            min_profit_ratio=take_profit_min_profit_ratio,
            tick_size=tick_size,
        )
        if take_profit_min_profit_ratio is not None and long_exit_reference_price is not None
        else None
    )
    short_non_loss_exit_price = (
        _nearest_profitable_exit_price(
            reference_price=short_exit_reference_price,
            side="BUY",
            min_profit_ratio=take_profit_min_profit_ratio,
            tick_size=tick_size,
        )
        if take_profit_min_profit_ratio is not None and short_exit_reference_price is not None
        else None
    )
    front_short_exit_reference_price = (
        highest_short_lot_price
        if highest_short_lot_price is not None
        else short_exit_reference_price
    )
    light_short_front_exit_price = None
    if short_exit_profit_guard_active and take_profit_min_profit_ratio is not None:
        light_short_front_exit_price = _nearest_profitable_exit_price(
            reference_price=(front_short_exit_reference_price or 0.0),
            side="BUY",
            min_profit_ratio=take_profit_min_profit_ratio,
            tick_size=tick_size,
        )
    price_tick = float(tick_size) if tick_size is not None and tick_size > 0 else 1e-12
    profit_step = max(float(step_price), price_tick)
    long_exit_anchor_price = None
    if long_exit_profit_guard_active and long_exit_reference_price is not None:
        market_anchor_price = _round_order_price(float(ask_price) + price_tick, tick_size, "SELL")
        min_profit_anchor_price = _round_order_price(
            float(long_exit_reference_price) + profit_step,
            tick_size,
            "SELL",
        )
        if market_anchor_price <= min_profit_anchor_price + float(step_price) + 1e-12:
            long_exit_anchor_price = min_profit_anchor_price
        else:
            long_exit_anchor_price = max(market_anchor_price, min_profit_anchor_price)

    short_exit_anchor_price = None
    if short_exit_profit_guard_active and short_exit_reference_price is not None:
        market_anchor_price = _round_order_price(max(float(bid_price) - price_tick, 0.0), tick_size, "BUY")
        max_profit_anchor_price = _round_order_price(
            max(float(short_exit_reference_price) - profit_step, 0.0),
            tick_size,
            "BUY",
        )
        if max_profit_anchor_price <= market_anchor_price + float(step_price) + 1e-12:
            short_exit_anchor_price = max_profit_anchor_price
        else:
            short_exit_anchor_price = min(market_anchor_price, max_profit_anchor_price)

    def _entry_notional(*, level: int, current_qty: float) -> float:
        if startup_large_entry_active and level == 1 and current_qty <= 1e-12:
            return per_order_notional * startup_multiplier
        return per_order_notional

    def _buy_price(level: int) -> float:
        distance_steps = max((float(level) - 1.0) + buy_offset_steps, 0.0)
        anchor_price = short_exit_anchor_price if short_exit_anchor_price is not None else float(bid_price)
        price_raw = float(Decimal(str(anchor_price)) - (Decimal(str(distance_steps)) * Decimal(str(step_price))))
        return _round_order_price(price_raw, tick_size, "BUY")

    def _sell_price(level: int) -> float:
        distance_steps = max((float(level) - 1.0) + sell_offset_steps, 0.0)
        anchor_price = long_exit_anchor_price if long_exit_anchor_price is not None else float(ask_price)
        price_raw = float(Decimal(str(anchor_price)) + (Decimal(str(distance_steps)) * Decimal(str(step_price))))
        return _round_order_price(price_raw, tick_size, "SELL")

    def _entry_buy_price(level: int) -> float:
        if not near_market_entries_allowed:
            distance_steps = max(float(level) + buy_offset_steps, 1.0)
            price_raw = float(Decimal(str(center_price)) - (Decimal(str(distance_steps)) * Decimal(str(step_price))))
        elif flat_inventory:
            distance_steps = max((float(level) - 1.0) + buy_offset_steps, 0.0)
            price_raw = float(Decimal(str(bid_price)) - (Decimal(str(distance_steps)) * Decimal(str(step_price))))
        elif held_long_same_side_entry and lowest_long_lot_price is not None:
            lot_distance_steps = max(float(level) + buy_offset_steps, 1.0)
            market_distance_steps = max((float(level) - 1.0) + buy_offset_steps, 0.0)
            lot_price_raw = float(
                Decimal(str(lowest_long_lot_price)) - (Decimal(str(lot_distance_steps)) * Decimal(str(step_price)))
            )
            market_price_raw = float(
                Decimal(str(bid_price)) - (Decimal(str(market_distance_steps)) * Decimal(str(step_price)))
            )
            price_raw = market_price_raw if lot_price_raw >= float(ask_price) else lot_price_raw
        else:
            distance_steps = max(float(level) + buy_offset_steps, 1.0)
            price_raw = float(Decimal(str(bid_price)) - (Decimal(str(distance_steps)) * Decimal(str(step_price))))
        return _round_order_price(price_raw, tick_size, "BUY")

    def _entry_sell_price(level: int) -> float:
        if not near_market_entries_allowed:
            distance_steps = max(float(level) + sell_offset_steps, 1.0)
            price_raw = float(Decimal(str(center_price)) + (Decimal(str(distance_steps)) * Decimal(str(step_price))))
        elif flat_inventory:
            distance_steps = max((float(level) - 1.0) + sell_offset_steps, 0.0)
            price_raw = float(Decimal(str(ask_price)) + (Decimal(str(distance_steps)) * Decimal(str(step_price))))
        elif held_short_same_side_entry and highest_short_lot_price is not None:
            lot_distance_steps = max(float(level) + sell_offset_steps, 1.0)
            market_distance_steps = max((float(level) - 1.0) + sell_offset_steps, 0.0)
            lot_price_raw = float(
                Decimal(str(highest_short_lot_price)) + (Decimal(str(lot_distance_steps)) * Decimal(str(step_price)))
            )
            market_price_raw = float(
                Decimal(str(ask_price)) + (Decimal(str(market_distance_steps)) * Decimal(str(step_price)))
            )
            price_raw = market_price_raw if lot_price_raw <= float(bid_price) else lot_price_raw
        else:
            distance_steps = max(float(level) + sell_offset_steps, 1.0)
            price_raw = float(Decimal(str(ask_price)) + (Decimal(str(distance_steps)) * Decimal(str(step_price))))
        return _round_order_price(price_raw, tick_size, "SELL")

    def _paused_probe_sell_price(level: int) -> float:
        distance_steps = max((float(level) - 1.0) + sell_offset_steps, 0.0)
        price_raw = float(Decimal(str(ask_price)) + (Decimal(str(distance_steps)) * Decimal(str(step_price))))
        return _round_order_price(price_raw, tick_size, "SELL")

    short_lot_exit_orders: list[PlanOrder] = []
    short_lot_exit_qty = 0.0
    if short_exit_profit_guard_active:
        short_lot_exit_orders, short_lot_exit_qty = _build_lot_take_profit_orders(
            lots=effective_short_lots,
            side="BUY",
            profit_step=profit_step,
            min_profit_ratio=take_profit_min_profit_ratio,
            tick_size=tick_size,
            step_size=step_size,
            min_qty=min_qty,
            min_notional=min_notional,
            role="take_profit_short",
            position_side="SHORT",
        )
        if short_lot_exit_orders and light_short_front_exit_price is not None:
            front_order = short_lot_exit_orders[0]
            target_front_exit_price = light_short_front_exit_price
            if target_front_exit_price >= float(ask_price):
                target_front_exit_price = _round_order_price(
                    max(float(ask_price) - price_tick, 0.0),
                    tick_size,
                    "BUY",
                )
            if 0 < target_front_exit_price < float(ask_price):
                short_lot_exit_orders[0] = _build_order(
                    side=front_order.side,
                    price=target_front_exit_price,
                    qty=front_order.qty,
                    level=front_order.level,
                    role=front_order.role,
                    position_side=front_order.position_side,
                    lot_tracked=front_order.lot_tracked,
                )
        for order in short_lot_exit_orders:
            buy_orders.append(order)

    remaining_short_exit_qty = _round_order_qty(max(effective_short_qty - short_lot_exit_qty, 0.0), step_size)
    for level in range(len(short_lot_exit_orders) + 1, len(short_lot_exit_orders) + buy_levels + 1):
        if remaining_short_exit_qty <= 0:
            break
        fallback_level = level - len(short_lot_exit_orders)
        price = _buy_price(fallback_level)
        desired_qty = _round_order_qty(per_order_notional / price, step_size)
        qty = _round_order_qty(min(desired_qty, remaining_short_exit_qty), step_size)
        notional = price * qty
        if short_exit_profit_guard_active:
            if short_exit_reference_price is not None and price >= short_exit_reference_price:
                continue
        if short_non_loss_exit_price is not None and price > short_non_loss_exit_price:
            continue
        if min_qty is not None and qty < min_qty:
            continue
        if min_notional is not None and notional < min_notional:
            continue
        if qty <= 0:
            continue
        buy_orders.append(
            _build_order(
                side="BUY",
                price=price,
                qty=qty,
                level=level,
                role="take_profit_short",
                position_side="SHORT",
            )
        )
        remaining_short_exit_qty = max(remaining_short_exit_qty - qty, 0.0)

    if not short_lot_exit_orders and light_short_front_exit_price is not None:
        for index, order in enumerate(buy_orders):
            if order.role != "take_profit_short":
                continue
            target_front_exit_price = light_short_front_exit_price
            if target_front_exit_price >= float(ask_price):
                target_front_exit_price = _round_order_price(
                    max(float(ask_price) - price_tick, 0.0),
                    tick_size,
                    "BUY",
                )
            if not (0 < target_front_exit_price < float(ask_price)):
                break
            buy_orders[index] = _build_order(
                side=order.side,
                price=target_front_exit_price,
                qty=order.qty,
                level=order.level,
                role=order.role,
                position_side=order.position_side,
                lot_tracked=order.lot_tracked,
            )
            break

    take_profit_short_price_keys = {
        f"{order.side}:{order.price:.10f}" for order in buy_orders if order.role == "take_profit_short"
    }
    if not bool(entry_long_paused) and (effective_short_qty <= 0 or dominant_long_with_tiny_short_residual):
        entry_buy_max_level = buy_levels + (1 if effective_short_qty > 0 else 0)
        for level in range(1, entry_buy_max_level + 1):
            price = _entry_buy_price(level)
            if effective_short_qty > 0 and f"BUY:{price:.10f}" in take_profit_short_price_keys:
                continue
            if long_entry_cost_guard_active:
                if (
                    light_long_entry_max_price is not None
                    and not held_long_same_side_entry
                    and price > light_long_entry_max_price
                ):
                    continue
                if long_entry_reference_price is not None and price > long_entry_reference_price:
                    continue
            if short_exit_profit_guard_active:
                if short_exit_reference_price is not None and price >= short_exit_reference_price:
                    continue
            qty = _round_order_qty(_entry_notional(level=level, current_qty=effective_long_qty) / price, step_size)
            notional = price * qty
            if price >= ask_price:
                continue
            if min_qty is not None and qty < min_qty:
                continue
            if min_notional is not None and notional < min_notional:
                continue
            if qty <= 0:
                continue
            buy_orders.append(
                _build_order(
                    side="BUY",
                    price=price,
                    qty=qty,
                    level=level,
                    role="entry_long",
                    position_side="LONG",
                )
            )
            if effective_short_qty > 0:
                break

    long_lot_exit_orders: list[PlanOrder] = []
    long_lot_exit_qty = 0.0
    if long_exit_profit_guard_active:
        long_lot_exit_orders, long_lot_exit_qty = _build_lot_take_profit_orders(
            lots=effective_long_lots,
            side="SELL",
            profit_step=profit_step,
            min_profit_ratio=take_profit_min_profit_ratio,
            tick_size=tick_size,
            step_size=step_size,
            min_qty=min_qty,
            min_notional=min_notional,
            role="take_profit_long",
            position_side="LONG",
        )
        for order in long_lot_exit_orders:
            sell_orders.append(order)

    remaining_long_exit_qty = _round_order_qty(max(effective_long_qty - long_lot_exit_qty, 0.0), step_size)
    for level in range(len(long_lot_exit_orders) + 1, len(long_lot_exit_orders) + sell_levels + 1):
        if remaining_long_exit_qty <= 0:
            break
        fallback_level = level - len(long_lot_exit_orders)
        price = _sell_price(fallback_level)
        desired_qty = _round_order_qty(per_order_notional / price, step_size)
        qty = _round_order_qty(min(desired_qty, remaining_long_exit_qty), step_size)
        notional = price * qty
        if long_exit_profit_guard_active:
            if long_exit_reference_price is not None and price <= long_exit_reference_price:
                continue
        if long_non_loss_exit_price is not None and price < long_non_loss_exit_price:
            continue
        if min_qty is not None and qty < min_qty:
            continue
        if min_notional is not None and notional < min_notional:
            continue
        if qty <= 0:
            continue
        sell_orders.append(
            _build_order(
                side="SELL",
                price=price,
                qty=qty,
                level=level,
                role="take_profit_long",
                position_side="LONG",
            )
        )
        remaining_long_exit_qty = max(remaining_long_exit_qty - qty, 0.0)

    take_profit_long_price_keys = {
        f"{order.side}:{order.price:.10f}" for order in sell_orders if order.role == "take_profit_long"
    }
    allow_paused_short_probe = bool(entry_short_paused and paused_entry_short_scale > 0)
    if (not bool(entry_short_paused) or allow_paused_short_probe) and (
        effective_long_qty <= 0 or dominant_short_with_tiny_long_residual
    ):
        entry_sell_max_level = sell_levels + (1 if effective_long_qty > 0 else 0)
        if allow_paused_short_probe and effective_long_qty <= 0:
            entry_sell_max_level = 1
        for level in range(1, entry_sell_max_level + 1):
            price = _paused_probe_sell_price(level) if allow_paused_short_probe else _entry_sell_price(level)
            if effective_long_qty > 0 and f"SELL:{price:.10f}" in take_profit_long_price_keys:
                continue
            if short_entry_cost_guard_active:
                if (
                    light_short_entry_min_price is not None
                    and not held_short_same_side_entry
                    and price < light_short_entry_min_price
                ):
                    continue
                if short_entry_reference_price is not None and price < short_entry_reference_price:
                    continue
            if long_exit_profit_guard_active:
                if long_exit_reference_price is not None and price <= long_exit_reference_price:
                    continue
            entry_notional = _entry_notional(level=level, current_qty=effective_short_qty)
            if allow_paused_short_probe:
                entry_notional *= paused_entry_short_scale
            qty = _round_order_qty(entry_notional / price, step_size)
            notional = price * qty
            if price <= bid_price:
                continue
            if min_qty is not None and qty < min_qty:
                continue
            if min_notional is not None and notional < min_notional:
                continue
            if qty <= 0:
                continue
            sell_orders.append(
                _build_order(
                    side="SELL",
                    price=price,
                    qty=qty,
                    level=level,
                    role="entry_short",
                    position_side="SHORT",
                )
            )
            if allow_paused_short_probe or effective_long_qty > 0:
                break

    target_long_base_qty = _round_order_qty(base_position_notional / center_price, step_size)
    target_short_base_qty = _round_order_qty(base_position_notional / center_price, step_size)
    bootstrap_long_qty = max(target_long_base_qty - effective_long_qty, 0.0)
    bootstrap_short_qty = max(target_short_base_qty - effective_short_qty, 0.0)

    bootstrap_orders: list[PlanOrder] = []
    if bootstrap_long_qty > 0:
        bootstrap_price = _round_order_price(bid_price, tick_size, "BUY")
        bootstrap_qty = _round_order_qty(bootstrap_long_qty, step_size)
        bootstrap_notional = bootstrap_price * bootstrap_qty
        if bootstrap_qty > 0 and (
            (min_qty is None or bootstrap_qty >= min_qty)
            and (min_notional is None or bootstrap_notional >= min_notional)
        ):
            bootstrap_orders.append(
                _build_order(
                    side="BUY",
                    price=bootstrap_price,
                    qty=bootstrap_qty,
                    level=0,
                    role="bootstrap_long",
                    position_side="LONG",
                )
            )
    if bootstrap_short_qty > 0:
        bootstrap_price = _round_order_price(ask_price, tick_size, "SELL")
        bootstrap_qty = _round_order_qty(bootstrap_short_qty, step_size)
        bootstrap_notional = bootstrap_price * bootstrap_qty
        if bootstrap_qty > 0 and (
            (min_qty is None or bootstrap_qty >= min_qty)
            and (min_notional is None or bootstrap_notional >= min_notional)
        ):
            bootstrap_orders.append(
                _build_order(
                    side="SELL",
                    price=bootstrap_price,
                    qty=bootstrap_qty,
                    level=0,
                    role="bootstrap_short",
                    position_side="SHORT",
                )
            )

    return {
        "buy_orders": [order.__dict__ for order in buy_orders],
        "sell_orders": [order.__dict__ for order in sell_orders],
        "bootstrap_orders": [order.__dict__ for order in bootstrap_orders],
        "target_long_base_qty": target_long_base_qty,
        "target_short_base_qty": target_short_base_qty,
        "bootstrap_long_qty": bootstrap_long_qty,
        "bootstrap_short_qty": bootstrap_short_qty,
        "effective_long_qty": effective_long_qty,
        "effective_short_qty": effective_short_qty,
        "residual_long_flat_notional": residual_long_flat_notional,
        "residual_long_flattened": residual_long_flattened,
        "residual_long_flattened_qty": residual_long_flattened_qty,
        "residual_long_flattened_notional": residual_long_flattened_notional,
        "residual_short_flat_notional": residual_short_flat_notional,
        "residual_short_flattened": residual_short_flattened,
        "residual_short_flattened_qty": residual_short_flattened_qty,
        "residual_short_flattened_notional": residual_short_flattened_notional,
        "synthetic_tiny_long_residual_notional": tiny_long_residual_threshold_notional,
        "synthetic_tiny_short_residual_notional": tiny_short_residual_threshold_notional,
        "dominant_long_with_tiny_short_residual": dominant_long_with_tiny_short_residual,
        "dominant_short_with_tiny_long_residual": dominant_short_with_tiny_long_residual,
        "long_entry_reference_price": long_entry_reference_price,
        "long_exit_reference_price": long_exit_reference_price,
        "short_entry_reference_price": short_entry_reference_price,
        "short_exit_reference_price": short_exit_reference_price,
        "lowest_long_lot_price": lowest_long_lot_price,
        "highest_long_lot_price": highest_long_lot_price,
        "lowest_short_lot_price": lowest_short_lot_price,
        "highest_short_lot_price": highest_short_lot_price,
    }


def _resolve_inventory_target_notional(
    *,
    center_price: float,
    mid_price: float,
    band_offsets: list[float],
    band_target_ratios: list[float],
    max_long_notional: float,
    max_short_notional: float,
) -> float:
    if center_price <= 0 or mid_price <= 0:
        return 0.0
    offset_ratio = (mid_price / center_price) - 1.0
    if offset_ratio <= 0:
        for band_offset, target_ratio in reversed(list(zip(band_offsets, band_target_ratios, strict=True))):
            if offset_ratio <= -band_offset:
                return max_long_notional * target_ratio
        return 0.0
    for band_offset, target_ratio in reversed(list(zip(band_offsets, band_target_ratios, strict=True))):
        if offset_ratio >= band_offset:
            return -max_short_notional * target_ratio
    return 0.0


def build_inventory_target_neutral_plan(
    *,
    center_price: float,
    mid_price: float,
    band_offsets: list[float],
    band_target_ratios: list[float],
    max_long_notional: float,
    max_short_notional: float,
    bid_price: float,
    ask_price: float,
    tick_size: float | None,
    step_size: float | None,
    min_qty: float | None,
    min_notional: float | None,
    current_net_qty: float,
) -> dict[str, Any]:
    if center_price <= 0 or mid_price <= 0:
        raise ValueError("center_price and mid_price must be > 0")
    if len(band_offsets) != len(band_target_ratios):
        raise ValueError("band_offsets and band_target_ratios must have the same length")
    if not band_offsets:
        raise ValueError("need at least one inventory target band")
    if any(offset <= 0 for offset in band_offsets):
        raise ValueError("band_offsets must be > 0")
    if any(ratio < 0 or ratio > 1 for ratio in band_target_ratios):
        raise ValueError("band_target_ratios must be between 0 and 1")
    if max_long_notional <= 0 or max_short_notional <= 0:
        raise ValueError("max_long_notional and max_short_notional must be > 0")

    pairs = sorted(zip(band_offsets, band_target_ratios, strict=True), key=lambda item: item[0])
    sorted_offsets = [item[0] for item in pairs]
    sorted_ratios = [item[1] for item in pairs]

    current_net_notional = float(current_net_qty) * float(mid_price)
    target_now_notional = _resolve_inventory_target_notional(
        center_price=center_price,
        mid_price=mid_price,
        band_offsets=sorted_offsets,
        band_target_ratios=sorted_ratios,
        max_long_notional=max_long_notional,
        max_short_notional=max_short_notional,
    )

    bootstrap_orders: list[PlanOrder] = []
    adjusted_net_notional = current_net_notional
    if target_now_notional > current_net_notional + 1e-12:
        bootstrap_price = _round_order_price(bid_price, tick_size, "BUY")
        bootstrap_qty = _round_order_qty((target_now_notional - current_net_notional) / bootstrap_price, step_size)
        bootstrap_notional = bootstrap_qty * bootstrap_price
        if bootstrap_price < ask_price and bootstrap_qty > 0 and (
            (min_qty is None or bootstrap_qty >= min_qty)
            and (min_notional is None or bootstrap_notional >= min_notional)
        ):
            bootstrap_orders.append(
                _build_order(
                    side="BUY",
                    price=bootstrap_price,
                    qty=bootstrap_qty,
                    level=0,
                    role="bootstrap_long",
                )
            )
            adjusted_net_notional += bootstrap_notional
    elif target_now_notional < current_net_notional - 1e-12:
        bootstrap_price = _round_order_price(ask_price, tick_size, "SELL")
        bootstrap_qty = _round_order_qty((current_net_notional - target_now_notional) / bootstrap_price, step_size)
        bootstrap_notional = bootstrap_qty * bootstrap_price
        if bootstrap_price > bid_price and bootstrap_qty > 0 and (
            (min_qty is None or bootstrap_qty >= min_qty)
            and (min_notional is None or bootstrap_notional >= min_notional)
        ):
            bootstrap_orders.append(
                _build_order(
                    side="SELL",
                    price=bootstrap_price,
                    qty=bootstrap_qty,
                    level=0,
                    role="bootstrap_short",
                )
            )
            adjusted_net_notional -= bootstrap_notional

    buy_orders: list[PlanOrder] = []
    running_buy_net_notional = adjusted_net_notional
    for level, (band_offset, target_ratio) in enumerate(zip(sorted_offsets, sorted_ratios, strict=True), start=1):
        target_notional = max_long_notional * target_ratio
        if target_notional <= running_buy_net_notional + 1e-12:
            continue
        price_raw = center_price * (1.0 - band_offset)
        price = _round_order_price(price_raw, tick_size, "BUY")
        qty = _round_order_qty((target_notional - running_buy_net_notional) / price, step_size)
        notional = price * qty
        if price >= ask_price:
            continue
        if qty <= 0:
            continue
        if min_qty is not None and qty < min_qty:
            continue
        if min_notional is not None and notional < min_notional:
            continue
        buy_orders.append(
            _build_order(
                side="BUY",
                price=price,
                qty=qty,
                level=level,
                role="entry_long",
            )
        )
        running_buy_net_notional += notional

    sell_orders: list[PlanOrder] = []
    running_sell_net_notional = adjusted_net_notional
    for level, (band_offset, target_ratio) in enumerate(zip(sorted_offsets, sorted_ratios, strict=True), start=1):
        target_notional = -(max_short_notional * target_ratio)
        if running_sell_net_notional <= target_notional + 1e-12:
            continue
        price_raw = center_price * (1.0 + band_offset)
        price = _round_order_price(price_raw, tick_size, "SELL")
        qty = _round_order_qty((running_sell_net_notional - target_notional) / price, step_size)
        notional = price * qty
        if price <= bid_price:
            continue
        if qty <= 0:
            continue
        if min_qty is not None and qty < min_qty:
            continue
        if min_notional is not None and notional < min_notional:
            continue
        sell_orders.append(
            _build_order(
                side="SELL",
                price=price,
                qty=qty,
                level=level,
                role="entry_short",
            )
        )
        running_sell_net_notional -= notional

    return {
        "buy_orders": [order.__dict__ for order in buy_orders],
        "sell_orders": [order.__dict__ for order in sell_orders],
        "bootstrap_orders": [order.__dict__ for order in bootstrap_orders],
        "current_target_notional": target_now_notional,
        "current_net_notional": current_net_notional,
        "band_offsets": sorted_offsets,
        "band_target_ratios": sorted_ratios,
    }


def diff_open_orders(
    *,
    existing_orders: list[dict[str, Any]],
    desired_orders: list[dict[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    desired_by_bucket: dict[str, list[dict[str, Any]]] = {}
    for order in desired_orders:
        key = _order_bucket_key(
            str(order["side"]),
            float(order["price"]),
            str(order.get("position_side", order.get("positionSide", "BOTH"))),
        )
        desired_by_bucket.setdefault(key, []).append(order)

    existing_limit_by_bucket: dict[str, list[dict[str, Any]]] = {}
    stale_orders: list[dict[str, Any]] = []
    kept_orders: list[dict[str, Any]] = []
    missing_orders: list[dict[str, Any]] = []

    for order in existing_orders:
        side = str(order.get("side", "")).upper()
        order_type = str(order.get("type", "")).upper()
        price = _safe_float(order.get("price"))
        qty = _safe_float(order.get("origQty", order.get("qty")))
        if side not in {"BUY", "SELL"} or price <= 0 or qty <= 0:
            continue
        if order_type and order_type not in {"LIMIT", "LIMIT_MAKER"}:
            continue
        key = _order_bucket_key(side, price, str(order.get("positionSide", "BOTH")))
        existing_limit_by_bucket.setdefault(key, []).append(order)

    all_keys = set(existing_limit_by_bucket) | set(desired_by_bucket)
    for key in all_keys:
        existing_group = existing_limit_by_bucket.get(key, [])
        desired_group = desired_by_bucket.get(key, [])
        if not desired_group:
            stale_orders.extend(existing_group)
            continue
        if not existing_group:
            missing_orders.extend(desired_group)
            continue

        existing_total = sum(
            (_quantity_decimal(order.get("origQty", order.get("qty"))) for order in existing_group),
            Decimal("0"),
        )
        desired_total = sum(
            (_quantity_decimal(order.get("qty", order.get("quantity"))) for order in desired_group),
            Decimal("0"),
        )

        if existing_total > desired_total:
            stale_orders.extend(existing_group)
            missing_orders.extend(desired_group)
            continue

        kept_orders.extend(existing_group)
        delta = desired_total - existing_total
        if delta > Decimal("0"):
            missing_orders.append(_clone_order_with_qty(desired_group[0], delta))

    return {
        "missing_orders": missing_orders,
        "stale_orders": stale_orders,
        "kept_orders": kept_orders,
    }


def preserve_sticky_entry_orders(
    *,
    existing_orders: list[dict[str, Any]],
    desired_orders: list[dict[str, Any]],
    price_tolerance: float,
    max_levels_per_group: int | None = None,
) -> list[dict[str, Any]]:
    tolerance = max(_safe_float(price_tolerance), 0.0)
    safe_max_levels = None if max_levels_per_group is None else max(int(max_levels_per_group), 0)
    if tolerance <= 0 or safe_max_levels == 0:
        return [dict(order) for order in desired_orders]

    adjusted_orders = [dict(order) for order in desired_orders]
    entry_roles = {"entry_long", "entry_short"}
    take_profit_roles = {"take_profit_long", "take_profit_short"}
    sticky_roles = entry_roles | take_profit_roles

    def _position_side(order: dict[str, Any]) -> str:
        raw = str(order.get("position_side", order.get("positionSide", "BOTH"))).upper().strip()
        return raw or "BOTH"

    def _price(order: dict[str, Any]) -> float:
        return _safe_float(order.get("price"))

    def _qty(order: dict[str, Any]) -> float:
        return _safe_float(order.get("qty", order.get("quantity", order.get("origQty"))))

    def _sort_key(order: dict[str, Any]) -> tuple[float, float]:
        side = str(order.get("side", "")).upper().strip()
        price = _price(order)
        if side == "BUY":
            return (-price, price)
        return (price, price)

    def _existing_is_closer(existing_order: dict[str, Any], desired_order: dict[str, Any]) -> bool:
        side = str(desired_order.get("side", "")).upper().strip()
        existing_price = _price(existing_order)
        desired_price = _price(desired_order)
        if side == "BUY":
            return existing_price > desired_price + 1e-12
        if side == "SELL":
            return existing_price + 1e-12 < desired_price
        return False

    occupied_buckets = {
        _order_bucket_key(str(order.get("side", "")), _price(order), _position_side(order))
        for order in adjusted_orders
        if str(order.get("role", "")).strip() in take_profit_roles and _price(order) > 0
    }

    desired_groups: dict[tuple[str, str, str], list[tuple[int, dict[str, Any]]]] = {}
    for index, order in enumerate(adjusted_orders):
        role = str(order.get("role", "")).strip()
        if role not in sticky_roles:
            continue
        side = str(order.get("side", "")).upper().strip()
        if side not in {"BUY", "SELL"}:
            continue
        key = (role, side, _position_side(order))
        desired_groups.setdefault(key, []).append((index, order))

    existing_groups: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for order in existing_orders:
        role = str(order.get("role", "")).strip()
        if role not in sticky_roles:
            continue
        side = str(order.get("side", "")).upper().strip()
        if side not in {"BUY", "SELL"}:
            continue
        if _price(order) <= 0 or _qty(order) <= 0:
            continue
        key = (role, side, _position_side(order))
        existing_groups.setdefault(key, []).append(order)

    for key, desired_group in desired_groups.items():
        existing_group = existing_groups.get(key, [])
        if not existing_group:
            continue
        desired_sorted = sorted(desired_group, key=lambda item: _sort_key(item[1]))
        existing_sorted = sorted(existing_group, key=_sort_key)
        if safe_max_levels is not None:
            desired_sorted = desired_sorted[:safe_max_levels]
            existing_sorted = existing_sorted[:safe_max_levels]
        for (index, desired_order), existing_order in zip(desired_sorted, existing_sorted):
            desired_price = _price(desired_order)
            existing_price = _price(existing_order)
            if desired_price <= 0 or existing_price <= 0:
                continue
            role = str(desired_order.get("role", "")).strip()
            if role in entry_roles and abs(existing_price - desired_price) > tolerance + 1e-12:
                continue
            if not _existing_is_closer(existing_order, desired_order):
                continue
            bucket = _order_bucket_key(
                str(desired_order.get("side", "")),
                existing_price,
                _position_side(desired_order),
            )
            if bucket in occupied_buckets:
                continue
            adjusted_order = dict(adjusted_orders[index])
            qty = _qty(adjusted_order)
            adjusted_order["price"] = existing_price
            if "notional" in adjusted_order:
                adjusted_order["notional"] = existing_price * qty
            adjusted_orders[index] = adjusted_order
            occupied_buckets.add(bucket)

    return adjusted_orders


def preserve_sticky_exit_orders(
    *,
    existing_orders: list[dict[str, Any]],
    desired_orders: list[dict[str, Any]],
    sticky_roles: set[str] | None = None,
) -> list[dict[str, Any]]:
    normalized_roles = {
        str(role).strip()
        for role in (sticky_roles or set())
        if str(role).strip()
    }
    if not normalized_roles:
        return [dict(order) for order in desired_orders]

    adjusted_orders = [dict(order) for order in desired_orders]

    def _position_side(order: dict[str, Any]) -> str:
        raw = str(order.get("position_side", order.get("positionSide", "BOTH"))).upper().strip()
        return raw or "BOTH"

    def _price(order: dict[str, Any]) -> float:
        return _safe_float(order.get("price"))

    def _qty(order: dict[str, Any]) -> float:
        return _safe_float(order.get("qty", order.get("quantity", order.get("origQty"))))

    def _sort_key(order: dict[str, Any]) -> tuple[float, float]:
        side = str(order.get("side", "")).upper().strip()
        price = _price(order)
        if side == "BUY":
            return (-price, price)
        return (price, price)

    desired_groups: dict[tuple[str, str, str], list[tuple[int, dict[str, Any]]]] = {}
    for index, order in enumerate(adjusted_orders):
        role = str(order.get("role", "")).strip()
        if role not in normalized_roles:
            continue
        side = str(order.get("side", "")).upper().strip()
        if side not in {"BUY", "SELL"}:
            continue
        key = (role, side, _position_side(order))
        desired_groups.setdefault(key, []).append((index, order))

    existing_groups: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for order in existing_orders:
        role = str(order.get("role", "")).strip()
        if role not in normalized_roles:
            continue
        side = str(order.get("side", "")).upper().strip()
        if side not in {"BUY", "SELL"}:
            continue
        if _price(order) <= 0 or _qty(order) <= 0:
            continue
        key = (role, side, _position_side(order))
        existing_groups.setdefault(key, []).append(order)

    for key, desired_group in desired_groups.items():
        existing_group = existing_groups.get(key, [])
        if not existing_group:
            continue
        desired_sorted = sorted(desired_group, key=lambda item: _sort_key(item[1]))
        existing_sorted = sorted(existing_group, key=_sort_key)
        for (index, _desired_order), existing_order in zip(desired_sorted, existing_sorted):
            existing_price = _price(existing_order)
            if existing_price <= 0:
                continue
            adjusted_order = dict(adjusted_orders[index])
            qty = _qty(adjusted_order)
            adjusted_order["price"] = existing_price
            if "notional" in adjusted_order:
                adjusted_order["notional"] = existing_price * qty
            adjusted_orders[index] = adjusted_order

    return adjusted_orders


def _config_payload(args: argparse.Namespace, symbol_info: dict[str, Any]) -> dict[str, Any]:
    config = {
        "symbol": args.symbol.upper().strip(),
        "strategy_mode": getattr(args, "strategy_mode", "one_way_long"),
        "step_price": args.step_price,
        "buy_levels": args.buy_levels,
        "sell_levels": args.sell_levels,
        "per_order_notional": args.per_order_notional,
        "base_position_notional": args.base_position_notional,
        "center_price": getattr(args, "center_price", None),
        "static_buy_offset_steps": getattr(args, "static_buy_offset_steps", 0.0),
        "static_sell_offset_steps": getattr(args, "static_sell_offset_steps", 0.0),
        "down_trigger_steps": args.down_trigger_steps,
        "up_trigger_steps": args.up_trigger_steps,
        "shift_steps": args.shift_steps,
        "tick_size": symbol_info.get("tick_size"),
        "step_size": symbol_info.get("step_size"),
        "min_qty": symbol_info.get("min_qty"),
        "min_notional": symbol_info.get("min_notional"),
    }
    optional_fields = (
        "flat_start_enabled",
        "warm_start_enabled",
        "fixed_center_enabled",
        "custom_grid_enabled",
        "custom_grid_direction",
        "custom_grid_level_mode",
        "custom_grid_min_price",
        "custom_grid_max_price",
        "custom_grid_n",
        "custom_grid_total_notional",
        "custom_grid_neutral_anchor_price",
        "custom_grid_roll_enabled",
        "custom_grid_roll_interval_minutes",
        "custom_grid_roll_trade_threshold",
        "custom_grid_roll_upper_distance_ratio",
        "custom_grid_roll_shift_levels",
        "neutral_center_interval_minutes",
        "neutral_band1_offset_ratio",
        "neutral_band2_offset_ratio",
        "neutral_band3_offset_ratio",
        "neutral_band1_target_ratio",
        "neutral_band2_target_ratio",
        "neutral_band3_target_ratio",
        "neutral_hourly_scale_enabled",
        "neutral_hourly_scale_stable",
        "neutral_hourly_scale_transition",
        "neutral_hourly_scale_defensive",
        "max_position_notional",
        "max_short_position_notional",
        "adaptive_step_enabled",
        "adaptive_step_30s_abs_return_ratio",
        "adaptive_step_30s_amplitude_ratio",
        "adaptive_step_1m_abs_return_ratio",
        "adaptive_step_1m_amplitude_ratio",
        "adaptive_step_3m_abs_return_ratio",
        "adaptive_step_5m_abs_return_ratio",
        "adaptive_step_max_scale",
        "adaptive_step_min_per_order_scale",
        "adaptive_step_min_position_limit_scale",
    )
    for key in optional_fields:
        if hasattr(args, key):
            config[key] = getattr(args, key)
    return config


def _config_signature(config: dict[str, Any]) -> str:
    return json.dumps(config, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def load_or_initialize_state(
    *,
    state_path: Path,
    args: argparse.Namespace,
    symbol_info: dict[str, Any],
    mid_price: float,
    reset_state: bool,
) -> dict[str, Any]:
    config = _config_payload(args, symbol_info)
    if state_path.exists() and not reset_state:
        state = json.loads(state_path.read_text(encoding="utf-8"))
        if str(state.get("config_signature", "")).strip() != _config_signature(config):
            raise SystemExit("State config does not match current CLI arguments. Use --reset-state to restart.")
        return state

    center_price = args.center_price if args.center_price is not None else mid_price
    center_price = _round_to_nearest_step(center_price, symbol_info.get("tick_size"))
    state = {
        "version": STATE_VERSION,
        "created_at": _isoformat(_utc_now()),
        "updated_at": _isoformat(_utc_now()),
        "startup_pending": True,
        "startup_completed_at": None,
        "config": config,
        "config_signature": _config_signature(config),
        "center_price": center_price,
        "last_mid_price": mid_price,
    }
    if getattr(args, "custom_grid_enabled", False):
        state.update(
            {
                "custom_grid_runtime_min_price": getattr(args, "custom_grid_min_price", None),
                "custom_grid_runtime_max_price": getattr(args, "custom_grid_max_price", None),
                "custom_grid_roll_last_check_bucket": None,
                "custom_grid_roll_trade_baseline": 0,
                "custom_grid_roll_trades_since_last_roll": 0,
                "custom_grid_roll_last_applied_at": None,
                "custom_grid_roll_last_applied_price": None,
                "custom_grid_roll_last_old_min_price": None,
                "custom_grid_roll_last_old_max_price": None,
                "custom_grid_roll_last_new_min_price": None,
                "custom_grid_roll_last_new_max_price": None,
            }
        )
    return state


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate a semi-automatic maker-only micro-grid order plan without sending real orders."
    )
    parser.add_argument("--symbol", type=str, default="NIGHTUSDT")
    parser.add_argument("--step-price", type=float, default=0.00002)
    parser.add_argument("--buy-levels", type=int, default=12)
    parser.add_argument("--sell-levels", type=int, default=12)
    parser.add_argument("--per-order-notional", type=float, default=25.25)
    parser.add_argument("--base-position-notional", type=float, default=151.5)
    parser.add_argument("--center-price", type=float, default=None)
    parser.add_argument("--down-trigger-steps", type=int, default=4)
    parser.add_argument("--up-trigger-steps", type=int, default=8)
    parser.add_argument("--shift-steps", type=int, default=4)
    parser.add_argument("--recv-window", type=int, default=5000)
    parser.add_argument("--state-path", type=str, default="output/night_semi_auto_state.json")
    parser.add_argument("--report-json", type=str, default="output/night_semi_auto_plan.json")
    parser.add_argument("--reset-state", action="store_true")
    return parser


def _print_plan(report: dict[str, Any]) -> None:
    print("=== Semi Auto Plan ===")
    print(f"Symbol: {report['symbol']}")
    print(
        f"Bid/Ask/Mid: {_price(report['bid_price'])} / {_price(report['ask_price'])} / {_price(report['mid_price'])}"
    )
    print(f"Center price: {_price(report['center_price'])}")
    print(f"Step price: {_price(report['step_price'])}")
    if report["funding_rate"] is not None:
        print(f"Funding rate: {_pct(report['funding_rate'])}")
    if report["shift_moves"]:
        print("Shift moves:")
        for item in report["shift_moves"]:
            print(f"  - {item['direction']} -> {_float(item['new_center_price'])}")
    print(
        f"Current long qty / target base qty / bootstrap qty: "
        f"{_float(report['current_long_qty'])} / {_float(report['target_base_qty'])} / {_float(report['bootstrap_qty'])}"
    )
    print(
        f"Existing open orders / keep / add / cancel: "
        f"{report['open_order_count']} / {len(report['kept_orders'])} / {len(report['missing_orders'])} / {len(report['stale_orders'])}"
    )
    if report["bootstrap_orders"]:
        print("Bootstrap orders:")
        for order in report["bootstrap_orders"]:
            print(
                f"  {order['side']} qty={order['qty']:.4f} price={_price(order['price'])} notional={order['notional']:.4f}"
            )
    print("Orders to add:")
    for order in report["missing_orders"]:
        print(
            f"  {order['role']} {order['side']} qty={order['qty']:.4f} price={_price(order['price'])} notional={order['notional']:.4f}"
        )
    if report["stale_orders"]:
        print("Orders to cancel:")
        for order in report["stale_orders"]:
            print(
                f"  {str(order.get('side', '')).upper()} qty={_safe_float(order.get('origQty', order.get('qty'))):.4f} "
                f"price={_price(_safe_float(order.get('price')))}"
            )
    print("")


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    if args.step_price <= 0:
        raise SystemExit("--step-price must be > 0")
    if args.buy_levels < 0 or args.sell_levels < 0:
        raise SystemExit("--buy-levels and --sell-levels must be >= 0")
    if args.per_order_notional <= 0:
        raise SystemExit("--per-order-notional must be > 0")
    if args.base_position_notional < 0:
        raise SystemExit("--base-position-notional must be >= 0")

    symbol = args.symbol.upper().strip()
    symbol_info = fetch_futures_symbol_config(symbol)
    book_rows = fetch_futures_book_tickers(symbol=symbol)
    premium_rows = fetch_futures_premium_index(symbol=symbol)
    if not book_rows:
        raise SystemExit(f"No book ticker returned for {symbol}")
    if not premium_rows:
        raise SystemExit(f"No premium index returned for {symbol}")
    book = book_rows[0]
    premium = premium_rows[0]
    bid_price = float(book["bid_price"])
    ask_price = float(book["ask_price"])
    mid_price = (bid_price + ask_price) / 2.0

    state_path = Path(args.state_path)
    state = load_or_initialize_state(
        state_path=state_path,
        args=args,
        symbol_info=symbol_info,
        mid_price=mid_price,
        reset_state=args.reset_state,
    )
    center_price, shift_moves = shift_center_price(
        center_price=float(state["center_price"]),
        mid_price=mid_price,
        step_price=args.step_price,
        tick_size=symbol_info.get("tick_size"),
        down_trigger_steps=args.down_trigger_steps,
        up_trigger_steps=args.up_trigger_steps,
        shift_steps=args.shift_steps,
    )

    credentials = load_binance_api_credentials()
    account_info: dict[str, Any] | None = None
    open_orders: list[dict[str, Any]] = []
    current_long_qty = 0.0
    dual_side_position = None
    if credentials is not None:
        api_key, api_secret = credentials
        account_info = fetch_futures_account_info_v3(api_key, api_secret, recv_window=args.recv_window)
        position_mode = fetch_futures_position_mode(api_key, api_secret, recv_window=args.recv_window)
        open_orders = fetch_futures_open_orders(symbol, api_key, api_secret, recv_window=args.recv_window)
        symbol_position = extract_symbol_position(account_info, symbol)
        position_amt = _safe_float(symbol_position.get("positionAmt"))
        current_long_qty = max(position_amt, 0.0)
        dual_side_position = _truthy(position_mode.get("dualSidePosition"))

    plan = build_micro_grid_plan(
        center_price=center_price,
        step_price=args.step_price,
        buy_levels=args.buy_levels,
        sell_levels=args.sell_levels,
        per_order_notional=args.per_order_notional,
        base_position_notional=args.base_position_notional,
        bid_price=bid_price,
        ask_price=ask_price,
        tick_size=symbol_info.get("tick_size"),
        step_size=symbol_info.get("step_size"),
        min_qty=symbol_info.get("min_qty"),
        min_notional=symbol_info.get("min_notional"),
        current_long_qty=current_long_qty,
    )

    desired_orders = [
        *plan["buy_orders"],
        *plan["sell_orders"],
    ]
    diff = diff_open_orders(existing_orders=open_orders, desired_orders=desired_orders)

    state["center_price"] = center_price
    state["last_mid_price"] = mid_price
    state["updated_at"] = _isoformat(_utc_now())
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")

    report = {
        "generated_at": _isoformat(_utc_now()),
        "symbol": symbol,
        "bid_price": bid_price,
        "ask_price": ask_price,
        "mid_price": mid_price,
        "funding_rate": _safe_float(premium.get("funding_rate")) if premium.get("funding_rate") is not None else None,
        "center_price": center_price,
        "step_price": args.step_price,
        "shift_moves": shift_moves,
        "current_long_qty": current_long_qty,
        "target_base_qty": plan["target_base_qty"],
        "bootstrap_qty": plan["bootstrap_qty"],
        "bootstrap_orders": plan["bootstrap_orders"],
        "buy_orders": plan["buy_orders"],
        "sell_orders": plan["sell_orders"],
        "open_order_count": len(open_orders),
        "kept_orders": diff["kept_orders"],
        "missing_orders": diff["missing_orders"],
        "stale_orders": diff["stale_orders"],
        "state_path": str(state_path),
        "symbol_info": symbol_info,
        "account_connected": credentials is not None,
        "dual_side_position": dual_side_position,
    }

    report_path = Path(args.report_json)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    _print_plan(report)
    print(f"JSON report saved: {report_path}")


if __name__ == "__main__":
    main()
