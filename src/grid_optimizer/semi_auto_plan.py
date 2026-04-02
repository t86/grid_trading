from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP
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
    shift_size = step_price * shift_steps

    while mid_price <= center - down_trigger:
        center = _round_to_nearest_step(center - shift_size, tick_size)
        moves.append({"direction": "down", "new_center_price": center})
    while mid_price >= center + up_trigger:
        center = _round_to_nearest_step(center + shift_size, tick_size)
        moves.append({"direction": "up", "new_center_price": center})
    return center, moves


def _build_order(
    *,
    side: str,
    price: float,
    qty: float,
    level: int,
    role: str,
    position_side: str = "BOTH",
) -> PlanOrder:
    return PlanOrder(
        side=side.upper(),
        price=float(price),
        qty=float(qty),
        notional=float(price) * float(qty),
        level=level,
        role=role,
        position_side=str(position_side or "BOTH").upper().strip() or "BOTH",
    )


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

    remaining_short_exit_qty = _round_order_qty(current_short_qty, step_size)
    for level in range(1, buy_levels + 1):
        if remaining_short_exit_qty <= 0:
            break
        price_raw = center_price - (level * step_price)
        price = _round_order_price(price_raw, tick_size, "BUY")
        desired_qty = _round_order_qty(per_order_notional / price, step_size)
        qty = _round_order_qty(min(desired_qty, remaining_short_exit_qty), step_size)
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
                role="take_profit_short",
                position_side="SHORT",
            )
        )
        remaining_short_exit_qty = max(remaining_short_exit_qty - qty, 0.0)

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

    remaining_long_exit_qty = _round_order_qty(current_long_qty, step_size)
    for level in range(1, sell_levels + 1):
        if remaining_long_exit_qty <= 0:
            break
        price_raw = center_price + (level * step_price)
        price = _round_order_price(price_raw, tick_size, "SELL")
        desired_qty = _round_order_qty(per_order_notional / price, step_size)
        qty = _round_order_qty(min(desired_qty, remaining_long_exit_qty), step_size)
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
                role="take_profit_long",
                position_side="LONG",
            )
        )
        remaining_long_exit_qty = max(remaining_long_exit_qty - qty, 0.0)

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

    target_long_base_qty = _round_order_qty(base_position_notional / center_price, step_size)
    target_short_base_qty = _round_order_qty(base_position_notional / center_price, step_size)
    bootstrap_long_qty = max(target_long_base_qty - current_long_qty, 0.0)
    bootstrap_short_qty = max(target_short_base_qty - current_short_qty, 0.0)

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
