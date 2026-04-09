from __future__ import annotations

from typing import Any

from .dry_run import _round_order_price, _round_order_qty
from .inventory_grid_state import build_forced_reduce_lot_plan

EPSILON = 1e-12


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _build_order(
    *,
    side: str,
    price: float,
    qty: float,
    role: str,
    position_side: str | None = None,
) -> dict[str, Any]:
    order = {
        "side": side.upper(),
        "price": float(price),
        "qty": float(qty),
        "notional": float(price) * float(qty),
        "role": role,
    }
    if position_side:
        order["position_side"] = position_side.upper()
    return order


def _position_notional(*, runtime: dict[str, Any], mid_price: float) -> float:
    total_qty = sum(max(_safe_float(item.get("qty")), 0.0) for item in list(runtime.get("position_lots") or []))
    return total_qty * max(mid_price, 0.0)


def _held_qty(*, runtime: dict[str, Any]) -> float:
    return sum(max(_safe_float(item.get("qty")), 0.0) for item in list(runtime.get("position_lots") or []))


def _bootstrap_orders(
    *,
    market_type: str,
    bid_price: float,
    ask_price: float,
    bootstrap_notional: float,
    tick_size: float | None,
    step_size: float | None,
    min_qty: float | None,
    min_notional: float | None,
) -> list[dict[str, Any]]:
    orders: list[dict[str, Any]] = []

    buy_price = _round_order_price(bid_price, tick_size, "BUY")
    buy_qty = _round_order_qty(bootstrap_notional / max(buy_price, EPSILON), step_size)
    buy_notional = buy_price * buy_qty
    if buy_qty > 0 and (min_qty is None or buy_qty >= min_qty) and (min_notional is None or buy_notional >= min_notional):
        orders.append(
            _build_order(
                side="BUY",
                price=buy_price,
                qty=buy_qty,
                role="bootstrap_entry",
                position_side="LONG" if market_type == "futures" else None,
            )
        )

    if market_type == "futures":
        sell_price = _round_order_price(ask_price, tick_size, "SELL")
        sell_qty = _round_order_qty(bootstrap_notional / max(sell_price, EPSILON), step_size)
        sell_notional = sell_price * sell_qty
        if sell_qty > 0 and (min_qty is None or sell_qty >= min_qty) and (min_notional is None or sell_notional >= min_notional):
            orders.append(
                _build_order(
                    side="SELL",
                    price=sell_price,
                    qty=sell_qty,
                    role="bootstrap_entry",
                    position_side="SHORT",
                )
            )

    return orders


def build_inventory_grid_orders(
    *,
    runtime: dict[str, Any],
    bid_price: float,
    ask_price: float,
    step_price: float,
    per_order_notional: float,
    first_order_multiplier: float,
    threshold_position_notional: float,
    max_order_position_notional: float,
    max_position_notional: float,
    tick_size: float | None,
    step_size: float | None,
    min_qty: float | None,
    min_notional: float | None,
) -> dict[str, Any]:
    market_type = str(runtime.get("market_type", "futures")).strip().lower()
    direction_state = str(runtime.get("direction_state", "flat")).strip().lower()
    bootstrap_notional = max(_safe_float(per_order_notional), 0.0) * max(_safe_float(first_order_multiplier), 0.0)
    mid_price = (max(_safe_float(bid_price), 0.0) + max(_safe_float(ask_price), 0.0)) / 2.0

    bootstrap_orders: list[dict[str, Any]] = []
    buy_orders: list[dict[str, Any]] = []
    sell_orders: list[dict[str, Any]] = []
    forced_reduce_orders: list[dict[str, Any]] = []

    if direction_state == "flat":
        bootstrap_orders = _bootstrap_orders(
            market_type=market_type,
            bid_price=bid_price,
            ask_price=ask_price,
            bootstrap_notional=bootstrap_notional,
            tick_size=tick_size,
            step_size=step_size,
            min_qty=min_qty,
            min_notional=min_notional,
        )
        return {
            "risk_state": "normal",
            "bootstrap_orders": bootstrap_orders,
            "buy_orders": buy_orders,
            "sell_orders": sell_orders,
            "forced_reduce_orders": forced_reduce_orders,
            "tail_cleanup_active": False,
        }

    inventory_notional = _position_notional(runtime=runtime, mid_price=mid_price)
    held_qty = _held_qty(runtime=runtime)
    one_order_qty = _round_order_qty(max(_safe_float(per_order_notional), 0.0) / max(mid_price, EPSILON), step_size)
    if max_position_notional > 0 and inventory_notional >= max_position_notional:
        risk_state = "hard_reduce_only"
    elif threshold_position_notional > 0 and inventory_notional >= threshold_position_notional:
        risk_state = "threshold_reduce_only"
    else:
        risk_state = "normal"

    tail_cleanup_active = risk_state == "normal" and 0.0 < held_qty < max(one_order_qty, EPSILON)

    if direction_state == "long_active":
        if tail_cleanup_active:
            cleanup_qty = _round_order_qty(held_qty, step_size)
            cleanup_price = _round_order_price(ask_price, tick_size, "SELL")
            if cleanup_qty > 0:
                sell_orders.append(
                    _build_order(
                        side="SELL",
                        price=cleanup_price,
                        qty=cleanup_qty,
                        role="tail_cleanup",
                    )
                )
            return {
                "risk_state": risk_state,
                "bootstrap_orders": bootstrap_orders,
                "buy_orders": buy_orders,
                "sell_orders": sell_orders,
                "forced_reduce_orders": forced_reduce_orders,
                "tail_cleanup_active": True,
            }

        exit_price = _round_order_price(ask_price, tick_size, "SELL")
        exit_qty = _round_order_qty(max(_safe_float(per_order_notional), 0.0) / max(exit_price, EPSILON), step_size)
        if exit_qty > 0 and (min_qty is None or exit_qty >= min_qty) and (min_notional is None or exit_qty * exit_price >= min_notional):
            sell_orders.append(_build_order(side="SELL", price=exit_price, qty=exit_qty, role="grid_exit"))

        entry_allowed = risk_state == "normal" and (
            max_order_position_notional <= 0 or inventory_notional + max(_safe_float(per_order_notional), 0.0) <= max_order_position_notional
        )
        if entry_allowed:
            entry_price = _round_order_price(max(_safe_float(bid_price) - max(_safe_float(step_price), 0.0), 0.0), tick_size, "BUY")
            entry_qty = _round_order_qty(max(_safe_float(per_order_notional), 0.0) / max(entry_price, EPSILON), step_size)
            if entry_qty > 0 and (min_qty is None or entry_qty >= min_qty) and (
                min_notional is None or entry_qty * entry_price >= min_notional
            ):
                buy_orders.append(_build_order(side="BUY", price=entry_price, qty=entry_qty, role="grid_entry"))

        if risk_state in {"threshold_reduce_only", "hard_reduce_only"}:
            reduce_qty = _round_order_qty(max(inventory_notional - max(_safe_float(threshold_position_notional), 0.0), 0.0) / max(mid_price, EPSILON), step_size)
            if reduce_qty > 0:
                lot_plan = build_forced_reduce_lot_plan(
                    runtime=runtime,
                    reduce_price=exit_price,
                    reduce_qty=reduce_qty,
                    step_price=step_price,
                )
                if risk_state == "hard_reduce_only" or int(runtime.get("pair_credit_steps", 0) or 0) >= int(lot_plan["forced_reduce_cost_steps"]):
                    forced_qty = _round_order_qty(sum(max(_safe_float(item.get("qty")), 0.0) for item in lot_plan["lots"]), step_size)
                    if forced_qty > 0:
                        order = _build_order(side="SELL", price=exit_price, qty=forced_qty, role="forced_reduce")
                        sell_orders.append(order)
                        forced_reduce_orders.append(order)

    elif direction_state == "short_active":
        if tail_cleanup_active:
            cleanup_qty = _round_order_qty(held_qty, step_size)
            cleanup_price = _round_order_price(bid_price, tick_size, "BUY")
            if cleanup_qty > 0:
                buy_orders.append(
                    _build_order(
                        side="BUY",
                        price=cleanup_price,
                        qty=cleanup_qty,
                        role="tail_cleanup",
                    )
                )
            return {
                "risk_state": risk_state,
                "bootstrap_orders": bootstrap_orders,
                "buy_orders": buy_orders,
                "sell_orders": sell_orders,
                "forced_reduce_orders": forced_reduce_orders,
                "tail_cleanup_active": True,
            }

        exit_price = _round_order_price(bid_price, tick_size, "BUY")
        exit_qty = _round_order_qty(max(_safe_float(per_order_notional), 0.0) / max(exit_price, EPSILON), step_size)
        if exit_qty > 0 and (min_qty is None or exit_qty >= min_qty) and (min_notional is None or exit_qty * exit_price >= min_notional):
            buy_orders.append(_build_order(side="BUY", price=exit_price, qty=exit_qty, role="grid_exit"))

        entry_allowed = risk_state == "normal" and (
            max_order_position_notional <= 0 or inventory_notional + max(_safe_float(per_order_notional), 0.0) <= max_order_position_notional
        )
        if entry_allowed:
            entry_price = _round_order_price(max(_safe_float(ask_price) + max(_safe_float(step_price), 0.0), 0.0), tick_size, "SELL")
            entry_qty = _round_order_qty(max(_safe_float(per_order_notional), 0.0) / max(entry_price, EPSILON), step_size)
            if entry_qty > 0 and (min_qty is None or entry_qty >= min_qty) and (
                min_notional is None or entry_qty * entry_price >= min_notional
            ):
                sell_orders.append(_build_order(side="SELL", price=entry_price, qty=entry_qty, role="grid_entry"))

        if risk_state in {"threshold_reduce_only", "hard_reduce_only"}:
            reduce_qty = _round_order_qty(max(inventory_notional - max(_safe_float(threshold_position_notional), 0.0), 0.0) / max(mid_price, EPSILON), step_size)
            if reduce_qty > 0:
                lot_plan = build_forced_reduce_lot_plan(
                    runtime=runtime,
                    reduce_price=exit_price,
                    reduce_qty=reduce_qty,
                    step_price=step_price,
                )
                if risk_state == "hard_reduce_only" or int(runtime.get("pair_credit_steps", 0) or 0) >= int(lot_plan["forced_reduce_cost_steps"]):
                    forced_qty = _round_order_qty(sum(max(_safe_float(item.get("qty")), 0.0) for item in lot_plan["lots"]), step_size)
                    if forced_qty > 0:
                        order = _build_order(side="BUY", price=exit_price, qty=forced_qty, role="forced_reduce")
                        buy_orders.append(order)
                        forced_reduce_orders.append(order)

    return {
        "risk_state": risk_state,
        "bootstrap_orders": bootstrap_orders,
        "buy_orders": buy_orders,
        "sell_orders": sell_orders,
        "forced_reduce_orders": forced_reduce_orders,
        "tail_cleanup_active": tail_cleanup_active,
    }
