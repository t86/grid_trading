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


def _safe_positive_int(value: Any, default: int = 1) -> int:
    try:
        number = int(value)
    except (TypeError, ValueError):
        return default
    return number if number > 0 else default


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


def _order_meets_mins(*, qty: float, price: float, min_qty: float | None, min_notional: float | None) -> bool:
    if qty <= 0:
        return False
    if min_qty is not None and qty < min_qty:
        return False
    if min_notional is not None and qty * price < min_notional:
        return False
    return True


def _normalized_recovery_mode(*, runtime: dict[str, Any]) -> str:
    return str(runtime.get("recovery_mode", "live")).strip().lower()


def _normalized_risk_state(*, runtime: dict[str, Any], recovery_mode: str) -> str:
    risk_state = str(runtime.get("risk_state", "normal")).strip().lower()
    if recovery_mode != "live":
        if risk_state in {"threshold_reduce_only", "hard_reduce_only"}:
            return risk_state
        return "hard_reduce_only"
    if risk_state in {"normal", "threshold_reduce_only", "hard_reduce_only"}:
        return risk_state
    return "normal"


def _position_notional(*, runtime: dict[str, Any], mid_price: float) -> float:
    total_qty = sum(max(_safe_float(item.get("qty")), 0.0) for item in list(runtime.get("position_lots") or []))
    return total_qty * max(mid_price, 0.0)


def _held_qty(*, runtime: dict[str, Any]) -> float:
    return sum(max(_safe_float(item.get("qty")), 0.0) for item in list(runtime.get("position_lots") or []))


def _consume_lots_preview(
    *,
    lots: list[dict[str, Any]],
    qty_to_consume: float,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    remaining = max(float(qty_to_consume), 0.0)
    new_lots: list[dict[str, Any]] = []
    matched: list[dict[str, Any]] = []
    for lot in lots:
        if remaining <= EPSILON:
            new_lots.append(dict(lot))
            continue
        qty = max(_safe_float(lot.get("qty")), 0.0)
        if qty <= EPSILON:
            continue
        take_qty = min(qty, remaining)
        matched.append({**dict(lot), "matched_qty": take_qty})
        left_qty = qty - take_qty
        if left_qty > EPSILON:
            kept = dict(lot)
            kept["qty"] = left_qty
            new_lots.append(kept)
        remaining -= take_qty
    return new_lots, matched


def _matched_break_even_price(*, matched: list[dict[str, Any]]) -> float:
    total_qty = sum(max(_safe_float(item.get("matched_qty")), 0.0) for item in matched)
    if total_qty <= EPSILON:
        return 0.0
    total_cost = sum(
        max(_safe_float(item.get("entry_price")), 0.0) * max(_safe_float(item.get("matched_qty")), 0.0)
        for item in matched
    )
    return total_cost / total_qty if total_cost > 0 else 0.0


def _next_unique_sell_price(*, price: float, used_prices: set[float], tick_size: float | None) -> float:
    adjusted = max(float(price), 0.0)
    if adjusted <= 0:
        return 0.0
    if adjusted not in used_prices:
        return adjusted
    tick = max(_safe_float(tick_size), 0.0)
    if tick > EPSILON:
        while adjusted in used_prices:
            adjusted = _round_order_price(adjusted + tick, tick_size, "SELL")
        return adjusted
    while adjusted in used_prices:
        adjusted += EPSILON
    return adjusted


def _apply_spot_non_loss_floor_to_sell_orders(
    *,
    orders: list[dict[str, Any]],
    position_lots: list[dict[str, Any]],
    tick_size: float | None,
    min_qty: float | None,
    min_notional: float | None,
) -> list[dict[str, Any]]:
    adjusted_orders: list[dict[str, Any]] = []
    remaining_lots = [dict(item) for item in list(position_lots or []) if isinstance(item, dict)]
    used_prices: set[float] = set()
    for order in orders:
        qty = max(_safe_float(order.get("qty")), 0.0)
        price = max(_safe_float(order.get("price")), 0.0)
        if qty <= EPSILON or price <= 0:
            continue
        remaining_lots, matched = _consume_lots_preview(lots=remaining_lots, qty_to_consume=qty)
        break_even_price = _matched_break_even_price(matched=matched)
        if break_even_price > 0:
            price = max(price, _round_order_price(break_even_price, tick_size, "SELL"))
        price = _next_unique_sell_price(price=price, used_prices=used_prices, tick_size=tick_size)
        if not _order_meets_mins(qty=qty, price=price, min_qty=min_qty, min_notional=min_notional):
            continue
        adjusted_orders.append(
            _build_order(
                side="SELL",
                price=price,
                qty=qty,
                role=str(order.get("role", "") or "grid_exit"),
                position_side=order.get("position_side"),
            )
        )
        used_prices.add(price)
    return adjusted_orders


def _resolved_reduce_target_notional(
    *,
    threshold_position_notional: float,
    threshold_reduce_target_notional: float | None,
) -> float:
    threshold_notional = max(_safe_float(threshold_position_notional), 0.0)
    explicit_target = max(_safe_float(threshold_reduce_target_notional), 0.0)
    if threshold_notional <= EPSILON:
        return explicit_target
    if explicit_target <= EPSILON or explicit_target >= threshold_notional:
        return threshold_notional
    return explicit_target


def _threshold_reduce_unlocks_without_credit(
    *,
    threshold_position_notional: float,
    threshold_reduce_target_notional: float | None,
) -> bool:
    threshold_notional = max(_safe_float(threshold_position_notional), 0.0)
    reduce_target_notional = _resolved_reduce_target_notional(
        threshold_position_notional=threshold_notional,
        threshold_reduce_target_notional=threshold_reduce_target_notional,
    )
    return threshold_notional > EPSILON and reduce_target_notional + EPSILON < threshold_notional


def _spot_long_entry_reference_price(
    *,
    anchor_price: float,
    bid_price: float,
    ask_price: float,
    step_price: float,
) -> float:
    reference_price = max(_safe_float(anchor_price), 0.0)
    best_bid = max(_safe_float(bid_price), 0.0)
    best_ask = max(_safe_float(ask_price), 0.0)
    step = max(_safe_float(step_price), 0.0)
    if reference_price <= EPSILON:
        return best_bid
    first_entry_price = max(reference_price - step, 0.0)
    if best_ask > EPSILON and first_entry_price + EPSILON >= best_ask:
        return max(best_bid + step, best_bid)
    return reference_price


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
        orders.append(_build_order(side="BUY", price=buy_price, qty=buy_qty, role="bootstrap_entry"))

    if market_type == "futures":
        sell_price = _round_order_price(ask_price, tick_size, "SELL")
        sell_qty = _round_order_qty(bootstrap_notional / max(sell_price, EPSILON), step_size)
        sell_notional = sell_price * sell_qty
        if sell_qty > 0 and (min_qty is None or sell_qty >= min_qty) and (min_notional is None or sell_notional >= min_notional):
            orders.append(_build_order(side="SELL", price=sell_price, qty=sell_qty, role="bootstrap_entry"))

    return orders


def _build_buy_ladder_orders(
    *,
    reference_price: float,
    step_price: float,
    levels: int,
    first_order_notional: float,
    per_order_notional: float,
    first_role: str,
    next_role: str,
    start_level: int,
    max_total_notional: float | None,
    tick_size: float | None,
    step_size: float | None,
    min_qty: float | None,
    min_notional: float | None,
) -> list[dict[str, Any]]:
    if max_total_notional is not None and max_total_notional <= EPSILON:
        return []

    orders: list[dict[str, Any]] = []
    used_prices: set[float] = set()
    planned_notional = 0.0

    safe_levels = _safe_positive_int(levels, default=1)
    for offset in range(safe_levels):
        level = start_level + offset
        raw_price = max(float(reference_price) - float(step_price) * float(level), 0.0)
        if raw_price <= 0:
            break
        price = _round_order_price(raw_price, tick_size, "BUY")
        if price <= 0 or price in used_prices:
            continue
        target_notional = max(float(first_order_notional if offset == 0 else per_order_notional), 0.0)
        if target_notional <= 0:
            continue
        qty = _round_order_qty(target_notional / max(price, EPSILON), step_size)
        actual_notional = qty * price
        if not _order_meets_mins(qty=qty, price=price, min_qty=min_qty, min_notional=min_notional):
            continue
        if max_total_notional is not None and max_total_notional > 0 and planned_notional + actual_notional > max_total_notional + EPSILON:
            break
        orders.append(
            _build_order(
                side="BUY",
                price=price,
                qty=qty,
                role=first_role if offset == 0 else next_role,
            )
        )
        used_prices.add(price)
        planned_notional += actual_notional

    return orders


def _build_sell_ladder_orders(
    *,
    reference_price: float,
    step_price: float,
    levels: int,
    per_order_notional: float,
    role: str,
    start_level: int,
    max_total_qty: float | None,
    tick_size: float | None,
    step_size: float | None,
    min_qty: float | None,
    min_notional: float | None,
) -> list[dict[str, Any]]:
    if max_total_qty is not None and max_total_qty <= EPSILON:
        return []

    orders: list[dict[str, Any]] = []
    used_prices: set[float] = set()
    remaining_qty = None if max_total_qty is None else max(float(max_total_qty), 0.0)

    safe_levels = _safe_positive_int(levels, default=1)
    for offset in range(safe_levels):
        level = start_level + offset
        raw_price = max(float(reference_price) + float(step_price) * float(level), 0.0)
        if raw_price <= 0:
            continue
        price = _round_order_price(raw_price, tick_size, "SELL")
        if price <= 0 or price in used_prices:
            continue
        target_notional = max(float(per_order_notional), 0.0)
        if target_notional <= 0:
            continue
        qty = _round_order_qty(target_notional / max(price, EPSILON), step_size)
        if remaining_qty is not None:
            remaining_qty_rounded = _round_order_qty(max(remaining_qty, 0.0), step_size)
            if remaining_qty_rounded <= 0:
                break
            qty = min(qty, remaining_qty_rounded)
        if not _order_meets_mins(qty=qty, price=price, min_qty=min_qty, min_notional=min_notional):
            continue
        orders.append(_build_order(side="SELL", price=price, qty=qty, role=role))
        used_prices.add(price)
        if remaining_qty is not None:
            remaining_qty = max(remaining_qty - qty, 0.0)
            if remaining_qty <= EPSILON:
                break

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
    threshold_reduce_target_notional: float | None = None,
    warmup_position_notional: float | None = None,
    require_non_loss_exit: bool = False,
    max_order_position_notional: float,
    max_position_notional: float,
    buy_levels: int = 1,
    tick_size: float | None,
    step_size: float | None,
    min_qty: float | None,
    min_notional: float | None,
    sell_levels: int = 1,
) -> dict[str, Any]:
    market_type = str(runtime.get("market_type", "futures")).strip().lower()
    direction_state = str(runtime.get("direction_state", "flat")).strip().lower()
    recovery_mode = _normalized_recovery_mode(runtime=runtime)
    runtime_risk_state = _normalized_risk_state(runtime=runtime, recovery_mode=recovery_mode)
    anchor_price = max(_safe_float(runtime.get("grid_anchor_price")), 0.0)
    bootstrap_notional = max(_safe_float(per_order_notional), 0.0) * max(_safe_float(first_order_multiplier), 0.0)
    warmup_notional = max(_safe_float(warmup_position_notional), 0.0)
    mid_price = (max(_safe_float(bid_price), 0.0) + max(_safe_float(ask_price), 0.0)) / 2.0

    bootstrap_orders: list[dict[str, Any]] = []
    buy_orders: list[dict[str, Any]] = []
    sell_orders: list[dict[str, Any]] = []
    forced_reduce_orders: list[dict[str, Any]] = []

    if direction_state == "flat":
        if recovery_mode != "live":
            return {
                "risk_state": runtime_risk_state,
                "bootstrap_orders": bootstrap_orders,
                "buy_orders": buy_orders,
                "sell_orders": sell_orders,
                "forced_reduce_orders": forced_reduce_orders,
                "tail_cleanup_active": False,
            }
        if market_type == "spot":
            bootstrap_levels = 1 if warmup_notional > EPSILON else buy_levels
            bootstrap_orders = _build_buy_ladder_orders(
                reference_price=bid_price,
                step_price=step_price,
                levels=bootstrap_levels,
                first_order_notional=bootstrap_notional,
                per_order_notional=per_order_notional,
                first_role="bootstrap_entry",
                next_role="grid_entry",
                start_level=0,
                max_total_notional=max_order_position_notional if max_order_position_notional > 0 else None,
                tick_size=tick_size,
                step_size=step_size,
                min_qty=min_qty,
                min_notional=min_notional,
            )
        else:
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
    if recovery_mode != "live":
        risk_state = runtime_risk_state
    else:
        if max_position_notional > 0 and inventory_notional >= max_position_notional:
            risk_state = "hard_reduce_only"
        elif threshold_position_notional > 0 and inventory_notional >= threshold_position_notional:
            risk_state = "threshold_reduce_only"
        else:
            risk_state = "normal"

    tail_cleanup_active = 0.0 < held_qty < max(one_order_qty, EPSILON)
    non_loss_spot_exit_active = market_type == "spot" and require_non_loss_exit and risk_state == "normal"
    warmup_top_of_book_active = (
        market_type == "spot"
        and risk_state == "normal"
        and warmup_notional > EPSILON
        and inventory_notional + EPSILON < warmup_notional
    )

    if direction_state == "long_active":
        if tail_cleanup_active and not warmup_top_of_book_active:
            cleanup_qty = _round_order_qty(held_qty, step_size)
            cleanup_price = _round_order_price(ask_price, tick_size, "SELL")
            if non_loss_spot_exit_active:
                cleanup_price = max(
                    cleanup_price,
                    _round_order_price(_matched_break_even_price(matched=_consume_lots_preview(lots=list(runtime.get("position_lots") or []), qty_to_consume=cleanup_qty)[1]), tick_size, "SELL"),
                )
            cleanup_notional = cleanup_qty * cleanup_price
            if cleanup_qty > 0 and (min_qty is None or cleanup_qty >= min_qty) and (
                min_notional is None or cleanup_notional >= min_notional
            ):
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

        closeable_qty = _round_order_qty(max(held_qty, 0.0), step_size)
        planned_closing_qty = 0.0
        sell_reference_price = anchor_price
        if market_type == "spot":
            sell_reference_price = max(sell_reference_price, max(_safe_float(ask_price), 0.0) - max(float(step_price), 0.0))
        raw_sell_orders: list[dict[str, Any]]
        if warmup_top_of_book_active:
            raw_sell_orders = []
            top_book_sell_price = _round_order_price(ask_price, tick_size, "SELL")
            if _order_meets_mins(
                qty=closeable_qty,
                price=top_book_sell_price,
                min_qty=min_qty,
                min_notional=min_notional,
            ):
                raw_sell_orders.append(
                    _build_order(
                        side="SELL",
                        price=top_book_sell_price,
                        qty=closeable_qty,
                        role="grid_exit",
                    )
                )
        else:
            raw_sell_orders = _build_sell_ladder_orders(
                reference_price=sell_reference_price,
                step_price=step_price,
                levels=sell_levels if risk_state == "normal" else 1,
                per_order_notional=per_order_notional,
                role="grid_exit",
                start_level=1,
                max_total_qty=closeable_qty,
                tick_size=tick_size,
                step_size=step_size,
                min_qty=min_qty,
                min_notional=min_notional,
            )
        if non_loss_spot_exit_active:
            sell_orders.extend(
                _apply_spot_non_loss_floor_to_sell_orders(
                    orders=raw_sell_orders,
                    position_lots=list(runtime.get("position_lots") or []),
                    tick_size=tick_size,
                    min_qty=min_qty,
                    min_notional=min_notional,
                )
            )
        else:
            sell_orders.extend(raw_sell_orders)
        planned_closing_qty = sum(max(_safe_float(item.get("qty")), 0.0) for item in sell_orders if item.get("role") == "grid_exit")

        if risk_state == "normal":
            remaining_entry_notional_cap = None
            if max_order_position_notional > 0:
                remaining_entry_notional_cap = max(max_order_position_notional - inventory_notional, 0.0)
            if warmup_top_of_book_active:
                buy_orders.extend(
                    _build_buy_ladder_orders(
                        reference_price=bid_price,
                        step_price=step_price,
                        levels=1,
                        first_order_notional=per_order_notional,
                        per_order_notional=per_order_notional,
                        first_role="grid_entry",
                        next_role="grid_entry",
                        start_level=0,
                        max_total_notional=remaining_entry_notional_cap,
                        tick_size=tick_size,
                        step_size=step_size,
                        min_qty=min_qty,
                        min_notional=min_notional,
                    )
                )
            else:
                buy_reference_price = anchor_price
                if market_type == "spot":
                    buy_reference_price = _spot_long_entry_reference_price(
                        anchor_price=anchor_price,
                        bid_price=bid_price,
                        ask_price=ask_price,
                        step_price=step_price,
                    )
                buy_orders.extend(
                    _build_buy_ladder_orders(
                        reference_price=buy_reference_price,
                        step_price=step_price,
                        levels=buy_levels,
                        first_order_notional=per_order_notional,
                        per_order_notional=per_order_notional,
                        first_role="grid_entry",
                        next_role="grid_entry",
                        start_level=1,
                        max_total_notional=remaining_entry_notional_cap,
                        tick_size=tick_size,
                        step_size=step_size,
                        min_qty=min_qty,
                        min_notional=min_notional,
                    )
                )

        if risk_state in {"threshold_reduce_only", "hard_reduce_only"}:
            reduce_target_notional = _resolved_reduce_target_notional(
                threshold_position_notional=threshold_position_notional,
                threshold_reduce_target_notional=threshold_reduce_target_notional,
            )
            raw_reduce_qty = max(inventory_notional - reduce_target_notional, 0.0) / max(mid_price, EPSILON)
            available_reduce_qty = _round_order_qty(max(held_qty - planned_closing_qty, 0.0), step_size)
            reduce_qty = min(_round_order_qty(raw_reduce_qty, step_size), available_reduce_qty)
            if reduce_qty > 0:
                forced_reduce_price = _safe_float(ask_price)
                forced_reduce_order_price = _round_order_price(ask_price, tick_size, "SELL")
                lot_plan = build_forced_reduce_lot_plan(
                    runtime=runtime,
                    reduce_price=forced_reduce_price,
                    reduce_qty=reduce_qty,
                    step_price=step_price,
                )
                if (
                    risk_state == "hard_reduce_only"
                    or _threshold_reduce_unlocks_without_credit(
                        threshold_position_notional=threshold_position_notional,
                        threshold_reduce_target_notional=threshold_reduce_target_notional,
                    )
                    or int(runtime.get("pair_credit_steps", 0) or 0) >= int(lot_plan["forced_reduce_cost_steps"])
                ):
                    forced_qty = _round_order_qty(sum(max(_safe_float(item.get("qty")), 0.0) for item in lot_plan["lots"]), step_size)
                    if _order_meets_mins(
                        qty=forced_qty,
                        price=forced_reduce_order_price,
                        min_qty=min_qty,
                        min_notional=min_notional,
                    ):
                        order = _build_order(side="SELL", price=forced_reduce_order_price, qty=forced_qty, role="forced_reduce")
                        sell_orders.append(order)
                        forced_reduce_orders.append(order)

    elif direction_state == "short_active":
        if tail_cleanup_active:
            cleanup_qty = _round_order_qty(held_qty, step_size)
            cleanup_price = _round_order_price(bid_price, tick_size, "BUY")
            cleanup_notional = cleanup_qty * cleanup_price
            if cleanup_qty > 0 and (min_qty is None or cleanup_qty >= min_qty) and (
                min_notional is None or cleanup_notional >= min_notional
            ):
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

        exit_price = _round_order_price(anchor_price - step_price, tick_size, "BUY")
        exit_qty = _round_order_qty(max(_safe_float(per_order_notional), 0.0) / max(exit_price, EPSILON), step_size)
        closeable_qty = _round_order_qty(max(held_qty, 0.0), step_size)
        exit_qty = min(exit_qty, closeable_qty)
        planned_closing_qty = 0.0
        if _order_meets_mins(qty=exit_qty, price=exit_price, min_qty=min_qty, min_notional=min_notional):
            buy_orders.append(_build_order(side="BUY", price=exit_price, qty=exit_qty, role="grid_exit"))
            planned_closing_qty = exit_qty

        entry_allowed = risk_state == "normal" and (
            max_order_position_notional <= 0 or inventory_notional + max(_safe_float(per_order_notional), 0.0) <= max_order_position_notional
        )
        if entry_allowed:
            entry_price = _round_order_price(anchor_price + step_price, tick_size, "SELL")
            entry_qty = _round_order_qty(max(_safe_float(per_order_notional), 0.0) / max(entry_price, EPSILON), step_size)
            if entry_qty > 0 and (min_qty is None or entry_qty >= min_qty) and (
                min_notional is None or entry_qty * entry_price >= min_notional
            ):
                sell_orders.append(_build_order(side="SELL", price=entry_price, qty=entry_qty, role="grid_entry"))

        if risk_state in {"threshold_reduce_only", "hard_reduce_only"}:
            reduce_target_notional = _resolved_reduce_target_notional(
                threshold_position_notional=threshold_position_notional,
                threshold_reduce_target_notional=threshold_reduce_target_notional,
            )
            raw_reduce_qty = max(inventory_notional - reduce_target_notional, 0.0) / max(mid_price, EPSILON)
            available_reduce_qty = _round_order_qty(max(held_qty - planned_closing_qty, 0.0), step_size)
            reduce_qty = min(_round_order_qty(raw_reduce_qty, step_size), available_reduce_qty)
            if reduce_qty > 0:
                forced_reduce_price = _safe_float(bid_price)
                forced_reduce_order_price = _round_order_price(bid_price, tick_size, "BUY")
                lot_plan = build_forced_reduce_lot_plan(
                    runtime=runtime,
                    reduce_price=forced_reduce_price,
                    reduce_qty=reduce_qty,
                    step_price=step_price,
                )
                if (
                    risk_state == "hard_reduce_only"
                    or _threshold_reduce_unlocks_without_credit(
                        threshold_position_notional=threshold_position_notional,
                        threshold_reduce_target_notional=threshold_reduce_target_notional,
                    )
                    or int(runtime.get("pair_credit_steps", 0) or 0) >= int(lot_plan["forced_reduce_cost_steps"])
                ):
                    forced_qty = _round_order_qty(sum(max(_safe_float(item.get("qty")), 0.0) for item in lot_plan["lots"]), step_size)
                    if _order_meets_mins(
                        qty=forced_qty,
                        price=forced_reduce_order_price,
                        min_qty=min_qty,
                        min_notional=min_notional,
                    ):
                        order = _build_order(side="BUY", price=forced_reduce_order_price, qty=forced_qty, role="forced_reduce")
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
