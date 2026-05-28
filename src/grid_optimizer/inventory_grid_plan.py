from __future__ import annotations

from typing import Any

from .dry_run import _round_order_price, _round_order_qty
from .inventory_grid_state import build_forced_reduce_lot_plan, select_grid_exit_lots, synthetic_frozen_position_report

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


def _filter_orders_meeting_mins(
    orders: list[dict[str, Any]],
    *,
    min_qty: float | None,
    min_notional: float | None,
) -> list[dict[str, Any]]:
    filtered: list[dict[str, Any]] = []
    for order in orders:
        qty = max(_safe_float(order.get("qty")), 0.0)
        price = max(_safe_float(order.get("price")), 0.0)
        if _order_meets_mins(qty=qty, price=price, min_qty=min_qty, min_notional=min_notional):
            filtered.append(order)
    return filtered


def _finalize_inventory_grid_plan(
    *,
    risk_state: str,
    bootstrap_orders: list[dict[str, Any]],
    buy_orders: list[dict[str, Any]],
    sell_orders: list[dict[str, Any]],
    forced_reduce_orders: list[dict[str, Any]],
    tail_cleanup_active: bool,
    min_qty: float | None,
    min_notional: float | None,
    synthetic_freeze_candidate: dict[str, Any] | None = None,
    synthetic_frozen_position: dict[str, Any] | None = None,
) -> dict[str, Any]:
    bootstrap_orders = _filter_orders_meeting_mins(bootstrap_orders, min_qty=min_qty, min_notional=min_notional)
    buy_orders = _filter_orders_meeting_mins(buy_orders, min_qty=min_qty, min_notional=min_notional)
    sell_orders = _filter_orders_meeting_mins(sell_orders, min_qty=min_qty, min_notional=min_notional)
    forced_reduce_orders = [order for order in sell_orders + buy_orders if order.get("role") == "forced_reduce"]
    return {
        "risk_state": risk_state,
        "bootstrap_orders": bootstrap_orders,
        "buy_orders": buy_orders,
        "sell_orders": sell_orders,
        "forced_reduce_orders": forced_reduce_orders,
        "tail_cleanup_active": tail_cleanup_active,
        "synthetic_freeze_candidate": synthetic_freeze_candidate or {"should_freeze": False},
        "synthetic_frozen_position": synthetic_frozen_position or {},
    }


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
    direction_state: str,
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
        if direction_state in {"long_active", "short_active"}:
            remaining_lots, matched = select_grid_exit_lots(
                lots=remaining_lots,
                direction_state=direction_state,
                close_qty=qty,
            )
        else:
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


def _spot_maker_buy_price(*, price: float, bid_price: float, tick_size: float | None) -> float:
    best_bid = max(_safe_float(bid_price), 0.0)
    if best_bid <= EPSILON:
        return _round_order_price(price, tick_size, "BUY")
    return min(_round_order_price(price, tick_size, "BUY"), _round_order_price(best_bid, tick_size, "BUY"))


def _spot_maker_sell_price(*, price: float, ask_price: float, tick_size: float | None) -> float:
    best_ask = max(_safe_float(ask_price), 0.0)
    if best_ask <= EPSILON:
        return _round_order_price(price, tick_size, "SELL")
    return max(_round_order_price(price, tick_size, "SELL"), _round_order_price(best_ask, tick_size, "SELL"))


def _synthetic_reduce_loss_stats(
    *,
    lot_plan: dict[str, Any],
    reduce_price: float,
    direction_state: str,
) -> dict[str, Any]:
    loss_quote = 0.0
    cost_quote = 0.0
    qty = 0.0
    safe_reduce_price = max(_safe_float(reduce_price), 0.0)
    for lot in list(lot_plan.get("lots") or []):
        if not isinstance(lot, dict):
            continue
        lot_qty = max(_safe_float(lot.get("qty")), 0.0)
        entry_price = max(_safe_float(lot.get("entry_price")), 0.0)
        if lot_qty <= EPSILON or entry_price <= EPSILON:
            continue
        qty += lot_qty
        cost_quote += entry_price * lot_qty
        if direction_state == "long_active":
            loss_quote += max(0.0, entry_price - safe_reduce_price) * lot_qty
        elif direction_state == "short_active":
            loss_quote += max(0.0, safe_reduce_price - entry_price) * lot_qty
    return {
        "qty": qty,
        "cost_quote": cost_quote,
        "loss_quote": loss_quote,
        "loss_ratio": loss_quote / cost_quote if cost_quote > EPSILON else 0.0,
    }


def _cap_lot_plan_qty(*, lot_plan: dict[str, Any], max_qty: float) -> dict[str, Any]:
    remaining = max(_safe_float(max_qty), 0.0)
    capped_lots: list[dict[str, Any]] = []
    for lot in list(lot_plan.get("lots") or []):
        if remaining <= EPSILON:
            break
        if not isinstance(lot, dict):
            continue
        lot_qty = max(_safe_float(lot.get("qty")), 0.0)
        take_qty = min(lot_qty, remaining)
        if take_qty <= EPSILON:
            continue
        capped_lot = dict(lot)
        capped_lot["qty"] = take_qty
        capped_lots.append(capped_lot)
        remaining -= take_qty

    capped_plan = dict(lot_plan)
    capped_plan["lots"] = capped_lots
    return capped_plan


def _lossy_lot_plan(
    *,
    lot_plan: dict[str, Any],
    reduce_price: float,
    direction_state: str,
    loss_ratio_threshold: float,
) -> dict[str, Any]:
    safe_reduce_price = max(_safe_float(reduce_price), 0.0)
    threshold = max(_safe_float(loss_ratio_threshold), 0.0)
    lossy_lots: list[dict[str, Any]] = []
    for lot in list(lot_plan.get("lots") or []):
        if not isinstance(lot, dict):
            continue
        qty = max(_safe_float(lot.get("qty")), 0.0)
        entry_price = max(_safe_float(lot.get("entry_price")), 0.0)
        if qty <= EPSILON or entry_price <= EPSILON:
            continue
        if direction_state == "long_active":
            loss_ratio = max((entry_price - safe_reduce_price) / entry_price, 0.0)
        elif direction_state == "short_active":
            loss_ratio = max((safe_reduce_price - entry_price) / entry_price, 0.0)
        else:
            loss_ratio = 0.0
        if loss_ratio + EPSILON < threshold or loss_ratio <= EPSILON:
            continue
        lossy_lot = dict(lot)
        lossy_lot["freeze_loss_ratio"] = loss_ratio
        lossy_lots.append(lossy_lot)

    lossy_plan = dict(lot_plan)
    lossy_plan["lots"] = lossy_lots
    return lossy_plan


def _synthetic_freeze_candidate(
    *,
    enabled: bool,
    risk_state: str,
    direction_state: str,
    reduce_side: str,
    reduce_price: float,
    order_price: float,
    lot_plan: dict[str, Any],
    loss_ratio_threshold: float,
    min_notional: float,
    inventory_notional: float = 0.0,
    threshold_position_notional: float = 0.0,
    current_side_notional: float = 0.0,
    max_side_notional: float = 0.0,
    cap_price: float = 0.0,
) -> dict[str, Any]:
    original_qty = sum(
        max(_safe_float(item.get("qty")), 0.0)
        for item in list(lot_plan.get("lots") or [])
        if isinstance(item, dict)
    )
    side_cap = max(_safe_float(max_side_notional), 0.0)
    side_notional = max(_safe_float(current_side_notional), 0.0)
    cap_remaining_notional = side_cap - side_notional if side_cap > EPSILON else 0.0
    cap_remaining_notional = max(cap_remaining_notional, 0.0)
    cap_unit_price = max(_safe_float(cap_price), _safe_float(reduce_price), 0.0)
    capped_by_max_side_notional = False
    if side_cap > EPSILON:
        max_freeze_qty = cap_remaining_notional / max(cap_unit_price, EPSILON)
        capped_by_max_side_notional = max_freeze_qty + EPSILON < original_qty
        lot_plan = _cap_lot_plan_qty(lot_plan=lot_plan, max_qty=max_freeze_qty)
    lot_plan = _lossy_lot_plan(
        lot_plan=lot_plan,
        reduce_price=reduce_price,
        direction_state=direction_state,
        loss_ratio_threshold=loss_ratio_threshold,
    )

    stats = _synthetic_reduce_loss_stats(
        lot_plan=lot_plan,
        reduce_price=reduce_price,
        direction_state=direction_state,
    )
    qty = max(_safe_float(stats.get("qty")), 0.0)
    notional = qty * max(_safe_float(reduce_price), 0.0)
    loss_ratio = max(_safe_float(stats.get("loss_ratio")), 0.0)
    normalized_risk_state = str(risk_state or "").strip().lower()
    threshold_notional = max(_safe_float(threshold_position_notional), 0.0)
    threshold_reached = threshold_notional > EPSILON and max(_safe_float(inventory_notional), 0.0) + EPSILON >= threshold_notional
    freeze_risk_allowed = normalized_risk_state == "threshold_reduce_only" or (
        normalized_risk_state == "hard_reduce_only" and threshold_reached
    )
    should_freeze = (
        bool(enabled)
        and freeze_risk_allowed
        and qty > EPSILON
        and notional + EPSILON >= max(_safe_float(min_notional), 0.0)
        and loss_ratio + EPSILON >= max(_safe_float(loss_ratio_threshold), 0.0)
        and loss_ratio > EPSILON
    )
    reason = "threshold_reduce_loss" if should_freeze else "not_triggered"
    if not should_freeze and side_cap > EPSILON and cap_remaining_notional <= EPSILON:
        reason = "max_side_notional_reached"
    return {
        "should_freeze": should_freeze,
        "reason": reason,
        "side": str(reduce_side or "").upper(),
        "qty": qty,
        "reduce_price": max(_safe_float(reduce_price), 0.0),
        "order_price": max(_safe_float(order_price), 0.0),
        "notional": notional,
        "loss_quote": max(_safe_float(stats.get("loss_quote")), 0.0),
        "loss_ratio": loss_ratio,
        "current_side_notional": side_notional,
        "max_side_notional": side_cap,
        "cap_remaining_notional": cap_remaining_notional,
        "capped_by_max_side_notional": capped_by_max_side_notional,
        "lots": list(lot_plan.get("lots") or []),
    }


def _weighted_entry_price(*, lots: list[dict[str, Any]]) -> float:
    qty = sum(max(_safe_float(item.get("qty")), 0.0) for item in lots)
    if qty <= EPSILON:
        return 0.0
    cost = sum(max(_safe_float(item.get("entry_price")), 0.0) * max(_safe_float(item.get("qty")), 0.0) for item in lots)
    return cost / qty if cost > 0 else 0.0


def _synthetic_frozen_release_orders(
    *,
    runtime: dict[str, Any],
    bid_price: float,
    ask_price: float,
    per_order_notional: float,
    release_profit_ratio: float,
    tick_size: float | None,
    step_size: float | None,
    min_qty: float | None,
    min_notional: float | None,
) -> list[dict[str, Any]]:
    frozen_lots = [
        dict(item)
        for item in list(runtime.get("frozen_position_lots") or [])
        if isinstance(item, dict)
        and str(item.get("source_role", "") or "").strip() != "synthetic_recovery"
        and str(item.get("freeze_reason", "") or "").strip() != "synthetic_cost_unknown"
    ]
    if not frozen_lots:
        return []

    orders: list[dict[str, Any]] = []
    profit_ratio = max(_safe_float(release_profit_ratio), 0.0)
    long_lots = [item for item in frozen_lots if str(item.get("side", "") or "").strip().lower() == "long"]
    short_lots = [item for item in frozen_lots if str(item.get("side", "") or "").strip().lower() == "short"]

    long_entry = _weighted_entry_price(lots=long_lots)
    sell_price = _round_order_price(ask_price, tick_size, "SELL")
    if long_entry > EPSILON and sell_price + EPSILON >= long_entry * (1.0 + profit_ratio):
        max_qty = sum(max(_safe_float(item.get("qty")), 0.0) for item in long_lots)
        target_qty = max_qty
        if per_order_notional > EPSILON:
            target_qty = min(target_qty, _round_order_qty(per_order_notional / max(sell_price, EPSILON), step_size))
        qty = _round_order_qty(target_qty, step_size)
        if _order_meets_mins(qty=qty, price=sell_price, min_qty=min_qty, min_notional=min_notional):
            orders.append(_build_order(side="SELL", price=sell_price, qty=qty, role="synthetic_frozen_release"))

    short_entry = _weighted_entry_price(lots=short_lots)
    buy_price = _round_order_price(bid_price, tick_size, "BUY")
    if not orders and short_entry > EPSILON and buy_price <= short_entry * (1.0 - profit_ratio) + EPSILON:
        max_qty = sum(max(_safe_float(item.get("qty")), 0.0) for item in short_lots)
        target_qty = max_qty
        if per_order_notional > EPSILON:
            target_qty = min(target_qty, _round_order_qty(per_order_notional / max(buy_price, EPSILON), step_size))
        qty = _round_order_qty(target_qty, step_size)
        if _order_meets_mins(qty=qty, price=buy_price, min_qty=min_qty, min_notional=min_notional):
            orders.append(_build_order(side="BUY", price=buy_price, qty=qty, role="synthetic_frozen_release"))

    return orders


def _synthetic_frozen_manual_release_orders(
    *,
    runtime: dict[str, Any],
    bid_price: float,
    ask_price: float,
    manual_side: str | None,
    manual_qty: float,
    manual_price: float,
    tick_size: float | None,
    step_size: float | None,
    min_qty: float | None,
    min_notional: float | None,
) -> list[dict[str, Any]]:
    side_text = str(manual_side or "").strip().lower()
    if side_text in {"long", "sell"}:
        frozen_side = "long"
        order_side = "SELL"
        price = _round_order_price(manual_price if manual_price > EPSILON else ask_price, tick_size, "SELL")
    elif side_text in {"short", "buy"}:
        frozen_side = "short"
        order_side = "BUY"
        price = _round_order_price(manual_price if manual_price > EPSILON else bid_price, tick_size, "BUY")
    else:
        return []

    frozen_lots = [
        dict(item)
        for item in list(runtime.get("frozen_position_lots") or [])
        if isinstance(item, dict) and str(item.get("side", "") or "").strip().lower() == frozen_side
    ]
    frozen_qty = sum(max(_safe_float(item.get("qty")), 0.0) for item in frozen_lots)
    target_qty = min(max(_safe_float(manual_qty), 0.0), frozen_qty)
    if target_qty <= EPSILON or price <= EPSILON:
        return []
    qty = _round_order_qty(target_qty, step_size)
    if not _order_meets_mins(qty=qty, price=price, min_qty=min_qty, min_notional=min_notional):
        return []
    return [_build_order(side=order_side, price=price, qty=qty, role="synthetic_frozen_manual_release")]


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
    if levels <= 0:
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
    first_role: str | None = None,
    next_role: str | None = None,
    start_level: int,
    max_total_qty: float | None,
    tick_size: float | None,
    step_size: float | None,
    min_qty: float | None,
    min_notional: float | None,
) -> list[dict[str, Any]]:
    if max_total_qty is not None and max_total_qty <= EPSILON:
        return []
    if levels <= 0:
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
        order_role = role
        if first_role is not None or next_role is not None:
            order_role = str(first_role if offset == 0 else (next_role or role))
        orders.append(_build_order(side="SELL", price=price, qty=qty, role=order_role))
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
    max_short_order_position_notional: float | None = None,
    max_short_position_notional: float | None = None,
    buy_levels: int = 1,
    tick_size: float | None,
    step_size: float | None,
    min_qty: float | None,
    min_notional: float | None,
    sell_levels: int = 1,
    synthetic_freeze_enabled: bool = False,
    synthetic_freeze_loss_ratio: float = 0.0,
    synthetic_freeze_min_notional: float = 0.0,
    synthetic_freeze_max_side_notional: float = 0.0,
    synthetic_freeze_release_profit_ratio: float = 0.0,
    synthetic_freeze_manual_release_side: str | None = None,
    synthetic_freeze_manual_release_qty: float = 0.0,
    synthetic_freeze_manual_release_price: float = 0.0,
) -> dict[str, Any]:
    market_type = str(runtime.get("market_type", "futures")).strip().lower()
    direction_state = str(runtime.get("direction_state", "flat")).strip().lower()
    recovery_mode = _normalized_recovery_mode(runtime=runtime)
    runtime_risk_state = _normalized_risk_state(runtime=runtime, recovery_mode=recovery_mode)
    anchor_price = max(_safe_float(runtime.get("grid_anchor_price")), 0.0)
    bootstrap_notional = max(_safe_float(per_order_notional), 0.0) * max(_safe_float(first_order_multiplier), 0.0)
    warmup_notional = max(_safe_float(warmup_position_notional), 0.0)
    mid_price = (max(_safe_float(bid_price), 0.0) + max(_safe_float(ask_price), 0.0)) / 2.0
    synthetic_neutral = bool(runtime.get("synthetic_neutral"))
    short_order_limit_notional = max(_safe_float(max_short_order_position_notional), 0.0)
    if short_order_limit_notional <= EPSILON:
        short_order_limit_notional = max(_safe_float(max_order_position_notional), 0.0)
    short_position_limit_notional = max(_safe_float(max_short_position_notional), 0.0)
    if short_position_limit_notional <= EPSILON:
        short_position_limit_notional = max(_safe_float(max_position_notional), 0.0)

    bootstrap_orders: list[dict[str, Any]] = []
    buy_orders: list[dict[str, Any]] = []
    sell_orders: list[dict[str, Any]] = []
    forced_reduce_orders: list[dict[str, Any]] = []
    synthetic_freeze_candidate: dict[str, Any] = {"should_freeze": False}
    synthetic_frozen_position = synthetic_frozen_position_report(runtime=runtime, mid_price=mid_price) if synthetic_neutral else {}
    synthetic_manual_release_orders = (
        _synthetic_frozen_manual_release_orders(
            runtime=runtime,
            bid_price=bid_price,
            ask_price=ask_price,
            manual_side=synthetic_freeze_manual_release_side,
            manual_qty=synthetic_freeze_manual_release_qty,
            manual_price=synthetic_freeze_manual_release_price,
            tick_size=tick_size,
            step_size=step_size,
            min_qty=min_qty,
            min_notional=min_notional,
        )
        if synthetic_neutral and synthetic_freeze_enabled
        else []
    )
    synthetic_release_orders = (
        _synthetic_frozen_release_orders(
            runtime=runtime,
            bid_price=bid_price,
            ask_price=ask_price,
            per_order_notional=per_order_notional,
            release_profit_ratio=synthetic_freeze_release_profit_ratio,
            tick_size=tick_size,
            step_size=step_size,
            min_qty=min_qty,
            min_notional=min_notional,
        )
        if synthetic_neutral and synthetic_freeze_enabled and not synthetic_manual_release_orders
        else []
    )
    for order in synthetic_manual_release_orders + synthetic_release_orders:
        if str(order.get("side", "")).upper() == "BUY":
            buy_orders.append(order)
        else:
            sell_orders.append(order)

    if direction_state == "flat":
        if synthetic_manual_release_orders or synthetic_release_orders:
            return _finalize_inventory_grid_plan(
                risk_state=runtime_risk_state,
                bootstrap_orders=bootstrap_orders,
                buy_orders=buy_orders,
                sell_orders=sell_orders,
                forced_reduce_orders=forced_reduce_orders,
                tail_cleanup_active=False,
                min_qty=min_qty,
                min_notional=min_notional,
                synthetic_freeze_candidate=synthetic_freeze_candidate,
                synthetic_frozen_position=synthetic_frozen_position,
            )
        if recovery_mode != "live":
            return _finalize_inventory_grid_plan(
                risk_state=runtime_risk_state,
                bootstrap_orders=bootstrap_orders,
                buy_orders=buy_orders,
                sell_orders=sell_orders,
                forced_reduce_orders=forced_reduce_orders,
                tail_cleanup_active=False,
                min_qty=min_qty,
                min_notional=min_notional,
                synthetic_freeze_candidate=synthetic_freeze_candidate,
                synthetic_frozen_position=synthetic_frozen_position,
            )
        if market_type == "spot" and not synthetic_neutral:
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
        elif synthetic_neutral:
            bootstrap_orders = _build_buy_ladder_orders(
                reference_price=bid_price,
                step_price=step_price,
                levels=buy_levels,
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
            max_short_qty = None
            flat_short_cap_notional = short_order_limit_notional
            if short_position_limit_notional > EPSILON:
                flat_short_cap_notional = (
                    min(flat_short_cap_notional, short_position_limit_notional)
                    if flat_short_cap_notional > EPSILON
                    else short_position_limit_notional
                )
            if flat_short_cap_notional > EPSILON:
                max_short_qty = flat_short_cap_notional / max(mid_price, EPSILON)
            bootstrap_orders.extend(
                _build_sell_ladder_orders(
                    reference_price=ask_price,
                    step_price=step_price,
                    levels=sell_levels,
                    per_order_notional=per_order_notional,
                    role="grid_entry",
                    first_role="bootstrap_entry",
                    next_role="grid_entry",
                    start_level=0,
                    max_total_qty=max_short_qty,
                    tick_size=tick_size,
                    step_size=step_size,
                    min_qty=min_qty,
                    min_notional=min_notional,
                )
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
        return _finalize_inventory_grid_plan(
            risk_state="normal",
            bootstrap_orders=bootstrap_orders,
            buy_orders=buy_orders,
            sell_orders=sell_orders,
            forced_reduce_orders=forced_reduce_orders,
            tail_cleanup_active=False,
            min_qty=min_qty,
            min_notional=min_notional,
            synthetic_freeze_candidate=synthetic_freeze_candidate,
            synthetic_frozen_position=synthetic_frozen_position,
        )

    inventory_notional = _position_notional(runtime=runtime, mid_price=mid_price)
    held_qty = _held_qty(runtime=runtime)
    one_order_qty = _round_order_qty(max(_safe_float(per_order_notional), 0.0) / max(mid_price, EPSILON), step_size)
    if recovery_mode != "live":
        risk_state = runtime_risk_state
    else:
        position_limit_notional = short_position_limit_notional if direction_state == "short_active" else max_position_notional
        if position_limit_notional > 0 and inventory_notional >= position_limit_notional:
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
                    _round_order_price(
                        _matched_break_even_price(
                            matched=select_grid_exit_lots(
                                lots=list(runtime.get("position_lots") or []),
                                direction_state=direction_state,
                                close_qty=cleanup_qty,
                            )[1]
                        ),
                        tick_size,
                        "SELL",
                    ),
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
                return _finalize_inventory_grid_plan(
                    risk_state=risk_state,
                    bootstrap_orders=bootstrap_orders,
                    buy_orders=buy_orders,
                    sell_orders=sell_orders,
                    forced_reduce_orders=forced_reduce_orders,
                    tail_cleanup_active=True,
                    min_qty=min_qty,
                    min_notional=min_notional,
                )

        closeable_qty = _round_order_qty(max(held_qty, 0.0), step_size)
        planned_closing_qty = 0.0
        sell_reference_price = anchor_price
        if synthetic_neutral:
            sell_reference_price = max(sell_reference_price, max(_safe_float(ask_price), 0.0) - max(float(step_price), 0.0))
        elif market_type == "spot":
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
                    direction_state=direction_state,
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
                synthetic_freeze_candidate = _synthetic_freeze_candidate(
                    enabled=synthetic_neutral and synthetic_freeze_enabled,
                    risk_state=risk_state,
                    direction_state=direction_state,
                    reduce_side="SELL",
                    reduce_price=forced_reduce_price,
                    order_price=forced_reduce_order_price,
                    lot_plan=lot_plan,
                    loss_ratio_threshold=synthetic_freeze_loss_ratio,
                    min_notional=synthetic_freeze_min_notional,
                    inventory_notional=inventory_notional,
                    threshold_position_notional=threshold_position_notional,
                    current_side_notional=_safe_float(synthetic_frozen_position.get("long_notional")),
                    max_side_notional=synthetic_freeze_max_side_notional,
                    cap_price=max(mid_price, forced_reduce_price),
                )
                if (
                    not bool(synthetic_freeze_candidate.get("should_freeze"))
                    and (
                        risk_state == "hard_reduce_only"
                        or _threshold_reduce_unlocks_without_credit(
                            threshold_position_notional=threshold_position_notional,
                            threshold_reduce_target_notional=threshold_reduce_target_notional,
                        )
                        or int(runtime.get("pair_credit_steps", 0) or 0) >= int(lot_plan["forced_reduce_cost_steps"])
                    )
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
                return _finalize_inventory_grid_plan(
                    risk_state=risk_state,
                    bootstrap_orders=bootstrap_orders,
                    buy_orders=buy_orders,
                    sell_orders=sell_orders,
                    forced_reduce_orders=forced_reduce_orders,
                    tail_cleanup_active=True,
                    min_qty=min_qty,
                    min_notional=min_notional,
                )

        exit_price = _round_order_price(anchor_price - step_price, tick_size, "BUY")
        if synthetic_neutral:
            exit_price = _spot_maker_buy_price(price=exit_price, bid_price=bid_price, tick_size=tick_size)
        exit_qty = _round_order_qty(max(_safe_float(per_order_notional), 0.0) / max(exit_price, EPSILON), step_size)
        closeable_qty = _round_order_qty(max(held_qty, 0.0), step_size)
        exit_qty = min(exit_qty, closeable_qty)
        planned_closing_qty = 0.0
        if _order_meets_mins(qty=exit_qty, price=exit_price, min_qty=min_qty, min_notional=min_notional):
            buy_orders.append(_build_order(side="BUY", price=exit_price, qty=exit_qty, role="grid_exit"))
            planned_closing_qty = exit_qty

        entry_allowed = risk_state == "normal" and (
            (
                short_order_limit_notional <= 0
                or inventory_notional + max(_safe_float(per_order_notional), 0.0) <= short_order_limit_notional
            )
            and (
                short_position_limit_notional <= 0
                or inventory_notional + max(_safe_float(per_order_notional), 0.0) <= short_position_limit_notional
            )
        )
        if entry_allowed:
            entry_price = _round_order_price(anchor_price + step_price, tick_size, "SELL")
            if synthetic_neutral:
                entry_price = _spot_maker_sell_price(price=entry_price, ask_price=ask_price, tick_size=tick_size)
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
                synthetic_freeze_candidate = _synthetic_freeze_candidate(
                    enabled=synthetic_neutral and synthetic_freeze_enabled,
                    risk_state=risk_state,
                    direction_state=direction_state,
                    reduce_side="BUY",
                    reduce_price=forced_reduce_price,
                    order_price=forced_reduce_order_price,
                    lot_plan=lot_plan,
                    loss_ratio_threshold=synthetic_freeze_loss_ratio,
                    min_notional=synthetic_freeze_min_notional,
                    inventory_notional=inventory_notional,
                    threshold_position_notional=threshold_position_notional,
                    current_side_notional=_safe_float(synthetic_frozen_position.get("short_notional")),
                    max_side_notional=synthetic_freeze_max_side_notional,
                    cap_price=max(mid_price, forced_reduce_price),
                )
                if (
                    not bool(synthetic_freeze_candidate.get("should_freeze"))
                    and (
                        risk_state == "hard_reduce_only"
                        or _threshold_reduce_unlocks_without_credit(
                            threshold_position_notional=threshold_position_notional,
                            threshold_reduce_target_notional=threshold_reduce_target_notional,
                        )
                        or int(runtime.get("pair_credit_steps", 0) or 0) >= int(lot_plan["forced_reduce_cost_steps"])
                    )
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

    return _finalize_inventory_grid_plan(
        risk_state=risk_state,
        bootstrap_orders=bootstrap_orders,
        buy_orders=buy_orders,
        sell_orders=sell_orders,
        forced_reduce_orders=forced_reduce_orders,
        tail_cleanup_active=tail_cleanup_active,
        min_qty=min_qty,
        min_notional=min_notional,
        synthetic_freeze_candidate=synthetic_freeze_candidate,
        synthetic_frozen_position=synthetic_frozen_position,
    )
