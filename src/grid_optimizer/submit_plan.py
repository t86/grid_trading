from __future__ import annotations

import argparse
import json
import math
import time
import traceback
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from pathlib import Path
from typing import Any, Iterable, Mapping

from .data import (
    delete_futures_order,
    fetch_futures_account_info_v3,
    fetch_futures_book_tickers,
    fetch_futures_open_orders,
    fetch_futures_position_mode,
    load_binance_api_credentials,
    post_futures_change_initial_leverage,
    post_futures_change_margin_type,
    post_futures_order,
)
from .dry_run import _round_order_price
from .live_check import extract_symbol_position


MIN_ORDER_QTY_BUMP_ROLES = {
    "bootstrap",
    "bootstrap_entry",
    "bootstrap_long",
    "bootstrap_short",
    "entry",
    "entry_long",
    "entry_short",
    "grid_entry",
}
FROZEN_MANUAL_REDUCE_IOC_CROSS_TICKS = 10
INVENTORY_UNLOCK_ALLOW_LOSS_ROLES = {
    "inventory_unlock_reduce_long",
    "inventory_unlock_reduce_short",
}


def _float(value: float) -> str:
    return f"{value:,.4f}"


def _price(value: float) -> str:
    return f"{value:.7f}"


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _parse_iso_ts(value: str | None) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"true", "1", "yes"}


def _quantity_decimal(value: Any) -> Decimal:
    try:
        return Decimal(str(value))
    except Exception:
        return Decimal("0")


def _round_order_qty_up(qty: float, step_size: float | None) -> float:
    safe_qty = max(_safe_float(qty), 0.0)
    safe_step = _safe_float(step_size)
    if safe_step <= 0:
        return safe_qty
    qty_dec = Decimal(str(safe_qty))
    step_dec = Decimal(str(safe_step))
    units = (qty_dec / step_dec).to_integral_value(rounding=ROUND_UP)
    return float(units * step_dec)


def _order_allows_min_order_qty_bump(order: dict[str, Any]) -> bool:
    if order.get("force_reduce_only") is not None and bool(order.get("force_reduce_only")):
        return False
    role = str(order.get("role", "entry")).strip().lower() or "entry"
    return role in MIN_ORDER_QTY_BUMP_ROLES


def _order_position_side(order: dict[str, Any]) -> str:
    raw = str(order.get("position_side", order.get("positionSide", "BOTH"))).upper().strip()
    return raw or "BOTH"


def _strategy_client_order_prefix(symbol: str) -> str:
    return f"gx-{str(symbol or '').lower().replace('usdt', 'u')}-"


def _is_strategy_order(order: dict[str, Any], symbol: str) -> bool:
    client_order_id = str(order.get("clientOrderId", "") or "")
    return client_order_id.startswith(_strategy_client_order_prefix(symbol))


def _order_bucket_key(side: str, price: float, position_side: str | None = None) -> str:
    normalized_position_side = str(position_side or "BOTH").upper().strip() or "BOTH"
    return f"{side.upper()}:{normalized_position_side}:{price:.10f}"


def _order_reconcile_lane(order: dict[str, Any]) -> str:
    client_order_id = str(order.get("clientOrderId", order.get("client_order_id", "")) or "").lower()
    if bool(order.get("frozen_inventory_per_lot_release")) or "-frozenpl-" in client_order_id:
        return "frozen_per_lot"
    return "default"


def _order_reconcile_bucket_key(order: dict[str, Any], *, price: float | None = None) -> str:
    side = str(order.get("side", "")).upper().strip()
    safe_price = _safe_float(order.get("price")) if price is None else _safe_float(price)
    return f"{_order_reconcile_lane(order)}:{_order_bucket_key(side, safe_price, _order_position_side(order))}"



def _resolve_reduce_only_flag(
    *,
    strategy_mode: str,
    side: str,
    role: str,
) -> bool | None:
    normalized_mode = str(strategy_mode or "").strip() or "one_way_long"
    normalized_side = str(side or "").upper().strip()
    normalized_role = str(role or "").lower().strip()
    if normalized_mode in {"hedge_neutral", "hedge_best_quote_maker_volume_v1"}:
        return None
    if normalized_role in {"grid_exit", "forced_reduce", "tail_cleanup"}:
        return True
    if normalized_side == "SELL" and normalized_role in {
        "take_profit",
        "take_profit_long",
        "active_delever_long",
        "soft_delever_long",
        "hard_delever_long",
        "flow_sleeve_long",
        "maker_reduce_long",
    }:
        return True
    if normalized_side == "BUY" and normalized_role in {
        "take_profit_short",
        "active_delever_short",
        "flow_sleeve_short",
        "maker_reduce_short",
    }:
        return True
    return None



def _clone_order_with_qty(order: dict[str, Any], qty: Decimal) -> dict[str, Any]:
    cloned = dict(order)
    qty_float = float(qty)
    cloned["qty"] = qty_float
    if "quantity" in cloned:
        cloned["quantity"] = qty_float
    if "origQty" in cloned:
        cloned["origQty"] = qty_float
    if "notional" in cloned:
        cloned["notional"] = _safe_float(cloned.get("price")) * qty_float
    return cloned


def _resize_order_to_notional(order: dict[str, Any], target_notional: float) -> dict[str, Any] | None:
    price = _safe_float(order.get("price"))
    safe_target = max(_safe_float(target_notional), 0.0)
    if price <= 0 or safe_target <= 0:
        return None
    requested_qty = _quantity_decimal(order.get("qty", order.get("quantity")))
    target_qty = Decimal(str(safe_target)) / Decimal(str(price))
    if target_qty <= Decimal("0"):
        return None
    if requested_qty > Decimal("0"):
        target_qty = min(target_qty, requested_qty)
        if requested_qty == requested_qty.to_integral_value():
            target_qty = target_qty.to_integral_value(rounding=ROUND_DOWN)
        else:
            target_qty = target_qty.quantize(requested_qty, rounding=ROUND_DOWN)
    if target_qty <= Decimal("0"):
        return None
    return _clone_order_with_qty(order, target_qty)


def _merge_place_orders_by_submitted_bucket(
    *,
    orders: list[dict[str, Any]],
    live_bid_price: float,
    live_ask_price: float,
    tick_size: float | None,
    min_qty: float | None,
    min_notional: float | None,
    step_size: float | None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    merged: list[dict[str, Any]] = []
    merged_index_by_bucket: dict[str, int] = {}
    qty_by_bucket: dict[str, Decimal] = {}
    dropped: list[dict[str, Any]] = []
    for order in orders:
        side = str(order.get("side", "")).upper().strip()
        if side not in {"BUY", "SELL"}:
            merged.append(order)
            continue
        prepared_order, _ = prepare_post_only_order_request(
            order=order,
            side=side,
            live_bid_price=live_bid_price,
            live_ask_price=live_ask_price,
            tick_size=tick_size,
            min_qty=min_qty,
            min_notional=min_notional,
            step_size=step_size,
            post_only=_order_prefers_post_only(order),
        )
        price = (
            _safe_float(prepared_order.get("submitted_price"))
            if prepared_order is not None
            else _safe_float(order.get("price"))
        )
        qty = (
            _quantity_decimal(prepared_order.get("qty"))
            if prepared_order is not None
            else _quantity_decimal(order.get("qty", order.get("quantity")))
        )
        if price <= 0 or qty <= Decimal("0"):
            merged.append(order)
            continue
        bucket = _order_reconcile_bucket_key(order, price=price)
        if bucket not in merged_index_by_bucket:
            template = dict(order)
            template["price"] = price
            merged_index_by_bucket[bucket] = len(merged)
            qty_by_bucket[bucket] = qty
            merged.append(_clone_order_with_qty(template, qty))
            continue
        qty_by_bucket[bucket] += qty
        merged[merged_index_by_bucket[bucket]] = _clone_order_with_qty(
            merged[merged_index_by_bucket[bucket]],
            qty_by_bucket[bucket],
        )
        dropped_order = dict(order)
        dropped_order["defer_reason"] = "merged_duplicate_place_bucket"
        dropped.append(dropped_order)
    return merged, dropped


def _choose_preserved_cancel_indices(
    quantities: list[Decimal],
    target_qty: Decimal,
) -> tuple[set[int], Decimal]:
    safe_target = max(Decimal("0"), target_qty)
    if safe_target <= Decimal("0") or not quantities:
        return set(), Decimal("0")

    best_indices: set[int] = set()
    best_total = Decimal("0")

    def _search(index: int, running_total: Decimal, chosen: list[int]) -> None:
        nonlocal best_indices, best_total
        if running_total > safe_target:
            return
        chosen_set = set(chosen)
        if (
            running_total > best_total
            or (running_total == best_total and len(chosen_set) > len(best_indices))
        ):
            best_total = running_total
            best_indices = chosen_set
            if best_total == safe_target and len(best_indices) == len(quantities):
                return
        if index >= len(quantities):
            return
        remaining_total = sum(quantities[index:], Decimal("0"))
        if running_total + remaining_total < best_total:
            return
        _search(index + 1, running_total + quantities[index], [*chosen, index])
        if best_total == safe_target:
            return
        _search(index + 1, running_total, chosen)

    _search(0, Decimal("0"), [])
    return best_indices, best_total


def _build_client_order_id(*, symbol: str, role: str, index: int) -> str:
    compact_symbol = symbol.lower().replace("usdt", "u")
    compact_role = role.lower().replace("_", "")[:8]
    suffix = str(int(time.time() * 1000))[-8:]
    return f"gx-{compact_symbol}-{compact_role}-{index}-{suffix}"[:36]


def _strategy_client_order_prefix(symbol: str) -> str:
    return f"gx-{str(symbol or '').lower().replace('usdt', 'u')}-"


def filter_strategy_open_orders(open_orders: Iterable[Any], symbol: str) -> list[dict[str, Any]]:
    prefix = _strategy_client_order_prefix(symbol)
    return [
        order
        for order in open_orders
        if isinstance(order, dict) and str(order.get("clientOrderId", "") or "").startswith(prefix)
    ]


def adjust_post_only_price(
    *,
    desired_price: float,
    side: str,
    live_bid_price: float,
    live_ask_price: float,
    tick_size: float | None,
) -> float:
    normalized_side = side.upper().strip()
    if normalized_side == "BUY":
        if tick_size and tick_size > 0:
            resting_ceiling = max(live_ask_price - tick_size, 0.0)
            return _round_order_price(min(desired_price, resting_ceiling), tick_size, normalized_side)
        return min(desired_price, live_bid_price)
    if tick_size and tick_size > 0:
        resting_floor = live_bid_price + tick_size
        return _round_order_price(max(desired_price, resting_floor), tick_size, normalized_side)
    return max(desired_price, live_ask_price)


def prepare_post_only_order_request(
    *,
    order: dict[str, Any],
    side: str,
    live_bid_price: float,
    live_ask_price: float,
    tick_size: float | None,
    min_qty: float | None,
    min_notional: float | None,
    step_size: float | None = None,
    post_only: bool = True,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    qty = _safe_float(order.get("qty"))
    desired_price = _safe_float(order.get("price"))
    normalized_side = side.upper().strip()
    if post_only:
        submit_price = adjust_post_only_price(
            desired_price=desired_price,
            side=normalized_side,
            live_bid_price=live_bid_price,
            live_ask_price=live_ask_price,
            tick_size=tick_size,
        )
    elif tick_size and tick_size > 0:
        submit_price = _round_order_price(desired_price, tick_size, normalized_side)
        if _authorized_frozen_manual_no_loss_bypass(order) == "manual_reduce":
            cross_distance = tick_size * FROZEN_MANUAL_REDUCE_IOC_CROSS_TICKS
            if normalized_side == "SELL" and live_bid_price > 0:
                crossed_price = _round_order_price(
                    max(live_bid_price - cross_distance, 0.0),
                    tick_size,
                    normalized_side,
                )
                submit_price = min(submit_price, crossed_price)
            elif normalized_side == "BUY" and live_ask_price > 0:
                crossed_price = _round_order_price(
                    live_ask_price + cross_distance,
                    tick_size,
                    normalized_side,
                )
                submit_price = max(submit_price, crossed_price)
    else:
        submit_price = desired_price
    submit_notional = qty * submit_price
    original_qty = qty
    qty_bumped = False
    if _order_allows_min_order_qty_bump(order) and submit_price > 0 and _safe_float(step_size) > 0:
        required_qty = qty
        if min_qty is not None and qty < float(min_qty):
            required_qty = max(required_qty, float(min_qty))
        if min_notional is not None and submit_notional < float(min_notional):
            required_qty = max(required_qty, float(min_notional) / submit_price)
        if required_qty > qty:
            qty = _round_order_qty_up(required_qty, step_size)
            submit_notional = qty * submit_price
            qty_bumped = qty > original_qty
    if qty <= 0:
        return None, {
            "reason": "non_positive_qty",
            "qty": qty,
            "desired_price": desired_price,
            "submitted_price": submit_price,
            "submitted_notional": submit_notional,
        }
    if min_qty is not None and qty < min_qty:
        return None, {
            "reason": "qty_below_min_qty",
            "qty": qty,
            "min_qty": float(min_qty),
            "desired_price": desired_price,
            "submitted_price": submit_price,
            "submitted_notional": submit_notional,
        }
    if min_notional is not None and submit_notional < min_notional:
        return None, {
            "reason": "submitted_notional_below_min_notional",
            "qty": qty,
            "min_notional": float(min_notional),
            "desired_price": desired_price,
            "submitted_price": submit_price,
            "submitted_notional": submit_notional,
        }
    return {
        "qty": qty,
        "desired_price": desired_price,
        "submitted_price": submit_price,
        "submitted_notional": submit_notional,
        "qty_bumped_to_min_order": qty_bumped,
        "original_qty": original_qty,
    }, None


def _order_prefers_post_only(order: dict[str, Any]) -> bool:
    return str(order.get("execution_type", "post_only")).strip().lower() != "aggressive"


def _order_remaining_qty(order: dict[str, Any]) -> Decimal:
    raw_qty = _quantity_decimal(order.get("origQty", order.get("qty", order.get("quantity"))))
    executed_qty = _quantity_decimal(order.get("executedQty"))
    return max(raw_qty - executed_qty, Decimal("0"))


def _order_matches_cancel(open_order: dict[str, Any], cancel_order: dict[str, Any]) -> bool:
    open_order_id = str(open_order.get("orderId", "")).strip()
    cancel_order_id = str(cancel_order.get("orderId", "")).strip()
    if open_order_id and cancel_order_id and open_order_id == cancel_order_id:
        return True
    open_client_id = str(open_order.get("clientOrderId", "")).strip()
    cancel_client_id = str(cancel_order.get("clientOrderId", "")).strip()
    return bool(open_client_id and cancel_client_id and open_client_id == cancel_client_id)


URGENT_REDUCE_ONLY_ROLES = {
    "active_delever_long",
    "active_delever_short",
    "adverse_reduce_long",
    "adverse_reduce_short",
    "hard_delever_long",
    "hard_delever_short",
    "hard_loss_forced_reduce_long",
    "hard_loss_forced_reduce_short",
    "maker_reduce_long",
    "maker_reduce_short",
    "soft_delever_long",
    "soft_delever_short",
}


def _compact_order_role(role: str) -> str:
    return str(role or "").lower().replace("_", "")[:8]


def _open_order_role(open_order: dict[str, Any]) -> str:
    explicit = str(open_order.get("role", "") or "").strip().lower()
    if explicit:
        return explicit
    client_order_id = str(open_order.get("clientOrderId", "") or "")
    parts = client_order_id.split("-")
    if len(parts) >= 3 and parts[0] == "gx":
        compact_role = parts[2].strip().lower()
        if compact_role == "takeprof":
            return "take_profit"
    return ""


def _is_urgent_reduce_only_order(order: dict[str, Any], *, strategy_mode: str) -> bool:
    if _reduce_only_cap_side(order=order, strategy_mode=strategy_mode) is None:
        return False
    if bool(order.get("frozen_inventory_per_lot_release")):
        return True
    role = str(order.get("role", "") or "").strip().lower()
    if role in URGENT_REDUCE_ONLY_ROLES:
        return True
    execution_type = str(order.get("execution_type", "") or "").strip().lower()
    return bool(order.get("force_reduce_only")) and execution_type in {"aggressive", "maker_timeout_release"}


def _place_order_template_key(order: dict[str, Any]) -> tuple[str, str, str, str, str, str, str, str]:
    side = str(order.get("side", "")).upper().strip()
    price = f"{_safe_float(order.get('price')):.10f}"
    qty = f"{_safe_float(order.get('qty', order.get('quantity'))):.10f}"
    role = str(order.get("role", "") or "").strip().lower()
    position_side = _order_position_side(order)
    force_reduce_only = str(bool(order.get("force_reduce_only"))).lower()
    execution_type = str(order.get("execution_type", "") or "").strip().lower()
    time_in_force = str(order.get("time_in_force", "") or "").strip().upper()
    return (side, price, qty, role, position_side, force_reduce_only, execution_type, time_in_force)


def _is_displaceable_reduce_only_open_order(open_order: dict[str, Any]) -> bool:
    if not _truthy(open_order.get("reduceOnly")):
        return False
    role = _open_order_role(open_order)
    if role.startswith("take_profit"):
        return True
    return _compact_order_role(role) == "takeprof"


def _reduce_only_cap_side(
    *,
    order: dict[str, Any],
    strategy_mode: str,
) -> str | None:
    side = str(order.get("side", "")).upper().strip()
    role = str(order.get("role", "entry"))
    reduce_only = _resolve_reduce_only_flag(strategy_mode=strategy_mode, side=side, role=role)
    if order.get("force_reduce_only") is not None:
        reduce_only = bool(order.get("force_reduce_only"))
    if reduce_only is not True:
        return None
    if side == "BUY":
        return "BUY"
    if side == "SELL":
        return "SELL"
    return None


def _authorized_frozen_manual_no_loss_bypass(order: dict[str, Any]) -> str | None:
    role = str(order.get("role", "") or "").strip().lower()
    manual_reduce = role.startswith("frozen_inventory_manual_reduce_") or bool(
        order.get("manual_frozen_inventory_reduce")
    )
    manual_limit = role.startswith("frozen_inventory_manual_limit_") or bool(
        order.get("manual_frozen_inventory_limit")
    )
    if not (manual_reduce or manual_limit):
        return None
    if not str(order.get("frozen_inventory_request_id", "") or "").strip():
        return None
    return "manual_reduce" if manual_reduce else "manual_limit"


def cap_reduce_only_place_orders_to_position(
    *,
    actions: dict[str, Any],
    strategy_mode: str,
    current_actual_net_qty: float,
    current_open_orders: list[dict[str, Any]],
) -> dict[str, Any]:
    """Cap one-way reduce-only places so TP and delever orders cannot over-close the live net position."""
    place_orders = [dict(item) for item in actions.get("place_orders", []) if isinstance(item, dict)]
    cancel_orders = [dict(item) for item in actions.get("cancel_orders", []) if isinstance(item, dict)]
    normalized_mode = str(strategy_mode or "").strip() or "one_way_long"
    if normalized_mode in {"hedge_neutral", "hedge_best_quote_maker_volume_v1"} or not place_orders:
        result = dict(actions)
        result["place_orders"] = place_orders
        result["cancel_orders"] = cancel_orders
        result["place_count"] = len(place_orders)
        result["cancel_count"] = len(cancel_orders)
        result["place_notional"] = sum(_safe_float(item.get("notional")) for item in place_orders)
        return result

    net_qty = Decimal(str(current_actual_net_qty))
    available_qty_by_side = {
        "BUY": max(-net_qty, Decimal("0")),
        "SELL": max(net_qty, Decimal("0")),
    }
    urgent_sides = {
        side
        for side in (
            _reduce_only_cap_side(order=order, strategy_mode=normalized_mode)
            for order in place_orders
            if _is_urgent_reduce_only_order(order, strategy_mode=normalized_mode)
        )
        if side is not None
    }
    displaced_reduce_only_orders: list[dict[str, Any]] = []
    for open_order in current_open_orders:
        if not isinstance(open_order, dict):
            continue
        if not _truthy(open_order.get("reduceOnly")):
            continue
        side = str(open_order.get("side", "")).upper().strip()
        if side in available_qty_by_side:
            if side in urgent_sides and _is_displaceable_reduce_only_open_order(open_order):
                if not any(_order_matches_cancel(open_order, existing) for existing in cancel_orders):
                    displaced = dict(open_order)
                    displaced["cancel_reason"] = "urgent_reduce_only_displaces_take_profit"
                    cancel_orders.append(displaced)
                    displaced_reduce_only_orders.append(displaced)
                continue
            # Binance only releases reduce-only capacity after the exchange confirms
            # cancellation, so pending-cancel orders must still count this cycle.
            available_qty_by_side[side] = max(
                available_qty_by_side[side] - _order_remaining_qty(open_order),
                Decimal("0"),
            )

    capped_place_orders: list[dict[str, Any]] = []
    dropped_count = 0
    resized_count = 0
    for order in place_orders:
        cap_side = _reduce_only_cap_side(order=order, strategy_mode=normalized_mode)
        if cap_side is None:
            capped_place_orders.append(order)
            continue
        available_qty = available_qty_by_side.get(cap_side, Decimal("0"))
        requested_qty = _quantity_decimal(order.get("qty"))
        if requested_qty <= Decimal("0") or available_qty <= Decimal("0"):
            dropped_count += 1
            continue
        if requested_qty <= available_qty:
            capped_place_orders.append(order)
            available_qty_by_side[cap_side] = max(available_qty - requested_qty, Decimal("0"))
            continue
        capped_place_orders.append(_clone_order_with_qty(order, available_qty))
        available_qty_by_side[cap_side] = Decimal("0")
        resized_count += 1

    result = dict(actions)
    result["place_orders"] = capped_place_orders
    result["cancel_orders"] = cancel_orders
    result["place_count"] = len(capped_place_orders)
    result["cancel_count"] = len(cancel_orders)
    result["place_notional"] = sum(_safe_float(item.get("notional")) for item in capped_place_orders)
    result["reduce_only_position_cap"] = {
        "enabled": True,
        "current_actual_net_qty": float(net_qty),
        "remaining_buy_reduce_qty": float(available_qty_by_side["BUY"]),
        "remaining_sell_reduce_qty": float(available_qty_by_side["SELL"]),
        "dropped_order_count": dropped_count,
        "resized_order_count": resized_count,
        "displaced_order_count": len(displaced_reduce_only_orders),
        "displaced_orders": displaced_reduce_only_orders,
    }
    return result


def apply_anti_chase_entry_guard_to_actions(
    *,
    actions: dict[str, Any],
    plan_report: dict[str, Any],
    strategy_mode: str,
) -> dict[str, Any]:
    """Drop new entry orders that would add risk in the same direction as a fast move."""
    guard = plan_report.get("anti_chase_entry_guard")
    if not isinstance(guard, dict) or not _truthy(guard.get("enabled")):
        return actions

    block_long_entries = bool(guard.get("block_long_entries"))
    block_short_entries = bool(guard.get("block_short_entries"))
    if not block_long_entries and not block_short_entries:
        return actions

    normalized_mode = str(strategy_mode or "").strip() or "one_way_long"
    kept_place_orders: list[dict[str, Any]] = []
    dropped_orders: list[dict[str, Any]] = []
    for order in [dict(item) for item in actions.get("place_orders", []) if isinstance(item, dict)]:
        side = str(order.get("side", "")).upper().strip()
        role = str(order.get("role", "entry")).strip()
        reduce_only = _resolve_reduce_only_flag(strategy_mode=normalized_mode, side=side, role=role)
        if order.get("force_reduce_only") is not None:
            reduce_only = bool(order.get("force_reduce_only"))

        should_drop = False
        reason = None
        if reduce_only is not True:
            if side == "BUY" and block_long_entries:
                should_drop = True
                reason = guard.get("long_reason") or guard.get("reason") or "anti_chase_up"
            elif side == "SELL" and block_short_entries:
                should_drop = True
                reason = guard.get("short_reason") or guard.get("reason") or "anti_chase_down"

        if should_drop:
            dropped = dict(order)
            dropped["anti_chase_drop_reason"] = reason
            dropped_orders.append(dropped)
        else:
            kept_place_orders.append(order)

    result = dict(actions)
    result["place_orders"] = kept_place_orders
    result["place_count"] = len(kept_place_orders)
    result["place_notional"] = sum(_safe_float(item.get("notional")) for item in kept_place_orders)
    result["anti_chase_entry_guard"] = {
        "enabled": True,
        "block_long_entries": block_long_entries,
        "block_short_entries": block_short_entries,
        "dropped_order_count": len(dropped_orders),
        "dropped_orders": dropped_orders,
    }
    return result


def apply_loss_inventory_no_cross_entry_guard_to_actions(
    *,
    actions: dict[str, Any],
    plan_report: dict[str, Any],
    strategy_mode: str,
) -> dict[str, Any]:
    """Prevent losing inventory recovery from turning into a fresh opposite entry."""
    normalized_mode = str(strategy_mode or "").strip() or "one_way_long"
    if normalized_mode in {"hedge_neutral", "hedge_best_quote_maker_volume_v1"}:
        return actions

    net_qty = _safe_float(plan_report.get("actual_net_qty"))
    unrealized_pnl = _safe_float(plan_report.get("unrealized_pnl"))
    min_profit_ratio = max(_safe_float(plan_report.get("take_profit_min_profit_ratio")), 0.0)
    if min_profit_ratio <= 0:
        guard = plan_report.get("take_profit_guard")
        if isinstance(guard, dict):
            min_profit_ratio = max(_safe_float(guard.get("effective_min_profit_ratio")), 0.0)

    short_avg = max(_safe_float(plan_report.get("current_short_avg_price")), 0.0)
    long_avg = max(_safe_float(plan_report.get("current_long_avg_price")), 0.0)
    short_ceiling = short_avg * (1.0 - min_profit_ratio) if short_avg > 0 else 0.0
    long_floor = long_avg * (1.0 + min_profit_ratio) if long_avg > 0 else 0.0
    step_price = max(
        _safe_float((plan_report.get("adaptive_step") or {}).get("effective_step_price")),
        _safe_float(plan_report.get("effective_step_price")),
        _safe_float(plan_report.get("step_price")),
        0.0,
    )
    mid_price = max(_safe_float(plan_report.get("mid_price")), 0.0)
    market_guard = plan_report.get("market_guard") if isinstance(plan_report.get("market_guard"), dict) else {}
    market_return_ratio = _safe_float(market_guard.get("return_ratio"))
    adverse_trend_threshold_ratio = (step_price / mid_price) if step_price > 0 and mid_price > 0 else 0.0
    long_same_side_entry_ceiling = (
        max(long_avg - step_price, 0.0)
        if step_price > 0 and long_avg > 0
        else long_avg
    )
    short_same_side_entry_floor = short_avg + step_price if step_price > 0 and short_avg > 0 else short_avg
    small_entry_notional = max(
        _safe_float(plan_report.get("loss_inventory_no_cross_small_entry_notional")),
        0.0,
    )
    loss_recovery_brush = plan_report.get("loss_recovery_brush")
    brush_allows_ordinary_recovery = bool(
        isinstance(loss_recovery_brush, dict)
        and _truthy(loss_recovery_brush.get("active"))
        and _safe_float(loss_recovery_brush.get("entry_notional")) > 0
    )
    symbol_info = plan_report.get("symbol_info") if isinstance(plan_report.get("symbol_info"), dict) else {}
    min_order_notional = max(_safe_float(symbol_info.get("min_notional")), 0.0)

    losing_short = net_qty < -1e-12 and unrealized_pnl < -1e-9 and short_ceiling > 0
    losing_long = net_qty > 1e-12 and unrealized_pnl < -1e-9 and long_floor > 0
    if not losing_short and not losing_long:
        return actions

    kept_place_orders: list[dict[str, Any]] = []
    dropped_orders: list[dict[str, Any]] = []
    converted_orders: list[dict[str, Any]] = []
    allowed_small_entry_orders: list[dict[str, Any]] = []
    resized_small_loss_reduce_orders: list[dict[str, Any]] = []
    allowed_same_side_entry_orders: list[dict[str, Any]] = []
    for order in [dict(item) for item in actions.get("place_orders", []) if isinstance(item, dict)]:
        side = str(order.get("side", "")).upper().strip()
        price = _safe_float(order.get("price"))
        order_notional = abs(_safe_float(order.get("notional")))
        entry_side = _entry_side_from_order(order, strategy_mode=normalized_mode)
        reduce_side = _reduce_only_cap_side(order=order, strategy_mode=normalized_mode)
        role = str(order.get("role", "") or "").strip().lower()
        hard_loss_forced_reduce = role in {"hard_loss_forced_reduce_long", "hard_loss_forced_reduce_short"}
        implicit_loss_reduce_side = None
        if not hard_loss_forced_reduce:
            if losing_short and side == "BUY" and net_qty < -1e-12:
                implicit_loss_reduce_side = "BUY"
            elif losing_long and side == "SELL" and net_qty > 1e-12:
                implicit_loss_reduce_side = "SELL"
        ordinary_entry = entry_side in {"long", "short"} and reduce_side is None
        explicit_loss_reduce = reduce_side is not None
        implicit_loss_reduce = implicit_loss_reduce_side is not None
        best_quote_implicit_loss_reduce = implicit_loss_reduce and ordinary_entry and role in {
            "best_quote_entry_long",
            "best_quote_entry_short",
        }
        small_loss_reduce_allowed = (
            (
                explicit_loss_reduce
                or (
                    implicit_loss_reduce
                    and (
                        not ordinary_entry
                        or best_quote_implicit_loss_reduce
                        or brush_allows_ordinary_recovery
                    )
                )
            )
            and small_entry_notional > 0
            and order_notional <= small_entry_notional + 1e-9
        )
        small_loss_reduce_resize_allowed = (
            (
                explicit_loss_reduce
                or (
                    implicit_loss_reduce
                    and (not ordinary_entry or brush_allows_ordinary_recovery)
                )
            )
            and small_entry_notional > 0
            and order_notional > small_entry_notional + 1e-9
        )
        short_recovery_order = entry_side == "long" or (
            reduce_side == "BUY" and not hard_loss_forced_reduce
        ) or implicit_loss_reduce_side == "BUY"
        long_recovery_order = entry_side == "short" or (
            reduce_side == "SELL" and not hard_loss_forced_reduce
        ) or implicit_loss_reduce_side == "SELL"
        if losing_short and entry_side == "short" and side == "SELL":
            adverse_uptrend = (
                adverse_trend_threshold_ratio > 0
                and market_return_ratio > adverse_trend_threshold_ratio
            )
            if price <= 0 or price + 1e-12 < short_same_side_entry_floor:
                dropped = dict(order)
                dropped["loss_inventory_no_cross_drop_reason"] = "losing_short_sell_below_entry_floor"
                dropped["loss_inventory_same_side_entry_floor"] = short_same_side_entry_floor
                dropped_orders.append(dropped)
                continue
            if adverse_uptrend:
                dropped = dict(order)
                dropped["loss_inventory_no_cross_drop_reason"] = "losing_short_adverse_uptrend"
                dropped["loss_inventory_market_return_ratio"] = market_return_ratio
                dropped["loss_inventory_adverse_trend_threshold_ratio"] = adverse_trend_threshold_ratio
                dropped_orders.append(dropped)
                continue
            order["loss_inventory_no_cross_guard"] = "short_same_side_entry_allowed"
            order["loss_inventory_same_side_entry_floor"] = short_same_side_entry_floor
            allowed_same_side_entry_orders.append(dict(order))
            kept_place_orders.append(order)
            continue
        if losing_long and entry_side == "long" and side == "BUY":
            adverse_downtrend = (
                adverse_trend_threshold_ratio > 0
                and market_return_ratio < -adverse_trend_threshold_ratio
            )
            if price <= 0 or price > long_same_side_entry_ceiling + 1e-12:
                dropped = dict(order)
                dropped["loss_inventory_no_cross_drop_reason"] = "losing_long_buy_above_entry_ceiling"
                dropped["loss_inventory_same_side_entry_ceiling"] = long_same_side_entry_ceiling
                dropped_orders.append(dropped)
                continue
            if adverse_downtrend:
                dropped = dict(order)
                dropped["loss_inventory_no_cross_drop_reason"] = "losing_long_adverse_downtrend"
                dropped["loss_inventory_market_return_ratio"] = market_return_ratio
                dropped["loss_inventory_adverse_trend_threshold_ratio"] = adverse_trend_threshold_ratio
                dropped_orders.append(dropped)
                continue
            order["loss_inventory_no_cross_guard"] = "long_same_side_entry_allowed"
            order["loss_inventory_same_side_entry_ceiling"] = long_same_side_entry_ceiling
            allowed_same_side_entry_orders.append(dict(order))
            kept_place_orders.append(order)
            continue
        if losing_short and short_recovery_order and side == "BUY":
            dust_target_notional = min(order_notional, abs(net_qty) * price) if price > 0 else order_notional
            if (
                implicit_loss_reduce
                and ordinary_entry
                and small_entry_notional > 0
                and order_notional <= small_entry_notional + 1e-9
                and min_order_notional > 0
                and dust_target_notional + 1e-9 < min_order_notional
            ):
                order["loss_inventory_no_cross_guard"] = "short_dust_cross_allowed"
                order["loss_inventory_min_notional"] = min_order_notional
                order["loss_inventory_target_notional"] = dust_target_notional
                order["loss_inventory_small_entry_notional_limit"] = small_entry_notional
                allowed_small_entry_orders.append(dict(order))
                kept_place_orders.append(order)
                continue
            if price > 0 and price <= short_ceiling:
                if entry_side == "long" or implicit_loss_reduce_side is not None:
                    order["force_reduce_only"] = True
                order["loss_inventory_no_cross_guard"] = "short_recover_no_cross"
                converted_orders.append(dict(order))
                kept_place_orders.append(order)
            else:
                if implicit_loss_reduce and ordinary_entry and small_entry_notional > 0:
                    target_notional = min(order_notional, abs(net_qty) * price)
                    if (
                        order_notional <= small_entry_notional + 1e-9
                        and min_order_notional > 0
                        and target_notional + 1e-9 < min_order_notional
                    ):
                        order["loss_inventory_no_cross_guard"] = "short_dust_cross_allowed"
                        order["loss_inventory_min_notional"] = min_order_notional
                        order["loss_inventory_target_notional"] = target_notional
                        order["loss_inventory_small_entry_notional_limit"] = small_entry_notional
                        allowed_small_entry_orders.append(dict(order))
                        kept_place_orders.append(order)
                        continue
                if small_loss_reduce_allowed:
                    if implicit_loss_reduce:
                        target_notional = min(order_notional, abs(net_qty) * price)
                        if min_order_notional > 0 and target_notional + 1e-9 < min_order_notional:
                            dropped = dict(order)
                            dropped["loss_inventory_no_cross_drop_reason"] = "losing_short_small_reduce_below_min_notional"
                            dropped["loss_inventory_min_notional"] = min_order_notional
                            dropped["loss_inventory_target_notional"] = target_notional
                            dropped_orders.append(dropped)
                            continue
                        resized = _resize_order_to_notional(order, target_notional)
                        if resized is None:
                            dropped = dict(order)
                            dropped["loss_inventory_no_cross_drop_reason"] = "losing_short_small_reduce_resize_failed"
                            dropped["loss_inventory_target_notional"] = target_notional
                            dropped_orders.append(dropped)
                            continue
                        order = resized
                        order["force_reduce_only"] = True
                    order["loss_inventory_no_cross_guard"] = "short_small_loss_reduce_allowed"
                    order["loss_inventory_small_entry_notional_limit"] = small_entry_notional
                    allowed_small_entry_orders.append(dict(order))
                    kept_place_orders.append(order)
                    continue
                if small_loss_reduce_resize_allowed:
                    target_notional = small_entry_notional
                    if implicit_loss_reduce:
                        target_notional = min(target_notional, abs(net_qty) * price)
                    if min_order_notional > 0 and target_notional + 1e-9 < min_order_notional:
                        dropped = dict(order)
                        dropped["loss_inventory_no_cross_drop_reason"] = "losing_short_small_reduce_below_min_notional"
                        dropped["loss_inventory_min_notional"] = min_order_notional
                        dropped["loss_inventory_target_notional"] = target_notional
                        dropped_orders.append(dropped)
                        continue
                    resized = _resize_order_to_notional(order, target_notional)
                    if resized is not None:
                        if implicit_loss_reduce:
                            resized["force_reduce_only"] = True
                        resized["loss_inventory_no_cross_guard"] = "short_small_loss_reduce_resized"
                        resized["loss_inventory_small_entry_notional_limit"] = small_entry_notional
                        resized["loss_inventory_original_notional"] = order_notional
                        allowed_small_entry_orders.append(dict(resized))
                        resized_small_loss_reduce_orders.append(dict(resized))
                        kept_place_orders.append(resized)
                        continue
                dropped = dict(order)
                dropped["loss_inventory_no_cross_drop_reason"] = "losing_short_buy_above_recovery_ceiling"
                dropped["loss_inventory_recovery_ceiling"] = short_ceiling
                dropped_orders.append(dropped)
            continue
        if losing_long and long_recovery_order and side == "SELL":
            dust_target_notional = min(order_notional, abs(net_qty) * price) if price > 0 else order_notional
            if (
                implicit_loss_reduce
                and ordinary_entry
                and small_entry_notional > 0
                and order_notional <= small_entry_notional + 1e-9
                and min_order_notional > 0
                and dust_target_notional + 1e-9 < min_order_notional
            ):
                order["loss_inventory_no_cross_guard"] = "long_dust_cross_allowed"
                order["loss_inventory_min_notional"] = min_order_notional
                order["loss_inventory_target_notional"] = dust_target_notional
                order["loss_inventory_small_entry_notional_limit"] = small_entry_notional
                allowed_small_entry_orders.append(dict(order))
                kept_place_orders.append(order)
                continue
            if price > 0 and price >= long_floor:
                if entry_side == "short" or implicit_loss_reduce_side is not None:
                    order["force_reduce_only"] = True
                order["loss_inventory_no_cross_guard"] = "long_recover_no_cross"
                converted_orders.append(dict(order))
                kept_place_orders.append(order)
            else:
                if implicit_loss_reduce and ordinary_entry and small_entry_notional > 0:
                    target_notional = min(order_notional, abs(net_qty) * price)
                    if (
                        order_notional <= small_entry_notional + 1e-9
                        and min_order_notional > 0
                        and target_notional + 1e-9 < min_order_notional
                    ):
                        order["loss_inventory_no_cross_guard"] = "long_dust_cross_allowed"
                        order["loss_inventory_min_notional"] = min_order_notional
                        order["loss_inventory_target_notional"] = target_notional
                        order["loss_inventory_small_entry_notional_limit"] = small_entry_notional
                        allowed_small_entry_orders.append(dict(order))
                        kept_place_orders.append(order)
                        continue
                if small_loss_reduce_allowed:
                    if implicit_loss_reduce:
                        target_notional = min(order_notional, abs(net_qty) * price)
                        if min_order_notional > 0 and target_notional + 1e-9 < min_order_notional:
                            dropped = dict(order)
                            dropped["loss_inventory_no_cross_drop_reason"] = "losing_long_small_reduce_below_min_notional"
                            dropped["loss_inventory_min_notional"] = min_order_notional
                            dropped["loss_inventory_target_notional"] = target_notional
                            dropped_orders.append(dropped)
                            continue
                        resized = _resize_order_to_notional(order, target_notional)
                        if resized is None:
                            dropped = dict(order)
                            dropped["loss_inventory_no_cross_drop_reason"] = "losing_long_small_reduce_resize_failed"
                            dropped["loss_inventory_target_notional"] = target_notional
                            dropped_orders.append(dropped)
                            continue
                        order = resized
                        order["force_reduce_only"] = True
                    order["loss_inventory_no_cross_guard"] = "long_small_loss_reduce_allowed"
                    order["loss_inventory_small_entry_notional_limit"] = small_entry_notional
                    allowed_small_entry_orders.append(dict(order))
                    kept_place_orders.append(order)
                    continue
                if small_loss_reduce_resize_allowed:
                    target_notional = small_entry_notional
                    if implicit_loss_reduce:
                        target_notional = min(target_notional, abs(net_qty) * price)
                    if min_order_notional > 0 and target_notional + 1e-9 < min_order_notional:
                        dropped = dict(order)
                        dropped["loss_inventory_no_cross_drop_reason"] = "losing_long_small_reduce_below_min_notional"
                        dropped["loss_inventory_min_notional"] = min_order_notional
                        dropped["loss_inventory_target_notional"] = target_notional
                        dropped_orders.append(dropped)
                        continue
                    resized = _resize_order_to_notional(order, target_notional)
                    if resized is not None:
                        if implicit_loss_reduce:
                            resized["force_reduce_only"] = True
                        resized["loss_inventory_no_cross_guard"] = "long_small_loss_reduce_resized"
                        resized["loss_inventory_small_entry_notional_limit"] = small_entry_notional
                        resized["loss_inventory_original_notional"] = order_notional
                        allowed_small_entry_orders.append(dict(resized))
                        resized_small_loss_reduce_orders.append(dict(resized))
                        kept_place_orders.append(resized)
                        continue
                dropped = dict(order)
                dropped["loss_inventory_no_cross_drop_reason"] = "losing_long_sell_below_recovery_floor"
                dropped["loss_inventory_recovery_floor"] = long_floor
                dropped_orders.append(dropped)
            continue
        kept_place_orders.append(order)

    result = dict(actions)
    result["place_orders"] = kept_place_orders
    result["place_count"] = len(kept_place_orders)
    result["place_notional"] = sum(_safe_float(item.get("notional")) for item in kept_place_orders)
    result["loss_inventory_no_cross_entry_guard"] = {
        "enabled": True,
        "active": bool(losing_short or losing_long),
        "direction": "short" if losing_short else "long",
        "actual_net_qty": net_qty,
        "unrealized_pnl": unrealized_pnl,
        "min_profit_ratio": min_profit_ratio,
        "short_recovery_ceiling": short_ceiling if losing_short else None,
        "long_recovery_floor": long_floor if losing_long else None,
        "step_price": step_price,
        "market_return_ratio": market_return_ratio,
        "adverse_trend_threshold_ratio": adverse_trend_threshold_ratio,
        "short_same_side_entry_floor": short_same_side_entry_floor if losing_short else None,
        "long_same_side_entry_ceiling": long_same_side_entry_ceiling if losing_long else None,
        "small_entry_notional_limit": small_entry_notional,
        "allowed_same_side_entry_count": len(allowed_same_side_entry_orders),
        "allowed_same_side_entry_orders": allowed_same_side_entry_orders,
        "allowed_small_entry_count": len(allowed_small_entry_orders),
        "allowed_small_entry_orders": allowed_small_entry_orders,
        "resized_small_loss_reduce_count": len(resized_small_loss_reduce_orders),
        "resized_small_loss_reduce_orders": resized_small_loss_reduce_orders,
        "converted_order_count": len(converted_orders),
        "dropped_order_count": len(dropped_orders),
        "converted_orders": converted_orders,
        "dropped_orders": dropped_orders,
    }
    return result


def apply_reduce_only_no_loss_guard_to_actions(
    *,
    actions: dict[str, Any],
    plan_report: dict[str, Any],
    strategy_mode: str,
    live_bid_price: float,
    live_ask_price: float,
    tick_size: float | None,
    min_qty: float | None,
    min_notional: float | None,
    step_size: float | None = None,
    enabled: bool = True,
    allow_loss_roles: set[str] | None = None,
) -> dict[str, Any]:
    """Drop reduce-only places that would submit beyond the no-loss floor/ceiling."""
    place_orders = [dict(item) for item in actions.get("place_orders", []) if isinstance(item, dict)]
    if not place_orders:
        result = dict(actions)
        result["place_orders"] = place_orders
        result["place_count"] = 0
        result["place_notional"] = 0.0
        return result
    if not enabled:
        result = dict(actions)
        result["place_orders"] = place_orders
        result["place_count"] = len(place_orders)
        result["place_notional"] = sum(_safe_float(item.get("notional")) for item in place_orders)
        result["reduce_only_no_loss_guard"] = {
            "enabled": False,
            "dropped_order_count": 0,
            "dropped_orders": [],
        }
        return result

    guard = plan_report.get("take_profit_guard") if isinstance(plan_report.get("take_profit_guard"), dict) else {}
    min_profit_ratio = max(
        _safe_float(guard.get("effective_min_profit_ratio")),
        _safe_float(plan_report.get("take_profit_min_profit_ratio")),
        0.0,
    )
    long_floor = max(_safe_float(guard.get("long_floor_price")), 0.0)
    if long_floor <= 0:
        long_avg = max(_safe_float(plan_report.get("current_long_avg_price")), 0.0)
        if long_avg > 0:
            long_floor = long_avg * (1.0 + min_profit_ratio)
    short_ceiling = max(_safe_float(guard.get("short_ceiling_price")), 0.0)
    if short_ceiling <= 0:
        short_avg = max(_safe_float(plan_report.get("current_short_avg_price")), 0.0)
        if short_avg > 0:
            short_ceiling = short_avg * (1.0 - min_profit_ratio)

    allowed_loss_roles = {str(item).strip().lower() for item in (allow_loss_roles or set()) if str(item).strip()}
    kept_place_orders: list[dict[str, Any]] = []
    dropped_orders: list[dict[str, Any]] = []
    for order in place_orders:
        side = str(order.get("side", "")).upper().strip()
        role = str(order.get("role", "") or "").strip().lower()
        if role.startswith("frozen_inventory_pair_release_") or bool(order.get("frozen_inventory_pair_release")):
            order["reduce_only_no_loss_guard"] = "bypassed_frozen_pair_release"
            kept_place_orders.append(order)
            continue
        frozen_manual_bypass = _authorized_frozen_manual_no_loss_bypass(order)
        if frozen_manual_bypass is not None:
            order["reduce_only_no_loss_guard"] = f"bypassed_frozen_inventory_{frozen_manual_bypass}"
            kept_place_orders.append(order)
            continue
        if role in {"hard_loss_forced_reduce_long", "hard_loss_forced_reduce_short"}:
            kept_place_orders.append(order)
            continue
        reduce_side = _reduce_only_cap_side(order=order, strategy_mode=strategy_mode)
        if reduce_side not in {"BUY", "SELL"}:
            kept_place_orders.append(order)
            continue
        prepared_order, skip_reason = prepare_post_only_order_request(
            order=order,
            side=side,
            live_bid_price=live_bid_price,
            live_ask_price=live_ask_price,
            tick_size=tick_size,
            min_qty=min_qty,
            min_notional=min_notional,
            step_size=step_size,
            post_only=_order_prefers_post_only(order),
        )
        submitted_price = (
            _safe_float(prepared_order.get("submitted_price"))
            if prepared_order is not None
            else _safe_float(order.get("price"))
        )
        drop_reason = ""
        guard_price = None
        if reduce_side == "BUY":
            guard_price = short_ceiling if short_ceiling > 0 else None
            if short_ceiling > 0 and (submitted_price <= 0 or submitted_price > short_ceiling + 1e-12):
                drop_reason = "short_reduce_above_no_loss_ceiling"
        elif reduce_side == "SELL":
            guard_price = long_floor if long_floor > 0 else None
            if long_floor > 0 and (submitted_price <= 0 or submitted_price + 1e-12 < long_floor):
                drop_reason = "long_reduce_below_no_loss_floor"
        if drop_reason:
            if role in allowed_loss_roles:
                order["reduce_only_no_loss_guard"] = f"bypassed_allow_loss_role_{role}"
                order["reduce_only_no_loss_guard_price"] = guard_price
                order["reduce_only_no_loss_submitted_price"] = submitted_price
                kept_place_orders.append(order)
                continue
            dropped = dict(order)
            dropped["reduce_only_no_loss_drop_reason"] = drop_reason
            dropped["reduce_only_no_loss_guard_price"] = guard_price
            dropped["reduce_only_no_loss_submitted_price"] = submitted_price
            if skip_reason:
                dropped["reduce_only_no_loss_prepare_skip_reason"] = skip_reason
            dropped_orders.append(dropped)
            continue
        order["reduce_only_no_loss_guard"] = "passed"
        order["reduce_only_no_loss_guard_price"] = guard_price
        order["reduce_only_no_loss_submitted_price"] = submitted_price
        kept_place_orders.append(order)

    result = dict(actions)
    result["place_orders"] = kept_place_orders
    result["place_count"] = len(kept_place_orders)
    result["place_notional"] = sum(_safe_float(item.get("notional")) for item in kept_place_orders)
    result["reduce_only_no_loss_guard"] = {
        "enabled": True,
        "long_floor_price": long_floor if long_floor > 0 else None,
        "short_ceiling_price": short_ceiling if short_ceiling > 0 else None,
        "dropped_order_count": len(dropped_orders),
        "dropped_orders": dropped_orders,
    }
    return result


def _entry_side_from_order(order: dict[str, Any], *, strategy_mode: str) -> str | None:
    if _reduce_only_cap_side(order=order, strategy_mode=strategy_mode) is not None:
        return None
    side = str(order.get("side", "")).upper().strip()
    role = str(order.get("role", "entry")).strip().lower()
    if role in {"entry_short", "bootstrap_short", "grid_entry_short", "best_quote_entry_short"}:
        return "short"
    if role in {"entry_long", "bootstrap_long", "grid_entry_long", "best_quote_entry_long"}:
        return "long"
    if role in {"entry", "bootstrap", "grid_entry"}:
        if side == "SELL":
            return "short"
        if side == "BUY":
            return "long"
    return None


def _hedge_entry_side_from_order(order: dict[str, Any]) -> str | None:
    """Return the hedge-side exposure an order can add, if any."""

    if _truthy(order.get("force_reduce_only")) or _truthy(order.get("reduceOnly")):
        return None
    side = str(order.get("side", "")).upper().strip()
    position_side = _order_position_side(order)
    if position_side == "LONG" and side == "BUY":
        return "long"
    if position_side == "SHORT" and side == "SELL":
        return "short"
    return None


def _order_notional(order: dict[str, Any]) -> float:
    notional = abs(_safe_float(order.get("notional")))
    if notional > 0:
        return notional
    return abs(_safe_float(order.get("price"))) * abs(
        _safe_float(
            order.get("origQty", order.get("qty", order.get("quantity")))
        )
    )


def _is_gtx_candidate(order: dict[str, Any]) -> bool:
    if not _order_prefers_post_only(order):
        return False
    return str(
        order.get("time_in_force", order.get("timeInForce", "GTX")) or "GTX"
    ).upper().strip() == "GTX"


def is_frozen_inventory_order(order: Mapping[str, Any]) -> bool:
    """Return whether an order belongs to the independent frozen ledger."""

    role = str(order.get("role") or "").lower().strip()
    book = str(order.get("book") or order.get("ledger_class") or "").lower().strip()
    explicitly_frozen = bool(
        book == "frozen_bq"
        or role.startswith("frozen_inventory_")
        or order.get("manual_frozen_inventory_reduce")
        or order.get("manual_frozen_inventory_limit")
        or order.get("frozen_inventory_pair_release")
        or order.get("frozen_inventory_per_lot_release")
    )
    if not explicitly_frozen:
        return False
    # Exchange/open-order rows are untrusted observations.  A role-looking or
    # legacy client ID must not remove them from ordinary risk projection unless
    # the runner bound the row to a durable frozen order ref.  Directive
    # authorization is deliberately separate: an expired or damaged directive
    # can block a frozen mutation, but it cannot transfer ownership back to the
    # ordinary ledger.
    exchange_identity = bool(
        str(order.get("orderId") or order.get("order_id") or "").strip()
        or str(
            order.get("clientOrderId")
            or order.get("client_order_id")
            or order.get("origClientOrderId")
            or ""
        ).strip()
    )
    return bool(
        explicitly_frozen
        and (
            not exchange_identity
            or _truthy(order.get("frozen_inventory_authorization_validated"))
            or _truthy(order.get("frozen_inventory_ownership_validated"))
        )
    )


def _frozen_order_is_gtx_maker(order: Mapping[str, Any]) -> bool:
    return bool(
        str(order.get("execution_type") or "").lower().strip()
        in {"maker", "post_only"}
        and str(order.get("time_in_force") or order.get("timeInForce") or "")
        .upper()
        .strip()
        == "GTX"
        and _truthy(order.get("post_only"))
    )


def _valid_frozen_pair_group(orders: list[dict[str, Any]]) -> bool:
    if not orders:
        return True
    if not all(_frozen_order_is_gtx_maker(order) for order in orders):
        return False
    request_ids = {
        str(order.get("frozen_inventory_request_id") or "").strip()
        for order in orders
    }
    if len(request_ids) != 1 or "" in request_ids:
        return False
    roles = [str(order.get("role") or "").lower().strip() for order in orders]
    if len(orders) == 1:
        expected_repair = (
            "LONG"
            if roles[0] == "frozen_inventory_pair_release_long"
            else "SHORT"
            if roles[0] == "frozen_inventory_pair_release_short"
            else ""
        )
        return bool(
            expected_repair
            and str(orders[0].get("frozen_inventory_pair_repair_side") or "")
            .upper()
            .strip()
            == expected_repair
        )
    if len(orders) != 2 or set(roles) != {
        "frozen_inventory_pair_release_long",
        "frozen_inventory_pair_release_short",
    }:
        return False
    if any(order.get("frozen_inventory_pair_repair_side") for order in orders):
        return False
    quantities = [_safe_float(order.get("qty", order.get("quantity"))) for order in orders]
    return bool(
        quantities[0] > 0
        and quantities[1] > 0
        and math.isclose(quantities[0], quantities[1], rel_tol=1e-12, abs_tol=1e-12)
    )


def apply_actual_net_exposure_decision_to_actions(
    *,
    actions: dict[str, Any],
    current_actual_net_qty: float,
    valuation_price: float,
    current_open_orders: Iterable[Any],
    max_actual_net_notional: float,
    owned_open_orders: Iterable[Any] | None = None,
) -> dict[str, Any]:
    """Make the final, single action decision for ordinary net exposure.

    The caller supplies normal inventory after subtracting the independent
    frozen ledger.  Frozen positions and frozen orders never participate in
    this cap.  For ordinary inventory BUY increases LONG-SHORT net and SELL
    decreases it, including hedge-mode SHORT reductions.  Opposite-side orders
    therefore may not offset each other in the projection.  If live ordinary
    orders can cross the cap, this round only cancels the exact risk direction.
    Otherwise a breached or newly projected cap keeps at most one GTX order
    that reduces absolute ordinary net.  All earlier pause, ownership and loss
    gates remain authoritative candidates; this function never synthesizes an
    order or opens loss permission.
    """

    def finite_number(value: Any) -> float | None:
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            return None
        return parsed if math.isfinite(parsed) else None

    parsed_cap = finite_number(max_actual_net_notional)
    parsed_mark = finite_number(valuation_price)
    parsed_current_qty = finite_number(current_actual_net_qty)
    invalid_fields = [
        name
        for name, value in (
            ("max_actual_net_notional", parsed_cap),
            ("valuation_price", parsed_mark),
            ("current_actual_net_qty", parsed_current_qty),
        )
        if value is None
    ]
    cap = max(parsed_cap or 0.0, 0.0)
    mark_price = max(parsed_mark or 0.0, 0.0)
    current_qty = parsed_current_qty or 0.0
    current = current_qty * mark_price
    all_place_orders = [
        dict(item)
        for item in actions.get("place_orders", [])
        if isinstance(item, dict)
    ]
    all_cancel_orders = [
        dict(item)
        for item in actions.get("cancel_orders", [])
        if isinstance(item, dict)
    ]
    place_orders = [
        item for item in all_place_orders if not is_frozen_inventory_order(item)
    ]
    cancel_orders = [
        item for item in all_cancel_orders if not is_frozen_inventory_order(item)
    ]
    current_open_order_rows = list(current_open_orders)
    open_orders = [
        dict(item)
        for item in current_open_order_rows
        if isinstance(item, dict) and not is_frozen_inventory_order(item)
    ]
    owned_orders = [
        dict(item)
        for item in (
            current_open_order_rows
            if owned_open_orders is None
            else owned_open_orders
        )
        if isinstance(item, dict) and not is_frozen_inventory_order(item)
    ]
    recovery_profile_gate = actions.get("recovery_profile_gate")
    managed_recovery = bool(
        isinstance(recovery_profile_gate, dict)
        and recovery_profile_gate.get("managed")
    )

    def finish(
        action: str,
        *,
        places: list[dict[str, Any]] | None = None,
        cancels: list[dict[str, Any]] | None = None,
        enabled: bool = True,
        **details: Any,
    ) -> dict[str, Any]:
        kept_places = place_orders if places is None else places
        kept_cancels = cancel_orders if cancels is None else cancels
        result = dict(actions)
        result.update(
            {
                "place_orders": kept_places,
                "place_count": len(kept_places),
                "place_notional": sum(_order_notional(row) for row in kept_places),
                "cancel_orders": kept_cancels,
                "cancel_count": len(kept_cancels),
                "actual_net_exposure_decision": {
                    "enabled": enabled,
                    "action": action,
                    "inventory_scope": "ordinary_excluding_frozen",
                    "current_actual_net_notional": current,
                    "current_ordinary_net_notional": current,
                    "max_actual_net_notional": cap if enabled else None,
                    **details,
                },
            }
        )
        return result

    if invalid_fields:
        return finish(
            "hold",
            places=[],
            cancels=[],
            reason="actual_net_input_non_finite",
            invalid_fields=invalid_fields,
        )
    if cap <= 0:
        return finish("normal", enabled=False)
    if mark_price <= 0:
        return finish(
            "hold",
            places=[],
            cancels=[],
            reason="actual_net_valuation_unavailable",
        )

    def marked_notional(order: dict[str, Any]) -> float:
        qty = float(_order_remaining_qty(order))
        if qty > 0:
            return qty * mark_price
        # An unquantified order cannot be proven inside a hard cap.
        return cap + abs(current) + 1.0

    open_by_side = {"BUY": [], "SELL": []}
    open_notional = {"BUY": 0.0, "SELL": 0.0}
    for order in open_orders:
        side = str(order.get("side") or "").upper().strip()
        if side not in open_by_side:
            continue
        open_by_side[side].append(order)
        open_notional[side] += marked_notional(order)

    projected_open_buy = current + open_notional["BUY"]
    projected_open_sell = current - open_notional["SELL"]
    risk_open_sides: list[str] = []
    if open_notional["BUY"] > 0 and projected_open_buy > cap + 1e-9:
        risk_open_sides.append("BUY")
    if open_notional["SELL"] > 0 and projected_open_sell < -cap - 1e-9:
        risk_open_sides.append("SELL")

    if risk_open_sides:
        if managed_recovery:
            # A managed owner may have rejected cancellation because the
            # generation/profile fence is stale.  The final exposure arbiter
            # only narrows those already-authorized candidates; it never
            # recreates a mutation from the exchange snapshot.
            risk_cancels = [
                order
                for order in cancel_orders
                if str(order.get("side") or "").upper().strip()
                in risk_open_sides
            ]
        else:
            risk_cancels = [
                order
                for side in risk_open_sides
                for order in open_by_side[side]
                if any(
                    _order_matches_cancel(order, owned_order)
                    for owned_order in owned_orders
                )
            ]
        if not risk_cancels:
            return finish(
                "hold",
                places=[],
                cancels=[],
                reason="risk_cancel_not_authorized",
                risk_open_sides=risk_open_sides,
            )
        return finish(
            "cancel_risk_increasing",
            places=[],
            cancels=risk_cancels,
            risk_open_sides=risk_open_sides,
        )

    candidate_notional = {"BUY": 0.0, "SELL": 0.0}
    for order in place_orders:
        side = str(order.get("side") or "").upper().strip()
        if side in candidate_notional:
            candidate_notional[side] += marked_notional(order)
    projected_buy = projected_open_buy + candidate_notional["BUY"]
    projected_sell = projected_open_sell - candidate_notional["SELL"]
    triggered_reasons: list[str] = []
    if abs(current) > cap + 1e-9:
        triggered_reasons.append("current_actual_net_over_cap")
    if projected_buy > cap + 1e-9:
        triggered_reasons.append("projected_buy_over_cap")
    if projected_sell < -cap - 1e-9:
        triggered_reasons.append("projected_sell_over_cap")
    if not triggered_reasons:
        return finish(
            "normal",
            projected_buy_notional=projected_buy,
            projected_sell_notional=projected_sell,
        )

    reducing_side = "SELL" if current > 1e-12 else "BUY" if current < -1e-12 else None
    existing_reducing_count = (
        len(open_by_side[reducing_side]) if reducing_side is not None else 0
    )
    reducing_refresh_cancels = (
        [
            cancel
            for cancel in cancel_orders
            if any(
                _order_matches_cancel(open_order, cancel)
                for open_order in open_by_side[reducing_side]
            )
        ]
        if reducing_side is not None
        else []
    )
    if reducing_refresh_cancels:
        return finish(
            "cancel_net_decrease_refresh",
            places=[],
            cancels=reducing_refresh_cancels,
            reducing_side=reducing_side,
            triggered_reasons=triggered_reasons,
        )
    selected: dict[str, Any] | None = None
    if reducing_side is not None and existing_reducing_count == 0:
        eligible: list[tuple[int, int, dict[str, Any]]] = []
        for index, order in enumerate(place_orders):
            if str(order.get("side") or "").upper().strip() != reducing_side:
                continue
            notional = marked_notional(order)
            if (
                notional <= 0
                or notional > abs(current) + 1e-9
                or not _is_gtx_candidate(order)
            ):
                continue
            role = str(order.get("role") or "").lower().strip()
            no_loss_state = str(
                order.get("reduce_only_no_loss_guard") or ""
            ).lower()
            if "reduce" in role and "bypassed" not in no_loss_state:
                priority = 0
            elif (role, reducing_side) in {
                ("best_quote_entry_short", "SELL"),
                ("best_quote_entry_long", "BUY"),
            }:
                priority = 1
            elif "reduce" in role:
                priority = 2
            else:
                continue
            eligible.append((priority, index, order))
        if eligible:
            selected = min(eligible, key=lambda item: (item[0], item[1]))[2]

    action = (
        "hold_existing_net_decrease"
        if existing_reducing_count > 0
        else "place_net_decrease"
        if selected is not None
        else "hold"
    )
    return finish(
        action,
        places=[selected] if selected is not None else [],
        cancels=[],
        reducing_side=reducing_side,
        triggered_reasons=triggered_reasons,
        selected_role=str(selected.get("role") or "") if selected else None,
    )


def coordinate_symbol_execution_action(
    *,
    actions: dict[str, Any],
    current_actual_net_qty: float,
    valuation_price: float,
    current_open_orders: Iterable[Any],
    max_actual_net_notional: float,
    owned_open_orders: Iterable[Any] | None = None,
) -> dict[str, Any]:
    """Select one logical mutation lane for this symbol and cycle.

    The ordinary cap arbiter never owns frozen orders.  This outer coordinator
    first evaluates ordinary safety using only ordinary candidates.  A real
    ordinary cap action or fail-closed HOLD wins over frozen work; otherwise an
    authorized frozen mutation group is one logical action and ordinary work is
    deferred to the next cycle.  A two-leg frozen pair remains one logical
    action even though it consumes two exchange requests.
    """

    all_places = [
        dict(item)
        for item in actions.get("place_orders", [])
        if isinstance(item, dict)
    ]
    all_cancels = [
        dict(item)
        for item in actions.get("cancel_orders", [])
        if isinstance(item, dict)
    ]
    frozen_place_candidates = [
        item for item in all_places if is_frozen_inventory_order(item)
    ]
    frozen_cancel_candidates = [
        item for item in all_cancels if is_frozen_inventory_order(item)
    ]
    authorized_frozen_places = [
        item
        for item in frozen_place_candidates
        if str(item.get("frozen_inventory_request_id") or "").strip()
        and _truthy(item.get("frozen_inventory_authorization_validated"))
        and _frozen_order_is_gtx_maker(item)
    ]
    authorized_frozen_cancels = [
        item
        for item in frozen_cancel_candidates
        if str(item.get("frozen_inventory_request_id") or "").strip()
        and _truthy(item.get("frozen_inventory_authorization_validated"))
    ]
    unauthorized_frozen_place_count = (
        len(frozen_place_candidates) - len(authorized_frozen_places)
    )
    unauthorized_frozen_cancel_count = (
        len(frozen_cancel_candidates) - len(authorized_frozen_cancels)
    )
    frozen_request_ids = list(
        dict.fromkeys(
            str(item.get("frozen_inventory_request_id") or "").strip()
            for item in [*authorized_frozen_cancels, *authorized_frozen_places]
        )
    )
    invalid_pair_request_ids = {
        request_id
        for request_id in frozen_request_ids
        if not _valid_frozen_pair_group(
            [
                item
                for item in authorized_frozen_places
                if str(item.get("frozen_inventory_request_id") or "").strip()
                == request_id
                and str(item.get("role") or "").lower().strip().startswith(
                    "frozen_inventory_pair_release_"
                )
            ]
        )
    }
    if invalid_pair_request_ids:
        authorized_frozen_places = [
            item
            for item in authorized_frozen_places
            if str(item.get("frozen_inventory_request_id") or "").strip()
            not in invalid_pair_request_ids
        ]
        authorized_frozen_cancels = [
            item
            for item in authorized_frozen_cancels
            if str(item.get("frozen_inventory_request_id") or "").strip()
            not in invalid_pair_request_ids
        ]
        unauthorized_frozen_place_count += sum(
            1
            for item in frozen_place_candidates
            if str(item.get("frozen_inventory_request_id") or "").strip()
            in invalid_pair_request_ids
        )
        unauthorized_frozen_cancel_count += sum(
            1
            for item in frozen_cancel_candidates
            if str(item.get("frozen_inventory_request_id") or "").strip()
            in invalid_pair_request_ids
        )
        frozen_request_ids = [
            request_id
            for request_id in frozen_request_ids
            if request_id not in invalid_pair_request_ids
        ]

    def _request_sort_key(request_id: str) -> tuple[bool, str, str, str]:
        rows = [
            item
            for item in [*authorized_frozen_cancels, *authorized_frozen_places]
            if str(item.get("frozen_inventory_request_id") or "").strip()
            == request_id
        ]
        last_selected = min(
            (
                str(item.get("frozen_inventory_last_selected_at") or "").strip()
                for item in rows
                if str(item.get("frozen_inventory_last_selected_at") or "").strip()
            ),
            default="",
        )
        requested_at = min(
            (
                str(item.get("frozen_inventory_requested_at") or "").strip()
                for item in rows
                if str(item.get("frozen_inventory_requested_at") or "").strip()
            ),
            default="",
        )
        return bool(last_selected), last_selected or requested_at, requested_at, request_id

    frozen_request_ids.sort(key=_request_sort_key)
    pair_repair_request_ids = [
        request_id
        for request_id in frozen_request_ids
        if any(
            str(item.get("frozen_inventory_pair_repair_side") or "").strip()
            for item in authorized_frozen_places
            if str(item.get("frozen_inventory_request_id") or "").strip()
            == request_id
        )
    ]
    selected_frozen_request_id = (
        pair_repair_request_ids[0]
        if pair_repair_request_ids
        else frozen_request_ids[0]
        if frozen_request_ids
        else ""
    )
    frozen_places = [
        item
        for item in authorized_frozen_places
        if str(item.get("frozen_inventory_request_id") or "").strip()
        == selected_frozen_request_id
    ]
    frozen_cancels = [
        item
        for item in authorized_frozen_cancels
        if str(item.get("frozen_inventory_request_id") or "").strip()
        == selected_frozen_request_id
    ]
    deferred_frozen_places = [
        item for item in authorized_frozen_places if item not in frozen_places
    ]
    deferred_frozen_cancels = [
        item for item in authorized_frozen_cancels if item not in frozen_cancels
    ]
    ordinary_places = [item for item in all_places if not is_frozen_inventory_order(item)]
    ordinary_cancels = [item for item in all_cancels if not is_frozen_inventory_order(item)]

    ordinary_actions = dict(actions)
    ordinary_actions.update(
        {
            "place_orders": ordinary_places,
            "place_count": len(ordinary_places),
            "place_notional": sum(_order_notional(row) for row in ordinary_places),
            "cancel_orders": ordinary_cancels,
            "cancel_count": len(ordinary_cancels),
        }
    )
    ordinary_result = apply_actual_net_exposure_decision_to_actions(
        actions=ordinary_actions,
        current_actual_net_qty=current_actual_net_qty,
        valuation_price=valuation_price,
        current_open_orders=current_open_orders,
        max_actual_net_notional=max_actual_net_notional,
        owned_open_orders=owned_open_orders,
    )
    ordinary_decision = dict(
        ordinary_result.get("actual_net_exposure_decision") or {}
    )
    ordinary_action = str(ordinary_decision.get("action") or "hold")
    ordinary_safety_wins = ordinary_action != "normal"
    recovery_gate = actions.get("recovery_profile_gate")
    current_gate = (
        recovery_gate.get("current_gate")
        if isinstance(recovery_gate, Mapping)
        else None
    )
    active_recovery_action = str(
        (current_gate or {}).get("active_action") or "noop"
    ).lower().strip()
    ordinary_recovery_wins = bool(
        isinstance(recovery_gate, Mapping)
        and recovery_gate.get("managed")
        and recovery_gate.get("authorized")
        and active_recovery_action not in {"", "noop"}
    )
    pair_repair_wins = bool(pair_repair_request_ids)

    if not authorized_frozen_places and not authorized_frozen_cancels:
        ordinary_decision.update(
            {
                "selected_lane": "ordinary",
                "dropped_unauthorized_frozen_place_count": unauthorized_frozen_place_count,
                "dropped_unauthorized_frozen_cancel_count": unauthorized_frozen_cancel_count,
            }
        )
        ordinary_result["actual_net_exposure_decision"] = ordinary_decision
        return ordinary_result

    if (ordinary_safety_wins or ordinary_recovery_wins) and not pair_repair_wins:
        ordinary_decision.update(
            {
                "selected_lane": "ordinary",
                "ordinary_recovery_action": active_recovery_action,
                "deferred_frozen_place_count": len(authorized_frozen_places),
                "deferred_frozen_cancel_count": len(authorized_frozen_cancels),
                "deferred_frozen_request_count": len(frozen_request_ids),
                "dropped_unauthorized_frozen_place_count": unauthorized_frozen_place_count,
                "dropped_unauthorized_frozen_cancel_count": unauthorized_frozen_cancel_count,
            }
        )
        ordinary_result["actual_net_exposure_decision"] = ordinary_decision
        return ordinary_result

    result = dict(actions)
    result.update(
        {
            "place_orders": frozen_places,
            "place_count": len(frozen_places),
            "place_notional": sum(_order_notional(row) for row in frozen_places),
            "cancel_orders": frozen_cancels,
            "cancel_count": len(frozen_cancels),
            "actual_net_exposure_decision": {
                "enabled": False,
                "action": "frozen_inventory_action",
                "selected_lane": "frozen",
                "inventory_scope": "frozen_ledger",
                "ordinary_candidate_action": ordinary_action,
                "selected_frozen_request_id": selected_frozen_request_id,
                "current_ordinary_net_notional": ordinary_decision.get(
                    "current_ordinary_net_notional"
                ),
                "max_actual_net_notional": ordinary_decision.get(
                    "max_actual_net_notional"
                ),
                "deferred_ordinary_place_count": len(ordinary_places),
                "deferred_ordinary_cancel_count": len(ordinary_cancels),
                "deferred_frozen_request_count": max(len(frozen_request_ids) - 1, 0),
                "deferred_frozen_place_count": len(deferred_frozen_places),
                "deferred_frozen_cancel_count": len(deferred_frozen_cancels),
                "dropped_unauthorized_frozen_place_count": unauthorized_frozen_place_count,
                "dropped_unauthorized_frozen_cancel_count": unauthorized_frozen_cancel_count,
            },
        }
    )
    return result


def apply_projected_entry_exposure_cap_to_actions(
    *,
    actions: dict[str, Any],
    strategy_mode: str,
    current_long_notional: float,
    current_short_notional: float,
    current_open_orders: Iterable[Any],
    max_long_notional: float,
    max_short_notional: float,
) -> dict[str, Any]:
    """Drop entries whose worst-case same-side fills would cross a cap.

    Existing live entries and each accepted candidate reserve capacity in
    order.  Reduce-only/frozen exits remain outside this entry-only guard.
    """

    normalized_mode = str(strategy_mode or "").strip() or "one_way_long"
    active_notional = {
        "long": max(_safe_float(current_long_notional), 0.0),
        "short": max(_safe_float(current_short_notional), 0.0),
    }
    caps = {
        "long": max(_safe_float(max_long_notional), 0.0),
        "short": max(_safe_float(max_short_notional), 0.0),
    }
    reserved = {"long": 0.0, "short": 0.0}

    def entry_side(order: dict[str, Any]) -> str | None:
        if normalized_mode in {
            "hedge_neutral",
            "hedge_best_quote_maker_volume_v1",
        }:
            return _hedge_entry_side_from_order(order)
        return _entry_side_from_order(order, strategy_mode=normalized_mode)

    for raw_order in current_open_orders:
        if not isinstance(raw_order, dict):
            continue
        if is_frozen_inventory_order(raw_order):
            continue
        side = entry_side(raw_order)
        if side is not None:
            reserved[side] += _order_notional(raw_order)

    kept_orders: list[dict[str, Any]] = []
    dropped_orders: list[dict[str, Any]] = []
    for raw_order in actions.get("place_orders", []):
        if not isinstance(raw_order, dict):
            continue
        order = dict(raw_order)
        if is_frozen_inventory_order(order):
            kept_orders.append(order)
            continue
        side = entry_side(order)
        notional = _order_notional(order)
        cap = caps[side] if side is not None else 0.0
        projected = (
            active_notional[side] + reserved[side] + notional
            if side is not None
            else 0.0
        )
        if side is not None and cap > 0 and projected > cap + 1e-9:
            order["projected_entry_exposure_cap_drop_reason"] = (
                f"{side}_projected_notional={projected:.8f}>cap={cap:.8f}"
            )
            dropped_orders.append(order)
            continue
        kept_orders.append(order)
        if side is not None:
            reserved[side] += notional

    result = dict(actions)
    result["place_orders"] = kept_orders
    result["place_count"] = len(kept_orders)
    result["place_notional"] = sum(
        _order_notional(item) for item in kept_orders
    )
    result["projected_entry_exposure_cap"] = {
        "enabled": bool(caps["long"] > 0 or caps["short"] > 0),
        "current_long_notional": active_notional["long"],
        "current_short_notional": active_notional["short"],
        "max_long_notional": caps["long"],
        "max_short_notional": caps["short"],
        "reserved_long_entry_notional": reserved["long"],
        "reserved_short_entry_notional": reserved["short"],
        "dropped_order_count": len(dropped_orders),
        "dropped_orders": dropped_orders,
    }
    return result


def _is_best_quote_volume_entry_order(order: dict[str, Any]) -> bool:
    role = str(order.get("role", "") or "").strip().lower()
    return role in {"best_quote_entry_long", "best_quote_entry_short"}


def _best_quote_volume_entries_are_isolated(plan_report: dict[str, Any], *, strategy_mode: str) -> bool:
    normalized_mode = str(strategy_mode or "").strip()
    if normalized_mode != "hedge_best_quote_maker_volume_v1":
        return False
    best_quote_metrics = plan_report.get("best_quote_maker_volume")
    if isinstance(best_quote_metrics, dict):
        reduce_freeze = best_quote_metrics.get("reduce_freeze")
        if isinstance(reduce_freeze, dict) and reduce_freeze.get("isolates_risk_metrics"):
            return True
    loss_scope = plan_report.get("runtime_guard_loss_scope")
    return isinstance(loss_scope, dict) and bool(loss_scope.get("enabled"))


def apply_hard_loss_rescue_entry_guard_to_actions(
    *,
    actions: dict[str, Any],
    plan_report: dict[str, Any],
    strategy_mode: str,
) -> dict[str, Any]:
    guard = plan_report.get("hard_loss_rescue_entry_guard")
    if not isinstance(guard, dict) or not bool(guard.get("active")):
        return actions

    block_long_entries = bool(guard.get("block_long_entries"))
    block_short_entries = bool(guard.get("block_short_entries"))
    if not block_long_entries and not block_short_entries:
        return actions

    kept_place_orders: list[dict[str, Any]] = []
    dropped_orders: list[dict[str, Any]] = []
    isolated_best_quote_entries = _best_quote_volume_entries_are_isolated(
        plan_report, strategy_mode=strategy_mode
    )
    isolated_orders: list[dict[str, Any]] = []
    for order in [dict(item) for item in actions.get("place_orders", []) if isinstance(item, dict)]:
        if isolated_best_quote_entries and _is_best_quote_volume_entry_order(order):
            isolated = dict(order)
            isolated["hard_loss_rescue_isolated_reason"] = "best_quote_volume_entry_uses_managed_ledger"
            isolated_orders.append(isolated)
            kept_place_orders.append(order)
            continue
        entry_side = _entry_side_from_order(order, strategy_mode=strategy_mode)
        should_drop = (entry_side == "long" and block_long_entries) or (
            entry_side == "short" and block_short_entries
        )
        if should_drop:
            dropped = dict(order)
            dropped["hard_loss_rescue_drop_reason"] = guard.get("reason") or "hard_loss_rescue_active"
            dropped_orders.append(dropped)
        else:
            kept_place_orders.append(order)

    result = dict(actions)
    result["place_orders"] = kept_place_orders
    result["place_count"] = len(kept_place_orders)
    result["place_notional"] = sum(_safe_float(item.get("notional")) for item in kept_place_orders)
    result["hard_loss_rescue_entry_guard"] = {
        "enabled": True,
        "active": True,
        "block_long_entries": block_long_entries,
        "block_short_entries": block_short_entries,
        "reason": guard.get("reason"),
        "dropped_order_count": len(dropped_orders),
        "dropped_orders": dropped_orders,
        "isolated_best_quote_entry_count": len(isolated_orders),
        "isolated_best_quote_entries": isolated_orders,
    }
    return result


def apply_loss_reduce_reentry_guard_to_actions(
    *,
    actions: dict[str, Any],
    plan_report: dict[str, Any],
    strategy_mode: str,
) -> dict[str, Any]:
    guard = plan_report.get("loss_reduce_reentry_guard")
    if not isinstance(guard, dict) or not bool(guard.get("active")):
        return actions

    block_long_entries = bool(guard.get("block_long_entries"))
    block_short_entries = bool(guard.get("block_short_entries"))
    if not block_long_entries and not block_short_entries:
        return actions

    kept_place_orders: list[dict[str, Any]] = []
    dropped_orders: list[dict[str, Any]] = []
    isolated_best_quote_entries = _best_quote_volume_entries_are_isolated(
        plan_report, strategy_mode=strategy_mode
    )
    isolated_orders: list[dict[str, Any]] = []
    for order in [dict(item) for item in actions.get("place_orders", []) if isinstance(item, dict)]:
        if isolated_best_quote_entries and _is_best_quote_volume_entry_order(order):
            isolated = dict(order)
            isolated["loss_reduce_reentry_isolated_reason"] = "best_quote_volume_entry_uses_managed_ledger"
            isolated_orders.append(isolated)
            kept_place_orders.append(order)
            continue
        entry_side = _entry_side_from_order(order, strategy_mode=strategy_mode)
        should_drop = (entry_side == "long" and block_long_entries) or (
            entry_side == "short" and block_short_entries
        )
        if should_drop:
            dropped = dict(order)
            dropped["loss_reduce_reentry_drop_reason"] = guard.get("reason") or "loss_reduce_reentry_guard_active"
            dropped_orders.append(dropped)
        else:
            kept_place_orders.append(order)

    result = dict(actions)
    result["place_orders"] = kept_place_orders
    result["place_count"] = len(kept_place_orders)
    result["place_notional"] = sum(_safe_float(item.get("notional")) for item in kept_place_orders)
    result["loss_reduce_reentry_guard"] = {
        "enabled": bool(guard.get("enabled", True)),
        "active": True,
        "block_long_entries": block_long_entries,
        "block_short_entries": block_short_entries,
        "reason": guard.get("reason"),
        "dropped_order_count": len(dropped_orders),
        "dropped_orders": dropped_orders,
        "isolated_best_quote_entry_count": len(isolated_orders),
        "isolated_best_quote_entries": isolated_orders,
    }
    return result


def build_execution_actions(plan_report: dict[str, Any]) -> dict[str, Any]:
    symbol = str(plan_report.get("symbol", "")).upper().strip()
    forced_reduce_orders = [
        item for item in plan_report.get("forced_reduce_orders", []) if isinstance(item, dict)
    ]
    bootstrap_orders = [
        item for item in plan_report.get("bootstrap_orders", []) if isinstance(item, dict)
    ]
    missing_orders = [
        item for item in plan_report.get("missing_orders", []) if isinstance(item, dict)
    ]
    stale_orders = [
        item for item in plan_report.get("stale_orders", []) if isinstance(item, dict)
    ]
    if symbol:
        stale_orders = [item for item in stale_orders if _is_strategy_order(item, symbol)]
    place_orders: list[dict[str, Any]] = []
    seen_place_templates: set[tuple[str, str, str, str, str, str, str, str]] = set()
    for order in [*forced_reduce_orders, *bootstrap_orders, *missing_orders]:
        key = _place_order_template_key(order)
        if key in seen_place_templates:
            continue
        seen_place_templates.add(key)
        place_orders.append(order)
    place_notional = sum(_safe_float(item.get("notional")) for item in place_orders)
    return {
        "place_orders": place_orders,
        "cancel_orders": stale_orders,
        "place_count": len(place_orders),
        "cancel_count": len(stale_orders),
        "place_notional": place_notional,
    }


def sort_cancel_orders_farthest_from_market_first(
    *,
    actions: dict[str, Any],
    live_bid_price: float,
    live_ask_price: float,
) -> dict[str, Any]:
    cancel_orders = [dict(item) for item in actions.get("cancel_orders", []) if isinstance(item, dict)]
    if len(cancel_orders) <= 1:
        return actions

    safe_bid = _safe_float(live_bid_price)
    safe_ask = _safe_float(live_ask_price)

    def _distance_key(order: dict[str, Any]) -> tuple[int, float, int]:
        side = str(order.get("side", "")).upper().strip()
        price = _safe_float(order.get("price"))
        cancel_reason = str(order.get("cancel_reason", "") or "").strip().lower()
        urgent = 0 if cancel_reason else 1
        if side == "BUY":
            reference = safe_bid if safe_bid > 0 else safe_ask
            distance = max(reference - price, 0.0) if reference > 0 and price > 0 else 0.0
        elif side == "SELL":
            reference = safe_ask if safe_ask > 0 else safe_bid
            distance = max(price - reference, 0.0) if reference > 0 and price > 0 else 0.0
        else:
            distance = 0.0
        return (urgent, -distance, 0)

    sorted_cancel_orders = sorted(enumerate(cancel_orders), key=lambda item: (_distance_key(item[1]), item[0]))
    result = dict(actions)
    result["cancel_orders"] = [item for _, item in sorted_cancel_orders]
    result["cancel_count"] = len(result["cancel_orders"])
    result["cancel_order_priority"] = {
        "enabled": True,
        "mode": "farthest_from_market_first",
        "live_bid_price": safe_bid,
        "live_ask_price": safe_ask,
    }
    return result


def suppress_same_side_nearby_place_orders(
    *,
    actions: dict[str, Any],
    current_open_orders: Iterable[Any] | None = None,
    min_price_spacing: float,
    live_bid_price: float,
    live_ask_price: float,
    tick_size: float | None,
    min_qty: float | None,
    min_notional: float | None,
    step_size: float | None = None,
    protect_cancelled_existing: bool = True,
) -> dict[str, Any]:
    """Keep only one non-urgent maker order per side inside the configured spacing."""
    place_orders = [dict(item) for item in actions.get("place_orders", []) if isinstance(item, dict)]
    cancel_orders = [dict(item) for item in actions.get("cancel_orders", []) if isinstance(item, dict)]
    safe_spacing = max(_safe_float(min_price_spacing), 0.0)
    if not place_orders or safe_spacing <= 0:
        return actions

    tick = max(_safe_float(tick_size), 0.0)
    existing_spacing = safe_spacing + tick
    existing_orders: list[tuple[dict[str, Any], str, str, float]] = []
    for open_order in current_open_orders or []:
        if not isinstance(open_order, dict):
            continue
        side = str(open_order.get("side", "")).upper().strip()
        price = _safe_float(open_order.get("price"))
        if side not in {"BUY", "SELL"} or price <= 0:
            continue
        if not protect_cancelled_existing and any(
            _order_matches_cancel(open_order, cancel_order) for cancel_order in cancel_orders
        ):
            continue
        existing_orders.append((dict(open_order), side, _order_position_side(open_order), price))

    side_orders: dict[str, list[tuple[int, dict[str, Any], float]]] = {"BUY": [], "SELL": []}
    kept_by_index: dict[int, dict[str, Any]] = {}
    suppressed_indices: set[int] = set()
    suppressed_orders: list[dict[str, Any]] = []
    protected_existing_orders: list[dict[str, Any]] = []
    for index, order in enumerate(place_orders):
        side = str(order.get("side", "")).upper().strip()
        if _is_urgent_reduce_only_order(order, strategy_mode="synthetic_neutral"):
            kept_by_index[index] = order
            continue
        if side not in side_orders:
            kept_by_index[index] = order
            continue
        prepared_order, _ = prepare_post_only_order_request(
            order=order,
            side=side,
            live_bid_price=live_bid_price,
            live_ask_price=live_ask_price,
            tick_size=tick_size,
            min_qty=min_qty,
            min_notional=min_notional,
            step_size=step_size,
            post_only=_order_prefers_post_only(order),
        )
        price = (
            _safe_float(prepared_order.get("submitted_price"))
            if prepared_order is not None
            else _safe_float(order.get("price"))
        )
        if price <= 0:
            kept_by_index[index] = order
            continue
        normalized = dict(order)
        normalized["price"] = price
        normalized["notional"] = price * _safe_float(normalized.get("qty"))
        position_side = _order_position_side(normalized)
        matching_existing = [
            open_order
            for open_order, open_side, open_position_side, open_price in existing_orders
            if open_side == side
            and open_position_side == position_side
            and abs(price - open_price) <= existing_spacing + 1e-12
        ]
        if matching_existing:
            suppressed = dict(normalized)
            suppressed["defer_reason"] = "existing_same_side_nearby_open_order"
            suppressed["min_price_spacing"] = safe_spacing
            suppressed_orders.append(suppressed)
            suppressed_indices.add(index)
            protected_existing_orders.extend(matching_existing)
            continue
        side_orders[side].append((index, normalized, price))

    for side, entries in side_orders.items():
        if not entries:
            continue
        reverse = side == "BUY"
        selected: list[tuple[int, dict[str, Any], float, str]] = []
        for index, order, price in sorted(entries, key=lambda item: item[2], reverse=reverse):
            position_side = _order_position_side(order)
            if any(
                position_side == selected_position_side
                and abs(price - selected_price) < safe_spacing - 1e-12
                for _, _, selected_price, selected_position_side in selected
            ):
                suppressed = dict(order)
                suppressed["defer_reason"] = "same_side_nearby_place_order"
                suppressed["min_price_spacing"] = safe_spacing
                suppressed_orders.append(suppressed)
                suppressed_indices.add(index)
                continue
            selected.append((index, order, price, position_side))
            kept_by_index[index] = order

    if not suppressed_orders:
        return actions

    protected_cancel_orders: list[dict[str, Any]] = []
    if protected_existing_orders and cancel_orders:
        adjusted_cancel_orders: list[dict[str, Any]] = []
        for cancel_order in cancel_orders:
            if any(_order_matches_cancel(open_order, cancel_order) for open_order in protected_existing_orders):
                protected_cancel_orders.append(cancel_order)
                continue
            adjusted_cancel_orders.append(cancel_order)
        cancel_orders = adjusted_cancel_orders

    kept_place_orders: list[dict[str, Any]] = []
    for index, order in enumerate(place_orders):
        if index in kept_by_index:
            kept_place_orders.append(kept_by_index[index])
            continue
        if index not in suppressed_indices:
            kept_place_orders.append(order)

    result = dict(actions)
    result["place_orders"] = kept_place_orders
    result["cancel_orders"] = cancel_orders
    result["place_count"] = len(kept_place_orders)
    result["cancel_count"] = len(cancel_orders)
    result["place_notional"] = sum(_safe_float(item.get("notional")) for item in kept_place_orders)
    result["same_side_spacing_guard"] = {
        "enabled": True,
        "min_price_spacing": safe_spacing,
        "suppressed_place_count": len(suppressed_orders),
        "suppressed_place_orders": suppressed_orders,
        "protected_cancel_count": len(protected_cancel_orders),
        "protected_cancel_orders": protected_cancel_orders,
    }
    return result


def preserve_queue_priority_in_execution_actions(
    *,
    actions: dict[str, Any],
    live_bid_price: float,
    live_ask_price: float,
    tick_size: float | None,
    min_qty: float | None,
    min_notional: float | None,
    step_size: float | None = None,
) -> dict[str, Any]:
    """Prefer preserving queued orders unless the refreshed plan truly needs less size or a new bucket."""
    place_orders = [dict(item) for item in actions.get("place_orders", []) if isinstance(item, dict)]
    cancel_orders = [dict(item) for item in actions.get("cancel_orders", []) if isinstance(item, dict)]
    urgent_place_orders: list[dict[str, Any]] = []
    queue_place_orders: list[dict[str, Any]] = []
    for order in place_orders:
        if _is_urgent_reduce_only_order(order, strategy_mode="synthetic_neutral"):
            urgent_place_orders.append(order)
        else:
            queue_place_orders.append(order)
    place_orders = queue_place_orders
    if not place_orders or not cancel_orders:
        place_orders, duplicate_place_orders = _merge_place_orders_by_submitted_bucket(
            orders=place_orders,
            live_bid_price=live_bid_price,
            live_ask_price=live_ask_price,
            tick_size=tick_size,
            min_qty=min_qty,
            min_notional=min_notional,
            step_size=step_size,
        )
        merged_place_orders = [*urgent_place_orders, *place_orders]
        result = {
            "place_orders": merged_place_orders,
            "cancel_orders": cancel_orders,
            "place_count": len(merged_place_orders),
            "cancel_count": len(cancel_orders),
            "place_notional": sum(_safe_float(item.get("notional")) for item in merged_place_orders),
        }
        if duplicate_place_orders:
            result["duplicate_place_bucket_guard"] = {
                "enabled": True,
                "merged_order_count": len(duplicate_place_orders),
                "merged_orders": duplicate_place_orders,
            }
        return result

    cancel_indices_by_bucket: dict[str, list[int]] = {}
    cancel_totals_by_bucket: dict[str, Decimal] = {}
    for index, order in enumerate(cancel_orders):
        side = str(order.get("side", "")).upper().strip()
        price = _safe_float(order.get("price"))
        qty = _quantity_decimal(order.get("origQty", order.get("qty")))
        if side not in {"BUY", "SELL"} or price <= 0 or qty <= Decimal("0"):
            continue
        key = _order_reconcile_bucket_key(order, price=price)
        cancel_indices_by_bucket.setdefault(key, []).append(index)
        cancel_totals_by_bucket[key] = cancel_totals_by_bucket.get(key, Decimal("0")) + qty

    projected_place_totals: dict[str, Decimal] = {}
    projected_place_templates: dict[str, dict[str, Any]] = {}
    for order in place_orders:
        side = str(order.get("side", "")).upper().strip()
        if side not in {"BUY", "SELL"}:
            continue
        prepared_order, _ = prepare_post_only_order_request(
            order=order,
            side=side,
            live_bid_price=live_bid_price,
            live_ask_price=live_ask_price,
            tick_size=tick_size,
            min_qty=min_qty,
            min_notional=min_notional,
            step_size=step_size,
            post_only=_order_prefers_post_only(order),
        )
        if prepared_order is None:
            continue
        qty = _quantity_decimal(prepared_order.get("qty"))
        price = _safe_float(prepared_order.get("submitted_price"))
        if qty <= Decimal("0") or price <= 0:
            continue
        key = _order_reconcile_bucket_key(order, price=price)
        projected_place_totals[key] = projected_place_totals.get(key, Decimal("0")) + qty
        if key not in projected_place_templates:
            template = dict(order)
            template["price"] = price
            template["qty"] = float(qty)
            template["notional"] = price * float(qty)
            projected_place_templates[key] = template

    if not projected_place_totals:
        merged_place_orders = [*urgent_place_orders, *place_orders]
        return {
            "place_orders": merged_place_orders,
            "cancel_orders": cancel_orders,
            "place_count": len(merged_place_orders),
            "cancel_count": len(cancel_orders),
            "place_notional": sum(_safe_float(item.get("notional")) for item in merged_place_orders),
        }

    preserved_cancel_indices: set[int] = set()
    reduced_place_totals = dict(projected_place_totals)
    for key, desired_total in projected_place_totals.items():
        existing_total = cancel_totals_by_bucket.get(key, Decimal("0"))
        if existing_total <= Decimal("0"):
            continue
        projected_template = projected_place_templates.get(key) or {}
        projected_role = str(projected_template.get("role", "")).strip()
        replace_smaller_reduce = projected_role in {"best_quote_reduce_long", "best_quote_reduce_short"}
        if replace_smaller_reduce:
            continue
        bucket_indices = cancel_indices_by_bucket.get(key, [])
        bucket_quantities = [
            _quantity_decimal(cancel_orders[index].get("origQty", cancel_orders[index].get("qty")))
            for index in bucket_indices
        ]
        if desired_total >= existing_total:
            for cancel_index in bucket_indices:
                preserved_cancel_indices.add(cancel_index)
            reduced_place_totals[key] = Decimal("0")
            continue

        chosen_local_indices, preserved_qty = _choose_preserved_cancel_indices(bucket_quantities, desired_total)
        if preserved_qty <= Decimal("0"):
            continue
        for local_index in chosen_local_indices:
            preserved_cancel_indices.add(bucket_indices[local_index])
        reduced_place_totals[key] = max(desired_total - preserved_qty, Decimal("0"))

    adjusted_place_orders: list[dict[str, Any]] = []
    consumed_projected_buckets: set[str] = set()
    for order in place_orders:
        side = str(order.get("side", "")).upper().strip()
        if side not in {"BUY", "SELL"}:
            adjusted_place_orders.append(order)
            continue
        prepared_order, _ = prepare_post_only_order_request(
            order=order,
            side=side,
            live_bid_price=live_bid_price,
            live_ask_price=live_ask_price,
            tick_size=tick_size,
            min_qty=min_qty,
            min_notional=min_notional,
            step_size=step_size,
            post_only=_order_prefers_post_only(order),
        )
        if prepared_order is None:
            adjusted_place_orders.append(order)
            continue
        projected_key = _order_reconcile_bucket_key(
            order,
            price=_safe_float(prepared_order.get("submitted_price")),
        )
        if projected_key not in reduced_place_totals:
            adjusted_place_orders.append(order)
            continue
        if projected_key in consumed_projected_buckets:
            continue
        consumed_projected_buckets.add(projected_key)
        delta = reduced_place_totals.get(projected_key, Decimal("0"))
        if delta > Decimal("0"):
            adjusted_place_orders.append(_clone_order_with_qty(projected_place_templates[projected_key], delta))

    adjusted_cancel_orders = [
        order for index, order in enumerate(cancel_orders) if index not in preserved_cancel_indices
    ]
    pending_cancel_buckets = {
        _order_reconcile_bucket_key(order)
        for order in adjusted_cancel_orders
        if str(order.get("side", "")).upper().strip() in {"BUY", "SELL"}
        and _safe_float(order.get("price")) > 0
    }
    if pending_cancel_buckets:
        safe_place_orders: list[dict[str, Any]] = []
        deferred_place_orders: list[dict[str, Any]] = []
        for order in adjusted_place_orders:
            side = str(order.get("side", "")).upper().strip()
            if side not in {"BUY", "SELL"}:
                safe_place_orders.append(order)
                continue
            prepared_order, _ = prepare_post_only_order_request(
                order=order,
                side=side,
                live_bid_price=live_bid_price,
                live_ask_price=live_ask_price,
                tick_size=tick_size,
                min_qty=min_qty,
                min_notional=min_notional,
                step_size=step_size,
                post_only=_order_prefers_post_only(order),
            )
            price = (
                _safe_float(prepared_order.get("submitted_price"))
                if prepared_order is not None
                else _safe_float(order.get("price"))
            )
            bucket = _order_reconcile_bucket_key(order, price=price)
            if bucket in pending_cancel_buckets:
                deferred = dict(order)
                deferred["defer_reason"] = "pending_same_bucket_cancel"
                deferred_place_orders.append(deferred)
                continue
            safe_place_orders.append(order)
        adjusted_place_orders = safe_place_orders
    adjusted_place_orders, duplicate_place_orders = _merge_place_orders_by_submitted_bucket(
        orders=adjusted_place_orders,
        live_bid_price=live_bid_price,
        live_ask_price=live_ask_price,
        tick_size=tick_size,
        min_qty=min_qty,
        min_notional=min_notional,
        step_size=step_size,
    )
    merged_place_orders = [*urgent_place_orders, *adjusted_place_orders]
    result = {
        "place_orders": merged_place_orders,
        "cancel_orders": adjusted_cancel_orders,
        "place_count": len(merged_place_orders),
        "cancel_count": len(adjusted_cancel_orders),
        "place_notional": sum(_safe_float(item.get("notional")) for item in merged_place_orders),
    }
    if pending_cancel_buckets:
        result["same_bucket_cancel_place_guard"] = {
            "enabled": True,
            "pending_cancel_bucket_count": len(pending_cancel_buckets),
            "deferred_place_count": len(deferred_place_orders),
            "deferred_place_orders": deferred_place_orders,
        }
    if duplicate_place_orders:
        result["duplicate_place_bucket_guard"] = {
            "enabled": True,
            "merged_order_count": len(duplicate_place_orders),
            "merged_orders": duplicate_place_orders,
        }
    return result


def suppress_place_orders_with_existing_submitted_buckets(
    *,
    actions: dict[str, Any],
    current_open_orders: Iterable[Any],
    live_bid_price: float,
    live_ask_price: float,
    tick_size: float | None,
    min_qty: float | None,
    min_notional: float | None,
    step_size: float | None = None,
) -> dict[str, Any]:
    """Drop place orders whose submitted bucket already exists on exchange."""
    place_orders = [dict(item) for item in actions.get("place_orders", []) if isinstance(item, dict)]
    if not place_orders:
        return actions

    existing_buckets: set[str] = set()
    for order in current_open_orders:
        if not isinstance(order, dict):
            continue
        side = str(order.get("side", "")).upper().strip()
        price = _safe_float(order.get("price"))
        if side not in {"BUY", "SELL"} or price <= 0:
            continue
        existing_buckets.add(_order_reconcile_bucket_key(order, price=price))

    if not existing_buckets:
        return actions

    kept_place_orders: list[dict[str, Any]] = []
    suppressed_place_orders: list[dict[str, Any]] = []
    for order in place_orders:
        side = str(order.get("side", "")).upper().strip()
        if side not in {"BUY", "SELL"}:
            kept_place_orders.append(order)
            continue
        prepared_order, _ = prepare_post_only_order_request(
            order=order,
            side=side,
            live_bid_price=live_bid_price,
            live_ask_price=live_ask_price,
            tick_size=tick_size,
            min_qty=min_qty,
            min_notional=min_notional,
            step_size=step_size,
            post_only=_order_prefers_post_only(order),
        )
        price = (
            _safe_float(prepared_order.get("submitted_price"))
            if prepared_order is not None
            else _safe_float(order.get("price"))
        )
        bucket = _order_reconcile_bucket_key(order, price=price)
        if bucket in existing_buckets:
            suppressed = dict(order)
            suppressed["defer_reason"] = "existing_same_submitted_bucket"
            suppressed["submitted_bucket"] = bucket
            suppressed_place_orders.append(suppressed)
            continue
        kept_place_orders.append(order)

    if not suppressed_place_orders:
        return actions

    result = dict(actions)
    result["place_orders"] = kept_place_orders
    result["place_count"] = len(kept_place_orders)
    result["place_notional"] = sum(_safe_float(item.get("notional")) for item in kept_place_orders)
    result["existing_submitted_bucket_guard"] = {
        "enabled": True,
        "suppressed_place_count": len(suppressed_place_orders),
        "suppressed_place_orders": suppressed_place_orders,
    }
    return result


def estimate_mid_drift_steps(
    *,
    report_mid_price: float,
    live_bid_price: float,
    live_ask_price: float,
    step_price: float,
) -> float:
    if step_price <= 0:
        return 0.0
    live_mid_price = (live_bid_price + live_ask_price) / 2.0
    return abs(live_mid_price - report_mid_price) / step_price


def validate_plan_report(
    *,
    plan_report: dict[str, Any],
    allow_symbol: str,
    max_new_orders: int,
    max_total_notional: float,
    cancel_stale: bool,
    max_plan_age_seconds: int,
    now: datetime,
    allow_dual_side_position: bool = False,
    enforce_place_limits: bool = True,
) -> dict[str, Any]:
    errors: list[str] = []
    actions = build_execution_actions(plan_report)
    symbol = str(plan_report.get("symbol", "")).upper().strip()

    if not symbol:
        errors.append("plan report is missing symbol")
    if symbol != allow_symbol.upper().strip():
        errors.append(f"symbol {symbol or '<empty>'} is not allowed; expected {allow_symbol.upper().strip()}")
    if _truthy(plan_report.get("dual_side_position")) and not allow_dual_side_position:
        errors.append("account is in hedge mode; only one-way mode is supported")
    if actions["cancel_count"] > 0 and not cancel_stale:
        errors.append("plan contains stale orders; rerun with --cancel-stale or regenerate the plan")
    if actions["place_count"] <= 0 and actions["cancel_count"] <= 0:
        errors.append("plan contains no actions to execute")

    generated_at = _parse_iso_ts(plan_report.get("generated_at"))
    if generated_at is None:
        errors.append("plan report is missing a valid generated_at timestamp")
        plan_age_seconds = None
    else:
        plan_age_seconds = max((now - generated_at.astimezone(timezone.utc)).total_seconds(), 0.0)
        if plan_age_seconds > max_plan_age_seconds:
            errors.append(
                f"plan age is {plan_age_seconds:.1f}s, above max_plan_age_seconds={max_plan_age_seconds}"
            )

    validation = {
        "ok": not errors,
        "errors": errors,
        "actions": actions,
        "plan_age_seconds": plan_age_seconds,
    }
    if enforce_place_limits:
        validation = enforce_execution_action_limits(
            validation=validation,
            max_new_orders=max_new_orders,
            max_total_notional=max_total_notional,
        )
    return validation


def enforce_execution_action_limits(
    *,
    validation: dict[str, Any],
    max_new_orders: int,
    max_total_notional: float,
) -> dict[str, Any]:
    actions = validation.get("actions") if isinstance(validation.get("actions"), dict) else {}
    errors = list(validation.get("errors") or [])
    if actions.get("place_count", 0) > max_new_orders:
        errors.append(f"plan places {actions['place_count']} new orders, above max_new_orders={max_new_orders}")
    if _safe_float(actions.get("place_notional")) > max_total_notional:
        errors.append(
            f"plan places total notional {_float(actions['place_notional'])}, above max_total_notional={_float(max_total_notional)}"
        )
    result = dict(validation)
    result["errors"] = errors
    result["ok"] = not errors
    return result


def _ignore_noop_error(exc: RuntimeError, allowed_markers: tuple[str, ...]) -> bool:
    message = str(exc).lower()
    return any(marker in message for marker in allowed_markers)


def _is_reduce_only_reject(exc: RuntimeError) -> bool:
    message = str(exc).lower()
    compact = message.replace(" ", "")
    return "-2022" in message and "reduceonlyorderisrejected" in compact


def _print_preview(
    *,
    plan_report: dict[str, Any],
    validation: dict[str, Any],
    drift_steps: float | None,
) -> None:
    actions = validation["actions"]
    print("=== Submit Plan ===")
    print(f"Symbol: {plan_report['symbol']}")
    print(
        f"Plan bid/ask/mid: {_price(_safe_float(plan_report.get('bid_price')))} / "
        f"{_price(_safe_float(plan_report.get('ask_price')))} / {_price(_safe_float(plan_report.get('mid_price')))}"
    )
    if validation["plan_age_seconds"] is not None:
        print(f"Plan age: {validation['plan_age_seconds']:.1f}s")
    if drift_steps is not None:
        print(f"Current mid drift: {drift_steps:.2f} steps")
    print(
        f"Orders to place / cancel / total notional: "
        f"{actions['place_count']} / {actions['cancel_count']} / {_float(actions['place_notional'])}"
    )
    if validation["errors"]:
        print("Errors:")
        for item in validation["errors"]:
            print(f"  - {item}")
    print("Place orders:")
    for order in actions["place_orders"]:
        print(
            f"  {str(order.get('role', 'entry'))} {str(order.get('side', '')).upper()} "
            f"qty={_float(_safe_float(order.get('qty')))} "
            f"price={_price(_safe_float(order.get('price')))} "
            f"notional={_float(_safe_float(order.get('notional')))}"
        )
    if actions["cancel_orders"]:
        print("Cancel orders:")
        for order in actions["cancel_orders"]:
            print(
                f"  {str(order.get('side', '')).upper()} "
                f"qty={_float(_safe_float(order.get('origQty', order.get('qty'))))} "
                f"price={_price(_safe_float(order.get('price')))}"
            )
    print("")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Submit a previously generated NIGHT semi-auto plan as real Binance USD-M futures orders."
    )
    parser.add_argument("--plan-json", type=str, default="output/night_semi_auto_plan.json")
    parser.add_argument("--allow-symbol", type=str, default="NIGHTUSDT")
    parser.add_argument("--margin-type", type=str, default="KEEP")
    parser.add_argument("--leverage", type=int, default=2)
    parser.add_argument("--recv-window", type=int, default=5000)
    parser.add_argument("--max-plan-age-seconds", type=int, default=60)
    parser.add_argument("--max-mid-drift-steps", type=float, default=4.0)
    parser.add_argument("--max-new-orders", type=int, default=24)
    parser.add_argument("--max-total-notional", type=float, default=500.0)
    parser.add_argument("--maker-retries", type=int, default=2)
    parser.add_argument("--cancel-stale", action="store_true")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--report-json", type=str, default="output/night_submit_report.json")
    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    if args.leverage <= 0:
        raise SystemExit("--leverage must be > 0")
    if args.max_plan_age_seconds <= 0:
        raise SystemExit("--max-plan-age-seconds must be > 0")
    if args.max_mid_drift_steps < 0:
        raise SystemExit("--max-mid-drift-steps must be >= 0")
    if args.max_new_orders <= 0:
        raise SystemExit("--max-new-orders must be > 0")
    if args.max_total_notional <= 0:
        raise SystemExit("--max-total-notional must be > 0")
    if args.maker_retries < 0:
        raise SystemExit("--maker-retries must be >= 0")

    plan_path = Path(args.plan_json)
    if not plan_path.exists():
        raise SystemExit(f"plan file not found: {plan_path}")
    plan_report = json.loads(plan_path.read_text(encoding="utf-8"))
    validation = validate_plan_report(
        plan_report=plan_report,
        allow_symbol=args.allow_symbol,
        max_new_orders=args.max_new_orders,
        max_total_notional=args.max_total_notional,
        cancel_stale=args.cancel_stale,
        max_plan_age_seconds=args.max_plan_age_seconds,
        now=datetime.now(timezone.utc),
        enforce_place_limits=not bool(args.apply),
    )

    symbol = str(plan_report.get("symbol", "")).upper().strip()
    step_price = _safe_float(plan_report.get("step_price"))
    book_rows = fetch_futures_book_tickers(symbol=symbol)
    if not book_rows:
        raise SystemExit(f"no book ticker returned for {symbol}")
    live_book = book_rows[0]
    live_bid_price = _safe_float(live_book.get("bid_price"))
    live_ask_price = _safe_float(live_book.get("ask_price"))
    drift_steps = estimate_mid_drift_steps(
        report_mid_price=_safe_float(plan_report.get("mid_price")),
        live_bid_price=live_bid_price,
        live_ask_price=live_ask_price,
        step_price=step_price,
    )
    if drift_steps > args.max_mid_drift_steps:
        validation["errors"].append(
            f"live mid drift is {drift_steps:.2f} steps, above max_mid_drift_steps={args.max_mid_drift_steps:.2f}"
        )
        validation["ok"] = False
    validation["actions"] = preserve_queue_priority_in_execution_actions(
        actions=validation["actions"],
        live_bid_price=live_bid_price,
        live_ask_price=live_ask_price,
        tick_size=_safe_float((plan_report.get("symbol_info") or {}).get("tick_size")),
        min_qty=(plan_report.get("symbol_info") or {}).get("min_qty"),
        min_notional=(plan_report.get("symbol_info") or {}).get("min_notional"),
        step_size=(plan_report.get("symbol_info") or {}).get("step_size"),
    )
    validation["actions"] = apply_reduce_only_no_loss_guard_to_actions(
        actions=validation["actions"],
        plan_report=plan_report,
        strategy_mode=str(plan_report.get("strategy_mode", "")).strip() or "one_way_long",
        live_bid_price=live_bid_price,
        live_ask_price=live_ask_price,
        tick_size=_safe_float((plan_report.get("symbol_info") or {}).get("tick_size")),
        min_qty=(plan_report.get("symbol_info") or {}).get("min_qty"),
        min_notional=(plan_report.get("symbol_info") or {}).get("min_notional"),
        step_size=(plan_report.get("symbol_info") or {}).get("step_size"),
        allow_loss_roles=INVENTORY_UNLOCK_ALLOW_LOSS_ROLES,
    )

    _print_preview(plan_report=plan_report, validation=validation, drift_steps=drift_steps)

    report = {
        "plan_json": str(plan_path),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "apply_requested": bool(args.apply),
        "validation": validation,
        "live_book": {
            "bid_price": live_bid_price,
            "ask_price": live_ask_price,
        },
        "executed": False,
        "canceled_orders": [],
        "placed_orders": [],
        "skipped_orders": [],
        "configuration": {
            "allow_symbol": args.allow_symbol.upper().strip(),
            "margin_type": str(args.margin_type).upper().strip(),
            "leverage": args.leverage,
            "cancel_stale": bool(args.cancel_stale),
        },
        "error": None,
    }

    report_path = Path(args.report_json)
    report_path.parent.mkdir(parents=True, exist_ok=True)

    if not args.apply:
        report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        print("Preview only. Re-run with --apply to send real orders.")
        print(f"JSON report saved: {report_path}")
        return

    if not validation["ok"]:
        report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        raise SystemExit("Refusing to place orders because validation failed")

    try:
        credentials = load_binance_api_credentials()
        if credentials is None:
            raise SystemExit("请先在本机环境变量中配置 BINANCE_API_KEY 和 BINANCE_API_SECRET")
        api_key, api_secret = credentials

        position_mode = fetch_futures_position_mode(api_key, api_secret, recv_window=args.recv_window)
        if _truthy(position_mode.get("dualSidePosition")):
            raise SystemExit("账户当前是双向持仓模式，当前提交器只支持单向模式")

        account_info = fetch_futures_account_info_v3(api_key, api_secret, recv_window=args.recv_window)
        multi_assets_margin = _truthy(account_info.get("multiAssetsMargin"))
        requested_margin_type = str(args.margin_type).upper().strip()
        report["account_mode"] = {
            "multi_assets_margin": multi_assets_margin,
            "requested_margin_type": requested_margin_type,
        }
        if multi_assets_margin and requested_margin_type == "ISOLATED":
            raise SystemExit(
                "账户当前启用了 Multi-Assets Mode，Binance 不允许在该模式下切到 ISOLATED。"
                "请先关闭 Multi-Assets Mode，或改用 --margin-type CROSSED。"
            )
        current_open_orders = fetch_futures_open_orders(symbol, api_key, api_secret, recv_window=args.recv_window)
        current_strategy_open_orders = filter_strategy_open_orders(current_open_orders, symbol)
        current_position = extract_symbol_position(account_info, symbol)
        current_actual_net_qty = _safe_float(current_position.get("positionAmt"))
        current_long_qty = max(current_actual_net_qty, 0.0)
        expected_open_order_count = int(plan_report.get("open_order_count", 0) or 0)
        expected_long_qty = _safe_float(plan_report.get("current_long_qty"))
        if len(current_strategy_open_orders) != expected_open_order_count:
            raise SystemExit("当前未成交委托数量与计划生成时不一致，请先重新生成计划")
        if abs(current_long_qty - expected_long_qty) > 1e-9:
            raise SystemExit("当前持仓与计划生成时不一致，请先重新生成计划")

        validation["actions"] = cap_reduce_only_place_orders_to_position(
            actions=validation["actions"],
            strategy_mode=str(plan_report.get("strategy_mode", "")).strip() or "one_way_long",
            current_actual_net_qty=current_actual_net_qty,
            current_open_orders=current_strategy_open_orders,
        )
        validation["actions"] = apply_reduce_only_no_loss_guard_to_actions(
            actions=validation["actions"],
            plan_report=plan_report,
            strategy_mode=str(plan_report.get("strategy_mode", "")).strip() or "one_way_long",
            live_bid_price=live_bid_price,
            live_ask_price=live_ask_price,
            tick_size=_safe_float((plan_report.get("symbol_info") or {}).get("tick_size")),
            min_qty=(plan_report.get("symbol_info") or {}).get("min_qty"),
            min_notional=(plan_report.get("symbol_info") or {}).get("min_notional"),
            step_size=(plan_report.get("symbol_info") or {}).get("step_size"),
            allow_loss_roles=INVENTORY_UNLOCK_ALLOW_LOSS_ROLES,
        )
        validation = enforce_execution_action_limits(
            validation=validation,
            max_new_orders=args.max_new_orders,
            max_total_notional=args.max_total_notional,
        )
        report["validation"] = validation
        report["reduce_only_position_cap"] = validation["actions"].get("reduce_only_position_cap")
        if validation["actions"]["place_count"] <= 0 and validation["actions"]["cancel_count"] <= 0:
            validation["errors"] = [
                item for item in validation["errors"] if "plan contains no actions to execute" not in item
            ]
            validation["ok"] = not validation["errors"]
            report["idle"] = True
            report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
            print("No executable orders after reduce-only position cap.")
            print(f"JSON report saved: {report_path}")
            return
        if not validation["ok"]:
            report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
            raise SystemExit("Refusing to place orders because validation failed")

        margin_response: dict[str, Any] | None = None
        leverage_response: dict[str, Any] | None = None
        if requested_margin_type != "KEEP":
            try:
                margin_response = post_futures_change_margin_type(
                    symbol=symbol,
                    margin_type=requested_margin_type,
                    api_key=api_key,
                    api_secret=api_secret,
                    recv_window=args.recv_window,
                )
            except RuntimeError as exc:
                if not _ignore_noop_error(exc, ("no need to change margin type",)):
                    raise
                margin_response = {"ignored_error": str(exc)}
        else:
            margin_response = {"skipped": True, "reason": "KEEP"}
        try:
            leverage_response = post_futures_change_initial_leverage(
                symbol=symbol,
                leverage=args.leverage,
                api_key=api_key,
                api_secret=api_secret,
                recv_window=args.recv_window,
            )
        except RuntimeError as exc:
            if not _ignore_noop_error(exc, ("no need to change leverage", "same leverage",)):
                raise
            leverage_response = {"ignored_error": str(exc)}

        report["margin_response"] = margin_response
        report["leverage_response"] = leverage_response

        if args.cancel_stale:
            for stale_order in validation["actions"]["cancel_orders"]:
                order_id = int(stale_order["orderId"]) if str(stale_order.get("orderId", "")).strip() else None
                orig_client_order_id = str(stale_order.get("clientOrderId", "")).strip() or None
                try:
                    cancel_response = delete_futures_order(
                        symbol=symbol,
                        order_id=order_id,
                        orig_client_order_id=orig_client_order_id,
                        api_key=api_key,
                        api_secret=api_secret,
                        recv_window=args.recv_window,
                    )
                except RuntimeError as exc:
                    if not _ignore_noop_error(exc, ("unknown order sent", "-2011")):
                        raise
                    cancel_response = {
                        "ignored_error": str(exc),
                        "orderId": order_id,
                        "origClientOrderId": orig_client_order_id,
                        "status": "ALREADY_GONE",
                    }
                report["canceled_orders"].append(cancel_response)

        tick_size = _safe_float((plan_report.get("symbol_info") or {}).get("tick_size"))
        min_qty = (plan_report.get("symbol_info") or {}).get("min_qty")
        min_notional = (plan_report.get("symbol_info") or {}).get("min_notional")
        strategy_mode = str(plan_report.get("strategy_mode", "")).strip() or "one_way_long"
        for index, order in enumerate(validation["actions"]["place_orders"], start=1):
            role = str(order.get("role", "entry"))
            side = str(order.get("side", "")).upper().strip()
            reduce_only = _resolve_reduce_only_flag(
                strategy_mode=strategy_mode,
                side=side,
                role=role,
            )
            last_exc: RuntimeError | None = None
            for attempt in range(args.maker_retries + 1):
                live_book = fetch_futures_book_tickers(symbol=symbol)[0]
                live_bid = _safe_float(live_book.get("bid_price"))
                live_ask = _safe_float(live_book.get("ask_price"))
                prepared_order, skip_reason = prepare_post_only_order_request(
                    order=order,
                    side=side,
                    live_bid_price=live_bid,
                    live_ask_price=live_ask,
                    tick_size=tick_size,
                    min_qty=min_qty,
                    min_notional=min_notional,
                    step_size=(plan_report.get("symbol_info") or {}).get("step_size"),
                )
                if prepared_order is None:
                    report["skipped_orders"].append(
                        {
                            "request": {
                                "role": role,
                                "side": side,
                                "qty": _safe_float(order.get("qty")),
                                "desired_price": _safe_float(order.get("price")),
                                "attempt": attempt + 1,
                            },
                            "reason": skip_reason or {},
                        }
                    )
                    last_exc = None
                    break
                try:
                    place_response = post_futures_order(
                        symbol=symbol,
                        side=side,
                        quantity=_safe_float(prepared_order.get("qty")),
                        price=_safe_float(prepared_order.get("submitted_price")),
                        api_key=api_key,
                        api_secret=api_secret,
                        recv_window=args.recv_window,
                        time_in_force="GTX",
                        new_client_order_id=_build_client_order_id(symbol=symbol, role=role, index=index),
                        reduce_only=reduce_only,
                    )
                    report["placed_orders"].append(
                        {
                            "request": {
                                "role": role,
                                "side": side,
                                "qty": _safe_float(prepared_order.get("qty")),
                                "desired_price": _safe_float(prepared_order.get("desired_price")),
                                "submitted_price": _safe_float(prepared_order.get("submitted_price")),
                                "submitted_notional": _safe_float(prepared_order.get("submitted_notional")),
                                "attempt": attempt + 1,
                            },
                            "response": place_response,
                        }
                    )
                    last_exc = None
                    break
                except RuntimeError as exc:
                    last_exc = exc
                    message = str(exc).lower()
                    if "-4164" in message and "notional must be no smaller than 5" in message:
                        report["skipped_orders"].append(
                            {
                                "request": {
                                    "role": role,
                                    "side": side,
                                    "qty": _safe_float(prepared_order.get("qty")),
                                    "desired_price": _safe_float(prepared_order.get("desired_price")),
                                    "submitted_price": _safe_float(prepared_order.get("submitted_price")),
                                    "submitted_notional": _safe_float(prepared_order.get("submitted_notional")),
                                    "attempt": attempt + 1,
                                    "reduce_only": reduce_only,
                                },
                                "reason": {
                                    "reason": "exchange_min_notional_reject",
                                    "error": str(exc),
                                },
                            }
                        )
                        last_exc = None
                        break
                    if reduce_only is True and _is_reduce_only_reject(exc):
                        report["skipped_orders"].append(
                            {
                                "request": {
                                    "role": role,
                                    "side": side,
                                    "qty": _safe_float(prepared_order.get("qty")),
                                    "desired_price": _safe_float(prepared_order.get("desired_price")),
                                    "submitted_price": _safe_float(prepared_order.get("submitted_price")),
                                    "submitted_notional": _safe_float(prepared_order.get("submitted_notional")),
                                    "attempt": attempt + 1,
                                    "reduce_only": reduce_only,
                                },
                                "reason": {
                                    "reason": "reduce_only_rejected",
                                    "error": str(exc),
                                },
                            }
                        )
                        last_exc = None
                        break
                    if "-5022" not in message and "post only order will be rejected" not in message:
                        raise
                    if attempt >= args.maker_retries:
                        raise
            if last_exc is not None:
                raise last_exc

        report["executed"] = True
        report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"Submitted {len(report['placed_orders'])} orders.")
        print(f"JSON report saved: {report_path}")
    except (Exception, SystemExit) as exc:
        report["error"] = {
            "type": exc.__class__.__name__,
            "message": str(exc),
            "traceback": traceback.format_exc(),
        }
        report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        raise


if __name__ == "__main__":
    main()
