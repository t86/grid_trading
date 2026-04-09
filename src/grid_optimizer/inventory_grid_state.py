from __future__ import annotations

from decimal import Decimal
from typing import Any

EPSILON = 1e-12
ANCHOR_UPDATE_ROLES = frozenset({"bootstrap_entry", "grid_entry", "grid_exit"})


def new_inventory_grid_runtime(*, market_type: str) -> dict[str, Any]:
    normalized = str(market_type or "").strip().lower()
    if normalized not in {"futures", "spot"}:
        raise ValueError(f"unsupported market_type: {market_type}")
    return {
        "market_type": normalized,
        "direction_state": "flat",
        "risk_state": "normal",
        "grid_anchor_price": 0.0,
        "position_lots": [],
        "pair_credit_steps": 0,
        "last_strategy_trade_time_ms": 0,
        "recovery_mode": "live",
        "recovery_errors": [],
    }


def _next_direction_state(*, market_type: str, current_state: str, side: str) -> str:
    normalized_side = str(side or "").upper().strip()
    if current_state != "flat":
        return current_state
    if normalized_side == "BUY":
        return "long_active"
    if market_type == "futures" and normalized_side == "SELL":
        return "short_active"
    return current_state


def _consume_lots(
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
        qty = max(float(lot.get("qty", 0.0) or 0.0), 0.0)
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


def _accumulate_pair_credit_steps(*, matched: list[dict[str, Any]], exit_price: float, step_price: float) -> int:
    total = 0
    if step_price <= 0:
        return total
    decimal_step_price = Decimal(str(step_price))
    decimal_exit_price = Decimal(str(exit_price))
    for item in matched:
        entry_price = max(float(item.get("entry_price", 0.0) or 0.0), 0.0)
        matched_qty = max(float(item.get("matched_qty", 0.0) or 0.0), 0.0)
        if entry_price <= 0 or matched_qty <= EPSILON:
            continue
        gross_steps = abs(decimal_exit_price - Decimal(str(entry_price))) / decimal_step_price
        total += int(gross_steps)
    return total


def apply_inventory_grid_fill(
    *,
    runtime: dict[str, Any],
    role: str,
    side: str,
    price: float,
    qty: float,
    fill_time_ms: int,
    step_price: float = 0.01,
) -> None:
    normalized_role = str(role or "").strip().lower()
    normalized_side = str(side or "").upper().strip()
    fill_price = max(float(price), 0.0)
    fill_qty = max(float(qty), 0.0)
    if fill_price <= 0 or fill_qty <= EPSILON:
        return

    runtime["direction_state"] = _next_direction_state(
        market_type=str(runtime.get("market_type", "futures")),
        current_state=str(runtime.get("direction_state", "flat")),
        side=normalized_side,
    )
    lots = list(runtime.get("position_lots") or [])

    if normalized_side == "BUY" and runtime["direction_state"] == "long_active":
        lots.append(
            {
                "lot_id": f"lot_{fill_time_ms}_{len(lots) + 1}",
                "side": "long",
                "qty": fill_qty,
                "entry_price": fill_price,
                "opened_at_ms": int(fill_time_ms),
                "source_role": normalized_role,
            }
        )
    elif normalized_side == "SELL" and runtime["direction_state"] == "long_active":
        lots, matched = _consume_lots(lots=lots, qty_to_consume=fill_qty)
        if normalized_role == "grid_exit":
            runtime["pair_credit_steps"] = int(runtime.get("pair_credit_steps", 0) or 0) + _accumulate_pair_credit_steps(
                matched=matched,
                exit_price=fill_price,
                step_price=step_price,
            )
        if not lots:
            runtime["direction_state"] = "flat"
    elif normalized_side == "SELL" and runtime["direction_state"] == "short_active":
        lots.append(
            {
                "lot_id": f"lot_{fill_time_ms}_{len(lots) + 1}",
                "side": "short",
                "qty": fill_qty,
                "entry_price": fill_price,
                "opened_at_ms": int(fill_time_ms),
                "source_role": normalized_role,
            }
        )
    elif normalized_side == "BUY" and runtime["direction_state"] == "short_active":
        lots, matched = _consume_lots(lots=lots, qty_to_consume=fill_qty)
        if normalized_role == "grid_exit":
            runtime["pair_credit_steps"] = int(runtime.get("pair_credit_steps", 0) or 0) + _accumulate_pair_credit_steps(
                matched=matched,
                exit_price=fill_price,
                step_price=step_price,
            )
        if not lots:
            runtime["direction_state"] = "flat"

    runtime["position_lots"] = lots
    runtime["last_strategy_trade_time_ms"] = int(fill_time_ms)
    if normalized_role in ANCHOR_UPDATE_ROLES:
        runtime["grid_anchor_price"] = fill_price


def build_forced_reduce_lot_plan(
    *,
    runtime: dict[str, Any],
    reduce_price: float,
    reduce_qty: float,
    step_price: float,
) -> dict[str, Any]:
    direction_state = str(runtime.get("direction_state", "flat"))
    lots = list(runtime.get("position_lots") or [])
    if direction_state == "long_active":
        ordered = sorted(lots, key=lambda item: float(item.get("entry_price", 0.0) or 0.0), reverse=True)
        step_value = lambda lot: max(0.0, (float(lot.get("entry_price", 0.0) or 0.0) - reduce_price) / step_price)
    else:
        ordered = sorted(lots, key=lambda item: float(item.get("entry_price", 0.0) or 0.0))
        step_value = lambda lot: max(0.0, (reduce_price - float(lot.get("entry_price", 0.0) or 0.0)) / step_price)

    remaining = max(float(reduce_qty), 0.0)
    selected: list[dict[str, Any]] = []
    forced_reduce_cost_steps = 0
    for lot in ordered:
        if remaining <= EPSILON:
            break
        lot_qty = max(float(lot.get("qty", 0.0) or 0.0), 0.0)
        take_qty = min(lot_qty, remaining)
        if take_qty <= EPSILON:
            continue
        selected.append({"lot_id": str(lot.get("lot_id") or ""), "qty": take_qty})
        forced_reduce_cost_steps += int(step_value(lot))
        remaining -= take_qty
    return {"lots": selected, "forced_reduce_cost_steps": forced_reduce_cost_steps}
