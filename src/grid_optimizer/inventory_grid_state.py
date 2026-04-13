from __future__ import annotations

from decimal import Decimal
from typing import Any

EPSILON = 1e-12
ANCHOR_UPDATE_ROLES = frozenset({"bootstrap_entry", "grid_entry", "grid_exit"})
OPENING_ROLES = frozenset({"bootstrap_entry", "grid_entry"})
CLOSING_ROLES = frozenset({"grid_exit", "forced_reduce", "tail_cleanup"})
VALID_DIRECTION_STATES = frozenset({"flat", "long_active", "short_active"})


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


def _total_lot_qty(*, lots: list[dict[str, Any]]) -> float:
    return sum(max(float(lot.get("qty", 0.0) or 0.0), 0.0) for lot in lots)


def _forced_reduce_lots(
    *,
    lots: list[dict[str, Any]],
    direction_state: str,
    reduce_qty: float,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if direction_state == "long_active":
        ordered = sorted(
            enumerate(lots),
            key=lambda item: (float(item[1].get("entry_price", 0.0) or 0.0), item[0]),
            reverse=True,
        )
    else:
        ordered = sorted(
            enumerate(lots),
            key=lambda item: (float(item[1].get("entry_price", 0.0) or 0.0), item[0]),
        )

    remaining = max(float(reduce_qty), 0.0)
    selection: dict[str, float] = {}
    selected: list[dict[str, Any]] = []
    for _, lot in ordered:
        if remaining <= EPSILON:
            break
        lot_id = str(lot.get("lot_id") or "")
        lot_qty = max(float(lot.get("qty", 0.0) or 0.0), 0.0)
        take_qty = min(lot_qty, remaining)
        if take_qty <= EPSILON:
            continue
        selection[lot_id] = selection.get(lot_id, 0.0) + take_qty
        selected.append(
            {
                "lot_id": lot_id,
                "qty": take_qty,
                "matched_qty": take_qty,
                "entry_price": max(float(lot.get("entry_price", 0.0) or 0.0), 0.0),
            }
        )
        remaining -= take_qty

    new_lots: list[dict[str, Any]] = []
    for lot in lots:
        lot_id = str(lot.get("lot_id") or "")
        take_qty = selection.get(lot_id, 0.0)
        if take_qty <= EPSILON:
            new_lots.append(dict(lot))
            continue
        lot_qty = max(float(lot.get("qty", 0.0) or 0.0), 0.0)
        left_qty = lot_qty - take_qty
        if left_qty > EPSILON:
            kept = dict(lot)
            kept["qty"] = left_qty
            new_lots.append(kept)
    return new_lots, selected


def select_grid_exit_lots(
    *,
    lots: list[dict[str, Any]],
    direction_state: str,
    close_qty: float,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if direction_state == "long_active":
        ordered = sorted(
            enumerate(lots),
            key=lambda item: (float(item[1].get("entry_price", 0.0) or 0.0), item[0]),
        )
    elif direction_state == "short_active":
        ordered = sorted(
            enumerate(lots),
            key=lambda item: (float(item[1].get("entry_price", 0.0) or 0.0), item[0]),
            reverse=True,
        )
    else:
        raise ValueError(f"unsupported direction_state for grid exit: {direction_state}")

    remaining = max(float(close_qty), 0.0)
    selection: dict[str, float] = {}
    selected: list[dict[str, Any]] = []
    for _, lot in ordered:
        if remaining <= EPSILON:
            break
        lot_id = str(lot.get("lot_id") or "")
        lot_qty = max(float(lot.get("qty", 0.0) or 0.0), 0.0)
        take_qty = min(lot_qty, remaining)
        if take_qty <= EPSILON:
            continue
        selection[lot_id] = selection.get(lot_id, 0.0) + take_qty
        selected.append(
            {
                "lot_id": lot_id,
                "qty": take_qty,
                "matched_qty": take_qty,
                "entry_price": max(float(lot.get("entry_price", 0.0) or 0.0), 0.0),
            }
        )
        remaining -= take_qty

    new_lots: list[dict[str, Any]] = []
    for lot in lots:
        lot_id = str(lot.get("lot_id") or "")
        take_qty = selection.get(lot_id, 0.0)
        if take_qty <= EPSILON:
            new_lots.append(dict(lot))
            continue
        lot_qty = max(float(lot.get("qty", 0.0) or 0.0), 0.0)
        left_qty = lot_qty - take_qty
        if left_qty > EPSILON:
            kept = dict(lot)
            kept["qty"] = left_qty
            new_lots.append(kept)
    return new_lots, selected


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


def _forced_reduce_cost_steps(*, entry_price: float, reduce_price: float, step_price: float, direction_state: str) -> int:
    if step_price <= 0:
        return 0
    if direction_state == "long_active":
        return int(max(0.0, (float(entry_price) - float(reduce_price)) / float(step_price)))
    if direction_state == "short_active":
        return int(max(0.0, (float(reduce_price) - float(entry_price)) / float(step_price)))
    raise ValueError(f"unsupported direction_state for forced reduce cost: {direction_state}")


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

    market_type = str(runtime.get("market_type", "futures")).strip().lower()
    current_state = str(runtime.get("direction_state", "flat")).strip().lower()
    if current_state not in VALID_DIRECTION_STATES:
        raise ValueError(f"unsupported direction_state: {runtime.get('direction_state')}")

    lots = list(runtime.get("position_lots") or [])

    if current_state == "flat":
        if normalized_side == "BUY" and normalized_role in OPENING_ROLES:
            runtime["direction_state"] = "long_active"
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
        elif market_type == "futures" and normalized_side == "SELL" and normalized_role in OPENING_ROLES:
            runtime["direction_state"] = "short_active"
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
        else:
            raise ValueError(
                f"cannot apply {normalized_role} {normalized_side} fill while flat in market_type={market_type}"
            )
    elif current_state == "long_active":
        if normalized_side == "BUY" and normalized_role in OPENING_ROLES:
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
        elif normalized_side == "SELL" and normalized_role in CLOSING_ROLES:
            if _total_lot_qty(lots=lots) + EPSILON < fill_qty:
                raise ValueError("insufficient long quantity for sell fill")
            if normalized_role == "forced_reduce":
                lots, matched = _forced_reduce_lots(
                    lots=lots,
                    direction_state=current_state,
                    reduce_qty=fill_qty,
                )
                forced_reduce_cost_steps = sum(
                    _forced_reduce_cost_steps(
                        entry_price=max(float(item.get("entry_price", 0.0) or 0.0), 0.0),
                        reduce_price=fill_price,
                        step_price=step_price,
                        direction_state=current_state,
                    )
                    for item in matched
                )
                runtime["pair_credit_steps"] = max(
                    0,
                    int(runtime.get("pair_credit_steps", 0) or 0) - forced_reduce_cost_steps,
                )
            elif normalized_role == "grid_exit":
                lots, matched = select_grid_exit_lots(
                    lots=lots,
                    direction_state=current_state,
                    close_qty=fill_qty,
                )
            else:
                lots, matched = _consume_lots(lots=lots, qty_to_consume=fill_qty)
            if normalized_role == "grid_exit":
                runtime["pair_credit_steps"] = int(runtime.get("pair_credit_steps", 0) or 0) + _accumulate_pair_credit_steps(
                    matched=matched,
                    exit_price=fill_price,
                    step_price=step_price,
                )
            if not lots:
                runtime["direction_state"] = "flat"
        else:
            raise ValueError(
                f"cannot apply {normalized_role} {normalized_side} fill while long_active"
            )
    elif current_state == "short_active":
        if normalized_side == "SELL" and normalized_role in OPENING_ROLES:
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
        elif normalized_side == "BUY" and normalized_role in CLOSING_ROLES:
            if _total_lot_qty(lots=lots) + EPSILON < fill_qty:
                raise ValueError("insufficient short quantity for buy fill")
            if normalized_role == "forced_reduce":
                lots, matched = _forced_reduce_lots(
                    lots=lots,
                    direction_state=current_state,
                    reduce_qty=fill_qty,
                )
                forced_reduce_cost_steps = sum(
                    _forced_reduce_cost_steps(
                        entry_price=max(float(item.get("entry_price", 0.0) or 0.0), 0.0),
                        reduce_price=fill_price,
                        step_price=step_price,
                        direction_state=current_state,
                    )
                    for item in matched
                )
                runtime["pair_credit_steps"] = max(
                    0,
                    int(runtime.get("pair_credit_steps", 0) or 0) - forced_reduce_cost_steps,
                )
            elif normalized_role == "grid_exit":
                lots, matched = select_grid_exit_lots(
                    lots=lots,
                    direction_state=current_state,
                    close_qty=fill_qty,
                )
            else:
                lots, matched = _consume_lots(lots=lots, qty_to_consume=fill_qty)
            if normalized_role == "grid_exit":
                runtime["pair_credit_steps"] = int(runtime.get("pair_credit_steps", 0) or 0) + _accumulate_pair_credit_steps(
                    matched=matched,
                    exit_price=fill_price,
                    step_price=step_price,
                )
            if not lots:
                runtime["direction_state"] = "flat"
        else:
            raise ValueError(
                f"cannot apply {normalized_role} {normalized_side} fill while short_active"
            )

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
    if direction_state not in {"long_active", "short_active"}:
        raise ValueError(f"unsupported direction_state for forced reduce: {runtime.get('direction_state')}")
    if step_price <= 0:
        raise ValueError("step_price must be positive")
    lots = list(runtime.get("position_lots") or [])
    if direction_state == "long_active":
        ordered = sorted(lots, key=lambda item: float(item.get("entry_price", 0.0) or 0.0), reverse=True)
    else:
        ordered = sorted(lots, key=lambda item: float(item.get("entry_price", 0.0) or 0.0))

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
        forced_reduce_cost_steps += _forced_reduce_cost_steps(
            entry_price=float(lot.get("entry_price", 0.0) or 0.0),
            reduce_price=reduce_price,
            step_price=step_price,
            direction_state=direction_state,
        )
        remaining -= take_qty
    return {"lots": selected, "forced_reduce_cost_steps": forced_reduce_cost_steps}
