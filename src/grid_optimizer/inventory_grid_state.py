from __future__ import annotations

from decimal import Decimal
from typing import Any

EPSILON = 1e-12
ANCHOR_UPDATE_ROLES = frozenset({"bootstrap_entry", "grid_entry", "grid_exit"})
OPENING_ROLES = frozenset({"bootstrap_entry", "grid_entry"})
CLOSING_ROLES = frozenset({"grid_exit", "forced_reduce", "tail_cleanup", "spot_app_loss_reduce", "spot_freeze_maker"})
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


def _total_lot_qty_for_side(*, lots: list[dict[str, Any]], side: str) -> float:
    normalized_side = str(side or "").strip().lower()
    return sum(
        max(float(lot.get("qty", 0.0) or 0.0), 0.0)
        for lot in lots
        if str(lot.get("side", "") or "").strip().lower() == normalized_side
    )


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
                "side": str(lot.get("side", "") or ""),
                "qty": take_qty,
                "matched_qty": take_qty,
                "entry_price": max(float(lot.get("entry_price", 0.0) or 0.0), 0.0),
                "opened_at_ms": int(lot.get("opened_at_ms", 0) or 0),
                "source_role": str(lot.get("source_role", "") or ""),
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


def _reduce_loss_quote(*, entry_price: float, reduce_price: float, qty: float, direction_state: str) -> float:
    if direction_state == "long_active":
        return max(0.0, (float(entry_price) - float(reduce_price)) * float(qty))
    if direction_state == "short_active":
        return max(0.0, (float(reduce_price) - float(entry_price)) * float(qty))
    raise ValueError(f"unsupported direction_state for reduce loss: {direction_state}")


def synthetic_frozen_position_report(*, runtime: dict[str, Any], mid_price: float) -> dict[str, Any]:
    frozen_lots = [dict(item) for item in list(runtime.get("frozen_position_lots") or []) if isinstance(item, dict)]
    long_qty = _total_lot_qty_for_side(lots=frozen_lots, side="long")
    short_qty = _total_lot_qty_for_side(lots=frozen_lots, side="short")
    long_cost = sum(
        max(float(item.get("entry_price", 0.0) or 0.0), 0.0) * max(float(item.get("qty", 0.0) or 0.0), 0.0)
        for item in frozen_lots
        if str(item.get("side", "") or "").strip().lower() == "long"
    )
    short_cost = sum(
        max(float(item.get("entry_price", 0.0) or 0.0), 0.0) * max(float(item.get("qty", 0.0) or 0.0), 0.0)
        for item in frozen_lots
        if str(item.get("side", "") or "").strip().lower() == "short"
    )
    safe_mid = max(float(mid_price), 0.0)
    return {
        "long_qty": long_qty,
        "short_qty": short_qty,
        "long_notional": long_qty * safe_mid,
        "short_notional": short_qty * safe_mid,
        "long_cost": long_cost,
        "short_cost": short_cost,
        "pair_eligible_qty": min(long_qty, short_qty),
        "lot_count": len(frozen_lots),
    }


def freeze_inventory_grid_reduce_lots(
    *,
    runtime: dict[str, Any],
    reduce_price: float,
    reduce_qty: float,
    step_price: float,
    freeze_time_ms: int,
    reason: str,
) -> dict[str, Any]:
    if not bool(runtime.get("synthetic_neutral")):
        return {"applied": False, "reason": "not_synthetic_neutral"}
    direction_state = str(runtime.get("direction_state", "flat") or "flat").strip().lower()
    if direction_state not in {"long_active", "short_active"}:
        return {"applied": False, "reason": "not_active"}
    if step_price <= 0:
        raise ValueError("step_price must be positive")

    frozen_side = "long" if direction_state == "long_active" else "short"
    frozen_lots = [dict(item) for item in list(runtime.get("frozen_position_lots") or []) if isinstance(item, dict)]

    lots = [dict(item) for item in list(runtime.get("position_lots") or []) if isinstance(item, dict)]
    remaining_lots, matched = _forced_reduce_lots(
        lots=lots,
        direction_state=direction_state,
        reduce_qty=reduce_qty,
    )
    freeze_qty = sum(max(float(item.get("matched_qty", 0.0) or 0.0), 0.0) for item in matched)
    if freeze_qty <= EPSILON:
        return {"applied": False, "reason": "nothing_to_freeze"}

    reduce_price_float = max(float(reduce_price), 0.0)
    loss_quote = sum(
        _reduce_loss_quote(
            entry_price=max(float(item.get("entry_price", 0.0) or 0.0), 0.0),
            reduce_price=reduce_price_float,
            qty=max(float(item.get("matched_qty", 0.0) or 0.0), 0.0),
            direction_state=direction_state,
        )
        for item in matched
    )
    cost_quote = sum(
        max(float(item.get("entry_price", 0.0) or 0.0), 0.0) * max(float(item.get("matched_qty", 0.0) or 0.0), 0.0)
        for item in matched
    )
    freeze_time = int(freeze_time_ms)
    frozen_lots.extend(
        {
            "lot_id": f"frozen_{str(item.get('lot_id') or 'lot')}_{freeze_time}_{idx}",
            "source_lot_id": str(item.get("lot_id") or ""),
            "side": frozen_side,
            "qty": max(float(item.get("matched_qty", 0.0) or 0.0), 0.0),
            "entry_price": max(float(item.get("entry_price", 0.0) or 0.0), 0.0),
            "opened_at_ms": int(item.get("opened_at_ms", 0) or 0),
            "source_role": str(item.get("source_role", "") or ""),
            "frozen_at_ms": freeze_time,
            "freeze_reason": str(reason or ""),
        }
        for idx, item in enumerate(matched, start=1)
    )

    runtime["position_lots"] = remaining_lots
    runtime["frozen_position_lots"] = frozen_lots
    if not remaining_lots:
        runtime["direction_state"] = "flat"
    return {
        "applied": True,
        "reason": str(reason or ""),
        "side": frozen_side,
        "freeze_qty": freeze_qty,
        "freeze_notional": freeze_qty * reduce_price_float,
        "loss_quote": loss_quote,
        "loss_ratio": loss_quote / cost_quote if cost_quote > EPSILON else 0.0,
        "frozen_lots": [dict(item) for item in matched],
    }


def _consume_frozen_lots_by_side(
    *,
    frozen_lots: list[dict[str, Any]],
    frozen_side: str,
    qty: float,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    normalized_side = str(frozen_side or "").strip().lower()
    if normalized_side not in {"long", "short"}:
        raise ValueError(f"unsupported frozen side: {frozen_side}")
    side_lots = [item for item in frozen_lots if str(item.get("side", "") or "").strip().lower() == frozen_side]
    if _total_lot_qty(lots=side_lots) + EPSILON < qty:
        raise ValueError(f"insufficient frozen {frozen_side} quantity")

    remaining = max(float(qty), 0.0)
    new_frozen_lots: list[dict[str, Any]] = []
    matched: list[dict[str, Any]] = []
    for lot in frozen_lots:
        if str(lot.get("side", "") or "").strip().lower() != frozen_side:
            new_frozen_lots.append(lot)
            continue
        lot_qty = max(float(lot.get("qty", 0.0) or 0.0), 0.0)
        if remaining <= EPSILON:
            new_frozen_lots.append(lot)
            continue
        take_qty = min(lot_qty, remaining)
        matched.append({**dict(lot), "matched_qty": take_qty})
        left_qty = lot_qty - take_qty
        if left_qty > EPSILON:
            kept = dict(lot)
            kept["qty"] = left_qty
            new_frozen_lots.append(kept)
        remaining -= take_qty
    return new_frozen_lots, matched


def _consume_synthetic_frozen_lots(
    *,
    runtime: dict[str, Any],
    side: str,
    qty: float,
) -> None:
    normalized_side = str(side or "").upper().strip()
    frozen_side = "long" if normalized_side == "SELL" else "short" if normalized_side == "BUY" else ""
    if frozen_side not in {"long", "short"}:
        raise ValueError(f"unsupported synthetic frozen release side: {side}")
    frozen_lots = [dict(item) for item in list(runtime.get("frozen_position_lots") or []) if isinstance(item, dict)]
    new_frozen_lots, _ = _consume_frozen_lots_by_side(
        frozen_lots=frozen_lots,
        frozen_side=frozen_side,
        qty=qty,
    )
    runtime["frozen_position_lots"] = new_frozen_lots


def apply_synthetic_frozen_pair_release(
    *,
    runtime: dict[str, Any],
    release_qty: float | None,
    release_time_ms: int,
    reason: str,
) -> dict[str, Any]:
    if not bool(runtime.get("synthetic_neutral")):
        return {"applied": False, "reason": "not_synthetic_neutral"}
    frozen_lots = [dict(item) for item in list(runtime.get("frozen_position_lots") or []) if isinstance(item, dict)]
    eligible_qty = min(
        _total_lot_qty_for_side(lots=frozen_lots, side="long"),
        _total_lot_qty_for_side(lots=frozen_lots, side="short"),
    )
    target_qty = eligible_qty if release_qty is None or float(release_qty or 0.0) <= EPSILON else min(float(release_qty), eligible_qty)
    if target_qty <= EPSILON:
        return {"applied": False, "reason": "no_pair_eligible_qty", "release_qty": 0.0}

    after_long, matched_long = _consume_frozen_lots_by_side(
        frozen_lots=frozen_lots,
        frozen_side="long",
        qty=target_qty,
    )
    after_short, matched_short = _consume_frozen_lots_by_side(
        frozen_lots=after_long,
        frozen_side="short",
        qty=target_qty,
    )
    long_cost = sum(
        max(float(item.get("entry_price", 0.0) or 0.0), 0.0) * max(float(item.get("matched_qty", 0.0) or 0.0), 0.0)
        for item in matched_long
    )
    short_cost = sum(
        max(float(item.get("entry_price", 0.0) or 0.0), 0.0) * max(float(item.get("matched_qty", 0.0) or 0.0), 0.0)
        for item in matched_short
    )
    runtime["frozen_position_lots"] = after_short
    runtime["last_strategy_trade_time_ms"] = int(release_time_ms)
    return {
        "applied": True,
        "reason": str(reason or ""),
        "release_qty": target_qty,
        "long_cost": long_cost,
        "short_cost": short_cost,
        "pair_release_pnl_quote": short_cost - long_cost,
    }


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

    if normalized_role in {"synthetic_frozen_release", "synthetic_frozen_manual_release"}:
        _consume_synthetic_frozen_lots(runtime=runtime, side=normalized_side, qty=fill_qty)
        runtime["last_strategy_trade_time_ms"] = int(fill_time_ms)
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
        selected.append(
            {
                "lot_id": str(lot.get("lot_id") or ""),
                "side": str(lot.get("side", "") or ""),
                "qty": take_qty,
                "entry_price": max(float(lot.get("entry_price", 0.0) or 0.0), 0.0),
                "opened_at_ms": int(lot.get("opened_at_ms", 0) or 0),
                "source_role": str(lot.get("source_role", "") or ""),
            }
        )
        forced_reduce_cost_steps += _forced_reduce_cost_steps(
            entry_price=float(lot.get("entry_price", 0.0) or 0.0),
            reduce_price=reduce_price,
            step_price=step_price,
            direction_state=direction_state,
        )
        remaining -= take_qty
    return {"lots": selected, "forced_reduce_cost_steps": forced_reduce_cost_steps}
