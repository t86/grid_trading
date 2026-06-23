from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Any


EPSILON = 1e-12
LEDGER_SCHEMA = "spot_frozen_ledger_v1"


@dataclass(frozen=True)
class FreezeConfig:
    enabled: bool = False
    deviation_notional: float = 0.0
    min_loss_ratio: float = 0.0
    max_per_cycle_notional: float = 0.0
    total_cap_notional: float = 0.0
    pair_release_enabled: bool = False
    profit_release_enabled: bool = False
    release_profit_ratio: float = 0.05


@dataclass(frozen=True)
class DeviationLossResult:
    usable: bool
    loss_ratio: float
    cost_avg: float
    reason: str


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def new_ledger() -> dict[str, Any]:
    return {
        "schema": LEDGER_SCHEMA,
        "long_lots": [],
        "short_lots": [],
        "frozen_long_qty": 0.0,
        "frozen_short_qty": 0.0,
        "pending_contract_actions": [],
        "last_reconcile_at": None,
    }


def _ensure_ledger(ledger: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(ledger, dict):
        return new_ledger()
    normalized = dict(ledger)
    normalized.setdefault("schema", LEDGER_SCHEMA)
    normalized.setdefault("long_lots", [])
    normalized.setdefault("short_lots", [])
    normalized.setdefault("pending_contract_actions", [])
    normalized.setdefault("last_reconcile_at", None)
    normalized.setdefault("frozen_long_qty", 0.0)
    normalized.setdefault("frozen_short_qty", 0.0)
    normalized["long_lots"] = [dict(lot) for lot in normalized.get("long_lots", [])]
    normalized["short_lots"] = [dict(lot) for lot in normalized.get("short_lots", [])]
    normalized["pending_contract_actions"] = [dict(action) for action in normalized.get("pending_contract_actions", [])]
    return normalized


def ledger_totals(ledger: dict[str, Any]) -> tuple[float, float]:
    long_qty = sum(max(_safe_float(lot.get("qty")), 0.0) for lot in ledger.get("long_lots", []))
    short_qty = sum(max(_safe_float(lot.get("qty")), 0.0) for lot in ledger.get("short_lots", []))
    ledger["frozen_long_qty"] = long_qty
    ledger["frozen_short_qty"] = short_qty
    return long_qty, short_qty


def pending_signed_short_delta(ledger: dict[str, Any]) -> float:
    delta = 0.0
    for action in ledger.get("pending_contract_actions", []):
        if str(action.get("position_side", "") or "").upper() != "SHORT":
            continue
        qty = max(_safe_float(action.get("qty")), 0.0)
        side = str(action.get("side", "") or "").upper()
        if side == "BUY":
            delta -= qty
        elif side == "SELL":
            delta += qty
    return delta


def expected_short_position_now(base_hedge_qty: float, ledger: dict[str, Any]) -> float:
    long_qty, short_qty = ledger_totals(ledger)
    expected_short = max(_safe_float(base_hedge_qty), 0.0) - long_qty + short_qty
    return expected_short - pending_signed_short_delta(ledger)


def reconcile_short_position(
    base_hedge_qty: float,
    contract_short_qty: float,
    ledger: dict[str, Any],
    tolerance_qty: float,
) -> tuple[bool, float]:
    expected = expected_short_position_now(base_hedge_qty, ledger)
    tolerance = max(_safe_float(tolerance_qty), 0.0)
    return abs(max(_safe_float(contract_short_qty), 0.0) - expected) <= tolerance + EPSILON, expected


def _unusable_deviation_loss(reason: str) -> DeviationLossResult:
    return DeviationLossResult(usable=False, loss_ratio=0.0, cost_avg=0.0, reason=reason)


def compute_deviation_loss(
    runtime: dict[str, Any],
    neutral_base_qty: float,
    mid_price: float,
    deviation_side: str,
    deviation_qty: float,
) -> DeviationLossResult:
    del neutral_base_qty

    if bool(runtime.get("synthetic_cost_unknown")):
        return _unusable_deviation_loss("synthetic_cost_unknown")
    if str(runtime.get("recovery_mode", "live") or "").strip().lower() != "live":
        return _unusable_deviation_loss("recovery_mode_not_live")

    side = str(deviation_side or "").strip().lower()
    if side not in {"long", "short"}:
        return _unusable_deviation_loss("invalid_side")

    required_qty = max(_safe_float(deviation_qty), 0.0)
    if required_qty <= EPSILON:
        return _unusable_deviation_loss("invalid_qty")

    lots = [
        dict(lot)
        for lot in runtime.get("position_lots", [])
        if str(lot.get("side", "") or "").strip().lower() == side and max(_safe_float(lot.get("qty")), 0.0) > EPSILON
    ]
    reverse = side == "long"
    lots.sort(key=lambda lot: _safe_float(lot.get("entry_price")), reverse=reverse)

    remaining = required_qty
    selected_notional = 0.0
    selected_qty = 0.0
    invalid_cost_seen = False
    for lot in lots:
        if remaining <= EPSILON:
            break
        if str(lot.get("freeze_reason", "") or "") == "synthetic_cost_unknown":
            return _unusable_deviation_loss("lot_cost_unknown")
        qty = max(_safe_float(lot.get("qty")), 0.0)
        cost = _safe_float(lot.get("entry_price"))
        if cost <= EPSILON:
            invalid_cost_seen = True
            continue
        take_qty = min(qty, remaining)
        selected_qty += take_qty
        selected_notional += take_qty * cost
        remaining -= take_qty

    if selected_qty + EPSILON < required_qty:
        if invalid_cost_seen and selected_qty <= EPSILON:
            return _unusable_deviation_loss("invalid_cost")
        return _unusable_deviation_loss("insufficient_lots")
    cost_avg = selected_notional / selected_qty if selected_qty > EPSILON else 0.0
    if cost_avg <= EPSILON:
        return _unusable_deviation_loss("invalid_cost")

    mid = _safe_float(mid_price)
    if side == "long":
        loss_ratio = max(cost_avg - mid, 0.0) / cost_avg
    else:
        loss_ratio = max(mid - cost_avg, 0.0) / cost_avg
    return DeviationLossResult(usable=True, loss_ratio=loss_ratio, cost_avg=cost_avg, reason="ok")


def _round_down_qty(qty: float, qty_step: float) -> float:
    step = _safe_float(qty_step)
    if step <= EPSILON:
        return max(_safe_float(qty), 0.0)
    return math.floor(max(_safe_float(qty), 0.0) / step) * step


def _avg_spot_price(response: dict[str, Any], fallback_price: float) -> float:
    executed_qty = _safe_float(response.get("executedQty"))
    quote_qty = _safe_float(response.get("cummulativeQuoteQty"))
    if executed_qty > EPSILON and quote_qty > EPSILON:
        return quote_qty / executed_qty
    avg_price = _safe_float(response.get("avg_price"))
    if avg_price > EPSILON:
        return avg_price
    return max(_safe_float(fallback_price), 0.0)


def _set_lot_pending(ledger: dict[str, Any], lot_id: str, pending: bool) -> None:
    for key in ("long_lots", "short_lots"):
        for lot in ledger.get(key, []):
            if str(lot.get("lot_id", "") or "") == lot_id:
                lot["hedge_pending"] = pending


def _repair_pending_actions(
    *,
    symbol: str,
    ledger: dict[str, Any],
    place_contract: Any,
    dry_run: bool,
    actions: list[dict[str, Any]],
    alerts: list[str],
) -> None:
    remaining: list[dict[str, Any]] = []
    for pending in ledger.get("pending_contract_actions", []):
        side = str(pending.get("side", "") or "").upper()
        qty = max(_safe_float(pending.get("qty")), 0.0)
        lot_id = str(pending.get("lot_id", "") or "")
        if side not in {"BUY", "SELL"} or qty <= EPSILON:
            continue
        if dry_run:
            actions.append({"type": "pending_repair_dry_run", "side": side, "qty": qty, "lot_id": lot_id})
            remaining.append(pending)
            continue
        try:
            place_contract(symbol=symbol, side=side, qty=qty, position_side="SHORT")
        except Exception:
            alerts.append("pending_contract_failed")
            remaining.append(pending)
            continue
        _set_lot_pending(ledger, lot_id, False)
        actions.append({"type": "pending_repaired", "side": side, "qty": qty, "lot_id": lot_id})
    ledger["pending_contract_actions"] = remaining


def freeze_cycle(
    *,
    symbol: str,
    neutral_base_qty: float,
    spot_inventory_qty: float,
    mid_price: float,
    mark_price: float,
    ledger: dict[str, Any],
    contract_short_qty: float,
    base_hedge_qty: float,
    deviation_loss_ratio: float,
    loss_ratio_usable: bool,
    config: FreezeConfig,
    qty_step: float,
    tolerance_qty: float,
    now: str,
    place_spot: Any,
    place_contract: Any,
    dry_run: bool,
) -> dict[str, Any]:
    del mark_price

    working = _ensure_ledger(ledger)
    actions: list[dict[str, Any]] = []
    alerts: list[str] = []

    reconcile_ok, expected_short = reconcile_short_position(base_hedge_qty, contract_short_qty, working, tolerance_qty)
    working["last_reconcile_at"] = now
    if not reconcile_ok:
        alerts.append(f"reconcile_drift: real={contract_short_qty} expected={expected_short}")
        return {"ledger": working, "actions": actions, "reconcile_ok": False, "alerts": alerts}

    if working.get("pending_contract_actions"):
        _repair_pending_actions(
            symbol=symbol,
            ledger=working,
            place_contract=place_contract,
            dry_run=dry_run,
            actions=actions,
            alerts=alerts,
        )
        ledger_totals(working)
        if working.get("pending_contract_actions"):
            return {"ledger": working, "actions": actions, "reconcile_ok": True, "alerts": alerts}

    if not config.enabled:
        ledger_totals(working)
        return {"ledger": working, "actions": actions, "reconcile_ok": True, "alerts": alerts}

    deviation_qty_signed = _safe_float(spot_inventory_qty) - _safe_float(neutral_base_qty)
    mid = max(_safe_float(mid_price), 0.0)
    deviation_notional = abs(deviation_qty_signed) * mid
    if mid <= EPSILON or deviation_notional <= max(_safe_float(config.deviation_notional), 0.0) + EPSILON:
        ledger_totals(working)
        return {"ledger": working, "actions": actions, "reconcile_ok": True, "alerts": alerts}
    if not loss_ratio_usable:
        alerts.append("cost_not_usable")
        ledger_totals(working)
        return {"ledger": working, "actions": actions, "reconcile_ok": True, "alerts": alerts}
    if _safe_float(deviation_loss_ratio) + EPSILON < max(_safe_float(config.min_loss_ratio), 0.0):
        ledger_totals(working)
        return {"ledger": working, "actions": actions, "reconcile_ok": True, "alerts": alerts}

    long_qty, short_qty = ledger_totals(working)
    cap = max(_safe_float(config.total_cap_notional), 0.0)
    frozen_notional = (long_qty + short_qty) * mid
    if cap > EPSILON and frozen_notional + EPSILON >= cap:
        alerts.append("total_cap_reached")
        return {"ledger": working, "actions": actions, "reconcile_ok": True, "alerts": alerts}

    max_qty = abs(deviation_qty_signed)
    per_cycle_cap = max(_safe_float(config.max_per_cycle_notional), 0.0)
    if per_cycle_cap > EPSILON:
        max_qty = min(max_qty, per_cycle_cap / mid)
    if cap > EPSILON:
        max_qty = min(max_qty, max((cap - frozen_notional) / mid, 0.0))
    qty = _round_down_qty(max_qty, qty_step)
    if qty <= EPSILON:
        alerts.append("qty_too_small")
        return {"ledger": working, "actions": actions, "reconcile_ok": True, "alerts": alerts}

    is_long_deviation = deviation_qty_signed > 0.0
    spot_side = "SELL" if is_long_deviation else "BUY"
    contract_side = "BUY" if is_long_deviation else "SELL"
    lot_key = "long_lots" if is_long_deviation else "short_lots"
    reason = "freeze_long_hedge" if is_long_deviation else "freeze_short_hedge"
    lot_id = f"spot_freeze_{len(working['long_lots']) + len(working['short_lots']) + 1}"

    if dry_run:
        actions.append({"type": "freeze_dry_run", "side": "long" if is_long_deviation else "short", "qty": qty})
        return {"ledger": working, "actions": actions, "reconcile_ok": True, "alerts": alerts}

    try:
        spot_response = place_spot(symbol=symbol, side=spot_side, qty=qty)
    except Exception:
        alerts.append("spot_failed")
        return {"ledger": working, "actions": actions, "reconcile_ok": True, "alerts": alerts}
    filled_qty = _round_down_qty(_safe_float(spot_response.get("executedQty")), qty_step)
    if filled_qty <= EPSILON:
        alerts.append("spot_no_fill")
        return {"ledger": working, "actions": actions, "reconcile_ok": True, "alerts": alerts}

    lot = {
        "lot_id": lot_id,
        "qty": filled_qty,
        "cost_price": _avg_spot_price(spot_response, mid),
        "frozen_at": now,
        "frozen_mid": mid,
        "hedge_pending": False,
    }
    try:
        place_contract(symbol=symbol, side=contract_side, qty=filled_qty, position_side="SHORT")
        actions.append({"type": "freeze", "side": "long" if is_long_deviation else "short", "qty": filled_qty})
    except Exception:
        lot["hedge_pending"] = True
        working["pending_contract_actions"].append(
            {"side": contract_side, "position_side": "SHORT", "qty": filled_qty, "reason": reason, "lot_id": lot_id}
        )
        alerts.append("contract_failed")
        actions.append({"type": "freeze_pending", "side": "long" if is_long_deviation else "short", "qty": filled_qty})

    working[lot_key].append(lot)
    ledger_totals(working)
    return {"ledger": working, "actions": actions, "reconcile_ok": True, "alerts": alerts}
