from __future__ import annotations

from dataclasses import dataclass
from typing import Any


EPSILON = 1e-12
LEDGER_SCHEMA = "spot_frozen_ledger_v1"


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
