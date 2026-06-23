from __future__ import annotations

from typing import Any


EPSILON = 1e-12
LEDGER_SCHEMA = "spot_frozen_ledger_v1"


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
