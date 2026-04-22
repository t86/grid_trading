from __future__ import annotations

from typing import Any

from .inventory_grid_state import apply_inventory_grid_fill, new_inventory_grid_runtime

POSITION_QTY_EPSILON = 1e-9


def _mark_conservative(runtime: dict[str, Any], *, error: str) -> dict[str, Any]:
    runtime["recovery_mode"] = "conservative_reduce_only"
    runtime["recovery_errors"] = [error]
    runtime["risk_state"] = "hard_reduce_only"
    runtime["pair_credit_steps"] = 0
    return runtime


def _total_position_qty(*, runtime: dict[str, Any]) -> float:
    return sum(max(float(lot.get("qty", 0.0) or 0.0), 0.0) for lot in list(runtime.get("position_lots") or []))


def _apply_conflicting_bootstrap_fill(
    *,
    runtime: dict[str, Any],
    side: str,
    price: float,
    qty: float,
    fill_time_ms: int,
    step_price: float,
) -> None:
    current_state = str(runtime.get("direction_state", "flat")).strip().lower()
    close_side = "SELL" if current_state == "long_active" else "BUY"
    available_qty = _total_position_qty(runtime=runtime)
    close_qty = min(max(float(qty), 0.0), available_qty)
    if close_qty > POSITION_QTY_EPSILON:
        apply_inventory_grid_fill(
            runtime=runtime,
            role="tail_cleanup",
            side=close_side,
            price=price,
            qty=close_qty,
            fill_time_ms=fill_time_ms,
            step_price=step_price,
        )
    remainder_qty = max(float(qty), 0.0) - close_qty
    if remainder_qty > POSITION_QTY_EPSILON:
        apply_inventory_grid_fill(
            runtime=runtime,
            role="bootstrap_entry",
            side=side,
            price=price,
            qty=remainder_qty,
            fill_time_ms=fill_time_ms,
            step_price=step_price,
        )


def rebuild_inventory_grid_runtime(
    *,
    market_type: str,
    trades: list[dict[str, Any]],
    order_refs: dict[str, dict[str, Any]],
    step_price: float,
    current_position_qty: float = 0.0,
) -> dict[str, Any]:
    runtime = new_inventory_grid_runtime(market_type=market_type)

    ordered_trades = sorted(
        list(trades or []),
        key=lambda row: (
            int(row.get("time", 0) or 0),
            int(row.get("id", 0) or 0),
        ),
    )

    replayed_any = False
    for trade in ordered_trades:
        order_id = str(int(trade.get("orderId", 0) or 0))
        order_ref = order_refs.get(order_id)
        if not isinstance(order_ref, dict):
            return _mark_conservative(runtime, error="missing_order_ref")

        role = str(order_ref.get("role", "") or "").strip()
        side = str(order_ref.get("side", "") or "").strip()
        if not role or not side:
            return _mark_conservative(runtime, error="unusable_order_ref")

        fill_price = float(trade.get("price", 0.0) or 0.0)
        fill_qty = abs(float(trade.get("qty", 0.0) or 0.0))
        fill_time_ms = int(trade.get("time", 0) or 0)

        try:
            current_state = str(runtime.get("direction_state", "flat")).strip().lower()
            if (
                str(runtime.get("market_type", "")).strip().lower() == "futures"
                and role == "bootstrap_entry"
                and current_state in {"long_active", "short_active"}
                and ((current_state == "long_active" and side.upper() == "SELL") or (current_state == "short_active" and side.upper() == "BUY"))
            ):
                _apply_conflicting_bootstrap_fill(
                    runtime=runtime,
                    side=side,
                    price=fill_price,
                    qty=fill_qty,
                    fill_time_ms=fill_time_ms,
                    step_price=step_price,
                )
                runtime["recovery_errors"] = ["conflicting_bootstrap_fills"]
                continue

            apply_inventory_grid_fill(
                runtime=runtime,
                role=role,
                side=side,
                price=fill_price,
                qty=fill_qty,
                fill_time_ms=fill_time_ms,
                step_price=step_price,
            )
            replayed_any = True
        except ValueError:
            return _mark_conservative(runtime, error="conflicting_bootstrap_fills")

    runtime["pair_credit_steps"] = 0

    position_lots = list(runtime.get("position_lots") or [])
    recovered_position_qty = _total_position_qty(runtime=runtime)
    expected_position_qty = max(float(current_position_qty), 0.0)
    if runtime.get("recovery_errors") == ["conflicting_bootstrap_fills"]:
        return _mark_conservative(runtime, error="conflicting_bootstrap_fills")

    if expected_position_qty > 0 and not replayed_any and not position_lots:
        return _mark_conservative(runtime, error="missing_strategy_trade_history")

    if abs(recovered_position_qty - expected_position_qty) > POSITION_QTY_EPSILON:
        return _mark_conservative(runtime, error="position_qty_mismatch")

    return runtime
