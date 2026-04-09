from __future__ import annotations

from typing import Any

from .inventory_grid_state import apply_inventory_grid_fill, new_inventory_grid_runtime


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

    bootstrap_sides: set[str] = set()
    for trade in ordered_trades:
        order_id = str(int(trade.get("orderId", 0) or 0))
        order_ref = order_refs.get(order_id)
        if not isinstance(order_ref, dict):
            continue

        role = str(order_ref.get("role", "") or "")
        side = str(order_ref.get("side", "") or "")
        if role == "bootstrap_entry":
            bootstrap_sides.add(side.upper())

    if len(bootstrap_sides) > 1:
        runtime["recovery_mode"] = "conservative_reduce_only"
        runtime["recovery_errors"] = ["conflicting_bootstrap_fills"]
        runtime["risk_state"] = "hard_reduce_only"
        return runtime

    for trade in ordered_trades:
        order_id = str(int(trade.get("orderId", 0) or 0))
        order_ref = order_refs.get(order_id)
        if not isinstance(order_ref, dict):
            continue

        try:
            apply_inventory_grid_fill(
                runtime=runtime,
                role=str(order_ref.get("role", "") or ""),
                side=str(order_ref.get("side", "") or ""),
                price=float(trade.get("price", 0.0) or 0.0),
                qty=abs(float(trade.get("qty", 0.0) or 0.0)),
                fill_time_ms=int(trade.get("time", 0) or 0),
                step_price=step_price,
            )
        except ValueError:
            runtime["recovery_mode"] = "conservative_reduce_only"
            runtime["recovery_errors"] = ["conflicting_bootstrap_fills"]
            runtime["risk_state"] = "hard_reduce_only"
            return runtime

    runtime["pair_credit_steps"] = 0

    position_lots = list(runtime.get("position_lots") or [])
    if max(float(current_position_qty), 0.0) > 0 and not position_lots:
        runtime["recovery_mode"] = "conservative_reduce_only"
        runtime["recovery_errors"] = ["missing_strategy_trade_history"]
        runtime["risk_state"] = "hard_reduce_only"
        return runtime

    return runtime
