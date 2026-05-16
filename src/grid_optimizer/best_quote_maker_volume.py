from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from .dry_run import _round_order_price, _round_order_qty


@dataclass(frozen=True)
class BestQuoteMakerVolumeConfig:
    enabled: bool = False
    quote_offset_ticks: int = 0
    defensive_offset_ticks: int = 3
    max_long_notional: float = 1_500.0
    max_short_notional: float = 1_500.0
    inventory_soft_ratio: float = 0.60
    loss_per_10k_soft: float = 0.5
    loss_per_10k_hard: float = 0.8
    soft_loss_budget_scale: float = 0.50
    min_cycle_budget_notional: float = 20.0


@dataclass(frozen=True)
class BestQuoteMakerVolumeInputs:
    bid_price: float
    ask_price: float
    mid_price: float
    current_net_qty: float
    cycle_budget_notional: float
    loss_per_10k_15m: float
    target_volume_remaining: float
    tick_size: float | None = None
    step_size: float | None = None
    min_qty: float | None = None
    min_notional: float | None = None
    open_entry_long_notional: float = 0.0
    open_entry_short_notional: float = 0.0
    pending_entry_buffer_notional: float = 0.0


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _clamp(value: float, lower: float, upper: float) -> float:
    return min(max(value, lower), upper)


def _tick_gap(tick_size: float | None, ticks: int) -> float:
    tick = _safe_float(tick_size)
    if tick <= 0:
        return 0.0
    return tick * max(int(ticks), 0)


def _price_with_gap(price: float, gap: float, sign: int) -> float:
    return float(Decimal(str(price)) + Decimal(sign) * Decimal(str(gap)))


def _build_order(
    *,
    side: str,
    price: float,
    notional: float,
    role: str,
    inputs: BestQuoteMakerVolumeInputs,
    force_reduce_only: bool | None = None,
) -> dict[str, Any] | None:
    rounded_price = _round_order_price(price, inputs.tick_size, side)
    if rounded_price <= 0 or notional <= 0:
        return None
    qty = _round_order_qty(notional / rounded_price, inputs.step_size)
    order_notional = qty * rounded_price
    if qty <= 0:
        return None
    if inputs.min_qty is not None and qty < inputs.min_qty:
        return None
    if inputs.min_notional is not None and order_notional < inputs.min_notional:
        return None
    order: dict[str, Any] = {
        "side": side.upper(),
        "price": rounded_price,
        "qty": qty,
        "notional": order_notional,
        "role": role,
        "position_side": "BOTH",
        "execution_type": "maker",
        "post_only": True,
    }
    if force_reduce_only is not None:
        order["force_reduce_only"] = bool(force_reduce_only)
    return order


def _append_order(bucket: list[dict[str, Any]], order: dict[str, Any] | None) -> None:
    if order is not None:
        bucket.append(order)


def build_best_quote_maker_volume_plan(
    *,
    config: BestQuoteMakerVolumeConfig,
    inputs: BestQuoteMakerVolumeInputs,
) -> dict[str, Any]:
    bid = _safe_float(inputs.bid_price)
    ask = _safe_float(inputs.ask_price)
    mid = _safe_float(inputs.mid_price) or ((bid + ask) / 2.0 if bid > 0 and ask > 0 else 0.0)
    if not config.enabled:
        return {
            "enabled": False,
            "regime": "disabled",
            "buy_orders": [],
            "sell_orders": [],
            "planned_notional": 0.0,
            "reasons": ["disabled"],
        }
    if bid <= 0 or ask <= 0 or bid >= ask or mid <= 0:
        return {
            "enabled": True,
            "regime": "blocked",
            "buy_orders": [],
            "sell_orders": [],
            "planned_notional": 0.0,
            "reasons": ["invalid_market_data"],
        }

    cycle_budget = min(
        max(_safe_float(inputs.cycle_budget_notional), 0.0),
        max(_safe_float(inputs.target_volume_remaining), 0.0),
    )
    if cycle_budget < max(_safe_float(config.min_cycle_budget_notional), 0.0):
        return {
            "enabled": True,
            "regime": "budget_exhausted",
            "buy_orders": [],
            "sell_orders": [],
            "planned_notional": 0.0,
            "reasons": ["budget_below_minimum"],
        }

    net_qty = _safe_float(inputs.current_net_qty)
    long_notional = max(net_qty, 0.0) * mid
    short_notional = max(-net_qty, 0.0) * mid
    open_entry_long_notional = max(_safe_float(inputs.open_entry_long_notional), 0.0)
    open_entry_short_notional = max(_safe_float(inputs.open_entry_short_notional), 0.0)
    pending_entry_buffer_notional = max(_safe_float(inputs.pending_entry_buffer_notional), 0.0)
    long_limit = max(_safe_float(config.max_long_notional), 0.0)
    short_limit = max(_safe_float(config.max_short_notional), 0.0)
    projected_long_entry_notional = long_notional + open_entry_long_notional + pending_entry_buffer_notional
    projected_short_entry_notional = short_notional + open_entry_short_notional + pending_entry_buffer_notional
    soft_ratio = _clamp(_safe_float(config.inventory_soft_ratio), 0.0, 1.0)
    long_soft = long_limit * soft_ratio if long_limit > 0 else 0.0
    short_soft = short_limit * soft_ratio if short_limit > 0 else 0.0
    loss_per_10k = max(_safe_float(inputs.loss_per_10k_15m), 0.0)

    reasons: list[str] = []
    regime = "normal"
    allow_entry_long = long_limit <= 0 or (
        projected_long_entry_notional < long_soft
        and projected_long_entry_notional < long_limit
    )
    allow_entry_short = short_limit <= 0 or (
        projected_short_entry_notional < short_soft
        and projected_short_entry_notional < short_limit
    )
    reduce_long_only = long_notional > 0 and not allow_entry_long
    reduce_short_only = short_notional > 0 and not allow_entry_short
    if reduce_long_only or reduce_short_only:
        regime = "inventory_recover"
        reasons.append("inventory_soft")
    elif not allow_entry_long or not allow_entry_short:
        regime = "inventory_recover"
        reasons.append("open_entry_exposure")

    hard_loss = loss_per_10k >= max(_safe_float(config.loss_per_10k_hard), 0.0) > 0
    soft_loss = loss_per_10k >= max(_safe_float(config.loss_per_10k_soft), 0.0) > 0
    if hard_loss:
        regime = "loss_defensive"
        reasons.append("loss_per_10k_hard")
        allow_entry_long = False
        allow_entry_short = False
    elif soft_loss and regime == "normal":
        regime = "loss_soft"
        reasons.append("loss_per_10k_soft")
        cycle_budget *= _clamp(_safe_float(config.soft_loss_budget_scale), 0.0, 1.0)

    offset_ticks = config.defensive_offset_ticks if regime in {"loss_soft", "inventory_recover"} else config.quote_offset_ticks
    gap = _tick_gap(inputs.tick_size, offset_ticks)
    per_side = cycle_budget / max((1 if allow_entry_long or net_qty < 0 else 0) + (1 if allow_entry_short or net_qty > 0 else 0), 1)

    buy_orders: list[dict[str, Any]] = []
    sell_orders: list[dict[str, Any]] = []
    if allow_entry_long:
        long_entry_notional = per_side
        if long_limit > 0:
            long_entry_notional = min(
                long_entry_notional,
                max(long_limit - projected_long_entry_notional, 0.0),
            )
        _append_order(
            buy_orders,
            _build_order(
                side="BUY",
                price=_price_with_gap(bid, gap, -1),
                notional=long_entry_notional,
                role="best_quote_entry_long",
                inputs=inputs,
            ),
        )
    elif net_qty < 0:
        _append_order(
            buy_orders,
            _build_order(
                side="BUY",
                price=_price_with_gap(bid, gap, -1),
                notional=min(per_side, short_notional),
                role="best_quote_reduce_short",
                inputs=inputs,
                force_reduce_only=True,
            ),
        )
    if net_qty > 0 and not allow_entry_long:
        _append_order(
            sell_orders,
            _build_order(
                side="SELL",
                price=_price_with_gap(ask, gap, 1),
                notional=min(per_side, long_notional),
                role="best_quote_reduce_long",
                inputs=inputs,
                force_reduce_only=True,
            ),
        )
    elif allow_entry_short:
        short_entry_notional = per_side
        if short_limit > 0:
            short_entry_notional = min(
                short_entry_notional,
                max(short_limit - projected_short_entry_notional, 0.0),
            )
        _append_order(
            sell_orders,
            _build_order(
                side="SELL",
                price=_price_with_gap(ask, gap, 1),
                notional=short_entry_notional,
                role="best_quote_entry_short",
                inputs=inputs,
            ),
        )
    elif net_qty > 0:
        _append_order(
            sell_orders,
            _build_order(
                side="SELL",
                price=_price_with_gap(ask, gap, 1),
                notional=min(per_side, long_notional),
                role="best_quote_reduce_long",
                inputs=inputs,
                force_reduce_only=True,
            ),
        )

    planned = sum(order["notional"] for order in [*buy_orders, *sell_orders])
    return {
        "enabled": True,
        "regime": regime,
        "bootstrap_orders": [],
        "buy_orders": buy_orders,
        "sell_orders": sell_orders,
        "aggressive_orders": [],
        "target_base_qty": 0.0,
        "bootstrap_qty": 0.0,
        "planned_notional": planned,
        "reasons": reasons,
        "metrics": {
            "loss_per_10k_15m": loss_per_10k,
            "long_notional": long_notional,
            "short_notional": short_notional,
            "open_entry_long_notional": open_entry_long_notional,
            "open_entry_short_notional": open_entry_short_notional,
            "pending_entry_buffer_notional": pending_entry_buffer_notional,
            "projected_long_entry_notional": projected_long_entry_notional,
            "projected_short_entry_notional": projected_short_entry_notional,
            "cycle_budget_notional": cycle_budget,
        },
    }
