from __future__ import annotations

from dataclasses import dataclass, replace
from decimal import Decimal
from typing import Any

from .dry_run import _round_order_price, _round_order_qty


@dataclass(frozen=True)
class BestQuoteMakerVolumeConfig:
    enabled: bool = False
    four_leg_cycle_enabled: bool = False
    quote_offset_ticks: int = 0
    defensive_offset_ticks: int = 3
    max_entry_orders_per_side: int = 1
    max_long_notional: float = 1_500.0
    max_short_notional: float = 1_500.0
    inventory_soft_ratio: float = 0.60
    loss_per_10k_soft: float = 0.5
    loss_per_10k_hard: float = 0.8
    soft_loss_budget_scale: float = 0.50
    soft_recovery_min_reduce_notional: float = 0.0
    min_cycle_budget_notional: float = 20.0
    dynamic_tick_enabled: bool = False
    dynamic_tick_tight_offset_ticks: int = 2
    dynamic_tick_low_loss_per_10k: float = 3.0
    dynamic_tick_mid_loss_per_10k: float = 5.0
    dynamic_tick_low_inventory_ratio: float = 0.35
    dynamic_tick_high_inventory_ratio: float = 0.75
    inventory_bias_enabled: bool = False
    inventory_bias_start_ratio: float = 0.25
    inventory_bias_min_ratio_gap: float = 0.05
    inventory_bias_min_notional_gap: float = 10.0
    inventory_bias_min_notional_gap_soft_ratio: float = 0.0
    inventory_bias_reduce_share: float = 0.70
    inventory_bias_opposite_entry_enabled: bool = False
    inventory_bias_same_side_extra_ticks: int = 2
    inventory_bias_reduce_extra_ticks: int = -1
    dynamic_control_enabled: bool = False
    dynamic_control_low_volatility_ratio: float = 0.0015
    dynamic_control_high_volatility_ratio: float = 0.0035
    dynamic_control_extreme_volatility_ratio: float = 0.007
    dynamic_control_low_volatility_budget_scale: float = 1.15
    dynamic_control_low_volatility_budget_max_inventory_ratio: float = 0.75
    dynamic_control_high_volatility_budget_scale: float = 0.75
    dynamic_control_extreme_volatility_budget_scale: float = 0.45
    dynamic_control_low_volatility_extra_offset_ticks: int = -1
    dynamic_control_high_volatility_extra_offset_ticks: int = 3
    dynamic_control_extreme_volatility_extra_offset_ticks: int = 8
    dynamic_control_low_volatility_step_scale: float = 0.75
    dynamic_control_high_volatility_step_scale: float = 1.5
    dynamic_control_extreme_volatility_step_scale: float = 2.5
    dynamic_control_trend_return_ratio: float = 0.002
    dynamic_control_trend_bias_max: float = 0.35
    dynamic_control_trend_bias_requires_inventory_imbalance: bool = False
    dynamic_control_trend_entry_guard_enabled: bool = False
    dynamic_control_trend_entry_guard_min_score: float = 0.75
    dynamic_control_trend_entry_guard_min_volatility_ratio: float = 0.0
    dynamic_control_trend_entry_guard_conflict_ratio: float = 0.25
    dynamic_control_trend_entry_guard_opposite_budget_scale: float = 0.0
    dynamic_control_trend_inventory_guard_enabled: bool = False
    dynamic_control_trend_inventory_guard_start_ratio: float = 0.70
    dynamic_control_trend_inventory_guard_min_score: float = 0.55
    dynamic_control_trend_inventory_guard_min_volatility_ratio: float = 0.0035
    dynamic_control_trend_inventory_guard_entry_budget_scale: float = 0.25
    dynamic_control_trend_inventory_guard_reduce_budget_scale: float = 0.50
    dynamic_control_trend_inventory_guard_reduce_extra_ticks: int = 4
    dynamic_control_trend_loss_reduce_guard_enabled: bool = False
    dynamic_control_trend_loss_reduce_guard_min_score: float = 0.75
    dynamic_control_trend_loss_reduce_guard_min_volatility_ratio: float = 0.0035
    dynamic_control_trend_loss_reduce_guard_reduce_budget_scale: float = 0.35
    dynamic_control_trend_loss_reduce_guard_reduce_extra_ticks: int = 6
    dynamic_control_trend_loss_reduce_guard_recent_loss_min: float = 0.5
    dynamic_control_trend_loss_reduce_guard_recent_loss_budget_scale: float = 0.20
    dynamic_control_trend_loss_reduce_guard_recent_loss_extra_ticks: int = 10
    dynamic_control_trend_loss_reduce_guard_relief_return_ratio: float = 0.0015
    dynamic_control_trend_loss_reduce_guard_relief_budget_scale: float = 0.50
    dynamic_control_trend_loss_reduce_guard_relief_extra_ticks: int = 4
    net_loss_reduce_enabled: bool = False
    net_loss_reduce_min_loss: float = 0.0
    net_loss_reduce_ratio: float = 0.0
    net_loss_reduce_realized_credit_ratio: float = 1.0
    net_loss_reduce_min_inventory_notional: float = 0.0
    same_side_entry_price_guard_enabled: bool = False
    same_side_entry_price_guard_report_only: bool = True
    same_side_entry_price_guard_min_notional: float = 0.0
    same_side_entry_price_guard_gap_ticks: int = 0
    frozen_v2_enabled: bool = False
    frozen_v2_pressure_ratio: float = 0.60
    frozen_v2_danger_ratio: float = 0.85
    frozen_v2_pressure_same_side_entry_scale: float = 0.50
    frozen_v2_danger_same_side_entry_scale: float = 0.20
    frozen_v2_capped_same_side_entry_scale: float = 0.0
    frozen_v2_pressure_recovery_budget_share: float = 0.30
    frozen_v2_danger_recovery_budget_share: float = 0.50
    frozen_v2_capped_recovery_budget_share: float = 0.70


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
    max_order_notional: float | None = None
    open_entry_long_notional: float = 0.0
    open_entry_short_notional: float = 0.0
    pending_entry_buffer_notional: float = 0.0
    entry_ladder_spacing: float | None = None
    current_long_qty: float | None = None
    current_short_qty: float | None = None
    current_long_avg_price: float = 0.0
    current_short_avg_price: float = 0.0
    current_long_lots: list[dict[str, Any]] | None = None
    current_short_lots: list[dict[str, Any]] | None = None
    exchange_long_avg_price: float = 0.0
    exchange_short_avg_price: float = 0.0
    position_side_mode: str = "one_way"
    market_return_1m: float = 0.0
    market_amplitude_1m: float = 0.0
    market_return_5m: float = 0.0
    market_amplitude_5m: float = 0.0
    unrealized_pnl: float = 0.0
    recent_realized_pnl: float = 0.0
    frozen_long_notional: float = 0.0
    frozen_short_notional: float = 0.0
    frozen_total_cap_notional: float = 0.0
    frozen_long_cap_notional: float = 0.0
    frozen_short_cap_notional: float = 0.0


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


def _four_leg_profit_price(
    *,
    position_side: str,
    position_cost: float,
    step_price: float,
    best_bid: float,
    best_ask: float,
    tick_size: float | None,
) -> float:
    tick = max(_safe_float(tick_size), 0.0)
    profit_gap = max(_safe_float(step_price), tick)
    if str(position_side or "").upper() == "LONG":
        return _round_order_price(
            max(best_ask, position_cost + profit_gap),
            tick_size,
            "SELL",
        )
    target_price = position_cost - profit_gap
    if target_price <= 0:
        return 0.0
    return _round_order_price(
        min(best_bid, target_price),
        tick_size,
        "BUY",
    )


def _build_order(
    *,
    side: str,
    price: float,
    notional: float,
    role: str,
    inputs: BestQuoteMakerVolumeInputs,
    position_side: str = "BOTH",
    force_reduce_only: bool | None = None,
    min_effective_notional_cap: float = 0.0,
) -> dict[str, Any] | None:
    rounded_price = _round_order_price(price, inputs.tick_size, side)
    if rounded_price <= 0 or notional <= 0:
        return None
    max_order_notional = max(
        _safe_float(inputs.max_order_notional),
        _safe_float(min_effective_notional_cap),
        0.0,
    )
    effective_notional = min(notional, max_order_notional) if max_order_notional > 0 else notional
    qty = _round_order_qty(effective_notional / rounded_price, inputs.step_size)
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
        "position_side": str(position_side or "BOTH").upper().strip() or "BOTH",
        "execution_type": "maker",
        "post_only": True,
    }
    if force_reduce_only is not None:
        order["force_reduce_only"] = bool(force_reduce_only)
    return order


def _append_order(bucket: list[dict[str, Any]], order: dict[str, Any] | None) -> None:
    if order is not None:
        bucket.append(order)


def _build_entry_ladder(
    *,
    side: str,
    anchor_price: float,
    base_gap: float,
    total_notional: float,
    slots: int,
    role: str,
    inputs: BestQuoteMakerVolumeInputs,
    position_side: str = "BOTH",
) -> list[dict[str, Any]]:
    safe_slots = max(int(slots), 0)
    if safe_slots <= 0 or total_notional <= 0:
        return []
    tick_gap = _tick_gap(inputs.tick_size, 1)
    ladder_gap = max(_safe_float(inputs.entry_ladder_spacing), tick_gap)
    sign = -1 if str(side).upper() == "BUY" else 1
    for candidate_slots in range(safe_slots, 0, -1):
        per_order_notional = total_notional / float(candidate_slots)
        orders: list[dict[str, Any]] = []
        for level in range(candidate_slots):
            gap = max(base_gap, 0.0) + (ladder_gap * level)
            _append_order(
                orders,
                _build_order(
                    side=side,
                    price=_price_with_gap(anchor_price, gap, sign),
                    notional=per_order_notional,
                    role=role,
                    inputs=inputs,
                    position_side=position_side,
                ),
            )
        if orders:
            return orders
    return []


def _prune_clustered_same_side_entry_orders(
    orders: list[dict[str, Any]],
    *,
    side: str,
    min_gap: float,
) -> tuple[list[dict[str, Any]], int]:
    if min_gap <= 0 or len(orders) <= 1:
        return orders, 0
    side = str(side or "").upper()
    protected = [
        order
        for order in orders
        if str(order.get("role", "")).lower().strip() not in {"best_quote_entry_long", "best_quote_entry_short"}
        or bool(order.get("force_reduce_only"))
    ]
    entries = [
        order
        for order in orders
        if str(order.get("role", "")).lower().strip() in {"best_quote_entry_long", "best_quote_entry_short"}
        and not bool(order.get("force_reduce_only"))
    ]
    if not entries:
        return orders, 0

    def _position_leg(order: dict[str, Any]) -> str:
        value = str(order.get("position_side", "BOTH") or "BOTH").upper().strip()
        return value if value in {"LONG", "SHORT"} else "BOTH"

    def _same_cluster_scope(left: str, right: str) -> bool:
        return left == "BOTH" or right == "BOTH" or left == right

    kept_entries: list[dict[str, Any]] = []
    anchor_prices = [
        (_safe_float(order.get("price")), _position_leg(order))
        for order in protected
        if _safe_float(order.get("price")) > 0
    ]
    reverse = side == "SELL"
    for order in sorted(entries, key=lambda item: _safe_float(item.get("price")), reverse=reverse):
        price = _safe_float(order.get("price"))
        if price <= 0:
            continue
        position_leg = _position_leg(order)
        if any(
            _same_cluster_scope(position_leg, existing_leg)
            and abs(price - existing_price) < min_gap - 1e-12
            for existing_price, existing_leg in anchor_prices
        ):
            continue
        kept_entries.append(order)
        anchor_prices.append((price, position_leg))

    kept_entry_ids = {id(order) for order in kept_entries}
    protected_ids = {id(order) for order in protected}
    pruned = [
        order
        for order in orders
        if id(order) in protected_ids or id(order) in kept_entry_ids
    ]
    return pruned, len(entries) - len(kept_entries)


def _build_paired_reduce_orders_for_entries(
    *,
    entries: list[dict[str, Any]],
    inputs: BestQuoteMakerVolumeInputs,
    bid_price: float,
    ask_price: float,
    available_notional: float,
    position_side: str,
) -> list[dict[str, Any]]:
    if not entries or available_notional <= 0:
        return []
    tick_gap = _tick_gap(inputs.tick_size, 1)
    ladder_gap = max(_safe_float(inputs.entry_ladder_spacing), tick_gap)
    remaining = max(_safe_float(available_notional), 0.0)
    orders: list[dict[str, Any]] = []
    for entry in entries:
        if remaining <= 0:
            break
        entry_side = str(entry.get("side", "")).upper().strip()
        entry_price = _safe_float(entry.get("price"))
        entry_notional = min(_safe_float(entry.get("notional")), remaining)
        if entry_side == "SELL" and str(entry.get("position_side", "")).upper().strip() == "SHORT":
            paired_price = min(
                _price_with_gap(entry_price, ladder_gap, -1),
                _price_with_gap(bid_price, tick_gap, -1),
            )
            order = _build_order(
                side="BUY",
                price=paired_price,
                notional=entry_notional,
                role="best_quote_reduce_short",
                inputs=inputs,
                position_side=position_side,
                force_reduce_only=True,
            )
        elif entry_side == "BUY" and str(entry.get("position_side", "")).upper().strip() == "LONG":
            paired_price = max(
                _price_with_gap(entry_price, ladder_gap, 1),
                _price_with_gap(ask_price, tick_gap, 1),
            )
            order = _build_order(
                side="SELL",
                price=paired_price,
                notional=entry_notional,
                role="best_quote_reduce_long",
                inputs=inputs,
                position_side=position_side,
                force_reduce_only=True,
            )
        else:
            order = None
        if order is not None:
            order["paired_entry_reduce"] = True
            orders.append(order)
            remaining -= _safe_float(order.get("notional"))
    return orders


def _frozen_v2_side_state(*, frozen_notional: float, cap_notional: float, pressure_ratio: float, danger_ratio: float) -> str:
    frozen_notional = max(_safe_float(frozen_notional), 0.0)
    cap_notional = max(_safe_float(cap_notional), 0.0)
    if cap_notional <= 0 or frozen_notional <= 0:
        return "normal"
    ratio = frozen_notional / cap_notional
    if ratio >= 1.0:
        return "capped"
    if ratio >= danger_ratio:
        return "danger"
    if ratio >= pressure_ratio:
        return "pressure"
    return "normal"


def _frozen_v2_state_scale(state: str, *, pressure: float, danger: float, capped: float) -> float:
    normalized = str(state or "").strip().lower()
    if normalized == "capped":
        return _clamp(_safe_float(capped), 0.0, 1.0)
    if normalized == "danger":
        return _clamp(_safe_float(danger), 0.0, 1.0)
    if normalized == "pressure":
        return _clamp(_safe_float(pressure), 0.0, 1.0)
    return 1.0


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

    hedge_position_sides = str(inputs.position_side_mode or "").strip().lower() == "hedge"
    net_qty = _safe_float(inputs.current_net_qty)
    if hedge_position_sides:
        long_notional = max(_safe_float(inputs.current_long_qty), 0.0) * mid
        short_notional = max(_safe_float(inputs.current_short_qty), 0.0) * mid
        net_qty = (long_notional - short_notional) / mid if mid > 0 else 0.0
    else:
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
    soft_entry_buffer = max(_safe_float(inputs.min_notional), 0.0)
    long_entry_ceiling = (
        max(long_soft - soft_entry_buffer, 0.0) if long_soft > 0 else long_limit
    )
    short_entry_ceiling = (
        max(short_soft - soft_entry_buffer, 0.0) if short_soft > 0 else short_limit
    )
    long_profit_reduce_trigger = max(long_entry_ceiling - soft_entry_buffer, 0.0)
    short_profit_reduce_trigger = max(short_entry_ceiling - soft_entry_buffer, 0.0)

    def _cap_entry_to_soft_headroom(
        requested_notional: float,
        *,
        ceiling_notional: float,
        projected_notional: float,
    ) -> float:
        requested = max(_safe_float(requested_notional), 0.0)
        if ceiling_notional <= 0:
            return requested
        return min(requested, max(ceiling_notional - projected_notional, 0.0))
    loss_per_10k = max(_safe_float(inputs.loss_per_10k_15m), 0.0)
    long_inventory_ratio = long_notional / long_soft if long_soft > 0 else 0.0
    short_inventory_ratio = short_notional / short_soft if short_soft > 0 else 0.0
    inventory_ratio = max(long_inventory_ratio, short_inventory_ratio)
    frozen_long_notional = max(_safe_float(inputs.frozen_long_notional), 0.0)
    frozen_short_notional = max(_safe_float(inputs.frozen_short_notional), 0.0)
    frozen_total_notional = frozen_long_notional + frozen_short_notional
    frozen_total_cap_notional = max(_safe_float(inputs.frozen_total_cap_notional), 0.0)
    frozen_long_cap_notional = max(
        _safe_float(inputs.frozen_long_cap_notional) or frozen_total_cap_notional,
        0.0,
    )
    frozen_short_cap_notional = max(
        _safe_float(inputs.frozen_short_cap_notional) or frozen_total_cap_notional,
        0.0,
    )
    frozen_v2_pressure_ratio = _clamp(_safe_float(config.frozen_v2_pressure_ratio), 0.0, 1.0)
    frozen_v2_danger_ratio = _clamp(
        max(_safe_float(config.frozen_v2_danger_ratio), frozen_v2_pressure_ratio),
        frozen_v2_pressure_ratio,
        1.0,
    )
    frozen_v2_long_state = _frozen_v2_side_state(
        frozen_notional=frozen_long_notional,
        cap_notional=frozen_long_cap_notional,
        pressure_ratio=frozen_v2_pressure_ratio,
        danger_ratio=frozen_v2_danger_ratio,
    )
    frozen_v2_short_state = _frozen_v2_side_state(
        frozen_notional=frozen_short_notional,
        cap_notional=frozen_short_cap_notional,
        pressure_ratio=frozen_v2_pressure_ratio,
        danger_ratio=frozen_v2_danger_ratio,
    )
    frozen_v2_total_ratio = (
        frozen_total_notional / frozen_total_cap_notional
        if frozen_total_cap_notional > 0
        else 0.0
    )
    frozen_v2_report = {
        "enabled": bool(config.frozen_v2_enabled),
        "applied": False,
        "reason": None,
        "pressure_ratio": frozen_v2_pressure_ratio,
        "danger_ratio": frozen_v2_danger_ratio,
        "frozen_long_notional": frozen_long_notional,
        "frozen_short_notional": frozen_short_notional,
        "frozen_total_notional": frozen_total_notional,
        "frozen_long_cap_notional": frozen_long_cap_notional,
        "frozen_short_cap_notional": frozen_short_cap_notional,
        "frozen_total_cap_notional": frozen_total_cap_notional,
        "frozen_long_ratio": frozen_long_notional / frozen_long_cap_notional if frozen_long_cap_notional > 0 else 0.0,
        "frozen_short_ratio": (
            frozen_short_notional / frozen_short_cap_notional if frozen_short_cap_notional > 0 else 0.0
        ),
        "frozen_total_ratio": frozen_v2_total_ratio,
        "long_state": frozen_v2_long_state,
        "short_state": frozen_v2_short_state,
        "long_entry_budget_scale": 1.0,
        "short_entry_budget_scale": 1.0,
        "long_recovery_budget_share": 0.0,
        "short_recovery_budget_share": 0.0,
    }

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
    entry_long_allowed_before_net_loss = allow_entry_long
    entry_short_allowed_before_net_loss = allow_entry_short
    reduce_long_only = long_notional > 0 and not allow_entry_long
    reduce_short_only = short_notional > 0 and not allow_entry_short
    if reduce_long_only or reduce_short_only:
        regime = "inventory_recover"
        reasons.append("inventory_soft")
    elif not allow_entry_long or not allow_entry_short:
        regime = "inventory_recover"
        reasons.append("open_entry_exposure")

    net_loss_reduce_report = {
        "enabled": bool(config.net_loss_reduce_enabled),
        "active": False,
        "reason": None,
        "unrealized_pnl": _safe_float(inputs.unrealized_pnl),
        "recent_realized_pnl": _safe_float(inputs.recent_realized_pnl),
        "realized_credit_ratio": _safe_float(config.net_loss_reduce_realized_credit_ratio),
        "net_pnl": 0.0,
        "net_loss": 0.0,
        "exposure_notional": long_notional + short_notional,
        "loss_ratio": 0.0,
        "min_loss": max(_safe_float(config.net_loss_reduce_min_loss), 0.0),
        "threshold_ratio": max(_safe_float(config.net_loss_reduce_ratio), 0.0),
        "min_inventory_notional": max(_safe_float(config.net_loss_reduce_min_inventory_notional), 0.0),
        "target_remaining_notional": 0.0,
        "hedge_recovery_action": None,
    }
    realized_credit_ratio = max(_safe_float(config.net_loss_reduce_realized_credit_ratio), 0.0)
    net_pnl = _safe_float(inputs.unrealized_pnl) + (_safe_float(inputs.recent_realized_pnl) * realized_credit_ratio)
    exposure_notional = max(long_notional + short_notional, 0.0)
    net_loss = max(-net_pnl, 0.0)
    net_loss_ratio = net_loss / exposure_notional if exposure_notional > 0 else 0.0
    net_loss_reduce_report.update(
        {
            "realized_credit_ratio": realized_credit_ratio,
            "net_pnl": net_pnl,
            "net_loss": net_loss,
            "exposure_notional": exposure_notional,
            "loss_ratio": net_loss_ratio,
        }
    )
    net_loss_reduce_floor = max(_safe_float(config.net_loss_reduce_min_inventory_notional), 0.0)
    net_loss_reduce_report["target_remaining_notional"] = min(net_loss_reduce_floor, exposure_notional)
    if (
        config.net_loss_reduce_enabled
        and exposure_notional > 0
        and exposure_notional > net_loss_reduce_floor + 1e-12
        and net_loss_reduce_report["min_loss"] > 0
        and net_loss_reduce_report["threshold_ratio"] > 0
        and net_loss >= net_loss_reduce_report["min_loss"]
        and net_loss_ratio >= net_loss_reduce_report["threshold_ratio"]
    ):
        allow_entry_long = False
        allow_entry_short = False
        regime = "inventory_recover" if regime == "normal" else regime
        reasons.append("net_loss_reduce")
        net_loss_reduce_report.update(
            {
                "active": True,
                "reason": (
                    f"net_loss={net_loss:.4f} ratio={net_loss_ratio:.4%} "
                    f">= {net_loss_reduce_report['threshold_ratio']:.4%}"
                ),
            }
        )

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
    base_cycle_budget = cycle_budget
    base_ladder_spacing = max(_safe_float(inputs.entry_ladder_spacing), 0.0)
    dynamic_control_report = {
        "enabled": bool(config.dynamic_control_enabled),
        "applied": False,
        "reason": None,
        "volatility_ratio": 0.0,
        "trend_score": 0.0,
        "budget_scale": 1.0,
        "budget_floor_applied": False,
        "budget_max_inventory_ratio": _safe_float(
            config.dynamic_control_low_volatility_budget_max_inventory_ratio
        ),
        "step_scale": 1.0,
        "extra_offset_ticks": 0,
        "buy_budget_share": None,
        "sell_budget_share": None,
        "base_cycle_budget_notional": base_cycle_budget,
        "effective_cycle_budget_notional": cycle_budget,
        "base_ladder_spacing": base_ladder_spacing,
        "effective_ladder_spacing": base_ladder_spacing,
    }
    trend_entry_guard_report = {
        "enabled": bool(config.dynamic_control_trend_entry_guard_enabled),
        "applied": False,
        "reason": None,
        "blocked_long_entry": False,
        "blocked_short_entry": False,
        "long_entry_budget_scale": 1.0,
        "short_entry_budget_scale": 1.0,
        "min_score": _safe_float(config.dynamic_control_trend_entry_guard_min_score),
        "min_volatility_ratio": _safe_float(config.dynamic_control_trend_entry_guard_min_volatility_ratio),
        "conflict_ratio": _safe_float(config.dynamic_control_trend_entry_guard_conflict_ratio),
        "opposite_budget_scale": _safe_float(config.dynamic_control_trend_entry_guard_opposite_budget_scale),
    }
    trend_inventory_guard_report = {
        "enabled": bool(config.dynamic_control_trend_inventory_guard_enabled),
        "applied": False,
        "reason": None,
        "guard_long_inventory": False,
        "guard_short_inventory": False,
        "long_entry_budget_scale": 1.0,
        "short_entry_budget_scale": 1.0,
        "reduce_long_budget_scale": 1.0,
        "reduce_short_budget_scale": 1.0,
        "reduce_long_extra_ticks": 0,
        "reduce_short_extra_ticks": 0,
        "start_ratio": _safe_float(config.dynamic_control_trend_inventory_guard_start_ratio),
        "min_score": _safe_float(config.dynamic_control_trend_inventory_guard_min_score),
        "min_volatility_ratio": _safe_float(config.dynamic_control_trend_inventory_guard_min_volatility_ratio),
        "entry_budget_scale": _safe_float(config.dynamic_control_trend_inventory_guard_entry_budget_scale),
        "reduce_budget_scale": _safe_float(config.dynamic_control_trend_inventory_guard_reduce_budget_scale),
        "reduce_extra_ticks": int(_safe_float(config.dynamic_control_trend_inventory_guard_reduce_extra_ticks)),
    }
    trend_loss_reduce_guard_report = {
        "enabled": bool(config.dynamic_control_trend_loss_reduce_guard_enabled),
        "applied": False,
        "reason": None,
        "guard_long_reduce": False,
        "guard_short_reduce": False,
        "reduce_long_budget_scale": 1.0,
        "reduce_short_budget_scale": 1.0,
        "reduce_long_extra_ticks": 0,
        "reduce_short_extra_ticks": 0,
        "min_score": _safe_float(config.dynamic_control_trend_loss_reduce_guard_min_score),
        "min_volatility_ratio": _safe_float(config.dynamic_control_trend_loss_reduce_guard_min_volatility_ratio),
        "reduce_budget_scale": _safe_float(config.dynamic_control_trend_loss_reduce_guard_reduce_budget_scale),
        "reduce_extra_ticks": int(_safe_float(config.dynamic_control_trend_loss_reduce_guard_reduce_extra_ticks)),
        "recent_loss_min": max(_safe_float(config.dynamic_control_trend_loss_reduce_guard_recent_loss_min), 0.0),
        "recent_loss_budget_scale": _safe_float(config.dynamic_control_trend_loss_reduce_guard_recent_loss_budget_scale),
        "recent_loss_extra_ticks": int(
            _safe_float(config.dynamic_control_trend_loss_reduce_guard_recent_loss_extra_ticks)
        ),
        "relief_return_ratio": max(_safe_float(config.dynamic_control_trend_loss_reduce_guard_relief_return_ratio), 0.0),
        "relief_budget_scale": _safe_float(config.dynamic_control_trend_loss_reduce_guard_relief_budget_scale),
        "relief_extra_ticks": int(_safe_float(config.dynamic_control_trend_loss_reduce_guard_relief_extra_ticks)),
        "recent_loss_active": False,
        "relief_active": False,
    }
    if config.dynamic_control_enabled and regime != "loss_defensive":
        return_1m = _safe_float(inputs.market_return_1m)
        return_5m = _safe_float(inputs.market_return_5m)
        amplitude_1m = max(_safe_float(inputs.market_amplitude_1m), 0.0)
        amplitude_5m = max(_safe_float(inputs.market_amplitude_5m), 0.0)
        volatility_ratio = max(abs(return_1m), abs(return_5m), amplitude_1m, amplitude_5m)
        low_vol = max(_safe_float(config.dynamic_control_low_volatility_ratio), 0.0)
        high_vol = max(_safe_float(config.dynamic_control_high_volatility_ratio), low_vol)
        extreme_vol = max(_safe_float(config.dynamic_control_extreme_volatility_ratio), high_vol)
        budget_scale = 1.0
        step_scale = 1.0
        extra_offset_ticks = 0
        reason = "normal"
        if extreme_vol > 0 and volatility_ratio >= extreme_vol:
            budget_scale = _clamp(_safe_float(config.dynamic_control_extreme_volatility_budget_scale), 0.0, 1.0)
            step_scale = max(_safe_float(config.dynamic_control_extreme_volatility_step_scale), 1.0)
            extra_offset_ticks = max(int(config.dynamic_control_extreme_volatility_extra_offset_ticks), 0)
            reason = "extreme_volatility_defensive"
        elif high_vol > 0 and volatility_ratio >= high_vol:
            budget_scale = _clamp(_safe_float(config.dynamic_control_high_volatility_budget_scale), 0.0, 1.0)
            step_scale = max(_safe_float(config.dynamic_control_high_volatility_step_scale), 1.0)
            extra_offset_ticks = max(int(config.dynamic_control_high_volatility_extra_offset_ticks), 0)
            reason = "high_volatility_defensive"
        elif low_vol > 0 and 0 < volatility_ratio <= low_vol:
            budget_max_inventory_ratio = max(
                _safe_float(config.dynamic_control_low_volatility_budget_max_inventory_ratio),
                0.0,
            )
            if inventory_ratio < budget_max_inventory_ratio and not soft_loss:
                budget_scale = max(_safe_float(config.dynamic_control_low_volatility_budget_scale), 1.0)
            step_scale = _clamp(_safe_float(config.dynamic_control_low_volatility_step_scale), 0.1, 1.0)
            extra_offset_ticks = min(int(config.dynamic_control_low_volatility_extra_offset_ticks), 0)
            reason = "low_volatility_expand" if budget_scale > 1.0 else "low_volatility_tighten"
        trend_threshold = max(_safe_float(config.dynamic_control_trend_return_ratio), 1e-12)
        trend_score = _clamp(((return_1m * 0.65) + (return_5m * 0.35)) / trend_threshold, -1.0, 1.0)
        cycle_budget *= budget_scale
        # Dynamic volatility scaling is intended to reduce throughput, not
        # make every maker order fall below the configured minimum. Preserve
        # one viable cycle so a defensive GRVT tier cannot become a prolonged
        # zero-entry state while the explicit volatility pause is inactive.
        dynamic_cycle_budget_floor = min(
            max(_safe_float(config.min_cycle_budget_notional), 0.0),
            base_cycle_budget,
        )
        budget_floor_applied = cycle_budget + 1e-12 < dynamic_cycle_budget_floor
        if budget_floor_applied:
            cycle_budget = dynamic_cycle_budget_floor
        if extra_offset_ticks != 0:
            offset_ticks = max(int(offset_ticks) + extra_offset_ticks, 0)
        effective_ladder_spacing = base_ladder_spacing * step_scale if base_ladder_spacing > 0 else base_ladder_spacing
        if effective_ladder_spacing > 0 and abs(effective_ladder_spacing - base_ladder_spacing) > 1e-12:
            inputs = replace(inputs, entry_ladder_spacing=effective_ladder_spacing)
        dynamic_control_report.update(
            {
                "applied": True,
                "reason": reason,
                "volatility_ratio": volatility_ratio,
                "trend_score": trend_score,
                "budget_scale": budget_scale,
                "budget_floor_applied": budget_floor_applied,
                "step_scale": step_scale,
                "extra_offset_ticks": extra_offset_ticks,
                "effective_cycle_budget_notional": cycle_budget,
                "effective_ladder_spacing": effective_ladder_spacing,
                "market_return_1m": return_1m,
                "market_amplitude_1m": amplitude_1m,
                "market_return_5m": return_5m,
                "market_amplitude_5m": amplitude_5m,
            }
        )
        if reason != "normal":
            reasons.append(reason)

    if config.dynamic_control_enabled and config.dynamic_control_trend_entry_guard_enabled and not hard_loss:
        trend_threshold = max(_safe_float(config.dynamic_control_trend_return_ratio), 1e-12)
        min_score = _clamp(_safe_float(config.dynamic_control_trend_entry_guard_min_score), 0.0, 1.0)
        min_volatility = max(_safe_float(config.dynamic_control_trend_entry_guard_min_volatility_ratio), 0.0)
        conflict_ratio = max(_safe_float(config.dynamic_control_trend_entry_guard_conflict_ratio), 0.0)
        opposite_scale = _clamp(_safe_float(config.dynamic_control_trend_entry_guard_opposite_budget_scale), 0.0, 1.0)
        trend_score = _clamp(_safe_float(dynamic_control_report.get("trend_score")), -1.0, 1.0)
        volatility_ratio = max(_safe_float(dynamic_control_report.get("volatility_ratio")), 0.0)
        return_1m = _safe_float(dynamic_control_report.get("market_return_1m"))
        return_5m = _safe_float(dynamic_control_report.get("market_return_5m"))
        volatility_ok = min_volatility <= 0 or volatility_ratio >= min_volatility
        strong_up = (
            volatility_ok
            and trend_score >= min_score
            and return_5m >= trend_threshold
            and return_1m >= -trend_threshold * conflict_ratio
        )
        strong_down = (
            volatility_ok
            and trend_score <= -min_score
            and return_5m <= -trend_threshold
            and return_1m <= trend_threshold * conflict_ratio
        )
        conflict_rebound = (
            volatility_ok
            and return_5m <= -trend_threshold
            and return_1m >= trend_threshold * conflict_ratio
        )
        conflict_pullback = (
            volatility_ok
            and return_5m >= trend_threshold
            and return_1m <= -trend_threshold * conflict_ratio
        )
        guard_reason = None
        if strong_up:
            guard_reason = "strong_uptrend_blocks_short_entry"
            trend_entry_guard_report["blocked_short_entry"] = True
            trend_entry_guard_report["short_entry_budget_scale"] = opposite_scale
        elif strong_down:
            guard_reason = "strong_downtrend_blocks_long_entry"
            trend_entry_guard_report["blocked_long_entry"] = True
            trend_entry_guard_report["long_entry_budget_scale"] = opposite_scale
        elif conflict_rebound:
            guard_reason = "conflicting_rebound_blocks_short_entry"
            trend_entry_guard_report["blocked_short_entry"] = True
            trend_entry_guard_report["short_entry_budget_scale"] = opposite_scale
        elif conflict_pullback:
            guard_reason = "conflicting_pullback_blocks_long_entry"
            trend_entry_guard_report["blocked_long_entry"] = True
            trend_entry_guard_report["long_entry_budget_scale"] = opposite_scale
        if guard_reason:
            trend_entry_guard_report.update(
                {
                    "applied": True,
                    "reason": guard_reason,
                    "min_score": min_score,
                    "min_volatility_ratio": min_volatility,
                    "conflict_ratio": conflict_ratio,
                    "opposite_budget_scale": opposite_scale,
                }
            )
            reasons.append("trend_entry_guard")
            if trend_entry_guard_report["blocked_long_entry"] and opposite_scale <= 0:
                allow_entry_long = False
            if trend_entry_guard_report["blocked_short_entry"] and opposite_scale <= 0:
                allow_entry_short = False

    if config.dynamic_control_enabled and config.dynamic_control_trend_inventory_guard_enabled and not hard_loss:
        min_score = _clamp(_safe_float(config.dynamic_control_trend_inventory_guard_min_score), 0.0, 1.0)
        min_volatility = max(_safe_float(config.dynamic_control_trend_inventory_guard_min_volatility_ratio), 0.0)
        start_ratio = max(_safe_float(config.dynamic_control_trend_inventory_guard_start_ratio), 0.0)
        entry_scale = _clamp(_safe_float(config.dynamic_control_trend_inventory_guard_entry_budget_scale), 0.0, 1.0)
        reduce_scale = _clamp(_safe_float(config.dynamic_control_trend_inventory_guard_reduce_budget_scale), 0.0, 1.0)
        reduce_extra_ticks = max(int(_safe_float(config.dynamic_control_trend_inventory_guard_reduce_extra_ticks)), 0)
        trend_score = _clamp(_safe_float(dynamic_control_report.get("trend_score")), -1.0, 1.0)
        volatility_ratio = max(_safe_float(dynamic_control_report.get("volatility_ratio")), 0.0)
        volatility_ok = min_volatility <= 0 or volatility_ratio >= min_volatility
        imbalance_floor = max(
            _safe_float(config.inventory_bias_min_notional_gap),
            max(long_soft, short_soft)
            * max(_safe_float(config.inventory_bias_min_notional_gap_soft_ratio), 0.0),
        )
        require_imbalance = bool(
            config.dynamic_control_trend_bias_requires_inventory_imbalance
        )
        long_guard = (
            volatility_ok
            and long_inventory_ratio >= start_ratio
            and (
                not require_imbalance
                or long_notional - short_notional >= imbalance_floor
            )
            and trend_score <= -min_score
        )
        short_guard = (
            volatility_ok
            and short_inventory_ratio >= start_ratio
            and (
                not require_imbalance
                or short_notional - long_notional >= imbalance_floor
            )
            and trend_score >= min_score
        )
        if long_guard or short_guard:
            if long_guard:
                trend_inventory_guard_report.update(
                    {
                        "guard_long_inventory": True,
                        "long_entry_budget_scale": entry_scale,
                        "reduce_long_budget_scale": reduce_scale,
                        "reduce_long_extra_ticks": reduce_extra_ticks,
                    }
                )
            if short_guard:
                trend_inventory_guard_report.update(
                    {
                        "guard_short_inventory": True,
                        "short_entry_budget_scale": entry_scale,
                        "reduce_short_budget_scale": reduce_scale,
                        "reduce_short_extra_ticks": reduce_extra_ticks,
                    }
                )
            trend_inventory_guard_report.update(
                {
                    "applied": True,
                    "reason": "adverse_trend_inventory_guard",
                    "start_ratio": start_ratio,
                    "min_score": min_score,
                    "min_volatility_ratio": min_volatility,
                    "entry_budget_scale": entry_scale,
                    "reduce_budget_scale": reduce_scale,
                    "reduce_extra_ticks": reduce_extra_ticks,
                    "trend_score": trend_score,
                    "volatility_ratio": volatility_ratio,
                    "imbalance_floor_notional": imbalance_floor,
                    "requires_imbalance": require_imbalance,
                }
            )
            reasons.append("trend_inventory_guard")

    if config.dynamic_control_enabled and config.dynamic_control_trend_loss_reduce_guard_enabled and not hard_loss:
        min_score = _clamp(_safe_float(config.dynamic_control_trend_loss_reduce_guard_min_score), 0.0, 1.0)
        min_volatility = max(_safe_float(config.dynamic_control_trend_loss_reduce_guard_min_volatility_ratio), 0.0)
        reduce_scale = _clamp(_safe_float(config.dynamic_control_trend_loss_reduce_guard_reduce_budget_scale), 0.0, 1.0)
        reduce_extra_ticks = max(int(_safe_float(config.dynamic_control_trend_loss_reduce_guard_reduce_extra_ticks)), 0)
        recent_loss_min = max(_safe_float(config.dynamic_control_trend_loss_reduce_guard_recent_loss_min), 0.0)
        recent_loss_scale = _clamp(
            _safe_float(config.dynamic_control_trend_loss_reduce_guard_recent_loss_budget_scale), 0.0, 1.0
        )
        recent_loss_extra_ticks = max(
            int(_safe_float(config.dynamic_control_trend_loss_reduce_guard_recent_loss_extra_ticks)), 0
        )
        relief_return_ratio = max(_safe_float(config.dynamic_control_trend_loss_reduce_guard_relief_return_ratio), 0.0)
        relief_scale = _clamp(
            _safe_float(config.dynamic_control_trend_loss_reduce_guard_relief_budget_scale), 0.0, 1.0
        )
        relief_extra_ticks = max(int(_safe_float(config.dynamic_control_trend_loss_reduce_guard_relief_extra_ticks)), 0)
        trend_score = _clamp(_safe_float(dynamic_control_report.get("trend_score")), -1.0, 1.0)
        volatility_ratio = max(_safe_float(dynamic_control_report.get("volatility_ratio")), 0.0)
        return_1m = _safe_float(dynamic_control_report.get("market_return_1m"))
        volatility_ok = min_volatility <= 0 or volatility_ratio >= min_volatility
        guard_short_reduce = volatility_ok and trend_score >= min_score and short_notional > 0
        guard_long_reduce = volatility_ok and trend_score <= -min_score and long_notional > 0
        if guard_long_reduce or guard_short_reduce:
            recent_loss_active = recent_loss_min > 0 and _safe_float(inputs.recent_realized_pnl) <= -recent_loss_min
            short_relief_active = guard_short_reduce and relief_return_ratio > 0 and return_1m <= -relief_return_ratio
            long_relief_active = guard_long_reduce and relief_return_ratio > 0 and return_1m >= relief_return_ratio
            relief_active = bool(short_relief_active or long_relief_active)
            effective_reduce_scale = reduce_scale
            effective_reduce_extra_ticks = reduce_extra_ticks
            guard_reason = "adverse_trend_reduce_chase_guard"
            if recent_loss_active:
                effective_reduce_scale = min(effective_reduce_scale, recent_loss_scale)
                effective_reduce_extra_ticks = max(effective_reduce_extra_ticks, recent_loss_extra_ticks)
                guard_reason = "recent_loss_trend_reduce_cooldown"
            if relief_active:
                effective_reduce_scale = max(effective_reduce_scale, min(relief_scale, 1.0))
                effective_reduce_extra_ticks = min(effective_reduce_extra_ticks, relief_extra_ticks)
                guard_reason = (
                    "pullback_relief_allows_short_reduce"
                    if short_relief_active
                    else "rebound_relief_allows_long_reduce"
                )
            if guard_long_reduce:
                trend_loss_reduce_guard_report.update(
                    {
                        "guard_long_reduce": True,
                        "reduce_long_budget_scale": effective_reduce_scale,
                        "reduce_long_extra_ticks": effective_reduce_extra_ticks,
                    }
                )
            if guard_short_reduce:
                trend_loss_reduce_guard_report.update(
                    {
                        "guard_short_reduce": True,
                        "reduce_short_budget_scale": effective_reduce_scale,
                        "reduce_short_extra_ticks": effective_reduce_extra_ticks,
                    }
                )
            trend_loss_reduce_guard_report.update(
                {
                    "applied": True,
                    "reason": guard_reason,
                    "min_score": min_score,
                    "min_volatility_ratio": min_volatility,
                    "reduce_budget_scale": reduce_scale,
                    "reduce_extra_ticks": reduce_extra_ticks,
                    "effective_reduce_budget_scale": effective_reduce_scale,
                    "effective_reduce_extra_ticks": effective_reduce_extra_ticks,
                    "recent_loss_min": recent_loss_min,
                    "recent_loss_budget_scale": recent_loss_scale,
                    "recent_loss_extra_ticks": recent_loss_extra_ticks,
                    "recent_loss_active": recent_loss_active,
                    "relief_return_ratio": relief_return_ratio,
                    "relief_budget_scale": relief_scale,
                    "relief_extra_ticks": relief_extra_ticks,
                    "relief_active": relief_active,
                    "return_1m": return_1m,
                    "trend_score": trend_score,
                    "volatility_ratio": volatility_ratio,
                }
            )
            reasons.append("trend_loss_reduce_guard")

    dynamic_tick_report = {
        "enabled": bool(config.dynamic_tick_enabled),
        "base_offset_ticks": int(offset_ticks),
        "offset_ticks": int(offset_ticks),
        "reason": None,
    }
    if config.dynamic_tick_enabled and regime == "normal":
        tight_ticks = max(int(_safe_float(config.dynamic_tick_tight_offset_ticks)), 0)
        low_loss = max(_safe_float(config.dynamic_tick_low_loss_per_10k), 0.0)
        mid_loss = max(_safe_float(config.dynamic_tick_mid_loss_per_10k), low_loss)
        low_inventory = _clamp(_safe_float(config.dynamic_tick_low_inventory_ratio), 0.0, 1.0)
        high_inventory = _clamp(_safe_float(config.dynamic_tick_high_inventory_ratio), low_inventory, 10.0)
        if loss_per_10k <= low_loss and inventory_ratio <= low_inventory:
            new_ticks = min(max(int(offset_ticks), 0), tight_ticks)
            if new_ticks != offset_ticks:
                dynamic_tick_report["reason"] = "low_loss_low_inventory_tighten"
            offset_ticks = new_ticks
        elif loss_per_10k >= mid_loss or inventory_ratio >= high_inventory:
            new_ticks = max(int(offset_ticks), int(config.defensive_offset_ticks))
            if new_ticks != offset_ticks:
                dynamic_tick_report["reason"] = "loss_or_inventory_widen"
            offset_ticks = new_ticks
    dynamic_tick_report["offset_ticks"] = int(offset_ticks)
    gap = _tick_gap(inputs.tick_size, offset_ticks)
    reduce_long_extra_ticks = max(
        int(_safe_float(trend_inventory_guard_report["reduce_long_extra_ticks"])),
        int(_safe_float(trend_loss_reduce_guard_report["reduce_long_extra_ticks"])),
        0,
    )
    reduce_short_extra_ticks = max(
        int(_safe_float(trend_inventory_guard_report["reduce_short_extra_ticks"])),
        int(_safe_float(trend_loss_reduce_guard_report["reduce_short_extra_ticks"])),
        0,
    )
    reduce_long_gap = _tick_gap(inputs.tick_size, max(int(offset_ticks) + reduce_long_extra_ticks, 0))
    reduce_short_gap = _tick_gap(inputs.tick_size, max(int(offset_ticks) + reduce_short_extra_ticks, 0))
    buy_slot_active = allow_entry_long or (short_notional > 0 and not allow_entry_short)
    sell_slot_active = allow_entry_short or (long_notional > 0 and not allow_entry_long)
    active_side_count = max((1 if buy_slot_active else 0) + (1 if sell_slot_active else 0), 1)
    buy_side_notional = cycle_budget / active_side_count
    sell_side_notional = cycle_budget / active_side_count
    if config.dynamic_control_enabled and buy_slot_active and sell_slot_active:
        max_bias = _clamp(_safe_float(config.dynamic_control_trend_bias_max), 0.0, 0.45)
        trend_score = _clamp(_safe_float(dynamic_control_report.get("trend_score")), -1.0, 1.0)
        bias_imbalance_floor = max(
            _safe_float(config.inventory_bias_min_notional_gap),
            max(long_soft, short_soft)
            * max(_safe_float(config.inventory_bias_min_notional_gap_soft_ratio), 0.0),
        )
        material_inventory_imbalance = (
            abs(short_notional - long_notional) >= bias_imbalance_floor
        )
        require_imbalance = bool(
            config.dynamic_control_trend_bias_requires_inventory_imbalance
        )
        effective_trend_score = (
            trend_score
            if material_inventory_imbalance or not require_imbalance
            else 0.0
        )
        buy_share = _clamp(0.5 + (effective_trend_score * max_bias), 0.05, 0.95)
        sell_share = 1.0 - buy_share
        buy_side_notional = cycle_budget * buy_share
        sell_side_notional = cycle_budget * sell_share
        dynamic_control_report["buy_budget_share"] = buy_share
        dynamic_control_report["sell_budget_share"] = sell_share
        dynamic_control_report["trend_budget_bias_applied"] = (
            material_inventory_imbalance or not require_imbalance
        )
        dynamic_control_report["trend_budget_bias_imbalance_floor_notional"] = bias_imbalance_floor
        dynamic_control_report["trend_budget_bias_requires_inventory_imbalance"] = require_imbalance
    long_entry_budget_scale = _clamp(_safe_float(trend_entry_guard_report["long_entry_budget_scale"]), 0.0, 1.0)
    short_entry_budget_scale = _clamp(_safe_float(trend_entry_guard_report["short_entry_budget_scale"]), 0.0, 1.0)
    long_entry_budget_scale *= _clamp(
        _safe_float(trend_inventory_guard_report["long_entry_budget_scale"]), 0.0, 1.0
    )
    short_entry_budget_scale *= _clamp(
        _safe_float(trend_inventory_guard_report["short_entry_budget_scale"]), 0.0, 1.0
    )
    reduce_long_budget_scale = _clamp(
        _safe_float(trend_inventory_guard_report["reduce_long_budget_scale"]), 0.0, 1.0
    )
    reduce_short_budget_scale = _clamp(
        _safe_float(trend_inventory_guard_report["reduce_short_budget_scale"]), 0.0, 1.0
    )
    reduce_long_budget_scale = min(
        reduce_long_budget_scale,
        _clamp(_safe_float(trend_loss_reduce_guard_report["reduce_long_budget_scale"]), 0.0, 1.0),
    )
    reduce_short_budget_scale = min(
        reduce_short_budget_scale,
        _clamp(_safe_float(trend_loss_reduce_guard_report["reduce_short_budget_scale"]), 0.0, 1.0),
    )
    if config.frozen_v2_enabled and not hard_loss:
        long_same_side_scale = _frozen_v2_state_scale(
            frozen_v2_long_state,
            pressure=config.frozen_v2_pressure_same_side_entry_scale,
            danger=config.frozen_v2_danger_same_side_entry_scale,
            capped=config.frozen_v2_capped_same_side_entry_scale,
        )
        short_same_side_scale = _frozen_v2_state_scale(
            frozen_v2_short_state,
            pressure=config.frozen_v2_pressure_same_side_entry_scale,
            danger=config.frozen_v2_danger_same_side_entry_scale,
            capped=config.frozen_v2_capped_same_side_entry_scale,
        )
        long_recovery_share = 0.0
        short_recovery_share = 0.0
        if frozen_v2_short_state == "capped":
            long_recovery_share = _clamp(_safe_float(config.frozen_v2_capped_recovery_budget_share), 0.0, 1.0)
        elif frozen_v2_short_state == "danger":
            long_recovery_share = _clamp(_safe_float(config.frozen_v2_danger_recovery_budget_share), 0.0, 1.0)
        elif frozen_v2_short_state == "pressure":
            long_recovery_share = _clamp(_safe_float(config.frozen_v2_pressure_recovery_budget_share), 0.0, 1.0)
        if frozen_v2_long_state == "capped":
            short_recovery_share = _clamp(_safe_float(config.frozen_v2_capped_recovery_budget_share), 0.0, 1.0)
        elif frozen_v2_long_state == "danger":
            short_recovery_share = _clamp(_safe_float(config.frozen_v2_danger_recovery_budget_share), 0.0, 1.0)
        elif frozen_v2_long_state == "pressure":
            short_recovery_share = _clamp(_safe_float(config.frozen_v2_pressure_recovery_budget_share), 0.0, 1.0)
        if long_same_side_scale < 1.0 or short_same_side_scale < 1.0 or long_recovery_share > 0 or short_recovery_share > 0:
            reasons.append("frozen_v2_pressure")
            if regime == "normal":
                regime = "frozen_v2_recovery"
            long_entry_budget_scale *= long_same_side_scale
            short_entry_budget_scale *= short_same_side_scale
            if frozen_v2_short_state == "capped" and short_same_side_scale <= 0:
                allow_entry_short = False
            if frozen_v2_long_state == "capped" and long_same_side_scale <= 0:
                allow_entry_long = False
            if allow_entry_long and long_recovery_share > 0:
                buy_side_notional += cycle_budget * long_recovery_share
            if allow_entry_short and short_recovery_share > 0:
                sell_side_notional += cycle_budget * short_recovery_share
            frozen_v2_report.update(
                {
                    "applied": True,
                    "reason": "directional_frozen_pressure",
                    "long_entry_budget_scale": long_same_side_scale,
                    "short_entry_budget_scale": short_same_side_scale,
                    "long_recovery_budget_share": long_recovery_share,
                    "short_recovery_budget_share": short_recovery_share,
                    "effective_buy_side_notional": buy_side_notional,
                    "effective_sell_side_notional": sell_side_notional,
                }
            )

    def _net_loss_reduce_cap(side_notional: float) -> float:
        side_notional = max(_safe_float(side_notional), 0.0)
        if not net_loss_reduce_report.get("active"):
            return side_notional
        floor = max(_safe_float(net_loss_reduce_report.get("min_inventory_notional")), 0.0)
        if floor <= 0:
            return side_notional
        other_notional = max(exposure_notional - side_notional, 0.0)
        return max(exposure_notional - floor - other_notional, 0.0)

    soft_recovery_min_reduce_notional = max(
        _safe_float(config.soft_recovery_min_reduce_notional),
        0.0,
    )

    def _soft_recovery_floor(side_notional: float, soft_notional: float) -> float:
        if soft_notional <= 0 or side_notional <= soft_notional + 1e-12:
            return 0.0
        return soft_recovery_min_reduce_notional

    def _recovery_reduce_notional(
        planned_notional: float,
        side_notional: float,
        soft_notional: float,
    ) -> float:
        requested = max(
            _safe_float(planned_notional),
            _soft_recovery_floor(side_notional, soft_notional),
        )
        return min(
            requested,
            max(_safe_float(side_notional), 0.0),
            _net_loss_reduce_cap(side_notional),
        )

    buy_orders: list[dict[str, Any]] = []
    sell_orders: list[dict[str, Any]] = []
    max_entry_orders_per_side = max(int(_safe_float(config.max_entry_orders_per_side)), 1)
    long_entry_position_side = "LONG" if hedge_position_sides else "BOTH"
    short_entry_position_side = "SHORT" if hedge_position_sides else "BOTH"
    reduce_long_position_side = "LONG" if hedge_position_sides else "BOTH"
    reduce_short_position_side = "SHORT" if hedge_position_sides else "BOTH"
    loss_blocked_reduce_fallback_report = {
        "long_reduce": False,
        "short_reduce": False,
        "long_entry": False,
        "short_entry": False,
        "reduce_long_price": None,
        "reduce_short_price": None,
        "long_avg_price": _safe_float(inputs.current_long_avg_price) or None,
        "short_avg_price": _safe_float(inputs.current_short_avg_price) or None,
    }
    inventory_bias_report = {
        "enabled": bool(config.inventory_bias_enabled),
        "applied": False,
        "side": None,
        "inventory_ratio": inventory_ratio,
        "reduce_share": None,
        "same_side_entry_share": None,
        "opposite_entry_enabled": bool(config.inventory_bias_opposite_entry_enabled),
        "reduce_offset_ticks": None,
        "same_side_offset_ticks": None,
        "ratio_gap": abs(short_inventory_ratio - long_inventory_ratio),
        "min_ratio_gap": None,
        "notional_gap": abs(short_notional - long_notional),
        "min_notional_gap": None,
        "min_notional_gap_soft_ratio": None,
        "recover_applied": False,
        "recover_opposite_entry_side": None,
    }
    bias_start = _clamp(_safe_float(config.inventory_bias_start_ratio), 0.0, 1.0)
    bias_min_ratio_gap = max(_safe_float(config.inventory_bias_min_ratio_gap), 0.0)
    bias_min_notional_gap = max(_safe_float(config.inventory_bias_min_notional_gap), 0.0)
    bias_min_notional_gap_soft_ratio = max(_safe_float(config.inventory_bias_min_notional_gap_soft_ratio), 0.0)
    if bias_min_notional_gap_soft_ratio > 0:
        bias_min_notional_gap = max(long_soft, short_soft) * bias_min_notional_gap_soft_ratio
    inventory_bias_report["min_ratio_gap"] = bias_min_ratio_gap
    inventory_bias_report["min_notional_gap"] = bias_min_notional_gap
    inventory_bias_report["min_notional_gap_soft_ratio"] = bias_min_notional_gap_soft_ratio
    bias_reduce_share = _clamp(_safe_float(config.inventory_bias_reduce_share), 0.0, 1.0)
    bias_entry_share = max(1.0 - bias_reduce_share, 0.0)
    short_notional_gap = short_notional - long_notional
    long_notional_gap = long_notional - short_notional
    if config.inventory_bias_enabled and regime == "inventory_recover" and not hard_loss:
        recover_reduce_notional = cycle_budget * bias_reduce_share
        recover_other_notional = cycle_budget * bias_entry_share
        if (
            (reduce_long_only or reduce_short_only or not allow_entry_long or not allow_entry_short)
            and long_notional_gap >= bias_min_notional_gap
            and long_soft > 0
            and long_notional >= long_soft * bias_start
        ):
            sell_side_notional = recover_reduce_notional
            buy_side_notional = recover_other_notional
            inventory_bias_report.update(
                {
                    "recover_applied": True,
                    "side": "long",
                    "reduce_share": bias_reduce_share,
                    "same_side_entry_share": bias_entry_share,
                }
            )
        elif (
            (reduce_short_only or reduce_long_only or not allow_entry_short or not allow_entry_long)
            and short_notional_gap >= bias_min_notional_gap
            and short_soft > 0
            and short_notional >= short_soft * bias_start
        ):
            buy_side_notional = recover_reduce_notional
            sell_side_notional = recover_other_notional
            inventory_bias_report.update(
                {
                    "recover_applied": True,
                    "side": "short",
                    "reduce_share": bias_reduce_share,
                    "same_side_entry_share": bias_entry_share,
                }
            )
    can_bias_short = (
        config.inventory_bias_enabled
        and regime == "normal"
        and short_notional > 0
        and short_notional_gap >= bias_min_notional_gap
        and allow_entry_long
        and allow_entry_short
        and short_soft > 0
        and short_notional >= short_soft * bias_start
    )
    can_bias_long = (
        config.inventory_bias_enabled
        and regime == "normal"
        and long_notional > 0
        and long_notional_gap >= bias_min_notional_gap
        and allow_entry_long
        and allow_entry_short
        and long_soft > 0
        and long_notional >= long_soft * bias_start
    )
    if can_bias_short and can_bias_long:
        inventory_ratio_gap = abs(short_inventory_ratio - long_inventory_ratio)
        if inventory_ratio_gap < bias_min_ratio_gap:
            can_bias_short = False
            can_bias_long = False
        else:
            can_bias_short = short_inventory_ratio > long_inventory_ratio
            can_bias_long = not can_bias_short
    if can_bias_short or can_bias_long:
        same_side_ticks = max(int(offset_ticks) + max(int(config.inventory_bias_same_side_extra_ticks), 0), 0)
        same_side_gap = _tick_gap(inputs.tick_size, same_side_ticks)
        reduce_notional = cycle_budget * bias_reduce_share
        same_side_notional = cycle_budget * bias_entry_share
        regime = "inventory_bias"
        reasons.append("inventory_bias")
        inventory_bias_report.update(
            {
                "applied": True,
                "side": "short" if can_bias_short else "long",
                "reduce_share": bias_reduce_share,
                "same_side_entry_share": bias_entry_share,
                "reduce_offset_ticks": None,
                "same_side_offset_ticks": same_side_ticks,
                "min_ratio_gap": bias_min_ratio_gap,
                "min_notional_gap": bias_min_notional_gap,
                "min_notional_gap_soft_ratio": bias_min_notional_gap_soft_ratio,
            }
        )
        if can_bias_short:
            short_reduce_ticks = max(
                int(offset_ticks) + int(config.inventory_bias_reduce_extra_ticks) + reduce_short_extra_ticks,
                0,
            )
            short_reduce_gap = _tick_gap(inputs.tick_size, short_reduce_ticks)
            inventory_bias_report["reduce_offset_ticks"] = short_reduce_ticks
            _append_order(
                buy_orders,
                _build_order(
                    side="BUY",
                    price=_price_with_gap(bid, short_reduce_gap, -1),
                    notional=_recovery_reduce_notional(
                        reduce_notional * reduce_short_budget_scale,
                        short_notional,
                        short_soft,
                    ),
                    role="best_quote_reduce_short",
                    inputs=inputs,
                    position_side=reduce_short_position_side,
                    force_reduce_only=True,
                    min_effective_notional_cap=_soft_recovery_floor(short_notional, short_soft),
                ),
            )
            if config.inventory_bias_opposite_entry_enabled:
                buy_orders.extend(
                    _build_entry_ladder(
                        side="BUY",
                        anchor_price=bid,
                        base_gap=same_side_gap,
                        total_notional=_cap_entry_to_soft_headroom(
                            same_side_notional * long_entry_budget_scale,
                            ceiling_notional=long_entry_ceiling,
                            projected_notional=projected_long_entry_notional,
                        ),
                        slots=max_entry_orders_per_side,
                        role="best_quote_entry_long",
                        inputs=inputs,
                        position_side=long_entry_position_side,
                    )
                )
            else:
                sell_orders.extend(
                    _build_entry_ladder(
                        side="SELL",
                        anchor_price=ask,
                        base_gap=same_side_gap,
                        total_notional=_cap_entry_to_soft_headroom(
                            same_side_notional * short_entry_budget_scale,
                            ceiling_notional=short_entry_ceiling,
                            projected_notional=projected_short_entry_notional,
                        ),
                        slots=max_entry_orders_per_side,
                        role="best_quote_entry_short",
                        inputs=inputs,
                        position_side=short_entry_position_side,
                    )
                )
        else:
            long_reduce_ticks = max(
                int(offset_ticks) + int(config.inventory_bias_reduce_extra_ticks) + reduce_long_extra_ticks,
                0,
            )
            long_reduce_gap = _tick_gap(inputs.tick_size, long_reduce_ticks)
            inventory_bias_report["reduce_offset_ticks"] = long_reduce_ticks
            _append_order(
                sell_orders,
                _build_order(
                    side="SELL",
                    price=_price_with_gap(ask, long_reduce_gap, 1),
                    notional=_recovery_reduce_notional(
                        reduce_notional * reduce_long_budget_scale,
                        long_notional,
                        long_soft,
                    ),
                    role="best_quote_reduce_long",
                    inputs=inputs,
                    position_side=reduce_long_position_side,
                    force_reduce_only=True,
                    min_effective_notional_cap=_soft_recovery_floor(long_notional, long_soft),
                ),
            )
            if config.inventory_bias_opposite_entry_enabled:
                sell_orders.extend(
                    _build_entry_ladder(
                        side="SELL",
                        anchor_price=ask,
                        base_gap=same_side_gap,
                        total_notional=_cap_entry_to_soft_headroom(
                            same_side_notional * short_entry_budget_scale,
                            ceiling_notional=short_entry_ceiling,
                            projected_notional=projected_short_entry_notional,
                        ),
                        slots=max_entry_orders_per_side,
                        role="best_quote_entry_short",
                        inputs=inputs,
                        position_side=short_entry_position_side,
                    )
                )
            else:
                buy_orders.extend(
                    _build_entry_ladder(
                        side="BUY",
                        anchor_price=bid,
                        base_gap=same_side_gap,
                        total_notional=_cap_entry_to_soft_headroom(
                            same_side_notional * long_entry_budget_scale,
                            ceiling_notional=long_entry_ceiling,
                            projected_notional=projected_long_entry_notional,
                        ),
                        slots=max_entry_orders_per_side,
                        role="best_quote_entry_long",
                        inputs=inputs,
                        position_side=long_entry_position_side,
                    )
                )

    if (
        not inventory_bias_report["applied"]
        and short_notional > 0
        and not allow_entry_short
    ):
        reduce_short_order = _build_order(
            side="BUY",
            price=_price_with_gap(bid, reduce_short_gap, -1),
            notional=_recovery_reduce_notional(
                buy_side_notional * reduce_short_budget_scale,
                short_notional,
                short_soft,
            ),
            role="best_quote_reduce_short",
            inputs=inputs,
            position_side=reduce_short_position_side,
            force_reduce_only=True,
            min_effective_notional_cap=_soft_recovery_floor(short_notional, short_soft),
        )
        _append_order(buy_orders, reduce_short_order)
        reduce_short_price = _safe_float(reduce_short_order.get("price")) if reduce_short_order else 0.0
        short_avg_price = _safe_float(inputs.current_short_avg_price)
        if (
            hedge_position_sides
            and allow_entry_long
            and reduce_short_price > 0
            and short_avg_price > 0
            and reduce_short_price > short_avg_price + 1e-12
            and inventory_bias_report["side"] != "long"
        ):
            long_entry_notional = _cap_entry_to_soft_headroom(
                buy_side_notional * long_entry_budget_scale,
                ceiling_notional=long_entry_ceiling,
                projected_notional=projected_long_entry_notional,
            )
            long_entries = _build_entry_ladder(
                side="BUY",
                anchor_price=bid,
                base_gap=gap,
                total_notional=long_entry_notional,
                slots=max_entry_orders_per_side,
                role="best_quote_entry_long",
                inputs=inputs,
                position_side=long_entry_position_side,
            )
            if long_entries:
                buy_orders.extend(long_entries)
                loss_blocked_reduce_fallback_report.update(
                    {
                        "short_reduce": True,
                        "long_entry": True,
                        "reduce_short_price": reduce_short_price,
                    }
                )
        # In recovery, the reduce leg can sit far away or be dropped by the
        # no-loss guard. Keep the opposite entry leg available without
        # bypassing soft/cap/projected limits or loss-reduce protections.
        if (
            inventory_bias_report["recover_applied"]
            and inventory_bias_report["side"] == "short"
            and config.inventory_bias_opposite_entry_enabled
            and allow_entry_long
            and not loss_blocked_reduce_fallback_report["long_entry"]
        ):
            opposite_long_notional = _cap_entry_to_soft_headroom(
                cycle_budget * bias_entry_share * long_entry_budget_scale,
                ceiling_notional=long_entry_ceiling,
                projected_notional=projected_long_entry_notional,
            )
            opposite_long_entries = _build_entry_ladder(
                side="BUY",
                anchor_price=bid,
                base_gap=gap,
                total_notional=opposite_long_notional,
                slots=max_entry_orders_per_side,
                role="best_quote_entry_long",
                inputs=inputs,
                position_side=long_entry_position_side,
            )
            if opposite_long_entries:
                buy_orders.extend(opposite_long_entries)
                inventory_bias_report["recover_opposite_entry_side"] = "long"
    elif not inventory_bias_report["applied"] and allow_entry_long:
        long_entry_notional = _cap_entry_to_soft_headroom(
            buy_side_notional * long_entry_budget_scale,
            ceiling_notional=long_entry_ceiling,
            projected_notional=projected_long_entry_notional,
        )
        buy_orders.extend(
            _build_entry_ladder(
                side="BUY",
                anchor_price=bid,
                base_gap=gap,
                total_notional=long_entry_notional,
                slots=max_entry_orders_per_side,
                role="best_quote_entry_long",
                inputs=inputs,
                position_side=long_entry_position_side,
            )
        )
    elif not inventory_bias_report["applied"] and short_notional > 0:
        reduce_short_order = _build_order(
            side="BUY",
            price=_price_with_gap(bid, reduce_short_gap, -1),
            notional=_recovery_reduce_notional(
                buy_side_notional * reduce_short_budget_scale,
                short_notional,
                short_soft,
            ),
            role="best_quote_reduce_short",
            inputs=inputs,
            position_side=reduce_short_position_side,
            force_reduce_only=True,
            min_effective_notional_cap=_soft_recovery_floor(short_notional, short_soft),
        )
        _append_order(buy_orders, reduce_short_order)
        if (
            reduce_short_order is None
            and inventory_bias_report["recover_applied"]
            and inventory_bias_report["side"] == "long"
            and buy_side_notional > 0
        ):
            buy_orders.extend(
                _build_entry_ladder(
                    side="BUY",
                    anchor_price=bid,
                    base_gap=gap,
                    total_notional=buy_side_notional * long_entry_budget_scale,
                    slots=max_entry_orders_per_side,
                    role="best_quote_entry_long",
                    inputs=inputs,
                    position_side=long_entry_position_side,
                )
            )
    if inventory_bias_report["applied"]:
        pass
    elif long_notional > 0 and not allow_entry_long:
        reduce_long_order = _build_order(
            side="SELL",
            price=_price_with_gap(ask, reduce_long_gap, 1),
            notional=_recovery_reduce_notional(
                sell_side_notional * reduce_long_budget_scale,
                long_notional,
                long_soft,
            ),
            role="best_quote_reduce_long",
            inputs=inputs,
            position_side=reduce_long_position_side,
            force_reduce_only=True,
            min_effective_notional_cap=_soft_recovery_floor(long_notional, long_soft),
        )
        _append_order(sell_orders, reduce_long_order)
        reduce_long_price = _safe_float(reduce_long_order.get("price")) if reduce_long_order else 0.0
        long_avg_price = _safe_float(inputs.current_long_avg_price)
        if (
            hedge_position_sides
            and allow_entry_short
            and reduce_long_price > 0
            and long_avg_price > 0
            and reduce_long_price + 1e-12 < long_avg_price
            and inventory_bias_report["side"] != "short"
        ):
            short_entry_notional = _cap_entry_to_soft_headroom(
                sell_side_notional * short_entry_budget_scale,
                ceiling_notional=short_entry_ceiling,
                projected_notional=projected_short_entry_notional,
            )
            short_entries = _build_entry_ladder(
                side="SELL",
                anchor_price=ask,
                base_gap=gap,
                total_notional=short_entry_notional,
                slots=max_entry_orders_per_side,
                role="best_quote_entry_short",
                inputs=inputs,
                position_side=short_entry_position_side,
            )
            if short_entries:
                sell_orders.extend(short_entries)
                loss_blocked_reduce_fallback_report.update(
                    {
                        "long_reduce": True,
                        "short_entry": True,
                        "reduce_long_price": reduce_long_price,
                    }
                )
        # Symmetric heavy-long recovery: keep entry_short in the submit path
        # even when reduce_long is far away or later dropped by no-loss guard.
        if (
            inventory_bias_report["recover_applied"]
            and inventory_bias_report["side"] == "long"
            and config.inventory_bias_opposite_entry_enabled
            and allow_entry_short
            and not loss_blocked_reduce_fallback_report["short_entry"]
        ):
            opposite_short_notional = _cap_entry_to_soft_headroom(
                cycle_budget * bias_entry_share * short_entry_budget_scale,
                ceiling_notional=short_entry_ceiling,
                projected_notional=projected_short_entry_notional,
            )
            opposite_short_entries = _build_entry_ladder(
                side="SELL",
                anchor_price=ask,
                base_gap=gap,
                total_notional=opposite_short_notional,
                slots=max_entry_orders_per_side,
                role="best_quote_entry_short",
                inputs=inputs,
                position_side=short_entry_position_side,
            )
            if opposite_short_entries:
                sell_orders.extend(opposite_short_entries)
                inventory_bias_report["recover_opposite_entry_side"] = "short"
    elif allow_entry_short:
        short_entry_notional = _cap_entry_to_soft_headroom(
            sell_side_notional * short_entry_budget_scale,
            ceiling_notional=short_entry_ceiling,
            projected_notional=projected_short_entry_notional,
        )
        sell_orders.extend(
            _build_entry_ladder(
                side="SELL",
                anchor_price=ask,
                base_gap=gap,
                total_notional=short_entry_notional,
                slots=max_entry_orders_per_side,
                role="best_quote_entry_short",
                inputs=inputs,
                position_side=short_entry_position_side,
            )
        )
    elif long_notional > 0:
        reduce_long_order = _build_order(
            side="SELL",
            price=_price_with_gap(ask, reduce_long_gap, 1),
            notional=_recovery_reduce_notional(
                sell_side_notional * reduce_long_budget_scale,
                long_notional,
                long_soft,
            ),
            role="best_quote_reduce_long",
            inputs=inputs,
            position_side=reduce_long_position_side,
            force_reduce_only=True,
            min_effective_notional_cap=_soft_recovery_floor(long_notional, long_soft),
        )
        _append_order(sell_orders, reduce_long_order)
        if (
            reduce_long_order is None
            and inventory_bias_report["recover_applied"]
            and inventory_bias_report["side"] == "short"
            and sell_side_notional > 0
        ):
            short_entries = _build_entry_ladder(
                side="SELL",
                anchor_price=ask,
                base_gap=gap,
                total_notional=sell_side_notional * short_entry_budget_scale,
                slots=max_entry_orders_per_side,
                role="best_quote_entry_short",
                inputs=inputs,
                position_side=short_entry_position_side,
            )
            sell_orders.extend(short_entries)
            buy_orders.extend(
                _build_paired_reduce_orders_for_entries(
                    entries=short_entries,
                    inputs=inputs,
                    bid_price=bid,
                    ask_price=ask,
                    available_notional=max(short_notional - sum(_safe_float(o.get("notional")) for o in buy_orders), 0.0),
                    position_side=reduce_short_position_side,
                )
            )

    balanced_cost_recovery_report = {
        "applied": False,
        "reason": None,
        "paired_entry_notional": 0.0,
        "hard_long_headroom": max(long_limit - projected_long_entry_notional, 0.0),
        "hard_short_headroom": max(short_limit - projected_short_entry_notional, 0.0),
    }
    balance_tolerance = max(bias_min_notional_gap, soft_entry_buffer)
    long_avg_price = _safe_float(inputs.current_long_avg_price)
    short_avg_price = _safe_float(inputs.current_short_avg_price)
    exchange_long_avg_price = _safe_float(inputs.exchange_long_avg_price)
    exchange_short_avg_price = _safe_float(inputs.exchange_short_avg_price)
    effective_long_avg_price = max(long_avg_price, exchange_long_avg_price)
    effective_short_avg_price = min(
        price
        for price in (short_avg_price, exchange_short_avg_price)
        if price > 0
    ) if short_avg_price > 0 or exchange_short_avg_price > 0 else 0.0
    balanced_cost_recovery_report.update(
        {
            "managed_long_avg_price": long_avg_price,
            "managed_short_avg_price": short_avg_price,
            "exchange_long_avg_price": exchange_long_avg_price,
            "exchange_short_avg_price": exchange_short_avg_price,
            "effective_long_avg_price": effective_long_avg_price,
            "effective_short_avg_price": effective_short_avg_price,
        }
    )
    if (
        hedge_position_sides
        and not hard_loss
        and not net_loss_reduce_report["active"]
        and not inventory_bias_report["applied"]
        and _safe_float(inputs.target_volume_remaining) > 0
        and long_limit > 0
        and short_limit > 0
        and long_profit_reduce_trigger > 0
        and short_profit_reduce_trigger > 0
        and long_notional >= long_profit_reduce_trigger - 1e-12
        and short_notional >= short_profit_reduce_trigger - 1e-12
        and abs(long_notional - short_notional) <= balance_tolerance + 1e-12
        and effective_long_avg_price > ask + 1e-12
        and effective_short_avg_price + 1e-12 < bid
    ):
        hard_long_headroom = max(long_limit - projected_long_entry_notional, 0.0)
        hard_short_headroom = max(short_limit - projected_short_entry_notional, 0.0)
        paired_entry_notional = min(
            buy_side_notional * long_entry_budget_scale,
            sell_side_notional * short_entry_budget_scale,
            hard_long_headroom,
            hard_short_headroom,
        )
        if paired_entry_notional >= soft_entry_buffer - 1e-12:
            buy_orders = [
                order
                for order in buy_orders
                if order.get("role") != "best_quote_entry_long"
            ]
            sell_orders = [
                order
                for order in sell_orders
                if order.get("role") != "best_quote_entry_short"
            ]
            long_entries = _build_entry_ladder(
                side="BUY",
                anchor_price=bid,
                base_gap=0.0,
                total_notional=paired_entry_notional,
                slots=max_entry_orders_per_side,
                role="best_quote_entry_long",
                inputs=inputs,
                position_side=long_entry_position_side,
            )
            short_entries = _build_entry_ladder(
                side="SELL",
                anchor_price=ask,
                base_gap=0.0,
                total_notional=paired_entry_notional,
                slots=max_entry_orders_per_side,
                role="best_quote_entry_short",
                inputs=inputs,
                position_side=short_entry_position_side,
            )
            if long_entries and short_entries:
                buy_orders.extend(long_entries)
                sell_orders.extend(short_entries)
                reasons.append("balanced_cost_recovery")
                balanced_cost_recovery_report.update(
                    {
                        "applied": True,
                        "reason": "near_soft_underwater_target_remaining",
                        "paired_entry_notional": paired_entry_notional,
                    }
                )

    proactive_profit_reduce_added = False
    if hedge_position_sides and not hard_loss:
        has_short_reduce = any(
            order.get("role") == "best_quote_reduce_short" for order in buy_orders
        )
        if (
            not has_short_reduce
            and short_profit_reduce_trigger > 0
            and short_notional >= short_profit_reduce_trigger - 1e-12
        ):
            before_count = len(buy_orders)
            _append_order(
                buy_orders,
                _build_order(
                    side="BUY",
                    price=bid,
                    notional=min(
                        buy_side_notional * reduce_short_budget_scale,
                        short_notional,
                    ),
                    role="best_quote_reduce_short",
                    inputs=inputs,
                    position_side=reduce_short_position_side,
                    force_reduce_only=True,
                ),
            )
            proactive_profit_reduce_added = len(buy_orders) > before_count

        has_long_reduce = any(
            order.get("role") == "best_quote_reduce_long" for order in sell_orders
        )
        if (
            not has_long_reduce
            and long_profit_reduce_trigger > 0
            and long_notional >= long_profit_reduce_trigger - 1e-12
        ):
            before_count = len(sell_orders)
            _append_order(
                sell_orders,
                _build_order(
                    side="SELL",
                    price=ask,
                    notional=min(
                        sell_side_notional * reduce_long_budget_scale,
                        long_notional,
                    ),
                    role="best_quote_reduce_long",
                    inputs=inputs,
                    position_side=reduce_long_position_side,
                    force_reduce_only=True,
                ),
            )
            proactive_profit_reduce_added = proactive_profit_reduce_added or len(sell_orders) > before_count
    if proactive_profit_reduce_added:
        reasons.append("near_soft_profit_reduce")

    if net_loss_reduce_report["active"] and hedge_position_sides:
        imbalance_notional = long_notional - short_notional
        hedge_notional = min(cycle_budget, abs(imbalance_notional))
        if imbalance_notional > 0 and short_notional > 0 and entry_short_allowed_before_net_loss:
            if short_limit > 0:
                hedge_notional = min(hedge_notional, max(short_limit - projected_short_entry_notional, 0.0))
            hedge_orders = _build_entry_ladder(
                side="SELL",
                anchor_price=ask,
                base_gap=gap,
                total_notional=hedge_notional,
                slots=1,
                role="best_quote_entry_short",
                inputs=inputs,
                position_side=short_entry_position_side,
            )
            if hedge_orders:
                buy_orders = []
                sell_orders = hedge_orders
                net_loss_reduce_report["hedge_recovery_action"] = "entry_short"
                reasons.append("net_loss_hedge_recovery")
        elif imbalance_notional < 0 and long_notional > 0 and entry_long_allowed_before_net_loss:
            if long_limit > 0:
                hedge_notional = min(hedge_notional, max(long_limit - projected_long_entry_notional, 0.0))
            hedge_orders = _build_entry_ladder(
                side="BUY",
                anchor_price=bid,
                base_gap=gap,
                total_notional=hedge_notional,
                slots=1,
                role="best_quote_entry_long",
                inputs=inputs,
                position_side=long_entry_position_side,
            )
            if hedge_orders:
                buy_orders = hedge_orders
                sell_orders = []
                net_loss_reduce_report["hedge_recovery_action"] = "entry_long"
                reasons.append("net_loss_hedge_recovery")

    four_leg_cycle_report: dict[str, Any] | None = None
    if config.four_leg_cycle_enabled:
        role_report: dict[str, dict[str, str]] = {}

        def _role_present(role: str) -> bool:
            return any(
                order.get("role") == role
                for order in [*buy_orders, *sell_orders]
            )

        def _record_entry_role(
            name: str,
            role: str,
            *,
            position_notional: float,
            projected_notional: float,
            soft_notional: float,
            entry_ceiling: float,
        ) -> None:
            if name in role_report:
                return
            if _role_present(role):
                role_report[name] = {
                    "status": "planned",
                    "reason": "order_present",
                }
                return
            if soft_notional > 0 and position_notional >= soft_notional - 1e-12:
                reason = "soft_threshold_active_pair"
            elif (
                entry_ceiling > 0
                and entry_ceiling - projected_notional
                < max(_safe_float(inputs.min_notional), 0.0) - 1e-12
            ):
                reason = "soft_headroom_below_min_notional"
            else:
                reason = "missing_entry"
            role_report[name] = {"status": "blocked", "reason": reason}

        def _ensure_biased_heavy_entry(
            *,
            report_name: str,
            role: str,
            position_side: str,
            projected_notional: float,
            entry_ceiling: float,
            side: str,
        ) -> None:
            target_bucket = buy_orders if side == "BUY" else sell_orders
            if any(order.get("role") == role for order in target_bucket):
                role_report[report_name] = {
                    "status": "planned",
                    "reason": "order_present",
                }
                return
            if (
                not hedge_position_sides
                or not inventory_bias_report["applied"]
                or inventory_bias_report["side"] != position_side.lower()
            ):
                return
            headroom = max(entry_ceiling - projected_notional, 0.0)
            entry_notional = min(cycle_budget * 0.25, headroom)
            min_notional = max(_safe_float(inputs.min_notional), 0.0)
            if entry_notional + 1e-12 < min_notional:
                role_report[report_name] = {
                    "status": "blocked",
                    "reason": "soft_headroom_below_min_notional",
                }
                return
            entry_ticks = max(
                int(offset_ticks)
                + max(int(config.inventory_bias_same_side_extra_ticks), 0),
                0,
            )
            entry_gap = _tick_gap(inputs.tick_size, entry_ticks)
            orders = _build_entry_ladder(
                side=side,
                anchor_price=bid if side == "BUY" else ask,
                base_gap=entry_gap,
                total_notional=entry_notional,
                slots=1,
                role=role,
                inputs=inputs,
                position_side=position_side,
            )
            if not orders:
                role_report[report_name] = {
                    "status": "blocked",
                    "reason": "entry_below_exchange_minimum",
                }
                return
            target_bucket.extend(orders)
            role_report[report_name] = {
                "status": "planned",
                "reason": "bounded_heavy_side_entry",
            }

        def _ensure_profit_reduce(
            *,
            report_name: str,
            role: str,
            position_side: str,
            position_notional: float,
            position_cost: float,
            cost_source: str,
            position_lots: list[dict[str, Any]] | None,
            soft_notional: float,
            side: str,
        ) -> None:
            nonlocal buy_orders, sell_orders
            target_bucket = sell_orders if side == "SELL" else buy_orders
            existing = [order for order in target_bucket if order.get("role") == role]

            def _remove_existing() -> None:
                if existing:
                    target_bucket[:] = [
                        order for order in target_bucket if order.get("role") != role
                    ]

            cost_audit: dict[str, Any] = {
                "cost_source": cost_source,
                "position_cost": position_cost if position_cost > 0 else None,
                "target_price": None,
            }
            if not hedge_position_sides:
                role_report[report_name] = {
                    "status": "not_applicable",
                    "reason": "not_hedge_mode",
                    **cost_audit,
                }
                return
            if position_notional <= 0:
                role_report[report_name] = {
                    "status": "not_applicable",
                    "reason": "no_ordinary_position",
                    **cost_audit,
                }
                return
            if hard_loss or net_loss_reduce_report["active"]:
                role_report[report_name] = {
                    "status": "blocked",
                    "reason": "loss_control_active",
                    **cost_audit,
                }
                return
            if soft_notional > 0 and position_notional >= soft_notional - 1e-12:
                role_report[report_name] = {
                    "status": "blocked",
                    "reason": "soft_threshold_active_pair",
                    **cost_audit,
                }
                return
            paired_lot_qty: float | None = None
            eligible_lots: list[dict[str, Any]] | None = None
            if position_lots is not None:
                entry_role = (
                    "best_quote_entry_long"
                    if position_side == "LONG"
                    else "best_quote_entry_short"
                )
                eligible_lots = [
                    lot
                    for lot in position_lots
                    if isinstance(lot, dict)
                    and str(lot.get("role") or "") == entry_role
                    and _safe_float(lot.get("qty")) > 1e-12
                    and _safe_float(lot.get("price", lot.get("entry_price"))) > 0
                ]
                if not eligible_lots:
                    _remove_existing()
                    role_report[report_name] = {
                        "status": "blocked",
                        "reason": "no_unreleased_entry_lot",
                        "cost_source": "ordinary_entry_lot",
                        "position_cost": None,
                        "target_price": None,
                    }
                    return
                eligible_lots.sort(
                    key=lambda lot: _safe_float(
                        lot.get("price", lot.get("entry_price"))
                    ),
                    reverse=position_side == "SHORT",
                )
                position_cost = _safe_float(
                    eligible_lots[0].get("price", eligible_lots[0].get("entry_price"))
                )
                cost_source = "ordinary_entry_lot"
                cost_audit.update(
                    {
                        "cost_source": cost_source,
                        "position_cost": position_cost,
                    }
                )
            if position_cost <= 0:
                role_report[report_name] = {
                    "status": "blocked",
                    "reason": "missing_position_cost",
                    **cost_audit,
                }
                return
            retained_notional = max(
                max(_safe_float(config.min_cycle_budget_notional), 0.0) * 0.5,
                max(_safe_float(inputs.min_notional), 0.0),
            )
            reducible_notional = max(position_notional - retained_notional, 0.0)
            cost_audit["retained_notional"] = retained_notional
            cost_audit["reducible_notional"] = reducible_notional
            if reducible_notional + 1e-12 < max(_safe_float(inputs.min_notional), 0.0):
                _remove_existing()
                role_report[report_name] = {
                    "status": "blocked",
                    "reason": "retained_inventory_floor",
                    **cost_audit,
                }
                return

            price = _four_leg_profit_price(
                position_side=position_side,
                position_cost=position_cost,
                step_price=_safe_float(inputs.entry_ladder_spacing),
                best_bid=bid,
                best_ask=ask,
                tick_size=inputs.tick_size,
            )
            if price <= 0:
                role_report[report_name] = {
                    "status": "blocked",
                    "reason": "invalid_profit_price",
                    **cost_audit,
                }
                return
            cost_audit["target_price"] = price
            if eligible_lots is not None:
                paired_lot_qty = sum(
                    max(_safe_float(lot.get("qty")), 0.0)
                    for lot in eligible_lots
                    if abs(
                        _four_leg_profit_price(
                            position_side=position_side,
                            position_cost=_safe_float(
                                lot.get("price", lot.get("entry_price"))
                            ),
                            step_price=_safe_float(inputs.entry_ladder_spacing),
                            best_bid=bid,
                            best_ask=ask,
                            tick_size=inputs.tick_size,
                        )
                        - price
                    )
                    <= 1e-12
                )

            profit_boundary = position_cost + max(
                _safe_float(inputs.entry_ladder_spacing),
                _safe_float(inputs.tick_size),
            ) if position_side == "LONG" else position_cost - max(
                _safe_float(inputs.entry_ladder_spacing),
                _safe_float(inputs.tick_size),
            )
            position_price = max(_safe_float(inputs.mid_price), price, 1e-12)
            reducible_qty = reducible_notional / position_price
            reduce_qty_cap = min(
                cycle_budget * 0.5 / price,
                reducible_qty,
                paired_lot_qty if paired_lot_qty is not None else float("inf"),
            )
            reduce_notional = reduce_qty_cap * price
            existing_qty = sum(max(_safe_float(order.get("qty")), 0.0) for order in existing)
            existing_is_eligible = bool(existing) and all(
                abs(_safe_float(order.get("price")) - price) <= 1e-12
                and (
                    _safe_float(order.get("price")) >= profit_boundary - 1e-12
                    if position_side == "LONG"
                    else _safe_float(order.get("price")) <= profit_boundary + 1e-12
                )
                for order in existing
            ) and existing_qty <= reduce_qty_cap + 1e-12
            if existing_is_eligible:
                role_report[report_name] = {
                    "status": "planned",
                    "reason": "existing_profit_order",
                    **cost_audit,
                }
                return

            _remove_existing()
            order = _build_order(
                side=side,
                price=price,
                notional=reduce_notional,
                role=role,
                inputs=inputs,
                position_side=position_side,
                force_reduce_only=True,
            )
            if order is None:
                role_report[report_name] = {
                    "status": "blocked",
                    "reason": "reduce_below_exchange_minimum",
                    **cost_audit,
                }
                return
            if paired_lot_qty is not None:
                order.update(
                    {
                        "ordinary_entry_lot_profit": True,
                        "ordinary_entry_lot_cost_price": position_cost,
                        "ordinary_entry_lot_profit_boundary": price,
                        "ordinary_entry_lot_qty_cap": paired_lot_qty,
                    }
                )
            target_bucket.append(order)
            role_report[report_name] = {
                "status": "planned",
                "reason": "cost_based_profit_order",
                **cost_audit,
            }

        def _ordinary_position_cost(
            *,
            managed_cost: float,
            exchange_cost: float,
            frozen_notional: float,
        ) -> tuple[float, str]:
            managed_cost = _safe_float(managed_cost)
            if managed_cost > 0:
                return managed_cost, "ordinary_ledger"
            if frozen_notional > 0:
                return 0.0, "blocked_by_frozen_isolation"
            exchange_cost = _safe_float(exchange_cost)
            if exchange_cost > 0:
                return exchange_cost, "exchange_position"
            return 0.0, "unavailable"

        long_position_cost, long_cost_source = _ordinary_position_cost(
            managed_cost=inputs.current_long_avg_price,
            exchange_cost=inputs.exchange_long_avg_price,
            frozen_notional=frozen_long_notional,
        )
        short_position_cost, short_cost_source = _ordinary_position_cost(
            managed_cost=inputs.current_short_avg_price,
            exchange_cost=inputs.exchange_short_avg_price,
            frozen_notional=frozen_short_notional,
        )

        _ensure_profit_reduce(
            report_name="long_profit_reduce",
            role="best_quote_reduce_long",
            position_side="LONG",
            position_notional=long_notional,
            position_cost=long_position_cost,
            cost_source=long_cost_source,
            position_lots=inputs.current_long_lots,
            soft_notional=long_soft,
            side="SELL",
        )
        _ensure_profit_reduce(
            report_name="short_profit_reduce",
            role="best_quote_reduce_short",
            position_side="SHORT",
            position_notional=short_notional,
            position_cost=short_position_cost,
            cost_source=short_cost_source,
            position_lots=inputs.current_short_lots,
            soft_notional=short_soft,
            side="BUY",
        )
        _ensure_biased_heavy_entry(
            report_name="long_entry",
            role="best_quote_entry_long",
            position_side="LONG",
            projected_notional=projected_long_entry_notional,
            entry_ceiling=long_entry_ceiling,
            side="BUY",
        )
        _ensure_biased_heavy_entry(
            report_name="short_entry",
            role="best_quote_entry_short",
            position_side="SHORT",
            projected_notional=projected_short_entry_notional,
            entry_ceiling=short_entry_ceiling,
            side="SELL",
        )
        _record_entry_role(
            "long_entry",
            "best_quote_entry_long",
            position_notional=long_notional,
            projected_notional=projected_long_entry_notional,
            soft_notional=long_soft,
            entry_ceiling=long_entry_ceiling,
        )
        _record_entry_role(
            "short_entry",
            "best_quote_entry_short",
            position_notional=short_notional,
            projected_notional=projected_short_entry_notional,
            soft_notional=short_soft,
            entry_ceiling=short_entry_ceiling,
        )
        four_leg_cycle_report = {
            "enabled": True,
            "roles": role_report,
            "complete": all(
                item["status"] in {"planned", "not_applicable"}
                for item in role_report.values()
            ),
        }

    same_side_entry_price_guard_report = {
        "enabled": bool(config.same_side_entry_price_guard_enabled),
        "applied": False,
        "report_only": bool(config.same_side_entry_price_guard_report_only),
        "blocked_long_entry": False,
        "blocked_short_entry": False,
        "would_block_long_entry": False,
        "would_block_short_entry": False,
        "min_inventory_notional": max(_safe_float(config.same_side_entry_price_guard_min_notional), 0.0),
        "gap_ticks": max(int(_safe_float(config.same_side_entry_price_guard_gap_ticks)), 0),
        "long_reference_price": None,
        "short_reference_price": None,
        "blocked_buy_orders": 0,
        "blocked_sell_orders": 0,
        "would_block_buy_orders": 0,
        "would_block_sell_orders": 0,
        "cluster_pruning_applied": False,
        "cluster_pruned_buy_orders": 0,
        "cluster_pruned_sell_orders": 0,
    }
    if config.same_side_entry_price_guard_enabled:
        guard_gap = _tick_gap(inputs.tick_size, int(same_side_entry_price_guard_report["gap_ticks"]))
        min_guard_notional = _safe_float(same_side_entry_price_guard_report["min_inventory_notional"])
        long_reference_price = _safe_float(inputs.current_long_avg_price)
        short_reference_price = _safe_float(inputs.current_short_avg_price)
        same_side_entry_price_guard_report["long_reference_price"] = (
            long_reference_price if long_reference_price > 0 else None
        )
        same_side_entry_price_guard_report["short_reference_price"] = (
            short_reference_price if short_reference_price > 0 else None
        )
        if long_notional >= min_guard_notional and long_reference_price > 0:
            max_long_entry_price = max(long_reference_price - guard_gap, 0.0)
            blocked_buy_orders: list[dict[str, Any]] = []
            for order in buy_orders:
                if (
                    str(order.get("role", "")).lower().strip() == "best_quote_entry_long"
                    and _safe_float(order.get("price")) > max_long_entry_price + 1e-12
                ):
                    blocked_buy_orders.append(order)
            if blocked_buy_orders:
                same_side_entry_price_guard_report["would_block_long_entry"] = True
                same_side_entry_price_guard_report["would_block_buy_orders"] = len(blocked_buy_orders)
                same_side_entry_price_guard_report["max_long_entry_price"] = max_long_entry_price
                if not config.same_side_entry_price_guard_report_only:
                    blocked_ids = {id(order) for order in blocked_buy_orders}
                    buy_orders = [order for order in buy_orders if id(order) not in blocked_ids]
                    same_side_entry_price_guard_report["applied"] = True
                    same_side_entry_price_guard_report["blocked_long_entry"] = True
                    same_side_entry_price_guard_report["blocked_buy_orders"] = len(blocked_buy_orders)
        if short_notional >= min_guard_notional and short_reference_price > 0:
            min_short_entry_price = short_reference_price + guard_gap
            blocked_sell_orders: list[dict[str, Any]] = []
            for order in sell_orders:
                if (
                    str(order.get("role", "")).lower().strip() == "best_quote_entry_short"
                    and _safe_float(order.get("price")) + 1e-12 < min_short_entry_price
                ):
                    blocked_sell_orders.append(order)
            if blocked_sell_orders:
                same_side_entry_price_guard_report["would_block_short_entry"] = True
                same_side_entry_price_guard_report["would_block_sell_orders"] = len(blocked_sell_orders)
                same_side_entry_price_guard_report["min_short_entry_price"] = min_short_entry_price
                if not config.same_side_entry_price_guard_report_only:
                    blocked_ids = {id(order) for order in blocked_sell_orders}
                    sell_orders = [order for order in sell_orders if id(order) not in blocked_ids]
                    same_side_entry_price_guard_report["applied"] = True
                    same_side_entry_price_guard_report["blocked_short_entry"] = True
                    same_side_entry_price_guard_report["blocked_sell_orders"] = len(blocked_sell_orders)
        if guard_gap > 0:
            buy_orders, pruned_buy_orders = _prune_clustered_same_side_entry_orders(
                buy_orders,
                side="BUY",
                min_gap=guard_gap,
            )
            sell_orders, pruned_sell_orders = _prune_clustered_same_side_entry_orders(
                sell_orders,
                side="SELL",
                min_gap=guard_gap,
            )
            if pruned_buy_orders or pruned_sell_orders:
                same_side_entry_price_guard_report["cluster_pruning_applied"] = True
                same_side_entry_price_guard_report["cluster_pruned_buy_orders"] = pruned_buy_orders
                same_side_entry_price_guard_report["cluster_pruned_sell_orders"] = pruned_sell_orders

    if four_leg_cycle_report is not None:
        for report_name, role in (
            ("long_entry", "best_quote_entry_long"),
            ("short_entry", "best_quote_entry_short"),
            ("long_profit_reduce", "best_quote_reduce_long"),
            ("short_profit_reduce", "best_quote_reduce_short"),
        ):
            status = four_leg_cycle_report["roles"][report_name]
            if status["status"] == "planned" and not _role_present(role):
                four_leg_cycle_report["roles"][report_name] = {
                    "status": "blocked",
                    "reason": "removed_by_post_plan_guard",
                }
        four_leg_cycle_report["complete"] = all(
            item["status"] in {"planned", "not_applicable"}
            for item in four_leg_cycle_report["roles"].values()
        )

    planned = sum(order["notional"] for order in [*buy_orders, *sell_orders])
    metrics = {
        "loss_per_10k_15m": loss_per_10k,
        "long_notional": long_notional,
        "short_notional": short_notional,
        "open_entry_long_notional": open_entry_long_notional,
        "open_entry_short_notional": open_entry_short_notional,
        "pending_entry_buffer_notional": pending_entry_buffer_notional,
        "projected_long_entry_notional": projected_long_entry_notional,
        "projected_short_entry_notional": projected_short_entry_notional,
        "cycle_budget_notional": cycle_budget,
        "base_cycle_budget_notional": base_cycle_budget,
        "soft_recovery_min_reduce_notional": soft_recovery_min_reduce_notional,
        "long_inventory_ratio": long_inventory_ratio,
        "short_inventory_ratio": short_inventory_ratio,
        "inventory_ratio": inventory_ratio,
        "buy_side_notional": buy_side_notional,
        "sell_side_notional": sell_side_notional,
        "effective_ladder_spacing": _safe_float(inputs.entry_ladder_spacing),
        "dynamic_control": dynamic_control_report,
        "trend_entry_guard": trend_entry_guard_report,
        "trend_inventory_guard": trend_inventory_guard_report,
        "trend_loss_reduce_guard": trend_loss_reduce_guard_report,
        "frozen_v2": frozen_v2_report,
        "net_loss_reduce": net_loss_reduce_report,
        "loss_blocked_reduce_fallback": loss_blocked_reduce_fallback_report,
        "balanced_cost_recovery": balanced_cost_recovery_report,
        "same_side_entry_price_guard": same_side_entry_price_guard_report,
        "dynamic_tick": dynamic_tick_report,
        "inventory_bias": inventory_bias_report,
    }
    if four_leg_cycle_report is not None:
        metrics["four_leg_cycle"] = four_leg_cycle_report
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
        "metrics": metrics,
    }
