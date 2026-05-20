from __future__ import annotations

from dataclasses import dataclass, replace
from decimal import Decimal
from typing import Any

from .dry_run import _round_order_price, _round_order_qty


@dataclass(frozen=True)
class BestQuoteMakerVolumeConfig:
    enabled: bool = False
    quote_offset_ticks: int = 0
    defensive_offset_ticks: int = 3
    max_entry_orders_per_side: int = 1
    max_long_notional: float = 1_500.0
    max_short_notional: float = 1_500.0
    inventory_soft_ratio: float = 0.60
    loss_per_10k_soft: float = 0.5
    loss_per_10k_hard: float = 0.8
    soft_loss_budget_scale: float = 0.50
    min_cycle_budget_notional: float = 20.0
    dynamic_tick_enabled: bool = False
    dynamic_tick_tight_offset_ticks: int = 2
    dynamic_tick_low_loss_per_10k: float = 3.0
    dynamic_tick_mid_loss_per_10k: float = 5.0
    dynamic_tick_low_inventory_ratio: float = 0.35
    dynamic_tick_high_inventory_ratio: float = 0.75
    inventory_bias_enabled: bool = False
    inventory_bias_start_ratio: float = 0.25
    inventory_bias_reduce_share: float = 0.70
    inventory_bias_same_side_extra_ticks: int = 2
    inventory_bias_reduce_extra_ticks: int = -1
    dynamic_control_enabled: bool = False
    dynamic_control_low_volatility_ratio: float = 0.0015
    dynamic_control_high_volatility_ratio: float = 0.0035
    dynamic_control_extreme_volatility_ratio: float = 0.007
    dynamic_control_low_volatility_budget_scale: float = 1.15
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
    entry_ladder_spacing: float | None = None
    current_long_qty: float | None = None
    current_short_qty: float | None = None
    position_side_mode: str = "one_way"
    market_return_1m: float = 0.0
    market_amplitude_1m: float = 0.0
    market_return_5m: float = 0.0
    market_amplitude_5m: float = 0.0


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
    position_side: str = "BOTH",
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
    per_order_notional = total_notional / float(safe_slots)
    tick_gap = _tick_gap(inputs.tick_size, 1)
    ladder_gap = max(_safe_float(inputs.entry_ladder_spacing), tick_gap)
    sign = -1 if str(side).upper() == "BUY" else 1
    orders: list[dict[str, Any]] = []
    for level in range(safe_slots):
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
    return orders


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
    loss_per_10k = max(_safe_float(inputs.loss_per_10k_15m), 0.0)
    long_inventory_ratio = long_notional / long_soft if long_soft > 0 else 0.0
    short_inventory_ratio = short_notional / short_soft if short_soft > 0 else 0.0
    inventory_ratio = max(long_inventory_ratio, short_inventory_ratio)

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
    base_cycle_budget = cycle_budget
    base_ladder_spacing = max(_safe_float(inputs.entry_ladder_spacing), 0.0)
    dynamic_control_report = {
        "enabled": bool(config.dynamic_control_enabled),
        "applied": False,
        "reason": None,
        "volatility_ratio": 0.0,
        "trend_score": 0.0,
        "budget_scale": 1.0,
        "step_scale": 1.0,
        "extra_offset_ticks": 0,
        "buy_budget_share": None,
        "sell_budget_share": None,
        "base_cycle_budget_notional": base_cycle_budget,
        "effective_cycle_budget_notional": cycle_budget,
        "base_ladder_spacing": base_ladder_spacing,
        "effective_ladder_spacing": base_ladder_spacing,
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
        elif low_vol > 0 and 0 < volatility_ratio <= low_vol and inventory_ratio < 0.75 and not soft_loss:
            budget_scale = max(_safe_float(config.dynamic_control_low_volatility_budget_scale), 1.0)
            step_scale = _clamp(_safe_float(config.dynamic_control_low_volatility_step_scale), 0.1, 1.0)
            extra_offset_ticks = min(int(config.dynamic_control_low_volatility_extra_offset_ticks), 0)
            reason = "low_volatility_expand"
        trend_threshold = max(_safe_float(config.dynamic_control_trend_return_ratio), 1e-12)
        trend_score = _clamp(((return_1m * 0.65) + (return_5m * 0.35)) / trend_threshold, -1.0, 1.0)
        cycle_budget *= budget_scale
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
    buy_slot_active = allow_entry_long or (short_notional > 0 and not allow_entry_short)
    sell_slot_active = allow_entry_short or (long_notional > 0 and not allow_entry_long)
    active_side_count = max((1 if buy_slot_active else 0) + (1 if sell_slot_active else 0), 1)
    buy_side_notional = cycle_budget / active_side_count
    sell_side_notional = cycle_budget / active_side_count
    if config.dynamic_control_enabled and buy_slot_active and sell_slot_active:
        max_bias = _clamp(_safe_float(config.dynamic_control_trend_bias_max), 0.0, 0.45)
        trend_score = _clamp(_safe_float(dynamic_control_report.get("trend_score")), -1.0, 1.0)
        buy_share = _clamp(0.5 + (trend_score * max_bias), 0.05, 0.95)
        sell_share = 1.0 - buy_share
        buy_side_notional = cycle_budget * buy_share
        sell_side_notional = cycle_budget * sell_share
        dynamic_control_report["buy_budget_share"] = buy_share
        dynamic_control_report["sell_budget_share"] = sell_share

    buy_orders: list[dict[str, Any]] = []
    sell_orders: list[dict[str, Any]] = []
    max_entry_orders_per_side = max(int(_safe_float(config.max_entry_orders_per_side)), 1)
    long_entry_position_side = "LONG" if hedge_position_sides else "BOTH"
    short_entry_position_side = "SHORT" if hedge_position_sides else "BOTH"
    reduce_long_position_side = "LONG" if hedge_position_sides else "BOTH"
    reduce_short_position_side = "SHORT" if hedge_position_sides else "BOTH"
    inventory_bias_report = {
        "enabled": bool(config.inventory_bias_enabled),
        "applied": False,
        "side": None,
        "inventory_ratio": inventory_ratio,
        "reduce_share": None,
        "same_side_entry_share": None,
        "reduce_offset_ticks": None,
        "same_side_offset_ticks": None,
    }
    bias_start = _clamp(_safe_float(config.inventory_bias_start_ratio), 0.0, 1.0)
    bias_reduce_share = _clamp(_safe_float(config.inventory_bias_reduce_share), 0.0, 1.0)
    bias_entry_share = max(1.0 - bias_reduce_share, 0.0)
    can_bias_short = (
        config.inventory_bias_enabled
        and regime == "normal"
        and short_notional > 0
        and allow_entry_long
        and allow_entry_short
        and short_soft > 0
        and short_notional >= short_soft * bias_start
    )
    can_bias_long = (
        config.inventory_bias_enabled
        and regime == "normal"
        and long_notional > 0
        and allow_entry_long
        and allow_entry_short
        and long_soft > 0
        and long_notional >= long_soft * bias_start
    )
    if can_bias_short and can_bias_long:
        can_bias_short = short_inventory_ratio >= long_inventory_ratio
        can_bias_long = not can_bias_short
    if can_bias_short or can_bias_long:
        reduce_ticks = max(int(offset_ticks) + int(config.inventory_bias_reduce_extra_ticks), 0)
        same_side_ticks = max(int(offset_ticks) + max(int(config.inventory_bias_same_side_extra_ticks), 0), 0)
        reduce_gap = _tick_gap(inputs.tick_size, reduce_ticks)
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
                "reduce_offset_ticks": reduce_ticks,
                "same_side_offset_ticks": same_side_ticks,
            }
        )
        if can_bias_short:
            _append_order(
                buy_orders,
                _build_order(
                    side="BUY",
                    price=_price_with_gap(bid, reduce_gap, -1),
                    notional=min(reduce_notional, short_notional),
                    role="best_quote_reduce_short",
                    inputs=inputs,
                    position_side=reduce_short_position_side,
                    force_reduce_only=True,
                ),
            )
            sell_orders.extend(
                _build_entry_ladder(
                    side="SELL",
                    anchor_price=ask,
                    base_gap=same_side_gap,
                    total_notional=same_side_notional,
                    slots=max_entry_orders_per_side,
                    role="best_quote_entry_short",
                    inputs=inputs,
                    position_side=short_entry_position_side,
                )
            )
        else:
            _append_order(
                sell_orders,
                _build_order(
                    side="SELL",
                    price=_price_with_gap(ask, reduce_gap, 1),
                    notional=min(reduce_notional, long_notional),
                    role="best_quote_reduce_long",
                    inputs=inputs,
                    position_side=reduce_long_position_side,
                    force_reduce_only=True,
                ),
            )
            buy_orders.extend(
                _build_entry_ladder(
                    side="BUY",
                    anchor_price=bid,
                    base_gap=same_side_gap,
                    total_notional=same_side_notional,
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
        _append_order(
            buy_orders,
            _build_order(
                side="BUY",
                price=_price_with_gap(bid, gap, -1),
                notional=min(buy_side_notional, short_notional),
                role="best_quote_reduce_short",
                inputs=inputs,
                position_side=reduce_short_position_side,
                force_reduce_only=True,
            ),
        )
    elif not inventory_bias_report["applied"] and allow_entry_long:
        long_entry_notional = buy_side_notional
        if long_limit > 0:
            long_entry_notional = min(
                long_entry_notional,
                max(long_limit - projected_long_entry_notional, 0.0),
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
        _append_order(
            buy_orders,
            _build_order(
                side="BUY",
                price=_price_with_gap(bid, gap, -1),
                notional=min(buy_side_notional, short_notional),
                role="best_quote_reduce_short",
                inputs=inputs,
                position_side=reduce_short_position_side,
                force_reduce_only=True,
            ),
        )
    if inventory_bias_report["applied"]:
        pass
    elif long_notional > 0 and not allow_entry_long:
        _append_order(
            sell_orders,
            _build_order(
                side="SELL",
                price=_price_with_gap(ask, gap, 1),
                notional=min(sell_side_notional, long_notional),
                role="best_quote_reduce_long",
                inputs=inputs,
                position_side=reduce_long_position_side,
                force_reduce_only=True,
            ),
        )
    elif allow_entry_short:
        short_entry_notional = sell_side_notional
        if short_limit > 0:
            short_entry_notional = min(
                short_entry_notional,
                max(short_limit - projected_short_entry_notional, 0.0),
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
        _append_order(
            sell_orders,
            _build_order(
                side="SELL",
                price=_price_with_gap(ask, gap, 1),
                notional=min(sell_side_notional, long_notional),
                role="best_quote_reduce_long",
                inputs=inputs,
                position_side=reduce_long_position_side,
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
            "base_cycle_budget_notional": base_cycle_budget,
            "long_inventory_ratio": long_inventory_ratio,
            "short_inventory_ratio": short_inventory_ratio,
            "inventory_ratio": inventory_ratio,
            "buy_side_notional": buy_side_notional,
            "sell_side_notional": sell_side_notional,
            "effective_ladder_spacing": _safe_float(inputs.entry_ladder_spacing),
            "dynamic_control": dynamic_control_report,
            "dynamic_tick": dynamic_tick_report,
            "inventory_bias": inventory_bias_report,
        },
    }
