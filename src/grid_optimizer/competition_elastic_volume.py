from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any


@dataclass(frozen=True)
class ElasticVolumeConfig:
    enabled: bool = False
    loss_per_10k_sprint: float = 0.3
    loss_per_10k_cruise: float = 0.8
    loss_per_10k_defensive: float = 1.2
    loss_per_10k_cooldown: float = 1.8
    inventory_soft_ratio: float = 0.6
    inventory_hard_ratio: float = 0.9
    inventory_soft_ratio_ping_pong_fast: float = 0.30
    inventory_hard_ratio_ping_pong_fast: float = 0.45
    inventory_soft_ratio_ping_pong_safe: float = 0.45
    inventory_hard_ratio_ping_pong_safe: float = 0.60
    inventory_soft_ratio_wide_step_attack: float = 0.45
    inventory_hard_ratio_wide_step_attack: float = 0.65
    inventory_soft_ratio_wide_step: float = 0.60
    inventory_hard_ratio_wide_step: float = 0.80
    adaptive_raw_scale_defensive: float = 1.2
    adaptive_raw_scale_cooldown: float = 2.0
    step_scale_sprint: float = 0.8
    step_scale_defensive: float = 1.8
    step_scale_cooldown: float = 3.0
    base_step_multiplier_ping_pong_fast: float = 0.8
    base_step_multiplier_ping_pong_safe: float = 1.2
    base_step_multiplier_wide_step_attack: float = 2.2
    base_step_multiplier_wide_step: float = 2.5
    base_step_multiplier_defensive: float = 3.5
    per_order_scale_sprint: float = 1.25
    per_order_scale_defensive: float = 0.65
    per_order_scale_ping_pong_fast: float = 1.0
    per_order_scale_ping_pong_safe: float = 0.9
    per_order_scale_wide_step_attack: float = 1.2
    per_order_scale_wide_step: float = 0.6
    per_order_scale_defensive_state: float = 0.4
    levels_scale_sprint: float = 1.25
    levels_scale_defensive: float = 0.65
    levels_scale_ping_pong_fast: float = 1.0
    levels_scale_ping_pong_safe: float = 0.85
    levels_scale_wide_step_attack: float = 1.1
    levels_scale_wide_step: float = 0.65
    levels_scale_defensive_state: float = 0.35
    threshold_scale_ping_pong_fast: float = 1.0
    threshold_scale_ping_pong_safe: float = 1.1
    threshold_scale_wide_step_attack: float = 1.5
    threshold_scale_wide_step: float = 1.5
    threshold_scale_defensive: float = 1.8
    pause_scale_ping_pong_fast: float = 1.0
    pause_scale_ping_pong_safe: float = 1.1
    pause_scale_wide_step_attack: float = 1.2
    pause_scale_wide_step: float = 1.5
    pause_scale_defensive: float = 1.8
    max_total_scale_ping_pong_fast: float = 1.0
    max_total_scale_ping_pong_safe: float = 1.1
    max_total_scale_wide_step_attack: float = 1.2
    max_total_scale_wide_step: float = 1.35
    max_total_scale_defensive: float = 1.6
    max_entry_orders_ping_pong_fast: int = 6
    max_entry_orders_ping_pong_safe: int = 4
    max_entry_orders_wide_step_attack: int = 4
    max_entry_orders_wide_step: int = 3
    max_entry_orders_defensive: int = 2
    early_micro_abs_return_ratio: float = 0.00025
    early_micro_amplitude_ratio: float = 0.00035
    early_safe_inventory_ratio: float = 0.45
    early_wide_inventory_ratio: float = 0.65
    early_wide_loss_per_10k_5m: float = 0.5
    cooldown_seconds: float = 120.0
    state_confirm_cycles: int = 3
    inventory_recover_exit_ratio: float = 0.45
    repair_stale_cycles: int = 4
    adverse_move_ticks: float = 3.0
    adverse_move_bps: float = 5.0
    repair_slice_ratio_passive: float = 0.10
    repair_slice_ratio_touch: float = 0.20
    repair_slice_ratio_near_cross: float = 0.30
    repair_slice_ratio_cross: float = 0.60
    max_repair_loss_per_10k: float = 0.0
    cancel_stale_entries_on_cooldown: bool = True


@dataclass(frozen=True)
class ElasticVolumeInputs:
    now: datetime
    last_state: dict[str, Any]
    gross_notional_5m: float
    net_pnl_5m: float
    commission_5m: float
    gross_notional_15m: float
    net_pnl_15m: float
    commission_15m: float
    competition_gross_notional: float
    competition_net_pnl: float
    competition_commission: float
    long_notional: float
    short_notional: float
    threshold_position_notional: float
    max_long_notional: float
    max_short_notional: float
    actual_net_notional: float
    adaptive_step_raw_scale: float
    volatility_entry_pause_active: bool = False
    volatility_entry_pause_reason: str | None = None
    volatility_10s_abs_return_ratio: float = 0.0
    volatility_10s_amplitude_ratio: float = 0.0
    volatility_1m_amplitude_ratio: float = 0.0
    volatility_5m_amplitude_ratio: float = 0.0
    multi_timeframe_bias_regime: str = "balanced"
    competition_required_pace: float | None = None
    competition_actual_pace: float | None = None
    mid_price: float = 0.0
    tick_size: float = 0.0


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _loss_per_10k(net_pnl: float, gross_notional: float) -> float:
    gross = max(_safe_float(gross_notional), 1e-12)
    loss = max(-_safe_float(net_pnl), 0.0)
    return loss / gross * 10_000.0


def _parse_datetime(value: Any) -> datetime | None:
    if value in {"", None}:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        try:
            dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _inventory_ratio(long_notional: float, short_notional: float, max_long: float, max_short: float) -> float:
    ratios = []
    if max_long > 0:
        ratios.append(max(long_notional, 0.0) / max_long)
    if max_short > 0:
        ratios.append(max(short_notional, 0.0) / max_short)
    return max(ratios or [0.0])


def _strategy_intent_for_regime(regime: str) -> str:
    if regime in {"sprint", "cruise", "ping-pong-fast"}:
        return "volume"
    if regime in {"recover", "ping-pong-safe", "wide-step", "wide-step-attack"}:
        return "recovery"
    if regime in {"defensive", "cooldown"}:
        return "protection"
    if regime == "disabled":
        return "disabled"
    return "unknown"


def _active_state_for_regime(regime: str, repair_ladder_level: str | None = None) -> str:
    if repair_ladder_level in {"touch", "near_cross", "cross"}:
        return "ADVERSE_REPAIR"
    if regime in {"sprint", "ping-pong-fast"}:
        return "PINGPONG_FAST"
    if regime == "cruise":
        return "BQ_NORMAL"
    if regime in {"recover", "ping-pong-safe", "wide-step", "wide-step-attack"}:
        return "RECOVERY_SAFE"
    if regime in {"defensive", "cooldown"}:
        return "PROTECTION"
    if regime == "disabled":
        return "DISABLED"
    return "UNKNOWN"


def _adverse_move(
    *,
    side: str | None,
    reference_price: float,
    mid_price: float,
    tick_size: float,
) -> dict[str, float]:
    if side not in {"SELL", "BUY"} or reference_price <= 0 or mid_price <= 0:
        return {"price": 0.0, "ticks": 0.0, "bps": 0.0}
    if side == "SELL":
        price_move = max(reference_price - mid_price, 0.0)
    else:
        price_move = max(mid_price - reference_price, 0.0)
    return {
        "price": price_move,
        "ticks": price_move / tick_size if tick_size > 0 else 0.0,
        "bps": price_move / reference_price * 10_000.0 if reference_price > 0 else 0.0,
    }


def _resolve_repair_ladder(
    *,
    config: ElasticVolumeConfig,
    inputs: ElasticVolumeInputs,
    regime: str,
    inventory_ratio: float,
    long_notional: float,
    short_notional: float,
    max_long: float,
    max_short: float,
) -> dict[str, Any]:
    side: str | None = None
    inventory_side: str | None = None
    if long_notional >= short_notional and long_notional > 0:
        side = "SELL"
        inventory_side = "long"
        cap_notional = max_long
        current_notional = long_notional
    elif short_notional > 0:
        side = "BUY"
        inventory_side = "short"
        cap_notional = max_short
        current_notional = short_notional
    else:
        cap_notional = 0.0
        current_notional = 0.0

    stale_cycles = max(int(_safe_float((inputs.last_state or {}).get("repair_stale_cycles"))), 0)
    mid_price = max(_safe_float(inputs.mid_price), 0.0)
    reference_price = max(_safe_float((inputs.last_state or {}).get("repair_reference_price")), 0.0)
    if reference_price <= 0:
        reference_price = mid_price
    tick_size = max(_safe_float(inputs.tick_size), 0.0)
    adverse = _adverse_move(
        side=side,
        reference_price=reference_price,
        mid_price=mid_price,
        tick_size=tick_size,
    )
    adverse_by_ticks = (
        config.adverse_move_ticks > 0 and adverse["ticks"] >= max(_safe_float(config.adverse_move_ticks), 0.0)
    )
    adverse_by_bps = config.adverse_move_bps > 0 and adverse["bps"] >= max(_safe_float(config.adverse_move_bps), 0.0)
    adverse_active = adverse_by_ticks or adverse_by_bps
    stale_active = stale_cycles >= max(int(config.repair_stale_cycles), 1)
    near_cross_active = (
        adverse_active
        and stale_active
        and (
            (
                config.adverse_move_ticks > 0
                and adverse["ticks"] >= max(_safe_float(config.adverse_move_ticks), 0.0) * 2.0
            )
            or (
                config.adverse_move_bps > 0
                and adverse["bps"] >= max(_safe_float(config.adverse_move_bps), 0.0) * 2.0
            )
            or (
                config.inventory_hard_ratio > 0
                and inventory_ratio >= max(_safe_float(config.inventory_hard_ratio), 0.0) * 0.85
            )
        )
    )

    if inventory_ratio >= max(_safe_float(config.inventory_hard_ratio), 0.0) > 0:
        level = "cross"
        slice_ratio = _safe_float(config.repair_slice_ratio_cross)
    elif regime == "cooldown":
        level = "cross"
        slice_ratio = _safe_float(config.repair_slice_ratio_cross)
    elif near_cross_active:
        level = "near_cross"
        slice_ratio = _safe_float(config.repair_slice_ratio_near_cross)
    elif adverse_active and stale_active:
        level = "touch"
        slice_ratio = _safe_float(config.repair_slice_ratio_touch)
    else:
        level = "passive"
        slice_ratio = _safe_float(config.repair_slice_ratio_passive)

    exit_notional = max(cap_notional, 0.0) * max(_safe_float(config.inventory_recover_exit_ratio), 0.0)
    recover_gap_notional = max(current_notional - exit_notional, 0.0) if cap_notional > 0 else current_notional
    return {
        "enabled": (
            side is not None
            and (
                regime in {"recover", "cooldown"}
                or inventory_ratio >= max(_safe_float(config.inventory_soft_ratio), 0.0) > 0
            )
        ),
        "level": level if side else "none",
        "side": side,
        "inventory_side": inventory_side,
        "stale_cycles": stale_cycles,
        "stale_threshold": max(int(config.repair_stale_cycles), 1),
        "reference_price": reference_price,
        "mid_price": mid_price,
        "adverse_move_price": adverse["price"],
        "adverse_move_ticks": adverse["ticks"],
        "adverse_move_bps": adverse["bps"],
        "adverse_move_active": adverse_active,
        "slice_ratio": max(min(slice_ratio, 1.0), 0.0),
        "recover_exit_ratio": max(_safe_float(config.inventory_recover_exit_ratio), 0.0),
        "recover_gap_notional": recover_gap_notional,
        "repair_slice_notional": recover_gap_notional * max(min(slice_ratio, 1.0), 0.0),
        "max_repair_loss_per_10k": max(_safe_float(config.max_repair_loss_per_10k), 0.0),
    }


def _allowed_sides_for_control(control: dict[str, Any], repair_ladder: dict[str, Any]) -> list[str]:
    if repair_ladder.get("enabled") and repair_ladder.get("side"):
        return [f"{repair_ladder['side']} reduceOnly"]
    allowed: list[str] = []
    if bool(control.get("entry_allowed")) and bool(control.get("allow_entry_long")):
        allowed.append("BUY entry")
    if bool(control.get("entry_allowed")) and bool(control.get("allow_entry_short")):
        allowed.append("SELL entry")
    if bool(control.get("allow_reduce_long")):
        allowed.append("SELL reduce")
    if bool(control.get("allow_reduce_short")):
        allowed.append("BUY reduce")
    return allowed


def _attach_state_report(
    control: dict[str, Any],
    *,
    config: ElasticVolumeConfig,
    inputs: ElasticVolumeInputs,
    inventory_ratio: float,
    long_notional: float,
    short_notional: float,
    max_long: float,
    max_short: float,
) -> dict[str, Any]:
    regime = str(control.get("regime") or "")
    repair_ladder = _resolve_repair_ladder(
        config=config,
        inputs=inputs,
        regime=regime,
        inventory_ratio=inventory_ratio,
        long_notional=long_notional,
        short_notional=short_notional,
        max_long=max_long,
        max_short=max_short,
    )
    if repair_ladder.get("enabled"):
        level = str(repair_ladder.get("level") or "")
        if level in {"touch", "near_cross"} and "repair_stale_adverse" not in control["reasons"]:
            control["reasons"].append("repair_stale_adverse")
        if level == "near_cross" and "repair_near_cross" not in control["reasons"]:
            control["reasons"].append("repair_near_cross")
        if level == "cross" and "repair_cross" not in control["reasons"]:
            control["reasons"].append("repair_cross")
    control["strategy_intent"] = "repair_inventory" if repair_ladder.get("enabled") else _strategy_intent_for_regime(regime)
    control["repair_ladder"] = repair_ladder
    control["repair_ladder_level"] = repair_ladder.get("level")
    control["active_state"] = _active_state_for_regime(regime, str(repair_ladder.get("level") or ""))
    control["state_reason"] = list(control.get("reasons") or [])
    control["allowed_sides"] = _allowed_sides_for_control(control, repair_ladder)
    control["recover_exit_ratio"] = repair_ladder.get("recover_exit_ratio")
    control["recover_gap_notional"] = repair_ladder.get("recover_gap_notional")
    return control


def _inventory_ratio_against_threshold(long_notional: float, short_notional: float, threshold_notional: float) -> float:
    threshold = max(_safe_float(threshold_notional), 1e-12)
    return max(max(_safe_float(long_notional), 0.0), max(_safe_float(short_notional), 0.0)) / threshold


def _inventory_ratio_against_scaled_threshold(
    long_notional: float,
    short_notional: float,
    threshold_notional: float,
    threshold_scale: float,
) -> float:
    effective_threshold = max(_safe_float(threshold_notional) * max(_safe_float(threshold_scale), 0.0), 1e-12)
    return max(max(_safe_float(long_notional), 0.0), max(_safe_float(short_notional), 0.0)) / effective_threshold


def _base_control(
    *,
    enabled: bool,
    regime: str,
    reasons: list[str] | None = None,
    step_scale: float = 1.0,
    per_order_scale: float = 1.0,
    levels_scale: float = 1.0,
    position_limit_scale: float = 1.0,
    threshold_scale: float = 1.0,
    pause_scale: float = 1.0,
    max_total_scale: float = 1.0,
) -> dict[str, Any]:
    return {
        "enabled": enabled,
        "regime": regime,
        "last_regime": None,
        "reasons": list(reasons or []),
        "step_scale": float(step_scale),
        "per_order_scale": float(per_order_scale),
        "levels_scale": float(levels_scale),
        "position_limit_scale": float(position_limit_scale),
        "threshold_scale": float(threshold_scale),
        "pause_scale": float(pause_scale),
        "max_total_scale": float(max_total_scale),
        "entry_allowed": True,
        "allow_entry_long": True,
        "allow_entry_short": True,
        "allow_reduce_long": True,
        "allow_reduce_short": True,
        "cooldown_until": None,
        "pending_regime": None,
        "pending_count": 0,
        "cancel_stale_entries": False,
        "max_entry_long_orders": None,
        "max_entry_short_orders": None,
        "metrics": {},
    }


def _state_scales(config: ElasticVolumeConfig, regime: str) -> dict[str, float]:
    if regime == "defensive":
        return {
            "step_scale": config.base_step_multiplier_defensive,
            "per_order_scale": config.per_order_scale_defensive_state,
            "levels_scale": config.levels_scale_defensive_state,
            "position_limit_scale": config.threshold_scale_defensive,
            "threshold_scale": config.threshold_scale_defensive,
            "pause_scale": config.pause_scale_defensive,
            "max_total_scale": config.max_total_scale_defensive,
            "max_entry_orders": config.max_entry_orders_defensive,
        }
    if regime == "wide-step-attack":
        return {
            "step_scale": config.base_step_multiplier_wide_step_attack,
            "per_order_scale": config.per_order_scale_wide_step_attack,
            "levels_scale": config.levels_scale_wide_step_attack,
            "position_limit_scale": config.threshold_scale_wide_step_attack,
            "threshold_scale": config.threshold_scale_wide_step_attack,
            "pause_scale": config.pause_scale_wide_step_attack,
            "max_total_scale": config.max_total_scale_wide_step_attack,
            "max_entry_orders": config.max_entry_orders_wide_step_attack,
        }
    if regime == "wide-step":
        return {
            "step_scale": config.base_step_multiplier_wide_step,
            "per_order_scale": config.per_order_scale_wide_step,
            "levels_scale": config.levels_scale_wide_step,
            "position_limit_scale": config.threshold_scale_wide_step,
            "threshold_scale": config.threshold_scale_wide_step,
            "pause_scale": config.pause_scale_wide_step,
            "max_total_scale": config.max_total_scale_wide_step,
            "max_entry_orders": config.max_entry_orders_wide_step,
        }
    if regime == "ping-pong-fast":
        return {
            "step_scale": config.base_step_multiplier_ping_pong_fast,
            "per_order_scale": config.per_order_scale_ping_pong_fast,
            "levels_scale": config.levels_scale_ping_pong_fast,
            "position_limit_scale": 1.0,
            "threshold_scale": config.threshold_scale_ping_pong_fast,
            "pause_scale": config.pause_scale_ping_pong_fast,
            "max_total_scale": config.max_total_scale_ping_pong_fast,
            "max_entry_orders": config.max_entry_orders_ping_pong_fast,
        }
    return {
        "step_scale": config.base_step_multiplier_ping_pong_safe,
        "per_order_scale": config.per_order_scale_ping_pong_safe,
        "levels_scale": config.levels_scale_ping_pong_safe,
        "position_limit_scale": config.threshold_scale_ping_pong_safe,
        "threshold_scale": config.threshold_scale_ping_pong_safe,
        "pause_scale": config.pause_scale_ping_pong_safe,
        "max_total_scale": config.max_total_scale_ping_pong_safe,
        "max_entry_orders": config.max_entry_orders_ping_pong_safe,
    }


def _control_for_state(config: ElasticVolumeConfig, regime: str, reasons: list[str] | None = None) -> dict[str, Any]:
    scales = _state_scales(config, regime)
    max_entry_orders = max(int(scales.pop("max_entry_orders", 0)), 0)
    control = _base_control(enabled=True, regime=regime, reasons=reasons, **scales)
    control["max_entry_long_orders"] = max_entry_orders
    control["max_entry_short_orders"] = max_entry_orders
    return control


def resolve_elastic_volume_control(
    *,
    config: ElasticVolumeConfig,
    inputs: ElasticVolumeInputs,
) -> dict[str, Any]:
    now = inputs.now.astimezone(timezone.utc)
    last_regime = str((inputs.last_state or {}).get("regime") or "").strip() or None
    last_pending_regime = str((inputs.last_state or {}).get("pending_regime") or "").strip() or None
    last_pending_count = int(_safe_float((inputs.last_state or {}).get("pending_count")))
    loss_5m = _loss_per_10k(inputs.net_pnl_5m, inputs.gross_notional_5m)
    loss_15m = _loss_per_10k(inputs.net_pnl_15m, inputs.gross_notional_15m)
    long_notional = max(_safe_float(inputs.long_notional), 0.0)
    short_notional = max(_safe_float(inputs.short_notional), 0.0)
    net = _safe_float(inputs.actual_net_notional)
    directional_long_notional = max(long_notional, net if net > 0 else 0.0)
    directional_short_notional = max(short_notional, -net if net < 0 else 0.0)
    max_long = max(_safe_float(inputs.max_long_notional), 0.0)
    max_short = max(_safe_float(inputs.max_short_notional), 0.0)
    inventory_ratio = _inventory_ratio(directional_long_notional, directional_short_notional, max_long, max_short)
    threshold_inventory_ratio = _inventory_ratio_against_threshold(
        directional_long_notional,
        directional_short_notional,
        _safe_float(inputs.threshold_position_notional),
    )
    fast_threshold_inventory_ratio = _inventory_ratio_against_scaled_threshold(
        long_notional,
        short_notional,
        _safe_float(inputs.threshold_position_notional),
        config.threshold_scale_ping_pong_fast,
    )
    safe_threshold_inventory_ratio = _inventory_ratio_against_scaled_threshold(
        long_notional,
        short_notional,
        _safe_float(inputs.threshold_position_notional),
        config.threshold_scale_ping_pong_safe,
    )
    wide_attack_threshold_inventory_ratio = _inventory_ratio_against_scaled_threshold(
        long_notional,
        short_notional,
        _safe_float(inputs.threshold_position_notional),
        config.threshold_scale_wide_step_attack,
    )
    wide_threshold_inventory_ratio = _inventory_ratio_against_scaled_threshold(
        long_notional,
        short_notional,
        _safe_float(inputs.threshold_position_notional),
        config.threshold_scale_wide_step,
    )
    raw_scale = max(_safe_float(inputs.adaptive_step_raw_scale), 0.0)
    abs_return_10s = max(_safe_float(inputs.volatility_10s_abs_return_ratio), 0.0)
    amp_10s = max(_safe_float(inputs.volatility_10s_amplitude_ratio), 0.0)
    amp_1m = max(_safe_float(inputs.volatility_1m_amplitude_ratio), 0.0)
    amp_5m = max(_safe_float(inputs.volatility_5m_amplitude_ratio), 0.0)
    entry_pause_active = bool(inputs.volatility_entry_pause_active)
    entry_pause_reason = str(inputs.volatility_entry_pause_reason or "").strip()
    bias_regime = str(inputs.multi_timeframe_bias_regime or "balanced")
    early_same_side_entry_gate = False

    if not config.enabled:
        control = _base_control(enabled=False, regime="disabled")
        control["last_regime"] = last_regime
        control["metrics"] = {
            "loss_per_10k_15m": loss_15m,
            "loss_per_10k_5m": loss_5m,
            "inventory_ratio": inventory_ratio,
            "threshold_inventory_ratio": threshold_inventory_ratio,
            "adaptive_step_raw_scale": raw_scale,
            "volatility_entry_pause_active": entry_pause_active,
            "volatility_entry_pause_reason": entry_pause_reason or None,
            "volatility_10s_abs_return_ratio": abs_return_10s,
            "volatility_10s_amplitude_ratio": amp_10s,
            "volatility_1m_amplitude_ratio": amp_1m,
            "volatility_5m_amplitude_ratio": amp_5m,
        }
        return _attach_state_report(
            control,
            config=config,
            inputs=inputs,
            inventory_ratio=inventory_ratio,
            long_notional=long_notional,
            short_notional=short_notional,
            max_long=max_long,
            max_short=max_short,
        )

    cooldown_until = _parse_datetime((inputs.last_state or {}).get("cooldown_until"))
    if last_regime == "cooldown" and cooldown_until and cooldown_until > now:
        control = _base_control(
            enabled=True,
            regime="cooldown",
            reasons=["cooldown_active"],
            step_scale=config.step_scale_cooldown,
            per_order_scale=config.per_order_scale_defensive,
            levels_scale=config.levels_scale_defensive,
            position_limit_scale=config.levels_scale_defensive,
            threshold_scale=config.threshold_scale_defensive,
            pause_scale=config.pause_scale_defensive,
            max_total_scale=config.max_total_scale_defensive,
        )
        control["entry_allowed"] = False
        control["allow_entry_long"] = False
        control["allow_entry_short"] = False
        control["cancel_stale_entries"] = bool(config.cancel_stale_entries_on_cooldown)
        control["cooldown_until"] = cooldown_until.isoformat()
    else:
        cap_pressure = inventory_ratio >= 0.9
        fast_soft = fast_threshold_inventory_ratio >= max(_safe_float(config.inventory_soft_ratio_ping_pong_fast), 0.0)
        fast_hard = fast_threshold_inventory_ratio >= max(_safe_float(config.inventory_hard_ratio_ping_pong_fast), 0.0)
        safe_soft = safe_threshold_inventory_ratio >= max(_safe_float(config.inventory_soft_ratio_ping_pong_safe), 0.0)
        safe_hard = safe_threshold_inventory_ratio >= max(_safe_float(config.inventory_hard_ratio_ping_pong_safe), 0.0)
        wide_attack_soft = max(_safe_float(config.inventory_soft_ratio_wide_step_attack), 0.0)
        wide_soft = max(_safe_float(config.inventory_soft_ratio_wide_step), 0.0)
        wide_hard = wide_threshold_inventory_ratio >= max(_safe_float(config.inventory_hard_ratio_wide_step), 0.0)
        early_micro_volatility = (
            abs_return_10s >= max(_safe_float(config.early_micro_abs_return_ratio), 0.0) > 0
            or amp_10s >= max(_safe_float(config.early_micro_amplitude_ratio), 0.0) > 0
        )
        early_volatility_active = entry_pause_active or early_micro_volatility
        early_safe = early_volatility_active and inventory_ratio >= max(_safe_float(config.early_safe_inventory_ratio), 0.0)
        early_wide_inventory = early_volatility_active and inventory_ratio >= max(_safe_float(config.early_wide_inventory_ratio), 0.0)
        early_wide_loss = early_volatility_active and loss_5m >= max(_safe_float(config.early_wide_loss_per_10k_5m), 0.0)
        early_same_side_entry_gate = early_safe or early_wide_inventory or early_wide_loss

        if amp_1m > 0.018 or amp_5m > 0.035 or wide_hard:
            reasons = ["extreme_volatility"] if (amp_1m > 0.018 or amp_5m > 0.035) else ["inventory_hard"]
            control = _control_for_state(config, "defensive", reasons)
        elif early_wide_inventory or early_wide_loss:
            reasons = []
            if entry_pause_active:
                reasons.append("volatility_entry_pause")
            if early_micro_volatility:
                reasons.append("early_micro_volatility")
            if early_wide_inventory:
                reasons.append("early_inventory_wide")
            if early_wide_loss:
                reasons.append("early_loss_wide")
            control = _control_for_state(config, "wide-step", reasons or ["early_wide"])
        elif early_safe:
            reasons = []
            if entry_pause_active:
                reasons.append("volatility_entry_pause")
            if early_micro_volatility:
                reasons.append("early_micro_volatility")
            reasons.append("early_inventory_safe")
            control = _control_for_state(config, "ping-pong-safe", reasons)
        elif amp_1m > 0.009 or amp_5m > 0.018 or fast_hard or safe_hard or cap_pressure:
            reasons: list[str] = []
            if amp_1m > 0.009 or amp_5m > 0.018:
                reasons.append("volatility")
            if safe_hard or wide_threshold_inventory_ratio >= wide_soft:
                reasons.append("inventory_hard")
            if cap_pressure:
                reasons.append("cap_pressure")
            if threshold_inventory_ratio < wide_attack_soft and not cap_pressure:
                control = _control_for_state(config, "wide-step-attack", reasons or ["inventory_attack"])
            else:
                control = _control_for_state(config, "wide-step", reasons or ["inventory_soft"])
        elif amp_1m < 0.0025 and amp_5m < 0.006 and not fast_soft and not fast_hard:
            control = _control_for_state(config, "ping-pong-fast", ["low_volatility", "light_inventory"])
        else:
            reasons = []
            if amp_1m > 0.0045 or amp_5m > 0.009:
                reasons.append("moderate_volatility")
            if fast_soft or safe_soft:
                reasons.append("inventory_soft")
            control = _control_for_state(config, "ping-pong-safe", reasons or ["balanced"])

        immediate_regimes = {"defensive", "wide-step", "wide-step-attack", "ping-pong-safe"}
        recovery_regimes = {"ping-pong-safe", "ping-pong-fast"}
        if last_regime in immediate_regimes and control["regime"] in recovery_regimes:
            candidate_regime = str(control["regime"])
            pending_count = last_pending_count + 1 if last_pending_regime == control["regime"] else 1
            if pending_count < max(int(config.state_confirm_cycles), 1):
                held_regime = last_regime
                control = _control_for_state(config, held_regime, ["recovery_confirming"])
                control["pending_regime"] = candidate_regime
                control["pending_count"] = pending_count
            else:
                control["pending_regime"] = None
                control["pending_count"] = 0
        elif control["regime"] in immediate_regimes or control["regime"] != last_pending_regime:
            control["pending_regime"] = None
            control["pending_count"] = 0

    control["last_regime"] = last_regime

    if control["regime"] in {"defensive", "wide-step", "wide-step-attack"}:
        same_side_risk_pressure = (
            control["regime"] == "defensive"
            or cap_pressure
            or threshold_inventory_ratio >= max(_safe_float(config.inventory_soft_ratio_ping_pong_fast), 0.0)
        )

        def clamp_same_side_risk_limits() -> None:
            control["position_limit_scale"] = min(_safe_float(control.get("position_limit_scale")), 1.0)
            control["threshold_scale"] = min(_safe_float(control.get("threshold_scale")), 1.0)
            control["pause_scale"] = min(_safe_float(control.get("pause_scale")), 1.0)
            if "same_side_risk_limit_clamp" not in control["reasons"]:
                control["reasons"].append("same_side_risk_limit_clamp")

        if directional_long_notional >= directional_short_notional and directional_long_notional > 0:
            control["allow_entry_long"] = False
            control["allow_reduce_long"] = True
            if same_side_risk_pressure:
                clamp_same_side_risk_limits()
            if control["regime"] in {"wide-step", "wide-step-attack"}:
                control["allow_entry_short"] = True
        elif directional_short_notional > 0:
            control["allow_entry_short"] = False
            control["allow_reduce_short"] = True
            if same_side_risk_pressure:
                clamp_same_side_risk_limits()
            if control["regime"] in {"wide-step", "wide-step-attack"}:
                control["allow_entry_long"] = True
        elif net > 0:
            control["allow_entry_long"] = False
            if same_side_risk_pressure:
                clamp_same_side_risk_limits()
        elif net < 0:
            control["allow_entry_short"] = False
            if same_side_risk_pressure:
                clamp_same_side_risk_limits()
    elif control["regime"] in {"ping-pong-fast", "ping-pong-safe"}:
        soft_ratio = (
            max(_safe_float(config.inventory_soft_ratio_ping_pong_fast), 0.0)
            if control["regime"] == "ping-pong-fast"
            else max(_safe_float(config.inventory_soft_ratio_ping_pong_safe), 0.0)
        )
        if threshold_inventory_ratio >= soft_ratio or early_same_side_entry_gate:
            if directional_long_notional >= directional_short_notional and directional_long_notional > 0:
                control["allow_entry_long"] = False
                control["reasons"].append("ping_pong_inventory_entry_gate")
            elif directional_short_notional > 0:
                control["allow_entry_short"] = False
                control["reasons"].append("ping_pong_inventory_entry_gate")
            elif net > 0:
                control["allow_entry_long"] = False
                control["reasons"].append("ping_pong_inventory_entry_gate")
            elif net < 0:
                control["allow_entry_short"] = False
                control["reasons"].append("ping_pong_inventory_entry_gate")

    if control["regime"] in {"wide-step", "wide-step-attack"} and (
        (bias_regime == "low_long_bias" and not control["allow_entry_long"])
        or (bias_regime == "high_short_bias" and not control["allow_entry_short"])
    ):
        control["reasons"].append("inventory_over_bias")

    control["metrics"] = {
        "loss_per_10k_15m": loss_15m,
        "loss_per_10k_5m": loss_5m,
        "gross_notional_5m": _safe_float(inputs.gross_notional_5m),
        "net_pnl_5m": _safe_float(inputs.net_pnl_5m),
        "commission_5m": _safe_float(inputs.commission_5m),
        "gross_notional_15m": _safe_float(inputs.gross_notional_15m),
        "net_pnl_15m": _safe_float(inputs.net_pnl_15m),
        "commission_15m": _safe_float(inputs.commission_15m),
        "competition_gross_notional": _safe_float(inputs.competition_gross_notional),
        "competition_net_pnl": _safe_float(inputs.competition_net_pnl),
        "competition_commission": _safe_float(inputs.competition_commission),
        "long_notional": long_notional,
        "short_notional": short_notional,
        "actual_net_notional": net,
        "directional_long_notional": directional_long_notional,
        "directional_short_notional": directional_short_notional,
        "inventory_ratio": inventory_ratio,
        "threshold_inventory_ratio": threshold_inventory_ratio,
        "wide_attack_threshold_inventory_ratio": wide_attack_threshold_inventory_ratio,
        "adaptive_step_raw_scale": raw_scale,
        "volatility_entry_pause_active": entry_pause_active,
        "volatility_entry_pause_reason": entry_pause_reason or None,
        "volatility_10s_abs_return_ratio": abs_return_10s,
        "volatility_10s_amplitude_ratio": amp_10s,
        "volatility_1m_amplitude_ratio": amp_1m,
        "volatility_5m_amplitude_ratio": amp_5m,
        "multi_timeframe_bias_regime": bias_regime,
        "competition_required_pace": (
            _safe_float(inputs.competition_required_pace)
            if inputs.competition_required_pace is not None
            else None
        ),
        "competition_actual_pace": (
            _safe_float(inputs.competition_actual_pace)
            if inputs.competition_actual_pace is not None
            else None
        ),
    }
    return _attach_state_report(
        control,
        config=config,
        inputs=inputs,
        inventory_ratio=inventory_ratio,
        long_notional=long_notional,
        short_notional=short_notional,
        max_long=max_long,
        max_short=max_short,
    )
