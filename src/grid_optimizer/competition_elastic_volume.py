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
    inventory_soft_ratio_wide_step: float = 0.60
    inventory_hard_ratio_wide_step: float = 0.80
    adaptive_raw_scale_defensive: float = 1.2
    adaptive_raw_scale_cooldown: float = 2.0
    step_scale_sprint: float = 0.8
    step_scale_defensive: float = 1.8
    step_scale_cooldown: float = 3.0
    base_step_multiplier_ping_pong_fast: float = 0.8
    base_step_multiplier_ping_pong_safe: float = 1.2
    base_step_multiplier_wide_step: float = 2.5
    base_step_multiplier_defensive: float = 3.5
    per_order_scale_sprint: float = 1.25
    per_order_scale_defensive: float = 0.65
    per_order_scale_ping_pong_fast: float = 1.0
    per_order_scale_ping_pong_safe: float = 0.9
    per_order_scale_wide_step: float = 0.6
    per_order_scale_defensive_state: float = 0.4
    levels_scale_sprint: float = 1.25
    levels_scale_defensive: float = 0.65
    levels_scale_ping_pong_fast: float = 1.0
    levels_scale_ping_pong_safe: float = 0.85
    levels_scale_wide_step: float = 0.65
    levels_scale_defensive_state: float = 0.35
    threshold_scale_ping_pong_fast: float = 1.0
    threshold_scale_ping_pong_safe: float = 1.1
    threshold_scale_wide_step: float = 1.5
    threshold_scale_defensive: float = 1.8
    pause_scale_ping_pong_fast: float = 1.0
    pause_scale_ping_pong_safe: float = 1.1
    pause_scale_wide_step: float = 1.5
    pause_scale_defensive: float = 1.8
    max_total_scale_ping_pong_fast: float = 1.0
    max_total_scale_ping_pong_safe: float = 1.1
    max_total_scale_wide_step: float = 1.35
    max_total_scale_defensive: float = 1.6
    max_entry_orders_ping_pong_fast: int = 6
    max_entry_orders_ping_pong_safe: int = 4
    max_entry_orders_wide_step: int = 3
    max_entry_orders_defensive: int = 2
    cooldown_seconds: float = 120.0
    state_confirm_cycles: int = 3
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
    volatility_1m_amplitude_ratio: float = 0.0
    volatility_5m_amplitude_ratio: float = 0.0
    multi_timeframe_bias_regime: str = "balanced"
    competition_required_pace: float | None = None
    competition_actual_pace: float | None = None


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


def _inventory_ratio_against_threshold(long_notional: float, short_notional: float, threshold_notional: float) -> float:
    threshold = max(_safe_float(threshold_notional), 1e-12)
    return max(max(_safe_float(long_notional), 0.0), max(_safe_float(short_notional), 0.0)) / threshold


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
    raw_scale = max(_safe_float(inputs.adaptive_step_raw_scale), 0.0)
    amp_1m = max(_safe_float(inputs.volatility_1m_amplitude_ratio), 0.0)
    amp_5m = max(_safe_float(inputs.volatility_5m_amplitude_ratio), 0.0)
    bias_regime = str(inputs.multi_timeframe_bias_regime or "balanced")

    if not config.enabled:
        control = _base_control(enabled=False, regime="disabled")
        control["last_regime"] = last_regime
        control["metrics"] = {
            "loss_per_10k_15m": loss_15m,
            "loss_per_10k_5m": loss_5m,
            "inventory_ratio": inventory_ratio,
            "threshold_inventory_ratio": threshold_inventory_ratio,
            "adaptive_step_raw_scale": raw_scale,
            "volatility_1m_amplitude_ratio": amp_1m,
            "volatility_5m_amplitude_ratio": amp_5m,
        }
        return control

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
        fast_soft = threshold_inventory_ratio >= max(_safe_float(config.inventory_soft_ratio_ping_pong_fast), 0.0)
        fast_hard = threshold_inventory_ratio >= max(_safe_float(config.inventory_hard_ratio_ping_pong_fast), 0.0)
        safe_soft = threshold_inventory_ratio >= max(_safe_float(config.inventory_soft_ratio_ping_pong_safe), 0.0)
        safe_hard = threshold_inventory_ratio >= max(_safe_float(config.inventory_hard_ratio_ping_pong_safe), 0.0)
        wide_hard = threshold_inventory_ratio >= max(_safe_float(config.inventory_hard_ratio_wide_step), 0.0)

        if amp_1m > 0.018 or amp_5m > 0.035 or wide_hard:
            reasons = ["extreme_volatility"] if (amp_1m > 0.018 or amp_5m > 0.035) else ["inventory_hard"]
            control = _control_for_state(config, "defensive", reasons)
        elif amp_1m > 0.009 or amp_5m > 0.018 or fast_hard or safe_hard or cap_pressure:
            reasons: list[str] = []
            if amp_1m > 0.009 or amp_5m > 0.018:
                reasons.append("volatility")
            if safe_hard:
                reasons.append("inventory_hard")
            if cap_pressure:
                reasons.append("cap_pressure")
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

        immediate_regimes = {"defensive", "wide-step", "ping-pong-safe"}
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

    if control["regime"] in {"defensive", "wide-step"}:
        if directional_long_notional >= directional_short_notional and directional_long_notional > 0:
            control["allow_entry_long"] = False
            control["allow_reduce_long"] = True
            if control["regime"] == "wide-step":
                control["allow_entry_short"] = True
        elif directional_short_notional > 0:
            control["allow_entry_short"] = False
            control["allow_reduce_short"] = True
            if control["regime"] == "wide-step":
                control["allow_entry_long"] = True
        elif net > 0:
            control["allow_entry_long"] = False
        elif net < 0:
            control["allow_entry_short"] = False
    elif control["regime"] in {"ping-pong-fast", "ping-pong-safe"}:
        soft_ratio = (
            max(_safe_float(config.inventory_soft_ratio_ping_pong_fast), 0.0)
            if control["regime"] == "ping-pong-fast"
            else max(_safe_float(config.inventory_soft_ratio_ping_pong_safe), 0.0)
        )
        if threshold_inventory_ratio >= soft_ratio:
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

    if control["regime"] == "wide-step" and (
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
        "adaptive_step_raw_scale": raw_scale,
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
    return control
