from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


REGIME_RANGE = "range_ping_pong"
REGIME_DOWN = "down_short_bias"
REGIME_UP = "up_long_bias"
REGIME_NO_TRADE = "no_trade"


@dataclass(frozen=True)
class AdaptiveRegimeRouterConfig:
    enabled: bool = False
    mode: str = "profit_adaptive_v1"
    confirm_cycles: int = 2
    min_dwell_seconds: float = 120.0
    max_spread_bps: float = 22.0
    min_depth_notional: float = 0.0
    shock_1m_abs_return_ratio: float = 0.018
    shock_1m_amplitude_ratio: float = 0.028
    shock_5m_amplitude_ratio: float = 0.055
    down_1m_return_ratio: float = -0.004
    down_5m_return_ratio: float = -0.010
    up_1m_return_ratio: float = 0.006
    up_5m_return_ratio: float = 0.014
    range_5m_abs_return_ratio: float = 0.006
    range_5m_amplitude_ratio: float = 0.018
    range_step_scale: float = 1.0
    range_per_order_scale: float = 1.0
    range_levels_scale: float = 1.0
    range_position_limit_scale: float = 1.0
    range_max_entry_orders: int = 4
    down_step_scale: float = 1.8
    down_per_order_scale: float = 0.55
    down_levels_scale: float = 0.65
    down_position_limit_scale: float = 0.70
    down_max_entry_long_orders: int = 0
    down_max_entry_short_orders: int = 2
    up_step_scale: float = 1.5
    up_per_order_scale: float = 0.75
    up_levels_scale: float = 0.75
    up_position_limit_scale: float = 0.85
    up_max_entry_long_orders: int = 2
    up_max_entry_short_orders: int = 0
    no_trade_step_scale: float = 2.5
    no_trade_per_order_scale: float = 0.35
    no_trade_levels_scale: float = 0.50
    no_trade_position_limit_scale: float = 0.60
    cancel_stale_entries_on_no_trade: bool = True


@dataclass(frozen=True)
class AdaptiveRegimeRouterInputs:
    now: datetime
    last_state: dict[str, Any]
    return_1m_ratio: float = 0.0
    amplitude_1m_ratio: float = 0.0
    return_5m_ratio: float = 0.0
    amplitude_5m_ratio: float = 0.0
    spread_bps: float = 0.0
    depth_notional: float | None = None
    long_notional: float = 0.0
    short_notional: float = 0.0
    actual_net_notional: float = 0.0


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _safe_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


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


def _base_control(*, enabled: bool, regime: str, reasons: list[str] | None = None) -> dict[str, Any]:
    return {
        "enabled": enabled,
        "regime": regime,
        "candidate_regime": regime,
        "last_regime": None,
        "pending_regime": None,
        "pending_count": 0,
        "confirm_cycles": 0,
        "min_dwell_seconds": 0.0,
        "last_switched_at": None,
        "switched": False,
        "reasons": list(reasons or []),
        "entry_allowed": True,
        "allow_entry_long": True,
        "allow_entry_short": True,
        "step_scale": 1.0,
        "per_order_scale": 1.0,
        "levels_scale": 1.0,
        "position_limit_scale": 1.0,
        "pause_scale": 1.0,
        "max_total_scale": 1.0,
        "max_entry_long_orders": None,
        "max_entry_short_orders": None,
        "cancel_stale_entries": False,
        "metrics": {},
    }


def _scales_for_regime(config: AdaptiveRegimeRouterConfig, regime: str) -> dict[str, Any]:
    if regime == REGIME_DOWN:
        return {
            "step_scale": config.down_step_scale,
            "per_order_scale": config.down_per_order_scale,
            "levels_scale": config.down_levels_scale,
            "position_limit_scale": config.down_position_limit_scale,
            "pause_scale": config.down_position_limit_scale,
            "max_total_scale": config.down_position_limit_scale,
            "allow_entry_long": False,
            "allow_entry_short": True,
            "max_entry_long_orders": config.down_max_entry_long_orders,
            "max_entry_short_orders": config.down_max_entry_short_orders,
        }
    if regime == REGIME_UP:
        return {
            "step_scale": config.up_step_scale,
            "per_order_scale": config.up_per_order_scale,
            "levels_scale": config.up_levels_scale,
            "position_limit_scale": config.up_position_limit_scale,
            "pause_scale": config.up_position_limit_scale,
            "max_total_scale": config.up_position_limit_scale,
            "allow_entry_long": True,
            "allow_entry_short": False,
            "max_entry_long_orders": config.up_max_entry_long_orders,
            "max_entry_short_orders": config.up_max_entry_short_orders,
        }
    if regime == REGIME_NO_TRADE:
        return {
            "step_scale": config.no_trade_step_scale,
            "per_order_scale": config.no_trade_per_order_scale,
            "levels_scale": config.no_trade_levels_scale,
            "position_limit_scale": config.no_trade_position_limit_scale,
            "pause_scale": config.no_trade_position_limit_scale,
            "max_total_scale": config.no_trade_position_limit_scale,
            "entry_allowed": False,
            "allow_entry_long": False,
            "allow_entry_short": False,
            "max_entry_long_orders": 0,
            "max_entry_short_orders": 0,
            "cancel_stale_entries": config.cancel_stale_entries_on_no_trade,
        }
    return {
        "step_scale": config.range_step_scale,
        "per_order_scale": config.range_per_order_scale,
        "levels_scale": config.range_levels_scale,
        "position_limit_scale": config.range_position_limit_scale,
        "pause_scale": config.range_position_limit_scale,
        "max_total_scale": config.range_position_limit_scale,
        "allow_entry_long": True,
        "allow_entry_short": True,
        "max_entry_long_orders": config.range_max_entry_orders,
        "max_entry_short_orders": config.range_max_entry_orders,
    }


def _classify_regime(
    *,
    config: AdaptiveRegimeRouterConfig,
    inputs: AdaptiveRegimeRouterInputs,
) -> tuple[str, list[str]]:
    reasons: list[str] = []
    ret_1m = _safe_float(inputs.return_1m_ratio)
    amp_1m = max(_safe_float(inputs.amplitude_1m_ratio), 0.0)
    ret_5m = _safe_float(inputs.return_5m_ratio)
    amp_5m = max(_safe_float(inputs.amplitude_5m_ratio), 0.0)
    spread_bps = max(_safe_float(inputs.spread_bps), 0.0)
    depth_notional = inputs.depth_notional

    if spread_bps >= max(_safe_float(config.max_spread_bps), 0.0) > 0:
        return REGIME_NO_TRADE, [f"spread_bps {spread_bps:.2f} >= {config.max_spread_bps:.2f}"]
    if (
        depth_notional is not None
        and max(_safe_float(config.min_depth_notional), 0.0) > 0
        and _safe_float(depth_notional) < _safe_float(config.min_depth_notional)
    ):
        return REGIME_NO_TRADE, [
            f"depth_notional {_safe_float(depth_notional):.2f} < {config.min_depth_notional:.2f}"
        ]
    if abs(ret_1m) >= max(_safe_float(config.shock_1m_abs_return_ratio), 0.0) > 0:
        return REGIME_NO_TRADE, [
            f"shock_1m_return {ret_1m:+.4%} >= {config.shock_1m_abs_return_ratio:.4%}"
        ]
    if amp_1m >= max(_safe_float(config.shock_1m_amplitude_ratio), 0.0) > 0:
        return REGIME_NO_TRADE, [
            f"shock_1m_amplitude {amp_1m:.4%} >= {config.shock_1m_amplitude_ratio:.4%}"
        ]
    if amp_5m >= max(_safe_float(config.shock_5m_amplitude_ratio), 0.0) > 0:
        return REGIME_NO_TRADE, [
            f"shock_5m_amplitude {amp_5m:.4%} >= {config.shock_5m_amplitude_ratio:.4%}"
        ]

    down = ret_5m <= _safe_float(config.down_5m_return_ratio) or (
        ret_1m <= _safe_float(config.down_1m_return_ratio) and ret_5m < 0
    )
    up = ret_5m >= _safe_float(config.up_5m_return_ratio) or ret_1m >= _safe_float(config.up_1m_return_ratio)
    if down and not up:
        reasons.append(f"downtrend ret_1m={ret_1m:+.4%} ret_5m={ret_5m:+.4%}")
        return REGIME_DOWN, reasons
    if up and not down:
        reasons.append(f"uptrend ret_1m={ret_1m:+.4%} ret_5m={ret_5m:+.4%}")
        return REGIME_UP, reasons

    if abs(ret_5m) <= _safe_float(config.range_5m_abs_return_ratio) and amp_5m <= _safe_float(
        config.range_5m_amplitude_ratio
    ):
        reasons.append(f"range ret_5m={ret_5m:+.4%} amp_5m={amp_5m:.4%}")
    else:
        reasons.append(f"mixed ret_1m={ret_1m:+.4%} ret_5m={ret_5m:+.4%} amp_5m={amp_5m:.4%}")
    return REGIME_RANGE, reasons


def resolve_adaptive_regime_router_control(
    *,
    config: AdaptiveRegimeRouterConfig,
    inputs: AdaptiveRegimeRouterInputs,
) -> dict[str, Any]:
    now = inputs.now.astimezone(timezone.utc)
    metrics = {
        "return_1m_ratio": _safe_float(inputs.return_1m_ratio),
        "amplitude_1m_ratio": _safe_float(inputs.amplitude_1m_ratio),
        "return_5m_ratio": _safe_float(inputs.return_5m_ratio),
        "amplitude_5m_ratio": _safe_float(inputs.amplitude_5m_ratio),
        "spread_bps": _safe_float(inputs.spread_bps),
        "depth_notional": None if inputs.depth_notional is None else _safe_float(inputs.depth_notional),
        "long_notional": _safe_float(inputs.long_notional),
        "short_notional": _safe_float(inputs.short_notional),
        "actual_net_notional": _safe_float(inputs.actual_net_notional),
    }
    if not config.enabled:
        control = _base_control(enabled=False, regime="disabled")
        control["metrics"] = metrics
        return control

    last_state = inputs.last_state if isinstance(inputs.last_state, dict) else {}
    last_regime = str(last_state.get("regime") or REGIME_RANGE).strip() or REGIME_RANGE
    pending_regime = str(last_state.get("pending_regime") or "").strip() or None
    pending_count = _safe_int(last_state.get("pending_count"))
    last_switched_at = _parse_datetime(last_state.get("last_switched_at"))
    candidate, reasons = _classify_regime(config=config, inputs=inputs)
    active = last_regime
    switched = False
    min_dwell = max(_safe_float(config.min_dwell_seconds), 0.0)
    confirm_cycles = max(_safe_int(config.confirm_cycles), 1)

    if candidate == REGIME_NO_TRADE:
        active = candidate
        pending_regime = None
        pending_count = 0
        switched = active != last_regime
    elif candidate != last_regime:
        dwell_ok = True
        if last_switched_at is not None and min_dwell > 0:
            dwell_ok = (now - last_switched_at).total_seconds() >= min_dwell
        if not dwell_ok:
            active = last_regime
            reasons.append("min_dwell_holding")
        else:
            if pending_regime == candidate:
                pending_count += 1
            else:
                pending_regime = candidate
                pending_count = 1
            if pending_count >= confirm_cycles:
                active = candidate
                pending_regime = None
                pending_count = 0
                switched = True
            else:
                active = last_regime
                reasons.append("confirming")
    else:
        pending_regime = None
        pending_count = 0

    control = _base_control(enabled=True, regime=active, reasons=reasons)
    control["candidate_regime"] = candidate
    control["last_regime"] = last_regime
    control["pending_regime"] = pending_regime
    control["pending_count"] = pending_count
    control["confirm_cycles"] = confirm_cycles
    control["min_dwell_seconds"] = min_dwell
    control["last_switched_at"] = now.isoformat() if switched else (last_state.get("last_switched_at") or None)
    control["switched"] = switched
    control["metrics"] = metrics
    control.update(_scales_for_regime(config, active))
    return control


def adaptive_regime_router_state_snapshot(control: dict[str, Any], *, updated_at: str) -> dict[str, Any]:
    return {
        "regime": control.get("regime"),
        "candidate_regime": control.get("candidate_regime"),
        "pending_regime": control.get("pending_regime"),
        "pending_count": _safe_int(control.get("pending_count")),
        "last_switched_at": control.get("last_switched_at"),
        "updated_at": updated_at,
    }
