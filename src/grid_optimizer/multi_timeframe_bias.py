from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class MultiTimeframeBiasConfig:
    enabled: bool = False
    low_zone_threshold: float = 0.35
    high_zone_threshold: float = 0.65
    strong_bias_threshold: float = 0.50
    max_level_delta: int = 4
    max_offset_steps: float = 1.0
    favored_position_scale: float = 1.25
    unfavored_position_scale: float = 0.75
    shock_abs_return_ratio: float = 0.018
    shock_amplitude_ratio: float = 0.025
    shock_step_scale: float = 1.5
    shock_notional_scale: float = 0.70
    scheduler_enabled: bool = True
    scheduler_zone_weight: float = 0.60
    scheduler_trend_weight: float = 0.25
    scheduler_inventory_weight: float = 0.55
    scheduler_min_budget_scale: float = 0.20
    scheduler_max_budget_scale: float = 1.15
    scheduler_inventory_skew_threshold: float = 0.70
    scheduler_max_offset_steps: float = 2.0
    scheduler_defensive_notional_scale: float = 0.60
    scheduler_defensive_step_scale: float = 1.35
    scheduler_reduce_max_order_scale: float = 0.65


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _clamp(value: float, lower: float, upper: float) -> float:
    return min(max(value, lower), upper)


def _latest_window(candles: list[Any]) -> dict[str, float] | None:
    if not candles:
        return None
    candle = candles[-1]
    open_price = max(_safe_float(getattr(candle, "open", 0.0)), 0.0)
    high_price = max(_safe_float(getattr(candle, "high", 0.0)), 0.0)
    low_price = max(_safe_float(getattr(candle, "low", 0.0)), 0.0)
    close_price = max(_safe_float(getattr(candle, "close", 0.0)), 0.0)
    if open_price <= 0 or high_price <= 0 or low_price <= 0 or close_price <= 0 or high_price < low_price:
        return None
    return {
        "open": open_price,
        "high": high_price,
        "low": low_price,
        "close": close_price,
        "return_ratio": (close_price / open_price) - 1.0,
        "amplitude_ratio": (high_price / low_price) - 1.0,
        "zone": _clamp((close_price - low_price) / (high_price - low_price), 0.0, 1.0)
        if high_price > low_price
        else 0.5,
    }


def resolve_multi_timeframe_bias(
    *,
    current_price: float,
    step_price: float,
    windows: dict[str, list[Any]],
    config: MultiTimeframeBiasConfig,
) -> dict[str, Any]:
    metrics = {name: _latest_window(list(windows.get(name) or [])) for name in ("1m", "15m", "1h", "4h")}
    available = bool(config.enabled) and all(metrics.values())
    report: dict[str, Any] = {
        "enabled": bool(config.enabled),
        "available": available,
        "regime": "disabled" if not config.enabled else "unavailable",
        "long_bias_score": 0.0,
        "short_bias_score": 0.0,
        "direction_score": 0.0,
        "zone_score": 0.5,
        "shock_active": False,
        "shock_reason": None,
        "metrics": metrics,
        "step_price": max(_safe_float(step_price), 0.0),
        "current_price": max(_safe_float(current_price), 0.0),
    }
    if not available:
        return report

    one_min = metrics["1m"] or {}
    fifteen = metrics["15m"] or {}
    one_hour = metrics["1h"] or {}
    four_hour = metrics["4h"] or {}
    zone_score = (
        _safe_float(fifteen.get("zone")) * 0.35
        + _safe_float(one_hour.get("zone")) * 0.25
        + _safe_float(four_hour.get("zone")) * 0.40
    )
    trend_score = _clamp(
        (_safe_float(fifteen.get("return_ratio")) * 35.0)
        + (_safe_float(one_hour.get("return_ratio")) * 20.0)
        + (_safe_float(four_hour.get("return_ratio")) * 10.0),
        -1.0,
        1.0,
    )
    low_component = _clamp((config.low_zone_threshold - zone_score) / max(config.low_zone_threshold, 1e-12), 0.0, 1.0)
    high_component = _clamp(
        (zone_score - config.high_zone_threshold) / max(1.0 - config.high_zone_threshold, 1e-12),
        0.0,
        1.0,
    )
    long_bias_score = _clamp((low_component * 0.75) + (_clamp(-trend_score, 0.0, 1.0) * 0.25), 0.0, 1.0)
    short_bias_score = _clamp((high_component * 0.75) + (_clamp(trend_score, 0.0, 1.0) * 0.25), 0.0, 1.0)
    shock_active = (
        abs(_safe_float(one_min.get("return_ratio"))) >= config.shock_abs_return_ratio
        or _safe_float(one_min.get("amplitude_ratio")) >= config.shock_amplitude_ratio
    )

    if long_bias_score >= config.strong_bias_threshold and long_bias_score >= short_bias_score:
        regime = "low_long_bias"
    elif short_bias_score >= config.strong_bias_threshold:
        regime = "high_short_bias"
    else:
        regime = "balanced"
    report.update(
        {
            "regime": regime,
            "long_bias_score": long_bias_score,
            "short_bias_score": short_bias_score,
            "direction_score": _clamp(short_bias_score - long_bias_score, -1.0, 1.0),
            "zone_score": zone_score,
            "trend_score": trend_score,
            "shock_active": shock_active,
            "shock_reason": (
                f"1m ret={_safe_float(one_min.get('return_ratio')):.4f} amp={_safe_float(one_min.get('amplitude_ratio')):.4f}"
                if shock_active
                else None
            ),
        }
    )
    return report


def apply_multi_timeframe_bias(
    *,
    buy_levels: int,
    sell_levels: int,
    per_order_notional: float,
    step_price: float,
    max_position_notional: float,
    max_short_position_notional: float,
    report: dict[str, Any],
    config: MultiTimeframeBiasConfig | None = None,
) -> dict[str, Any]:
    cfg = config or MultiTimeframeBiasConfig(enabled=bool(report.get("enabled")))
    if not report.get("available"):
        return {
            "buy_levels": int(buy_levels),
            "sell_levels": int(sell_levels),
            "per_order_notional": float(per_order_notional),
            "step_price": float(step_price),
            "max_position_notional": float(max_position_notional),
            "max_short_position_notional": float(max_short_position_notional),
            "buy_offset_steps": 0.0,
            "sell_offset_steps": 0.0,
            "applied": False,
        }

    direction_score = _clamp(_safe_float(report.get("direction_score")), -1.0, 1.0)
    level_delta = int(round(abs(direction_score) * max(int(cfg.max_level_delta), 0)))
    base_buy = max(int(buy_levels), 0)
    base_sell = max(int(sell_levels), 0)
    long_scale = 1.0
    short_scale = 1.0
    if direction_score < 0:
        base_buy += level_delta
        base_sell = max(base_sell - level_delta, 1)
        long_scale = cfg.favored_position_scale
        short_scale = cfg.unfavored_position_scale
    elif direction_score > 0:
        base_sell += level_delta
        base_buy = max(base_buy - level_delta, 1)
        short_scale = cfg.favored_position_scale
        long_scale = cfg.unfavored_position_scale

    notional_scale = cfg.shock_notional_scale if report.get("shock_active") else 1.0
    step_scale = cfg.shock_step_scale if report.get("shock_active") else 1.0
    return {
        "buy_levels": base_buy,
        "sell_levels": base_sell,
        "per_order_notional": max(float(per_order_notional) * notional_scale, 0.0),
        "step_price": max(float(step_price) * step_scale, 0.0),
        "max_position_notional": max(float(max_position_notional) * long_scale, 0.0),
        "max_short_position_notional": max(float(max_short_position_notional) * short_scale, 0.0),
        "buy_offset_steps": direction_score * cfg.max_offset_steps,
        "sell_offset_steps": -direction_score * cfg.max_offset_steps,
        "applied": True,
    }


def resolve_dynamic_side_scheduler(
    *,
    report: dict[str, Any],
    current_long_notional: float,
    current_short_notional: float,
    max_position_notional: float | None,
    max_short_position_notional: float | None,
    pause_buy_position_notional: float | None,
    pause_short_position_notional: float | None,
    per_order_notional: float,
    step_price: float,
    recent_loss_ratio: float = 0.0,
    active_delever_loss_count: int = 0,
    config: MultiTimeframeBiasConfig | None = None,
) -> dict[str, Any]:
    cfg = config or MultiTimeframeBiasConfig(enabled=bool(report.get("enabled")))
    base: dict[str, Any] = {
        "enabled": bool(cfg.enabled and cfg.scheduler_enabled),
        "available": bool(report.get("available")),
        "applied": False,
        "defensive_mode": False,
        "long_permission": 1.0,
        "short_permission": 1.0,
        "long_budget_scale": 1.0,
        "short_budget_scale": 1.0,
        "per_order_notional_scale": 1.0,
        "step_scale": 1.0,
        "reduce_max_order_scale": 1.0,
        "buy_offset_steps": 0.0,
        "sell_offset_steps": 0.0,
        "max_position_notional": max(_safe_float(max_position_notional), 0.0),
        "max_short_position_notional": max(_safe_float(max_short_position_notional), 0.0),
        "pause_buy_position_notional": max(_safe_float(pause_buy_position_notional), 0.0),
        "pause_short_position_notional": max(_safe_float(pause_short_position_notional), 0.0),
        "per_order_notional": max(_safe_float(per_order_notional), 0.0),
        "step_price": max(_safe_float(step_price), 0.0),
        "direction_score": _clamp(_safe_float(report.get("direction_score")), -1.0, 1.0),
        "zone_score": _clamp(_safe_float(report.get("zone_score")), 0.0, 1.0),
        "inventory_skew_score": 0.0,
        "reasons": [],
    }
    if not base["enabled"] or not base["available"]:
        return base

    reasons: list[str] = []
    long_notional = max(_safe_float(current_long_notional), 0.0)
    short_notional = max(_safe_float(current_short_notional), 0.0)
    long_limit = max(base["pause_buy_position_notional"], base["max_position_notional"], long_notional, 1e-12)
    short_limit = max(base["pause_short_position_notional"], base["max_short_position_notional"], short_notional, 1e-12)
    long_inventory_ratio = _clamp(long_notional / long_limit, 0.0, 1.5)
    short_inventory_ratio = _clamp(short_notional / short_limit, 0.0, 1.5)
    inventory_skew_score = _clamp(short_inventory_ratio - long_inventory_ratio, -1.0, 1.0)

    direction_score = base["direction_score"]
    zone_score = base["zone_score"]
    low_pressure = _clamp((cfg.low_zone_threshold - zone_score) / max(cfg.low_zone_threshold, 1e-12), 0.0, 1.0)
    high_pressure = _clamp((zone_score - cfg.high_zone_threshold) / max(1.0 - cfg.high_zone_threshold, 1e-12), 0.0, 1.0)
    trend_pressure = abs(direction_score)
    long_adverse_score = _clamp(
        (high_pressure * cfg.scheduler_zone_weight)
        + (_clamp(direction_score, 0.0, 1.0) * cfg.scheduler_trend_weight)
        + (_clamp(-inventory_skew_score, 0.0, 1.0) * cfg.scheduler_inventory_weight),
        0.0,
        1.0,
    )
    short_adverse_score = _clamp(
        (low_pressure * cfg.scheduler_zone_weight)
        + (_clamp(-direction_score, 0.0, 1.0) * cfg.scheduler_trend_weight)
        + (_clamp(inventory_skew_score, 0.0, 1.0) * cfg.scheduler_inventory_weight),
        0.0,
        1.0,
    )

    if long_inventory_ratio >= cfg.scheduler_inventory_skew_threshold and inventory_skew_score < -0.15:
        reasons.append("long_inventory_skew")
    if short_inventory_ratio >= cfg.scheduler_inventory_skew_threshold and inventory_skew_score > 0.15:
        reasons.append("short_inventory_skew")
    if low_pressure > 0:
        reasons.append("low_zone")
    if high_pressure > 0:
        reasons.append("high_zone")
    if trend_pressure >= cfg.strong_bias_threshold:
        reasons.append("trend_bias")

    min_budget = _clamp(cfg.scheduler_min_budget_scale, 0.0, 1.0)
    max_budget = max(cfg.scheduler_max_budget_scale, 1.0)
    long_budget_scale = _clamp(1.0 - long_adverse_score, min_budget, max_budget)
    short_budget_scale = _clamp(1.0 - short_adverse_score, min_budget, max_budget)
    if direction_score < 0:
        long_budget_scale = _clamp(long_budget_scale + abs(direction_score) * 0.25, min_budget, max_budget)
    elif direction_score > 0:
        short_budget_scale = _clamp(short_budget_scale + direction_score * 0.25, min_budget, max_budget)

    shock_active = bool(report.get("shock_active"))
    loss_state = _safe_float(recent_loss_ratio) >= 0.70 or int(active_delever_loss_count or 0) > 0
    defensive_mode = shock_active or loss_state
    if shock_active:
        reasons.append("shock")
    if loss_state:
        reasons.append("loss_state")

    per_order_scale = min(long_budget_scale, short_budget_scale)
    step_scale = 1.0
    reduce_scale = 1.0
    if defensive_mode:
        per_order_scale = min(per_order_scale, cfg.scheduler_defensive_notional_scale, cfg.shock_notional_scale)
        step_scale = max(cfg.scheduler_defensive_step_scale, cfg.shock_step_scale if shock_active else 1.0)
        reduce_scale = _clamp(cfg.scheduler_reduce_max_order_scale, 0.0, 1.0)

    buy_offset_steps = long_adverse_score * cfg.scheduler_max_offset_steps
    sell_offset_steps = short_adverse_score * cfg.scheduler_max_offset_steps
    if direction_score < 0 and long_adverse_score < 0.20:
        buy_offset_steps -= abs(direction_score) * min(cfg.max_offset_steps, cfg.scheduler_max_offset_steps)
    if direction_score > 0 and short_adverse_score < 0.20:
        sell_offset_steps -= direction_score * min(cfg.max_offset_steps, cfg.scheduler_max_offset_steps)

    long_limit_scale = long_budget_scale
    short_limit_scale = short_budget_scale
    base.update(
        {
            "applied": True,
            "defensive_mode": defensive_mode,
            "long_permission": _clamp(long_budget_scale, 0.0, 1.0),
            "short_permission": _clamp(short_budget_scale, 0.0, 1.0),
            "long_budget_scale": long_budget_scale,
            "short_budget_scale": short_budget_scale,
            "per_order_notional_scale": _clamp(per_order_scale, 0.0, max_budget),
            "step_scale": max(step_scale, 1.0),
            "reduce_max_order_scale": reduce_scale,
            "buy_offset_steps": buy_offset_steps,
            "sell_offset_steps": sell_offset_steps,
            "max_position_notional": max(base["max_position_notional"] * long_limit_scale, long_notional),
            "max_short_position_notional": max(base["max_short_position_notional"] * short_limit_scale, short_notional),
            "pause_buy_position_notional": max(base["pause_buy_position_notional"] * long_limit_scale, 0.0),
            "pause_short_position_notional": max(base["pause_short_position_notional"] * short_limit_scale, 0.0),
            "per_order_notional": base["per_order_notional"] * _clamp(per_order_scale, 0.0, max_budget),
            "step_price": base["step_price"] * max(step_scale, 1.0),
            "long_inventory_ratio": long_inventory_ratio,
            "short_inventory_ratio": short_inventory_ratio,
            "inventory_skew_score": inventory_skew_score,
            "long_adverse_score": long_adverse_score,
            "short_adverse_score": short_adverse_score,
            "reasons": reasons,
        }
    )
    return base
