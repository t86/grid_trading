from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class MultiTimeframeBiasConfig:
    enabled: bool = False
    mode_adapter: str = "auto"
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
    defensive_notional_scale: float = 0.70
    defensive_position_scale: float = 0.70
    recovery_notional_scale: float = 0.50


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _clamp(value: float, lower: float, upper: float) -> float:
    return min(max(value, lower), upper)


def _normalized_adapter(value: str | None) -> str:
    adapter = str(value or "auto").strip() or "auto"
    return "synthetic_neutral" if adapter == "auto" else adapter


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
    fifteen_return = _safe_float(fifteen.get("return_ratio"))
    one_hour_return = _safe_float(one_hour.get("return_ratio"))
    four_hour_return = _safe_float(four_hour.get("return_ratio"))
    one_min_return = _safe_float(one_min.get("return_ratio"))
    one_min_zone = _safe_float(one_min.get("zone"))
    recovery_probe_active = (
        not shock_active
        and four_hour_return <= -0.04
        and one_min_return > 0.0
        and one_min_zone >= 0.65
        and (config.low_zone_threshold * 0.75) < zone_score < config.high_zone_threshold
    )
    downtrend_defensive_active = (
        not recovery_probe_active
        and zone_score <= config.low_zone_threshold
        and trend_score <= -0.45
        and four_hour_return <= -0.025
        and fifteen_return < 0.0
        and one_hour_return < 0.0
    )
    uptrend_defensive_active = (
        zone_score >= config.high_zone_threshold
        and trend_score >= 0.45
        and four_hour_return >= 0.025
        and fifteen_return > 0.0
        and one_hour_return > 0.0
    )

    if shock_active:
        regime = "shock_defensive"
    elif downtrend_defensive_active:
        regime = "downtrend_defensive"
        long_bias_score = min(long_bias_score, config.strong_bias_threshold * 0.5)
        short_bias_score = max(short_bias_score, min(0.35, 1.0))
    elif uptrend_defensive_active:
        regime = "uptrend_defensive"
        short_bias_score = min(short_bias_score, config.strong_bias_threshold * 0.5)
        long_bias_score = max(long_bias_score, min(0.35, 1.0))
    elif recovery_probe_active:
        regime = "recovery_probe"
        long_bias_score = min(long_bias_score, config.strong_bias_threshold * 0.5)
        short_bias_score = min(short_bias_score, config.strong_bias_threshold * 0.5)
    elif long_bias_score >= config.strong_bias_threshold and long_bias_score >= short_bias_score:
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
            "downtrend_defensive_active": downtrend_defensive_active,
            "uptrend_defensive_active": uptrend_defensive_active,
            "recovery_probe_active": recovery_probe_active,
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
    adapter = _normalized_adapter(cfg.mode_adapter)
    if not report.get("available"):
        return {
            "adapter": adapter,
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
    base_notional = max(float(per_order_notional), 0.0)
    base_step = max(float(step_price), 0.0)
    base_long_cap = max(float(max_position_notional), 0.0)
    base_short_cap = max(float(max_short_position_notional), 0.0)
    long_scale = 1.0
    short_scale = 1.0
    buy_offset_steps = 0.0
    sell_offset_steps = 0.0
    regime = str(report.get("regime") or "")
    if regime == "shock_defensive":
        base_buy = max(base_buy - max(level_delta, 1), 1)
        base_sell = max(base_sell - max(level_delta, 1), 1)
        long_scale = min(cfg.defensive_position_scale, 1.0)
        short_scale = min(cfg.defensive_position_scale, 1.0)
        base_notional *= min(cfg.defensive_notional_scale, 1.0)
    elif regime == "downtrend_defensive":
        base_buy = max(base_buy - max(level_delta, 1), 1)
        long_scale = min(cfg.defensive_position_scale, 1.0)
        base_notional *= min(cfg.defensive_notional_scale, 1.0)
        buy_offset_steps = abs(direction_score) * cfg.max_offset_steps
        if adapter in {"synthetic_neutral", "inventory_grid"}:
            base_sell += min(level_delta, 1 if level_delta > 0 else 0)
            short_scale = max(1.0, 1.0 + ((cfg.favored_position_scale - 1.0) * 0.25))
    elif regime == "uptrend_defensive":
        base_sell = max(base_sell - max(level_delta, 1), 1)
        short_scale = min(cfg.defensive_position_scale, 1.0)
        base_notional *= min(cfg.defensive_notional_scale, 1.0)
        sell_offset_steps = -abs(direction_score) * cfg.max_offset_steps
        if adapter in {"synthetic_neutral", "inventory_grid"}:
            base_buy += min(level_delta, 1 if level_delta > 0 else 0)
            long_scale = max(1.0, 1.0 + ((cfg.favored_position_scale - 1.0) * 0.25))
    elif regime == "recovery_probe":
        base_notional *= min(cfg.recovery_notional_scale, 1.0)
        long_scale = min(1.0, 1.0 + ((cfg.favored_position_scale - 1.0) * 0.25))
        short_scale = min(1.0, 1.0 + ((cfg.favored_position_scale - 1.0) * 0.25))
    elif adapter == "one_way_long":
        long_bias = _clamp(_safe_float(report.get("long_bias_score")), 0.0, 1.0)
        short_bias = _clamp(_safe_float(report.get("short_bias_score")), 0.0, 1.0)
        if long_bias >= short_bias and direction_score < 0:
            base_buy += level_delta
            long_scale = cfg.favored_position_scale
            buy_offset_steps = direction_score * cfg.max_offset_steps
        elif short_bias > 0:
            de_risk = _clamp(short_bias, 0.0, 1.0)
            base_buy = max(base_buy - max(level_delta, 1), 1)
            base_notional *= max(cfg.unfavored_position_scale, 0.0)
            long_scale = cfg.unfavored_position_scale
            buy_offset_steps = de_risk * cfg.max_offset_steps
    elif adapter == "one_way_short":
        long_bias = _clamp(_safe_float(report.get("long_bias_score")), 0.0, 1.0)
        short_bias = _clamp(_safe_float(report.get("short_bias_score")), 0.0, 1.0)
        if short_bias >= long_bias and direction_score > 0:
            base_sell += level_delta
            short_scale = cfg.favored_position_scale
            sell_offset_steps = -direction_score * cfg.max_offset_steps
        elif long_bias > 0:
            de_risk = _clamp(long_bias, 0.0, 1.0)
            base_sell = max(base_sell - max(level_delta, 1), 1)
            base_notional *= max(cfg.unfavored_position_scale, 0.0)
            short_scale = cfg.unfavored_position_scale
            sell_offset_steps = -de_risk * cfg.max_offset_steps
    elif adapter == "inventory_grid":
        conservative_delta = min(level_delta, 1 if level_delta > 0 else 0)
        if direction_score < 0:
            base_buy += conservative_delta
            base_sell = max(base_sell - conservative_delta, 1)
            long_scale = 1.0 + ((cfg.favored_position_scale - 1.0) * 0.5)
            short_scale = 1.0 + ((cfg.unfavored_position_scale - 1.0) * 0.5)
            buy_offset_steps = direction_score * cfg.max_offset_steps * 0.5
            sell_offset_steps = -direction_score * cfg.max_offset_steps * 0.5
        elif direction_score > 0:
            base_sell += conservative_delta
            base_buy = max(base_buy - conservative_delta, 1)
            short_scale = 1.0 + ((cfg.favored_position_scale - 1.0) * 0.5)
            long_scale = 1.0 + ((cfg.unfavored_position_scale - 1.0) * 0.5)
            buy_offset_steps = direction_score * cfg.max_offset_steps * 0.5
            sell_offset_steps = -direction_score * cfg.max_offset_steps * 0.5
    elif direction_score < 0:
        base_buy += level_delta
        base_sell = max(base_sell - level_delta, 1)
        long_scale = cfg.favored_position_scale
        short_scale = cfg.unfavored_position_scale
        buy_offset_steps = direction_score * cfg.max_offset_steps
        sell_offset_steps = -direction_score * cfg.max_offset_steps
    elif direction_score > 0:
        base_sell += level_delta
        base_buy = max(base_buy - level_delta, 1)
        short_scale = cfg.favored_position_scale
        long_scale = cfg.unfavored_position_scale
        buy_offset_steps = direction_score * cfg.max_offset_steps
        sell_offset_steps = -direction_score * cfg.max_offset_steps

    notional_scale = cfg.shock_notional_scale if report.get("shock_active") else 1.0
    step_scale = cfg.shock_step_scale if report.get("shock_active") else 1.0
    if report.get("shock_active"):
        long_scale = min(long_scale, 1.0)
        short_scale = min(short_scale, 1.0)
    return {
        "adapter": adapter,
        "buy_levels": base_buy,
        "sell_levels": base_sell,
        "per_order_notional": max(base_notional * notional_scale, 0.0),
        "step_price": max(base_step * step_scale, 0.0),
        "max_position_notional": max(base_long_cap * long_scale, 0.0),
        "max_short_position_notional": max(base_short_cap * short_scale, 0.0),
        "buy_offset_steps": buy_offset_steps,
        "sell_offset_steps": sell_offset_steps,
        "applied": True,
    }
