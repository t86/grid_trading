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
    adaptive_raw_scale_defensive: float = 1.2
    adaptive_raw_scale_cooldown: float = 2.0
    step_scale_sprint: float = 0.8
    step_scale_defensive: float = 1.8
    step_scale_cooldown: float = 3.0
    per_order_scale_sprint: float = 1.25
    per_order_scale_defensive: float = 0.65
    levels_scale_sprint: float = 1.25
    levels_scale_defensive: float = 0.65
    cooldown_seconds: float = 120.0
    state_confirm_cycles: int = 3


@dataclass(frozen=True)
class ElasticVolumeInputs:
    now: datetime
    last_state: dict[str, Any]
    gross_notional_15m: float
    net_pnl_15m: float
    competition_gross_notional: float
    competition_net_pnl: float
    competition_commission: float
    long_notional: float
    short_notional: float
    max_long_notional: float
    max_short_notional: float
    actual_net_notional: float
    adaptive_step_raw_scale: float
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


def _base_control(
    *,
    enabled: bool,
    regime: str,
    reasons: list[str] | None = None,
    step_scale: float = 1.0,
    per_order_scale: float = 1.0,
    levels_scale: float = 1.0,
    position_limit_scale: float = 1.0,
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
        "entry_allowed": True,
        "allow_entry_long": True,
        "allow_entry_short": True,
        "allow_reduce_long": True,
        "allow_reduce_short": True,
        "cooldown_until": None,
        "metrics": {},
    }


def resolve_elastic_volume_control(
    *,
    config: ElasticVolumeConfig,
    inputs: ElasticVolumeInputs,
) -> dict[str, Any]:
    now = inputs.now.astimezone(timezone.utc)
    last_regime = str((inputs.last_state or {}).get("regime") or "").strip() or None
    loss_15m = _loss_per_10k(inputs.net_pnl_15m, inputs.gross_notional_15m)
    long_notional = max(_safe_float(inputs.long_notional), 0.0)
    short_notional = max(_safe_float(inputs.short_notional), 0.0)
    max_long = max(_safe_float(inputs.max_long_notional), 0.0)
    max_short = max(_safe_float(inputs.max_short_notional), 0.0)
    inventory_ratio = _inventory_ratio(long_notional, short_notional, max_long, max_short)
    raw_scale = max(_safe_float(inputs.adaptive_step_raw_scale), 0.0)
    bias_regime = str(inputs.multi_timeframe_bias_regime or "balanced")

    if not config.enabled:
        control = _base_control(enabled=False, regime="disabled")
        control["last_regime"] = last_regime
        control["metrics"] = {
            "loss_per_10k_15m": loss_15m,
            "inventory_ratio": inventory_ratio,
            "adaptive_step_raw_scale": raw_scale,
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
        )
        control["entry_allowed"] = False
        control["allow_entry_long"] = False
        control["allow_entry_short"] = False
        control["cooldown_until"] = cooldown_until.isoformat()
    else:
        reasons: list[str] = []
        hard_inventory = inventory_ratio >= max(_safe_float(config.inventory_hard_ratio), 0.0) > 0
        soft_inventory = inventory_ratio >= max(_safe_float(config.inventory_soft_ratio), 0.0) > 0
        if loss_15m >= max(_safe_float(config.loss_per_10k_cooldown), 0.0) > 0:
            reasons.append("loss_per_10k_15m")
        if raw_scale >= max(_safe_float(config.adaptive_raw_scale_cooldown), 0.0) > 0:
            reasons.append("adaptive_raw_scale")
        if hard_inventory:
            reasons.append("inventory_hard")

        if reasons:
            control = _base_control(
                enabled=True,
                regime="cooldown",
                reasons=reasons,
                step_scale=config.step_scale_cooldown,
                per_order_scale=config.per_order_scale_defensive,
                levels_scale=config.levels_scale_defensive,
                position_limit_scale=config.levels_scale_defensive,
            )
            control["entry_allowed"] = False
            control["allow_entry_long"] = False
            control["allow_entry_short"] = False
            control["cooldown_until"] = (now + timedelta(seconds=max(config.cooldown_seconds, 0.0))).isoformat()
        elif soft_inventory:
            control = _base_control(
                enabled=True,
                regime="recover",
                reasons=["inventory_soft"],
                step_scale=max(config.step_scale_defensive, 1.0),
                per_order_scale=config.per_order_scale_defensive,
                levels_scale=1.0,
                position_limit_scale=config.levels_scale_defensive,
            )
        elif loss_15m > config.loss_per_10k_cruise or raw_scale >= config.adaptive_raw_scale_defensive:
            control = _base_control(
                enabled=True,
                regime="defensive",
                reasons=["loss_per_10k_15m"] if loss_15m > config.loss_per_10k_cruise else ["adaptive_raw_scale"],
                step_scale=max(config.step_scale_defensive, 1.0),
                per_order_scale=config.per_order_scale_defensive,
                levels_scale=config.levels_scale_defensive,
                position_limit_scale=config.levels_scale_defensive,
            )
        elif (
            loss_15m <= config.loss_per_10k_sprint
            and raw_scale < 0.8
            and inventory_ratio < 0.35
            and (
                inputs.competition_required_pace is None
                or inputs.competition_actual_pace is None
                or _safe_float(inputs.competition_actual_pace) <= _safe_float(inputs.competition_required_pace)
            )
        ):
            control = _base_control(
                enabled=True,
                regime="sprint",
                reasons=["low_loss", "low_volatility"],
                step_scale=config.step_scale_sprint,
                per_order_scale=config.per_order_scale_sprint,
                levels_scale=config.levels_scale_sprint,
                position_limit_scale=1.0,
            )
        else:
            control = _base_control(enabled=True, regime="cruise")

    control["last_regime"] = last_regime

    net = _safe_float(inputs.actual_net_notional)
    if control["regime"] in {"recover", "cooldown"}:
        if long_notional >= short_notional and long_notional > 0:
            control["allow_entry_long"] = False
            control["allow_reduce_long"] = True
            if control["regime"] == "recover":
                control["allow_entry_short"] = True
        elif short_notional > 0:
            control["allow_entry_short"] = False
            control["allow_reduce_short"] = True
            if control["regime"] == "recover":
                control["allow_entry_long"] = True
        elif net > 0:
            control["allow_entry_long"] = False
        elif net < 0:
            control["allow_entry_short"] = False

    if control["regime"] == "recover" and (
        (bias_regime == "low_long_bias" and not control["allow_entry_long"])
        or (bias_regime == "high_short_bias" and not control["allow_entry_short"])
    ):
        control["reasons"].append("inventory_over_bias")

    control["metrics"] = {
        "loss_per_10k_15m": loss_15m,
        "gross_notional_15m": _safe_float(inputs.gross_notional_15m),
        "net_pnl_15m": _safe_float(inputs.net_pnl_15m),
        "competition_gross_notional": _safe_float(inputs.competition_gross_notional),
        "competition_net_pnl": _safe_float(inputs.competition_net_pnl),
        "competition_commission": _safe_float(inputs.competition_commission),
        "long_notional": long_notional,
        "short_notional": short_notional,
        "actual_net_notional": net,
        "inventory_ratio": inventory_ratio,
        "adaptive_step_raw_scale": raw_scale,
        "multi_timeframe_bias_regime": bias_regime,
    }
    return control
