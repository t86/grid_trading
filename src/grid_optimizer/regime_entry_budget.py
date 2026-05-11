from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


@dataclass(frozen=True)
class RegimeEntryBudgetConfig:
    enabled: bool = False
    report_only: bool = True
    base_per_order_notional: float = 60.0
    base_step_ratio: float = 0.0025
    fast_step_multiplier: float = 0.8
    safe_step_multiplier: float = 1.2
    wide_step_multiplier: float = 2.0
    fast_per_order_multiplier: float = 0.8
    safe_per_order_multiplier: float = 1.2
    wide_per_order_multiplier: float = 2.0
    fast_side_budget_units: int = 10
    safe_side_budget_units: int = 15
    wide_side_budget_units: int = 20
    switch_reconcile_confirm_cycles: int = 2
    shock_reconcile_confirm_cycles: int = 3
    shock_30s_abs_return_ratio: float = 0.016
    shock_30s_amplitude_ratio: float = 0.024
    shock_1m_abs_return_ratio: float = 0.025
    shock_1m_amplitude_ratio: float = 0.030
    shock_reconcile_diff_count: int = 2
    defensive_loss_per_10k_15m: float = 8.0
    defensive_inventory_usage_ratio: float = 0.80
    defensive_min_gross_notional_15m: float = 1_000.0
    low_vol_score_threshold: float = 0.25
    mid_vol_score_threshold: float = 0.60
    high_vol_score_threshold: float = 1.00
    low_vol_step_scale: float = 0.70
    low_vol_per_order_scale: float = 0.70
    mid_vol_step_scale: float = 0.85
    mid_vol_per_order_scale: float = 0.85
    high_vol_step_scale: float = 1.25
    high_vol_per_order_scale: float = 0.85
    tick_dominated_ratio: float = 0.0035
    coarse_tick_ratio: float = 0.0080
    min_profit_or_fee_buffer_ratio: float = 0.0008
    coarse_tick_per_order_scale: float = 0.70
    coarse_tick_budget_scale: float = 0.70


@dataclass(frozen=True)
class RegimeEntryBudgetInputs:
    now: datetime
    last_state: dict[str, Any]
    candidate_regime: str
    mid_price: float
    tick_size: float
    current_long_notional: float
    current_short_notional: float
    open_entry_long_notional: float = 0.0
    open_entry_short_notional: float = 0.0
    open_non_reduce_entry_orders: int = 0
    reconcile_ok: bool = True
    reconcile_diff_count: int = 0
    cancel_pending: bool = False
    post_only_reject_count: int = 0
    gross_notional_15m: float = 0.0
    loss_per_10k_15m: float = 0.0
    abs_return_30s_ratio: float = 0.0
    amplitude_30s_ratio: float = 0.0
    abs_return_1m_ratio: float = 0.0
    amplitude_1m_ratio: float = 0.0
    amplitude_5m_ratio: float = 0.0


_REGIMES = {"ping-pong-fast", "ping-pong-safe", "wide-step", "defensive", "shock-guard"}


def _safe_float(value: Any) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return 0.0
    if math.isnan(result) or math.isinf(result):
        return 0.0
    return result


def _safe_int(value: Any) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def _normalize_regime(value: Any) -> str:
    regime = str(value or "").strip()
    return regime if regime in _REGIMES else "ping-pong-safe"


def _ceil_to_tick(value: float, tick_size: float) -> float:
    if value <= 0:
        return 0.0
    if tick_size <= 0:
        return value
    return math.ceil((value - 1e-15) / tick_size) * tick_size


def _regime_defaults(config: RegimeEntryBudgetConfig, regime: str) -> dict[str, float]:
    if regime == "ping-pong-fast":
        return {
            "step_multiplier": config.fast_step_multiplier,
            "per_order_multiplier": config.fast_per_order_multiplier,
            "budget_units": float(config.fast_side_budget_units),
            "min_tick_steps": 1.0,
        }
    if regime == "wide-step":
        return {
            "step_multiplier": config.wide_step_multiplier,
            "per_order_multiplier": config.wide_per_order_multiplier,
            "budget_units": float(config.wide_side_budget_units),
            "min_tick_steps": 2.0,
        }
    if regime == "defensive":
        return {"step_multiplier": 0.0, "per_order_multiplier": 0.0, "budget_units": 0.0, "min_tick_steps": 3.0}
    return {
        "step_multiplier": config.safe_step_multiplier,
        "per_order_multiplier": config.safe_per_order_multiplier,
        "budget_units": float(config.safe_side_budget_units),
        "min_tick_steps": 1.0,
    }


def _micro_scales(config: RegimeEntryBudgetConfig, vol_score: float, regime: str) -> tuple[float, float, str]:
    if regime in {"defensive", "shock-guard"}:
        return 1.0, 1.0, "blocked"
    if vol_score < config.low_vol_score_threshold:
        return config.low_vol_step_scale, config.low_vol_per_order_scale, "low"
    if vol_score < config.mid_vol_score_threshold:
        return config.mid_vol_step_scale, config.mid_vol_per_order_scale, "mid"
    if vol_score < config.high_vol_score_threshold:
        return 1.0, 1.0, "normal"
    return config.high_vol_step_scale, config.high_vol_per_order_scale, "high"


def _vol_score(config: RegimeEntryBudgetConfig, inputs: RegimeEntryBudgetInputs) -> float:
    scores = []
    if config.shock_30s_amplitude_ratio > 0:
        scores.append(max(_safe_float(inputs.amplitude_30s_ratio), 0.0) / config.shock_30s_amplitude_ratio)
    if config.shock_1m_amplitude_ratio > 0:
        scores.append(max(_safe_float(inputs.amplitude_1m_ratio), 0.0) / config.shock_1m_amplitude_ratio)
    if config.shock_1m_amplitude_ratio > 0:
        scores.append(max(_safe_float(inputs.amplitude_5m_ratio), 0.0) / max(config.shock_1m_amplitude_ratio * 1.5, 1e-12))
    return max(scores or [0.0])


def _base_report(config: RegimeEntryBudgetConfig, inputs: RegimeEntryBudgetInputs, state: str) -> dict[str, Any]:
    return {
        "enabled": bool(config.enabled),
        "report_only": bool(config.report_only),
        "state": state,
        "candidate_regime": _normalize_regime(inputs.candidate_regime),
        "target_regime": None,
        "switching": False,
        "shock_guard_active": False,
        "cancel_entry_required": False,
        "allow_entry_long": True,
        "allow_entry_short": True,
        "open_entry_long_notional": max(_safe_float(inputs.open_entry_long_notional), 0.0),
        "open_entry_short_notional": max(_safe_float(inputs.open_entry_short_notional), 0.0),
        "open_non_reduce_entry_orders": max(_safe_int(inputs.open_non_reduce_entry_orders), 0),
        "long_side_budget": None,
        "short_side_budget": None,
        "long_entry_capacity": None,
        "short_entry_capacity": None,
        "effective_step_ratio": None,
        "effective_step_price": None,
        "effective_per_order_notional": None,
        "tick_ratio": 0.0,
        "tick_dominated": False,
        "coarse_tick": False,
        "reconcile_ok_count": 0,
        "blocked_reason": None,
        "reasons": [],
        "metrics": {},
    }


def resolve_regime_entry_budget_control(
    *,
    config: RegimeEntryBudgetConfig,
    inputs: RegimeEntryBudgetInputs,
) -> dict[str, Any]:
    last_state = inputs.last_state if isinstance(inputs.last_state, dict) else {}
    last_regime = _normalize_regime(last_state.get("state") or last_state.get("regime") or inputs.candidate_regime)
    candidate_regime = _normalize_regime(inputs.candidate_regime)
    if not config.enabled:
        report = _base_report(config, inputs, "disabled")
        report["reasons"].append("disabled")
        return report

    reconcile_ok = bool(inputs.reconcile_ok) and not bool(inputs.cancel_pending)
    last_reconcile_ok_count = _safe_int(last_state.get("reconcile_ok_count"))
    reconcile_ok_count = last_reconcile_ok_count + 1 if reconcile_ok else 0
    open_non_reduce_entry_orders = max(_safe_int(inputs.open_non_reduce_entry_orders), 0)
    current_long = max(_safe_float(inputs.current_long_notional), 0.0)
    current_short = max(_safe_float(inputs.current_short_notional), 0.0)
    open_entry_long = max(_safe_float(inputs.open_entry_long_notional), 0.0)
    open_entry_short = max(_safe_float(inputs.open_entry_short_notional), 0.0)
    loss_per_10k_15m = max(_safe_float(inputs.loss_per_10k_15m), 0.0)
    gross_15m = max(_safe_float(inputs.gross_notional_15m), 0.0)
    reasons: list[str] = []

    shock = (
        abs(_safe_float(inputs.abs_return_30s_ratio)) >= config.shock_30s_abs_return_ratio
        or max(_safe_float(inputs.amplitude_30s_ratio), 0.0) >= config.shock_30s_amplitude_ratio
        or abs(_safe_float(inputs.abs_return_1m_ratio)) >= config.shock_1m_abs_return_ratio
        or max(_safe_float(inputs.amplitude_1m_ratio), 0.0) >= config.shock_1m_amplitude_ratio
        or max(_safe_int(inputs.reconcile_diff_count), 0) >= config.shock_reconcile_diff_count
        or max(_safe_int(inputs.post_only_reject_count), 0) >= config.shock_reconcile_diff_count
    )
    if shock:
        reasons.append("shock_guard_threshold")

    active_regime_for_usage = candidate_regime if candidate_regime not in {"defensive", "shock-guard"} else last_regime
    usage_defaults = _regime_defaults(config, active_regime_for_usage)
    usage_budget = (
        config.base_per_order_notional
        * max(usage_defaults["per_order_multiplier"], 0.0)
        * max(usage_defaults["budget_units"], 0.0)
    )
    inventory_usage = (
        max(current_long + open_entry_long, current_short + open_entry_short) / usage_budget if usage_budget > 0 else 0.0
    )
    defensive = candidate_regime == "defensive" or (
        gross_15m >= config.defensive_min_gross_notional_15m
        and loss_per_10k_15m >= config.defensive_loss_per_10k_15m
    ) or inventory_usage >= config.defensive_inventory_usage_ratio
    if defensive:
        reasons.append("defensive_threshold")

    if defensive:
        desired_regime = "defensive"
    elif shock:
        desired_regime = "shock-guard"
    else:
        desired_regime = candidate_regime

    last_switching = bool(last_state.get("switching"))
    last_target = _normalize_regime(last_state.get("target_regime") or desired_regime)
    target_regime = last_target if last_switching else desired_regime
    switching = last_switching or (desired_regime != last_regime)
    if desired_regime in {"shock-guard", "defensive"}:
        target_regime = desired_regime
        switching = True
    elif last_switching and open_non_reduce_entry_orders == 0 and reconcile_ok_count >= config.switch_reconcile_confirm_cycles:
        switching = False
        last_regime = target_regime
        reasons.append("switch_committed")
    elif switching:
        target_regime = desired_regime
        reasons.append("switching_cancel_first")

    state = target_regime if switching and target_regime in {"shock-guard", "defensive"} else last_regime
    if not switching:
        state = desired_regime

    defaults = _regime_defaults(config, target_regime if not switching else desired_regime)
    vol_score = _vol_score(config, inputs)
    step_micro_scale, per_micro_scale, vol_bucket = _micro_scales(config, vol_score, desired_regime)
    target_step_ratio = config.base_step_ratio * max(defaults["step_multiplier"], 0.0) * step_micro_scale
    effective_per_order = config.base_per_order_notional * max(defaults["per_order_multiplier"], 0.0) * per_micro_scale
    side_budget = (
        config.base_per_order_notional
        * max(defaults["per_order_multiplier"], 0.0)
        * max(defaults["budget_units"], 0.0)
    )

    mid_price = max(_safe_float(inputs.mid_price), 0.0)
    tick_size = max(_safe_float(inputs.tick_size), 0.0)
    tick_ratio = tick_size / mid_price if mid_price > 0 and tick_size > 0 else 0.0
    tick_dominated = tick_ratio >= config.tick_dominated_ratio
    coarse_tick = tick_ratio >= config.coarse_tick_ratio
    if coarse_tick and desired_regime not in {"defensive", "shock-guard"}:
        effective_per_order *= max(config.coarse_tick_per_order_scale, 0.0)
        side_budget *= max(config.coarse_tick_budget_scale, 0.0)
        reasons.append("coarse_tick_risk_scaled")
    target_step_price = mid_price * target_step_ratio if mid_price > 0 else 0.0
    tick_step = tick_size * max(defaults["min_tick_steps"], 1.0) if tick_size > 0 else 0.0
    fee_buffer_step = mid_price * max(config.min_profit_or_fee_buffer_ratio, 0.0)
    effective_step_price = _ceil_to_tick(max(target_step_price, tick_step, fee_buffer_step), tick_size)
    effective_step_ratio = effective_step_price / mid_price if mid_price > 0 else 0.0

    cancel_required = switching or desired_regime in {"shock-guard", "defensive"}
    entry_blocked = cancel_required
    blocked_reason = "cancel_confirm_required" if cancel_required else None
    if desired_regime in {"shock-guard", "defensive"}:
        blocked_reason = desired_regime

    def capacity(side_budget_value: float, current: float, open_entry: float) -> int:
        if entry_blocked or effective_per_order <= 0:
            return 0
        remaining = side_budget_value - current - open_entry
        return max(int(math.floor((remaining + 1e-12) / effective_per_order)), 0)

    long_capacity = capacity(side_budget, current_long, open_entry_long)
    short_capacity = capacity(side_budget, current_short, open_entry_short)

    report = _base_report(config, inputs, state)
    report.update(
        {
            "candidate_regime": candidate_regime,
            "target_regime": target_regime if switching else None,
            "switching": bool(switching),
            "shock_guard_active": desired_regime == "shock-guard",
            "cancel_entry_required": bool(cancel_required),
            "allow_entry_long": long_capacity > 0,
            "allow_entry_short": short_capacity > 0,
            "open_entry_long_notional": open_entry_long,
            "open_entry_short_notional": open_entry_short,
            "open_non_reduce_entry_orders": open_non_reduce_entry_orders,
            "long_side_budget": 0.0 if desired_regime in {"defensive", "shock-guard"} else side_budget,
            "short_side_budget": 0.0 if desired_regime in {"defensive", "shock-guard"} else side_budget,
            "long_entry_capacity": long_capacity,
            "short_entry_capacity": short_capacity,
            "effective_step_ratio": effective_step_ratio,
            "effective_step_price": effective_step_price,
            "effective_per_order_notional": 0.0 if desired_regime in {"defensive", "shock-guard"} else effective_per_order,
            "tick_ratio": tick_ratio,
            "tick_dominated": tick_dominated,
            "coarse_tick": coarse_tick,
            "reconcile_ok_count": reconcile_ok_count,
            "blocked_reason": blocked_reason,
            "reasons": reasons,
            "metrics": {
                "vol_score": vol_score,
                "vol_bucket": vol_bucket,
                "inventory_usage_ratio": inventory_usage,
                "loss_per_10k_15m": loss_per_10k_15m,
                "gross_notional_15m": gross_15m,
                "abs_return_30s_ratio": _safe_float(inputs.abs_return_30s_ratio),
                "amplitude_30s_ratio": _safe_float(inputs.amplitude_30s_ratio),
                "abs_return_1m_ratio": _safe_float(inputs.abs_return_1m_ratio),
                "amplitude_1m_ratio": _safe_float(inputs.amplitude_1m_ratio),
                "amplitude_5m_ratio": _safe_float(inputs.amplitude_5m_ratio),
                "step_micro_scale": step_micro_scale,
                "per_order_micro_scale": per_micro_scale,
            },
        }
    )
    return report


def regime_entry_budget_state_snapshot(report: dict[str, Any], *, updated_at: str | None = None) -> dict[str, Any]:
    snapshot = {
        "state": report.get("state"),
        "target_regime": report.get("target_regime"),
        "switching": bool(report.get("switching")),
        "shock_guard_active": bool(report.get("shock_guard_active")),
        "reconcile_ok_count": _safe_int(report.get("reconcile_ok_count")),
    }
    if updated_at is None:
        updated_at = datetime.now(timezone.utc).isoformat()
    snapshot["updated_at"] = updated_at
    return snapshot
