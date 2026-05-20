from __future__ import annotations

import argparse
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class StrategyProfileSchema:
    family: str
    intent: str
    required_position_mode: str
    allowed_runtime_switches: frozenset[str]
    allowed_params: frozenset[str]
    required_position_mode_defaulted: bool = False


ONE_WAY_POSITION_MODE = "one_way"
HEDGE_POSITION_MODE = "hedge"
VALID_POSITION_MODES = frozenset({ONE_WAY_POSITION_MODE, HEDGE_POSITION_MODE})

RUNTIME_SWITCH_DEFAULTS: dict[str, Any] = {
    "auto_regime_enabled": False,
    "market_bias_enabled": False,
    "market_bias_regime_switch_enabled": False,
    "multi_timeframe_bias_enabled": False,
    "synthetic_flow_sleeve_enabled": False,
    "synthetic_trend_follow_enabled": False,
    "volume_long_v4_flow_sleeve_enabled": False,
    "custom_grid_enabled": False,
    "elastic_volume_enabled": False,
    "hard_loss_forced_reduce_enabled": False,
    "adverse_reduce_enabled": False,
    "exposure_escalation_enabled": False,
    "excess_inventory_reduce_only_enabled": False,
}

PARAMETER_NEUTRAL_DEFAULTS: dict[str, Any] = {
    **RUNTIME_SWITCH_DEFAULTS,
    "auto_regime_confirm_cycles": 2,
    "auto_regime_stable_15m_max_amplitude_ratio": 0.02,
    "auto_regime_stable_60m_max_amplitude_ratio": 0.05,
    "auto_regime_stable_60m_return_floor_ratio": -0.01,
    "auto_regime_defensive_15m_amplitude_ratio": 0.035,
    "auto_regime_defensive_60m_amplitude_ratio": 0.08,
    "auto_regime_defensive_15m_return_ratio": -0.015,
    "auto_regime_defensive_60m_return_ratio": -0.03,
    "market_bias_max_shift_steps": 0.75,
    "market_bias_signal_steps": 2.0,
    "market_bias_drift_weight": 0.65,
    "market_bias_return_weight": 0.35,
    "market_bias_weak_buy_pause_enabled": False,
    "market_bias_weak_buy_pause_threshold": 0.15,
    "market_bias_weak_buy_probe_scale": 0.0,
    "market_bias_strong_short_pause_enabled": False,
    "market_bias_strong_short_pause_threshold": 0.15,
    "market_bias_strong_short_probe_scale": 0.25,
    "market_bias_regime_switch_confirm_cycles": 2,
    "market_bias_regime_switch_weak_threshold": 0.15,
    "market_bias_regime_switch_strong_threshold": 0.15,
    "multi_timeframe_bias_low_zone_threshold": 0.35,
    "multi_timeframe_bias_high_zone_threshold": 0.65,
    "multi_timeframe_bias_strong_threshold": 0.50,
    "multi_timeframe_bias_max_level_delta": 4,
    "multi_timeframe_bias_max_offset_steps": 1.0,
    "multi_timeframe_bias_favored_position_scale": 1.25,
    "multi_timeframe_bias_unfavored_position_scale": 0.75,
    "multi_timeframe_bias_shock_abs_return_ratio": 0.018,
    "multi_timeframe_bias_shock_amplitude_ratio": 0.025,
    "multi_timeframe_bias_shock_step_scale": 1.5,
    "multi_timeframe_bias_shock_notional_scale": 0.70,
    "multi_timeframe_bias_scheduler_enabled": True,
    "multi_timeframe_bias_scheduler_zone_weight": 0.60,
    "multi_timeframe_bias_scheduler_trend_weight": 0.25,
    "multi_timeframe_bias_scheduler_inventory_weight": 0.55,
    "multi_timeframe_bias_scheduler_min_budget_scale": 0.20,
    "multi_timeframe_bias_scheduler_max_budget_scale": 1.15,
    "multi_timeframe_bias_scheduler_inventory_skew_threshold": 0.70,
    "multi_timeframe_bias_scheduler_max_offset_steps": 2.0,
    "multi_timeframe_bias_scheduler_defensive_notional_scale": 0.60,
    "multi_timeframe_bias_scheduler_defensive_step_scale": 1.35,
    "multi_timeframe_bias_scheduler_reduce_max_order_scale": 0.65,
    "synthetic_residual_long_flat_notional": None,
    "synthetic_residual_short_flat_notional": None,
    "synthetic_tiny_long_residual_notional": None,
    "synthetic_tiny_short_residual_notional": None,
    "static_buy_offset_steps": 0.0,
    "static_sell_offset_steps": 0.0,
    "synthetic_trend_follow_1m_abs_return_ratio": 0.0,
    "synthetic_trend_follow_1m_amplitude_ratio": 0.0,
    "synthetic_trend_follow_3m_abs_return_ratio": 0.0,
    "synthetic_trend_follow_3m_amplitude_ratio": 0.0,
    "synthetic_trend_follow_min_efficiency_ratio": 0.0,
    "synthetic_trend_follow_reverse_delay_seconds": 0.0,
    "synthetic_flow_sleeve_trigger_notional": None,
    "synthetic_flow_sleeve_notional": 0.0,
    "synthetic_flow_sleeve_levels": 0,
    "synthetic_flow_sleeve_order_notional": None,
    "synthetic_flow_sleeve_max_loss_ratio": 0.0025,
    "volume_long_v4_flow_sleeve_trigger_notional": None,
    "volume_long_v4_flow_sleeve_reduce_to_notional": None,
    "volume_long_v4_flow_sleeve_notional": 0.0,
    "volume_long_v4_flow_sleeve_levels": 0,
    "volume_long_v4_flow_sleeve_order_notional": None,
    "volume_long_v4_flow_sleeve_max_loss_ratio": 0.0025,
    "volume_long_v4_soft_loss_steps": 0.5,
    "volume_long_v4_hard_loss_steps": 1.5,
    "custom_grid_enabled": False,
    "custom_grid_direction": None,
    "custom_grid_level_mode": "arithmetic",
    "custom_grid_min_price": None,
    "custom_grid_max_price": None,
    "custom_grid_n": None,
    "custom_grid_total_notional": None,
    "custom_grid_neutral_anchor_price": None,
    "custom_grid_roll_enabled": False,
    "custom_grid_roll_interval_minutes": 5,
    "custom_grid_roll_trade_threshold": 100,
    "custom_grid_roll_upper_distance_ratio": 0.30,
    "custom_grid_roll_shift_levels": 1,
    "adaptive_step_enabled": False,
    "adaptive_step_30s_abs_return_ratio": 0.0,
    "adaptive_step_30s_amplitude_ratio": 0.0,
    "adaptive_step_1m_abs_return_ratio": 0.0,
    "adaptive_step_1m_amplitude_ratio": 0.0,
    "adaptive_step_3m_abs_return_ratio": 0.0,
    "adaptive_step_3m_amplitude_ratio": 0.0,
    "adaptive_step_5m_abs_return_ratio": 0.0,
    "adaptive_step_5m_amplitude_ratio": 0.0,
    "adaptive_step_max_scale": 1.0,
    "adaptive_step_min_per_order_scale": 1.0,
    "adaptive_step_min_position_limit_scale": 1.0,
    "min_mid_price_for_buys": None,
    "buy_pause_amp_trigger_ratio": None,
    "buy_pause_down_return_trigger_ratio": None,
    "short_cover_pause_amp_trigger_ratio": None,
    "short_cover_pause_down_return_trigger_ratio": None,
    "freeze_shift_abs_return_trigger_ratio": None,
    "inventory_tier_start_notional": None,
    "inventory_tier_end_notional": None,
    "inventory_tier_buy_levels": None,
    "inventory_tier_sell_levels": None,
    "inventory_tier_per_order_notional": None,
    "inventory_tier_base_position_notional": None,
    "threshold_position_notional": 0.0,
    "short_threshold_timeout_seconds": 60.0,
    "inventory_pause_timeout_seconds": 0.0,
    "near_market_entry_max_center_distance_steps": 2.0,
    "grid_inventory_rebalance_min_center_distance_steps": 3.0,
    "near_market_reentry_confirm_cycles": 2,
    "threshold_reduce_target_ratio": 0.0,
    "threshold_reduce_taker_timeout_seconds": 60.0,
    "adverse_reduce_short_trigger_ratio": 0.01,
    "adverse_reduce_long_trigger_ratio": 0.01,
    "adverse_reduce_target_ratio": 0.75,
    "adverse_reduce_maker_timeout_seconds": 45.0,
    "adverse_reduce_max_order_notional": 0.0,
    "adverse_reduce_keep_probe_scale": None,
    "exposure_escalation_notional": None,
    "exposure_escalation_hold_seconds": 600.0,
    "exposure_escalation_target_notional": None,
    "exposure_escalation_max_loss_ratio": None,
    "exposure_escalation_hard_unrealized_loss_limit": 10.0,
    "exposure_escalation_buy_pause_cooldown_seconds": 0.0,
    "hard_loss_forced_reduce_target_notional": None,
    "hard_loss_forced_reduce_max_order_notional": None,
    "hard_loss_forced_reduce_unrealized_loss_limit": 10.0,
    "maker_base_spread_bps": 4.0,
    "maker_wide_spread_bps": 12.0,
    "maker_order_notional": 30.0,
    "maker_max_long_notional": 300.0,
    "maker_max_short_notional": 300.0,
    "maker_inventory_soft_ratio": 0.7,
    "maker_volatility_window": "1m",
    "maker_volatility_wide_threshold": 0.006,
    "maker_extreme_volatility_threshold": 0.012,
    "maker_directional_move_threshold": 0.004,
    "maker_cooldown_seconds": 30.0,
    "neutral_center_interval_minutes": 3,
    "synthetic_center_fast_catchup_trigger_steps": 6.0,
    "synthetic_center_fast_catchup_confirm_cycles": 3,
    "synthetic_center_fast_catchup_shift_steps": 2,
    "neutral_band1_offset_ratio": 0.005,
    "neutral_band2_offset_ratio": 0.01,
    "neutral_band3_offset_ratio": 0.02,
    "neutral_band1_target_ratio": 0.20,
    "neutral_band2_target_ratio": 0.50,
    "neutral_band3_target_ratio": 1.00,
    "neutral_hourly_scale_enabled": False,
    "neutral_hourly_scale_stable": 1.0,
    "neutral_hourly_scale_transition": 0.85,
    "neutral_hourly_scale_defensive": 0.65,
}

COMMON_RUNNER_PARAMS = frozenset(
    {
        "symbol",
        "strategy_profile",
        "strategy_mode",
        "required_position_mode",
        "strict_strategy_profile_schema_enabled",
        "margin_type",
        "leverage",
        "recv_window",
        "max_plan_age_seconds",
        "max_mid_drift_steps",
        "max_new_orders",
        "max_total_notional",
        "reconcile_interval_cycles",
        "maker_retries",
        "cancel_stale",
        "apply",
        "sleep_seconds",
        "iterations",
        "max_consecutive_errors",
        "state_path",
        "plan_json",
        "submit_report_json",
        "summary_jsonl",
        "run_start_time",
        "run_end_time",
        "runtime_guard_stats_start_time",
        "rolling_hourly_loss_limit",
        "max_cumulative_notional",
        "max_actual_net_notional",
        "max_synthetic_drift_notional",
        "reset_state",
        "market_stream",
    }
)

BEST_QUOTE_CORE_PARAMS = frozenset(
    {
        "step_price",
        "buy_levels",
        "sell_levels",
        "per_order_notional",
        "startup_entry_multiplier",
        "base_position_notional",
        "sticky_entry_levels",
        "first_order_multiplier",
        "max_order_position_notional",
        "center_price",
        "flat_start_enabled",
        "warm_start_enabled",
        "fixed_center_enabled",
        "fixed_center_roll_enabled",
        "fixed_center_roll_trigger_steps",
        "fixed_center_roll_confirm_cycles",
        "fixed_center_roll_shift_steps",
        "down_trigger_steps",
        "up_trigger_steps",
        "shift_steps",
        "pause_buy_position_notional",
        "pause_short_position_notional",
        "inventory_pause_long_probe_scale",
        "inventory_pause_short_probe_scale",
        "max_position_notional",
        "max_short_position_notional",
        "take_profit_min_profit_ratio",
    }
)

ELASTIC_PARAMS = frozenset(
    {
        "elastic_volume_enabled",
        "elastic_volume_mode",
        "elastic_loss_per_10k_sprint",
        "elastic_loss_per_10k_cruise",
        "elastic_loss_per_10k_defensive",
        "elastic_loss_per_10k_cooldown",
        "elastic_inventory_soft_ratio",
        "elastic_inventory_hard_ratio",
        "elastic_step_scale_sprint",
        "elastic_step_scale_defensive",
        "elastic_step_scale_cooldown",
        "elastic_per_order_scale_sprint",
        "elastic_per_order_scale_defensive",
        "elastic_levels_scale_sprint",
        "elastic_levels_scale_defensive",
        "elastic_cooldown_seconds",
        "elastic_state_confirm_cycles",
        "elastic_inventory_recover_exit_ratio",
        "elastic_repair_stale_cycles",
        "elastic_adverse_move_ticks",
        "elastic_adverse_move_bps",
        "elastic_repair_slice_ratio_passive",
        "elastic_repair_slice_ratio_touch",
        "elastic_repair_slice_ratio_near_cross",
        "elastic_repair_slice_ratio_cross",
        "elastic_max_repair_loss_per_10k",
        "elastic_cancel_stale_entries_on_cooldown",
    }
)

RISK_REPAIR_PARAMS = frozenset(
    {
        "hard_loss_forced_reduce_enabled",
        "hard_loss_forced_reduce_target_notional",
        "hard_loss_forced_reduce_max_order_notional",
        "hard_loss_forced_reduce_unrealized_loss_limit",
        "adverse_reduce_enabled",
        "adverse_reduce_short_trigger_ratio",
        "adverse_reduce_long_trigger_ratio",
        "adverse_reduce_target_ratio",
        "adverse_reduce_maker_timeout_seconds",
        "adverse_reduce_max_order_notional",
        "adverse_reduce_keep_probe_scale",
        "exposure_escalation_enabled",
        "exposure_escalation_notional",
        "exposure_escalation_hold_seconds",
        "exposure_escalation_target_notional",
        "exposure_escalation_max_loss_ratio",
        "exposure_escalation_hard_unrealized_loss_limit",
        "exposure_escalation_buy_pause_cooldown_seconds",
        "threshold_position_notional",
        "short_threshold_timeout_seconds",
        "inventory_pause_timeout_seconds",
        "near_market_entry_max_center_distance_steps",
        "grid_inventory_rebalance_min_center_distance_steps",
        "near_market_reentry_confirm_cycles",
        "threshold_reduce_target_ratio",
        "threshold_reduce_taker_timeout_seconds",
        "excess_inventory_reduce_only_enabled",
    }
)

BEST_QUOTE_RUNTIME_SWITCHES = frozenset(
    {
        "elastic_volume_enabled",
    }
)
BEST_QUOTE_ALLOWED_PARAMS = (
    COMMON_RUNNER_PARAMS
    | BEST_QUOTE_CORE_PARAMS
    | BEST_QUOTE_RUNTIME_SWITCHES
    | ELASTIC_PARAMS
)

SYNTHETIC_PING_PONG_RUNTIME_SWITCHES = frozenset(
    {
        "market_bias_enabled",
        "market_bias_regime_switch_enabled",
        "multi_timeframe_bias_enabled",
        "synthetic_flow_sleeve_enabled",
        "synthetic_trend_follow_enabled",
        "elastic_volume_enabled",
        "hard_loss_forced_reduce_enabled",
        "adverse_reduce_enabled",
        "exposure_escalation_enabled",
    }
)
SYNTHETIC_PING_PONG_ALLOWED_PARAMS = (
    COMMON_RUNNER_PARAMS
    | BEST_QUOTE_CORE_PARAMS
    | SYNTHETIC_PING_PONG_RUNTIME_SWITCHES
    | ELASTIC_PARAMS
    | RISK_REPAIR_PARAMS
    | frozenset(
        {
            "static_buy_offset_steps",
            "static_sell_offset_steps",
            "synthetic_residual_long_flat_notional",
            "synthetic_residual_short_flat_notional",
            "synthetic_tiny_long_residual_notional",
            "synthetic_tiny_short_residual_notional",
            "synthetic_flow_sleeve_trigger_notional",
            "synthetic_flow_sleeve_notional",
            "synthetic_flow_sleeve_levels",
            "synthetic_flow_sleeve_order_notional",
            "synthetic_flow_sleeve_max_loss_ratio",
            "synthetic_trend_follow_1m_abs_return_ratio",
            "synthetic_trend_follow_1m_amplitude_ratio",
            "synthetic_trend_follow_3m_abs_return_ratio",
            "synthetic_trend_follow_3m_amplitude_ratio",
            "synthetic_trend_follow_min_efficiency_ratio",
            "synthetic_trend_follow_reverse_delay_seconds",
            "market_bias_max_shift_steps",
            "market_bias_signal_steps",
            "market_bias_drift_weight",
            "market_bias_return_weight",
            "market_bias_weak_buy_pause_enabled",
            "market_bias_weak_buy_pause_threshold",
            "market_bias_weak_buy_probe_scale",
            "market_bias_strong_short_pause_enabled",
            "market_bias_strong_short_pause_threshold",
            "market_bias_strong_short_probe_scale",
            "market_bias_regime_switch_confirm_cycles",
            "market_bias_regime_switch_weak_threshold",
            "market_bias_regime_switch_strong_threshold",
        }
    )
)
HEDGE_BQ_PING_PONG_RUNTIME_SWITCHES = SYNTHETIC_PING_PONG_RUNTIME_SWITCHES
HEDGE_BQ_PING_PONG_ALLOWED_PARAMS = SYNTHETIC_PING_PONG_ALLOWED_PARAMS

ONE_WAY_LONG_RUNTIME_SWITCHES = frozenset(
    {
        "auto_regime_enabled",
        "multi_timeframe_bias_enabled",
        "volume_long_v4_flow_sleeve_enabled",
        "elastic_volume_enabled",
        "hard_loss_forced_reduce_enabled",
        "adverse_reduce_enabled",
        "exposure_escalation_enabled",
    }
)
ONE_WAY_LONG_ALLOWED_PARAMS = COMMON_RUNNER_PARAMS | BEST_QUOTE_CORE_PARAMS | ONE_WAY_LONG_RUNTIME_SWITCHES | ELASTIC_PARAMS | RISK_REPAIR_PARAMS


def _safe_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _values_equal(left: Any, right: Any) -> bool:
    if isinstance(left, bool) or isinstance(right, bool):
        return _safe_bool(left) == _safe_bool(right)
    if left is None or right is None:
        return left is right
    if isinstance(left, (int, float)) or isinstance(right, (int, float)):
        try:
            return abs(float(left) - float(right)) <= 1e-12
        except (TypeError, ValueError):
            return left == right
    return left == right


def _is_active_unknown_value(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return abs(float(value)) > 1e-12
    if isinstance(value, str):
        return value.strip() != ""
    if isinstance(value, (list, tuple, set, dict)):
        return len(value) > 0
    return True


def resolve_strategy_profile_schema(*, strategy_mode: str, strategy_profile: str) -> StrategyProfileSchema:
    mode = str(strategy_mode or "").strip()
    profile = str(strategy_profile or "").strip()
    profile_lower = profile.lower()
    is_hedge_bq_profile = (
        profile_lower.startswith(("hedge_bq", "hedge-bq"))
        or "_hedge_bq" in profile_lower
        or "-hedge-bq" in profile_lower
    )

    if mode == "hedge_neutral" or is_hedge_bq_profile:
        return StrategyProfileSchema(
            family="hedge_bq_ping_pong",
            intent="volume",
            required_position_mode=HEDGE_POSITION_MODE,
            allowed_runtime_switches=HEDGE_BQ_PING_PONG_RUNTIME_SWITCHES,
            allowed_params=frozenset(HEDGE_BQ_PING_PONG_ALLOWED_PARAMS),
        )

    if (
        profile in {"ethusdc_best_quote_long_ping_pong_v1", "btcusdc_best_quote_long_ping_pong_v1"}
        or profile_lower.endswith("_best_quote_maker_volume_v1")
        or profile_lower.endswith("_competition_neutral_ping_pong_v1")
        or profile_lower == "robo_best_quote_short_v1"
    ):
        return StrategyProfileSchema(
            family="best_quote",
            intent="volume",
            required_position_mode=ONE_WAY_POSITION_MODE,
            allowed_runtime_switches=BEST_QUOTE_RUNTIME_SWITCHES,
            allowed_params=frozenset(BEST_QUOTE_ALLOWED_PARAMS),
        )

    if mode == "synthetic_neutral" and ("ping_pong" in profile_lower or "ping-pong" in profile_lower):
        return StrategyProfileSchema(
            family="synthetic_ping_pong",
            intent="volume",
            required_position_mode=ONE_WAY_POSITION_MODE,
            allowed_runtime_switches=SYNTHETIC_PING_PONG_RUNTIME_SWITCHES,
            allowed_params=frozenset(SYNTHETIC_PING_PONG_ALLOWED_PARAMS),
        )

    if mode == "one_way_long":
        return StrategyProfileSchema(
            family="one_way_long",
            intent="volume",
            required_position_mode=ONE_WAY_POSITION_MODE,
            allowed_runtime_switches=ONE_WAY_LONG_RUNTIME_SWITCHES,
            allowed_params=frozenset(ONE_WAY_LONG_ALLOWED_PARAMS),
        )

    return StrategyProfileSchema(
        family="unknown",
        intent="unknown",
        required_position_mode=ONE_WAY_POSITION_MODE,
        allowed_runtime_switches=frozenset(RUNTIME_SWITCH_DEFAULTS),
        allowed_params=frozenset(COMMON_RUNNER_PARAMS),
        required_position_mode_defaulted=True,
    )


def apply_strategy_profile_schema(
    args: argparse.Namespace,
    *,
    enabled: bool,
) -> tuple[argparse.Namespace, dict[str, Any]]:
    effective = argparse.Namespace(**vars(args))
    strategy_mode = str(getattr(args, "strategy_mode", "") or "").strip()
    strategy_profile = str(getattr(args, "strategy_profile", "") or "").strip()
    schema = resolve_strategy_profile_schema(strategy_mode=strategy_mode, strategy_profile=strategy_profile)
    ignored_params: list[str] = []
    unknown_params: list[str] = []

    if enabled and schema.family != "unknown":
        for key, default_value in PARAMETER_NEUTRAL_DEFAULTS.items():
            if key in schema.allowed_params or not hasattr(effective, key):
                continue
            current_value = getattr(effective, key)
            if not _values_equal(current_value, default_value):
                ignored_params.append(key)
            setattr(effective, key, default_value)
        known_params = schema.allowed_params | frozenset(PARAMETER_NEUTRAL_DEFAULTS)
        for key, value in vars(args).items():
            if key.startswith("_") or key in known_params:
                continue
            if _is_active_unknown_value(value):
                unknown_params.append(key)

    report = {
        "enabled": True,
        "strict_enabled": bool(enabled),
        "strict_ok": not unknown_params,
        "schema_known": schema.family != "unknown",
        "strategy_mode": strategy_mode,
        "strategy_profile": strategy_profile,
        "profile_family": schema.family,
        "strategy_intent": schema.intent,
        "required_position_mode": schema.required_position_mode,
        "required_position_mode_defaulted": schema.required_position_mode_defaulted,
        "allowed_runtime_switches": sorted(schema.allowed_runtime_switches),
        "allowed_params": sorted(schema.allowed_params),
        "ignored_params": sorted(ignored_params),
        "unknown_params": sorted(unknown_params),
    }
    return effective, report
