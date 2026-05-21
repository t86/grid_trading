from __future__ import annotations

import unittest
from argparse import Namespace

from grid_optimizer.strategy_profile_schema import apply_strategy_profile_schema


def _args(**overrides):
    data = {
        "strategy_mode": "one_way_long",
        "strategy_profile": "ethusdc_best_quote_long_ping_pong_v1",
        "per_order_notional": 40.0,
        "buy_levels": 3,
        "sell_levels": 2,
        "max_position_notional": 220.0,
        "max_short_position_notional": 220.0,
        "pause_buy_position_notional": 180.0,
        "pause_short_position_notional": 180.0,
        "auto_regime_enabled": True,
        "market_bias_enabled": True,
        "market_bias_regime_switch_enabled": True,
        "multi_timeframe_bias_enabled": True,
        "synthetic_flow_sleeve_enabled": True,
        "synthetic_trend_follow_enabled": True,
        "volume_long_v4_flow_sleeve_enabled": True,
        "custom_grid_enabled": True,
        "elastic_volume_enabled": True,
        "hard_loss_forced_reduce_enabled": True,
        "adverse_reduce_enabled": True,
        "excess_inventory_reduce_only_enabled": True,
        "exposure_escalation_enabled": True,
        "take_profit_min_profit_ratio": 0.0002,
        "max_new_orders": 12,
        "max_total_notional": 0.0,
        "cancel_stale": True,
        "max_mid_drift_steps": 8.0,
        "rolling_hourly_loss_limit": 0.0,
        "max_cumulative_notional": 0.0,
        "max_actual_net_notional": 0.0,
        "max_synthetic_drift_notional": 0.0,
        "near_market_entry_max_center_distance_steps": 2.0,
        "grid_inventory_rebalance_min_center_distance_steps": 3.0,
        "static_buy_offset_steps": 2.0,
        "static_sell_offset_steps": 3.0,
        "market_bias_max_shift_steps": 4.0,
        "synthetic_flow_sleeve_notional": 120.0,
        "synthetic_flow_sleeve_levels": 3,
        "synthetic_residual_long_flat_notional": 35.0,
        "volume_long_v4_soft_loss_steps": 8.0,
        "custom_grid_n": 12,
    }
    data.update(overrides)
    return Namespace(**data)


class StrategyProfileSchemaTests(unittest.TestCase):
    def test_strict_best_quote_schema_prunes_unrelated_strategy_modules(self) -> None:
        args = _args()
        effective, report = apply_strategy_profile_schema(args, enabled=True)

        self.assertEqual(report["profile_family"], "best_quote")
        self.assertEqual(report["strategy_intent"], "volume")
        self.assertEqual(report["required_position_mode"], "one_way")
        self.assertFalse(report["required_position_mode_defaulted"])
        self.assertTrue(report["strict_enabled"])
        self.assertEqual(effective.per_order_notional, 40.0)
        self.assertEqual(effective.buy_levels, 3)
        self.assertTrue(effective.elastic_volume_enabled)
        self.assertFalse(effective.hard_loss_forced_reduce_enabled)
        self.assertFalse(effective.adverse_reduce_enabled)
        self.assertFalse(effective.excess_inventory_reduce_only_enabled)
        self.assertFalse(effective.exposure_escalation_enabled)
        self.assertFalse(effective.auto_regime_enabled)
        self.assertFalse(effective.market_bias_enabled)
        self.assertFalse(effective.market_bias_regime_switch_enabled)
        self.assertFalse(effective.multi_timeframe_bias_enabled)
        self.assertFalse(effective.synthetic_flow_sleeve_enabled)
        self.assertFalse(effective.synthetic_trend_follow_enabled)
        self.assertFalse(effective.volume_long_v4_flow_sleeve_enabled)
        self.assertFalse(effective.custom_grid_enabled)
        self.assertIn("synthetic_flow_sleeve_enabled", report["ignored_params"])
        self.assertIn("market_bias_enabled", report["ignored_params"])
        self.assertIn("adverse_reduce_enabled", report["ignored_params"])
        self.assertIn("excess_inventory_reduce_only_enabled", report["ignored_params"])

    def test_strict_best_quote_schema_resets_disallowed_scalar_params(self) -> None:
        effective, report = apply_strategy_profile_schema(_args(), enabled=True)

        self.assertEqual(effective.static_buy_offset_steps, 0.0)
        self.assertEqual(effective.static_sell_offset_steps, 0.0)
        self.assertEqual(effective.market_bias_max_shift_steps, 0.75)
        self.assertEqual(effective.synthetic_flow_sleeve_notional, 0.0)
        self.assertEqual(effective.synthetic_flow_sleeve_levels, 0)
        self.assertIsNone(effective.synthetic_residual_long_flat_notional)
        self.assertEqual(effective.volume_long_v4_soft_loss_steps, 0.5)
        self.assertIsNone(effective.custom_grid_n)
        self.assertIn("static_buy_offset_steps", report["ignored_params"])
        self.assertIn("synthetic_flow_sleeve_notional", report["ignored_params"])
        self.assertIn("custom_grid_n", report["ignored_params"])
        self.assertNotIn("per_order_notional", report["ignored_params"])

    def test_strict_best_quote_schema_reports_active_unknown_params(self) -> None:
        effective, report = apply_strategy_profile_schema(
            _args(experimental_leftover_knob=123.0),
            enabled=True,
        )

        self.assertEqual(effective.experimental_leftover_knob, 123.0)
        self.assertIn("experimental_leftover_knob", report["unknown_params"])
        self.assertFalse(report["strict_ok"])

    def test_synthetic_ping_pong_schema_keeps_synthetic_modules(self) -> None:
        effective, report = apply_strategy_profile_schema(
            _args(
                strategy_mode="synthetic_neutral",
                strategy_profile="volume_neutral_ping_pong_v1",
                synthetic_flow_sleeve_enabled=True,
                synthetic_trend_follow_enabled=True,
                market_bias_enabled=True,
                auto_regime_enabled=True,
            ),
            enabled=True,
        )

        self.assertEqual(report["profile_family"], "synthetic_ping_pong")
        self.assertTrue(effective.synthetic_flow_sleeve_enabled)
        self.assertTrue(effective.synthetic_trend_follow_enabled)
        self.assertTrue(effective.market_bias_enabled)
        self.assertFalse(effective.auto_regime_enabled)
        self.assertIn("auto_regime_enabled", report["ignored_params"])

    def test_schema_report_only_when_strict_disabled(self) -> None:
        args = _args()
        effective, report = apply_strategy_profile_schema(args, enabled=False)

        self.assertIsNot(effective, args)
        self.assertFalse(report["strict_enabled"])
        self.assertTrue(effective.market_bias_enabled)
        self.assertEqual(report["ignored_params"], [])

    def test_unknown_legacy_profile_defaults_to_one_way_position_mode(self) -> None:
        _, report = apply_strategy_profile_schema(
            _args(
                strategy_mode="legacy_neutral",
                strategy_profile="legacy_unclassified_profile_v1",
            ),
            enabled=True,
        )

        self.assertEqual(report["profile_family"], "unknown")
        self.assertEqual(report["required_position_mode"], "one_way")
        self.assertTrue(report["required_position_mode_defaulted"])

    def test_hedge_neutral_bq_ping_pong_profile_requires_hedge_mode(self) -> None:
        _, report = apply_strategy_profile_schema(
            _args(
                strategy_mode="hedge_neutral",
                strategy_profile="aigensynusdt_hedge_bq_pingpong_sprint_v1",
            ),
            enabled=True,
        )

        self.assertEqual(report["profile_family"], "hedge_bq_ping_pong")
        self.assertEqual(report["strategy_intent"], "volume")
        self.assertEqual(report["required_position_mode"], "hedge")
        self.assertFalse(report["required_position_mode_defaulted"])

    def test_global_safety_preflight_reports_order_limiters_and_stop_guards(self) -> None:
        _, report = apply_strategy_profile_schema(
            _args(
                buy_levels=2,
                sell_levels=2,
                per_order_notional=750.0,
                max_new_orders=3,
                max_total_notional=2_000.0,
                cancel_stale=False,
                max_mid_drift_steps=1.0,
                rolling_hourly_loss_limit=25.0,
                max_cumulative_notional=100_000.0,
                max_actual_net_notional=8_000.0,
            ),
            enabled=True,
        )

        preflight = report["global_safety_preflight"]
        self.assertEqual(preflight["estimated_cycle_order_count"], 4)
        self.assertEqual(preflight["estimated_cycle_notional"], 3_000.0)
        self.assertIn("max_new_orders", preflight["limiting_params"])
        self.assertIn("max_total_notional", preflight["limiting_params"])
        self.assertIn("cancel_stale", preflight["blocking_params"])
        self.assertIn("max_mid_drift_steps", preflight["warning_params"])
        self.assertIn("rolling_hourly_loss_limit", preflight["stop_guard_params"])
        self.assertIn("max_cumulative_notional", preflight["stop_guard_params"])
        self.assertIn("max_actual_net_notional", preflight["stop_guard_params"])

    def test_global_safety_preflight_reports_invalid_execution_caps_as_blocking(self) -> None:
        _, report = apply_strategy_profile_schema(
            _args(max_new_orders=0, max_total_notional=0.0),
            enabled=True,
        )

        preflight = report["global_safety_preflight"]
        self.assertIn("max_new_orders", preflight["blocking_params"])
        self.assertIn("max_total_notional", preflight["blocking_params"])

    def test_startup_preflight_blocks_strict_unknowns_and_safety_blockers(self) -> None:
        _, report = apply_strategy_profile_schema(
            _args(
                experimental_leftover_knob=123.0,
                max_new_orders=0,
                max_total_notional=0.0,
                cancel_stale=False,
            ),
            enabled=True,
        )

        preflight = report["startup_preflight"]
        self.assertFalse(preflight["can_start"])
        self.assertEqual(preflight["status"], "blocked")
        self.assertIn("strict_unknown_params", preflight["blocker_codes"])
        self.assertIn("global_safety_blocking_params", preflight["blocker_codes"])
        self.assertIn("experimental_leftover_knob", preflight["unknown_params"])
        self.assertIn("max_new_orders", preflight["blocking_params"])
        self.assertIn("max_total_notional", preflight["blocking_params"])
        self.assertIn("cancel_stale", preflight["blocking_params"])

    def test_startup_preflight_warns_for_ignored_params_without_blocking(self) -> None:
        _, report = apply_strategy_profile_schema(
            _args(
                max_total_notional=1_000.0,
                hard_loss_forced_reduce_enabled=True,
                excess_inventory_reduce_only_enabled=True,
            ),
            enabled=True,
        )

        preflight = report["startup_preflight"]
        self.assertTrue(preflight["can_start"])
        self.assertEqual(preflight["status"], "warning")
        self.assertIn("ignored_params", preflight["warning_codes"])
        self.assertIn("hard_loss_forced_reduce_enabled", preflight["ignored_params"])
        self.assertIn("excess_inventory_reduce_only_enabled", preflight["ignored_params"])

    def test_global_safety_preflight_reflects_pruned_disallowed_takeover_switches(self) -> None:
        effective, report = apply_strategy_profile_schema(
            _args(hard_loss_forced_reduce_enabled=True, excess_inventory_reduce_only_enabled=True),
            enabled=True,
        )

        preflight = report["global_safety_preflight"]
        self.assertFalse(effective.hard_loss_forced_reduce_enabled)
        self.assertFalse(effective.excess_inventory_reduce_only_enabled)
        self.assertNotIn("hard_loss_forced_reduce_enabled", preflight["takeover_params"])
        self.assertNotIn("excess_inventory_reduce_only_enabled", preflight["limiting_params"])
        self.assertIn("hard_loss_forced_reduce_enabled", report["ignored_params"])
        self.assertIn("excess_inventory_reduce_only_enabled", report["ignored_params"])

    def test_aigensyn_best_quote_profile_boundary_reports_active_allowed_and_global_safety_params(self) -> None:
        _, report = apply_strategy_profile_schema(
            _args(
                strategy_mode="one_way_long",
                strategy_profile="aigensynusdt_best_quote_maker_volume_v1",
                required_position_mode="one_way",
                buy_levels=1,
                sell_levels=1,
                per_order_notional=750.0,
                pause_short_position_notional=None,
                max_short_position_notional=None,
                max_new_orders=8,
                max_total_notional=3_000.0,
                cancel_stale=True,
                auto_regime_enabled=False,
                elastic_volume_enabled=True,
                market_bias_regime_switch_enabled=False,
                market_bias_enabled=False,
                multi_timeframe_bias_enabled=False,
                synthetic_flow_sleeve_enabled=False,
                synthetic_flow_sleeve_notional=0.0,
                synthetic_flow_sleeve_levels=0,
                synthetic_residual_long_flat_notional=None,
                synthetic_trend_follow_enabled=False,
                volume_long_v4_flow_sleeve_enabled=False,
                volume_long_v4_soft_loss_steps=0.5,
                custom_grid_enabled=False,
                custom_grid_n=None,
                hard_loss_forced_reduce_enabled=False,
                adverse_reduce_enabled=False,
                excess_inventory_reduce_only_enabled=False,
                exposure_escalation_enabled=False,
                static_buy_offset_steps=0.0,
                static_sell_offset_steps=0.0,
                market_bias_max_shift_steps=0.75,
            ),
            enabled=True,
        )

        boundary = report["profile_boundary"]
        self.assertTrue(boundary["overlay_known"])
        self.assertEqual(boundary["profile_key"], "aigensynusdt_best_quote_maker_volume_v1")
        self.assertEqual(boundary["status"], "ready")
        self.assertIn("per_order_notional", boundary["allowed_params"])
        self.assertIn("per_order_notional", boundary["active_allowed_params"])
        self.assertIn("elastic_volume_enabled", boundary["active_allowed_params"])
        self.assertIn("max_new_orders", boundary["global_safety_params"])
        self.assertIn("max_total_notional", boundary["active_global_safety_params"])
        self.assertIn("cancel_stale", boundary["active_global_safety_params"])
        self.assertEqual(boundary["forbidden_active_params"], [])
        self.assertEqual(boundary["required_missing_params"], [])

    def test_pharos_hedge_best_quote_maker_volume_requires_hedge_and_known_overlay(self) -> None:
        _, report = apply_strategy_profile_schema(
            _args(
                symbol="PHAROSUSDT",
                strategy_profile="pharosusdt_hedge_best_quote_maker_volume_v1",
                strategy_mode="hedge_best_quote_maker_volume_v1",
                best_quote_maker_volume_enabled=True,
                best_quote_maker_volume_cycle_budget_notional=40.0,
                best_quote_maker_volume_target_remaining_notional=200_000.0,
                best_quote_maker_volume_quote_offset_ticks=3,
                best_quote_maker_volume_max_long_notional=700.0,
                best_quote_maker_volume_max_short_notional=700.0,
                hard_loss_forced_reduce_enabled=True,
                hard_loss_forced_reduce_target_notional=220.0,
                volatility_entry_pause_enabled=True,
                anti_chase_entry_guard_enabled=True,
                max_total_notional=1450.0,
                required_position_mode="hedge",
            ),
            enabled=True,
        )

        self.assertEqual(report["profile_family"], "hedge_best_quote_maker_volume")
        self.assertEqual(report["required_position_mode"], "hedge")
        self.assertTrue(report["profile_boundary"]["overlay_known"])
        self.assertNotIn("best_quote_maker_volume_enabled", report["unknown_params"])

    def test_pharos_overlay_marks_unrelated_modules_forbidden(self) -> None:
        _, report = apply_strategy_profile_schema(
            _args(
                symbol="PHAROSUSDT",
                strategy_profile="pharosusdt_hedge_best_quote_maker_volume_v1",
                strategy_mode="hedge_best_quote_maker_volume_v1",
                required_position_mode="hedge",
                synthetic_flow_sleeve_enabled=True,
                volume_long_v4_flow_sleeve_enabled=True,
                custom_grid_enabled=True,
                best_quote_maker_volume_enabled=True,
                max_total_notional=1450.0,
            ),
            enabled=True,
        )

        forbidden_active = report["profile_boundary"]["forbidden_active_params"]
        self.assertIn("synthetic_flow_sleeve_enabled", forbidden_active)
        self.assertIn("volume_long_v4_flow_sleeve_enabled", forbidden_active)
        self.assertIn("custom_grid_enabled", forbidden_active)

    def test_pharos_hedge_best_quote_maker_volume_accepts_runtime_param_families(self) -> None:
        _, report = apply_strategy_profile_schema(
            _args(
                symbol="PHAROSUSDT",
                strategy_profile="pharosusdt_hedge_best_quote_maker_volume_v1",
                strategy_mode="hedge_best_quote_maker_volume_v1",
                required_position_mode="hedge",
                best_quote_maker_volume_enabled=True,
                best_quote_maker_volume_dynamic_control_enabled=True,
                best_quote_maker_volume_inventory_bias_enabled=True,
                best_quote_maker_volume_trend_guard_enabled=True,
                best_quote_maker_volume_defensive_offset_ticks=4,
                adaptive_regime_router_enabled=True,
                adaptive_regime_router_mode="auto",
                execution_request_budget_per_cycle=8,
                execution_request_min_interval_seconds=0.05,
                loss_reentry_guard_enabled=True,
                loss_recovery_brush_entry_notional=10.0,
                rolling_hourly_loss_per_10k_limit=3.0,
                unrealized_loss_entry_guard_ratio=0.4,
                volatility_entry_pause_10s_abs_return_ratio=0.0015,
            ),
            enabled=True,
        )

        self.assertTrue(report["strict_ok"])
        self.assertNotIn("best_quote_maker_volume_dynamic_control_enabled", report["unknown_params"])
        self.assertNotIn("adaptive_regime_router_enabled", report["unknown_params"])
        self.assertNotIn("loss_reentry_guard_enabled", report["unknown_params"])

    def test_profile_boundary_reports_forbidden_active_params_without_changing_effective_args(self) -> None:
        effective, report = apply_strategy_profile_schema(
            _args(
                strategy_mode="hedge_neutral",
                strategy_profile="aigensynusdt_hedge_bq_pingpong_sprint_v1",
                required_position_mode="hedge",
                hard_loss_forced_reduce_enabled=True,
                adverse_reduce_enabled=True,
                max_total_notional=3_000.0,
            ),
            enabled=True,
        )

        boundary = report["profile_boundary"]
        self.assertTrue(boundary["overlay_known"])
        self.assertEqual(boundary["status"], "warning")
        self.assertIn("hard_loss_forced_reduce_enabled", boundary["forbidden_active_params"])
        self.assertIn("adverse_reduce_enabled", boundary["forbidden_active_params"])
        self.assertTrue(effective.hard_loss_forced_reduce_enabled)
        self.assertTrue(effective.adverse_reduce_enabled)
        self.assertTrue(report["startup_preflight"]["can_start"])
        self.assertIn("profile_forbidden_active_params", report["startup_preflight"]["warning_codes"])

    def test_profile_boundary_reports_required_missing_params(self) -> None:
        _, report = apply_strategy_profile_schema(
            _args(
                strategy_mode="one_way_long",
                strategy_profile="aigensynusdt_best_quote_maker_volume_v1",
                required_position_mode="one_way",
                per_order_notional=0.0,
                max_total_notional=3_000.0,
            ),
            enabled=True,
        )

        boundary = report["profile_boundary"]
        self.assertEqual(boundary["status"], "blocked")
        self.assertIn("per_order_notional", boundary["required_missing_params"])
        self.assertTrue(report["startup_preflight"]["can_start"])
        self.assertIn("profile_required_missing_params", report["startup_preflight"]["warning_codes"])

    def test_competition_inventory_grid_profile_has_known_overlay(self) -> None:
        _, report = apply_strategy_profile_schema(
            _args(
                strategy_mode="competition_inventory_grid",
                strategy_profile="competition_inventory_grid_v1",
                required_position_mode="one_way",
                buy_levels=0,
                sell_levels=0,
                first_order_multiplier=4.0,
                threshold_position_notional=50.0,
                max_order_position_notional=80.0,
                max_position_notional=120.0,
                pause_short_position_notional=None,
                max_short_position_notional=None,
                max_total_notional=560.0,
                auto_regime_enabled=False,
                market_bias_regime_switch_enabled=False,
                market_bias_enabled=False,
                multi_timeframe_bias_enabled=False,
                synthetic_flow_sleeve_enabled=False,
                synthetic_flow_sleeve_notional=0.0,
                synthetic_flow_sleeve_levels=0,
                synthetic_residual_long_flat_notional=None,
                synthetic_trend_follow_enabled=False,
                volume_long_v4_flow_sleeve_enabled=False,
                volume_long_v4_soft_loss_steps=0.5,
                custom_grid_enabled=False,
                custom_grid_n=None,
                elastic_volume_enabled=False,
                hard_loss_forced_reduce_enabled=False,
                adverse_reduce_enabled=False,
                excess_inventory_reduce_only_enabled=False,
                exposure_escalation_enabled=False,
                static_buy_offset_steps=0.0,
                static_sell_offset_steps=0.0,
                market_bias_max_shift_steps=0.75,
            ),
            enabled=True,
        )

        boundary = report["profile_boundary"]
        self.assertEqual(report["profile_family"], "competition_inventory_grid")
        self.assertTrue(report["schema_known"])
        self.assertTrue(boundary["overlay_known"])
        self.assertEqual(boundary["status"], "ready")
        self.assertIn("first_order_multiplier", boundary["active_allowed_params"])
        self.assertIn("threshold_position_notional", boundary["active_allowed_params"])

    def test_unknown_profile_boundary_warns_but_uses_family_fallback(self) -> None:
        _, report = apply_strategy_profile_schema(
            _args(
                strategy_mode="one_way_long",
                strategy_profile="local_unregistered_one_way_profile_v1",
                required_position_mode="one_way",
                max_total_notional=1_000.0,
            ),
            enabled=True,
        )

        boundary = report["profile_boundary"]
        self.assertEqual(report["profile_family"], "one_way_long")
        self.assertTrue(report["schema_known"])
        self.assertFalse(boundary["overlay_known"])
        self.assertEqual(boundary["status"], "warning")
        self.assertIn("per_order_notional", boundary["allowed_params"])
        self.assertIn("profile_overlay_unknown", report["startup_preflight"]["warning_codes"])

    def test_profile_boundary_active_allowed_params_hide_disabled_module_defaults(self) -> None:
        _, report = apply_strategy_profile_schema(
            _args(
                strategy_mode="one_way_long",
                strategy_profile="volume_long_v4",
                required_position_mode="one_way",
                elastic_volume_enabled=False,
                elastic_repair_stale_cycles=4,
                hard_loss_forced_reduce_enabled=False,
                hard_loss_forced_reduce_unrealized_loss_limit=10.0,
                adverse_reduce_enabled=False,
                adverse_reduce_short_trigger_ratio=0.01,
                exposure_escalation_enabled=False,
                exposure_escalation_hold_seconds=600.0,
                volume_long_v4_flow_sleeve_enabled=False,
                volume_long_v4_soft_loss_steps=0.5,
                max_total_notional=1_000.0,
            ),
            enabled=True,
        )

        active_allowed = set(report["profile_boundary"]["active_allowed_params"])
        self.assertIn("per_order_notional", active_allowed)
        self.assertIn("buy_levels", active_allowed)
        self.assertNotIn("elastic_repair_stale_cycles", active_allowed)
        self.assertNotIn("adverse_reduce_short_trigger_ratio", active_allowed)
        self.assertNotIn("hard_loss_forced_reduce_unrealized_loss_limit", active_allowed)
        self.assertNotIn("exposure_escalation_hold_seconds", active_allowed)
        self.assertNotIn("volume_long_v4_soft_loss_steps", active_allowed)


if __name__ == "__main__":
    unittest.main()
