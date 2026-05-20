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


if __name__ == "__main__":
    unittest.main()
