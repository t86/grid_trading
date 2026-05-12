from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from grid_optimizer.competition_elastic_volume import (
    ElasticVolumeConfig,
    ElasticVolumeInputs,
    resolve_elastic_volume_control,
)


NOW = datetime(2026, 5, 7, 8, 0, tzinfo=timezone.utc)


def _inputs(**overrides):
    data = {
        "now": NOW,
        "last_state": {},
        "gross_notional_5m": 50_000.0,
        "net_pnl_5m": -1.0,
        "commission_5m": 0.0,
        "gross_notional_15m": 100_000.0,
        "net_pnl_15m": -2.0,
        "commission_15m": 0.0,
        "competition_gross_notional": 500_000.0,
        "competition_net_pnl": -20.0,
        "competition_commission": 0.0,
        "long_notional": 50.0,
        "short_notional": 30.0,
        "threshold_position_notional": 1_500.0,
        "max_long_notional": 500.0,
        "max_short_notional": 500.0,
        "actual_net_notional": 20.0,
        "adaptive_step_raw_scale": 0.5,
        "volatility_1m_amplitude_ratio": 0.004,
        "volatility_5m_amplitude_ratio": 0.007,
        "multi_timeframe_bias_regime": "balanced",
        "competition_required_pace": 50_000.0,
        "competition_actual_pace": 40_000.0,
    }
    data.update(overrides)
    return ElasticVolumeInputs(**data)


class CompetitionElasticVolumeTests(unittest.TestCase):
    def test_four_state_defaults_expose_base_step_multipliers(self) -> None:
        config = ElasticVolumeConfig(enabled=True)

        self.assertEqual(config.base_step_multiplier_ping_pong_fast, 0.8)
        self.assertEqual(config.base_step_multiplier_ping_pong_safe, 1.2)
        self.assertEqual(config.base_step_multiplier_wide_step_attack, 2.2)
        self.assertEqual(config.base_step_multiplier_wide_step, 2.5)
        self.assertEqual(config.base_step_multiplier_defensive, 3.5)
        self.assertEqual(config.threshold_scale_wide_step, 1.5)
        self.assertEqual(config.pause_scale_defensive, 1.8)

    def test_disabled_returns_passthrough_cruise(self) -> None:
        control = resolve_elastic_volume_control(
            config=ElasticVolumeConfig(enabled=False),
            inputs=_inputs(),
        )

        self.assertFalse(control["enabled"])
        self.assertEqual(control["regime"], "disabled")
        self.assertEqual(control["step_scale"], 1.0)
        self.assertTrue(control["entry_allowed"])

    def test_low_vol_low_inventory_enters_ping_pong_fast(self) -> None:
        control = resolve_elastic_volume_control(
            config=ElasticVolumeConfig(enabled=True),
            inputs=_inputs(
                net_pnl_15m=-2.0,
                adaptive_step_raw_scale=0.4,
                long_notional=100.0,
                short_notional=60.0,
                max_long_notional=5_000.0,
                max_short_notional=5_000.0,
                volatility_1m_amplitude_ratio=0.0020,
                volatility_5m_amplitude_ratio=0.0050,
            ),
        )

        self.assertEqual(control["regime"], "ping-pong-fast")
        self.assertLess(control["step_scale"], 1.0)
        self.assertEqual(control["per_order_scale"], 1.0)
        self.assertEqual(control["threshold_scale"], 1.0)

    def test_moderate_volatility_enters_ping_pong_safe(self) -> None:
        control = resolve_elastic_volume_control(
            config=ElasticVolumeConfig(enabled=True),
            inputs=_inputs(
                long_notional=300.0,
                short_notional=220.0,
                max_long_notional=5_000.0,
                max_short_notional=5_000.0,
                adaptive_step_raw_scale=0.4,
                volatility_1m_amplitude_ratio=0.0050,
                volatility_5m_amplitude_ratio=0.0100,
            ),
        )

        self.assertEqual(control["regime"], "ping-pong-safe")
        self.assertGreater(control["step_scale"], 1.0)
        self.assertLess(control["step_scale"], 2.5)

    def test_ping_pong_fast_hard_inventory_jumps_to_wide_step(self) -> None:
        control = resolve_elastic_volume_control(
            config=ElasticVolumeConfig(enabled=True),
            inputs=_inputs(
                long_notional=700.0,
                short_notional=120.0,
                actual_net_notional=580.0,
                max_long_notional=2_000.0,
                max_short_notional=2_000.0,
                volatility_1m_amplitude_ratio=0.0030,
                volatility_5m_amplitude_ratio=0.0060,
            ),
        )

        self.assertEqual(control["regime"], "wide-step")
        self.assertGreater(control["step_scale"], 2.0)
        self.assertAlmostEqual(control["threshold_scale"], 1.5)
        self.assertAlmostEqual(control["pause_scale"], 1.5)
        self.assertAlmostEqual(control["position_limit_scale"], 1.5)

    def test_light_inventory_high_volatility_enters_wide_step_attack(self) -> None:
        control = resolve_elastic_volume_control(
            config=ElasticVolumeConfig(enabled=True),
            inputs=_inputs(
                long_notional=180.0,
                short_notional=120.0,
                actual_net_notional=60.0,
                max_long_notional=4_000.0,
                max_short_notional=4_000.0,
                volatility_1m_amplitude_ratio=0.0105,
                volatility_5m_amplitude_ratio=0.0140,
            ),
        )

        self.assertEqual(control["regime"], "wide-step-attack")
        self.assertGreater(control["per_order_scale"], 1.0)
        self.assertGreater(control["levels_scale"], 1.0)
        self.assertAlmostEqual(control["threshold_scale"], 1.5)
        self.assertEqual(control["max_entry_long_orders"], 4)

    def test_heavier_inventory_high_volatility_stays_on_conservative_wide_step(self) -> None:
        control = resolve_elastic_volume_control(
            config=ElasticVolumeConfig(enabled=True),
            inputs=_inputs(
                long_notional=900.0,
                short_notional=250.0,
                actual_net_notional=650.0,
                max_long_notional=4_000.0,
                max_short_notional=4_000.0,
                volatility_1m_amplitude_ratio=0.0105,
                volatility_5m_amplitude_ratio=0.0140,
            ),
        )

        self.assertEqual(control["regime"], "wide-step")
        self.assertLess(control["per_order_scale"], 1.0)
        self.assertLess(control["levels_scale"], 1.0)
        self.assertEqual(control["max_entry_long_orders"], 3)

    def test_wide_step_uses_scaled_threshold_before_escalating_to_defensive(self) -> None:
        control = resolve_elastic_volume_control(
            config=ElasticVolumeConfig(
                enabled=True,
                inventory_hard_ratio_ping_pong_fast=0.45,
                inventory_hard_ratio_ping_pong_safe=0.60,
                inventory_hard_ratio_wide_step=0.80,
                threshold_scale_wide_step=1.5,
            ),
            inputs=_inputs(
                threshold_position_notional=825.0,
                short_notional=686.94,
                long_notional=0.0,
                actual_net_notional=-686.94,
                max_long_notional=1_650.0,
                max_short_notional=1_650.0,
                volatility_1m_amplitude_ratio=0.0038,
                volatility_5m_amplitude_ratio=0.0098,
            ),
        )

        self.assertEqual(control["regime"], "wide-step")
        self.assertAlmostEqual(control["threshold_scale"], 1.5)

    def test_high_inventory_or_extreme_volatility_enters_defensive(self) -> None:
        control = resolve_elastic_volume_control(
            config=ElasticVolumeConfig(enabled=True),
            inputs=_inputs(
                long_notional=1_200.0,
                short_notional=50.0,
                actual_net_notional=1_150.0,
                max_long_notional=1_500.0,
                max_short_notional=1_500.0,
                volatility_1m_amplitude_ratio=0.0200,
                volatility_5m_amplitude_ratio=0.0400,
            ),
        )

        self.assertEqual(control["regime"], "defensive")
        self.assertGreater(control["step_scale"], 1.0)
        self.assertLess(control["per_order_scale"], 1.0)
        self.assertTrue(control["reasons"])
        self.assertAlmostEqual(control["threshold_scale"], 1.8)

    def test_ping_pong_safe_recovery_confirms_before_fast(self) -> None:
        control = resolve_elastic_volume_control(
            config=ElasticVolumeConfig(enabled=True, state_confirm_cycles=3),
            inputs=_inputs(
                last_state={"regime": "ping-pong-safe", "pending_regime": "ping-pong-fast", "pending_count": 1},
                long_notional=120.0,
                short_notional=50.0,
                max_long_notional=5_000.0,
                max_short_notional=5_000.0,
                volatility_1m_amplitude_ratio=0.0020,
                volatility_5m_amplitude_ratio=0.0050,
            ),
        )

        self.assertEqual(control["regime"], "ping-pong-safe")
        self.assertEqual(control["pending_regime"], "ping-pong-fast")
        self.assertEqual(control["pending_count"], 2)

    def test_hard_inventory_enters_defensive(self) -> None:
        control = resolve_elastic_volume_control(
            config=ElasticVolumeConfig(enabled=True),
            inputs=_inputs(
                short_notional=1_950.0,
                long_notional=20.0,
                actual_net_notional=-1_930.0,
                max_long_notional=2_000.0,
                max_short_notional=2_000.0,
            ),
        )

        self.assertEqual(control["regime"], "defensive")
        self.assertFalse(control["allow_entry_short"])
        self.assertTrue(control["allow_reduce_short"])
        self.assertIn("inventory_hard", control["reasons"])

    def test_disabled_state_keeps_metrics_with_volatility_fields(self) -> None:
        cooldown_until = (NOW + timedelta(seconds=60)).isoformat()
        control = resolve_elastic_volume_control(
            config=ElasticVolumeConfig(enabled=True),
            inputs=_inputs(last_state={"regime": "cooldown", "cooldown_until": cooldown_until}),
        )

        self.assertIn("volatility_1m_amplitude_ratio", control["metrics"])
        self.assertIn("volatility_5m_amplitude_ratio", control["metrics"])

    def test_defensive_requires_confirmed_recovery_before_ping_pong_safe(self) -> None:
        control = resolve_elastic_volume_control(
            config=ElasticVolumeConfig(enabled=True, state_confirm_cycles=3),
            inputs=_inputs(
                last_state={"regime": "defensive", "pending_regime": "ping-pong-safe", "pending_count": 1},
                net_pnl_15m=-6.0,
                adaptive_step_raw_scale=0.9,
                volatility_1m_amplitude_ratio=0.0045,
                volatility_5m_amplitude_ratio=0.0070,
            ),
        )

        self.assertEqual(control["regime"], "defensive")
        self.assertEqual(control["pending_regime"], "ping-pong-safe")
        self.assertEqual(control["pending_count"], 2)
        self.assertIn("recovery_confirming", control["reasons"])

    def test_defensive_recovers_after_confirm_cycles(self) -> None:
        control = resolve_elastic_volume_control(
            config=ElasticVolumeConfig(enabled=True, state_confirm_cycles=3),
            inputs=_inputs(
                last_state={"regime": "defensive", "pending_regime": "ping-pong-safe", "pending_count": 2},
                net_pnl_15m=-6.0,
                adaptive_step_raw_scale=0.9,
                volatility_1m_amplitude_ratio=0.0045,
                volatility_5m_amplitude_ratio=0.0070,
            ),
        )

        self.assertEqual(control["regime"], "ping-pong-safe")
        self.assertEqual(control["pending_regime"], None)
        self.assertEqual(control["pending_count"], 0)

    def test_defensive_tracks_ping_pong_safe_as_recovery_candidate(self) -> None:
        control = resolve_elastic_volume_control(
            config=ElasticVolumeConfig(enabled=True, state_confirm_cycles=3),
            inputs=_inputs(
                last_state={"regime": "defensive"},
                net_pnl_15m=-2.0,
                adaptive_step_raw_scale=0.4,
                volatility_1m_amplitude_ratio=0.0045,
                volatility_5m_amplitude_ratio=0.0070,
            ),
        )

        self.assertEqual(control["regime"], "defensive")
        self.assertEqual(control["pending_regime"], "ping-pong-safe")
        self.assertEqual(control["pending_count"], 1)

    def test_inventory_priority_over_low_long_bias(self) -> None:
        control = resolve_elastic_volume_control(
            config=ElasticVolumeConfig(enabled=True),
            inputs=_inputs(
                long_notional=700.0,
                short_notional=20.0,
                actual_net_notional=680.0,
                multi_timeframe_bias_regime="low_long_bias",
            ),
        )

        self.assertEqual(control["regime"], "wide-step")
        self.assertFalse(control["allow_entry_long"])
        self.assertTrue(control["allow_entry_short"])
        self.assertIn("inventory_over_bias", control["reasons"])

    def test_metrics_include_short_window_loss(self) -> None:
        control = resolve_elastic_volume_control(
            config=ElasticVolumeConfig(enabled=True),
            inputs=_inputs(gross_notional_5m=20_000.0, net_pnl_5m=-3.0),
        )

        self.assertAlmostEqual(control["metrics"]["loss_per_10k_5m"], 1.5)
        self.assertAlmostEqual(control["metrics"]["gross_notional_5m"], 20_000.0)

    def test_ping_pong_soft_inventory_blocks_same_direction_entry(self) -> None:
        control = resolve_elastic_volume_control(
            config=ElasticVolumeConfig(enabled=True, inventory_soft_ratio_ping_pong_safe=0.30),
            inputs=_inputs(
                long_notional=0.0,
                short_notional=0.0,
                actual_net_notional=500.0,
                threshold_position_notional=1_500.0,
                max_long_notional=5_000.0,
                max_short_notional=5_000.0,
                volatility_1m_amplitude_ratio=0.0040,
                volatility_5m_amplitude_ratio=0.0070,
            ),
        )

        self.assertEqual(control["regime"], "ping-pong-safe")
        self.assertFalse(control["allow_entry_long"])
        self.assertTrue(control["allow_entry_short"])
        self.assertIn("ping_pong_inventory_entry_gate", control["reasons"])
        self.assertAlmostEqual(control["metrics"]["directional_long_notional"], 500.0)

    def test_elastic_control_exposes_entry_order_caps(self) -> None:
        control = resolve_elastic_volume_control(
            config=ElasticVolumeConfig(
                enabled=True,
                max_entry_orders_ping_pong_safe=3,
                max_entry_orders_wide_step=2,
            ),
            inputs=_inputs(
                volatility_1m_amplitude_ratio=0.0040,
                volatility_5m_amplitude_ratio=0.0070,
            ),
        )

        self.assertEqual(control["regime"], "ping-pong-safe")
        self.assertEqual(control["max_entry_long_orders"], 3)
        self.assertEqual(control["max_entry_short_orders"], 3)


if __name__ == "__main__":
    unittest.main()
