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
        "gross_notional_15m": 100_000.0,
        "net_pnl_15m": -2.0,
        "competition_gross_notional": 500_000.0,
        "competition_net_pnl": -20.0,
        "competition_commission": 0.0,
        "long_notional": 50.0,
        "short_notional": 30.0,
        "max_long_notional": 500.0,
        "max_short_notional": 500.0,
        "actual_net_notional": 20.0,
        "adaptive_step_raw_scale": 0.5,
        "multi_timeframe_bias_regime": "balanced",
        "competition_required_pace": 50_000.0,
        "competition_actual_pace": 40_000.0,
    }
    data.update(overrides)
    return ElasticVolumeInputs(**data)


class CompetitionElasticVolumeTests(unittest.TestCase):
    def test_disabled_returns_passthrough_cruise(self) -> None:
        control = resolve_elastic_volume_control(
            config=ElasticVolumeConfig(enabled=False),
            inputs=_inputs(),
        )

        self.assertFalse(control["enabled"])
        self.assertEqual(control["regime"], "disabled")
        self.assertEqual(control["step_scale"], 1.0)
        self.assertTrue(control["entry_allowed"])

    def test_low_loss_low_volatility_enters_sprint(self) -> None:
        control = resolve_elastic_volume_control(
            config=ElasticVolumeConfig(enabled=True),
            inputs=_inputs(net_pnl_15m=-2.0, adaptive_step_raw_scale=0.4),
        )

        self.assertEqual(control["regime"], "sprint")
        self.assertLess(control["step_scale"], 1.0)
        self.assertGreater(control["per_order_scale"], 1.0)
        self.assertIn("low_loss", control["reasons"])

    def test_normal_loss_enters_cruise(self) -> None:
        control = resolve_elastic_volume_control(
            config=ElasticVolumeConfig(enabled=True),
            inputs=_inputs(net_pnl_15m=-6.0, adaptive_step_raw_scale=0.9),
        )

        self.assertEqual(control["regime"], "cruise")
        self.assertEqual(control["step_scale"], 1.0)

    def test_worse_loss_enters_defensive(self) -> None:
        control = resolve_elastic_volume_control(
            config=ElasticVolumeConfig(enabled=True),
            inputs=_inputs(net_pnl_15m=-10.0),
        )

        self.assertEqual(control["regime"], "defensive")
        self.assertGreater(control["step_scale"], 1.0)
        self.assertLess(control["per_order_scale"], 1.0)
        self.assertIn("loss_per_10k_15m", control["reasons"])

    def test_soft_inventory_enters_recover_and_blocks_risk_side(self) -> None:
        control = resolve_elastic_volume_control(
            config=ElasticVolumeConfig(enabled=True),
            inputs=_inputs(long_notional=330.0, short_notional=20.0, actual_net_notional=310.0),
        )

        self.assertEqual(control["regime"], "recover")
        self.assertFalse(control["allow_entry_long"])
        self.assertTrue(control["allow_reduce_long"])
        self.assertTrue(control["allow_entry_short"])

    def test_hard_inventory_enters_cooldown(self) -> None:
        control = resolve_elastic_volume_control(
            config=ElasticVolumeConfig(enabled=True),
            inputs=_inputs(short_notional=455.0, long_notional=20.0, actual_net_notional=-435.0),
        )

        self.assertEqual(control["regime"], "cooldown")
        self.assertFalse(control["entry_allowed"])
        self.assertFalse(control["allow_entry_short"])
        self.assertTrue(control["allow_reduce_short"])
        self.assertIn("inventory_hard", control["reasons"])

    def test_cooldown_persists_until_until_time(self) -> None:
        cooldown_until = (NOW + timedelta(seconds=60)).isoformat()
        control = resolve_elastic_volume_control(
            config=ElasticVolumeConfig(enabled=True),
            inputs=_inputs(last_state={"regime": "cooldown", "cooldown_until": cooldown_until}),
        )

        self.assertEqual(control["regime"], "cooldown")
        self.assertIn("cooldown_active", control["reasons"])

    def test_inventory_priority_over_low_long_bias(self) -> None:
        control = resolve_elastic_volume_control(
            config=ElasticVolumeConfig(enabled=True),
            inputs=_inputs(
                long_notional=330.0,
                short_notional=20.0,
                actual_net_notional=310.0,
                multi_timeframe_bias_regime="low_long_bias",
            ),
        )

        self.assertEqual(control["regime"], "recover")
        self.assertFalse(control["allow_entry_long"])
        self.assertTrue(control["allow_entry_short"])
        self.assertIn("inventory_over_bias", control["reasons"])

    def test_soft_inventory_reports_passive_repair_ladder(self) -> None:
        control = resolve_elastic_volume_control(
            config=ElasticVolumeConfig(enabled=True),
            inputs=_inputs(
                long_notional=330.0,
                short_notional=20.0,
                actual_net_notional=310.0,
                mid_price=1.000,
                tick_size=0.001,
            ),
        )

        self.assertEqual(control["regime"], "recover")
        self.assertEqual(control["strategy_intent"], "repair_inventory")
        self.assertEqual(control["active_state"], "RECOVERY_SAFE")
        self.assertEqual(control["repair_ladder_level"], "passive")
        self.assertEqual(control["repair_ladder"]["side"], "SELL")
        self.assertAlmostEqual(control["repair_ladder"]["slice_ratio"], 0.10)

    def test_stale_adverse_repair_upgrades_to_touch(self) -> None:
        control = resolve_elastic_volume_control(
            config=ElasticVolumeConfig(enabled=True, repair_stale_cycles=3),
            inputs=_inputs(
                last_state={
                    "regime": "recover",
                    "repair_reference_price": 100.000,
                    "repair_stale_cycles": 3,
                },
                long_notional=330.0,
                short_notional=20.0,
                actual_net_notional=310.0,
                mid_price=99.996,
                tick_size=0.001,
            ),
        )

        self.assertEqual(control["repair_ladder_level"], "touch")
        self.assertEqual(control["active_state"], "ADVERSE_REPAIR")
        self.assertIn("repair_stale_adverse", control["reasons"])
        self.assertGreaterEqual(control["repair_ladder"]["adverse_move_ticks"], 3.0)

    def test_large_adverse_move_upgrades_to_near_cross(self) -> None:
        control = resolve_elastic_volume_control(
            config=ElasticVolumeConfig(enabled=True, repair_stale_cycles=3),
            inputs=_inputs(
                last_state={
                    "regime": "recover",
                    "repair_reference_price": 100.000,
                    "repair_stale_cycles": 5,
                },
                short_notional=330.0,
                long_notional=20.0,
                actual_net_notional=-310.0,
                mid_price=100.010,
                tick_size=0.001,
            ),
        )

        self.assertEqual(control["repair_ladder_level"], "near_cross")
        self.assertEqual(control["repair_ladder"]["side"], "BUY")
        self.assertAlmostEqual(control["repair_ladder"]["slice_ratio"], 0.30)

    def test_hard_inventory_reports_cross_repair(self) -> None:
        control = resolve_elastic_volume_control(
            config=ElasticVolumeConfig(enabled=True),
            inputs=_inputs(
                short_notional=455.0,
                long_notional=20.0,
                actual_net_notional=-435.0,
                mid_price=1.010,
                tick_size=0.001,
            ),
        )

        self.assertEqual(control["regime"], "cooldown")
        self.assertEqual(control["strategy_intent"], "repair_inventory")
        self.assertEqual(control["active_state"], "ADVERSE_REPAIR")
        self.assertEqual(control["repair_ladder_level"], "cross")
        self.assertEqual(control["repair_ladder"]["side"], "BUY")


if __name__ == "__main__":
    unittest.main()
