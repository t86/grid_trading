from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from grid_optimizer.multi_timeframe_bias import (
    MultiTimeframeBiasConfig,
    apply_multi_timeframe_bias,
    resolve_multi_timeframe_bias,
)
from grid_optimizer.types import Candle


def _candle(minutes_ago: int, open_price: float, high: float, low: float, close: float) -> Candle:
    close_time = datetime(2026, 5, 7, 0, 0, tzinfo=timezone.utc) - timedelta(minutes=minutes_ago)
    return Candle(
        open_time=close_time - timedelta(minutes=1),
        close_time=close_time,
        open=open_price,
        high=high,
        low=low,
        close=close,
    )


class MultiTimeframeBiasTests(unittest.TestCase):
    def test_low_zone_bias_favors_buy_long_entries(self) -> None:
        config = MultiTimeframeBiasConfig(enabled=True)
        report = resolve_multi_timeframe_bias(
            current_price=91.0,
            step_price=1.0,
            windows={
                "1m": [_candle(0, 91.5, 92.0, 90.8, 91.0)],
                "15m": [_candle(0, 91.2, 95.0, 90.0, 91.0)],
                "1h": [_candle(0, 91.1, 98.0, 90.0, 91.0)],
                "4h": [_candle(0, 91.0, 110.0, 90.0, 91.0)],
            },
            config=config,
        )

        adjusted = apply_multi_timeframe_bias(
            buy_levels=8,
            sell_levels=8,
            per_order_notional=100.0,
            step_price=1.0,
            max_position_notional=1000.0,
            max_short_position_notional=1000.0,
            report=report,
        )

        self.assertEqual(report["regime"], "low_long_bias")
        self.assertGreater(report["long_bias_score"], 0.5)
        self.assertGreater(adjusted["buy_levels"], adjusted["sell_levels"])
        self.assertGreater(adjusted["max_position_notional"], adjusted["max_short_position_notional"])
        self.assertLess(adjusted["buy_offset_steps"], 0.0)
        self.assertGreater(adjusted["sell_offset_steps"], 0.0)

    def test_high_zone_bias_favors_sell_short_entries(self) -> None:
        report = resolve_multi_timeframe_bias(
            current_price=109.0,
            step_price=1.0,
            windows={
                "1m": [_candle(0, 108.5, 109.2, 108.0, 109.0)],
                "15m": [_candle(0, 108.8, 110.0, 105.0, 109.0)],
                "1h": [_candle(0, 108.9, 110.0, 100.0, 109.0)],
                "4h": [_candle(0, 109.0, 110.0, 90.0, 109.0)],
            },
            config=MultiTimeframeBiasConfig(enabled=True),
        )
        adjusted = apply_multi_timeframe_bias(
            buy_levels=8,
            sell_levels=8,
            per_order_notional=100.0,
            step_price=1.0,
            max_position_notional=1000.0,
            max_short_position_notional=1000.0,
            report=report,
        )

        self.assertEqual(report["regime"], "high_short_bias")
        self.assertGreater(report["short_bias_score"], 0.5)
        self.assertGreater(adjusted["sell_levels"], adjusted["buy_levels"])
        self.assertGreater(adjusted["max_short_position_notional"], adjusted["max_position_notional"])
        self.assertGreater(adjusted["buy_offset_steps"], 0.0)
        self.assertLess(adjusted["sell_offset_steps"], 0.0)

    def test_fast_one_minute_shock_widens_step_and_reduces_notional(self) -> None:
        report = resolve_multi_timeframe_bias(
            current_price=96.0,
            step_price=1.0,
            windows={
                "1m": [_candle(0, 100.0, 100.4, 95.8, 96.0)],
                "15m": [_candle(0, 100.0, 101.0, 95.0, 96.0)],
                "1h": [_candle(0, 100.0, 102.0, 94.0, 96.0)],
                "4h": [_candle(0, 100.0, 110.0, 90.0, 96.0)],
            },
            config=MultiTimeframeBiasConfig(enabled=True),
        )
        adjusted = apply_multi_timeframe_bias(
            buy_levels=8,
            sell_levels=8,
            per_order_notional=100.0,
            step_price=1.0,
            max_position_notional=1000.0,
            max_short_position_notional=1000.0,
            report=report,
        )

        self.assertTrue(report["shock_active"])
        self.assertGreater(adjusted["step_price"], 1.0)
        self.assertLess(adjusted["per_order_notional"], 100.0)

    def test_one_way_long_low_zone_accelerates_long_entries(self) -> None:
        report = resolve_multi_timeframe_bias(
            current_price=91.0,
            step_price=1.0,
            windows={
                "1m": [_candle(0, 91.5, 92.0, 90.8, 91.0)],
                "15m": [_candle(0, 91.2, 95.0, 90.0, 91.0)],
                "1h": [_candle(0, 91.1, 98.0, 90.0, 91.0)],
                "4h": [_candle(0, 91.0, 110.0, 90.0, 91.0)],
            },
            config=MultiTimeframeBiasConfig(enabled=True, mode_adapter="one_way_long"),
        )

        adjusted = apply_multi_timeframe_bias(
            buy_levels=8,
            sell_levels=8,
            per_order_notional=100.0,
            step_price=1.0,
            max_position_notional=1000.0,
            max_short_position_notional=0.0,
            report=report,
            config=MultiTimeframeBiasConfig(enabled=True, mode_adapter="one_way_long"),
        )

        self.assertEqual(adjusted["adapter"], "one_way_long")
        self.assertGreater(adjusted["buy_levels"], 8)
        self.assertEqual(adjusted["sell_levels"], 8)
        self.assertGreater(adjusted["max_position_notional"], 1000.0)
        self.assertEqual(adjusted["max_short_position_notional"], 0.0)
        self.assertLess(adjusted["buy_offset_steps"], 0.0)
        self.assertEqual(adjusted["sell_offset_steps"], 0.0)

    def test_one_way_long_high_zone_de_risks_long_entries(self) -> None:
        report = resolve_multi_timeframe_bias(
            current_price=109.0,
            step_price=1.0,
            windows={
                "1m": [_candle(0, 108.5, 109.2, 108.0, 109.0)],
                "15m": [_candle(0, 109.1, 110.0, 105.0, 109.0)],
                "1h": [_candle(0, 109.0, 110.0, 100.0, 109.0)],
                "4h": [_candle(0, 109.0, 110.0, 90.0, 109.0)],
            },
            config=MultiTimeframeBiasConfig(enabled=True, mode_adapter="one_way_long"),
        )

        adjusted = apply_multi_timeframe_bias(
            buy_levels=8,
            sell_levels=8,
            per_order_notional=100.0,
            step_price=1.0,
            max_position_notional=1000.0,
            max_short_position_notional=0.0,
            report=report,
            config=MultiTimeframeBiasConfig(enabled=True, mode_adapter="one_way_long"),
        )

        self.assertLess(adjusted["buy_levels"], 8)
        self.assertLess(adjusted["per_order_notional"], 100.0)
        self.assertLess(adjusted["max_position_notional"], 1000.0)
        self.assertGreater(adjusted["buy_offset_steps"], 0.0)
        self.assertEqual(adjusted["max_short_position_notional"], 0.0)

    def test_one_way_short_high_zone_accelerates_short_entries(self) -> None:
        report = resolve_multi_timeframe_bias(
            current_price=109.0,
            step_price=1.0,
            windows={
                "1m": [_candle(0, 108.5, 109.2, 108.0, 109.0)],
                "15m": [_candle(0, 109.1, 110.0, 105.0, 109.0)],
                "1h": [_candle(0, 109.0, 110.0, 100.0, 109.0)],
                "4h": [_candle(0, 109.0, 110.0, 90.0, 109.0)],
            },
            config=MultiTimeframeBiasConfig(enabled=True, mode_adapter="one_way_short"),
        )

        adjusted = apply_multi_timeframe_bias(
            buy_levels=8,
            sell_levels=8,
            per_order_notional=100.0,
            step_price=1.0,
            max_position_notional=0.0,
            max_short_position_notional=1000.0,
            report=report,
            config=MultiTimeframeBiasConfig(enabled=True, mode_adapter="one_way_short"),
        )

        self.assertEqual(adjusted["adapter"], "one_way_short")
        self.assertEqual(adjusted["buy_levels"], 8)
        self.assertGreater(adjusted["sell_levels"], 8)
        self.assertEqual(adjusted["max_position_notional"], 0.0)
        self.assertGreater(adjusted["max_short_position_notional"], 1000.0)
        self.assertLess(adjusted["sell_offset_steps"], 0.0)
        self.assertEqual(adjusted["buy_offset_steps"], 0.0)

    def test_inventory_grid_adapter_keeps_both_sides_present(self) -> None:
        report = resolve_multi_timeframe_bias(
            current_price=109.0,
            step_price=1.0,
            windows={
                "1m": [_candle(0, 108.5, 109.2, 108.0, 109.0)],
                "15m": [_candle(0, 109.1, 110.0, 105.0, 109.0)],
                "1h": [_candle(0, 109.0, 110.0, 100.0, 109.0)],
                "4h": [_candle(0, 109.0, 110.0, 90.0, 109.0)],
            },
            config=MultiTimeframeBiasConfig(enabled=True, mode_adapter="inventory_grid"),
        )

        adjusted = apply_multi_timeframe_bias(
            buy_levels=2,
            sell_levels=2,
            per_order_notional=100.0,
            step_price=1.0,
            max_position_notional=1000.0,
            max_short_position_notional=1000.0,
            report=report,
            config=MultiTimeframeBiasConfig(enabled=True, mode_adapter="inventory_grid"),
        )

        self.assertEqual(adjusted["adapter"], "inventory_grid")
        self.assertGreaterEqual(adjusted["buy_levels"], 1)
        self.assertGreaterEqual(adjusted["sell_levels"], 1)
        self.assertGreater(adjusted["sell_levels"], adjusted["buy_levels"])
        self.assertLess(adjusted["max_position_notional"], 1000.0)
        self.assertGreater(adjusted["max_short_position_notional"], 1000.0)

    def test_unavailable_candles_return_base_values(self) -> None:
        report = resolve_multi_timeframe_bias(
            current_price=100.0,
            step_price=1.0,
            windows={"1m": [_candle(0, 100.0, 101.0, 99.0, 100.0)]},
            config=MultiTimeframeBiasConfig(enabled=True, mode_adapter="one_way_long"),
        )
        adjusted = apply_multi_timeframe_bias(
            buy_levels=8,
            sell_levels=8,
            per_order_notional=100.0,
            step_price=1.0,
            max_position_notional=1000.0,
            max_short_position_notional=0.0,
            report=report,
            config=MultiTimeframeBiasConfig(enabled=True, mode_adapter="one_way_long"),
        )

        self.assertFalse(report["available"])
        self.assertFalse(adjusted["applied"])
        self.assertEqual(adjusted["adapter"], "one_way_long")
        self.assertEqual(adjusted["buy_levels"], 8)
        self.assertEqual(adjusted["per_order_notional"], 100.0)

    def test_shock_does_not_increase_risk_for_one_way_long(self) -> None:
        report = resolve_multi_timeframe_bias(
            current_price=91.0,
            step_price=1.0,
            windows={
                "1m": [_candle(0, 95.0, 95.2, 90.8, 91.0)],
                "15m": [_candle(0, 94.0, 95.0, 90.0, 91.0)],
                "1h": [_candle(0, 96.0, 98.0, 90.0, 91.0)],
                "4h": [_candle(0, 100.0, 110.0, 90.0, 91.0)],
            },
            config=MultiTimeframeBiasConfig(enabled=True, mode_adapter="one_way_long"),
        )
        adjusted = apply_multi_timeframe_bias(
            buy_levels=8,
            sell_levels=8,
            per_order_notional=100.0,
            step_price=1.0,
            max_position_notional=1000.0,
            max_short_position_notional=0.0,
            report=report,
            config=MultiTimeframeBiasConfig(enabled=True, mode_adapter="one_way_long"),
        )

        self.assertTrue(report["shock_active"])
        self.assertLess(adjusted["per_order_notional"], 100.0)
        self.assertGreater(adjusted["step_price"], 1.0)
        self.assertLessEqual(adjusted["max_position_notional"], 1000.0)

    def test_low_zone_downtrend_switches_to_downtrend_defensive(self) -> None:
        report = resolve_multi_timeframe_bias(
            current_price=51.985,
            step_price=0.0001,
            windows={
                "1m": [_candle(0, 0.05207, 0.05208, 0.05202, 0.05204)],
                "15m": [_candle(0, 0.05268, 0.05272, 0.05212, 0.05224)],
                "1h": [_candle(0, 0.05258, 0.05286, 0.05212, 0.05224)],
                "4h": [_candle(0, 0.05480, 0.05525, 0.05128, 0.05181)],
            },
            config=MultiTimeframeBiasConfig(enabled=True),
        )
        adjusted = apply_multi_timeframe_bias(
            buy_levels=12,
            sell_levels=12,
            per_order_notional=80.0,
            step_price=0.0001,
            max_position_notional=1500.0,
            max_short_position_notional=950.0,
            report=report,
            config=MultiTimeframeBiasConfig(enabled=True),
        )

        self.assertEqual(report["regime"], "downtrend_defensive")
        self.assertTrue(report["downtrend_defensive_active"])
        self.assertLess(adjusted["buy_levels"], 12)
        self.assertLess(adjusted["max_position_notional"], 1500.0)
        self.assertGreater(adjusted["buy_offset_steps"], 0.0)
        self.assertLessEqual(adjusted["per_order_notional"], 80.0)

    def test_high_zone_uptrend_switches_to_uptrend_defensive(self) -> None:
        report = resolve_multi_timeframe_bias(
            current_price=109.0,
            step_price=1.0,
            windows={
                "1m": [_candle(0, 108.9, 109.4, 108.8, 109.2)],
                "15m": [_candle(0, 105.0, 110.0, 104.8, 109.0)],
                "1h": [_candle(0, 102.0, 110.0, 101.5, 109.0)],
                "4h": [_candle(0, 96.0, 110.0, 95.5, 109.0)],
            },
            config=MultiTimeframeBiasConfig(enabled=True),
        )
        adjusted = apply_multi_timeframe_bias(
            buy_levels=12,
            sell_levels=12,
            per_order_notional=80.0,
            step_price=1.0,
            max_position_notional=950.0,
            max_short_position_notional=1500.0,
            report=report,
            config=MultiTimeframeBiasConfig(enabled=True),
        )

        self.assertEqual(report["regime"], "uptrend_defensive")
        self.assertTrue(report["uptrend_defensive_active"])
        self.assertLess(adjusted["sell_levels"], 12)
        self.assertLess(adjusted["max_short_position_notional"], 1500.0)
        self.assertLess(adjusted["sell_offset_steps"], 0.0)

    def test_fast_shock_uses_shock_defensive_regime(self) -> None:
        report = resolve_multi_timeframe_bias(
            current_price=96.0,
            step_price=1.0,
            windows={
                "1m": [_candle(0, 100.0, 100.4, 95.8, 96.0)],
                "15m": [_candle(0, 96.0, 101.0, 95.0, 96.0)],
                "1h": [_candle(0, 96.0, 102.0, 94.0, 96.0)],
                "4h": [_candle(0, 96.0, 110.0, 90.0, 96.0)],
            },
            config=MultiTimeframeBiasConfig(enabled=True),
        )
        adjusted = apply_multi_timeframe_bias(
            buy_levels=8,
            sell_levels=8,
            per_order_notional=100.0,
            step_price=1.0,
            max_position_notional=1000.0,
            max_short_position_notional=1000.0,
            report=report,
        )

        self.assertEqual(report["regime"], "shock_defensive")
        self.assertGreater(adjusted["step_price"], 1.0)
        self.assertLess(adjusted["per_order_notional"], 100.0)
        self.assertLessEqual(adjusted["max_position_notional"], 1000.0)
        self.assertLessEqual(adjusted["max_short_position_notional"], 1000.0)

    def test_stabilizing_after_downtrend_uses_recovery_probe(self) -> None:
        report = resolve_multi_timeframe_bias(
            current_price=98.0,
            step_price=1.0,
            windows={
                "1m": [_candle(0, 97.2, 98.2, 97.0, 98.0)],
                "15m": [_candle(0, 100.0, 101.0, 96.5, 98.0)],
                "1h": [_candle(0, 101.0, 102.0, 96.0, 98.0)],
                "4h": [_candle(0, 104.0, 105.0, 95.0, 98.0)],
            },
            config=MultiTimeframeBiasConfig(enabled=True),
        )
        adjusted = apply_multi_timeframe_bias(
            buy_levels=8,
            sell_levels=8,
            per_order_notional=100.0,
            step_price=1.0,
            max_position_notional=1000.0,
            max_short_position_notional=1000.0,
            report=report,
        )

        self.assertEqual(report["regime"], "recovery_probe")
        self.assertTrue(report["recovery_probe_active"])
        self.assertEqual(adjusted["buy_levels"], 8)
        self.assertEqual(adjusted["sell_levels"], 8)
        self.assertLess(adjusted["per_order_notional"], 100.0)
        self.assertLessEqual(adjusted["max_position_notional"], 1000.0)


if __name__ == "__main__":
    unittest.main()
