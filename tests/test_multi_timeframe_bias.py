from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from grid_optimizer.multi_timeframe_bias import (
    MultiTimeframeBiasConfig,
    apply_multi_timeframe_bias,
    resolve_dynamic_side_scheduler,
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
                "15m": [_candle(0, 94.0, 95.0, 90.0, 91.0)],
                "1h": [_candle(0, 96.0, 98.0, 90.0, 91.0)],
                "4h": [_candle(0, 100.0, 110.0, 90.0, 91.0)],
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
                "15m": [_candle(0, 106.0, 110.0, 105.0, 109.0)],
                "1h": [_candle(0, 103.0, 110.0, 100.0, 109.0)],
                "4h": [_candle(0, 100.0, 110.0, 90.0, 109.0)],
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

    def test_scheduler_restricts_short_accumulation_when_low_zone_already_short_heavy(self) -> None:
        report = resolve_multi_timeframe_bias(
            current_price=91.0,
            step_price=1.0,
            windows={
                "1m": [_candle(0, 91.4, 91.6, 90.8, 91.0)],
                "15m": [_candle(0, 94.0, 95.0, 90.0, 91.0)],
                "1h": [_candle(0, 96.0, 98.0, 90.0, 91.0)],
                "4h": [_candle(0, 100.0, 110.0, 90.0, 91.0)],
            },
            config=MultiTimeframeBiasConfig(enabled=True),
        )

        scheduler = resolve_dynamic_side_scheduler(
            report=report,
            current_long_notional=120.0,
            current_short_notional=820.0,
            max_position_notional=1000.0,
            max_short_position_notional=1000.0,
            pause_buy_position_notional=800.0,
            pause_short_position_notional=800.0,
            per_order_notional=100.0,
            step_price=1.0,
        )

        self.assertTrue(scheduler["applied"])
        self.assertLess(scheduler["short_budget_scale"], 0.35)
        self.assertGreater(scheduler["long_budget_scale"], 0.85)
        self.assertGreater(scheduler["sell_offset_steps"], 1.0)
        self.assertLess(scheduler["per_order_notional_scale"], 1.0)
        self.assertIn("short_inventory_skew", scheduler["reasons"])

    def test_scheduler_restricts_long_accumulation_when_high_zone_already_long_heavy(self) -> None:
        report = resolve_multi_timeframe_bias(
            current_price=109.0,
            step_price=1.0,
            windows={
                "1m": [_candle(0, 108.4, 109.2, 108.3, 109.0)],
                "15m": [_candle(0, 106.0, 110.0, 105.0, 109.0)],
                "1h": [_candle(0, 103.0, 110.0, 100.0, 109.0)],
                "4h": [_candle(0, 100.0, 110.0, 90.0, 109.0)],
            },
            config=MultiTimeframeBiasConfig(enabled=True),
        )

        scheduler = resolve_dynamic_side_scheduler(
            report=report,
            current_long_notional=830.0,
            current_short_notional=100.0,
            max_position_notional=1000.0,
            max_short_position_notional=1000.0,
            pause_buy_position_notional=800.0,
            pause_short_position_notional=800.0,
            per_order_notional=100.0,
            step_price=1.0,
        )

        self.assertTrue(scheduler["applied"])
        self.assertLess(scheduler["long_budget_scale"], 0.35)
        self.assertGreater(scheduler["short_budget_scale"], 0.85)
        self.assertGreater(scheduler["buy_offset_steps"], 1.0)
        self.assertIn("long_inventory_skew", scheduler["reasons"])

    def test_scheduler_keeps_middle_zone_balanced(self) -> None:
        report = resolve_multi_timeframe_bias(
            current_price=100.0,
            step_price=1.0,
            windows={
                "1m": [_candle(0, 100.1, 100.3, 99.9, 100.0)],
                "15m": [_candle(0, 100.0, 102.0, 98.0, 100.0)],
                "1h": [_candle(0, 100.0, 104.0, 96.0, 100.0)],
                "4h": [_candle(0, 100.0, 110.0, 90.0, 100.0)],
            },
            config=MultiTimeframeBiasConfig(enabled=True),
        )

        scheduler = resolve_dynamic_side_scheduler(
            report=report,
            current_long_notional=300.0,
            current_short_notional=320.0,
            max_position_notional=1000.0,
            max_short_position_notional=1000.0,
            pause_buy_position_notional=800.0,
            pause_short_position_notional=800.0,
            per_order_notional=100.0,
            step_price=1.0,
        )

        self.assertTrue(scheduler["applied"])
        self.assertGreater(scheduler["long_budget_scale"], 0.85)
        self.assertGreater(scheduler["short_budget_scale"], 0.85)
        self.assertLess(abs(scheduler["buy_offset_steps"]), 0.5)
        self.assertLess(abs(scheduler["sell_offset_steps"]), 0.5)

    def test_scheduler_defensive_mode_on_shock_or_loss_state(self) -> None:
        report = resolve_multi_timeframe_bias(
            current_price=96.0,
            step_price=1.0,
            windows={
                "1m": [_candle(0, 100.0, 100.5, 95.8, 96.0)],
                "15m": [_candle(0, 100.0, 101.0, 95.0, 96.0)],
                "1h": [_candle(0, 100.0, 102.0, 94.0, 96.0)],
                "4h": [_candle(0, 100.0, 110.0, 90.0, 96.0)],
            },
            config=MultiTimeframeBiasConfig(enabled=True),
        )

        scheduler = resolve_dynamic_side_scheduler(
            report=report,
            current_long_notional=400.0,
            current_short_notional=400.0,
            max_position_notional=1000.0,
            max_short_position_notional=1000.0,
            pause_buy_position_notional=800.0,
            pause_short_position_notional=800.0,
            per_order_notional=100.0,
            step_price=1.0,
            recent_loss_ratio=0.85,
            active_delever_loss_count=2,
        )

        self.assertTrue(scheduler["defensive_mode"])
        self.assertGreater(scheduler["step_scale"], 1.0)
        self.assertLess(scheduler["per_order_notional_scale"], 0.75)
        self.assertLess(scheduler["reduce_max_order_scale"], 1.0)
        self.assertIn("loss_state", scheduler["reasons"])


if __name__ == "__main__":
    unittest.main()
