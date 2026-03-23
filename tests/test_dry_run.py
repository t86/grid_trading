from __future__ import annotations

import unittest
from argparse import Namespace
from datetime import datetime, timezone

from grid_optimizer.dry_run import (
    apply_market_snapshot,
    build_paper_grid_legs,
    initialize_paper_state,
)


class DryRunTests(unittest.TestCase):
    def test_build_paper_grid_legs_rounds_prices_and_qty(self) -> None:
        legs = build_paper_grid_legs(
            min_price=100.03,
            max_price=110.07,
            n=2,
            total_buy_notional=1000,
            grid_level_mode="arithmetic",
            allocation_mode="equal",
            strategy_direction="long",
            neutral_anchor_price=None,
            tick_size=0.1,
            step_size=0.1,
            min_qty=0.1,
            min_notional=5.0,
        )
        self.assertEqual(len(legs), 2)
        self.assertAlmostEqual(legs[0]["entry_price"], 100.0, places=8)
        self.assertAlmostEqual(legs[0]["exit_price"], 105.1, places=8)
        self.assertGreater(legs[0]["qty"], 0.0)
        self.assertEqual(legs[0]["state"], "flat")

    def test_apply_market_snapshot_opens_and_closes_long_position(self) -> None:
        args = Namespace(
            symbol="ENSOUSDT",
            min_price=100.0,
            max_price=110.0,
            n=1,
            total_buy_notional=1000.0,
            grid_level_mode="arithmetic",
            allocation_mode="equal",
            strategy_direction="long",
            neutral_anchor_price=None,
            fee_rate=0.0002,
        )
        symbol_info = {
            "tick_size": 0.1,
            "step_size": 0.1,
            "min_qty": 0.1,
            "min_notional": 5.0,
        }
        state = initialize_paper_state(args, symbol_info)

        place_entry_snapshot = apply_market_snapshot(
            state=state,
            bid_price=100.9,
            ask_price=101.0,
            mark_price=101.0,
            funding_rate=-0.001,
            next_funding_time=None,
            ts=datetime(2026, 3, 12, 6, 0, tzinfo=timezone.utc),
        )
        self.assertEqual(len(place_entry_snapshot["fills_this_cycle"]), 0)
        self.assertEqual(state["legs"][0]["resting_order_intent"], "entry")

        open_snapshot = apply_market_snapshot(
            state=state,
            bid_price=99.8,
            ask_price=99.9,
            mark_price=99.9,
            funding_rate=-0.001,
            next_funding_time=None,
            ts=datetime(2026, 3, 12, 6, 1, tzinfo=timezone.utc),
        )
        self.assertEqual(len(open_snapshot["fills_this_cycle"]), 1)
        self.assertEqual(state["legs"][0]["position_state"], "open")
        self.assertGreater(open_snapshot["open_position_notional"], 0.0)

        place_exit_snapshot = apply_market_snapshot(
            state=state,
            bid_price=109.8,
            ask_price=109.9,
            mark_price=109.9,
            funding_rate=-0.001,
            next_funding_time=None,
            ts=datetime(2026, 3, 12, 6, 2, tzinfo=timezone.utc),
        )
        self.assertEqual(len(place_exit_snapshot["fills_this_cycle"]), 0)
        self.assertEqual(state["legs"][0]["resting_order_intent"], "exit")

        close_snapshot = apply_market_snapshot(
            state=state,
            bid_price=110.1,
            ask_price=110.2,
            mark_price=110.1,
            funding_rate=-0.001,
            next_funding_time=None,
            ts=datetime(2026, 3, 12, 6, 3, tzinfo=timezone.utc),
        )
        self.assertEqual(len(close_snapshot["fills_this_cycle"]), 1)
        self.assertEqual(state["legs"][0]["position_state"], "flat")
        self.assertGreater(close_snapshot["metrics"]["realized_pnl"], 0.0)
        self.assertEqual(close_snapshot["metrics"]["closed_trades"], 1)

    def test_apply_market_snapshot_flags_out_of_range(self) -> None:
        args = Namespace(
            symbol="ENSOUSDT",
            min_price=100.0,
            max_price=110.0,
            n=1,
            total_buy_notional=1000.0,
            grid_level_mode="arithmetic",
            allocation_mode="equal",
            strategy_direction="long",
            neutral_anchor_price=None,
            fee_rate=0.0002,
        )
        symbol_info = {
            "tick_size": 0.1,
            "step_size": 0.1,
            "min_qty": 0.1,
            "min_notional": 5.0,
        }
        state = initialize_paper_state(args, symbol_info)
        snapshot = apply_market_snapshot(
            state=state,
            bid_price=98.0,
            ask_price=98.1,
            mark_price=98.1,
            funding_rate=-0.001,
            next_funding_time=None,
            ts=datetime(2026, 3, 12, 6, 2, tzinfo=timezone.utc),
        )
        self.assertEqual(snapshot["market_status"], "below_range")
        self.assertTrue(snapshot["recenter_required"])


if __name__ == "__main__":
    unittest.main()
