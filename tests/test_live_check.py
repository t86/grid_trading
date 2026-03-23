from __future__ import annotations

import unittest

from grid_optimizer.live_check import build_live_entry_orders, evaluate_live_readiness


class LiveCheckTests(unittest.TestCase):
    def test_build_live_entry_orders_only_keeps_resting_enabled_entries(self) -> None:
        legs = [
            {
                "grid_index": 0,
                "state": "flat",
                "position_state": "flat",
                "entry_order_side": "BUY",
                "entry_price": 100.0,
                "qty": 1.2,
            },
            {
                "grid_index": 1,
                "state": "flat",
                "position_state": "flat",
                "entry_order_side": "BUY",
                "entry_price": 101.0,
                "qty": 1.0,
            },
            {
                "grid_index": 2,
                "state": "disabled",
                "position_state": "flat",
                "entry_order_side": "BUY",
                "entry_price": 99.0,
                "qty": 1.0,
            },
            {
                "grid_index": 3,
                "state": "flat",
                "position_state": "open",
                "entry_order_side": "BUY",
                "entry_price": 98.0,
                "qty": 1.0,
            },
        ]

        orders = build_live_entry_orders(legs=legs, bid_price=100.8, ask_price=100.9)

        self.assertEqual(len(orders), 1)
        self.assertEqual(orders[0]["grid_index"], 0)
        self.assertAlmostEqual(orders[0]["notional"], 120.0, places=8)

    def test_evaluate_live_readiness_blocks_existing_state_and_hedge_mode(self) -> None:
        report = evaluate_live_readiness(
            symbol="ENSOUSDT",
            total_buy_notional=100.0,
            leverage=2,
            strategy_direction="long",
            market_status="in_range",
            funding_rate=-0.0001,
            account_info={
                "availableBalance": "80",
                "totalWalletBalance": "100",
                "positions": [{"symbol": "ENSOUSDT", "positionAmt": "3.2"}],
            },
            position_mode={"dualSidePosition": True},
            open_orders=[{"orderId": 1}],
            entry_orders=[{"grid_index": 0, "side": "BUY", "price": 1.15, "qty": 5.0, "notional": 5.75}],
        )

        self.assertFalse(report["ok"])
        self.assertEqual(len(report["errors"]), 3)
        self.assertTrue(report["dual_side_position"])
        self.assertAlmostEqual(report["position_amt"], 3.2, places=8)

    def test_evaluate_live_readiness_warns_on_balance_and_funding(self) -> None:
        report = evaluate_live_readiness(
            symbol="ENSOUSDT",
            total_buy_notional=100.0,
            leverage=2,
            strategy_direction="long",
            market_status="in_range",
            funding_rate=0.0002,
            account_info={
                "availableBalance": "40",
                "totalWalletBalance": "60",
                "positions": [{"symbol": "ENSOUSDT", "positionAmt": "0"}],
            },
            position_mode={"dualSidePosition": False},
            open_orders=[],
            entry_orders=[{"grid_index": 0, "side": "BUY", "price": 1.15, "qty": 5.0, "notional": 5.75}],
        )

        self.assertTrue(report["ok"])
        self.assertEqual(report["errors"], [])
        self.assertGreaterEqual(len(report["warnings"]), 2)
        self.assertAlmostEqual(report["estimated_initial_margin"], 50.0, places=8)


if __name__ == "__main__":
    unittest.main()
