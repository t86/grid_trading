from __future__ import annotations

import unittest

from grid_optimizer.maker_flatten_runner import build_flatten_orders_from_snapshot


class MakerFlattenRunnerTests(unittest.TestCase):
    def test_one_way_long_uses_best_ask_reduce_only_sell(self) -> None:
        result = build_flatten_orders_from_snapshot(
            symbol="TESTUSDT",
            bid_price=1.23,
            ask_price=1.24,
            dual_side_position=False,
            account_info={
                "positions": [
                    {
                        "symbol": "TESTUSDT",
                        "positionAmt": "10",
                        "positionSide": "BOTH",
                    }
                ]
            },
            symbol_info={"tick_size": 0.01, "step_size": 1.0, "min_qty": 1.0, "min_notional": 5.0},
        )
        self.assertEqual(len(result["orders"]), 1)
        order = result["orders"][0]
        self.assertEqual(order["side"], "SELL")
        self.assertEqual(order["price"], 1.24)
        self.assertEqual(order["quantity"], 10.0)
        self.assertTrue(order["reduce_only"])
        self.assertNotIn("position_side", order)

    def test_one_way_short_uses_best_bid_reduce_only_buy(self) -> None:
        result = build_flatten_orders_from_snapshot(
            symbol="TESTUSDT",
            bid_price=1.23,
            ask_price=1.24,
            dual_side_position=False,
            account_info={
                "positions": [
                    {
                        "symbol": "TESTUSDT",
                        "positionAmt": "-8",
                        "positionSide": "BOTH",
                    }
                ]
            },
            symbol_info={"tick_size": 0.01, "step_size": 1.0, "min_qty": 1.0, "min_notional": 5.0},
        )
        self.assertEqual(len(result["orders"]), 1)
        order = result["orders"][0]
        self.assertEqual(order["side"], "BUY")
        self.assertEqual(order["price"], 1.23)
        self.assertEqual(order["quantity"], 8.0)
        self.assertTrue(order["reduce_only"])
        self.assertNotIn("position_side", order)

    def test_hedge_mode_closes_both_legs_at_top_of_book(self) -> None:
        result = build_flatten_orders_from_snapshot(
            symbol="TESTUSDT",
            bid_price=1.23,
            ask_price=1.24,
            dual_side_position=True,
            account_info={
                "positions": [
                    {"symbol": "TESTUSDT", "positionAmt": "5", "positionSide": "LONG"},
                    {"symbol": "TESTUSDT", "positionAmt": "-7", "positionSide": "SHORT"},
                ]
            },
            symbol_info={"tick_size": 0.01, "step_size": 1.0, "min_qty": 1.0, "min_notional": 5.0},
        )
        self.assertEqual(len(result["orders"]), 2)
        self.assertEqual(result["orders"][0]["side"], "SELL")
        self.assertEqual(result["orders"][0]["price"], 1.24)
        self.assertEqual(result["orders"][0]["position_side"], "LONG")
        self.assertEqual(result["orders"][1]["side"], "BUY")
        self.assertEqual(result["orders"][1]["price"], 1.23)
        self.assertEqual(result["orders"][1]["position_side"], "SHORT")


if __name__ == "__main__":
    unittest.main()
