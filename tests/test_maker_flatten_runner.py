from __future__ import annotations

import unittest

from grid_optimizer.maker_flatten_runner import build_flatten_orders_from_snapshot
from grid_optimizer.semi_auto_plan import diff_open_orders


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
                        "entryPrice": "1.20",
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
                        "entryPrice": "1.30",
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

    def test_one_way_long_reduces_only_above_target_notional(self) -> None:
        result = build_flatten_orders_from_snapshot(
            symbol="TESTUSDT",
            bid_price=1.23,
            ask_price=1.25,
            dual_side_position=False,
            account_info={
                "positions": [
                    {
                        "symbol": "TESTUSDT",
                        "positionAmt": "200",
                        "positionSide": "BOTH",
                        "entryPrice": "1.20",
                    }
                ]
            },
            symbol_info={"tick_size": 0.01, "step_size": 1.0, "min_qty": 1.0, "min_notional": 5.0},
            target_position_notional=150.0,
        )

        self.assertEqual(result["target_mode"], "reduce_to_notional")
        self.assertEqual(len(result["orders"]), 1)
        order = result["orders"][0]
        self.assertEqual(order["side"], "SELL")
        self.assertEqual(order["price"], 1.25)
        self.assertEqual(order["quantity"], 80.0)
        self.assertTrue(order["reduce_only"])

    def test_one_way_long_target_reached_places_no_order(self) -> None:
        result = build_flatten_orders_from_snapshot(
            symbol="TESTUSDT",
            bid_price=1.23,
            ask_price=1.25,
            dual_side_position=False,
            account_info={
                "positions": [
                    {
                        "symbol": "TESTUSDT",
                        "positionAmt": "100",
                        "positionSide": "BOTH",
                        "entryPrice": "1.20",
                    }
                ]
            },
            symbol_info={"tick_size": 0.01, "step_size": 1.0, "min_qty": 1.0, "min_notional": 5.0},
            target_position_notional=150.0,
        )

        self.assertEqual(result["orders"], [])
        self.assertTrue(result["long_target_reached"])
        self.assertIn("目标 150.0000U", result["warnings"][0])

    def test_hedge_mode_closes_both_legs_at_top_of_book(self) -> None:
        result = build_flatten_orders_from_snapshot(
            symbol="TESTUSDT",
            bid_price=1.23,
            ask_price=1.24,
            dual_side_position=True,
            account_info={
                "positions": [
                    {"symbol": "TESTUSDT", "positionAmt": "5", "positionSide": "LONG", "entryPrice": "1.20"},
                    {"symbol": "TESTUSDT", "positionAmt": "-7", "positionSide": "SHORT", "entryPrice": "1.30"},
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

    def test_one_way_long_blocks_loss_by_default(self) -> None:
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
                        "entryPrice": "1.30",
                    }
                ]
            },
            symbol_info={"tick_size": 0.01, "step_size": 1.0, "min_qty": 1.0, "min_notional": 5.0},
        )

        self.assertEqual(result["orders"], [])
        self.assertTrue(any("亏损" in warning for warning in result["warnings"]))

    def test_one_way_short_blocks_loss_by_default(self) -> None:
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
                        "entryPrice": "1.10",
                    }
                ]
            },
            symbol_info={"tick_size": 0.01, "step_size": 1.0, "min_qty": 1.0, "min_notional": 5.0},
        )

        self.assertEqual(result["orders"], [])
        self.assertTrue(any("亏损" in warning for warning in result["warnings"]))

    def test_flatten_blocks_missing_cost_basis_by_default(self) -> None:
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

        self.assertEqual(result["orders"], [])
        self.assertTrue(any("缺少成本" in warning for warning in result["warnings"]))

    def test_flatten_allows_loss_when_explicitly_enabled(self) -> None:
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
                        "entryPrice": "1.30",
                    }
                ]
            },
            symbol_info={"tick_size": 0.01, "step_size": 1.0, "min_qty": 1.0, "min_notional": 5.0},
            allow_loss=True,
        )

        self.assertEqual(len(result["orders"]), 1)
        self.assertEqual(result["orders"][0]["side"], "SELL")

    def test_matching_flatten_order_is_kept(self) -> None:
        desired = {
            "side": "SELL",
            "price": 1.24,
            "quantity": 10.0,
            "qty": 10.0,
            "position_side": "BOTH",
            "time_in_force": "GTX",
        }
        diff = diff_open_orders(
            existing_orders=[
                {
                    "side": "SELL",
                    "type": "LIMIT",
                    "price": "1.24",
                    "origQty": "10",
                    "positionSide": "BOTH",
                    "orderId": 1,
                }
            ],
            desired_orders=[desired],
        )
        self.assertEqual(len(diff["kept_orders"]), 1)
        self.assertEqual(len(diff["missing_orders"]), 0)
        self.assertEqual(len(diff["stale_orders"]), 0)


if __name__ == "__main__":
    unittest.main()
