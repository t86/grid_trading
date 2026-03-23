from __future__ import annotations

import unittest

from grid_optimizer.semi_auto_plan import (
    build_hedge_micro_grid_plan,
    build_inventory_target_neutral_plan,
    build_micro_grid_plan,
    diff_open_orders,
    shift_center_price,
)


class SemiAutoPlanTests(unittest.TestCase):
    def test_shift_center_price_moves_down_and_up_in_steps(self) -> None:
        new_center, moves = shift_center_price(
            center_price=0.05050,
            mid_price=0.05040,
            step_price=0.00002,
            tick_size=0.00001,
            down_trigger_steps=4,
            up_trigger_steps=8,
            shift_steps=4,
        )

        self.assertAlmostEqual(new_center, 0.05042, places=8)
        self.assertEqual(len(moves), 1)
        self.assertEqual(moves[0]["direction"], "down")

        new_center, moves = shift_center_price(
            center_price=0.05040,
            mid_price=0.05058,
            step_price=0.00002,
            tick_size=0.00001,
            down_trigger_steps=4,
            up_trigger_steps=8,
            shift_steps=4,
        )

        self.assertAlmostEqual(new_center, 0.05048, places=8)
        self.assertEqual(len(moves), 1)
        self.assertEqual(moves[0]["direction"], "up")

    def test_build_micro_grid_plan_uses_inventory_for_sell_orders_and_bootstrap(self) -> None:
        plan = build_micro_grid_plan(
            center_price=0.05057,
            step_price=0.00002,
            buy_levels=3,
            sell_levels=3,
            per_order_notional=25.0,
            base_position_notional=75.0,
            bid_price=0.05057,
            ask_price=0.05058,
            tick_size=0.00001,
            step_size=1.0,
            min_qty=1.0,
            min_notional=5.0,
            current_long_qty=0.0,
        )

        self.assertEqual(len(plan["buy_orders"]), 3)
        self.assertEqual(len(plan["sell_orders"]), 0)
        self.assertEqual(len(plan["bootstrap_orders"]), 1)
        self.assertGreater(plan["bootstrap_qty"], 0)

        plan = build_micro_grid_plan(
            center_price=0.05057,
            step_price=0.00002,
            buy_levels=3,
            sell_levels=3,
            per_order_notional=25.0,
            base_position_notional=75.0,
            bid_price=0.05057,
            ask_price=0.05058,
            tick_size=0.00001,
            step_size=1.0,
            min_qty=1.0,
            min_notional=5.0,
            current_long_qty=2000.0,
        )

        self.assertEqual(len(plan["sell_orders"]), 3)
        self.assertEqual(plan["bootstrap_qty"], 0.0)

    def test_diff_open_orders_finds_missing_and_stale_orders(self) -> None:
        existing = [
            {"side": "BUY", "type": "LIMIT", "price": "0.05030", "origQty": "500"},
            {"side": "SELL", "type": "LIMIT", "price": "0.05070", "origQty": "500"},
            {"side": "BUY", "type": "LIMIT", "price": "0.05010", "origQty": "500"},
        ]
        desired = [
            {"side": "BUY", "price": 0.05030, "qty": 500.0, "notional": 25.15, "level": 1, "role": "entry"},
            {"side": "SELL", "price": 0.05070, "qty": 500.0, "notional": 25.35, "level": 1, "role": "take_profit"},
            {"side": "BUY", "price": 0.05020, "qty": 500.0, "notional": 25.10, "level": 2, "role": "entry"},
        ]

        diff = diff_open_orders(existing_orders=existing, desired_orders=desired)

        self.assertEqual(len(diff["kept_orders"]), 2)
        self.assertEqual(len(diff["missing_orders"]), 1)
        self.assertEqual(len(diff["stale_orders"]), 1)
        self.assertAlmostEqual(float(diff["missing_orders"][0]["price"]), 0.05020, places=8)

    def test_diff_open_orders_separates_position_side(self) -> None:
        existing = [
            {"side": "BUY", "type": "LIMIT", "price": "0.05030", "origQty": "500", "positionSide": "LONG"},
            {"side": "BUY", "type": "LIMIT", "price": "0.05030", "origQty": "500", "positionSide": "SHORT"},
        ]
        desired = [
            {
                "side": "BUY",
                "price": 0.05030,
                "qty": 500.0,
                "notional": 25.15,
                "level": 1,
                "role": "entry_long",
                "position_side": "LONG",
            },
            {
                "side": "BUY",
                "price": 0.05030,
                "qty": 500.0,
                "notional": 25.15,
                "level": 1,
                "role": "take_profit_short",
                "position_side": "SHORT",
            },
        ]

        diff = diff_open_orders(existing_orders=existing, desired_orders=desired)

        self.assertEqual(len(diff["kept_orders"]), 2)
        self.assertEqual(len(diff["missing_orders"]), 0)
        self.assertEqual(len(diff["stale_orders"]), 0)

    def test_diff_open_orders_treats_limit_maker_as_limit(self) -> None:
        existing = [
            {"side": "BUY", "type": "LIMIT_MAKER", "price": "0.02685", "origQty": "744"},
        ]
        desired = [
            {"side": "BUY", "price": 0.02685, "qty": 744.0, "notional": 19.9764, "level": 1, "role": "entry"},
        ]

        diff = diff_open_orders(existing_orders=existing, desired_orders=desired)

        self.assertEqual(len(diff["kept_orders"]), 1)
        self.assertEqual(len(diff["missing_orders"]), 0)
        self.assertEqual(len(diff["stale_orders"]), 0)

    def test_build_hedge_micro_grid_plan_builds_both_sides(self) -> None:
        plan = build_hedge_micro_grid_plan(
            center_price=0.05057,
            step_price=0.00002,
            buy_levels=2,
            sell_levels=2,
            per_order_notional=25.0,
            base_position_notional=75.0,
            bid_price=0.05057,
            ask_price=0.05058,
            tick_size=0.00001,
            step_size=1.0,
            min_qty=1.0,
            min_notional=5.0,
            current_long_qty=0.0,
            current_short_qty=0.0,
        )

        self.assertEqual(len(plan["bootstrap_orders"]), 2)
        self.assertEqual(
            {item["position_side"] for item in plan["bootstrap_orders"]},
            {"LONG", "SHORT"},
        )
        self.assertEqual(
            {item["position_side"] for item in plan["buy_orders"]},
            {"LONG"},
        )
        self.assertEqual(
            {item["position_side"] for item in plan["sell_orders"]},
            {"SHORT"},
        )
        self.assertGreater(plan["bootstrap_long_qty"], 0)
        self.assertGreater(plan["bootstrap_short_qty"], 0)

        plan = build_hedge_micro_grid_plan(
            center_price=0.05057,
            step_price=0.00002,
            buy_levels=2,
            sell_levels=2,
            per_order_notional=25.0,
            base_position_notional=75.0,
            bid_price=0.05057,
            ask_price=0.05058,
            tick_size=0.00001,
            step_size=1.0,
            min_qty=1.0,
            min_notional=5.0,
            current_long_qty=2000.0,
            current_short_qty=2000.0,
        )

        self.assertTrue(any(item["role"] == "take_profit_short" for item in plan["buy_orders"]))
        self.assertTrue(any(item["role"] == "take_profit_long" for item in plan["sell_orders"]))

    def test_build_inventory_target_neutral_plan_builds_band_orders_and_bootstrap(self) -> None:
        plan = build_inventory_target_neutral_plan(
            center_price=1.0,
            mid_price=0.988,
            band_offsets=[0.005, 0.01, 0.02],
            band_target_ratios=[0.25, 0.55, 1.0],
            max_long_notional=500.0,
            max_short_notional=500.0,
            bid_price=0.9879,
            ask_price=0.9881,
            tick_size=0.0001,
            step_size=1.0,
            min_qty=1.0,
            min_notional=5.0,
            current_net_qty=50.0,
        )

        self.assertEqual(plan["bootstrap_orders"][0]["side"], "BUY")
        self.assertAlmostEqual(plan["current_target_notional"], 275.0)
        self.assertTrue(any(item["role"] == "entry_long" for item in plan["buy_orders"]))
        self.assertTrue(any(item["role"] == "entry_short" for item in plan["sell_orders"]))

    def test_build_inventory_target_neutral_plan_builds_sell_bootstrap_when_price_above_center(self) -> None:
        plan = build_inventory_target_neutral_plan(
            center_price=1.0,
            mid_price=1.012,
            band_offsets=[0.005, 0.01, 0.02],
            band_target_ratios=[0.25, 0.55, 1.0],
            max_long_notional=500.0,
            max_short_notional=500.0,
            bid_price=1.0119,
            ask_price=1.0121,
            tick_size=0.0001,
            step_size=1.0,
            min_qty=1.0,
            min_notional=5.0,
            current_net_qty=80.0,
        )

        self.assertEqual(plan["bootstrap_orders"][0]["side"], "SELL")
        self.assertLess(plan["current_target_notional"], 0.0)


if __name__ == "__main__":
    unittest.main()
