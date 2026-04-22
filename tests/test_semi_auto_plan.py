from __future__ import annotations

import multiprocessing as mp
import unittest

from grid_optimizer.semi_auto_plan import (
    build_hedge_micro_grid_plan,
    build_inventory_target_neutral_plan,
    build_micro_grid_plan,
    diff_open_orders,
    shift_center_price,
)


def _shift_center_price_in_subprocess(queue: mp.Queue, kwargs: dict) -> None:
    try:
        queue.put(("ok", shift_center_price(**kwargs)))
    except Exception as exc:  # pragma: no cover - surfaced to parent assertion
        queue.put(("error", repr(exc)))


class SemiAutoPlanTests(unittest.TestCase):
    def _run_shift_center_price(self, **kwargs):
        ctx = mp.get_context("spawn")
        queue = ctx.Queue()
        proc = ctx.Process(target=_shift_center_price_in_subprocess, args=(queue, kwargs))
        proc.start()
        proc.join(timeout=1.0)
        if proc.is_alive():
            proc.terminate()
            proc.join(timeout=1.0)
            self.fail("shift_center_price did not terminate")
        if queue.empty():
            self.fail("shift_center_price produced no result")
        status, payload = queue.get_nowait()
        if status != "ok":
            self.fail(payload)
        return payload

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

    def test_shift_center_price_terminates_when_tick_size_exceeds_shift_size_down(self) -> None:
        new_center, moves = self._run_shift_center_price(
            center_price=0.0101,
            mid_price=0.01004,
            step_price=0.00002,
            tick_size=0.0001,
            down_trigger_steps=2,
            up_trigger_steps=2,
            shift_steps=2,
        )

        self.assertAlmostEqual(new_center, 0.0100, places=8)
        self.assertGreaterEqual(len(moves), 1)
        self.assertEqual(moves[0]["direction"], "down")

    def test_shift_center_price_terminates_when_tick_size_exceeds_shift_size_up(self) -> None:
        new_center, moves = self._run_shift_center_price(
            center_price=0.0100,
            mid_price=0.01006,
            step_price=0.00002,
            tick_size=0.0001,
            down_trigger_steps=2,
            up_trigger_steps=2,
            shift_steps=2,
        )

        self.assertAlmostEqual(new_center, 0.0101, places=8)
        self.assertGreaterEqual(len(moves), 1)
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

    def test_diff_open_orders_keeps_same_price_order_and_adds_delta_when_qty_increases(self) -> None:
        existing = [
            {"side": "BUY", "type": "LIMIT", "price": "0.02685", "origQty": "744", "orderId": 1},
        ]
        desired = [
            {"side": "BUY", "price": 0.02685, "qty": 900.0, "notional": 24.165, "level": 1, "role": "entry"},
        ]

        diff = diff_open_orders(existing_orders=existing, desired_orders=desired)

        self.assertEqual(len(diff["kept_orders"]), 1)
        self.assertEqual(len(diff["stale_orders"]), 0)
        self.assertEqual(len(diff["missing_orders"]), 1)
        self.assertAlmostEqual(float(diff["missing_orders"][0]["qty"]), 156.0, places=8)
        self.assertAlmostEqual(float(diff["missing_orders"][0]["price"]), 0.02685, places=8)

    def test_diff_open_orders_replaces_same_price_order_when_qty_decreases(self) -> None:
        existing = [
            {"side": "BUY", "type": "LIMIT", "price": "0.02685", "origQty": "744", "orderId": 1},
        ]
        desired = [
            {"side": "BUY", "price": 0.02685, "qty": 600.0, "notional": 16.11, "level": 1, "role": "entry"},
        ]

        diff = diff_open_orders(existing_orders=existing, desired_orders=desired)

        self.assertEqual(len(diff["kept_orders"]), 0)
        self.assertEqual(len(diff["stale_orders"]), 1)
        self.assertEqual(len(diff["missing_orders"]), 1)
        self.assertAlmostEqual(float(diff["missing_orders"][0]["qty"]), 600.0, places=8)

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

    def test_build_hedge_micro_grid_plan_limits_reverse_orders_to_one_non_overlap_level(self) -> None:
        plan = build_hedge_micro_grid_plan(
            center_price=1.0,
            step_price=0.01,
            buy_levels=4,
            sell_levels=4,
            per_order_notional=25.0,
            base_position_notional=0.0,
            bid_price=0.999,
            ask_price=1.001,
            tick_size=0.001,
            step_size=1.0,
            min_qty=1.0,
            min_notional=5.0,
            current_long_qty=50.0,
            current_short_qty=50.0,
        )

        take_profit_long_prices = {
            item["price"] for item in plan["sell_orders"] if item["role"] == "take_profit_long"
        }
        take_profit_short_prices = {
            item["price"] for item in plan["buy_orders"] if item["role"] == "take_profit_short"
        }
        entry_short_orders = [item for item in plan["sell_orders"] if item["role"] == "entry_short"]
        entry_long_orders = [item for item in plan["buy_orders"] if item["role"] == "entry_long"]

        self.assertEqual(len(entry_short_orders), 1)
        self.assertEqual(len(entry_long_orders), 1)
        self.assertNotIn(entry_short_orders[0]["price"], take_profit_long_prices)
        self.assertNotIn(entry_long_orders[0]["price"], take_profit_short_prices)

    def test_build_hedge_micro_grid_plan_light_long_only_exits_latest_profitable_lot(self) -> None:
        plan = build_hedge_micro_grid_plan(
            center_price=1.0,
            step_price=0.01,
            buy_levels=4,
            sell_levels=4,
            per_order_notional=25.0,
            base_position_notional=0.0,
            bid_price=0.999,
            ask_price=1.001,
            tick_size=0.001,
            step_size=1.0,
            min_qty=1.0,
            min_notional=5.0,
            current_long_qty=50.0,
            current_short_qty=0.0,
            current_long_lots=[
                {"qty": 25.0, "price": 0.98},
                {"qty": 25.0, "price": 1.035},
            ],
            profit_protect_notional_threshold=260.0,
        )

        take_profit_long_orders = [item for item in plan["sell_orders"] if item["role"] == "take_profit_long"]
        entry_short_orders = [item for item in plan["sell_orders"] if item["role"] == "entry_short"]

        self.assertEqual(len(take_profit_long_orders), 1)
        self.assertEqual(take_profit_long_orders[0]["price"], 1.04)
        self.assertLessEqual(take_profit_long_orders[0]["qty"], 25.0)
        self.assertEqual(entry_short_orders, [])

    def test_build_hedge_micro_grid_plan_heavy_long_allows_deleveraging_before_profit(self) -> None:
        plan = build_hedge_micro_grid_plan(
            center_price=1.0,
            step_price=0.01,
            buy_levels=4,
            sell_levels=4,
            per_order_notional=25.0,
            base_position_notional=0.0,
            bid_price=0.999,
            ask_price=1.001,
            tick_size=0.001,
            step_size=1.0,
            min_qty=1.0,
            min_notional=5.0,
            current_long_qty=300.0,
            current_short_qty=0.0,
            current_long_lots=[
                {"qty": 150.0, "price": 0.98},
                {"qty": 150.0, "price": 1.035},
            ],
            profit_protect_notional_threshold=260.0,
        )

        take_profit_long_orders = [item for item in plan["sell_orders"] if item["role"] == "take_profit_long"]
        entry_short_orders = [item for item in plan["sell_orders"] if item["role"] == "entry_short"]

        self.assertTrue(any(item["price"] == 1.01 for item in take_profit_long_orders))
        self.assertEqual(len(entry_short_orders), 1)

    def test_build_hedge_micro_grid_plan_heavy_long_can_require_profit_before_reversal(self) -> None:
        plan = build_hedge_micro_grid_plan(
            center_price=1.0,
            step_price=0.01,
            buy_levels=4,
            sell_levels=4,
            per_order_notional=25.0,
            base_position_notional=0.0,
            bid_price=0.999,
            ask_price=1.001,
            tick_size=0.001,
            step_size=1.0,
            min_qty=1.0,
            min_notional=5.0,
            current_long_qty=300.0,
            current_short_qty=0.0,
            current_long_lots=[
                {"qty": 150.0, "price": 0.98},
                {"qty": 150.0, "price": 1.035},
            ],
            profit_protect_notional_threshold=260.0,
            delever_before_profit_allowed=False,
        )

        take_profit_long_orders = [item for item in plan["sell_orders"] if item["role"] == "take_profit_long"]
        entry_short_orders = [item for item in plan["sell_orders"] if item["role"] == "entry_short"]

        self.assertEqual(len(take_profit_long_orders), 1)
        self.assertEqual(take_profit_long_orders[0]["price"], 1.04)
        self.assertEqual(entry_short_orders, [])

    def test_build_hedge_micro_grid_plan_light_long_blocks_worse_cost_same_side_reentries(self) -> None:
        plan = build_hedge_micro_grid_plan(
            center_price=1.02,
            step_price=0.01,
            buy_levels=2,
            sell_levels=2,
            per_order_notional=25.0,
            base_position_notional=0.0,
            bid_price=1.019,
            ask_price=1.021,
            tick_size=0.001,
            step_size=1.0,
            min_qty=1.0,
            min_notional=5.0,
            current_long_qty=40.0,
            current_short_qty=0.0,
            current_long_lots=[
                {"qty": 40.0, "price": 0.995},
            ],
            entry_long_cost_guard_release_notional=100.0,
        )

        entry_long_orders = [item for item in plan["buy_orders"] if item["role"] == "entry_long"]

        self.assertEqual(entry_long_orders, [])

    def test_build_hedge_micro_grid_plan_light_short_blocks_worse_cost_same_side_reentries(self) -> None:
        plan = build_hedge_micro_grid_plan(
            center_price=0.98,
            step_price=0.01,
            buy_levels=2,
            sell_levels=2,
            per_order_notional=25.0,
            base_position_notional=0.0,
            bid_price=0.979,
            ask_price=0.981,
            tick_size=0.001,
            step_size=1.0,
            min_qty=1.0,
            min_notional=5.0,
            current_long_qty=0.0,
            current_short_qty=40.0,
            current_short_lots=[
                {"qty": 40.0, "price": 1.005},
            ],
            entry_short_cost_guard_release_notional=100.0,
        )

        entry_short_orders = [item for item in plan["sell_orders"] if item["role"] == "entry_short"]

        self.assertEqual(entry_short_orders, [])

    def test_build_hedge_micro_grid_plan_blocks_loss_making_long_exits(self) -> None:
        plan = build_hedge_micro_grid_plan(
            center_price=0.1712,
            step_price=0.00045,
            buy_levels=12,
            sell_levels=1,
            per_order_notional=30.0,
            base_position_notional=0.0,
            bid_price=0.1708,
            ask_price=0.1709,
            tick_size=0.0001,
            step_size=1.0,
            min_qty=1.0,
            min_notional=5.0,
            current_long_qty=175.0,
            current_short_qty=0.0,
            current_long_lots=[
                {"qty": 175.0, "price": 0.1714},
            ],
            take_profit_min_profit_ratio=0.0003,
        )

        take_profit_long_orders = [item for item in plan["sell_orders"] if item["role"] == "take_profit_long"]
        entry_short_orders = [item for item in plan["sell_orders"] if item["role"] == "entry_short"]

        self.assertEqual(take_profit_long_orders, [])
        self.assertEqual(entry_short_orders, [])

    def test_build_hedge_micro_grid_plan_blocks_loss_making_short_exits(self) -> None:
        plan = build_hedge_micro_grid_plan(
            center_price=0.1712,
            step_price=0.00045,
            buy_levels=1,
            sell_levels=12,
            per_order_notional=30.0,
            base_position_notional=0.0,
            bid_price=0.1716,
            ask_price=0.1717,
            tick_size=0.0001,
            step_size=1.0,
            min_qty=1.0,
            min_notional=5.0,
            current_long_qty=0.0,
            current_short_qty=175.0,
            current_short_lots=[
                {"qty": 175.0, "price": 0.1710},
            ],
            take_profit_min_profit_ratio=0.0003,
        )

        take_profit_short_orders = [item for item in plan["buy_orders"] if item["role"] == "take_profit_short"]
        entry_long_orders = [item for item in plan["buy_orders"] if item["role"] == "entry_long"]

        self.assertEqual(take_profit_short_orders, [])
        self.assertEqual(entry_long_orders, [])

    def test_build_hedge_micro_grid_plan_light_inventory_skips_top_of_book_entries(self) -> None:
        plan = build_hedge_micro_grid_plan(
            center_price=1.01,
            step_price=0.01,
            buy_levels=2,
            sell_levels=2,
            per_order_notional=25.0,
            base_position_notional=0.0,
            bid_price=1.000,
            ask_price=1.020,
            tick_size=0.001,
            step_size=1.0,
            min_qty=1.0,
            min_notional=5.0,
            current_long_qty=0.0,
            current_short_qty=0.0,
            entry_long_cost_guard_release_notional=100.0,
            entry_short_cost_guard_release_notional=100.0,
        )

        entry_long_prices = {item["price"] for item in plan["buy_orders"] if item["role"] == "entry_long"}
        entry_short_prices = {item["price"] for item in plan["sell_orders"] if item["role"] == "entry_short"}

        self.assertNotIn(1.0, entry_long_prices)
        self.assertNotIn(1.02, entry_short_prices)
        self.assertIn(0.99, entry_long_prices)
        self.assertIn(1.03, entry_short_prices)

    def test_build_hedge_micro_grid_plan_reverts_to_grid_when_center_far_from_market(self) -> None:
        plan = build_hedge_micro_grid_plan(
            center_price=1.05,
            step_price=0.01,
            buy_levels=2,
            sell_levels=2,
            per_order_notional=25.0,
            base_position_notional=0.0,
            bid_price=0.999,
            ask_price=1.001,
            tick_size=0.001,
            step_size=1.0,
            min_qty=1.0,
            min_notional=5.0,
            current_long_qty=0.0,
            current_short_qty=0.0,
            near_market_entry_max_center_distance_steps=2.0,
        )

        entry_long_prices = [item["price"] for item in plan["buy_orders"] if item["role"] == "entry_long"]
        entry_short_prices = [item["price"] for item in plan["sell_orders"] if item["role"] == "entry_short"]

        self.assertEqual(entry_long_prices[:2], [1.04, 1.03])
        self.assertEqual(entry_short_prices[:2], [1.06, 1.07])

    def test_build_hedge_micro_grid_plan_keeps_near_market_entries_within_center_threshold(self) -> None:
        plan = build_hedge_micro_grid_plan(
            center_price=1.01,
            step_price=0.01,
            buy_levels=2,
            sell_levels=2,
            per_order_notional=25.0,
            base_position_notional=0.0,
            bid_price=0.999,
            ask_price=1.001,
            tick_size=0.001,
            step_size=1.0,
            min_qty=1.0,
            min_notional=5.0,
            current_long_qty=0.0,
            current_short_qty=0.0,
            near_market_entry_max_center_distance_steps=2.0,
        )

        entry_long_prices = [item["price"] for item in plan["buy_orders"] if item["role"] == "entry_long"]
        entry_short_prices = [item["price"] for item in plan["sell_orders"] if item["role"] == "entry_short"]

        self.assertEqual(entry_long_prices[:2], [0.999, 0.989])
        self.assertEqual(entry_short_prices[:2], [1.001, 1.011])

    def test_build_hedge_micro_grid_plan_heavy_long_relaxes_same_side_entry_guard(self) -> None:
        plan = build_hedge_micro_grid_plan(
            center_price=1.02,
            step_price=0.01,
            buy_levels=2,
            sell_levels=2,
            per_order_notional=25.0,
            base_position_notional=0.0,
            bid_price=1.019,
            ask_price=1.021,
            tick_size=0.001,
            step_size=1.0,
            min_qty=1.0,
            min_notional=5.0,
            current_long_qty=120.0,
            current_short_qty=0.0,
            current_long_lots=[
                {"qty": 120.0, "price": 0.995},
            ],
            entry_long_cost_guard_release_notional=100.0,
        )

        entry_long_prices = {item["price"] for item in plan["buy_orders"] if item["role"] == "entry_long"}

        self.assertIn(1.01, entry_long_prices)

    def test_build_hedge_micro_grid_plan_short_entry_pause_blocks_reverse_sell_only(self) -> None:
        plan = build_hedge_micro_grid_plan(
            center_price=1.0,
            step_price=0.01,
            buy_levels=4,
            sell_levels=4,
            per_order_notional=25.0,
            base_position_notional=0.0,
            bid_price=0.999,
            ask_price=1.001,
            tick_size=0.001,
            step_size=1.0,
            min_qty=1.0,
            min_notional=5.0,
            current_long_qty=300.0,
            current_short_qty=0.0,
            entry_short_paused=True,
        )

        take_profit_long_orders = [item for item in plan["sell_orders"] if item["role"] == "take_profit_long"]
        entry_short_orders = [item for item in plan["sell_orders"] if item["role"] == "entry_short"]

        self.assertTrue(take_profit_long_orders)
        self.assertEqual(entry_short_orders, [])

    def test_build_hedge_micro_grid_plan_long_entry_pause_blocks_reverse_buy_only(self) -> None:
        plan = build_hedge_micro_grid_plan(
            center_price=1.0,
            step_price=0.01,
            buy_levels=4,
            sell_levels=4,
            per_order_notional=25.0,
            base_position_notional=0.0,
            bid_price=0.999,
            ask_price=1.001,
            tick_size=0.001,
            step_size=1.0,
            min_qty=1.0,
            min_notional=5.0,
            current_long_qty=0.0,
            current_short_qty=300.0,
            entry_long_paused=True,
        )

        take_profit_short_orders = [item for item in plan["buy_orders"] if item["role"] == "take_profit_short"]
        entry_long_orders = [item for item in plan["buy_orders"] if item["role"] == "entry_long"]

        self.assertTrue(take_profit_short_orders)
        self.assertEqual(entry_long_orders, [])

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
