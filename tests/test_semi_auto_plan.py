from __future__ import annotations

import multiprocessing as mp
import unittest

from grid_optimizer.semi_auto_plan import (
    build_best_quote_long_flip_plan,
    build_hedge_micro_grid_plan,
    build_inventory_target_neutral_plan,
    build_micro_grid_plan,
    diff_open_orders,
    preserve_sticky_exit_orders,
    preserve_sticky_entry_orders,
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

    def test_build_best_quote_long_flip_plan_places_near_quote_ping_pong(self) -> None:
        plan = build_best_quote_long_flip_plan(
            bid_price=2329.99,
            ask_price=2330.00,
            per_order_notional=25.0,
            max_position_notional=900.0,
            max_entry_orders=3,
            max_exit_orders=2,
            current_long_qty=0.107,
            current_long_notional=249.0,
            tick_size=0.01,
            step_size=0.001,
            min_qty=0.001,
            min_notional=20.0,
        )

        self.assertEqual(len(plan["sell_orders"]), 2)
        self.assertEqual(plan["sell_orders"][0]["side"], "SELL")
        self.assertEqual(plan["sell_orders"][0]["price"], 2330.0)
        self.assertEqual(plan["sell_orders"][0]["qty"], 0.01)
        self.assertEqual(plan["sell_orders"][0]["role"], "take_profit")
        self.assertEqual(plan["sell_orders"][1]["price"], 2330.01)
        self.assertEqual(plan["sell_orders"][1]["qty"], 0.01)
        self.assertEqual(len(plan["buy_orders"]), 3)
        for index, order in enumerate(plan["buy_orders"]):
            self.assertEqual(order["side"], "BUY")
            self.assertAlmostEqual(order["price"], 2329.99 - (index * 0.01))
            self.assertEqual(order["qty"], 0.01)
            self.assertEqual(order["role"], "entry")
        self.assertEqual(plan["bootstrap_orders"], [])
        self.assertEqual(plan["target_base_qty"], 0.0)

    def test_build_best_quote_long_flip_plan_skips_sell_without_inventory(self) -> None:
        plan = build_best_quote_long_flip_plan(
            bid_price=2329.99,
            ask_price=2330.00,
            per_order_notional=25.0,
            max_position_notional=900.0,
            max_entry_orders=2,
            max_exit_orders=2,
            current_long_qty=0.0,
            current_long_notional=0.0,
            tick_size=0.01,
            step_size=0.001,
            min_qty=0.001,
            min_notional=20.0,
        )

        self.assertEqual(plan["sell_orders"], [])
        self.assertEqual(len(plan["buy_orders"]), 2)

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

        self.assertAlmostEqual(new_center, 0.05044, places=8)
        self.assertEqual(len(moves), 1)
        self.assertEqual(moves[0]["direction"], "down")
        self.assertEqual(moves[0]["shift_steps"], 3)
        self.assertEqual(moves[0]["shift_ratio"], 0.5)

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
        self.assertEqual(moves[0]["shift_steps"], 4)
        self.assertEqual(moves[0]["shift_ratio"], 0.5)

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

    def test_preserve_sticky_entry_orders_keeps_closer_existing_entries_within_step_tolerance(self) -> None:
        existing = [
            {
                "orderId": 1,
                "side": "BUY",
                "type": "LIMIT",
                "price": "0.3173",
                "origQty": "141",
                "positionSide": "BOTH",
                "role": "entry_long",
            },
            {
                "orderId": 2,
                "side": "SELL",
                "type": "LIMIT",
                "price": "0.3187",
                "origQty": "141",
                "positionSide": "BOTH",
                "role": "entry_short",
            },
        ]
        desired = [
            {"side": "BUY", "price": 0.3172, "qty": 141.0, "notional": 44.7252, "level": 1, "role": "entry_long"},
            {"side": "SELL", "price": 0.3188, "qty": 141.0, "notional": 44.9508, "level": 1, "role": "entry_short"},
        ]

        adjusted = preserve_sticky_entry_orders(
            existing_orders=existing,
            desired_orders=desired,
            price_tolerance=0.0007,
        )

        self.assertEqual(adjusted[0]["price"], 0.3173)
        self.assertEqual(adjusted[0]["qty"], 141.0)
        self.assertEqual(adjusted[1]["price"], 0.3187)
        self.assertEqual(adjusted[1]["qty"], 141.0)

    def test_preserve_sticky_entry_orders_skips_prices_that_collide_with_take_profit(self) -> None:
        existing = [
            {
                "orderId": 1,
                "side": "BUY",
                "type": "LIMIT",
                "price": "0.3173",
                "origQty": "141",
                "positionSide": "BOTH",
                "role": "entry_long",
            },
        ]
        desired = [
            {"side": "BUY", "price": 0.3172, "qty": 141.0, "notional": 44.7252, "level": 1, "role": "entry_long"},
            {"side": "BUY", "price": 0.3173, "qty": 100.0, "notional": 31.73, "level": 1, "role": "take_profit_short"},
        ]

        adjusted = preserve_sticky_entry_orders(
            existing_orders=existing,
            desired_orders=desired,
            price_tolerance=0.0007,
        )

        entry_long = next(item for item in adjusted if item["role"] == "entry_long")
        self.assertEqual(entry_long["price"], 0.3172)

    def test_preserve_sticky_entry_orders_keeps_closer_take_profit_orders(self) -> None:
        existing = [
            {
                "orderId": 1,
                "side": "SELL",
                "type": "LIMIT",
                "price": "0.0612",
                "origQty": "775",
                "positionSide": "LONG",
                "role": "take_profit_long",
            },
            {
                "orderId": 2,
                "side": "BUY",
                "type": "LIMIT",
                "price": "0.0588",
                "origQty": "765",
                "positionSide": "SHORT",
                "role": "take_profit_short",
            },
        ]
        desired = [
            {
                "side": "SELL",
                "price": 0.0614,
                "qty": 775.0,
                "notional": 47.585,
                "level": 1,
                "role": "take_profit_long",
                "position_side": "LONG",
            },
            {
                "side": "BUY",
                "price": 0.0586,
                "qty": 765.0,
                "notional": 44.829,
                "level": 1,
                "role": "take_profit_short",
                "position_side": "SHORT",
            },
        ]

        adjusted = preserve_sticky_entry_orders(
            existing_orders=existing,
            desired_orders=desired,
            price_tolerance=0.0002,
        )

        take_profit_long = next(item for item in adjusted if item["role"] == "take_profit_long")
        take_profit_short = next(item for item in adjusted if item["role"] == "take_profit_short")
        self.assertEqual(take_profit_long["price"], 0.0612)
        self.assertEqual(take_profit_short["price"], 0.0588)

    def test_preserve_sticky_entry_orders_limits_preservation_to_front_levels(self) -> None:
        existing = [
            {
                "orderId": 1,
                "side": "BUY",
                "type": "LIMIT",
                "price": "99.9",
                "origQty": "1",
                "positionSide": "BOTH",
                "role": "entry_long",
            },
            {
                "orderId": 2,
                "side": "BUY",
                "type": "LIMIT",
                "price": "99.8",
                "origQty": "1",
                "positionSide": "BOTH",
                "role": "entry_long",
            },
        ]
        desired = [
            {"side": "BUY", "price": 99.8, "qty": 1.0, "notional": 99.8, "level": 1, "role": "entry_long"},
            {"side": "BUY", "price": 99.7, "qty": 1.0, "notional": 99.7, "level": 2, "role": "entry_long"},
        ]

        adjusted = preserve_sticky_entry_orders(
            existing_orders=existing,
            desired_orders=desired,
            price_tolerance=0.1,
            max_levels_per_group=1,
        )

        self.assertEqual(adjusted[0]["price"], 99.9)

    def test_preserve_sticky_exit_orders_freezes_existing_exit_prices(self) -> None:
        existing = [
            {
                "orderId": 1,
                "side": "SELL",
                "type": "LIMIT",
                "price": "0.1731",
                "origQty": "173",
                "positionSide": "LONG",
                "role": "take_profit_long",
            },
            {
                "orderId": 2,
                "side": "SELL",
                "type": "LIMIT",
                "price": "0.1733",
                "origQty": "119",
                "positionSide": "LONG",
                "role": "take_profit_long",
            },
            {
                "orderId": 3,
                "side": "SELL",
                "type": "LIMIT",
                "price": "0.1734",
                "origQty": "50",
                "positionSide": "LONG",
                "role": "active_delever_long",
            },
        ]
        desired = [
            {
                "side": "SELL",
                "price": 0.1732,
                "qty": 173.0,
                "notional": 29.9636,
                "level": 1,
                "role": "take_profit_long",
                "position_side": "LONG",
            },
            {
                "side": "SELL",
                "price": 0.1735,
                "qty": 119.0,
                "notional": 20.6465,
                "level": 2,
                "role": "take_profit_long",
                "position_side": "LONG",
            },
            {
                "side": "SELL",
                "price": 0.1737,
                "qty": 50.0,
                "notional": 8.685,
                "level": 1,
                "role": "active_delever_long",
                "position_side": "LONG",
            },
        ]

        adjusted = preserve_sticky_exit_orders(
            existing_orders=existing,
            desired_orders=desired,
            sticky_roles={"take_profit_long", "active_delever_long"},
        )

        self.assertEqual([item["price"] for item in adjusted], [0.1731, 0.1733, 0.1734])

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

    def test_build_hedge_micro_grid_plan_can_enlarge_startup_entry_without_bootstrap(self) -> None:
        plan = build_hedge_micro_grid_plan(
            center_price=1.0,
            step_price=0.01,
            buy_levels=4,
            sell_levels=4,
            per_order_notional=25.0,
            startup_entry_multiplier=4.0,
            startup_large_entry_active=True,
            base_position_notional=0.0,
            bid_price=0.999,
            ask_price=1.001,
            tick_size=0.001,
            step_size=1.0,
            min_qty=1.0,
            min_notional=5.0,
            current_long_qty=0.0,
            current_short_qty=0.0,
        )

        self.assertEqual(plan["bootstrap_orders"], [])
        self.assertGreater(plan["buy_orders"][0]["notional"], plan["buy_orders"][1]["notional"] * 3.9)
        self.assertGreater(plan["sell_orders"][0]["notional"], plan["sell_orders"][1]["notional"] * 3.9)

        regular_plan = build_hedge_micro_grid_plan(
            center_price=1.0,
            step_price=0.01,
            buy_levels=4,
            sell_levels=4,
            per_order_notional=25.0,
            startup_entry_multiplier=4.0,
            startup_large_entry_active=False,
            base_position_notional=0.0,
            bid_price=0.999,
            ask_price=1.001,
            tick_size=0.001,
            step_size=1.0,
            min_qty=1.0,
            min_notional=5.0,
            current_long_qty=0.0,
            current_short_qty=0.0,
        )

        self.assertLess(abs(regular_plan["buy_orders"][0]["notional"] - regular_plan["buy_orders"][1]["notional"]), 0.5)
        self.assertLess(abs(regular_plan["sell_orders"][0]["notional"] - regular_plan["sell_orders"][1]["notional"]), 0.5)

    def test_build_hedge_micro_grid_plan_can_shift_buy_and_sell_sides_independently(self) -> None:
        regular = build_hedge_micro_grid_plan(
            center_price=1.0,
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
        )
        biased = build_hedge_micro_grid_plan(
            center_price=1.0,
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
            buy_offset_steps=0.5,
            sell_offset_steps=-0.5,
        )

        self.assertLess(biased["buy_orders"][0]["price"], regular["buy_orders"][0]["price"])
        self.assertEqual(biased["sell_orders"][0]["price"], regular["sell_orders"][0]["price"])
        self.assertLess(biased["sell_orders"][1]["price"], regular["sell_orders"][1]["price"])

    def test_build_hedge_micro_grid_plan_anchors_first_level_to_best_quote(self) -> None:
        plan = build_hedge_micro_grid_plan(
            center_price=4600.005,
            step_price=0.01,
            buy_levels=2,
            sell_levels=2,
            per_order_notional=25.0,
            base_position_notional=0.0,
            bid_price=4600.01,
            ask_price=4600.02,
            tick_size=0.01,
            step_size=0.001,
            min_qty=0.001,
            min_notional=5.0,
            current_long_qty=0.0,
            current_short_qty=0.0,
        )

        self.assertEqual(plan["buy_orders"][0]["price"], 4600.01)
        self.assertEqual(plan["buy_orders"][1]["price"], 4600.0)
        self.assertEqual(plan["sell_orders"][0]["price"], 4600.02)
        self.assertEqual(plan["sell_orders"][1]["price"], 4600.03)

    def test_build_hedge_micro_grid_plan_uses_center_grid_when_near_market_entries_are_disabled(self) -> None:
        plan = build_hedge_micro_grid_plan(
            center_price=100.0,
            step_price=0.5,
            buy_levels=2,
            sell_levels=2,
            per_order_notional=20.0,
            base_position_notional=0.0,
            bid_price=99.9,
            ask_price=100.1,
            tick_size=0.1,
            step_size=0.1,
            min_qty=0.1,
            min_notional=5.0,
            current_long_qty=0.0,
            current_short_qty=0.0,
            near_market_entry_max_center_distance_steps=2.0,
            near_market_entries_allowed=False,
        )

        self.assertEqual([item["price"] for item in plan["buy_orders"]], [99.5, 99.0])
        self.assertEqual([item["price"] for item in plan["sell_orders"]], [100.5, 101.0])

    def test_build_hedge_micro_grid_plan_keeps_one_small_short_probe_when_short_entries_paused_and_flat(self) -> None:
        plan = build_hedge_micro_grid_plan(
            center_price=1.0,
            step_price=0.01,
            buy_levels=4,
            sell_levels=4,
            per_order_notional=40.0,
            base_position_notional=0.0,
            bid_price=0.999,
            ask_price=1.001,
            tick_size=0.001,
            step_size=1.0,
            min_qty=1.0,
            min_notional=5.0,
            current_long_qty=0.0,
            current_short_qty=0.0,
            entry_short_paused=True,
            paused_entry_short_scale=0.25,
        )

        entry_short_orders = [item for item in plan["sell_orders"] if item["role"] == "entry_short"]

        self.assertEqual(len(entry_short_orders), 1)
        self.assertEqual(entry_short_orders[0]["level"], 1)
        self.assertEqual(entry_short_orders[0]["price"], 1.001)
        self.assertAlmostEqual(entry_short_orders[0]["notional"], 10.0, delta=1.0)

    def test_build_hedge_micro_grid_plan_paused_short_probe_stays_at_ask_even_with_existing_short_inventory(self) -> None:
        plan = build_hedge_micro_grid_plan(
            center_price=4732.8,
            step_price=1.0,
            buy_levels=20,
            sell_levels=20,
            per_order_notional=50.0,
            base_position_notional=0.0,
            bid_price=4732.89,
            ask_price=4732.9,
            tick_size=0.01,
            step_size=0.001,
            min_qty=0.001,
            min_notional=5.0,
            current_long_qty=0.0,
            current_short_qty=0.082,
            current_short_avg_price=4730.0,
            current_short_lots=[{"qty": 0.082, "price": 4732.27}],
            entry_short_paused=True,
            paused_entry_short_scale=0.25,
        )

        entry_short_orders = [item for item in plan["sell_orders"] if item["role"] == "entry_short"]

        self.assertEqual(len(entry_short_orders), 1)
        self.assertEqual(entry_short_orders[0]["price"], 4732.9)
        self.assertLess(entry_short_orders[0]["price"], 4733.27)

    def test_build_hedge_micro_grid_plan_light_long_blocks_worse_avg_cost_same_side_reentries(self) -> None:
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
            current_long_avg_price=0.995,
            entry_long_cost_guard_release_notional=100.0,
        )

        entry_long_orders = [item for item in plan["buy_orders"] if item["role"] == "entry_long"]

        self.assertEqual(entry_long_orders, [])

    def test_build_hedge_micro_grid_plan_light_long_uses_lowest_lot_over_average_cost(self) -> None:
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
            current_long_avg_price=0.995,
            current_long_lots=[
                {"qty": 20.0, "price": 0.98},
                {"qty": 20.0, "price": 1.015},
            ],
            entry_long_cost_guard_release_notional=100.0,
        )

        entry_long_prices = {item["price"] for item in plan["buy_orders"] if item["role"] == "entry_long"}

        self.assertEqual(entry_long_prices, {0.97, 0.96})
        self.assertEqual(plan["long_entry_reference_price"], 0.98)
        self.assertEqual(plan["long_exit_reference_price"], 1.015)

    def test_build_hedge_micro_grid_plan_light_long_anchors_first_profitable_sell_order(self) -> None:
        plan = build_hedge_micro_grid_plan(
            center_price=1.0,
            step_price=0.01,
            buy_levels=2,
            sell_levels=2,
            per_order_notional=25.0,
            base_position_notional=0.0,
            bid_price=0.999,
            ask_price=1.0,
            tick_size=0.001,
            step_size=1.0,
            min_qty=1.0,
            min_notional=5.0,
            current_long_qty=20.0,
            current_short_qty=0.0,
            current_long_avg_price=1.02,
            current_long_lots=[{"qty": 20.0, "price": 1.02}],
            entry_long_cost_guard_release_notional=100.0,
        )

        self.assertEqual(
            [item["price"] for item in plan["sell_orders"] if item["role"] == "take_profit_long"],
            [1.03],
        )

    def test_build_hedge_micro_grid_plan_light_short_blocks_worse_avg_cost_same_side_reentries(self) -> None:
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
            current_short_avg_price=1.005,
            entry_short_cost_guard_release_notional=100.0,
        )

        entry_short_orders = [item for item in plan["sell_orders"] if item["role"] == "entry_short"]

        self.assertEqual(entry_short_orders, [])

    def test_build_hedge_micro_grid_plan_light_short_uses_highest_lot_over_average_cost(self) -> None:
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
            current_short_avg_price=1.005,
            current_short_lots=[
                {"qty": 20.0, "price": 1.02},
                {"qty": 20.0, "price": 0.985},
            ],
            entry_short_cost_guard_release_notional=100.0,
        )

        entry_short_prices = {item["price"] for item in plan["sell_orders"] if item["role"] == "entry_short"}

        self.assertEqual(entry_short_prices, {1.03, 1.04})
        self.assertEqual(plan["short_entry_reference_price"], 1.02)
        self.assertEqual(plan["short_exit_reference_price"], 0.985)

    def test_build_hedge_micro_grid_plan_light_short_anchors_first_profitable_buy_order(self) -> None:
        plan = build_hedge_micro_grid_plan(
            center_price=1.0,
            step_price=0.01,
            buy_levels=2,
            sell_levels=2,
            per_order_notional=25.0,
            base_position_notional=0.0,
            bid_price=1.0,
            ask_price=1.001,
            tick_size=0.001,
            step_size=1.0,
            min_qty=1.0,
            min_notional=5.0,
            current_long_qty=0.0,
            current_short_qty=20.0,
            current_short_avg_price=0.98,
            current_short_lots=[{"qty": 20.0, "price": 0.98}],
            entry_short_cost_guard_release_notional=100.0,
        )

        self.assertEqual(
            [item["price"] for item in plan["buy_orders"] if item["role"] == "take_profit_short"],
            [0.97],
        )

    def test_build_hedge_micro_grid_plan_light_short_anchors_first_profitable_buy_order_to_lowest_short_lot(self) -> None:
        plan = build_hedge_micro_grid_plan(
            center_price=1.0,
            step_price=0.01,
            buy_levels=4,
            sell_levels=2,
            per_order_notional=25.0,
            base_position_notional=0.0,
            bid_price=1.001,
            ask_price=1.004,
            tick_size=0.001,
            step_size=1.0,
            min_qty=1.0,
            min_notional=5.0,
            current_long_qty=0.0,
            current_short_qty=20.0,
            current_short_avg_price=1.0025,
            current_short_lots=[
                {"qty": 10.0, "price": 1.02},
                {"qty": 10.0, "price": 0.985},
            ],
            entry_short_cost_guard_release_notional=100.0,
        )

        self.assertEqual(
            [item["price"] for item in plan["buy_orders"] if item["role"] == "take_profit_short"],
            [1.01, 0.975],
        )

    def test_build_hedge_micro_grid_plan_light_short_uses_highest_short_lot_for_front_exit_when_profit_ratio_is_zero(self) -> None:
        plan = build_hedge_micro_grid_plan(
            center_price=1.0,
            step_price=0.01,
            buy_levels=4,
            sell_levels=2,
            per_order_notional=25.0,
            base_position_notional=0.0,
            bid_price=1.001,
            ask_price=1.004,
            tick_size=0.001,
            step_size=1.0,
            min_qty=1.0,
            min_notional=5.0,
            current_long_qty=0.0,
            current_short_qty=20.0,
            current_short_avg_price=1.0025,
            current_short_lots=[
                {"qty": 10.0, "price": 1.02},
                {"qty": 10.0, "price": 0.985},
            ],
            entry_short_cost_guard_release_notional=100.0,
            take_profit_min_profit_ratio=0.0,
        )

        take_profit_short = [item for item in plan["buy_orders"] if item["role"] == "take_profit_short"]

        self.assertEqual([item["price"] for item in take_profit_short], [1.003, 0.975])
        self.assertTrue(all(item.get("lot_tracked") for item in take_profit_short))

    def test_build_hedge_micro_grid_plan_profitable_short_exit_keeps_one_step_pairing(self) -> None:
        plan = build_hedge_micro_grid_plan(
            center_price=0.955,
            step_price=0.01,
            buy_levels=4,
            sell_levels=2,
            per_order_notional=25.0,
            base_position_notional=0.0,
            bid_price=0.95,
            ask_price=0.951,
            tick_size=0.001,
            step_size=1.0,
            min_qty=1.0,
            min_notional=5.0,
            current_long_qty=0.0,
            current_short_qty=20.0,
            current_short_avg_price=1.0,
            current_short_lots=[{"qty": 20.0, "price": 1.0}],
            entry_short_cost_guard_release_notional=100.0,
        )

        self.assertEqual(
            [item["price"] for item in plan["buy_orders"] if item["role"] == "take_profit_short"],
            [0.99],
        )

    def test_build_hedge_micro_grid_plan_light_long_anchors_first_profitable_sell_order_to_highest_long_lot(self) -> None:
        plan = build_hedge_micro_grid_plan(
            center_price=1.0,
            step_price=0.01,
            buy_levels=2,
            sell_levels=4,
            per_order_notional=25.0,
            base_position_notional=0.0,
            bid_price=0.999,
            ask_price=1.0,
            tick_size=0.001,
            step_size=1.0,
            min_qty=1.0,
            min_notional=5.0,
            current_long_qty=20.0,
            current_short_qty=0.0,
            current_long_avg_price=0.9975,
            current_long_lots=[
                {"qty": 10.0, "price": 0.98},
                {"qty": 10.0, "price": 1.015},
            ],
            entry_long_cost_guard_release_notional=100.0,
        )

        self.assertEqual(
            [item["price"] for item in plan["sell_orders"] if item["role"] == "take_profit_long"],
            [0.99, 1.025],
        )

    def test_build_hedge_micro_grid_plan_profitable_long_exit_keeps_one_step_pairing(self) -> None:
        plan = build_hedge_micro_grid_plan(
            center_price=1.045,
            step_price=0.01,
            buy_levels=2,
            sell_levels=4,
            per_order_notional=25.0,
            base_position_notional=0.0,
            bid_price=1.049,
            ask_price=1.05,
            tick_size=0.001,
            step_size=1.0,
            min_qty=1.0,
            min_notional=5.0,
            current_long_qty=20.0,
            current_short_qty=0.0,
            current_long_avg_price=1.0,
            current_long_lots=[{"qty": 20.0, "price": 1.0}],
            entry_long_cost_guard_release_notional=100.0,
        )

        self.assertEqual(
            [item["price"] for item in plan["sell_orders"] if item["role"] == "take_profit_long"],
            [1.01],
        )

    def test_build_hedge_micro_grid_plan_heavy_long_keeps_lot_tracked_take_profit_orders(self) -> None:
        plan = build_hedge_micro_grid_plan(
            center_price=1.0,
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
            current_long_qty=500.0,
            current_short_qty=0.0,
            current_long_avg_price=1.004,
            current_long_lots=[
                {"qty": 200.0, "price": 0.98},
                {"qty": 300.0, "price": 1.02},
            ],
            entry_long_cost_guard_release_notional=100.0,
        )

        take_profit_long = [item for item in plan["sell_orders"] if item["role"] == "take_profit_long"]

        self.assertEqual([item["price"] for item in take_profit_long], [0.99, 1.03])
        self.assertEqual([item["qty"] for item in take_profit_long], [200.0, 300.0])
        self.assertTrue(all(item.get("lot_tracked") for item in take_profit_long))

    def test_build_hedge_micro_grid_plan_flat_entries_can_start_at_best_quote(self) -> None:
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
        )

        entry_long_prices = {item["price"] for item in plan["buy_orders"] if item["role"] == "entry_long"}
        entry_short_prices = {item["price"] for item in plan["sell_orders"] if item["role"] == "entry_short"}

        self.assertIn(1.0, entry_long_prices)
        self.assertIn(1.02, entry_short_prices)
        self.assertIn(0.99, entry_long_prices)
        self.assertIn(1.03, entry_short_prices)

    def test_build_hedge_micro_grid_plan_held_long_inventory_anchors_same_side_entries_to_lot_cost(self) -> None:
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
            current_long_qty=20.0,
            current_short_qty=0.0,
            current_long_lots=[{"qty": 20.0, "price": 1.02}],
            entry_long_cost_guard_release_notional=100.0,
            entry_short_cost_guard_release_notional=100.0,
        )

        entry_long_prices = {item["price"] for item in plan["buy_orders"] if item["role"] == "entry_long"}
        entry_short_prices = {item["price"] for item in plan["sell_orders"] if item["role"] == "entry_short"}
        take_profit_long_prices = {item["price"] for item in plan["sell_orders"] if item["role"] == "take_profit_long"}

        self.assertIn(1.01, entry_long_prices)
        self.assertIn(1.00, entry_long_prices)
        self.assertNotIn(1.02, entry_long_prices)
        self.assertEqual(entry_short_prices, set())
        self.assertIn(1.03, take_profit_long_prices)

    def test_build_hedge_micro_grid_plan_held_short_inventory_anchors_same_side_entries_to_lot_cost(self) -> None:
        plan = build_hedge_micro_grid_plan(
            center_price=0.97,
            step_price=0.01,
            buy_levels=2,
            sell_levels=2,
            per_order_notional=25.0,
            base_position_notional=0.0,
            bid_price=0.960,
            ask_price=0.980,
            tick_size=0.001,
            step_size=1.0,
            min_qty=1.0,
            min_notional=5.0,
            current_long_qty=0.0,
            current_short_qty=20.0,
            current_short_lots=[{"qty": 20.0, "price": 0.98}],
            entry_long_cost_guard_release_notional=100.0,
            entry_short_cost_guard_release_notional=100.0,
        )

        entry_long_prices = {item["price"] for item in plan["buy_orders"] if item["role"] == "entry_long"}
        entry_short_prices = {item["price"] for item in plan["sell_orders"] if item["role"] == "entry_short"}
        take_profit_short_prices = {item["price"] for item in plan["buy_orders"] if item["role"] == "take_profit_short"}

        self.assertIn(0.97, take_profit_short_prices)
        self.assertIn(0.99, entry_short_prices)
        self.assertIn(1.00, entry_short_prices)
        self.assertNotIn(0.98, entry_short_prices)
        self.assertEqual(entry_long_prices, set())

    def test_build_hedge_micro_grid_plan_pairs_each_short_lot_with_one_step_buy_order(self) -> None:
        plan = build_hedge_micro_grid_plan(
            center_price=4696.7,
            step_price=1.0,
            buy_levels=8,
            sell_levels=5,
            per_order_notional=20.0,
            base_position_notional=0.0,
            bid_price=4695.9,
            ask_price=4696.1,
            tick_size=0.1,
            step_size=0.001,
            min_qty=0.001,
            min_notional=5.0,
            current_long_qty=0.0,
            current_short_qty=0.012,
            current_short_avg_price=4697.2,
            current_short_lots=[
                {"qty": 0.004, "price": 4696.2},
                {"qty": 0.004, "price": 4697.2},
                {"qty": 0.004, "price": 4698.2},
            ],
            entry_short_cost_guard_release_notional=100.0,
        )

        take_profit_short_prices = [
            item["price"] for item in plan["buy_orders"] if item["role"] == "take_profit_short"
        ]
        entry_short_prices = [item["price"] for item in plan["sell_orders"] if item["role"] == "entry_short"]

        self.assertEqual(take_profit_short_prices, [4697.2, 4696.2, 4695.2])
        self.assertEqual(entry_short_prices[:3], [4699.2, 4700.2, 4701.2])

    def test_build_hedge_micro_grid_plan_mixed_inventory_only_uses_take_profit_orders(self) -> None:
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

        entry_short_orders = [item for item in plan["sell_orders"] if item["role"] == "entry_short"]
        entry_long_orders = [item for item in plan["buy_orders"] if item["role"] == "entry_long"]

        self.assertEqual(entry_short_orders, [])
        self.assertEqual(entry_long_orders, [])
        self.assertTrue(any(item["role"] == "take_profit_short" for item in plan["buy_orders"]))
        self.assertTrue(any(item["role"] == "take_profit_long" for item in plan["sell_orders"]))

    def test_build_hedge_micro_grid_plan_held_short_only_keeps_take_profit_short_orders(self) -> None:
        plan = build_hedge_micro_grid_plan(
            center_price=0.98,
            step_price=0.01,
            buy_levels=4,
            sell_levels=4,
            per_order_notional=25.0,
            base_position_notional=0.0,
            bid_price=0.979,
            ask_price=0.981,
            tick_size=0.001,
            step_size=1.0,
            min_qty=1.0,
            min_notional=5.0,
            current_long_qty=0.0,
            current_short_qty=50.0,
            current_short_avg_price=1.0,
            current_short_lots=[{"qty": 50.0, "price": 1.0}],
            entry_short_cost_guard_release_notional=100.0,
        )

        self.assertTrue(any(item["role"] == "take_profit_short" for item in plan["buy_orders"]))
        self.assertFalse(any(item["role"] == "entry_long" for item in plan["buy_orders"]))

    def test_build_hedge_micro_grid_plan_held_long_only_keeps_take_profit_long_orders(self) -> None:
        plan = build_hedge_micro_grid_plan(
            center_price=1.02,
            step_price=0.01,
            buy_levels=4,
            sell_levels=4,
            per_order_notional=25.0,
            base_position_notional=0.0,
            bid_price=1.019,
            ask_price=1.021,
            tick_size=0.001,
            step_size=1.0,
            min_qty=1.0,
            min_notional=5.0,
            current_long_qty=50.0,
            current_short_qty=0.0,
            current_long_avg_price=1.0,
            current_long_lots=[{"qty": 50.0, "price": 1.0}],
            entry_long_cost_guard_release_notional=100.0,
        )

        self.assertTrue(any(item["role"] == "take_profit_long" for item in plan["sell_orders"]))
        self.assertFalse(any(item["role"] == "entry_short" for item in plan["sell_orders"]))

    def test_build_hedge_micro_grid_plan_dust_long_inventory_uses_flat_entry_spacing(self) -> None:
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
            current_long_qty=2.0,
            current_short_qty=0.0,
            current_long_avg_price=1.03,
            current_long_lots=[{"qty": 2.0, "price": 1.03}],
            entry_long_cost_guard_release_notional=100.0,
        )

        entry_long_prices = [item["price"] for item in plan["buy_orders"] if item["role"] == "entry_long"]

        self.assertEqual(entry_long_prices, [1.019, 1.009])
        self.assertFalse(any(item["role"] == "take_profit_long" for item in plan["sell_orders"]))

    def test_build_hedge_micro_grid_plan_dust_short_inventory_uses_flat_entry_spacing(self) -> None:
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
            current_short_qty=2.0,
            current_short_avg_price=0.97,
            current_short_lots=[{"qty": 2.0, "price": 0.97}],
            entry_short_cost_guard_release_notional=100.0,
        )

        entry_short_prices = [item["price"] for item in plan["sell_orders"] if item["role"] == "entry_short"]

        self.assertEqual(entry_short_prices, [0.981, 0.991])
        self.assertFalse(any(item["role"] == "take_profit_short" for item in plan["buy_orders"]))

    def test_build_hedge_micro_grid_plan_dominant_short_with_tiny_long_residual_keeps_entry_short(self) -> None:
        plan = build_hedge_micro_grid_plan(
            center_price=1.0,
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
            min_notional=1.0,
            current_long_qty=2.0,
            current_short_qty=50.0,
            current_long_avg_price=1.02,
            current_short_avg_price=0.98,
            current_long_lots=[{"qty": 2.0, "price": 1.02}],
            current_short_lots=[{"qty": 50.0, "price": 0.98}],
            entry_short_cost_guard_release_notional=100.0,
        )

        self.assertTrue(any(item["role"] == "take_profit_short" for item in plan["buy_orders"]))
        self.assertTrue(any(item["role"] == "entry_short" for item in plan["sell_orders"]))
        self.assertFalse(any(item["role"] == "entry_long" for item in plan["buy_orders"]))

    def test_build_hedge_micro_grid_plan_custom_tiny_long_threshold_decouples_from_per_order_notional(self) -> None:
        plan = build_hedge_micro_grid_plan(
            center_price=1.0,
            step_price=0.01,
            buy_levels=2,
            sell_levels=2,
            per_order_notional=20.0,
            base_position_notional=0.0,
            bid_price=0.999,
            ask_price=1.001,
            tick_size=0.001,
            step_size=1.0,
            min_qty=1.0,
            min_notional=1.0,
            current_long_qty=38.0,
            current_short_qty=220.0,
            current_long_avg_price=1.02,
            current_short_avg_price=0.98,
            current_long_lots=[{"qty": 38.0, "price": 1.02}],
            current_short_lots=[{"qty": 220.0, "price": 0.98}],
            synthetic_tiny_long_residual_notional=45.0,
            entry_short_cost_guard_release_notional=100.0,
        )

        self.assertAlmostEqual(plan["synthetic_tiny_long_residual_notional"], 45.0)
        self.assertTrue(plan["dominant_short_with_tiny_long_residual"])
        self.assertTrue(any(item["role"] == "entry_short" for item in plan["sell_orders"]))
        self.assertFalse(any(item["role"] == "entry_long" for item in plan["buy_orders"]))

    def test_build_hedge_micro_grid_plan_dominant_short_with_tiny_long_residual_ignores_long_profit_guard(self) -> None:
        plan = build_hedge_micro_grid_plan(
            center_price=1.0,
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
            min_notional=1.0,
            current_long_qty=2.0,
            current_short_qty=50.0,
            current_long_avg_price=1.10,
            current_short_avg_price=0.98,
            current_long_lots=[{"qty": 2.0, "price": 1.10}],
            current_short_lots=[{"qty": 50.0, "price": 0.98}],
            entry_short_cost_guard_release_notional=100.0,
            entry_long_cost_guard_release_notional=100.0,
        )

        self.assertTrue(any(item["role"] == "entry_short" for item in plan["sell_orders"]))

    def test_build_hedge_micro_grid_plan_dominant_short_with_tiny_long_residual_reanchors_entry_short_to_market(self) -> None:
        plan = build_hedge_micro_grid_plan(
            center_price=0.3242,
            step_price=0.0001,
            buy_levels=8,
            sell_levels=4,
            per_order_notional=45.0,
            base_position_notional=0.0,
            bid_price=0.3242,
            ask_price=0.3243,
            tick_size=0.0001,
            step_size=1.0,
            min_qty=1.0,
            min_notional=5.0,
            current_long_qty=8.0,
            current_short_qty=280.0,
            current_long_avg_price=0.3188,
            current_short_avg_price=0.31995,
            current_long_lots=[{"qty": 8.0, "price": 0.3188}],
            current_short_lots=[{"qty": 140.0, "price": 0.3199}, {"qty": 140.0, "price": 0.32}],
            entry_long_cost_guard_release_notional=300.0,
            entry_short_cost_guard_release_notional=110.0,
        )

        self.assertTrue(any(item["role"] == "entry_short" for item in plan["sell_orders"]))

    def test_build_hedge_micro_grid_plan_dominant_long_with_tiny_short_residual_keeps_entry_long(self) -> None:
        plan = build_hedge_micro_grid_plan(
            center_price=1.0,
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
            min_notional=1.0,
            current_long_qty=50.0,
            current_short_qty=2.0,
            current_long_avg_price=1.02,
            current_short_avg_price=0.98,
            current_long_lots=[{"qty": 50.0, "price": 1.02}],
            current_short_lots=[{"qty": 2.0, "price": 0.98}],
            entry_long_cost_guard_release_notional=100.0,
        )

        self.assertTrue(any(item["role"] == "take_profit_long" for item in plan["sell_orders"]))
        self.assertTrue(any(item["role"] == "entry_long" for item in plan["buy_orders"]))
        self.assertFalse(any(item["role"] == "entry_short" for item in plan["sell_orders"]))

    def test_build_hedge_micro_grid_plan_dominant_long_with_tiny_short_residual_ignores_short_profit_guard(self) -> None:
        plan = build_hedge_micro_grid_plan(
            center_price=1.0,
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
            min_notional=1.0,
            current_long_qty=50.0,
            current_short_qty=2.0,
            current_long_avg_price=1.02,
            current_short_avg_price=0.90,
            current_long_lots=[{"qty": 50.0, "price": 1.02}],
            current_short_lots=[{"qty": 2.0, "price": 0.90}],
            entry_short_cost_guard_release_notional=100.0,
            entry_long_cost_guard_release_notional=100.0,
        )

        self.assertTrue(any(item["role"] == "entry_long" for item in plan["buy_orders"]))

    def test_build_hedge_micro_grid_plan_dominant_long_with_tiny_short_residual_reanchors_entry_long_to_market(self) -> None:
        plan = build_hedge_micro_grid_plan(
            center_price=0.3242,
            step_price=0.0001,
            buy_levels=8,
            sell_levels=4,
            per_order_notional=45.0,
            base_position_notional=0.0,
            bid_price=0.3242,
            ask_price=0.3243,
            tick_size=0.0001,
            step_size=1.0,
            min_qty=1.0,
            min_notional=5.0,
            current_long_qty=280.0,
            current_short_qty=8.0,
            current_long_avg_price=0.3240,
            current_short_avg_price=0.3290,
            current_long_lots=[{"qty": 140.0, "price": 0.3240}, {"qty": 140.0, "price": 0.3239}],
            current_short_lots=[{"qty": 8.0, "price": 0.3290}],
            entry_long_cost_guard_release_notional=300.0,
            entry_short_cost_guard_release_notional=110.0,
        )

        self.assertTrue(any(item["role"] == "entry_long" for item in plan["buy_orders"]))

    def test_build_hedge_micro_grid_plan_small_short_residual_can_resume_two_sided_entries(self) -> None:
        plan = build_hedge_micro_grid_plan(
            center_price=4675.5,
            step_price=0.1,
            buy_levels=2,
            sell_levels=2,
            per_order_notional=45.0,
            base_position_notional=0.0,
            bid_price=4675.4,
            ask_price=4675.5,
            tick_size=0.01,
            step_size=0.001,
            min_qty=0.001,
            min_notional=5.0,
            current_long_qty=0.0,
            current_short_qty=0.005,
            current_short_avg_price=4675.8,
            current_short_lots=[{"qty": 0.005, "price": 4675.8}],
            entry_short_cost_guard_release_notional=110.0,
            residual_short_flat_notional=30.0,
        )

        buy_roles = [item["role"] for item in plan["buy_orders"]]
        sell_roles = [item["role"] for item in plan["sell_orders"]]

        self.assertTrue(plan["residual_short_flattened"])
        self.assertAlmostEqual(plan["effective_short_qty"], 0.0)
        self.assertIn("entry_long", buy_roles)
        self.assertIn("entry_short", sell_roles)
        self.assertNotIn("take_profit_short", buy_roles)

    def test_build_hedge_micro_grid_plan_small_long_residual_can_resume_two_sided_entries(self) -> None:
        plan = build_hedge_micro_grid_plan(
            center_price=0.0563,
            step_price=0.0001,
            buy_levels=2,
            sell_levels=2,
            per_order_notional=50.0,
            base_position_notional=0.0,
            bid_price=0.0563,
            ask_price=0.0564,
            tick_size=0.0001,
            step_size=1.0,
            min_qty=1.0,
            min_notional=5.0,
            current_long_qty=901.0,
            current_short_qty=0.0,
            current_long_avg_price=0.0564,
            current_long_lots=[{"qty": 901.0, "price": 0.0564}],
            residual_long_flat_notional=300.0,
        )

        buy_roles = [item["role"] for item in plan["buy_orders"]]
        sell_roles = [item["role"] for item in plan["sell_orders"]]

        self.assertTrue(plan["residual_long_flattened"])
        self.assertAlmostEqual(plan["effective_long_qty"], 0.0)
        self.assertIn("entry_long", buy_roles)
        self.assertIn("entry_short", sell_roles)
        self.assertNotIn("take_profit_long", sell_roles)

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
            current_long_avg_price=0.995,
            entry_long_cost_guard_release_notional=100.0,
        )

        entry_long_prices = {item["price"] for item in plan["buy_orders"] if item["role"] == "entry_long"}

        self.assertIn(1.009, entry_long_prices)

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
