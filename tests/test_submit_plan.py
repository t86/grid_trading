from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from grid_optimizer.submit_plan import (
    adjust_post_only_price,
    apply_anti_chase_entry_guard_to_actions,
    build_execution_actions,
    cap_reduce_only_place_orders_to_position,
    enforce_execution_action_limits,
    estimate_mid_drift_steps,
    filter_strategy_open_orders,
    preserve_queue_priority_in_execution_actions,
    prepare_post_only_order_request,
    validate_plan_report,
)


class SubmitPlanTests(unittest.TestCase):
    def test_filter_strategy_open_orders_excludes_manual_orders(self) -> None:
        result = filter_strategy_open_orders(
            [
                {"clientOrderId": "gx-btcusdc-entry-1", "orderId": 1},
                {"clientOrderId": "mt_btcusdc_buy_1", "orderId": 2},
                {"clientOrderId": "", "orderId": 3},
            ],
            "BTCUSDC",
        )

        self.assertEqual(result, [{"clientOrderId": "gx-btcusdc-entry-1", "orderId": 1}])

    def test_build_execution_actions_combines_bootstrap_and_missing_orders(self) -> None:
        report = {
            "symbol": "BARDUSDT",
            "bootstrap_orders": [
                {"side": "BUY", "price": 0.05043, "qty": 3011.0, "notional": 151.84473, "role": "bootstrap"}
            ],
            "missing_orders": [
                {"side": "BUY", "price": 0.05029, "qty": 502.0, "notional": 25.24558, "role": "entry"},
                {"side": "SELL", "price": 0.05047, "qty": 500.0, "notional": 25.23500, "role": "take_profit"},
            ],
            "stale_orders": [
                {"orderId": 1, "clientOrderId": "gx-bardu-entry-1", "side": "BUY", "price": "0.05001", "origQty": "500"}
            ],
        }

        actions = build_execution_actions(report)

        self.assertEqual(actions["place_count"], 3)
        self.assertEqual(actions["cancel_count"], 1)
        self.assertAlmostEqual(actions["place_notional"], 202.32531, places=8)

    def test_build_execution_actions_excludes_manual_stale_orders(self) -> None:
        report = {
            "symbol": "BTCUSDC",
            "bootstrap_orders": [],
            "missing_orders": [],
            "stale_orders": [
                {"orderId": 1, "clientOrderId": "gx-btcusdc-entry-1", "side": "BUY", "price": "1", "origQty": "1"},
                {"orderId": 2, "clientOrderId": "mt_btcusdc_openlong_b_1", "side": "BUY", "price": "1", "origQty": "1"},
            ],
        }

        actions = build_execution_actions(report)

        self.assertEqual(actions["cancel_orders"], [report["stale_orders"][0]])
        self.assertEqual(actions["cancel_count"], 1)

    def test_reduce_only_cap_counts_orders_pending_cancel_until_exchange_releases_qty(self) -> None:
        actions = {
            "place_orders": [
                {"side": "BUY", "price": 0.1674, "qty": 358.0, "notional": 59.9292, "role": "take_profit_short"}
            ],
            "cancel_orders": [
                {"orderId": 1, "side": "BUY", "price": "0.1676", "origQty": "358", "reduceOnly": True}
            ],
            "place_count": 1,
            "cancel_count": 1,
        }
        current_open_orders = [
            {
                "orderId": 1,
                "side": "BUY",
                "price": "0.1676",
                "origQty": "358",
                "executedQty": "0",
                "reduceOnly": True,
            }
        ]

        capped = cap_reduce_only_place_orders_to_position(
            actions=actions,
            strategy_mode="synthetic_neutral",
            current_actual_net_qty=-358.0,
            current_open_orders=current_open_orders,
        )

        self.assertEqual(capped["place_orders"], [])
        self.assertEqual(capped["cancel_orders"], actions["cancel_orders"])
        self.assertEqual(capped["place_count"], 0)
        self.assertEqual(capped["cancel_count"], 1)
        self.assertEqual(capped["reduce_only_position_cap"]["remaining_buy_reduce_qty"], 0.0)
        self.assertEqual(capped["reduce_only_position_cap"]["dropped_order_count"], 1)

    def test_reduce_only_cap_treats_flow_sleeve_short_as_reduce_only_buy(self) -> None:
        actions = {
            "place_orders": [
                {"side": "BUY", "price": 0.1690, "qty": 150.0, "notional": 25.35, "role": "flow_sleeve_short"},
                {"side": "BUY", "price": 0.1688, "qty": 150.0, "notional": 25.32, "role": "flow_sleeve_short"},
            ],
            "cancel_orders": [],
            "place_count": 2,
            "cancel_count": 0,
        }

        capped = cap_reduce_only_place_orders_to_position(
            actions=actions,
            strategy_mode="synthetic_neutral",
            current_actual_net_qty=-200.0,
            current_open_orders=[],
        )

        self.assertEqual(capped["place_count"], 2)
        self.assertEqual(capped["place_orders"][0]["qty"], 150.0)
        self.assertEqual(capped["place_orders"][1]["qty"], 50.0)
        self.assertEqual(capped["reduce_only_position_cap"]["resized_order_count"], 1)

    def test_urgent_reduce_only_displaces_existing_take_profit_capacity(self) -> None:
        actions = {
            "place_orders": [
                {
                    "side": "BUY",
                    "price": 2.382,
                    "qty": 283.6,
                    "notional": 675.5352,
                    "role": "active_delever_short",
                    "force_reduce_only": True,
                    "execution_type": "maker_timeout_release",
                }
            ],
            "cancel_orders": [],
            "place_count": 1,
            "cancel_count": 0,
        }
        current_open_orders = [
            {
                "orderId": 1019001946,
                "side": "BUY",
                "price": "2.360000",
                "origQty": "283.60",
                "executedQty": "0",
                "reduceOnly": True,
                "clientOrderId": "gx-trumpusdc-takeprof-1-89452771",
            }
        ]

        capped = cap_reduce_only_place_orders_to_position(
            actions=actions,
            strategy_mode="synthetic_neutral",
            current_actual_net_qty=-283.6,
            current_open_orders=current_open_orders,
        )

        self.assertEqual(capped["place_count"], 1)
        self.assertEqual(capped["cancel_count"], 1)
        self.assertEqual(capped["place_orders"][0]["role"], "active_delever_short")
        self.assertEqual(capped["cancel_orders"][0]["orderId"], 1019001946)
        self.assertEqual(capped["cancel_orders"][0]["cancel_reason"], "urgent_reduce_only_displaces_take_profit")
        self.assertEqual(capped["reduce_only_position_cap"]["dropped_order_count"], 0)
        self.assertEqual(capped["reduce_only_position_cap"]["displaced_order_count"], 1)

    def test_anti_chase_guard_drops_long_entries_but_keeps_reduce_only_sells(self) -> None:
        actions = {
            "place_orders": [
                {"side": "BUY", "price": 2.42, "qty": 10.0, "notional": 24.2, "role": "entry"},
                {
                    "side": "SELL",
                    "price": 2.43,
                    "qty": 10.0,
                    "notional": 24.3,
                    "role": "active_delever_long",
                    "force_reduce_only": True,
                },
            ],
            "cancel_orders": [],
            "place_count": 2,
            "cancel_count": 0,
            "place_notional": 48.5,
        }
        plan_report = {
            "anti_chase_entry_guard": {
                "enabled": True,
                "block_long_entries": True,
                "block_short_entries": False,
                "long_reason": "window_1m return=0.30% >= 0.25%",
            }
        }

        guarded = apply_anti_chase_entry_guard_to_actions(
            actions=actions,
            plan_report=plan_report,
            strategy_mode="synthetic_neutral",
        )

        self.assertEqual(guarded["place_count"], 1)
        self.assertEqual(guarded["place_orders"][0]["role"], "active_delever_long")
        self.assertEqual(guarded["anti_chase_entry_guard"]["dropped_order_count"], 1)

    def test_anti_chase_guard_drops_short_entries_but_keeps_short_exits(self) -> None:
        actions = {
            "place_orders": [
                {"side": "SELL", "price": 2.30, "qty": 10.0, "notional": 23.0, "role": "entry_short"},
                {"side": "BUY", "price": 2.29, "qty": 10.0, "notional": 22.9, "role": "take_profit_short"},
            ],
            "cancel_orders": [],
            "place_count": 2,
            "cancel_count": 0,
            "place_notional": 45.9,
        }
        plan_report = {
            "anti_chase_entry_guard": {
                "enabled": True,
                "block_long_entries": False,
                "block_short_entries": True,
                "short_reason": "window_1m return=-0.30% <= -0.25%",
            }
        }

        guarded = apply_anti_chase_entry_guard_to_actions(
            actions=actions,
            plan_report=plan_report,
            strategy_mode="synthetic_neutral",
        )

        self.assertEqual(guarded["place_count"], 1)
        self.assertEqual(guarded["place_orders"][0]["role"], "take_profit_short")
        self.assertEqual(guarded["anti_chase_entry_guard"]["dropped_order_count"], 1)

    def test_deferred_action_limits_allow_capped_reduce_only_orders(self) -> None:
        now = datetime(2026, 5, 5, 0, 20, tzinfo=timezone.utc)
        report = {
            "symbol": "CHIPUSDT",
            "generated_at": now.isoformat(),
            "dual_side_position": False,
            "bootstrap_orders": [],
            "missing_orders": [
                {
                    "side": "BUY",
                    "price": 0.06064,
                    "qty": 5904.0,
                    "notional": 358.01856,
                    "role": "active_delever_short",
                    "force_reduce_only": True,
                },
                {
                    "side": "BUY",
                    "price": 0.06044,
                    "qty": 5904.0,
                    "notional": 356.83776,
                    "role": "take_profit_short",
                },
            ],
            "stale_orders": [],
        }

        validation = validate_plan_report(
            plan_report=report,
            allow_symbol="CHIPUSDT",
            max_new_orders=10,
            max_total_notional=360.0,
            cancel_stale=True,
            max_plan_age_seconds=60,
            now=now,
            enforce_place_limits=False,
        )
        validation["actions"] = cap_reduce_only_place_orders_to_position(
            actions=validation["actions"],
            strategy_mode="synthetic_neutral",
            current_actual_net_qty=-5904.0,
            current_open_orders=[],
        )
        validation = enforce_execution_action_limits(
            validation=validation,
            max_new_orders=10,
            max_total_notional=360.0,
        )

        self.assertTrue(validation["ok"])
        self.assertEqual(validation["actions"]["place_count"], 1)
        self.assertAlmostEqual(validation["actions"]["place_notional"], 358.01856, places=8)
        self.assertEqual(validation["actions"]["reduce_only_position_cap"]["dropped_order_count"], 1)

    def test_preserve_queue_priority_drops_replace_when_post_only_projects_back_to_same_bucket(self) -> None:
        actions = {
            "place_orders": [
                {"side": "BUY", "price": 0.05064, "qty": 500.0, "notional": 25.32, "role": "entry"}
            ],
            "cancel_orders": [
                {"orderId": 1, "side": "BUY", "price": "0.05062", "origQty": "500", "positionSide": "BOTH"}
            ],
        }

        adjusted = preserve_queue_priority_in_execution_actions(
            actions=actions,
            live_bid_price=0.05062,
            live_ask_price=0.05063,
            tick_size=0.00001,
            min_qty=0.1,
            min_notional=5.0,
        )

        self.assertEqual(adjusted["place_count"], 0)
        self.assertEqual(adjusted["cancel_count"], 0)

    def test_preserve_queue_priority_only_places_delta_when_post_only_projects_back_to_same_bucket(self) -> None:
        actions = {
            "place_orders": [
                {"side": "BUY", "price": 0.05064, "qty": 650.0, "notional": 32.916, "role": "entry"}
            ],
            "cancel_orders": [
                {"orderId": 1, "side": "BUY", "price": "0.05062", "origQty": "500", "positionSide": "BOTH"}
            ],
        }

        adjusted = preserve_queue_priority_in_execution_actions(
            actions=actions,
            live_bid_price=0.05062,
            live_ask_price=0.05063,
            tick_size=0.00001,
            min_qty=0.1,
            min_notional=5.0,
        )

        self.assertEqual(adjusted["cancel_count"], 0)
        self.assertEqual(adjusted["place_count"], 1)
        self.assertAlmostEqual(adjusted["place_orders"][0]["price"], 0.05062, places=8)
        self.assertAlmostEqual(adjusted["place_orders"][0]["qty"], 150.0, places=8)

    def test_preserve_queue_priority_keeps_replace_when_projected_bucket_needs_smaller_qty(self) -> None:
        actions = {
            "place_orders": [
                {"side": "BUY", "price": 0.05064, "qty": 350.0, "notional": 17.724, "role": "entry"}
            ],
            "cancel_orders": [
                {"orderId": 1, "side": "BUY", "price": "0.05062", "origQty": "500", "positionSide": "BOTH"}
            ],
        }

        adjusted = preserve_queue_priority_in_execution_actions(
            actions=actions,
            live_bid_price=0.05062,
            live_ask_price=0.05063,
            tick_size=0.00001,
            min_qty=0.1,
            min_notional=5.0,
        )

        self.assertEqual(adjusted["cancel_count"], 1)
        self.assertEqual(adjusted["place_count"], 1)
        self.assertAlmostEqual(adjusted["place_orders"][0]["price"], 0.05062, places=8)
        self.assertAlmostEqual(adjusted["place_orders"][0]["qty"], 350.0, places=8)

    def test_preserve_queue_priority_keeps_exact_same_bucket_subset_when_qty_shrinks(self) -> None:
        actions = {
            "place_orders": [
                {"side": "BUY", "price": 0.0564, "qty": 886.0, "notional": 49.9704, "role": "take_profit_short"}
            ],
            "cancel_orders": [
                {"orderId": 1, "side": "BUY", "price": "0.0564", "origQty": "884", "positionSide": "BOTH"},
                {"orderId": 2, "side": "BUY", "price": "0.0564", "origQty": "886", "positionSide": "BOTH"},
            ],
        }

        adjusted = preserve_queue_priority_in_execution_actions(
            actions=actions,
            live_bid_price=0.0564,
            live_ask_price=0.0565,
            tick_size=0.0001,
            min_qty=0.1,
            min_notional=5.0,
        )

        self.assertEqual(adjusted["place_count"], 0)
        self.assertEqual(adjusted["cancel_count"], 1)
        self.assertEqual(adjusted["cancel_orders"][0]["orderId"], 1)

    def test_preserve_queue_priority_keeps_subset_and_places_delta_when_qty_shrinks(self) -> None:
        actions = {
            "place_orders": [
                {"side": "BUY", "price": 0.05064, "qty": 650.0, "notional": 32.916, "role": "entry"}
            ],
            "cancel_orders": [
                {"orderId": 1, "side": "BUY", "price": "0.05062", "origQty": "500", "positionSide": "BOTH"},
                {"orderId": 2, "side": "BUY", "price": "0.05062", "origQty": "500", "positionSide": "BOTH"},
            ],
        }

        adjusted = preserve_queue_priority_in_execution_actions(
            actions=actions,
            live_bid_price=0.05062,
            live_ask_price=0.05063,
            tick_size=0.00001,
            min_qty=0.1,
            min_notional=5.0,
        )

        self.assertEqual(adjusted["cancel_count"], 1)
        self.assertEqual(adjusted["place_count"], 1)
        self.assertAlmostEqual(adjusted["place_orders"][0]["price"], 0.05062, places=8)
        self.assertAlmostEqual(adjusted["place_orders"][0]["qty"], 150.0, places=8)

    def test_validate_plan_report_rejects_old_plan_and_stale_orders_without_flag(self) -> None:
        now = datetime(2026, 3, 16, 10, 0, tzinfo=timezone.utc)
        report = {
            "symbol": "NIGHTUSDT",
            "generated_at": (now - timedelta(seconds=120)).isoformat(),
            "dual_side_position": False,
            "bootstrap_orders": [],
            "missing_orders": [{"side": "BUY", "price": 0.05029, "qty": 502.0, "notional": 25.24558}],
            "stale_orders": [
                {"orderId": 1, "clientOrderId": "gx-nightu-entry-1", "side": "BUY", "price": "0.05001", "origQty": "500"}
            ],
        }

        validation = validate_plan_report(
            plan_report=report,
            allow_symbol="NIGHTUSDT",
            max_new_orders=24,
            max_total_notional=500.0,
            cancel_stale=False,
            max_plan_age_seconds=60,
            now=now,
        )

        self.assertFalse(validation["ok"])
        self.assertEqual(len(validation["errors"]), 2)

    def test_validate_plan_report_rejects_symbol_and_notional_limit(self) -> None:
        now = datetime(2026, 3, 16, 10, 0, tzinfo=timezone.utc)
        report = {
            "symbol": "ENSOUSDT",
            "generated_at": now.isoformat(),
            "dual_side_position": False,
            "bootstrap_orders": [{"side": "BUY", "price": 1.2, "qty": 100.0, "notional": 120.0}],
            "missing_orders": [{"side": "BUY", "price": 1.1, "qty": 100.0, "notional": 110.0}],
            "stale_orders": [],
        }

        validation = validate_plan_report(
            plan_report=report,
            allow_symbol="NIGHTUSDT",
            max_new_orders=1,
            max_total_notional=200.0,
            cancel_stale=True,
            max_plan_age_seconds=60,
            now=now,
        )

        self.assertFalse(validation["ok"])
        self.assertEqual(len(validation["errors"]), 3)

    def test_estimate_mid_drift_steps(self) -> None:
        drift = estimate_mid_drift_steps(
            report_mid_price=0.05043,
            live_bid_price=0.05047,
            live_ask_price=0.05048,
            step_price=0.00002,
        )

        self.assertAlmostEqual(drift, 2.25, places=8)

    def test_adjust_post_only_price_moves_inside_resting_side(self) -> None:
        buy_price = adjust_post_only_price(
            desired_price=0.05064,
            side="BUY",
            live_bid_price=0.05062,
            live_ask_price=0.05063,
            tick_size=0.00001,
        )
        sell_price = adjust_post_only_price(
            desired_price=0.05060,
            side="SELL",
            live_bid_price=0.05058,
            live_ask_price=0.05059,
            tick_size=0.00001,
        )

        self.assertAlmostEqual(buy_price, 0.05062, places=8)
        self.assertAlmostEqual(sell_price, 0.05060, places=8)

    def test_prepare_post_only_order_request_skips_order_below_min_notional_after_price_adjustment(self) -> None:
        prepared, skipped = prepare_post_only_order_request(
            order={"qty": 9.9, "price": 0.51},
            side="BUY",
            live_bid_price=0.49,
            live_ask_price=0.51,
            tick_size=0.01,
            min_qty=0.1,
            min_notional=5.0,
        )

        self.assertIsNone(prepared)
        self.assertEqual(skipped["reason"], "submitted_notional_below_min_notional")
        self.assertAlmostEqual(skipped["submitted_price"], 0.50, places=8)
        self.assertAlmostEqual(skipped["submitted_notional"], 4.95, places=8)

    def test_prepare_post_only_order_request_raises_entry_qty_to_min_notional(self) -> None:
        prepared, skipped = prepare_post_only_order_request(
            order={"role": "entry_long", "qty": 24.0, "price": 0.172},
            side="BUY",
            live_bid_price=0.1719,
            live_ask_price=0.1721,
            tick_size=0.0001,
            min_qty=1.0,
            min_notional=5.0,
            step_size=1.0,
        )

        self.assertIsNone(skipped)
        self.assertEqual(prepared["qty"], 30.0)
        self.assertAlmostEqual(prepared["submitted_price"], 0.172, places=8)
        self.assertAlmostEqual(prepared["submitted_notional"], 5.16, places=8)
        self.assertTrue(prepared["qty_bumped_to_min_order"])

    def test_prepare_post_only_order_request_does_not_raise_take_profit_qty_to_min_notional(self) -> None:
        prepared, skipped = prepare_post_only_order_request(
            order={"role": "take_profit_long", "qty": 24.0, "price": 0.172},
            side="SELL",
            live_bid_price=0.1719,
            live_ask_price=0.1721,
            tick_size=0.0001,
            min_qty=1.0,
            min_notional=5.0,
            step_size=1.0,
        )

        self.assertIsNone(prepared)
        self.assertEqual(skipped["reason"], "submitted_notional_below_min_notional")


if __name__ == "__main__":
    unittest.main()
