from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from grid_optimizer.submit_plan import (
    adjust_post_only_price,
    build_execution_actions,
    estimate_mid_drift_steps,
    preserve_queue_priority_in_execution_actions,
    prepare_post_only_order_request,
    validate_plan_report,
)


class SubmitPlanTests(unittest.TestCase):
    def test_build_execution_actions_combines_bootstrap_and_missing_orders(self) -> None:
        report = {
            "bootstrap_orders": [
                {"side": "BUY", "price": 0.05043, "qty": 3011.0, "notional": 151.84473, "role": "bootstrap"}
            ],
            "missing_orders": [
                {"side": "BUY", "price": 0.05029, "qty": 502.0, "notional": 25.24558, "role": "entry"},
                {"side": "SELL", "price": 0.05047, "qty": 500.0, "notional": 25.23500, "role": "take_profit"},
            ],
            "stale_orders": [{"orderId": 1, "side": "BUY", "price": "0.05001", "origQty": "500"}],
        }

        actions = build_execution_actions(report)

        self.assertEqual(actions["place_count"], 3)
        self.assertEqual(actions["cancel_count"], 1)
        self.assertAlmostEqual(actions["place_notional"], 202.32531, places=8)

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
            "stale_orders": [{"orderId": 1, "side": "BUY", "price": "0.05001", "origQty": "500"}],
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


if __name__ == "__main__":
    unittest.main()
