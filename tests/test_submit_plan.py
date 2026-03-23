from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from grid_optimizer.submit_plan import (
    adjust_post_only_price,
    build_execution_actions,
    estimate_mid_drift_steps,
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


if __name__ == "__main__":
    unittest.main()
