from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from grid_optimizer.recovery_stall_monitor import assess_recovery_stall, update_stall_state


class RecoveryStallMonitorTests(unittest.TestCase):
    def test_normal_bilateral_plan_resets_state(self) -> None:
        now = datetime(2026, 5, 18, 0, 0, tzinfo=timezone.utc)
        assessment = assess_recovery_stall(
            symbol="BILLUSDT",
            now=now,
            plan={
                "generated_at": now.isoformat(),
                "best_quote_maker_volume": {"regime": "normal"},
                "volatility_entry_pause": {"active": False},
                "buy_orders": [{"side": "BUY"}],
                "sell_orders": [{"side": "SELL"}],
            },
            submit={"generated_at": now.isoformat(), "validation": {"actions": {"place_orders": []}}},
        )
        state, should_alert = update_stall_state(
            symbol="BILLUSDT",
            assessment=assessment,
            state={},
            now=now,
            threshold_seconds=600,
        )

        self.assertTrue(assessment["normal_bilateral"])
        self.assertFalse(should_alert)
        self.assertEqual(state["BILLUSDT"]["status"], "normal")

    def test_non_bilateral_over_ten_minutes_alerts_once(self) -> None:
        now = datetime(2026, 5, 18, 0, 12, tzinfo=timezone.utc)
        assessment = assess_recovery_stall(
            symbol="BILLUSDT",
            now=now,
            plan={
                "generated_at": now.isoformat(),
                "best_quote_maker_volume": {"regime": "inventory_recover", "reasons": ["inventory_soft"]},
                "volatility_entry_pause": {"active": False},
                "buy_orders": [],
                "sell_orders": [{"side": "SELL", "force_reduce_only": True}],
            },
            submit={
                "generated_at": now.isoformat(),
                "validation": {"actions": {"place_orders": [{"side": "SELL", "force_reduce_only": True}]}},
            },
        )
        state = {
            "BILLUSDT": {
                "status": "non_normal",
                "first_non_normal_at": (now - timedelta(minutes=11)).isoformat(),
                "reasons": assessment["reasons"],
                "alert_sent": False,
            }
        }

        state, should_alert = update_stall_state(
            symbol="BILLUSDT",
            assessment=assessment,
            state=state,
            now=now,
            threshold_seconds=600,
        )
        state, should_alert_again = update_stall_state(
            symbol="BILLUSDT",
            assessment=assessment,
            state=state,
            now=now + timedelta(minutes=1),
            threshold_seconds=600,
        )

        self.assertFalse(assessment["normal_bilateral"])
        self.assertIn("best_quote_regime:inventory_recover", assessment["reasons"])
        self.assertIn("not_bilateral_orders", assessment["reasons"])
        self.assertTrue(should_alert)
        self.assertFalse(should_alert_again)


if __name__ == "__main__":
    unittest.main()
