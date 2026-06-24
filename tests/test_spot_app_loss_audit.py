from __future__ import annotations

import unittest

from grid_optimizer.spot_app_loss_audit import compute_spot_app_loss_audit, evaluate_spot_app_loss_recovery_gate


class SpotAppLossAuditTests(unittest.TestCase):
    def test_compute_spot_app_loss_matches_binance_app_formula_for_long_window(self) -> None:
        result = compute_spot_app_loss_audit(
            trades=[
                {
                    "id": 1,
                    "isBuyer": True,
                    "isMaker": True,
                    "price": "0.08848071697943515",
                    "qty": "30794.9",
                    "quoteQty": "2724.6815",
                    "time": "1000",
                },
                {
                    "id": 2,
                    "isBuyer": False,
                    "isMaker": True,
                    "price": "0.08849647300501478",
                    "qty": "26673.1",
                    "quoteQty": "2360.53094",
                    "time": "2000",
                },
            ],
            bid_price=0.0868,
            ask_price=0.0869,
            tick_size=0.0001,
        )

        self.assertEqual(result["trade_count"], 2)
        self.assertEqual(result["maker_count"], 2)
        self.assertAlmostEqual(result["buy_notional"], 2724.6815)
        self.assertAlmostEqual(result["sell_notional"], 2360.53094)
        self.assertAlmostEqual(result["net_qty"], 4121.8)
        self.assertEqual(result["mark_source"], "bid")
        self.assertAlmostEqual(result["mark_price"], 0.0868)
        self.assertAlmostEqual(result["app_loss"], 6.37832, places=5)
        self.assertAlmostEqual(result["app_loss_per_10k"], 12.542878149649142)
        self.assertAlmostEqual(result["break_even_price"], 0.08834745984763938)
        self.assertAlmostEqual(result["safe_maker_sell_price"], 0.0884)
        self.assertAlmostEqual(result["safe_maker_sell_gap_ticks"], 15.0)

    def test_compute_spot_app_loss_uses_mid_when_window_is_flat(self) -> None:
        result = compute_spot_app_loss_audit(
            trades=[
                {"isBuyer": True, "isMaker": True, "price": "1.0", "qty": "10", "quoteQty": "10"},
                {"isBuyer": False, "isMaker": True, "price": "1.01", "qty": "10", "quoteQty": "10.1"},
            ],
            bid_price=1.0,
            ask_price=1.02,
            tick_size=0.01,
        )

        self.assertEqual(result["net_qty"], 0.0)
        self.assertEqual(result["mark_source"], "mid")
        self.assertAlmostEqual(result["raw_app_loss"], -0.1)
        self.assertEqual(result["app_loss"], 0.0)
        self.assertEqual(result["safe_maker_sell_price"], 0.0)

    def test_recovery_gate_allows_small_observation_when_loss_and_gap_are_good(self) -> None:
        audit = compute_spot_app_loss_audit(
            trades=[
                {"isBuyer": True, "isMaker": True, "price": "0.08848", "qty": "30794.9", "quoteQty": "2724.6815"},
                {"isBuyer": False, "isMaker": True, "price": "0.08849", "qty": "26673.1", "quoteQty": "2360.53094"},
            ],
            bid_price=0.0894,
            ask_price=0.0895,
            tick_size=0.0001,
        )

        gate = evaluate_spot_app_loss_recovery_gate(audit)

        self.assertTrue(gate["allowed"])
        self.assertEqual(gate["reasons"], [])
        self.assertEqual(gate["maker_ratio"], 1.0)
        self.assertEqual(gate["app_loss_per_10k"], 0.0)
        self.assertLessEqual(gate["safe_maker_sell_gap_ticks"], 2.0)

    def test_recovery_gate_rejects_high_loss_and_far_safe_maker_sell(self) -> None:
        audit = compute_spot_app_loss_audit(
            trades=[
                {"isBuyer": True, "isMaker": True, "price": "0.08848", "qty": "30794.9", "quoteQty": "2724.6815"},
                {"isBuyer": False, "isMaker": True, "price": "0.08849", "qty": "26673.1", "quoteQty": "2360.53094"},
            ],
            bid_price=0.0868,
            ask_price=0.0869,
            tick_size=0.0001,
        )

        gate = evaluate_spot_app_loss_recovery_gate(audit)

        self.assertFalse(gate["allowed"])
        self.assertIn("app_loss_per_10k_above_limit", gate["reasons"])
        self.assertIn("safe_maker_sell_too_far", gate["reasons"])


if __name__ == "__main__":
    unittest.main()
