from __future__ import annotations

import unittest
from unittest.mock import patch

from grid_optimizer.spot_app_loss_audit import (
    build_live_spot_app_loss_audit,
    compute_spot_app_loss_audit,
    evaluate_spot_app_loss_recovery_gate,
    main,
)


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

    def test_compute_spot_app_loss_treats_tiny_net_qty_as_flat(self) -> None:
        result = compute_spot_app_loss_audit(
            trades=[
                {"isBuyer": True, "isMaker": True, "price": "0.08845", "qty": "41413.6", "quoteQty": "3662.9036"},
                {
                    "isBuyer": False,
                    "isMaker": True,
                    "price": "0.08830",
                    "qty": "41413.59999999999",
                    "quoteQty": "3656.88436",
                },
            ],
            bid_price=0.0914,
            ask_price=0.0915,
            tick_size=0.0001,
        )

        self.assertEqual(result["net_qty"], 0.0)
        self.assertEqual(result["holding_qty"], 0.0)
        self.assertEqual(result["mark_source"], "mid")
        self.assertEqual(result["break_even_price"], 0.0)
        self.assertEqual(result["safe_maker_sell_gap_ticks"], 0.0)
        gate = evaluate_spot_app_loss_recovery_gate(result, max_app_loss_per_10k=1.0, min_bid_break_even_buffer_ticks=3.0)
        self.assertEqual(gate["reasons"], ["app_loss_per_10k_above_limit"])

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

    def test_recovery_gate_allows_empty_fresh_window_when_no_minimum_gross_is_required(self) -> None:
        audit = compute_spot_app_loss_audit(trades=[], bid_price=0.0928, ask_price=0.0929, tick_size=0.00001)

        gate = evaluate_spot_app_loss_recovery_gate(audit, min_gross_notional=0.0)

        self.assertTrue(gate["allowed"])
        self.assertEqual(gate["reasons"], [])

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

    def test_recovery_gate_rejects_when_bid_break_even_buffer_is_too_small(self) -> None:
        audit = compute_spot_app_loss_audit(
            trades=[
                {"isBuyer": True, "isMaker": True, "price": "0.08848", "qty": "30794.9", "quoteQty": "2724.6815"},
                {"isBuyer": False, "isMaker": True, "price": "0.08849", "qty": "26673.1", "quoteQty": "2360.53094"},
            ],
            bid_price=0.0885,
            ask_price=0.0886,
            tick_size=0.0001,
        )

        gate = evaluate_spot_app_loss_recovery_gate(audit, min_bid_break_even_buffer_ticks=3.0)

        self.assertFalse(gate["allowed"])
        self.assertIn("bid_break_even_buffer_below_min", gate["reasons"])
        self.assertLess(gate["bid_break_even_buffer_ticks"], 3.0)

    def test_main_returns_nonzero_when_required_gate_rejects(self) -> None:
        audit = compute_spot_app_loss_audit(
            trades=[
                {"isBuyer": True, "isMaker": True, "price": "0.08848", "qty": "30794.9", "quoteQty": "2724.6815"},
                {"isBuyer": False, "isMaker": True, "price": "0.08849", "qty": "26673.1", "quoteQty": "2360.53094"},
            ],
            bid_price=0.0868,
            ask_price=0.0869,
            tick_size=0.0001,
        )
        audit.update({"symbol": "XPLUSDT", "truncated": False})

        with patch("grid_optimizer.spot_app_loss_audit.build_live_spot_app_loss_audit", return_value=audit):
            code = main(["--symbol", "XPLUSDT", "--require-gate"])

        self.assertEqual(code, 2)

    def test_main_returns_nonzero_when_required_bid_buffer_rejects(self) -> None:
        audit = compute_spot_app_loss_audit(
            trades=[
                {"isBuyer": True, "isMaker": True, "price": "0.08848", "qty": "30794.9", "quoteQty": "2724.6815"},
                {"isBuyer": False, "isMaker": True, "price": "0.08849", "qty": "26673.1", "quoteQty": "2360.53094"},
            ],
            bid_price=0.0885,
            ask_price=0.0886,
            tick_size=0.0001,
        )
        audit.update({"symbol": "XPLUSDT", "truncated": False})

        with patch("grid_optimizer.spot_app_loss_audit.build_live_spot_app_loss_audit", return_value=audit):
            code = main(["--symbol", "XPLUSDT", "--min-bid-break-even-buffer-ticks", "3", "--require-gate"])

        self.assertEqual(code, 2)

    def test_main_returns_zero_when_required_gate_allows(self) -> None:
        audit = compute_spot_app_loss_audit(
            trades=[
                {"isBuyer": True, "isMaker": True, "price": "0.08848", "qty": "30794.9", "quoteQty": "2724.6815"},
                {"isBuyer": False, "isMaker": True, "price": "0.08849", "qty": "26673.1", "quoteQty": "2360.53094"},
            ],
            bid_price=0.0894,
            ask_price=0.0895,
            tick_size=0.0001,
        )
        audit.update({"symbol": "XPLUSDT", "truncated": False})

        with patch("grid_optimizer.spot_app_loss_audit.build_live_spot_app_loss_audit", return_value=audit):
            code = main(["--symbol", "XPLUSDT", "--require-gate"])

        self.assertEqual(code, 0)

    def test_build_live_spot_app_loss_audit_paginates_multi_day_windows(self) -> None:
        first_day = [
            {"id": i, "isBuyer": True, "isMaker": True, "price": "1.0", "qty": "1", "quoteQty": "1", "time": i}
            for i in range(750)
        ]
        second_day = [
            {
                "id": 1000 + i,
                "isBuyer": False,
                "isMaker": True,
                "price": "1.01",
                "qty": "1",
                "quoteQty": "1.01",
                "time": 86_400_000 + i,
            }
            for i in range(750)
        ]
        calls: list[tuple[int | None, int | None]] = []

        def fake_fetch_spot_user_trades(**kwargs):
            start_time_ms = kwargs.get("start_time_ms")
            end_time_ms = kwargs.get("end_time_ms")
            calls.append((start_time_ms, end_time_ms))
            if start_time_ms == 0 and end_time_ms == 172_800_000:
                return (first_day + second_day)[:1000]
            if start_time_ms == 0:
                return first_day
            return second_day

        with (
            patch("grid_optimizer.spot_app_loss_audit.load_binance_api_credentials", return_value=("key", "secret")),
            patch("grid_optimizer.spot_app_loss_audit.fetch_spot_user_trades", side_effect=fake_fetch_spot_user_trades),
            patch(
                "grid_optimizer.spot_app_loss_audit.fetch_spot_book_tickers",
                return_value=[{"bid_price": "1.0", "ask_price": "1.01"}],
            ),
            patch("grid_optimizer.spot_app_loss_audit.fetch_spot_symbol_config", return_value={"tick_size": 0.01}),
        ):
            audit = build_live_spot_app_loss_audit(
                symbol="MEGAUSDT",
                start_time_ms=0,
                end_time_ms=172_800_000,
                limit=1000,
            )

        self.assertEqual(audit["trade_count"], 1500)
        self.assertEqual(audit["maker_count"], 1500)
        self.assertFalse(audit["truncated"])
        self.assertGreaterEqual(len(calls), 2)


if __name__ == "__main__":
    unittest.main()
