from __future__ import annotations

import unittest

from grid_optimizer.web import (
    MANUAL_TRADE_PAGE,
    _build_manual_trade_plan,
    _is_manual_trade_order,
    _manual_trade_client_order_prefix,
)


class ManualTradeTests(unittest.TestCase):
    def _symbol_info(self) -> dict[str, float]:
        return {
            "tick_size": 0.0001,
            "step_size": 0.1,
            "min_qty": 0.1,
            "min_notional": 5.0,
        }

    def test_manual_trade_prefix_is_symbol_scoped(self) -> None:
        prefix = _manual_trade_client_order_prefix("BARDUSDT")

        self.assertTrue(prefix.startswith("mt_bardusdt"))
        self.assertTrue(_is_manual_trade_order({"clientOrderId": f"{prefix}_buy_123"}, prefix))
        self.assertFalse(_is_manual_trade_order({"clientOrderId": "grid_bardusdt_buy_123"}, prefix))

    def test_buy_plan_closes_short_before_opening_long(self) -> None:
        plan = _build_manual_trade_plan(
            symbol="BARDUSDT",
            side="BUY",
            notional=100.0,
            bid_price=1.0,
            ask_price=1.01,
            position_amt=-40.0,
            symbol_info=self._symbol_info(),
        )

        self.assertEqual([leg["role"] for leg in plan["legs"]], ["close_short", "open_long"])
        self.assertEqual(plan["legs"][0]["side"], "BUY")
        self.assertTrue(plan["legs"][0]["reduce_only"])
        self.assertAlmostEqual(plan["legs"][0]["quantity"], 40.0)
        self.assertFalse(plan["legs"][1]["reduce_only"])
        self.assertAlmostEqual(plan["legs"][1]["quantity"], 59.0)

    def test_sell_plan_closes_long_before_opening_short(self) -> None:
        plan = _build_manual_trade_plan(
            symbol="BARDUSDT",
            side="SELL",
            notional=80.0,
            bid_price=2.0,
            ask_price=2.02,
            position_amt=12.0,
            symbol_info=self._symbol_info(),
        )

        self.assertEqual([leg["role"] for leg in plan["legs"]], ["close_long", "open_short"])
        self.assertEqual(plan["legs"][0]["side"], "SELL")
        self.assertTrue(plan["legs"][0]["reduce_only"])
        self.assertAlmostEqual(plan["legs"][0]["quantity"], 12.0)
        self.assertFalse(plan["legs"][1]["reduce_only"])
        self.assertAlmostEqual(plan["legs"][1]["quantity"], 28.0)

    def test_plan_rejects_too_small_notional_after_rounding(self) -> None:
        with self.assertRaisesRegex(ValueError, "below minimum notional"):
            _build_manual_trade_plan(
                symbol="BARDUSDT",
                side="BUY",
                notional=1.0,
                bid_price=1.0,
                ask_price=1.01,
                position_amt=0.0,
                symbol_info=self._symbol_info(),
            )

    def test_manual_trade_page_contains_required_controls(self) -> None:
        self.assertIn('id="manual_symbol"', MANUAL_TRADE_PAGE)
        self.assertIn('id="manual_notional"', MANUAL_TRADE_PAGE)
        self.assertIn("/api/manual_trade/status", MANUAL_TRADE_PAGE)
        self.assertIn("/api/manual_trade/maker", MANUAL_TRADE_PAGE)
        self.assertIn("/api/manual_trade/take", MANUAL_TRADE_PAGE)
        self.assertIn("/api/manual_trade/cancel", MANUAL_TRADE_PAGE)

    def test_monitor_page_links_to_manual_trade_page(self) -> None:
        from grid_optimizer.web import MONITOR_PAGE

        self.assertIn('href="/manual_trade"', MONITOR_PAGE)


if __name__ == "__main__":
    unittest.main()
