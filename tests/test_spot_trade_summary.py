from __future__ import annotations

import unittest

from grid_optimizer.spot_trade_summary import summarize_spot_trades


class SpotTradeSummaryTests(unittest.TestCase):
    def test_summarize_spot_trades_builds_window_metrics(self) -> None:
        now_ms = 1_700_000_000_000
        trades = [
            {"time": now_ms - 5 * 60_000, "isBuyer": True, "quoteQty": "10.5", "qty": "100", "price": "0.105"},
            {"time": now_ms - 8 * 60_000, "isBuyer": False, "quoteQty": "12.0", "qty": "110", "price": "0.1090909"},
            {"time": now_ms - 30 * 60_000, "isBuyer": True, "quoteQty": "20.0", "qty": "200", "price": "0.10"},
            {"time": now_ms - 3 * 60 * 60_000, "isBuyer": False, "quoteQty": "40.0", "qty": "400", "price": "0.10"},
            {"time": now_ms - 30 * 60 * 60_000, "isBuyer": False, "quoteQty": "99.0", "qty": "900", "price": "0.11"},
        ]

        result = summarize_spot_trades(trades, now_ms=now_ms)

        self.assertEqual(result["windows"]["10m"]["trade_count"], 2)
        self.assertAlmostEqual(result["windows"]["10m"]["gross_notional"], 22.5)
        self.assertAlmostEqual(result["windows"]["10m"]["buy_notional"], 10.5)
        self.assertAlmostEqual(result["windows"]["10m"]["sell_notional"], 12.0)
        self.assertAlmostEqual(result["windows"]["10m"]["net_sell_notional"], 1.5)

        self.assertEqual(result["windows"]["1h"]["trade_count"], 3)
        self.assertAlmostEqual(result["windows"]["1h"]["gross_notional"], 42.5)
        self.assertAlmostEqual(result["windows"]["1h"]["net_buy_notional"], 18.5)

        self.assertEqual(result["windows"]["24h"]["trade_count"], 4)
        self.assertAlmostEqual(result["windows"]["24h"]["gross_notional"], 82.5)

    def test_summarize_spot_trades_sorts_recent_trades_desc(self) -> None:
        now_ms = 1_700_000_000_000
        trades = [
            {"time": now_ms - 60_000, "isBuyer": True, "quoteQty": "1", "qty": "10", "price": "0.1"},
            {"time": now_ms - 10_000, "isBuyer": False, "quoteQty": "2", "qty": "20", "price": "0.1"},
        ]

        result = summarize_spot_trades(trades, now_ms=now_ms, recent_limit=2)

        self.assertEqual(result["recent_trades"][0]["side"], "SELL")
        self.assertEqual(result["recent_trades"][1]["side"], "BUY")


if __name__ == "__main__":
    unittest.main()
