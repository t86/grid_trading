from __future__ import annotations

import unittest

from grid_optimizer.web import _build_basis_rows, _build_borrow_payload_for_item


class BasisMonitorTests(unittest.TestCase):
    def test_build_basis_rows_covers_both_arbitrage_directions(self) -> None:
        rows = _build_basis_rows(
            futures_symbols=["BTCUSDT", "ETHUSDT"],
            spot_markets=[
                {"symbol": "BTCUSDT", "base_asset": "BTC", "quote_asset": "USDT"},
                {"symbol": "ETHUSDT", "base_asset": "ETH", "quote_asset": "USDT"},
            ],
            spot_tickers=[
                {"symbol": "BTCUSDT", "bid_price": 99.8, "ask_price": 100.0},
                {"symbol": "ETHUSDT", "bid_price": 102.0, "ask_price": 102.2},
            ],
            futures_tickers=[
                {"symbol": "BTCUSDT", "bid_price": 101.5, "ask_price": 101.7},
                {"symbol": "ETHUSDT", "bid_price": 101.0, "ask_price": 101.2},
            ],
            premium_rows=[
                {"symbol": "BTCUSDT", "funding_rate": 0.0005, "mark_price": 101.6, "next_funding_time": 1_700_000_000_000},
                {"symbol": "ETHUSDT", "funding_rate": -0.0004, "mark_price": 101.1, "next_funding_time": 1_700_000_000_000},
            ],
            contract_type="usdm",
            spot_quote_mode="major_stables",
        )

        by_symbol = {row["futures_symbol"]: row for row in rows}

        btc = by_symbol["BTCUSDT"]
        self.assertEqual(btc["arbitrage_side"], "long_spot_short_perp")
        self.assertEqual(btc["strategy_label"], "买现货 / 卖合约")
        self.assertEqual(btc["carry_alignment"], "aligned")
        self.assertGreater(btc["spread_long_spot_short_perp"], 0.01)

        eth = by_symbol["ETHUSDT"]
        self.assertEqual(eth["arbitrage_side"], "short_spot_long_perp")
        self.assertEqual(eth["strategy_label"], "借币卖现货 / 买合约")
        self.assertEqual(eth["carry_alignment"], "aligned")
        self.assertGreater(eth["spread_short_spot_long_perp"], 0.0)

    def test_build_basis_rows_falls_back_to_usdt_spot_for_coinm(self) -> None:
        rows = _build_basis_rows(
            futures_symbols=["BTCUSD_PERP"],
            spot_markets=[
                {"symbol": "BTCUSDC", "base_asset": "BTC", "quote_asset": "USDC"},
                {"symbol": "BTCUSDT", "base_asset": "BTC", "quote_asset": "USDT"},
            ],
            spot_tickers=[
                {"symbol": "BTCUSDT", "bid_price": 100.0, "ask_price": 100.1},
                {"symbol": "BTCUSDC", "bid_price": 100.0, "ask_price": 100.2},
            ],
            futures_tickers=[
                {"symbol": "BTCUSD_PERP", "bid_price": 101.0, "ask_price": 101.1},
            ],
            premium_rows=[
                {"symbol": "BTCUSD_PERP", "funding_rate": 0.0002, "mark_price": 101.0, "next_funding_time": 1_700_000_000_000},
            ],
            contract_type="coinm",
            spot_quote_mode="usdt",
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["spot_symbol"], "BTCUSDT")
        self.assertEqual(rows[0]["arbitrage_side"], "long_spot_short_perp")

    def test_build_borrow_payload_collects_cross_isolated_and_vip(self) -> None:
        payload = _build_borrow_payload_for_item(
            {
                "futures_symbol": "PIXELUSDT",
                "spot_symbol": "PIXELUSDT",
                "base_asset": "PIXEL",
                "arbitrage_side": "short_spot_long_perp",
            },
            {
                "status": "ok",
                "mode": "full",
                "all_assets": {"PIXEL": {"is_borrowable": True}},
                "restricted_open_long": set(),
                "restricted_max_collateral": set(),
                "inventory_margin": {"PIXEL": 12345.0},
                "inventory_isolated": {"PIXEL": 3456.0},
                "isolated_symbols": {"PIXELUSDT"},
                "cross_rates": {"PIXEL": 0.000031},
                "isolated_rates": {"PIXEL": 0.000041},
                "cross_max": {"PIXEL": {"amount": 1200.0, "borrow_limit": 3000.0}},
                "isolated_max": {"PIXELUSDT": {"amount": 800.0, "borrow_limit": 2000.0}},
                "vip_loanable": {
                    "PIXEL": {
                        "max_limit": 100000.0,
                        "min_limit": 100.0,
                        "flexible_daily_interest_rate": 0.0012,
                    }
                },
                "vip_rates": {
                    "PIXEL": {
                        "flexible_daily_interest_rate": 0.0013,
                        "flexible_yearly_interest_rate": 0.55,
                    }
                },
                "errors": [],
            },
        )

        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["mode"], "full")
        self.assertTrue(payload["needs_borrow"])
        self.assertTrue(payload["cross"]["supported"])
        self.assertAlmostEqual(float(payload["cross"]["max_borrow"] or 0.0), 1200.0, places=8)
        self.assertTrue(payload["isolated"]["supported"])
        self.assertAlmostEqual(float(payload["isolated"]["max_borrow"] or 0.0), 800.0, places=8)
        self.assertTrue(payload["vip"]["available"])
        self.assertAlmostEqual(float(payload["vip"]["max_limit"] or 0.0), 100000.0, places=8)

    def test_build_borrow_payload_safe_mode_only_returns_theoretical_status(self) -> None:
        payload = _build_borrow_payload_for_item(
            {
                "futures_symbol": "PIXELUSDT",
                "spot_symbol": "PIXELUSDT",
                "base_asset": "PIXEL",
                "arbitrage_side": "short_spot_long_perp",
            },
            {
                "status": "safe_readonly",
                "mode": "safe",
                "all_assets": {"PIXEL": {"is_borrowable": True}},
                "restricted_open_long": set(),
                "restricted_max_collateral": set(),
                "cross_rates": {"PIXEL": 0.000031},
                "isolated_rates": {"PIXEL": 0.000041},
                "errors": [],
            },
        )

        self.assertEqual(payload["status"], "safe_readonly")
        self.assertEqual(payload["mode"], "safe")
        self.assertTrue(payload["cross"]["supported"])
        self.assertIsNone(payload["cross"]["max_borrow"])
        self.assertIsNone(payload["isolated"]["supported"])
        self.assertIsNone(payload["vip"]["available"])
        self.assertIn("安全模式", payload["note"])


if __name__ == "__main__":
    unittest.main()
