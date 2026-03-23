from __future__ import annotations

import unittest
from unittest.mock import patch

from grid_optimizer.web import (
    HTML_PAGE,
    _estimated_grid_liquidation_snapshot,
    _normalize_grid_preview_payload,
    _run_grid_preview,
)


class GridPreviewTests(unittest.TestCase):
    def test_html_page_contains_grid_preview_section(self) -> None:
        self.assertIn("币安式网格预览器", HTML_PAGE)
        self.assertIn('id="market_type"', HTML_PAGE)
        self.assertIn("现货 V1（单向做多）", HTML_PAGE)
        self.assertIn('id="preview_btn"', HTML_PAGE)
        self.assertIn('id="preview_tbody"', HTML_PAGE)
        self.assertIn('id="preview_summary"', HTML_PAGE)
        self.assertIn("启动库存", HTML_PAGE)

    def test_normalize_grid_preview_payload_rejects_invalid_bounds(self) -> None:
        with self.assertRaises(ValueError):
            _normalize_grid_preview_payload(
                {
                    "market_type": "futures",
                    "contract_type": "usdm",
                    "symbol": "BTCUSDT",
                    "strategy_direction": "neutral",
                    "grid_level_mode": "arithmetic",
                    "min_price": 10,
                    "max_price": 10,
                    "n": 20,
                    "margin_amount": 100,
                    "leverage": 2,
                }
            )

    @patch("grid_optimizer.web._validate_market_symbol")
    def test_normalize_grid_preview_payload_forces_spot_long_one_x(self, _mock_validate_symbol) -> None:
        payload = _normalize_grid_preview_payload(
            {
                "market_type": "spot",
                "symbol": "BTCUSDT",
                "strategy_direction": "neutral",
                "grid_level_mode": "arithmetic",
                "min_price": 10,
                "max_price": 20,
                "n": 5,
                "margin_amount": 100,
                "leverage": 3,
            }
        )

        self.assertEqual(payload["market_type"], "spot")
        self.assertIsNone(payload["contract_type"])
        self.assertEqual(payload["strategy_direction"], "long")
        self.assertEqual(payload["leverage"], 1.0)

    def test_estimated_grid_liquidation_snapshot_handles_long_and_short(self) -> None:
        long_snapshot = _estimated_grid_liquidation_snapshot(
            side="long",
            avg_entry_price=100.0,
            position_qty=10.0,
            allocated_margin=200.0,
        )
        short_snapshot = _estimated_grid_liquidation_snapshot(
            side="short",
            avg_entry_price=100.0,
            position_qty=10.0,
            allocated_margin=200.0,
        )

        self.assertAlmostEqual(long_snapshot["entry_notional"], 1000.0, places=8)
        self.assertAlmostEqual(short_snapshot["entry_notional"], 1000.0, places=8)
        self.assertAlmostEqual(long_snapshot["liquidation_price"], 84.2105263158, places=6)
        self.assertAlmostEqual(short_snapshot["liquidation_price"], 114.2857142857, places=6)

    @patch("grid_optimizer.web.fetch_futures_book_tickers")
    @patch("grid_optimizer.web.fetch_futures_symbol_config")
    def test_run_grid_preview_builds_preview_rows(self, mock_symbol_config, mock_book_tickers) -> None:
        mock_symbol_config.return_value = {
            "tick_size": 0.0001,
            "step_size": 0.1,
            "min_qty": 0.1,
            "min_notional": 5.0,
        }
        mock_book_tickers.return_value = [
            {
                "symbol": "OPNUSDT",
                "bid_price": 0.2499,
                "ask_price": 0.2501,
            }
        ]

        result = _run_grid_preview(
            {
                "market_type": "futures",
                "contract_type": "usdm",
                "symbol": "OPNUSDT",
                "strategy_direction": "neutral",
                "grid_level_mode": "arithmetic",
                "min_price": 0.20,
                "max_price": 0.30,
                "n": 4,
                "margin_amount": 100.0,
                "leverage": 2.0,
            }
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["summary"]["symbol"], "OPNUSDT")
        self.assertEqual(result["summary"]["grid_count"], 4)
        self.assertEqual(result["summary"]["allocation_mode"], "equal")
        self.assertEqual(len(result["rows"]), 4)
        self.assertGreaterEqual(result["summary"]["active_buy_orders"], 1)
        self.assertGreaterEqual(result["summary"]["active_sell_orders"], 1)
        self.assertIn("startup_state", result["rows"][0])
        self.assertIn("active_order_price", result["rows"][0])
        notionals = [row["entry_notional"] for row in result["rows"]]
        self.assertTrue(max(notionals) - min(notionals) < 1e-6)
        qtys = [row["qty"] for row in result["rows"]]
        self.assertNotAlmostEqual(qtys[0], qtys[-1], places=6)

    @patch("grid_optimizer.web.fetch_spot_book_tickers")
    @patch("grid_optimizer.web.fetch_spot_symbol_config")
    def test_run_grid_preview_builds_spot_rows_without_bootstrap(self, mock_symbol_config, mock_book_tickers) -> None:
        mock_symbol_config.return_value = {
            "tick_size": 0.0001,
            "step_size": 0.1,
            "min_qty": 0.1,
            "min_notional": 5.0,
        }
        mock_book_tickers.return_value = [
            {
                "symbol": "OPNUSDT",
                "bid_price": 0.2499,
                "ask_price": 0.2501,
            }
        ]

        result = _run_grid_preview(
            {
                "market_type": "spot",
                "contract_type": None,
                "symbol": "OPNUSDT",
                "strategy_direction": "long",
                "grid_level_mode": "arithmetic",
                "min_price": 0.20,
                "max_price": 0.30,
                "n": 4,
                "margin_amount": 100.0,
                "leverage": 1.0,
            }
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["summary"]["market_type"], "spot")
        self.assertEqual(result["summary"]["strategy_direction"], "long")
        self.assertEqual(result["summary"]["active_sell_orders"], 0)
        self.assertGreaterEqual(result["summary"]["active_buy_orders"], 1)
        self.assertEqual({row["grid_side"] for row in result["rows"]}, {"long"})
        self.assertTrue(all(row["entry_side"] == "BUY" for row in result["rows"]))
        self.assertTrue(all(row["exit_side"] == "SELL" for row in result["rows"]))
        self.assertIn(result["rows"][0]["startup_state"], {"待买入", "等待重新站回该格上方后激活"})


if __name__ == "__main__":
    unittest.main()
