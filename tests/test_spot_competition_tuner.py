from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import grid_optimizer.web as web
from grid_optimizer.spot_competition_tuner import (
    SpotCompetitionTuningInputs,
    recommend_spot_competition_config,
)
from grid_optimizer.types import Candle


def _candles(*, start: float, step: float, count: int, amp: float) -> list[Candle]:
    now = datetime(2026, 6, 28, tzinfo=timezone.utc)
    rows: list[Candle] = []
    price = start
    for idx in range(count):
        open_price = price
        close_price = price + step
        high = max(open_price, close_price) * (1 + amp)
        low = min(open_price, close_price) * (1 - amp)
        rows.append(
            Candle(
                open_time=now + timedelta(minutes=idx),
                close_time=now + timedelta(minutes=idx + 1),
                open=open_price,
                high=high,
                low=low,
                close=close_price,
            )
        )
        price = close_price
    return rows


class SpotCompetitionTunerTests(unittest.TestCase):
    def test_page_is_exposed(self) -> None:
        self.assertIn("现货交易赛参数推荐", web.SPOT_COMPETITION_TUNER_PAGE)
        self.assertIn("/api/spot_competition_tuner/recommend", web.SPOT_COMPETITION_TUNER_PAGE)
        self.assertIn("/api/spot_competition_tuner/save", web.SPOT_COMPETITION_TUNER_PAGE)
        self.assertIn("/spot_competition_tuner", web.SPOT_RUNNER_PAGE)

    def test_recommendation_tight_liquid_uses_tighter_step_and_larger_inventory(self) -> None:
        metrics = {
            "mid_price": 100.0,
            "quote_volume": 100_000_000.0,
            "quote_volume_per_minute": 600_000.0,
            "avg_amplitude_ratio": 0.0005,
            "realized_volatility": 0.0002,
            "trend_return_ratio": 0.001,
            "spread_ratio": 0.0002,
            "book_depth_notional": 1_000_000.0,
            "orderbook_imbalance": 0.0,
            "candle_count": 180,
        }

        result = recommend_spot_competition_config(
            inputs=SpotCompetitionTuningInputs(symbol="BTCUSDT", budget_quote=10_000),
            metrics=metrics,
            symbol_config={"tick_size": 0.01, "min_notional": 5.0},
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["classification"]["regime"], "tight_liquid")
        config = result["recommended_config"]
        self.assertEqual(config["strategy_mode"], "spot_competition_inventory_grid")
        self.assertLess(config["step_price"], 0.2)
        self.assertGreater(config["max_position_notional"], 4000)
        self.assertFalse(config["spot_taker_exit_enabled"])

    def test_recommendation_thin_volatile_is_small_and_defensive(self) -> None:
        metrics = {
            "mid_price": 10.0,
            "quote_volume": 5_000.0,
            "quote_volume_per_minute": 25.0,
            "avg_amplitude_ratio": 0.02,
            "realized_volatility": 0.01,
            "trend_return_ratio": -0.03,
            "spread_ratio": 0.004,
            "book_depth_notional": 500.0,
            "orderbook_imbalance": -0.2,
            "candle_count": 180,
        }

        result = recommend_spot_competition_config(
            inputs=SpotCompetitionTuningInputs(
                symbol="ALTUSDT",
                budget_quote=10_000,
                risk_level="conservative",
                target_mode="synthetic_neutral",
            ),
            metrics=metrics,
            symbol_config={"tick_size": 0.0001, "min_notional": 5.0},
        )

        self.assertEqual(result["classification"]["regime"], "thin_volatile")
        config = result["recommended_config"]
        self.assertEqual(config["strategy_mode"], "spot_competition_synthetic_neutral_grid")
        self.assertLess(config["per_order_notional"], 100)
        self.assertLess(config["max_position_notional"], 1000)
        self.assertTrue(config["spot_slow_trend_step_enabled"])
        self.assertTrue(any("盘口薄" in note for note in result["notes"]))

    @patch("grid_optimizer.spot_competition_tuner.fetch_spot_agg_trades")
    @patch("grid_optimizer.spot_competition_tuner.fetch_spot_klines")
    @patch("grid_optimizer.spot_competition_tuner.fetch_spot_book_tickers")
    @patch("grid_optimizer.spot_competition_tuner.fetch_spot_symbol_config")
    def test_web_recommendation_builder_uses_public_market_data(
        self,
        mock_symbol_config,
        mock_book,
        mock_klines,
        mock_trades,
    ) -> None:
        mock_symbol_config.return_value = {"tick_size": 0.01, "min_notional": 5.0}
        mock_book.return_value = [{"symbol": "BTCUSDT", "bid_price": 99.99, "ask_price": 100.01, "bid_qty": 10, "ask_qty": 10}]
        mock_klines.return_value = _candles(start=100.0, step=0.01, count=60, amp=0.0002)
        mock_trades.return_value = [{"p": "100", "q": "10"} for _ in range(1000)]

        result = web.build_spot_competition_recommendation(
            {"symbol": "BTCUSDT", "budget_quote": 2000, "window_minutes": 60}
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["symbol"], "BTCUSDT")
        self.assertGreater(result["metrics"]["quote_volume"], 0)

    @patch("grid_optimizer.web._validate_market_symbol")
    def test_tuner_config_can_save_through_existing_spot_runner_normalizer(self, _mock_validate) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            (root / "output").mkdir()
            previous_cwd = Path.cwd()
            try:
                import os

                os.chdir(root)
                result = web._save_spot_runner_config_without_start(
                    {
                        "symbol": "BTCUSDT",
                        "strategy_mode": "spot_competition_inventory_grid",
                        "total_quote_budget": 1000,
                        "step_price": 0.1,
                        "per_order_notional": 20,
                        "max_order_position_notional": 400,
                        "max_position_notional": 600,
                    }
                )
                saved_payload = json.loads(Path(result["runner"]["control_path"]).read_text())
            finally:
                os.chdir(previous_cwd)

        self.assertTrue(result["saved"])
        self.assertEqual(result["config"]["strategy_mode"], "spot_competition_inventory_grid")
        self.assertEqual(result["config"]["symbol"], "BTCUSDT")
        self.assertEqual(saved_payload["symbol"], "BTCUSDT")


if __name__ == "__main__":
    unittest.main()
