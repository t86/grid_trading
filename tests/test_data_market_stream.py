from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import patch

from grid_optimizer.data import (
    clear_futures_market_data_caches,
    fetch_futures_symbol_config,
    resolve_futures_market_snapshot,
)


class FuturesMarketSnapshotTests(unittest.TestCase):
    def tearDown(self) -> None:
        clear_futures_market_data_caches()

    @patch("grid_optimizer.data._http_get_json")
    @patch("grid_optimizer.data.time.time")
    def test_fetch_futures_symbol_config_uses_long_lived_cache(self, mock_time, mock_http) -> None:
        mock_http.side_effect = [
            {
                "symbols": [
                    {
                        "symbol": "BTCUSDT",
                        "status": "TRADING",
                        "contractType": "PERPETUAL",
                        "pricePrecision": 2,
                        "quantityPrecision": 3,
                        "filters": [],
                    }
                ]
            },
            {
                "symbols": [
                    {
                        "symbol": "BTCUSDT",
                        "status": "TRADING",
                        "contractType": "PERPETUAL",
                        "pricePrecision": 4,
                        "quantityPrecision": 5,
                        "filters": [],
                    }
                ]
            },
        ]

        mock_time.return_value = 1_000.0
        first = fetch_futures_symbol_config("BTCUSDT")

        mock_time.return_value = 1_600.0
        second = fetch_futures_symbol_config("BTCUSDT")

        mock_time.return_value = 30_000.0
        third = fetch_futures_symbol_config("BTCUSDT")

        self.assertEqual(first["price_precision"], 2)
        self.assertEqual(second["price_precision"], 2)
        self.assertEqual(third["price_precision"], 4)
        self.assertEqual(mock_http.call_count, 2)

    @patch("grid_optimizer.data.build_futures_rest_market_snapshot")
    def test_resolve_futures_market_snapshot_prefers_fresh_stream_snapshot(self, mock_rest) -> None:
        stream = SimpleNamespace(
            snapshot=lambda max_age_seconds=None: {
                "symbol": "BTCUSDT",
                "bid_price": 10.0,
                "ask_price": 10.2,
                "mark_price": 10.1,
                "funding_rate": 0.0001,
                "next_funding_time": 1234567890,
                "source": "websocket",
            }
        )

        snapshot = resolve_futures_market_snapshot("BTCUSDT", stream=stream)

        self.assertEqual(snapshot["source"], "websocket")
        mock_rest.assert_not_called()

    @patch("grid_optimizer.data.build_futures_rest_market_snapshot")
    def test_resolve_futures_market_snapshot_falls_back_to_rest_when_stream_is_unavailable(self, mock_rest) -> None:
        mock_rest.return_value = {
            "symbol": "BTCUSDT",
            "bid_price": 9.9,
            "ask_price": 10.1,
            "mark_price": 10.0,
            "funding_rate": 0.0,
            "next_funding_time": None,
            "source": "rest",
        }
        stream = SimpleNamespace(snapshot=lambda max_age_seconds=None: None)

        snapshot = resolve_futures_market_snapshot("BTCUSDT", stream=stream)

        self.assertEqual(snapshot["source"], "rest")
        self.assertEqual(snapshot["mark_price"], 10.0)
        mock_rest.assert_called_once()


if __name__ == "__main__":
    unittest.main()
