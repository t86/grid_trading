from __future__ import annotations

import unittest
from unittest.mock import patch

from grid_optimizer.data import (
    clear_futures_signed_response_caches,
    fetch_futures_account_info_v3,
    fetch_futures_open_orders,
    fetch_futures_position_mode,
)


class FuturesSignedResponseCacheTests(unittest.TestCase):
    def tearDown(self) -> None:
        clear_futures_signed_response_caches()

    @patch("grid_optimizer.data._http_signed_request_json")
    @patch("grid_optimizer.data.time.time")
    def test_fetch_futures_position_mode_uses_long_lived_cache(self, mock_time, mock_http) -> None:
        mock_http.side_effect = [
            {"dualSidePosition": False},
            {"dualSidePosition": True},
        ]

        mock_time.return_value = 1000.0
        first = fetch_futures_position_mode("key", "secret")

        mock_time.return_value = 1060.0
        second = fetch_futures_position_mode("key", "secret")

        mock_time.return_value = 1405.0
        third = fetch_futures_position_mode("key", "secret")

        self.assertEqual(first, {"dualSidePosition": False})
        self.assertEqual(second, {"dualSidePosition": False})
        self.assertEqual(third, {"dualSidePosition": True})
        self.assertEqual(mock_http.call_count, 2)

    @patch("grid_optimizer.data._http_signed_request_json")
    @patch("grid_optimizer.data.time.time")
    def test_fetch_futures_account_info_uses_short_lived_cache(self, mock_time, mock_http) -> None:
        mock_http.side_effect = [
            {"availableBalance": "12.5"},
            {"availableBalance": "18.0"},
        ]

        mock_time.return_value = 2000.0
        first = fetch_futures_account_info_v3("key", "secret")

        mock_time.return_value = 2000.1
        second = fetch_futures_account_info_v3("key", "secret")

        mock_time.return_value = 2001.0
        third = fetch_futures_account_info_v3("key", "secret")

        self.assertEqual(first, {"availableBalance": "12.5"})
        self.assertEqual(second, {"availableBalance": "12.5"})
        self.assertEqual(third, {"availableBalance": "18.0"})
        self.assertEqual(mock_http.call_count, 2)

    @patch("grid_optimizer.data._http_signed_request_json")
    @patch("grid_optimizer.data.time.time")
    def test_fetch_futures_open_orders_uses_short_lived_symbol_cache(self, mock_time, mock_http) -> None:
        mock_http.side_effect = [
            [{"orderId": 1}],
            [{"orderId": 2}],
        ]

        mock_time.return_value = 3000.0
        first = fetch_futures_open_orders("BTCUSDT", "key", "secret")

        mock_time.return_value = 3000.1
        second = fetch_futures_open_orders("BTCUSDT", "key", "secret")

        mock_time.return_value = 3001.0
        third = fetch_futures_open_orders("BTCUSDT", "key", "secret")

        self.assertEqual(first, [{"orderId": 1}])
        self.assertEqual(second, [{"orderId": 1}])
        self.assertEqual(third, [{"orderId": 2}])
        self.assertEqual(mock_http.call_count, 2)


if __name__ == "__main__":
    unittest.main()
