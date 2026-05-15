from __future__ import annotations

from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from grid_optimizer import data
from grid_optimizer.data import (
    clear_futures_signed_response_caches,
    fetch_futures_account_info_v3,
    fetch_futures_open_orders,
    fetch_futures_position_mode,
    fetch_futures_user_trades,
)


class FuturesSignedResponseCacheTests(unittest.TestCase):
    def tearDown(self) -> None:
        clear_futures_signed_response_caches()

    @patch("grid_optimizer.data.time.sleep")
    @patch("grid_optimizer.data._http_request_json")
    def test_signed_request_retries_short_local_cooldown(self, mock_request, mock_sleep) -> None:
        mock_request.side_effect = [
            RuntimeError("Binance API local cooldown active for futures; retry after 0.1s"),
            {"ok": True},
        ]

        with patch.object(data, "FUTURES_SIGNED_API_MIN_INTERVAL_SECONDS", 0.0):
            result = data._http_signed_request_json(
                "https://fapi.binance.com/fapi/v1/order",
                {},
                "key",
                "secret",
            )

        self.assertEqual(result, {"ok": True})
        self.assertEqual(mock_request.call_count, 2)
        mock_sleep.assert_called_once_with(0.1)

    @patch("grid_optimizer.data.time.sleep")
    @patch("grid_optimizer.data.time.time")
    @patch("grid_optimizer.data._http_request_json")
    def test_signed_request_enters_local_cooldown_after_binance_rate_limit(
        self,
        mock_request,
        mock_time,
        mock_sleep,
    ) -> None:
        mock_time.side_effect = [1000.0, 1000.0, 1000.5, 1000.5, 1000.5, 1000.5]
        mock_request.side_effect = [
            RuntimeError("Binance API error 429: Too many requests"),
            {"ok": True},
        ]

        with patch.object(data, "FUTURES_SIGNED_API_MIN_INTERVAL_SECONDS", 0.0):
            with self.assertRaisesRegex(RuntimeError, "Too many requests"):
                data._http_signed_request_json(
                    "https://fapi.binance.com/fapi/v1/account",
                    {},
                    "key",
                    "secret",
                )

            result = data._http_signed_request_json(
                "https://fapi.binance.com/fapi/v1/account",
                {},
                "key",
                "secret",
            )

        self.assertEqual(result, {"ok": True})
        self.assertEqual(mock_request.call_count, 2)
        mock_sleep.assert_called_once_with(1.0)

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

    @patch("grid_optimizer.data.fetch_futures_position_risk_v3")
    @patch("grid_optimizer.data._http_signed_request_json")
    @patch("grid_optimizer.data.time.time")
    def test_fetch_futures_account_info_uses_short_lived_cache(
        self,
        mock_time,
        mock_http,
        mock_position_risk,
    ) -> None:
        mock_http.side_effect = [
            {"availableBalance": "12.5"},
            {"availableBalance": "18.0"},
        ]
        mock_position_risk.return_value = []

        mock_time.return_value = 2000.0
        first = fetch_futures_account_info_v3("key", "secret")

        mock_time.return_value = 2000.1
        second = fetch_futures_account_info_v3("key", "secret")

        mock_time.return_value = 2003.0
        third = fetch_futures_account_info_v3("key", "secret")

        self.assertEqual(first.get("availableBalance"), "12.5")
        self.assertEqual(second.get("availableBalance"), "12.5")
        self.assertEqual(third.get("availableBalance"), "18.0")
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

        mock_time.return_value = 3003.0
        third = fetch_futures_open_orders("BTCUSDT", "key", "secret")

        self.assertEqual(first, [{"orderId": 1}])
        self.assertEqual(second, [{"orderId": 1}])
        self.assertEqual(third, [{"orderId": 2}])
        self.assertEqual(mock_http.call_count, 2)

    @patch("grid_optimizer.data._http_signed_request_json")
    @patch("grid_optimizer.data.time.time")
    def test_fetch_futures_user_trades_uses_short_lived_symbol_window_cache(self, mock_time, mock_http) -> None:
        mock_http.side_effect = [
            [{"id": 1, "time": 1000}],
            [{"id": 2, "time": 1001}],
        ]

        mock_time.return_value = 4000.0
        first = fetch_futures_user_trades(
            symbol="BTCUSDT",
            api_key="key",
            api_secret="secret",
            start_time_ms=100,
            end_time_ms=200,
        )

        mock_time.return_value = 4001.0
        second = fetch_futures_user_trades(
            symbol="BTCUSDT",
            api_key="key",
            api_secret="secret",
            start_time_ms=100,
            end_time_ms=200,
        )

        mock_time.return_value = 4006.0
        third = fetch_futures_user_trades(
            symbol="BTCUSDT",
            api_key="key",
            api_secret="secret",
            start_time_ms=100,
            end_time_ms=200,
        )

        self.assertEqual(first, [{"id": 1, "time": 1000}])
        self.assertEqual(second, [{"id": 1, "time": 1000}])
        self.assertEqual(third, [{"id": 2, "time": 1001}])
        self.assertEqual(mock_http.call_count, 2)

    @patch("grid_optimizer.data._http_signed_request_json")
    @patch("grid_optimizer.data.time.time")
    def test_open_orders_cache_survives_memory_cache_clear_via_file_cache(self, mock_time, mock_http) -> None:
        mock_http.return_value = [{"orderId": 99}]
        with tempfile.TemporaryDirectory() as temp_dir, patch.object(
            data, "FUTURES_SIGNED_FILE_CACHE_DIR", Path(temp_dir)
        ):
            mock_time.return_value = 5000.0
            first = fetch_futures_open_orders("BTCUSDT", "key", "secret")

            with data._FUTURES_SIGNED_RESPONSE_CACHE_LOCK:
                data._FUTURES_OPEN_ORDERS_CACHE.clear()

            mock_time.return_value = 5001.0
            second = fetch_futures_open_orders("BTCUSDT", "key", "secret")

        self.assertEqual(first, [{"orderId": 99}])
        self.assertEqual(second, [{"orderId": 99}])
        self.assertEqual(mock_http.call_count, 1)

    @patch("grid_optimizer.data.urlencode", return_value="")
    @patch("grid_optimizer.data.urlopen")
    @patch("grid_optimizer.data.time.time")
    def test_binance_1003_sets_cross_process_cooldown(self, mock_time, mock_urlopen, _mock_urlencode) -> None:
        class FakeResponse:
            def __init__(self, payload: bytes) -> None:
                self.payload = payload
                self.status = 200

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return None

            def read(self, _size: int = -1) -> bytes:
                payload, self.payload = self.payload, b""
                return payload

        mock_time.return_value = 10.0
        with tempfile.TemporaryDirectory() as temp_dir, patch.object(
            data, "BINANCE_RATE_LIMIT_FILE_PATH", Path(temp_dir) / "cooldown.json"
        ), patch.object(data, "BINANCE_RATE_LIMIT_COOLDOWN_SECONDS", 30.0):
            mock_urlopen.return_value = FakeResponse(
                b'{"code":-1003,"msg":"Too many requests; please use websocket"}'
            )
            with self.assertRaisesRegex(RuntimeError, "Binance API error -1003"):
                data._http_get_json("https://fapi.binance.com/fapi/v1/premiumIndex", {})

            mock_urlopen.reset_mock()
            mock_time.return_value = 20.0
            with self.assertRaisesRegex(RuntimeError, "local cooldown active"):
                data._http_get_json("https://fapi.binance.com/fapi/v1/premiumIndex", {})
            mock_urlopen.assert_not_called()


if __name__ == "__main__":
    unittest.main()
