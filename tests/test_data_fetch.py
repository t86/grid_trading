from __future__ import annotations

import socket
import unittest
from io import BytesIO
from datetime import datetime, timedelta, timezone
from unittest.mock import patch
from urllib.error import HTTPError

from grid_optimizer import data as data_module
from grid_optimizer.data import (
    COINM_MAX_KLINE_WINDOW_MS,
    FUNDING_DEFAULT_STEP_MS,
    _http_request_json,
    _ipv4_first_getaddrinfo,
    _prefer_ipv4,
    fetch_funding_rates,
    fetch_futures_klines,
    fetch_futures_quote_volume_sum,
    fetch_futures_symbol_config,
    fetch_spot_symbol_config,
    load_or_fetch_candles,
    parse_interval_ms,
)


class DataFetchTests(unittest.TestCase):
    class _FakeResponse:
        def __init__(self, payload: bytes, *, headers: dict[str, str] | None = None) -> None:
            self._payload = payload
            self._offset = 0
            self.headers = headers or {}

        def __enter__(self) -> "DataFetchTests._FakeResponse":
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def read(self, size: int = -1) -> bytes:
            if size is None or size < 0:
                size = len(self._payload) - self._offset
            chunk = self._payload[self._offset : self._offset + size]
            self._offset += len(chunk)
            return chunk

    def test_prefer_ipv4_nested_context_is_reentrant_and_restores_socket(self) -> None:
        original = socket.getaddrinfo

        with _prefer_ipv4():
            self.assertIs(socket.getaddrinfo, _ipv4_first_getaddrinfo)
            with _prefer_ipv4():
                self.assertIs(socket.getaddrinfo, _ipv4_first_getaddrinfo)
                self.assertGreaterEqual(data_module._GETADDRINFO_PATCH_DEPTH, 2)

        self.assertIs(socket.getaddrinfo, original)
        self.assertEqual(data_module._GETADDRINFO_PATCH_DEPTH, 0)

    def test_parse_interval_supports_seconds(self) -> None:
        self.assertEqual(parse_interval_ms("1s"), 1_000)
        self.assertEqual(parse_interval_ms("30s"), 30_000)

    @patch("grid_optimizer.data.urlopen")
    def test_http_request_json_surfaces_binance_http_error_body(self, mock_urlopen) -> None:
        payload = b'{"code":-4046,"msg":"No need to change margin type."}'
        mock_urlopen.side_effect = HTTPError(
            url="https://fapi.binance.com/fapi/v1/marginType",
            code=400,
            msg="Bad Request",
            hdrs=None,
            fp=BytesIO(payload),
        )

        with self.assertRaises(RuntimeError) as ctx:
            _http_request_json("https://fapi.binance.com/fapi/v1/marginType", {}, method="POST")

        self.assertIn("No need to change margin type", str(ctx.exception))

    @patch("grid_optimizer.data.MAX_HTTP_RESPONSE_BYTES", 64, create=True)
    @patch("grid_optimizer.data.urlopen")
    def test_http_request_json_rejects_large_content_length(self, mock_urlopen) -> None:
        payload = b'{"data":"' + (b"x" * 100) + b'"}'
        mock_urlopen.return_value = self._FakeResponse(
            payload,
            headers={"Content-Length": str(len(payload))},
        )

        with self.assertRaises(RuntimeError) as ctx:
            _http_request_json("https://example.com/huge", {})

        self.assertIn("too large", str(ctx.exception).lower())

    @patch("grid_optimizer.data.MAX_HTTP_RESPONSE_BYTES", 64, create=True)
    @patch("grid_optimizer.data.urlopen")
    def test_http_request_json_rejects_large_chunked_body(self, mock_urlopen) -> None:
        payload = b'{"data":"' + (b"y" * 100) + b'"}'
        mock_urlopen.return_value = self._FakeResponse(payload)

        with self.assertRaises(RuntimeError) as ctx:
            _http_request_json("https://example.com/chunked", {})

        self.assertIn("too large", str(ctx.exception).lower())

    @patch("grid_optimizer.data.MAX_HTTP_RESPONSE_BYTES", 64, create=True)
    @patch("grid_optimizer.data.urlopen")
    def test_http_request_json_rejects_large_http_error_body(self, mock_urlopen) -> None:
        payload = b'{"code":-1000,"msg":"' + (b"z" * 100) + b'"}'
        mock_urlopen.side_effect = HTTPError(
            url="https://example.com/huge-error",
            code=502,
            msg="Bad Gateway",
            hdrs={"Content-Length": str(len(payload))},
            fp=BytesIO(payload),
        )

        with self.assertRaises(RuntimeError) as ctx:
            _http_request_json("https://example.com/huge-error", {})

        self.assertIn("too large", str(ctx.exception).lower())

    def test_second_level_interval_range_limit(self) -> None:
        start = datetime(2025, 1, 1, tzinfo=timezone.utc)
        end = start + timedelta(days=32)
        with self.assertRaises(ValueError):
            load_or_fetch_candles(
                symbol="BTCUSDT",
                interval="1s",
                start_time=start,
                end_time=end,
                cache_dir="data",
                contract_type="usdm",
                refresh=False,
            )

    @patch("grid_optimizer.data._http_get_json")
    def test_coinm_kline_requests_are_chunked_to_200_days(self, mock_http_get_json) -> None:
        mock_http_get_json.return_value = []
        day_ms = 24 * 60 * 60 * 1000
        end_ms = 401 * day_ms

        candles = fetch_futures_klines(
            symbol="ETHUSD_PERP",
            interval="1d",
            start_ms=0,
            end_ms=end_ms,
            contract_type="coinm",
        )

        self.assertEqual(candles, [])
        self.assertGreater(len(mock_http_get_json.call_args_list), 1)
        for call in mock_http_get_json.call_args_list:
            params = call.args[1]
            self.assertLessEqual(params["endTime"] - params["startTime"], COINM_MAX_KLINE_WINDOW_MS - 1)

    @patch("grid_optimizer.data._http_get_json")
    def test_coinm_kline_can_continue_after_empty_early_chunk(self, mock_http_get_json) -> None:
        day_ms = 24 * 60 * 60 * 1000
        split_ms = COINM_MAX_KLINE_WINDOW_MS

        def _fake(_url: str, params: dict[str, int | str]) -> list[list]:
            start_ms = int(params["startTime"])
            end_ms = int(params["endTime"])
            if start_ms < split_ms:
                return []
            close_ms = min(start_ms + day_ms - 1, end_ms)
            return [[start_ms, "100", "110", "90", "105", "0", close_ms, "0", "0", "0", "0", "0"]]

        mock_http_get_json.side_effect = _fake

        candles = fetch_futures_klines(
            symbol="ETHUSD_PERP",
            interval="1d",
            start_ms=0,
            end_ms=split_ms + 10 * day_ms,
            contract_type="coinm",
        )

        self.assertGreaterEqual(len(candles), 1)
        self.assertGreaterEqual(int(candles[0].open_time.timestamp() * 1000), split_ms)

    @patch("grid_optimizer.data._http_get_json")
    def test_kline_pagination_handles_tail_truncation(self, mock_http_get_json) -> None:
        # Emulate Binance behavior: when a query spans more than `limit` bars,
        # it may return the tail slice close to endTime.
        interval_ms = 60_000
        start_ms = 0
        end_ms = 5_000 * interval_ms
        limit = 1_500

        def _fake(_url: str, params: dict[str, int | str]) -> list[list]:
            q_start = int(params["startTime"])
            q_end = int(params["endTime"])
            all_opens = list(range(q_start, q_end + 1, interval_ms))
            if not all_opens:
                return []
            selected = all_opens[-limit:]
            rows: list[list] = []
            for open_ms in selected:
                close_ms = open_ms + interval_ms - 1
                rows.append(
                    [open_ms, "100", "101", "99", "100", "0", close_ms, "0", "0", "0", "0", "0"]
                )
            return rows

        mock_http_get_json.side_effect = _fake
        candles = fetch_futures_klines(
            symbol="ETHUSD_PERP",
            interval="1m",
            start_ms=start_ms,
            end_ms=end_ms,
            contract_type="coinm",
            limit=limit,
        )

        expected = (end_ms - start_ms) // interval_ms
        self.assertEqual(len(candles), expected)
        self.assertEqual(int(candles[0].open_time.timestamp() * 1000), start_ms)
        self.assertEqual(int(candles[-1].open_time.timestamp() * 1000), end_ms - interval_ms)

    @patch("grid_optimizer.data._http_get_json")
    def test_funding_pagination_handles_tail_truncation(self, mock_http_get_json) -> None:
        start_ms = 0
        end_ms = 4_000 * FUNDING_DEFAULT_STEP_MS
        limit = 1_000

        def _fake(_url: str, params: dict[str, int | str]) -> list[dict[str, str | int]]:
            q_start = int(params["startTime"])
            q_end = int(params["endTime"])
            # Binance funding events are aligned to fixed 8h boundaries; startTime
            # only filters the aligned series and does not redefine the cadence.
            aligned_start = ((q_start + FUNDING_DEFAULT_STEP_MS - 1) // FUNDING_DEFAULT_STEP_MS) * FUNDING_DEFAULT_STEP_MS
            all_times = list(range(aligned_start, q_end + 1, FUNDING_DEFAULT_STEP_MS))
            if not all_times:
                return []
            selected = all_times[-limit:]
            return [
                {"fundingTime": t, "fundingRate": "0.0001", "symbol": "ETHUSD_PERP"}
                for t in selected
            ]

        mock_http_get_json.side_effect = _fake
        rows = fetch_funding_rates(
            symbol="ETHUSD_PERP",
            start_ms=start_ms,
            end_ms=end_ms,
            contract_type="coinm",
            limit=limit,
        )

        expected = end_ms // FUNDING_DEFAULT_STEP_MS
        self.assertEqual(len(rows), expected)
        self.assertEqual(int(rows[0].ts.timestamp() * 1000), start_ms)
        self.assertEqual(
            int(rows[-1].ts.timestamp() * 1000),
            end_ms - FUNDING_DEFAULT_STEP_MS,
        )

    @patch("grid_optimizer.data._http_get_json")
    def test_fetch_futures_symbol_config_selects_exact_symbol(self, mock_http_get_json) -> None:
        mock_http_get_json.return_value = {
            "symbols": [
                {
                    "symbol": "BTCUSDT",
                    "status": "TRADING",
                    "contractType": "PERPETUAL",
                    "pricePrecision": 2,
                    "quantityPrecision": 3,
                    "filters": [
                        {"filterType": "PRICE_FILTER", "tickSize": "0.10", "minPrice": "1", "maxPrice": "1000000"},
                        {"filterType": "LOT_SIZE", "stepSize": "0.001", "minQty": "0.001", "maxQty": "1000"},
                        {"filterType": "MIN_NOTIONAL", "notional": "100"},
                    ],
                },
                {
                    "symbol": "ENSOUSDT",
                    "status": "TRADING",
                    "contractType": "PERPETUAL",
                    "pricePrecision": 6,
                    "quantityPrecision": 1,
                    "filters": [
                        {"filterType": "PRICE_FILTER", "tickSize": "0.000100", "minPrice": "0.000100", "maxPrice": "200"},
                        {"filterType": "LOT_SIZE", "stepSize": "0.1", "minQty": "0.1", "maxQty": "800000"},
                        {"filterType": "MARKET_LOT_SIZE", "stepSize": "0.1", "minQty": "0.1", "maxQty": "80000"},
                        {"filterType": "MIN_NOTIONAL", "notional": "5"},
                    ],
                },
            ]
        }

        config = fetch_futures_symbol_config("ENSOUSDT")

        self.assertEqual(config["symbol"], "ENSOUSDT")
        self.assertAlmostEqual(float(config["tick_size"] or 0.0), 0.0001, places=8)
        self.assertAlmostEqual(float(config["step_size"] or 0.0), 0.1, places=8)
        self.assertAlmostEqual(float(config["min_notional"] or 0.0), 5.0, places=8)

    @patch("grid_optimizer.data._http_get_json")
    def test_fetch_spot_symbol_config_selects_exact_symbol(self, mock_http_get_json) -> None:
        mock_http_get_json.return_value = {
            "symbols": [
                {
                    "symbol": "BTCUSDT",
                    "status": "TRADING",
                    "filters": [
                        {"filterType": "PRICE_FILTER", "tickSize": "0.01", "minPrice": "0.01", "maxPrice": "1000000"},
                        {"filterType": "LOT_SIZE", "stepSize": "0.00001", "minQty": "0.00001", "maxQty": "9000"},
                        {"filterType": "NOTIONAL", "minNotional": "10"},
                    ],
                },
                {
                    "symbol": "ENSOUSDT",
                    "status": "TRADING",
                    "filters": [
                        {"filterType": "PRICE_FILTER", "tickSize": "0.000100", "minPrice": "0.000100", "maxPrice": "200"},
                        {"filterType": "LOT_SIZE", "stepSize": "0.1", "minQty": "0.1", "maxQty": "800000"},
                        {"filterType": "NOTIONAL", "minNotional": "5"},
                    ],
                },
            ]
        }

        config = fetch_spot_symbol_config("ENSOUSDT")

        self.assertEqual(config["symbol"], "ENSOUSDT")
        self.assertAlmostEqual(float(config["tick_size"] or 0.0), 0.0001, places=8)
        self.assertAlmostEqual(float(config["step_size"] or 0.0), 0.1, places=8)
        self.assertAlmostEqual(float(config["min_notional"] or 0.0), 5.0, places=8)

    @patch("grid_optimizer.data._http_get_json")
    def test_fetch_futures_quote_volume_sum_aggregates_quote_volume(self, mock_http_get_json) -> None:
        end_ms = 5 * 60_000
        mock_http_get_json.return_value = [
            [0, "1", "1", "1", "1", "100", 59_999, "11.5", "0", "0", "0", "0"],
            [60_000, "1", "1", "1", "1", "100", 119_999, "13.0", "0", "0", "0", "0"],
            [120_000, "1", "1", "1", "1", "100", 179_999, "17.25", "0", "0", "0", "0"],
            [120_000, "1", "1", "1", "1", "100", 179_999, "99.99", "0", "0", "0", "0"],
            [180_000, "1", "1", "1", "1", "100", 239_999, "19.75", "0", "0", "0", "0"],
            [240_000, "1", "1", "1", "1", "100", 299_999, "23.5", "0", "0", "0", "0"],
        ]

        volume = fetch_futures_quote_volume_sum(
            "BARDUSDT",
            window_minutes=5,
            end_time_ms=end_ms,
        )

        self.assertAlmostEqual(volume, 85.0, places=8)


if __name__ == "__main__":
    unittest.main()
