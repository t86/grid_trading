from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from grid_optimizer.carry_watchlist import check_watchlist, load_watchlist, update_watchlist


class CarryWatchlistTests(unittest.TestCase):
    def test_watchlist_add_remove(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "watchlist.json"
            self.assertEqual(update_watchlist("btcusdt", True, path), ["BTCUSDT"])
            self.assertEqual(load_watchlist(path), ["BTCUSDT"])
            self.assertEqual(update_watchlist("BTCUSDT", False, path), [])

    def test_positive_to_negative_alert_is_limited_to_one_attempt_per_day(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            folder = Path(temp_dir)
            watchlist_path = folder / "watchlist.json"
            state_path = folder / "state.json"
            update_watchlist("BTCUSDT", True, watchlist_path)
            moments = [
                datetime(2026, 9, 7, 0, 0, tzinfo=timezone.utc),
                datetime(2026, 9, 7, 1, 0, tzinfo=timezone.utc),
                datetime(2026, 9, 7, 2, 0, tzinfo=timezone.utc),
                datetime(2026, 9, 7, 3, 0, tzinfo=timezone.utc),
            ]
            positive = [{"symbol": "BTCUSDT", "funding_rate": 0.0001}]
            negative = [{"symbol": "BTCUSDT", "funding_rate": -0.0001}]
            with patch("grid_optimizer.carry_watchlist.fetch_futures_premium_index", side_effect=[positive, negative, positive, negative]), patch(
                "grid_optimizer.carry_watchlist.bark_configured", return_value=True
            ), patch("grid_optimizer.carry_watchlist._send_bark", return_value={"sent": False, "error": "network"}) as send:
                first = check_watchlist(watchlist_path=watchlist_path, state_path=state_path, now=moments[0])
                second = check_watchlist(watchlist_path=watchlist_path, state_path=state_path, now=moments[1])
                third = check_watchlist(watchlist_path=watchlist_path, state_path=state_path, now=moments[2])
                fourth = check_watchlist(watchlist_path=watchlist_path, state_path=state_path, now=moments[3])
            self.assertFalse(first["rows"][0]["transition"])
            self.assertTrue(second["rows"][0]["transition"])
            self.assertEqual(send.call_count, 1)
            self.assertFalse(third["rows"][0]["transition"])
            self.assertTrue(fourth["rows"][0]["transition"])
