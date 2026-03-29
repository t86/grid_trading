from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

from grid_optimizer.monitor import (
    _filter_events_since,
    _filter_rows_since,
    _read_runner_process,
    summarize_hourly_metrics,
    summarize_income,
    summarize_loop_events,
    summarize_user_trades,
)
from grid_optimizer.types import Candle


class MonitorTests(unittest.TestCase):
    def test_summarize_user_trades_accumulates_counts_volume_and_pnl(self) -> None:
        trades = [
            {
                "id": 1,
                "time": 1000,
                "side": "BUY",
                "price": "0.05",
                "qty": "100",
                "maker": True,
                "realizedPnl": "0",
                "commission": "0.01",
            },
            {
                "id": 2,
                "time": 2000,
                "side": "SELL",
                "price": "0.051",
                "qty": "100",
                "maker": True,
                "realizedPnl": "0.10",
                "commission": "0.01",
            },
        ]

        summary = summarize_user_trades(trades)

        self.assertEqual(summary["trade_count"], 2)
        self.assertEqual(summary["maker_count"], 2)
        self.assertAlmostEqual(summary["gross_notional"], 10.1, places=8)
        self.assertAlmostEqual(summary["realized_pnl"], 0.1, places=8)
        self.assertAlmostEqual(summary["commission"], 0.02, places=8)
        self.assertEqual(len(summary["series"]), 2)

    def test_summarize_income_sums_funding_fee(self) -> None:
        rows = [
            {"time": 1000, "income": "0.05", "incomeType": "FUNDING_FEE", "asset": "USDT", "info": ""},
            {"time": 2000, "income": "-0.02", "incomeType": "FUNDING_FEE", "asset": "USDT", "info": ""},
        ]

        summary = summarize_income(rows)

        self.assertAlmostEqual(summary["funding_fee"], 0.03, places=8)
        self.assertEqual(len(summary["recent_income"]), 2)

    def test_summarize_user_trades_can_convert_non_usdt_commission(self) -> None:
        trades = [
            {
                "id": 1,
                "time": 1000,
                "side": "BUY",
                "price": "0.05",
                "qty": "100",
                "maker": True,
                "realizedPnl": "0",
                "commission": "0.001",
                "commissionAsset": "BNB",
            },
        ]

        summary = summarize_user_trades(trades, commission_converter=lambda _: 0.75)

        self.assertAlmostEqual(summary["commission"], 0.75, places=8)
        self.assertAlmostEqual(summary["commission_raw_by_asset"]["BNB"], 0.001, places=8)
        self.assertAlmostEqual(summary["recent_trades"][0]["commission_usdt"], 0.75, places=8)

    def test_summarize_hourly_metrics_combines_trades_income_and_candles(self) -> None:
        hour0 = datetime(2026, 3, 19, 8, 0, tzinfo=timezone.utc)
        hour1 = datetime(2026, 3, 19, 9, 0, tzinfo=timezone.utc)
        candles = [
            Candle(open_time=hour0, close_time=hour0 + timedelta(hours=1), open=1.0, high=1.1, low=0.95, close=1.05),
            Candle(open_time=hour1, close_time=hour1 + timedelta(hours=1), open=1.05, high=1.2, low=1.0, close=1.1),
        ]
        trades = [
            {
                "id": 1,
                "time": int((hour0 + timedelta(minutes=10)).timestamp() * 1000),
                "side": "BUY",
                "price": "1.00",
                "qty": "10",
                "realizedPnl": "0",
                "commission": "0.1",
            },
            {
                "id": 2,
                "time": int((hour0 + timedelta(minutes=40)).timestamp() * 1000),
                "side": "SELL",
                "price": "1.10",
                "qty": "10",
                "realizedPnl": "0.5",
                "commission": "0.1",
            },
            {
                "id": 3,
                "time": int((hour1 + timedelta(minutes=5)).timestamp() * 1000),
                "side": "SELL",
                "price": "1.08",
                "qty": "5",
                "realizedPnl": "-0.2",
                "commission": "0.05",
            },
        ]
        income_rows = [
            {
                "time": int((hour1 + timedelta(minutes=30)).timestamp() * 1000),
                "income": "0.03",
                "incomeType": "FUNDING_FEE",
            }
        ]

        summary = summarize_hourly_metrics(trades, income_rows, candles, limit=24)

        self.assertEqual(summary["row_count"], 2)
        latest = summary["rows"][0]
        previous = summary["rows"][1]
        self.assertEqual(latest["hour_start"], hour1.isoformat())
        self.assertAlmostEqual(latest["gross_notional"], 5.4, places=8)
        self.assertAlmostEqual(latest["realized_pnl"], -0.2, places=8)
        self.assertAlmostEqual(latest["commission"], 0.05, places=8)
        self.assertAlmostEqual(latest["funding_fee"], 0.03, places=8)
        self.assertAlmostEqual(latest["net_after_fees_and_funding"], -0.22, places=8)
        self.assertAlmostEqual(latest["return_ratio"], (1.1 / 1.05) - 1.0, places=8)
        self.assertAlmostEqual(latest["amplitude_ratio"], (1.2 / 1.0) - 1.0, places=8)
        self.assertEqual(previous["hour_start"], hour0.isoformat())
        self.assertAlmostEqual(previous["gross_notional"], 21.0, places=8)
        self.assertAlmostEqual(previous["net_after_fees_and_funding"], 0.3, places=8)

    def test_summarize_loop_events_marks_recent_loop_alive(self) -> None:
        now = datetime.now(timezone.utc)
        rows = [
            {
                "ts": (now - timedelta(seconds=40)).isoformat(),
                "cycle": 1,
                "mid_price": 0.05,
                "error_message": None,
            },
            {
                "ts": (now - timedelta(seconds=20)).isoformat(),
                "cycle": 2,
                "mid_price": 0.051,
                "error_message": None,
            },
        ]

        summary = summarize_loop_events(rows)

        self.assertTrue(summary["is_alive"])
        self.assertEqual(summary["cycle_count"], 2)
        self.assertEqual(summary["error_count"], 0)
        self.assertEqual(summary["success_count"], 2)

    def test_filter_rows_since_discards_older_trade_rows(self) -> None:
        floor = datetime(2026, 3, 29, 0, 0, tzinfo=timezone.utc)
        rows = [
            {"time": int((floor - timedelta(minutes=1)).timestamp() * 1000)},
            {"time": int((floor + timedelta(minutes=1)).timestamp() * 1000)},
        ]
        filtered = _filter_rows_since(rows, since=floor, row_time_ms=lambda item: int(item.get("time", 0)))
        self.assertEqual(len(filtered), 1)
        self.assertGreaterEqual(filtered[0]["time"], int(floor.timestamp() * 1000))

    def test_filter_events_since_discards_older_loop_events(self) -> None:
        floor = datetime(2026, 3, 29, 0, 0, tzinfo=timezone.utc)
        rows = [
            {"ts": (floor - timedelta(seconds=1)).isoformat()},
            {"ts": (floor + timedelta(seconds=1)).isoformat()},
        ]
        filtered = _filter_events_since(rows, floor)
        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered[0]["ts"], (floor + timedelta(seconds=1)).isoformat())

    def test_read_runner_process_merges_control_payload_with_parsed_args(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            pid_path = root / "runner.pid"
            control_path = root / "runner.json"
            pid_path.write_text("999999", encoding="utf-8")
            control_path.write_text('{"strategy_profile":"volume_long_v4","symbol":"NIGHTUSDT"}', encoding="utf-8")

            result = _read_runner_process(pid_path=pid_path, control_path=control_path)

            self.assertEqual(result["config"]["strategy_profile"], "volume_long_v4")
            self.assertEqual(result["config"]["symbol"], "NIGHTUSDT")


if __name__ == "__main__":
    unittest.main()
