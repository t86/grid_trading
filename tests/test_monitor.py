from __future__ import annotations

import json
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock
from unittest.mock import patch

from grid_optimizer.monitor import (
    _MONITOR_CACHE,
    _MONITOR_CACHE_LOCK,
    _MONITOR_INFLIGHT,
    _competition_current_metric_value,
    _count_monitor_audit_lines,
    _filter_events_since,
    _filter_rows_since,
    _load_or_fetch_income_rows,
    _load_or_fetch_trade_rows,
    _extract_futures_asset_snapshot,
    _read_event_window,
    _read_runner_process,
    build_monitor_alerts,
    build_monitor_snapshot,
    build_risk_activation_timeline,
    summarize_hourly_metrics,
    summarize_income,
    summarize_loop_events,
    summarize_user_trades,
)
from grid_optimizer.types import Candle


class MonitorTests(unittest.TestCase):
    def setUp(self) -> None:
        with _MONITOR_CACHE_LOCK:
            _MONITOR_CACHE.clear()
            _MONITOR_INFLIGHT.clear()

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

    def test_summarize_user_trades_accepts_string_trade_ids(self) -> None:
        trades = [
            {
                "id": "23256419:gx-pharosu-bestquot-2-91789828:1779191790879:17.0:0.6348",
                "time": 1000,
                "side": "BUY",
                "price": "0.6348",
                "qty": "17",
                "maker": True,
                "realizedPnl": "0",
                "commission": "0.01",
            }
        ]

        summary = summarize_user_trades(trades)

        self.assertEqual(summary["trade_count"], 1)
        self.assertAlmostEqual(summary["gross_notional"], 10.7916, places=8)

    def test_summarize_user_trades_groups_daily_volume_by_utc_day(self) -> None:
        day0 = datetime(2026, 5, 19, 15, 30, tzinfo=timezone.utc)
        day0_late = datetime(2026, 5, 19, 23, 30, tzinfo=timezone.utc)
        day1 = datetime(2026, 5, 20, 0, 30, tzinfo=timezone.utc)
        trades = [
            {
                "id": 1,
                "time": int(day0.timestamp() * 1000),
                "side": "BUY",
                "price": "2",
                "qty": "10",
                "realizedPnl": "0",
                "commission": "0",
            },
            {
                "id": 2,
                "time": int(day0_late.timestamp() * 1000),
                "side": "SELL",
                "price": "3",
                "qty": "10",
                "realizedPnl": "0",
                "commission": "0",
            },
            {
                "id": 3,
                "time": int(day1.timestamp() * 1000),
                "side": "SELL",
                "price": "5",
                "qty": "10",
                "realizedPnl": "0",
                "commission": "0",
            },
        ]

        summary = summarize_user_trades(trades)

        self.assertEqual(summary["daily_volume_rows"][0]["date"], "2026-05-20")
        self.assertEqual(summary["daily_volume_rows"][0]["timezone"], "UTC+0")
        self.assertAlmostEqual(summary["daily_volume_rows"][0]["gross_notional"], 50.0, places=8)
        self.assertEqual(summary["daily_volume_rows"][1]["date"], "2026-05-19")
        self.assertEqual(summary["daily_volume_rows"][1]["timezone"], "UTC+0")
        self.assertAlmostEqual(summary["daily_volume_rows"][1]["gross_notional"], 50.0, places=8)

    def test_pharos_competition_metric_uses_daily_sqrt_score(self) -> None:
        trade_summary = {
            "gross_notional": 1_250_000.0,
            "daily_volume_rows": [
                {"date": "2026-05-21", "gross_notional": 1_000_000.0},
                {"date": "2026-05-20", "gross_notional": 250_000.9999},
            ],
        }
        board = {
            "source_slug": "futures_pharos",
            "symbol": "PHAROS",
            "metric_field": "grade",
        }

        current = _competition_current_metric_value(board, trade_summary)

        self.assertAlmostEqual(current, 1500.0, places=8)

    def test_non_pharos_competition_metric_uses_gross_notional(self) -> None:
        trade_summary = {
            "gross_notional": 1_250_000.0,
            "daily_volume_rows": [{"gross_notional": 1_000_000.0}],
        }
        board = {
            "source_slug": "futures_bill",
            "symbol": "BILL",
            "metric_field": "tradingVolume",
        }

        current = _competition_current_metric_value(board, trade_summary)

        self.assertAlmostEqual(current, 1_250_000.0, places=8)

    @patch("grid_optimizer.monitor.fetch_time_paged")
    def test_load_or_fetch_trade_rows_prefers_full_exchange_window_over_audit_only_rows(
        self, mock_fetch_time_paged
    ) -> None:
        start = datetime(2026, 5, 14, 10, 0, tzinfo=timezone.utc)
        old_api_trade = {
            "id": 1,
            "time": int((start + timedelta(minutes=1)).timestamp() * 1000),
            "symbol": "PHAROSUSDT",
            "price": "1",
            "qty": "10",
        }
        duplicate_trade = {
            "id": 2,
            "time": int((start + timedelta(minutes=2)).timestamp() * 1000),
            "symbol": "PHAROSUSDT",
            "price": "1",
            "qty": "20",
        }
        audit_only_trade = {
            "id": 3,
            "time": int((start + timedelta(minutes=3)).timestamp() * 1000),
            "symbol": "PHAROSUSDT",
            "price": "1",
            "qty": "30",
        }
        mock_fetch_time_paged.return_value = [old_api_trade, duplicate_trade]

        with TemporaryDirectory() as tmpdir:
            audit_path = Path(tmpdir) / "pharosusdt_loop_trade_audit.jsonl"
            audit_path.write_text(
                json.dumps(duplicate_trade) + "\n" + json.dumps(audit_only_trade) + "\n",
                encoding="utf-8",
            )

            rows, meta = _load_or_fetch_trade_rows(
                audit_path=audit_path,
                symbol="PHAROSUSDT",
                api_key="key",
                api_secret="secret",
                session_start=start,
            )

        self.assertEqual([row["id"] for row in rows], [1, 2])
        self.assertEqual(meta["source"], "api")
        self.assertEqual(meta["audit_row_count"], 2)
        self.assertEqual(meta["api_row_count"], 2)
        self.assertEqual(meta["ignored_audit_only_row_count"], 1)

    def test_monitor_stats_anchor_persists_first_data_start(self) -> None:
        from grid_optimizer.monitor import _resolve_monitor_stats_anchor

        first_trade = datetime(2026, 5, 19, 13, 26, 56, tzinfo=timezone.utc)
        restarted_session = datetime(2026, 5, 20, 2, 12, 46, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            anchor_path = Path(tmpdir) / "pharosusdt_loop_monitor_anchor.json"

            anchor, meta = _resolve_monitor_stats_anchor(
                anchor_path=anchor_path,
                symbol="PHAROSUSDT",
                candidates=[
                    ("audit_trade_start", first_trade),
                    ("session_start", restarted_session),
                ],
            )
            self.assertEqual(anchor, first_trade)
            self.assertEqual(meta["source"], "audit_trade_start")
            self.assertTrue(anchor_path.exists())

            anchor, meta = _resolve_monitor_stats_anchor(
                anchor_path=anchor_path,
                symbol="PHAROSUSDT",
                candidates=[("session_start", restarted_session + timedelta(hours=1))],
            )
            self.assertEqual(anchor, first_trade)
            self.assertEqual(meta["source"], "persisted")

    def test_monitor_stats_anchor_moves_earlier_but_never_later(self) -> None:
        from grid_optimizer.monitor import _resolve_monitor_stats_anchor

        persisted = datetime(2026, 5, 19, 13, 26, 56, tzinfo=timezone.utc)
        competition_start = datetime(2026, 5, 14, 10, 0, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            anchor_path = Path(tmpdir) / "pharosusdt_loop_monitor_anchor.json"
            anchor_path.write_text(
                json.dumps(
                    {
                        "symbol": "PHAROSUSDT",
                        "stats_start_at": persisted.isoformat(),
                        "source": "audit_trade_start",
                    }
                ),
                encoding="utf-8",
            )

            anchor, meta = _resolve_monitor_stats_anchor(
                anchor_path=anchor_path,
                symbol="PHAROSUSDT",
                candidates=[
                    ("competition_start", competition_start),
                    ("session_start", persisted + timedelta(hours=2)),
                ],
            )

        self.assertEqual(anchor, competition_start)
        self.assertEqual(meta["source"], "competition_start")

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

    def test_summarize_user_trades_accepts_non_numeric_trade_id(self) -> None:
        trades = [
            {
                "id": "526323933:gx-chipu-takeprof-1-58228126:1778558235739:1447.0:0.06218",
                "time": 1000,
                "side": "BUY",
                "price": "0.06218",
                "qty": "1447",
                "maker": True,
                "realizedPnl": "0",
                "commission": "0.02",
            },
            {
                "id": 2,
                "time": 1000,
                "side": "SELL",
                "price": "0.06220",
                "qty": "1000",
                "maker": False,
                "realizedPnl": "0.05",
                "commission": "0.01",
            },
        ]

        summary = summarize_user_trades(trades)

        self.assertEqual(summary["trade_count"], 2)
        self.assertAlmostEqual(summary["gross_notional"], 152.17446, places=8)
        self.assertAlmostEqual(summary["commission"], 0.03, places=8)
        self.assertEqual(
            summary["recent_trades"][1]["id"],
            "526323933:gx-chipu-takeprof-1-58228126:1778558235739:1447.0:0.06218",
        )

    @patch("grid_optimizer.monitor.fetch_time_paged")
    def test_load_or_fetch_trade_rows_prefers_api_rows_over_audit_copy(self, mock_fetch_time_paged) -> None:
        session_start = datetime(2026, 3, 19, 8, 0, tzinfo=timezone.utc)
        audit_trade = {
            "id": 10,
            "symbol": "CHIPUSDT",
            "time": int((session_start + timedelta(minutes=5)).timestamp() * 1000),
            "side": "BUY",
            "price": "1.00",
            "qty": "3",
        }
        manual_trade = {
            "id": 11,
            "symbol": "CHIPUSDT",
            "time": int((session_start + timedelta(minutes=20)).timestamp() * 1000),
            "side": "SELL",
            "price": "1.10",
            "qty": "2",
            "realizedPnl": "0.2",
        }
        duplicate_audit_trade = dict(audit_trade)
        duplicate_audit_trade["price"] = "999"
        mock_fetch_time_paged.return_value = [manual_trade, duplicate_audit_trade]

        with TemporaryDirectory() as tmp:
            audit_path = Path(tmp) / "chipusdt_loop_trade_audit.jsonl"
            audit_path.write_text(json.dumps(audit_trade) + "\n", encoding="utf-8")

            rows, meta = _load_or_fetch_trade_rows(
                audit_path=audit_path,
                symbol="CHIPUSDT",
                api_key="key",
                api_secret="secret",
                session_start=session_start,
            )

        self.assertEqual([item["id"] for item in rows], [10, 11])
        self.assertEqual(meta["source"], "api")
        self.assertEqual(meta["audit_row_count"], 1)
        self.assertEqual(meta["api_row_count"], 2)
        self.assertEqual(meta["merged_row_count"], 2)
        self.assertEqual(meta["deduped_row_count"], 0)
        self.assertEqual(meta["ignored_audit_only_row_count"], 0)
        self.assertAlmostEqual(summarize_user_trades(rows)["gross_notional"], 2999.2, places=8)

    @patch("grid_optimizer.monitor.fetch_time_paged")
    def test_load_or_fetch_income_rows_merges_audit_and_api_funding(self, mock_fetch_time_paged) -> None:
        session_start = datetime(2026, 5, 8, 10, 0, tzinfo=timezone.utc)
        audit_income = {
            "symbol": "BILLUSDT",
            "incomeType": "FUNDING_FEE",
            "income": "0.03",
            "time": int((session_start + timedelta(hours=6)).timestamp() * 1000),
            "tranId": 100,
        }
        api_income = {
            "symbol": "BILLUSDT",
            "incomeType": "FUNDING_FEE",
            "income": "-0.02",
            "time": int((session_start + timedelta(hours=2)).timestamp() * 1000),
            "tranId": 99,
        }
        duplicate_audit_income = dict(audit_income)
        duplicate_audit_income["income"] = "999"
        mock_fetch_time_paged.return_value = [api_income, duplicate_audit_income]

        with TemporaryDirectory() as tmp:
            audit_path = Path(tmp) / "billusdt_loop_income_audit.jsonl"
            audit_path.write_text(json.dumps(audit_income) + "\n", encoding="utf-8")

            rows, meta = _load_or_fetch_income_rows(
                audit_path=audit_path,
                symbol="BILLUSDT",
                api_key="key",
                api_secret="secret",
                session_start=session_start,
            )

        self.assertEqual([item["tranId"] for item in rows], [99, 100])
        self.assertEqual(meta["source"], "audit+api")
        self.assertEqual(meta["audit_row_count"], 1)
        self.assertEqual(meta["api_row_count"], 2)
        self.assertEqual(meta["merged_row_count"], 2)
        self.assertEqual(meta["deduped_row_count"], 1)
        self.assertAlmostEqual(summarize_income(rows)["funding_fee"], 0.01, places=8)

    @patch("grid_optimizer.monitor.fetch_time_paged")
    def test_load_or_fetch_trade_rows_prefers_api_id_over_audit_composite_id(self, mock_fetch_time_paged) -> None:
        session_start = datetime(2026, 5, 12, 10, 0, tzinfo=timezone.utc)
        trade_time_ms = int((session_start + timedelta(minutes=5)).timestamp() * 1000)
        audit_trade = {
            "id": "84866504:gx-aigensynu-entrysho-2-86848575:1778586861251:5369.0:0.03352",
            "orderId": 84866504,
            "clientOrderId": "gx-aigensynu-entrysho-2-86848575",
            "symbol": "AIGENSYNUSDT",
            "time": trade_time_ms,
            "side": "SELL",
            "price": 0.03352,
            "qty": 5369.0,
            "realizedPnl": "0",
            "commission": "0.00003911",
            "commissionAsset": "BNB",
        }
        api_trade = {
            "id": 10324910,
            "orderId": 84866504,
            "symbol": "AIGENSYNUSDT",
            "time": trade_time_ms - 611,
            "side": "SELL",
            "price": "0.0335200",
            "qty": "5369",
            "realizedPnl": "0",
            "commission": "0.00003911",
            "commissionAsset": "BNB",
        }
        later_api_trade = {
            "id": 10324911,
            "orderId": 84866505,
            "symbol": "AIGENSYNUSDT",
            "time": trade_time_ms + 1000,
            "side": "BUY",
            "price": "0.03351",
            "qty": "100",
            "realizedPnl": "0.05",
            "commission": "0.001",
            "commissionAsset": "USDT",
        }
        mock_fetch_time_paged.return_value = [api_trade, later_api_trade]

        with TemporaryDirectory() as tmp:
            audit_path = Path(tmp) / "aigensynusdt_loop_trade_audit.jsonl"
            audit_path.write_text(json.dumps(audit_trade) + "\n", encoding="utf-8")

            rows, meta = _load_or_fetch_trade_rows(
                audit_path=audit_path,
                symbol="AIGENSYNUSDT",
                api_key="key",
                api_secret="secret",
                session_start=session_start,
            )

        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["id"], api_trade["id"])
        self.assertEqual(rows[1]["id"], later_api_trade["id"])
        self.assertEqual(meta["source"], "api")
        self.assertEqual(meta["audit_row_count"], 1)
        self.assertEqual(meta["api_row_count"], 2)
        self.assertEqual(meta["merged_row_count"], 2)
        self.assertEqual(meta["deduped_row_count"], 0)
        self.assertEqual(meta["ignored_audit_only_row_count"], 0)
        self.assertAlmostEqual(
            summarize_user_trades(rows)["gross_notional"],
            (0.03352 * 5369) + (0.03351 * 100),
            places=8,
        )

    @mock.patch("grid_optimizer.monitor.fetch_futures_user_trades", return_value=[])
    def test_load_or_fetch_trade_rows_bypasses_signed_cache(self, mock_fetch_user_trades) -> None:
        def fake_fetch_time_paged(**kwargs: object) -> list[dict[str, object]]:
            fetch_page = kwargs["fetch_page"]
            return fetch_page(start_time_ms=1, end_time_ms=2, limit=1000)

        with TemporaryDirectory() as tmpdir, mock.patch(
            "grid_optimizer.monitor.fetch_time_paged",
            side_effect=fake_fetch_time_paged,
        ):
            _load_or_fetch_trade_rows(
                audit_path=Path(tmpdir) / "trades.jsonl",
                symbol="CACHEUSDT",
                api_key="key",
                api_secret="secret",
                session_start=datetime.now(timezone.utc),
            )

        self.assertFalse(mock_fetch_user_trades.call_args.kwargs["use_cache"])

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

    def test_summarize_hourly_metrics_reports_trade_quality_breakdown(self) -> None:
        hour0 = datetime(2026, 3, 19, 8, 0, tzinfo=timezone.utc)
        candles = [
            Candle(open_time=hour0, close_time=hour0 + timedelta(hours=1), open=1.0, high=1.1, low=0.95, close=1.05),
        ]
        trades = [
            {
                "orderId": 10,
                "time": int((hour0 + timedelta(minutes=1)).timestamp() * 1000),
                "side": "BUY",
                "price": "1.00",
                "qty": "600",
                "realizedPnl": "0",
                "commission": "0.3",
            },
            {
                "orderId": 11,
                "time": int((hour0 + timedelta(minutes=2)).timestamp() * 1000),
                "side": "SELL",
                "price": "1.02",
                "qty": "600",
                "realizedPnl": "2.5",
                "commission": "0.3",
            },
            {
                "orderId": 12,
                "time": int((hour0 + timedelta(minutes=3)).timestamp() * 1000),
                "side": "SELL",
                "price": "1.01",
                "qty": "700",
                "realizedPnl": "-12",
                "commission": "0.2",
            },
            {
                "orderId": 13,
                "time": int((hour0 + timedelta(minutes=4)).timestamp() * 1000),
                "side": "BUY",
                "price": "1.00",
                "qty": "100",
                "realizedPnl": "-1",
                "commission": "0.05",
            },
        ]

        summary = summarize_hourly_metrics(
            trades,
            [],
            candles,
            order_role_lookup={
                "10": {"role": "entry_long"},
                "11": {"role": "take_profit_long"},
                "12": {"role": "hard_loss_forced_reduce_long"},
            },
            limit=24,
        )

        row = summary["rows"][0]
        quality = row["trade_quality"]
        self.assertEqual(quality["verdict"], "abnormal")
        self.assertGreaterEqual(len(quality["reasons"]), 1)
        self.assertEqual(quality["buy_count"], 2)
        self.assertEqual(quality["sell_count"], 2)
        self.assertAlmostEqual(quality["buckets"]["normal_grid"]["gross_notional"], 600.0, places=8)
        self.assertAlmostEqual(quality["buckets"]["take_profit_recycle"]["gross_notional"], 612.0, places=8)
        self.assertEqual(quality["buckets"]["hard_loss_forced_reduce"]["count"], 1)
        self.assertAlmostEqual(quality["buckets"]["hard_loss_forced_reduce"]["realized_pnl"], -12.0, places=8)
        self.assertEqual(quality["buckets"]["manual_unknown"]["count"], 1)
        self.assertAlmostEqual(row["buy_sell_imbalance_ratio"], abs(700.0 - 1319.0) / 2019.0, places=8)

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

    def test_build_risk_activation_timeline_tracks_regime_thresholds_and_recovery(self) -> None:
        base = datetime(2026, 5, 4, 8, 0, tzinfo=timezone.utc)
        rows = [
            {
                "ts": base.isoformat(),
                "cycle": 1,
                "mid_price": 0.025,
                "current_long_notional": 100.0,
                "current_short_notional": 90.0,
                "execution_regime_enabled": True,
                "execution_regime_state": "SAFE",
                "execution_regime_risk_score": 0.18,
                "execution_regime_quote_offset_bps": 1.5,
                "execution_regime_refresh_interval_ms": 500,
                "execution_regime_order_size_pct": 120.0,
            },
            {
                "ts": (base + timedelta(seconds=5)).isoformat(),
                "cycle": 2,
                "mid_price": 0.0251,
                "current_long_notional": 220.0,
                "current_short_notional": 110.0,
                "pause_buy_position_notional": 220.0,
                "max_position_notional": 260.0,
                "buy_paused": True,
                "pause_reasons": ["current_long_notional >= pause_buy_position_notional"],
                "adaptive_step_controls_active": True,
                "adaptive_step_base_step_price": 0.0002,
                "adaptive_step_effective_step_price": 0.0006,
                "adaptive_step_scale": 3.0,
                "execution_regime_enabled": True,
                "execution_regime_state": "CAUTION",
                "execution_regime_risk_score": 0.71,
                "execution_regime_hard_risk": False,
                "execution_regime_reason_codes": ["risk_score:caution"],
                "execution_regime_quote_offset_bps": 4.0,
                "execution_regime_refresh_interval_ms": 1500,
                "execution_regime_order_size_pct": 60.0,
            },
            {
                "ts": (base + timedelta(seconds=10)).isoformat(),
                "cycle": 3,
                "mid_price": 0.0252,
                "current_long_notional": 180.0,
                "current_short_notional": 100.0,
                "pause_buy_position_notional": 220.0,
                "max_position_notional": 260.0,
                "buy_paused": False,
                "pause_reasons": [],
                "adaptive_step_controls_active": False,
                "adaptive_step_base_step_price": 0.0002,
                "adaptive_step_effective_step_price": 0.0002,
                "adaptive_step_scale": 1.0,
                "execution_regime_enabled": True,
                "execution_regime_state": "NORMAL",
                "execution_regime_risk_score": 0.42,
                "execution_regime_quote_offset_bps": 2.5,
                "execution_regime_refresh_interval_ms": 1000,
                "execution_regime_order_size_pct": 100.0,
            },
        ]

        timeline = build_risk_activation_timeline(rows)

        self.assertEqual([item["type"] for item in timeline], [
            "execution_regime",
            "execution_regime",
            "adaptive_step",
            "soft_threshold",
            "execution_regime",
            "adaptive_step",
            "soft_threshold",
        ])
        self.assertEqual(timeline[1]["state"], "CAUTION")
        self.assertEqual(timeline[-1]["state"], "RECOVERED")
        self.assertAlmostEqual(timeline[2]["step"]["effective_step_price"], 0.0006)

    def test_build_monitor_alerts_maps_margin_error_and_pause_reason(self) -> None:
        loop_summary = {
            "recent": [
                {
                    "ts": "2026-03-30T15:00:00+00:00",
                    "cycle": 12,
                    "error_message": "RuntimeError: Binance API error -2019: Margin is insufficient.",
                }
            ]
        }
        risk_controls = {
            "buy_paused": True,
            "pause_reasons": ["current_long_notional=820.0000 >= pause_buy_position_notional=750.0000"],
            "short_paused": False,
            "short_pause_reasons": [],
            "buy_cap_applied": False,
            "short_cap_applied": False,
            "volatility_buy_pause": False,
            "shift_frozen": False,
        }

        alerts = build_monitor_alerts(
            loop_summary=loop_summary,
            warnings=[],
            risk_controls=risk_controls,
            runner={"configured": True, "is_running": True},
        )

        self.assertEqual(alerts[0]["code"], "margin_insufficient")
        self.assertEqual(alerts[0]["severity"], "critical")
        self.assertEqual(alerts[1]["code"], "buy_paused")

    def test_build_monitor_alerts_maps_rate_limit_and_dead_runner(self) -> None:
        loop_summary = {
            "recent": [
                {
                    "ts": "2026-03-30T15:00:00+00:00",
                    "cycle": 22,
                    "error_message": "RuntimeError: Binance API error -1003: Too many requests;",
                }
            ]
        }
        risk_controls = {
            "buy_paused": False,
            "pause_reasons": [],
            "short_paused": False,
            "short_pause_reasons": [],
            "buy_cap_applied": False,
            "short_cap_applied": False,
            "volatility_buy_pause": False,
            "shift_frozen": False,
        }

        alerts = build_monitor_alerts(
            loop_summary=loop_summary,
            warnings=["runner_pid_present_but_process_not_running"],
            risk_controls=risk_controls,
            runner={"configured": True, "is_running": False},
        )

        codes = [item["code"] for item in alerts]
        self.assertIn("runner_not_running", codes)
        self.assertIn("rate_limited", codes)

    def test_extract_futures_asset_snapshot_reads_asset_rows_with_usdt_fallback(self) -> None:
        account_info = {
            "availableBalance": "123.45",
            "totalWalletBalance": "156.78",
            "totalMarginBalance": "160.00",
            "maxWithdrawAmount": "120.00",
            "assets": [
                {
                    "asset": "BNB",
                    "walletBalance": "0.52",
                    "availableBalance": "0.31",
                    "marginBalance": "0.54",
                    "crossUnPnl": "0.01",
                    "maxWithdrawAmount": "0.30",
                }
            ],
        }

        usdt = _extract_futures_asset_snapshot(account_info, "USDT")
        bnb = _extract_futures_asset_snapshot(account_info, "BNB")

        self.assertIsNotNone(usdt)
        self.assertEqual(usdt["asset"], "USDT")
        self.assertAlmostEqual(usdt["wallet_balance"], 156.78, places=8)
        self.assertAlmostEqual(usdt["available_balance"], 123.45, places=8)
        self.assertAlmostEqual(usdt["max_withdraw_amount"], 120.0, places=8)

        self.assertIsNotNone(bnb)
        self.assertEqual(bnb["asset"], "BNB")
        self.assertAlmostEqual(bnb["wallet_balance"], 0.52, places=8)
        self.assertAlmostEqual(bnb["available_balance"], 0.31, places=8)

        self.assertIsNone(_extract_futures_asset_snapshot(account_info, "ETH"))

    @patch("grid_optimizer.monitor._load_or_fetch_income_rows", return_value=([], {"source": "test"}))
    @patch(
        "grid_optimizer.monitor._load_or_fetch_trade_rows",
        return_value=(
            [
                {"id": 1, "time": 1778222400000, "side": "BUY", "price": "0.05", "qty": "100", "realizedPnl": "0", "commission": "0.01"},
                {"id": 2, "time": 1778222460000, "side": "SELL", "price": "0.06", "qty": "50", "realizedPnl": "0.5", "commission": "0.01"},
            ],
            {"source": "audit+api", "row_count": 2},
        ),
    )
    @patch("grid_optimizer.monitor.fetch_futures_open_orders", return_value=[])
    @patch("grid_optimizer.monitor.fetch_futures_position_mode", return_value={"dualSidePosition": False})
    @patch("grid_optimizer.monitor.resolve_active_competition_board", return_value={})
    @patch(
        "grid_optimizer.monitor.fetch_futures_account_info_v3",
        return_value={
            "multiAssetsMargin": False,
            "availableBalance": "1000",
            "totalWalletBalance": "1000",
            "positions": [{"symbol": "CHIPUSDT", "positionAmt": "0", "entryPrice": "0", "breakEvenPrice": "0", "unRealizedProfit": "0"}],
        },
    )
    @patch("grid_optimizer.monitor.fetch_futures_klines", return_value=[])
    @patch("grid_optimizer.monitor.load_binance_api_credentials", return_value=("key", "secret"))
    @patch("grid_optimizer.monitor.fetch_futures_premium_index", return_value=[{"funding_rate": "0", "mark_price": "0.05"}])
    @patch("grid_optimizer.monitor.fetch_futures_book_tickers", return_value=[{"bid_price": "0.05", "ask_price": "0.051"}])
    def test_build_monitor_snapshot_uses_merged_trade_volume_for_risk_cumulative(
        self,
        _mock_book,
        _mock_premium,
        _mock_credentials,
        _mock_klines,
        _mock_account,
        _mock_competition,
        _mock_position_mode,
        _mock_open_orders,
        _mock_trade_rows,
        _mock_income_rows,
    ) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            events_path = root / "chipusdt_loop_events.jsonl"
            plan_path = root / "chipusdt_loop_latest_plan.json"
            submit_path = root / "chipusdt_loop_latest_submit.json"
            events_path.write_text(
                json.dumps({"ts": "2026-05-08T06:00:00+00:00", "cycle": 1, "cumulative_gross_notional": 1.0}) + "\n",
                encoding="utf-8",
            )
            plan_path.write_text("{}", encoding="utf-8")
            submit_path.write_text("{}", encoding="utf-8")

            snapshot = build_monitor_snapshot(
                symbol="CHIPUSDT",
                events_path=events_path,
                plan_path=plan_path,
                submit_report_path=submit_path,
                runner_process={"configured": True, "is_running": False, "config": {"symbol": "CHIPUSDT"}},
            )

        self.assertAlmostEqual(snapshot["trade_summary"]["gross_notional"], 8.0, places=8)
        self.assertAlmostEqual(snapshot["risk_controls"]["cumulative_gross_notional"], 8.0, places=8)
        self.assertEqual(snapshot["audit"]["trade_source"]["source"], "audit+api")

    @patch("grid_optimizer.monitor.build_competition_entry_volume_targets", return_value={"targets": []})
    @patch("grid_optimizer.monitor.build_competition_displacement_volume", return_value={"rank_steps": []})
    @patch("grid_optimizer.monitor.build_reward_volume_targets", return_value={"tiers": []})
    @patch("grid_optimizer.monitor._load_or_fetch_income_rows", return_value=([], {"source": "test"}))
    @patch("grid_optimizer.monitor.fetch_futures_open_orders", return_value=[])
    @patch("grid_optimizer.monitor.fetch_futures_position_mode", return_value={"dualSidePosition": False})
    @patch(
        "grid_optimizer.monitor.resolve_active_competition_board",
        return_value={"source_slug": "futures_pharos", "symbol": "PHAROS", "metric_field": "grade"},
    )
    @patch(
        "grid_optimizer.monitor.fetch_futures_account_info_v3",
        return_value={
            "multiAssetsMargin": False,
            "availableBalance": "1000",
            "totalWalletBalance": "1000",
            "positions": [{"symbol": "PHAROSUSDT", "positionAmt": "0", "entryPrice": "0", "breakEvenPrice": "0", "unRealizedProfit": "0"}],
        },
    )
    @patch("grid_optimizer.monitor.fetch_futures_klines", return_value=[])
    @patch("grid_optimizer.monitor.load_binance_api_credentials", return_value=("key", "secret"))
    @patch("grid_optimizer.monitor.fetch_futures_premium_index", return_value=[{"funding_rate": "0", "mark_price": "1"}])
    @patch("grid_optimizer.monitor.fetch_futures_book_tickers", return_value=[{"bid_price": "1", "ask_price": "1"}])
    def test_build_monitor_snapshot_uses_pharos_score_for_competition_cards(
        self,
        _mock_book,
        _mock_premium,
        _mock_credentials,
        _mock_klines,
        _mock_account,
        _mock_competition,
        _mock_position_mode,
        _mock_open_orders,
        _mock_income_rows,
        mock_reward_targets,
        mock_displacement,
        mock_entry_targets,
    ) -> None:
        day0 = datetime(2026, 5, 21, 1, 0, tzinfo=timezone.utc)
        day1 = datetime(2026, 5, 22, 1, 0, tzinfo=timezone.utc)
        trades = [
            {"id": 1, "time": int(day0.timestamp() * 1000), "side": "BUY", "price": "1000", "qty": "1000", "realizedPnl": "0", "commission": "0"},
            {"id": 2, "time": int(day1.timestamp() * 1000), "side": "SELL", "price": "500", "qty": "500", "realizedPnl": "0", "commission": "0"},
        ]

        with TemporaryDirectory() as tmpdir, patch(
            "grid_optimizer.monitor._load_or_fetch_trade_rows",
            return_value=(trades, {"source": "audit+api", "row_count": 2}),
        ):
            root = Path(tmpdir)
            events_path = root / "pharosusdt_loop_events.jsonl"
            plan_path = root / "pharosusdt_loop_latest_plan.json"
            submit_path = root / "pharosusdt_loop_latest_submit.json"
            events_path.write_text("{}", encoding="utf-8")
            plan_path.write_text("{}", encoding="utf-8")
            submit_path.write_text("{}", encoding="utf-8")

            snapshot = build_monitor_snapshot(
                symbol="PHAROSUSDT",
                events_path=events_path,
                plan_path=plan_path,
                submit_report_path=submit_path,
                runner_process={"configured": True, "is_running": False, "config": {"symbol": "PHAROSUSDT"}},
            )

        self.assertAlmostEqual(snapshot["trade_summary"]["gross_notional"], 1_250_000.0, places=8)
        self.assertAlmostEqual(snapshot["trade_summary"]["competition_current_metric"], 1500.0, places=8)
        self.assertEqual(snapshot["trade_summary"]["competition_current_metric_label"], "每日成交量开根号积分")
        self.assertAlmostEqual(mock_reward_targets.call_args.kwargs["current_volume"], 1500.0, places=8)
        self.assertAlmostEqual(mock_displacement.call_args.kwargs["current_volume"], 1500.0, places=8)
        self.assertAlmostEqual(mock_entry_targets.call_args.kwargs["current_volume"], 1500.0, places=8)

    @patch("grid_optimizer.monitor._load_or_fetch_income_rows", return_value=([], {"source": "test"}))
    @patch("grid_optimizer.monitor._load_or_fetch_trade_rows", return_value=([], {"source": "test"}))
    @patch("grid_optimizer.monitor.fetch_futures_open_orders", return_value=[])
    @patch("grid_optimizer.monitor.fetch_futures_position_mode", return_value={"dualSidePosition": False})
    @patch("grid_optimizer.monitor.resolve_active_competition_board", return_value={})
    @patch(
        "grid_optimizer.monitor.fetch_futures_account_info_v3",
        return_value={
            "multiAssetsMargin": False,
            "availableBalance": "1000",
            "totalWalletBalance": "1000",
            "positions": [
                {
                    "symbol": "XAUTUSDT",
                    "positionAmt": "0",
                    "entryPrice": "0",
                    "breakEvenPrice": "0",
                    "unRealizedProfit": "0",
                    "isolated": False,
                    "leverage": "2",
                }
            ],
        },
    )
    @patch("grid_optimizer.monitor.fetch_futures_klines", return_value=[])
    @patch("grid_optimizer.monitor.load_binance_api_credentials", return_value=("key", "secret"))
    @patch("grid_optimizer.monitor.fetch_futures_premium_index", return_value=[{"funding_rate": "0.0001", "mark_price": "4558.7"}])
    @patch("grid_optimizer.monitor.fetch_futures_book_tickers", return_value=[{"bid_price": "4558.6", "ask_price": "4558.8"}])
    def test_build_monitor_snapshot_exposes_xaut_adaptive_state(
        self,
        _mock_book,
        _mock_premium,
        _mock_credentials,
        _mock_klines,
        _mock_account,
        _mock_competition,
        _mock_position_mode,
        _mock_open_orders,
        _mock_trade_rows,
        _mock_income_rows,
    ) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            events_path = root / "xaut_events.jsonl"
            plan_path = root / "xaut_plan.json"
            submit_path = root / "xaut_submit.json"
            events_path.write_text("", encoding="utf-8")
            plan_path.write_text(
                '{"strategy_mode":"one_way_long","effective_strategy_profile":"xaut_long_adaptive_v1","effective_strategy_label":"XAUT 自适应做多 v1","xaut_adaptive":{"enabled":true,"active_state":"reduce_only","candidate_state":"reduce_only","pending_count":0,"reason":"15m amp=1.00% ret=-0.80%"}}',
                encoding="utf-8",
            )
            submit_path.write_text("{}", encoding="utf-8")

            snapshot = build_monitor_snapshot(
                symbol="XAUTUSDT",
                events_path=events_path,
                plan_path=plan_path,
                submit_report_path=submit_path,
                runner_process={
                    "configured": True,
                    "is_running": True,
                    "config": {
                        "symbol": "XAUTUSDT",
                        "strategy_profile": "xaut_long_adaptive_v1",
                        "strategy_mode": "one_way_long",
                    },
                },
            )

        risk = snapshot["risk_controls"]
        self.assertTrue(risk["xaut_adaptive_enabled"])
        self.assertEqual(risk["xaut_adaptive_state"], "reduce_only")
        self.assertEqual(risk["xaut_adaptive_candidate_state"], "reduce_only")
        self.assertEqual(risk["xaut_adaptive_reason"], "15m amp=1.00% ret=-0.80%")

    @patch("grid_optimizer.monitor._load_or_fetch_income_rows", return_value=([], {"source": "test"}))
    @patch("grid_optimizer.monitor._load_or_fetch_trade_rows", return_value=([], {"source": "test"}))
    @patch("grid_optimizer.monitor.fetch_futures_open_orders", return_value=[])
    @patch("grid_optimizer.monitor.fetch_futures_position_mode", return_value={"dualSidePosition": False})
    @patch("grid_optimizer.monitor.resolve_active_competition_board", return_value={})
    @patch(
        "grid_optimizer.monitor.fetch_futures_account_info_v3",
        return_value={
            "multiAssetsMargin": False,
            "availableBalance": "1000",
            "totalWalletBalance": "1000",
            "positions": [
                {
                    "symbol": "BARDUSDT",
                    "positionAmt": "0",
                    "entryPrice": "0",
                    "breakEvenPrice": "0",
                    "unRealizedProfit": "0",
                    "isolated": False,
                    "leverage": "2",
                }
            ],
        },
    )
    @patch("grid_optimizer.monitor.fetch_futures_klines", return_value=[])
    @patch("grid_optimizer.monitor.load_binance_api_credentials", return_value=("key", "secret"))
    @patch("grid_optimizer.monitor.fetch_futures_premium_index", return_value=[{"funding_rate": "0", "mark_price": "0.3268"}])
    @patch("grid_optimizer.monitor.fetch_futures_book_tickers", return_value=[{"bid_price": "0.3268", "ask_price": "0.3269"}])
    def test_build_monitor_snapshot_uses_live_flat_position_when_synthetic_runner_stopped(
        self,
        _mock_book,
        _mock_premium,
        _mock_credentials,
        _mock_klines,
        _mock_account,
        _mock_competition,
        _mock_position_mode,
        _mock_open_orders,
        _mock_trade_rows,
        _mock_income_rows,
    ) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            events_path = root / "bard_events.jsonl"
            plan_path = root / "bard_plan.json"
            submit_path = root / "bard_submit.json"
            events_path.write_text("", encoding="utf-8")
            plan_path.write_text(
                '{"strategy_mode":"synthetic_neutral","current_long_qty":4478,"current_short_qty":0,"synthetic_net_qty":4478,"current_long_notional":1463.6343,"current_short_notional":0}',
                encoding="utf-8",
            )
            submit_path.write_text(
                '{"plan_snapshot":{"current_long_qty":4478,"current_short_qty":0,"synthetic_net_qty":4478,"current_long_notional":1463.6343,"current_short_notional":0}}',
                encoding="utf-8",
            )

            snapshot = build_monitor_snapshot(
                symbol="BARDUSDT",
                events_path=events_path,
                plan_path=plan_path,
                submit_report_path=submit_path,
                runner_process={
                    "configured": True,
                    "is_running": False,
                    "config": {
                        "symbol": "BARDUSDT",
                        "strategy_mode": "synthetic_neutral",
                    },
                },
            )

        position = snapshot["position"]
        self.assertEqual(position["position_amt"], 0.0)
        self.assertEqual(position["virtual_long_qty"], 0.0)
        self.assertEqual(position["virtual_short_qty"], 0.0)
        self.assertEqual(str(position["virtual_short_qty"]), "0.0")
        self.assertEqual(position["virtual_net_qty"], 0.0)
        self.assertEqual(snapshot["risk_controls"]["current_long_notional"], 0.0)

    @patch("grid_optimizer.monitor._load_or_fetch_income_rows", return_value=([], {"source": "test"}))
    @patch("grid_optimizer.monitor._load_or_fetch_trade_rows", return_value=([], {"source": "test"}))
    @patch(
        "grid_optimizer.monitor.fetch_futures_open_orders",
        return_value=[
            {
                "orderId": 1,
                "clientOrderId": "close-long",
                "side": "SELL",
                "price": "1.11",
                "origQty": "4.5",
                "executedQty": "1.0",
                "reduceOnly": True,
                "positionSide": "LONG",
                "time": 123,
            },
            {
                "orderId": 2,
                "clientOrderId": "close-short",
                "side": "BUY",
                "price": "1.03",
                "origQty": "3.0",
                "executedQty": "0.25",
                "reduceOnly": True,
                "positionSide": "SHORT",
                "time": 124,
            },
        ],
    )
    @patch("grid_optimizer.monitor.fetch_futures_position_mode", return_value={"dualSidePosition": True})
    @patch("grid_optimizer.monitor.resolve_active_competition_board", return_value={})
    @patch(
        "grid_optimizer.monitor.fetch_futures_account_info_v3",
        return_value={
            "multiAssetsMargin": False,
            "availableBalance": "1000",
            "totalWalletBalance": "1000",
            "positions": [
                {
                    "symbol": "DUALUSDT",
                    "positionSide": "LONG",
                    "positionAmt": "10",
                    "entryPrice": "1.20",
                    "breakEvenPrice": "1.21",
                    "unRealizedProfit": "2.5",
                    "isolated": False,
                    "leverage": "5",
                },
                {
                    "symbol": "DUALUSDT",
                    "positionSide": "SHORT",
                    "positionAmt": "-7",
                    "entryPrice": "1.35",
                    "breakEvenPrice": "1.34",
                    "unRealizedProfit": "-1.5",
                    "isolated": False,
                    "leverage": "5",
                },
            ],
        },
    )
    @patch("grid_optimizer.monitor.fetch_futures_klines", return_value=[])
    @patch("grid_optimizer.monitor.load_binance_api_credentials", return_value=("key", "secret"))
    @patch("grid_optimizer.monitor.fetch_futures_premium_index", return_value=[{"funding_rate": "0", "mark_price": "1.10"}])
    @patch("grid_optimizer.monitor.fetch_futures_book_tickers", return_value=[{"bid_price": "1.09", "ask_price": "1.11"}])
    def test_build_monitor_snapshot_exposes_dual_side_position_legs_and_frozen_qty(
        self,
        _mock_book,
        _mock_premium,
        _mock_credentials,
        _mock_klines,
        _mock_account,
        _mock_competition,
        _mock_position_mode,
        _mock_open_orders,
        _mock_trade_rows,
        _mock_income_rows,
    ) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            events_path = root / "dual_events.jsonl"
            plan_path = root / "dual_plan.json"
            submit_path = root / "dual_submit.json"
            events_path.write_text("", encoding="utf-8")
            plan_path.write_text("{}", encoding="utf-8")
            submit_path.write_text("{}", encoding="utf-8")

            snapshot = build_monitor_snapshot(
                symbol="DUALUSDT",
                events_path=events_path,
                plan_path=plan_path,
                submit_report_path=submit_path,
                runner_process={
                    "configured": True,
                    "is_running": False,
                    "config": {"symbol": "DUALUSDT", "strategy_mode": "hedge_neutral"},
                },
            )

        position = snapshot["position"]
        self.assertFalse(position["one_way_mode"])
        self.assertEqual(position["position_amt"], 3.0)
        self.assertEqual(position["long_qty"], 10.0)
        self.assertEqual(position["short_qty"], 7.0)
        self.assertEqual(position["long_entry_price"], 1.20)
        self.assertEqual(position["short_entry_price"], 1.35)
        self.assertEqual(position["long_break_even_price"], 1.21)
        self.assertEqual(position["short_break_even_price"], 1.34)
        self.assertEqual(position["long_unrealized_pnl"], 2.5)
        self.assertEqual(position["short_unrealized_pnl"], -1.5)
        self.assertEqual(position["frozen_long_qty"], 3.5)
        self.assertEqual(position["frozen_short_qty"], 2.75)

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

    def test_read_runner_process_removes_stale_pid_file_when_process_missing(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            pid_path = root / "runner.pid"
            control_path = root / "runner.json"
            pid_path.write_text("999999", encoding="utf-8")
            control_path.write_text('{"strategy_profile":"volume_long_v4","symbol":"NIGHTUSDT"}', encoding="utf-8")

            result = _read_runner_process(pid_path=pid_path, control_path=control_path)

            self.assertFalse(result["is_running"])
            self.assertFalse(pid_path.exists())

    def test_read_event_window_keeps_recent_rows_and_bounds(self) -> None:
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "events.jsonl"
            base = datetime(2026, 4, 1, 0, 0, tzinfo=timezone.utc)
            path.write_text(
                "\n".join(
                    [
                        f'{{"ts": "{(base + timedelta(minutes=idx)).isoformat()}", "cycle": {idx}}}'
                        for idx in range(5)
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            rows, first_ts, last_ts = _read_event_window(
                path,
                since=base + timedelta(minutes=1),
                limit=2,
            )

        self.assertEqual([row["cycle"] for row in rows], [3, 4])
        self.assertEqual(first_ts, base + timedelta(minutes=1))
        self.assertEqual(last_ts, base + timedelta(minutes=4))

    def test_build_monitor_snapshot_deduplicates_inflight_requests(self) -> None:
        started = threading.Event()
        release = threading.Event()
        call_count = 0

        def fake_build(**kwargs: object) -> dict[str, object]:
            nonlocal call_count
            call_count += 1
            started.set()
            release.wait(timeout=2)
            return {"ok": True, "symbol": kwargs["symbol"], "ts": "2026-04-02T00:00:00+00:00"}

        with mock.patch("grid_optimizer.monitor._build_monitor_snapshot_uncached", side_effect=fake_build):
            with ThreadPoolExecutor(max_workers=2) as pool:
                future1 = pool.submit(build_monitor_snapshot, symbol="NIGHTUSDT")
                self.assertTrue(started.wait(timeout=2))
                future2 = pool.submit(build_monitor_snapshot, symbol="NIGHTUSDT")
                release.set()
                result1 = future1.result(timeout=2)
                result2 = future2.result(timeout=2)

        self.assertEqual(call_count, 1)
        self.assertEqual(result1, result2)

    def test_build_monitor_snapshot_times_out_waiting_for_stuck_inflight(self) -> None:
        started = threading.Event()
        release = threading.Event()

        def fake_build(**kwargs: object) -> dict[str, object]:
            started.set()
            release.wait(timeout=2)
            return {"ok": True, "symbol": kwargs["symbol"]}

        with mock.patch("grid_optimizer.monitor._build_monitor_snapshot_uncached", side_effect=fake_build):
            with mock.patch.dict(
                "os.environ",
                {"GRID_MONITOR_INFLIGHT_WAIT_TIMEOUT_SECONDS": "0.05"},
            ):
                with ThreadPoolExecutor(max_workers=2) as pool:
                    owner = pool.submit(build_monitor_snapshot, symbol="WAITUSDT")
                    self.assertTrue(started.wait(timeout=1))
                    waiter = pool.submit(build_monitor_snapshot, symbol="WAITUSDT")
                    try:
                        with self.assertRaisesRegex(RuntimeError, "timed out waiting for monitor snapshot"):
                            waiter.result(timeout=1)
                    finally:
                        release.set()
                    self.assertTrue(owner.result(timeout=1)["ok"])

    def test_count_monitor_audit_lines_skips_uncached_large_file(self) -> None:
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "large.jsonl"
            with path.open("wb") as f:
                f.truncate(128)

            with mock.patch.dict("os.environ", {"GRID_MONITOR_AUDIT_COUNT_MAX_SCAN_BYTES": "64"}):
                self.assertIsNone(_count_monitor_audit_lines(path))

    @patch("grid_optimizer.monitor._load_or_fetch_income_rows", return_value=([], {"source": "test"}))
    @patch("grid_optimizer.monitor._load_or_fetch_trade_rows", side_effect=RuntimeError("trade refresh unavailable"))
    @patch("grid_optimizer.monitor.fetch_futures_klines", return_value=[])
    @patch("grid_optimizer.monitor.fetch_futures_open_orders", return_value=[])
    @patch("grid_optimizer.monitor.fetch_futures_position_mode", return_value={"dualSidePosition": False})
    @patch(
        "grid_optimizer.monitor.fetch_futures_account_info_v3",
        return_value={
            "multiAssetsMargin": False,
            "availableBalance": "1000",
            "totalWalletBalance": "1000",
            "positions": [
                {
                    "symbol": "TESTUSDT",
                    "positionAmt": "4",
                    "entryPrice": "1.0",
                    "breakEvenPrice": "1.0",
                    "unRealizedProfit": "0.5",
                    "isolated": False,
                    "leverage": "2",
                }
            ],
        },
    )
    @patch("grid_optimizer.monitor.load_binance_api_credentials", return_value=("key", "secret"))
    @patch("grid_optimizer.monitor.fetch_futures_premium_index", return_value=[{"funding_rate": "0", "mark_price": "1.01"}])
    @patch("grid_optimizer.monitor.fetch_futures_book_tickers", return_value=[{"bid_price": "1.00", "ask_price": "1.02"}])
    @patch("grid_optimizer.monitor.resolve_active_competition_board", return_value={})
    def test_build_monitor_snapshot_keeps_position_when_trade_refresh_fails(
        self,
        _mock_competition,
        _mock_book,
        _mock_premium,
        _mock_credentials,
        _mock_account,
        _mock_position_mode,
        _mock_open_orders,
        _mock_klines,
        _mock_trade_rows,
        _mock_income_rows,
    ) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            events_path = root / "events.jsonl"
            plan_path = root / "plan.json"
            submit_path = root / "submit.json"
            events_path.write_text("", encoding="utf-8")
            plan_path.write_text("{}", encoding="utf-8")
            submit_path.write_text("{}", encoding="utf-8")

            snapshot = build_monitor_snapshot(
                symbol="TESTUSDT",
                events_path=events_path,
                plan_path=plan_path,
                submit_report_path=submit_path,
                runner_process={
                    "configured": True,
                    "is_running": False,
                    "config": {"symbol": "TESTUSDT", "strategy_mode": "one_way_long"},
                },
            )

        self.assertEqual(snapshot["position"]["position_amt"], 4.0)
        self.assertIn("trade_audit_refresh_failed: RuntimeError: trade refresh unavailable", snapshot["warnings"])
        self.assertFalse(any(str(item).startswith("account_read_failed:") for item in snapshot["warnings"]))

    @patch("grid_optimizer.monitor.load_binance_api_credentials", return_value=None)
    @patch("grid_optimizer.monitor.fetch_futures_open_orders", side_effect=AssertionError("should not fetch open orders"))
    @patch("grid_optimizer.monitor.fetch_futures_position_mode", side_effect=AssertionError("should not fetch position mode"))
    @patch("grid_optimizer.monitor.fetch_futures_account_info_v3", side_effect=AssertionError("should not fetch account info"))
    @patch("grid_optimizer.monitor.fetch_futures_premium_index", side_effect=AssertionError("should not fetch premium index"))
    @patch("grid_optimizer.monitor.fetch_futures_book_tickers", side_effect=AssertionError("should not fetch book ticker"))
    def test_build_monitor_snapshot_exposes_reduce_state_from_local_runtime(
        self,
        _mock_book,
        _mock_premium,
        _mock_account,
        _mock_position_mode,
        _mock_open_orders,
        _mock_credentials,
    ) -> None:
        now = datetime.now(timezone.utc)
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            events_path = root / "reduce_events.jsonl"
            plan_path = root / "reduce_plan.json"
            submit_path = root / "reduce_submit.json"
            events_path.write_text(
                json.dumps(
                    {
                        "ts": now.isoformat(),
                        "cycle": 88,
                        "strategy_mode": "synthetic_neutral",
                        "threshold_reduce_enabled": True,
                        "threshold_reduce_active": True,
                        "threshold_reduce_short_taker_timeout_active": True,
                        "adverse_reduce_enabled": True,
                        "adverse_reduce_active": False,
                        "adverse_reduce_short_aggressive": False,
                        "hard_loss_forced_reduce_enabled": True,
                        "hard_loss_forced_reduce_active": True,
                        "hard_loss_forced_reduce_reason": "hard_unrealized_loss_limit",
                        "hard_loss_forced_reduce_unrealized_pnl": -12.5,
                    }
                ),
                encoding="utf-8",
            )
            plan_path.write_text(
                json.dumps(
                    {
                        "generated_at": now.isoformat(),
                        "strategy_mode": "synthetic_neutral",
                        "bid_price": 1.24,
                        "ask_price": 1.25,
                        "mid_price": 1.245,
                        "actual_net_qty": -5.0,
                        "current_long_qty": 0.0,
                        "current_short_qty": 5.0,
                        "current_long_avg_price": 0.0,
                        "current_short_avg_price": 1.30,
                    }
                ),
                encoding="utf-8",
            )
            submit_path.write_text("{}", encoding="utf-8")

            snapshot = build_monitor_snapshot(
                symbol="CLUSDT",
                events_path=events_path,
                plan_path=plan_path,
                submit_report_path=submit_path,
                runner_process={
                    "configured": True,
                    "is_running": True,
                    "config": {
                        "symbol": "CLUSDT",
                        "strategy_profile": "clusdt_competition_maker_neutral_conservative_v1",
                        "strategy_mode": "synthetic_neutral",
                    },
                },
            )

        risk = snapshot["risk_controls"]
        self.assertTrue(risk["threshold_reduce_enabled"])
        self.assertTrue(risk["threshold_reduce_active"])
        self.assertTrue(risk["threshold_reduce_short_taker_timeout_active"])
        self.assertTrue(risk["adverse_reduce_enabled"])
        self.assertFalse(risk["adverse_reduce_active"])
        self.assertTrue(risk["hard_loss_forced_reduce_enabled"])
        self.assertTrue(risk["hard_loss_forced_reduce_active"])
        self.assertEqual(risk["hard_loss_forced_reduce_reason"], "hard_unrealized_loss_limit")
        self.assertAlmostEqual(risk["hard_loss_forced_reduce_unrealized_pnl"], -12.5)

    @patch("grid_optimizer.monitor.load_binance_api_credentials", return_value=None)
    @patch("grid_optimizer.monitor.fetch_futures_open_orders", side_effect=AssertionError("should not fetch open orders"))
    @patch("grid_optimizer.monitor.fetch_futures_position_mode", side_effect=AssertionError("should not fetch position mode"))
    @patch("grid_optimizer.monitor.fetch_futures_account_info_v3", side_effect=AssertionError("should not fetch account info"))
    @patch("grid_optimizer.monitor.fetch_futures_premium_index", side_effect=AssertionError("should not fetch premium index"))
    @patch("grid_optimizer.monitor.fetch_futures_book_tickers", side_effect=AssertionError("should not fetch book ticker"))
    def test_build_monitor_snapshot_prefers_local_runtime_snapshot_for_live_runner(
        self,
        _mock_book,
        _mock_premium,
        _mock_account,
        _mock_position_mode,
        _mock_open_orders,
        _mock_credentials,
    ) -> None:
        now = datetime.now(timezone.utc)
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            events_path = root / "test_events.jsonl"
            plan_path = root / "test_plan.json"
            submit_path = root / "test_submit.json"
            events_path.write_text(
                "\n".join(
                    [
                        json.dumps(
                            {
                                "ts": (now - timedelta(seconds=12)).isoformat(),
                                "cycle": 101,
                                "mid_price": 1.24,
                                "error_message": None,
                            }
                        ),
                        json.dumps(
                            {
                                "ts": (now - timedelta(seconds=4)).isoformat(),
                                "cycle": 102,
                                "mid_price": 1.245,
                                "error_message": None,
                            }
                        ),
                    ]
                ),
                encoding="utf-8",
            )
            plan_path.write_text(
                json.dumps(
                    {
                        "generated_at": now.isoformat(),
                        "strategy_mode": "one_way_long",
                        "bid_price": 1.24,
                        "ask_price": 1.25,
                        "mid_price": 1.245,
                        "funding_rate": 0.0003,
                        "actual_net_qty": 9.0,
                        "current_long_qty": 9.0,
                        "current_short_qty": 0.0,
                        "current_long_avg_price": 1.18,
                        "current_short_avg_price": 0.0,
                        "dual_side_position": False,
                        "kept_orders": [
                            {
                                "orderId": 11,
                                "clientOrderId": "keep-buy",
                                "side": "BUY",
                                "type": "LIMIT",
                                "price": "1.20",
                                "origQty": "4",
                                "executedQty": "0",
                                "reduceOnly": False,
                                "positionSide": "BOTH",
                                "time": 123456,
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            submit_path.write_text(
                json.dumps(
                    {
                        "generated_at": now.isoformat(),
                        "executed": True,
                        "placed_orders": [
                            {
                                "request": {
                                    "role": "take_profit",
                                    "side": "SELL",
                                    "position_side": "BOTH",
                                    "qty": 4.0,
                                    "submitted_price": 1.31,
                                },
                                "response": {
                                    "orderId": 22,
                                    "clientOrderId": "new-sell",
                                    "side": "SELL",
                                    "price": "1.31",
                                    "origQty": "4",
                                    "executedQty": "0",
                                    "reduceOnly": True,
                                    "positionSide": "BOTH",
                                    "time": 123999,
                                },
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            snapshot = build_monitor_snapshot(
                symbol="TESTUSDT",
                events_path=events_path,
                plan_path=plan_path,
                submit_report_path=submit_path,
                runner_process={
                    "configured": True,
                    "is_running": True,
                    "config": {
                        "symbol": "TESTUSDT",
                        "strategy_profile": "test_profile",
                        "strategy_mode": "one_way_long",
                        "leverage": 3,
                    },
                },
            )

        self.assertEqual(snapshot["market"]["bid_price"], 1.24)
        self.assertEqual(snapshot["market"]["ask_price"], 1.25)
        self.assertEqual(snapshot["market"]["mid_price"], 1.245)
        self.assertEqual(snapshot["market"]["funding_rate"], 0.0003)
        self.assertEqual(snapshot["position"]["position_amt"], 9.0)
        self.assertEqual(snapshot["position"]["long_qty"], 9.0)
        self.assertEqual(snapshot["position"]["short_qty"], 0.0)
        self.assertEqual(snapshot["position"]["entry_price"], 1.18)
        self.assertEqual(snapshot["position"]["leverage"], 3)
        self.assertEqual(len(snapshot["open_orders"]), 2)
        self.assertEqual({item["client_order_id"] for item in snapshot["open_orders"]}, {"keep-buy", "new-sell"})
        self.assertIn("missing_binance_api_credentials", snapshot["warnings"])
        self.assertNotIn("market_read_failed: AssertionError: should not fetch book ticker", snapshot["warnings"])

    @patch("grid_optimizer.monitor.load_binance_api_credentials", return_value=None)
    @patch("grid_optimizer.monitor.fetch_futures_open_orders", side_effect=AssertionError("should not fetch open orders"))
    @patch("grid_optimizer.monitor.fetch_futures_position_mode", side_effect=AssertionError("should not fetch position mode"))
    @patch("grid_optimizer.monitor.fetch_futures_account_info_v3", side_effect=AssertionError("should not fetch account info"))
    @patch("grid_optimizer.monitor.fetch_futures_premium_index", side_effect=AssertionError("should not fetch premium index"))
    @patch("grid_optimizer.monitor.fetch_futures_book_tickers", side_effect=AssertionError("should not fetch book ticker"))
    @patch("grid_optimizer.monitor.resolve_active_competition_board", return_value=None)
    def test_build_monitor_snapshot_adds_frozen_inventory_back_to_dual_side_position(
        self,
        _mock_competition,
        _mock_book,
        _mock_premium,
        _mock_account,
        _mock_position_mode,
        _mock_open_orders,
        _mock_credentials,
    ) -> None:
        now = datetime.now(timezone.utc)
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            events_path = root / "test_events.jsonl"
            plan_path = root / "test_plan.json"
            submit_path = root / "test_submit.json"
            events_path.write_text(
                json.dumps(
                    {
                        "ts": (now - timedelta(seconds=4)).isoformat(),
                        "cycle": 102,
                        "mid_price": 0.64,
                        "error_message": None,
                    }
                ),
                encoding="utf-8",
            )
            plan_path.write_text(
                json.dumps(
                    {
                        "generated_at": now.isoformat(),
                        "strategy_mode": "hedge_best_quote_maker_volume_v1",
                        "dual_side_position": True,
                        "bid_price": 0.639,
                        "ask_price": 0.641,
                        "mid_price": 0.64,
                        "actual_net_qty": 243.0,
                        "strategy_actual_net_qty": -57.0,
                        "current_long_qty": 56.0,
                        "current_short_qty": 113.0,
                        "current_long_avg_price": 0.64095,
                        "current_short_avg_price": 0.64531,
                    }
                ),
                encoding="utf-8",
            )
            submit_path.write_text(json.dumps({"generated_at": now.isoformat()}), encoding="utf-8")

            snapshot = build_monitor_snapshot(
                symbol="PHAROSUSDT",
                events_path=events_path,
                plan_path=plan_path,
                submit_report_path=submit_path,
                runner_process={
                    "configured": True,
                    "is_running": True,
                    "config": {
                        "symbol": "PHAROSUSDT",
                        "strategy_profile": "pharosusdt_hedge_best_quote_maker_volume_v1",
                        "strategy_mode": "hedge_best_quote_maker_volume_v1",
                    },
                },
            )

        position = snapshot["position"]
        self.assertEqual(position["position_amt"], 243.0)
        self.assertEqual(position["active_long_qty"], 56.0)
        self.assertEqual(position["active_short_qty"], 113.0)
        self.assertEqual(position["inventory_frozen_long_qty"], 300.0)
        self.assertEqual(position["inventory_frozen_short_qty"], 0.0)
        self.assertEqual(position["frozen_long_qty"], 300.0)
        self.assertEqual(position["frozen_short_qty"], 0.0)
        self.assertEqual(position["long_qty"], 356.0)
        self.assertEqual(position["short_qty"], 113.0)
        self.assertAlmostEqual(snapshot["risk_controls"]["current_long_notional"], 35.84, places=8)
        self.assertAlmostEqual(snapshot["risk_controls"]["current_short_notional"], 72.32, places=8)

    @patch("grid_optimizer.monitor.resolve_active_competition_board", return_value=None)
    @patch("grid_optimizer.monitor.load_binance_api_credentials", return_value=("key", "secret"))
    @patch("grid_optimizer.monitor.fetch_futures_open_orders", return_value=[])
    @patch("grid_optimizer.monitor.fetch_futures_position_mode", return_value={"dualSidePosition": True})
    @patch(
        "grid_optimizer.monitor.fetch_futures_account_info_v3",
        return_value={
            "availableBalance": "1000",
            "totalWalletBalance": "1200",
            "multiAssetsMargin": False,
            "positions": [
                {
                    "symbol": "PHAROSUSDT",
                    "positionSide": "LONG",
                    "positionAmt": "577",
                    "entryPrice": "0.6385726",
                    "breakEvenPrice": "0.6385726",
                    "unRealizedProfit": "1.0",
                    "isolated": False,
                    "leverage": "5",
                },
                {
                    "symbol": "PHAROSUSDT",
                    "positionSide": "SHORT",
                    "positionAmt": "-393",
                    "entryPrice": "0.6397188",
                    "breakEvenPrice": "0.6397188",
                    "unRealizedProfit": "-0.5",
                    "isolated": False,
                    "leverage": "5",
                },
            ],
        },
    )
    def test_build_monitor_snapshot_uses_exchange_legs_for_frozen_inventory_display(
        self,
        _mock_account,
        _mock_position_mode,
        _mock_open_orders,
        _mock_credentials,
        _mock_competition,
    ) -> None:
        now = datetime.now(timezone.utc)
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            events_path = root / "test_events.jsonl"
            plan_path = root / "test_plan.json"
            submit_path = root / "test_submit.json"
            events_path.write_text(
                json.dumps({"ts": (now - timedelta(seconds=4)).isoformat(), "cycle": 102}),
                encoding="utf-8",
            )
            plan_path.write_text(
                json.dumps(
                    {
                        "generated_at": now.isoformat(),
                        "strategy_mode": "hedge_best_quote_maker_volume_v1",
                        "dual_side_position": True,
                        "bid_price": 0.638,
                        "ask_price": 0.640,
                        "mid_price": 0.639,
                        "actual_net_qty": 184.0,
                        "strategy_actual_net_qty": -236.0,
                        "current_long_qty": 157.0,
                        "current_short_qty": 393.0,
                    }
                ),
                encoding="utf-8",
            )
            submit_path.write_text(json.dumps({"generated_at": now.isoformat()}), encoding="utf-8")

            snapshot = build_monitor_snapshot(
                symbol="PHAROSUSDT",
                events_path=events_path,
                plan_path=plan_path,
                submit_report_path=submit_path,
                runner_process={
                    "configured": True,
                    "is_running": True,
                    "config": {
                        "symbol": "PHAROSUSDT",
                        "strategy_profile": "pharosusdt_hedge_best_quote_maker_volume_v1",
                        "strategy_mode": "hedge_best_quote_maker_volume_v1",
                    },
                },
            )

        position = snapshot["position"]
        self.assertEqual(position["long_qty"], 577.0)
        self.assertEqual(position["short_qty"], 393.0)
        self.assertEqual(position["position_amt"], 184.0)
        self.assertEqual(position["active_long_qty"], 157.0)
        self.assertEqual(position["active_short_qty"], 393.0)
        self.assertEqual(position["inventory_frozen_long_qty"], 420.0)
        self.assertEqual(position["inventory_frozen_short_qty"], 0.0)
        self.assertEqual(position["frozen_long_qty"], 420.0)
        self.assertEqual(position["frozen_short_qty"], 0.0)
        self.assertAlmostEqual(snapshot["risk_controls"]["current_long_notional"], 100.323, places=8)
        self.assertAlmostEqual(snapshot["risk_controls"]["current_short_notional"], 251.127, places=8)

    @patch("grid_optimizer.monitor.load_binance_api_credentials", return_value=None)
    @patch("grid_optimizer.monitor.fetch_futures_book_tickers", return_value=[{"bid_price": "96.1", "ask_price": "96.3"}])
    @patch("grid_optimizer.monitor.fetch_futures_premium_index", return_value=[{"funding_rate": "0.0002", "mark_price": "96.2"}])
    def test_build_monitor_snapshot_handles_missing_creds_without_local_position_snapshot_for_synthetic_mode(
        self,
        _mock_premium,
        _mock_book,
        _mock_credentials,
    ) -> None:
        stale = datetime.now(timezone.utc) - timedelta(minutes=10)
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            events_path = root / "stale_events.jsonl"
            plan_path = root / "stale_plan.json"
            submit_path = root / "stale_submit.json"
            events_path.write_text(
                json.dumps(
                    {
                        "ts": stale.isoformat(),
                        "cycle": 1,
                        "strategy_mode": "synthetic_neutral",
                    }
                ),
                encoding="utf-8",
            )
            plan_path.write_text(
                json.dumps(
                    {
                        "generated_at": stale.isoformat(),
                        "strategy_mode": "synthetic_neutral",
                    }
                ),
                encoding="utf-8",
            )
            submit_path.write_text("{}", encoding="utf-8")

            snapshot = build_monitor_snapshot(
                symbol="CLUSDT",
                events_path=events_path,
                plan_path=plan_path,
                submit_report_path=submit_path,
                runner_process={
                    "configured": True,
                    "is_running": True,
                    "config": {
                        "symbol": "CLUSDT",
                        "strategy_profile": "clusdt_competition_maker_neutral_conservative_v1",
                        "strategy_mode": "synthetic_neutral",
                    },
                },
            )

        self.assertAlmostEqual(snapshot["market"]["mid_price"], 96.2)
        self.assertEqual(snapshot["position"]["virtual_long_qty"], 0.0)
        self.assertEqual(snapshot["position"]["virtual_short_qty"], 0.0)
        self.assertEqual(snapshot["position"]["virtual_net_qty"], 0.0)
        self.assertIn("missing_binance_api_credentials", snapshot["warnings"])


if __name__ == "__main__":
    unittest.main()
