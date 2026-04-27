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
    _filter_events_since,
    _filter_rows_since,
    _extract_futures_asset_snapshot,
    _read_event_window,
    _read_runner_process,
    build_monitor_alerts,
    build_monitor_snapshot,
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


if __name__ == "__main__":
    unittest.main()
