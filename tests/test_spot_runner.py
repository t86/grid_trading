from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

from grid_optimizer.types import Candle
from grid_optimizer.web import (
    SPOT_RUNNER_DEFAULT_CONFIG,
    SPOT_RUNNER_PAGE,
    SPOT_STRATEGIES_PAGE,
    _build_spot_runner_snapshot,
    _build_spot_runner_command,
    _normalize_spot_runner_payload,
    _start_spot_runner_process,
)


class SpotRunnerTests(unittest.TestCase):
    def test_spot_runner_page_contains_controls(self) -> None:
        self.assertIn("现货比赛执行台", SPOT_RUNNER_PAGE)
        self.assertIn('id="start_btn"', SPOT_RUNNER_PAGE)
        self.assertIn('id="stop_btn"', SPOT_RUNNER_PAGE)
        self.assertIn('id="strategy_mode"', SPOT_RUNNER_PAGE)
        self.assertIn("Spot V1 单向做多静态网格", SPOT_RUNNER_PAGE)
        self.assertIn("/spot_strategies", SPOT_RUNNER_PAGE)
        self.assertIn('id="hourly_meta"', SPOT_RUNNER_PAGE)
        self.assertIn('id="hourly_body"', SPOT_RUNNER_PAGE)

    def test_spot_strategies_page_contains_overview(self) -> None:
        self.assertIn("现货策略总览", SPOT_STRATEGIES_PAGE)
        self.assertIn('id="symbols_input"', SPOT_STRATEGIES_PAGE)
        self.assertIn("/api/spot_runner/status", SPOT_STRATEGIES_PAGE)

    @patch("grid_optimizer.web._validate_market_symbol")
    def test_normalize_spot_runner_payload_sets_defaults(self, _mock_validate_symbol) -> None:
        payload = _normalize_spot_runner_payload(
            {
                "symbol": "ethusdt",
                "grid_level_mode": "geometric",
                "min_price": 1200,
                "max_price": 3200,
                "n": 16,
                "total_quote_budget": 2500,
                "sleep_seconds": 8,
                "cancel_stale": True,
                "apply": False,
            }
        )

        self.assertEqual(payload["market_type"], "spot")
        self.assertEqual(payload["strategy_mode"], "spot_one_way_long")
        self.assertEqual(payload["symbol"], "ETHUSDT")
        self.assertEqual(payload["grid_level_mode"], "geometric")
        self.assertEqual(payload["n"], 16)
        self.assertEqual(payload["total_quote_budget"], 2500.0)
        self.assertEqual(payload["sleep_seconds"], 8.0)
        self.assertFalse(payload["apply"])
        self.assertIn("state_path", payload)
        self.assertIn("summary_jsonl", payload)
        self.assertIn("client_order_prefix", payload)
        self.assertEqual(payload["grid_band_ratio"], 0.045)
        self.assertEqual(payload["idle_buy1_after_seconds"], 15.0)
        self.assertEqual(payload["idle_buy1_levels"], 1)

    def test_build_spot_runner_command_uses_spot_loop_runner(self) -> None:
        config = dict(SPOT_RUNNER_DEFAULT_CONFIG)
        config.update(
            {
                "symbol": "BTCUSDT",
                "strategy_mode": "spot_volume_shift_long",
                "min_price": 50000.0,
                "max_price": 130000.0,
                "n": 20,
                "total_quote_budget": 1200.0,
                "attack_sell_loss_tolerance_ratio": 0.0006,
                "attack_sell_tight_levels": 6,
                "idle_buy1_after_seconds": 9.0,
                "idle_buy1_levels": 1,
                "client_order_prefix": "sgbtcusd",
            }
        )

        command = _build_spot_runner_command(config)

        self.assertIn("grid_optimizer.spot_loop_runner", command)
        self.assertIn("--strategy-mode", command)
        self.assertIn("--total-quote-budget", command)
        self.assertIn("--client-order-prefix", command)
        self.assertIn("--apply", command)
        self.assertIn("--cancel-stale", command)
        self.assertIn("--attack-sell-loss-tolerance-ratio", command)
        self.assertIn("--attack-sell-tight-levels", command)
        self.assertIn("--idle-buy1-after-seconds", command)
        self.assertIn("--idle-buy1-levels", command)

    @patch("grid_optimizer.web.fetch_spot_open_orders")
    @patch("grid_optimizer.web.fetch_spot_account_info")
    @patch("grid_optimizer.web.load_binance_api_credentials")
    @patch("grid_optimizer.web.fetch_spot_klines")
    @patch("grid_optimizer.web.fetch_spot_book_tickers")
    @patch("grid_optimizer.web.fetch_spot_symbol_config")
    @patch("grid_optimizer.web._tail_jsonl_dicts")
    @patch("grid_optimizer.web._read_json_dict")
    @patch("grid_optimizer.web._read_spot_runner_process_for_symbol")
    @patch("grid_optimizer.web._load_spot_runner_control_config")
    def test_build_spot_runner_snapshot_includes_balances_when_credentials_present(
        self,
        mock_load_config,
        mock_read_runner,
        mock_read_json,
        mock_tail_jsonl,
        mock_symbol_config,
        mock_book_tickers,
        mock_spot_klines,
        mock_load_credentials,
        mock_fetch_account,
        mock_fetch_open_orders,
    ) -> None:
        mock_load_config.return_value = dict(SPOT_RUNNER_DEFAULT_CONFIG)
        mock_read_runner.return_value = {"configured": False, "pid": None, "is_running": False, "args": None, "config": {}}
        mock_read_json.return_value = {
            "cycle": 2,
            "last_trade_time_ms": 123,
            "cells": {},
            "inventory_lots": [{"qty": 0.5, "cost_quote": 30000.0}],
            "metrics": {
                "gross_notional": 2000.0,
                "buy_notional": 1100.0,
                "sell_notional": 900.0,
                "commission_quote": 3.5,
                "realized_pnl": 25.0,
                "recycle_loss_abs": 1.25,
                "trade_count": 6,
                "maker_count": 6,
                "buy_count": 3,
                "sell_count": 3,
                "recent_trades": [{"id": 1, "side": "BUY"}],
            },
            "center_price": 67950.0,
            "last_mode": "attack",
            "center_shift_count": 2,
        }
        mock_tail_jsonl.return_value = [{"mid_price": 68000.5, "mode": "attack"}]
        mock_symbol_config.return_value = {
            "base_asset": "BTC",
            "quote_asset": "USDT",
            "tick_size": 0.01,
            "step_size": 0.00001,
            "min_qty": 0.00001,
            "min_notional": 5.0,
        }
        mock_book_tickers.return_value = [{"bid_price": 68000.0, "ask_price": 68001.0}]
        now = datetime(2026, 3, 24, 12, 0, tzinfo=timezone.utc)
        mock_spot_klines.return_value = [
            Candle(
                open_time=now - timedelta(hours=1),
                close_time=now,
                open=67500.0,
                high=68100.0,
                low=67400.0,
                close=68000.0,
            )
        ]
        mock_load_credentials.return_value = ("key", "secret")
        mock_fetch_account.return_value = {
            "balances": [
                {"asset": "BTC", "free": "0.123", "locked": "0.010"},
                {"asset": "USDT", "free": "456.78", "locked": "12.34"},
            ]
        }
        mock_fetch_open_orders.return_value = [{"clientOrderId": "sgbtc_entry_1"}]

        snapshot = _build_spot_runner_snapshot("BTCUSDT")

        self.assertEqual(snapshot["balances"]["base_free"], 0.123)
        self.assertEqual(snapshot["balances"]["base_locked"], 0.01)
        self.assertEqual(snapshot["balances"]["base_total"], 0.133)
        self.assertEqual(snapshot["balances"]["quote_free"], 456.78)
        self.assertEqual(snapshot["balances"]["quote_locked"], 12.34)
        self.assertAlmostEqual(snapshot["balances"]["quote_total"], 469.12, places=8)
        self.assertEqual(snapshot["open_orders"], [{"clientOrderId": "sgbtc_entry_1"}])
        self.assertTrue(any("库存口径与账户总持仓暂未完全对齐" in item for item in snapshot["warnings"]))
        self.assertEqual(snapshot["trade_summary"]["gross_notional"], 2000.0)
        self.assertEqual(snapshot["trade_summary"]["recycle_loss_abs"], 1.25)
        self.assertEqual(snapshot["state"]["inventory_qty"], 0.5)
        self.assertEqual(snapshot["risk_controls"]["center_shift_count"], 2)
        self.assertIsNotNone(snapshot["hourly_summary"])
        self.assertEqual(snapshot["hourly_summary"]["row_count"], 1)

    @patch("grid_optimizer.web.fetch_spot_open_orders")
    @patch("grid_optimizer.web.fetch_spot_account_info")
    @patch("grid_optimizer.web.load_binance_api_credentials")
    @patch("grid_optimizer.web.fetch_spot_book_tickers")
    @patch("grid_optimizer.web.fetch_spot_symbol_config")
    @patch("grid_optimizer.web._tail_jsonl_dicts")
    @patch("grid_optimizer.web._read_json_dict")
    @patch("grid_optimizer.web._read_spot_runner_process_for_symbol")
    @patch("grid_optimizer.web._load_spot_runner_control_config")
    def test_build_spot_runner_snapshot_tolerates_missing_optional_numeric_fields(
        self,
        mock_load_config,
        mock_read_runner,
        mock_read_json,
        mock_tail_jsonl,
        mock_symbol_config,
        mock_book_tickers,
        mock_load_credentials,
        mock_fetch_account,
        mock_fetch_open_orders,
    ) -> None:
        mock_load_config.return_value = dict(SPOT_RUNNER_DEFAULT_CONFIG)
        mock_read_runner.return_value = {"configured": False, "pid": None, "is_running": False, "args": None, "config": {}}
        mock_read_json.return_value = {"cycle": 0, "last_trade_time_ms": 0, "cells": {}, "metrics": {}, "inventory_lots": []}
        mock_tail_jsonl.return_value = [{}]
        mock_symbol_config.return_value = {
            "base_asset": "BTC",
            "quote_asset": "USDT",
            "tick_size": 0.01,
            "step_size": 0.00001,
            "min_qty": 0.00001,
            "min_notional": 5.0,
        }
        mock_book_tickers.return_value = [{"bid_price": 68000.0, "ask_price": 68001.0}]
        mock_load_credentials.return_value = ("key", "secret")
        mock_fetch_account.return_value = {"balances": []}
        mock_fetch_open_orders.return_value = []

        snapshot = _build_spot_runner_snapshot("BTCUSDT")

        self.assertEqual(snapshot["state"]["center_shift_count"], 0)
        self.assertEqual(snapshot["risk_controls"]["center_shift_count"], 0)
        self.assertEqual(snapshot["trade_summary"]["gross_notional"], 0.0)
        self.assertEqual(snapshot["trade_summary"]["realized_pnl"], 0.0)

    @patch("grid_optimizer.web.fetch_spot_open_orders")
    @patch("grid_optimizer.web.fetch_spot_account_info")
    @patch("grid_optimizer.web.load_binance_api_credentials")
    @patch("grid_optimizer.web.fetch_spot_book_tickers")
    @patch("grid_optimizer.web.fetch_spot_symbol_config")
    @patch("grid_optimizer.web._tail_jsonl_dicts")
    @patch("grid_optimizer.web._read_json_dict")
    @patch("grid_optimizer.web._read_spot_runner_process_for_symbol")
    @patch("grid_optimizer.web._load_spot_runner_control_config")
    def test_build_spot_runner_snapshot_tolerates_explicit_none_numeric_fields(
        self,
        mock_load_config,
        mock_read_runner,
        mock_read_json,
        mock_tail_jsonl,
        mock_symbol_config,
        mock_book_tickers,
        mock_load_credentials,
        mock_fetch_account,
        mock_fetch_open_orders,
    ) -> None:
        config = dict(SPOT_RUNNER_DEFAULT_CONFIG)
        config["inventory_soft_limit_notional"] = None
        config["inventory_hard_limit_notional"] = None
        mock_load_config.return_value = config
        mock_read_runner.return_value = {"configured": False, "pid": None, "is_running": False, "args": None, "config": {}}
        mock_read_json.return_value = {
            "cycle": 0,
            "last_trade_time_ms": 0,
            "cells": {"1": {"position_qty": None}},
            "inventory_lots": [{"qty": None, "cost_quote": None}],
            "metrics": {
                "gross_notional": None,
                "buy_notional": None,
                "sell_notional": None,
                "commission_quote": None,
                "realized_pnl": None,
                "recycle_realized_pnl": None,
                "recycle_loss_abs": None,
                "trade_count": None,
                "maker_count": None,
                "buy_count": None,
                "sell_count": None,
            },
            "center_price": None,
            "center_shift_count": None,
        }
        mock_tail_jsonl.return_value = [
            {
                "mid_price": None,
                "market_guard_return_ratio": None,
                "market_guard_amplitude_ratio": None,
                "effective_buy_levels": None,
                "effective_sell_levels": None,
                "effective_per_order_notional": None,
            }
        ]
        mock_symbol_config.return_value = {
            "base_asset": "BTC",
            "quote_asset": "USDT",
            "tick_size": 0.01,
            "step_size": 0.00001,
            "min_qty": 0.00001,
            "min_notional": 5.0,
        }
        mock_book_tickers.return_value = [{"bid_price": 68000.0, "ask_price": 68001.0}]
        mock_load_credentials.return_value = ("key", "secret")
        mock_fetch_account.return_value = {"balances": []}
        mock_fetch_open_orders.return_value = []

        snapshot = _build_spot_runner_snapshot("BTCUSDT")

        self.assertEqual(snapshot["state"]["center_price"], 0.0)
        self.assertEqual(snapshot["state"]["center_shift_count"], 0)
        self.assertEqual(snapshot["trade_summary"]["gross_notional"], 0.0)
        self.assertEqual(snapshot["trade_summary"]["trade_count"], 0)
        self.assertEqual(snapshot["risk_controls"]["inventory_soft_limit_notional"], 0.0)
        self.assertEqual(snapshot["risk_controls"]["inventory_hard_limit_notional"], 0.0)
        self.assertEqual(snapshot["risk_controls"]["effective_buy_levels"], 0)
        self.assertEqual(snapshot["risk_controls"]["effective_per_order_notional"], 0.0)

    @patch("grid_optimizer.web.load_binance_api_credentials")
    @patch("grid_optimizer.web.fetch_spot_klines")
    @patch("grid_optimizer.web.fetch_spot_book_tickers")
    @patch("grid_optimizer.web.fetch_spot_symbol_config")
    @patch("grid_optimizer.web._tail_jsonl_dicts")
    @patch("grid_optimizer.web._read_json_dict")
    @patch("grid_optimizer.web._read_spot_runner_process_for_symbol")
    @patch("grid_optimizer.web._load_spot_runner_control_config")
    def test_build_spot_runner_snapshot_includes_hourly_summary_from_recent_trades(
        self,
        mock_load_config,
        mock_read_runner,
        mock_read_json,
        mock_tail_jsonl,
        mock_symbol_config,
        mock_book_tickers,
        mock_spot_klines,
        mock_load_credentials,
    ) -> None:
        mock_load_config.return_value = dict(SPOT_RUNNER_DEFAULT_CONFIG)
        mock_read_runner.return_value = {"configured": False, "pid": None, "is_running": False, "args": None, "config": {}}
        trade_time_ms = int(datetime(2026, 3, 24, 10, 25, tzinfo=timezone.utc).timestamp() * 1000)
        mock_read_json.return_value = {
            "cycle": 1,
            "last_trade_time_ms": trade_time_ms,
            "cells": {},
            "inventory_lots": [],
            "metrics": {
                "gross_notional": 20.5,
                "buy_notional": 12.3,
                "sell_notional": 8.2,
                "commission_quote": 0.03,
                "realized_pnl": 0.22,
                "trade_count": 2,
                "maker_count": 2,
                "buy_count": 1,
                "sell_count": 1,
                "recent_trades": [
                    {
                        "time": trade_time_ms,
                        "side": "BUY",
                        "price": 0.025,
                        "qty": 400.0,
                        "notional": 10.0,
                        "commission_quote": 0.01,
                        "realized_pnl": -0.01,
                    },
                    {
                        "time": trade_time_ms + 20_000,
                        "side": "SELL",
                        "price": 0.02625,
                        "qty": 400.0,
                        "notional": 10.5,
                        "commission_quote": 0.02,
                        "realized_pnl": 0.23,
                    },
                ],
            },
        }
        mock_tail_jsonl.return_value = [{"mid_price": 0.0261, "mode": "attack"}]
        mock_symbol_config.return_value = {
            "base_asset": "SAHARA",
            "quote_asset": "USDT",
            "tick_size": 0.00001,
            "step_size": 1.0,
            "min_qty": 1.0,
            "min_notional": 5.0,
        }
        mock_book_tickers.return_value = [{"bid_price": 0.02608, "ask_price": 0.02612}]
        mock_spot_klines.return_value = [
            Candle(
                open_time=datetime(2026, 3, 24, 10, 0, tzinfo=timezone.utc),
                close_time=datetime(2026, 3, 24, 11, 0, tzinfo=timezone.utc),
                open=0.0250,
                high=0.0264,
                low=0.0248,
                close=0.0261,
            )
        ]
        mock_load_credentials.return_value = None

        snapshot = _build_spot_runner_snapshot("SAHARAUSDT")

        hourly = snapshot["hourly_summary"]
        self.assertIsNotNone(hourly)
        self.assertEqual(hourly["row_count"], 1)
        self.assertEqual(hourly["rows"][0]["trade_count"], 2)
        self.assertAlmostEqual(hourly["rows"][0]["gross_notional"], 20.5, places=8)
        self.assertAlmostEqual(hourly["rows"][0]["commission"], 0.03, places=8)
        self.assertAlmostEqual(hourly["rows"][0]["realized_pnl"], 0.22, places=8)
        self.assertAlmostEqual(hourly["rows"][0]["net_after_fees_and_funding"], 0.22, places=8)
        self.assertEqual(hourly["source_label"], "小时成交基于当前策略成交记录")

    @patch("grid_optimizer.web.load_binance_api_credentials")
    @patch("grid_optimizer.web.fetch_spot_klines")
    @patch("grid_optimizer.web.fetch_spot_book_tickers")
    @patch("grid_optimizer.web.fetch_spot_symbol_config")
    @patch("grid_optimizer.web._tail_jsonl_dicts")
    @patch("grid_optimizer.web._read_json_dict")
    @patch("grid_optimizer.web._read_spot_runner_process_for_symbol")
    @patch("grid_optimizer.web._load_spot_runner_control_config")
    def test_build_spot_runner_snapshot_prefers_persisted_hourly_buckets(
        self,
        mock_load_config,
        mock_read_runner,
        mock_read_json,
        mock_tail_jsonl,
        mock_symbol_config,
        mock_book_tickers,
        mock_spot_klines,
        mock_load_credentials,
    ) -> None:
        mock_load_config.return_value = dict(SPOT_RUNNER_DEFAULT_CONFIG)
        mock_read_runner.return_value = {"configured": False, "pid": None, "is_running": False, "args": None, "config": {}}
        hour_start = datetime(2026, 3, 24, 9, 0, tzinfo=timezone.utc)
        hour_start_ms = int(hour_start.timestamp() * 1000)
        mock_read_json.return_value = {
            "cycle": 1,
            "last_trade_time_ms": hour_start_ms + 25_000,
            "cells": {},
            "inventory_lots": [],
            "metrics": {
                "gross_notional": 20.5,
                "buy_notional": 12.3,
                "sell_notional": 8.2,
                "commission_quote": 0.03,
                "realized_pnl": 0.22,
                "trade_count": 2,
                "maker_count": 2,
                "buy_count": 1,
                "sell_count": 1,
                "recent_trades": [],
                "hourly_buckets": {
                    str(hour_start_ms): {
                        "hour_start_ms": hour_start_ms,
                        "gross_notional": 20.5,
                        "buy_notional": 12.3,
                        "sell_notional": 8.2,
                        "trade_count": 2,
                        "buy_count": 1,
                        "sell_count": 1,
                        "realized_pnl": 0.22,
                        "commission": 0.03,
                    }
                },
            },
        }
        mock_tail_jsonl.return_value = [{"mid_price": 0.0261, "mode": "attack"}]
        mock_symbol_config.return_value = {
            "base_asset": "SAHARA",
            "quote_asset": "USDT",
            "tick_size": 0.00001,
            "step_size": 1.0,
            "min_qty": 1.0,
            "min_notional": 5.0,
        }
        mock_book_tickers.return_value = [{"bid_price": 0.02608, "ask_price": 0.02612}]
        mock_spot_klines.return_value = [
            Candle(
                open_time=hour_start,
                close_time=hour_start + timedelta(hours=1),
                open=0.0259,
                high=0.0264,
                low=0.0255,
                close=0.0261,
            )
        ]
        mock_load_credentials.return_value = None

        snapshot = _build_spot_runner_snapshot("SAHARAUSDT")

        hourly = snapshot["hourly_summary"]
        self.assertIsNotNone(hourly)
        self.assertEqual(hourly["row_count"], 1)
        self.assertEqual(hourly["rows"][0]["trade_count"], 2)
        self.assertAlmostEqual(hourly["rows"][0]["gross_notional"], 20.5, places=8)
        self.assertAlmostEqual(hourly["rows"][0]["commission"], 0.03, places=8)
        self.assertAlmostEqual(hourly["rows"][0]["realized_pnl"], 0.22, places=8)
        self.assertAlmostEqual(hourly["rows"][0]["net_after_fees_and_funding"], 0.22, places=8)
        self.assertEqual(hourly["source_label"], "小时成交基于持久化策略状态")

    @patch("grid_optimizer.web.time.sleep")
    @patch("grid_optimizer.web.subprocess.Popen")
    @patch("grid_optimizer.web._build_spot_runner_command")
    @patch("grid_optimizer.web._save_spot_runner_control_config")
    @patch("grid_optimizer.web._cancel_spot_strategy_orders")
    @patch("grid_optimizer.web._read_spot_runner_process_for_symbol")
    def test_start_spot_runner_process_accepts_truthy_reset_state(
        self,
        mock_read_runner,
        mock_cancel_orders,
        _mock_save_config,
        mock_build_command,
        mock_popen,
        _mock_sleep,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            state_path = tmp_path / "spot_state.json"
            log_path = tmp_path / "spot_runner.log"
            pid_path = tmp_path / "spot_runner.pid"
            state_path.write_text("{}", encoding="utf-8")

            config = dict(SPOT_RUNNER_DEFAULT_CONFIG)
            config.update(
                {
                    "symbol": "SAHARAUSDT",
                    "reset_state": "true",
                    "state_path": str(state_path),
                    "summary_jsonl": str(tmp_path / "spot_events.jsonl"),
                }
            )
            mock_read_runner.side_effect = [
                {"configured": False, "pid": None, "is_running": False, "args": None, "config": {}},
                {"configured": True, "pid": 4321, "is_running": True, "args": None, "config": config},
            ]
            mock_cancel_orders.return_value = {"canceled": 2}
            mock_build_command.return_value = ["python3", "-m", "grid_optimizer.spot_loop_runner"]
            mock_popen.return_value = MagicMock(pid=4321)

            with patch("grid_optimizer.web._spot_runner_log_path", return_value=log_path), patch(
                "grid_optimizer.web._spot_runner_pid_path", return_value=pid_path
            ):
                result = _start_spot_runner_process(config)
                self.assertTrue(result["started"])
                self.assertEqual(result["cleanup"]["canceled"], 2)
                self.assertFalse(state_path.exists())
                self.assertEqual(pid_path.read_text(encoding="utf-8"), "4321")


if __name__ == "__main__":
    unittest.main()
