from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from grid_optimizer.web import (
    SPOT_RUNNER_DEFAULT_CONFIG,
    SPOT_RUNNER_PAGE,
    SPOT_STRATEGIES_PAGE,
    _build_spot_runner_snapshot,
    _build_spot_runner_command,
    _normalize_spot_runner_payload,
    _save_spot_runner_config_without_start,
    _start_spot_runner_process,
)


class SpotRunnerTests(unittest.TestCase):
    def test_spot_runner_page_contains_controls(self) -> None:
        self.assertIn("现货比赛执行台", SPOT_RUNNER_PAGE)
        self.assertIn('id="start_btn"', SPOT_RUNNER_PAGE)
        self.assertIn('id="stop_btn"', SPOT_RUNNER_PAGE)
        self.assertIn('id="save_btn"', SPOT_RUNNER_PAGE)
        self.assertIn('id="strategy_mode"', SPOT_RUNNER_PAGE)
        self.assertIn('controlRunner("save")', SPOT_RUNNER_PAGE)
        self.assertIn("Spot V1 单向做多静态网格", SPOT_RUNNER_PAGE)
        self.assertIn("现货竞赛库存网格", SPOT_RUNNER_PAGE)
        self.assertIn('id="competition_fields"', SPOT_RUNNER_PAGE)
        self.assertIn("/spot_strategies", SPOT_RUNNER_PAGE)
        self.assertIn('placeholder="输入币种，如 XAUTUSDT"', SPOT_RUNNER_PAGE)
        self.assertIn('["XAUTUSDT", "BARDUSDT", "SAHARAUSDT", "NIGHTUSDT", "CFGUSDT"]', SPOT_RUNNER_PAGE)

    def test_spot_strategies_page_contains_overview(self) -> None:
        self.assertIn("现货策略总览", SPOT_STRATEGIES_PAGE)
        self.assertIn('id="symbols_input"', SPOT_STRATEGIES_PAGE)
        self.assertIn("/api/spot_runner/status", SPOT_STRATEGIES_PAGE)
        self.assertIn('value="XAUTUSDT,SAHARAUSDT,NIGHTUSDT,CFGUSDT"', SPOT_STRATEGIES_PAGE)

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
        self.assertIsNone(payload["run_start_time"])
        self.assertIsNone(payload["run_end_time"])
        self.assertIsNone(payload["rolling_hourly_loss_limit"])
        self.assertIsNone(payload["max_cumulative_notional"])

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

    def test_build_spot_runner_command_includes_runtime_guard_arguments(self) -> None:
        config = dict(SPOT_RUNNER_DEFAULT_CONFIG)
        config.update(
            {
                "symbol": "BTCUSDT",
                "run_start_time": "2026-03-31T01:00:00+00:00",
                "run_end_time": "2026-03-31T03:00:00+00:00",
                "rolling_hourly_loss_limit": 250.0,
                "max_cumulative_notional": 100000.0,
            }
        )

        command = _build_spot_runner_command(config)

        self.assertIn("--run-start-time", command)
        self.assertIn("2026-03-31T01:00:00+00:00", command)
        self.assertIn("--run-end-time", command)
        self.assertIn("--rolling-hourly-loss-limit", command)
        self.assertIn("--max-cumulative-notional", command)

    @patch("grid_optimizer.web._validate_market_symbol")
    def test_normalize_spot_runner_payload_accepts_competition_inventory_grid(self, _mock_validate_symbol) -> None:
        payload = _normalize_spot_runner_payload(
            {
                "symbol": "btcusdt",
                "strategy_mode": "spot_competition_inventory_grid",
                "total_quote_budget": 1800,
                "step_price": 12.5,
                "per_order_notional": 45,
                "first_order_multiplier": 3,
                "threshold_position_notional": 320,
                "max_order_position_notional": 420,
                "max_position_notional": 520,
            }
        )

        self.assertEqual(payload["strategy_mode"], "spot_competition_inventory_grid")
        self.assertEqual(payload["symbol"], "BTCUSDT")
        self.assertEqual(payload["step_price"], 12.5)
        self.assertEqual(payload["per_order_notional"], 45.0)
        self.assertEqual(payload["first_order_multiplier"], 3.0)
        self.assertEqual(payload["threshold_position_notional"], 320.0)
        self.assertEqual(payload["max_order_position_notional"], 420.0)
        self.assertEqual(payload["max_position_notional"], 520.0)

    @patch("grid_optimizer.web._read_spot_runner_process_for_symbol")
    @patch("grid_optimizer.web._save_spot_runner_control_config")
    @patch("grid_optimizer.web._validate_market_symbol")
    def test_save_spot_runner_config_without_start_persists_config(
        self,
        _mock_validate_symbol,
        mock_save_config,
        mock_read_runner,
    ) -> None:
        mock_read_runner.return_value = {
            "configured": True,
            "pid": None,
            "is_running": False,
            "args": None,
            "config": {"symbol": "VANAUSDT"},
        }

        result = _save_spot_runner_config_without_start(
            {
                "symbol": "vanausdt",
                "strategy_mode": "spot_competition_inventory_grid",
                "total_quote_budget": 300,
                "step_price": 0.004,
                "per_order_notional": 12,
                "first_order_multiplier": 1.5,
                "threshold_position_notional": 80,
                "max_order_position_notional": 120,
                "max_position_notional": 180,
                "sleep_seconds": 12,
                "rolling_hourly_loss_limit": 20,
                "max_cumulative_notional": 30000,
            }
        )

        self.assertTrue(result["saved"])
        self.assertEqual(result["symbol"], "VANAUSDT")
        self.assertFalse(result["runner"]["is_running"])
        saved_config = mock_save_config.call_args.args[0]
        self.assertEqual(saved_config["symbol"], "VANAUSDT")
        self.assertEqual(saved_config["strategy_mode"], "spot_competition_inventory_grid")
        self.assertEqual(saved_config["step_price"], 0.004)
        self.assertEqual(saved_config["per_order_notional"], 12.0)
        self.assertEqual(saved_config["max_position_notional"], 180.0)
        self.assertEqual(saved_config["state_path"], "output/vanausdt_spot_state.json")
        mock_save_config.assert_called_once()
        self.assertEqual(mock_save_config.call_args.kwargs["symbol"], "VANAUSDT")

    def test_build_spot_runner_command_includes_competition_inventory_grid_arguments(self) -> None:
        config = dict(SPOT_RUNNER_DEFAULT_CONFIG)
        config.update(
            {
                "symbol": "BTCUSDT",
                "strategy_mode": "spot_competition_inventory_grid",
                "total_quote_budget": 1800.0,
                "step_price": 12.5,
                "per_order_notional": 45.0,
                "first_order_multiplier": 3.0,
                "threshold_position_notional": 320.0,
                "max_order_position_notional": 420.0,
                "max_position_notional": 520.0,
            }
        )

        command = _build_spot_runner_command(config)

        self.assertIn("--step-price", command)
        self.assertIn("--per-order-notional", command)
        self.assertIn("--first-order-multiplier", command)
        self.assertIn("--threshold-position-notional", command)
        self.assertIn("--max-order-position-notional", command)
        self.assertIn("--max-position-notional", command)

    @patch("grid_optimizer.web.fetch_spot_open_orders")
    @patch("grid_optimizer.web.fetch_spot_account_info")
    @patch("grid_optimizer.web.load_binance_api_credentials")
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
        mock_load_credentials,
        mock_fetch_account,
        mock_fetch_open_orders,
    ) -> None:
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
        config = dict(SPOT_RUNNER_DEFAULT_CONFIG)
        config.update(
            {
                "run_start_time": "2026-03-31T01:00:00+00:00",
                "run_end_time": "2026-03-31T03:00:00+00:00",
                "rolling_hourly_loss_limit": 150.0,
                "max_cumulative_notional": 100000.0,
            }
        )
        mock_load_config.return_value = config
        mock_tail_jsonl.return_value = [
            {
                "mid_price": 68000.5,
                "mode": "attack",
                "runtime_status": "stopped",
                "stop_triggered": True,
                "stop_reason": "max_cumulative_notional_hit",
                "rolling_hourly_loss": 123.4,
                "cumulative_gross_notional": 99999.0,
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
        self.assertEqual(snapshot["balances"]["quote_free"], 456.78)
        self.assertEqual(snapshot["balances"]["quote_locked"], 12.34)
        self.assertEqual(snapshot["open_orders"], [{"clientOrderId": "sgbtc_entry_1"}])
        self.assertEqual(snapshot["warnings"], [])
        self.assertEqual(snapshot["trade_summary"]["gross_notional"], 2000.0)
        self.assertEqual(snapshot["trade_summary"]["recycle_loss_abs"], 1.25)
        self.assertEqual(snapshot["state"]["inventory_qty"], 0.5)
        self.assertEqual(snapshot["risk_controls"]["center_shift_count"], 2)
        self.assertEqual(snapshot["risk_controls"]["runtime_status"], "stopped")
        self.assertTrue(snapshot["risk_controls"]["stop_triggered"])
        self.assertEqual(snapshot["risk_controls"]["stop_reason"], "max_cumulative_notional_hit")
        self.assertAlmostEqual(snapshot["risk_controls"]["rolling_hourly_loss"], 123.4)
        self.assertEqual(snapshot["risk_controls"]["run_start_time"], "2026-03-31T01:00:00+00:00")

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
