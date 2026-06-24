from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import grid_optimizer.web as web
from grid_optimizer.spot_loop_runner import (
    _build_parser as _build_spot_loop_parser,
    _build_spot_competition_inventory_grid_orders,
)
from grid_optimizer.web import (
    SPOT_RUNNER_DEFAULT_CONFIG,
    SPOT_MONITOR_PAGE,
    SPOT_RUNNER_PAGE,
    SPOT_STRATEGIES_PAGE,
    _build_spot_monitor_payload,
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
        self.assertIn("现货竞赛合成中性网格", SPOT_RUNNER_PAGE)
        self.assertIn('id="competition_fields"', SPOT_RUNNER_PAGE)
        self.assertIn("/spot_strategies", SPOT_RUNNER_PAGE)
        self.assertIn('placeholder="输入币种，如 XAUTUSDT"', SPOT_RUNNER_PAGE)
        self.assertIn('["XAUTUSDT", "BARDUSDT", "SAHARAUSDT", "NIGHTUSDT", "CFGUSDT"]', SPOT_RUNNER_PAGE)

    def test_spot_strategies_page_contains_overview(self) -> None:
        self.assertIn("现货策略总览", SPOT_STRATEGIES_PAGE)
        self.assertIn('id="symbols_input"', SPOT_STRATEGIES_PAGE)
        self.assertIn("/api/spot_runner/status", SPOT_STRATEGIES_PAGE)
        self.assertIn('value="XAUTUSDT,SAHARAUSDT,NIGHTUSDT,CFGUSDT"', SPOT_STRATEGIES_PAGE)

    def test_spot_monitor_page_contains_hour_day_controls(self) -> None:
        self.assertIn("现货成交监控", SPOT_MONITOR_PAGE)
        self.assertIn('id="granularity"', SPOT_MONITOR_PAGE)
        self.assertIn('value="hour"', SPOT_MONITOR_PAGE)
        self.assertIn('value="day"', SPOT_MONITOR_PAGE)
        self.assertIn('placeholder="留空自动读取本机现货 runner"', SPOT_MONITOR_PAGE)
        self.assertIn("/api/spot_monitor", SPOT_MONITOR_PAGE)

    @patch("grid_optimizer.web._read_spot_runner_process_for_symbol")
    def test_build_spot_monitor_payload_groups_hourly_volume_and_equity_loss(self, mock_runner) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            output_dir = root / "output"
            output_dir.mkdir()
            state_path = output_dir / "tonusdt_spot_state.json"
            events_path = output_dir / "tonusdt_spot_events.jsonl"
            control_path = output_dir / "tonusdt_spot_loop_runner_control.json"
            control_path.write_text(
                json.dumps(
                    {
                        "symbol": "TONUSDT",
                        "state_path": str(state_path),
                        "summary_jsonl": str(events_path),
                    }
                ),
                encoding="utf-8",
            )
            state_path.write_text(
                json.dumps(
                    {
                        "metrics": {
                            "recent_trades": [
                                {
                                    "id": "1",
                                    "time": "2026-02-02T00:05:00+00:00",
                                    "side": "BUY",
                                    "price": 2.0,
                                    "qty": 100.0,
                                    "commission_quote": 0.1,
                                    "realized_pnl": -0.1,
                                },
                                {
                                    "id": "2",
                                    "time": "2026-02-02T00:35:00+00:00",
                                    "side": "SELL",
                                    "price": 2.1,
                                    "qty": 100.0,
                                    "commission_quote": 0.1,
                                    "realized_pnl": 9.9,
                                },
                            ]
                        }
                    }
                ),
                encoding="utf-8",
            )
            events_path.write_text(
                "\n".join(
                    [
                        json.dumps({"ts": "2026-02-02T00:00:00+00:00", "equity_usdt": 1000.0}),
                        json.dumps({"ts": "2026-02-02T00:59:00+00:00", "equity_usdt": 997.0}),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            mock_runner.return_value = {"configured": True, "is_running": True, "pid": 123, "config": {}}

            previous_cwd = os.getcwd()
            try:
                os.chdir(root)
                payload = _build_spot_monitor_payload(symbols=["TONUSDT"], granularity="hour", limit=12)
            finally:
                os.chdir(previous_cwd)

        self.assertTrue(payload["ok"])
        self.assertEqual(payload["granularity"], "hour")
        self.assertEqual(payload["summary"]["symbol_count"], 1)
        self.assertAlmostEqual(payload["summary"]["gross_notional"], 410.0)
        self.assertAlmostEqual(payload["summary"]["equity_loss_usdt"], 3.0)
        symbol_payload = payload["symbols"][0]
        self.assertEqual(symbol_payload["symbol"], "TONUSDT")
        row = symbol_payload["rows"][0]
        self.assertEqual(row["bucket_start"], "2026-02-02T00:00:00+00:00")
        self.assertAlmostEqual(row["gross_notional"], 410.0)
        self.assertAlmostEqual(row["buy_notional"], 200.0)
        self.assertAlmostEqual(row["sell_notional"], 210.0)
        self.assertAlmostEqual(row["equity_start_usdt"], 1000.0)
        self.assertAlmostEqual(row["equity_end_usdt"], 997.0)
        self.assertAlmostEqual(row["equity_delta_usdt"], -3.0)
        self.assertAlmostEqual(row["equity_loss_usdt"], 3.0)

    @patch("grid_optimizer.web._read_spot_runner_process_for_symbol")
    def test_build_spot_monitor_payload_groups_daily_rows(self, mock_runner) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            output_dir = root / "output"
            output_dir.mkdir()
            state_path = output_dir / "tonusdt_spot_state.json"
            events_path = output_dir / "tonusdt_spot_events.jsonl"
            (output_dir / "tonusdt_spot_loop_runner_control.json").write_text(
                json.dumps({"symbol": "TONUSDT", "state_path": str(state_path), "summary_jsonl": str(events_path)}),
                encoding="utf-8",
            )
            state_path.write_text(
                json.dumps(
                    {
                        "metrics": {
                            "recent_trades": [
                                {"id": "1", "time": "2026-02-02T00:05:00+00:00", "side": "BUY", "price": 2.0, "qty": 100.0},
                                {"id": "2", "time": "2026-02-03T00:05:00+00:00", "side": "SELL", "price": 2.1, "qty": 100.0},
                            ]
                        }
                    }
                ),
                encoding="utf-8",
            )
            events_path.write_text(
                "\n".join(
                    [
                        json.dumps({"ts": "2026-02-02T00:00:00+00:00", "equity_usdt": 1000.0}),
                        json.dumps({"ts": "2026-02-02T23:59:00+00:00", "equity_usdt": 998.0}),
                        json.dumps({"ts": "2026-02-03T00:00:00+00:00", "equity_usdt": 998.0}),
                        json.dumps({"ts": "2026-02-03T23:59:00+00:00", "equity_usdt": 1001.0}),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            mock_runner.return_value = {"configured": True, "is_running": False, "pid": None, "config": {}}

            previous_cwd = os.getcwd()
            try:
                os.chdir(root)
                payload = _build_spot_monitor_payload(symbols=["TONUSDT"], granularity="day", limit=12)
            finally:
                os.chdir(previous_cwd)

        rows = payload["symbols"][0]["rows"]
        self.assertEqual([row["bucket_start"] for row in rows], ["2026-02-03T00:00:00+00:00", "2026-02-02T00:00:00+00:00"])
        self.assertAlmostEqual(rows[0]["gross_notional"], 210.0)
        self.assertAlmostEqual(rows[0]["equity_delta_usdt"], 3.0)
        self.assertAlmostEqual(rows[1]["gross_notional"], 200.0)
        self.assertAlmostEqual(rows[1]["equity_loss_usdt"], 2.0)

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
                "rolling_hourly_loss_per_10k_limit": 40.0,
                "rolling_hourly_loss_per_10k_min_notional": 10000.0,
                "max_cumulative_notional": 100000.0,
            }
        )

        command = _build_spot_runner_command(config)

        self.assertIn("--run-start-time", command)
        self.assertIn("2026-03-31T01:00:00+00:00", command)
        self.assertIn("--run-end-time", command)
        self.assertIn("--rolling-hourly-loss-limit", command)
        self.assertIn("--rolling-hourly-loss-per-10k-limit", command)
        self.assertIn("--rolling-hourly-loss-per-10k-min-notional", command)
        self.assertIn("--max-cumulative-notional", command)
        args = _build_spot_loop_parser().parse_args(command[3:])
        self.assertEqual(args.rolling_hourly_loss_per_10k_limit, 40.0)
        self.assertEqual(args.rolling_hourly_loss_per_10k_min_notional, 10000.0)

    def test_build_spot_runner_command_includes_spot_app_loss_guard_arguments(self) -> None:
        config = dict(SPOT_RUNNER_DEFAULT_CONFIG)
        config.update(
            {
                "symbol": "MEGAUSDT",
                "strategy_mode": "spot_competition_synthetic_neutral_grid",
                "spot_app_loss_guard_enabled": True,
                "spot_app_loss_min_notional": 5000.0,
                "spot_app_loss_per_10k_soft": 1.0,
                "spot_app_loss_per_10k_hard": 1.5,
                "spot_app_loss_recovery_reduce_only_enabled": True,
            }
        )

        command = _build_spot_runner_command(config)

        self.assertIn("--spot-app-loss-guard-enabled", command)
        self.assertIn("--spot-app-loss-min-notional", command)
        self.assertIn("--spot-app-loss-per-10k-soft", command)
        self.assertIn("--spot-app-loss-per-10k-hard", command)
        self.assertIn("--spot-app-loss-recovery-reduce-only-enabled", command)
        args = _build_spot_loop_parser().parse_args(command[3:])
        self.assertTrue(args.spot_app_loss_guard_enabled)
        self.assertTrue(args.spot_app_loss_recovery_reduce_only_enabled)
        self.assertEqual(args.spot_app_loss_min_notional, 5000.0)
        self.assertEqual(args.spot_app_loss_per_10k_soft, 1.0)
        self.assertEqual(args.spot_app_loss_per_10k_hard, 1.5)

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

    @patch("grid_optimizer.web._validate_market_symbol")
    def test_normalize_spot_runner_payload_accepts_synthetic_neutral_grid(self, _mock_validate_symbol) -> None:
        payload = _normalize_spot_runner_payload(
            {
                "symbol": "spkusdt",
                "strategy_mode": "spot_competition_synthetic_neutral_grid",
                "total_quote_budget": 1500,
                "neutral_base_qty": 30000,
                "step_price": 0.00010,
                "per_order_notional": 18,
                "first_order_multiplier": 1,
                "threshold_position_notional": 520,
                "max_order_position_notional": 700,
                "max_position_notional": 900,
                "max_short_position_notional": 650,
                "elastic_volume_enabled": True,
                "spot_slow_trend_step_enabled": True,
                "spot_slow_trend_step_5m_return_ratio": 0.0045,
                "spot_slow_trend_step_15m_return_ratio": 0.008,
                "spot_slow_trend_step_5m_amplitude_ratio": 0.005,
                "spot_slow_trend_step_15m_amplitude_ratio": 0.01,
                "spot_slow_trend_step_scale": 1.8,
            }
        )

        self.assertEqual(payload["strategy_mode"], "spot_competition_synthetic_neutral_grid")
        self.assertEqual(payload["symbol"], "SPKUSDT")
        self.assertEqual(payload["neutral_base_qty"], 30000.0)
        self.assertEqual(payload["max_short_position_notional"], 650.0)
        self.assertTrue(payload["elastic_volume_enabled"])
        self.assertTrue(payload["spot_slow_trend_step_enabled"])
        self.assertEqual(payload["spot_slow_trend_step_5m_return_ratio"], 0.0045)
        self.assertEqual(payload["spot_slow_trend_step_15m_return_ratio"], 0.008)
        self.assertEqual(payload["spot_slow_trend_step_5m_amplitude_ratio"], 0.005)
        self.assertEqual(payload["spot_slow_trend_step_15m_amplitude_ratio"], 0.01)
        self.assertEqual(payload["spot_slow_trend_step_scale"], 1.8)

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

    def test_save_spot_runner_control_config_preserves_prestart_gate_fields(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            output_dir = root / "output"
            output_dir.mkdir()
            control_path = output_dir / "xplusdt_spot_loop_runner_control.json"
            control_path.write_text(
                json.dumps(
                    {
                        "symbol": "XPLUSDT",
                        "spot_app_loss_prestart_gate_enabled": True,
                        "spot_app_loss_prestart_gate_start_time": "2026-06-24T19:57:00+08:00",
                        "spot_app_loss_prestart_gate_max_loss_per_10k": 1.0,
                    }
                ),
                encoding="utf-8",
            )

            previous_cwd = os.getcwd()
            try:
                os.chdir(root)
                web._save_spot_runner_control_config({"symbol": "XPLUSDT", "market_type": "spot"}, symbol="XPLUSDT")
            finally:
                os.chdir(previous_cwd)

            saved = json.loads(control_path.read_text(encoding="utf-8"))

        self.assertTrue(saved["spot_app_loss_prestart_gate_enabled"])
        self.assertEqual(saved["spot_app_loss_prestart_gate_start_time"], "2026-06-24T19:57:00+08:00")
        self.assertEqual(saved["spot_app_loss_prestart_gate_max_loss_per_10k"], 1.0)

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
                "spot_taker_exit_enabled": True,
                "spot_taker_exit_fee_ratio": 0.001,
                "spot_taker_exit_min_profit_ratio": 0.0002,
            }
        )

        command = _build_spot_runner_command(config)

        self.assertIn("--step-price", command)
        self.assertIn("--per-order-notional", command)
        self.assertIn("--first-order-multiplier", command)
        self.assertIn("--threshold-position-notional", command)
        self.assertIn("--max-order-position-notional", command)
        self.assertIn("--max-position-notional", command)
        self.assertIn("--spot-taker-exit-enabled", command)
        self.assertIn("--spot-taker-exit-fee-ratio", command)
        self.assertIn("--spot-taker-exit-min-profit-ratio", command)

    def test_build_spot_runner_command_includes_synthetic_neutral_arguments(self) -> None:
        config = dict(SPOT_RUNNER_DEFAULT_CONFIG)
        config.update(
            {
                "symbol": "SPKUSDT",
                "strategy_mode": "spot_competition_synthetic_neutral_grid",
                "total_quote_budget": 1500.0,
                "neutral_base_qty": 30000.0,
                "step_price": 0.00010,
                "per_order_notional": 18.0,
                "first_order_multiplier": 1.0,
                "threshold_position_notional": 520.0,
                "max_order_position_notional": 700.0,
                "max_position_notional": 900.0,
                "max_short_position_notional": 650.0,
                "elastic_volume_enabled": True,
                "spot_slow_trend_step_enabled": True,
                "spot_slow_trend_step_5m_return_ratio": 0.0045,
                "spot_slow_trend_step_15m_return_ratio": 0.008,
                "spot_slow_trend_step_5m_amplitude_ratio": 0.005,
                "spot_slow_trend_step_15m_amplitude_ratio": 0.01,
                "spot_slow_trend_step_scale": 2.0,
            }
        )

        command = _build_spot_runner_command(config)

        self.assertIn("spot_competition_synthetic_neutral_grid", command)
        self.assertIn("--neutral-base-qty", command)
        self.assertIn("30000.0", command)
        self.assertIn("--max-short-position-notional", command)
        self.assertIn("650.0", command)
        self.assertIn("--elastic-volume-enabled", command)
        self.assertIn("--spot-slow-trend-step-enabled", command)
        self.assertIn("--spot-slow-trend-step-5m-return-ratio", command)
        self.assertIn("0.0045", command)
        self.assertIn("--spot-slow-trend-step-15m-return-ratio", command)
        self.assertIn("--spot-slow-trend-step-5m-amplitude-ratio", command)
        self.assertIn("--spot-slow-trend-step-15m-amplitude-ratio", command)
        self.assertIn("--spot-slow-trend-step-scale", command)
        self.assertIn("2.0", command)

    def test_build_spot_runner_command_includes_synthetic_freeze_arguments(self) -> None:
        config = dict(SPOT_RUNNER_DEFAULT_CONFIG)
        config.update(
            {
                "symbol": "OPGUSDT",
                "strategy_mode": "spot_competition_synthetic_neutral_grid",
                "synthetic_freeze_enabled": True,
                "synthetic_freeze_loss_ratio": 0.006,
                "synthetic_freeze_min_notional": 20.0,
                "synthetic_freeze_max_side_notional": 1000.0,
                "synthetic_freeze_release_profit_ratio": 0.0015,
                "synthetic_freeze_pair_release_enabled": True,
            }
        )

        command = _build_spot_runner_command(config)

        self.assertIn("--synthetic-freeze-enabled", command)
        self.assertIn("--synthetic-freeze-loss-ratio", command)
        self.assertIn("0.006", command)
        self.assertIn("--synthetic-freeze-min-notional", command)
        self.assertIn("20.0", command)
        self.assertIn("--synthetic-freeze-max-side-notional", command)
        self.assertIn("1000.0", command)
        self.assertIn("--synthetic-freeze-release-profit-ratio", command)
        self.assertIn("0.0015", command)
        self.assertIn("--synthetic-freeze-pair-release-enabled", command)

    def test_build_spot_runner_command_includes_spot_freeze_arguments(self) -> None:
        config = dict(SPOT_RUNNER_DEFAULT_CONFIG)
        config.update(
            {
                "symbol": "MEGAUSDT",
                "strategy_mode": "spot_competition_synthetic_neutral_grid",
                "spot_freeze_enabled": True,
                "spot_freeze_dry_run": True,
                "spot_freeze_market_execution_enabled": True,
                "spot_freeze_base_hedge_qty": 100.0,
                "spot_freeze_tolerance_qty": 0.01,
                "spot_freeze_deviation_notional": 50.0,
                "spot_freeze_min_loss_ratio": 0.01,
                "spot_freeze_max_per_cycle_notional": 100.0,
                "spot_freeze_total_cap_notional": 500.0,
                "spot_freeze_pair_release_enabled": True,
                "spot_freeze_profit_release_enabled": True,
                "spot_freeze_release_profit_ratio": 0.02,
            }
        )

        command = _build_spot_runner_command(config)

        self.assertIn("--spot-freeze-enabled", command)
        self.assertIn("--spot-freeze-dry-run", command)
        self.assertIn("--spot-freeze-market-execution-enabled", command)
        self.assertIn("--spot-freeze-base-hedge-qty", command)
        self.assertIn("100.0", command)
        self.assertIn("--spot-freeze-tolerance-qty", command)
        self.assertIn("0.01", command)
        self.assertIn("--spot-freeze-deviation-notional", command)
        self.assertIn("50.0", command)
        self.assertIn("--spot-freeze-min-loss-ratio", command)
        self.assertIn("--spot-freeze-max-per-cycle-notional", command)
        self.assertIn("--spot-freeze-total-cap-notional", command)
        self.assertIn("500.0", command)
        self.assertIn("--spot-freeze-pair-release-enabled", command)
        self.assertIn("--spot-freeze-profit-release-enabled", command)
        self.assertIn("--spot-freeze-release-profit-ratio", command)
        self.assertIn("0.02", command)

    @patch("grid_optimizer.web._validate_market_symbol")
    def test_normalize_spot_runner_payload_preserves_synthetic_freeze_arguments(
        self, _mock_validate_symbol
    ) -> None:
        payload = _normalize_spot_runner_payload(
            {
                "symbol": "opgusdt",
                "strategy_mode": "spot_competition_synthetic_neutral_grid",
                "step_price": 0.0002,
                "per_order_notional": 180,
                "threshold_position_notional": 800,
                "threshold_reduce_target_notional": 650,
                "max_order_position_notional": 7000,
                "max_position_notional": 8200,
                "neutral_base_qty": 10000,
                "max_short_position_notional": 320,
                "synthetic_freeze_enabled": True,
                "synthetic_freeze_loss_ratio": 0.006,
                "synthetic_freeze_min_notional": 20,
                "synthetic_freeze_max_side_notional": 1000,
                "synthetic_freeze_release_profit_ratio": 0.0015,
                "synthetic_freeze_pair_release_enabled": True,
            }
        )

        self.assertTrue(payload["synthetic_freeze_enabled"])
        self.assertEqual(payload["synthetic_freeze_loss_ratio"], 0.006)
        self.assertEqual(payload["synthetic_freeze_min_notional"], 20.0)
        self.assertEqual(payload["synthetic_freeze_max_side_notional"], 1000.0)
        self.assertEqual(payload["synthetic_freeze_release_profit_ratio"], 0.0015)
        self.assertTrue(payload["synthetic_freeze_pair_release_enabled"])

    @patch("grid_optimizer.web._validate_market_symbol")
    def test_normalize_spot_runner_payload_preserves_spot_freeze_arguments(
        self, _mock_validate_symbol
    ) -> None:
        payload = _normalize_spot_runner_payload(
            {
                "symbol": "megausdt",
                "strategy_mode": "spot_competition_synthetic_neutral_grid",
                "step_price": 0.0002,
                "per_order_notional": 180,
                "threshold_position_notional": 800,
                "threshold_reduce_target_notional": 650,
                "max_order_position_notional": 7000,
                "max_position_notional": 8200,
                "neutral_base_qty": 10000,
                "max_short_position_notional": 320,
                "spot_freeze_enabled": True,
                "spot_freeze_dry_run": True,
                "spot_freeze_market_execution_enabled": True,
                "spot_freeze_base_hedge_qty": 10000,
                "spot_freeze_tolerance_qty": 0.01,
                "spot_freeze_deviation_notional": 50,
                "spot_freeze_min_loss_ratio": 0.01,
                "spot_freeze_max_per_cycle_notional": 100,
                "spot_freeze_total_cap_notional": 500,
                "spot_freeze_pair_release_enabled": True,
                "spot_freeze_profit_release_enabled": True,
                "spot_freeze_release_profit_ratio": 0.02,
            }
        )

        self.assertTrue(payload["spot_freeze_enabled"])
        self.assertTrue(payload["spot_freeze_dry_run"])
        self.assertTrue(payload["spot_freeze_market_execution_enabled"])
        self.assertEqual(payload["spot_freeze_base_hedge_qty"], 10000.0)
        self.assertEqual(payload["spot_freeze_tolerance_qty"], 0.01)
        self.assertEqual(payload["spot_freeze_deviation_notional"], 50.0)
        self.assertEqual(payload["spot_freeze_min_loss_ratio"], 0.01)
        self.assertEqual(payload["spot_freeze_max_per_cycle_notional"], 100.0)
        self.assertEqual(payload["spot_freeze_total_cap_notional"], 500.0)
        self.assertTrue(payload["spot_freeze_pair_release_enabled"])
        self.assertTrue(payload["spot_freeze_profit_release_enabled"])
        self.assertEqual(payload["spot_freeze_release_profit_ratio"], 0.02)

    @patch("grid_optimizer.web._validate_market_symbol")
    def test_normalize_spot_runner_payload_preserves_spot_app_loss_guard_arguments(
        self, _mock_validate_symbol
    ) -> None:
        payload = _normalize_spot_runner_payload(
            {
                "symbol": "megausdt",
                "strategy_mode": "spot_competition_synthetic_neutral_grid",
                "step_price": 0.00003,
                "per_order_notional": 120,
                "threshold_position_notional": 800,
                "threshold_reduce_target_notional": 650,
                "max_order_position_notional": 7000,
                "max_position_notional": 8200,
                "neutral_base_qty": 10000,
                "max_short_position_notional": 320,
                "spot_app_loss_guard_enabled": True,
                "spot_app_loss_min_notional": 5000,
                "spot_app_loss_per_10k_soft": 1.0,
                "spot_app_loss_per_10k_hard": 1.5,
                "spot_app_loss_recovery_reduce_only_enabled": True,
            }
        )

        self.assertTrue(payload["spot_app_loss_guard_enabled"])
        self.assertTrue(payload["spot_app_loss_recovery_reduce_only_enabled"])
        self.assertEqual(payload["spot_app_loss_min_notional"], 5000.0)
        self.assertEqual(payload["spot_app_loss_per_10k_soft"], 1.0)
        self.assertEqual(payload["spot_app_loss_per_10k_hard"], 1.5)

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

    @patch("grid_optimizer.web.time.sleep")
    @patch("grid_optimizer.web.subprocess.Popen")
    @patch("grid_optimizer.web._build_spot_runner_command")
    @patch("grid_optimizer.web._save_spot_runner_control_config")
    @patch("grid_optimizer.web._cancel_spot_strategy_orders")
    @patch("grid_optimizer.web._stop_spot_runner_process")
    @patch("grid_optimizer.web._read_spot_runner_process_for_symbol")
    def test_start_spot_runner_process_restarts_when_spot_freeze_config_changes(
        self,
        mock_read_runner,
        mock_stop_runner,
        mock_cancel_orders,
        _mock_save_config,
        mock_build_command,
        mock_popen,
        _mock_sleep,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            log_path = tmp_path / "spot_runner.log"
            pid_path = tmp_path / "spot_runner.pid"

            current_config = dict(SPOT_RUNNER_DEFAULT_CONFIG)
            current_config.update(
                {
                    "symbol": "MEGAUSDT",
                    "reset_state": False,
                    "state_path": str(tmp_path / "spot_state.json"),
                    "summary_jsonl": str(tmp_path / "spot_events.jsonl"),
                }
            )
            desired_config = dict(current_config)
            desired_config.update(
                {
                    "spot_freeze_enabled": True,
                    "spot_freeze_base_hedge_qty": 100.0,
                    "spot_freeze_deviation_notional": 50.0,
                }
            )
            mock_read_runner.side_effect = [
                {"configured": True, "pid": 1234, "is_running": True, "args": None, "config": current_config},
                {"configured": True, "pid": 4321, "is_running": True, "args": None, "config": desired_config},
            ]
            mock_stop_runner.return_value = {"stopped": True}
            mock_cancel_orders.return_value = {"canceled": 0}
            mock_build_command.return_value = ["python3", "-m", "grid_optimizer.spot_loop_runner"]
            mock_popen.return_value = MagicMock(pid=4321)

            with patch("grid_optimizer.web._spot_runner_log_path", return_value=log_path), patch(
                "grid_optimizer.web._spot_runner_pid_path", return_value=pid_path
            ):
                result = _start_spot_runner_process(desired_config)

        self.assertTrue(result["started"])
        self.assertTrue(result["restarted"])
        mock_stop_runner.assert_called_once_with("MEGAUSDT", cancel_orders=False)

    @patch("grid_optimizer.web.time.sleep")
    @patch("grid_optimizer.web.subprocess.Popen")
    @patch("grid_optimizer.web._build_spot_runner_command")
    @patch("grid_optimizer.web._save_spot_runner_control_config")
    @patch("grid_optimizer.web._cancel_spot_strategy_orders")
    @patch("grid_optimizer.web._read_spot_runner_process_for_symbol")
    def test_start_spot_runner_process_prefers_repo_src_over_runtime_src(
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
            runtime_dir = tmp_path / "runtime"
            runtime_dir.mkdir()
            log_path = tmp_path / "spot_runner.log"
            pid_path = tmp_path / "spot_runner.pid"

            config = dict(SPOT_RUNNER_DEFAULT_CONFIG)
            config.update(
                {
                    "symbol": "ETHU",
                    "reset_state": False,
                    "state_path": str(tmp_path / "spot_state.json"),
                    "summary_jsonl": str(tmp_path / "spot_events.jsonl"),
                }
            )
            mock_read_runner.side_effect = [
                {"configured": False, "pid": None, "is_running": False, "args": None, "config": {}},
                {"configured": True, "pid": 4321, "is_running": True, "args": None, "config": config},
            ]
            mock_cancel_orders.return_value = {"canceled": 0}
            mock_build_command.return_value = ["python3", "-m", "grid_optimizer.spot_loop_runner"]
            mock_popen.return_value = MagicMock(pid=4321)

            with patch("grid_optimizer.web.Path.cwd", return_value=runtime_dir), patch(
                "grid_optimizer.web._spot_runner_log_path", return_value=log_path
            ), patch("grid_optimizer.web._spot_runner_pid_path", return_value=pid_path), patch.dict(
                os.environ, {"PYTHONPATH": str(runtime_dir / "src")}, clear=False
            ):
                _start_spot_runner_process(config)

            env = mock_popen.call_args.kwargs["env"]
            first_pythonpath = env["PYTHONPATH"].split(os.pathsep)[0]
            self.assertEqual(first_pythonpath, str(Path(web.__file__).resolve().parents[1]))

    @patch("grid_optimizer.web.time.sleep")
    @patch("grid_optimizer.web.subprocess.Popen")
    @patch("grid_optimizer.web._build_spot_runner_command")
    @patch("grid_optimizer.web._save_spot_runner_control_config")
    @patch("grid_optimizer.web._cancel_spot_strategy_orders")
    @patch("grid_optimizer.web._read_spot_runner_process_for_symbol")
    def test_start_spot_runner_process_ignores_missing_adaptive_scale_in_old_events(
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
            events_path = tmp_path / "spot_events.jsonl"
            events_path.write_text('{"symbol":"ETHU","runtime_status":"running"}\n', encoding="utf-8")
            log_path = tmp_path / "spot_runner.log"
            pid_path = tmp_path / "spot_runner.pid"

            config = dict(SPOT_RUNNER_DEFAULT_CONFIG)
            config.update(
                {
                    "symbol": "ETHU",
                    "reset_state": False,
                    "state_path": str(tmp_path / "spot_state.json"),
                    "summary_jsonl": str(events_path),
                }
            )
            mock_read_runner.side_effect = [
                {"configured": False, "pid": None, "is_running": False, "args": None, "config": {}},
                {"configured": True, "pid": 4321, "is_running": True, "args": None, "config": config},
            ]
            mock_cancel_orders.return_value = {"canceled": 0}
            mock_build_command.return_value = ["python3", "-m", "grid_optimizer.spot_loop_runner"]
            mock_popen.return_value = MagicMock(pid=4321)

            with patch("grid_optimizer.web._spot_runner_log_path", return_value=log_path), patch(
                "grid_optimizer.web._spot_runner_pid_path", return_value=pid_path
            ):
                result = _start_spot_runner_process(config)

            self.assertTrue(result["started"])

    def test_synthetic_neutral_cached_conservative_reduce_realigns_base_below_soft_threshold(self) -> None:
        state = {
            "symbol": "XLMUSDT",
            "spot_competition_synthetic_neutral_grid_runtime_cache": {
                "strategy_mode": "spot_competition_synthetic_neutral_grid",
                "market_type": "futures",
                "runtime": {
                    "market_type": "futures",
                    "direction_state": "long_active",
                    "risk_state": "hard_reduce_only",
                    "recovery_mode": "conservative_reduce_only",
                    "recovery_errors": ["position_qty_mismatch"],
                    "synthetic_cost_unknown": True,
                    "synthetic_neutral": True,
                    "neutral_base_qty": 1142.0,
                    "position_lots": [],
                    "grid_anchor_price": 0.20315,
                },
                "applied_trade_keys": [],
            },
        }

        desired, controls = _build_spot_competition_inventory_grid_orders(
            state=state,
            trades=[],
            bid_price=0.2031,
            ask_price=0.2032,
            step_price=0.0001,
            buy_levels=2,
            sell_levels=2,
            first_order_multiplier=1.0,
            per_order_notional=30.0,
            threshold_position_notional=60.0,
            threshold_reduce_target_notional=0.0,
            warmup_position_notional=0.0,
            require_non_loss_exit=False,
            max_order_position_notional=60.0,
            max_position_notional=60.0,
            tick_size=0.0001,
            step_size=1.0,
            min_qty=1.0,
            min_notional=5.0,
            synthetic_neutral=True,
            neutral_base_qty=1142.0,
            max_short_position_notional=60.0,
            actual_base_qty=1340.0,
            symbol="XLMUSDT",
        )

        forced_reduce = [item for item in desired if item.get("role") == "forced_reduce"]
        self.assertTrue(forced_reduce)
        self.assertEqual(forced_reduce[0]["side"], "SELL")
        self.assertLess(forced_reduce[0]["notional"], 60.0)
        self.assertEqual(controls["risk_state"], "hard_reduce_only")
        self.assertEqual(controls["effective_buy_levels"], 0)
        self.assertGreaterEqual(controls["effective_sell_levels"], 1)


if __name__ == "__main__":
    unittest.main()
