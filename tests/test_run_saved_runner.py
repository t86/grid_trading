from __future__ import annotations

import sys
import unittest
from unittest.mock import patch

from grid_optimizer import run_saved_runner


class RunSavedRunnerTests(unittest.TestCase):
    @patch("grid_optimizer.run_saved_runner.os.chdir")
    @patch("grid_optimizer.run_saved_runner.os.getcwd", return_value="/repo")
    @patch("grid_optimizer.run_saved_runner.atexit.register")
    @patch("grid_optimizer.run_saved_runner._write_pid")
    @patch("grid_optimizer.run_saved_runner.os.execvpe")
    @patch("grid_optimizer.run_saved_runner._build_runner_command")
    @patch("grid_optimizer.run_saved_runner._load_runner_control_config")
    def test_main_loads_runner_control_config_and_execs(
        self,
        mock_load_runner_control_config,
        mock_build_runner_command,
        mock_execvpe,
        mock_write_pid,
        mock_atexit_register,
        _mock_getcwd,
        mock_chdir,
    ) -> None:
        mock_load_runner_control_config.return_value = {"symbol": "SOONUSDT"}
        mock_build_runner_command.return_value = ["python", "-m", "grid_optimizer.loop_runner", "--symbol", "SOONUSDT"]

        with patch.dict("os.environ", {}, clear=True), patch.object(
            sys, "argv", ["run_saved_runner.py", "--symbol", "soonusdt"]
        ):
            run_saved_runner.main()

        mock_write_pid.assert_called_once()
        mock_atexit_register.assert_called_once()
        mock_chdir.assert_not_called()
        mock_load_runner_control_config.assert_called_once_with("SOONUSDT")
        mock_build_runner_command.assert_called_once_with({"symbol": "SOONUSDT"})
        mock_execvpe.assert_called_once()
        _, _, exec_env = mock_execvpe.call_args.args
        self.assertEqual(exec_env["GRID_RUNNER_SERVICE_TEMPLATE"], "grid-loop@{symbol}.service")
        self.assertEqual(exec_env["GRID_AUTO_RESET_ON_CONFIG_CHANGE"], "1")

    @patch("grid_optimizer.run_saved_runner.os.chdir")
    @patch("grid_optimizer.run_saved_runner.os.getcwd", return_value="/repo")
    @patch("grid_optimizer.run_saved_runner.atexit.register")
    @patch("grid_optimizer.run_saved_runner._write_pid")
    @patch("grid_optimizer.run_saved_runner.os.execvpe")
    @patch("grid_optimizer.run_saved_runner._build_runner_command")
    @patch("grid_optimizer.run_saved_runner._load_runner_control_config")
    def test_main_switches_to_runner_work_dir_before_loading_control_config(
        self,
        mock_load_runner_control_config,
        mock_build_runner_command,
        mock_execvpe,
        mock_write_pid,
        mock_atexit_register,
        _mock_getcwd,
        mock_chdir,
    ) -> None:
        mock_load_runner_control_config.return_value = {"symbol": "SOONUSDT"}
        mock_build_runner_command.return_value = ["python", "-m", "grid_optimizer.loop_runner", "--symbol", "SOONUSDT"]

        with patch.dict("os.environ", {"GRID_RUNNER_WORK_DIR": "/tmp/runtime"}, clear=True), patch.object(
            sys, "argv", ["run_saved_runner.py", "--symbol", "soonusdt"]
        ):
            run_saved_runner.main()

        self.assertEqual(len(mock_chdir.call_args_list), 2)
        self.assertEqual(mock_chdir.call_args_list[0].args, ("/tmp/runtime",))
        self.assertEqual(mock_chdir.call_args_list[1].args, ("/repo",))
        mock_load_runner_control_config.assert_called_once_with("SOONUSDT")
        mock_execvpe.assert_called_once()

    @patch("grid_optimizer.run_saved_runner.os.chdir")
    @patch("grid_optimizer.run_saved_runner.os.getcwd", return_value="/repo")
    @patch("grid_optimizer.run_saved_runner.atexit.register")
    @patch("grid_optimizer.run_saved_runner._write_pid")
    @patch("grid_optimizer.run_saved_runner.os.execvpe")
    @patch("grid_optimizer.run_saved_runner._build_runner_command")
    @patch("grid_optimizer.run_saved_runner._load_runner_control_config")
    def test_main_anchors_relative_runtime_paths_to_runner_work_dir(
        self,
        mock_load_runner_control_config,
        mock_build_runner_command,
        mock_execvpe,
        mock_write_pid,
        mock_atexit_register,
        _mock_getcwd,
        mock_chdir,
    ) -> None:
        mock_load_runner_control_config.return_value = {"symbol": "SOONUSDT"}
        mock_build_runner_command.return_value = [
            "python",
            "-m",
            "grid_optimizer.loop_runner",
            "--state-path",
            "output/state.json",
            "--plan-json",
            "output/plan.json",
            "--submit-report-json",
            "/already/absolute/submit.json",
            "--summary-jsonl",
            "output/events.jsonl",
        ]

        with patch.dict("os.environ", {"GRID_RUNNER_WORK_DIR": "/tmp/runtime"}, clear=True), patch.object(
            sys, "argv", ["run_saved_runner.py", "--symbol", "soonusdt"]
        ):
            run_saved_runner.main()

        command = mock_execvpe.call_args.args[1]
        self.assertEqual(command[command.index("--state-path") + 1], "/tmp/runtime/output/state.json")
        self.assertEqual(command[command.index("--plan-json") + 1], "/tmp/runtime/output/plan.json")
        self.assertEqual(command[command.index("--submit-report-json") + 1], "/already/absolute/submit.json")
        self.assertEqual(command[command.index("--summary-jsonl") + 1], "/tmp/runtime/output/events.jsonl")

    @patch("grid_optimizer.run_saved_runner.os.chdir")
    @patch("grid_optimizer.run_saved_runner.os.getcwd", return_value="/repo")
    @patch("grid_optimizer.run_saved_runner.atexit.register")
    @patch("grid_optimizer.run_saved_runner._write_pid")
    @patch("grid_optimizer.run_saved_runner.os.execvpe")
    @patch("grid_optimizer.run_saved_runner._should_use_spot_runner", return_value=True)
    @patch("grid_optimizer.run_saved_runner._build_spot_runner_command")
    @patch("grid_optimizer.run_saved_runner._load_spot_runner_control_config")
    @patch("grid_optimizer.run_saved_runner._load_runner_control_config")
    def test_main_falls_back_to_spot_runner_control_config(
        self,
        mock_load_runner_control_config,
        mock_load_spot_runner_control_config,
        mock_build_spot_runner_command,
        _mock_should_use_spot_runner,
        mock_execvpe,
        mock_write_pid,
        mock_atexit_register,
        _mock_getcwd,
        mock_chdir,
    ) -> None:
        mock_load_spot_runner_control_config.return_value = {
            "symbol": "SPKUSDT",
            "market_type": "spot",
            "strategy_mode": "spot_competition_synthetic_neutral_grid",
        }
        mock_build_spot_runner_command.return_value = [
            "python",
            "-m",
            "grid_optimizer.spot_loop_runner",
            "--symbol",
            "SPKUSDT",
        ]

        with patch.dict("os.environ", {}, clear=True), patch.object(
            sys, "argv", ["run_saved_runner.py", "--symbol", "spkusdt"]
        ):
            run_saved_runner.main()

        mock_write_pid.assert_called_once()
        mock_atexit_register.assert_called_once()
        mock_chdir.assert_not_called()
        mock_load_runner_control_config.assert_not_called()
        mock_load_spot_runner_control_config.assert_called_once_with("SPKUSDT")
        mock_build_spot_runner_command.assert_called_once_with(mock_load_spot_runner_control_config.return_value)
        mock_execvpe.assert_called_once()
        command = mock_execvpe.call_args.args[1]
        self.assertIn("grid_optimizer.spot_loop_runner", command)

    @patch("grid_optimizer.run_saved_runner.os.chdir")
    @patch("grid_optimizer.run_saved_runner.os.getcwd", return_value="/repo")
    @patch("grid_optimizer.run_saved_runner.atexit.register")
    @patch("grid_optimizer.run_saved_runner._write_pid")
    @patch("grid_optimizer.run_saved_runner.os.execvpe")
    @patch("grid_optimizer.run_saved_runner._should_use_spot_runner", return_value=True)
    @patch("grid_optimizer.run_saved_runner._build_spot_runner_command")
    @patch("grid_optimizer.run_saved_runner._load_spot_runner_control_config")
    @patch("grid_optimizer.run_saved_runner.spot_app_loss_audit_main", return_value=2)
    def test_main_blocks_spot_runner_when_app_loss_prestart_gate_rejects(
        self,
        mock_app_loss_audit_main,
        mock_load_spot_runner_control_config,
        mock_build_spot_runner_command,
        _mock_should_use_spot_runner,
        mock_execvpe,
        _mock_write_pid,
        _mock_atexit_register,
        _mock_getcwd,
        _mock_chdir,
    ) -> None:
        mock_load_spot_runner_control_config.return_value = {
            "symbol": "XPLUSDT",
            "market_type": "spot",
            "strategy_mode": "spot_competition_synthetic_neutral_grid",
            "spot_app_loss_prestart_gate_enabled": True,
            "spot_app_loss_prestart_gate_start_time": "2026-06-24T19:57:00+08:00",
            "spot_app_loss_prestart_gate_max_loss_per_10k": 1.0,
            "spot_app_loss_prestart_gate_max_safe_sell_gap_ticks": 2.0,
            "spot_app_loss_prestart_gate_min_maker_ratio": 0.99,
            "spot_app_loss_prestart_gate_min_gross_notional": 5000.0,
        }

        with patch.dict("os.environ", {}, clear=True), patch.object(
            sys, "argv", ["run_saved_runner.py", "--symbol", "xplusdt"]
        ):
            with self.assertRaises(SystemExit) as raised:
                run_saved_runner.main()

        self.assertEqual(raised.exception.code, 2)
        mock_app_loss_audit_main.assert_called_once_with(
            [
                "--symbol",
                "XPLUSDT",
                "--start-time",
                "2026-06-24T19:57:00+08:00",
                "--max-app-loss-per-10k",
                "1.0",
                "--max-safe-maker-sell-gap-ticks",
                "2.0",
                "--min-maker-ratio",
                "0.99",
                "--min-gross-notional",
                "5000.0",
                "--require-gate",
            ]
        )

    @patch("grid_optimizer.run_saved_runner.os.chdir")
    @patch("grid_optimizer.run_saved_runner.os.getcwd", return_value="/repo")
    @patch("grid_optimizer.run_saved_runner.atexit.register")
    @patch("grid_optimizer.run_saved_runner._write_pid")
    @patch("grid_optimizer.run_saved_runner.os.execvpe")
    @patch("grid_optimizer.run_saved_runner._should_use_spot_runner", return_value=True)
    @patch("grid_optimizer.run_saved_runner._build_spot_runner_command")
    @patch("grid_optimizer.run_saved_runner._load_spot_runner_control_config")
    @patch("grid_optimizer.run_saved_runner.spot_app_loss_audit_main", return_value=2)
    def test_main_allows_matching_live_freeze_hedge_until_app_loss_gate(
        self,
        mock_app_loss_audit_main,
        mock_load_spot_runner_control_config,
        mock_build_spot_runner_command,
        _mock_should_use_spot_runner,
        mock_execvpe,
        _mock_write_pid,
        _mock_atexit_register,
        _mock_getcwd,
        _mock_chdir,
    ) -> None:
        mock_load_spot_runner_control_config.return_value = {
            "symbol": "XPLUSDT",
            "market_type": "spot",
            "strategy_mode": "spot_competition_synthetic_neutral_grid",
            "spot_app_loss_guard_enabled": True,
            "spot_app_loss_recovery_reduce_only_enabled": True,
            "spot_app_loss_prestart_gate_enabled": True,
            "spot_app_loss_prestart_gate_start_time": "2026-06-24T19:57:00+08:00",
            "spot_app_loss_prestart_gate_max_loss_per_10k": 1.0,
            "spot_app_loss_prestart_gate_max_safe_sell_gap_ticks": 2.0,
            "spot_app_loss_prestart_gate_min_maker_ratio": 0.99,
            "spot_app_loss_prestart_gate_min_gross_notional": 5000.0,
            "spot_freeze_enabled": True,
            "spot_freeze_maker_execution_enabled": True,
            "spot_freeze_base_hedge_qty": 4800.0,
            "spot_freeze_tolerance_qty": 0.2,
            "spot_freeze_deviation_notional": 50.0,
            "spot_freeze_total_cap_notional": 900.0,
            "spot_freeze_max_per_cycle_notional": 180.0,
            "per_order_notional": 60.0,
            "summary_jsonl": "",
        }

        with patch("grid_optimizer.web.load_binance_api_credentials", return_value=("key", "secret")), patch(
            "grid_optimizer.web.fetch_futures_position_mode", return_value={"dualSidePosition": True}
        ), patch(
            "grid_optimizer.web.fetch_futures_position_risk_v3",
            return_value=[
                {"symbol": "XPLUSDT", "positionSide": "SHORT", "positionAmt": "-4800"},
                {"symbol": "XPLUSDT", "positionSide": "LONG", "positionAmt": "0"},
            ],
        ), patch.dict("os.environ", {}, clear=True), patch.object(
            sys, "argv", ["run_saved_runner.py", "--symbol", "xplusdt"]
        ):
            with self.assertRaises(SystemExit) as raised:
                run_saved_runner.main()

        self.assertEqual(raised.exception.code, 2)
        mock_app_loss_audit_main.assert_called_once()
        mock_build_spot_runner_command.assert_not_called()
        mock_execvpe.assert_not_called()

    @patch("grid_optimizer.run_saved_runner.os.chdir")
    @patch("grid_optimizer.run_saved_runner.os.getcwd", return_value="/repo")
    @patch("grid_optimizer.run_saved_runner.atexit.register")
    @patch("grid_optimizer.run_saved_runner._write_pid")
    @patch("grid_optimizer.run_saved_runner.os.execvpe")
    @patch("grid_optimizer.run_saved_runner._should_use_spot_runner", return_value=True)
    @patch("grid_optimizer.run_saved_runner._build_spot_runner_command")
    @patch("grid_optimizer.run_saved_runner._load_spot_runner_control_config")
    @patch("grid_optimizer.run_saved_runner.spot_app_loss_audit_main", return_value=0)
    def test_main_blocks_spot_runner_when_freeze_preflight_rejects_app_loss_recovery(
        self,
        mock_app_loss_audit_main,
        mock_load_spot_runner_control_config,
        mock_build_spot_runner_command,
        _mock_should_use_spot_runner,
        mock_execvpe,
        _mock_write_pid,
        _mock_atexit_register,
        _mock_getcwd,
        _mock_chdir,
    ) -> None:
        mock_load_spot_runner_control_config.return_value = {
            "symbol": "XPLUSDT",
            "market_type": "spot",
            "strategy_mode": "spot_competition_synthetic_neutral_grid",
            "spot_app_loss_guard_enabled": True,
            "spot_app_loss_recovery_reduce_only_enabled": True,
            "spot_app_loss_prestart_gate_enabled": True,
            "spot_freeze_enabled": False,
            "summary_jsonl": "",
        }

        with patch.dict("os.environ", {}, clear=True), patch.object(
            sys, "argv", ["run_saved_runner.py", "--symbol", "xplusdt"]
        ):
            with self.assertRaises(ValueError) as raised:
                run_saved_runner.main()

        self.assertIn("spot_freeze_enabled", str(raised.exception))
        mock_app_loss_audit_main.assert_not_called()
        mock_build_spot_runner_command.assert_not_called()
        mock_execvpe.assert_not_called()


if __name__ == "__main__":
    unittest.main()
