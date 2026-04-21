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


if __name__ == "__main__":
    unittest.main()
