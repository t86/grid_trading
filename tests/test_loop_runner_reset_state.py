from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock, patch

from grid_optimizer.loop_runner import _build_parser, main


class LoopRunnerResetStateTests(unittest.TestCase):
    @patch("grid_optimizer.loop_runner.time.sleep")
    @patch("grid_optimizer.loop_runner._print_cycle_summary")
    @patch("grid_optimizer.loop_runner._append_jsonl")
    @patch("grid_optimizer.loop_runner._maybe_handle_runtime_guard")
    @patch("grid_optimizer.loop_runner._build_parser")
    def test_main_preserves_reset_state_while_waiting_for_start_window(
        self,
        mock_build_parser,
        mock_runtime_guard,
        mock_append_jsonl,
        mock_print_cycle_summary,
        mock_sleep,
    ) -> None:
        del mock_append_jsonl, mock_print_cycle_summary, mock_sleep
        with TemporaryDirectory() as tmpdir:
            args = _build_parser().parse_args([])
            args.iterations = 1
            args.reset_state = True
            args.run_start_time = "2026-04-02T07:40:00+00:00"
            args.state_path = str(Path(tmpdir) / "bardusdt_loop_state.json")
            args.plan_json = str(Path(tmpdir) / "bardusdt_loop_latest_plan.json")
            args.submit_report_json = str(Path(tmpdir) / "bardusdt_loop_latest_submit.json")
            args.summary_jsonl = str(Path(tmpdir) / "bardusdt_loop_events.jsonl")

            parser = Mock()
            parser.parse_args.return_value = args
            mock_build_parser.return_value = parser
            mock_runtime_guard.return_value = {
                "runtime_status": "waiting",
                "stop_triggered": False,
                "stop_reason": "before_start_window",
            }

            main()

        self.assertTrue(args.reset_state)

    @patch("grid_optimizer.loop_runner.time.sleep")
    @patch("grid_optimizer.loop_runner._print_cycle_summary")
    @patch("grid_optimizer.loop_runner._append_jsonl")
    @patch("grid_optimizer.loop_runner._maybe_handle_runtime_guard")
    @patch("grid_optimizer.loop_runner._build_parser")
    def test_main_preserves_reset_state_after_pre_start_error(
        self,
        mock_build_parser,
        mock_runtime_guard,
        mock_append_jsonl,
        mock_print_cycle_summary,
        mock_sleep,
    ) -> None:
        del mock_append_jsonl, mock_print_cycle_summary, mock_sleep
        with TemporaryDirectory() as tmpdir:
            args = _build_parser().parse_args([])
            args.iterations = 1
            args.reset_state = True
            args.run_start_time = "2026-04-02T07:40:00+00:00"
            args.state_path = str(Path(tmpdir) / "bardusdt_loop_state.json")
            args.plan_json = str(Path(tmpdir) / "bardusdt_loop_latest_plan.json")
            args.submit_report_json = str(Path(tmpdir) / "bardusdt_loop_latest_submit.json")
            args.summary_jsonl = str(Path(tmpdir) / "bardusdt_loop_events.jsonl")

            parser = Mock()
            parser.parse_args.return_value = args
            mock_build_parser.return_value = parser
            mock_runtime_guard.side_effect = RuntimeError("transient pre-start failure")

            main()

        self.assertTrue(args.reset_state)


if __name__ == "__main__":
    unittest.main()
