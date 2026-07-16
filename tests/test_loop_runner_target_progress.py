from __future__ import annotations

import argparse
import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from grid_optimizer import runtime_guards
from grid_optimizer.loop_runner import (
    _load_futures_runtime_guard_inputs,
    _maybe_handle_runtime_guard,
    _persisted_futures_target_progress,
)
from grid_optimizer.futures_run_lifecycle import (
    bind_run_contract_owner,
    run_contract_identity_from_config,
)
from grid_optimizer.runtime_guards import normalize_runtime_guard_config


START = datetime(2026, 7, 15, 8, 0, tzinfo=timezone.utc)
END = START + timedelta(hours=2)


class LoopRunnerTargetProgressTests(unittest.TestCase):
    def _target_args(self, state_path: Path) -> argparse.Namespace:
        return argparse.Namespace(
            symbol="BCHUSDT",
            recv_window=5000,
            strategy_profile="bch_probe",
            strategy_mode="hedge_best_quote_maker_volume_v1",
            run_start_time=START.isoformat(),
            runtime_guard_stats_start_time=START.isoformat(),
            run_end_time=END.isoformat(),
            max_cumulative_notional=20_000.0,
            rolling_hourly_loss_limit=None,
            rolling_hourly_loss_per_10k_limit=None,
            rolling_hourly_loss_per_10k_min_notional=10_000.0,
            max_actual_net_notional=None,
            max_synthetic_drift_notional=None,
            max_unrealized_loss=None,
            terminal_drain_exit_policy="drain_then_preserve",
            terminal_drain_absolute_loss_budget=5.0,
            terminal_drain_max_wait_seconds=900.0,
            terminal_drain_stop_preserve_reason=None,
            terminal_drain_max_order_notional=22.0,
            terminal_drain_loss_lease_seconds=300.0,
            terminal_drain_order_reprice_seconds=120.0,
            terminal_drain_flat_confirm_cycles=2,
            per_order_notional=22.0,
            state_path=str(state_path),
            auto_regime_enabled=False,
        )

    def _authorize_args(self, args: argparse.Namespace, tmpdir: str) -> None:
        control, _ = bind_run_contract_owner(
            vars(args),
            activated_at=START,
        )
        control_path = Path(tmpdir) / "bch-control.json"
        control_path.write_text(json.dumps(control), encoding="utf-8")
        args.recovery_control_path = str(control_path)

    def test_exact_target_progress_counts_partial_fills_and_excludes_end(self) -> None:
        start_ms = int(START.timestamp() * 1000)
        end_ms = int(END.timestamp() * 1000)
        exchange_rows = [
            {
                "id": 1,
                "orderId": 77,
                "time": start_ms,
                "quoteQty": "10",
                "realizedPnl": "0",
            },
            {
                "id": 2,
                "orderId": 77,
                "time": end_ms - 1,
                "quoteQty": "15",
                "realizedPnl": "0",
            },
            {
                "id": 3,
                "orderId": 77,
                "time": end_ms,
                "quoteQty": "1000",
                "realizedPnl": "0",
            },
        ]

        def fetch_page(**kwargs):
            return [
                row
                for row in exchange_rows
                if kwargs["start_time_ms"]
                <= int(row["time"])
                <= kwargs["end_time_ms"]
            ]

        with patch(
            "grid_optimizer.loop_runner.summarize_futures_runtime_guard_inputs",
            return_value=(9999.0, [], START),
        ), patch(
            "grid_optimizer.loop_runner.load_binance_api_credentials",
            return_value=("key", "secret"),
        ), patch(
            "grid_optimizer.loop_runner.fetch_futures_user_trades",
            side_effect=fetch_page,
        ) as mock_fetch:
            gross, _events, stats_start = _load_futures_runtime_guard_inputs(
                Path("output/bch_loop_events.jsonl"),
                runtime_guard_stats_start_time=START,
                symbol="BCHUSDT",
                now=END + timedelta(minutes=5),
                run_end_time=END,
                max_cumulative_notional=20_000.0,
                authoritative_target_progress=True,
            )

        self.assertEqual(gross, 25.0)
        self.assertEqual(stats_start, START)
        request = mock_fetch.call_args.kwargs
        self.assertEqual(request["start_time_ms"], start_ms)
        self.assertEqual(request["end_time_ms"], end_ms - 1)

    def test_immutable_target_window_ignores_later_competition_board_start(self) -> None:
        trade_rows = [
            {
                "id": 1,
                "orderId": 11,
                "time": int((START + timedelta(minutes=30)).timestamp() * 1000),
                "quoteQty": "10",
                "realizedPnl": "0",
            },
            {
                "id": 2,
                "orderId": 12,
                "time": int(END.timestamp() * 1000),
                "quoteQty": "1000",
                "realizedPnl": "0",
            },
        ]
        with patch(
            "grid_optimizer.runtime_guards.read_jsonl",
            side_effect=[trade_rows, []],
        ), patch(
            "grid_optimizer.runtime_guards.resolve_active_competition_board",
            return_value={
                "activity_start_at": (START + timedelta(hours=1)).isoformat()
            },
        ) as mock_board:
            gross, _events, stats_start = (
                runtime_guards.summarize_futures_runtime_guard_inputs(
                    Path("output/bch_loop_events.jsonl"),
                    runtime_guard_stats_start_time=START,
                    runtime_guard_stats_end_time=END,
                    symbol="BCHUSDT",
                    now=END,
                    immutable_window=True,
                )
            )

        self.assertEqual(gross, 10.0)
        self.assertEqual(stats_start, START)
        mock_board.assert_not_called()

    def test_exchange_truth_failure_during_window_keeps_trading_without_terminal_side_effect(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "state.json"
            state_path.write_text("{}", encoding="utf-8")
            args = self._target_args(state_path)
            self._authorize_args(args, tmpdir)
            with patch(
                "grid_optimizer.loop_runner._load_futures_runtime_guard_inputs",
                side_effect=RuntimeError("exchange unavailable"),
            ) as mock_inputs, patch(
                "grid_optimizer.loop_runner.evaluate_runtime_guards"
            ) as mock_evaluate, patch(
                "grid_optimizer.loop_runner._handle_terminal_drain_round"
            ) as mock_terminal:
                summary = _maybe_handle_runtime_guard(
                    args=args,
                    cycle=1,
                    cycle_started_at=START + timedelta(hours=1),
                    summary_path=Path(tmpdir) / "events.jsonl",
                )

            self.assertIsNone(summary)
            self.assertTrue(
                mock_inputs.call_args.kwargs["authoritative_target_progress"]
            )
            mock_evaluate.assert_not_called()
            mock_terminal.assert_not_called()
            persisted = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual(
                persisted["futures_target_progress_error"]["reason"],
                "target_progress_exchange_truth_unavailable",
            )

    def test_exchange_truth_failure_at_deadline_starts_terminal_owner_clock(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "state.json"
            state_path.write_text("{}", encoding="utf-8")
            args = self._target_args(state_path)
            self._authorize_args(args, tmpdir)
            terminal_summary = {
                "runtime_status": "draining",
                "runner_exit_allowed": False,
                "run_outcome": "target_unmet_deadline",
            }
            with patch(
                "grid_optimizer.loop_runner._load_futures_runtime_guard_inputs",
                side_effect=RuntimeError("exchange unavailable"),
            ), patch(
                "grid_optimizer.loop_runner._handle_terminal_drain_round",
                return_value=terminal_summary,
            ) as mock_terminal:
                summary = _maybe_handle_runtime_guard(
                    args=args,
                    cycle=1,
                    cycle_started_at=END,
                    summary_path=Path(tmpdir) / "events.jsonl",
                )

            self.assertEqual(summary["run_outcome"], "target_unmet_deadline")
            self.assertEqual(
                summary["target_progress_status"],
                "observation_unavailable_at_deadline",
            )
            call = mock_terminal.call_args.kwargs
            self.assertEqual(call["cycle_started_at"], END)
            self.assertEqual(call["cumulative_gross_notional"], 0.0)
            self.assertEqual(
                call["runtime_guard_result"].matched_reasons,
                ["after_end_window"],
            )
            self.assertEqual(
                call["runtime_guard_result"].primary_reason,
                "after_end_window",
            )
            persisted = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual(
                persisted["futures_target_progress_error"]["reason"],
                "target_progress_exchange_truth_unavailable",
            )

    def test_exchange_truth_failure_before_start_still_waits(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "state.json"
            state_path.write_text("{}", encoding="utf-8")
            args = self._target_args(state_path)
            self._authorize_args(args, tmpdir)
            with patch(
                "grid_optimizer.loop_runner._load_futures_runtime_guard_inputs",
                side_effect=RuntimeError("exchange unavailable"),
            ), patch(
                "grid_optimizer.loop_runner._handle_terminal_drain_round"
            ) as mock_terminal:
                summary = _maybe_handle_runtime_guard(
                    args=args,
                    cycle=1,
                    cycle_started_at=START - timedelta(seconds=1),
                    summary_path=Path(tmpdir) / "events.jsonl",
                )

            self.assertEqual(summary["runtime_status"], "waiting")
            self.assertEqual(summary["lifecycle_action"], "wait_for_start")
            self.assertEqual(
                summary["target_progress_status"],
                "observation_unavailable_before_start",
            )
            mock_terminal.assert_not_called()

    def test_post_cycle_summary_reuses_fenced_target_snapshot(self) -> None:
        args = self._target_args(Path("output/bch_state.json"))
        config = normalize_runtime_guard_config(vars(args))
        contract_id = run_contract_identity_from_config(vars(args))
        state = {
            "futures_target_progress": {
                "schema": "futures_target_progress_v1",
                "symbol": "BCHUSDT",
                "run_contract_id": contract_id,
                "window_start": START.isoformat(),
                "window_end": END.isoformat(),
                "query_end": (START + timedelta(hours=1)).isoformat(),
                "gross_notional": 12_345.0,
                "target_value": 20_000.0,
                "observed_at": (START + timedelta(hours=1)).isoformat(),
            }
        }

        observed = _persisted_futures_target_progress(
            state=state,
            args=args,
            runtime_guard_config=config,
        )

        self.assertEqual(observed, 12_345.0)

    def test_post_cycle_summary_rejects_target_snapshot_from_other_contract(self) -> None:
        args = self._target_args(Path("output/bch_state.json"))
        config = normalize_runtime_guard_config(vars(args))
        state = {
            "futures_target_progress": {
                "schema": "futures_target_progress_v1",
                "symbol": "BCHUSDT",
                "run_contract_id": "futures-run-contract-v3-wrong",
                "window_start": START.isoformat(),
                "window_end": END.isoformat(),
                "query_end": START.isoformat(),
                "gross_notional": 1.0,
                "target_value": 20_000.0,
                "observed_at": START.isoformat(),
            }
        }

        with self.assertRaisesRegex(ValueError, "contract mismatch"):
            _persisted_futures_target_progress(
                state=state,
                args=args,
                runtime_guard_config=config,
            )

    def test_before_start_waits_without_creating_terminal_owner(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "state.json"
            state_path.write_text("{}", encoding="utf-8")
            args = self._target_args(state_path)
            self._authorize_args(args, tmpdir)
            with patch(
                "grid_optimizer.loop_runner._load_futures_runtime_guard_inputs",
                return_value=(0.0, [], START),
            ), patch(
                "grid_optimizer.loop_runner._handle_terminal_drain_round"
            ) as mock_terminal:
                summary = _maybe_handle_runtime_guard(
                    args=args,
                    cycle=1,
                    cycle_started_at=START - timedelta(minutes=1),
                    summary_path=Path(tmpdir) / "events.jsonl",
                )

        self.assertEqual(summary["runtime_status"], "waiting")
        self.assertEqual(summary["stop_reason"], "before_start_window")
        self.assertEqual(summary["lifecycle_action"], "wait_for_start")
        self.assertFalse(summary["stop_triggered"])
        self.assertFalse(summary["runner_exit_allowed"])
        mock_terminal.assert_not_called()


if __name__ == "__main__":
    unittest.main()
