from __future__ import annotations

import argparse
import json
import unittest
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock, patch

from grid_optimizer.futures_run_lifecycle import (
    run_contract_identity_from_config,
    run_contract_snapshot_from_config,
)
from grid_optimizer.futures_terminal_drain import (
    DrainStage,
    TerminalDrainState,
    terminal_drain_state_to_dict,
)
from grid_optimizer.loop_runner import (
    _build_parser,
    _terminal_drain_ack_handoff_after_normal_event,
    _terminal_drain_exit_contract_from_snapshot,
    _terminal_drain_owner_integrity_digest,
    _terminal_drain_owner_transition,
    _terminal_drain_runtime_integrity_digest,
    main,
)
from grid_optimizer.semi_auto_plan import load_or_initialize_state


class LoopRunnerResetStateTests(unittest.TestCase):
    def test_reset_preserves_pending_terminal_handoff_until_normal_event_ack(
        self,
    ) -> None:
        with TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "bchusdt_loop_state.json"
            current_contract = {
                "symbol": "BCHUSDT",
                "strategy_profile": "new_profile",
                "strategy_mode": "hedge_best_quote_maker_volume_v1",
                "run_start_time": "2026-07-16T00:00:00+00:00",
                "runtime_guard_stats_start_time": "2026-07-16T00:00:00+00:00",
                "run_end_time": "2026-07-17T00:00:00+00:00",
                "max_cumulative_notional": 20_000.0,
                "terminal_drain_exit_policy": "drain_then_preserve",
                "terminal_drain_absolute_loss_budget": 2.0,
                "terminal_drain_max_wait_seconds": 600.0,
                "terminal_drain_stop_preserve_reason": None,
                "terminal_drain_max_order_notional": 20.0,
                "terminal_drain_loss_lease_seconds": 300.0,
                "terminal_drain_order_reprice_seconds": 120.0,
                "terminal_drain_flat_confirm_cycles": 2,
            }
            old_contract = {**current_contract, "strategy_profile": "old_profile"}
            old_snapshot = run_contract_snapshot_from_config(old_contract)
            old_contract_id = run_contract_identity_from_config(old_snapshot)
            current_contract_id = run_contract_identity_from_config(current_contract)
            old_decision_id = f"BCHUSDT|{old_contract_id}"
            started_at = datetime(2026, 7, 15, 23, 0, tzinfo=timezone.utc)
            drain_state = replace(
                TerminalDrainState.initial(
                    "BCHUSDT",
                    decision_id=old_decision_id,
                ),
                stage=DrainStage.STOPPED_PRESERVED,
            )
            old_owner = {
                "schema": "futures_terminal_drain_runtime_v1",
                "decision_id": old_decision_id,
                "run_contract_id": old_contract_id,
                "run_contract_snapshot": old_snapshot,
                "started_at": started_at.isoformat(),
                "run_outcome": "target_unmet_deadline",
                "exit_status": "stopped_preserved",
                "exit_contract": _terminal_drain_exit_contract_from_snapshot(
                    old_snapshot,
                    captured_at=started_at.isoformat(),
                ),
                "initial_frozen_long_qty": 0.0,
                "initial_frozen_short_qty": 0.0,
                "drain_state": terminal_drain_state_to_dict(drain_state),
                "intents": [],
                "active_intent_ids": [],
            }
            old_owner["owner_integrity_digest"] = (
                _terminal_drain_owner_integrity_digest(old_owner)
            )
            old_owner["runtime_integrity_digest"] = (
                _terminal_drain_runtime_integrity_digest(old_owner)
            )
            handoff_history = [
                {
                    "status": "acknowledged",
                    "from_decision_id": "BCHUSDT|older-run",
                }
            ]
            state_path.write_text(
                json.dumps(
                    {
                        "futures_terminal_drain": old_owner,
                        "futures_terminal_intent": {
                            "schema": "futures_lifecycle_intent_v2",
                            "intent_id": "BCHUSDT-old-intent",
                            "symbol": "BCHUSDT",
                            "status": "stopped_preserved",
                        },
                        "futures_terminal_handoff_history": handoff_history,
                    }
                ),
                encoding="utf-8",
            )
            transition_state = json.loads(state_path.read_text(encoding="utf-8"))
            transitioned = _terminal_drain_owner_transition(
                state=transition_state,
                state_path=state_path,
                current_decision_id=f"BCHUSDT|{current_contract_id}",
                now=datetime(2026, 7, 16, 0, 1, tzinfo=timezone.utc),
            )
            self.assertEqual(transitioned, "archived")
            before_reset = json.loads(state_path.read_text(encoding="utf-8"))
            pending_handoff = before_reset["futures_terminal_handoff"]
            drain_history = before_reset["futures_terminal_drain_history"]
            intent_history = before_reset["futures_terminal_intent_history"]
            planner_args = argparse.Namespace(
                symbol="BCHUSDT",
                strategy_mode="hedge_best_quote_maker_volume_v1",
                step_price=0.1,
                buy_levels=1,
                sell_levels=1,
                per_order_notional=20.0,
                base_position_notional=0.0,
                center_price=None,
                down_trigger_steps=4,
                up_trigger_steps=8,
                shift_steps=4,
            )

            reset_state = load_or_initialize_state(
                state_path=state_path,
                args=planner_args,
                symbol_info={
                    "tick_size": 0.1,
                    "step_size": 0.001,
                    "min_qty": 0.001,
                    "min_notional": 5.0,
                },
                mid_price=500.0,
                reset_state=True,
            )
            state_path.write_text(json.dumps(reset_state), encoding="utf-8")

            self.assertEqual(
                reset_state["futures_terminal_handoff"], pending_handoff
            )
            self.assertEqual(
                reset_state["futures_terminal_drain_history"], drain_history
            )
            self.assertEqual(
                reset_state["futures_terminal_intent_history"], intent_history
            )
            self.assertEqual(
                reset_state["futures_terminal_handoff_history"], handoff_history
            )

            acknowledged_at = datetime(
                2026, 7, 16, 0, 2, tzinfo=timezone.utc
            )
            acknowledged = _terminal_drain_ack_handoff_after_normal_event(
                args=argparse.Namespace(**current_contract),
                state_path=state_path,
                acknowledged_at=acknowledged_at,
            )
            persisted = json.loads(state_path.read_text(encoding="utf-8"))

        self.assertTrue(acknowledged)
        self.assertNotIn("futures_terminal_handoff", persisted)
        self.assertEqual(
            persisted["futures_terminal_drain_history"], drain_history
        )
        self.assertEqual(
            persisted["futures_terminal_intent_history"], intent_history
        )
        self.assertEqual(len(persisted["futures_terminal_handoff_history"]), 2)
        self.assertEqual(
            persisted["futures_terminal_handoff_history"][-1]["status"],
            "acknowledged",
        )

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
