from __future__ import annotations

import argparse
import json
import tempfile
import unittest
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from grid_optimizer.futures_run_lifecycle import (
    bind_run_contract_owner,
    run_contract_identity_from_config,
    run_contract_snapshot_from_config,
)
from grid_optimizer.loop_runner import (
    _cancel_futures_strategy_entry_orders,
    _cancel_futures_strategy_orders,
    _terminal_drain_ack_handoff_after_normal_event,
    _terminal_drain_exit_contract_from_snapshot,
    _terminal_drain_owner_integrity_digest,
    _terminal_drain_runtime_integrity_digest,
    _maybe_handle_runtime_guard,
    _resolve_loop_authoritative_run_contract,
    _suppress_place_orders_during_runtime_guard_loss_cooldown,
)
from grid_optimizer.futures_terminal_drain import (
    DrainStage,
    TerminalDrainState,
    terminal_drain_state_to_dict,
)
from grid_optimizer.futures_terminal_ownership import terminal_intent_id


class LoopRunnerRuntimeGuardFlattenTests(unittest.TestCase):
    def _bounded_args(self, tmpdir: str) -> argparse.Namespace:
        return argparse.Namespace(
            symbol="BCHUSDT",
            strategy_profile="bch-volume-v1",
            strategy_mode="hedge_best_quote_maker_volume_v1",
            per_order_notional=20.0,
            terminal_drain_max_order_notional=20.0,
            terminal_drain_loss_lease_seconds=300.0,
            terminal_drain_order_reprice_seconds=120.0,
            terminal_drain_flat_confirm_cycles=2,
            run_start_time="2026-07-16T00:00:00+00:00",
            runtime_guard_stats_start_time="2026-07-16T00:00:00+00:00",
            run_end_time="2026-07-17T00:00:00+00:00",
            max_cumulative_notional=20_000.0,
            terminal_drain_exit_policy="drain_then_preserve",
            terminal_drain_absolute_loss_budget=2.0,
            terminal_drain_max_wait_seconds=600.0,
            terminal_drain_stop_preserve_reason=None,
            state_path=str(Path(tmpdir) / "bch-state.json"),
            recv_window=5000,
            recovery_control_path=None,
        )

    def _authorize_args(
        self,
        args: argparse.Namespace,
        tmpdir: str,
    ) -> dict[str, object]:
        control, _ = bind_run_contract_owner(
            vars(args),
            activated_at=datetime(2026, 7, 16, tzinfo=timezone.utc),
        )
        control_path = Path(tmpdir) / f"{str(args.symbol).lower()}-control.json"
        control_path.write_text(json.dumps(control), encoding="utf-8")
        args.recovery_control_path = str(control_path)
        return control

    @patch("grid_optimizer.loop_runner._load_futures_runtime_guard_inputs")
    def test_bounded_loop_without_authoritative_owner_holds_before_exchange(
        self,
        mock_inputs,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            args = self._bounded_args(tmpdir)
            summary = _maybe_handle_runtime_guard(
                args=args,
                cycle=1,
                cycle_started_at=datetime(2026, 7, 16, 1, tzinfo=timezone.utc),
                summary_path=Path(tmpdir) / "events.jsonl",
            )

        self.assertEqual(summary["terminal_drain_action"], "hold_blocked")
        self.assertEqual(summary["stop_reason"], "run_contract_owner_missing")
        self.assertFalse(summary["runner_exit_allowed"])
        mock_inputs.assert_not_called()

    @patch("grid_optimizer.loop_runner._load_futures_runtime_guard_inputs")
    def test_bounded_loop_with_args_drift_holds_before_exchange(
        self,
        mock_inputs,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            args = self._bounded_args(tmpdir)
            control, _ = bind_run_contract_owner(
                vars(args),
                activated_at=datetime(2026, 7, 16, tzinfo=timezone.utc),
            )
            control_path = Path(tmpdir) / "bch-control.json"
            control_path.write_text(json.dumps(control), encoding="utf-8")
            args.recovery_control_path = str(control_path)
            args.max_cumulative_notional = 25_000.0

            summary = _maybe_handle_runtime_guard(
                args=args,
                cycle=1,
                cycle_started_at=datetime(2026, 7, 16, 1, tzinfo=timezone.utc),
                summary_path=Path(tmpdir) / "events.jsonl",
            )

        self.assertEqual(summary["terminal_drain_action"], "hold_blocked")
        self.assertEqual(summary["stop_reason"], "run_contract_args_owner_mismatch")
        self.assertFalse(summary["runner_exit_allowed"])
        mock_inputs.assert_not_called()

    def test_loop_owner_allows_recovery_to_resize_normal_per_order(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            args = self._bounded_args(tmpdir)
            control = self._authorize_args(args, tmpdir)
            control["per_order_notional"] = 8.0
            Path(args.recovery_control_path).write_text(
                json.dumps(control), encoding="utf-8"
            )

            snapshot, contract_id = _resolve_loop_authoritative_run_contract(args)

        self.assertEqual(snapshot["terminal_drain_max_order_notional"], 20.0)
        self.assertEqual(
            contract_id,
            control["futures_run_contract_owner"]["run_contract_id"],
        )

    def _terminal_owner(
        self,
        *,
        control: dict[str, object],
        started_at: datetime,
        exit_status: str,
    ) -> dict[str, object]:
        snapshot = run_contract_snapshot_from_config(control)
        contract_id = run_contract_identity_from_config(snapshot)
        decision_id = f"{snapshot['symbol']}|{contract_id}"
        drain_state = TerminalDrainState.initial(
            str(snapshot["symbol"]),
            decision_id=decision_id,
        )
        if exit_status == "stopped_clean":
            drain_state = replace(drain_state, stage=DrainStage.STOPPED_CLEAN)
        elif exit_status == "stopped_preserved":
            drain_state = replace(drain_state, stage=DrainStage.STOPPED_PRESERVED)
        owner: dict[str, object] = {
            "schema": "futures_terminal_drain_runtime_v1",
            "decision_id": decision_id,
            "run_contract_id": contract_id,
            "run_contract_snapshot": snapshot,
            "started_at": started_at.isoformat(),
            "run_outcome": "target_unmet_deadline",
            "exit_status": exit_status,
            "exit_contract": _terminal_drain_exit_contract_from_snapshot(
                snapshot,
                captured_at=started_at.isoformat(),
            ),
            "initial_frozen_long_qty": 0.0,
            "initial_frozen_short_qty": 0.0,
            "drain_state": terminal_drain_state_to_dict(drain_state),
            "intents": [],
            "active_intent_ids": [],
        }
        owner["owner_integrity_digest"] = (
            _terminal_drain_owner_integrity_digest(owner)
        )
        owner["runtime_integrity_digest"] = (
            _terminal_drain_runtime_integrity_digest(owner)
        )
        return owner

    def _pending_terminal_intent(
        self,
        *,
        control: dict[str, object],
        requested_at: datetime,
        suffix: str,
    ) -> dict[str, object]:
        snapshot = run_contract_snapshot_from_config(control)
        contract_id = run_contract_identity_from_config(snapshot)
        target = float(snapshot["max_cumulative_notional"])
        return {
            "schema": "futures_lifecycle_intent_v2",
            "intent_id": terminal_intent_id(
                symbol="BCHUSDT",
                source="competition_target_gate",
                trigger_reason="target_reached",
                run_contract_id=contract_id,
            ),
            "symbol": "BCHUSDT",
            "source": "competition_target_gate",
            "action": "lifecycle_drain",
            "trigger_reason": "target_reached",
            "requested_at": requested_at.isoformat(),
            "status": "pending",
            "exit_policy": "use_immutable_run_contract",
            "run_contract_id": contract_id,
            "run_contract_snapshot": snapshot,
            "observed": {
                "gross_notional": target + 1.0,
                "target": target,
                "realized_pnl": 0.0,
                "wear_per_10k": 0.0,
                "trade_count": 1,
                "window_start": snapshot["runtime_guard_stats_start_time"],
                "window_end": snapshot["run_end_time"],
                "query_end": requested_at.isoformat(),
            },
        }

    @staticmethod
    def _tradable_guard_result() -> argparse.Namespace:
        return argparse.Namespace(
            tradable=True,
            runtime_status="running",
            stop_triggered=False,
            primary_reason=None,
            matched_reasons=[],
            triggered_at=None,
            rolling_hourly_loss=0.0,
            rolling_hourly_gross_notional=0.0,
            rolling_hourly_loss_per_10k=0.0,
            rolling_hourly_loss_per_10k_active=False,
            cumulative_gross_notional=20_001.0,
            unrealized_loss=0.0,
        )

    @patch("grid_optimizer.loop_runner.delete_futures_order")
    @patch("grid_optimizer.loop_runner.fetch_futures_open_orders")
    def test_cancel_futures_strategy_orders_treats_unknown_order_as_complete(
        self,
        mock_open_orders,
        mock_delete_order,
    ) -> None:
        mock_open_orders.return_value = [
            {"orderId": 101, "clientOrderId": "gx-ordiusdc-entry-1"},
            {"orderId": 102, "clientOrderId": "gx-ordiusdc-exit-1"},
        ]
        mock_delete_order.side_effect = [
            RuntimeError("Binance API error -2011: Unknown order sent."),
            {"orderId": 102, "status": "CANCELED"},
        ]

        count = _cancel_futures_strategy_orders(
            symbol="ORDIUSDC",
            api_key="key",
            api_secret="secret",
            recv_window=5000,
        )

        self.assertEqual(count, 2)
        self.assertEqual(mock_delete_order.call_count, 2)

    @patch("grid_optimizer.loop_runner.delete_futures_order")
    @patch("grid_optimizer.loop_runner.fetch_futures_open_orders")
    def test_cancel_futures_strategy_orders_preserves_durable_frozen_but_cancels_prefix_spoof(
        self,
        mock_open_orders,
        mock_delete_order,
    ) -> None:
        frozen_client_id = "gx-bchu-frozenin-1-deadbeef"
        spoofed_client_id = "gx-bchu-frozenin-99-spoofed"
        mock_open_orders.return_value = [
            {
                "orderId": 101,
                "clientOrderId": frozen_client_id,
                "side": "SELL",
                "positionSide": "LONG",
                "price": "220.50",
                "origQty": "0.100",
                "executedQty": "0",
            },
            {
                "orderId": 102,
                "clientOrderId": spoofed_client_id,
                "side": "BUY",
                "positionSide": "LONG",
                "price": "220.00",
                "origQty": "0.100",
                "executedQty": "0",
            },
        ]
        state = {
            "best_quote_frozen_inventory": {
                "long_qty": 0.283,
                "short_qty": 0.0,
                "long_lots": [{"qty": 0.283, "price": 234.0}],
                "short_lots": [],
            },
            "best_quote_frozen_inventory_manual_limit": {
                "long": {
                    "requested": True,
                    "submitted": True,
                    "request_id": "frozen-limit-long-1",
                    "requested_qty": 0.1,
                }
            },
            "best_quote_volume_order_refs": {
                "101": {
                    "book": "frozen_bq",
                    "role": "frozen_inventory_manual_limit_long",
                    "side": "SELL",
                    "position_side": "LONG",
                    "client_order_id": frozen_client_id,
                    "frozen_inventory_request_id": "frozen-limit-long-1",
                }
            },
        }

        count = _cancel_futures_strategy_orders(
            symbol="BCHUSDT",
            api_key="key",
            api_secret="secret",
            recv_window=5000,
            state=state,
        )

        self.assertEqual(count, 1)
        self.assertEqual(mock_delete_order.call_count, 1)
        self.assertEqual(
            mock_delete_order.call_args.kwargs["orig_client_order_id"],
            spoofed_client_id,
        )

    @patch("grid_optimizer.loop_runner.delete_futures_order")
    @patch("grid_optimizer.loop_runner.fetch_futures_open_orders")
    def test_cancel_futures_strategy_orders_preserves_durable_frozen_when_directive_is_missing(
        self,
        mock_open_orders,
        mock_delete_order,
    ) -> None:
        frozen_client_id = "gx-bchu-frozenin-1-deadbeef"
        mock_open_orders.return_value = [
            {
                "orderId": 101,
                "clientOrderId": frozen_client_id,
                "side": "SELL",
                "positionSide": "LONG",
                "price": "220.50",
                "origQty": "0.100",
                "executedQty": "0",
            }
        ]
        state = {
            "best_quote_frozen_inventory": {
                "long_qty": 0.283,
                "short_qty": 0.0,
                "long_lots": [{"qty": 0.283, "price": 234.0}],
                "short_lots": [],
            },
            "best_quote_volume_order_refs": {
                "101": {
                    "book": "frozen_bq",
                    "role": "frozen_inventory_manual_limit_long",
                    "side": "SELL",
                    "position_side": "LONG",
                    "client_order_id": frozen_client_id,
                    "frozen_inventory_request_id": "lost-directive",
                }
            },
        }

        count = _cancel_futures_strategy_orders(
            symbol="BCHUSDT",
            api_key="key",
            api_secret="secret",
            recv_window=5000,
            state=state,
        )

        self.assertEqual(count, 0)
        mock_delete_order.assert_not_called()

    @patch("grid_optimizer.loop_runner.delete_futures_order")
    @patch("grid_optimizer.loop_runner.fetch_futures_open_orders")
    def test_cancel_futures_strategy_entry_orders_preserves_durable_frozen_but_cancels_prefix_spoof(
        self,
        mock_open_orders,
        mock_delete_order,
    ) -> None:
        frozen_client_id = "gx-bchu-frozenin-1-deadbeef"
        spoofed_client_id = "gx-bchu-frozenin-99-spoofed"
        mock_open_orders.return_value = [
            {
                "orderId": 101,
                "clientOrderId": frozen_client_id,
                "side": "SELL",
                "positionSide": "LONG",
                "reduceOnly": False,
                "price": "220.50",
                "origQty": "0.100",
                "executedQty": "0",
            },
            {
                "orderId": 102,
                "clientOrderId": spoofed_client_id,
                "side": "BUY",
                "positionSide": "LONG",
                "reduceOnly": False,
                "price": "220.00",
                "origQty": "0.100",
                "executedQty": "0",
            },
        ]
        state = {
            "best_quote_frozen_inventory": {
                "long_qty": 0.283,
                "short_qty": 0.0,
                "long_lots": [{"qty": 0.283, "price": 234.0}],
                "short_lots": [],
            },
            "best_quote_frozen_inventory_manual_limit": {
                "long": {
                    "requested": True,
                    "submitted": True,
                    "request_id": "frozen-limit-long-1",
                    "requested_qty": 0.1,
                }
            },
            "best_quote_volume_order_refs": {
                "101": {
                    "book": "frozen_bq",
                    "role": "frozen_inventory_manual_limit_long",
                    "side": "SELL",
                    "position_side": "LONG",
                    "client_order_id": frozen_client_id,
                    "frozen_inventory_request_id": "frozen-limit-long-1",
                }
            },
        }

        count = _cancel_futures_strategy_entry_orders(
            symbol="BCHUSDT",
            strategy_mode="hedge_best_quote_maker_volume_v1",
            api_key="key",
            api_secret="secret",
            recv_window=5000,
            state=state,
        )

        self.assertEqual(count, 1)
        self.assertEqual(mock_delete_order.call_count, 1)
        self.assertEqual(
            mock_delete_order.call_args.kwargs["orig_client_order_id"],
            spoofed_client_id,
        )

    @patch("grid_optimizer.loop_runner._start_futures_flatten_process")
    @patch("grid_optimizer.loop_runner.load_live_flatten_snapshot")
    @patch("grid_optimizer.loop_runner._cancel_futures_strategy_orders")
    @patch("grid_optimizer.loop_runner.load_binance_api_credentials")
    @patch("grid_optimizer.loop_runner._load_futures_runtime_guard_inputs")
    def test_runtime_guard_loss_stop_uses_cooldown_without_flatten(
        self,
        mock_inputs,
        mock_credentials,
        mock_cancel,
        mock_snapshot,
        mock_start_flatten,
    ) -> None:
        args = argparse.Namespace(
            symbol="ORDIUSDC",
            recv_window=5000,
            strategy_profile="test",
            strategy_mode="synthetic_neutral",
            runtime_guard_stats_start_time=None,
            run_start_time=None,
            run_end_time=None,
            rolling_hourly_loss_limit=8.0,
            runtime_guard_loss_recovery_enabled=True,
            runtime_guard_loss_recovery_cooldown_seconds=180.0,
            runtime_guard_loss_recovery_max_1m_amplitude_ratio=0.012,
            runtime_guard_loss_recovery_max_3m_amplitude_ratio=0.025,
            max_cumulative_notional=None,
            max_actual_net_notional=None,
            max_synthetic_drift_notional=None,
            auto_regime_enabled=False,
            state_path="output/ordiusdc_loop_state.json",
        )
        now = datetime(2026, 5, 3, 0, 18, tzinfo=timezone.utc)
        mock_inputs.return_value = (
            1000.0,
            [{"ts": "2026-05-03T00:16:00+00:00", "net_pnl": -9.0}],
            None,
        )
        mock_credentials.return_value = ("key", "secret")
        mock_cancel.return_value = 0
        mock_snapshot.return_value = {
            "orders": [{"side": "SELL", "quantity": 1.0}],
            "bid_price": 5.0,
            "ask_price": 5.001,
            "long_qty": 1.0,
            "short_qty": 0.0,
            "net_qty": 1.0,
        }
        mock_start_flatten.return_value = {"started": True, "already_running": False}

        summary = _maybe_handle_runtime_guard(
            args=args,
            cycle=1,
            cycle_started_at=now,
            summary_path=Path("output/ordiusdc_loop_events.jsonl"),
        )

        self.assertIsNotNone(summary)
        self.assertEqual(summary["runtime_status"], "cooldown")
        self.assertFalse(summary["stop_triggered"])
        self.assertEqual(summary["stop_reason"], "rolling_hourly_loss_cooling_down")
        self.assertFalse(summary["flatten_started"])
        mock_snapshot.assert_called_once_with(
            "ORDIUSDC",
            "key",
            "secret",
            allow_loss=True,
            max_loss_ratio=None,
            preserve_long_qty=0.0,
            preserve_short_qty=0.0,
        )
        mock_start_flatten.assert_not_called()

    @patch("grid_optimizer.loop_runner._load_futures_runtime_guard_inputs")
    def test_runtime_guard_tradable_after_cooldown_clears_submit_block(self, mock_inputs) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            now = datetime(2026, 7, 13, 10, 30, tzinfo=timezone.utc)
            stopped_at = now - timedelta(minutes=20)
            state_path = Path(tmpdir) / "state.json"
            plan_path = Path(tmpdir) / "plan.json"
            state_path.write_text(
                json.dumps(
                    {
                        "runtime_guard_loss_recovery": {
                            "stopped_at": stopped_at.isoformat(),
                            "last_reason": "rolling_hourly_loss_limit_hit",
                            "last_rolling_hourly_loss": 20.8,
                        }
                    }
                ),
                encoding="utf-8",
            )
            plan_path.write_text(
                json.dumps(
                    {
                        "generated_at": now.isoformat(),
                        "mid_price": 1.0,
                        "actual_net_notional": 0.0,
                        "unrealized_pnl": 0.0,
                    }
                ),
                encoding="utf-8",
            )
            args = argparse.Namespace(
                symbol="ARXUSDT",
                strategy_profile="test",
                strategy_mode="hedge_best_quote_maker_volume_v1",
                runtime_guard_stats_start_time=None,
                run_start_time=None,
                run_end_time=None,
                rolling_hourly_loss_limit=20.0,
                rolling_hourly_loss_per_10k_limit=None,
                runtime_guard_loss_recovery_enabled=True,
                runtime_guard_loss_recovery_cooldown_seconds=180.0,
                max_cumulative_notional=None,
                max_actual_net_notional=None,
                max_synthetic_drift_notional=None,
                max_unrealized_loss=None,
                state_path=str(state_path),
                plan_json=str(plan_path),
            )
            mock_inputs.return_value = (1000.0, [], None)

            summary = _maybe_handle_runtime_guard(
                args=args,
                cycle=1,
                cycle_started_at=now,
                summary_path=Path(tmpdir) / "events.jsonl",
            )

            self.assertIsNone(summary)
            recovery = json.loads(state_path.read_text(encoding="utf-8"))["runtime_guard_loss_recovery"]
            self.assertEqual(recovery["recovered_at"], now.isoformat())
            self.assertEqual(recovery["last_reason"], "runtime_guard_tradable_after_loss_cooldown")
            self.assertEqual(recovery["cooldown_elapsed_seconds"], 1200.0)

    @patch("grid_optimizer.loop_runner._handle_terminal_drain_round")
    @patch("grid_optimizer.loop_runner.evaluate_runtime_guards")
    @patch("grid_optimizer.loop_runner._load_futures_runtime_guard_inputs")
    def test_new_run_archives_completed_terminal_owner_and_releases_trading(
        self,
        mock_inputs,
        mock_evaluate,
        mock_terminal_drain,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            now = datetime(2026, 7, 16, 1, 0, tzinfo=timezone.utc)
            state_path = Path(tmpdir) / "state.json"
            plan_path = Path(tmpdir) / "plan.json"
            intent_path = Path(tmpdir) / "bchusdt_terminal_intent.json"
            args = argparse.Namespace(
                symbol="BCHUSDT",
                strategy_profile="bchusdt_altcoins_probe_v1",
                strategy_mode="hedge_best_quote_maker_volume_v1",
                runtime_guard_stats_start_time="2026-07-16T09:00:00+08:00",
                run_start_time="2026-07-16T09:00:00+08:00",
                run_end_time="2026-07-16T10:00:00+08:00",
                rolling_hourly_loss_limit=None,
                rolling_hourly_loss_per_10k_limit=None,
                rolling_hourly_loss_per_10k_min_notional=None,
                runtime_guard_loss_recovery_enabled=True,
                max_cumulative_notional=20_000.0,
                max_actual_net_notional=None,
                max_synthetic_drift_notional=None,
                max_unrealized_loss=None,
                terminal_drain_exit_policy="drain_then_preserve",
                terminal_drain_absolute_loss_budget=5.0,
                terminal_drain_max_wait_seconds=900.0,
                state_path=str(state_path),
                plan_json=str(plan_path),
            )
            old_control = {**vars(args), "strategy_profile": "bchusdt_old_probe"}
            old_owner = self._terminal_owner(
                control=old_control,
                started_at=now - timedelta(hours=2),
                exit_status="stopped_preserved",
            )
            old_intent = self._pending_terminal_intent(
                control=old_control,
                requested_at=now,
                suffix="completed-old-run",
            )
            old_intent["status"] = "stopped_preserved"
            state_path.write_text(
                json.dumps(
                    {
                        "futures_terminal_drain": old_owner,
                        "futures_terminal_intent": old_intent,
                    }
                ),
                encoding="utf-8",
            )
            intent_path.write_text(
                json.dumps(old_intent),
                encoding="utf-8",
            )
            self._authorize_args(args, tmpdir)
            mock_inputs.return_value = (0.0, [], None)
            mock_evaluate.return_value = argparse.Namespace(
                tradable=True,
                runtime_status="running",
                stop_triggered=False,
                primary_reason=None,
                matched_reasons=[],
                triggered_at=None,
                rolling_hourly_loss=0.0,
                rolling_hourly_gross_notional=0.0,
                rolling_hourly_loss_per_10k=0.0,
                rolling_hourly_loss_per_10k_active=False,
                cumulative_gross_notional=0.0,
                unrealized_loss=0.0,
            )

            summary = _maybe_handle_runtime_guard(
                args=args,
                cycle=1,
                cycle_started_at=now,
                summary_path=Path(tmpdir) / "events.jsonl",
            )
            persisted = json.loads(state_path.read_text(encoding="utf-8"))

        self.assertIsNone(summary)
        self.assertNotIn("futures_terminal_drain", persisted)
        self.assertEqual(len(persisted["futures_terminal_drain_history"]), 1)
        self.assertEqual(len(persisted["futures_terminal_intent_history"]), 1)
        self.assertNotIn("futures_terminal_intent", persisted)
        self.assertFalse(intent_path.exists())
        archived = persisted["futures_terminal_drain_history"][0]
        self.assertEqual(archived["decision_id"], old_owner["decision_id"])
        self.assertEqual(archived["replaced_by_decision_id"].split("|")[0], "BCHUSDT")
        self.assertEqual(persisted["futures_terminal_handoff"]["status"], "pending")
        self.assertEqual(
            persisted["futures_terminal_handoff"]["from_decision_id"],
            old_owner["decision_id"],
        )
        mock_terminal_drain.assert_not_called()

    def test_normal_event_acknowledges_terminal_handoff_once(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "state.json"
            current_control = {
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
            old_control = {**current_control, "strategy_profile": "old_profile"}
            old_snapshot = run_contract_snapshot_from_config(old_control)
            old_contract_id = run_contract_identity_from_config(old_snapshot)
            current_contract_id = run_contract_identity_from_config(current_control)
            state_path.write_text(
                json.dumps(
                    {
                        "futures_terminal_handoff": {
                            "schema": "futures_terminal_handoff_v1",
                            "symbol": "BCHUSDT",
                            "status": "pending",
                            "from_decision_id": f"BCHUSDT|{old_contract_id}",
                            "to_decision_id": f"BCHUSDT|{current_contract_id}",
                            "run_contract_id": old_contract_id,
                            "run_contract_snapshot": old_snapshot,
                            "created_at": "2026-07-16T00:01:00+00:00",
                        }
                    }
                ),
                encoding="utf-8",
            )
            acknowledged_at = datetime(2026, 7, 16, 0, 2, tzinfo=timezone.utc)

            first = _terminal_drain_ack_handoff_after_normal_event(
                args=argparse.Namespace(**current_control),
                state_path=state_path,
                acknowledged_at=acknowledged_at,
            )
            second = _terminal_drain_ack_handoff_after_normal_event(
                args=argparse.Namespace(**current_control),
                state_path=state_path,
                acknowledged_at=acknowledged_at + timedelta(seconds=1),
            )
            persisted = json.loads(state_path.read_text(encoding="utf-8"))
            handoff = persisted["futures_terminal_handoff_history"][-1]

        self.assertTrue(first)
        self.assertFalse(second)
        self.assertNotIn("futures_terminal_handoff", persisted)
        self.assertEqual(handoff["status"], "acknowledged")
        self.assertEqual(handoff["acknowledged_at"], acknowledged_at.isoformat())

    @patch("grid_optimizer.loop_runner.evaluate_runtime_guards")
    @patch("grid_optimizer.loop_runner._load_futures_runtime_guard_inputs")
    def test_invalid_pending_handoff_blocks_before_normal_planning_inputs(
        self,
        mock_inputs,
        mock_evaluate,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "state.json"
            state_path.write_text(
                json.dumps(
                    {
                        "futures_terminal_handoff": {
                            "schema": "corrupt",
                            "symbol": "BCHUSDT",
                            "status": "pending",
                        }
                    }
                ),
                encoding="utf-8",
            )
            summary = _maybe_handle_runtime_guard(
                args=argparse.Namespace(
                    symbol="BCHUSDT",
                    state_path=str(state_path),
                ),
                cycle=1,
                cycle_started_at=datetime(
                    2026,
                    7,
                    16,
                    1,
                    0,
                    tzinfo=timezone.utc,
                ),
                summary_path=Path(tmpdir) / "events.jsonl",
            )

        self.assertEqual(summary["runtime_status"], "exit_blocked")
        self.assertFalse(summary["runner_exit_allowed"])
        self.assertEqual(summary["stop_reason"], "terminal_handoff_schema_invalid")
        mock_inputs.assert_not_called()
        mock_evaluate.assert_not_called()

    @patch("grid_optimizer.loop_runner._handle_terminal_drain_round")
    @patch("grid_optimizer.loop_runner.evaluate_runtime_guards")
    @patch("grid_optimizer.loop_runner._load_futures_runtime_guard_inputs")
    def test_new_run_must_finish_incomplete_terminal_owner_before_trading(
        self,
        mock_inputs,
        mock_evaluate,
        mock_terminal_drain,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            now = datetime(2026, 7, 16, 1, 0, tzinfo=timezone.utc)
            state_path = Path(tmpdir) / "state.json"
            plan_path = Path(tmpdir) / "plan.json"
            state_path.write_text(
                json.dumps(
                    {
                        "futures_terminal_drain": {
                            "schema": "futures_terminal_drain_runtime_v1",
                            "decision_id": "BCHUSDT|old-run",
                            "run_outcome": "target_unmet_deadline",
                            "target_value": 20_000.0,
                            "achieved_value": 1_817.0,
                            "target_shortfall": 18_183.0,
                            "exit_status": "exit_blocked",
                            "drain_state": {
                                "symbol": "BCHUSDT",
                                "decision_id": "BCHUSDT|old-run",
                                "phase": "exit_blocked",
                            },
                        }
                    }
                ),
                encoding="utf-8",
            )
            args = argparse.Namespace(
                symbol="BCHUSDT",
                strategy_profile="bchusdt_altcoins_probe_v1",
                strategy_mode="hedge_best_quote_maker_volume_v1",
                runtime_guard_stats_start_time="2026-07-16T09:00:00+08:00",
                run_start_time="2026-07-16T09:00:00+08:00",
                run_end_time="2026-07-16T10:00:00+08:00",
                rolling_hourly_loss_limit=None,
                rolling_hourly_loss_per_10k_limit=None,
                rolling_hourly_loss_per_10k_min_notional=None,
                runtime_guard_loss_recovery_enabled=True,
                max_cumulative_notional=20_000.0,
                max_actual_net_notional=None,
                max_synthetic_drift_notional=None,
                max_unrealized_loss=None,
                terminal_drain_exit_policy="drain_then_preserve",
                terminal_drain_absolute_loss_budget=5.0,
                terminal_drain_max_wait_seconds=900.0,
                state_path=str(state_path),
                plan_json=str(plan_path),
            )
            self._authorize_args(args, tmpdir)
            mock_inputs.return_value = (0.0, [], None)
            mock_evaluate.return_value = argparse.Namespace(
                tradable=True,
                runtime_status="running",
                stop_triggered=False,
                primary_reason=None,
                matched_reasons=[],
                triggered_at=None,
                rolling_hourly_loss=0.0,
                rolling_hourly_gross_notional=0.0,
                rolling_hourly_loss_per_10k=0.0,
                rolling_hourly_loss_per_10k_active=False,
                cumulative_gross_notional=0.0,
                unrealized_loss=0.0,
            )
            mock_terminal_drain.return_value = {
                "runtime_status": "exit_blocked",
                "stop_triggered": False,
                "runner_exit_allowed": False,
                "terminal_drain_action": "hold_blocked",
            }

            summary = _maybe_handle_runtime_guard(
                args=args,
                cycle=1,
                cycle_started_at=now,
                summary_path=Path(tmpdir) / "events.jsonl",
            )

        self.assertEqual(summary["terminal_drain_action"], "hold_blocked")
        mock_terminal_drain.assert_called_once()
        owner_state = mock_terminal_drain.call_args.kwargs["state"]["futures_terminal_drain"]
        self.assertEqual(owner_state["decision_id"], "BCHUSDT|old-run")

    @patch("grid_optimizer.loop_runner._handle_terminal_drain_round")
    @patch("grid_optimizer.loop_runner.evaluate_runtime_guards")
    @patch("grid_optimizer.loop_runner._load_futures_runtime_guard_inputs")
    def test_new_run_auto_handoffs_after_prior_owner_finishes_without_second_restart(
        self,
        mock_inputs,
        mock_evaluate,
        mock_terminal_drain,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            now = datetime(2026, 7, 16, 1, 0, tzinfo=timezone.utc)
            state_path = Path(tmpdir) / "state.json"
            state_path.write_text(
                json.dumps(
                    {
                        "futures_terminal_drain": {
                            "schema": "futures_terminal_drain_runtime_v1",
                            "decision_id": "BCHUSDT|old-run",
                            "run_outcome": "target_unmet_deadline",
                            "exit_status": "exit_blocked",
                        }
                    }
                ),
                encoding="utf-8",
            )
            args = argparse.Namespace(
                symbol="BCHUSDT",
                strategy_profile="bchusdt_altcoins_probe_v2",
                strategy_mode="hedge_best_quote_maker_volume_v1",
                runtime_guard_stats_start_time="2026-07-16T09:00:00+08:00",
                run_start_time="2026-07-16T09:00:00+08:00",
                run_end_time="2026-07-16T10:00:00+08:00",
                rolling_hourly_loss_limit=None,
                rolling_hourly_loss_per_10k_limit=None,
                rolling_hourly_loss_per_10k_min_notional=None,
                runtime_guard_loss_recovery_enabled=True,
                max_cumulative_notional=20_000.0,
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
                state_path=str(state_path),
                plan_json=str(Path(tmpdir) / "plan.json"),
            )
            old_control = {**vars(args), "strategy_profile": "bchusdt_altcoins_probe_v1"}
            old_owner = self._terminal_owner(
                control=old_control,
                started_at=now - timedelta(hours=2),
                exit_status="exit_blocked",
            )
            state_path.write_text(
                json.dumps({"futures_terminal_drain": old_owner}),
                encoding="utf-8",
            )
            self._authorize_args(args, tmpdir)
            mock_inputs.return_value = (0.0, [], None)
            mock_evaluate.return_value = argparse.Namespace(
                tradable=True,
                runtime_status="running",
                stop_triggered=False,
                primary_reason=None,
                matched_reasons=[],
                triggered_at=None,
                rolling_hourly_loss=0.0,
                rolling_hourly_gross_notional=0.0,
                rolling_hourly_loss_per_10k=0.0,
                rolling_hourly_loss_per_10k_active=False,
                cumulative_gross_notional=0.0,
                unrealized_loss=0.0,
            )

            def complete_old_owner(**kwargs):
                owner = kwargs["state"]["futures_terminal_drain"]
                owner["exit_status"] = "stopped_preserved"
                owner["drain_state"]["stage"] = "stopped_preserved"
                owner["runtime_integrity_digest"] = (
                    _terminal_drain_runtime_integrity_digest(owner)
                )
                kwargs["state_path"].write_text(
                    json.dumps(kwargs["state"]),
                    encoding="utf-8",
                )
                return {
                    "runtime_status": "stopped",
                    "stop_triggered": True,
                    "runner_exit_allowed": True,
                    "exit_status": "stopped_preserved",
                    "terminal_drain_action": "stop_preserve",
                }

            mock_terminal_drain.side_effect = complete_old_owner

            summary = _maybe_handle_runtime_guard(
                args=args,
                cycle=1,
                cycle_started_at=now,
                summary_path=Path(tmpdir) / "events.jsonl",
            )
            next_cycle_summary = _maybe_handle_runtime_guard(
                args=args,
                cycle=2,
                cycle_started_at=now + timedelta(seconds=1),
                summary_path=Path(tmpdir) / "events.jsonl",
            )
            persisted = json.loads(state_path.read_text(encoding="utf-8"))

        self.assertFalse(summary["stop_triggered"])
        self.assertFalse(summary["runner_exit_allowed"])
        self.assertEqual(summary["runtime_status"], "handoff_pending")
        self.assertTrue(summary["terminal_owner_handoff_pending"])
        self.assertIsNone(next_cycle_summary)
        self.assertEqual(mock_terminal_drain.call_count, 1)
        self.assertNotIn("futures_terminal_drain", persisted)
        self.assertEqual(
            persisted["futures_terminal_drain_history"][0]["decision_id"],
            old_owner["decision_id"],
        )

    @patch("grid_optimizer.loop_runner._handle_terminal_drain_round")
    @patch("grid_optimizer.loop_runner.evaluate_runtime_guards")
    @patch("grid_optimizer.loop_runner._load_futures_runtime_guard_inputs")
    def test_late_pending_intent_merges_into_active_same_contract_owner_before_resume(
        self,
        mock_inputs,
        mock_evaluate,
        mock_terminal_drain,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            now = datetime(2026, 7, 16, 1, 0, tzinfo=timezone.utc)
            args = self._bounded_args(tmpdir)
            control = self._authorize_args(args, tmpdir)
            owner = self._terminal_owner(
                control=control,
                started_at=now - timedelta(minutes=1),
                exit_status="exiting",
            )
            state_path = Path(args.state_path)
            state_path.write_text(
                json.dumps({"futures_terminal_drain": owner}),
                encoding="utf-8",
            )
            intent_path = Path(tmpdir) / "bchusdt_terminal_intent.json"
            intent = self._pending_terminal_intent(
                control=control,
                requested_at=now,
                suffix="active-owner",
            )
            intent_path.write_text(json.dumps(intent), encoding="utf-8")
            mock_inputs.return_value = (20_001.0, [], None)
            mock_evaluate.return_value = self._tradable_guard_result()
            mock_terminal_drain.return_value = {
                "runtime_status": "draining",
                "stop_triggered": False,
                "runner_exit_allowed": False,
                "terminal_drain_action": "block_entry",
            }

            summary = _maybe_handle_runtime_guard(
                args=args,
                cycle=1,
                cycle_started_at=now,
                summary_path=Path(tmpdir) / "events.jsonl",
            )
            persisted = json.loads(state_path.read_text(encoding="utf-8"))
            persisted_intent = json.loads(intent_path.read_text(encoding="utf-8"))

        self.assertEqual(summary["runtime_status"], "draining")
        self.assertEqual(
            persisted["futures_terminal_drain"]["decision_id"],
            owner["decision_id"],
        )
        self.assertNotIn("futures_terminal_drain_history", persisted)
        self.assertEqual(persisted["futures_terminal_intent"]["status"], "accepted")
        self.assertEqual(
            persisted["futures_terminal_intent"]["decision_id"],
            owner["decision_id"],
        )
        self.assertEqual(persisted_intent["status"], "accepted")
        resumed_state = mock_terminal_drain.call_args.kwargs["state"]
        self.assertEqual(resumed_state["futures_terminal_intent"]["status"], "accepted")

    @patch("grid_optimizer.loop_runner._handle_terminal_drain_round")
    @patch("grid_optimizer.loop_runner.evaluate_runtime_guards")
    @patch("grid_optimizer.loop_runner._load_futures_runtime_guard_inputs")
    def test_late_pending_intent_terminalizes_against_completed_same_contract_owner(
        self,
        mock_inputs,
        mock_evaluate,
        mock_terminal_drain,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            now = datetime(2026, 7, 16, 1, 0, tzinfo=timezone.utc)
            args = self._bounded_args(tmpdir)
            control = self._authorize_args(args, tmpdir)
            owner = self._terminal_owner(
                control=control,
                started_at=now - timedelta(minutes=2),
                exit_status="stopped_preserved",
            )
            state_path = Path(args.state_path)
            state_path.write_text(
                json.dumps({"futures_terminal_drain": owner}),
                encoding="utf-8",
            )
            intent_path = Path(tmpdir) / "bchusdt_terminal_intent.json"
            intent = self._pending_terminal_intent(
                control=control,
                requested_at=now,
                suffix="completed-owner",
            )
            intent_path.write_text(json.dumps(intent), encoding="utf-8")
            mock_inputs.return_value = (20_001.0, [], None)
            mock_evaluate.return_value = self._tradable_guard_result()
            mock_terminal_drain.return_value = {
                "runtime_status": "stopped",
                "stop_triggered": True,
                "runner_exit_allowed": True,
                "terminal_drain_action": "stop_preserve",
                "exit_status": "stopped_preserved",
            }

            summary = _maybe_handle_runtime_guard(
                args=args,
                cycle=1,
                cycle_started_at=now,
                summary_path=Path(tmpdir) / "events.jsonl",
            )
            persisted = json.loads(state_path.read_text(encoding="utf-8"))
            persisted_intent = json.loads(intent_path.read_text(encoding="utf-8"))

        self.assertTrue(summary["runner_exit_allowed"])
        self.assertEqual(
            persisted["futures_terminal_drain"]["decision_id"],
            owner["decision_id"],
        )
        self.assertNotIn("futures_terminal_drain_history", persisted)
        self.assertEqual(
            persisted["futures_terminal_intent"]["status"],
            "stopped_preserved",
        )
        self.assertEqual(persisted_intent["status"], "stopped_preserved")
        resumed_state = mock_terminal_drain.call_args.kwargs["state"]
        self.assertEqual(
            resumed_state["futures_terminal_intent"]["status"],
            "stopped_preserved",
        )

    @patch("grid_optimizer.loop_runner._handle_terminal_drain_round")
    @patch("grid_optimizer.loop_runner.evaluate_runtime_guards")
    @patch("grid_optimizer.loop_runner._load_futures_runtime_guard_inputs")
    def test_external_terminal_intent_is_durably_accepted_by_single_loop_owner(
        self,
        mock_inputs,
        mock_evaluate,
        mock_terminal_drain,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            now = datetime(2026, 7, 16, 1, 0, tzinfo=timezone.utc)
            state_path = Path(tmpdir) / "bchusdt_loop_state.json"
            plan_path = Path(tmpdir) / "plan.json"
            intent_path = Path(tmpdir) / "bchusdt_terminal_intent.json"
            state_path.write_text("{}", encoding="utf-8")
            args = argparse.Namespace(
                symbol="BCHUSDT",
                strategy_profile="bchusdt_altcoins_probe_v1",
                strategy_mode="hedge_best_quote_maker_volume_v1",
                runtime_guard_stats_start_time="2026-07-16T09:00:00+08:00",
                run_start_time="2026-07-16T09:00:00+08:00",
                run_end_time="2026-07-16T10:00:00+08:00",
                rolling_hourly_loss_limit=None,
                rolling_hourly_loss_per_10k_limit=None,
                rolling_hourly_loss_per_10k_min_notional=None,
                runtime_guard_loss_recovery_enabled=True,
                max_cumulative_notional=20_000.0,
                max_actual_net_notional=None,
                max_synthetic_drift_notional=None,
                max_unrealized_loss=None,
                terminal_drain_exit_policy="drain_then_preserve",
                terminal_drain_absolute_loss_budget=5.0,
                terminal_drain_max_wait_seconds=900.0,
                state_path=str(state_path),
                plan_json=str(plan_path),
            )
            intent_path.write_text(
                json.dumps(
                    {
                        "schema": "futures_lifecycle_intent_v2",
                        "intent_id": terminal_intent_id(
                            symbol="BCHUSDT",
                            source="competition_target_gate",
                            trigger_reason="target_reached",
                            run_contract_id=run_contract_identity_from_config(
                                vars(args)
                            ),
                        ),
                        "symbol": "BCHUSDT",
                        "source": "competition_target_gate",
                        "action": "lifecycle_drain",
                        "trigger_reason": "target_reached",
                        "requested_at": now.isoformat(),
                        "status": "pending",
                        "exit_policy": "use_immutable_run_contract",
                        "run_contract_id": run_contract_identity_from_config(vars(args)),
                        "run_contract_snapshot": run_contract_snapshot_from_config(vars(args)),
                        "observed": {
                            "gross_notional": 20_001.0,
                            "target": 20_000.0,
                            "realized_pnl": 0.0,
                            "wear_per_10k": 0.0,
                            "trade_count": 1,
                            "window_start": "2026-07-16T01:00:00+00:00",
                            "window_end": "2026-07-16T02:00:00+00:00",
                            "query_end": "2026-07-16T01:00:00+00:00",
                        },
                    }
                ),
                encoding="utf-8",
            )
            # Local audit may contain late/out-of-window fills.  The accepted
            # lifecycle intent must use its frozen exchange observation exactly.
            self._authorize_args(args, tmpdir)
            mock_inputs.return_value = (99_999.0, [], None)
            mock_evaluate.return_value = argparse.Namespace(
                tradable=True,
                runtime_status="running",
                stop_triggered=False,
                primary_reason=None,
                matched_reasons=[],
                triggered_at=None,
                rolling_hourly_loss=0.0,
                rolling_hourly_gross_notional=0.0,
                rolling_hourly_loss_per_10k=0.0,
                rolling_hourly_loss_per_10k_active=False,
                cumulative_gross_notional=19_999.0,
                unrealized_loss=0.0,
            )
            mock_terminal_drain.return_value = {
                "runtime_status": "draining",
                "stop_triggered": False,
                "runner_exit_allowed": False,
                "terminal_drain_action": "block_entry",
                "run_outcome": "target_reached",
            }

            summary = _maybe_handle_runtime_guard(
                args=args,
                cycle=1,
                cycle_started_at=now,
                summary_path=Path(tmpdir) / "events.jsonl",
            )
            persisted_state = json.loads(state_path.read_text(encoding="utf-8"))
            persisted_intent = json.loads(intent_path.read_text(encoding="utf-8"))

        self.assertEqual(summary["terminal_drain_action"], "block_entry")
        self.assertEqual(
            persisted_state["futures_terminal_intent"]["intent_id"],
            terminal_intent_id(
                symbol="BCHUSDT",
                source="competition_target_gate",
                trigger_reason="target_reached",
                run_contract_id=run_contract_identity_from_config(vars(args)),
            ),
        )
        self.assertEqual(persisted_state["futures_terminal_intent"]["status"], "accepted")
        self.assertEqual(persisted_intent["status"], "accepted")
        call = mock_terminal_drain.call_args.kwargs
        self.assertEqual(call["runtime_guard_result"].primary_reason, "terminal_intent_target_reached")
        self.assertEqual(call["cumulative_gross_notional"], 20_001.0)

    @patch("grid_optimizer.loop_runner._handle_terminal_drain_round")
    @patch("grid_optimizer.loop_runner.evaluate_runtime_guards")
    @patch("grid_optimizer.loop_runner._load_futures_runtime_guard_inputs")
    def test_external_terminal_intent_resumes_its_frozen_contract_after_runner_config_changes(
        self,
        mock_inputs,
        mock_evaluate,
        mock_terminal_drain,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            now = datetime(2026, 7, 16, 1, 0, tzinfo=timezone.utc)
            state_path = Path(tmpdir) / "bchusdt_loop_state.json"
            intent_path = Path(tmpdir) / "bchusdt_terminal_intent.json"
            state_path.write_text("{}", encoding="utf-8")
            old_contract = {
                "symbol": "BCHUSDT",
                "strategy_profile": "bchusdt_altcoins_probe_v1",
                "strategy_mode": "hedge_best_quote_maker_volume_v1",
                "per_order_notional": 20.0,
                "runtime_guard_stats_start_time": "2026-07-16T08:00:00+08:00",
                "run_start_time": "2026-07-16T08:00:00+08:00",
                "run_end_time": "2026-07-16T09:00:00+08:00",
                "max_cumulative_notional": 10_000.0,
                "terminal_drain_exit_policy": "drain_then_preserve",
                "terminal_drain_absolute_loss_budget": 2.0,
                "terminal_drain_max_wait_seconds": 600.0,
            }
            old_snapshot = run_contract_snapshot_from_config(old_contract)
            old_contract_id = run_contract_identity_from_config(old_snapshot)
            intent_path.write_text(
                json.dumps(
                    {
                        "schema": "futures_lifecycle_intent_v2",
                        "intent_id": terminal_intent_id(
                            symbol="BCHUSDT",
                            source="competition_target_gate",
                            trigger_reason="target_reached",
                            run_contract_id=old_contract_id,
                        ),
                        "symbol": "BCHUSDT",
                        "source": "competition_target_gate",
                        "action": "lifecycle_drain",
                        "trigger_reason": "target_reached",
                        "requested_at": now.isoformat(),
                        "status": "pending",
                        "exit_policy": "use_immutable_run_contract",
                        "run_contract_id": old_contract_id,
                        "run_contract_snapshot": old_snapshot,
                        "observed": {
                            "gross_notional": 10_001.0,
                            "target": 10_000.0,
                            "realized_pnl": 0.0,
                            "wear_per_10k": 0.0,
                            "trade_count": 1,
                            "window_start": "2026-07-16T00:00:00+00:00",
                            "window_end": "2026-07-16T01:00:00+00:00",
                            "query_end": "2026-07-16T01:00:00+00:00",
                        },
                    }
                ),
                encoding="utf-8",
            )
            args = argparse.Namespace(
                symbol="BCHUSDT",
                strategy_profile="bchusdt_altcoins_probe_v2",
                strategy_mode="hedge_best_quote_maker_volume_v1",
                runtime_guard_stats_start_time="2026-07-16T09:00:00+08:00",
                run_start_time="2026-07-16T09:00:00+08:00",
                run_end_time="2026-07-16T10:00:00+08:00",
                rolling_hourly_loss_limit=None,
                rolling_hourly_loss_per_10k_limit=None,
                rolling_hourly_loss_per_10k_min_notional=None,
                runtime_guard_loss_recovery_enabled=True,
                max_cumulative_notional=20_000.0,
                max_actual_net_notional=None,
                max_synthetic_drift_notional=None,
                max_unrealized_loss=None,
                terminal_drain_exit_policy="drain_then_preserve",
                terminal_drain_absolute_loss_budget=5.0,
                terminal_drain_max_wait_seconds=900.0,
                state_path=str(state_path),
                plan_json=str(Path(tmpdir) / "plan.json"),
            )
            self._authorize_args(args, tmpdir)
            mock_inputs.return_value = (19_999.0, [], None)
            mock_evaluate.return_value = argparse.Namespace(
                tradable=True,
                runtime_status="running",
                stop_triggered=False,
                primary_reason=None,
                matched_reasons=[],
                triggered_at=None,
                rolling_hourly_loss=0.0,
                rolling_hourly_gross_notional=0.0,
                rolling_hourly_loss_per_10k=0.0,
                rolling_hourly_loss_per_10k_active=False,
                cumulative_gross_notional=19_999.0,
                unrealized_loss=0.0,
            )
            mock_terminal_drain.return_value = {
                "runtime_status": "draining",
                "stop_triggered": False,
                "runner_exit_allowed": False,
                "terminal_drain_action": "block_entry",
            }

            summary = _maybe_handle_runtime_guard(
                args=args,
                cycle=1,
                cycle_started_at=now,
                summary_path=Path(tmpdir) / "events.jsonl",
            )
            persisted_state = json.loads(state_path.read_text(encoding="utf-8"))
            persisted_intent = json.loads(intent_path.read_text(encoding="utf-8"))

        self.assertEqual(summary["runtime_status"], "draining")
        self.assertFalse(summary["stop_triggered"])
        self.assertFalse(summary["runner_exit_allowed"])
        self.assertEqual(persisted_state["futures_terminal_intent"]["status"], "accepted")
        self.assertEqual(persisted_intent["status"], "accepted")
        self.assertEqual(persisted_intent["run_contract_id"], old_contract_id)
        call = mock_terminal_drain.call_args.kwargs
        self.assertEqual(call["args"].strategy_profile, "bchusdt_altcoins_probe_v1")
        self.assertEqual(call["args"].terminal_drain_absolute_loss_budget, 2.0)
        self.assertEqual(call["args"].terminal_drain_max_wait_seconds, 600.0)
        self.assertEqual(call["runtime_guard_config"].max_cumulative_notional, 10_000.0)
        self.assertEqual(
            run_contract_identity_from_config(vars(call["args"])),
            old_contract_id,
        )

    @patch("grid_optimizer.loop_runner._handle_terminal_drain_round")
    @patch("grid_optimizer.loop_runner.evaluate_runtime_guards")
    @patch("grid_optimizer.loop_runner._load_futures_runtime_guard_inputs")
    def test_external_terminal_intent_tamper_blocks_without_exchange_effects(
        self,
        mock_inputs,
        mock_evaluate,
        mock_terminal_drain,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            now = datetime(2026, 7, 16, 1, 0, tzinfo=timezone.utc)
            state_path = Path(tmpdir) / "bchusdt_loop_state.json"
            intent_path = Path(tmpdir) / "bchusdt_terminal_intent.json"
            state_path.write_text("{}", encoding="utf-8")
            args = argparse.Namespace(
                symbol="BCHUSDT",
                strategy_profile="bchusdt_altcoins_probe_v1",
                strategy_mode="hedge_best_quote_maker_volume_v1",
                per_order_notional=20.0,
                runtime_guard_stats_start_time="2026-07-16T09:00:00+08:00",
                run_start_time="2026-07-16T09:00:00+08:00",
                run_end_time="2026-07-16T10:00:00+08:00",
                rolling_hourly_loss_limit=None,
                rolling_hourly_loss_per_10k_limit=None,
                rolling_hourly_loss_per_10k_min_notional=None,
                runtime_guard_loss_recovery_enabled=True,
                max_cumulative_notional=20_000.0,
                max_actual_net_notional=None,
                max_synthetic_drift_notional=None,
                max_unrealized_loss=None,
                terminal_drain_exit_policy="drain_then_preserve",
                terminal_drain_absolute_loss_budget=5.0,
                terminal_drain_max_wait_seconds=900.0,
                state_path=str(state_path),
                plan_json=str(Path(tmpdir) / "plan.json"),
            )
            snapshot = run_contract_snapshot_from_config(vars(args))
            contract_id = run_contract_identity_from_config(snapshot)
            snapshot["terminal_drain_absolute_loss_budget"] = 500.0
            intent_path.write_text(
                json.dumps(
                    {
                        "schema": "futures_lifecycle_intent_v2",
                        "intent_id": "BCHUSDT-tampered-intent",
                        "symbol": "BCHUSDT",
                        "source": "competition_target_gate",
                        "action": "lifecycle_drain",
                        "trigger_reason": "target_reached",
                        "requested_at": now.isoformat(),
                        "status": "pending",
                        "exit_policy": "use_immutable_run_contract",
                        "run_contract_id": contract_id,
                        "run_contract_snapshot": snapshot,
                        "observed": {"gross_notional": 20_001.0, "target": 20_000.0},
                    }
                ),
                encoding="utf-8",
            )
            self._authorize_args(args, tmpdir)
            mock_inputs.return_value = (19_999.0, [], None)
            mock_evaluate.return_value = argparse.Namespace(
                tradable=True,
                runtime_status="running",
                stop_triggered=False,
                primary_reason=None,
                matched_reasons=[],
                triggered_at=None,
                rolling_hourly_loss=0.0,
                rolling_hourly_gross_notional=0.0,
                rolling_hourly_loss_per_10k=0.0,
                rolling_hourly_loss_per_10k_active=False,
                cumulative_gross_notional=19_999.0,
                unrealized_loss=0.0,
            )

            summary = _maybe_handle_runtime_guard(
                args=args,
                cycle=1,
                cycle_started_at=now,
                summary_path=Path(tmpdir) / "events.jsonl",
            )
            persisted_intent = json.loads(intent_path.read_text(encoding="utf-8"))

        self.assertEqual(summary["runtime_status"], "exit_blocked")
        self.assertIn("terminal_intent_snapshot_digest_mismatch", summary["terminal_drain_reasons"])
        self.assertEqual(persisted_intent["status"], "exit_blocked")
        mock_terminal_drain.assert_not_called()

    @patch("grid_optimizer.loop_runner._start_futures_flatten_process")
    @patch("grid_optimizer.loop_runner.load_live_flatten_snapshot")
    @patch("grid_optimizer.loop_runner._cancel_futures_strategy_orders")
    @patch("grid_optimizer.loop_runner.load_binance_api_credentials")
    @patch("grid_optimizer.loop_runner.evaluate_runtime_guards")
    @patch("grid_optimizer.loop_runner._load_futures_runtime_guard_inputs")
    def test_runtime_guard_best_quote_stop_uses_capped_loss_flatten(
        self,
        mock_inputs,
        mock_evaluate,
        mock_credentials,
        mock_cancel,
        mock_snapshot,
        mock_start_flatten,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "state.json"
            state_path.write_text("{}", encoding="utf-8")
            args = argparse.Namespace(
                symbol="ARXUSDT",
                recv_window=5000,
                strategy_profile="test",
                strategy_mode="hedge_best_quote_maker_volume_v1",
                runtime_guard_stats_start_time=None,
                run_start_time=None,
                run_end_time=None,
                rolling_hourly_loss_limit=None,
                rolling_hourly_loss_per_10k_limit=None,
                rolling_hourly_loss_per_10k_min_notional=None,
                runtime_guard_loss_recovery_enabled=True,
                runtime_guard_loss_recovery_cooldown_seconds=180.0,
                runtime_guard_loss_recovery_max_1m_amplitude_ratio=0.012,
                runtime_guard_loss_recovery_max_3m_amplitude_ratio=0.025,
                max_cumulative_notional=None,
                max_actual_net_notional=400.0,
                max_synthetic_drift_notional=None,
                max_unrealized_loss=None,
                best_quote_maker_volume_reduce_freeze_loss_ratio=0.01,
                auto_regime_enabled=False,
                state_path=str(state_path),
                plan_json=str(Path(tmpdir) / "plan.json"),
            )
            now = datetime(2026, 7, 5, 22, 36, tzinfo=timezone.utc)
            mock_inputs.return_value = (1000.0, [], None)
            mock_evaluate.return_value = argparse.Namespace(
                tradable=False,
                runtime_status="stopped",
                stop_triggered=True,
                primary_reason="max_actual_net_notional_hit",
                matched_reasons=["max_actual_net_notional_hit"],
                triggered_at=now.isoformat(),
                rolling_hourly_loss=0.0,
                rolling_hourly_gross_notional=0.0,
                rolling_hourly_loss_per_10k=0.0,
                rolling_hourly_loss_per_10k_active=False,
                cumulative_gross_notional=1000.0,
                unrealized_loss=0.0,
            )
            mock_credentials.return_value = ("key", "secret")
            mock_cancel.return_value = 2
            mock_snapshot.return_value = {
                "orders": [{"side": "SELL", "quantity": 5838.0}],
                "bid_price": 0.2052,
                "ask_price": 0.2053,
                "long_qty": 5838.0,
                "short_qty": 0.0,
                "net_qty": 5838.0,
            }
            mock_start_flatten.return_value = {"started": True, "already_running": False}

            summary = _maybe_handle_runtime_guard(
                args=args,
                cycle=1,
                cycle_started_at=now,
                summary_path=Path(tmpdir) / "events.jsonl",
            )

        self.assertIsNotNone(summary)
        mock_snapshot.assert_called_once_with(
            "ARXUSDT",
            "key",
            "secret",
            allow_loss=True,
            max_loss_ratio=0.01,
            preserve_long_qty=0.0,
            preserve_short_qty=0.0,
        )
        mock_start_flatten.assert_called_once_with(
            "ARXUSDT",
            allow_loss=True,
            max_loss_ratio=0.01,
            preserve_long_qty=0.0,
            preserve_short_qty=0.0,
        )

    @patch("grid_optimizer.loop_runner._handle_terminal_drain_round")
    @patch("grid_optimizer.loop_runner._start_futures_flatten_process")
    @patch("grid_optimizer.loop_runner.load_live_flatten_snapshot")
    @patch("grid_optimizer.loop_runner._cancel_futures_strategy_orders")
    @patch("grid_optimizer.loop_runner.load_binance_api_credentials")
    @patch("grid_optimizer.loop_runner.evaluate_runtime_guards")
    @patch("grid_optimizer.loop_runner._load_futures_runtime_guard_inputs")
    def test_runtime_guard_end_window_enters_terminal_drain_without_stopping(
        self,
        mock_inputs,
        mock_evaluate,
        mock_credentials,
        mock_cancel,
        mock_snapshot,
        mock_start_flatten,
        mock_terminal_drain,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "state.json"
            state_path.write_text(
                json.dumps({"best_quote_frozen_inventory": {"short_qty": 80.0, "short_entry_price": 234.0}}),
                encoding="utf-8",
            )
            args = argparse.Namespace(
                symbol="ARXUSDT",
                recv_window=5000,
                strategy_profile="test",
                strategy_mode="hedge_best_quote_maker_volume_v1",
                runtime_guard_stats_start_time=None,
                run_start_time=None,
                run_end_time="2026-07-05T08:00:00+08:00",
                rolling_hourly_loss_limit=None,
                rolling_hourly_loss_per_10k_limit=None,
                rolling_hourly_loss_per_10k_min_notional=None,
                runtime_guard_loss_recovery_enabled=True,
                runtime_guard_loss_recovery_cooldown_seconds=180.0,
                runtime_guard_loss_recovery_max_1m_amplitude_ratio=0.012,
                runtime_guard_loss_recovery_max_3m_amplitude_ratio=0.025,
                max_cumulative_notional=None,
                max_actual_net_notional=400.0,
                max_synthetic_drift_notional=None,
                max_unrealized_loss=None,
                terminal_drain_exit_policy="drain_then_preserve",
                terminal_drain_absolute_loss_budget=5.0,
                terminal_drain_max_wait_seconds=900.0,
                best_quote_maker_volume_reduce_freeze_loss_ratio=0.01,
                runtime_guard_stop_auto_flatten_enabled=False,
                auto_regime_enabled=False,
                state_path=str(state_path),
                plan_json=str(Path(tmpdir) / "plan.json"),
            )
            now = datetime(2026, 7, 5, 0, 0, 1, tzinfo=timezone.utc)
            self._authorize_args(args, tmpdir)
            mock_inputs.return_value = (1000.0, [], None)
            mock_evaluate.return_value = argparse.Namespace(
                tradable=False,
                runtime_status="stopped",
                stop_triggered=True,
                primary_reason="after_end_window",
                matched_reasons=["after_end_window"],
                triggered_at=now.isoformat(),
                rolling_hourly_loss=0.0,
                rolling_hourly_gross_notional=0.0,
                rolling_hourly_loss_per_10k=0.0,
                rolling_hourly_loss_per_10k_active=False,
                cumulative_gross_notional=1000.0,
                unrealized_loss=0.0,
            )
            mock_credentials.return_value = ("key", "secret")
            mock_cancel.return_value = 2
            mock_terminal_drain.return_value = {
                "runtime_status": "draining",
                "stop_triggered": False,
                "stop_reason": "after_end_window",
                "terminal_drain_action": "block_entry",
                "canceled_count": 0,
                "flatten_started": False,
            }

            summary = _maybe_handle_runtime_guard(
                args=args,
                cycle=1,
                cycle_started_at=now,
                summary_path=Path(tmpdir) / "events.jsonl",
            )

        self.assertIsNotNone(summary)
        self.assertEqual(summary["stop_reason"], "after_end_window")
        self.assertEqual(summary["runtime_status"], "draining")
        self.assertFalse(summary["stop_triggered"])
        self.assertEqual(summary["terminal_drain_action"], "block_entry")
        self.assertEqual(summary["canceled_count"], 0)
        self.assertFalse(summary["flatten_started"])
        mock_terminal_drain.assert_called_once_with(
            args=args,
            cycle=1,
            cycle_started_at=now,
            stats_start_time=None,
            runtime_guard_config=unittest.mock.ANY,
            runtime_guard_result=mock_evaluate.return_value,
            cumulative_gross_notional=1000.0,
            state=unittest.mock.ANY,
            state_path=state_path,
        )
        mock_cancel.assert_not_called()
        mock_snapshot.assert_not_called()
        mock_start_flatten.assert_not_called()

    @patch("grid_optimizer.loop_runner._start_futures_flatten_process")
    @patch("grid_optimizer.loop_runner.load_live_flatten_snapshot")
    @patch("grid_optimizer.loop_runner._cancel_futures_strategy_orders")
    @patch("grid_optimizer.loop_runner.load_binance_api_credentials")
    @patch("grid_optimizer.loop_runner.evaluate_runtime_guards")
    @patch("grid_optimizer.loop_runner._load_futures_runtime_guard_inputs")
    def test_bounded_non_loss_stop_enters_condition_unmet_terminal_owner(
        self,
        mock_inputs,
        mock_evaluate,
        mock_credentials,
        mock_cancel,
        mock_snapshot,
        mock_start_flatten,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "state.json"
            state_path.write_text(
                json.dumps(
                    {
                        "best_quote_frozen_inventory_manual_limit": {
                            "short": {
                                "requested": True,
                                "request_id": "manual-limit-short-1",
                                "qty": 500.0,
                                "price": 0.66,
                            }
                        }
                    }
                ),
                encoding="utf-8",
            )
            args = argparse.Namespace(
                symbol="PHAROSUSDT",
                recv_window=5000,
                strategy_profile="test",
                strategy_mode="hedge_best_quote_maker_volume_v1",
                runtime_guard_stats_start_time="2026-05-22T14:45:00+00:00",
                run_start_time="2026-05-22T14:45:00+00:00",
                run_end_time="2026-05-22T15:45:00+00:00",
                rolling_hourly_loss_limit=None,
                rolling_hourly_loss_per_10k_limit=None,
                rolling_hourly_loss_per_10k_min_notional=None,
                runtime_guard_loss_recovery_enabled=True,
                runtime_guard_loss_recovery_cooldown_seconds=180.0,
                runtime_guard_loss_recovery_max_1m_amplitude_ratio=0.012,
                runtime_guard_loss_recovery_max_3m_amplitude_ratio=0.025,
                max_cumulative_notional=None,
                max_actual_net_notional=100.0,
                max_synthetic_drift_notional=None,
                max_unrealized_loss=None,
                terminal_drain_exit_policy="drain_then_preserve",
                terminal_drain_absolute_loss_budget=5.0,
                terminal_drain_max_wait_seconds=900.0,
                runtime_guard_stop_auto_flatten_enabled=False,
                auto_regime_enabled=False,
                state_path=str(state_path),
                plan_json=str(Path(tmpdir) / "plan.json"),
            )
            mock_credentials.return_value = ("key", "secret")
            mock_cancel.return_value = 2
            now = datetime(2026, 5, 22, 15, 30, tzinfo=timezone.utc)
            self._authorize_args(args, tmpdir)
            mock_inputs.return_value = (1000.0, [], None)
            mock_evaluate.return_value = argparse.Namespace(
                tradable=False,
                runtime_status="stopped",
                stop_triggered=True,
                primary_reason="max_actual_net_notional_hit",
                matched_reasons=["max_actual_net_notional_hit"],
                triggered_at=now.isoformat(),
                rolling_hourly_loss=0.0,
                rolling_hourly_gross_notional=0.0,
                rolling_hourly_loss_per_10k=0.0,
                rolling_hourly_loss_per_10k_active=False,
                cumulative_gross_notional=1000.0,
                unrealized_loss=0.0,
            )

            with patch(
                "grid_optimizer.loop_runner._handle_terminal_drain_round",
                return_value={
                    "runtime_status": "draining",
                    "stop_triggered": False,
                    "runner_exit_allowed": False,
                    "run_outcome": "condition_unmet",
                    "terminal_drain_action": "block_entry",
                },
            ) as mock_terminal_drain:
                summary = _maybe_handle_runtime_guard(
                    args=args,
                    cycle=1,
                    cycle_started_at=now,
                    summary_path=Path(tmpdir) / "events.jsonl",
                )

            self.assertIsNotNone(summary)
            self.assertFalse(summary["stop_triggered"])
            self.assertEqual(summary["run_outcome"], "condition_unmet")
            self.assertEqual(summary["terminal_drain_action"], "block_entry")
            mock_terminal_drain.assert_called_once()
            mock_cancel.assert_not_called()
            mock_credentials.assert_not_called()
            mock_snapshot.assert_not_called()
            mock_start_flatten.assert_not_called()

    def test_submit_place_orders_are_suppressed_while_loss_recovery_is_unrecovered(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "state.json"
            stopped_at = datetime(2026, 5, 3, 0, 0, tzinfo=timezone.utc)
            state_path.write_text(
                json.dumps(
                    {
                        "runtime_guard_loss_recovery": {
                            "stopped_at": stopped_at.isoformat(),
                            "last_reason": "rolling_hourly_loss_limit_hit",
                        }
                    }
                ),
                encoding="utf-8",
            )
            args = argparse.Namespace(
                state_path=str(state_path),
                runtime_guard_loss_recovery_enabled=True,
                runtime_guard_loss_recovery_cooldown_seconds=180.0,
            )

            actions = _suppress_place_orders_during_runtime_guard_loss_cooldown(
                actions={
                    "place_count": 1,
                    "cancel_count": 0,
                    "place_orders": [{"role": "entry_short", "side": "SELL"}],
                    "cancel_orders": [],
                },
                args=args,
                now=stopped_at + timedelta(seconds=30),
            )

            self.assertEqual(actions["place_count"], 0)
            self.assertEqual(actions["place_orders"], [])
            self.assertEqual(actions["runtime_guard_loss_cooldown"]["reason"], "runtime_guard_loss_cooling_down")
            self.assertEqual(actions["dropped_place_count_by_runtime_guard_loss_cooldown"], 1)

    @patch("grid_optimizer.loop_runner._runtime_guard_market_is_stable_for_recovery")
    def test_submit_loss_recovery_self_heals_when_frozen_inventory_is_strategy_flat(self, mock_stable) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "state.json"
            plan_path = Path(tmpdir) / "plan.json"
            stopped_at = datetime(2026, 5, 22, 15, 30, tzinfo=timezone.utc)
            now = stopped_at + timedelta(minutes=10)
            state_path.write_text(
                json.dumps(
                    {
                        "runtime_guard_loss_recovery": {
                            "stopped_at": stopped_at.isoformat(),
                            "last_reason": "rolling_hourly_loss_limit_hit",
                            "manual_frozen_inventory_override": True,
                            "manual_frozen_inventory_override_reason": "stale",
                        },
                        "runtime_guard_manual_frozen_inventory_override": {"active": True},
                        "best_quote_frozen_inventory": {
                            "long_qty": 0.0,
                            "short_qty": 2274.0,
                        },
                    }
                ),
                encoding="utf-8",
            )
            plan_path.write_text(
                json.dumps(
                    {
                        "generated_at": now.isoformat(),
                        "mid_price": 0.663,
                        "current_long_qty": 0.0,
                        "current_short_qty": 0.0,
                        "current_long_notional": 0.0,
                        "current_short_notional": 0.0,
                        "strategy_actual_net_notional": 0.0,
                        "synthetic_drift_qty": 0.0,
                    }
                ),
                encoding="utf-8",
            )
            args = argparse.Namespace(
                state_path=str(state_path),
                plan_json=str(plan_path),
                symbol="PHAROSUSDT",
                runtime_guard_loss_recovery_enabled=True,
                runtime_guard_loss_recovery_cooldown_seconds=180.0,
                runtime_guard_loss_recovery_max_1m_amplitude_ratio=0.012,
                runtime_guard_loss_recovery_max_3m_amplitude_ratio=0.025,
            )
            mock_stable.return_value = (True, {"available": True})

            actions = _suppress_place_orders_during_runtime_guard_loss_cooldown(
                actions={
                    "place_count": 1,
                    "cancel_count": 0,
                    "place_orders": [{"role": "entry_short", "side": "SELL"}],
                    "cancel_orders": [],
                },
                args=args,
                now=now,
            )

            self.assertEqual(actions["place_count"], 1)
            self.assertNotIn("runtime_guard_loss_cooldown", actions)
            state = json.loads(state_path.read_text(encoding="utf-8"))
            recovery = state["runtime_guard_loss_recovery"]
            self.assertIn("recovered_at", recovery)
            self.assertEqual(recovery["flat_basis"], "strategy_exposure_excluding_frozen_inventory")
            self.assertNotIn("manual_frozen_inventory_override", recovery)
            self.assertNotIn("manual_frozen_inventory_override_reason", recovery)
            self.assertNotIn("runtime_guard_manual_frozen_inventory_override", state)

    @patch("grid_optimizer.loop_runner._runtime_guard_market_is_stable_for_recovery")
    @patch("grid_optimizer.loop_runner.load_binance_api_credentials")
    @patch("grid_optimizer.loop_runner.evaluate_runtime_guards")
    @patch("grid_optimizer.loop_runner._load_futures_runtime_guard_inputs")
    def test_frozen_inventory_loss_recovery_uses_strategy_flat_exposure(
        self,
        mock_inputs,
        mock_evaluate,
        mock_credentials,
        mock_stable,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            now = datetime(2026, 5, 22, 15, 50, tzinfo=timezone.utc)
            stopped_at = now - timedelta(minutes=10)
            state_path = Path(tmpdir) / "state.json"
            plan_path = Path(tmpdir) / "plan.json"
            state_path.write_text(
                json.dumps(
                    {
                        "runtime_guard_loss_recovery": {
                            "stopped_at": stopped_at.isoformat(),
                            "last_reason": "rolling_hourly_loss_limit_hit",
                            "manual_frozen_inventory_override": True,
                            "manual_frozen_inventory_override_reason": "stale",
                        },
                        "runtime_guard_manual_frozen_inventory_override": {
                            "active": True,
                            "reason": "allow_reduce_only_frozen_inventory_directive",
                        },
                        "best_quote_frozen_inventory": {
                            "long_qty": 0.0,
                            "short_qty": 2274.0,
                        },
                    }
                ),
                encoding="utf-8",
            )
            plan_path.write_text(
                json.dumps(
                    {
                        "generated_at": now.isoformat(),
                        "mid_price": 0.663,
                        "current_long_qty": 0.0,
                        "current_short_qty": 0.0,
                        "current_long_notional": 0.0,
                        "current_short_notional": 0.0,
                        "strategy_actual_net_notional": 0.0,
                        "synthetic_drift_qty": 0.0,
                    }
                ),
                encoding="utf-8",
            )
            args = argparse.Namespace(
                symbol="PHAROSUSDT",
                recv_window=5000,
                strategy_profile="test",
                strategy_mode="hedge_best_quote_maker_volume_v1",
                runtime_guard_stats_start_time=None,
                run_start_time=None,
                run_end_time=None,
                rolling_hourly_loss_limit=8.0,
                rolling_hourly_loss_per_10k_limit=None,
                rolling_hourly_loss_per_10k_min_notional=None,
                runtime_guard_loss_recovery_enabled=True,
                runtime_guard_loss_recovery_cooldown_seconds=180.0,
                runtime_guard_loss_recovery_max_1m_amplitude_ratio=0.012,
                runtime_guard_loss_recovery_max_3m_amplitude_ratio=0.025,
                max_cumulative_notional=None,
                max_actual_net_notional=None,
                max_synthetic_drift_notional=None,
                max_unrealized_loss=None,
                auto_regime_enabled=False,
                state_path=str(state_path),
                plan_json=str(plan_path),
            )
            mock_inputs.return_value = (1000.0, [], None)
            mock_evaluate.return_value = argparse.Namespace(
                tradable=False,
                runtime_status="stopped",
                stop_triggered=True,
                primary_reason="rolling_hourly_loss_limit_hit",
                matched_reasons=["rolling_hourly_loss_limit_hit"],
                triggered_at=now.isoformat(),
                rolling_hourly_loss=9.0,
                rolling_hourly_gross_notional=1000.0,
                rolling_hourly_loss_per_10k=90.0,
                rolling_hourly_loss_per_10k_active=False,
                cumulative_gross_notional=1000.0,
                unrealized_loss=0.0,
            )
            mock_stable.return_value = (True, {"available": True, "amplitude_1m": 0.002, "amplitude_3m": 0.004})

            summary = _maybe_handle_runtime_guard(
                args=args,
                cycle=5,
                cycle_started_at=now,
                summary_path=Path(tmpdir) / "events.jsonl",
            )

            self.assertIsNone(summary)
            state = json.loads(state_path.read_text(encoding="utf-8"))
            recovery = state["runtime_guard_loss_recovery"]
            self.assertEqual(recovery["flat_basis"], "strategy_exposure_excluding_frozen_inventory")
            self.assertTrue(recovery["flat"])
            self.assertIn("recovered_at", recovery)
            self.assertNotIn("runtime_guard_manual_frozen_inventory_override", state)
            mock_credentials.assert_not_called()

    def test_submit_place_orders_are_suppressed_during_manual_frozen_override(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "state.json"
            state_path.write_text(
                json.dumps(
                    {
                        "runtime_guard_manual_frozen_inventory_override": {
                            "active": True,
                            "stop_reasons": ["max_actual_net_notional_hit"],
                        },
                        "best_quote_frozen_inventory_manual_limit": {
                            "short": {"requested": True, "qty": 500.0, "price": 0.66}
                        },
                    }
                ),
                encoding="utf-8",
            )
            args = argparse.Namespace(
                state_path=str(state_path),
                runtime_guard_loss_recovery_enabled=True,
                runtime_guard_loss_recovery_cooldown_seconds=180.0,
            )

            actions = _suppress_place_orders_during_runtime_guard_loss_cooldown(
                actions={
                    "place_count": 2,
                    "cancel_count": 0,
                    "place_orders": [
                        {"role": "entry_short", "side": "SELL"},
                        {
                            "role": "frozen_inventory_manual_limit_short",
                            "side": "BUY",
                            "force_reduce_only": True,
                            "frozen_inventory_request_id": "manual-limit-short-1",
                            "frozen_inventory_authorization_validated": True,
                        },
                    ],
                    "cancel_orders": [],
                },
                args=args,
            )

            self.assertEqual(actions["place_count"], 1)
            self.assertEqual(actions["place_orders"][0]["role"], "frozen_inventory_manual_limit_short")
            override = actions["runtime_guard_manual_frozen_inventory_override"]
            self.assertEqual(override["reason"], "runtime_guard_manual_frozen_inventory_override")
            self.assertEqual(actions["dropped_place_count_by_runtime_guard_manual_frozen_inventory_override"], 1)

    def test_auto_per_lot_release_does_not_hold_manual_frozen_override(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "state.json"
            state_path.write_text(
                json.dumps(
                    {
                        "runtime_guard_manual_frozen_inventory_override": {
                            "active": True,
                            "stop_reasons": ["max_actual_net_notional_hit"],
                        },
                        "best_quote_frozen_inventory_manual_reduce": {
                            "short": {
                                "requested": True,
                                "source": "auto_single_leg_take_profit",
                                "frozen_inventory_per_lot_release": True,
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )
            args = argparse.Namespace(
                state_path=str(state_path),
                runtime_guard_loss_recovery_enabled=True,
                runtime_guard_loss_recovery_cooldown_seconds=180.0,
            )

            actions = _suppress_place_orders_during_runtime_guard_loss_cooldown(
                actions={
                    "place_count": 1,
                    "cancel_count": 0,
                    "place_orders": [{"role": "entry_short", "side": "SELL"}],
                    "cancel_orders": [],
                },
                args=args,
            )

            self.assertEqual(1, actions["place_count"])
            self.assertNotIn("runtime_guard_manual_frozen_inventory_override", actions)
            state = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertNotIn("runtime_guard_manual_frozen_inventory_override", state)

    @patch("grid_optimizer.loop_runner._handle_terminal_drain_round")
    @patch("grid_optimizer.loop_runner._start_futures_flatten_process")
    @patch("grid_optimizer.loop_runner.load_live_flatten_snapshot")
    @patch("grid_optimizer.loop_runner._cancel_futures_strategy_orders")
    @patch("grid_optimizer.loop_runner.load_binance_api_credentials")
    @patch("grid_optimizer.loop_runner.evaluate_runtime_guards")
    @patch("grid_optimizer.loop_runner._load_futures_runtime_guard_inputs")
    def test_target_reached_with_frozen_inventory_enters_drain_instead_of_stopping(
        self,
        mock_inputs,
        mock_evaluate,
        mock_credentials,
        mock_cancel,
        mock_snapshot,
        mock_start_flatten,
        mock_terminal_drain,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "state.json"
            state_path.write_text(
                json.dumps({"best_quote_frozen_inventory": {"short_qty": 2474.0, "short_entry_price": 0.664}}),
                encoding="utf-8",
            )
            args = argparse.Namespace(
                symbol="PHAROSUSDT",
                recv_window=5000,
                strategy_profile="test",
                strategy_mode="hedge_best_quote_maker_volume_v1",
                runtime_guard_stats_start_time="2026-05-22T14:45:00+00:00",
                run_start_time="2026-05-22T14:45:00+00:00",
                run_end_time="2026-05-22T16:45:00+00:00",
                rolling_hourly_loss_limit=None,
                rolling_hourly_loss_per_10k_limit=None,
                rolling_hourly_loss_per_10k_min_notional=None,
                runtime_guard_loss_recovery_enabled=True,
                runtime_guard_loss_recovery_cooldown_seconds=180.0,
                runtime_guard_loss_recovery_max_1m_amplitude_ratio=0.012,
                runtime_guard_loss_recovery_max_3m_amplitude_ratio=0.025,
                max_cumulative_notional=1_000.0,
                max_actual_net_notional=None,
                max_synthetic_drift_notional=None,
                max_unrealized_loss=None,
                terminal_drain_exit_policy="drain_then_preserve",
                terminal_drain_absolute_loss_budget=5.0,
                terminal_drain_max_wait_seconds=900.0,
                runtime_guard_stop_auto_flatten_enabled=False,
                auto_regime_enabled=False,
                state_path=str(state_path),
                plan_json=str(Path(tmpdir) / "plan.json"),
            )
            now = datetime(2026, 5, 22, 15, 45, tzinfo=timezone.utc)
            self._authorize_args(args, tmpdir)
            mock_inputs.return_value = (1000.0, [], None)
            mock_evaluate.return_value = argparse.Namespace(
                tradable=False,
                runtime_status="stopped",
                stop_triggered=True,
                primary_reason="max_cumulative_notional_hit",
                matched_reasons=["max_cumulative_notional_hit"],
                triggered_at=now.isoformat(),
                rolling_hourly_loss=0.0,
                rolling_hourly_gross_notional=0.0,
                rolling_hourly_loss_per_10k=0.0,
                rolling_hourly_loss_per_10k_active=False,
                cumulative_gross_notional=1000.0,
                unrealized_loss=0.0,
            )
            mock_credentials.return_value = ("key", "secret")
            mock_cancel.return_value = 3
            mock_terminal_drain.return_value = {
                "runtime_status": "draining",
                "stop_triggered": False,
                "runner_exit_allowed": False,
                "stop_reason": "max_cumulative_notional_hit",
                "run_outcome": "target_reached",
                "terminal_drain_action": "block_entry",
                "canceled_count": 0,
                "flatten_started": False,
            }

            summary = _maybe_handle_runtime_guard(
                args=args,
                cycle=1,
                cycle_started_at=now,
                summary_path=Path(tmpdir) / "events.jsonl",
            )

            self.assertIsNotNone(summary)
            self.assertFalse(summary["stop_triggered"])
            self.assertFalse(summary["runner_exit_allowed"])
            self.assertEqual(summary["run_outcome"], "target_reached")
            self.assertEqual(summary["terminal_drain_action"], "block_entry")
            self.assertEqual(summary["stop_reason"], "max_cumulative_notional_hit")
            self.assertEqual(summary["canceled_count"], 0)
            self.assertFalse(summary["flatten_started"])
            mock_terminal_drain.assert_called_once()
            mock_cancel.assert_not_called()
            mock_snapshot.assert_not_called()
            mock_start_flatten.assert_not_called()

    @patch("grid_optimizer.loop_runner._start_futures_flatten_process")
    @patch("grid_optimizer.loop_runner.load_live_flatten_snapshot")
    @patch("grid_optimizer.loop_runner._cancel_futures_strategy_orders")
    @patch("grid_optimizer.loop_runner.load_binance_api_credentials")
    @patch("grid_optimizer.loop_runner.evaluate_runtime_guards")
    @patch("grid_optimizer.loop_runner._load_futures_runtime_guard_inputs")
    def test_bounded_loss_stop_enters_terminal_owner_when_recovery_disabled(
        self,
        mock_inputs,
        mock_evaluate,
        mock_credentials,
        mock_cancel,
        mock_snapshot,
        mock_start_flatten,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "state.json"
            state_path.write_text(
                json.dumps({"best_quote_frozen_inventory": {"long_qty": 0.3, "long_entry_price": 234.0}}),
                encoding="utf-8",
            )
            args = argparse.Namespace(
                symbol="BCHUSDT",
                recv_window=5000,
                strategy_profile="bchusdt_altcoins_probe_v1",
                strategy_mode="hedge_best_quote_maker_volume_v1",
                runtime_guard_stats_start_time="2026-07-15T08:00:00+00:00",
                run_start_time="2026-07-15T08:00:00+00:00",
                run_end_time="2026-07-15T10:00:00+00:00",
                rolling_hourly_loss_limit=None,
                rolling_hourly_loss_per_10k_limit=2.1,
                rolling_hourly_loss_per_10k_min_notional=5_000.0,
                runtime_guard_loss_recovery_enabled=False,
                max_cumulative_notional=20_000.0,
                max_actual_net_notional=120.0,
                max_synthetic_drift_notional=None,
                max_unrealized_loss=None,
                terminal_drain_exit_policy="drain_then_preserve",
                terminal_drain_absolute_loss_budget=5.0,
                terminal_drain_max_wait_seconds=900.0,
                runtime_guard_stop_auto_flatten_enabled=False,
                auto_regime_enabled=False,
                state_path=str(state_path),
                plan_json=str(Path(tmpdir) / "plan.json"),
            )
            now = datetime(2026, 7, 15, 9, 0, tzinfo=timezone.utc)
            self._authorize_args(args, tmpdir)
            mock_inputs.return_value = (5_500.0, [], None)
            mock_evaluate.return_value = argparse.Namespace(
                tradable=False,
                runtime_status="stopped",
                stop_triggered=True,
                primary_reason="rolling_hourly_loss_per_10k_limit_hit",
                matched_reasons=["rolling_hourly_loss_per_10k_limit_hit"],
                triggered_at=now.isoformat(),
                rolling_hourly_loss=1.2,
                rolling_hourly_gross_notional=5_500.0,
                rolling_hourly_loss_per_10k=2.18,
                rolling_hourly_loss_per_10k_active=True,
                cumulative_gross_notional=5_500.0,
                unrealized_loss=0.0,
            )
            mock_credentials.return_value = ("key", "secret")
            mock_cancel.return_value = 2

            with patch(
                "grid_optimizer.loop_runner._handle_terminal_drain_round",
                return_value={
                    "runtime_status": "draining",
                    "stop_triggered": False,
                    "runner_exit_allowed": False,
                    "run_outcome": "condition_unmet",
                    "terminal_drain_action": "block_entry",
                },
            ) as mock_terminal_drain:
                summary = _maybe_handle_runtime_guard(
                    args=args,
                    cycle=8,
                    cycle_started_at=now,
                    summary_path=Path(tmpdir) / "events.jsonl",
                )

            self.assertIsNotNone(summary)
            self.assertFalse(summary["stop_triggered"])
            self.assertEqual(summary["run_outcome"], "condition_unmet")
            self.assertEqual(summary["terminal_drain_action"], "block_entry")
            mock_terminal_drain.assert_called_once()
            mock_cancel.assert_not_called()
            mock_credentials.assert_not_called()
            mock_snapshot.assert_not_called()
            mock_start_flatten.assert_not_called()

    def test_submit_place_orders_are_not_blocked_during_frozen_override_without_manual_directive(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "state.json"
            state_path.write_text(
                json.dumps(
                    {
                        "runtime_guard_manual_frozen_inventory_override": {
                            "active": True,
                            "stop_reasons": ["max_actual_net_notional_hit"],
                            "frozen_inventory_present": True,
                            "manual_directive_pending": False,
                        },
                        "runtime_guard_loss_recovery": {
                            "manual_frozen_inventory_override": True,
                            "manual_frozen_inventory_override_reason": "stale",
                        },
                        "best_quote_frozen_inventory": {"short_qty": 2474.0},
                    }
                ),
                encoding="utf-8",
            )
            args = argparse.Namespace(
                state_path=str(state_path),
                runtime_guard_loss_recovery_enabled=True,
                runtime_guard_loss_recovery_cooldown_seconds=180.0,
            )

            actions = _suppress_place_orders_during_runtime_guard_loss_cooldown(
                actions={
                    "place_count": 1,
                    "cancel_count": 0,
                    "place_orders": [{"role": "best_quote_reduce_short", "side": "BUY"}],
                    "cancel_orders": [],
                },
                args=args,
            )

            self.assertEqual(actions["place_count"], 1)
            self.assertEqual(actions["place_orders"][0]["role"], "best_quote_reduce_short")
            self.assertNotIn("runtime_guard_manual_frozen_inventory_override", actions)
            state = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertNotIn("runtime_guard_manual_frozen_inventory_override", state)
            recovery = state["runtime_guard_loss_recovery"]
            self.assertNotIn("manual_frozen_inventory_override", recovery)
            self.assertNotIn("manual_frozen_inventory_override_reason", recovery)

    @patch("grid_optimizer.loop_runner._runtime_guard_market_is_stable_for_recovery")
    @patch("grid_optimizer.loop_runner.load_live_flatten_snapshot")
    @patch("grid_optimizer.loop_runner._cancel_futures_strategy_orders")
    @patch("grid_optimizer.loop_runner.load_binance_api_credentials")
    @patch("grid_optimizer.loop_runner._load_futures_runtime_guard_inputs")
    def test_runtime_guard_loss_stop_recovers_after_cooldown_flat_and_stable(
        self,
        mock_inputs,
        mock_credentials,
        mock_cancel,
        mock_snapshot,
        mock_stable,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "state.json"
            stopped_at = datetime(2026, 5, 3, 0, 0, tzinfo=timezone.utc)
            state_path.write_text(
                (
                    '{"runtime_guard_loss_recovery": '
                    f'{{"stopped_at": "{stopped_at.isoformat()}"}}}}'
                ),
                encoding="utf-8",
            )
            args = argparse.Namespace(
                symbol="ORDIUSDC",
                recv_window=5000,
                strategy_profile="test",
                strategy_mode="synthetic_neutral",
                runtime_guard_stats_start_time=None,
                run_start_time=None,
                run_end_time=None,
                rolling_hourly_loss_limit=8.0,
                runtime_guard_loss_recovery_enabled=True,
                runtime_guard_loss_recovery_cooldown_seconds=180.0,
                runtime_guard_loss_recovery_max_1m_amplitude_ratio=0.012,
                runtime_guard_loss_recovery_max_3m_amplitude_ratio=0.025,
                max_cumulative_notional=None,
                max_actual_net_notional=None,
                max_synthetic_drift_notional=None,
                auto_regime_enabled=False,
                state_path=str(state_path),
            )
            now = stopped_at + timedelta(minutes=5)
            mock_inputs.return_value = (
                1000.0,
                [{"ts": (now - timedelta(minutes=1)).isoformat(), "net_pnl": -9.0}],
                None,
            )
            mock_credentials.return_value = ("key", "secret")
            mock_cancel.return_value = 0
            mock_snapshot.return_value = {
                "orders": [],
                "bid_price": 5.0,
                "ask_price": 5.001,
                "long_qty": 0.0,
                "short_qty": 0.0,
                "net_qty": 0.0,
            }
            mock_stable.return_value = (True, {"available": True, "amplitude_1m": 0.002, "amplitude_3m": 0.004})

            summary = _maybe_handle_runtime_guard(
                args=args,
                cycle=2,
                cycle_started_at=now,
                summary_path=Path(tmpdir) / "events.jsonl",
            )

            self.assertIsNone(summary)
            self.assertIn("recovered_at", state_path.read_text(encoding="utf-8"))
            mock_cancel.assert_called_once()

    @patch("grid_optimizer.loop_runner._runtime_guard_market_is_stable_for_recovery")
    @patch("grid_optimizer.loop_runner.load_live_flatten_snapshot")
    @patch("grid_optimizer.loop_runner._cancel_futures_strategy_orders")
    @patch("grid_optimizer.loop_runner.load_binance_api_credentials")
    @patch("grid_optimizer.loop_runner._load_futures_runtime_guard_inputs")
    def test_runtime_guard_second_loss_after_recovery_starts_fresh_cooldown(
        self,
        mock_inputs,
        mock_credentials,
        mock_cancel,
        mock_snapshot,
        mock_stable,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "state.json"
            old_stop = datetime(2026, 5, 3, 0, 0, tzinfo=timezone.utc)
            recovered_at = datetime(2026, 5, 3, 0, 5, tzinfo=timezone.utc)
            now = datetime(2026, 5, 3, 0, 7, tzinfo=timezone.utc)
            state_path.write_text(
                json.dumps(
                    {
                        "runtime_guard_loss_recovery": {
                            "stopped_at": old_stop.isoformat(),
                            "recovered_at": recovered_at.isoformat(),
                        }
                    }
                ),
                encoding="utf-8",
            )
            args = argparse.Namespace(
                symbol="ORDIUSDC",
                recv_window=5000,
                strategy_profile="test",
                strategy_mode="synthetic_neutral",
                runtime_guard_stats_start_time=None,
                run_start_time=None,
                run_end_time=None,
                rolling_hourly_loss_limit=8.0,
                runtime_guard_loss_recovery_enabled=True,
                runtime_guard_loss_recovery_cooldown_seconds=180.0,
                runtime_guard_loss_recovery_max_1m_amplitude_ratio=0.012,
                runtime_guard_loss_recovery_max_3m_amplitude_ratio=0.025,
                max_cumulative_notional=None,
                max_actual_net_notional=None,
                max_synthetic_drift_notional=None,
                auto_regime_enabled=False,
                state_path=str(state_path),
            )
            mock_inputs.return_value = (
                1000.0,
                [
                    {"ts": (recovered_at - timedelta(minutes=1)).isoformat(), "net_pnl": -50.0},
                    {"ts": (now - timedelta(seconds=30)).isoformat(), "net_pnl": -9.0},
                ],
                None,
            )
            mock_credentials.return_value = ("key", "secret")
            mock_cancel.return_value = 0
            mock_snapshot.return_value = {
                "orders": [],
                "bid_price": 5.0,
                "ask_price": 5.001,
                "long_qty": 0.0,
                "short_qty": 0.0,
                "net_qty": 0.0,
            }
            mock_stable.return_value = (True, {"available": True, "amplitude_1m": 0.002, "amplitude_3m": 0.004})

            summary = _maybe_handle_runtime_guard(
                args=args,
                cycle=3,
                cycle_started_at=now,
                summary_path=Path(tmpdir) / "events.jsonl",
            )

            self.assertIsNotNone(summary)
            self.assertEqual(summary["runtime_status"], "cooldown")
            self.assertFalse(summary["stop_triggered"])
            self.assertEqual(summary["stop_reason"], "rolling_hourly_loss_cooling_down")
            recovery = json.loads(state_path.read_text(encoding="utf-8"))["runtime_guard_loss_recovery"]
            self.assertNotIn("recovered_at", recovery)
            self.assertEqual(recovery["stopped_at"], now.isoformat())
            self.assertAlmostEqual(recovery["last_rolling_hourly_loss"], 9.0)
            mock_cancel.assert_called_once()


if __name__ == "__main__":
    unittest.main()
