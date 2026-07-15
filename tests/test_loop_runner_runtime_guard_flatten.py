from __future__ import annotations

import argparse
import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from grid_optimizer.loop_runner import (
    _cancel_futures_strategy_orders,
    _maybe_handle_runtime_guard,
    _suppress_place_orders_during_runtime_guard_loss_cooldown,
)


class LoopRunnerRuntimeGuardFlattenTests(unittest.TestCase):
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

    @patch("grid_optimizer.loop_runner._start_futures_flatten_process")
    @patch("grid_optimizer.loop_runner.load_live_flatten_snapshot")
    @patch("grid_optimizer.loop_runner._cancel_futures_strategy_orders")
    @patch("grid_optimizer.loop_runner.load_binance_api_credentials")
    @patch("grid_optimizer.loop_runner.evaluate_runtime_guards")
    @patch("grid_optimizer.loop_runner._load_futures_runtime_guard_inputs")
    def test_runtime_guard_end_window_skips_flatten(
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
                best_quote_maker_volume_reduce_freeze_loss_ratio=0.01,
                runtime_guard_stop_auto_flatten_enabled=False,
                auto_regime_enabled=False,
                state_path=str(state_path),
                plan_json=str(Path(tmpdir) / "plan.json"),
            )
            now = datetime(2026, 7, 5, 0, 0, 1, tzinfo=timezone.utc)
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

            summary = _maybe_handle_runtime_guard(
                args=args,
                cycle=1,
                cycle_started_at=now,
                summary_path=Path(tmpdir) / "events.jsonl",
            )

        self.assertIsNotNone(summary)
        self.assertEqual(summary["stop_reason"], "after_end_window")
        self.assertEqual(summary["canceled_count"], 2)
        self.assertFalse(summary["flatten_started"])
        mock_cancel.assert_called_once_with(
            symbol="ARXUSDT",
            api_key="key",
            api_secret="secret",
            recv_window=5000,
        )
        mock_snapshot.assert_not_called()
        mock_start_flatten.assert_not_called()

    @patch("grid_optimizer.loop_runner._start_futures_flatten_process")
    @patch("grid_optimizer.loop_runner.load_live_flatten_snapshot")
    @patch("grid_optimizer.loop_runner._cancel_futures_strategy_orders")
    @patch("grid_optimizer.loop_runner.load_binance_api_credentials")
    @patch("grid_optimizer.loop_runner.evaluate_runtime_guards")
    @patch("grid_optimizer.loop_runner._load_futures_runtime_guard_inputs")
    def test_runtime_guard_non_loss_stop_overrides_frozen_manual_directive_and_stops(
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
                            "short": {"requested": True, "qty": 500.0, "price": 0.66}
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
                max_actual_net_notional=100.0,
                max_synthetic_drift_notional=None,
                max_unrealized_loss=None,
                runtime_guard_stop_auto_flatten_enabled=False,
                auto_regime_enabled=False,
                state_path=str(state_path),
                plan_json=str(Path(tmpdir) / "plan.json"),
            )
            mock_credentials.return_value = ("key", "secret")
            mock_cancel.return_value = 2
            now = datetime(2026, 5, 22, 15, 30, tzinfo=timezone.utc)
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

            summary = _maybe_handle_runtime_guard(
                args=args,
                cycle=1,
                cycle_started_at=now,
                summary_path=Path(tmpdir) / "events.jsonl",
            )

            self.assertIsNotNone(summary)
            self.assertTrue(summary["stop_triggered"])
            self.assertEqual(summary["stop_reason"], "max_actual_net_notional_hit")
            self.assertEqual(summary["canceled_count"], 2)
            self.assertFalse(summary["flatten_started"])
            state = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertNotIn("runtime_guard_manual_frozen_inventory_override", state)
            mock_cancel.assert_called_once_with(
                symbol="PHAROSUSDT",
                api_key="key",
                api_secret="secret",
                recv_window=5000,
            )
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

    @patch("grid_optimizer.loop_runner._start_futures_flatten_process")
    @patch("grid_optimizer.loop_runner.load_live_flatten_snapshot")
    @patch("grid_optimizer.loop_runner._cancel_futures_strategy_orders")
    @patch("grid_optimizer.loop_runner.load_binance_api_credentials")
    @patch("grid_optimizer.loop_runner.evaluate_runtime_guards")
    @patch("grid_optimizer.loop_runner._load_futures_runtime_guard_inputs")
    def test_runtime_guard_hard_stop_cancels_orders_when_frozen_inventory_exists(
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
                json.dumps({"best_quote_frozen_inventory": {"short_qty": 2474.0, "short_entry_price": 0.664}}),
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
                runtime_guard_stop_auto_flatten_enabled=False,
                auto_regime_enabled=False,
                state_path=str(state_path),
                plan_json=str(Path(tmpdir) / "plan.json"),
            )
            now = datetime(2026, 5, 22, 15, 45, tzinfo=timezone.utc)
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

            summary = _maybe_handle_runtime_guard(
                args=args,
                cycle=1,
                cycle_started_at=now,
                summary_path=Path(tmpdir) / "events.jsonl",
            )

            self.assertIsNotNone(summary)
            self.assertTrue(summary["stop_triggered"])
            self.assertEqual(summary["stop_reason"], "max_cumulative_notional_hit")
            self.assertEqual(summary["canceled_count"], 3)
            self.assertFalse(summary["flatten_started"])
            state = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertNotIn("runtime_guard_manual_frozen_inventory_override", state)
            mock_cancel.assert_called_once_with(
                symbol="PHAROSUSDT",
                api_key="key",
                api_secret="secret",
                recv_window=5000,
            )
            mock_snapshot.assert_not_called()
            mock_start_flatten.assert_not_called()

    @patch("grid_optimizer.loop_runner._start_futures_flatten_process")
    @patch("grid_optimizer.loop_runner.load_live_flatten_snapshot")
    @patch("grid_optimizer.loop_runner._cancel_futures_strategy_orders")
    @patch("grid_optimizer.loop_runner.load_binance_api_credentials")
    @patch("grid_optimizer.loop_runner.evaluate_runtime_guards")
    @patch("grid_optimizer.loop_runner._load_futures_runtime_guard_inputs")
    def test_runtime_guard_loss_stop_is_hard_when_recovery_disabled_with_frozen_inventory(
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
                runtime_guard_stats_start_time=None,
                run_start_time=None,
                run_end_time=None,
                rolling_hourly_loss_limit=None,
                rolling_hourly_loss_per_10k_limit=2.1,
                rolling_hourly_loss_per_10k_min_notional=5_000.0,
                runtime_guard_loss_recovery_enabled=False,
                max_cumulative_notional=20_000.0,
                max_actual_net_notional=120.0,
                max_synthetic_drift_notional=None,
                max_unrealized_loss=None,
                runtime_guard_stop_auto_flatten_enabled=False,
                auto_regime_enabled=False,
                state_path=str(state_path),
                plan_json=str(Path(tmpdir) / "plan.json"),
            )
            now = datetime(2026, 7, 15, 9, 0, tzinfo=timezone.utc)
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

            summary = _maybe_handle_runtime_guard(
                args=args,
                cycle=8,
                cycle_started_at=now,
                summary_path=Path(tmpdir) / "events.jsonl",
            )

            self.assertIsNotNone(summary)
            self.assertTrue(summary["stop_triggered"])
            self.assertEqual(summary["stop_reason"], "rolling_hourly_loss_per_10k_limit_hit")
            self.assertEqual(summary["canceled_count"], 2)
            self.assertFalse(summary["flatten_started"])
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
