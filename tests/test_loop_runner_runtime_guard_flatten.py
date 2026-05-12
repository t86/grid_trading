from __future__ import annotations

import argparse
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from grid_optimizer.loop_runner import _maybe_handle_runtime_guard


class LoopRunnerRuntimeGuardFlattenTests(unittest.TestCase):
    @patch("grid_optimizer.loop_runner._start_futures_flatten_process")
    @patch("grid_optimizer.loop_runner.load_live_flatten_snapshot")
    @patch("grid_optimizer.loop_runner._cancel_futures_strategy_orders")
    @patch("grid_optimizer.loop_runner.load_binance_api_credentials")
    @patch("grid_optimizer.loop_runner._load_futures_runtime_guard_inputs")
    def test_runtime_guard_loss_stop_allows_loss_flatten(
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
        self.assertTrue(summary["flatten_started"])
        mock_snapshot.assert_called_once_with("ORDIUSDC", "key", "secret", allow_loss=True)
        mock_start_flatten.assert_called_once_with("ORDIUSDC", allow_loss=True)

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


if __name__ == "__main__":
    unittest.main()
