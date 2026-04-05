from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from grid_optimizer.runtime_guards import (
    RuntimeGuardConfig,
    evaluate_runtime_guards,
    normalize_runtime_guard_config,
    normalize_runtime_guard_payload,
    resolve_runtime_guard_stats_start_time,
)


class RuntimeGuardsTests(unittest.TestCase):
    def test_normalize_runtime_guard_config_accepts_empty_values(self) -> None:
        cfg = normalize_runtime_guard_config(
            {
                "run_start_time": "",
                "run_end_time": None,
                "rolling_hourly_loss_limit": "",
                "max_cumulative_notional": None,
            }
        )
        self.assertIsNone(cfg.run_start_time)
        self.assertIsNone(cfg.run_end_time)
        self.assertIsNone(cfg.rolling_hourly_loss_limit)
        self.assertIsNone(cfg.max_cumulative_notional)

    def test_normalize_runtime_guard_config_rejects_inverted_window(self) -> None:
        with self.assertRaisesRegex(ValueError, "run_start_time must be earlier than run_end_time"):
            normalize_runtime_guard_config(
                {
                    "run_start_time": "2026-03-30T10:00:00+00:00",
                    "run_end_time": "2026-03-30T09:00:00+00:00",
                }
            )

    def test_evaluate_runtime_guards_returns_waiting_before_start(self) -> None:
        now = datetime(2026, 3, 30, 8, 0, tzinfo=timezone.utc)
        cfg = RuntimeGuardConfig(
            run_start_time=datetime(2026, 3, 30, 9, 0, tzinfo=timezone.utc),
            run_end_time=None,
            rolling_hourly_loss_limit=None,
            max_cumulative_notional=None,
            max_actual_net_notional=None,
            max_synthetic_drift_notional=None,
        )
        result = evaluate_runtime_guards(
            config=cfg,
            now=now,
            cumulative_gross_notional=0.0,
            pnl_events=[],
        )
        self.assertFalse(result.tradable)
        self.assertFalse(result.stop_triggered)
        self.assertEqual(result.runtime_status, "waiting")
        self.assertEqual(result.primary_reason, "before_start_window")

    def test_evaluate_runtime_guards_stops_after_end_time(self) -> None:
        now = datetime(2026, 3, 30, 10, 0, tzinfo=timezone.utc)
        cfg = RuntimeGuardConfig(
            run_start_time=None,
            run_end_time=datetime(2026, 3, 30, 9, 30, tzinfo=timezone.utc),
            rolling_hourly_loss_limit=None,
            max_cumulative_notional=None,
            max_actual_net_notional=None,
            max_synthetic_drift_notional=None,
        )
        result = evaluate_runtime_guards(
            config=cfg,
            now=now,
            cumulative_gross_notional=0.0,
            pnl_events=[],
        )
        self.assertFalse(result.tradable)
        self.assertTrue(result.stop_triggered)
        self.assertEqual(result.primary_reason, "after_end_window")

    def test_evaluate_runtime_guards_stops_on_rolling_loss(self) -> None:
        now = datetime(2026, 3, 30, 10, 0, tzinfo=timezone.utc)
        cfg = RuntimeGuardConfig(
            run_start_time=None,
            run_end_time=None,
            rolling_hourly_loss_limit=50.0,
            max_cumulative_notional=None,
            max_actual_net_notional=None,
            max_synthetic_drift_notional=None,
        )
        result = evaluate_runtime_guards(
            config=cfg,
            now=now,
            cumulative_gross_notional=0.0,
            pnl_events=[
                {"ts": (now - timedelta(minutes=20)).isoformat(), "net_pnl": -30.0},
                {"ts": (now - timedelta(minutes=5)).isoformat(), "net_pnl": -25.0},
            ],
        )
        self.assertTrue(result.stop_triggered)
        self.assertEqual(result.primary_reason, "rolling_hourly_loss_limit_hit")
        self.assertAlmostEqual(result.rolling_hourly_loss, 55.0)

    def test_evaluate_runtime_guards_stops_on_cumulative_notional(self) -> None:
        now = datetime(2026, 3, 30, 10, 0, tzinfo=timezone.utc)
        cfg = RuntimeGuardConfig(
            run_start_time=None,
            run_end_time=None,
            rolling_hourly_loss_limit=None,
            max_cumulative_notional=1000.0,
            max_actual_net_notional=None,
            max_synthetic_drift_notional=None,
        )
        result = evaluate_runtime_guards(
            config=cfg,
            now=now,
            cumulative_gross_notional=1000.0,
            pnl_events=[],
        )
        self.assertTrue(result.stop_triggered)
        self.assertEqual(result.primary_reason, "max_cumulative_notional_hit")

    def test_evaluate_runtime_guards_prefers_end_time_over_other_stop_reasons(self) -> None:
        now = datetime(2026, 3, 30, 10, 0, tzinfo=timezone.utc)
        cfg = RuntimeGuardConfig(
            run_start_time=None,
            run_end_time=datetime(2026, 3, 30, 9, 30, tzinfo=timezone.utc),
            rolling_hourly_loss_limit=50.0,
            max_cumulative_notional=1000.0,
            max_actual_net_notional=None,
            max_synthetic_drift_notional=None,
        )
        result = evaluate_runtime_guards(
            config=cfg,
            now=now,
            cumulative_gross_notional=1200.0,
            pnl_events=[
                {"ts": (now - timedelta(minutes=20)).isoformat(), "net_pnl": -30.0},
                {"ts": (now - timedelta(minutes=5)).isoformat(), "net_pnl": -25.0},
            ],
        )
        self.assertEqual(
            result.matched_reasons,
            [
                "after_end_window",
                "rolling_hourly_loss_limit_hit",
                "max_cumulative_notional_hit",
            ],
        )
        self.assertEqual(result.primary_reason, "after_end_window")

    @patch("grid_optimizer.runtime_guards.resolve_active_competition_board")
    def test_resolve_runtime_guard_stats_start_time_clamps_stale_explicit_start_to_active_phase(self, mock_board) -> None:
        mock_board.return_value = {
            "activity_start_at": "2026-04-05T08:00:00+08:00",
            "activity_end_at": "2026-04-15T07:59:00+08:00",
        }

        resolved = resolve_runtime_guard_stats_start_time(
            runtime_guard_stats_start_time="2026-03-26T18:00:00+08:00",
            symbol="BARDUSDT",
            market="futures",
            now=datetime(2026, 4, 5, 12, 0, tzinfo=timezone.utc),
        )

        self.assertEqual(resolved, datetime(2026, 4, 5, 0, 0, tzinfo=timezone.utc))

    @patch("grid_optimizer.runtime_guards.resolve_active_competition_board")
    def test_resolve_runtime_guard_stats_start_time_keeps_later_manual_start(self, mock_board) -> None:
        mock_board.return_value = {
            "activity_start_at": "2026-04-05T08:00:00+08:00",
            "activity_end_at": "2026-04-15T07:59:00+08:00",
        }

        resolved = resolve_runtime_guard_stats_start_time(
            runtime_guard_stats_start_time="2026-04-06T09:30:00+08:00",
            symbol="BARDUSDT",
            market="futures",
            now=datetime(2026, 4, 6, 2, 0, tzinfo=timezone.utc),
        )

        self.assertEqual(resolved, datetime(2026, 4, 6, 1, 30, tzinfo=timezone.utc))

    @patch("grid_optimizer.runtime_guards.resolve_active_competition_board")
    def test_normalize_runtime_guard_payload_uses_active_competition_phase_start(self, mock_board) -> None:
        mock_board.return_value = {
            "activity_start_at": "2026-04-05T08:00:00+08:00",
            "activity_end_at": "2026-04-15T07:59:00+08:00",
        }

        payload = normalize_runtime_guard_payload(
            {
                "symbol": "BARDUSDT",
                "runtime_guard_stats_start_time": "2026-03-26T18:00:00+08:00",
            },
            symbol="BARDUSDT",
            market="futures",
            now=datetime(2026, 4, 5, 12, 0, tzinfo=timezone.utc),
        )

        self.assertEqual(payload["runtime_guard_stats_start_time"], "2026-04-05T00:00:00+00:00")


if __name__ == "__main__":
    unittest.main()
