from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from grid_optimizer.runtime_guards import (
    RuntimeGuardConfig,
    evaluate_runtime_guards,
    normalize_runtime_guard_config,
)


class RuntimeGuardsTests(unittest.TestCase):
    def test_normalize_runtime_guard_config_accepts_empty_values(self) -> None:
        cfg = normalize_runtime_guard_config(
            {
                "run_start_time": "",
                "run_end_time": None,
                "rolling_hourly_loss_limit": "",
                "max_cumulative_notional": None,
                "max_cumulative_loss_limit": "",
            }
        )
        self.assertIsNone(cfg.run_start_time)
        self.assertIsNone(cfg.run_end_time)
        self.assertIsNone(cfg.rolling_hourly_loss_limit)
        self.assertIsNone(cfg.max_cumulative_notional)
        self.assertIsNone(cfg.max_cumulative_loss_limit)

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
            max_cumulative_loss_limit=None,
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
            max_cumulative_loss_limit=None,
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
            max_cumulative_loss_limit=None,
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
            max_cumulative_loss_limit=None,
        )
        result = evaluate_runtime_guards(
            config=cfg,
            now=now,
            cumulative_gross_notional=1000.0,
            pnl_events=[],
        )
        self.assertTrue(result.stop_triggered)
        self.assertEqual(result.primary_reason, "max_cumulative_notional_hit")

    def test_evaluate_runtime_guards_stops_on_cumulative_loss(self) -> None:
        now = datetime(2026, 3, 30, 10, 0, tzinfo=timezone.utc)
        cfg = RuntimeGuardConfig(
            run_start_time=None,
            run_end_time=None,
            rolling_hourly_loss_limit=None,
            max_cumulative_notional=None,
            max_cumulative_loss_limit=800.0,
        )
        result = evaluate_runtime_guards(
            config=cfg,
            now=now,
            cumulative_gross_notional=0.0,
            pnl_events=[
                {"ts": (now - timedelta(days=2)).isoformat(), "net_pnl": -500.0},
                {"ts": (now - timedelta(minutes=5)).isoformat(), "net_pnl": -310.0},
                {"ts": (now + timedelta(minutes=5)).isoformat(), "net_pnl": -1000.0},
            ],
        )
        self.assertTrue(result.stop_triggered)
        self.assertEqual(result.primary_reason, "max_cumulative_loss_limit_hit")
        self.assertAlmostEqual(result.cumulative_loss, 810.0)

    def test_evaluate_runtime_guards_prefers_end_time_over_other_stop_reasons(self) -> None:
        now = datetime(2026, 3, 30, 10, 0, tzinfo=timezone.utc)
        cfg = RuntimeGuardConfig(
            run_start_time=None,
            run_end_time=datetime(2026, 3, 30, 9, 30, tzinfo=timezone.utc),
            rolling_hourly_loss_limit=50.0,
            max_cumulative_notional=1000.0,
            max_cumulative_loss_limit=800.0,
        )
        result = evaluate_runtime_guards(
            config=cfg,
            now=now,
            cumulative_gross_notional=1200.0,
            pnl_events=[
                {"ts": (now - timedelta(minutes=20)).isoformat(), "net_pnl": -30.0},
                {"ts": (now - timedelta(minutes=5)).isoformat(), "net_pnl": -780.0},
            ],
        )
        self.assertEqual(
            result.matched_reasons,
            [
                "after_end_window",
                "rolling_hourly_loss_limit_hit",
                "max_cumulative_notional_hit",
                "max_cumulative_loss_limit_hit",
            ],
        )
        self.assertEqual(result.primary_reason, "after_end_window")


if __name__ == "__main__":
    unittest.main()
