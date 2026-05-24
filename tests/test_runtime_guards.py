from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from grid_optimizer.runtime_guards import (
    RuntimeGuardConfig,
    evaluate_runtime_guards,
    normalize_runtime_guard_config,
    normalize_runtime_guard_payload,
    resolve_runtime_guard_stats_start_time,
    summarize_futures_runtime_guard_inputs,
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

    def test_beijing_08_daily_stats_start_resolves_to_current_window(self) -> None:
        resolved = resolve_runtime_guard_stats_start_time(
            runtime_guard_stats_start_time="beijing_08_daily",
            now=datetime(2026, 5, 23, 11, 30, tzinfo=timezone.utc),
        )
        self.assertEqual(resolved, datetime(2026, 5, 23, 0, 0, tzinfo=timezone.utc))

    def test_beijing_08_daily_stats_start_uses_previous_day_before_8am(self) -> None:
        resolved = resolve_runtime_guard_stats_start_time(
            runtime_guard_stats_start_time="beijing_08_daily",
            now=datetime(2026, 5, 22, 23, 30, tzinfo=timezone.utc),
        )
        self.assertEqual(resolved, datetime(2026, 5, 22, 0, 0, tzinfo=timezone.utc))

    def test_summarize_futures_runtime_guard_inputs_dedupes_order_notional(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            summary_path = Path(tmp) / "pharosusdt_hedge_bq_events.jsonl"
            trade_path = Path(tmp) / "pharosusdt_hedge_bq_trade_audit.jsonl"
            rows = [
                {
                    "time": 1779570000000,
                    "orderId": 1001,
                    "price": "0.6500",
                    "qty": "10",
                    "quoteQty": "6.50",
                    "realizedPnl": "0",
                    "commission": "0",
                    "commissionAsset": "USDT",
                },
                {
                    "time": 1779570000000,
                    "orderId": 1001,
                    "price": "0.6500",
                    "qty": "10",
                    "quoteQty": "6.50",
                    "realizedPnl": "0",
                    "commission": "0",
                    "commissionAsset": "USDT",
                },
                {
                    "time": 1779570060000,
                    "orderId": 1002,
                    "price": "0.6400",
                    "qty": "20",
                    "realizedPnl": "0",
                    "commission": "0",
                    "commissionAsset": "USDT",
                },
            ]
            trade_path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")

            gross, pnl_events, _ = summarize_futures_runtime_guard_inputs(
                summary_path,
                runtime_guard_stats_start_time="2026-05-23T00:00:00+00:00",
                now=datetime(2026, 5, 23, 23, 30, tzinfo=timezone.utc),
            )

            self.assertAlmostEqual(gross, 6.50 + 12.80)
            self.assertEqual(len(pnl_events), 3)

    def test_summarize_futures_runtime_guard_inputs_counts_only_normal_bq_book(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            summary_path = Path(tmp) / "pharosusdt_hedge_bq_events.jsonl"
            trade_path = Path(tmp) / "pharosusdt_hedge_bq_trade_audit.jsonl"
            rows = [
                {
                    "time": 1779570000000,
                    "orderId": 1,
                    "price": "1",
                    "qty": "100",
                    "quoteQty": "100",
                    "realizedPnl": "0",
                    "commission": "0",
                    "commissionAsset": "USDT",
                },
                {
                    "time": 1779570001000,
                    "orderId": 2,
                    "price": "1",
                    "qty": "500",
                    "quoteQty": "500",
                    "realizedPnl": "0",
                    "commission": "0",
                    "commissionAsset": "USDT",
                },
                {
                    "time": 1779570002000,
                    "orderId": 3,
                    "price": "1",
                    "qty": "900",
                    "quoteQty": "900",
                    "realizedPnl": "0",
                    "commission": "0",
                    "commissionAsset": "USDT",
                },
            ]
            trade_path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")
            refs_path = Path(tmp) / "state.json"
            refs_path.write_text(
                json.dumps(
                    {
                        "best_quote_volume_order_refs": {
                            "1": {"book": "normal_bq"},
                            "2": {"book": "frozen_bq"},
                            "3": {"book": "unknown"},
                        }
                    }
                ),
                encoding="utf-8",
            )

            gross, pnl_events, _ = summarize_futures_runtime_guard_inputs(
                summary_path,
                runtime_guard_stats_start_time="2026-05-23T00:00:00+00:00",
                now=datetime(2026, 5, 23, 23, 30, tzinfo=timezone.utc),
                bq_order_refs_path=refs_path,
                bq_book_scope="normal_bq",
            )

            self.assertEqual(gross, 100.0)
            self.assertEqual([event["order_id"] for event in pnl_events], [1])

    def test_evaluate_runtime_guards_returns_waiting_before_start(self) -> None:
        now = datetime(2026, 3, 30, 8, 0, tzinfo=timezone.utc)
        cfg = RuntimeGuardConfig(
            run_start_time=datetime(2026, 3, 30, 9, 0, tzinfo=timezone.utc),
            run_end_time=None,
            rolling_hourly_loss_limit=None,
            rolling_hourly_loss_per_10k_limit=None,
            rolling_hourly_loss_per_10k_min_notional=10000.0,
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
            rolling_hourly_loss_per_10k_limit=None,
            rolling_hourly_loss_per_10k_min_notional=10000.0,
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
            rolling_hourly_loss_per_10k_limit=None,
            rolling_hourly_loss_per_10k_min_notional=10000.0,
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

    def test_evaluate_runtime_guards_stops_on_rolling_loss_per_10k(self) -> None:
        now = datetime(2026, 3, 30, 10, 0, tzinfo=timezone.utc)
        cfg = RuntimeGuardConfig(
            run_start_time=None,
            run_end_time=None,
            rolling_hourly_loss_limit=100.0,
            rolling_hourly_loss_per_10k_limit=5.0,
            rolling_hourly_loss_per_10k_min_notional=5_000.0,
            max_cumulative_notional=None,
            max_actual_net_notional=None,
            max_synthetic_drift_notional=None,
        )
        result = evaluate_runtime_guards(
            config=cfg,
            now=now,
            cumulative_gross_notional=50_000.0,
            pnl_events=[
                {
                    "ts": (now - timedelta(minutes=20)).isoformat(),
                    "net_pnl": -6.0,
                    "gross_notional": 5_000.0,
                },
            ],
        )
        self.assertTrue(result.stop_triggered)
        self.assertEqual(result.primary_reason, "rolling_hourly_loss_per_10k_limit_hit")
        self.assertAlmostEqual(result.rolling_hourly_loss, 6.0)
        self.assertAlmostEqual(result.rolling_hourly_gross_notional, 5_000.0)
        self.assertAlmostEqual(result.rolling_hourly_loss_per_10k, 12.0)
        self.assertTrue(result.rolling_hourly_loss_per_10k_active)

    def test_evaluate_runtime_guards_ignores_per_10k_below_min_notional(self) -> None:
        now = datetime(2026, 3, 30, 10, 0, tzinfo=timezone.utc)
        cfg = RuntimeGuardConfig(
            run_start_time=None,
            run_end_time=None,
            rolling_hourly_loss_limit=100.0,
            rolling_hourly_loss_per_10k_limit=5.0,
            rolling_hourly_loss_per_10k_min_notional=10_000.0,
            max_cumulative_notional=None,
            max_actual_net_notional=None,
            max_synthetic_drift_notional=None,
        )
        result = evaluate_runtime_guards(
            config=cfg,
            now=now,
            cumulative_gross_notional=50_000.0,
            pnl_events=[
                {
                    "ts": (now - timedelta(minutes=20)).isoformat(),
                    "net_pnl": -6.0,
                    "gross_notional": 5_000.0,
                },
            ],
        )
        self.assertTrue(result.tradable)
        self.assertAlmostEqual(result.rolling_hourly_loss_per_10k, 12.0)
        self.assertFalse(result.rolling_hourly_loss_per_10k_active)

    def test_evaluate_runtime_guards_does_not_stop_per_10k_without_window_volume(self) -> None:
        now = datetime(2026, 3, 30, 10, 0, tzinfo=timezone.utc)
        cfg = RuntimeGuardConfig(
            run_start_time=None,
            run_end_time=None,
            rolling_hourly_loss_limit=None,
            rolling_hourly_loss_per_10k_limit=5.0,
            rolling_hourly_loss_per_10k_min_notional=10000.0,
            max_cumulative_notional=None,
            max_actual_net_notional=None,
            max_synthetic_drift_notional=None,
        )
        result = evaluate_runtime_guards(
            config=cfg,
            now=now,
            cumulative_gross_notional=50_000.0,
            pnl_events=[
                {
                    "ts": (now - timedelta(minutes=20)).isoformat(),
                    "net_pnl": -6.0,
                    "gross_notional": 0.0,
                },
            ],
        )
        self.assertTrue(result.tradable)
        self.assertAlmostEqual(result.rolling_hourly_loss_per_10k, 0.0)

    def test_evaluate_runtime_guards_stops_on_cumulative_notional(self) -> None:
        now = datetime(2026, 3, 30, 10, 0, tzinfo=timezone.utc)
        cfg = RuntimeGuardConfig(
            run_start_time=None,
            run_end_time=None,
            rolling_hourly_loss_limit=None,
            rolling_hourly_loss_per_10k_limit=None,
            rolling_hourly_loss_per_10k_min_notional=10000.0,
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

    def test_evaluate_runtime_guards_stops_on_unrealized_loss(self) -> None:
        now = datetime(2026, 3, 30, 10, 0, tzinfo=timezone.utc)
        cfg = RuntimeGuardConfig(
            run_start_time=None,
            run_end_time=None,
            rolling_hourly_loss_limit=None,
            rolling_hourly_loss_per_10k_limit=None,
            rolling_hourly_loss_per_10k_min_notional=10000.0,
            max_cumulative_notional=None,
            max_actual_net_notional=None,
            max_synthetic_drift_notional=None,
            max_unrealized_loss=25.0,
        )
        result = evaluate_runtime_guards(
            config=cfg,
            now=now,
            cumulative_gross_notional=0.0,
            pnl_events=[],
            unrealized_pnl=-30.0,
        )
        self.assertTrue(result.stop_triggered)
        self.assertEqual(result.primary_reason, "max_unrealized_loss_hit")
        self.assertAlmostEqual(result.unrealized_loss, 30.0)

    def test_evaluate_runtime_guards_prefers_end_time_over_other_stop_reasons(self) -> None:
        now = datetime(2026, 3, 30, 10, 0, tzinfo=timezone.utc)
        cfg = RuntimeGuardConfig(
            run_start_time=None,
            run_end_time=datetime(2026, 3, 30, 9, 30, tzinfo=timezone.utc),
            rolling_hourly_loss_limit=50.0,
            rolling_hourly_loss_per_10k_limit=None,
            rolling_hourly_loss_per_10k_min_notional=10000.0,
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
