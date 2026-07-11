from __future__ import annotations

import json
import subprocess
import unittest
from contextlib import redirect_stdout
from datetime import datetime, timedelta, timezone
from io import StringIO
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from grid_optimizer import bq_volume_recovery_guard
from grid_optimizer.bq_volume_recovery_guard import (
    apply_daily_target_pace_floor,
    apply_target_pace_cycle_budget_floor,
    check_symbol,
    fetch_recent_user_trades,
    recover_corrupt_loop_state,
    recover_inactive_runner,
    summarize_recent_volume,
)


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _append_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.write_text(
        "\n".join(json.dumps(row, ensure_ascii=False) for row in rows),
        encoding="utf-8",
    )


class BqVolumeRecoveryGuardTests(unittest.TestCase):
    def test_main_never_restarts_symbol_after_target_gate_done(self) -> None:
        now = datetime.now(timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            marker = output_dir / f"arxusdt_target_gate_done_{now.strftime('%Y%m%d')}.flag"
            marker.write_text(now.isoformat(), encoding="utf-8")
            stdout = StringIO()

            with (
                patch.object(bq_volume_recovery_guard, "_runner_is_active", return_value=False) as runner_active,
                patch.object(bq_volume_recovery_guard, "_fetch_exchange_user_trades") as fetch_trades,
                redirect_stdout(stdout),
            ):
                exit_code = bq_volume_recovery_guard.main(
                    [
                        "--output-dir",
                        str(output_dir),
                        "--state-path",
                        str(output_dir / "state.json"),
                        "--symbols",
                        "ARXUSDT",
                    ]
                )

            self.assertEqual(exit_code, 0)
            runner_active.assert_called_once_with("ARXUSDT")
            fetch_trades.assert_not_called()
            payload = json.loads(stdout.getvalue())
            self.assertEqual(payload["results"][0]["action"], "skip_target_gate_done_terminal")
            self.assertEqual(payload["results"][0]["target_gate_done_marker"], str(marker.resolve()))

    def test_main_stops_active_runner_after_target_gate_done(self) -> None:
        now = datetime.now(timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            marker = output_dir / f"ousdt_target_gate_done_{now.strftime('%Y%m%d')}.flag"
            marker.write_text(now.isoformat(), encoding="utf-8")
            stdout = StringIO()

            with (
                patch.object(bq_volume_recovery_guard, "_runner_is_active", return_value=True),
                patch.object(bq_volume_recovery_guard, "_default_stop_runner") as stop_runner,
                redirect_stdout(stdout),
            ):
                exit_code = bq_volume_recovery_guard.main(
                    [
                        "--output-dir",
                        str(output_dir),
                        "--state-path",
                        str(output_dir / "state.json"),
                        "--runner-wrapper",
                        "/usr/local/bin/test-wrapper",
                        "--symbols",
                        "OUSDT",
                    ]
                )

            self.assertEqual(exit_code, 0)
            stop_runner.assert_called_once_with("OUSDT", runner_wrapper="/usr/local/bin/test-wrapper")
            payload = json.loads(stdout.getvalue())
            self.assertEqual(payload["results"][0]["action"], "stop_runner_target_gate_done")

    def test_restores_persistent_corrupt_state_from_recent_valid_backup(self) -> None:
        now = datetime(2026, 7, 11, 11, 20, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(output_dir, now=now - timedelta(minutes=10))
            state_path = output_dir / "reusdt_loop_state.json"
            state_path.write_text('{"broken":', encoding="utf-8")
            backup_path = output_dir / "reusdt_loop_state.json.bak_autorealign_123"
            _write_json(backup_path, {"best_quote_volume_ledger": {"long_lots": []}})

            result = recover_corrupt_loop_state(
                symbol="REUSDT",
                output_dir=output_dir,
                now=now,
                max_backup_age_seconds=3600,
                min_corrupt_age_seconds=0,
                max_snapshot_age_seconds=300,
                dry_run=False,
            )

            self.assertIsNotNone(result)
            assert result is not None
            self.assertEqual(result["action"], "restore_corrupt_state_from_autorealign_backup")
            self.assertEqual(
                json.loads(state_path.read_text(encoding="utf-8")),
                {"best_quote_volume_ledger": {"long_lots": []}},
            )
            self.assertTrue(Path(result["corrupt_archive_path"]).exists())
            self.assertTrue(Path(result["archived_plan_path"]).exists())

    def test_corrupt_state_recovery_respects_explicit_stop_reason(self) -> None:
        now = datetime(2026, 7, 11, 11, 21, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(output_dir, now=now - timedelta(minutes=10))
            state_path = output_dir / "reusdt_loop_state.json"
            state_path.write_text('{"broken":', encoding="utf-8")
            _write_json(
                output_dir / "reusdt_loop_state.json.bak_autorealign_123",
                {"best_quote_volume_ledger": {"long_lots": []}},
            )
            plan_path = output_dir / "reusdt_loop_latest_plan.json"
            plan = json.loads(plan_path.read_text(encoding="utf-8"))
            plan["stop_reason"] = "manual_stop"
            _write_json(plan_path, plan)

            result = recover_corrupt_loop_state(
                symbol="REUSDT",
                output_dir=output_dir,
                now=now,
                max_backup_age_seconds=3600,
                min_corrupt_age_seconds=0,
                max_snapshot_age_seconds=300,
                dry_run=False,
            )

            self.assertIsNotNone(result)
            assert result is not None
            self.assertEqual(result["action"], "skip_corrupt_state_recovery_safety_gate")
            self.assertIn("explicit_stop_reason", result["state_corruption"]["blocking_reasons"])
            with self.assertRaises(json.JSONDecodeError):
                json.loads(state_path.read_text(encoding="utf-8"))

    def test_main_checks_active_runner_normally(self) -> None:
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            stdout = StringIO()
            service = "grid-loop@ARXUSDT.service"
            active = subprocess.CompletedProcess(
                ["systemctl", "is-active", service],
                returncode=0,
                stdout="active\n",
                stderr="",
            )
            trades = [{"id": 1, "time": 1, "quoteQty": "10"}]
            checked_result = {
                "symbol": "ARXUSDT",
                "action": "healthy",
                "restart_failed": None,
            }

            with (
                patch.object(
                    bq_volume_recovery_guard.subprocess,
                    "run",
                    return_value=active,
                ) as run,
                patch.object(
                    bq_volume_recovery_guard,
                    "_fetch_exchange_user_trades",
                    return_value=trades,
                ) as fetch_trades,
                patch.object(
                    bq_volume_recovery_guard,
                    "check_symbol",
                    return_value=checked_result,
                ) as check_symbol_mock,
                redirect_stdout(stdout),
            ):
                exit_code = bq_volume_recovery_guard.main(
                    [
                        "--output-dir",
                        str(output_dir),
                        "--state-path",
                        str(output_dir / "state.json"),
                        "--symbols",
                        "ARXUSDT",
                        "--dry-run",
                    ]
                )

            self.assertEqual(exit_code, 0)
            run.assert_called_once_with(
                ["systemctl", "is-active", service],
                capture_output=True,
                text=True,
            )
            fetch_trades.assert_called_once()
            check_symbol_mock.assert_called_once()
            self.assertEqual(check_symbol_mock.call_args.kwargs["trade_rows"], trades)
            self.assertEqual(
                check_symbol_mock.call_args.kwargs["volume_source"],
                "exchange_user_trades",
            )
            payload = json.loads(stdout.getvalue())
            self.assertEqual(payload["results"], [checked_result])

    def test_main_skips_inactive_runner_before_exchange_fetch(self) -> None:
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            stdout = StringIO()
            service = "grid-loop@ARXUSDT.service"
            inactive = subprocess.CompletedProcess(
                ["systemctl", "is-active", service],
                returncode=3,
                stdout="inactive\n",
                stderr="",
            )
            unexpected_result = {
                "symbol": "ARXUSDT",
                "action": "unexpected_check_symbol",
                "restart_failed": None,
            }

            with (
                patch.object(
                    bq_volume_recovery_guard.subprocess,
                    "run",
                    return_value=inactive,
                ) as run,
                patch.object(
                    bq_volume_recovery_guard,
                    "_fetch_exchange_user_trades",
                    return_value=[],
                ) as fetch_trades,
                patch.object(
                    bq_volume_recovery_guard,
                    "check_symbol",
                    return_value=unexpected_result,
                ) as check_symbol_mock,
                redirect_stdout(stdout),
            ):
                exit_code = bq_volume_recovery_guard.main(
                    [
                        "--output-dir",
                        str(output_dir),
                        "--state-path",
                        str(output_dir / "state.json"),
                        "--symbols",
                        "ARXUSDT",
                        "--dry-run",
                    ]
                )

            self.assertEqual(exit_code, 0)
            run.assert_called_once_with(
                ["systemctl", "is-active", service],
                capture_output=True,
                text=True,
            )
            fetch_trades.assert_not_called()
            check_symbol_mock.assert_not_called()
            payload = json.loads(stdout.getvalue())
            event = json.loads(
                (output_dir / "bq_volume_recovery_guard_events.jsonl").read_text(encoding="utf-8")
            )
            self.assertEqual(payload["results"][0]["action"], "skip_runner_inactive_safety_gate")
            self.assertEqual(event["action"], "skip_runner_inactive_safety_gate")
            self.assertIn("missing_control", event["inactive_restart_gate"]["reasons"])

    def test_recent_volume_deduplicates_exchange_trade_ids(self) -> None:
        now = datetime(2026, 6, 26, 7, 0, tzinfo=timezone.utc)
        rows = [
            {"id": 101, "time": int((now - timedelta(seconds=10)).timestamp() * 1000), "quoteQty": "40"},
            {"id": 101, "time": int((now - timedelta(seconds=10)).timestamp() * 1000), "quoteQty": "40"},
            {"id": 102, "time": int((now - timedelta(seconds=20)).timestamp() * 1000), "quoteQty": "30"},
        ]

        summary = summarize_recent_volume(rows=rows, now=now, window_seconds=60)

        self.assertEqual(summary["trade_count"], 2)
        self.assertEqual(summary["gross_notional"], 70.0)

    def test_daily_target_pace_floor_raises_fixed_low_volume_threshold(self) -> None:
        now = datetime(2026, 7, 11, 4, 0, tzinfo=timezone.utc)
        rows = [
            {
                "id": 1,
                "time": int((now - timedelta(hours=3)).timestamp() * 1000),
                "quoteQty": "19840",
            },
            {
                "id": 2,
                "time": int((now - timedelta(seconds=30)).timestamp() * 1000),
                "quoteQty": "160",
            },
        ]
        summary = summarize_recent_volume(rows=rows, now=now, window_seconds=180)

        floor = apply_daily_target_pace_floor(
            volume_summary=summary,
            rows=rows,
            now=now,
            window_seconds=180,
            min_volume_notional=125,
            daily_target_notional=100000,
            target_pace_fraction=0.9,
            target_pace_max_multiplier=2.0,
        )

        self.assertEqual(summary["daily_gross_notional"], 20000.0)
        self.assertEqual(summary["required_hourly_notional"], 4000.0)
        self.assertEqual(summary["target_pace_floor_notional"], 180.0)
        self.assertEqual(floor, 180.0)

    def test_daily_target_pace_floor_is_bounded_by_static_multiplier(self) -> None:
        now = datetime(2026, 7, 11, 23, 0, tzinfo=timezone.utc)
        summary: dict[str, object] = {}

        floor = apply_daily_target_pace_floor(
            volume_summary=summary,
            rows=[],
            now=now,
            window_seconds=180,
            min_volume_notional=125,
            daily_target_notional=100000,
            target_pace_fraction=0.9,
            target_pace_max_multiplier=2.0,
        )

        self.assertEqual(summary["target_pace_floor_notional"], 250.0)
        self.assertEqual(floor, 250.0)

    def test_daily_target_pace_floor_supports_early_completion_deadline(self) -> None:
        now = datetime(2026, 7, 11, 4, 0, tzinfo=timezone.utc)
        rows = [
            {
                "id": 1,
                "time": int((now - timedelta(hours=3)).timestamp() * 1000),
                "quoteQty": "20000",
            }
        ]
        summary = summarize_recent_volume(rows=rows, now=now, window_seconds=180)

        floor = apply_daily_target_pace_floor(
            volume_summary=summary,
            rows=rows,
            now=now,
            window_seconds=180,
            min_volume_notional=125,
            daily_target_notional=100000,
            target_pace_fraction=0.9,
            target_pace_max_multiplier=5.0,
            target_completion_buffer_seconds=10800,
        )

        self.assertAlmostEqual(summary["required_hourly_notional"], 80000 / 17)
        self.assertAlmostEqual(summary["target_pace_floor_notional"], (80000 / 17) * 0.05 * 0.9)
        self.assertEqual(summary["target_completion_buffer_seconds"], 10800.0)
        self.assertEqual(summary["target_deadline"], "2026-07-11T21:00:00+00:00")
        self.assertAlmostEqual(floor, (80000 / 17) * 0.05 * 0.9)

    def test_parse_symbol_notionals_accepts_comma_separated_targets(self) -> None:
        targets = bq_volume_recovery_guard._parse_symbol_notionals(
            ["ARXUSDT=100000,OUSDT=60000", "INVALID", "BAD=0"]
        )

        self.assertEqual(targets, {"ARXUSDT": 100000.0, "OUSDT": 60000.0})

    def test_inactive_runner_restarts_after_safe_confirmation(self) -> None:
        now = datetime(2026, 6, 26, 7, 0, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "run_start_time": (now - timedelta(hours=1)).isoformat(),
                    "run_end_time": (now + timedelta(hours=1)).isoformat(),
                },
                open_order_count=2,
                active_order_count=2,
                orders_near_market=True,
            )
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "first_inactive_at": (now - timedelta(seconds=130)).isoformat(),
                    }
                }
            }
            restarts: list[str] = []

            result = recover_inactive_runner(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                trigger_seconds=120,
                restart_cooldown_seconds=600,
                max_snapshot_age_seconds=300,
                runner_wrapper="/usr/local/bin/grid-saved-runner",
                dry_run=False,
                restart_runner=restarts.append,
            )

            self.assertEqual(result["action"], "restart_runner_inactive")
            self.assertTrue(result["inactive_restart_gate"]["ok"])
            self.assertEqual(restarts, ["REUSDT"])

    def test_inactive_runner_does_not_restart_outside_run_window(self) -> None:
        now = datetime(2026, 6, 26, 7, 0, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "run_start_time": (now - timedelta(hours=2)).isoformat(),
                    "run_end_time": (now - timedelta(seconds=1)).isoformat(),
                },
                open_order_count=2,
                active_order_count=2,
                orders_near_market=True,
            )
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "first_inactive_at": (now - timedelta(seconds=130)).isoformat(),
                    }
                }
            }
            restarts: list[str] = []

            result = recover_inactive_runner(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                trigger_seconds=120,
                restart_cooldown_seconds=600,
                max_snapshot_age_seconds=300,
                runner_wrapper="/usr/local/bin/grid-saved-runner",
                dry_run=False,
                restart_runner=restarts.append,
            )

            self.assertEqual(result["action"], "skip_runner_inactive_safety_gate")
            self.assertIn("after_run_window", result["inactive_restart_gate"]["reasons"])
            self.assertEqual(restarts, [], result)

    def test_fetch_recent_user_trades_pages_and_deduplicates_ids(self) -> None:
        now = datetime(2026, 6, 26, 7, 0, tzinfo=timezone.utc)
        start_ms = int((now - timedelta(minutes=3)).timestamp() * 1000)
        first_time = start_ms + 1_000
        second_time = start_ms + 2_000
        calls: list[int] = []

        def fetch_page(*, start_time_ms: int, limit: int) -> list[dict[str, object]]:
            calls.append(start_time_ms)
            if len(calls) == 1:
                return ([{"id": index, "time": first_time, "quoteQty": "1"} for index in range(1_000)])
            return [
                {"id": 999, "time": first_time, "quoteQty": "1"},
                {"id": 1_000, "time": second_time, "quoteQty": "2"},
            ]

        rows = fetch_recent_user_trades(
            fetch_page=fetch_page,
            now=now,
            window_seconds=180,
            max_pages=3,
        )

        self.assertEqual(len(rows), 1_001)
        self.assertEqual(calls, [start_ms, first_time])

    def _write_common_files(
        self,
        output_dir: Path,
        *,
        now: datetime,
        control: dict[str, object] | None = None,
        long_notional: float = 990.0,
        short_notional: float = 980.0,
        open_order_count: int = 0,
        active_order_count: int = 0,
        orders_near_market: bool = False,
        recent_trade_notional: float = 0.0,
    ) -> None:
        control_payload = {
            "symbol": "REUSDT",
            "best_quote_maker_volume_allow_loss_reduce_only": False,
            "best_quote_maker_volume_net_loss_reduce_enabled": False,
            "best_quote_maker_volume_max_long_notional": 1000.0,
            "best_quote_maker_volume_max_short_notional": 1000.0,
        }
        if control:
            control_payload.update(control)
        _write_json(output_dir / "reusdt_loop_runner_control.json", control_payload)

        buy_price = 0.5968 if orders_near_market else 0.5800
        sell_price = 0.5972 if orders_near_market else 0.6200
        _write_json(
            output_dir / "reusdt_loop_latest_plan.json",
            {
                "generated_at": now.isoformat(),
                "symbol": "REUSDT",
                "bid_price": 0.5968,
                "ask_price": 0.5972,
                "symbol_info": {"tick_size": 0.0001},
                "current_long_notional": long_notional,
                "current_short_notional": short_notional,
                "effective_max_position_notional": 1000.0,
                "effective_max_short_position_notional": 1000.0,
                "open_order_count": open_order_count,
                "total_open_order_count": open_order_count,
                "buy_orders": ([{"side": "BUY", "price": buy_price, "qty": 16.0}] if open_order_count else []),
                "sell_orders": ([{"side": "SELL", "price": sell_price, "qty": 16.0}] if open_order_count else []),
            },
        )
        _write_json(
            output_dir / "reusdt_loop_latest_submit.json",
            {
                "generated_at": now.isoformat(),
                "observed_strategy_open_order_state": {"active_order_count": active_order_count},
                "plan_summary": {"open_order_count": open_order_count},
                "validation": {"actions": {"place_orders": []}},
                "live_book": {"bid_price": 0.5968, "ask_price": 0.5972},
            },
        )

        rows = []
        if recent_trade_notional > 0:
            rows.append(
                {
                    "time": int((now - timedelta(seconds=20)).timestamp() * 1000),
                    "quoteQty": recent_trade_notional,
                }
            )
        else:
            rows.append({"time": int((now - timedelta(minutes=10)).timestamp() * 1000), "quoteQty": 500})
        _append_jsonl(output_dir / "reusdt_loop_trade_audit.jsonl", rows)

    def test_enables_temporary_allow_loss_after_persistent_near_cap_stall(self) -> None:
        now = datetime(2026, 6, 26, 7, 0, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={"best_quote_maker_volume_net_loss_reduce_enabled": True},
            )
            state: dict[str, object] = {}
            restarts: list[str] = []

            first = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=1,
                trigger_seconds=120,
                restart_runner=restarts.append,
            )
            second = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now + timedelta(seconds=130),
                window_seconds=60,
                min_volume_notional=1,
                trigger_seconds=120,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))
            backups = list(output_dir.glob("reusdt_loop_runner_control.json.bak_bq_volume_recovery_*"))

            self.assertEqual(first["action"], "wait_low_volume_confirmation")
            self.assertEqual(second["action"], "enable_allow_loss_reduce_only")
            self.assertTrue(control["best_quote_maker_volume_allow_loss_reduce_only"])
            self.assertFalse(control["best_quote_maker_volume_net_loss_reduce_enabled"])
            self.assertEqual(restarts, ["REUSDT"])
            self.assertEqual(len(backups), 1)

    def test_enables_temporary_allow_loss_when_recent_volume_masks_zero_active_orders(self) -> None:
        now = datetime(2026, 6, 26, 7, 5, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_min_cycle_budget_notional": 24.0,
                    "pause_buy_position_notional": 900.0,
                    "pause_short_position_notional": 900.0,
                },
                long_notional=500.0,
                short_notional=550.0,
                open_order_count=0,
                active_order_count=0,
                recent_trade_notional=100.0,
            )
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "low_volume",
                        "first_low_volume_at": (now - timedelta(minutes=4)).isoformat(),
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=1,
                trigger_seconds=120,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))

            self.assertEqual(result["action"], "enable_allow_loss_reduce_only")
            self.assertIn("no_active_orders", result["assessment"]["reasons"])
            self.assertTrue(result["assessment"]["low_volume"])
            self.assertTrue(control["best_quote_maker_volume_allow_loss_reduce_only"])
            self.assertEqual(control["pause_buy_position_notional"], 476.0)
            self.assertEqual(control["pause_short_position_notional"], 526.0)
            self.assertEqual(restarts, ["REUSDT"])

    def test_keeps_allow_loss_enabled_until_inventory_has_recovered_buffer(self) -> None:
        now = datetime(2026, 6, 26, 7, 10, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={"best_quote_maker_volume_allow_loss_reduce_only": True},
                long_notional=982.0,
                short_notional=978.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
                recent_trade_notional=50.0,
            )
            state: dict[str, object] = {"symbols": {"REUSDT": {"status": "recovery_active"}}}
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=1,
                trigger_seconds=120,
                recover_min_volume_notional=10,
                recover_cap_ratio=0.96,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))

            self.assertEqual(result["action"], "hold_recovery_until_cap_buffer")
            self.assertTrue(control["best_quote_maker_volume_allow_loss_reduce_only"])
            self.assertEqual(restarts, [])

    def test_forces_net_loss_reduce_off_while_holding_recovery(self) -> None:
        now = datetime(2026, 6, 26, 7, 11, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_allow_loss_reduce_only": True,
                    "best_quote_maker_volume_net_loss_reduce_enabled": True,
                },
                long_notional=982.0,
                short_notional=978.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
                recent_trade_notional=50.0,
            )
            state: dict[str, object] = {"symbols": {"REUSDT": {"status": "recovery_active"}}}
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=1,
                trigger_seconds=120,
                recover_min_volume_notional=10,
                recover_cap_ratio=0.96,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))

            self.assertEqual(result["action"], "disable_net_loss_reduce")
            self.assertTrue(control["best_quote_maker_volume_allow_loss_reduce_only"])
            self.assertFalse(control["best_quote_maker_volume_net_loss_reduce_enabled"])
            self.assertEqual(restarts, ["REUSDT"])

    def test_restores_allow_loss_after_volume_orders_and_cap_buffer_recover(self) -> None:
        now = datetime(2026, 6, 26, 7, 12, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={"best_quote_maker_volume_allow_loss_reduce_only": True},
                long_notional=940.0,
                short_notional=930.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
                recent_trade_notional=80.0,
            )
            state: dict[str, object] = {"symbols": {"REUSDT": {"status": "recovery_active"}}}
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=1,
                trigger_seconds=120,
                recover_min_volume_notional=10,
                recover_cap_ratio=0.96,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))

            self.assertEqual(result["action"], "disable_allow_loss_after_recovery")
            self.assertFalse(control["best_quote_maker_volume_allow_loss_reduce_only"])
            self.assertFalse(control["best_quote_maker_volume_net_loss_reduce_enabled"])
            self.assertEqual(restarts, ["REUSDT"])

    def test_recovery_owned_loss_reduce_closes_once_both_sides_are_below_original_soft(self) -> None:
        now = datetime(2026, 6, 26, 7, 15, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_allow_loss_reduce_only": True,
                    "best_quote_maker_volume_cycle_budget_notional": 72.0,
                    "best_quote_maker_volume_inventory_soft_ratio": 0.8,
                    "pause_buy_position_notional": 600.0,
                    "pause_short_position_notional": 600.0,
                },
                long_notional=850.0,
                short_notional=820.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
                recent_trade_notional=20.0,
            )
            plan_path = output_dir / "reusdt_loop_latest_plan.json"
            plan = json.loads(plan_path.read_text(encoding="utf-8"))
            plan["best_quote_maker_volume"] = {
                "reduce_freeze": {
                    "actual_long_notional": 750.0,
                    "actual_short_notional": 700.0,
                    "frozen_long_notional": 0.0,
                    "frozen_short_notional": 0.0,
                }
            }
            _write_json(plan_path, plan)
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "recovery_active",
                        "recovery_owned": True,
                        "recovery_started_at": (now - timedelta(minutes=3)).isoformat(),
                        "guard_original_controls": {
                            "best_quote_maker_volume_allow_loss_reduce_only": False,
                            "best_quote_maker_volume_cycle_budget_notional": 72.0,
                            "pause_buy_position_notional": 800.0,
                            "pause_short_position_notional": 800.0,
                        },
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=100,
                trigger_seconds=120,
                recovery_min_hold_seconds=120,
                cycle_budget_floor_notional=108,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))
            self.assertEqual(result["action"], "restore_after_inventory_below_soft")
            self.assertFalse(control["best_quote_maker_volume_allow_loss_reduce_only"])
            self.assertEqual(control["best_quote_maker_volume_cycle_budget_notional"], 108.0)
            self.assertEqual(control["pause_buy_position_notional"], 800.0)
            self.assertEqual(control["pause_short_position_notional"], 800.0)
            self.assertEqual(state["symbols"]["REUSDT"]["status"], "normal")
            self.assertIn("cooldown_until", state["symbols"]["REUSDT"])
            self.assertEqual(
                state["symbols"]["REUSDT"]["post_restore_budget_cooldown_until"],
                state["symbols"]["REUSDT"]["cooldown_until"],
            )
            self.assertEqual(restarts, ["REUSDT"])

    def test_frozen_inventory_prevents_actual_position_recovery_fallback(self) -> None:
        recovered = bq_volume_recovery_guard._recovery_inventory_buffer_ok(
            {
                "current_long_notional": 850.0,
                "current_short_notional": 820.0,
                "actual_long_notional": 750.0,
                "actual_short_notional": 700.0,
                "frozen_total_notional": 100.0,
                "max_long_notional": 1000.0,
                "max_short_notional": 1000.0,
                "long_soft_limit_notional": 600.0,
                "short_soft_limit_notional": 600.0,
                "inventory_soft_ratio": 0.8,
            },
            recover_cap_ratio=0.96,
            original_controls={
                "pause_buy_position_notional": 800.0,
                "pause_short_position_notional": 800.0,
            },
        )

        self.assertFalse(recovered)

    def test_recovery_buffer_applies_below_soft_boundary(self) -> None:
        assessment = {
            "current_long_notional": 790.0,
            "current_short_notional": 700.0,
            "actual_long_notional": 790.0,
            "actual_short_notional": 700.0,
            "frozen_total_notional": 0.0,
            "max_long_notional": 1000.0,
            "max_short_notional": 1000.0,
            "long_soft_limit_notional": 800.0,
            "short_soft_limit_notional": 800.0,
            "inventory_soft_ratio": 0.8,
        }

        self.assertFalse(
            bq_volume_recovery_guard._recovery_inventory_buffer_ok(
                assessment,
                recover_cap_ratio=0.96,
                original_controls={
                    "pause_buy_position_notional": 800.0,
                    "pause_short_position_notional": 800.0,
                },
            )
        )
        assessment["actual_long_notional"] = 760.0
        self.assertTrue(
            bq_volume_recovery_guard._recovery_inventory_buffer_ok(
                assessment,
                recover_cap_ratio=0.96,
                original_controls={
                    "pause_buy_position_notional": 800.0,
                    "pause_short_position_notional": 800.0,
                },
            )
        )

    def test_loss_reduce_temporarily_adds_one_quote_offset_tick_without_ratchet(self) -> None:
        updates = bq_volume_recovery_guard._loss_reduce_recovery_updates(
            control={
                "best_quote_maker_volume_allow_loss_reduce_only": False,
                "best_quote_maker_volume_quote_offset_ticks": 0,
            },
            assessment={},
            quote_offset_extra_ticks=1,
        )
        self.assertEqual(updates["best_quote_maker_volume_quote_offset_ticks"], 1)

        already_active = bq_volume_recovery_guard._loss_reduce_recovery_updates(
            control={
                "best_quote_maker_volume_allow_loss_reduce_only": True,
                "best_quote_maker_volume_quote_offset_ticks": 1,
            },
            assessment={},
            quote_offset_extra_ticks=1,
        )
        self.assertNotIn("best_quote_maker_volume_quote_offset_ticks", already_active)

    def test_dry_run_reports_recovery_without_mutating_control(self) -> None:
        now = datetime(2026, 6, 26, 7, 20, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(output_dir, now=now)
            before = (output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8")
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "low_volume",
                        "first_low_volume_at": (now - timedelta(minutes=5)).isoformat(),
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=1,
                trigger_seconds=120,
                dry_run=True,
                restart_runner=restarts.append,
            )
            after = (output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8")

            self.assertEqual(result["action"], "dry_run_enable_allow_loss_reduce_only")
            self.assertEqual(before, after)
            self.assertEqual(restarts, [])

    def test_restores_only_guard_changed_cost_gate_after_volume_recovers(self) -> None:
        now = datetime(2026, 6, 26, 7, 30, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_allow_loss_reduce_only": True,
                    "best_quote_maker_volume_inventory_cost_gate_enabled": False,
                },
                long_notional=940.0,
                short_notional=930.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
                recent_trade_notional=80.0,
            )
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "recovery_active",
                        "guard_original_controls": {
                            "best_quote_maker_volume_inventory_cost_gate_enabled": True,
                        },
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=1,
                trigger_seconds=120,
                recover_min_volume_notional=10,
                recover_cap_ratio=0.96,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))

            self.assertEqual(result["action"], "restore_recovery_controls")
            self.assertFalse(control["best_quote_maker_volume_allow_loss_reduce_only"])
            self.assertTrue(control["best_quote_maker_volume_inventory_cost_gate_enabled"])
            self.assertEqual(restarts, ["REUSDT"])

    def test_times_out_temporary_loss_reduce_and_enters_cooldown(self) -> None:
        now = datetime(2026, 6, 26, 7, 40, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={"best_quote_maker_volume_allow_loss_reduce_only": True},
                long_notional=982.0,
                short_notional=978.0,
            )
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "recovery_active",
                        "recovery_started_at": (now - timedelta(minutes=6)).isoformat(),
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=1,
                trigger_seconds=120,
                max_recovery_seconds=300,
                cooldown_seconds=600,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))
            item = state["symbols"]["REUSDT"]

            self.assertEqual(result["action"], "recovery_timeout_cooldown")
            self.assertFalse(control["best_quote_maker_volume_allow_loss_reduce_only"])
            self.assertEqual(item["status"], "cooldown")
            self.assertEqual(restarts, ["REUSDT"])

    def test_relaxes_ordinary_inventory_bias_when_effective_orders_still_have_no_volume(self) -> None:
        now = datetime(2026, 6, 26, 7, 50, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={"best_quote_maker_volume_inventory_bias_min_notional_gap": 80.0},
                long_notional=990.0,
                short_notional=850.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
            )
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "low_volume",
                        "first_low_volume_at": (now - timedelta(minutes=4)).isoformat(),
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=1,
                trigger_seconds=120,
                inventory_bias_relief_notional_margin=24,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))

            self.assertEqual(result["action"], "relax_inventory_bias_for_volume")
            self.assertEqual(control["best_quote_maker_volume_inventory_bias_min_notional_gap"], 164.0)
            self.assertFalse(control["best_quote_maker_volume_allow_loss_reduce_only"])
            self.assertEqual(restarts, ["REUSDT"])

    def test_restores_inventory_bias_after_volume_recovers(self) -> None:
        now = datetime(2026, 6, 26, 8, 0, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={"best_quote_maker_volume_inventory_bias_min_notional_gap": 164.0},
                long_notional=940.0,
                short_notional=850.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
                recent_trade_notional=80.0,
            )
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "recovery_active",
                        "guard_original_controls": {
                            "best_quote_maker_volume_inventory_bias_min_notional_gap": 80.0,
                        },
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=1,
                trigger_seconds=120,
                recover_min_volume_notional=10,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))

            self.assertEqual(result["action"], "restore_recovery_controls")
            self.assertEqual(control["best_quote_maker_volume_inventory_bias_min_notional_gap"], 80.0)
            self.assertEqual(restarts, ["REUSDT"])

    def test_enables_bounded_loss_reduce_when_bias_is_already_relaxed_but_volume_stays_zero(self) -> None:
        now = datetime(2026, 6, 26, 8, 10, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={"best_quote_maker_volume_inventory_bias_min_notional_gap": 200.0},
                long_notional=990.0,
                short_notional=850.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
            )
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "low_volume",
                        "first_low_volume_at": (now - timedelta(minutes=4)).isoformat(),
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=1,
                trigger_seconds=120,
                inventory_bias_relief_notional_margin=24,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))

            self.assertEqual(result["action"], "enable_allow_loss_reduce_only")
            self.assertTrue(control["best_quote_maker_volume_allow_loss_reduce_only"])
            self.assertEqual(restarts, ["REUSDT"])

    def test_enables_loss_reduce_when_near_market_fills_stay_below_volume_floor(self) -> None:
        now = datetime(2026, 6, 26, 8, 12, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={"best_quote_maker_volume_inventory_bias_min_notional_gap": 200.0},
                long_notional=990.0,
                short_notional=850.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
                recent_trade_notional=70.0,
            )
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "low_volume",
                        "first_low_volume_at": (now - timedelta(minutes=4)).isoformat(),
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=100,
                trigger_seconds=120,
                inventory_bias_relief_notional_margin=24,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))

            self.assertEqual(result["action"], "enable_allow_loss_reduce_only")
            self.assertTrue(control["best_quote_maker_volume_allow_loss_reduce_only"])
            self.assertEqual(restarts, ["REUSDT"])

    def test_holds_near_market_flow_at_recovery_floor(self) -> None:
        now = datetime(2026, 6, 26, 8, 12, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={"best_quote_maker_volume_inventory_bias_min_notional_gap": 200.0},
                long_notional=990.0,
                short_notional=850.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
                recent_trade_notional=70.0,
            )
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "low_volume",
                        "first_low_volume_at": (now - timedelta(minutes=4)).isoformat(),
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=100,
                recover_min_volume_notional=70,
                trigger_seconds=120,
                inventory_bias_relief_notional_margin=24,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))

            self.assertEqual(result["action"], "hold_effective_near_market_flow")
            self.assertFalse(control["best_quote_maker_volume_allow_loss_reduce_only"])
            self.assertEqual(restarts, [])

    def test_manual_loss_reduce_close_wins_when_near_market_flow_is_effective(self) -> None:
        now = datetime(2026, 6, 26, 8, 13, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_allow_loss_reduce_only": False,
                    "best_quote_maker_volume_inventory_bias_min_notional_gap": 164.0,
                },
                long_notional=990.0,
                short_notional=850.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
                recent_trade_notional=120.0,
            )
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "recovery_active",
                        "recovery_started_at": (now - timedelta(minutes=3)).isoformat(),
                        "guard_original_controls": {
                            "best_quote_maker_volume_allow_loss_reduce_only": False,
                            "best_quote_maker_volume_inventory_bias_min_notional_gap": 80.0,
                        },
                        "guard_recovery_controls": {
                            "best_quote_maker_volume_allow_loss_reduce_only": True,
                            "best_quote_maker_volume_inventory_bias_min_notional_gap": 164.0,
                        },
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=100,
                trigger_seconds=120,
                recover_min_volume_notional=100,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))
            item = state["symbols"]["REUSDT"]

            self.assertEqual(result["action"], "abandon_loss_reduce_for_effective_flow")
            self.assertFalse(control["best_quote_maker_volume_allow_loss_reduce_only"])
            self.assertEqual(control["best_quote_maker_volume_inventory_bias_min_notional_gap"], 80.0)
            self.assertEqual(item["status"], "normal")
            self.assertNotIn("guard_recovery_controls", item)
            self.assertEqual(restarts, ["REUSDT"])

    def test_does_not_relax_inventory_bias_when_current_gap_is_below_threshold(self) -> None:
        now = datetime(2026, 6, 26, 8, 15, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={"best_quote_maker_volume_inventory_bias_min_notional_gap": 150.0},
                long_notional=990.0,
                short_notional=850.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
            )
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "low_volume",
                        "first_low_volume_at": (now - timedelta(minutes=4)).isoformat(),
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=1,
                trigger_seconds=120,
                inventory_bias_relief_notional_margin=24,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))

            self.assertEqual(result["action"], "enable_allow_loss_reduce_only")
            self.assertEqual(control["best_quote_maker_volume_inventory_bias_min_notional_gap"], 150.0)
            self.assertTrue(control["best_quote_maker_volume_allow_loss_reduce_only"])
            self.assertEqual(restarts, ["REUSDT"])

            self._write_common_files(
                output_dir,
                now=now,
                control={"best_quote_maker_volume_inventory_bias_min_notional_gap": 150.0},
                long_notional=990.0,
                short_notional=850.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
            )
            state = {
                "symbols": {
                    "REUSDT": {
                        "status": "low_volume",
                        "first_low_volume_at": (now - timedelta(minutes=4)).isoformat(),
                    }
                }
            }
            restarts.clear()
            gated = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=1,
                trigger_seconds=120,
                inventory_bias_relief_notional_margin=24,
                require_soft_pressure_for_allow_loss=True,
                restart_runner=restarts.append,
            )
            gated_control = json.loads(
                (output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8")
            )
            self.assertEqual(gated["action"], "hold_low_volume_without_soft_pressure")
            self.assertFalse(gated_control["best_quote_maker_volume_allow_loss_reduce_only"])
            self.assertEqual(restarts, [])

    def test_raises_cycle_budget_when_orders_are_effective_and_inventory_is_not_near_cap(self) -> None:
        now = datetime(2026, 6, 26, 8, 20, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={"best_quote_maker_volume_cycle_budget_notional": 48.0},
                long_notional=800.0,
                short_notional=700.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
            )
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "low_volume",
                        "first_low_volume_at": (now - timedelta(minutes=4)).isoformat(),
                        "recovery_started_at": (now - timedelta(minutes=20)).isoformat(),
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=1,
                trigger_seconds=120,
                volume_recovery_cycle_budget_increment=12,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))

            self.assertEqual(result["action"], "raise_cycle_budget_for_volume")
            self.assertEqual(control["best_quote_maker_volume_cycle_budget_notional"], 60.0)
            self.assertEqual(state["symbols"]["REUSDT"]["recovery_started_at"], now.isoformat())
            self.assertEqual(restarts, ["REUSDT"])

    def test_cycle_budget_floor_recovers_external_budget_override_in_one_step(self) -> None:
        now = datetime(2026, 6, 26, 8, 22, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={"best_quote_maker_volume_cycle_budget_notional": 72.0},
                long_notional=400.0,
                short_notional=350.0,
                open_order_count=2,
                active_order_count=2,
                orders_near_market=True,
            )
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "low_volume",
                        "first_low_volume_at": (now - timedelta(minutes=4)).isoformat(),
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=100,
                trigger_seconds=120,
                volume_recovery_cycle_budget_increment=12,
                cycle_budget_floor_notional=108,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))
            self.assertEqual(result["action"], "raise_cycle_budget_for_volume")
            self.assertEqual(control["best_quote_maker_volume_cycle_budget_notional"], 108.0)
            self.assertEqual(restarts, ["REUSDT"])

    def test_target_pace_cycle_floor_raises_one_bounded_step(self) -> None:
        now = datetime(2026, 7, 11, 12, 0, tzinfo=timezone.utc)
        rows = [
            {
                "id": 1,
                "time": int((now - timedelta(hours=2)).timestamp() * 1000),
                "quoteQty": "80000",
            },
            {
                "id": 2,
                "time": int((now - timedelta(minutes=30)).timestamp() * 1000),
                "quoteQty": "4000",
            },
        ]
        volume_summary = {
            "required_hourly_notional": 11000.0,
        }
        floor = apply_target_pace_cycle_budget_floor(
            volume_summary=volume_summary,
            rows=rows,
            now=now,
            control={
                "best_quote_maker_volume_cycle_budget_notional": 128.0,
                "per_order_notional": 32.0,
                "maker_order_notional": 32.0,
                "buy_levels": 4,
                "sell_levels": 4,
            },
            assessment={
                "actual_long_notional": 400.0,
                "actual_short_notional": 350.0,
                "frozen_total_notional": 0.0,
                "max_long_notional": 700.0,
                "max_short_notional": 700.0,
            },
            static_floor_notional=108.0,
            target_pace_fraction=0.9,
            cycle_budget_increment=12.0,
        )

        self.assertEqual(floor, 192.0)
        self.assertEqual(volume_summary["trailing_60m_gross_notional"], 4000.0)
        self.assertEqual(volume_summary["target_cycle_budget_ladder_capacity"], 256.0)

    def test_target_pace_budget_is_capped_during_bounded_loss_reduce(self) -> None:
        now = datetime(2026, 7, 11, 12, 0, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_allow_loss_reduce_only": True,
                    "best_quote_maker_volume_cycle_budget_notional": 144.0,
                    "per_order_notional": 18.0,
                    "maker_order_notional": 18.0,
                    "buy_levels": 4,
                    "sell_levels": 4,
                },
                long_notional=400.0,
                short_notional=700.0,
                open_order_count=2,
                active_order_count=2,
                orders_near_market=True,
            )
            state: dict[str, object] = {}
            restarts: list[str] = []
            rows = [
                {
                    "id": 1,
                    "time": int((now - timedelta(hours=2)).timestamp() * 1000),
                    "quoteQty": "80000",
                },
                {
                    "id": 2,
                    "time": int((now - timedelta(minutes=30)).timestamp() * 1000),
                    "quoteQty": "3000",
                },
            ]

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=180,
                min_volume_notional=100,
                trigger_seconds=120,
                daily_target_notional=180000,
                target_completion_buffer_seconds=10800,
                cycle_budget_floor_notional=108,
                volume_recovery_cycle_budget_increment=12,
                trade_rows=rows,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))
            self.assertEqual(result["action"], "cap_loss_reduce_budget_for_wear")
            self.assertEqual(control["best_quote_maker_volume_cycle_budget_notional"], 108.0)
            self.assertTrue(control["best_quote_maker_volume_allow_loss_reduce_only"])
            self.assertEqual(restarts, ["REUSDT"])

    def test_target_pace_raise_respects_validation_safety_gate(self) -> None:
        now = datetime(2026, 7, 11, 12, 0, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_cycle_budget_notional": 108.0,
                    "per_order_notional": 18.0,
                    "maker_order_notional": 18.0,
                    "buy_levels": 4,
                    "sell_levels": 4,
                },
                long_notional=400.0,
                short_notional=350.0,
                open_order_count=2,
                active_order_count=2,
                orders_near_market=True,
            )
            submit_path = output_dir / "reusdt_loop_latest_submit.json"
            submit = json.loads(submit_path.read_text(encoding="utf-8"))
            submit["validation"] = {"ok": False, "errors": ["invalid order"]}
            _write_json(submit_path, submit)
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state={},
                now=now,
                window_seconds=180,
                min_volume_notional=100,
                trigger_seconds=120,
                daily_target_notional=180000,
                target_completion_buffer_seconds=10800,
                cycle_budget_floor_notional=108,
                trade_rows=[],
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))
            self.assertEqual(result["action"], "skip_recovery_safety_gate")
            self.assertIn("latest_validation_failed", result["recovery_gate"]["reasons"])
            self.assertEqual(control["best_quote_maker_volume_cycle_budget_notional"], 108.0)
            self.assertEqual(restarts, [])

    def test_restores_explicit_pause_baseline_after_recovery_drift(self) -> None:
        now = datetime(2026, 7, 11, 12, 5, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "pause_buy_position_notional": 529.0,
                    "pause_short_position_notional": 620.0,
                },
                long_notional=300.0,
                short_notional=500.0,
                open_order_count=2,
                active_order_count=2,
                orders_near_market=True,
            )
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state={},
                now=now,
                window_seconds=180,
                min_volume_notional=100,
                trigger_seconds=120,
                pause_baseline_long_notional=620.0,
                pause_baseline_short_notional=620.0,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))
            self.assertEqual(result["action"], "restore_pause_baseline_after_recovery_drift")
            self.assertEqual(control["pause_buy_position_notional"], 620.0)
            self.assertEqual(control["pause_short_position_notional"], 620.0)
            self.assertEqual(restarts, ["REUSDT"])

    def test_disables_active_pair_reduce_when_it_suppresses_the_only_entry(self) -> None:
        now = datetime(2026, 7, 11, 12, 10, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={"best_quote_maker_volume_active_pair_reduce_enabled": True},
                long_notional=280.0,
                short_notional=315.0,
                open_order_count=0,
                active_order_count=0,
            )
            plan_path = output_dir / "reusdt_loop_latest_plan.json"
            plan = json.loads(plan_path.read_text(encoding="utf-8"))
            plan["best_quote_active_pair_reduce"] = {
                "enabled": True,
                "active": True,
                "reason": "no_valid_reduce_order",
                "order_count": 0,
                "suppressed_entry_order_count": 1,
            }
            _write_json(plan_path, plan)
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state={},
                now=now,
                window_seconds=180,
                min_volume_notional=100,
                trigger_seconds=120,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))
            self.assertEqual(result["action"], "disable_stalled_active_pair_reduce_suppression")
            self.assertFalse(control["best_quote_maker_volume_active_pair_reduce_enabled"])
            self.assertEqual(restarts, ["REUSDT"])

    def test_disables_stale_soft_pair_reduce_after_actual_inventory_recovers(self) -> None:
        now = datetime(2026, 7, 11, 12, 12, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_active_pair_reduce_enabled": True,
                    "pause_buy_position_notional": 740.0,
                    "pause_short_position_notional": 740.0,
                },
                long_notional=470.0,
                short_notional=450.0,
                open_order_count=4,
                active_order_count=2,
            )
            plan_path = output_dir / "reusdt_loop_latest_plan.json"
            plan = json.loads(plan_path.read_text(encoding="utf-8"))
            plan["best_quote_maker_volume"] = {
                "reduce_freeze": {
                    "actual_long_notional": 470.0,
                    "actual_short_notional": 450.0,
                    "frozen_long_notional": 0.0,
                    "frozen_short_notional": 0.0,
                }
            }
            plan["best_quote_active_pair_reduce"] = {
                "enabled": True,
                "active": True,
                "reason": "soft_pair_reduce",
                "order_count": 4,
                "normal_entry_suppressed": True,
                "suppressed_entry_order_count": 2,
                "completed": False,
            }
            _write_json(plan_path, plan)
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state={},
                now=now,
                window_seconds=180,
                min_volume_notional=100,
                trigger_seconds=120,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))
            self.assertEqual(result["action"], "disable_stalled_active_pair_reduce_suppression")
            self.assertTrue(result["assessment"]["active_pair_reduce_below_soft_deadlock"])
            self.assertFalse(control["best_quote_maker_volume_active_pair_reduce_enabled"])
            self.assertEqual(restarts, ["REUSDT"])

    def test_keeps_soft_pair_reduce_when_actual_inventory_is_not_below_soft(self) -> None:
        now = datetime(2026, 7, 11, 12, 13, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_active_pair_reduce_enabled": True,
                    "pause_buy_position_notional": 740.0,
                    "pause_short_position_notional": 740.0,
                },
                long_notional=760.0,
                short_notional=510.0,
                open_order_count=1,
                active_order_count=1,
            )
            plan_path = output_dir / "reusdt_loop_latest_plan.json"
            plan = json.loads(plan_path.read_text(encoding="utf-8"))
            plan["best_quote_maker_volume"] = {
                "reduce_freeze": {
                    "actual_long_notional": 760.0,
                    "actual_short_notional": 510.0,
                    "frozen_long_notional": 0.0,
                    "frozen_short_notional": 0.0,
                }
            }
            plan["best_quote_active_pair_reduce"] = {
                "enabled": True,
                "active": True,
                "reason": "soft_pair_reduce",
                "order_count": 1,
                "normal_entry_suppressed": True,
                "suppressed_entry_order_count": 2,
                "completed": False,
            }
            _write_json(plan_path, plan)

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state={},
                now=now,
                window_seconds=180,
                min_volume_notional=100,
                trigger_seconds=120,
                restart_runner=lambda _: None,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))
            self.assertFalse(result["assessment"]["active_pair_reduce_below_soft_deadlock"])
            self.assertTrue(control["best_quote_maker_volume_active_pair_reduce_enabled"])

    def test_does_not_enable_loss_reduce_for_stale_ledger_soft_pressure(self) -> None:
        now = datetime(2026, 7, 11, 12, 14, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "pause_buy_position_notional": 800.0,
                    "pause_short_position_notional": 800.0,
                },
                long_notional=810.0,
                short_notional=805.0,
                open_order_count=0,
                active_order_count=0,
            )
            plan_path = output_dir / "reusdt_loop_latest_plan.json"
            plan = json.loads(plan_path.read_text(encoding="utf-8"))
            plan["best_quote_maker_volume"] = {
                "reduce_freeze": {
                    "actual_long_notional": 500.0,
                    "actual_short_notional": 450.0,
                    "frozen_long_notional": 0.0,
                    "frozen_short_notional": 0.0,
                }
            }
            _write_json(plan_path, plan)

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state={},
                now=now,
                window_seconds=180,
                min_volume_notional=100,
                trigger_seconds=0,
                pause_baseline_long_notional=800.0,
                pause_baseline_short_notional=800.0,
                restart_runner=lambda _: None,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))
            self.assertTrue(result["assessment"]["inventory_soft_pressure"])
            self.assertTrue(result["assessment"]["actual_inventory_below_soft"])
            self.assertFalse(result["assessment"]["effective_inventory_soft_pressure"])
            self.assertEqual(result["action"], "hold_loss_reduce_when_actual_inventory_below_soft")
            self.assertFalse(control["best_quote_maker_volume_allow_loss_reduce_only"])

    def test_soft_pressure_blocks_budget_raise_when_target_pace_is_behind(self) -> None:
        now = datetime(2026, 7, 11, 12, 14, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_cycle_budget_notional": 108.0,
                    "pause_buy_position_notional": 800.0,
                    "pause_short_position_notional": 800.0,
                },
                long_notional=850.0,
                short_notional=850.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
                recent_trade_notional=120.0,
            )
            plan_path = output_dir / "reusdt_loop_latest_plan.json"
            plan = json.loads(plan_path.read_text(encoding="utf-8"))
            plan["best_quote_maker_volume"] = {
                "reduce_freeze": {
                    "actual_long_notional": 500.0,
                    "actual_short_notional": 450.0,
                    "frozen_long_notional": 0.0,
                    "frozen_short_notional": 0.0,
                }
            }
            _write_json(plan_path, plan)
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "cooldown",
                        "cooldown_until": (now + timedelta(minutes=5)).isoformat(),
                        "first_low_volume_at": (now - timedelta(minutes=4)).isoformat(),
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=180,
                min_volume_notional=100,
                trigger_seconds=120,
                daily_target_notional=10_000.0,
                target_pace_fraction=1.05,
                target_pace_max_multiplier=1.0,
                cycle_budget_floor_notional=108.0,
                volume_recovery_cycle_budget_increment=12.0,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))
            self.assertEqual(result["action"], "relax_inventory_bias_for_volume")
            self.assertFalse(result["assessment"]["low_volume"])
            self.assertTrue(result["assessment"]["target_pace_behind"])
            self.assertTrue(result["assessment"]["inventory_soft_pressure"])
            self.assertEqual(control["best_quote_maker_volume_cycle_budget_notional"], 108.0)
            self.assertFalse(control["best_quote_maker_volume_allow_loss_reduce_only"])
            self.assertEqual(restarts, ["REUSDT"])

    def test_post_restore_cooldown_blocks_target_pace_budget_raise(self) -> None:
        now = datetime(2026, 7, 11, 12, 14, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_cycle_budget_notional": 108.0,
                    "pause_buy_position_notional": 800.0,
                    "pause_short_position_notional": 800.0,
                },
                long_notional=500.0,
                short_notional=450.0,
                open_order_count=2,
                active_order_count=2,
                orders_near_market=True,
                recent_trade_notional=20.0,
            )
            cooldown_until = (now + timedelta(minutes=5)).isoformat()
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "normal",
                        "cooldown_until": cooldown_until,
                        "post_restore_budget_cooldown_until": cooldown_until,
                        "first_low_volume_at": (now - timedelta(minutes=4)).isoformat(),
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=180,
                min_volume_notional=100,
                trigger_seconds=120,
                daily_target_notional=10_000.0,
                target_pace_fraction=1.05,
                target_pace_max_multiplier=1.0,
                cycle_budget_floor_notional=108.0,
                volume_recovery_cycle_budget_increment=12.0,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))
            self.assertNotEqual(result["action"], "raise_cycle_budget_for_volume")
            self.assertEqual(control["best_quote_maker_volume_cycle_budget_notional"], 108.0)
            self.assertEqual(restarts, [])

    def test_recent_non_safety_action_debounces_budget_raise(self) -> None:
        now = datetime(2026, 7, 11, 12, 14, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_cycle_budget_notional": 108.0,
                    "pause_buy_position_notional": 800.0,
                    "pause_short_position_notional": 800.0,
                },
                long_notional=500.0,
                short_notional=450.0,
                open_order_count=2,
                active_order_count=2,
                orders_near_market=True,
                recent_trade_notional=20.0,
            )
            plan_path = output_dir / "reusdt_loop_latest_plan.json"
            plan = json.loads(plan_path.read_text(encoding="utf-8"))
            plan["best_quote_maker_volume"] = {
                "reduce_freeze": {
                    "actual_long_notional": 500.0,
                    "actual_short_notional": 450.0,
                    "frozen_long_notional": 0.0,
                    "frozen_short_notional": 0.0,
                }
            }
            _write_json(plan_path, plan)
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "low_volume",
                        "first_low_volume_at": (now - timedelta(minutes=4)).isoformat(),
                        "last_recovery_action": "relax_inventory_bias_for_volume",
                        "last_recovery_action_at": (now - timedelta(seconds=60)).isoformat(),
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=180,
                min_volume_notional=100,
                trigger_seconds=120,
                daily_target_notional=10_000.0,
                cycle_budget_floor_notional=120.0,
                volume_recovery_cycle_budget_increment=12.0,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))
            self.assertNotEqual(result["action"], "raise_cycle_budget_for_volume")
            self.assertEqual(control["best_quote_maker_volume_cycle_budget_notional"], 108.0)
            self.assertEqual(restarts, [], result)

    def test_keeps_loss_reduce_eligible_when_frozen_inventory_exists(self) -> None:
        now = datetime(2026, 7, 11, 12, 15, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "pause_buy_position_notional": 800.0,
                    "pause_short_position_notional": 800.0,
                },
                long_notional=810.0,
                short_notional=805.0,
                open_order_count=0,
                active_order_count=0,
            )
            plan_path = output_dir / "reusdt_loop_latest_plan.json"
            plan = json.loads(plan_path.read_text(encoding="utf-8"))
            plan["best_quote_maker_volume"] = {
                "reduce_freeze": {
                    "actual_long_notional": 500.0,
                    "actual_short_notional": 450.0,
                    "frozen_long_notional": 0.0,
                    "frozen_short_notional": 100.0,
                }
            }
            _write_json(plan_path, plan)

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state={},
                now=now,
                window_seconds=180,
                min_volume_notional=100,
                trigger_seconds=0,
                pause_baseline_long_notional=800.0,
                pause_baseline_short_notional=800.0,
                restart_runner=lambda _: None,
            )

            self.assertFalse(result["assessment"]["actual_inventory_below_soft"])
            self.assertTrue(result["assessment"]["effective_inventory_soft_pressure"])

    def test_does_not_enable_loss_reduce_when_hourly_target_pace_is_ahead(self) -> None:
        now = datetime(2026, 7, 11, 12, 15, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_inventory_soft_ratio": 0.8,
                    "pause_buy_position_notional": 800.0,
                    "pause_short_position_notional": 800.0,
                },
                long_notional=850.0,
                short_notional=700.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
            )
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "low_volume",
                        "first_low_volume_at": (now - timedelta(minutes=4)).isoformat(),
                    }
                }
            }
            rows = [
                {
                    "id": 1,
                    "time": int((now - timedelta(minutes=30)).timestamp() * 1000),
                    "quoteQty": "5000",
                }
            ]

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=180,
                min_volume_notional=100,
                trigger_seconds=120,
                daily_target_notional=6000,
                target_pace_fraction=1.05,
                trade_rows=rows,
                restart_runner=lambda symbol: self.fail(f"unexpected restart: {symbol}"),
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))
            self.assertEqual(result["action"], "hold_loss_reduce_while_target_pace_ahead")
            self.assertFalse(control["best_quote_maker_volume_allow_loss_reduce_only"])

    def test_closes_owned_loss_reduce_when_hourly_target_pace_is_ahead(self) -> None:
        now = datetime(2026, 7, 11, 12, 20, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_allow_loss_reduce_only": True,
                    "pause_buy_position_notional": 760.0,
                    "pause_short_position_notional": 760.0,
                },
                long_notional=850.0,
                short_notional=700.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
            )
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "recovery_active",
                        "recovery_owned": True,
                        "recovery_started_at": (now - timedelta(minutes=3)).isoformat(),
                        "guard_original_controls": {
                            "best_quote_maker_volume_allow_loss_reduce_only": False,
                            "pause_buy_position_notional": 800.0,
                            "pause_short_position_notional": 800.0,
                        },
                    }
                }
            }
            rows = [
                {
                    "id": 1,
                    "time": int((now - timedelta(minutes=30)).timestamp() * 1000),
                    "quoteQty": "5000",
                }
            ]
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=180,
                min_volume_notional=100,
                trigger_seconds=120,
                daily_target_notional=6000,
                target_pace_fraction=1.05,
                trade_rows=rows,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))
            self.assertEqual(result["action"], "restore_loss_reduce_when_target_pace_ahead")
            self.assertFalse(control["best_quote_maker_volume_allow_loss_reduce_only"])
            self.assertEqual(control["pause_buy_position_notional"], 800.0)
            self.assertEqual(restarts, ["REUSDT"])

    def test_reapplies_non_loss_recovery_control_after_external_override(self) -> None:
        now = datetime(2026, 6, 26, 8, 25, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={"best_quote_maker_volume_cycle_budget_notional": 48.0},
                long_notional=800.0,
                short_notional=700.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
            )
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "recovery_active",
                        "recovery_started_at": (now - timedelta(minutes=2)).isoformat(),
                        "guard_original_controls": {
                            "best_quote_maker_volume_cycle_budget_notional": 48.0,
                        },
                        "guard_recovery_controls": {
                            "best_quote_maker_volume_cycle_budget_notional": 60.0,
                        },
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=1,
                trigger_seconds=120,
                max_recovery_seconds=300,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))

            self.assertEqual(result["action"], "reapply_recovery_controls_after_drift")
            self.assertEqual(control["best_quote_maker_volume_cycle_budget_notional"], 60.0)
            self.assertEqual(restarts, ["REUSDT"])

    def test_times_out_non_loss_recovery_and_restores_original_control(self) -> None:
        now = datetime(2026, 6, 26, 8, 30, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={"best_quote_maker_volume_cycle_budget_notional": 60.0},
                long_notional=800.0,
                short_notional=700.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
            )
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "recovery_active",
                        "recovery_started_at": (now - timedelta(minutes=6)).isoformat(),
                        "guard_original_controls": {
                            "best_quote_maker_volume_cycle_budget_notional": 48.0,
                        },
                        "guard_recovery_controls": {
                            "best_quote_maker_volume_cycle_budget_notional": 60.0,
                        },
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=1,
                trigger_seconds=120,
                max_recovery_seconds=300,
                cooldown_seconds=600,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))
            item = state["symbols"]["REUSDT"]

            self.assertEqual(result["action"], "recovery_timeout_cooldown")
            self.assertEqual(control["best_quote_maker_volume_cycle_budget_notional"], 48.0)
            self.assertEqual(item["status"], "cooldown")
            self.assertNotIn("guard_recovery_controls", item)
            self.assertEqual(restarts, ["REUSDT"])

    def test_recovered_volume_clears_recovery_instead_of_reapplying_drifted_control(self) -> None:
        now = datetime(2026, 6, 26, 8, 35, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={"best_quote_maker_volume_cycle_budget_notional": 48.0},
                long_notional=800.0,
                short_notional=700.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
                recent_trade_notional=80.0,
            )
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "recovery_active",
                        "recovery_started_at": (now - timedelta(minutes=2)).isoformat(),
                        "guard_original_controls": {
                            "best_quote_maker_volume_cycle_budget_notional": 48.0,
                        },
                        "guard_recovery_controls": {
                            "best_quote_maker_volume_cycle_budget_notional": 60.0,
                        },
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=1,
                trigger_seconds=120,
                recover_min_volume_notional=10,
                max_recovery_seconds=300,
                restart_runner=restarts.append,
            )

            item = state["symbols"]["REUSDT"]

            self.assertEqual(result["action"], "restore_recovery_controls")
            self.assertEqual(item["status"], "normal")
            self.assertNotIn("guard_recovery_controls", item)
            self.assertEqual(restarts, [])

    def test_stale_inputs_do_not_bypass_non_loss_recovery_timeout(self) -> None:
        now = datetime(2026, 6, 26, 8, 40, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now - timedelta(minutes=10),
                control={"best_quote_maker_volume_cycle_budget_notional": 60.0},
                long_notional=800.0,
                short_notional=700.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
            )
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "recovery_active",
                        "recovery_started_at": (now - timedelta(minutes=6)).isoformat(),
                        "guard_original_controls": {
                            "best_quote_maker_volume_cycle_budget_notional": 48.0,
                        },
                        "guard_recovery_controls": {
                            "best_quote_maker_volume_cycle_budget_notional": 60.0,
                        },
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=1,
                trigger_seconds=120,
                max_recovery_seconds=300,
                cooldown_seconds=600,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))

            self.assertEqual(result["action"], "recovery_timeout_cooldown")
            self.assertEqual(control["best_quote_maker_volume_cycle_budget_notional"], 48.0)
            self.assertEqual(state["symbols"]["REUSDT"]["status"], "cooldown")
            self.assertEqual(restarts, ["REUSDT"])

    def test_restart_failure_still_records_non_loss_recovery_start_time(self) -> None:
        now = datetime(2026, 6, 26, 8, 45, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={"best_quote_maker_volume_cycle_budget_notional": 48.0},
                long_notional=800.0,
                short_notional=700.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
            )
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "low_volume",
                        "first_low_volume_at": (now - timedelta(minutes=4)).isoformat(),
                    }
                }
            }

            def fail_restart(symbol: str) -> None:
                raise subprocess.CalledProcessError(1, ["restart", symbol])

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=1,
                trigger_seconds=120,
                volume_recovery_cycle_budget_increment=12,
                restart_runner=fail_restart,
            )

            item = state["symbols"]["REUSDT"]

            self.assertEqual(result["action"], "restart_failed")
            self.assertEqual(item["status"], "recovery_active")
            self.assertEqual(item["recovery_started_at"], now.isoformat())
            self.assertEqual(
                item["guard_recovery_controls"]["best_quote_maker_volume_cycle_budget_notional"],
                60.0,
            )

    def test_cost_gate_recovery_records_expected_disabled_target(self) -> None:
        now = datetime(2026, 6, 26, 8, 50, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={"best_quote_maker_volume_inventory_cost_gate_enabled": True},
            )
            plan_path = output_dir / "reusdt_loop_latest_plan.json"
            plan = json.loads(plan_path.read_text(encoding="utf-8"))
            plan["best_quote_maker_volume"] = {
                "inventory_cost_gate": {"blocked_buy_orders": 1, "blocked_sell_orders": 0}
            }
            _write_json(plan_path, plan)
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "low_volume",
                        "first_low_volume_at": (now - timedelta(minutes=4)).isoformat(),
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=1,
                trigger_seconds=120,
                restart_runner=restarts.append,
            )

            item = state["symbols"]["REUSDT"]

            self.assertEqual(result["action"], "disable_inventory_cost_gate")
            self.assertEqual(
                item["guard_recovery_controls"]["best_quote_maker_volume_inventory_cost_gate_enabled"],
                False,
            )
            self.assertEqual(item["recovery_started_at"], now.isoformat())
            self.assertEqual(restarts, ["REUSDT"])

    def test_bias_restart_failure_still_records_recovery_start_and_target(self) -> None:
        now = datetime(2026, 6, 26, 8, 55, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={"best_quote_maker_volume_inventory_bias_min_notional_gap": 80.0},
                long_notional=990.0,
                short_notional=850.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
            )
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "low_volume",
                        "first_low_volume_at": (now - timedelta(minutes=4)).isoformat(),
                    }
                }
            }

            def fail_restart(symbol: str) -> None:
                raise subprocess.CalledProcessError(1, ["restart", symbol])

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=1,
                trigger_seconds=120,
                inventory_bias_relief_notional_margin=24,
                restart_runner=fail_restart,
            )

            item = state["symbols"]["REUSDT"]

            self.assertEqual(result["action"], "restart_failed")
            self.assertEqual(item["status"], "recovery_active")
            self.assertEqual(item["recovery_started_at"], now.isoformat())
            self.assertEqual(
                item["guard_recovery_controls"]["best_quote_maker_volume_inventory_bias_min_notional_gap"],
                164.0,
            )

    def test_loss_reduce_restart_failure_still_records_recovery_start(self) -> None:
        now = datetime(2026, 6, 26, 9, 0, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={"best_quote_maker_volume_inventory_bias_min_notional_gap": 200.0},
                long_notional=990.0,
                short_notional=850.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
            )
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "low_volume",
                        "first_low_volume_at": (now - timedelta(minutes=4)).isoformat(),
                    }
                }
            }

            def fail_restart(symbol: str) -> None:
                raise subprocess.CalledProcessError(1, ["restart", symbol])

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=1,
                trigger_seconds=120,
                inventory_bias_relief_notional_margin=24,
                restart_runner=fail_restart,
            )

            item = state["symbols"]["REUSDT"]

            self.assertEqual(result["action"], "restart_failed")
            self.assertEqual(item["status"], "recovery_active")
            self.assertEqual(item["recovery_started_at"], now.isoformat())

    def test_recovery_holds_before_restoring_controls(self) -> None:
        now = datetime(2026, 6, 26, 9, 10, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={"best_quote_maker_volume_allow_loss_reduce_only": True},
                long_notional=800.0,
                short_notional=700.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
                recent_trade_notional=80.0,
            )
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "recovery_active",
                        "recovery_started_at": (now - timedelta(seconds=60)).isoformat(),
                        "guard_original_controls": {
                            "best_quote_maker_volume_allow_loss_reduce_only": False,
                        },
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=1,
                trigger_seconds=120,
                recover_min_volume_notional=10,
                recovery_min_hold_seconds=120,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))
            self.assertEqual(result["action"], "hold_recovery_min_duration")
            self.assertTrue(control["best_quote_maker_volume_allow_loss_reduce_only"])
            self.assertEqual(restarts, [])

    def test_recovery_control_drift_is_debounced(self) -> None:
        now = datetime(2026, 6, 26, 9, 20, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={"best_quote_maker_volume_cycle_budget_notional": 48.0},
                long_notional=800.0,
                short_notional=700.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
            )
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "recovery_active",
                        "recovery_started_at": (now - timedelta(seconds=60)).isoformat(),
                        "last_recovery_action_at": (now - timedelta(seconds=60)).isoformat(),
                        "guard_original_controls": {
                            "best_quote_maker_volume_cycle_budget_notional": 48.0,
                        },
                        "guard_recovery_controls": {
                            "best_quote_maker_volume_cycle_budget_notional": 60.0,
                        },
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=1,
                trigger_seconds=120,
                recovery_reapply_min_seconds=180,
                restart_runner=restarts.append,
            )

            self.assertEqual(result["action"], "hold_recovery_control_drift_debounce")
            self.assertEqual(restarts, [])

    def test_soft_inventory_pressure_uses_loss_reduce_before_budget_increase(self) -> None:
        now = datetime(2026, 6, 26, 9, 30, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_cycle_budget_notional": 48.0,
                    "best_quote_maker_volume_inventory_soft_ratio": 0.8,
                    "pause_buy_position_notional": 800.0,
                    "pause_short_position_notional": 800.0,
                },
                long_notional=850.0,
                short_notional=700.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
            )
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "low_volume",
                        "first_low_volume_at": (now - timedelta(minutes=4)).isoformat(),
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=1,
                trigger_seconds=120,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))
            self.assertEqual(result["action"], "enable_soft_inventory_loss_reduce")
            self.assertTrue(result["assessment"]["long_near_soft"])
            self.assertTrue(control["best_quote_maker_volume_allow_loss_reduce_only"])
            self.assertEqual(control["best_quote_maker_volume_cycle_budget_notional"], 48.0)
            self.assertEqual(restarts, ["REUSDT"])

    def test_budget_recovery_switches_to_soft_inventory_loss_reduce(self) -> None:
        now = datetime(2026, 6, 26, 9, 40, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_cycle_budget_notional": 60.0,
                    "best_quote_maker_volume_inventory_soft_ratio": 0.8,
                    "pause_buy_position_notional": 800.0,
                    "pause_short_position_notional": 800.0,
                },
                long_notional=850.0,
                short_notional=700.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
            )
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "recovery_active",
                        "recovery_started_at": (now - timedelta(minutes=3)).isoformat(),
                        "last_recovery_action_at": (now - timedelta(minutes=3)).isoformat(),
                        "guard_original_controls": {
                            "best_quote_maker_volume_cycle_budget_notional": 48.0,
                        },
                        "guard_recovery_controls": {
                            "best_quote_maker_volume_cycle_budget_notional": 60.0,
                        },
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=1,
                trigger_seconds=120,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))
            self.assertEqual(result["action"], "switch_to_soft_inventory_loss_reduce")
            self.assertEqual(control["best_quote_maker_volume_cycle_budget_notional"], 48.0)
            self.assertTrue(control["best_quote_maker_volume_allow_loss_reduce_only"])
            self.assertEqual(state["symbols"]["REUSDT"]["recovery_started_at"], now.isoformat())
            self.assertEqual(restarts, ["REUSDT"])

    def test_recovered_volume_does_not_close_loss_reduce_above_soft_limit(self) -> None:
        now = datetime(2026, 6, 26, 9, 50, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_allow_loss_reduce_only": True,
                    "best_quote_maker_volume_inventory_soft_ratio": 0.8,
                    "pause_buy_position_notional": 800.0,
                    "pause_short_position_notional": 800.0,
                },
                long_notional=850.0,
                short_notional=700.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
                recent_trade_notional=80.0,
            )
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "recovery_active",
                        "recovery_started_at": (now - timedelta(minutes=3)).isoformat(),
                        "guard_original_controls": {
                            "best_quote_maker_volume_allow_loss_reduce_only": False,
                        },
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=1,
                trigger_seconds=120,
                recover_min_volume_notional=10,
                recovery_min_hold_seconds=120,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))
            self.assertEqual(result["action"], "hold_recovery_until_cap_buffer")
            self.assertTrue(control["best_quote_maker_volume_allow_loss_reduce_only"])
            self.assertEqual(restarts, [])

    def test_soft_inventory_recovery_gets_one_bounded_extension(self) -> None:
        now = datetime(2026, 6, 26, 10, 0, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_allow_loss_reduce_only": True,
                    "best_quote_maker_volume_inventory_soft_ratio": 0.8,
                    "pause_buy_position_notional": 800.0,
                    "pause_short_position_notional": 800.0,
                },
                long_notional=805.0,
                short_notional=700.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
                recent_trade_notional=20.0,
            )
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "recovery_active",
                        "recovery_started_at": (now - timedelta(minutes=6)).isoformat(),
                        "soft_recovery_extension_count": 0,
                        "guard_original_controls": {
                            "best_quote_maker_volume_allow_loss_reduce_only": False,
                        },
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=100,
                trigger_seconds=120,
                max_recovery_seconds=300,
                max_soft_recovery_extensions=1,
                restart_runner=restarts.append,
            )

            item = state["symbols"]["REUSDT"]
            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))
            self.assertEqual(result["action"], "extend_soft_inventory_loss_recovery")
            self.assertEqual(item["soft_recovery_extension_count"], 1)
            self.assertEqual(item["recovery_started_at"], now.isoformat())
            self.assertTrue(control["best_quote_maker_volume_allow_loss_reduce_only"])
            self.assertEqual(restarts, [])

    def test_exhausted_soft_recovery_holds_until_both_sides_are_below_soft(self) -> None:
        now = datetime(2026, 6, 26, 10, 5, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_allow_loss_reduce_only": True,
                    "best_quote_maker_volume_inventory_soft_ratio": 0.8,
                    "pause_buy_position_notional": 800.0,
                    "pause_short_position_notional": 800.0,
                },
                long_notional=805.0,
                short_notional=700.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
                recent_trade_notional=20.0,
            )
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "recovery_active",
                        "recovery_started_at": (now - timedelta(minutes=6)).isoformat(),
                        "soft_recovery_extension_count": 1,
                        "guard_original_controls": {
                            "best_quote_maker_volume_allow_loss_reduce_only": False,
                        },
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=100,
                trigger_seconds=120,
                max_recovery_seconds=300,
                max_soft_recovery_extensions=1,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))
            self.assertEqual(
                result["action"],
                "hold_soft_inventory_loss_recovery_until_both_below",
            )
            self.assertEqual(state["symbols"]["REUSDT"]["status"], "recovery_active")
            self.assertTrue(control["best_quote_maker_volume_allow_loss_reduce_only"])
            self.assertEqual(restarts, [])

    def test_stalled_loss_recovery_enables_bounded_pair_reduce_when_no_reduce_orders_exist(self) -> None:
        now = datetime(2026, 6, 26, 10, 7, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_allow_loss_reduce_only": True,
                    "best_quote_maker_volume_active_pair_reduce_enabled": False,
                    "best_quote_maker_volume_active_pair_reduce_min_side_notional": 100.0,
                    "best_quote_maker_volume_inventory_soft_ratio": 0.8,
                    "pause_buy_position_notional": 800.0,
                    "pause_short_position_notional": 800.0,
                },
                long_notional=805.0,
                short_notional=700.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
                recent_trade_notional=20.0,
            )
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "recovery_active",
                        "recovery_started_at": (now - timedelta(minutes=6)).isoformat(),
                        "last_recovery_action_at": (now - timedelta(minutes=4)).isoformat(),
                        "soft_recovery_extension_count": 1,
                        "guard_original_controls": {
                            "best_quote_maker_volume_allow_loss_reduce_only": False,
                        },
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=100,
                trigger_seconds=120,
                max_recovery_seconds=300,
                max_soft_recovery_extensions=1,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))
            item = state["symbols"]["REUSDT"]
            self.assertEqual(
                result["action"],
                "enable_bounded_pair_reduce_for_stalled_loss_recovery",
            )
            self.assertTrue(control["best_quote_maker_volume_active_pair_reduce_enabled"])
            self.assertFalse(
                item["guard_original_controls"]["best_quote_maker_volume_active_pair_reduce_enabled"]
            )
            self.assertEqual(restarts, ["REUSDT"])

    def test_stalled_loss_recovery_does_not_enable_pair_reduce_for_imbalanced_inventory(self) -> None:
        now = datetime(2026, 6, 26, 10, 8, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_allow_loss_reduce_only": True,
                    "best_quote_maker_volume_active_pair_reduce_enabled": False,
                    "best_quote_maker_volume_active_pair_reduce_min_side_notional": 100.0,
                    "best_quote_maker_volume_inventory_soft_ratio": 0.8,
                    "pause_buy_position_notional": 800.0,
                    "pause_short_position_notional": 800.0,
                },
                long_notional=805.0,
                short_notional=300.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
                recent_trade_notional=20.0,
            )
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "recovery_active",
                        "recovery_started_at": (now - timedelta(minutes=6)).isoformat(),
                        "last_recovery_action_at": (now - timedelta(minutes=4)).isoformat(),
                        "soft_recovery_extension_count": 1,
                        "guard_original_controls": {
                            "best_quote_maker_volume_allow_loss_reduce_only": False,
                        },
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=100,
                trigger_seconds=120,
                max_recovery_seconds=300,
                max_soft_recovery_extensions=1,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))
            self.assertEqual(result["action"], "hold_soft_inventory_loss_recovery_until_both_below")
            self.assertFalse(control["best_quote_maker_volume_active_pair_reduce_enabled"])
            self.assertEqual(restarts, [])

    def test_timed_out_loss_recovery_restores_after_both_sides_clear_original_soft_limits(self) -> None:
        now = datetime(2026, 6, 26, 10, 9, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_allow_loss_reduce_only": True,
                    "best_quote_maker_volume_active_pair_reduce_enabled": True,
                    "best_quote_maker_volume_inventory_soft_ratio": 0.8,
                    "pause_buy_position_notional": 600.0,
                    "pause_short_position_notional": 600.0,
                },
                long_notional=750.0,
                short_notional=700.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
                recent_trade_notional=20.0,
            )
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "recovery_active",
                        "recovery_started_at": (now - timedelta(minutes=6)).isoformat(),
                        "last_recovery_action_at": (now - timedelta(minutes=4)).isoformat(),
                        "soft_recovery_extension_count": 1,
                        "guard_original_controls": {
                            "best_quote_maker_volume_allow_loss_reduce_only": False,
                            "best_quote_maker_volume_active_pair_reduce_enabled": False,
                            "pause_buy_position_notional": 800.0,
                            "pause_short_position_notional": 800.0,
                        },
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=100,
                trigger_seconds=120,
                max_recovery_seconds=300,
                max_soft_recovery_extensions=1,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))
            self.assertEqual(result["action"], "recovery_timeout_cooldown")
            self.assertFalse(control["best_quote_maker_volume_allow_loss_reduce_only"])
            self.assertFalse(control["best_quote_maker_volume_active_pair_reduce_enabled"])
            self.assertEqual(control["pause_buy_position_notional"], 800.0)
            self.assertEqual(control["pause_short_position_notional"], 800.0)
            self.assertEqual(restarts, ["REUSDT"])

    def test_one_sided_inventory_bias_stall_tightens_entry_sticky(self) -> None:
        now = datetime(2026, 6, 26, 10, 10, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_cycle_budget_notional": 72.0,
                    "sticky_entry_price_tolerance_steps": 8.0,
                },
                long_notional=650.0,
                short_notional=350.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
                recent_trade_notional=40.0,
            )
            plan_path = output_dir / "reusdt_loop_latest_plan.json"
            plan = json.loads(plan_path.read_text(encoding="utf-8"))
            plan.update(
                {
                    "buy_paused": True,
                    "short_paused": False,
                    "pause_reasons": ["inventory_bias"],
                    "short_pause_reasons": ["inventory_bias"],
                }
            )
            _write_json(plan_path, plan)
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "low_volume",
                        "first_low_volume_at": (now - timedelta(minutes=4)).isoformat(),
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=100,
                trigger_seconds=120,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))
            self.assertEqual(result["action"], "tighten_sticky_for_one_sided_stall")
            self.assertTrue(result["assessment"]["one_sided_inventory_bias"])
            self.assertEqual(control["sticky_entry_price_tolerance_steps"], 1.0)
            self.assertEqual(control["best_quote_maker_volume_cycle_budget_notional"], 72.0)
            self.assertEqual(restarts, ["REUSDT"])

    def test_severe_one_sided_stall_bypasses_general_cooldown(self) -> None:
        now = datetime(2026, 6, 26, 10, 10, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_cycle_budget_notional": 72.0,
                    "sticky_entry_price_tolerance_steps": 8.0,
                },
                long_notional=650.0,
                short_notional=350.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
                recent_trade_notional=40.0,
            )
            plan_path = output_dir / "reusdt_loop_latest_plan.json"
            plan = json.loads(plan_path.read_text(encoding="utf-8"))
            plan.update(
                {
                    "buy_paused": True,
                    "short_paused": False,
                    "pause_reasons": ["inventory_bias"],
                    "short_pause_reasons": ["inventory_bias"],
                }
            )
            _write_json(plan_path, plan)
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "cooldown",
                        "cooldown_until": (now + timedelta(minutes=5)).isoformat(),
                        "first_low_volume_at": (now - timedelta(minutes=4)).isoformat(),
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=100,
                trigger_seconds=120,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))
            self.assertEqual(result["action"], "tighten_sticky_for_one_sided_stall")
            self.assertEqual(control["sticky_entry_price_tolerance_steps"], 1.0)
            self.assertEqual(restarts, ["REUSDT"])

    def test_soft_inventory_low_volume_bypasses_general_cooldown_after_confirmation(self) -> None:
        now = datetime(2026, 6, 26, 10, 10, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_cycle_budget_notional": 72.0,
                    "pause_buy_position_notional": 700.0,
                    "pause_short_position_notional": 700.0,
                },
                long_notional=750.0,
                short_notional=350.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
                recent_trade_notional=40.0,
            )
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "cooldown",
                        "cooldown_until": (now + timedelta(minutes=5)).isoformat(),
                        "first_low_volume_at": (now - timedelta(minutes=4)).isoformat(),
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=100,
                trigger_seconds=120,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))
            self.assertEqual(result["action"], "enable_soft_inventory_loss_reduce")
            self.assertTrue(control["best_quote_maker_volume_allow_loss_reduce_only"])
            self.assertFalse(control["best_quote_maker_volume_net_loss_reduce_enabled"])
            self.assertEqual(restarts, ["REUSDT"])

    def test_multiple_same_direction_entries_are_still_one_sided_inventory_bias(self) -> None:
        now = datetime(2026, 6, 26, 10, 15, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_cycle_budget_notional": 72.0,
                    "sticky_entry_price_tolerance_steps": 8.0,
                },
                long_notional=340.0,
                short_notional=180.0,
                open_order_count=2,
                active_order_count=2,
                orders_near_market=True,
                recent_trade_notional=0.0,
            )
            plan_path = output_dir / "reusdt_loop_latest_plan.json"
            plan = json.loads(plan_path.read_text(encoding="utf-8"))
            plan.update(
                {
                    "buy_orders": [],
                    "sell_orders": [
                        {"side": "SELL", "price": 0.5972, "qty": 16.0, "role": "best_quote_entry_short"},
                        {"side": "SELL", "price": 0.5973, "qty": 16.0, "role": "best_quote_entry_short"},
                    ],
                    "buy_paused": True,
                    "short_paused": False,
                    "pause_reasons": ["inventory_bias"],
                    "short_pause_reasons": ["inventory_bias"],
                }
            )
            _write_json(plan_path, plan)
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "low_volume",
                        "first_low_volume_at": (now - timedelta(minutes=4)).isoformat(),
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=100,
                trigger_seconds=120,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))
            self.assertEqual(result["action"], "tighten_sticky_for_one_sided_stall")
            self.assertTrue(result["assessment"]["one_sided_inventory_bias"])
            self.assertEqual(control["sticky_entry_price_tolerance_steps"], 1.0)
            self.assertEqual(control["best_quote_maker_volume_cycle_budget_notional"], 72.0)
            self.assertEqual(restarts, ["REUSDT"])

    def test_inventory_bias_stall_with_tight_sticky_relaxes_bias_instead_of_budget(self) -> None:
        now = datetime(2026, 6, 26, 10, 16, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_cycle_budget_notional": 72.0,
                    "best_quote_maker_volume_inventory_bias_min_notional_gap": 100.0,
                    "sticky_entry_price_tolerance_steps": 1.0,
                },
                long_notional=340.0,
                short_notional=180.0,
                open_order_count=2,
                active_order_count=2,
                orders_near_market=True,
            )
            plan_path = output_dir / "reusdt_loop_latest_plan.json"
            plan = json.loads(plan_path.read_text(encoding="utf-8"))
            plan.update(
                {
                    "buy_orders": [],
                    "sell_orders": [
                        {"side": "SELL", "price": 0.5972, "qty": 16.0, "role": "best_quote_entry_short"},
                        {"side": "SELL", "price": 0.5973, "qty": 16.0, "role": "best_quote_entry_short"},
                    ],
                    "buy_paused": True,
                    "short_paused": False,
                    "pause_reasons": ["inventory_bias"],
                    "short_pause_reasons": ["inventory_bias"],
                }
            )
            _write_json(plan_path, plan)
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "low_volume",
                        "first_low_volume_at": (now - timedelta(minutes=4)).isoformat(),
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=100,
                trigger_seconds=120,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))
            self.assertEqual(result["action"], "relax_inventory_bias_for_volume")
            self.assertEqual(control["best_quote_maker_volume_cycle_budget_notional"], 72.0)
            self.assertEqual(control["best_quote_maker_volume_inventory_bias_min_notional_gap"], 184.0)
            self.assertEqual(restarts, ["REUSDT"])

    def test_active_budget_recovery_switches_to_one_sided_sticky_requote(self) -> None:
        now = datetime(2026, 6, 26, 10, 20, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_cycle_budget_notional": 84.0,
                    "sticky_entry_price_tolerance_steps": 8.0,
                },
                long_notional=650.0,
                short_notional=350.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
                recent_trade_notional=40.0,
            )
            plan_path = output_dir / "reusdt_loop_latest_plan.json"
            plan = json.loads(plan_path.read_text(encoding="utf-8"))
            plan.update(
                {
                    "buy_paused": True,
                    "short_paused": False,
                    "pause_reasons": ["inventory_bias"],
                }
            )
            _write_json(plan_path, plan)
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "recovery_active",
                        "recovery_started_at": (now - timedelta(minutes=3)).isoformat(),
                        "guard_original_controls": {
                            "best_quote_maker_volume_cycle_budget_notional": 72.0,
                        },
                        "guard_recovery_controls": {
                            "best_quote_maker_volume_cycle_budget_notional": 84.0,
                        },
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=100,
                trigger_seconds=120,
                recovery_min_hold_seconds=120,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))
            self.assertEqual(result["action"], "switch_to_one_sided_sticky_requote")
            self.assertEqual(control["best_quote_maker_volume_cycle_budget_notional"], 72.0)
            self.assertEqual(control["sticky_entry_price_tolerance_steps"], 1.0)
            self.assertEqual(restarts, ["REUSDT"])

    def test_active_sticky_recovery_escalates_to_inventory_bias_relief(self) -> None:
        now = datetime(2026, 6, 26, 10, 25, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_cycle_budget_notional": 72.0,
                    "best_quote_maker_volume_inventory_bias_min_notional_gap": 150.0,
                    "sticky_entry_price_tolerance_steps": 1.0,
                },
                long_notional=340.0,
                short_notional=179.0,
                open_order_count=2,
                active_order_count=2,
                orders_near_market=True,
            )
            plan_path = output_dir / "reusdt_loop_latest_plan.json"
            plan = json.loads(plan_path.read_text(encoding="utf-8"))
            plan.update(
                {
                    "buy_orders": [],
                    "sell_orders": [
                        {"side": "SELL", "price": 0.5972, "qty": 16.0, "role": "best_quote_entry_short"},
                        {"side": "SELL", "price": 0.5973, "qty": 16.0, "role": "best_quote_entry_short"},
                    ],
                    "buy_paused": True,
                    "short_paused": False,
                    "pause_reasons": ["inventory_bias"],
                    "short_pause_reasons": ["inventory_bias"],
                }
            )
            _write_json(plan_path, plan)
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "recovery_active",
                        "recovery_started_at": (now - timedelta(minutes=3)).isoformat(),
                        "last_recovery_action_at": (now - timedelta(minutes=3)).isoformat(),
                        "guard_original_controls": {
                            "sticky_entry_price_tolerance_steps": 8.0,
                        },
                        "guard_recovery_controls": {
                            "sticky_entry_price_tolerance_steps": 1.0,
                        },
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=100,
                trigger_seconds=120,
                recovery_min_hold_seconds=120,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))
            item = state["symbols"]["REUSDT"]
            self.assertEqual(result["action"], "relax_inventory_bias_after_sticky_stall")
            self.assertEqual(control["sticky_entry_price_tolerance_steps"], 1.0)
            self.assertEqual(control["best_quote_maker_volume_cycle_budget_notional"], 72.0)
            self.assertEqual(control["best_quote_maker_volume_inventory_bias_min_notional_gap"], 185.0)
            self.assertEqual(
                item["guard_original_controls"]["best_quote_maker_volume_inventory_bias_min_notional_gap"],
                150.0,
            )
            self.assertEqual(restarts, ["REUSDT"])

    def test_resolved_inventory_relief_switches_to_cycle_budget_recovery(self) -> None:
        now = datetime(2026, 6, 26, 10, 28, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={
                    "best_quote_maker_volume_cycle_budget_notional": 72.0,
                    "best_quote_maker_volume_inventory_bias_min_notional_gap": 185.0,
                    "sticky_entry_price_tolerance_steps": 1.0,
                },
                long_notional=400.0,
                short_notional=380.0,
                open_order_count=2,
                active_order_count=2,
                orders_near_market=True,
                recent_trade_notional=40.0,
            )
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "recovery_active",
                        "recovery_started_at": (now - timedelta(minutes=3)).isoformat(),
                        "last_recovery_action_at": (now - timedelta(minutes=3)).isoformat(),
                        "guard_original_controls": {
                            "sticky_entry_price_tolerance_steps": 8.0,
                            "best_quote_maker_volume_inventory_bias_min_notional_gap": 150.0,
                        },
                        "guard_recovery_controls": {
                            "sticky_entry_price_tolerance_steps": 1.0,
                            "best_quote_maker_volume_inventory_bias_min_notional_gap": 185.0,
                        },
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=100,
                trigger_seconds=120,
                recovery_min_hold_seconds=120,
                volume_recovery_cycle_budget_increment=12,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))
            self.assertEqual(result["action"], "switch_to_cycle_budget_after_inventory_relief")
            self.assertEqual(control["sticky_entry_price_tolerance_steps"], 8.0)
            self.assertEqual(control["best_quote_maker_volume_inventory_bias_min_notional_gap"], 150.0)
            self.assertEqual(control["best_quote_maker_volume_cycle_budget_notional"], 84.0)
            self.assertEqual(restarts, ["REUSDT"])

    def test_near_market_reduce_only_flow_does_not_raise_cycle_budget(self) -> None:
        now = datetime(2026, 6, 26, 10, 30, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            self._write_common_files(
                output_dir,
                now=now,
                control={"best_quote_maker_volume_cycle_budget_notional": 72.0},
                long_notional=650.0,
                short_notional=620.0,
                open_order_count=1,
                active_order_count=1,
                orders_near_market=True,
            )
            plan_path = output_dir / "reusdt_loop_latest_plan.json"
            plan = json.loads(plan_path.read_text(encoding="utf-8"))
            plan["buy_orders"] = []
            plan["sell_orders"] = [
                {
                    "side": "SELL",
                    "price": 0.5972,
                    "qty": 16.0,
                    "role": "best_quote_active_pair_reduce_long",
                    "force_reduce_only": True,
                }
            ]
            _write_json(plan_path, plan)
            state: dict[str, object] = {
                "symbols": {
                    "REUSDT": {
                        "status": "low_volume",
                        "first_low_volume_at": (now - timedelta(minutes=4)).isoformat(),
                    }
                }
            }
            restarts: list[str] = []

            result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=100,
                trigger_seconds=120,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))
            self.assertEqual(result["action"], "hold_near_market_reduce_only_flow")
            self.assertTrue(result["assessment"]["planned_reduce_only_only"])
            self.assertEqual(control["best_quote_maker_volume_cycle_budget_notional"], 72.0)
            self.assertEqual(restarts, [])

            floor_result = check_symbol(
                symbol="REUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=60,
                min_volume_notional=100,
                trigger_seconds=120,
                cycle_budget_floor_notional=108,
                restart_runner=restarts.append,
            )

            control = json.loads((output_dir / "reusdt_loop_runner_control.json").read_text(encoding="utf-8"))
            self.assertEqual(floor_result["action"], "raise_cycle_budget_for_volume")
            self.assertEqual(control["best_quote_maker_volume_cycle_budget_notional"], 108.0)
            self.assertEqual(restarts, ["REUSDT"])


if __name__ == "__main__":
    unittest.main()
