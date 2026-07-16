from __future__ import annotations

import os
import json
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch

from grid_optimizer.bq_liveness_terminal_guard import (
    clear_stale_drift_flag_if_reconciled,
    invalid_state_reason,
    main,
    target_done_marker,
)


class BqLivenessTerminalGuardTests(TestCase):
    def test_target_done_marker_uses_utc_trading_day(self) -> None:
        marker = target_done_marker(
            workdir=Path("/tmp/wangge"),
            symbol="OUSDT",
            now=datetime(2026, 7, 11, 23, 59, tzinfo=timezone.utc),
        )
        self.assertEqual(marker.name, "ousdt_target_gate_done_20260711.flag")

    def test_terminal_marker_skips_legacy_watchdog(self) -> None:
        with TemporaryDirectory() as tmpdir:
            workdir = Path(tmpdir)
            marker = workdir / "output" / "ousdt_target_gate_done_20260711.flag"
            marker.parent.mkdir(parents=True)
            marker.touch()
            with (
                patch("grid_optimizer.bq_liveness_terminal_guard.datetime") as mocked_datetime,
                patch("grid_optimizer.bq_liveness_terminal_guard.runpy.run_path") as run_path,
            ):
                mocked_datetime.now.return_value = datetime(2026, 7, 11, 12, tzinfo=timezone.utc)
                self.assertEqual(main(["--symbol", "OUSDT", "--workdir", str(workdir), "--enforce"]), 0)
            run_path.assert_not_called()

    def test_invalid_state_blocks_legacy_watchdog(self) -> None:
        with TemporaryDirectory() as tmpdir:
            workdir = Path(tmpdir)
            state_path = workdir / "output" / "ousdt_loop_state.json"
            state_path.parent.mkdir(parents=True)
            state_path.write_text('{"best_quote_volume_ledger": [', encoding="utf-8")
            with patch("grid_optimizer.bq_liveness_terminal_guard.runpy.run_path") as run_path:
                self.assertEqual(main(["--symbol", "OUSDT", "--workdir", str(workdir), "--enforce"]), 0)
            run_path.assert_not_called()
            reason = invalid_state_reason(workdir=workdir, symbol="OUSDT")
            self.assertIsNotNone(reason)
            self.assertEqual(reason["state_path"], str(state_path))

    def test_valid_state_allows_legacy_watchdog_for_unmanaged_symbol(self) -> None:
        with TemporaryDirectory() as tmpdir:
            workdir = Path(tmpdir)
            output_dir = workdir / "output"
            legacy = output_dir / "ops" / "bq_liveness_watchdog.py"
            legacy.parent.mkdir(parents=True)
            legacy.write_text("", encoding="utf-8")
            (output_dir / "reusdt_loop_state.json").write_text("{}", encoding="utf-8")
            with patch("grid_optimizer.bq_liveness_terminal_guard.runpy.run_path") as run_path:
                self.assertEqual(main(["--symbol", "REUSDT", "--workdir", str(workdir), "--enforce"]), 0)
            run_path.assert_called_once()

    def test_managed_symbol_never_executes_untracked_legacy_watchdog(self) -> None:
        with TemporaryDirectory() as tmpdir:
            workdir = Path(tmpdir)
            output_dir = workdir / "output"
            legacy = output_dir / "ops" / "bq_liveness_watchdog.py"
            legacy.parent.mkdir(parents=True)
            legacy.write_text("raise AssertionError('must not run')", encoding="utf-8")
            (output_dir / "arxusdt_loop_state.json").write_text("{}", encoding="utf-8")
            with patch("grid_optimizer.bq_liveness_terminal_guard.runpy.run_path") as run_path:
                self.assertEqual(main(["--symbol", "ARXUSDT", "--workdir", str(workdir), "--enforce"]), 0)
            run_path.assert_not_called()

    def test_registered_bch_presence_never_executes_untracked_legacy_watchdog(self) -> None:
        with TemporaryDirectory() as tmpdir:
            workdir = Path(tmpdir)
            output_dir = workdir / "output"
            legacy = output_dir / "ops" / "bq_liveness_watchdog.py"
            legacy.parent.mkdir(parents=True)
            legacy.write_text("raise AssertionError('must not run')", encoding="utf-8")
            (output_dir / "bchusdt_loop_state.json").write_text(
                "{}", encoding="utf-8"
            )
            (output_dir / "bchusdt_loop_runner_control.json").write_text(
                json.dumps({"_futures_recovery_state": None}),
                encoding="utf-8",
            )
            with patch(
                "grid_optimizer.bq_liveness_terminal_guard.runpy.run_path"
            ) as run_path:
                self.assertEqual(
                    main(
                        [
                            "--symbol",
                            "BCHUSDT",
                            "--workdir",
                            str(workdir),
                            "--enforce",
                        ]
                    ),
                    0,
                )
            run_path.assert_not_called()

    def test_clears_stale_drift_flag_after_fresh_safe_reconcile(self) -> None:
        now = datetime(2026, 7, 12, 19, 30, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            workdir = Path(tmpdir)
            output_dir = workdir / "output"
            output_dir.mkdir(parents=True)
            flag_path = output_dir / "OUSDT_DRIFT_ALERT.flag"
            flag_path.write_text("historical drift\n", encoding="utf-8")
            flag_time = now.timestamp() - 600
            flag_path.touch()
            os.utime(flag_path, (flag_time, flag_time))
            (output_dir / "ousdt_loop_state.json").write_text(
                '{"updated_at":"2026-07-12T19:29:30+00:00",'
                '"last_reconcile":{"open_order_diff":1,"actual_net_qty_diff":0,'
                '"protective_stop_required":false}}',
                encoding="utf-8",
            )

            cleared = clear_stale_drift_flag_if_reconciled(
                workdir=workdir,
                symbol="OUSDT",
                now=now,
            )

            self.assertEqual(cleared, flag_path)
            self.assertFalse(flag_path.exists())

    def test_keeps_stale_drift_flag_when_current_reconcile_is_unsafe(self) -> None:
        now = datetime(2026, 7, 12, 19, 30, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            workdir = Path(tmpdir)
            output_dir = workdir / "output"
            output_dir.mkdir(parents=True)
            flag_path = output_dir / "OUSDT_DRIFT_ALERT.flag"
            flag_path.write_text("current drift\n", encoding="utf-8")
            flag_time = now.timestamp() - 600
            os.utime(flag_path, (flag_time, flag_time))
            (output_dir / "ousdt_loop_state.json").write_text(
                '{"updated_at":"2026-07-12T19:29:30+00:00",'
                '"last_reconcile":{"open_order_diff":11,"actual_net_qty_diff":0,'
                '"protective_stop_required":false}}',
                encoding="utf-8",
            )

            cleared = clear_stale_drift_flag_if_reconciled(
                workdir=workdir,
                symbol="OUSDT",
                now=now,
            )

            self.assertIsNone(cleared)
            self.assertTrue(flag_path.exists())
