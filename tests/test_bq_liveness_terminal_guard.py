from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch

from grid_optimizer.bq_liveness_terminal_guard import invalid_state_reason, main, target_done_marker


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

    def test_valid_state_allows_legacy_watchdog(self) -> None:
        with TemporaryDirectory() as tmpdir:
            workdir = Path(tmpdir)
            output_dir = workdir / "output"
            legacy = output_dir / "ops" / "bq_liveness_watchdog.py"
            legacy.parent.mkdir(parents=True)
            legacy.write_text("", encoding="utf-8")
            (output_dir / "ousdt_loop_state.json").write_text("{}", encoding="utf-8")
            with patch("grid_optimizer.bq_liveness_terminal_guard.runpy.run_path") as run_path:
                self.assertEqual(main(["--symbol", "OUSDT", "--workdir", str(workdir), "--enforce"]), 0)
            run_path.assert_called_once()
