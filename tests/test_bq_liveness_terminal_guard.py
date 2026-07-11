from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch

from grid_optimizer.bq_liveness_terminal_guard import main, target_done_marker


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
