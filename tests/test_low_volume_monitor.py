from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

from grid_optimizer.low_volume_monitor import check_symbol, summarize_recent_volume


class LowVolumeMonitorTests(unittest.TestCase):
    def test_summarize_recent_volume_uses_trade_time_window(self) -> None:
        now = datetime(2026, 5, 18, 1, 10, tzinfo=timezone.utc)
        rows = [
            {"time": int((now - timedelta(minutes=61)).timestamp() * 1000), "quoteQty": 900},
            {"time": int((now - timedelta(minutes=59)).timestamp() * 1000), "quoteQty": 400},
            {"time": int((now - timedelta(minutes=1)).timestamp() * 1000), "quoteQty": 250},
        ]

        summary = summarize_recent_volume(rows=rows, now=now, window_seconds=3600)

        self.assertEqual(summary["trade_count"], 2)
        self.assertAlmostEqual(summary["gross_notional"], 650.0)

    def test_hourly_low_volume_alerts_once_and_resets_after_recovery(self) -> None:
        now = datetime(2026, 5, 18, 1, 10, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            trade_path = output_dir / "billusdt_loop_trade_audit.jsonl"
            trade_path.write_text(
                "\n".join(
                    [
                        '{"time": %d, "quoteQty": 400}' % int((now - timedelta(minutes=59)).timestamp() * 1000),
                        '{"time": %d, "quoteQty": 250}' % int((now - timedelta(minutes=1)).timestamp() * 1000),
                    ]
                ),
                encoding="utf-8",
            )
            state: dict[str, object] = {
                "BILLUSDT": {
                    "status": "low_volume",
                    "first_low_volume_at": (now - timedelta(minutes=10)).isoformat(),
                    "alert_sent": False,
                }
            }

            result = check_symbol(
                symbol="BILLUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                window_seconds=3600,
                min_volume_notional=1000,
                threshold_seconds=0,
                alert_config_path=None,
                send_email=False,
            )
            repeat = check_symbol(
                symbol="BILLUSDT",
                output_dir=output_dir,
                state=state,
                now=now + timedelta(minutes=1),
                window_seconds=3600,
                min_volume_notional=1000,
                threshold_seconds=0,
                alert_config_path=None,
                send_email=False,
            )

            self.assertTrue(result["should_alert"])
            self.assertFalse(repeat["should_alert"])

            trade_path.write_text(
                '{"time": %d, "quoteQty": 1500}' % int((now + timedelta(minutes=2)).timestamp() * 1000),
                encoding="utf-8",
            )
            recovered = check_symbol(
                symbol="BILLUSDT",
                output_dir=output_dir,
                state=state,
                now=now + timedelta(minutes=2),
                window_seconds=3600,
                min_volume_notional=1000,
                threshold_seconds=0,
                alert_config_path=None,
                send_email=False,
            )

            self.assertFalse(recovered["should_alert"])
            self.assertEqual(state["BILLUSDT"]["status"], "normal")


if __name__ == "__main__":
    unittest.main()
