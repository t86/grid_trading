from __future__ import annotations

import json
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

from grid_optimizer.volume_spike_monitor import check_symbol, summarize_event_volume


class VolumeSpikeMonitorTests(unittest.TestCase):
    def test_summarize_event_volume_compares_current_minute_to_baseline(self) -> None:
        now = datetime(2026, 6, 9, 6, 30, tzinfo=timezone.utc)
        rows = [
            {"ts": (now - timedelta(minutes=11)).isoformat(), "cumulative_gross_notional": 1000},
            {"ts": (now - timedelta(minutes=1)).isoformat(), "cumulative_gross_notional": 2000},
            {"ts": now.isoformat(), "cumulative_gross_notional": 3500},
        ]

        summary = summarize_event_volume(
            rows=rows,
            now=now,
            current_window_seconds=60,
            baseline_window_seconds=600,
        )

        self.assertTrue(summary["available"])
        self.assertAlmostEqual(summary["baseline_per_minute"], 100.0)
        self.assertAlmostEqual(summary["current_volume"], 1500.0)

    def test_check_symbol_alerts_once_during_cooldown(self) -> None:
        now = datetime(2026, 6, 9, 6, 30, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            path = output_dir / "straxusdt_spot_events.jsonl"
            path.write_text(
                "\n".join(
                    json.dumps(row)
                    for row in [
                        {"ts": (now - timedelta(minutes=11)).isoformat(), "cumulative_gross_notional": 1000},
                        {"ts": (now - timedelta(minutes=1)).isoformat(), "cumulative_gross_notional": 2000},
                        {"ts": now.isoformat(), "cumulative_gross_notional": 5000},
                    ]
                ),
                encoding="utf-8",
            )
            state: dict[str, object] = {}

            first = check_symbol(
                symbol="STRAXUSDT",
                output_dir=output_dir,
                state=state,
                now=now,
                current_window_seconds=60,
                baseline_window_seconds=600,
                min_spike_notional=1000,
                multiplier=3,
                cooldown_seconds=900,
                alert_config_path=None,
                bark_config_path=None,
                send_notifications=False,
            )
            later = now + timedelta(minutes=1)
            path.write_text(
                "\n".join(
                    json.dumps(row)
                    for row in [
                        {"ts": (now - timedelta(minutes=10)).isoformat(), "cumulative_gross_notional": 1000},
                        {"ts": now.isoformat(), "cumulative_gross_notional": 5000},
                        {"ts": later.isoformat(), "cumulative_gross_notional": 9000},
                    ]
                ),
                encoding="utf-8",
            )
            repeat = check_symbol(
                symbol="STRAXUSDT",
                output_dir=output_dir,
                state=state,
                now=later,
                current_window_seconds=60,
                baseline_window_seconds=600,
                min_spike_notional=1000,
                multiplier=3,
                cooldown_seconds=900,
                alert_config_path=None,
                bark_config_path=None,
                send_notifications=False,
            )

            self.assertTrue(first["spike"])
            self.assertTrue(first["should_alert"])
            self.assertTrue(repeat["spike"])
            self.assertFalse(repeat["should_alert"])


if __name__ == "__main__":
    unittest.main()
