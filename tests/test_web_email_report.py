from __future__ import annotations

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from grid_optimizer.audit import build_audit_paths
from grid_optimizer.web import _build_hourly_symbol_report_email


class WebEmailReportTests(unittest.TestCase):
    def test_build_hourly_symbol_report_email_includes_large_loss_context(self) -> None:
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir) / "output"
            output_dir.mkdir(parents=True, exist_ok=True)
            summary_path = output_dir / "soonusdt_loop_events.jsonl"
            audit_paths = build_audit_paths(summary_path)

            events = [
                {
                    "ts": "2026-04-20T09:00:10+00:00",
                    "cycle": 11,
                    "runtime_status": "running",
                    "center_price": 0.1773,
                    "mid_price": 0.1768,
                    "actual_net_qty": -169.0,
                    "actual_net_notional": -29.88,
                    "cumulative_gross_notional": 1200.0,
                }
            ]
            summary_path.write_text(
                "\n".join(json.dumps(item, ensure_ascii=False) for item in events) + "\n",
                encoding="utf-8",
            )

            trades = [
                {
                    "time": 1776675615000,
                    "orderId": 123,
                    "side": "SELL",
                    "positionSide": "BOTH",
                    "price": "0.1768",
                    "qty": "168",
                    "realizedPnl": "-0.62",
                    "commission": "0.01",
                }
            ]
            audit_paths["trade_audit"].write_text(
                "\n".join(json.dumps(item, ensure_ascii=False) for item in trades) + "\n",
                encoding="utf-8",
            )
            audit_paths["income_audit"].write_text("", encoding="utf-8")

            payload = _build_hourly_symbol_report_email(
                {"symbol": "SOONUSDT", "summary_jsonl": str(summary_path)},
                window_minutes=10_000,
            )

            self.assertIsNotNone(payload)
            body = str(payload["body"])
            self.assertIn("large_loss_count: 1", body)
            self.assertIn("realized_pnl=-0.62000000", body)
            self.assertIn("center_price=0.17730000", body)
            self.assertIn("mid_price=0.17680000", body)


if __name__ == "__main__":
    unittest.main()
