from __future__ import annotations

import json
import unittest
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from grid_optimizer.audit import build_audit_paths
from grid_optimizer.web import _build_hourly_symbol_report_email


class WebEmailReportTests(unittest.TestCase):
    def test_build_hourly_symbol_report_email_uses_chinese_hourly_summary(self) -> None:
        now = datetime(2026, 4, 24, 2, 30, tzinfo=timezone.utc)
        trade_time_ms = int(datetime(2026, 4, 24, 2, 6, tzinfo=timezone.utc).timestamp() * 1000)
        income_time_ms = int(datetime(2026, 4, 24, 2, 10, tzinfo=timezone.utc).timestamp() * 1000)
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir) / "output"
            output_dir.mkdir(parents=True, exist_ok=True)
            summary_path = output_dir / "soonusdt_loop_events.jsonl"
            audit_paths = build_audit_paths(summary_path)

            summary_path.write_text(
                json.dumps(
                    {
                        "ts": "2026-04-24T02:25:00+00:00",
                        "runtime_status": "running",
                        "center_price": 0.1773,
                        "mid_price": 0.1768,
                        "actual_net_qty": -684.0,
                        "actual_net_notional": -121.0549,
                        "cumulative_gross_notional": 1200.0,
                    },
                    ensure_ascii=False,
                )
                + "\n",
                encoding="utf-8",
            )
            audit_paths["trade_audit"].write_text(
                json.dumps(
                    {
                        "time": trade_time_ms,
                        "orderId": 123,
                        "side": "SELL",
                        "positionSide": "BOTH",
                        "price": "1.210549",
                        "qty": "100",
                        "realizedPnl": "-79.4098",
                        "commission": "0.2200",
                    },
                    ensure_ascii=False,
                )
                + "\n",
                encoding="utf-8",
            )
            audit_paths["income_audit"].write_text(
                json.dumps(
                    {
                        "time": income_time_ms,
                        "asset": "USDT",
                        "incomeType": "FUNDING_FEE",
                        "income": "0.0000",
                    },
                    ensure_ascii=False,
                )
                + "\n",
                encoding="utf-8",
            )

            snapshot = {
                "position": {
                    "position_amt": -684.0,
                    "long_qty": 0.0,
                    "short_qty": 684.0,
                    "unrealized_pnl": 0.1161,
                    "wallet_balance": 920.0,
                    "available_balance": 870.0,
                },
                "account_assets": {
                    "USDT": {
                        "asset": "USDT",
                        "available_balance": 870.0,
                        "wallet_balance": 920.0,
                    },
                    "BNB": {
                        "asset": "BNB",
                        "available_balance": 0.1234,
                        "wallet_balance": 0.2234,
                    },
                },
                "open_orders": [
                    {
                        "order_id": 456,
                        "side": "BUY",
                        "position_side": "BOTH",
                        "price": 1.205,
                        "orig_qty": 50.0,
                        "executed_qty": 10.0,
                        "reduce_only": False,
                    }
                ],
            }

            with (
                patch("grid_optimizer.web.alert_source_label", return_value="114"),
                patch("grid_optimizer.web.build_monitor_snapshot", return_value=snapshot),
                patch("grid_optimizer.web.read_symbol_runner_process", return_value={"is_running": True}),
            ):
                payload = _build_hourly_symbol_report_email(
                    {"symbol": "SOONUSDT", "summary_jsonl": str(summary_path)},
                    window_minutes=60,
                    now=now,
                )

            self.assertIsNotNone(payload)
            self.assertEqual(payload["subject"], "[grid][114] SOONUSDT 一小时交易通知")
            body = str(payload["body"])
            self.assertIn("这一小时交易量: 121.0549 USDT", body)
            self.assertIn("损耗值: -79.6298 USDT", body)
            self.assertIn("手续费: 0.2200 USDT", body)
            self.assertIn("当前库存: 净仓 -684.0000 · 多 0.0000 · 空 684.0000", body)
            self.assertIn("库存价值: -121.0549 USDT", body)
            self.assertIn("净收益估算: -79.5137 USDT", body)
            self.assertIn("已实现: -79.4098 · 浮盈: +0.1161", body)
            self.assertIn("剩余BNB: 可用 0.1234 · 钱包 0.2234", body)
            self.assertIn("剩余资产: USDT 可用 870.0000 · 钱包 920.0000", body)
            self.assertIn("当前委托单:", body)
            self.assertIn("BUY BOTH 价格 1.20500000 · 数量 50.0000 · 已成交 10.0000", body)


if __name__ == "__main__":
    unittest.main()
