from __future__ import annotations

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from grid_optimizer.web import (
    _build_competition_volume_chart,
    _build_competition_volume_fills,
    _build_competition_volume_summary,
    _list_competition_volume_symbols,
)


class CompetitionVolumeMonitorTests(unittest.TestCase):
    def test_lists_symbols_from_local_control_files(self) -> None:
        with TemporaryDirectory() as tmp:
            output = Path(tmp)
            (output / "chipusdt_loop_runner_control.json").write_text(
                json.dumps({"symbol": "CHIPUSDT", "summary_jsonl": str(output / "chipusdt_loop_events.jsonl")}),
                encoding="utf-8",
            )

            result = _list_competition_volume_symbols(output_dir=output)

        self.assertEqual(result["symbols"], ["CHIPUSDT"])

    def test_summary_uses_trade_income_and_latest_elastic_event(self) -> None:
        with TemporaryDirectory() as tmp:
            output = Path(tmp)
            summary_path = output / "chipusdt_loop_events.jsonl"
            trade_path = output / "chipusdt_loop_trade_audit.jsonl"
            income_path = output / "chipusdt_loop_income_audit.jsonl"
            summary_path.write_text(
                json.dumps(
                    {
                        "ts": "2026-05-07T08:02:00+00:00",
                        "elastic_volume": {"regime": "defensive"},
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            trade_path.write_text(
                "\n".join(
                    [
                        json.dumps(
                            {
                                "symbol": "CHIPUSDT",
                                "time": 1778140800000,
                                "price": "2.0",
                                "qty": "10",
                                "realizedPnl": "-0.5",
                                "commission": "0.02",
                                "commissionAsset": "USDT",
                                "maker": True,
                            }
                        ),
                        json.dumps(
                            {
                                "symbol": "CHIPUSDT",
                                "time": 1778140860000,
                                "price": "2.1",
                                "qty": "5",
                                "realizedPnl": "0.1",
                                "commission": "0.01",
                                "commissionAsset": "USDT",
                                "maker": True,
                            }
                        ),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            income_path.write_text(
                json.dumps({"symbol": "CHIPUSDT", "time": 1778140900000, "income": "-0.03"}) + "\n",
                encoding="utf-8",
            )

            result = _build_competition_volume_summary(
                symbol="CHIPUSDT",
                summary_path=summary_path,
                competition_start_at="2026-05-07T08:00:00+00:00",
            )

        self.assertAlmostEqual(result["competition_gross_notional"], 30.5)
        self.assertAlmostEqual(result["competition_commission"], 0.03)
        self.assertAlmostEqual(result["competition_net_pnl"], -0.46)
        self.assertEqual(result["elastic_regime"], "defensive")
        self.assertAlmostEqual(result["maker_ratio"], 1.0)

    def test_fills_returns_recent_trade_rows(self) -> None:
        with TemporaryDirectory() as tmp:
            output = Path(tmp)
            summary_path = output / "chipusdt_loop_events.jsonl"
            summary_path.write_text("", encoding="utf-8")
            trade_path = output / "chipusdt_loop_trade_audit.jsonl"
            trade_path.write_text(
                json.dumps(
                    {
                        "symbol": "CHIPUSDT",
                        "time": 1778140800000,
                        "side": "BUY",
                        "price": "2.0",
                        "qty": "10",
                        "realizedPnl": "-0.5",
                        "commission": "0.02",
                        "commissionAsset": "USDT",
                        "maker": True,
                        "orderId": 123,
                        "clientOrderId": "gx-chip-1",
                    }
                )
                + "\n",
                encoding="utf-8",
            )

            result = _build_competition_volume_fills(symbol="CHIPUSDT", summary_path=summary_path, limit=20)

        self.assertEqual(len(result["fills"]), 1)
        self.assertEqual(result["fills"][0]["side"], "BUY")
        self.assertAlmostEqual(result["fills"][0]["notional"], 20.0)
        self.assertTrue(result["fills"][0]["maker"])

    def test_chart_returns_cumulative_series(self) -> None:
        with TemporaryDirectory() as tmp:
            output = Path(tmp)
            summary_path = output / "chipusdt_loop_events.jsonl"
            summary_path.write_text("", encoding="utf-8")
            trade_path = output / "chipusdt_loop_trade_audit.jsonl"
            trade_path.write_text(
                "\n".join(
                    [
                        json.dumps(
                            {
                                "symbol": "CHIPUSDT",
                                "time": 1778140800000,
                                "price": "2.0",
                                "qty": "10",
                                "realizedPnl": "-0.5",
                                "commission": "0.02",
                                "commissionAsset": "USDT",
                            }
                        ),
                        json.dumps(
                            {
                                "symbol": "CHIPUSDT",
                                "time": 1778140860000,
                                "price": "2.1",
                                "qty": "5",
                                "realizedPnl": "0.1",
                                "commission": "0.01",
                                "commissionAsset": "USDT",
                            }
                        ),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            result = _build_competition_volume_chart(
                symbol="CHIPUSDT",
                summary_path=summary_path,
                competition_start_at="2026-05-07T08:00:00+00:00",
            )

        self.assertEqual(len(result["points"]), 2)
        self.assertAlmostEqual(result["points"][-1]["cumulative_notional"], 30.5)
        self.assertAlmostEqual(result["points"][-1]["cumulative_commission"], 0.03)
        self.assertAlmostEqual(result["points"][-1]["cumulative_net_pnl"], -0.43)


if __name__ == "__main__":
    unittest.main()
