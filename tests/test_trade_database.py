from __future__ import annotations

import os
import sys
import types
import unittest
from unittest.mock import MagicMock, patch

from grid_optimizer.trade_database import persist_cycle_snapshot, persist_trade_rows, trade_database_enabled


class TradeDatabaseTest(unittest.TestCase):
    def test_trade_database_disabled_by_default(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            self.assertFalse(trade_database_enabled())
            result = persist_trade_rows(
                symbol="BTCUSDT",
                market_type="futures",
                strategy_mode="volume_long_v4",
                config={},
                trade_rows=[{"id": 1}],
            )
        self.assertEqual(result["enabled"], False)
        self.assertEqual(result["trade_inserted"], 0)

    def test_persist_trade_rows_upserts_run_and_trade_fill(self) -> None:
        cursor = MagicMock()
        cursor.__enter__.return_value = cursor
        cursor.rowcount = 1
        conn = MagicMock()
        conn.__enter__.return_value = conn
        conn.cursor.return_value = cursor
        psycopg_stub = types.SimpleNamespace(connect=MagicMock(return_value=conn))

        with patch.dict(sys.modules, {"psycopg": psycopg_stub}), patch.dict(
            os.environ,
            {
                "GRID_TRADE_DB_ENABLED": "1",
                "GRID_TRADE_DB_WORKSPACE": "prod-110",
                "GRID_TRADE_DB_ACCOUNT": "binance-main",
            },
        ):
            result = persist_trade_rows(
                symbol="KATUSDT",
                market_type="futures",
                strategy_mode="volume_long_v4",
                config={"strategy_profile": "volume_long_v4", "step_price": 0.001},
                trade_rows=[
                    {
                        "id": "fill-1",
                        "orderId": 123,
                        "clientOrderId": "gx-kat-takeprof-1-abcd",
                        "symbol": "KATUSDT",
                        "side": "SELL",
                        "time": 1_800_000_000_000,
                        "price": "2.5",
                        "qty": "4",
                        "realizedPnl": "0.8",
                        "commission": "0.01",
                        "commissionAsset": "USDT",
                        "maker": True,
                    }
                ],
                income_rows=[
                    {
                        "tranId": "fund-1",
                        "time": 1_800_000_001_000,
                        "incomeType": "FUNDING_FEE",
                        "asset": "USDT",
                        "income": "0.02",
                    }
                ],
                database_url="postgresql://unit-test",
            )

        self.assertTrue(result["enabled"])
        self.assertEqual(result["trade_inserted"], 1)
        self.assertEqual(result["income_inserted"], 1)
        self.assertGreaterEqual(cursor.execute.call_count, 3)
        all_sql = "\n".join(str(call.args[0]) for call in cursor.execute.call_args_list)
        self.assertIn("strategy_runs_audit", all_sql)
        self.assertIn("strategy_trade_fills_audit", all_sql)
        self.assertIn("strategy_income_audit", all_sql)

    def test_persist_cycle_snapshot_records_market_features_and_outcome(self) -> None:
        cursor = MagicMock()
        cursor.__enter__.return_value = cursor
        cursor.rowcount = 1
        conn = MagicMock()
        conn.__enter__.return_value = conn
        conn.cursor.return_value = cursor
        psycopg_stub = types.SimpleNamespace(connect=MagicMock(return_value=conn))

        with patch.dict(sys.modules, {"psycopg": psycopg_stub}), patch.dict(
            os.environ,
            {
                "GRID_TRADE_DB_ENABLED": "1",
                "GRID_TRADE_DB_WORKSPACE": "prod-110",
                "GRID_TRADE_DB_ACCOUNT": "binance-main",
            },
        ):
            result = persist_cycle_snapshot(
                symbol="XAUTUSDT",
                market_type="futures",
                strategy_mode="xaut_volume_guarded_bard_v2",
                config={"strategy_profile": "xaut_volume_guarded_bard_v2", "step_price": 1.2},
                summary={
                    "ts": "2026-05-17T08:00:00+00:00",
                    "cycle": 7,
                    "symbol": "XAUTUSDT",
                    "strategy_mode": "one_way_long",
                    "mid_price": 3200.0,
                    "market_guard_return_ratio": -0.002,
                    "market_guard_amplitude_ratio": 0.004,
                    "effective_step_price": 1.8,
                    "base_step_price": 1.2,
                    "gross_notional": 10000,
                    "realized_pnl": -1.5,
                    "commission_quote": 2.0,
                    "net_pnl_estimate": -3.5,
                    "trade_count": 12,
                    "risk_state": "reduce_only",
                },
                database_url="postgresql://unit-test",
            )

        self.assertTrue(result["enabled"])
        self.assertEqual(result["cycle_inserted"], 1)
        all_sql = "\n".join(str(call.args[0]) for call in cursor.execute.call_args_list)
        self.assertIn("strategy_cycle_snapshots_audit", all_sql)


if __name__ == "__main__":
    unittest.main()
