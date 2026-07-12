from __future__ import annotations

import unittest
from datetime import datetime, timezone

from grid_optimizer.production_volume_snapshot import build_exchange_volume_snapshot


class ProductionVolumeSnapshotTests(unittest.TestCase):
    def test_snapshot_uses_exchange_ids_and_deduplicates(self) -> None:
        now = datetime(2026, 7, 12, 4, 0, tzinfo=timezone.utc)
        rows = [
            {"id": 1, "time": int(now.timestamp() * 1000) - 60_000, "quoteQty": "88", "realizedPnl": "-1"},
            {"id": "1", "time": int(now.timestamp() * 1000) - 60_000, "quoteQty": "88.0", "realizedPnl": "-1"},
            {"id": 2, "time": int(now.timestamp() * 1000) - 600_000, "quoteQty": "12", "realizedPnl": "0.5"},
        ]

        snapshot = build_exchange_volume_snapshot(
            symbol="ARXUSDT",
            rows=rows,
            now=now,
            target_notional=120_000,
        )

        self.assertTrue(snapshot["canonical"])
        self.assertEqual(snapshot["source"], "binance_futures_userTrades")
        self.assertEqual(snapshot["dedupe_key"], "exchange_trade_id")
        self.assertEqual(snapshot["day"]["gross_quote_qty"], 100.0)
        self.assertEqual(snapshot["day"]["trade_count"], 2)
        self.assertEqual(snapshot["windows"]["5m"]["gross_quote_qty"], 88.0)
        self.assertEqual(snapshot["windows"]["15m"]["gross_quote_qty"], 100.0)
        self.assertEqual(snapshot["day"]["realized_pnl"], -0.5)

    def test_snapshot_rejects_rows_without_exchange_trade_id(self) -> None:
        now = datetime(2026, 7, 12, 4, 0, tzinfo=timezone.utc)

        with self.assertRaisesRegex(ValueError, "missing canonical trade id"):
            build_exchange_volume_snapshot(
                symbol="ARXUSDT",
                rows=[{"time": int(now.timestamp() * 1000), "quoteQty": "88"}],
                now=now,
            )


if __name__ == "__main__":
    unittest.main()
