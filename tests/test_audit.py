from __future__ import annotations

import unittest
from pathlib import Path

from grid_optimizer.audit import build_audit_paths, collect_new_rows, income_row_key, income_row_time_ms, trade_row_key, trade_row_time_ms


class AuditTests(unittest.TestCase):
    def test_build_audit_paths_derives_consistent_filenames(self) -> None:
        paths = build_audit_paths(Path("output/night_loop_events.jsonl"))

        self.assertEqual(str(paths["plan_audit"]), "output/night_loop_plan_audit.jsonl")
        self.assertEqual(str(paths["submit_audit"]), "output/night_loop_submit_audit.jsonl")
        self.assertEqual(str(paths["order_audit"]), "output/night_loop_order_audit.jsonl")
        self.assertEqual(str(paths["trade_audit"]), "output/night_loop_trade_audit.jsonl")
        self.assertEqual(str(paths["income_audit"]), "output/night_loop_income_audit.jsonl")
        self.assertEqual(str(paths["audit_state"]), "output/night_loop_audit_state.json")

    def test_collect_new_rows_skips_already_synced_trade_rows_at_same_timestamp(self) -> None:
        rows = [
            {"id": 1, "time": 1000, "side": "BUY", "price": "0.05", "qty": "100"},
            {"id": 2, "time": 1000, "side": "SELL", "price": "0.051", "qty": "100"},
            {"id": 3, "time": 2000, "side": "BUY", "price": "0.049", "qty": "120"},
        ]

        fresh_rows, last_time_ms, last_keys = collect_new_rows(
            rows=rows,
            last_time_ms=1000,
            last_keys_at_time=["1"],
            row_time_ms=trade_row_time_ms,
            row_key=trade_row_key,
        )

        self.assertEqual([int(item["id"]) for item in fresh_rows], [2, 3])
        self.assertEqual(last_time_ms, 2000)
        self.assertEqual(last_keys, ["3"])

    def test_collect_new_rows_tracks_income_rows_by_tran_id(self) -> None:
        rows = [
            {"tranId": 10, "time": 1000, "income": "0.1", "incomeType": "FUNDING_FEE"},
            {"tranId": 11, "time": 1000, "income": "-0.1", "incomeType": "FUNDING_FEE"},
            {"tranId": 12, "time": 3000, "income": "0.2", "incomeType": "FUNDING_FEE"},
        ]

        fresh_rows, last_time_ms, last_keys = collect_new_rows(
            rows=rows,
            last_time_ms=1000,
            last_keys_at_time=[income_row_key(rows[0])],
            row_time_ms=income_row_time_ms,
            row_key=income_row_key,
        )

        self.assertEqual([int(item["tranId"]) for item in fresh_rows], [11, 12])
        self.assertEqual(last_time_ms, 3000)
        self.assertEqual(last_keys, [income_row_key(rows[2])])


if __name__ == "__main__":
    unittest.main()
