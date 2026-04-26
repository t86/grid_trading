from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from grid_optimizer.audit import (
    build_audit_paths,
    collect_new_rows,
    fetch_time_paged,
    income_row_key,
    income_row_time_ms,
    parse_iso_ts,
    read_jsonl,
    read_jsonl_filtered,
    scan_iso_bounds,
    trade_row_key,
    trade_row_time_ms,
)


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

    def test_read_jsonl_keeps_only_tail_when_limit_is_set(self) -> None:
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "rows.jsonl"
            path.write_text(
                "\n".join(
                    [
                        '{"idx": 1}',
                        '{"idx": 2}',
                        '{"idx": 3}',
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            rows = read_jsonl(path, limit=2)

        self.assertEqual([row["idx"] for row in rows], [2, 3])

    def test_read_jsonl_filtered_applies_predicate(self) -> None:
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "rows.jsonl"
            path.write_text(
                "\n".join(
                    [
                        '{"time": 1000, "id": 1}',
                        '{"time": 2000, "id": 2}',
                        '{"time": 3000, "id": 3}',
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            rows = read_jsonl_filtered(path, limit=0, predicate=lambda item: int(item["time"]) >= 2000)

        self.assertEqual([row["id"] for row in rows], [2, 3])

    def test_fetch_time_paged_splits_requests_by_max_window(self) -> None:
        calls: list[tuple[int | None, int | None]] = []

        def fetch_page(**params: int) -> list[dict[str, int]]:
            start = params.get("start_time_ms")
            end = params.get("end_time_ms")
            calls.append((start, end))
            if start == 1_001:
                return [{"time": 1_500, "id": 1}]
            if start == 2_002:
                return [{"time": 2_500, "id": 2}]
            return []

        rows = fetch_time_paged(
            fetch_page=fetch_page,
            start_time_ms=0,
            end_time_ms=3_000,
            limit=1000,
            row_time_ms=trade_row_time_ms,
            row_key=trade_row_key,
            max_window_ms=1_000,
        )

        self.assertEqual([row["id"] for row in rows], [1, 2])
        self.assertEqual(calls, [(0, 1_000), (1_001, 2_001), (2_002, 3_000)])

    def test_scan_iso_bounds_streams_file(self) -> None:
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "rows.jsonl"
            path.write_text(
                "\n".join(
                    [
                        '{"ts": "2026-04-01T00:00:00+00:00"}',
                        '{"ts": "2026-04-01T01:00:00+00:00"}',
                        '{"ts": "2026-04-01T02:00:00+00:00"}',
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            first_ts, last_ts = scan_iso_bounds(path)

        self.assertEqual(first_ts, parse_iso_ts("2026-04-01T00:00:00+00:00"))
        self.assertEqual(last_ts, parse_iso_ts("2026-04-01T02:00:00+00:00"))


if __name__ == "__main__":
    unittest.main()
