from __future__ import annotations

import unittest
from threading import Thread
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from grid_optimizer.audit import (
    build_audit_paths,
    collect_new_rows,
    exclusive_json_state_lock,
    fetch_time_paged,
    income_row_key,
    income_row_time_ms,
    parse_iso_ts,
    read_json,
    read_jsonl,
    read_jsonl_filtered,
    read_trade_audit_rows,
    scan_iso_bounds,
    trade_row_key,
    trade_row_time_ms,
    write_json,
)


class AuditTests(unittest.TestCase):
    def test_json_state_lock_is_reentrant_and_excludes_other_threads(self) -> None:
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "state.json"
            contender_errors: list[BaseException] = []

            with exclusive_json_state_lock(path):
                with exclusive_json_state_lock(path):
                    pass

                def contend() -> None:
                    try:
                        with exclusive_json_state_lock(path, timeout_seconds=0.05):
                            pass
                    except BaseException as exc:  # captured for the main test thread
                        contender_errors.append(exc)

                thread = Thread(target=contend)
                thread.start()
                thread.join(timeout=1.0)

            self.assertFalse(thread.is_alive())
            self.assertEqual(len(contender_errors), 1)
            self.assertIsInstance(contender_errors[0], TimeoutError)
            with exclusive_json_state_lock(path, timeout_seconds=0.05):
                pass

    def test_json_state_lock_canonicalizes_symlink_aliases(self) -> None:
        with TemporaryDirectory() as tmpdir:
            real_dir = Path(tmpdir) / "real"
            real_dir.mkdir()
            real_path = real_dir / "state.json"
            real_path.write_text("{}", encoding="utf-8")
            alias_path = Path(tmpdir) / "alias-state.json"
            alias_path.symlink_to(real_path)
            contender_errors: list[BaseException] = []

            with exclusive_json_state_lock(real_path):
                def contend() -> None:
                    try:
                        with exclusive_json_state_lock(
                            alias_path,
                            timeout_seconds=0.05,
                        ):
                            pass
                    except BaseException as exc:
                        contender_errors.append(exc)

                thread = Thread(target=contend)
                thread.start()
                thread.join(timeout=1.0)

            self.assertFalse(thread.is_alive())
            self.assertEqual(len(contender_errors), 1)
            self.assertIsInstance(contender_errors[0], TimeoutError)

    def test_json_state_lock_accepts_legacy_read_only_lockfile(self) -> None:
        with TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "state.json"
            state_path.write_text("{}", encoding="utf-8")
            state_path.chmod(0o600)
            lock_path = state_path.with_suffix(".json.state.lock")
            lock_path.write_text("", encoding="utf-8")
            lock_path.chmod(0o444)
            original_open = Path.open

            def reject_lockfile_write_open(
                target: Path,
                mode: str = "r",
                *args: object,
                **kwargs: object,
            ):
                if (
                    target.resolve(strict=False) == lock_path.resolve(strict=False)
                    and any(flag in mode for flag in ("a", "w", "+"))
                ):
                    raise PermissionError("legacy lockfile is not writable")
                return original_open(target, mode, *args, **kwargs)

            with patch.object(Path, "open", new=reject_lockfile_write_open):
                with exclusive_json_state_lock(state_path, timeout_seconds=0.05):
                    pass

            self.assertEqual(state_path.stat().st_mode & 0o777, 0o600)
            self.assertEqual(lock_path.stat().st_mode & 0o777, 0o444)

    def test_write_json_preserves_existing_file_when_write_is_interrupted(self) -> None:
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "state.json"
            old_payload = {"cycle": 17}
            path.write_text('{"cycle": 17}', encoding="utf-8")
            original_write_text = Path.write_text

            def interrupted_write(
                target: Path,
                data: str,
                encoding: str | None = None,
                errors: str | None = None,
                newline: str | None = None,
            ) -> int:
                original_write_text(
                    target,
                    data[:5],
                    encoding=encoding,
                    errors=errors,
                    newline=newline,
                )
                raise OSError("interrupted")

            with patch.object(Path, "write_text", new=interrupted_write):
                with self.assertRaisesRegex(OSError, "interrupted"):
                    write_json(path, {"cycle": 18})

            self.assertEqual(read_json(path), old_payload)

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

    def test_trade_row_key_normalizes_numeric_fallback_fields(self) -> None:
        integer_row = {"time": 1000, "orderId": 7, "side": "BUY", "price": "0.1800", "qty": "88"}
        decimal_row = {"time": "1000", "orderId": "7", "side": "buy", "price": 0.18, "qty": "88.0"}

        self.assertEqual(trade_row_key(integer_row), trade_row_key(decimal_row))

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

    def test_read_trade_audit_rows_includes_archives_and_dedupes(self) -> None:
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir) / "output"
            archive_dir = output_dir / "archive_removed_chip114_20260428_011520"
            archive_dir.mkdir(parents=True)
            current_path = output_dir / "chipusdt_loop_trade_audit.jsonl"
            archive_path = archive_dir / "chipusdt_loop_trade_audit.jsonl"
            archive_path.write_text(
                "\n".join(
                    [
                        '{"id": 1, "time": 1000, "side": "BUY", "price": "0.1", "qty": "10"}',
                        '{"id": 2, "time": 2000, "side": "SELL", "price": "0.2", "qty": "10"}',
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            current_path.write_text(
                "\n".join(
                    [
                        '{"id": 2, "time": 2000, "side": "SELL", "price": "0.2", "qty": "10"}',
                        '{"id": 3, "time": 3000, "side": "BUY", "price": "0.3", "qty": "10"}',
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            rows = read_trade_audit_rows(current_path, limit=0)

        self.assertEqual([row["id"] for row in rows], [1, 2, 3])

    def test_read_trade_audit_rows_refreshes_archive_cache_when_file_changes(self) -> None:
        with TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir) / "output"
            archive_dir = output_dir / "archive_removed_chip114_20260428_011520"
            archive_dir.mkdir(parents=True)
            current_path = output_dir / "chipusdt_loop_trade_audit.jsonl"
            archive_path = archive_dir / "chipusdt_loop_trade_audit.jsonl"
            current_path.write_text("", encoding="utf-8")
            archive_path.write_text(
                '{"id": 1, "time": 1000, "side": "BUY", "price": "0.1", "qty": "10"}\n',
                encoding="utf-8",
            )

            first_rows = read_trade_audit_rows(current_path, limit=0)
            archive_path.write_text(
                '{"id": 2, "time": 2000, "side": "SELL", "price": "0.2", "qty": "10"}\n',
                encoding="utf-8",
            )
            second_rows = read_trade_audit_rows(current_path, limit=0)

        self.assertEqual([row["id"] for row in first_rows], [1])
        self.assertEqual([row["id"] for row in second_rows], [2])

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
