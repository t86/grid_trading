from __future__ import annotations

import json
import io
import subprocess
import tempfile
import unittest
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from grid_optimizer import competition_board
from grid_optimizer.competition_board import (
    COMPETITION_BOARD_PAGE,
    _entry_projection,
    _hinted_boards_for_source,
    _load_history_index,
    _normalize_entries,
    _parse_activity_period_bounds,
    _parse_segments,
    _per_user_reward_from_segment,
    CompetitionSource,
    build_reward_volume_targets,
    resolve_active_competition_board,
    upsert_competition_entry,
    build_competition_board_snapshot,
)


class CompetitionBoardTests(unittest.TestCase):
    def test_parse_segments_extracts_fixed_reward_brackets(self) -> None:
        text = """
        奖池结构
        第 1 - 200 名
        平分 1,600,000 SAHARA
        第 201 - 5000 名
        平分 2,400,000 SAHARA
        """
        segments = _parse_segments(text, total_rows=5000)
        self.assertEqual(len(segments), 2)
        self.assertEqual(segments[0]["start_rank"], 1)
        self.assertEqual(segments[0]["end_rank"], 200)
        self.assertEqual(segments[0]["reward_text"], "平分 1,600,000 SAHARA")
        self.assertEqual(segments[1]["start_rank"], 201)
        self.assertEqual(segments[1]["end_rank"], 5000)

    def test_parse_segments_falls_back_to_proportional_pool(self) -> None:
        text = """
        最终奖励将根据用户的累计交易量占合格用户总累计交易量的比例进行分配。
        总奖池 3,200,000 KAT
        单人奖励上限为80,000 KAT
        """
        segments = _parse_segments(text, total_rows=3979)
        self.assertEqual(
            segments,
            [
                {
                    "start_rank": 1,
                    "end_rank": 3979,
                    "rank_label": "全部合格用户",
                    "reward_text": "按交易量占比瓜分总奖池（单人上限 80,000 KAT）",
                }
            ],
        )

    def test_per_user_reward_handles_direct_and_shared_rewards(self) -> None:
        self.assertEqual(
            _per_user_reward_from_segment("80,000 OPN", start_rank=1, end_rank=1, prize_pool_value=None),
            80000.0,
        )
        self.assertEqual(
            _per_user_reward_from_segment(
                "平分 120,000 OPN",
                start_rank=6,
                end_rank=20,
                prize_pool_value=None,
            ),
            8000.0,
        )
        self.assertEqual(
            _per_user_reward_from_segment(
                "奖池的 10% 奖励",
                start_rank=1,
                end_rank=1,
                prize_pool_value=3200000.0,
            ),
            320000.0,
        )

    def test_entry_projection_computes_rank_gap_and_reward(self) -> None:
        board = {
            "rows": [
                {"rank": 1, "value": 1000.0},
                {"rank": 2, "value": 700.0},
                {"rank": 3, "value": 500.0},
            ],
            "segments": [
                {
                    "start_rank": 1,
                    "end_rank": 1,
                    "rank_label": "第 1 名",
                    "reward_text": "80,000 OPN",
                    "per_user_reward": 80000.0,
                },
                {
                    "start_rank": 2,
                    "end_rank": 3,
                    "rank_label": "第 2 - 3 名",
                    "reward_text": "平分 120,000 OPN",
                    "per_user_reward": 60000.0,
                },
            ],
            "threshold_value": 500.0,
            "reward_unit": "OPN",
        }
        projection = _entry_projection(
            {
                "id": "e1",
                "board_key": "futures_opn:交易量挑战赛",
                "name": "alice",
                "value": 750.0,
                "note": "",
                "updated_at_utc": "2026-03-23T00:00:00+00:00",
            },
            board,
        )
        self.assertEqual(projection["projected_rank"], 2)
        self.assertTrue(projection["eligible"])
        self.assertEqual(projection["projected_reward"], "60,000.00 OPN")
        self.assertEqual(projection["segment_label"], "第 2 - 3 名")
        self.assertEqual(projection["gap_to_next"], 250.0)

    def test_entry_projection_marks_rank_as_lower_bound_for_truncated_board(self) -> None:
        board = {
            "rows": [
                {"rank": 1, "value": 1000.0},
                {"rank": 2, "value": 900.0},
                {"rank": 200, "value": 500.0},
            ],
            "segments": [],
            "threshold_value": 500.0,
            "reward_unit": "OPN",
            "rows_truncated": True,
            "last_rank_fetched": 200,
        }
        projection = _entry_projection(
            {
                "id": "e2",
                "board_key": "futures_opn:交易量挑战赛",
                "name": "alice",
                "value": 450.0,
                "note": "",
                "updated_at_utc": "2026-03-23T00:00:00+00:00",
            },
            board,
        )
        self.assertEqual(projection["projected_rank"], 201)
        self.assertEqual(projection["projected_rank_text"], ">200")

    def test_hinted_kat_second_stage_can_render_without_resource_id(self) -> None:
        source = CompetitionSource(
            slug="futures_kat",
            symbol="KAT",
            market="futures",
            label="KAT 合约交易挑战赛",
            url="https://www.binance.com/zh-CN/activity/trading-competition/futures-kat-challenge?ref=YEK2JZJT",
        )
        hinted = _hinted_boards_for_source(source)
        self.assertIsNotNone(hinted)
        _, boards = hinted or ({}, [])
        self.assertEqual(len(boards), 2)
        self.assertEqual(boards[1].get("resourceId"), 46951)
        self.assertEqual(boards[1]["tabLabel"], "交易量挑战赛 - 第二阶段")

    def test_hinted_xaut_two_stage_boards_include_real_resource_ids(self) -> None:
        source = CompetitionSource(
            slug="futures_xaut",
            symbol="XAUT",
            market="futures",
            label="XAUT 合约交易挑战赛",
            url="https://www.bmwweb.technology/zh-CN/activity/trading-competition/futures-xaut-challenge-n?ref=1118658029",
        )
        hinted = _hinted_boards_for_source(source)
        self.assertIsNotNone(hinted)
        _, boards = hinted or ({}, [])
        self.assertEqual(len(boards), 2)
        self.assertEqual(boards[0].get("resourceId"), 47697)
        self.assertEqual(boards[0]["tabLabel"], "交易量挑战赛 - 第一阶段")
        self.assertEqual(boards[0]["activityEndAt"], "2026-04-06T07:59:00+08:00")
        self.assertEqual(boards[1].get("resourceId"), 47699)
        self.assertEqual(boards[1]["tabLabel"], "交易量挑战赛 - 第二阶段")
        self.assertEqual(boards[1]["activityEndAt"], "2026-04-16T07:59:00+08:00")

    def test_parse_activity_period_bounds_extracts_start_and_end(self) -> None:
        start_at, end_at = _parse_activity_period_bounds("2026/03/29 08:00 - 2026/04/08 07:59")
        self.assertEqual(start_at, "2026-03-29T08:00:00+08:00")
        self.assertEqual(end_at, "2026-04-08T07:59:00+08:00")

    def test_run_playwright_extract_uses_short_session_name(self) -> None:
        calls: list[list[str]] = []

        def fake_run(cmd: list[str], **_: object) -> subprocess.CompletedProcess[str]:
            calls.append(cmd)
            if "eval" in cmd:
                return subprocess.CompletedProcess(cmd, 0, stdout='### Result\n{"meta": {}, "boards": []}', stderr="")
            return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")

        with patch.object(competition_board.subprocess, "run", side_effect=fake_run):
            payload = competition_board._run_playwright_extract("https://example.com")

        self.assertEqual(payload, {"meta": {}, "boards": []})
        self.assertEqual(len(calls), 3)
        session_values = [cmd[cmd.index("--session") + 1] for cmd in calls]
        self.assertEqual(len(set(session_values)), 1)
        self.assertLessEqual(len(session_values[0]), 12)

    def test_ended_analytics_ui_explains_missing_previous_day_snapshot(self) -> None:
        self.assertIn("缺少前一天本地快照", COMPETITION_BOARD_PAGE)
        self.assertIn("结束时间：", COMPETITION_BOARD_PAGE)
        self.assertIn("board.forecast_coefficients", COMPETITION_BOARD_PAGE)
        self.assertIn("Math.min(predicted, previousPredictedValue)", COMPETITION_BOARD_PAGE)

    def test_fetch_symbol_close_price_usdt_falls_back_to_futures_archive(self) -> None:
        end_at = datetime.fromisoformat("2026-01-17T07:59:00+08:00")

        archive_buffer = io.BytesIO()
        with zipfile.ZipFile(archive_buffer, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            zf.writestr(
                "MAGMAUSDT-1m-2026-01-16.csv",
                "1737071880000,1,1,1,0.25,0,1737071939999,0,0,0,0,0\n"
                "1737071940000,1,1,1,0.30,0,1737071999999,0,0,0,0,0\n",
            )

        class FakeResponse:
            def __init__(self, status_code: int, payload: object = None, content: bytes = b"") -> None:
                self.status_code = status_code
                self._payload = payload
                self.content = content

            def raise_for_status(self) -> None:
                if self.status_code >= 400:
                    raise RuntimeError(f"http {self.status_code}")

            def json(self) -> object:
                return self._payload

        def fake_get(url: str, params: dict[str, object] | None = None, timeout: int = 0) -> FakeResponse:
            if "api/v3/klines" in url:
                return FakeResponse(200, payload={"code": -1121, "msg": "Invalid symbol."})
            if "data.binance.vision" in url:
                return FakeResponse(200, content=archive_buffer.getvalue())
            raise AssertionError(url)

        with tempfile.TemporaryDirectory() as temp_dir, patch.object(
            competition_board.requests, "get", side_effect=fake_get
        ), patch.object(
            competition_board, "REWARD_PRICE_CACHE_PATH", Path(temp_dir) / "reward_price_cache.json"
        ):
            price = competition_board._fetch_symbol_close_price_usdt("MAGMA", end_at)

        self.assertEqual(price, 0.30)

    def test_build_ended_boards_analytics_keeps_reward_rows_without_previous_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            history_dir = base / "competition_board_history"
            history_dir.mkdir(parents=True, exist_ok=True)
            history_index_path = base / "competition_board_history_index.json"
            history_index_path.write_text("{}", encoding="utf-8")

            board = {
                "board_key": "futures_magma:交易量挑战赛",
                "label": "MAGMA · 交易量挑战赛",
                "symbol": "MAGMA",
                "market": "futures",
                "activity_end_at": "2026-01-17T07:59:00+08:00",
                "updated_at_utc": "2026-01-16T23:59:59+00:00",
                "reward_unit": "MAGMA",
                "rows": [{"rank": 1, "value": 21832348.11}],
                "segments": [
                    {
                        "start_rank": 1,
                        "end_rank": 1,
                        "rank_label": "第 1 名",
                        "reward_text": "50,000 MAGMA",
                        "cutoff_value": 21832348.11,
                        "cutoff_value_text": "21,832,348.11",
                        "per_user_reward": 50000.0,
                        "per_user_reward_text": "50,000.00 MAGMA",
                    }
                ],
            }
            payload = {
                "board_key": board["board_key"],
                "capture_key": "2026-01-17 07:00",
                "capture_label": "2026-01-17 07:00",
                "capture_date": "2026-01-17",
                "capture_granularity": "hourly",
                "captured_at_utc": "2026-01-16T23:59:59+00:00",
                "board": board,
            }
            path = history_dir / "2026-01-17_0700__futures_magma交易量挑战赛.json"
            path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

            with patch.object(competition_board, "HISTORY_INDEX_PATH", history_index_path), patch.object(
                competition_board, "HISTORY_DIR_PATH", history_dir
            ), patch.object(competition_board, "_fetch_symbol_close_price_usdt", return_value=0.25):
                analytics = competition_board._build_ended_boards_analytics([board])

        self.assertEqual(analytics["delta_rows"], [])
        self.assertEqual(len(analytics["reward_rows"]), 1)
        self.assertEqual(analytics["reward_rows"][0]["symbol"], "MAGMA")
        self.assertEqual(analytics["reward_rows"][0]["activity_end_at"], "2026-01-17T07:59:00+08:00")
        self.assertEqual(analytics["reward_rows"][0]["reward_price_usdt"], 0.25)
        self.assertEqual(analytics["reward_rows"][0]["reward_value_usdt"], 12500.0)

    def test_build_ongoing_boards_analytics_derives_board_specific_forecast_coefficients(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            history_dir = base / "competition_board_history"
            history_dir.mkdir(parents=True, exist_ok=True)
            history_index_path = base / "competition_board_history_index.json"
            history_index_path.write_text("{}", encoding="utf-8")

            current_board = {
                "board_key": "futures_kat:交易量挑战赛_-_第二阶段",
                "label": "KAT · 交易量挑战赛 - 第二阶段",
                "symbol": "KAT",
                "market": "futures",
                "activity_end_at": "2099-04-08T07:59:00+08:00",
                "updated_at_utc": "2026-03-31T23:59:59+00:00",
                "eligible_metric_total": 1826125192.93,
                "reward_unit": "KAT",
                "rows": [
                    {"rank": 20, "value": 140469.4},
                    {"rank": 30, "value": 64772.2},
                    {"rank": 50, "value": 36076.57},
                    {"rank": 100, "value": 11196.67},
                    {"rank": 150, "value": 6126.98},
                    {"rank": 200, "value": 3239.57},
                ],
                "segments": [],
            }
            previous_board = {
                **current_board,
                "updated_at_utc": "2026-03-30T23:59:59+00:00",
                "eligible_metric_total": 990759286.12,
                "rows": [
                    {"rank": 20, "value": 101037.73},
                    {"rank": 30, "value": 41613.32},
                    {"rank": 50, "value": 21810.6},
                    {"rank": 100, "value": 7750.05},
                    {"rank": 150, "value": 3391.42},
                    {"rank": 200, "value": 1682.55},
                ],
            }
            payload = {
                "board_key": current_board["board_key"],
                "capture_key": "2026-03-31 07:00",
                "capture_label": "2026-03-31 07:00",
                "capture_date": "2026-03-31",
                "capture_granularity": "hourly",
                "captured_at_utc": "2026-03-30T23:59:59+00:00",
                "board": previous_board,
            }
            path = history_dir / "2026-03-31_0700__futures_kat交易量挑战赛-第二阶段.json"
            path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

            with patch.object(competition_board, "HISTORY_INDEX_PATH", history_index_path), patch.object(
                competition_board, "HISTORY_DIR_PATH", history_dir
            ), patch.object(competition_board, "_fetch_symbol_close_price_usdt", return_value=0.01):
                analytics = competition_board._build_ongoing_boards_analytics([current_board])

        self.assertEqual(len(analytics["board_rows"]), 1)
        row = analytics["board_rows"][0]
        self.assertAlmostEqual(row["latest_day_volume"], 835365906.81, places=2)
        self.assertAlmostEqual(row["forecast_coefficients"]["20"], 39431.67 / 835365906.81, places=12)
        self.assertAlmostEqual(row["forecast_coefficients"]["150"], 2735.56 / 835365906.81, places=12)
        self.assertGreater(row["forecast_coefficients"]["100"], row["forecast_coefficients"]["150"])

    def test_build_ongoing_boards_analytics_skips_empty_previous_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            history_dir = base / "competition_board_history"
            history_dir.mkdir(parents=True, exist_ok=True)
            history_index_path = base / "competition_board_history_index.json"
            history_index_path.write_text("{}", encoding="utf-8")

            current_board = {
                "board_key": "futures_kat:交易量挑战赛_-_第二阶段",
                "label": "KAT · 交易量挑战赛 - 第二阶段",
                "symbol": "KAT",
                "market": "futures",
                "activity_end_at": "2099-04-08T07:59:00+08:00",
                "updated_at_utc": "2026-03-31T23:59:59+00:00",
                "eligible_metric_total": 1826125192.93,
                "reward_unit": "KAT",
                "rows": [
                    {"rank": 20, "value": 140469.4},
                    {"rank": 200, "value": 3239.57},
                ],
                "segments": [],
            }
            empty_previous_board = {
                **current_board,
                "updated_at_utc": "",
                "eligible_metric_total": 0.0,
                "rows": [],
            }
            valid_previous_board = {
                **current_board,
                "updated_at_utc": "2026-03-30T23:59:59+00:00",
                "eligible_metric_total": 990759286.12,
                "rows": [
                    {"rank": 20, "value": 101037.73},
                    {"rank": 200, "value": 1682.55},
                ],
            }
            for file_name, captured_at_utc, board in [
                ("2026-03-31_0700__futures_kat交易量挑战赛-第二阶段.json", "2026-03-30T23:20:37.518091+00:00", empty_previous_board),
                ("2026-03-30_0700__futures_kat交易量挑战赛-第二阶段.json", "2026-03-29T23:20:37.518091+00:00", valid_previous_board),
            ]:
                payload = {
                    "board_key": current_board["board_key"],
                    "capture_key": file_name[:13].replace("_", " "),
                    "capture_label": file_name[:13].replace("_", " "),
                    "capture_date": file_name[:10],
                    "capture_granularity": "hourly",
                    "captured_at_utc": captured_at_utc,
                    "board": board,
                }
                path = history_dir / file_name
                path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

            with patch.object(competition_board, "HISTORY_INDEX_PATH", history_index_path), patch.object(
                competition_board, "HISTORY_DIR_PATH", history_dir
            ), patch.object(competition_board, "_fetch_symbol_close_price_usdt", return_value=0.01):
                analytics = competition_board._build_ongoing_boards_analytics([current_board])

        row = analytics["board_rows"][0]
        self.assertEqual(row["previous_capture"], "2026-03-30 07")
        self.assertAlmostEqual(row["forecast_coefficients"]["20"], 39431.67 / 835365906.81, places=12)

    def test_resolve_active_competition_board_prefers_current_phase(self) -> None:
        snapshot = {
            "boards": [
                {
                    "symbol": "KAT",
                    "market": "futures",
                    "label": "KAT · 交易量挑战赛 - 第一阶段",
                    "activity_start_at": "2026-03-19T17:00:00+08:00",
                    "activity_end_at": "2026-03-29T07:59:00+08:00",
                },
                {
                    "symbol": "KAT",
                    "market": "futures",
                    "label": "KAT · 交易量挑战赛 - 第二阶段",
                    "activity_start_at": "2026-03-29T08:00:00+08:00",
                    "activity_end_at": "2026-04-08T07:59:00+08:00",
                },
            ]
        }
        board = resolve_active_competition_board(
            "KATUSDT",
            "futures",
            snapshot=snapshot,
            now=datetime(2026, 3, 29, 0, 30, tzinfo=timezone.utc),
        )
        self.assertIsNotNone(board)
        self.assertEqual(board["label"], "KAT · 交易量挑战赛 - 第二阶段")

    def test_build_reward_volume_targets_returns_segment_targets(self) -> None:
        targets = build_reward_volume_targets(
            {
                "reward_unit": "KAT",
                "segments": [
                    {
                        "start_rank": 6,
                        "end_rank": 20,
                        "rank_label": "第 6 - 20 名",
                        "reward_text": "平分 562,500 KAT",
                        "per_user_reward": 37500.0,
                        "per_user_reward_text": "37,500.00 KAT",
                        "cutoff_value": 140469.4,
                    }
                ],
            }
        )
        self.assertEqual(
            targets,
            [
                {
                    "rank_label": "第 6 - 20 名",
                    "start_rank": 6,
                    "end_rank": 20,
                    "reward_text": "平分 562,500 KAT",
                    "reward_unit": "KAT",
                    "per_user_reward": 37500.0,
                    "per_user_reward_text": "37,500.00 KAT",
                    "cutoff_value": 140469.4,
                    "cutoff_value_text": "140,469.40",
                }
            ],
        )

    def test_load_history_index_merges_disk_files_with_stale_index(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            history_dir = base / "competition_board_history"
            history_dir.mkdir(parents=True, exist_ok=True)
            history_index_path = base / "competition_board_history_index.json"
            history_index_path.write_text(
                """
{
  "updated_at_utc": "2026-03-28T00:00:00+00:00",
  "boards": {
    "futures_kat:交易量挑战赛_-_第一阶段": [
      {
        "capture_key": "2026-03-25 07:00",
        "capture_label": "2026-03-25 07:00",
        "capture_date": "2026-03-25",
        "capture_granularity": "hourly",
        "captured_at_utc": "2026-03-25T00:00:00+00:00",
        "path": "output/competition_board_history/2026-03-25_0700__futures_kat旧.json",
        "label": "KAT · 交易量挑战赛 - 第一阶段",
        "updated_at_utc": "2026-03-24T23:59:59+00:00"
      }
    ]
  }
}
""",
                encoding="utf-8",
            )
            (history_dir / "2026-03-28_0700__futures_kat新.json").write_text(
                """
{
  "board_key": "futures_kat:交易量挑战赛_-_第一阶段",
  "capture_key": "2026-03-28 07:00",
  "capture_label": "2026-03-28 07:00",
  "capture_date": "2026-03-28",
  "capture_granularity": "hourly",
  "captured_at_utc": "2026-03-28T05:25:15.510863+00:00",
  "board": {
    "board_key": "futures_kat:交易量挑战赛_-_第一阶段",
    "label": "KAT · 交易量挑战赛 - 第一阶段",
    "updated_at_utc": "2026-03-27T23:59:59+00:00"
  }
}
""",
                encoding="utf-8",
            )
            with patch.object(competition_board, "HISTORY_INDEX_PATH", history_index_path), patch.object(
                competition_board, "HISTORY_DIR_PATH", history_dir
            ):
                index = _load_history_index()
            entries = index["futures_kat:交易量挑战赛_-_第一阶段"]
            self.assertEqual(entries[0]["capture_label"], "2026-03-28 07:00")
            self.assertEqual(len(entries), 2)

    def test_refresh_persists_boards_to_history_for_ended_analytics(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            cache_path = base / "competition_board_cache.json"
            entries_path = base / "competition_board_entries.json"
            history_dir = base / "competition_board_history"
            history_index_path = base / "competition_board_history_index.json"
            entries_path.write_text("[]", encoding="utf-8")
            stale_board = {
                "board_key": "futures_kat:交易量挑战赛_-_第一阶段",
                "label": "KAT · 交易量挑战赛 - 第一阶段",
                "symbol": "KAT",
                "market": "futures",
                "updated_at_utc": "2026-03-27T23:59:59+00:00",
                "activity_end_at": "2026-03-29T07:59:00+08:00",
                "rows": [{"rank": 200, "value": 37994.9}],
                "segments": [],
            }
            previous_payload = {
                "board_key": stale_board["board_key"],
                "capture_key": "2026-03-28 07:00",
                "capture_label": "2026-03-28 07:00",
                "capture_date": "2026-03-28",
                "capture_granularity": "hourly",
                "captured_at_utc": "2026-03-28T05:25:15.510863+00:00",
                "board": stale_board,
            }
            history_dir.mkdir(parents=True, exist_ok=True)
            (history_dir / "2026-03-28_0700__futures_kat交易量挑战赛_-_第一阶段.json").write_text(
                json.dumps(previous_payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            fresh_board = dict(stale_board)
            fresh_board["updated_at_utc"] = "2026-03-28T23:59:59+00:00"
            fresh_board["rows"] = [{"rank": 200, "value": 208428.74}]
            fresh_board["eligible_user_count"] = 40000
            fresh_board["current_floor_value_text"] = "208,428.74"

            def fake_refresh() -> dict[str, object]:
                payload = {
                    "generated_at_utc": "2026-03-29T00:10:00+00:00",
                    "boards": [fresh_board],
                    "markets": {"futures": [fresh_board], "spot": []},
                    "entries": [],
                    "errors": [],
                }
                competition_board._persist_boards_to_history([fresh_board], snapshot_generated_at_utc=payload["generated_at_utc"])
                return competition_board._attach_snapshot_analytics(payload)

            with patch.object(competition_board, "CACHE_PATH", cache_path), patch.object(
                competition_board, "ENTRIES_PATH", entries_path
            ), patch.object(competition_board, "HISTORY_INDEX_PATH", history_index_path), patch.object(
                competition_board, "HISTORY_DIR_PATH", history_dir
            ), patch.object(competition_board, "_refresh_competition_data", side_effect=fake_refresh), patch.object(
                competition_board, "COMPETITION_SOURCES", ()
            ):
                competition_board._MEMORY_CACHE["loaded_at"] = 0.0
                competition_board._MEMORY_CACHE["data"] = None
                snapshot = build_competition_board_snapshot(refresh=True)
            ended = snapshot["ended_analytics"]["delta_rows"]
            self.assertEqual(len(ended), 1)
            self.assertEqual(ended[0]["final_capture"], "2026-03-29 07:00")
            self.assertAlmostEqual(float(ended[0]["deltas"]["200"]), 170433.84, places=2)

    def test_build_snapshot_reprojects_entries_from_cached_boards(self) -> None:
        now_iso = datetime.now(timezone.utc).isoformat()
        with tempfile.TemporaryDirectory() as temp_dir:
            cache_path = Path(temp_dir) / "competition_board_cache.json"
            entries_path = Path(temp_dir) / "competition_board_entries.json"
            cache_path.write_text(
                """
{
  "generated_at_utc": "%s",
  "boards": [
    {
      "board_key": "spot_sahara:默认",
      "label": "SAHARA 现货交易竞赛",
      "metric_label": "交易量",
      "rows": [
        {"rank": 1, "value": 1000.0},
        {"rank": 2, "value": 700.0},
        {"rank": 3, "value": 500.0}
      ],
      "segments": [
        {
          "start_rank": 1,
          "end_rank": 2,
          "rank_label": "第 1 - 2 名",
          "reward_text": "平分 100,000 SAHARA",
          "per_user_reward": 50000.0
        }
      ],
      "threshold_value": 500.0,
      "reward_unit": "SAHARA"
    }
  ],
  "markets": {"spot": [], "futures": []},
  "entries": [],
  "errors": []
}
"""
                % now_iso,
                encoding="utf-8",
            )
            entries_path.write_text(
                """
[
  {
    "id": "entry-1",
    "board_key": "spot_sahara:默认",
    "name": "bob",
    "value": 800.0,
    "note": "manual",
    "updated_at_utc": "%s"
  }
]
"""
                % now_iso,
                encoding="utf-8",
            )
            with patch.object(competition_board, "CACHE_PATH", cache_path), patch.object(
                competition_board, "ENTRIES_PATH", entries_path
            ):
                competition_board._MEMORY_CACHE["loaded_at"] = 0.0
                competition_board._MEMORY_CACHE["data"] = None
                snapshot = build_competition_board_snapshot(refresh=False)
            self.assertEqual(len(snapshot["entries"]), 1)
            self.assertEqual(snapshot["entries"][0]["name"], "bob")
            self.assertEqual(snapshot["entries"][0]["projected_rank"], 2)
            self.assertEqual(snapshot["entries"][0]["projected_reward"], "50,000.00 SAHARA")

    def test_normalize_entries_keeps_latest_same_board_same_name(self) -> None:
        entries = [
            {
                "id": "entry-1",
                "board_key": "spot_sahara:默认",
                "name": "tl",
                "value": 1200.0,
                "note": "old",
                "updated_at_utc": "2026-03-23T00:00:00+00:00",
            },
            {
                "id": "entry-2",
                "board_key": "spot_sahara:默认",
                "name": "TL",
                "value": 1500.0,
                "note": "new",
                "updated_at_utc": "2026-03-23T01:00:00+00:00",
            },
        ]
        normalized = _normalize_entries(entries)
        self.assertEqual(len(normalized), 1)
        self.assertEqual(normalized[0]["id"], "entry-2")
        self.assertEqual(normalized[0]["value"], 1500.0)

    def test_upsert_updates_existing_entry_for_same_board_and_name(self) -> None:
        now_iso = datetime.now(timezone.utc).isoformat()
        with tempfile.TemporaryDirectory() as temp_dir:
            cache_path = Path(temp_dir) / "competition_board_cache.json"
            entries_path = Path(temp_dir) / "competition_board_entries.json"
            cache_path.write_text(
                """
{
  "generated_at_utc": "%s",
  "boards": [
    {
      "board_key": "spot_sahara:默认",
      "label": "SAHARA 现货交易竞赛",
      "metric_label": "交易量",
      "rows": [
        {"rank": 1, "value": 2000.0},
        {"rank": 2, "value": 1000.0}
      ],
      "segments": [],
      "threshold_value": 500.0,
      "reward_unit": "SAHARA"
    }
  ],
  "markets": {"spot": [], "futures": []},
  "entries": [],
  "errors": []
}
"""
                % now_iso,
                encoding="utf-8",
            )
            entries_path.write_text(
                """
[
  {
    "id": "entry-1",
    "board_key": "spot_sahara:默认",
    "name": "tl",
    "value": 1200.0,
    "note": "old",
    "updated_at_utc": "%s"
  }
]
"""
                % now_iso,
                encoding="utf-8",
            )
            with patch.object(competition_board, "CACHE_PATH", cache_path), patch.object(
                competition_board, "ENTRIES_PATH", entries_path
            ):
                competition_board._MEMORY_CACHE["loaded_at"] = 0.0
                competition_board._MEMORY_CACHE["data"] = {
                    "generated_at_utc": now_iso,
                    "boards": [
                        {
                            "board_key": "spot_sahara:默认",
                            "label": "SAHARA 现货交易竞赛",
                            "metric_label": "交易量",
                            "rows": [
                                {"rank": 1, "value": 2000.0},
                                {"rank": 2, "value": 1000.0},
                            ],
                            "segments": [],
                            "threshold_value": 500.0,
                            "reward_unit": "SAHARA",
                        }
                    ],
                    "markets": {"spot": [], "futures": []},
                    "entries": [],
                    "errors": [],
                }
                result = upsert_competition_entry(
                    {
                        "board_key": "spot_sahara:默认",
                        "name": "TL",
                        "value": 1500.0,
                        "note": "new",
                    }
                )
                saved = competition_board._load_entries()
                self.assertEqual(result["entry_id"], "entry-1")
                self.assertEqual(len(saved), 1)
                self.assertEqual(saved[0]["value"], 1500.0)
                self.assertEqual(saved[0]["note"], "new")
                self.assertEqual(len(competition_board._MEMORY_CACHE["data"]["entries"]), 1)
                self.assertEqual(competition_board._MEMORY_CACHE["data"]["entries"][0]["name"], "TL")


if __name__ == "__main__":
    unittest.main()
