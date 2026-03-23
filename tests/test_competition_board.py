from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from grid_optimizer import competition_board
from grid_optimizer.competition_board import (
    _entry_projection,
    _hinted_boards_for_source,
    _normalize_entries,
    _parse_segments,
    _per_user_reward_from_segment,
    CompetitionSource,
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
        self.assertIsNone(boards[1].get("resourceId"))
        self.assertEqual(boards[1]["tabLabel"], "交易量挑战赛 - 第二阶段")

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
