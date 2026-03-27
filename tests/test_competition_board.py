from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from grid_optimizer import competition_board
from grid_optimizer.competition_board import (
    _entry_projection,
    _forecast_board_with_entry,
    _hinted_boards_for_source,
    _normalize_entries,
    _parse_segments,
    _per_user_reward_from_segment,
    CompetitionSource,
    archive_competition_board_history,
    capture_due_competition_board_history,
    capture_daily_competition_board_history,
    load_competition_board_history,
    load_competition_board_trend,
    upsert_competition_entry,
    build_competition_board_snapshot,
)


class CompetitionBoardTests(unittest.TestCase):
    def _sample_snapshot(self) -> dict[str, object]:
        return {
            "generated_at_utc": "2026-03-24T05:00:00+00:00",
            "boards": [
                {
                    "board_key": "spot_sahara:默认",
                    "label": "SAHARA 现货交易竞赛",
                    "base_label": "SAHARA 现货交易竞赛",
                    "market": "spot",
                    "symbol": "SAHARA",
                    "tab_label": "默认",
                    "url": "https://example.com",
                    "metric_label": "交易量 (USD)",
                    "threshold_value": 500.0,
                    "threshold_unit": "USD",
                    "reward_unit": "SAHARA",
                    "eligible_user_count": 100,
                    "current_floor_value": 666.0,
                    "current_floor_value_text": "666.00",
                    "updated_text": "2026/3/24 13:59:59",
                    "updated_at_utc": "2026-03-24T05:59:59+00:00",
                    "rows_truncated": False,
                    "segments": [{"cutoff_value": 666.0, "cutoff_value_text": "666.00"}],
                    "top_rows": [{"rank": 1, "name": "alice", "value": 1000.0, "value_text": "1,000.00"}],
                    "rows": [{"rank": 1, "name": "alice", "value": 1000.0, "value_text": "1,000.00"}],
                }
            ],
            "markets": {"spot": [], "futures": []},
            "entries": [],
            "errors": [],
        }

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

    def test_hinted_bard_two_stage_metadata_is_available(self) -> None:
        source = CompetitionSource(
            slug="futures_bard",
            symbol="BARD",
            market="futures",
            label="BARD 合约交易挑战赛",
            url="https://www.binance.com/zh-CN/activity/trading-competition/futures-bard-challenge2?ref=YEK2JZJT",
        )
        hinted = _hinted_boards_for_source(source)
        self.assertIsNotNone(hinted)
        _, boards = hinted or ({}, [])
        self.assertEqual(len(boards), 2)
        self.assertEqual(boards[0]["tabLabel"], "交易量挑战赛 - 第一阶段")
        self.assertEqual(boards[0]["resourceId"], 47457)
        self.assertEqual(boards[1]["tabLabel"], "交易量挑战赛 - 第二阶段")
        self.assertIsNone(boards[1].get("resourceId"))

    def test_forecast_board_with_entry_returns_three_scenarios(self) -> None:
        board = {
            "board_key": "futures_opn:交易量挑战赛",
            "label": "OPN 合约交易挑战赛",
            "metric_label": "交易量 (USDT)",
            "activity_end_at": "2026-03-26T23:59:00+08:00",
            "eligible_metric_total": 20000.0,
            "eligible_metric_total_text": "20,000.00",
            "current_floor_value": 500.0,
            "current_floor_value_text": "500.00",
            "rows": [
                {"rank": 1, "name": "r1", "value": 1000.0, "value_text": "1,000.00"},
                {"rank": 5, "name": "r5", "value": 900.0, "value_text": "900.00"},
                {"rank": 20, "name": "r20", "value": 800.0, "value_text": "800.00"},
                {"rank": 50, "name": "r50", "value": 700.0, "value_text": "700.00"},
                {"rank": 200, "name": "r200", "value": 500.0, "value_text": "500.00"},
            ],
            "segments": [
                {"start_rank": 1, "end_rank": 1, "rank_label": "第 1 名", "reward_text": "80,000 OPN", "per_user_reward": 80000.0},
                {"start_rank": 2, "end_rank": 20, "rank_label": "第 2 - 20 名", "reward_text": "平分 120,000 OPN", "per_user_reward": 6315.79},
            ],
            "threshold_value": 500.0,
            "reward_unit": "OPN",
            "rows_truncated": False,
            "last_rank_fetched": 200,
        }
        trend = {
            "board_key": board["board_key"],
            "label": board["label"],
            "metric_label": board["metric_label"],
            "granularity": "daily",
            "rank_targets": [1, 5, 20, 50, 200],
            "points": [
                {
                    "date": "2026-03-23",
                    "eligible_metric_total": 12000.0,
                    "current_floor_value": 350.0,
                    "rank_cutoffs": {
                        "1": {"value": 700.0},
                        "5": {"value": 650.0},
                        "20": {"value": 550.0},
                        "50": {"value": 450.0},
                        "200": {"value": 350.0},
                    },
                },
                {
                    "date": "2026-03-24",
                    "eligible_metric_total": 15000.0,
                    "current_floor_value": 420.0,
                    "rank_cutoffs": {
                        "1": {"value": 820.0},
                        "5": {"value": 760.0},
                        "20": {"value": 660.0},
                        "50": {"value": 540.0},
                        "200": {"value": 420.0},
                    },
                },
                {
                    "date": "2026-03-25",
                    "eligible_metric_total": 20000.0,
                    "current_floor_value": 500.0,
                    "rank_cutoffs": {
                        "1": {"value": 1000.0},
                        "5": {"value": 900.0},
                        "20": {"value": 800.0},
                        "50": {"value": 700.0},
                        "200": {"value": 500.0},
                    },
                },
            ],
        }
        entry = {
            "id": "e1",
            "board_key": board["board_key"],
            "name": "alice",
            "value": 750.0,
            "note": "",
            "updated_at_utc": "2026-03-25T02:00:00+00:00",
        }
        forecast = _forecast_board_with_entry(
            board,
            trend,
            next_day_volume=1200.0,
            entry=entry,
            now=datetime(2026, 3, 25, 12, 0, tzinfo=timezone.utc),
        )
        self.assertEqual(forecast["board_key"], board["board_key"])
        self.assertEqual(forecast["entry"]["name"], "alice")
        self.assertEqual(forecast["entry"]["predicted_value"], 1950.0)
        self.assertEqual(list(forecast["scenarios"].keys()), ["conservative", "base", "aggressive"])
        self.assertGreater(forecast["scenarios"]["base"]["predicted_total"], 20000.0)
        self.assertGreater(
            forecast["scenarios"]["aggressive"]["predicted_total"],
            forecast["scenarios"]["base"]["predicted_total"],
        )
        self.assertLess(
            forecast["scenarios"]["conservative"]["predicted_total"],
            forecast["scenarios"]["base"]["predicted_total"],
        )
        self.assertIn("projected_entry", forecast["scenarios"]["base"])

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

    def test_upsert_updates_existing_entry_by_id(self) -> None:
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
    },
    {
      "board_key": "spot_cfg:默认",
      "label": "CFG 现货交易竞赛",
      "metric_label": "交易量",
      "rows": [
        {"rank": 1, "value": 800.0},
        {"rank": 2, "value": 600.0}
      ],
      "segments": [],
      "threshold_value": 500.0,
      "reward_unit": "CFG"
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
                            "rows": [{"rank": 1, "value": 2000.0}, {"rank": 2, "value": 1000.0}],
                            "segments": [],
                            "threshold_value": 500.0,
                            "reward_unit": "SAHARA",
                        },
                        {
                            "board_key": "spot_cfg:默认",
                            "label": "CFG 现货交易竞赛",
                            "metric_label": "交易量",
                            "rows": [{"rank": 1, "value": 800.0}, {"rank": 2, "value": 600.0}],
                            "segments": [],
                            "threshold_value": 500.0,
                            "reward_unit": "CFG",
                        },
                    ],
                    "markets": {"spot": [], "futures": []},
                    "entries": [],
                    "errors": [],
                }
                result = upsert_competition_entry(
                    {
                        "id": "entry-1",
                        "board_key": "spot_cfg:默认",
                        "name": "TL 2",
                        "value": 750.0,
                        "note": "edited",
                    }
                )
                saved = competition_board._load_entries()
                self.assertEqual(result["entry_id"], "entry-1")
                self.assertEqual(len(saved), 1)
                self.assertEqual(saved[0]["board_key"], "spot_cfg:默认")
                self.assertEqual(saved[0]["name"], "TL 2")
                self.assertEqual(saved[0]["value"], 750.0)
                self.assertEqual(saved[0]["note"], "edited")
                self.assertEqual(len(competition_board._MEMORY_CACHE["data"]["entries"]), 1)
                self.assertEqual(competition_board._MEMORY_CACHE["data"]["entries"][0]["board_key"], "spot_cfg:默认")

    def test_archive_and_load_competition_board_history(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            history_dir = Path(temp_dir) / "history"
            history_index_path = Path(temp_dir) / "history_index.json"
            snapshot = self._sample_snapshot()
            with patch.object(competition_board, "HISTORY_DIR_PATH", history_dir), patch.object(
                competition_board, "HISTORY_INDEX_PATH", history_index_path
            ):
                result = archive_competition_board_history(snapshot, capture_date="2026-03-24")
                self.assertEqual(result["created_board_keys"], ["spot_sahara:默认"])
                history = load_competition_board_history("spot_sahara:默认")
            self.assertEqual(history["selected_date"], "2026-03-24")
            self.assertEqual(history["available_dates"], ["2026-03-24"])
            self.assertEqual(history["history"]["board"]["label"], "SAHARA 现货交易竞赛")

    def test_load_competition_board_trend_groups_latest_record_per_day(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            history_dir = Path(temp_dir) / "history"
            history_index_path = Path(temp_dir) / "history_index.json"
            first_snapshot = self._sample_snapshot()
            second_snapshot = self._sample_snapshot()
            second_snapshot["boards"][0]["updated_text"] = "2026/3/24 14:59:59"
            second_snapshot["boards"][0]["updated_at_utc"] = "2026-03-24T06:59:59+00:00"
            second_snapshot["boards"][0]["eligible_metric_total"] = 23456.0
            second_snapshot["boards"][0]["eligible_metric_total_text"] = "23,456.00"
            second_snapshot["boards"][0]["current_floor_value"] = 888.0
            second_snapshot["boards"][0]["current_floor_value_text"] = "888.00"
            second_snapshot["boards"][0]["rows"] = [
                {"rank": 1, "name": "alice", "value": 5000.0, "value_text": "5,000.00"},
                {"rank": 5, "name": "eve", "value": 4000.0, "value_text": "4,000.00"},
                {"rank": 20, "name": "u20", "value": 3000.0, "value_text": "3,000.00"},
                {"rank": 50, "name": "u50", "value": 2000.0, "value_text": "2,000.00"},
                {"rank": 200, "name": "u200", "value": 1000.0, "value_text": "1,000.00"},
            ]
            third_snapshot = self._sample_snapshot()
            third_snapshot["boards"][0]["updated_text"] = "2026/3/25 13:59:59"
            third_snapshot["boards"][0]["updated_at_utc"] = "2026-03-25T05:59:59+00:00"
            third_snapshot["boards"][0]["eligible_metric_total"] = 34567.0
            third_snapshot["boards"][0]["eligible_metric_total_text"] = "34,567.00"
            third_snapshot["boards"][0]["current_floor_value"] = 999.0
            third_snapshot["boards"][0]["current_floor_value_text"] = "999.00"
            third_snapshot["boards"][0]["rows"] = [
                {"rank": 1, "name": "alice", "value": 6000.0, "value_text": "6,000.00"},
                {"rank": 5, "name": "eve", "value": 4500.0, "value_text": "4,500.00"},
                {"rank": 20, "name": "u20", "value": 3200.0, "value_text": "3,200.00"},
                {"rank": 50, "name": "u50", "value": 2200.0, "value_text": "2,200.00"},
                {"rank": 200, "name": "u200", "value": 1200.0, "value_text": "1,200.00"},
            ]
            with patch.object(competition_board, "HISTORY_DIR_PATH", history_dir), patch.object(
                competition_board, "HISTORY_INDEX_PATH", history_index_path
            ):
                archive_competition_board_history(first_snapshot, capture_date="2026-03-24")
                archive_competition_board_history(second_snapshot, capture_date="2026-03-24 14:00")
                archive_competition_board_history(third_snapshot, capture_date="2026-03-25")
                trend = load_competition_board_trend("spot_sahara:默认")
            self.assertEqual(trend["board_key"], "spot_sahara:默认")
            self.assertEqual(trend["rank_targets"], [1, 5, 20, 50, 200])
            self.assertEqual([item["date"] for item in trend["points"]], ["2026-03-24", "2026-03-25"])
            self.assertEqual(trend["points"][0]["capture_key"], "2026-03-24 14:00")
            self.assertEqual(trend["points"][0]["eligible_metric_total_text"], "23,456.00")
            self.assertEqual(trend["points"][0]["rank_cutoffs"]["20"]["value_text"], "3,000.00")
            self.assertEqual(trend["points"][1]["rank_cutoffs"]["200"]["value_text"], "1,200.00")

    def test_capture_daily_competition_board_history_skips_when_day_already_archived(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            history_dir = Path(temp_dir) / "history"
            history_index_path = Path(temp_dir) / "history_index.json"
            snapshot = self._sample_snapshot()
            with patch.object(competition_board, "HISTORY_DIR_PATH", history_dir), patch.object(
                competition_board, "HISTORY_INDEX_PATH", history_index_path
            ), patch.object(competition_board, "_expected_board_keys", return_value=["spot_sahara:默认"]), patch.object(
                competition_board, "build_competition_board_snapshot", return_value=snapshot
            ) as build_snapshot:
                first = capture_daily_competition_board_history(capture_date="2026-03-24")
                second = capture_daily_competition_board_history(capture_date="2026-03-24")
            self.assertEqual(first["status"], "captured")
            self.assertEqual(first["created_board_keys"], ["spot_sahara:默认"])
            self.assertEqual(second["status"], "skipped_already_captured")
            self.assertEqual(second["created_board_keys"], [])
            self.assertEqual(build_snapshot.call_count, 1)

    def test_capture_due_competition_board_history_uses_hour_slot_from_board_update_time(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            history_dir = Path(temp_dir) / "history"
            history_index_path = Path(temp_dir) / "history_index.json"
            snapshot = self._sample_snapshot()
            with patch.object(competition_board, "HISTORY_DIR_PATH", history_dir), patch.object(
                competition_board, "HISTORY_INDEX_PATH", history_index_path
            ), patch.object(competition_board, "build_competition_board_snapshot", return_value=snapshot):
                result = capture_due_competition_board_history()
                history = load_competition_board_history("spot_sahara:默认")
            self.assertEqual(result["status"], "captured")
            self.assertEqual(result["board_capture_keys"]["spot_sahara:默认"], "2026-03-24 13:00")
            self.assertEqual(history["selected_date"], "2026-03-24 13:00")
            self.assertEqual(history["available_dates"], ["2026-03-24 13:00"])

    def test_capture_due_competition_board_history_skips_same_update_slot(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            history_dir = Path(temp_dir) / "history"
            history_index_path = Path(temp_dir) / "history_index.json"
            snapshot = self._sample_snapshot()
            with patch.object(competition_board, "HISTORY_DIR_PATH", history_dir), patch.object(
                competition_board, "HISTORY_INDEX_PATH", history_index_path
            ), patch.object(competition_board, "build_competition_board_snapshot", return_value=snapshot) as build_snapshot:
                first = capture_due_competition_board_history()
                second = capture_due_competition_board_history()
            self.assertEqual(first["status"], "captured")
            self.assertEqual(second["status"], "skipped_no_new_update_slot")
            self.assertEqual(build_snapshot.call_count, 2)

    def test_build_snapshot_attaches_history_compare_for_list_page(self) -> None:
        now_iso = datetime.now(timezone.utc).isoformat()
        with tempfile.TemporaryDirectory() as temp_dir:
            cache_path = Path(temp_dir) / "competition_board_cache.json"
            entries_path = Path(temp_dir) / "competition_board_entries.json"
            history_dir = Path(temp_dir) / "history"
            history_index_path = Path(temp_dir) / "history_index.json"
            current_snapshot = self._sample_snapshot()
            previous_snapshot = self._sample_snapshot()
            previous_board = previous_snapshot["boards"][0]
            previous_board["current_floor_value"] = 500.0
            previous_board["current_floor_value_text"] = "500.00"
            previous_board["eligible_user_count"] = 80
            previous_board["segments"] = [{"cutoff_value": 500.0, "cutoff_value_text": "500.00"}]
            previous_board["updated_at_utc"] = "2026-03-23T05:59:59+00:00"
            cache_path.write_text(json.dumps(current_snapshot, ensure_ascii=False), encoding="utf-8")
            entries_path.write_text("[]", encoding="utf-8")
            with patch.object(competition_board, "CACHE_PATH", cache_path), patch.object(
                competition_board, "ENTRIES_PATH", entries_path
            ), patch.object(competition_board, "HISTORY_DIR_PATH", history_dir), patch.object(
                competition_board, "HISTORY_INDEX_PATH", history_index_path
            ):
                archive_competition_board_history({"generated_at_utc": now_iso, "boards": [previous_board]}, capture_date="2026-03-23")
                competition_board._MEMORY_CACHE["loaded_at"] = 0.0
                competition_board._MEMORY_CACHE["data"] = None
                snapshot = build_competition_board_snapshot(refresh=False)
            compare = snapshot["boards"][0]["history_compare"]
            self.assertEqual(compare["title"], "上一档 vs 当前")
            self.assertEqual(compare["previous_capture_key"], "2026-03-23")
            self.assertEqual(compare["floor_delta"], 166.0)
            self.assertEqual(compare["eligible_delta"], 20)


if __name__ == "__main__":
    unittest.main()
