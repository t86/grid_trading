from __future__ import annotations

import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from grid_optimizer import master_sprint
from grid_optimizer.master_sprint import SprintBoardConfig


class MasterSprintTests(unittest.TestCase):
    def test_cached_snapshot_due_waits_until_15_cst(self) -> None:
        now = datetime.fromisoformat("2026-05-03T14:30:00+08:00")
        cached = {"refreshed_at_utc": "2026-05-02T07:05:00+00:00"}

        self.assertFalse(master_sprint._cached_snapshot_due(cached, now))

    def test_cached_snapshot_due_after_15_cst_requires_same_day_refresh(self) -> None:
        now = datetime.fromisoformat("2026-05-03T15:30:00+08:00")
        cached = {"refreshed_at_utc": "2026-05-02T07:05:00+00:00"}

        self.assertTrue(master_sprint._cached_snapshot_due(cached, now))

    def test_build_snapshot_payload_aggregates_official_and_local_metrics(self) -> None:
        config = SprintBoardConfig(
            slug="um_week2",
            label="UM 大师赛",
            phase_label="第 2 期",
            resource_id=52057,
            referer="https://example.com",
            start_at="2026-04-28T08:00:00+08:00",
            end_at="2026-05-05T07:59:00+08:00",
            entry_threshold=500.0,
            threshold_unit="USDT",
            leaderboard_unit="USDT",
            symbols=("ETHUSDC", "BTCUSDC"),
            reward_pools=(
                {
                    "label": "UM 周奖池",
                    "value": 330.0,
                    "unit": "BNB",
                },
            ),
        )
        fake_official = {
            "resource_id": 52057,
            "rows_fetched": 500,
            "total_ranked_users": 12000,
            "top500_total_volume": 5432000000.0,
            "rank_500_value": 1200000.0,
            "updated_at_utc": "2026-05-03T06:58:00+00:00",
            "updated_at_cst": "2026-05-03 14:58",
        }
        symbol_metrics = [
            {
                "symbol": "ETHUSDC",
                "volume": 320.0,
                "trade_count": 18,
                "last_trade_time_utc": "2026-05-01T03:00:00+00:00",
                "last_trade_time_cst": "2026-05-01 11:00",
            },
            {
                "symbol": "BTCUSDC",
                "volume": 480.0,
                "trade_count": 12,
                "last_trade_time_utc": "2026-05-02T04:00:00+00:00",
                "last_trade_time_cst": "2026-05-02 12:00",
            },
        ]
        with patch.object(master_sprint, "SPRINT_BOARD_CONFIGS", (config,)), patch.object(
            master_sprint, "_fetch_board_rows", return_value=fake_official
        ), patch.object(master_sprint, "_local_symbol_metrics", side_effect=symbol_metrics), patch.object(
            master_sprint, "_fetch_symbol_close_price_usdt", return_value=600.0
        ):
            payload = master_sprint._build_snapshot_payload(datetime(2026, 5, 3, 8, 10, tzinfo=timezone.utc))

        self.assertEqual(payload["competition_count"], 1)
        self.assertEqual(payload["official_success_count"], 1)
        competition = payload["competitions"][0]
        self.assertEqual(competition["official"]["top500_total_volume"], 5432000000.0)
        self.assertEqual(competition["official"]["rank_500_value"], 1200000.0)
        self.assertEqual(competition["local"]["total_volume"], 800.0)
        self.assertEqual(competition["local"]["trade_count"], 30)
        self.assertEqual(competition["local"]["distance_to_entry_threshold"], 300.0)
        self.assertEqual(competition["local"]["last_trade_time_cst"], "2026-05-02 12:00")
        self.assertEqual(competition["reward_estimates"][0]["reward_value_usdt"], 198000.0)
        self.assertAlmostEqual(competition["reward_estimates"][0]["reward_per_10k_volume_usdt"], 0.364507, places=6)

    def test_master_sprint_page_points_to_api(self) -> None:
        self.assertIn("/api/master_sprint_board", master_sprint.MASTER_SPRINT_PAGE)
        self.assertIn("大师赛追踪看板", master_sprint.MASTER_SPRINT_PAGE)
        self.assertIn("每万 U 预估奖励", master_sprint.MASTER_SPRINT_PAGE)


if __name__ == "__main__":
    unittest.main()
