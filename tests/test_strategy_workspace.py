from __future__ import annotations

import unittest


class StrategyWorkspaceTests(unittest.TestCase):
    def test_build_strategy_workspace_payload_groups_new_and_legacy_items(self) -> None:
        from grid_optimizer.strategy_workspace import build_strategy_workspace_payload

        payload = build_strategy_workspace_payload(
            {
                "ok": True,
                "ts": "2026-05-08T07:00:00+00:00",
                "view_mode": "cross",
                "summary": {"server_count": 3},
                "servers": [
                    {
                        "ok": True,
                        "server_id": "srv_114",
                        "server_label": "114",
                        "server_base_url": "http://114",
                        "groups": {
                            "running": [
                                {
                                    "symbol": "soonusdt",
                                    "is_running": True,
                                    "strategy_profile": "soon-fast",
                                    "strategy_mode": "synthetic_neutral",
                                    "config": {"grid": "a"},
                                    "target_url": "/running_status?symbol=SOONUSDT",
                                    "total_volume": "120.5",
                                    "recent_hour_volume": 20,
                                    "total_pnl": "7.25",
                                    "trade_pnl": 4,
                                    "unrealized_pnl": 3.25,
                                    "total_fees": "1.5",
                                    "funding_fee": "-0.2",
                                    "open_order_count": "2",
                                    "current_position_display": "long 1",
                                    "updated_at": "2026-05-08T06:59:00+00:00",
                                }
                            ],
                            "saved_idle": [
                                {
                                    "symbol": "chipusdt",
                                    "strategy_name": "chip-grid",
                                    "total_volume": 33,
                                    "fees": 0.3,
                                }
                            ],
                        },
                    },
                    {
                        "ok": True,
                        "label": "111",
                        "url": "http://111",
                        "symbols": [
                            {
                                "symbol": "SOONUSDT",
                                "strategy_profile": "soon-legacy",
                                "is_running": True,
                                "total_volume": 50,
                                "recent_hour_volume": "5.5",
                                "fees": 0.4,
                                "position_summary": "flat",
                            }
                        ],
                    },
                ],
            }
        )

        self.assertTrue(payload["ok"])
        self.assertEqual(payload["ts"], "2026-05-08T07:00:00+00:00")
        self.assertEqual(payload["view_mode"], "cross")
        self.assertEqual(payload["summary"], {"server_count": 3})
        self.assertEqual(payload["symbols"], ["SOONUSDT", "CHIPUSDT"])

        soon_group = payload["symbols_by_symbol"][0]
        self.assertEqual(soon_group["symbol"], "SOONUSDT")
        self.assertEqual(soon_group["status"], "running")
        self.assertEqual(len(soon_group["servers"]), 2)
        self.assertEqual(soon_group["totals"]["total_volume"], 170.5)
        self.assertEqual(soon_group["totals"]["recent_hour_volume"], 25.5)
        self.assertEqual(soon_group["totals"]["total_pnl"], 7.25)
        self.assertEqual(soon_group["totals"]["trade_pnl"], 4.0)
        self.assertEqual(soon_group["totals"]["unrealized_pnl"], 3.25)
        self.assertEqual(soon_group["totals"]["fees"], 1.9)
        self.assertEqual(soon_group["totals"]["funding_fee"], -0.2)
        self.assertEqual(soon_group["totals"]["open_order_count"], 2.0)

        first_cell = soon_group["servers"][0]
        self.assertEqual(first_cell["server_id"], "srv_114")
        self.assertEqual(first_cell["server_label"], "114")
        self.assertEqual(first_cell["server_base_url"], "http://114")
        self.assertEqual(first_cell["status"], "running")
        self.assertEqual(first_cell["position_summary"], "long 1")
        self.assertEqual(first_cell["raw"]["strategy_profile"], "soon-fast")

        chip_group = payload["symbols_by_symbol"][1]
        self.assertEqual(chip_group["symbol"], "CHIPUSDT")
        self.assertEqual(chip_group["status"], "saved_idle")
        self.assertEqual(chip_group["totals"]["total_volume"], 33.0)

    def test_build_strategy_workspace_payload_sorts_statuses_and_ignores_bad_symbols(self) -> None:
        from grid_optimizer.strategy_workspace import build_strategy_workspace_payload

        payload = build_strategy_workspace_payload(
            {
                "ok": True,
                "scope": "cross",
                "servers": [
                    {
                        "ok": True,
                        "server_label": "local",
                        "groups": {
                            "saved_idle": [
                                {"symbol": "bbb", "is_running": False},
                                {"symbol": "   "},
                                {"symbol": None},
                            ]
                        },
                    },
                    {
                        "ok": True,
                        "server_label": "remote",
                        "groups": {"running": [{"symbol": "ccc", "running": True}]},
                    },
                    {
                        "ok": True,
                        "server_label": "idle",
                        "symbols": [{"symbol": "aaa", "is_running": False}],
                    },
                ],
            }
        )

        self.assertEqual(payload["view_mode"], "cross")
        self.assertEqual(
            [(group["symbol"], group["status"]) for group in payload["symbols_by_symbol"]],
            [("CCC", "running"), ("BBB", "saved_idle"), ("AAA", "idle")],
        )
        self.assertEqual(payload["symbols"], ["CCC", "BBB", "AAA"])

    def test_build_strategy_workspace_payload_collects_server_errors(self) -> None:
        from grid_optimizer.strategy_workspace import build_strategy_workspace_payload

        payload = build_strategy_workspace_payload(
            {
                "ok": True,
                "servers": [
                    {
                        "ok": False,
                        "server_label": "broken",
                        "server_base_url": "http://broken",
                        "error": "TimeoutError: timed out",
                    },
                    {
                        "ok": True,
                        "server_label": "healthy",
                        "groups": {"running": [{"symbol": "okusdt", "total_volume": 10}]},
                    },
                ],
            }
        )

        self.assertEqual(
            payload["server_errors"],
            [
                {
                    "server_id": None,
                    "server_label": "broken",
                    "server_base_url": "http://broken",
                    "url": None,
                    "error": "TimeoutError: timed out",
                }
            ],
        )
        self.assertEqual(payload["symbols"], ["OKUSDT"])


if __name__ == "__main__":
    unittest.main()
