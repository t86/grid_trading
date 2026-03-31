from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from grid_optimizer.console_registry import load_console_registry
from grid_optimizer.console_overview import build_console_overview


class ConsoleRegistryTests(unittest.TestCase):
    def test_load_console_registry_selects_highest_priority_enabled_account(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            registry_path = Path(temp_dir) / "console_registry.json"
            registry_path.write_text(
                json.dumps(
                    {
                        "servers": [
                            {
                                "id": "srv_150",
                                "label": "A",
                                "base_url": "http://127.0.0.1:8788",
                                "enabled": True,
                                "capabilities": ["futures_monitor"],
                            }
                        ],
                        "accounts": [
                            {
                                "id": "acct_low",
                                "label": "Low",
                                "server_id": "srv_150",
                                "kind": "futures",
                                "priority": 10,
                                "enabled": True,
                                "default_symbols": ["BARDUSDT"],
                                "competition_symbols": ["BARD"],
                                "pages": ["/monitor"],
                            },
                            {
                                "id": "acct_high",
                                "label": "High",
                                "server_id": "srv_150",
                                "kind": "futures",
                                "priority": 100,
                                "enabled": True,
                                "default_symbols": ["XAUTUSDT"],
                                "competition_symbols": ["XAUT"],
                                "pages": ["/monitor"],
                            },
                        ],
                        "competition_source": {"server_id": "srv_150", "path": "/api/competition_board"},
                    }
                ),
                encoding="utf-8",
            )

            registry = load_console_registry(registry_path)

        self.assertEqual(registry["default_account"]["id"], "acct_high")
        self.assertEqual(registry["accounts_by_id"]["acct_high"]["server_id"], "srv_150")

    def test_load_console_registry_skips_enabled_account_on_disabled_server(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            registry_path = Path(temp_dir) / "console_registry.json"
            registry_path.write_text(
                json.dumps(
                    {
                        "servers": [
                            {
                                "id": "srv_disabled",
                                "label": "Disabled",
                                "base_url": "http://127.0.0.1:8788",
                                "enabled": False,
                                "capabilities": ["futures_monitor"],
                            },
                            {
                                "id": "srv_enabled",
                                "label": "Enabled",
                                "base_url": "http://127.0.0.1:8789",
                                "enabled": True,
                                "capabilities": ["futures_monitor"],
                            },
                        ],
                        "accounts": [
                            {
                                "id": "acct_disabled_server",
                                "label": "Disabled server",
                                "server_id": "srv_disabled",
                                "kind": "futures",
                                "priority": 100,
                                "enabled": True,
                                "default_symbols": ["BARDUSDT"],
                                "competition_symbols": ["BARD"],
                                "pages": ["/monitor"],
                            },
                            {
                                "id": "acct_enabled_server",
                                "label": "Enabled server",
                                "server_id": "srv_enabled",
                                "kind": "futures",
                                "priority": 10,
                                "enabled": True,
                                "default_symbols": ["XAUTUSDT"],
                                "competition_symbols": ["XAUT"],
                                "pages": ["/monitor"],
                            },
                        ],
                        "competition_source": {"server_id": "srv_enabled", "path": "/api/competition_board"},
                    }
                ),
                encoding="utf-8",
            )

            registry = load_console_registry(registry_path)

        self.assertEqual(registry["default_account"]["id"], "acct_enabled_server")

    def test_load_console_registry_rejects_non_boolean_enabled_values(self) -> None:
        cases = [
            (
                "server",
                {
                    "servers": [
                        {
                            "id": "srv_bad",
                            "label": "Bad server",
                            "base_url": "http://127.0.0.1:8788",
                            "enabled": "false",
                            "capabilities": ["futures_monitor"],
                        }
                    ],
                    "accounts": [
                        {
                            "id": "acct_main",
                            "label": "Main",
                            "server_id": "srv_bad",
                            "kind": "futures",
                            "priority": 1,
                            "enabled": True,
                            "default_symbols": ["BARDUSDT"],
                            "competition_symbols": ["BARD"],
                            "pages": ["/monitor"],
                        }
                    ],
                    "competition_source": {"server_id": "srv_bad", "path": "/api/competition_board"},
                },
            ),
            (
                "account",
                {
                    "servers": [
                        {
                            "id": "srv_bad",
                            "label": "Good server",
                            "base_url": "http://127.0.0.1:8788",
                            "enabled": True,
                            "capabilities": ["futures_monitor"],
                        }
                    ],
                    "accounts": [
                        {
                            "id": "acct_main",
                            "label": "Main",
                            "server_id": "srv_bad",
                            "kind": "futures",
                            "priority": 1,
                            "enabled": 1,
                            "default_symbols": ["BARDUSDT"],
                            "competition_symbols": ["BARD"],
                            "pages": ["/monitor"],
                        }
                    ],
                    "competition_source": {"server_id": "srv_bad", "path": "/api/competition_board"},
                },
            ),
        ]

        for field_name, payload in cases:
            with self.subTest(field_name=field_name), tempfile.TemporaryDirectory() as temp_dir:
                registry_path = Path(temp_dir) / "console_registry.json"
                registry_path.write_text(json.dumps(payload), encoding="utf-8")

                with self.assertRaisesRegex(ValueError, "must include a boolean enabled"):
                    load_console_registry(registry_path)

    def test_load_console_registry_rejects_unknown_account_server(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            registry_path = Path(temp_dir) / "console_registry.json"
            registry_path.write_text(
                json.dumps(
                    {
                        "servers": [],
                        "accounts": [
                            {
                                "id": "acct_bad",
                                "label": "Bad",
                                "server_id": "srv_missing",
                                "kind": "futures",
                                "priority": 1,
                                "enabled": True,
                                "default_symbols": ["BARDUSDT"],
                                "competition_symbols": ["BARD"],
                                "pages": ["/monitor"],
                            }
                        ],
                        "competition_source": {"server_id": "srv_missing", "path": "/api/competition_board"},
                    }
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "Unknown server_id"):
                load_console_registry(registry_path)

    def test_load_console_registry_rejects_all_disabled_accounts(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            registry_path = Path(temp_dir) / "console_registry.json"
            registry_path.write_text(
                json.dumps(
                    {
                        "servers": [
                            {
                                "id": "srv_150",
                                "label": "A",
                                "base_url": "http://127.0.0.1:8788",
                                "enabled": True,
                                "capabilities": ["futures_monitor"],
                            }
                        ],
                        "accounts": [
                            {
                                "id": "acct_low",
                                "label": "Low",
                                "server_id": "srv_150",
                                "kind": "futures",
                                "priority": 10,
                                "enabled": False,
                                "default_symbols": ["BARDUSDT"],
                                "competition_symbols": ["BARD"],
                                "pages": ["/monitor"],
                            }
                        ],
                        "competition_source": {"server_id": "srv_150", "path": "/api/competition_board"},
                    }
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "No enabled accounts available"):
                load_console_registry(registry_path)

    def test_load_console_registry_rejects_duplicate_server_or_account_ids(self) -> None:
        cases = [
            (
                "server",
                {
                    "servers": [
                        {
                            "id": "srv_150",
                            "label": "A",
                            "base_url": "http://127.0.0.1:8788",
                            "enabled": True,
                            "capabilities": ["futures_monitor"],
                        },
                        {
                            "id": "srv_150",
                            "label": "B",
                            "base_url": "http://127.0.0.1:8789",
                            "enabled": True,
                            "capabilities": ["futures_monitor"],
                        },
                    ],
                    "accounts": [
                        {
                            "id": "acct_main",
                            "label": "Main",
                            "server_id": "srv_150",
                            "kind": "futures",
                            "priority": 1,
                            "enabled": True,
                            "default_symbols": ["BARDUSDT"],
                            "competition_symbols": ["BARD"],
                            "pages": ["/monitor"],
                        }
                    ],
                    "competition_source": {"server_id": "srv_150", "path": "/api/competition_board"},
                },
                "Duplicate server_id",
            ),
            (
                "account",
                {
                    "servers": [
                        {
                            "id": "srv_150",
                            "label": "A",
                            "base_url": "http://127.0.0.1:8788",
                            "enabled": True,
                            "capabilities": ["futures_monitor"],
                        }
                    ],
                    "accounts": [
                        {
                            "id": "acct_main",
                            "label": "Main A",
                            "server_id": "srv_150",
                            "kind": "futures",
                            "priority": 1,
                            "enabled": True,
                            "default_symbols": ["BARDUSDT"],
                            "competition_symbols": ["BARD"],
                            "pages": ["/monitor"],
                        },
                        {
                            "id": "acct_main",
                            "label": "Main B",
                            "server_id": "srv_150",
                            "kind": "futures",
                            "priority": 2,
                            "enabled": True,
                            "default_symbols": ["XAUTUSDT"],
                            "competition_symbols": ["XAUT"],
                            "pages": ["/monitor"],
                        },
                    ],
                    "competition_source": {"server_id": "srv_150", "path": "/api/competition_board"},
                },
                "Duplicate account id",
            ),
        ]

        for field_name, payload, message in cases:
            with self.subTest(field_name=field_name), tempfile.TemporaryDirectory() as temp_dir:
                registry_path = Path(temp_dir) / "console_registry.json"
                registry_path.write_text(json.dumps(payload), encoding="utf-8")

                with self.assertRaisesRegex(ValueError, message):
                    load_console_registry(registry_path)


class ConsoleOverviewTests(unittest.TestCase):
    def _registry(self) -> dict[str, object]:
        account = {
            "id": "acct_main_a",
            "label": "Main",
            "server_id": "srv_150",
            "kind": "futures",
            "priority": 100,
            "enabled": True,
            "default_symbols": ["BARDUSDT"],
            "competition_symbols": ["KAT", "BARD"],
            "pages": ["/monitor", "/strategies", "/competition_board", "/spot_runner", "/spot_strategies", "/basis"],
        }
        server = {
            "id": "srv_150",
            "label": "A",
            "base_url": "http://node-a:8788",
            "enabled": True,
            "capabilities": ["futures_monitor", "competition_board", "strategies", "spot_runner", "spot_strategies", "basis"],
        }
        return {
            "servers": [server],
            "servers_by_id": {"srv_150": server},
            "accounts": [account],
            "accounts_by_id": {"acct_main_a": account},
            "default_account": account,
            "competition_source": {"server_id": "srv_150", "path": "/api/competition_board"},
        }

    @patch("grid_optimizer.console_overview._fetch_remote_json")
    def test_build_console_overview_returns_links_and_competitions(self, mock_fetch_remote_json) -> None:
        mock_fetch_remote_json.side_effect = [
            {"ok": True},
            {"ok": True, "symbol": "BARDUSDT", "snapshot": {"runner_status": "running", "open_orders": [1, 2]}},
            {
                "ok": True,
                "snapshot": {
                    "boards": [
                        {"symbol": "KAT", "market": "spot", "label": "KAT activity"},
                        {"symbol": "ENSO", "market": "futures", "label": "ENSO activity"},
                    ]
                },
            },
        ]

        overview = build_console_overview(self._registry(), "acct_main_a")

        self.assertTrue(overview["ok"])
        self.assertEqual(overview["account"]["id"], "acct_main_a")
        self.assertEqual(overview["links"]["monitor"], "http://node-a:8788/monitor")
        self.assertEqual(overview["links"]["competition_board"], "http://node-a:8788/competition_board")
        self.assertEqual(len(overview["competitions"]), 1)
        self.assertEqual(overview["competitions"][0]["symbol"], "KAT")

    @patch("grid_optimizer.console_overview._fetch_remote_json")
    def test_build_console_overview_degrades_when_health_fails(self, mock_fetch_remote_json) -> None:
        mock_fetch_remote_json.side_effect = RuntimeError("health timeout")

        overview = build_console_overview(self._registry(), "acct_main_a")

        self.assertTrue(overview["ok"])
        self.assertEqual(overview["health"]["status"], "offline")
        self.assertEqual(overview["links"]["competition_board"], "http://node-a:8788/competition_board")
        self.assertGreaterEqual(len(overview["warnings"]), 1)
        self.assertIn("health timeout", overview["warnings"][0])

    @patch("grid_optimizer.console_overview._fetch_remote_json")
    def test_build_console_overview_skips_market_fetches_when_pages_do_not_imply_them(self, mock_fetch_remote_json) -> None:
        account = dict(self._registry()["accounts_by_id"]["acct_main_a"])
        account["pages"] = ["/competition_board"]
        registry = self._registry()
        registry["accounts"] = [account]
        registry["accounts_by_id"] = {"acct_main_a": account}
        registry["default_account"] = account
        mock_fetch_remote_json.side_effect = [
            {"ok": True},
            {"ok": True, "snapshot": {"boards": []}},
        ]

        overview = build_console_overview(registry, "acct_main_a")

        self.assertEqual(overview["futures"], [])
        self.assertEqual(overview["spot"], [])
        self.assertEqual(mock_fetch_remote_json.call_count, 2)

    @patch("grid_optimizer.console_overview._fetch_remote_json")
    def test_build_console_overview_keeps_successful_market_snapshots_after_later_failure(self, mock_fetch_remote_json) -> None:
        account = dict(self._registry()["accounts_by_id"]["acct_main_a"])
        account["default_symbols"] = ["BARDUSDT", "XAUTUSDT"]
        account["competition_symbols"] = []
        registry = self._registry()
        registry["accounts"] = [account]
        registry["accounts_by_id"] = {"acct_main_a": account}
        registry["default_account"] = account
        mock_fetch_remote_json.side_effect = [
            {"ok": True},
            {"ok": True, "snapshot": {"runner_status": "running", "open_orders": [1]}},
            RuntimeError("second symbol failed"),
        ]

        overview = build_console_overview(registry, "acct_main_a")

        self.assertEqual([item["symbol"] for item in overview["futures"]], ["BARDUSDT"])
        self.assertGreaterEqual(len(overview["warnings"]), 1)
        self.assertIn("second symbol failed", overview["warnings"][0])

    @patch("grid_optimizer.console_overview._fetch_remote_json")
    def test_build_console_overview_skips_competitions_fetch_when_symbols_empty(self, mock_fetch_remote_json) -> None:
        account = dict(self._registry()["accounts_by_id"]["acct_main_a"])
        account["pages"] = ["/competition_board"]
        account["competition_symbols"] = []
        registry = self._registry()
        registry["accounts"] = [account]
        registry["accounts_by_id"] = {"acct_main_a": account}
        registry["default_account"] = account
        mock_fetch_remote_json.side_effect = [{"ok": True}]

        overview = build_console_overview(registry, "acct_main_a")

        self.assertEqual(overview["competitions"], [])
        self.assertEqual(overview["warnings"], [])
        self.assertEqual(mock_fetch_remote_json.call_count, 1)


if __name__ == "__main__":
    unittest.main()
