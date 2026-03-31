from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from grid_optimizer.console_registry import load_console_registry


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


if __name__ == "__main__":
    unittest.main()
