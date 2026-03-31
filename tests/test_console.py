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


if __name__ == "__main__":
    unittest.main()
