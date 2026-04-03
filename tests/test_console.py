from __future__ import annotations

import json
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

import requests

from grid_optimizer.console_registry import load_console_registry
from grid_optimizer.console_overview import (
    _clear_console_overview_cache,
    _fetch_remote_json,
    build_console_overview,
)
from grid_optimizer.console_page import build_console_page
from grid_optimizer.web import (
    _console_overview_payload,
    _console_redirect_target,
    _console_registry_payload,
)


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
    def setUp(self) -> None:
        _clear_console_overview_cache()

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
                        {
                            "symbol": "KAT",
                            "market": "spot",
                            "label": "KAT activity",
                            "url": "https://example.test/kat",
                            "activity_end_at": "2026-04-02T09:00:00+08:00",
                            "current_floor_value_text": "1044.96",
                            "top_rows": [1, 2, 3],
                        },
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
        self.assertEqual(overview["competitions"][0]["url"], "https://example.test/kat")
        self.assertEqual(overview["competitions"][0]["current_floor"], "1044.96")
        self.assertNotIn("board", overview["competitions"][0])

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

    @patch("grid_optimizer.console_overview._fetch_competitions")
    @patch("grid_optimizer.console_overview._fetch_market_overview")
    @patch("grid_optimizer.console_overview._fetch_health")
    def test_build_console_overview_fetches_sections_concurrently(
        self,
        mock_fetch_health,
        mock_fetch_market_overview,
        mock_fetch_competitions,
    ) -> None:
        def slow_health(server, warnings):
            time.sleep(0.2)
            return {"ok": True, "status": "online", "error": None}

        def slow_market(server, account, warnings, *, market):
            time.sleep(0.2)
            return [{"symbol": f"{market}-item", "ok": True, "status": "online", "snapshot": {}}]

        def slow_competitions(registry, account, warnings):
            time.sleep(0.2)
            return [{"symbol": "KAT", "market": "spot", "label": "KAT activity"}]

        mock_fetch_health.side_effect = slow_health
        mock_fetch_market_overview.side_effect = slow_market
        mock_fetch_competitions.side_effect = slow_competitions

        started = time.perf_counter()
        overview = build_console_overview(self._registry(), "acct_main_a")
        elapsed = time.perf_counter() - started

        self.assertTrue(overview["ok"])
        self.assertLess(elapsed, 0.45)

    @patch("grid_optimizer.console_overview._fetch_competitions")
    @patch("grid_optimizer.console_overview._fetch_market_overview")
    @patch("grid_optimizer.console_overview._fetch_health")
    def test_build_console_overview_uses_short_ttl_cache(
        self,
        mock_fetch_health,
        mock_fetch_market_overview,
        mock_fetch_competitions,
    ) -> None:
        mock_fetch_health.return_value = {"ok": True, "status": "online", "error": None}
        mock_fetch_market_overview.side_effect = [
            [{"symbol": "BARDUSDT", "ok": True, "status": "running", "snapshot": {}}],
            [],
        ]
        mock_fetch_competitions.return_value = [{"symbol": "KAT", "market": "spot", "label": "KAT activity"}]

        with patch("grid_optimizer.console_overview.time.monotonic", side_effect=[100.0, 100.1, 100.5]):
            first = build_console_overview(self._registry(), "acct_main_a")
            second = build_console_overview(self._registry(), "acct_main_a")

        self.assertTrue(first["ok"])
        self.assertTrue(second["ok"])
        self.assertEqual(mock_fetch_health.call_count, 1)
        self.assertEqual(mock_fetch_market_overview.call_count, 2)
        self.assertEqual(mock_fetch_competitions.call_count, 1)
        self.assertEqual(first["summary"], second["summary"])
        self.assertEqual(first["competitions"], second["competitions"])


class ConsoleRemoteFetchTests(unittest.TestCase):
    def setUp(self) -> None:
        _clear_console_overview_cache()

    @patch.dict(
        "os.environ",
        {
            "GRID_NODE_SRV_150_USERNAME": "grid",
            "GRID_NODE_SRV_150_PASSWORD": "secret",
        },
        clear=False,
    )
    @patch("grid_optimizer.console_overview.requests.get")
    def test_fetch_remote_json_retries_single_read_timeout(self, mock_get) -> None:
        response = unittest.mock.Mock()
        response.json.return_value = {"ok": True}
        response.raise_for_status.return_value = None
        mock_get.side_effect = [
            requests.ReadTimeout("first timeout"),
            response,
        ]

        payload = _fetch_remote_json(
            {
                "id": "srv_150",
                "base_url": "http://node-a:8788",
            },
            "/api/health",
        )

        self.assertEqual(payload, {"ok": True})
        self.assertEqual(mock_get.call_count, 2)


class ConsolePageTests(unittest.TestCase):
    def test_build_console_page_contains_interaction_shell_controls(self) -> None:
        html = build_console_page()

        self.assertIn('id="open-account-sheet"', html)
        self.assertIn('id="refresh-console"', html)
        self.assertIn('id="account-sheet-backdrop"', html)
        self.assertIn('id="account-list"', html)
        self.assertIn('id="quick-actions-panel"', html)
        self.assertIn('id="competition-panel"', html)
        self.assertIn('id="runtime-panel"', html)
        self.assertIn('id="overview-panel"', html)

    def test_build_console_page_wires_registry_default_account_into_overview_request(self) -> None:
        html = build_console_page()

        self.assertIn("/api/console/registry", html)
        self.assertIn("default_account_id", html)
        self.assertIn("registry.default_account.id", html)
        self.assertIn('/api/console/overview?account_id=', html)

    def test_build_console_page_exposes_fixed_bottom_sheet_account_shell(self) -> None:
        html = build_console_page()

        self.assertIn('class="account-sheet-shell"', html)
        self.assertIn('id="account-sheet-shell"', html)
        self.assertIn("position: fixed", html)
        self.assertIn("padding: 88px 16px 160px", html)
        self.assertIn('id="account-sheet"', html)
        self.assertIn('role="dialog"', html)
        self.assertIn('aria-label="Account picker"', html)

    def test_build_console_page_contains_required_section_anchors(self) -> None:
        html = build_console_page()

        self.assertIn('id="overview"', html)
        self.assertIn('id="competition"', html)
        self.assertIn('id="runtime"', html)
        self.assertIn('id="quick-actions"', html)
        self.assertIn('id="runtime-shell"', html)
        self.assertIn("Overview", html)
        self.assertIn("Competition", html)
        self.assertIn("Runtime", html)
        self.assertIn("Quick Actions", html)

    def test_build_console_page_contains_account_switching_and_render_helpers(self) -> None:
        html = build_console_page()

        self.assertIn("function openAccountSheet()", html)
        self.assertIn("function closeAccountSheet()", html)
        self.assertIn("function renderAccountList(", html)
        self.assertIn("function renderOverviewPanel(", html)
        self.assertIn("function renderRuntimeDesk(", html)
        self.assertIn("function renderQuickActions(", html)
        self.assertIn("function renderOverview(", html)
        self.assertIn("async function loadOverview(accountId)", html)
        self.assertIn("Refreshing…", html)
        self.assertIn("Show more", html)
        self.assertIn("current_floor", html)
        self.assertIn("activity_end_at", html)
        self.assertIn('"/api/console/overview?account_id=" + encodeURIComponent(accountId)', html)
        self.assertIn('document.getElementById("refresh-console")', html)


class ConsoleRouteGlueTests(unittest.TestCase):
    def test_console_redirect_target_handles_root_and_hub(self) -> None:
        self.assertEqual(_console_redirect_target("/"), "/console")
        self.assertEqual(_console_redirect_target("/index.html"), "/console")
        self.assertEqual(_console_redirect_target("/hub"), "/console")
        self.assertEqual(_console_redirect_target("/portal"), "/console")
        self.assertIsNone(_console_redirect_target("/monitor"))

    @patch("grid_optimizer.web.load_console_registry")
    def test_console_registry_payload_serializes_registry(self, mock_load_console_registry) -> None:
        mock_load_console_registry.return_value = {
            "servers": [],
            "accounts": [],
            "default_account": {"id": "acct_main_a"},
            "competition_source": {"server_id": "srv_150", "path": "/api/competition_board"},
        }

        payload = _console_registry_payload()

        self.assertTrue(payload["ok"])
        self.assertEqual(payload["default_account_id"], "acct_main_a")

    @patch("grid_optimizer.web.build_console_overview")
    @patch("grid_optimizer.web.load_console_registry")
    def test_console_overview_payload_requires_account_id(self, mock_load_console_registry, mock_build_console_overview) -> None:
        payload, status = _console_overview_payload({})

        self.assertFalse(payload["ok"])
        self.assertEqual(status, 400)
        mock_load_console_registry.assert_not_called()
        mock_build_console_overview.assert_not_called()


if __name__ == "__main__":
    unittest.main()
