# Unified Control Console Phase 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a mobile-first `/console` entry that models Binance API key accounts explicitly, aggregates per-account node summaries on the server, and redirects `/` plus `/hub` into the new control console.

**Architecture:** Keep the existing Python HTTP server and route table, but split new control-console behavior into focused modules for registry loading, overview aggregation, and HTML rendering. `web.py` remains the entrypoint and route glue only; all new console behavior lives behind pure helper functions so it can be tested with the repository's existing `pytest + unittest` style.

**Tech Stack:** Python 3.10+, stdlib `http.server`, `json`, `pathlib`, `urllib`, `unittest`, `pytest`, and `requests`

---

## File Structure

**Create:**

- `config/console_registry.json`
- `src/grid_optimizer/console_registry.py`
- `src/grid_optimizer/console_overview.py`
- `src/grid_optimizer/console_page.py`
- `tests/test_console.py`

**Modify:**

- `src/grid_optimizer/web.py`

**Responsibilities:**

- `config/console_registry.json`: non-sensitive server/account metadata and default account priority
- `src/grid_optimizer/console_registry.py`: load, validate, normalize, and serialize registry data
- `src/grid_optimizer/console_overview.py`: fetch remote node summaries, build deep links, and assemble degradable `/api/console/overview` payloads
- `src/grid_optimizer/console_page.py`: render the mobile-first `/console` page and the small client-side fetch/render logic
- `src/grid_optimizer/web.py`: redirect `/` and `/hub`, expose `/console`, `/api/console/registry`, and `/api/console/overview`
- `tests/test_console.py`: focused tests for registry parsing, overview aggregation, page rendering, and route glue

### Task 1: Add Registry Config And Loader

**Files:**

- Create: `config/console_registry.json`
- Create: `src/grid_optimizer/console_registry.py`
- Test: `tests/test_console.py`

- [ ] **Step 1: Write the failing registry tests**

```python
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
```

- [ ] **Step 2: Run registry tests to verify they fail**

Run: `PYTHONPATH=src pytest tests/test_console.py -k registry -v`

Expected: FAIL with `ModuleNotFoundError: No module named 'grid_optimizer.console_registry'`

- [ ] **Step 3: Write the minimal registry implementation and initial config**

```python
# src/grid_optimizer/console_registry.py
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

DEFAULT_CONSOLE_REGISTRY_PATH = Path("config/console_registry.json")
ALLOWED_ACCOUNT_KINDS = {"futures", "spot", "mixed"}


def load_console_registry(path: Path | None = None) -> dict[str, Any]:
    raw = json.loads((path or DEFAULT_CONSOLE_REGISTRY_PATH).read_text(encoding="utf-8"))
    servers = [_normalize_server(item) for item in raw.get("servers", [])]
    servers_by_id = {server["id"]: server for server in servers}
    accounts = [_normalize_account(item, servers_by_id) for item in raw.get("accounts", [])]
    default_account = _select_default_account(accounts)
    competition_source = _normalize_competition_source(raw.get("competition_source"), servers_by_id)
    return {
        "servers": servers,
        "servers_by_id": servers_by_id,
        "accounts": accounts,
        "accounts_by_id": {account["id"]: account for account in accounts},
        "default_account": default_account,
        "competition_source": competition_source,
    }


def serialize_console_registry(registry: dict[str, Any]) -> dict[str, Any]:
    return {
        "servers": registry["servers"],
        "accounts": registry["accounts"],
        "default_account_id": registry["default_account"]["id"],
        "competition_source": registry["competition_source"],
    }
```

```json
// config/console_registry.json
{
  "servers": [
    {
      "id": "srv_150",
      "label": "主入口 A",
      "base_url": "http://43.131.232.150:8788",
      "location": "Singapore",
      "enabled": true,
      "capabilities": ["futures_monitor", "competition_board", "strategies"],
      "notes": "统一入口首选节点"
    },
    {
      "id": "srv_114",
      "label": "主入口 B",
      "base_url": "http://43.155.163.114:8788",
      "location": "Singapore",
      "enabled": true,
      "capabilities": ["futures_monitor", "spot_runner", "spot_strategies", "strategies"]
    },
    {
      "id": "srv_111",
      "label": "在线入口",
      "base_url": "http://43.155.136.111:8787",
      "location": "Oracle",
      "enabled": true,
      "capabilities": ["futures_monitor", "competition_board", "strategies"]
    }
  ],
  "accounts": [
    {
      "id": "acct_main_a",
      "label": "主账号 A",
      "server_id": "srv_150",
      "kind": "futures",
      "priority": 100,
      "enabled": true,
      "default_symbols": ["BARDUSDT", "XAUTUSDT"],
      "competition_symbols": ["KAT", "NIGHT", "BARD", "XAUT"],
      "pages": ["/monitor", "/strategies", "/competition_board"],
      "notes": "默认首页账号"
    }
  ],
  "competition_source": {
    "server_id": "srv_150",
    "path": "/api/competition_board"
  }
}
```

- [ ] **Step 4: Run registry tests to verify they pass**

Run: `PYTHONPATH=src pytest tests/test_console.py -k registry -v`

Expected: PASS for both registry tests

- [ ] **Step 5: Commit the registry foundation**

```bash
git add config/console_registry.json src/grid_optimizer/console_registry.py tests/test_console.py
git commit -m "feat: add console registry loader"
```

### Task 2: Add Overview Aggregation And Degradation Logic

**Files:**

- Create: `src/grid_optimizer/console_overview.py`
- Modify: `tests/test_console.py`

- [ ] **Step 1: Write the failing overview tests**

```python
from unittest.mock import patch

from grid_optimizer.console_overview import build_console_overview


class ConsoleOverviewTests(unittest.TestCase):
    def _registry(self) -> dict[str, object]:
        return {
            "servers": [
                {
                    "id": "srv_150",
                    "label": "A",
                    "base_url": "http://node-a:8788",
                    "enabled": True,
                    "capabilities": ["futures_monitor", "competition_board", "strategies"],
                }
            ],
            "servers_by_id": {
                "srv_150": {
                    "id": "srv_150",
                    "label": "A",
                    "base_url": "http://node-a:8788",
                    "enabled": True,
                    "capabilities": ["futures_monitor", "competition_board", "strategies"],
                }
            },
            "accounts": [
                {
                    "id": "acct_main_a",
                    "label": "主账号 A",
                    "server_id": "srv_150",
                    "kind": "futures",
                    "priority": 100,
                    "enabled": True,
                    "default_symbols": ["BARDUSDT"],
                    "competition_symbols": ["KAT", "BARD"],
                    "pages": ["/monitor", "/strategies", "/competition_board"],
                }
            ],
            "accounts_by_id": {},
            "default_account": {
                "id": "acct_main_a",
                "label": "主账号 A",
                "server_id": "srv_150",
                "kind": "futures",
                "priority": 100,
                "enabled": True,
                "default_symbols": ["BARDUSDT"],
                "competition_symbols": ["KAT", "BARD"],
                "pages": ["/monitor", "/strategies", "/competition_board"],
            },
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
                        {"symbol": "KAT", "market": "spot", "label": "KAT 活动"},
                        {"symbol": "ENSO", "market": "futures", "label": "ENSO 活动"}
                    ]
                }
            },
        ]

        registry = self._registry()
        registry["accounts_by_id"] = {"acct_main_a": registry["default_account"]}

        overview = build_console_overview(registry, "acct_main_a")

        self.assertTrue(overview["ok"])
        self.assertEqual(overview["account"]["id"], "acct_main_a")
        self.assertEqual(overview["links"]["monitor"], "http://node-a:8788/monitor")
        self.assertEqual(len(overview["competitions"]), 1)
        self.assertEqual(overview["competitions"][0]["symbol"], "KAT")

    @patch("grid_optimizer.console_overview._fetch_remote_json")
    def test_build_console_overview_degrades_when_health_fails(self, mock_fetch_remote_json) -> None:
        mock_fetch_remote_json.side_effect = RuntimeError("health timeout")
        registry = self._registry()
        registry["accounts_by_id"] = {"acct_main_a": registry["default_account"]}

        overview = build_console_overview(registry, "acct_main_a")

        self.assertTrue(overview["ok"])
        self.assertEqual(overview["health"]["status"], "offline")
        self.assertIn("health timeout", overview["warnings"][0])
        self.assertEqual(overview["links"]["competition_board"], "http://node-a:8788/competition_board")
```

- [ ] **Step 2: Run overview tests to verify they fail**

Run: `PYTHONPATH=src pytest tests/test_console.py -k overview -v`

Expected: FAIL with `ModuleNotFoundError: No module named 'grid_optimizer.console_overview'`

- [ ] **Step 3: Write the minimal overview aggregator**

```python
# src/grid_optimizer/console_overview.py
from __future__ import annotations

import base64
import os
import time
from typing import Any

import requests


def build_console_overview(registry: dict[str, Any], account_id: str) -> dict[str, Any]:
    account = registry["accounts_by_id"][account_id]
    server = registry["servers_by_id"][account["server_id"]]
    warnings: list[str] = []
    health = _fetch_health(server, warnings)
    futures = _fetch_futures_cards(server, account, warnings)
    spot = _fetch_spot_cards(server, account, warnings)
    competitions = _fetch_competitions(registry, account, warnings)
    links = build_console_links(server, account)
    return {
        "ok": True,
        "account": account,
        "server": server,
        "health": health,
        "summary": _build_summary(account, health, futures, spot, warnings),
        "futures": futures,
        "spot": spot,
        "competitions": competitions,
        "links": links,
        "warnings": warnings,
        "fetched_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
    }


def build_console_links(server: dict[str, Any], account: dict[str, Any]) -> dict[str, str]:
    base_url = server["base_url"].rstrip("/")
    page_map = {
        "/monitor": "monitor",
        "/spot_runner": "spot_runner",
        "/strategies": "strategies",
        "/spot_strategies": "spot_strategies",
        "/competition_board": "competition_board",
        "/basis": "basis",
    }
    return {
        page_map[path]: f"{base_url}{path}"
        for path in account.get("pages", [])
        if path in page_map
    }
```

```python
def _fetch_remote_json(url: str, server_id: str) -> dict[str, Any]:
    username = os.environ[f"GRID_NODE_{server_id.upper()}_USERNAME"]
    password = os.environ[f"GRID_NODE_{server_id.upper()}_PASSWORD"]
    token = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")
    response = requests.get(
        url,
        headers={"Authorization": f"Basic {token}"},
        timeout=4,
    )
    response.raise_for_status()
    return response.json()
```

- [ ] **Step 4: Run overview tests to verify they pass**

Run: `PYTHONPATH=src pytest tests/test_console.py -k overview -v`

Expected: PASS for the overview tests

- [ ] **Step 5: Commit the overview aggregation layer**

```bash
git add src/grid_optimizer/console_overview.py tests/test_console.py
git commit -m "feat: add console overview aggregation"
```

### Task 3: Add The Mobile-First Console Page

**Files:**

- Create: `src/grid_optimizer/console_page.py`
- Modify: `tests/test_console.py`

- [ ] **Step 1: Write the failing page-render tests**

```python
from grid_optimizer.console_page import build_console_page


class ConsolePageTests(unittest.TestCase):
    def test_build_console_page_contains_console_api_bootstrap(self) -> None:
        html = build_console_page()
        self.assertIn("/api/console/registry", html)
        self.assertIn("/api/console/overview", html)
        self.assertIn("account-sheet", html)

    def test_build_console_page_contains_mobile_sections(self) -> None:
        html = build_console_page()
        self.assertIn("Hero Summary", html)
        self.assertIn("Competition", html)
        self.assertIn("Runtime", html)
        self.assertIn("Legacy Entries", html)
        self.assertIn("Server", html)
```

- [ ] **Step 2: Run page-render tests to verify they fail**

Run: `PYTHONPATH=src pytest tests/test_console.py -k "page or render" -v`

Expected: FAIL with `ModuleNotFoundError: No module named 'grid_optimizer.console_page'`

- [ ] **Step 3: Write the minimal console page renderer**

```python
# src/grid_optimizer/console_page.py
from __future__ import annotations


def build_console_page() -> str:
    return """<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>统一控制台</title>
  <style>
    :root {
      --bg: #f4efe6;
      --panel: rgba(255, 252, 247, 0.96);
      --line: #d8cfc0;
      --text: #1f1b16;
      --muted: #6d6458;
      --brand: #9a3412;
      --good: #166534;
      --warn: #b45309;
      --bad: #b91c1c;
    }
    body { margin: 0; font-family: "Avenir Next", "PingFang SC", sans-serif; background: linear-gradient(180deg, #fbf7f0 0%, #efe6d8 100%); color: var(--text); }
    .console-shell { max-width: 640px; margin: 0 auto; padding: 16px; display: grid; gap: 14px; }
    .context-bar { position: sticky; top: 0; z-index: 10; background: rgba(244, 239, 230, 0.92); backdrop-filter: blur(14px); }
    .card { border: 1px solid var(--line); border-radius: 18px; background: var(--panel); padding: 14px; box-shadow: 0 14px 30px rgba(48, 37, 24, 0.08); }
    .metric-grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 10px; }
    .link-grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 10px; }
    .account-sheet { position: fixed; inset: auto 0 0 0; display: none; }
    .account-sheet[data-open="true"] { display: block; }
  </style>
</head>
<body>
  <main class="console-shell">
    <section class="context-bar card">
      <div id="current-account">Loading...</div>
      <button id="open-account-sheet" type="button">切换账号</button>
      <button id="refresh-overview" type="button">刷新</button>
    </section>
    <section class="card"><h2>Hero Summary</h2><div id="hero-summary" class="metric-grid"></div></section>
    <section class="card"><h2>Competition</h2><div id="competition-list"></div></section>
    <section class="card"><h2>Runtime</h2><div id="runtime-list"></div></section>
    <section class="card"><h2>Legacy Entries</h2><div id="legacy-links" class="link-grid"></div></section>
    <section class="card"><h2>Server</h2><div id="server-meta"></div></section>
  </main>
  <section id="account-sheet" class="account-sheet" data-open="false"></section>
  <script>
    async function loadRegistry() {
      const response = await fetch('/api/console/registry');
      return response.json();
    }

    async function loadOverview(accountId) {
      const response = await fetch(`/api/console/overview?account_id=${encodeURIComponent(accountId)}`);
      return response.json();
    }
  </script>
</body>
</html>"""
```

- [ ] **Step 4: Run page-render tests to verify they pass**

Run: `PYTHONPATH=src pytest tests/test_console.py -k "page or render" -v`

Expected: PASS for both page-render tests

- [ ] **Step 5: Commit the console UI shell**

```bash
git add src/grid_optimizer/console_page.py tests/test_console.py
git commit -m "feat: add console page shell"
```

### Task 4: Wire `/console` Routes And Redirect `/` Plus `/hub`

**Files:**

- Modify: `src/grid_optimizer/web.py`
- Modify: `tests/test_console.py`
- Test: `tests/test_web_security.py`

- [ ] **Step 1: Write the failing route-glue tests**

```python
from grid_optimizer.web import (
    _console_redirect_target,
    _console_registry_payload,
    _console_overview_payload,
)


class ConsoleRouteGlueTests(unittest.TestCase):
    def test_console_redirect_target_handles_root_and_hub(self) -> None:
        self.assertEqual(_console_redirect_target("/"), "/console")
        self.assertEqual(_console_redirect_target("/hub"), "/console")
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

    @patch("grid_optimizer.web.load_console_registry")
    @patch("grid_optimizer.web.build_console_overview")
    def test_console_overview_payload_requires_account_id(self, mock_build_console_overview, mock_load_console_registry) -> None:
        payload, status = _console_overview_payload({})
        self.assertFalse(payload["ok"])
        self.assertEqual(status, 400)
        mock_build_console_overview.assert_not_called()
```

- [ ] **Step 2: Run the route-glue tests to verify they fail**

Run: `PYTHONPATH=src pytest tests/test_console.py -k "route or glue" -v`

Expected: FAIL with `ImportError` for the new helper functions in `grid_optimizer.web`

- [ ] **Step 3: Add the route glue in `web.py`**

```python
# src/grid_optimizer/web.py
from .console_overview import build_console_overview
from .console_page import build_console_page
from .console_registry import load_console_registry, serialize_console_registry
```

```python
def _console_redirect_target(path: str) -> str | None:
    if path in {"/", "/index.html", "/hub", "/hub.html", "/portal", "/portal.html"}:
        return "/console"
    return None


def _console_registry_payload() -> dict[str, Any]:
    registry = load_console_registry()
    payload = serialize_console_registry(registry)
    return {"ok": True, **payload}


def _console_overview_payload(query: dict[str, list[str]]) -> tuple[dict[str, Any], int]:
    account_id = str(query.get("account_id", [""])[0]).strip()
    if not account_id:
        return {"ok": False, "error": "account_id is required"}, 400
    registry = load_console_registry()
    return build_console_overview(registry, account_id), 200
```

```python
class _Handler(BaseHTTPRequestHandler):
    def _send_redirect(self, location: str, status: int = HTTPStatus.FOUND) -> None:
        self.send_response(status)
        self.send_header("Location", location)
        for key, value in _security_headers().items():
            self.send_header(key, value)
        self.end_headers()

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path
        query = parse_qs(parsed.query)
        redirect_target = _console_redirect_target(path)
        if redirect_target is not None:
            self._send_redirect(redirect_target)
            return
        if not self._authorize_request():
            return
        if path in {"/console", "/console.html"}:
            self._send_html(build_console_page(), status=HTTPStatus.OK)
            return
        if path == "/api/console/registry":
            self._send_json(_console_registry_payload(), status=HTTPStatus.OK)
            return
        if path == "/api/console/overview":
            payload, status = _console_overview_payload(query)
            self._send_json(payload, status=status)
            return

    def do_HEAD(self) -> None:
        redirect_target = _console_redirect_target(urlparse(self.path).path)
        if redirect_target is not None:
            self._send_redirect(redirect_target)
            return
        # Mirror the new /console and /api/console/* branches here as well.
```

- [ ] **Step 4: Run the new route tests and the existing web security tests**

Run: `PYTHONPATH=src pytest tests/test_console.py tests/test_web_security.py -k "route or glue" -v`

Expected: PASS, including confirmation that `/` and `/hub` now redirect to `/console`

- [ ] **Step 5: Commit the route integration**

```bash
git add src/grid_optimizer/web.py tests/test_console.py tests/test_web_security.py
git commit -m "feat: wire console routes into web server"
```

### Task 5: Verify End-To-End Behavior And Lock The Baseline

**Files:**

- Modify: `tests/test_console.py`
- Modify: `src/grid_optimizer/console_page.py`
- Modify: `src/grid_optimizer/console_overview.py`

- [ ] **Step 1: Add the final focused verification tests**

```python
class ConsoleIntegrationShapeTests(unittest.TestCase):
    @patch("grid_optimizer.console_overview._fetch_remote_json")
    def test_build_console_overview_preserves_links_when_runtime_calls_fail(self, mock_fetch_remote_json) -> None:
        mock_fetch_remote_json.side_effect = [RuntimeError("down"), RuntimeError("down"), RuntimeError("down")]
        registry = ConsoleOverviewTests()._registry()
        registry["accounts_by_id"] = {"acct_main_a": registry["default_account"]}

        overview = build_console_overview(registry, "acct_main_a")

        self.assertEqual(overview["health"]["status"], "offline")
        self.assertEqual(overview["links"]["monitor"], "http://node-a:8788/monitor")
        self.assertGreaterEqual(len(overview["warnings"]), 1)

    def test_build_console_page_uses_mobile_first_layout_primitives(self) -> None:
        html = build_console_page()
        self.assertIn("metric-grid", html)
        self.assertIn("link-grid", html)
        self.assertIn("position: sticky", html)
```

- [ ] **Step 2: Run the focused console suite**

Run: `PYTHONPATH=src pytest tests/test_console.py -v`

Expected: PASS for registry, overview, page-render, route-glue, and final shape tests

- [ ] **Step 3: Run the full repository test suite**

Run: `PYTHONPATH=src pytest -q`

Expected: PASS for the existing repository tests plus the new console coverage

- [ ] **Step 4: Verify the app boots and serves the console entry**

Run: `PYTHONPATH=src python -m grid_optimizer.web --host 127.0.0.1 --port 8788`

Expected: server starts without import errors; opening `http://127.0.0.1:8788/` redirects to `/console`

- [ ] **Step 5: Commit the verified Phase 1 baseline**

```bash
git add src/grid_optimizer/console_page.py src/grid_optimizer/console_overview.py tests/test_console.py
git commit -m "test: verify unified control console phase1 baseline"
```
