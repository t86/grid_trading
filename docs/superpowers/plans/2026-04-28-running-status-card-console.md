# Running Status Card Console Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reclaim `/running_status` into the repo and turn it into a cross-server card console with local drawer editing, grouped by running and saved-idle futures runners.

**Architecture:** Keep the existing Python single-process web app, add a new `/api/running_status` backend with `scope=local` and `scope=cross`, and render a new `/running_status` page that reuses shared runner-form metadata from `/monitor`. Cross-server view only summarizes and links out; local view owns inline editing and control actions.

**Tech Stack:** Python stdlib HTTP server in `src/grid_optimizer/web.py`, existing monitor snapshot helpers in `src/grid_optimizer/monitor.py`, existing console registry helpers in `src/grid_optimizer/console_registry.py` and `src/grid_optimizer/console_overview.py`, vanilla HTML/CSS/JS, `pytest`.

---

## File Structure

- Modify: `src/grid_optimizer/web.py`
  - Add shared runner editor schema/html helpers usable by `/monitor` and `/running_status`
  - Add `/api/running_status` local/cross handlers
  - Add `/running_status` page route and page template
- Create: `tests/test_running_status.py`
  - Cover local grouping, cross aggregation shape, and control-file scanning
- Modify: `tests/test_web_security.py`
  - Cover page route, API route auth/shape, and query-param behavior
- Reuse: `src/grid_optimizer/monitor.py`
  - Consume `build_monitor_snapshot(...)` instead of duplicating risk/open-order logic
- Reuse: `src/grid_optimizer/console_registry.py`
  - Consume registered server list for cross-server fetch fanout

### Task 1: Add backend tests for local running-status grouping

**Files:**
- Create: `tests/test_running_status.py`
- Modify: `src/grid_optimizer/web.py`

- [ ] **Step 1: Write the failing test for grouping running vs saved-idle symbols**

```python
from pathlib import Path

from grid_optimizer.web import _build_running_status_local_payload


def test_build_running_status_local_payload_groups_running_and_saved_idle(tmp_path, monkeypatch):
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    (output_dir / "soonusdt_loop_runner_control.json").write_text('{"symbol":"SOONUSDT","strategy_profile":"a"}', encoding="utf-8")
    (output_dir / "chipusdt_loop_runner_control.json").write_text('{"symbol":"CHIPUSDT","strategy_profile":"b"}', encoding="utf-8")

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        "grid_optimizer.web._read_runner_process_for_symbol",
        lambda symbol: {
            "configured": True,
            "is_running": symbol == "SOONUSDT",
            "pid": 11 if symbol == "SOONUSDT" else None,
            "config": {"symbol": symbol, "strategy_profile": "a" if symbol == "SOONUSDT" else "b"},
        },
    )
    monkeypatch.setattr(
        "grid_optimizer.web.build_monitor_snapshot",
        lambda **kwargs: {
            "ok": True,
            "symbol": kwargs["symbol"],
            "runner": {
                "configured": True,
                "is_running": kwargs["symbol"] == "SOONUSDT",
                "pid": 11 if kwargs["symbol"] == "SOONUSDT" else None,
                "config": {"symbol": kwargs["symbol"], "strategy_profile": "a" if kwargs["symbol"] == "SOONUSDT" else "b"},
            },
            "risk_controls": {"strategy_mode": "synthetic_neutral"},
            "market": {"mid_price": 1.0},
            "position": {"position_amt": 0.0, "long_qty": 0.0, "short_qty": 0.0, "unrealized_pnl": 0.0},
            "open_orders": [],
            "trade_summary": {"gross_notional": 0.0, "realized_pnl": 0.0, "net_pnl_estimate": 0.0, "commission": 0.0},
            "income_summary": {"funding_fee": 0.0},
            "warnings": [],
        },
    )

    payload = _build_running_status_local_payload()

    assert [item["symbol"] for item in payload["groups"]["running"]] == ["SOONUSDT"]
    assert [item["symbol"] for item in payload["groups"]["saved_idle"]] == ["CHIPUSDT"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTHONPATH=src PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 pytest tests/test_running_status.py::test_build_running_status_local_payload_groups_running_and_saved_idle -q`

Expected: FAIL with `ImportError` or `AttributeError` because `_build_running_status_local_payload` does not exist yet.

- [ ] **Step 3: Write minimal implementation for local payload grouping**

Implement in `src/grid_optimizer/web.py`:

```python
def _iter_futures_runner_control_symbols(output_dir: Path | None = None) -> list[str]:
    ...


def _build_running_status_local_payload(symbol: str | None = None) -> dict[str, Any]:
    ...
```

Requirements:
- Scan `output/*_loop_runner_control.json`
- Read each symbol config
- Build one card per symbol
- Put running symbols only in `groups["running"]`
- Put non-running symbols only in `groups["saved_idle"]`
- Sort cards by symbol for deterministic tests

- [ ] **Step 4: Run test to verify it passes**

Run: `PYTHONPATH=src PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 pytest tests/test_running_status.py::test_build_running_status_local_payload_groups_running_and_saved_idle -q`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add tests/test_running_status.py src/grid_optimizer/web.py
git commit -m "test: add local running status grouping coverage"
```

### Task 2: Add local payload shape and focused-symbol tests

**Files:**
- Modify: `tests/test_running_status.py`
- Modify: `src/grid_optimizer/web.py`

- [ ] **Step 1: Write the failing test for focused-symbol expansion metadata**

```python
from grid_optimizer.web import _build_running_status_local_payload


def test_build_running_status_local_payload_marks_focused_symbol(tmp_path, monkeypatch):
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    (output_dir / "chipusdt_loop_runner_control.json").write_text('{"symbol":"CHIPUSDT","strategy_profile":"b"}', encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        "grid_optimizer.web._read_runner_process_for_symbol",
        lambda symbol: {"configured": True, "is_running": False, "pid": None, "config": {"symbol": symbol, "strategy_profile": "b"}},
    )
    monkeypatch.setattr(
        "grid_optimizer.web.build_monitor_snapshot",
        lambda **kwargs: {
            "ok": True,
            "symbol": kwargs["symbol"],
            "runner": {"configured": True, "is_running": False, "pid": None, "config": {"symbol": kwargs["symbol"], "strategy_profile": "b"}},
            "risk_controls": {"strategy_mode": "synthetic_neutral"},
            "market": {"mid_price": 1.0},
            "position": {"position_amt": 0.0, "long_qty": 0.0, "short_qty": 0.0, "unrealized_pnl": 0.0},
            "open_orders": [],
            "trade_summary": {"gross_notional": 0.0, "realized_pnl": 0.0, "net_pnl_estimate": 0.0, "commission": 0.0},
            "income_summary": {"funding_fee": 0.0},
            "warnings": [],
        },
    )

    payload = _build_running_status_local_payload(symbol="CHIPUSDT")

    assert payload["view_mode"] == "local"
    assert payload["focused_symbol"] == "CHIPUSDT"
    assert payload["groups"]["saved_idle"][0]["focused"] is True
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTHONPATH=src PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 pytest tests/test_running_status.py::test_build_running_status_local_payload_marks_focused_symbol -q`

Expected: FAIL because focused-symbol metadata is missing.

- [ ] **Step 3: Extend local payload shape**

Add to `src/grid_optimizer/web.py`:

```python
payload["view_mode"] = "local"
payload["focused_symbol"] = normalized_symbol_or_none
card["focused"] = card["symbol"] == normalized_symbol_or_none
```

Also include:
- `server_id`
- `server_label`
- `server_base_url`
- card-level `target_url`

- [ ] **Step 4: Run tests to verify they pass**

Run: `PYTHONPATH=src PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 pytest tests/test_running_status.py -q`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add tests/test_running_status.py src/grid_optimizer/web.py
git commit -m "test: cover focused local running status cards"
```

### Task 3: Add cross-server aggregation tests and backend implementation

**Files:**
- Modify: `tests/test_running_status.py`
- Modify: `src/grid_optimizer/web.py`

- [ ] **Step 1: Write the failing test for cross payload aggregation**

```python
from grid_optimizer.web import _build_running_status_cross_payload


def test_build_running_status_cross_payload_aggregates_servers(monkeypatch):
    monkeypatch.setattr(
        "grid_optimizer.web.load_console_registry",
        lambda: {
            "servers": [
                {"id": "srv_114", "label": "114", "base_url": "http://114", "enabled": True, "capabilities": []},
                {"id": "srv_111", "label": "111", "base_url": "http://111", "enabled": True, "capabilities": []},
            ],
            "accounts": [],
            "competition_source": {"server_id": "srv_114", "path": "/api/competition_board"},
        },
    )
    monkeypatch.setattr(
        "grid_optimizer.web._local_running_status_server_id",
        lambda: "srv_114",
    )
    monkeypatch.setattr(
        "grid_optimizer.web._build_running_status_local_payload",
        lambda symbol=None: {
            "ok": True,
            "view_mode": "local",
            "server_id": "srv_114",
            "server_label": "114",
            "server_base_url": "http://114",
            "groups": {"running": [{"symbol": "SOONUSDT"}], "saved_idle": [{"symbol": "BTCUSDC"}]},
            "summary": {"running_symbol_count": 1, "saved_idle_symbol_count": 1, "running_total_volume": 100.0, "recent_hour_volume": 10.0, "total_pnl": 3.0},
        },
    )
    monkeypatch.setattr(
        "grid_optimizer.web._fetch_remote_running_status_local_payload",
        lambda server: {
            "ok": True,
            "view_mode": "local",
            "server_id": "srv_111",
            "server_label": "111",
            "server_base_url": "http://111",
            "groups": {"running": [{"symbol": "CHIPUSDT"}], "saved_idle": []},
            "summary": {"running_symbol_count": 1, "saved_idle_symbol_count": 0, "running_total_volume": 50.0, "recent_hour_volume": 5.0, "total_pnl": -1.0},
        },
    )

    payload = _build_running_status_cross_payload()

    assert payload["scope"] == "cross"
    assert payload["summary"]["running_symbol_count"] == 2
    assert payload["summary"]["saved_idle_symbol_count"] == 1
    assert len(payload["servers"]) == 2
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTHONPATH=src PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 pytest tests/test_running_status.py::test_build_running_status_cross_payload_aggregates_servers -q`

Expected: FAIL because `_build_running_status_cross_payload` does not exist yet.

- [ ] **Step 3: Implement cross aggregation**

Implement in `src/grid_optimizer/web.py`:

```python
def _local_running_status_server_id() -> str | None:
    ...


def _fetch_remote_running_status_local_payload(server: dict[str, Any]) -> dict[str, Any]:
    ...


def _build_running_status_cross_payload() -> dict[str, Any]:
    ...
```

Requirements:
- Read enabled servers from `load_console_registry()`
- Use local payload builder for the local server
- Fetch `GET <base_url>/api/running_status?scope=local` for remote servers
- Aggregate summary counts and running-only financial totals
- Preserve per-server errors without failing whole response

- [ ] **Step 4: Run tests to verify they pass**

Run: `PYTHONPATH=src PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 pytest tests/test_running_status.py -q`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add tests/test_running_status.py src/grid_optimizer/web.py
git commit -m "feat: add cross-server running status aggregation"
```

### Task 4: Add API route coverage before implementing handlers

**Files:**
- Modify: `tests/test_web_security.py`
- Modify: `src/grid_optimizer/web.py`

- [ ] **Step 1: Write the failing API route tests**

```python
def test_api_running_status_local_returns_ok(client):
    response = client.get("/api/running_status?scope=local")
    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    assert payload["scope"] == "local"


def test_running_status_page_serves_html(client):
    response = client.get("/running_status")
    assert response.status_code == 200
    assert "text/html" in response.headers["Content-Type"]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `PYTHONPATH=src PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 pytest tests/test_web_security.py::test_api_running_status_local_returns_ok tests/test_web_security.py::test_running_status_page_serves_html -q`

Expected: FAIL with 404 or missing route assertions.

- [ ] **Step 3: Implement API and page routing**

Add to `src/grid_optimizer/web.py`:

```python
if path == "/running_status":
    ...

if path == "/api/running_status":
    ...
```

Behavior:
- `scope=local` -> `_build_running_status_local_payload(...)`
- `scope=cross` -> `_build_running_status_cross_payload()`
- default page route serves the new HTML page

- [ ] **Step 4: Run tests to verify they pass**

Run: `PYTHONPATH=src PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 pytest tests/test_web_security.py::test_api_running_status_local_returns_ok tests/test_web_security.py::test_running_status_page_serves_html -q`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add tests/test_web_security.py src/grid_optimizer/web.py
git commit -m "feat: add running status routes"
```

### Task 5: Extract shared runner-editor metadata from `/monitor`

**Files:**
- Modify: `src/grid_optimizer/web.py`
- Test: `tests/test_web_security.py`

- [ ] **Step 1: Write the failing test for shared runner editor markers**

```python
def test_running_status_page_contains_shared_runner_editor_markers(client):
    response = client.get("/running_status?symbol=SOONUSDT")
    text = response.get_data(as_text=True)
    assert "runner_field_strategy_mode" in text
    assert "高级 JSON" in text
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTHONPATH=src PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 pytest tests/test_web_security.py::test_running_status_page_contains_shared_runner_editor_markers -q`

Expected: FAIL because the page does not yet include the editor.

- [ ] **Step 3: Extract reusable HTML/JS fragments**

In `src/grid_optimizer/web.py`:
- Move runner editor field HTML into a shared string builder
- Move `RUNNER_FORM_FIELDS` and related mode lists into a shared JS fragment inserted into both `/monitor` and `/running_status`

Minimum shared pieces:
- form field ids
- mode visibility rules
- form<->JSON sync helpers

- [ ] **Step 4: Run test to verify it passes**

Run: `PYTHONPATH=src PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 pytest tests/test_web_security.py::test_running_status_page_contains_shared_runner_editor_markers -q`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/grid_optimizer/web.py tests/test_web_security.py
git commit -m "refactor: share runner editor fragments"
```

### Task 6: Build the `/running_status` frontend views and drawer

**Files:**
- Modify: `src/grid_optimizer/web.py`
- Test: `tests/test_web_security.py`

- [ ] **Step 1: Write the failing page-content tests**

```python
def test_running_status_page_contains_running_and_saved_sections(client):
    response = client.get("/running_status")
    text = response.get_data(as_text=True)
    assert "运行中" in text
    assert "已保存未启动" in text
    assert "保存并重启" in text
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTHONPATH=src PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 pytest tests/test_web_security.py::test_running_status_page_contains_running_and_saved_sections -q`

Expected: FAIL because the page is still missing the new console structure.

- [ ] **Step 3: Implement the page UI**

In `src/grid_optimizer/web.py`, implement `RUNNING_STATUS_PAGE` with:
- cross/local view detection from URL params
- grouped card grids
- right drawer for local cards
- remote card links to `base_url + /running_status?symbol=...`
- local action buttons:
  - running cards: `保存` / `保存并重启` / `停止`
  - saved-idle cards: `保存` / `启动`

Use existing endpoints:
- `/api/runner/save`
- `/api/runner/start`
- `/api/runner/stop`

- [ ] **Step 4: Run focused tests to verify they pass**

Run: `PYTHONPATH=src PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 pytest tests/test_running_status.py tests/test_web_security.py -q`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/grid_optimizer/web.py tests/test_running_status.py tests/test_web_security.py
git commit -m "feat: add running status card console UI"
```

### Task 7: Run full verification and prepare the branch

**Files:**
- Modify: none expected
- Test: `tests/test_running_status.py`
- Test: `tests/test_web_security.py`

- [ ] **Step 1: Run focused regression suite**

Run: `PYTHONPATH=src PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 pytest tests/test_running_status.py tests/test_web_security.py -q`

Expected: PASS

- [ ] **Step 2: Run syntax verification**

Run: `PYTHONPATH=src python -m py_compile src/grid_optimizer/web.py src/grid_optimizer/monitor.py`

Expected: no output

- [ ] **Step 3: Inspect git diff**

Run: `git status --short && git diff -- src/grid_optimizer/web.py tests/test_running_status.py tests/test_web_security.py docs/superpowers/plans/2026-04-28-running-status-card-console.md`

Expected: only intended files changed

- [ ] **Step 4: Commit remaining changes**

```bash
git add src/grid_optimizer/web.py tests/test_running_status.py tests/test_web_security.py docs/superpowers/plans/2026-04-28-running-status-card-console.md
git commit -m "feat: implement running status card console"
```
