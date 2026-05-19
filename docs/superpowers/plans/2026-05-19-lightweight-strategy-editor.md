# Lightweight Strategy Editor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a standalone `/strategy_editor` page that edits runner strategy parameters without auto-refreshing or loading heavy monitor data.

**Architecture:** Keep the first implementation inside `src/grid_optimizer/web.py` to match the existing page pattern. Add one lightweight status builder that reads runner config/process/local runtime files only, then add a static editor page that only fetches data when buttons are clicked. Tests lock down that the page has no timer and the API does not call the full monitor snapshot.

**Tech Stack:** Python stdlib HTTP server, existing `web.py` helpers, inline HTML/JS, pytest/unittest.

---

### Task 1: Lightweight Status API

**Files:**
- Modify: `src/grid_optimizer/web.py`
- Test: `tests/test_web_security.py`

- [ ] **Step 1: Write failing tests**

Add tests that import `_build_strategy_editor_status`, patch `build_monitor_snapshot` to raise if called, and assert the response shape contains `runner`, `position`, `orders`, `latest_loop`, and `runner.config`.

Run:

```bash
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 PYTHONPATH=src python -m pytest tests/test_web_security.py::WebSecurityTests::test_strategy_editor_status_uses_lightweight_local_sources -q
```

Expected: fail because `_build_strategy_editor_status` does not exist.

- [ ] **Step 2: Implement minimal status builder**

Add `_build_strategy_editor_status(symbol)` near the running status helpers. It should:

- normalize symbol to uppercase
- read `runner = _read_runner_process_for_symbol(symbol)`
- read `config = _load_runner_control_config(symbol)`
- read one latest event from `summary_jsonl`
- read plan/submit files only through `_read_json_dict`
- compute a small position summary from the latest plan
- compute strategy open-order count from submit placed orders plus plan kept orders
- return partial data if files are missing

- [ ] **Step 3: Add GET route**

In `_Handler.do_GET`, add:

```python
if path == "/api/strategy_editor/status":
    symbol = str(query.get("symbol", [""])[0]).upper().strip()
    if not symbol:
        self._send_json({"ok": False, "error": "symbol is required"}, status=400)
        return
    try:
        payload = _build_strategy_editor_status(symbol)
    except Exception as exc:
        self._send_json({"ok": False, "error": f"{type(exc).__name__}: {exc}"}, status=500)
        return
    self._send_json(payload, status=HTTPStatus.OK)
    return
```

- [ ] **Step 4: Verify targeted tests pass**

Run:

```bash
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 PYTHONPATH=src python -m pytest tests/test_web_security.py::WebSecurityTests::test_strategy_editor_status_uses_lightweight_local_sources -q
```

Expected: pass.

### Task 2: Standalone Editor Page

**Files:**
- Modify: `src/grid_optimizer/web.py`
- Test: `tests/test_web_security.py`

- [ ] **Step 1: Write failing page tests**

Add tests that assert:

- `STRATEGY_EDITOR_PAGE` exists
- it contains `策略参数编辑器`
- it contains `刷新状态`, `载入当前参数`, `载入预设参数`, `保存参数`
- it does not contain `setInterval(`
- it does not contain `loadMonitor()`
- it uses `/api/strategy_editor/status`
- it saves through `/api/runner/save`
- it does not call `/api/runner/start`

Run:

```bash
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 PYTHONPATH=src python -m pytest tests/test_web_security.py::WebSecurityTests::test_strategy_editor_page_is_manual_and_lightweight -q
```

Expected: fail because `STRATEGY_EDITOR_PAGE` does not exist.

- [ ] **Step 2: Implement `STRATEGY_EDITOR_PAGE`**

Add an HTML string in `web.py` with:

- static header and safety note
- symbol select plus manual symbol input
- preset select
- status panel
- parameter form for the common runner fields already used by the monitor editor
- advanced JSON textarea
- explanation panel
- JS functions for manual refresh, load current config, load preset config, and save

Do not add timers or automatic status fetch on page load.

- [ ] **Step 3: Add page route**

In `_Handler.do_GET`, add:

```python
if path in {"/strategy_editor", "/strategy_editor.html"}:
    self._send_html(STRATEGY_EDITOR_PAGE, status=HTTPStatus.OK)
    return
```

- [ ] **Step 4: Verify targeted page tests pass**

Run:

```bash
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 PYTHONPATH=src python -m pytest tests/test_web_security.py::WebSecurityTests::test_strategy_editor_page_is_manual_and_lightweight -q
```

Expected: pass.

### Task 3: Handler and Save Safety Tests

**Files:**
- Modify: `tests/test_web_security.py`
- Modify: `src/grid_optimizer/web.py`

- [ ] **Step 1: Write route tests**

Add tests using `_Handler`'s existing route surface or page constant assertions to confirm `/strategy_editor` is registered in `do_GET` and `/api/strategy_editor/status` is registered without calling heavy monitor helpers.

- [ ] **Step 2: Write save safety test**

Add a page-level assertion that `STRATEGY_EDITOR_PAGE` contains `/api/runner/save` and does not contain `/api/runner/start` or `/api/runner/stop`.

- [ ] **Step 3: Run web tests**

Run:

```bash
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 PYTHONPATH=src python -m pytest tests/test_web_security.py -q
```

Expected: pass.

### Task 4: Browser Verification

**Files:**
- Runtime only

- [ ] **Step 1: Start or reuse local web server**

Run the app locally if no server is active:

```bash
PYTHONPATH=src python -m grid_optimizer.web --host 127.0.0.1 --port 8787
```

- [ ] **Step 2: Open `/strategy_editor` in the in-app browser**

Verify visually that the page loads, has no auto-refresh timer behavior, and shows the manual status and editor controls.

- [ ] **Step 3: Click manual buttons where safe**

Use a harmless symbol and verify:

- `刷新状态` updates only the status panel
- `载入预设参数` fills editor JSON
- no automatic status request happens after waiting

### Task 5: Final Verification

**Files:**
- All touched files

- [ ] **Step 1: Run relevant tests**

```bash
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 PYTHONPATH=src python -m pytest tests/test_strategy_profile_schema.py tests/test_competition_elastic_volume.py tests/test_loop_runner.py tests/test_web_security.py -q
```

- [ ] **Step 2: Run whitespace check**

```bash
git diff --check
```

- [ ] **Step 3: Review status**

```bash
git status --short --untracked-files=all
```
