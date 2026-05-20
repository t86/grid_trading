# Profile Boundary Overlay Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add report-only profile-level parameter boundary diagnostics on top of the existing family-level strategy schema.

**Architecture:** Keep family schema enforcement unchanged, then add a profile overlay registry that narrows and annotates important profiles for diagnostics. The overlay report is passed through `startup_preflight`, consumed by strategy diagnostics, and rendered by the existing manual-refresh strategy editor.

**Tech Stack:** Python dataclasses and pytest/unittest tests in `src/grid_optimizer`; existing HTML/JS string rendering in `src/grid_optimizer/web.py`.

---

### Task 1: Schema Report Red Test

**Files:**
- Modify: `/Volumes/WORK/binance/wangge/.worktrees/strategy-workspace-controller/tests/test_strategy_profile_schema.py`
- Modify later: `/Volumes/WORK/binance/wangge/.worktrees/strategy-workspace-controller/src/grid_optimizer/strategy_profile_schema.py`

- [ ] **Step 1: Write failing schema tests**

Add tests that call `apply_strategy_profile_schema()` for `aigensynusdt_best_quote_maker_volume_v1`, `competition_inventory_grid_v1`, and an unknown profile. Assert the new `profile_boundary` object reports `overlay_known`, `active_allowed_params`, `active_global_safety_params`, `forbidden_active_params`, `required_missing_params`, and `status`.

- [ ] **Step 2: Run red tests**

Run:

```bash
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 PYTHONPATH=src python -m pytest tests/test_strategy_profile_schema.py::StrategyProfileSchemaTests::test_aigensyn_best_quote_profile_boundary_reports_active_allowed_and_global_safety_params tests/test_strategy_profile_schema.py::StrategyProfileSchemaTests::test_profile_boundary_reports_forbidden_active_params_without_changing_effective_args tests/test_strategy_profile_schema.py::StrategyProfileSchemaTests::test_profile_boundary_reports_required_missing_params tests/test_strategy_profile_schema.py::StrategyProfileSchemaTests::test_competition_inventory_grid_profile_has_known_overlay tests/test_strategy_profile_schema.py::StrategyProfileSchemaTests::test_unknown_profile_boundary_warns_but_uses_family_fallback -q
```

Expected: fail with missing `profile_boundary` or missing keys.

### Task 2: Schema Overlay Implementation

**Files:**
- Modify: `/Volumes/WORK/binance/wangge/.worktrees/strategy-workspace-controller/src/grid_optimizer/strategy_profile_schema.py`
- Test: `/Volumes/WORK/binance/wangge/.worktrees/strategy-workspace-controller/tests/test_strategy_profile_schema.py`

- [ ] **Step 1: Add overlay dataclass and registry**

Add `ProfileBoundaryOverlay` with `key`, `label`, `allowed_params`, `forbidden_params`, `required_params`, `global_safety_params`, and `runtime_switches`. Add overlays for:

```python
"aigensynusdt_best_quote_maker_volume_v1"
"aigensynusdt_hedge_bq_pingpong_sprint_v1"
"volume_long_v4"
"volume_neutral_ping_pong_v1"
"competition_inventory_grid_v1"
```

- [ ] **Step 2: Build report-only boundary object**

Add helper functions to compute:

```python
profile_boundary = {
    "profile_key": profile_key,
    "overlay_known": overlay is not None,
    "status": "ready" | "warning" | "blocked",
    "allowed_params": sorted(effective_allowed_params),
    "active_allowed_params": sorted(active_allowed),
    "global_safety_params": sorted(global_safety),
    "active_global_safety_params": sorted(active_global_safety),
    "forbidden_params": sorted(forbidden),
    "forbidden_active_params": sorted(active_forbidden),
    "required_params": sorted(required),
    "required_missing_params": sorted(missing),
    "ignored_params": ignored_params_sorted,
    "unknown_params": unknown_params_sorted,
    "messages": messages,
}
```

Keep existing strict pruning based on `schema.allowed_params`; do not prune or block solely from the overlay.

- [ ] **Step 3: Run schema tests**

Run:

```bash
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 PYTHONPATH=src python -m pytest tests/test_strategy_profile_schema.py -q
```

Expected: all schema tests pass.

### Task 3: Startup Preflight Warnings

**Files:**
- Modify: `/Volumes/WORK/binance/wangge/.worktrees/strategy-workspace-controller/src/grid_optimizer/strategy_profile_schema.py`
- Test: `/Volumes/WORK/binance/wangge/.worktrees/strategy-workspace-controller/tests/test_strategy_profile_schema.py`

- [ ] **Step 1: Extend startup report**

Add non-blocking startup warning codes:

```python
"profile_overlay_unknown"
"profile_forbidden_active_params"
"profile_required_missing_params"
```

Expose `profile_boundary`, `forbidden_active_params`, and `required_missing_params` through `startup_preflight`.

- [ ] **Step 2: Preserve can_start semantics**

Assert `startup_preflight["can_start"]` remains true when the only issues are forbidden active params or required missing params, because v1 is report-only.

- [ ] **Step 3: Run focused tests**

Run:

```bash
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 PYTHONPATH=src python -m pytest tests/test_strategy_profile_schema.py::StrategyProfileSchemaTests::test_profile_boundary_reports_forbidden_active_params_without_changing_effective_args tests/test_strategy_profile_schema.py::StrategyProfileSchemaTests::test_profile_boundary_reports_required_missing_params -q
```

Expected: pass.

### Task 4: Diagnostics Consumption

**Files:**
- Modify: `/Volumes/WORK/binance/wangge/.worktrees/strategy-workspace-controller/tests/test_strategy_diagnostics.py`
- Modify later: `/Volumes/WORK/binance/wangge/.worktrees/strategy-workspace-controller/src/grid_optimizer/strategy_diagnostics.py`

- [ ] **Step 1: Write failing diagnostics test**

Add a test where `startup_preflight["profile_boundary"]` includes active allowed params, active global safety params, forbidden active params, and required missing params. Assert `strategy_diagnostics.profile_boundary` includes items:

```python
"profile_overlay"
"active_allowed_params"
"active_global_safety_params"
"forbidden_active_params"
"required_missing_params"
```

- [ ] **Step 2: Run red diagnostics test**

Run:

```bash
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 PYTHONPATH=src python -m pytest tests/test_strategy_diagnostics.py::StrategyDiagnosticsTests::test_profile_boundary_shows_overlay_details -q
```

Expected: fail because the diagnostics section does not yet read `profile_boundary`.

- [ ] **Step 3: Implement diagnostics items**

Update `_profile_boundary_section()` to prefer `startup_preflight["profile_boundary"]` when present. Keep legacy `ignored_params`, `unknown_params`, and `required_position_mode` rendering for compatibility.

- [ ] **Step 4: Run diagnostics tests**

Run:

```bash
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 PYTHONPATH=src python -m pytest tests/test_strategy_diagnostics.py -q
```

Expected: all diagnostics tests pass.

### Task 5: Web API and Page Verification

**Files:**
- Modify if needed: `/Volumes/WORK/binance/wangge/.worktrees/strategy-workspace-controller/tests/test_web_security.py`
- Modify if needed: `/Volumes/WORK/binance/wangge/.worktrees/strategy-workspace-controller/src/grid_optimizer/web.py`

- [ ] **Step 1: Add API assertion**

Extend the existing strategy editor status test to assert the payload includes `strategy_profile_schema["profile_boundary"]` and diagnostics section `profile_boundary`.

- [ ] **Step 2: Keep page manual-refresh only**

Assert `STRATEGY_EDITOR_PAGE` still contains no `setInterval`, and the diagnostics renderer can display current values and reference values from the generic item fields.

- [ ] **Step 3: Run web tests**

Run:

```bash
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 PYTHONPATH=src python -m pytest tests/test_web_security.py::WebSecurityTests::test_strategy_editor_status_uses_lightweight_local_sources tests/test_web_security.py::WebSecurityTests::test_strategy_editor_page_has_detailed_diagnostics_without_auto_refresh -q
```

Expected: pass.

### Task 6: Full Verification, Commit, Deploy

**Files:**
- All modified files from earlier tasks.

- [ ] **Step 1: Run related suite**

Run:

```bash
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 PYTHONPATH=src python -m pytest tests/test_strategy_profile_schema.py tests/test_strategy_diagnostics.py tests/test_web_security.py tests/test_loop_runner.py tests/test_submit_plan.py -q
```

Expected: pass.

- [ ] **Step 2: Run diff check**

Run:

```bash
git diff --check
```

Expected: no output.

- [ ] **Step 3: Verify local strategy editor**

Use the local server at `http://127.0.0.1:62328/strategy_editor`, click manual refresh, and confirm the diagnostics section shows the refined profile boundary details without auto-refresh.

- [ ] **Step 4: Commit and push**

Run:

```bash
git add docs/superpowers/specs/2026-05-20-profile-boundary-overlay-design.md docs/superpowers/plans/2026-05-20-profile-boundary-overlay.md src/grid_optimizer/strategy_profile_schema.py src/grid_optimizer/strategy_diagnostics.py tests/test_strategy_profile_schema.py tests/test_strategy_diagnostics.py tests/test_web_security.py
git commit -m "Add profile boundary overlays"
git push origin codex/strategy-workspace-controller
```

- [ ] **Step 5: Deploy to 110**

Run:

```bash
ssh srv-43-156-35-110 'SERVICE_NAME=grid-web-controller BRANCH=codex/strategy-workspace-controller /usr/local/bin/grid-web-update'
```

Expected: service restarts successfully and `/api/health` returns `{"ok": true}`.
