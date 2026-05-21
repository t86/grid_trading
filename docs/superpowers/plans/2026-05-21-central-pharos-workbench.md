# Central PHAROS Workbench Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the first vertical slice of the 110 central strategy config workbench so PHAROS hedge best-quote parameters on 114/150 are visible, grouped by scope, and preflighted without changing strategy algorithms.

**Architecture:** Add a small `central_strategy_workbench` service module that owns target catalog, remote-read helpers, config sectioning, and preflight payloads. Extend `strategy_profile_schema` with the PHAROS hedge BQ family/overlay, then add a lightweight page and API endpoints in `web.py` that render the catalog and grouped parameters.

**Tech Stack:** Python stdlib HTTP server in `src/grid_optimizer/web.py`, new Python service module under `src/grid_optimizer`, existing unittest/pytest tests, existing shell/SSH access patterns.

---

### Task 1: PHAROS Hedge BQ Schema Overlay

**Files:**
- Modify: `/Volumes/WORK/binance/wangge/.worktrees/strategy-workspace-controller/src/grid_optimizer/strategy_profile_schema.py`
- Modify: `/Volumes/WORK/binance/wangge/.worktrees/strategy-workspace-controller/tests/test_strategy_profile_schema.py`

- [ ] **Step 1: Write failing schema tests**

Add tests:

```python
def test_pharos_hedge_best_quote_maker_volume_requires_hedge_and_known_overlay(self) -> None:
    _, report = apply_strategy_profile_schema(
        _args(
            symbol="PHAROSUSDT",
            strategy_profile="pharosusdt_hedge_best_quote_maker_volume_v1",
            strategy_mode="hedge_best_quote_maker_volume_v1",
            best_quote_maker_volume_enabled=True,
            best_quote_maker_volume_cycle_budget_notional=40.0,
            best_quote_maker_volume_target_remaining_notional=200_000.0,
            best_quote_maker_volume_quote_offset_ticks=3,
            best_quote_maker_volume_max_long_notional=700.0,
            best_quote_maker_volume_max_short_notional=700.0,
            hard_loss_forced_reduce_enabled=True,
            hard_loss_forced_reduce_target_notional=220.0,
            volatility_entry_pause_enabled=True,
            anti_chase_entry_guard_enabled=True,
            max_total_notional=1450.0,
            required_position_mode="hedge",
        ),
        enabled=True,
    )

    self.assertEqual(report["profile_family"], "hedge_best_quote_maker_volume")
    self.assertEqual(report["required_position_mode"], "hedge")
    self.assertTrue(report["profile_boundary"]["overlay_known"])
    self.assertNotIn("best_quote_maker_volume_enabled", report["unknown_params"])
```

```python
def test_pharos_overlay_marks_unrelated_modules_forbidden(self) -> None:
    _, report = apply_strategy_profile_schema(
        _args(
            symbol="PHAROSUSDT",
            strategy_profile="pharosusdt_hedge_best_quote_maker_volume_v1",
            strategy_mode="hedge_best_quote_maker_volume_v1",
            required_position_mode="hedge",
            best_quote_maker_volume_enabled=True,
            synthetic_flow_sleeve_enabled=True,
            volume_long_v4_flow_sleeve_enabled=True,
            custom_grid_enabled=True,
            max_total_notional=1450.0,
        ),
        enabled=True,
    )

    forbidden = report["profile_boundary"]["forbidden_active_params"]
    self.assertIn("synthetic_flow_sleeve_enabled", forbidden)
    self.assertIn("volume_long_v4_flow_sleeve_enabled", forbidden)
    self.assertIn("custom_grid_enabled", forbidden)
```

- [ ] **Step 2: Verify red**

Run:

```bash
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 PYTHONPATH=src python -m pytest tests/test_strategy_profile_schema.py::StrategyProfileSchemaTests::test_pharos_hedge_best_quote_maker_volume_requires_hedge_and_known_overlay tests/test_strategy_profile_schema.py::StrategyProfileSchemaTests::test_pharos_overlay_marks_unrelated_modules_forbidden -q
```

Expected: fail because PHAROS is still classified as `best_quote` and overlay is unknown.

- [ ] **Step 3: Implement minimal schema support**

Add:

- `BEST_QUOTE_MAKER_VOLUME_PARAMS`
- `HEDGE_BEST_QUOTE_MAKER_VOLUME_RUNTIME_SWITCHES`
- `HEDGE_BEST_QUOTE_MAKER_VOLUME_ALLOWED_PARAMS`
- `PHAROS_HEDGE_BQ_FORBIDDEN_PARAMS`
- profile overlay for `pharosusdt_hedge_best_quote_maker_volume_v1`
- resolver branch before the generic `_best_quote_maker_volume_v1` suffix rule

Keep behavior report-only.

- [ ] **Step 4: Verify green**

Run:

```bash
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 PYTHONPATH=src python -m pytest tests/test_strategy_profile_schema.py -q
```

Expected: all schema tests pass.

### Task 2: Central Workbench Service Module

**Files:**
- Create: `/Volumes/WORK/binance/wangge/.worktrees/strategy-workspace-controller/src/grid_optimizer/central_strategy_workbench.py`
- Create: `/Volumes/WORK/binance/wangge/.worktrees/strategy-workspace-controller/tests/test_central_strategy_workbench.py`

- [ ] **Step 1: Write failing service tests**

Add tests:

```python
def test_pharos_catalog_scopes_common_params_by_target() -> None:
    targets = list_central_strategy_targets()
    target = get_central_strategy_target("114", "PHAROSUSDT", "pharosusdt_hedge_best_quote_maker_volume_v1")

    assert {item["server_id"] for item in targets} >= {"114", "150"}
    assert target["scope"] == {
        "server_id": "114",
        "symbol": "PHAROSUSDT",
        "strategy_profile": "pharosusdt_hedge_best_quote_maker_volume_v1",
    }
    assert "max_new_orders" in target["common_params"]
    assert "best_quote_maker_volume_cycle_budget_notional" in target["profile_params"]
```

```python
def test_build_workbench_payload_groups_config_by_owner() -> None:
    payload = build_workbench_payload(
        server_id="114",
        symbol="PHAROSUSDT",
        strategy_profile="pharosusdt_hedge_best_quote_maker_volume_v1",
        config={
            "symbol": "PHAROSUSDT",
            "strategy_profile": "pharosusdt_hedge_best_quote_maker_volume_v1",
            "strategy_mode": "hedge_best_quote_maker_volume_v1",
            "required_position_mode": "hedge",
            "max_new_orders": 5,
            "max_total_notional": 1450.0,
            "best_quote_maker_volume_enabled": True,
            "best_quote_maker_volume_cycle_budget_notional": 40.0,
            "synthetic_flow_sleeve_enabled": True,
            "old_unknown_knob": 1,
        },
    )

    assert payload["scope"]["server_id"] == "114"
    assert "max_new_orders" in payload["sections"]["common"]
    assert "best_quote_maker_volume_cycle_budget_notional" in payload["sections"]["profile"]
    assert "synthetic_flow_sleeve_enabled" in payload["sections"]["forbidden"]
    assert "old_unknown_knob" in payload["sections"]["unknown"]
```

- [ ] **Step 2: Verify red**

Run:

```bash
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 PYTHONPATH=src python -m pytest tests/test_central_strategy_workbench.py -q
```

Expected: fail because module does not exist.

- [ ] **Step 3: Implement service module**

Create:

- `CentralStrategyTarget` dataclass
- `CENTRAL_STRATEGY_TARGETS`
- `list_central_strategy_targets()`
- `get_central_strategy_target(server_id, symbol, strategy_profile)`
- `build_workbench_payload(server_id, symbol, strategy_profile, config)`
- `read_remote_target_config(target)` using `ssh` and server-specific control file path
- `build_remote_workbench_status(server_id, symbol, strategy_profile)` for API use

The service module must not apply/restart runners in this task.

- [ ] **Step 4: Verify green**

Run:

```bash
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 PYTHONPATH=src python -m pytest tests/test_central_strategy_workbench.py -q
```

Expected: pass.

### Task 3: Central Workbench API And Page

**Files:**
- Modify: `/Volumes/WORK/binance/wangge/.worktrees/strategy-workspace-controller/src/grid_optimizer/web.py`
- Modify: `/Volumes/WORK/binance/wangge/.worktrees/strategy-workspace-controller/tests/test_web_security.py`

- [ ] **Step 1: Write failing web tests**

Add tests:

```python
def test_central_strategy_workbench_page_is_available(self) -> None:
    self.assertIn("中央策略配置工作台", CENTRAL_STRATEGY_WORKBENCH_PAGE)
    self.assertIn("/api/central_strategy_workbench/status", CENTRAL_STRATEGY_WORKBENCH_PAGE)
    self.assertIn("PHAROSUSDT", CENTRAL_STRATEGY_WORKBENCH_PAGE)
    self.assertNotIn("setInterval(", CENTRAL_STRATEGY_WORKBENCH_PAGE)
```

```python
@patch("grid_optimizer.web.build_remote_workbench_status")
def test_central_strategy_workbench_status_api_returns_grouped_sections(self, mock_status) -> None:
    mock_status.return_value = {
        "ok": True,
        "scope": {"server_id": "114", "symbol": "PHAROSUSDT", "strategy_profile": "pharosusdt_hedge_best_quote_maker_volume_v1"},
        "sections": {"common": {"max_new_orders": 5}, "profile": {"best_quote_maker_volume_cycle_budget_notional": 40.0}, "global_safety": {}, "forbidden": {}, "unknown": {}},
    }
    payload = _build_central_strategy_workbench_status("114", "PHAROSUSDT", "pharosusdt_hedge_best_quote_maker_volume_v1")
    self.assertTrue(payload["ok"])
    self.assertIn("common", payload["sections"])
```

- [ ] **Step 2: Verify red**

Run:

```bash
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 PYTHONPATH=src python -m pytest tests/test_web_security.py::WebSecurityTests::test_central_strategy_workbench_page_is_available tests/test_web_security.py::WebSecurityTests::test_central_strategy_workbench_status_api_returns_grouped_sections -q
```

Expected: fail because page/API are missing.

- [ ] **Step 3: Implement page and GET API**

Add:

- import from `central_strategy_workbench`
- `CENTRAL_STRATEGY_WORKBENCH_PAGE`
- `_build_central_strategy_workbench_status(server_id, symbol, strategy_profile)`
- GET route `/api/central_strategy_workbench/status`
- HEAD route support
- HTML route `/central_strategy_workbench`

The page should:

- be manual refresh only
- default to `114 / PHAROSUSDT / pharosusdt_hedge_best_quote_maker_volume_v1`
- show target selector
- show Common / Strategy-Owned / Global Safety / Forbidden / Unknown sections
- include disabled Save / Save And Apply buttons labelled "下一阶段开放" in this first vertical slice

- [ ] **Step 4: Verify green**

Run:

```bash
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 PYTHONPATH=src python -m pytest tests/test_web_security.py::WebSecurityTests::test_central_strategy_workbench_page_is_available tests/test_web_security.py::WebSecurityTests::test_central_strategy_workbench_status_api_returns_grouped_sections -q
```

Expected: pass.

### Task 4: Integration Verification And Deploy

**Files:**
- All files changed above.

- [ ] **Step 1: Run focused suite**

Run:

```bash
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 PYTHONPATH=src python -m pytest tests/test_strategy_profile_schema.py tests/test_central_strategy_workbench.py tests/test_web_security.py::WebSecurityTests::test_central_strategy_workbench_page_is_available tests/test_web_security.py::WebSecurityTests::test_central_strategy_workbench_status_api_returns_grouped_sections -q
```

Expected: pass.

- [ ] **Step 2: Run broad related suite**

Run:

```bash
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 PYTHONPATH=src python -m pytest tests/test_strategy_profile_schema.py tests/test_central_strategy_workbench.py tests/test_strategy_diagnostics.py tests/test_web_security.py tests/test_loop_runner.py tests/test_submit_plan.py -q
```

Expected: pass.

- [ ] **Step 3: Verify local page**

Start or restart local web on `127.0.0.1:62328`, open:

```text
http://127.0.0.1:62328/central_strategy_workbench
```

Click refresh and confirm PHAROS grouped parameters render.

- [ ] **Step 4: Commit and push**

Run:

```bash
git add src/grid_optimizer/strategy_profile_schema.py src/grid_optimizer/central_strategy_workbench.py src/grid_optimizer/web.py tests/test_strategy_profile_schema.py tests/test_central_strategy_workbench.py tests/test_web_security.py docs/superpowers/plans/2026-05-21-central-pharos-workbench.md
git commit -m "Add central PHAROS strategy workbench"
git push origin codex/strategy-workspace-controller
```

- [ ] **Step 5: Deploy to 110**

Run:

```bash
ssh srv-43-156-35-110 'SERVICE_NAME=grid-web-controller BRANCH=codex/strategy-workspace-controller /usr/local/bin/grid-web-update'
```

Verify:

```bash
ssh srv-43-156-35-110 'systemctl is-active grid-web-controller.service && curl -sS http://127.0.0.1:8787/api/health'
```

Then verify the authenticated 110 page/API contain `中央策略配置工作台` and grouped PHAROS sections.
