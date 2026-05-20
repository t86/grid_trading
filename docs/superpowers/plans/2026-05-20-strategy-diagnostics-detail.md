# Strategy Diagnostics Detail Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a detailed, report-only `strategy_diagnostics` layer to explain order blockers, volume caps, inventory threshold pressure, state mode, profile boundary issues, and 200k/500k target feasibility in the lightweight strategy editor.

**Architecture:** Create a pure diagnostic builder in `src/grid_optimizer/strategy_diagnostics.py` so the logic is independent from the very large web module. Wire the builder into `_build_strategy_editor_status`, then render the returned sections in `/strategy_editor` without adding polling or heavy monitor calls.

**Tech Stack:** Python stdlib, existing `unittest` tests, existing static HTML/JS page in `src/grid_optimizer/web.py`, local browser verification through the Codex in-app browser.

---

## File Structure

- Create `src/grid_optimizer/strategy_diagnostics.py`
  - Owns pure report construction.
  - Must not import `grid_optimizer.web` or call exchange/monitor code.
  - Public API: `build_strategy_diagnostics(...) -> dict[str, Any]`.
- Create `tests/test_strategy_diagnostics.py`
  - Unit coverage for the diagnostic builder.
- Modify `src/grid_optimizer/web.py`
  - Import `build_strategy_diagnostics`.
  - Add `strategy_diagnostics` to `_build_strategy_editor_status`.
  - Add static page diagnostics markup, CSS, and JS rendering.
- Modify `tests/test_web_security.py`
  - Assert API status includes `strategy_diagnostics`.
  - Assert the page renders diagnostics hooks and remains manual-refresh only.

## Parallel Ownership

- Worker A owns only:
  - `src/grid_optimizer/strategy_diagnostics.py`
  - `tests/test_strategy_diagnostics.py`
- Worker B owns only:
  - static diagnostics rendering inside `STRATEGY_EDITOR_PAGE` in `src/grid_optimizer/web.py`
  - page-token assertions in `tests/test_web_security.py`
- Main integrator owns:
  - `_build_strategy_editor_status` API wiring in `src/grid_optimizer/web.py`
  - web status assertions in `tests/test_web_security.py`
  - final integration fixes, full tests, browser verification, commit/deploy

Workers must not edit files outside their ownership. Everyone must preserve `.learnings/` as untracked and untouched.

---

### Task 1: Pure Diagnostic Builder

**Files:**
- Create: `src/grid_optimizer/strategy_diagnostics.py`
- Create: `tests/test_strategy_diagnostics.py`

- [ ] **Step 1: Write failing tests for execution caps, target checks, and startup blockers**

Create `tests/test_strategy_diagnostics.py` with:

```python
import unittest

from grid_optimizer.strategy_diagnostics import build_strategy_diagnostics


class StrategyDiagnosticsTests(unittest.TestCase):
    def test_execution_caps_explain_partial_order_submission(self) -> None:
        report = build_strategy_diagnostics(
            config={
                "strategy_profile": "aigensynusdt_best_quote_maker_volume_v1",
                "strategy_mode": "one_way_long",
                "required_position_mode": "one_way",
                "buy_levels": 2,
                "sell_levels": 2,
                "per_order_notional": 120.0,
                "max_new_orders": 3,
                "max_total_notional": 400.0,
            },
            startup_preflight={"can_start": True, "status": "warning", "warning_codes": ["global_safety_limiting_params"]},
            safety_preflight={
                "estimated_cycle_order_count": 4,
                "estimated_cycle_notional": 480.0,
                "limiting_params": ["max_new_orders", "max_total_notional"],
                "blocking_params": [],
                "warning_params": [],
                "stop_guard_params": [],
                "takeover_params": [],
            },
        )

        self.assertEqual(report["status"], "warning")
        self.assertTrue(report["can_start"])
        self.assertEqual(report["order_cycle"]["estimated_order_count"], 4)
        self.assertEqual(report["order_cycle"]["estimated_notional"], 480.0)
        self.assertIn("max_total_notional", _item_keys(report, "execution_caps"))
        self.assertIn("max_new_orders", _item_keys(report, "execution_caps"))
        item = _item(report, "execution_caps", "max_total_notional")
        self.assertEqual(item["severity"], "warning")
        self.assertEqual(item["category"], "limits_volume")
        self.assertIn("只提交部分挂单", item["impact"])

    def test_startup_blocker_is_promoted_to_blocked_status(self) -> None:
        report = build_strategy_diagnostics(
            config={"buy_levels": 1, "sell_levels": 1, "per_order_notional": 100.0},
            startup_preflight={
                "can_start": False,
                "status": "blocked",
                "blocker_codes": ["global_safety_blocking_params"],
                "warning_codes": [],
                "blocking_params": ["cancel_stale"],
            },
            safety_preflight={"blocking_params": ["cancel_stale"], "limiting_params": []},
        )

        self.assertFalse(report["can_start"])
        self.assertEqual(report["status"], "blocked")
        self.assertGreaterEqual(report["blocker_count"], 1)
        self.assertIn("cancel_stale", _item_keys(report, "startup"))

    def test_volume_targets_include_200k_and_500k_feasibility(self) -> None:
        report = build_strategy_diagnostics(
            config={"buy_levels": 4, "sell_levels": 4, "per_order_notional": 250.0},
            startup_preflight={"can_start": True, "status": "ready"},
            safety_preflight={
                "estimated_cycle_order_count": 8,
                "estimated_cycle_notional": 2_000.0,
                "limiting_params": [],
                "blocking_params": [],
            },
        )

        targets = {int(item["target_notional"]): item for item in report["volume_targets"]}
        self.assertEqual(sorted(targets), [200_000, 500_000])
        self.assertEqual(targets[200_000]["required_full_cycles_per_hour"], 100.0)
        self.assertEqual(targets[200_000]["required_seconds_per_full_cycle"], 36.0)
        self.assertEqual(targets[500_000]["required_full_cycles_per_hour"], 250.0)
        self.assertEqual(targets[500_000]["required_seconds_per_full_cycle"], 14.4)
```

Append helper functions in the same test file:

```python
def _section(report: dict, key: str) -> dict:
    for section in report["sections"]:
        if section["key"] == key:
            return section
    raise AssertionError(f"section not found: {key}")


def _item_keys(report: dict, section_key: str) -> set[str]:
    return {item["key"] for item in _section(report, section_key)["items"]}


def _item(report: dict, section_key: str, item_key: str) -> dict:
    for item in _section(report, section_key)["items"]:
        if item["key"] == item_key:
            return item
    raise AssertionError(f"item not found: {section_key}.{item_key}")


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Verify the tests fail**

Run:

```bash
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 PYTHONPATH=src python -m pytest tests/test_strategy_diagnostics.py -q
```

Expected: FAIL with `ModuleNotFoundError: No module named 'grid_optimizer.strategy_diagnostics'`.

- [ ] **Step 3: Implement the pure builder**

Create `src/grid_optimizer/strategy_diagnostics.py` with a pure function:

```python
from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any


DEFAULT_VOLUME_TARGETS = (200_000.0, 500_000.0)


def build_strategy_diagnostics(
    *,
    config: Mapping[str, Any],
    startup_preflight: Mapping[str, Any] | None = None,
    safety_preflight: Mapping[str, Any] | None = None,
    position: Mapping[str, Any] | None = None,
    position_mode: Mapping[str, Any] | None = None,
    latest_loop: Mapping[str, Any] | None = None,
    orders: Mapping[str, Any] | None = None,
    plan_report: Mapping[str, Any] | None = None,
    submit_report: Mapping[str, Any] | None = None,
    runner_running: bool | None = None,
    volume_targets: Sequence[float] = DEFAULT_VOLUME_TARGETS,
) -> dict[str, Any]:
    """Build a lightweight report-only diagnostic summary for strategy editor."""
    cfg = dict(config or {})
    startup = dict(startup_preflight or {})
    safety = dict(safety_preflight or {})
    pos = dict(position or {})
    pos_mode = dict(position_mode or {})
    loop = dict(latest_loop or {})
    submit = dict(submit_report or {})

    estimated_order_count = max(_as_int(safety.get("estimated_cycle_order_count"), _as_int(cfg.get("buy_levels")) + _as_int(cfg.get("sell_levels"))), 0)
    estimated_notional = max(_as_float(safety.get("estimated_cycle_notional"), estimated_order_count * max(_as_float(cfg.get("per_order_notional")), 0.0)), 0.0)
    sections = [
        _startup_section(startup, safety, pos_mode),
        _execution_caps_section(cfg, estimated_order_count, estimated_notional, safety),
        _order_refresh_section(cfg, safety),
        _drift_guards_section(safety),
        _loss_stop_section(safety),
        _takeover_section(safety),
        _inventory_section(cfg, pos, pos_mode),
        _state_section(loop, submit, runner_running),
        _profile_boundary_section(startup, cfg),
    ]
    targets = _volume_targets(volume_targets, estimated_notional, safety)
    sections.append(
        {
            "key": "volume_targets",
            "title": "刷量目标可行性",
            "status": _max_status(item["severity"] for item in targets),
            "items": targets,
        }
    )
    blocker_count = sum(1 for section in sections for item in section["items"] if item["severity"] == "blocker")
    warning_count = sum(1 for section in sections for item in section["items"] if item["severity"] == "warning")
    status = "blocked" if blocker_count else "warning" if warning_count else "ready"
    mode = _classify_state(loop, submit, runner_running)
    return {
        "status": status,
        "can_start": blocker_count == 0 and bool(startup.get("can_start", True)),
        "mode": mode,
        "summary": _summary(status, mode, blocker_count, warning_count),
        "issue_count": blocker_count + warning_count,
        "blocker_count": blocker_count,
        "warning_count": warning_count,
        "sections": sections,
        "volume_targets": targets,
        "inventory": _inventory_snapshot(cfg, pos),
        "order_cycle": {
            "estimated_order_count": estimated_order_count,
            "estimated_notional": estimated_notional,
            "per_order_notional": _as_float(cfg.get("per_order_notional")),
            "buy_levels": _as_int(cfg.get("buy_levels")),
            "sell_levels": _as_int(cfg.get("sell_levels")),
        },
        "state": {
            "mode": mode,
            "active_state": loop.get("active_state") or "",
            "repair_ladder_level": loop.get("repair_ladder_level") or "",
            "error_message": loop.get("error_message") or submit.get("error_message") or submit.get("error") or "",
        },
    }
```

Add the helper functions in the same file. They should:

- coerce numbers with `_as_float` and `_as_int`
- create items with `_item`
- aggregate section status with `_max_status`
- create explicit messages for `max_new_orders`, `max_total_notional`, `cancel_stale`, startup blockers, ignored params, unknown params, inventory soft/hard distances, state classification, and volume targets
- keep all logic pure and deterministic

- [ ] **Step 4: Verify builder tests pass**

Run:

```bash
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 PYTHONPATH=src python -m pytest tests/test_strategy_diagnostics.py -q
```

Expected: PASS.

---

### Task 2: Inventory, State, and Profile Boundary Coverage

**Files:**
- Modify: `tests/test_strategy_diagnostics.py`
- Modify: `src/grid_optimizer/strategy_diagnostics.py`

- [ ] **Step 1: Add failing tests for inventory thresholds, state classification, and ignored params**

Append tests:

```python
    def test_one_way_inventory_near_soft_threshold_warns(self) -> None:
        report = build_strategy_diagnostics(
            config={
                "strategy_mode": "one_way_long",
                "pause_buy_position_notional": 1_000.0,
                "max_position_notional": 1_500.0,
            },
            startup_preflight={"can_start": True, "status": "ready"},
            safety_preflight={},
            position={"long_notional": 950.0, "short_notional": 0.0, "net_notional": 950.0},
        )

        item = _item(report, "inventory_thresholds", "long_soft_threshold")
        self.assertEqual(item["severity"], "warning")
        self.assertEqual(item["category"], "forces_repair")
        self.assertIn("接近软阈值", item["why"])

    def test_one_way_inventory_over_hard_threshold_blocks(self) -> None:
        report = build_strategy_diagnostics(
            config={
                "strategy_mode": "one_way_long",
                "pause_buy_position_notional": 1_000.0,
                "max_position_notional": 1_500.0,
            },
            startup_preflight={"can_start": True, "status": "ready"},
            safety_preflight={},
            position={"long_notional": 1_600.0, "short_notional": 0.0, "net_notional": 1_600.0},
        )

        item = _item(report, "inventory_thresholds", "long_hard_threshold")
        self.assertEqual(item["severity"], "blocker")
        self.assertEqual(item["category"], "forces_repair")
        self.assertEqual(report["status"], "blocked")

    def test_repair_state_is_classified_from_active_state_and_ladder(self) -> None:
        report = build_strategy_diagnostics(
            config={},
            startup_preflight={"can_start": True, "status": "warning"},
            latest_loop={"active_state": "inventory_recover", "repair_ladder_level": "passive_reduce"},
            runner_running=True,
        )

        self.assertEqual(report["mode"], "repair")
        self.assertIn("修仓状态", report["summary"])

    def test_profile_boundary_lists_ignored_and_unknown_params(self) -> None:
        report = build_strategy_diagnostics(
            config={"required_position_mode": "one_way"},
            startup_preflight={
                "can_start": True,
                "status": "warning",
                "ignored_params": ["hard_loss_forced_reduce_enabled"],
                "unknown_params": ["old_cross_strategy_knob"],
                "required_position_mode": "one_way",
                "required_position_mode_defaulted": True,
            },
        )

        keys = _item_keys(report, "profile_boundary")
        self.assertIn("ignored_params", keys)
        self.assertIn("unknown_params", keys)
        self.assertIn("required_position_mode", keys)
```

- [ ] **Step 2: Verify the new tests fail for missing details**

Run:

```bash
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 PYTHONPATH=src python -m pytest tests/test_strategy_diagnostics.py -q
```

Expected: FAIL on missing `long_soft_threshold`, `long_hard_threshold`, or profile boundary items until implemented.

- [ ] **Step 3: Implement missing details**

Update `strategy_diagnostics.py` so:

- `inventory_thresholds` reports `long_soft_threshold` and `long_hard_threshold`
- hard threshold severity is `blocker`
- soft threshold severity is `warning` when inventory is at or above 80% of the soft threshold
- `state_machine` reports `repair` for active states containing `recover`, `safe`, `repair`, or `reduce`
- `profile_boundary` emits explicit `ignored_params`, `unknown_params`, and `required_position_mode` items

- [ ] **Step 4: Verify all builder tests pass**

Run:

```bash
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 PYTHONPATH=src python -m pytest tests/test_strategy_diagnostics.py -q
```

Expected: PASS.

---

### Task 3: Strategy Editor Page Rendering

**Files:**
- Modify: `src/grid_optimizer/web.py`
- Modify: `tests/test_web_security.py`

- [ ] **Step 1: Add failing page assertions**

In `test_strategy_editor_page_is_manual_and_lightweight`, add:

```python
self.assertIn("极细诊断", STRATEGY_EDITOR_PAGE)
self.assertIn('id="diagnostics_panel"', STRATEGY_EDITOR_PAGE)
self.assertIn("renderDiagnostics", STRATEGY_EDITOR_PAGE)
self.assertIn("strategy_diagnostics", STRATEGY_EDITOR_PAGE)
```

Run:

```bash
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 PYTHONPATH=src python -m pytest tests/test_web_security.py::WebSecurityTests::test_strategy_editor_page_is_manual_and_lightweight -q
```

Expected: FAIL because diagnostics rendering hooks do not exist yet.

- [ ] **Step 2: Add static diagnostics container and styles**

In `STRATEGY_EDITOR_PAGE`, add a panel below the existing status cards:

```html
<section class="panel">
  <h2>极细诊断</h2>
  <div id="diagnostics_panel" class="diagnostics-panel">
    <div class="empty-state">点击刷新状态后显示策略诊断。</div>
  </div>
</section>
```

Add compact CSS for:

- `.diagnostics-panel`
- `.diagnostics-summary`
- `.diagnostics-section`
- `.diagnostics-item`
- `.diagnostics-item.blocker`
- `.diagnostics-item.warning`
- `.diagnostics-item.info`
- `.diagnostics-kv`

- [ ] **Step 3: Add JS rendering**

Add `const diagnosticsPanel = document.getElementById("diagnostics_panel");`.

Add a `renderDiagnostics(data)` function that:

- reads `data.strategy_diagnostics || {}`
- renders summary, status, mode, issue counts, and order cycle
- renders sections and items
- displays `why`, `impact`, `suggestion`, and `tradeoff`
- uses `escapeHtml`
- does not fetch anything by itself
- does not use `setInterval`

Call `renderDiagnostics(data)` inside `renderStatus(data)` after the status cards are rendered.

- [ ] **Step 4: Verify page test passes**

Run:

```bash
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 PYTHONPATH=src python -m pytest tests/test_web_security.py::WebSecurityTests::test_strategy_editor_page_is_manual_and_lightweight -q
```

Expected: PASS.

---

### Task 4: API Wiring

**Files:**
- Modify: `src/grid_optimizer/web.py`
- Modify: `tests/test_web_security.py`

- [ ] **Step 1: Add failing API assertions**

In `test_strategy_editor_status_uses_lightweight_local_sources`, add:

```python
self.assertIn("strategy_diagnostics", payload)
self.assertEqual(payload["strategy_diagnostics"]["status"], "blocked")
self.assertEqual(payload["strategy_diagnostics"]["mode"], "volume")
self.assertGreaterEqual(payload["strategy_diagnostics"]["issue_count"], 1)
self.assertIn("execution_caps", {section["key"] for section in payload["strategy_diagnostics"]["sections"]})
self.assertIn(200_000, {int(item["target_notional"]) for item in payload["strategy_diagnostics"]["volume_targets"]})
self.assertIn(500_000, {int(item["target_notional"]) for item in payload["strategy_diagnostics"]["volume_targets"]})
```

Run:

```bash
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 PYTHONPATH=src python -m pytest tests/test_web_security.py::WebSecurityTests::test_strategy_editor_status_uses_lightweight_local_sources -q
```

Expected: FAIL because `strategy_diagnostics` is not yet returned.

- [ ] **Step 2: Wire the builder into `_build_strategy_editor_status`**

In `src/grid_optimizer/web.py`, import:

```python
from grid_optimizer.strategy_diagnostics import build_strategy_diagnostics
```

In `_build_strategy_editor_status`, after `latest_loop`, `position`, `position_mode`, and order summary values are known, build:

```python
position_payload = {
    "summary": _strategy_editor_position_summary(long_notional, short_notional, net_notional),
    "long_qty": long_qty,
    "short_qty": short_qty,
    "long_notional": long_notional,
    "short_notional": short_notional,
    "net_notional": net_notional,
    "unrealized_pnl": _status_float(position.get("unrealized_pnl")),
    "mid_price": mid_price,
}
position_mode_payload = {
    "required": required_position_mode,
    "current": current_position_mode,
    "dual_side_position": _truthy(dual_side_raw) if dual_side_known else None,
    "compatible": position_mode_compatible,
}
orders_payload = {
    "strategy_open_order_count": len(open_orders),
    "buy_count": sum(1 for item in open_orders if str((item or {}).get("side", "")).upper() == "BUY"),
    "sell_count": sum(1 for item in open_orders if str((item or {}).get("side", "")).upper() == "SELL"),
}
strategy_diagnostics = build_strategy_diagnostics(
    config=config,
    startup_preflight=startup_preflight,
    safety_preflight=safety_preflight,
    position=position_payload,
    position_mode=position_mode_payload,
    latest_loop=latest_loop,
    orders=orders_payload,
    plan_report=plan_report,
    submit_report=submit_report,
    runner_running=bool(runner.get("is_running")),
)
```

Return those payload variables rather than duplicating inline dict literals.

- [ ] **Step 3: Verify API test passes**

Run:

```bash
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 PYTHONPATH=src python -m pytest tests/test_web_security.py::WebSecurityTests::test_strategy_editor_status_uses_lightweight_local_sources -q
```

Expected: PASS and `mock_build_snapshot.assert_not_called()` still passes.

---

### Task 5: Integration Verification

**Files:**
- Modify only if tests reveal integration issues.

- [ ] **Step 1: Run focused tests**

Run:

```bash
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 PYTHONPATH=src python -m pytest tests/test_strategy_diagnostics.py tests/test_web_security.py::WebSecurityTests::test_strategy_editor_status_uses_lightweight_local_sources tests/test_web_security.py::WebSecurityTests::test_strategy_editor_page_is_manual_and_lightweight -q
```

Expected: PASS.

- [ ] **Step 2: Run existing related suite**

Run:

```bash
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 PYTHONPATH=src python -m pytest tests/test_strategy_profile_schema.py tests/test_web_security.py tests/test_loop_runner.py tests/test_submit_plan.py -q
```

Expected: PASS.

- [ ] **Step 3: Verify formatting**

Run:

```bash
git diff --check
```

Expected: no output and exit code 0.

- [ ] **Step 4: Browser verification**

Start or reuse the local strategy editor server on port 62328. Open:

```text
http://127.0.0.1:62328/strategy_editor
```

Click `刷新状态`. Confirm:

- the page shows `极细诊断`
- diagnostics summary renders
- sections render
- no automatic refresh starts
- browser console has no errors

- [ ] **Step 5: Commit**

Commit all implementation changes:

```bash
git add src/grid_optimizer/strategy_diagnostics.py src/grid_optimizer/web.py tests/test_strategy_diagnostics.py tests/test_web_security.py docs/superpowers/plans/2026-05-20-strategy-diagnostics-detail.md
git commit -m "Add detailed strategy diagnostics"
```

Expected: commit succeeds and `.learnings/` remains untracked.
