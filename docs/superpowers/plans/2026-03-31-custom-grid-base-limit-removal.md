# Custom Grid Base Limit Removal Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove base-position hard limits from `custom_grid_enabled=true` futures grid runners without changing non-grid futures strategies.

**Architecture:** Keep the change localized to the custom-grid plan builder, the custom-grid branch in `loop_runner`, and the Web-side config normalizer. Preserve all existing non-custom-grid behavior by leaving the micro-grid and hedge planners untouched.

**Tech Stack:** Python, unittest, existing grid runner/backtest helpers

---

### Task 1: Lock the approved scope in tests

**Files:**
- Modify: `tests/test_loop_runner.py`
- Modify: `tests/test_web_security.py`

- [ ] **Step 1: Write the failing tests**

```python
def test_build_static_binance_grid_plan_can_disable_base_bootstrap(self) -> None:
    plan = build_static_binance_grid_plan(..., bootstrap_positions=False)
    self.assertEqual(plan["bootstrap_orders"], [])
    self.assertEqual(plan["target_base_qty"], 0.0)
    self.assertEqual(plan["bootstrap_qty"], 0.0)

def test_runner_preset_payload_disables_excess_inventory_reduce_only_for_custom_grid(self) -> None:
    payload = _runner_preset_payload("custom_grid_opn_long", {"symbol": "OPNUSDT"})
    self.assertFalse(payload["excess_inventory_reduce_only_enabled"])
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_loop_runner.py -k "static_binance_grid_plan" tests/test_web_security.py -k "custom_grid_opn_long" -v`
Expected: FAIL because `build_static_binance_grid_plan()` does not yet accept the new toggle and custom-grid payload still enables `excess_inventory_reduce_only`.

- [ ] **Step 3: Write minimal implementation**

```python
def build_static_binance_grid_plan(..., bootstrap_positions: bool = True) -> dict[str, Any]:
    ...
```

```python
normalized["excess_inventory_reduce_only_enabled"] = False
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_loop_runner.py -k "static_binance_grid_plan" tests/test_web_security.py -k "custom_grid_opn_long" -v`
Expected: PASS

### Task 2: Remove runtime base gating from custom-grid execution

**Files:**
- Modify: `src/grid_optimizer/loop_runner.py`
- Modify: `src/grid_optimizer/semi_auto_plan.py`
- Test: `tests/test_loop_runner.py`

- [ ] **Step 1: Write the failing regression test**

```python
def test_custom_grid_long_plan_does_not_apply_excess_inventory_gate(self) -> None:
    # Build a custom-grid plan with current inventory above historical startup qty
    # and assert buy orders remain present instead of being cleared.
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_loop_runner.py -k "custom_grid" -v`
Expected: FAIL because the current custom-grid runner path still applies `apply_excess_inventory_reduce_only()`.

- [ ] **Step 3: Write minimal implementation**

```python
plan = build_static_binance_grid_plan(..., bootstrap_positions=False)
# Skip apply_excess_inventory_reduce_only() in all custom_grid_enabled branches
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_loop_runner.py -k "custom_grid" -v`
Expected: PASS

### Task 3: Run focused verification

**Files:**
- No code changes

- [ ] **Step 1: Run focused suite**

Run: `python -m pytest tests/test_semi_auto_plan.py tests/test_loop_runner.py tests/test_web_security.py -v`
Expected: All targeted tests pass.

- [ ] **Step 2: Review for unintended scope expansion**

Run: `git diff -- src/grid_optimizer/semi_auto_plan.py src/grid_optimizer/loop_runner.py src/grid_optimizer/web.py tests/test_loop_runner.py tests/test_web_security.py`
Expected: Only custom-grid-related changes plus the new regression tests appear.
