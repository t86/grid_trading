# Maker Volatility Inventory Futures Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a futures `maker_volatility_inventory_v1` strategy mode that generates top-of-book maker orders with volatility widening, inventory reduction, hard reduce-only, and cooldown behavior.

**Architecture:** Add a focused plan builder in `src/grid_optimizer/semi_auto_plan.py`, then integrate it into `src/grid_optimizer/loop_runner.py` as a new strategy branch. Keep execution, post-only handling, order diff, runtime guards, state persistence, and audit behavior on the existing futures runner path.

**Tech Stack:** Python, argparse, pytest/unittest, existing Binance futures runner modules.

---

### Task 1: Maker Plan Builder

**Files:**
- Modify: `src/grid_optimizer/semi_auto_plan.py`
- Test: `tests/test_loop_runner.py`

- [ ] **Step 1: Write failing tests**

Add tests that import `build_maker_volatility_inventory_plan` and verify normal, volatility-wide, soft inventory, hard inventory, cooldown, directional one-sided moves, and invalid market data behavior.

- [ ] **Step 2: Run tests and verify failure**

Run: `pytest tests/test_loop_runner.py -k maker_volatility_inventory -q`

Expected: fails because `build_maker_volatility_inventory_plan` does not exist.

- [ ] **Step 3: Implement minimal builder**

Create `build_maker_volatility_inventory_plan(...)` in `semi_auto_plan.py`. It should return the same order-list shape as existing plan builders: `buy_orders`, `sell_orders`, `bootstrap_orders`, `target_base_qty`, `bootstrap_qty`, plus a `maker_state` dict with regime/reason/metrics.

- [ ] **Step 4: Run tests and verify pass**

Run: `pytest tests/test_loop_runner.py -k maker_volatility_inventory -q`

Expected: all focused builder tests pass.

### Task 2: Runner Integration

**Files:**
- Modify: `src/grid_optimizer/loop_runner.py`
- Test: `tests/test_loop_runner.py`

- [ ] **Step 1: Write failing integration test**

Add a `generate_plan_report()` test with patched futures market/account functions asserting `strategy_mode="maker_volatility_inventory_v1"` produces a compatible plan and includes `maker_volatility_inventory`.

- [ ] **Step 2: Run test and verify failure**

Run: `pytest tests/test_loop_runner.py -k maker_volatility_inventory_generate_plan -q`

Expected: fails because the strategy mode is unsupported.

- [ ] **Step 3: Integrate strategy mode**

Add the new mode to parser choices and supported strategy-mode validation. In `generate_plan_report()`, calculate one-way net long/short quantities for this mode, call the new plan builder, set controls/cap-controls from builder output, and include `maker_volatility_inventory` in the report.

- [ ] **Step 4: Run integration test and verify pass**

Run: `pytest tests/test_loop_runner.py -k maker_volatility_inventory_generate_plan -q`

Expected: pass.

### Task 3: Verification

**Files:**
- Modify: none unless tests expose a defect.

- [ ] **Step 1: Run focused tests**

Run: `pytest tests/test_loop_runner.py -k maker_volatility_inventory -q`

Expected: pass.

- [ ] **Step 2: Run existing runner tests**

Run: `pytest tests/test_loop_runner.py tests/test_runtime_guards.py -q`

Expected: pass.

- [ ] **Step 3: Review diff**

Run: `git diff -- src/grid_optimizer/semi_auto_plan.py src/grid_optimizer/loop_runner.py tests/test_loop_runner.py docs/superpowers/specs/2026-04-29-maker-volatility-inventory-futures-design.md docs/superpowers/plans/2026-04-29-maker-volatility-inventory-futures.md`

Expected: only scoped strategy, tests, plan, and spec changes.
