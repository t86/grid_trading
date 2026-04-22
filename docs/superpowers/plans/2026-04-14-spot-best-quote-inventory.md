# Spot Best Quote Inventory Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a spot strategy mode that bootstraps a base inventory, quotes maker orders at the best bid/ask while inventory stays light, and forces inventory reduction when spot exposure reaches twice the base target.

**Architecture:** Extend `spot_loop_runner` with a dedicated best-quote inventory planner instead of mutating the existing volume-shift planner. Reuse current lot accounting, open-order diffing, and trade sync, then layer inventory-state transitions plus a timed taker reduction fallback on top.

**Tech Stack:** Python, unittest, existing Binance spot order helpers, existing web preset payload wiring

---

### Task 1: Lock the planner contract with tests

**Files:**
- Modify: `tests/test_spot_loop_runner.py`
- Test: `tests/test_spot_loop_runner.py`

- [ ] Add failing tests for quote-mode order generation, reduce-only transition, cost-protected sell quoting, and timed taker fallback.
- [ ] Run `pytest tests/test_spot_loop_runner.py -q` and confirm the new tests fail for missing behavior.

### Task 2: Implement best-quote inventory planning in the runner

**Files:**
- Modify: `src/grid_optimizer/spot_loop_runner.py`
- Test: `tests/test_spot_loop_runner.py`

- [ ] Add helpers for inventory mode resolution, best-quote maker order building, reduce-only timeout tracking, and taker target sizing.
- [ ] Wire the new planner into `_run_cycle` under a new `strategy_mode`.
- [ ] Keep existing spot modes unchanged.
- [ ] Re-run `pytest tests/test_spot_loop_runner.py -q` until the new tests pass.

### Task 3: Expose the mode in the web preset layer

**Files:**
- Modify: `src/grid_optimizer/web.py`
- Test: `tests/test_web_security.py`

- [ ] Add a BARD-oriented preset for the new spot strategy mode with conservative default notional values.
- [ ] Update text summaries so the console describes bootstrap, best-quote quoting, and reduce-only/taker fallback correctly.
- [ ] Run the targeted web preset tests.

### Task 4: Verify end-to-end targeted coverage

**Files:**
- Test: `tests/test_spot_loop_runner.py`
- Test: `tests/test_web_security.py`

- [ ] Run the targeted pytest commands for spot runner and web preset coverage.
- [ ] Fix any regression surfaced by the focused suite before closing the task.
