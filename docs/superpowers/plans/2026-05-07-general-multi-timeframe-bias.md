# General Multi-Timeframe Bias Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the `1m / 15m / 1h / 4h` direction layer reusable across futures strategy modes through a mode adapter.

**Architecture:** Keep `multi_timeframe_bias.py` as the pure signal and adjustment module. Add an adapter field to the config, make `apply_multi_timeframe_bias()` mode-aware, then wire parser validation, plan reporting, saved-runner config, and web command building.

**Tech Stack:** Python, argparse, unittest, existing futures runner command/config plumbing.

---

### Task 1: Pure Adapter Behavior

**Files:**
- Modify: `tests/test_multi_timeframe_bias.py`
- Modify: `src/grid_optimizer/multi_timeframe_bias.py`

- [ ] Add failing tests for `one_way_long`, `one_way_short`, `competition_inventory_grid`, unavailable candles, and shock no-risk-increase behavior.
- [ ] Run `PYTHONPATH=src python3 -m unittest tests.test_multi_timeframe_bias` and confirm the new adapter tests fail because adapter support is missing.
- [ ] Extend `MultiTimeframeBiasConfig` with `mode_adapter`.
- [ ] Implement adapter resolution and mode-aware adjustments inside `apply_multi_timeframe_bias()`.
- [ ] Run `PYTHONPATH=src python3 -m unittest tests.test_multi_timeframe_bias` and confirm all pure module tests pass.

### Task 2: Runner Parser And Validation

**Files:**
- Modify: `tests/test_loop_runner.py`
- Modify: `src/grid_optimizer/loop_runner.py`

- [ ] Add failing parser/validation tests for `--multi-timeframe-bias-mode-adapter`, `one_way_long`, and invalid explicit adapter combinations.
- [ ] Run the focused tests and confirm they fail on missing parser/validation support.
- [ ] Add the parser flag and config construction field.
- [ ] Replace the old synthetic-only validation with supported futures-mode adapter validation.
- [ ] Run the focused loop-runner tests and confirm they pass.

### Task 3: Runner Plan Report Plumbing

**Files:**
- Modify: `tests/test_loop_runner.py`
- Modify: `src/grid_optimizer/loop_runner.py`

- [ ] Add a failing plan/report test asserting the adapter name appears in `multi_timeframe_bias` and flattened event fields.
- [ ] Run the focused test and confirm it fails before report plumbing.
- [ ] Include `adapter` in plan report, event summary, and human-readable runner log line.
- [ ] Run the focused report tests and confirm they pass.

### Task 4: Web Config And Saved Runner Command

**Files:**
- Modify: `tests/test_web_security.py`
- Modify: `src/grid_optimizer/web.py`

- [ ] Add a failing test that `_build_runner_command()` emits `--multi-timeframe-bias-mode-adapter`.
- [ ] Run the focused web test and confirm it fails.
- [ ] Add the default config field, request parsing allowlist, integer/float/string handling, and command argument.
- [ ] Run the focused web command tests and confirm they pass.

### Task 5: Verification

**Files:**
- Test only.

- [ ] Run `PYTHONPATH=src python3 -m unittest tests.test_multi_timeframe_bias`.
- [ ] Run focused loop-runner parser/report tests.
- [ ] Run focused web command tests.
- [ ] Run `PYTHONPATH=src python3 -m py_compile src/grid_optimizer/multi_timeframe_bias.py src/grid_optimizer/loop_runner.py src/grid_optimizer/web.py`.
- [ ] Commit the implementation.
