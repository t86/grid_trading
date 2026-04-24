# All Board Validation Configs Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Save low-intensity validation configs for every active board gap, without starting spot strategies and without overwriting running CHIPUSDT.

**Architecture:** Add a spot runner save-only path that reuses existing spot payload normalization and control-file persistence. Use existing futures runner presets for missing XAGUSDT, BZUSDT, and ORDIUSDC validation configs. Deploy through the 114 pull-based wrapper, then save runtime configs through local HTTP APIs.

**Tech Stack:** Python `unittest`, existing `grid_optimizer.web` runner helpers, 114 `/usr/local/bin/grid-web-update`, local `/api/runner/save` and new `/api/spot_runner/save`.

---

### Task 1: Add Spot Save-Only API

**Files:**
- Modify: `tests/test_spot_runner.py`
- Modify: `src/grid_optimizer/web.py`

- [x] **Step 1: Write failing tests**

Add tests that assert the spot runner page exposes a save button/API and `_save_spot_runner_config_without_start()` saves normalized `spot_competition_inventory_grid` config without starting a process.

- [x] **Step 2: Verify red**

Run: `PYTHONPATH=src python3 -m unittest tests.test_spot_runner.SpotRunnerTests.test_spot_runner_page_contains_controls tests.test_spot_runner.SpotRunnerTests.test_save_spot_runner_config_without_start_persists_config`

Expected: failure because `/api/spot_runner/save`, `id="save_btn"`, and `_save_spot_runner_config_without_start` do not exist yet.

- [x] **Step 3: Implement save-only helper and route**

Add `_save_spot_runner_config_without_start(payload)` that calls `_normalize_spot_runner_payload`, saves with `_save_spot_runner_control_config`, and returns the saved config plus `_read_spot_runner_process_for_symbol(symbol)`. Add `/api/spot_runner/save` to POST routing and lightweight preflight handling.

- [x] **Step 4: Add UI save action**

Add a `保存参数不启动` button to `SPOT_RUNNER_PAGE`, wire it to `POST /api/spot_runner/save`, and keep start/stop behavior unchanged.

- [x] **Step 5: Verify green**

Run: `PYTHONPATH=src python3 -m unittest tests.test_spot_runner`

Expected: all spot runner tests pass.

### Task 2: Verify And Deploy

**Files:**
- Modify: `src/grid_optimizer/web.py`
- Modify: `tests/test_spot_runner.py`

- [x] **Step 1: Run regression checks**

Run:
`PYTHONPATH=src python3 -m unittest tests.test_spot_runner tests.test_web_security tests.test_symbol_lists`
`python3 -m py_compile src/grid_optimizer/web.py tests/test_spot_runner.py`
`git diff --check`

- [ ] **Step 2: Commit and push**

Commit the API/UI change and push `main` to `origin/main`.

- [ ] **Step 3: Deploy 114**

Run `ssh srv-43-155-163-114 '/usr/local/bin/grid-web-update'`.

### Task 3: Save Missing Validation Configs

**Files:**
- No repository files. Runtime configs are saved through 114 local APIs.

- [ ] **Step 1: Save missing futures configs without starting**

POST the following to `http://127.0.0.1:8788/api/runner/save` on 114:
`XAGUSDT -> xagusdt_competition_maker_neutral_conservative_v1`
`BZUSDT -> bzusdt_competition_maker_neutral_conservative_v1`
`ORDIUSDC -> ordiusdc_competition_maker_neutral_conservative_v1`

- [ ] **Step 2: Save spot configs without starting**

POST `spot_competition_inventory_grid` configs to `http://127.0.0.1:8788/api/spot_runner/save` on 114 for `VANAUSDT`, `TONUSDT`, and `PLUMEUSDT`. Use low-intensity budgets and runtime guards so manual starts are bounded.

- [ ] **Step 3: Verify saved and stopped**

Check control JSON files for the six newly saved symbols. Confirm `grid-saved-runner status XAGUSDT/BZUSDT/ORDIUSDC` remains stopped and spot status for `VANAUSDT/TONUSDT/PLUMEUSDT` remains `is_running=false`. Confirm CHIPUSDT remains untouched.
