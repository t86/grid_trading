# ETHUSDC Volume Long Preset Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an ETHUSDC UM sprint preset that runs a maker-only one-way-long volume push strategy and save it to 114 without starting the runner.

**Architecture:** Extend the existing runner preset registry in `src/grid_optimizer/web.py` with a symbol-bound `ethusdc_um_volume_long_v1` preset. Add tests that verify payload values, symbol filtering, and UI visibility, then deploy pull-based to 114 and save the preset through the local runner-save API.

**Tech Stack:** Python `unittest`, existing `grid_optimizer.web` runner preset helpers, 114 `/usr/local/bin/grid-web-update`, local `/api/runner/save`.

---

### Task 1: Add Preset Tests

**Files:**
- Modify: `tests/test_web_security.py`

- [x] **Step 1: Add a failing payload test**

Add a test asserting `_runner_preset_payload("ethusdc_um_volume_long_v1", {"symbol": "ETHUSDC"})` returns `strategy_mode="one_way_long"`, `step_price=0.6`, `buy_levels=6`, `sell_levels=10`, `per_order_notional=100.0`, `base_position_notional=250.0`, `max_position_notional=900.0`, `max_total_notional=1800.0`, `take_profit_min_profit_ratio=0.0`, `volatility_trigger_enabled=False`, and no close-all flags.

- [x] **Step 2: Add visibility assertions**

Extend the sprint preset summaries test to require `ethusdc_um_volume_long_v1` for `ETHUSDC`, and not for `BTCUSDC`.

- [x] **Step 3: Verify red**

Run: `PYTHONPATH=src python3 -m unittest tests.test_web_security.WebSecurityTests.test_runner_preset_payload_applies_ethusdc_um_volume_long_profile tests.test_web_security.WebSecurityTests.test_runner_preset_summaries_include_sprint_symbol_presets_with_config`

Expected: failure because the preset does not exist yet.

### Task 2: Implement Preset

**Files:**
- Modify: `src/grid_optimizer/web.py`

- [x] **Step 1: Add backend preset**

Add `ethusdc_um_volume_long_v1` to `RUNNER_STRATEGY_PRESETS`, symbol-bound to `ETHUSDC`, `kind="one_way"`, with the approved one-way-long maker-only configuration.

- [x] **Step 2: Add visibility**

Add the key to `RUNNER_PRESET_VISIBILITY_WHITELIST`, `VISIBLE_STRATEGY_PRESET_KEYS`, and the UM sprint preset group.

- [x] **Step 3: Add local UI fallback and guide note**

Add a matching local preset entry in `LOCAL_STRATEGY_PRESETS` and a short `RUNNER_PROFILE_GUIDE_NOTES` entry.

- [x] **Step 4: Verify green**

Run: `PYTHONPATH=src python3 -m unittest tests.test_web_security.WebSecurityTests.test_runner_preset_payload_applies_ethusdc_um_volume_long_profile tests.test_web_security.WebSecurityTests.test_runner_preset_summaries_include_sprint_symbol_presets_with_config`

Expected: both tests pass.

### Task 3: Deploy And Save

**Files:**
- Modify: `src/grid_optimizer/web.py`
- Modify: `tests/test_web_security.py`

- [x] **Step 1: Run regression checks**

Run:
`PYTHONPATH=src python3 -m unittest tests.test_web_security`
`python3 -m py_compile src/grid_optimizer/web.py tests/test_web_security.py`
`git diff --check`

- [ ] **Step 2: Commit and push**

Commit changes on `main` and push to `origin/main`.

- [ ] **Step 3: Deploy 114**

Run `ssh srv-43-155-163-114 '/usr/local/bin/grid-web-update'`.

- [ ] **Step 4: Save without start**

POST `{"symbol":"ETHUSDC","strategy_profile":"ethusdc_um_volume_long_v1","reset_state":true}` to `http://127.0.0.1:8788/api/runner/save` on 114.

- [ ] **Step 5: Verify saved and stopped**

Check `/home/ubuntu/wangge/output/ethusdc_loop_runner_control.json` has `strategy_profile="ethusdc_um_volume_long_v1"` and `/usr/local/bin/grid-saved-runner status ETHUSDC` still reports stopped.
