# Strategy Profile Isolation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the first code layer for clean strategy profiles: strict parameter schema enforcement, explicit state intent, and adverse repair ladder reporting.

**Architecture:** Keep the first cut modular. `strategy_profile_schema.py` owns profile allowlists and strict argument pruning; `competition_elastic_volume.py` owns the volume/recovery/protection state report and repair ladder. `loop_runner.py` only wires parser flags, config construction, state persistence, and summary fields.

**Tech Stack:** Python 3.10+, dataclasses, argparse Namespace, existing pytest/unittest tests.

---

### Task 1: Profile Schema Guard

**Files:**
- Create: `src/grid_optimizer/strategy_profile_schema.py`
- Test: `tests/test_strategy_profile_schema.py`

- [x] Write tests for strict BQ pruning and synthetic pass-through.
- [x] Implement minimal schema registry and `apply_strategy_profile_schema`.
- [x] Verify `tests/test_strategy_profile_schema.py`.

### Task 2: Repair Ladder State

**Files:**
- Modify: `src/grid_optimizer/competition_elastic_volume.py`
- Test: `tests/test_competition_elastic_volume.py`

- [x] Write tests for passive, touch, near-cross, and hard-risk repair states.
- [x] Add config/input fields and ladder resolution.
- [x] Keep existing sprint/cruise/recover/cooldown tests passing.

### Task 3: Runner Wiring

**Files:**
- Modify: `src/grid_optimizer/loop_runner.py`
- Test: `tests/test_loop_runner.py`

- [x] Add strict schema parser flag and elastic repair CLI parameters.
- [x] Apply strict schema before runtime guard modules consume unrelated params.
- [x] Persist repair ladder state and expose flat summary fields.
- [x] Apply BQ repair ladder to one-way best-quote reduce orders.

### Task 4: Verification

**Files:**
- Test: `tests/test_strategy_profile_schema.py`
- Test: `tests/test_competition_elastic_volume.py`
- Test: relevant `tests/test_loop_runner.py` cases

- [x] Run targeted pytest with `PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 PYTHONPATH=src`.
- [x] Inspect `git diff` for unintended production config changes.

### Task 5: AIGENSYN Best Quote Clean Preset

**Files:**
- Modify: `src/grid_optimizer/web.py`
- Modify: `docs/STRATEGY_EXECUTION_GUIDE.md`
- Test: `tests/test_web_security.py`

- [x] Add a strict AIGENSYNUSDT `best_quote_maker_volume_v1` web preset from 111 historical backups.
- [x] Expose strict schema and elastic repair fields in the monitor parameter form.
- [x] Pass strict schema and elastic repair parameters through `_build_runner_command`.
- [x] Document usage, thresholds, repair behavior, and historical source configs.
