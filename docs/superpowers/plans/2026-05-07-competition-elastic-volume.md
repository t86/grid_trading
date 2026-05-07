# Competition Elastic Volume Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `competition_elastic_volume_v1` and a local read-only competition volume monitor so each server can adapt contract volume trading by loss budget and inspect live fills, cumulative volume, PnL, fees, and prices.

**Architecture:** Add a pure strategy-control module with deterministic metrics, state transitions, and parameter scaling. Wire `loop_runner` to apply the controller before plan generation and emit `elastic_volume` events. Add local Web APIs and a dashboard page that read existing audit/event files for one server and one selected symbol.

**Tech Stack:** Python standard library, unittest, existing `grid_optimizer.loop_runner`, `grid_optimizer.web`, JSONL audit files, vanilla HTML/CSS/JS.

---

### Task 1: Pure Elastic Controller

**Files:**
- Create: `src/grid_optimizer/competition_elastic_volume.py`
- Create: `tests/test_competition_elastic_volume.py`

- [ ] Write failing tests for disabled behavior, sprint, cruise, defensive, recover, cooldown, cooldown persistence, and inventory-over-bias priority.
- [ ] Run `PYTHONPATH=src python3 -m unittest tests.test_competition_elastic_volume`.
- [ ] Implement `ElasticVolumeConfig`, `ElasticVolumeInputs`, `resolve_elastic_volume_control()`, and helper functions.
- [ ] Run `PYTHONPATH=src python3 -m unittest tests.test_competition_elastic_volume`.

### Task 2: Runner Wiring

**Files:**
- Modify: `src/grid_optimizer/loop_runner.py`
- Modify: `src/grid_optimizer/web.py`
- Test: `tests/test_loop_runner.py`
- Test: `tests/test_web_security.py`

- [ ] Write failing tests that runner config normalization accepts elastic fields and that plan/event summaries include an `elastic_volume` object when enabled.
- [ ] Run focused tests and verify they fail for missing elastic support.
- [ ] Add CLI/config fields, validation, control resolution, state persistence, effective parameter application, and event summary fields.
- [ ] Run focused loop/web tests.

### Task 3: Local Competition Volume Monitor API

**Files:**
- Modify: `src/grid_optimizer/web.py`
- Create: `tests/test_competition_volume_monitor.py`

- [ ] Write failing tests for `/api/competition-volume/symbols`, summary, fills, and chart helper functions using temporary audit JSONL files.
- [ ] Run `PYTHONPATH=src python3 -m unittest tests.test_competition_volume_monitor`.
- [ ] Implement local symbol discovery and read-only summary/fill/chart builders.
- [ ] Wire `_Handler.do_GET` routes for the API.
- [ ] Run monitor API tests.

### Task 4: Dashboard UI

**Files:**
- Modify: `src/grid_optimizer/web.py`
- Test: `tests/test_web_security.py`

- [ ] Write failing tests that the running status page or new dashboard page includes symbol selector, cycle volume, cycle PnL, fees, live fills, and chart endpoint references.
- [ ] Implement the local dashboard HTML, CSS, and JS with server label and symbol dropdown only.
- [ ] Run focused web tests.

### Task 5: Verification

**Files:**
- All touched files

- [ ] Run `PYTHONPATH=src python3 -m unittest tests.test_competition_elastic_volume tests.test_competition_volume_monitor tests.test_loop_runner tests.test_web_security`.
- [ ] Run `git diff --check`.
- [ ] Review `git diff` to ensure unrelated existing changes are not reverted.
