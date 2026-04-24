# Daily Strategy Panel Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a daily strategy reference panel to `/competition_board` using post-14:00 CST official leaderboard snapshots.

**Architecture:** Reuse the existing `competition_board_history` hourly snapshots and derive a compact daily strategy summary during snapshot analytics attachment. Keep the data read-only and display it in the existing competition board page without adding a second scheduler.

**Tech Stack:** Python `unittest`, existing `grid_optimizer.competition_board` module, static HTML/JS embedded in `COMPETITION_BOARD_PAGE`.

---

### Task 1: Daily Snapshot Analytics

**Files:**
- Modify: `src/grid_optimizer/competition_board.py`
- Test: `tests/test_competition_board.py`

- [ ] **Step 1: Add failing tests**

Add tests that create two history entries around 14:00 CST and verify the daily strategy summary chooses only the post-14:00 entry. Add a second test that verifies the summary is attached under `snapshot["daily_strategy"]`.

- [ ] **Step 2: Run focused tests to verify failure**

Run: `PYTHONPATH=src python3 -m unittest tests.test_competition_board`

Expected: tests fail because `_build_daily_strategy_analytics` does not exist.

- [ ] **Step 3: Implement daily strategy aggregation**

Add helpers in `competition_board.py` to select the latest post-14:00 CST history board per day, extract ranks 20/50/100/200/500, compute day-over-day deltas, and include an `arena` status placeholder for the futures master ranking.

- [ ] **Step 4: Verify focused tests pass**

Run: `PYTHONPATH=src python3 -m unittest tests.test_competition_board`

Expected: all competition board tests pass.

### Task 2: Web Panel Rendering

**Files:**
- Modify: `src/grid_optimizer/competition_board.py`

- [ ] **Step 1: Add page section**

Insert a “每日策略参考” card above the existing ongoing analytics section.

- [ ] **Step 2: Render daily strategy rows**

Add JavaScript rendering that shows date, source snapshot time, official update time, participants, total volume, rank thresholds, day-over-day deltas, and master arena status.

- [ ] **Step 3: Verify HTML compiles**

Run: `python3 -m py_compile src/grid_optimizer/competition_board.py`

Expected: exit code 0.

### Task 3: Verification And Deploy

**Files:**
- Modify: `src/grid_optimizer/competition_board.py`
- Modify: `tests/test_competition_board.py`

- [ ] **Step 1: Run regression tests**

Run:
`PYTHONPATH=src python3 -m unittest tests.test_competition_board`
`PYTHONPATH=src python3 -m unittest tests.test_symbol_lists tests.test_competition`
`python3 -m py_compile src/grid_optimizer/competition_board.py tests/test_competition_board.py`
`git diff --check`

- [ ] **Step 2: Commit and push**

Commit on `main` and push to `origin/main`.

- [ ] **Step 3: Deploy 114**

Run `/usr/local/bin/grid-web-update` on `srv-43-155-163-114`. Do not copy files manually.

- [ ] **Step 4: Verify 114**

Confirm `git rev-parse HEAD`, `grid-web.service` is active, `/api/health` returns ok, and `/api/competition_board` contains `daily_strategy`.
