# Competition Board Forecast Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add next-day leaderboard forecasting to the competition board page using recent daily leaderboard changes plus a user-provided next-day volume estimate.

**Architecture:** Extend `competition_board.py` with a forecast calculator and JSON API, then wire a forecast card into the existing `COMPETITION_BOARD_PAGE` UI. Reuse current snapshot, trend, and entry projection logic rather than introducing a new service.

**Tech Stack:** Python stdlib, existing `competition_board.py` data model, existing HTTP server in `web.py`, vanilla JS in the embedded page, pytest/unittest.

---

### Task 1: Forecast calculator tests

**Files:**
- Modify: `/Volumes/WORK/binance/wangge/tests/test_competition_board.py`
- Modify: `/Volumes/WORK/binance/wangge/src/grid_optimizer/competition_board.py`

- [ ] Add failing tests for computing next-day forecast from synthetic trend points.
- [ ] Add failing tests for projecting an entered user into forecast scenarios.
- [ ] Run targeted pytest and verify failure is due to missing forecast functions.

### Task 2: Back-end forecast model

**Files:**
- Modify: `/Volumes/WORK/binance/wangge/src/grid_optimizer/competition_board.py`
- Test: `/Volumes/WORK/binance/wangge/tests/test_competition_board.py`

- [ ] Implement forecast helpers for weighted daily deltas, end-of-competition multipliers, and scenario expansion.
- [ ] Build predicted cutoff values for total volume and key ranks.
- [ ] Reuse `_entry_projection()` with a synthetic predicted board.
- [ ] Run targeted pytest and verify green.

### Task 3: Forecast API

**Files:**
- Modify: `/Volumes/WORK/binance/wangge/src/grid_optimizer/web.py`
- Test: `/Volumes/WORK/binance/wangge/tests/test_web_security.py`

- [ ] Add a POST API endpoint that accepts `board_key`, `next_day_volume`, and optional `entry_id` / `name`.
- [ ] Return forecast payload for conservative/base/aggressive scenarios.
- [ ] Add a failing API test first, then implement until green.

### Task 4: Competition board UI

**Files:**
- Modify: `/Volumes/WORK/binance/wangge/src/grid_optimizer/competition_board.py`

- [ ] Add a “次日榜单预测” card below entry management.
- [ ] Populate board options and optional entry options from current snapshot.
- [ ] Submit forecast requests and render scenario results inline.
- [ ] Show predicted total, cutoff lines, user projected rank, reward band, and next-gap text.

### Task 5: Real-data validation

**Files:**
- Modify: `/Volumes/WORK/binance/wangge/output/` (generated artifacts only)

- [ ] Run the forecast API or helper against current `OPN` and `ROBO` board history data.
- [ ] Save validation JSON snapshots under `/Volumes/WORK/binance/wangge/output/`.
- [ ] Summarize observed behavior and any caveats in the final report.
