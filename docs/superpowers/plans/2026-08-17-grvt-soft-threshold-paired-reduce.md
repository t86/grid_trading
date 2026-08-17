# GRVT Soft-Threshold Paired Reduce Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make GRVT ordinary inventory recover below its soft threshold with bounded, simultaneous best-bid/best-ask reduce-only orders, while preserving profitable bilateral releases and frozen-position isolation.

**Architecture:** Extend the existing active-pair threshold reducer with an opt-in paired-threshold mode. The mode keeps the current single-heavy-side behavior as the default, but GRVT enables paired LONG/SHORT reduce orders, a below-threshold completion condition, and a 60-second rearm cooldown that suppresses only the currently heavy entry side. Existing ordinary/frozen position separation remains upstream and unchanged.

**Tech Stack:** Python 3, pytest, existing `grid_optimizer.loop_runner` strategy pipeline, saved runner deployment wrappers.

---

## Task 1: Lock the paired-threshold behavior with failing unit tests

**Files:**
- Modify: `tests/test_loop_runner.py`
- Reference: `src/grid_optimizer/loop_runner.py:24125`

- [ ] Add a test where only ordinary LONG exceeds the soft threshold but opt-in paired mode creates both `SELL LONG reduce-only` at best ask and `BUY SHORT reduce-only` at best bid.
- [ ] Assert each planned side is capped at 100U notional and never exceeds its ordinary reducible quantity.
- [ ] Add a test proving the pair remains active when a first 100U cycle leaves one side slightly above the threshold, then completes only after both ordinary sides are below the threshold.
- [ ] Run the focused tests and confirm they fail because paired-threshold mode is not implemented:

```bash
.venv/bin/python -m pytest tests/test_loop_runner.py -k 'paired_threshold' -q
```

## Task 2: Implement opt-in paired threshold reduction

**Files:**
- Modify: `src/grid_optimizer/loop_runner.py:24125`
- Test: `tests/test_loop_runner.py`

- [ ] Add a default-false `pair_all_sides_on_threshold` argument to `apply_best_quote_active_pair_reduce` so all non-GRVT callers retain current behavior.
- [ ] Keep the actual threshold-trigger sides separate from the sides selected for paired orders.
- [ ] Add a distinct persisted mode key for paired threshold state so stale single-side memory is safely reset.
- [ ] When either ordinary side exceeds the soft threshold, select both ordinary sides for bounded paired reduction.
- [ ] Compute targets so each cycle reduces at most 100U per side and the heavy side cannot report completion while still above the soft threshold; retain the exchange minimum-notional guard.
- [ ] Define paired-mode completion as both current ordinary sides being at or below the configured soft threshold.
- [ ] Run focused and existing active-pair tests:

```bash
.venv/bin/python -m pytest tests/test_loop_runner.py -k 'best_quote_active_pair_reduce or threshold_mode' -q
```

## Task 3: Add cooldown and imbalance-entry behavior

**Files:**
- Modify: `tests/test_loop_runner.py`
- Modify: `src/grid_optimizer/loop_runner.py:33730`

- [ ] Add a failing test for the 60-second paired-mode cooldown: while a side remains above threshold, its new entry is removed, but the lighter side entry remains eligible for existing guards.
- [ ] In paired cooldown handling, suppress only the currently over-threshold entry side rather than both historically paired sides.
- [ ] Update only the GRVT bounded-loss-recovery call site to enable paired threshold mode, use a 60-second rearm cooldown, and disable immediate rearm.
- [ ] Preserve normal profitable bilateral reduce orders when neither side exceeds the threshold.
- [ ] Run the focused tests:

```bash
.venv/bin/python -m pytest tests/test_loop_runner.py -k 'paired_threshold or grvt_bounded_loss_recovery or profitable' -q
```

## Task 4: Verify safety and regression boundaries

**Files:**
- Test: `tests/test_loop_runner.py`
- Test: existing GRVT/frozen integration tests discovered by pytest collection

- [ ] Run the complete loop-runner test file.
- [ ] Run the existing GRVT frozen-ledger/order-submit tests to confirm ordinary paired orders cannot consume frozen quantities.
- [ ] Confirm target and risk constants are unchanged: 150,000U target, cumulative cap, ordinary 1,000U hard cap, loss cap, and all frozen ledger/release settings.
- [ ] Inspect the diff for unrelated formatting or behavior changes.

```bash
.venv/bin/python -m pytest tests/test_loop_runner.py -q
git diff --check
git diff -- src/grid_optimizer/loop_runner.py tests/test_loop_runner.py
```

## Task 5: Commit, push, deploy, and verify production

**Files:**
- Commit only: `src/grid_optimizer/loop_runner.py`, `tests/test_loop_runner.py`, this plan if not already committed
- Production: `/home/ubuntu/wangge` on 111 and 114

- [ ] Commit the verified implementation on local `main`, push `main`, and confirm both servers can fast-forward from the pushed commit using the server-local pull/update wrapper.
- [ ] Back up each server's runner control before enabling the new path.
- [ ] Enable the existing active-pair/loss-reduce controls on both servers without changing protected targets, caps, or frozen parameters.
- [ ] Restart with `/usr/local/bin/grid-saved-runner restart GRVTUSDT`; verify both required services and `grid-web` are active.
- [ ] Verify fresh best-quote paired reduce-only orders, real fills, ordinary/frozen attribution, and that each side stays within the 100U-per-cycle boundary.
- [ ] Recheck 15-minute/60-minute volume and loss-reduction frequency; immediately disable the path again if wrong-side reduction, frozen contamination, sub-60-second churn, or a protected-limit change appears.

```bash
git status --short
git log -1 --oneline
# production verification uses read-only SSH checks plus saved runner wrappers
```
