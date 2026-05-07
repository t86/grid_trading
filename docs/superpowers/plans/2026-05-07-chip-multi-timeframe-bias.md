# CHIP Multi-Timeframe Bias Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an optional 1m/15m/1h/4h K-line bias layer for synthetic-neutral runners so CHIP can favor low buy-long entries and high sell-short entries while controlling loss per 10k volume.

**Architecture:** Put deterministic scoring and parameter adjustment in a small pure module, then wire the runner to fetch K-lines, apply the bias to effective args, and expose the report. Default is disabled, so existing symbols are unchanged unless their saved config enables it.

**Tech Stack:** Python, `grid_optimizer.loop_runner`, Binance futures K-line fetcher, unittest.

---

### Task 1: Pure Multi-Timeframe Bias Module

**Files:**
- Create: `src/grid_optimizer/multi_timeframe_bias.py`
- Test: `tests/test_multi_timeframe_bias.py`

- [ ] Write failing tests for low-zone long bias, high-zone short bias, and fast-shock throttling.
- [ ] Implement candle-window metrics, score blending, and effective parameter calculation.
- [ ] Run `PYTHONPATH=src python3 -m unittest tests.test_multi_timeframe_bias`.

### Task 2: Runner Wiring

**Files:**
- Modify: `src/grid_optimizer/loop_runner.py`
- Test: `tests/test_loop_runner.py`

- [ ] Add parser flags, namespace plumbing, and non-negative validation.
- [ ] Fetch 1m/15m/1h/4h closed windows when enabled.
- [ ] Apply adjusted `buy_levels`, `sell_levels`, `per_order_notional`, `step_price`, pause scales, and market-bias offsets before plan build.
- [ ] Include `multi_timeframe_bias` in plan reports and event summaries.
- [ ] Run focused loop runner tests.

### Task 3: Production Config And Deploy

**Files:**
- Modify server saved control JSON for `CHIPUSDT` on 150 through existing runner controls.

- [ ] Enable the bias layer on 150 CHIP with conservative caps.
- [ ] Restart `grid-saved-runner-api2 CHIPUSDT`.
- [ ] Verify runner status, command line, latest plan report, 15m/60m volume, and loss per 10k.
