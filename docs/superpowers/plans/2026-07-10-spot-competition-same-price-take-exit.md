# Spot Competition Same-Price Take Exit Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Recycle a `spot_competition_synthetic_neutral_grid` maker BUY fill through a spot `LIMIT` + `IOC` SELL at the fill price when the current best bid can absorb it.

**Architecture:** Keep a bounded list of processed maker-BUY trade IDs in strategy state. A helper filters only tracked BUY fills, verifies price, visible bid quantity and exchange minimums, then invokes the spot-only market-order wrapper. It runs after trade synchronization and before maker-grid reconciliation; other strategy modes and all futures paths remain untouched.

**Tech Stack:** Python, Binance spot REST wrapper, `unittest`, existing runner state persistence.

---

### Task 1: Define the behavior with failing tests

**Files:**
- Modify: `tests/test_spot_loop_runner.py`
- Modify: `src/grid_optimizer/spot_loop_runner.py`

- [ ] Add tests for a known synthetic-neutral maker BUY fill at `1.62`, current bid `1.62`, visible bid quantity above the fill quantity, and a callback recording one `SELL` of the fill quantity.
- [ ] Add guard tests that require no callback when the current bid is below the fill price, visible bid quantity is smaller than the fill quantity, the fill is not a tracked BUY, or its trade ID was already processed.
- [ ] Run `PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 .venv/bin/python -m pytest tests/test_spot_loop_runner.py -k same_price_take_exit -v` and confirm collection fails because the helper is absent.

### Task 2: Implement the stateful spot-only helper

**Files:**
- Modify: `src/grid_optimizer/spot_loop_runner.py`
- Test: `tests/test_spot_loop_runner.py`

- [ ] Add `_run_same_price_take_exits` beside the synthetic-neutral trade functions. It must resolve `trade.orderId` through `state["known_orders"]`, accept only `side == "BUY"`, quantize with `_round_order_qty`, enforce `_spot_order_meets_exchange_mins`, and retain at most 5,000 processed IDs.
- [ ] Mark a trade ID processed only after the market-sell callback returns successfully; record skipped and submitted quantities for the cycle summary.
- [ ] Run the focused tests again and confirm they pass.

### Task 3: Wire it into the synthetic-neutral cycle

**Files:**
- Modify: `src/grid_optimizer/spot_loop_runner.py`
- Modify: `tests/test_spot_loop_runner.py`

- [ ] Add a failing `_run_cycle` test that mocks a tracked maker BUY fill and a fresh book with same-price, sufficient best-bid liquidity. It must assert one `post_spot_order` call using `side="SELL"`, `order_type="LIMIT"`, `time_in_force="IOC"`, the fill price, and the actual fill quantity; assert no futures API call.
- [ ] After synthetic-neutral trade synchronization and before `diff_open_orders`, fetch a fresh spot book ticker and invoke the helper only when `args.apply` is true. In dry-run, return diagnostic eligibility without submitting.
- [ ] Reuse existing `post_spot_order(..., order_type="LIMIT", time_in_force="IOC")`; do not cancel or alter unrelated grid orders before the exit.
- [ ] Run the new cycle test and `PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 .venv/bin/python -m pytest tests/test_spot_loop_runner.py -k 'same_price_take_exit or synthetic_neutral' -v`.

### Task 4: Verify and deploy safely

**Files:**
- Modify: `docs/SPOT_COMPETITION_PLAYBOOK.md`

- [ ] Document that this path is synthetic-neutral-only, requires best bid at or above the maker fill price and best-bid quantity at or above the sell quantity, and still incurs taker fees.
- [ ] Run `PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 .venv/bin/python -m pytest tests/test_spot_loop_runner.py -v`.
- [ ] Review the existing dirty worktree, stage only the scoped files, commit and push `main` only if the user authorizes the production deployment.
- [ ] Use the installed pull-based update wrappers and saved-runner wrappers to update/restart only `GRAMUSDT` on 114 and 150; verify the remote commit, live runner command, exactly-once SELL execution, and no futures order or position change.
