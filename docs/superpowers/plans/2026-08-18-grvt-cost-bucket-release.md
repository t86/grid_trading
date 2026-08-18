# GRVT Cost-Bucket Release Guard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prevent ordinary same-cost inventory from being reduced repeatedly without a matching refill, forbid below-soft ordinary reductions with negative aggregate realized PnL, and retain bounded pressure-side loss reduction above the soft threshold.

**Architecture:** Keep the existing four-leg planner and ordinary/frozen ledgers. Add a small ordinary profit-release lease record to `best_quote_volume_ledger`, carry its identity through order metadata and order refs, and settle it from execution events/trade sync. Reuse only the Hedge Mode close-capacity semantics from `89e50bfa`; do not merge the ARX recovery coordinator.

**Tech Stack:** Python 3, dataclasses, JSON runner state, Binance Hedge Mode order metadata, unittest/pytest.

---

## File map

- `src/grid_optimizer/best_quote_maker_volume.py`: choose an eligible cost bucket, apply ordinary aggregate no-loss gating, and attach a deterministic release lease to the planned order.
- `src/grid_optimizer/loop_runner.py`: persist/settle leases, expose locked/active cost buckets to the planner, and keep normal/frozen ownership isolated.
- `src/grid_optimizer/submit_plan.py`: count all Hedge Mode close orders against available side capacity, including exchange orders whose `reduceOnly` field is false.
- `tests/test_best_quote_maker_volume.py`: planner RED/GREEN tests for bucket watermarks and aggregate no-loss gating.
- `tests/test_loop_runner.py`: lease lifecycle and ledger settlement tests.
- `tests/test_submit_plan.py`: Hedge Mode exchange-capacity regression.

### Task 1: Hedge Mode close-order capacity

**Files:**
- Modify: `src/grid_optimizer/submit_plan.py:503-670`
- Test: `tests/test_submit_plan.py:2180-2225`

- [ ] **Step 1: Write the failing Hedge capacity test**

Add:

```python
def test_reduce_only_cap_hedge_counts_existing_close_order_without_reduce_only_flag(self) -> None:
    capped = cap_reduce_only_place_orders_to_position(
        actions={
            "place_orders": [{
                "side": "BUY",
                "position_side": "SHORT",
                "price": 2.0,
                "qty": 0.6,
                "notional": 1.2,
                "role": "best_quote_reduce_short",
                "force_reduce_only": True,
            }],
            "cancel_orders": [],
        },
        strategy_mode="hedge_best_quote_maker_volume_v1",
        current_actual_net_qty=0.0,
        current_hedge_long_qty=0.0,
        current_hedge_short_qty=1.0,
        current_open_orders=[{
            "side": "BUY",
            "positionSide": "SHORT",
            "origQty": "0.7",
            "executedQty": "0",
            "reduceOnly": False,
        }],
    )
    self.assertEqual(capped["place_orders"][0]["qty"], 0.3)
```

- [ ] **Step 2: Run RED**

Run:

```bash
.venv/bin/python -m pytest -q tests/test_submit_plan.py::SubmitPlanTests::test_reduce_only_cap_hedge_counts_existing_close_order_without_reduce_only_flag
```

Expected: FAIL because the existing Hedge close order is ignored and the new quantity remains `0.6`.

- [ ] **Step 3: Implement Hedge close recognition**

Change `_is_displaceable_reduce_only_open_order` to accept `require_reduce_only`. In `cap_reduce_only_place_orders_to_position`, derive existing close side from `(positionSide=LONG, side=SELL)` or `(positionSide=SHORT, side=BUY)` in Hedge Mode, without requiring `reduceOnly=true`. Preserve the old requirement in one-way mode.

- [ ] **Step 4: Run GREEN and adjacent cap tests**

Run:

```bash
.venv/bin/python -m pytest -q tests/test_submit_plan.py -k 'reduce_only_cap or urgent_hedge_reduce'
```

Expected: all selected tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/grid_optimizer/submit_plan.py tests/test_submit_plan.py
git commit -m "fix: count hedge close orders against reduce capacity"
```

### Task 2: Cost-bucket eligibility and aggregate no-loss gate

**Files:**
- Modify: `src/grid_optimizer/best_quote_maker_volume.py:100-135,2030-2250`
- Test: `tests/test_best_quote_maker_volume.py:259-430`

- [ ] **Step 1: Write failing planner tests**

Add two tests:

```python
def test_four_leg_profit_reduce_blocks_locked_bucket_without_new_entry(self) -> None:
    inputs = _inputs(
        bid_price=100.0,
        ask_price=100.1,
        mid_price=100.05,
        cycle_budget_notional=800.0,
        entry_ladder_spacing=0.1,
        tick_size=0.1,
        position_side_mode="hedge",
        current_long_qty=5.0,
        current_short_qty=0.0,
        current_long_avg_price=99.5,
        current_long_lots=[{
            "qty": 3.0,
            "price": 99.5,
            "opened_at": "2026-08-18T00:00:01+00:00",
            "role": "best_quote_entry_long",
            "ordinary_profit_release_locked": True,
        }],
    )
    plan = build_best_quote_maker_volume_plan(config=_four_leg_config(), inputs=inputs)
    self.assertNotIn("best_quote_reduce_long", {o["role"] for o in plan["sell_orders"]})
    self.assertEqual(
        plan["metrics"]["four_leg_cycle"]["roles"]["long_profit_reduce"]["reason"],
        "no_new_entry_since_profit_release",
    )


def test_four_leg_profit_reduce_blocks_lot_profit_below_aggregate_cost(self) -> None:
    inputs = _inputs(
        bid_price=100.0,
        ask_price=100.1,
        mid_price=100.05,
        cycle_budget_notional=800.0,
        entry_ladder_spacing=0.1,
        tick_size=0.1,
        position_side_mode="hedge",
        current_long_qty=5.0,
        current_short_qty=0.0,
        current_long_avg_price=101.0,
        current_long_lots=[{
            "qty": 1.0,
            "price": 99.5,
            "opened_at": "2026-08-18T00:00:02+00:00",
            "role": "best_quote_entry_long",
        }],
    )
    plan = build_best_quote_maker_volume_plan(config=_four_leg_config(), inputs=inputs)
    self.assertNotIn("best_quote_reduce_long", {o["role"] for o in plan["sell_orders"]})
    self.assertEqual(
        plan["metrics"]["four_leg_cycle"]["roles"]["long_profit_reduce"]["reason"],
        "aggregate_no_loss_blocked",
    )
```

Use an existing local config constructor instead of adding a production helper if `_four_leg_config` does not already exist.

- [ ] **Step 2: Run RED**

Run:

```bash
.venv/bin/python -m pytest -q \
  tests/test_best_quote_maker_volume.py::BestQuoteMakerVolumeTests::test_four_leg_profit_reduce_blocks_locked_bucket_without_new_entry \
  tests/test_best_quote_maker_volume.py::BestQuoteMakerVolumeTests::test_four_leg_profit_reduce_blocks_lot_profit_below_aggregate_cost
```

Expected: the locked lot is still selected and the below-aggregate-cost order is still planned.

- [ ] **Step 3: Implement planner filtering and gate**

For `best_quote_entry_long/short` lots, exclude `ordinary_profit_release_locked=true` when creating a new release. Keep locked lots visible only for an already-open order of the same role/target so a partially filled active lease is not replaced by a second lease.

Before building a new order require:

```python
aggregate_no_loss = (
    price >= current_long_avg_price - 1e-12
    if position_side == "LONG"
    else price <= current_short_avg_price + 1e-12
)
```

This branch already runs only below soft threshold; when false, remove the ordinary profit order and report `aggregate_no_loss_blocked`. Add order metadata:

```python
{
    "ordinary_profit_release_bucket_side": position_side,
    "ordinary_profit_release_bucket_price": position_cost,
    "ordinary_profit_release_entry_cutoff_at": max(opened_at),
    "ordinary_profit_release_lease_id": deterministic_lease_id,
    "ordinary_profit_release_authorized_qty": order["qty"],
}
```

The deterministic ID must depend only on side, normalized cost price, and cutoff, so cancel/reprice retains the same identity.

- [ ] **Step 4: Run GREEN and four-leg suite**

```bash
.venv/bin/python -m pytest -q tests/test_best_quote_maker_volume.py -k 'four_leg'
```

Expected: all four-leg tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/grid_optimizer/best_quote_maker_volume.py tests/test_best_quote_maker_volume.py
git commit -m "fix: gate GRVT profit exits by cost bucket"
```

### Task 3: Persist and settle profit-release leases

**Files:**
- Modify: `src/grid_optimizer/loop_runner.py:3390-4320,10390-10550,32090-32220,38690-38780`
- Test: `tests/test_loop_runner.py:4270-4350`

- [ ] **Step 1: Write failing lease lifecycle tests**

Add tests around a state containing ordinary long lots at the same cost:

```python
def test_profit_release_fill_locks_only_entries_at_or_before_lease_cutoff(self) -> None:
    state = {
        "best_quote_volume_order_refs": {
            "101": {
                "book": "normal",
                "role": "best_quote_reduce_long",
                "ordinary_profit_release_lease_id": "LONG:99.5:cutoff-1",
                "ordinary_profit_release_bucket_side": "LONG",
                "ordinary_profit_release_bucket_price": 99.5,
                "ordinary_profit_release_entry_cutoff_at": "2026-08-18T00:00:02+00:00",
                "ordinary_profit_release_authorized_qty": 1.0,
            }
        },
        "best_quote_volume_ledger": {
            "initialized": True,
            "long_lots": [
                {"qty": 1.0, "price": 99.5, "opened_at": "2026-08-18T00:00:01+00:00", "role": "best_quote_entry_long"},
                {"qty": 1.0, "price": 99.5, "opened_at": "2026-08-18T00:00:03+00:00", "role": "best_quote_entry_long"},
            ],
            "short_lots": [],
        },
    }
    result = _apply_ordinary_profit_release_execution_events(
        state=state,
        execution_events=[{
            "kind": "ORDER_FILLED",
            "order_id": 101,
            "cumulative_filled_qty": 1.0,
        }],
    )
    self.assertTrue(result["settled"])
    self.assertTrue(state["best_quote_volume_ledger"]["long_lots"][0]["ordinary_profit_release_locked"])
    self.assertNotIn("ordinary_profit_release_locked", state["best_quote_volume_ledger"]["long_lots"][1])
```

Also test:

- `ORDER_CANCELED` with zero fill leaves the bucket unlocked;
- `ORDER_PARTIALLY_FILLED` keeps the lease active;
- a later `ORDER_CANCELED` with positive cumulative fill locks the pre-cutoff lots;
- frozen refs are ignored.

- [ ] **Step 2: Run RED**

```bash
.venv/bin/python -m pytest -q tests/test_loop_runner.py -k 'ordinary_profit_release'
```

Expected: import/name failure because the event settler does not exist.

- [ ] **Step 3: Persist metadata in order refs**

Extend `_update_best_quote_volume_order_refs_unlocked` to copy only the five `ordinary_profit_release_*` fields from accepted ordinary orders into their order ref. Do not copy them for frozen orders.

- [ ] **Step 4: Implement event settlement**

Add `_apply_ordinary_profit_release_execution_events(state, execution_events)`:

- resolve exact order ref by `order_id`;
- ignore non-normal books and non-profit-release refs;
- store `ordinary_profit_release_leases[lease_id]` under `best_quote_volume_ledger`;
- update `filled_qty` using the maximum cumulative fill, never by summing repeated stream events;
- keep status active for NEW/PARTIAL;
- remove a zero-fill canceled/rejected lease without locking;
- for FILLED, or canceled/expired with positive fill, mark matching side/cost lots with `opened_at <= cutoff` as `ordinary_profit_release_locked=true` and record the lease ID;
- leave later matching entries unlocked;
- make replay idempotent.

Call it immediately after `_record_loss_reduce_reentry_fill_guard` while holding the state JSON lock internally. Store its audit report in latest submit when it changes state.

- [ ] **Step 5: Run GREEN and ledger tests**

```bash
.venv/bin/python -m pytest -q tests/test_loop_runner.py -k 'best_quote_volume_ledger or ordinary_profit_release or loss_reduce_reentry'
```

Expected: all selected tests PASS.

- [ ] **Step 6: Commit**

```bash
git add src/grid_optimizer/loop_runner.py tests/test_loop_runner.py
git commit -m "fix: persist GRVT profit release leases"
```

### Task 4: Verify threshold-only loss fallback

**Files:**
- Modify only if RED proves a gap: `src/grid_optimizer/best_quote_maker_volume.py`, `src/grid_optimizer/loop_runner.py`
- Test: `tests/test_best_quote_maker_volume.py`, `tests/test_loop_runner.py`

- [ ] **Step 1: Add pressure-boundary regressions**

Assert all of the following with real planner functions:

```python
# Below soft: aggregate-negative lot exit absent; no active-pair loss order.
# Long over soft: only SELL/LONG active-pair reduce exists, <= 100U.
# Short over soft: only BUY/SHORT active-pair reduce exists, <= 100U.
# Returning below soft removes the loss order on the next plan.
```

Use unique order IDs when testing cooldown state, and assert an independent same-side loss order inside 60 seconds is rejected.

- [ ] **Step 2: Run RED or prove current behavior**

```bash
.venv/bin/python -m pytest -q tests/test_best_quote_maker_volume.py tests/test_loop_runner.py -k 'soft_threshold or active_pair or pressure_reduce or loss_reduce_reentry'
```

Expected: if existing behavior already satisfies the assertions, retain it without production edits. If a new test fails, implement only the failing boundary.

- [ ] **Step 3: Run GREEN**

Repeat the selected command and expect all tests PASS.

- [ ] **Step 4: Commit only if production code changed**

```bash
git add src/grid_optimizer/best_quote_maker_volume.py src/grid_optimizer/loop_runner.py tests/test_best_quote_maker_volume.py tests/test_loop_runner.py
git commit -m "fix: constrain GRVT loss fallback to pressure side"
```

### Task 5: Full verification and 114 canary preparation

**Files:**
- Verify all files changed by Tasks 1-4

- [ ] **Step 1: Run focused suites**

```bash
.venv/bin/python -m pytest -q \
  tests/test_best_quote_maker_volume.py \
  tests/test_submit_plan.py \
  tests/test_loop_runner.py
```

Expected: no new failures; document any unrelated pre-existing baseline failures separately.

- [ ] **Step 2: Run static checks**

```bash
git diff --check c9d9c73c...HEAD
git status --short
```

Expected: only intentional files plus the pre-existing untracked `.playwright-cli/`.

- [ ] **Step 3: Replay the observed 114 failure shape**

Build a test fixture with multiple same-cost entry lots, one completed release, no matching refill, and a second planner cycle. Assert the second cycle has no new release. Add one matching refill and assert only the refill quantity becomes eligible.

- [ ] **Step 4: Push the experiment branch**

```bash
git push -u origin codex/grvt-entry-lot-profit-guard
```

- [ ] **Step 5: Pull-based 114 deployment only after local verification**

On 114, verify no tracked dirty files, fetch the experiment branch, switch the production checkout to the exact pushed commit, and restart only through:

```bash
/usr/local/bin/grid-saved-runner restart GRVTUSDT
```

Never copy files with scp/rsync. Verify commit, service, control, latest plan, ordinary/frozen attribution, active orders, and actual fills.

- [ ] **Step 6: Canary acceptance**

Do not copy to 111 until one complete 114 target window proves:

- target completion and speed not below 111;
- clean ordinary share at least 80%;
- loss per 10k not above 111;
- no below-soft negative-realized ordinary reduce;
- no second same-price release without matching refill;
- every authorized loss order is on the pressure side, no more than about 100U, and at least 60 seconds from the prior same-side loss order;
- frozen ledger remains synchronized and untouched by ordinary flow.
