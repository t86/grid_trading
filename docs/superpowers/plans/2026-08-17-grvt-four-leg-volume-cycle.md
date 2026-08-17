# GRVT Four-Leg Volume Cycle Implementation Plan

> **For Codex:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add an opt-in GRVT futures planner mode that keeps one ordinary entry and one cost-based profitable reduce order on each ordinary LONG/SHORT side, retains a smaller heavy-side entry under inventory bias, and leaves the existing paired soft-threshold loss-reduction path as the bounded escape valve.

**Architecture:** Keep all new planner behavior behind one default-off feature flag so the existing planner, active-pair loss reductions, frozen ledger, caps, and target logic remain byte-for-byte behaviorally unchanged when disabled. Build the four ordinary roles in `best_quote_maker_volume.py`; continue to let `loop_runner.py`'s existing active-pair guard replace ordinary work only when a side reaches its soft threshold. Expose role completeness in planner metrics, then validate 114 as canary against unchanged 111 before any merge to `main`.

**Tech Stack:** Python 3, pytest, argparse, systemd, SSH, existing `grid_optimizer` runner/web configuration.

---

## Non-negotiable invariants

- The feature is disabled by default and the disabled plan is identical to today's plan.
- Do not change the 150,000U target, `max_cumulative_notional`, 1,000U per-side hard cap, loss caps, frozen ledger, or frozen-release settings.
- Ordinary orders never consume frozen inventory. Frozen orders remain classified and accounted independently.
- A profitable LONG reduce is `reduce_only SELL` at or above `long_cost + step`; a profitable SHORT reduce is `reduce_only BUY` at or below `short_cost - step`.
- Below the soft threshold, the desired ordinary role set is `long_entry`, `long_profit_reduce`, `short_entry`, `short_profit_reduce`, except where an explicit cap/headroom/min-notional guard makes a role illegal.
- At or above the soft threshold, reuse the existing paired active-pair best-bid/best-ask loss-reduction lease. It remains capped near 100U per side and stops after the side falls below the soft threshold.
- Under inventory bias, preserve the light-side entry budget and restore a smaller heavy-side entry capped at 25% of the cycle budget and by remaining soft headroom.
- Do not merge to `main` until the 114 canary passes the acceptance gates at the end of this plan.

### Task 1: Add a default-off feature flag through every configuration boundary

**Files:**
- Modify: `src/grid_optimizer/best_quote_maker_volume.py`
- Modify: `src/grid_optimizer/loop_runner.py`
- Modify: `src/grid_optimizer/web.py`
- Modify: `tests/test_best_quote_maker_volume.py`
- Modify: `tests/test_loop_runner.py`
- Modify: `tests/test_web_security.py`

**Step 1: Write failing default-compatibility tests**

Add a planner test that constructs the same inputs twice—once with the existing default config and once with `four_leg_cycle_enabled=False`—and asserts complete equality:

```python
def test_four_leg_cycle_disabled_is_exactly_backward_compatible() -> None:
    default_cfg = BestQuoteMakerVolumeConfig(enabled=True)
    explicit_off_cfg = replace(default_cfg, four_leg_cycle_enabled=False)

    default_plan = build_best_quote_maker_volume_plan(config=default_cfg, **_inputs())
    explicit_off_plan = build_best_quote_maker_volume_plan(config=explicit_off_cfg, **_inputs())

    assert explicit_off_plan == default_plan
```

Add parser and web-command assertions:

```python
assert args.best_quote_maker_volume_four_leg_cycle_enabled is False
assert "--no-best-quote-maker-volume-four-leg-cycle-enabled" in command
```

**Step 2: Run the focused tests and confirm they fail**

Run:

```bash
.venv/bin/pytest -q \
  tests/test_best_quote_maker_volume.py \
  tests/test_loop_runner.py \
  tests/test_web_security.py \
  -k 'four_leg_cycle or best_quote_maker_volume_command'
```

Expected: failures because the dataclass field and CLI option do not exist.

**Step 3: Add the feature flag with a false default**

In `BestQuoteMakerVolumeConfig` add:

```python
four_leg_cycle_enabled: bool = False
```

In the runner parser add:

```python
parser.add_argument(
    "--best-quote-maker-volume-four-leg-cycle-enabled",
    action=argparse.BooleanOptionalAction,
    default=False,
)
```

Pass it into `BestQuoteMakerVolumeConfig`:

```python
four_leg_cycle_enabled=bool(
    args.best_quote_maker_volume_four_leg_cycle_enabled
),
```

Add `best_quote_maker_volume_four_leg_cycle_enabled: False` to the web defaults/profile, include it in the boolean configuration whitelist, and emit exactly one of these command arguments:

```python
"--best-quote-maker-volume-four-leg-cycle-enabled"
"--no-best-quote-maker-volume-four-leg-cycle-enabled"
```

**Step 4: Run focused tests**

Run the Step 2 command.

Expected: PASS, including the exact disabled-plan equality assertion.

**Step 5: Commit**

```bash
git add src/grid_optimizer/best_quote_maker_volume.py src/grid_optimizer/loop_runner.py src/grid_optimizer/web.py tests/test_best_quote_maker_volume.py tests/test_loop_runner.py tests/test_web_security.py
git commit -m "feat: gate four-leg volume cycle"
```

### Task 2: Generate cost-based profitable reduce orders for both ordinary sides

**Files:**
- Modify: `src/grid_optimizer/best_quote_maker_volume.py`
- Modify: `tests/test_best_quote_maker_volume.py`

**Step 1: Write failing planner tests**

Add tests with both ordinary sides below soft threshold and valid average costs. Assert all four roles and the reduce-order price/PnL direction:

```python
roles = {order["role"] for order in plan["orders"]}
assert {
    "best_quote_entry_long",
    "best_quote_reduce_long",
    "best_quote_entry_short",
    "best_quote_reduce_short",
} <= roles

long_reduce = next(order for order in plan["orders"] if order["role"] == "best_quote_reduce_long")
short_reduce = next(order for order in plan["orders"] if order["role"] == "best_quote_reduce_short")
assert long_reduce["reduce_only"] is True
assert long_reduce["side"] == "SELL"
assert Decimal(str(long_reduce["price"])) >= long_cost + step
assert short_reduce["reduce_only"] is True
assert short_reduce["side"] == "BUY"
assert Decimal(str(short_reduce["price"])) <= short_cost - step
```

Add a missing-cost test that asserts the feature does not invent a profitable reduce and records an explicit block reason:

```python
assert report["roles"]["long_profit_reduce"]["status"] == "blocked"
assert report["roles"]["long_profit_reduce"]["reason"] == "missing_position_cost"
```

**Step 2: Run the planner tests and confirm failure**

Run:

```bash
.venv/bin/pytest -q tests/test_best_quote_maker_volume.py -k 'four_leg_cycle'
```

Expected: the legacy planner lacks both cost-based reduce roles and completeness metrics.

**Step 3: Add isolated four-leg helpers**

Add helpers used only when the flag is true:

```python
def _four_leg_profit_price(
    *,
    position_side: str,
    position_cost: float,
    step_ratio: float,
    best_bid: float,
    best_ask: float,
    tick_size: float,
) -> float:
    if position_side == "LONG":
        raw_price = max(best_ask, position_cost * (1.0 + step_ratio))
        return _round_order_price(raw_price, tick_size, "SELL")
    raw_price = min(best_bid, position_cost * (1.0 - step_ratio))
    return _round_order_price(raw_price, tick_size, "BUY")
```

Add `_ensure_four_leg_profit_reduces(...)` after ordinary plan generation and before the existing active-pair runner guard. For each ordinary side:

1. Use only unconsumed `best_quote_entry_long` / `best_quote_entry_short` lots as profitable-release credit. Bootstrap, reconcile, and frozen lots are not credit.
2. Group the oldest eligible entry-price level and calculate its exit at entry cost ± step. A reduce fill consumes these eligible lots before historical inventory, so the same cost/price level cannot be released repeatedly without a new entry fill.
3. Retain at least `max(min_cycle_budget_notional * 0.5, exchange_min_notional)` of ordinary inventory on each side; remove an oversized pre-existing reduce instead of allowing it to bypass the floor.
4. Size the reduce by the smallest of eligible lot quantity, remaining inventory above the retained floor, and half the cycle budget; apply existing quantity and min-notional checks.
5. If no eligible lot exists, record `no_unreleased_entry_lot`; if the side is at the floor, record `retained_inventory_floor`.
6. Build `best_quote_reduce_long` or `best_quote_reduce_short` with `reduce_only=True` and ordinary ownership metadata.

Expose:

```python
metrics["four_leg_cycle"] = {
    "enabled": True,
    "roles": role_report,
    "complete": all(item["status"] in {"planned", "not_applicable"} for item in role_report.values()),
}
```

When disabled, do not add this metric and do not call any new helper.

**Step 4: Run the focused tests**

Run the Step 2 command.

Expected: PASS for four-role presence, reduce-only ownership, cost ± step prices, and explicit blocked reasons.

**Step 5: Commit**

```bash
git add src/grid_optimizer/best_quote_maker_volume.py tests/test_best_quote_maker_volume.py
git commit -m "feat: add cost-based ordinary profit exits"
```

### Task 3: Keep a smaller heavy-side entry instead of deleting it under bias

**Files:**
- Modify: `src/grid_optimizer/best_quote_maker_volume.py`
- Modify: `tests/test_best_quote_maker_volume.py`

**Step 1: Write failing bias and headroom tests**

Create a biased LONG case below soft threshold. Assert both entries remain, the heavy LONG entry is smaller, and neither crosses soft headroom:

```python
long_entries = [order for order in plan["orders"] if order["role"] == "best_quote_entry_long"]
short_entries = [order for order in plan["orders"] if order["role"] == "best_quote_entry_short"]
assert long_entries
assert short_entries
assert sum(order["notional"] for order in long_entries) < sum(order["notional"] for order in short_entries)
assert sum(order["notional"] for order in long_entries) <= long_soft_headroom
```

Add a case where heavy-side headroom is below exchange minimum. Assert the role is explicitly blocked rather than silently missing:

```python
assert report["roles"]["long_entry"] == {
    "status": "blocked",
    "reason": "soft_headroom_below_min_notional",
}
```

Keep the existing flag-off test `test_inventory_bias_stops_heavy_side_and_caps_light_side_at_soft_headroom` unchanged and passing.

**Step 2: Run the bias tests and confirm failure**

Run:

```bash
.venv/bin/pytest -q tests/test_best_quote_maker_volume.py -k 'four_leg_cycle and bias'
```

Expected: the heavy-side entry is currently absent.

**Step 3: Add the feature-only heavy-entry backfill**

After the legacy bias block has built its plan, call `_ensure_four_leg_entries(...)` only when the feature is enabled. If a side's entry role is missing and the side is below soft threshold:

```python
heavy_budget = min(
    cycle_budget_notional * 0.25,
    max(soft_limit_notional - current_side_notional, 0.0),
)
```

Use the existing entry price and anti-chase logic for that side, keep the light-side orders unchanged, and add at most one heavy-side entry. If the calculated budget is below exchange minimum, record the explicit headroom block reason. Do not backfill an entry at or above the soft threshold.

Update `metrics["four_leg_cycle"]["roles"]` after both profit-reduce and entry helpers run so its status reflects the final planner output.

**Step 4: Run planner tests**

Run:

```bash
.venv/bin/pytest -q tests/test_best_quote_maker_volume.py
```

Expected: all legacy flag-off and new flag-on planner tests pass.

**Step 5: Commit**

```bash
git add src/grid_optimizer/best_quote_maker_volume.py tests/test_best_quote_maker_volume.py
git commit -m "feat: retain bounded heavy-side volume entry"
```

### Task 4: Verify the existing soft-threshold active-pair path remains the only loss escape valve

**Files:**
- Modify: `tests/test_loop_runner.py`
- Modify: `tests/test_best_quote_maker_volume.py`

**Step 1: Add interaction tests**

Add tests proving:

- Below soft threshold, the planner emits four ordinary roles and no active-pair loss-reduction role.
- At soft threshold, existing runner logic emits paired `best_quote_active_pair_reduce_long` and `best_quote_active_pair_reduce_short` orders at BBO.
- Each side remains capped by the existing lease cap near 100U.
- After the measured side returns below soft threshold, the active-pair report becomes inactive and ordinary four-leg planning can resume.
- Frozen quantities never increase the ordinary reduce quantity and frozen ownership tags are absent from ordinary orders.

Use existing active-pair test fixtures and assert unique order roles, not partial-fill rows:

```python
assert {order["role"] for order in active_orders} == {
    "best_quote_active_pair_reduce_long",
    "best_quote_active_pair_reduce_short",
}
assert sum(order["notional"] for order in active_orders if order["position_side"] == "LONG") <= 100.0
assert sum(order["notional"] for order in active_orders if order["position_side"] == "SHORT") <= 100.0
```

**Step 2: Run tests and confirm new assertions expose any integration gap**

Run:

```bash
.venv/bin/pytest -q \
  tests/test_best_quote_maker_volume.py \
  tests/test_loop_runner.py \
  -k 'four_leg_cycle or active_pair'
```

Expected: new tests either pass through existing active-pair code or fail only at the narrow four-leg/active-pair boundary.

**Step 3: Make the minimum boundary fix if the tests fail**

Keep the active-pair sizing, threshold, cooldown, audit, and cap code unchanged. The only permitted boundary fix is to ensure the active-pair result takes precedence over ordinary four-leg orders for a side at/above soft threshold, and that the four-leg helper runs again after the active-pair report is inactive.

**Step 4: Run the complete affected suites**

Run:

```bash
.venv/bin/pytest -q \
  tests/test_best_quote_maker_volume.py \
  tests/test_loop_runner.py \
  tests/test_web_security.py
```

Expected: PASS. Record any pre-existing unrelated failure separately and prove it reproduces at `1c0d7dc6` before accepting it as unrelated.

**Step 5: Commit**

```bash
git add tests/test_best_quote_maker_volume.py tests/test_loop_runner.py src/grid_optimizer/loop_runner.py
git commit -m "test: preserve active-pair loss-reduction boundary"
```

### Task 5: Replay production-shaped plans offline

**Files:**
- Create: `tests/fixtures/grvt_four_leg_111_control.json`
- Create: `tests/fixtures/grvt_four_leg_114_canary.json`
- Modify: `tests/test_best_quote_maker_volume.py`

**Step 1: Capture sanitized planner inputs**

From current production latest-plan/state data, capture only non-secret planner inputs: BBO, tick/step sizes, ordinary LONG/SHORT quantities and average costs, frozen quantities, soft/hard limits, cycle budget, min notional, and anti-chase state. Do not copy API keys, account IDs, signatures, cookies, or full environment files.

**Step 2: Add deterministic replay assertions**

For each fixture, run flag-off and flag-on builds:

```python
assert off_plan == expected_control_plan
assert _ordinary_role_names(on_plan) == {
    "best_quote_entry_long",
    "best_quote_reduce_long",
    "best_quote_entry_short",
    "best_quote_reduce_short",
}
assert _all_orders_respect_hard_and_frozen_boundaries(on_plan)
```

The fixture case may report a documented blocked role when soft headroom is below min notional; that block must be explicit and the active-pair threshold case must be covered separately.

**Step 3: Run replay and full affected tests**

Run:

```bash
.venv/bin/pytest -q \
  tests/test_best_quote_maker_volume.py \
  tests/test_loop_runner.py \
  tests/test_web_security.py
```

Expected: PASS with deterministic plans.

**Step 4: Commit**

```bash
git add tests/fixtures/grvt_four_leg_111_control.json tests/fixtures/grvt_four_leg_114_canary.json tests/test_best_quote_maker_volume.py
git commit -m "test: replay GRVT four-leg planner states"
```

### Task 6: Review, push the branch, and prepare a reversible 114-only canary

**Files:**
- Review all branch changes since `1c0d7dc6`

**Step 1: Inspect scope and secrets**

Run:

```bash
git status --short
git diff --stat 1c0d7dc6..HEAD
git diff --check 1c0d7dc6..HEAD
git diff 1c0d7dc6..HEAD -- src tests docs
```

Expected: only planned source/tests/docs change; `.playwright-cli/` remains untouched and untracked; no credentials or production environment files appear.

**Step 2: Run final local verification**

Run:

```bash
.venv/bin/pytest -q \
  tests/test_best_quote_maker_volume.py \
  tests/test_loop_runner.py \
  tests/test_web_security.py
```

Expected: PASS.

**Step 3: Push only the canary branch**

Run:

```bash
git push -u origin codex/grvt-four-leg-canary
```

Expected: remote branch exists; `main` remains unchanged.

**Step 4: Verify both production worktrees before changing 114**

Run read-only checks:

```bash
ssh srv-43-155-136-111 'cd /home/ubuntu/wangge && git status --short && git rev-parse HEAD && systemctl is-active grid-loop@GRVTUSDT'
ssh srv-43-155-163-114 'cd /home/ubuntu/wangge && git status --short && git rev-parse HEAD && systemctl is-active grid-loop@GRVTUSDT'
```

Expected: 111 stays on the production control commit and active; 114 is clean enough for a detached canary checkout. Stop if tracked production files are dirty.

**Step 5: Deploy the branch commit to 114 only**

On 114, fetch the exact branch commit, switch to detached HEAD at that SHA, install/update only if dependency metadata changed, enable `best_quote_maker_volume_four_leg_cycle_enabled=true` atomically in `output/grvtusdt_loop_runner_control.json`, and restart with:

```bash
/usr/local/bin/grid-saved-runner restart GRVTUSDT
```

Do not change 111. Do not use `/usr/local/bin/grid-web-update` for this branch canary because it follows `main`.

**Step 6: Five-minute warmup verification**

Confirm on 114:

- both services active;
- runner control reports the new flag true;
- latest plan reports the four-leg metric and either four planned roles or explicit legal block reasons;
- new live orders match ordinary/frozen ownership;
- at least one real post-restart fill occurs, unless the market-wide control also has zero fills during the same interval.

If the runner fails, ownership is ambiguous, or the ledger drifts, immediately set the flag false atomically and restart the runner.

### Task 7: Run the controlled canary and decide merge or rollback

**Files:**
- No repository changes unless validation exposes a failing testable defect

**Step 1: Observe 111 control and 114 canary for 30–60 minutes**

Every 15 minutes, aggregate unique orders by `orderId/clientOrderId`, merge partial fills, and compare:

- rolling 15m and 60m gross volume;
- required pace to reach 150,000U by two hours before window end;
- ordinary entry + ordinary profitable reduce volume share;
- ordinary profitable reduces, authorized loss reduces, and frozen fills separately;
- loss-reduce unique-order count, notional, realized loss, average/minimum interval, direction, and trigger;
- ordinary LONG/SHORT/net positions, frozen LONG/SHORT and ledger delta;
- live order role retention and latest-plan role completeness;
- service and error/audit logs.

**Step 2: Apply acceptance gates**

114 passes only if all are true after warmup:

1. Rolling 15m pace meets its required target pace; if both machines are in a common low-liquidity interval, 114 is at least 90% of simultaneous 111 and at least 90% of its own pre-canary 60m pace.
2. Ordinary entries + profitable ordinary reduces are at least 60% of 114 gross volume and at least 10 percentage points above simultaneous 111.
3. Same-side loss-reduce orders are no more than 3 per 15m, independent-order average gap is at least 60 seconds, and each side remains within the existing approximately 100U lease cap.
4. No wrong-side reduction, non-reduce-only negative PnL, worse-price reverse refill within two minutes, full-side clear, frozen/ordinary ownership error, frozen-ledger drift, cap breach, or plan/fill mismatch.
5. The four ordinary roles remain planned or carry an explicit legal block reason; no plan silently degrades to far-only leftovers.

**Step 3: Roll back on any hard failure**

Atomically set `best_quote_maker_volume_four_leg_cycle_enabled=false` on 114 and restart the runner. If code stability itself is affected, check 114 back out to the exact 111 control SHA and restart. Recheck services, orders, fills, ownership, and ledger after rollback.

**Step 4: Merge only after a pass**

After a documented pass, fast-forward or merge `codex/grvt-four-leg-canary` into local `main`, rerun the full affected suites, push `main`, deploy through the standard update path, and enable the flag separately on 111 only after production verification on 114 remains healthy.

Do not perform this step merely because the branch tests pass; real canary acceptance is required.
