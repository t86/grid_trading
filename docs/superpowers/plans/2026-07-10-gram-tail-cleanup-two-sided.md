# GRAM Tail Cleanup Two-Sided Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Keep synthetic-neutral GRAM maker orders two-sided while a sub-order residual is cleaned up, without weakening threshold/hard-risk or explicit base-restore behavior.

**Architecture:** Change only `build_inventory_grid_orders` in the inventory planner. A valid normal-risk synthetic tail cleanup remains the sole reduce-side order for the residual, while planning continues into the opposite entry side; all non-synthetic and reduce-only paths retain their current early return.

**Tech Stack:** Python 3.12, pytest, existing `grid_optimizer.inventory_grid_plan` planner and saved-runner deployment wrappers.

---

### Task 1: Align The Existing Top-Of-Book Baseline Test

**Files:**
- Modify: `tests/test_inventory_grid_plan.py:111-138`

- [ ] **Step 1: Rename the stale test and update its expected synthetic-neutral flat prices**

The deployed planner starts the first BUY at best bid and the first SELL at best
ask. Rename the test to `test_synthetic_neutral_flat_starts_at_top_of_book` and
change the two stale price assertions:

```python
assert [order["price"] for order in plan["bootstrap_orders"] if order["side"] == "BUY"] == [0.0880, 0.0879]
assert [order["price"] for order in plan["bootstrap_orders"] if order["side"] == "SELL"] == [0.0881, 0.0882]
```

- [ ] **Step 2: Run the baseline test**

Run:

```bash
PYTHONPATH=src PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 /opt/anaconda3/bin/python3 -m pytest \
  tests/test_inventory_grid_plan.py::test_synthetic_neutral_flat_starts_at_top_of_book -q
```

Expected: PASS.

- [ ] **Step 3: Commit the baseline correction**

```bash
git add tests/test_inventory_grid_plan.py
git commit -m "test: align synthetic neutral best bid expectation"
```

### Task 2: Add Failing Normal-Risk Synthetic Tail Tests

**Files:**
- Modify: `tests/test_inventory_grid_plan.py`

- [ ] **Step 1: Add the failing small-short test**

```python
def test_synthetic_neutral_short_tail_keeps_sell_entry_live() -> None:
    runtime = new_inventory_grid_runtime(market_type="futures")
    runtime["synthetic_neutral"] = True
    runtime["direction_state"] = "short_active"
    runtime["grid_anchor_price"] = 1.677
    runtime["position_lots"] = [
        {
            "lot_id": "short_tail",
            "side": "short",
            "qty": 32.0,
            "entry_price": 1.677,
            "opened_at_ms": 1,
            "source_role": "grid_entry",
        }
    ]

    plan = build_inventory_grid_orders(
        runtime=runtime,
        bid_price=1.677,
        ask_price=1.678,
        step_price=0.001,
        per_order_notional=80.0,
        first_order_multiplier=1.0,
        threshold_position_notional=220.0,
        threshold_reduce_target_notional=220.0,
        max_order_position_notional=240.0,
        max_position_notional=240.0,
        max_short_order_position_notional=240.0,
        max_short_position_notional=240.0,
        buy_levels=3,
        sell_levels=3,
        tick_size=0.001,
        step_size=0.01,
        min_qty=0.01,
        min_notional=5.0,
    )

    assert [(item["side"], item["role"]) for item in plan["buy_orders"]] == [("BUY", "tail_cleanup")]
    assert [(item["side"], item["role"]) for item in plan["sell_orders"]] == [("SELL", "grid_entry")]
    assert sum(item["qty"] for item in plan["buy_orders"]) == 32.0
```

- [ ] **Step 2: Add the failing small-long test**

```python
def test_synthetic_neutral_long_tail_keeps_buy_entry_live() -> None:
    runtime = new_inventory_grid_runtime(market_type="futures")
    runtime["synthetic_neutral"] = True
    runtime["direction_state"] = "long_active"
    runtime["grid_anchor_price"] = 1.677
    runtime["position_lots"] = [
        {
            "lot_id": "long_tail",
            "side": "long",
            "qty": 32.0,
            "entry_price": 1.677,
            "opened_at_ms": 1,
            "source_role": "grid_entry",
        }
    ]

    plan = build_inventory_grid_orders(
        runtime=runtime,
        bid_price=1.677,
        ask_price=1.678,
        step_price=0.001,
        per_order_notional=80.0,
        first_order_multiplier=1.0,
        threshold_position_notional=220.0,
        threshold_reduce_target_notional=220.0,
        max_order_position_notional=240.0,
        max_position_notional=240.0,
        max_short_order_position_notional=240.0,
        max_short_position_notional=240.0,
        buy_levels=3,
        sell_levels=3,
        tick_size=0.001,
        step_size=0.01,
        min_qty=0.01,
        min_notional=5.0,
    )

    assert [(item["side"], item["role"]) for item in plan["sell_orders"]] == [("SELL", "tail_cleanup")]
    assert [(item["side"], item["role"]) for item in plan["buy_orders"]] == [
        ("BUY", "grid_entry"),
        ("BUY", "grid_entry"),
    ]
    assert sum(item["qty"] for item in plan["sell_orders"]) == 32.0
```

Two BUY entries are expected because the current long inventory plus two 80U
entries remains below the 240U order-position cap; the third would exceed it.

- [ ] **Step 3: Run both tests and verify RED**

Run:

```bash
PYTHONPATH=src PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 /opt/anaconda3/bin/python3 -m pytest \
  tests/test_inventory_grid_plan.py::test_synthetic_neutral_short_tail_keeps_sell_entry_live \
  tests/test_inventory_grid_plan.py::test_synthetic_neutral_long_tail_keeps_buy_entry_live -q
```

Expected: both FAIL because the planner currently returns immediately after adding `tail_cleanup`.

### Task 3: Continue Into The Opposite Entry Side

**Files:**
- Modify: `src/grid_optimizer/inventory_grid_plan.py:982-1116`
- Modify: `src/grid_optimizer/inventory_grid_plan.py:1158-1245`
- Test: `tests/test_inventory_grid_plan.py`

- [ ] **Step 1: Preserve normal synthetic long cleanup and continue**

Introduce `tail_cleanup_planned_qty = 0.0` before the long cleanup block. After appending a valid cleanup order:

```python
if not synthetic_neutral or risk_state != "normal":
    return _finalize_inventory_grid_plan(
        risk_state=risk_state,
        bootstrap_orders=bootstrap_orders,
        buy_orders=buy_orders,
        sell_orders=sell_orders,
        forced_reduce_orders=forced_reduce_orders,
        tail_cleanup_active=True,
        min_qty=min_qty,
        min_notional=min_notional,
    )
tail_cleanup_planned_qty = cleanup_qty
```

Initialize `planned_closing_qty` from `tail_cleanup_planned_qty`. When it is positive, skip `_build_sell_ladder_orders`; otherwise retain the current warmup/grid-exit logic. Add any generated grid-exit quantity instead of replacing the existing cleanup quantity:

```python
planned_closing_qty = tail_cleanup_planned_qty
if tail_cleanup_planned_qty > EPSILON:
    raw_sell_orders = []
elif warmup_top_of_book_active:
    raw_sell_orders = []
    top_book_sell_price = _round_order_price(ask_price, tick_size, "SELL")
    if _order_meets_mins(
        qty=closeable_qty,
        price=top_book_sell_price,
        min_qty=min_qty,
        min_notional=min_notional,
    ):
        raw_sell_orders.append(
            _build_order(
                side="SELL",
                price=top_book_sell_price,
                qty=closeable_qty,
                role="grid_exit",
            )
        )
else:
    raw_sell_orders = _build_sell_ladder_orders(
        reference_price=sell_reference_price,
        step_price=step_price,
        levels=sell_levels if risk_state == "normal" else 1,
        per_order_notional=per_order_notional,
        role="grid_exit",
        start_level=1,
        max_total_qty=closeable_qty,
        tick_size=tick_size,
        step_size=step_size,
        min_qty=min_qty,
        min_notional=min_notional,
    )

planned_closing_qty += sum(
    max(_safe_float(item.get("qty")), 0.0)
    for item in sell_orders
    if item.get("role") == "grid_exit"
)
```

This keeps one cleanup SELL and allows the existing BUY-entry branch to run.

- [ ] **Step 2: Preserve normal synthetic short cleanup and continue**

Introduce `tail_cleanup_planned_qty = 0.0` before the short cleanup block. Use the same early-return condition after adding a valid BUY cleanup. Initialize `planned_closing_qty` from it and skip the ordinary `grid_exit` BUY when cleanup already covers the full residual:

```python
planned_closing_qty = tail_cleanup_planned_qty
if tail_cleanup_planned_qty <= EPSILON and _order_meets_mins(
    qty=exit_qty,
    price=exit_price,
    min_qty=min_qty,
    min_notional=min_notional,
):
    buy_orders.append(_build_order(side="BUY", price=exit_price, qty=exit_qty, role="grid_exit"))
    planned_closing_qty = exit_qty
```

The existing short-entry SELL branch then runs under the unchanged position caps.

- [ ] **Step 3: Run focused tests and verify GREEN**

Run:

```bash
PYTHONPATH=src PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 /opt/anaconda3/bin/python3 -m pytest \
  tests/test_inventory_grid_plan.py::test_synthetic_neutral_short_tail_keeps_sell_entry_live \
  tests/test_inventory_grid_plan.py::test_synthetic_neutral_long_tail_keeps_buy_entry_live \
  tests/test_inventory_grid_plan.py::test_tail_cleanup_emits_single_cleanup_order \
  tests/test_inventory_grid_plan.py::test_tail_cleanup_overrides_threshold_reduce_only \
  tests/test_inventory_grid_plan.py::test_tail_cleanup_below_exchange_mins_keeps_grid_entries_live -q
```

Expected: 5 passed.

- [ ] **Step 4: Run planner and spot-loop regressions**

Run:

```bash
PYTHONPATH=src PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 /opt/anaconda3/bin/python3 -m pytest \
  tests/test_inventory_grid_plan.py tests/test_spot_loop_runner.py -q
```

Expected: every planner test passes. The combined run may retain the six
pre-existing `test_spot_loop_runner.py` failures reproduced unchanged at
baseline commit `6bcb1c0`; the failure set must not grow or change.

- [ ] **Step 5: Commit the implementation**

```bash
git add src/grid_optimizer/inventory_grid_plan.py tests/test_inventory_grid_plan.py
git commit -m "fix: keep synthetic tail cleanup two-sided"
```

### Task 4: Integrate And Deploy

**Files:**
- No additional source files.

- [ ] **Step 1: Rebase onto current `origin/main` and rerun focused tests**

```bash
git fetch origin main
git rebase origin/main
PYTHONPATH=src PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 /opt/anaconda3/bin/python3 -m pytest \
  tests/test_inventory_grid_plan.py tests/test_spot_loop_runner.py -q
```

Expected: rebase succeeds and tests pass.

- [ ] **Step 2: Push the branch commits to `main`**

```bash
git push origin HEAD:main
```

Expected: fast-forward update succeeds.

- [ ] **Step 3: Pull and restart GRAM on both hosts**

114:

```bash
ssh srv-43-155-163-114 '/usr/local/bin/grid-web-update && /usr/local/bin/grid-saved-runner restart GRAMUSDT'
```

150:

```bash
ssh srv-43-131-232-150 '/usr/local/bin/grid-web-api2-update && /usr/local/bin/grid-saved-runner-api2 restart GRAMUSDT'
```

- [ ] **Step 4: Verify production invariants**

Confirm both hosts run the pushed commit, services are active, OPN/SXT remain stopped, and GRAM retains:

```text
per_order_notional=80
attack_buy_levels=3
attack_sell_levels=3
same-price LIMIT+IOC enabled
pure MARKET TAKE disabled
```

For a normal small residual, verify local events show both `active_buy_orders > 0` and `active_sell_orders > 0`. If the hosts are flat, run the planner offline against a copied live state with a synthetic 32 GRAM residual; do not mutate production state.

- [ ] **Step 5: Verify target progress from exchange trades**

Use paginated spot `userTrades` from `1783669438000`, deduplicate by Binance trade ID, and report per host:

```text
gross_notional / 30000
remaining_notional
elapsed_minutes
required_hourly_rate_for_remaining_window
maker_count / taker_count
price loss excluding fees
fees by commission asset
```

Do not mark the goal complete until both exchange totals are at least 30,000U in this target window.
