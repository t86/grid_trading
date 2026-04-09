# Unified Competition Inventory Grid Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a fill-driven competition inventory grid engine shared by futures and spot runners, with strategy-owned anchor price, lot ledger, in-memory pair credits, and threshold/hard reduce-only behavior.

**Architecture:** Add three focused shared modules: runtime state bookkeeping, exchange-trade recovery, and order planning. Integrate them as new strategy modes in the futures and spot runners so existing modes stay intact. Pair credits remain process-local and reset on restart; recovery rebuilds direction, lots, anchor, and risk posture from exchange trades plus local order references.

**Tech Stack:** Python 3, existing `grid_optimizer` package, Binance REST helpers in `src/grid_optimizer/data.py`, `unittest` tests executed with `pytest`

---

## File Map

- Create: `src/grid_optimizer/inventory_grid_state.py`
  Responsibility: Own the shared runtime shape, lot bookkeeping, anchor updates, pair-credit accounting, and adverse-lot selection.
- Create: `src/grid_optimizer/inventory_grid_recovery.py`
  Responsibility: Rebuild shared runtime state from exchange trades plus persisted order refs, and mark conservative recovery failures.
- Create: `src/grid_optimizer/inventory_grid_plan.py`
  Responsibility: Turn runtime state + market snapshot + config into bootstrap orders, active grid orders, forced-reduce orders, and tail-cleanup orders.
- Create: `tests/test_inventory_grid_state.py`
  Responsibility: Unit coverage for fill accounting, anchor updates, pair-credit accumulation, and adverse-lot selection.
- Create: `tests/test_inventory_grid_recovery.py`
  Responsibility: Unit coverage for restart recovery, missing-order-ref fallback, and pair-credit reset on restart.
- Create: `tests/test_inventory_grid_plan.py`
  Responsibility: Unit coverage for flat/bootstrap behavior, active-grid behavior, threshold/hard reduce-only behavior, and tail cleanup.
- Modify: `src/grid_optimizer/loop_runner.py`
  Responsibility: Add `competition_inventory_grid` mode, futures recovery wiring, futures plan generation, order-ref persistence, and summary fields.
- Modify: `tests/test_loop_runner.py`
  Responsibility: Exercise the new futures strategy mode and parser wiring without regressing existing modes.
- Modify: `src/grid_optimizer/spot_loop_runner.py`
  Responsibility: Add `spot_competition_inventory_grid` mode, spot recovery wiring, spot plan generation, and trade sync for the new runtime shape.
- Modify: `tests/test_spot_loop_runner.py`
  Responsibility: Exercise the new spot strategy mode and parser wiring without regressing the existing spot modes.
- Modify: `docs/STRATEGY_EXECUTION_GUIDE.md`
  Responsibility: Document the new competition inventory grid modes, recovery behavior, and reduce-only semantics.

Out of scope for this plan:

- `src/grid_optimizer/web.py`
- console presets
- new UI wiring

### Task 1: Shared Runtime State And Fill Accounting

**Files:**
- Create: `src/grid_optimizer/inventory_grid_state.py`
- Test: `tests/test_inventory_grid_state.py`

- [ ] **Step 1: Write the failing tests**

```python
from grid_optimizer.inventory_grid_state import (
    apply_inventory_grid_fill,
    build_forced_reduce_lot_plan,
    new_inventory_grid_runtime,
)


def test_apply_fill_updates_anchor_only_for_grid_roles() -> None:
    runtime = new_inventory_grid_runtime(market_type="futures")

    apply_inventory_grid_fill(
        runtime=runtime,
        role="bootstrap_entry",
        side="BUY",
        price=0.10,
        qty=400.0,
        fill_time_ms=1,
    )
    assert runtime["direction_state"] == "long_active"
    assert runtime["grid_anchor_price"] == 0.10

    apply_inventory_grid_fill(
        runtime=runtime,
        role="forced_reduce",
        side="SELL",
        price=0.09,
        qty=100.0,
        fill_time_ms=2,
    )
    assert runtime["grid_anchor_price"] == 0.10


def test_apply_fill_accumulates_pair_credit_steps_for_completed_pairs() -> None:
    runtime = new_inventory_grid_runtime(market_type="spot")

    apply_inventory_grid_fill(
        runtime=runtime,
        role="bootstrap_entry",
        side="BUY",
        price=0.10,
        qty=400.0,
        fill_time_ms=1,
    )
    apply_inventory_grid_fill(
        runtime=runtime,
        role="grid_exit",
        side="SELL",
        price=0.11,
        qty=100.0,
        fill_time_ms=2,
    )

    assert runtime["pair_credit_steps"] == 1
    assert runtime["position_lots"][0]["qty"] == 300.0


def test_build_forced_reduce_lot_plan_prefers_most_adverse_longs() -> None:
    runtime = new_inventory_grid_runtime(market_type="futures")
    runtime["direction_state"] = "long_active"
    runtime["position_lots"] = [
        {"lot_id": "cheap", "side": "long", "qty": 100.0, "entry_price": 0.09, "opened_at_ms": 1, "source_role": "grid_entry"},
        {"lot_id": "expensive", "side": "long", "qty": 100.0, "entry_price": 0.10, "opened_at_ms": 2, "source_role": "grid_entry"},
    ]

    lot_plan = build_forced_reduce_lot_plan(
        runtime=runtime,
        reduce_price=0.095,
        reduce_qty=100.0,
        step_price=0.01,
    )

    assert [item["lot_id"] for item in lot_plan["lots"]] == ["expensive"]
    assert lot_plan["forced_reduce_cost_steps"] == 0
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `python -m pytest tests/test_inventory_grid_state.py -q`

Expected: FAIL with `ModuleNotFoundError: No module named 'grid_optimizer.inventory_grid_state'`

- [ ] **Step 3: Write the minimal implementation**

```python
from __future__ import annotations

from typing import Any

EPSILON = 1e-12
ANCHOR_UPDATE_ROLES = frozenset({"bootstrap_entry", "grid_entry", "grid_exit"})


def new_inventory_grid_runtime(*, market_type: str) -> dict[str, Any]:
    normalized = str(market_type or "").strip().lower()
    if normalized not in {"futures", "spot"}:
        raise ValueError(f"unsupported market_type: {market_type}")
    return {
        "market_type": normalized,
        "direction_state": "flat",
        "risk_state": "normal",
        "grid_anchor_price": 0.0,
        "position_lots": [],
        "pair_credit_steps": 0,
        "last_strategy_trade_time_ms": 0,
        "recovery_mode": "live",
        "recovery_errors": [],
    }


def _next_direction_state(*, market_type: str, current_state: str, side: str) -> str:
    normalized_side = str(side or "").upper().strip()
    if current_state != "flat":
        return current_state
    if normalized_side == "BUY":
        return "long_active"
    if market_type == "futures" and normalized_side == "SELL":
        return "short_active"
    return current_state


def _consume_lots(
    *,
    lots: list[dict[str, Any]],
    qty_to_consume: float,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    remaining = max(float(qty_to_consume), 0.0)
    new_lots: list[dict[str, Any]] = []
    matched: list[dict[str, Any]] = []
    for lot in lots:
        if remaining <= EPSILON:
            new_lots.append(dict(lot))
            continue
        qty = max(float(lot.get("qty", 0.0) or 0.0), 0.0)
        if qty <= EPSILON:
            continue
        take_qty = min(qty, remaining)
        matched.append({**dict(lot), "matched_qty": take_qty})
        left_qty = qty - take_qty
        if left_qty > EPSILON:
            kept = dict(lot)
            kept["qty"] = left_qty
            new_lots.append(kept)
        remaining -= take_qty
    return new_lots, matched


def _accumulate_pair_credit_steps(*, matched: list[dict[str, Any]], exit_price: float, step_price: float) -> int:
    total = 0
    if step_price <= 0:
        return total
    for item in matched:
        entry_price = max(float(item.get("entry_price", 0.0) or 0.0), 0.0)
        matched_qty = max(float(item.get("matched_qty", 0.0) or 0.0), 0.0)
        if entry_price <= 0 or matched_qty <= EPSILON:
            continue
        gross_steps = abs(exit_price - entry_price) / step_price
        total += int(gross_steps)
    return total


def apply_inventory_grid_fill(
    *,
    runtime: dict[str, Any],
    role: str,
    side: str,
    price: float,
    qty: float,
    fill_time_ms: int,
    step_price: float = 0.01,
) -> None:
    normalized_role = str(role or "").strip().lower()
    normalized_side = str(side or "").upper().strip()
    fill_price = max(float(price), 0.0)
    fill_qty = max(float(qty), 0.0)
    if fill_price <= 0 or fill_qty <= EPSILON:
        return

    runtime["direction_state"] = _next_direction_state(
        market_type=str(runtime.get("market_type", "futures")),
        current_state=str(runtime.get("direction_state", "flat")),
        side=normalized_side,
    )
    lots = list(runtime.get("position_lots") or [])

    if normalized_side == "BUY" and runtime["direction_state"] == "long_active":
        lots.append(
            {
                "lot_id": f"lot_{fill_time_ms}_{len(lots) + 1}",
                "side": "long",
                "qty": fill_qty,
                "entry_price": fill_price,
                "opened_at_ms": int(fill_time_ms),
                "source_role": normalized_role,
            }
        )
    elif normalized_side == "SELL" and runtime["direction_state"] == "long_active":
        lots, matched = _consume_lots(lots=lots, qty_to_consume=fill_qty)
        if normalized_role == "grid_exit":
            runtime["pair_credit_steps"] = int(runtime.get("pair_credit_steps", 0) or 0) + _accumulate_pair_credit_steps(
                matched=matched,
                exit_price=fill_price,
                step_price=step_price,
            )
        if not lots:
            runtime["direction_state"] = "flat"
    elif normalized_side == "SELL" and runtime["direction_state"] == "short_active":
        lots.append(
            {
                "lot_id": f"lot_{fill_time_ms}_{len(lots) + 1}",
                "side": "short",
                "qty": fill_qty,
                "entry_price": fill_price,
                "opened_at_ms": int(fill_time_ms),
                "source_role": normalized_role,
            }
        )
    elif normalized_side == "BUY" and runtime["direction_state"] == "short_active":
        lots, matched = _consume_lots(lots=lots, qty_to_consume=fill_qty)
        if normalized_role == "grid_exit":
            runtime["pair_credit_steps"] = int(runtime.get("pair_credit_steps", 0) or 0) + _accumulate_pair_credit_steps(
                matched=matched,
                exit_price=fill_price,
                step_price=step_price,
            )
        if not lots:
            runtime["direction_state"] = "flat"

    runtime["position_lots"] = lots
    runtime["last_strategy_trade_time_ms"] = int(fill_time_ms)
    if normalized_role in ANCHOR_UPDATE_ROLES:
        runtime["grid_anchor_price"] = fill_price


def build_forced_reduce_lot_plan(
    *,
    runtime: dict[str, Any],
    reduce_price: float,
    reduce_qty: float,
    step_price: float,
) -> dict[str, Any]:
    direction_state = str(runtime.get("direction_state", "flat"))
    lots = list(runtime.get("position_lots") or [])
    if direction_state == "long_active":
        ordered = sorted(lots, key=lambda item: float(item.get("entry_price", 0.0) or 0.0), reverse=True)
        step_value = lambda lot: max(0.0, (float(lot.get("entry_price", 0.0) or 0.0) - reduce_price) / step_price)
    else:
        ordered = sorted(lots, key=lambda item: float(item.get("entry_price", 0.0) or 0.0))
        step_value = lambda lot: max(0.0, (reduce_price - float(lot.get("entry_price", 0.0) or 0.0)) / step_price)

    remaining = max(float(reduce_qty), 0.0)
    selected: list[dict[str, Any]] = []
    forced_reduce_cost_steps = 0
    for lot in ordered:
        if remaining <= EPSILON:
            break
        lot_qty = max(float(lot.get("qty", 0.0) or 0.0), 0.0)
        take_qty = min(lot_qty, remaining)
        if take_qty <= EPSILON:
            continue
        selected.append({"lot_id": str(lot.get("lot_id") or ""), "qty": take_qty})
        forced_reduce_cost_steps += int(step_value(lot))
        remaining -= take_qty
    return {"lots": selected, "forced_reduce_cost_steps": forced_reduce_cost_steps}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `python -m pytest tests/test_inventory_grid_state.py -q`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add tests/test_inventory_grid_state.py src/grid_optimizer/inventory_grid_state.py
git commit -m "feat: add inventory grid state bookkeeping"
```

### Task 2: Recovery From Exchange Trades

**Files:**
- Create: `src/grid_optimizer/inventory_grid_recovery.py`
- Test: `tests/test_inventory_grid_recovery.py`
- Modify: `src/grid_optimizer/inventory_grid_state.py`

- [ ] **Step 1: Write the failing tests**

```python
from grid_optimizer.inventory_grid_recovery import rebuild_inventory_grid_runtime


def test_rebuild_runtime_recovers_anchor_and_lots_from_strategy_trades() -> None:
    runtime = rebuild_inventory_grid_runtime(
        market_type="futures",
        trades=[
            {"id": 1, "time": 1000, "price": "0.1000", "qty": "400", "side": "BUY", "orderId": 11},
            {"id": 2, "time": 2000, "price": "0.1100", "qty": "100", "side": "SELL", "orderId": 12},
        ],
        order_refs={
            "11": {"role": "bootstrap_entry", "side": "BUY"},
            "12": {"role": "grid_exit", "side": "SELL"},
        },
        step_price=0.01,
    )

    assert runtime["direction_state"] == "long_active"
    assert runtime["grid_anchor_price"] == 0.11
    assert runtime["pair_credit_steps"] == 0
    assert runtime["position_lots"][0]["qty"] == 300.0


def test_rebuild_runtime_marks_conservative_mode_when_position_cannot_be_explained() -> None:
    runtime = rebuild_inventory_grid_runtime(
        market_type="spot",
        trades=[],
        order_refs={},
        step_price=0.01,
        current_position_qty=50.0,
    )

    assert runtime["recovery_mode"] == "conservative_reduce_only"
    assert "missing_strategy_trade_history" in runtime["recovery_errors"]


def test_rebuild_runtime_marks_conservative_mode_when_both_bootstrap_sides_fill() -> None:
    runtime = rebuild_inventory_grid_runtime(
        market_type="futures",
        trades=[
            {"id": 1, "time": 1000, "price": "0.1000", "qty": "100", "side": "BUY", "orderId": 11},
            {"id": 2, "time": 1001, "price": "0.1001", "qty": "100", "side": "SELL", "orderId": 12},
        ],
        order_refs={
            "11": {"role": "bootstrap_entry", "side": "BUY"},
            "12": {"role": "bootstrap_entry", "side": "SELL"},
        },
        step_price=0.01,
    )

    assert runtime["recovery_mode"] == "conservative_reduce_only"
    assert "conflicting_bootstrap_fills" in runtime["recovery_errors"]
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `python -m pytest tests/test_inventory_grid_recovery.py -q`

Expected: FAIL with `ModuleNotFoundError: No module named 'grid_optimizer.inventory_grid_recovery'`

- [ ] **Step 3: Write the minimal implementation**

```python
from __future__ import annotations

from typing import Any

from .inventory_grid_state import apply_inventory_grid_fill, new_inventory_grid_runtime


def rebuild_inventory_grid_runtime(
    *,
    market_type: str,
    trades: list[dict[str, Any]],
    order_refs: dict[str, dict[str, Any]],
    step_price: float,
    current_position_qty: float = 0.0,
) -> dict[str, Any]:
    runtime = new_inventory_grid_runtime(market_type=market_type)
    runtime["pair_credit_steps"] = 0

    ordered_trades = sorted(
        list(trades or []),
        key=lambda row: (
            int(row.get("time", 0) or 0),
            int(row.get("id", 0) or 0),
        ),
    )
    for trade in ordered_trades:
        order_id = str(int(trade.get("orderId", 0) or 0))
        order_ref = order_refs.get(order_id)
        if not isinstance(order_ref, dict):
            continue
        apply_inventory_grid_fill(
            runtime=runtime,
            role=str(order_ref.get("role", "") or ""),
            side=str(order_ref.get("side", "") or ""),
            price=float(trade.get("price", 0.0) or 0.0),
            qty=abs(float(trade.get("qty", 0.0) or 0.0)),
            fill_time_ms=int(trade.get("time", 0) or 0),
            step_price=step_price,
        )

    if max(float(current_position_qty), 0.0) > 0 and not list(runtime.get("position_lots") or []):
        runtime["recovery_mode"] = "conservative_reduce_only"
        runtime["recovery_errors"] = ["missing_strategy_trade_history"]
        runtime["risk_state"] = "hard_reduce_only"
        return runtime

    lot_sides = {str(item.get("side", "") or "") for item in runtime.get("position_lots", [])}
    if market_type == "futures" and "long" in lot_sides and "short" in lot_sides:
        runtime["recovery_mode"] = "conservative_reduce_only"
        runtime["recovery_errors"] = ["conflicting_bootstrap_fills"]
        runtime["risk_state"] = "hard_reduce_only"
    return runtime
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `python -m pytest tests/test_inventory_grid_recovery.py -q`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add tests/test_inventory_grid_recovery.py src/grid_optimizer/inventory_grid_recovery.py src/grid_optimizer/inventory_grid_state.py
git commit -m "feat: add inventory grid recovery helpers"
```

### Task 3: Shared Order Planning And Risk Resolution

**Files:**
- Create: `src/grid_optimizer/inventory_grid_plan.py`
- Test: `tests/test_inventory_grid_plan.py`
- Modify: `src/grid_optimizer/inventory_grid_state.py`

- [ ] **Step 1: Write the failing tests**

```python
from grid_optimizer.inventory_grid_plan import build_inventory_grid_orders
from grid_optimizer.inventory_grid_state import new_inventory_grid_runtime


def test_futures_flat_starts_with_two_bootstrap_orders() -> None:
    runtime = new_inventory_grid_runtime(market_type="futures")

    plan = build_inventory_grid_orders(
        runtime=runtime,
        bid_price=0.0999,
        ask_price=0.1001,
        step_price=0.01,
        per_order_notional=10.0,
        first_order_multiplier=4.0,
        threshold_position_notional=50.0,
        max_order_position_notional=80.0,
        max_position_notional=120.0,
        tick_size=0.0001,
        step_size=1.0,
        min_qty=1.0,
        min_notional=5.0,
    )

    assert {item["side"] for item in plan["bootstrap_orders"]} == {"BUY", "SELL"}
    assert plan["buy_orders"] == []
    assert plan["sell_orders"] == []


def test_spot_flat_starts_with_single_bootstrap_buy() -> None:
    runtime = new_inventory_grid_runtime(market_type="spot")

    plan = build_inventory_grid_orders(
        runtime=runtime,
        bid_price=0.0999,
        ask_price=0.1001,
        step_price=0.01,
        per_order_notional=10.0,
        first_order_multiplier=4.0,
        threshold_position_notional=50.0,
        max_order_position_notional=80.0,
        max_position_notional=120.0,
        tick_size=0.0001,
        step_size=1.0,
        min_qty=1.0,
        min_notional=5.0,
    )

    assert len(plan["bootstrap_orders"]) == 1
    assert plan["bootstrap_orders"][0]["side"] == "BUY"


def test_threshold_reduce_only_blocks_same_side_entries_until_pair_credit_covers_cost() -> None:
    runtime = new_inventory_grid_runtime(market_type="futures")
    runtime["direction_state"] = "long_active"
    runtime["grid_anchor_price"] = 0.10
    runtime["position_lots"] = [
        {"lot_id": "l1", "side": "long", "qty": 100.0, "entry_price": 0.10, "opened_at_ms": 1, "source_role": "bootstrap_entry"},
        {"lot_id": "l2", "side": "long", "qty": 100.0, "entry_price": 0.09, "opened_at_ms": 2, "source_role": "grid_entry"},
    ]

    plan = build_inventory_grid_orders(
        runtime=runtime,
        bid_price=0.0799,
        ask_price=0.0801,
        step_price=0.01,
        per_order_notional=10.0,
        first_order_multiplier=4.0,
        threshold_position_notional=12.0,
        max_order_position_notional=30.0,
        max_position_notional=40.0,
        tick_size=0.0001,
        step_size=1.0,
        min_qty=1.0,
        min_notional=5.0,
    )

    assert plan["risk_state"] == "threshold_reduce_only"
    assert plan["buy_orders"] == []
    assert all(item["role"] != "forced_reduce" for item in plan["sell_orders"])


def test_pair_credit_unlocks_forced_reduce_order() -> None:
    runtime = new_inventory_grid_runtime(market_type="futures")
    runtime["direction_state"] = "long_active"
    runtime["grid_anchor_price"] = 0.10
    runtime["pair_credit_steps"] = 5
    runtime["position_lots"] = [
        {"lot_id": "l1", "side": "long", "qty": 100.0, "entry_price": 0.10, "opened_at_ms": 1, "source_role": "bootstrap_entry"},
    ]

    plan = build_inventory_grid_orders(
        runtime=runtime,
        bid_price=0.0899,
        ask_price=0.0901,
        step_price=0.01,
        per_order_notional=10.0,
        first_order_multiplier=4.0,
        threshold_position_notional=5.0,
        max_order_position_notional=30.0,
        max_position_notional=40.0,
        tick_size=0.0001,
        step_size=1.0,
        min_qty=1.0,
        min_notional=5.0,
    )

    assert any(item["role"] == "forced_reduce" for item in plan["sell_orders"])


def test_tail_cleanup_emits_single_cleanup_order() -> None:
    runtime = new_inventory_grid_runtime(market_type="spot")
    runtime["direction_state"] = "long_active"
    runtime["grid_anchor_price"] = 0.10
    runtime["position_lots"] = [
        {"lot_id": "tail", "side": "long", "qty": 5.0, "entry_price": 0.10, "opened_at_ms": 1, "source_role": "grid_entry"},
    ]

    plan = build_inventory_grid_orders(
        runtime=runtime,
        bid_price=0.0999,
        ask_price=0.1001,
        step_price=0.01,
        per_order_notional=10.0,
        first_order_multiplier=4.0,
        threshold_position_notional=50.0,
        max_order_position_notional=80.0,
        max_position_notional=120.0,
        tick_size=0.0001,
        step_size=1.0,
        min_qty=1.0,
        min_notional=5.0,
    )

    assert plan["tail_cleanup_active"] is True
    assert [item["role"] for item in plan["sell_orders"]] == ["tail_cleanup"]


def test_max_order_position_notional_blocks_new_same_side_entries() -> None:
    runtime = new_inventory_grid_runtime(market_type="futures")
    runtime["direction_state"] = "long_active"
    runtime["grid_anchor_price"] = 0.10
    runtime["position_lots"] = [
        {"lot_id": "l1", "side": "long", "qty": 200.0, "entry_price": 0.10, "opened_at_ms": 1, "source_role": "bootstrap_entry"},
    ]

    plan = build_inventory_grid_orders(
        runtime=runtime,
        bid_price=0.0999,
        ask_price=0.1001,
        step_price=0.01,
        per_order_notional=10.0,
        first_order_multiplier=4.0,
        threshold_position_notional=1000.0,
        max_order_position_notional=15.0,
        max_position_notional=120.0,
        tick_size=0.0001,
        step_size=1.0,
        min_qty=1.0,
        min_notional=5.0,
    )

    assert plan["risk_state"] == "normal"
    assert plan["buy_orders"] == []
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `python -m pytest tests/test_inventory_grid_plan.py -q`

Expected: FAIL with `ModuleNotFoundError: No module named 'grid_optimizer.inventory_grid_plan'`

- [ ] **Step 3: Write the minimal implementation**

```python
from __future__ import annotations

from typing import Any

from .dry_run import _round_order_price, _round_order_qty
from .inventory_grid_state import build_forced_reduce_lot_plan

EPSILON = 1e-12


def _position_notional(runtime: dict[str, Any], mid_price: float) -> float:
    return sum(max(float(item.get("qty", 0.0) or 0.0), 0.0) for item in runtime.get("position_lots", [])) * mid_price


def _build_order(*, side: str, price: float, qty: float, role: str) -> dict[str, Any]:
    return {
        "side": side,
        "price": price,
        "qty": qty,
        "notional": price * qty,
        "role": role,
    }


def build_inventory_grid_orders(
    *,
    runtime: dict[str, Any],
    bid_price: float,
    ask_price: float,
    step_price: float,
    per_order_notional: float,
    first_order_multiplier: float,
    threshold_position_notional: float,
    max_order_position_notional: float,
    max_position_notional: float,
    tick_size: float | None,
    step_size: float | None,
    min_qty: float | None,
    min_notional: float | None,
) -> dict[str, Any]:
    market_type = str(runtime.get("market_type", "futures"))
    direction_state = str(runtime.get("direction_state", "flat"))
    anchor = float(runtime.get("grid_anchor_price", 0.0) or 0.0)
    mid_price = (bid_price + ask_price) / 2.0
    first_order_notional = per_order_notional * first_order_multiplier
    bootstrap_orders: list[dict[str, Any]] = []
    buy_orders: list[dict[str, Any]] = []
    sell_orders: list[dict[str, Any]] = []

    if direction_state == "flat":
        buy_qty = _round_order_qty(first_order_notional / max(bid_price, EPSILON), step_size)
        if buy_qty >= (min_qty or 0.0) and buy_qty * bid_price >= (min_notional or 0.0):
            bootstrap_orders.append(_build_order(side="BUY", price=_round_order_price(bid_price, tick_size, "BUY"), qty=buy_qty, role="bootstrap_entry"))
        if market_type == "futures":
            sell_qty = _round_order_qty(first_order_notional / max(ask_price, EPSILON), step_size)
            if sell_qty >= (min_qty or 0.0) and sell_qty * ask_price >= (min_notional or 0.0):
                bootstrap_orders.append(_build_order(side="SELL", price=_round_order_price(ask_price, tick_size, "SELL"), qty=sell_qty, role="bootstrap_entry"))
        return {
            "risk_state": "normal",
            "bootstrap_orders": bootstrap_orders,
            "buy_orders": buy_orders,
            "sell_orders": sell_orders,
            "forced_reduce_orders": [],
            "tail_cleanup_active": False,
        }

    inventory_notional = _position_notional(runtime, mid_price)
    risk_state = "normal"
    if max_position_notional > 0 and inventory_notional >= max_position_notional:
        risk_state = "hard_reduce_only"
    elif threshold_position_notional > 0 and inventory_notional >= threshold_position_notional:
        risk_state = "threshold_reduce_only"

    pair_credit_steps = int(runtime.get("pair_credit_steps", 0) or 0)
    one_order_qty = _round_order_qty(per_order_notional / max(mid_price, EPSILON), step_size)
    held_qty = sum(max(float(item.get("qty", 0.0) or 0.0), 0.0) for item in runtime.get("position_lots", []))
    tail_cleanup_active = 0 < held_qty < max(one_order_qty, EPSILON)
    same_side_entry_budget = max(max_order_position_notional - inventory_notional, 0.0) if max_order_position_notional > 0 else per_order_notional
    if direction_state == "long_active":
        if tail_cleanup_active:
            cleanup_qty = _round_order_qty(held_qty, step_size)
            if cleanup_qty >= (min_qty or 0.0) and cleanup_qty * ask_price >= (min_notional or 0.0):
                sell_orders.append(_build_order(side="SELL", price=_round_order_price(ask_price, tick_size, "SELL"), qty=cleanup_qty, role="tail_cleanup"))
            return {
                "risk_state": risk_state,
                "bootstrap_orders": bootstrap_orders,
                "buy_orders": [],
                "sell_orders": sell_orders,
                "forced_reduce_orders": [],
                "tail_cleanup_active": True,
            }
        nearest_buy = _round_order_price(anchor - step_price, tick_size, "BUY")
        nearest_sell = _round_order_price(anchor + step_price, tick_size, "SELL")
        exit_qty = _round_order_qty(per_order_notional / max(nearest_sell, EPSILON), step_size)
        if exit_qty >= (min_qty or 0.0) and exit_qty * nearest_sell >= (min_notional or 0.0):
            sell_orders.append(_build_order(side="SELL", price=nearest_sell, qty=exit_qty, role="grid_exit"))
        if risk_state == "normal" and same_side_entry_budget >= per_order_notional:
            entry_qty = _round_order_qty(per_order_notional / max(nearest_buy, EPSILON), step_size)
            if entry_qty >= (min_qty or 0.0) and entry_qty * nearest_buy >= (min_notional or 0.0):
                buy_orders.append(_build_order(side="BUY", price=nearest_buy, qty=entry_qty, role="grid_entry"))
        if risk_state in {"threshold_reduce_only", "hard_reduce_only"}:
            reduce_qty = _round_order_qty(max((inventory_notional - threshold_position_notional) / max(mid_price, EPSILON), 0.0), step_size)
            lot_plan = build_forced_reduce_lot_plan(runtime=runtime, reduce_price=ask_price, reduce_qty=reduce_qty, step_price=step_price)
            if risk_state == "hard_reduce_only" or pair_credit_steps >= lot_plan["forced_reduce_cost_steps"]:
                if reduce_qty > (min_qty or 0.0):
                    sell_orders.append(_build_order(side="SELL", price=_round_order_price(ask_price, tick_size, "SELL"), qty=reduce_qty, role="forced_reduce"))
    else:
        if tail_cleanup_active:
            cleanup_qty = _round_order_qty(held_qty, step_size)
            if cleanup_qty >= (min_qty or 0.0) and cleanup_qty * bid_price >= (min_notional or 0.0):
                buy_orders.append(_build_order(side="BUY", price=_round_order_price(bid_price, tick_size, "BUY"), qty=cleanup_qty, role="tail_cleanup"))
            return {
                "risk_state": risk_state,
                "bootstrap_orders": bootstrap_orders,
                "buy_orders": buy_orders,
                "sell_orders": [],
                "forced_reduce_orders": [],
                "tail_cleanup_active": True,
            }
        nearest_sell = _round_order_price(anchor + step_price, tick_size, "SELL")
        nearest_buy = _round_order_price(anchor - step_price, tick_size, "BUY")
        exit_qty = _round_order_qty(per_order_notional / max(nearest_buy, EPSILON), step_size)
        if exit_qty >= (min_qty or 0.0) and exit_qty * nearest_buy >= (min_notional or 0.0):
            buy_orders.append(_build_order(side="BUY", price=nearest_buy, qty=exit_qty, role="grid_exit"))
        if risk_state == "normal" and same_side_entry_budget >= per_order_notional:
            entry_qty = _round_order_qty(per_order_notional / max(nearest_sell, EPSILON), step_size)
            if entry_qty >= (min_qty or 0.0) and entry_qty * nearest_sell >= (min_notional or 0.0):
                sell_orders.append(_build_order(side="SELL", price=nearest_sell, qty=entry_qty, role="grid_entry"))

    return {
        "risk_state": risk_state,
        "bootstrap_orders": bootstrap_orders,
        "buy_orders": buy_orders,
        "sell_orders": sell_orders,
        "forced_reduce_orders": [item for item in sell_orders + buy_orders if item["role"] == "forced_reduce"],
        "tail_cleanup_active": tail_cleanup_active,
    }
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `python -m pytest tests/test_inventory_grid_plan.py -q`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add tests/test_inventory_grid_plan.py src/grid_optimizer/inventory_grid_plan.py src/grid_optimizer/inventory_grid_state.py
git commit -m "feat: add inventory grid order planner"
```

### Task 4: Integrate The Futures Runner

**Files:**
- Modify: `src/grid_optimizer/loop_runner.py`
- Modify: `tests/test_loop_runner.py`

- [ ] **Step 1: Write the failing tests**

```python
from grid_optimizer.loop_runner import (
    _build_parser,
    _generate_competition_inventory_grid_plan,
    _update_inventory_grid_order_refs,
)


def test_parser_accepts_competition_inventory_grid_mode() -> None:
    parser = _build_parser()
    args = parser.parse_args(["--strategy-mode", "competition_inventory_grid"])
    assert args.strategy_mode == "competition_inventory_grid"


def test_generate_competition_inventory_grid_plan_starts_flat_with_two_bootstrap_orders() -> None:
    plan = _generate_competition_inventory_grid_plan(
        state={"inventory_grid_order_refs": {}},
        symbol="NIGHTUSDT",
        bid_price=0.0999,
        ask_price=0.1001,
        current_long_qty=0.0,
        current_short_qty=0.0,
        step_price=0.01,
        per_order_notional=10.0,
        first_order_multiplier=4.0,
        threshold_position_notional=50.0,
        max_order_position_notional=80.0,
        max_position_notional=120.0,
        tick_size=0.0001,
        step_size=1.0,
        min_qty=1.0,
        min_notional=5.0,
        trade_rows=[],
    )

    assert {item["side"] for item in plan["bootstrap_orders"]} == {"BUY", "SELL"}
    assert plan["strategy_mode"] == "competition_inventory_grid"


def test_update_inventory_grid_order_refs_tracks_placed_orders() -> None:
    state: dict[str, object] = {}

    _update_inventory_grid_order_refs(
        state=state,
        submit_report={
            "placed_orders": [
                {
                    "request": {"role": "bootstrap_entry", "side": "BUY"},
                    "response": {"orderId": 12345, "clientOrderId": "gx-night-bootstrap"},
                }
            ]
        },
    )

    refs = state["inventory_grid_order_refs"]
    assert refs["12345"]["role"] == "bootstrap_entry"
    assert refs["12345"]["side"] == "BUY"
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `python -m pytest tests/test_loop_runner.py -q`

Expected: FAIL because `_build_parser()` rejects `competition_inventory_grid` and `_generate_competition_inventory_grid_plan` does not exist yet

- [ ] **Step 3: Write the minimal implementation**

```python
from .inventory_grid_plan import build_inventory_grid_orders
from .inventory_grid_recovery import rebuild_inventory_grid_runtime


def _update_inventory_grid_order_refs(*, state: dict[str, Any], submit_report: dict[str, Any]) -> None:
    refs = state.setdefault("inventory_grid_order_refs", {})
    for item in submit_report.get("placed_orders", []):
        if not isinstance(item, dict):
            continue
        request = item.get("request", {}) if isinstance(item.get("request"), dict) else {}
        response = item.get("response", {}) if isinstance(item.get("response"), dict) else {}
        order_id = str(int(response.get("orderId", 0) or 0))
        if not order_id or order_id == "0":
            continue
        refs[order_id] = {
            "role": str(request.get("role", "") or ""),
            "side": str(request.get("side", "") or "").upper(),
            "client_order_id": str(response.get("clientOrderId", "") or ""),
        }


def _generate_competition_inventory_grid_plan(
    *,
    state: dict[str, Any],
    symbol: str,
    bid_price: float,
    ask_price: float,
    current_long_qty: float,
    current_short_qty: float,
    step_price: float,
    per_order_notional: float,
    first_order_multiplier: float,
    threshold_position_notional: float,
    max_order_position_notional: float,
    max_position_notional: float,
    tick_size: float | None,
    step_size: float | None,
    min_qty: float | None,
    min_notional: float | None,
    trade_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    runtime = rebuild_inventory_grid_runtime(
        market_type="futures",
        trades=trade_rows,
        order_refs=dict(state.get("inventory_grid_order_refs") or {}),
        step_price=step_price,
        current_position_qty=max(current_long_qty, current_short_qty),
    )
    inventory_plan = build_inventory_grid_orders(
        runtime=runtime,
        bid_price=bid_price,
        ask_price=ask_price,
        step_price=step_price,
        per_order_notional=per_order_notional,
        first_order_multiplier=first_order_multiplier,
        threshold_position_notional=threshold_position_notional,
        max_order_position_notional=max_order_position_notional,
        max_position_notional=max_position_notional,
        tick_size=tick_size,
        step_size=step_size,
        min_qty=min_qty,
        min_notional=min_notional,
    )
    inventory_plan["strategy_mode"] = "competition_inventory_grid"
    inventory_plan["grid_anchor_price"] = float(runtime.get("grid_anchor_price", 0.0) or 0.0)
    inventory_plan["direction_state"] = str(runtime.get("direction_state", "flat"))
    inventory_plan["pair_credit_steps"] = int(runtime.get("pair_credit_steps", 0) or 0)
    inventory_plan["recovery_mode"] = str(runtime.get("recovery_mode", "live"))
    return inventory_plan


# In `_build_parser()`, extend the existing futures parser with:
parser.add_argument(
    "--strategy-mode",
    type=str,
    default="one_way_long",
    choices=(
        "one_way_long",
        "one_way_short",
        "hedge_neutral",
        "synthetic_neutral",
        "inventory_target_neutral",
        "competition_inventory_grid",
    ),
)
parser.add_argument("--first-order-multiplier", type=float, default=4.0)
parser.add_argument("--threshold-position-notional", type=float, default=0.0)
parser.add_argument("--max-order-position-notional", type=float, default=0.0)
```

- [ ] **Step 4: Run the targeted futures tests**

Run: `python -m pytest tests/test_inventory_grid_state.py tests/test_inventory_grid_recovery.py tests/test_inventory_grid_plan.py tests/test_loop_runner.py -q`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/grid_optimizer/loop_runner.py tests/test_loop_runner.py src/grid_optimizer/inventory_grid_state.py src/grid_optimizer/inventory_grid_recovery.py src/grid_optimizer/inventory_grid_plan.py
git commit -m "feat: wire competition inventory grid into futures runner"
```

### Task 5: Integrate The Spot Runner

**Files:**
- Modify: `src/grid_optimizer/spot_loop_runner.py`
- Modify: `tests/test_spot_loop_runner.py`

- [ ] **Step 1: Write the failing tests**

```python
from grid_optimizer.spot_loop_runner import _build_parser, _build_spot_competition_inventory_grid_orders


def test_spot_parser_accepts_competition_inventory_grid_mode() -> None:
    parser = _build_parser()
    args = parser.parse_args(["--strategy-mode", "spot_competition_inventory_grid"])
    assert args.strategy_mode == "spot_competition_inventory_grid"


def test_build_spot_competition_inventory_grid_orders_only_places_bootstrap_buy_when_flat() -> None:
    desired, controls = _build_spot_competition_inventory_grid_orders(
        state={"inventory_grid_runtime": {}},
        bid_price=0.0999,
        ask_price=0.1001,
        symbol_info={"tick_size": 0.0001, "step_size": 1.0, "min_qty": 1.0, "min_notional": 5.0},
        total_quote_budget=100.0,
        step_price=0.01,
        per_order_notional=10.0,
        first_order_multiplier=4.0,
        threshold_position_notional=50.0,
        max_order_position_notional=80.0,
        max_position_notional=120.0,
        trades=[],
    )

    assert [item["side"] for item in desired] == ["BUY"]
    assert controls["direction_state"] == "flat"
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `python -m pytest tests/test_spot_loop_runner.py -q`

Expected: FAIL because `_build_spot_competition_inventory_grid_orders` does not exist yet

- [ ] **Step 3: Write the minimal implementation**

```python
from .inventory_grid_plan import build_inventory_grid_orders
from .inventory_grid_recovery import rebuild_inventory_grid_runtime
from .inventory_grid_state import new_inventory_grid_runtime


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Spot runner with static, volume-shift, and competition inventory-grid modes.")
    parser.add_argument("--symbol", type=str, required=True)
    parser.add_argument(
        "--strategy-mode",
        type=str,
        default="spot_one_way_long",
        choices=("spot_one_way_long", "spot_volume_shift_long", "spot_competition_inventory_grid"),
    )
    return parser


def _build_spot_competition_inventory_grid_orders(
    *,
    state: dict[str, Any],
    bid_price: float,
    ask_price: float,
    symbol_info: dict[str, Any],
    total_quote_budget: float,
    step_price: float,
    per_order_notional: float,
    first_order_multiplier: float,
    threshold_position_notional: float,
    max_order_position_notional: float,
    max_position_notional: float,
    trades: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    runtime = rebuild_inventory_grid_runtime(
        market_type="spot",
        trades=trades,
        order_refs=dict(state.get("known_orders") or {}),
        step_price=step_price,
        current_position_qty=sum(float(item.get("qty", 0.0) or 0.0) for item in state.get("inventory_lots", [])),
    )
    if not state.get("inventory_grid_runtime"):
        state["inventory_grid_runtime"] = new_inventory_grid_runtime(market_type="spot")
    state["inventory_grid_runtime"] = runtime

    plan = build_inventory_grid_orders(
        runtime=runtime,
        bid_price=bid_price,
        ask_price=ask_price,
        step_price=step_price,
        per_order_notional=per_order_notional,
        first_order_multiplier=first_order_multiplier,
        threshold_position_notional=threshold_position_notional,
        max_order_position_notional=max_order_position_notional,
        max_position_notional=max_position_notional,
        tick_size=symbol_info.get("tick_size"),
        step_size=symbol_info.get("step_size"),
        min_qty=symbol_info.get("min_qty"),
        min_notional=symbol_info.get("min_notional"),
    )
    desired_orders = plan["bootstrap_orders"] + plan["buy_orders"] + plan["sell_orders"]
    controls = {
        "mode": "spot_competition_inventory_grid",
        "direction_state": runtime["direction_state"],
        "risk_state": plan["risk_state"],
        "grid_anchor_price": runtime["grid_anchor_price"],
        "pair_credit_steps": runtime["pair_credit_steps"],
        "inventory_notional": sum(float(item["notional"]) for item in desired_orders if item["side"] == "BUY"),
    }
    return desired_orders, controls


# In `_build_parser()`, append the competition-grid-specific spot args:
parser.add_argument("--first-order-multiplier", type=float, default=4.0)
parser.add_argument("--threshold-position-notional", type=float, default=0.0)
parser.add_argument("--max-order-position-notional", type=float, default=0.0)
parser.add_argument("--max-position-notional", type=float, default=0.0)
```

- [ ] **Step 4: Run the targeted spot tests**

Run: `python -m pytest tests/test_inventory_grid_state.py tests/test_inventory_grid_recovery.py tests/test_inventory_grid_plan.py tests/test_spot_loop_runner.py -q`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/grid_optimizer/spot_loop_runner.py tests/test_spot_loop_runner.py src/grid_optimizer/inventory_grid_state.py src/grid_optimizer/inventory_grid_recovery.py src/grid_optimizer/inventory_grid_plan.py
git commit -m "feat: wire competition inventory grid into spot runner"
```

### Task 6: Operator Docs And Regression Sweep

**Files:**
- Modify: `docs/STRATEGY_EXECUTION_GUIDE.md`
- Modify: `docs/superpowers/specs/2026-04-09-unified-competition-inventory-grid-design.md` only if wording no longer matches shipped behavior

- [ ] **Step 1: Update the operator documentation**

```markdown
### `competition_inventory_grid`

- 核心：以“本策略自己的最近有效成交价”为网格锚点，而不是市场最新成交或普通中心迁移。
- `futures flat` 时双向贴盘口等首单；首单决定方向。
- 强制减仓和尾仓清理不会更新锚点。
- `threshold_position_notional` 先看配对步长积分是否够覆盖直接减仓代价；不够则暂停同方向扩仓。
- `max_position_notional` 进入硬 reduce-only，只保留减仓相关订单。

### `spot_competition_inventory_grid`

- 核心：现货底仓滚动版的同一套引擎。
- `flat` 时只挂买侧首单，不允许净卖空。
- 重启后恢复 lot 与锚点，但 `pair_credit_steps` 始终从 `0` 开始。
```

- [ ] **Step 2: Run the full targeted regression suite**

Run: `python -m pytest tests/test_inventory_grid_state.py tests/test_inventory_grid_recovery.py tests/test_inventory_grid_plan.py tests/test_loop_runner.py tests/test_spot_loop_runner.py -q`

Expected: PASS

- [ ] **Step 3: Run one CLI smoke check per runner**

Run: `PYTHONPATH=src python3 -m grid_optimizer.loop_runner --help`

Expected: PASS and help output includes `competition_inventory_grid`

Run: `PYTHONPATH=src python3 -m grid_optimizer.spot_loop_runner --help`

Expected: PASS and help output includes `spot_competition_inventory_grid`

- [ ] **Step 4: Review git diff for accidental spillover**

Run: `git diff -- src/grid_optimizer/loop_runner.py src/grid_optimizer/spot_loop_runner.py src/grid_optimizer/inventory_grid_state.py src/grid_optimizer/inventory_grid_recovery.py src/grid_optimizer/inventory_grid_plan.py tests/test_inventory_grid_state.py tests/test_inventory_grid_recovery.py tests/test_inventory_grid_plan.py tests/test_loop_runner.py tests/test_spot_loop_runner.py docs/STRATEGY_EXECUTION_GUIDE.md`

Expected: Only the new shared inventory-grid engine, the two runner integrations, tests, and docs changes appear

- [ ] **Step 5: Commit**

```bash
git add docs/STRATEGY_EXECUTION_GUIDE.md src/grid_optimizer/inventory_grid_state.py src/grid_optimizer/inventory_grid_recovery.py src/grid_optimizer/inventory_grid_plan.py src/grid_optimizer/loop_runner.py src/grid_optimizer/spot_loop_runner.py tests/test_inventory_grid_state.py tests/test_inventory_grid_recovery.py tests/test_inventory_grid_plan.py tests/test_loop_runner.py tests/test_spot_loop_runner.py
git commit -m "feat: add unified competition inventory grid runners"
```
