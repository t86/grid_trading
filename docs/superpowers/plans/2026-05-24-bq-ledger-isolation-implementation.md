# BQ Ledger Isolation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make PHAROS best-quote normal brushing independent from frozen-inventory experiment and cleanup trades.

**Architecture:** Keep existing state keys for compatibility, but add explicit book ownership to BQ order refs and fill processing. Normal brushing continues to use `best_quote_volume_ledger`; frozen cleanup stays in `best_quote_frozen_inventory`; runtime guards count only normal-book fills for the 40k Beijing-day cap.

**Tech Stack:** Python, pytest, JSON state files, existing `grid_optimizer.loop_runner` and `grid_optimizer.runtime_guards`.

---

## File Structure

- Modify `src/grid_optimizer/loop_runner.py`
  - Add BQ book constants/helpers.
  - Add `book` to `best_quote_volume_order_refs`.
  - Make `sync_best_quote_volume_ledger()` apply only `normal_bq` fills.
  - Track frozen/unknown skipped fill diagnostics without mutating normal lots or gross notional.
  - Preserve ledger keys during reset/config-signature changes if needed by existing reset code.
- Modify `src/grid_optimizer/runtime_guards.py`
  - Add optional BQ order refs path/scope arguments to summarize futures guard inputs.
  - Exclude rows whose order ref says `frozen_bq` or `unknown` from cumulative gross notional when BQ normal scope is requested.
- Modify `tests/test_loop_runner.py`
  - Add tests for order ref book writing and normal ledger fill filtering.
  - Add reset preservation coverage if current preservation is incomplete.
- Modify `tests/test_runtime_guards.py`
  - Add tests proving the runtime cap ignores frozen-book and unknown-book fills.
- Create or modify deploy wrapper tests only after ledger/runtime changes are green.

## Tasks

### Task 1: Add Explicit BQ Order Book Ownership

**Files:**
- Modify: `src/grid_optimizer/loop_runner.py`
- Test: `tests/test_loop_runner.py`

- [ ] **Step 1: Write failing test for normal order refs**

Add this test near the existing order-ref and BQ ledger tests:

```python
def test_update_best_quote_volume_order_refs_marks_normal_book(self) -> None:
    with TemporaryDirectory() as tmpdir:
        state_path = Path(tmpdir) / "state.json"
        state_path.write_text("{}", encoding="utf-8")
        update_best_quote_volume_order_refs(
            state_path=state_path,
            strategy_mode="hedge_best_quote_maker_volume",
            submit_report={
                "placed_orders": [
                    {
                        "request": {
                            "role": "best_quote_entry_short",
                            "side": "SELL",
                            "position_side": "SHORT",
                        },
                        "response": {
                            "orderId": 41974646,
                            "clientOrderId": "gx-pharosu-bestquot-1-87716360",
                        },
                    }
                ]
            },
        )

        state = json.loads(state_path.read_text(encoding="utf-8"))

    ref = state["best_quote_volume_order_refs"]["41974646"]
    self.assertEqual(ref["book"], "normal_bq")
    self.assertEqual(ref["role"], "best_quote_entry_short")
    self.assertEqual(ref["position_side"], "SHORT")
```

- [ ] **Step 2: Write failing test for frozen order refs**

Add:

```python
def test_update_best_quote_volume_order_refs_marks_frozen_book(self) -> None:
    with TemporaryDirectory() as tmpdir:
        state_path = Path(tmpdir) / "state.json"
        state_path.write_text("{}", encoding="utf-8")
        update_best_quote_volume_order_refs(
            state_path=state_path,
            strategy_mode="hedge_best_quote_maker_volume",
            submit_report={
                "placed_orders": [
                    {
                        "request": {
                            "role": "frozen_inventory_manual_reduce_long",
                            "side": "SELL",
                            "position_side": "LONG",
                        },
                        "response": {
                            "orderId": 41974647,
                            "clientOrderId": "gx-pharosu-frozen-1-87716361",
                        },
                    }
                ]
            },
        )

        state = json.loads(state_path.read_text(encoding="utf-8"))

    ref = state["best_quote_volume_order_refs"]["41974647"]
    self.assertEqual(ref["book"], "frozen_bq")
    self.assertEqual(ref["role"], "frozen_inventory_manual_reduce_long")
```

- [ ] **Step 3: Run tests to verify they fail**

Run:

```bash
PYTHONPATH=src PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 python -m pytest tests/test_loop_runner.py -q -k "update_best_quote_volume_order_refs_marks"
```

Expected: both tests fail because `book` is missing.

- [ ] **Step 4: Implement minimal book inference**

In `src/grid_optimizer/loop_runner.py`, add helpers near `_best_quote_trade_role_from_row`:

```python
BQ_BOOK_NORMAL = "normal_bq"
BQ_BOOK_FROZEN = "frozen_bq"
BQ_BOOK_UNKNOWN = "unknown"


def _best_quote_order_book_from_role(role: Any) -> str:
    normalized = str(role or "").lower().strip()
    if normalized in {
        "best_quote_entry_long",
        "best_quote_reduce_long",
        "best_quote_entry_short",
        "best_quote_reduce_short",
    }:
        return BQ_BOOK_NORMAL
    if (
        normalized.startswith("frozen_inventory_manual_reduce_")
        or normalized.startswith("frozen_inventory_manual_limit_")
        or normalized.startswith("frozen_inventory_pair_release_")
    ):
        return BQ_BOOK_FROZEN
    return BQ_BOOK_UNKNOWN
```

Then update `update_best_quote_volume_order_refs()` so each ref includes:

```python
"book": _best_quote_order_book_from_role(request.get("role")),
```

- [ ] **Step 5: Run tests to verify they pass**

Run the same pytest command. Expected: pass.

- [ ] **Step 6: Commit**

```bash
git add src/grid_optimizer/loop_runner.py tests/test_loop_runner.py
git commit -m "Add BQ order book ownership refs"
```

### Task 2: Make Normal Ledger Ignore Frozen And Unknown Fills

**Files:**
- Modify: `src/grid_optimizer/loop_runner.py`
- Test: `tests/test_loop_runner.py`

- [ ] **Step 1: Write failing test for frozen fill isolation**

Add:

```python
def test_best_quote_volume_ledger_ignores_frozen_book_fill(self) -> None:
    state = {
        "best_quote_volume_ledger": {
            "initialized": True,
            "sync_ok": True,
            "long_lots": [],
            "short_lots": [],
            "gross_notional": 0.0,
            "last_trade_time_ms": 1,
            "last_trade_keys_at_time": [],
        },
        "best_quote_volume_order_refs": {
            "41974647": {
                "book": "frozen_bq",
                "role": "frozen_inventory_manual_reduce_long",
                "side": "SELL",
                "position_side": "LONG",
            }
        },
    }

    sync_best_quote_volume_ledger(
        state=state,
        symbol="PHAROSUSDT",
        api_key="",
        api_secret="",
        recv_window=5000,
        current_long_qty=0.0,
        current_short_qty=0.0,
        current_long_avg_price=0.0,
        current_short_avg_price=0.0,
        mid_price=0.1,
        observed_trade_rows=[
            {
                "orderId": 41974647,
                "side": "SELL",
                "positionSide": "LONG",
                "qty": "100",
                "price": "0.1",
                "quoteQty": "10",
                "time": 2000,
            }
        ],
        allow_exchange_position_bootstrap=False,
    )

    ledger = state["best_quote_volume_ledger"]
    self.assertEqual(ledger["gross_notional"], 0.0)
    self.assertEqual(ledger["applied_trade_count_total"], 0)
    self.assertEqual(ledger["last_skipped_frozen_trade_count"], 1)
```

- [ ] **Step 2: Write failing test for unknown fill isolation**

Add:

```python
def test_best_quote_volume_ledger_ignores_unknown_book_fill(self) -> None:
    state = {
        "best_quote_volume_ledger": {
            "initialized": True,
            "sync_ok": True,
            "long_lots": [],
            "short_lots": [],
            "gross_notional": 0.0,
            "last_trade_time_ms": 1,
            "last_trade_keys_at_time": [],
        },
        "best_quote_volume_order_refs": {
            "41974648": {
                "book": "unknown",
                "role": "",
                "side": "BUY",
                "position_side": "LONG",
            }
        },
    }

    sync_best_quote_volume_ledger(
        state=state,
        symbol="PHAROSUSDT",
        api_key="",
        api_secret="",
        recv_window=5000,
        current_long_qty=0.0,
        current_short_qty=0.0,
        current_long_avg_price=0.0,
        current_short_avg_price=0.0,
        mid_price=0.1,
        observed_trade_rows=[
            {
                "orderId": 41974648,
                "side": "BUY",
                "positionSide": "LONG",
                "qty": "100",
                "price": "0.1",
                "quoteQty": "10",
                "time": 2000,
            }
        ],
        allow_exchange_position_bootstrap=False,
    )

    ledger = state["best_quote_volume_ledger"]
    self.assertEqual(ledger["gross_notional"], 0.0)
    self.assertEqual(ledger["long_qty"], 0.0)
    self.assertEqual(ledger["last_unknown_trade_count"], 1)
```

- [ ] **Step 3: Verify red**

Run:

```bash
PYTHONPATH=src PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 python -m pytest tests/test_loop_runner.py -q -k "ignores_frozen_book_fill or ignores_unknown_book_fill"
```

Expected: tests fail because frozen/unknown order-ref rows can still be inferred by side/position and applied to normal ledger, and diagnostics are absent.

- [ ] **Step 4: Implement fill book classification**

Add helpers in `loop_runner.py`:

```python
def _best_quote_order_ref_for_trade(
    row: Mapping[str, Any] | dict[str, Any],
    state: Mapping[str, Any] | dict[str, Any],
) -> Mapping[str, Any] | None:
    refs = state.get("best_quote_volume_order_refs") if isinstance(state, Mapping) else None
    if not isinstance(refs, Mapping):
        return None
    order_id = str((row or {}).get("orderId") or (row or {}).get("order_id") or "").strip()
    ref = refs.get(order_id) if order_id else None
    return ref if isinstance(ref, Mapping) else None


def _best_quote_trade_book_from_order_ref(
    row: Mapping[str, Any] | dict[str, Any],
    state: Mapping[str, Any] | dict[str, Any],
) -> str:
    ref = _best_quote_order_ref_for_trade(row, state)
    if ref is None:
        return BQ_BOOK_UNKNOWN
    book = str(ref.get("book") or "").lower().strip()
    if book in {BQ_BOOK_NORMAL, BQ_BOOK_FROZEN, BQ_BOOK_UNKNOWN}:
        return book
    return _best_quote_order_book_from_role(ref.get("role"))
```

In `sync_best_quote_volume_ledger()`, before role inference, compute:

```python
book = _best_quote_trade_book_from_order_ref(row, state)
if book == BQ_BOOK_FROZEN:
    skipped_frozen += 1
    continue
if book != BQ_BOOK_NORMAL:
    unknown += 1
    continue
```

Then role should come from order ref first, and legacy client-id inference should be used only when there is no order ref and the legacy row clearly has a normal best quote client id.

Store diagnostics:

```python
ledger["last_skipped_frozen_trade_count"] = skipped_frozen
ledger["last_unknown_trade_count"] = unknown
```

- [ ] **Step 5: Verify green**

Run the same pytest command. Expected: pass.

- [ ] **Step 6: Run focused ledger regression**

Run:

```bash
PYTHONPATH=src PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 python -m pytest tests/test_loop_runner.py -q -k "best_quote_volume_ledger"
```

Expected: existing normal ledger tests pass.

- [ ] **Step 7: Commit**

```bash
git add src/grid_optimizer/loop_runner.py tests/test_loop_runner.py
git commit -m "Isolate frozen BQ fills from normal ledger"
```

### Task 3: Make Runtime Guard 40k Cap Count Normal Book Only

**Files:**
- Modify: `src/grid_optimizer/runtime_guards.py`
- Modify: `src/grid_optimizer/loop_runner.py`
- Test: `tests/test_runtime_guards.py`

- [ ] **Step 1: Write failing runtime guard input test**

Add to `tests/test_runtime_guards.py`:

```python
def test_summarize_futures_runtime_guard_inputs_counts_only_normal_bq_book(self) -> None:
    with TemporaryDirectory() as tmpdir:
        summary_path = Path(tmpdir) / "summary.jsonl"
        audit_paths = build_audit_paths(summary_path)
        audit_paths["trade_audit"].parent.mkdir(parents=True, exist_ok=True)
        rows = [
            {
                "orderId": 1,
                "price": "1",
                "qty": "100",
                "quoteQty": "100",
                "time": 1766550000000,
            },
            {
                "orderId": 2,
                "price": "1",
                "qty": "500",
                "quoteQty": "500",
                "time": 1766550001000,
            },
            {
                "orderId": 3,
                "price": "1",
                "qty": "900",
                "quoteQty": "900",
                "time": 1766550002000,
            },
        ]
        audit_paths["trade_audit"].write_text(
            "".join(json.dumps(row) + "\n" for row in rows),
            encoding="utf-8",
        )
        refs_path = Path(tmpdir) / "state.json"
        refs_path.write_text(
            json.dumps(
                {
                    "best_quote_volume_order_refs": {
                        "1": {"book": "normal_bq"},
                        "2": {"book": "frozen_bq"},
                        "3": {"book": "unknown"},
                    }
                }
            ),
            encoding="utf-8",
        )

        gross, pnl_events, _ = summarize_futures_runtime_guard_inputs(
            summary_path,
            runtime_guard_stats_start_time="2025-12-24T00:00:00+00:00",
            bq_order_refs_path=refs_path,
            bq_book_scope="normal_bq",
        )

    self.assertEqual(gross, 100.0)
    self.assertEqual([event["order_id"] for event in pnl_events], [1])
```

- [ ] **Step 2: Verify red**

Run:

```bash
PYTHONPATH=src PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 python -m pytest tests/test_runtime_guards.py -q -k "counts_only_normal_bq_book"
```

Expected: fails because `summarize_futures_runtime_guard_inputs()` does not accept the new arguments.

- [ ] **Step 3: Implement optional BQ scope filtering**

In `runtime_guards.py`, extend function signature:

```python
def summarize_futures_runtime_guard_inputs(
    summary_path: Path,
    *,
    runtime_guard_stats_start_time: Any = None,
    symbol: str | None = None,
    now: datetime | None = None,
    bq_order_refs_path: Path | None = None,
    bq_book_scope: str | None = None,
) -> tuple[float, list[dict[str, Any]], datetime | None]:
```

Load refs once:

```python
order_books: dict[str, str] = {}
if bq_order_refs_path is not None and bq_book_scope:
    try:
        raw_state = json.loads(bq_order_refs_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        raw_state = {}
    raw_refs = raw_state.get("best_quote_volume_order_refs") if isinstance(raw_state, dict) else {}
    if isinstance(raw_refs, dict):
        for order_id, ref in raw_refs.items():
            if isinstance(ref, dict):
                order_books[str(order_id)] = str(ref.get("book") or "unknown").lower().strip() or "unknown"
```

Before adding notional/PnL for each trade row:

```python
if bq_book_scope:
    order_id = str(row.get("orderId") or row.get("order_id") or "").strip()
    if order_books.get(order_id, "unknown") != bq_book_scope:
        continue
```

Add `import json` if not present.

- [ ] **Step 4: Wire loop runner runtime guard input loading**

In `_load_futures_runtime_guard_inputs()` in `loop_runner.py`, add optional `state_path: Path | None = None` and `bq_book_scope: str | None = None`, then pass those into `summarize_futures_runtime_guard_inputs()`.

At the two call sites where PHAROS BQ runtime guard is evaluated, pass:

```python
bq_book_scope="normal_bq" if _is_hedge_best_quote_maker_volume_mode(strategy_mode) else None
state_path=state_path
```

Use the actual local strategy mode variable available at each call site.

- [ ] **Step 5: Verify green**

Run:

```bash
PYTHONPATH=src PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 python -m pytest tests/test_runtime_guards.py tests/test_loop_runner_runtime_guard_flatten.py -q
```

Expected: pass.

- [ ] **Step 6: Commit**

```bash
git add src/grid_optimizer/runtime_guards.py src/grid_optimizer/loop_runner.py tests/test_runtime_guards.py
git commit -m "Scope BQ runtime cap to normal book"
```

### Task 4: Preserve Ledger Ownership Across Reset

**Files:**
- Modify: `src/grid_optimizer/loop_runner.py`
- Test: `tests/test_loop_runner.py`

- [ ] **Step 1: Confirm current reset preservation**

Run:

```bash
PYTHONPATH=src PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 python -m pytest tests/test_loop_runner.py -q -k "reset_state_preserves_best_quote_frozen_inventory"
```

Expected: pass on current branch.

- [ ] **Step 2: Add test for order refs and applied fill keys preservation**

Extend reset preservation coverage to include:

```python
"best_quote_volume_order_refs": {
    "41974646": {"book": "normal_bq", "role": "best_quote_entry_short"},
    "41974647": {"book": "frozen_bq", "role": "frozen_inventory_manual_reduce_long"},
},
"best_quote_volume_ledger": {
    "initialized": True,
    "sync_ok": True,
    "long_lots": [],
    "short_lots": [],
    "applied_trade_fill_keys": ["41974646:SELL:SHORT:2000:100:0.1"],
},
```

Assert both keys are still present after reset.

- [ ] **Step 3: Verify red or existing green**

Run the same reset test. If it already passes, keep the test as regression coverage.

- [ ] **Step 4: Implement preservation only if red**

Add missing keys to the reset preservation allow-list:

```python
"best_quote_volume_order_refs",
"best_quote_volume_ledger",
```

Do not clear or rewrite `book` values during reset.

- [ ] **Step 5: Commit**

```bash
git add src/grid_optimizer/loop_runner.py tests/test_loop_runner.py
git commit -m "Preserve BQ ledger ownership on reset"
```

### Task 5: Full Verification

**Files:**
- Existing tests only.

- [ ] **Step 1: Run focused BQ test set**

```bash
PYTHONPATH=src PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 python -m pytest tests/test_loop_runner.py tests/test_runtime_guards.py -q -k "best_quote or runtime_guard"
```

Expected: pass.

- [ ] **Step 2: Run existing baseline from design phase**

```bash
PYTHONPATH=src PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 python -m pytest tests/test_loop_runner.py -q -k "best_quote_volume_ledger or best_quote_frozen_pair_release or reduce_freeze"
```

Expected: pass.

- [ ] **Step 3: Static checks**

```bash
git diff --check
python -m compileall -q src/grid_optimizer
```

Expected: both commands exit 0.

- [ ] **Step 4: Final commit if any verification-only fixes were needed**

```bash
git status --short
```

Expected: clean working tree after commits.

## Self-Review

- Spec coverage: tasks cover explicit order ownership, fill routing, normal cap isolation, reset/state preservation, and focused verification.
- Deferred from this implementation plan: production wrapper branch/disk guards are documented in the design but should be a separate deployment-safety plan because they touch server scripts and operational flow, not ledger correctness.
- Placeholder scan: no task uses placeholders; every code-changing step includes concrete code or exact behavior.
- Type consistency: `book` values are `normal_bq`, `frozen_bq`, and `unknown` across tests and implementation.
