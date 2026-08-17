# ARX Single Recovery Owner Release Candidate Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Merge the latest `main` improvements into `codex/arx-single-recovery-owner`, close the audited production blockers, and validate the resulting branch as a release candidate in an isolated server checkout.

**Architecture:** Keep `main`'s latest maker-flow behavior while retaining the branch's single recovery owner and durable baseline protocol. Make profit-gated completion fail closed on incomplete attribution, fence runner effects against the exact recovery generation/decision/stage/epoch, and reject or defer baseline mutations before they can create stale run contracts. Production services remain untouched until the branch is promoted through `main`.

**Tech Stack:** Python 3, pytest, shell deployment wrappers, Git worktrees, systemd inspection over SSH.

---

### Task 1: Merge the latest main baseline

**Files:**
- Merge: `main` into `codex/arx-single-recovery-owner`
- Verify: `src/grid_optimizer/best_quote_maker_volume.py`
- Verify: `src/grid_optimizer/loop_runner.py`
- Verify: `tests/test_best_quote_maker_volume.py`
- Verify: `tests/test_loop_runner.py`

- [ ] **Step 1: Record both parents**

Run:

```bash
git rev-parse HEAD main origin/main
git status --short --branch
```

Expected: clean target worktree; `HEAD=13b3b5fa...`; local `main=c9d9c73c...`.

- [ ] **Step 2: Merge local main**

Run:

```bash
git merge --no-ff main -m "Merge main into arx single recovery owner"
```

Expected: merge completes without discarding either the recovery coordinator changes or main's paired-flow fixes.

- [ ] **Step 3: Run the main-only strategy regressions**

Run:

```bash
PYTHONPATH=.:src .venv/bin/pytest -q tests/test_best_quote_maker_volume.py tests/test_loop_runner.py
```

Expected: only already-documented baseline failures may remain; no failures in the five main-only paired-flow fixes.

### Task 2: Make target profit proof complete and fail closed

**Files:**
- Modify: `src/grid_optimizer/runtime_guards.py`
- Modify: `src/grid_optimizer/loop_runner.py`
- Test: `tests/test_runtime_guards.py`
- Test: `tests/test_loop_runner.py`

- [ ] **Step 1: Add failing event-completeness tests**

Add tests proving that:

```python
assert summarize_runtime_total_pnl(
    [{"ts": NOW.isoformat(), "net_pnl": 1.0, "pnl_observation_available": False}],
    start_time=None,
    now=NOW,
    unrealized_pnl=0.0,
) is None

assert summarize_runtime_total_pnl(
    [{"ts": NOW.isoformat(), "net_pnl": 1.0}],
    start_time=None,
    now=NOW,
    unrealized_pnl=float("nan"),
) is None
```

Also add a runtime input test where a non-zero BNB commission marks the event unavailable for a USDT profit gate.

- [ ] **Step 2: Add a failing normal-ledger PnL test**

Construct normal `bestquot`, `hardloss`, `inventory`, `rc`, `tlr`, and funding events with frozen inventory. Assert that profit completion never reduces them to only the `bestquot` event; if funding cannot be attributed between ordinary and frozen positions, assert total PnL is unavailable.

- [ ] **Step 3: Verify RED**

Run:

```bash
PYTHONPATH=.:src .venv/bin/pytest -q tests/test_runtime_guards.py tests/test_loop_runner.py -k 'target_total_pnl or target_profit or non_quote_commission or normal_pnl'
```

Expected: failures show invalid uPnL/commission or the second best-quote-only filter incorrectly proving profit.

- [ ] **Step 4: Implement strict profit evidence**

Implement the following behavior:

```python
def summarize_runtime_total_pnl(...):
    if unrealized_pnl is None or not math.isfinite(float(unrealized_pnl)):
        return None
    for event in pnl_events:
        if event.get("pnl_observation_available") is False:
            return None
        event_pnl = _event_net_pnl(event)
        if not math.isfinite(event_pnl):
            return None
    ...
```

Mark malformed realized PnL, malformed commission, and non-zero non-quote commission as unavailable. Mark income rows with `pnl_event_type="income"`; when frozen inventory exists, income without an ordinary/frozen allocation makes target PnL unavailable. Use the already `normal_bq`-scoped event list for target PnL and keep `_runtime_guard_isolated_bq_pnl_events` only for its original loss-recovery purpose. Add `strategy_unrealized_pnl_available` to the plan report and do not coerce unavailable/invalid target uPnL to zero.

- [ ] **Step 5: Remove the unrelated inventory-unlock price clamp**

Delete the `64399d5d` additions that force long release price above `floor_price` or short release price below `ceiling_price`, and restore the main assertions. This is unrelated to profit-gated completion and reverses established inventory-unlock behavior.

- [ ] **Step 6: Verify GREEN**

Run the Task 2 tests again and expect all selected profit-proof tests to pass.

### Task 3: Make baseline changes safe at submission time

**Files:**
- Modify: `src/grid_optimizer/futures_recovery_coordinator.py`
- Modify: `src/grid_optimizer/web.py`
- Test: `tests/test_futures_recovery_baseline_change.py`
- Test: `tests/test_web_security.py`

- [ ] **Step 1: Add failing non-STABLE zero-write tests**

For ACTIVE, CLEANING, RESTORING, STOP_PENDING, or any state with a lease, cleanup obligation, or pending effect, call `change_baseline()` and assert:

```python
assert outcome.status is BaselineChangeStatus.DEFERRED
assert store.read("ARXUSDT") == before
assert store.commit_count("ARXUSDT") == before_commits
```

Add a daily rollover regression proving that an expired candidate cannot be stored during recovery and later applied after returning to STABLE.

- [ ] **Step 2: Add failing Web owner/source tests**

Register a bounded baseline with `futures_run_contract_owner`, then submit a Web edit to `max_cumulative_notional` or `target_min_total_pnl`. Assert rejection with `explicit run contract handoff is required` and no baseline record. Submit `source="trusted_resume"` and assert rejection before storage.

- [ ] **Step 3: Verify RED**

Run:

```bash
PYTHONPATH=.:src .venv/bin/pytest -q tests/test_futures_recovery_baseline_change.py tests/test_web_security.py -k 'non_stable or expired or contract_change or trusted_resume'
```

Expected: current code persists non-STABLE requests and accepts the inconsistent owner/source payload.

- [ ] **Step 4: Implement strict baseline admission**

Add one predicate equivalent to:

```python
return (
    state.phase is RecoveryPhase.STABLE
    and state.active_action is ActionId.NOOP
    and state.action_lease is None
    and state.safety_lease is None
    and state.cleanup_obligation is None
    and state.pending_effect_stage is EffectStage.NONE
    and state.pending_effect_epoch is None
)
```

Return a zero-write DEFERRED outcome when it is false. In the registered Web path, run `bind_run_contract_owner(candidate, activated_at=requested_at)` without a handoff reason so immutable drift is rejected before creating `BaselineChange`. Reject the reserved `trusted_resume` source from HTTP input.

- [ ] **Step 5: Verify GREEN**

Run the Task 3 tests again and expect the new cases to pass.

### Task 4: Fence restart and stop effects

**Files:**
- Modify: `src/grid_optimizer/futures_recovery_coordinator.py`
- Modify: `src/grid_optimizer/bq_volume_recovery_guard.py`
- Modify: `src/grid_optimizer/futures_recovery_store.py` only if a public lock-held decoder is required
- Test: `tests/test_futures_recovery_coordinator.py`
- Test: `tests/test_bq_volume_recovery_guard.py`

- [ ] **Step 1: Add failing command identity tests**

Extend `EffectCommand` with the planned generation and assert coordinator-created commands carry `plan.next_state.generation`.

- [ ] **Step 2: Add a failing stale restart test**

Persist decision A with pending `RUNNER_RESTART`, then replace it with decision B/`RUNNER_STOP` before invoking A's effect. Assert the restart callback is never called and the stale command returns a visible error.

- [ ] **Step 3: Verify RED**

Run:

```bash
PYTHONPATH=.:src .venv/bin/pytest -q tests/test_futures_recovery_coordinator.py tests/test_bq_volume_recovery_guard.py -k 'effect_command or stale_restart or generation_fence'
```

Expected: the current command has no generation and the old restart callback still runs.

- [ ] **Step 4: Implement the lock-held fence**

Build every command with `(generation, decision_id, stage, effect_epoch)`. For runner restart/stop, acquire the existing per-symbol actuator lock, decode the current dual-slot recovery state without reacquiring that lock, and require:

```python
current.generation == command.generation
current.decision_id == command.decision_id
current.pending_effect_stage is command.stage
current.pending_effect_epoch == command.effect_epoch
current.desired_runner_state == ("running" if restart else "stopped")
```

Hold the fence through the wrapper's process mutation and only then record the receipt. A mismatch must fail before calling the wrapper.

- [ ] **Step 5: Verify GREEN**

Run the Task 4 tests again and expect both current-command execution and stale-command rejection to pass.

### Task 5: Preserve legacy behavior outside registered recovery

**Files:**
- Modify: `src/grid_optimizer/submit_plan.py`
- Test: `tests/test_submit_plan.py`

- [ ] **Step 1: Add the failing registration-boundary test**

Create two cases with identical ordinary and authorized frozen orders:

```python
legacy_actions = {"place_orders": [...], "cancel_orders": []}
registered_actions = {
    **legacy_actions,
    "recovery_profile_gate": {
        "managed": True,
        "authorized": True,
        "current_gate": {"active_action": "noop"},
    },
}
```

Assert the legacy case keeps main's `ordinary_with_frozen` result while the registered case selects only the frozen lane and defers ordinary mutations.

- [ ] **Step 2: Verify RED**

Run:

```bash
PYTHONPATH=.:src .venv/bin/pytest -q tests/test_submit_plan.py -k 'ordinary_flow_with_frozen_pair or registration_boundary'
```

Expected: the target branch serializes both cases.

- [ ] **Step 3: Implement the registration gate**

Restore main's combined ordinary/frozen block only when `recovery_profile_gate.managed` is false. Keep one-lane serialization for explicitly registered recovery symbols.

- [ ] **Step 4: Verify GREEN**

Run the Task 5 tests again and expect both boundary cases to pass.

### Task 6: Release-candidate verification and publication

**Files:**
- Verify all files changed since `main`
- Update: this plan's checkbox state only if useful; no generated artifacts

- [ ] **Step 1: Run focused regression suites**

Run:

```bash
PYTHONPATH=.:src .venv/bin/pytest -q \
  tests/test_best_quote_maker_volume.py \
  tests/test_competition_daily_rollover.py \
  tests/test_competition_ops_stack.py \
  tests/test_deploy_scripts.py \
  tests/test_futures_recovery_baseline_change.py \
  tests/test_futures_recovery_coordinator.py \
  tests/test_futures_run_lifecycle.py \
  tests/test_futures_terminal_ownership.py \
  tests/test_loop_runner.py \
  tests/test_runtime_guards.py \
  tests/test_submit_plan.py \
  tests/test_web_security.py
```

Expected: no new failures relative to the documented baseline; release-blocker tests all pass.

- [ ] **Step 2: Run static repository checks**

Run:

```bash
git diff --check main...HEAD
git status --short
```

Expected: no whitespace errors and only intentional source/test/plan changes.

- [ ] **Step 3: Commit the fixes**

Run:

```bash
git add docs/superpowers/plans/2026-08-17-arx-single-recovery-owner-release-candidate.md \
  src/grid_optimizer tests
git commit -m "fix: close recovery release blockers"
```

- [ ] **Step 4: Push the candidate branch**

Run:

```bash
git push -u origin codex/arx-single-recovery-owner
```

Expected: remote branch resolves to the verified local commit.

### Task 7: Pull-based isolated server validation

**Files:**
- Server candidate: inspect `114` and `150`; prefer `150` if it has sufficient disk and no conflicting test worktree
- Do not modify: live control JSON, live runner state, live systemd unit, exchange orders, positions

- [ ] **Step 1: Inspect both servers read-only**

Run SSH checks for repository HEAD/status, disk space, Python version, service status, and active runner symbols. Do not restart anything.

- [ ] **Step 2: Create a server-local candidate worktree**

On the selected server, fetch the pushed branch and create a separate `/tmp` or operator-owned test worktree from `origin/codex/arx-single-recovery-owner`. Do not switch the production repository checkout.

- [ ] **Step 3: Run release-blocker tests on the server**

Use the server repository's virtualenv with `PYTHONPATH=<candidate>:<candidate>/src` to run Tasks 2–5 tests. Confirm server HEAD equals the pushed candidate commit.

- [ ] **Step 4: Report the promotion gate**

Record server, commit, test counts, and untouched service/runner state. Promotion into `main`, update-wrapper deployment, and any real runner restart remain a separate explicit step after this isolated test succeeds.
