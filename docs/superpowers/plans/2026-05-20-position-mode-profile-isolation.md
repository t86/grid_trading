# Position Mode Profile Isolation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** Add profile-level position-mode isolation so one-way remains the default while hedge profiles must explicitly opt in and are blocked on incompatible accounts.

**Architecture:** `strategy_profile_schema.py` owns profile metadata and default `one_way` resolution. `loop_runner.py` and `submit_plan.py` enforce mode compatibility from plan/profile metadata. `web.py` exposes mode metadata in presets and the lightweight strategy editor, with AIGENSYN one-way recover as the production-safe default and a hedge sprint preset as explicit opt-in.

**Tech Stack:** Python 3, argparse, stdlib unittest/pytest, existing embedded HTML/JS in `src/grid_optimizer/web.py`.

---

### Task 1: Profile Mode Schema

**Files:**
- Modify: `src/grid_optimizer/strategy_profile_schema.py`
- Test: `tests/test_strategy_profile_schema.py`

- [x] **Step 1: Write failing schema tests**

Add tests proving:

```python
def test_best_quote_defaults_to_one_way_position_mode(self) -> None:
    _, report = apply_strategy_profile_schema(_args(strategy_profile="legacy_profile_without_mode"), enabled=True)

    self.assertEqual(report["required_position_mode"], "one_way")
    self.assertTrue(report["required_position_mode_defaulted"])


def test_explicit_hedge_profile_reports_hedge_position_mode(self) -> None:
    _, report = apply_strategy_profile_schema(
        _args(strategy_mode="hedge_neutral", strategy_profile="aigensyn_hedge_bq_pingpong_sprint_v1"),
        enabled=True,
    )

    self.assertEqual(report["required_position_mode"], "hedge")
    self.assertFalse(report["required_position_mode_defaulted"])
    self.assertEqual(report["profile_family"], "hedge_bq_ping_pong")
```

- [x] **Step 2: Run schema tests and verify RED**

Run:

```bash
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 PYTHONPATH=src python -m pytest tests/test_strategy_profile_schema.py -q
```

Expected: fail because mode metadata does not exist yet.

- [x] **Step 3: Implement schema metadata**

Change `StrategyProfileSchema` to include:

```python
required_position_mode: str
required_position_mode_defaulted: bool = False
```

Add constants:

```python
ONE_WAY_POSITION_MODE = "one_way"
HEDGE_POSITION_MODE = "hedge"
VALID_POSITION_MODES = frozenset({ONE_WAY_POSITION_MODE, HEDGE_POSITION_MODE})
```

Return `required_position_mode="one_way"` for existing one-way/best-quote/synthetic schemas. Return `required_position_mode="hedge"` for `strategy_mode == "hedge_neutral"` or profiles beginning `aigensyn_hedge_`. Unknown legacy profiles default to `one_way` with `required_position_mode_defaulted=True`.

Include `required_position_mode` and `required_position_mode_defaulted` in the report.

- [x] **Step 4: Run schema tests and verify GREEN**

Run:

```bash
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 PYTHONPATH=src python -m pytest tests/test_strategy_profile_schema.py -q
```

Expected: all schema tests pass.

### Task 2: Runner And Submit Mode Compatibility

**Files:**
- Modify: `src/grid_optimizer/loop_runner.py`
- Modify: `src/grid_optimizer/submit_plan.py`
- Test: `tests/test_submit_plan.py`
- Test: `tests/test_loop_runner.py`

- [x] **Step 1: Write failing submit validation tests**

Add tests to `tests/test_submit_plan.py`:

```python
def test_validate_plan_report_rejects_hedge_account_for_one_way_required_profile(self) -> None:
    now = datetime(2026, 5, 20, 10, 0, tzinfo=timezone.utc)
    report = {
        "symbol": "AIGENSYNUSDT",
        "generated_at": now.isoformat(),
        "dual_side_position": True,
        "required_position_mode": "one_way",
        "missing_orders": [{"side": "BUY", "price": 0.02, "qty": 100.0, "notional": 2.0}],
        "stale_orders": [],
    }

    validation = validate_plan_report(
        plan_report=report,
        allow_symbol="AIGENSYNUSDT",
        max_new_orders=8,
        max_total_notional=500.0,
        cancel_stale=True,
        max_plan_age_seconds=60,
        now=now,
        allow_dual_side_position=True,
    )

    self.assertFalse(validation["ok"])
    self.assertIn("requires one-way position mode", " ".join(validation["errors"]))


def test_validate_plan_report_accepts_hedge_account_for_hedge_required_profile(self) -> None:
    now = datetime(2026, 5, 20, 10, 0, tzinfo=timezone.utc)
    report = {
        "symbol": "AIGENSYNUSDT",
        "generated_at": now.isoformat(),
        "dual_side_position": True,
        "required_position_mode": "hedge",
        "missing_orders": [{"side": "BUY", "position_side": "LONG", "price": 0.02, "qty": 100.0, "notional": 2.0}],
        "stale_orders": [],
    }

    validation = validate_plan_report(
        plan_report=report,
        allow_symbol="AIGENSYNUSDT",
        max_new_orders=8,
        max_total_notional=500.0,
        cancel_stale=True,
        max_plan_age_seconds=60,
        now=now,
        allow_dual_side_position=True,
    )

    self.assertTrue(validation["ok"])
```

- [x] **Step 2: Run submit tests and verify RED**

Run:

```bash
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 PYTHONPATH=src python -m pytest tests/test_submit_plan.py -q
```

Expected: fail because `required_position_mode` is not validated.

- [x] **Step 3: Implement submit validation**

In `validate_plan_report`, resolve:

```python
required_position_mode = str(plan_report.get("required_position_mode") or "one_way").strip().lower()
```

Reject unknown values. Reject `required_position_mode == "one_way"` with `dual_side_position=true`. Reject `required_position_mode == "hedge"` with `dual_side_position=false`. Keep the existing `allow_dual_side_position` guard as an additional safety net for callers that do not understand hedge plans.

- [x] **Step 4: Add runner report fields**

In `loop_runner.py`, ensure the plan report includes:

```python
"required_position_mode": strategy_profile_schema.get("required_position_mode", "one_way"),
"required_position_mode_defaulted": bool(strategy_profile_schema.get("required_position_mode_defaulted")),
"position_mode_compatible": required mode matches account `dualSidePosition`,
```

Use the same values when calling `validate_plan_report`.

- [x] **Step 5: Run runner/submit tests**

Run:

```bash
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 PYTHONPATH=src python -m pytest tests/test_submit_plan.py tests/test_loop_runner.py -q
```

Expected: pass.

### Task 3: Web Presets And Strategy Editor Compatibility

**Files:**
- Modify: `src/grid_optimizer/web.py`
- Test: `tests/test_web_security.py`

- [x] **Step 1: Write failing web tests**

Add tests proving:

```python
def test_aigensyn_one_way_preset_declares_default_position_mode(self) -> None:
    payload = _runner_preset_payload("aigensynusdt_best_quote_maker_volume_v1", {"symbol": "AIGENSYNUSDT"})
    self.assertEqual(payload["required_position_mode"], "one_way")


def test_aigensyn_hedge_bq_preset_is_explicit_opt_in(self) -> None:
    payload = _runner_preset_payload("aigensynusdt_hedge_bq_pingpong_sprint_v1", {"symbol": "AIGENSYNUSDT"})
    self.assertEqual(payload["required_position_mode"], "hedge")
    self.assertEqual(payload["strategy_mode"], "hedge_neutral")
    self.assertEqual(payload["max_short_position_notional"], 1500.0)


def test_strategy_editor_status_reports_profile_position_mode(self) -> None:
    payload = _build_strategy_editor_status("AIGENSYNUSDT")
    self.assertIn("position_mode", payload)
    self.assertIn("required_position_mode", payload["runner"]["config"])
```

- [x] **Step 2: Run web tests and verify RED**

Run:

```bash
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 PYTHONPATH=src python -m pytest tests/test_web_security.py -q
```

Expected: fail because the new preset/status metadata is missing.

- [x] **Step 3: Implement presets**

Add `required_position_mode="one_way"` to the existing AIGENSYN one-way preset.

Add a new preset:

```text
aigensynusdt_hedge_bq_pingpong_sprint_v1
```

with:

- `strategy_mode="hedge_neutral"`
- `required_position_mode="hedge"`
- `strict_strategy_profile_schema_enabled=true`
- `buy_levels=1`
- `sell_levels=1`
- `per_order_notional=750`
- `pause_buy_position_notional=900`
- `pause_short_position_notional=900`
- `max_position_notional=1500`
- `max_short_position_notional=1500`
- `max_total_notional=3000`

Add it to visibility allowlists and AIGENSYN summaries.

- [x] **Step 4: Implement editor metadata**

Add `required_position_mode` to runner config normalization and allowed save fields.

In `_build_strategy_editor_status`, include:

```python
"position_mode": {
    "current": "hedge" if account mode is known dual-side else "one_way",
    "dual_side_position": bool or None,
    "required": config required mode,
    "compatible": bool or None,
}
```

Do not make the endpoint heavy; if account mode is unavailable cheaply, return `current=null` and `compatible=null`.

In the embedded editor JS, show a status card for account/profile mode and label preset options with `模式不匹配` when metadata says incompatible.

- [x] **Step 5: Run web tests**

Run:

```bash
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 PYTHONPATH=src python -m pytest tests/test_web_security.py -q
```

Expected: pass.

### Task 4: End-To-End Verification

**Files:**
- No new files expected.

- [x] **Step 1: Run targeted suite**

Run:

```bash
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 PYTHONPATH=src python -m pytest tests/test_strategy_profile_schema.py tests/test_submit_plan.py tests/test_loop_runner.py tests/test_web_security.py -q
```

Expected: pass.

- [x] **Step 2: Run diff hygiene**

Run:

```bash
git diff --check
```

Expected: no output.

- [x] **Step 3: Review changed files**

Run:

```bash
git status --short
git diff --stat
```

Expected: only intended source, tests, docs plan, and optional local `.learnings/` untracked.

