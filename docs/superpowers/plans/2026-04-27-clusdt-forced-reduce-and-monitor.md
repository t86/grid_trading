# CLUSDT Forced Reduce And Monitor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `hard_loss_forced_reduce_enabled` a real runner setting, expose the forced-reduce state in runner summaries, and highlight threshold/adverse/hard-loss reduce status in `/monitor`.

**Architecture:** Keep the existing `threshold_target_reduce` and `adverse_inventory_reduce` execution paths intact. Only add missing config plumbing for hard-loss forced reduce, add summary fields at the runner/event layer, and render a dedicated monitor block using the already-produced plan/summary data.

**Tech Stack:** Python, stdlib HTTP server UI, unittest/pytest, existing `grid_optimizer.web` + `grid_optimizer.loop_runner` plan/summary flow.

---

### Task 1: Wire `hard_loss_forced_reduce_enabled` through runner config and CLI

**Files:**
- Modify: `src/grid_optimizer/web.py`
- Modify: `src/grid_optimizer/loop_runner.py`
- Test: `tests/test_web_security.py`

- [ ] **Step 1: Write the failing config-plumbing tests**

```python
@patch("grid_optimizer.web.fetch_futures_book_tickers")
@patch("grid_optimizer.web.fetch_futures_symbol_config")
def test_resolve_runner_start_config_keeps_hard_loss_forced_reduce_flag(
    self, mock_symbol_config, mock_book_tickers
) -> None:
    mock_symbol_config.return_value = self._mock_symbol_config()
    mock_book_tickers.return_value = self._mock_book()

    config = _resolve_runner_start_config(
        {
            "symbol": "CLUSDT",
            "strategy_profile": "clusdt_competition_maker_neutral_conservative_v1",
            "hard_loss_forced_reduce_enabled": True,
        }
    )

    self.assertTrue(config["hard_loss_forced_reduce_enabled"])


def test_build_runner_command_includes_hard_loss_forced_reduce_argument(self) -> None:
    command = _build_runner_command(
        {
            "symbol": "CLUSDT",
            "strategy_profile": "clusdt_competition_maker_neutral_conservative_v1",
            "strategy_mode": "synthetic_neutral",
            "step_price": 0.06,
            "buy_levels": 8,
            "sell_levels": 8,
            "per_order_notional": 40.0,
            "base_position_notional": 0.0,
            "hard_loss_forced_reduce_enabled": True,
            "margin_type": "KEEP",
            "leverage": 5,
            "max_plan_age_seconds": 30,
            "max_mid_drift_steps": 4.0,
            "maker_retries": 2,
            "max_new_orders": 20,
            "max_total_notional": 650.0,
            "sleep_seconds": 3,
            "state_path": "output/clusdt_loop_state.json",
            "plan_json": "output/clusdt_loop_latest_plan.json",
            "submit_report_json": "output/clusdt_loop_latest_submit.json",
            "summary_jsonl": "output/clusdt_loop_events.jsonl",
        }
    )

    self.assertIn("--hard-loss-forced-reduce-enabled", command)
```

- [ ] **Step 2: Run the focused web tests and confirm RED**

Run:

```bash
PYTHONPATH=src PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 pytest tests/test_web_security.py -q
```

Expected:
- Failure because `hard_loss_forced_reduce_enabled` is ignored by `_resolve_runner_start_config()`
- Failure because `_build_runner_command()` does not emit `--hard-loss-forced-reduce-enabled`

- [ ] **Step 3: Implement the minimal config and CLI plumbing**

Add the flag to the runner config pipeline in `src/grid_optimizer/web.py`:

```python
RUNNER_DEFAULT_CONFIG: dict[str, Any] = {
    ...
    "hard_loss_forced_reduce_enabled": False,
    ...
}
```

```python
bool_fields = {
    ...
    "hard_loss_forced_reduce_enabled",
    ...
}
```

```python
command.append(
    "--hard-loss-forced-reduce-enabled"
    if config.get("hard_loss_forced_reduce_enabled", False)
    else "--no-hard-loss-forced-reduce-enabled"
)
```

Add the parser argument in `src/grid_optimizer/loop_runner.py` using the established style:

```python
parser.add_argument(
    "--hard-loss-forced-reduce-enabled",
    action=argparse.BooleanOptionalAction,
    default=False,
)
```

- [ ] **Step 4: Re-run the focused web tests and confirm GREEN**

Run:

```bash
PYTHONPATH=src PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 pytest tests/test_web_security.py -q
```

Expected:
- New tests pass
- Existing runner-command tests stay green

- [ ] **Step 5: Commit Task 1**

```bash
git add src/grid_optimizer/web.py src/grid_optimizer/loop_runner.py tests/test_web_security.py
git commit -m "Wire hard loss forced reduce runner flag"
```

### Task 2: Expose hard-loss and threshold/adverse reduce state in runner plan and summary

**Files:**
- Modify: `src/grid_optimizer/loop_runner.py`
- Test: `tests/test_loop_runner.py`

- [ ] **Step 1: Write the failing runner-state tests**

Add a focused parser test and a summary-field test in `tests/test_loop_runner.py`:

```python
def test_build_parser_accepts_hard_loss_forced_reduce_enabled_flag(self) -> None:
    parser = _build_parser()
    args = parser.parse_args(["--hard-loss-forced-reduce-enabled"])
    self.assertTrue(args.hard_loss_forced_reduce_enabled)
```

```python
def test_loop_event_summary_includes_threshold_and_hard_loss_reduce_fields(self) -> None:
    plan_report = {
        "symbol": "CLUSDT",
        "strategy_mode": "synthetic_neutral",
        "threshold_target_reduce": {
            "enabled": True,
            "active": True,
            "short_active": True,
            "short_elapsed_seconds": 18.0,
            "short_taker_timeout_active": True,
            "placed_reduce_orders": 1,
        },
        "adverse_inventory_reduce": {
            "enabled": True,
            "active": True,
            "direction": "short",
            "short_aggressive": True,
            "forced_reduce_orders": [{"side": "BUY", "role": "forced_reduce"}],
        },
        "hard_loss_forced_reduce": {
            "enabled": True,
            "active": True,
            "reason": "hard_loss_steps_exceeded",
            "forced_reduce_orders": [{"side": "SELL", "role": "forced_reduce"}],
        },
    }
    submit_report = {"placed_orders": [], "canceled_orders": []}

    summary = build_loop_event_summary(plan_report=plan_report, submit_report=submit_report)

    self.assertTrue(summary["threshold_reduce_enabled"])
    self.assertTrue(summary["threshold_reduce_active"])
    self.assertTrue(summary["threshold_reduce_short_taker_timeout_active"])
    self.assertTrue(summary["adverse_reduce_enabled"])
    self.assertTrue(summary["adverse_reduce_active"])
    self.assertTrue(summary["hard_loss_forced_reduce_enabled"])
    self.assertTrue(summary["hard_loss_forced_reduce_active"])
```

- [ ] **Step 2: Run the focused loop-runner tests and confirm RED**

Run:

```bash
PYTHONPATH=src PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 pytest tests/test_loop_runner.py -q
```

Expected:
- Failure because parser does not yet expose/round-trip the new flag everywhere needed
- Failure because summary fields for threshold/hard-loss reduce are missing

- [ ] **Step 3: Implement the minimal runner-report fields**

In `src/grid_optimizer/loop_runner.py`, add explicit plan/summary fields instead of making monitor parse deep nested objects ad hoc:

```python
hard_loss_forced_reduce = {
    "enabled": bool(getattr(args, "hard_loss_forced_reduce_enabled", False)),
    "active": False,
    "reason": None,
    "blocked_reason": "disabled" if not getattr(args, "hard_loss_forced_reduce_enabled", False) else None,
    "forced_reduce_orders": [],
}
```

Return the object from `generate_plan_report()` alongside:

```python
"threshold_target_reduce": threshold_target_reduce,
"adverse_inventory_reduce": adverse_inventory_reduce,
"hard_loss_forced_reduce": hard_loss_forced_reduce,
"forced_reduce_orders": plan.get("forced_reduce_orders", []),
```

And extend loop-event summary generation with flattened fields:

```python
"threshold_reduce_enabled": bool((plan_report.get("threshold_target_reduce") or {}).get("enabled")),
"threshold_reduce_active": bool((plan_report.get("threshold_target_reduce") or {}).get("active")),
"threshold_reduce_short_taker_timeout_active": bool(
    (plan_report.get("threshold_target_reduce") or {}).get("short_taker_timeout_active")
),
"hard_loss_forced_reduce_enabled": bool(
    (plan_report.get("hard_loss_forced_reduce") or {}).get("enabled")
),
"hard_loss_forced_reduce_active": bool(
    (plan_report.get("hard_loss_forced_reduce") or {}).get("active")
),
"hard_loss_forced_reduce_reason": (
    (plan_report.get("hard_loss_forced_reduce") or {}).get("reason")
),
```

For this task, do not redesign reduce logic. Only make the state visible and consistent.

- [ ] **Step 4: Re-run the focused loop-runner tests and confirm GREEN**

Run:

```bash
PYTHONPATH=src PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 pytest tests/test_loop_runner.py -q
```

Expected:
- New tests pass
- Existing threshold/adverse reduce tests stay green

- [ ] **Step 5: Commit Task 2**

```bash
git add src/grid_optimizer/loop_runner.py tests/test_loop_runner.py
git commit -m "Expose forced reduce runner state in summaries"
```

### Task 3: Highlight threshold/adverse/hard-loss reduce status in `/monitor`

**Files:**
- Modify: `src/grid_optimizer/web.py`
- Test: `tests/test_web_security.py`

- [ ] **Step 1: Write the failing monitor API/UI-oriented tests**

Add a monitor-query test that proves the API result preserves the new summary state:

```python
@patch("grid_optimizer.web.read_symbol_runner_process")
@patch("grid_optimizer.web.build_monitor_snapshot")
def test_run_loop_monitor_query_preserves_forced_reduce_summary_fields(
    self, mock_snapshot, mock_read_runner
) -> None:
    mock_read_runner.return_value = {
        "configured": True,
        "config": {"symbol": "CLUSDT"},
    }
    mock_snapshot.return_value = {
        "ok": True,
        "local": {
            "loop_summary": {
                "latest": {
                    "threshold_reduce_enabled": True,
                    "threshold_reduce_active": True,
                    "adverse_reduce_enabled": True,
                    "hard_loss_forced_reduce_enabled": True,
                }
            }
        },
    }

    result = _run_loop_monitor_query({"symbol": ["CLUSDT"]})

    latest = (((result.get("local") or {}).get("loop_summary") or {}).get("latest") or {})
    self.assertTrue(latest["threshold_reduce_enabled"])
    self.assertTrue(latest["adverse_reduce_enabled"])
    self.assertTrue(latest["hard_loss_forced_reduce_enabled"])
```

- [ ] **Step 2: Run the focused monitor tests and confirm RED**

Run:

```bash
PYTHONPATH=src PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 pytest tests/test_web_security.py -q
```

Expected:
- Failure if the monitor query or page rendering path drops or mishandles the new fields

- [ ] **Step 3: Implement the monitor highlight block**

In `src/grid_optimizer/web.py`, add a dedicated risk/status section in the monitor rendering path using existing `risk` / `latest event` data:

```javascript
const forcedReduceItems = [
  ["阈值减仓", risk.threshold_reduce_enabled ? (risk.threshold_reduce_active ? "激活" : "待命") : "关闭"],
  ["阈值减仓超时追价", risk.threshold_reduce_short_taker_timeout_active || risk.threshold_reduce_long_taker_timeout_active ? "是" : "否"],
  ["逆向减仓", risk.adverse_reduce_enabled ? (risk.adverse_reduce_active ? "激活" : "待命") : "关闭"],
  ["逆向减仓追价", risk.adverse_reduce_long_aggressive || risk.adverse_reduce_short_aggressive ? "是" : "否"],
  ["硬损强减", risk.hard_loss_forced_reduce_enabled ? (risk.hard_loss_forced_reduce_active ? "激活" : "待命") : "关闭"],
  ["硬损原因", risk.hard_loss_forced_reduce_reason || "--"],
];
```

Render it as a separate table/card adjacent to the existing position summary, not buried inside the raw event list.

- [ ] **Step 4: Re-run the targeted web tests and confirm GREEN**

Run:

```bash
PYTHONPATH=src PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 pytest tests/test_web_security.py -q
PYTHONPATH=src python -m py_compile src/grid_optimizer/web.py src/grid_optimizer/loop_runner.py
```

Expected:
- Tests pass
- Python compile succeeds

- [ ] **Step 5: Commit Task 3**

```bash
git add src/grid_optimizer/web.py tests/test_web_security.py
git commit -m "Show forced reduce state in monitor"
```

### Task 4: Full verification and deploy to `114`

**Files:**
- Verify only: `src/grid_optimizer/web.py`
- Verify only: `src/grid_optimizer/loop_runner.py`
- Verify only: `tests/test_web_security.py`
- Verify only: `tests/test_loop_runner.py`

- [ ] **Step 1: Run the combined local verification**

Run:

```bash
PYTHONPATH=src PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 pytest tests/test_loop_runner.py tests/test_web_security.py -q
PYTHONPATH=src python -m py_compile src/grid_optimizer/web.py src/grid_optimizer/loop_runner.py
```

Expected:
- All targeted tests pass
- No syntax errors

- [ ] **Step 2: Merge onto `main` worktree and push**

Run:

```bash
git log --oneline --decorate -5
git status --short
```

Then cherry-pick or merge the task commits into the `main` worktree and push `origin/main`.

- [ ] **Step 3: Deploy to `114` using the standard wrapper**

Run:

```bash
ssh srv-43-155-163-114 '/usr/local/bin/grid-web-update'
ssh srv-43-155-163-114 '/usr/local/bin/grid-saved-runner restart CLUSDT'
ssh srv-43-155-163-114 '/usr/local/bin/grid-saved-runner status CLUSDT'
```

Expected:
- `grid-web.service` active
- `CLUSDT` runner restarts successfully

- [ ] **Step 4: Verify live `CLUSDT` behavior and monitor output**

Run:

```bash
ssh srv-43-155-163-114 "ps -eo pid,args | grep -E '[g]rid_optimizer\\.loop_runner --symbol CLUSDT'"
ssh srv-43-155-163-114 "python3 - <<'PY'
import json
from pathlib import Path
plan = json.loads(Path('/home/ubuntu/wangge/output/clusdt_loop_latest_plan.json').read_text())
print(plan.get('hard_loss_forced_reduce'))
print(plan.get('threshold_target_reduce'))
print(plan.get('adverse_inventory_reduce'))
PY"
curl -s 'http://43.155.163.114:8788/api/loop_monitor?symbol=CLUSDT'
```

Expected:
- CLI contains `--hard-loss-forced-reduce-enabled` when configured on
- Plan JSON includes the three reduce-state objects
- Monitor API exposes the new summary fields without breaking the page

- [ ] **Step 5: Commit or document any required runtime config adjustment**

If the live runner needs the control file updated:

```bash
ssh srv-43-155-163-114 "python3 - <<'PY'
import json
from pathlib import Path
p = Path('/home/ubuntu/wangge/output/clusdt_loop_runner_control.json')
d = json.loads(p.read_text())
d['hard_loss_forced_reduce_enabled'] = True
p.write_text(json.dumps(d, ensure_ascii=False, indent=2), encoding='utf-8')
print('updated')
PY"
ssh srv-43-155-163-114 '/usr/local/bin/grid-saved-runner restart CLUSDT'
```

Expected:
- `CLUSDT` picks up the newly wired hard-loss flag
