# Custom Grid Roll Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 为 `custom_grid_enabled=true` 且 `custom_grid_direction=long` 的静态自定义网格增加按时间桶检查、按成交累计与距离上沿层数共同触发的条件下移能力，并把配置和监控信息接到 Web 端。

**Architecture:** 在 `loop_runner` 内新增一组纯函数处理自定义网格下移的时间桶、成交计数、梯子层数和区间平移，再把结果写回 runner state 与 cycle summary。`web.py` 负责把新配置写入 preset / runner command，`monitor.py` 从 plan report、latest loop 和 runner config 里拼出监控展示所需字段。

**Tech Stack:** Python 3.11, `unittest`, 现有 futures loop runner / web / monitor 模块

---

### Task 1: Runner Roll Primitives

**Files:**
- Modify: `src/grid_optimizer/loop_runner.py`
- Test: `tests/test_loop_runner.py`

- [ ] **Step 1: Write failing runner helper tests**

```python
    def test_custom_grid_roll_levels_above_current_counts_remaining_complete_ladders(self) -> None:
        self.assertEqual(_custom_grid_levels_above_current([10.0, 11.0, 12.0, 13.0, 14.0], 12.0), 2)
        self.assertEqual(_custom_grid_levels_above_current([10.0, 11.0, 12.0, 13.0, 14.0], 11.99), 2)
        self.assertEqual(_custom_grid_levels_above_current([10.0, 11.0, 12.0, 13.0, 14.0], 9.5), 4)

    def test_shift_custom_grid_bounds_supports_arithmetic_and_geometric(self) -> None:
        shifted = _shift_custom_grid_bounds(min_price=10.0, max_price=14.0, n=4, grid_level_mode="arithmetic", shift_levels=1)
        self.assertAlmostEqual(shifted["min_price"], 9.0)
        self.assertAlmostEqual(shifted["max_price"], 13.0)
```

- [ ] **Step 2: Run red tests**

Run: `PYTHONPATH=src python -m pytest tests/test_loop_runner.py -k custom_grid_roll -q`
Expected: fail with missing helper functions / assertions

- [ ] **Step 3: Implement minimal helper functions**

```python
def _custom_grid_levels_above_current(levels: list[float], current_price: float) -> int:
    ...

def _shift_custom_grid_bounds(*, min_price: float, max_price: float, n: int, grid_level_mode: str, shift_levels: int) -> dict[str, float]:
    ...

def _current_check_bucket(now: datetime, interval_minutes: int) -> str:
    ...
```

- [ ] **Step 4: Run green tests**

Run: `PYTHONPATH=src python -m pytest tests/test_loop_runner.py -k custom_grid_roll -q`
Expected: pass

### Task 2: Runner Roll Evaluation And State

**Files:**
- Modify: `src/grid_optimizer/loop_runner.py`
- Modify: `src/grid_optimizer/semi_auto_plan.py`
- Test: `tests/test_loop_runner.py`

- [ ] **Step 1: Write failing tests for roll evaluation**

```python
    def test_custom_grid_roll_check_triggers_and_resets_trade_baseline(self) -> None:
        ...

    def test_custom_grid_roll_check_skips_when_bucket_already_checked(self) -> None:
        ...
```

- [ ] **Step 2: Run red tests**

Run: `PYTHONPATH=src python -m pytest tests/test_loop_runner.py -k custom_grid_roll_check -q`
Expected: fail because evaluation function and state fields do not exist

- [ ] **Step 3: Implement roll state/config plumbing**

```python
optional_fields = (
    ...,
    "custom_grid_roll_enabled",
    "custom_grid_roll_interval_minutes",
    "custom_grid_roll_trade_threshold",
    "custom_grid_roll_upper_distance_ratio",
    "custom_grid_roll_shift_levels",
)
```

```python
roll_check = _evaluate_custom_grid_roll(...)
state["custom_grid_roll_last_check_bucket"] = roll_check["last_check_bucket"]
state["custom_grid_roll_trade_baseline"] = roll_check["trade_baseline"]
```

- [ ] **Step 4: Extend cycle summary / plan report**

```python
"custom_grid_roll": roll_check,
"custom_grid_roll_checked": bool(roll_check["checked"]),
"custom_grid_roll_triggered": bool(roll_check["triggered"]),
```

- [ ] **Step 5: Run focused tests**

Run: `PYTHONPATH=src python -m pytest tests/test_loop_runner.py -k custom_grid_roll -q`
Expected: pass

### Task 3: Web Config And Command Wiring

**Files:**
- Modify: `src/grid_optimizer/web.py`
- Test: `tests/test_web_security.py`

- [ ] **Step 1: Write failing web tests**

```python
    def test_build_runner_command_includes_custom_grid_roll_arguments(self) -> None:
        ...

    def test_runner_preset_payload_normalizes_custom_grid_roll_defaults(self) -> None:
        ...
```

- [ ] **Step 2: Run red tests**

Run: `PYTHONPATH=src python -m pytest tests/test_web_security.py -k custom_grid_roll -q`
Expected: fail because defaults / CLI args are missing

- [ ] **Step 3: Implement preset/config defaults and command args**

```python
config.update(
    {
        "custom_grid_roll_enabled": False,
        "custom_grid_roll_interval_minutes": 5,
        "custom_grid_roll_trade_threshold": 100,
        "custom_grid_roll_upper_distance_ratio": 0.30,
        "custom_grid_roll_shift_levels": 1,
    }
)
```

- [ ] **Step 4: Add custom grid form payload fields**

```javascript
custom_grid_roll_enabled: Boolean(customGridRollEnabledEl.checked),
custom_grid_roll_interval_minutes: Number(customGridRollIntervalEl.value || 5),
```

- [ ] **Step 5: Run focused tests**

Run: `PYTHONPATH=src python -m pytest tests/test_web_security.py -k custom_grid_roll -q`
Expected: pass

### Task 4: Monitor Snapshot And UI Copy

**Files:**
- Modify: `src/grid_optimizer/monitor.py`
- Modify: `src/grid_optimizer/web.py`
- Test: `tests/test_web_security.py`

- [ ] **Step 1: Write failing monitor/UI tests**

```python
    def test_monitor_page_contains_custom_grid_roll_controls(self) -> None:
        self.assertIn('id="custom_grid_roll_enabled"', MONITOR_PAGE)
```

- [ ] **Step 2: Run red tests**

Run: `PYTHONPATH=src python -m pytest tests/test_web_security.py -k monitor_page_contains_custom_grid_roll -q`
Expected: fail because controls / copy are absent

- [ ] **Step 3: Implement risk snapshot fields and UI text**

```python
snapshot["risk_controls"]["custom_grid_roll"] = ...
```

```javascript
parts.push(`条件下移：${risk.custom_grid_roll_enabled ? "开启" : "关闭"}`);
```

- [ ] **Step 4: Run focused tests**

Run: `PYTHONPATH=src python -m pytest tests/test_web_security.py -k custom_grid_roll -q`
Expected: pass

### Task 5: Full Verification

**Files:**
- Modify: `src/grid_optimizer/loop_runner.py`
- Modify: `src/grid_optimizer/semi_auto_plan.py`
- Modify: `src/grid_optimizer/monitor.py`
- Modify: `src/grid_optimizer/web.py`
- Modify: `tests/test_loop_runner.py`
- Modify: `tests/test_web_security.py`

- [ ] **Step 1: Run runner and web test files**

Run: `PYTHONPATH=src python -m pytest tests/test_loop_runner.py tests/test_web_security.py -q`
Expected: pass

- [ ] **Step 2: Run targeted repo regression if needed**

Run: `PYTHONPATH=src python -m pytest tests/test_audit.py -q`
Expected: pass

- [ ] **Step 3: Review spec coverage**

Checklist:
- runner config fields added
- state additions persisted
- trade count sourced from `trade_audit`
- time-bucket dedupe works
- arithmetic/geometric shift logic works
- summary / monitor fields visible

