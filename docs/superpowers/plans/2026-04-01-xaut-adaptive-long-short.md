# XAUT Adaptive Long/Short Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 为 `XAUTUSDT` 新增专用的 long/short 三态自适应微网格策略，在 `normal / defensive / reduce_only` 间自动切换，并在极端波动时立即撤掉同方向开仓单，只保留减仓单。

**Architecture:** 保持现有 one-way long/short planner 和提交链路不变，在 `loop_runner.py` 中新增一层 XAUT 专用状态机。web 只新增两个 preset，monitor/web 读取新的 summary 字段显示当前状态，测试先覆盖状态分类和 `reduce_only` 裁单，再补 preset/monitor 接线。

**Tech Stack:** Python 3, unittest, argparse, 现有 `grid_optimizer.loop_runner` / `grid_optimizer.web` / `grid_optimizer.monitor`

---

### Task 1: XAUT 状态机测试与核心逻辑

**Files:**
- Modify: `tests/test_loop_runner.py`
- Modify: `src/grid_optimizer/loop_runner.py`

- [x] **Step 1: 先写失败测试，覆盖 XAUT long/short 状态分类与切换**

```python
def test_assess_xaut_long_state_prefers_reduce_only_on_extreme_drop(self) -> None:
    report = assess_xaut_adaptive_regime(
        symbol="XAUTUSDT",
        strategy_mode="one_way_long",
        now=datetime(2026, 4, 1, 0, 0, tzinfo=timezone.utc),
    )
    self.assertEqual(report["candidate_state"], "reduce_only")

def test_assess_xaut_short_state_prefers_reduce_only_on_extreme_rally(self) -> None:
    report = assess_xaut_adaptive_regime(
        symbol="XAUTUSDT",
        strategy_mode="one_way_short",
        now=datetime(2026, 4, 1, 0, 0, tzinfo=timezone.utc),
    )
    self.assertEqual(report["candidate_state"], "reduce_only")

def test_resolve_xaut_state_requires_reduce_only_to_pass_through_defensive(self) -> None:
    state = {"xaut_adaptive_state": {"active_state": "reduce_only"}}
    resolved = resolve_xaut_adaptive_state(
        state=state,
        regime_report={"candidate_state": "normal", "reason": "stable"},
        strategy_mode="one_way_long",
        now=datetime(2026, 4, 1, 0, 5, tzinfo=timezone.utc),
    )
    self.assertEqual(resolved["active_state"], "reduce_only")
    self.assertEqual(resolved["candidate_state"], "defensive")
```

- [x] **Step 2: 运行单测，确认它先红**

Run: `pytest tests/test_loop_runner.py -k "xaut or reduce_only" -v`

Expected: FAIL，报 `assess_xaut_adaptive_regime` / `resolve_xaut_adaptive_state` 不存在或行为不匹配。

- [x] **Step 3: 在 `loop_runner.py` 中新增 XAUT 常量、状态判断与状态解析**

```python
XAUT_LONG_ADAPTIVE_PROFILE = "xaut_long_adaptive_v1"
XAUT_SHORT_ADAPTIVE_PROFILE = "xaut_short_adaptive_v1"
XAUT_ADAPTIVE_PROFILES = {
    XAUT_LONG_ADAPTIVE_PROFILE: "one_way_long",
    XAUT_SHORT_ADAPTIVE_PROFILE: "one_way_short",
}

def is_xaut_adaptive_profile(profile: str) -> bool:
    return str(profile).strip() in XAUT_ADAPTIVE_PROFILES
```

```python
def assess_xaut_adaptive_regime(*, symbol: str, strategy_mode: str, now: datetime | None = None) -> dict[str, Any]:
    ...
    return {
        "available": True,
        "candidate_state": candidate_state,
        "reason": reason,
        "metrics": {"window_15m": window_15m, "window_60m": window_60m},
    }
```

```python
def resolve_xaut_adaptive_state(*, state: dict[str, Any], regime_report: dict[str, Any], strategy_mode: str, now: datetime | None = None) -> dict[str, Any]:
    ...
    state["xaut_adaptive_state"] = adaptive_state
    return adaptive_state
```

- [x] **Step 4: 再跑同一组单测，确认转绿**

Run: `pytest tests/test_loop_runner.py -k "xaut or reduce_only" -v`

Expected: PASS

- [ ] **Step 5: 提交这一小步**

```bash
git add tests/test_loop_runner.py src/grid_optimizer/loop_runner.py
git commit -m "feat: add XAUT adaptive regime state machine"
```

### Task 2: `reduce_only` 裁单与 runner 接线

**Files:**
- Modify: `tests/test_loop_runner.py`
- Modify: `src/grid_optimizer/loop_runner.py`

- [x] **Step 1: 先写失败测试，覆盖 long/short 的 `reduce_only` 裁单与 plan report 接线**

```python
def test_apply_xaut_reduce_only_pruning_drops_long_opening_orders(self) -> None:
    plan = {"bootstrap_orders": [{"side": "BUY"}], "buy_orders": [{"side": "BUY"}], "sell_orders": [{"side": "SELL"}]}
    apply_xaut_reduce_only_pruning(plan=plan, strategy_mode="one_way_long")
    assert plan["bootstrap_orders"] == []
    assert plan["buy_orders"] == []
    assert len(plan["sell_orders"]) == 1
```

```python
def test_apply_xaut_reduce_only_pruning_drops_short_opening_orders(self) -> None:
    plan = {"bootstrap_orders": [{"side": "SELL"}], "buy_orders": [{"side": "BUY"}], "sell_orders": [{"side": "SELL"}]}
    apply_xaut_reduce_only_pruning(plan=plan, strategy_mode="one_way_short")
    assert plan["bootstrap_orders"] == []
    assert plan["sell_orders"] == []
    assert len(plan["buy_orders"]) == 1
```

- [x] **Step 2: 运行单测，确认先红**

Run: `pytest tests/test_loop_runner.py -k "xaut and pruning" -v`

Expected: FAIL

- [x] **Step 3: 把 XAUT 状态机接入 `generate_plan_report`**

```python
if is_xaut_adaptive_profile(requested_strategy_profile):
    if symbol != "XAUTUSDT":
        raise RuntimeError("XAUT adaptive profiles require symbol=XAUTUSDT")
    xaut_report = assess_xaut_adaptive_regime(...)
    xaut_state = resolve_xaut_adaptive_state(...)
    effective_args = build_xaut_adaptive_runner_args(args=args, active_state=xaut_state["active_state"])
```

```python
if xaut_state and xaut_state.get("active_state") == "reduce_only":
    apply_xaut_reduce_only_pruning(plan=plan, strategy_mode=strategy_mode)
```

- [x] **Step 4: 跑针对性测试，确认转绿**

Run: `pytest tests/test_loop_runner.py -k "xaut" -v`

Expected: PASS

- [ ] **Step 5: 提交这一小步**

```bash
git add tests/test_loop_runner.py src/grid_optimizer/loop_runner.py
git commit -m "feat: wire XAUT adaptive reduce-only flow"
```

### Task 3: web preset 与 monitor 展示

**Files:**
- Modify: `tests/test_web_security.py`
- Modify: `tests/test_monitor.py`
- Modify: `src/grid_optimizer/web.py`
- Modify: `src/grid_optimizer/monitor.py`

- [x] **Step 1: 先写失败测试，覆盖 preset payload 和 monitor risk 字段**

```python
def test_runner_preset_payload_applies_xaut_long_adaptive_profile(self) -> None:
    payload = _runner_preset_payload("xaut_long_adaptive_v1", {"symbol": "XAUTUSDT"})
    self.assertEqual(payload["strategy_profile"], "xaut_long_adaptive_v1")
    self.assertEqual(payload["symbol"], "XAUTUSDT")
    self.assertFalse(payload["autotune_symbol_enabled"])
```

```python
def test_monitor_summary_exposes_xaut_adaptive_state(self) -> None:
    ...
    self.assertEqual(risk["xaut_adaptive_state"], "reduce_only")
```

- [x] **Step 2: 运行单测，确认先红**

Run: `pytest tests/test_web_security.py -k "xaut_long_adaptive_v1" -v`

Run: `pytest tests/test_monitor.py -k "xaut_adaptive" -v`

Expected: FAIL

- [x] **Step 3: 新增 preset，并在 monitor 中回填 XAUT 状态字段**

```python
"xaut_long_adaptive_v1": {
    "label": "XAUT 自适应做多 v1",
    "description": "...",
    "startable": True,
    "kind": "one_way",
    "config": {"symbol": "XAUTUSDT", "strategy_mode": "one_way_long", ...},
},
```

```python
snapshot["risk_controls"].update(
    {
        "xaut_adaptive_enabled": xaut_adaptive_enabled,
        "xaut_adaptive_state": xaut_adaptive_state,
        "xaut_adaptive_reason": xaut_adaptive_reason,
    }
)
```

- [x] **Step 4: 跑接线测试，确认转绿**

Run: `pytest tests/test_web_security.py -k "xaut_long_adaptive_v1 or xaut_short_adaptive_v1" -v`

Run: `pytest tests/test_monitor.py -k "xaut_adaptive" -v`

Expected: PASS

- [ ] **Step 5: 提交这一小步**

```bash
git add tests/test_web_security.py tests/test_monitor.py src/grid_optimizer/web.py src/grid_optimizer/monitor.py
git commit -m "feat: add XAUT adaptive presets and monitoring fields"
```

### Task 4: 最终验证

**Files:**
- Modify: `docs/superpowers/plans/2026-04-01-xaut-adaptive-long-short.md`

- [x] **Step 1: 跑最终回归**

Run: `pytest tests/test_loop_runner.py tests/test_web_security.py tests/test_monitor.py -v`

Expected: PASS

- [x] **Step 2: 手工核对需求**

Checklist:

- [x] `xaut_long_adaptive_v1` 和 `xaut_short_adaptive_v1` 可出现在 preset 列表中
- [x] `XAUTUSDT` 之外启动时会报错
- [x] `normal / defensive / reduce_only` 会自动切换
- [x] `reduce_only` 会立即裁掉 long 的买单和 short 的卖单
- [x] monitor/web 能看到 XAUT 当前状态和原因

- [x] **Step 3: 标记计划完成**

把本文件中的已完成步骤勾掉，保留未完成项为空。
