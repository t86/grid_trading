# GRVT Profit Release Recovery Integration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 修复 GRVT 成本桶盈利退出的错桶、部分成交锁死和 POST 崩溃窗口，并把软阈值压力减仓纳入每 symbol 单动作恢复所有权。

**Architecture:** 新增纯领域模块 `ordinary_profit_release.py`，统一维护成本桶租约的预留、成交、终结与恢复；`loop_runner.py` 仅负责把交易所事件和提交结果转换成领域事件。压力减仓继续复用现有 recovery profile gate 和 symbol execution coordinator，不新增第二恢复所有者。

**Tech Stack:** Python 3.11、`unittest`/pytest、JSON runner state、Binance Futures LIMIT/GTX 提交路径。

---

### Task 1: 建立集中式成本桶租约状态模型

**Files:**
- Create: `src/grid_optimizer/ordinary_profit_release.py`
- Create: `tests/test_ordinary_profit_release.py`

- [ ] **Step 1: 写出错桶消费和 cutoff 隔离的失败测试**

```python
def test_fill_consumes_only_the_leased_bucket_before_cutoff() -> None:
    ledger = {
        "long_lots": [
            _lot(price=99.0, qty=1.0, opened_at="2026-08-18T00:00:00+00:00"),
            _lot(price=100.0, qty=2.0, opened_at="2026-08-18T00:01:00+00:00"),
            _lot(price=100.0, qty=3.0, opened_at="2026-08-18T00:03:00+00:00"),
        ]
    }
    lease = _lease(
        side="LONG",
        bucket_price=100.0,
        cutoff="2026-08-18T00:02:00+00:00",
        authorized_qty=2.0,
    )

    result = apply_profit_release_fill(ledger=ledger, lease=lease, fill_qty=0.5)

    assert result.applied_qty == 0.5
    assert _qty(ledger, price=99.0) == 1.0
    assert _qty(ledger, price=100.0, opened_at="2026-08-18T00:01:00+00:00") == 1.5
    assert _qty(ledger, price=100.0, opened_at="2026-08-18T00:03:00+00:00") == 3.0
```

- [ ] **Step 2: 运行测试并确认 RED**

Run:

```bash
PYTHONPATH=.:src PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 /tmp/wangge-pytest/bin/python -m pytest -q tests/test_ordinary_profit_release.py
```

Expected: FAIL，因为 `ordinary_profit_release` 模块尚不存在。

- [ ] **Step 3: 实现最小领域模型**

```python
class ProfitReleaseLeaseStatus(StrEnum):
    PENDING_SUBMIT = "pending_submit"
    OPEN = "open"
    UNKNOWN = "unknown"
    TERMINAL = "terminal"


@dataclass(frozen=True)
class ProfitReleaseLeaseKey:
    position_side: str
    bucket_price: float
    entry_cutoff_at: str


def eligible_lot_qty(*, ledger: Mapping[str, Any], key: ProfitReleaseLeaseKey) -> float:
    return sum(
        _lot_qty(lot)
        for lot in _matching_lots(ledger=ledger, key=key)
    )


def apply_profit_release_fill(
    *, ledger: dict[str, Any], lease: Mapping[str, Any], fill_qty: float
) -> ProfitReleaseFillResult:
    key = lease_key(lease)
    remaining = min(
        max(float(fill_qty), 0.0),
        max(float(lease["authorized_qty"]) - float(lease.get("filled_qty", 0.0)), 0.0),
    )
    # 只遍历 key 完全匹配的普通 entry lot，按 opened_at 稳定消费。
    applied = _consume_matching_lots(ledger=ledger, key=key, qty=remaining)
    return ProfitReleaseFillResult(applied_qty=applied)
```

实现必须拒绝 side、price、cutoff、数量缺失或非法的租约；价格归一化沿用交易所 tick/现有 decimal 字符串，不使用模糊跨桶匹配。

- [ ] **Step 4: 增加部分成交终结和零成交终结测试**

```python
def test_partial_fill_terminal_releases_unfilled_reservation() -> None:
    ledger = {"long_lots": [_lot(price=100.0, qty=2.0, opened_at="2026-08-18T00:00:00+00:00")]}
    lease = reserve_profit_release_lease(
        ledger=ledger,
        lease_id="lease-1",
        client_order_id="client-1",
        position_side="LONG",
        bucket_price=100.0,
        entry_cutoff_at="2026-08-18T00:01:00+00:00",
        authorized_qty=2.0,
    )
    apply_profit_release_fill(ledger=ledger, lease=lease, fill_qty=0.5)
    settle_profit_release_lease(
        ledger=ledger,
        lease_id=lease["lease_id"],
        terminal_reason="canceled",
    )
    assert settled_filled_qty(ledger=ledger, lease_id="lease-1") == 0.5
    assert available_qty(
        ledger=ledger,
        position_side="LONG",
        bucket_price=100.0,
        entry_cutoff_at="2026-08-18T00:01:00+00:00",
    ) == 1.5


def test_zero_fill_reject_releases_all_reservation() -> None:
    ledger = {"long_lots": [_lot(price=100.0, qty=2.0, opened_at="2026-08-18T00:00:00+00:00")]}
    lease = reserve_profit_release_lease(
        ledger=ledger,
        lease_id="lease-2",
        client_order_id="client-2",
        position_side="LONG",
        bucket_price=100.0,
        entry_cutoff_at="2026-08-18T00:01:00+00:00",
        authorized_qty=2.0,
    )
    settle_profit_release_lease(
        ledger=ledger,
        lease_id=lease["lease_id"],
        terminal_reason="rejected",
    )
    assert available_qty(
        ledger=ledger,
        position_side="LONG",
        bucket_price=100.0,
        entry_cutoff_at="2026-08-18T00:01:00+00:00",
    ) == 2.0
```

- [ ] **Step 5: 运行 Task 1 测试并提交**

Run:

```bash
PYTHONPATH=.:src PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 /tmp/wangge-pytest/bin/python -m pytest -q tests/test_ordinary_profit_release.py
git diff --check
```

Expected: PASS。

Commit:

```bash
git add src/grid_optimizer/ordinary_profit_release.py tests/test_ordinary_profit_release.py
git commit -m "fix: account profit release by exact entry bucket"
```

### Task 2: 将 runner 成交与终结事件迁移到集中状态模型

**Files:**
- Modify: `src/grid_optimizer/loop_runner.py:1934-2252`
- Modify: `src/grid_optimizer/loop_runner.py:3831-3878`
- Modify: `src/grid_optimizer/loop_runner.py:4568-4643`
- Test: `tests/test_loop_runner.py`

- [ ] **Step 1: 写出 runner 层跨桶和部分成交撤单失败测试**

```python
def test_profit_release_trade_consumes_exact_leased_bucket(self) -> None:
    state = _state_with_long_entry_lots([(99.0, 1.0), (100.0, 2.0)])
    _attach_profit_release_ref(
        state,
        order_id="7",
        side="LONG",
        bucket_price=100.0,
        cutoff="2026-08-18T00:02:00+00:00",
        authorized_qty=2.0,
    )
    _apply_best_quote_volume_trade_ledger(
        state=state,
        trades=[_reduce_long_trade(order_id=7, price=101.0, qty=0.5)],
    )
    assert _bucket_qty(state, 99.0) == 1.0
    assert _bucket_qty(state, 100.0) == 1.5


def test_profit_release_cancel_after_partial_fill_releases_remainder(self) -> None:
    state = _state_with_open_profit_lease(authorized_qty=2.0, filled_qty=0.5)
    report = _apply_ordinary_profit_release_execution_events(
        state=state,
        execution_events=[_canceled_event(order_id=7, cumulative_filled_qty=0.5)],
    )
    assert report["settled_filled_qty"] == 0.5
    assert report["released_reserved_qty"] == 1.5
```

- [ ] **Step 2: 运行两个测试并确认 RED**

Expected: 旧代码消费错误价格桶，并把整个 cutoff 桶标记 locked。

- [ ] **Step 3: 用领域模块替换旧 helper**

删除 `_lock_ordinary_profit_release_bucket` 的整桶锁定语义。`_apply_ordinary_profit_release_execution_events` 只负责：

```python
transition_profit_release_order_event(
    ledger=ledger,
    order_ref=ref,
    event_kind=kind,
    cumulative_filled_qty=event_filled_qty,
)
```

普通盈利 lease 成交不得再调用 `_best_quote_volume_consume_entry_lots_first`；非 lease 的历史普通减仓继续保留原 FIFO 行为，避免扩大范围。

- [ ] **Step 4: 验证 runner 状态重放幂等**

同一 trade key 和 terminal event 重放两次，`filled_qty`、lot qty、settled waterline 均只能变化一次。

- [ ] **Step 5: 运行相关测试并提交**

Run:

```bash
PYTHONPATH=.:src PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 /tmp/wangge-pytest/bin/python -m pytest -q tests/test_ordinary_profit_release.py tests/test_loop_runner.py -k 'profit_release or entry_lot'
```

Commit:

```bash
git add src/grid_optimizer/loop_runner.py tests/test_loop_runner.py
git commit -m "fix: settle profit release by actual fills"
```

### Task 3: 在 POST 前持久化租约 fence

**Files:**
- Modify: `src/grid_optimizer/ordinary_profit_release.py`
- Modify: `src/grid_optimizer/loop_runner.py:36720-37002`
- Test: `tests/test_loop_runner.py`

- [ ] **Step 1: 写 POST 崩溃窗口失败测试**

```python
def test_profit_release_lease_is_persisted_before_exchange_post(
    profit_release_execution_harness,
) -> None:
    observed = {}

    def crash_after_observing_state(**kwargs):
        observed.update(read_json(state_path))
        raise SystemExit("crash after POST boundary")

    with patch("grid_optimizer.loop_runner.post_futures_order", side_effect=crash_after_observing_state):
        with pytest.raises(SystemExit):
            profit_release_execution_harness.execute()

    lease = _only_profit_release_lease(observed)
    assert lease["status"] == "pending_submit"
    assert lease["client_order_id"]
```

再补 fresh runner 重启测试：相同桶存在 `PENDING_SUBMIT` 或 `UNKNOWN` 时，不得生成第二个 lease/clientOrderId。

- [ ] **Step 2: 运行测试并确认 RED**

Expected: POST 发生时 state 尚无租约。

- [ ] **Step 3: 实现 prepare/accept/reject/unknown 协议**

提交顺序固定为：

```python
reservation = reserve_pending_profit_release(
    state_path=state_path,
    order=prepared_order,
    client_order_id=client_order_id,
)
try:
    response = post_futures_order(
        symbol=symbol,
        side=side,
        quantity=prepared_order["qty"],
        price=prepared_order["submitted_price"],
        api_key=api_key,
        api_secret=api_secret,
        recv_window=args.recv_window,
        time_in_force="GTX",
        new_client_order_id=client_order_id,
        reduce_only=True,
        position_side=position_side,
    )
except DeterministicReject:
    mark_profit_release_terminal(
        state_path=state_path,
        lease_id=reservation.lease_id,
        release_unfilled=True,
    )
    raise
except Exception:
    mark_profit_release_unknown(
        state_path=state_path,
        lease_id=reservation.lease_id,
    )
    raise
else:
    mark_profit_release_open(
        state_path=state_path,
        lease_id=reservation.lease_id,
        order_id=response.get("orderId"),
    )
```

所有状态转移必须通过 state path 的现有原子写/锁边界；不能先读后裸写覆盖并发 ledger 更新。

- [ ] **Step 4: 增加 UNKNOWN 对账测试**

```python
def test_unknown_lease_reconciles_by_client_id_without_second_submit(
    profit_release_execution_harness,
) -> None:
    profit_release_execution_harness.seed_unknown_lease(client_order_id="client-unknown")
    profit_release_execution_harness.exchange_orders = [
        {"clientOrderId": "client-unknown", "orderId": 77, "status": "NEW"}
    ]
    report = profit_release_execution_harness.reconcile()
    assert report["opened_lease_ids"] == ["lease-unknown"]
    assert profit_release_execution_harness.post_count == 0


def test_unknown_lease_releases_when_exchange_proves_absent(
    profit_release_execution_harness,
) -> None:
    profit_release_execution_harness.seed_unknown_lease(client_order_id="client-absent")
    profit_release_execution_harness.exchange_orders = []
    profit_release_execution_harness.exchange_order_history_complete = True
    report = profit_release_execution_harness.reconcile()
    assert report["terminal_lease_ids"] == ["lease-unknown"]
    assert profit_release_execution_harness.available_qty == 2.0
```

只有交易所明确返回终态或经订单历史证明不存在时才释放 UNKNOWN；查询失败继续保持 fence。

- [ ] **Step 5: 运行提交/重启测试并提交**

Run:

```bash
PYTHONPATH=.:src PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 /tmp/wangge-pytest/bin/python -m pytest -q tests/test_loop_runner.py -k 'profit_release and (submit or restart or unknown or reconcile)'
```

Commit:

```bash
git add src/grid_optimizer/ordinary_profit_release.py src/grid_optimizer/loop_runner.py tests/test_loop_runner.py
git commit -m "fix: fence profit release before exchange submit"
```

### Task 4: 关闭软阈值等号死区并只选择压力恢复动作

**Files:**
- Modify: `src/grid_optimizer/loop_runner.py:24424-24431`
- Modify: `src/grid_optimizer/loop_runner.py:24623-25203`
- Modify: `src/grid_optimizer/loop_runner.py:34419-34488`
- Modify: `src/grid_optimizer/submit_plan.py:2031-2288`
- Test: `tests/test_loop_runner.py`
- Test: `tests/test_submit_plan.py`

- [ ] **Step 1: 写阈值等号与单侧恢复失败测试**

```python
def test_grvt_pressure_recovery_triggers_at_exact_soft_threshold() -> None:
    plan = {"buy_orders": [], "sell_orders": []}
    report = apply_grvt_ordinary_inventory_pressure_guard(
        plan=plan,
        current_long_notional=900.0,
        long_pressure_notional=900.0,
        current_short_notional=0.0,
        short_pressure_notional=900.0,
        max_reduce_notional=100.0,
        step_size=0.001,
    )
    assert report["active"]
    assert report["eligible_sides"] == ["long"]
```

同时锁定最新并发提交 `dbe6a15e` 的边界：普通 entry 不得为了制造恢复条件而故意从软阈值下方跨到阈值上方。

```python
def test_near_soft_lane_does_not_add_entry_that_crosses_soft_threshold() -> None:
    plan = {"buy_orders": [], "sell_orders": []}
    report = apply_grvt_near_soft_bridge_entry(
        plan=plan,
        enabled=True,
        current_long_notional=898.0,
        long_threshold_notional=900.0,
        max_long_notional=1000.0,
        bid_price=100.0,
        step_size=0.001,
        min_qty=0.001,
        min_notional=5.0,
        long_profit_reduce_reason="aggregate_no_loss_blocked",
        long_entry_reason="soft_headroom_below_min_notional",
        long_entry_blocked_by_anti_chase=False,
        volatility_entry_pause_active=False,
    )
    assert not report["added"]
    assert report["reason"] == "soft_headroom_below_min_notional"
    assert plan["buy_orders"] == []
```

- [ ] **Step 2: 写同周期单动作失败测试**

```python
def test_symbol_coordinator_defers_normal_brush_during_pressure_recovery() -> None:
    actions = {
        "place_orders": [light_side_entry, pressure_reduce],
        "cancel_orders": [ordinary_cancel],
        "recovery_profile_gate": _authorized_pressure_recovery_gate("LONG"),
    }
    result = coordinate_symbol_execution_action(
        actions=actions,
        current_actual_net_qty=0.0,
        valuation_price=100.0,
        current_open_orders=[],
        max_actual_net_notional=1000.0,
    )
    assert result["place_orders"] == [pressure_reduce]
    assert result["cancel_orders"] == []
    assert result["actual_net_exposure_decision"]["selected_lane"] == "ordinary_recovery"
    assert result["actual_net_exposure_decision"]["deferred_ordinary_place_count"] == 1
```

- [ ] **Step 3: 运行测试并确认 RED**

Expected: 等号不触发；轻侧 entry 与 pressure reduce 同周期保留。

- [ ] **Step 4: 最小实现阈值和 coordinator 路由**

将压力判定统一改为 `notional >= threshold - epsilon`。planner 在压力恢复激活时把 pressure reduce 标记为当前 recovery gate 唯一授权请求；coordinator 选择 `ordinary_recovery` lane 后只保留该请求组，普通 entry/profit place/cancel 全部 deferred。

`dbe6a15e` 的跨阈值 bridge 在等号死区关闭后不再成立：优先删除该 helper 和调用点；如果保留审计报告，则只能返回 `soft_headroom_below_min_notional`，不得创建跨越软阈值的 entry。

不得新增 symbol 名称硬编码所有权；必须复用 `recovery_profile_gate.managed/authorized/current_gate.active_action`。

- [ ] **Step 5: 增加恢复退出测试**

回到软阈值以下后，gate receipt 完成，临时 allow_loss 被回收；下一周期 normal 四腿重新出现。冻结候选仍按既定更高优先级/独立阈值处理。

- [ ] **Step 6: 运行相关测试并提交**

Run:

```bash
PYTHONPATH=.:src PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 /tmp/wangge-pytest/bin/python -m pytest -q tests/test_submit_plan.py -k 'symbol_coordinator or pressure_recovery'
PYTHONPATH=.:src PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 /tmp/wangge-pytest/bin/python -m pytest -q tests/test_loop_runner.py -k 'active_pair or pressure or recovery_profile'
```

Commit:

```bash
git add src/grid_optimizer/loop_runner.py src/grid_optimizer/submit_plan.py tests/test_loop_runner.py tests/test_submit_plan.py
git commit -m "fix: serialize GRVT pressure recovery actions"
```

### Task 5: 分支回归、合并与独立审查

**Files:**
- Modify only if tests reveal a regression directly caused by Tasks 1-4.

- [ ] **Step 1: 运行 GRVT 和恢复核心回归**

```bash
PYTHONPATH=.:src PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 /tmp/wangge-pytest/bin/python -m pytest -q \
  tests/test_ordinary_profit_release.py \
  tests/test_best_quote_maker_volume.py \
  tests/test_submit_plan.py \
  tests/test_loop_runner.py \
  tests/test_futures_recovery_coordinator.py \
  tests/test_futures_recovery_store.py \
  tests/test_runtime_guards.py
```

- [ ] **Step 2: 运行完整测试并记录既有基线**

```bash
PYTHONPATH=.:src PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 /tmp/wangge-pytest/bin/python -m pytest -q
```

任何新增失败必须修复；当前已知 18 个失败需逐个与合并前 HEAD 对照，GRVT 自主恢复相关失败不得在主力策略推广前继续忽略。

- [ ] **Step 3: 独立规格与质量审查**

审查必须验证：精确桶消费、部分成交剩余额度、POST 崩溃 fence、UNKNOWN 对账、阈值等号、单动作、GTX、冻结隔离和 allow_loss 回收。

- [ ] **Step 4: 合入 `codex/arx-single-recovery-owner`**

先确认两个 worktree 干净并获取最新远端引用，再在当前分支执行普通 merge；不得 squash 掉 GRVT 修复的独立回滚边界。

```bash
git merge --no-edit codex/grvt-entry-lot-profit-guard
```

- [ ] **Step 5: 在真实合并结果上重跑核心回归**

重复 Step 1，并运行 `git diff --check`、`py_compile`。完成后只提交/保留分支，不 push、不部署、不修改 111/114/150。

## Plan Self-Review

- 规格覆盖：12 项测试要求分别落在 Task 1-4；合并与灰度边界落在 Task 5。
- 占位符检查：无 TBD/TODO/“类似处理”。测试辅助函数名在各测试文件内实现，不形成生产 API。
- 类型一致性：领域模块统一使用 `lease_id/client_order_id/position_side/bucket_price/entry_cutoff_at/authorized_qty/filled_qty/status`；runner 不再维护另一套锁定字段。
- 范围：不改变目标值、仓位上限、冻结参数或部署配置；只修复已确认的四个阻塞。
