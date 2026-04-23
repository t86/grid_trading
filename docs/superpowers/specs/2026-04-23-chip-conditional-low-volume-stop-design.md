# CHIP Conditional Low-Volume Stop Design

日期：2026-04-23

## 背景

`CHIPUSDT` 当前的 guarded v2 在真实运行里暴露出两个问题：

- `self_volume_trigger` 只看“最近窗口的策略自身成交额是否低于阈值”，会把“市场没波动、所以自然打不出量”和“策略已经失效、库存风险在恶化”混为一谈。
- `self_volume_trigger` 的自动恢复依赖“策略自己的成交额重新达到 start_threshold”，但 runner 停机后不会继续产生新的自成交，因此这个恢复语义在停机后基本无法成立。
- 停机后的清仓当前走 `maker_flatten`，默认 `allow_loss=False`。当库存已经浮亏时，会出现“策略停了，但残仓没清”的情况。

本设计将 CHIP 的低量保护改成“低量只是背景信号，只有和库存/浮亏风险同时成立才停机”，并把恢复条件改成“市场重新回到可刷区间后自动恢复”。

## 目标

- 低量本身不再直接触发 CHIP 停机。
- 只有“低量 + 风险”同时成立时，才执行停机、撤单、清仓。
- 停机后的清仓必须允许亏损退出，不能再留下残仓。
- 自动恢复不再看策略自成交，而是看市场 `15m` 波动是否回到可运行区间。
- 在放大到 `per_order_notional = 40` 的前提下，仍保持“控回撤优先”。

## 非目标

- 不重写 `synthetic_neutral` 计划生成逻辑。
- 不取消已有 `1m volatility_trigger` 快速风控。
- 不为所有币种同步切换到这套规则；首版只要求 CHIP 可用。
- 不在本设计里引入趋势预测、盘口建模或新的策略模式。

## 方案选择

### 方案 A：仅降低 `self_volume_trigger` 阈值

把 `15m stop/start` 从 `1000/1800` 下调到更低。

问题：

- 仍然无法区分“没波动所以没量”和“库存已经失控所以没量”。
- 自动恢复依赖自成交，停机后依旧不成立。

### 方案 B：低量只做告警，不参与停机

完全取消低量自动停机，只保留现有库存和波动风控。

问题：

- 无法表达“库存已经偏离，且策略已经明显打不出量”的组合失效场景。
- 会继续把库存暴露在失效窗口里等待回归。

### 方案 C：低量 + 风险 组合停机，波动冷却后自动恢复

低量只作为前置条件；当且仅当低量和风险任一红线同时成立时停机。恢复改看 `15m` 市场波动。

优点：

- 能区分“正常冷清”和“失效冷清”。
- 自动恢复语义成立，不再依赖停机后的自成交。
- 符合 CHIP 高波动、但又需要允许策略自行恢复的产品目标。

本设计采用方案 C。

## 已确认产品语义

### 1. 低量条件

- 观察窗口：`30m`
- 指标来源：策略自身成交审计里的 `gross_notional`
- 低量阈值：最近 `30m gross_notional < 300`

说明：

- 低量本身不触发停机。
- 它只是说明“最近 30 分钟这套策略基本没有有效刷出量”。

### 2. 风险条件

低量成立后，再判断以下风险条件，满足任一项即判定为“低量 + 风险”：

- `actual_net_notional >= 180`
- `short adverse ratio >= 4.0%`

说明：

- `actual_net_notional` 用于兜住库存已经明显堆大的情况。
- `short adverse ratio` 用于兜住库存尚未特别大，但挂单和现价已经严重偏离、空仓浮亏加深的情况。

### 3. 停机动作

当“低量 + 风险”成立时，执行：

1. 停 runner
2. 撤策略单
3. 允许亏损的硬清仓
4. 清仓完成前，不允许自动恢复

### 4. 自动恢复

恢复不再看策略自己的成交额，而改为看市场是否重新进入可刷区间。

恢复条件：

- 最近 `15m amplitude <= 5.0%`
- 最近 `15m abs return <= 3.0%`
- 清仓进程已结束
- 当前无残余仓位
- 当前无残余挂单
- 上次停机原因是本设计里的“低量 + 风险”

满足以上全部条件时，允许自动恢复 runner。

## CHIP 参数包

本次同时将 CHIP 的单笔名义上调，但不做等比例放大库存上限。

### 核心成交参数

- `per_order_notional = 40`
- `self_volume_trigger_window = 30m`
- `self_volume_trigger_stop_threshold = 300`

说明：

- `self_volume_trigger_start_threshold` 不再作为 CHIP 的恢复条件使用。
- 首版可以保留字段兼容，但 CHIP preset 不再依赖该字段。

### 库存护栏

- `pause_buy_position_notional = 120`
- `max_position_notional = 160`
- `pause_short_position_notional = 220`
- `max_short_position_notional = 300`
- `max_total_notional = 400`
- `max_actual_net_notional = 180`

说明：

- `max_actual_net_notional = 180` 仍然保留为独立的最终硬保险丝。
- 本设计里的“低量 + 风险”也会读取 `actual_net_notional >= 180`，但只有在低量已经成立时才走“可自动恢复”的组合停机语义。
- 若没有低量背景、只是单纯净敞口直接撞到 `max_actual_net_notional`，则仍按现有 runtime guard 的硬停语义处理，不纳入自动恢复。

### 主动减仓

- `adverse_reduce_enabled = true`
- `adverse_reduce_short_trigger_ratio = 4.0%` 之外仍保留现有较早减仓阈值，不与停机条件混用
- `adverse_reduce_max_order_notional = 40`

说明：

- `adverse_reduce_short_trigger_ratio` 负责“先减仓”，仍沿用现有更早触发值。
- 本设计新增的 `short adverse ratio >= 4.0%` 是“低量背景下的停机条件”，两者不是同一阈值。

## 实现设计

### 1. `self_volume_trigger` 从双阈值状态机改成“条件停机 + 波动恢复”

当前逻辑：

- runner 运行中，`gross < stop_threshold` 就停
- runner 停止且 `paused_by_trigger=True` 时，`gross >= start_threshold` 才恢复

改为：

- runner 运行中：
  - 计算最近 `30m self gross_notional`
  - 若 `gross >= 300`，动作 = `None`
  - 若 `gross < 300`，再判断风险条件：
    - `actual_net_notional >= 180`
    - `short adverse ratio >= 4.0%`
  - 只要任一风险条件成立，动作 = `stop`
- runner 停止且 `paused_by_trigger=True`：
  - 忽略 `start_threshold`
  - 检查 `15m` 市场波动恢复条件
  - 只有“波动恢复 + flatten 完成 + 无仓位 + 无挂单”全部成立，动作 = `start`

### 2. 为 `self_volume_trigger` 增加恢复波动配置

新增字段：

- `self_volume_trigger_resume_window`
- `self_volume_trigger_resume_amplitude_ratio`
- `self_volume_trigger_resume_abs_return_ratio`

CHIP 首版默认：

- `self_volume_trigger_resume_window = 15m`
- `self_volume_trigger_resume_amplitude_ratio = 0.05`
- `self_volume_trigger_resume_abs_return_ratio = 0.03`

### 3. 为 `self_volume_trigger` 增加风险门控配置

新增字段：

- `self_volume_trigger_risk_actual_net_notional`
- `self_volume_trigger_risk_short_adverse_ratio`

CHIP 首版默认：

- `self_volume_trigger_risk_actual_net_notional = 180`
- `self_volume_trigger_risk_short_adverse_ratio = 0.04`

说明：

- 这两个字段只在“低量已经成立”时参与判断。
- 若两者都未配置，则视为该 trigger 只做观测、不停机。
- `actual_net_notional` 的组合停机判断，不会替代现有 runtime guard 的独立硬保险丝。

### 4. 停机后的“硬清仓闭环”

当前实现的问题是停机后只启动默认 `maker_flatten`，而该逻辑默认禁止亏损退出。

改为以下顺序：

1. 停 runner
2. 撤策略单
3. 读取当前真实仓位
4. 先尝试允许亏损的即时减仓单清掉当前净仓
   - 允许使用 IOC / taker 语义
   - 目标是先把主库存快速打净
5. 若仍有残余仓位，再启动 `maker_flatten`
   - 但需要 `allow_loss=True`
   - 只负责扫尾，而不是承担主退出职责

这样可以避免再出现“停机成功，但残仓留在场上”的状态。

### 5. 触发状态文件与监控页展示

`self_volume_trigger_status` 需要新增可观测字段：

- `low_volume_condition_met`
- `risk_actual_net_notional_met`
- `risk_short_adverse_ratio_met`
- `resume_window`
- `resume_amplitude_ratio`
- `resume_abs_return_ratio`
- `resume_market_amplitude`
- `resume_market_abs_return`
- `resume_ready`

监控页展示语义调整为：

- 当前 `30m self gross`
- 当前低量条件是否成立
- 当前风险条件是否命中
- 当前恢复波动条件是否满足
- 当前是否因该 trigger 停机等待恢复

## 失败与边界场景

### 1. 市场没波动，但库存也很轻

结果：

- 不停机
- 继续挂着等机会

原因：

- 这只是“冷清”，不是“失效”。

### 2. 市场没波动，但库存已经堆大

结果：

- 触发“低量 + 风险”停机

原因：

- 已经不是简单冷清，而是策略在低效状态下持续占用风险预算。

### 3. 市场剧烈波动，策略被停机后短时仍残留少量仓位

结果：

- 不允许自动恢复
- 必须等清仓进程结束且残余仓位归零

### 4. 市场重新平静，但仍有旧挂单残留

结果：

- 不自动恢复

原因：

- 旧挂单会污染恢复后的新一轮 runner 状态。

## 测试计划

### 单元测试

- `gross >= 300` 时，`self_volume_trigger` 不停机
- `gross < 300` 但风险条件都不满足时，不停机
- `gross < 300` 且 `actual_net_notional >= 180` 时，停机
- `gross < 300` 且 `short adverse ratio >= 4.0%` 时，停机
- 被该 trigger 停机后，不再要求 `start_threshold`
- 被该 trigger 停机后，只有 `15m amplitude <= 5.0%` 且 `15m abs return <= 3.0%` 时才允许恢复
- 恢复时若 flatten 仍在跑、有残仓或有挂单，则不恢复

### 集成测试

- 触发“低量 + 风险”后，`/api/loop_monitor` 正确显示停机原因
- 停机后先撤策略单，再执行硬清仓
- 主库存能在亏损状态下被真正打平
- 若存在残余，再由 `maker_flatten(allow_loss=True)` 扫尾
- 恢复后 runner 使用最新 CHIP preset 正常重启

## 上线策略

首版只更新 CHIP：

- 新增 `chip_short_bias_ping_pong_guarded_v3`
- 保留 `chip_short_bias_ping_pong_guarded_v2` 作为回滚项

部署顺序：

1. 本地补状态机与清仓闭环
2. 本地测试
3. 部署 114
4. 先用 `v3` 启动 CHIP
5. 观察一次完整的“低量 + 风险停机 -> 清仓 -> 波动恢复 -> 自动恢复”闭环

## 风险

- `actual_net_notional >= 180` 已明显高于 v2 的实际净敞口硬线，因此首版更依赖主动减仓先起作用。
- `15m amplitude <= 5% / abs return <= 3%` 对 CHIP 来说已经明显放宽，恢复会比原设想更积极，需重点观察是否出现“刚恢复又再次停机”的抖动。
- 若 taker/IOC 清仓失败，必须确保 fallback flatten 真的能继续接管，否则仍会留残仓。

## 结论

CHIP 不适合“只要低量就停机”的规则。首版应改成：

- 低量只作为背景条件
- 低量和风险共振时才停机
- 停机必须允许亏损退出并真正清仓
- 恢复看 `15m` 市场波动，而不是停机后不可能再增长的策略自成交

这套语义更符合 CHIP 的高波动现实，也更贴近“控回撤优先，但允许自动恢复继续刷量”的目标。
