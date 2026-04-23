# Self-Volume Trigger + Guarded Ping-Pong Design

日期：2026-04-23

## 背景

当前 `SOONUSDT` 与 `CHIPUSDT` 的 `synthetic_neutral` 刷量方案都存在同一类风险：

- 单边行情里，库存会先超过软阈值，再因为“等保本回补”而滞留在亏损区。
- 当策略自己已经连续一段时间打不出量时，系统仍可能继续保留库存和挂单，等下一段流动性回归。
- 现有 `volume_trigger` 观察的是市场总成交额，不等于策略自己是否还在有效刷量。

本设计解决两件事：

1. 新增“策略自身成交额过低”的 `self_volume_trigger`
2. 将 `CHIP` / `SOON` 的单边库存风险改成“先主动减仓，后硬停清仓”的双层保护

## 目标

- 当策略自己最近 `15m gross_notional` 过低时，自动停机、撤单、清仓。
- 当流动性恢复且策略自身重新打出足够成交额时，允许自动恢复。
- 当单边库存逆势扩大时，先主动减仓，而不是直接继续扛单等待自然回补。
- 当风险继续恶化到红线外时，直接停机并清仓，不自动恢复。
- `CHIP` 与 `SOON` 都使用同一套风控骨架，但参数分别独立。

## 非目标

- 不新增新的 `strategy_mode`
- 不重写 `synthetic_neutral` 核心计划生成逻辑
- 不改交易所持仓模式、杠杆模型或保证金模型
- 不在本设计里引入新的趋势预测模型

## 已确认的产品语义

### 1. 低成交触发器

- 指标来源：策略自身成交额，而不是市场总成交额
- 主指标：`gross_notional`
- 默认窗口：`15m`
- 停机动作：停 runner、撤策略单、清仓
- 恢复动作：清仓完成后，若自身 `15m gross_notional` 回到启动阈值以上，允许自动恢复

### 2. 单边风险处理

- “单边亏损/库存失控”与“低成交”分开处理
- 单边风险：
  - 先主动减仓
  - 仍失控时硬停并清仓
  - 硬停后不自动恢复
- 低成交：
  - 直接停机并清仓
  - 后续允许自动恢复

## 现有能力梳理

当前代码库里已经有以下可复用能力：

- `threshold_reduce_target_ratio`
  - 仓位超过 `pause_*_position_notional` 后，将库存压回到 `pause * target_ratio`
  - 支持 maker 超时后转更激进价格
- `adverse_inventory_reduce`
  - 根据持仓成本价与当前价格的逆向偏离比例触发主动减仓
  - 支持 maker 超时后转 aggressive
- `runtime_guard`
  - 支持 `rolling_hourly_loss_limit`
  - 支持 `max_cumulative_notional`
  - 支持 `max_actual_net_notional`
  - 支持 `max_synthetic_drift_notional`
- `volume_trigger / volatility_trigger`
  - 已有后台巡检、停机、清仓、自动恢复框架

关键约束：

- 当前实现里，`threshold_reduce_target_ratio > 0` 时，会抑制 `adverse_inventory_reduce`
- 因此首版必须明确选择一条主动减仓主路径，不能两套同时开

## 方案比较

### 方案 A：只调参数，不新增 trigger

仅使用现有 `adverse_reduce + runtime_guard + volume_trigger`

优点：

- 改动最小
- 上线最快

缺点：

- 无法表达“策略自己 15 分钟几乎没打出量”的业务语义
- 容易把市场活跃但本策略失效的场景漏掉

### 方案 B：新增最小化 `self_volume_trigger`

复用现有 trigger 框架，只把观察指标换成策略自身 `15m gross_notional`

优点：

- 和目标完全一致
- 代码改动集中在 `web.py` 后台巡检和配置面
- 不需要改审计模型，直接复用 `_summarize_symbol_trade_window(...)`

缺点：

- 需要新增一组配置字段
- 需要补自动恢复状态语义

### 方案 C：把低成交也做成 runtime guard

把“15m 自身成交额过低”做成硬风控红线

优点：

- 风控语义简单

缺点：

- 和“低成交后允许自动恢复”的需求冲突
- 更容易在短暂失活后永久挂死

推荐采用方案 B。

## 总体设计

### 1. 主动减仓主路径

首版统一选择：

- `adverse_reduce_enabled = true`
- `threshold_reduce_target_ratio = 0`

原因：

- `adverse_inventory_reduce` 更符合“单边亏损先减仓”的产品语义
- 它明确依赖持仓成本与现价的逆向偏离，而不只是库存超过 pause
- 既能在 `CHIP` 上限制被持续拉升的空仓，也能在 `SOON` 上更早处理被打穿的空仓

### 2. 低成交主路径

新增 `self_volume_trigger`，语义与现有 `volume_trigger` 平行，但指标不同。

新增字段：

- `self_volume_trigger_enabled`
- `self_volume_trigger_window`
- `self_volume_trigger_start_threshold`
- `self_volume_trigger_stop_threshold`
- `self_volume_trigger_stop_cancel_open_orders`
- `self_volume_trigger_stop_close_all_positions`

默认行为：

- 最近 `15m gross_notional < stop_threshold`
  - 停 runner
  - 撤策略单
  - 启动 maker flatten 清仓
- flatten 完成后，最近 `15m gross_notional >= start_threshold`
  - 允许自动恢复

### 3. 硬停主路径

以下红线视为不可自动恢复的硬停：

- `rolling_hourly_loss_limit`
- `max_actual_net_notional`
- `max_synthetic_drift_notional`

触发后动作：

- 停 runner
- 撤单
- 清仓
- 不自动恢复

`max_cumulative_notional` 是否作为硬停保留，由具体币种策略决定：

- `CHIP` 保留，用于控制单次试跑总暴露时长
- `SOON` 保留，但阈值更低

## 触发规则

### 1. `self_volume_trigger`

统计函数：

- 复用 `_summarize_symbol_trade_window(summary_path, window_minutes=...)`
- 指标使用返回值里的 `gross_notional`

决策逻辑：

- runner 运行中：
  - 若 `gross_notional < stop_threshold`，动作 = `stop`
- runner 未运行，且上次是被 `self_volume_trigger` 停掉：
  - 若 `gross_notional >= start_threshold` 且 flatten 未运行，动作 = `start`
- 其他情况：
  - 动作 = `None`

要求：

- `start_threshold > stop_threshold`
- 默认窗口值只允许复用现有窗口集合：
  - `15m`
  - `30m`
  - `1h`
  - `4h`
  - `24h`

首版默认值统一使用 `15m`。

### 2. `adverse_inventory_reduce`

净空触发条件：

- `adverse_reduce_enabled = true`
- `current_short_notional > pause_short_position_notional`
- `short_cost_price > 0`
- `(mid_price - short_cost_price) / short_cost_price >= adverse_reduce_short_trigger_ratio`

净多触发条件：

- `adverse_reduce_enabled = true`
- `current_long_notional > pause_buy_position_notional`
- `long_cost_price > 0`
- `(long_cost_price - mid_price) / long_cost_price >= adverse_reduce_long_trigger_ratio`

触发动作：

- 同方向 entry 停止继续扩仓
- 先挂 maker `forced_reduce_order`
- 超过 `adverse_reduce_maker_timeout_seconds` 后，切 aggressive 价格
- 每轮减仓名义受 `adverse_reduce_max_order_notional` 限制
- 目标库存回落到 `pause * adverse_reduce_target_ratio`

### 3. `runtime_guard`

仍沿用现有逻辑，但在本设计里语义明确为：

- “最终保险丝”
- 一旦触发，不自动恢复

## CHIP 首版配置

新 profile：

- `chip_short_bias_ping_pong_guarded_v2`

核心运行参数：

- `strategy_mode = synthetic_neutral`
- `symbol = CHIPUSDT`
- `step_price = 0.0012`
- `buy_levels = 6`
- `sell_levels = 10`
- `per_order_notional = 20`
- `startup_entry_multiplier = 1.5`
- `base_position_notional = 0`
- `pause_buy_position_notional = 90`
- `pause_short_position_notional = 160`
- `max_position_notional = 120`
- `max_short_position_notional = 220`
- `max_total_notional = 300`
- `max_actual_net_notional = 70`
- `max_synthetic_drift_notional = 25`
- `rolling_hourly_loss_limit = 4`
- `max_cumulative_notional = 30000`

静态偏空结构：

- `static_buy_offset_steps = 1.2`
- `static_sell_offset_steps = 0.6`
- `near_market_entry_max_center_distance_steps = 3`
- `grid_inventory_rebalance_min_center_distance_steps = 5`
- `near_market_reentry_confirm_cycles = 4`
- `take_profit_min_profit_ratio = 0.0008`

主动减仓：

- `threshold_reduce_target_ratio = 0`
- `adverse_reduce_enabled = true`
- `adverse_reduce_short_trigger_ratio = 0.010`
- `adverse_reduce_long_trigger_ratio = 0.015`
- `adverse_reduce_target_ratio = 0.50`
- `adverse_reduce_maker_timeout_seconds = 30`
- `adverse_reduce_max_order_notional = 40`
- `adverse_reduce_keep_probe_scale = 0.20`

低成交触发：

- `self_volume_trigger_enabled = true`
- `self_volume_trigger_window = 15m`
- `self_volume_trigger_stop_threshold = 1000`
- `self_volume_trigger_start_threshold = 1800`
- `self_volume_trigger_stop_cancel_open_orders = true`
- `self_volume_trigger_stop_close_all_positions = true`

说明：

- `CHIP` 的首版目标是“量不够就撤、逆着走就减、继续恶化就硬停”
- 这版不追求最大换手，只验证库存回落链路是否稳

## SOON 首版配置

新 profile：

- `soon_volume_neutral_ping_pong_guarded_v2`

核心运行参数：

- `strategy_mode = synthetic_neutral`
- `symbol = SOONUSDT`
- `step_price = 0.00025`
- `buy_levels = 12`
- `sell_levels = 10`
- `per_order_notional = 25`
- `startup_entry_multiplier = 1.0`
- `base_position_notional = 0`
- `pause_buy_position_notional = 180`
- `pause_short_position_notional = 120`
- `max_position_notional = 220`
- `max_short_position_notional = 160`
- `max_total_notional = 340`
- `max_actual_net_notional = 50`
- `max_synthetic_drift_notional = 18`
- `rolling_hourly_loss_limit = 3`
- `max_cumulative_notional = 20000`

结构调整：

- `static_buy_offset_steps = 0.70`
- `static_sell_offset_steps = 0.95`
- `take_profit_min_profit_ratio = 0.0004`
- `adaptive_step_enabled = true`

主动减仓：

- `threshold_reduce_target_ratio = 0`
- `adverse_reduce_enabled = true`
- `adverse_reduce_short_trigger_ratio = 0.007`
- `adverse_reduce_long_trigger_ratio = 0.010`
- `adverse_reduce_target_ratio = 0.50`
- `adverse_reduce_maker_timeout_seconds = 45`
- `adverse_reduce_max_order_notional = 50`
- `adverse_reduce_keep_probe_scale = 0.20`

低成交触发：

- `self_volume_trigger_enabled = true`
- `self_volume_trigger_window = 15m`
- `self_volume_trigger_stop_threshold = 600`
- `self_volume_trigger_start_threshold = 1200`
- `self_volume_trigger_stop_cancel_open_orders = true`
- `self_volume_trigger_stop_close_all_positions = true`

说明：

- `SOON` 的重点不是继续放量，而是把“被单边拉穿的空仓”尽快压回安全区
- 先把空侧 pause / max_short 收紧，再让 adverse reduce 更早触发

## 配置与界面要求

### 1. 后端配置面

`RUNNER_DEFAULT_CONFIG` 与 `_normalize_runner_control_payload(...)` 需要支持新增字段：

- `self_volume_trigger_enabled`
- `self_volume_trigger_window`
- `self_volume_trigger_start_threshold`
- `self_volume_trigger_stop_threshold`
- `self_volume_trigger_stop_cancel_open_orders`
- `self_volume_trigger_stop_close_all_positions`

### 2. 命令行透传

runner command 需要新增对应 CLI 参数透传。

### 3. 监控页展示

监控页至少展示：

- 当前 `self_volume_trigger` 窗口
- 当前 `self_volume_trigger` 最近窗口 `gross_notional`
- start / stop 阈值
- 当前状态：
  - `runner_running`
  - `flatten_running`
  - `paused_by_trigger`
  - `last_action`

同时在 plan/summary 里保留：

- `adverse_inventory_reduce`
- `stop_reason`
- `stop_reasons`

## 测试要求

### 1. `self_volume_trigger`

- runner 运行中且 `15m gross_notional < stop_threshold` 时触发 stop
- stop 时会请求撤单与清仓
- 被 trigger 停掉后，若 `gross_notional >= start_threshold` 且 flatten 已结束，会触发 start
- `start_threshold <= stop_threshold` 时配置校验失败

### 2. `adverse_reduce`

- `threshold_reduce_target_ratio = 0` 时，`adverse_reduce` 可正常触发
- `threshold_reduce_target_ratio > 0` 时，`adverse_reduce` 被正确抑制并给出 blocked reason
- `CHIP` / `SOON` preset 中正确写入 adverse reduce 参数

### 3. preset 可见性

- `chip_short_bias_ping_pong_guarded_v2` 只在 `CHIPUSDT` 出现
- `soon_volume_neutral_ping_pong_guarded_v2` 只在 `SOONUSDT` 出现

## 上线顺序

1. 先实现 `self_volume_trigger` 基础框架与配置校验
2. 再补 `CHIP` / `SOON` guarded preset
3. 先部署 `CHIP` guarded v2，不立即放量
4. 观察：
   - `self_volume_trigger` 是否频繁抖动
   - `adverse_reduce` 是否能有效压回库存
   - 是否仍会被单边打穿至 runtime guard
5. 再切 `SOON` guarded v2

## 风险与注意事项

- `self_volume_trigger` 看的是策略自己成交额，若策略本身参数已失真，会更频繁停机；这正是设计目标，不视为误报。
- `adverse_reduce` 与 `threshold_target_reduce` 当前不可叠加；实现与 preset 必须保持这一约束一致。
- `self_volume_trigger` 自动恢复只适用于“低成交停机”，不适用于 runtime guard 红线停机。
- 若 flatten 长时间未完成，自动恢复必须继续被阻塞。

