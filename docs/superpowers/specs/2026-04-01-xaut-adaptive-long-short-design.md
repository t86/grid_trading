# XAUT 自适应多空策略设计

日期：2026-04-01

## 摘要

新增两个仅用于 XAUT 的策略模板：

- `xaut_long_adaptive_v1`
- `xaut_short_adaptive_v1`

这两个模板都是面向 `XAUTUSDT` 的专用状态机策略。它们保留现有单向微网格执行模型，但会在以下三种风控状态之间自动切换：

- `normal`
- `defensive`
- `reduce_only`

目标是：

- 在平稳行情下尽量保持成交量
- 在波动升高时减少库存继续扩张
- 在极端波动时立即停止同方向新增库存
- 在波动回落后自动恢复到正常刷量状态

本设计**不会**自动全平仓，也**不会**停止 runner 进程。在 `reduce_only` 状态下，runner 仍继续运行，但只会下能减少现有库存的订单。

## 问题背景

当前 runner 已经有两个可用能力，但对 XAUT 来说都不够：

1. 通用 `auto_regime` 只支持 `stable <-> defensive` 切换，而且是按通用品种阈值设计，不适合 XAUT。
2. `excess_inventory_reduce_only_enabled` 能在当前库存超过目标底仓后阻止继续扩仓，但它本身不会根据 XAUT 的波动状态自动触发。

对 `XAUTUSDT` 来说，最近的市场波动明显比当前通用 `auto_regime` 的阈值更窄。

基于 2026-03-31 Binance Futures 公开市场数据观察：

- 近 48 小时 `15m` 振幅中位数：约 `0.204%`
- 近 48 小时 `15m` 振幅 `P95`：约 `0.614%`
- 近 7 天 `1h` 振幅中位数：约 `0.341%`
- 近 7 天 `1h` 振幅 `P95`：约 `1.186%`

当前通用阈值明显宽于这些范围，因此 XAUT 很少会真正从 stable 切出，这不符合目标风控行为。

## 范围

纳入范围：

- 新增 XAUT 专用的自适应 long 和 short 模板
- 新增 XAUT 专用的状态判断逻辑
- 新增三态切换和滞后确认机制
- 新增 `reduce_only` 行为：立即移除同方向开仓单
- 在 monitor/web 中展示当前 XAUT 状态和原因
- 为状态切换逻辑和订单裁剪行为补测试

不纳入范围：

- 修改非 XAUT 策略模板
- 自动停机
- 自动全平仓/清仓
- 抽象成通用多币种自适应框架

## 策略模板

### `xaut_long_adaptive_v1`

方向：`one_way_long`

行为：

- `normal`：标准 XAUT 做多刷量网格
- `defensive`：更轻地继续接多，更宽的步长，更快卸掉库存
- `reduce_only`：立即移除所有新增多仓订单，只保留能卖出减多仓的订单

### `xaut_short_adaptive_v1`

方向：`one_way_short`

行为：

- `normal`：标准 XAUT 做空镜像网格
- `defensive`：更轻地继续加空，更宽的步长，更偏向快速回补空仓
- `reduce_only`：立即移除所有新增空仓订单，只保留能买回减空仓的订单

## 状态模型

两个模板都使用相同的状态名：

- `normal`
- `defensive`
- `reduce_only`

两个模板都使用相同的波动输入：

- `15m` 振幅
- `60m` 振幅
- `15m` 涨跌幅
- `60m` 涨跌幅

定义：

- 振幅 = `(high - low) / open`
- 涨跌幅 = `(close - open) / open`

状态优先级：

1. `reduce_only`
2. `defensive`
3. `normal`

如果多个条件同时满足，取优先级更高的状态。

## 阈值设计

这些阈值是 XAUT 专用的，依据 2026-03-31 的波动分布设定，刻意比当前通用 `auto_regime` 更紧。

### 共享稳定阈值

用于 long 和 short 的恢复判断：

- `stable_15m_max_amplitude_ratio = 0.0035`
- `stable_60m_max_amplitude_ratio = 0.0075`

### Long 阈值

进入 `normal` 的条件：

- `15m amplitude <= 0.35%`
- `60m amplitude <= 0.75%`
- `60m return >= -0.30%`

进入 `defensive` 的条件，满足任一即可：

- `15m amplitude >= 0.60%`
- `60m amplitude >= 1.20%`
- `15m return <= -0.40%`
- `60m return <= -0.80%`

进入 `reduce_only` 的条件，满足任一即可：

- `15m amplitude >= 0.90%`
- `60m amplitude >= 1.60%`
- `15m return <= -0.70%`
- `60m return <= -1.20%`

### Short 阈值

进入 `normal` 的条件：

- `15m amplitude <= 0.35%`
- `60m amplitude <= 0.75%`
- `60m return <= 0.30%`

进入 `defensive` 的条件，满足任一即可：

- `15m amplitude >= 0.60%`
- `60m amplitude >= 1.20%`
- `15m return >= 0.40%`
- `60m return >= 0.80%`

进入 `reduce_only` 的条件，满足任一即可：

- `15m amplitude >= 0.90%`
- `60m amplitude >= 1.60%`
- `15m return >= 0.70%`
- `60m return >= 1.20%`

## 状态切换规则

相邻状态切换需要连续确认，以避免来回抖动：

- `normal -> defensive`：连续 2 轮
- `defensive -> normal`：连续 2 轮
- `defensive -> reduce_only`：连续 1 轮
- `reduce_only -> defensive`：连续 2 轮

直接切换规则：

- `normal -> reduce_only`：允许首轮满足即立刻切换
- `reduce_only -> normal`：不允许直接切，必须先经过 `defensive`

设计原因：

- 进入 `reduce_only` 必须足够快，因为用户明确要求极端波动时立即停止继续买入/开仓
- 恢复速度要慢于升级速度
- 强制 `reduce_only -> defensive -> normal`，可以让系统先经历冷却阶段，而不是立刻回到激进刷量模式

## 参数集

这些参数有意比当前通用模板更轻，更适合 XAUT 的价格水平和波动结构。

### Long `normal`

- `step_price = 7.5`
- `buy_levels = 6`
- `sell_levels = 10`
- `per_order_notional = 80`
- `base_position_notional = 320`
- `up_trigger_steps = 5`
- `down_trigger_steps = 4`
- `shift_steps = 3`
- `pause_buy_position_notional = 520`
- `max_position_notional = 680`
- `buy_pause_amp_trigger_ratio = 0.0060`
- `buy_pause_down_return_trigger_ratio = -0.0045`
- `freeze_shift_abs_return_trigger_ratio = 0.0048`
- `inventory_tier_start_notional = 420`
- `inventory_tier_end_notional = 520`
- `inventory_tier_buy_levels = 3`
- `inventory_tier_sell_levels = 12`
- `inventory_tier_per_order_notional = 70`
- `inventory_tier_base_position_notional = 160`

### Long `defensive`

- `step_price = 12.0`
- `buy_levels = 2`
- `sell_levels = 14`
- `per_order_notional = 45`
- `base_position_notional = 100`
- `up_trigger_steps = 4`
- `down_trigger_steps = 6`
- `shift_steps = 2`
- `pause_buy_position_notional = 180`
- `max_position_notional = 260`
- `buy_pause_amp_trigger_ratio = 0.0045`
- `buy_pause_down_return_trigger_ratio = -0.0035`
- `freeze_shift_abs_return_trigger_ratio = 0.0040`
- `inventory_tier_start_notional = 140`
- `inventory_tier_end_notional = 180`
- `inventory_tier_buy_levels = 1`
- `inventory_tier_sell_levels = 16`
- `inventory_tier_per_order_notional = 40`
- `inventory_tier_base_position_notional = 60`

### Long `reduce_only`

沿用 `defensive` 参数集，并额外施加以下行为覆盖：

- 强制 `excess_inventory_reduce_only_enabled = true`
- 移除所有 `bootstrap_orders`
- 移除所有 `buy_orders`
- 依赖 stale-order cancellation 立即把当前属于本 runner 的买单从盘口撤掉

### Short `normal`

- `step_price = 7.5`
- `buy_levels = 10`
- `sell_levels = 6`
- `per_order_notional = 80`
- `base_position_notional = 320`
- `up_trigger_steps = 4`
- `down_trigger_steps = 5`
- `shift_steps = 3`
- `pause_short_position_notional = 520`
- `max_short_position_notional = 680`
- `short_cover_pause_amp_trigger_ratio = 0.0060`
- `short_cover_pause_down_return_trigger_ratio = -0.0045`
- `inventory_tier_start_notional = 420`
- `inventory_tier_end_notional = 520`
- `inventory_tier_buy_levels = 12`
- `inventory_tier_sell_levels = 3`
- `inventory_tier_per_order_notional = 70`
- `inventory_tier_base_position_notional = 160`

### Short `defensive`

- `step_price = 12.0`
- `buy_levels = 14`
- `sell_levels = 2`
- `per_order_notional = 45`
- `base_position_notional = 100`
- `up_trigger_steps = 6`
- `down_trigger_steps = 4`
- `shift_steps = 2`
- `pause_short_position_notional = 180`
- `max_short_position_notional = 260`
- `short_cover_pause_amp_trigger_ratio = 0.0045`
- `short_cover_pause_down_return_trigger_ratio = -0.0035`
- `inventory_tier_start_notional = 140`
- `inventory_tier_end_notional = 180`
- `inventory_tier_buy_levels = 16`
- `inventory_tier_sell_levels = 1`
- `inventory_tier_per_order_notional = 40`
- `inventory_tier_base_position_notional = 60`

### Short `reduce_only`

沿用 `defensive` 参数集，并额外施加以下行为覆盖：

- 强制 `excess_inventory_reduce_only_enabled = true`
- 移除所有 `bootstrap_orders`
- 移除所有 `sell_orders`
- 依赖 stale-order cancellation 立即把当前属于本 runner 的卖单从盘口撤掉

## 执行流程

每轮执行：

1. 像现在的 runner 一样加载市场数据和 symbol info。
2. 如果选中的模板是新的 XAUT 自适应模板，则从最近 `15m` 和 `1h` K 线计算 XAUT 状态指标。
3. 根据方向评估候选状态。
4. 按状态切换确认规则解析当前生效状态。
5. 用当前状态对应的参数集覆盖 runner 运行参数。
6. 沿用现有单向 long 或单向 short planner 生成计划。
7. 如果当前状态是 `reduce_only`，则从计划中裁掉同方向开仓单。
8. 像现在一样执行提交/撤单逻辑，并依赖现有 stale-order cancellation 撤掉旧单。

这样可以保留当前执行主路径，只改变参数选择和最终订单裁剪。

## 实现形态

### Preset

在 web/server 的 preset 注册表中新增：

- `xaut_long_adaptive_v1`
- `xaut_short_adaptive_v1`

这些 preset 应该：

- 固定 `symbol = XAUTUSDT`
- 打开新的 XAUT 自适应模式开关
- 包含状态阈值和确认轮数
- 默认 `autotune_symbol_enabled = false`
- 默认 `cancel_stale = true`

### Loop runner

在 `loop_runner.py` 中新增 XAUT 专用辅助逻辑：

- 状态常量
- long 和 short 各自的状态参数表
- XAUT 当前状态评估函数
- 带滞后的状态切换解析函数
- 当前状态参数覆盖函数
- 计划生成后的 `reduce_only` 裁单函数

这部分应与现有通用 `auto_regime` 路径保持分离，而不是硬把通用 stable/defensive 模型扩成三态 XAUT 控制器。

### Monitor/Web

在 summary 输出中增加以下字段：

- `xaut_adaptive_enabled`
- `xaut_adaptive_direction`
- `xaut_adaptive_state`
- `xaut_adaptive_candidate_state`
- `xaut_adaptive_pending_count`
- `xaut_adaptive_reason`
- `xaut_adaptive_metrics`

web/monitor 需要清晰展示当前是否处于 `reduce_only`，以及是否正在抑制同方向开仓单。

## 异常处理

如果无法计算状态指标：

- 保持上一个生效状态不变
- 在 loop summary 中记录 warning
- 不因缺失数据进行状态切换

如果用户把 XAUT 自适应模板错误地用在非 `XAUTUSDT` 的 symbol 上：

- 启动时直接报清晰的配置错误

如果关闭了 stale-order cancellation：

- `reduce_only` 仍然应当从生成计划中移除开仓单
- 但 preset 默认仍保持 `cancel_stale = true`
- monitor 输出中要提示：盘口上已有的同方向旧单可能不会立即消失，需要手动清理

## 测试

新增测试覆盖：

- long 在 normal/defensive/reduce_only 三态下的分类
- short 在 normal/defensive/reduce_only 三态下的分类
- 带确认轮数的状态切换行为
- 强制 `normal -> reduce_only`
- 强制 `reduce_only -> defensive`
- 不允许直接 `reduce_only -> normal`
- long 在 `reduce_only` 下裁掉 `buy_orders` 和 `bootstrap_orders`
- short 在 `reduce_only` 下裁掉 `sell_orders` 和 `bootstrap_orders`
- preset 接线和 symbol 保护
- monitor/web summary 正确展示新状态字段

## 风险

- 阈值可能对当前样本窗口拟合过度
- 如果恢复阈值过松，状态可能仍会来回抖动
- 即使进入 `reduce_only`，旧的同方向挂单也可能因撤单延迟而短暂残留在盘口

## 非目标

- 面向所有币种的通用自适应框架
- 运行时动态重估阈值
- 自动停机或自动全平仓
