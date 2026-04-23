# CHIPUSDT Short-Bias Ping-Pong v1 Design

日期：2026-04-23

## 目标

基于当前 `SOONUSDT` 专用运行配置 [soonusdt_loop_runner_control.json](/Volumes/WORK/binance/wangge/.worktrees/soon-loss-guard/output/soonusdt_loop_runner_control.json) 的近价 `synthetic_neutral` 结构，设计一套适合 `CHIPUSDT` 的保守版偏空刷量方案：

- 优先控制回撤，而不是优先冲量
- 先验证 `CHIP` 上的库存稳定性、回补节奏和熔断触发质量
- 只在效果稳定后再考虑提高单笔、层数和总限仓

这次设计默认只复用现有 runner / planner / guard 能力，不新增引擎逻辑。后续实现应以“新增一个 `CHIPUSDT` 专用 preset”为主，而不是改策略模式本身。

## 范围

纳入范围：

- `synthetic_neutral` 模式下的 `CHIPUSDT` 专用参数集
- `volume_neutral_ping_pong_v1` 风格的近价回转结构
- 长短两侧不对称限仓、挂单距离和熔断阈值
- 首轮试跑所需的运行时护栏

不纳入范围：

- 新增 `strategy_mode`
- 修改 `build_hedge_micro_grid_plan(...)` 或 `synthetic_neutral` 核心逻辑
- 自动按收益率动态放量
- 任何基于 `hedge mode` 的真双向持仓实现

## 基线与数据依据

### 1. SOON 母版

当前 `SOONUSDT` 专用控制文件使用：

- `strategy_profile = volume_neutral_ping_pong_v1`
- `strategy_mode = synthetic_neutral`
- `step_price = 0.00045`
- `buy_levels = 12`
- `sell_levels = 12`
- `per_order_notional = 30`
- `pause_buy_position_notional = 220`
- `pause_short_position_notional = 220`
- `max_position_notional = 260`
- `max_short_position_notional = 260`
- `max_total_notional = 520`
- `volatility_trigger_window = 1m`
- `volatility_trigger_amplitude_ratio = 0.015`
- `volatility_trigger_abs_return_ratio = 0.009`

这说明 `SOON` 方案本质上不是宽带库存网格，而是：

- 零底仓
- 近市场双边回转
- 小利润阈值止盈
- 1m 熔断保护
- 轻库存中性

### 2. 市场对比窗口

基于 Binance USD-M 公共行情接口，取到 2026-04-23 09:00（UTC+8）附近的最近 24h 窗口：

`SOONUSDT`

- 5m 平均振幅约 `0.62%`
- 1m 平均振幅约 `0.26%`
- 24h 涨幅约 `+10.5%`
- 5m 平均成交额约 `31.6 万 USDT`
- top20 深度双边各约 `10.5 万 / 10.4 万 USDT`
- 近 1m 实际最小有效跳动约 `0.0001`

`CHIPUSDT`

- 5m 平均振幅约 `3.83%`
- 1m 平均振幅约 `1.36%`
- 24h 涨幅约 `+116.9%`
- 5m 平均成交额约 `956 万 USDT`
- top20 深度双边各约 `2.26 万 / 2.34 万 USDT`
- 近 1m 实际最小有效跳动约 `0.00001`

### 3. 直接结论

`CHIP` 相比 `SOON` 有四个关键差异：

- 短周期振幅约大 `6x`
- 成交额约大 `30x`
- 盘口深度反而更薄
- 仍处于明显的强趋势和高关注窗口

因此不能直接平移 `SOON` 参数。若继续使用对称、贴价、轻熔断的中性回转，`CHIP` 上最容易出现的问题是：

- 近价双边被 1m 噪声反复扫单
- 强趋势上行时空头库存过快抬升
- 深度较薄下，大单回补比预期更容易打滑

## 方案选型

### 方案 A：直接镜像 SOON

保留 `12 x 12`、对称限仓和近似比例步长，只把 `step_price` 放大。

优点：

- 改动最小
- 最容易直接复刻

缺点：

- 对 `CHIP` 这种强趋势新盘过于乐观
- 长短对称会让空头库存上行过快
- 控回撤目标不成立

### 方案 B：改成 `inventory_target_neutral`

放弃近价 ping-pong，改用目标净仓曲线。

优点：

- 空仓目标更容易偏置到空侧
- 仓位语义更清晰

缺点：

- 失去 `SOON` 方案当前依赖的近价回转节奏
- `CHIP` 刚上市阶段，带宽很难一次定准
- 对“先跑起来看效果”的目标不够直接

### 方案 C：保留 ping-pong 骨架，改成偏空护栏版

继续使用 `synthetic_neutral` + 近价回转，但把库存、层数、报价距离和熔断都改成偏空不对称。

优点：

- 延续 `SOON` 当前可验证的执行结构
- 只新增 preset，不改引擎
- 能先跑出真实成交和库存反馈

缺点：

- 仍不是“真正仓位曲线控制器”
- 在单边急拉中仍会有空头库存风险，只是更可控

推荐采用方案 C。

## 设计

### 1. 核心思路

`CHIPUSDT` 的首版应该定义为“偏空护栏型近价中性”，而不是“标准中性”。

具体做法：

- 保留 `synthetic_neutral`
- 仍用近价挂单和 lot 配对回转
- 卖侧更近、更多、容量更大
- 买侧更远、更少、容量更小
- 在 1m 急跌时，不轻易暂停回补；优先允许小规模回补把短空仓位收回来
- 在 1m 急跌扩振时，更早暂停新的 long entry，避免本来偏空的策略被反向买单拖成偏多

这套的目标不是“绝对中性”，而是：

- 正常震荡时仍有双边成交
- 上冲时更容易形成轻净空
- 下跌时能逐步回补，但不在极端急跌里追着全部买回
- 一旦偏离过大，靠运行时护栏直接停新增

### 2. 参数原则

首版参数应满足四个约束：

- `step_price` 明显大于 `SOON` 的相对步长
- `sell_levels > buy_levels`
- `max_short_position_notional > max_position_notional`
- 明确启用 `max_actual_net_notional / max_synthetic_drift_notional / rolling_hourly_loss_limit`

为减少变量，首版不启用：

- `autotune_symbol_enabled`
- `adaptive_step_enabled`
- `market_bias_enabled`
- `synthetic_trend_follow_enabled`

原因很简单：这次先验证静态护栏是否足够，不引入第二层动态行为。

### 3. 保守版参数

建议作为 `chip_short_bias_ping_pong_v1` 的首版配置：

```json
{
  "strategy_profile": "chip_short_bias_ping_pong_v1",
  "strategy_mode": "synthetic_neutral",
  "symbol": "CHIPUSDT",

  "step_price": 0.0012,
  "buy_levels": 6,
  "sell_levels": 10,
  "per_order_notional": 20.0,
  "startup_entry_multiplier": 1.5,
  "base_position_notional": 0.0,

  "flat_start_enabled": true,
  "warm_start_enabled": true,

  "up_trigger_steps": 1,
  "down_trigger_steps": 1,
  "shift_steps": 1,

  "pause_buy_position_notional": 90.0,
  "pause_short_position_notional": 180.0,
  "max_position_notional": 120.0,
  "max_short_position_notional": 260.0,
  "max_total_notional": 320.0,
  "max_actual_net_notional": 80.0,
  "max_synthetic_drift_notional": 35.0,

  "static_buy_offset_steps": 1.2,
  "static_sell_offset_steps": 0.6,
  "near_market_entry_max_center_distance_steps": 3.0,
  "grid_inventory_rebalance_min_center_distance_steps": 5.0,
  "near_market_reentry_confirm_cycles": 4,

  "take_profit_min_profit_ratio": 0.0008,
  "threshold_position_notional": 0.0,
  "max_new_orders": 20,

  "buy_pause_amp_trigger_ratio": 0.018,
  "buy_pause_down_return_trigger_ratio": -0.010,
  "short_cover_pause_amp_trigger_ratio": 0.010,
  "short_cover_pause_down_return_trigger_ratio": -0.006,
  "freeze_shift_abs_return_trigger_ratio": 0.012,

  "rolling_hourly_loss_limit": 6.0,
  "max_cumulative_notional": 30000.0,

  "volatility_trigger_enabled": true,
  "volatility_trigger_window": "1m",
  "volatility_trigger_amplitude_ratio": 0.035,
  "volatility_trigger_abs_return_ratio": 0.020,
  "volatility_trigger_stop_cancel_open_orders": true,
  "volatility_trigger_stop_close_all_positions": false,
  "volatility_trigger_recover_before_resume": true,

  "sleep_seconds": 3.0,
  "leverage": 3,
  "maker_retries": 2,
  "autotune_symbol_enabled": false,
  "adaptive_step_enabled": false,
  "market_bias_enabled": false,
  "synthetic_trend_follow_enabled": false
}
```

### 4. 参数解释

#### `step_price = 0.0012`

按 2026-04-23 附近 `CHIP` 价格约 `0.121` 计算，单步约 `0.99%`。

这样做的原因：

- 明显宽于 `SOON` 的约 `0.25%`
- 低于 `CHIP` 常态 5m 平均振幅 `3.83%`
- 仍保留可成交性，不至于第一版就把量彻底做没

#### `buy_levels = 6` / `sell_levels = 10`

卖侧层数更高，意味着：

- 上冲时更容易逐级补空
- 下跌时回补更轻

这正是偏空而非对称中性的核心。

#### `pause_* / max_*`

长侧与空侧故意不对称：

- 长侧暂停更早：`90`
- 长侧上限更小：`120`
- 空侧暂停更晚：`180`
- 空侧上限更高：`260`

但首版仍要加上：

- `max_actual_net_notional = 80`
- `max_synthetic_drift_notional = 35`

这样即使虚拟账本或净敞口偏移，也会比库存上限更早触发停新增。

#### `static_buy_offset_steps > static_sell_offset_steps`

- 买侧离盘口更远
- 卖侧离盘口更近

这让策略天然更愿意在反弹里挂空，而不是在下跌里积极抢回补。

#### `buy_pause_*`

`buy_pause_*` 对应 long entry 的暂停条件。这里设得相对严格，是为了：

- `CHIP` 急跌扩振时，优先阻止策略误开更多 long leg
- 避免偏空方案被“抄底式反向补仓”拖坏

#### `short_cover_pause_*`

`short_cover_pause_*` 对应“暂停 BUY 回补空头”。这组阈值不能设得太紧，否则会在 `CHIP` 的常态高波动里经常停回补，反而把 short inventory 顶得更高。

因此首版用：

- `1m amplitude >= 1.0%`
- 且 `1m return <= -0.6%`

它的含义是：

- 普通扩振不暂停回补
- 只有在明显急跌时，才暂停追着买回

这比“量优先空头版”的激进空仓持有更保守。

#### `rolling_hourly_loss_limit = 6`

首轮试跑应允许策略自己收敛，但不能无限扛。

用小时滚动亏损上限的目的：

- 避免 `CHIP` 强单边里越刷越亏
- 给第一轮实盘一个明确停机线

#### `max_cumulative_notional = 30000`

这是首轮观察护栏，不是长期容量目标。

达到后应先停下来复盘：

- 单位成交损耗
- 实际净敞口峰值
- synthetic drift 触发频次
- 熔断后恢复质量

### 5. 预期行为

理想情况下，这套首版会表现为：

- 横盘时有一定双边成交，但不会像 `SOON` 那样高频
- 上冲时逐步偏空，但净空速度比纯做空模板慢
- 下跌时会有回补，但不会在每次急跌里把 short leg 很快全部买平
- 一旦 drift、净暴露或亏损超线，会比库存上限更早停新增

## 风险与取舍

已知取舍：

- 成交量一定低于更激进的 `CHIP` 版本
- 在极强趋势继续上冲时，仍可能因为偏空而承压
- `step_price` 放宽后，若 `CHIP` 波动突然钝化，成交会明显下降

但这些取舍是有意的。第一版的成功标准不是“量最大”，而是：

- 不容易被单边打穿
- 库存和 drift 可解释
- 熔断与停机阈值触发合理

## 测试与验证

实现时应至少验证：

- 新 preset 只复用现有 `synthetic_neutral` 参数，不修改 runner 逻辑
- `buy_levels / sell_levels`、`pause_*`、`max_*`、`offset_steps` 能被完整透传
- `max_actual_net_notional`、`max_synthetic_drift_notional`、`rolling_hourly_loss_limit` 会在 summary 中正确展示
- `volatility_trigger_*` 在 `CHIP` 这类 1m 高波动 symbol 上不会频繁误触发

首轮上线后的观察重点：

- 首个 30 分钟的成交密度
- `actual_net_notional` 峰值
- `synthetic_drift_notional` 峰值
- `short_cover_pause` 与 `volatility_trigger` 的触发频次
- 每 1 万 USDT 成交额对应的净损耗

## 部署建议

首轮实现建议只做两件事：

1. 在 `RUNNER_STRATEGY_PRESETS` 中新增 `CHIPUSDT` 专用 preset。
2. 用较低累计成交额护栏先跑一轮，确认行为正确后再讨论放量版。

不建议首轮同时做：

- 动态步长
- bias 自动换挡
- 更高单笔
- 更高总限仓

先把静态保守版跑稳，再决定是否派生 `chip_short_bias_ping_pong_v1_aggressive`。
