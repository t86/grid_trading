# BASED Overnight Inventory Entry Offset Design

日期：2026-04-10

## 目标

基于 `origin/main` 新增的 `BASED` 候选策略 [basedusdt_overnight_volume_v1.json](/Volumes/WORK/binance/wangge/deploy/oracle/runtime_configs/basedusdt_overnight_volume_v1.json)，调整 `synthetic_neutral` 的入场挂单行为：

- `0` 仓启动时，保留贴盘口的 `buy1/sell1`
- 一旦已有库存，不再让新的 `entry_long/entry_short` 贴盘口
- 仅把开仓单向外移动 `1 * step_price`
- `take_profit_long/take_profit_short` 逻辑保持不变

目标是在不明显牺牲成交量的前提下，降低轻仓阶段因为贴盘口反复补仓带来的磨损。

## 范围

纳入范围：

- `build_hedge_micro_grid_plan(...)` 的 `entry_long/entry_short` 价格生成
- `BASED overnight volume` 候选配置的部署适配
- 覆盖 `flat`、轻多仓、轻空仓三类核心测试

不纳入范围：

- `take_profit_*` 的价格逻辑
- 仓位阈值、buffer 减仓、强制减仓逻辑
- `XAUT` 或其他 symbol 的策略切换
- 停止策略后的 `maker_flatten` 逻辑

## 当前问题

`origin/main` 当前的 `synthetic_neutral` 计划器里：

- `flat_inventory` 时，首档 `entry_long` 直接锚定 `bid`
- `flat_inventory` 时，首档 `entry_short` 直接锚定 `ask`
- 有库存时，`_entry_buy_price()` 和 `_entry_sell_price()` 仍可能把首档开仓单放在最贴近盘口的一档

这会带来两个后果：

- `0` 仓启动成交快，这部分符合目标
- 轻仓后继续用贴盘口的新增开仓单追价，容易在单边里增加磨损

## 设计

### 1. 行为规则

订单分成两类处理：

- `entry_*`
  - 负责继续开仓或补仓
- `take_profit_*`
  - 负责已有 lot 的止盈或回补

新规则只改 `entry_*`：

- 当 `flat_inventory=true`
  - `entry_long` 首档仍从 `bid_price` 开始
  - `entry_short` 首档仍从 `ask_price` 开始
- 当存在任一方向库存
  - `entry_long` 的最近价格从 `bid_price - step_price` 开始
  - `entry_short` 的最近价格从 `ask_price + step_price` 开始

这里的“存在库存”按当前计划器已有语义判断：

- 实际进入价格生成的是 `effective_long_qty/effective_short_qty`
- tiny residual 和 residual flatten 先照旧处理
- 只有在这些归一化处理之后仍然非 `flat_inventory`，才启用“外移一格”

### 2. 价格细节

价格规则保持对称：

- `flat`：
  - 买侧距离步数为 `level - 1 + buy_offset_steps`
  - 卖侧距离步数为 `level - 1 + sell_offset_steps`
- `non-flat`：
  - 买侧最小距离至少为 `1`
  - 卖侧最小距离至少为 `1`

也就是：

- `level=1`
  - flat 时仍可能贴 `buy1/sell1`
  - non-flat 时必须至少离盘口 `1 * step_price`

已有 `buy_offset_steps/sell_offset_steps` 仍然生效，但它们叠加在“non-flat 最小距离为 1”这个新下限之上，而不是把它绕开。

### 3. 不变部分

以下行为不改：

- `take_profit_long/take_profit_short` 继续按 lot 和 profit guard 挂
- flat 启动的首轮双边贴盘口仍保留
- 仓位阈值、pause、buffer、active_delever 逻辑不改
- `bootstrap_*` 逻辑不改

## 影响

预期影响：

- `0` 仓启动时，成交速度基本不变
- 轻仓后，新增开仓单不会再紧贴盘口反复追价
- 轻仓时的总成交量会略降，但不应像“整套静态外移”那样明显下降

已知取舍：

- 如果行情快速反向，轻仓后的再次开仓会比贴盘口慢一档
- 这是有意接受的换量降损

## 测试

新增或更新以下回归测试：

- `flat` 状态下仍允许 `entry_long=bid`、`entry_short=ask`
- 有多仓时，`entry_short` 最近档不再等于 `ask`
- 有空仓时，`entry_long` 最近档不再等于 `bid`
- `take_profit_*` 价格不受此改动影响

## 部署建议

部署候选使用 [basedusdt_overnight_volume_v1.json](/Volumes/WORK/binance/wangge/deploy/oracle/runtime_configs/basedusdt_overnight_volume_v1.json) 作为基线，但在真正上 `111` 前，需要先合入上述 planner 变更。否则主线默认行为仍会在轻仓时保留贴盘口首档，和本设计不一致。
