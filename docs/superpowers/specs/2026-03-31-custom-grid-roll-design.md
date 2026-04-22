# Custom Grid Conditional Roll-Down Design

## Goal

为 `custom_grid_enabled=true` 且 `custom_grid_direction=long` 的自定义币安式静态网格增加一个可选的“条件下移一格”能力：

- 定时检查网格是否已经明显偏离当前市场位置
- 仅在成交足够活跃且价格已经远离上沿时触发
- 每次最多下移一格
- 下移后重置成交计数，重新观察下一轮

这项能力的目标不是追价，而是在长时间下跌或弱势震荡中，缓慢把静态多头网格的工作区间往下搬，避免网格长期卡在过高区间导致“只剩减仓或只剩下方买单”的低效率状态。

## Non-Goals

- 不改普通滚动微网格逻辑
- 不改 `one_way_long` / `one_way_short` 的主策略行为
- 不做自动上移
- 不在一次检查里连续移动多格
- 不直接实现“追着价格挂减仓单直到全部成交”的接管模式

## Why This Is Needed

当前自定义静态网格已经按“当前价切分固定梯子”工作，但它仍然有一个问题：

- 当价格持续向下远离上沿时，静态区间会逐渐失去贴盘能力
- 价格长期停留在区间偏下位置时，卖格越来越远，成交主要集中在下方
- 如果区间不动，最终会出现量下降、减仓效率变弱或策略结构失衡

用户希望引入一种**慢速、受限、条件触发**的下移机制，而不是把静态网格改成完全滚动中心。

## User-Approved Trigger Definition

本设计只实现“下移”。

触发必须同时满足以下条件：

1. 达到检查时间点
2. 自上一次下移后累计成交笔数达到阈值
3. 当前价格距离网格最高价的剩余梯子距离达到阈值

本次确认的行为规则：

- 检查频率：按固定分钟间隔检查，默认 `5` 分钟，可配置
- 成交阈值：默认 `100` 笔，可配置
- 成交计数方式：**自上一次成功下移后重新累计**
- 价格条件：使用“距离上沿的剩余梯子层数”来衡量，而不是直接用绝对价格距离
- 每次触发：**只允许整体下移 1 格**

## Precise Price Distance Rule

给定固定梯子：

- `min_price`
- `max_price`
- `n` 格
- 网格模式：等差或等比

先构造完整梯子价格序列，再基于**当前成交参考价**计算：

- `levels_above_current`
  - 指当前价格到网格上沿之间，还剩多少个完整梯子层级
- `required_levels_above`
  - `ceil(n * upper_distance_ratio)`

当：

- `levels_above_current >= required_levels_above`

时，价格条件成立。

默认：

- `upper_distance_ratio = 0.30`

这表示：

- 当前价格已经比上沿低了至少 `30%` 的网格宽度

该定义适用于：

- 等差网格
- 等比网格

因为它使用的是“梯子层数”，而不是价格绝对差。

## Proposed Configuration Fields

以下字段仅对自定义静态网格有效，默认关闭：

- `custom_grid_roll_enabled: bool`
  - 是否启用条件下移
- `custom_grid_roll_interval_minutes: int`
  - 检查间隔，默认 `5`
- `custom_grid_roll_trade_threshold: int`
  - 自上次下移后需要累计的最少成交笔数，默认 `100`
- `custom_grid_roll_upper_distance_ratio: float`
  - 当前价格距上沿的剩余梯子比例阈值，默认 `0.30`
- `custom_grid_roll_shift_levels: int`
  - 每次触发时整体移动的梯子层数，默认 `1`

虽然当前需求是“一次一格”，但这里保留 `custom_grid_roll_shift_levels` 作为显式配置项，默认仍为 `1`。实现与 UI 应将它视为高级参数。

## State Additions

在 runner state 中新增：

- `custom_grid_roll_last_check_bucket: str | null`
  - 最近一次已执行检查的时间桶标识，例如 `2026-03-31T14:25Z`
- `custom_grid_roll_trade_baseline: int`
  - 上次成功下移后的累计成交基线
- `custom_grid_roll_trades_since_last_roll: int`
  - 当前相对基线的累计成交数
- `custom_grid_roll_last_applied_at: str | null`
  - 最近一次成功下移的时间
- `custom_grid_roll_last_applied_price: float | null`
  - 最近一次成功下移时的参考价格

## Trade Count Source

成交计数不依赖内存中的本轮 fill，而依赖现有审计链路：

- `summary_jsonl` 对应的审计路径由 `build_audit_paths()` 推导
- 实际成交数据来自 `trade_audit.jsonl`

推荐实现方式：

1. 在每轮 cycle 中读取该 symbol 的 `trade_audit` 总行数或最新累计值
2. 用 `current_trade_count - custom_grid_roll_trade_baseline` 得到“自上次下移后的成交笔数”
3. 下移成功后，把 `custom_grid_roll_trade_baseline` 更新为当前累计成交值

这样可以保证：

- 进程重启后可恢复
- 不依赖内存态
- 与现有审计系统一致

## Execution Flow

仅在以下条件全满足时才进入判定：

- `custom_grid_enabled = true`
- `custom_grid_direction = long`
- `custom_grid_roll_enabled = true`
- 当前币种有有效的静态梯子定义

每轮 cycle 执行顺序建议为：

1. 读取当前固定网格配置与 runner state
2. 读取当前成交参考价
3. 读取当前 `trade_audit` 累计成交笔数
4. 计算当前所在检查时间桶
5. 若本时间桶已检查过，则跳过
6. 计算：
   - `trades_since_last_roll`
   - `levels_above_current`
   - `required_levels_above`
7. 若成交阈值与价格阈值同时满足：
   - 将 `min_price`、`max_price` 整体下移 `shift_levels * ladder_step`
   - 重建 `center_price` / 梯子参考价
   - 更新 state 中的成交基线和最近一次成功下移信息
8. 无论是否下移，都更新 `custom_grid_roll_last_check_bucket`

## How The Downward Shift Is Applied

### Arithmetic

- 下移价格步长 = 单格固定价差
- 新区间：
  - `new_min = old_min - shift_levels * step`
  - `new_max = old_max - shift_levels * step`

### Geometric

- 下移按梯子层数移动，不直接按固定价差移动
- 建议使用当前梯子序列，把区间整体映射到更低的相邻层级
- 实现重点不是“简单相减”，而是保持等比梯子的层级关系不变

## Safety Rules

必须同时实现以下边界保护：

- 每个检查桶最多执行一次
- 每次最多移动 `custom_grid_roll_shift_levels`
- 若下移后 `new_min <= 0`，跳过并记 warning
- 若梯子重建失败，保留旧配置，不写半成状态
- 若当前价格高于上沿，不触发下移
- 若当前价格低于下沿，也仍然只允许一次检查最多下移一格

## UI Changes

在自定义币安式网格策略区域新增以下配置项：

- 启用条件下移
- 检查周期（分钟）
- 成交阈值（笔）
- 距离上沿比例阈值
- 每次下移层数

在监控页新增展示：

- 条件下移：开 / 关
- 下移检查周期
- 自上次下移后累计成交
- 当前距离上沿剩余层数
- 最近一次下移时间
- 最近一次下移前后区间

## Monitoring / Observability

每次检查应在 summary 或 audit 里输出一条结构化信息，至少包括：

- `custom_grid_roll_checked`
- `custom_grid_roll_triggered`
- `custom_grid_roll_reason`
- `current_trade_count`
- `trades_since_last_roll`
- `levels_above_current`
- `required_levels_above`
- `old_min_price`
- `old_max_price`
- `new_min_price`
- `new_max_price`

这样后续才方便验证：

- 为什么没触发
- 为什么触发
- 触发后区间变成了什么

## Recommended Defaults

首版默认值：

- `custom_grid_roll_enabled = false`
- `custom_grid_roll_interval_minutes = 5`
- `custom_grid_roll_trade_threshold = 100`
- `custom_grid_roll_upper_distance_ratio = 0.30`
- `custom_grid_roll_shift_levels = 1`

## Risks

- 下移本质上是在承认区间判断偏高，可能放大“高位库存尚未完全释放”的遗留问题
- 若检查周期太短、成交阈值太低，会把静态网格做成慢速追价网格
- 若实现时直接按价格差移动等比区间，会破坏几何梯子的结构

## Recommendation

首版建议：

- 只对 `custom_grid_direction=long` 开启
- 只做“下移”
- 默认关闭
- 只在自定义静态网格里生效

等这版验证稳定后，再考虑：

- short 网格镜像的“条件上移”
- 与超额减仓接管模式联动
- 结合 realized pnl 或 inventory notional 做更智能的区间迁移

## Future Extension: Conditional Roll-Up

这部分**不是当前实现范围**，只作为后续扩展草案。

如果后续要解决“价格持续上涨、静态网格逐渐脱离下方、卖格越来越少或完全跑出区间”的问题，建议为 `custom_grid_direction=long` 增加一套与本次“条件下移”对称的`条件上移`能力。

### Goal

在价格长期向上、当前静态网格已经明显偏离现价时，缓慢把整个长网格区间往上搬，避免：

- 现价长期高于上沿
- 策略逐渐退化成单边残留委托
- 已持有底仓无法在更合适的位置恢复双边网格

### Proposed Trigger Model

建议仍然沿用“定时检查 + 成交确认 + 价格位置确认 + 单次只移动一格”的框架，保持与条件下移规则对称。

候选配置项：

- `custom_grid_lift_enabled: bool`
  - 是否启用条件上移
- `custom_grid_lift_interval_minutes: int`
  - 检查间隔，建议默认与下移一致
- `custom_grid_lift_trade_threshold: int`
  - 自上次成功上移后需要累计的最少成交笔数
- `custom_grid_lift_lower_distance_ratio: float`
  - 当前价格距下沿的剩余梯子比例阈值
- `custom_grid_lift_shift_levels: int`
  - 每次上移的层数，建议默认 `1`

### Suggested Price Rule

与下移规则保持镜像：

- 先计算当前价格到网格下沿之间，还剩多少个完整梯子层级
- 记为 `levels_below_current`
- 计算：
  - `required_levels_below = ceil(n * lower_distance_ratio)`

当：

- `levels_below_current >= required_levels_below`

时，价格条件成立。

默认可考虑：

- `lower_distance_ratio = 0.30`

直观含义是：

- 当前价格已经比下沿高出了至少 `30%` 的网格宽度

### Suggested Additional Guard

相比下移，上移更容易把长网格做成追涨结构，因此建议额外加一层限制，至少满足以下其一才允许上移：

- 当前价格已连续多个检查桶停留在区间上半部
- 当前价格高于上沿或非常接近上沿
- 当前已持仓低于目标底仓，说明原有卖格已大量成交

推荐首选这一条：

- `现价 >= 上沿`
  - 或 `现价距离上沿不足 1-2 格`

这样能避免区间只因短期反弹就频繁上移。

### State Additions

若未来实现条件上移，建议新增：

- `custom_grid_lift_last_check_bucket`
- `custom_grid_lift_trade_baseline`
- `custom_grid_lift_trades_since_last_roll`
- `custom_grid_lift_last_applied_at`
- `custom_grid_lift_last_applied_price`

命名与下移保持镜像，减少维护复杂度。

### Interaction With Conditional Roll-Down

如果未来上下移都支持，必须定义优先级，避免同一检查桶内出现相互冲突：

- 同一时间桶最多只允许一个方向生效
- 若上下条件同时满足，建议优先执行“更接近脱离网格的方向”
- 或更简单：默认优先保持不动，等待下一检查桶再判定

首版更稳妥的建议是：

- 同时满足时，不移动，并记录 warning

### Recommendation For Future Phase

如果后续要做主动上移，建议按以下顺序推进：

1. 先上线并观察“条件下移”单向规则
2. 评估长期运行中：
   - 网格脱离上沿的频率
   - 卖格消失时的成交效率损失
   - 人工重设区间的频率
3. 再决定是否引入条件上移

这样可以避免在还没验证单向区间迁移稳定性之前，就把静态网格过早变成“双向自动追价网格”。
