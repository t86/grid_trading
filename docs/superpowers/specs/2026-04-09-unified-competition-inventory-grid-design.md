# Unified Competition Inventory Grid Design

日期：2026-04-09

## 目标

设计一套同时适用于 `futures` 与 `spot` 的通用刷交易赛执行框架。它不再以“市场最近成交”或“中心价迁移”为核心，而是以“本策略自己的最近有效成交价 + 库存 lot 账本 + 配对步长积分”作为统一驱动。

这套设计要满足以下约束：

- `最近成交价` 明确定义为“本策略在币安上的最近一笔有效成交价”，不是市场最近成交价
- `spot` 只允许底仓式 `long`，不允许净卖空
- `futures` 在空仓时双向等待首单，谁先成交谁定义当前方向
- 强制减仓、尾仓清理不更新网格锚点
- 超阈值减仓优先按“最不利 lot”处理
- 配对收益按“赚回多少步长”累计，不计手续费
- 配对积分只存在内存里，服务重启后不恢复

## 范围

纳入范围：

- 一个统一的状态模型，覆盖 `futures` 与 `spot`
- 首单、常规网格、尾仓清理、阈值减仓、硬上限减仓
- 基于 lot 的库存账本
- 基于已完成配对的步长积分账本
- 基于交易所成交历史的重启恢复逻辑

不纳入范围：

- `spot` 净卖空
- 自动切换“方向信号”
- taker 强平或超时转 taker
- 资金费、手续费、滑点驱动的动态阈值
- 多策略共享同一 symbol 的协调问题

## 核心定义

### 1. 成交与价格定义

- `book_price`
  - 指当前盘口价
  - 买侧用买一，卖侧用卖一
- `strategy_fill`
  - 指本策略自己产生并在交易所成交的订单成交
- `grid_anchor_price`
  - 当前网格锚点价格
  - 仅由 `首单成交` 与 `常规网格成交` 更新
- `risk_fill_price`
  - 风控减仓类订单的成交价
  - 不会写回 `grid_anchor_price`

### 2. 订单角色

- `bootstrap_entry`
  - 空仓状态下的首单
- `grid_entry`
  - 常规同方向补仓单
- `grid_exit`
  - 常规反向减仓/止盈单
- `forced_reduce`
  - 阈值或硬上限触发后的专门减仓单
- `tail_cleanup`
  - 剩余仓位不足一格时的贴盘口清仓单

### 3. 账本定义

- `position_lots`
  - 当前仍未完全对掉的库存 lot
  - `long/spot` lot 记录买入数量与成本价
  - `short` lot 记录开空数量与开空价
- `pair_credit_steps`
  - 历史上已完成配对累计赚回的“步长积分”
  - 只保存在内存
  - 服务重启后归零

## 统一状态模型

### 1. 市场维度

- `market_type = futures | spot`

### 2. 方向状态

- `flat`
- `long_active`
- `short_active`

约束：

- `spot` 只允许 `flat -> long_active -> flat`
- `futures` 允许 `flat -> long_active` 或 `flat -> short_active`
- 不存在显式“切方向”动作
- 只有当前仓位真正清零，才允许回到 `flat`
- 从 `flat` 再次进入方向态时，由新的首单成交自然决定方向

### 3. 风控状态

- `normal`
- `threshold_reduce_only`
- `hard_reduce_only`

风控状态与方向状态独立组合，例如：

- `long_active + normal`
- `long_active + threshold_reduce_only`
- `short_active + hard_reduce_only`

### 4. 尾仓状态

尾仓不单独建新主状态，而是作为一个执行模式：

- 当剩余仓位不足标准一格时，进入 `tail_cleanup` 执行模式
- 此时不再维护完整网格，只生成一笔贴盘口的 maker 清仓单

## 参数模型

### 1. 通用参数

- `step_price`
- `per_order_notional`
- `first_order_multiplier`
- `threshold_position_notional`
- `max_order_position_notional`
- `max_position_notional`

推导项：

- `first_order_notional = per_order_notional * first_order_multiplier`

### 2. 风控口径

- `threshold_position_notional`
  - 实际持仓名义达到该值后，进入阈值减仓判断
- `max_order_position_notional`
  - 运行中用于约束“实际持仓 + 同方向未成交挂单潜在新增持仓”
- `max_position_notional`
  - 实际持仓硬上限

统一按“名义金额”配置，不按格数或数量配置。

## 订单生成规则

### 1. `flat` 状态首单

#### `futures`

- 在买一挂一笔 `bootstrap_entry` 买单
- 在卖一挂一笔 `bootstrap_entry` 卖单
- 两边都使用 `first_order_notional`
- 谁先成交，当前方向即确定为该方向
- 另一边未成交首单立即撤销

#### `spot`

- 只在买一挂一笔 `bootstrap_entry` 买单
- 数量按 `first_order_notional`
- 成交后进入 `long_active`

### 2. 首单成交后的处理

- 写入 `grid_anchor_price = 首单成交价`
- 把成交拆成一个或多个 `position_lots`
- 进入方向态
- 生成第一轮常规网格

### 3. 常规网格的核心原则

常规网格不做“整套围绕新锚点整体平移”，而只保证离 `grid_anchor_price` 最近的优先档位存在。

#### `long_active`

- 最近买单价格：`grid_anchor_price - 1 * step_price`
- 最近卖单价格：`grid_anchor_price + 1 * step_price`

#### `short_active`

- 最近卖单价格：`grid_anchor_price + 1 * step_price`
- 最近买单价格：`grid_anchor_price - 1 * step_price`

这两个最近档位是硬优先档位，优先于所有远端档位。

### 4. 远端档位扩展

在最近档位之外，按 `2, 3, 4 ...` 个步长向外扩展远端档位。是否继续扩展，受以下条件共同裁剪：

- `threshold_position_notional`
- `max_order_position_notional`
- `max_position_notional`
- 当前余额或保证金
- 交易所最小数量与最小名义限制

执行原则：

- 近端优先
- 同价位优先补差额，不主动撤旧单重挂
- 只有目标价位消失或同价位目标缩量时才撤旧单

### 5. 哪些成交会更新锚点

以下成交会更新 `grid_anchor_price`：

- `bootstrap_entry`
- `grid_entry`
- `grid_exit`

以下成交不会更新 `grid_anchor_price`：

- `forced_reduce`
- `tail_cleanup`

### 6. 尾仓清理

当剩余持仓不足一格标准量时：

- 停止维护完整网格
- 只保留一笔 `tail_cleanup`
- 该订单贴盘口、maker-only
- 成交后方向状态回到 `flat`

## 风控与减仓状态机

### 1. 状态切换

- `normal -> threshold_reduce_only`
  - `effective_position_notional >= threshold_position_notional`
- `normal -> hard_reduce_only`
  - `effective_position_notional >= max_position_notional`
- `threshold_reduce_only -> hard_reduce_only`
  - 实际持仓继续增加并达到 `max_position_notional`
- `threshold_reduce_only -> normal`
  - 实际持仓回落到 `threshold_position_notional` 以下
- `hard_reduce_only -> threshold_reduce_only`
  - 实际持仓降回硬上限以下但仍高于阈值
- `hard_reduce_only -> normal`
  - 实际持仓直接降回阈值以下

### 2. `max_order_position_notional` 的意义

该限制只在服务运行时生效。

定义：

- `same_side_open_orders_exposure`
  - 当前所有同方向未成交挂单全部成交后，可能新增的持仓名义
- `effective_order_position_notional`
  - `effective_position_notional + same_side_open_orders_exposure`

规则：

- 当 `effective_order_position_notional >= max_order_position_notional` 时
  - 禁止继续新增同方向挂单
  - 允许保留反向减仓单

服务停止后，不再额外为潜在新增持仓预留运行期保护。

### 3. `threshold_reduce_only`

该状态下不立即无脑强减，而先判断当前已累计的配对步长积分是否足够覆盖直接减仓代价。

#### 超额仓位

- `excess_notional = effective_position_notional - threshold_position_notional`

#### 直接减仓代价

把超额仓位按“最不利 lot 优先”映射到若干 lot，再对每个被选中的 lot 计算当前立刻贴盘口减掉它所需吞掉的步长：

- `long/spot`
  - 先减买入价最高的 lot
- `short`
  - 先减开空价最低的 lot

每个被选中 lot 的 `cost_steps` 为：

- `long/spot`
  - `max(0, (lot_entry_price - current_reduce_sell_price) / step_price)`
- `short`
  - `max(0, (current_reduce_buy_price - lot_entry_price) / step_price)`

所有 lot 的 `cost_steps` 之和即：

- `forced_reduce_cost_steps`

实现上应在交易所价格精度下进行量化，并向下取整为整数步长积分，避免高估已覆盖收益。

#### 配对积分判定

如果：

- `pair_credit_steps >= forced_reduce_cost_steps`

则允许生成 `forced_reduce` 订单，并从 `pair_credit_steps` 中扣掉实际消耗的积分。

否则：

- 不直接减仓
- 暂停新的同方向补仓单
- 保留普通反向减仓网格
- 等待进一步卸仓或新的配对积分累积

### 4. `hard_reduce_only`

该状态不再看积分是否足够。

规则：

- 立即移除所有同方向扩仓单
- 只保留普通反向减仓单与专门 `forced_reduce` 单
- `forced_reduce` 仍按“最不利 lot 优先”生成
- 直到持仓回到硬上限以下

### 5. 强制减仓订单模式

当前版本固定为 `maker-only`：

- 卖出减 `long/spot`：贴卖一
- 买回减 `short`：贴买一

当前版本不支持：

- 超时自动转 taker
- 直接 market reduce

## 配对积分账本

### 1. 积分来源

仅统计“已完成配对”的成交，不考虑手续费。

当某个开仓 lot 被后续反向成交部分或全部对掉时，生成 `matched_pair`，并累计：

- `matched_steps = abs(exit_price - entry_price) / step_price`

实现时同样应在价格精度下量化为整数步长，不得向上取整。

### 2. 积分生命周期

- 积分只保存在内存
- 服务重启后清零
- 不从交易所历史回补
- 发生直接减仓时，只扣除本次真实消耗的积分
- 未消耗部分继续保留

## 库存 lot 账本

### 1. `long/spot`

每个 lot 至少记录：

- `lot_id`
- `side = long`
- `qty`
- `entry_price`
- `opened_at`
- `source_role`

### 2. `short`

每个 lot 至少记录：

- `lot_id`
- `side = short`
- `qty`
- `entry_price`
- `opened_at`
- `source_role`

### 3. lot 消耗原则

- `grid_exit`
  - 正常按既定 lot 消耗逻辑配对，产出 `matched_pair`
- `forced_reduce`
  - 优先消耗“最不利 lot”
- `tail_cleanup`
  - 按剩余 lot 顺序全部消费完毕

## 重启恢复逻辑

### 1. 恢复原则

服务重启后，以交易所近期成交和当前持仓为准重建状态，本地状态只作为缓存，不作为权威来源。

### 2. 必要前提

要可靠重建，订单必须有稳定的策略标识，例如：

- `clientOrderId` 前缀
- 明确的 `role` / `strategy_instance_id`

没有可识别的策略成交，不允许推测市场整体成交来恢复本策略状态。

### 3. 重建内容

从交易所读取：

- 当前实际持仓
- 当前未成交挂单
- 近期本策略成交历史

重建：

- `direction_state`
- `position_lots`
- `grid_anchor_price`
- 当前活跃订单集合

不重建：

- `pair_credit_steps`

### 4. 重建失败时的安全行为

如果出现以下任一情况：

- 当前有持仓，但近期成交历史不足以重建 lot
- 同时检测到双向残余首单或方向不一致的库存
- 订单标签缺失，无法可靠识别本策略订单

则进入保守模式：

- 不新增同方向扩仓单
- 只允许减仓或尾仓清理
- 记录错误原因并要求人工处理或显式重置

## 异常与边界情况

### 1. 双向首单竞争

`futures flat` 时，买卖两边首单同时存在。若一边先成交，应立即撤销另一边。

如果在撤销确认前另一边也发生部分成交：

- 先以真实成交更新 `position_lots`
- 暂不继续扩新方向
- runner 进入保守减仓模式
- 优先把仓位整理回单方向或 `flat`
- 整理完成后才恢复正常逻辑

### 2. 部分成交

- 所有 lot、配对、积分、强减成本计算都必须支持部分数量
- 任何一次部分成交都要先更新账本，再生成下一轮计划

### 3. 剩余仓位不足最小下单单位

如果剩余仓位小于交易所最小可下单量：

- 标记为 `dust_position`
- 不再生成新的同方向挂单
- 保留在监控和日志中显式提示

### 4. 挂单与真实仓位短暂不一致

订单 diff 发生在离散轮次中，因此短时间内允许：

- 仓位已变化，但旧单尚未撤销
- 目标单已生成，但旧单尚未从交易所完全消失

在这种窗口里，所有新增同方向单都必须再次经过 `max_order_position_notional` 检查。

## 实现建议

### 1. 合约与现货的共享抽象

建议把以下能力抽成共享层：

- 成交分类与 `role` 识别
- `position_lots` 维护
- `pair_credit_steps` 维护
- `grid_anchor_price` 更新判定
- 风控状态机
- `forced_reduce_cost_steps` 计算

再由 `futures` / `spot` 各自负责：

- 订单方向约束
- 账户可用余额或保证金计算
- 交易所接口调用

### 2. 与现有 runner 的关系

该设计应尽量复用现有：

- [loop_runner.py](/Volumes/WORK/binance/wangge/src/grid_optimizer/loop_runner.py)
- [spot_loop_runner.py](/Volumes/WORK/binance/wangge/src/grid_optimizer/spot_loop_runner.py)
- [semi_auto_plan.py](/Volumes/WORK/binance/wangge/src/grid_optimizer/semi_auto_plan.py)

其中已有的库存限制、只减仓保护、订单 diff 逻辑可以继续使用，但网格中心不应再由“市场中价迁移”主导，而应让位于 `grid_anchor_price`。

## 测试计划

至少补以下测试：

- `futures flat` 双向首单，买侧先成交
- `futures flat` 双向首单，卖侧先成交
- `spot flat` 只挂买侧首单
- 首单成交后正确写入 `grid_anchor_price`
- 常规 `grid_entry/grid_exit` 成交后正确更新 `grid_anchor_price`
- `forced_reduce` 成交不更新锚点
- `tail_cleanup` 成交不更新锚点
- `threshold_reduce_only` 下积分不足时暂停同方向补仓
- `threshold_reduce_only` 下积分足够时允许直接减仓且正确扣减积分
- `hard_reduce_only` 无视积分直接进入强减
- `long/spot` 强减时优先处理最高成本 lot
- `short` 强减时优先处理最低开空价 lot
- 重启后用交易所成交重建 lot 与方向
- 重启后积分不恢复，默认为 `0`
- 重建失败时进入保守模式
- `futures` 双边首单同时部分成交时进入保守减仓模式

## 风险

- `maker-only` 强减在单边急速行情下可能长期不能成交，因此 `hard_reduce_only` 不能等价理解为“立即降仓成功”
- 配对积分不跨重启持久化，意味着重启后会暂时更保守
- 若策略标签不完整，交易所历史无法可靠重建 lot，会导致实例只能进入保守模式
- “只保最近一买一卖”的设计能减少撤单，但也会使远端旧单残留更久，必须依赖稳定的订单 diff 逻辑
