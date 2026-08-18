# GRVT 盈利释放与单动作恢复整合设计

状态：待用户书面复核。

## 背景

`codex/grvt-entry-lot-profit-guard` 已实现普通仓四腿刷量、按入场成本桶盈利退出和软阈值压力减仓，但当前实现仍有四个会阻止其成为合约主力策略的问题：

1. 普通减仓成交先按角色和价格顺序消费任意 entry lot，没有严格绑定签发租约的成本桶。
2. 租约部分成交后终结会锁定 cutoff 前整个同价桶，未成交数量也永久失去释放资格。
3. 订单 POST 成功后才持久化租约元数据，进程在该窗口退出时可能重复签发同一成本桶退出。
4. 软阈值恢复存在等号死区，并可能与轻侧普通 entry 在同一 symbol、同一周期同时执行。

本设计只修复上述状态与所有权问题，不改变每日成交目标、普通仓软硬上限、冻结仓参数、损耗上限、GTX maker-only 约束或真实灰度流程。

## 不变量

- 每个 symbol 每周期只选择一个逻辑动作；一个动作可以包含实现该动作必需的多笔 GTX 委托。
- 正常四腿刷量属于一个 `NORMAL_BRUSH` 动作。
- 软阈值压力减仓属于一个 `ORDINARY_PRESSURE_RECOVERY` 动作；该动作执行时，本周期所有普通 entry 和普通盈利退出均延后。
- 冻结仓位、冻结订单、冻结成本和冻结释放继续完全独立，不进入普通仓成本桶、软硬阈值或恢复预算。
- 所有新增普通委托继续使用 `LIMIT + GTX + post-only`；普通减仓继续使用正确 Hedge Mode side/positionSide 和 reduce-only 语义。
- `volatility_entry_pause` 默认保持开启；临时 `allow_loss` 只能由现有持久恢复租约授权，并在恢复完成或条件消失时自动回收。
- 未知、缺失或损坏的成本桶/租约状态失败关闭普通盈利减仓，不阻止另一侧合法普通开仓，也不新增第二恢复所有者。

## 领域模型

新增一个聚焦的普通盈利释放模块，统一拥有成本桶租约判断。建议文件为：

`src/grid_optimizer/ordinary_profit_release.py`

模块只处理纯领域状态，不负责请求交易所。核心概念如下：

### 成本桶键

成本桶键为：

`(position_side, normalized_entry_price)`

租约同时保存 `entry_cutoff_at`，因此可释放范围是该成本桶中 `opened_at <= cutoff` 的普通 entry lot。

### 租约状态

租约使用以下持久状态：

- `PENDING_SUBMIT`：已原子预留成本桶数量和 clientOrderId，尚未证明交易所接受。
- `OPEN`：交易所已接受，订单仍可能成交。
- `UNKNOWN`：请求结果不确定；必须先按 clientOrderId/orderId 对账，不得生成新租约。
- `TERMINAL`：订单已完全成交、撤销或确定拒绝，租约不再占用提交权。

每条租约至少保存：

- `lease_id`
- `client_order_id`
- `position_side`
- `bucket_price`
- `entry_cutoff_at`
- `authorized_qty`
- `filled_qty`
- `status`
- `order_id`（收到回执后）

## 数量与水位规则

### 预留

签发前从指定成本桶计算：

`available_qty = eligible_entry_qty - active_reserved_qty - settled_filled_qty`

新租约授权量不得超过 `available_qty`、交易所可减仓量和订单自身数量上限。

### 成交

成交只能消费与租约 `position_side + bucket_price + cutoff` 完全匹配的 entry lot。禁止用其他价格桶或 cutoff 之后的新入场补足。

每个成交事件按交易所唯一 trade key 幂等处理。`filled_qty` 只增加本次尚未处理的成交量，且不得超过 `authorized_qty`。

### 终结

- 完全成交：仅实际 `filled_qty` 成为已释放水位。
- 部分成交后撤单：仅实际成交量成为已释放水位；`authorized_qty - filled_qty` 立即解除预留，重新可用于后续租约。
- 零成交撤单或确定拒绝：全部预留解除。
- UNKNOWN：继续占用预留，直到交易所对账能够证明 OPEN 或 TERMINAL。

不得再使用“锁定 cutoff 前整个成本桶”表达实际释放水位。

## 崩溃安全提交协议

普通盈利退出下单按以下顺序执行：

1. 生成稳定的 `lease_id` 和 `client_order_id`。
2. 在 runner state 的原子写边界持久化 `PENDING_SUBMIT` 租约及数量预留。
3. 调用交易所 POST。
4. 成功回执：持久化 `OPEN + order_id`。
5. 确定拒绝：持久化 `TERMINAL` 并释放全部预留。
6. 超时、断网或回执不确定：持久化 `UNKNOWN`；重启后先按 clientOrderId 查询交易所，再决定 OPEN/TERMINAL。

进程可能在任意一步退出。恢复后不得出现“交易所有订单但本地没有租约”的窗口，也不得通过新 clientOrderId 绕过原租约。

## 软阈值恢复与单动作协调

压力侧判定统一使用：

`ordinary_side_notional >= ordinary_side_soft_notional`

只要求压力侧自身仓位和最小可执行数量满足条件，不要求另一侧存在仓位。

当任一侧达到软阈值且没有足够的合格盈利退出时，planner 产生一个 `ORDINARY_PRESSURE_RECOVERY` 候选，并携带：

- 压力侧
- 当前普通名义值
- 软阈值
- 本租约剩余亏损减仓预算
- 冷却状态
- 允许的 reduce-only GTX 请求

该候选必须进入现有 symbol execution coordinator，由 coordinator 按既定优先级选择。选中压力恢复后，本周期普通四腿 place/cancel 全部记录为 deferred，不与轻侧 entry 合并。

恢复回到软阈值以下或租约预算耗尽时，持久状态明确转为完成；临时 `allow_loss` 自动回收，下一周期才允许恢复 `NORMAL_BRUSH`。

压力恢复不得创建新的控制所有者；它复用当前分支的普通恢复 ownership、decision id、effect epoch 和 receipt fence。

## 失败与恢复

- 成本桶状态损坏：阻止该侧新的盈利退出，记录明确原因；不猜测成本。
- 租约状态损坏：保持 fail-closed，禁止重复签发；由持久状态修复路径恢复。
- POST 后状态写失败：保留原 `PENDING_SUBMIT/UNKNOWN` fence，并在重启后对账。
- 交易所查询失败：维持 UNKNOWN，不生成替代租约。
- 软阈值等号：必须进入压力恢复判定，不允许停在“盈利退出已禁用、压力恢复未触发”的空档。
- 不满足最小名义值：返回确定的 `condition_unmet` 原因，并等待仓位/价格变化；不得反复重启。

## 测试

所有修复先写失败测试并确认 RED，至少覆盖：

1. 两个不同价格桶存在时，租约成交只消费指定价格桶。
2. 同价桶中 cutoff 之后的新 entry 不被旧租约消费。
3. 授权 2、成交 0.5 后撤单，仅 0.5 推进水位，剩余 1.5 可再次签发。
4. 零成交撤单释放全部预留。
5. POST 前已持久化 PENDING；模拟 POST 后、回执持久化前崩溃，重启不得签发第二租约。
6. UNKNOWN 按 clientOrderId 对账后分别转 OPEN 或 TERMINAL。
7. 普通仓名义值恰好等于软阈值时触发压力恢复。
8. 对侧为零或低于最小名义值时，压力侧仍可独立恢复。
9. 压力恢复执行周期不保留轻侧普通 entry 或其他普通刷量委托。
10. 回到软阈值以下后回收临时 allow_loss，下一周期恢复普通四腿。
11. 冻结仓位和冻结订单不参与上述计算。
12. 所有提交订单继续满足 LIMIT、GTX、post-only 和正确 reduce-only 语义。

## 合并与验收

实现先提交到 `codex/grvt-entry-lot-profit-guard`，完成该分支聚焦测试和完整相关回归，再模拟合入 `codex/arx-single-recovery-owner`。

合并快照必须通过：

- GRVT planner/runner/submit tests
- futures recovery coordinator/store/runtime tests
- runtime guard 与 frozen inventory isolation tests
- 当前分支已确认的 symbol 单动作回归

已存在于当前分支的失败测试不能算作本次新增回归，但其中与 GRVT 自主恢复直接相关的失败必须在推广为主力策略前清零或以新规范明确替换。

代码合并与实机推广分开：本轮只提交分支，不部署。114 至少完成一个完整目标窗口、满足既有速度/损耗/退出门禁后，才允许推广到其他服务器。

## 回滚

- 代码回滚：逆序 revert 本设计对应的修复提交和后续合并提交。
- 运行回滚：关闭四腿 feature flag 并重启 runner，恢复旧普通刷量路径；不人工平仓、不人工撤单。
- 持久租约回滚时不得直接删除 UNKNOWN/OPEN 租约；必须先与交易所订单对账，避免回滚后重复释放。
