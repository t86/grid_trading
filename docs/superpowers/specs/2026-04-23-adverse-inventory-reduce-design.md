# 持仓逆向偏离主动减仓设计

## 背景

SOONUSDT 的 `synthetic_neutral` 刷量策略在空仓超过软阈值后仍需要保留一定短侧 probe，否则可能长时间只挂远端减仓单，成交量中断。但当前保护主要依赖盈利买回和少量 flow sleeve，遇到单边上涨时，空仓会持续留在亏损区，等待不亏损减仓的机会不现实。

目标是保留软阈值后的流动性，同时在持仓相对成本出现足够大的逆向偏离时，主动把库存降回安全目标，避免单边无限深套。

## 目标

- 超过软阈值后继续允许小比例同向 probe，保持刷量和挂单流动性。
- 净空时，价格相对空仓成本上涨超过阈值后主动 reduce-only 买回。
- 净多时，价格相对多仓成本下跌超过阈值后主动 reduce-only 卖出。
- 主动减仓不是清仓，而是把库存压回软阈值的一定比例。
- 先用 maker 减仓，超时后升级为 aggressive 减仓。
- 所有主动减仓订单继续走现有 submit 保护、订单 diff 和 reduce-only 安全路径。

## 非目标

- 不改变软阈值后 probe 的基本存在理由。
- 不把普通 take-profit 平仓改成亏损平仓。
- 不改交易所持仓模式、杠杆、保证金模式。
- 不在本设计里做独立趋势识别或行情预测。

## 新参数

- `adverse_reduce_enabled`
  - 默认 `false`。
  - 开启后才评估逆向偏离主动减仓。
- `adverse_reduce_short_trigger_ratio`
  - 净空触发阈值，建议 SOON 初始 `0.008` 到 `0.012`。
  - 触发公式：`(mid_price - short_cost_price) / short_cost_price >= ratio`。
- `adverse_reduce_long_trigger_ratio`
  - 净多触发阈值。
  - 触发公式：`(long_cost_price - mid_price) / long_cost_price >= ratio`。
- `adverse_reduce_target_ratio`
  - 主动减仓目标，默认建议 `0.75`。
  - 净空目标名义：`pause_short_position_notional * target_ratio`。
  - 净多目标名义：`pause_buy_position_notional * target_ratio`。
- `adverse_reduce_maker_timeout_seconds`
  - maker 减仓等待时间，建议 `30` 到 `60`。
  - 超时后同一方向减仓订单升级为 aggressive。
- `adverse_reduce_max_order_notional`
  - 单轮主动减仓最大名义。
  - 防止一次计划生成过大的主动减仓单。
- `adverse_reduce_keep_probe_scale`
  - 触发主动减仓时仍允许保留的同向 probe 比例。
  - 默认使用当前 `inventory_pause_short_probe_scale` 的语义；显式设置时覆盖。

## 成本基准

成本基准按优先级读取：

1. 交易所持仓 `breakEvenPrice`，如果可用且大于 0。
2. 交易所持仓 `entryPrice`，如果可用且大于 0。
3. synthetic ledger 的 `virtual_short_avg_price` / `virtual_long_avg_price`。
4. 如果仍取不到成本，主动减仓保护不触发，并在 plan report 里记录 `cost_basis_missing`。

净空和净多分别独立判断。当前 SOON 案例使用净空路径。

## 触发逻辑

净空触发必须同时满足：

- `adverse_reduce_enabled=true`。
- `current_short_qty > 0`。
- `pause_short_position_notional > 0`。
- `current_short_notional > pause_short_position_notional`。
- `short_cost_price > 0`。
- `(mid_price - short_cost_price) / short_cost_price >= adverse_reduce_short_trigger_ratio`。

净多触发做对称判断：

- `current_long_qty > 0`。
- `current_long_notional > pause_buy_position_notional`。
- `(long_cost_price - mid_price) / long_cost_price >= adverse_reduce_long_trigger_ratio`。

触发状态按方向写入 runtime state，记录 `entered_at`、`last_checked_at`、`direction`、`cost_price` 和 `adverse_ratio`。当库存回到目标以下，或偏离回落到阈值以下，清除该方向状态。

## 减仓订单

触发后生成 `forced_reduce` 订单：

- 净空：生成 `BUY reduce-only`。
- 净多：生成 `SELL reduce-only`。
- 初始阶段 `execution_type=post_only`，`time_in_force=GTX`。
- 超过 `adverse_reduce_maker_timeout_seconds` 后，`execution_type=aggressive`，`time_in_force=GTC`。
- 减仓名义为：
  - `current_notional - pause_notional * adverse_reduce_target_ratio`
  - 再被 `adverse_reduce_max_order_notional` 裁剪。
- 减仓数量不得超过当前持仓数量。
- 订单价格使用现有 `_build_threshold_target_reduce_order` 的价格/数量舍入和最小名义检查。

## Probe 保留

主动减仓触发后，不应完全关闭刷量：

- 先沿用软阈值后的 probe 机制，保留小比例同向 entry。
- 如果配置了 `adverse_reduce_keep_probe_scale`，触发主动减仓时用该比例覆盖软阈值 probe。
- 如果 active reduce 订单和同向 probe 会同时存在，reduce-only 订单优先出现在目标订单列表前部，保证 diff 和提交时优先处理减仓。

## 报告字段

plan report 增加 `adverse_reduce`：

- `enabled`
- `active`
- `direction`
- `cost_basis_source`
- `cost_price`
- `adverse_ratio`
- `trigger_ratio`
- `target_notional`
- `reduce_notional`
- `maker_timeout_seconds`
- `elapsed_seconds`
- `aggressive`
- `placed_reduce_orders`
- `blocked_reason`

summary jsonl 增加必要摘要字段，便于监控台展示和后续回放。

## 测试

新增单元测试覆盖：

- 净空超过软阈值但未达到逆向偏离，不触发主动减仓。
- 净空达到逆向偏离，生成 `BUY forced_reduce reduce-only`。
- 净空触发后单轮减仓名义受 `adverse_reduce_max_order_notional` 限制。
- maker 超时后切换 aggressive。
- 成本缺失时不触发，并返回明确 `blocked_reason`。
- 净多路径对称生成 `SELL forced_reduce reduce-only`。

## SOON 初始建议

线上先用保守配置验证：

- `adverse_reduce_enabled=true`
- `adverse_reduce_short_trigger_ratio=0.01`
- `adverse_reduce_long_trigger_ratio=0.01`
- `adverse_reduce_target_ratio=0.75`
- `adverse_reduce_maker_timeout_seconds=45`
- `adverse_reduce_max_order_notional=120`
- `inventory_pause_short_probe_scale` 保持 `0.15` 到 `0.25`

这样软阈值后仍然刷量，但当 SOON 净空相对成本上涨约 1% 且仓位仍在软阈值以上时，每轮最多主动减约一单名义，直到库存回到目标附近。
