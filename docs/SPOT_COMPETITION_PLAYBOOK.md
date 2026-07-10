# 现货赛策略作战手册

适用范围：114/150 的现货刷量、maker 网格、maker 买入后 taker 卖出、纯 TAKE 补量、底舱回归和赛末清仓。

本手册只沉淀稳定的方法。每次操作前仍必须以交易所、进程和当前 control/state 的实时状态为准。

## 1. 先固定统计口径

- 目标、是否继续、是否停止、最终成交量：只用交易所现货 `userTrades`。
- 统计全窗口时按 Binance trade `id` 去重；单页 1000 笔不是全量，必须分页或按时间区间拆分。
- 当天口径从本地时区 00:00 开始。runner 自身累计、事件文件和订单回执只能用于实时辅助，不能替代全量总量。
- 被中断的 SSH/终端会话不代表远端订单没有成交。任何中断后先核对：远端进程、`openOrders`、交易所 `userTrades`。
- 现货 APP 损耗口径：`买入总额 - 卖出总额 - 当前持仓数量 * 最新价`。手续费单列，不和 APP 损耗混算。

## 2. 每次开赛前的预检

按每个主机、每个交易对单独完成：

1. 确认运行目录、wrapper、现有 runner 进程和 control/state 文件。
2. 拉交易所现货余额、现货 `openOrders`、当前盘口和 symbol step/min-notional。
3. 读取 exchange `userTrades` 的当日总量与 5/15/60 分钟量。
4. 记录底舱 `neutral_base_qty`、实际 base、库存偏离、双边实际挂单数。
5. 明确目标是“当天交易所总量”还是“runner 生命周期累计”。两者不能混用。

目标为当日成交 `target_today` 时，runner 的累计上限应按下式换算，而不是直接填 `target_today`：

`max_cumulative_notional = runner_current_gross + max(target_today - exchange_today_gross, 0)`

## 3. 双边挂单优先于单边回归

正常刷量区间应尽量保留近盘口 BUY 与 SELL。只有底舱偏离进入约定的恢复带后，才允许单边拉回。

### 必须对齐的阈值

- `neutral_base_qty`：底舱锚点。
- `threshold_reduce_target_notional`：允许的底舱短侧/卖单地板缓冲。过小会在很小偏离时过滤 SELL，导致只剩 BUY。
- `threshold_position_notional`：库存状态机阈值，应不小于上述回归缓冲。
- `spot_base_rebalance_soft_tolerance_qty` / `spot_base_rebalance_hard_tolerance_qty`：软/硬回归带，应与上述名义阈值的数量级一致。
- `max_order_position_notional` / `max_position_notional`：仍是最终风险上限，不因提速而取消。

### 最小名义值陷阱

双边策略的可卖额度即使为正，也可能低于交易所 `min_notional`，SELL 会被过滤而形成单边。调整时先计算：

`可卖数量 * 当前价格 >= min_notional`

若不成立，应放宽回归缓冲或减少每次可触发的回归幅度，而不是误判为市场没有成交。

### 当前 GRAM 基线

- 底舱：`100 GRAM`。
- 双边优先回归带：`threshold_position_notional=30U`、`threshold_reduce_target_notional=25U`、软/硬底舱偏离 `15 / 30 GRAM`。
- 这些值的目的不是扩大风险上限，而是避免约 5 GRAM 的小偏离触发单边恢复。

## 4. 提速顺序

先确保双边稳定，再提高效率。每次只改一档，并以连续 10-15 分钟的交易所成交验证。

1. 增加近盘口双边层数，例如 `2/2 -> 3/3`。
2. 适度提高 `per_order_notional` 与 attack/defense 单笔名义，但不得越过 `max_order_position_notional`。
3. 检查 `max_single_cycle_new_orders`、总 quote budget 和盘口深度是否成为瓶颈。
4. 若仍不够，再缩短轮询间隔；先确认 API 请求余量，不能靠高频 REST 轮询换量。

每次提速后必须同时看：

- 5/15/60 分钟 exchange 成交量与达到目标所需速率。
- 实际 BUY/SELL 挂单数量、库存偏离、是否有单边过滤。
- 订单是否靠近盘口、是否频繁 post-only reject、是否出现 API cooldown。

不要直接放大单笔来追目标；如果双边已消失，单笔放大通常只会更快触发库存保护。

## 5. Maker 买入后 TAKE 卖出

适合短时测试，必须满足：

- Maker BUY 在最佳买价时保留排队位置；只有不再贴买一时才撤单重挂。
- 有任何已成交数量即立即按实际成交数量 MARKET SELL，不等整单成交。
- 若买单成交后，原买入价仍是可成交的买一，且该价位可见买盘足够覆盖实际成交数量，则优先用原买入价的 `LIMIT + IOC` TAKE 卖出该数量，同时继续在原买价补回 maker BUY 排队。这样可缩短库存占用和提高周转；IOC 未成交时不得降价追卖。
- `spot_competition_synthetic_neutral_grid` 已内置该路径：仅消费本 runner 已登记的 maker BUY 成交；要求最新买一不低于成交价、买一可见数量覆盖待卖数量并满足交易所最小值。每个成交 ID 只提交一次，冻结 maker 成交不会进入该路径。
- 结束、终端中断或手工停止时：撤掉未成交 BUY，再卖出残余 base。
- 仅用该循环的本地订单回执统计循环进度；赛总量判断仍回到全窗口 `userTrades`。

禁止把“本地脚本被终止”当作“远端循环和挂单已停止”。必须查进程和交易所订单。

### 磨损预期

盘口足够厚只会降低 TAKE 卖出的冲击成本，不能保证零磨损。Maker 买单成交本身常带有逆向选择：价格下行时更容易成交，随后市价卖出可能落在更低的买盘；此外还有买卖手续费、盘口跳动、部分成交后的残余仓位和撤单重挂风险。

- 原价 TAKE 的价差磨损可以接近零，但 taker 手续费仍会增加；目标应是“在预设万 U 损耗预算内稳定刷量”，而不是“盘口厚就可近似零损耗”。
- 仅在买入成交后，盘口深度足以覆盖该实际成交数量、买一价格仍在可接受卖出价范围内，才立即 TAKE 卖出。
- 盘口变薄、买一跳空或可实现损耗超过阈值时，暂停 TAKE，保留已成交数量等待更好的退出盘口或按底舱规则处理。
- 复盘时分开记录：现货交易对损耗、手续费，以及 maker 买入到 taker 卖出的单轮价差；不能只看成交量。

## 6. 纯 TAKE 补量

适合赛末补目标，不用于替代 maker 策略。

- 走现货 `MARKET`，BUY 后立即 SELL 同一 `executedQty`。
- 数量用 `Decimal` 按 `step_size` 向下量化。
- 单腿名义不超过盘口可见深度的保守比例；价差超过预设上限则等待。
- 循环内进度使用每笔 MARKET 回执 `cummulativeQuoteQty`，避免每轮拉 account/myTrades 导致 `-1003`。
- 最终一次性核对：`userTrades`、现货余额、`openOrders=0`、同名合约仓位。
- 若需回到底舱，先预留回归成交额；交易所最小名义值导致的不可避免超量必须先说明。

## 7. API 冷却与中断处理

- 出现 `-1003`、429 或本地 cooldown：立即降低读取频率；不要继续叠加全窗口查询。
- 量化循环和审计分离：运行中只读最少状态，完整成交统计放在停机或检查窗口。
- 远端命令使用 stdin heredoc，避免本地 shell 改写 Python 或正则内容。
- 对当前运行中的策略，优先读取本地 state/event 做诊断；决定目标是否达到时再做交易所全量审计。

## 8. 现货与合约边界

- 普通底舱回归、maker 刷量、TAKE 补量都只用现货。
- 只有用户明确要求且属于冻结账本/对冲操作时，才联动同名合约。
- 清仓指令必须明确是否包括现货、合约、挂单和后台 runner；默认逐台核验，不把一台结果外推到另一台。

## 9. 停止与复盘清单

停止或达标后，逐台完成：

1. wrapper/service inactive，且没有裸 `spot_loop_runner` / `loop_runner` 进程。
2. 交易所现货 `openOrders=0`；如要求清仓，base free/locked 均为零。
3. 如要求清合约，position risk 与 futures open orders 均为空。
4. 用全窗口 `userTrades` 产出买入、卖出、gross、笔数、手续费资产和损耗。
5. 记录目标、实际总量、超量/欠量原因、参数改动、底舱偏离、API 错误和是否存在中断。

## 10. 赛前/赛中记录模板

```text
symbol / host / mode:
today exchange gross / target / remaining:
5m / 15m / 60m gross:
neutral base / actual base / deviation:
actual BUY orders / SELL orders:
effective per-order / levels / step / max position:
active protection or one-sided reason:
change made / exact before-after values:
verification window / exchange trade ids deduped:
```
