# 2026-03-30 `ecs-114` KAT / BARD 运行异常记录

## 结论

`43.155.163.114` 上 `KATUSDT` 和 `BARDUSDT` 昨晚的问题，不是代码版本和其他机器不一致，而是 **运行配置漂移** 和 **仓位/保证金风控触发** 叠加导致的。

更准确地说：

1. `114` 上实际运行的是一套较老的 `volume_long_v4` 配置，和 `111/150` 上后续验证过的配置不一致。
2. 两个币种昨晚都 **不是“只有首笔买入后完全没有成交”**。  
   审计日志显示二者后续都有多笔成交，只是很快进入了：
   - `KAT`：仓位达到停买阈值后只剩减仓单，成交密度迅速下降
   - `BARD`：仓位达到停买阈值后又连续触发 `-2019 Margin is insufficient`，runner 随后停止
3. `114` 的问题核心不在 Git 仓库代码，而在 `output/*_loop_runner_control.json`。  
   这类运行配置不在 Git 里，服务器同步代码时不会自动覆盖，所以三台机器即使代码一致，也可能跑出不同行为。

## 现场证据

服务器：

- `ecs-114`
- 路径：`/home/ubuntu/wangge`

涉及文件：

- `/home/ubuntu/wangge/output/katusdt_loop_runner_control.json`
- `/home/ubuntu/wangge/output/bardusdt_loop_runner_control.json`
- `/home/ubuntu/wangge/output/katusdt_loop_events.jsonl`
- `/home/ubuntu/wangge/output/bardusdt_loop_events.jsonl`
- `/home/ubuntu/wangge/output/katusdt_loop_trade_audit.jsonl`
- `/home/ubuntu/wangge/output/bardusdt_loop_trade_audit.jsonl`

### 1. `114` 上实际运行配置偏旧

昨晚 `KAT / BARD` 实际控制文件都是这组老配置：

- `strategy_profile = volume_long_v4`
- `strategy_mode = one_way_long`
- `autotune_symbol_enabled = true`
- `sleep_seconds = 15`
- `up_trigger_steps = 6`
- `down_trigger_steps = 4`
- `shift_steps = 4`
- `pause_buy_position_notional = 750`
- `max_position_notional = 900`
- `inventory_tier_start_notional = 600`
- `inventory_tier_end_notional = 750`

其中：

- `KAT step_price = 0.00002`
- `BARD step_price = 0.0002`

这说明 `114` 跑的不是后来在 `111/150` 上验证过的“更快跟价、更轻仓位”方案，而是更早的一版旧配置。

## KAT 问题

### 现象

- 首笔成交时间：`2026-03-30T22:46:33.952+08:00`
- 最后一笔成交时间：`2026-03-30T23:05:10.152+08:00`
- 总成交笔数：`14`
- 买/卖笔数：`BUY 10` / `SELL 4`
- 累计成交额：`1329.16595`
- 已实现盈亏：`+1.020806`

所以，**KAT 不是只有首笔买入成交**。  
日志显示它在 `22:46` 到 `23:05` 之间持续有成交，只是后半段明显变慢。

### 直接问题

#### 1. 仓位很快触发停买

第一次进入停买的时间：

- `2026-03-30T22:50:09.904932+08:00`

对应原因：

- `current_long_notional=767.6820 >= pause_buy_position_notional=750.0000`

这意味着：

- 启动后多仓很快累积到 `750U+`
- 策略随后停止继续挂新的买单
- 只剩上方的减仓卖单在等待成交

#### 2. 中心下移太慢

KAT 昨晚整段运行里，事件日志只记录到 **1 次** 中心迁移：

- 最后一次迁移：`2026-03-30T23:08:56.894057+08:00`
- 方向：`down`
- 新中心：`0.01137`

而在停买前后，日志长期是：

- `mid ≈ 0.011405 ~ 0.011375`
- `center = 0.01145`

这说明：

- 价格已经往下走
- 中心仍长时间挂在更高位置
- 上方减仓卖单离现价偏远
- 停买以后，成交自然迅速下降

#### 3. 后续又出现下单错误

`katusdt_loop_runner.log` 后段连续出现：

- `Binance API error -2019: Margin is insufficient.`

最终：

- `Stopped after 10 consecutive errors`

这会导致后续计划刷新和订单更新中断。

### 根因判断

KAT 的核心问题是三件事叠加：

1. `114` 仍在跑旧配置  
2. `6/4/4 + 15s` 的中心迁移节奏过慢  
3. `pause_buy_position_notional = 750` 触发太早，导致买盘很快停掉

结果就是：

- 启动后先有一波连续买入
- 仓位上来后立刻停买
- 卖单又挂得偏高
- 所以后面主观感觉像“只有第一笔成交，后面没动”

## BARD 问题

### 现象

- 首笔成交时间：`2026-03-30T23:05:04.829+08:00`
- 最后一笔成交时间：`2026-03-30T23:13:36.175+08:00`
- 总成交笔数：`14`
- 买/卖笔数：`BUY 11` / `SELL 3`
- 累计成交额：`1188.0606`
- 已实现盈亏：`-0.320638`

所以，**BARD 也不是只有首笔买入成交**。  
它在 `23:05` 到 `23:13` 之间持续有后续成交。

### 直接问题

#### 1. 仓位在短时间内触发停买

第一次进入停买的时间：

- `2026-03-30T23:13:36.322427+08:00`

对应原因：

- `current_long_notional=768.2322 >= pause_buy_position_notional=750.0000`

和 KAT 一样：

- 仓位一旦到 `750U+`
- 新买单就停掉
- 只剩减仓卖单

#### 2. 保证金不足导致 runner 停机

第一次明确报错时间：

- `2026-03-30T23:07:10.200363+08:00`

主要错误：

- `Binance API error -2019: Margin is insufficient.`

另有一次：

- `Binance API error -5022: Post Only order will be rejected`

后续连续报错，最终：

- `Stopped after 10 consecutive errors`

这说明 BARD 的问题比 KAT 更硬：

- 不只是成交变少
- 而是 runner 在继续补单/换单时直接因为保证金不足被打停

#### 3. 中心没有及时下移

`bardusdt_loop_events.jsonl` 这段运行里：

- `shift_event_count = 0`

也就是：

- 整段运行没有记录到中心迁移

而最新计划里仍然能看到：

- `mid_price = 0.35725`
- `center_price = 0.3578`

说明：

- 价格已经往下走
- 中心仍然偏高
- 对做多网格来说，跟随明显偏慢

### 根因判断

BARD 的问题比 KAT 更明显：

1. 运行配置旧，跟价节奏慢  
2. 仓位很快超过 `pause_buy_position_notional = 750`
3. 在已有仓位和挂单的情况下，账户剩余保证金不足
4. `-2019` 连续触发后，runner 直接停止

所以主观上会看到：

- 首笔买入后短时间还有一些零散成交
- 然后策略很快就像“卡死”或“完全不动”

本质上是：

- 停买 + 保证金不足 + runner 退出

## 为什么三台机器代码一致，行为却不一样

因为真正决定实盘行为的不只是代码，还有：

- `output/katusdt_loop_runner_control.json`
- `output/bardusdt_loop_runner_control.json`

这些运行配置文件：

- 不在 Git 仓库里
- 代码同步不会自动覆盖
- 每台服务器可能长期漂移

这也是 `114` 昨晚问题和 `111/150` 不一致的根本原因。

## 解决方案

### 方案 1：统一运行配置来源

不要再让三台机器各自保留独立、长期漂移的 `output/*_loop_runner_control.json`。

建议：

1. 明确每个币种的基线参数
2. 由同一份配置模板生成各机器控制文件
3. 每次调整参数时，记录到仓库文档或配置模板，而不是只改线上 JSON

如果不做这一步，后面还会继续出现：

- 代码一样
- 机器行为不一样

### 方案 2：KAT 改成更快跟价、更晚停买

KAT 这条策略的直接问题是：

- 停买阈值过早触发
- 中心下移太慢

建议方向：

1. `sleep_seconds` 从 `15` 降到 `5`
2. `autotune_symbol_enabled` 关掉，避免手工参数被重写
3. `up/down/shift` 调快，例如从 `6/4/4` 收紧到 `2/2/2` 或对齐已经验证过的版本
4. 重新评估 `pause_buy_position_notional`
   - 当前 `750` 对这套仓位扩张速度来说过低
   - 容易刚起量就停买
5. 若目标是先控回撤，再提高成交密度，可以启用更强的库存减仓模式

### 方案 3：BARD 改成轻仓快跟随版本

BARD 当前这套旧参数不适合继续跑。

建议方向：

1. 使用已经在别的机器上验证过的更轻仓参数
2. `autotune_symbol_enabled = false`
3. 缩短 `sleep_seconds`
4. 加快中心迁移触发
5. 降低初始库存和库存分层强度
6. 控制 `pause_buy_position_notional / max_position_notional`
   - 避免短时间冲到 `750U+`
7. 在保证金不足频繁发生的机器上，优先降低单笔名义金额和总库存目标

### 方案 4：把“保证金不足”当成硬故障处理

`-2019 Margin is insufficient` 不应该只当作普通日志。

建议：

1. 监控页明确显示该错误
2. 连续出现时自动停止策略，并提示“当前是保证金不足，不是行情没成交”
3. 文档化要求：
   - 启动前检查当前可用保证金
   - 不满足最低余量就不允许启动

## 最终判断

昨晚 `114` 上这两条策略的问题，可以概括成：

- `KAT`：不是没成交，而是旧配置下先快速建仓，随后过早停买，中心下移偏慢，后续成交密度明显下降
- `BARD`：不是没成交，而是旧配置下先成交几笔，然后仓位过快上升并触发保证金不足，runner 最终被错误中断

所以真正要修的不是某一行代码，而是：

1. 统一服务器运行配置
2. 把 `114` 的 `KAT/BARD` 从旧参数切到经过验证的新参数
3. 把保证金不足纳入启动前检查和运行中告警
