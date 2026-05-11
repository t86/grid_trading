# Regime Entry Budget v2 设计

## 目标

为竞赛型中性刷量策略增加一个 regime 级别的 entry 风险预算控制层。它的目标是在快速行情里降低损耗放大，同时在只是波动变宽、但仍然可交易的行情里保留正常双边刷量能力。

这次要解决的问题不只是“波动大时 step 需要变宽”。真正的故障链路是：

- regime 已经变化，但旧 entry 挂单仍然留在交易所；
- 快速涨跌在撤单确认前扫掉旧挂单；
- 库存变成单边；
- 后续 close / reduce 订单兑现亏损；
- 小成交额窗口放大 `loss_per_10k`，真实 rolling loss 也可能继续打穿 guard。

`regime_entry_budget_v2` 要把“场上最大可被扫的 entry 暴露”变成显式预算，并且把状态切换建模为“撤单确认后再切换”，而不是只重新计算一组参数。

## 非目标

- 不替代 `competition_elastic_volume_v1` 或 `adaptive_step`。
- 不把 `wide-step` 定义成只能防守的状态。
- 不把撤单速度当成唯一保护。
- 不允许 step 变小、单笔变小后，总侧向暴露悄悄变大。
- 不把 reduce-only / risk-reduce 变成无限制止损。

## 状态模型

控制器使用五个高层状态：

- `ping-pong-fast`：低波动、低库存、低损耗，贴近盘口高频刷量。
- `ping-pong-safe`：常规刷量态，仍然积极，但比 fast 更保守。
- `wide-step`：高波动但仍可交易的刷量态。step 变宽，在 entry 预算允许时仍支持双边挂单。
- `shock-guard`：切换过渡和急速波动保护态。先撤 live entry，并在状态与交易所 open orders 一致前禁止新增 entry。
- `defensive`：损耗、库存、reconcile 或极端波动恶化后的防守态。禁止新增 entry，只允许 exit、repair 和 risk-reduce。

关键定义：`wide-step` 不等于 `defensive`。`wide-step` 是受预算约束的宽步长刷量态；`defensive` 才是禁止新增 entry 的状态。

## 基础预算参数

第一版使用简单基础参数：

- `base_per_order_notional = 60`
- `base_step_ratio = 0.0025`，即 `0.25%`

默认 regime 参数：

| Regime | Step 倍数 | Step 比例 | 单笔倍数 | 单笔 | 单侧预算单位 | 单侧预算 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `ping-pong-fast` | `0.8` | `0.20%` | `0.8` | `48` | `10` | `480` |
| `ping-pong-safe` | `1.2` | `0.30%` | `1.2` | `72` | `15` | `1080` |
| `wide-step` | `2.0` | `0.50%` | `2.0` | `120` | `20` | `2400` |
| `defensive` | n/a | n/a | n/a | n/a | `0` | `0` |

这些值是策略默认值。saved runner 可以按机器、币种、账户规模或活动目标继续缩放。

对 150 这类小预算 runner，不建议机械复制 114 的比例，而应该使用更低的 `base_per_order_notional`。小预算账户的成交额分母更小、loss guard 更低，即使 `per_order / threshold` 比例一样，也更容易出现更差的 `loss_per_10k`。

## Entry 预算公式

每一侧的 entry capacity 由当前库存和场上同向 entry 共同扣减：

```text
long_entry_capacity =
  floor((long_side_budget - current_long_notional - open_entry_long_notional) / effective_per_order)

short_entry_capacity =
  floor((short_side_budget - current_short_notional - open_entry_short_notional) / effective_per_order)
```

然后做边界限制：

```text
long_entry_capacity = clamp(long_entry_capacity, 0, regime_max_entry_orders)
short_entry_capacity = clamp(short_entry_capacity, 0, regime_max_entry_orders)
```

这意味着 `wide-step` 下同向 entry 不会被天然禁止。只要这一侧预算还有余量，同向挂单仍然允许存在。

例子：

```text
regime = ping-pong-fast
effective_per_order = 48
long_side_budget = 480
current_long_notional = 96
open_entry_long_notional = 0

long_entry_capacity = floor((480 - 96) / 48) = 8
```

风险不变量：

```text
same_side_inventory + same_side_open_entry_notional <= regime_side_budget
```

## 状态切换规则

任何会改变 entry 价格、entry 预算或 entry 侧向权限的 regime 变化，都必须先进入 switching 阶段。

```text
if candidate_regime != current_regime:
    switching = true
    target_regime = candidate_regime
    allow_entry_long = false
    allow_entry_short = false
    cancel_all_non_reduce_entry_orders = true
```

只有满足以下条件后才允许提交切换：

```text
open_non_reduce_entry_orders == 0
reconcile_ok_count >= 2
cancel_pending == false
```

随后：

```text
current_regime = target_regime
switching = false
allow_entry_* = derived from target regime budget
```

活跃状态之间的切换也必须走这个流程：

```text
ping-pong-fast -> shock-guard/switching -> wide-step
wide-step -> shock-guard/switching -> ping-pong-safe
```

这样可以避免 plan 已经切到 `wide-step`，但交易所上旧的紧 step `ping-pong` entry 仍然活着。

不要要求所有 open orders 都为 0。清理条件只针对：

```text
open_non_reduce_entry_orders == 0
```

reduce-only、take-profit、risk-reduce 订单如果是库存修复的一部分，可以继续保留。

## Shock Guard

`shock-guard` 是专门处理“行情快到可能跑赢撤单/换单 loop”的快速保护态。

任一条件满足即可触发：

- `30s_abs_return_ratio >= 0.016`
- `30s_amplitude_ratio >= 0.024`
- `1m_abs_return_ratio >= 0.025`
- `1m_amplitude_ratio >= 0.030`
- `open_orders diff` 连续至少 2 次 reconcile 检查存在
- cancel 请求 pending 超过配置超时
- post-only reject 在短窗口内异常增加

行为：

```text
allow_entry_long = false
allow_entry_short = false
cancel_all_non_reduce_entry_orders = true
preserve reduce-only / take-profit / risk-reduce orders
```

退出条件：

```text
open_non_reduce_entry_orders == 0
reconcile_ok_count >= 2 or 3
30s/1m shock metrics are below threshold
rolling loss is below defensive threshold
```

退出 `shock-guard` 后重新判断 candidate。它可能进入 `wide-step`、`ping-pong-safe` 或 `defensive`。

## Defensive 状态

以下任一高风险条件满足时进入 `defensive`：

- rolling loss 达到 hourly limit 的 `60%` 到 `80%`；
- 单侧库存使用率达到 regime side budget 或配置 threshold 的 `80%` 以上；
- `shock-guard` 超时仍无法清理 entry；
- `loss_per_10k_15m` 超过 defensive 阈值，且成交额分母不是微小值；
- entry 已撤后，行情仍然继续单边极端移动；
- reconcile 长时间不一致。

行为：

```text
entry_budget = 0
allow_entry_long = false
allow_entry_short = false
cancel_all_non_reduce_entry_orders = true
allow take_profit / inventory_repair / risk_reduce
```

普通高波动不应该直接进入 `defensive`。普通高波动由 `wide-step` 处理。

## 状态内连续缩放

Regime 决定基础预算。在同一个 regime 内，再叠加连续的波动缩放。

先计算归一化波动分数：

```text
vol_score = max(
  amp_30s / threshold_30s,
  amp_1m / threshold_1m,
  amp_5m / threshold_5m
)
```

第一版建议缩放：

| 波动分数 | Step 微调倍数 | 单笔微调倍数 | 预算行为 |
| ---: | ---: | ---: | --- |
| `< 0.25` | `0.70` | `0.70` | 单侧预算不变，更多小单 |
| `0.25 - 0.60` | `0.85` | `0.85` | 单侧预算不变 |
| `0.60 - 1.00` | `1.00` | `1.00` | 单侧预算不变 |
| `1.00 - shock` | `1.20 - 1.50` | `0.70 - 0.90` | 单侧预算不变或收缩 |
| shock | n/a | n/a | 禁止新增 entry |

低波动时，通过缩小 step 和单笔来加快刷量，但必须保持总侧向预算不变：

```text
side_budget stays fixed
effective_per_order decreases
entry_capacity increases
```

高波动时，step 应该变宽，单笔应该变小。除非显式配置，否则不应该增加 side budget。

## Tick 粗粒度币种

有些币种的最小 tick 很粗。此时百分比 step 可能小于一个可成交 tick，导致配置目标没有实际意义。

计算：

```text
tick_ratio = tick_size / mid_price
```

建议模式：

```text
tick_dominated = tick_ratio >= 0.0035
coarse_tick = tick_ratio >= 0.0080
```

实际 step 取以下值的最大值，并向上对齐 tick：

```text
target_step = mid_price * target_step_ratio
tick_step = tick_size * min_tick_steps
fee_buffer_step = mid_price * min_profit_or_fee_buffer_ratio

effective_step = round_up_to_tick(max(target_step, tick_step, fee_buffer_step))
```

Regime 默认 tick 数：

| Regime | 最小 tick 步数 |
| --- | ---: |
| `ping-pong-fast` | `1` |
| `ping-pong-safe` | `1` 或 `2` |
| `wide-step` | `2` 或 `3` |
| `defensive` | `3+` 或禁止 entry |

如果 `coarse_tick=true`，说明一个 tick 本身就代表较大价格跳动，需要降低风险：

- 降低 `effective_per_order`；
- 必要时降低 side budget；
- 减少 levels；
- 解读 `loss_per_10k` 时结合 tick_ratio，避免把单 tick 跳动误判成策略失控；
- 避免多个百分比 step 最终落到同一个 tick 价格。

## Entry 与 Exit 逻辑分离

Entry step 和 exit step 必须分开。

`wide-step` 应该让 entry 间距变宽，但不能因为 entry 变宽，就把 take-profit 或库存修复单也挂到离盘口很远的位置。

订单类型：

- `entry_*`：受 regime step、side budget、switching 和 shock guard 控制。
- `take_profit_*`：优先保本或盈利退出，不因为 entry step 变宽而自动拉远。
- `risk_reduce_*`：有限亏损库存修复，只能在明确亏损预算内启用。

多仓库存：

```text
non_loss_sell = long_avg_price * (1 + min_profit_ratio)
market_sell = ask + 1 tick
max_loss_sell = long_avg_price * (1 - max_reduce_loss_ratio)
```

规则：

```text
if market_sell >= non_loss_sell:
    place near-quote take_profit_long
elif market_sell >= max_loss_sell and rolling_loss_budget_available:
    place small risk_reduce_long
else:
    trapped_inventory; cancel entry and wait
```

空仓库存：

```text
non_loss_buy = short_avg_price * (1 - min_profit_ratio)
market_buy = bid - 1 tick
max_loss_buy = short_avg_price * (1 + max_reduce_loss_ratio)
```

规则：

```text
if market_buy <= non_loss_buy:
    place near-quote take_profit_short
elif market_buy <= max_loss_buy and rolling_loss_budget_available:
    place small risk_reduce_short
else:
    trapped_inventory; cancel entry and wait
```

这可以同时避免两种失败模式：

- exit 被挂到离盘口太远，库存无法修复；
- 为了成交无限制亏损平仓。

## 与现有控制器的关系

数据流：

1. `adaptive_step` 计算短周期原始波动。
2. `competition_elastic_volume_v1` 给出 candidate regime 和基础缩放。
3. `regime_entry_budget_v2` 解决 switching、shock guard、side budget 和最终 entry capacity。
4. plan 生成使用最终有效参数。
5. submit / cancel 逻辑执行 cancel-first 状态切换要求。

现有 loss guard、runtime guard、hard-loss forced reduce、保证金保护和交易所校验继续拥有最终约束权。

## Plan 与 Event 报告

新增报告对象：

```json
{
  "regime_entry_budget": {
    "enabled": true,
    "state": "wide-step",
    "candidate_regime": "wide-step",
    "target_regime": null,
    "switching": false,
    "shock_guard_active": false,
    "cancel_entry_required": false,
    "open_entry_long_notional": 0.0,
    "open_entry_short_notional": 0.0,
    "long_side_budget": 2400.0,
    "short_side_budget": 2400.0,
    "long_entry_capacity": 20,
    "short_entry_capacity": 20,
    "effective_step_ratio": 0.005,
    "effective_step_price": 0.00075,
    "effective_per_order_notional": 120.0,
    "tick_dominated": false,
    "coarse_tick": false,
    "reconcile_ok_count": 3,
    "blocked_reason": null
  }
}
```

Event 摘要增加扁平字段，方便巡检脚本读取：

- `regime_entry_budget_state`
- `regime_entry_budget_switching`
- `regime_entry_budget_shock_guard_active`
- `regime_entry_budget_long_capacity`
- `regime_entry_budget_short_capacity`
- `regime_entry_budget_cancel_entry_required`
- `regime_entry_budget_tick_dominated`

## 测试

纯模块测试：

- fast regime 在低波动下缩小 step/per，并保持 side budget 不变。
- safe regime 根据库存和 open entry notional 计算 capacity。
- wide-step 在 side budget 有余量时允许同向 entry。
- wide-step 在库存加 open entry 达到预算后阻止同向 entry。
- candidate regime 变化会进入 switching，并阻止所有新增 entry。
- switching 只有在 non-reduce entry 归零且 reconcile 连续 OK 后才提交。
- shock 阈值触发后立即进入 `shock-guard`。
- defensive 状态把 entry budget 置为 0。
- coarse tick 模式在百分比目标小于 tick 时使用 tick step。
- exit step 与 entry step 相互独立。

Runner 测试：

- parser / config normalization 接受 v2 字段。
- saved-runner command 保留 v2 配置。
- plan JSON 包含 `regime_entry_budget`。
- event JSONL 包含 v2 扁平摘要字段。
- entry gate 接收 v2 输出的 capacity 限制。
- cancel-first switching 会在 plan 生成时压制 entry orders。

事故回归测试：

- 复现 BILLUSDT 凌晨 3 点故障形态：短周期高波动、旧 entry 仍在场、单边库存、reconcile diff。
- 断言控制器进入 `shock-guard`，取消 entry，输出 0 新 entry capacity，并且在 entry 清理完成前不会带着新 entry 进入 `wide-step`。

## 上线节奏

1. 先实现纯模块，默认关闭。
2. 增加 report-only 模式：计算 v2 决策但不影响订单。
3. 在低风险 runner 上开启 cancel-first switching。
4. 在保留现有 elastic regimes 的前提下启用 side-budget capacity。
5. 启用 wide-step 双边预算挂单。
6. 启用低波动状态内连续缩放。
7. 将 tick 粗粒度处理推广到所有 competition neutral profiles。

生产部署必须保持服务器 pull-based deploy。不要复制文件，也不要在 runtime 目录手工热补丁。
