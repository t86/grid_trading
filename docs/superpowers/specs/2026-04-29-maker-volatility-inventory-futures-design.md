# 合约波动率库存做市策略设计

## 目标

新增一个可测试的合约 maker 策略，用简单的双边挂单提供流动性，同时控制亏损、库存漂移和订单抖动。第一版使用名义金额口径，所以“单笔 30、单边最大 300”表示报价资产名义金额，不表示币数量。

## 范围

策略运行在现有合约 `loop_runner` 执行框架内。它不是一个单独 bot，也不应该改变已有 `synthetic_neutral`、`hedge_neutral`、`one_way_long` 或 `one_way_short` 预设的行为。

新增策略模式：

```text
maker_volatility_inventory_v1
```

策略复用现有 runner 能力：

- 合约行情数据，包括 REST 或 websocket 快照
- 账户和持仓读取
- post-only 订单准备和重试处理
- plan 校验、`max_new_orders`、`max_total_notional`、旧单处理
- runtime guards、状态文件、审计文件和 runner 控制路径

## 策略行为

每轮读取买一、卖一、中价、当前净持仓和最近合约 K 线。策略在盘口附近生成目标 maker 订单：

- `normal`：使用 `maker_base_spread_bps`，围绕当前盘口挂一笔买单和一笔卖单。
- `volatility_wide`：如果最近振幅或绝对涨跌超过配置阈值，使用 `maker_wide_spread_bps` 加宽双边报价。
- `long_inventory_reduce`：如果多头名义金额达到软库存阈值，减少或暂停新的买单，保留卖单。
- `short_inventory_reduce`：如果空头名义金额达到软库存阈值，减少或暂停新的卖单，保留买单。
- `hard_reduce_only`：如果单边名义金额达到硬上限，只生成降低该方向敞口的订单。
- `cooldown`：极端波动或连续提交拒单信号之后，暂停新增开仓订单，直到冷却结束。

急速单边行情按防守优先处理：

- 普通高波动先进入 `volatility_wide`，加宽 spread，但仍允许双边 maker 单。
- 如果高波动同时伴随方向性急跌，并且多头库存达到软阈值，进入 `long_inventory_reduce`，暂停新的 BUY，只保留降低多头敞口的 SELL。
- 如果高波动同时伴随方向性急涨，并且空头库存达到软阈值，进入 `short_inventory_reduce`，暂停新的 SELL，只保留降低空头敞口的 BUY。
- 如果单边库存达到硬上限，直接进入 `hard_reduce_only`，不再新增风险方向。
- 如果最近振幅或绝对涨跌达到极端阈值，进入 `cooldown`，暂停新增开仓订单，等待盘口重新稳定。
- v1 不主动吃单止损；如果后续实盘证明 maker 减仓太慢，再增加 aggressive reduce。

库存规则基于实际净持仓名义金额：

```text
long_notional = max(net_qty, 0) * mid
short_notional = max(-net_qty, 0) * mid
soft_limit = hard_limit * maker_inventory_soft_ratio
```

## 配置

第一版字段：

```json
{
  "strategy_mode": "maker_volatility_inventory_v1",
  "maker_base_spread_bps": 4.0,
  "maker_wide_spread_bps": 12.0,
  "maker_order_notional": 30.0,
  "maker_max_long_notional": 300.0,
  "maker_max_short_notional": 300.0,
  "maker_inventory_soft_ratio": 0.7,
  "maker_volatility_window": "1m",
  "maker_volatility_wide_threshold": 0.006,
  "maker_extreme_volatility_threshold": 0.012,
  "maker_directional_move_threshold": 0.004,
  "maker_cooldown_seconds": 30.0
}
```

第一版每轮每边最多生成一笔订单。现有 runner 级别的 `max_new_orders` 和 `max_total_notional` 仍然是最终提交限制。

## 订单构造

每个方向的规则：

- 买单价格在 spread 调整后低于或等于买一侧可挂价格。
- 卖单价格在 spread 调整后高于或等于卖一侧可挂价格。
- 数量按 `maker_order_notional / price` 计算，并沿用现有交易对 step 规则取整。
- 订单使用能明确表达 reduce-only 语义的 role：
  - `maker_entry_long`
  - `maker_entry_short`
  - `maker_reduce_long`
  - `maker_reduce_short`

post-only 价格调整仍由现有提交路径负责。

## 状态

runner state 中保存一个小的策略状态对象：

```json
{
  "maker_volatility_inventory": {
    "regime": "normal",
    "last_regime": "normal",
    "cooldown_until": null,
    "last_reason": null
  }
}
```

v1 的状态切换只依赖当前轮数据。实盘如果发现状态过于频繁抖动，再增加滞后确认逻辑。

## 错误和保护

如果行情数据缺失或无效，plan 不应生成新订单，并且要包含清晰的 blocked reason。

如果账户不是单向持仓模式，v1 仍然按实际净敞口计算，避免引入 hedge mode 专属行为。双向持仓支持可以等测试明确 position-side 语义后再加。

策略不能有意自成交。它只通过正常策略订单前缀提交 maker 订单，并依赖 Binance/账户 STP 设置以及现有订单 diff，减少不必要的交叉行为。

## 测试

实现前先增加聚焦单元测试：

- `normal` 状态生成一笔买单和一笔卖单。
- 高波动时，相比基础 spread，报价会变宽。
- 极端波动时进入 `cooldown`，不生成新增开仓单。
- 多头软库存触发时，抑制或减少买单。
- 空头软库存触发时，抑制或减少卖单。
- 急跌且多头软库存触发时，只保留降低多头敞口的卖单。
- 急涨且空头软库存触发时，只保留降低空头敞口的买单。
- 多头达到硬上限时，只生成降低多头敞口的卖单。
- 空头达到硬上限时，只生成降低空头敞口的买单。
- 行情数据无效时，不生成订单，并返回 blocked reason。

集成测试需要覆盖 `generate_plan_report()` 能识别新的 `strategy_mode`，并且生成的 plan 仍然兼容现有提交校验。
