# 现货 APP 损耗准则

本文档用于现货刷量策略的启动、停机和恢复判断。这里的损耗口径以币安 APP 交易分析页为准，而不是 runner 的库存成本或手续费口径。

## APP 损耗公式

币安 APP 的现货交易分析按买入、卖出和当前持仓价值计算：

```text
APP 损耗 = 买入成交额 - 卖出成交额 - 窗口净持仓数量 * 当前价格
APP 万U损耗 = APP 损耗 / (买入成交额 + 卖出成交额) * 10000
```

这个口径不包含 BNB 手续费抵扣，也不使用 runner 的库存批次成本。只要窗口内买入多于卖出，剩余持仓会直接按当前价格计值；价格低于窗口盈亏平衡价时，即使全部成交都是 maker，APP 仍会显示损耗。

## 114 XPL 停机快照

2026-06-24 22:04 CST，114 的 `XPLUSDT` 已按高损耗风险停机：

- 生产代码：`f3cd083`
- systemd：`grid-loop@XPLUSDT.service` 为 `disabled` / `inactive`
- 交易所挂单：`open_orders=0`
- 控制文件：`output/xplusdt_spot_loop_runner_control.json` 中 `apply=false`
- 停机保护备份：`output/xplusdt_spot_loop_runner_control.json.bak_safe_stop_20260624T135840Z`

同一窗口的 Binance 成交核算：

- 成交数：128
- maker 数：128
- 总成交额：`5085.21244` USDT
- 买入成交额：`2724.6815` USDT
- 卖出成交额：`2360.53094` USDT
- 买入数量：`30794.9` XPL
- 卖出数量：`26673.1` XPL
- 窗口净持仓：`4121.8` XPL
- 窗口盈亏平衡价：`0.0883474598`
- 当时盘口：bid `0.0868` / ask `0.0869`
- APP 损耗：`6.37832` USDT
- APP 万U损耗：`12.542878`

结论：这次 XPL 的高损耗不是 taker 造成的，实际成交全部是 maker；核心问题是旧的合成中性恢复把 APP 窗口做成净多仓，价格低于窗口盈亏平衡价后，APP 直接把未卖出的持仓按当前价格计损。

## 冻结仓位边界

合成中性策略不能把 `neutral_base_qty` 的回补当成无损恢复。APP 窗口已经净多时，继续买入只是把 APP 损耗暴露扩大。

恢复逻辑必须遵守：

- APP 损耗 guard 触发且 APP 窗口净多时，只允许 SELL 方向减 APP 窗口暴露。
- APP 窗口净多时，runner 的 guard 用 bid 估算持仓价值，不用 mid；这样不会因为半个 spread 的乐观估值低估 APP 万U损耗。
- 如果 `reset_state=true` 或 `known_orders` 丢失，runner 不能把 APP 窗口当作空白窗口。APP guard 必须从 Binance 原始 `myTrades` fallback 重建买卖数量和成交额，再决定是否允许恢复。
- 减仓 SELL 的价格必须不低于窗口盈亏平衡价，并向上按 tick 取整。例如当前 XPL 窗口 break-even 约 `0.08835`，合法 maker SELL 价至少是 `0.0884`。
- 减仓数量按 APP 窗口净持仓计算，不按 `actual_base_qty - neutral_base_qty` 单独计算。
- 如果当前 ask 明显低于 break-even，成交速度慢是正确结果；为了速度在低价卖出，会直接锁定 APP 损耗。
- 冻结仓位没有明确启用并通过小窗口验证前，不允许把旧的 40 万目标直接恢复到生产。

## 恢复前硬条件

恢复 `XPLUSDT` 或同类现货刷量前，必须先完成以下检查：

- 用户明确同意恢复。
- 重新读取 Binance `myTrades`，按 APP 公式计算窗口损耗和 break-even。
- 确认当前盘口到 break-even 的 tick 距离；若距离过大，只能接受低速挂 break-even maker SELL，不能为了速度降价卖。
- 使用小观察额度恢复，不直接使用原 40 万目标。
- 启动后先观察一个小窗口，确认 APP 万U损耗低于 10 或转正，再扩大目标。
- 若 APP 万U损耗继续高于 10，立即停机、撤单，并把控制文件改回 `apply=false`。
