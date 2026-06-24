# 现货 APP 损耗准则

本文档用于现货刷量策略的启动、停机和恢复判断。这里的损耗口径以币安 APP 交易分析页为准，而不是 runner 的库存成本或手续费口径。

## APP 损耗公式

币安 APP 的现货交易分析按买入、卖出和当前持仓价值计算：

```text
APP 损耗 = 买入成交额 - 卖出成交额 - 窗口净持仓数量 * 当前价格
APP 万U损耗 = APP 损耗 / (买入成交额 + 卖出成交额) * 10000
```

这个口径不包含 BNB 手续费抵扣，也不使用 runner 的库存批次成本。只要窗口内买入多于卖出，剩余持仓会直接按当前价格计值；价格低于窗口盈亏平衡价时，即使全部成交都是 maker，APP 仍会显示损耗。

`grid_optimizer.spot_app_loss_audit` 必须完整分页读取 `myTrades`。如果审计输出 `truncated=true`，该结果只能用于提示风险，不能用于判断 APP 损耗已经达标，也不能作为恢复依据。

## 114 XPL 停机快照

2026-06-24 22:04 CST，114 的 `XPLUSDT` 已按高损耗风险停机：

- 生产代码：`2615084`
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

## 2026-06-24 MEGA 完整审计结论

使用分页审计 `2026-06-17T00:00:00Z` 起的 `MEGAUSDT` APP 窗口后：

- 114：`trade_count=7031`，`maker_count=5744`，总成交额约 `1,016,307.52` USDT，窗口净持仓 `-8162.9` MEGA，APP 万U损耗 `0.0`，`truncated=false`。
- 150：`trade_count=6842`，`maker_count=5569`，总成交额约 `1,024,389.69` USDT，窗口净持仓 `15148.7` MEGA，APP 损耗约 `245.36` USDT，APP 万U损耗约 `2.395`，窗口 break-even 约 `0.06655678`，当时 ask 约 `0.05037`，安全 maker SELL 距离约 `1617` tick，`truncated=false`。

结论：

- MEGA 的高损耗不是手续费口径问题，而是 APP 窗口净持仓和成交价差问题。
- `take` 成交显著拉低 maker 占比，两台 maker_ratio 都只有约 `81%`；但 maker-only 子集也可能留下净多或净空，不能把“全 maker”当作低损耗保证。
- 150 当前不能恢复扩多；只能等价格回到 break-even 附近，或启用 `spot_app_loss_recovery_reduce_only_enabled=true` 后只做 maker SELL 减 APP 窗口暴露。
- 若安全 maker SELL 离 ask 超过 `2` tick，不应为了速度降价卖出；降价会把 APP 损耗锁死。

## 冻结仓位边界

合成中性策略不能把 `neutral_base_qty` 的回补当成无损恢复。APP 窗口已经净多时，继续买入只是把 APP 损耗暴露扩大。

恢复逻辑必须遵守：

- APP 损耗 guard 触发且 APP 窗口净多时，只允许 SELL 方向减 APP 窗口暴露。
- APP 窗口净多时，runner 的 guard 用 bid 估算持仓价值，不用 mid；这样不会因为半个 spread 的乐观估值低估 APP 万U损耗。
- 如果 `reset_state=true` 或 `known_orders` 丢失，runner 不能把 APP 窗口当作空白窗口。APP guard 必须从 Binance 原始 `myTrades` fallback 重建买卖数量和成交额，再决定是否允许恢复。
- 减仓 SELL 的价格必须不低于窗口盈亏平衡价，并向上按 tick 取整。例如当前 XPL 窗口 break-even 约 `0.08835`，合法 maker SELL 价至少是 `0.0884`。
- 如果启用 `spot_app_loss_recovery_reduce_only_enabled=true`，即使启动前审计因价格恢复而放行，只要 APP 窗口仍是净多，runner 也只能先做恢复减仓：删除 BUY，删除低于 `max(ask, break-even)` 的 SELL，并补一张 `LIMIT_MAKER` SELL。
- synthetic neutral 下不能只看 APP 窗口净持仓。减偏离方向和数量必须以 `actual_base_qty - neutral_base_qty` 为边界：高于 neutral 才能 SELL，低于 neutral 只能 BUY，不能卖穿 neutral 底仓。
- 低于 neutral 的净空偏离只能用 maker BUY 慢慢回补。BUY reduce 单必须按当前 bid 侧参考，不能保留高于 bid 的 BUY 单；高于 bid 的 BUY 会变成吃单或在高价回补，直接放大 APP 损耗。
- 如果设置了 `spot_app_loss_prestart_gate_min_bid_break_even_buffer_ticks`，启动后 runner 也会持续检查同一 bid-buffer。APP 损耗仍为 0 但 bid 到 break-even 的距离已经低于门槛时，runner 必须阻断扩多型 BUY；若 `actual_base_qty < neutral_base_qty`，仍只允许按 bid 上限 maker BUY 回补到 neutral，避免为了低损耗把组合长期卡在净空偏离。
- runner 还会逐张预测 BUY 成交后的 APP break-even。当前 bid-buffer 虽然达标，但某张 BUY 一旦成交会把 buffer 打到门槛以下时，该 BUY 不应挂出；不能等成交后下一轮才发现 APP 万U损耗重新变高。`actual_base_qty < neutral_base_qty` 时，回补到 neutral 以内的 maker BUY 是例外，代价是 break-even 可能短期被抬高；超出 neutral 的扩多 BUY 只整单保留或整单丢弃，巡航态不把普通 grid 单半截改成 reduce 单。
- 如果 `spot_freeze_skip_reason=short_hedge_capacity_exhausted`，说明冻结仓位没有合约 SHORT 容量继续抵消这段净空现货偏离；不要指望冻结自动生效，必须先低价 maker BUY 补回 neutral。
- 当 `actual_base_qty` 已回到 `neutral_base_qty`，且 APP 万U损耗仍低于 soft/hard 门槛时，runner 应退出 `recovery_reduce_only` 回到 `cruise`/`observe`。如果 `reduce_side=""` 仍卡在 recovery 状态，属于恢复空转，应停下来排查，不能扩大目标。
- 如果当前 ask 明显低于 break-even，成交速度慢是正确结果；为了速度在低价卖出，会直接锁定 APP 损耗。
- 冻结仓位没有明确启用并通过小窗口验证前，不允许把旧的 40 万目标直接恢复到生产。

## 恢复前硬条件

恢复 `XPLUSDT` 或同类现货刷量前，必须先完成以下检查：

- 用户明确同意恢复。
- 重新读取 Binance `myTrades`，按 APP 公式计算窗口损耗和 break-even。
- 使用只读审计命令留痕，例如：

```bash
PYTHONPATH=src python -m grid_optimizer.spot_app_loss_audit --symbol XPLUSDT --start-time 2026-06-24T11:57:00Z --require-gate
```

- 审计结果中的 `recovery_gate.allowed` 必须为 `true`，否则不恢复。带 `--require-gate` 时，gate 不通过会以非 0 退出码结束。恢复目标是 APP 万U损耗低于 `1` 或转正；maker 占比不低于 `0.99`；如果窗口净多，安全 maker SELL 价距离当前 ask 不超过 `2` 个 tick，且当前 bid 至少高于 APP break-even 指定 tick 数；`myTrades` 结果不能被 `limit` 截断。
- 通过 `/usr/local/bin/grid-saved-runner` 或 systemd 恢复前，控制文件必须显式保留预启动门禁：

```json
{
  "spot_app_loss_prestart_gate_enabled": true,
  "spot_app_loss_prestart_gate_start_time": "2026-06-24T19:57:00+08:00",
  "runtime_guard_stats_start_time": "2026-06-24T19:57:00+08:00",
  "spot_app_loss_prestart_gate_max_loss_per_10k": 1.0,
  "spot_app_loss_prestart_gate_max_safe_sell_gap_ticks": 2.0,
  "spot_app_loss_prestart_gate_min_bid_break_even_buffer_ticks": 3.0,
  "spot_app_loss_prestart_gate_min_maker_ratio": 0.99,
  "spot_app_loss_prestart_gate_min_gross_notional": 5000.0,
  "spot_app_loss_recovery_reduce_only_enabled": true
}
```

`grid_optimizer.run_saved_runner` 会在执行真实 runner 前调用同一审计命令；门禁非 0 时直接退出，不会启动 spot runner。Web 保存现货 runner 配置时也必须保留这一组 `spot_app_loss_prestart_gate_` 字段。systemd unit 必须安装 `RestartPreventExitStatus=2`，确保 gate 拒绝不是 `Restart=always` 的可重试失败；否则会反复审计 Binance，并可能在门禁临界变好时自动启动。
- 确认当前盘口到 break-even 的 tick 距离；若距离过大，只能接受低速挂 break-even maker SELL，不能为了速度降价卖。
- 使用小观察额度恢复，不直接使用原 40 万目标。
- 启动后先观察一个小窗口，确认 APP 万U损耗低于 `1` 或转正，再扩大目标。
- 若 APP 万U损耗继续高于 `1`，立即停机、撤单，并把控制文件改回 `apply=false`。
