# Runner Start Safety Policy

这份文档把生产 runner 的启动、恢复、手动干预、以及大波动后处理规则固化下来。它不是建议，而是启动前必须满足的约束。

适用范围：

- 所有合约 runner
- 所有现货 runner
- 所有 heartbeat / 自动巡检 / 自主调参任务
- 所有通过 web/API/manual wrapper 执行的 start / restart / resume

## 核心原则

如果状态一致性已经不可信，宁可保持 stopped，也不要为了刷量勉强恢复。

最危险的情况不是短时亏损，而是“策略以为自己已经收手，交易所其实还有旧单和脏状态继续成交”。

## 强制规则

### 1. 人工干预优先级最高

只要发生以下任一情况，都按“人工干预”处理：

- 手动减仓
- 手动强平
- 手动改仓
- 手动撤部分单但保留策略运行

处理规则：

1. 先停 runner。
2. 核对交易所真实仓位和 open orders。
3. 在仓位/挂单未完全理解前，不得恢复。

不允许一边人工处理仓位，一边让 runner 继续自动补单。

### 2. Reconcile Drift 是硬风险信号

`open_orders diff` 不能再被当成普通噪声。

- 单位数 drift：可以观察。
- 连续两轮 `open_orders diff > 10`：禁止恢复正常开仓。
- `open_orders diff > 20`：只允许防守、减仓、平衡、flatten。
- `open_orders diff > 30`：直接停 runner。
- 如果大 drift 同时伴随 API 错误、计划不一致、validation 失败，也必须停 runner。

典型危险信号：

- `open_orders diff=+35`
- `open_orders diff=+70`
- `当前未成交委托数量与计划生成时不一致`

### 3. Shock 后必须有真实观察期

出现以下任一情况后，不能立刻恢复正常开仓：

- 急涨急跌
- 连续 `volatility_entry_pause`
- `adaptive_step` 明显放大
- 单边库存快速积累

恢复前必须保留至少 `3-5` 分钟观察期。

仅靠少量 `recover_confirm_cycles` 通过，不足以视为可以恢复。

### 4. Restart 前必须过一致性检查

任何 incident 后，restart 前至少确认：

1. runner 已经真的 stopped
2. 不存在残留 `loop_runner` 进程
3. exchange-side 仓位和 open orders 已理解
4. recent API / reconcile / submit / validation 错误已检查
5. 最新 control / plan 与 intended defensive or recovery profile 一致

有任一项不明确时，保持 stopped。

### 5. 恢复必须分级

从 defensive 恢复到正常刷量时，不能一步回满。

正确顺序应当是：

1. 大 step、小单笔、低密度
2. 确认波动平稳、库存可控、drift 干净
3. 再逐步缩 step、放大单笔、恢复层数

不允许从 shock 刚结束直接跳回 rush / full-volume 档。

## 事故复盘结论

这套规则来自一次真实事故，其共同特征是：

1. 波动保护触发过，但恢复过快
2. `open_orders diff` 很大，挂单账不干净
3. 交易所报错导致本地计划与真实执行脱钩
4. 人工减仓时 runner 仍然活着
5. 最后出现“人工在减，策略在补”的冲突

根因不是某一个参数错了，而是：

- 执行层状态一致性失真后，系统仍被允许恢复交易

## 代码与巡检约束

这份规则必须同时体现在两层：

1. heartbeat / automation prompt 规则
2. 代码级启动前 preflight

只存在于文档、不存在于启动代码里的规则，不算真正落地。
