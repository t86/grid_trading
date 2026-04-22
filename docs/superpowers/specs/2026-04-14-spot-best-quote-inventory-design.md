# Spot Best Quote Inventory Design

## Goal

为现货 runner 增加一套适合交易赛刷量的轻仓模式：先补到底仓，再在库存受控前提下尽量贴 `bid1` / `ask1` 做 maker 双边换手；库存过重时只减仓，必要时用 taker 兜底。

## Constraints

- 仅适用于现货，不允许卖出超过现货可卖库存。
- 库存目标围绕 `base_position_notional` 波动。
- 库存硬上限固定为 `2 x base_position_notional`。
- 到达硬上限后进入 `reduce_only`，停止新增买单。
- `reduce_only` 状态下减仓委托尽量贴盘口边缘。
- 如果 `reduce_only` 连续 60 秒仍未把库存压回安全区，允许触发一次 taker 市价减仓。
- 正常卖单默认要求价格至少高于库存均价/成本保护线，先观察刷量效果，再决定是否放宽。

## State Model

### bootstrap

- 库存名义低于底仓目标时运行。
- 只负责补仓，不做买一/卖一双边刷量。
- 若账户已有现货库存，可直接识别并跳过冷启动。

### quote

- 库存达到底仓后进入。
- 维护最多一张贴 `bid1` 的买单和一张贴 `ask1` 的卖单。
- 买单仅在库存名义低于硬上限时保留。
- 卖单仅在价格不低于成本保护线且可卖数量足够时保留。

### reduce_only

- 库存名义达到或超过 `2 x base_position_notional` 时进入。
- 取消买单，只保留贴边减仓卖单。
- 记录进入时间；超过 60 秒且库存仍高于回落目标时，允许一次 taker 市价减仓，把库存打回安全区。

## Order Rules

- 正常刷量阶段使用 `LIMIT_MAKER`。
- 买单价格尽量取 `bid1`；卖单价格尽量取 `ask1`，但不得低于成本保护线。
- 若成本保护线高于当前可挂 maker 卖价，则本轮不挂卖单。
- `reduce_only` 阶段的贴边卖单优先以盘口最贴边的 maker 价格提交。
- taker 减仓采用 `MARKET SELL`，目标库存回落到 `1.4 x base_position_notional`。

## Integration

- 在 `src/grid_optimizer/spot_loop_runner.py` 新增第三种 `strategy_mode`。
- 复用现有 `inventory_lots`、成交同步、运行时风控与下单基础设施。
- 在 `src/grid_optimizer/web.py` 增加对应预设和文案。
- 在 `tests/test_spot_loop_runner.py` 覆盖状态切换、库存上限、成本保护和 taker 兜底。
