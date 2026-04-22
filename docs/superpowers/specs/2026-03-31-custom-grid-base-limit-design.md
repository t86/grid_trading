# Custom Grid Base Limit Design

## Goal

让所有 `custom_grid_enabled=true` 的合约自定义网格不再受“底仓目标 / 超额库存只减仓”这一类底仓硬限制影响，同时不改变刷量/防守型合约策略的行为。

## Scope

- 只影响运行时 `custom_grid_enabled=true` 的合约 runner。
- 不影响 `one_way`、`hedge`、`synthetic`、`inventory_target_neutral` 这些非自定义网格预设。
- 不影响现货 runner。

## Design

### 1. 自定义网格不再维护底仓目标

`build_static_binance_grid_plan()` 新增一个关闭底仓补齐的开关。关闭后：

- 不生成 `bootstrap_orders`
- `target_base_qty` / `bootstrap_qty` 返回 `0`
- `target_long_base_qty` / `target_short_base_qty` / `bootstrap_long_qty` / `bootstrap_short_qty` 返回 `0`

网格主体挂单仍然保留：

- 下方接回撤买单 / 上方开空卖单
- 基于当前真实持仓生成的止盈/回补单

### 2. 自定义网格跳过 excess inventory gate

`loop_runner` 在 `custom_grid_enabled=true` 分支不再调用 `apply_excess_inventory_reduce_only()`。

这样即使历史控制文件里还保留 `excess_inventory_reduce_only_enabled=true`，自定义网格也不会因为“当前仓位高于 target_base_qty”而清掉新的买单/卖单。

### 3. Web 默认配置同步

`_normalize_custom_grid_runtime_config()` 把 `excess_inventory_reduce_only_enabled` 默认值改成 `False`，使新生成的自定义网格配置和运行时行为一致。

## Risks

- 自定义网格冷启动时不会再主动补出“预期底仓”，因此如果当前没有持仓，网格上方的止盈卖单不会凭空出现，必须等下方买单成交后才会形成对应卖单。
- 这属于预期变化，目的是让合约网格只受真实成交和仓位上限控制，而不是被底仓目标强行约束。
