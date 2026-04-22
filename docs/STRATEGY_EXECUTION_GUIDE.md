# 策略执行说明

本文档对应监控台“策略参数编辑”右侧说明面板，目标是把每个预设在实盘里的真实执行逻辑讲清楚，而不是简单解释字段名。

## 通用执行框架

所有 runner 预设都遵循同一条主流程：

1. 读取当前盘口、中价、持仓、已有挂单。
2. 决定这一轮的中心价。
3. 按策略模式生成目标订单。
4. 套用停单、限仓、库存分层、极端行情保护。
5. 把“目标订单”和“当前挂单”做 diff，再决定保留、补单、撤单。

### 中心价怎么确定

- `one_way_long` / `one_way_short` / `hedge_neutral` / `synthetic_neutral`
  - 默认用 `up_trigger_steps`、`down_trigger_steps`、`shift_steps` 迁移中心。
  - 触发条件是“中价和中心的偏离格数”，不是百分比。
  - 一轮里会连续移动，直到中价重新回到触发带内。
- `fixed_center_enabled=true`
  - 中心价固定，不再按普通迁移规则追价。
  - 如果同时开了 `fixed_center_roll_enabled`，才会按单独的 roll 规则缓慢滚动。
- `inventory_target_neutral`
  - 不用 up/down/shift 迁移。
  - 直接取最近一根闭合 `3m/5m` K 线收盘价作为中心。

### 订单 diff 与撤单规则

当前实现的撤单逻辑不是“每轮先全撤再重挂”，而是按价位桶比较：

- 同方向、同 `position_side`、同价格：
  - 目标总量不变：保留原单。
  - 目标总量增加：保留原单，只补差额。
  - 目标总量减少：旧单撤掉，按新总量重挂。
- 目标价位整个消失：旧单撤掉。

这意味着：

- 同价位增量补单不会丢掉原来的排队位置。
- 真正会触发撤单的，是价位变化或同价位缩量。

### 提交保护

- `cancel_stale=false`
  - 如果新计划里存在需要撤掉的旧单，提交器会直接拒绝执行。
- `max_plan_age_seconds`
  - 计划超过这个年龄就不提交。
- `max_mid_drift_steps`
  - 计划生成后，如果实时中价已经偏离太多格，这一轮不提交。
- `maker_retries`
  - post-only 被拒时，最多重试这么多次。

## 策略模式

### 1. `one_way_long`

典型预设：

- `volume_long_v4`
- `volatility_defensive_v1`
- `adaptive_volatility_v1`
- `defensive_quasi_neutral_v1`
- `defensive_quasi_neutral_aggressive_v1`

执行方式：

- 中心下方挂买单，负责继续接多。
- 中心上方挂卖单，负责卖出现有多仓。
- 如果当前多仓低于 `base_position_notional` 对应底仓，会在买一附近先补一笔 `bootstrap` 买单。
- 卖单只会按现有多仓数量生成，不会为了卖单反手开空。

风险控制：

- `pause_buy_position_notional`
  - 多仓名义达到阈值后，清掉 bootstrap 和买单，只保留卖单卸仓。
- `max_position_notional`
  - 如果这一轮新增买单会把总多仓推过上限，买单会被裁剪。
- `buy_pause_amp_trigger_ratio` + `buy_pause_down_return_trigger_ratio`
  - 最近 1 分钟同时满足“大振幅 + 明显收跌”时，暂停 LONG 开仓买单。
- `freeze_shift_abs_return_trigger_ratio`
  - 最近 1 分钟绝对涨跌过大时，本轮冻结中心迁移。

库存分层：

- 当持仓名义从 `inventory_tier_start_notional` 增加到 `inventory_tier_end_notional` 时，
  - 买层数、卖层数、单笔名义、基础底仓会线性过渡。
- 常见用法是：
  - 多仓越重，买单越少，卖单越多，基础底仓越轻。

### 2. `one_way_short`

典型预设：

- `volume_short_v1`
- `volume_short_v1_aggressive`
- `night_volume_short_v1`
- `volume_short_v1_conservative`

执行方式：

- 中心上方挂卖单，负责开空。
- 中心下方挂买单，负责回补已有空仓。
- 如果当前空仓低于 `base_position_notional` 对应基础空仓，会在卖一附近补 `bootstrap_short`。
- 买单只按现有空仓生成，不会因为买回而反手开多。

风险控制：

- `pause_short_position_notional`
  - 空仓名义达到阈值后，清掉 bootstrap_short 和新的卖单，只保留买回补空单。
- `max_short_position_notional`
  - 如果新增卖单会把总空仓推过上限，卖单会被裁剪。

实现注意：

- `short_cover_pause_amp_trigger_ratio`
- `short_cover_pause_down_return_trigger_ratio`

这两个字段目前还没有接入 `loop_runner` 的实际暂停逻辑，现阶段只是保存在配置里，不会真的触发“暂停买回补空”。

### 3. `hedge_neutral`

典型预设：

- `neutral_hedge_v1`

执行方式：

- LONG 腿：
  - 下方买入开多，上方卖出平多。
- SHORT 腿：
  - 上方卖出开空，下方买入平空。
- 两条腿各自维护自己的基础仓位和网格，不互相抵消。

要求：

- 账户必须是真正的双向持仓模式，否则提交器会拒绝。

风险控制：

- LONG 和 SHORT 各自有独立的 pause / max notional 限制。
- LONG 侧仍然会受到 “1 分钟极端下跌停买” 的保护。

### 4. `synthetic_neutral`

典型预设：

- `synthetic_neutral_v1`

执行方式：

- 先像 `hedge_neutral` 一样生成一套双边计划。
- 再把 LONG / SHORT 两本账折成单向账户可以提交的委托。
- 系统会持续同步一份“虚拟 long/short 账本”，用来判断当前应该补哪边。

适用场景：

- 账户不能切 hedge mode，但又想跑近似双边中性逻辑。

局限：

- 实际净仓和虚拟账本之间会有同步成本，所以要比真 hedge 更依赖状态一致性。

### 5. `inventory_target_neutral`

典型预设：

- `volume_neutral_target_v1`

这套不是传统一格一格的网格，而是“目标净仓曲线执行器”。

执行方式：

- 每隔 `neutral_center_interval_minutes` 分钟，取最新闭合 K 线收盘价作为中心。
- 价格跌到中心下方时：
  - 逐步把目标净仓提升到 `max_position_notional × target_ratio`
- 价格涨到中心上方时：
  - 逐步把目标净仓降低到负值，也就是转成净空。
- 如果当前净仓和“此刻目标净仓”不一致，会先发 bootstrap 单把净仓拉向当前目标，再在更深的带宽位置挂后续单。

真正生效的核心字段：

- `neutral_center_interval_minutes`
- `neutral_band1/2/3_offset_ratio`
- `neutral_band1/2/3_target_ratio`
- `max_position_notional`
- `max_short_position_notional`
- `neutral_hourly_scale_*`

当前不直接驱动实际挂单的字段：

- `buy_levels`
- `sell_levels`
- `per_order_notional`
- `base_position_notional`
- `up_trigger_steps`
- `down_trigger_steps`
- `shift_steps`

也就是说，这些字段虽然仍然出现在 JSON 里，但在这个模式下不是决定性参数。

## 预设策略说明

### `volume_long_v4`

- 核心：偏多、量优先。
- 先保底仓，再靠上下微网格滚动成交。
- 更适合稳定或偏强走势。

### `volatility_defensive_v1`

- 核心：高波动防守。
- 比 `volume_long_v4` 更轻仓、更早停买、更慢继续接。
- 更适合下跌扩振或极端波动窗口。

### `adaptive_volatility_v1`

- 核心：自动在“量优先做多”和“高波动防守”之间切换。
- 不是固定参数集。
- 会根据 15m / 60m 振幅和跌幅连续确认后再切档。

### `volume_short_v1`

- 核心：标准做空微网格。
- 适合偏弱、冲高回落、反抽后继续走弱的窗口。

### `volume_short_v1_aggressive`

- 核心：做空冲量版。
- 思路和 `volume_short_v1` 一样，但更强调换手。

### `night_volume_short_v1`

- 核心：NIGHT 低价高换手专用。
- 第一笔卖空和第一笔买回更靠近，轮询也更快。
- 目标是缩短一个来回成交的时间。

### `volume_short_v1_conservative`

- 核心：保守试空。
- 空仓扩张更慢，单笔更轻，优先控制回补风险。

### `defensive_quasi_neutral_v1`

- 核心：准中性降损。
- 仍然是做多策略，不是真正中性。
- 通过减少买侧、增加卖侧，尽量降低库存继续累积的速度。

### `defensive_quasi_neutral_aggressive_v1`

- 核心：准中性降损的高量版。
- 仍然是做多实现，只是卖侧更重、轮询更快、仓位上限更高。

### `volume_neutral_target_v1`

- 核心：按目标净仓曲线做中性。
- 不是传统微网格。
- 更关注净仓偏离，而不是一层层固定价差单。

### `based_competition_neutral_v1`

- 核心：`BASEDUSDT` 专用的偏空中性交易赛预设。
- 仍按目标净仓曲线运行，但净空上限高于净多上限。
- 适合高波动、宽震荡、整体偏弱的刷量窗口。

### `based_competition_neutral_aggressive_v1`

- 核心：`BASEDUSDT` 专用的冲量中性交易赛预设。
- 把目标净仓带收紧到 `0.5% / 1.0% / 2.0%`，同时显著抬高净仓与总名义上限。
- 适合优先冲成交量的比赛窗口；手续费损耗和短时回撤会明显高于稳健版。

### `neutral_hedge_v1`

- 核心：真双向中性。
- 需要账户原生支持 LONG/SHORT 双腿。

### `synthetic_neutral_v1`

- 核心：单向账户里的合成双边中性。
- 用虚拟账本模拟 hedge。

## 额外说明

### `autotune_symbol_enabled`

如果开启，启动前 web 端会按币种最小 tick、盘口 spread、最小成交额去修正：

- `step_price`
- `per_order_notional`
- `base_position_notional`

所以：

- 你在 JSON 里看到的值，不一定就是最终执行值。
- 如果你要精确手动调参，通常应关闭它。

### “准中性”不等于“真中性”

目前名字里带“准中性”的两个预设：

- `defensive_quasi_neutral_v1`
- `defensive_quasi_neutral_aggressive_v1`

实现上都还是 `one_way_long`。

它们只是：

- 让买侧更轻
- 让卖侧更重
- 让已有多仓更快被拆掉

而不是真正同时维护 LONG / SHORT 两条腿。
