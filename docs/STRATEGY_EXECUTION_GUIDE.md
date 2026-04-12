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

### 市场成交额自动启停

这套能力运行在 `web.py` 的后台巡检线程里，不属于 `loop_runner` 主循环本身。

- 量能来源
  - 取 Binance 合约最近 `1m` K 线的 `quote volume`，按窗口汇总成“市场成交额”。
  - 当前支持 `15m / 30m / 1h / 4h / 24h`。
- 自动启动
  - 当策略当前未运行，且最近窗口市场成交额 `>= volume_trigger_start_threshold` 时，后台会按保存下来的控制参数自动拉起 runner。
- 自动停止
  - 当策略当前正在运行，且最近窗口市场成交额 `< volume_trigger_stop_threshold` 时，后台会自动停机。
  - 如果启用了 `volume_trigger_stop_cancel_open_orders`，停机前会先撤当前交易对的未成交委托。
  - 如果启用了 `volume_trigger_stop_close_all_positions`，停机时会继续启动 maker flatten，直到仓位归零。

实盘建议：

- `start_threshold` 和 `stop_threshold` 最好不要相同。
- 常见做法是 `stop_threshold < start_threshold`，留一段回差，减少高低边界反复穿越时的频繁启停。
- 如果你想等放量后再入场，通常配合“保存参数不启动”更顺手。

### 市场波动自动暂停

这套能力同样运行在 `web.py` 的后台巡检线程里，用最近窗口聚合后的整窗波动来决定是否暂停策略。

- 波动来源
  - 取 Binance 合约最近 `1m` K 线，聚合成最近窗口的开高低收。
  - 当前支持 `15m / 30m / 1h / 4h / 24h`。
  - `volatility_trigger_amplitude_ratio` 口径是 `window_high / window_low - 1`。
  - `volatility_trigger_abs_return_ratio` 口径是 `abs(window_close / window_open - 1)`。
- 自动暂停
  - 当策略当前正在运行，且最近窗口振幅或绝对涨跌达到阈值时，后台会自动停机。
  - 如果启用了 `volatility_trigger_stop_cancel_open_orders`，停机前会先撤当前交易对的未成交委托。
  - 如果启用了 `volatility_trigger_stop_close_all_positions`，停机时会继续启动 maker flatten，直到仓位归零。
- 自动恢复
  - 如果这次停机是由波动触发的，后台会在波动回落到阈值以内后自动恢复。
  - 手动点“停止策略”或“保存参数不启动”会清掉这个自动恢复状态，避免用户明明手停了，后台又自己拉起。
- 默认值
  - 默认窗口是 `1h`。
  - 默认振幅阈值是 `4%`。
  - 默认绝对涨跌阈值是 `2%`。
  - 默认动作是“撤单并清仓”，这样单边快速扩展时不会因为保留仓位继续扩大亏损；如果你想只做保护性暂停，也可以手动关掉自动清仓。

### 生产操作约束

- 生产机器上的策略 runner、flatten runner、`output/*_loop_runner.pid`、`output/*_loop_runner_control.json` 必须由 `ubuntu` 用户持有和启动。
- 日常启动、停止、重启，优先走监控台页面，或由 `ubuntu` 用户调用本机 `grid-web` / `wangge-web` 提供的 `/api/runner/start`、`/api/runner/stop`。
- 不要用 `root` 直接执行 `python -m grid_optimizer.loop_runner`、`python -m grid_optimizer.maker_flatten_runner`，也不要让 `root` 写入 runner 的 pid / control / state 文件。
- 如果需要跨电脑查看 `KAT` 巡检结果，使用 [`docs/KAT_GUARD_AUTOMATION.md`](./KAT_GUARD_AUTOMATION.md) 里约定的远端落盘路径，不要依赖本机 automation 列表。

原因：

- 当前页面和 web 进程在生产上默认由 `ubuntu` 运行。
- 如果 runner 是 `root` 拉起，pid 文件或进程本身会变成 `root` 所有，页面停机、撤单、重启时会出现权限错误。
- 结果会变成“策略还在跑，但界面按钮失效”，只能再用 `sudo` 手工清理。

如果发现进程已经被 `root` 拉起，处理顺序应当是：

1. 用 `sudo` 停掉错误的 `root` 进程。
2. 清理对应的 `output/*_loop_runner.pid`，必要时检查 control / state 文件所有者。
3. 切回 `ubuntu` 用户，通过页面或本机鉴权 API 重新启动。

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

最近 1 分钟如果同时满足“振幅 >= short_cover_pause_amp_trigger_ratio”且“收跌 <= short_cover_pause_down_return_trigger_ratio”，`loop_runner` 会暂停本轮买回补空单，避免在急跌扩振里过早回补。

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
- `volume_neutral_ping_pong_v1`

执行方式：

- 先像 `hedge_neutral` 一样生成一套双边计划。
- 再把 LONG / SHORT 两本账折成单向账户可以提交的委托。
- 系统会持续同步一份“虚拟 long/short 账本”，用来判断当前应该补哪边。
- 如果启用了 `startup_entry_multiplier`，首轮 `startup_pending` 时会把买一 / 卖一放大；
  首轮之后恢复为普通 `per_order_notional`。

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

### 6. `competition_inventory_grid`

- 核心：围绕这条策略自己最近一次有效成交的锚点滚动，不是围绕市场 last trade，也不是按通用中心迁移逻辑整体平移。
- 执行方式：
  - `flat` 时会在买一 / 卖一各挂一笔 `bootstrap_entry` maker 单。
  - futures 里哪一边先成交，就把方向切到 `long_active` 或 `short_active`，后续网格围绕这次有效成交继续展开。
  - `grid_entry` / `grid_exit` 成交会继续更新 `grid_anchor_price`。
- 重启 / 恢复：
  - 优先读取本地 state 里的 competition runtime cache，再把交易所最近能映射到策略订单引用的成交增量应用进去。
  - 只有缓存缺失、缓存和当前实仓对不上，或者增量应用失败时，才退回到“按近端策略成交回放重建 runtime”。
  - 外部成交不会拿来更新这套 runtime；`pair_credit_steps` 只保留在内存里，重启后恢复为 0。
- 锚点写回规则：
  - 会更新锚点的成交：`bootstrap_entry`、`grid_entry`、`grid_exit`
  - 不会更新锚点的成交：`forced_reduce`、`tail_cleanup`
- 风险控制：
  - `threshold_position_notional`
    - 达到阈值后进入 `threshold_reduce_only`。
    - 先看 pair-credit 是否足够覆盖本轮 `forced_reduce` 的成本；不够时，本轮不挂 `forced_reduce`，同侧新增仓位也会暂停。
  - `max_position_notional`
    - 到达硬上限后进入 `hard_reduce_only`。
    - 这时只保留减仓逻辑，不再允许新增同向开仓；`forced_reduce` 不再受 pair-credit 余额限制。
- 适用前提：
  - 这是单向持仓模式的 futures 竞赛库存网格，不能在双向持仓账户里跑。

### 7. `spot_competition_inventory_grid`

- 核心：和上面的竞赛库存网格是同一套引擎，但适配成 spot 的 long-only 库存滚动。
- 执行方式：
  - `flat` 时只挂一笔 `bootstrap_entry` BUY，不会反向开空。
  - 后续只围绕已持有现货库存做买入补仓、卖出减仓和尾部清理。
- 重启 / 恢复：
  - 优先读取本地 state 里的 spot competition runtime cache，再把交易所最近能映射到 `known_orders` 的策略成交增量应用进去。
  - 只有缓存缺失、缓存和当前 spot 库存数量对不上，或者增量应用失败时，才退回到“按近端策略成交回放重建 runtime”。
  - 外部成交不会拿来重建这套 runtime；`pair_credit_steps` 不持久化，重启后从 0 开始。

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

### `neutral_hedge_v1`

- 核心：真双向中性。
- 需要账户原生支持 LONG/SHORT 双腿。

### `synthetic_neutral_v1`

- 核心：单向账户里的合成双边中性。
- 用虚拟账本模拟 hedge。

### `volume_neutral_ping_pong_v1`

- 核心：量优先的单向合成中性。
- 不持有初始底仓，首轮买一 / 卖一可放大，后续反手单回到常规尺寸。
- 更适合想保留中性结构、又不想一上来先 bootstrap 出库存的场景。

### `bard_volume_long_v2`

- 核心：BARDUSDT 专用做多预设。
- 启动前先做 `flat_start` 门禁，避免旧挂单或反向仓位直接混进来。
- 如果账户已经带着同向多仓启动，首轮会先禁掉 `bootstrap`，让网格先顺着现有库存运转。

### `bard_12h_push_neutral_v2`

- 核心：BARDUSDT 的固定节奏双向冲量模板。
- 固定 `step_price=0.0005`、`8` 买 `4` 卖、零底仓、`1` 格追中心，`per_order_notional=45`，长侧停买/硬上限 `420/650`、短侧回补护栏 `220/320`。
- 轻仓回补默认带 `take_profit_min_profit_ratio=0.0001`，也就是首档至少保 1 tick 盈利，不再走亏损贴边平仓。
- 默认关闭 `autotune_symbol_enabled`、`excess_inventory_reduce_only_enabled`、`volatility_trigger_enabled` 和量能自动启停，优先按固定节奏控损刷量。
- 如果后续要复用“贴盘口先成交、成交优先”的旧风格，改用 `synthetic_neutral_bard_style_v1`；当前这个 preset 已默认偏向微网格盈利。

### `synthetic_neutral_bard_style_v1`

- 核心：保留一份可复用的 BARD 贴边成交模板。
- 固定 `step_price=0.0007`、`8` 买 `4` 卖、零底仓、`1` 格追中心，`per_order_notional=45`，长侧上限 `2000/2400`、短侧回补护栏 `220/320`。
- 默认 `take_profit_min_profit_ratio=0.0`，保留贴盘口首档回补的旧成交风格，更适合成交量优先、愿意接受更强贴边出清的场景。
- 如果要降低损耗、让轻仓回补先追求微利，优先从 `bard_12h_push_neutral_v2` 出发，而不是从这个模板出发。
- 仓库里附了一个可复制模板文件：
  `deploy/oracle/runtime_configs/synthetic_neutral_bard_style_v1.template.json`

### `based_volume_long_trigger_v1`

- 核心：BASEDUSDT 的放量启动做多。
- 思路参考 BARD 的活跃窗口做法，但把步长放宽、仓位缩小、卖侧加重。
- 默认只在最近 `15m` 市场成交额达到阈值后自动启动；量能回落后自动停机、撤单并清仓。

### `based_volume_push_bard_v1`

- 核心：通用的双向冲量自适应模板。
- 启动时会按币价、tick、点差和最小下单约束自动重设基础 `step_price` 与 `per_order_notional`，并在急波动和持续单边时动态放大步长、收紧仓位。
- 更适合拿来快速试一个“能工作”的 burst 型通用框架；如果你要复用 `114/150` 那套固定节奏，应该从 `synthetic_neutral_bard_style_v1` 出发，而不是从这套自适应模板出发。

### 迁移 `synthetic_neutral_bard_style_v1` 到其他币种时先改什么

- `symbol` 和四个输出路径：避免和原币种共用 state / plan / submit / summary 文件。
- `step_price`：先确认最小 tick、常态点差和盘口密度，别机械照搬 `0.0007`。
- `per_order_notional`：至少要覆盖最小下单名义，还要和盘口深度匹配，避免一笔过大把库存抬太快。
- `pause_buy_position_notional` / `max_position_notional` / `pause_short_position_notional` / `max_short_position_notional`：这四个阈值要按你愿意承受的多空库存重算，不建议直接套用。
- 如果币种波动更大或盘口更稀，优先先放宽 `step_price`，而不是先把层数和仓位上限抬高。

## 额外说明

### `autotune_symbol_enabled`

如果开启，启动前 web 端会按币种最小 tick、盘口 spread、最小成交额去修正：

- `step_price`
- `per_order_notional`
- `base_position_notional`
- `startup_entry_multiplier` 不会被自动改写，但它实际对应的首轮大单名义会跟着 `per_order_notional` 一起变化。

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
