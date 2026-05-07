# 通用实时多周期方向层设计

## 目标

把当前 `1m / 15m / 1h / 4h` 实时方向层做成 futures 合约 runner 的通用能力。它要保留 CHIP 当前已经验证有效的逻辑，同时去掉只能用于 `synthetic_neutral` 的限制，让同一套方向信号可以安全适配其它 futures 策略模式。

目标覆盖模式：

- `synthetic_neutral`
- `one_way_long`
- `one_way_short`
- `competition_inventory_grid`

现货 runner 暂不纳入本次设计。

## 当前状态

`src/grid_optimizer/multi_timeframe_bias.py` 已经有一个纯信号模块：

- 读取最近闭合的 `1m`、`15m`、`1h`、`4h` K 线窗口。
- 计算区间位置、趋势、多头偏向、空头偏向、1m 冲击状态。
- 可以调整层数、步长、单笔名义金额、仓位上限，以及买卖偏移。

`loop_runner.py` 已经把这个模块接入 futures 计划生成、plan 报告、event 摘要、命令行参数、校验，以及 saved-runner 命令构建。当前仍有一条校验限制：启用方向层时必须是 `--strategy-mode synthetic_neutral`。

## 设计

保留多周期信号作为唯一共享信号源，然后新增“策略模式适配层”。

信号阶段保持策略无关：

- 仅在启用时拉取闭合的 `1m`、`15m`、`1h`、`4h` K 线窗口。
- 计算 `zone_score`、`trend_score`、`long_bias_score`、`short_bias_score`、`direction_score`、`shock_active`。
- 即使关闭或数据不可用，也返回完整 report，方便 plan/event 统一展示。

适配阶段按策略模式映射信号：

- `synthetic_neutral`：保持当前 CHIP 行为。低位偏买开多，高位偏卖开空，多空两侧仓位上限可以反向缩放。
- `one_way_long`：低位可以增加买入层数、单笔名义金额和多头上限；高位降低追多强度、把买入入口放远，并保留原有止盈/减仓逻辑。该模式绝不创建空头敞口。
- `one_way_short`：高位可以增加卖空层数、单笔名义金额和空头上限；低位降低追空强度、把卖空入口放远。该模式绝不创建多头敞口。
- `competition_inventory_grid`：只调整买卖网格密度、偏移和库存上下限，不改变库存网格策略本身的平衡结构。这个适配器应偏保守，避免破坏竞赛刷量所需的双侧节奏。

新增 CLI/config 字段：

- `multi_timeframe_bias_mode_adapter`
- 默认值：`auto`
- 允许值：`auto`、`synthetic_neutral`、`one_way_long`、`one_way_short`、`inventory_grid`

`auto` 根据 `strategy_mode` 自动解析。显式值用于调试或生产分阶段灰度。

## 安全规则

- 默认保持关闭。
- 现有 saved config 不启用 `multi_timeframe_bias_enabled=true` 时行为不变。
- 冲击行情只允许降强度或加宽 step，不能放大单笔、层数或仓位上限。
- 单向适配器绝不创建反向敞口。
- K 线不可用或异常时，返回基础参数，并标记 `applied=false`。
- 现有损耗保护、波动保护、止盈保护、逆势减仓、硬亏损强制减仓继续拥有最终约束权。
- 对 adapter/mode 兼容性做校验，不支持的组合直接 fail fast。

## 数据流

1. 解析 runner config，并解析有效策略模式。
2. 构建 `MultiTimeframeBiasConfig`，包含 adapter 模式。
3. 启用时只拉取 futures 闭合的 `1m / 15m / 1h / 4h` K 线。
4. 解析共享信号 report。
5. 从 `auto` 或显式配置解析 adapter。
6. 在 plan 生成前，把模式适配后的调整应用到 effective planning 参数。
7. 在 plan JSON 和 event JSONL 中写入信号 report、adapter 名称和 adjustment report。

## 报告字段

plan 和 event 摘要需要暴露：

- `multi_timeframe_bias.enabled`
- `multi_timeframe_bias.available`
- `multi_timeframe_bias.applied`
- `multi_timeframe_bias.adapter`
- `multi_timeframe_bias.regime`
- `multi_timeframe_bias.zone_score`
- `multi_timeframe_bias.long_bias_score`
- `multi_timeframe_bias.short_bias_score`
- `multi_timeframe_bias.direction_score`
- `multi_timeframe_bias.shock_active`
- `multi_timeframe_bias.adjustments`

现有扁平 event 字段保留，兼容 dashboard 和脚本。

## 测试

纯模块测试覆盖：

- `synthetic_neutral` 低位偏多。
- `synthetic_neutral` 高位偏空。
- `one_way_long` 低位加速、高位降风险。
- `one_way_short` 高位加速、低位降风险。
- `competition_inventory_grid` 适配器保留双侧，并只做保守偏移。
- 冲击状态降低名义金额或加宽 step，不增加风险。
- K 线不可用时返回基础值并标记 `applied=false`。

Runner 测试覆盖：

- parser 接受新的 adapter 参数。
- 校验拒绝不支持的 adapter 值或不兼容的显式 adapter。
- `one_way_long` 可以启用多周期方向层，不再触发旧的 synthetic-only 失败。
- `one_way_short` 可以启用多周期方向层。
- saved-runner command 包含 adapter 参数。
- plan/event 摘要包含 adapter 和 adjustments。

## 上线节奏

1. 代码默认关闭。
2. CHIP 继续使用 `synthetic_neutral` adapter，先确认无行为回归。
3. 选择一个低风险 `one_way_long` 合约，用保守参数启用。
4. 选择一个 `one_way_short` 合约，用保守参数启用。
5. 单向模式稳定观察 30 到 60 分钟后，再选一个 `competition_inventory_grid` 合约启用。

生产上线继续使用服务器 pull-based deploy 和 saved-runner restart。不要在服务器上复制文件或做临时热补丁。
