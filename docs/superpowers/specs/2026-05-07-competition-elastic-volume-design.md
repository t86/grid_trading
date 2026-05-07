# 合约交易赛弹性刷量策略与实时监控设计

## 目标

新增 `competition_elastic_volume_v1`，用于合约交易赛中按指定合约真实成交额刷量，同时把损耗控制在可接受预算内。比赛规则只统计指定合约真实成交额，没有盈利要求；策略目标不是预测方向盈利，而是在真实对手盘成交前提下最大化有效成交速度。

核心目标函数：

```text
maximize gross_notional_per_hour

subject to:
  loss_per_10k_notional <= configured_budget
  actual_net_notional <= hard_limit
  synthetic_drift_notional <= hard_limit
  maker_ratio >= expected_threshold
  run only inside competition window
```

策略不得设计自成交、多账户对敲或规避交易所 STP 的行为。所有成交必须来自正常订单簿真实对手盘。

## 范围

第一版复用现有 `loop_runner`、`synthetic_neutral`、ping-pong 预设、`adaptive_step`、`multi_timeframe_bias`、runtime guards、audit 文件和 Web 监控框架。

第一版不做跨服务器聚合。每台服务器运行自己的本地监控页面，页面只读取本机 runner、audit 和 event 文件。页面顶部显示当前服务器名作为只读信息，并提供本机正在运行币种的下拉框。

跨服聚合留到后续版本，避免第一版引入额外鉴权、网络可达性、跨服数据一致性和操作风险。

## 控制层架构

新增纯模块：

```text
src/grid_optimizer/competition_elastic_volume.py
```

输入：

- runner 基础配置
- 当前行情快照
- `adaptive_step` 指标
- `multi_timeframe_bias` 报告
- 当前账户持仓
- 最近成交审计和 income 审计
- 比赛窗口、已成交额、目标成交额和剩余时间
- 上一轮 elastic state

输出：

```json
{
  "regime": "sprint",
  "last_regime": "cruise",
  "reasons": ["low_loss", "low_volatility"],
  "step_scale": 0.8,
  "per_order_scale": 1.25,
  "levels_scale": 1.25,
  "position_limit_scale": 1.0,
  "entry_allowed": true,
  "allow_entry_long": true,
  "allow_entry_short": true,
  "allow_reduce_long": true,
  "allow_reduce_short": true,
  "cooldown_until": null,
  "metrics": {}
}
```

`loop_runner` 在生成 plan 前应用输出的 effective 参数，然后继续调用现有 plan builder。`elastic_volume_enabled=false` 时，现有策略行为必须完全不变。

## 状态机

状态：

```text
sprint
cruise
defensive
recover
cooldown
```

含义：

- `sprint`：低波动、低损耗、库存轻，主动提高成交速度。
- `cruise`：常态刷量。
- `defensive`：波动、库存或损耗任一指标变差，主动降速。
- `recover`：库存偏移明显，禁止增加风险方向，优先 maker 减库存。
- `cooldown`：极端行情或亏损预算击穿，停止新增开仓。

第一版使用确认周期避免状态抖动。进入防守状态可以单轮触发；从 `defensive` 或 `recover` 回到 `cruise` 需要连续满足恢复条件。`cooldown` 至少保持配置的冷却时间。

## 指标

滚动窗口至少计算：

```text
gross_notional_5m
gross_notional_15m
competition_gross_notional
net_pnl_5m
net_pnl_15m
competition_net_pnl
commission_5m
commission_15m
competition_commission
funding_fee
loss_per_10k_5m
loss_per_10k_15m
maker_ratio
actual_net_notional
long_notional
short_notional
inventory_ratio
adaptive_step_raw_scale
adaptive_step_dominant_metric
multi_timeframe_bias.regime
competition_required_pace
competition_actual_pace
```

损耗口径：

```text
loss_per_10k = max(-net_pnl, 0) / max(gross_notional, 1e-12) * 10000
```

`net_pnl` 包含 realized PnL、稳定币手续费、资金费。可复用 `runtime_guards.py` 中已有的 audit 汇总路径和 competition board 开始时间解析。

## 状态切换

默认配置：

```json
{
  "elastic_loss_per_10k_sprint": 0.3,
  "elastic_loss_per_10k_cruise": 0.8,
  "elastic_loss_per_10k_defensive": 1.2,
  "elastic_loss_per_10k_cooldown": 1.8,
  "elastic_inventory_soft_ratio": 0.6,
  "elastic_inventory_hard_ratio": 0.9,
  "elastic_adaptive_raw_scale_defensive": 1.2,
  "elastic_adaptive_raw_scale_cooldown": 2.0,
  "elastic_recover_confirm_cycles": 3,
  "elastic_cooldown_seconds": 120
}
```

切换规则：

- `sprint`：`loss_per_10k_15m` 不高于 sprint 阈值，`adaptive_step_raw_scale < 0.8`，库存低于硬上限 35%，且比赛成交速度需要追赶或处于低风险窗口。
- `cruise`：`loss_per_10k_15m` 不高于 cruise 阈值，库存低于软阈值，无极端波动。
- `defensive`：`loss_per_10k_15m` 高于 cruise 阈值，或 `adaptive_step_raw_scale` 达到 defensive 阈值，或库存达到软阈值。
- `recover`：库存达到软阈值且偏向单边，或短窗口损耗恶化同时库存偏高。
- `cooldown`：`loss_per_10k_15m` 达到 cooldown 阈值，或库存达到硬阈值，或 `adaptive_step_raw_scale` 达到 cooldown 阈值，或 1m 极端振幅/涨跌触发。

库存和硬风控优先级高于多周期 bias。若 bias 鼓励的方向会增加已有风险敞口，必须降级或禁止。

## 参数弹性

状态参数缩放：

```text
sprint:
  step_scale = 0.8
  per_order_scale = 1.25
  levels_scale = 1.25
  position_limit_scale = 1.0

cruise:
  step_scale = 1.0
  per_order_scale = 1.0
  levels_scale = 1.0
  position_limit_scale = 1.0

defensive:
  step_scale = 1.5-2.5
  per_order_scale = 0.5-0.8
  levels_scale = 0.5-0.8
  position_limit_scale = 0.6-0.8

recover:
  step_scale = 1.2-2.0
  per_order_scale = 0.5
  risk-increasing entry = disabled
  reduce side = enabled

cooldown:
  entry_allowed = false
  reduce_only = true
  cancel stale entry orders when configured
```

需要支持方向开关：

```text
allow_entry_long
allow_entry_short
allow_reduce_long
allow_reduce_short
```

如果第一版 plan builder 暂时不能完整表达方向开关，可以先通过 effective `buy_levels`、`sell_levels` 和订单 role 过滤近似实现；后续版本补齐 role 级过滤。

## 配置

新增配置字段：

```json
{
  "elastic_volume_enabled": true,
  "elastic_volume_mode": "competition_elastic_volume_v1",
  "elastic_eval_window_seconds": 300,
  "elastic_loss_per_10k_sprint": 0.3,
  "elastic_loss_per_10k_cruise": 0.8,
  "elastic_loss_per_10k_defensive": 1.2,
  "elastic_loss_per_10k_cooldown": 1.8,
  "elastic_inventory_soft_ratio": 0.6,
  "elastic_inventory_hard_ratio": 0.9,
  "elastic_step_scale_sprint": 0.8,
  "elastic_step_scale_defensive": 1.8,
  "elastic_step_scale_cooldown": 3.0,
  "elastic_per_order_scale_sprint": 1.25,
  "elastic_per_order_scale_defensive": 0.65,
  "elastic_levels_scale_sprint": 1.25,
  "elastic_levels_scale_defensive": 0.65,
  "elastic_cooldown_seconds": 120,
  "elastic_state_confirm_cycles": 3,
  "elastic_cancel_stale_entries_on_cooldown": true
}
```

## 日志

每轮 event summary 增加：

```json
{
  "elastic_volume": {
    "enabled": true,
    "regime": "defensive",
    "last_regime": "cruise",
    "reasons": ["loss_per_10k_15m", "inventory_soft"],
    "loss_per_10k_5m": 1.1,
    "loss_per_10k_15m": 0.9,
    "competition_gross_notional": 1820000.0,
    "competition_net_pnl": -132.4,
    "competition_commission": 0.0,
    "gross_notional_15m": 118240.0,
    "inventory_ratio": 0.42,
    "step_scale": 1.6,
    "per_order_scale": 0.65,
    "levels_scale": 0.6,
    "entry_allowed": true
  }
}
```

## 实时监控页面

新增本地监控页面或扩展现有 Web 监控页。第一版部署在每台服务器本机，读取本机文件，不跨服拉取。

顶部控件：

- 当前服务器：只读标签，来自 hostname 或配置。
- 币种下拉：从本机正在运行的 runner/control/status 文件解析。
- 比赛窗口：显示开始、结束和剩余时间。
- 刷新间隔：默认 2 秒。

KPI：

- 比赛周期累计成交额
- 比赛周期总盈亏
- 比赛周期手续费
- 15m 成交额
- `loss_per_10k`
- 当前 elastic regime
- 当前库存比例
- maker ratio

实时成交列表字段：

```text
time
symbol
side
price
qty
notional
fee
realized_pnl
maker/taker
order_id
client_order_id
position_side
```

图表：

- 成交价格曲线
- 比赛周期累计成交额
- 比赛周期累计总盈亏
- 比赛周期累计手续费

第一版使用同一时间轴联动展示，可切换 `5m / 15m / 比赛周期`。如果信息密度过高，后续拆成上下多图，但仍保持同一时间轴。

API 建议：

```text
GET /api/competition-volume/symbols
GET /api/competition-volume/summary?symbol=CHIPUSDT
GET /api/competition-volume/fills?symbol=CHIPUSDT&limit=200
GET /api/competition-volume/chart?symbol=CHIPUSDT&window=competition
```

所有接口只读。第一版不在该页面提供启动、停止、平仓、改参数等写操作。

## 测试

纯模块测试：

- 低损耗低波动进入 `sprint`。
- 正常损耗进入 `cruise`。
- `loss_per_10k` 恶化进入 `defensive`。
- 库存超过软阈值进入 `recover`。
- 库存超过硬阈值进入 `cooldown`。
- `adaptive_step_raw_scale` 极端进入 `cooldown`。
- `cooldown` 时间未过时保持 `cooldown`。
- 连续恢复 N 轮后从 `defensive` 回到 `cruise`。
- 低位 bias 增强 buy-long。
- 高位 bias 增强 sell-short。
- 库存与 bias 冲突时库存优先。

集成测试：

- `loop_runner` 应用 effective 参数后生成 plan。
- `elastic_volume_enabled=false` 时现有行为不变。
- event summary 写出 `elastic_volume`。
- runtime guard 触发时优先停止交易。
- Web summary API 能汇总比赛周期成交额、盈亏和手续费。
- Web fills API 能返回最近成交列表。
- Web chart API 能返回同一时间轴的价格、累计成交额、累计盈亏和累计手续费。

## 上线步骤

1. 实现纯模块和单元测试。
2. 用已有 audit jsonl 做本地回放，比较不同阈值下的状态切换。
3. 先接入 Web 只读监控页面，确认成交额、盈亏和手续费口径正确。
4. 在小资金配置启用 `elastic_volume_enabled=true`。
5. 第一小时禁用 `sprint`，只允许 `cruise/defensive/recover/cooldown`。
6. 确认 `loss_per_10k` 稳定后再打开 `sprint`。
7. 每 15 分钟观察成交额、`loss_per_10k`、库存比例、状态切换次数和 maker ratio。

## 后续版本

- 跨服务器聚合监控。
- WebSocket 推送实时成交，而不是轮询。
- 图表支持成交点点击后定位到成交列表。
- 按 order role 分析损耗，区分 entry、reduce、flatten。
- 自动参数建议，根据最近窗口反推更合适的 loss budget、step scale 和 position limit。
