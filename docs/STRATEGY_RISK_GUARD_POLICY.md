# Strategy Risk Guard Policy

新开合约刷量策略必须先通过启动前风控校验。策略的默认目标可以是刷量，但在快速波动时，系统必须先保护仓位和损耗。

## 硬准则

1. 快速波动必须第一时间停止加仓。
   - 必须开启 `volatility_entry_pause_enabled`。
   - 必须配置 30s 和 1m 的涨跌幅阈值：`volatility_entry_pause_30s_abs_return_ratio`、`volatility_entry_pause_1m_abs_return_ratio`。
   - 必须配置 30s 和 1m 的振幅阈值：`volatility_entry_pause_30s_amplitude_ratio`、`volatility_entry_pause_1m_amplitude_ratio`。

2. 触发快速波动暂停后只能减仓，不能继续加仓。
   - LONG 侧不得继续新增 `entry_long` / bootstrap 买入。
   - SHORT 侧不得继续新增 `entry_short` / bootstrap_short 卖出。
   - 已有仓位允许保留 reduce-only 止盈单和保护性减仓单。

3. 单向做多和中性策略必须有下跌暂停买入。
   - 必须配置 `buy_pause_amp_trigger_ratio > 0`。
   - 必须配置 `buy_pause_down_return_trigger_ratio < 0`。
   - 下跌达到阈值时，策略只能卖出减多或等待，不能继续接多仓。

4. 极端波动必须暂停交易或减到安全仓位。
   - 必须开启 `volatility_trigger_enabled`。
   - 必须配置 `volatility_trigger_abs_return_ratio > 0` 和 `volatility_trigger_amplitude_ratio > 0`。
   - 必须开启 `volatility_trigger_stop_cancel_open_orders`。
   - 必须配置 `volatility_trigger_stop_close_all_positions`，或配置 `volatility_trigger_stop_reduce_to_notional > 0`。

5. 所有可开仓策略必须有事后减仓保护。
   - LONG 必须有 `exposure_escalation` 和 `hard_loss_forced_reduce`。
   - SHORT 必须有 `adverse_reduce`、超时减仓和 `hard_loss_forced_reduce`。
   - 中性策略必须同时具备 long/short 两侧 `adverse_reduce` 和 hard loss。

## 推荐基线

普通刷量策略的初始阈值建议：

```text
volatility_entry_pause_30s_abs_return_ratio = 0.0015
volatility_entry_pause_30s_amplitude_ratio = 0.0025
volatility_entry_pause_1m_abs_return_ratio = 0.0025
volatility_entry_pause_1m_amplitude_ratio = 0.0035

buy_pause_down_return_trigger_ratio = -0.0015
buy_pause_amp_trigger_ratio = 0.0025-0.0030

volatility_trigger_abs_return_ratio = 0.025
volatility_trigger_amplitude_ratio = 0.04
volatility_trigger_stop_cancel_open_orders = true
volatility_trigger_stop_reduce_to_notional = 5.0
```

品种波动更大的时候，可以把暂停阈值略放宽，但不能关闭这些保护。任何为了刷量临时关闭快速暂停或极端波动停机的配置，都不能作为生产策略启动。
