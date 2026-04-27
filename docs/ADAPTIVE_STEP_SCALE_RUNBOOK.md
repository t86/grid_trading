# Adaptive Step / Scale Runbook

本文记录 2026-04-27 在 114 服务器上为 BTCUSDC、ETHUSDC 调整 step/scale 的处理过程。目标是让其他线程、其他电脑继续运行项目时，不需要翻聊天记录，也能理解当前参数、诊断口径和后续调参方法。

## 生产上下文

- 服务器：`ecs-114`
- 生产目录：`/home/ubuntu/wangge`
- 监控台：`http://43.155.163.114:8788/monitor`
- 部署方式：本地提交并推送 `main`，生产机执行 `/usr/local/bin/grid-web-update`
- runner 启停：统一使用 `/usr/local/bin/grid-saved-runner`

生产约束：

- 不要用 `scp`、`rsync`、手工 `cp` 热更生产代码。
- 不要手敲 `nohup python -m grid_optimizer.loop_runner ...` 启动策略。
- saved runner 需要通过 `/usr/local/bin/grid-saved-runner restart SYMBOL` 重启，确保 cwd、env、pid、control 文件归属一致。
- 修改 `output/*_loop_runner_control.json` 前先备份；修改后重启对应 symbol。

## 概念

`step_price` 是基础步长，也就是低波动时的最小挂单间距。

`adaptive_step_max_scale` 是最大放大倍数。实际步长大致为：

```text
effective_step_price = step_price * min(raw_scale, adaptive_step_max_scale)
```

当 `raw_scale <= 1` 时，实际步长保持 `step_price`，不会缩小到比基础步长更低。因此如果希望稳定行情下更贴近盘口，应该把 `step_price` 设置成稳定行情可接受的步长；如果希望波动时拉开距离，就调低触发阈值或调高 `adaptive_step_max_scale`。

`raw_scale` 来自多个短周期波动指标的最大值：

- `30s_abs_return_ratio / adaptive_step_30s_abs_return_threshold`
- `30s_amplitude_ratio / adaptive_step_30s_amplitude_threshold`
- `1m_abs_return_ratio / adaptive_step_1m_abs_return_threshold`
- `1m_amplitude_ratio / adaptive_step_1m_amplitude_threshold`
- `3m_abs_return_ratio / adaptive_step_3m_abs_return_threshold`
- `5m_abs_return_ratio / adaptive_step_5m_abs_return_threshold`

其中：

- `abs_return_ratio = abs(close / open - 1)`
- `amplitude_ratio = high / low - 1`

调参目标是让波动和步长保持正相关：稳定时贴近盘口跑量，波动放大时自动拉开 step，避免低位卖多仓、高位平空仓。

## 当前生产参数

### BTCUSDC

当前使用防守版微网格参数，重点是先控损：

- `step_price = 30`
- `adaptive_step_max_scale = 1.5`，理论最大 step 约 `45`
- `buy_levels = 2`
- `sell_levels = 2`
- `per_order_notional = 240`
- `pause_buy_position_notional = 1200`
- `pause_short_position_notional = 1200`
- `max_position_notional = 1600`
- `max_short_position_notional = 1600`
- `max_total_notional = 3200`
- `max_actual_net_notional = 1200`

当前阈值：

- `adaptive_step_30s_abs_return_threshold = 0.001`
- `adaptive_step_30s_amplitude_threshold = 0.0018`
- `adaptive_step_1m_abs_return_threshold = 0.0015`
- `adaptive_step_1m_amplitude_threshold = 0.0025`
- `adaptive_step_3m_abs_return_threshold = 0.0035`
- `adaptive_step_5m_abs_return_threshold = 0.005`

### ETHUSDC

当前使用比 BTC 更宽的 ETH step：

- `step_price = 2`
- `adaptive_step_max_scale = 2`，理论最大 step 约 `4`
- `buy_levels = 3`
- `sell_levels = 3`
- `per_order_notional = 85`
- `pause_buy_position_notional = 650`
- `pause_short_position_notional = 650`
- `max_position_notional = 850`
- `max_short_position_notional = 850`

当前阈值：

- `adaptive_step_30s_abs_return_threshold = 0.0012`
- `adaptive_step_30s_amplitude_threshold = 0.002`
- `adaptive_step_1m_abs_return_threshold = 0.0018`
- `adaptive_step_1m_amplitude_threshold = 0.003`
- `adaptive_step_3m_abs_return_threshold = 0.004`
- `adaptive_step_5m_abs_return_threshold = 0.006`

## 这次排查结论

BTC、ETH 最近高损耗主要不是手续费问题：

- 成交均为 maker。
- USDC 交易对挂单手续费和 funding 在统计窗口内为 0。
- 损耗主要来自库存被震荡打穿后的反向兑现：低位卖多仓、高位回补空仓。

BTC 在调整前的一个窗口表现：

- 近 60 分钟成交额约 `104,627.9U`
- realized PnL 约 `-9.5825U`
- 损耗约 `0.916U / 10,000U`
- 其中近 20 分钟恶化到约 `1.739U / 10,000U`

当时看到的核心问题：

- step 相对波动偏小。
- BTC 单笔较大时，库存来回穿越导致亏损集中。
- adaptive step 没有明显触发，因为阈值相对实际 30s/1m 波动仍偏高。

历史重构看到的 raw scale 示例：

- BTC 最大 raw scale 约 `0.8005`，未达到 `1`，所以没有放大 step。
- ETH 最大 raw scale 约 `0.6951`，未达到 `1`，所以没有放大 step。

因此后续不要只看“肉眼觉得波动大”，要看日志里每个窗口的 `return/amplitude` 和对应 threshold 的比例。

## 日志字段

从提交 `056d5f6` 开始，runner 每轮事件日志会写入完整 adaptive step 数据。

日志文件：

- `output/btcusdc_loop_events.jsonl`
- `output/ethusdc_loop_events.jsonl`

关键字段：

- `adaptive_step`
- `adaptive_step_enabled`
- `adaptive_step_active`
- `adaptive_step_base_step_price`
- `adaptive_step_effective_step_price`
- `adaptive_step_scale`
- `adaptive_step_raw_scale`
- `adaptive_step_per_order_scale`
- `adaptive_step_position_limit_scale`
- `adaptive_step_dominant_window`
- `adaptive_step_dominant_metric`
- `adaptive_step_dominant_value`
- `adaptive_step_dominant_threshold`
- `adaptive_step_reason`
- `adaptive_step_history_count`
- `adaptive_step_window_30s_abs_return_ratio`
- `adaptive_step_window_30s_amplitude_ratio`
- `adaptive_step_window_1m_abs_return_ratio`
- `adaptive_step_window_1m_amplitude_ratio`
- `adaptive_step_window_3m_abs_return_ratio`
- `adaptive_step_window_5m_abs_return_ratio`

快速查看最新状态：

```bash
ssh -F ~/.ssh/config ecs-114 'cd /home/ubuntu/wangge && .venv/bin/python - << "PY"
import json
from pathlib import Path

for symbol in ("btcusdc", "ethusdc"):
    path = Path(f"output/{symbol}_loop_events.jsonl")
    line = path.read_text().strip().splitlines()[-1]
    event = json.loads(line)
    print(symbol.upper())
    print("  effective_step:", event.get("adaptive_step_effective_step_price"))
    print("  raw_scale:", event.get("adaptive_step_raw_scale"))
    print("  active:", event.get("adaptive_step_active"))
    print("  dominant:", event.get("adaptive_step_dominant_window"), event.get("adaptive_step_dominant_metric"))
    print("  dominant_value:", event.get("adaptive_step_dominant_value"))
    print("  dominant_threshold:", event.get("adaptive_step_dominant_threshold"))
PY'
```

## 标准调参流程

1. 查看 runner 状态。

```bash
ssh -F ~/.ssh/config ecs-114 '/usr/local/bin/grid-saved-runner status BTCUSDC; /usr/local/bin/grid-saved-runner status ETHUSDC'
```

2. 备份 control JSON。

```bash
ssh -F ~/.ssh/config ecs-114 'cd /home/ubuntu/wangge && cp output/btcusdc_loop_runner_control.json output/btcusdc_loop_runner_control.json.bak_step_scale_$(date +%Y%m%d_%H%M%S)'
```

3. 用结构化 JSON 修改参数，不要手工字符串替换。

```bash
ssh -F ~/.ssh/config ecs-114 'cd /home/ubuntu/wangge && .venv/bin/python - << "PY"
import json
from pathlib import Path

path = Path("output/btcusdc_loop_runner_control.json")
data = json.loads(path.read_text())
data["step_price"] = 30
data["adaptive_step_enabled"] = True
data["adaptive_step_max_scale"] = 1.5
path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")
PY'
```

4. 通过 saved runner 重启。

```bash
ssh -F ~/.ssh/config ecs-114 '/usr/local/bin/grid-saved-runner restart BTCUSDC'
```

5. 验证实际命令、最新 plan、最新 event。

```bash
ssh -F ~/.ssh/config ecs-114 'ps -eo pid,args | grep -i "grid_optimizer.loop_runner" | grep BTCUSDC | grep -v grep'
ssh -F ~/.ssh/config ecs-114 'cd /home/ubuntu/wangge && tail -n 1 output/btcusdc_semi_auto_plan.json'
ssh -F ~/.ssh/config ecs-114 'cd /home/ubuntu/wangge && tail -n 1 output/btcusdc_loop_events.jsonl'
```

## 调参判断

如果损耗高但 taker 为 0、commission 为 0，优先判断为库存磨损，而不是手续费问题。

如果 `adaptive_step_raw_scale < 1`，但是行情已经明显打穿库存，说明阈值太钝或基础 step 太小：

- 想立刻控损：先提高 `step_price`。
- 想保留低波动跑量：降低对应窗口 threshold，让 scale 更早触发。
- 想限制极端波动：提高 `adaptive_step_max_scale`，但同时降低 levels 或仓位上限。

如果库存已经接近 pause 或 max：

- 降低 `pause_*_position_notional`，让策略更早停止继续加仓。
- 降低 `max_*_position_notional` 和 `max_actual_net_notional`。
- 减少 levels，避免同方向多层订单继续叠库存。

如果 step 放大后成交太少：

- 优先小幅增加 levels 或 per-order，而不是直接把 step 降回很小。
- 观察 `loss_per_10k_notional` 是否随成交恢复而恶化。
- 如果成交恢复但损耗也同步变高，说明 step 仍不够或库存阈值太松。

## 后续数据分析方向

后续应该按 5 分钟或 10 分钟分桶记录并比较：

- `adaptive_step_window_30s_amplitude_ratio`
- `adaptive_step_window_1m_amplitude_ratio`
- `adaptive_step_window_3m_abs_return_ratio`
- `adaptive_step_effective_step_price`
- 成交笔数
- gross notional
- realized PnL
- loss per 10k notional
- 当前 long/short/net inventory
- 是否 active delever

目标是验证：

- 低波动时 step 低、成交高、库存低。
- 高波动时 step 自动放大、成交下降但损耗明显收敛。
- BTC 的 step/scale 曲线应比 ETH 更保守，因为 BTC 单价高、短时跳动对库存磨损更明显。

建议先用这些目标档位做回看：

- BTC：稳定 `15`，普通波动 `30`，较高波动 `45`
- ETH：稳定 `2`，普通波动 `3`，较高波动 `4`

如果日志显示大多数亏损窗口的 `raw_scale` 仍小于 `1`，下一轮优先降低 threshold，而不是继续只靠手工提高基础 step。
