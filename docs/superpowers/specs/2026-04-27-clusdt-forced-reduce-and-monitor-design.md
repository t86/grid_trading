# CLUSDT 强制减仓接线与监控高亮设计

## 背景

`CLUSDT` 当前已经在线启用了两类库存压降保护：

- `threshold_target_reduce`
- `adverse_inventory_reduce`

这两类保护都支持先走 maker 减仓，超时后升级为 aggressive，从而允许直接吃盘。

但当前还有两个明显缺口：

1. `hard_loss_forced_reduce_enabled` 只是控制文件字段，还没有完整接入 `web.py -> loop_runner parser -> plan/summary` 链路。
2. `/monitor` 虽然能看到部分摘要字段，但无法直观看到 `threshold_target_reduce`、`adverse_inventory_reduce`、`hard_loss_forced_reduce` 的状态、超时、是否 aggressive、以及是否已经生成 `forced_reduce_orders`。

用户目标不是重写整个风控系统，而是在不打乱现有 `CLUSDT` 在线逻辑的前提下，补齐 hard loss 开关接线，并把 3 条减仓保护在监控页里明确展示出来。

## 目标

- 让 `hard_loss_forced_reduce_enabled` 成为真实可用的 runner 配置。
- 保持现有执行语义：
  - `threshold_target_reduce` 先 maker，超时后直接 taker。
  - `adverse_inventory_reduce` 先 maker，超时后直接 taker。
  - `hard_loss_forced_reduce` 触发时允许直接 taker 强制减仓。
- 在 `/monitor` 高亮展示 3 条减仓保护的当前状态。
- 部署到 `114` 后，继续沿用当前 `CLUSDT` runner 做实跑验证。

## 非目标

- 不合并 3 条保护为一个统一总状态机。
- 不重写 `forced_reduce` 下单模型。
- 不改动 `CLUSDT` 当前基础网格参数（`per_order_notional`、`step_price`、levels 等）。
- 不在这次工作中修改 Binance API 或订单 diff 主流程。

## 方案选项

1. **最小补线方案（推荐）**
   - 只补 `hard_loss_forced_reduce_enabled` 配置链路。
   - 复用现有 `forced_reduce` 订单结构和执行器。
   - 在 `/monitor` 上增加针对 3 条保护的专门状态块。
   - 优点：改动面最小，最适合当前线上 runner。

2. **统一 forced reduce 状态层**
   - 先把 `threshold/adverse/hard_loss` 抽象成统一状态对象，再统一渲染和执行。
   - 优点：结构更整齐。
   - 缺点：会碰更多 plan 和 summary 逻辑，线上回归风险偏高。

3. **只做监控展示，不补 hard loss**
   - 只增强 UI，不碰策略执行。
   - 优点：最稳。
   - 缺点：用户明确要求 hard loss 真正生效，不满足目标。

推荐方案：1。当前需求是让线上 `CLUSDT` 可控可见，不值得为了形式统一而扩大改动面。

## 设计

### 1. `hard_loss_forced_reduce_enabled` 接线

补齐以下链路：

- `RUNNER_DEFAULT_CONFIG` / 相关 preset 默认值
- `_normalize_runner_control_payload()`
- `_build_runner_command()`
- `loop_runner` parser
- `generate_plan_report()` 的报告字段
- `summary jsonl` 摘要字段

要求：

- 如果配置里显式开启，runner 命令行中必须出现 `--hard-loss-forced-reduce-enabled`
- 如果关闭，则必须出现 `--no-hard-loss-forced-reduce-enabled`
- 相关状态必须进入 `plan_json` 和 `summary_jsonl`，供 `/monitor` 读取

### 2. hard loss 执行语义

本次不发明新执行器，直接沿用现有 `forced_reduce` 语义：

- `threshold_target_reduce`
  - 初始 `post_only / GTX`
  - 超过 `threshold_reduce_taker_timeout_seconds` 后切 `aggressive / GTC`
- `adverse_inventory_reduce`
  - 初始 `post_only / GTX`
  - 超过 `adverse_reduce_maker_timeout_seconds` 后切 `aggressive / GTC`
- `hard_loss_forced_reduce`
  - 触发后允许直接进入 aggressive 路径
  - 订单继续标记 `role = "forced_reduce"` 和 `reduce_only`

这里的关键不是新增复杂逻辑，而是确保 hard loss 的配置真的能把已有强制减仓路径打开。

### 3. `/monitor` 风控高亮

在监控页补一个独立的“减仓保护”展示块，至少显示：

- `threshold_target_reduce`
  - `enabled`
  - `active`
  - `long_active / short_active`
  - `elapsed_seconds`
  - `taker_timeout_seconds`
  - `*_taker_timeout_active`
  - `target_notional`
  - `reduce_notional`
  - `placed_reduce_orders`
- `adverse_inventory_reduce`
  - `enabled`
  - `active`
  - `direction`
  - `blocked_reason`
  - `long_adverse_ratio / short_adverse_ratio`
  - `trigger_ratio`
  - `maker_timeout_seconds`
  - `*_elapsed_seconds`
  - `*_aggressive`
  - `forced_reduce_orders`
- `hard_loss_forced_reduce`
  - `enabled`
  - `active`
  - `reason`
  - `blocked_reason`
  - `forced_reduce_orders`

展示原则：

- 激活状态优先显示，避免埋在 JSON 里
- 数值尽量中文标签化
- aggressive / taker 超时状态要显眼

### 4. 114 验证

部署后必须验证：

- `CLUSDT` runner 命令行含预期参数
- `/home/ubuntu/wangge/output/clusdt_loop_runner_control.json` 与命令行一致
- `plan_json` 中能看到 3 条保护的状态对象
- `/monitor` 能高亮显示这些状态
- 至少确认一轮 summary/event 写入正常，没有因为新增字段导致序列化或页面渲染报错

## 影响文件

- `src/grid_optimizer/web.py`
- `src/grid_optimizer/loop_runner.py`
- `src/grid_optimizer/monitor.py` 或 `web.py` 中 `/api/loop_monitor` 组装逻辑
- `tests/test_web_security.py`
- `tests/test_loop_runner.py`
- 可能新增一小部分 monitor 相关测试

## 测试

需要新增或补齐以下测试：

- `web.py`
  - `hard_loss_forced_reduce_enabled` 能被规范化
  - `hard_loss_forced_reduce_enabled` 能进入 runner command
- `loop_runner.py`
  - parser 能接受 `--hard-loss-forced-reduce-enabled`
  - summary/plan 中能正确带出 hard loss 状态
  - 现有 `threshold/adverse` aggressive 超时测试保持通过
- monitor/web API
  - `/api/loop_monitor` 返回值中包含 3 条保护摘要
  - 页面渲染使用这些字段时不会因缺失字段报错

## 风险与控制

- 风险：把 hard loss 配置接进命令链路时，影响现有 runner 默认行为
  - 控制：默认值保持关闭，只有显式开启才改变行为
- 风险：monitor 展示字段过多导致前端报错或空值处理不一致
  - 控制：全部按可空字段处理，显示层做默认值兜底
- 风险：误把 `threshold/adverse` 当前已在线逻辑改坏
  - 控制：不改核心减仓构造逻辑，只补接线和展示

## 验收标准

- `hard_loss_forced_reduce_enabled` 能从控制文件一路进入 runner 实际进程
- `threshold/adverse/hard_loss` 三条保护都能在 `/monitor` 看到明确状态
- `CLUSDT` 当前线上 runner 在部署后继续稳定运行
- 现有 `threshold/adverse` 的 maker -> taker 超时行为保持不变
