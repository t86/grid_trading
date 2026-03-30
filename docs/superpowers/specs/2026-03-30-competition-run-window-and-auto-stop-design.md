# 交易赛 Runner 运行时间窗与自动停止设计

**日期：** 2026-03-30

## 概述

为合约和现货两套交易赛 runner 增加统一的运行时控制能力，使它们可以：

- 在配置的开始时间之前等待，不下策略单
- 在配置的结束时间到达后自动停止
- 在最近 60 分钟滚动亏损达到阈值后自动停止
- 在累计成交额达到阈值后自动停止

当任一停止条件命中时，系统必须停止主策略、撤掉策略自身挂单，并复用现有的买一卖一顶档 `maker` 平仓追单机制，使其持续跟随盘口直到仓位完全归零。

本次变更适用于：

- 从 `/api/runner/start` 启动的合约 runner
- 从 `/api/spot_runner/start` 启动的现货 runner
- 对应的 runner 状态页和 snapshot 数据

本次工作的部署范围为当前仓库代码，以及合并后更新到 `43.155.136.111`。

## 目标

- 为合约和现货交易赛策略支持显式的开始时间和结束时间。
- 为合约和现货支持基于最近 60 分钟滚动亏损的自动停机。
- 为合约和现货支持基于累计成交额的自动停机。
- 复用现有停止执行路径，而不是再造一套新的清仓逻辑。
- 在 web snapshot 和页面上展示停止状态与停止原因。

## 非目标

- 不修改交易赛榜单本身的数据模型。
- 不扩展到 `43.155.136.111` 之外的部署目标。
- 不在触发结束条件后自动重启 runner。
- 不改变现有 flatten runner 的 `maker` 追单机制本身，只负责将它接入新的自动停止流程。

## 当前上下文

仓库里已经有两套独立的实时策略循环：

- 合约：[`/Volumes/WORK/binance/grid_trading/src/grid_optimizer/loop_runner.py`](/Volumes/WORK/binance/grid_trading/src/grid_optimizer/loop_runner.py)
- 现货：[`/Volumes/WORK/binance/grid_trading/src/grid_optimizer/spot_loop_runner.py`](/Volumes/WORK/binance/grid_trading/src/grid_optimizer/spot_loop_runner.py)

运行时配置和 web UI 组装位于 [`/Volumes/WORK/binance/grid_trading/src/grid_optimizer/web.py`](/Volumes/WORK/binance/grid_trading/src/grid_optimizer/web.py)。

当前代码库在合约侧已经支持手动停止流程，可以：

- 停止主 runner
- 撤掉策略挂单
- 启动一个 maker flatten 进程，在盘口顶档持续工作直到仓位归零

对于现货，当前代码只支持停止 runner 和撤掉策略挂单，还没有与合约等价的持续 `maker` 平仓追单循环。本次功能需要补齐这条现货侧关闭路径，使现货和合约都能满足你要求的行为。

## 用户可见行为

### 开始时间窗

- 如果未设置 `run_start_time`，runner 可以立即交易。
- 如果设置了 `run_start_time`，且当前时间早于该时间，runner 进程可以保持存活，但不得下新的策略单。
- 在开始时间到来之前，UI 需要明确显示 runner 正在等待开始时间。

### 运行时间窗内

- 如果当前时间位于配置的运行窗口内，runner 正常运行。
- 每一轮循环在下新单之前，都要检查自动停止阈值。

### 结束条件

以下条件任一满足时，策略停止：

- 当前时间大于等于 `run_end_time`
- 最近 60 分钟滚动亏损大于等于 `rolling_hourly_loss_limit`
- 累计成交额大于等于 `max_cumulative_notional`

如果同一轮内同时命中多个条件，停止元数据需要同时记录全部命中原因，并给出一个主原因用于页面展示。

### 停止动作

一旦命中结束条件：

1. 主策略循环停止继续下单
2. 撤掉策略自身挂单
3. 启动或继续已有的顶档 `maker` 平仓流程
4. 持续按盘口顶档追单，直到仓位数量归零

从操作者视角看，现货和合约的行为必须一致。实现上，合约可以复用现有 flatten runner，现货则需要新增一条等价的平仓追单流程。

## 配置项新增

为合约和现货 runner 都新增以下字段：

- `run_start_time`：可选，ISO 8601 时间
- `run_end_time`：可选，ISO 8601 时间
- `rolling_hourly_loss_limit`：可选，正数浮点值
- `max_cumulative_notional`：可选，正数浮点值

校验规则：

- 如果同时设置了开始和结束时间，则 `run_start_time` 必须早于 `run_end_time`
- 亏损阈值一旦设置，必须严格大于 0
- 累计成交额阈值一旦设置，必须严格大于 0
- 所有时间统一存储为带时区的 UTC 时间并以 UTC 比较

## 数据语义

### 时间

- web 层接收时间输入后，统一归一化为带时区的 UTC 字符串
- runner 内部统一按当前 UTC 时间比较
- snapshot 返回归一化后的 ISO 8601 时间字符串

### 最近 60 分钟滚动亏损

这里使用滚动窗口，而不是自然小时。

窗口定义：

- 纳入 `now - 60 分钟` 到 `now` 之间的交易与收益事件

合约侧亏损口径：

- 使用 futures runner 现有监控路径里可获得的已实现收益和 income/audit 数据
- 用于比较阈值的值为 `max(0, -window_net_pnl)`

现货侧亏损口径：

- 使用最近 60 分钟现货已实现收益
- 再扣除 state/summary 中已经维护的手续费和 `recycle_loss_abs`
- 用于比较阈值的值为 `max(0, -window_net_pnl)`

如果没有设置滚动亏损阈值，则该条件关闭。

### 累计成交额

- 直接使用 runner 已维护的累计 `gross_notional`
- 与 `max_cumulative_notional` 比较
- 如果没有设置该阈值，则该条件关闭

## 架构设计

### 共享运行时守卫 Helper

新增一个很薄的共享 helper 模块，专门负责运行时 gate 判断。这个 helper 需要：

- 归一化可选的运行时守卫配置
- 判断 runner 当前处于开始前、运行中还是结束后
- 计算最近 60 分钟滚动亏损
- 判断累计成交额阈值
- 返回统一的 guard 结果，至少包括：
  - `tradable`
  - `stop_triggered`
  - `primary_reason`
  - `matched_reasons`
  - `triggered_at`
  - 用于展示的计算结果

这样 futures 和 spot 可以共用一套决策模型，同时各自仍能提供自身的指标输入。

### 合约 Runner 接入

在 [`/Volumes/WORK/binance/grid_trading/src/grid_optimizer/loop_runner.py`](/Volumes/WORK/binance/grid_trading/src/grid_optimizer/loop_runner.py) 中：

- 解析新的 CLI 参数
- 在每轮循环开始处尽早执行运行时守卫判断，且早于计划执行和下单逻辑
- 如果还没到开始时间，则跳过计划提交，只写一条等待状态 summary
- 如果命中停止条件，则把停止元数据写入 summary/state，调用现有的合约停止路径执行撤单 + flatten，然后让主循环干净退出

### 现货 Runner 接入

在 [`/Volumes/WORK/binance/grid_trading/src/grid_optimizer/spot_loop_runner.py`](/Volumes/WORK/binance/grid_trading/src/grid_optimizer/spot_loop_runner.py) 中：

- 解析新的 CLI 参数
- 在构建或提交目标订单之前执行运行时守卫判断
- 如果还没到开始时间，则跳过下单，只写一条等待状态 summary
- 如果命中停止条件，则把停止元数据写入 summary/state，调用新的现货关闭路径：先撤单，再启动顶档 `maker` 平仓追单，最后让主循环干净退出

### Flatten 接入

关闭路径需要按市场类型分别落地，但对外保持一致的停止契约：

- 合约：复用现有 maker flatten runner
- 现货：新增一个具有相同操作语义的 maker 平仓循环或等价模块，要求：
  - 在盘口顶档挂 maker 平仓单
  - 盘口变动时撤单重挂
  - 持续运行直到受管库存归零
  - 同一 symbol 避免重复启动多个 flatten 进程

### Web 层接入

在 [`/Volumes/WORK/binance/grid_trading/src/grid_optimizer/web.py`](/Volumes/WORK/binance/grid_trading/src/grid_optimizer/web.py) 中：

- 为合约 runner 的 payload 归一化增加这 4 个新字段
- 为现货 runner 的 payload 归一化增加这 4 个新字段
- 在两套 runner 的启动命令组装中加入新的 CLI 参数
- 在 snapshot 中增加以下展示字段，使页面可以看到：
  - 配置的运行时间窗
  - 当前运行状态（`waiting`、`running`、`stopped`）
  - 最近 60 分钟滚动亏损
  - 累计成交额
  - 是否已经触发自动停止
  - 停止原因和停止时间

## 停止原因模型

使用稳定的 reason code：

- `before_start_window`
- `after_end_window`
- `rolling_hourly_loss_limit_hit`
- `max_cumulative_notional_hit`

行为定义：

- `before_start_window` 是等待状态，不是终态停止
- 另外三个都是终态停止条件
- snapshot 既要暴露机器可读的 reason code，也要暴露简短的人类可读文案

## State 与 Summary 变更

合约和现货两边都需要输出足够的运行信息，方便排障和页面展示。

建议新增或持久化以下字段：

- `run_start_time`
- `run_end_time`
- `rolling_hourly_loss_limit`
- `max_cumulative_notional`
- `runtime_status`
- `rolling_hourly_loss`
- `cumulative_gross_notional`
- `stop_triggered`
- `stop_reason`
- `stop_reasons`
- `stop_triggered_at`

在开始时间之前的等待周期里，也应该持续追加轻量状态事件，方便操作者确认 runner 还活着，而且是有意空转等待。

## 错误处理

- 非法配置应在 web payload 归一化或 CLI 参数解析阶段失败，而不是运行中途才报错。
- 如果停止动作部分失败，summary 仍然必须记录失败细节，并保留主停止原因。
- 如果 flatten 进程在自动停止触发前已经运行，则应直接复用，而不是重复启动。
- 如果停止时本身已经没有仓位，流程仍然需要完成撤单并干净退出。

## 测试策略

### Web 测试

扩展 web 侧测试，验证：

- 合约新增字段可以正确归一化
- 现货新增字段可以正确归一化
- 命令构造中包含新的 CLI 参数
- snapshot 能暴露运行时间窗和停止元数据

### 合约 Runner 测试

新增测试覆盖：

- `run_start_time` 之前处于等待状态
- 到达 `run_end_time` 时自动停止
- 最近 60 分钟滚动亏损超过阈值时自动停止
- 累计 `gross_notional` 超过阈值时自动停止
- 能正确走现有的撤单 + flatten 停止路径

### 现货 Runner 测试

新增测试覆盖：

- `run_start_time` 之前处于等待状态
- 到达 `run_end_time` 时自动停止
- 最近 60 分钟滚动亏损超过阈值时自动停止
- 累计 `gross_notional` 超过阈值时自动停止
- 能正确走现货的停止执行路径以及新的 maker flatten 行为

### 验证

部署前需要：

- 跑 web、futures runner、spot runner 的目标单测
- 至少对两套 runner 各做一次命令级验证，确认新参数可正确解析

## 部署计划

在实现完成并完成本地验证后：

1. 将变更合并到 `main`
2. 使用现有 Oracle 部署流程 [`/Volumes/WORK/binance/grid_trading/deploy/oracle/install_or_update.sh`](/Volumes/WORK/binance/grid_trading/deploy/oracle/install_or_update.sh) 更新服务
3. 更新指定主机 `43.155.136.111`
4. 核对相关服务状态，并确认新的运行时字段已在 web UI 或 snapshot 中生效

本次部署步骤只覆盖你指定的 `43.155.136.111`。

## 实现备注

- 运行时守卫判断优先抽成共享 helper，不要在两套 runner 里复制逻辑。
- 停止和 flatten 编排优先复用现有机制，不要再造第二套退出路径。
- UI 新增项应偏运维视角，确保操作者能一眼看出当前是等待中、运行中还是已自动停止，以及具体原因。
