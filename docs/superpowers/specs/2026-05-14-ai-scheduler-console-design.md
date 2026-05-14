# AI 定时任务控制台设计

## Goal

在 `110` 服务器现有 `grid_optimizer.web` 控制台中新增一套 AI 定时任务控制面，使用户可以在 Web 页面中：

- 先设置共通的基础目标和执行口径
- 从服务器列表勾选少数目标服务器
- 从目标服务器的“运行中 + 已保存未启动”币种中选择一个或多个币种
- 为每个 `服务器 x 币种` 组合自动生成独立任务
- 设置周期、启停状态、全局默认规则和任务级覆盖规则
- 查看 AI 每次实际执行了什么

执行面采用“Web 控制台 + 独立 scheduler worker”的结构。调度触发后，任务在目标服务器上的 Codex 环境中运行，但执行必须经过本地动作契约和规则校验，不能依赖运行时人工确认。

## Current Problem

当前项目已经有：

- `running_status`、`strategy_workspace` 这类本地/跨服状态页
- 现成的 runner 保存、启动、停止控制
- `console_registry` 提供的服务器列表

但缺少一套面向“AI 定时执行”的控制面：

1. 用户不能在 Web 上定义“任务目标 + 周期 + 安全边界”。
2. 用户不能把多个服务器、多个币种的 AI 执行任务集中管理。
3. 用户不能清晰看到每次 AI 到底做了什么、为什么成功、为什么被拒绝。
4. 如果直接让 AI 获得自由 shell，再在运行时等待确认，会与用户实际使用场景冲突：
   - 用户是在 Web 页面上配置任务
   - 任务将运行在服务器上的 Codex
   - 运行时无法再回来向用户确认

因此，这一版需要的是一套“预授权 + 明确禁止项 + 结构化动作执行 + 可审计结果页”的完整闭环，而不是单纯的提醒或 prompt 存档页面。

## Confirmed Product Decisions

本设计基于以下已经确认的选择：

- 页面部署在 `110`
- 第一版即支持“页面在 `110`，但可手动勾选少数服务器”
- 候选币种来源为“运行中 + 已保存未启动”
- 选择多个币种、多个服务器后，自动拆成独立任务
- 全局规则先给默认值，新任务自动继承，但每条任务都可单独覆盖
- 任务执行时不等待人工确认
- 命中禁止项时直接拒绝执行并记录原因
- 默认执行模型为“独立执行 + 持久化币种记忆”
- 少数任务可声明为更粘性的记忆模式，但第一版不把真正的长驻交互会话作为主路径
- 需要单独的“结果执行页”，并按“服务器 -> 币种”分组展示

## Scope

### In Scope

- `grid_optimizer.web` 中新增 AI 定时任务页面和结果页
- 任务、规则、执行记录、运行时记忆的本地文件存储
- 独立 scheduler worker
- 目标服务器选择、币种候选拉取、任务 CRUD、启停、立即执行
- 使用目标服务器上的 Codex 生成结构化决策
- 用本地白名单动作执行器应用允许的动作
- 结果页按服务器、币种、执行记录分组展示

### Out of Scope

- 不引入新前端框架
- 不引入数据库服务
- 不把现有 runner 控制流程整体改写成新框架
- 不做“任意 shell 自由执行后再回头审计”的无边界 AI 模式
- 不把真正的交互式长期 Codex 会话作为第一版核心依赖
- 不做全自动服务器发现；第一版只使用手动勾选的已登记服务器

## Architecture

整体结构分为三层：

```text
Web 控制台（110）
  ├─ 任务创建/编辑/启停
  ├─ 全局规则配置
  ├─ 候选服务器与候选币种展示
  └─ 执行结果页

任务仓库与运行时状态（110 本地文件）
  ├─ global_policy.json
  ├─ tasks/*.json
  ├─ runtime/*.json
  └─ task_runs/*.jsonl

scheduler worker（110）
  ├─ 扫描到期任务
  ├─ 拉取目标服务器最新状态
  ├─ 组装 Codex 输入
  ├─ 调用目标服务器 Codex 产出结构化动作
  ├─ 规则校验
  ├─ 应用允许动作
  └─ 回写执行记录与运行时记忆
```

### Why a Separate Worker

调度器必须和 Web 页面解耦：

- Web 重启不应中断调度
- 任务扫描与执行不应阻塞页面请求
- 执行日志和页面渲染应通过共享状态解耦

因此第一版不采用“页面进程自己调度自己执行”的方式。

## UI Design

### 1. AI 定时任务页

该页是主控制面，保留三段式大布局，但内容落成四个功能区：

#### A. 顶部控制区

- 当前控制台服务器标识
- 可用目标服务器列表，复选框勾选少数服务器
- 自动刷新和手动刷新
- 进入结果页的入口

#### B. 左侧：候选币种区

候选币种只从已勾选服务器中读取，数据来源为：

- `running`
- `saved_idle`

展示方式按服务器分组：

```text
110
  - BARDUSDT (运行中)
  - XAUTUSDT (已保存未启动)

114
  - BASEDUSDT (运行中)
  - NIGHTUSDT (已保存未启动)
```

用户可：

- 单个选择币种
- 服务器内批量选择
- 按状态筛选“只看运行中 / 只看已保存 / 全部”

#### C. 中间：任务定义区

用于输入本次任务的共通目标：

- 任务模板名称
- 基础目标/执行意图
- 附加限制说明
- 执行周期
  - 每 10 分钟
  - 每小时
  - 每天
  - 自定义 cron/interval
- 执行模式
  - `one_shot`
  - `sticky_memory`

用户提交后，系统按照 `已选服务器 x 已选币种` 展开成独立任务。

#### D. 右侧：规则与预览区

显示：

- 全局默认规则
- 本次任务覆盖项
- 本次将生成多少条任务
- 任务展开预览

例如：

```text
已选服务器：110, 114
已选币种：BARDUSDT, BASEDUSDT, XAUTUSDT
将生成：6 条任务
```

### 2. 任务列表区

任务页下半部分展示已创建任务，默认按“任务行”展示，但支持切换为“按服务器查看”。

每条任务展示：

- 任务名
- 服务器
- 币种
- 周期
- 是否启用
- 执行模式
- 下次触发时间
- 最近一次执行结果摘要
- 操作：编辑 / 启停 / 立即执行 / 查看结果

状态至少区分：

- `enabled`
- `paused`
- `running`
- `last_success`
- `last_failed`
- `last_rejected`

### 3. 全局默认规则编辑

全局规则不单独做新页面，采用抽屉或侧栏编辑，供任务创建和任务编辑共用。

字段包括：

- 允许直接调参数：是/否
- 允许直接启停策略：是/否
- 允许直接清理挂单：是/否
- 允许直接平仓/减仓：是/否
- 允许直接切换策略模式：是/否
- 单次最大调参幅度
- 单次最大名义调整

任务级可覆盖这些字段，但必须明确展示“继承自全局”还是“本任务覆盖”。

### 4. 执行结果页

执行结果页单独成页，按以下层级展示：

```text
服务器
  └─ 币种
       └─ 每次执行记录
```

例如：

```text
110
  BARDUSDT
    - 10:00 调小 step_price 8%，保存并重启
    - 10:10 命中禁止项：减仓，已拒绝

114
  BASEDUSDT
    - 09:00 服务器不可达，执行失败
```

每次执行记录展示：

- 触发时间
- 任务名
- 服务器
- 币种
- 模型结论摘要
- 结构化动作列表
- 实际已应用动作列表
- 是否命中禁止项
- 最终状态：成功 / 拒绝 / 失败
- 日志查看入口

## Server Registry Requirements

现有 `config/console_registry.json` 只覆盖页面跳转所需信息，不足以支持远程 AI 执行。第一版需要为可调度服务器增加执行元数据，例如：

- `ssh_host`
- `ssh_user`
- `workspace_dir`
- `codex_path`
- `python_path`
- `scheduler_enabled`

只有同时满足以下条件的服务器才允许被勾选：

- `enabled = true`
- `scheduler_enabled = true`
- 有可用的 `base_url`
- 有可用的远程执行元数据

`110` 也必须作为本地服务器登记到同一份 registry 中。

## Storage Design

第一版采用本地文件仓库，路径建议为：

```text
output/ai_scheduler/
  global_policy.json
  tasks/
    <task_id>.json
  runtime/
    <task_id>.json
  task_runs/
    <task_id>.jsonl
```

### global_policy.json

保存全局默认规则，以及 UI 的最近默认输入。

### tasks/<task_id>.json

任务定义至少包含：

- `task_id`
- `name`
- `server_id`
- `symbol`
- `enabled`
- `schedule`
- `execution_mode`
- `goal_prompt`
- `policy_overrides`
- `effective_policy_snapshot`
- `created_at`
- `updated_at`
- `last_run_at`
- `next_run_at`

一条任务只对应一个 `server_id + symbol`。

### runtime/<task_id>.json

保存任务运行记忆，供后续执行补充上下文：

- 最近参数快照
- 最近一次成功动作
- 最近一次失败原因
- 最近几次执行摘要
- 失败计数
- 风险拦截计数
- 粘性记忆模式的附加上下文

该文件是“更懂这个币种”的主来源，不依赖无限增长的聊天上下文。

### task_runs/<task_id>.jsonl

逐行保存执行记录，供结果页直接读取。

每条记录至少包含：

- `run_id`
- `triggered_at`
- `finished_at`
- `status`
- `server_id`
- `symbol`
- `decision_summary`
- `proposed_actions`
- `applied_actions`
- `rejected_actions`
- `error`

## Execution Model

### Default: one_shot

默认模式下，每次触发都执行一次独立决策：

1. 读取任务定义
2. 拉取最新服务器/币种状态
3. 读取该任务的 runtime 记忆
4. 调用目标服务器上的 Codex 生成结构化动作建议
5. 校验规则
6. 应用动作
7. 记录结果并刷新 runtime

优点：

- 稳定
- 易复现
- 易调试
- 不会因旧上下文长期污染执行判断

### sticky_memory

第一版允许任务标记为更粘性的记忆模式，但其核心不是长期开放式交互会话，而是：

- 给该任务保留更完整的 runtime 记忆
- 每次执行时带入更丰富的近期结论
- 保持任务自己的决策连续性

第一版不把“持续打开的交互式 Codex TUI 会话”作为主实现依赖，因为当前 CLI 的非交互续发接口不够稳定，不适合做生产主路径。

这意味着第一版的“C 模式”是：

- 默认独立执行
- 持久化币种记忆
- 为更深上下文连续性预留字段和扩展位

## Codex Decision Contract

这是第一版最关键的安全边界。

任务执行不能让 Codex 自由 shell 后再事后审计，否则“禁止项直接拒绝”无法可靠成立。正确做法是：

1. Codex 只负责产生结构化动作决策
2. 本地执行器只接受白名单动作类型
3. 规则层先校验，再真正落动作

### Input to Codex

每次调用 Codex 时，至少提供：

- 任务目标
- 服务器与币种信息
- 当前策略状态
- 当前 runner 配置快照
- 最近执行结果摘要
- runtime 记忆
- 全局规则
- 任务覆盖规则
- 允许动作类型列表
- 禁止动作类型列表
- 输出 JSON schema

### Output from Codex

Codex 必须返回结构化 JSON，而不是自由文本。格式类似：

```json
{
  "summary": "当前库存平稳，建议小幅收紧 step 并重启 runner 生效。",
  "confidence": 0.74,
  "actions": [
    {
      "type": "update_runner_config",
      "changes": {
        "step_price": {
          "from": 0.00025,
          "to": 0.00023
        }
      }
    },
    {
      "type": "restart_runner"
    }
  ],
  "forbidden_actions_considered": []
}
```

### Whitelisted Action Types

第一版只允许以下动作族：

- `noop`
- `update_runner_config`
- `save_runner_config`
- `start_runner`
- `stop_runner`
- `restart_runner`
- `cancel_open_orders`
- `flatten_or_reduce_position`

是否真正可执行，要再经过任务规则裁决。

### Validation Layer

校验器必须至少验证：

- 动作类型是否白名单内
- 是否命中禁止项
- 是否超出单次最大调参幅度
- 是否超出单次最大名义调整
- 是否试图切换策略模式而任务不允许
- 动作 JSON 是否完整且类型正确

任一校验不通过：

- 不等待用户确认
- 直接拒绝本次动作
- 记录拒绝原因

## Action Application

动作真正落地时，不通过“让 Codex 自己跑任意 shell”实现，而通过项目内部现有控制能力完成：

- 保存参数
- 启动/停止 runner
- 停止并清仓
- 清挂单

能复用现有 Python 函数就复用函数，能复用现有本地 API 就复用本地 API。Web 是控制面，真正的执行器应尽量走受控接口。

对于远程服务器：

- `110` 上的 worker 通过 SSH 连接目标服务器
- 在目标服务器的 `wangge` 工作目录中运行受控 helper
- helper 本地调用 Codex 生成决策 JSON
- helper 本地校验并落动作
- 最终把结果返回给 `110`，由 `110` 统一写入任务记录

这样可以满足“任务运行在服务器的 Codex 上”，同时仍由 `110` 维护统一控制台和审计视图。

## Scheduler Worker

worker 为独立进程或 systemd service，职责包括：

- 周期扫描任务
- 计算 `next_run_at`
- 处理“立即执行”请求
- 为同一任务加互斥锁，避免重入
- 调用远程 helper
- 记录执行结果

### Schedule Types

第一版支持：

- 固定间隔：10 分钟、1 小时、1 天
- 自定义 interval
- 自定义 cron 表达式

worker 内部统一归一化成：

- `schedule_kind`
- `schedule_value`
- `timezone`
- `next_run_at`

## Error Handling

第一版必须明确区分以下失败类型：

- `server_unreachable`
- `codex_unavailable`
- `invalid_decision_payload`
- `forbidden_action_rejected`
- `apply_failed`
- `symbol_not_found`
- `task_disabled`

这些状态不仅写日志，也要在任务列表和结果页中可见。

## Results and Observability

结果页以“服务器 -> 币种 -> 记录”的方式聚合，但任务列表还需要保留任务维度的最近摘要，例如：

- 最近成功时间
- 最近失败时间
- 最近拒绝原因
- 连续失败次数
- 本日执行次数

此外，每个币种结果分组建议增加一个小摘要区，显示：

- 最近一次动作
- 最近一次风险拦截
- 最近一次调参幅度
- 当前启用任务数

## Deployment

第一版部署新增两类组件：

### 1. Web 页面扩展

继续运行在现有 `grid-web` 服务中。

### 2. grid-ai-scheduler service

在 `110` 新增独立 systemd service，例如：

```text
grid-ai-scheduler.service
```

职责：

- 启动 scheduler worker
- 守护任务扫描循环
- 管理互斥锁与调度状态

远程服务器不需要常驻新守护进程，但必须具备：

- `wangge` 仓库
- 可用的 `codex-cli`
- 可用的 Python 环境
- 可通过 SSH 触发 helper

## Testing

### Unit Tests

- 规则继承与覆盖合并
- 任务展开逻辑（服务器 x 币种）
- 调度时间计算
- 动作校验器
- 结果页分组逻辑

### Integration Tests

- Web API 的任务 CRUD
- 候选币种聚合
- 任务立即执行
- 远程执行器的 SSH 包装与结果回传

### Safety Tests

- 禁止项命中后必须拒绝
- 调参超限必须拒绝
- 非法 JSON 决策必须拒绝
- 远程服务器不可达时不能误标记成功

## Rollout

### Phase 1

- `110` 控制台上线
- 支持勾选少数目标服务器
- 支持任务创建、任务列表、结果页
- 默认 `one_shot` 执行
- runtime 记忆生效

### Phase 1.1

- 增强 `sticky_memory` 输入上下文
- 优化结果页聚合摘要
- 增加更多任务筛选和批量操作

### Phase 2

- 如果 Codex CLI 后续提供稳定的非交互续发接口，再评估真正长驻会话执行器

## Expected Outcome

完成后，用户将获得一套真正可用的 AI 定时任务控制面：

- 在 `110` 页面中创建和管理任务
- 对少数目标服务器进行精细化下发
- 对每个 `服务器 x 币种` 独立启停、独立编辑、独立审计
- 不需要运行时人工确认
- 命中禁止项时严格拒绝
- 在结果页里按服务器、再按币种清楚看到 AI 到底做了什么
