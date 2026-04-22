# Runner Preset Capture And Sync Design

## Goal

为 futures runner 增加一条通用能力：把“当前正在运行的策略”的完整参数保存为可复用的策略预设，并把这些用户预设纳入仓库统一管理，再同步到各服务器。保存后的预设必须能出现在监控台的策略预设下拉列表中；用户后续点击“停止策略”后，再点击“启动策略/重启策略”时，系统必须严格按当前下拉选中的预设完整参数执行。

这次需求先覆盖两台线上服务器当前正在运行的 `BARD` 策略，但实现必须是通用的 futures runner 能力，而不是只针对 `BARD` 的一次性脚本。

## Non-Goals

- 不修改现有现货 runner 预设体系。
- 不引入“最近一次运行参数自动恢复”的隐式优先级。
- 不让保存运行中策略的动作直接覆盖内置预设。
- 不做双向自动同步或多主写入；仓库是唯一真源。

## Current State

- futures runner 内置预设由 `src/grid_optimizer/web.py` 中的 `RUNNER_STRATEGY_PRESETS` 提供。
- 自定义预设目前只覆盖 custom grid，存储在本地 `output/custom_runner_presets.json`，并通过 `create/update/delete_grid` API 维护。
- 页面已经支持“载入当前运行参数到编辑器”，但普通 futures runner 还没有“把当前运行参数固化为通用预设”的入口。
- 启动/重启逻辑会基于当前页面 payload 解析配置，但用户预设并没有仓库级统一存储，因此无法稳定同步到多台服务器。

## Requirements

1. 用户可以把当前运行中的 futures runner 完整配置保存为新的策略预设。
2. 保存后的预设必须进入 futures 策略页下拉列表，并可被选择、载入、启动、重启。
3. 启动/重启时必须严格使用当前下拉选中的预设完整参数，不允许偷偷回退到“最近一次运行参数”。
4. 用户预设必须纳入仓库统一管理，并能通过部署同步到多台服务器。
5. 保存运行中配置时，必须保留真实执行所需参数；除了 symbol 绑定的运行时路径外，不得丢字段。
6. 现有内置预设保持只读，不受用户保存动作影响。

## Approach Options

## Option 1: 仓库级用户预设文件 + 页面新增“保存当前运行策略”

新增一个仓库内的 futures 用户预设 JSON 文件，作为用户自定义 futures 预设真源。页面提供“将当前运行策略保存为预设”入口，从当前 runner config 读取完整参数，标准化后保存为新的用户预设。下拉列表把内置预设与仓库级用户预设合并展示。

优点：

- 单一真源清晰，符合“仓库统一管理，再同步到各服务器”。
- 改动集中在 futures preset 读写与 UI，和当前 custom grid 模式一致。
- 启动/重启语义容易保持清楚：选哪个预设就按哪个启动。

缺点：

- 需要为现有 custom grid 本地预设与新仓库级用户预设做边界区分。
- 需要把线上现有运行中参数显式保存一次，才能进入统一管理。

## Option 2: 保留本地 output 预设，再加仓库同步层

继续把用户预设先写本地 `output/`，再增加脚本把本地文件收集回仓库并分发。

优点：

- 对现有 custom grid 代码入侵更小。

缺点：

- 数据源变成“仓库 + 线上本地”，后续谁覆盖谁不清楚。
- 更容易出现两台服务器参数漂移和同步冲突。

## Option 3: 直接允许覆盖内置预设

把当前运行参数直接写回同名 preset。

优点：

- UI 最简单。

缺点：

- 风险最高，用户可能无意改坏内置模板。
- 预设语义会漂移，后续难审计。

## Recommended Design

采用 Option 1。

### Data Model

新增仓库级 futures 用户预设文件，例如 `config/runner_user_presets.json`。文件结构与现有 preset summary/lookup 兼容，每个条目至少包含：

- `key`
- `label`
- `description`
- `symbol`
- `custom: true`
- `startable: true`
- `kind`
- `source`
- `created_at`
- `updated_at`
- `config`

其中 `config` 保存标准化后的完整 futures runner 配置。`source` 至少支持：

- `captured_running_config`
- `custom_grid`

`kind` 从 `strategy_mode` 推导，保持和现有内置 preset 分类一致。

### Preset Resolution

futures preset 读取顺序改为：

1. 内置 `RUNNER_STRATEGY_PRESETS`
2. 仓库级 `config/runner_user_presets.json`

同名 key 不允许覆盖内置预设；用户预设必须生成独立 key。若后续需要编辑用户预设，只允许更新用户预设自身。

### Capture Flow

新增 API，例如：

- `POST /api/runner/presets/save_running`

请求参数：

- `symbol`
- `name`
- `description`（可选）

处理流程：

1. 根据 `symbol` 读取当前 runner process/config。
2. 若 runner 未运行或没有可用 config，返回错误。
3. 对 config 走与启动前一致的标准化流程。
4. 对 `state_path / plan_json / submit_report_json / summary_jsonl` 按 symbol 做路径规范化，避免从别的 symbol 复制错误运行时路径。
5. 生成新的用户 preset 条目并写入仓库级预设文件。
6. 返回 `preset_key`、`preset` 和刷新后的 preset summaries。

### UI Changes

在 futures 策略页新增：

- “保存当前运行策略为预设”按钮
- 预设名称输入框
- 可选描述输入框

推荐交互：

1. 用户先点“载入当前运行参数”确认页面参数。
2. 再点“保存当前运行策略为预设”。
3. 保存成功后，下拉自动刷新并选中新预设。
4. 后续点击“启动策略/重启策略”时，严格按该预设完整参数执行。

页面不新增“停止后自动恢复最近运行参数”逻辑，避免与已确认的优先级冲突。

### Start/Restart Semantics

启动/重启逻辑保持：

- 当前下拉选中的 preset 是唯一显式配置源。
- 若用户点“载入预设参数”，编辑器应完整填充该 preset 的 config。
- 若用户直接点击“启动策略/重启策略”，后端也必须基于选中的 preset key 解析完整 config，再与表单显式覆盖字段合成最终 payload。

换句话说，保存运行中策略只是生成新的可选 preset，不改变“选中的预设优先”这一规则。

### Compatibility With Custom Grid

现有 custom grid futures preset 不再只写 `output/custom_runner_presets.json`，而是迁移到统一的仓库级用户预设文件，或至少由同一套读写接口管理。这样：

- futures 下拉只维护一套用户 preset 来源；
- custom grid 与 captured running config 都能被统一展示和同步；
- 旧本地文件可做一次性兼容读取，但新写入只进仓库级文件。

### Sync Model

仓库级用户预设文件纳入版本控制。部署到服务器时，该文件随代码同步。这样两台服务器都会读取同一份用户 preset 集合，满足统一管理要求。

这意味着：

- 新增/更新用户预设后，需要走正常代码同步流程到 8788 和 8789。
- 仓库是唯一真源；服务器本地不再作为长期保存源。

### Initial BARD Capture

这次交付中需要额外完成一次实际数据固化：

1. 读取 `http://43.155.136.111:8788/monitor` 当前 `BARD` runner config。
2. 读取 `http://43.131.232.150:8789/monitor` 当前 `BARD` runner config。
3. 对两台机器的运行参数分别保存为明确命名的仓库级用户预设。
4. 确认这两个预设在下拉可见，且配置与原运行参数一致。

如果两台机器当前参数完全一致，可以保留两个不同命名的预设，也可以在人工确认后只保留一个共享预设；本次默认保留两个独立预设，避免误合并。

## Error Handling

- runner 未运行：禁止保存，提示当前 symbol 没有运行中策略。
- 运行中 config 缺少 `symbol` 或 `strategy_mode`：禁止保存并提示配置不完整。
- 预设名称为空：前端阻止提交，后端再次校验。
- 预设 key 冲突：自动追加后缀，绝不覆盖内置预设。
- symbol 与预设绑定不一致：沿用现有校验并返回错误。
- 仓库级预设文件损坏：读失败时回退为空集合，但写入前返回明确错误，避免 silent overwrite。

## Testing Strategy

先补失败测试，再实现。

### Backend tests

- `save_running` 在 runner 运行且 config 完整时，能写入仓库级用户预设。
- 保存后的 preset 会出现在 `_runner_preset_summaries(symbol)`。
- 保存的 `config` 与运行中 config 一致，只有 symbol 运行时路径按规范归一化。
- 内置 preset 不可被同名覆盖。
- custom grid 与 captured preset 能通过统一接口读到。
- 启动/重启时，如果选中的是 captured preset，最终 runner config 与 preset 完整一致。

### UI tests

- futures 页面包含新的保存入口。
- 保存成功后下拉刷新并自动选中新 preset。
- 从新 preset 载入参数时，编辑器字段和 preset config 一致。

### Verification

- 本地跑针对性测试：`tests/test_web_security.py`
- 如有需要补 `tests/test_monitor.py`
- 实际读取两台线上 monitor 的 `BARD` 当前运行参数，保存并核对生成的 preset 内容。

## Risks

- 当前工作区已有未提交修改，`src/grid_optimizer/web.py` 和测试文件也在脏状态；实现时必须只追加本需求改动，不能覆盖已有修改。
- 如果线上两台服务器的代码版本与当前仓库不一致，抓回的运行参数可能包含本地代码未定义的新字段；保存逻辑必须对未知字段宽容透传。
- 若部署流程未覆盖新预设文件，同步目标无法达成；交付时需要显式验证该文件会随代码同步。

## Open Decisions Resolved

- 预设存储：仓库统一管理，再同步到服务器。
- 启动优先级：严格按下拉选中的预设执行。
- 通用范围：futures runner 通用能力，不只针对 BARD。
