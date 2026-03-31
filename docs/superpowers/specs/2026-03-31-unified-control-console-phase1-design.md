# Unified Control Console Phase 1 Design

## Goal

为当前多服务器、多账号、多页面的交易管理方式补上一层统一入口，交付一个新的 `/console` 页面作为手机优先的统一控制台。

Phase 1 的目标不是重写现有交易引擎，而是把已经存在的页面、监控接口和比赛页面收拢到一个账号优先的入口里，让用户在手机上可以：

- 快速切换不同 Binance API key 账号
- 一眼看到当前账号对应的服务器、运行状态、比赛状态和关键风险
- 从同一入口跳转到现有的监控页、执行台、策略总览页和比赛榜单页
- 只登录一次统一入口，不再手动记忆和切换多台服务器地址

## Scope

Phase 1 的范围固定为“统一入口 + 统一上下文 + 统一概览”：

- 新增 `/console` 页面
- 新增 `/api/console/registry`
- 新增 `/api/console/overview`
- 新增 `config/console_registry.json`
- 将 `/` 和 `/hub` 统一导向 `/console`
- 保留 `/monitor`、`/spot_runner`、`/strategies`、`/spot_strategies`、`/competition_board` 作为深链目标

## Non-Goals

Phase 1 明确不做：

- 不开发桌面应用或原生移动 App
- 不替换现有 `/monitor`、`/spot_runner`、`/competition_board`、`/strategies` 等页面的完整功能
- 不重写现有 futures runner / spot runner 的运行逻辑
- 不集中存储 Binance API Key / Secret，密钥仍保留在各节点机器本地
- 不做跨节点统一下单、统一启停、统一参数编辑代理
- 不做 SSO、复杂 RBAC、推送通知或数据库化权限系统

Phase 1 的职责是统一入口，不是完整控制平面。

## Current State

当前项目已经具备多个业务页面和接口，但它们还没有被组织成统一管理台：

- `/hub` 只是一个硬编码服务器地址的跳转页，不理解账号、比赛和运行状态
- `src/grid_optimizer/web.py` 同时承载 HTML 页面、接口路由、runner 启停、监控聚合和排行榜逻辑
- 合约与现货监控都基于每台机器本地 `output/*.json` / `output/*.jsonl` 文件组织
- 比赛榜单录入和历史也保存在本地 JSON 文件
- 当前页面与配置更多围绕币种和节点组织，没有显式的多账号抽象

这意味着当前系统的真实结构其实是：

1. 每台服务器各自运行一份本地交易工作台
2. 每台服务器各自维护自己的 runner 状态和本地快照
3. 用户通过记忆 IP、端口和页面路径来切换不同节点
4. “账号”“服务器”“默认交易标的”三者的关系没有被显式建模

## Why Phase 1 First

统一入口的第一步不应该先做重型后端改造，而应该先补上管理视角的壳层。

原因有三个：

1. 现有节点已经有可用页面和 API，可以直接复用
2. 当前最大的痛点是入口分散和手机切换成本高，不是交易引擎本身
3. 先把账号、服务器、页面的关系显式建模，后续才适合继续做统一操作代理和标准化节点接口

因此，Phase 1 选择一个低风险路径：

- 新增统一控制台
- 新增服务器/账号注册表
- 统一入口服务在服务端聚合远端节点摘要
- 用户只面对一个移动端入口
- 深度操作仍回到原有页面完成

## Recommended Form Factor

Phase 1 采用：

- Responsive Web
- 手机优先设计
- 页面结构兼容后续 PWA 化

不采用：

- Electron / 桌面壳
- 原生 iOS / Android App

原因很简单：当前系统本质上是 web 控制台加本地节点服务，手机常用但不依赖强设备能力。先做移动优先 web，收益最高，改造最小，后面若需要再补 PWA 即可。

## Implementation Approach

Phase 1 不采用额外的前后端分离工程，也不继续把控制台逻辑直接堆进 `web.py`。

推荐实现边界为：

- 保留当前 Python 单进程服务与现有路由体系
- 在单体内部拆出独立的 console 模块
- `web.py` 只负责路由挂载、首页跳转和少量胶水逻辑

建议新增的模块：

- `src/grid_optimizer/console_registry.py`
- `src/grid_optimizer/console_overview.py`
- `src/grid_optimizer/console_page.py`

这样可以在不增加部署复杂度的前提下，为后续逐步迁移旧页面留出清晰边界。

## Phase 1 Deliverables

Phase 1 交付以下内容：

1. 新页面 `/console`
2. 新接口：
   - `/api/console/registry`
   - `/api/console/overview`
3. 新配置文件：
   - `config/console_registry.json`
4. 统一移动端 UI：
   - 账号切换
   - 当前账号总览
   - 比赛模块入口
   - 运行模块入口
   - 服务器状态模块
   - 旧页面深链入口
5. 首页入口调整：
   - `/` -> `/console`
   - `/hub` -> `/console`

## Information Model

Phase 1 先引入三个核心模型。

### Server

表示一台实际部署节点机器。

字段建议：

- `id`
- `label`
- `base_url`
- `location`
- `enabled`
- `capabilities`
- `notes`

其中 `capabilities` 至少支持：

- `futures_monitor`
- `spot_runner`
- `competition_board`
- `strategies`
- `spot_strategies`

### Account

表示一个 Binance API key 对应的交易账号上下文，也是控制台的主切换对象。

字段建议：

- `id`
- `label`
- `server_id`
- `kind`
- `priority`
- `enabled`
- `default_symbols`
- `competition_symbols`
- `pages`
- `notes`

说明：

- `kind` 用于区分 `futures` / `spot` / `mixed`
- `default_symbols` 表示这个账号在控制台默认关注的交易标的
- `competition_symbols` 表示这个账号当前关注的比赛标的
- `pages` 用于描述这个账号常用的深链页面
- Phase 1 固定 `1 个 account = 1 台 server = 1 组默认交易标的`

这里必须明确：

- `account` 不是币种
- `KAT`、`NIGHT`、`BARD`、`XAUT` 这类是交易或比赛标的
- 账号应对应真实的 Binance API key 使用上下文

### Competition Source

Phase 1 不把比赛榜单拆成独立服务，只在注册表里指定一个统一来源。

字段建议：

- `server_id`
- `path`

默认指向一个已部署并可稳定访问 `competition_board` 的节点。

## Registry Design

新增 `config/console_registry.json`，作为 Phase 1 的唯一配置入口。

该文件只存非敏感元数据，不存远端节点密码，也不存 Binance API key。

示例结构：

```json
{
  "servers": [
    {
      "id": "srv_150",
      "label": "主入口 A",
      "base_url": "http://43.131.232.150:8788",
      "location": "Singapore",
      "enabled": true,
      "capabilities": ["futures_monitor", "competition_board", "strategies"],
      "notes": "统一入口首选节点"
    },
    {
      "id": "srv_114",
      "label": "主入口 B",
      "base_url": "http://43.155.163.114:8788",
      "location": "Singapore",
      "enabled": true,
      "capabilities": ["futures_monitor", "spot_runner", "spot_strategies", "strategies"]
    },
    {
      "id": "srv_111",
      "label": "在线入口",
      "base_url": "http://43.155.136.111:8787",
      "location": "Oracle",
      "enabled": true,
      "capabilities": ["futures_monitor", "competition_board", "strategies"]
    }
  ],
  "accounts": [
    {
      "id": "acct_main_a",
      "label": "主账号 A",
      "server_id": "srv_150",
      "kind": "futures",
      "priority": 100,
      "enabled": true,
      "default_symbols": ["BARDUSDT", "XAUTUSDT"],
      "competition_symbols": ["KAT", "NIGHT", "BARD", "XAUT"],
      "pages": ["/monitor", "/strategies", "/competition_board"],
      "notes": "默认首页账号"
    },
    {
      "id": "acct_spot_b",
      "label": "现货账号 B",
      "server_id": "srv_114",
      "kind": "spot",
      "priority": 80,
      "enabled": true,
      "default_symbols": ["KATUSDT", "NIGHTUSDT"],
      "competition_symbols": ["KAT", "NIGHT"],
      "pages": ["/spot_runner", "/spot_strategies", "/competition_board"]
    }
  ],
  "competition_source": {
    "server_id": "srv_150",
    "path": "/api/competition_board"
  }
}
```

默认账号选择规则：

- 仅从 `enabled = true` 的账号中选择
- 取 `priority` 最高的账号
- 若同优先级出现多个账号，按注册表中的出现顺序取第一个

## Authentication Design

当前节点页面使用 Basic Auth。Phase 1 不要求用户直接面对所有节点的认证提示，而由统一入口服务在服务端聚合远端数据。

设计要求：

- 用户只登录统一入口一次
- 控制台服务端去请求远端节点 API
- 远端节点认证凭证由入口服务从环境变量读取

建议约定：

- `GRID_NODE_<SERVER_ID>_USERNAME`
- `GRID_NODE_<SERVER_ID>_PASSWORD`

例如：

- `GRID_NODE_SRV_150_USERNAME`
- `GRID_NODE_SRV_150_PASSWORD`

这样可以保证：

- 节点密码不落盘到仓库
- 统一入口可无感聚合多节点数据
- 未来切换到 token 或反向代理时，注册表结构不用推翻

## Data Source Strategy

Phase 1 不新增复杂节点协议，优先复用现有接口。

### Server Health

对每台节点请求：

- `GET /api/health`

用于：

- 在线 / 离线状态
- 基础延迟
- 最近探测时间

### Futures Overview

对账号配置的每个 futures symbol 请求：

- `GET /api/loop_monitor?symbol=<SYMBOL>`

复用现有：

- runner 状态
- 仓位
- 当前挂单
- 累计成交额
- 净收益估算
- 风控摘要

### Spot Overview

对账号配置的每个 spot symbol 请求：

- `GET /api/spot_runner/status?symbol=<SYMBOL>`

复用现有：

- runner 状态
- 库存
- 当前挂单
- 累计成交额
- 净收益估算
- 停买状态

### Competition Overview

从统一配置的比赛来源请求：

- `GET /api/competition_board`

控制台只提取：

- 当前进行中的比赛摘要
- 当前账号 `competition_symbols` 命中的比赛条目
- 与该账号相关的比赛入口链接

第一版默认以你当前确认的比赛集合为主：

- `KAT`
- `NIGHT`
- `BARD`
- `XAUT`

## Aggregation Rules

新增 `/api/console/overview`，由统一入口服务在服务端完成聚合。

输入：

- `account_id`

输出结构建议：

```json
{
  "ok": true,
  "account": {},
  "server": {},
  "health": {},
  "summary": {},
  "futures": [],
  "spot": [],
  "competitions": [],
  "links": {},
  "warnings": [],
  "fetched_at": "2026-03-31T10:30:00+08:00"
}
```

其中：

- `account`：当前账号元信息
- `server`：所属服务器信息
- `health`：服务器在线状态
- `summary`：首屏 Hero 指标
- `futures`：该账号对应的合约摘要数组
- `spot`：该账号对应的现货摘要数组
- `competitions`：与当前账号相关的比赛摘要
- `links`：指向旧页面的深链
- `warnings`：局部失败、认证缺失、超时等告警
- `fetched_at`：服务端聚合完成时间

聚合失败策略：

- 单模块失败不应让整个页面报错
- 每个模块返回自己的 `ok/error` 或标准化 unavailable 状态
- 控制台页面必须可以部分展示

## UI Architecture

Phase 1 的 `/console` 页面是一个单页控制台，不拆多路由。

页面结构从上到下如下。

### 1. Global Context Bar

固定在顶部。

包含：

- 当前账号名称
- 账号切换入口
- 账号类型标签：`合约` / `现货` / `混合`
- 当前服务器在线状态
- 刷新按钮

手机上账号切换入口采用底部弹窗选择器，不使用原生 `<select>`。

理由：

- 账号数量变多后更适合搜索
- 更符合手机操作习惯
- 可以在列表里同时展示账号、服务器和类型信息

### 2. Hero Summary

显示当前账号最关键的 4 个指标。

建议优先级：

- 当前运行状态
- 累计成交额
- 净收益估算
- 当前风险提示数量

若当前账号无 live 数据，则展示：

- 所属服务器
- 可用模块
- 快速入口

### 3. Competition Section

显示当前账号关注的比赛卡片。

卡片信息建议：

- 比赛名
- 市场类型
- 当前阶段
- 命中的比赛标的
- 入口按钮：打开榜单页

这里只做摘要，不在 `/console` 内完整重绘整个榜单页。

### 4. Runtime Section

根据账号 `kind` 展示不同内容：

- futures 账号：合约策略摘要卡
- spot 账号：现货策略摘要卡
- mixed 账号：分两组展示

每张卡片只展示：

- 币种
- runner 运行状态
- 当前仓位或库存
- 当前挂单数
- 累计成交额
- 净收益估算
- 快速入口按钮

### 5. Legacy Entry Section

保留现有页面深链，作为 Phase 1 的实际操作入口。

按钮建议：

- 打开监控页
- 打开执行台
- 打开策略总览
- 打开比赛榜单
- 打开基础测算页

### 6. Server Section

展示当前账号对应服务器的运行健康信息。

内容建议：

- 在线 / 离线
- base URL
- 最近探测时间
- 支持模块

## Mobile Interaction Rules

Phase 1 必须以 390px 左右宽度的手机视口为主标准设计。

具体规则：

- 不使用宽表格作为主呈现形式
- 控制台首页全部改用卡片
- 顶部上下文栏 sticky
- 账号切换两步内完成
- 所有深链按钮高度不小于 40px
- 卡片区块保持 12px 以上点击间距
- 单屏优先显示状态，不优先显示参数明细

以下信息不要放在首屏：

- 大段参数 JSON
- 完整挂单表
- 完整成交流水
- 大型可排序表格

这些信息继续留在原有页面。

## Deep Link Rules

控制台不复制旧页面功能，只提供一致的跳转方式。

统一规则：

- 所有深链从账号上下文生成
- 用户不需要自己记 IP 和端口
- 所有链接由 `base_url + path` 拼接生成
- `/` 和 `/hub` 统一作为控制台入口，不再直接暴露旧式入口页

默认支持的页面：

- `/monitor`
- `/spot_runner`
- `/competition_board`
- `/strategies`
- `/spot_strategies`
- `/basis`

若某节点无某模块能力，则按钮隐藏。

## API Outline

### `GET /api/console/registry`

返回：

- 服务器列表
- 账号列表
- 默认账号
- 比赛来源

用于：

- 初始化账号选择器
- 生成默认账号上下文
- 渲染页面导航

### `GET /api/console/overview?account_id=<ID>`

返回当前账号概览数据。

最小字段要求：

- `account`
- `server`
- `health`
- `summary`
- `competitions`
- `futures`
- `spot`
- `links`
- `warnings`
- `fetched_at`

## Error Handling

统一控制台必须对远端节点暂时不可用有明确降级能力。

规则：

- 服务器离线时，仍能展示账号元信息、服务器信息和旧链接
- 单个模块请求失败时，只显示该模块 unavailable 或 warning 卡片，不影响其它模块
- 比赛摘要失败时，不影响 runtime 摘要
- 远端认证缺失时，在 `warnings` 中明确标识对应 `server_id`
- 所有失败都给出简短错误文本，不抛整页空白

## Security Requirements

Phase 1 至少保证以下安全边界：

- 统一入口继续沿用现有 web 认证能力
- 远端节点密码只从环境变量读取
- 注册表不存密码
- 注册表不存 Binance API key
- 聚合接口不回传节点密码
- 页面上不暴露内网或备用地址，除非明确配置为公开入口

## Testing Strategy

至少覆盖以下测试：

- 注册表加载与字段校验
- 默认账号选择逻辑
- 深链拼接逻辑
- `/` 与 `/hub` 到 `/console` 的跳转
- `/api/console/registry` 返回稳定结构
- `/api/console/overview` 在节点在线 / 离线两种情况下都返回稳定结构
- 控制台首页在 390px 宽度下无需横向滚动
- 两步内完成账号切换

## Acceptance Criteria

满足以下条件即可认为 Phase 1 完成：

1. 用户访问 `/console` 后，可以看到账号列表并切换账号
2. 用户访问 `/` 或 `/hub` 时，会进入 `/console`
3. 切换账号后，页面能刷新出对应服务器和对应模块摘要
4. 手机宽度下，页面无需横向滚动即可完成主要查看和跳转
5. 用户无需手动输入服务器地址即可进入对应旧页面
6. 任一远端节点故障时，控制台仍能展示其它模块与可用入口
7. 全流程不需要把 Binance 密钥集中搬到统一入口

## Risks

- 若节点 Basic Auth 配置不一致，统一入口的服务端聚合会增加配置复杂度
- 若远端节点响应慢，首页概览可能出现局部卡顿，需要设置超时和并发抓取
- `web.py` 当前已较大，若不做最小边界拆分，后续继续加 `/console` 容易让维护成本进一步上升
- 若注册表中的账号与真实节点部署关系不一致，控制台会产生错误深链和错误摘要

## Recommendation

Phase 1 推荐采取“统一控制台 + 服务端聚合 + 旧页面深链 + 单体内分模块”的方案。

这条路径的优点是：

- 对现有 runner 和监控逻辑侵入最小
- 能最快把多服务器、多账号、手机切换这三个核心问题先解决
- 不增加额外前端工程和部署复杂度
- 后续如果要做 Phase 2 的统一操作代理和标准化节点协议，可以直接在本设计之上继续扩展
