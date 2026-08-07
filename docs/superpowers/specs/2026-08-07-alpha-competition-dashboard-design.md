# Alpha 交易赛门槛面板设计

日期：2026-08-07
状态：已确认，待实现

## 1. 背景

生产页面 `http://43.156.35.110/alpha/` 当前展示 `ALPHA_SYMBOLS` 中币种的价格、1 分钟成交额、1 小时成交额和量能倍数。用户需要在保留该行情监控的基础上，增加 Alpha 交易赛进度和入榜门槛估算。

当前服务位于主机 `srv-43-156-35-110`：

- 服务：`binance-alpha-dashboard.service`
- 目录：`/home/ubuntu/binance-alpha-volume-alert`
- 币种配置：`/home/ubuntu/.config/binance-alpha-volume-alert.env` 中的 `ALPHA_SYMBOLS`
- 页面程序：`dashboard.py`
- Alpha 行情客户端：`binance_alpha_volume_alert.py`

## 2. 目标

对 `ALPHA_SYMBOLS` 中每个币种自动发现最新 Binance Alpha 交易赛规则，并展示当前正在进行的一轮：

- 当前早鸟倍速；
- 当前轮加权总交易量；
- 官方获奖人数；
- 加权总量除以获奖人数得到的平均量；
- 观察线、参考线和安全线；
- 数据来源、更新时间、轮次状态及官方公告链接。

原实时行情表和 `Check Alert` 行为保持不变。

## 3. 非目标

- 不估算或计入“新锐交易者”个人 1.2 倍加成；
- 不计算个人账户的实际排名、个人有效交易量或手续费；
- 不保存跨轮次的长期历史数据库；
- 不新建独立服务或 SQLite 数据库；
- 不把第一轮和第二轮合并累计；
- 不以无法验证的数据冒充官方交易赛总量。

## 4. 已确认口径

### 4.1 当前轮

只统计当前 UTC 时间所在的活动轮次。所有轮次和活动日使用左闭右开区间 `[start, end)`，因此到达下一轮开始时刻后立即以新一轮开始时间为零点重新计算，不重复计入边界成交。若当前不在任何轮次内，则展示“未开始”或“本轮已结束”，不输出伪造门槛。

### 4.2 加权总交易量

逐个官方活动日计算：

```text
当日有效交易量 = 当日实际交易量 × 当日早鸟倍速
当前轮加权总交易量 = Σ 当日有效交易量
```

活动日边界使用官方公告给出的 UTC 起止时间，不使用自然日零点。当前公告通常以 13:00 UTC 为日界，但实现不得写死 13:00。

### 4.3 数据源优先级

1. 如果 Binance 已验证的公开活动响应中明确提供带时间戳的当前轮官方总交易量，优先采用并标记为“官方”。实现不得猜测未验证接口或把公告中的其他数值当成交易总量；未发现明确字段时，官方总量提供器直接返回“不可用”。
2. 若官方规则页没有公开总量，则使用 Binance Alpha 交易接口的 1 小时 K 线 quote volume，按官方活动日边界和倍速累计，并标记为“Alpha K线估算”。

K 线估算按实际交易对选择 USDT 或 USDC，数值统一显示为 `U`。USDC 与 USDT 在该估算中按 1:1 名义美元处理。估算值只覆盖公开 Alpha 交易接口可见成交，页面必须保留来源标签，不能表示为官方排行榜总量。

### 4.4 获奖人数和门槛

`入榜人数` 使用官方公告公布的获奖名额，例如“top 2,500 users”。

```text
平均量 = 当前轮加权总交易量 ÷ 官方获奖人数
观察线 = 平均量 × 0.4
参考线 = 平均量 × 0.6
安全线 = 平均量 × 1.0
```

三档均为市场总量启发式估算，不是官方实际榜尾。页面需展示“估算门槛”说明。

## 5. 官方规则自动发现

### 5.1 公告列表

使用 Binance 官方 CMS 公共接口：

```text
GET https://www.binance.com/bapi/composite/v1/public/cms/article/list/query
    ?type=1&catalogId=93&pageNo=1&pageSize=50
```

`catalogId=93` 为 “Latest Activities”。对每个 `ALPHA_SYMBOLS` 币种：

1. 仅匹配标题以 `Binance Alpha Trading Competition:` 开头的公告；
2. 标题必须含精确的 `(SYMBOL)`，避免 `O` 等短代码误匹配；
3. 同一币种存在多次活动时，按 `releaseDate` 选择最新公告；
4. 保存公告 `code`、标题、发布日期及官方详情链接。

正常刷新只读取最新 50 条。初次启动、规则缓存未命中或目标币种在第一页缺失时，按页回溯最近 60 天，找到该币种最新公告或超过 60 天截止线后停止。已验证公告按 `code` 持久化，不能因为公告被新内容挤出第一页就删除。

### 5.2 公告详情

```text
GET https://www.binance.com/bapi/composite/v1/public/cms/article/detail/query
    ?articleCode={code}
```

详情中的 `body` 是 JSON 节点树。解析器递归提取所有文本节点，再解析：

- 第一轮和第二轮的起止 UTC 时间；
- 官方 top-N 获奖人数；
- Day 1 至 Day 7 的时间区间及早鸟倍速；
- 公告标题、代码和更新时间。

解析结果必须通过以下验证：

- 至少存在一个完整轮次；
- 当前轮起止时间有效且不倒置；
- 每个活动日区间连续、无重叠并覆盖当前轮；
- 倍速为正数；
- 获奖人数为正整数。

验证失败时，该币种显示“规则待补充”，不得猜测规则。

## 6. 服务端结构

生产实现必须先落入当前 Git 仓库的 `main`，不能让 `/home/ubuntu/binance-alpha-volume-alert` 继续成为唯一源码。新增 `src/grid_optimizer/alpha_competition_metrics.py`，把交易赛逻辑与现有行情快照隔离；页面入口归入 `src/grid_optimizer/alpha_competition_dashboard.py`，并由安装脚本更新现有 systemd 服务。交易赛模块包含四个边界清晰的部分：

### 6.1 `BinanceCompetitionRuleProvider`

- 查询公告列表和详情；
- 按精确币种匹配公告；
- 解析并验证轮次、获奖人数和每日倍速；
- 返回结构化 `CompetitionRule`。

### 6.2 `CompetitionVolumeProvider`

- 先尝试获取官方公开总量；
- 官方总量不存在时调用 Alpha K 线；
- 按当前轮的每日窗口计算加权总量；
- 返回数值、来源、数据截止时间及是否过期。

K 线请求显式传入当前轮起止时间；单轮 7 天使用不超过 200 根的接口限制即可覆盖。当前进行中的 1 小时 K 线可计入其实时 quote volume，确保页面不会固定延迟一小时。响应按 open time 去重，每根 K 线按其 open time 落入对应活动日左闭右开窗口，轮次结束后的 K 线不得计入。

### 6.3 `CompetitionMetricsCalculator`

- 选择当前轮；
- 找出当前 Day 和当前倍速；
- 计算平均量及三档门槛；
- 不计新锐交易者加成；
- 输出纯数据结构，便于单元测试。

### 6.4 `CompetitionCache`

- 官方规则缓存 6 小时；
- 成交量与门槛缓存 60 秒；
- 规则最近成功值持久化到 `/home/ubuntu/.cache/binance-alpha-volume-alert/competition_rules.json`，启动时按需创建父目录；
- 规则缓存按公告代码和币种保存；新增币种或缓存未命中时立即获取，不等待已有币种的 6 小时 TTL；
- 网络失败时可继续使用最近成功规则，但标记 `stale=true`；
- 成交量最近成功值只需进程内保存；
- 单个币种失败不得中断其他币种。

## 7. API 设计

保留现有 `/api/snapshot`，新增：

```text
GET /api/competition
```

响应示意：

```json
{
  "generatedAtUtc": "2026-08-07T01:00:00+00:00",
  "rows": [
    {
      "symbol": "QUID",
      "name": "Squid",
      "round": 1,
      "day": 2,
      "roundStartUtc": "2026-08-05T13:00:00+00:00",
      "roundEndUtc": "2026-08-12T13:00:00+00:00",
      "currentMultiplier": 3.0,
      "weightedVolume": 4800000.0,
      "volumeSource": "alpha_kline_estimate",
      "volumeUpdatedAtUtc": "2026-08-07T00:59:59+00:00",
      "winnerCount": 2500,
      "averageVolume": 1920.0,
      "watchThreshold": 768.0,
      "referenceThreshold": 1152.0,
      "safeThreshold": 1920.0,
      "articleUrl": "https://www.binance.com/en/support/announcement/detail/18d7255a59f74b3d90139c755cc806dd",
      "stale": false,
      "status": "active"
    }
  ],
  "errors": []
}
```

状态值限定为：`upcoming`、`active`、`between_rounds`、`ended`、`rule_unavailable`、`volume_unavailable`。

现有 `Refresh` 使用独立请求刷新 `/api/snapshot` 和 `/api/competition`。任一请求失败时仍渲染另一部分。`Check Alert` 继续只执行现有告警检查。

## 8. 页面设计

采用已确认的 B 方案：上下分区。

### 8.1 上方：交易赛门槛

按 `ALPHA_SYMBOLS` 配置顺序展示：

- 币种名称；
- 当前轮次、Day 和结束倒计时；
- 当前早鸟倍速；
- 加权总交易量；
- 数据来源和更新时间；
- 官方获奖人数；
- 平均量；
- 观察线、参考线、安全线；
- 官方公告链接；
- 过期或错误状态。

桌面端使用比较表格。移动端在窄屏下切换为纵向币种卡片，避免横向挤压和文字竖排。

### 8.2 下方：实时行情监控

保持现有 KPI、行情表、刷新按钮和告警按钮，不改变现有字段含义和告警逻辑。

### 8.3 状态展示

- `官方`：绿色来源标签；
- `Alpha K线估算`：中性蓝色来源标签；
- `数据已过期`：黄色提示并保留最后成功值；
- `规则待补充` 或 `成交量不可用`：红色行内错误；
- 未开始、轮间空档和已结束使用明确文本，不显示零门槛。

## 9. 错误处理

- CMS 列表失败：使用未过期或最近成功规则缓存；无缓存则逐币种返回规则错误；
- 单篇公告解析失败：只影响对应币种；
- 单个交易对 K 线失败：只影响对应币种；
- 官方总量字段缺失：正常降级，不作为错误；
- K 线估算失败且无最近成功值：不计算门槛；
- 缓存内容损坏：忽略损坏缓存并重新获取；
- API 返回错误时不输出半计算结果或默认零值。

## 10. 测试与验收

### 10.1 单元测试

- 以 QUID、GRVT、O、PRL、CAP 官方公告响应作为固定测试夹具；
- 精确币种标题匹配，特别覆盖短代码 `O`；
- 同币种多篇公告选择最新一篇；
- 公告节点树文本提取；
- 两轮活动解析；
- Day 1 至 Day 7 倍速解析；
- top-N 获奖人数解析；
- 当前轮、轮间空档和已结束状态选择；
- 非自然日边界的 K 线分组；
- USDT 与 USDC 交易对；
- 加权总量和 `0.4/0.6/1.0` 三档计算；
- 不计新锐交易者加成；
- 规则缓存过期、最近成功值降级及损坏缓存；
- 单币种错误隔离。

### 10.2 API 测试

- `/api/competition` JSON 字段和状态；
- `/api/snapshot` 行为不变；
- 两个端点之一失败时另一个仍可用；
- Basic Auth 继续保护新端点。

### 10.3 页面验证

- 桌面端交易赛表在上、行情表在下；
- 移动端交易赛行转换为卡片；
- 无文字竖排、列溢出或遮挡；
- 数据来源、更新时间、倒计时和状态可见；
- Refresh 同时更新两块，Check Alert 不变。

### 10.4 生产验收

部署前备份远端源文件。重启 `binance-alpha-dashboard.service` 后必须验证：

- `systemctl is-active` 返回 `active`；
- 当前 `ALPHA_SYMBOLS` 每个币种均有独立结果或明确错误；
- 至少抽查一个当前活动币种的官方轮次、获奖人数和当前倍速；
- 服务端实时计算与页面显示一致；
- 原实时行情快照和告警接口仍正常；
- 浏览器桌面及移动视口视觉检查通过。

## 11. 部署与回滚

只修改 Alpha 监控服务相关文件。实现和测试先在本地 `main` 完成；部署前确认本地 `main` 与 `origin/main` 的关系，并记录 110 主机 `/home/ubuntu/wangge` 的 commit 和工作树状态。分别备份现有 systemd unit、`dashboard.py`、`binance_alpha_volume_alert.py`（如有改动）及新增模块。若服务启动、API 或页面验证失败，恢复备份与旧 unit 并重启服务。

本次不修改 Wangge 网格策略、交易参数、订单、仓位或其他生产服务。
