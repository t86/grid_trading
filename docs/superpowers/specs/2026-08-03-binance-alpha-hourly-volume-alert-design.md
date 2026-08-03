# Binance Alpha 交易赛小时成交额邮件告警设计

日期：2026-08-03
目标主机：Controller 110（`43.156.35.110`）
状态：已确认，待实施

## 1. 目标

自动发现当前正在进行的全球标准单币种 Binance Alpha Trading Competition，持续监控每个比赛币种最近 1 小时的 USDT 成交额。当某币种最近 1 小时成交额严格大于 `10,000,000 USDT` 时，通过 110 已配置的 QQ SMTP 邮箱发送提醒。同一币种在任意连续 24 小时内最多成功发送一次。

名单必须随 Binance 官方比赛周期自动变化：新比赛开始后自动加入，最后一个比赛阶段结束后自动剔除。剔除后的币种不再请求 K 线，也不再参与告警判断。

## 2. 已确认事实与口径

- 110 上现有 `/alpha/` 页面监控 `PRL`、`ZEST`、`CAP`、`QAIT`、`O`，由独立的 `binance-alpha-dashboard.service` 提供。
- 页面中的 `1h Vol` 是最近 60 根已收盘 1 分钟 K 线的 quote volume 之和，单位为 USDT。
- 旧告警规则是“最近一根已收盘 1 分钟 K 线成交额至少 60,000 USDT，冷却 30 分钟”。对应 timer 当前为 `disabled / inactive`，没有本次开机触发日志，也没有状态文件；旧规则不再保留。
- 110 已配置 QQ SMTP：`smtp.qq.com:587`，发件、收件地址及授权密码保存在现有专用环境文件 `/home/ubuntu/.config/binance-alpha-email.env`。实现不得输出、复制到 Git 或改写这些秘密值。
- Alpha token list 没有 `competitionActive` 一类字段，不能单独用来识别交易赛。
- Binance 官方 Latest Activities 公告列表与公告详情包含标准交易赛标题和精确 UTC `Promotion Period`，可作为名单事实来源。研究证据见 `docs/research/2026-08-03-binance-alpha-competition-discovery.md`。

## 3. 范围

### 3.1 纳入

只自动纳入满足以下条件的公告：

1. 来自 Binance 官方 Latest Activities 公告 API。
2. 标题严格符合全球标准单币种形式：`Binance Alpha Trading Competition: Trade <name> (<symbol>) ...`。
3. 详情正文中能解析出与该 symbol 一致的一个或多个 `Trading Competition Promotion Period` UTC 时间段。
4. 当前时间落入至少一个时间段，采用半开区间 `start <= now < end`。
5. Alpha token list 能把 symbol 唯一映射到可交易的 `alphaId`，且 `offline`、`fullyDelisted` 均不为真。

### 3.2 排除

- 地区限定活动，例如 `Balkan Exclusive`、`MENA Exclusive`。
- 一个公告覆盖多个币种的生态赛、链上赛或代币化证券活动。
- Alpha Deposit Campaign、空投、TGE、普通上币公告。
- 通过成交量排名、`onlineAirdrop`、`onlineTge`、网页展示顺序或人工猜测得出的币种。
- 旧的 1 分钟 60,000 USDT 放量告警。
- 对交易策略、现货仓位、合约仓位或冻结账本进行任何修改。

如果 Binance 日后改变公告格式，程序应保留最近一次可信发现结果并报告解析错误，不应扩大匹配规则去猜测新格式。

## 4. 代码与服务边界

生产逻辑落入当前 Git 仓库，不继续把 `/home/ubuntu/binance-alpha-volume-alert` 作为唯一源码。

新增两个聚焦模块：

- `src/grid_optimizer/alpha_competition_volume_monitor.py`
  - Binance CMS 公告列表、详情和 Alpha 市场数据客户端。
  - 公告正文解析、比赛周期计算和 active symbol 发现。
  - 最近 1 小时成交额计算、阈值判断、24 小时去重、QQ SMTP 发信和状态持久化。
  - 提供一次性 CLI，供 systemd oneshot 每分钟调用。
- `src/grid_optimizer/alpha_competition_dashboard.py`
  - 复用同一发现与行情逻辑提供 `/alpha/` 页面和只读 snapshot API。
  - 页面展示自动发现的当前名单、比赛结束时间、最近 1 小时成交额和告警状态。
  - Dashboard 不负责后台定时发信，避免网页进程重启或多线程造成重复发送。

新增 110 安装脚本，沿用仓库现有 `deploy/oracle/install_*` 风格，安装或更新：

- `binance-alpha-dashboard.service`：从 `/home/ubuntu/wangge` 运行仓库内 Dashboard 模块。
- `binance-alpha-volume-alert-check.service`：运行一次监控检查并加载专用 Alpha 与 QQ 邮箱环境文件。
- `binance-alpha-volume-alert-check.timer`：每 60 秒触发一次，部署验证完成后启用。

Nginx 已有 `/alpha/` 反向代理时不改配置；只有现有 upstream 与新 Dashboard 端口不一致时才做最小调整。

## 5. 官方比赛名单自动发现

### 5.1 数据源

公告列表：

```text
GET https://www.binance.com/bapi/composite/v1/public/cms/article/list/query
    ?type=1&catalogId=93&pageNo=1&pageSize=50
```

公告详情：

```text
GET https://www.binance.com/bapi/composite/v1/public/cms/article/detail/query
    ?articleCode={code}
```

Alpha token 映射：

```text
GET https://www.binance.com/bapi/defi/v1/public/wallet-direct/buw/wallet/cex/alpha/all/token/list
```

### 5.2 发现节奏

- systemd 每分钟启动一次监控 CLI。
- CLI 从持久化状态读取上次成功发现时间。
- 距离上次成功发现不足 10 分钟时，不请求 CMS，只根据缓存周期和当前 UTC 时间计算 active symbols。
- 达到 10 分钟时刷新公告列表。
- 初次部署或没有缓存时，分页回溯最近 60 天的标准交易赛公告。
- 后续普通刷新读取最新 50 条；已发现公告按 `article code` 持久化，不因公告被挤出第一页而丢失。
- 新公告立即拉详情。当前仍未结束的已知公告至少每 60 分钟重新拉一次详情，以接收 Binance 对比赛时间的修订。
- 对 429 和临时 5xx 使用有界指数退避；单轮任务不能无限等待。

### 5.3 解析与状态

- `data.body` 按 JSON 富文本树解析，递归收集 text 节点并归一化空白。
- 只解析带有 `<symbol> Trading Competition Promotion Period` 标签的时间段，不扫描正文中其他奖励或领取日期。
- 每个公告以 `article code` 为主键，保存标题、symbol、所有 UTC 周期、发布时间、详情更新时间和最后成功读取时间。
- 同一 symbol 可有多个不同公告；只要任一可信公告当前 active，该 symbol 就进入监控一次。
- 已公告但尚未开始的比赛保存在缓存中，到开始时间自动加入。
- 当前时间达到公告最后一个周期的 end 后，该公告不再使 symbol active。
- token 已下线时，即使公告周期尚未结束也不监控该 symbol。

## 6. 发现失败与 last-known-good

发现状态必须持久化，不能把网络失败解释成“没有比赛”。

- 列表、详情、富文本解析或 token list 失败：保留最近一次成功缓存，并记录可见错误。
- 缓存中的周期仍按本地 UTC 时间推进；达到明确 end 后照常剔除，不能因网络失败无限保留已结束币种。
- 新交易赛只有在官方接口恢复并成功解析后才会加入。
- 一次刷新返回零个标准公告，但过去 60 天缓存或响应规模显示明显异常时，不覆盖缓存。
- 初次启动没有可信缓存且官方发现失败时，本轮不监控、不发信，并以非零状态或明确错误结束。部署时必须先成功完成一次发现，再启用 timer。
- 状态文件采用同目录临时文件加原子替换，避免进程中断写出半个 JSON。

## 7. 小时成交额计算

对每个 active symbol：

1. 用 token list 的 `alphaId` 组成交易对 `ALPHA_xxxUSDT`。
2. 请求 `1m` K 线，排除仍在形成的最后一根 K 线。
3. 取最近 60 根已收盘 K 线的 quote volume 字段并求和。
4. 必须得到 60 根时间连续、间隔为 1 分钟的已收盘 K 线；数据不足、重复或断档时跳过该币种并记录错误，避免使用不完整窗口判断。
5. 告警条件为 `hour_quote_volume > 10_000_000`，等于 10,000,000 不触发。

窗口会随每分钟执行滚动，不按自然整点分桶。

## 8. 邮件与 24 小时去重

### 8.1 候选选择

某 active symbol 同时满足以下条件才成为本轮候选：

- 完整最近 1 小时成交额严格大于阈值。
- 状态中没有该 symbol 的成功发送记录，或当前时间距离上次成功发送时间已达到 24 小时。

币种被剔除时不删除其发送历史。同一币种以后参加新一轮比赛时，仍遵循用户要求的任意连续 24 小时最多一次。

### 8.2 合并邮件

同一分钟有多个候选时合并为一封邮件。建议标题：

```text
【Binance Alpha】1小时成交额突破1000万：CAP、PRL
```

正文对每个币种列出：

- symbol、name、alphaId 和交易对。
- 最近 1 小时成交额与阈值。
- 最近一根已收盘 K 线时间。
- 当前价格、24 小时成交额。
- 当前交易赛最后结束时间，分别显示 UTC 和 Asia/Shanghai。
- 触发检查时间。

### 8.3 成功与失败

- 只有 SMTP 完成发送后，才为邮件内每个 symbol 写入 `last_sent_at`、触发成交额和窗口结束时间。
- 发送失败时不进入冷却，下分钟仍满足条件则重试。
- 状态写入失败不得伪装为成功；进程应失败并留下日志。为避免“邮件已发但状态未写”造成下一分钟重复，发送前先确保状态目录可写，并在发送后立即原子落盘。仍存在 SMTP 成功后进程崩溃的极小重复窗口，接受这一边界，不引入数据库或分布式事务。

## 9. 状态文件

默认使用仓库外生产输出目录内的单个 JSON，例如：

```text
/home/ubuntu/wangge/output/alpha_competition_volume_monitor_state.json
```

逻辑结构：

```json
{
  "version": 1,
  "discovery": {
    "last_success_at": "UTC ISO timestamp",
    "last_error": null,
    "articles": {
      "article-code": {
        "symbol": "CAP",
        "title": "...",
        "periods": [{"start": "...", "end": "..."}],
        "last_detail_success_at": "..."
      }
    }
  },
  "alerts": {
    "CAP": {
      "last_sent_at": "UTC ISO timestamp",
      "hour_quote_volume": 12345678.9
    }
  },
  "last_run": {
    "checked_at": "UTC ISO timestamp",
    "active_symbols": ["CAP"],
    "errors": []
  }
}
```

不在状态中保存 SMTP 密码、Basic Auth 密码或任何 API 密钥。

## 10. Dashboard

`/alpha/` 保持 Basic Auth 和现有访问路径，数据改为来自自动发现结果。页面至少显示：

- 当前 active symbol 数量和列表。
- 每个币种的公告名称、比赛最后结束时间。
- 最近 1 分钟、最近 1 小时和 24 小时成交额。
- 最近一次成功名单刷新时间、当前是否使用缓存、最近错误。
- 1 小时告警阈值与该币种上次成功发信时间。

页面每 30 秒刷新行情不等于每次刷新 CMS；服务端继续执行 10 分钟发现缓存策略。Dashboard 只读，不提供触发真实邮件的按钮。

## 11. 测试策略

实现遵循测试先行。最小测试集合：

### 11.1 公告解析

- 标准单币种标题能提取 symbol。
- 地区限定、多币种、Deposit Campaign 和普通 Alpha 标题被排除。
- Binance 富文本 JSON 能抽取一个或多个 Promotion Period。
- 正文其他 UTC 日期不会被误当成比赛周期。
- `start <= now < end` 边界正确；到 end 的瞬间剔除。
- 同一 symbol 多公告去重为一个 active symbol。

### 11.2 发现缓存

- 新公告加入、未来公告到时加入、过期公告剔除。
- 公告从第一页消失但未到 end 时仍从缓存保留。
- 429、超时、损坏 JSON 和零结果异常不会清空 last-known-good。
- 缓存达到明确 end 后，即使 API 失败也会剔除。
- 初次无缓存且发现失败时不产生监控名单。

### 11.3 成交量与邮件

- 60 根连续已收盘 K 线正确求和。
- 少于 60 根、断档、重复或包含形成中 K 线时不告警。
- `10,000,000` 不触发，`10,000,000.01` 触发。
- 同币种 24 小时内跳过，达到 24 小时后可再次候选。
- 多币种合并成一封邮件。
- SMTP 成功才更新所有候选状态；失败不更新并允许重试。
- 已剔除币种不请求 K 线、不告警。

### 11.4 Dashboard 与部署

- snapshot 只返回当前 active symbols，并暴露缓存/错误状态。
- systemd 单元加载正确环境文件、工作目录和模块路径。
- 安装脚本语法检查通过。

完成后运行相关单测、完整测试套件、模块 CLI dry-run、110 上 systemd oneshot 手工执行和 Dashboard API/页面检查。

## 12. 部署与验证

1. 在本地 `main` 实现并通过测试。
2. 确认本地 `main` 与 `origin/main` 关系，按要求提交和推送。
3. 110 的 `/home/ubuntu/wangge` 当前不在 `main` 且有未跟踪文件；部署前不得直接 pull 覆盖。先记录现有 commit 和脏文件，确认这些旧文件不承载当前生产修改，再使用可回滚方式同步已验证的 `main`。
4. 备份现有 Alpha systemd unit 和 `/home/ubuntu/binance-alpha-volume-alert` 目录；不复制邮箱秘密。
5. 安装新 unit 但先不启用 timer。
6. 手工运行 discovery/dry-run，确认 active symbols、公告截止时间和 Dashboard 与官方样例一致。
7. 使用专门的邮件测试模式发送一封明确标注测试的 QQ 邮件，只有在用户已授权实际发送测试邮件时执行。
8. 启用并启动每分钟 timer，检查下一次触发时间和至少两轮无异常日志。
9. 验证 `/alpha/` 仍受 Basic Auth 保护，页面名单来自自动发现。
10. 保留旧目录备份和旧 unit 内容，确认稳定后再决定是否清理；本次不主动删除。

## 13. 验收标准

- 当前时间 2026-08-03 的自动名单能识别 `CAP`、`PRL`、`O`、`ZEST`、`QAIT`，并排除已结束的 `UP`。
- 新标准单币种 Alpha 交易赛公告进入周期后无需改配置即可加入。
- 到最后一个 Promotion Period end 后，该币种不再获取 K 线或触发邮件。
- 最近 1 小时成交额严格大于 1000 万才成为候选。
- 同一币种任意连续 24 小时最多成功发信一次。
- SMTP 失败不会消耗冷却时间。
- CMS 暂时失败不会清空仍有效的缓存名单，缓存中的明确过期币种仍准时剔除。
- QQ SMTP 密码和 Web Basic Auth 密码不进入 Git、日志或状态文件。
- `/alpha/` 显示的币种与告警监控使用同一份 active symbol 计算结果。

## 14. 非目标与已接受边界

- 不自动识别格式不同的地区赛、多币种生态赛或证券活动；需要纳入时另行明确规则。
- 不为 Binance 未承诺稳定性的网页 BAPI 建立复杂兼容层；结构变化时保留 last-known-good 并暴露错误。
- 不保证邮件 exactly-once。SMTP 已成功但进程在状态落盘前崩溃时可能重复一封；为消除该极小窗口而引入数据库事务不符合当前最小实现原则。
- 不新增数据库、消息队列、浏览器自动化或外部 SaaS 监控依赖。
