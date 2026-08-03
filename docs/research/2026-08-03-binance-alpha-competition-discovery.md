# Binance Alpha 交易赛币种自动发现研究

日期：2026-08-03
范围：仅使用 Binance 第一方网页与 `binance.com` 公开 BAPI；未修改生产代码。

## 结论

可以自动识别当前进行中的 Binance Alpha 交易赛币种，但不能只依赖 Alpha token 列表，也没有发现一个可直接返回“进行中交易赛币种”的明确状态字段。

最小且可靠的判定链是：

1. 从 Binance 公告栏目 BAPI 拉取最近活动公告。
2. 只保留标题以 `Binance Alpha Trading Competition:` 开头的公告，排除普通 Alpha 上币、空投和 Deposit Campaign。
3. 用公告 `code` 请求详情 BAPI。
4. 从详情正文的 `... Trading Competition Promotion Period: <start> (UTC) to <end> (UTC)` 解析一个或多个 UTC 时间段。
5. 当前 UTC 时间落入任一时间段时，该公告对应币种为“当前进行中”；所有时间段均结束后立即从监控名单剔除。

截至本次取样时间，按公告正文周期计算，当前名单为 `CAP`、`PRL`、`O`、`ZEST`、`QAIT`；`UP` 已于 2026-07-31 13:00 UTC 结束，应剔除。

## 第一方数据源

### 1. 活动公告列表

```text
GET https://www.binance.com/bapi/composite/v1/public/cms/article/list/query?type=1&catalogId=93&pageNo=1&pageSize=50
```

- `catalogId=93` 在详情元数据中对应 `firstCatalogName: "Latest Activities"`。
- 响应成功标志为 `code: "000000"`。
- `data.articles[]` 的有用字段：
  - `id`
  - `code`：详情请求与公告身份的稳定键
  - `title`
- 列表响应中的 `body`、`publishDate` 等字段可能为 `null`，不能靠列表直接判断活动状态。

2026-08-03 的真实标题样例：

```json
{
  "code": "b935934fe980452fa96722ecaf30abde",
  "title": "Binance Alpha Trading Competition: Trade Cap (CAP) and Share $200K Worth of Rewards (2026-07-30)"
}
```

同一页还返回了 PRL、O、ZEST、QAIT、UP 等 Alpha 交易赛公告。普通 Alpha 活动也会出现在活动公告中，例如：

```text
Binance Wallet Alpha Deposit Campaign: Deposit Grvt (GRVT) and Share $20K Worth of Rewards
```

因此必须使用交易赛标题前缀过滤，不能把“标题包含 Alpha”作为充分条件。

来源：[Binance Latest Activities BAPI](https://www.binance.com/bapi/composite/v1/public/cms/article/list/query?type=1&catalogId=93&pageNo=1&pageSize=50)

### 2. 公告详情

```text
GET https://www.binance.com/bapi/composite/v1/public/cms/article/detail/query?articleCode={code}
```

CAP 样例：

```text
GET https://www.binance.com/bapi/composite/v1/public/cms/article/detail/query?articleCode=b935934fe980452fa96722ecaf30abde
```

关键字段：

```json
{
  "code": "000000",
  "data": {
    "id": 281335,
    "code": "b935934fe980452fa96722ecaf30abde",
    "title": "Binance Alpha Trading Competition: Trade Cap (CAP) and Share $200K Worth of Rewards (2026-07-30)",
    "publishDate": 1785411007454,
    "firstCatalogId": 93,
    "firstCatalogName": "Latest Activities",
    "body": "{...JSON encoded rich-text tree...}"
  }
}
```

`data.body` 不是纯 HTML，而是一个 JSON 字符串。解析这层 JSON 后，递归收集 `node == "text"` 的 `text` 字段并进行 HTML entity 解码、空白归一化，可得到正文。CAP 公告正文开头包含：

```text
1st CAP Trading Competition Promotion Period: 2026-07-30 13:00 (UTC) to 2026-08-06 13:00 (UTC)
2nd CAP Trading Competition Promotion Period: 2026-08-06 13:00 (UTC) to 2026-08-13 13:00 (UTC)
```

来源：[Binance CAP 公告详情 BAPI](https://www.binance.com/bapi/composite/v1/public/cms/article/detail/query?articleCode=b935934fe980452fa96722ecaf30abde)

### 3. Alpha token 列表只用于符号映射和交易可用性校验

```text
GET https://www.binance.com/bapi/defi/v1/public/wallet-direct/buw/wallet/cex/alpha/all/token/list
```

关键字段包括：

```json
{
  "symbol": "CAP",
  "name": "Cap",
  "alphaId": "ALPHA_1005",
  "offline": false,
  "offsell": false,
  "fullyDelisted": false,
  "listingTime": 1782475200000,
  "onlineTge": true,
  "onlineAirdrop": false
}
```

该接口当前返回所有 Alpha token，包含 `onlineTge`、`onlineAirdrop`、`offline` 等字段，但没有交易赛 ID、交易赛开始/结束时间或 `competitionActive` 一类字段。`onlineTge` / `onlineAirdrop` 也不能代表交易赛状态：本次进行中的币种在这些字段上的取值并不一致。因此：

- 它能确认公告符号对应的 `alphaId`，用于拼出 `ALPHA_xxxUSDT`。
- 它能辅助排除已经 `offline` / `fullyDelisted` 的币。
- 它不能区分“正在交易赛”“已结束交易赛”和“普通 Alpha 上币”。

来源：[Binance Alpha token list BAPI](https://www.binance.com/bapi/defi/v1/public/wallet-direct/buw/wallet/cex/alpha/all/token/list)

## 状态、币种与新增/剔除判定

### 是否存在直接状态字段

没有在已验证的第一方响应中发现可直接区分以下三类的字段：

- 当前进行中的 Alpha 交易赛
- 已结束的 Alpha 交易赛
- 普通 Alpha 上币/空投/充值活动

公告详情中的 `publishDate` 只是发布时间，不能代表比赛周期。可靠状态来自正文明确写出的 UTC `Promotion Period`。

### 交易赛识别

列表初筛建议严格匹配：

```regex
^Binance Alpha Trading Competition:\s*Trade\b
```

然后从标题中的 `Trade <name> (<symbol>) and ...` 提取符号，并用详情正文再次校验存在：

```text
<symbol> Trading Competition Promotion Period
```

不要只匹配 `Alpha` 或 `Competition`，否则会混入 Alpha Deposit Campaign、Alpha 空投或非 Alpha 的 P2P/Futures 交易赛。

### 活跃状态

从正文解析所有主活动周期：

```regex
(?:\d+(?:st|nd|rd|th)\s+)?([A-Z0-9._-]+)\s+Trading Competition Promotion Period:\s*
(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2})\s*\(UTC\)\s+to\s+
(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2})\s*\(UTC\)
```

实现时不应只用一条正则扫整篇正文后取最早/最晚的所有日期，因为正文后半段还包含每日倍率、奖励领取截止时间等其他 UTC 时间。应限定在 `Trading Competition Promotion Period` 标签附近，并保留公告中的多个主周期。

状态规则：

- `now < min(start)`：已公告但尚未开始，不监控成交量。
- 任一周期满足 `start <= now < end`：当前进行中，加入监控。
- 周期间无缝或重叠：取周期并集。
- `now >= max(end)`：已结束，从监控名单剔除。
- 活动公告被 Binance 更新时，以同一 `code` 最新详情的正文周期为准。

### 新增

每次刷新公告列表后，用 `article code` 去重。发现新的符合标题前缀的 `code` 时请求详情；若已进入周期则立即加入，若尚未开始则保存并在开始时间自动加入。

不要用币种符号本身作为公告唯一键：同一币种未来可能有多轮不同公告，或一个公告包含多个连续周期。

### 剔除

满足以下任一条件时从本轮监控名单剔除：

1. 当前时间已达到该公告所有 Promotion Period 的最后结束时间。
2. Binance 更新同一公告，新的详情已不再包含有效交易赛周期。
3. Alpha token list 中对应币种为 `offline: true` 或 `fullyDelisted: true`；这是安全兜底，不取代公告周期判定。

公告从列表第一页消失不能直接视为剔除。程序必须持久化已发现公告及其结束时间，否则公告被新内容挤出第一页时会错误漏监控。

## 2026-08-03 样例状态

以下周期均来自各自 Binance 公告详情正文：

| symbol | article code | Promotion Periods（UTC） | 2026-08-03 状态 |
|---|---|---|---|
| CAP | `b935934fe980452fa96722ecaf30abde` | 07-30 13:00 → 08-06 13:00；08-06 13:00 → 08-13 13:00 | 进行中 |
| PRL | `21f8a7399c2542e68452f266ecb0a8ef` | 07-29 11:00 → 08-05 11:00；08-05 11:00 → 08-12 11:00 | 进行中 |
| O | `cc164ab36d114ea2a123309c9bbed74f` | 07-23 13:00 → 07-30 13:00；07-30 13:00 → 08-06 13:00 | 进行中 |
| ZEST | `16a8223b13034f7a9c4647e062d16f8a` | 07-22 13:00 → 07-29 13:00；07-29 13:00 → 08-05 13:00 | 进行中 |
| QAIT | `b718aecb004740a6a5c5a7290fe70744` | 07-21 13:00 → 07-28 13:00；07-28 13:00 → 08-04 13:00 | 进行中 |
| UP | `c5dd00b8cb0d4f4abc73edd444e3e4c9` | 07-17 13:00 → 07-24 13:00；07-24 13:00 → 07-31 13:00 | 已结束，剔除 |

CAP、PRL、O、ZEST、QAIT 详情来源分别为：

- [CAP](https://www.binance.com/bapi/composite/v1/public/cms/article/detail/query?articleCode=b935934fe980452fa96722ecaf30abde)
- [PRL](https://www.binance.com/bapi/composite/v1/public/cms/article/detail/query?articleCode=21f8a7399c2542e68452f266ecb0a8ef)
- [O](https://www.binance.com/bapi/composite/v1/public/cms/article/detail/query?articleCode=cc164ab36d114ea2a123309c9bbed74f)
- [ZEST](https://www.binance.com/bapi/composite/v1/public/cms/article/detail/query?articleCode=16a8223b13034f7a9c4647e062d16f8a)
- [QAIT](https://www.binance.com/bapi/composite/v1/public/cms/article/detail/query?articleCode=b718aecb004740a6a5c5a7290fe70744)
- [UP](https://www.binance.com/bapi/composite/v1/public/cms/article/detail/query?articleCode=c5dd00b8cb0d4f4abc73edd444e3e4c9)

## 失败与结构变化风险

这些 BAPI 是 Binance 网页公开使用的第一方接口，但不是对外承诺兼容性的正式开发者 API，主要风险如下：

- `catalogId=93`、路径或响应结构可能变化。
- `body` 的富文本节点结构可能变化，或 Binance 改用 `contentJson`。
- 标题措辞、大小写、币种符号格式可能变化。
- 活动正文可能从两个周期改为一个或更多周期，或改用不同日期格式。
- 接口会出现 `429 Too Many Requests`；实测短时间连续请求 CMS BAPI 可被限流。
- `binance.com` 可能因地域、WAF 或网络线路超时；不能把抓取失败解释为“当前没有活动”。
- 公告可能被修改、撤销或从列表中隐藏。
- 只读第一页会在公告密集时漏掉仍未结束但已被挤到后页的活动。

保护措施：

- 使用明确的 `User-Agent`、`Accept: application/json`，设置连接/读取超时。
- 公告列表低频刷新即可，例如每 5 分钟；成交量检查仍可每分钟运行。
- 对 429/5xx 做指数退避，不高频重试。
- 持久化 `article code`、标题、币种、周期、最后成功刷新时间和原始详情摘要。
- 拉取或解析失败时继续使用最近一次成功名单（last-known-good），并记录错误；绝不能把名单清空。
- 只有成功获得并成功解析一轮公告数据后，才应用新增/剔除结果。
- 初次启动分页回溯至少 30～60 天；之后依赖持久化缓存，不要求每轮重扫历史页。
- 解析结果为零但上一轮有活动时视为异常，保留旧名单并报警/写日志。
- 同一公告定期按 `lastUpdateTime` 或固定周期重新拉详情，以捕捉 Binance 修改活动时间。

## 推荐最小方案

在现有成交量告警脚本前增加一个很薄的“比赛名单发现”步骤，不引入浏览器自动化：

1. 每 5 分钟请求 `catalogId=93` 的公告列表，正常轮询读取前 50 条；初次启动额外分页回溯 60 天。
2. 标题按严格前缀过滤，用 `code` 做公告主键。
3. 对新公告或已更新公告请求 detail BAPI，解析 `body` 富文本中的主 Promotion Period 和币种。
4. 计算所有 `start <= now_utc < end` 的公告币种集合，作为成交量监控名单。
5. 用 Alpha token list 把 `symbol` 映射到 `alphaId`，并校验没有下线。
6. 成功刷新后原子写入本地 JSON 缓存；失败则沿用 last-known-good。
7. 成交量告警的 24 小时冷却状态按 `symbol` 保留；币种被剔除时无需删除其历史冷却记录，未来同币种重新进入新比赛时仍可按最近一次成功发信时间去重。

这套方案的优点是代码面最小、数据全来自 Binance、能自动新增和按 UTC 结束时间剔除，并且不会把普通 Alpha 上币误当作交易赛。不要用 `onlineAirdrop`、`onlineTge`、成交量排名或网页当前展示顺序猜测比赛名单。
