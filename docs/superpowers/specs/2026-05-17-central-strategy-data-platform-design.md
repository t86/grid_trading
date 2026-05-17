# 114 试点中心策略数据平台设计

## 背景

当前 runner 已经能通过本地 `output/*.jsonl` 保存一部分运行事件、成交审计和资金流水，但这种方式有几个核心问题：

- 数据分散在 111 / 114 / 150 等多台服务器，无法统一复盘。
- JSONL 容易被轮转、清理或覆盖，几天后历史上下文不完整。
- 交易所可以补成交和 K 线，但无法补出 runner 当时的参数、风险状态、停买原因、减仓逻辑和保护判断。
- 后续想回答“某币种在某种 K 线波动结构下，应该用什么模式和参数”，必须把每轮策略运行样本完整保存下来。

因此本次设计目标不是简单加一个成交流水库，而是建设一个中心化的策略实验数据库。每条样本都要能回答：

```text
某个币种
在某段市场/K线/波动环境下
使用某个策略模式和参数
实际产生了多少成交、收益、亏损、手续费、减仓、保护动作
```

## 目标

- 第一阶段在 114 服务器建设试点中心 PostgreSQL 数据库，长期保存 111 / 114 / 150 的策略数据。
- 111 / 114 / 150 等交易服务器保持本地 runner 架构，但把数据实时写回 114。
- 保留本地 JSONL 作为断网缓冲和二次回灌来源。
- 保存成交、资金流水、策略周期快照、K线/市场特征、参数版本和服务器来源。
- 支持后续按币种、K线环境、策略模式和参数指纹做复盘分析。
- 支持后续做“输入币种 + 最近K线 -> 推荐合理模式和参数”的分析服务。

## 非目标

- 不在第一阶段重写 runner 主循环。
- 不要求交易策略依赖数据库才能运行。
- 不把中心数据库作为下单前强依赖，数据库故障不能阻断实盘交易。
- 不在第一阶段引入复杂机器学习训练服务。
- 不一次性替换现有 JSONL / state 文件机制。
- 不要求所有历史数据都能百分百补齐；交易所可补成交和 K线，但历史策略决策上下文只能尽量从现存 JSONL 回灌。

## 核心原则

### 数据完整性优先

成交结果、策略参数、市场环境、风控判断必须统一留存。只存成交不足以分析策略好坏，因为不知道当时为什么开仓、为什么减仓、为什么暂停、为什么 step 变宽。

### 交易安全优先

数据库写入失败不能影响下单、撤单、减仓、保护。实盘 runner 的正确行为优先于数据落盘。

### 可回灌

所有实时写库都必须有本地 JSONL 备份。断网或中心库短故障后，通过 backfill 恢复。

### 可追溯

所有派生字段都要保留原始 payload，例如 raw trade、raw income、raw summary、参数 JSON。后续指标口径变化时，可以重新计算。

### 可渐进

先做中心库和写入链路，再做 K线事实表，再做分析 API，最后做参数推荐。

## 推荐架构

```text
111 / 114 / 150 / 其他交易服务器
  - 继续跑现有 runner
  - 本地继续写 state / JSONL
  - 实时把成交、资金流水、周期快照写入 114 PostgreSQL
  - 写库失败只记录错误，不阻断交易
  - 定时 backfill 补漏

114
  - 第一阶段 PostgreSQL 试点中心库
  - 备份任务
  - schema 初始化和迁移
  - 数据质量巡检
  - 分析 API / 图表页面
  - 后续参数推荐服务

未来可迁移到 Seoul 同 VPC 的独立数据库服务器
  - 2C4G 或更高
  - 独立数据盘
  - 承担长期 K线采集、分析报表和推荐任务
```

## 网络拓扑与访问方式

数据库流量优先走腾讯云内网/VPC，不建议让 111 / 114 / 150 通过公网访问 PostgreSQL。原因是数据库会长期保存完整策略与成交数据，公网暴露会增加安全风险，也会引入公网链路抖动、出口策略和安全组误配风险。

但是内网访问不能只按“都是腾讯云服务器”假设成立，必须实测同 VPC、路由和安全组。

2026-05-17 实测结果：

| 服务器 | 公网 IP | 腾讯云 local-ipv4 | 内网网段 |
| --- | --- | --- | --- |
| 110 | 43.156.35.110 | 10.3.0.14 | 10.3.0.0/22 |
| 111 | 43.155.136.111 | 10.8.0.11 | 10.8.0.0/22 |
| 114 | 43.155.163.114 | 10.8.0.16 | 10.8.0.0/22 |
| 150 | 43.131.232.150 | 10.8.0.2 | 10.8.0.0/22 |

当前 111 / 114 / 150 到 110 的 `10.3.0.14` ping 不通，`10.3.0.14:5432` TCP 连接超时。由于 110 位于 `ap-singapore`，111 / 114 / 150 位于 `ap-seoul`，若坚持 110 做中心库，需要腾讯云跨地域云联网，可能产生额外费用和运维复杂度。

因此第一阶段改为 114 做试点中心库。114 与 111 / 150 同在 `ap-seoul` 的 `10.8.0.0/22` 内网，当前资源余量也相对最好。

第一阶段目标链路：

```text
111 10.8.0.11 -> 114 10.8.0.16:5432
114 127.0.0.1 -> 114 PostgreSQL
150 10.8.0.2  -> 114 10.8.0.16:5432
```

交易服务器环境变量：

```bash
GRID_PLATFORM_DATABASE_URL=postgresql://grid:***@10.8.0.16:5432/grid_platform
```

前提是从 111 / 150 执行以下检查通过：

```bash
ping -c 2 10.8.0.16
nc -vz -w 2 10.8.0.16 5432
```

## 数据库选择

114 试点中心库使用 PostgreSQL。

推荐版本：

```text
PostgreSQL 16
```

原因：

- 多服务器集中写入比 SQLite 更合适。
- 策略参数和原始 payload 需要 JSONB。
- 按 symbol、time、strategy_mode、category 做 SQL 分析方便。
- 后续可以接 Grafana / Metabase / 自研分析页面。
- 当前数据规模没必要一开始上 ClickHouse；等 K线和周期样本量巨大后，可以再同步到 OLAP。

## 服务器职责

### 114

职责：

- PostgreSQL 试点中心库。
- 只允许指定交易服务器内网 IP 访问数据库端口。
- 每日备份，至少保留 14 到 30 天。
- 运行轻量数据质量巡检任务。
- 可运行全量 backfill 或跨服务器汇总脚本。

建议环境变量：

```bash
GRID_PLATFORM_DATABASE_URL=postgresql://grid:***@127.0.0.1:5432/grid_platform
GRID_TRADE_DB_WORKSPACE=prod
GRID_SERVER_ID=server-114
```

注意：114 当前仍承担 runner/web 职责，PostgreSQL 第一阶段必须按轻量配置运行，不在 114 上跑重分析和大批量 K线回算。长期建议迁移到 Seoul 同 VPC 的独立 DB 机。

### 111 / 150

职责：

- 继续执行 runner。
- 本地保存 JSONL 和 state。
- 实时写入 114 PostgreSQL。
- 定时执行 backfill。
- 数据库写入失败时报警。

建议环境变量：

```bash
GRID_TRADE_DB_ENABLED=1
GRID_TRADE_DB_WORKSPACE=prod
GRID_TRADE_DB_ACCOUNT=binance-main
GRID_PLATFORM_DATABASE_URL=postgresql://grid:***@10.8.0.16:5432/grid_platform
GRID_SERVER_ID=server-111
```

每台服务器的 `GRID_SERVER_ID` 必须不同。

## 数据来源

### 1. 成交数据

来源：

- Futures user trades
- Spot user trades
- runner user data stream observed fills
- 本地 `*_trade_audit.jsonl`

要求：

- 按交易所 trade id / order id / time 幂等写入。
- 分类成交角色：
  - `grid`
  - `reduce`
  - `protection`
  - `other`
- 保留 `role`、`clientOrderId` 和原始 payload。
- 合约需要保存 realized pnl、fee、position side、reduce-only 语义。
- 现货需要保存基于库存成本计算出的 realized pnl 和手续费折算。

### 2. 资金流水

来源：

- Futures income history
- 本地 `*_income_audit.jsonl`

要求：

- 保存 funding fee、commission rebate、bonus 等收入类型。
- 和 run_key / symbol / market_type 关联。
- 按 tranId 或合成 key 幂等。

### 3. 策略周期快照

来源：

- 每轮 runner summary
- 本地 `*_loop_events.jsonl`
- 本地 `*_spot_loop_events.jsonl`

要求：

- 每轮保存一条样本。
- 保存完整参数快照。
- 保存市场特征。
- 保存仓位/库存/挂单状态。
- 保存风控状态、停买、减仓、保护触发。
- 保存结果指标，如成交额、净收益、万U损耗。
- 保存 raw summary。

这部分最关键，因为交易所无法补出策略当时的判断过程。

### 4. K线数据

来源：

- Binance futures klines
- Binance spot klines

建议采集周期：

- 1m 原始 K线长期保存。
- 5m / 15m / 1h 可由 1m 聚合生成，也可以缓存派生事实。

要求：

- 按 symbol + market_type + interval + open_time 幂等。
- 保存 open/high/low/close/volume/quote_volume/trade_count。
- 支持缺口检测和补拉。
- 与策略周期快照按时间对齐。

### 5. 策略配置版本

来源：

- runner 启动配置
- saved runner control JSON
- Web 保存配置
- 手工启动命令

要求：

- 每次启动都生成参数指纹。
- 每次修改配置都保存版本。
- 保存服务器 ID、启动时间、操作者/来源、原因备注。
- 策略周期快照必须能回指当时参数。

## 核心表设计

### `strategy_runs_audit`

用途：记录一次策略运行或一组持续运行参数。

关键字段：

- `run_key`
- `workspace`
- `account_alias`
- `server_id`
- `venue`
- `symbol`
- `market_type`
- `strategy_mode`
- `config_fingerprint`
- `config_json`
- `first_seen_at`
- `last_seen_at`

`run_key` 生成建议：

```text
workspace + account + server_id + symbol + market_type + strategy_mode + run_start_time + config_fingerprint
```

### `strategy_trade_fills_audit`

用途：保存成交事实。

关键字段：

- `run_key`
- `server_id`
- `symbol`
- `market_type`
- `strategy_mode`
- `venue_trade_id`
- `venue_order_id`
- `venue_client_order_id`
- `side`
- `position_side`
- `role`
- `category`
- `price`
- `qty`
- `quote_qty`
- `realized_pnl`
- `fee`
- `fee_asset`
- `is_maker`
- `filled_at`
- `raw_payload_json`

唯一约束：

```text
workspace + account_alias + venue + market_type + symbol + venue_trade_id
```

### `strategy_income_audit`

用途：保存资金流水。

关键字段：

- `run_key`
- `server_id`
- `symbol`
- `market_type`
- `income_id`
- `income_type`
- `asset`
- `income`
- `occurred_at`
- `raw_payload_json`

### `strategy_cycle_snapshots_audit`

用途：保存每轮策略样本，是后续参数推荐的核心事实表。

关键字段：

- `run_key`
- `server_id`
- `symbol`
- `market_type`
- `strategy_mode`
- `cycle`
- `observed_at`
- `config_fingerprint`
- `market_features_json`
- `params_json`
- `state_json`
- `outcome_json`
- `raw_summary_json`

`market_features_json` 建议字段：

```json
{
  "mid_price": 0,
  "bid_price": 0,
  "ask_price": 0,
  "mark_price": 0,
  "return_1m": 0,
  "amplitude_1m": 0,
  "return_5m": 0,
  "amplitude_5m": 0,
  "return_15m": 0,
  "amplitude_15m": 0,
  "effective_step_price": 0,
  "base_step_price": 0,
  "effective_step_ratio": 0,
  "step_scale": 0,
  "execution_regime": "normal",
  "risk_state": "normal",
  "direction_state": "long_active"
}
```

`state_json` 建议字段：

```json
{
  "position_qty": 0,
  "position_notional": 0,
  "long_notional": 0,
  "short_notional": 0,
  "open_strategy_orders": 0,
  "active_buy_orders": 0,
  "active_sell_orders": 0,
  "buy_paused": false,
  "shift_frozen": false,
  "stop_triggered": false,
  "stop_reason": null
}
```

`outcome_json` 建议字段：

```json
{
  "gross_notional": 0,
  "trade_count": 0,
  "realized_pnl": 0,
  "commission": 0,
  "funding_fee": 0,
  "net_pnl": 0,
  "loss_per_10k": 0,
  "maker_count": 0,
  "buy_notional": 0,
  "sell_notional": 0,
  "unrealized_pnl": 0,
  "inventory_notional": 0
}
```

### `market_klines`

用途：保存原始 K线。

关键字段：

- `symbol`
- `market_type`
- `interval`
- `open_time`
- `close_time`
- `open`
- `high`
- `low`
- `close`
- `volume`
- `quote_volume`
- `trade_count`
- `raw_payload_json`

唯一约束：

```text
symbol + market_type + interval + open_time
```

### `market_feature_facts`

用途：保存派生市场特征，供快速分析和推荐。

关键字段：

- `symbol`
- `market_type`
- `feature_time`
- `window`
- `return_ratio`
- `amplitude_ratio`
- `realized_volatility`
- `quote_volume`
- `volume_zscore`
- `trend_score`
- `shock_score`
- `raw_features_json`

## 采集链路

### 实时写库

runner 每轮执行后：

1. 本地写 JSONL。
2. 如果 `GRID_TRADE_DB_ENABLED=1`：
   - 确保 schema 存在。
   - 写入新成交。
   - 写入新资金流水。
   - 写入周期快照。
3. 写库失败：
   - 不抛出阻断交易。
   - 写入 summary 的 `trade_database.error` 或 `cycle_database.error`。
   - 触发报警。

### 定时 backfill

每台交易服务器建议每 5 到 15 分钟跑一次轻量 backfill：

```bash
python3 -m grid_optimizer.trade_database_backfill --output-dir output
```

职责：

- 扫描本地 trade audit。
- 扫描本地 income audit。
- 扫描本地 loop events。
- 幂等写入 114。

### K线采集

第一阶段可以先不启用 K线采集器，先保存 runner 周期快照和成交数据。需要 K线事实表时，建议优先放在未来 Seoul 独立 DB 机；如果短期必须在 114 上采集，只采集小范围币种并限制频率，避免影响交易 runner。

职责：

- 按监控币种列表拉 1m K线。
- 每轮从数据库最后 open_time 继续。
- 每小时跑一次缺口检测。
- 派生 5m/15m/1h 特征。

K线采集器失败不影响交易，但影响后续推荐分析，所以需要报警。

## 中断要求

策略运行过程不要求绝不中断，但不同数据的可补性不同：

### 可补数据

- 交易所成交。
- 资金流水。
- K线。
- 部分订单状态。

这些可以通过交易所 API 和本地 JSONL 补。

### 不完全可补数据

- runner 当轮参数。
- runner 当轮风险状态。
- 停买原因。
- 减仓保护触发原因。
- 当时生成但未提交的计划。
- 当时被 guard 丢弃的订单。

这些只有 runner 运行时 summary 能完整保存。

结论：

```text
交易结果可以补。
K线可以补。
策略判断过程最好不中断。
```

生产要求：

- runner 用 systemd/watchdog 常驻。
- 数据库写入失败不停止 runner。
- runner 崩溃必须自动拉起。
- 每台服务器必须保留本地 JSONL 至少 7 到 14 天。
- backfill 必须定时运行。
- 114 数据库必须每日备份；迁移到独立 DB 机后由独立 DB 机备份。

## 其他服务器改造

### 系统依赖

安装 PostgreSQL client / psycopg：

```bash
pip install "psycopg[binary]>=3.1,<4"
```

### 环境变量

每台服务器：

```bash
GRID_TRADE_DB_ENABLED=1
GRID_TRADE_DB_WORKSPACE=prod
GRID_TRADE_DB_ACCOUNT=binance-main
GRID_PLATFORM_DATABASE_URL=postgresql://grid:***@10.8.0.16:5432/grid_platform
GRID_SERVER_ID=server-114
```

111 / 150 启用前要求先完成到 `10.8.0.16:5432` 的连通性验证。114 本机可使用 `127.0.0.1:5432`。

### 网络

114 PostgreSQL 只开放给交易服务器内网 IP。目标链路：

```text
111 -> 114:5432
114 -> 127.0.0.1:5432
150 -> 114:5432
```

优先使用内网源地址：

```text
111 10.8.0.11 -> 114 10.8.0.16:5432
114 127.0.0.1 -> 114 127.0.0.1:5432
150 10.8.0.2  -> 114 10.8.0.16:5432
```

需同时配置：

- 114 安全组允许来源 `10.8.0.11/32`、`10.8.0.2/32` 访问 `5432`。
- 114 本机防火墙允许相同来源访问 `5432`。
- PostgreSQL `listen_addresses` 监听 `10.8.0.16` 和 `127.0.0.1`，或受防火墙保护的 `0.0.0.0`。
- `pg_hba.conf` 只允许 `127.0.0.1/32`、`10.8.0.11/32`、`10.8.0.2/32` 使用 `scram-sha-256` 连接。

不允许公网任意访问。

### Runner 改造要求

- 合约 runner 写入成交、资金、周期快照。
- 现货 runner 写入成交、周期快照。
- 所有写入幂等。
- 所有写入失败不阻断交易。
- summary 暴露写库状态。
- 保留本地 JSONL。

## 数据质量监控

114 每 5 到 15 分钟巡检：

- 最近 10 分钟是否有 cycle snapshot。
- 最近 10 分钟是否有 trade fill。
- 各服务器最后写入时间。
- 数据库写入错误次数。
- K线缺口。
- 本地 JSONL 和数据库行数差异。

建议告警：

- 某服务器 10 分钟没有 cycle snapshot。
- 某运行中 runner 有交易但数据库没有成交。
- K线缺口超过 3 根 1m。
- backfill 连续失败。
- PostgreSQL 磁盘使用超过 80%。
- 数据库备份失败。

## 分析口径

### 参数指纹

对策略参数 JSON 做稳定序列化和 hash，得到 `config_fingerprint`。

用途：

- 比较同一策略模式下不同参数效果。
- 聚合同一参数组在不同币种上的表现。
- 找出某个币种最适合的参数区间。

### 波动桶

建议初始波动桶：

```text
1m amplitude: 0-0.1%, 0.1-0.3%, 0.3-0.6%, >0.6%
5m amplitude: 0-0.3%, 0.3-0.8%, 0.8-1.5%, >1.5%
15m amplitude: 0-0.5%, 0.5-1.2%, 1.2-2.5%, >2.5%
direction: up / down / flat
volume: normal / high / extreme
```

### 结果指标

核心指标：

- `gross_notional`
- `net_pnl`
- `loss_per_10k`
- `realized_pnl`
- `commission`
- `funding_fee`
- `maker_ratio`
- `reduce_count`
- `protection_count`
- `stop_trigger_count`
- `inventory_peak_notional`
- `drawdown_proxy`

推荐排序：

```text
先按 loss_per_10k 升序
再按成交效率降序
再按保护触发次数升序
再按净收益降序
```

## 推荐服务思路

输入：

```json
{
  "symbol": "XAUTUSDT",
  "market_type": "futures",
  "recent_klines": "最近1m/5m/15m/1h特征",
  "risk_budget": {
    "max_loss_per_10k": 3,
    "max_position_notional": 500
  }
}
```

处理：

1. 计算当前市场特征。
2. 找历史上相似的波动桶。
3. 按策略模式和参数指纹聚合结果。
4. 过滤成交样本不足的参数。
5. 过滤超出风险预算的参数。
6. 输出候选模式和参数。

输出：

```json
{
  "symbol": "XAUTUSDT",
  "recommended": [
    {
      "strategy_mode": "xaut_volume_guarded_bard_v2",
      "config_fingerprint": "...",
      "params": {},
      "expected_loss_per_10k": 1.2,
      "sample_count": 430,
      "reason": "同类15m振幅下亏损最低且成交量稳定"
    }
  ]
}
```

第一版推荐不需要机器学习，用 SQL 分桶统计即可。

## 分阶段计划

### Phase 1: 114 试点中心库 MVP

- 在 114 安装 PostgreSQL。
- 建库、建用户、限制访问 IP。
- 初始化审计表。
- 合约 runner 写入成交、资金、周期快照。
- 现货 runner 写入成交、周期快照。
- backfill 回灌现有 JSONL。

验收：

- 114 能看到本机试点 runner 写入。
- 写库失败不影响 runner。
- 单币可以查询成交、减仓、保护和周期样本。

### Phase 2: 全服务器接入

- 111 / 114 / 150 配置数据库环境变量。
- systemd 加 `GRID_SERVER_ID`。
- 每台服务器定时 backfill。
- 114 建数据质量巡检。

验收：

- 每台服务器都有最近 cycle snapshot。
- 每台服务器 backfill 可重复执行且幂等。
- 断开数据库后 runner 正常交易，恢复后可补数据。

### Phase 3: K线中心采集

- 在 114 上小范围试点 K线采集，或等待迁移到 Seoul 独立 DB 机后启用。
- 保存 1m K线。
- 派生 5m/15m/1h 特征。
- 补缺口。

验收：

- 指定币种 1m K线连续。
- 策略周期样本可关联最近 K线特征。
- 缺口巡检可发现并补拉。

### Phase 4: 分析报表

- 按币种、模式、参数、波动桶统计。
- 展示成交额、净收益、万U损耗、减仓和保护次数。
- 支持查看不同模式参数在不同币种上的实际效果。

验收：

- 能回答“XAUT 在高振幅下哪组参数亏损最低”。
- 能回答“BARD 哪些参数成交效率高但保护触发少”。
- 能比较同一模式跨币种效果。

### Phase 5: 参数推荐

- 输入币种和最近 K线。
- 匹配历史相似环境。
- 输出候选模式和参数。
- 给出样本数量、预期损耗、风险说明。

验收：

- 推荐结果可追溯到历史样本。
- 推荐理由包含具体统计指标。
- 不满足样本量时明确提示“不足以推荐”。

## 验收 SQL 示例

按策略模式统计：

```sql
SELECT
  symbol,
  market_type,
  strategy_mode,
  count(*) AS fills,
  sum(quote_qty) AS volume,
  sum(realized_pnl - fee) AS net_pnl
FROM strategy_trade_fills_audit
GROUP BY 1,2,3
ORDER BY volume DESC;
```

按成交分类统计：

```sql
SELECT
  symbol,
  category,
  count(*) AS fills,
  sum(quote_qty) AS volume,
  sum(realized_pnl - fee) AS net_pnl
FROM strategy_trade_fills_audit
GROUP BY 1,2
ORDER BY symbol, category;
```

按波动状态和参数表现统计：

```sql
SELECT
  symbol,
  strategy_mode,
  config_fingerprint,
  market_features_json->>'risk_state' AS risk_state,
  round(avg((market_features_json->>'amplitude_1m')::numeric), 6) AS avg_amp_1m,
  sum((outcome_json->>'gross_notional')::numeric) AS volume,
  sum((outcome_json->>'net_pnl')::numeric) AS net_pnl,
  round(avg((outcome_json->>'loss_per_10k')::numeric), 4) AS avg_loss_per_10k,
  count(*) AS sample_count
FROM strategy_cycle_snapshots_audit
GROUP BY 1,2,3,4
HAVING count(*) >= 20
ORDER BY avg_loss_per_10k ASC, volume DESC;
```

## 风险

### 数据库不可用

影响：

- 实时落盘失败。
- 交易不受影响。

缓解：

- 本地 JSONL 继续写。
- summary 写错误。
- backfill 补数据。
- 告警。

### 策略运行中断

影响：

- 成交和 K线可补。
- 策略决策上下文不可完整补。

缓解：

- systemd/watchdog。
- runner health check。
- 114 数据质量巡检。

### 数据口径混乱

影响：

- 推荐结果不可信。

缓解：

- 保存 raw payload。
- 角色分类规则版本化。
- 统计口径在 SQL 和代码中固化。
- 每次调整分类规则可重新回算。

### 多服务器重复写入

影响：

- 可能重复成交或周期样本。

缓解：

- 成交按交易所 trade id 幂等。
- 周期样本按 run_key + cycle + observed_at 幂等。
- 每台服务器设置唯一 `GRID_SERVER_ID`。

## 开放问题

- 何时从 114 试点库迁移到 Seoul 同 VPC 独立 DB 机。
- PostgreSQL 是否由 systemd 本机部署，还是使用托管/容器。
- JSONL 本地保留周期定为 7 天、14 天还是 30 天。
- 是否需要把手工交易也纳入同一策略数据库。
- K线采集币种列表从 `symbol_lists` 读取，还是从数据库运行中 symbol 自动发现。
- 参数推荐第一版是否只做 futures，还是 spot/futures 同时做。

## 推荐决策

推荐先按以下路径推进：

1. 114 使用 PostgreSQL 16 做试点中心库。
2. 先接入 114 本机低风险 runner 做试点。
3. 试点稳定后接 111 / 150。
4. 每台服务器保留本地 JSONL，114 做阶段性中心汇总。
5. 先用 SQL 分桶统计做推荐，不急于上模型。
6. 等样本量和分析任务变重后，迁移到 Seoul 独立 DB 机，再考虑机器学习或 Bayesian 参数搜索。
