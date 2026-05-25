# 114 试点交易数据库落盘 Runbook

目标：第一阶段在 114 服务器保存 111 / 114 / 150 的策略成交、资金流水、每轮策略参数快照、市场波动特征和运行结果，避免 `output/*.jsonl` 轮转或清理后无法完整复盘。后续数据量和分析任务变重后，再迁移到 Seoul 同 VPC 的独立数据库服务器。

## 0. 网络前置检查

数据库流量优先走腾讯云内网/VPC。不要默认所有腾讯云机器天然互通，必须先验证路由和安全组。

2026-05-17 实测：

| 服务器 | 公网 IP | 腾讯云 local-ipv4 |
| --- | --- | --- |
| 110 | 43.156.35.110 | 10.3.0.14 |
| 111 | 43.155.136.111 | 10.8.0.11 |
| 114 | 43.155.163.114 | 10.8.0.16 |
| 150 | 43.131.232.150 | 10.8.0.2 |

当前 111 / 114 / 150 到 110 的 `10.3.0.14` 不通，`10.3.0.14:5432` 连接超时。110 位于 `ap-singapore`，111 / 114 / 150 位于 `ap-seoul`，跨地域内网需要云联网并可能产生额外费用。因此第一阶段不使用 110 做中心库，改为 114 试点中心库。

从 111 / 150 验证到 114：

```bash
ping -c 2 10.8.0.16
nc -vz -w 2 10.8.0.16 5432
```

验证通过后，111 / 150 使用：

```bash
export GRID_PLATFORM_DATABASE_URL='postgresql://grid:替换成强密码@10.8.0.16:5432/grid_platform'
```

114 本机使用：

```bash
export GRID_PLATFORM_DATABASE_URL='postgresql://grid:替换成强密码@127.0.0.1:5432/grid_platform'
```

不允许公网任意访问 PostgreSQL。

2026-05-17 当前部署状态：

- 114 已安装 PostgreSQL 16。
- 114 已创建 `grid_platform` 数据库和 `grid` 用户。
- PostgreSQL 已监听 `127.0.0.1` 和 `10.8.0.16`。
- 保守参数已生效：`max_connections=30`、`shared_buffers=128MB`、`work_mem=4MB`。
- 114 本机 `grid` 用户连接 `grid_platform` 已验证通过。
- 111 / 150 到 `10.8.0.16:5432` 仍超时；114 的 UFW 未启用，剩余阻塞大概率是腾讯云安全组未放行 TCP `5432`。

腾讯云安全组待配置：

- 114 入站允许 TCP `5432`，来源 `10.8.0.11/32`。
- 114 入站允许 TCP `5432`，来源 `10.8.0.2/32`。
- 不允许 `0.0.0.0/0` 访问 TCP `5432`。

## 1. PostgreSQL

建议单独建库：

```bash
sudo -u postgres psql
CREATE USER grid WITH PASSWORD '替换成强密码';
CREATE DATABASE grid_platform OWNER grid;
\q
```

数据库 URL：

```bash
export GRID_PLATFORM_DATABASE_URL='postgresql://grid:替换成强密码@127.0.0.1:5432/grid_platform'
```

允许远端交易服务器写入时，PostgreSQL 还需要配置：

- `listen_addresses = '127.0.0.1,10.8.0.16'`，或在安全组和防火墙严格限制时使用 `listen_addresses = '*'`。
- `pg_hba.conf` 只允许 `127.0.0.1/32`、`10.8.0.11/32`、`10.8.0.2/32` 使用 `scram-sha-256`。
- 腾讯云安全组只允许 `10.8.0.11/32`、`10.8.0.2/32` 访问 114 的 `5432`。
- 114 本机防火墙做同样限制。

114 当前本机 UFW 为 inactive。若后续启用 UFW，必须先保留 SSH/Web 现有访问规则，再加：

```bash
sudo ufw allow from 10.8.0.11 to any port 5432 proto tcp
sudo ufw allow from 10.8.0.2 to any port 5432 proto tcp
```

## 2. Runner 环境变量

在 114 本机 runner systemd/env 文件里加：

```bash
export GRID_TRADE_DB_ENABLED=1
export GRID_TRADE_DB_WORKSPACE=prod
export GRID_TRADE_DB_ACCOUNT=binance-main
export GRID_PLATFORM_DATABASE_URL='postgresql://grid:替换成强密码@127.0.0.1:5432/grid_platform'
export GRID_SERVER_ID='server-114'
```

在 111 / 150 的 runner systemd/env 文件里加：

```bash
export GRID_TRADE_DB_ENABLED=1
export GRID_TRADE_DB_WORKSPACE=prod
export GRID_TRADE_DB_ACCOUNT=binance-main
export GRID_PLATFORM_DATABASE_URL='postgresql://grid:替换成强密码@10.8.0.16:5432/grid_platform'
export GRID_SERVER_ID='server-111'
```

每台机器的 `GRID_SERVER_ID` 必须不同，例如 `server-111`、`server-114`、`server-150`。

写库失败不会阻断交易循环，状态会出现在每轮 summary 的 `trade_database` / `cycle_database` 字段里。

## 3. 首次回灌现有 JSONL

在 `/home/ubuntu/wangge` 或实际部署目录执行：

```bash
python3 -m grid_optimizer.trade_database_backfill --output-dir output
```

只导入部分币种：

```bash
python3 -m grid_optimizer.trade_database_backfill --output-dir output --symbols XAUTUSDT,BARDUSDT
```

## 4. 验证

```sql
SELECT symbol, market_type, strategy_mode, count(*) AS fills, sum(quote_qty) AS volume, sum(realized_pnl - fee) AS net_pnl
FROM strategy_trade_fills_audit
GROUP BY 1,2,3
ORDER BY volume DESC;

SELECT symbol, category, count(*) AS fills, sum(quote_qty) AS volume, sum(realized_pnl - fee) AS net_pnl
FROM strategy_trade_fills_audit
GROUP BY 1,2
ORDER BY symbol, category;

SELECT
  symbol,
  strategy_mode,
  market_features_json->>'risk_state' AS risk_state,
  round(avg((market_features_json->>'amplitude_1m')::numeric), 6) AS avg_amp_1m,
  round(avg((market_features_json->>'effective_step_ratio')::numeric), 6) AS avg_step_ratio,
  sum((outcome_json->>'gross_notional')::numeric) AS volume,
  sum((outcome_json->>'net_pnl')::numeric) AS net_pnl,
  round(avg((outcome_json->>'loss_per_10k')::numeric), 4) AS avg_loss_per_10k
FROM strategy_cycle_snapshots_audit
GROUP BY 1,2,3
ORDER BY avg_loss_per_10k ASC, volume DESC;
```

分类口径：

- `grid`: 网格开仓、止盈、bootstrap、inventory build 等常规网格成交。
- `reduce`: forced reduce、delever、adverse reduce、fast/taker exit、recycle 等减仓成交。
- `protection`: 角色名包含 guard/protect/stop 等保护语义但未归入 reduce 的成交。
- `other`: 当前无法从 role/clientOrderId 明确识别的成交，会保留原始 payload，后续可补规则。

## 5. 后续分析口径

`strategy_cycle_snapshots_audit` 是后续“给一个币种 + K线，推荐模式和参数”的基础样本表。

核心字段：

- `params_json`: 当前 runner 参数快照。
- `market_features_json`: 当轮中价、1m 涨跌/振幅、有效 step、step ratio、风险状态。
- `state_json`: 当轮仓位/库存、挂单数量、停买/停机状态。
- `outcome_json`: 成交额、成交数、已实现、手续费、净收益、万U损耗。
- `raw_summary_json`: 原始 summary，后续补新指标时可以重新提取。

推荐分析分组：

- 币种 + 市场类型 + 策略模式 + 参数指纹。
- 波动桶：1m/5m/15m 振幅、方向涨跌、成交量突增。
- 结果桶：万U损耗、净收益、成交效率、减仓次数、保护触发次数。
