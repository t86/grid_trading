# Central Strategy Data Platform Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a 114-centered PostgreSQL pilot strategy experiment database so every runner on 111 / 114 / 150 can persist trades, income, cycle snapshots, market/K-line features, and parameter versions for later strategy analysis and parameter recommendation.

**Architecture:** Keep existing runners and local JSONL/state files. Add best-effort PostgreSQL writes behind a feature flag, keep JSONL as the recovery buffer, add idempotent backfill, then build lightweight analysis queries on 114. When data volume or analysis load grows, migrate the database to a dedicated Seoul same-VPC DB server.

**Tech Stack:** Python, PostgreSQL 16, `psycopg`, existing `grid_optimizer.loop_runner`, `spot_loop_runner`, JSONL audit files, systemd/watchdog, SQL analytics.

---

### Task 1: 114 PostgreSQL Pilot Foundation

**Files:**
- Create/modify deployment docs and server env files.
- Use spec: `docs/superpowers/specs/2026-05-17-central-strategy-data-platform-design.md`

- [ ] Confirm 114 remains the pilot DB host and future migration target is a dedicated Seoul same-VPC DB server.
- [x] Verify from 111: `ping -c 2 10.8.0.16` passes; `nc -vz -w 2 10.8.0.16 5432` is still blocked pending Tencent Cloud security group.
- [ ] Verify from 150: `ping -c 2 10.8.0.16` and `nc -vz -w 2 10.8.0.16 5432`; currently both are blocked or filtered.
- [x] Install PostgreSQL 16 on 114.
- [x] Create `grid_platform` database and `grid` user.
- [ ] Restrict PostgreSQL access to `127.0.0.1`, 111 private IP `10.8.0.11`, and 150 private IP `10.8.0.2` in Tencent Cloud security group, host firewall, and `pg_hba.conf`.
- [x] Configure conservative PostgreSQL memory settings for 114, because it still runs web/runner workloads.
- [ ] Add daily backup job and retention policy.
- [x] Verify `psql` connection locally on 114.
- [ ] Verify remote connection from one runner server.

### Task 2: Audit Schema And Python Writer

**Files:**
- Create/modify: `src/grid_optimizer/trade_database.py`
- Test: `tests/test_trade_database.py`

- [ ] Add schema creation for `strategy_runs_audit`.
- [ ] Add schema creation for `strategy_trade_fills_audit`.
- [ ] Add schema creation for `strategy_income_audit`.
- [ ] Add schema creation for `strategy_cycle_snapshots_audit`.
- [ ] Add stable run key and config fingerprint generation.
- [ ] Add idempotent trade fill insert.
- [ ] Add idempotent income insert.
- [ ] Add idempotent cycle snapshot insert.
- [ ] Ensure `GRID_TRADE_DB_ENABLED` gates all writes.
- [ ] Ensure write failure returns an error payload and does not raise into trading flow.
- [ ] Add unit tests with mocked `psycopg`.

Verification:

```bash
PYTHONPATH=src python3 -m pytest tests/test_trade_database.py -q
python3 -m py_compile src/grid_optimizer/trade_database.py
```

### Task 3: Futures Runner Wiring

**Files:**
- Modify: `src/grid_optimizer/loop_runner.py`
- Test: focused runner/audit tests.

- [ ] After account audit sync, persist fresh trade rows to PostgreSQL.
- [ ] After account audit sync, persist fresh income rows to PostgreSQL.
- [ ] At the end of each successful cycle, persist cycle snapshot.
- [ ] Include `trade_database` status in summary.
- [ ] Include `cycle_database` status in summary.
- [ ] Confirm database write failures do not stop the runner.
- [ ] Keep local JSONL writes unchanged.

Verification:

```bash
PYTHONPATH=src python3 -m pytest tests/test_trade_database.py tests/test_loop_runner_execution_events.py -q
python3 -m py_compile src/grid_optimizer/loop_runner.py
```

### Task 4: Spot Runner Wiring

**Files:**
- Modify: `src/grid_optimizer/spot_loop_runner.py`
- Test: `tests/test_spot_runner.py`, `tests/test_trade_database.py`

- [ ] Convert recent spot metrics trades into audit-shaped fill rows.
- [ ] Persist spot trade rows to PostgreSQL.
- [ ] Persist spot cycle snapshots to PostgreSQL.
- [ ] Include write status in summary.
- [ ] Confirm runtime guard early-stop path also saves cycle snapshot.
- [ ] Confirm write failure does not stop runner.

Verification:

```bash
PYTHONPATH=src python3 -m pytest tests/test_spot_runner.py tests/test_trade_database.py -q
python3 -m py_compile src/grid_optimizer/spot_loop_runner.py
```

### Task 5: JSONL Backfill

**Files:**
- Create/modify: `src/grid_optimizer/trade_database_backfill.py`
- Add command in `pyproject.toml`.
- Docs: `docs/TRADE_DATABASE_114_RUNBOOK.md`

- [ ] Scan `output/*_loop_events.jsonl`.
- [ ] Resolve matching trade/income audit paths.
- [ ] Resolve saved runner config/control JSON for params.
- [ ] Backfill trade rows.
- [ ] Backfill income rows.
- [ ] Backfill cycle snapshots.
- [ ] Make reruns idempotent.
- [ ] Add CLI `grid-trade-db-backfill`.

Verification:

```bash
PYTHONPATH=src python3 -m grid_optimizer.trade_database_backfill --output-dir output --symbols XAUTUSDT
```

### Task 6: K-line Collector

**Files:**
- Create: `src/grid_optimizer/market_data_collector.py`
- Test: `tests/test_market_data_collector.py`

- [ ] Add `market_klines` schema.
- [ ] Add `market_feature_facts` schema.
- [ ] Fetch 1m futures klines for configured symbols.
- [ ] Fetch 1m spot klines for configured symbols.
- [ ] Insert klines idempotently by symbol / market type / interval / open time.
- [ ] Compute 5m / 15m / 1h return, amplitude, quote volume, volatility, trend score, shock score.
- [ ] Detect gaps and backfill missing 1m candles.
- [ ] Defer 114 K-line collector until the DB write pilot is stable, or run only a tiny symbol set with strict resource limits.

Verification:

```sql
SELECT symbol, market_type, count(*), min(open_time), max(open_time)
FROM market_klines
GROUP BY 1,2;
```

### Task 7: Server Rollout

**Files:**
- Deploy env/systemd configs on 111 / 114 / 150.
- Docs: `docs/TRADE_DATABASE_114_RUNBOOK.md`

- [ ] Enable DB env vars on 114 first.
- [ ] Restart one low-risk runner on 114.
- [ ] Verify `strategy_trade_fills_audit` receives rows.
- [ ] Verify `strategy_cycle_snapshots_audit` receives rows.
- [ ] Run backfill on 114.
- [ ] Repeat for 111.
- [ ] Repeat for 150.
- [ ] Add cron/systemd timer for periodic backfill on each server.

Acceptance:

- Every active runner has a recent cycle snapshot.
- Every server can be identified by `server_id`.
- Database outage simulation does not stop runner.
- Backfill catches up after database restoration.

### Task 8: Data Quality Monitor

**Files:**
- Create: `src/grid_optimizer/trade_database_monitor.py`
- Optional web/API integration later.

- [ ] Check latest cycle snapshot per server/symbol.
- [ ] Check latest fill per server/symbol.
- [ ] Check database write errors in recent summaries.
- [ ] Check Kline gaps.
- [ ] Check backup freshness.
- [ ] Send alert when thresholds fail.

Acceptance:

- Missing cycle snapshots are detected within 10 minutes.
- Kline gaps over 3 minutes are detected.
- Backfill failures are reported.

### Task 9: Analysis Queries And First Reports

**Files:**
- Create: `scripts/strategy_data_report.py`
- Docs: add SQL snippets to runbook.

- [ ] Report by symbol + strategy mode + config fingerprint.
- [ ] Report by symbol + trade category.
- [ ] Report by volatility bucket + strategy mode.
- [ ] Report best parameter groups by `loss_per_10k`.
- [ ] Report sample count and reject low-sample candidates.
- [ ] Produce CSV/JSON outputs for later Web charts.

Acceptance:

- Can answer: "XAUT high-amplitude windows: which params had lowest loss_per_10k?"
- Can answer: "BARD by mode: which mode had high volume and low protection count?"

### Task 10: Parameter Recommendation MVP

**Files:**
- Create: `src/grid_optimizer/strategy_parameter_recommender.py`
- Test: `tests/test_strategy_parameter_recommender.py`

- [ ] Define input payload: symbol, market type, current Kline features, risk budget.
- [ ] Match historical cycle samples by volatility bucket.
- [ ] Aggregate candidates by strategy mode + config fingerprint.
- [ ] Filter by minimum sample count.
- [ ] Filter by max loss budget.
- [ ] Rank by loss_per_10k, volume efficiency, protection count, net pnl.
- [ ] Return recommended modes/params with reasons.

Acceptance:

- Recommendation output references historical sample counts.
- Recommendation output includes expected loss metrics.
- If samples are insufficient, output `insufficient_data` rather than guessing.

## Rollout Order

1. 114 PostgreSQL and schema.
2. Writer and tests.
3. 114 single-runner trial.
4. Backfill on 114.
5. 111 / 150 rollout.
6. Lightweight reports.
7. Decide whether to migrate to a dedicated Seoul DB host.
8. Kline collector.
9. Recommender MVP.

## Stop Conditions

- Database writes block or slow trading cycles materially.
- Duplicate rows appear despite unique constraints.
- Backfill cannot be rerun idempotently.
- Data quality monitor reports missing cycle snapshots for active runners.
- Recommendation cannot show sample counts and traceable historical evidence.
