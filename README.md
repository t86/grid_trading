# Arithmetic Long-Grid Optimizer (Binance Perpetuals)

This tool optimizes arithmetic long-grid count (`N`) on Binance perpetual klines.

Strategy assumptions (V1):
- Arithmetic grid between `min_price` and `max_price`.
- Long-only grid:
  - In each candle, trigger by intrabar path (`open-low-high-close` for bullish candles, `open-high-low-close` for bearish candles).
  - Crossing down a grid line: open long for that grid.
  - Crossing up to next line: close that grid long.
- Per-side fee defaults to `0.0002` (万二), applied to each fill.
- `total_buy_notional` is the total notional fully deployed if price reaches `min_price`.
- Optional: include historical funding rates (`fundingRate`) into net PnL for higher fidelity.
- Allocation modes (complete list in current system):
  - `equal`: equal notional per grid.
  - `equal_qty`: equal base quantity per grid (closer to exchange quantity-grid behavior).
  - `linear`: lower price grids get larger notional (linearly).
  - `linear_reverse`: higher price grids get larger notional (linearly).
  - `quadratic`: lower price grids get larger notional (quadratic).
  - `quadratic_reverse`: higher price grids get larger notional (quadratic).
  - `geometric`: lower price grids get larger notional (geometric ratio).
  - `geometric_reverse`: higher price grids get larger notional (geometric ratio).
  - `center_heavy`: middle grids get larger notional.
  - `edge_heavy`: edge grids get larger notional.

## Install / Run

```bash
cd /Volumes/WORK/binance/wangge
python3 -m pip install -e .
```

If your Python is externally managed (PEP 668), you can run with source path directly:

```bash
PYTHONPATH=src python3 -m grid_optimizer.cli --help
```

Example:

```bash
grid-opt \
  --symbol BTCUSDT \
  --min-price 70000 \
  --max-price 120000 \
  --total-buy-notional 100000 \
  --start-time 2025-01-01T00:00:00Z \
  --end-time 2026-01-01T00:00:00Z \
  --interval 1h \
  --n-min 5 \
  --n-max 200 \
  --fee-rate 0.0002 \
  --include-funding \
  --allocation-modes equal,linear \
  --objective net_profit \
  --min-trade-count 60 \
  --min-avg-capital-usage 0.05 \
  --top-k 5 \
  --report-json output/btc_report.json \
  --plan-csv output/btc_plan.csv
```

Competition backtest example:

```bash
cd /Volumes/WORK/binance/wangge
PYTHONPATH=src python3 -m grid_optimizer.competition_report \
  --symbols ENSOUSDT,OPNUSDT,ROBOUSDT \
  --window-days 3 \
  --refresh \
  --report-json output/competition_3d_report.json
```

Notes:
- Competition templates currently support `ENSOUSDT`, `OPNUSDT`, `ROBOUSDT`.
- Competition backtests default to `1m` candles.
- `conservative` template uses current close `±3%` with `n=20`.
- `aggressive` template uses current close `±4%` with `n=25`.
- Both templates use `arithmetic + linear_reverse + long + funding included`.

Objective options:
- `calmar` (default): risk-adjusted, may prefer fewer trades and lower drawdown
- `net_profit`: maximize absolute net profit
- `total_return`: maximize total return
- `annualized_return`: maximize annualized return
- `gross_trade_notional`: maximize cumulative filled notional, useful for volume-focused competitions
- `competition_volume`: prioritize target traded notional first, then smaller loss / higher profit

Neutral-grid note:
- `--strategy-direction neutral` defaults the anchor to the first candle open.
- Use `--neutral-anchor-price <price>` when you want the long/short split centered around a specific level, such as the current price.

Activity filters:
- `--min-trade-count`: require minimum number of fills
- `--min-avg-capital-usage`: require minimum average capital usage ratio (0-1)

You can also run module mode:

```bash
PYTHONPATH=src python3 -m grid_optimizer.cli --symbol ETHUSDT --min-price 3000 --max-price 6500 --total-buy-notional 50000
```

## Web UI

Start the local web UI:

```bash
cd /Volumes/WORK/binance/wangge
PYTHONPATH=src python3 -m grid_optimizer.web --host 127.0.0.1 --port 8787
```

Then open:

```text
http://127.0.0.1:8787
```

You can input parameters in browser and run optimization directly.

Competition guide:
- [TRADE_COMPETITION_GUIDE.md](TRADE_COMPETITION_GUIDE.md): Chinese guide for competition-oriented settings, smart range suggestions, and result interpretation.

## Dry Run Executor

Use the paper executor to monitor a live futures symbol and simulate a maker-only grid without placing any real orders:

```bash
cd /Volumes/WORK/binance/wangge
PYTHONPATH=src grid-dry-run \
  --symbol ENSOUSDT \
  --min-price 1.1148 \
  --max-price 1.1838 \
  --n 20 \
  --total-buy-notional 10000 \
  --grid-level-mode arithmetic \
  --allocation-mode linear_reverse \
  --strategy-direction long \
  --iterations 1 \
  --state-path output/enso_dry_run_state.json \
  --snapshot-path output/enso_dry_run_snapshot.json
```

Notes:
- This command reads only public market data and never sends orders to Binance.
- State is persisted to the JSON file, so repeated runs continue the same paper session unless you pass `--reset-state`.
- Snapshot output includes estimated next funding impact using the current `premiumIndex` funding rate. It is an estimate, not a booked funding settlement.

## Live Launch Check

Use the live check CLI before any real deployment. It reads your Binance API credentials from local environment variables, verifies account state, builds the first batch of maker entry orders, and validates them with Binance `order/test`. It does not place real orders.

```bash
cd /Volumes/WORK/binance/wangge
read -s "BINANCE_API_KEY?BINANCE_API_KEY: "; export BINANCE_API_KEY; echo
read -s "BINANCE_API_SECRET?BINANCE_API_SECRET: "; export BINANCE_API_SECRET; echo

PYTHONPATH=src grid-live-check
```

Competition profile defaults:
- `symbol=ENSOUSDT`
- `min_price=1.1148`
- `max_price=1.1838`
- `n=10`
- `total_buy_notional=100`
- `allocation_mode=linear_reverse`
- `strategy_direction=long`
- `leverage=2`

Notes:
- The CLI currently targets Binance `USDⓈ-M Futures`.
- It requires a clean starting state for the symbol: no open position and no open orders.
- It also requires one-way position mode; hedge mode is rejected.
- Output is saved to `output/enso_live_competition_check.json` by default.

## Semi Auto Plan

Use the semi-auto planner to generate the current maker-only order plan without placing any real orders. When API credentials are available, it also compares the desired ladder with your existing open orders and reports what to add or cancel.

```bash
cd /Volumes/WORK/binance/wangge
source /tmp/binance_api_env.sh

PYTHONPATH=src grid-semi-auto-plan \
  --symbol NIGHTUSDT \
  --step-price 0.00002 \
  --buy-levels 12 \
  --sell-levels 12 \
  --per-order-notional 25.25 \
  --base-position-notional 151.5 \
  --report-json output/night_semi_auto_plan.json
```

Notes:
- This command never sends a real order to Binance.
- It persists the current ladder center in `output/night_semi_auto_state.json` by default.
- Re-run it before manual order updates; the report will show `Orders to add` and `Orders to cancel`.

## Submit Plan

Use the submitter to turn a freshly generated semi-auto plan into real `LIMIT + GTX` Binance futures orders. It is guarded by default:

- It only accepts a recent plan file.
- It re-checks the live mid-price drift before placing orders.
- It only places orders when you pass `--apply`.

```bash
cd /Volumes/WORK/binance/wangge
source /tmp/binance_api_env.sh

PYTHONPATH=src grid-semi-auto-plan \
  --symbol NIGHTUSDT \
  --step-price 0.00002 \
  --buy-levels 12 \
  --sell-levels 12 \
  --per-order-notional 25.25 \
  --base-position-notional 151.5 \
  --report-json output/night_semi_auto_plan.json

PYTHONPATH=src grid-submit-plan \
  --plan-json output/night_semi_auto_plan.json \
  --allow-symbol NIGHTUSDT \
  --margin-type KEEP \
  --leverage 2 \
  --max-plan-age-seconds 60 \
  --max-mid-drift-steps 4 \
  --max-new-orders 24 \
  --max-total-notional 500 \
  --apply \
  --report-json output/night_submit_report.json
```

Notes:
- Omit `--apply` to preview only.
- `--margin-type KEEP` leaves the symbol's existing margin mode unchanged; use this in Multi-Assets environments.
- The submitter refuses to place orders if the symbol, plan age, mid-price drift, position mode, current position, or open-order count no longer match the plan.
- Sell ladder orders are sent as `reduceOnly` when they are generated as `take_profit` orders.

## Loop Runner

Use the loop runner to keep refreshing the plan and submitting only the delta. It is intended for a single symbol and a single strategy writer process.

```bash
cd /Volumes/WORK/binance/wangge
source /tmp/binance_api_env.sh

PYTHONPATH=src grid-loop-runner \
  --symbol NIGHTUSDT \
  --step-price 0.00002 \
  --buy-levels 8 \
  --sell-levels 8 \
  --per-order-notional 12.6 \
  --base-position-notional 75.6 \
  --pause-buy-position-notional 180 \
  --max-position-notional 220 \
  --margin-type KEEP \
  --leverage 2 \
  --max-plan-age-seconds 30 \
  --max-mid-drift-steps 4 \
  --maker-retries 2 \
  --max-new-orders 16 \
  --max-total-notional 220 \
  --sleep-seconds 20 \
  --apply \
  --state-path output/night_small_semi_auto_state.json \
  --plan-json output/night_loop_latest_plan.json \
  --submit-report-json output/night_loop_latest_submit.json \
  --summary-jsonl output/night_loop_events.jsonl
```

Notes:
- When `current_long_notional` reaches `--pause-buy-position-notional`, the runner stops placing new buy orders and only maintains sell-side de-risking orders.
- `--max-position-notional` is a hard cap on `current_long_notional + desired buy-side orders`. The runner trims bootstrap and entry buys to stay within that budget.
- You can enable inventory tiering before the hard pause by setting `--inventory-tier-start-notional` and the tier target fields. This progressively reduces buy-side intensity and expands sell-side coverage as inventory grows.
- `--cancel-stale` is enabled by default, so old ladder orders are removed when the center shifts.
- The latest cycle plan and submission result are kept as JSON files, and a per-cycle summary is appended to `output/night_loop_events.jsonl`.
- Full append-only audit logs are also written next to the summary file:
  - `output/night_loop_plan_audit.jsonl`
  - `output/night_loop_submit_audit.jsonl`
  - `output/night_loop_order_audit.jsonl`
  - `output/night_loop_trade_audit.jsonl`
  - `output/night_loop_income_audit.jsonl`
- `output/night_loop_audit_state.json` stores the trade/income sync cursors so restarts do not duplicate audit rows.

Web UI notes:
- Symbol dropdown supports Binance listed perpetual contracts (`/api/symbols`), with local fallback if API is unavailable.
- Backtest supports explicit `start_time` and `end_time` (local timezone input in browser).
- `最大买入金额` is used as max total notional deployment (same semantics as original `total_buy_notional`).
- Funding can be toggled on/off in UI, and funding history is cached locally.
- 新增现货 / 永续价差监控页：`http://127.0.0.1:8787/basis`
  - 自动聚合所有同时存在现货与永续合约的币种
  - 展示现货买一/卖一、合约买一/卖一、中位价差、两种可执行套利价差、当前资金费率、上一期资金费率、上一期结算时刻的现货/合约价差
  - 支持按预警阈值、最小可执行价差、套利方向、资金费方向、资金费协同、币种关键词筛选
  - 支持表头排序与自动刷新
  - 当策略为 `借币卖现货 / 买合约` 时，可补充 Binance 借币来源信息（默认安全模式）
  - 默认 `safe` 安全模式：只显示 Cross Margin 理论可借状态与参考利率，不查询账户额度、库存、逐仓支持与 VIP Loan；只需配置 `BINANCE_API_KEY`
  - 若需要显示最大可借额度、库存、逐仓支持、VIP Loan 等全量信息，再额外配置 `BINANCE_API_SECRET`，并设置 `BINANCE_BORROW_LOOKUP_MODE=full`
- 新增实盘网格监控页：`http://127.0.0.1:8787/monitor`
  - 读取 loop runner 本地事件日志、最新计划、最新提交结果
  - 读取 Binance 真实账户的当前持仓、未成交委托、用户成交、资金费
  - 展示会话成交笔数、累计成交额、净收益估算、当前挂单、最近成交和最近循环摘要
  - 默认按 `output/night_loop_events.jsonl`、`output/night_loop_latest_plan.json`、`output/night_loop_latest_submit.json` 读取
  - 若要公网暴露，建议至少配置 `GRID_WEB_USERNAME` 和 `GRID_WEB_PASSWORD` 开启 Basic Auth
  - 可选配置 `GRID_WEB_ALLOWED_CIDRS=1.2.3.4/32,5.6.7.0/24` 做来源 IP 白名单；未配置时默认允许任意来源
  - `/api/health` 默认不要求认证，便于 systemd / LB 健康检查
- `当前方案` 下新增资金费明细表（每个资金费节点的费率、持仓名义、本次与累计资金费），支持 CSV 导出。
- 新增全市场排行模块：
  - 指定时间段内按“年化波动率”排序合约列表
  - 指定时间段内按“总资金费率”排序合约列表
  - 支持每 60 秒及以上自动刷新
  - 支持“仅本地缓存（快）”或“允许增量拉取（慢）”
  - 另有独立页面：`http://127.0.0.1:8787/rankings`，支持表头点击切换升降序、币种模糊过滤
- Supports two modes:
  - `优化模式`: search best `N` and allocation mode in a range.
  - `固定参数模式`: input `min/max/fixed_n` and choose per-grid buy by either `金额` or `币种份额（数量）`.
- Allocation modes are now checkbox multi-select; you can combine any subset.
- Optimization runs as async jobs with progress bar + ETA in UI.
- Click any row in `Top 候选` to switch the active plan and immediately view that candidate's per-grid buy plan.
- Candle cache is stored locally in `data/<SYMBOL>_<INTERVAL>.csv` (for example: `data/BTCUSDT_1m.csv`).
- Funding cache is stored locally in `data/<SYMBOL>_funding.csv`.
- Kline cache is unified by `symbol+interval`; different time ranges incrementally reuse local cache.

## Output

- Best `N`
- Top-N candidates (score/net profit/drawdown/fees/trade count/trade volume/target coverage)
- Per-grid plan (`buy_price`, `sell_price`, `buy_notional`, `qty`)
- Optional JSON report and CSV plan

## Notes

- If Binance futures endpoint is unavailable in your region/network, data fetch will fail.
- V1 does not include funding fee in PnL; use `--funding-buffer` to enforce a stricter cost filter.
- This is a research/backtest tool, not financial advice.
