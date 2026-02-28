# Arithmetic Long-Grid Optimizer (BTC/ETH Perp)

This tool optimizes arithmetic long-grid count (`N`) on Binance perpetual klines.

Strategy assumptions (V1):
- Arithmetic grid between `min_price` and `max_price`.
- Long-only grid:
  - In each candle, trigger by intrabar path (`open-low-high-close` for bullish candles, `open-high-low-close` for bearish candles).
  - Crossing down a grid line: open long for that grid.
  - Crossing up to next line: close that grid long.
- Per-side fee defaults to `0.0002` (万二), applied to each fill.
- `total_buy_notional` is the total notional fully deployed if price reaches `min_price`.
- Allocation modes (complete list in current system):
  - `equal`: equal notional per grid.
  - `equal_qty`: equal base-asset quantity per grid (closer to exchange quantity grid behavior).
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
cd <repo>
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
  --lookback-days 365 \
  --interval 1h \
  --n-min 5 \
  --n-max 200 \
  --fee-rate 0.0002 \
  --allocation-modes equal,linear \
  --objective net_profit \
  --min-trade-count 60 \
  --min-avg-capital-usage 0.05 \
  --top-k 5 \
  --report-json output/btc_report.json \
  --plan-csv output/btc_plan.csv
```

Objective options:
- `calmar` (default): risk-adjusted, may prefer fewer trades and lower drawdown
- `net_profit`: maximize absolute net profit
- `total_return`: maximize total return
- `annualized_return`: maximize annualized return
- `competition_volume`: for trading competitions, prioritize target traded notional first, then smaller loss / higher profit

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
cd <repo>
PYTHONPATH=src python3 -m grid_optimizer.web --host 127.0.0.1 --port 8787
```

Then open:

```text
http://127.0.0.1:8787
```

You can input parameters in browser and run optimization directly.

Web UI notes:
- Supports two modes:
  - `优化模式`: search best `N` and allocation mode in a range.
  - `固定参数模式`: input `min/max/fixed_n/per_grid_notional` and backtest directly.
- Supports explicit backtest window with `开始时间` and `结束时间` (instead of only lookback days).
- Supports `目标交易量（成交额）` and `competition_volume` objective for Binance competition-style ranking.
- Supports `智能建议 min/max` button to auto-reverse-engineer candidate price ranges for competition goals.
- `最大投入金额` is used as strategy capital upper bound in optimization mode.
- Allocation modes are now checkbox multi-select; you can combine any subset.
- Optimization runs as async jobs with progress bar + ETA in UI.
- Layer generator supports:
  - `拟合当前方案`: segment-fit per-grid buy amount with error metrics (MAPE/MAE).
  - `递减覆盖（币安数量模式）`: nested-coverage layers for quantity-based exchange grids.
  - `分层组合回测 vs 当前方案` side-by-side metrics comparison.
- Click any row in `Top 候选` to switch the active plan and immediately view that candidate's per-grid buy plan.
- Candle cache is stored locally in `data/<SYMBOL>_<INTERVAL>.csv` (for example: `data/BTCUSDT_1m.csv`).
- Minute-level cache is reused across different lookback windows, so first run is slower and later runs are much faster.
- `1s` interval is supported by aggregating Binance Futures `aggTrades` into 1-second OHLC candles, with local CSV cache reuse.

## Deploy (Oracle Always Free)

This repository includes automatic deployment to Oracle VM through GitHub Actions:

- Workflow: `.github/workflows/deploy-oracle.yml`
- Remote installer: `deploy/oracle/install_or_update.sh`
- Full guide: `deploy/oracle/README.md`

Required GitHub secrets:
- `ORACLE_HOST`
- `ORACLE_USER`
- `ORACLE_SSH_KEY`

## Output

- Best `N`
- Top-N candidates (score/net profit/drawdown/fees/trade count/trade volume/target coverage)
- Per-grid plan (`buy_price`, `sell_price`, `buy_notional`, `qty`)
- Optional JSON report and CSV plan

## Notes

- If Binance futures endpoint is unavailable in your region/network, data fetch will fail.
- V1 does not include funding fee in PnL; use `--funding-buffer` to enforce a stricter cost filter.
- This is a research/backtest tool, not financial advice.
