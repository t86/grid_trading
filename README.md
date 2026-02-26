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
- Allocation modes:
  - `equal`: equal notional per grid.
  - `linear`: lower price grids get larger notional (linearly).

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

Web UI notes:
- Click any row in `Top 候选` to switch the active plan and immediately view that candidate's per-grid buy plan.
- Candle cache is stored locally in `data/<SYMBOL>_<INTERVAL>.csv` (for example: `data/BTCUSDT_1m.csv`).
- Minute-level cache is reused across different lookback windows, so first run is slower and later runs are much faster.

## Output

- Best `N`
- Top-N candidates (score/net profit/drawdown/fees/trade count)
- Per-grid plan (`buy_price`, `sell_price`, `buy_notional`, `qty`)
- Optional JSON report and CSV plan

## Notes

- If Binance futures endpoint is unavailable in your region/network, data fetch will fail.
- V1 does not include funding fee in PnL; use `--funding-buffer` to enforce a stricter cost filter.
- This is a research/backtest tool, not financial advice.
