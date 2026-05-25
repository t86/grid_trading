# PHAROS Hedge Best Quote Maker Volume Strategy Runbook

This runbook documents the PHAROSUSDT hedge best-quote maker-volume setup used on
114 and 150.

For the 114-only frozen inventory ledger experiment, see
[PHAROS Frozen Inventory Ledger Experiment](PHAROS_FROZEN_INVENTORY_EXPERIMENT.md).

## Goal

The strategy is designed to create maker volume on both sides while keeping gross
inventory from getting stuck. It should:

- quote near best bid/ask in calm markets;
- keep small two-way participation when inventory allows;
- reduce gross inventory when one side grows too large;
- avoid opening into strong adverse trends;
- avoid chasing losing reduce orders during fast one-way moves;
- keep hard-loss reduction available as the final safety path.

## Core Order Roles

In hedge mode, long and short inventory are managed independently:

- `best_quote_entry_long`: BUY LONG, opens or adds long.
- `best_quote_entry_short`: SELL SHORT, opens or adds short.
- `best_quote_reduce_long`: SELL LONG reduce-only, reduces long.
- `best_quote_reduce_short`: BUY SHORT reduce-only, reduces short.
- `hard_loss_forced_reduce_long`: IOC SELL LONG reduce-only, emergency long reduction.
- `hard_loss_forced_reduce_short`: IOC BUY SHORT reduce-only, emergency short reduction.

## Baseline PHAROS Parameters

Current target operating band:

- `cycle_budget_notional=40`
- `max_new_orders=5`
- `max_long_notional=700`
- `max_short_notional=700`
- `inventory_soft_ratio=0.5`, so soft band is about `350U` per side
- `pause_buy_position_notional=630`
- `pause_short_position_notional=630`
- `threshold_position_notional=630`
- `max_total_notional=1450`
- base `step_price=0.00025`

## Normal Market Behavior

When inventory and loss are low, the runner places maker orders near the top of
book:

- BUY side receives long-entry orders when long entry is allowed.
- SELL side receives short-entry orders when short entry is allowed.
- With `max_new_orders=5`, the strategy can keep multiple levels while avoiding
  too many live mutations per cycle.

When both sides are active, dynamic trend bias can shift more budget to the
favored side. This is a volume allocation signal, not a risk stop by itself.

## Inventory Management

The strategy computes long/short notional and compares each side with its soft
band.

Normal mode:

- if both sides are below soft and balanced enough, quote both directions;
- if one side is materially heavier, `inventory_bias` can allocate more budget
  to reduce the heavier side while leaving a small same-side entry for two-way
  volume.

Inventory recover mode:

- if long exceeds soft, prioritize `best_quote_reduce_long`;
- if short exceeds soft, prioritize `best_quote_reduce_short`;
- with `inventory_bias_reduce_share=0.8`, about 80% of cycle budget is assigned
  to the reduce side when bias applies;
- small fallback entry is allowed only to keep two-way participation, not as a
  reason to scale up volume.

Inventory bias should not trigger on tiny residual differences. The notional gap
is tied to soft band by:

- `inventory_bias_min_notional_gap_soft_ratio=0.35`

With a `350U` soft band, the side gap must be roughly `122.5U` before the bias
acts like a real one-sided inventory correction.

## Dynamic Volatility Control

Dynamic control uses short-window return and amplitude:

- low volatility: step can tighten and budget can expand;
- high volatility: step widens and budget shrinks;
- extreme volatility: step widens more and budget shrinks more.

PHAROS target values:

- low volatility ratio: `0.004`
- low volatility budget scale: `1.5`
- low volatility budget max inventory ratio: `0.85`
- high volatility budget scale: normally below `1.0`
- extreme volatility budget scale: normally much lower, around `0.45`

Low-vol budget expansion is allowed only while inventory has room and soft-loss
conditions are not active.

## Trend Entry Guard

`trend_entry_guard` prevents new adverse entries in strong high-volatility
trends:

- strong uptrend blocks or minimizes SELL SHORT entry;
- strong downtrend blocks or minimizes BUY LONG entry;
- reduce-only orders are not blocked by this guard.

Target values:

- enabled: `true`
- `min_score=0.75`
- `min_volatility_ratio=0.0035`
- `conflict_ratio=0.25`
- `opposite_budget_scale=0.0`

This guard prevents adding new inventory into a one-way move, but it does not by
itself stop losing reduce orders from chasing the market.

## Trend Inventory Guard

`trend_inventory_guard` activates when inventory is already meaningfully high
and the market trend is adverse to reducing that side:

- high short inventory during uptrend: shrink short entry, slow BUY SHORT reduce,
  and move reduce farther from top of book;
- high long inventory during downtrend: shrink long entry, slow SELL LONG reduce,
  and move reduce farther from top of book.

Target values:

- enabled: `true`
- `start_ratio=0.70`
- `min_score=0.55`
- `min_volatility_ratio=0.0035`
- `entry_budget_scale=0.25`
- `reduce_budget_scale=0.50`
- `reduce_extra_ticks=4`

This is a high-inventory anti-chase guard. It does not cover smaller inventory
where strong trends still make reduce orders costly.

## Trend Loss Reduce Guard

`trend_loss_reduce_guard` addresses the main observed PHAROS loss source:
defensive/recover reduce orders chasing during a strong trend.

Behavior:

- strong uptrend + any short inventory: slow `best_quote_reduce_short`;
- strong downtrend + any long inventory: slow `best_quote_reduce_long`;
- reduce order size is scaled down;
- reduce price is moved farther from the top of book;
- hard-loss forced reduce is not blocked.

Target values:

- enabled: `true`
- `min_score=0.75`
- `min_volatility_ratio=0.0035`
- `reduce_budget_scale=0.35`
- `reduce_extra_ticks=6`

This guard is intentionally narrower than a general pause. It only changes
adverse reduce behavior in strong high-volatility trends, so normal two-way BQ
volume remains available in calmer markets.

When both `trend_inventory_guard` and `trend_loss_reduce_guard` apply, the runner
uses the more conservative budget scale and the larger extra tick offset. The
scales are not multiplied, so reduce orders do not become too small to place.

## Hard-Loss Forced Reduce

Hard-loss forced reduce is the final safety path. It places IOC reduce-only
orders toward a target notional when unrealized loss exceeds the configured
limit.

PHAROS safety intent:

- keep hard-loss enabled;
- keep max order notional small enough to avoid panic-size execution;
- do not let trend anti-chase guards block hard-loss orders.

## What To Watch

Every patrol should check:

- runner active and command still uses `hedge_best_quote_maker_volume_v1`;
- control JSON contains the target parameters;
- latest plan has two-way BQ orders or reduce-only orders consistent with state;
- `trend_entry_guard`, `trend_inventory_guard`, and `trend_loss_reduce_guard`
  reports;
- `rolling_hourly_loss`, `rolling_hourly_loss_per_10k`, and realized loss by
  reduce side;
- open-order reconcile drift;
- Binance API errors, especially `-4061`;
- volatility pause and defensive states;
- long/short notional versus soft and pause thresholds.

Do not scale volume just because hourly volume is low if:

- recent realized loss is high;
- soft inventory is active;
- trend loss reduce guard is repeatedly active;
- reconcile drift is persistent;
- API errors or runtime guard cooldowns are present.

## Deployment Notes

Production deploys must be pull-based:

1. Commit locally.
2. Push to remote `main`.
3. Run the server update wrapper.
4. Restart saved runners only through the saved-runner wrapper.
5. Verify HEAD, service, runner command, control JSON, latest plan, and reconcile.

Server wrappers:

- 114: `/usr/local/bin/grid-web-update`, `/usr/local/bin/grid-saved-runner`
- 150: `/usr/local/bin/grid-web-api2-update`, `/usr/local/bin/grid-saved-runner-api2`
