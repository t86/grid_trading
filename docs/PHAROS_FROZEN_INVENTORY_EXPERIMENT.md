# PHAROS Frozen Inventory Ledger Experiment

This document defines the PHAROSUSDT frozen inventory ledger experiment for the
hedge best-quote maker-volume runner. It is a strategy experiment, not an
exchange account mode change.

## Scope

The experiment is intended for `114` first. `150` should stay on the normal
PHAROS Hedge BQ configuration unless explicitly moved into the experiment.

Production paths:

- `114`: `/home/ubuntu/wangge`, runner `/usr/local/bin/grid-saved-runner`
- `150`: `/home/ubuntu/wangge_api2_repo` and `/home/ubuntu/wangge_api2`, runner
  `/usr/local/bin/grid-saved-runner-api2`

The current experiment is for `PHAROSUSDT` with strategy mode
`hedge_best_quote_maker_volume_v1`.

## Intent

The strategy is split into two conceptual layers:

- BQ volume layer: keeps normal two-sided maker volume and manages only
  non-frozen inventory.
- Frozen inventory ledger layer: records losing inventory that should be
  isolated from BQ volume decisions and handled separately.

The desired behavior is:

- keep BQ volume running when inventory is manageable;
- when normal reduce starts to become expensive, stop chasing reduce orders;
- isolate the losing inventory into a strategy ledger;
- remove the frozen inventory from BQ risk metrics and budget calculations;
- allow later manual or strategy-directed cleanup without reopening the same
  loss spiral.

The ledger is a strategy accounting layer. It does not move positions to
Binance isolated margin and does not create a separate exchange-side position.
Exchange-side long/short position remains the actual source of truth.

## Independent Accounting Model

The experiment must use independent strategy ledgers. It must not infer BQ cost
or BQ PnL from Binance total position, mixed hedge entry price, or exchange
unrealized PnL after inventory has been frozen.

There are three accounting buckets:

- `best_quote_volume_ledger`: the BQ volume ledger. It owns only normal BQ
  entry/reduce lots, cost basis, BQ realized PnL, BQ unrealized PnL, fees, and
  BQ gross notional.
- `best_quote_frozen_inventory`: the frozen-inventory ledger. It owns frozen
  lots, frozen cost basis, pair-release/manual-limit/manual-reduce state, and
  frozen realized/unrealized PnL.
- manual/external accounting: operator activity that is not a BQ order and not
  an explicit frozen-inventory directive. This must be reported as external
  adjustment or drift; it must not silently change BQ PnL.

Rules:

- BQ order placement, BQ inventory caps, BQ soft/pause thresholds, BQ
  volatility pause, BQ trend guards, and BQ loss gates must read from
  `best_quote_volume_ledger`, not from frozen ledger fields.
- Frozen qty, frozen cost, frozen unrealized PnL, manual frozen cleanup PnL, and
  paired-release PnL must not enter BQ loss or BQ volume metrics.
- When BQ inventory is frozen, it is an internal transfer: the selected BQ lots
  and their cost basis are removed from `best_quote_volume_ledger` and appended
  to `best_quote_frozen_inventory`.
- Exchange total position is used only for reconciliation and safety checks. It
  is not a valid source for BQ cost basis once frozen inventory exists.
- If the BQ ledger cannot match a freeze transfer quantity, the runner must
  report `bq_volume_ledger_transfer_shortfall` instead of inventing cost from
  mixed exchange average price.

## Trigger Logic

When the runner enters normal reduce for a side:

- allow normal reduce while the side's unrealized loss ratio is below the
  configured threshold;
- once unrealized loss reaches or exceeds the effective threshold for enough
  consecutive confirmation cycles, stop normal reduce for that side;
- record the remaining side inventory into `best_quote_frozen_inventory`;
- treat that side inventory as frozen and outside the BQ volume layer.

The initial experiment threshold is:

```text
best_quote_maker_volume_reduce_freeze_loss_ratio = 0.01
best_quote_maker_volume_reduce_freeze_confirm_cycles = 3
best_quote_maker_volume_reduce_freeze_stress_loss_ratio = 0.015
best_quote_maker_volume_reduce_freeze_stress_1m_abs_return_ratio = 0.0025
best_quote_maker_volume_reduce_freeze_stress_1m_amplitude_ratio = 0.0035
```

This means roughly 1% unrealized loss on the reducing side under normal market
conditions, confirmed for three consecutive cycles. When the 1m absolute return
or 1m amplitude crosses the stress thresholds, the freeze threshold is raised to
1.5% before the same confirmation rule is applied. The threshold should be
evaluated against the side's entry price and current market price:

- LONG loss ratio: `(entry_price - mid_price) / entry_price`
- SHORT loss ratio: `(mid_price - entry_price) / entry_price`

The freeze check must not wait for inventory to reach the soft threshold. A side
can be frozen below soft if the reduce loss threshold has already been reached.
If the candidate side, managed quantity, effective threshold, or stress state
changes before confirmation completes, the confirmation count restarts. This is
intentional: it prevents a short-lived 1% touch, or rapidly changing managed
inventory, from immediately creating a new frozen lot.

## Ledger Fields

The normal BQ layer stores an independent ledger:

```json
"best_quote_volume_ledger": {
  "schema": "best_quote_volume_ledger_v1",
  "initialized": true,
  "long_lots": [{"qty": 0.0, "price": 0.0}],
  "short_lots": [{"qty": 0.0, "price": 0.0}],
  "realized_pnl": 0.0,
  "commission": 0.0,
  "gross_notional": 0.0,
  "last_trade_time_ms": 0,
  "last_trade_keys_at_time": [],
  "sync_ok": true
}
```

Only `gx-...-bestquot-...` fills are allowed into this ledger. In hedge mode the
role is reconstructed from side and position side:

- BUY LONG: `best_quote_entry_long`
- SELL LONG: `best_quote_reduce_long`
- SELL SHORT: `best_quote_entry_short`
- BUY SHORT: `best_quote_reduce_short`

Any `gx-...-frozenin-...` fill is frozen-ledger activity and must be excluded
from BQ accounting.

The strategy state file stores the ledger under:

```json
"best_quote_frozen_inventory": {
  "long_qty": 0.0,
  "short_qty": 0.0,
  "long_notional": 0.0,
  "short_notional": 0.0,
  "long_entry_price": 0.0,
  "short_entry_price": 0.0,
  "long_frozen_at": "",
  "short_frozen_at": "",
  "offset_qty": 0.0,
  "offset_notional": 0.0,
  "updated_at": ""
}
```

Rules:

- `long_qty` and `short_qty` represent strategy-frozen exchange inventory.
- `long_entry_price` and `short_entry_price` are the ledger cost references.
- `offset_qty` shows how much opposite-side frozen inventory can potentially
  offset, for example a later frozen short can offset an earlier frozen long.
- `updated_at` records the latest ledger mutation time.

The ledger must be visible in the running status UI. The intended UI entry point
is the running-status / monitor surface that already exposes
`/api/runner/frozen_inventory`.

## Isolation Rules

Frozen inventory must be excluded from the BQ layer's managed risk metrics.
The current model is:

- managed long qty = `best_quote_volume_ledger.long_qty`;
- managed short qty = `best_quote_volume_ledger.short_qty`;
- managed cost basis = BQ ledger lot cost;
- managed unrealized PnL = BQ ledger unrealized PnL;
- strategy unrealized PnL uses BQ ledger unrealized PnL while frozen inventory is
  present, even if exchange total unrealized PnL differs;
- BQ max-long, max-short, soft, pause, entry, and reduce decisions must be based
  on BQ ledger inventory, not total exchange inventory and not frozen inventory.

The submit-side position consistency check must still accept exchange position
equal to `managed + frozen`. Otherwise the runner will reject all submits after
freezing inventory.

The older `exchange - frozen` calculation is allowed only as a legacy fallback
when `best_quote_volume_ledger` is unavailable. It must be marked as a fallback
source and must not be trusted for PnL-sensitive decisions if exchange PnL sync
is already known to be inconsistent.

## Reduced Soft Threshold

Because the experiment can accumulate frozen inventory, normal BQ capacity must
be more conservative while testing. For the experiment host, reduce the soft
threshold by 30%.

If the normal soft ratio is:

```text
best_quote_maker_volume_inventory_soft_ratio = 0.5
```

the experiment target is:

```text
best_quote_maker_volume_inventory_soft_ratio = 0.35
```

Do not compensate by raising `max_long_notional`, `max_short_notional`, or
`max_total_notional` during the experiment. If frozen inventory grows faster
than expected, stop the runner and review the ledger.

## Allowed Cleanup Path

Manual handling must be real position cleanup, not just a cosmetic ledger mark.
However, cleanup must be routed through an explicit frozen-inventory directive
so the strategy can account for it.

Supported control actions:

- `reduce_long`: request real reduce-only cleanup of frozen long inventory;
- `reduce_short`: request real reduce-only cleanup of frozen short inventory;
- `limit_long`: request a persistent reduce-only limit order for frozen long
  inventory;
- `limit_short`: request a persistent reduce-only limit order for frozen short
  inventory;
- `clear_long`: mark frozen long as handled only after the actual exchange-side
  position has been independently confirmed clean;
- `clear_short`: mark frozen short as handled only after the actual exchange-side
  position has been independently confirmed clean;
- `reset`: clear the entire ledger only after manual verification.

The strategy writes a directive:

```json
"best_quote_frozen_inventory_manual_reduce": {
  "long": {
    "requested": true,
    "requested_at": "...",
    "source": "running_status_ui"
  }
}
```

or:

```json
"best_quote_frozen_inventory_manual_reduce": {
  "short": {
    "requested": true,
    "requested_at": "...",
    "source": "running_status_ui"
  }
}
```

The runner then places explicit frozen cleanup orders:

- `frozen_inventory_manual_reduce_long`: SELL LONG reduce-only IOC
- `frozen_inventory_manual_reduce_short`: BUY SHORT reduce-only IOC

Only these explicit frozen cleanup roles should be allowed to intentionally
reduce frozen inventory.

### Manual Limit Close Isolation

Frozen inventory also supports operator-directed limit close orders:

```json
"best_quote_frozen_inventory_manual_limit": {
  "short": {
    "requested": true,
    "requested_qty": 100.0,
    "price": 0.7,
    "requested_at": "...",
    "source": "running_status_ui"
  }
}
```

When a limit-close directive is submitted, the requested quantity is immediately
recorded in the frozen ledger as isolated:

```json
"best_quote_frozen_inventory": {
  "short_manual_limit_isolated_qty": 100.0,
  "pair_eligible_short_qty": 2654.0
}
```

The runner then keeps a persistent reduce-only post-only limit order in the plan:

- `frozen_inventory_manual_limit_long`: SELL LONG reduce-only GTX
- `frozen_inventory_manual_limit_short`: BUY SHORT reduce-only GTX

The isolated quantity must not be used by automatic paired release. Pair release
can only use `pair_eligible_long_qty` and `pair_eligible_short_qty`, i.e. frozen
inventory not already assigned to a manual limit close order.

Manual limit close orders can be cancelled independently for each side:

- `cancel_limit_long`: remove the frozen-long limit-close directive and release
  `long_manual_limit_isolated_qty` back into pair-eligible frozen inventory;
- `cancel_limit_short`: remove the frozen-short limit-close directive and
  release `short_manual_limit_isolated_qty` back into pair-eligible frozen
  inventory.

If the post-only order has already reached the exchange, removing the directive
causes the runner's next reconcile cycle to stop preserving that manual-limit
order, so the stale order is cancelled by the normal cancel-stale path.

Manual frozen-inventory cleanup directives are operator actions, not normal
volume generation. Runtime loss cooldown must still block ordinary entry/place
orders, but it must allow frozen manual reduce-only IOC orders, frozen manual
limit reduce-only GTX orders, and one-shot frozen pair-release reduce-only
orders to be generated and submitted.

## Automatic Paired Frozen Release

When both sides have meaningful frozen inventory, the strategy may reduce gross
exchange exposure by pairing frozen long and frozen short inventory.

The automatic paired release rule is:

- require frozen long notional and frozen short notional to each be at least the
  configured side threshold;
- for the 114 experiment, use `100U` as the side threshold;
- release at most one configured batch per runner cycle;
- for the 114 experiment, use `100U` as the batch notional;
- place both legs together as reduce-only IOC orders:
  - `frozen_inventory_pair_release_long`: SELL LONG reduce-only IOC;
  - `frozen_inventory_pair_release_short`: BUY SHORT reduce-only IOC;
- keep the existing market-stability gates before placing the pair:
  `max_30s_abs_return_ratio`, `max_1m_abs_return_ratio`, and
  `max_1m_amplitude_ratio`;
- keep the pair PnL buffer gate unless the operator explicitly changes it.

The intended configuration for this behavior is:

```text
best_quote_maker_volume_frozen_pair_release_enabled = true
best_quote_maker_volume_frozen_pair_release_max_notional = 100
best_quote_maker_volume_frozen_pair_release_min_side_notional = 100
```

This is not a generic flatten. It is only allowed to touch inventory already
tracked in `best_quote_frozen_inventory`, and normal managed BQ reduce orders
must still be capped to managed inventory only.

## Forbidden Cleanup Path

Generic manual flatten or close-position orders must not process frozen
inventory.

Forbidden examples:

- `maker_flatten_runner` close-long / close-short orders;
- generic close-all-position buttons;
- manual trade close-short / close-long legs that do not reference the frozen
  inventory directive;
- any `mf...closeshort...` or `mf...closelong...` order used as a side effect of
  runtime loss cooldown.

The incident observed on `114` showed why this matters:

- short inventory was frozen into the ledger;
- `runtime_guard` entered loss cooldown;
- the cooldown path still started `maker_flatten_runner`;
- an `mfpharos_closesho...` BUY SHORT order closed the frozen short inventory;
- the frozen ledger disappeared and realized loss was taken immediately.

This is not valid experiment behavior.

Runtime guard stops must cancel/block normal strategy orders. They must not
start flattening frozen inventory when an explicit frozen-inventory manual
directive is pending.

## Runtime Guard Interaction

Runtime loss cooldown is a hard gate for new BQ orders.

When `runtime_guard_loss_recovery` has `stopped_at` and no later `recovered_at`:

- submit must drop all new place orders;
- cancel actions may still run;
- normal BQ entry and normal reduce are not allowed;
- frozen cleanup is allowed only through an explicit frozen-inventory cleanup
  directive and only after operator intent is clear.

The same exception applies to non-loss runtime stops such as
`max_actual_net_notional_hit`: if a pending frozen-inventory manual directive is
present, the runner records
`runtime_guard_manual_frozen_inventory_override` and continues only far enough
to submit the explicit frozen reduce-only action. Normal BQ entry, normal BQ
reduce, and full flatten remain blocked. Manual frozen operations must not be
blocked by cooldown or max-notional gates, because those are exactly the tools
used to reduce or release isolated frozen inventory.

Each runner cycle should sync the latest trade audit before evaluating runtime
guard. This prevents the sequence:

1. submit new BQ orders;
2. sync recent losing fills;
3. discover loss cooldown after the damage is already done.

## Daily Volume Cap Interaction

Daily volume cap remains independent from frozen inventory.

For PHAROS patrol, daily volume is measured per server from Beijing time
`08:00` to next-day `08:00`, using order-level gross notional from
`output/pharosusdt_hedge_bq_trade_audit.jsonl`.

Rules:

- dedupe by `orderId`;
- use `quoteQty` when available;
- otherwise use `price * abs(qty)`;
- do not sum partial fills or `tradeId` rows as separate cap volume.

If a host reaches `40000U`, stop that host's PHAROS runner. Do not restart just
because frozen inventory exists.

## Operating Procedure

Before enabling the experiment on `114`:

1. Confirm runner is stopped.
2. Confirm open orders are zero or intentionally understood.
3. Confirm exchange long/short positions.
4. Confirm `best_quote_frozen_inventory` is either empty or matches exchange
   inventory that should be isolated.
5. Confirm runtime cooldown state is understood.
6. Confirm soft threshold is reduced by 30%.
7. Start through `/usr/local/bin/grid-saved-runner start PHAROSUSDT` or restart
   only after the restart gate is satisfied.

During the experiment:

- watch frozen long/short qty and notional;
- watch managed long/short qty separately from exchange long/short qty;
- watch whether normal BQ is truly two-sided;
- watch whether any `mf...close...` order appears;
- watch whether loss cooldown suppresses new places;
- watch whether frozen cleanup roles are the only orders touching frozen
  inventory.

Stop immediately if:

- frozen inventory is reduced by a generic flatten/manual close path;
- runtime cooldown still places new BQ orders;
- open-order reconcile drift persists;
- Binance API errors appear, especially `-4061`;
- frozen inventory grows without offset or without an operator plan.

## Restart Gate After Incident

After a freeze-ledger incident, do not auto-restart.

Require:

- runner inactive;
- no live open orders;
- exchange long/short positions known;
- frozen ledger reconciled against exchange position;
- runtime guard state reviewed;
- recent realized loss reviewed;
- operator decision recorded: continue experiment, re-freeze residual inventory,
  or manually flatten and reset ledger.

If the ledger was accidentally cleared by a real order, do not pretend the
ledger still represents inventory. Either re-create the ledger from current
exchange position intentionally, or leave it empty and report the realized
cleanup.

## Known Failure Mode From 2026-05-22

On `114`, frozen short inventory was not preserved:

- frozen short qty existed after reduce-freeze;
- a loss cooldown path started `maker_flatten_runner`;
- an `mfpharos_closesho...` BUY SHORT order closed the frozen short;
- the ledger became empty;
- later a normal BQ `SELL SHORT` opened a smaller short again.

Fix requirements from this incident:

- loss-only cooldown must not start flatten;
- submit must suppress all new place orders while loss recovery is unrecovered;
- audit sync must happen before runtime guard evaluation;
- generic manual/flatten paths must not be used as frozen cleanup.

## Review Checklist

For each patrol, report:

- runner status;
- exchange open-order count;
- exchange long/short qty and notional;
- ledger long/short qty and notional;
- managed long/short qty and notional;
- whether BQ is normal two-sided, soft reduce, hard reduce, or cooldown;
- whether any frozen cleanup directive is active;
- whether any forbidden `mf...close...` order appeared;
- daily 08:00 window volume versus `40000U`;
- whether parameters were changed.
