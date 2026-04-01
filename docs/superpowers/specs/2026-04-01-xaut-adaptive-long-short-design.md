# XAUT Adaptive Long/Short Design

Date: 2026-04-01

## Summary

Add two XAUT-only strategy profiles:

- `xaut_long_adaptive_v1`
- `xaut_short_adaptive_v1`

Both profiles are specialized state machines for `XAUTUSDT`. They keep the existing one-way micro-grid execution model, but automatically switch among three risk states:

- `normal`
- `defensive`
- `reduce_only`

The goal is:

- keep volume high during stable conditions
- reduce inventory expansion during elevated volatility
- immediately stop new same-direction inventory accumulation during extreme moves
- automatically recover back to normal after volatility cools down

This design does **not** auto-flatten the entire position and does **not** stop the runner process. In `reduce_only`, the runner continues working but only places orders that reduce existing inventory.

## Problem

The current runner has two useful pieces, but neither is enough for XAUT:

1. Generic `auto_regime` supports only `stable <-> defensive` switching and is tuned for broad use, not for XAUT.
2. `excess_inventory_reduce_only_enabled` can prevent further inventory growth once current inventory exceeds target base inventory, but it does not react to XAUT-specific volatility regimes by itself.

For `XAUTUSDT`, recent market behavior is materially tighter than the generic thresholds currently used by `auto_regime`.

Observed on 2026-03-31 from Binance Futures public market data:

- recent 48h `15m` amplitude median: about `0.204%`
- recent 48h `15m` amplitude `P95`: about `0.614%`
- recent 7d `1h` amplitude median: about `0.341%`
- recent 7d `1h` amplitude `P95`: about `1.186%`

The current generic thresholds are much wider than these ranges, so they would rarely move XAUT out of the stable path. That does not meet the desired risk behavior.

## Scope

In scope:

- add XAUT-only adaptive long and short profiles
- add XAUT-specific regime assessment logic
- add three-state switching with hysteresis
- add `reduce_only` behavior that immediately removes new same-direction opening orders
- add monitor/web visibility for active XAUT state and reason
- add automated tests for transition logic and order-pruning behavior

Out of scope:

- changing non-XAUT profiles
- automatic process stop
- automatic full flatten/clear position
- generic multi-symbol adaptive framework

## Profiles

### `xaut_long_adaptive_v1`

Direction: `one_way_long`

Behavior:

- `normal`: standard XAUT long volume-first grid
- `defensive`: lighter long accumulation, wider spacing, faster inventory unloading
- `reduce_only`: immediately remove all new long-opening orders and only keep sell orders that reduce long inventory

### `xaut_short_adaptive_v1`

Direction: `one_way_short`

Behavior:

- `normal`: standard XAUT short mirror grid
- `defensive`: lighter short accumulation, wider spacing, faster short cover bias
- `reduce_only`: immediately remove all new short-opening orders and only keep buy orders that reduce short inventory

## State Model

Both profiles use the same state names:

- `normal`
- `defensive`
- `reduce_only`

Both profiles use the same volatility inputs:

- `15m` amplitude
- `60m` amplitude
- `15m` return
- `60m` return

Definitions:

- amplitude = `(high - low) / open`
- return = `(close - open) / open`

State priority:

1. `reduce_only`
2. `defensive`
3. `normal`

If multiple conditions match, the higher-priority state wins.

## Thresholds

These thresholds are XAUT-specific and based on the observed amplitude distribution from 2026-03-31 market data. They are intentionally tighter than the current generic auto-regime thresholds.

### Shared stable-amplitude gate

Used for both long and short recovery:

- `stable_15m_max_amplitude_ratio = 0.0035`
- `stable_60m_max_amplitude_ratio = 0.0075`

### Long thresholds

`normal` eligibility:

- `15m amplitude <= 0.35%`
- `60m amplitude <= 0.75%`
- `60m return >= -0.30%`

Enter `defensive` when any of these is true:

- `15m amplitude >= 0.60%`
- `60m amplitude >= 1.20%`
- `15m return <= -0.40%`
- `60m return <= -0.80%`

Enter `reduce_only` when any of these is true:

- `15m amplitude >= 0.90%`
- `60m amplitude >= 1.60%`
- `15m return <= -0.70%`
- `60m return <= -1.20%`

### Short thresholds

`normal` eligibility:

- `15m amplitude <= 0.35%`
- `60m amplitude <= 0.75%`
- `60m return <= 0.30%`

Enter `defensive` when any of these is true:

- `15m amplitude >= 0.60%`
- `60m amplitude >= 1.20%`
- `15m return >= 0.40%`
- `60m return >= 0.80%`

Enter `reduce_only` when any of these is true:

- `15m amplitude >= 0.90%`
- `60m amplitude >= 1.60%`
- `15m return >= 0.70%`
- `60m return >= 1.20%`

## Transition Rules

Adjacent-state transitions require confirmation to avoid flip-flopping:

- `normal -> defensive`: 2 consecutive loops
- `defensive -> normal`: 2 consecutive loops
- `defensive -> reduce_only`: 1 loop
- `reduce_only -> defensive`: 2 consecutive loops

Direct transitions:

- `normal -> reduce_only`: allowed immediately on first qualifying loop
- `reduce_only -> normal`: not allowed directly; must pass through `defensive`

Reasoning:

- escalation into `reduce_only` should be fast because the user explicitly wants to stop buying/opening immediately during extreme moves
- recovery should be slower than escalation
- forcing `reduce_only -> defensive -> normal` gives a cooldown phase instead of snapping straight back to aggressive volume mode

## Parameter Sets

These values are intentionally lighter than the current generic presets and are tuned for XAUT’s price level and volatility.

### Long `normal`

- `step_price = 7.5`
- `buy_levels = 6`
- `sell_levels = 10`
- `per_order_notional = 80`
- `base_position_notional = 320`
- `up_trigger_steps = 5`
- `down_trigger_steps = 4`
- `shift_steps = 3`
- `pause_buy_position_notional = 520`
- `max_position_notional = 680`
- `buy_pause_amp_trigger_ratio = 0.0060`
- `buy_pause_down_return_trigger_ratio = -0.0045`
- `freeze_shift_abs_return_trigger_ratio = 0.0048`
- `inventory_tier_start_notional = 420`
- `inventory_tier_end_notional = 520`
- `inventory_tier_buy_levels = 3`
- `inventory_tier_sell_levels = 12`
- `inventory_tier_per_order_notional = 70`
- `inventory_tier_base_position_notional = 160`

### Long `defensive`

- `step_price = 12.0`
- `buy_levels = 2`
- `sell_levels = 14`
- `per_order_notional = 45`
- `base_position_notional = 100`
- `up_trigger_steps = 4`
- `down_trigger_steps = 6`
- `shift_steps = 2`
- `pause_buy_position_notional = 180`
- `max_position_notional = 260`
- `buy_pause_amp_trigger_ratio = 0.0045`
- `buy_pause_down_return_trigger_ratio = -0.0035`
- `freeze_shift_abs_return_trigger_ratio = 0.0040`
- `inventory_tier_start_notional = 140`
- `inventory_tier_end_notional = 180`
- `inventory_tier_buy_levels = 1`
- `inventory_tier_sell_levels = 16`
- `inventory_tier_per_order_notional = 40`
- `inventory_tier_base_position_notional = 60`

### Long `reduce_only`

Keep the `defensive` parameter set, plus these behavioral overrides:

- force `excess_inventory_reduce_only_enabled = true`
- remove all `bootstrap_orders`
- remove all `buy_orders`
- rely on stale-order cancellation to remove existing runner-owned buy orders from the book immediately

### Short `normal`

- `step_price = 7.5`
- `buy_levels = 10`
- `sell_levels = 6`
- `per_order_notional = 80`
- `base_position_notional = 320`
- `up_trigger_steps = 4`
- `down_trigger_steps = 5`
- `shift_steps = 3`
- `pause_short_position_notional = 520`
- `max_short_position_notional = 680`
- `short_cover_pause_amp_trigger_ratio = 0.0060`
- `short_cover_pause_down_return_trigger_ratio = -0.0045`
- `inventory_tier_start_notional = 420`
- `inventory_tier_end_notional = 520`
- `inventory_tier_buy_levels = 12`
- `inventory_tier_sell_levels = 3`
- `inventory_tier_per_order_notional = 70`
- `inventory_tier_base_position_notional = 160`

### Short `defensive`

- `step_price = 12.0`
- `buy_levels = 14`
- `sell_levels = 2`
- `per_order_notional = 45`
- `base_position_notional = 100`
- `up_trigger_steps = 6`
- `down_trigger_steps = 4`
- `shift_steps = 2`
- `pause_short_position_notional = 180`
- `max_short_position_notional = 260`
- `short_cover_pause_amp_trigger_ratio = 0.0045`
- `short_cover_pause_down_return_trigger_ratio = -0.0035`
- `inventory_tier_start_notional = 140`
- `inventory_tier_end_notional = 180`
- `inventory_tier_buy_levels = 16`
- `inventory_tier_sell_levels = 1`
- `inventory_tier_per_order_notional = 40`
- `inventory_tier_base_position_notional = 60`

### Short `reduce_only`

Keep the `defensive` parameter set, plus these behavioral overrides:

- force `excess_inventory_reduce_only_enabled = true`
- remove all `bootstrap_orders`
- remove all `sell_orders`
- rely on stale-order cancellation to remove existing runner-owned sell orders from the book immediately

## Execution Flow

Per loop:

1. Load market data and symbol info as today’s runner already does.
2. If selected profile is one of the new XAUT adaptive profiles, compute XAUT regime metrics from recent `15m` and `1h` klines.
3. Evaluate the state candidate for the chosen direction.
4. Resolve the active state using the per-transition confirmation rules.
5. Overlay the parameter set for the active state onto the runner args.
6. Generate the plan using the existing one-way long or one-way short planner.
7. If the active state is `reduce_only`, strip same-direction opening orders from the generated plan.
8. Submit/cancel orders as normal with existing stale-order cancellation behavior.

This preserves the existing execution path and only changes parameter selection and final same-direction order pruning.

## Implementation Shape

### Presets

Add new preset definitions in the web/server preset registry:

- `xaut_long_adaptive_v1`
- `xaut_short_adaptive_v1`

These presets should:

- set `symbol = XAUTUSDT`
- enable a new XAUT adaptive mode flag
- include the state thresholds and confirmation counts
- default `autotune_symbol_enabled = false`
- default `cancel_stale = true`

### Loop runner

Add XAUT-specific helpers in `loop_runner.py`:

- state constants
- state config tables for long and short
- helper to assess current XAUT regime
- helper to resolve state transitions with hysteresis
- helper to overlay active-state parameters onto runtime args
- helper to apply `reduce_only` order pruning after plan generation

This should be kept separate from the existing generic `auto_regime` path rather than trying to stretch the generic stable/defensive model into a three-state XAUT-specific controller.

### Monitor/Web

Expose these fields in summary output:

- `xaut_adaptive_enabled`
- `xaut_adaptive_direction`
- `xaut_adaptive_state`
- `xaut_adaptive_candidate_state`
- `xaut_adaptive_pending_count`
- `xaut_adaptive_reason`
- `xaut_adaptive_metrics`

Web/monitor should clearly show when `reduce_only` is active and that same-direction opening orders are being suppressed.

## Failure Handling

If regime metrics cannot be computed:

- keep the previously active state
- record a warning in the loop summary
- do not switch state based on missing data

If the user somehow selects an XAUT adaptive profile on a symbol other than `XAUTUSDT`:

- fail startup with a clear configuration error

If stale-order cancellation is disabled:

- `reduce_only` should still remove opening orders from the generated plan
- but preset defaults should keep `cancel_stale = true`
- monitor output should indicate that existing opening orders may remain on-book until manually cleared

## Testing

Add tests for:

- long state classification for normal/defensive/reduce_only
- short state classification for normal/defensive/reduce_only
- confirmation-count transition behavior
- forced `normal -> reduce_only`
- forced `reduce_only -> defensive`
- no direct `reduce_only -> normal`
- long `reduce_only` strips `buy_orders` and `bootstrap_orders`
- short `reduce_only` strips `sell_orders` and `bootstrap_orders`
- preset wiring and symbol guard
- monitor/web summary includes the new state fields

## Risks

- overfitting thresholds to a narrow sample window
- state flapping if recovery thresholds are too loose
- stale-order cancellation latency could leave same-direction orders live briefly after entering `reduce_only`

## Non-Goals

- generic adaptive framework across all symbols
- dynamic threshold re-estimation at runtime
- auto-stop or auto-flatten behavior
