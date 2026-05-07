# General Multi-Timeframe Bias Design

## Goal

Make the current `1m / 15m / 1h / 4h` real-time direction layer reusable across futures contract runners. The feature should keep the CHIP behavior that is already useful, but remove the `synthetic_neutral`-only limitation and adapt the same direction signal safely for other futures strategy modes.

The target modes are:

- `synthetic_neutral`
- `one_way_long`
- `one_way_short`
- `competition_inventory_grid`

Spot runners are out of scope for this design.

## Current State

`src/grid_optimizer/multi_timeframe_bias.py` already contains a pure signal module:

- It reads the latest closed `1m`, `15m`, `1h`, and `4h` candle windows.
- It scores zone, trend, long bias, short bias, and 1m shock state.
- It can adjust levels, step price, per-order notional, position caps, and buy/sell offsets.

`loop_runner.py` wires the module into futures plan generation, plan reports, event summaries, parser flags, validation, and saved-runner command building. The current validation still requires `--strategy-mode synthetic_neutral` when the layer is enabled.

## Design

Keep the multi-timeframe signal as one shared source of truth, then add a strategy-mode adapter layer.

The signal phase stays strategy-neutral:

- Fetch closed `1m`, `15m`, `1h`, and `4h` windows when enabled.
- Compute `zone_score`, `trend_score`, `long_bias_score`, `short_bias_score`, `direction_score`, and `shock_active`.
- Return a full report even when disabled or unavailable.

The adapter phase maps the signal to each mode:

- `synthetic_neutral`: keep current behavior. Low zones favor buy-long entries, high zones favor sell-short entries, and both side caps can be scaled in opposite directions.
- `one_way_long`: low zones may increase buy levels, per-order notional, and long cap. High zones reduce entry aggression, move buy entries farther away, and leave take-profit/delever behavior intact. This mode must not create short exposure.
- `one_way_short`: high zones may increase sell-short levels, per-order notional, and short cap. Low zones reduce entry aggression and move sell entries farther away. This mode must not create long exposure.
- `competition_inventory_grid`: adjust buy/sell density, offsets, and inventory limits without changing the strategy's inventory-grid identity. The adapter should be conservative so the grid remains balanced enough for competition-style volume.

Add a new CLI/config field:

- `multi_timeframe_bias_mode_adapter`
- Default: `auto`
- Allowed values: `auto`, `synthetic_neutral`, `one_way_long`, `one_way_short`, `inventory_grid`

`auto` resolves from `strategy_mode`. Explicit values are for debugging or staged production rollout.

## Safety Rules

- Default remains disabled.
- Existing saved configs behave the same unless `multi_timeframe_bias_enabled=true`.
- Shock state may only reduce intensity or widen step. It must not increase notional, levels, or position caps.
- One-way adapters must never create opposite-side exposure.
- If candles are unavailable or invalid, the adapter returns base values with `applied=false`.
- Keep existing loss guards, volatility guards, take-profit guards, adverse-reduce, and hard-loss reduce behavior authoritative after this layer.
- Validate adapter/mode compatibility and fail fast for unsupported combinations.

## Data Flow

1. Parse runner config and resolve the effective strategy mode.
2. Build `MultiTimeframeBiasConfig`, including the adapter mode.
3. Fetch closed `1m / 15m / 1h / 4h` futures K-lines only when enabled.
4. Resolve the shared signal report.
5. Resolve the adapter from `auto` or explicit config.
6. Apply mode-specific adjustments to the effective planning parameters before plan generation.
7. Include the signal report, adapter name, and adjustment report in plan JSON and event JSONL.

## Reporting

Plan and event summaries should expose:

- `multi_timeframe_bias.enabled`
- `multi_timeframe_bias.available`
- `multi_timeframe_bias.applied`
- `multi_timeframe_bias.adapter`
- `multi_timeframe_bias.regime`
- `multi_timeframe_bias.zone_score`
- `multi_timeframe_bias.long_bias_score`
- `multi_timeframe_bias.short_bias_score`
- `multi_timeframe_bias.direction_score`
- `multi_timeframe_bias.shock_active`
- `multi_timeframe_bias.adjustments`

The existing flattened event fields should remain for dashboards and scripts.

## Testing

Pure module tests should cover:

- Low-zone long bias in `synthetic_neutral`.
- High-zone short bias in `synthetic_neutral`.
- Low-zone acceleration and high-zone de-risking in `one_way_long`.
- High-zone acceleration and low-zone de-risking in `one_way_short`.
- Inventory-grid adapter keeps both sides present and applies conservative offsets.
- Shock state reduces notional or widens step without increasing risk.
- Unavailable candles return base values and `applied=false`.

Runner tests should cover:

- Parser accepts the new adapter flag.
- Validation rejects unsupported adapter values or incompatible explicit adapters.
- `one_way_long` can enable multi-timeframe bias without the old synthetic-only failure.
- `one_way_short` can enable multi-timeframe bias.
- Saved-runner command includes the adapter argument.
- Plan/event summaries include adapter and adjustments.

## Rollout

1. Ship code with defaults disabled.
2. Keep CHIP on `synthetic_neutral` adapter and verify no behavior regression.
3. Enable one low-risk `one_way_long` symbol with conservative scales.
4. Enable one `one_way_short` symbol with conservative scales.
5. Enable one `competition_inventory_grid` symbol after 30 to 60 minutes of stable event reports in the one-way modes.

Production rollout should continue to use pull-based deploys and saved-runner restarts. No server-side file copying or hot patches.
