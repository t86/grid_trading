# Forced Threshold Reduce (Futures) - Design

## Goal
When position notional exceeds configured threshold(s), the strategy must actively reduce the position instead of only pausing. The reduction must be deterministic, parameter-aligned, and safe to re-run each loop.

## Scope
- Applies to futures runners using threshold controls (long and/or short).
- Applies to all modes where thresholds are active.
- Does not change other entry/exit logic beyond the forced reduction path.

## Requirements
- If `current_long_notional > pause_long_position_notional`, force reduce long to `pause_long_position_notional * 0.8`.
- If `current_short_notional > pause_short_position_notional`, force reduce short to `pause_short_position_notional * 0.8`.
- A single reduce-only order per side per cycle (one-shot to the target).
- Use last price to determine order size and to evaluate tick ratio.
- Pricing rule:
  - Compute `tick_ratio = tick_size / last_price`.
  - If `tick_ratio > 0.0003`, place a taker order (at current price).
  - Else, place a post-only maker order at best bid/ask; if not filled, refresh each cycle.
- If calculated reduce quantity is below exchange min size, skip with a clear log.
- If last price is unavailable or invalid, skip with a clear log.
- Forced reduce must be marked reduce-only and tagged as a dedicated role for visibility.

## Approach Options
1. **Inline in existing threshold pause handler (recommended).**
   - Extend the existing pause logic to emit a forced reduce order before returning.
   - Minimal surface area, keeps threshold logic centralized.
2. **New guard module to emit reduce orders.**
   - Separate component called from loop runner.
   - Cleaner separation but introduces a new path and more wiring.
3. **Plan-level reduction (modify plan generator).**
   - Inject forced reduce into the plan output.
   - Riskier: entangles with other plan logic and tests.

Recommendation: Option 1 for lowest risk and alignment with current threshold control flow.

## Design

### Data Flow
1. Loop runner computes current notional for long/short.
2. Threshold checks run:
   - If exceeded, compute target notional = threshold * 0.8.
   - Compute reduce notional = current - target.
   - Convert to quantity using last price and round to step size.
3. Build a single order with:
   - `reduce_only = True`
   - `role = "forced_reduce"`
   - side determined by direction (sell to reduce long, buy to reduce short)
4. Apply pricing rule (taker vs post-only maker).
5. Emit order and pause new entries on that side for the cycle.

### Components
- **Threshold handler** in loop runner: calculates forced reduction, returns/attaches order(s).
- **Order builder**: reuses existing order creation helpers to ensure compliance with exchange rules.
- **Telemetry/logging**: add summary fields for forced reduce orders, and log when skipping.

### Error Handling
- If `last_price <= 0`, skip reduction with log.
- If `reduce_qty < min_qty`, skip with log.
- If best bid/ask unavailable and maker path chosen, fallback to taker at last price.

### Testing
- Unit tests for:
  - reduce order creation when notional exceeds thresholds.
  - correct target notional (80% of threshold).
  - taker vs maker decision based on tick ratio.
  - reduce_only flag and role tagging.
  - skipping on invalid price or min qty.

## Open Questions
None. Behavior and thresholds confirmed by user.
