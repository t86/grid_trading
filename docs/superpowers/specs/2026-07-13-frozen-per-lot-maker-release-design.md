# Frozen Per-Lot Maker Release Design

## Goal

Release profitable frozen inventory continuously without allowing an expensive lot to block cheaper profitable lots.

## Release semantics

- Evaluate each frozen lot independently.
- A frozen long lot is eligible when the current best bid is at least `entry_price * (1 + min_profit_ratio)`.
- A frozen short lot is eligible when the current best ask is at most `entry_price * (1 - min_profit_ratio)`.
- Select eligible lots in ledger order up to the existing configured release batch notional (20 USDT for ARX).
- Keep non-eligible lots frozen with their original entry metadata.
- Preserve the existing one-USDT residual only when releasing every remaining lot on that side; an ineligible residual already satisfies the inventory-retention intent.

## Execution semantics

- Long release: post a reduce-only SELL at the current best ask.
- Short release: post a reduce-only BUY at the current best bid.
- Use GTX/post-only. Never cross the spread and never fall back automatically to IOC/taker execution.
- Maintain at most one active frozen single-leg release order per side. The normal maker reconciliation may cancel and replace it as the best quote changes.
- After a fill, immediately evaluate the remaining eligible inventory so release can continue batch by batch.

## Ledger correctness

- Each release directive records the selected lot allocations, not only an aggregate quantity.
- Partial fills consume only the selected allocations and only up to the filled quantity.
- Unselected lots, including higher-cost lots, must not be consumed by FIFO fallback.
- Restart/reconciliation must retain enough order metadata to apply late fills to the selected allocations.
- An unconsumed client binding blocks the next batch on that side until REST order state and `userTrades` confirm the terminal outcome. Filled quantity is backfilled before the binding clears; zero-fill cancellation and never-created POST reservations clear without consuming a lot.

## Safety and compatibility

- Pair release stays disabled for ARX. If it is enabled later, it will share the same configured 1% profit threshold.
- The configured minimum profit ratio is `0.01` for ARX.
- The configured per-release notional remains the existing 20 USDT.
- If no eligible lot exists, no single-leg release order is armed.
- Existing frozen total caps and managed-position soft/hard thresholds are unchanged.

## Acceptance checks

1. A profitable low-cost lot is released while a high-cost lot remains frozen.
2. No selected batch exceeds the configured notional after quantity-step rounding.
3. Generated release orders are best-quote GTX maker orders and never IOC.
4. Partial fills reduce only the selected lot allocations.
5. A completed batch immediately permits the next eligible batch to be armed.
6. A submitted but not-yet-consumed batch cannot arm the same lot again.
7. Existing pair-release and non-frozen runner tests remain green.
