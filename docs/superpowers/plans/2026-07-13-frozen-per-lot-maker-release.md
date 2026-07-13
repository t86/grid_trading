# Frozen Per-Lot Maker Release Implementation Plan

## Objective

Change automatic frozen single-leg profit release from whole-side IOC liquidation to continuous, per-lot, 20 USDT maker batches while preserving exact frozen-lot accounting.

## Tasks

1. Add failing tests in `tests/test_loop_runner.py`.
   - Select a profitable lot while another lot on the same side is underwater.
   - Cover configurable long and short thresholds, deployed at 1% for ARX.
   - Cap each automatic batch to 20 USDT.
   - Assert long SELL at ask and short BUY at bid with maker/GTX.
   - Assert partial fills consume only selected lots and a completed batch allows the next batch.

2. Add lot selection and allocation helpers in `src/grid_optimizer/loop_runner.py`.
   - Assign stable IDs to legacy frozen lots when first evaluated.
   - Select only individually eligible lots in ledger order.
   - Trim allocation quantities to the configured batch notional and final one-USDT retention rule.

3. Keep automatic maker directives alive until confirmed fills.
   - Store selected allocations and a per-lot selection marker on the directive.
   - Build best-quote maker/GTX orders and revalidate the selected lots' profit floor each cycle.
   - Leave legacy/manual reduce directives on their existing aggressive IOC path.

4. Persist allocations through submission and synchronize fills precisely.
   - Copy request ID, source, and selected allocations into submit reports and order refs.
   - Consume selected lot IDs for partial fills; never fall back to FIFO for marked per-lot releases.
   - Reduce or clear the persistent directive after confirmed fills.
   - Block a new side batch while any submitted allocation remains unconsumed; reconcile terminal orders from REST order state plus `userTrades`, including full fill, partial-fill cancellation, zero-fill cancellation, and a never-created POST reservation.

5. Wire the existing frozen release max-notional into automatic arming and persist ARX runtime defaults.
   - Pass the existing pair-release max notional (20 USDT) to the single-leg arm call.
   - Ensure both tracked ARX host configs retain enabled=true, min profit 0.01, max batch 20, and pair release disabled.

6. Verify and deploy.
   - Run focused tests first, then the full `tests/test_loop_runner.py` suite with the repository's isolated pytest command.
   - Review the diff for unrelated changes.
   - Commit and push `main`, deploy by each host's tracked update wrapper, run `configure_arx_single_leg_freeze.py` against each live output control file, restart ARX only, and verify commit, live min profit `0.01`, runner health, order type, selected allocation state, and frozen totals.
