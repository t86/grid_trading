# GRAM Synthetic-Neutral Tail Cleanup Two-Sided Design

## Objective

Keep the GRAM spot competition strategy two-sided while a small synthetic-neutral
inventory residual is being cleaned up. The current planner returns immediately
after creating a `tail_cleanup` order, so the opposite maker entry disappears
whenever the residual is smaller than one configured order.

This change targets `spot_competition_synthetic_neutral_grid` only. It does not
enable pure MARKET TAKE volume, change the same-price `LIMIT + IOC` exit rules,
or alter futures hedge positions.

## Confirmed Production Evidence

- Both 114 and 150 entered `short_active` with about 32 GRAM of residual inventory.
- With `per_order_notional=80`, the residual was smaller than one order, so the
  planner emitted only a BUY `tail_cleanup` order and zero SELL orders.
- Offline planning with the live state reproduced the same single-order output.
- Once the cleanup completed and inventory returned to flat, both hosts restored
  the expected 3 BUY / 3 SELL grid.

## Selected Behavior

When all of the following are true:

- the runtime is `synthetic_neutral`;
- `risk_state` is `normal`;
- the active residual is smaller than one order and qualifies for `tail_cleanup`;

the planner will:

1. Keep exactly one reduce-side `tail_cleanup` order for the full residual.
2. Skip any additional reduce-side grid-exit order for that same residual.
3. Continue planning the opposite-side maker entry orders within existing
   position and notional caps.

Examples:

- Small short residual: keep BUY `tail_cleanup`, also allow SELL `grid_entry`.
- Small long residual: keep SELL `tail_cleanup`, also allow BUY `grid_entry`.

The existing early return remains unchanged when:

- the strategy is not synthetic-neutral;
- risk is `threshold_reduce_only` or `hard_reduce_only`;
- the cleanup order does not meet exchange minimums.

`spot_base_restore_only` remains authoritative downstream: when explicitly
enabled, it will still remove any order that moves spot farther from the neutral
base. The same-price IOC path also continues to reject exits while base restore
is active.

## Alternatives Considered

### Parameter-only tuning

Lowering `per_order_notional` can make the residual exceed one order, but every
later partial fill can recreate a smaller tail. This does not remove the
structural one-sided state.

### Disable tail cleanup

This would preserve entries but leave residual inventory unmanaged and weaken
the neutral-base invariant.

### Selected: branch-aware continuation

Preserve the cleanup order and continue only into the opposite entry branch.
This is the smallest change that restores volume without double-closing the
residual or weakening risk states.

## Test Plan

Add focused planner tests that prove:

1. A small synthetic short produces one BUY `tail_cleanup` and a SELL entry.
2. A small synthetic long produces one SELL `tail_cleanup` and a BUY entry.
3. The residual is not covered twice by cleanup plus grid-exit orders.
4. Threshold/hard-risk and non-synthetic behavior keep the existing early return.

Run the focused inventory-grid tests and the spot-loop regression tests with the
verified local command:

```bash
PYTHONPATH=src PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 /opt/anaconda3/bin/python3 -m pytest <targets>
```

The existing stale test
`test_synthetic_neutral_flat_defaults_to_one_tick_away_from_book` is a known
baseline mismatch from the previously deployed best-bid change. It must be
updated separately to expect the current best-bid behavior; it is not evidence
of a tail-cleanup regression.

## Deployment And Verification

Deploy through the pull-based production wrappers to 114 and 150. Verify:

- both repos run the pushed commit;
- GRAM runners and web services are active;
- normal small-residual states show cleanup plus the opposite maker side;
- threshold/hard-risk states remain one-sided;
- same-price IOC remains enabled and pure MARKET TAKE remains disabled;
- exchange `userTrades`, deduplicated by trade ID, shows the post-change volume
  rate without relying on local runner totals.
