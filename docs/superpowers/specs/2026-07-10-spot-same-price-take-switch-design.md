# Spot Same-Price TAKE Switch Design

## Goal

Allow a synthetic-neutral spot competition runner to remain fully maker-only by disabling the automatic same-price `LIMIT + IOC` sell after a maker buy fill.

## Design

Add `spot_same_price_take_exit_enabled` as an independent boolean control. It defaults to `true` so existing synthetic-neutral runners keep their current behavior. A saved runner config can set it to `false`; the command builder must then pass an explicit negative CLI flag so the runner cannot fall back to its default.

When disabled, `_run_cycle` must not fetch a second book snapshot for this path and must not submit an IOC order. The cycle summary reports `enabled=false` and `reason=disabled`. Ordinary maker grid planning, bottom-inventory restoration, the fixed futures hedge, and `spot_taker_exit_enabled` remain unchanged.

## Alternatives Rejected

- Switching to `spot_competition_inventory_grid` would lose synthetic-neutral bottom-inventory semantics.
- Removing same-price IOC globally would silently alter existing runners that intentionally use it.

## Verification

- Parser test covers the explicit negative flag.
- Cycle test proves a qualifying maker buy fill submits no IOC while disabled.
- Saved-runner command test proves `false` becomes `--no-spot-same-price-take-exit-enabled`.
- Existing enabled-path test continues to prove the backward-compatible default.
