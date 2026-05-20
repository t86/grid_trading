# Profile Boundary Overlay Design

## Goal

Make strategy parameter isolation explicit at the profile level, not only at the strategy-family level.

The first version is report-only. It must not change order planning, order submission, runner start/stop behavior, or saved config writes. Its job is to make the strategy editor and diagnostics show exactly which parameters are:

- allowed for the selected profile
- active and allowed
- ignored because they belong to another profile/family
- explicitly forbidden for the selected profile
- required but missing
- global safety valves rather than strategy-owned knobs

This closes the remaining ambiguity where a strategy family may allow a broad group of parameters, while a specific competition profile should be much cleaner.

## Background

The current schema layer already has family-level isolation:

- `best_quote`
- `one_way_long`
- `synthetic_ping_pong`
- `hedge_bq_ping_pong`
- `unknown`

It outputs:

- `allowed_params`
- `ignored_params`
- `unknown_params`
- `allowed_runtime_switches`
- position mode metadata
- startup and global safety preflight reports

This is useful, but still too broad. For example, two `best_quote` style profiles may share family-level parameters while one profile should not allow elastic repair, adverse reduce, inventory tiers, or synthetic-only switches. The user needs a profile-level view before changing configs or switching strategies.

## Non-Goals

This design does not:

- block saving config from the strategy editor
- change runner planning or submit behavior
- delete existing config keys automatically
- migrate all historical strategies at once
- rewrite `RUNNER_STRATEGY_PRESETS`
- create git branches per strategy
- add historical PnL or wear analytics

Strict enforcement can be added later after the report-only layer is trusted.

## Selected Approach

Add a profile overlay registry on top of the existing family schema.

Family schema remains the baseline because it already prevents large cross-strategy leaks. Profile overlays narrow or annotate that baseline for important profiles.

An overlay can define:

```python
ProfileBoundaryOverlay(
    key="aigensynusdt_best_quote_maker_volume_v1",
    label="AIGENSYNUSDT Best Quote 冲量",
    allowed_params=frozenset({...}),
    forbidden_params=frozenset({...}),
    required_params=frozenset({...}),
    global_safety_params=frozenset({...}),
    runtime_switches=frozenset({...}),
)
```

The schema report should include a new `profile_boundary` object:

```json
{
  "profile_key": "aigensynusdt_best_quote_maker_volume_v1",
  "overlay_known": true,
  "status": "warning",
  "allowed_params": [],
  "active_allowed_params": [],
  "global_safety_params": [],
  "active_global_safety_params": [],
  "forbidden_params": [],
  "forbidden_active_params": [],
  "required_params": [],
  "required_missing_params": [],
  "ignored_params": [],
  "unknown_params": [],
  "messages": []
}
```

Existing top-level fields remain for compatibility:

- `allowed_params`
- `ignored_params`
- `unknown_params`

`profile_boundary` is the more detailed layer used by diagnostics and the editor.

## Initial Profile Coverage

The first implementation should cover these profile keys:

- `aigensynusdt_best_quote_maker_volume_v1`
- `aigensynusdt_hedge_bq_pingpong_sprint_v1`
- `volume_long_v4`
- `volume_neutral_ping_pong_v1`
- `competition_inventory_grid_v1`

Unknown overlays should be allowed. They should report:

```json
{
  "overlay_known": false,
  "status": "warning"
}
```

The schema should still fall back to family-level allowed params.

## Boundary Semantics

### Allowed Params

`allowed_params` means the selected profile can use this parameter.

For profile overlays, this should be narrower than the family list where practical. Common runtime/file parameters remain allowed because all runners need them.

### Active Allowed Params

`active_allowed_params` includes allowed params whose current value is active or meaningful.

Examples:

- positive numbers
- non-empty strings
- `True` booleans
- non-empty lists/dicts

Inactive defaults should not clutter the primary diagnostics.

### Global Safety Params

Global safety params are allowed but not strategy-owned. They may stop, cap, or take over execution.

Initial list:

- `max_new_orders`
- `max_total_notional`
- `cancel_stale`
- `max_mid_drift_steps`
- `rolling_hourly_loss_limit`
- `max_cumulative_notional`
- `max_actual_net_notional`
- `max_synthetic_drift_notional`

They should be displayed separately so users do not confuse them with strategy behavior.

### Forbidden Params

`forbidden_params` means the parameter should not be active for this profile even if some broader family might know it.

Examples:

- synthetic-only flow sleeves on AIGENSYN one-way best quote
- hedge-only controls on one-way profiles
- one-way inventory repair controls on hedge-native profile when they do not apply
- adverse/forced reduce switches on profiles that should not let those modules take over

The first version reports `forbidden_active_params`. It does not block saving.

### Required Params

`required_params` are the minimal fields needed to understand and safely run the profile.

For volume profiles this normally includes:

- `strategy_profile`
- `strategy_mode`
- `required_position_mode`
- `buy_levels`
- `sell_levels`
- `per_order_notional`
- `max_new_orders`
- `max_total_notional`
- `cancel_stale`

If a required param is absent or empty, report it in `required_missing_params`.

## Diagnostics UI

`strategy_diagnostics.profile_boundary` should show:

- overlay known or unknown
- boundary status
- active allowed params count
- active global safety params
- forbidden active params
- required missing params
- ignored and unknown params

For each issue item, show:

- title
- current value when useful
- why it matters
- impact
- suggestion
- tradeoff

The page remains manual-refresh only.

## Status Rules

Profile boundary status:

- `blocked` when strict schema has unknown active params or required params are missing
- `warning` when forbidden active params, ignored params, unknown overlay, or defaulted position mode exist
- `ready` when none of the above are present

The first implementation may keep `startup_preflight.can_start` unchanged. It should not introduce new blocking behavior outside the report.

## Testing

Add tests before implementation:

1. Known overlay reports `overlay_known=true`.
2. AIGENSYN best quote reports active allowed params and active global safety params separately.
3. AIGENSYN best quote reports synthetic-only active params as forbidden or ignored.
4. Required missing params are reported.
5. Unknown overlay falls back to family-level schema and warns.
6. `strategy_diagnostics.profile_boundary` consumes the detailed boundary report.
7. `/api/strategy_editor/status` includes `profile_boundary`.
8. `/strategy_editor` renders profile boundary counts without auto-refresh.

## Rollout

Step 1: Add overlay dataclass and registry.

Step 2: Build `profile_boundary` report in `apply_strategy_profile_schema`.

Step 3: Extend `strategy_diagnostics` profile boundary section to use the new report.

Step 4: Surface the new report through the strategy editor status API.

Step 5: Render compact boundary counts/details in `/strategy_editor`.

Step 6: Verify locally and deploy to 110.

## Acceptance Criteria

The work is acceptable when:

- important profiles have explicit overlays
- diagnostics distinguish strategy-owned params from global safety valves
- active forbidden params are visible
- missing required params are visible
- ignored and unknown params remain visible
- existing strict schema behavior remains backward compatible
- strategy editor remains manual-refresh only
- tests cover schema, diagnostics, API, and page rendering

