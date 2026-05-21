# Central Strategy Config Workbench Design

## Goal

Build a 110-hosted central strategy configuration workbench that can configure, validate, save, and apply strategy profiles on 114 and 150 without changing strategy algorithms.

The first target profile is:

- `PHAROSUSDT`
- `pharosusdt_hedge_best_quote_maker_volume_v1`
- `hedge_best_quote_maker_volume_v1`
- target servers: `114`, `150`

The core isolation rule is:

> Parameter types can be common; parameter scope must not be common.

In Chinese product language:

> 参数类型通用，生效范围不通用。

Changing a common execution parameter in the workbench must only affect the selected `server_id + symbol + strategy_profile` target. It must not change other symbols, other profiles, or the same symbol on another server unless the user explicitly requests synchronization.

## Non-Goals

This design does not:

- change PHAROS strategy planning or order-generation logic
- change current live runner behavior automatically
- enable strict schema on PHAROS before the profile overlay is clean
- introduce one shared global config for all strategies
- copy files from local to production servers
- replace the existing server-side saved-runner wrappers
- remove historical PHAROS backup configs

## Current Context

On 2026-05-21, both 114 and 150 are running:

```text
symbol=PHAROSUSDT
strategy_profile=pharosusdt_hedge_best_quote_maker_volume_v1
strategy_mode=hedge_best_quote_maker_volume_v1
```

Both use profile-specific runtime artifacts:

114:

```text
/home/ubuntu/wangge/output/pharosusdt_hedge_bq_state.json
/home/ubuntu/wangge/output/pharosusdt_hedge_bq_latest_plan.json
/home/ubuntu/wangge/output/pharosusdt_hedge_bq_latest_submit.json
/home/ubuntu/wangge/output/pharosusdt_hedge_bq_events.jsonl
```

150:

```text
/home/ubuntu/wangge_api2/output/pharosusdt_hedge_bq_state.json
/home/ubuntu/wangge_api2/output/pharosusdt_hedge_bq_latest_plan.json
/home/ubuntu/wangge_api2/output/pharosusdt_hedge_bq_latest_submit.json
/home/ubuntu/wangge_api2/output/pharosusdt_hedge_bq_events.jsonl
```

The current live control files are still symbol-level:

114:

```text
/home/ubuntu/wangge/output/pharosusdt_loop_runner_control.json
```

150:

```text
/home/ubuntu/wangge_api2/output/pharosusdt_loop_runner_control.json
```

The current local profile schema incorrectly classifies `pharosusdt_hedge_best_quote_maker_volume_v1` as one-way `best_quote`, because it matches the suffix `_best_quote_maker_volume_v1`. This must be fixed before enabling strict schema for PHAROS.

## Isolation Contract

Every saved config and apply action must be scoped by:

```text
server_id + symbol + strategy_profile
```

Examples:

- Editing `114 / PHAROSUSDT / pharosusdt_hedge_best_quote_maker_volume_v1` only affects that target.
- Editing `150 / PHAROSUSDT / pharosusdt_hedge_best_quote_maker_volume_v1` only affects 150.
- Editing `114 / AIGENSYNUSDT / aigensynusdt_best_quote_maker_volume_v1` does not affect PHAROS.
- A common parameter such as `max_new_orders` is common by type, but it is saved into only the selected target config.

The workbench must not write a global value such as "all strategies use `max_new_orders=5`".

## UI Structure

The central workbench should be hosted on 110 and support selecting:

```text
Server: 114 / 150
Symbol: PHAROSUSDT
Profile: pharosusdt_hedge_best_quote_maker_volume_v1
```

The page should show four sections.

### 1. Target Selector

Fields:

- `server_id`
- `symbol`
- `strategy_profile`
- current runner status
- current server-side git/head summary
- current profile schema status

The selected target controls every read, save, preflight, and apply action.

### 2. Common Execution Parameters

These are common by type but scoped to the selected target:

- `symbol`
- `strategy_profile`
- `strategy_mode`
- `required_position_mode`
- `margin_type`
- `leverage`
- `sleep_seconds`
- `max_plan_age_seconds`
- `max_mid_drift_steps`
- `max_new_orders`
- `max_total_notional`
- `rolling_hourly_loss_limit`
- `max_cumulative_notional`
- `max_actual_net_notional`
- `state_path`
- `plan_json`
- `submit_report_json`
- `summary_jsonl`

This section should visually label these as execution/runtime parameters, not strategy-owned logic.

### 3. Strategy-Owned Parameters

For PHAROS hedge BQ, this section should be driven by a profile overlay and should include only PHAROS-owned or PHAROS-authorized parameters:

- `best_quote_maker_volume_enabled`
- `best_quote_maker_volume_cycle_budget_notional`
- `best_quote_maker_volume_target_remaining_notional`
- `best_quote_maker_volume_quote_offset_ticks`
- `best_quote_maker_volume_defensive_offset_ticks`
- `best_quote_maker_volume_max_long_notional`
- `best_quote_maker_volume_max_short_notional`
- `best_quote_maker_volume_inventory_soft_ratio`
- `best_quote_maker_volume_inventory_bias_*`
- `best_quote_maker_volume_dynamic_control_*`
- `hard_loss_forced_reduce_*`
- `loss_reentry_guard_*`
- `volatility_entry_pause_*`
- `anti_chase_entry_guard_*`
- PHAROS hedge long/short inventory thresholds

The section should not show unrelated strategy modules unless they are explicitly active as forbidden/unknown diagnostics.

Examples of parameters that should not be profile-owned by PHAROS hedge BQ:

- `volume_long_v4_flow_sleeve_*`
- `synthetic_flow_sleeve_*`
- `synthetic_trend_follow_*`
- `custom_grid_*`
- unrelated one-way-only profile controls

### 4. Preflight And Apply

Before saving or applying, the page should show:

- profile overlay known or unknown
- required position mode
- active strategy-owned params
- active common execution params
- active global safety valves
- forbidden active params
- unknown params
- required missing params
- estimated per-cycle order count and notional
- whether `max_new_orders` or `max_total_notional` will cap the strategy
- whether cumulative notional / loss / net exposure limits may stop the run

Actions:

- `Refresh`: read current target state
- `Load Profile Defaults`: load clean profile defaults for the selected target
- `Validate`: run preflight only
- `Save`: write only the selected target config
- `Save And Apply`: write selected target config, then call the selected target's saved-runner wrapper
- `Sync To Other Server`: explicit cross-server copy after preflight

No action should implicitly apply to both 114 and 150.

## Data Model

The workbench should use a strategy catalog entry for each selectable profile:

```python
StrategyCatalogEntry(
    server_ids={"114", "150"},
    symbol="PHAROSUSDT",
    profile="pharosusdt_hedge_best_quote_maker_volume_v1",
    mode="hedge_best_quote_maker_volume_v1",
    required_position_mode="hedge",
    common_params={
        "symbol",
        "strategy_profile",
        "strategy_mode",
        "required_position_mode",
        "margin_type",
        "leverage",
        "sleep_seconds",
        "max_new_orders",
        "max_total_notional",
    },
    profile_params={
        "best_quote_maker_volume_enabled",
        "best_quote_maker_volume_cycle_budget_notional",
        "best_quote_maker_volume_target_remaining_notional",
        "best_quote_maker_volume_quote_offset_ticks",
        "best_quote_maker_volume_defensive_offset_ticks",
        "best_quote_maker_volume_max_long_notional",
        "best_quote_maker_volume_max_short_notional",
    },
    global_safety_params={
        "max_mid_drift_steps",
        "rolling_hourly_loss_limit",
        "max_cumulative_notional",
        "max_actual_net_notional",
    },
    forbidden_params={
        "synthetic_flow_sleeve_enabled",
        "synthetic_trend_follow_enabled",
        "volume_long_v4_flow_sleeve_enabled",
        "custom_grid_enabled",
    },
    runtime_paths_by_server={
        "114": {
            "state_path": "/home/ubuntu/wangge/output/pharosusdt_hedge_bq_state.json",
            "plan_json": "/home/ubuntu/wangge/output/pharosusdt_hedge_bq_latest_plan.json",
        },
        "150": {
            "state_path": "/home/ubuntu/wangge_api2/output/pharosusdt_hedge_bq_state.json",
            "plan_json": "/home/ubuntu/wangge_api2/output/pharosusdt_hedge_bq_latest_plan.json",
        },
    },
    apply_wrapper_by_server={
        "114": "/usr/local/bin/grid-saved-runner restart PHAROSUSDT",
        "150": "/usr/local/bin/grid-saved-runner-api2 restart PHAROSUSDT",
    },
)
```

The catalog entry should be separate from strategy implementation code. It describes how to configure and run an existing strategy.

## Config Composition

For every target, the workbench should build config in this order:

```text
common defaults
+ profile defaults
+ server-specific runtime paths
+ user overrides from UI
```

After composition, the schema layer must strip or report anything outside the selected profile boundary.

The resulting config is written only to the selected target's control file.

## Server-Side Apply

110 should not copy code or runtime files to 114/150.

Apply should use existing server-local wrappers:

114:

```bash
/usr/local/bin/grid-saved-runner restart PHAROSUSDT
```

150:

```bash
/usr/local/bin/grid-saved-runner-api2 restart PHAROSUSDT
```

The workbench may call these wrappers through SSH from 110 or expose a small authenticated endpoint on each target server. The first implementation should prefer the existing wrapper path and avoid introducing a new remote execution daemon.

## PHAROS Schema Changes Required

Before enabling strict schema for PHAROS, add:

1. A new schema family:

```text
hedge_best_quote_maker_volume
```

2. Resolver precedence:

```text
if strategy_mode == hedge_best_quote_maker_volume_v1
or profile contains hedge_best_quote_maker_volume
then required_position_mode=hedge
```

This must run before the generic `_best_quote_maker_volume_v1` suffix rule.

3. A parameter set:

```text
BEST_QUOTE_MAKER_VOLUME_PARAMS
```

It should include all `best_quote_maker_volume_*` parameters used by PHAROS.

4. A PHAROS overlay:

```text
pharosusdt_hedge_best_quote_maker_volume_v1
```

5. Report-only rollout:

The first version should keep startup behavior unchanged and only show diagnostics. Strict startup blocking for PHAROS should be enabled after unknown/forbidden diagnostics are clean.

## Safety Rules

- Save cannot write keys outside the selected profile schema unless they are known common/global safety params.
- Save must preserve only current target config, never all server configs.
- Apply must run only for the selected target.
- Sync to another server must require explicit user action.
- Strict mode must not be enabled automatically for a profile whose overlay is unknown.
- Position mode mismatch must be visible before apply.
- A profile requiring hedge mode must not be presented as one-way.

## Rollout Plan

### Phase 1: Read-Only Central View

- Add 114/150 target selector on 110.
- Read current PHAROS configs and latest plan files.
- Show common/profile/global/unknown parameter sections.
- No saving or applying.

### Phase 2: PHAROS Overlay

- Add `hedge_best_quote_maker_volume` schema family.
- Add PHAROS overlay.
- Add tests using current 114/150 PHAROS config samples.
- Confirm `best_quote_maker_volume_*` are no longer unknown.
- Confirm required mode is `hedge`.

### Phase 3: Save Current Target Only

- Save edited config for one selected target.
- Preserve explicit target scope in UI and API.
- Add preflight before save.

### Phase 4: Save And Apply

- Add apply wrapper calls for 114 and 150.
- Show command/result output.
- Verify latest runner process after apply.

### Phase 5: Optional Cross-Server Sync

- Add explicit "Sync 114 -> 150" or "Sync 150 -> 114".
- Show diff before sync.
- Require preflight on destination before write/apply.

## Acceptance Criteria

The design is implemented correctly when:

- PHAROS appears in the 110 strategy list for both 114 and 150.
- Editing common execution params for 114 PHAROS does not change 150 or any other symbol.
- Editing PHAROS strategy-owned params does not expose unrelated strategy modules as editable fields.
- PHAROS schema resolves to hedge mode, not one-way best quote.
- Current PHAROS `best_quote_maker_volume_*` params are recognized by schema.
- Forbidden/unknown params are visible before save/apply.
- Save writes only the selected target config.
- Apply restarts only the selected target runner via the correct wrapper.
- No strategy algorithm code is changed.
