# Position Mode Profile Isolation Design

## Goal

Support both Binance Futures position modes in one codebase without letting strategy behavior leak across modes.

The system should support:

- existing one-way profiles
- new hedge-mode native profiles
- clear profile-level mode requirements
- startup blocking when the account mode does not match the selected profile
- clean order semantics for both modes
- UI visibility into account mode and profile compatibility

The key rule is:

```text
One repository supports both modes.
Each strategy profile requires exactly one mode.
The runner never silently adapts a profile to the other mode.
The operational default is one-way mode unless a profile explicitly opts into hedge mode.
```

## Background

Binance USD-M Futures supports One-way Mode and Hedge Mode. The API exposes the current mode through `positionSide/dual`.

Mode differences that affect strategy code:

- One-way Mode uses `positionSide=BOTH` by default.
- Hedge Mode requires `positionSide=LONG` or `positionSide=SHORT`.
- Hedge Mode cannot send `reduceOnly` on new orders.
- In One-way Mode, buy and sell orders mutate one net position.
- In Hedge Mode, long and short are separate position legs.
- Binance position mode is account-level, not per-symbol in the current endpoint behavior.
- Position mode cannot be changed while there are open positions or open orders.

Current code already has partial support for `hedge_neutral`, and already blocks most one-way strategies when the account is in dual-side mode. That is the right safety direction, but profile isolation should make the rule explicit and visible before a user starts a runner.

## Non-Goals

First version will not:

- automatically switch the Binance account position mode
- convert a live one-way position into hedge legs
- allow a single profile to run in either mode
- run incompatible profiles by rewriting order fields at submit time
- migrate all historical strategies in one step
- change competition board logic or leaderboard collection

Manual account mode switching remains an operational step. The software should verify and explain, not silently mutate account mode.

## Selected Approach

Use profile-level mode isolation.

Because the current production accounts are one-way accounts, the default required mode is `one_way`. Existing legacy profiles that do not yet declare `required_position_mode` are treated as one-way profiles, and the report should mark that the mode was defaulted. Hedge profiles must opt in explicitly with `required_position_mode=hedge`.

New or migrated runner profiles should declare:

```json
{
  "strategy_profile": "aigensyn_hedge_bq_pingpong_sprint_v1",
  "required_position_mode": "hedge",
  "strategy_mode": "hedge_neutral"
}
```

or:

```json
{
  "strategy_profile": "aigensyn_oneway_bq_long_recover_v1",
  "required_position_mode": "one_way",
  "strategy_mode": "one_way_long"
}
```

The runner validates account mode before planning and before submitting:

- `required_position_mode=one_way` requires `dualSidePosition=false`
- `required_position_mode=hedge` requires `dualSidePosition=true`
- missing mode requirement defaults to `one_way` for legacy compatibility and should be reported as defaulted
- unknown mode requirement is invalid when strict profile schema is enabled

The strategy editor and monitor should show the current account mode and label presets as compatible or incompatible.

## Profile Taxonomy

### One-Way Profiles

These remain valid only on one-way accounts:

- `one_way_long` profiles
- `one_way_short` profiles
- `synthetic_neutral` profiles
- `competition_inventory_grid` profiles
- current one-way best-quote long recover profiles

`synthetic_neutral` should be treated as one-way legacy. It exists to emulate long and short books on a single net position. If the account is already in Hedge Mode, hedge-native profiles should replace it.

### Hedge Profiles

These require dual-side accounts:

- existing `hedge_neutral` profiles
- new hedge-native AIGENSYN best-quote ping-pong profiles
- future hedge versions of `ping-pong-fast`, `ping-pong-safe`, and `BQ inventory_recover`

The first new hedge-native competition profile should be:

```text
aigensyn_hedge_bq_pingpong_sprint_v1
```

It should use best bid / best ask style maker orders with separate long and short legs.

Recommended follow-up profiles:

```text
aigensyn_hedge_bq_pingpong_safe_v1
aigensyn_hedge_bq_inventory_recover_v1
```

## Order Intent Model

Strategy planners should express order intent before exchange-specific fields are attached.

Canonical intents:

```text
open_long
close_long
open_short
close_short
```

Intent compilation:

| Intent | One-way Order | Hedge Order |
| --- | --- | --- |
| `open_long` | `BUY`, `positionSide=BOTH`, `reduceOnly=false` | `BUY`, `positionSide=LONG`, no `reduceOnly` |
| `close_long` | `SELL`, `positionSide=BOTH`, `reduceOnly=true` | `SELL`, `positionSide=LONG`, no `reduceOnly` |
| `open_short` | `SELL`, `positionSide=BOTH`, `reduceOnly=false` | `SELL`, `positionSide=SHORT`, no `reduceOnly` |
| `close_short` | `BUY`, `positionSide=BOTH`, `reduceOnly=true` | `BUY`, `positionSide=SHORT`, no `reduceOnly` |

Profile allowlists decide which intents are legal. For example:

- one-way long profiles allow `open_long` and `close_long`
- one-way short profiles allow `open_short` and `close_short`
- one-way synthetic profiles may emit all four intents internally, but they compile through synthetic ledger rules
- hedge profiles may emit all four intents directly

The submit layer should not guess strategy meaning from role strings alone. Role strings can remain for reporting, but mode-sensitive execution should come from normalized intent and profile mode.

## Strategy Behavior

### One-Way Best Quote Recover

This is the conservative long-side best-quote strategy:

- open long at best bid
- close long at best ask when inventory exists
- stop or reduce new buys near soft inventory
- enter repair mode when inventory is high or stale

This profile is useful when the account must remain one-way or when the goal is lower operational complexity.

### Hedge Best Quote Ping-Pong

This is the hedge-native high-volume version:

- open long at best bid using `positionSide=LONG`
- open short at best ask using `positionSide=SHORT`
- close long with `SELL positionSide=LONG`
- close short with `BUY positionSide=SHORT`
- manage long and short inventory independently
- report long-leg state and short-leg state separately

Normal state can place both sides.

When long inventory reaches soft:

- long entries stop or shrink
- long close orders remain
- short entries may continue if short inventory is below its own soft limit

When short inventory reaches soft:

- short entries stop or shrink
- short close orders remain
- long entries may continue if long inventory is below its own soft limit

When either leg reaches hard:

- that leg enters repair state
- same-side entries are blocked
- close orders for that leg are prioritized
- the opposite leg may continue only if gross exposure and margin budget allow it

This should increase volume capacity in deep markets, but gross exposure must be treated as first-class risk.

## Risk Model

Hedge Mode needs both net and gross exposure.

Reports should include:

```json
{
  "position_mode": "hedge",
  "long_notional": 900.0,
  "short_notional": 760.0,
  "net_notional": 140.0,
  "gross_notional": 1660.0,
  "long_state": "normal",
  "short_state": "repair",
  "strategy_intent": "make_volume"
}
```

Required hedge risk limits:

- max long notional
- max short notional
- max gross notional
- max net notional
- long soft ratio
- short soft ratio
- long hard ratio
- short hard ratio
- per-leg repair slice ratio
- optional global rolling loss guard

For competition use, gross notional controls margin usage and liquidation risk more directly than net notional. A hedge profile can look neutral by net exposure while still carrying large long and short legs.

## State Isolation

State and order references must include profile and mode.

Recommended runtime paths:

```text
output/{symbol}_{required_position_mode}_{strategy_profile}_state.json
output/{symbol}_{required_position_mode}_{strategy_profile}_latest_plan.json
output/{symbol}_{required_position_mode}_{strategy_profile}_latest_submit.json
output/{symbol}_{required_position_mode}_{strategy_profile}_summary.jsonl
```

Client order IDs should include a compact mode/profile marker when feasible:

```text
gx-aigensynu-hbq-...
gx-aigensynu-owbq-...
```

Existing older order prefixes should still be detected for cleanup, but a mode migration should require no open strategy orders before start.

## Profile Schema

`strategy_profile_schema.py` should define:

```text
required_position_mode: one_way | hedge
allowed_intents
allowed_runtime_switches
allowed_params
```

Strict schema behavior:

- incompatible parameters are pruned or rejected according to existing strict rules
- missing `required_position_mode` defaults to `one_way` for existing legacy profiles
- hedge profiles must explicitly declare `required_position_mode=hedge`
- new strict profiles should declare `required_position_mode` even when they are one-way
- a one-way profile cannot enable hedge-only fields
- a hedge profile cannot enable synthetic-ledger-only fields
- ignored parameters are reported in the loop summary and strategy editor status

## Runner Flow

Plan generation:

1. Load profile schema.
2. Resolve `required_position_mode`, defaulting missing legacy profiles to `one_way`.
3. Fetch or read cached account position mode.
4. Validate `required_position_mode`.
5. Apply strict profile schema.
6. Build plan using the selected strategy mode.
7. Compile order intents according to profile mode.
8. Write plan report with mode, required mode, defaulted flag, and compatibility status.

Submit flow:

1. Re-fetch current account position mode.
2. Validate it still matches the plan.
3. Validate current positions and open strategy orders still match the plan snapshot.
4. Submit orders with correct mode semantics.
5. Never send `reduceOnly` in Hedge Mode.
6. Never send `positionSide=LONG/SHORT` in One-way Mode unless the exchange requires a future change.

The existing submitter already has some of these checks. The implementation should consolidate the rule around profile mode instead of scattering mode checks by strategy name.

## Strategy Editor UX

The lightweight strategy editor should show:

- current account position mode
- selected profile required mode
- whether the required mode was explicitly declared or defaulted to one-way
- compatibility status
- disabled run/save warnings for incompatible profiles
- per-profile parameter explanations

Preset list behavior:

- one-way presets appear first because the current accounts are one-way by default
- compatible presets appear normally
- incompatible presets are still visible but marked `模式不匹配`
- loading an incompatible preset is allowed for inspection
- starting or applying an incompatible preset is blocked

The editor must not automatically switch account mode.

## Migration Runbook

Before switching an account to Hedge Mode:

1. Stop all runners on that account.
2. Cancel all strategy open orders.
3. Flatten all positions.
4. Confirm there are no open positions and no open orders.
5. Switch account position mode manually or with a dedicated explicit admin command.
6. Start only hedge-required profiles.

Before switching back to One-way Mode, repeat the same cleanout process.

This should be documented in the strategy guide and surfaced in the page as a checklist.

## First Implementation Slice

First slice should not migrate every strategy.

Implement:

1. profile-level `required_position_mode` with legacy default `one_way`
2. runner and submitter compatibility validation
3. strategy editor compatibility display
4. one clean AIGENSYN one-way BQ recover profile as the production-safe default
5. one clean AIGENSYN hedge BQ sprint profile as explicit opt-in
6. tests proving one-way profiles are blocked on hedge accounts and hedge profiles are blocked on one-way accounts

Defer:

- full migration of all synthetic neutral profiles
- automatic account mode switching
- historical data backfill for every strategy
- multi-account mode orchestration

## Testing

Unit tests:

- schema returns required mode for known one-way and hedge profiles
- schema defaults missing legacy mode requirement to one-way and reports it
- strict schema rejects unknown mode requirement
- order intent compiler maps `open_long`, `close_long`, `open_short`, `close_short` correctly
- hedge compiler omits `reduceOnly`
- one-way compiler uses `reduceOnly` for close intents
- runner blocks one-way profile on hedge account
- runner blocks hedge profile on one-way account
- submitter blocks plan/account mode mismatch
- strategy editor status includes current and required mode

Regression tests:

- existing one-way long profiles still produce the same order plan under one-way mode
- existing `hedge_neutral` behavior remains valid under hedge mode
- strict AIGENSYN BQ profile still prunes unrelated synthetic/custom-grid parameters

Manual verification:

- load one-way profile on hedge account and confirm page shows incompatible
- load hedge profile on one-way account and confirm page shows incompatible
- dry-run hedge BQ profile and inspect generated orders for `positionSide=LONG/SHORT`
- test-order hedge BQ orders with tiny notional before live trading

## Success Criteria

The feature is successful when:

- one-way and hedge profiles cannot accidentally run in the wrong account mode
- one-way remains the default for existing accounts and legacy profiles
- strategy editor explains mode compatibility before start
- AIGENSYN has separate one-way recover and hedge sprint profiles
- hedge orders never send `reduceOnly`
- one-way close orders still use reduce-only protection
- state files and order references are separated by profile/mode
- the strategy guide clearly tells the operator when to use each profile
