# Competition Runner Window And Auto-Stop Design

**Date:** 2026-03-30

## Summary

Add shared runtime controls for both futures and spot competition runners so they can:

- wait until a configured start time before placing strategy orders
- stop at a configured end time
- stop automatically when rolling 60-minute loss reaches a threshold
- stop automatically when cumulative traded notional reaches a threshold

When any stop condition is hit, the system must stop the main strategy, cancel strategy-owned open orders, and reuse the existing top-of-book maker flatten flow so it keeps chasing the best bid/ask until the position is fully closed.

This change applies to:

- futures runner started from `/api/runner/start`
- spot runner started from `/api/spot_runner/start`
- the corresponding runner status pages and snapshots

Deployment scope for this work is the existing repository plus post-merge update on `43.155.136.111`.

## Goals

- Support explicit runner start and end timestamps for futures and spot competition strategies.
- Support rolling 60-minute loss based auto-stop for futures and spot.
- Support cumulative traded notional based auto-stop for futures and spot.
- Reuse existing stop execution behavior instead of creating a second liquidation path.
- Make stop state visible in web snapshots and pages.

## Non-Goals

- No new competition board ranking model changes.
- No new deployment target beyond the requested update to `43.155.136.111`.
- No attempt to auto-restart a runner after an end condition has triggered.
- No change to how the existing flatten runner places maker chase orders beyond wiring it into the new auto-stop path.

## Current Context

The repository already has two independent live strategy loops:

- futures: [`/Volumes/WORK/binance/grid_trading/src/grid_optimizer/loop_runner.py`](/Volumes/WORK/binance/grid_trading/src/grid_optimizer/loop_runner.py)
- spot: [`/Volumes/WORK/binance/grid_trading/src/grid_optimizer/spot_loop_runner.py`](/Volumes/WORK/binance/grid_trading/src/grid_optimizer/spot_loop_runner.py)

Runtime configuration and the web UI are assembled in [`/Volumes/WORK/binance/grid_trading/src/grid_optimizer/web.py`](/Volumes/WORK/binance/grid_trading/src/grid_optimizer/web.py).

The codebase already supports futures-side manual stop behavior that can:

- stop the main runner
- cancel strategy orders
- start a maker flatten process that keeps working the top of book until positions are closed

For spot, the codebase currently supports stopping the runner and canceling strategy orders, but it does not yet have an equivalent continuously chasing maker flatten loop. This feature must add that missing spot-side close path so spot and futures match the requested behavior.

## User-Facing Behavior

### Start Window

- If `run_start_time` is unset, the runner may trade immediately.
- If `run_start_time` is set and current time is earlier than it, the runner process may remain alive but must not place new strategy orders.
- During the pre-start period, the UI should clearly show that the runner is waiting for the configured start time.

### Active Window

- If current time is within the configured run window, the runner behaves normally.
- Each loop cycle evaluates the configured auto-stop thresholds before placing new orders.

### End Conditions

The following conditions stop the strategy:

- current time is at or after `run_end_time`
- rolling 60-minute loss is greater than or equal to `rolling_hourly_loss_limit`
- cumulative traded notional is greater than or equal to `max_cumulative_notional`

If multiple conditions are true in the same cycle, the stop metadata should record all matched reasons and a primary reason for display.

### Stop Action

Once an end condition is hit:

1. stop the main strategy loop from placing further orders
2. cancel strategy-owned open orders
3. start or continue the existing maker flatten process
4. keep chasing top-of-book maker close orders until position size reaches zero

This behavior must be identical for futures and spot from the operator point of view, even though the futures implementation can reuse an existing flatten runner and spot needs a new equivalent close loop.

## Configuration Additions

Add the following fields to both futures and spot runner configs:

- `run_start_time`: optional ISO 8601 datetime
- `run_end_time`: optional ISO 8601 datetime
- `rolling_hourly_loss_limit`: optional positive float
- `max_cumulative_notional`: optional positive float

Validation rules:

- if both start and end are set, `run_start_time` must be earlier than `run_end_time`
- loss limit must be strictly positive when set
- cumulative notional limit must be strictly positive when set
- all timestamps are stored and compared in timezone-aware UTC form

## Data Semantics

### Time

- The web layer accepts datetime input and normalizes it to timezone-aware UTC strings.
- The runners compare against current UTC time.
- Snapshots return normalized ISO 8601 timestamps.

### Rolling 60-Minute Loss

This uses a rolling window, not a natural clock hour.

Window definition:

- include runner trade and income effects whose timestamps fall within `now - 60 minutes` through `now`

Futures loss basis:

- use realized PnL and income/audit data already available to the futures runner monitor path
- the value used for stop comparison is `max(0, -window_net_pnl)`

Spot loss basis:

- use spot realized PnL for the last 60 minutes
- subtract fees and recycle loss already tracked in state/summary
- the value used for stop comparison is `max(0, -window_net_pnl)`

If the rolling loss limit is unset, the condition is disabled.

### Cumulative Traded Notional

- use the runner's cumulative `gross_notional`
- compare against `max_cumulative_notional`
- if the threshold is unset, the condition is disabled

## Architecture

### Shared Runtime Guard Helper

Add a small shared helper module for runtime gate evaluation. The helper should:

- normalize optional runtime guard config
- determine whether the runner is before start, active, or after end
- compute rolling 60-minute loss from the available event/state inputs
- evaluate cumulative notional threshold
- return a normalized guard result with:
  - `tradable`
  - `stop_triggered`
  - `primary_reason`
  - `matched_reasons`
  - `triggered_at`
  - computed metrics for display

This keeps futures and spot on the same decision model while still allowing each runner to provide its own metric inputs.

### Futures Runner Integration

In [`/Volumes/WORK/binance/grid_trading/src/grid_optimizer/loop_runner.py`](/Volumes/WORK/binance/grid_trading/src/grid_optimizer/loop_runner.py):

- parse new CLI arguments
- evaluate runtime guards near the start of each cycle, before plan execution places or refreshes orders
- if before start, skip plan submission and write a waiting summary
- if a stop condition is hit, write stop metadata to summary/state, invoke the existing futures stop execution path with cancel + flatten behavior, and exit the main loop cleanly

### Spot Runner Integration

In [`/Volumes/WORK/binance/grid_trading/src/grid_optimizer/spot_loop_runner.py`](/Volumes/WORK/binance/grid_trading/src/grid_optimizer/spot_loop_runner.py):

- parse new CLI arguments
- evaluate runtime guards before building or submitting desired orders
- if before start, skip order placement and write a waiting summary
- if a stop condition is hit, write stop metadata to summary/state, invoke a new spot close path that cancels strategy orders and starts a top-of-book maker chase until spot inventory is flat, and exit the main loop cleanly

### Flattening Integration

The close path must be market-specific behind a consistent stop contract:

- futures reuses the existing maker flatten runner
- spot adds a new maker-style flatten loop or equivalent module with the same operational semantics:
  - place maker close orders at top of book
  - cancel/replace as the quote moves
  - continue until managed inventory reaches zero
  - avoid duplicate flatten processes for the same symbol

### Web Layer Integration

In [`/Volumes/WORK/binance/grid_trading/src/grid_optimizer/web.py`](/Volumes/WORK/binance/grid_trading/src/grid_optimizer/web.py):

- add the four new config fields to futures runner payload normalization
- add the four new config fields to spot runner payload normalization
- include the new fields in command construction for both runners
- add snapshot fields so the UI can show:
  - configured run window
  - runtime status (`waiting`, `running`, `stopped`)
  - rolling 60-minute loss
  - cumulative traded notional
  - whether auto-stop fired
  - stop reason and stop timestamp

## Stop Reason Model

Use stable reason codes:

- `before_start_window`
- `after_end_window`
- `rolling_hourly_loss_limit_hit`
- `max_cumulative_notional_hit`

Behavior:

- `before_start_window` is a waiting state, not a terminal stop
- the other three are terminal stop conditions
- snapshots should expose both machine-readable reason codes and short human-readable text

## State And Summary Changes

Both runners should emit enough data for troubleshooting and UI display.

Add or persist fields such as:

- `run_start_time`
- `run_end_time`
- `rolling_hourly_loss_limit`
- `max_cumulative_notional`
- `runtime_status`
- `rolling_hourly_loss`
- `cumulative_gross_notional`
- `stop_triggered`
- `stop_reason`
- `stop_reasons`
- `stop_triggered_at`

For waiting cycles before start, summaries should still append a lightweight status event so operators can confirm the runner is alive and intentionally idle.

## Error Handling

- Invalid config should fail at web payload normalization or CLI parsing, not mid-cycle.
- If stop actions partially fail, summaries must still record the failure details and preserve the primary stop reason.
- If flattening is already running when an auto-stop fires, the system should reuse that state instead of launching duplicate flatten processes.
- If no position exists at stop time, the process still cancels strategy orders and exits cleanly.

## Testing Strategy

### Web Tests

Extend web tests to verify:

- new futures fields normalize correctly
- new spot fields normalize correctly
- command builders include the new CLI flags
- snapshot payloads expose runtime window and stop metadata

### Futures Runner Tests

Add tests for:

- waiting before `run_start_time`
- stopping at `run_end_time`
- stopping when rolling loss exceeds threshold
- stopping when cumulative gross notional exceeds threshold
- routing through existing cancel + flatten stop behavior

### Spot Runner Tests

Add tests for:

- waiting before `run_start_time`
- stopping at `run_end_time`
- stopping when rolling loss exceeds threshold
- stopping when cumulative gross notional exceeds threshold
- routing through spot stop execution and the new maker flatten behavior

### Verification

Before deployment:

- run targeted unit tests for web, futures runner, and spot runner
- run at least one focused command-level verification for each runner path to ensure the new flags parse correctly

## Deployment Plan

After implementation and local verification:

1. merge the change into `main`
2. deploy using the existing Oracle deployment flow in [`/Volumes/WORK/binance/grid_trading/deploy/oracle/install_or_update.sh`](/Volumes/WORK/binance/grid_trading/deploy/oracle/install_or_update.sh)
3. update the requested host `43.155.136.111`
4. confirm the relevant service status and that the new runtime fields are visible in the web UI or snapshot response

Only the requested `43.155.136.111` host is in scope for this deployment step.

## Implementation Notes

- Prefer a shared helper for guard evaluation over duplicating logic in both runners.
- Reuse existing stop and flatten orchestration rather than inventing a second exit path.
- Keep the UI additions explicit and operationally oriented: operators should immediately see whether a runner is waiting, actively trading, or auto-stopped and why.
