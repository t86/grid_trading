# Strategy Diagnostics Detail Design

## Goal

Make the strategy editor explain, in fine detail, why a selected strategy can or cannot place orders before the user starts or changes a runner.

The first version is report-only. It must not change planning, order submission, cancel behavior, thresholds, or state transitions. Its job is to expose the effective behavior that already exists:

- whether startup is blocked
- whether the strategy is likely to place fewer orders than expected
- which parameters are active, ignored, unknown, or outside the selected strategy boundary
- which global safety valves may stop, reduce, or take over execution
- whether the runner is currently in volume mode, repair mode, blocked mode, or unknown mode
- how close current inventory is to soft and hard thresholds
- whether 200k or 500k hourly volume targets are likely to be capped by current settings

The output should be visible in `/strategy_editor` and reusable as API data for future startup checks and deployment validation.

## Background

The current profile isolation work already reports:

- strict profile schema status
- ignored and unknown parameters
- required position mode, defaulting to `one_way`
- global safety preflight details
- startup status as `ready`, `warning`, or `blocked`

This is useful but still too coarse. The user needs to see the exact reason a strategy may fail to place orders, enter repair too early, reduce position unexpectedly, or miss aggressive volume targets.

The diagnostic layer should turn raw preflight fields into operational explanations. For example:

```text
max_total_notional is active and lower than one theoretical order cycle.
Impact: this strategy may generate 16 orders, but execution may cap the cycle before all orders are submitted.
Suggested action: increase max_total_notional above buy_levels + sell_levels multiplied by per_order_notional.
Risk: raising it allows larger instantaneous exposure.
```

## Non-Goals

This design does not:

- start, stop, or restart runners from the strategy editor
- automatically tune parameters
- automatically switch Binance position mode
- read heavy monitor snapshots
- perform historical performance analytics
- change any order planning or order submission behavior
- replace profile schema validation

Historical PnL and wear analysis should be a later feature after diagnostics are trustworthy.

## Selected Approach

Add a new report object named `strategy_diagnostics`.

`strategy_diagnostics` should be built from already lightweight data:

- effective runner config
- `startup_preflight`
- `global_safety_preflight`
- latest plan report
- latest submit report when available
- latest summary event
- lightweight runtime position and open order snapshot already used by the strategy editor

The report should be returned by `/api/strategy_editor/status` and rendered in `/strategy_editor`.

The existing `startup_preflight` and `safety_preflight` remain in place. `strategy_diagnostics` is a higher-level explanation layer, not a replacement.

## Report Shape

The report should be stable JSON with these top-level fields:

```json
{
  "status": "blocked|warning|ready|unknown",
  "can_start": true,
  "mode": "volume|repair|blocked|idle|unknown",
  "summary": "可启动，有 3 个风险会影响冲量。",
  "issue_count": 3,
  "blocker_count": 0,
  "warning_count": 3,
  "sections": [],
  "volume_targets": [],
  "inventory": {},
  "order_cycle": {},
  "state": {}
}
```

### Sections

Each section represents one diagnostic area:

```json
{
  "key": "execution_caps",
  "title": "执行容量限制",
  "status": "warning",
  "items": []
}
```

Initial sections:

- `startup`: strict schema, unknown params, ignored params, position mode
- `execution_caps`: `max_new_orders`, `max_total_notional`, estimated cycle capacity
- `order_refresh`: `cancel_stale`, stale order replacement risk
- `drift_guards`: `max_mid_drift_steps`, near-market entry guard, center distance guards
- `loss_and_stop_guards`: hourly loss, cumulative notional, net/gross exposure stop guards
- `takeover_modules`: forced reduce, excess inventory reduce-only, adverse reduce style modules
- `inventory_thresholds`: current long/short inventory versus soft and hard thresholds
- `state_machine`: volume state versus repair state, active state, repair ladder
- `profile_boundary`: allowed params, ignored params, unknown params, required position mode
- `volume_targets`: 200k and 500k feasibility checks

### Diagnostic Items

Each item should be explicit enough to show on the page without extra lookup:

```json
{
  "key": "max_total_notional",
  "severity": "warning",
  "category": "limits_volume",
  "current_value": 1000,
  "expected_value": 1600,
  "active": true,
  "title": "单轮名义金额上限低于理论挂单名义金额",
  "why": "当前买卖档位和单笔金额预计生成 1600U 挂单，但 max_total_notional 只有 1000U。",
  "impact": "冲量策略可能每轮只提交部分挂单，表现为刷量速度低于预期。",
  "suggestion": "如果要跑冲量，将 max_total_notional 调到单轮理论挂单名义金额以上，并预留盘口波动空间。",
  "tradeoff": "调高后单轮瞬时敞口和误成交风险都会变大。",
  "related_params": ["buy_levels", "sell_levels", "per_order_notional", "max_total_notional"]
}
```

Severity values:

- `blocker`: very likely to block startup or order submission
- `warning`: likely to reduce volume, increase churn, or cause unexpected state changes
- `info`: useful explanation, not a problem
- `ok`: checked and acceptable

Category values should be machine-readable and stable. Initial categories:

- `blocks_start`
- `blocks_orders`
- `limits_volume`
- `increases_wear`
- `forces_repair`
- `takes_over_orders`
- `stops_runner`
- `outside_profile`
- `position_mode`
- `inventory_distance`
- `target_feasibility`

## Diagnostic Rules

### Startup

Show a blocker when:

- `startup_preflight.can_start` is false
- strict schema has unknown active parameters
- global safety preflight has blocking params
- current account position mode is known and incompatible with profile required mode

Show a warning when:

- profile schema is unknown
- `required_position_mode` was defaulted to `one_way`
- ignored params exist
- safety warnings, stop guards, takeover modules, or limiting params exist

### Execution Capacity

Compute:

```text
estimated_cycle_order_count = buy_levels + sell_levels
estimated_cycle_notional = estimated_cycle_order_count * per_order_notional
```

Warn when:

- `max_new_orders` is lower than `estimated_cycle_order_count`
- `max_total_notional` is lower than `estimated_cycle_notional`
- either cap is present but non-positive

Explain the user-facing symptom:

- fewer submitted orders than expected
- only one side or partial layers appearing
- high-volume strategy unable to reach expected hourly volume

### Volume Targets

Add built-in target checks for:

- `200000` hourly notional
- `500000` hourly notional

For each target, estimate:

```text
required_full_cycles_per_hour = target / max(estimated_cycle_notional, 1)
required_seconds_per_full_cycle = 3600 / required_full_cycles_per_hour
```

Report:

- whether current single-cycle capacity is enough to make the target plausible
- which caps would prevent the target
- how many theoretical full cycles per minute are required

This is only a feasibility check, not a promise. It should explicitly say that real fill rate depends on market depth, spread, queue position, and volatility.

### Inventory Thresholds

For one-way long style profiles:

- compare `long_notional` or net positive notional against `pause_buy_position_notional`
- compare against `max_position_notional`
- show distance to soft and hard thresholds
- warn when current position is near soft
- block or severe warn when at or above hard

For hedge profiles:

- report long and short legs separately
- compare long leg against long thresholds
- compare short leg against short thresholds
- report gross and net exposure

For legacy or unknown profile types:

- report all available long/short/net values
- mark threshold interpretation as best-effort

### State Machine

Classify current mode from latest plan fields:

- `volume` when active state looks like `normal`, `fast`, `make`, or equivalent
- `repair` when active state or repair ladder indicates recover, safe, reduce, repair, or inventory recovery
- `blocked` when latest plan or submit report has an error, refusal, or no-submit reason
- `idle` when runner is not running
- `unknown` when there is not enough data

The page should clearly show:

```text
当前处于刷量状态 / 修仓状态 / 阻塞状态 / 未运行 / 未知
```

If state is repair, show which thresholds and modules likely caused it when the data is available.

### Profile Boundary

List:

- active allowed params
- ignored params
- unknown params
- global safety params
- required position mode

The UI should make ignored params explicit:

```text
这些旧参数存在于配置中，但不会对当前 profile 生效。
```

This is central to preventing mixed strategy behavior.

## UI Design

`/strategy_editor` remains manual-refresh only. It must not add auto-refresh or heavy monitor calls.

Add a diagnostics area below the existing status cards:

1. Top summary row:
   - startup status
   - current strategy mode: volume / repair / blocked / idle / unknown
   - blockers / warnings count
   - estimated cycle orders and notional

2. Important issues first:
   - blockers first
   - warnings second
   - info and ok collapsed or visually lighter

3. Section groups:
   - Startup
   - Execution Caps
   - Volume Targets
   - Inventory Thresholds
   - State Machine
   - Profile Boundary
   - Stop/Takeover Modules

4. Each issue card shows:
   - current value
   - expected or safe reference value when available
   - why
   - impact
   - suggestion
   - tradeoff

The page should stay lightweight:

- no polling
- no charts in the first version
- no large tables by default
- use collapsible details for long sections if needed

## API Design

`GET /api/strategy_editor/status?symbol=AIGENSYNUSDT` should add:

```json
{
  "strategy_diagnostics": {}
}
```

Existing fields must remain compatible:

- `startup_preflight`
- `safety_preflight`
- `runner`
- `position`
- `position_mode`
- `latest_loop`
- `orders`
- `presets`

If diagnostics cannot be built, the API should still return `ok: true` with:

```json
{
  "strategy_diagnostics": {
    "status": "unknown",
    "can_start": false,
    "summary": "诊断数据不足。",
    "sections": []
  }
}
```

It should not fail the whole status endpoint unless the existing status endpoint would already fail.

## Testing

Add focused tests before implementation:

1. Schema diagnostic unit tests:
   - capacity warning when `max_total_notional` is below estimated cycle notional
   - order count warning when `max_new_orders` is below estimated order count
   - blocker when startup preflight is blocked
   - ignored params appear under profile boundary
   - defaulted one-way mode appears as a warning, not a blocker

2. Inventory tests:
   - one-way long near soft threshold warns
   - one-way long over hard threshold is severe
   - hedge long and short legs are reported separately when available

3. Volume target tests:
   - 200k and 500k targets are present
   - required cycles per hour and seconds per cycle are computed
   - infeasible target cites limiting params

4. Web status tests:
   - `/api/strategy_editor/status` includes `strategy_diagnostics`
   - the endpoint still avoids heavy monitor snapshot calls
   - existing response fields are unchanged

5. Page tests:
   - `STRATEGY_EDITOR_PAGE` contains diagnostics rendering
   - page remains manual-refresh only and contains no `setInterval`

6. Browser verification:
   - open `/strategy_editor`
   - refresh status manually
   - confirm diagnostics area renders
   - confirm no console errors
   - confirm page remains responsive

## Rollout

Step 1: Add pure diagnostic builder and tests.

Step 2: Wire diagnostics into `_build_strategy_editor_status`.

Step 3: Render diagnostics in `/strategy_editor`.

Step 4: Verify locally with tests and browser.

Step 5: Commit and deploy to 110 after approval.

## Follow-Up Tasks After Diagnostics

Once diagnostics are clear, continue with:

1. Complete per-profile parameter allowlists and forbidden parameter reporting.
2. Split strategy state machines into explicit normal, fast, recover, safe, reduce, and blocked transitions.
3. Add fixed replay samples for AIGENSYN best-quote and synthetic multi-layer profiles.
4. Add historical strategy performance summaries: volume, wear, fill rate, maker ratio, realized PnL, floating drawdown, repair duration.
5. Build separate verified profiles for competition stages:
   - early conservative profit-seeking
   - mid-stage balanced volume
   - last-day aggressive volume sprint
6. Add hedge-mode native diagnostics after the account migration starts.
7. Only after the above, change real order planning behavior.

## Acceptance Criteria

The feature is acceptable when:

- the user can see exactly why a strategy may not place orders
- the user can see exactly why volume may be capped below target
- ignored and unknown parameters are visible and separated from active strategy params
- current state is labeled as volume, repair, blocked, idle, or unknown
- inventory soft/hard threshold distance is visible
- 200k and 500k target feasibility is visible
- the strategy editor remains lightweight and manual-refresh only
- tests cover the diagnostic builder, API shape, and page rendering hooks
