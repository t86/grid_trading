# Maker Volatility Inventory Futures Design

## Goal

Add a testable futures maker strategy that provides simple two-sided liquidity while controlling loss, inventory drift, and order churn. The first version uses notional sizing, so "single order 30, max one-sided 300" means quote notional, not coin quantity.

## Scope

The strategy runs inside the existing futures `loop_runner` execution framework. It should not be a separate bot and should not modify the behavior of existing `synthetic_neutral`, `hedge_neutral`, `one_way_long`, or `one_way_short` presets.

New strategy mode:

```text
maker_volatility_inventory_v1
```

The strategy reuses existing runner capabilities:

- futures market data via REST or websocket snapshot
- account and position reads
- post-only order preparation and retry handling
- plan validation, `max_new_orders`, `max_total_notional`, stale-order handling
- runtime guards, state files, audit files, and runner control paths

## Strategy Behavior

Each cycle reads best bid, best ask, mid price, current net position, and recent futures klines. It generates target maker orders near the top of book:

- `normal`: place one buy and one sell around the current book using `maker_base_spread_bps`.
- `volatility_wide`: if recent amplitude or absolute return exceeds the configured threshold, widen both sides using `maker_wide_spread_bps`.
- `long_inventory_reduce`: if long notional reaches the soft inventory threshold, reduce or pause new buy orders and keep sell orders.
- `short_inventory_reduce`: if short notional reaches the soft inventory threshold, reduce or pause new sell orders and keep buy orders.
- `hard_reduce_only`: if one-sided notional reaches the hard maximum, only generate orders that reduce that exposure.
- `cooldown`: after extreme volatility or repeated submit rejection signals, pause new opening orders until the cooldown expires.

Inventory rules are based on actual net position notional:

```text
long_notional = max(net_qty, 0) * mid
short_notional = max(-net_qty, 0) * mid
soft_limit = hard_limit * maker_inventory_soft_ratio
```

## Configuration

Initial fields:

```json
{
  "strategy_mode": "maker_volatility_inventory_v1",
  "maker_base_spread_bps": 4.0,
  "maker_wide_spread_bps": 12.0,
  "maker_order_notional": 30.0,
  "maker_max_long_notional": 300.0,
  "maker_max_short_notional": 300.0,
  "maker_inventory_soft_ratio": 0.7,
  "maker_volatility_window": "1m",
  "maker_volatility_wide_threshold": 0.006,
  "maker_extreme_volatility_threshold": 0.012,
  "maker_cooldown_seconds": 30.0
}
```

The first version supports one order per side per cycle. Existing runner-level `max_new_orders` and `max_total_notional` remain the final submission limits.

## Order Construction

For each side:

- Buy price is below or equal to the resting bid side after spread adjustment.
- Sell price is above or equal to the resting ask side after spread adjustment.
- Quantity is `maker_order_notional / price`, rounded by existing symbol step rules.
- Orders use role names that make reduce-only handling explicit:
  - `maker_entry_long`
  - `maker_entry_short`
  - `maker_reduce_long`
  - `maker_reduce_short`

Post-only price adjustment remains the responsibility of the existing submit path.

## State

The runner state stores a small strategy state object:

```json
{
  "maker_volatility_inventory": {
    "regime": "normal",
    "last_regime": "normal",
    "cooldown_until": null,
    "last_reason": null
  }
}
```

State transitions should require only the current cycle in v1. Hysteresis can be added later if live behavior is too noisy.

## Errors And Guards

If market data is missing or invalid, the plan should contain no new orders and include a clear blocked reason.

If the account is not in one-way mode, v1 should still use actual net exposure and avoid hedge-position-specific behavior. Hedge mode support can be added later after tests define position-side semantics.

The strategy must not self-trade intentionally. It only submits maker orders through the normal strategy order prefix and relies on Binance/account STP settings plus the existing order diff to avoid unnecessary crossed behavior.

## Testing

Add focused unit tests before implementation:

- normal regime generates one buy and one sell order.
- high volatility widens prices relative to the base spread.
- long soft inventory suppresses or reduces buy orders.
- short soft inventory suppresses or reduces sell orders.
- hard long inventory generates only long-reducing sell orders.
- hard short inventory generates only short-reducing buy orders.
- invalid market data produces no orders with a blocked reason.

Integration tests should cover `generate_plan_report()` recognizing the new `strategy_mode` and still producing a plan compatible with existing submit validation.
