from __future__ import annotations

import argparse
import json
import math
import time
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from pathlib import Path
from typing import Any

from .backtest import (
    _build_grid_sides,
    _entry_reference_price,
    build_grid_levels,
    build_per_grid_notionals,
    supported_allocation_modes,
    supported_grid_level_modes,
    supported_strategy_directions,
)
from .data import (
    fetch_futures_book_tickers,
    fetch_futures_premium_index,
    fetch_futures_symbol_config,
)

STATE_VERSION = 1


def _float_str(value: float) -> str:
    return f"{value:,.4f}"


def _pct_str(value: float) -> str:
    return f"{value * 100:.2f}%"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _isoformat(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    return dt.astimezone(timezone.utc).isoformat()


def _step_decimal(step: float | None) -> Decimal | None:
    if step is None or step <= 0:
        return None
    return Decimal(str(step))


def _quantize_step(value: float, step: float | None, rounding: str) -> float:
    step_dec = _step_decimal(step)
    if step_dec is None:
        return float(value)
    value_dec = Decimal(str(value))
    units = (value_dec / step_dec).to_integral_value(rounding=rounding)
    return float(units * step_dec)


def _round_order_price(price: float, tick_size: float | None, order_side: str) -> float:
    rounding = ROUND_DOWN if order_side.upper() == "BUY" else ROUND_UP
    return _quantize_step(price, tick_size, rounding)


def _round_order_qty(qty: float, step_size: float | None) -> float:
    return _quantize_step(qty, step_size, ROUND_DOWN)


def build_paper_grid_legs(
    *,
    min_price: float,
    max_price: float,
    n: int,
    total_buy_notional: float,
    grid_level_mode: str,
    allocation_mode: str,
    strategy_direction: str,
    neutral_anchor_price: float | None,
    tick_size: float | None,
    step_size: float | None,
    min_qty: float | None,
    min_notional: float | None,
) -> list[dict[str, Any]]:
    levels = build_grid_levels(
        min_price=min_price,
        max_price=max_price,
        n=n,
        grid_level_mode=grid_level_mode,
    )
    normalized_direction = strategy_direction.strip().lower()
    if normalized_direction == "neutral":
        anchor_price = float(neutral_anchor_price) if neutral_anchor_price is not None else (min_price + max_price) / 2.0
        if anchor_price <= 0:
            raise ValueError("neutral_anchor_price must be > 0")
    else:
        anchor_price = None
    grid_sides = _build_grid_sides(levels, normalized_direction, anchor_price)
    entry_ref_prices = [_entry_reference_price(levels, i, grid_sides[i]) for i in range(n)]

    raw_notionals = build_per_grid_notionals(
        total_buy_notional=total_buy_notional,
        n=n,
        allocation_mode=allocation_mode,
        price_levels=levels,
    )
    raw_qty = [raw_notionals[i] / entry_ref_prices[i] for i in range(n)]
    if normalized_direction in {"long", "short"}:
        target_price = min_price if normalized_direction == "long" else max_price
        total_qty_raw = sum(raw_qty)
        if target_price > 0 and total_qty_raw > 0:
            target_total_qty = total_buy_notional / target_price
            scale = target_total_qty / total_qty_raw
            raw_qty = [qty * scale for qty in raw_qty]

    legs: list[dict[str, Any]] = []
    for idx in range(n):
        grid_side = grid_sides[idx]
        if grid_side == "long":
            entry_order_side = "BUY"
            exit_order_side = "SELL"
            entry_price_raw = levels[idx]
            exit_price_raw = levels[idx + 1]
        else:
            entry_order_side = "SELL"
            exit_order_side = "BUY"
            entry_price_raw = levels[idx + 1]
            exit_price_raw = levels[idx]

        entry_price = _round_order_price(entry_price_raw, tick_size, entry_order_side)
        exit_price = _round_order_price(exit_price_raw, tick_size, exit_order_side)
        qty = _round_order_qty(raw_qty[idx], step_size)
        entry_notional = qty * entry_price
        status = "flat"
        disabled_reason = None
        if min_qty is not None and qty < min_qty:
            status = "disabled"
            disabled_reason = "qty_below_min_qty"
        elif min_notional is not None and entry_notional < min_notional:
            status = "disabled"
            disabled_reason = "entry_notional_below_min_notional"
        elif qty <= 0:
            status = "disabled"
            disabled_reason = "qty_rounded_to_zero"

        legs.append(
            {
                "grid_index": idx,
                "grid_side": grid_side,
                "entry_order_side": entry_order_side,
                "exit_order_side": exit_order_side,
                "entry_price": entry_price,
                "exit_price": exit_price,
                "entry_price_raw": entry_price_raw,
                "exit_price_raw": exit_price_raw,
                "qty": qty,
                "entry_notional": entry_notional,
                "state": status,
                "disabled_reason": disabled_reason,
                "position_state": "flat",
                "position_qty": 0.0,
                "position_entry_price": None,
                "position_entry_time": None,
                "fill_count": 0,
                "resting_order_intent": None,
            }
        )
    return legs


def _config_payload(args: argparse.Namespace, symbol_info: dict[str, Any]) -> dict[str, Any]:
    return {
        "symbol": args.symbol.upper(),
        "min_price": args.min_price,
        "max_price": args.max_price,
        "n": args.n,
        "total_buy_notional": args.total_buy_notional,
        "grid_level_mode": args.grid_level_mode,
        "allocation_mode": args.allocation_mode,
        "strategy_direction": args.strategy_direction,
        "neutral_anchor_price": args.neutral_anchor_price,
        "fee_rate": args.fee_rate,
        "tick_size": symbol_info.get("tick_size"),
        "step_size": symbol_info.get("step_size"),
        "min_qty": symbol_info.get("min_qty"),
        "min_notional": symbol_info.get("min_notional"),
    }


def _config_signature(config: dict[str, Any]) -> str:
    return json.dumps(config, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def initialize_paper_state(args: argparse.Namespace, symbol_info: dict[str, Any]) -> dict[str, Any]:
    config = _config_payload(args, symbol_info)
    legs = build_paper_grid_legs(
        min_price=args.min_price,
        max_price=args.max_price,
        n=args.n,
        total_buy_notional=args.total_buy_notional,
        grid_level_mode=args.grid_level_mode,
        allocation_mode=args.allocation_mode,
        strategy_direction=args.strategy_direction,
        neutral_anchor_price=args.neutral_anchor_price,
        tick_size=symbol_info.get("tick_size"),
        step_size=symbol_info.get("step_size"),
        min_qty=symbol_info.get("min_qty"),
        min_notional=symbol_info.get("min_notional"),
    )
    return {
        "version": STATE_VERSION,
        "created_at": _isoformat(_utc_now()),
        "updated_at": _isoformat(_utc_now()),
        "config": config,
        "config_signature": _config_signature(config),
        "market_status": "unknown",
        "metrics": {
            "gross_trade_notional": 0.0,
            "realized_pnl": 0.0,
            "unrealized_pnl": 0.0,
            "net_pnl_excluding_funding": 0.0,
            "total_fees": 0.0,
            "fill_count": 0,
            "closed_trades": 0,
            "wins": 0,
            "estimated_next_funding_pnl": 0.0,
        },
        "legs": legs,
        "fills": [],
        "last_snapshot": {},
    }


def load_or_initialize_state(
    *,
    state_path: Path,
    args: argparse.Namespace,
    symbol_info: dict[str, Any],
    reset_state: bool,
) -> dict[str, Any]:
    if state_path.exists() and not reset_state:
        payload = json.loads(state_path.read_text(encoding="utf-8"))
        expected_signature = _config_signature(_config_payload(args, symbol_info))
        actual_signature = str(payload.get("config_signature", "")).strip()
        if actual_signature != expected_signature:
            raise SystemExit(
                "State file config does not match current CLI arguments. "
                "Use --reset-state or a different --state-path."
            )
        return payload
    state = initialize_paper_state(args, symbol_info)
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    return state


def _append_fill(state: dict[str, Any], fill: dict[str, Any]) -> None:
    fills = state.setdefault("fills", [])
    fills.append(fill)
    if len(fills) > 200:
        del fills[:-200]


def apply_market_snapshot(
    *,
    state: dict[str, Any],
    bid_price: float,
    ask_price: float,
    mark_price: float | None,
    funding_rate: float | None,
    next_funding_time: int | None,
    ts: datetime,
) -> dict[str, Any]:
    if bid_price <= 0 or ask_price <= 0:
        raise ValueError("bid_price and ask_price must be > 0")
    if bid_price > ask_price:
        raise ValueError("bid_price cannot exceed ask_price")

    config = state["config"]
    metrics = state["metrics"]
    legs = state["legs"]
    fee_rate = float(config["fee_rate"])
    mid_price = (bid_price + ask_price) / 2.0
    ref_price = mark_price if mark_price and mark_price > 0 else mid_price

    if ask_price < float(config["min_price"]):
        market_status = "below_range"
    elif bid_price > float(config["max_price"]):
        market_status = "above_range"
    else:
        market_status = "in_range"

    fills_this_cycle: list[dict[str, Any]] = []
    for leg in legs:
        if leg["state"] == "disabled":
            continue
        position_state = leg.get("position_state", "flat")
        resting_order_intent = leg.get("resting_order_intent")
        qty = float(leg["qty"])

        if position_state == "flat" and resting_order_intent == "entry":
            entry_side = str(leg["entry_order_side"]).upper()
            entry_price = float(leg["entry_price"])
            can_fill_entry = (entry_side == "BUY" and ask_price <= entry_price) or (
                entry_side == "SELL" and bid_price >= entry_price
            )
            if not can_fill_entry:
                continue
            notional = entry_price * qty
            fee = notional * fee_rate
            leg["position_state"] = "open"
            leg["position_qty"] = qty
            leg["position_entry_price"] = entry_price
            leg["position_entry_time"] = _isoformat(ts)
            leg["fill_count"] = int(leg.get("fill_count", 0)) + 1
            leg["resting_order_intent"] = None
            metrics["gross_trade_notional"] += notional
            metrics["total_fees"] += fee
            metrics["fill_count"] += 1
            fill = {
                "ts": _isoformat(ts),
                "grid_index": leg["grid_index"],
                "event": "open",
                "side": entry_side,
                "price": entry_price,
                "qty": qty,
                "notional": notional,
                "fee": fee,
            }
            fills_this_cycle.append(fill)
            _append_fill(state, fill)
            continue

        if position_state != "open" or resting_order_intent != "exit":
            continue

        exit_side = str(leg["exit_order_side"]).upper()
        exit_price = float(leg["exit_price"])
        can_fill_exit = (exit_side == "SELL" and bid_price >= exit_price) or (
            exit_side == "BUY" and ask_price <= exit_price
        )
        if not can_fill_exit:
            continue

        entry_price = float(leg["position_entry_price"])
        notional = exit_price * qty
        fee = notional * fee_rate
        if leg["grid_side"] == "long":
            realized_pnl = (exit_price - entry_price) * qty
        else:
            realized_pnl = (entry_price - exit_price) * qty

        leg["position_state"] = "flat"
        leg["position_qty"] = 0.0
        leg["position_entry_price"] = None
        leg["position_entry_time"] = None
        leg["fill_count"] = int(leg.get("fill_count", 0)) + 1
        leg["resting_order_intent"] = None
        metrics["gross_trade_notional"] += notional
        metrics["total_fees"] += fee
        metrics["realized_pnl"] += realized_pnl
        metrics["fill_count"] += 1
        metrics["closed_trades"] += 1
        if realized_pnl > 0:
            metrics["wins"] += 1
        fill = {
            "ts": _isoformat(ts),
            "grid_index": leg["grid_index"],
            "event": "close",
            "side": exit_side,
            "price": exit_price,
            "qty": qty,
            "notional": notional,
            "fee": fee,
            "realized_pnl": realized_pnl,
        }
        fills_this_cycle.append(fill)
        _append_fill(state, fill)

    open_position_notional = 0.0
    unrealized_pnl = 0.0
    active_orders: list[dict[str, Any]] = []
    for leg in legs:
        if leg["state"] == "disabled":
            continue
        qty = float(leg["qty"])
        position_state = leg.get("position_state", "flat")
        if position_state == "open":
            entry_price = float(leg["position_entry_price"])
            signed_qty = qty if leg["grid_side"] == "long" else -qty
            open_position_notional += signed_qty * ref_price
            if leg["grid_side"] == "long":
                unrealized_pnl += (ref_price - entry_price) * qty
            else:
                unrealized_pnl += (entry_price - ref_price) * qty
            exit_side = str(leg["exit_order_side"]).upper()
            exit_price = float(leg["exit_price"])
            is_resting = (exit_side == "SELL" and exit_price > bid_price) or (
                exit_side == "BUY" and exit_price < ask_price
            )
            if is_resting:
                leg["resting_order_intent"] = "exit"
                active_orders.append(
                    {
                        "grid_index": leg["grid_index"],
                        "intent": "exit",
                        "side": exit_side,
                        "price": exit_price,
                        "qty": qty,
                        "notional": exit_price * qty,
                    }
                )
            else:
                leg["resting_order_intent"] = None
            continue

        entry_side = str(leg["entry_order_side"]).upper()
        entry_price = float(leg["entry_price"])
        is_resting = (entry_side == "BUY" and entry_price < ask_price) or (
            entry_side == "SELL" and entry_price > bid_price
        )
        if is_resting:
            leg["resting_order_intent"] = "entry"
            active_orders.append(
                {
                    "grid_index": leg["grid_index"],
                    "intent": "entry",
                    "side": entry_side,
                    "price": entry_price,
                    "qty": qty,
                    "notional": entry_price * qty,
                }
            )
        else:
            leg["resting_order_intent"] = None

    metrics["unrealized_pnl"] = unrealized_pnl
    metrics["estimated_next_funding_pnl"] = (
        -(open_position_notional * funding_rate) if funding_rate is not None else 0.0
    )
    metrics["net_pnl_excluding_funding"] = (
        metrics["realized_pnl"] + metrics["unrealized_pnl"] - metrics["total_fees"]
    )
    state["market_status"] = market_status
    state["updated_at"] = _isoformat(ts)

    summary = {
        "ts": _isoformat(ts),
        "market_status": market_status,
        "bid_price": bid_price,
        "ask_price": ask_price,
        "mid_price": mid_price,
        "mark_price": ref_price,
        "funding_rate": funding_rate,
        "next_funding_time": next_funding_time,
        "open_position_notional": open_position_notional,
        "metrics": dict(metrics),
        "fills_this_cycle": fills_this_cycle,
        "active_orders": active_orders,
        "recenter_required": market_status != "in_range",
        "carry_headwind": (
            funding_rate is not None
            and (
                (config["strategy_direction"] == "long" and funding_rate > 0)
                or (config["strategy_direction"] == "short" and funding_rate < 0)
            )
        ),
    }
    state["last_snapshot"] = summary
    return summary


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Paper executor for Binance perpetual grid strategies. Reads public data only and never places orders."
    )
    parser.add_argument("--symbol", type=str, default="ENSOUSDT")
    parser.add_argument("--min-price", type=float, required=True)
    parser.add_argument("--max-price", type=float, required=True)
    parser.add_argument("--n", type=int, required=True)
    parser.add_argument("--total-buy-notional", type=float, required=True)
    parser.add_argument(
        "--grid-level-mode",
        type=str,
        default="arithmetic",
        choices=supported_grid_level_modes(),
    )
    parser.add_argument(
        "--allocation-mode",
        type=str,
        default="linear_reverse",
        choices=supported_allocation_modes(),
    )
    parser.add_argument(
        "--strategy-direction",
        type=str,
        default="long",
        choices=supported_strategy_directions(),
    )
    parser.add_argument("--neutral-anchor-price", type=float, default=None)
    parser.add_argument("--fee-rate", type=float, default=0.0002)
    parser.add_argument("--iterations", type=int, default=1, help="Number of snapshot loops to run")
    parser.add_argument("--sleep-seconds", type=float, default=15.0, help="Sleep between loops when iterations > 1")
    parser.add_argument(
        "--state-path",
        type=str,
        default="output/grid_dry_run_state.json",
        help="Persistent dry-run state json path",
    )
    parser.add_argument(
        "--snapshot-path",
        type=str,
        default="output/grid_dry_run_snapshot.json",
        help="Latest snapshot json path",
    )
    parser.add_argument("--reset-state", action="store_true", help="Reset paper state instead of resuming")
    return parser


def _print_summary(state: dict[str, Any], snapshot: dict[str, Any]) -> None:
    metrics = snapshot["metrics"]
    active_orders = snapshot["active_orders"]
    fills = snapshot["fills_this_cycle"]
    print("=== Dry Run Snapshot ===")
    print(f"Time: {snapshot['ts']}")
    print(f"Symbol: {state['config']['symbol']}")
    print(
        f"Bid/Ask/Mid: {_float_str(snapshot['bid_price'])} / {_float_str(snapshot['ask_price'])} / {_float_str(snapshot['mid_price'])}"
    )
    print(f"Market status: {snapshot['market_status']}")
    if snapshot["funding_rate"] is not None:
        print(f"Current funding rate: {_pct_str(snapshot['funding_rate'])}")
        print(f"Estimated next funding pnl: {_float_str(metrics['estimated_next_funding_pnl'])}")
    print(f"Open position notional: {_float_str(snapshot['open_position_notional'])}")
    print(f"Gross traded notional: {_float_str(metrics['gross_trade_notional'])}")
    print(f"Realized pnl: {_float_str(metrics['realized_pnl'])}")
    print(f"Unrealized pnl: {_float_str(metrics['unrealized_pnl'])}")
    print(f"Net pnl ex funding: {_float_str(metrics['net_pnl_excluding_funding'])}")
    print(f"Total fees: {_float_str(metrics['total_fees'])}")
    print(f"Fill count / closed trades: {metrics['fill_count']} / {metrics['closed_trades']}")
    print(f"Active resting orders: {len(active_orders)}")
    if fills:
        print("Fills this cycle:")
        for fill in fills:
            realized_pnl = fill.get("realized_pnl")
            suffix = f", pnl={_float_str(realized_pnl)}" if realized_pnl is not None else ""
            print(
                f"  grid {fill['grid_index'] + 1} {fill['event']} {fill['side']} "
                f"qty={fill['qty']:.4f} price={fill['price']:.4f}{suffix}"
            )
    if snapshot["recenter_required"]:
        print("Recenter required: market moved outside the configured range.")
    if snapshot["carry_headwind"]:
        print("Carry headwind: current funding is against this directional bias.")
    print("")


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    if args.min_price >= args.max_price:
        raise SystemExit("--min-price must be < --max-price")
    if args.n <= 0:
        raise SystemExit("--n must be > 0")
    if args.total_buy_notional <= 0:
        raise SystemExit("--total-buy-notional must be > 0")
    if args.fee_rate < 0:
        raise SystemExit("--fee-rate must be >= 0")
    if args.neutral_anchor_price is not None and args.neutral_anchor_price <= 0:
        raise SystemExit("--neutral-anchor-price must be > 0")
    if args.iterations <= 0:
        raise SystemExit("--iterations must be > 0")
    if args.sleep_seconds < 0:
        raise SystemExit("--sleep-seconds must be >= 0")

    symbol_info = fetch_futures_symbol_config(args.symbol)
    state_path = Path(args.state_path)
    snapshot_path = Path(args.snapshot_path)
    state = load_or_initialize_state(
        state_path=state_path,
        args=args,
        symbol_info=symbol_info,
        reset_state=args.reset_state,
    )

    for iteration in range(args.iterations):
        book_rows = fetch_futures_book_tickers(symbol=args.symbol)
        premium_rows = fetch_futures_premium_index(symbol=args.symbol)
        if not book_rows:
            raise SystemExit(f"No book ticker returned for {args.symbol}")
        if not premium_rows:
            raise SystemExit(f"No premium index returned for {args.symbol}")
        book = book_rows[0]
        premium = premium_rows[0]
        snapshot = apply_market_snapshot(
            state=state,
            bid_price=float(book["bid_price"]),
            ask_price=float(book["ask_price"]),
            mark_price=float(premium["mark_price"]) if premium.get("mark_price") else None,
            funding_rate=float(premium["funding_rate"]) if premium.get("funding_rate") is not None else None,
            next_funding_time=int(premium["next_funding_time"]) if premium.get("next_funding_time") else None,
            ts=_utc_now(),
        )
        state_path.parent.mkdir(parents=True, exist_ok=True)
        state_path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
        snapshot_path.parent.mkdir(parents=True, exist_ok=True)
        snapshot_path.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2), encoding="utf-8")
        _print_summary(state, snapshot)
        if iteration + 1 < args.iterations and args.sleep_seconds > 0:
            time.sleep(args.sleep_seconds)


if __name__ == "__main__":
    main()
