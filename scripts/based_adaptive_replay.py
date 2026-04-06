#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import mean
from typing import Any, Callable

from grid_optimizer.data import fetch_futures_symbol_config, load_candles_from_csv
from grid_optimizer.loop_runner import (
    _build_parser as build_runner_parser,
    _planner_namespace,
    _convert_plan_orders_to_one_way,
    _isoformat,
    _safe_float,
    apply_hedge_position_controls,
    apply_hedge_position_notional_caps,
    resolve_adaptive_step_price,
)
from grid_optimizer.semi_auto_plan import (
    _round_to_nearest_step,
    build_hedge_micro_grid_plan,
    shift_center_price,
)
from grid_optimizer.types import Candle

FEE_RATE = 0.0002
DEFAULT_REFERENCE_JSON = "output/backtests/based_adaptive_step_replay_20260406.json"
ADAPTIVE_STEP_DEFAULTS = {
    "adaptive_step_enabled": True,
    "adaptive_step_30s_abs_return_ratio": 0.0028,
    "adaptive_step_30s_amplitude_ratio": 0.0035,
    "adaptive_step_1m_abs_return_ratio": 0.0045,
    "adaptive_step_1m_amplitude_ratio": 0.0065,
    "adaptive_step_3m_abs_return_ratio": 0.01,
    "adaptive_step_5m_abs_return_ratio": 0.014,
    "adaptive_step_max_scale": 3.0,
}


@dataclass
class InventoryLedger:
    long_qty: float = 0.0
    long_avg_price: float = 0.0
    short_qty: float = 0.0
    short_avg_price: float = 0.0


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Replay the BASED synthetic-neutral runner on local 1m candles, "
            "compare baseline / step-only / dual-linked adaptive controls, "
            "and report loss efficiency per 10k USDT traded."
        )
    )
    parser.add_argument("--symbol", type=str, default="BASEDUSDT")
    parser.add_argument("--candles-csv", type=str, default="data/BASEDUSDT_1m.csv")
    parser.add_argument(
        "--base-config-json",
        type=str,
        default="deploy/oracle/runtime_configs/basedusdt_volume_push_bard_v1.json",
    )
    parser.add_argument(
        "--reference-json",
        type=str,
        default=DEFAULT_REFERENCE_JSON,
        help="Optional saved replay json used to reuse window definitions and compare reference metrics.",
    )
    parser.add_argument(
        "--output-json",
        type=str,
        default="output/backtests/based_dual_linked_replay_20260406.json",
    )
    return parser.parse_args()


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _load_json(path: str | Path) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _build_runner_args(config: dict[str, Any]) -> argparse.Namespace:
    parser = build_runner_parser()
    args = parser.parse_args([])
    for key, value in config.items():
        setattr(args, key, value)
    args.symbol = str(getattr(args, "symbol", config.get("symbol", "BASEDUSDT"))).upper().strip()
    args.apply = False
    args.reset_state = False
    return args


def _merge_missing(target: dict[str, Any], source: dict[str, Any]) -> dict[str, Any]:
    merged = dict(target)
    for key, value in source.items():
        if key not in merged:
            merged[key] = value
    return merged


def _window_selector_candidates() -> dict[str, Callable[[Candle, datetime, datetime], bool]]:
    return {
        "open_ge_start_lt_end": lambda candle, start, end: start <= candle.open_time < end,
        "open_ge_start_le_end": lambda candle, start, end: start <= candle.open_time <= end,
        "close_gt_start_le_end": lambda candle, start, end: start < candle.close_time <= end,
        "close_ge_start_le_end": lambda candle, start, end: start <= candle.close_time <= end,
    }


def _select_window_candles(
    candles: list[Candle],
    *,
    start: datetime,
    end: datetime,
    expected_count: int | None = None,
) -> tuple[list[Candle], str]:
    candidates = _window_selector_candidates()
    if expected_count is not None:
        matching: list[tuple[str, list[Candle]]] = []
        for name, selector in candidates.items():
            chosen = [item for item in candles if selector(item, start, end)]
            if len(chosen) == expected_count:
                matching.append((name, chosen))
        if matching:
            name, chosen = matching[0]
            return chosen, name
    default_name = "open_ge_start_lt_end"
    default_selector = candidates[default_name]
    return [item for item in candles if default_selector(item, start, end)], default_name


def _compact_intrabar_points(candle: Candle) -> list[float]:
    if candle.close >= candle.open:
        raw = [candle.open, candle.low, candle.high, candle.close]
    else:
        raw = [candle.open, candle.high, candle.low, candle.close]
    points = [raw[0]]
    for price in raw[1:]:
        if abs(price - points[-1]) > 1e-12:
            points.append(price)
    return points


def _segment_starts(candle: Candle, point_count: int) -> list[datetime]:
    if point_count <= 1:
        return [candle.open_time]
    total_seconds = max((candle.close_time - candle.open_time).total_seconds(), 1.0)
    segment_count = point_count - 1
    starts: list[datetime] = []
    for index in range(segment_count):
        offset_seconds = total_seconds * index / segment_count
        starts.append(candle.open_time + timedelta(seconds=offset_seconds))
    return starts


def _latest_closed_guard_candle(candles: list[Candle], now: datetime) -> Candle | None:
    closed = [item for item in candles if item.close_time <= now]
    if not closed:
        return None
    return closed[-1]


def _assess_market_guard_local(
    *,
    candles: list[Candle],
    now: datetime,
    buy_pause_amp_trigger_ratio: float | None,
    buy_pause_down_return_trigger_ratio: float | None,
    short_cover_pause_amp_trigger_ratio: float | None,
    short_cover_pause_down_return_trigger_ratio: float | None,
    freeze_shift_abs_return_trigger_ratio: float | None,
) -> dict[str, Any]:
    guard = {
        "enabled": any(
            threshold is not None and threshold != 0
            for threshold in (
                buy_pause_amp_trigger_ratio,
                buy_pause_down_return_trigger_ratio,
                short_cover_pause_amp_trigger_ratio,
                short_cover_pause_down_return_trigger_ratio,
                freeze_shift_abs_return_trigger_ratio,
            )
        ),
        "available": False,
        "warning": None,
        "buy_pause_active": False,
        "short_cover_pause_active": False,
        "shift_frozen": False,
        "buy_pause_reasons": [],
        "short_cover_pause_reasons": [],
        "shift_freeze_reasons": [],
        "candle": None,
        "return_ratio": 0.0,
        "amplitude_ratio": 0.0,
    }
    if not guard["enabled"]:
        return guard
    candle = _latest_closed_guard_candle(candles, now)
    if candle is None:
        guard["warning"] = "no_closed_1m_candle"
        return guard

    open_price = max(float(candle.open), 0.0)
    high_price = max(float(candle.high), 0.0)
    low_price = max(float(candle.low), 0.0)
    close_price = max(float(candle.close), 0.0)
    return_ratio = ((close_price / open_price) - 1.0) if open_price > 0 else 0.0
    amplitude_ratio = ((high_price / low_price) - 1.0) if low_price > 0 else 0.0

    def _matches_extreme_down(amp_trigger: float | None, down_trigger: float | None) -> bool:
        return (
            amp_trigger is not None
            and amp_trigger > 0
            and down_trigger is not None
            and amplitude_ratio >= amp_trigger
            and return_ratio <= down_trigger
        )

    buy_pause_reasons: list[str] = []
    short_cover_pause_reasons: list[str] = []
    shift_freeze_reasons: list[str] = []
    if _matches_extreme_down(buy_pause_amp_trigger_ratio, buy_pause_down_return_trigger_ratio):
        buy_pause_reasons.append(
            f"1m_extreme_down_candle amp={amplitude_ratio * 100:.2f}% ret={return_ratio * 100:.2f}%"
        )
    if _matches_extreme_down(short_cover_pause_amp_trigger_ratio, short_cover_pause_down_return_trigger_ratio):
        short_cover_pause_reasons.append(
            f"1m_short_cover_pause amp={amplitude_ratio * 100:.2f}% ret={return_ratio * 100:.2f}%"
        )
    if (
        freeze_shift_abs_return_trigger_ratio is not None
        and freeze_shift_abs_return_trigger_ratio > 0
        and abs(return_ratio) >= freeze_shift_abs_return_trigger_ratio
    ):
        shift_freeze_reasons.append(f"1m_shift_freeze abs_ret={abs(return_ratio) * 100:.2f}%")

    guard.update(
        {
            "available": True,
            "buy_pause_active": bool(buy_pause_reasons),
            "short_cover_pause_active": bool(short_cover_pause_reasons),
            "shift_frozen": bool(shift_freeze_reasons),
            "buy_pause_reasons": buy_pause_reasons,
            "short_cover_pause_reasons": short_cover_pause_reasons,
            "shift_freeze_reasons": shift_freeze_reasons,
            "candle": {
                "open_time": candle.open_time.isoformat(),
                "close_time": candle.close_time.isoformat(),
                "open": open_price,
                "high": high_price,
                "low": low_price,
                "close": close_price,
            },
            "return_ratio": return_ratio,
            "amplitude_ratio": amplitude_ratio,
        }
    )
    return guard


def _initial_state(args: argparse.Namespace, symbol_info: dict[str, Any], mid_price: float) -> dict[str, Any]:
    planner_args = _planner_namespace(args)
    center_price = (
        float(planner_args.center_price)
        if getattr(planner_args, "center_price", None) is not None
        else float(mid_price)
    )
    center_price = _round_to_nearest_step(center_price, symbol_info.get("tick_size"))
    return {
        "version": 1,
        "created_at": _isoformat(_utc_now()),
        "updated_at": _isoformat(_utc_now()),
        "startup_pending": True,
        "startup_completed_at": None,
        "center_price": center_price,
        "last_mid_price": float(mid_price),
        "adaptive_step_history": [],
        "synthetic_ledger_resync_required": False,
    }


def _copy_namespace(args: argparse.Namespace) -> argparse.Namespace:
    return argparse.Namespace(**vars(args))


def _build_segment_plan(
    *,
    base_args: argparse.Namespace,
    state: dict[str, Any],
    symbol_info: dict[str, Any],
    now: datetime,
    price: float,
    history_candles: list[Candle],
    ledger: InventoryLedger,
) -> tuple[list[dict[str, Any]], dict[str, Any], dict[str, Any], argparse.Namespace]:
    effective_args = base_args
    adaptive_step = resolve_adaptive_step_price(
        state=state,
        now=now,
        mid_price=price,
        base_step_price=effective_args.step_price,
        tick_size=symbol_info.get("tick_size"),
        enabled=bool(getattr(effective_args, "adaptive_step_enabled", False)),
        window_30s_abs_return_ratio=getattr(effective_args, "adaptive_step_30s_abs_return_ratio", None),
        window_30s_amplitude_ratio=getattr(effective_args, "adaptive_step_30s_amplitude_ratio", None),
        window_1m_abs_return_ratio=getattr(effective_args, "adaptive_step_1m_abs_return_ratio", None),
        window_1m_amplitude_ratio=getattr(effective_args, "adaptive_step_1m_amplitude_ratio", None),
        window_3m_abs_return_ratio=getattr(effective_args, "adaptive_step_3m_abs_return_ratio", None),
        window_5m_abs_return_ratio=getattr(effective_args, "adaptive_step_5m_abs_return_ratio", None),
        max_scale=float(getattr(effective_args, "adaptive_step_max_scale", 1.0)),
        base_per_order_notional=getattr(effective_args, "per_order_notional", None),
        base_base_position_notional=getattr(effective_args, "base_position_notional", None),
        base_inventory_tier_start_notional=getattr(effective_args, "inventory_tier_start_notional", None),
        base_inventory_tier_end_notional=getattr(effective_args, "inventory_tier_end_notional", None),
        base_inventory_tier_per_order_notional=getattr(effective_args, "inventory_tier_per_order_notional", None),
        base_inventory_tier_base_position_notional=getattr(effective_args, "inventory_tier_base_position_notional", None),
        base_pause_buy_position_notional=getattr(effective_args, "pause_buy_position_notional", None),
        base_pause_short_position_notional=getattr(effective_args, "pause_short_position_notional", None),
        base_max_position_notional=getattr(effective_args, "max_position_notional", None),
        base_max_short_position_notional=getattr(effective_args, "max_short_position_notional", None),
        base_max_total_notional=getattr(effective_args, "max_total_notional", None),
        min_per_order_scale=float(getattr(effective_args, "adaptive_step_min_per_order_scale", 1.0)),
        min_position_limit_scale=float(getattr(effective_args, "adaptive_step_min_position_limit_scale", 1.0)),
    )
    if adaptive_step["controls_active"]:
        effective_args = _copy_namespace(effective_args)
        effective_args.step_price = float(adaptive_step["effective_step_price"])
        for field in (
            "per_order_notional",
            "base_position_notional",
            "inventory_tier_start_notional",
            "inventory_tier_end_notional",
            "inventory_tier_per_order_notional",
            "inventory_tier_base_position_notional",
            "pause_buy_position_notional",
            "pause_short_position_notional",
            "max_position_notional",
            "max_short_position_notional",
            "max_total_notional",
        ):
            effective_key = f"effective_{field}"
            if adaptive_step.get(effective_key) is not None:
                setattr(effective_args, field, float(adaptive_step[effective_key]))

    market_guard = _assess_market_guard_local(
        candles=history_candles,
        now=now,
        buy_pause_amp_trigger_ratio=getattr(effective_args, "buy_pause_amp_trigger_ratio", None),
        buy_pause_down_return_trigger_ratio=getattr(effective_args, "buy_pause_down_return_trigger_ratio", None),
        short_cover_pause_amp_trigger_ratio=getattr(effective_args, "short_cover_pause_amp_trigger_ratio", None),
        short_cover_pause_down_return_trigger_ratio=getattr(effective_args, "short_cover_pause_down_return_trigger_ratio", None),
        freeze_shift_abs_return_trigger_ratio=getattr(effective_args, "freeze_shift_abs_return_trigger_ratio", None),
    )
    center_price = float(state["center_price"])
    if not market_guard["shift_frozen"]:
        center_price, _ = shift_center_price(
            center_price=center_price,
            mid_price=price,
            step_price=effective_args.step_price,
            tick_size=symbol_info.get("tick_size"),
            down_trigger_steps=effective_args.down_trigger_steps,
            up_trigger_steps=effective_args.up_trigger_steps,
            shift_steps=effective_args.shift_steps,
        )
    state["center_price"] = center_price
    state["last_mid_price"] = float(price)
    state["updated_at"] = _isoformat(now)

    hedge_plan = build_hedge_micro_grid_plan(
        center_price=center_price,
        step_price=effective_args.step_price,
        buy_levels=int(effective_args.buy_levels),
        sell_levels=int(effective_args.sell_levels),
        per_order_notional=float(effective_args.per_order_notional),
        base_position_notional=float(effective_args.base_position_notional),
        bid_price=price,
        ask_price=price,
        tick_size=symbol_info.get("tick_size"),
        step_size=symbol_info.get("step_size"),
        min_qty=symbol_info.get("min_qty"),
        min_notional=symbol_info.get("min_notional"),
        current_long_qty=ledger.long_qty,
        current_short_qty=ledger.short_qty,
        startup_entry_multiplier=float(getattr(effective_args, "startup_entry_multiplier", 1.0)),
        startup_large_entry_active=bool(
            state.get("startup_pending") or (ledger.long_qty <= 1e-12 and ledger.short_qty <= 1e-12)
        ),
        buy_offset_steps=0.0,
        sell_offset_steps=0.0,
    )
    plan = _convert_plan_orders_to_one_way(hedge_plan)
    controls = apply_hedge_position_controls(
        plan=plan,
        current_long_qty=ledger.long_qty,
        current_short_qty=ledger.short_qty,
        mid_price=price,
        pause_long_position_notional=getattr(effective_args, "pause_buy_position_notional", None),
        pause_short_position_notional=getattr(effective_args, "pause_short_position_notional", None),
        min_mid_price_for_buys=getattr(effective_args, "min_mid_price_for_buys", None),
        external_long_pause=bool(market_guard["buy_pause_active"]),
        external_pause_reasons=list(market_guard["buy_pause_reasons"]),
        external_short_pause=False,
        external_short_pause_reasons=[],
    )
    apply_hedge_position_notional_caps(
        plan=plan,
        current_long_notional=controls["current_long_notional"],
        current_short_notional=controls["current_short_notional"],
        max_long_position_notional=getattr(effective_args, "max_position_notional", None),
        max_short_position_notional=getattr(effective_args, "max_short_position_notional", None),
        step_size=symbol_info.get("step_size"),
        min_qty=symbol_info.get("min_qty"),
        min_notional=symbol_info.get("min_notional"),
    )
    state["startup_pending"] = False
    return [*plan.get("bootstrap_orders", []), *plan.get("buy_orders", []), *plan.get("sell_orders", [])], adaptive_step, market_guard, effective_args


def _apply_fill(
    *,
    ledger: InventoryLedger,
    order: dict[str, Any],
    fill_price: float,
    fee_rate: float,
    totals: dict[str, Any],
    ts: datetime,
) -> None:
    qty = float(order.get("qty") or 0.0)
    role = str(order.get("role", "")).strip().lower()
    notional = float(fill_price) * qty
    fee = notional * fee_rate
    realized_pnl = 0.0
    if qty <= 0 or fill_price <= 0:
        return

    if role in {"bootstrap_long", "entry_long"}:
        total_cost = ledger.long_avg_price * ledger.long_qty + fill_price * qty
        ledger.long_qty += qty
        ledger.long_avg_price = total_cost / ledger.long_qty if ledger.long_qty > 0 else 0.0
    elif role == "take_profit_long":
        close_qty = min(qty, ledger.long_qty)
        realized_pnl = (fill_price - ledger.long_avg_price) * close_qty
        ledger.long_qty = max(ledger.long_qty - close_qty, 0.0)
        if ledger.long_qty <= 1e-12:
            ledger.long_qty = 0.0
            ledger.long_avg_price = 0.0
    elif role in {"bootstrap_short", "entry_short"}:
        total_proceeds = ledger.short_avg_price * ledger.short_qty + fill_price * qty
        ledger.short_qty += qty
        ledger.short_avg_price = total_proceeds / ledger.short_qty if ledger.short_qty > 0 else 0.0
    elif role == "take_profit_short":
        close_qty = min(qty, ledger.short_qty)
        realized_pnl = (ledger.short_avg_price - fill_price) * close_qty
        ledger.short_qty = max(ledger.short_qty - close_qty, 0.0)
        if ledger.short_qty <= 1e-12:
            ledger.short_qty = 0.0
            ledger.short_avg_price = 0.0
    else:
        return

    totals["fills"] += 1
    totals["gross_notional"] += notional
    totals["fees"] += fee
    totals["realized_pnl"] += realized_pnl
    totals["last_fill_ts"] = ts.isoformat()


def _mark_to_market(ledger: InventoryLedger, price: float) -> dict[str, float]:
    long_unrealized = (price - ledger.long_avg_price) * ledger.long_qty if ledger.long_qty > 0 else 0.0
    short_unrealized = (ledger.short_avg_price - price) * ledger.short_qty if ledger.short_qty > 0 else 0.0
    long_notional = ledger.long_qty * price
    short_notional = ledger.short_qty * price
    net_qty = ledger.long_qty - ledger.short_qty
    return {
        "unrealized_pnl": long_unrealized + short_unrealized,
        "long_notional": long_notional,
        "short_notional": short_notional,
        "net_notional_abs": abs(net_qty * price),
        "total_inventory_notional": abs(long_notional) + abs(short_notional),
        "net_qty": net_qty,
    }


def _segment_fill_candidates(
    orders: list[dict[str, Any]],
    *,
    start_price: float,
    end_price: float,
) -> list[dict[str, Any]]:
    touched: list[dict[str, Any]] = []
    for sequence, item in enumerate(orders):
        order = dict(item)
        order["sequence"] = sequence
        price = _safe_float(order.get("price"))
        side = str(order.get("side", "")).upper().strip()
        if price <= 0:
            continue
        if abs(price - start_price) <= 1e-12 and str(order.get("role", "")).startswith("bootstrap"):
            touched.append(order)
            continue
        if end_price < start_price and side == "BUY" and end_price <= price <= start_price:
            touched.append(order)
        elif end_price > start_price and side == "SELL" and start_price <= price <= end_price:
            touched.append(order)

    if end_price < start_price:
        touched.sort(key=lambda item: (-_safe_float(item.get("price")), int(item["sequence"])))
    elif end_price > start_price:
        touched.sort(key=lambda item: (_safe_float(item.get("price")), int(item["sequence"])))
    else:
        touched.sort(key=lambda item: int(item["sequence"]))
    return touched


def _replay_window(
    *,
    candles: list[Candle],
    args: argparse.Namespace,
    symbol_info: dict[str, Any],
    fee_rate: float,
) -> dict[str, Any]:
    if not candles:
        raise ValueError("window candles cannot be empty")

    state = _initial_state(args, symbol_info, candles[0].open)
    ledger = InventoryLedger()
    totals: dict[str, Any] = {
        "fills": 0,
        "gross_notional": 0.0,
        "realized_pnl": 0.0,
        "fees": 0.0,
        "last_fill_ts": None,
    }
    equity_curve: list[float] = [0.0]
    max_long_notional = 0.0
    max_short_notional = 0.0
    max_net_notional_abs = 0.0
    max_total_inventory_notional = 0.0
    adaptive_active_count = 0
    adaptive_step_prices: list[float] = []
    plan_count = 0

    for candle_index, candle in enumerate(candles):
        points = _compact_intrabar_points(candle)
        starts = _segment_starts(candle, len(points))
        for segment_index in range(len(points) - 1):
            now = starts[segment_index]
            start_price = float(points[segment_index])
            end_price = float(points[segment_index + 1])
            seen_candles = candles[:candle_index]
            orders, adaptive_step, _market_guard, effective_args = _build_segment_plan(
                base_args=args,
                state=state,
                symbol_info=symbol_info,
                now=now,
                price=start_price,
                history_candles=seen_candles,
                ledger=ledger,
            )
            plan_count += 1
            if adaptive_step.get("active"):
                adaptive_active_count += 1
            adaptive_step_prices.append(float(getattr(effective_args, "step_price", args.step_price)))

            for order in _segment_fill_candidates(orders, start_price=start_price, end_price=end_price):
                _apply_fill(
                    ledger=ledger,
                    order=order,
                    fill_price=_safe_float(order.get("price")),
                    fee_rate=fee_rate,
                    totals=totals,
                    ts=now,
                )

            mark = _mark_to_market(ledger, end_price)
            equity = totals["realized_pnl"] + mark["unrealized_pnl"] - totals["fees"]
            equity_curve.append(equity)
            max_long_notional = max(max_long_notional, mark["long_notional"])
            max_short_notional = max(max_short_notional, mark["short_notional"])
            max_net_notional_abs = max(max_net_notional_abs, mark["net_notional_abs"])
            max_total_inventory_notional = max(max_total_inventory_notional, mark["total_inventory_notional"])

    final_mark = _mark_to_market(ledger, candles[-1].close)
    net_pnl = totals["realized_pnl"] + final_mark["unrealized_pnl"] - totals["fees"]
    peak_equity = equity_curve[0]
    max_drawdown = 0.0
    for equity in equity_curve:
        peak_equity = max(peak_equity, equity)
        max_drawdown = max(max_drawdown, peak_equity - equity)

    gross_notional = float(totals["gross_notional"])
    net_per_10k = (net_pnl / gross_notional * 10000.0) if gross_notional > 0 else 0.0
    loss_per_10k = (max(-net_pnl, 0.0) / gross_notional * 10000.0) if gross_notional > 0 else 0.0
    return {
        "fills": int(totals["fills"]),
        "gross_notional": gross_notional,
        "realized_pnl": float(totals["realized_pnl"]),
        "fees": float(totals["fees"]),
        "unrealized_pnl": float(final_mark["unrealized_pnl"]),
        "net_pnl": float(net_pnl),
        "net_per_10k": float(net_per_10k),
        "loss_per_10k": float(loss_per_10k),
        "final_long_qty": float(ledger.long_qty),
        "final_short_qty": float(ledger.short_qty),
        "final_net_qty": float(final_mark["net_qty"]),
        "max_long_notional": float(max_long_notional),
        "max_short_notional": float(max_short_notional),
        "max_net_notional_abs": float(max_net_notional_abs),
        "max_total_inventory_notional": float(max_total_inventory_notional),
        "max_drawdown": float(max_drawdown),
        "adaptive_active_ratio": (adaptive_active_count / plan_count) if plan_count > 0 else 0.0,
        "adaptive_max_scale": float(getattr(args, "adaptive_step_max_scale", 1.0)),
        "avg_step_price": float(mean(adaptive_step_prices)) if adaptive_step_prices else float(args.step_price),
        "last_fill_ts": totals["last_fill_ts"],
        "missed_close_qty": 0.0,
    }


def _delta_report(current: dict[str, Any], baseline: dict[str, Any]) -> dict[str, float]:
    keys = [
        "net_pnl",
        "realized_pnl",
        "fees",
        "gross_notional",
        "max_long_notional",
        "max_short_notional",
        "max_net_notional_abs",
        "max_total_inventory_notional",
        "max_drawdown",
        "net_per_10k",
        "loss_per_10k",
    ]
    return {key: float(current.get(key, 0.0)) - float(baseline.get(key, 0.0)) for key in keys}


def _with_volume_retention(current: dict[str, Any], baseline: dict[str, Any]) -> dict[str, Any]:
    enriched = dict(current)
    baseline_gross = float(baseline.get("gross_notional", 0.0))
    current_gross = float(current.get("gross_notional", 0.0))
    enriched["volume_retention_vs_baseline"] = (current_gross / baseline_gross) if baseline_gross > 0 else 0.0
    return enriched


def main() -> None:
    cli_args = _parse_args()
    symbol = cli_args.symbol.upper().strip()
    candles = load_candles_from_csv(Path(cli_args.candles_csv))
    symbol_info = fetch_futures_symbol_config(symbol)
    base_config = _load_json(cli_args.base_config_json)
    reference = None
    reference_windows: dict[str, Any] = {}
    reference_path = Path(cli_args.reference_json)
    if reference_path.exists():
        reference = _load_json(reference_path)
        reference_windows = dict(reference.get("windows") or {})

    baseline_config = dict(base_config)
    baseline_config["adaptive_step_enabled"] = False
    baseline_config["adaptive_step_min_per_order_scale"] = 1.0
    baseline_config["adaptive_step_min_position_limit_scale"] = 1.0

    adaptive_defaults = dict(ADAPTIVE_STEP_DEFAULTS)
    if reference and isinstance(reference.get("config_adaptive"), dict):
        adaptive_defaults = _merge_missing(dict(reference["config_adaptive"]), ADAPTIVE_STEP_DEFAULTS)

    step_only_config = _merge_missing(dict(base_config), adaptive_defaults)
    step_only_config["adaptive_step_enabled"] = True
    step_only_config["adaptive_step_min_per_order_scale"] = 1.0
    step_only_config["adaptive_step_min_position_limit_scale"] = 1.0

    dual_linked_config = _merge_missing(dict(base_config), adaptive_defaults)
    dual_linked_config["adaptive_step_enabled"] = True
    dual_linked_config["adaptive_step_min_per_order_scale"] = float(
        dual_linked_config.get("adaptive_step_min_per_order_scale", 1.0)
    )
    dual_linked_config["adaptive_step_min_position_limit_scale"] = float(
        dual_linked_config.get("adaptive_step_min_position_limit_scale", 0.65)
    )

    output: dict[str, Any] = {
        "generated_at": _utc_now().isoformat(),
        "symbol": symbol,
        "fee_rate": FEE_RATE,
        "assumptions": [
            "使用 Binance 1m K 线，并按 intrabar 路径 open-low-high-close / open-high-low-close 拆成子段重放。",
            "每个子段按当前价格重建 synthetic_neutral 计划，并按限价被触达视为成交。",
            "包含中心价 shift、库存软暂停、名义上限裁单、adaptive step；未模拟真实盘口点差抖动与撮合排队。",
            "重点输出净收益、成交额、每万U净收益 / 损耗，以及相对 baseline 的成交额保留率。",
        ],
        "config_baseline": baseline_config,
        "config_step_only": step_only_config,
        "config_dual_linked": dual_linked_config,
        "windows": {},
    }

    if not reference_windows:
        reference_windows = {
            "recent_24h": {
                "start_local": "2026-04-05T08:00:00+08:00",
                "end_local": "2026-04-06T08:00:00+08:00",
            },
            "loss_hotspot_4h": {
                "start_local": "2026-04-05T23:00:00+08:00",
                "end_local": "2026-04-06T03:00:00+08:00",
            },
        }

    baseline_args = _build_runner_args(baseline_config)
    step_only_args = _build_runner_args(step_only_config)
    dual_linked_args = _build_runner_args(dual_linked_config)

    for window_name, window_ref in reference_windows.items():
        start = datetime.fromisoformat(str(window_ref["start_local"]))
        end = datetime.fromisoformat(str(window_ref["end_local"]))
        expected_count = int(window_ref.get("candles", 0) or 0) if isinstance(window_ref, dict) else 0
        window_candles, selector_name = _select_window_candles(
            candles,
            start=start.astimezone(timezone.utc),
            end=end.astimezone(timezone.utc),
            expected_count=expected_count or None,
        )
        baseline = _replay_window(
            candles=window_candles,
            args=baseline_args,
            symbol_info=symbol_info,
            fee_rate=FEE_RATE,
        )
        step_only = _with_volume_retention(
            _replay_window(
                candles=window_candles,
                args=step_only_args,
                symbol_info=symbol_info,
                fee_rate=FEE_RATE,
            ),
            baseline,
        )
        dual_linked = _with_volume_retention(
            _replay_window(
                candles=window_candles,
                args=dual_linked_args,
                symbol_info=symbol_info,
                fee_rate=FEE_RATE,
            ),
            baseline,
        )
        output["windows"][window_name] = {
            "start_local": start.isoformat(),
            "end_local": end.isoformat(),
            "candles": len(window_candles),
            "selector": selector_name,
            "baseline": baseline,
            "step_only": step_only,
            "dual_linked": dual_linked,
            "delta_step_only_minus_baseline": _delta_report(step_only, baseline),
            "delta_dual_linked_minus_baseline": _delta_report(dual_linked, baseline),
        }
        if reference and isinstance(reference.get("windows", {}).get(window_name), dict):
            ref_window = reference["windows"][window_name]
            output["windows"][window_name]["reference"] = {
                "baseline": ref_window.get("baseline"),
                "adaptive": ref_window.get("adaptive"),
            }
            output["windows"][window_name]["reference_delta_step_only"] = {
                "net_pnl": float(step_only.get("net_pnl", 0.0)) - float(((ref_window.get("adaptive") or {}).get("net_pnl") or 0.0)),
                "gross_notional": float(step_only.get("gross_notional", 0.0)) - float(((ref_window.get("adaptive") or {}).get("gross_notional") or 0.0)),
                "max_drawdown": float(step_only.get("max_drawdown", 0.0)) - float(((ref_window.get("adaptive") or {}).get("max_drawdown") or 0.0)),
            }

    output_path = Path(cli_args.output_json)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(output, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
