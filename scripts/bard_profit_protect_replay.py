#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from statistics import mean
from typing import Any

from grid_optimizer.data import fetch_futures_symbol_config, load_or_fetch_candles
from grid_optimizer.dry_run import _round_order_price, _round_order_qty
from grid_optimizer.loop_runner import (
    _build_parser as build_runner_parser,
    _convert_plan_orders_to_one_way,
    _safe_float,
    apply_hedge_position_controls,
    apply_hedge_position_notional_caps,
)
from grid_optimizer.semi_auto_plan import _round_to_nearest_step, shift_center_price
from grid_optimizer.types import Candle

FEE_RATE = 0.0002
LOCAL_TZ = timezone(timedelta(hours=8))
WINDOW_MINUTES = {
    "5m": 5,
    "15m": 15,
    "30m": 30,
    "1h": 60,
    "4h": 240,
    "1d": 1440,
}


@dataclass
class Ledger:
    long_qty: float = 0.0
    long_avg: float = 0.0
    short_qty: float = 0.0
    short_avg: float = 0.0
    long_lots: list[dict[str, float]] = field(default_factory=list)
    short_lots: list[dict[str, float]] = field(default_factory=list)


def clamp_signed(value: float, limit: float = 1.0) -> float:
    safe_limit = max(float(limit), 0.0)
    if safe_limit <= 0:
        return 0.0
    return max(-safe_limit, min(float(value), safe_limit))


def resolve_market_bias_offsets(
    *,
    enabled: bool,
    center_price: float,
    mid_price: float,
    step_price: float,
    market_guard_return_ratio: float,
    max_shift_steps: float,
    signal_steps: float,
    drift_weight: float,
    return_weight: float,
) -> dict[str, Any]:
    safe_max_shift = max(float(max_shift_steps), 0.0)
    safe_signal_steps = max(float(signal_steps), 0.01)
    safe_step_price = max(float(step_price), 0.0)
    safe_mid_price = max(float(mid_price), 0.0)
    drift_steps = ((float(mid_price) - float(center_price)) / safe_step_price) if safe_step_price > 0 else 0.0
    step_return_ratio = (safe_step_price / safe_mid_price) if safe_mid_price > 0 and safe_step_price > 0 else 0.0
    return_steps = (float(market_guard_return_ratio) / step_return_ratio) if step_return_ratio > 0 else 0.0
    safe_drift_weight = max(float(drift_weight), 0.0)
    safe_return_weight = max(float(return_weight), 0.0)
    total_weight = safe_drift_weight + safe_return_weight
    if total_weight <= 1e-12:
        safe_drift_weight = 1.0
        safe_return_weight = 0.0
        total_weight = 1.0
    normalized_drift_weight = safe_drift_weight / total_weight
    normalized_return_weight = safe_return_weight / total_weight
    drift_component = clamp_signed(drift_steps / safe_signal_steps)
    return_component = clamp_signed(return_steps / safe_signal_steps)
    bias_score = clamp_signed(
        (drift_component * normalized_drift_weight) + (return_component * normalized_return_weight)
    )
    shift_steps = bias_score * safe_max_shift
    regime = "neutral"
    if bias_score >= 0.15:
        regime = "strong"
    elif bias_score <= -0.15:
        regime = "weak"
    return {
        "enabled": bool(enabled),
        "active": bool(enabled and safe_max_shift > 0 and abs(shift_steps) > 1e-9),
        "regime": regime,
        "bias_score": bias_score,
        "shift_steps": shift_steps,
        "buy_offset_steps": -shift_steps,
        "sell_offset_steps": shift_steps,
        "drift_steps": drift_steps,
        "return_steps": return_steps,
        "step_return_ratio": step_return_ratio,
        "signal_steps": safe_signal_steps,
        "max_shift_steps": safe_max_shift,
    }


def resolve_market_bias_entry_pause(
    *,
    buy_pause_enabled: bool,
    short_pause_enabled: bool,
    market_bias: dict[str, Any] | None,
    weak_buy_pause_threshold: float,
    strong_short_pause_threshold: float,
) -> dict[str, Any]:
    buy_threshold = max(float(weak_buy_pause_threshold), 0.0)
    short_threshold = max(float(strong_short_pause_threshold), 0.0)
    score = _safe_float((market_bias or {}).get("bias_score"))
    regime = str(((market_bias or {}).get("regime", "") or "")).strip() or "neutral"
    buy_pause_active = bool(buy_pause_enabled and score <= (-buy_threshold))
    short_pause_active = bool(short_pause_enabled and score >= short_threshold)
    buy_reasons: list[str] = []
    short_reasons: list[str] = []
    if buy_pause_active:
        buy_reasons.append(f"market_bias_weak_buy_pause regime={regime} score={score:+.2f} <= -{buy_threshold:.2f}")
    if short_pause_active:
        short_reasons.append(
            f"market_bias_strong_short_pause regime={regime} score={score:+.2f} >= +{short_threshold:.2f}"
        )
    return {
        "enabled": bool(buy_pause_enabled or short_pause_enabled),
        "buy_pause_active": buy_pause_active,
        "buy_pause_reasons": buy_reasons,
        "short_pause_active": short_pause_active,
        "short_pause_reasons": short_reasons,
        "regime": regime,
        "bias_score": score,
    }


def normalize_volatility_trigger_window(value: Any) -> str:
    key = str(value or "1h").strip().lower()
    return key if key in WINDOW_MINUTES else "1h"


def normalize_runner_volatility_trigger_config(config: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(config)
    normalized["volatility_trigger_enabled"] = bool(normalized.get("volatility_trigger_enabled", True))
    normalized["volatility_trigger_window"] = normalize_volatility_trigger_window(
        normalized.get("volatility_trigger_window")
    )
    for key in {"volatility_trigger_amplitude_ratio", "volatility_trigger_abs_return_ratio"}:
        value = normalized.get(key)
        if value in {"", None}:
            normalized[key] = None
            continue
        threshold = float(value)
        if threshold <= 0:
            raise ValueError(f"{key} must be > 0")
        normalized[key] = threshold
    normalized["volatility_trigger_stop_cancel_open_orders"] = bool(
        normalized.get("volatility_trigger_stop_cancel_open_orders", True)
    )
    normalized["volatility_trigger_stop_close_all_positions"] = bool(
        normalized.get("volatility_trigger_stop_close_all_positions", True)
    )
    if normalized["volatility_trigger_stop_close_all_positions"]:
        normalized["volatility_trigger_stop_cancel_open_orders"] = True
    return normalized


def evaluate_runner_volatility_trigger_signal(
    config: dict[str, Any],
    *,
    current_amplitude_ratio: float,
    current_return_ratio: float,
) -> dict[str, Any]:
    normalized_config = normalize_runner_volatility_trigger_config(config)
    amplitude_threshold = normalized_config.get("volatility_trigger_amplitude_ratio")
    abs_return_threshold = normalized_config.get("volatility_trigger_abs_return_ratio")
    matched_reasons: list[str] = []
    if amplitude_threshold is not None and float(current_amplitude_ratio) >= float(amplitude_threshold):
        matched_reasons.append("amplitude_above_threshold")
    if abs_return_threshold is not None and abs(float(current_return_ratio)) >= float(abs_return_threshold):
        matched_reasons.append("abs_return_above_threshold")
    return {
        "hit": bool(matched_reasons),
        "matched_reasons": matched_reasons,
        "amplitude_threshold": amplitude_threshold,
        "abs_return_threshold": abs_return_threshold,
    }


def resolve_volatility_trigger_action(
    config: dict[str, Any],
    *,
    current_amplitude_ratio: float,
    current_return_ratio: float,
    runner_running: bool,
    paused_by_trigger: bool,
    flatten_running: bool = False,
) -> dict[str, Any]:
    normalized_config = normalize_runner_volatility_trigger_config(config)
    if not normalized_config.get("volatility_trigger_enabled"):
        return {"action": None, "reason": "disabled"}
    signal_summary = evaluate_runner_volatility_trigger_signal(
        normalized_config,
        current_amplitude_ratio=current_amplitude_ratio,
        current_return_ratio=current_return_ratio,
    )
    if flatten_running and not runner_running:
        return {"action": None, "reason": "flatten_running", **signal_summary}
    if runner_running:
        if signal_summary["hit"]:
            return {"action": "stop", "reason": "volatility_above_threshold", **signal_summary}
        return {"action": None, "reason": "runner_running", **signal_summary}
    if paused_by_trigger:
        if signal_summary["hit"]:
            return {"action": None, "reason": "waiting_for_volatility_cooldown", **signal_summary}
        return {"action": "start", "reason": "volatility_back_within_threshold", **signal_summary}
    if signal_summary["hit"]:
        return {"action": None, "reason": "volatility_above_threshold", **signal_summary}
    return {"action": None, "reason": "runner_stopped", **signal_summary}


def window_price_stats(candles: list[Candle], now: datetime, window_minutes: int) -> dict[str, float]:
    closed = [item for item in candles if item.close_time <= now]
    if not closed:
        return {"amplitude_ratio": 0.0, "return_ratio": 0.0}
    window_start = now - timedelta(minutes=max(int(window_minutes), 1))
    selected = [item for item in closed if item.close_time > window_start]
    if not selected:
        selected = [closed[-1]]
    high_price = max(float(item.high) for item in selected)
    low_price = min(float(item.low) for item in selected)
    open_price = float(selected[0].open)
    close_price = float(selected[-1].close)
    amplitude_ratio = (high_price / low_price - 1.0) if low_price > 0 else 0.0
    return_ratio = (close_price / open_price - 1.0) if open_price > 0 else 0.0
    return {"amplitude_ratio": amplitude_ratio, "return_ratio": return_ratio}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", default="BARDUSDT")
    parser.add_argument("--cache-dir", default="data")
    parser.add_argument("--output-json", required=True)
    parser.add_argument("--configs", nargs="+", required=True)
    parser.add_argument("--refresh", action="store_true")
    return parser.parse_args()


def load_json(path: str | Path) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def build_args(config: dict[str, Any]) -> argparse.Namespace:
    parser = build_runner_parser()
    args = parser.parse_args([])
    for key, value in config.items():
        setattr(args, key, value)
    args.apply = False
    args.reset_state = False
    args.symbol = str(config.get("symbol") or args.symbol).upper().strip()
    return args


def compact_points(candle: Candle) -> list[float]:
    if candle.close >= candle.open:
        raw = [candle.open, candle.low, candle.high, candle.close]
    else:
        raw = [candle.open, candle.high, candle.low, candle.close]
    points = [float(raw[0])]
    for item in raw[1:]:
        price = float(item)
        if abs(price - points[-1]) > 1e-12:
            points.append(price)
    return points


def segment_starts(candle: Candle, point_count: int) -> list[datetime]:
    if point_count <= 1:
        return [candle.open_time]
    total_seconds = max((candle.close_time - candle.open_time).total_seconds(), 1.0)
    return [
        candle.open_time + timedelta(seconds=total_seconds * index / (point_count - 1))
        for index in range(point_count - 1)
    ]


def latest_closed(candles: list[Candle], now: datetime) -> Candle | None:
    previous = [item for item in candles if item.close_time <= now]
    return previous[-1] if previous else None


def market_guard_from_previous(candles: list[Candle], now: datetime, args: argparse.Namespace) -> dict[str, Any]:
    candle = latest_closed(candles, now)
    result = {
        "buy_pause_active": False,
        "buy_pause_reasons": [],
        "short_cover_pause_active": False,
        "short_cover_pause_reasons": [],
        "shift_frozen": False,
        "return_ratio": 0.0,
        "amplitude_ratio": 0.0,
    }
    if candle is None or candle.open <= 0 or candle.low <= 0:
        return result
    return_ratio = candle.close / candle.open - 1.0
    amplitude_ratio = candle.high / candle.low - 1.0
    result["return_ratio"] = return_ratio
    result["amplitude_ratio"] = amplitude_ratio

    buy_amp = getattr(args, "buy_pause_amp_trigger_ratio", None)
    buy_down = getattr(args, "buy_pause_down_return_trigger_ratio", None)
    short_amp = getattr(args, "short_cover_pause_amp_trigger_ratio", None)
    short_down = getattr(args, "short_cover_pause_down_return_trigger_ratio", None)
    freeze_abs = getattr(args, "freeze_shift_abs_return_trigger_ratio", None)
    if buy_amp and buy_down and amplitude_ratio >= buy_amp and return_ratio <= buy_down:
        result["buy_pause_active"] = True
        result["buy_pause_reasons"] = ["1m_extreme_down_candle"]
    if short_amp and short_down and amplitude_ratio >= short_amp and return_ratio <= short_down:
        result["short_cover_pause_active"] = True
        result["short_cover_pause_reasons"] = ["1m_short_cover_pause"]
    if freeze_abs and abs(return_ratio) >= freeze_abs:
        result["shift_frozen"] = True
    return result


def order_price(order: dict[str, Any]) -> float:
    return _safe_float(order.get("price"))


def touched_orders(orders: list[dict[str, Any]], start_price: float, end_price: float) -> list[dict[str, Any]]:
    touched: list[dict[str, Any]] = []
    for sequence, raw in enumerate(orders):
        order = dict(raw)
        order["sequence"] = sequence
        price = order_price(order)
        side = str(order.get("side", "")).upper()
        if price <= 0:
            continue
        if str(order.get("role", "")).startswith("bootstrap") and abs(price - start_price) <= 1e-12:
            touched.append(order)
        elif end_price < start_price and side == "BUY" and end_price <= price <= start_price:
            touched.append(order)
        elif end_price > start_price and side == "SELL" and start_price <= price <= end_price:
            touched.append(order)
    if end_price < start_price:
        touched.sort(key=lambda item: (-order_price(item), int(item["sequence"])))
    else:
        touched.sort(key=lambda item: (order_price(item), int(item["sequence"])))
    return touched


def normalize_lots(lots: list[dict[str, Any]] | None) -> list[dict[str, float]]:
    normalized: list[dict[str, float]] = []
    for item in lots or []:
        if not isinstance(item, dict):
            continue
        qty = max(_safe_float(item.get("qty")), 0.0)
        price = max(_safe_float(item.get("price")), 0.0)
        if qty <= 1e-12 or price <= 0:
            continue
        normalized.append({"qty": qty, "price": price})
    return normalized


def lots_book(lots: list[dict[str, float]]) -> tuple[float, float]:
    total_qty = sum(item["qty"] for item in lots)
    if total_qty <= 1e-12:
        return 0.0, 0.0
    total_cost = sum(item["qty"] * item["price"] for item in lots)
    return total_qty, total_cost / total_qty


def consume_lots_lifo(lots: list[dict[str, float]], qty: float) -> list[dict[str, float]]:
    remaining = max(float(qty), 0.0)
    updated = [{"qty": float(item["qty"]), "price": float(item["price"])} for item in lots]
    while updated and remaining > 1e-12:
        tail = updated[-1]
        if tail["qty"] <= remaining + 1e-12:
            remaining = max(remaining - tail["qty"], 0.0)
            updated.pop()
            continue
        tail["qty"] -= remaining
        remaining = 0.0
    return updated


def apply_fill(ledger: Ledger, order: dict[str, Any], fill_price: float, totals: dict[str, Any]) -> None:
    qty = _safe_float(order.get("qty"))
    role = str(order.get("role", "")).lower()
    if qty <= 0 or fill_price <= 0:
        return
    notional = qty * fill_price
    fee = notional * FEE_RATE
    realized = 0.0
    if role in {"bootstrap_long", "entry_long"}:
        ledger.long_lots.append({"qty": qty, "price": fill_price})
        ledger.long_qty, ledger.long_avg = lots_book(ledger.long_lots)
    elif role == "take_profit_long":
        close_qty = min(qty, ledger.long_qty)
        realized = (fill_price - ledger.long_avg) * close_qty
        ledger.long_lots = consume_lots_lifo(ledger.long_lots, close_qty)
        ledger.long_qty, ledger.long_avg = lots_book(ledger.long_lots)
    elif role in {"bootstrap_short", "entry_short"}:
        ledger.short_lots.append({"qty": qty, "price": fill_price})
        ledger.short_qty, ledger.short_avg = lots_book(ledger.short_lots)
    elif role == "take_profit_short":
        close_qty = min(qty, ledger.short_qty)
        realized = (ledger.short_avg - fill_price) * close_qty
        ledger.short_lots = consume_lots_lifo(ledger.short_lots, close_qty)
        ledger.short_qty, ledger.short_avg = lots_book(ledger.short_lots)
    else:
        return
    totals["fills"] += 1
    totals["gross_notional"] += notional
    totals["fees"] += fee
    totals["realized_pnl"] += realized


def flatten_ledger(ledger: Ledger, fill_price: float, totals: dict[str, Any]) -> None:
    if fill_price <= 0:
        return
    if ledger.long_qty > 1e-12:
        notional = ledger.long_qty * fill_price
        totals["fills"] += 1
        totals["gross_notional"] += notional
        totals["fees"] += notional * FEE_RATE
        totals["realized_pnl"] += (fill_price - ledger.long_avg) * ledger.long_qty
        ledger.long_qty = 0.0
        ledger.long_avg = 0.0
        ledger.long_lots = []
    if ledger.short_qty > 1e-12:
        notional = ledger.short_qty * fill_price
        totals["fills"] += 1
        totals["gross_notional"] += notional
        totals["fees"] += notional * FEE_RATE
        totals["realized_pnl"] += (ledger.short_avg - fill_price) * ledger.short_qty
        ledger.short_qty = 0.0
        ledger.short_avg = 0.0
        ledger.short_lots = []


def mark(ledger: Ledger, price: float) -> dict[str, float]:
    long_unrealized = (price - ledger.long_avg) * ledger.long_qty if ledger.long_qty > 0 else 0.0
    short_unrealized = (ledger.short_avg - price) * ledger.short_qty if ledger.short_qty > 0 else 0.0
    return {
        "long_notional": ledger.long_qty * price,
        "short_notional": ledger.short_qty * price,
        "net_qty": ledger.long_qty - ledger.short_qty,
        "unrealized_pnl": long_unrealized + short_unrealized,
    }


def build_order(
    *,
    side: str,
    price: float,
    qty: float,
    level: int,
    role: str,
    position_side: str,
) -> dict[str, Any]:
    return {
        "side": side.upper(),
        "price": float(price),
        "qty": float(qty),
        "quantity": float(qty),
        "notional": float(price) * float(qty),
        "level": int(level),
        "role": role,
        "position_side": position_side.upper(),
    }


def build_profit_protected_hedge_plan(
    *,
    center_price: float,
    step_price: float,
    buy_levels: int,
    sell_levels: int,
    per_order_notional: float,
    base_position_notional: float,
    bid_price: float,
    ask_price: float,
    tick_size: float | None,
    step_size: float | None,
    min_qty: float | None,
    min_notional: float | None,
    current_long_qty: float,
    current_short_qty: float,
    current_long_lots: list[dict[str, Any]] | None = None,
    current_short_lots: list[dict[str, Any]] | None = None,
    startup_entry_multiplier: float = 1.0,
    startup_large_entry_active: bool = False,
    buy_offset_steps: float = 0.0,
    sell_offset_steps: float = 0.0,
    profit_protect_notional_threshold: float = 0.0,
    entry_long_paused: bool = False,
    entry_short_paused: bool = False,
) -> dict[str, Any]:
    if center_price <= 0 or step_price <= 0:
        raise ValueError("center_price and step_price must be > 0")
    buy_orders: list[dict[str, Any]] = []
    sell_orders: list[dict[str, Any]] = []
    current_long_qty = max(float(current_long_qty), 0.0)
    current_short_qty = max(float(current_short_qty), 0.0)
    current_long_lots = normalize_lots(current_long_lots)
    current_short_lots = normalize_lots(current_short_lots)
    startup_multiplier = max(float(startup_entry_multiplier), 1.0)
    buy_offset_steps = float(buy_offset_steps)
    sell_offset_steps = float(sell_offset_steps)
    mid_price = (
        (float(bid_price) + float(ask_price)) / 2.0
        if bid_price is not None and ask_price is not None and bid_price > 0 and ask_price > 0
        else float(center_price)
    )
    profit_threshold = max(float(profit_protect_notional_threshold), 0.0)
    profit_protect_long_active = (
        profit_threshold > 0
        and current_long_qty > 0
        and current_long_qty * mid_price < profit_threshold
        and bool(current_long_lots)
    )
    profit_protect_short_active = (
        profit_threshold > 0
        and current_short_qty > 0
        and current_short_qty * mid_price < profit_threshold
        and bool(current_short_lots)
    )

    def entry_notional(*, level: int, current_qty: float) -> float:
        if startup_large_entry_active and level == 1 and current_qty <= 1e-12:
            return per_order_notional * startup_multiplier
        return per_order_notional

    def buy_price(level: int) -> float:
        distance_steps = max((float(level) - 1.0) + buy_offset_steps, 0.0)
        price_raw = float(Decimal(str(bid_price)) - (Decimal(str(distance_steps)) * Decimal(str(step_price))))
        return _round_order_price(price_raw, tick_size, "BUY")

    def sell_price(level: int) -> float:
        distance_steps = max((float(level) - 1.0) + sell_offset_steps, 0.0)
        price_raw = float(Decimal(str(ask_price)) + (Decimal(str(distance_steps)) * Decimal(str(step_price))))
        return _round_order_price(price_raw, tick_size, "SELL")

    if profit_protect_short_active:
        latest_short_lot = current_short_lots[-1]
        remaining_short_exit_qty = _round_order_qty(min(current_short_qty, latest_short_lot["qty"]), step_size)
        for level in range(1, buy_levels + 1):
            if remaining_short_exit_qty <= 0:
                break
            price = buy_price(level)
            if price >= latest_short_lot["price"]:
                continue
            desired_qty = _round_order_qty(per_order_notional / price, step_size)
            qty = _round_order_qty(min(desired_qty, remaining_short_exit_qty), step_size)
            notional = price * qty
            if price >= ask_price or qty <= 0:
                continue
            if min_qty is not None and qty < min_qty:
                continue
            if min_notional is not None and notional < min_notional:
                continue
            buy_orders.append(
                build_order(
                    side="BUY",
                    price=price,
                    qty=qty,
                    level=level,
                    role="take_profit_short",
                    position_side="SHORT",
                )
            )
            break
    else:
        remaining_short_exit_qty = _round_order_qty(current_short_qty, step_size)
        for level in range(1, buy_levels + 1):
            if remaining_short_exit_qty <= 0:
                break
            price = buy_price(level)
            desired_qty = _round_order_qty(per_order_notional / price, step_size)
            qty = _round_order_qty(min(desired_qty, remaining_short_exit_qty), step_size)
            notional = price * qty
            if price >= ask_price or qty <= 0:
                continue
            if min_qty is not None and qty < min_qty:
                continue
            if min_notional is not None and notional < min_notional:
                continue
            buy_orders.append(
                build_order(
                    side="BUY",
                    price=price,
                    qty=qty,
                    level=level,
                    role="take_profit_short",
                    position_side="SHORT",
                )
            )
            remaining_short_exit_qty = max(remaining_short_exit_qty - qty, 0.0)

    take_profit_short_price_keys = {
        f"{item['side']}:{item['price']:.10f}" for item in buy_orders if item["role"] == "take_profit_short"
    }
    if not profit_protect_short_active and not bool(entry_long_paused):
        entry_buy_max_level = buy_levels + (1 if current_short_qty > 0 else 0)
        for level in range(1, entry_buy_max_level + 1):
            price = buy_price(level)
            if current_short_qty > 0 and f"BUY:{price:.10f}" in take_profit_short_price_keys:
                continue
            qty = _round_order_qty(entry_notional(level=level, current_qty=current_long_qty) / price, step_size)
            notional = price * qty
            if price >= ask_price or qty <= 0:
                continue
            if min_qty is not None and qty < min_qty:
                continue
            if min_notional is not None and notional < min_notional:
                continue
            buy_orders.append(
                build_order(
                    side="BUY",
                    price=price,
                    qty=qty,
                    level=level,
                    role="entry_long",
                    position_side="LONG",
                )
            )
            if current_short_qty > 0:
                break

    if profit_protect_long_active:
        latest_long_lot = current_long_lots[-1]
        remaining_long_exit_qty = _round_order_qty(min(current_long_qty, latest_long_lot["qty"]), step_size)
        for level in range(1, sell_levels + 1):
            if remaining_long_exit_qty <= 0:
                break
            price = sell_price(level)
            if price <= latest_long_lot["price"]:
                continue
            desired_qty = _round_order_qty(per_order_notional / price, step_size)
            qty = _round_order_qty(min(desired_qty, remaining_long_exit_qty), step_size)
            notional = price * qty
            if price <= bid_price or qty <= 0:
                continue
            if min_qty is not None and qty < min_qty:
                continue
            if min_notional is not None and notional < min_notional:
                continue
            sell_orders.append(
                build_order(
                    side="SELL",
                    price=price,
                    qty=qty,
                    level=level,
                    role="take_profit_long",
                    position_side="LONG",
                )
            )
            break
    else:
        remaining_long_exit_qty = _round_order_qty(current_long_qty, step_size)
        for level in range(1, sell_levels + 1):
            if remaining_long_exit_qty <= 0:
                break
            price = sell_price(level)
            desired_qty = _round_order_qty(per_order_notional / price, step_size)
            qty = _round_order_qty(min(desired_qty, remaining_long_exit_qty), step_size)
            notional = price * qty
            if price <= bid_price or qty <= 0:
                continue
            if min_qty is not None and qty < min_qty:
                continue
            if min_notional is not None and notional < min_notional:
                continue
            sell_orders.append(
                build_order(
                    side="SELL",
                    price=price,
                    qty=qty,
                    level=level,
                    role="take_profit_long",
                    position_side="LONG",
                )
            )
            remaining_long_exit_qty = max(remaining_long_exit_qty - qty, 0.0)

    take_profit_long_price_keys = {
        f"{item['side']}:{item['price']:.10f}" for item in sell_orders if item["role"] == "take_profit_long"
    }
    if not profit_protect_long_active and not bool(entry_short_paused):
        entry_sell_max_level = sell_levels + (1 if current_long_qty > 0 else 0)
        for level in range(1, entry_sell_max_level + 1):
            price = sell_price(level)
            if current_long_qty > 0 and f"SELL:{price:.10f}" in take_profit_long_price_keys:
                continue
            qty = _round_order_qty(entry_notional(level=level, current_qty=current_short_qty) / price, step_size)
            notional = price * qty
            if price <= bid_price or qty <= 0:
                continue
            if min_qty is not None and qty < min_qty:
                continue
            if min_notional is not None and notional < min_notional:
                continue
            sell_orders.append(
                build_order(
                    side="SELL",
                    price=price,
                    qty=qty,
                    level=level,
                    role="entry_short",
                    position_side="SHORT",
                )
            )
            if current_long_qty > 0:
                break

    target_long_base_qty = _round_order_qty(base_position_notional / center_price, step_size)
    target_short_base_qty = _round_order_qty(base_position_notional / center_price, step_size)
    bootstrap_long_qty = max(target_long_base_qty - current_long_qty, 0.0)
    bootstrap_short_qty = max(target_short_base_qty - current_short_qty, 0.0)
    bootstrap_orders: list[dict[str, Any]] = []
    if bootstrap_long_qty > 0:
        bootstrap_price = _round_order_price(bid_price, tick_size, "BUY")
        bootstrap_qty = _round_order_qty(bootstrap_long_qty, step_size)
        bootstrap_notional = bootstrap_price * bootstrap_qty
        if bootstrap_qty > 0 and (
            (min_qty is None or bootstrap_qty >= min_qty)
            and (min_notional is None or bootstrap_notional >= min_notional)
        ):
            bootstrap_orders.append(
                build_order(
                    side="BUY",
                    price=bootstrap_price,
                    qty=bootstrap_qty,
                    level=0,
                    role="bootstrap_long",
                    position_side="LONG",
                )
            )
    if bootstrap_short_qty > 0:
        bootstrap_price = _round_order_price(ask_price, tick_size, "SELL")
        bootstrap_qty = _round_order_qty(bootstrap_short_qty, step_size)
        bootstrap_notional = bootstrap_price * bootstrap_qty
        if bootstrap_qty > 0 and (
            (min_qty is None or bootstrap_qty >= min_qty)
            and (min_notional is None or bootstrap_notional >= min_notional)
        ):
            bootstrap_orders.append(
                build_order(
                    side="SELL",
                    price=bootstrap_price,
                    qty=bootstrap_qty,
                    level=0,
                    role="bootstrap_short",
                    position_side="SHORT",
                )
            )
    return {
        "buy_orders": buy_orders,
        "sell_orders": sell_orders,
        "bootstrap_orders": bootstrap_orders,
        "target_long_base_qty": target_long_base_qty,
        "target_short_base_qty": target_short_base_qty,
        "bootstrap_long_qty": bootstrap_long_qty,
        "bootstrap_short_qty": bootstrap_short_qty,
    }


def build_orders(
    *,
    args: argparse.Namespace,
    state: dict[str, Any],
    symbol_info: dict[str, Any],
    history: list[Candle],
    now: datetime,
    price: float,
    ledger: Ledger,
    startup_pending: bool,
) -> tuple[list[dict[str, Any]], dict[str, Any], bool]:
    guard = market_guard_from_previous(history, now, args)
    center_price = float(state["center_price"])
    if not guard["shift_frozen"]:
        center_price, _moves = shift_center_price(
            center_price=center_price,
            mid_price=price,
            step_price=float(args.step_price),
            tick_size=symbol_info.get("tick_size"),
            down_trigger_steps=float(args.down_trigger_steps),
            up_trigger_steps=float(args.up_trigger_steps),
            shift_steps=int(args.shift_steps),
        )
    state["center_price"] = center_price

    bias = resolve_market_bias_offsets(
        enabled=bool(getattr(args, "market_bias_enabled", False)),
        center_price=center_price,
        mid_price=price,
        step_price=float(args.step_price),
        market_guard_return_ratio=float(guard.get("return_ratio") or 0.0),
        max_shift_steps=float(getattr(args, "market_bias_max_shift_steps", 0.0)),
        signal_steps=float(getattr(args, "market_bias_signal_steps", 3.0)),
        drift_weight=float(getattr(args, "market_bias_drift_weight", 0.45)),
        return_weight=float(getattr(args, "market_bias_return_weight", 0.55)),
    )
    pause = resolve_market_bias_entry_pause(
        buy_pause_enabled=bool(getattr(args, "market_bias_weak_buy_pause_enabled", False)),
        short_pause_enabled=bool(getattr(args, "market_bias_strong_short_pause_enabled", False)),
        market_bias=bias,
        weak_buy_pause_threshold=float(getattr(args, "market_bias_weak_buy_pause_threshold", 0.75)),
        strong_short_pause_threshold=float(getattr(args, "market_bias_strong_short_pause_threshold", 0.75)),
    )
    hedge_plan = build_profit_protected_hedge_plan(
        center_price=center_price,
        step_price=float(args.step_price),
        buy_levels=int(args.buy_levels),
        sell_levels=int(args.sell_levels),
        per_order_notional=float(args.per_order_notional),
        base_position_notional=float(args.base_position_notional),
        bid_price=price,
        ask_price=price,
        tick_size=symbol_info.get("tick_size"),
        step_size=symbol_info.get("step_size"),
        min_qty=symbol_info.get("min_qty"),
        min_notional=symbol_info.get("min_notional"),
        current_long_qty=ledger.long_qty,
        current_short_qty=ledger.short_qty,
        current_long_lots=ledger.long_lots,
        current_short_lots=ledger.short_lots,
        startup_entry_multiplier=float(getattr(args, "startup_entry_multiplier", 1.0)),
        startup_large_entry_active=bool(startup_pending or (ledger.long_qty <= 1e-12 and ledger.short_qty <= 1e-12)),
        buy_offset_steps=float(bias.get("buy_offset_steps") or 0.0),
        sell_offset_steps=float(bias.get("sell_offset_steps") or 0.0),
        profit_protect_notional_threshold=float(getattr(args, "profit_protect_notional_threshold", 0.0)),
        entry_long_paused=bool(guard["buy_pause_active"] or pause["buy_pause_active"]),
        entry_short_paused=bool(pause["short_pause_active"]),
    )
    plan = _convert_plan_orders_to_one_way(hedge_plan)
    controls = apply_hedge_position_controls(
        plan=plan,
        current_long_qty=ledger.long_qty,
        current_short_qty=ledger.short_qty,
        mid_price=price,
        pause_long_position_notional=float(args.pause_buy_position_notional),
        pause_short_position_notional=float(args.pause_short_position_notional),
        min_mid_price_for_buys=getattr(args, "min_mid_price_for_buys", None),
        external_long_pause=bool(guard["buy_pause_active"] or pause["buy_pause_active"]),
        external_pause_reasons=list(guard["buy_pause_reasons"]) + list(pause["buy_pause_reasons"]),
        external_short_pause=bool(pause["short_pause_active"]),
        external_short_pause_reasons=list(pause["short_pause_reasons"]),
    )
    cap = apply_hedge_position_notional_caps(
        plan=plan,
        current_long_notional=controls["current_long_notional"],
        current_short_notional=controls["current_short_notional"],
        max_long_position_notional=float(args.max_position_notional),
        max_short_position_notional=float(args.max_short_position_notional),
        step_size=symbol_info.get("step_size"),
        min_qty=symbol_info.get("min_qty"),
        min_notional=symbol_info.get("min_notional"),
    )
    orders = [*plan.get("bootstrap_orders", []), *plan.get("buy_orders", []), *plan.get("sell_orders", [])]
    return orders, {
        "bias_score": float(bias.get("bias_score") or 0.0),
        "bias_shift_steps": float(bias.get("shift_steps") or 0.0),
        "buy_pause": bool(pause["buy_pause_active"] or guard["buy_pause_active"]),
        "short_pause": bool(pause["short_pause_active"]),
        "planned_buy_notional": float(cap.get("planned_buy_notional") or 0.0),
        "planned_short_notional": float(cap.get("planned_short_notional") or 0.0),
    }, False


def replay(candles: list[Candle], config: dict[str, Any], symbol_info: dict[str, Any]) -> dict[str, Any]:
    args = build_args(config)
    center = _round_to_nearest_step(candles[0].open, symbol_info.get("tick_size"))
    state = {"center_price": center}
    ledger = Ledger()
    totals = {"fills": 0, "gross_notional": 0.0, "fees": 0.0, "realized_pnl": 0.0}
    equity_curve = [0.0]
    max_long = max_short = max_abs_net = max_total_inventory = 0.0
    bias_scores: list[float] = []
    buy_pause_count = short_pause_count = plan_count = 0
    volatility_stop_count = 0
    volatility_resume_count = 0
    volatility_paused = False
    startup_pending = True

    for idx, candle in enumerate(candles):
        points = compact_points(candle)
        starts = segment_starts(candle, len(points))
        for segment_index in range(len(points) - 1):
            now = starts[segment_index]
            start_price = points[segment_index]
            end_price = points[segment_index + 1]
            volatility_config = normalize_runner_volatility_trigger_config(vars(args))
            if volatility_config.get("volatility_trigger_enabled"):
                stats = window_price_stats(candles[:idx], now, WINDOW_MINUTES[volatility_config["volatility_trigger_window"]])
                volatility_action = resolve_volatility_trigger_action(
                    volatility_config,
                    current_amplitude_ratio=float(stats.get("amplitude_ratio") or 0.0),
                    current_return_ratio=float(stats.get("return_ratio") or 0.0),
                    runner_running=not volatility_paused,
                    paused_by_trigger=volatility_paused,
                )
                if volatility_action.get("action") == "stop":
                    volatility_stop_count += 1
                    volatility_paused = True
                    if volatility_config.get("volatility_trigger_stop_close_all_positions", True):
                        flatten_ledger(ledger, start_price, totals)
                elif volatility_action.get("action") == "start":
                    volatility_resume_count += 1
                    volatility_paused = False
            if volatility_paused:
                m = mark(ledger, end_price)
                equity = totals["realized_pnl"] + m["unrealized_pnl"] - totals["fees"]
                equity_curve.append(equity)
                max_long = max(max_long, m["long_notional"])
                max_short = max(max_short, m["short_notional"])
                max_abs_net = max(max_abs_net, abs(m["net_qty"] * end_price))
                max_total_inventory = max(max_total_inventory, m["long_notional"] + m["short_notional"])
                continue
            orders, meta, startup_pending = build_orders(
                args=args,
                state=state,
                symbol_info=symbol_info,
                history=candles[:idx],
                now=now,
                price=start_price,
                ledger=ledger,
                startup_pending=startup_pending,
            )
            plan_count += 1
            bias_scores.append(meta["bias_score"])
            buy_pause_count += int(meta["buy_pause"])
            short_pause_count += int(meta["short_pause"])
            for order in touched_orders(orders, start_price, end_price):
                apply_fill(ledger, order, order_price(order), totals)

            m = mark(ledger, end_price)
            equity = totals["realized_pnl"] + m["unrealized_pnl"] - totals["fees"]
            equity_curve.append(equity)
            max_long = max(max_long, m["long_notional"])
            max_short = max(max_short, m["short_notional"])
            max_abs_net = max(max_abs_net, abs(m["net_qty"] * end_price))
            max_total_inventory = max(max_total_inventory, m["long_notional"] + m["short_notional"])

    final = mark(ledger, candles[-1].close)
    realized_after_fee = totals["realized_pnl"] - totals["fees"]
    net_pnl = realized_after_fee + final["unrealized_pnl"]
    gross = totals["gross_notional"]
    peak = equity_curve[0]
    max_drawdown = 0.0
    for equity in equity_curve:
        peak = max(peak, equity)
        max_drawdown = max(max_drawdown, peak - equity)
    return {
        "candles": len(candles),
        "fills": int(totals["fills"]),
        "gross_notional": gross,
        "realized_pnl": totals["realized_pnl"],
        "fees": totals["fees"],
        "realized_after_fee": realized_after_fee,
        "unrealized_pnl": final["unrealized_pnl"],
        "net_pnl": net_pnl,
        "realized_after_fee_per_10k": (realized_after_fee / gross * 10000.0) if gross > 0 else 0.0,
        "realized_fee_loss_per_10k": (max(-realized_after_fee, 0.0) / gross * 10000.0) if gross > 0 else 0.0,
        "mtm_loss_per_10k": (max(-net_pnl, 0.0) / gross * 10000.0) if gross > 0 else 0.0,
        "net_per_10k": (net_pnl / gross * 10000.0) if gross > 0 else 0.0,
        "max_drawdown_usdt": max_drawdown,
        "max_long_notional": max_long,
        "max_short_notional": max_short,
        "max_abs_net_notional": max_abs_net,
        "max_total_inventory_notional": max_total_inventory,
        "final_long_notional": final["long_notional"],
        "final_short_notional": final["short_notional"],
        "final_net_qty": final["net_qty"],
        "avg_bias_score": mean(bias_scores) if bias_scores else 0.0,
        "max_abs_bias_score": max((abs(x) for x in bias_scores), default=0.0),
        "buy_pause_ratio": buy_pause_count / plan_count if plan_count else 0.0,
        "short_pause_ratio": short_pause_count / plan_count if plan_count else 0.0,
        "volatility_stop_count": volatility_stop_count,
        "volatility_resume_count": volatility_resume_count,
    }


def summarize_config(config: dict[str, Any]) -> dict[str, Any]:
    keys = [
        "step_price",
        "buy_levels",
        "sell_levels",
        "per_order_notional",
        "pause_buy_position_notional",
        "max_position_notional",
        "pause_short_position_notional",
        "max_short_position_notional",
        "up_trigger_steps",
        "down_trigger_steps",
        "market_bias_max_shift_steps",
        "market_bias_weak_buy_pause_threshold",
        "market_bias_strong_short_pause_threshold",
        "profit_protect_notional_threshold",
    ]
    return {key: config.get(key) for key in keys}


def main() -> int:
    cli = parse_args()
    symbol = cli.symbol.upper().strip()
    symbol_info = fetch_futures_symbol_config(symbol)
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    issue_start = datetime(2026, 4, 7, 10, 30, tzinfo=LOCAL_TZ).astimezone(timezone.utc)
    issue_end = min(now, datetime(2026, 4, 7, 11, 45, tzinfo=LOCAL_TZ).astimezone(timezone.utc))
    windows = {
        "issue_window": (issue_start, issue_end),
        "recent_6h": (now - timedelta(hours=6), now),
        "recent_24h": (now - timedelta(hours=24), now),
    }
    fetch_start = min(start for start, _end in windows.values())
    fetch_end = max(end for _start, end in windows.values())
    candles = load_or_fetch_candles(
        symbol=symbol,
        interval="1m",
        start_time=fetch_start,
        end_time=fetch_end,
        cache_dir=cli.cache_dir,
        market_type="futures",
        refresh=cli.refresh,
    )

    configs = {Path(path).name: load_json(path) for path in cli.configs}
    output: dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "symbol": symbol,
        "fee_rate": FEE_RATE,
        "assumptions": [
            "1m K line intrabar path: open-low-high-close for green candles, open-high-low-close for red candles.",
            "A limit order is treated as filled when touched; queue position and spread microstructure are not modeled.",
            "Market-bias offset/pause, center shift, synthetic one-way virtual long/short ledger, and long/short notional caps are modeled.",
            "Light inventory uses LIFO lot protection: exits must clear the latest lot at a profitable price; reverse chase orders stay disabled until the configured threshold is reached.",
            "Funding is excluded; maker fee is modeled conservatively as 2 bps per fill.",
        ],
        "configs": {name: summarize_config(config) for name, config in configs.items()},
        "windows": {},
    }
    for window_name, (start, end) in windows.items():
        selected = [c for c in candles if start <= c.open_time < end]
        window_result: dict[str, Any] = {
            "start_local": start.astimezone(LOCAL_TZ).isoformat(),
            "end_local": end.astimezone(LOCAL_TZ).isoformat(),
            "candles": len(selected),
            "results": {},
        }
        if selected:
            for config_name, config in configs.items():
                window_result["results"][config_name] = replay(selected, config, symbol_info)
        output["windows"][window_name] = window_result

    out = Path(cli.output_json)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(output, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
