from __future__ import annotations

import argparse
import bisect
import json
import math
import time
import traceback
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .audit import (
    DEFAULT_AUDIT_LOOKBACK_DAYS,
    append_jsonl as _append_jsonl,
    build_audit_paths,
    count_jsonl_lines,
    collect_new_rows,
    epoch_ms,
    fetch_time_paged,
    income_row_key,
    income_row_time_ms,
    read_json,
    scan_iso_bounds,
    trade_row_key,
    trade_row_time_ms,
    write_json as _write_json,
)
from .backtest import build_grid_levels
from .data import (
    delete_futures_order,
    fetch_futures_account_info_v3,
    fetch_futures_book_tickers,
    fetch_futures_income_history,
    fetch_futures_klines,
    fetch_futures_open_orders,
    fetch_futures_position_mode,
    fetch_futures_premium_index,
    fetch_futures_symbol_config,
    fetch_futures_user_trades,
    load_binance_api_credentials,
    post_futures_change_initial_leverage,
    post_futures_change_margin_type,
    post_futures_order,
)
from .live_check import extract_symbol_position
from .semi_auto_plan import (
    STATE_VERSION,
    _isoformat,
    _safe_float,
    _truthy,
    _round_to_nearest_step,
    _utc_now,
    build_static_binance_grid_plan,
    build_hedge_micro_grid_plan,
    build_inventory_target_neutral_plan,
    build_micro_grid_plan,
    build_short_micro_grid_plan,
    build_best_quote_short_flip_plan,
    diff_open_orders,
    load_or_initialize_state,
    shift_center_price,
)
from .submit_plan import (
    _build_client_order_id,
    _ignore_noop_error,
    estimate_mid_drift_steps,
    prepare_post_only_order_request,
    validate_plan_report,
)
from .dry_run import _round_order_qty

AUTO_REGIME_STABLE_PROFILE = "volume_long_v4"
AUTO_REGIME_DEFENSIVE_PROFILE = "volatility_defensive_v1"
XAUT_LONG_ADAPTIVE_PROFILE = "xaut_long_adaptive_v1"
XAUT_SHORT_ADAPTIVE_PROFILE = "xaut_short_adaptive_v1"
XAUT_ADAPTIVE_PROFILES = {
    XAUT_LONG_ADAPTIVE_PROFILE: "one_way_long",
    XAUT_SHORT_ADAPTIVE_PROFILE: "one_way_short",
}
XAUT_ADAPTIVE_STATE_NORMAL = "normal"
XAUT_ADAPTIVE_STATE_DEFENSIVE = "defensive"
XAUT_ADAPTIVE_STATE_REDUCE_ONLY = "reduce_only"
AUDIT_SYNC_MIN_INTERVAL_SECONDS = 60.0
AUTO_REGIME_PROFILE_OVERRIDES: dict[str, dict[str, Any]] = {
    AUTO_REGIME_STABLE_PROFILE: {
        "buy_levels": 8,
        "sell_levels": 8,
        "per_order_notional": 70.0,
        "base_position_notional": 420.0,
        "up_trigger_steps": 6,
        "down_trigger_steps": 4,
        "shift_steps": 4,
        "pause_buy_position_notional": 750.0,
        "max_position_notional": 900.0,
        "buy_pause_amp_trigger_ratio": 0.0075,
        "buy_pause_down_return_trigger_ratio": -0.0035,
        "freeze_shift_abs_return_trigger_ratio": 0.005,
        "inventory_tier_start_notional": 600.0,
        "inventory_tier_end_notional": 750.0,
        "inventory_tier_buy_levels": 4,
        "inventory_tier_sell_levels": 12,
        "inventory_tier_per_order_notional": 70.0,
        "inventory_tier_base_position_notional": 280.0,
    },
    AUTO_REGIME_DEFENSIVE_PROFILE: {
        "buy_levels": 4,
        "sell_levels": 12,
        "per_order_notional": 45.0,
        "base_position_notional": 120.0,
        "up_trigger_steps": 5,
        "down_trigger_steps": 7,
        "shift_steps": 3,
        "pause_buy_position_notional": 300.0,
        "max_position_notional": 420.0,
        "buy_pause_amp_trigger_ratio": 0.0055,
        "buy_pause_down_return_trigger_ratio": -0.0025,
        "freeze_shift_abs_return_trigger_ratio": 0.0035,
        "inventory_tier_start_notional": 220.0,
        "inventory_tier_end_notional": 320.0,
        "inventory_tier_buy_levels": 2,
        "inventory_tier_sell_levels": 14,
        "inventory_tier_per_order_notional": 45.0,
        "inventory_tier_base_position_notional": 90.0,
    },
}
AUTO_REGIME_PROFILE_LABELS = {
    AUTO_REGIME_STABLE_PROFILE: "量优先做多 v4",
    AUTO_REGIME_DEFENSIVE_PROFILE: "高波动防守 v1",
    XAUT_LONG_ADAPTIVE_PROFILE: "XAUT 自适应做多 v1",
    XAUT_SHORT_ADAPTIVE_PROFILE: "XAUT 自适应做空 v1",
}
AUTO_REGIME_PROFILE_STEP_HINTS: dict[str, tuple[float, int]] = {
    AUTO_REGIME_STABLE_PROFILE: (0.0004, 2),
    AUTO_REGIME_DEFENSIVE_PROFILE: (0.0008, 4),
}
BEST_QUOTE_SHORT_PROFILE = "robo_best_quote_short_v1"
XAUT_ADAPTIVE_TRANSITION_CONFIRMATIONS = {
    (XAUT_ADAPTIVE_STATE_NORMAL, XAUT_ADAPTIVE_STATE_DEFENSIVE): 2,
    (XAUT_ADAPTIVE_STATE_DEFENSIVE, XAUT_ADAPTIVE_STATE_NORMAL): 2,
    (XAUT_ADAPTIVE_STATE_DEFENSIVE, XAUT_ADAPTIVE_STATE_REDUCE_ONLY): 1,
    (XAUT_ADAPTIVE_STATE_REDUCE_ONLY, XAUT_ADAPTIVE_STATE_DEFENSIVE): 2,
    (XAUT_ADAPTIVE_STATE_NORMAL, XAUT_ADAPTIVE_STATE_REDUCE_ONLY): 1,
}
XAUT_ADAPTIVE_STATE_CONFIGS: dict[str, dict[str, dict[str, Any]]] = {
    "one_way_long": {
        XAUT_ADAPTIVE_STATE_NORMAL: {
            "step_price": 7.5,
            "buy_levels": 6,
            "sell_levels": 10,
            "per_order_notional": 80.0,
            "base_position_notional": 320.0,
            "up_trigger_steps": 5,
            "down_trigger_steps": 4,
            "shift_steps": 3,
            "pause_buy_position_notional": 520.0,
            "max_position_notional": 680.0,
            "buy_pause_amp_trigger_ratio": 0.0060,
            "buy_pause_down_return_trigger_ratio": -0.0045,
            "freeze_shift_abs_return_trigger_ratio": 0.0048,
            "inventory_tier_start_notional": 420.0,
            "inventory_tier_end_notional": 520.0,
            "inventory_tier_buy_levels": 3,
            "inventory_tier_sell_levels": 12,
            "inventory_tier_per_order_notional": 70.0,
            "inventory_tier_base_position_notional": 160.0,
            "autotune_symbol_enabled": False,
            "excess_inventory_reduce_only_enabled": False,
        },
        XAUT_ADAPTIVE_STATE_DEFENSIVE: {
            "step_price": 12.0,
            "buy_levels": 2,
            "sell_levels": 14,
            "per_order_notional": 45.0,
            "base_position_notional": 100.0,
            "up_trigger_steps": 4,
            "down_trigger_steps": 6,
            "shift_steps": 2,
            "pause_buy_position_notional": 180.0,
            "max_position_notional": 260.0,
            "buy_pause_amp_trigger_ratio": 0.0045,
            "buy_pause_down_return_trigger_ratio": -0.0035,
            "freeze_shift_abs_return_trigger_ratio": 0.0040,
            "inventory_tier_start_notional": 140.0,
            "inventory_tier_end_notional": 180.0,
            "inventory_tier_buy_levels": 1,
            "inventory_tier_sell_levels": 16,
            "inventory_tier_per_order_notional": 40.0,
            "inventory_tier_base_position_notional": 60.0,
            "autotune_symbol_enabled": False,
            "excess_inventory_reduce_only_enabled": False,
        },
        XAUT_ADAPTIVE_STATE_REDUCE_ONLY: {
            "step_price": 12.0,
            "buy_levels": 2,
            "sell_levels": 14,
            "per_order_notional": 45.0,
            "base_position_notional": 100.0,
            "up_trigger_steps": 4,
            "down_trigger_steps": 6,
            "shift_steps": 2,
            "pause_buy_position_notional": 180.0,
            "max_position_notional": 260.0,
            "buy_pause_amp_trigger_ratio": 0.0045,
            "buy_pause_down_return_trigger_ratio": -0.0035,
            "freeze_shift_abs_return_trigger_ratio": 0.0040,
            "inventory_tier_start_notional": 140.0,
            "inventory_tier_end_notional": 180.0,
            "inventory_tier_buy_levels": 1,
            "inventory_tier_sell_levels": 16,
            "inventory_tier_per_order_notional": 40.0,
            "inventory_tier_base_position_notional": 60.0,
            "autotune_symbol_enabled": False,
            "excess_inventory_reduce_only_enabled": True,
        },
    },
    "one_way_short": {
        XAUT_ADAPTIVE_STATE_NORMAL: {
            "step_price": 8.5,
            "buy_levels": 10,
            "sell_levels": 6,
            "per_order_notional": 80.0,
            "base_position_notional": 280.0,
            "up_trigger_steps": 4,
            "down_trigger_steps": 5,
            "shift_steps": 3,
            "pause_short_position_notional": 560.0,
            "max_short_position_notional": 620.0,
            "inventory_tier_start_notional": 360.0,
            "inventory_tier_end_notional": 450.0,
            "inventory_tier_buy_levels": 14,
            "inventory_tier_sell_levels": 2,
            "inventory_tier_per_order_notional": 65.0,
            "inventory_tier_base_position_notional": 120.0,
            "short_cover_pause_amp_trigger_ratio": 0.0060,
            "short_cover_pause_down_return_trigger_ratio": -0.0045,
            "autotune_symbol_enabled": False,
            "excess_inventory_reduce_only_enabled": False,
        },
        XAUT_ADAPTIVE_STATE_DEFENSIVE: {
            "step_price": 12.0,
            "buy_levels": 14,
            "sell_levels": 2,
            "per_order_notional": 45.0,
            "base_position_notional": 100.0,
            "up_trigger_steps": 6,
            "down_trigger_steps": 4,
            "shift_steps": 2,
            "pause_short_position_notional": 180.0,
            "max_short_position_notional": 260.0,
            "inventory_tier_start_notional": 140.0,
            "inventory_tier_end_notional": 180.0,
            "inventory_tier_buy_levels": 16,
            "inventory_tier_sell_levels": 1,
            "inventory_tier_per_order_notional": 40.0,
            "inventory_tier_base_position_notional": 60.0,
            "short_cover_pause_amp_trigger_ratio": 0.0045,
            "short_cover_pause_down_return_trigger_ratio": -0.0035,
            "autotune_symbol_enabled": False,
            "excess_inventory_reduce_only_enabled": False,
        },
        XAUT_ADAPTIVE_STATE_REDUCE_ONLY: {
            "step_price": 12.0,
            "buy_levels": 14,
            "sell_levels": 2,
            "per_order_notional": 45.0,
            "base_position_notional": 100.0,
            "up_trigger_steps": 6,
            "down_trigger_steps": 4,
            "shift_steps": 2,
            "pause_short_position_notional": 180.0,
            "max_short_position_notional": 260.0,
            "inventory_tier_start_notional": 140.0,
            "inventory_tier_end_notional": 180.0,
            "inventory_tier_buy_levels": 16,
            "inventory_tier_sell_levels": 1,
            "inventory_tier_per_order_notional": 40.0,
            "inventory_tier_base_position_notional": 60.0,
            "short_cover_pause_amp_trigger_ratio": 0.0045,
            "short_cover_pause_down_return_trigger_ratio": -0.0035,
            "autotune_symbol_enabled": False,
            "excess_inventory_reduce_only_enabled": True,
        },
    },
}


def _float(value: float) -> str:
    return f"{value:,.4f}"


def _price(value: float) -> str:
    return f"{value:.7f}"


def is_xaut_adaptive_profile(profile: str) -> bool:
    return str(profile).strip() in XAUT_ADAPTIVE_PROFILES


def _xaut_transition_confirm_cycles(active_state: str, candidate_state: str) -> int:
    return max(int(XAUT_ADAPTIVE_TRANSITION_CONFIRMATIONS.get((str(active_state), str(candidate_state)), 1) or 1), 1)


def _round_up_to_step(value: float, step: float | None) -> float:
    if step is None or step <= 0:
        return float(value)
    units = max(int(math.ceil(max(float(value), 0.0) / float(step))), 0)
    return units * float(step)


def _resolve_fixed_center_roll(
    *,
    state: dict[str, Any],
    center_price: float,
    mid_price: float,
    step_price: float,
    tick_size: float | None,
    enabled: bool,
    trigger_steps: float,
    confirm_cycles: int,
    shift_steps: int,
) -> tuple[float, list[dict[str, Any]], dict[str, Any]]:
    pending_direction = str(state.get("fixed_center_roll_pending_direction", "") or "")
    pending_count = int(state.get("fixed_center_roll_pending_count", 0) or 0)
    safe_step_price = max(float(step_price), 0.0)
    safe_trigger_steps = max(float(trigger_steps), 0.0)
    safe_confirm_cycles = max(int(confirm_cycles), 1)
    safe_shift_steps = max(int(shift_steps), 1)
    drift_steps = ((float(mid_price) - float(center_price)) / safe_step_price) if safe_step_price > 0 else 0.0
    direction = ""
    moves: list[dict[str, Any]] = []
    shifted = False

    if enabled and safe_step_price > 0 and safe_trigger_steps > 0:
        if drift_steps >= safe_trigger_steps:
            direction = "up"
        elif drift_steps <= -safe_trigger_steps:
            direction = "down"

        if direction:
            if direction == pending_direction:
                pending_count += 1
            else:
                pending_direction = direction
                pending_count = 1
        else:
            pending_direction = ""
            pending_count = 0

        if direction and pending_count >= safe_confirm_cycles:
            delta = safe_shift_steps * safe_step_price * (1.0 if direction == "up" else -1.0)
            old_center_price = float(center_price)
            center_price = _round_to_nearest_step(old_center_price + delta, tick_size)
            pending_direction = ""
            pending_count = 0
            if abs(center_price - old_center_price) > 1e-12:
                shifted = True
                moves.append(
                    {
                        "direction": direction,
                        "reason": "fixed_center_roll",
                        "trigger_steps": safe_trigger_steps,
                        "confirm_cycles": safe_confirm_cycles,
                        "shift_steps": safe_shift_steps,
                        "old_center_price": old_center_price,
                        "new_center_price": center_price,
                        "mid_price": float(mid_price),
                        "drift_steps": drift_steps,
                    }
                )
    else:
        pending_direction = ""
        pending_count = 0

    state["fixed_center_roll_pending_direction"] = pending_direction
    state["fixed_center_roll_pending_count"] = pending_count
    return center_price, moves, {
        "enabled": bool(enabled),
        "trigger_steps": safe_trigger_steps,
        "confirm_cycles": safe_confirm_cycles,
        "shift_steps": safe_shift_steps,
        "drift_steps": drift_steps,
        "pending_direction": pending_direction,
        "pending_count": pending_count,
        "shifted": shifted,
    }


def _current_check_bucket(now: datetime, interval_minutes: int) -> str:
    safe_interval = max(int(interval_minutes), 1)
    utc_now = now.astimezone(timezone.utc)
    bucket_minute = (utc_now.minute // safe_interval) * safe_interval
    bucket = utc_now.replace(minute=bucket_minute, second=0, microsecond=0)
    return bucket.strftime("%Y-%m-%dT%H:%MZ")


def _custom_grid_levels_above_current(levels: list[float], current_price: float) -> int:
    if len(levels) < 2:
        return 0
    insertion = bisect.bisect_left(levels, float(current_price))
    return max((len(levels) - 1) - insertion, 0)


def _shift_custom_grid_bounds(
    *,
    min_price: float,
    max_price: float,
    n: int,
    grid_level_mode: str,
    shift_levels: int,
) -> dict[str, float]:
    safe_n = max(int(n), 1)
    safe_shift_levels = max(int(shift_levels), 1)
    if min_price <= 0 or max_price <= 0 or max_price <= min_price:
        raise ValueError("invalid custom grid bounds")
    mode = str(grid_level_mode or "arithmetic").strip().lower() or "arithmetic"
    if mode == "arithmetic":
        step_price = (float(max_price) - float(min_price)) / float(safe_n)
        if step_price <= 0:
            raise ValueError("invalid arithmetic custom grid step")
        delta = step_price * safe_shift_levels
        return {
            "min_price": float(min_price) - delta,
            "max_price": float(max_price) - delta,
        }

    ratio = (float(max_price) / float(min_price)) ** (1.0 / float(safe_n))
    if ratio <= 1.0:
        raise ValueError("invalid geometric custom grid ratio")
    divisor = ratio**safe_shift_levels
    return {
        "min_price": float(min_price) / divisor,
        "max_price": float(max_price) / divisor,
    }


def _read_custom_grid_trade_count(summary_path: str | Path) -> int:
    trade_audit_path = build_audit_paths(summary_path)["trade_audit"]
    return count_jsonl_lines(trade_audit_path)


def _resolve_custom_grid_roll(
    *,
    state: dict[str, Any],
    summary_path: str | Path,
    now: datetime,
    current_price: float,
    min_price: float,
    max_price: float,
    n: int,
    grid_level_mode: str,
    direction: str,
    enabled: bool,
    interval_minutes: int,
    trade_threshold: int,
    upper_distance_ratio: float,
    shift_levels: int,
) -> tuple[float, float, dict[str, Any]]:
    safe_direction = str(direction or "").strip().lower()
    safe_enabled = bool(enabled)
    safe_interval_minutes = max(int(interval_minutes), 1)
    safe_trade_threshold = max(int(trade_threshold), 0)
    safe_upper_distance_ratio = max(min(float(upper_distance_ratio), 1.0), 0.0)
    safe_shift_levels = max(int(shift_levels), 1)
    safe_n = max(int(n), 1)
    current_min_price = float(min_price)
    current_max_price = float(max_price)
    current_trade_count = _read_custom_grid_trade_count(summary_path)
    trade_baseline = max(int(state.get("custom_grid_roll_trade_baseline", 0) or 0), 0)
    if current_trade_count < trade_baseline:
        trade_baseline = current_trade_count
    trades_since_last_roll = max(current_trade_count - trade_baseline, 0)
    current_bucket = _current_check_bucket(now, safe_interval_minutes)
    last_check_bucket = str(state.get("custom_grid_roll_last_check_bucket", "") or "")
    levels = build_grid_levels(
        min_price=current_min_price,
        max_price=current_max_price,
        n=safe_n,
        grid_level_mode=grid_level_mode,
    )
    levels_above_current = _custom_grid_levels_above_current(levels, current_price)
    required_levels_above = min(max(int(math.ceil(float(safe_n) * safe_upper_distance_ratio)), 0), safe_n)
    last_applied_at = state.get("custom_grid_roll_last_applied_at")
    last_applied_price = _safe_float(state.get("custom_grid_roll_last_applied_price"))
    old_min_price = current_min_price
    old_max_price = current_max_price
    new_min_price = current_min_price
    new_max_price = current_max_price

    state["custom_grid_runtime_min_price"] = current_min_price
    state["custom_grid_runtime_max_price"] = current_max_price
    state["custom_grid_roll_trade_baseline"] = trade_baseline
    state["custom_grid_roll_trades_since_last_roll"] = trades_since_last_roll

    result = {
        "enabled": safe_enabled,
        "direction": safe_direction,
        "checked": False,
        "triggered": False,
        "reason": "disabled" if not safe_enabled else "direction_not_long" if safe_direction != "long" else "pending_check",
        "interval_minutes": safe_interval_minutes,
        "trade_threshold": safe_trade_threshold,
        "upper_distance_ratio": safe_upper_distance_ratio,
        "shift_levels": safe_shift_levels,
        "current_trade_count": current_trade_count,
        "trade_baseline": trade_baseline,
        "trades_since_last_roll": trades_since_last_roll,
        "levels_above_current": levels_above_current,
        "required_levels_above": required_levels_above,
        "current_price": float(current_price),
        "current_bucket": current_bucket,
        "last_check_bucket": last_check_bucket or None,
        "current_min_price": current_min_price,
        "current_max_price": current_max_price,
        "old_min_price": old_min_price,
        "old_max_price": old_max_price,
        "new_min_price": new_min_price,
        "new_max_price": new_max_price,
        "last_applied_at": last_applied_at,
        "last_applied_price": last_applied_price if last_applied_price > 0 else None,
        "last_old_min_price": _safe_float(state.get("custom_grid_roll_last_old_min_price")),
        "last_old_max_price": _safe_float(state.get("custom_grid_roll_last_old_max_price")),
        "last_new_min_price": _safe_float(state.get("custom_grid_roll_last_new_min_price")),
        "last_new_max_price": _safe_float(state.get("custom_grid_roll_last_new_max_price")),
    }

    if not safe_enabled or safe_direction != "long":
        return current_min_price, current_max_price, result

    if last_check_bucket == current_bucket:
        result["reason"] = "already_checked_bucket"
        return current_min_price, current_max_price, result

    result["checked"] = True
    state["custom_grid_roll_last_check_bucket"] = current_bucket
    result["last_check_bucket"] = current_bucket

    if float(current_price) > current_max_price:
        result["reason"] = "price_above_upper"
        return current_min_price, current_max_price, result
    if trades_since_last_roll < safe_trade_threshold:
        result["reason"] = "trade_threshold_not_met"
        return current_min_price, current_max_price, result
    if levels_above_current < required_levels_above:
        result["reason"] = "upper_distance_not_met"
        return current_min_price, current_max_price, result

    shifted = _shift_custom_grid_bounds(
        min_price=current_min_price,
        max_price=current_max_price,
        n=safe_n,
        grid_level_mode=grid_level_mode,
        shift_levels=safe_shift_levels,
    )
    new_min_price = float(shifted["min_price"])
    new_max_price = float(shifted["max_price"])
    if new_min_price <= 0 or new_max_price <= new_min_price:
        result["reason"] = "shift_would_cross_zero"
        return current_min_price, current_max_price, result

    applied_at = now.astimezone(timezone.utc).isoformat()
    state["custom_grid_runtime_min_price"] = new_min_price
    state["custom_grid_runtime_max_price"] = new_max_price
    state["custom_grid_roll_trade_baseline"] = current_trade_count
    state["custom_grid_roll_trades_since_last_roll"] = 0
    state["custom_grid_roll_last_applied_at"] = applied_at
    state["custom_grid_roll_last_applied_price"] = float(current_price)
    state["custom_grid_roll_last_old_min_price"] = current_min_price
    state["custom_grid_roll_last_old_max_price"] = current_max_price
    state["custom_grid_roll_last_new_min_price"] = new_min_price
    state["custom_grid_roll_last_new_max_price"] = new_max_price

    result.update(
        {
            "triggered": True,
            "reason": "triggered",
            "trade_baseline": current_trade_count,
            "trades_since_last_roll": 0,
            "current_min_price": new_min_price,
            "current_max_price": new_max_price,
            "new_min_price": new_min_price,
            "new_max_price": new_max_price,
            "last_applied_at": applied_at,
            "last_applied_price": float(current_price),
            "last_old_min_price": current_min_price,
            "last_old_max_price": current_max_price,
            "last_new_min_price": new_min_price,
            "last_new_max_price": new_max_price,
        }
    )
    return new_min_price, new_max_price, result


def _run_periodic_reconcile(
    *,
    state: dict[str, Any],
    cycle: int,
    interval_cycles: int,
    symbol: str,
    strategy_mode: str,
    api_key: str,
    api_secret: str,
    recv_window: int,
    expected_open_order_count: int,
    expected_actual_net_qty: float,
) -> dict[str, Any]:
    safe_interval_cycles = max(int(interval_cycles), 0)
    snapshot = dict(state.get("last_reconcile") or {})
    snapshot["enabled"] = safe_interval_cycles > 0
    snapshot["interval_cycles"] = safe_interval_cycles
    snapshot["checked"] = False
    snapshot["cycle"] = cycle
    if safe_interval_cycles <= 0 or cycle % safe_interval_cycles != 0:
        return snapshot

    current_open_orders = fetch_futures_open_orders(symbol, api_key, api_secret, recv_window=recv_window)
    account_info = fetch_futures_account_info_v3(api_key, api_secret, recv_window=recv_window)
    current_position = extract_symbol_position(account_info, symbol)
    actual_net_qty = _safe_float(current_position.get("positionAmt"))
    open_order_diff = len(current_open_orders) - int(expected_open_order_count or 0)
    actual_net_qty_diff = actual_net_qty - float(expected_actual_net_qty or 0.0)
    ok = abs(open_order_diff) == 0 and abs(actual_net_qty_diff) <= 1e-9
    message_parts: list[str] = []
    if open_order_diff:
        message_parts.append(f"open_orders diff={open_order_diff:+d}")
    if abs(actual_net_qty_diff) > 1e-9:
        message_parts.append(f"net_qty diff={actual_net_qty_diff:+.8f}")

    snapshot = {
        "enabled": True,
        "interval_cycles": safe_interval_cycles,
        "checked": True,
        "checked_at": _isoformat(_utc_now()),
        "cycle": cycle,
        "strategy_mode": strategy_mode,
        "expected_open_order_count": int(expected_open_order_count or 0),
        "actual_open_order_count": len(current_open_orders),
        "expected_actual_net_qty": float(expected_actual_net_qty or 0.0),
        "actual_actual_net_qty": actual_net_qty,
        "open_order_diff": open_order_diff,
        "actual_net_qty_diff": actual_net_qty_diff,
        "ok": ok,
        "message": "OK" if ok else "; ".join(message_parts),
    }
    state["last_reconcile"] = snapshot
    return snapshot


def _position_qty(position: dict[str, Any], *, position_side: str | None = None) -> float:
    qty = _safe_float(position.get("positionAmt"))
    if not str(position_side or "").strip():
        return qty
    return abs(qty)


def _order_position_side(order: dict[str, Any]) -> str:
    return str(order.get("position_side", order.get("positionSide", "BOTH"))).upper().strip() or "BOTH"


def _order_role(order: dict[str, Any]) -> str:
    return str(order.get("role", "")).lower().strip()


def _is_long_entry_order(order: dict[str, Any]) -> bool:
    role = _order_role(order)
    position_side = _order_position_side(order)
    return role in {"bootstrap", "entry", "bootstrap_long", "entry_long"} or (
        position_side in {"BOTH", "LONG"} and role in {"bootstrap", "entry"}
    )


def _is_short_entry_order(order: dict[str, Any]) -> bool:
    role = _order_role(order)
    return role in {"bootstrap_short", "entry_short"}


def _is_synthetic_neutral_mode(strategy_mode: str) -> bool:
    return str(strategy_mode).strip() == "synthetic_neutral"


def _is_inventory_target_neutral_mode(strategy_mode: str) -> bool:
    return str(strategy_mode).strip() == "inventory_target_neutral"


def _is_custom_grid_mode(args: argparse.Namespace) -> bool:
    return bool(getattr(args, "custom_grid_enabled", False))


def _is_one_way_short_mode(strategy_mode: str) -> bool:
    return str(strategy_mode).strip() == "one_way_short"


def _is_best_quote_short_profile(strategy_profile: str) -> bool:
    return str(strategy_profile).strip() == BEST_QUOTE_SHORT_PROFILE


def _convert_plan_orders_to_one_way(plan: dict[str, Any]) -> dict[str, Any]:
    converted = dict(plan)
    for key in ("bootstrap_orders", "buy_orders", "sell_orders"):
        orders: list[dict[str, Any]] = []
        for item in plan.get(key, []):
            if not isinstance(item, dict):
                continue
            order = dict(item)
            order["position_side"] = "BOTH"
            orders.append(order)
        converted[key] = orders
    return converted


def _load_synthetic_ledger(
    *,
    state: dict[str, Any],
    actual_position_qty: float,
    entry_price: float,
) -> dict[str, Any]:
    ledger = dict(state.get("synthetic_ledger") or {})
    if not _truthy(ledger.get("initialized")):
        net_qty = float(actual_position_qty)
        if net_qty > 0:
            ledger.update(
                {
                    "virtual_long_qty": net_qty,
                    "virtual_long_avg_price": max(float(entry_price), 0.0),
                    "virtual_short_qty": 0.0,
                    "virtual_short_avg_price": 0.0,
                }
            )
        elif net_qty < 0:
            ledger.update(
                {
                    "virtual_long_qty": 0.0,
                    "virtual_long_avg_price": 0.0,
                    "virtual_short_qty": abs(net_qty),
                    "virtual_short_avg_price": max(float(entry_price), 0.0),
                }
            )
        else:
            ledger.update(
                {
                    "virtual_long_qty": 0.0,
                    "virtual_long_avg_price": 0.0,
                    "virtual_short_qty": 0.0,
                    "virtual_short_avg_price": 0.0,
                }
            )
        ledger["initialized"] = True
        ledger["last_trade_time_ms"] = int(ledger.get("last_trade_time_ms") or 0)
        ledger["last_trade_keys_at_time"] = list(ledger.get("last_trade_keys_at_time") or [])
        ledger["unmatched_trade_count"] = int(ledger.get("unmatched_trade_count") or 0)
    state["synthetic_ledger"] = ledger
    return ledger


def _synthetic_order_ref_from_state(state: dict[str, Any], order_id: int | None) -> dict[str, Any] | None:
    refs = state.get("synthetic_order_refs")
    if not isinstance(refs, dict) or order_id is None:
        return None
    item = refs.get(str(int(order_id)))
    return dict(item) if isinstance(item, dict) else None


def _apply_synthetic_trade_fill(
    *,
    ledger: dict[str, Any],
    trade: dict[str, Any],
    order_ref: dict[str, Any] | None,
) -> bool:
    if order_ref is None:
        return False
    role = str(order_ref.get("role", "")).lower().strip()
    qty = _safe_float(trade.get("qty"))
    price = _safe_float(trade.get("price"))
    if qty <= 0 or price <= 0:
        return False

    long_qty = max(_safe_float(ledger.get("virtual_long_qty")), 0.0)
    long_avg = max(_safe_float(ledger.get("virtual_long_avg_price")), 0.0)
    short_qty = max(_safe_float(ledger.get("virtual_short_qty")), 0.0)
    short_avg = max(_safe_float(ledger.get("virtual_short_avg_price")), 0.0)

    if role in {"bootstrap_long", "entry_long"}:
        total_cost = long_avg * long_qty + price * qty
        long_qty += qty
        long_avg = total_cost / long_qty if long_qty > 0 else 0.0
    elif role == "take_profit_long":
        long_qty = max(long_qty - qty, 0.0)
        if long_qty <= 1e-12:
            long_qty = 0.0
            long_avg = 0.0
    elif role in {"bootstrap_short", "entry_short"}:
        total_proceeds = short_avg * short_qty + price * qty
        short_qty += qty
        short_avg = total_proceeds / short_qty if short_qty > 0 else 0.0
    elif role == "take_profit_short":
        short_qty = max(short_qty - qty, 0.0)
        if short_qty <= 1e-12:
            short_qty = 0.0
            short_avg = 0.0
    else:
        return False

    ledger["virtual_long_qty"] = long_qty
    ledger["virtual_long_avg_price"] = long_avg
    ledger["virtual_short_qty"] = short_qty
    ledger["virtual_short_avg_price"] = short_avg
    return True


def sync_synthetic_ledger(
    *,
    state: dict[str, Any],
    symbol: str,
    api_key: str,
    api_secret: str,
    recv_window: int,
    actual_position_qty: float,
    entry_price: float,
) -> dict[str, Any]:
    ledger = _load_synthetic_ledger(
        state=state,
        actual_position_qty=actual_position_qty,
        entry_price=entry_price,
    )
    trade_start_time_ms = int(ledger.get("last_trade_time_ms") or 0)
    if trade_start_time_ms <= 0:
        trade_start_time_ms = max(int((_utc_now().timestamp() * 1000) - 48 * 3600 * 1000), 0)

    trade_rows = _fetch_trade_rows_since(
        symbol=symbol,
        api_key=api_key,
        api_secret=api_secret,
        start_time_ms=trade_start_time_ms,
        recv_window=recv_window,
    )
    fresh_rows, last_time_ms, keys_at_time = collect_new_rows(
        rows=trade_rows,
        last_time_ms=int(ledger.get("last_trade_time_ms") or 0) or None,
        last_keys_at_time=list(ledger.get("last_trade_keys_at_time", [])),
        row_time_ms=trade_row_time_ms,
        row_key=trade_row_key,
    )

    applied = 0
    unmatched = 0
    for row in fresh_rows:
        order_ref = _synthetic_order_ref_from_state(state, epoch_ms(row.get("orderId")))
        if _apply_synthetic_trade_fill(ledger=ledger, trade=row, order_ref=order_ref):
            applied += 1
        else:
            unmatched += 1

    ledger["last_trade_time_ms"] = int(last_time_ms or ledger.get("last_trade_time_ms") or 0)
    ledger["last_trade_keys_at_time"] = list(keys_at_time)
    ledger["unmatched_trade_count"] = int(ledger.get("unmatched_trade_count") or 0) + unmatched
    state["synthetic_ledger"] = ledger
    return {
        "virtual_long_qty": max(_safe_float(ledger.get("virtual_long_qty")), 0.0),
        "virtual_long_avg_price": max(_safe_float(ledger.get("virtual_long_avg_price")), 0.0),
        "virtual_short_qty": max(_safe_float(ledger.get("virtual_short_qty")), 0.0),
        "virtual_short_avg_price": max(_safe_float(ledger.get("virtual_short_avg_price")), 0.0),
        "applied_trade_count": applied,
        "unmatched_trade_count": unmatched,
    }


def update_synthetic_order_refs(
    *,
    state_path: Path,
    strategy_mode: str,
    submit_report: dict[str, Any],
) -> None:
    if not _is_synthetic_neutral_mode(strategy_mode) or not state_path.exists():
        return
    try:
        state = json.loads(state_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return

    refs = state.get("synthetic_order_refs")
    if not isinstance(refs, dict):
        refs = {}

    for item in submit_report.get("placed_orders", []):
        if not isinstance(item, dict):
            continue
        request = item.get("request", {}) if isinstance(item.get("request"), dict) else {}
        response = item.get("response", {}) if isinstance(item.get("response"), dict) else {}
        order_id = epoch_ms(response.get("orderId"))
        if order_id <= 0:
            continue
        refs[str(order_id)] = {
            "role": str(request.get("role", "")).strip(),
            "side": str(request.get("side", "")).upper().strip(),
            "position_side": str(request.get("position_side", "BOTH")).upper().strip() or "BOTH",
            "client_order_id": str(response.get("clientOrderId", "")).strip(),
            "updated_at": _isoformat(_utc_now()),
        }

    if len(refs) > 5000:
        recent_items = list(refs.items())[-5000:]
        refs = {key: value for key, value in recent_items}

    state["synthetic_order_refs"] = refs
    try:
        state_path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    except OSError:
        return


def _planner_namespace(args: argparse.Namespace) -> argparse.Namespace:
    return argparse.Namespace(
        symbol=args.symbol,
        strategy_mode=args.strategy_mode,
        step_price=args.step_price,
        buy_levels=args.buy_levels,
        sell_levels=args.sell_levels,
        per_order_notional=args.per_order_notional,
        base_position_notional=args.base_position_notional,
        center_price=args.center_price,
        fixed_center_enabled=getattr(args, "fixed_center_enabled", False),
        custom_grid_enabled=getattr(args, "custom_grid_enabled", False),
        custom_grid_direction=getattr(args, "custom_grid_direction", None),
        custom_grid_level_mode=getattr(args, "custom_grid_level_mode", None),
        custom_grid_min_price=getattr(args, "custom_grid_min_price", None),
        custom_grid_max_price=getattr(args, "custom_grid_max_price", None),
        custom_grid_n=getattr(args, "custom_grid_n", None),
        custom_grid_total_notional=getattr(args, "custom_grid_total_notional", None),
        custom_grid_neutral_anchor_price=getattr(args, "custom_grid_neutral_anchor_price", None),
        custom_grid_roll_enabled=getattr(args, "custom_grid_roll_enabled", False),
        custom_grid_roll_interval_minutes=getattr(args, "custom_grid_roll_interval_minutes", 5),
        custom_grid_roll_trade_threshold=getattr(args, "custom_grid_roll_trade_threshold", 100),
        custom_grid_roll_upper_distance_ratio=getattr(args, "custom_grid_roll_upper_distance_ratio", 0.30),
        custom_grid_roll_shift_levels=getattr(args, "custom_grid_roll_shift_levels", 1),
        down_trigger_steps=args.down_trigger_steps,
        up_trigger_steps=args.up_trigger_steps,
        shift_steps=args.shift_steps,
        neutral_center_interval_minutes=getattr(args, "neutral_center_interval_minutes", 3),
        neutral_band1_offset_ratio=getattr(args, "neutral_band1_offset_ratio", 0.005),
        neutral_band2_offset_ratio=getattr(args, "neutral_band2_offset_ratio", 0.01),
        neutral_band3_offset_ratio=getattr(args, "neutral_band3_offset_ratio", 0.02),
        neutral_band1_target_ratio=getattr(args, "neutral_band1_target_ratio", 0.20),
        neutral_band2_target_ratio=getattr(args, "neutral_band2_target_ratio", 0.50),
        neutral_band3_target_ratio=getattr(args, "neutral_band3_target_ratio", 1.00),
        neutral_hourly_scale_enabled=getattr(args, "neutral_hourly_scale_enabled", False),
        neutral_hourly_scale_stable=getattr(args, "neutral_hourly_scale_stable", 1.0),
        neutral_hourly_scale_transition=getattr(args, "neutral_hourly_scale_transition", 0.85),
        neutral_hourly_scale_defensive=getattr(args, "neutral_hourly_scale_defensive", 0.65),
        max_position_notional=getattr(args, "max_position_notional", None),
        max_short_position_notional=getattr(args, "max_short_position_notional", None),
    )


def _load_audit_state(path: Path) -> dict[str, Any]:
    payload = read_json(path)
    return payload if payload else {}


def _should_sync_account_audit(audit_state: dict[str, Any], *, now: datetime) -> bool:
    updated_at_raw = str((audit_state or {}).get("updated_at") or "").strip()
    if not updated_at_raw:
        return True
    try:
        updated_at = datetime.fromisoformat(updated_at_raw)
    except ValueError:
        return True
    if updated_at.tzinfo is None:
        updated_at = updated_at.replace(tzinfo=timezone.utc)
    elapsed = (now.astimezone(timezone.utc) - updated_at.astimezone(timezone.utc)).total_seconds()
    return elapsed >= AUDIT_SYNC_MIN_INTERVAL_SECONDS


def _fetch_trade_rows_since(
    *,
    symbol: str,
    api_key: str,
    api_secret: str,
    start_time_ms: int,
    recv_window: int,
) -> list[dict[str, Any]]:
    end_time_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    return fetch_time_paged(
        fetch_page=lambda **params: fetch_futures_user_trades(
            symbol=symbol,
            api_key=api_key,
            api_secret=api_secret,
            recv_window=recv_window,
            **params,
        ),
        start_time_ms=start_time_ms,
        end_time_ms=end_time_ms,
        limit=1000,
        row_time_ms=trade_row_time_ms,
        row_key=trade_row_key,
    )


def _fetch_income_rows_since(
    *,
    symbol: str,
    api_key: str,
    api_secret: str,
    start_time_ms: int,
    recv_window: int,
) -> list[dict[str, Any]]:
    end_time_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    return fetch_time_paged(
        fetch_page=lambda **params: fetch_futures_income_history(
            symbol=symbol,
            income_type="FUNDING_FEE",
            api_key=api_key,
            api_secret=api_secret,
            recv_window=recv_window,
            **params,
        ),
        start_time_ms=start_time_ms,
        end_time_ms=end_time_ms,
        limit=1000,
        row_time_ms=income_row_time_ms,
        row_key=income_row_key,
    )


def _sync_account_audit(
    *,
    symbol: str,
    cycle: int,
    summary_path: Path,
    recv_window: int,
) -> dict[str, Any]:
    credentials = load_binance_api_credentials()
    if credentials is None:
        return {"trade_appended": 0, "income_appended": 0, "skipped": "missing_credentials"}
    api_key, api_secret = credentials

    audit_paths = build_audit_paths(summary_path)
    audit_state_path = audit_paths["audit_state"]
    audit_state = _load_audit_state(audit_state_path)
    now = datetime.now(timezone.utc)
    if not _should_sync_account_audit(audit_state, now=now):
        return {
            "trade_appended": 0,
            "income_appended": 0,
            "skipped": f"throttled<{int(AUDIT_SYNC_MIN_INTERVAL_SECONDS)}s",
            "trade_audit_path": str(audit_paths["trade_audit"]),
            "income_audit_path": str(audit_paths["income_audit"]),
            "audit_state_path": str(audit_state_path),
        }
    default_start_time_ms = int((now - timedelta(days=DEFAULT_AUDIT_LOOKBACK_DAYS)).timestamp() * 1000)
    event_start, _ = scan_iso_bounds(summary_path)
    event_start_time_ms = int(event_start.timestamp() * 1000) if event_start else 0

    trade_start_time_ms = int(audit_state.get("trade_last_time_ms") or 0)
    if trade_start_time_ms <= 0:
        trade_start_time_ms = int(audit_state.get("session_start_time_ms") or 0) or default_start_time_ms
    income_start_time_ms = int(audit_state.get("income_last_time_ms") or 0)
    if income_start_time_ms <= 0:
        income_start_time_ms = int(audit_state.get("session_start_time_ms") or 0) or default_start_time_ms

    trade_rows = _fetch_trade_rows_since(
        symbol=symbol,
        api_key=api_key,
        api_secret=api_secret,
        start_time_ms=trade_start_time_ms,
        recv_window=recv_window,
    )
    fresh_trades, trade_last_time_ms, trade_keys_at_time = collect_new_rows(
        rows=trade_rows,
        last_time_ms=int(audit_state.get("trade_last_time_ms") or 0) or None,
        last_keys_at_time=list(audit_state.get("trade_last_keys_at_time", [])),
        row_time_ms=trade_row_time_ms,
        row_key=trade_row_key,
    )

    income_rows = _fetch_income_rows_since(
        symbol=symbol,
        api_key=api_key,
        api_secret=api_secret,
        start_time_ms=income_start_time_ms,
        recv_window=recv_window,
    )
    fresh_income, income_last_time_ms, income_keys_at_time = collect_new_rows(
        rows=income_rows,
        last_time_ms=int(audit_state.get("income_last_time_ms") or 0) or None,
        last_keys_at_time=list(audit_state.get("income_last_keys_at_time", [])),
        row_time_ms=income_row_time_ms,
        row_key=income_row_key,
    )

    synced_at = datetime.now(timezone.utc).isoformat()
    for row in fresh_trades:
        payload = dict(row)
        payload["audit_cycle"] = cycle
        payload["audit_synced_at"] = synced_at
        _append_jsonl(audit_paths["trade_audit"], payload)
    for row in fresh_income:
        payload = dict(row)
        payload["audit_cycle"] = cycle
        payload["audit_synced_at"] = synced_at
        _append_jsonl(audit_paths["income_audit"], payload)

    session_start_time_ms = int(audit_state.get("session_start_time_ms") or 0)
    earliest_appended_trade_time = min((trade_row_time_ms(item) for item in fresh_trades), default=0)
    earliest_appended_income_time = min((income_row_time_ms(item) for item in fresh_income), default=0)
    earliest_appended = min(value for value in [earliest_appended_trade_time, earliest_appended_income_time] if value > 0) if any(
        value > 0 for value in [earliest_appended_trade_time, earliest_appended_income_time]
    ) else 0
    if session_start_time_ms <= 0:
        session_start_time_ms = earliest_appended or event_start_time_ms or default_start_time_ms
    elif earliest_appended > 0:
        session_start_time_ms = min(session_start_time_ms, earliest_appended)

    audit_state.update(
        {
            "symbol": symbol.upper().strip(),
            "updated_at": synced_at,
            "session_start_time_ms": session_start_time_ms,
            "trade_last_time_ms": trade_last_time_ms,
            "trade_last_keys_at_time": trade_keys_at_time,
            "income_last_time_ms": income_last_time_ms,
            "income_last_keys_at_time": income_keys_at_time,
        }
    )
    _write_json(audit_state_path, audit_state)
    return {
        "trade_appended": len(fresh_trades),
        "income_appended": len(fresh_income),
        "trade_audit_path": str(audit_paths["trade_audit"]),
        "income_audit_path": str(audit_paths["income_audit"]),
        "audit_state_path": str(audit_state_path),
    }


def apply_position_controls(
    *,
    plan: dict[str, Any],
    current_long_qty: float,
    mid_price: float,
    pause_buy_position_notional: float | None,
    min_mid_price_for_buys: float | None,
    external_buy_pause: bool = False,
    external_pause_reasons: list[str] | None = None,
) -> dict[str, Any]:
    current_long_notional = max(current_long_qty, 0.0) * max(mid_price, 0.0)
    buy_paused = bool(external_buy_pause)
    reasons: list[str] = list(external_pause_reasons or [])
    if pause_buy_position_notional is not None and current_long_notional >= pause_buy_position_notional:
        buy_paused = True
        reasons.append(
            f"current_long_notional={_float(current_long_notional)} >= pause_buy_position_notional={_float(pause_buy_position_notional)}"
        )
    if min_mid_price_for_buys is not None and mid_price <= min_mid_price_for_buys:
        buy_paused = True
        reasons.append(
            f"mid_price={_price(mid_price)} <= min_mid_price_for_buys={_price(min_mid_price_for_buys)}"
        )
    if buy_paused:
        plan["bootstrap_orders"] = []
        plan["buy_orders"] = []
    return {
        "buy_paused": buy_paused,
        "pause_reasons": reasons,
        "current_long_notional": current_long_notional,
    }


def apply_excess_inventory_reduce_only(
    *,
    plan: dict[str, Any],
    strategy_mode: str,
    current_long_qty: float,
    current_short_qty: float,
    target_base_qty: float,
    enabled: bool,
) -> dict[str, Any]:
    result = {
        "enabled": bool(enabled),
        "active": False,
        "direction": None,
        "excess_qty": 0.0,
        "reason": None,
    }
    if not enabled:
        return result

    tolerance = 1e-9
    mode = str(strategy_mode).strip()
    safe_target_base_qty = max(float(target_base_qty), 0.0)
    if mode == "one_way_long":
        excess_qty = max(float(current_long_qty) - safe_target_base_qty, 0.0)
        result["excess_qty"] = excess_qty
        if excess_qty > tolerance:
            plan["bootstrap_orders"] = []
            plan["buy_orders"] = []
            result.update(
                {
                    "active": True,
                    "direction": "long",
                    "reason": (
                        f"current_long_qty={current_long_qty:.8f} > "
                        f"target_base_qty={safe_target_base_qty:.8f}"
                    ),
                }
            )
    elif mode == "one_way_short":
        excess_qty = max(float(current_short_qty) - safe_target_base_qty, 0.0)
        result["excess_qty"] = excess_qty
        if excess_qty > tolerance:
            plan["bootstrap_orders"] = []
            plan["sell_orders"] = []
            result.update(
                {
                    "active": True,
                    "direction": "short",
                    "reason": (
                        f"current_short_qty={current_short_qty:.8f} > "
                        f"target_base_qty={safe_target_base_qty:.8f}"
                    ),
                }
            )
    return result


def apply_hedge_position_controls(
    *,
    plan: dict[str, Any],
    current_long_qty: float,
    current_short_qty: float,
    mid_price: float,
    pause_long_position_notional: float | None,
    pause_short_position_notional: float | None,
    min_mid_price_for_buys: float | None,
    external_long_pause: bool = False,
    external_pause_reasons: list[str] | None = None,
) -> dict[str, Any]:
    current_long_notional = max(current_long_qty, 0.0) * max(mid_price, 0.0)
    current_short_notional = max(current_short_qty, 0.0) * max(mid_price, 0.0)
    long_paused = bool(external_long_pause)
    short_paused = False
    long_reasons: list[str] = list(external_pause_reasons or [])
    short_reasons: list[str] = []

    if pause_long_position_notional is not None and current_long_notional >= pause_long_position_notional:
        long_paused = True
        long_reasons.append(
            f"current_long_notional={_float(current_long_notional)} >= pause_long_position_notional={_float(pause_long_position_notional)}"
        )
    if pause_short_position_notional is not None and current_short_notional >= pause_short_position_notional:
        short_paused = True
        short_reasons.append(
            f"current_short_notional={_float(current_short_notional)} >= pause_short_position_notional={_float(pause_short_position_notional)}"
        )
    if min_mid_price_for_buys is not None and mid_price <= min_mid_price_for_buys:
        long_paused = True
        long_reasons.append(
            f"mid_price={_price(mid_price)} <= min_mid_price_for_buys={_price(min_mid_price_for_buys)}"
        )

    if long_paused:
        plan["bootstrap_orders"] = [item for item in plan.get("bootstrap_orders", []) if not _is_long_entry_order(item)]
        plan["buy_orders"] = [item for item in plan.get("buy_orders", []) if not _is_long_entry_order(item)]
    if short_paused:
        plan["bootstrap_orders"] = [item for item in plan.get("bootstrap_orders", []) if not _is_short_entry_order(item)]
        plan["sell_orders"] = [item for item in plan.get("sell_orders", []) if not _is_short_entry_order(item)]

    return {
        "long_paused": long_paused,
        "short_paused": short_paused,
        "long_pause_reasons": long_reasons,
        "short_pause_reasons": short_reasons,
        "buy_paused": long_paused,
        "pause_reasons": long_reasons + short_reasons,
        "current_long_notional": current_long_notional,
        "current_short_notional": current_short_notional,
    }


def apply_short_cover_pause(
    *,
    plan: dict[str, Any],
    active: bool,
    pause_reasons: list[str] | None = None,
) -> dict[str, Any]:
    reasons = list(pause_reasons or [])
    if active:
        plan["buy_orders"] = []
    return {
        "short_cover_paused": bool(active),
        "short_cover_pause_reasons": reasons,
    }


def assess_market_guard(
    *,
    symbol: str,
    buy_pause_amp_trigger_ratio: float | None,
    buy_pause_down_return_trigger_ratio: float | None,
    short_cover_pause_amp_trigger_ratio: float | None,
    short_cover_pause_down_return_trigger_ratio: float | None,
    freeze_shift_abs_return_trigger_ratio: float | None,
    now: datetime | None = None,
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

    current_time = now or datetime.now(timezone.utc)
    end_ms = int(current_time.timestamp() * 1000)
    start_ms = end_ms - int(timedelta(minutes=3).total_seconds() * 1000)
    try:
        candles = fetch_futures_klines(
            symbol=symbol,
            interval="1m",
            start_ms=start_ms,
            end_ms=end_ms,
            limit=10,
        )
    except Exception as exc:
        guard["warning"] = f"{exc.__class__.__name__}: {exc}"
        return guard

    closed_candles = [item for item in candles if item.close_time <= current_time]
    if not closed_candles:
        guard["warning"] = "no_closed_1m_candle"
        return guard

    candle = closed_candles[-1]
    open_price = max(float(candle.open), 0.0)
    high_price = max(float(candle.high), 0.0)
    low_price = max(float(candle.low), 0.0)
    close_price = max(float(candle.close), 0.0)
    return_ratio = ((close_price / open_price) - 1.0) if open_price > 0 else 0.0
    amplitude_ratio = ((high_price / low_price) - 1.0) if low_price > 0 else 0.0

    buy_pause_reasons: list[str] = []
    short_cover_pause_reasons: list[str] = []
    shift_freeze_reasons: list[str] = []

    def _matches_extreme_down(amp_trigger: float | None, down_trigger: float | None) -> bool:
        return (
            amp_trigger is not None
            and amp_trigger > 0
            and down_trigger is not None
            and amplitude_ratio >= amp_trigger
            and return_ratio <= down_trigger
        )

    if _matches_extreme_down(buy_pause_amp_trigger_ratio, buy_pause_down_return_trigger_ratio):
        buy_pause_reasons.append(
            "1m_extreme_down_candle "
            f"amp={amplitude_ratio * 100:.2f}% "
            f"ret={return_ratio * 100:.2f}%"
        )
    if _matches_extreme_down(short_cover_pause_amp_trigger_ratio, short_cover_pause_down_return_trigger_ratio):
        short_cover_pause_reasons.append(
            "1m_short_cover_pause "
            f"amp={amplitude_ratio * 100:.2f}% "
            f"ret={return_ratio * 100:.2f}%"
        )
    if (
        freeze_shift_abs_return_trigger_ratio is not None
        and freeze_shift_abs_return_trigger_ratio > 0
        and abs(return_ratio) >= freeze_shift_abs_return_trigger_ratio
    ):
        shift_freeze_reasons.append(
            "1m_shift_freeze "
            f"abs_ret={abs(return_ratio) * 100:.2f}%"
        )

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


def _aggregate_recent_window(candles: list[Any], *, count: int) -> dict[str, Any] | None:
    if count <= 0 or len(candles) < count:
        return None
    window = candles[-count:]
    open_price = max(float(window[0].open), 0.0)
    close_price = max(float(window[-1].close), 0.0)
    high_price = max(max(float(item.high), 0.0) for item in window)
    low_price = min(max(float(item.low), 0.0) for item in window)
    return_ratio = ((close_price / open_price) - 1.0) if open_price > 0 else 0.0
    amplitude_ratio = ((high_price / low_price) - 1.0) if low_price > 0 else 0.0
    return {
        "open_time": window[0].open_time.isoformat(),
        "close_time": window[-1].close_time.isoformat(),
        "open": open_price,
        "close": close_price,
        "high": high_price,
        "low": low_price,
        "return_ratio": return_ratio,
        "amplitude_ratio": amplitude_ratio,
    }


def _latest_closed_window(candles: list[Any], *, now: datetime | None = None) -> dict[str, Any] | None:
    current_time = now or datetime.now(timezone.utc)
    closed = [item for item in candles if item.close_time <= current_time]
    if not closed:
        return None
    candle = closed[-1]
    open_price = max(float(candle.open), 0.0)
    high_price = max(float(candle.high), 0.0)
    low_price = max(float(candle.low), 0.0)
    close_price = max(float(candle.close), 0.0)
    amplitude_ratio = ((high_price / low_price) - 1.0) if low_price > 0 else 0.0
    return_ratio = ((close_price / open_price) - 1.0) if open_price > 0 else 0.0
    return {
        "open_time": candle.open_time.isoformat(),
        "close_time": candle.close_time.isoformat(),
        "open": open_price,
        "high": high_price,
        "low": low_price,
        "close": close_price,
        "amplitude_ratio": amplitude_ratio,
        "return_ratio": return_ratio,
    }


def assess_xaut_adaptive_regime(
    *,
    symbol: str,
    strategy_mode: str,
    now: datetime | None = None,
) -> dict[str, Any]:
    report = {
        "enabled": True,
        "available": False,
        "warning": None,
        "candidate_state": XAUT_ADAPTIVE_STATE_DEFENSIVE,
        "reason": None,
        "metrics": {},
    }
    current_time = now or datetime.now(timezone.utc)
    normalized_mode = str(strategy_mode or "").strip() or "one_way_long"
    try:
        candles_15m = fetch_futures_klines(symbol=symbol, interval="15m", limit=8)
        candles_60m = fetch_futures_klines(symbol=symbol, interval="1h", limit=8)
    except Exception as exc:
        report["warning"] = f"{exc.__class__.__name__}: {exc}"
        return report

    window_15m = _latest_closed_window(candles_15m, now=current_time)
    window_60m = _latest_closed_window(candles_60m, now=current_time)
    if window_15m is None or window_60m is None:
        report["warning"] = "insufficient_closed_candles"
        return report

    report["available"] = True
    report["metrics"] = {
        "window_15m": window_15m,
        "window_60m": window_60m,
    }

    amp_15m = float(window_15m["amplitude_ratio"])
    amp_60m = float(window_60m["amplitude_ratio"])
    ret_15m = float(window_15m["return_ratio"])
    ret_60m = float(window_60m["return_ratio"])

    if normalized_mode == "one_way_short":
        reduce_only = (
            amp_15m >= 0.009
            or amp_60m >= 0.016
            or ret_15m >= 0.007
            or ret_60m >= 0.012
        )
        defensive = (
            amp_15m >= 0.006
            or amp_60m >= 0.012
            or ret_15m >= 0.004
            or ret_60m >= 0.008
        )
        normal = amp_15m <= 0.0035 and amp_60m <= 0.0075 and ret_60m <= 0.003
    else:
        reduce_only = (
            amp_15m >= 0.009
            or amp_60m >= 0.016
            or ret_15m <= -0.007
            or ret_60m <= -0.012
        )
        defensive = (
            amp_15m >= 0.006
            or amp_60m >= 0.012
            or ret_15m <= -0.004
            or ret_60m <= -0.008
        )
        normal = amp_15m <= 0.0035 and amp_60m <= 0.0075 and ret_60m >= -0.003

    if reduce_only:
        candidate_state = XAUT_ADAPTIVE_STATE_REDUCE_ONLY
    elif defensive or not normal:
        candidate_state = XAUT_ADAPTIVE_STATE_DEFENSIVE
    else:
        candidate_state = XAUT_ADAPTIVE_STATE_NORMAL

    report["candidate_state"] = candidate_state
    report["reason"] = (
        f"15m amp={amp_15m * 100:.2f}% ret={ret_15m * 100:.2f}% · "
        f"60m amp={amp_60m * 100:.2f}% ret={ret_60m * 100:.2f}%"
    )
    return report


def resolve_xaut_adaptive_state(
    *,
    state: dict[str, Any],
    regime_report: dict[str, Any],
    strategy_mode: str,
    now: datetime | None = None,
) -> dict[str, Any]:
    current_time = now or datetime.now(timezone.utc)
    adaptive_state = dict(state.get("xaut_adaptive_state") or {})
    active_state = str(adaptive_state.get("active_state") or XAUT_ADAPTIVE_STATE_NORMAL).strip() or XAUT_ADAPTIVE_STATE_NORMAL
    pending_state = str(adaptive_state.get("pending_state") or "").strip() or None
    pending_count = int(adaptive_state.get("pending_count") or 0)
    candidate_state = str(regime_report.get("candidate_state") or active_state).strip() or active_state
    warning = regime_report.get("warning")
    switched = False
    report_available = bool(regime_report.get("available", "candidate_state" in regime_report))

    if not report_available:
        candidate_state = active_state
        pending_state = None
        pending_count = 0
    elif active_state == XAUT_ADAPTIVE_STATE_REDUCE_ONLY and candidate_state == XAUT_ADAPTIVE_STATE_NORMAL:
        candidate_state = XAUT_ADAPTIVE_STATE_DEFENSIVE

    if candidate_state != active_state:
        if pending_state == candidate_state:
            pending_count += 1
        else:
            pending_state = candidate_state
            pending_count = 1
        if pending_count >= _xaut_transition_confirm_cycles(active_state, candidate_state):
            active_state = candidate_state
            pending_state = None
            pending_count = 0
            switched = True
    else:
        pending_state = None
        pending_count = 0

    adaptive_state.update(
        {
            "enabled": True,
            "direction": "short" if str(strategy_mode).strip() == "one_way_short" else "long",
            "active_state": active_state,
            "candidate_state": candidate_state,
            "pending_state": pending_state,
            "pending_count": pending_count,
            "reason": regime_report.get("reason"),
            "metrics": dict(regime_report.get("metrics") or {}),
            "warning": warning,
            "switched": switched,
            "updated_at": current_time.isoformat(),
        }
    )
    state["xaut_adaptive_state"] = adaptive_state
    return adaptive_state


def build_xaut_adaptive_runner_args(
    *,
    args: argparse.Namespace,
    active_state: str,
) -> argparse.Namespace:
    effective = argparse.Namespace(**vars(args))
    strategy_mode = str(getattr(args, "strategy_mode", "one_way_long")).strip() or "one_way_long"
    overrides = dict(XAUT_ADAPTIVE_STATE_CONFIGS.get(strategy_mode, {}).get(str(active_state), {}))
    for key, value in overrides.items():
        setattr(effective, key, value)
    return effective


def apply_xaut_reduce_only_pruning(*, plan: dict[str, Any], strategy_mode: str) -> None:
    mode = str(strategy_mode).strip()
    plan["bootstrap_orders"] = []
    if mode == "one_way_short":
        plan["sell_orders"] = []
    else:
        plan["buy_orders"] = []


def assess_auto_regime(
    *,
    symbol: str,
    stable_15m_max_amplitude_ratio: float,
    stable_60m_max_amplitude_ratio: float,
    stable_60m_return_floor_ratio: float,
    defensive_15m_amplitude_ratio: float,
    defensive_60m_amplitude_ratio: float,
    defensive_15m_return_ratio: float,
    defensive_60m_return_ratio: float,
    now: datetime | None = None,
) -> dict[str, Any]:
    report = {
        "enabled": True,
        "available": False,
        "warning": None,
        "regime": "unknown",
        "candidate_profile": None,
        "reason": None,
        "metrics": {},
    }
    current_time = now or datetime.now(timezone.utc)
    end_ms = int(current_time.timestamp() * 1000)
    start_ms = end_ms - int(timedelta(minutes=125).total_seconds() * 1000)
    try:
        candles = fetch_futures_klines(
            symbol=symbol,
            interval="5m",
            start_ms=start_ms,
            end_ms=end_ms,
            limit=40,
        )
    except Exception as exc:
        report["warning"] = f"{exc.__class__.__name__}: {exc}"
        return report

    closed_candles = [item for item in candles if item.close_time <= current_time]
    window_15m = _aggregate_recent_window(closed_candles, count=3)
    window_60m = _aggregate_recent_window(closed_candles, count=12)
    if window_15m is None or window_60m is None:
        report["warning"] = "insufficient_closed_5m_candles"
        return report

    report["available"] = True
    report["metrics"] = {
        "window_15m": window_15m,
        "window_60m": window_60m,
    }

    amp_15m = float(window_15m["amplitude_ratio"])
    amp_60m = float(window_60m["amplitude_ratio"])
    ret_15m = float(window_15m["return_ratio"])
    ret_60m = float(window_60m["return_ratio"])

    defensive = (
        amp_15m >= defensive_15m_amplitude_ratio
        or amp_60m >= defensive_60m_amplitude_ratio
        or ret_15m <= defensive_15m_return_ratio
        or ret_60m <= defensive_60m_return_ratio
        or (amp_15m >= defensive_15m_amplitude_ratio * 0.8 and ret_15m <= defensive_15m_return_ratio * 0.5)
    )
    stable = (
        amp_15m <= stable_15m_max_amplitude_ratio
        and amp_60m <= stable_60m_max_amplitude_ratio
        and ret_60m >= stable_60m_return_floor_ratio
    )

    if defensive:
        report["regime"] = "defensive"
        report["candidate_profile"] = AUTO_REGIME_DEFENSIVE_PROFILE
        report["reason"] = (
            f"15m amp={amp_15m * 100:.2f}% ret={ret_15m * 100:.2f}% · "
            f"60m amp={amp_60m * 100:.2f}% ret={ret_60m * 100:.2f}%"
        )
    elif stable:
        report["regime"] = "stable"
        report["candidate_profile"] = AUTO_REGIME_STABLE_PROFILE
        report["reason"] = (
            f"15m amp={amp_15m * 100:.2f}% · "
            f"60m amp={amp_60m * 100:.2f}% ret={ret_60m * 100:.2f}%"
        )
    else:
        report["regime"] = "transition"
        report["reason"] = (
            f"15m amp={amp_15m * 100:.2f}% ret={ret_15m * 100:.2f}% · "
            f"60m amp={amp_60m * 100:.2f}% ret={ret_60m * 100:.2f}%"
        )
    return report


def determine_interval_center_price(
    *,
    symbol: str,
    interval_minutes: int,
    tick_size: float | None,
    now: datetime | None = None,
) -> dict[str, Any]:
    if interval_minutes not in {3, 5}:
        raise ValueError("neutral center interval currently supports 3m or 5m")
    current_time = now or datetime.now(timezone.utc)
    end_ms = int(current_time.timestamp() * 1000)
    start_ms = end_ms - int(timedelta(minutes=interval_minutes * 4).total_seconds() * 1000)
    interval = f"{interval_minutes}m"
    report = {
        "available": False,
        "warning": None,
        "interval": interval,
        "center_price": None,
        "candle": None,
    }
    try:
        candles = fetch_futures_klines(
            symbol=symbol,
            interval=interval,
            start_ms=start_ms,
            end_ms=end_ms,
            limit=10,
        )
    except Exception as exc:
        report["warning"] = f"{exc.__class__.__name__}: {exc}"
        return report
    closed_candles = [item for item in candles if item.close_time <= current_time]
    if not closed_candles:
        report["warning"] = f"no_closed_{interval}_candle"
        return report
    candle = closed_candles[-1]
    center_price = _round_to_nearest_step(float(candle.close), tick_size)
    report.update(
        {
            "available": True,
            "center_price": center_price,
            "candle": {
                "open_time": candle.open_time.isoformat(),
                "close_time": candle.close_time.isoformat(),
                "open": float(candle.open),
                "high": float(candle.high),
                "low": float(candle.low),
                "close": float(candle.close),
            },
        }
    )
    return report


def resolve_neutral_hourly_scale(
    *,
    state: dict[str, Any],
    symbol: str,
    enabled: bool,
    stable_scale: float,
    transition_scale: float,
    defensive_scale: float,
    stable_15m_max_amplitude_ratio: float,
    stable_60m_max_amplitude_ratio: float,
    stable_60m_return_floor_ratio: float,
    defensive_15m_amplitude_ratio: float,
    defensive_60m_amplitude_ratio: float,
    defensive_15m_return_ratio: float,
    defensive_60m_return_ratio: float,
    now: datetime | None = None,
) -> dict[str, Any]:
    current_time = now or datetime.now(timezone.utc)
    bucket_start = current_time.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)
    bucket = bucket_start.isoformat()
    cached = dict(state.get("neutral_hourly_scale_state") or {})
    if enabled and str(cached.get("bucket", "")).strip() == bucket:
        return {
            "enabled": True,
            "bucket": bucket,
            "scale": float(cached.get("scale", 1.0) or 1.0),
            "regime": str(cached.get("regime", "unknown")).strip() or "unknown",
            "reason": cached.get("reason"),
            "metrics": dict(cached.get("metrics") or {}),
            "cached": True,
        }

    report = {
        "enabled": enabled,
        "bucket": bucket,
        "scale": 1.0,
        "regime": "disabled",
        "reason": None,
        "metrics": {},
        "cached": False,
    }
    if not enabled:
        state["neutral_hourly_scale_state"] = {
            "bucket": bucket,
            "scale": 1.0,
            "regime": "disabled",
            "reason": None,
            "metrics": {},
        }
        return report

    regime_report = assess_auto_regime(
        symbol=symbol,
        stable_15m_max_amplitude_ratio=stable_15m_max_amplitude_ratio,
        stable_60m_max_amplitude_ratio=stable_60m_max_amplitude_ratio,
        stable_60m_return_floor_ratio=stable_60m_return_floor_ratio,
        defensive_15m_amplitude_ratio=defensive_15m_amplitude_ratio,
        defensive_60m_amplitude_ratio=defensive_60m_amplitude_ratio,
        defensive_15m_return_ratio=defensive_15m_return_ratio,
        defensive_60m_return_ratio=defensive_60m_return_ratio,
        now=current_time,
    )
    regime = str(regime_report.get("regime", "transition")).strip() or "transition"
    if not regime_report.get("available"):
        regime = "transition"
    scale_map = {
        "stable": float(stable_scale),
        "transition": float(transition_scale),
        "defensive": float(defensive_scale),
    }
    scale = max(scale_map.get(regime, float(transition_scale)), 0.0)
    report.update(
        {
            "scale": scale,
            "regime": regime,
            "reason": regime_report.get("reason") or regime_report.get("warning"),
            "metrics": dict(regime_report.get("metrics") or {}),
        }
    )
    state["neutral_hourly_scale_state"] = {
        "bucket": bucket,
        "scale": scale,
        "regime": regime,
        "reason": report["reason"],
        "metrics": report["metrics"],
    }
    return report


def resolve_auto_regime_profile(
    *,
    state: dict[str, Any],
    regime_report: dict[str, Any],
    confirm_cycles: int,
    now: datetime | None = None,
) -> dict[str, Any]:
    current_time = now or datetime.now(timezone.utc)
    auto_state = dict(state.get("auto_regime_state") or {})
    active_profile = str(auto_state.get("active_profile") or AUTO_REGIME_STABLE_PROFILE).strip() or AUTO_REGIME_STABLE_PROFILE
    pending_profile = str(auto_state.get("pending_profile") or "").strip() or None
    pending_count = int(auto_state.get("pending_count") or 0)
    candidate_profile = regime_report.get("candidate_profile")
    regime = str(regime_report.get("regime") or "unknown").strip() or "unknown"
    switched = False

    if candidate_profile and candidate_profile != active_profile:
        if pending_profile == candidate_profile:
            pending_count += 1
        else:
            pending_profile = candidate_profile
            pending_count = 1
        if pending_count >= max(confirm_cycles, 1):
            active_profile = candidate_profile
            pending_profile = None
            pending_count = 0
            switched = True
            auto_state["last_switched_at"] = current_time.isoformat()
            auto_state["last_switch_reason"] = regime_report.get("reason")
            auto_state["last_regime"] = regime
    else:
        pending_profile = None
        pending_count = 0

    auto_state.update(
        {
            "active_profile": active_profile,
            "active_profile_label": AUTO_REGIME_PROFILE_LABELS.get(active_profile, active_profile),
            "candidate_profile": candidate_profile,
            "candidate_profile_label": AUTO_REGIME_PROFILE_LABELS.get(candidate_profile, candidate_profile) if candidate_profile else None,
            "pending_profile": pending_profile,
            "pending_count": pending_count,
            "confirm_cycles": max(confirm_cycles, 1),
            "regime": regime,
            "reason": regime_report.get("reason"),
            "metrics": regime_report.get("metrics") if isinstance(regime_report.get("metrics"), dict) else {},
            "warning": regime_report.get("warning"),
            "switched": switched,
            "updated_at": current_time.isoformat(),
        }
    )
    state["auto_regime_state"] = auto_state
    return auto_state


def build_effective_runner_args(
    *,
    args: argparse.Namespace,
    active_profile: str,
    symbol_info: dict[str, Any],
    bid_price: float,
    ask_price: float,
    mid_price: float,
) -> argparse.Namespace:
    effective = argparse.Namespace(**vars(args))
    profile_overrides = AUTO_REGIME_PROFILE_OVERRIDES.get(active_profile, {})
    for key, value in profile_overrides.items():
        setattr(effective, key, value)

    tick_size = _safe_float(symbol_info.get("tick_size"))
    spread = max(ask_price - bid_price, 0.0)
    step_ratio, min_ticks = AUTO_REGIME_PROFILE_STEP_HINTS.get(active_profile, (0.0004, 2))
    desired_step = max(mid_price * step_ratio, tick_size * min_ticks, spread * 2.0)
    if desired_step > 0:
        effective.step_price = _round_up_to_step(desired_step, tick_size if tick_size > 0 else None)
    return effective


def _clamp_ratio(value: float) -> float:
    return min(max(value, 0.0), 1.0)


def apply_inventory_tiering(
    *,
    current_long_notional: float,
    buy_levels: int,
    sell_levels: int,
    per_order_notional: float,
    base_position_notional: float,
    tier_start_notional: float | None,
    tier_end_notional: float | None,
    tier_buy_levels: int | None,
    tier_sell_levels: int | None,
    tier_per_order_notional: float | None,
    tier_base_position_notional: float | None,
) -> dict[str, Any]:
    if tier_start_notional is None or tier_start_notional <= 0:
        return {
            "enabled": False,
            "active": False,
            "ratio": 0.0,
            "start_notional": tier_start_notional,
            "end_notional": tier_end_notional,
            "effective_buy_levels": buy_levels,
            "effective_sell_levels": sell_levels,
            "effective_per_order_notional": per_order_notional,
            "effective_base_position_notional": base_position_notional,
        }

    end_notional = tier_end_notional if tier_end_notional is not None and tier_end_notional > tier_start_notional else tier_start_notional
    if current_long_notional <= tier_start_notional:
        ratio = 0.0
    elif end_notional <= tier_start_notional:
        ratio = 1.0
    else:
        ratio = _clamp_ratio((current_long_notional - tier_start_notional) / (end_notional - tier_start_notional))

    def _lerp(base_value: float, target_value: float | None) -> float:
        if target_value is None:
            return float(base_value)
        return float(base_value) + (float(target_value) - float(base_value)) * ratio

    effective_buy_levels = max(0, int(round(_lerp(float(buy_levels), float(tier_buy_levels) if tier_buy_levels is not None else None))))
    effective_sell_levels = max(0, int(round(_lerp(float(sell_levels), float(tier_sell_levels) if tier_sell_levels is not None else None))))
    effective_per_order_notional = max(_lerp(per_order_notional, tier_per_order_notional), 0.0)
    effective_base_position_notional = max(_lerp(base_position_notional, tier_base_position_notional), 0.0)

    return {
        "enabled": True,
        "active": ratio > 0,
        "ratio": ratio,
        "start_notional": tier_start_notional,
        "end_notional": end_notional,
        "effective_buy_levels": effective_buy_levels,
        "effective_sell_levels": effective_sell_levels,
        "effective_per_order_notional": effective_per_order_notional,
        "effective_base_position_notional": effective_base_position_notional,
    }


def _trim_order_to_notional(
    *,
    order: dict[str, Any],
    remaining_notional: float,
    step_size: float | None,
    min_qty: float | None,
    min_notional: float | None,
) -> dict[str, Any] | None:
    price = _safe_float(order.get("price"))
    qty = _safe_float(order.get("qty"))
    if price <= 0 or qty <= 0 or remaining_notional <= 0:
        return None
    desired_qty = min(qty, _round_order_qty(remaining_notional / price, step_size))
    notional = desired_qty * price
    if desired_qty <= 0:
        return None
    if min_qty is not None and desired_qty < min_qty:
        return None
    if min_notional is not None and notional < min_notional:
        return None
    trimmed = dict(order)
    trimmed["qty"] = float(desired_qty)
    trimmed["notional"] = float(notional)
    return trimmed


def apply_max_position_notional_cap(
    *,
    plan: dict[str, Any],
    current_long_notional: float,
    max_position_notional: float | None,
    step_size: float | None,
    min_qty: float | None,
    min_notional: float | None,
) -> dict[str, Any]:
    if max_position_notional is None or max_position_notional <= 0:
        return {
            "cap_applied": False,
            "max_position_notional": max_position_notional,
            "buy_budget_notional": None,
            "planned_buy_notional": sum(_safe_float(x.get("notional")) for x in plan.get("bootstrap_orders", []))
            + sum(_safe_float(x.get("notional")) for x in plan.get("buy_orders", [])),
        }

    buy_budget_notional = max(max_position_notional - max(current_long_notional, 0.0), 0.0)
    bootstrap_orders = [dict(item) for item in plan.get("bootstrap_orders", []) if isinstance(item, dict)]
    buy_orders = [dict(item) for item in plan.get("buy_orders", []) if isinstance(item, dict)]
    planned_buy_notional = sum(_safe_float(x.get("notional")) for x in bootstrap_orders) + sum(
        _safe_float(x.get("notional")) for x in buy_orders
    )
    if planned_buy_notional <= buy_budget_notional + 1e-12:
        return {
            "cap_applied": False,
            "max_position_notional": max_position_notional,
            "buy_budget_notional": buy_budget_notional,
            "planned_buy_notional": planned_buy_notional,
        }

    remaining_notional = buy_budget_notional
    capped_bootstrap: list[dict[str, Any]] = []
    capped_buys: list[dict[str, Any]] = []

    for order in bootstrap_orders:
        trimmed = _trim_order_to_notional(
            order=order,
            remaining_notional=remaining_notional,
            step_size=step_size,
            min_qty=min_qty,
            min_notional=min_notional,
        )
        if trimmed is None:
            continue
        capped_bootstrap.append(trimmed)
        remaining_notional = max(remaining_notional - _safe_float(trimmed.get("notional")), 0.0)

    for order in buy_orders:
        trimmed = _trim_order_to_notional(
            order=order,
            remaining_notional=remaining_notional,
            step_size=step_size,
            min_qty=min_qty,
            min_notional=min_notional,
        )
        if trimmed is None:
            continue
        capped_buys.append(trimmed)
        remaining_notional = max(remaining_notional - _safe_float(trimmed.get("notional")), 0.0)

    plan["bootstrap_orders"] = capped_bootstrap
    plan["buy_orders"] = capped_buys
    return {
        "cap_applied": True,
        "max_position_notional": max_position_notional,
        "buy_budget_notional": buy_budget_notional,
        "planned_buy_notional": sum(_safe_float(x.get("notional")) for x in capped_bootstrap)
        + sum(_safe_float(x.get("notional")) for x in capped_buys),
    }


def _trim_matching_orders(
    *,
    orders: list[dict[str, Any]],
    remaining_notional: float,
    predicate: Any,
    step_size: float | None,
    min_qty: float | None,
    min_notional: float | None,
) -> tuple[list[dict[str, Any]], float]:
    kept: list[dict[str, Any]] = []
    consumed_notional = 0.0
    budget = max(remaining_notional, 0.0)
    for item in orders:
        order = dict(item)
        if not predicate(order):
            kept.append(order)
            continue
        trimmed = _trim_order_to_notional(
            order=order,
            remaining_notional=budget,
            step_size=step_size,
            min_qty=min_qty,
            min_notional=min_notional,
        )
        if trimmed is None:
            continue
        consumed = _safe_float(trimmed.get("notional"))
        consumed_notional += consumed
        budget = max(budget - consumed, 0.0)
        kept.append(trimmed)
    return kept, consumed_notional


def apply_hedge_position_notional_caps(
    *,
    plan: dict[str, Any],
    current_long_notional: float,
    current_short_notional: float,
    max_long_position_notional: float | None,
    max_short_position_notional: float | None,
    step_size: float | None,
    min_qty: float | None,
    min_notional: float | None,
) -> dict[str, Any]:
    bootstrap_orders = [dict(item) for item in plan.get("bootstrap_orders", []) if isinstance(item, dict)]
    buy_orders = [dict(item) for item in plan.get("buy_orders", []) if isinstance(item, dict)]
    sell_orders = [dict(item) for item in plan.get("sell_orders", []) if isinstance(item, dict)]

    long_budget_notional = None
    short_budget_notional = None
    if max_long_position_notional is not None and max_long_position_notional > 0:
        long_budget_notional = max(max_long_position_notional - max(current_long_notional, 0.0), 0.0)
    if max_short_position_notional is not None and max_short_position_notional > 0:
        short_budget_notional = max(max_short_position_notional - max(current_short_notional, 0.0), 0.0)

    original_planned_long = sum(_safe_float(item.get("notional")) for item in bootstrap_orders if _is_long_entry_order(item))
    original_planned_long += sum(_safe_float(item.get("notional")) for item in buy_orders if _is_long_entry_order(item))
    original_planned_short = sum(_safe_float(item.get("notional")) for item in bootstrap_orders if _is_short_entry_order(item))
    original_planned_short += sum(_safe_float(item.get("notional")) for item in sell_orders if _is_short_entry_order(item))

    planned_long_notional = original_planned_long
    planned_short_notional = original_planned_short
    long_cap_applied = False
    short_cap_applied = False

    if long_budget_notional is not None and original_planned_long > long_budget_notional + 1e-12:
        long_cap_applied = True
        bootstrap_orders, consumed_bootstrap = _trim_matching_orders(
            orders=bootstrap_orders,
            remaining_notional=long_budget_notional,
            predicate=_is_long_entry_order,
            step_size=step_size,
            min_qty=min_qty,
            min_notional=min_notional,
        )
        buy_orders, consumed_buys = _trim_matching_orders(
            orders=buy_orders,
            remaining_notional=max(long_budget_notional - consumed_bootstrap, 0.0),
            predicate=_is_long_entry_order,
            step_size=step_size,
            min_qty=min_qty,
            min_notional=min_notional,
        )
        planned_long_notional = consumed_bootstrap + consumed_buys

    if short_budget_notional is not None and original_planned_short > short_budget_notional + 1e-12:
        short_cap_applied = True
        bootstrap_orders, consumed_bootstrap = _trim_matching_orders(
            orders=bootstrap_orders,
            remaining_notional=short_budget_notional,
            predicate=_is_short_entry_order,
            step_size=step_size,
            min_qty=min_qty,
            min_notional=min_notional,
        )
        sell_orders, consumed_sells = _trim_matching_orders(
            orders=sell_orders,
            remaining_notional=max(short_budget_notional - consumed_bootstrap, 0.0),
            predicate=_is_short_entry_order,
            step_size=step_size,
            min_qty=min_qty,
            min_notional=min_notional,
        )
        planned_short_notional = consumed_bootstrap + consumed_sells

    plan["bootstrap_orders"] = bootstrap_orders
    plan["buy_orders"] = buy_orders
    plan["sell_orders"] = sell_orders
    return {
        "cap_applied": long_cap_applied or short_cap_applied,
        "buy_cap_applied": long_cap_applied,
        "short_cap_applied": short_cap_applied,
        "max_position_notional": max_long_position_notional,
        "max_short_position_notional": max_short_position_notional,
        "buy_budget_notional": long_budget_notional,
        "short_budget_notional": short_budget_notional,
        "planned_buy_notional": planned_long_notional,
        "planned_short_notional": planned_short_notional,
    }


def generate_plan_report(args: argparse.Namespace) -> dict[str, Any]:
    symbol = args.symbol.upper().strip()
    strategy_mode = str(getattr(args, "strategy_mode", "one_way_long")).strip() or "one_way_long"
    summary_path = Path(args.summary_jsonl)
    if strategy_mode not in {"one_way_long", "one_way_short", "hedge_neutral", "synthetic_neutral", "inventory_target_neutral"}:
        raise RuntimeError(f"Unsupported strategy_mode: {strategy_mode}")
    requested_strategy_profile = str(getattr(args, "strategy_profile", AUTO_REGIME_STABLE_PROFILE)).strip() or AUTO_REGIME_STABLE_PROFILE
    symbol_info = fetch_futures_symbol_config(symbol)
    book_rows = fetch_futures_book_tickers(symbol=symbol)
    premium_rows = fetch_futures_premium_index(symbol=symbol)
    if not book_rows:
        raise RuntimeError(f"No book ticker returned for {symbol}")
    if not premium_rows:
        raise RuntimeError(f"No premium index returned for {symbol}")
    book = book_rows[0]
    premium = premium_rows[0]
    bid_price = float(book["bid_price"])
    ask_price = float(book["ask_price"])
    mid_price = (bid_price + ask_price) / 2.0

    state_path = Path(args.state_path)
    planner_args = _planner_namespace(args)
    state = load_or_initialize_state(
        state_path=state_path,
        args=planner_args,
        symbol_info=symbol_info,
        mid_price=mid_price,
        reset_state=args.reset_state,
    )
    auto_regime: dict[str, Any] | None = None
    xaut_adaptive: dict[str, Any] | None = None
    effective_args = args
    effective_strategy_profile = requested_strategy_profile
    if is_xaut_adaptive_profile(requested_strategy_profile):
        if symbol != "XAUTUSDT":
            raise RuntimeError("XAUT adaptive profiles require symbol=XAUTUSDT")
        expected_mode = XAUT_ADAPTIVE_PROFILES.get(requested_strategy_profile)
        if expected_mode and strategy_mode != expected_mode:
            raise RuntimeError(f"{requested_strategy_profile} requires strategy_mode={expected_mode}")
        regime_report = assess_xaut_adaptive_regime(
            symbol=symbol,
            strategy_mode=strategy_mode,
        )
        xaut_adaptive = resolve_xaut_adaptive_state(
            state=state,
            regime_report=regime_report,
            strategy_mode=strategy_mode,
        )
        effective_args = build_xaut_adaptive_runner_args(
            args=args,
            active_state=str(xaut_adaptive.get("active_state") or XAUT_ADAPTIVE_STATE_NORMAL),
        )
    elif getattr(args, "auto_regime_enabled", False) and strategy_mode == "one_way_long":
        regime_report = assess_auto_regime(
            symbol=symbol,
            stable_15m_max_amplitude_ratio=args.auto_regime_stable_15m_max_amplitude_ratio,
            stable_60m_max_amplitude_ratio=args.auto_regime_stable_60m_max_amplitude_ratio,
            stable_60m_return_floor_ratio=args.auto_regime_stable_60m_return_floor_ratio,
            defensive_15m_amplitude_ratio=args.auto_regime_defensive_15m_amplitude_ratio,
            defensive_60m_amplitude_ratio=args.auto_regime_defensive_60m_amplitude_ratio,
            defensive_15m_return_ratio=args.auto_regime_defensive_15m_return_ratio,
            defensive_60m_return_ratio=args.auto_regime_defensive_60m_return_ratio,
        )
        auto_regime = resolve_auto_regime_profile(
            state=state,
            regime_report=regime_report,
            confirm_cycles=args.auto_regime_confirm_cycles,
        )
        effective_strategy_profile = str(auto_regime.get("active_profile") or requested_strategy_profile).strip() or requested_strategy_profile
        effective_args = build_effective_runner_args(
            args=args,
            active_profile=effective_strategy_profile,
            symbol_info=symbol_info,
            bid_price=bid_price,
            ask_price=ask_price,
            mid_price=mid_price,
        )
    market_guard = assess_market_guard(
        symbol=symbol,
        buy_pause_amp_trigger_ratio=effective_args.buy_pause_amp_trigger_ratio,
        buy_pause_down_return_trigger_ratio=effective_args.buy_pause_down_return_trigger_ratio,
        short_cover_pause_amp_trigger_ratio=getattr(effective_args, "short_cover_pause_amp_trigger_ratio", None),
        short_cover_pause_down_return_trigger_ratio=getattr(effective_args, "short_cover_pause_down_return_trigger_ratio", None),
        freeze_shift_abs_return_trigger_ratio=effective_args.freeze_shift_abs_return_trigger_ratio,
    )
    center_price = float(state["center_price"])
    shift_moves: list[dict[str, Any]] = []
    center_source: dict[str, Any] | None = None
    neutral_hourly_scale: dict[str, Any] | None = None
    fixed_center_roll: dict[str, Any] | None = None
    custom_grid_roll: dict[str, Any] | None = None
    fixed_center_enabled = bool(getattr(args, "fixed_center_enabled", False))
    if _is_custom_grid_mode(args):
        custom_min_price = _safe_float(
            state.get("custom_grid_runtime_min_price", getattr(args, "custom_grid_min_price", None))
        )
        custom_max_price = _safe_float(
            state.get("custom_grid_runtime_max_price", getattr(args, "custom_grid_max_price", None))
        )
        custom_direction = str(getattr(args, "custom_grid_direction", strategy_mode) or strategy_mode).strip().lower()
        custom_min_price, custom_max_price, custom_grid_roll = _resolve_custom_grid_roll(
            state=state,
            summary_path=summary_path,
            now=_utc_now(),
            current_price=mid_price,
            min_price=custom_min_price,
            max_price=custom_max_price,
            n=max(int(getattr(args, "custom_grid_n", 0) or 0), 1),
            grid_level_mode=str(getattr(args, "custom_grid_level_mode", "arithmetic") or "arithmetic"),
            direction=custom_direction,
            enabled=bool(getattr(args, "custom_grid_roll_enabled", False)),
            interval_minutes=int(getattr(args, "custom_grid_roll_interval_minutes", 5)),
            trade_threshold=int(getattr(args, "custom_grid_roll_trade_threshold", 100)),
            upper_distance_ratio=float(getattr(args, "custom_grid_roll_upper_distance_ratio", 0.30)),
            shift_levels=int(getattr(args, "custom_grid_roll_shift_levels", 1)),
        )
        center_price = (custom_min_price + custom_max_price) / 2.0 if custom_min_price > 0 and custom_max_price > custom_min_price else center_price
        center_source = {
            "available": True,
            "center_price": center_price,
            "interval": "static_ladder",
            "reason": f"custom_grid_{custom_direction}",
        }
        fixed_center_roll = {
            "enabled": False,
            "trigger_steps": 0.0,
            "confirm_cycles": 0,
            "shift_steps": 0,
            "drift_steps": 0.0,
            "pending_direction": "",
            "pending_count": 0,
            "shifted": False,
        }
    elif fixed_center_enabled:
        center_source = {
            "available": True,
            "center_price": center_price,
            "interval": "fixed",
            "reason": "fixed_center_enabled",
        }
        center_price, fixed_center_shift_moves, fixed_center_roll = _resolve_fixed_center_roll(
            state=state,
            center_price=center_price,
            mid_price=mid_price,
            step_price=effective_args.step_price,
            tick_size=symbol_info.get("tick_size"),
            enabled=bool(getattr(args, "fixed_center_roll_enabled", False)),
            trigger_steps=float(getattr(args, "fixed_center_roll_trigger_steps", 1.0)),
            confirm_cycles=int(getattr(args, "fixed_center_roll_confirm_cycles", 3)),
            shift_steps=int(getattr(args, "fixed_center_roll_shift_steps", 1)),
        )
        if fixed_center_shift_moves:
            shift_moves.extend(fixed_center_shift_moves)
            center_source = {
                "available": True,
                "center_price": center_price,
                "interval": "fixed",
                "reason": "fixed_center_roll",
            }
    elif _is_inventory_target_neutral_mode(strategy_mode):
        center_source = determine_interval_center_price(
            symbol=symbol,
            interval_minutes=args.neutral_center_interval_minutes,
            tick_size=symbol_info.get("tick_size"),
        )
        if center_source.get("available"):
            center_price = float(center_source["center_price"])
        neutral_hourly_scale = resolve_neutral_hourly_scale(
            state=state,
            symbol=symbol,
            enabled=bool(getattr(args, "neutral_hourly_scale_enabled", False)),
            stable_scale=float(getattr(args, "neutral_hourly_scale_stable", 1.0)),
            transition_scale=float(getattr(args, "neutral_hourly_scale_transition", 0.85)),
            defensive_scale=float(getattr(args, "neutral_hourly_scale_defensive", 0.65)),
            stable_15m_max_amplitude_ratio=args.auto_regime_stable_15m_max_amplitude_ratio,
            stable_60m_max_amplitude_ratio=args.auto_regime_stable_60m_max_amplitude_ratio,
            stable_60m_return_floor_ratio=args.auto_regime_stable_60m_return_floor_ratio,
            defensive_15m_amplitude_ratio=args.auto_regime_defensive_15m_amplitude_ratio,
            defensive_60m_amplitude_ratio=args.auto_regime_defensive_60m_amplitude_ratio,
            defensive_15m_return_ratio=args.auto_regime_defensive_15m_return_ratio,
            defensive_60m_return_ratio=args.auto_regime_defensive_60m_return_ratio,
        )
    elif not market_guard["shift_frozen"]:
        center_price, shift_moves = shift_center_price(
            center_price=center_price,
            mid_price=mid_price,
            step_price=effective_args.step_price,
            tick_size=symbol_info.get("tick_size"),
            down_trigger_steps=effective_args.down_trigger_steps,
            up_trigger_steps=effective_args.up_trigger_steps,
            shift_steps=effective_args.shift_steps,
        )

    credentials = load_binance_api_credentials()
    if credentials is None:
        raise RuntimeError("请先在本机环境变量中配置 BINANCE_API_KEY 和 BINANCE_API_SECRET")
    api_key, api_secret = credentials

    account_info = fetch_futures_account_info_v3(api_key, api_secret, recv_window=args.recv_window)
    position_mode = fetch_futures_position_mode(api_key, api_secret, recv_window=args.recv_window)
    open_orders = fetch_futures_open_orders(symbol, api_key, api_secret, recv_window=args.recv_window)
    dual_side_position = _truthy(position_mode.get("dualSidePosition"))
    actual_position = extract_symbol_position(account_info, symbol)
    actual_net_qty = _safe_float(actual_position.get("positionAmt"))
    actual_entry_price = _safe_float(actual_position.get("entryPrice"))
    long_position = extract_symbol_position(account_info, symbol, "LONG" if strategy_mode == "hedge_neutral" else None)
    short_position = extract_symbol_position(account_info, symbol, "SHORT") if strategy_mode == "hedge_neutral" else {}
    if _is_inventory_target_neutral_mode(strategy_mode):
        current_long_qty = max(actual_net_qty, 0.0)
        current_short_qty = max(-actual_net_qty, 0.0)
    elif _is_one_way_short_mode(strategy_mode):
        current_long_qty = 0.0
        current_short_qty = max(-actual_net_qty, 0.0)
    else:
        current_long_qty = max(_position_qty(long_position, position_side="LONG" if strategy_mode == "hedge_neutral" else None), 0.0)
        current_short_qty = max(_position_qty(short_position, position_side="SHORT"), 0.0) if strategy_mode == "hedge_neutral" else 0.0
    current_long_notional = current_long_qty * max(mid_price, 0.0)
    current_short_notional = current_short_qty * max(mid_price, 0.0)
    synthetic_ledger_snapshot: dict[str, Any] | None = None
    excess_inventory_gate = {
        "enabled": bool(getattr(args, "excess_inventory_reduce_only_enabled", False)),
        "active": False,
        "direction": None,
        "excess_qty": 0.0,
        "reason": None,
    }
    xaut_reduce_only_active = bool((xaut_adaptive or {}).get("active_state") == XAUT_ADAPTIVE_STATE_REDUCE_ONLY)
    short_cover_controls = {
        "short_cover_paused": False,
        "short_cover_pause_reasons": [],
    }

    if _is_custom_grid_mode(args):
        excess_inventory_gate["enabled"] = False
        custom_direction = str(getattr(args, "custom_grid_direction", strategy_mode) or strategy_mode).strip().lower()
        if custom_direction not in {"long", "short", "neutral"}:
            raise RuntimeError(f"Unsupported custom_grid_direction: {custom_direction}")
        if custom_direction == "neutral":
            if dual_side_position:
                raise RuntimeError("单向自定义中性网格要求账户处于单向持仓模式")
            synthetic_ledger_snapshot = sync_synthetic_ledger(
                state=state,
                symbol=symbol,
                api_key=api_key,
                api_secret=api_secret,
                recv_window=args.recv_window,
                actual_position_qty=actual_net_qty,
                entry_price=actual_entry_price,
            )
            current_long_qty = max(_safe_float(synthetic_ledger_snapshot.get("virtual_long_qty")), 0.0)
            current_short_qty = max(_safe_float(synthetic_ledger_snapshot.get("virtual_short_qty")), 0.0)
            current_long_notional = current_long_qty * max(mid_price, 0.0)
            current_short_notional = current_short_qty * max(mid_price, 0.0)
            strategy_mode = "synthetic_neutral"
        elif custom_direction == "short":
            strategy_mode = "one_way_short"
        else:
            strategy_mode = "one_way_long"

        inventory_tier = {
            "enabled": False,
            "active": False,
            "ratio": 0.0,
            "start_notional": None,
            "end_notional": None,
            "effective_buy_levels": 0,
            "effective_sell_levels": 0,
            "effective_per_order_notional": 0.0,
            "effective_base_position_notional": 0.0,
        }
        plan = build_static_binance_grid_plan(
            strategy_direction=custom_direction,
            grid_level_mode=str(getattr(args, "custom_grid_level_mode", "arithmetic") or "arithmetic"),
            min_price=custom_min_price,
            max_price=custom_max_price,
            n=max(int(getattr(args, "custom_grid_n", 0) or 0), 1),
            total_grid_notional=_safe_float(getattr(args, "custom_grid_total_notional", None)),
            neutral_anchor_price=(
                _safe_float(getattr(args, "custom_grid_neutral_anchor_price", None))
                if custom_direction == "neutral"
                else None
            ),
            bid_price=bid_price,
            ask_price=ask_price,
            tick_size=symbol_info.get("tick_size"),
            step_size=symbol_info.get("step_size"),
            min_qty=symbol_info.get("min_qty"),
            min_notional=symbol_info.get("min_notional"),
            current_long_qty=current_long_qty,
            current_short_qty=current_short_qty,
            bootstrap_positions=False,
        )
        inventory_tier["effective_buy_levels"] = int(plan.get("active_buy_order_count", len(plan.get("buy_orders", []))) or 0)
        inventory_tier["effective_sell_levels"] = int(plan.get("active_sell_order_count", len(plan.get("sell_orders", []))) or 0)
        if custom_direction == "neutral":
            controls = apply_hedge_position_controls(
                plan=plan,
                current_long_qty=current_long_qty,
                current_short_qty=current_short_qty,
                mid_price=mid_price,
                pause_long_position_notional=effective_args.pause_buy_position_notional,
                pause_short_position_notional=effective_args.pause_short_position_notional,
                min_mid_price_for_buys=effective_args.min_mid_price_for_buys,
                external_long_pause=bool(market_guard["buy_pause_active"]),
                external_pause_reasons=list(market_guard["buy_pause_reasons"]),
            )
            cap_controls = apply_hedge_position_notional_caps(
                plan=plan,
                current_long_notional=controls["current_long_notional"],
                current_short_notional=controls["current_short_notional"],
                max_long_position_notional=effective_args.max_position_notional,
                max_short_position_notional=effective_args.max_short_position_notional,
                step_size=symbol_info.get("step_size"),
                min_qty=symbol_info.get("min_qty"),
                min_notional=symbol_info.get("min_notional"),
            )
            target_base_qty = 0.0
            bootstrap_qty = 0.0
        elif custom_direction == "short":
            controls = apply_hedge_position_controls(
                plan=plan,
                current_long_qty=0.0,
                current_short_qty=current_short_qty,
                mid_price=mid_price,
                pause_long_position_notional=None,
                pause_short_position_notional=effective_args.pause_short_position_notional,
                min_mid_price_for_buys=None,
                external_long_pause=False,
                external_pause_reasons=[],
            )
            cap_controls = apply_hedge_position_notional_caps(
                plan=plan,
                current_long_notional=0.0,
                current_short_notional=controls["current_short_notional"],
                max_long_position_notional=None,
                max_short_position_notional=effective_args.max_short_position_notional,
                step_size=symbol_info.get("step_size"),
                min_qty=symbol_info.get("min_qty"),
                min_notional=symbol_info.get("min_notional"),
            )
            short_cover_controls = apply_short_cover_pause(
                plan=plan,
                active=bool(market_guard.get("short_cover_pause_active")),
                pause_reasons=list(market_guard.get("short_cover_pause_reasons", [])),
            )
            if short_cover_controls["short_cover_paused"]:
                controls["pause_reasons"] = list(controls.get("pause_reasons", []))
                for reason in short_cover_controls["short_cover_pause_reasons"]:
                    controls["pause_reasons"].append(f"short_cover_pause: {reason}")
            target_base_qty = 0.0
            bootstrap_qty = 0.0
        else:
            controls = apply_position_controls(
                plan=plan,
                current_long_qty=current_long_qty,
                mid_price=mid_price,
                pause_buy_position_notional=effective_args.pause_buy_position_notional,
                min_mid_price_for_buys=effective_args.min_mid_price_for_buys,
                external_buy_pause=bool(market_guard["buy_pause_active"]),
                external_pause_reasons=list(market_guard["buy_pause_reasons"]),
            )
            cap_controls = apply_max_position_notional_cap(
                plan=plan,
                current_long_notional=controls["current_long_notional"],
                max_position_notional=effective_args.max_position_notional,
                step_size=symbol_info.get("step_size"),
                min_qty=symbol_info.get("min_qty"),
                min_notional=symbol_info.get("min_notional"),
            )
            target_base_qty = 0.0
            bootstrap_qty = 0.0
    elif strategy_mode == "hedge_neutral":
        inventory_tier = {
            "enabled": False,
            "active": False,
            "ratio": 0.0,
            "start_notional": None,
            "end_notional": None,
            "effective_buy_levels": effective_args.buy_levels,
            "effective_sell_levels": effective_args.sell_levels,
            "effective_per_order_notional": effective_args.per_order_notional,
            "effective_base_position_notional": effective_args.base_position_notional,
        }
        plan = build_hedge_micro_grid_plan(
            center_price=center_price,
            step_price=effective_args.step_price,
            buy_levels=inventory_tier["effective_buy_levels"],
            sell_levels=inventory_tier["effective_sell_levels"],
            per_order_notional=inventory_tier["effective_per_order_notional"],
            base_position_notional=inventory_tier["effective_base_position_notional"],
            bid_price=bid_price,
            ask_price=ask_price,
            tick_size=symbol_info.get("tick_size"),
            step_size=symbol_info.get("step_size"),
            min_qty=symbol_info.get("min_qty"),
            min_notional=symbol_info.get("min_notional"),
            current_long_qty=current_long_qty,
            current_short_qty=current_short_qty,
        )
        controls = apply_hedge_position_controls(
            plan=plan,
            current_long_qty=current_long_qty,
            current_short_qty=current_short_qty,
            mid_price=mid_price,
            pause_long_position_notional=effective_args.pause_buy_position_notional,
            pause_short_position_notional=effective_args.pause_short_position_notional,
            min_mid_price_for_buys=effective_args.min_mid_price_for_buys,
            external_long_pause=bool(market_guard["buy_pause_active"]),
            external_pause_reasons=list(market_guard["buy_pause_reasons"]),
        )
        cap_controls = apply_hedge_position_notional_caps(
            plan=plan,
            current_long_notional=controls["current_long_notional"],
            current_short_notional=controls["current_short_notional"],
            max_long_position_notional=effective_args.max_position_notional,
            max_short_position_notional=effective_args.max_short_position_notional,
            step_size=symbol_info.get("step_size"),
            min_qty=symbol_info.get("min_qty"),
            min_notional=symbol_info.get("min_notional"),
        )
        target_base_qty = plan["target_long_base_qty"]
        bootstrap_qty = plan["bootstrap_long_qty"]
    elif _is_synthetic_neutral_mode(strategy_mode):
        if dual_side_position:
            raise RuntimeError("单向 synthetic neutral 要求账户处于单向持仓模式")
        synthetic_ledger_snapshot = sync_synthetic_ledger(
            state=state,
            symbol=symbol,
            api_key=api_key,
            api_secret=api_secret,
            recv_window=args.recv_window,
            actual_position_qty=actual_net_qty,
            entry_price=actual_entry_price,
        )
        current_long_qty = max(_safe_float(synthetic_ledger_snapshot.get("virtual_long_qty")), 0.0)
        current_short_qty = max(_safe_float(synthetic_ledger_snapshot.get("virtual_short_qty")), 0.0)
        current_long_notional = current_long_qty * max(mid_price, 0.0)
        current_short_notional = current_short_qty * max(mid_price, 0.0)
        inventory_tier = {
            "enabled": False,
            "active": False,
            "ratio": 0.0,
            "start_notional": None,
            "end_notional": None,
            "effective_buy_levels": effective_args.buy_levels,
            "effective_sell_levels": effective_args.sell_levels,
            "effective_per_order_notional": effective_args.per_order_notional,
            "effective_base_position_notional": effective_args.base_position_notional,
        }
        hedge_plan = build_hedge_micro_grid_plan(
            center_price=center_price,
            step_price=effective_args.step_price,
            buy_levels=inventory_tier["effective_buy_levels"],
            sell_levels=inventory_tier["effective_sell_levels"],
            per_order_notional=inventory_tier["effective_per_order_notional"],
            base_position_notional=inventory_tier["effective_base_position_notional"],
            bid_price=bid_price,
            ask_price=ask_price,
            tick_size=symbol_info.get("tick_size"),
            step_size=symbol_info.get("step_size"),
            min_qty=symbol_info.get("min_qty"),
            min_notional=symbol_info.get("min_notional"),
            current_long_qty=current_long_qty,
            current_short_qty=current_short_qty,
        )
        plan = _convert_plan_orders_to_one_way(hedge_plan)
        controls = apply_hedge_position_controls(
            plan=plan,
            current_long_qty=current_long_qty,
            current_short_qty=current_short_qty,
            mid_price=mid_price,
            pause_long_position_notional=effective_args.pause_buy_position_notional,
            pause_short_position_notional=effective_args.pause_short_position_notional,
            min_mid_price_for_buys=effective_args.min_mid_price_for_buys,
            external_long_pause=bool(market_guard["buy_pause_active"]),
            external_pause_reasons=list(market_guard["buy_pause_reasons"]),
        )
        cap_controls = apply_hedge_position_notional_caps(
            plan=plan,
            current_long_notional=controls["current_long_notional"],
            current_short_notional=controls["current_short_notional"],
            max_long_position_notional=effective_args.max_position_notional,
            max_short_position_notional=effective_args.max_short_position_notional,
            step_size=symbol_info.get("step_size"),
            min_qty=symbol_info.get("min_qty"),
            min_notional=symbol_info.get("min_notional"),
        )
        target_base_qty = hedge_plan["target_long_base_qty"]
        bootstrap_qty = hedge_plan["bootstrap_long_qty"]
    elif _is_inventory_target_neutral_mode(strategy_mode):
        if dual_side_position:
            raise RuntimeError("单向目标仓位中性策略要求账户处于单向持仓模式")
        scale_ratio = float((neutral_hourly_scale or {}).get("scale", 1.0) or 1.0)
        target_band_ratios = [
            _clamp_ratio(args.neutral_band1_target_ratio * scale_ratio),
            _clamp_ratio(args.neutral_band2_target_ratio * scale_ratio),
            _clamp_ratio(args.neutral_band3_target_ratio * scale_ratio),
        ]
        inventory_tier = {
            "enabled": False,
            "active": False,
            "ratio": 0.0,
            "start_notional": None,
            "end_notional": None,
            "effective_buy_levels": len([args.neutral_band1_offset_ratio, args.neutral_band2_offset_ratio, args.neutral_band3_offset_ratio]),
            "effective_sell_levels": len([args.neutral_band1_offset_ratio, args.neutral_band2_offset_ratio, args.neutral_band3_offset_ratio]),
            "effective_per_order_notional": 0.0,
            "effective_base_position_notional": 0.0,
        }
        plan = build_inventory_target_neutral_plan(
            center_price=center_price,
            mid_price=mid_price,
            band_offsets=[
                args.neutral_band1_offset_ratio,
                args.neutral_band2_offset_ratio,
                args.neutral_band3_offset_ratio,
            ],
            band_target_ratios=target_band_ratios,
            max_long_notional=effective_args.max_position_notional,
            max_short_notional=effective_args.max_short_position_notional or effective_args.max_position_notional,
            bid_price=bid_price,
            ask_price=ask_price,
            tick_size=symbol_info.get("tick_size"),
            step_size=symbol_info.get("step_size"),
            min_qty=symbol_info.get("min_qty"),
            min_notional=symbol_info.get("min_notional"),
            current_net_qty=actual_net_qty,
        )
        controls = apply_hedge_position_controls(
            plan=plan,
            current_long_qty=current_long_qty,
            current_short_qty=current_short_qty,
            mid_price=mid_price,
            pause_long_position_notional=effective_args.pause_buy_position_notional,
            pause_short_position_notional=effective_args.pause_short_position_notional,
            min_mid_price_for_buys=effective_args.min_mid_price_for_buys,
            external_long_pause=bool(market_guard["buy_pause_active"]),
            external_pause_reasons=list(market_guard["buy_pause_reasons"]),
        )
        cap_controls = apply_hedge_position_notional_caps(
            plan=plan,
            current_long_notional=controls["current_long_notional"],
            current_short_notional=controls["current_short_notional"],
            max_long_position_notional=effective_args.max_position_notional,
            max_short_position_notional=effective_args.max_short_position_notional,
            step_size=symbol_info.get("step_size"),
            min_qty=symbol_info.get("min_qty"),
            min_notional=symbol_info.get("min_notional"),
        )
        target_base_qty = 0.0
        bootstrap_qty = 0.0
    elif _is_one_way_short_mode(strategy_mode):
        if _is_best_quote_short_profile(effective_strategy_profile):
            plan = build_best_quote_short_flip_plan(
                bid_price=bid_price,
                ask_price=ask_price,
                per_order_notional=effective_args.per_order_notional,
                max_short_position_notional=effective_args.max_short_position_notional,
                max_entry_orders=effective_args.sell_levels,
                current_short_qty=current_short_qty,
                current_short_notional=current_short_notional,
                tick_size=symbol_info.get("tick_size"),
                step_size=symbol_info.get("step_size"),
                min_qty=symbol_info.get("min_qty"),
                min_notional=symbol_info.get("min_notional"),
            )
            inventory_tier = {
                "enabled": False,
                "active": False,
                "ratio": 0.0,
                "start_notional": None,
                "end_notional": None,
                "effective_buy_levels": int(plan.get("active_buy_order_count", len(plan.get("buy_orders", []))) or 0),
                "effective_sell_levels": int(plan.get("active_sell_order_count", len(plan.get("sell_orders", []))) or 0),
                "effective_per_order_notional": effective_args.per_order_notional,
                "effective_base_position_notional": 0.0,
            }
        else:
            inventory_tier = apply_inventory_tiering(
                current_long_notional=current_short_notional,
                buy_levels=effective_args.buy_levels,
                sell_levels=effective_args.sell_levels,
                per_order_notional=effective_args.per_order_notional,
                base_position_notional=effective_args.base_position_notional,
                tier_start_notional=effective_args.inventory_tier_start_notional,
                tier_end_notional=effective_args.inventory_tier_end_notional,
                tier_buy_levels=effective_args.inventory_tier_buy_levels,
                tier_sell_levels=effective_args.inventory_tier_sell_levels,
                tier_per_order_notional=effective_args.inventory_tier_per_order_notional,
                tier_base_position_notional=effective_args.inventory_tier_base_position_notional,
            )
            plan = build_short_micro_grid_plan(
                center_price=center_price,
                step_price=effective_args.step_price,
                buy_levels=inventory_tier["effective_buy_levels"],
                sell_levels=inventory_tier["effective_sell_levels"],
                per_order_notional=inventory_tier["effective_per_order_notional"],
                base_position_notional=inventory_tier["effective_base_position_notional"],
                bid_price=bid_price,
                ask_price=ask_price,
                tick_size=symbol_info.get("tick_size"),
                step_size=symbol_info.get("step_size"),
                min_qty=symbol_info.get("min_qty"),
                min_notional=symbol_info.get("min_notional"),
                current_short_qty=current_short_qty,
            )
        excess_inventory_gate = apply_excess_inventory_reduce_only(
            plan=plan,
            strategy_mode=strategy_mode,
            current_long_qty=0.0,
            current_short_qty=current_short_qty,
            target_base_qty=plan["target_base_qty"],
            enabled=bool(getattr(effective_args, "excess_inventory_reduce_only_enabled", False)),
        )
        if xaut_reduce_only_active:
            apply_xaut_reduce_only_pruning(plan=plan, strategy_mode=strategy_mode)
        controls = apply_hedge_position_controls(
            plan=plan,
            current_long_qty=0.0,
            current_short_qty=current_short_qty,
            mid_price=mid_price,
            pause_long_position_notional=None,
            pause_short_position_notional=effective_args.pause_short_position_notional,
            min_mid_price_for_buys=None,
            external_long_pause=False,
            external_pause_reasons=[],
        )
        cap_controls = apply_hedge_position_notional_caps(
            plan=plan,
            current_long_notional=0.0,
            current_short_notional=controls["current_short_notional"],
            max_long_position_notional=None,
            max_short_position_notional=effective_args.max_short_position_notional,
            step_size=symbol_info.get("step_size"),
            min_qty=symbol_info.get("min_qty"),
            min_notional=symbol_info.get("min_notional"),
        )
        short_cover_controls = apply_short_cover_pause(
            plan=plan,
            active=bool(market_guard.get("short_cover_pause_active")),
            pause_reasons=list(market_guard.get("short_cover_pause_reasons", [])),
        )
        if short_cover_controls["short_cover_paused"]:
            controls["pause_reasons"] = list(controls.get("pause_reasons", []))
            for reason in short_cover_controls["short_cover_pause_reasons"]:
                controls["pause_reasons"].append(f"short_cover_pause: {reason}")
        target_base_qty = 0.0
        bootstrap_qty = 0.0
    else:
        inventory_tier = apply_inventory_tiering(
            current_long_notional=current_long_notional,
            buy_levels=effective_args.buy_levels,
            sell_levels=effective_args.sell_levels,
            per_order_notional=effective_args.per_order_notional,
            base_position_notional=effective_args.base_position_notional,
            tier_start_notional=effective_args.inventory_tier_start_notional,
            tier_end_notional=effective_args.inventory_tier_end_notional,
            tier_buy_levels=effective_args.inventory_tier_buy_levels,
            tier_sell_levels=effective_args.inventory_tier_sell_levels,
            tier_per_order_notional=effective_args.inventory_tier_per_order_notional,
            tier_base_position_notional=effective_args.inventory_tier_base_position_notional,
        )
        plan = build_micro_grid_plan(
            center_price=center_price,
            step_price=effective_args.step_price,
            buy_levels=inventory_tier["effective_buy_levels"],
            sell_levels=inventory_tier["effective_sell_levels"],
            per_order_notional=inventory_tier["effective_per_order_notional"],
            base_position_notional=inventory_tier["effective_base_position_notional"],
            bid_price=bid_price,
            ask_price=ask_price,
            tick_size=symbol_info.get("tick_size"),
            step_size=symbol_info.get("step_size"),
            min_qty=symbol_info.get("min_qty"),
            min_notional=symbol_info.get("min_notional"),
            current_long_qty=current_long_qty,
        )
        excess_inventory_gate = apply_excess_inventory_reduce_only(
            plan=plan,
            strategy_mode=strategy_mode,
            current_long_qty=current_long_qty,
            current_short_qty=0.0,
            target_base_qty=plan["target_base_qty"],
            enabled=bool(getattr(effective_args, "excess_inventory_reduce_only_enabled", False)),
        )
        if xaut_reduce_only_active:
            apply_xaut_reduce_only_pruning(plan=plan, strategy_mode=strategy_mode)
        controls = apply_position_controls(
            plan=plan,
            current_long_qty=current_long_qty,
            mid_price=mid_price,
            pause_buy_position_notional=effective_args.pause_buy_position_notional,
            min_mid_price_for_buys=effective_args.min_mid_price_for_buys,
            external_buy_pause=bool(market_guard["buy_pause_active"]),
            external_pause_reasons=list(market_guard["buy_pause_reasons"]),
        )
        if excess_inventory_gate["active"]:
            controls["buy_paused"] = True
            controls["pause_reasons"] = list(controls.get("pause_reasons", []))
            if excess_inventory_gate["reason"]:
                controls["pause_reasons"].append(
                    f"excess_inventory_reduce_only: {excess_inventory_gate['reason']}"
                )
        cap_controls = apply_max_position_notional_cap(
            plan=plan,
            current_long_notional=controls["current_long_notional"],
            max_position_notional=effective_args.max_position_notional,
            step_size=symbol_info.get("step_size"),
            min_qty=symbol_info.get("min_qty"),
            min_notional=symbol_info.get("min_notional"),
        )
        target_base_qty = plan["target_base_qty"]
        bootstrap_qty = plan["bootstrap_qty"]
    desired_orders = [
        *plan["buy_orders"],
        *plan["sell_orders"],
    ]
    diff = diff_open_orders(existing_orders=open_orders, desired_orders=desired_orders)

    state["center_price"] = center_price
    state["last_mid_price"] = mid_price
    state["updated_at"] = _isoformat(_utc_now())
    state["version"] = STATE_VERSION
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")

    report = {
        "generated_at": _isoformat(_utc_now()),
        "symbol": symbol,
        "strategy_mode": strategy_mode,
        "requested_strategy_profile": requested_strategy_profile,
        "effective_strategy_profile": effective_strategy_profile,
        "effective_strategy_label": AUTO_REGIME_PROFILE_LABELS.get(effective_strategy_profile),
        "bid_price": bid_price,
        "ask_price": ask_price,
        "mid_price": mid_price,
        "funding_rate": _safe_float(premium.get("funding_rate")) if premium.get("funding_rate") is not None else None,
        "center_price": center_price,
        "step_price": effective_args.step_price,
        "center_source": center_source,
        "custom_grid_roll": custom_grid_roll,
        "custom_grid_runtime_min_price": custom_min_price if _is_custom_grid_mode(args) else None,
        "custom_grid_runtime_max_price": custom_max_price if _is_custom_grid_mode(args) else None,
        "fixed_center_roll": fixed_center_roll,
        "excess_inventory_gate": excess_inventory_gate,
        "neutral_hourly_scale": neutral_hourly_scale,
        "shift_moves": shift_moves,
        "market_guard": market_guard,
        "auto_regime": auto_regime,
        "xaut_adaptive": xaut_adaptive,
        "current_long_qty": current_long_qty,
        "current_long_notional": controls["current_long_notional"],
        "current_short_qty": current_short_qty,
        "current_short_notional": controls.get("current_short_notional", current_short_notional),
        "actual_net_qty": actual_net_qty,
        "actual_net_notional": actual_net_qty * max(mid_price, 0.0),
        "synthetic_ledger": synthetic_ledger_snapshot,
        "synthetic_net_qty": current_long_qty - current_short_qty if _is_synthetic_neutral_mode(strategy_mode) else None,
        "synthetic_drift_qty": (
            actual_net_qty - (current_long_qty - current_short_qty)
            if _is_synthetic_neutral_mode(strategy_mode)
            else None
        ),
        "inventory_tier": inventory_tier,
        "effective_buy_levels": inventory_tier["effective_buy_levels"],
        "effective_sell_levels": inventory_tier["effective_sell_levels"],
        "effective_per_order_notional": inventory_tier["effective_per_order_notional"],
        "effective_base_position_notional": inventory_tier["effective_base_position_notional"],
        "inventory_target_bands": (
            {
                "offsets": [
                    args.neutral_band1_offset_ratio,
                    args.neutral_band2_offset_ratio,
                    args.neutral_band3_offset_ratio,
                ],
                "base_target_ratios": [
                    args.neutral_band1_target_ratio,
                    args.neutral_band2_target_ratio,
                    args.neutral_band3_target_ratio,
                ],
                "target_ratios": list(plan.get("band_target_ratios") or []),
                "current_target_notional": plan.get("current_target_notional"),
                "current_net_notional": plan.get("current_net_notional"),
            }
            if _is_inventory_target_neutral_mode(strategy_mode)
            else None
        ),
        "target_base_qty": target_base_qty,
        "bootstrap_qty": bootstrap_qty,
        "target_long_base_qty": plan.get("target_long_base_qty", target_base_qty),
        "target_short_base_qty": plan.get("target_short_base_qty", 0.0),
        "bootstrap_long_qty": plan.get("bootstrap_long_qty", bootstrap_qty),
        "bootstrap_short_qty": plan.get("bootstrap_short_qty", 0.0),
        "bootstrap_orders": plan["bootstrap_orders"],
        "buy_orders": plan["buy_orders"],
        "sell_orders": plan["sell_orders"],
        "open_order_count": len(open_orders),
        "kept_orders": diff["kept_orders"],
        "missing_orders": diff["missing_orders"],
        "stale_orders": diff["stale_orders"],
        "state_path": str(state_path),
        "symbol_info": symbol_info,
        "account_connected": True,
        "dual_side_position": dual_side_position,
        "buy_paused": controls["buy_paused"],
        "pause_reasons": controls["pause_reasons"],
        "short_paused": bool(controls.get("short_paused")),
        "short_pause_reasons": list(controls.get("short_pause_reasons", [])),
        "short_cover_paused": bool(short_cover_controls.get("short_cover_paused")),
        "short_cover_pause_reasons": list(short_cover_controls.get("short_cover_pause_reasons", [])),
        "buy_cap_applied": cap_controls["cap_applied"],
        "buy_budget_notional": cap_controls["buy_budget_notional"],
        "planned_buy_notional": cap_controls["planned_buy_notional"],
        "max_position_notional": cap_controls["max_position_notional"],
        "short_cap_applied": bool(cap_controls.get("short_cap_applied")),
        "short_budget_notional": cap_controls.get("short_budget_notional"),
        "planned_short_notional": cap_controls.get("planned_short_notional"),
        "max_short_position_notional": cap_controls.get("max_short_position_notional"),
    }
    return report


def execute_plan_report(args: argparse.Namespace, plan_report: dict[str, Any]) -> dict[str, Any]:
    strategy_mode = str(plan_report.get("strategy_mode") or getattr(args, "strategy_mode", "one_way_long")).strip() or "one_way_long"
    validation = validate_plan_report(
        plan_report=plan_report,
        allow_symbol=args.symbol,
        max_new_orders=args.max_new_orders,
        max_total_notional=args.max_total_notional,
        cancel_stale=args.cancel_stale,
        max_plan_age_seconds=args.max_plan_age_seconds,
        now=datetime.now(timezone.utc),
        allow_dual_side_position=(strategy_mode == "hedge_neutral"),
    )

    symbol = str(plan_report.get("symbol", "")).upper().strip()
    step_price = _safe_float(plan_report.get("step_price"))
    live_book_rows = fetch_futures_book_tickers(symbol=symbol)
    if not live_book_rows:
        raise RuntimeError(f"no book ticker returned for {symbol}")
    live_book = live_book_rows[0]
    live_bid_price = _safe_float(live_book.get("bid_price"))
    live_ask_price = _safe_float(live_book.get("ask_price"))
    drift_steps = estimate_mid_drift_steps(
        report_mid_price=_safe_float(plan_report.get("mid_price")),
        live_bid_price=live_bid_price,
        live_ask_price=live_ask_price,
        step_price=step_price,
    )
    if drift_steps > args.max_mid_drift_steps:
        validation["errors"].append(
            f"live mid drift is {drift_steps:.2f} steps, above max_mid_drift_steps={args.max_mid_drift_steps:.2f}"
        )
        validation["ok"] = False

    report = {
        "plan_json": str(args.plan_json),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "apply_requested": bool(args.apply),
        "validation": validation,
        "live_book": {
            "bid_price": live_bid_price,
            "ask_price": live_ask_price,
        },
        "executed": False,
        "canceled_orders": [],
        "placed_orders": [],
        "skipped_orders": [],
        "configuration": {
            "allow_symbol": args.symbol.upper().strip(),
            "strategy_mode": strategy_mode,
            "margin_type": str(args.margin_type).upper().strip(),
            "leverage": args.leverage,
            "cancel_stale": bool(args.cancel_stale),
            "maker_retries": args.maker_retries,
        },
        "plan_snapshot": {
            "current_long_qty": _safe_float(plan_report.get("current_long_qty")),
            "current_long_notional": _safe_float(plan_report.get("current_long_notional")),
            "current_short_qty": _safe_float(plan_report.get("current_short_qty")),
            "current_short_notional": _safe_float(plan_report.get("current_short_notional")),
            "actual_net_qty": _safe_float(plan_report.get("actual_net_qty")),
            "actual_net_notional": _safe_float(plan_report.get("actual_net_notional")),
            "synthetic_net_qty": _safe_float(plan_report.get("synthetic_net_qty")),
            "synthetic_drift_qty": _safe_float(plan_report.get("synthetic_drift_qty")),
            "open_order_count": int(plan_report.get("open_order_count", 0) or 0),
            "buy_paused": bool(plan_report.get("buy_paused")),
            "pause_reasons": list(plan_report.get("pause_reasons", [])),
            "buy_cap_applied": bool(plan_report.get("buy_cap_applied")),
            "buy_budget_notional": _safe_float(plan_report.get("buy_budget_notional")),
            "planned_buy_notional": _safe_float(plan_report.get("planned_buy_notional")),
            "max_position_notional": _safe_float(plan_report.get("max_position_notional")),
            "short_paused": bool(plan_report.get("short_paused")),
            "short_pause_reasons": list(plan_report.get("short_pause_reasons", [])),
            "short_cap_applied": bool(plan_report.get("short_cap_applied")),
            "short_budget_notional": _safe_float(plan_report.get("short_budget_notional")),
            "planned_short_notional": _safe_float(plan_report.get("planned_short_notional")),
            "max_short_position_notional": _safe_float(plan_report.get("max_short_position_notional")),
            "inventory_tier": dict(plan_report.get("inventory_tier") or {}),
            "synthetic_ledger": dict(plan_report.get("synthetic_ledger") or {}),
        },
        "error": None,
    }

    if validation["actions"]["place_count"] <= 0 and validation["actions"]["cancel_count"] <= 0:
        validation["errors"] = [
            item for item in validation["errors"] if "plan contains no actions to execute" not in item
        ]
        validation["ok"] = not validation["errors"]
        report["idle"] = True
        return report

    if not args.apply:
        return report

    if not validation["ok"]:
        raise RuntimeError("Refusing to place orders because validation failed")

    credentials = load_binance_api_credentials()
    if credentials is None:
        raise RuntimeError("请先在本机环境变量中配置 BINANCE_API_KEY 和 BINANCE_API_SECRET")
    api_key, api_secret = credentials

    position_mode = fetch_futures_position_mode(api_key, api_secret, recv_window=args.recv_window)
    dual_side_position = _truthy(position_mode.get("dualSidePosition"))
    if strategy_mode == "hedge_neutral":
        if not dual_side_position:
            raise RuntimeError("中性 Hedge 策略要求账户处于双向持仓模式")
    elif _is_synthetic_neutral_mode(strategy_mode):
        if dual_side_position:
            raise RuntimeError("单向 synthetic neutral 策略要求账户处于单向持仓模式")
    elif _is_inventory_target_neutral_mode(strategy_mode):
        if dual_side_position:
            raise RuntimeError("单向目标仓位中性策略要求账户处于单向持仓模式")
    elif _is_one_way_short_mode(strategy_mode):
        if dual_side_position:
            raise RuntimeError("单向固定中心做空策略要求账户处于单向持仓模式")
    elif dual_side_position:
        raise RuntimeError("账户当前是双向持仓模式，当前提交器只支持单向模式")

    account_info = fetch_futures_account_info_v3(api_key, api_secret, recv_window=args.recv_window)
    multi_assets_margin = _truthy(account_info.get("multiAssetsMargin"))
    requested_margin_type = str(args.margin_type).upper().strip()
    report["account_mode"] = {
        "multi_assets_margin": multi_assets_margin,
        "requested_margin_type": requested_margin_type,
    }
    if multi_assets_margin and requested_margin_type == "ISOLATED":
        raise RuntimeError(
            "账户当前启用了 Multi-Assets Mode，Binance 不允许在该模式下切到 ISOLATED。"
        )

    current_open_orders = fetch_futures_open_orders(symbol, api_key, api_secret, recv_window=args.recv_window)
    expected_open_order_count = int(plan_report.get("open_order_count", 0) or 0)
    expected_long_qty = _safe_float(plan_report.get("current_long_qty"))
    expected_short_qty = _safe_float(plan_report.get("current_short_qty"))
    expected_actual_net_qty = _safe_float(plan_report.get("actual_net_qty"))
    if strategy_mode == "hedge_neutral":
        current_long_position = extract_symbol_position(account_info, symbol, "LONG")
        current_short_position = extract_symbol_position(account_info, symbol, "SHORT")
        current_long_qty = max(_position_qty(current_long_position, position_side="LONG"), 0.0)
        current_short_qty = max(_position_qty(current_short_position, position_side="SHORT"), 0.0)
        current_actual_net_qty = current_long_qty - current_short_qty
    else:
        current_position = extract_symbol_position(account_info, symbol)
        current_actual_net_qty = _safe_float(current_position.get("positionAmt"))
        if _is_synthetic_neutral_mode(strategy_mode):
            current_long_qty = expected_long_qty
            current_short_qty = expected_short_qty
        elif _is_inventory_target_neutral_mode(strategy_mode):
            current_long_qty = max(current_actual_net_qty, 0.0)
            current_short_qty = max(-current_actual_net_qty, 0.0)
        elif _is_one_way_short_mode(strategy_mode):
            current_long_qty = 0.0
            current_short_qty = max(-current_actual_net_qty, 0.0)
        else:
            current_long_qty = max(current_actual_net_qty, 0.0)
            current_short_qty = 0.0
    if len(current_open_orders) != expected_open_order_count:
        raise RuntimeError("当前未成交委托数量与计划生成时不一致，请等待下一轮刷新")
    if _is_synthetic_neutral_mode(strategy_mode):
        if abs(current_actual_net_qty - expected_actual_net_qty) > 1e-9:
            raise RuntimeError("当前净持仓与计划生成时不一致，请等待下一轮刷新")
    elif _is_one_way_short_mode(strategy_mode):
        if abs(current_short_qty - expected_short_qty) > 1e-9:
            raise RuntimeError("当前空头持仓与计划生成时不一致，请等待下一轮刷新")
    elif abs(current_long_qty - expected_long_qty) > 1e-9:
        raise RuntimeError("当前持仓与计划生成时不一致，请等待下一轮刷新")
    if strategy_mode == "hedge_neutral" and abs(current_short_qty - expected_short_qty) > 1e-9:
        raise RuntimeError("当前空头持仓与计划生成时不一致，请等待下一轮刷新")

    if requested_margin_type != "KEEP":
        try:
            report["margin_response"] = post_futures_change_margin_type(
                symbol=symbol,
                margin_type=requested_margin_type,
                api_key=api_key,
                api_secret=api_secret,
                recv_window=args.recv_window,
            )
        except RuntimeError as exc:
            if not _ignore_noop_error(exc, ("no need to change margin type",)):
                raise
            report["margin_response"] = {"ignored_error": str(exc)}
    else:
        report["margin_response"] = {"skipped": True, "reason": "KEEP"}

    try:
        report["leverage_response"] = post_futures_change_initial_leverage(
            symbol=symbol,
            leverage=args.leverage,
            api_key=api_key,
            api_secret=api_secret,
            recv_window=args.recv_window,
        )
    except RuntimeError as exc:
        if not _ignore_noop_error(exc, ("no need to change leverage", "same leverage")):
            raise
        report["leverage_response"] = {"ignored_error": str(exc)}

    if args.cancel_stale:
        for stale_order in validation["actions"]["cancel_orders"]:
            cancel_response = delete_futures_order(
                symbol=symbol,
                order_id=int(stale_order["orderId"]) if str(stale_order.get("orderId", "")).strip() else None,
                orig_client_order_id=str(stale_order.get("clientOrderId", "")).strip() or None,
                api_key=api_key,
                api_secret=api_secret,
                recv_window=args.recv_window,
            )
            report["canceled_orders"].append(cancel_response)

    symbol_info = plan_report.get("symbol_info") or {}
    tick_size = _safe_float(symbol_info.get("tick_size"))
    min_qty = symbol_info.get("min_qty")
    min_notional = symbol_info.get("min_notional")
    for index, order in enumerate(validation["actions"]["place_orders"], start=1):
        role = str(order.get("role", "entry"))
        side = str(order.get("side", "")).upper().strip()
        position_side = _order_position_side(order)
        if strategy_mode in {"hedge_neutral", "synthetic_neutral", "inventory_target_neutral"}:
            reduce_only = None
        else:
            reduce_only = True if (side == "SELL" and role == "take_profit") or (side == "BUY" and role == "take_profit_short") else None
        last_exc: RuntimeError | None = None
        for attempt in range(args.maker_retries + 1):
            current_book = fetch_futures_book_tickers(symbol=symbol)[0]
            current_bid = _safe_float(current_book.get("bid_price"))
            current_ask = _safe_float(current_book.get("ask_price"))
            prepared_order, skip_reason = prepare_post_only_order_request(
                order=order,
                side=side,
                live_bid_price=current_bid,
                live_ask_price=current_ask,
                tick_size=tick_size,
                min_qty=min_qty,
                min_notional=min_notional,
            )
            if prepared_order is None:
                report["skipped_orders"].append(
                    {
                        "request": {
                            "role": role,
                            "side": side,
                            "position_side": position_side,
                            "qty": _safe_float(order.get("qty")),
                            "desired_price": _safe_float(order.get("price")),
                            "attempt": attempt + 1,
                        },
                        "reason": skip_reason or {},
                    }
                )
                last_exc = None
                break
            try:
                place_response = post_futures_order(
                    symbol=symbol,
                    side=side,
                    quantity=_safe_float(prepared_order.get("qty")),
                    price=_safe_float(prepared_order.get("submitted_price")),
                    api_key=api_key,
                    api_secret=api_secret,
                    recv_window=args.recv_window,
                    time_in_force="GTX",
                    new_client_order_id=_build_client_order_id(symbol=symbol, role=role, index=index),
                    reduce_only=reduce_only,
                    position_side=None if position_side == "BOTH" else position_side,
                )
                report["placed_orders"].append(
                    {
                        "request": {
                            "role": role,
                            "side": side,
                            "position_side": position_side,
                            "qty": _safe_float(prepared_order.get("qty")),
                            "desired_price": _safe_float(prepared_order.get("desired_price")),
                            "submitted_price": _safe_float(prepared_order.get("submitted_price")),
                            "submitted_notional": _safe_float(prepared_order.get("submitted_notional")),
                            "attempt": attempt + 1,
                        },
                        "response": place_response,
                    }
                )
                last_exc = None
                break
            except RuntimeError as exc:
                last_exc = exc
                message = str(exc).lower()
                if "-5022" not in message and "post only order will be rejected" not in message:
                    raise
                if attempt >= args.maker_retries:
                    raise
        if last_exc is not None:
            raise last_exc

    update_synthetic_order_refs(
        state_path=Path(str(plan_report.get("state_path", args.state_path))),
        strategy_mode=strategy_mode,
        submit_report=report,
    )
    report["executed"] = True
    return report


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Continuously refresh and execute a micro-grid plan with position caps."
    )
    parser.add_argument("--symbol", type=str, default="NIGHTUSDT")
    parser.add_argument("--strategy-profile", type=str, default=AUTO_REGIME_STABLE_PROFILE)
    parser.add_argument("--strategy-mode", type=str, default="one_way_long", choices=("one_way_long", "one_way_short", "hedge_neutral", "synthetic_neutral", "inventory_target_neutral"))
    parser.add_argument("--step-price", type=float, default=0.00002)
    parser.add_argument("--buy-levels", type=int, default=8)
    parser.add_argument("--sell-levels", type=int, default=8)
    parser.add_argument("--per-order-notional", type=float, default=12.6)
    parser.add_argument("--base-position-notional", type=float, default=75.6)
    parser.add_argument("--center-price", type=float, default=None)
    parser.add_argument("--fixed-center-enabled", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--fixed-center-roll-enabled", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--excess-inventory-reduce-only-enabled", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--custom-grid-enabled", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--custom-grid-direction", type=str, default=None)
    parser.add_argument("--custom-grid-level-mode", type=str, default="arithmetic")
    parser.add_argument("--custom-grid-min-price", type=float, default=None)
    parser.add_argument("--custom-grid-max-price", type=float, default=None)
    parser.add_argument("--custom-grid-n", type=int, default=None)
    parser.add_argument("--custom-grid-total-notional", type=float, default=None)
    parser.add_argument("--custom-grid-neutral-anchor-price", type=float, default=None)
    parser.add_argument("--custom-grid-roll-enabled", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--custom-grid-roll-interval-minutes", type=int, default=5)
    parser.add_argument("--custom-grid-roll-trade-threshold", type=int, default=100)
    parser.add_argument("--custom-grid-roll-upper-distance-ratio", type=float, default=0.30)
    parser.add_argument("--custom-grid-roll-shift-levels", type=int, default=1)
    parser.add_argument("--fixed-center-roll-trigger-steps", type=float, default=1.0)
    parser.add_argument("--fixed-center-roll-confirm-cycles", type=int, default=3)
    parser.add_argument("--fixed-center-roll-shift-steps", type=int, default=1)
    parser.add_argument("--down-trigger-steps", type=int, default=4)
    parser.add_argument("--up-trigger-steps", type=int, default=8)
    parser.add_argument("--shift-steps", type=int, default=4)
    parser.add_argument("--pause-buy-position-notional", type=float, default=180.0)
    parser.add_argument("--pause-short-position-notional", type=float, default=None)
    parser.add_argument("--max-position-notional", type=float, default=None)
    parser.add_argument("--max-short-position-notional", type=float, default=None)
    parser.add_argument("--min-mid-price-for-buys", type=float, default=None)
    parser.add_argument("--buy-pause-amp-trigger-ratio", type=float, default=None)
    parser.add_argument("--buy-pause-down-return-trigger-ratio", type=float, default=None)
    parser.add_argument("--short-cover-pause-amp-trigger-ratio", type=float, default=None)
    parser.add_argument("--short-cover-pause-down-return-trigger-ratio", type=float, default=None)
    parser.add_argument("--freeze-shift-abs-return-trigger-ratio", type=float, default=None)
    parser.add_argument("--auto-regime-enabled", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--auto-regime-confirm-cycles", type=int, default=2)
    parser.add_argument("--auto-regime-stable-15m-max-amplitude-ratio", type=float, default=0.02)
    parser.add_argument("--auto-regime-stable-60m-max-amplitude-ratio", type=float, default=0.05)
    parser.add_argument("--auto-regime-stable-60m-return-floor-ratio", type=float, default=-0.01)
    parser.add_argument("--auto-regime-defensive-15m-amplitude-ratio", type=float, default=0.035)
    parser.add_argument("--auto-regime-defensive-60m-amplitude-ratio", type=float, default=0.08)
    parser.add_argument("--auto-regime-defensive-15m-return-ratio", type=float, default=-0.015)
    parser.add_argument("--auto-regime-defensive-60m-return-ratio", type=float, default=-0.03)
    parser.add_argument("--neutral-center-interval-minutes", type=int, default=3)
    parser.add_argument("--neutral-band1-offset-ratio", type=float, default=0.005)
    parser.add_argument("--neutral-band2-offset-ratio", type=float, default=0.01)
    parser.add_argument("--neutral-band3-offset-ratio", type=float, default=0.02)
    parser.add_argument("--neutral-band1-target-ratio", type=float, default=0.20)
    parser.add_argument("--neutral-band2-target-ratio", type=float, default=0.50)
    parser.add_argument("--neutral-band3-target-ratio", type=float, default=1.00)
    parser.add_argument("--neutral-hourly-scale-enabled", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--neutral-hourly-scale-stable", type=float, default=1.0)
    parser.add_argument("--neutral-hourly-scale-transition", type=float, default=0.85)
    parser.add_argument("--neutral-hourly-scale-defensive", type=float, default=0.65)
    parser.add_argument("--inventory-tier-start-notional", type=float, default=None)
    parser.add_argument("--inventory-tier-end-notional", type=float, default=None)
    parser.add_argument("--inventory-tier-buy-levels", type=int, default=None)
    parser.add_argument("--inventory-tier-sell-levels", type=int, default=None)
    parser.add_argument("--inventory-tier-per-order-notional", type=float, default=None)
    parser.add_argument("--inventory-tier-base-position-notional", type=float, default=None)
    parser.add_argument("--margin-type", type=str, default="KEEP")
    parser.add_argument("--leverage", type=int, default=2)
    parser.add_argument("--recv-window", type=int, default=5000)
    parser.add_argument("--max-plan-age-seconds", type=int, default=30)
    parser.add_argument("--max-mid-drift-steps", type=float, default=4.0)
    parser.add_argument("--max-new-orders", type=int, default=16)
    parser.add_argument("--max-total-notional", type=float, default=220.0)
    parser.add_argument("--reconcile-interval-cycles", type=int, default=5)
    parser.add_argument("--maker-retries", type=int, default=2)
    parser.add_argument("--cancel-stale", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--apply", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--sleep-seconds", type=float, default=20.0)
    parser.add_argument("--iterations", type=int, default=0, help="0 means run forever")
    parser.add_argument("--max-consecutive-errors", type=int, default=10)
    parser.add_argument("--state-path", type=str, default="output/night_small_semi_auto_state.json")
    parser.add_argument("--plan-json", type=str, default="output/night_loop_latest_plan.json")
    parser.add_argument("--submit-report-json", type=str, default="output/night_loop_latest_submit.json")
    parser.add_argument("--summary-jsonl", type=str, default="output/night_loop_events.jsonl")
    parser.add_argument("--reset-state", action="store_true")
    return parser


def _print_cycle_summary(summary: dict[str, Any]) -> None:
    print(
        f"[{summary['cycle']}] mid={_price(summary['mid_price'])} center={_price(summary['center_price'])} "
        f"mode={summary.get('strategy_mode', 'one_way_long')} "
        f"profile={summary.get('effective_strategy_profile', summary.get('strategy_profile', AUTO_REGIME_STABLE_PROFILE))} "
        f"long_qty={_float(summary['current_long_qty'])} long_notional={_float(summary['current_long_notional'])} "
        f"open={summary['open_order_count']} keep={summary['kept_order_count']} "
        f"missing={summary['missing_order_count']} stale={summary['stale_order_count']} "
        f"placed={summary['placed_count']} canceled={summary['canceled_count']} "
        f"buy_paused={'yes' if summary['buy_paused'] else 'no'} "
        f"buy_cap={'yes' if summary['buy_cap_applied'] else 'no'}"
    )
    if summary.get("error_message"):
        print(f"  error: {summary['error_message']}")
    if summary.get("pause_reasons"):
        print(f"  pause: {'; '.join(summary['pause_reasons'])}")
    if summary.get("volatility_buy_pause") or summary.get("short_cover_paused") or summary.get("shift_frozen"):
        print(
            "  market_guard: "
            f"buy_pause={'yes' if summary.get('volatility_buy_pause') else 'no'} "
            f"short_cover_pause={'yes' if summary.get('short_cover_paused') else 'no'} "
            f"shift_frozen={'yes' if summary.get('shift_frozen') else 'no'} "
            f"ret={summary.get('market_guard_return_ratio', 0.0) * 100:.2f}% "
            f"amp={summary.get('market_guard_amplitude_ratio', 0.0) * 100:.2f}%"
        )
    if summary.get("max_position_notional"):
        print(
            "  buy_budget: "
            f"{_float(summary.get('buy_budget_notional', 0.0))} / "
            f"{_float(summary.get('planned_buy_notional', 0.0))} "
            f"(max_pos={_float(summary['max_position_notional'])})"
        )
    if summary.get("current_short_notional") or summary.get("max_short_position_notional"):
        print(
            "  short_budget: "
            f"short_notional={_float(summary.get('current_short_notional', 0.0))} "
            f"budget={_float(summary.get('short_budget_notional', 0.0))} "
            f"planned={_float(summary.get('planned_short_notional', 0.0))} "
            f"(max_short={_float(summary.get('max_short_position_notional', 0.0))})"
        )
    if summary.get("strategy_mode") == "synthetic_neutral":
        print(
            "  synthetic: "
            f"actual_net={_float(summary.get('actual_net_qty', 0.0))} "
            f"virtual_net={_float(summary.get('synthetic_net_qty', 0.0))} "
            f"drift={_float(summary.get('synthetic_drift_qty', 0.0))} "
            f"unmatched_trades={int(summary.get('synthetic_unmatched_trade_count', 0) or 0)}"
        )
    if summary.get("shift_moves"):
        shift_text = ", ".join(f"{item['direction']}->{_price(item['new_center_price'])}" for item in summary["shift_moves"])
        print(f"  shifts: {shift_text}")
    fixed_center_roll = summary.get("fixed_center_roll") or {}
    if fixed_center_roll.get("enabled"):
        print(
            "  fixed_center_roll: "
            f"drift={fixed_center_roll.get('drift_steps', 0.0):+.2f} "
            f"pending={fixed_center_roll.get('pending_direction', '-') or '-'} "
            f"{int(fixed_center_roll.get('pending_count', 0) or 0)}/{int(fixed_center_roll.get('confirm_cycles', 0) or 0)} "
            f"trigger={fixed_center_roll.get('trigger_steps', 0.0):.2f} "
            f"shift={int(fixed_center_roll.get('shift_steps', 0) or 0)}"
        )
    if summary.get("reconcile_checked"):
        print(
            "  reconcile: "
            f"{'ok' if summary.get('reconcile_ok') else 'diff'} "
            f"open={int(summary.get('reconcile_actual_open_order_count', 0) or 0)}/"
            f"{int(summary.get('reconcile_expected_open_order_count', 0) or 0)} "
            f"net={summary.get('reconcile_actual_actual_net_qty', 0.0):+.8f}/"
            f"{summary.get('reconcile_expected_actual_net_qty', 0.0):+.8f} "
            f"{summary.get('reconcile_message') or ''}".rstrip()
        )
    if summary.get("auto_regime_enabled"):
        print(
            "  auto_regime: "
            f"{summary.get('auto_regime_regime', '--')} -> {summary.get('effective_strategy_profile', '--')} "
            f"(pending={int(summary.get('auto_regime_pending_count', 0) or 0)}/{int(summary.get('auto_regime_confirm_cycles', 0) or 0)})"
        )
        if summary.get("auto_regime_reason"):
            print(f"  auto_reason: {summary['auto_regime_reason']}")
    if summary.get("xaut_adaptive_enabled"):
        print(
            "  xaut_adaptive: "
            f"{summary.get('xaut_adaptive_state', '--')} "
            f"(candidate={summary.get('xaut_adaptive_candidate_state', '--')} "
            f"pending={int(summary.get('xaut_adaptive_pending_count', 0) or 0)})"
        )
        if summary.get("xaut_adaptive_reason"):
            print(f"  xaut_reason: {summary['xaut_adaptive_reason']}")
    if summary.get("neutral_hourly_scale_enabled"):
        print(
            "  neutral_hourly: "
            f"{summary.get('neutral_hourly_regime', '--')} "
            f"scale={_float(summary.get('neutral_hourly_scale_ratio', 1.0))} "
            f"bucket={summary.get('neutral_hourly_bucket', '--')}"
        )
        if summary.get("neutral_hourly_reason"):
            print(f"  neutral_reason: {summary['neutral_hourly_reason']}")
    if summary.get("inventory_tier_active"):
        print(
            "  inventory_tier: "
            f"{summary.get('inventory_tier_ratio', 0.0) * 100:.0f}% "
            f"buy_levels={int(summary.get('effective_buy_levels', 0) or 0)} "
            f"sell_levels={int(summary.get('effective_sell_levels', 0) or 0)} "
            f"per_order={_float(summary.get('effective_per_order_notional', 0.0))} "
            f"base={_float(summary.get('effective_base_position_notional', 0.0))}"
        )
    if summary.get("trade_audit_appended") or summary.get("income_audit_appended"):
        print(
            f"  audit: trades+{int(summary.get('trade_audit_appended', 0) or 0)} "
            f"income+{int(summary.get('income_audit_appended', 0) or 0)}"
        )
    if summary.get("audit_error"):
        print(f"  audit_error: {summary['audit_error']}")


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    if args.step_price <= 0:
        raise SystemExit("--step-price must be > 0")
    if args.buy_levels < 0 or args.sell_levels < 0:
        raise SystemExit("--buy-levels and --sell-levels must be >= 0")
    if args.per_order_notional <= 0 or args.base_position_notional < 0:
        raise SystemExit("--per-order-notional must be > 0 and --base-position-notional must be >= 0")
    if args.max_position_notional is not None and args.max_position_notional <= 0:
        raise SystemExit("--max-position-notional must be > 0")
    if args.pause_short_position_notional is not None and args.pause_short_position_notional <= 0:
        raise SystemExit("--pause-short-position-notional must be > 0")
    if args.max_short_position_notional is not None and args.max_short_position_notional <= 0:
        raise SystemExit("--max-short-position-notional must be > 0")
    if args.buy_pause_amp_trigger_ratio is not None and args.buy_pause_amp_trigger_ratio <= 0:
        raise SystemExit("--buy-pause-amp-trigger-ratio must be > 0")
    if args.buy_pause_down_return_trigger_ratio is not None and args.buy_pause_down_return_trigger_ratio >= 0:
        raise SystemExit("--buy-pause-down-return-trigger-ratio must be < 0")
    if args.short_cover_pause_amp_trigger_ratio is not None and args.short_cover_pause_amp_trigger_ratio <= 0:
        raise SystemExit("--short-cover-pause-amp-trigger-ratio must be > 0")
    if args.short_cover_pause_down_return_trigger_ratio is not None and args.short_cover_pause_down_return_trigger_ratio >= 0:
        raise SystemExit("--short-cover-pause-down-return-trigger-ratio must be < 0")
    if args.freeze_shift_abs_return_trigger_ratio is not None and args.freeze_shift_abs_return_trigger_ratio <= 0:
        raise SystemExit("--freeze-shift-abs-return-trigger-ratio must be > 0")
    if args.auto_regime_confirm_cycles <= 0:
        raise SystemExit("--auto-regime-confirm-cycles must be > 0")
    if args.auto_regime_stable_15m_max_amplitude_ratio <= 0 or args.auto_regime_stable_60m_max_amplitude_ratio <= 0:
        raise SystemExit("--auto-regime stable amplitude thresholds must be > 0")
    if args.auto_regime_defensive_15m_amplitude_ratio <= 0 or args.auto_regime_defensive_60m_amplitude_ratio <= 0:
        raise SystemExit("--auto-regime defensive amplitude thresholds must be > 0")
    if args.auto_regime_stable_60m_return_floor_ratio >= 0:
        raise SystemExit("--auto-regime-stable-60m-return-floor-ratio must be < 0")
    if args.auto_regime_defensive_15m_return_ratio >= 0 or args.auto_regime_defensive_60m_return_ratio >= 0:
        raise SystemExit("--auto-regime defensive return thresholds must be < 0")
    if args.neutral_center_interval_minutes not in {3, 5}:
        raise SystemExit("--neutral-center-interval-minutes must be 3 or 5")
    neutral_offsets = [
        args.neutral_band1_offset_ratio,
        args.neutral_band2_offset_ratio,
        args.neutral_band3_offset_ratio,
    ]
    neutral_ratios = [
        args.neutral_band1_target_ratio,
        args.neutral_band2_target_ratio,
        args.neutral_band3_target_ratio,
    ]
    if any(value <= 0 for value in neutral_offsets):
        raise SystemExit("--neutral-band*-offset-ratio must be > 0")
    if neutral_offsets != sorted(neutral_offsets):
        raise SystemExit("--neutral-band offsets must be ascending")
    if any(value < 0 or value > 1 for value in neutral_ratios):
        raise SystemExit("--neutral-band*-target-ratio must be between 0 and 1")
    if neutral_ratios != sorted(neutral_ratios):
        raise SystemExit("--neutral-band target ratios must be ascending")
    neutral_hourly_scales = [
        args.neutral_hourly_scale_stable,
        args.neutral_hourly_scale_transition,
        args.neutral_hourly_scale_defensive,
    ]
    if any(value <= 0 for value in neutral_hourly_scales):
        raise SystemExit("--neutral-hourly-scale-* must be > 0")
    if args.inventory_tier_start_notional is not None and args.inventory_tier_start_notional <= 0:
        raise SystemExit("--inventory-tier-start-notional must be > 0")
    if args.inventory_tier_end_notional is not None and args.inventory_tier_end_notional <= 0:
        raise SystemExit("--inventory-tier-end-notional must be > 0")
    if (
        args.inventory_tier_start_notional is not None
        and args.inventory_tier_end_notional is not None
        and args.inventory_tier_end_notional <= args.inventory_tier_start_notional
    ):
        raise SystemExit("--inventory-tier-end-notional must be > --inventory-tier-start-notional")
    if args.inventory_tier_buy_levels is not None and args.inventory_tier_buy_levels < 0:
        raise SystemExit("--inventory-tier-buy-levels must be >= 0")
    if args.inventory_tier_sell_levels is not None and args.inventory_tier_sell_levels < 0:
        raise SystemExit("--inventory-tier-sell-levels must be >= 0")
    if args.inventory_tier_per_order_notional is not None and args.inventory_tier_per_order_notional <= 0:
        raise SystemExit("--inventory-tier-per-order-notional must be > 0")
    if args.inventory_tier_base_position_notional is not None and args.inventory_tier_base_position_notional < 0:
        raise SystemExit("--inventory-tier-base-position-notional must be >= 0")
    if args.leverage <= 0:
        raise SystemExit("--leverage must be > 0")
    if args.max_plan_age_seconds <= 0:
        raise SystemExit("--max-plan-age-seconds must be > 0")
    if args.max_mid_drift_steps < 0:
        raise SystemExit("--max-mid-drift-steps must be >= 0")
    if args.max_new_orders <= 0 or args.max_total_notional <= 0:
        raise SystemExit("--max-new-orders and --max-total-notional must be > 0")
    if args.maker_retries < 0:
        raise SystemExit("--maker-retries must be >= 0")
    if args.sleep_seconds <= 0:
        raise SystemExit("--sleep-seconds must be > 0")
    if args.iterations < 0:
        raise SystemExit("--iterations must be >= 0")
    if args.max_consecutive_errors <= 0:
        raise SystemExit("--max-consecutive-errors must be > 0")
    if args.custom_grid_enabled:
        direction = str(args.custom_grid_direction or "").strip().lower()
        if direction not in {"long", "short", "neutral"}:
            raise SystemExit("--custom-grid-direction must be one of: long, short, neutral")
        if str(args.custom_grid_level_mode or "").strip().lower() not in {"arithmetic", "geometric"}:
            raise SystemExit("--custom-grid-level-mode must be arithmetic or geometric")
        if args.custom_grid_min_price is None or args.custom_grid_max_price is None:
            raise SystemExit("--custom-grid-min-price and --custom-grid-max-price are required when --custom-grid-enabled")
        if args.custom_grid_min_price <= 0 or args.custom_grid_max_price <= 0:
            raise SystemExit("--custom-grid-min-price and --custom-grid-max-price must be > 0")
        if args.custom_grid_max_price <= args.custom_grid_min_price:
            raise SystemExit("--custom-grid-max-price must be > --custom-grid-min-price")
        if args.custom_grid_n is None or args.custom_grid_n <= 0:
            raise SystemExit("--custom-grid-n must be > 0 when --custom-grid-enabled")
        if args.custom_grid_total_notional is None or args.custom_grid_total_notional <= 0:
            raise SystemExit("--custom-grid-total-notional must be > 0 when --custom-grid-enabled")
    if args.custom_grid_roll_interval_minutes <= 0:
        raise SystemExit("--custom-grid-roll-interval-minutes must be > 0")
    if args.custom_grid_roll_trade_threshold < 0:
        raise SystemExit("--custom-grid-roll-trade-threshold must be >= 0")
    if args.custom_grid_roll_upper_distance_ratio < 0 or args.custom_grid_roll_upper_distance_ratio > 1:
        raise SystemExit("--custom-grid-roll-upper-distance-ratio must be between 0 and 1")
    if args.custom_grid_roll_shift_levels <= 0:
        raise SystemExit("--custom-grid-roll-shift-levels must be > 0")

    plan_path = Path(args.plan_json)
    submit_report_path = Path(args.submit_report_json)
    summary_path = Path(args.summary_jsonl)
    audit_paths = build_audit_paths(summary_path)
    consecutive_errors = 0
    cycle = 0

    while True:
        cycle += 1
        cycle_started_at = datetime.now(timezone.utc)
        try:
            plan_report = generate_plan_report(args)
            _write_json(plan_path, plan_report)
            _append_jsonl(
                audit_paths["plan_audit"],
                {
                    "ts": cycle_started_at.isoformat(),
                    "cycle": cycle,
                    "symbol": args.symbol.upper().strip(),
                    "report": plan_report,
                },
            )

            submit_report = execute_plan_report(args, plan_report)
            _write_json(submit_report_path, submit_report)
            _append_jsonl(
                audit_paths["submit_audit"],
                {
                    "ts": cycle_started_at.isoformat(),
                    "cycle": cycle,
                    "symbol": args.symbol.upper().strip(),
                    "report": submit_report,
                },
            )
            for item in submit_report.get("canceled_orders", []):
                _append_jsonl(
                    audit_paths["order_audit"],
                    {
                        "ts": cycle_started_at.isoformat(),
                        "cycle": cycle,
                        "symbol": args.symbol.upper().strip(),
                        "event_type": "cancel",
                        "payload": item,
                    },
                )
            for item in submit_report.get("placed_orders", []):
                _append_jsonl(
                    audit_paths["order_audit"],
                    {
                        "ts": cycle_started_at.isoformat(),
                        "cycle": cycle,
                        "symbol": args.symbol.upper().strip(),
                        "event_type": "place",
                        "payload": item,
                    },
                )
            audit_sync = {
                "trade_appended": 0,
                "income_appended": 0,
                "error": None,
            }
            try:
                audit_sync = _sync_account_audit(
                    symbol=args.symbol,
                    cycle=cycle,
                    summary_path=summary_path,
                    recv_window=args.recv_window,
                )
            except Exception as audit_exc:
                audit_sync["error"] = f"{audit_exc.__class__.__name__}: {audit_exc}"
            state_path = Path(str(plan_report.get("state_path", args.state_path)))
            state = read_json(state_path) or {}
            reconcile_credentials = load_binance_api_credentials()
            if reconcile_credentials is None:
                reconcile_snapshot = {
                    "enabled": False,
                    "checked": False,
                    "cycle": cycle,
                    "message": "missing_credentials",
                }
            else:
                reconcile_api_key, reconcile_api_secret = reconcile_credentials
                reconcile_snapshot = _run_periodic_reconcile(
                    state=state,
                    cycle=cycle,
                    interval_cycles=int(getattr(args, "reconcile_interval_cycles", 5)),
                    symbol=args.symbol.upper().strip(),
                    strategy_mode=str(getattr(args, "strategy_mode", "one_way_long")),
                    api_key=reconcile_api_key,
                    api_secret=reconcile_api_secret,
                    recv_window=args.recv_window,
                    expected_open_order_count=len(plan_report.get("kept_orders", [])) + len(submit_report.get("placed_orders", [])),
                    expected_actual_net_qty=_safe_float(plan_report.get("actual_net_qty")),
                )
            state["last_reconcile"] = reconcile_snapshot
            _write_json(state_path, state)

            summary = {
                "ts": cycle_started_at.isoformat(),
                "cycle": cycle,
                "symbol": args.symbol.upper().strip(),
                "strategy_profile": str(getattr(args, "strategy_profile", AUTO_REGIME_STABLE_PROFILE)),
                "effective_strategy_profile": str(
                    plan_report.get("effective_strategy_profile") or getattr(args, "strategy_profile", AUTO_REGIME_STABLE_PROFILE)
                ),
                "effective_strategy_label": str(plan_report.get("effective_strategy_label") or ""),
                "strategy_mode": str(getattr(args, "strategy_mode", "one_way_long")),
                "mid_price": _safe_float(plan_report.get("mid_price")),
                "center_price": _safe_float(plan_report.get("center_price")),
                "current_long_qty": _safe_float(plan_report.get("current_long_qty")),
                "current_long_notional": _safe_float(plan_report.get("current_long_notional")),
                "current_short_qty": _safe_float(plan_report.get("current_short_qty")),
                "current_short_notional": _safe_float(plan_report.get("current_short_notional")),
                "actual_net_qty": _safe_float(plan_report.get("actual_net_qty")),
                "actual_net_notional": _safe_float(plan_report.get("actual_net_notional")),
                "synthetic_net_qty": _safe_float(plan_report.get("synthetic_net_qty")),
                "synthetic_drift_qty": _safe_float(plan_report.get("synthetic_drift_qty")),
                "synthetic_unmatched_trade_count": int(
                    ((plan_report.get("synthetic_ledger") or {}).get("unmatched_trade_count", 0) or 0)
                ),
                "open_order_count": int(plan_report.get("open_order_count", 0) or 0),
                "kept_order_count": len(plan_report.get("kept_orders", [])),
                "missing_order_count": len(plan_report.get("missing_orders", [])),
                "stale_order_count": len(plan_report.get("stale_orders", [])),
                "placed_count": len(submit_report.get("placed_orders", [])),
                "canceled_count": len(submit_report.get("canceled_orders", [])),
                "buy_paused": bool(plan_report.get("buy_paused")),
                "pause_reasons": list(plan_report.get("pause_reasons", [])),
                "buy_cap_applied": bool(plan_report.get("buy_cap_applied")),
                "buy_budget_notional": _safe_float(plan_report.get("buy_budget_notional")),
                "planned_buy_notional": _safe_float(plan_report.get("planned_buy_notional")),
                "max_position_notional": _safe_float(plan_report.get("max_position_notional")),
                "short_cap_applied": bool(plan_report.get("short_cap_applied")),
                "short_cover_paused": bool(plan_report.get("short_cover_paused")),
                "short_budget_notional": _safe_float(plan_report.get("short_budget_notional")),
                "planned_short_notional": _safe_float(plan_report.get("planned_short_notional")),
                "max_short_position_notional": _safe_float(plan_report.get("max_short_position_notional")),
                "inventory_tier_active": bool((plan_report.get("inventory_tier") or {}).get("active")),
                "inventory_tier_ratio": _safe_float((plan_report.get("inventory_tier") or {}).get("ratio")),
                "volatility_buy_pause": bool((plan_report.get("market_guard") or {}).get("buy_pause_active")),
                "shift_frozen": bool((plan_report.get("market_guard") or {}).get("shift_frozen")),
                "market_guard_return_ratio": _safe_float((plan_report.get("market_guard") or {}).get("return_ratio")),
                "market_guard_amplitude_ratio": _safe_float((plan_report.get("market_guard") or {}).get("amplitude_ratio")),
                "auto_regime_enabled": bool(getattr(args, "auto_regime_enabled", False)),
                "auto_regime_regime": str(((plan_report.get("auto_regime") or {}).get("regime", "") or "")),
                "auto_regime_reason": (plan_report.get("auto_regime") or {}).get("reason"),
                "auto_regime_pending_count": int(((plan_report.get("auto_regime") or {}).get("pending_count", 0) or 0)),
                "auto_regime_confirm_cycles": int(((plan_report.get("auto_regime") or {}).get("confirm_cycles", 0) or 0)),
                "auto_regime_switched": bool((plan_report.get("auto_regime") or {}).get("switched")),
                "xaut_adaptive_enabled": bool(plan_report.get("xaut_adaptive")),
                "xaut_adaptive_state": str(((plan_report.get("xaut_adaptive") or {}).get("active_state", "") or "")),
                "xaut_adaptive_candidate_state": str(((plan_report.get("xaut_adaptive") or {}).get("candidate_state", "") or "")),
                "xaut_adaptive_pending_count": int(((plan_report.get("xaut_adaptive") or {}).get("pending_count", 0) or 0)),
                "xaut_adaptive_reason": (plan_report.get("xaut_adaptive") or {}).get("reason"),
                "neutral_hourly_scale_enabled": bool(((plan_report.get("neutral_hourly_scale") or {}).get("enabled"))),
                "neutral_hourly_scale_ratio": _safe_float((plan_report.get("neutral_hourly_scale") or {}).get("scale")),
                "neutral_hourly_regime": str(((plan_report.get("neutral_hourly_scale") or {}).get("regime", "") or "")),
                "neutral_hourly_reason": (plan_report.get("neutral_hourly_scale") or {}).get("reason"),
                "neutral_hourly_bucket": (plan_report.get("neutral_hourly_scale") or {}).get("bucket"),
                "effective_buy_levels": int(plan_report.get("effective_buy_levels", 0) or 0),
                "effective_sell_levels": int(plan_report.get("effective_sell_levels", 0) or 0),
                "effective_per_order_notional": _safe_float(plan_report.get("effective_per_order_notional")),
                "effective_base_position_notional": _safe_float(plan_report.get("effective_base_position_notional")),
                "custom_grid_runtime_min_price": _safe_float(plan_report.get("custom_grid_runtime_min_price")),
                "custom_grid_runtime_max_price": _safe_float(plan_report.get("custom_grid_runtime_max_price")),
                "custom_grid_roll_enabled": bool(((plan_report.get("custom_grid_roll") or {}).get("enabled"))),
                "custom_grid_roll_checked": bool(((plan_report.get("custom_grid_roll") or {}).get("checked"))),
                "custom_grid_roll_triggered": bool(((plan_report.get("custom_grid_roll") or {}).get("triggered"))),
                "custom_grid_roll_reason": (plan_report.get("custom_grid_roll") or {}).get("reason"),
                "custom_grid_roll_interval_minutes": int(((plan_report.get("custom_grid_roll") or {}).get("interval_minutes", 0) or 0)),
                "custom_grid_roll_trade_threshold": int(((plan_report.get("custom_grid_roll") or {}).get("trade_threshold", 0) or 0)),
                "custom_grid_roll_upper_distance_ratio": _safe_float((plan_report.get("custom_grid_roll") or {}).get("upper_distance_ratio")),
                "custom_grid_roll_shift_levels": int(((plan_report.get("custom_grid_roll") or {}).get("shift_levels", 0) or 0)),
                "custom_grid_roll_current_trade_count": int(((plan_report.get("custom_grid_roll") or {}).get("current_trade_count", 0) or 0)),
                "custom_grid_roll_trade_baseline": int(((plan_report.get("custom_grid_roll") or {}).get("trade_baseline", 0) or 0)),
                "custom_grid_roll_trades_since_last_roll": int(((plan_report.get("custom_grid_roll") or {}).get("trades_since_last_roll", 0) or 0)),
                "custom_grid_roll_levels_above_current": int(((plan_report.get("custom_grid_roll") or {}).get("levels_above_current", 0) or 0)),
                "custom_grid_roll_required_levels_above": int(((plan_report.get("custom_grid_roll") or {}).get("required_levels_above", 0) or 0)),
                "custom_grid_roll_last_check_bucket": (plan_report.get("custom_grid_roll") or {}).get("last_check_bucket"),
                "custom_grid_roll_last_applied_at": (plan_report.get("custom_grid_roll") or {}).get("last_applied_at"),
                "custom_grid_roll_last_applied_price": _safe_float((plan_report.get("custom_grid_roll") or {}).get("last_applied_price")),
                "custom_grid_roll_old_min_price": _safe_float((plan_report.get("custom_grid_roll") or {}).get("old_min_price")),
                "custom_grid_roll_old_max_price": _safe_float((plan_report.get("custom_grid_roll") or {}).get("old_max_price")),
                "custom_grid_roll_new_min_price": _safe_float((plan_report.get("custom_grid_roll") or {}).get("new_min_price")),
                "custom_grid_roll_new_max_price": _safe_float((plan_report.get("custom_grid_roll") or {}).get("new_max_price")),
                "custom_grid_roll_last_old_min_price": _safe_float((plan_report.get("custom_grid_roll") or {}).get("last_old_min_price")),
                "custom_grid_roll_last_old_max_price": _safe_float((plan_report.get("custom_grid_roll") or {}).get("last_old_max_price")),
                "custom_grid_roll_last_new_min_price": _safe_float((plan_report.get("custom_grid_roll") or {}).get("last_new_min_price")),
                "custom_grid_roll_last_new_max_price": _safe_float((plan_report.get("custom_grid_roll") or {}).get("last_new_max_price")),
                "fixed_center_roll": dict(plan_report.get("fixed_center_roll") or {}),
                "shift_moves": list(plan_report.get("shift_moves", [])),
                "reconcile": reconcile_snapshot,
                "reconcile_checked": bool(reconcile_snapshot.get("checked")),
                "reconcile_ok": reconcile_snapshot.get("ok"),
                "reconcile_message": reconcile_snapshot.get("message"),
                "reconcile_interval_cycles": int(reconcile_snapshot.get("interval_cycles", 0) or 0),
                "reconcile_expected_open_order_count": int(reconcile_snapshot.get("expected_open_order_count", 0) or 0),
                "reconcile_actual_open_order_count": int(reconcile_snapshot.get("actual_open_order_count", 0) or 0),
                "reconcile_expected_actual_net_qty": _safe_float(reconcile_snapshot.get("expected_actual_net_qty")),
                "reconcile_actual_actual_net_qty": _safe_float(reconcile_snapshot.get("actual_actual_net_qty")),
                "executed": bool(submit_report.get("executed")),
                "trade_audit_appended": int(audit_sync.get("trade_appended", 0) or 0),
                "income_audit_appended": int(audit_sync.get("income_appended", 0) or 0),
                "audit_error": audit_sync.get("error"),
                "error_message": None,
            }
            _append_jsonl(summary_path, summary)
            _print_cycle_summary(summary)
            consecutive_errors = 0
        except KeyboardInterrupt:
            raise
        except Exception as exc:
            consecutive_errors += 1
            error_report = {
                "ts": cycle_started_at.isoformat(),
                "cycle": cycle,
                "symbol": args.symbol.upper().strip(),
                "error_type": exc.__class__.__name__,
                "error_message": str(exc),
                "traceback": traceback.format_exc(),
                "consecutive_errors": consecutive_errors,
            }
            _append_jsonl(summary_path, error_report)
            print(f"[{cycle}] error: {exc}")
            if consecutive_errors >= args.max_consecutive_errors:
                raise SystemExit(
                    f"Stopped after {consecutive_errors} consecutive errors. Inspect {summary_path} and {submit_report_path}."
                ) from exc

        if args.reset_state:
            args.reset_state = False
        if args.iterations and cycle >= args.iterations:
            break
        time.sleep(args.sleep_seconds)


if __name__ == "__main__":
    main()
