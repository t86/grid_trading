from __future__ import annotations

import argparse
import bisect
import copy
import json
import math
import os
import subprocess
import sys
import time
import traceback
from datetime import datetime, timedelta, timezone
from decimal import Decimal
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
    read_jsonl,
    scan_iso_bounds,
    trade_row_key,
    trade_row_time_ms,
    write_json as _write_json,
)
from .backtest import build_grid_levels
from .data import (
    FuturesMarketStream,
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
    resolve_futures_market_snapshot,
)
from .live_check import extract_symbol_position
from .maker_flatten_runner import (
    flatten_client_order_prefix,
    is_flatten_order,
    load_live_flatten_snapshot,
)
from .runtime_guards import evaluate_runtime_guards, normalize_runtime_guard_config, summarize_futures_runtime_guard_inputs
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
    build_best_quote_long_flip_plan,
    build_micro_grid_plan,
    build_short_micro_grid_plan,
    build_best_quote_short_flip_plan,
    diff_open_orders,
    load_or_initialize_state,
    preserve_sticky_exit_orders,
    preserve_sticky_entry_orders,
    shift_center_price,
)
from .submit_plan import (
    _build_client_order_id,
    _ignore_noop_error,
    _is_reduce_only_reject,
    cap_reduce_only_place_orders_to_position,
    estimate_mid_drift_steps,
    preserve_queue_priority_in_execution_actions,
    prepare_post_only_order_request,
    validate_plan_report,
)
from .dry_run import _round_order_price, _round_order_qty
from .inventory_grid_plan import build_inventory_grid_orders
from .inventory_grid_recovery import rebuild_inventory_grid_runtime
from .inventory_grid_state import apply_inventory_grid_fill, new_inventory_grid_runtime

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
EXECUTION_MARKET_SNAPSHOT_CACHE_TTL_SECONDS = 0.25
RUNNER_MARKET_SNAPSHOT_MAX_AGE_SECONDS = 3.0
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
    "bard_12h_push_neutral_v2": "通用刷量V1",
    XAUT_LONG_ADAPTIVE_PROFILE: "XAUT 自适应做多 v1",
    XAUT_SHORT_ADAPTIVE_PROFILE: "XAUT 自适应做空 v1",
    "xaut_near_price_guarded_v1": "XAUT 近价护栏 v1",
    "xaut_volume_guarded_bard_v2": "XAUT 冲量控损 v2",
}
AUTO_REGIME_PROFILE_STEP_HINTS: dict[str, tuple[float, int]] = {
    AUTO_REGIME_STABLE_PROFILE: (0.0004, 2),
    AUTO_REGIME_DEFENSIVE_PROFILE: (0.0008, 4),
}
BEST_QUOTE_SHORT_PROFILE = "robo_best_quote_short_v1"
BEST_QUOTE_LONG_PROFILES = {
    "ethusdc_best_quote_long_ping_pong_v1",
    "btcusdc_best_quote_long_ping_pong_v1",
}
COMPETITION_RUNTIME_CACHE_KEY = "competition_inventory_grid_runtime_cache"
COMPETITION_POSITION_QTY_EPSILON = 1e-9
XAUT_ADAPTIVE_TRANSITION_CONFIRMATIONS = {
    (XAUT_ADAPTIVE_STATE_NORMAL, XAUT_ADAPTIVE_STATE_DEFENSIVE): 2,
    (XAUT_ADAPTIVE_STATE_DEFENSIVE, XAUT_ADAPTIVE_STATE_NORMAL): 2,
    (XAUT_ADAPTIVE_STATE_DEFENSIVE, XAUT_ADAPTIVE_STATE_REDUCE_ONLY): 1,
    (XAUT_ADAPTIVE_STATE_REDUCE_ONLY, XAUT_ADAPTIVE_STATE_DEFENSIVE): 2,
    (XAUT_ADAPTIVE_STATE_NORMAL, XAUT_ADAPTIVE_STATE_REDUCE_ONLY): 1,
}
XAUT_ADAPTIVE_NORMAL_15M_AMPLITUDE_RATIO = 0.0035
XAUT_ADAPTIVE_NORMAL_60M_AMPLITUDE_RATIO = 0.0075
XAUT_ADAPTIVE_DEFENSIVE_15M_AMPLITUDE_RATIO = 0.006
XAUT_ADAPTIVE_DEFENSIVE_60M_AMPLITUDE_RATIO = 0.012
XAUT_ADAPTIVE_REDUCE_ONLY_15M_AMPLITUDE_RATIO = 0.009
XAUT_ADAPTIVE_REDUCE_ONLY_60M_AMPLITUDE_RATIO = 0.016
XAUT_ADAPTIVE_MIN_STEP_BASE_RATIO = 0.30
XAUT_ADAPTIVE_MIN_STEP_MID_RATIO = 0.00045
XAUT_ADAPTIVE_MIN_STEP_TICKS = 20
XAUT_ADAPTIVE_MIN_STEP_SPREADS = 20.0
ADAPTIVE_STEP_HISTORY_RETENTION_SECONDS = 10 * 60
ADAPTIVE_STEP_MIN_COVERAGE_RATIO = 0.5
SYNTHETIC_TP_ONLY_WATCHDOG_MIN_GAP_STEPS = 8.0
SYNTHETIC_TP_ONLY_WATCHDOG_MIN_DURATION_SECONDS = 180.0


class StartupProtectionError(RuntimeError):
    """Raised when startup preconditions fail and the runner should stop."""


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
            "step_price": 7.4,
            "buy_levels": 10,
            "sell_levels": 6,
            "per_order_notional": 60.0,
            "base_position_notional": 220.0,
            "up_trigger_steps": 4,
            "down_trigger_steps": 5,
            "shift_steps": 3,
            "pause_short_position_notional": 620.0,
            "max_short_position_notional": 720.0,
            "inventory_tier_start_notional": 320.0,
            "inventory_tier_end_notional": 520.0,
            "inventory_tier_buy_levels": 14,
            "inventory_tier_sell_levels": 3,
            "inventory_tier_per_order_notional": 45.0,
            "inventory_tier_base_position_notional": 100.0,
            "short_cover_pause_amp_trigger_ratio": 0.0060,
            "short_cover_pause_down_return_trigger_ratio": -0.0045,
            "autotune_symbol_enabled": False,
            "excess_inventory_reduce_only_enabled": False,
        },
        XAUT_ADAPTIVE_STATE_DEFENSIVE: {
            "step_price": 10.5,
            "buy_levels": 14,
            "sell_levels": 2,
            "per_order_notional": 35.0,
            "base_position_notional": 70.0,
            "up_trigger_steps": 6,
            "down_trigger_steps": 4,
            "shift_steps": 2,
            "pause_short_position_notional": 620.0,
            "max_short_position_notional": 720.0,
            "inventory_tier_start_notional": 180.0,
            "inventory_tier_end_notional": 300.0,
            "inventory_tier_buy_levels": 16,
            "inventory_tier_sell_levels": 2,
            "inventory_tier_per_order_notional": 30.0,
            "inventory_tier_base_position_notional": 50.0,
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


def _normalize_adaptive_step_history(
    raw: Any,
    *,
    now: datetime,
    retention_seconds: int = ADAPTIVE_STEP_HISTORY_RETENTION_SECONDS,
) -> list[tuple[datetime, float]]:
    if not isinstance(raw, list):
        return []
    cutoff = now - timedelta(seconds=max(int(retention_seconds), 60))
    future_tolerance = now + timedelta(seconds=5)
    dedup: dict[datetime, float] = {}
    for item in raw:
        if not isinstance(item, dict):
            continue
        ts_raw = item.get("ts")
        mid_price = _safe_float(item.get("mid_price"))
        if not ts_raw or mid_price <= 0:
            continue
        try:
            ts = datetime.fromisoformat(str(ts_raw))
        except (TypeError, ValueError):
            continue
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        else:
            ts = ts.astimezone(timezone.utc)
        if ts < cutoff or ts > future_tolerance:
            continue
        dedup[ts] = mid_price
    return sorted(dedup.items(), key=lambda item: item[0])


def _serialize_adaptive_step_history(history: list[tuple[datetime, float]]) -> list[dict[str, Any]]:
    return [{"ts": ts.isoformat(), "mid_price": float(price)} for ts, price in history]


def _append_adaptive_step_sample(
    *,
    state: dict[str, Any],
    now: datetime,
    mid_price: float,
) -> list[tuple[datetime, float]]:
    history = _normalize_adaptive_step_history(state.get("adaptive_step_history"), now=now)
    safe_mid_price = max(float(mid_price), 0.0)
    if safe_mid_price > 0:
        history.append((now, safe_mid_price))
        dedup: dict[datetime, float] = {}
        for ts, price in history:
            dedup[ts] = price
        history = sorted(dedup.items(), key=lambda item: item[0])
    state["adaptive_step_history"] = _serialize_adaptive_step_history(history)
    return history


def _resolve_adaptive_step_window_stats(
    history: list[tuple[datetime, float]],
    *,
    now: datetime,
    window_seconds: int,
) -> dict[str, Any] | None:
    safe_window_seconds = max(int(window_seconds), 0)
    if safe_window_seconds <= 0 or len(history) < 2:
        return None
    boundary = now - timedelta(seconds=safe_window_seconds)
    timestamps = [ts for ts, _ in history]
    start_index = bisect.bisect_left(timestamps, boundary)
    if start_index > 0:
        start_index -= 1
    window = [(ts, price) for ts, price in history[start_index:] if ts <= now]
    if len(window) < 2:
        return None
    coverage_seconds = max((window[-1][0] - window[0][0]).total_seconds(), 0.0)
    if coverage_seconds < safe_window_seconds * ADAPTIVE_STEP_MIN_COVERAGE_RATIO:
        return None
    prices = [price for _, price in window]
    open_price = prices[0]
    close_price = prices[-1]
    high_price = max(prices)
    low_price = min(prices)
    return {
        "window_seconds": safe_window_seconds,
        "sample_count": len(window),
        "coverage_seconds": coverage_seconds,
        "open_time": window[0][0].isoformat(),
        "close_time": window[-1][0].isoformat(),
        "open": open_price,
        "close": close_price,
        "high": high_price,
        "low": low_price,
        "return_ratio": (close_price / open_price - 1.0) if open_price > 0 else 0.0,
        "amplitude_ratio": (high_price / low_price - 1.0) if low_price > 0 else 0.0,
    }


def resolve_adaptive_step_price(
    *,
    state: dict[str, Any],
    now: datetime,
    mid_price: float,
    base_step_price: float,
    tick_size: float | None,
    enabled: bool,
    window_30s_abs_return_ratio: float | None,
    window_30s_amplitude_ratio: float | None,
    window_1m_abs_return_ratio: float | None,
    window_1m_amplitude_ratio: float | None,
    window_3m_abs_return_ratio: float | None,
    window_5m_abs_return_ratio: float | None,
    max_scale: float,
    base_per_order_notional: float | None = None,
    base_base_position_notional: float | None = None,
    base_inventory_tier_start_notional: float | None = None,
    base_inventory_tier_end_notional: float | None = None,
    base_inventory_tier_per_order_notional: float | None = None,
    base_inventory_tier_base_position_notional: float | None = None,
    base_pause_buy_position_notional: float | None = None,
    base_pause_short_position_notional: float | None = None,
    base_max_position_notional: float | None = None,
    base_max_short_position_notional: float | None = None,
    base_max_total_notional: float | None = None,
    min_per_order_scale: float = 1.0,
    min_position_limit_scale: float = 1.0,
) -> dict[str, Any]:
    def _safe_min_scale(value: float | None) -> float:
        return min(max(_safe_float(value), 0.0), 1.0) or 0.0

    def _scale_if_positive(value: float | None, ratio: float) -> float | None:
        if value is None:
            return None
        safe_value = float(value)
        if safe_value <= 0:
            return safe_value
        return safe_value * ratio

    history = _append_adaptive_step_sample(
        state=state,
        now=now,
        mid_price=mid_price,
    )
    safe_base_step = max(float(base_step_price), 0.0)
    safe_tick = max(_safe_float(tick_size), 0.0)
    safe_max_scale = max(float(max_scale), 1.0)
    safe_min_per_order_scale = _safe_min_scale(min_per_order_scale) or 1.0
    safe_min_position_limit_scale = _safe_min_scale(min_position_limit_scale) or 1.0
    report = {
        "enabled": bool(enabled),
        "active": False,
        "controls_active": False,
        "base_step_price": safe_base_step,
        "effective_step_price": safe_base_step,
        "scale": 1.0,
        "raw_scale": 1.0,
        "per_order_scale": 1.0,
        "position_limit_scale": 1.0,
        "dominant_window": None,
        "dominant_metric": None,
        "dominant_value": 0.0,
        "dominant_threshold": 0.0,
        "reason": None,
        "history_count": len(history),
        "metrics": {
            "window_30s": _resolve_adaptive_step_window_stats(history, now=now, window_seconds=30),
            "window_1m": _resolve_adaptive_step_window_stats(history, now=now, window_seconds=60),
            "window_3m": _resolve_adaptive_step_window_stats(history, now=now, window_seconds=180),
            "window_5m": _resolve_adaptive_step_window_stats(history, now=now, window_seconds=300),
        },
        "effective_per_order_notional": _scale_if_positive(base_per_order_notional, 1.0),
        "effective_base_position_notional": _scale_if_positive(base_base_position_notional, 1.0),
        "effective_inventory_tier_start_notional": _scale_if_positive(base_inventory_tier_start_notional, 1.0),
        "effective_inventory_tier_end_notional": _scale_if_positive(base_inventory_tier_end_notional, 1.0),
        "effective_inventory_tier_per_order_notional": _scale_if_positive(base_inventory_tier_per_order_notional, 1.0),
        "effective_inventory_tier_base_position_notional": _scale_if_positive(base_inventory_tier_base_position_notional, 1.0),
        "effective_pause_buy_position_notional": _scale_if_positive(base_pause_buy_position_notional, 1.0),
        "effective_pause_short_position_notional": _scale_if_positive(base_pause_short_position_notional, 1.0),
        "effective_max_position_notional": _scale_if_positive(base_max_position_notional, 1.0),
        "effective_max_short_position_notional": _scale_if_positive(base_max_short_position_notional, 1.0),
        "effective_max_total_notional": _scale_if_positive(base_max_total_notional, 1.0),
    }
    if not enabled or safe_base_step <= 0:
        return report

    thresholds = {
        "window_30s": {
            "abs_return_ratio": _safe_float(window_30s_abs_return_ratio),
            "amplitude_ratio": _safe_float(window_30s_amplitude_ratio),
        },
        "window_1m": {
            "abs_return_ratio": _safe_float(window_1m_abs_return_ratio),
            "amplitude_ratio": _safe_float(window_1m_amplitude_ratio),
        },
        "window_3m": {
            "abs_return_ratio": _safe_float(window_3m_abs_return_ratio),
        },
        "window_5m": {
            "abs_return_ratio": _safe_float(window_5m_abs_return_ratio),
        },
    }
    candidates: list[tuple[float, str, str, float, float]] = []
    for window_key, window_stats in report["metrics"].items():
        if not isinstance(window_stats, dict):
            continue
        abs_return_ratio = abs(_safe_float(window_stats.get("return_ratio")))
        amplitude_ratio = _safe_float(window_stats.get("amplitude_ratio"))
        return_threshold = _safe_float((thresholds.get(window_key) or {}).get("abs_return_ratio"))
        amplitude_threshold = _safe_float((thresholds.get(window_key) or {}).get("amplitude_ratio"))
        if return_threshold > 0:
            candidates.append(
                (
                    abs_return_ratio / return_threshold,
                    window_key,
                    "abs_return_ratio",
                    abs_return_ratio,
                    return_threshold,
                )
            )
        if amplitude_threshold > 0:
            candidates.append(
                (
                    amplitude_ratio / amplitude_threshold,
                    window_key,
                    "amplitude_ratio",
                    amplitude_ratio,
                    amplitude_threshold,
                )
            )
    if not candidates:
        return report

    raw_scale, dominant_window, dominant_metric, dominant_value, dominant_threshold = max(
        candidates,
        key=lambda item: item[0],
    )
    report.update(
        {
            "raw_scale": raw_scale,
            "dominant_window": dominant_window,
            "dominant_metric": dominant_metric,
            "dominant_value": dominant_value,
            "dominant_threshold": dominant_threshold,
        }
    )
    if raw_scale <= 1.0:
        return report

    effective_scale = min(raw_scale, safe_max_scale)
    desired_step = safe_base_step * effective_scale
    effective_step_price = _round_up_to_step(desired_step, safe_tick if safe_tick > 0 else None)
    scale = (effective_step_price / safe_base_step) if safe_base_step > 0 else 1.0
    per_order_scale = max(1.0 / effective_scale, safe_min_per_order_scale)
    position_limit_scale = max(1.0 / effective_scale, safe_min_position_limit_scale)
    report.update(
        {
            "controls_active": True,
            "active": effective_step_price > safe_base_step + 1e-12,
            "effective_step_price": effective_step_price,
            "scale": scale,
            "per_order_scale": per_order_scale,
            "position_limit_scale": position_limit_scale,
            "reason": (
                f"{dominant_window} {dominant_metric}={dominant_value * 100:.2f}% "
                f">= {dominant_threshold * 100:.2f}%"
            ),
            "effective_per_order_notional": _scale_if_positive(base_per_order_notional, per_order_scale),
            "effective_base_position_notional": _scale_if_positive(base_base_position_notional, position_limit_scale),
            "effective_inventory_tier_start_notional": _scale_if_positive(base_inventory_tier_start_notional, position_limit_scale),
            "effective_inventory_tier_end_notional": _scale_if_positive(base_inventory_tier_end_notional, position_limit_scale),
            "effective_inventory_tier_per_order_notional": _scale_if_positive(base_inventory_tier_per_order_notional, per_order_scale),
            "effective_inventory_tier_base_position_notional": _scale_if_positive(base_inventory_tier_base_position_notional, position_limit_scale),
            "effective_pause_buy_position_notional": _scale_if_positive(base_pause_buy_position_notional, 1.0),
            "effective_pause_short_position_notional": _scale_if_positive(base_pause_short_position_notional, 1.0),
            "effective_max_position_notional": _scale_if_positive(base_max_position_notional, position_limit_scale),
            "effective_max_short_position_notional": _scale_if_positive(base_max_short_position_notional, position_limit_scale),
            "effective_max_total_notional": _scale_if_positive(base_max_total_notional, position_limit_scale),
        }
    )
    return report


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


def _normalize_live_book_snapshot(book_row: dict[str, Any] | None, *, symbol: str) -> dict[str, float]:
    row = book_row if isinstance(book_row, dict) else {}
    bid_price = _safe_float(row.get("bid_price", row.get("bidPrice")))
    ask_price = _safe_float(row.get("ask_price", row.get("askPrice")))
    if bid_price <= 0 or ask_price <= 0:
        raise RuntimeError(f"no book ticker returned for {symbol}")
    return {
        "bid_price": bid_price,
        "ask_price": ask_price,
    }


def _build_execution_market_snapshot_fetcher(
    *,
    symbol: str,
    initial_book: dict[str, Any] | None = None,
    snapshot_loader: Any | None = None,
    ttl_seconds: float = EXECUTION_MARKET_SNAPSHOT_CACHE_TTL_SECONDS,
) -> Any:
    cached_snapshot: dict[str, float] | None = None
    cached_at = 0.0
    safe_ttl = max(float(ttl_seconds), 0.0)

    if initial_book is not None:
        cached_snapshot = _normalize_live_book_snapshot(initial_book, symbol=symbol)
        cached_at = time.monotonic()

    def _fetch(*, force_refresh: bool = False) -> dict[str, float]:
        nonlocal cached_snapshot, cached_at
        now = time.monotonic()
        if not force_refresh and cached_snapshot is not None and (now - cached_at) <= safe_ttl:
            return dict(cached_snapshot)
        if snapshot_loader is not None:
            live_snapshot = snapshot_loader()
            cached_snapshot = _normalize_live_book_snapshot(live_snapshot, symbol=symbol)
        else:
            live_book_rows = fetch_futures_book_tickers(symbol=symbol)
            if not live_book_rows:
                raise RuntimeError(f"no book ticker returned for {symbol}")
            cached_snapshot = _normalize_live_book_snapshot(live_book_rows[0], symbol=symbol)
        cached_at = time.monotonic()
        return dict(cached_snapshot)

    return _fetch


def _resolve_runner_market_snapshot(args: argparse.Namespace, *, symbol: str) -> dict[str, Any]:
    market_stream = getattr(args, "market_stream", None)
    if market_stream is not None:
        return resolve_futures_market_snapshot(
            symbol,
            stream=market_stream,
            max_snapshot_age_seconds=RUNNER_MARKET_SNAPSHOT_MAX_AGE_SECONDS,
        )
    book_rows = fetch_futures_book_tickers(symbol=symbol)
    premium_rows = fetch_futures_premium_index(symbol=symbol)
    if not book_rows:
        raise RuntimeError(f"No book ticker returned for {symbol}")
    if not premium_rows:
        raise RuntimeError(f"No premium index returned for {symbol}")
    book = book_rows[0]
    premium = premium_rows[0]
    return {
        "symbol": symbol,
        "bid_price": _safe_float(book.get("bid_price")),
        "ask_price": _safe_float(book.get("ask_price")),
        "mark_price": _safe_float(premium.get("mark_price")),
        "funding_rate": _safe_float(premium.get("funding_rate")),
        "next_funding_time": premium.get("next_funding_time"),
        "book_time": book.get("time"),
        "mark_time": premium.get("time"),
        "source": "rest",
    }


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


def _uses_entry_price_cost_basis(strategy_profile: str | None) -> bool:
    normalized_profile = str(strategy_profile or "").strip()
    return normalized_profile in {
        "volume_long_v4",
        "chip_low_wear_guarded_v1",
        "soon_volume_neutral_ping_pong_v1",
        "soonusdt_competition_neutral_ping_pong_v1",
    } or normalized_profile.endswith("_competition_neutral_ping_pong_v1")


def _uses_volume_long_v4_staged_delever(strategy_profile: str | None) -> bool:
    return str(strategy_profile or "").strip() == "volume_long_v4"


def _position_cost_basis_price(position: dict[str, Any], *, prefer_entry_price: bool = False) -> float:
    keys = ("entryPrice", "breakEvenPrice") if prefer_entry_price else ("breakEvenPrice", "entryPrice")
    for key in keys:
        price = max(_safe_float(position.get(key)), 0.0)
        if price > 0:
            return price
    return 0.0


def _resolve_adverse_reduce_cost_basis(
    *,
    side: str,
    requested_strategy_mode: str,
    actual_net_qty: float,
    actual_cost_basis_price: float,
    long_position: dict[str, Any],
    short_position: dict[str, Any],
    synthetic_ledger_snapshot: dict[str, Any] | None,
) -> tuple[float, str | None]:
    normalized_side = str(side or "").strip().lower()
    synthetic_snapshot = synthetic_ledger_snapshot or {}
    normalized_mode = str(requested_strategy_mode or "").strip()
    if normalized_side == "long":
        if normalized_mode == "hedge_neutral":
            actual_long_qty = max(_position_qty(long_position, position_side="LONG"), 0.0)
            actual_long_price = _position_cost_basis_price(long_position)
            if actual_long_qty > 1e-12 and actual_long_price > 0:
                return actual_long_price, "actual_position_long"
        elif actual_net_qty > 1e-12 and actual_cost_basis_price > 0:
            return actual_cost_basis_price, "actual_position"
        synthetic_long_price = max(_safe_float(synthetic_snapshot.get("virtual_long_avg_price")), 0.0)
        if synthetic_long_price > 0:
            return synthetic_long_price, "synthetic_ledger"
        return 0.0, None

    if normalized_mode == "hedge_neutral":
        actual_short_qty = max(_position_qty(short_position, position_side="SHORT"), 0.0)
        actual_short_price = _position_cost_basis_price(short_position)
        if actual_short_qty > 1e-12 and actual_short_price > 0:
            return actual_short_price, "actual_position_short"
    elif actual_net_qty < -1e-12 and actual_cost_basis_price > 0:
        return actual_cost_basis_price, "actual_position"
    synthetic_short_price = max(_safe_float(synthetic_snapshot.get("virtual_short_avg_price")), 0.0)
    if synthetic_short_price > 0:
        return synthetic_short_price, "synthetic_ledger"
    return 0.0, None


def _order_position_side(order: dict[str, Any]) -> str:
    return str(order.get("position_side", order.get("positionSide", "BOTH"))).upper().strip() or "BOTH"


def _order_role(order: dict[str, Any]) -> str:
    return str(order.get("role", "")).lower().strip()


def _volume_long_v4_order_role(order: dict[str, Any]) -> str:
    explicit_role = _order_role(order)
    if explicit_role:
        return explicit_role
    client_order_id = str(order.get("clientOrderId", "")).strip().lower()
    if not client_order_id.startswith("gx-"):
        return ""
    if "-takeprof-" in client_order_id:
        return "take_profit"
    if "-softdele-" in client_order_id:
        return "soft_delever_long"
    if "-harddele-" in client_order_id:
        return "hard_delever_long"
    if "-flowslee-" in client_order_id:
        return "flow_sleeve_long"
    if "-activede-" in client_order_id:
        return "active_delever_long"
    return ""


def _decorate_volume_long_v4_open_orders(open_orders: list[dict[str, Any]]) -> list[dict[str, Any]]:
    decorated_orders: list[dict[str, Any]] = []
    for order in open_orders:
        if not isinstance(order, dict):
            continue
        decorated = dict(order)
        role = _volume_long_v4_order_role(decorated)
        if role:
            decorated["role"] = role
        decorated_orders.append(decorated)
    return decorated_orders


def apply_volume_long_v4_staged_delever(
    *,
    plan: dict[str, Any],
    current_long_qty: float,
    current_long_notional: float,
    current_long_avg_price: float,
    pause_long_position_notional: float | None,
    max_position_notional: float | None,
    per_order_notional: float,
    step_price: float,
    tick_size: float | None,
    bid_price: float,
    ask_price: float,
    soft_loss_steps: float = 0.5,
    hard_loss_steps: float = 1.5,
    soft_max_levels: int = 2,
    hard_max_levels: int = 4,
) -> dict[str, Any]:
    pause_notional = max(_safe_float(pause_long_position_notional), 0.0)
    hard_notional = max(_safe_float(max_position_notional), 0.0)
    report = {
        "enabled": True,
        "active": False,
        "stage": None,
        "pause_long_position_notional": pause_long_position_notional,
        "hard_long_position_notional": max_position_notional,
        "active_sell_order_count": 0,
        "loss_budget_steps": 0.0,
        "loss_floor_price": None,
    }
    if current_long_qty <= 1e-12 or step_price <= 0 or pause_notional <= 0 or current_long_notional < pause_notional:
        return report

    take_profit_orders = [
        dict(item)
        for item in plan.get("sell_orders", [])
        if isinstance(item, dict) and _order_role(item) == "take_profit"
    ]
    if not take_profit_orders:
        return report
    take_profit_orders.sort(key=lambda item: _safe_float(item.get("price")))
    other_sell_orders = [
        dict(item)
        for item in plan.get("sell_orders", [])
        if isinstance(item, dict) and _order_role(item) != "take_profit"
    ]

    hard_stage_active = hard_notional > pause_notional and current_long_notional >= hard_notional - 1e-12
    stage = "hard" if hard_stage_active else "soft"
    safe_per_order_notional = max(_safe_float(per_order_notional), 1e-12)
    if stage == "hard":
        excess_notional = max(current_long_notional - max(hard_notional, pause_notional), 0.0)
        requested_count = max(2, 2 + int(math.ceil(excess_notional / safe_per_order_notional)))
        max_levels = max(int(hard_max_levels), 0)
        loss_budget_steps = max(_safe_float(hard_loss_steps), 0.0)
        role = "hard_delever_long"
    else:
        excess_notional = max(current_long_notional - pause_notional, 0.0)
        requested_count = max(1, int(math.ceil(excess_notional / safe_per_order_notional)))
        max_levels = max(int(soft_max_levels), 0)
        loss_budget_steps = max(_safe_float(soft_loss_steps), 0.0)
        role = "soft_delever_long"
    active_count = min(len(take_profit_orders), max_levels, requested_count)
    if active_count <= 0:
        return report

    loss_floor_price = None
    if current_long_avg_price > 0 and loss_budget_steps >= 0:
        floor_raw = max(current_long_avg_price - (loss_budget_steps * step_price), 0.0)
        loss_floor_price = _round_order_price(floor_raw, tick_size, "SELL")
        if loss_floor_price is not None and loss_floor_price <= 0:
            loss_floor_price = None

    active_orders: list[dict[str, Any]] = []
    for index, order in enumerate(take_profit_orders[:active_count], start=1):
        active = dict(order)
        target_price_raw = max(float(ask_price), 0.0) + (max(index - 1, 0) * float(step_price))
        target_price = _round_order_price(target_price_raw, tick_size, "SELL")
        if loss_floor_price is not None:
            target_price = max(target_price, loss_floor_price)
        minimum_resting_price = _round_order_price(
            max(float(bid_price) + (float(tick_size) if tick_size and tick_size > 0 else 0.0), 0.0),
            tick_size,
            "SELL",
        )
        target_price = max(target_price, minimum_resting_price)
        active["role"] = role
        active["price"] = target_price
        active["notional"] = target_price * _safe_float(active.get("qty"))
        active["level"] = index
        active["delever_stage"] = stage
        active["loss_budget_steps"] = loss_budget_steps
        active_orders.append(active)

    plan["sell_orders"] = sorted(
        [*active_orders, *take_profit_orders[active_count:], *other_sell_orders],
        key=lambda item: (_safe_float(item.get("price")), str(item.get("side", "")).upper().strip()),
    )
    report.update(
        {
            "active": True,
            "stage": stage,
            "active_sell_order_count": active_count,
            "loss_budget_steps": loss_budget_steps,
            "loss_floor_price": loss_floor_price,
        }
    )
    return report


def apply_volume_long_v4_flow_sleeve(
    *,
    plan: dict[str, Any],
    enabled: bool,
    current_long_qty: float,
    current_long_notional: float,
    current_long_cost_basis_price: float,
    trigger_notional: float | None,
    reduce_to_notional: float | None,
    sleeve_notional: float,
    levels: int,
    per_order_notional: float,
    order_notional: float | None,
    max_loss_ratio: float | None,
    step_price: float,
    tick_size: float | None,
    step_size: float | None,
    min_qty: float | None,
    min_notional: float | None,
    bid_price: float,
    ask_price: float,
    sell_offset_steps: float = 0.0,
) -> dict[str, Any]:
    report = {
        "enabled": bool(enabled),
        "active": False,
        "trigger_notional": trigger_notional,
        "reduce_to_notional": reduce_to_notional,
        "sleeve_notional": sleeve_notional,
        "remaining_sleeve_notional": 0.0,
        "order_notional": order_notional,
        "levels": levels,
        "max_loss_ratio": max_loss_ratio,
        "cost_basis_price": current_long_cost_basis_price,
        "loss_floor_price": None,
        "released_take_profit_order_count": 0,
        "released_take_profit_notional": 0.0,
        "placed_order_count": 0,
        "placed_notional": 0.0,
        "loss_guard_clamped_order_count": 0,
        "blocked_reason": None,
    }
    if not enabled:
        return report

    safe_trigger_notional = max(_safe_float(trigger_notional), 0.0)
    safe_reduce_to_notional = max(_safe_float(reduce_to_notional), 0.0)
    safe_sleeve_notional = max(_safe_float(sleeve_notional), 0.0)
    safe_levels = max(int(levels or 0), 0)
    safe_step_price = max(_safe_float(step_price), 0.0)
    safe_order_notional = max(_safe_float(order_notional), _safe_float(per_order_notional), 0.0)
    safe_max_loss_ratio = max(_safe_float(max_loss_ratio), 0.0)
    long_notional = max(_safe_float(current_long_notional), 0.0)
    long_qty = max(_safe_float(current_long_qty), 0.0)
    if (
        long_qty <= 1e-12
        or long_notional <= 1e-12
        or safe_trigger_notional <= 0
        or safe_sleeve_notional <= 0
        or safe_levels <= 0
        or safe_step_price <= 0
        or safe_order_notional <= 0
    ):
        report["blocked_reason"] = "invalid_config_or_position"
        return report
    if long_notional < safe_trigger_notional:
        report["blocked_reason"] = "below_trigger"
        return report
    if safe_reduce_to_notional <= 0:
        report["blocked_reason"] = "missing_reduce_to_notional"
        return report
    sell_budget_notional = min(safe_sleeve_notional, max(long_notional - safe_reduce_to_notional, 0.0))
    report["remaining_sleeve_notional"] = sell_budget_notional
    if sell_budget_notional <= 1e-12:
        report["blocked_reason"] = "at_or_below_reduce_target"
        return report

    safe_cost_basis_price = max(_safe_float(current_long_cost_basis_price), 0.0)
    if safe_cost_basis_price <= 0 or safe_max_loss_ratio <= 0:
        report["blocked_reason"] = "missing_loss_guard"
        return report
    min_allowed_price = safe_cost_basis_price * (1.0 - safe_max_loss_ratio)
    floor_price = _round_order_price(min_allowed_price, tick_size, "SELL")
    report["loss_floor_price"] = floor_price if floor_price > 0 else None

    kept_sell_orders: list[dict[str, Any]] = []
    take_profit_candidates: list[dict[str, Any]] = []
    for item in plan.get("sell_orders", []):
        if not isinstance(item, dict):
            continue
        order = dict(item)
        if _volume_long_v4_order_role(order) == "take_profit":
            take_profit_candidates.append(order)
        else:
            kept_sell_orders.append(order)
    released_notional = 0.0
    released_count = 0
    for order in sorted(take_profit_candidates, key=lambda item: _safe_float(item.get("price")), reverse=True):
        if released_notional < sell_budget_notional - 1e-12:
            released_notional += _safe_float(order.get("notional"))
            released_count += 1
            continue
        kept_sell_orders.append(order)
    prospective_sell_orders = kept_sell_orders

    existing_keys = {
        f"{str(item.get('side', '')).upper().strip()}:{_safe_float(item.get('price')):.10f}"
        for key in ("buy_orders", "bootstrap_orders")
        for item in plan.get(key, [])
        if isinstance(item, dict)
    }
    existing_keys.update(
        f"{str(item.get('side', '')).upper().strip()}:{_safe_float(item.get('price')):.10f}"
        for item in prospective_sell_orders
        if isinstance(item, dict)
    )
    flow_orders: list[dict[str, Any]] = []
    placed_count = 0
    placed_notional = 0.0
    clamped_count = 0
    remaining = sell_budget_notional
    for level in range(1, safe_levels + 1):
        distance_steps = max((float(level) - 1.0) + float(sell_offset_steps), 0.0)
        raw_price = float(Decimal(str(ask_price)) + (Decimal(str(distance_steps)) * Decimal(str(safe_step_price))))
        price = _round_order_price(raw_price, tick_size, "SELL")
        if floor_price > 0 and price < floor_price:
            price = floor_price
            clamped_count += 1
        if price <= bid_price:
            continue
        if min_allowed_price > 0 and price < min_allowed_price - 1e-12:
            report["blocked_reason"] = "loss_guard"
            break
        qty = _round_order_qty(min(safe_order_notional, remaining) / price, step_size)
        notional = price * qty
        if qty <= 0:
            continue
        if min_qty is not None and qty < min_qty:
            continue
        if min_notional is not None and notional < min_notional:
            continue
        key = f"SELL:{price:.10f}"
        if key in existing_keys:
            continue
        existing_keys.add(key)
        flow_orders.append(
            {
                "side": "SELL",
                "price": price,
                "qty": qty,
                "notional": notional,
                "level": 940 + level,
                "role": "flow_sleeve_long",
                "position_side": "BOTH",
                "force_reduce_only": True,
                "flow_sleeve": True,
            }
        )
        placed_count += 1
        placed_notional += notional
        remaining = max(remaining - notional, 0.0)
        if remaining <= 1e-12:
            break

    report["active"] = placed_count > 0
    report["placed_order_count"] = placed_count
    report["placed_notional"] = placed_notional
    report["loss_guard_clamped_order_count"] = clamped_count
    report["remaining_sleeve_notional"] = max(remaining, 0.0)
    if placed_count > 0:
        plan["sell_orders"] = sorted(
            [*prospective_sell_orders, *flow_orders],
            key=lambda item: (_safe_float(item.get("price")), str(item.get("side", "")).upper().strip()),
        )
        report["released_take_profit_order_count"] = released_count
        report["released_take_profit_notional"] = released_notional
        report["blocked_reason"] = None
    elif not report.get("blocked_reason"):
        report["blocked_reason"] = "no_valid_order"
    return report


def resolve_exposure_escalation(
    *,
    state: dict[str, Any],
    enabled: bool,
    now: datetime,
    current_long_notional: float,
    current_long_qty: float,
    current_long_cost_basis_price: float,
    mid_price: float,
    trigger_notional: float | None,
    hold_seconds: float | None,
    target_notional: float | None,
    max_loss_ratio: float | None,
    hard_unrealized_loss_limit: float | None,
) -> dict[str, Any]:
    current_time = now.astimezone(timezone.utc)
    report = {
        "enabled": bool(enabled),
        "active": False,
        "reason": None,
        "blocked_reason": None,
        "trigger_notional": trigger_notional,
        "hold_seconds": hold_seconds,
        "target_notional": target_notional,
        "max_loss_ratio": max_loss_ratio,
        "hard_unrealized_loss_limit": hard_unrealized_loss_limit,
        "observed_since": None,
        "held_seconds": 0.0,
        "current_long_notional": max(_safe_float(current_long_notional), 0.0),
        "unrealized_pnl": 0.0,
    }
    escalation_state = state.setdefault("exposure_escalation", {})
    if not isinstance(escalation_state, dict):
        escalation_state = {}
        state["exposure_escalation"] = escalation_state

    if not enabled:
        escalation_state["observed_since"] = None
        report["blocked_reason"] = "disabled"
        return report

    safe_trigger = max(_safe_float(trigger_notional), 0.0)
    safe_hold = max(_safe_float(hold_seconds), 0.0)
    safe_target = max(_safe_float(target_notional), 0.0)
    safe_max_loss = max(_safe_float(max_loss_ratio), 0.0)
    safe_hard_loss = max(_safe_float(hard_unrealized_loss_limit), 0.0)
    long_notional = report["current_long_notional"]
    long_qty = max(_safe_float(current_long_qty), 0.0)
    cost_basis = max(_safe_float(current_long_cost_basis_price), 0.0)
    mid = max(_safe_float(mid_price), 0.0)
    unrealized_pnl = (mid - cost_basis) * long_qty if long_qty > 0 and cost_basis > 0 and mid > 0 else 0.0
    report["unrealized_pnl"] = unrealized_pnl

    if safe_target <= 0 or safe_max_loss <= 0:
        escalation_state["observed_since"] = None
        report["blocked_reason"] = "invalid_config"
        return report

    if long_notional <= safe_target + 1e-12:
        escalation_state["observed_since"] = None
        report.update(
            {
                "blocked_reason": "below_target",
                "target_notional": safe_target,
                "max_loss_ratio": safe_max_loss,
                "hard_unrealized_loss_limit": safe_hard_loss if safe_hard_loss > 0 else hard_unrealized_loss_limit,
            }
        )
        return report

    hard_loss_active = safe_hard_loss > 0 and unrealized_pnl <= -safe_hard_loss
    if hard_loss_active:
        report.update(
            {
                "active": True,
                "reason": "hard_unrealized_loss_limit",
                "blocked_reason": None,
                "target_notional": safe_target,
                "max_loss_ratio": safe_max_loss,
                "hard_unrealized_loss_limit": safe_hard_loss,
            }
        )
        escalation_state["last_active_at"] = current_time.isoformat()
        escalation_state["last_reason"] = report["reason"]
        return report

    if safe_trigger <= 0:
        escalation_state["observed_since"] = None
        report["blocked_reason"] = "missing_trigger"
        return report
    if long_notional < safe_trigger:
        escalation_state["observed_since"] = None
        report["blocked_reason"] = "below_trigger"
        return report

    observed_since = _parse_state_datetime(escalation_state.get("observed_since"))
    if observed_since is None:
        observed_since = current_time
        escalation_state["observed_since"] = observed_since.isoformat()
    held_seconds = max((current_time - observed_since).total_seconds(), 0.0)
    report["observed_since"] = observed_since.isoformat()
    report["held_seconds"] = held_seconds
    if held_seconds < safe_hold:
        report["blocked_reason"] = "waiting_hold_time"
        return report

    report.update(
        {
            "active": True,
            "reason": "hold_time_exceeded",
            "blocked_reason": None,
            "target_notional": safe_target,
            "max_loss_ratio": safe_max_loss,
        }
    )
    escalation_state["last_active_at"] = current_time.isoformat()
    escalation_state["last_reason"] = report["reason"]
    return report


def _is_long_entry_order(order: dict[str, Any]) -> bool:
    role = _order_role(order)
    position_side = _order_position_side(order)
    return role in {"bootstrap", "entry", "bootstrap_long", "entry_long"} or (
        position_side in {"BOTH", "LONG"} and role in {"bootstrap", "entry"}
    )


def _is_short_entry_order(order: dict[str, Any]) -> bool:
    role = _order_role(order)
    return role in {"bootstrap_short", "entry_short"}


def _is_long_exit_order(order: dict[str, Any]) -> bool:
    return _order_role(order) in {
        "take_profit_long",
        "active_delever_long",
        "soft_delever_long",
        "hard_delever_long",
        "flow_sleeve_long",
    }


def _is_short_exit_order(order: dict[str, Any]) -> bool:
    return _order_role(order) in {"take_profit_short", "active_delever_short", "flow_sleeve_short"}



def _resolve_reduce_only_flag(
    *,
    strategy_mode: str,
    side: str,
    role: str,
) -> bool | None:
    normalized_mode = str(strategy_mode or "").strip() or "one_way_long"
    normalized_side = str(side or "").upper().strip()
    normalized_role = str(role or "").lower().strip()
    if normalized_mode == "hedge_neutral":
        return None
    if normalized_role in {"grid_exit", "forced_reduce", "tail_cleanup"}:
        return True
    if normalized_side == "SELL" and normalized_role in {
        "take_profit",
        "take_profit_long",
        "active_delever_long",
        "soft_delever_long",
        "hard_delever_long",
        "flow_sleeve_long",
    }:
        return True
    if normalized_side == "BUY" and normalized_role in {
        "take_profit_short",
        "active_delever_short",
        "flow_sleeve_short",
    }:
        return True
    return None



def assess_synthetic_tp_only_watchdog(
    *,
    state: dict[str, Any],
    strategy_mode: str,
    plan: dict[str, Any],
    current_long_qty: float,
    current_short_qty: float,
    mid_price: float,
    step_price: float,
    now: datetime | None = None,
) -> dict[str, Any]:
    current_time = now or _utc_now()
    report = {
        "enabled": _is_synthetic_neutral_mode(strategy_mode),
        "tp_only_mode": False,
        "candidate": False,
        "active": False,
        "started_at": None,
        "last_seen_at": None,
        "duration_seconds": 0.0,
        "min_gap_steps": SYNTHETIC_TP_ONLY_WATCHDOG_MIN_GAP_STEPS,
        "min_duration_seconds": SYNTHETIC_TP_ONLY_WATCHDOG_MIN_DURATION_SECONDS,
        "buy_gap_steps": None,
        "sell_gap_steps": None,
        "nearest_buy_price": None,
        "nearest_sell_price": None,
        "buy_roles": [],
        "sell_roles": [],
        "reason": None,
    }
    if not report["enabled"]:
        state["synthetic_tp_only_watchdog_state"] = report
        return report

    buy_orders = [item for item in plan.get("buy_orders", []) if isinstance(item, dict)]
    sell_orders = [item for item in plan.get("sell_orders", []) if isinstance(item, dict)]
    bootstrap_orders = [item for item in plan.get("bootstrap_orders", []) if isinstance(item, dict)]
    buy_roles = sorted({_order_role(item) for item in buy_orders if _order_role(item)})
    sell_roles = sorted({_order_role(item) for item in sell_orders if _order_role(item)})
    nearest_buy_price = max((_safe_float(item.get("price")) for item in buy_orders), default=None)
    nearest_sell_price = min((_safe_float(item.get("price")) for item in sell_orders), default=None)
    safe_step_price = max(_safe_float(step_price), 0.0)
    safe_mid_price = max(_safe_float(mid_price), 0.0)
    buy_gap_steps = None
    sell_gap_steps = None
    if nearest_buy_price is not None and safe_step_price > 0 and safe_mid_price > 0:
        buy_gap_steps = max((safe_mid_price - nearest_buy_price) / safe_step_price, 0.0)
    if nearest_sell_price is not None and safe_step_price > 0 and safe_mid_price > 0:
        sell_gap_steps = max((nearest_sell_price - safe_mid_price) / safe_step_price, 0.0)

    report["buy_roles"] = buy_roles
    report["sell_roles"] = sell_roles
    report["nearest_buy_price"] = nearest_buy_price
    report["nearest_sell_price"] = nearest_sell_price
    report["buy_gap_steps"] = buy_gap_steps
    report["sell_gap_steps"] = sell_gap_steps

    tp_only_mode = (
        current_long_qty > 1e-12
        and current_short_qty > 1e-12
        and not bootstrap_orders
        and bool(buy_orders)
        and bool(sell_orders)
        and set(buy_roles).issubset({"take_profit_short", "active_delever_short"})
        and set(sell_roles).issubset({"take_profit_long", "active_delever_long"})
        and not any(_is_long_entry_order(item) or _is_short_entry_order(item) for item in [*buy_orders, *sell_orders])
    )
    report["tp_only_mode"] = tp_only_mode

    candidate = (
        tp_only_mode
        and buy_gap_steps is not None
        and sell_gap_steps is not None
        and buy_gap_steps >= SYNTHETIC_TP_ONLY_WATCHDOG_MIN_GAP_STEPS
        and sell_gap_steps >= SYNTHETIC_TP_ONLY_WATCHDOG_MIN_GAP_STEPS
    )
    report["candidate"] = candidate

    previous = dict(state.get("synthetic_tp_only_watchdog_state") or {})
    started_at = None
    duration_seconds = 0.0
    if candidate:
        previous_started_at = str(previous.get("started_at") or "").strip()
        if bool(previous.get("candidate")) and previous_started_at:
            started_at = previous_started_at
            try:
                duration_seconds = max(
                    (current_time - datetime.fromisoformat(previous_started_at)).total_seconds(),
                    0.0,
                )
            except ValueError:
                started_at = None
                duration_seconds = 0.0
        if started_at is None:
            started_at = current_time.isoformat()
            duration_seconds = 0.0

    report["started_at"] = started_at
    report["last_seen_at"] = current_time.isoformat() if candidate else None
    report["duration_seconds"] = duration_seconds
    report["active"] = candidate and duration_seconds >= SYNTHETIC_TP_ONLY_WATCHDOG_MIN_DURATION_SECONDS
    if candidate:
        report["reason"] = (
            f"buy_gap={buy_gap_steps:.1f} step sell_gap={sell_gap_steps:.1f} step "
            f"long={current_long_qty * safe_mid_price:.2f} short={current_short_qty * safe_mid_price:.2f}"
        )

    state["synthetic_tp_only_watchdog_state"] = report
    return report



def apply_synthetic_inventory_exit_priority(
    *,
    plan: dict[str, Any],
    strategy_mode: str,
    current_long_qty: float,
    current_short_qty: float,
    current_long_notional: float,
    current_short_notional: float,
    pause_long_position_notional: float | None,
    pause_short_position_notional: float | None,
    near_market_entries_allowed: bool,
    step_size: float | None,
    strict_exit_only: bool = False,
) -> dict[str, Any]:
    qty_tolerance = max(_safe_float(step_size) * 0.5, 1e-9)
    report = {
        "enabled": _is_synthetic_neutral_mode(strategy_mode),
        "active": False,
        "direction": None,
        "qty_tolerance": qty_tolerance,
        "exit_ready": False,
        "pruned_bootstrap_orders": 0,
        "pruned_entry_orders": 0,
        "reason": None,
    }
    if not report["enabled"]:
        return report

    pure_long_inventory = current_long_qty > qty_tolerance and current_short_qty <= qty_tolerance
    pure_short_inventory = current_short_qty > qty_tolerance and current_long_qty <= qty_tolerance
    if not pure_long_inventory and not pure_short_inventory:
        return report

    if pure_long_inventory:
        exit_ready = any(
            _is_long_exit_order(item)
            for item in plan.get("sell_orders", [])
            if isinstance(item, dict)
        )
        report["direction"] = "long"
        report["exit_ready"] = exit_ready
        pause_notional = max(_safe_float(pause_long_position_notional), 0.0)
        pause_reached = pause_notional > 0 and current_long_notional >= pause_notional - 1e-12
        if not exit_ready and not strict_exit_only:
            return report
        if near_market_entries_allowed and not pause_reached and not strict_exit_only:
            report["reason"] = "near_market_below_pause"
            return report
        bootstrap_before = len(plan.get("bootstrap_orders", []))
        entry_before = len(plan.get("buy_orders", []))
        plan["bootstrap_orders"] = [item for item in plan.get("bootstrap_orders", []) if not _is_long_entry_order(item)]
        plan["buy_orders"] = [item for item in plan.get("buy_orders", []) if not _is_long_entry_order(item)]
        report["pruned_bootstrap_orders"] = bootstrap_before - len(plan.get("bootstrap_orders", []))
        report["pruned_entry_orders"] = entry_before - len(plan.get("buy_orders", []))
        report["active"] = report["pruned_bootstrap_orders"] > 0 or report["pruned_entry_orders"] > 0
        if report["active"]:
            report["reason"] = "pure_long_inventory_with_exit_ready"
        return report

    exit_ready = any(
        _is_short_exit_order(item)
        for item in plan.get("buy_orders", [])
        if isinstance(item, dict)
    )
    report["direction"] = "short"
    report["exit_ready"] = exit_ready
    if not exit_ready and not strict_exit_only:
        return report
    pause_notional = max(_safe_float(pause_short_position_notional), 0.0)
    pause_reached = pause_notional > 0 and current_short_notional >= pause_notional - 1e-12
    if near_market_entries_allowed and not pause_reached and not strict_exit_only:
        report["reason"] = "near_market_below_pause"
        return report
    bootstrap_before = len(plan.get("bootstrap_orders", []))
    entry_before = len(plan.get("sell_orders", []))
    plan["bootstrap_orders"] = [item for item in plan.get("bootstrap_orders", []) if not _is_short_entry_order(item)]
    plan["sell_orders"] = [item for item in plan.get("sell_orders", []) if not _is_short_entry_order(item)]
    report["pruned_bootstrap_orders"] = bootstrap_before - len(plan.get("bootstrap_orders", []))
    report["pruned_entry_orders"] = entry_before - len(plan.get("sell_orders", []))
    report["active"] = report["pruned_bootstrap_orders"] > 0 or report["pruned_entry_orders"] > 0
    if report["active"]:
        report["reason"] = "pure_short_inventory_with_exit_ready"
    return report



def _direction_from_return_ratio(return_ratio: float, *, tolerance: float = 1e-12) -> str | None:
    if return_ratio > tolerance:
        return "up"
    if return_ratio < -tolerance:
        return "down"
    return None


def _window_directional_efficiency(window: dict[str, Any] | None) -> float:
    if not isinstance(window, dict):
        return 0.0
    amplitude_ratio = max(_safe_float(window.get("amplitude_ratio")), 0.0)
    if amplitude_ratio <= 0:
        return 0.0
    return min(abs(_safe_float(window.get("return_ratio"))) / amplitude_ratio, 1.0)


def _is_synthetic_neutral_mode(strategy_mode: str) -> bool:
    return str(strategy_mode).strip() == "synthetic_neutral"


def resolve_sticky_exit_mode(
    *,
    active_delever: dict[str, Any],
    synthetic_inventory_exit_priority: dict[str, Any],
) -> dict[str, Any]:
    keys: list[str] = []
    roles: set[str] = set()

    if bool(active_delever.get("active")):
        trigger_mode = str(active_delever.get("trigger_mode") or "active").strip() or "active"
        if int(active_delever.get("active_sell_order_count") or 0) > 0:
            keys.append(f"active_delever_long:{trigger_mode}")
            roles.update({"take_profit_long", "active_delever_long"})
        if int(active_delever.get("active_buy_order_count") or 0) > 0:
            keys.append(f"active_delever_short:{trigger_mode}")
            roles.update({"take_profit_short", "active_delever_short"})

    if not keys and bool(synthetic_inventory_exit_priority.get("active")):
        direction = str(synthetic_inventory_exit_priority.get("direction") or "").strip()
        if direction == "long":
            keys.append("synthetic_inventory_exit_priority:long")
            roles.add("take_profit_long")
        elif direction == "short":
            keys.append("synthetic_inventory_exit_priority:short")
            roles.add("take_profit_short")

    return {
        "key": "|".join(keys) if keys else None,
        "roles": sorted(roles),
    }


def _is_inventory_target_neutral_mode(strategy_mode: str) -> bool:
    return str(strategy_mode).strip() == "inventory_target_neutral"


def _is_competition_inventory_grid_mode(strategy_mode: str) -> bool:
    return str(strategy_mode).strip() == "competition_inventory_grid"


def _is_custom_grid_mode(args: argparse.Namespace) -> bool:
    return bool(getattr(args, "custom_grid_enabled", False))


def _is_one_way_short_mode(strategy_mode: str) -> bool:
    return str(strategy_mode).strip() == "one_way_short"


def _is_best_quote_short_profile(strategy_profile: str) -> bool:
    return str(strategy_profile).strip() == BEST_QUOTE_SHORT_PROFILE


def _is_best_quote_long_profile(strategy_profile: str) -> bool:
    return str(strategy_profile).strip() in BEST_QUOTE_LONG_PROFILES


def _is_best_quote_neutral_profile(strategy_profile: str) -> bool:
    return str(strategy_profile).strip().endswith("_competition_neutral_ping_pong_v1")


def _is_strict_neutral_ping_pong_profile(strategy_profile: str) -> bool:
    return str(strategy_profile).strip() == "ethusdc_strict_competition_neutral_ping_pong_v1"


def _startup_pending(state: dict[str, Any]) -> bool:
    return _truthy((state or {}).get("startup_pending"))


def assess_flat_start_guard(
    *,
    strategy_mode: str,
    actual_net_qty: float,
    open_orders: list[dict[str, Any]],
    enabled: bool,
    block_open_orders: bool = True,
) -> dict[str, Any]:
    mode = str(strategy_mode or "").strip()
    open_order_count = len([item for item in open_orders if isinstance(item, dict)])
    result = {
        "enabled": bool(enabled),
        "blocked": False,
        "mode": mode,
        "open_order_count": open_order_count,
        "block_open_orders": bool(block_open_orders),
        "reverse_qty": 0.0,
        "reason": None,
    }
    if not enabled or mode not in {"one_way_long", "one_way_short"}:
        return result

    tolerance = 1e-9
    safe_actual_net_qty = float(actual_net_qty)
    if mode == "one_way_long" and safe_actual_net_qty < -tolerance:
        result.update(
            {
                "blocked": True,
                "reverse_qty": abs(safe_actual_net_qty),
                "reason": (
                    "flat-start 已拦截启动：one_way_long 启动前必须先清空反向空仓，"
                    f"当前 actual_net_qty={safe_actual_net_qty:.8f}"
                ),
            }
        )
        return result
    if mode == "one_way_short" and safe_actual_net_qty > tolerance:
        result.update(
            {
                "blocked": True,
                "reverse_qty": abs(safe_actual_net_qty),
                "reason": (
                    "flat-start 已拦截启动：one_way_short 启动前必须先清空反向多仓，"
                    f"当前 actual_net_qty={safe_actual_net_qty:.8f}"
                ),
            }
        )
        return result
    if open_order_count > 0 and block_open_orders:
        result.update(
            {
                "blocked": True,
                "reason": (
                    "flat-start 已拦截启动：启动前必须先撤掉遗留挂单，"
                    f"当前 open_order_count={open_order_count}"
                ),
            }
        )
    elif open_order_count > 0:
        result["reason"] = (
            "flat-start 检测到遗留挂单，但 cancel_stale 已启用，"
            f"允许启动后自动撤单；当前 open_order_count={open_order_count}"
        )
    return result


def apply_warm_start_bootstrap_guard(
    *,
    plan: dict[str, Any],
    strategy_mode: str,
    current_long_qty: float,
    current_short_qty: float,
    startup_pending: bool,
    enabled: bool,
) -> dict[str, Any]:
    result = {
        "enabled": bool(enabled),
        "active": False,
        "direction": None,
        "existing_qty": 0.0,
        "suppressed_bootstrap_qty": 0.0,
        "suppressed_bootstrap_notional": 0.0,
        "reason": None,
    }
    if not enabled or not startup_pending:
        return result

    tolerance = 1e-9
    mode = str(strategy_mode or "").strip()
    if mode == "one_way_long" and float(current_long_qty) > tolerance:
        suppressed_orders = [item for item in plan.get("bootstrap_orders", []) if _is_long_entry_order(item)]
        suppressed_qty = sum(_safe_float(item.get("qty")) for item in suppressed_orders)
        suppressed_notional = sum(_safe_float(item.get("notional")) for item in suppressed_orders)
        if suppressed_qty > tolerance or suppressed_notional > tolerance:
            plan["bootstrap_orders"] = [item for item in plan.get("bootstrap_orders", []) if not _is_long_entry_order(item)]
            plan["bootstrap_qty"] = 0.0
            if "bootstrap_long_qty" in plan:
                plan["bootstrap_long_qty"] = 0.0
            result.update(
                {
                    "active": True,
                    "direction": "long",
                    "existing_qty": float(current_long_qty),
                    "suppressed_bootstrap_qty": suppressed_qty,
                    "suppressed_bootstrap_notional": suppressed_notional,
                    "reason": (
                        f"warm-start: startup inventory current_long_qty={float(current_long_qty):.8f} > 0, "
                        "bootstrap suppressed"
                    ),
                }
            )
    elif mode == "one_way_short" and float(current_short_qty) > tolerance:
        suppressed_orders = [item for item in plan.get("bootstrap_orders", []) if _is_short_entry_order(item)]
        suppressed_qty = sum(_safe_float(item.get("qty")) for item in suppressed_orders)
        suppressed_notional = sum(_safe_float(item.get("notional")) for item in suppressed_orders)
        if suppressed_qty > tolerance or suppressed_notional > tolerance:
            plan["bootstrap_orders"] = [item for item in plan.get("bootstrap_orders", []) if not _is_short_entry_order(item)]
            plan["bootstrap_qty"] = 0.0
            if "bootstrap_short_qty" in plan:
                plan["bootstrap_short_qty"] = 0.0
            result.update(
                {
                    "active": True,
                    "direction": "short",
                    "existing_qty": float(current_short_qty),
                    "suppressed_bootstrap_qty": suppressed_qty,
                    "suppressed_bootstrap_notional": suppressed_notional,
                    "reason": (
                        f"warm-start: startup inventory current_short_qty={float(current_short_qty):.8f} > 0, "
                        "bootstrap suppressed"
                    ),
                }
            )
    return result


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
                    "virtual_long_lots": [{"qty": net_qty, "price": max(float(entry_price), 0.0)}],
                    "virtual_short_qty": 0.0,
                    "virtual_short_avg_price": 0.0,
                    "virtual_short_lots": [],
                }
            )
        elif net_qty < 0:
            ledger.update(
                {
                    "virtual_long_qty": 0.0,
                    "virtual_long_avg_price": 0.0,
                    "virtual_long_lots": [],
                    "virtual_short_qty": abs(net_qty),
                    "virtual_short_avg_price": max(float(entry_price), 0.0),
                    "virtual_short_lots": [{"qty": abs(net_qty), "price": max(float(entry_price), 0.0)}],
                }
            )
        else:
            ledger.update(
                {
                    "virtual_long_qty": 0.0,
                    "virtual_long_avg_price": 0.0,
                    "virtual_long_lots": [],
                    "virtual_short_qty": 0.0,
                    "virtual_short_avg_price": 0.0,
                    "virtual_short_lots": [],
                }
            )
        ledger["initialized"] = True
        ledger["last_trade_time_ms"] = int(ledger.get("last_trade_time_ms") or 0)
        ledger["last_trade_keys_at_time"] = list(ledger.get("last_trade_keys_at_time") or [])
        ledger["unmatched_trade_count"] = int(ledger.get("unmatched_trade_count") or 0)
    else:
        long_qty = max(_safe_float(ledger.get("virtual_long_qty")), 0.0)
        long_avg = max(_safe_float(ledger.get("virtual_long_avg_price")), 0.0)
        short_qty = max(_safe_float(ledger.get("virtual_short_qty")), 0.0)
        short_avg = max(_safe_float(ledger.get("virtual_short_avg_price")), 0.0)
        ledger["virtual_long_lots"] = _normalize_synthetic_lots(
            ledger.get("virtual_long_lots"),
            fallback_qty=long_qty,
            fallback_price=long_avg,
        )
        ledger["virtual_short_lots"] = _normalize_synthetic_lots(
            ledger.get("virtual_short_lots"),
            fallback_qty=short_qty,
            fallback_price=short_avg,
        )
        ledger["virtual_long_qty"], ledger["virtual_long_avg_price"] = _synthetic_book_from_lots(
            ledger["virtual_long_lots"]
        )
        ledger["virtual_short_qty"], ledger["virtual_short_avg_price"] = _synthetic_book_from_lots(
            ledger["virtual_short_lots"]
        )
    _ensure_synthetic_buffer_fields(ledger)
    state["synthetic_ledger"] = ledger
    return ledger


def _reset_synthetic_ledger_to_actual(
    *,
    state: dict[str, Any],
    actual_position_qty: float,
    entry_price: float,
    reason: str,
    now: datetime | None = None,
) -> dict[str, Any]:
    current_time = now or _utc_now()
    net_qty = float(actual_position_qty)
    ledger = {
        "initialized": True,
        "virtual_long_qty": max(net_qty, 0.0),
        "virtual_long_avg_price": max(float(entry_price), 0.0) if net_qty > 0 else 0.0,
        "virtual_long_lots": [{"qty": max(net_qty, 0.0), "price": max(float(entry_price), 0.0)}] if net_qty > 0 else [],
        "virtual_short_qty": max(-net_qty, 0.0),
        "virtual_short_avg_price": max(float(entry_price), 0.0) if net_qty < 0 else 0.0,
        "virtual_short_lots": [{"qty": max(-net_qty, 0.0), "price": max(float(entry_price), 0.0)}] if net_qty < 0 else [],
        "grid_buffer_realized_notional": 0.0,
        "grid_buffer_spent_notional": 0.0,
        "last_trade_time_ms": int(current_time.timestamp() * 1000),
        "last_trade_keys_at_time": [],
        "unmatched_trade_count": 0,
        "resynced_at": current_time.isoformat(),
        "resync_reason": str(reason or "manual_reset"),
    }
    state["synthetic_ledger"] = ledger
    state["synthetic_order_refs"] = {}
    state["synthetic_ledger_resync_required"] = False
    return ledger


def _synthetic_order_ref_from_state(state: dict[str, Any], order_id: int | None) -> dict[str, Any] | None:
    refs = state.get("synthetic_order_refs")
    if not isinstance(refs, dict) or order_id is None:
        return None
    item = refs.get(str(int(order_id)))
    return dict(item) if isinstance(item, dict) else None


def _decorate_synthetic_open_orders(
    *,
    state: dict[str, Any],
    open_orders: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    refs = state.get("synthetic_order_refs")
    client_refs: dict[str, dict[str, Any]] = {}
    if isinstance(refs, dict):
        for item in refs.values():
            if not isinstance(item, dict):
                continue
            client_order_id = str(item.get("client_order_id", "")).strip()
            if client_order_id and client_order_id not in client_refs:
                client_refs[client_order_id] = dict(item)

    decorated_orders: list[dict[str, Any]] = []
    for order in open_orders:
        if not isinstance(order, dict):
            continue
        decorated = dict(order)
        order_id = epoch_ms(order.get("orderId"))
        ref = _synthetic_order_ref_from_state(state, order_id)
        if ref is None:
            client_order_id = str(order.get("clientOrderId", "")).strip()
            ref = client_refs.get(client_order_id)
        if isinstance(ref, dict):
            role = str(ref.get("role", "")).strip()
            if role:
                decorated["role"] = role
            position_side = str(ref.get("position_side", "")).upper().strip()
            if position_side:
                decorated["positionSide"] = position_side
        decorated_orders.append(decorated)
    return decorated_orders


def _normalize_synthetic_lots(
    raw_lots: Any,
    *,
    fallback_qty: float = 0.0,
    fallback_price: float = 0.0,
) -> list[dict[str, float]]:
    lots: list[dict[str, float]] = []
    if isinstance(raw_lots, list):
        for item in raw_lots:
            if not isinstance(item, dict):
                continue
            qty = max(_safe_float(item.get("qty")), 0.0)
            price = max(_safe_float(item.get("price")), 0.0)
            if qty <= 1e-12 or price <= 0:
                continue
            lots.append({"qty": qty, "price": price})
    if lots:
        return lots
    if fallback_qty > 1e-12 and fallback_price > 0:
        return [{"qty": float(fallback_qty), "price": float(fallback_price)}]
    return []


def _synthetic_book_from_lots(lots: list[dict[str, float]]) -> tuple[float, float]:
    total_qty = sum(max(_safe_float(item.get("qty")), 0.0) for item in lots)
    if total_qty <= 1e-12:
        return 0.0, 0.0
    total_cost = sum(max(_safe_float(item.get("qty")), 0.0) * max(_safe_float(item.get("price")), 0.0) for item in lots)
    return total_qty, (total_cost / total_qty if total_qty > 0 else 0.0)


def _consume_synthetic_lots_lifo(lots: list[dict[str, float]], qty: float) -> list[dict[str, float]]:
    remaining = max(float(qty), 0.0)
    updated = [{"qty": float(item["qty"]), "price": float(item["price"])} for item in lots]
    while updated and remaining > 1e-12:
        tail = updated[-1]
        tail_qty = max(_safe_float(tail.get("qty")), 0.0)
        if tail_qty <= remaining + 1e-12:
            remaining = max(remaining - tail_qty, 0.0)
            updated.pop()
            continue
        tail["qty"] = tail_qty - remaining
        remaining = 0.0
    return updated


def _consume_synthetic_lots_lifo_with_cost(
    lots: list[dict[str, float]],
    qty: float,
) -> tuple[list[dict[str, float]], float]:
    remaining = max(float(qty), 0.0)
    realized_cost = 0.0
    updated = [{"qty": float(item["qty"]), "price": float(item["price"])} for item in lots]
    while updated and remaining > 1e-12:
        tail = updated[-1]
        tail_qty = max(_safe_float(tail.get("qty")), 0.0)
        tail_price = max(_safe_float(tail.get("price")), 0.0)
        consumed_qty = min(tail_qty, remaining)
        realized_cost += consumed_qty * tail_price
        if tail_qty <= remaining + 1e-12:
            remaining = max(remaining - tail_qty, 0.0)
            updated.pop()
            continue
        tail["qty"] = tail_qty - remaining
        remaining = 0.0
    return updated, realized_cost


def _consume_synthetic_lots_fifo_with_cost(
    lots: list[dict[str, float]],
    qty: float,
) -> tuple[list[dict[str, float]], float]:
    remaining = max(float(qty), 0.0)
    realized_cost = 0.0
    updated = [{"qty": float(item["qty"]), "price": float(item["price"])} for item in lots]
    while updated and remaining > 1e-12:
        head = updated[0]
        head_qty = max(_safe_float(head.get("qty")), 0.0)
        head_price = max(_safe_float(head.get("price")), 0.0)
        consumed_qty = min(head_qty, remaining)
        realized_cost += consumed_qty * head_price
        if head_qty <= remaining + 1e-12:
            remaining = max(remaining - head_qty, 0.0)
            updated.pop(0)
            continue
        head["qty"] = head_qty - remaining
        remaining = 0.0
    return updated, realized_cost


def _consume_synthetic_lots_with_cost(
    lots: list[dict[str, float]],
    qty: float,
    *,
    consume_mode: str = "lifo",
) -> tuple[list[dict[str, float]], float]:
    normalized_mode = str(consume_mode or "lifo").strip().lower()
    if normalized_mode == "fifo":
        return _consume_synthetic_lots_fifo_with_cost(lots, qty)
    return _consume_synthetic_lots_lifo_with_cost(lots, qty)


def _synthetic_flow_eligible_qty_for_price(
    *,
    lots: list[dict[str, float]],
    side: str,
    price: float,
    max_loss_ratio: float,
    consume_mode: str = "lifo",
) -> float:
    safe_price = max(_safe_float(price), 0.0)
    if not lots or safe_price <= 0:
        return 0.0
    safe_ratio = max(_safe_float(max_loss_ratio), 0.0)
    normalized_side = str(side or "").upper().strip()
    normalized_mode = str(consume_mode or "lifo").strip().lower()
    ordered_lots = list(lots if normalized_mode == "fifo" else reversed(lots))
    eligible_qty = 0.0
    for item in ordered_lots:
        qty = max(_safe_float(item.get("qty")), 0.0)
        lot_price = max(_safe_float(item.get("price")), 0.0)
        if qty <= 1e-12 or lot_price <= 0:
            continue
        if normalized_side == "BUY":
            minimum_cost = safe_price / (1.0 + safe_ratio) if safe_ratio > 0 else safe_price
            eligible = lot_price + 1e-12 >= minimum_cost
        else:
            maximum_cost = safe_price / max(1.0 - safe_ratio, 1e-12) if safe_ratio > 0 else safe_price
            eligible = lot_price <= maximum_cost + 1e-12
        if not eligible:
            break
        eligible_qty += qty
    return eligible_qty


def _project_remaining_synthetic_book(
    *,
    raw_lots: list[dict[str, Any]],
    fallback_qty: float,
    fallback_price: float,
    consume_qty: float,
    consume_mode: str = "lifo",
) -> dict[str, Any]:
    lots = _normalize_synthetic_lots(
        raw_lots,
        fallback_qty=max(float(fallback_qty), 0.0),
        fallback_price=max(float(fallback_price), 0.0),
    )
    if consume_qty <= 1e-12:
        remaining_qty, remaining_avg = _synthetic_book_from_lots(lots)
        return {
            "remaining_lots": lots,
            "remaining_qty": remaining_qty,
            "remaining_avg_price": remaining_avg,
            "realized_cost": 0.0,
        }
    remaining_lots, realized_cost = _consume_synthetic_lots_with_cost(
        lots,
        consume_qty,
        consume_mode=consume_mode,
    )
    remaining_qty, remaining_avg = _synthetic_book_from_lots(remaining_lots)
    return {
        "remaining_lots": remaining_lots,
        "remaining_qty": remaining_qty,
        "remaining_avg_price": remaining_avg,
        "realized_cost": realized_cost,
    }


def _ensure_synthetic_buffer_fields(ledger: dict[str, Any]) -> None:
    ledger["grid_buffer_realized_notional"] = max(_safe_float(ledger.get("grid_buffer_realized_notional")), 0.0)
    ledger["grid_buffer_spent_notional"] = max(_safe_float(ledger.get("grid_buffer_spent_notional")), 0.0)


def _snapshot_synthetic_ledger(
    ledger: dict[str, Any],
    *,
    applied_trade_count: int = 0,
    unmatched_trade_count: int = 0,
) -> dict[str, Any]:
    return {
        "virtual_long_qty": max(_safe_float(ledger.get("virtual_long_qty")), 0.0),
        "virtual_long_avg_price": max(_safe_float(ledger.get("virtual_long_avg_price")), 0.0),
        "virtual_long_lots": list(ledger.get("virtual_long_lots") or []),
        "virtual_short_qty": max(_safe_float(ledger.get("virtual_short_qty")), 0.0),
        "virtual_short_avg_price": max(_safe_float(ledger.get("virtual_short_avg_price")), 0.0),
        "virtual_short_lots": list(ledger.get("virtual_short_lots") or []),
        "grid_buffer_realized_notional": max(_safe_float(ledger.get("grid_buffer_realized_notional")), 0.0),
        "grid_buffer_spent_notional": max(_safe_float(ledger.get("grid_buffer_spent_notional")), 0.0),
        "synthetic_net_compacted_qty_total": max(
            _safe_float(ledger.get("synthetic_net_compacted_qty_total")),
            0.0,
        ),
        "synthetic_net_compacted_last_qty": max(
            _safe_float(ledger.get("synthetic_net_compacted_last_qty")),
            0.0,
        ),
        "synthetic_net_compacted_at": str(ledger.get("synthetic_net_compacted_at") or ""),
        "applied_trade_count": int(applied_trade_count or 0),
        "unmatched_trade_count": int(unmatched_trade_count or 0),
    }


def _compact_synthetic_ledger_matched_exposure(
    *,
    ledger: dict[str, Any],
    actual_position_qty: float,
    entry_price: float,
    fallback_price: float,
    qty_tolerance: float | None,
    now: datetime | None = None,
) -> dict[str, Any]:
    _ensure_synthetic_buffer_fields(ledger)
    long_lots = _normalize_synthetic_lots(
        ledger.get("virtual_long_lots"),
        fallback_qty=max(_safe_float(ledger.get("virtual_long_qty")), 0.0),
        fallback_price=max(_safe_float(ledger.get("virtual_long_avg_price")), 0.0),
    )
    short_lots = _normalize_synthetic_lots(
        ledger.get("virtual_short_lots"),
        fallback_qty=max(_safe_float(ledger.get("virtual_short_qty")), 0.0),
        fallback_price=max(_safe_float(ledger.get("virtual_short_avg_price")), 0.0),
    )
    long_qty, long_avg = _synthetic_book_from_lots(long_lots)
    short_qty, short_avg = _synthetic_book_from_lots(short_lots)
    matched_qty = min(long_qty, short_qty)
    if matched_qty <= 1e-12:
        ledger["virtual_long_qty"] = long_qty
        ledger["virtual_long_avg_price"] = long_avg
        ledger["virtual_long_lots"] = long_lots
        ledger["virtual_short_qty"] = short_qty
        ledger["virtual_short_avg_price"] = short_avg
        ledger["virtual_short_lots"] = short_lots
        ledger["synthetic_net_compacted_last_qty"] = 0.0
        return {"compacted": False, "matched_qty": 0.0}

    tolerance = max(_safe_float(qty_tolerance), 0.0)
    synthetic_net_qty = long_qty - short_qty
    actual_net_qty = float(actual_position_qty)
    use_actual_net = tolerance > 0 and abs(actual_net_qty - synthetic_net_qty) <= tolerance
    target_net_qty = actual_net_qty if use_actual_net else synthetic_net_qty
    if abs(target_net_qty) <= max(tolerance, 1e-12):
        target_net_qty = 0.0

    residual_long_lots, _ = _consume_synthetic_lots_fifo_with_cost(long_lots, matched_qty)
    residual_short_lots, _ = _consume_synthetic_lots_fifo_with_cost(short_lots, matched_qty)
    residual_long_qty, residual_long_avg = _synthetic_book_from_lots(residual_long_lots)
    residual_short_qty, residual_short_avg = _synthetic_book_from_lots(residual_short_lots)
    authoritative_price = max(_safe_float(entry_price), 0.0) if use_actual_net else 0.0
    fallback = max(_safe_float(fallback_price), 0.0)

    if target_net_qty > 0:
        qty = max(float(target_net_qty), 0.0)
        price = authoritative_price or residual_long_avg or long_avg or fallback
        ledger["virtual_long_lots"] = [{"qty": qty, "price": price}] if price > 0 else residual_long_lots
        ledger["virtual_short_lots"] = []
    elif target_net_qty < 0:
        qty = max(abs(float(target_net_qty)), 0.0)
        price = authoritative_price or residual_short_avg or short_avg or fallback
        ledger["virtual_long_lots"] = []
        ledger["virtual_short_lots"] = [{"qty": qty, "price": price}] if price > 0 else residual_short_lots
    else:
        ledger["virtual_long_lots"] = []
        ledger["virtual_short_lots"] = []

    ledger["virtual_long_qty"], ledger["virtual_long_avg_price"] = _synthetic_book_from_lots(
        ledger["virtual_long_lots"]
    )
    ledger["virtual_short_qty"], ledger["virtual_short_avg_price"] = _synthetic_book_from_lots(
        ledger["virtual_short_lots"]
    )
    ledger["synthetic_net_compacted_qty_total"] = max(
        _safe_float(ledger.get("synthetic_net_compacted_qty_total")),
        0.0,
    ) + matched_qty
    ledger["synthetic_net_compacted_last_qty"] = matched_qty
    ledger["synthetic_net_compacted_at"] = (now or _utc_now()).isoformat()
    ledger["synthetic_net_compacted_actual_net_used"] = bool(use_actual_net)
    ledger["synthetic_net_compacted_residual_long_qty"] = residual_long_qty
    ledger["synthetic_net_compacted_residual_short_qty"] = residual_short_qty
    return {
        "compacted": True,
        "matched_qty": matched_qty,
        "target_net_qty": target_net_qty,
        "used_actual_net": bool(use_actual_net),
    }


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
    _ensure_synthetic_buffer_fields(ledger)
    lot_consume_mode = str(order_ref.get("lot_consume_mode", "lifo") or "lifo").strip().lower()

    long_lots = _normalize_synthetic_lots(
        ledger.get("virtual_long_lots"),
        fallback_qty=max(_safe_float(ledger.get("virtual_long_qty")), 0.0),
        fallback_price=max(_safe_float(ledger.get("virtual_long_avg_price")), 0.0),
    )
    short_lots = _normalize_synthetic_lots(
        ledger.get("virtual_short_lots"),
        fallback_qty=max(_safe_float(ledger.get("virtual_short_qty")), 0.0),
        fallback_price=max(_safe_float(ledger.get("virtual_short_avg_price")), 0.0),
    )

    if role in {"bootstrap_long", "entry_long"}:
        long_lots.append({"qty": qty, "price": price})
    elif role == "take_profit_long":
        long_lots, realized_cost = _consume_synthetic_lots_lifo_with_cost(long_lots, qty)
        ledger["grid_buffer_realized_notional"] += max((qty * price) - realized_cost, 0.0)
    elif role in {"active_delever_long", "flow_sleeve_long"}:
        long_lots, realized_cost = _consume_synthetic_lots_with_cost(
            long_lots,
            qty,
            consume_mode=lot_consume_mode,
        )
        ledger["grid_buffer_spent_notional"] += max(realized_cost - (qty * price), 0.0)
    elif role in {"bootstrap_short", "entry_short"}:
        short_lots.append({"qty": qty, "price": price})
    elif role == "take_profit_short":
        short_lots, realized_cost = _consume_synthetic_lots_lifo_with_cost(short_lots, qty)
        ledger["grid_buffer_realized_notional"] += max(realized_cost - (qty * price), 0.0)
    elif role in {"active_delever_short", "flow_sleeve_short"}:
        short_lots, realized_cost = _consume_synthetic_lots_with_cost(
            short_lots,
            qty,
            consume_mode=lot_consume_mode,
        )
        ledger["grid_buffer_spent_notional"] += max((qty * price) - realized_cost, 0.0)
    else:
        return False

    long_qty, long_avg = _synthetic_book_from_lots(long_lots)
    short_qty, short_avg = _synthetic_book_from_lots(short_lots)
    ledger["virtual_long_qty"] = long_qty
    ledger["virtual_long_avg_price"] = long_avg
    ledger["virtual_long_lots"] = long_lots
    ledger["virtual_short_qty"] = short_qty
    ledger["virtual_short_avg_price"] = short_avg
    ledger["virtual_short_lots"] = short_lots
    return True


def _synthetic_net_qty_from_snapshot(snapshot: dict[str, Any]) -> float:
    return max(_safe_float(snapshot.get("virtual_long_qty")), 0.0) - max(_safe_float(snapshot.get("virtual_short_qty")), 0.0)


def _resolve_synthetic_resync_price(
    *,
    actual_position_qty: float,
    entry_price: float,
    fallback_price: float,
    snapshot: dict[str, Any],
) -> float:
    candidates = [max(float(entry_price), 0.0)]
    if actual_position_qty > 0:
        candidates.append(max(_safe_float(snapshot.get("virtual_long_avg_price")), 0.0))
    elif actual_position_qty < 0:
        candidates.append(max(_safe_float(snapshot.get("virtual_short_avg_price")), 0.0))
    if abs(float(actual_position_qty)) <= 1e-12:
        candidates.append(max(float(fallback_price), 0.0))
    for candidate in candidates:
        if candidate > 0:
            return candidate
    return 0.0


def _maybe_resync_synthetic_ledger_to_actual(
    *,
    state: dict[str, Any],
    snapshot: dict[str, Any],
    actual_position_qty: float,
    entry_price: float,
    fallback_price: float,
    qty_tolerance: float | None,
) -> dict[str, Any]:
    tolerance = max(_safe_float(qty_tolerance), 0.0)
    drift_qty = float(actual_position_qty) - _synthetic_net_qty_from_snapshot(snapshot)
    if tolerance <= 0 or abs(drift_qty) <= tolerance:
        snapshot["resynced_to_actual"] = False
        return snapshot

    resolved_price = _resolve_synthetic_resync_price(
        actual_position_qty=actual_position_qty,
        entry_price=entry_price,
        fallback_price=fallback_price,
        snapshot=snapshot,
    )
    ledger = _reset_synthetic_ledger_to_actual(
        state=state,
        actual_position_qty=actual_position_qty,
        entry_price=resolved_price,
        reason="actual_position_drift",
    )
    result = _snapshot_synthetic_ledger(
        ledger,
        applied_trade_count=int(snapshot.get("applied_trade_count") or 0),
        unmatched_trade_count=int(snapshot.get("unmatched_trade_count") or 0),
    )
    result["resynced_to_actual"] = True
    return result


def sync_synthetic_ledger(
    *,
    state: dict[str, Any],
    symbol: str,
    api_key: str,
    api_secret: str,
    recv_window: int,
    actual_position_qty: float,
    entry_price: float,
    qty_tolerance: float | None = None,
    fallback_price: float = 0.0,
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
    snapshot = _snapshot_synthetic_ledger(
        ledger,
        applied_trade_count=applied,
        unmatched_trade_count=unmatched,
    )
    snapshot = _maybe_resync_synthetic_ledger_to_actual(
        state=state,
        snapshot=snapshot,
        actual_position_qty=actual_position_qty,
        entry_price=entry_price,
        fallback_price=fallback_price,
        qty_tolerance=qty_tolerance,
    )
    if not _truthy(snapshot.get("resynced_to_actual")):
        ledger = dict(state.get("synthetic_ledger") or ledger)
        compact_report = _compact_synthetic_ledger_matched_exposure(
            ledger=ledger,
            actual_position_qty=actual_position_qty,
            entry_price=entry_price,
            fallback_price=fallback_price,
            qty_tolerance=qty_tolerance,
        )
        if bool(compact_report.get("compacted")):
            state["synthetic_ledger"] = ledger
            snapshot = _snapshot_synthetic_ledger(
                ledger,
                applied_trade_count=applied,
                unmatched_trade_count=unmatched,
            )
            snapshot["resynced_to_actual"] = False
            snapshot["synthetic_net_compacted"] = True
            snapshot["synthetic_net_compacted_qty"] = float(compact_report.get("matched_qty") or 0.0)
            snapshot["synthetic_net_compacted_used_actual_net"] = bool(compact_report.get("used_actual_net"))
        else:
            snapshot["synthetic_net_compacted"] = False
    return snapshot


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
            "lot_consume_mode": str(request.get("lot_consume_mode", "")).strip().lower(),
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


def _update_inventory_grid_order_refs(
    *,
    state_path: Path,
    strategy_mode: str,
    submit_report: dict[str, Any],
) -> None:
    if not _is_competition_inventory_grid_mode(strategy_mode) or not state_path.exists():
        return
    try:
        state = json.loads(state_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return

    refs = state.get("inventory_grid_order_refs")
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
            "client_order_id": str(response.get("clientOrderId", "")).strip(),
            "updated_at": _isoformat(_utc_now()),
        }

    state["inventory_grid_order_refs"] = refs
    try:
        state_path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    except OSError:
        return


def _competition_runtime_position_qty(runtime: dict[str, Any]) -> float:
    lots = runtime.get("position_lots")
    if not isinstance(lots, list):
        return 0.0
    return sum(max(_safe_float((lot or {}).get("qty")), 0.0) for lot in lots if isinstance(lot, dict))


def _competition_runtime_trade_key(trade: dict[str, Any]) -> str:
    return ":".join(
        (
            str(int(trade.get("time", 0) or 0)),
            str(int(trade.get("id", 0) or 0)),
            str(int(trade.get("orderId", 0) or 0)),
        )
    )


def _load_cached_competition_runtime(state: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
    payload = state.get(COMPETITION_RUNTIME_CACHE_KEY)
    if not isinstance(payload, dict):
        return None, []
    if str(payload.get("strategy_mode", "")).strip() != "competition_inventory_grid":
        state.pop(COMPETITION_RUNTIME_CACHE_KEY, None)
        return None, []
    if str(payload.get("market_type", "")).strip().lower() != "futures":
        state.pop(COMPETITION_RUNTIME_CACHE_KEY, None)
        return None, []

    runtime = payload.get("runtime")
    if not isinstance(runtime, dict) or str(runtime.get("market_type", "")).strip().lower() != "futures":
        state.pop(COMPETITION_RUNTIME_CACHE_KEY, None)
        return None, []

    applied_trade_keys = [str(item).strip() for item in list(payload.get("applied_trade_keys") or []) if str(item).strip()]
    return copy.deepcopy(runtime), applied_trade_keys


def _store_cached_competition_runtime(
    *,
    state: dict[str, Any],
    runtime: dict[str, Any],
    applied_trade_keys: list[str],
) -> None:
    cached_runtime = copy.deepcopy(runtime)
    cached_runtime["pair_credit_steps"] = 0
    cached_runtime["recovery_mode"] = "live"
    cached_runtime["recovery_errors"] = []
    state[COMPETITION_RUNTIME_CACHE_KEY] = {
        "strategy_mode": "competition_inventory_grid",
        "market_type": "futures",
        "runtime": cached_runtime,
        "applied_trade_keys": list(applied_trade_keys),
    }


def _apply_competition_runtime_trade_delta(
    *,
    runtime: dict[str, Any],
    trade: dict[str, Any],
    order_refs: dict[str, dict[str, Any]],
    step_price: float,
) -> bool:
    order_id = str(int(trade.get("orderId", 0) or 0))
    order_ref = order_refs.get(order_id)
    if not isinstance(order_ref, dict):
        return False

    role = str(order_ref.get("role", "") or "").strip()
    side = str(order_ref.get("side", "") or "").upper().strip()
    if not role or not side:
        return False

    current_state = str(runtime.get("direction_state", "flat")).strip().lower()
    if role == "bootstrap_entry" and current_state in {"long_active", "short_active"}:
        if (current_state == "long_active" and side == "SELL") or (current_state == "short_active" and side == "BUY"):
            return False

    try:
        apply_inventory_grid_fill(
            runtime=runtime,
            role=role,
            side=side,
            price=_safe_float(trade.get("price")),
            qty=abs(_safe_float(trade.get("qty"))),
            fill_time_ms=int(trade.get("time", 0) or 0),
            step_price=step_price,
        )
    except ValueError:
        return False
    return True


def _resolve_competition_inventory_grid_runtime(
    *,
    state: dict[str, Any],
    trades: list[dict[str, Any]],
    order_refs: dict[str, dict[str, Any]],
    step_price: float,
    current_position_qty: float,
) -> dict[str, Any]:
    expected_position_qty = max(float(current_position_qty), 0.0)
    strategy_trades = sorted(
        [
            trade
            for trade in list(trades or [])
            if isinstance(trade, dict) and str(int(trade.get("orderId", 0) or 0)) in order_refs
        ],
        key=lambda row: (
            int(row.get("time", 0) or 0),
            int(row.get("id", 0) or 0),
        ),
    )

    cached_runtime, applied_trade_keys = _load_cached_competition_runtime(state)
    if cached_runtime is not None:
        applied_trade_key_set = set(applied_trade_keys)
        cache_valid = True
        for trade in strategy_trades:
            trade_key = _competition_runtime_trade_key(trade)
            if trade_key in applied_trade_key_set:
                continue
            if not _apply_competition_runtime_trade_delta(
                runtime=cached_runtime,
                trade=trade,
                order_refs=order_refs,
                step_price=step_price,
            ):
                cache_valid = False
                break
            applied_trade_keys.append(trade_key)
            applied_trade_key_set.add(trade_key)

        cached_runtime["pair_credit_steps"] = 0
        cached_runtime["recovery_mode"] = "live"
        cached_runtime["recovery_errors"] = []
        if cache_valid and abs(_competition_runtime_position_qty(cached_runtime) - expected_position_qty) <= COMPETITION_POSITION_QTY_EPSILON:
            _store_cached_competition_runtime(
                state=state,
                runtime=cached_runtime,
                applied_trade_keys=applied_trade_keys,
            )
            return cached_runtime

    runtime = rebuild_inventory_grid_runtime(
        market_type="futures",
        trades=strategy_trades,
        order_refs=order_refs,
        step_price=step_price,
        current_position_qty=expected_position_qty,
    )
    if str(runtime.get("recovery_mode", "live") or "live") == "live":
        _store_cached_competition_runtime(
            state=state,
            runtime=runtime,
            applied_trade_keys=[_competition_runtime_trade_key(trade) for trade in strategy_trades],
        )
    elif expected_position_qty <= COMPETITION_POSITION_QTY_EPSILON:
        flat_runtime = new_inventory_grid_runtime(market_type="futures")
        _store_cached_competition_runtime(
            state=state,
            runtime=flat_runtime,
            applied_trade_keys=[],
        )
    return runtime


def _generate_competition_inventory_grid_plan(
    *,
    state: dict[str, Any],
    trades: list[dict[str, Any]],
    current_position_qty: float,
    bid_price: float,
    ask_price: float,
    step_price: float,
    first_order_multiplier: float,
    per_order_notional: float,
    threshold_position_notional: float,
    max_order_position_notional: float,
    max_position_notional: float,
    tick_size: float | None,
    step_size: float | None,
    min_qty: float | None,
    min_notional: float | None,
) -> dict[str, Any]:
    order_refs = state.get("inventory_grid_order_refs")
    runtime = _resolve_competition_inventory_grid_runtime(
        state=state,
        trades=trades,
        order_refs=order_refs if isinstance(order_refs, dict) else {},
        step_price=step_price,
        current_position_qty=max(float(current_position_qty), 0.0),
    )
    if _safe_float(runtime.get("grid_anchor_price")) <= 0:
        runtime["grid_anchor_price"] = (max(float(bid_price), 0.0) + max(float(ask_price), 0.0)) / 2.0

    plan = build_inventory_grid_orders(
        runtime=runtime,
        bid_price=bid_price,
        ask_price=ask_price,
        step_price=step_price,
        per_order_notional=per_order_notional,
        first_order_multiplier=first_order_multiplier,
        threshold_position_notional=threshold_position_notional,
        max_order_position_notional=max_order_position_notional,
        max_position_notional=max_position_notional,
        tick_size=tick_size,
        step_size=step_size,
        min_qty=min_qty,
        min_notional=min_notional,
    )
    return {
        "strategy_mode": "competition_inventory_grid",
        **plan,
        "grid_anchor_price": _safe_float(runtime.get("grid_anchor_price")),
        "direction_state": str(runtime.get("direction_state", "flat") or "flat"),
        "pair_credit_steps": int(runtime.get("pair_credit_steps", 0) or 0),
        "recovery_mode": str(runtime.get("recovery_mode", "live") or "live"),
    }


def _planner_namespace(args: argparse.Namespace) -> argparse.Namespace:
    return argparse.Namespace(
        symbol=args.symbol,
        strategy_mode=args.strategy_mode,
        flat_start_enabled=getattr(args, "flat_start_enabled", True),
        warm_start_enabled=getattr(args, "warm_start_enabled", True),
        step_price=args.step_price,
        buy_levels=args.buy_levels,
        sell_levels=args.sell_levels,
        per_order_notional=args.per_order_notional,
        startup_entry_multiplier=getattr(args, "startup_entry_multiplier", 1.0),
        base_position_notional=args.base_position_notional,
        center_price=args.center_price,
        static_buy_offset_steps=getattr(args, "static_buy_offset_steps", 0.0),
        static_sell_offset_steps=getattr(args, "static_sell_offset_steps", 0.0),
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
        first_order_multiplier=getattr(args, "first_order_multiplier", None),
        threshold_position_notional=getattr(args, "threshold_position_notional", None),
        max_position_notional=getattr(args, "max_position_notional", None),
        max_order_position_notional=getattr(args, "max_order_position_notional", None),
        max_short_position_notional=getattr(args, "max_short_position_notional", None),
        inventory_pause_long_probe_scale=getattr(args, "inventory_pause_long_probe_scale", 0.0),
        inventory_pause_short_probe_scale=getattr(args, "inventory_pause_short_probe_scale", 0.25),
        adaptive_step_enabled=getattr(args, "adaptive_step_enabled", False),
        adaptive_step_30s_abs_return_ratio=getattr(args, "adaptive_step_30s_abs_return_ratio", 0.0),
        adaptive_step_30s_amplitude_ratio=getattr(args, "adaptive_step_30s_amplitude_ratio", 0.0),
        adaptive_step_1m_abs_return_ratio=getattr(args, "adaptive_step_1m_abs_return_ratio", 0.0),
        adaptive_step_1m_amplitude_ratio=getattr(args, "adaptive_step_1m_amplitude_ratio", 0.0),
        adaptive_step_3m_abs_return_ratio=getattr(args, "adaptive_step_3m_abs_return_ratio", 0.0),
        adaptive_step_5m_abs_return_ratio=getattr(args, "adaptive_step_5m_abs_return_ratio", 0.0),
        adaptive_step_max_scale=getattr(args, "adaptive_step_max_scale", 1.0),
        adaptive_step_min_per_order_scale=getattr(args, "adaptive_step_min_per_order_scale", 1.0),
        adaptive_step_min_position_limit_scale=getattr(args, "adaptive_step_min_position_limit_scale", 1.0),
        synthetic_trend_follow_enabled=getattr(args, "synthetic_trend_follow_enabled", False),
        synthetic_trend_follow_1m_abs_return_ratio=getattr(args, "synthetic_trend_follow_1m_abs_return_ratio", 0.0),
        synthetic_trend_follow_1m_amplitude_ratio=getattr(args, "synthetic_trend_follow_1m_amplitude_ratio", 0.0),
        synthetic_trend_follow_3m_abs_return_ratio=getattr(args, "synthetic_trend_follow_3m_abs_return_ratio", 0.0),
        synthetic_trend_follow_3m_amplitude_ratio=getattr(args, "synthetic_trend_follow_3m_amplitude_ratio", 0.0),
        synthetic_trend_follow_min_efficiency_ratio=getattr(args, "synthetic_trend_follow_min_efficiency_ratio", 0.0),
        synthetic_trend_follow_reverse_delay_seconds=getattr(args, "synthetic_trend_follow_reverse_delay_seconds", 0.0),
        take_profit_min_profit_ratio=getattr(args, "take_profit_min_profit_ratio", None),
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


def _strategy_client_order_prefix(symbol: str) -> str:
    return f"gx-{symbol.lower().replace('usdt', 'u')}-"


def _flatten_pid_path(symbol: str) -> Path:
    slug = "".join(ch if ch.isalnum() else "_" for ch in str(symbol or "").strip().lower()).strip("_") or "symbol"
    return Path(f"output/{slug}_maker_flatten.pid")


def _flatten_log_path(symbol: str) -> Path:
    slug = "".join(ch if ch.isalnum() else "_" for ch in str(symbol or "").strip().lower()).strip("_") or "symbol"
    return Path(f"output/{slug}_maker_flatten.log")


def _flatten_events_path(symbol: str) -> Path:
    slug = "".join(ch if ch.isalnum() else "_" for ch in str(symbol or "").strip().lower()).strip("_") or "symbol"
    return Path(f"output/{slug}_maker_flatten_events.jsonl")


def _pid_is_running(pid_path: Path) -> bool:
    if not pid_path.exists():
        return False
    try:
        pid = int(pid_path.read_text(encoding="utf-8").strip())
    except (OSError, ValueError):
        pid_path.unlink(missing_ok=True)
        return False
    try:
        os.kill(pid, 0)
    except OSError:
        pid_path.unlink(missing_ok=True)
        return False
    return True


def _start_futures_flatten_process(symbol: str) -> dict[str, Any]:
    pid_path = _flatten_pid_path(symbol)
    if _pid_is_running(pid_path):
        return {"started": False, "already_running": True}
    log_path = _flatten_log_path(symbol)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    src_path = str((Path.cwd() / "src").resolve())
    current_pythonpath = env.get("PYTHONPATH", "").strip()
    env["PYTHONPATH"] = src_path if not current_pythonpath else f"{src_path}{os.pathsep}{current_pythonpath}"
    command = [
        sys.executable,
        "-m",
        "grid_optimizer.maker_flatten_runner",
        "--symbol",
        str(symbol),
        "--client-order-prefix",
        flatten_client_order_prefix(symbol),
        "--sleep-seconds",
        "2.0",
        "--recv-window",
        "5000",
        "--max-consecutive-errors",
        "20",
        "--events-jsonl",
        str(_flatten_events_path(symbol)),
    ]
    with log_path.open("ab") as log_file:
        proc = subprocess.Popen(
            command,
            cwd=str(Path.cwd()),
            env=env,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    pid_path.write_text(str(proc.pid), encoding="utf-8")
    return {"started": True, "already_running": False, "pid": proc.pid}


def _load_futures_runtime_guard_inputs(
    summary_path: Path,
    *,
    runtime_guard_stats_start_time: Any = None,
    symbol: str,
    now: datetime | None = None,
) -> tuple[float, list[dict[str, Any]], datetime | None]:
    return summarize_futures_runtime_guard_inputs(
        summary_path,
        runtime_guard_stats_start_time=runtime_guard_stats_start_time,
        symbol=symbol,
        now=now,
    )


def _cancel_futures_strategy_orders(
    *,
    symbol: str,
    api_key: str,
    api_secret: str,
    recv_window: int,
) -> int:
    strategy_prefix = _strategy_client_order_prefix(symbol)
    flatten_prefix = flatten_client_order_prefix(symbol)
    open_orders = fetch_futures_open_orders(symbol, api_key, api_secret, recv_window=recv_window)
    count = 0
    for order in open_orders:
        if not isinstance(order, dict):
            continue
        client_order_id = str(order.get("clientOrderId", "") or "")
        if not client_order_id.startswith(strategy_prefix) or is_flatten_order(order, flatten_prefix):
            continue
        delete_futures_order(
            symbol=symbol,
            api_key=api_key,
            api_secret=api_secret,
            order_id=int(order["orderId"]) if str(order.get("orderId", "")).strip() else None,
            orig_client_order_id=client_order_id or None,
            recv_window=recv_window,
        )
        count += 1
    return count


def _synthetic_drift_notional(mid_price: float, synthetic_drift_qty: float) -> float:
    return abs(float(synthetic_drift_qty)) * max(float(mid_price), 0.0)


def _build_runtime_guard_stop_summary(
    *,
    args: argparse.Namespace,
    cycle: int,
    cycle_started_at: datetime,
    stats_start_time: datetime | None,
    runtime_guard_config: Any,
    runtime_guard_result: Any,
    canceled_count: int,
    flatten_result: dict[str, Any],
    flatten_snapshot: dict[str, Any],
    metrics: dict[str, Any] | None = None,
) -> dict[str, Any]:
    metric_source = dict(metrics or {})
    live_mid_price = (
        (_safe_float(flatten_snapshot.get("bid_price")) + _safe_float(flatten_snapshot.get("ask_price"))) / 2.0
        if (_safe_float(flatten_snapshot.get("bid_price")) > 0 and _safe_float(flatten_snapshot.get("ask_price")) > 0)
        else 0.0
    )
    mid_price = _safe_float(metric_source.get("mid_price")) or live_mid_price
    current_long_qty = _safe_float(metric_source.get("current_long_qty"))
    if current_long_qty <= 0:
        current_long_qty = _safe_float(flatten_snapshot.get("long_qty"))
    current_short_qty = _safe_float(metric_source.get("current_short_qty"))
    if current_short_qty <= 0:
        current_short_qty = _safe_float(flatten_snapshot.get("short_qty"))
    actual_net_qty = _safe_float(metric_source.get("actual_net_qty"))
    if abs(actual_net_qty) <= 1e-12:
        actual_net_qty = _safe_float(flatten_snapshot.get("net_qty"))
    current_long_notional = _safe_float(metric_source.get("current_long_notional")) or (current_long_qty * mid_price)
    current_short_notional = _safe_float(metric_source.get("current_short_notional")) or (current_short_qty * mid_price)
    actual_net_notional = _safe_float(metric_source.get("actual_net_notional")) or (actual_net_qty * mid_price)
    synthetic_net_qty = _safe_float(metric_source.get("synthetic_net_qty"))
    synthetic_drift_qty = _safe_float(metric_source.get("synthetic_drift_qty"))
    synthetic_drift_notional = _synthetic_drift_notional(mid_price, synthetic_drift_qty)

    return {
        "ts": cycle_started_at.isoformat(),
        "cycle": cycle,
        "symbol": args.symbol.upper().strip(),
        "strategy_profile": str(getattr(args, "strategy_profile", AUTO_REGIME_STABLE_PROFILE)),
        "effective_strategy_profile": str(
            metric_source.get("effective_strategy_profile") or getattr(args, "strategy_profile", AUTO_REGIME_STABLE_PROFILE)
        ),
        "strategy_mode": str(metric_source.get("strategy_mode") or getattr(args, "strategy_mode", "one_way_long")),
        "mid_price": mid_price,
        "center_price": _safe_float(metric_source.get("center_price")),
        "current_long_qty": current_long_qty,
        "current_long_notional": current_long_notional,
        "current_short_qty": current_short_qty,
        "current_short_notional": current_short_notional,
        "actual_net_qty": actual_net_qty,
        "actual_net_notional": actual_net_notional,
        "synthetic_net_qty": synthetic_net_qty,
        "synthetic_drift_qty": synthetic_drift_qty,
        "synthetic_drift_notional": synthetic_drift_notional,
        "synthetic_unmatched_trade_count": int(metric_source.get("synthetic_unmatched_trade_count", 0) or 0),
        "open_order_count": 0,
        "kept_order_count": 0,
        "missing_order_count": 0,
        "stale_order_count": 0,
        "placed_count": 0,
        "canceled_count": canceled_count,
        "buy_paused": False,
        "pause_reasons": [],
        "buy_cap_applied": False,
        "buy_budget_notional": 0.0,
        "planned_buy_notional": 0.0,
        "max_position_notional": _safe_float(metric_source.get("max_position_notional")),
        "short_cap_applied": False,
        "short_budget_notional": 0.0,
        "planned_short_notional": 0.0,
        "max_short_position_notional": _safe_float(metric_source.get("max_short_position_notional")),
        "inventory_tier_active": False,
        "inventory_tier_ratio": 0.0,
        "volatility_buy_pause": False,
        "shift_frozen": False,
        "market_guard_return_ratio": 0.0,
        "market_guard_amplitude_ratio": 0.0,
        "auto_regime_enabled": bool(getattr(args, "auto_regime_enabled", False)),
        "auto_regime_regime": "",
        "auto_regime_reason": None,
        "auto_regime_pending_count": 0,
        "auto_regime_confirm_cycles": 0,
        "auto_regime_switched": False,
        "neutral_hourly_scale_enabled": False,
        "neutral_hourly_scale_ratio": 0.0,
        "neutral_hourly_regime": "",
        "neutral_hourly_reason": None,
        "neutral_hourly_bucket": None,
        "effective_buy_levels": 0,
        "effective_sell_levels": 0,
        "effective_per_order_notional": 0.0,
        "effective_base_position_notional": 0.0,
        "custom_grid_runtime_min_price": 0.0,
        "custom_grid_runtime_max_price": 0.0,
        "custom_grid_roll_enabled": False,
        "custom_grid_roll_checked": False,
        "custom_grid_roll_triggered": False,
        "custom_grid_roll_reason": None,
        "custom_grid_roll_interval_minutes": 0,
        "custom_grid_roll_trade_threshold": 0,
        "custom_grid_roll_upper_distance_ratio": 0.0,
        "custom_grid_roll_shift_levels": 0,
        "custom_grid_roll_current_trade_count": 0,
        "custom_grid_roll_trade_baseline": 0,
        "custom_grid_roll_trades_since_last_roll": 0,
        "custom_grid_roll_levels_above_current": 0,
        "custom_grid_roll_required_levels_above": 0,
        "custom_grid_roll_last_check_bucket": None,
        "custom_grid_roll_last_applied_at": None,
        "custom_grid_roll_last_applied_price": 0.0,
        "custom_grid_roll_old_min_price": 0.0,
        "custom_grid_roll_old_max_price": 0.0,
        "custom_grid_roll_new_min_price": 0.0,
        "custom_grid_roll_new_max_price": 0.0,
        "custom_grid_roll_last_old_min_price": 0.0,
        "custom_grid_roll_last_old_max_price": 0.0,
        "custom_grid_roll_last_new_min_price": 0.0,
        "custom_grid_roll_last_new_max_price": 0.0,
        "fixed_center_roll": {},
        "shift_moves": [],
        "reconcile": {},
        "reconcile_checked": False,
        "reconcile_ok": None,
        "reconcile_message": None,
        "reconcile_interval_cycles": 0,
        "reconcile_expected_open_order_count": 0,
        "reconcile_actual_open_order_count": 0,
        "reconcile_expected_actual_net_qty": 0.0,
        "reconcile_actual_actual_net_qty": 0.0,
        "executed": False,
        "trade_audit_appended": 0,
        "income_audit_appended": 0,
        "audit_error": None,
        "error_message": None,
        "runtime_status": runtime_guard_result.runtime_status,
        "stop_triggered": runtime_guard_result.stop_triggered,
        "stop_reason": runtime_guard_result.primary_reason,
        "stop_reasons": runtime_guard_result.matched_reasons,
        "stop_triggered_at": runtime_guard_result.triggered_at,
        "run_start_time": runtime_guard_config.run_start_time.isoformat() if runtime_guard_config.run_start_time else None,
        "run_end_time": runtime_guard_config.run_end_time.isoformat() if runtime_guard_config.run_end_time else None,
        "runtime_guard_stats_start_time": stats_start_time.isoformat() if stats_start_time else None,
        "rolling_hourly_loss": runtime_guard_result.rolling_hourly_loss,
        "rolling_hourly_loss_limit": runtime_guard_config.rolling_hourly_loss_limit,
        "cumulative_gross_notional": runtime_guard_result.cumulative_gross_notional,
        "max_cumulative_notional": runtime_guard_config.max_cumulative_notional,
        "max_actual_net_notional": runtime_guard_config.max_actual_net_notional,
        "max_synthetic_drift_notional": runtime_guard_config.max_synthetic_drift_notional,
        "flatten_started": bool(flatten_result.get("started")),
        "flatten_already_running": bool(flatten_result.get("already_running")),
    }


def _maybe_handle_runtime_guard(
    *,
    args: argparse.Namespace,
    cycle: int,
    cycle_started_at: datetime,
    summary_path: Path,
) -> dict[str, Any] | None:
    runtime_guard_config = normalize_runtime_guard_config(vars(args))
    if not any(
        (
            runtime_guard_config.run_start_time,
            runtime_guard_config.run_end_time,
            runtime_guard_config.rolling_hourly_loss_limit,
            runtime_guard_config.max_cumulative_notional,
        )
    ):
        return None
    cumulative_gross_notional, pnl_events, stats_start_time = _load_futures_runtime_guard_inputs(
        summary_path,
        runtime_guard_stats_start_time=getattr(args, "runtime_guard_stats_start_time", None),
        symbol=args.symbol.upper().strip(),
        now=cycle_started_at,
    )
    runtime_guard_result = evaluate_runtime_guards(
        config=runtime_guard_config,
        now=cycle_started_at,
        cumulative_gross_notional=cumulative_gross_notional,
        pnl_events=pnl_events,
    )
    if runtime_guard_result.tradable:
        return None
    credentials = load_binance_api_credentials()
    if credentials is None:
        raise RuntimeError("请先在本机环境变量中配置 BINANCE_API_KEY 和 BINANCE_API_SECRET")
    api_key, api_secret = credentials
    canceled_count = _cancel_futures_strategy_orders(
        symbol=args.symbol.upper().strip(),
        api_key=api_key,
        api_secret=api_secret,
        recv_window=args.recv_window,
    )
    flatten_result = {"started": False, "already_running": False}
    flatten_snapshot = load_live_flatten_snapshot(args.symbol.upper().strip(), api_key, api_secret)
    if list(flatten_snapshot.get("orders", [])):
        flatten_result = _start_futures_flatten_process(args.symbol.upper().strip())
    return _build_runtime_guard_stop_summary(
        args=args,
        cycle=cycle,
        cycle_started_at=cycle_started_at,
        stats_start_time=stats_start_time,
        runtime_guard_config=runtime_guard_config,
        runtime_guard_result=runtime_guard_result,
        canceled_count=canceled_count,
        flatten_result=flatten_result,
        flatten_snapshot=flatten_snapshot,
    )


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
    external_short_pause: bool = False,
    external_short_pause_reasons: list[str] | None = None,
    preserve_long_entry_on_inventory_pause: bool = False,
    preserve_short_entry_on_external_pause: bool = False,
    preserve_short_entry_on_inventory_pause: bool = False,
) -> dict[str, Any]:
    current_long_notional = max(current_long_qty, 0.0) * max(mid_price, 0.0)
    current_short_notional = max(current_short_qty, 0.0) * max(mid_price, 0.0)
    long_paused = bool(external_long_pause)
    short_paused = bool(external_short_pause)
    inventory_long_paused = False
    inventory_short_paused = False
    long_reasons: list[str] = list(external_pause_reasons or [])
    short_reasons: list[str] = list(external_short_pause_reasons or [])

    if pause_long_position_notional is not None and current_long_notional >= pause_long_position_notional:
        long_paused = True
        inventory_long_paused = True
        long_reasons.append(
            f"current_long_notional={_float(current_long_notional)} >= pause_long_position_notional={_float(pause_long_position_notional)}"
        )
    if pause_short_position_notional is not None and current_short_notional >= pause_short_position_notional:
        short_paused = True
        inventory_short_paused = True
        short_reasons.append(
            f"current_short_notional={_float(current_short_notional)} >= pause_short_position_notional={_float(pause_short_position_notional)}"
        )
    if min_mid_price_for_buys is not None and mid_price <= min_mid_price_for_buys:
        long_paused = True
        long_reasons.append(
            f"mid_price={_price(mid_price)} <= min_mid_price_for_buys={_price(min_mid_price_for_buys)}"
        )

    if long_paused:
        keep_long_probe = preserve_long_entry_on_inventory_pause if inventory_long_paused else False
        if not keep_long_probe:
            plan["bootstrap_orders"] = [item for item in plan.get("bootstrap_orders", []) if not _is_long_entry_order(item)]
            plan["buy_orders"] = [item for item in plan.get("buy_orders", []) if not _is_long_entry_order(item)]
    if short_paused:
        plan["bootstrap_orders"] = [item for item in plan.get("bootstrap_orders", []) if not _is_short_entry_order(item)]
        keep_short_probe = (
            preserve_short_entry_on_inventory_pause
            if inventory_short_paused
            else preserve_short_entry_on_external_pause
        )
        if not keep_short_probe:
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


def apply_take_profit_profit_guard(
    *,
    plan: dict[str, Any],
    current_long_qty: float,
    current_short_qty: float,
    current_long_avg_price: float,
    current_short_avg_price: float,
    synthetic_long_avg_price: float | None = None,
    synthetic_short_avg_price: float | None = None,
    current_long_notional: float,
    current_short_notional: float,
    pause_long_position_notional: float | None,
    pause_short_position_notional: float | None,
    threshold_long_position_notional: float | None = None,
    threshold_short_position_notional: float | None = None,
    min_profit_ratio: float | None,
    tick_size: float | None,
    bid_price: float,
    ask_price: float,
) -> dict[str, Any]:
    safe_ratio = max(_safe_float(min_profit_ratio), 0.0)
    long_pause_threshold = max(_safe_float(pause_long_position_notional), 0.0)
    short_pause_threshold = max(_safe_float(pause_short_position_notional), 0.0)
    long_threshold_notional = max(_safe_float(threshold_long_position_notional), 0.0)
    short_threshold_notional = max(_safe_float(threshold_short_position_notional), 0.0)
    long_relax_threshold = max(long_pause_threshold, long_threshold_notional)
    short_relax_threshold = max(short_pause_threshold, short_threshold_notional)
    report = {
        "enabled": True,
        "min_profit_ratio": min_profit_ratio,
        "effective_min_profit_ratio": safe_ratio,
        "long_active": False,
        "short_active": False,
        "long_cost_basis_missing": False,
        "short_cost_basis_missing": False,
        "long_floor_price": None,
        "short_ceiling_price": None,
        "long_relax_threshold_notional": long_relax_threshold if long_relax_threshold > 0 else pause_long_position_notional,
        "short_relax_threshold_notional": short_relax_threshold if short_relax_threshold > 0 else pause_short_position_notional,
        "adjusted_sell_orders": 0,
        "adjusted_buy_orders": 0,
        "dropped_sell_orders": 0,
        "dropped_buy_orders": 0,
        "relaxed_sell_orders": 0,
        "relaxed_buy_orders": 0,
        "release_floor_price": None,
        "release_ceiling_price": None,
    }

    actual_long_avg = max(_safe_float(current_long_avg_price), 0.0)
    actual_short_avg = max(_safe_float(current_short_avg_price), 0.0)
    synthetic_long_avg = max(_safe_float(synthetic_long_avg_price), 0.0)
    synthetic_short_avg = max(_safe_float(synthetic_short_avg_price), 0.0)
    long_guard_roles = {"take_profit", "take_profit_long", "active_delever_long"}
    short_guard_roles = {"take_profit_short", "active_delever_short"}

    long_floor_candidates: list[float] = []
    short_ceiling_candidates: list[float] = []
    if actual_long_avg > 0:
        actual_long_floor = _round_order_price(actual_long_avg * (1.0 + safe_ratio), tick_size, "SELL")
        if actual_long_floor > 0:
            long_floor_candidates.append(actual_long_floor)
    if synthetic_long_avg > 0:
        synthetic_long_floor = _round_order_price(synthetic_long_avg * (1.0 + safe_ratio), tick_size, "SELL")
        if synthetic_long_floor > 0:
            long_floor_candidates.append(synthetic_long_floor)
    if actual_short_avg > 0:
        actual_short_ceiling = _round_order_price(actual_short_avg * (1.0 - safe_ratio), tick_size, "BUY")
        if actual_short_ceiling > 0:
            short_ceiling_candidates.append(actual_short_ceiling)
    if synthetic_short_avg > 0:
        synthetic_short_ceiling = _round_order_price(synthetic_short_avg * (1.0 - safe_ratio), tick_size, "BUY")
        if synthetic_short_ceiling > 0:
            short_ceiling_candidates.append(synthetic_short_ceiling)

    long_has_position = max(_safe_float(current_long_qty), 0.0) > 1e-12
    short_has_position = max(_safe_float(current_short_qty), 0.0) > 1e-12
    long_active = long_has_position and bool(long_floor_candidates)
    short_active = short_has_position and bool(short_ceiling_candidates)
    long_cost_basis_missing = long_has_position and not long_floor_candidates
    short_cost_basis_missing = short_has_position and not short_ceiling_candidates
    report["long_active"] = long_active
    report["short_active"] = short_active
    report["long_cost_basis_missing"] = long_cost_basis_missing
    report["short_cost_basis_missing"] = short_cost_basis_missing

    def _drop_missing_cost_orders(
        *,
        order_key: str,
        guarded_roles: set[str],
        counter_key: str,
    ) -> None:
        updated_orders: list[dict[str, Any]] = []
        for item in plan.get(order_key, []):
            if not isinstance(item, dict):
                continue
            order = dict(item)
            if _order_role(order) in guarded_roles:
                report[counter_key] += 1
                continue
            updated_orders.append(order)
        plan[order_key] = updated_orders

    if long_cost_basis_missing:
        _drop_missing_cost_orders(
            order_key="sell_orders",
            guarded_roles=long_guard_roles,
            counter_key="dropped_sell_orders",
        )
    if short_cost_basis_missing:
        _drop_missing_cost_orders(
            order_key="buy_orders",
            guarded_roles=short_guard_roles,
            counter_key="dropped_buy_orders",
        )

    long_floor_price = max(long_floor_candidates) if long_floor_candidates else None
    if long_floor_price and long_floor_price > 0:
        report["long_floor_price"] = long_floor_price

    short_ceiling_price = min(short_ceiling_candidates) if short_ceiling_candidates else None
    if short_ceiling_price and short_ceiling_price > 0:
        report["short_ceiling_price"] = short_ceiling_price

    if long_floor_price and long_floor_price > 0:
        updated_sell_orders: list[dict[str, Any]] = []
        for item in plan.get("sell_orders", []):
            if not isinstance(item, dict):
                continue
            order = dict(item)
            if _order_role(order) not in long_guard_roles:
                updated_sell_orders.append(order)
                continue
            release_floor = max(_safe_float(order.get("take_profit_guard_release_floor")), 0.0)
            effective_floor = long_floor_price
            if release_floor > 0:
                effective_floor = min(effective_floor, release_floor)
                report["relaxed_sell_orders"] += 1
                current_release_floor = _safe_float(report.get("release_floor_price"))
                report["release_floor_price"] = (
                    release_floor
                    if current_release_floor <= 0
                    else min(current_release_floor, release_floor)
                )
            adjusted_price = max(_safe_float(order.get("price")), effective_floor)
            if adjusted_price <= bid_price:
                report["dropped_sell_orders"] += 1
                continue
            if abs(adjusted_price - _safe_float(order.get("price"))) > 1e-12:
                order["price"] = adjusted_price
                order["notional"] = adjusted_price * _safe_float(order.get("qty"))
                report["adjusted_sell_orders"] += 1
            updated_sell_orders.append(order)
        plan["sell_orders"] = updated_sell_orders

    if short_ceiling_price and short_ceiling_price > 0:
        updated_buy_orders: list[dict[str, Any]] = []
        for item in plan.get("buy_orders", []):
            if not isinstance(item, dict):
                continue
            order = dict(item)
            if _order_role(order) not in short_guard_roles:
                updated_buy_orders.append(order)
                continue
            release_ceiling = max(_safe_float(order.get("take_profit_guard_release_ceiling")), 0.0)
            effective_ceiling = short_ceiling_price
            if release_ceiling > 0:
                effective_ceiling = max(effective_ceiling, release_ceiling)
                report["relaxed_buy_orders"] += 1
                current_release_ceiling = _safe_float(report.get("release_ceiling_price"))
                report["release_ceiling_price"] = max(current_release_ceiling, release_ceiling)
            adjusted_price = min(_safe_float(order.get("price")), effective_ceiling)
            allow_cross = bool(order.get("force_reduce_only")) and str(order.get("execution_type", "")).strip().lower() in {
                "aggressive",
                "passive_release",
            }
            if adjusted_price <= 0 or (adjusted_price >= ask_price and not allow_cross):
                report["dropped_buy_orders"] += 1
                continue
            if abs(adjusted_price - _safe_float(order.get("price"))) > 1e-12:
                order["price"] = adjusted_price
                order["notional"] = adjusted_price * _safe_float(order.get("qty"))
                report["adjusted_buy_orders"] += 1
            updated_buy_orders.append(order)
        plan["buy_orders"] = updated_buy_orders

    return report


def apply_synthetic_flow_sleeve(
    *,
    plan: dict[str, Any],
    enabled: bool,
    current_long_notional: float,
    current_short_notional: float,
    current_long_avg_price: float,
    current_short_avg_price: float,
    trigger_notional: float | None,
    sleeve_notional: float,
    levels: int,
    per_order_notional: float,
    order_notional: float | None,
    max_loss_ratio: float | None,
    step_price: float,
    tick_size: float | None,
    step_size: float | None,
    min_qty: float | None,
    min_notional: float | None,
    bid_price: float,
    ask_price: float,
    buy_offset_steps: float = 0.0,
    sell_offset_steps: float = 0.0,
    current_long_lots: list[dict[str, Any]] | None = None,
    current_short_lots: list[dict[str, Any]] | None = None,
    flow_lot_consume_mode: str = "lifo",
) -> dict[str, Any]:
    report = {
        "enabled": bool(enabled),
        "active": False,
        "direction": None,
        "trigger_notional": trigger_notional,
        "sleeve_notional": sleeve_notional,
        "remaining_sleeve_notional": 0.0,
        "order_notional": order_notional,
        "levels": levels,
        "max_loss_ratio": max_loss_ratio,
        "loss_floor_price": None,
        "loss_ceiling_price": None,
        "loss_guard_clamped_order_count": 0,
        "lot_cost_guard_active": False,
        "lot_cost_guard_blocked_order_count": 0,
        "lot_cost_guard_consumed_qty": 0.0,
        "placed_order_count": 0,
        "placed_notional": 0.0,
        "blocked_reason": None,
    }
    if not enabled:
        return report

    safe_sleeve_notional = max(_safe_float(sleeve_notional), 0.0)
    safe_levels = max(int(levels or 0), 0)
    safe_step_price = max(_safe_float(step_price), 0.0)
    safe_per_order_notional = max(_safe_float(order_notional), _safe_float(per_order_notional), 0.0)
    safe_trigger_notional = max(_safe_float(trigger_notional), 0.0)
    safe_max_loss_ratio = max(_safe_float(max_loss_ratio), 0.0)
    if safe_sleeve_notional <= 0 or safe_levels <= 0 or safe_step_price <= 0 or safe_per_order_notional <= 0:
        report["blocked_reason"] = "invalid_config"
        return report
    if safe_trigger_notional <= 0:
        report["blocked_reason"] = "missing_trigger_notional"
        return report

    long_notional = max(_safe_float(current_long_notional), 0.0)
    short_notional = max(_safe_float(current_short_notional), 0.0)
    if long_notional < safe_trigger_notional and short_notional < safe_trigger_notional:
        report["blocked_reason"] = "below_trigger"
        return report

    direction: str
    if long_notional >= short_notional:
        direction = "long_inventory_sell_flow"
        remaining_sleeve_notional = max(safe_sleeve_notional - short_notional, 0.0)
    else:
        direction = "short_inventory_buy_flow"
        remaining_sleeve_notional = max(safe_sleeve_notional - long_notional, 0.0)
    report["direction"] = direction
    report["remaining_sleeve_notional"] = remaining_sleeve_notional
    if remaining_sleeve_notional <= 1e-12:
        report["blocked_reason"] = "sleeve_full"
        return report

    existing_keys = {
        f"{str(item.get('side', '')).upper().strip()}:{_safe_float(item.get('price')):.10f}"
        for key in ("buy_orders", "sell_orders", "bootstrap_orders")
        for item in plan.get(key, [])
        if isinstance(item, dict)
    }
    long_flow_lots = _normalize_synthetic_lots(current_long_lots or [])
    short_flow_lots = _normalize_synthetic_lots(current_short_lots or [])
    normalized_flow_lot_consume_mode = str(flow_lot_consume_mode or "lifo").strip().lower()
    if normalized_flow_lot_consume_mode not in {"fifo", "lifo"}:
        normalized_flow_lot_consume_mode = "lifo"
    long_lot_guard_active = direction == "long_inventory_sell_flow" and bool(long_flow_lots) and safe_max_loss_ratio > 0
    short_lot_guard_active = direction == "short_inventory_buy_flow" and bool(short_flow_lots) and safe_max_loss_ratio > 0
    report["lot_cost_guard_active"] = bool(long_lot_guard_active or short_lot_guard_active)

    def _append_flow_order(*, side: str, price: float, notional_budget: float, role: str, level: int) -> dict[str, Any] | None:
        qty = _round_order_qty(max(notional_budget, 0.0) / price, step_size)
        notional = price * qty
        if qty <= 0:
            return None
        if min_qty is not None and qty < min_qty:
            return None
        if min_notional is not None and notional < min_notional:
            return None
        key = f"{side}:{price:.10f}"
        if key in existing_keys:
            return None
        existing_keys.add(key)
        order = {
            "side": side,
            "price": price,
            "qty": qty,
            "notional": notional,
            "level": level,
            "role": role,
            "position_side": "BOTH",
            "force_reduce_only": True,
            "flow_sleeve": True,
        }
        if side == "BUY":
            plan.setdefault("buy_orders", []).append(order)
        else:
            plan.setdefault("sell_orders", []).append(order)
        return order

    placed_count = 0
    placed_notional = 0.0
    clamped_count = 0
    remaining = remaining_sleeve_notional
    if direction == "long_inventory_sell_flow":
        min_allowed_price = 0.0
        if safe_max_loss_ratio > 0 and current_long_avg_price > 0 and not long_lot_guard_active:
            min_allowed_price = float(current_long_avg_price) * (1.0 - safe_max_loss_ratio)
            floor_price = _round_order_price(min_allowed_price, tick_size, "SELL")
            if floor_price > 0:
                report["loss_floor_price"] = floor_price
        for level in range(1, safe_levels + 1):
            distance_steps = max((float(level) - 1.0) + float(sell_offset_steps), 0.0)
            raw_price = float(Decimal(str(ask_price)) + (Decimal(str(distance_steps)) * Decimal(str(safe_step_price))))
            price = _round_order_price(raw_price, tick_size, "SELL")
            notional_budget = min(safe_per_order_notional, remaining)
            if long_lot_guard_active:
                eligible_qty = _synthetic_flow_eligible_qty_for_price(
                    lots=long_flow_lots,
                    side="SELL",
                    price=price,
                    max_loss_ratio=safe_max_loss_ratio,
                    consume_mode=normalized_flow_lot_consume_mode,
                )
                notional_budget = min(notional_budget, max(eligible_qty, 0.0) * price)
                if notional_budget <= 1e-12:
                    report["lot_cost_guard_blocked_order_count"] += 1
                    continue
            else:
                floor_price = max(_safe_float(report.get("loss_floor_price")), 0.0)
                if floor_price > 0 and price < floor_price:
                    price = floor_price
                    clamped_count += 1
                if min_allowed_price > 0 and price < min_allowed_price - 1e-12:
                    report["blocked_reason"] = "loss_guard"
                    break
            if price <= bid_price:
                continue
            order = _append_flow_order(
                side="SELL",
                price=price,
                notional_budget=notional_budget,
                role="flow_sleeve_long",
                level=900 + level,
            )
            if not order:
                continue
            consumed = _safe_float(order.get("notional"))
            consumed_qty = max(_safe_float(order.get("qty")), 0.0)
            if long_lot_guard_active and consumed_qty > 0:
                long_flow_lots, realized_cost = _consume_synthetic_lots_with_cost(
                    long_flow_lots,
                    consumed_qty,
                    consume_mode=normalized_flow_lot_consume_mode,
                )
                report["lot_cost_guard_consumed_qty"] += consumed_qty
                lot_avg_price = realized_cost / consumed_qty if consumed_qty > 1e-12 else 0.0
                lot_floor = _round_order_price(lot_avg_price * (1.0 - safe_max_loss_ratio), tick_size, "SELL")
                if lot_floor > 0:
                    current_floor = _safe_float(report.get("loss_floor_price"))
                    report["loss_floor_price"] = lot_floor if current_floor <= 0 else min(current_floor, lot_floor)
            placed_count += 1
            placed_notional += consumed
            remaining = max(remaining - consumed, 0.0)
            if remaining <= 1e-12:
                break
    else:
        max_allowed_price = 0.0
        if safe_max_loss_ratio > 0 and current_short_avg_price > 0 and not short_lot_guard_active:
            max_allowed_price = float(current_short_avg_price) * (1.0 + safe_max_loss_ratio)
            ceiling_price = _round_order_price(max_allowed_price, tick_size, "BUY")
            if ceiling_price > 0:
                report["loss_ceiling_price"] = ceiling_price
        for level in range(1, safe_levels + 1):
            distance_steps = max((float(level) - 1.0) + float(buy_offset_steps), 0.0)
            raw_price = float(Decimal(str(bid_price)) - (Decimal(str(distance_steps)) * Decimal(str(safe_step_price))))
            price = _round_order_price(raw_price, tick_size, "BUY")
            notional_budget = min(safe_per_order_notional, remaining)
            if short_lot_guard_active:
                eligible_qty = _synthetic_flow_eligible_qty_for_price(
                    lots=short_flow_lots,
                    side="BUY",
                    price=price,
                    max_loss_ratio=safe_max_loss_ratio,
                    consume_mode=normalized_flow_lot_consume_mode,
                )
                notional_budget = min(notional_budget, max(eligible_qty, 0.0) * price)
                if notional_budget <= 1e-12:
                    report["lot_cost_guard_blocked_order_count"] += 1
                    continue
            else:
                ceiling_price = max(_safe_float(report.get("loss_ceiling_price")), 0.0)
                if ceiling_price > 0 and price > ceiling_price:
                    price = ceiling_price
                    clamped_count += 1
                if max_allowed_price > 0 and price > max_allowed_price + 1e-12:
                    report["blocked_reason"] = "loss_guard"
                    break
            if price <= 0 or price >= ask_price:
                continue
            order = _append_flow_order(
                side="BUY",
                price=price,
                notional_budget=notional_budget,
                role="flow_sleeve_short",
                level=900 + level,
            )
            if not order:
                continue
            consumed = _safe_float(order.get("notional"))
            consumed_qty = max(_safe_float(order.get("qty")), 0.0)
            if short_lot_guard_active and consumed_qty > 0:
                short_flow_lots, realized_cost = _consume_synthetic_lots_with_cost(
                    short_flow_lots,
                    consumed_qty,
                    consume_mode=normalized_flow_lot_consume_mode,
                )
                report["lot_cost_guard_consumed_qty"] += consumed_qty
                lot_avg_price = realized_cost / consumed_qty if consumed_qty > 1e-12 else 0.0
                lot_ceiling = _round_order_price(lot_avg_price * (1.0 + safe_max_loss_ratio), tick_size, "BUY")
                if lot_ceiling > 0:
                    current_ceiling = _safe_float(report.get("loss_ceiling_price"))
                    report["loss_ceiling_price"] = max(current_ceiling, lot_ceiling)
            placed_count += 1
            placed_notional += consumed
            remaining = max(remaining - consumed, 0.0)
            if remaining <= 1e-12:
                break

    report["active"] = placed_count > 0
    report["placed_order_count"] = placed_count
    report["placed_notional"] = placed_notional
    report["loss_guard_clamped_order_count"] = clamped_count
    report["remaining_sleeve_notional"] = max(remaining, 0.0)
    if placed_count > 0:
        report["blocked_reason"] = None
    elif not report.get("blocked_reason"):
        report["blocked_reason"] = "no_valid_order"
    return report


def resolve_short_threshold_timeout_state(
    *,
    state: dict[str, Any],
    current_short_notional: float,
    pause_short_position_notional: float | None,
    threshold_position_notional: float | None,
    hold_seconds: float | None,
    now: datetime | None = None,
) -> dict[str, Any]:
    current_time = now or _utc_now()
    current_notional = max(_safe_float(current_short_notional), 0.0)
    pause_notional = max(_safe_float(pause_short_position_notional), 0.0)
    threshold_notional = max(_safe_float(threshold_position_notional), 0.0)
    safe_hold_seconds = max(_safe_float(hold_seconds), 0.0)
    report = {
        "enabled": False,
        "threshold_notional": threshold_position_notional,
        "target_notional": pause_short_position_notional,
        "hold_seconds": safe_hold_seconds,
        "above_threshold": False,
        "armed": False,
        "timeout_active": False,
        "duration_seconds": 0.0,
        "above_threshold_started_at": None,
        "timeout_active_since": None,
    }
    if threshold_notional <= max(pause_notional, 0.0) or safe_hold_seconds <= 0:
        state.pop("short_threshold_timeout_state", None)
        return report

    report["enabled"] = True

    def _parse_state_ts(value: Any) -> datetime | None:
        raw = str(value or "").strip()
        if not raw:
            return None
        try:
            return datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            return None

    timeout_state = dict(state.get("short_threshold_timeout_state") or {})
    armed_at = _parse_state_ts(timeout_state.get("above_threshold_started_at"))
    active_since = _parse_state_ts(timeout_state.get("timeout_active_since"))
    timeout_active = bool(timeout_state.get("timeout_active"))

    if current_notional <= pause_notional + 1e-12:
        state.pop("short_threshold_timeout_state", None)
        return report

    if timeout_active and current_notional > pause_notional + 1e-12:
        if armed_at is None:
            armed_at = active_since or current_time
        if active_since is None:
            active_since = current_time
        report.update(
            {
                "above_threshold": current_notional >= threshold_notional - 1e-12,
                "armed": True,
                "timeout_active": True,
                "duration_seconds": max((current_time - armed_at).total_seconds(), 0.0),
                "above_threshold_started_at": armed_at.isoformat(),
                "timeout_active_since": active_since.isoformat(),
            }
        )
        state["short_threshold_timeout_state"] = {
            "threshold_notional": threshold_notional,
            "target_notional": pause_notional,
            "hold_seconds": safe_hold_seconds,
            "above_threshold_started_at": armed_at.isoformat(),
            "timeout_active_since": active_since.isoformat(),
            "timeout_active": True,
            "updated_at": current_time.isoformat(),
        }
        return report

    if current_notional >= threshold_notional - 1e-12:
        if armed_at is None:
            armed_at = current_time
        duration_seconds = max((current_time - armed_at).total_seconds(), 0.0)
        timeout_active = duration_seconds >= safe_hold_seconds
        if timeout_active and active_since is None:
            active_since = current_time
        report.update(
            {
                "above_threshold": True,
                "armed": True,
                "timeout_active": timeout_active,
                "duration_seconds": duration_seconds,
                "above_threshold_started_at": armed_at.isoformat(),
                "timeout_active_since": active_since.isoformat() if active_since else None,
            }
        )
        state["short_threshold_timeout_state"] = {
            "threshold_notional": threshold_notional,
            "target_notional": pause_notional,
            "hold_seconds": safe_hold_seconds,
            "above_threshold_started_at": armed_at.isoformat(),
            "timeout_active_since": active_since.isoformat() if active_since else None,
            "timeout_active": timeout_active,
            "updated_at": current_time.isoformat(),
        }
        return report

    state["short_threshold_timeout_state"] = {
        "threshold_notional": threshold_notional,
        "target_notional": pause_notional,
        "hold_seconds": safe_hold_seconds,
        "above_threshold_started_at": None,
        "timeout_active_since": None,
        "timeout_active": False,
        "updated_at": current_time.isoformat(),
    }
    return report


def resolve_inventory_pause_timeout_state(
    *,
    state: dict[str, Any],
    side: str,
    current_notional: float,
    pause_position_notional: float | None,
    hold_seconds: float | None,
    now: datetime | None = None,
) -> dict[str, Any]:
    normalized_side = "short" if str(side).strip().lower() == "short" else "long"
    state_key = f"{normalized_side}_inventory_pause_timeout_state"
    current_time = now or _utc_now()
    safe_current_notional = max(_safe_float(current_notional), 0.0)
    pause_notional = max(_safe_float(pause_position_notional), 0.0)
    safe_hold_seconds = max(_safe_float(hold_seconds), 0.0)
    report = {
        "enabled": False,
        "side": normalized_side,
        "target_notional": pause_position_notional,
        "hold_seconds": safe_hold_seconds,
        "above_pause": False,
        "armed": False,
        "timeout_active": False,
        "duration_seconds": 0.0,
        "above_pause_started_at": None,
        "timeout_active_since": None,
    }
    if pause_notional <= 0 or safe_hold_seconds <= 0:
        state.pop(state_key, None)
        return report

    report["enabled"] = True

    def _parse_state_ts(value: Any) -> datetime | None:
        raw = str(value or "").strip()
        if not raw:
            return None
        try:
            return datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            return None

    timeout_state = dict(state.get(state_key) or {})
    armed_at = _parse_state_ts(timeout_state.get("above_pause_started_at"))
    active_since = _parse_state_ts(timeout_state.get("timeout_active_since"))
    timeout_active = bool(timeout_state.get("timeout_active"))

    if safe_current_notional <= pause_notional + 1e-12:
        state.pop(state_key, None)
        return report

    if armed_at is None:
        armed_at = current_time
    duration_seconds = max((current_time - armed_at).total_seconds(), 0.0)
    if timeout_active:
        active_since = active_since or current_time
    else:
        timeout_active = duration_seconds >= safe_hold_seconds
        if timeout_active and active_since is None:
            active_since = current_time

    report.update(
        {
            "above_pause": True,
            "armed": True,
            "timeout_active": timeout_active,
            "duration_seconds": duration_seconds,
            "above_pause_started_at": armed_at.isoformat(),
            "timeout_active_since": active_since.isoformat() if active_since else None,
        }
    )
    state[state_key] = {
        "target_notional": pause_notional,
        "hold_seconds": safe_hold_seconds,
        "above_pause_started_at": armed_at.isoformat(),
        "timeout_active_since": active_since.isoformat() if active_since else None,
        "timeout_active": timeout_active,
        "updated_at": current_time.isoformat(),
    }
    return report


def _estimate_long_delever_cost(
    *,
    current_long_lots: list[dict[str, Any]],
    current_long_qty: float,
    current_long_avg_price: float,
    sell_orders: list[dict[str, Any]],
    consume_mode: str = "lifo",
) -> float:
    lots = _normalize_synthetic_lots(
        current_long_lots,
        fallback_qty=max(float(current_long_qty), 0.0),
        fallback_price=max(float(current_long_avg_price), 0.0),
    )
    total_cost = 0.0
    for order in sell_orders:
        qty = max(_safe_float(order.get("qty")), 0.0)
        price = max(_safe_float(order.get("price")), 0.0)
        if qty <= 0 or price <= 0:
            continue
        lots, realized_cost = _consume_synthetic_lots_with_cost(lots, qty, consume_mode=consume_mode)
        total_cost += max(realized_cost - (qty * price), 0.0)
    return total_cost


def _estimate_short_delever_cost(
    *,
    current_short_lots: list[dict[str, Any]],
    current_short_qty: float,
    current_short_avg_price: float,
    buy_orders: list[dict[str, Any]],
    consume_mode: str = "lifo",
) -> float:
    lots = _normalize_synthetic_lots(
        current_short_lots,
        fallback_qty=max(float(current_short_qty), 0.0),
        fallback_price=max(float(current_short_avg_price), 0.0),
    )
    total_cost = 0.0
    for order in buy_orders:
        qty = max(_safe_float(order.get("qty")), 0.0)
        price = max(_safe_float(order.get("price")), 0.0)
        if qty <= 0 or price <= 0:
            continue
        lots, realized_cost = _consume_synthetic_lots_with_cost(lots, qty, consume_mode=consume_mode)
        total_cost += max((qty * price) - realized_cost, 0.0)
    return total_cost


def assess_adverse_inventory_reduce(
    *,
    enabled: bool,
    mid_price: float,
    current_long_qty: float,
    current_long_notional: float,
    current_short_qty: float,
    current_short_notional: float,
    current_long_cost_price: float,
    current_short_cost_price: float,
    current_long_cost_basis_source: str | None,
    current_short_cost_basis_source: str | None,
    pause_long_position_notional: float | None,
    pause_short_position_notional: float | None,
    long_trigger_ratio: float | None,
    short_trigger_ratio: float | None,
    target_ratio: float | None = None,
) -> dict[str, Any]:
    report: dict[str, Any] = {
        "enabled": bool(enabled),
        "active": False,
        "direction": None,
        "blocked_reason": None,
        "long_active": False,
        "short_active": False,
        "long_cost_basis_source": current_long_cost_basis_source,
        "short_cost_basis_source": current_short_cost_basis_source,
        "long_cost_price": current_long_cost_price,
        "short_cost_price": current_short_cost_price,
        "long_adverse_ratio": None,
        "short_adverse_ratio": None,
        "long_trigger_ratio": max(_safe_float(long_trigger_ratio), 0.0),
        "short_trigger_ratio": max(_safe_float(short_trigger_ratio), 0.0),
        "target_ratio": max(_safe_float(target_ratio), 0.0),
        "long_activation_mode": None,
        "short_activation_mode": None,
        "maker_timeout_seconds": 0.0,
        "long_elapsed_seconds": 0.0,
        "short_elapsed_seconds": 0.0,
        "long_aggressive": False,
        "short_aggressive": False,
        "long_target_notional": None,
        "short_target_notional": None,
        "long_reduce_notional": 0.0,
        "short_reduce_notional": 0.0,
        "placed_reduce_orders": 0,
        "forced_reduce_orders": [],
    }
    if not enabled:
        return report

    safe_mid_price = max(_safe_float(mid_price), 0.0)
    long_pause = max(_safe_float(pause_long_position_notional), 0.0)
    short_pause = max(_safe_float(pause_short_position_notional), 0.0)
    safe_target_ratio = min(max(_safe_float(target_ratio), 0.0), 0.999999)
    short_probe_floor = short_pause * safe_target_ratio if short_pause > 0 and safe_target_ratio > 0 else 0.0
    safe_long_cost = max(_safe_float(current_long_cost_price), 0.0)
    safe_short_cost = max(_safe_float(current_short_cost_price), 0.0)
    long_ratio = ((safe_long_cost - safe_mid_price) / safe_long_cost) if safe_long_cost > 0 and safe_mid_price > 0 else None
    short_ratio = ((safe_mid_price - safe_short_cost) / safe_short_cost) if safe_short_cost > 0 and safe_mid_price > 0 else None
    report["long_adverse_ratio"] = long_ratio
    report["short_adverse_ratio"] = short_ratio
    blocked_reasons: list[str] = []

    if (
        max(_safe_float(current_long_qty), 0.0) > 1e-12
        and long_pause > 0
        and max(_safe_float(current_long_notional), 0.0) > long_pause
    ):
        if safe_long_cost <= 0:
            blocked_reasons.append("long_cost_basis_missing")
        elif report["long_trigger_ratio"] <= 0:
            blocked_reasons.append("long_trigger_ratio_disabled")
        elif long_ratio is not None and long_ratio >= report["long_trigger_ratio"]:
            report["long_active"] = True
            report["long_activation_mode"] = "pause"
        else:
            blocked_reasons.append("long_adverse_ratio_below_trigger")

    if (
        max(_safe_float(current_short_qty), 0.0) > 1e-12
        and short_pause > 0
        and max(_safe_float(current_short_notional), 0.0) > min(
            value for value in (short_pause, short_probe_floor) if value > 0
        )
    ):
        if safe_short_cost <= 0:
            blocked_reasons.append("short_cost_basis_missing")
        elif report["short_trigger_ratio"] <= 0:
            blocked_reasons.append("short_trigger_ratio_disabled")
        elif short_ratio is not None and short_ratio >= report["short_trigger_ratio"]:
            report["short_active"] = True
            report["short_activation_mode"] = (
                "pause" if max(_safe_float(current_short_notional), 0.0) > short_pause else "below_pause_probe"
            )
        else:
            blocked_reasons.append("short_adverse_ratio_below_trigger")

    report["active"] = bool(report["long_active"] or report["short_active"])
    if report["long_active"] and report["short_active"]:
        report["direction"] = "both"
    elif report["long_active"]:
        report["direction"] = "long"
    elif report["short_active"]:
        report["direction"] = "short"
    elif blocked_reasons:
        report["blocked_reason"] = blocked_reasons[0]
    return report


def resolve_adverse_reduce_probe_scale_override(
    *,
    enabled: bool,
    keep_probe_scale: float | None,
    mid_price: float,
    current_long_qty: float,
    current_long_notional: float,
    current_short_qty: float,
    current_short_notional: float,
    current_long_cost_price: float,
    current_short_cost_price: float,
    pause_long_position_notional: float | None,
    pause_short_position_notional: float | None,
    long_trigger_ratio: float | None,
    short_trigger_ratio: float | None,
) -> float | None:
    if keep_probe_scale is None or not enabled:
        return None
    report = assess_adverse_inventory_reduce(
        enabled=True,
        mid_price=mid_price,
        current_long_qty=current_long_qty,
        current_long_notional=current_long_notional,
        current_short_qty=current_short_qty,
        current_short_notional=current_short_notional,
        current_long_cost_price=current_long_cost_price,
        current_short_cost_price=current_short_cost_price,
        current_long_cost_basis_source="current_avg_price" if _safe_float(current_long_cost_price) > 0 else None,
        current_short_cost_basis_source="current_avg_price" if _safe_float(current_short_cost_price) > 0 else None,
        pause_long_position_notional=pause_long_position_notional,
        pause_short_position_notional=pause_short_position_notional,
        long_trigger_ratio=long_trigger_ratio,
        short_trigger_ratio=short_trigger_ratio,
    )
    if not report.get("short_active"):
        return None
    return min(max(_safe_float(keep_probe_scale), 0.0), 1.0)


def _parse_adverse_reduce_time(value: Any) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.astimezone(timezone.utc) if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _build_adverse_reduce_order(
    *,
    side: str,
    price: float,
    reduce_notional: float,
    available_qty: float,
    tick_size: float | None,
    step_size: float | None,
    min_qty: float | None,
    min_notional: float | None,
    aggressive: bool,
) -> dict[str, Any] | None:
    normalized_side = str(side or "").upper().strip()
    rounded_price = _round_order_price(max(_safe_float(price), 0.0), tick_size, normalized_side)
    if rounded_price is None or rounded_price <= 0:
        return None
    max_qty = max(_safe_float(available_qty), 0.0)
    desired_qty = _round_order_qty(min(max_qty, max(_safe_float(reduce_notional), 0.0) / rounded_price), step_size)
    if desired_qty <= 0 or desired_qty - max(_safe_float(min_qty), 0.0) < -1e-12:
        return None
    notional = desired_qty * rounded_price
    if max(_safe_float(min_notional), 0.0) > 0 and notional + 1e-12 < max(_safe_float(min_notional), 0.0):
        return None
    return {
        "side": normalized_side,
        "price": rounded_price,
        "qty": desired_qty,
        "notional": notional,
        "role": "adverse_reduce_long" if normalized_side == "SELL" else "adverse_reduce_short",
        "position_side": "BOTH",
        "force_reduce_only": True,
        "execution_type": "aggressive" if aggressive else "maker",
        "time_in_force": "GTC" if aggressive else "GTX",
    }


def apply_adverse_inventory_reduce(
    *,
    plan: dict[str, Any],
    state: dict[str, Any],
    report: dict[str, Any],
    now: datetime,
    current_long_qty: float,
    current_long_notional: float,
    current_short_qty: float,
    current_short_notional: float,
    pause_long_position_notional: float | None,
    pause_short_position_notional: float | None,
    target_ratio: float,
    max_order_notional: float | None,
    bid_price: float,
    ask_price: float,
    tick_size: float | None,
    step_size: float | None,
    min_qty: float | None,
    min_notional: float | None,
    maker_timeout_seconds: float = 60.0,
) -> dict[str, Any]:
    state_key = "adverse_inventory_reduce"
    safe_now = now.astimezone(timezone.utc) if now.tzinfo else now.replace(tzinfo=timezone.utc)
    timeout_seconds = max(_safe_float(maker_timeout_seconds), 0.0)
    safe_target_ratio = max(_safe_float(target_ratio), 0.0)
    safe_max_order_notional = max(_safe_float(max_order_notional), 0.0)
    adverse_report = dict(report or {})
    adverse_report.setdefault("enabled", False)
    adverse_report["target_ratio"] = safe_target_ratio
    adverse_report["maker_timeout_seconds"] = timeout_seconds
    adverse_report["placed_reduce_orders"] = 0
    adverse_report["forced_reduce_orders"] = []
    adverse_report["long_elapsed_seconds"] = 0.0
    adverse_report["short_elapsed_seconds"] = 0.0
    adverse_report["long_aggressive"] = False
    adverse_report["short_aggressive"] = False
    adverse_report["long_target_notional"] = None
    adverse_report["short_target_notional"] = None
    adverse_report["long_reduce_notional"] = 0.0
    adverse_report["short_reduce_notional"] = 0.0
    plan["forced_reduce_orders"] = []

    if not adverse_report.get("enabled"):
        state.pop(state_key, None)
        return adverse_report

    timeout_state = dict(state.get(state_key) or {})

    def _apply_side(*, side_key: str, current_qty: float, current_notional: float, pause_notional: float | None) -> None:
        normalized_key = side_key.strip().lower()
        is_long = normalized_key == "long"
        if not adverse_report.get(f"{normalized_key}_active"):
            timeout_state[f"{normalized_key}_entered_at"] = None
            return

        entered_at = _parse_adverse_reduce_time(timeout_state.get(f"{normalized_key}_entered_at")) or safe_now
        elapsed = max((safe_now - entered_at).total_seconds(), 0.0)
        aggressive = timeout_seconds > 0 and elapsed >= timeout_seconds
        timeout_state[f"{normalized_key}_entered_at"] = _isoformat(entered_at)
        target_notional = max(_safe_float(pause_notional), 0.0) * safe_target_ratio
        reduce_notional = max(_safe_float(current_notional) - target_notional, 0.0)
        if safe_max_order_notional > 0:
            reduce_notional = min(reduce_notional, safe_max_order_notional)
        order = _build_adverse_reduce_order(
            side="SELL" if is_long else "BUY",
            price=(bid_price if aggressive else ask_price) if is_long else (ask_price if aggressive else bid_price),
            reduce_notional=reduce_notional,
            available_qty=current_qty,
            tick_size=tick_size,
            step_size=step_size,
            min_qty=min_qty,
            min_notional=min_notional,
            aggressive=aggressive,
        )
        adverse_report[f"{normalized_key}_elapsed_seconds"] = elapsed
        adverse_report[f"{normalized_key}_aggressive"] = aggressive
        adverse_report[f"{normalized_key}_target_notional"] = target_notional
        adverse_report[f"{normalized_key}_reduce_notional"] = reduce_notional
        if order is None:
            adverse_report["blocked_reason"] = adverse_report.get("blocked_reason") or f"{normalized_key}_reduce_order_below_min_constraints"
            return
        target_key = "sell_orders" if is_long else "buy_orders"
        plan[target_key] = [order, *list(plan.get(target_key, []))]
        plan["forced_reduce_orders"].append(order)
        adverse_report["forced_reduce_orders"].append(order)
        adverse_report["placed_reduce_orders"] = int(adverse_report.get("placed_reduce_orders") or 0) + 1

    _apply_side(
        side_key="long",
        current_qty=current_long_qty,
        current_notional=current_long_notional,
        pause_notional=pause_long_position_notional,
    )
    _apply_side(
        side_key="short",
        current_qty=current_short_qty,
        current_notional=current_short_notional,
        pause_notional=pause_short_position_notional,
    )
    if plan["forced_reduce_orders"]:
        timeout_state["last_checked_at"] = _isoformat(safe_now)
        state[state_key] = timeout_state
    else:
        state.pop(state_key, None)
    return adverse_report


def _project_nearest_long_non_loss_exit_price(
    *,
    current_long_lots: list[dict[str, Any]],
    current_long_qty: float,
    current_long_avg_price: float,
    consume_qty: float,
    consume_mode: str,
    step_price: float,
    tick_size: float | None,
    min_profit_ratio: float | None,
) -> float | None:
    projection = _project_remaining_synthetic_book(
        raw_lots=current_long_lots,
        fallback_qty=current_long_qty,
        fallback_price=current_long_avg_price,
        consume_qty=consume_qty,
        consume_mode=consume_mode,
    )
    remaining_qty = max(_safe_float(projection.get("remaining_qty")), 0.0)
    if remaining_qty <= 1e-12:
        return None

    profit_step = max(
        float(step_price),
        float(tick_size) if tick_size is not None and tick_size > 0 else 0.0,
        1e-12,
    )
    nearest_exit_price = None
    for lot in projection.get("remaining_lots", []):
        if not isinstance(lot, dict):
            continue
        lot_price = max(_safe_float(lot.get("price")), 0.0)
        if lot_price <= 0:
            continue
        candidate_price = _round_order_price(lot_price + profit_step, tick_size, "SELL")
        if candidate_price <= 0:
            continue
        if nearest_exit_price is None or candidate_price < nearest_exit_price:
            nearest_exit_price = candidate_price

    remaining_avg_price = max(_safe_float(projection.get("remaining_avg_price")), 0.0)
    if nearest_exit_price is None and remaining_avg_price > 0:
        nearest_exit_price = _round_order_price(remaining_avg_price + profit_step, tick_size, "SELL")

    safe_ratio = max(_safe_float(min_profit_ratio), 0.0)
    if safe_ratio > 0 and remaining_avg_price > 0:
        guard_floor = _round_order_price(remaining_avg_price * (1.0 + safe_ratio), tick_size, "SELL")
        if guard_floor > 0:
            nearest_exit_price = guard_floor if nearest_exit_price is None else max(nearest_exit_price, guard_floor)

    return nearest_exit_price if nearest_exit_price is not None and nearest_exit_price > 0 else None


def _project_nearest_short_non_loss_exit_price(
    *,
    current_short_lots: list[dict[str, Any]],
    current_short_qty: float,
    current_short_avg_price: float,
    consume_qty: float,
    consume_mode: str,
    step_price: float,
    tick_size: float | None,
    min_profit_ratio: float | None,
) -> float | None:
    projection = _project_remaining_synthetic_book(
        raw_lots=current_short_lots,
        fallback_qty=current_short_qty,
        fallback_price=current_short_avg_price,
        consume_qty=consume_qty,
        consume_mode=consume_mode,
    )
    remaining_qty = max(_safe_float(projection.get("remaining_qty")), 0.0)
    if remaining_qty <= 1e-12:
        return None

    profit_step = max(
        float(step_price),
        float(tick_size) if tick_size is not None and tick_size > 0 else 0.0,
        1e-12,
    )
    nearest_exit_price = None
    for lot in projection.get("remaining_lots", []):
        if not isinstance(lot, dict):
            continue
        lot_price = max(_safe_float(lot.get("price")), 0.0)
        if lot_price <= 0:
            continue
        candidate_price = _round_order_price(max(lot_price - profit_step, 0.0), tick_size, "BUY")
        if candidate_price <= 0:
            continue
        if nearest_exit_price is None or candidate_price > nearest_exit_price:
            nearest_exit_price = candidate_price

    remaining_avg_price = max(_safe_float(projection.get("remaining_avg_price")), 0.0)
    if nearest_exit_price is None and remaining_avg_price > 0:
        nearest_exit_price = _round_order_price(max(remaining_avg_price - profit_step, 0.0), tick_size, "BUY")

    safe_ratio = max(_safe_float(min_profit_ratio), 0.0)
    if safe_ratio > 0 and remaining_avg_price > 0:
        guard_ceiling = _round_order_price(remaining_avg_price * (1.0 - safe_ratio), tick_size, "BUY")
        if guard_ceiling > 0:
            nearest_exit_price = guard_ceiling if nearest_exit_price is None else min(nearest_exit_price, guard_ceiling)

    return nearest_exit_price if nearest_exit_price is not None and nearest_exit_price > 0 else None


def _resolve_near_market_release_price(
    *,
    side: str,
    level_index: int,
    bid_price: float,
    ask_price: float,
    step_price: float,
    tick_size: float | None,
) -> float:
    safe_side = str(side or "").upper().strip()
    safe_level_index = max(int(level_index or 1), 1)
    safe_bid = max(_safe_float(bid_price), 0.0)
    safe_ask = max(_safe_float(ask_price), 0.0)
    safe_step = max(_safe_float(step_price), 0.0)
    safe_tick = max(_safe_float(tick_size), 0.0)
    distance = max(float(safe_level_index - 1), 0.0) * safe_step

    if safe_side == "BUY":
        raw_price = max(safe_bid - distance, 0.0)
        price = _round_order_price(raw_price, tick_size, "BUY")
        if safe_ask > 0 and price >= safe_ask:
            fallback = max(safe_ask - safe_tick, 0.0) if safe_tick > 0 else min(safe_bid, safe_ask)
            price = _round_order_price(max(fallback, 0.0), tick_size, "BUY")
        return max(_safe_float(price), 0.0)

    raw_price = safe_ask + distance
    price = _round_order_price(raw_price, tick_size, "SELL")
    if safe_bid > 0 and price <= safe_bid:
        fallback = safe_bid + safe_tick if safe_tick > 0 else max(safe_bid, safe_ask)
        price = _round_order_price(max(fallback, 0.0), tick_size, "SELL")
    return max(_safe_float(price), 0.0)


def _build_near_market_release_seed_orders(
    *,
    side: str,
    role: str,
    current_qty: float,
    current_notional: float,
    per_order_notional: float,
    max_levels: int,
    step_price: float,
    tick_size: float | None,
    step_size: float | None,
    min_qty: float | None,
    min_notional: float | None,
    bid_price: float,
    ask_price: float,
) -> list[dict[str, Any]]:
    safe_qty = max(_safe_float(current_qty), 0.0)
    safe_notional = max(_safe_float(current_notional), 0.0)
    safe_per_order_notional = max(_safe_float(per_order_notional), 0.0)
    safe_levels = max(int(max_levels or 0), 0)
    if safe_qty <= 1e-12 or safe_notional <= 1e-12 or safe_per_order_notional <= 0 or safe_levels <= 0:
        return []

    orders: list[dict[str, Any]] = []
    remaining_qty = safe_qty
    remaining_notional = safe_notional
    for level in range(1, safe_levels + 1):
        price = _resolve_near_market_release_price(
            side=side,
            level_index=level,
            bid_price=bid_price,
            ask_price=ask_price,
            step_price=step_price,
            tick_size=tick_size,
        )
        if price <= 0:
            continue
        notional_budget = min(safe_per_order_notional, remaining_notional)
        qty = _round_order_qty(min(remaining_qty, notional_budget / price), step_size)
        notional = price * qty
        if qty <= 0:
            continue
        if min_qty is not None and qty < min_qty:
            continue
        if min_notional is not None and notional < min_notional:
            continue
        orders.append(
            {
                "side": side,
                "price": price,
                "qty": qty,
                "notional": notional,
                "level": level,
                "role": role,
                "position_side": "BOTH",
                "force_reduce_only": True,
                "synthetic_release_source": True,
            }
        )
        remaining_qty = max(remaining_qty - qty, 0.0)
        remaining_notional = max(remaining_notional - notional, 0.0)
        if remaining_qty <= 1e-12 or remaining_notional <= 1e-12:
            break
    return orders


def apply_active_delever_long(
    *,
    plan: dict[str, Any],
    current_long_qty: float,
    current_long_notional: float,
    current_long_avg_price: float,
    current_long_lots: list[dict[str, Any]],
    pause_long_position_notional: float | None,
    threshold_position_notional: float | None,
    per_order_notional: float,
    step_price: float,
    tick_size: float | None,
    bid_price: float,
    ask_price: float,
    min_profit_ratio: float | None,
    market_guard_buy_pause_active: bool,
    grid_buffer_realized_notional: float,
    grid_buffer_spent_notional: float,
    step_size: float | None = None,
    min_qty: float | None = None,
    min_notional: float | None = None,
    max_active_levels: int = 3,
    inventory_pause_timeout_active: bool = False,
    timeout_target_notional: float | None = None,
    force_release_active: bool = False,
    force_release_target_notional: float | None = None,
    force_release_trigger_mode: str = "force_release",
) -> dict[str, Any]:
    available_buffer = max(float(grid_buffer_realized_notional) - float(grid_buffer_spent_notional), 0.0)
    report = {
        "enabled": max_active_levels > 0,
        "active": False,
        "trigger_mode": None,
        "available_buffer_notional": available_buffer,
        "estimated_cost_notional": 0.0,
        "active_sell_order_count": 0,
        "active_buy_order_count": 0,
        "pause_long_position_notional": pause_long_position_notional,
        "pause_short_position_notional": None,
        "release_sell_order_count": 0,
        "release_floor_price": None,
        "inventory_pause_timeout_active": bool(inventory_pause_timeout_active),
        "inventory_pause_timeout_target_notional": timeout_target_notional,
        "inventory_pause_timeout_aggressive": False,
        "synthesized_release_source_order_count": 0,
        "pruned_flow_sleeve_order_count": 0,
        "force_release_active": bool(force_release_active),
        "force_release_target_notional": force_release_target_notional,
    }
    threshold_notional = max(_safe_float(threshold_position_notional), 0.0)
    pause_notional = max(_safe_float(pause_long_position_notional), 0.0)
    timeout_target = max(_safe_float(timeout_target_notional), 0.0)
    pause_trigger_active = pause_notional > 0 and current_long_notional >= pause_notional - 1e-12
    inventory_pause_timeout_trigger_active = (
        bool(inventory_pause_timeout_active) and timeout_target > 0 and current_long_notional > timeout_target
    )
    force_target_notional = max(_safe_float(force_release_target_notional), 0.0)
    force_release_trigger_active = bool(force_release_active) and (
        force_target_notional <= 0 or current_long_notional > force_target_notional + 1e-12
    )
    threshold_enabled = threshold_notional > max(pause_notional, 0.0)
    if (
        max_active_levels <= 0
        or current_long_qty <= 1e-12
        or step_price <= 0
    ):
        return report
    if (
        not threshold_enabled
        and pause_notional <= 0
        and not inventory_pause_timeout_trigger_active
        and not force_release_trigger_active
    ):
        return report
    if threshold_enabled:
        if (
            current_long_notional < threshold_notional
            and current_long_notional < pause_notional
            and not inventory_pause_timeout_trigger_active
            and not force_release_trigger_active
        ):
            return report
    elif current_long_notional < pause_notional and not inventory_pause_timeout_trigger_active and not force_release_trigger_active:
        return report

    take_profit_orders = [
        dict(item)
        for item in plan.get("sell_orders", [])
        if isinstance(item, dict) and _order_role(item) == "take_profit_long"
    ]
    if not take_profit_orders and (
        pause_trigger_active
        or inventory_pause_timeout_trigger_active
        or force_release_trigger_active
    ):
        take_profit_orders = _build_near_market_release_seed_orders(
            side="SELL",
            role="take_profit_long",
            current_qty=current_long_qty,
            current_notional=current_long_notional,
            per_order_notional=per_order_notional,
            max_levels=max_active_levels,
            step_price=step_price,
            tick_size=tick_size,
            step_size=step_size,
            min_qty=min_qty,
            min_notional=min_notional,
            bid_price=bid_price,
            ask_price=ask_price,
        )
        report["synthesized_release_source_order_count"] = len(take_profit_orders)
    if not take_profit_orders:
        return report
    take_profit_orders.sort(key=lambda item: _safe_float(item.get("price")))
    other_sell_orders = [
        dict(item)
        for item in plan.get("sell_orders", [])
        if isinstance(item, dict) and _order_role(item) != "take_profit_long"
    ]

    requested_levels = min(max_active_levels, len(take_profit_orders))
    if requested_levels <= 0:
        return report

    active_count = 0
    estimated_cost = 0.0
    trigger_mode: str | None = None
    projected_exit_price = None
    if threshold_enabled and current_long_notional >= threshold_notional:
        threshold_consume_mode = "fifo"
        safe_per_order_notional = max(float(per_order_notional), 1e-12)
        excess_notional = max(current_long_notional - threshold_notional, 0.0)
        active_count = min(len(take_profit_orders), max(1, int(math.ceil(excess_notional / safe_per_order_notional))))
        estimated_cost = _estimate_long_delever_cost(
            current_long_lots=current_long_lots,
            current_long_qty=current_long_qty,
            current_long_avg_price=current_long_avg_price,
            sell_orders=take_profit_orders[:active_count],
            consume_mode=threshold_consume_mode,
        )
        target_active_count = max(active_count, requested_levels)
        projected_exit_price = _project_nearest_long_non_loss_exit_price(
            current_long_lots=current_long_lots,
            current_long_qty=current_long_qty,
            current_long_avg_price=current_long_avg_price,
            consume_qty=sum(max(_safe_float(item.get("qty")), 0.0) for item in take_profit_orders[:active_count]),
            consume_mode=threshold_consume_mode,
            step_price=step_price,
            tick_size=tick_size,
            min_profit_ratio=min_profit_ratio,
        )
        while active_count < target_active_count:
            if projected_exit_price is None or projected_exit_price <= float(ask_price) + float(step_price) + 1e-12:
                break
            next_active_count = active_count + 1
            next_projected_exit_price = _project_nearest_long_non_loss_exit_price(
                current_long_lots=current_long_lots,
                current_long_qty=current_long_qty,
                current_long_avg_price=current_long_avg_price,
                consume_qty=sum(max(_safe_float(item.get("qty")), 0.0) for item in take_profit_orders[:next_active_count]),
                consume_mode=threshold_consume_mode,
                step_price=step_price,
                tick_size=tick_size,
                min_profit_ratio=min_profit_ratio,
            )
            if next_projected_exit_price is None:
                active_count = next_active_count
                estimated_cost = _estimate_long_delever_cost(
                    current_long_lots=current_long_lots,
                    current_long_qty=current_long_qty,
                    current_long_avg_price=current_long_avg_price,
                    sell_orders=take_profit_orders[:active_count],
                    consume_mode=threshold_consume_mode,
                )
                break
            if projected_exit_price - next_projected_exit_price <= 1e-12:
                break
            active_count = next_active_count
            projected_exit_price = next_projected_exit_price
            estimated_cost = _estimate_long_delever_cost(
                current_long_lots=current_long_lots,
                current_long_qty=current_long_qty,
                current_long_avg_price=current_long_avg_price,
                sell_orders=take_profit_orders[:active_count],
                consume_mode=threshold_consume_mode,
            )
        trigger_mode = "threshold"
    else:
        safe_per_order_notional = max(float(per_order_notional), 1e-12)
        if force_release_trigger_active:
            excess_notional = max(current_long_notional - force_target_notional, 0.0)
            active_count = min(
                len(take_profit_orders),
                requested_levels,
                max(1, int(math.ceil(max(excess_notional, safe_per_order_notional) / safe_per_order_notional))),
            )
            estimated_cost = _estimate_long_delever_cost(
                current_long_lots=current_long_lots,
                current_long_qty=current_long_qty,
                current_long_avg_price=current_long_avg_price,
                sell_orders=take_profit_orders[:active_count],
                consume_mode="fifo",
            )
            trigger_mode = str(force_release_trigger_mode or "force_release").strip() or "force_release"
        elif inventory_pause_timeout_trigger_active:
            excess_notional = max(current_long_notional - timeout_target, 0.0)
            active_count = min(
                len(take_profit_orders),
                requested_levels,
                max(1, int(math.ceil(max(excess_notional, safe_per_order_notional) / safe_per_order_notional))),
            )
            estimated_cost = _estimate_long_delever_cost(
                current_long_lots=current_long_lots,
                current_long_qty=current_long_qty,
                current_long_avg_price=current_long_avg_price,
                sell_orders=take_profit_orders[:active_count],
                consume_mode="fifo",
            )
            trigger_mode = "pause_timeout"
        elif pause_trigger_active:
            excess_notional = max(current_long_notional - pause_notional, 0.0)
            active_count = min(
                len(take_profit_orders),
                requested_levels,
                max(1, int(math.ceil(max(excess_notional, safe_per_order_notional) / safe_per_order_notional))),
            )
            estimated_cost = _estimate_long_delever_cost(
                current_long_lots=current_long_lots,
                current_long_qty=current_long_qty,
                current_long_avg_price=current_long_avg_price,
                sell_orders=take_profit_orders[:active_count],
                consume_mode="fifo",
            )
            trigger_mode = "pause"
        elif market_guard_buy_pause_active:
            active_count = requested_levels
            estimated_cost = _estimate_long_delever_cost(
                current_long_lots=current_long_lots,
                current_long_qty=current_long_qty,
                current_long_avg_price=current_long_avg_price,
                sell_orders=take_profit_orders[:active_count],
            )
            trigger_mode = "market_guard"
        else:
            for level_count in range(1, requested_levels + 1):
                level_cost = _estimate_long_delever_cost(
                    current_long_lots=current_long_lots,
                    current_long_qty=current_long_qty,
                    current_long_avg_price=current_long_avg_price,
                    sell_orders=take_profit_orders[:level_count],
                )
                if available_buffer + 1e-12 >= level_cost:
                    active_count = level_count
                    estimated_cost = level_cost
            if active_count > 0:
                trigger_mode = "buffer"

    if active_count <= 0:
        return report

    shift_distance = float(step_price) * float(active_count)
    shifted_take_profit_orders: list[dict[str, Any]] = []
    shift_source_orders = [item for item in take_profit_orders if not bool(item.get("synthetic_release_source"))]
    for index, order in enumerate(shift_source_orders, start=1):
        shifted = dict(order)
        shifted_price = _round_order_price(_safe_float(order.get("price")) + shift_distance, tick_size, "SELL")
        shifted["price"] = shifted_price
        shifted["notional"] = shifted_price * _safe_float(shifted.get("qty"))
        shifted["level"] = int(order.get("level", index) or index) + active_count
        shifted_take_profit_orders.append(shifted)

    active_orders: list[dict[str, Any]] = []
    release_active = (
        trigger_mode in {"threshold", "pause"}
        and pause_notional > 0
        and current_long_notional >= pause_notional - 1e-12
    ) or (
        force_release_trigger_active
        and bool(trigger_mode)
    )
    release_floor_price: float | None = None
    release_order_count = 0
    pruned_flow_sleeve_count = 0
    for index, order in enumerate(take_profit_orders[:active_count], start=1):
        active = dict(order)
        active["role"] = "active_delever_long"
        active["level"] = index
        if trigger_mode in {"threshold", "pause_timeout", "pause"}:
            active["lot_consume_mode"] = "fifo"
        if trigger_mode == "pause_timeout":
            release_price = _resolve_near_market_release_price(
                side="SELL",
                level_index=index,
                bid_price=bid_price,
                ask_price=ask_price,
                step_price=step_price,
                tick_size=tick_size,
            )
            if release_price > 0:
                active["price"] = release_price
                active["notional"] = release_price * _safe_float(active.get("qty"))
                active["take_profit_guard_release_floor"] = release_price
            active["execution_type"] = "maker_timeout_release"
            active["time_in_force"] = "GTX"
            active["force_reduce_only"] = True
            release_floor_price = (
                _safe_float(active.get("price"))
                if release_floor_price is None
                else min(release_floor_price, _safe_float(active.get("price")))
            )
            release_order_count += 1
        elif release_active:
            release_price = _resolve_near_market_release_price(
                side="SELL",
                level_index=index,
                bid_price=bid_price,
                ask_price=ask_price,
                step_price=step_price,
                tick_size=tick_size,
            )
            if release_price > 0:
                active["price"] = release_price
                active["notional"] = release_price * _safe_float(active.get("qty"))
                active["execution_type"] = "passive_release"
                active["time_in_force"] = "GTX"
                active["force_reduce_only"] = True
                active["take_profit_guard_release_floor"] = release_price
                release_floor_price = (
                    release_price
                    if release_floor_price is None
                    else min(release_floor_price, release_price)
                )
                release_order_count += 1
        active_orders.append(active)

    if release_active or trigger_mode == "pause_timeout":
        original_other_count = len(other_sell_orders)
        other_sell_orders = [item for item in other_sell_orders if _order_role(item) != "flow_sleeve_long"]
        pruned_flow_sleeve_count = original_other_count - len(other_sell_orders)

    plan["sell_orders"] = sorted(
        [*active_orders, *shifted_take_profit_orders, *other_sell_orders],
        key=lambda item: (_safe_float(item.get("price")), str(item.get("side", "")).upper().strip()),
    )
    report.update(
        {
            "active": True,
            "trigger_mode": trigger_mode,
            "estimated_cost_notional": estimated_cost,
            "active_sell_order_count": active_count,
            "release_sell_order_count": release_order_count,
            "release_floor_price": release_floor_price,
            "inventory_pause_timeout_aggressive": trigger_mode == "pause_timeout",
            "pruned_flow_sleeve_order_count": pruned_flow_sleeve_count,
        }
    )
    return report


def apply_active_delever_short(
    *,
    plan: dict[str, Any],
    current_short_qty: float,
    current_short_notional: float,
    current_short_avg_price: float,
    current_short_lots: list[dict[str, Any]],
    pause_short_position_notional: float | None,
    threshold_position_notional: float | None,
    per_order_notional: float,
    step_price: float,
    tick_size: float | None,
    bid_price: float,
    ask_price: float | None,
    min_profit_ratio: float | None,
    grid_buffer_realized_notional: float,
    grid_buffer_spent_notional: float,
    step_size: float | None = None,
    min_qty: float | None = None,
    min_notional: float | None = None,
    max_active_levels: int = 3,
    threshold_timeout_active: bool = False,
    timeout_target_notional: float | None = None,
    inventory_pause_timeout_active: bool = False,
    inventory_pause_timeout_target_notional: float | None = None,
    force_release_active: bool = False,
    force_release_target_notional: float | None = None,
    force_release_trigger_mode: str = "force_release",
) -> dict[str, Any]:
    available_buffer = max(float(grid_buffer_realized_notional) - float(grid_buffer_spent_notional), 0.0)
    report = {
        "enabled": max_active_levels > 0,
        "active": False,
        "trigger_mode": None,
        "available_buffer_notional": available_buffer,
        "estimated_cost_notional": 0.0,
        "active_sell_order_count": 0,
        "active_buy_order_count": 0,
        "pause_long_position_notional": None,
        "pause_short_position_notional": pause_short_position_notional,
        "threshold_timeout_active": bool(threshold_timeout_active),
        "threshold_timeout_target_notional": timeout_target_notional,
        "threshold_timeout_aggressive": False,
        "inventory_pause_timeout_active": bool(inventory_pause_timeout_active),
        "inventory_pause_timeout_target_notional": inventory_pause_timeout_target_notional,
        "inventory_pause_timeout_aggressive": False,
        "release_buy_order_count": 0,
        "release_ceiling_price": None,
        "synthesized_release_source_order_count": 0,
        "pruned_flow_sleeve_order_count": 0,
        "force_release_active": bool(force_release_active),
        "force_release_target_notional": force_release_target_notional,
    }
    threshold_notional = max(_safe_float(threshold_position_notional), 0.0)
    pause_notional = max(_safe_float(pause_short_position_notional), 0.0)
    timeout_target = max(_safe_float(timeout_target_notional), 0.0)
    inventory_timeout_target = max(_safe_float(inventory_pause_timeout_target_notional), 0.0)
    force_target_notional = max(_safe_float(force_release_target_notional), 0.0)
    force_release_trigger_active = bool(force_release_active) and (
        force_target_notional <= 0 or current_short_notional > force_target_notional + 1e-12
    )
    pause_trigger_active = pause_notional > 0 and current_short_notional >= pause_notional - 1e-12
    threshold_timeout_trigger_active = bool(threshold_timeout_active) and timeout_target > 0 and current_short_notional > timeout_target
    inventory_pause_timeout_trigger_active = (
        bool(inventory_pause_timeout_active)
        and inventory_timeout_target > 0
        and current_short_notional > inventory_timeout_target
    )
    threshold_trigger_active = threshold_notional > max(pause_notional, 0.0) and current_short_notional >= threshold_notional
    threshold_enabled = threshold_notional > max(pause_notional, 0.0)
    if (
        max_active_levels <= 0
        or current_short_qty <= 1e-12
        or step_price <= 0
    ):
        return report
    if (
        not threshold_enabled
        and pause_notional <= 0
        and not inventory_pause_timeout_trigger_active
        and not force_release_trigger_active
    ):
        return report
    if threshold_enabled:
        if (
            current_short_notional < threshold_notional
            and current_short_notional < pause_notional
            and not inventory_pause_timeout_trigger_active
            and not force_release_trigger_active
        ):
            return report
    elif current_short_notional < pause_notional and not inventory_pause_timeout_trigger_active and not force_release_trigger_active:
        return report

    take_profit_orders = [
        dict(item)
        for item in plan.get("buy_orders", [])
        if isinstance(item, dict) and _order_role(item) == "take_profit_short"
    ]
    if not take_profit_orders and (
        pause_trigger_active
        or threshold_timeout_trigger_active
        or threshold_trigger_active
        or inventory_pause_timeout_trigger_active
        or force_release_trigger_active
    ):
        take_profit_orders = _build_near_market_release_seed_orders(
            side="BUY",
            role="take_profit_short",
            current_qty=current_short_qty,
            current_notional=current_short_notional,
            per_order_notional=per_order_notional,
            max_levels=max_active_levels,
            step_price=step_price,
            tick_size=tick_size,
            step_size=step_size,
            min_qty=min_qty,
            min_notional=min_notional,
            bid_price=bid_price,
            ask_price=_safe_float(ask_price),
        )
        report["synthesized_release_source_order_count"] = len(take_profit_orders)
    if not take_profit_orders:
        return report
    take_profit_orders.sort(key=lambda item: _safe_float(item.get("price")), reverse=True)
    other_buy_orders = [
        dict(item)
        for item in plan.get("buy_orders", [])
        if isinstance(item, dict) and _order_role(item) != "take_profit_short"
    ]

    requested_levels = min(max_active_levels, len(take_profit_orders))
    if requested_levels <= 0:
        return report

    active_count = 0
    estimated_cost = 0.0
    trigger_mode: str | None = None
    projected_exit_price = None
    if force_release_trigger_active:
        safe_per_order_notional = max(float(per_order_notional), 1e-12)
        excess_notional = max(current_short_notional - force_target_notional, 0.0)
        active_count = min(
            len(take_profit_orders),
            requested_levels,
            max(1, int(math.ceil(max(excess_notional, safe_per_order_notional) / safe_per_order_notional))),
        )
        estimated_cost = _estimate_short_delever_cost(
            current_short_lots=current_short_lots,
            current_short_qty=current_short_qty,
            current_short_avg_price=current_short_avg_price,
            buy_orders=take_profit_orders[:active_count],
            consume_mode="fifo",
        )
        trigger_mode = str(force_release_trigger_mode or "force_release").strip() or "force_release"
    elif threshold_timeout_trigger_active:
        safe_per_order_notional = max(float(per_order_notional), 1e-12)
        excess_notional = max(current_short_notional - timeout_target, 0.0)
        active_count = min(len(take_profit_orders), requested_levels, max(1, int(math.ceil(excess_notional / safe_per_order_notional))))
        estimated_cost = _estimate_short_delever_cost(
            current_short_lots=current_short_lots,
            current_short_qty=current_short_qty,
            current_short_avg_price=current_short_avg_price,
            buy_orders=take_profit_orders[:active_count],
            consume_mode="fifo",
        )
        trigger_mode = "threshold_timeout"
    elif inventory_pause_timeout_trigger_active:
        safe_per_order_notional = max(float(per_order_notional), 1e-12)
        excess_notional = max(current_short_notional - inventory_timeout_target, 0.0)
        active_count = min(
            len(take_profit_orders),
            requested_levels,
            max(1, int(math.ceil(max(excess_notional, safe_per_order_notional) / safe_per_order_notional))),
        )
        estimated_cost = _estimate_short_delever_cost(
            current_short_lots=current_short_lots,
            current_short_qty=current_short_qty,
            current_short_avg_price=current_short_avg_price,
            buy_orders=take_profit_orders[:active_count],
            consume_mode="fifo",
        )
        trigger_mode = "pause_timeout"
    elif threshold_enabled and current_short_notional >= threshold_notional:
        threshold_consume_mode = "fifo"
        safe_per_order_notional = max(float(per_order_notional), 1e-12)
        excess_notional = max(current_short_notional - threshold_notional, 0.0)
        active_count = min(len(take_profit_orders), max(1, int(math.ceil(excess_notional / safe_per_order_notional))))
        estimated_cost = _estimate_short_delever_cost(
            current_short_lots=current_short_lots,
            current_short_qty=current_short_qty,
            current_short_avg_price=current_short_avg_price,
            buy_orders=take_profit_orders[:active_count],
            consume_mode=threshold_consume_mode,
        )
        target_active_count = max(active_count, requested_levels)
        projected_exit_price = _project_nearest_short_non_loss_exit_price(
            current_short_lots=current_short_lots,
            current_short_qty=current_short_qty,
            current_short_avg_price=current_short_avg_price,
            consume_qty=sum(max(_safe_float(item.get("qty")), 0.0) for item in take_profit_orders[:active_count]),
            consume_mode=threshold_consume_mode,
            step_price=step_price,
            tick_size=tick_size,
            min_profit_ratio=min_profit_ratio,
        )
        while active_count < target_active_count:
            if projected_exit_price is None or projected_exit_price >= max(float(bid_price) - float(step_price), 0.0) - 1e-12:
                break
            next_active_count = active_count + 1
            next_projected_exit_price = _project_nearest_short_non_loss_exit_price(
                current_short_lots=current_short_lots,
                current_short_qty=current_short_qty,
                current_short_avg_price=current_short_avg_price,
                consume_qty=sum(max(_safe_float(item.get("qty")), 0.0) for item in take_profit_orders[:next_active_count]),
                consume_mode=threshold_consume_mode,
                step_price=step_price,
                tick_size=tick_size,
                min_profit_ratio=min_profit_ratio,
            )
            if next_projected_exit_price is None:
                active_count = next_active_count
                estimated_cost = _estimate_short_delever_cost(
                    current_short_lots=current_short_lots,
                    current_short_qty=current_short_qty,
                    current_short_avg_price=current_short_avg_price,
                    buy_orders=take_profit_orders[:active_count],
                    consume_mode=threshold_consume_mode,
                )
                break
            if next_projected_exit_price - projected_exit_price <= 1e-12:
                break
            active_count = next_active_count
            projected_exit_price = next_projected_exit_price
            estimated_cost = _estimate_short_delever_cost(
                current_short_lots=current_short_lots,
                current_short_qty=current_short_qty,
                current_short_avg_price=current_short_avg_price,
                buy_orders=take_profit_orders[:active_count],
                consume_mode=threshold_consume_mode,
            )
        trigger_mode = "threshold"
    else:
        safe_per_order_notional = max(float(per_order_notional), 1e-12)
        if pause_trigger_active:
            excess_notional = max(current_short_notional - pause_notional, 0.0)
            active_count = min(
                len(take_profit_orders),
                requested_levels,
                max(1, int(math.ceil(max(excess_notional, safe_per_order_notional) / safe_per_order_notional))),
            )
            estimated_cost = _estimate_short_delever_cost(
                current_short_lots=current_short_lots,
                current_short_qty=current_short_qty,
                current_short_avg_price=current_short_avg_price,
                buy_orders=take_profit_orders[:active_count],
                consume_mode="fifo",
            )
            trigger_mode = "pause"
        else:
            for level_count in range(1, requested_levels + 1):
                level_cost = _estimate_short_delever_cost(
                    current_short_lots=current_short_lots,
                    current_short_qty=current_short_qty,
                    current_short_avg_price=current_short_avg_price,
                    buy_orders=take_profit_orders[:level_count],
                )
                if available_buffer + 1e-12 >= level_cost:
                    active_count = level_count
                    estimated_cost = level_cost
            if active_count > 0:
                trigger_mode = "buffer"

    if active_count <= 0:
        return report

    shift_distance = float(step_price) * float(active_count)
    shifted_take_profit_orders: list[dict[str, Any]] = []
    shift_source_orders = [item for item in take_profit_orders if not bool(item.get("synthetic_release_source"))]
    for index, order in enumerate(shift_source_orders, start=1):
        shifted = dict(order)
        shifted_price = _round_order_price(_safe_float(order.get("price")) - shift_distance, tick_size, "BUY")
        shifted["price"] = shifted_price
        shifted["notional"] = shifted_price * _safe_float(shifted.get("qty"))
        shifted["level"] = int(order.get("level", index) or index) + active_count
        shifted_take_profit_orders.append(shifted)

    active_orders: list[dict[str, Any]] = []
    release_active = (
        trigger_mode in {"threshold", "pause"}
        and pause_notional > 0
        and current_short_notional >= pause_notional - 1e-12
    ) or (
        force_release_trigger_active
        and bool(trigger_mode)
    )
    release_ceiling_price: float | None = None
    release_order_count = 0
    pruned_flow_sleeve_count = 0
    for index, order in enumerate(take_profit_orders[:active_count], start=1):
        active = dict(order)
        active["role"] = "active_delever_short"
        active["level"] = index
        if trigger_mode in {"threshold", "threshold_timeout", "pause_timeout", "pause"}:
            active["lot_consume_mode"] = "fifo"
        if trigger_mode in {"threshold_timeout", "pause_timeout"}:
            release_price = _resolve_near_market_release_price(
                side="BUY",
                level_index=index,
                bid_price=bid_price,
                ask_price=_safe_float(ask_price),
                step_price=step_price,
                tick_size=tick_size,
            )
            if release_price > 0:
                active["price"] = release_price
                active["notional"] = release_price * _safe_float(active.get("qty"))
                active["take_profit_guard_release_ceiling"] = release_price
            active["execution_type"] = "maker_timeout_release"
            active["time_in_force"] = "GTX"
            active["force_reduce_only"] = True
            release_ceiling_price = (
                _safe_float(active.get("price"))
                if release_ceiling_price is None
                else max(release_ceiling_price, _safe_float(active.get("price")))
            )
            release_order_count += 1
        elif release_active:
            release_price = _resolve_near_market_release_price(
                side="BUY",
                level_index=index,
                bid_price=bid_price,
                ask_price=_safe_float(ask_price),
                step_price=step_price,
                tick_size=tick_size,
            )
            if release_price > 0:
                active["price"] = release_price
                active["notional"] = release_price * _safe_float(active.get("qty"))
                active["execution_type"] = "passive_release"
                active["time_in_force"] = "GTX"
                active["force_reduce_only"] = True
                active["take_profit_guard_release_ceiling"] = release_price
                release_ceiling_price = (
                    release_price
                    if release_ceiling_price is None
                    else max(release_ceiling_price, release_price)
                )
                release_order_count += 1
        active_orders.append(active)

    if release_active or trigger_mode in {"threshold_timeout", "pause_timeout"}:
        original_other_count = len(other_buy_orders)
        other_buy_orders = [item for item in other_buy_orders if _order_role(item) != "flow_sleeve_short"]
        pruned_flow_sleeve_count = original_other_count - len(other_buy_orders)

    plan["buy_orders"] = sorted(
        [*active_orders, *shifted_take_profit_orders, *other_buy_orders],
        key=lambda item: (-_safe_float(item.get("price")), str(item.get("side", "")).upper().strip()),
    )
    report.update(
        {
            "active": True,
            "trigger_mode": trigger_mode,
            "estimated_cost_notional": estimated_cost,
            "active_buy_order_count": active_count,
            "threshold_timeout_aggressive": trigger_mode == "threshold_timeout",
            "inventory_pause_timeout_aggressive": trigger_mode == "pause_timeout",
            "release_buy_order_count": release_order_count,
            "release_ceiling_price": release_ceiling_price,
            "pruned_flow_sleeve_order_count": pruned_flow_sleeve_count,
        }
    )
    return report


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
        "window_1m": None,
        "window_3m": None,
        "closed_candle_count": 0,
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
            "closed_candle_count": len(closed_candles),
            "candle": {
                "open_time": candle.open_time.isoformat(),
                "close_time": candle.close_time.isoformat(),
                "open": open_price,
                "high": high_price,
                "low": low_price,
                "close": close_price,
            },
            "window_1m": {
                "open_time": candle.open_time.isoformat(),
                "close_time": candle.close_time.isoformat(),
                "open": open_price,
                "high": high_price,
                "low": low_price,
                "close": close_price,
                "return_ratio": return_ratio,
                "amplitude_ratio": amplitude_ratio,
            },
            "window_3m": _aggregate_recent_window(closed_candles, count=3),
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


def resolve_synthetic_trend_follow(
    *,
    state: dict[str, Any],
    market_guard: dict[str, Any] | None,
    now: datetime,
    current_long_qty: float,
    current_short_qty: float,
    enabled: bool,
    window_1m_abs_return_ratio: float | None,
    window_1m_amplitude_ratio: float | None,
    window_3m_abs_return_ratio: float | None,
    window_3m_amplitude_ratio: float | None,
    min_efficiency_ratio: float | None,
    reverse_delay_seconds: float | None,
) -> dict[str, Any]:
    tracker = dict(state.get("synthetic_trend_follow_state") or {})
    now_ms = int(now.timestamp() * 1000)
    tolerance = 1e-9
    safe_min_efficiency_ratio = max(min(_safe_float(min_efficiency_ratio), 1.0), 0.0)
    safe_reverse_delay_seconds = max(_safe_float(reverse_delay_seconds), 0.0)
    safe_window_1m_abs_return_ratio = max(_safe_float(window_1m_abs_return_ratio), 0.0)
    safe_window_1m_amplitude_ratio = max(_safe_float(window_1m_amplitude_ratio), 0.0)
    safe_window_3m_abs_return_ratio = max(_safe_float(window_3m_abs_return_ratio), 0.0)
    safe_window_3m_amplitude_ratio = max(_safe_float(window_3m_amplitude_ratio), 0.0)

    current_inventory_direction: str | None = None
    current_inventory_qty = 0.0
    if float(current_short_qty) > tolerance and float(current_long_qty) <= tolerance:
        current_inventory_direction = "short"
        current_inventory_qty = float(current_short_qty)
    elif float(current_long_qty) > tolerance and float(current_short_qty) <= tolerance:
        current_inventory_direction = "long"
        current_inventory_qty = float(current_long_qty)

    report = {
        "enabled": bool(enabled),
        "available": False,
        "active": False,
        "direction": None,
        "reason": "disabled" if not enabled else "insufficient_market_guard_data",
        "mode": "disabled" if not enabled else "inactive",
        "window_1m": None,
        "window_3m": None,
        "window_1m_efficiency_ratio": 0.0,
        "window_3m_efficiency_ratio": 0.0,
        "min_efficiency_ratio": safe_min_efficiency_ratio,
        "reverse_delay_seconds": safe_reverse_delay_seconds,
        "reverse_delay_active": False,
        "reverse_delay_remaining_seconds": 0.0,
        "cooldown_until": None,
        "flow_entry_side": None,
        "flow_inventory_direction": None,
        "current_inventory_direction": current_inventory_direction,
        "current_inventory_qty": current_inventory_qty,
    }

    if not enabled:
        state["synthetic_trend_follow_state"] = {
            "enabled": False,
            "inventory_direction": current_inventory_direction,
            "inventory_qty": current_inventory_qty,
            "cooldown_until_ms": 0,
            "last_trade_time_ms": int(((state.get("synthetic_ledger") or {}).get("last_trade_time_ms")) or 0),
            "updated_at": now.isoformat(),
        }
        return report

    window_1m = (market_guard or {}).get("window_1m")
    window_3m = (market_guard or {}).get("window_3m")
    report["window_1m"] = dict(window_1m) if isinstance(window_1m, dict) else None
    report["window_3m"] = dict(window_3m) if isinstance(window_3m, dict) else None
    available = isinstance(window_1m, dict) and isinstance(window_3m, dict)
    report["available"] = available
    if not available:
        state["synthetic_trend_follow_state"] = {
            "enabled": True,
            "inventory_direction": current_inventory_direction,
            "inventory_qty": current_inventory_qty,
            "cooldown_until_ms": 0,
            "last_trade_time_ms": int(((state.get("synthetic_ledger") or {}).get("last_trade_time_ms")) or 0),
            "updated_at": now.isoformat(),
        }
        return report

    one_min_return_ratio = _safe_float(window_1m.get("return_ratio"))
    one_min_amplitude_ratio = max(_safe_float(window_1m.get("amplitude_ratio")), 0.0)
    three_min_return_ratio = _safe_float(window_3m.get("return_ratio"))
    three_min_amplitude_ratio = max(_safe_float(window_3m.get("amplitude_ratio")), 0.0)
    one_min_efficiency = _window_directional_efficiency(window_1m)
    three_min_efficiency = _window_directional_efficiency(window_3m)
    report["window_1m_efficiency_ratio"] = one_min_efficiency
    report["window_3m_efficiency_ratio"] = three_min_efficiency

    one_min_direction = _direction_from_return_ratio(one_min_return_ratio)
    three_min_direction = _direction_from_return_ratio(three_min_return_ratio)
    active = (
        one_min_direction is not None
        and one_min_direction == three_min_direction
        and abs(one_min_return_ratio) >= safe_window_1m_abs_return_ratio
        and one_min_amplitude_ratio >= safe_window_1m_amplitude_ratio
        and abs(three_min_return_ratio) >= safe_window_3m_abs_return_ratio
        and three_min_amplitude_ratio >= safe_window_3m_amplitude_ratio
        and one_min_efficiency >= safe_min_efficiency_ratio
        and three_min_efficiency >= safe_min_efficiency_ratio
    )
    direction = one_min_direction if active else None
    flow_entry_side = "SELL" if direction == "up" else "BUY" if direction == "down" else None
    flow_inventory_direction = "short" if direction == "up" else "long" if direction == "down" else None
    report["active"] = active
    report["direction"] = direction
    report["flow_entry_side"] = flow_entry_side
    report["flow_inventory_direction"] = flow_inventory_direction

    last_trade_time_ms = int(((state.get("synthetic_ledger") or {}).get("last_trade_time_ms")) or 0)
    previous_inventory_direction = str(tracker.get("inventory_direction") or "").strip() or None
    previous_inventory_qty = max(_safe_float(tracker.get("inventory_qty")), 0.0)
    previous_trade_time_ms = int(tracker.get("last_trade_time_ms") or 0)
    cooldown_until_ms = int(tracker.get("cooldown_until_ms") or 0)

    if not active or current_inventory_direction is None or current_inventory_direction != flow_inventory_direction:
        cooldown_until_ms = 0
    elif safe_reverse_delay_seconds > 0:
        entry_fill_detected = (
            current_inventory_direction != previous_inventory_direction
            or current_inventory_qty > previous_inventory_qty + tolerance
            or last_trade_time_ms > previous_trade_time_ms
        )
        if entry_fill_detected:
            base_trade_time_ms = last_trade_time_ms if last_trade_time_ms > 0 else now_ms
            cooldown_until_ms = max(base_trade_time_ms, now_ms) + int(safe_reverse_delay_seconds * 1000)

    reverse_delay_active = cooldown_until_ms > now_ms
    reverse_delay_remaining_seconds = max((cooldown_until_ms - now_ms) / 1000.0, 0.0) if reverse_delay_active else 0.0
    report["reverse_delay_active"] = reverse_delay_active
    report["reverse_delay_remaining_seconds"] = reverse_delay_remaining_seconds
    report["cooldown_until"] = (
        datetime.fromtimestamp(cooldown_until_ms / 1000.0, tz=timezone.utc).isoformat()
        if cooldown_until_ms > 0
        else None
    )

    if active:
        if current_inventory_direction is None:
            report["mode"] = "flat_follow_short" if flow_inventory_direction == "short" else "flat_follow_long"
            report["reason"] = (
                f"{direction}_trend flat_only_follow flow={flow_entry_side.lower()} "
                f"1m_ret={one_min_return_ratio * 100:.3f}% "
                f"3m_ret={three_min_return_ratio * 100:.3f}%"
            )
        elif current_inventory_direction == "short":
            report["mode"] = "hold_short_wait" if reverse_delay_active else "hold_short_exit"
            report["reason"] = (
                f"{direction}_trend short_inventory delay={reverse_delay_remaining_seconds:.1f}s"
                if reverse_delay_active
                else f"{direction}_trend short_inventory exit_only"
            )
        else:
            report["mode"] = "hold_long_wait" if reverse_delay_active else "hold_long_exit"
            report["reason"] = (
                f"{direction}_trend long_inventory delay={reverse_delay_remaining_seconds:.1f}s"
                if reverse_delay_active
                else f"{direction}_trend long_inventory exit_only"
            )
    else:
        report["reason"] = (
            "trend_threshold_not_met "
            f"1m_ret={one_min_return_ratio * 100:.3f}% "
            f"1m_amp={one_min_amplitude_ratio * 100:.3f}% "
            f"3m_ret={three_min_return_ratio * 100:.3f}% "
            f"3m_amp={three_min_amplitude_ratio * 100:.3f}%"
        )

    state["synthetic_trend_follow_state"] = {
        "enabled": True,
        "active": active,
        "direction": direction,
        "mode": report["mode"],
        "inventory_direction": current_inventory_direction,
        "inventory_qty": current_inventory_qty,
        "flow_inventory_direction": flow_inventory_direction,
        "cooldown_until_ms": cooldown_until_ms,
        "last_trade_time_ms": last_trade_time_ms,
        "updated_at": now.isoformat(),
    }
    return report


def apply_synthetic_trend_follow_guard(
    *,
    plan: dict[str, Any],
    trend_follow: dict[str, Any] | None,
) -> dict[str, Any]:
    mode = str((trend_follow or {}).get("mode") or "inactive").strip() or "inactive"
    result = {
        "applied": False,
        "mode": mode,
        "buy_orders_before": len(plan.get("buy_orders", [])),
        "sell_orders_before": len(plan.get("sell_orders", [])),
        "bootstrap_orders_before": len(plan.get("bootstrap_orders", [])),
        "buy_orders_after": len(plan.get("buy_orders", [])),
        "sell_orders_after": len(plan.get("sell_orders", [])),
        "bootstrap_orders_after": len(plan.get("bootstrap_orders", [])),
    }
    if mode in {"disabled", "inactive"}:
        return result

    if mode == "flat_follow_short":
        plan["bootstrap_orders"] = []
        plan["buy_orders"] = []
        plan["sell_orders"] = [item for item in plan.get("sell_orders", []) if _is_short_entry_order(item)]
    elif mode == "flat_follow_long":
        plan["bootstrap_orders"] = []
        plan["sell_orders"] = []
        plan["buy_orders"] = [item for item in plan.get("buy_orders", []) if _is_long_entry_order(item)]
    elif mode == "hold_short_wait":
        plan["bootstrap_orders"] = []
        plan["buy_orders"] = []
        plan["sell_orders"] = []
    elif mode == "hold_short_exit":
        plan["bootstrap_orders"] = []
        plan["sell_orders"] = []
        plan["buy_orders"] = [item for item in plan.get("buy_orders", []) if _is_short_exit_order(item)]
    elif mode == "hold_long_wait":
        plan["bootstrap_orders"] = []
        plan["buy_orders"] = []
        plan["sell_orders"] = []
    elif mode == "hold_long_exit":
        plan["bootstrap_orders"] = []
        plan["buy_orders"] = []
        plan["sell_orders"] = [item for item in plan.get("sell_orders", []) if _is_long_exit_order(item)]

    result["applied"] = True
    result["buy_orders_after"] = len(plan.get("buy_orders", []))
    result["sell_orders_after"] = len(plan.get("sell_orders", []))
    result["bootstrap_orders_after"] = len(plan.get("bootstrap_orders", []))
    return result


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
    end_ms = int(current_time.timestamp() * 1000)
    start_15m_ms = int((current_time - timedelta(minutes=15 * 8)).timestamp() * 1000)
    start_60m_ms = int((current_time - timedelta(hours=8)).timestamp() * 1000)
    normalized_mode = str(strategy_mode or "").strip() or "one_way_long"
    try:
        candles_15m = fetch_futures_klines(
            symbol=symbol,
            interval="15m",
            start_ms=start_15m_ms,
            end_ms=end_ms,
            limit=8,
        )
        candles_60m = fetch_futures_klines(
            symbol=symbol,
            interval="1h",
            start_ms=start_60m_ms,
            end_ms=end_ms,
            limit=8,
        )
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
            amp_15m >= XAUT_ADAPTIVE_REDUCE_ONLY_15M_AMPLITUDE_RATIO
            or amp_60m >= XAUT_ADAPTIVE_REDUCE_ONLY_60M_AMPLITUDE_RATIO
            or ret_15m >= 0.007
            or ret_60m >= 0.012
        )
        defensive = (
            amp_15m >= XAUT_ADAPTIVE_DEFENSIVE_15M_AMPLITUDE_RATIO
            or amp_60m >= XAUT_ADAPTIVE_DEFENSIVE_60M_AMPLITUDE_RATIO
            or ret_15m >= 0.004
            or ret_60m >= 0.008
        )
        normal = (
            amp_15m <= XAUT_ADAPTIVE_NORMAL_15M_AMPLITUDE_RATIO
            and amp_60m <= XAUT_ADAPTIVE_NORMAL_60M_AMPLITUDE_RATIO
            and ret_60m <= 0.003
        )
    else:
        reduce_only = (
            amp_15m >= XAUT_ADAPTIVE_REDUCE_ONLY_15M_AMPLITUDE_RATIO
            or amp_60m >= XAUT_ADAPTIVE_REDUCE_ONLY_60M_AMPLITUDE_RATIO
            or ret_15m <= -0.007
            or ret_60m <= -0.012
        )
        defensive = (
            amp_15m >= XAUT_ADAPTIVE_DEFENSIVE_15M_AMPLITUDE_RATIO
            or amp_60m >= XAUT_ADAPTIVE_DEFENSIVE_60M_AMPLITUDE_RATIO
            or ret_15m <= -0.004
            or ret_60m <= -0.008
        )
        normal = (
            amp_15m <= XAUT_ADAPTIVE_NORMAL_15M_AMPLITUDE_RATIO
            and amp_60m <= XAUT_ADAPTIVE_NORMAL_60M_AMPLITUDE_RATIO
            and ret_60m >= -0.003
        )

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
    regime_metrics: dict[str, Any] | None = None,
    bid_price: float = 0.0,
    ask_price: float = 0.0,
    mid_price: float = 0.0,
    tick_size: float | None = None,
) -> argparse.Namespace:
    effective = argparse.Namespace(**vars(args))
    strategy_mode = str(getattr(args, "strategy_mode", "one_way_long")).strip() or "one_way_long"
    overrides = dict(XAUT_ADAPTIVE_STATE_CONFIGS.get(strategy_mode, {}).get(str(active_state), {}))
    for key, value in overrides.items():
        setattr(effective, key, value)
    scaled_step = resolve_xaut_adaptive_step_price(
        strategy_mode=strategy_mode,
        active_state=active_state,
        regime_metrics=regime_metrics,
        bid_price=bid_price,
        ask_price=ask_price,
        mid_price=mid_price,
        tick_size=tick_size,
    )
    if scaled_step is not None and scaled_step > 0:
        effective.step_price = scaled_step
    return effective


def resolve_xaut_adaptive_step_price(
    *,
    strategy_mode: str,
    active_state: str,
    regime_metrics: dict[str, Any] | None,
    bid_price: float,
    ask_price: float,
    mid_price: float,
    tick_size: float | None,
) -> float | None:
    profile_config = XAUT_ADAPTIVE_STATE_CONFIGS.get(str(strategy_mode).strip() or "one_way_long", {})
    normal_step = _safe_float((profile_config.get(XAUT_ADAPTIVE_STATE_NORMAL) or {}).get("step_price"))
    max_step = _safe_float((profile_config.get(XAUT_ADAPTIVE_STATE_DEFENSIVE) or {}).get("step_price"))
    if normal_step <= 0 or max_step <= 0:
        return None

    safe_tick = max(_safe_float(tick_size), 0.0)
    spread = max(float(ask_price) - float(bid_price), 0.0)
    floor_step = max(
        normal_step * XAUT_ADAPTIVE_MIN_STEP_BASE_RATIO,
        max(float(mid_price), 0.0) * XAUT_ADAPTIVE_MIN_STEP_MID_RATIO,
        safe_tick * XAUT_ADAPTIVE_MIN_STEP_TICKS,
        spread * XAUT_ADAPTIVE_MIN_STEP_SPREADS,
    )

    metrics = dict(regime_metrics or {})
    window_15m = metrics.get("window_15m") if isinstance(metrics.get("window_15m"), dict) else {}
    window_60m = metrics.get("window_60m") if isinstance(metrics.get("window_60m"), dict) else {}
    amp_15m = _safe_float(window_15m.get("amplitude_ratio"))
    amp_60m = _safe_float(window_60m.get("amplitude_ratio"))
    if amp_15m <= 0 and amp_60m <= 0:
        if str(active_state).strip() == XAUT_ADAPTIVE_STATE_REDUCE_ONLY:
            return _round_up_to_step(max_step, safe_tick if safe_tick > 0 else None)
        return None

    calm_ratio = max(
        amp_15m / XAUT_ADAPTIVE_NORMAL_15M_AMPLITUDE_RATIO if XAUT_ADAPTIVE_NORMAL_15M_AMPLITUDE_RATIO > 0 else 0.0,
        amp_60m / XAUT_ADAPTIVE_NORMAL_60M_AMPLITUDE_RATIO if XAUT_ADAPTIVE_NORMAL_60M_AMPLITUDE_RATIO > 0 else 0.0,
    )
    if calm_ratio <= 1.0:
        desired_step = floor_step + (normal_step - floor_step) * _clamp_ratio(calm_ratio)
    else:
        expansion_ratio = max(
            _clamp_ratio(
                (amp_15m - XAUT_ADAPTIVE_NORMAL_15M_AMPLITUDE_RATIO)
                / (XAUT_ADAPTIVE_REDUCE_ONLY_15M_AMPLITUDE_RATIO - XAUT_ADAPTIVE_NORMAL_15M_AMPLITUDE_RATIO)
            )
            if XAUT_ADAPTIVE_REDUCE_ONLY_15M_AMPLITUDE_RATIO > XAUT_ADAPTIVE_NORMAL_15M_AMPLITUDE_RATIO
            else 0.0,
            _clamp_ratio(
                (amp_60m - XAUT_ADAPTIVE_NORMAL_60M_AMPLITUDE_RATIO)
                / (XAUT_ADAPTIVE_REDUCE_ONLY_60M_AMPLITUDE_RATIO - XAUT_ADAPTIVE_NORMAL_60M_AMPLITUDE_RATIO)
            )
            if XAUT_ADAPTIVE_REDUCE_ONLY_60M_AMPLITUDE_RATIO > XAUT_ADAPTIVE_NORMAL_60M_AMPLITUDE_RATIO
            else 0.0,
        )
        desired_step = normal_step + (max_step - normal_step) * expansion_ratio

    if str(active_state).strip() == XAUT_ADAPTIVE_STATE_REDUCE_ONLY:
        desired_step = max(desired_step, max_step)
    return _round_up_to_step(desired_step, safe_tick if safe_tick > 0 else None)


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
    if interval_minutes <= 0:
        raise ValueError("neutral center interval must be > 0")
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


def _parse_state_datetime(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


CENTER_INTERVAL_SHIFT_RATIO = 1.0 / 2.0


def _proportional_center_shift_steps(drift_steps: float, ratio: float = CENTER_INTERVAL_SHIFT_RATIO) -> int:
    abs_drift_steps = abs(float(drift_steps))
    if abs_drift_steps < 1.0:
        return 0
    scaled = int(math.floor((abs_drift_steps * float(ratio)) + 0.5))
    max_without_overshoot = max(int(math.floor(abs_drift_steps)), 1)
    return min(max(scaled, 1), max_without_overshoot)


def resolve_interval_locked_center_price(
    *,
    state: dict[str, Any],
    symbol: str,
    current_center_price: float,
    mid_price: float,
    step_price: float,
    interval_minutes: int,
    tick_size: float | None,
    fast_catchup_trigger_steps: float = 0.0,
    fast_catchup_confirm_cycles: int = 3,
    fast_catchup_shift_steps: int = 1,
    now: datetime | None = None,
) -> tuple[float, dict[str, Any]]:
    current_time = now or _utc_now()
    safe_interval_minutes = max(int(interval_minutes), 1)
    safe_step_price = max(_safe_float(step_price), 0.0)
    safe_fast_catchup_trigger_steps = max(_safe_float(fast_catchup_trigger_steps), 0.0)
    safe_fast_catchup_confirm_cycles = max(int(fast_catchup_confirm_cycles), 1)
    safe_fast_catchup_shift_steps = max(int(fast_catchup_shift_steps), 1)
    center_mid_drift_steps = ((float(mid_price) - float(current_center_price)) / safe_step_price) if safe_step_price > 0 else 0.0
    fast_pending_direction = str(state.get("center_fast_catchup_pending_direction", "") or "")
    fast_pending_count = int(state.get("center_fast_catchup_pending_count", 0) or 0)
    fast_direction = ""
    if safe_fast_catchup_trigger_steps > 0 and safe_step_price > 0:
        if center_mid_drift_steps >= safe_fast_catchup_trigger_steps:
            fast_direction = "up"
        elif center_mid_drift_steps <= -safe_fast_catchup_trigger_steps:
            fast_direction = "down"
    if fast_direction:
        if fast_direction == fast_pending_direction:
            fast_pending_count += 1
        else:
            fast_pending_direction = fast_direction
            fast_pending_count = 1
    else:
        fast_pending_direction = ""
        fast_pending_count = 0

    last_update = (
        _parse_state_datetime(state.get("last_center_update_ts"))
        or _parse_state_datetime(state.get("updated_at"))
        or _parse_state_datetime(state.get("created_at"))
    )
    if last_update is None:
        state["last_center_update_ts"] = _isoformat(current_time)
        state["center_fast_catchup_pending_direction"] = fast_pending_direction
        state["center_fast_catchup_pending_count"] = fast_pending_count
        return current_center_price, {
            "available": True,
            "center_price": current_center_price,
            "interval": f"{safe_interval_minutes}m",
            "reason": "center_lock_initialized",
            "locked": True,
            "elapsed_seconds": 0.0,
            "center_drift_steps": center_mid_drift_steps,
            "fast_catchup_trigger_steps": safe_fast_catchup_trigger_steps,
            "fast_catchup_confirm_cycles": safe_fast_catchup_confirm_cycles,
            "fast_catchup_shift_steps": safe_fast_catchup_shift_steps,
            "fast_catchup_pending_direction": fast_pending_direction,
            "fast_catchup_pending_count": fast_pending_count,
            "next_update_after": _isoformat(current_time + timedelta(minutes=safe_interval_minutes)),
        }

    if not str(state.get("last_center_update_ts") or "").strip():
        state["last_center_update_ts"] = _isoformat(last_update)
    elapsed_seconds = max((current_time - last_update).total_seconds(), 0.0)
    interval_seconds = safe_interval_minutes * 60.0
    if elapsed_seconds < interval_seconds:
        if fast_direction and fast_pending_count >= safe_fast_catchup_confirm_cycles and safe_step_price > 0:
            applied_shift_steps = min(safe_fast_catchup_shift_steps, max(int(math.floor(abs(center_mid_drift_steps))), 1))
            old_center_price = float(current_center_price)
            direction = 1.0 if fast_direction == "up" else -1.0
            new_center_price = _round_to_nearest_step(
                old_center_price + (direction * applied_shift_steps * safe_step_price),
                tick_size,
            )
            state["center_fast_catchup_pending_direction"] = ""
            state["center_fast_catchup_pending_count"] = 0
            if abs(new_center_price - old_center_price) > 1e-12:
                target_center_price = _round_to_nearest_step(float(mid_price), tick_size)
                state["last_center_update_ts"] = _isoformat(current_time)
                state["last_center_old_price"] = old_center_price
                state["last_center_new_price"] = new_center_price
                state["last_center_target_price"] = target_center_price
                state["last_center_shift_steps"] = applied_shift_steps
                state["last_center_roll_reason"] = "center_interval_fast_catchup"
                return new_center_price, {
                    "available": True,
                    "center_price": new_center_price,
                    "target_center_price": target_center_price,
                    "interval": f"{safe_interval_minutes}m",
                    "reason": "center_interval_fast_catchup",
                    "locked": False,
                    "elapsed_seconds": elapsed_seconds,
                    "old_center_price": old_center_price,
                    "center_drift_steps": center_mid_drift_steps,
                    "center_shift_steps": applied_shift_steps,
                    "center_shift_ratio": None,
                    "fast_catchup_trigger_steps": safe_fast_catchup_trigger_steps,
                    "fast_catchup_confirm_cycles": safe_fast_catchup_confirm_cycles,
                    "fast_catchup_shift_steps": safe_fast_catchup_shift_steps,
                    "fast_catchup_pending_direction": "",
                    "fast_catchup_pending_count": 0,
                    "next_update_after": _isoformat(current_time + timedelta(minutes=safe_interval_minutes)),
                }
        state["center_fast_catchup_pending_direction"] = fast_pending_direction
        state["center_fast_catchup_pending_count"] = fast_pending_count
        return current_center_price, {
            "available": True,
            "center_price": current_center_price,
            "interval": f"{safe_interval_minutes}m",
            "reason": "center_interval_locked",
            "locked": True,
            "elapsed_seconds": elapsed_seconds,
            "center_drift_steps": center_mid_drift_steps,
            "fast_catchup_trigger_steps": safe_fast_catchup_trigger_steps,
            "fast_catchup_confirm_cycles": safe_fast_catchup_confirm_cycles,
            "fast_catchup_shift_steps": safe_fast_catchup_shift_steps,
            "fast_catchup_pending_direction": fast_pending_direction,
            "fast_catchup_pending_count": fast_pending_count,
            "next_update_after": _isoformat(last_update + timedelta(minutes=safe_interval_minutes)),
        }

    center_source = determine_interval_center_price(
        symbol=symbol,
        interval_minutes=safe_interval_minutes,
        tick_size=tick_size,
        now=current_time,
    )
    old_center_price = float(current_center_price)
    if center_source.get("available") and center_source.get("center_price") is not None:
        target_center_price = float(center_source["center_price"])
        reason = "center_interval_roll"
    else:
        target_center_price = _round_to_nearest_step(float(mid_price), tick_size)
        reason = "center_interval_roll_mid_fallback"
    center_drift_steps = 0.0
    center_shift_steps = 0
    if safe_step_price > 0:
        center_drift_steps = (target_center_price - old_center_price) / safe_step_price
        center_shift_steps = _proportional_center_shift_steps(center_drift_steps)
        if center_shift_steps > 0:
            direction = 1.0 if center_drift_steps > 0 else -1.0
            new_center_price = _round_to_nearest_step(
                old_center_price + (direction * center_shift_steps * safe_step_price),
                tick_size,
            )
        else:
            new_center_price = old_center_price
    else:
        new_center_price = target_center_price
    state["last_center_update_ts"] = _isoformat(current_time)
    state["last_center_old_price"] = old_center_price
    state["last_center_new_price"] = new_center_price
    state["last_center_target_price"] = target_center_price
    state["last_center_shift_steps"] = center_shift_steps
    state["last_center_roll_reason"] = reason
    state["center_fast_catchup_pending_direction"] = ""
    state["center_fast_catchup_pending_count"] = 0
    center_source.update(
        {
            "available": True,
            "center_price": new_center_price,
            "target_center_price": target_center_price,
            "interval": f"{safe_interval_minutes}m",
            "reason": reason,
            "locked": False,
            "elapsed_seconds": elapsed_seconds,
            "old_center_price": old_center_price,
            "center_drift_steps": center_drift_steps,
            "center_shift_steps": center_shift_steps,
            "center_shift_ratio": CENTER_INTERVAL_SHIFT_RATIO,
            "fast_catchup_trigger_steps": safe_fast_catchup_trigger_steps,
            "fast_catchup_confirm_cycles": safe_fast_catchup_confirm_cycles,
            "fast_catchup_shift_steps": safe_fast_catchup_shift_steps,
            "fast_catchup_pending_direction": "",
            "fast_catchup_pending_count": 0,
            "next_update_after": _isoformat(current_time + timedelta(minutes=safe_interval_minutes)),
        }
    )
    return new_center_price, center_source


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


def resolve_near_market_entry_state(
    *,
    state: dict[str, Any],
    center_distance_steps: float,
    near_market_max_steps: float,
    grid_rebalance_min_steps: float,
    confirm_cycles: int,
) -> dict[str, Any]:
    distance = abs(_safe_float(center_distance_steps))
    near_max = max(_safe_float(near_market_max_steps), 0.0)
    grid_min = max(_safe_float(grid_rebalance_min_steps), near_max)
    safe_confirm_cycles = max(int(confirm_cycles or 1), 1)
    previous = state.get("near_market_entry_state")
    previous_mode = (
        str((previous or {}).get("mode") or "").strip()
        if isinstance(previous, dict)
        else ""
    )
    if previous_mode not in {"near_market", "grid_rebalance"}:
        previous_mode = "near_market" if distance <= near_max + 1e-12 else "grid_rebalance"

    candidate_mode = previous_mode
    reason = "hysteresis_hold"
    if distance <= near_max + 1e-12:
        candidate_mode = "near_market"
        reason = "within_near_market_band"
    elif distance >= grid_min - 1e-12:
        candidate_mode = "grid_rebalance"
        reason = "outside_grid_rebalance_band"

    previous_pending_mode = (
        str((previous or {}).get("pending_mode") or "").strip()
        if isinstance(previous, dict)
        else ""
    )
    previous_pending_count = int(_safe_float((previous or {}).get("pending_count") if isinstance(previous, dict) else 0))
    pending_count = 0
    active_mode = previous_mode
    switched = False
    if candidate_mode == previous_mode:
        pending_mode = None
    else:
        pending_mode = candidate_mode
        pending_count = previous_pending_count + 1 if previous_pending_mode == candidate_mode else 1
        if pending_count >= safe_confirm_cycles:
            active_mode = candidate_mode
            pending_mode = None
            pending_count = 0
            switched = True

    report = {
        "mode": active_mode,
        "near_market_entries_allowed": active_mode == "near_market",
        "center_distance_steps": distance,
        "near_market_entry_max_center_distance_steps": near_max,
        "grid_inventory_rebalance_min_center_distance_steps": grid_min,
        "near_market_reentry_confirm_cycles": safe_confirm_cycles,
        "candidate_mode": candidate_mode,
        "pending_mode": pending_mode,
        "pending_count": pending_count,
        "switched": switched,
        "reason": reason,
    }
    state["near_market_entry_state"] = dict(report)
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


def _resolve_effective_take_profit_min_profit_ratio(
    *,
    strategy_mode: str,
    configured_ratio: float | None,
) -> float | None:
    if configured_ratio is not None:
        return configured_ratio
    if strategy_mode in {"synthetic_neutral", "hedge_neutral"}:
        return 0.0
    return None


def _clamp_ratio(value: float) -> float:
    return min(max(value, 0.0), 1.0)


def _clamp_signed(value: float, limit: float = 1.0) -> float:
    safe_limit = max(float(limit), 0.0)
    if safe_limit <= 0:
        return 0.0
    return min(max(float(value), -safe_limit), safe_limit)


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
    drift_component = _clamp_signed(drift_steps / safe_signal_steps)
    return_component = _clamp_signed(return_steps / safe_signal_steps)
    bias_score = _clamp_signed(
        (drift_component * normalized_drift_weight) + (return_component * normalized_return_weight)
    )
    shift_steps = bias_score * safe_max_shift
    regime = "neutral"
    if bias_score >= 0.15:
        regime = "strong"
    elif bias_score <= -0.15:
        regime = "weak"

    report = {
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
        "drift_weight": normalized_drift_weight,
        "return_weight": normalized_return_weight,
    }
    if not enabled or safe_max_shift <= 0 or safe_step_price <= 0 or safe_mid_price <= 0:
        report.update(
            {
                "active": False,
                "regime": "disabled" if not enabled else "neutral",
                "bias_score": 0.0,
                "shift_steps": 0.0,
                "buy_offset_steps": 0.0,
                "sell_offset_steps": 0.0,
            }
        )
    return report


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
        buy_reasons.append(
            "market_bias_weak_buy_pause "
            f"regime={regime} score={score:+.2f} <= -{buy_threshold:.2f}"
        )
    if short_pause_active:
        short_reasons.append(
            "market_bias_strong_short_pause "
            f"regime={regime} score={score:+.2f} >= +{short_threshold:.2f}"
        )
    return {
        "enabled": bool(buy_pause_enabled or short_pause_enabled),
        "buy_pause_active": buy_pause_active,
        "buy_pause_threshold": buy_threshold,
        "buy_pause_reasons": buy_reasons,
        "short_pause_active": short_pause_active,
        "short_pause_threshold": short_threshold,
        "short_pause_reasons": short_reasons,
        "regime": regime,
        "bias_score": score,
    }


def resolve_market_bias_regime_switch(
    *,
    state: dict[str, Any],
    enabled: bool,
    requested_strategy_mode: str,
    market_bias: dict[str, Any] | None,
    weak_threshold: float,
    strong_threshold: float,
    confirm_cycles: int,
    now: datetime | None = None,
) -> dict[str, Any]:
    requested_mode = str(requested_strategy_mode or "synthetic_neutral").strip() or "synthetic_neutral"
    supported = requested_mode == "synthetic_neutral"
    current_time = now or datetime.now(timezone.utc)
    effective_enabled = bool(enabled and supported)
    switch_state = dict(state.get("market_bias_regime_switch_state") or {})
    active_mode = str(switch_state.get("active_mode") or requested_mode).strip() or requested_mode
    pending_mode = str(switch_state.get("pending_mode") or "").strip() or None
    pending_count = int(switch_state.get("pending_count") or 0)
    safe_confirm_cycles = max(int(confirm_cycles), 1)
    safe_weak_threshold = max(float(weak_threshold), 0.0)
    safe_strong_threshold = max(float(strong_threshold), 0.0)
    score = _safe_float((market_bias or {}).get("bias_score"))
    regime = str(((market_bias or {}).get("regime", "") or "")).strip() or "neutral"
    candidate_mode = requested_mode
    reason = "disabled"
    switched = False

    if effective_enabled:
        if score <= (-safe_weak_threshold):
            candidate_mode = "one_way_short"
            reason = f"weak score={score:+.2f} <= -{safe_weak_threshold:.2f}"
        elif score >= safe_strong_threshold:
            candidate_mode = "one_way_long"
            reason = f"strong score={score:+.2f} >= +{safe_strong_threshold:.2f}"
        else:
            reason = (
                f"neutral score={score:+.2f} within "
                f"[-{safe_weak_threshold:.2f}, +{safe_strong_threshold:.2f}]"
            )
    elif not supported:
        reason = f"unsupported requested_mode={requested_mode}"

    if candidate_mode != active_mode:
        if pending_mode == candidate_mode:
            pending_count += 1
        else:
            pending_mode = candidate_mode
            pending_count = 1
        if pending_count >= safe_confirm_cycles:
            active_mode = candidate_mode
            pending_mode = None
            pending_count = 0
            switched = True
            switch_state["last_switched_at"] = current_time.isoformat()
            switch_state["last_switch_reason"] = reason
    else:
        pending_mode = None
        pending_count = 0

    switch_state.update(
        {
            "enabled": effective_enabled,
            "supported": supported,
            "requested_mode": requested_mode,
            "active_mode": active_mode,
            "candidate_mode": candidate_mode,
            "pending_mode": pending_mode,
            "pending_count": pending_count,
            "confirm_cycles": safe_confirm_cycles,
            "weak_threshold": safe_weak_threshold,
            "strong_threshold": safe_strong_threshold,
            "regime": regime,
            "bias_score": score,
            "reason": reason,
            "switched": switched,
            "updated_at": current_time.isoformat(),
        }
    )
    state["market_bias_regime_switch_state"] = switch_state
    return switch_state


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
    plan_now = _utc_now()
    requested_strategy_mode = str(getattr(args, "strategy_mode", "one_way_long")).strip() or "one_way_long"
    strategy_mode = requested_strategy_mode
    summary_path = Path(args.summary_jsonl)
    if requested_strategy_mode not in {
        "one_way_long",
        "one_way_short",
        "hedge_neutral",
        "synthetic_neutral",
        "inventory_target_neutral",
        "competition_inventory_grid",
    }:
        raise RuntimeError(f"Unsupported strategy_mode: {requested_strategy_mode}")
    requested_strategy_profile = str(getattr(args, "strategy_profile", AUTO_REGIME_STABLE_PROFILE)).strip() or AUTO_REGIME_STABLE_PROFILE
    symbol_info = fetch_futures_symbol_config(symbol)
    market_snapshot = _resolve_runner_market_snapshot(args, symbol=symbol)
    premium = {
        "funding_rate": market_snapshot.get("funding_rate"),
        "mark_price": market_snapshot.get("mark_price"),
        "next_funding_time": market_snapshot.get("next_funding_time"),
        "time": market_snapshot.get("mark_time"),
    }
    bid_price = _safe_float(market_snapshot.get("bid_price"))
    ask_price = _safe_float(market_snapshot.get("ask_price"))
    if bid_price <= 0 or ask_price <= 0:
        raise RuntimeError(f"No valid market snapshot returned for {symbol}")
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
    startup_pending = _startup_pending(state)
    auto_regime: dict[str, Any] | None = None
    xaut_adaptive: dict[str, Any] | None = None
    synthetic_tp_only_watchdog: dict[str, Any] | None = None
    synthetic_inventory_exit_priority: dict[str, Any] | None = None
    synthetic_flow_sleeve: dict[str, Any] = {
        "enabled": bool(getattr(args, "synthetic_flow_sleeve_enabled", False)),
        "active": False,
        "direction": None,
        "trigger_notional": getattr(args, "synthetic_flow_sleeve_trigger_notional", None),
        "sleeve_notional": getattr(args, "synthetic_flow_sleeve_notional", 0.0),
        "remaining_sleeve_notional": 0.0,
        "order_notional": getattr(args, "synthetic_flow_sleeve_order_notional", None),
        "levels": getattr(args, "synthetic_flow_sleeve_levels", 0),
        "max_loss_ratio": getattr(args, "synthetic_flow_sleeve_max_loss_ratio", None),
        "placed_order_count": 0,
        "placed_notional": 0.0,
        "blocked_reason": "not_evaluated",
    }
    volume_long_v4_flow_sleeve: dict[str, Any] = {
        "enabled": bool(getattr(args, "volume_long_v4_flow_sleeve_enabled", False)),
        "active": False,
        "trigger_notional": getattr(args, "volume_long_v4_flow_sleeve_trigger_notional", None),
        "reduce_to_notional": getattr(args, "volume_long_v4_flow_sleeve_reduce_to_notional", None),
        "sleeve_notional": getattr(args, "volume_long_v4_flow_sleeve_notional", 0.0),
        "remaining_sleeve_notional": 0.0,
        "order_notional": getattr(args, "volume_long_v4_flow_sleeve_order_notional", None),
        "levels": getattr(args, "volume_long_v4_flow_sleeve_levels", 0),
        "max_loss_ratio": getattr(args, "volume_long_v4_flow_sleeve_max_loss_ratio", None),
        "cost_basis_price": 0.0,
        "loss_floor_price": None,
        "released_take_profit_order_count": 0,
        "released_take_profit_notional": 0.0,
        "placed_order_count": 0,
        "placed_notional": 0.0,
        "blocked_reason": "not_evaluated",
    }
    exposure_escalation: dict[str, Any] = {
        "enabled": bool(getattr(args, "exposure_escalation_enabled", False)),
        "active": False,
        "reason": None,
        "blocked_reason": "not_evaluated",
        "trigger_notional": getattr(args, "exposure_escalation_notional", None),
        "hold_seconds": getattr(args, "exposure_escalation_hold_seconds", None),
        "target_notional": getattr(args, "exposure_escalation_target_notional", None),
        "max_loss_ratio": getattr(args, "exposure_escalation_max_loss_ratio", None),
        "hard_unrealized_loss_limit": getattr(args, "exposure_escalation_hard_unrealized_loss_limit", None),
        "observed_since": None,
        "held_seconds": 0.0,
        "current_long_notional": 0.0,
        "unrealized_pnl": 0.0,
    }
    adverse_inventory_reduce: dict[str, Any] = {
        "enabled": bool(getattr(args, "adverse_reduce_enabled", False)),
        "active": False,
        "direction": None,
        "blocked_reason": "not_evaluated",
        "placed_reduce_orders": 0,
        "forced_reduce_orders": [],
    }
    effective_args = args
    effective_strategy_profile = requested_strategy_profile
    if is_xaut_adaptive_profile(requested_strategy_profile):
        if symbol != "XAUTUSDT":
            raise RuntimeError("XAUT adaptive profiles require symbol=XAUTUSDT")
        expected_mode = XAUT_ADAPTIVE_PROFILES.get(requested_strategy_profile)
        if expected_mode and requested_strategy_mode != expected_mode:
            raise RuntimeError(f"{requested_strategy_profile} requires strategy_mode={expected_mode}")
        regime_report = assess_xaut_adaptive_regime(
            symbol=symbol,
            strategy_mode=requested_strategy_mode,
        )
        xaut_adaptive = resolve_xaut_adaptive_state(
            state=state,
            regime_report=regime_report,
            strategy_mode=requested_strategy_mode,
        )
        effective_args = build_xaut_adaptive_runner_args(
            args=args,
            active_state=str(xaut_adaptive.get("active_state") or XAUT_ADAPTIVE_STATE_NORMAL),
            regime_metrics=xaut_adaptive.get("metrics") if isinstance(xaut_adaptive.get("metrics"), dict) else None,
            bid_price=bid_price,
            ask_price=ask_price,
            mid_price=mid_price,
            tick_size=_safe_float(symbol_info.get("tick_size")),
        )
        xaut_adaptive["step_price"] = effective_args.step_price
    elif getattr(args, "auto_regime_enabled", False) and requested_strategy_mode == "one_way_long":
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
    adaptive_step = resolve_adaptive_step_price(
        state=state,
        now=plan_now,
        mid_price=mid_price,
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
        effective_args = argparse.Namespace(**vars(effective_args))
        effective_args.step_price = float(adaptive_step["effective_step_price"])
        if adaptive_step.get("effective_per_order_notional") is not None:
            effective_args.per_order_notional = float(adaptive_step["effective_per_order_notional"])
        if adaptive_step.get("effective_base_position_notional") is not None:
            effective_args.base_position_notional = float(adaptive_step["effective_base_position_notional"])
        if adaptive_step.get("effective_inventory_tier_start_notional") is not None:
            effective_args.inventory_tier_start_notional = float(adaptive_step["effective_inventory_tier_start_notional"])
        if adaptive_step.get("effective_inventory_tier_end_notional") is not None:
            effective_args.inventory_tier_end_notional = float(adaptive_step["effective_inventory_tier_end_notional"])
        if adaptive_step.get("effective_inventory_tier_per_order_notional") is not None:
            effective_args.inventory_tier_per_order_notional = float(adaptive_step["effective_inventory_tier_per_order_notional"])
        if adaptive_step.get("effective_inventory_tier_base_position_notional") is not None:
            effective_args.inventory_tier_base_position_notional = float(adaptive_step["effective_inventory_tier_base_position_notional"])
        if adaptive_step.get("effective_pause_buy_position_notional") is not None:
            effective_args.pause_buy_position_notional = float(adaptive_step["effective_pause_buy_position_notional"])
        if adaptive_step.get("effective_pause_short_position_notional") is not None:
            effective_args.pause_short_position_notional = float(adaptive_step["effective_pause_short_position_notional"])
        if adaptive_step.get("effective_max_position_notional") is not None:
            effective_args.max_position_notional = float(adaptive_step["effective_max_position_notional"])
        if adaptive_step.get("effective_max_short_position_notional") is not None:
            effective_args.max_short_position_notional = float(adaptive_step["effective_max_short_position_notional"])
        if adaptive_step.get("effective_max_total_notional") is not None:
            effective_args.max_total_notional = float(adaptive_step["effective_max_total_notional"])

    effective_args = argparse.Namespace(**vars(effective_args))
    effective_args.take_profit_min_profit_ratio = _resolve_effective_take_profit_min_profit_ratio(
        strategy_mode=requested_strategy_mode,
        configured_ratio=getattr(effective_args, "take_profit_min_profit_ratio", None),
    )
    market_guard = assess_market_guard(
        symbol=symbol,
        buy_pause_amp_trigger_ratio=effective_args.buy_pause_amp_trigger_ratio,
        buy_pause_down_return_trigger_ratio=effective_args.buy_pause_down_return_trigger_ratio,
        short_cover_pause_amp_trigger_ratio=getattr(effective_args, "short_cover_pause_amp_trigger_ratio", None),
        short_cover_pause_down_return_trigger_ratio=getattr(effective_args, "short_cover_pause_down_return_trigger_ratio", None),
        freeze_shift_abs_return_trigger_ratio=effective_args.freeze_shift_abs_return_trigger_ratio,
        now=plan_now,
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
    elif _is_inventory_target_neutral_mode(requested_strategy_mode):
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
    prefer_entry_price_cost_basis = _uses_entry_price_cost_basis(effective_strategy_profile)
    actual_cost_basis_price = _position_cost_basis_price(
        actual_position,
        prefer_entry_price=prefer_entry_price_cost_basis,
    )
    actual_break_even_price = max(_safe_float(actual_position.get("breakEvenPrice")), 0.0)
    long_position = extract_symbol_position(account_info, symbol, "LONG" if requested_strategy_mode == "hedge_neutral" else None)
    short_position = extract_symbol_position(account_info, symbol, "SHORT") if requested_strategy_mode == "hedge_neutral" else {}
    if _is_inventory_target_neutral_mode(requested_strategy_mode) or _is_competition_inventory_grid_mode(requested_strategy_mode):
        current_long_qty = max(actual_net_qty, 0.0)
        current_short_qty = max(-actual_net_qty, 0.0)
    elif _is_one_way_short_mode(requested_strategy_mode):
        current_long_qty = 0.0
        current_short_qty = max(-actual_net_qty, 0.0)
    else:
        current_long_qty = max(_position_qty(long_position, position_side="LONG" if requested_strategy_mode == "hedge_neutral" else None), 0.0)
        current_short_qty = max(_position_qty(short_position, position_side="SHORT"), 0.0) if requested_strategy_mode == "hedge_neutral" else 0.0
    current_long_notional = current_long_qty * max(mid_price, 0.0)
    current_short_notional = current_short_qty * max(mid_price, 0.0)
    current_long_avg_price = 0.0
    current_short_avg_price = 0.0
    if requested_strategy_mode == "hedge_neutral":
        current_long_avg_price = (
            _position_cost_basis_price(long_position, prefer_entry_price=prefer_entry_price_cost_basis)
            if current_long_qty > 1e-12
            else 0.0
        )
        current_short_avg_price = (
            _position_cost_basis_price(short_position, prefer_entry_price=prefer_entry_price_cost_basis)
            if current_short_qty > 1e-12
            else 0.0
        )
    else:
        if actual_net_qty > 1e-12:
            current_long_avg_price = max(actual_cost_basis_price, 0.0)
        elif actual_net_qty < -1e-12:
            current_short_avg_price = max(actual_cost_basis_price, 0.0)
    exchange_long_avg_price = current_long_avg_price
    exchange_short_avg_price = current_short_avg_price
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
    take_profit_guard = {
        "enabled": True,
        "min_profit_ratio": getattr(effective_args, "take_profit_min_profit_ratio", None),
        "effective_min_profit_ratio": max(_safe_float(getattr(effective_args, "take_profit_min_profit_ratio", None)), 0.0),
        "long_active": False,
        "short_active": False,
        "long_cost_basis_missing": False,
        "short_cost_basis_missing": False,
        "long_floor_price": None,
        "short_ceiling_price": None,
        "long_relax_threshold_notional": getattr(args, "pause_buy_position_notional", None),
        "short_relax_threshold_notional": getattr(args, "pause_short_position_notional", None),
        "adjusted_sell_orders": 0,
        "adjusted_buy_orders": 0,
        "dropped_sell_orders": 0,
        "dropped_buy_orders": 0,
    }
    volume_long_v4_delever = {
        "enabled": False,
        "active": False,
        "stage": None,
        "pause_long_position_notional": getattr(effective_args, "pause_buy_position_notional", None),
        "hard_long_position_notional": getattr(effective_args, "max_position_notional", None),
        "active_sell_order_count": 0,
        "loss_budget_steps": 0.0,
        "loss_floor_price": None,
    }
    active_delever = {
        "enabled": True,
        "active": False,
        "trigger_mode": None,
        "available_buffer_notional": 0.0,
        "estimated_cost_notional": 0.0,
        "active_sell_order_count": 0,
        "active_buy_order_count": 0,
        "pause_long_position_notional": getattr(args, "pause_buy_position_notional", None),
        "pause_short_position_notional": getattr(args, "pause_short_position_notional", None),
        "threshold_timeout_active": False,
        "threshold_timeout_duration_seconds": 0.0,
        "threshold_timeout_hold_seconds": _safe_float(getattr(args, "short_threshold_timeout_seconds", None)),
        "threshold_timeout_target_notional": getattr(args, "pause_short_position_notional", None),
        "threshold_timeout_aggressive": False,
        "inventory_pause_timeout_active": False,
        "inventory_pause_timeout_duration_seconds": 0.0,
        "inventory_pause_timeout_hold_seconds": _safe_float(getattr(args, "inventory_pause_timeout_seconds", None)),
        "inventory_pause_timeout_target_notional": None,
        "inventory_pause_timeout_aggressive": False,
    }
    long_inventory_pause_timeout = {
        "enabled": False,
        "side": "long",
        "target_notional": getattr(effective_args, "pause_buy_position_notional", None),
        "hold_seconds": _safe_float(getattr(effective_args, "inventory_pause_timeout_seconds", None)),
        "above_pause": False,
        "armed": False,
        "timeout_active": False,
        "duration_seconds": 0.0,
        "above_pause_started_at": None,
        "timeout_active_since": None,
    }
    short_inventory_pause_timeout = {
        "enabled": False,
        "side": "short",
        "target_notional": getattr(effective_args, "pause_short_position_notional", None),
        "hold_seconds": _safe_float(getattr(effective_args, "inventory_pause_timeout_seconds", None)),
        "above_pause": False,
        "armed": False,
        "timeout_active": False,
        "duration_seconds": 0.0,
        "above_pause_started_at": None,
        "timeout_active_since": None,
    }
    short_threshold_timeout = {
        "enabled": False,
        "threshold_notional": getattr(effective_args, "threshold_position_notional", None),
        "target_notional": getattr(effective_args, "pause_short_position_notional", None),
        "hold_seconds": _safe_float(getattr(effective_args, "short_threshold_timeout_seconds", None)),
        "above_threshold": False,
        "armed": False,
        "timeout_active": False,
        "duration_seconds": 0.0,
        "above_threshold_started_at": None,
        "timeout_active_since": None,
    }
    flat_start_guard = {
        "enabled": bool(getattr(args, "flat_start_enabled", True)),
        "blocked": False,
        "mode": strategy_mode,
        "open_order_count": 0,
        "reverse_qty": 0.0,
        "reason": None,
    }
    warm_start = {
        "enabled": bool(getattr(args, "warm_start_enabled", True)),
        "active": False,
        "direction": None,
        "existing_qty": 0.0,
        "suppressed_bootstrap_qty": 0.0,
        "suppressed_bootstrap_notional": 0.0,
        "reason": None,
    }
    market_bias = {
        "enabled": False,
        "active": False,
        "regime": "disabled",
        "bias_score": 0.0,
        "shift_steps": 0.0,
        "buy_offset_steps": 0.0,
        "sell_offset_steps": 0.0,
        "drift_steps": 0.0,
        "return_steps": 0.0,
        "step_return_ratio": 0.0,
        "signal_steps": 0.0,
        "max_shift_steps": 0.0,
        "drift_weight": 0.0,
        "return_weight": 0.0,
        "weak_buy_pause_enabled": False,
        "weak_buy_pause_active": False,
        "weak_buy_pause_threshold": 0.0,
        "weak_buy_pause_reasons": [],
        "strong_short_pause_enabled": False,
        "strong_short_pause_active": False,
        "strong_short_pause_threshold": 0.0,
        "strong_short_pause_reasons": [],
    }
    market_bias_entry_pause = {
        "enabled": False,
        "buy_pause_active": False,
        "buy_pause_threshold": 0.0,
        "buy_pause_reasons": [],
        "short_pause_active": False,
        "short_pause_threshold": 0.0,
        "short_pause_reasons": [],
        "regime": "disabled",
        "bias_score": 0.0,
    }
    market_bias_regime_switch = {
        "enabled": False,
        "supported": False,
        "requested_mode": requested_strategy_mode,
        "active_mode": requested_strategy_mode,
        "candidate_mode": requested_strategy_mode,
        "pending_mode": None,
        "pending_count": 0,
        "confirm_cycles": 0,
        "weak_threshold": 0.0,
        "strong_threshold": 0.0,
        "regime": "disabled",
        "bias_score": 0.0,
        "reason": None,
        "switched": False,
    }
    synthetic_trend_follow = {
        "enabled": bool(getattr(args, "synthetic_trend_follow_enabled", False)),
        "available": False,
        "active": False,
        "direction": None,
        "reason": "disabled" if not getattr(args, "synthetic_trend_follow_enabled", False) else "pending",
        "mode": "disabled" if not getattr(args, "synthetic_trend_follow_enabled", False) else "inactive",
        "window_1m": None,
        "window_3m": None,
        "window_1m_efficiency_ratio": 0.0,
        "window_3m_efficiency_ratio": 0.0,
        "min_efficiency_ratio": max(_safe_float(getattr(args, "synthetic_trend_follow_min_efficiency_ratio", 0.0)), 0.0),
        "reverse_delay_seconds": max(_safe_float(getattr(args, "synthetic_trend_follow_reverse_delay_seconds", 0.0)), 0.0),
        "reverse_delay_active": False,
        "reverse_delay_remaining_seconds": 0.0,
        "cooldown_until": None,
        "flow_entry_side": None,
        "flow_inventory_direction": None,
        "current_inventory_direction": None,
        "current_inventory_qty": 0.0,
    }
    center_entry_guard = {
        "enabled": False,
        "supported": strategy_mode in {"hedge_neutral", "synthetic_neutral"},
        "drift_steps": 0.0,
        "max_mid_drift_steps": getattr(args, "max_mid_drift_steps", None),
        "reduce_only_active": False,
        "reduce_only_direction": None,
        "long_pause_active": False,
        "short_pause_active": False,
        "long_pause_reasons": [],
        "short_pause_reasons": [],
    }
    near_market_entry_state = {
        "mode": "near_market",
        "near_market_entries_allowed": True,
        "center_distance_steps": 0.0,
        "near_market_entry_max_center_distance_steps": max(
            _safe_float(getattr(effective_args, "near_market_entry_max_center_distance_steps", 2.0)),
            0.0,
        ),
        "grid_inventory_rebalance_min_center_distance_steps": max(
            _safe_float(getattr(effective_args, "grid_inventory_rebalance_min_center_distance_steps", 3.0)),
            _safe_float(getattr(effective_args, "near_market_entry_max_center_distance_steps", 2.0)),
        ),
        "near_market_reentry_confirm_cycles": max(int(getattr(effective_args, "near_market_reentry_confirm_cycles", 2) or 1), 1),
        "candidate_mode": "near_market",
        "pending_mode": None,
        "pending_count": 0,
        "switched": False,
        "reason": "unsupported",
    }

    if startup_pending and not _is_custom_grid_mode(args) and requested_strategy_mode in {"one_way_long", "one_way_short"}:
        flat_start_guard = assess_flat_start_guard(
            strategy_mode=requested_strategy_mode,
            actual_net_qty=actual_net_qty,
            open_orders=open_orders,
            enabled=bool(getattr(args, "flat_start_enabled", True)),
            block_open_orders=not bool(getattr(args, "cancel_stale", True)),
        )
        if flat_start_guard["blocked"]:
            raise StartupProtectionError(str(flat_start_guard["reason"]))

    if requested_strategy_mode in {"hedge_neutral", "synthetic_neutral"}:
        market_bias = resolve_market_bias_offsets(
            enabled=bool(getattr(effective_args, "market_bias_enabled", False)),
            center_price=center_price,
            mid_price=mid_price,
            step_price=effective_args.step_price,
            market_guard_return_ratio=_safe_float((market_guard or {}).get("return_ratio")),
            max_shift_steps=float(getattr(effective_args, "market_bias_max_shift_steps", 0.75)),
            signal_steps=float(getattr(effective_args, "market_bias_signal_steps", 2.0)),
            drift_weight=float(getattr(effective_args, "market_bias_drift_weight", 0.65)),
            return_weight=float(getattr(effective_args, "market_bias_return_weight", 0.35)),
        )
        market_bias_entry_pause = resolve_market_bias_entry_pause(
            buy_pause_enabled=bool(getattr(effective_args, "market_bias_weak_buy_pause_enabled", False)),
            short_pause_enabled=bool(getattr(effective_args, "market_bias_strong_short_pause_enabled", False)),
            market_bias=market_bias,
            weak_buy_pause_threshold=float(getattr(effective_args, "market_bias_weak_buy_pause_threshold", 0.15)),
            strong_short_pause_threshold=float(getattr(effective_args, "market_bias_strong_short_pause_threshold", 0.15)),
        )
        strong_short_probe_scale = (
            _clamp_ratio(float(getattr(effective_args, "market_bias_strong_short_probe_scale", 0.25)))
            if bool(market_bias_entry_pause["short_pause_active"])
            else 0.0
        )
        market_bias.update(
            {
                "weak_buy_pause_enabled": bool(getattr(effective_args, "market_bias_weak_buy_pause_enabled", False)),
                "weak_buy_pause_active": bool(market_bias_entry_pause["buy_pause_active"]),
                "weak_buy_pause_threshold": float(market_bias_entry_pause["buy_pause_threshold"]),
                "weak_buy_pause_reasons": list(market_bias_entry_pause["buy_pause_reasons"]),
                "strong_short_pause_enabled": bool(getattr(effective_args, "market_bias_strong_short_pause_enabled", False)),
                "strong_short_pause_active": bool(market_bias_entry_pause["short_pause_active"]),
                "strong_short_pause_threshold": float(market_bias_entry_pause["short_pause_threshold"]),
                "strong_short_pause_reasons": list(market_bias_entry_pause["short_pause_reasons"]),
                "strong_short_probe_scale": float(strong_short_probe_scale),
            }
        )
        market_bias_regime_switch = resolve_market_bias_regime_switch(
            state=state,
            enabled=bool(getattr(effective_args, "market_bias_regime_switch_enabled", False)),
            requested_strategy_mode=requested_strategy_mode,
            market_bias=market_bias,
            weak_threshold=float(getattr(effective_args, "market_bias_regime_switch_weak_threshold", 0.15)),
            strong_threshold=float(getattr(effective_args, "market_bias_regime_switch_strong_threshold", 0.15)),
            confirm_cycles=int(getattr(effective_args, "market_bias_regime_switch_confirm_cycles", 2)),
        )
        strategy_mode = str(market_bias_regime_switch.get("active_mode") or requested_strategy_mode).strip() or requested_strategy_mode
        if requested_strategy_mode == "synthetic_neutral" and strategy_mode != "synthetic_neutral":
            state["synthetic_ledger_resync_required"] = True

    if _is_inventory_target_neutral_mode(strategy_mode):
        current_long_qty = max(actual_net_qty, 0.0)
        current_short_qty = max(-actual_net_qty, 0.0)
    elif strategy_mode == "hedge_neutral":
        current_long_qty = max(_position_qty(long_position, position_side="LONG"), 0.0)
        current_short_qty = max(_position_qty(short_position, position_side="SHORT"), 0.0)
    elif _is_one_way_short_mode(strategy_mode):
        current_long_qty = 0.0
        current_short_qty = max(-actual_net_qty, 0.0)
    else:
        current_long_qty = max(actual_net_qty, 0.0)
        current_short_qty = 0.0
    current_long_notional = current_long_qty * max(mid_price, 0.0)
    current_short_notional = current_short_qty * max(mid_price, 0.0)
    synthetic_lot_cost_guard_profiles = {
        "bard_12h_push_neutral_v2",
        "based_volume_guarded_bard_v2",
        "xaut_competition_push_neutral_v1",
        "xaut_volume_guarded_bard_v2",
    }
    synthetic_residual_long_flat_notional = max(
        _safe_float(getattr(effective_args, "synthetic_residual_long_flat_notional", None)),
        0.0,
    )
    synthetic_residual_short_flat_notional = max(
        _safe_float(getattr(effective_args, "synthetic_residual_short_flat_notional", None)),
        0.0,
    )
    synthetic_tiny_long_residual_notional = getattr(effective_args, "synthetic_tiny_long_residual_notional", None)
    synthetic_tiny_short_residual_notional = getattr(effective_args, "synthetic_tiny_short_residual_notional", None)
    synthetic_lot_cost_guard_enabled = (
        requested_strategy_mode in {"hedge_neutral", "synthetic_neutral"}
        and (
            str(getattr(effective_args, "strategy_profile", "")).strip() in synthetic_lot_cost_guard_profiles
            or synthetic_residual_long_flat_notional > 0
            or synthetic_residual_short_flat_notional > 0
        )
    )
    synthetic_entry_long_cost_guard_release_notional = (
        max(_safe_float(effective_args.pause_buy_position_notional), 0.0) * 0.5
        if synthetic_lot_cost_guard_enabled
        else 0.0
    )
    synthetic_entry_short_cost_guard_release_notional = (
        max(_safe_float(effective_args.pause_short_position_notional), 0.0) * 0.5
        if synthetic_lot_cost_guard_enabled
        else 0.0
    )
    if center_entry_guard["supported"]:
        center_guard_step_price = max(_safe_float(effective_args.step_price), 0.0)
        center_guard_max_steps = max(_safe_float(getattr(args, "max_mid_drift_steps", None)), 0.0)
        if center_guard_step_price > 0 and center_guard_max_steps > 0 and center_price > 0 and mid_price > 0:
            center_drift_steps = (mid_price - center_price) / center_guard_step_price
            near_market_entry_state = resolve_near_market_entry_state(
                state=state,
                center_distance_steps=abs(center_drift_steps),
                near_market_max_steps=getattr(effective_args, "near_market_entry_max_center_distance_steps", 2.0),
                grid_rebalance_min_steps=getattr(effective_args, "grid_inventory_rebalance_min_center_distance_steps", 3.0),
                confirm_cycles=getattr(effective_args, "near_market_reentry_confirm_cycles", 2),
            )
            center_entry_guard.update(
                {
                    "enabled": True,
                    "drift_steps": center_drift_steps,
                    "max_mid_drift_steps": center_guard_max_steps,
                    "near_market_entry_state": dict(near_market_entry_state),
                }
            )
            grid_rebalance_confirmed = str(near_market_entry_state.get("mode") or "") == "grid_rebalance"
            if grid_rebalance_confirmed and center_drift_steps > 0:
                center_entry_guard["long_pause_active"] = True
                center_entry_guard["long_pause_reasons"].append(
                    "center_entry_guard: "
                    f"mid_price={_price(mid_price)} is {center_drift_steps:.2f} steps above "
                    f"center_price={_price(center_price)}, confirmed grid_rebalance mode after "
                    f"{near_market_entry_state.get('near_market_reentry_confirm_cycles')} cycles"
                )
            if grid_rebalance_confirmed and center_drift_steps < 0:
                center_entry_guard["short_pause_active"] = True
                center_entry_guard["short_pause_reasons"].append(
                    "center_entry_guard: "
                    f"mid_price={_price(mid_price)} is {abs(center_drift_steps):.2f} steps below "
                    f"center_price={_price(center_price)}, confirmed grid_rebalance mode after "
                    f"{near_market_entry_state.get('near_market_reentry_confirm_cycles')} cycles"
                )

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
                entry_price=actual_cost_basis_price,
                qty_tolerance=max(_safe_float(symbol_info.get("step_size")), 1e-9),
                fallback_price=mid_price,
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
                external_long_pause=bool(market_guard["buy_pause_active"] or market_bias_entry_pause["buy_pause_active"]),
                external_pause_reasons=list(market_guard["buy_pause_reasons"]) + list(market_bias_entry_pause["buy_pause_reasons"]),
                external_short_pause=bool(market_bias_entry_pause["short_pause_active"]),
                external_short_pause_reasons=list(market_bias_entry_pause["short_pause_reasons"]),
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
        inventory_long_pause_active = (
            _safe_float(getattr(effective_args, "pause_buy_position_notional", None)) > 0
            and current_long_notional >= _safe_float(getattr(effective_args, "pause_buy_position_notional", None)) - 1e-12
        )
        inventory_short_pause_active = (
            _safe_float(getattr(effective_args, "pause_short_position_notional", None)) > 0
            and current_short_notional >= _safe_float(getattr(effective_args, "pause_short_position_notional", None)) - 1e-12
        )
        inventory_long_probe_scale = (
            _clamp_ratio(float(getattr(effective_args, "inventory_pause_long_probe_scale", 0.0)))
            if inventory_long_pause_active
            else 0.0
        )
        adverse_short_probe_scale_override = resolve_adverse_reduce_probe_scale_override(
            enabled=bool(getattr(effective_args, "adverse_reduce_enabled", False)),
            keep_probe_scale=getattr(effective_args, "adverse_reduce_keep_probe_scale", None),
            mid_price=mid_price,
            current_long_qty=current_long_qty,
            current_long_notional=current_long_notional,
            current_short_qty=current_short_qty,
            current_short_notional=current_short_notional,
            current_long_cost_price=current_long_avg_price,
            current_short_cost_price=current_short_avg_price,
            pause_long_position_notional=effective_args.pause_buy_position_notional,
            pause_short_position_notional=effective_args.pause_short_position_notional,
            long_trigger_ratio=getattr(effective_args, "adverse_reduce_long_trigger_ratio", None),
            short_trigger_ratio=getattr(effective_args, "adverse_reduce_short_trigger_ratio", None),
        )
        inventory_short_probe_scale = (
            _clamp_ratio(
                float(
                    adverse_short_probe_scale_override
                    if adverse_short_probe_scale_override is not None
                    else getattr(effective_args, "inventory_pause_short_probe_scale", 0.25)
                )
            )
            if inventory_short_pause_active
            else 0.0
        )
        combined_short_probe_scale = max(strong_short_probe_scale, inventory_short_probe_scale)
        short_threshold_timeout = resolve_short_threshold_timeout_state(
            state=state,
            current_short_notional=current_short_notional,
            pause_short_position_notional=effective_args.pause_short_position_notional,
            threshold_position_notional=getattr(args, "threshold_position_notional", None),
            hold_seconds=getattr(effective_args, "short_threshold_timeout_seconds", None),
            now=plan_now,
        )
        long_inventory_pause_timeout = resolve_inventory_pause_timeout_state(
            state=state,
            side="long",
            current_notional=current_long_notional,
            pause_position_notional=effective_args.pause_buy_position_notional,
            hold_seconds=getattr(effective_args, "inventory_pause_timeout_seconds", None),
            now=plan_now,
        )
        short_inventory_pause_timeout = resolve_inventory_pause_timeout_state(
            state=state,
            side="short",
            current_notional=current_short_notional,
            pause_position_notional=effective_args.pause_short_position_notional,
            hold_seconds=getattr(effective_args, "inventory_pause_timeout_seconds", None),
            now=plan_now,
        )
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
            startup_entry_multiplier=getattr(effective_args, "startup_entry_multiplier", 1.0),
            startup_large_entry_active=(startup_pending or (current_long_qty <= 1e-12 and current_short_qty <= 1e-12)),
            near_market_entry_max_center_distance_steps=getattr(
                effective_args,
                "near_market_entry_max_center_distance_steps",
                2.0,
            ),
            near_market_entries_allowed=bool(near_market_entry_state.get("near_market_entries_allowed")),
            buy_offset_steps=(
                float(getattr(effective_args, "static_buy_offset_steps", 0.0))
                + float((market_bias or {}).get("buy_offset_steps", 0.0))
            ),
            sell_offset_steps=(
                float(getattr(effective_args, "static_sell_offset_steps", 0.0))
                + float((market_bias or {}).get("sell_offset_steps", 0.0))
            ),
            entry_long_cost_guard_release_notional=synthetic_entry_long_cost_guard_release_notional,
            entry_short_cost_guard_release_notional=synthetic_entry_short_cost_guard_release_notional,
            residual_long_flat_notional=synthetic_residual_long_flat_notional,
            take_profit_min_profit_ratio=getattr(effective_args, "take_profit_min_profit_ratio", None),
            residual_short_flat_notional=synthetic_residual_short_flat_notional,
            synthetic_tiny_long_residual_notional=synthetic_tiny_long_residual_notional,
            synthetic_tiny_short_residual_notional=synthetic_tiny_short_residual_notional,
            entry_long_paused=bool(
                market_guard["buy_pause_active"]
                or market_bias_entry_pause["buy_pause_active"]
                or inventory_long_pause_active
                or center_entry_guard["long_pause_active"]
            ),
            entry_short_paused=bool(
                market_bias_entry_pause["short_pause_active"]
                or inventory_short_pause_active
                or center_entry_guard["short_pause_active"]
            ),
            paused_entry_long_scale=inventory_long_probe_scale,
            paused_entry_short_scale=combined_short_probe_scale,
        )
        controls = apply_hedge_position_controls(
            plan=plan,
            current_long_qty=current_long_qty,
            current_short_qty=current_short_qty,
            mid_price=mid_price,
            pause_long_position_notional=effective_args.pause_buy_position_notional,
            pause_short_position_notional=effective_args.pause_short_position_notional,
            min_mid_price_for_buys=effective_args.min_mid_price_for_buys,
            external_long_pause=bool(
                market_guard["buy_pause_active"]
                or market_bias_entry_pause["buy_pause_active"]
                or center_entry_guard["long_pause_active"]
                or exposure_hard_long_pause
            ),
            external_pause_reasons=(
                list(market_guard["buy_pause_reasons"])
                + list(market_bias_entry_pause["buy_pause_reasons"])
                + list(center_entry_guard["long_pause_reasons"])
                + (["exposure_escalation: hard_unrealized_loss_limit"] if exposure_hard_long_pause else [])
            ),
            external_short_pause=bool(
                market_bias_entry_pause["short_pause_active"] or center_entry_guard["short_pause_active"]
            ),
            external_short_pause_reasons=(
                list(market_bias_entry_pause["short_pause_reasons"])
                + list(center_entry_guard["short_pause_reasons"])
            ),
            preserve_long_entry_on_inventory_pause=inventory_long_probe_scale > 0,
            preserve_short_entry_on_external_pause=strong_short_probe_scale > 0
            and not center_entry_guard["short_pause_active"],
            preserve_short_entry_on_inventory_pause=inventory_short_probe_scale > 0,
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
        if _truthy(state.get("synthetic_ledger_resync_required")):
            _reset_synthetic_ledger_to_actual(
                state=state,
                actual_position_qty=actual_net_qty,
                entry_price=actual_cost_basis_price,
                reason=str((market_bias_regime_switch or {}).get("reason") or "market_bias_regime_switch_return_to_neutral"),
            )
        synthetic_ledger_snapshot = sync_synthetic_ledger(
            state=state,
            symbol=symbol,
            api_key=api_key,
            api_secret=api_secret,
            recv_window=args.recv_window,
            actual_position_qty=actual_net_qty,
            entry_price=actual_cost_basis_price,
            qty_tolerance=max(_safe_float(symbol_info.get("step_size")), 1e-9),
            fallback_price=mid_price,
        )
        current_long_qty = max(_safe_float(synthetic_ledger_snapshot.get("virtual_long_qty")), 0.0)
        current_short_qty = max(_safe_float(synthetic_ledger_snapshot.get("virtual_short_qty")), 0.0)
        current_long_avg_price = max(_safe_float(synthetic_ledger_snapshot.get("virtual_long_avg_price")), 0.0)
        current_short_avg_price = max(_safe_float(synthetic_ledger_snapshot.get("virtual_short_avg_price")), 0.0)
        current_long_notional = current_long_qty * max(mid_price, 0.0)
        current_short_notional = current_short_qty * max(mid_price, 0.0)
        inventory_long_pause_active = (
            _safe_float(getattr(effective_args, "pause_buy_position_notional", None)) > 0
            and current_long_notional >= _safe_float(getattr(effective_args, "pause_buy_position_notional", None)) - 1e-12
        )
        inventory_short_pause_active = (
            _safe_float(getattr(effective_args, "pause_short_position_notional", None)) > 0
            and current_short_notional >= _safe_float(getattr(effective_args, "pause_short_position_notional", None)) - 1e-12
        )
        inventory_long_probe_scale = (
            _clamp_ratio(float(getattr(effective_args, "inventory_pause_long_probe_scale", 0.0)))
            if inventory_long_pause_active
            else 0.0
        )
        adverse_short_probe_scale_override = resolve_adverse_reduce_probe_scale_override(
            enabled=bool(getattr(effective_args, "adverse_reduce_enabled", False)),
            keep_probe_scale=getattr(effective_args, "adverse_reduce_keep_probe_scale", None),
            mid_price=mid_price,
            current_long_qty=current_long_qty,
            current_long_notional=current_long_notional,
            current_short_qty=current_short_qty,
            current_short_notional=current_short_notional,
            current_long_cost_price=current_long_avg_price,
            current_short_cost_price=current_short_avg_price,
            pause_long_position_notional=effective_args.pause_buy_position_notional,
            pause_short_position_notional=effective_args.pause_short_position_notional,
            long_trigger_ratio=getattr(effective_args, "adverse_reduce_long_trigger_ratio", None),
            short_trigger_ratio=getattr(effective_args, "adverse_reduce_short_trigger_ratio", None),
        )
        inventory_short_probe_scale = (
            _clamp_ratio(
                float(
                    adverse_short_probe_scale_override
                    if adverse_short_probe_scale_override is not None
                    else getattr(effective_args, "inventory_pause_short_probe_scale", 0.25)
                )
            )
            if inventory_short_pause_active
            else 0.0
        )
        combined_short_probe_scale = max(strong_short_probe_scale, inventory_short_probe_scale)
        short_threshold_timeout = resolve_short_threshold_timeout_state(
            state=state,
            current_short_notional=current_short_notional,
            pause_short_position_notional=effective_args.pause_short_position_notional,
            threshold_position_notional=getattr(args, "threshold_position_notional", None),
            hold_seconds=getattr(effective_args, "short_threshold_timeout_seconds", None),
            now=plan_now,
        )
        long_inventory_pause_timeout = resolve_inventory_pause_timeout_state(
            state=state,
            side="long",
            current_notional=current_long_notional,
            pause_position_notional=effective_args.pause_buy_position_notional,
            hold_seconds=getattr(effective_args, "inventory_pause_timeout_seconds", None),
            now=plan_now,
        )
        short_inventory_pause_timeout = resolve_inventory_pause_timeout_state(
            state=state,
            side="short",
            current_notional=current_short_notional,
            pause_position_notional=effective_args.pause_short_position_notional,
            hold_seconds=getattr(effective_args, "inventory_pause_timeout_seconds", None),
            now=plan_now,
        )
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
        synthetic_flow_enabled = bool(getattr(effective_args, "synthetic_flow_sleeve_enabled", False))
        synthetic_flow_trigger_notional = getattr(effective_args, "synthetic_flow_sleeve_trigger_notional", None)
        synthetic_flow_sleeve_notional = getattr(effective_args, "synthetic_flow_sleeve_notional", 0.0)
        synthetic_flow_max_loss_ratio = getattr(effective_args, "synthetic_flow_sleeve_max_loss_ratio", None)
        exposure_escalation = resolve_exposure_escalation(
            state=state,
            enabled=bool(getattr(effective_args, "exposure_escalation_enabled", False)),
            now=datetime.now(timezone.utc),
            current_long_notional=current_long_notional,
            current_long_qty=current_long_qty,
            current_long_cost_basis_price=current_long_avg_price,
            mid_price=mid_price,
            trigger_notional=getattr(effective_args, "exposure_escalation_notional", None),
            hold_seconds=getattr(effective_args, "exposure_escalation_hold_seconds", None),
            target_notional=getattr(effective_args, "exposure_escalation_target_notional", None),
            max_loss_ratio=getattr(effective_args, "exposure_escalation_max_loss_ratio", None),
            hard_unrealized_loss_limit=getattr(
                effective_args,
                "exposure_escalation_hard_unrealized_loss_limit",
                None,
            ),
        )
        exposure_hard_long_pause = exposure_escalation.get("reason") == "hard_unrealized_loss_limit"
        if exposure_escalation.get("active"):
            synthetic_flow_enabled = True
            synthetic_flow_trigger_notional = min(
                max(_safe_float(synthetic_flow_trigger_notional), _safe_float(exposure_escalation.get("trigger_notional"))),
                current_long_notional,
            )
            synthetic_flow_sleeve_notional = max(
                _safe_float(synthetic_flow_sleeve_notional),
                current_long_notional - _safe_float(exposure_escalation.get("target_notional")),
                0.0,
            )
            synthetic_flow_max_loss_ratio = exposure_escalation.get("max_loss_ratio")
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
            current_long_avg_price=max(_safe_float(synthetic_ledger_snapshot.get("virtual_long_avg_price")), 0.0),
            current_short_avg_price=max(_safe_float(synthetic_ledger_snapshot.get("virtual_short_avg_price")), 0.0),
            current_long_lots=list(synthetic_ledger_snapshot.get("virtual_long_lots") or []),
            current_short_lots=list(synthetic_ledger_snapshot.get("virtual_short_lots") or []),
            startup_entry_multiplier=getattr(effective_args, "startup_entry_multiplier", 1.0),
            startup_large_entry_active=(startup_pending or (current_long_qty <= 1e-12 and current_short_qty <= 1e-12)),
            near_market_entry_max_center_distance_steps=getattr(
                effective_args,
                "near_market_entry_max_center_distance_steps",
                2.0,
            ),
            near_market_entries_allowed=bool(near_market_entry_state.get("near_market_entries_allowed")),
            buy_offset_steps=(
                float(getattr(effective_args, "static_buy_offset_steps", 0.0))
                + float((market_bias or {}).get("buy_offset_steps", 0.0))
            ),
            sell_offset_steps=(
                float(getattr(effective_args, "static_sell_offset_steps", 0.0))
                + float((market_bias or {}).get("sell_offset_steps", 0.0))
            ),
            entry_long_cost_guard_release_notional=synthetic_entry_long_cost_guard_release_notional,
            entry_short_cost_guard_release_notional=synthetic_entry_short_cost_guard_release_notional,
            residual_long_flat_notional=synthetic_residual_long_flat_notional,
            take_profit_min_profit_ratio=getattr(effective_args, "take_profit_min_profit_ratio", None),
            residual_short_flat_notional=synthetic_residual_short_flat_notional,
            synthetic_tiny_long_residual_notional=synthetic_tiny_long_residual_notional,
            synthetic_tiny_short_residual_notional=synthetic_tiny_short_residual_notional,
            entry_long_paused=bool(
                market_guard["buy_pause_active"]
                or market_bias_entry_pause["buy_pause_active"]
                or inventory_long_pause_active
                or center_entry_guard["long_pause_active"]
                or exposure_hard_long_pause
            ),
            entry_short_paused=bool(
                market_bias_entry_pause["short_pause_active"]
                or inventory_short_pause_active
                or center_entry_guard["short_pause_active"]
            ),
            paused_entry_long_scale=inventory_long_probe_scale,
            paused_entry_short_scale=combined_short_probe_scale,
        )
        plan = _convert_plan_orders_to_one_way(hedge_plan)
        synthetic_flow_sleeve = apply_synthetic_flow_sleeve(
            plan=plan,
            enabled=synthetic_flow_enabled,
            current_long_notional=current_long_notional,
            current_short_notional=current_short_notional,
            current_long_avg_price=current_long_avg_price,
            current_short_avg_price=current_short_avg_price,
            trigger_notional=synthetic_flow_trigger_notional,
            sleeve_notional=synthetic_flow_sleeve_notional,
            levels=int(getattr(effective_args, "synthetic_flow_sleeve_levels", 0) or 0),
            per_order_notional=inventory_tier["effective_per_order_notional"],
            order_notional=getattr(effective_args, "synthetic_flow_sleeve_order_notional", None),
            max_loss_ratio=synthetic_flow_max_loss_ratio,
            step_price=effective_args.step_price,
            tick_size=symbol_info.get("tick_size"),
            step_size=symbol_info.get("step_size"),
            min_qty=symbol_info.get("min_qty"),
            min_notional=symbol_info.get("min_notional"),
            bid_price=bid_price,
            ask_price=ask_price,
            buy_offset_steps=(
                float(getattr(effective_args, "static_buy_offset_steps", 0.0))
                + float((market_bias or {}).get("buy_offset_steps", 0.0))
            ),
            sell_offset_steps=(
                float(getattr(effective_args, "static_sell_offset_steps", 0.0))
                + float((market_bias or {}).get("sell_offset_steps", 0.0))
            ),
            current_long_lots=list(synthetic_ledger_snapshot.get("virtual_long_lots") or []),
            current_short_lots=list(synthetic_ledger_snapshot.get("virtual_short_lots") or []),
        )
        synthetic_trend_follow = resolve_synthetic_trend_follow(
            state=state,
            market_guard=market_guard,
            now=plan_now,
            current_long_qty=current_long_qty,
            current_short_qty=current_short_qty,
            enabled=bool(getattr(effective_args, "synthetic_trend_follow_enabled", False)),
            window_1m_abs_return_ratio=getattr(effective_args, "synthetic_trend_follow_1m_abs_return_ratio", None),
            window_1m_amplitude_ratio=getattr(effective_args, "synthetic_trend_follow_1m_amplitude_ratio", None),
            window_3m_abs_return_ratio=getattr(effective_args, "synthetic_trend_follow_3m_abs_return_ratio", None),
            window_3m_amplitude_ratio=getattr(effective_args, "synthetic_trend_follow_3m_amplitude_ratio", None),
            min_efficiency_ratio=getattr(effective_args, "synthetic_trend_follow_min_efficiency_ratio", None),
            reverse_delay_seconds=getattr(effective_args, "synthetic_trend_follow_reverse_delay_seconds", None),
        )
        synthetic_trend_follow.update(
            apply_synthetic_trend_follow_guard(
                plan=plan,
                trend_follow=synthetic_trend_follow,
            )
        )
        controls = apply_hedge_position_controls(
            plan=plan,
            current_long_qty=current_long_qty,
            current_short_qty=current_short_qty,
            mid_price=mid_price,
            pause_long_position_notional=effective_args.pause_buy_position_notional,
            pause_short_position_notional=effective_args.pause_short_position_notional,
            min_mid_price_for_buys=effective_args.min_mid_price_for_buys,
            external_long_pause=bool(
                market_guard["buy_pause_active"]
                or market_bias_entry_pause["buy_pause_active"]
                or center_entry_guard["long_pause_active"]
            ),
            external_pause_reasons=(
                list(market_guard["buy_pause_reasons"])
                + list(market_bias_entry_pause["buy_pause_reasons"])
                + list(center_entry_guard["long_pause_reasons"])
            ),
            external_short_pause=bool(
                market_bias_entry_pause["short_pause_active"] or center_entry_guard["short_pause_active"]
            ),
            external_short_pause_reasons=(
                list(market_bias_entry_pause["short_pause_reasons"])
                + list(center_entry_guard["short_pause_reasons"])
            ),
            preserve_long_entry_on_inventory_pause=inventory_long_probe_scale > 0,
            preserve_short_entry_on_external_pause=strong_short_probe_scale > 0
            and not center_entry_guard["short_pause_active"],
            preserve_short_entry_on_inventory_pause=inventory_short_probe_scale > 0,
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
        long_active_delever = apply_active_delever_long(
            plan=plan,
            current_long_qty=current_long_qty,
            current_long_notional=controls["current_long_notional"],
            current_long_avg_price=max(_safe_float(synthetic_ledger_snapshot.get("virtual_long_avg_price")), 0.0),
            current_long_lots=list(synthetic_ledger_snapshot.get("virtual_long_lots") or []),
            pause_long_position_notional=effective_args.pause_buy_position_notional,
            threshold_position_notional=getattr(effective_args, "threshold_position_notional", None),
            per_order_notional=inventory_tier["effective_per_order_notional"],
            step_price=effective_args.step_price,
            tick_size=symbol_info.get("tick_size"),
            bid_price=bid_price,
            ask_price=ask_price,
            min_profit_ratio=getattr(effective_args, "take_profit_min_profit_ratio", None),
            market_guard_buy_pause_active=bool(market_guard["buy_pause_active"]),
            grid_buffer_realized_notional=_safe_float(synthetic_ledger_snapshot.get("grid_buffer_realized_notional")),
            grid_buffer_spent_notional=_safe_float(synthetic_ledger_snapshot.get("grid_buffer_spent_notional")),
            step_size=symbol_info.get("step_size"),
            min_qty=symbol_info.get("min_qty"),
            min_notional=symbol_info.get("min_notional"),
            inventory_pause_timeout_active=bool(long_inventory_pause_timeout.get("timeout_active")),
            timeout_target_notional=effective_args.pause_buy_position_notional,
            force_release_active=bool(exposure_escalation.get("active")),
            force_release_target_notional=exposure_escalation.get("target_notional"),
            force_release_trigger_mode="exposure_escalation",
        )
        short_active_delever = apply_active_delever_short(
            plan=plan,
            current_short_qty=current_short_qty,
            current_short_notional=controls["current_short_notional"],
            current_short_avg_price=max(_safe_float(synthetic_ledger_snapshot.get("virtual_short_avg_price")), 0.0),
            current_short_lots=list(synthetic_ledger_snapshot.get("virtual_short_lots") or []),
            pause_short_position_notional=effective_args.pause_short_position_notional,
            threshold_position_notional=getattr(effective_args, "threshold_position_notional", None),
            per_order_notional=inventory_tier["effective_per_order_notional"],
            step_price=effective_args.step_price,
            tick_size=symbol_info.get("tick_size"),
            bid_price=bid_price,
            ask_price=ask_price,
            min_profit_ratio=getattr(effective_args, "take_profit_min_profit_ratio", None),
            grid_buffer_realized_notional=_safe_float(synthetic_ledger_snapshot.get("grid_buffer_realized_notional")),
            grid_buffer_spent_notional=_safe_float(synthetic_ledger_snapshot.get("grid_buffer_spent_notional")),
            step_size=symbol_info.get("step_size"),
            min_qty=symbol_info.get("min_qty"),
            min_notional=symbol_info.get("min_notional"),
            threshold_timeout_active=bool(short_threshold_timeout.get("timeout_active")),
            timeout_target_notional=effective_args.pause_short_position_notional,
            inventory_pause_timeout_active=bool(short_inventory_pause_timeout.get("timeout_active")),
            inventory_pause_timeout_target_notional=effective_args.pause_short_position_notional,
        )
        if _is_best_quote_neutral_profile(effective_strategy_profile):
            take_profit_guard = {
                "enabled": False,
                "long_active": False,
                "short_active": False,
                "adjusted_sell_orders": 0,
                "adjusted_buy_orders": 0,
                "dropped_sell_orders": 0,
                "dropped_buy_orders": 0,
                "reason": "best_quote_neutral_near_quote",
            }
        else:
            take_profit_guard = apply_take_profit_profit_guard(
                plan=plan,
                current_long_qty=current_long_qty,
                current_short_qty=current_short_qty,
                current_long_avg_price=exchange_long_avg_price,
                current_short_avg_price=exchange_short_avg_price,
                synthetic_long_avg_price=current_long_avg_price,
                synthetic_short_avg_price=current_short_avg_price,
                current_long_notional=current_long_notional,
                current_short_notional=current_short_notional,
                pause_long_position_notional=effective_args.pause_buy_position_notional,
                pause_short_position_notional=effective_args.pause_short_position_notional,
                threshold_long_position_notional=getattr(effective_args, "threshold_position_notional", None),
                threshold_short_position_notional=getattr(effective_args, "threshold_position_notional", None),
                min_profit_ratio=getattr(effective_args, "take_profit_min_profit_ratio", None),
                tick_size=symbol_info.get("tick_size"),
                bid_price=bid_price,
                ask_price=ask_price,
            )
        active_delever = {
            "enabled": bool(long_active_delever.get("enabled")) or bool(short_active_delever.get("enabled")),
            "active": bool(long_active_delever.get("active")) or bool(short_active_delever.get("active")),
            "trigger_mode": long_active_delever.get("trigger_mode") or short_active_delever.get("trigger_mode"),
            "available_buffer_notional": max(
                _safe_float(long_active_delever.get("available_buffer_notional")),
                _safe_float(short_active_delever.get("available_buffer_notional")),
            ),
            "estimated_cost_notional": _safe_float(long_active_delever.get("estimated_cost_notional")) + _safe_float(
                short_active_delever.get("estimated_cost_notional")
            ),
            "active_sell_order_count": int(long_active_delever.get("active_sell_order_count") or 0),
            "active_buy_order_count": int(short_active_delever.get("active_buy_order_count") or 0),
            "release_sell_order_count": int(long_active_delever.get("release_sell_order_count") or 0),
            "release_buy_order_count": int(short_active_delever.get("release_buy_order_count") or 0),
            "release_floor_price": long_active_delever.get("release_floor_price"),
            "release_ceiling_price": short_active_delever.get("release_ceiling_price"),
            "synthesized_release_source_order_count": int(
                long_active_delever.get("synthesized_release_source_order_count") or 0
            )
            + int(short_active_delever.get("synthesized_release_source_order_count") or 0),
            "pruned_flow_sleeve_order_count": int(long_active_delever.get("pruned_flow_sleeve_order_count") or 0)
            + int(short_active_delever.get("pruned_flow_sleeve_order_count") or 0),
            "force_release_active": bool(long_active_delever.get("force_release_active"))
            or bool(short_active_delever.get("force_release_active")),
            "force_release_target_notional": long_active_delever.get("force_release_target_notional")
            or short_active_delever.get("force_release_target_notional"),
            "pause_long_position_notional": long_active_delever.get("pause_long_position_notional"),
            "pause_short_position_notional": short_active_delever.get("pause_short_position_notional"),
            "threshold_timeout_active": bool(short_threshold_timeout.get("timeout_active")),
            "threshold_timeout_duration_seconds": _safe_float(short_threshold_timeout.get("duration_seconds")),
            "threshold_timeout_hold_seconds": _safe_float(short_threshold_timeout.get("hold_seconds")),
            "threshold_timeout_target_notional": short_threshold_timeout.get("target_notional"),
            "threshold_timeout_aggressive": bool(short_active_delever.get("threshold_timeout_aggressive")),
            "inventory_pause_timeout_active": bool(long_inventory_pause_timeout.get("timeout_active"))
            or bool(short_inventory_pause_timeout.get("timeout_active")),
            "inventory_pause_timeout_side": (
                "long"
                if bool(long_inventory_pause_timeout.get("timeout_active"))
                else ("short" if bool(short_inventory_pause_timeout.get("timeout_active")) else None)
            ),
            "inventory_pause_timeout_duration_seconds": max(
                _safe_float(long_inventory_pause_timeout.get("duration_seconds")),
                _safe_float(short_inventory_pause_timeout.get("duration_seconds")),
            ),
            "inventory_pause_timeout_hold_seconds": _safe_float(
                long_inventory_pause_timeout.get("hold_seconds")
                or short_inventory_pause_timeout.get("hold_seconds")
            ),
            "inventory_pause_timeout_target_notional": (
                long_inventory_pause_timeout.get("target_notional")
                if bool(long_inventory_pause_timeout.get("timeout_active"))
                else short_inventory_pause_timeout.get("target_notional")
            ),
            "inventory_pause_timeout_aggressive": bool(long_active_delever.get("inventory_pause_timeout_aggressive"))
            or bool(short_active_delever.get("inventory_pause_timeout_aggressive")),
        }
        synthetic_inventory_exit_priority = apply_synthetic_inventory_exit_priority(
            plan=plan,
            strategy_mode=strategy_mode,
            current_long_qty=current_long_qty,
            current_short_qty=current_short_qty,
            current_long_notional=current_long_notional,
            current_short_notional=current_short_notional,
            pause_long_position_notional=effective_args.pause_buy_position_notional,
            pause_short_position_notional=effective_args.pause_short_position_notional,
            near_market_entries_allowed=bool(near_market_entry_state.get("near_market_entries_allowed")),
            step_size=symbol_info.get("step_size"),
            strict_exit_only=_is_strict_neutral_ping_pong_profile(effective_strategy_profile),
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
        take_profit_guard = apply_take_profit_profit_guard(
            plan=plan,
            current_long_qty=current_long_qty,
            current_short_qty=current_short_qty,
            current_long_avg_price=current_long_avg_price,
            current_short_avg_price=current_short_avg_price,
            current_long_notional=current_long_notional,
            current_short_notional=current_short_notional,
            pause_long_position_notional=effective_args.pause_buy_position_notional,
            pause_short_position_notional=effective_args.pause_short_position_notional,
            threshold_long_position_notional=getattr(effective_args, "threshold_position_notional", None),
            threshold_short_position_notional=getattr(effective_args, "threshold_position_notional", None),
            min_profit_ratio=getattr(effective_args, "take_profit_min_profit_ratio", None),
            tick_size=symbol_info.get("tick_size"),
            bid_price=bid_price,
            ask_price=ask_price,
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
    elif _is_competition_inventory_grid_mode(strategy_mode):
        if dual_side_position:
            raise RuntimeError("competition inventory grid 策略要求账户处于单向持仓模式")
        max_position_notional_arg = getattr(args, "max_position_notional", None)
        inventory_tier = {
            "enabled": False,
            "active": False,
            "ratio": 0.0,
            "start_notional": None,
            "end_notional": None,
            "effective_buy_levels": 0,
            "effective_sell_levels": 0,
            "effective_per_order_notional": effective_args.per_order_notional,
            "effective_base_position_notional": 0.0,
        }
        inventory_grid_trades = _fetch_trade_rows_since(
            symbol=symbol,
            api_key=api_key,
            api_secret=api_secret,
            start_time_ms=max(int((_utc_now().timestamp() * 1000) - 48 * 3600 * 1000), 0),
            recv_window=args.recv_window,
        )
        plan = _generate_competition_inventory_grid_plan(
            state=state,
            trades=inventory_grid_trades,
            current_position_qty=abs(actual_net_qty),
            bid_price=bid_price,
            ask_price=ask_price,
            step_price=effective_args.step_price,
            first_order_multiplier=float(getattr(args, "first_order_multiplier", 1.0)),
            per_order_notional=effective_args.per_order_notional,
            threshold_position_notional=float(getattr(args, "threshold_position_notional", 0.0)),
            max_order_position_notional=float(getattr(args, "max_order_position_notional", 0.0)),
            max_position_notional=_safe_float(max_position_notional_arg),
            tick_size=symbol_info.get("tick_size"),
            step_size=symbol_info.get("step_size"),
            min_qty=symbol_info.get("min_qty"),
            min_notional=symbol_info.get("min_notional"),
        )
        inventory_tier["effective_buy_levels"] = len(plan.get("buy_orders", []))
        inventory_tier["effective_sell_levels"] = len(plan.get("sell_orders", []))
        controls = {
            "buy_paused": False,
            "pause_reasons": [],
            "short_paused": False,
            "short_pause_reasons": [],
            "current_long_notional": current_long_notional,
            "current_short_notional": current_short_notional,
        }
        cap_controls = {
            "cap_applied": False,
            "buy_budget_notional": None,
            "planned_buy_notional": sum(
                _safe_float(item.get("notional"))
                for item in [*list(plan.get("bootstrap_orders", [])), *list(plan.get("buy_orders", []))]
                if str(item.get("side", "")).upper() == "BUY"
            ),
            "max_position_notional": max_position_notional_arg,
            "short_cap_applied": False,
            "short_budget_notional": None,
            "planned_short_notional": sum(
                _safe_float(item.get("notional"))
                for item in [*list(plan.get("bootstrap_orders", [])), *list(plan.get("sell_orders", []))]
                if str(item.get("side", "")).upper() == "SELL"
            ),
            "max_short_position_notional": None,
        }
        target_base_qty = abs(actual_net_qty)
        bootstrap_qty = sum(_safe_float(item.get("qty")) for item in list(plan.get("bootstrap_orders", [])))
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
        take_profit_guard = apply_take_profit_profit_guard(
            plan=plan,
            current_long_qty=0.0,
            current_short_qty=current_short_qty,
            current_long_avg_price=0.0,
            current_short_avg_price=current_short_avg_price,
            current_long_notional=0.0,
            current_short_notional=current_short_notional,
            pause_long_position_notional=None,
            pause_short_position_notional=effective_args.pause_short_position_notional,
            threshold_short_position_notional=getattr(effective_args, "threshold_position_notional", None),
            min_profit_ratio=getattr(effective_args, "take_profit_min_profit_ratio", None),
            tick_size=symbol_info.get("tick_size"),
            bid_price=bid_price,
            ask_price=ask_price,
        )
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
        if _is_best_quote_long_profile(effective_strategy_profile):
            protected_base_notional = max(_safe_float(getattr(effective_args, "base_position_notional", 0.0)), 0.0)
            protected_base_qty = 0.0
            if protected_base_notional > 0 and mid_price > 0:
                protected_base_qty = protected_base_notional / mid_price
            reduce_available_qty = max(current_long_qty - protected_base_qty, 0.0)
            plan = build_best_quote_long_flip_plan(
                bid_price=bid_price,
                ask_price=ask_price,
                per_order_notional=effective_args.per_order_notional,
                max_position_notional=effective_args.max_position_notional,
                max_entry_orders=effective_args.buy_levels,
                max_exit_orders=effective_args.sell_levels,
                current_long_qty=current_long_qty,
                current_long_notional=current_long_notional,
                reduce_available_qty=reduce_available_qty,
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
                "effective_base_position_notional": protected_base_notional,
                "protected_base_qty": protected_base_qty,
                "reduce_available_qty": reduce_available_qty,
            }
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
        if _is_best_quote_long_profile(effective_strategy_profile):
            take_profit_guard = {
                "enabled": False,
                "long_active": False,
                "short_active": False,
                "adjusted_sell_orders": 0,
                "adjusted_buy_orders": 0,
                "dropped_sell_orders": 0,
                "dropped_buy_orders": 0,
                "reason": "best_quote_long_near_quote",
            }
        else:
            take_profit_guard = apply_take_profit_profit_guard(
                plan=plan,
                current_long_qty=current_long_qty,
                current_short_qty=0.0,
                current_long_avg_price=current_long_avg_price,
                current_short_avg_price=0.0,
                current_long_notional=current_long_notional,
                current_short_notional=0.0,
                pause_long_position_notional=effective_args.pause_buy_position_notional,
                pause_short_position_notional=None,
                threshold_long_position_notional=getattr(effective_args, "threshold_position_notional", None),
                min_profit_ratio=getattr(effective_args, "take_profit_min_profit_ratio", None),
                tick_size=symbol_info.get("tick_size"),
                bid_price=bid_price,
                ask_price=ask_price,
            )
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
        if _uses_volume_long_v4_staged_delever(effective_strategy_profile):
            volume_long_v4_delever = apply_volume_long_v4_staged_delever(
                plan=plan,
                current_long_qty=current_long_qty,
                current_long_notional=controls["current_long_notional"],
                current_long_avg_price=current_long_avg_price,
                pause_long_position_notional=effective_args.pause_buy_position_notional,
                max_position_notional=effective_args.max_position_notional,
                per_order_notional=inventory_tier["effective_per_order_notional"],
                step_price=effective_args.step_price,
                tick_size=symbol_info.get("tick_size"),
                bid_price=bid_price,
                ask_price=ask_price,
            )
            flow_cost_basis_price = actual_break_even_price if actual_break_even_price > 0 else current_long_avg_price
            exposure_escalation = resolve_exposure_escalation(
                state=state,
                enabled=bool(getattr(effective_args, "exposure_escalation_enabled", False)),
                now=datetime.now(timezone.utc),
                current_long_notional=controls["current_long_notional"],
                current_long_qty=current_long_qty,
                current_long_cost_basis_price=flow_cost_basis_price,
                mid_price=mid_price,
                trigger_notional=getattr(effective_args, "exposure_escalation_notional", None),
                hold_seconds=getattr(effective_args, "exposure_escalation_hold_seconds", None),
                target_notional=getattr(effective_args, "exposure_escalation_target_notional", None),
                max_loss_ratio=getattr(effective_args, "exposure_escalation_max_loss_ratio", None),
                hard_unrealized_loss_limit=getattr(
                    effective_args,
                    "exposure_escalation_hard_unrealized_loss_limit",
                    None,
                ),
            )
            flow_enabled = bool(getattr(effective_args, "volume_long_v4_flow_sleeve_enabled", False))
            flow_trigger_notional = getattr(effective_args, "volume_long_v4_flow_sleeve_trigger_notional", None)
            flow_reduce_to_notional = getattr(effective_args, "volume_long_v4_flow_sleeve_reduce_to_notional", None)
            flow_sleeve_notional = getattr(effective_args, "volume_long_v4_flow_sleeve_notional", 0.0)
            flow_max_loss_ratio = getattr(effective_args, "volume_long_v4_flow_sleeve_max_loss_ratio", None)
            if exposure_escalation.get("active"):
                flow_enabled = True
                flow_trigger_notional = min(
                    max(_safe_float(flow_trigger_notional), _safe_float(exposure_escalation.get("trigger_notional"))),
                    controls["current_long_notional"],
                )
                flow_reduce_to_notional = exposure_escalation.get("target_notional")
                flow_sleeve_notional = max(
                    _safe_float(flow_sleeve_notional),
                    controls["current_long_notional"] - _safe_float(exposure_escalation.get("target_notional")),
                    0.0,
                )
                flow_max_loss_ratio = exposure_escalation.get("max_loss_ratio")
                if exposure_escalation.get("reason") == "hard_unrealized_loss_limit":
                    controls["buy_paused"] = True
                    controls["pause_reasons"] = list(controls.get("pause_reasons", []))
                    controls["pause_reasons"].append("exposure_escalation: hard_unrealized_loss_limit")
            volume_long_v4_flow_sleeve = apply_volume_long_v4_flow_sleeve(
                plan=plan,
                enabled=flow_enabled,
                current_long_qty=current_long_qty,
                current_long_notional=controls["current_long_notional"],
                current_long_cost_basis_price=flow_cost_basis_price,
                trigger_notional=flow_trigger_notional,
                reduce_to_notional=flow_reduce_to_notional,
                sleeve_notional=flow_sleeve_notional,
                levels=int(getattr(effective_args, "volume_long_v4_flow_sleeve_levels", 0) or 0),
                per_order_notional=inventory_tier["effective_per_order_notional"],
                order_notional=getattr(effective_args, "volume_long_v4_flow_sleeve_order_notional", None),
                max_loss_ratio=flow_max_loss_ratio,
                step_price=effective_args.step_price,
                tick_size=symbol_info.get("tick_size"),
                step_size=symbol_info.get("step_size"),
                min_qty=symbol_info.get("min_qty"),
                min_notional=symbol_info.get("min_notional"),
                bid_price=bid_price,
                ask_price=ask_price,
                sell_offset_steps=getattr(effective_args, "static_sell_offset_steps", 0.0),
            )
        target_base_qty = plan["target_base_qty"]
        bootstrap_qty = plan["bootstrap_qty"]

    if startup_pending and not _is_custom_grid_mode(args) and requested_strategy_mode in {"one_way_long", "one_way_short"}:
        warm_start = apply_warm_start_bootstrap_guard(
            plan=plan,
            strategy_mode=strategy_mode,
            current_long_qty=current_long_qty,
            current_short_qty=current_short_qty,
            startup_pending=startup_pending,
            enabled=bool(getattr(args, "warm_start_enabled", True)),
        )
        bootstrap_qty = _safe_float(plan.get("bootstrap_qty", bootstrap_qty))

    adverse_long_cost_price, adverse_long_cost_basis_source = _resolve_adverse_reduce_cost_basis(
        side="long",
        requested_strategy_mode=requested_strategy_mode,
        actual_net_qty=actual_net_qty,
        actual_cost_basis_price=actual_cost_basis_price,
        long_position=long_position,
        short_position=short_position,
        synthetic_ledger_snapshot=synthetic_ledger_snapshot,
    )
    adverse_short_cost_price, adverse_short_cost_basis_source = _resolve_adverse_reduce_cost_basis(
        side="short",
        requested_strategy_mode=requested_strategy_mode,
        actual_net_qty=actual_net_qty,
        actual_cost_basis_price=actual_cost_basis_price,
        long_position=long_position,
        short_position=short_position,
        synthetic_ledger_snapshot=synthetic_ledger_snapshot,
    )
    adverse_reduce_enabled = bool(getattr(effective_args, "adverse_reduce_enabled", False)) and not bool(
        (exposure_escalation or {}).get("active")
    )
    adverse_inventory_reduce = assess_adverse_inventory_reduce(
        enabled=adverse_reduce_enabled,
        mid_price=mid_price,
        current_long_qty=current_long_qty,
        current_long_notional=controls.get("current_long_notional", current_long_notional),
        current_short_qty=current_short_qty,
        current_short_notional=controls.get("current_short_notional", current_short_notional),
        current_long_cost_price=adverse_long_cost_price,
        current_short_cost_price=adverse_short_cost_price,
        current_long_cost_basis_source=adverse_long_cost_basis_source,
        current_short_cost_basis_source=adverse_short_cost_basis_source,
        pause_long_position_notional=effective_args.pause_buy_position_notional,
        pause_short_position_notional=effective_args.pause_short_position_notional,
        long_trigger_ratio=getattr(effective_args, "adverse_reduce_long_trigger_ratio", None),
        short_trigger_ratio=getattr(effective_args, "adverse_reduce_short_trigger_ratio", None),
        target_ratio=getattr(effective_args, "adverse_reduce_target_ratio", 0.75),
    )
    adverse_inventory_reduce = apply_adverse_inventory_reduce(
        plan=plan,
        state=state,
        report=adverse_inventory_reduce,
        now=plan_now,
        current_long_qty=current_long_qty,
        current_long_notional=controls.get("current_long_notional", current_long_notional),
        current_short_qty=current_short_qty,
        current_short_notional=controls.get("current_short_notional", current_short_notional),
        pause_long_position_notional=effective_args.pause_buy_position_notional,
        pause_short_position_notional=effective_args.pause_short_position_notional,
        target_ratio=getattr(effective_args, "adverse_reduce_target_ratio", 0.75),
        max_order_notional=getattr(effective_args, "adverse_reduce_max_order_notional", 0.0),
        bid_price=bid_price,
        ask_price=ask_price,
        tick_size=symbol_info.get("tick_size"),
        step_size=symbol_info.get("step_size"),
        min_qty=symbol_info.get("min_qty"),
        min_notional=symbol_info.get("min_notional"),
        maker_timeout_seconds=getattr(effective_args, "adverse_reduce_maker_timeout_seconds", 45.0),
    )

    volume_long_v4_open_orders = (
        _decorate_volume_long_v4_open_orders(open_orders)
        if _uses_volume_long_v4_staged_delever(effective_strategy_profile) and strategy_mode == "one_way_long"
        else open_orders
    )
    desired_orders = [
        *plan["buy_orders"],
        *plan["sell_orders"],
    ]
    if _uses_volume_long_v4_staged_delever(effective_strategy_profile) and strategy_mode == "one_way_long":
        desired_orders = preserve_sticky_exit_orders(
            existing_orders=volume_long_v4_open_orders,
            desired_orders=desired_orders,
            sticky_roles={"soft_delever_long", "hard_delever_long"},
        )
        plan["buy_orders"] = [
            dict(item)
            for item in desired_orders
            if str(item.get("side", "")).upper().strip() == "BUY"
        ]
        plan["sell_orders"] = [
            dict(item)
            for item in desired_orders
            if str(item.get("side", "")).upper().strip() == "SELL"
        ]
    open_orders_for_diff = open_orders
    sticky_exit_mode = {"key": None, "roles": []}
    previous_sticky_exit_mode_key = str(state.get("sticky_exit_mode_key") or "").strip() or None
    if _is_synthetic_neutral_mode(strategy_mode):
        open_orders_for_diff = _decorate_synthetic_open_orders(state=state, open_orders=open_orders)
        sticky_exit_mode = resolve_sticky_exit_mode(
            active_delever=active_delever,
            synthetic_inventory_exit_priority=synthetic_inventory_exit_priority,
        )
        if (
            sticky_exit_mode["key"]
            and sticky_exit_mode["key"] == previous_sticky_exit_mode_key
            and sticky_exit_mode["roles"]
        ):
            desired_orders = preserve_sticky_exit_orders(
                existing_orders=open_orders_for_diff,
                desired_orders=desired_orders,
                sticky_roles=set(sticky_exit_mode["roles"]),
            )
        desired_orders = preserve_sticky_entry_orders(
            existing_orders=open_orders_for_diff,
            desired_orders=desired_orders,
            price_tolerance=effective_args.step_price,
            max_levels_per_group=getattr(effective_args, "sticky_entry_levels", None),
        )
        synthetic_tp_only_watchdog = assess_synthetic_tp_only_watchdog(
            state=state,
            strategy_mode=strategy_mode,
            plan={
                "bootstrap_orders": list(plan.get("bootstrap_orders") or []),
                "buy_orders": [
                    dict(item)
                    for item in desired_orders
                    if str(item.get("side", "")).upper().strip() == "BUY"
                ],
                "sell_orders": [
                    dict(item)
                    for item in desired_orders
                    if str(item.get("side", "")).upper().strip() == "SELL"
                ],
            },
            current_long_qty=current_long_qty,
            current_short_qty=current_short_qty,
            mid_price=mid_price,
            step_price=effective_args.step_price,
            now=plan_now,
        )
    else:
        state.pop("synthetic_tp_only_watchdog_state", None)
    diff = diff_open_orders(existing_orders=open_orders_for_diff, desired_orders=desired_orders)

    state_now = _isoformat(_utc_now())
    if startup_pending:
        state["startup_pending"] = False
        state["startup_completed_at"] = state_now
    state["center_price"] = center_price
    state["last_mid_price"] = mid_price
    state["updated_at"] = state_now
    state["version"] = STATE_VERSION
    if sticky_exit_mode["key"]:
        state["sticky_exit_mode_key"] = sticky_exit_mode["key"]
    else:
        state.pop("sticky_exit_mode_key", None)
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")

    report = {
        "generated_at": _isoformat(_utc_now()),
        "symbol": symbol,
        "strategy_mode": strategy_mode,
        "requested_strategy_mode": requested_strategy_mode,
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
        "startup_pending": startup_pending,
        "flat_start_guard": flat_start_guard,
        "warm_start": warm_start,
        "excess_inventory_gate": excess_inventory_gate,
        "neutral_hourly_scale": neutral_hourly_scale,
        "market_bias": market_bias,
        "market_bias_regime_switch": market_bias_regime_switch,
        "center_entry_guard": center_entry_guard,
        "near_market_entry_state": near_market_entry_state,
        "sticky_exit_mode": sticky_exit_mode,
        "shift_moves": shift_moves,
        "market_guard": market_guard,
        "adaptive_step": adaptive_step,
        "synthetic_trend_follow": synthetic_trend_follow,
        "synthetic_flow_sleeve": synthetic_flow_sleeve,
        "adverse_inventory_reduce": adverse_inventory_reduce,
        "take_profit_guard": take_profit_guard,
        "volume_long_v4_delever": volume_long_v4_delever,
        "volume_long_v4_flow_sleeve": volume_long_v4_flow_sleeve,
        "exposure_escalation": exposure_escalation,
        "active_delever": active_delever,
        "long_inventory_pause_timeout": long_inventory_pause_timeout,
        "short_inventory_pause_timeout": short_inventory_pause_timeout,
        "short_threshold_timeout": short_threshold_timeout,
        "position_cost_basis_source": "entryPrice" if prefer_entry_price_cost_basis else "breakEvenPrice",
        "auto_regime": auto_regime,
        "xaut_adaptive": xaut_adaptive,
        "current_long_qty": current_long_qty,
        "current_long_notional": controls["current_long_notional"],
        "current_long_avg_price": current_long_avg_price,
        "current_short_qty": current_short_qty,
        "current_short_notional": controls.get("current_short_notional", current_short_notional),
        "current_short_avg_price": current_short_avg_price,
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
        "effective_pause_buy_position_notional": getattr(effective_args, "pause_buy_position_notional", None),
        "effective_pause_short_position_notional": getattr(effective_args, "pause_short_position_notional", None),
        "effective_inventory_pause_long_probe_scale": getattr(effective_args, "inventory_pause_long_probe_scale", None),
        "effective_inventory_pause_short_probe_scale": getattr(effective_args, "inventory_pause_short_probe_scale", None),
        "effective_inventory_pause_timeout_seconds": getattr(effective_args, "inventory_pause_timeout_seconds", None),
        "effective_adverse_reduce_enabled": bool(getattr(effective_args, "adverse_reduce_enabled", False)),
        "effective_adverse_reduce_long_trigger_ratio": getattr(effective_args, "adverse_reduce_long_trigger_ratio", None),
        "effective_adverse_reduce_short_trigger_ratio": getattr(effective_args, "adverse_reduce_short_trigger_ratio", None),
        "effective_adverse_reduce_target_ratio": getattr(effective_args, "adverse_reduce_target_ratio", None),
        "effective_adverse_reduce_maker_timeout_seconds": getattr(effective_args, "adverse_reduce_maker_timeout_seconds", None),
        "effective_adverse_reduce_max_order_notional": getattr(effective_args, "adverse_reduce_max_order_notional", None),
        "effective_adverse_reduce_keep_probe_scale": getattr(effective_args, "adverse_reduce_keep_probe_scale", None),
        "effective_short_threshold_timeout_seconds": getattr(effective_args, "short_threshold_timeout_seconds", None),
        "effective_synthetic_residual_long_flat_notional": getattr(
            effective_args,
            "synthetic_residual_long_flat_notional",
            None,
        ),
        "effective_synthetic_residual_short_flat_notional": getattr(
            effective_args,
            "synthetic_residual_short_flat_notional",
            None,
        ),
        "effective_synthetic_tiny_long_residual_notional": getattr(
            effective_args,
            "synthetic_tiny_long_residual_notional",
            None,
        ),
        "effective_synthetic_tiny_short_residual_notional": getattr(
            effective_args,
            "synthetic_tiny_short_residual_notional",
            None,
        ),
        "effective_max_position_notional": getattr(effective_args, "max_position_notional", None),
        "effective_max_short_position_notional": getattr(effective_args, "max_short_position_notional", None),
        "effective_max_total_notional": getattr(effective_args, "max_total_notional", None),
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
        "effective_long_plan_qty": plan.get("effective_long_qty"),
        "effective_short_plan_qty": plan.get("effective_short_qty"),
        "residual_long_flat_notional": plan.get("residual_long_flat_notional"),
        "residual_long_flattened": bool(plan.get("residual_long_flattened")),
        "residual_long_flattened_qty": plan.get("residual_long_flattened_qty"),
        "residual_long_flattened_notional": plan.get("residual_long_flattened_notional"),
        "residual_short_flat_notional": plan.get("residual_short_flat_notional"),
        "residual_short_flattened": bool(plan.get("residual_short_flattened")),
        "residual_short_flattened_qty": plan.get("residual_short_flattened_qty"),
        "residual_short_flattened_notional": plan.get("residual_short_flattened_notional"),
        "synthetic_tiny_long_residual_notional": plan.get("synthetic_tiny_long_residual_notional"),
        "synthetic_tiny_short_residual_notional": plan.get("synthetic_tiny_short_residual_notional"),
        "dominant_long_with_tiny_short_residual": bool(plan.get("dominant_long_with_tiny_short_residual")),
        "dominant_short_with_tiny_long_residual": bool(plan.get("dominant_short_with_tiny_long_residual")),
        "synthetic_tp_only_watchdog": synthetic_tp_only_watchdog,
        "synthetic_inventory_exit_priority": synthetic_inventory_exit_priority,
        "bootstrap_orders": plan["bootstrap_orders"],
        "buy_orders": plan["buy_orders"],
        "sell_orders": plan["sell_orders"],
        "forced_reduce_orders": plan.get("forced_reduce_orders", []),
        "grid_anchor_price": plan.get("grid_anchor_price"),
        "direction_state": plan.get("direction_state"),
        "pair_credit_steps": plan.get("pair_credit_steps"),
        "recovery_mode": plan.get("recovery_mode"),
        "inventory_grid_risk_state": plan.get("risk_state"),
        "tail_cleanup_active": bool(plan.get("tail_cleanup_active")),
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
    effective_max_total_notional = _safe_float(plan_report.get("effective_max_total_notional"))
    validation = validate_plan_report(
        plan_report=plan_report,
        allow_symbol=args.symbol,
        max_new_orders=args.max_new_orders,
        max_total_notional=effective_max_total_notional if effective_max_total_notional > 0 else args.max_total_notional,
        cancel_stale=args.cancel_stale,
        max_plan_age_seconds=args.max_plan_age_seconds,
        now=datetime.now(timezone.utc),
        allow_dual_side_position=(strategy_mode == "hedge_neutral"),
    )

    symbol = str(plan_report.get("symbol", "")).upper().strip()
    step_price = _safe_float(plan_report.get("step_price"))
    initial_market_snapshot = _resolve_runner_market_snapshot(args, symbol=symbol)
    market_fetcher = _build_execution_market_snapshot_fetcher(
        symbol=symbol,
        initial_book=initial_market_snapshot,
        snapshot_loader=lambda: _resolve_runner_market_snapshot(args, symbol=symbol),
    )
    live_book = market_fetcher()
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
    validation["actions"] = preserve_queue_priority_in_execution_actions(
        actions=validation["actions"],
        live_bid_price=live_bid_price,
        live_ask_price=live_ask_price,
        tick_size=_safe_float((plan_report.get("symbol_info") or {}).get("tick_size")),
        min_qty=(plan_report.get("symbol_info") or {}).get("min_qty"),
        min_notional=(plan_report.get("symbol_info") or {}).get("min_notional"),
        step_size=(plan_report.get("symbol_info") or {}).get("step_size"),
    )

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
    elif _is_competition_inventory_grid_mode(strategy_mode):
        if dual_side_position:
            raise RuntimeError("competition inventory grid 策略要求账户处于单向持仓模式")
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
        elif _is_competition_inventory_grid_mode(strategy_mode):
            current_long_qty = max(current_actual_net_qty, 0.0)
            current_short_qty = max(-current_actual_net_qty, 0.0)
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
    elif _is_competition_inventory_grid_mode(strategy_mode):
        if abs(current_actual_net_qty - expected_actual_net_qty) > 1e-9:
            raise RuntimeError("当前净持仓与计划生成时不一致，请等待下一轮刷新")
    elif _is_one_way_short_mode(strategy_mode):
        if abs(current_short_qty - expected_short_qty) > 1e-9:
            raise RuntimeError("当前空头持仓与计划生成时不一致，请等待下一轮刷新")
    elif abs(current_long_qty - expected_long_qty) > 1e-9:
        raise RuntimeError("当前持仓与计划生成时不一致，请等待下一轮刷新")
    if strategy_mode == "hedge_neutral" and abs(current_short_qty - expected_short_qty) > 1e-9:
        raise RuntimeError("当前空头持仓与计划生成时不一致，请等待下一轮刷新")

    validation["actions"] = cap_reduce_only_place_orders_to_position(
        actions=validation["actions"],
        strategy_mode=strategy_mode,
        current_actual_net_qty=current_actual_net_qty,
        current_open_orders=current_open_orders,
    )
    report["reduce_only_position_cap"] = validation["actions"].get("reduce_only_position_cap")
    if validation["actions"]["place_count"] <= 0 and validation["actions"]["cancel_count"] <= 0:
        validation["errors"] = [
            item for item in validation["errors"] if "plan contains no actions to execute" not in item
        ]
        validation["ok"] = not validation["errors"]
        report["idle"] = True
        return report

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
        post_only = str(order.get("execution_type", "post_only")).strip().lower() != "aggressive"
        time_in_force = (
            "GTX"
            if post_only
            else (str(order.get("time_in_force", "GTC")).upper().strip() or "GTC")
        )
        reduce_only = _resolve_reduce_only_flag(
            strategy_mode=strategy_mode,
            side=side,
            role=role,
        )
        if order.get("force_reduce_only") is not None:
            reduce_only = bool(order.get("force_reduce_only"))
        last_exc: RuntimeError | None = None
        for attempt in range(args.maker_retries + 1):
            current_book = market_fetcher(force_refresh=attempt > 0)
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
                step_size=symbol_info.get("step_size"),
                post_only=post_only,
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
                            "time_in_force": time_in_force,
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
                    time_in_force=time_in_force,
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
                            "time_in_force": time_in_force,
                        },
                        "response": place_response,
                    }
                )
                last_exc = None
                break
            except RuntimeError as exc:
                last_exc = exc
                message = str(exc).lower()
                if "-4164" in message and "notional must be no smaller than 5" in message:
                    report["skipped_orders"].append(
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
                                "time_in_force": time_in_force,
                                "reduce_only": reduce_only,
                            },
                            "reason": {
                                "reason": "exchange_min_notional_reject",
                                "error": str(exc),
                            },
                        }
                    )
                    last_exc = None
                    break
                if reduce_only is True and _is_reduce_only_reject(exc):
                    report["skipped_orders"].append(
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
                                "time_in_force": time_in_force,
                                "reduce_only": reduce_only,
                            },
                            "reason": {
                                "reason": "reduce_only_rejected",
                                "error": str(exc),
                            },
                        }
                    )
                    last_exc = None
                    break
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
    _update_inventory_grid_order_refs(
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
    parser.add_argument("--strategy-mode", type=str, default="one_way_long", choices=("one_way_long", "one_way_short", "hedge_neutral", "synthetic_neutral", "inventory_target_neutral", "competition_inventory_grid"))
    parser.add_argument("--step-price", type=float, default=0.00002)
    parser.add_argument("--buy-levels", type=int, default=8)
    parser.add_argument("--sell-levels", type=int, default=8)
    parser.add_argument("--per-order-notional", type=float, default=12.6)
    parser.add_argument("--startup-entry-multiplier", type=float, default=1.0)
    parser.add_argument("--base-position-notional", type=float, default=75.6)
    parser.add_argument("--sticky-entry-levels", type=int, default=None)
    parser.add_argument("--synthetic-residual-long-flat-notional", type=float, default=None)
    parser.add_argument("--synthetic-residual-short-flat-notional", type=float, default=None)
    parser.add_argument("--synthetic-tiny-long-residual-notional", type=float, default=None)
    parser.add_argument("--synthetic-tiny-short-residual-notional", type=float, default=None)
    parser.add_argument("--static-buy-offset-steps", type=float, default=0.0)
    parser.add_argument("--static-sell-offset-steps", type=float, default=0.0)
    parser.add_argument("--market-bias-enabled", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--market-bias-max-shift-steps", type=float, default=0.75)
    parser.add_argument("--market-bias-signal-steps", type=float, default=2.0)
    parser.add_argument("--market-bias-drift-weight", type=float, default=0.65)
    parser.add_argument("--market-bias-return-weight", type=float, default=0.35)
    parser.add_argument("--market-bias-weak-buy-pause-enabled", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--market-bias-weak-buy-pause-threshold", type=float, default=0.15)
    parser.add_argument("--market-bias-strong-short-pause-enabled", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--market-bias-strong-short-pause-threshold", type=float, default=0.15)
    parser.add_argument("--market-bias-regime-switch-enabled", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--market-bias-regime-switch-confirm-cycles", type=int, default=2)
    parser.add_argument("--market-bias-regime-switch-weak-threshold", type=float, default=0.15)
    parser.add_argument("--market-bias-regime-switch-strong-threshold", type=float, default=0.15)
    parser.add_argument("--first-order-multiplier", type=float, default=4.0)
    parser.add_argument("--threshold-position-notional", type=float, default=0.0)
    parser.add_argument("--short-threshold-timeout-seconds", type=float, default=60.0)
    parser.add_argument("--inventory-pause-timeout-seconds", type=float, default=0.0)
    parser.add_argument("--near-market-entry-max-center-distance-steps", type=float, default=2.0)
    parser.add_argument("--grid-inventory-rebalance-min-center-distance-steps", type=float, default=3.0)
    parser.add_argument("--near-market-reentry-confirm-cycles", type=int, default=2)
    parser.add_argument("--threshold-reduce-target-ratio", type=float, default=0.0)
    parser.add_argument("--threshold-reduce-taker-timeout-seconds", type=float, default=60.0)
    parser.add_argument("--adverse-reduce-enabled", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--adverse-reduce-short-trigger-ratio", type=float, default=0.01)
    parser.add_argument("--adverse-reduce-long-trigger-ratio", type=float, default=0.01)
    parser.add_argument("--adverse-reduce-target-ratio", type=float, default=0.75)
    parser.add_argument("--adverse-reduce-maker-timeout-seconds", type=float, default=45.0)
    parser.add_argument("--adverse-reduce-max-order-notional", type=float, default=0.0)
    parser.add_argument("--adverse-reduce-keep-probe-scale", type=float, default=None)
    parser.add_argument("--max-order-position-notional", type=float, default=80.0)
    parser.add_argument("--center-price", type=float, default=None)
    parser.add_argument("--flat-start-enabled", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--warm-start-enabled", action=argparse.BooleanOptionalAction, default=True)
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
    parser.add_argument("--inventory-pause-long-probe-scale", type=float, default=0.0)
    parser.add_argument("--inventory-pause-short-probe-scale", type=float, default=0.25)
    parser.add_argument("--max-position-notional", type=float, default=None)
    parser.add_argument("--max-short-position-notional", type=float, default=None)
    parser.add_argument("--min-mid-price-for-buys", type=float, default=None)
    parser.add_argument("--buy-pause-amp-trigger-ratio", type=float, default=None)
    parser.add_argument("--buy-pause-down-return-trigger-ratio", type=float, default=None)
    parser.add_argument("--short-cover-pause-amp-trigger-ratio", type=float, default=None)
    parser.add_argument("--short-cover-pause-down-return-trigger-ratio", type=float, default=None)
    parser.add_argument("--take-profit-min-profit-ratio", type=float, default=None)
    parser.add_argument("--freeze-shift-abs-return-trigger-ratio", type=float, default=None)
    parser.add_argument("--adaptive-step-enabled", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--adaptive-step-30s-abs-return-ratio", type=float, default=0.0)
    parser.add_argument("--adaptive-step-30s-amplitude-ratio", type=float, default=0.0)
    parser.add_argument("--adaptive-step-1m-abs-return-ratio", type=float, default=0.0)
    parser.add_argument("--adaptive-step-1m-amplitude-ratio", type=float, default=0.0)
    parser.add_argument("--adaptive-step-3m-abs-return-ratio", type=float, default=0.0)
    parser.add_argument("--adaptive-step-5m-abs-return-ratio", type=float, default=0.0)
    parser.add_argument("--adaptive-step-max-scale", type=float, default=1.0)
    parser.add_argument("--adaptive-step-min-per-order-scale", type=float, default=1.0)
    parser.add_argument("--adaptive-step-min-position-limit-scale", type=float, default=1.0)
    parser.add_argument("--synthetic-trend-follow-enabled", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--synthetic-trend-follow-1m-abs-return-ratio", type=float, default=0.0)
    parser.add_argument("--synthetic-trend-follow-1m-amplitude-ratio", type=float, default=0.0)
    parser.add_argument("--synthetic-trend-follow-3m-abs-return-ratio", type=float, default=0.0)
    parser.add_argument("--synthetic-trend-follow-3m-amplitude-ratio", type=float, default=0.0)
    parser.add_argument("--synthetic-trend-follow-min-efficiency-ratio", type=float, default=0.0)
    parser.add_argument("--synthetic-trend-follow-reverse-delay-seconds", type=float, default=0.0)
    parser.add_argument("--synthetic-flow-sleeve-enabled", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--synthetic-flow-sleeve-trigger-notional", type=float, default=None)
    parser.add_argument("--synthetic-flow-sleeve-notional", type=float, default=0.0)
    parser.add_argument("--synthetic-flow-sleeve-levels", type=int, default=2)
    parser.add_argument("--synthetic-flow-sleeve-order-notional", type=float, default=None)
    parser.add_argument("--synthetic-flow-sleeve-max-loss-ratio", type=float, default=0.0025)
    parser.add_argument("--volume-long-v4-flow-sleeve-enabled", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--volume-long-v4-flow-sleeve-trigger-notional", type=float, default=None)
    parser.add_argument("--volume-long-v4-flow-sleeve-reduce-to-notional", type=float, default=None)
    parser.add_argument("--volume-long-v4-flow-sleeve-notional", type=float, default=0.0)
    parser.add_argument("--volume-long-v4-flow-sleeve-levels", type=int, default=2)
    parser.add_argument("--volume-long-v4-flow-sleeve-order-notional", type=float, default=None)
    parser.add_argument("--volume-long-v4-flow-sleeve-max-loss-ratio", type=float, default=0.0025)
    parser.add_argument("--exposure-escalation-enabled", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--exposure-escalation-notional", type=float, default=None)
    parser.add_argument("--exposure-escalation-hold-seconds", type=float, default=600.0)
    parser.add_argument("--exposure-escalation-target-notional", type=float, default=None)
    parser.add_argument("--exposure-escalation-max-loss-ratio", type=float, default=None)
    parser.add_argument("--exposure-escalation-hard-unrealized-loss-limit", type=float, default=None)
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
    parser.add_argument("--synthetic-center-fast-catchup-trigger-steps", type=float, default=6.0)
    parser.add_argument("--synthetic-center-fast-catchup-confirm-cycles", type=int, default=3)
    parser.add_argument("--synthetic-center-fast-catchup-shift-steps", type=int, default=2)
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
    parser.add_argument("--run-start-time", type=str, default=None)
    parser.add_argument("--run-end-time", type=str, default=None)
    parser.add_argument("--runtime-guard-stats-start-time", type=str, default=None)
    parser.add_argument("--rolling-hourly-loss-limit", type=float, default=None)
    parser.add_argument("--max-cumulative-notional", type=float, default=None)
    parser.add_argument("--max-actual-net-notional", type=float, default=None)
    parser.add_argument("--max-synthetic-drift-notional", type=float, default=None)
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
    exposure_escalation = (
        summary.get("exposure_escalation") if isinstance(summary.get("exposure_escalation"), dict) else {}
    )
    if exposure_escalation.get("enabled") and (
        exposure_escalation.get("active") or exposure_escalation.get("blocked_reason") != "disabled"
    ):
        print(
            "  exposure_escalation: "
            f"active={'yes' if exposure_escalation.get('active') else 'no'} "
            f"reason={exposure_escalation.get('reason') or exposure_escalation.get('blocked_reason') or '-'} "
            f"held={_float(exposure_escalation.get('held_seconds', 0.0))}s "
            f"target={_float(exposure_escalation.get('target_notional', 0.0))} "
            f"max_loss={_float(exposure_escalation.get('max_loss_ratio', 0.0))} "
            f"unrealized={_float(exposure_escalation.get('unrealized_pnl', 0.0))}"
        )
    if summary.get("runtime_status") and summary.get("runtime_status") != "running":
        print(
            "  runtime_guard: "
            f"status={summary.get('runtime_status')} "
            f"rolling_loss={_float(summary.get('rolling_hourly_loss', 0.0))} "
            f"gross={_float(summary.get('cumulative_gross_notional', 0.0))}"
        )
    if summary.get("stop_reason"):
        print(f"  stop_reason: {summary['stop_reason']}")
    if summary.get("residual_short_flattened"):
        print(
            "  residual_short_flattened: "
            f"qty={_float(summary.get('residual_short_flattened_qty', 0.0))} "
            f"notional={_float(summary.get('residual_short_flattened_notional', 0.0))} "
            f"threshold={_float(summary.get('residual_short_flat_notional', 0.0))}"
        )
    if summary.get("residual_long_flattened"):
        print(
            "  residual_long_flattened: "
            f"qty={_float(summary.get('residual_long_flattened_qty', 0.0))} "
            f"notional={_float(summary.get('residual_long_flattened_notional', 0.0))} "
            f"threshold={_float(summary.get('residual_long_flat_notional', 0.0))}"
        )
    if summary.get("volatility_buy_pause") or summary.get("short_cover_paused") or summary.get("shift_frozen"):
        print(
            "  market_guard: "
            f"buy_pause={'yes' if summary.get('volatility_buy_pause') else 'no'} "
            f"short_cover_pause={'yes' if summary.get('short_cover_paused') else 'no'} "
            f"shift_frozen={'yes' if summary.get('shift_frozen') else 'no'} "
            f"ret={summary.get('market_guard_return_ratio', 0.0) * 100:.2f}% "
            f"amp={summary.get('market_guard_amplitude_ratio', 0.0) * 100:.2f}%"
        )
    adaptive_step = summary.get("adaptive_step") if isinstance(summary.get("adaptive_step"), dict) else {}
    if adaptive_step.get("controls_active"):
        print(
            "  adaptive_step: "
            f"base={_price(_safe_float(adaptive_step.get('base_step_price')))} "
            f"effective={_price(_safe_float(adaptive_step.get('effective_step_price')))} "
            f"scale={_safe_float(adaptive_step.get('scale')):.2f} "
            f"reason={adaptive_step.get('reason')}"
        )
        print(
            "  adaptive_risk: "
            f"per_order_scale={_safe_float(adaptive_step.get('per_order_scale')):.2f} "
            f"position_limit_scale={_safe_float(adaptive_step.get('position_limit_scale')):.2f}"
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
    take_profit_guard = summary.get("take_profit_guard") if isinstance(summary.get("take_profit_guard"), dict) else {}
    if take_profit_guard.get("enabled") and (
        take_profit_guard.get("long_active")
        or take_profit_guard.get("short_active")
        or take_profit_guard.get("adjusted_sell_orders")
        or take_profit_guard.get("adjusted_buy_orders")
        or take_profit_guard.get("dropped_sell_orders")
        or take_profit_guard.get("dropped_buy_orders")
        or take_profit_guard.get("long_cost_basis_missing")
        or take_profit_guard.get("short_cost_basis_missing")
    ):
        print(
            "  tp_guard: "
            f"ratio={_float(take_profit_guard.get('effective_min_profit_ratio', 0.0))} "
            f"long_active={'yes' if take_profit_guard.get('long_active') else 'no'} "
            f"short_active={'yes' if take_profit_guard.get('short_active') else 'no'} "
            f"sell_adj={int(take_profit_guard.get('adjusted_sell_orders', 0) or 0)} "
            f"buy_adj={int(take_profit_guard.get('adjusted_buy_orders', 0) or 0)} "
            f"sell_drop={int(take_profit_guard.get('dropped_sell_orders', 0) or 0)} "
            f"buy_drop={int(take_profit_guard.get('dropped_buy_orders', 0) or 0)} "
            f"cost_missing="
            f"{'long' if take_profit_guard.get('long_cost_basis_missing') else ''}"
            f"{'short' if take_profit_guard.get('short_cost_basis_missing') else ''}"
        )
    volume_long_v4_delever = (
        summary.get("volume_long_v4_delever") if isinstance(summary.get("volume_long_v4_delever"), dict) else {}
    )
    if volume_long_v4_delever.get("active"):
        print(
            "  volume_delever: "
            f"stage={volume_long_v4_delever.get('stage')} "
            f"active_sells={int(volume_long_v4_delever.get('active_sell_order_count', 0) or 0)} "
            f"loss_steps={_safe_float(volume_long_v4_delever.get('loss_budget_steps', 0.0)):.2f} "
            f"loss_floor={_price(_safe_float(volume_long_v4_delever.get('loss_floor_price')))}"
        )
    volume_long_v4_flow_sleeve = (
        summary.get("volume_long_v4_flow_sleeve") if isinstance(summary.get("volume_long_v4_flow_sleeve"), dict) else {}
    )
    if volume_long_v4_flow_sleeve.get("enabled"):
        print(
            "  volume_flow: "
            f"active={'yes' if volume_long_v4_flow_sleeve.get('active') else 'no'} "
            f"placed={int(volume_long_v4_flow_sleeve.get('placed_order_count', 0) or 0)} "
            f"notional={_float(volume_long_v4_flow_sleeve.get('placed_notional', 0.0))} "
            f"release={int(volume_long_v4_flow_sleeve.get('released_take_profit_order_count', 0) or 0)} "
            f"floor={_price(_safe_float(volume_long_v4_flow_sleeve.get('loss_floor_price')))} "
            f"reason={volume_long_v4_flow_sleeve.get('blocked_reason') or '-'}"
        )
    if summary.get("strategy_mode") == "synthetic_neutral":
        print(
            "  synthetic: "
            f"actual_net={_float(summary.get('actual_net_qty', 0.0))} "
            f"virtual_net={_float(summary.get('synthetic_net_qty', 0.0))} "
            f"drift={_float(summary.get('synthetic_drift_qty', 0.0))} "
            f"unmatched_trades={int(summary.get('synthetic_unmatched_trade_count', 0) or 0)}"
        )
        if summary.get("effective_synthetic_tiny_long_residual_notional") or summary.get(
            "effective_synthetic_tiny_short_residual_notional"
        ):
            print(
                "  synthetic_residual: "
                f"tiny_long<={_float(summary.get('effective_synthetic_tiny_long_residual_notional', 0.0))} "
                f"tiny_short<={_float(summary.get('effective_synthetic_tiny_short_residual_notional', 0.0))} "
                f"dominant_short_tiny_long={'yes' if summary.get('dominant_short_with_tiny_long_residual') else 'no'} "
                f"dominant_long_tiny_short={'yes' if summary.get('dominant_long_with_tiny_short_residual') else 'no'}"
            )
        if summary.get("synthetic_tp_only_watchdog_candidate") or summary.get("synthetic_tp_only_watchdog_active"):
            print(
                "  synthetic_watchdog: "
                f"candidate={'yes' if summary.get('synthetic_tp_only_watchdog_candidate') else 'no'} "
                f"active={'yes' if summary.get('synthetic_tp_only_watchdog_active') else 'no'} "
                f"duration={_float(summary.get('synthetic_tp_only_watchdog_duration_seconds', 0.0))}s "
                f"buy_gap={_float(summary.get('synthetic_tp_only_watchdog_buy_gap_steps', 0.0))} "
                f"sell_gap={_float(summary.get('synthetic_tp_only_watchdog_sell_gap_steps', 0.0))}"
            )
            if summary.get("synthetic_tp_only_watchdog_reason"):
                print(f"  synthetic_watchdog_reason: {summary['synthetic_tp_only_watchdog_reason']}")
    if summary.get("market_bias_enabled"):
        print(
            "  market_bias: "
            f"{summary.get('market_bias_regime', '--')} "
            f"score={summary.get('market_bias_score', 0.0):+.2f} "
            f"buy_shift={summary.get('market_bias_buy_offset_steps', 0.0):+.2f} "
            f"sell_shift={summary.get('market_bias_sell_offset_steps', 0.0):+.2f} "
            f"drift={summary.get('market_bias_drift_steps', 0.0):+.2f} "
            f"ret_steps={summary.get('market_bias_return_steps', 0.0):+.2f}"
        )
        guard_flags: list[str] = []
        if summary.get("market_bias_weak_buy_pause_active"):
            guard_flags.append(
                f"weak_buy_pause=yes threshold={summary.get('market_bias_weak_buy_pause_threshold', 0.0):.2f}"
            )
        if summary.get("market_bias_strong_short_pause_active"):
            guard_flags.append(
                f"strong_short_pause=yes threshold={summary.get('market_bias_strong_short_pause_threshold', 0.0):.2f}"
            )
        if guard_flags:
            print("  market_bias_guard: " + " ".join(guard_flags))
    if summary.get("market_bias_regime_switch_enabled"):
        print(
            "  market_bias_switch: "
            f"requested={summary.get('requested_strategy_mode', '--')} "
            f"active={summary.get('market_bias_switch_active_mode', '--')} "
            f"candidate={summary.get('market_bias_switch_candidate_mode', '--')} "
            f"(pending={int(summary.get('market_bias_switch_pending_count', 0) or 0)}/"
            f"{int(summary.get('market_bias_switch_confirm_cycles', 0) or 0)})"
        )
        if summary.get("market_bias_switch_reason"):
            print(f"  switch_reason: {summary['market_bias_switch_reason']}")
    if summary.get("synthetic_trend_follow_enabled"):
        print(
            "  synthetic_trend: "
            f"mode={summary.get('synthetic_trend_follow_mode', '--')} "
            f"active={'yes' if summary.get('synthetic_trend_follow_active') else 'no'} "
            f"dir={summary.get('synthetic_trend_follow_direction', '--')} "
            f"delay={'yes' if summary.get('synthetic_trend_follow_reverse_delay_active') else 'no'}"
        )
        if summary.get("synthetic_trend_follow_reason"):
            print(f"  trend_reason: {summary['synthetic_trend_follow_reason']}")
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
    if args.sticky_entry_levels is not None and args.sticky_entry_levels < 0:
        raise SystemExit("--sticky-entry-levels must be >= 0")
    if args.synthetic_residual_long_flat_notional is not None and args.synthetic_residual_long_flat_notional < 0:
        raise SystemExit("--synthetic-residual-long-flat-notional must be >= 0")
    if args.synthetic_residual_short_flat_notional is not None and args.synthetic_residual_short_flat_notional < 0:
        raise SystemExit("--synthetic-residual-short-flat-notional must be >= 0")
    if args.synthetic_tiny_long_residual_notional is not None and args.synthetic_tiny_long_residual_notional < 0:
        raise SystemExit("--synthetic-tiny-long-residual-notional must be >= 0")
    if args.synthetic_tiny_short_residual_notional is not None and args.synthetic_tiny_short_residual_notional < 0:
        raise SystemExit("--synthetic-tiny-short-residual-notional must be >= 0")
    if args.static_buy_offset_steps < 0 or args.static_sell_offset_steps < 0:
        raise SystemExit("--static-buy-offset-steps and --static-sell-offset-steps must be >= 0")
    if args.market_bias_max_shift_steps < 0:
        raise SystemExit("--market-bias-max-shift-steps must be >= 0")
    if args.market_bias_signal_steps <= 0:
        raise SystemExit("--market-bias-signal-steps must be > 0")
    if args.market_bias_drift_weight < 0 or args.market_bias_return_weight < 0:
        raise SystemExit("--market-bias-* weights must be >= 0")
    if args.market_bias_enabled and (args.market_bias_drift_weight + args.market_bias_return_weight) <= 0:
        raise SystemExit("--market-bias-enabled requires at least one positive weight")
    if args.market_bias_weak_buy_pause_threshold < 0:
        raise SystemExit("--market-bias-weak-buy-pause-threshold must be >= 0")
    if args.market_bias_weak_buy_pause_enabled and not args.market_bias_enabled:
        raise SystemExit("--market-bias-weak-buy-pause-enabled requires --market-bias-enabled")
    if args.market_bias_strong_short_pause_threshold < 0:
        raise SystemExit("--market-bias-strong-short-pause-threshold must be >= 0")
    if args.market_bias_strong_short_pause_enabled and not args.market_bias_enabled:
        raise SystemExit("--market-bias-strong-short-pause-enabled requires --market-bias-enabled")
    if args.market_bias_regime_switch_confirm_cycles <= 0:
        raise SystemExit("--market-bias-regime-switch-confirm-cycles must be > 0")
    if args.market_bias_regime_switch_weak_threshold < 0:
        raise SystemExit("--market-bias-regime-switch-weak-threshold must be >= 0")
    if args.market_bias_regime_switch_strong_threshold < 0:
        raise SystemExit("--market-bias-regime-switch-strong-threshold must be >= 0")
    if args.market_bias_regime_switch_enabled and not args.market_bias_enabled:
        raise SystemExit("--market-bias-regime-switch-enabled requires --market-bias-enabled")
    if args.market_bias_regime_switch_enabled and str(args.strategy_mode).strip() != "synthetic_neutral":
        raise SystemExit("--market-bias-regime-switch-enabled currently requires --strategy-mode synthetic_neutral")
    if args.first_order_multiplier <= 0:
        raise SystemExit("--first-order-multiplier must be > 0")
    if args.threshold_position_notional < 0:
        raise SystemExit("--threshold-position-notional must be >= 0")
    if args.near_market_entry_max_center_distance_steps < 0:
        raise SystemExit("--near-market-entry-max-center-distance-steps must be >= 0")
    if args.grid_inventory_rebalance_min_center_distance_steps < 0:
        raise SystemExit("--grid-inventory-rebalance-min-center-distance-steps must be >= 0")
    if args.near_market_reentry_confirm_cycles <= 0:
        raise SystemExit("--near-market-reentry-confirm-cycles must be > 0")
    if args.short_threshold_timeout_seconds < 0:
        raise SystemExit("--short-threshold-timeout-seconds must be >= 0")
    if args.inventory_pause_timeout_seconds < 0:
        raise SystemExit("--inventory-pause-timeout-seconds must be >= 0")
    if args.adverse_reduce_short_trigger_ratio < 0 or args.adverse_reduce_long_trigger_ratio < 0:
        raise SystemExit("--adverse-reduce-*-trigger-ratio must be >= 0")
    if args.adverse_reduce_target_ratio < 0 or args.adverse_reduce_target_ratio >= 1:
        raise SystemExit("--adverse-reduce-target-ratio must be >= 0 and < 1")
    if args.adverse_reduce_maker_timeout_seconds < 0:
        raise SystemExit("--adverse-reduce-maker-timeout-seconds must be >= 0")
    if args.adverse_reduce_max_order_notional < 0:
        raise SystemExit("--adverse-reduce-max-order-notional must be >= 0")
    if args.adverse_reduce_keep_probe_scale is not None and (
        args.adverse_reduce_keep_probe_scale < 0 or args.adverse_reduce_keep_probe_scale > 1
    ):
        raise SystemExit("--adverse-reduce-keep-probe-scale must be within [0, 1]")
    if args.max_order_position_notional < 0:
        raise SystemExit("--max-order-position-notional must be >= 0")
    if args.max_position_notional is not None and args.max_position_notional <= 0:
        raise SystemExit("--max-position-notional must be > 0")
    if args.pause_short_position_notional is not None and args.pause_short_position_notional <= 0:
        raise SystemExit("--pause-short-position-notional must be > 0")
    if args.inventory_pause_long_probe_scale < 0 or args.inventory_pause_long_probe_scale > 1:
        raise SystemExit("--inventory-pause-long-probe-scale must be within [0, 1]")
    if args.inventory_pause_short_probe_scale < 0 or args.inventory_pause_short_probe_scale > 1:
        raise SystemExit("--inventory-pause-short-probe-scale must be within [0, 1]")
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
    if args.take_profit_min_profit_ratio is not None and args.take_profit_min_profit_ratio < 0:
        raise SystemExit("--take-profit-min-profit-ratio must be >= 0")
    if args.freeze_shift_abs_return_trigger_ratio is not None and args.freeze_shift_abs_return_trigger_ratio <= 0:
        raise SystemExit("--freeze-shift-abs-return-trigger-ratio must be > 0")
    if args.adaptive_step_30s_abs_return_ratio < 0 or args.adaptive_step_30s_amplitude_ratio < 0:
        raise SystemExit("--adaptive-step 30s thresholds must be >= 0")
    if args.adaptive_step_1m_abs_return_ratio < 0 or args.adaptive_step_1m_amplitude_ratio < 0:
        raise SystemExit("--adaptive-step 1m thresholds must be >= 0")
    if args.adaptive_step_3m_abs_return_ratio < 0 or args.adaptive_step_5m_abs_return_ratio < 0:
        raise SystemExit("--adaptive-step trend thresholds must be >= 0")
    if args.adaptive_step_enabled and args.adaptive_step_max_scale <= 1.0:
        raise SystemExit("--adaptive-step-max-scale must be > 1 when adaptive step is enabled")
    if args.adaptive_step_min_per_order_scale <= 0 or args.adaptive_step_min_per_order_scale > 1.0:
        raise SystemExit("--adaptive-step-min-per-order-scale must be within (0, 1]")
    if args.adaptive_step_min_position_limit_scale <= 0 or args.adaptive_step_min_position_limit_scale > 1.0:
        raise SystemExit("--adaptive-step-min-position-limit-scale must be within (0, 1]")
    if args.adaptive_step_enabled and not any(
        threshold > 0
        for threshold in (
            args.adaptive_step_30s_abs_return_ratio,
            args.adaptive_step_30s_amplitude_ratio,
            args.adaptive_step_1m_abs_return_ratio,
            args.adaptive_step_1m_amplitude_ratio,
            args.adaptive_step_3m_abs_return_ratio,
            args.adaptive_step_5m_abs_return_ratio,
        )
    ):
        raise SystemExit("adaptive step requires at least one positive shock/trend trigger threshold")
    if args.synthetic_trend_follow_1m_abs_return_ratio < 0 or args.synthetic_trend_follow_1m_amplitude_ratio < 0:
        raise SystemExit("--synthetic-trend-follow 1m thresholds must be >= 0")
    if args.synthetic_trend_follow_3m_abs_return_ratio < 0 or args.synthetic_trend_follow_3m_amplitude_ratio < 0:
        raise SystemExit("--synthetic-trend-follow 3m thresholds must be >= 0")
    if (
        args.synthetic_trend_follow_min_efficiency_ratio < 0
        or args.synthetic_trend_follow_min_efficiency_ratio > 1.0
    ):
        raise SystemExit("--synthetic-trend-follow-min-efficiency-ratio must be within [0, 1]")
    if args.synthetic_trend_follow_reverse_delay_seconds < 0:
        raise SystemExit("--synthetic-trend-follow-reverse-delay-seconds must be >= 0")
    if args.synthetic_trend_follow_enabled and str(args.strategy_mode).strip() != "synthetic_neutral":
        raise SystemExit("--synthetic-trend-follow-enabled currently requires --strategy-mode synthetic_neutral")
    if args.synthetic_trend_follow_enabled and not any(
        threshold > 0
        for threshold in (
            args.synthetic_trend_follow_1m_abs_return_ratio,
            args.synthetic_trend_follow_1m_amplitude_ratio,
            args.synthetic_trend_follow_3m_abs_return_ratio,
            args.synthetic_trend_follow_3m_amplitude_ratio,
        )
    ):
        raise SystemExit("synthetic trend follow requires at least one positive trigger threshold")
    if args.volume_long_v4_flow_sleeve_levels < 0:
        raise SystemExit("--volume-long-v4-flow-sleeve-levels must be >= 0")
    for value, label in (
        (args.volume_long_v4_flow_sleeve_trigger_notional, "--volume-long-v4-flow-sleeve-trigger-notional"),
        (args.volume_long_v4_flow_sleeve_reduce_to_notional, "--volume-long-v4-flow-sleeve-reduce-to-notional"),
        (args.volume_long_v4_flow_sleeve_order_notional, "--volume-long-v4-flow-sleeve-order-notional"),
        (args.volume_long_v4_flow_sleeve_max_loss_ratio, "--volume-long-v4-flow-sleeve-max-loss-ratio"),
    ):
        if value is not None and value < 0:
            raise SystemExit(f"{label} must be >= 0")
    if args.volume_long_v4_flow_sleeve_notional < 0:
        raise SystemExit("--volume-long-v4-flow-sleeve-notional must be >= 0")
    if args.exposure_escalation_hold_seconds < 0:
        raise SystemExit("--exposure-escalation-hold-seconds must be >= 0")
    for value, label in (
        (args.exposure_escalation_notional, "--exposure-escalation-notional"),
        (args.exposure_escalation_target_notional, "--exposure-escalation-target-notional"),
        (args.exposure_escalation_max_loss_ratio, "--exposure-escalation-max-loss-ratio"),
        (
            args.exposure_escalation_hard_unrealized_loss_limit,
            "--exposure-escalation-hard-unrealized-loss-limit",
        ),
    ):
        if value is not None and value < 0:
            raise SystemExit(f"{label} must be >= 0")
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
    if args.neutral_center_interval_minutes <= 0:
        raise SystemExit("--neutral-center-interval-minutes must be > 0")
    if args.synthetic_center_fast_catchup_trigger_steps < 0:
        raise SystemExit("--synthetic-center-fast-catchup-trigger-steps must be >= 0")
    if args.synthetic_center_fast_catchup_confirm_cycles <= 0:
        raise SystemExit("--synthetic-center-fast-catchup-confirm-cycles must be > 0")
    if args.synthetic_center_fast_catchup_shift_steps <= 0:
        raise SystemExit("--synthetic-center-fast-catchup-shift-steps must be > 0")
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
    normalize_runtime_guard_config(vars(args))

    plan_path = Path(args.plan_json)
    submit_report_path = Path(args.submit_report_json)
    summary_path = Path(args.summary_jsonl)
    audit_paths = build_audit_paths(summary_path)
    consecutive_errors = 0
    cycle = 0
    market_stream: FuturesMarketStream | None = None
    try:
        market_stream = FuturesMarketStream(args.symbol)
        market_stream.start()
        setattr(args, "market_stream", market_stream)
    except Exception as exc:
        setattr(args, "market_stream", None)
        print(f"[market-stream] disabled for {args.symbol}: {exc}")

    try:
        while True:
            cycle += 1
            cycle_started_at = datetime.now(timezone.utc)
            try:
                runtime_guard_summary = _maybe_handle_runtime_guard(
                    args=args,
                    cycle=cycle,
                    cycle_started_at=cycle_started_at,
                    summary_path=summary_path,
                )
                if runtime_guard_summary is not None:
                    _append_jsonl(summary_path, runtime_guard_summary)
                    _print_cycle_summary(runtime_guard_summary)
                    consecutive_errors = 0
                    if runtime_guard_summary.get("stop_triggered"):
                        break
                    if args.iterations and cycle >= args.iterations:
                        break
                    time.sleep(args.sleep_seconds)
                    continue
                plan_report = generate_plan_report(args)
                if args.reset_state:
                    args.reset_state = False
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
                runtime_guard_config = normalize_runtime_guard_config(vars(args))
                runtime_cumulative_gross_notional, runtime_pnl_events, runtime_stats_start_time = _load_futures_runtime_guard_inputs(
                    summary_path,
                    runtime_guard_stats_start_time=getattr(args, "runtime_guard_stats_start_time", None),
                    symbol=args.symbol.upper().strip(),
                    now=cycle_started_at,
                )
                runtime_guard_result = evaluate_runtime_guards(
                    config=runtime_guard_config,
                    now=cycle_started_at,
                    cumulative_gross_notional=runtime_cumulative_gross_notional,
                    pnl_events=runtime_pnl_events,
                )

                summary = {
                "ts": cycle_started_at.isoformat(),
                "cycle": cycle,
                "symbol": args.symbol.upper().strip(),
                "strategy_profile": str(getattr(args, "strategy_profile", AUTO_REGIME_STABLE_PROFILE)),
                "effective_strategy_profile": str(
                    plan_report.get("effective_strategy_profile") or getattr(args, "strategy_profile", AUTO_REGIME_STABLE_PROFILE)
                ),
                "effective_strategy_label": str(plan_report.get("effective_strategy_label") or ""),
                "strategy_mode": str(plan_report.get("strategy_mode") or getattr(args, "strategy_mode", "one_way_long")),
                "requested_strategy_mode": str(
                    plan_report.get("requested_strategy_mode") or getattr(args, "strategy_mode", "one_way_long")
                ),
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
                "short_threshold_timeout_active": bool((plan_report.get("short_threshold_timeout") or {}).get("timeout_active")),
                "short_threshold_timeout_duration_seconds": _safe_float(
                    (plan_report.get("short_threshold_timeout") or {}).get("duration_seconds")
                ),
                "short_threshold_timeout_hold_seconds": _safe_float(
                    (plan_report.get("short_threshold_timeout") or {}).get("hold_seconds")
                ),
                "effective_short_plan_qty": _safe_float(plan_report.get("effective_short_plan_qty")),
                "residual_long_flat_notional": _safe_float(plan_report.get("residual_long_flat_notional")),
                "residual_long_flattened": bool(plan_report.get("residual_long_flattened")),
                "residual_long_flattened_qty": _safe_float(plan_report.get("residual_long_flattened_qty")),
                "residual_long_flattened_notional": _safe_float(plan_report.get("residual_long_flattened_notional")),
                "residual_short_flat_notional": _safe_float(plan_report.get("residual_short_flat_notional")),
                "residual_short_flattened": bool(plan_report.get("residual_short_flattened")),
                "residual_short_flattened_qty": _safe_float(plan_report.get("residual_short_flattened_qty")),
                "residual_short_flattened_notional": _safe_float(plan_report.get("residual_short_flattened_notional")),
                "effective_synthetic_tiny_long_residual_notional": _safe_float(
                    plan_report.get("effective_synthetic_tiny_long_residual_notional")
                ),
                "effective_synthetic_tiny_short_residual_notional": _safe_float(
                    plan_report.get("effective_synthetic_tiny_short_residual_notional")
                ),
                "synthetic_tiny_long_residual_notional": _safe_float(plan_report.get("synthetic_tiny_long_residual_notional")),
                "synthetic_tiny_short_residual_notional": _safe_float(plan_report.get("synthetic_tiny_short_residual_notional")),
                "dominant_long_with_tiny_short_residual": bool(plan_report.get("dominant_long_with_tiny_short_residual")),
                "dominant_short_with_tiny_long_residual": bool(plan_report.get("dominant_short_with_tiny_long_residual")),
                "synthetic_tp_only_watchdog_candidate": bool(
                    ((plan_report.get("synthetic_tp_only_watchdog") or {}).get("candidate"))
                ),
                "synthetic_tp_only_watchdog_active": bool(
                    ((plan_report.get("synthetic_tp_only_watchdog") or {}).get("active"))
                ),
                "synthetic_tp_only_watchdog_duration_seconds": _safe_float(
                    (plan_report.get("synthetic_tp_only_watchdog") or {}).get("duration_seconds")
                ),
                "synthetic_tp_only_watchdog_buy_gap_steps": _safe_float(
                    (plan_report.get("synthetic_tp_only_watchdog") or {}).get("buy_gap_steps")
                ),
                "synthetic_tp_only_watchdog_sell_gap_steps": _safe_float(
                    (plan_report.get("synthetic_tp_only_watchdog") or {}).get("sell_gap_steps")
                ),
                "synthetic_tp_only_watchdog_reason": (plan_report.get("synthetic_tp_only_watchdog") or {}).get("reason"),
                "take_profit_guard": dict(plan_report.get("take_profit_guard") or {}),
                "volume_long_v4_delever": dict(plan_report.get("volume_long_v4_delever") or {}),
                "volume_long_v4_flow_sleeve": dict(plan_report.get("volume_long_v4_flow_sleeve") or {}),
                "exposure_escalation": dict(plan_report.get("exposure_escalation") or {}),
                "adverse_reduce_enabled": bool((plan_report.get("adverse_inventory_reduce") or {}).get("enabled")),
                "adverse_reduce_active": bool((plan_report.get("adverse_inventory_reduce") or {}).get("active")),
                "adverse_reduce_direction": str(
                    ((plan_report.get("adverse_inventory_reduce") or {}).get("direction", "") or "")
                ),
                "adverse_reduce_blocked_reason": (
                    (plan_report.get("adverse_inventory_reduce") or {}).get("blocked_reason")
                ),
                "adverse_reduce_short_adverse_ratio": _safe_float(
                    (plan_report.get("adverse_inventory_reduce") or {}).get("short_adverse_ratio")
                ),
                "adverse_reduce_placed_reduce_orders": int(
                    ((plan_report.get("adverse_inventory_reduce") or {}).get("placed_reduce_orders", 0) or 0)
                ),
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
                "market_bias_enabled": bool(((plan_report.get("market_bias") or {}).get("enabled"))),
                "market_bias_active": bool(((plan_report.get("market_bias") or {}).get("active"))),
                "market_bias_regime": str(((plan_report.get("market_bias") or {}).get("regime", "") or "")),
                "market_bias_score": _safe_float((plan_report.get("market_bias") or {}).get("bias_score")),
                "market_bias_shift_steps": _safe_float((plan_report.get("market_bias") or {}).get("shift_steps")),
                "market_bias_buy_offset_steps": _safe_float((plan_report.get("market_bias") or {}).get("buy_offset_steps")),
                "market_bias_sell_offset_steps": _safe_float((plan_report.get("market_bias") or {}).get("sell_offset_steps")),
                "market_bias_drift_steps": _safe_float((plan_report.get("market_bias") or {}).get("drift_steps")),
                "market_bias_return_steps": _safe_float((plan_report.get("market_bias") or {}).get("return_steps")),
                "market_bias_weak_buy_pause_enabled": bool(((plan_report.get("market_bias") or {}).get("weak_buy_pause_enabled"))),
                "market_bias_weak_buy_pause_active": bool(((plan_report.get("market_bias") or {}).get("weak_buy_pause_active"))),
                "market_bias_weak_buy_pause_threshold": _safe_float((plan_report.get("market_bias") or {}).get("weak_buy_pause_threshold")),
                "market_bias_strong_short_pause_enabled": bool(((plan_report.get("market_bias") or {}).get("strong_short_pause_enabled"))),
                "market_bias_strong_short_pause_active": bool(((plan_report.get("market_bias") or {}).get("strong_short_pause_active"))),
                "market_bias_strong_short_pause_threshold": _safe_float((plan_report.get("market_bias") or {}).get("strong_short_pause_threshold")),
                "market_bias_regime_switch_enabled": bool(((plan_report.get("market_bias_regime_switch") or {}).get("enabled"))),
                "market_bias_switch_active_mode": str(((plan_report.get("market_bias_regime_switch") or {}).get("active_mode", "") or "")),
                "market_bias_switch_candidate_mode": str(((plan_report.get("market_bias_regime_switch") or {}).get("candidate_mode", "") or "")),
                "market_bias_switch_pending_count": int(((plan_report.get("market_bias_regime_switch") or {}).get("pending_count", 0) or 0)),
                "market_bias_switch_confirm_cycles": int(((plan_report.get("market_bias_regime_switch") or {}).get("confirm_cycles", 0) or 0)),
                "market_bias_switch_reason": (plan_report.get("market_bias_regime_switch") or {}).get("reason"),
                "market_bias_switch_switched": bool(((plan_report.get("market_bias_regime_switch") or {}).get("switched"))),
                "synthetic_trend_follow_enabled": bool(((plan_report.get("synthetic_trend_follow") or {}).get("enabled"))),
                "synthetic_trend_follow_active": bool(((plan_report.get("synthetic_trend_follow") or {}).get("active"))),
                "synthetic_trend_follow_mode": str(((plan_report.get("synthetic_trend_follow") or {}).get("mode", "") or "")),
                "synthetic_trend_follow_direction": str(((plan_report.get("synthetic_trend_follow") or {}).get("direction", "") or "")),
                "synthetic_trend_follow_reason": (plan_report.get("synthetic_trend_follow") or {}).get("reason"),
                "synthetic_trend_follow_reverse_delay_active": bool(
                    ((plan_report.get("synthetic_trend_follow") or {}).get("reverse_delay_active"))
                ),
                "synthetic_trend_follow_reverse_delay_remaining_seconds": _safe_float(
                    (plan_report.get("synthetic_trend_follow") or {}).get("reverse_delay_remaining_seconds")
                ),
                "synthetic_flow_sleeve_enabled": bool(((plan_report.get("synthetic_flow_sleeve") or {}).get("enabled"))),
                "synthetic_flow_sleeve_active": bool(((plan_report.get("synthetic_flow_sleeve") or {}).get("active"))),
                "synthetic_flow_sleeve_direction": str(
                    ((plan_report.get("synthetic_flow_sleeve") or {}).get("direction", "") or "")
                ),
                "synthetic_flow_sleeve_placed_order_count": int(
                    ((plan_report.get("synthetic_flow_sleeve") or {}).get("placed_order_count", 0) or 0)
                ),
                "synthetic_flow_sleeve_placed_notional": _safe_float(
                    (plan_report.get("synthetic_flow_sleeve") or {}).get("placed_notional")
                ),
                "synthetic_flow_sleeve_remaining_notional": _safe_float(
                    (plan_report.get("synthetic_flow_sleeve") or {}).get("remaining_sleeve_notional")
                ),
                "synthetic_flow_sleeve_blocked_reason": (
                    (plan_report.get("synthetic_flow_sleeve") or {}).get("blocked_reason")
                ),
                "volume_long_v4_flow_sleeve_enabled": bool(
                    ((plan_report.get("volume_long_v4_flow_sleeve") or {}).get("enabled"))
                ),
                "volume_long_v4_flow_sleeve_active": bool(
                    ((plan_report.get("volume_long_v4_flow_sleeve") or {}).get("active"))
                ),
                "volume_long_v4_flow_sleeve_placed_order_count": int(
                    ((plan_report.get("volume_long_v4_flow_sleeve") or {}).get("placed_order_count", 0) or 0)
                ),
                "volume_long_v4_flow_sleeve_placed_notional": _safe_float(
                    (plan_report.get("volume_long_v4_flow_sleeve") or {}).get("placed_notional")
                ),
                "volume_long_v4_flow_sleeve_released_take_profit_order_count": int(
                    ((plan_report.get("volume_long_v4_flow_sleeve") or {}).get("released_take_profit_order_count", 0) or 0)
                ),
                "volume_long_v4_flow_sleeve_released_take_profit_notional": _safe_float(
                    (plan_report.get("volume_long_v4_flow_sleeve") or {}).get("released_take_profit_notional")
                ),
                "volume_long_v4_flow_sleeve_remaining_notional": _safe_float(
                    (plan_report.get("volume_long_v4_flow_sleeve") or {}).get("remaining_sleeve_notional")
                ),
                "volume_long_v4_flow_sleeve_loss_floor_price": _safe_float(
                    (plan_report.get("volume_long_v4_flow_sleeve") or {}).get("loss_floor_price")
                ),
                "volume_long_v4_flow_sleeve_blocked_reason": (
                    (plan_report.get("volume_long_v4_flow_sleeve") or {}).get("blocked_reason")
                ),
                "exposure_escalation_enabled": bool(
                    ((plan_report.get("exposure_escalation") or {}).get("enabled"))
                ),
                "exposure_escalation_active": bool(
                    ((plan_report.get("exposure_escalation") or {}).get("active"))
                ),
                "exposure_escalation_reason": (
                    (plan_report.get("exposure_escalation") or {}).get("reason")
                    or (plan_report.get("exposure_escalation") or {}).get("blocked_reason")
                ),
                "exposure_escalation_held_seconds": _safe_float(
                    (plan_report.get("exposure_escalation") or {}).get("held_seconds")
                ),
                "exposure_escalation_unrealized_pnl": _safe_float(
                    (plan_report.get("exposure_escalation") or {}).get("unrealized_pnl")
                ),
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
                "runtime_status": runtime_guard_result.runtime_status,
                "stop_triggered": runtime_guard_result.stop_triggered,
                "stop_reason": runtime_guard_result.primary_reason,
                "stop_reasons": runtime_guard_result.matched_reasons,
                "stop_triggered_at": runtime_guard_result.triggered_at,
                "run_start_time": runtime_guard_config.run_start_time.isoformat() if runtime_guard_config.run_start_time else None,
                "run_end_time": runtime_guard_config.run_end_time.isoformat() if runtime_guard_config.run_end_time else None,
                "runtime_guard_stats_start_time": runtime_stats_start_time.isoformat() if runtime_stats_start_time else None,
                "rolling_hourly_loss": runtime_guard_result.rolling_hourly_loss,
                "rolling_hourly_loss_limit": runtime_guard_config.rolling_hourly_loss_limit,
                "cumulative_gross_notional": runtime_guard_result.cumulative_gross_notional,
                "max_cumulative_notional": runtime_guard_config.max_cumulative_notional,
            }
                _append_jsonl(summary_path, summary)
                _print_cycle_summary(summary)
                consecutive_errors = 0
            except KeyboardInterrupt:
                raise
            except Exception as exc:
                if isinstance(exc, StartupProtectionError):
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
                    raise SystemExit(str(exc)) from exc
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

            if args.iterations and cycle >= args.iterations:
                break
            time.sleep(args.sleep_seconds)
    finally:
        if market_stream is not None:
            market_stream.stop()


if __name__ == "__main__":
    main()
