#!/usr/bin/env python3
from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


BASE = Path(__file__).resolve().parents[1]
OUTPUT = BASE / "output"
LOG_PATH = OUTPUT / "competition_autotune_111.jsonl"
SYMBOLS = ("BTCUSDC", "ETHUSDC", "XAGUSDT")
LOW_LOSS_PER_10K = 0.5
TARGET_LOSS_PER_10K = 0.8
HIGH_LOSS_PER_10K = 1.0
BTC_BEST_QUOTE_MIN_GROSS_15M = 8_000.0
ETH_SWITCH_MIN_RELATIVE_VOLUME = 0.30
ROLLING_LOSS_LIMITS = {
    "BTCUSDC": 18.0,
    "ETHUSDC": 12.0,
    "XAGUSDT": 4.0,
}
RECOVERABLE_STABLE_SYMBOLS = {"BTCUSDC", "XAGUSDT"}
RECOVERY_MAX_1M_AMP = {
    "BTCUSDC": 0.0012,
    "XAGUSDT": 0.0035,
}


@dataclass(frozen=True)
class SymbolFiles:
    symbol: str
    slug: str
    control: Path
    plan: Path
    events: Path
    state: Path


def _slug(symbol: str) -> str:
    return symbol.lower()


def _files(symbol: str) -> SymbolFiles:
    slug = _slug(symbol)
    return SymbolFiles(
        symbol=symbol,
        slug=slug,
        control=OUTPUT / f"{slug}_loop_runner_control.json",
        plan=OUTPUT / f"{slug}_loop_latest_plan.json",
        events=OUTPUT / f"{slug}_loop_events.jsonl",
        state=OUTPUT / f"{slug}_loop_state.json",
    )


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _read_last_jsonl(path: Path, max_lines: int = 200) -> dict[str, Any]:
    try:
        with path.open("rb") as handle:
            handle.seek(0, 2)
            end = handle.tell()
            size = min(end, 256 * 1024)
            handle.seek(end - size)
            text = handle.read().decode("utf-8", errors="ignore")
    except OSError:
        return {}
    for line in reversed(text.splitlines()[-max_lines:]):
        line = line.strip()
        if not line:
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            return payload
    return {}


def _read_recent_jsonl(path: Path, max_lines: int = 600) -> list[dict[str, Any]]:
    try:
        with path.open("rb") as handle:
            handle.seek(0, 2)
            end = handle.tell()
            size = min(end, 1024 * 1024)
            handle.seek(end - size)
            text = handle.read().decode("utf-8", errors="ignore")
    except OSError:
        return []
    records: list[dict[str, Any]] = []
    for line in text.splitlines()[-max_lines:]:
        line = line.strip()
        if not line:
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            records.append(payload)
    return records


def _parse_event_ts(payload: dict[str, Any]) -> datetime | None:
    raw = payload.get("ts") or payload.get("timestamp") or payload.get("time")
    if not raw:
        return None
    try:
        text = str(raw).replace("Z", "+00:00")
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _window_gross_from_events(records: list[dict[str, Any]], latest_event: dict[str, Any], minutes: int) -> float:
    latest_ts = _parse_event_ts(latest_event)
    latest_gross = _safe_float(latest_event.get("cumulative_gross_notional"))
    if latest_ts is None or latest_gross <= 0:
        return 0.0
    cutoff = latest_ts - timedelta(minutes=minutes)
    baseline_gross: float | None = None
    for record in records:
        record_ts = _parse_event_ts(record)
        if record_ts is None or record_ts > cutoff:
            continue
        gross = _safe_float(record.get("cumulative_gross_notional"))
        if gross > 0:
            baseline_gross = gross
    if baseline_gross is None:
        first_gross = next((_safe_float(item.get("cumulative_gross_notional")) for item in records), 0.0)
        baseline_gross = first_gross if first_gross > 0 else latest_gross
    return max(0.0, latest_gross - baseline_gross)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _nested(payload: dict[str, Any], *keys: str) -> Any:
    current: Any = payload
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _plan_metrics(
    plan: dict[str, Any], latest_event: dict[str, Any], recent_events: list[dict[str, Any]] | None = None
) -> dict[str, float | bool | str | None]:
    elastic_metrics = _nested(plan, "elastic_volume", "metrics") or {}
    volatility = plan.get("volatility_entry_pause") if isinstance(plan.get("volatility_entry_pause"), dict) else {}
    recovery = plan.get("runtime_guard_loss_recovery") if isinstance(plan.get("runtime_guard_loss_recovery"), dict) else {}
    market = recovery.get("market") if isinstance(recovery.get("market"), dict) else {}
    loss_15m = _safe_float(elastic_metrics.get("loss_per_10k_15m"))
    loss_5m = _safe_float(elastic_metrics.get("loss_per_10k_5m"))
    gross_15m = _safe_float(elastic_metrics.get("gross_notional_15m"))
    gross_5m = _safe_float(elastic_metrics.get("gross_notional_5m"))
    if gross_15m <= 0 and recent_events:
        gross_15m = _window_gross_from_events(recent_events, latest_event, 15)
    if gross_5m <= 0 and recent_events:
        gross_5m = _window_gross_from_events(recent_events, latest_event, 5)
    rolling_loss = _safe_float(latest_event.get("rolling_hourly_loss"), _safe_float(plan.get("rolling_hourly_loss")))
    cumulative_gross = _safe_float(
        latest_event.get("cumulative_gross_notional"),
        _safe_float(plan.get("cumulative_gross_notional")),
    )
    rolling_per_10k = rolling_loss / cumulative_gross * 10000.0 if cumulative_gross > 0 else 0.0
    amp_1m = _safe_float(_nested(volatility, "metrics", "window_1m", "amplitude_ratio"))
    amp_3m = _safe_float(_nested(volatility, "metrics", "window_3m", "amplitude_ratio"))
    market_stable = bool(recovery.get("market_stable")) or (
        bool(plan.get("center_price")) and amp_1m <= 0.0012 and amp_3m <= 0.003
    )
    return {
        "loss_15m": loss_15m,
        "loss_5m": loss_5m,
        "gross_15m": gross_15m,
        "gross_5m": gross_5m,
        "rolling_loss": rolling_loss,
        "rolling_per_10k": rolling_per_10k,
        "cumulative_gross": cumulative_gross,
        "runtime_status": str(latest_event.get("runtime_status") or plan.get("runtime_status") or ""),
        "stop_reason": latest_event.get("stop_reason") or plan.get("stop_reason"),
        "strategy_mode": str(latest_event.get("strategy_mode") or plan.get("strategy_mode") or ""),
        "effective_strategy_profile": str(
            latest_event.get("effective_strategy_profile") or plan.get("effective_strategy_profile") or ""
        ),
        "actual_net_notional": _safe_float(latest_event.get("actual_net_notional"), _safe_float(plan.get("actual_net_notional"))),
        "volatility_pause_active": bool(_nested(volatility, "active")),
        "volatility_pause_reason": _nested(volatility, "reason"),
        "market_stable": market_stable,
        "recovery_flat": bool(recovery.get("flat")),
        "market_amp_1m": _safe_float(market.get("amplitude_1m"), amp_1m),
        "market_amp_3m": _safe_float(market.get("amplitude_3m"), amp_3m),
    }


PROFILES: dict[str, dict[str, dict[str, Any]]] = {
    "BTCUSDC": {
        "aggressive": {
            "strategy_profile": "btcusdc_best_quote_maker_volume_v1",
            "strategy_mode": "best_quote_maker_volume_v1",
            "best_quote_maker_volume_enabled": True,
            "best_quote_maker_volume_cycle_budget_notional": 500.0,
            "best_quote_maker_volume_target_remaining_notional": 1_000_000.0,
            "best_quote_maker_volume_quote_offset_ticks": 0,
            "best_quote_maker_volume_defensive_offset_ticks": 3,
            "best_quote_maker_volume_max_long_notional": 1300.0,
            "best_quote_maker_volume_max_short_notional": 1300.0,
            "best_quote_maker_volume_inventory_soft_ratio": 0.50,
            "best_quote_maker_volume_loss_per_10k_soft": 0.35,
            "best_quote_maker_volume_loss_per_10k_hard": 0.8,
            "best_quote_maker_volume_soft_loss_budget_scale": 0.35,
            "best_quote_maker_volume_min_cycle_budget_notional": 20.0,
            "step_price": 0.1,
            "per_order_notional": 120.0,
            "buy_levels": 1,
            "sell_levels": 1,
            "max_new_orders": 8,
            "max_total_notional": 2400.0,
            "max_position_notional": 1300.0,
            "max_short_position_notional": 1300.0,
            "pause_buy_position_notional": 850.0,
            "pause_short_position_notional": 850.0,
            "threshold_position_notional": 950.0,
            "max_actual_net_notional": 1000.0,
            "rolling_hourly_loss_limit": 18.0,
            "flat_start_enabled": False,
            "warm_start_enabled": True,
            "elastic_volume_enabled": False,
            "adaptive_step_enabled": False,
            "volatility_entry_pause_enabled": True,
            "volatility_entry_pause_10s_abs_return_ratio": 0.00060,
            "volatility_entry_pause_10s_amplitude_ratio": 0.00090,
            "volatility_entry_pause_30s_abs_return_ratio": 0.00100,
            "volatility_entry_pause_30s_amplitude_ratio": 0.00155,
            "volatility_entry_pause_1m_abs_return_ratio": 0.00160,
            "volatility_entry_pause_1m_amplitude_ratio": 0.00230,
            "volatility_entry_pause_3m_abs_return_ratio": 0.00300,
            "volatility_entry_pause_3m_amplitude_ratio": 0.00420,
            "volatility_entry_pause_recover_confirm_cycles": 2,
            "anti_chase_entry_guard_enabled": True,
            "anti_chase_entry_guard_1m_abs_return_ratio": 0.0020,
            "anti_chase_entry_guard_1m_amplitude_ratio": 0.0030,
            "anti_chase_entry_guard_3m_abs_return_ratio": 0.0050,
            "anti_chase_entry_guard_3m_amplitude_ratio": 0.0070,
        },
        "conservative": {
            "strategy_profile": "btcusdc_best_quote_maker_volume_v1",
            "strategy_mode": "best_quote_maker_volume_v1",
            "best_quote_maker_volume_enabled": True,
            "best_quote_maker_volume_cycle_budget_notional": 140.0,
            "best_quote_maker_volume_target_remaining_notional": 1_000_000.0,
            "best_quote_maker_volume_quote_offset_ticks": 0,
            "best_quote_maker_volume_defensive_offset_ticks": 5,
            "best_quote_maker_volume_max_long_notional": 500.0,
            "best_quote_maker_volume_max_short_notional": 500.0,
            "best_quote_maker_volume_inventory_soft_ratio": 0.35,
            "best_quote_maker_volume_loss_per_10k_soft": 0.30,
            "best_quote_maker_volume_loss_per_10k_hard": 0.60,
            "best_quote_maker_volume_soft_loss_budget_scale": 0.25,
            "best_quote_maker_volume_min_cycle_budget_notional": 20.0,
            "step_price": 0.1,
            "per_order_notional": 80.0,
            "buy_levels": 1,
            "sell_levels": 1,
            "max_new_orders": 4,
            "max_total_notional": 900.0,
            "max_position_notional": 500.0,
            "max_short_position_notional": 500.0,
            "pause_buy_position_notional": 280.0,
            "pause_short_position_notional": 280.0,
            "threshold_position_notional": 350.0,
            "max_actual_net_notional": 350.0,
            "rolling_hourly_loss_limit": 18.0,
            "flat_start_enabled": False,
            "warm_start_enabled": True,
            "elastic_volume_enabled": False,
            "adaptive_step_enabled": False,
            "volatility_entry_pause_enabled": True,
            "volatility_entry_pause_10s_abs_return_ratio": 0.00035,
            "volatility_entry_pause_10s_amplitude_ratio": 0.00055,
            "volatility_entry_pause_30s_abs_return_ratio": 0.00055,
            "volatility_entry_pause_30s_amplitude_ratio": 0.00090,
            "volatility_entry_pause_1m_abs_return_ratio": 0.00090,
            "volatility_entry_pause_1m_amplitude_ratio": 0.00140,
            "volatility_entry_pause_3m_abs_return_ratio": 0.00200,
            "volatility_entry_pause_3m_amplitude_ratio": 0.00300,
            "volatility_entry_pause_recover_confirm_cycles": 2,
            "anti_chase_entry_guard_enabled": True,
            "anti_chase_entry_guard_1m_abs_return_ratio": 0.0018,
            "anti_chase_entry_guard_1m_amplitude_ratio": 0.0026,
            "anti_chase_entry_guard_3m_abs_return_ratio": 0.0045,
            "anti_chase_entry_guard_3m_amplitude_ratio": 0.0065,
        },
    },
    "ETHUSDC": {
        "aggressive": {
            "step_price": 2.05,
            "per_order_notional": 1050.0,
            "buy_levels": 15,
            "sell_levels": 15,
            "max_new_orders": 96,
            "max_position_notional": 15000.0,
            "max_short_position_notional": 15000.0,
            "pause_buy_position_notional": 13000.0,
            "pause_short_position_notional": 13000.0,
            "threshold_position_notional": 20000.0,
            "max_total_notional": 45000.0,
            "rolling_hourly_loss_limit": 34.0,
            "max_actual_net_notional": 13000.0,
            "take_profit_min_profit_ratio": 0.00009,
            "elastic_loss_per_10k_sprint": 0.10,
            "elastic_loss_per_10k_cruise": 0.32,
            "elastic_loss_per_10k_defensive": 0.65,
            "adaptive_step_30s_abs_return_ratio": 0.0010,
            "adaptive_step_1m_abs_return_ratio": 0.0015,
            "volatility_entry_pause_30s_abs_return_ratio": 0.0012,
            "volatility_entry_pause_1m_abs_return_ratio": 0.0018,
        },
        "best_quote_candidate": {
            "strategy_profile": "ethusdc_best_quote_maker_volume_v1",
            "strategy_mode": "best_quote_maker_volume_v1",
            "best_quote_maker_volume_enabled": True,
            "best_quote_maker_volume_cycle_budget_notional": 380.0,
            "best_quote_maker_volume_target_remaining_notional": 1_000_000.0,
            "best_quote_maker_volume_quote_offset_ticks": 0,
            "best_quote_maker_volume_defensive_offset_ticks": 4,
            "best_quote_maker_volume_max_long_notional": 1200.0,
            "best_quote_maker_volume_max_short_notional": 1200.0,
            "best_quote_maker_volume_inventory_soft_ratio": 0.45,
            "best_quote_maker_volume_loss_per_10k_soft": 0.30,
            "best_quote_maker_volume_loss_per_10k_hard": 0.65,
            "best_quote_maker_volume_soft_loss_budget_scale": 0.30,
            "best_quote_maker_volume_min_cycle_budget_notional": 25.0,
            "step_price": 0.01,
            "per_order_notional": 90.0,
            "buy_levels": 1,
            "sell_levels": 1,
            "max_new_orders": 7,
            "max_total_notional": 2200.0,
            "max_position_notional": 1200.0,
            "max_short_position_notional": 1200.0,
            "pause_buy_position_notional": 720.0,
            "pause_short_position_notional": 720.0,
            "threshold_position_notional": 900.0,
            "max_actual_net_notional": 900.0,
            "rolling_hourly_loss_limit": 12.0,
            "flat_start_enabled": False,
            "warm_start_enabled": True,
            "elastic_volume_enabled": False,
            "adaptive_step_enabled": False,
            "volatility_entry_pause_enabled": True,
            "volatility_entry_pause_10s_abs_return_ratio": 0.00080,
            "volatility_entry_pause_10s_amplitude_ratio": 0.00120,
            "volatility_entry_pause_30s_abs_return_ratio": 0.00120,
            "volatility_entry_pause_30s_amplitude_ratio": 0.00180,
            "volatility_entry_pause_1m_abs_return_ratio": 0.00180,
            "volatility_entry_pause_1m_amplitude_ratio": 0.00260,
            "volatility_entry_pause_3m_abs_return_ratio": 0.00320,
            "volatility_entry_pause_3m_amplitude_ratio": 0.00480,
            "volatility_entry_pause_recover_confirm_cycles": 2,
            "anti_chase_entry_guard_enabled": True,
            "anti_chase_entry_guard_1m_abs_return_ratio": 0.0028,
            "anti_chase_entry_guard_1m_amplitude_ratio": 0.0040,
            "anti_chase_entry_guard_3m_abs_return_ratio": 0.0060,
            "anti_chase_entry_guard_3m_amplitude_ratio": 0.0085,
        },
        "conservative": {
            "step_price": 2.75,
            "per_order_notional": 550.0,
            "buy_levels": 10,
            "sell_levels": 10,
            "max_new_orders": 48,
            "max_position_notional": 6500.0,
            "max_short_position_notional": 6500.0,
            "pause_buy_position_notional": 4200.0,
            "pause_short_position_notional": 4200.0,
            "threshold_position_notional": 5200.0,
            "max_total_notional": 28000.0,
            "rolling_hourly_loss_limit": 10.0,
            "max_actual_net_notional": 4500.0,
            "take_profit_min_profit_ratio": 0.00016,
            "hard_loss_forced_reduce_enabled": True,
            "hard_loss_forced_reduce_target_notional": 350.0,
            "hard_loss_forced_reduce_max_order_notional": 160.0,
            "hard_loss_forced_reduce_unrealized_loss_limit": 2.5,
            "adverse_reduce_short_trigger_ratio": 0.0022,
            "adverse_reduce_long_trigger_ratio": 0.0022,
            "adverse_reduce_target_ratio": 0.45,
            "elastic_loss_per_10k_sprint": 0.08,
            "elastic_loss_per_10k_cruise": 0.25,
            "elastic_loss_per_10k_defensive": 0.50,
            "elastic_loss_per_10k_cooldown": 0.85,
            "elastic_step_scale_defensive": 1.8,
            "elastic_step_scale_cooldown": 3.0,
            "elastic_per_order_scale_defensive": 0.45,
            "elastic_levels_scale_defensive": 0.45,
            "adaptive_step_30s_abs_return_ratio": 0.00065,
            "adaptive_step_1m_abs_return_ratio": 0.0010,
            "volatility_entry_pause_30s_abs_return_ratio": 0.00075,
            "volatility_entry_pause_1m_abs_return_ratio": 0.0012,
        },
    },
    "XAGUSDT": {
        "aggressive": {
            "step_price": 0.16,
            "per_order_notional": 70.0,
            "buy_levels": 8,
            "sell_levels": 8,
            "max_new_orders": 24,
            "max_position_notional": 800.0,
            "max_short_position_notional": 800.0,
            "pause_buy_position_notional": 520.0,
            "pause_short_position_notional": 520.0,
            "threshold_position_notional": 650.0,
            "max_total_notional": 3200.0,
            "rolling_hourly_loss_limit": 4.0,
            "max_actual_net_notional": 480.0,
            "take_profit_min_profit_ratio": 0.0,
            "adaptive_step_30s_abs_return_ratio": 0.0010,
            "adaptive_step_1m_abs_return_ratio": 0.0016,
            "adaptive_step_1m_amplitude_ratio": 0.0027,
            "adaptive_step_3m_abs_return_ratio": 0.0036,
            "adaptive_step_3m_amplitude_ratio": 0.0055,
            "volatility_entry_pause_30s_abs_return_ratio": 0.0020,
            "volatility_entry_pause_1m_abs_return_ratio": 0.0032,
            "volatility_entry_pause_1m_amplitude_ratio": 0.0050,
            "volatility_entry_pause_3m_abs_return_ratio": 0.0065,
            "volatility_entry_pause_3m_amplitude_ratio": 0.0090,
            "hard_loss_forced_reduce_enabled": True,
            "hard_loss_forced_reduce_target_notional": 220.0,
            "hard_loss_forced_reduce_max_order_notional": 90.0,
            "hard_loss_forced_reduce_unrealized_loss_limit": 2.0,
            "adverse_reduce_short_trigger_ratio": 0.0042,
            "adverse_reduce_long_trigger_ratio": 0.0042,
            "adverse_reduce_target_ratio": 0.45,
            "elastic_loss_per_10k_sprint": 0.08,
            "elastic_loss_per_10k_cruise": 0.25,
            "elastic_loss_per_10k_defensive": 0.50,
            "elastic_loss_per_10k_cooldown": 0.85,
            "elastic_step_scale_defensive": 1.8,
            "elastic_step_scale_cooldown": 3.0,
            "elastic_per_order_scale_defensive": 0.45,
            "elastic_levels_scale_defensive": 0.45,
        },
        "conservative": {
            "step_price": 0.28,
            "per_order_notional": 22.0,
            "buy_levels": 4,
            "sell_levels": 4,
            "max_new_orders": 10,
            "max_position_notional": 240.0,
            "max_short_position_notional": 240.0,
            "pause_buy_position_notional": 130.0,
            "pause_short_position_notional": 130.0,
            "threshold_position_notional": 180.0,
            "max_total_notional": 900.0,
            "rolling_hourly_loss_limit": 1.5,
            "max_actual_net_notional": 130.0,
            "take_profit_min_profit_ratio": 0.0,
            "hard_loss_forced_reduce_enabled": True,
            "hard_loss_forced_reduce_target_notional": 40.0,
            "hard_loss_forced_reduce_max_order_notional": 18.0,
            "hard_loss_forced_reduce_unrealized_loss_limit": 0.25,
            "adverse_reduce_short_trigger_ratio": 0.0016,
            "adverse_reduce_long_trigger_ratio": 0.0016,
            "adverse_reduce_target_ratio": 0.18,
            "elastic_loss_per_10k_sprint": 0.08,
            "elastic_loss_per_10k_cruise": 0.25,
            "elastic_loss_per_10k_defensive": 0.50,
            "elastic_loss_per_10k_cooldown": 0.85,
            "elastic_step_scale_defensive": 2.5,
            "elastic_step_scale_cooldown": 4.0,
            "elastic_per_order_scale_defensive": 0.30,
            "elastic_levels_scale_defensive": 0.30,
            "adaptive_step_30s_abs_return_ratio": 0.00055,
            "adaptive_step_1m_abs_return_ratio": 0.0009,
            "adaptive_step_1m_amplitude_ratio": 0.0016,
            "adaptive_step_3m_abs_return_ratio": 0.0022,
            "adaptive_step_3m_amplitude_ratio": 0.0032,
            "volatility_entry_pause_30s_abs_return_ratio": 0.0008,
            "volatility_entry_pause_1m_abs_return_ratio": 0.0012,
            "volatility_entry_pause_1m_amplitude_ratio": 0.0020,
            "volatility_entry_pause_3m_abs_return_ratio": 0.0026,
            "volatility_entry_pause_3m_amplitude_ratio": 0.0038,
        },
    },
}


def _desired_mode(symbol: str, metrics: dict[str, Any], comparison: dict[str, Any] | None = None) -> tuple[str, str]:
    loss_15m = float(metrics["loss_15m"])
    loss_5m = float(metrics["loss_5m"])
    rolling_per_10k = float(metrics["rolling_per_10k"])
    market_stable = bool(metrics["market_stable"])
    runtime_status = str(metrics["runtime_status"])
    stop_reason = str(metrics["stop_reason"] or "")
    is_cooldown = runtime_status == "cooldown" or "rolling_hourly_loss" in stop_reason
    if float(metrics["rolling_loss"]) >= ROLLING_LOSS_LIMITS[symbol]:
        return "conservative", "rolling_hourly_loss_still_high"
    high_loss = max(loss_15m, loss_5m) > HIGH_LOSS_PER_10K
    if high_loss:
        return "conservative", "loss_per_10k_above_1"
    if symbol == "ETHUSDC" and comparison and comparison.get("eth_best_quote_switch_ready"):
        return "best_quote_candidate", "btc_best_quote_sample_good_switch_eth_candidate"
    if (
        symbol == "ETHUSDC"
        and comparison
        and comparison.get("eth_strategy") == "best_quote_maker_volume_v1"
        and max(loss_15m, loss_5m, rolling_per_10k) <= TARGET_LOSS_PER_10K
        and market_stable
    ):
        return "best_quote_candidate", "eth_best_quote_under_target_keep_running"
    if is_cooldown:
        if (
            symbol in RECOVERABLE_STABLE_SYMBOLS
            and market_stable
            and max(loss_15m, loss_5m, rolling_per_10k) < LOW_LOSS_PER_10K
            and float(metrics["market_amp_1m"]) <= RECOVERY_MAX_1M_AMP[symbol]
        ):
            return "conservative", f"recover_{symbol.lower()}_after_stable_cooldown"
        return "conservative", "runtime_loss_cooldown"
    if symbol == "XAGUSDT" and float(metrics["gross_15m"]) < 5_000.0:
        return "conservative", "xag_low_sample_keep_conservative"
    if max(loss_15m, loss_5m, rolling_per_10k) < LOW_LOSS_PER_10K and market_stable:
        return "aggressive", "loss_per_10k_below_0.5_and_stable"
    return "conservative", "middle_loss_or_unstable"


def _apply_profile(files: SymbolFiles, control: dict[str, Any], mode: str, reason: str, metrics: dict[str, Any]) -> bool:
    patch = dict(PROFILES[files.symbol][mode])
    if reason.startswith("recover_") and reason.endswith("_after_stable_cooldown"):
        patch["runtime_guard_stats_start_time"] = _now_iso()
        patch["reset_state"] = True
        patch["reset_state_reason"] = "autotune_recover_after_stable_loss_cooldown"
    changed = False
    for key, value in patch.items():
        if control.get(key) != value:
            control[key] = value
            changed = True
    if control.get("autotune_mode") != mode:
        control["autotune_mode"] = mode
        changed = True
    control["autotune_symbol_enabled"] = True
    control["updated_at"] = _now_iso()
    control["updated_by"] = "competition_autotune_111"
    control["autotune_reason"] = reason
    control["autotune_observed_loss_per_10k_15m"] = metrics["loss_15m"]
    control["autotune_observed_loss_per_10k_5m"] = metrics["loss_5m"]
    control["autotune_observed_rolling_loss_per_10k"] = metrics["rolling_per_10k"]
    if changed:
        _write_json(files.control, control)
    return changed


def _restart(symbol: str) -> bool:
    result = subprocess.run(
        ["systemctl", "restart", f"grid-loop@{symbol}.service"],
        cwd=str(BASE),
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError((result.stderr or result.stdout or f"restart failed for {symbol}").strip())
    return True


def _append_log(record: dict[str, Any]) -> None:
    with LOG_PATH.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")


def _symbol_snapshot(symbol: str) -> dict[str, Any]:
    files = _files(symbol)
    control = _read_json(files.control)
    plan = _read_json(files.plan)
    recent_events = _read_recent_jsonl(files.events)
    latest_event = _read_last_jsonl(files.events)
    if not latest_event and recent_events:
        latest_event = recent_events[-1]
    metrics = _plan_metrics(plan, latest_event, recent_events) if plan else {}
    return {
        "symbol": symbol,
        "files": files,
        "control": control,
        "plan": plan,
        "recent_events": recent_events,
        "latest_event": latest_event,
        "metrics": metrics,
    }


def _comparison(snapshots: dict[str, dict[str, Any]]) -> dict[str, Any]:
    btc = snapshots.get("BTCUSDC", {})
    eth = snapshots.get("ETHUSDC", {})
    btc_metrics = btc.get("metrics") if isinstance(btc.get("metrics"), dict) else {}
    eth_metrics = eth.get("metrics") if isinstance(eth.get("metrics"), dict) else {}
    btc_control = btc.get("control") if isinstance(btc.get("control"), dict) else {}
    eth_control = eth.get("control") if isinstance(eth.get("control"), dict) else {}

    btc_loss = max(
        _safe_float(btc_metrics.get("loss_15m")),
        _safe_float(btc_metrics.get("loss_5m")),
        _safe_float(btc_metrics.get("rolling_per_10k")),
    )
    eth_loss = max(
        _safe_float(eth_metrics.get("loss_15m")),
        _safe_float(eth_metrics.get("loss_5m")),
        _safe_float(eth_metrics.get("rolling_per_10k")),
    )
    btc_gross_15m = _safe_float(btc_metrics.get("gross_15m"))
    eth_gross_15m = _safe_float(eth_metrics.get("gross_15m"))
    relative_volume = btc_gross_15m / eth_gross_15m if eth_gross_15m > 0 else None
    btc_best_quote = (
        btc_control.get("strategy_mode") == "best_quote_maker_volume_v1"
        or btc_metrics.get("strategy_mode") == "best_quote_maker_volume_v1"
    )
    btc_sample_good = (
        btc_best_quote
        and btc_metrics.get("runtime_status") == "running"
        and not btc_metrics.get("volatility_pause_active")
        and btc_gross_15m >= BTC_BEST_QUOTE_MIN_GROSS_15M
        and btc_loss <= TARGET_LOSS_PER_10K
        and bool(btc_metrics.get("market_stable"))
    )
    volume_ok = relative_volume is None or relative_volume >= ETH_SWITCH_MIN_RELATIVE_VOLUME
    eth_best_quote_switch_ready = btc_sample_good and volume_ok
    if not btc_best_quote:
        switch_reason = "btc_not_best_quote"
    elif btc_metrics.get("runtime_status") != "running":
        switch_reason = "btc_not_running"
    elif btc_metrics.get("volatility_pause_active"):
        switch_reason = "btc_volatility_pause"
    elif btc_gross_15m < BTC_BEST_QUOTE_MIN_GROSS_15M:
        switch_reason = "btc_sample_gross_too_low"
    elif btc_loss > TARGET_LOSS_PER_10K:
        switch_reason = "btc_loss_above_target"
    elif not bool(btc_metrics.get("market_stable")):
        switch_reason = "btc_market_unstable"
    elif not volume_ok:
        switch_reason = "btc_volume_too_low_vs_eth"
    else:
        switch_reason = "ready"
    return {
        "target_loss_per_10k": TARGET_LOSS_PER_10K,
        "btc_strategy": btc_control.get("strategy_mode") or btc_metrics.get("strategy_mode"),
        "eth_strategy": eth_control.get("strategy_mode") or eth_metrics.get("strategy_mode"),
        "btc_loss_score": btc_loss,
        "eth_loss_score": eth_loss,
        "btc_gross_15m": btc_gross_15m,
        "eth_gross_15m": eth_gross_15m,
        "btc_to_eth_gross_15m_ratio": relative_volume,
        "eth_best_quote_switch_ready": eth_best_quote_switch_ready,
        "eth_switch_reason": switch_reason,
    }


def tune_snapshot(snapshot: dict[str, Any], comparison: dict[str, Any] | None = None) -> dict[str, Any]:
    symbol = str(snapshot["symbol"])
    files = snapshot["files"]
    control = snapshot.get("control") if isinstance(snapshot.get("control"), dict) else {}
    plan = snapshot.get("plan") if isinstance(snapshot.get("plan"), dict) else {}
    if not control:
        return {"symbol": symbol, "action": "skip", "reason": "missing_control"}
    if not plan:
        return {"symbol": symbol, "action": "skip", "reason": "missing_plan"}
    metrics = snapshot.get("metrics") if isinstance(snapshot.get("metrics"), dict) else {}
    mode, reason = _desired_mode(symbol, metrics, comparison)
    changed = _apply_profile(files, control, mode, reason, metrics)
    restarted = False
    if changed:
        restarted = _restart(symbol)
    return {
        "symbol": symbol,
        "action": "updated" if changed else "unchanged",
        "mode": mode,
        "reason": reason,
        "restarted": restarted,
        "metrics": metrics,
    }


def main() -> None:
    snapshots = {symbol: _symbol_snapshot(symbol) for symbol in SYMBOLS}
    comparison = _comparison(snapshots)
    records = []
    for symbol in SYMBOLS:
        try:
            records.append(tune_snapshot(snapshots[symbol], comparison))
        except Exception as exc:
            records.append({"symbol": symbol, "action": "error", "error": str(exc)})
    record = {"ts": _now_iso(), "comparison": comparison, "records": records}
    _append_log(record)
    print(json.dumps(record, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
