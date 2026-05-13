#!/usr/bin/env python3
from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


BASE = Path(__file__).resolve().parents[1]
OUTPUT = BASE / "output"
LOG_PATH = OUTPUT / "competition_autotune_111.jsonl"
SYMBOLS = ("BTCUSDC", "ETHUSDC", "XAGUSDT")
LOW_LOSS_PER_10K = 0.5
HIGH_LOSS_PER_10K = 1.0
ROLLING_LOSS_LIMITS = {
    "BTCUSDC": 14.0,
    "ETHUSDC": 10.0,
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


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _nested(payload: dict[str, Any], *keys: str) -> Any:
    current: Any = payload
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _plan_metrics(plan: dict[str, Any], latest_event: dict[str, Any]) -> dict[str, float | bool | str | None]:
    elastic_metrics = _nested(plan, "elastic_volume", "metrics") or {}
    volatility = plan.get("volatility_entry_pause") if isinstance(plan.get("volatility_entry_pause"), dict) else {}
    recovery = plan.get("runtime_guard_loss_recovery") if isinstance(plan.get("runtime_guard_loss_recovery"), dict) else {}
    market = recovery.get("market") if isinstance(recovery.get("market"), dict) else {}
    loss_15m = _safe_float(elastic_metrics.get("loss_per_10k_15m"))
    loss_5m = _safe_float(elastic_metrics.get("loss_per_10k_5m"))
    gross_15m = _safe_float(elastic_metrics.get("gross_notional_15m"))
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
        "rolling_loss": rolling_loss,
        "rolling_per_10k": rolling_per_10k,
        "cumulative_gross": cumulative_gross,
        "runtime_status": str(latest_event.get("runtime_status") or plan.get("runtime_status") or ""),
        "stop_reason": latest_event.get("stop_reason") or plan.get("stop_reason"),
        "market_stable": market_stable,
        "recovery_flat": bool(recovery.get("flat")),
        "market_amp_1m": _safe_float(market.get("amplitude_1m"), amp_1m),
        "market_amp_3m": _safe_float(market.get("amplitude_3m"), amp_3m),
    }


PROFILES: dict[str, dict[str, dict[str, Any]]] = {
    "BTCUSDC": {
        "aggressive": {
            "step_price": 30.0,
            "per_order_notional": 950.0,
            "buy_levels": 15,
            "sell_levels": 15,
            "max_new_orders": 104,
            "max_position_notional": 22000.0,
            "max_short_position_notional": 22000.0,
            "pause_buy_position_notional": 17000.0,
            "pause_short_position_notional": 17000.0,
            "threshold_position_notional": 21000.0,
            "max_total_notional": 50000.0,
            "rolling_hourly_loss_limit": 45.0,
            "max_actual_net_notional": 17000.0,
            "take_profit_min_profit_ratio": 0.00004,
            "elastic_loss_per_10k_sprint": 0.18,
            "elastic_loss_per_10k_cruise": 0.50,
            "elastic_loss_per_10k_defensive": 0.85,
            "adaptive_step_30s_abs_return_ratio": 0.0004,
            "adaptive_step_1m_abs_return_ratio": 0.00075,
            "volatility_entry_pause_30s_abs_return_ratio": 0.00035,
            "volatility_entry_pause_1m_abs_return_ratio": 0.00065,
        },
        "conservative": {
            "step_price": 42.0,
            "per_order_notional": 500.0,
            "buy_levels": 10,
            "sell_levels": 10,
            "max_new_orders": 56,
            "max_position_notional": 8000.0,
            "max_short_position_notional": 8000.0,
            "pause_buy_position_notional": 5200.0,
            "pause_short_position_notional": 5200.0,
            "threshold_position_notional": 6500.0,
            "max_total_notional": 30000.0,
            "rolling_hourly_loss_limit": 14.0,
            "max_actual_net_notional": 6000.0,
            "take_profit_min_profit_ratio": 0.00008,
            "hard_loss_forced_reduce_enabled": True,
            "hard_loss_forced_reduce_target_notional": 800.0,
            "hard_loss_forced_reduce_max_order_notional": 180.0,
            "hard_loss_forced_reduce_unrealized_loss_limit": 3.0,
            "adverse_reduce_short_trigger_ratio": 0.0018,
            "adverse_reduce_long_trigger_ratio": 0.0018,
            "adverse_reduce_target_ratio": 0.45,
            "elastic_loss_per_10k_sprint": 0.12,
            "elastic_loss_per_10k_cruise": 0.32,
            "elastic_loss_per_10k_defensive": 0.60,
            "elastic_loss_per_10k_cooldown": 0.9,
            "elastic_step_scale_defensive": 1.8,
            "elastic_step_scale_cooldown": 3.0,
            "elastic_per_order_scale_defensive": 0.45,
            "elastic_levels_scale_defensive": 0.45,
            "adaptive_step_30s_abs_return_ratio": 0.00022,
            "adaptive_step_1m_abs_return_ratio": 0.00050,
            "volatility_entry_pause_30s_abs_return_ratio": 0.00020,
            "volatility_entry_pause_1m_abs_return_ratio": 0.00045,
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
            "step_price": 0.08,
            "per_order_notional": 180.0,
            "buy_levels": 12,
            "sell_levels": 12,
            "max_new_orders": 48,
            "max_position_notional": 2800.0,
            "max_short_position_notional": 2800.0,
            "pause_buy_position_notional": 2100.0,
            "pause_short_position_notional": 2100.0,
            "threshold_position_notional": 2400.0,
            "max_total_notional": 10000.0,
            "rolling_hourly_loss_limit": 12.0,
            "max_actual_net_notional": 2000.0,
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
            "step_price": 0.16,
            "per_order_notional": 65.0,
            "buy_levels": 8,
            "sell_levels": 8,
            "max_new_orders": 24,
            "max_position_notional": 850.0,
            "max_short_position_notional": 850.0,
            "pause_buy_position_notional": 520.0,
            "pause_short_position_notional": 520.0,
            "threshold_position_notional": 650.0,
            "max_total_notional": 3600.0,
            "rolling_hourly_loss_limit": 4.0,
            "max_actual_net_notional": 520.0,
            "take_profit_min_profit_ratio": 0.0,
            "hard_loss_forced_reduce_enabled": True,
            "hard_loss_forced_reduce_target_notional": 120.0,
            "hard_loss_forced_reduce_max_order_notional": 45.0,
            "hard_loss_forced_reduce_unrealized_loss_limit": 0.8,
            "adverse_reduce_short_trigger_ratio": 0.0028,
            "adverse_reduce_long_trigger_ratio": 0.0028,
            "adverse_reduce_target_ratio": 0.35,
            "elastic_loss_per_10k_sprint": 0.08,
            "elastic_loss_per_10k_cruise": 0.25,
            "elastic_loss_per_10k_defensive": 0.50,
            "elastic_loss_per_10k_cooldown": 0.85,
            "elastic_step_scale_defensive": 2.2,
            "elastic_step_scale_cooldown": 3.5,
            "elastic_per_order_scale_defensive": 0.35,
            "elastic_levels_scale_defensive": 0.35,
            "adaptive_step_30s_abs_return_ratio": 0.00075,
            "adaptive_step_1m_abs_return_ratio": 0.0012,
            "adaptive_step_1m_amplitude_ratio": 0.0022,
            "adaptive_step_3m_abs_return_ratio": 0.0028,
            "adaptive_step_3m_amplitude_ratio": 0.0042,
            "volatility_entry_pause_30s_abs_return_ratio": 0.0015,
            "volatility_entry_pause_1m_abs_return_ratio": 0.0024,
            "volatility_entry_pause_1m_amplitude_ratio": 0.0038,
            "volatility_entry_pause_3m_abs_return_ratio": 0.0048,
            "volatility_entry_pause_3m_amplitude_ratio": 0.0070,
        },
    },
}


def _desired_mode(symbol: str, metrics: dict[str, Any]) -> tuple[str, str]:
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
    if is_cooldown:
        if (
            symbol in RECOVERABLE_STABLE_SYMBOLS
            and market_stable
            and max(loss_15m, loss_5m, rolling_per_10k) < LOW_LOSS_PER_10K
            and float(metrics["market_amp_1m"]) <= RECOVERY_MAX_1M_AMP[symbol]
        ):
            return "conservative", f"recover_{symbol.lower()}_after_stable_cooldown"
        return "conservative", "runtime_loss_cooldown"
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


def tune_symbol(symbol: str) -> dict[str, Any]:
    files = _files(symbol)
    control = _read_json(files.control)
    plan = _read_json(files.plan)
    latest_event = _read_last_jsonl(files.events)
    if not control:
        return {"symbol": symbol, "action": "skip", "reason": "missing_control"}
    if not plan:
        return {"symbol": symbol, "action": "skip", "reason": "missing_plan"}
    metrics = _plan_metrics(plan, latest_event)
    mode, reason = _desired_mode(symbol, metrics)
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
    records = []
    for symbol in SYMBOLS:
        try:
            records.append(tune_symbol(symbol))
        except Exception as exc:
            records.append({"symbol": symbol, "action": "error", "error": str(exc)})
    record = {"ts": _now_iso(), "records": records}
    _append_log(record)
    print(json.dumps(record, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
