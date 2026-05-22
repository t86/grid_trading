from __future__ import annotations

import argparse
import json
import shlex
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from .strategy_profile_schema import (
    BEST_QUOTE_CORE_PARAMS,
    COMMON_RUNNER_PARAMS,
    GLOBAL_SAFETY_PARAM_INFO,
    PROFILE_GLOBAL_SAFETY_PARAMS,
    apply_strategy_profile_schema,
)


REMOTE_READ_TIMEOUT_SECONDS = 25
REMOTE_WRITE_TIMEOUT_SECONDS = 25
REMOTE_RUNNER_TIMEOUT_SECONDS = 60
REMOTE_HISTORY_TIMEOUT_SECONDS = 25
REMOTE_ROLLBACK_TIMEOUT_SECONDS = 35
REMOTE_RUNTIME_TIMEOUT_SECONDS = 25
PHAROS_SYMBOL = "PHAROSUSDT"
PHAROS_PROFILE = "pharosusdt_hedge_best_quote_maker_volume_v1"

COMMON_EXECUTION_PARAMS = frozenset(COMMON_RUNNER_PARAMS - PROFILE_GLOBAL_SAFETY_PARAMS) | frozenset(
    {
        "profile",
        "mode",
        "max_new_orders",
        "max_total_notional",
        "cancel_stale",
        "max_mid_drift_steps",
    }
)

GLOBAL_SAFETY_PARAMS = (PROFILE_GLOBAL_SAFETY_PARAMS - COMMON_EXECUTION_PARAMS) | frozenset(GLOBAL_SAFETY_PARAM_INFO)

PHAROS_BEST_QUOTE_MAKER_VOLUME_PARAMS = frozenset(
    {
        "best_quote_maker_volume_enabled",
        "best_quote_maker_volume_cycle_budget_notional",
        "best_quote_maker_volume_target_remaining_notional",
        "best_quote_maker_volume_quote_offset_ticks",
        "best_quote_maker_volume_max_long_notional",
        "best_quote_maker_volume_max_short_notional",
        "volatility_entry_pause_enabled",
        "volatility_alert_email_enabled",
        "volatility_alert_1m_amplitude_ratio",
        "volatility_alert_15m_amplitude_ratio",
        "volatility_alert_60m_amplitude_ratio",
        "volatility_alert_increase_multiplier",
        "volatility_alert_cooldown_seconds",
        "anti_chase_entry_guard_enabled",
        "anti_chase_entry_guard_1m_abs_return_ratio",
        "anti_chase_entry_guard_1m_amplitude_ratio",
        "anti_chase_entry_guard_3m_abs_return_ratio",
        "anti_chase_entry_guard_3m_amplitude_ratio",
        "hard_loss_forced_reduce_target_notional",
        "hard_loss_forced_reduce_max_order_notional",
        "hard_loss_forced_reduce_unrealized_loss_limit",
    }
)

PHAROS_PROFILE_PARAMS = frozenset(BEST_QUOTE_CORE_PARAMS) | PHAROS_BEST_QUOTE_MAKER_VOLUME_PARAMS
PHAROS_PROFILE_PARAM_PREFIXES = frozenset(
    {
        "adaptive_regime_router_",
        "best_quote_maker_volume_",
        "loss_inventory_",
        "loss_recovery_brush_",
        "loss_reentry_",
        "sticky_entry_",
        "unrealized_loss_entry_guard_",
        "volatility_entry_pause_",
    }
)
PHAROS_GLOBAL_SAFETY_PARAM_PREFIXES = frozenset(
    {
        "rolling_hourly_loss_per_10k_",
        "volatility_trigger_",
        "volume_trigger_",
    }
)
PHAROS_COMMON_EXECUTION_PARAM_PREFIXES = frozenset(
    {
        "execution_cancel_",
        "execution_place_",
        "execution_request_",
    }
)
PHAROS_FORBIDDEN_PARAM_PREFIXES = frozenset(
    {
        "adaptive_step_",
        "adverse_reduce_",
        "autotune_symbol_",
        "elastic_",
        "execution_regime_",
        "exposure_escalation_",
        "inventory_tier_",
        "market_bias_",
        "neutral_",
        "regime_entry_budget_",
        "synthetic_",
    }
)
PHAROS_GLOBAL_SAFETY_EXACT_PARAMS = frozenset(
    {
        "near_market_reentry_confirm_cycles",
        "threshold_position_notional",
    }
)
PHAROS_COMMON_EXECUTION_EXACT_PARAMS = frozenset(
    {
        "startup_jitter_seconds",
    }
)
PHAROS_PROFILE_EXACT_PARAMS = frozenset(
    {
        "cycle_jitter_seconds",
        "strategy_label",
    }
)

PHAROS_FORBIDDEN_PARAMS = frozenset(
    {
        "auto_regime_enabled",
        "market_bias_enabled",
        "market_bias_regime_switch_enabled",
        "multi_timeframe_bias_enabled",
        "synthetic_flow_sleeve_enabled",
        "synthetic_trend_follow_enabled",
        "volume_long_v4_flow_sleeve_enabled",
        "custom_grid_enabled",
        "adaptive_step_enabled",
        "excess_inventory_reduce_only_enabled",
    }
)


@dataclass(frozen=True)
class CentralStrategyTarget:
    server_id: str
    symbol: str
    strategy_profile: str
    ssh_host: str
    control_path: str
    runner_wrapper: str
    common_params: tuple[str, ...]
    profile_params: tuple[str, ...]
    global_safety_params: tuple[str, ...]
    forbidden_params: tuple[str, ...]
    strategy_mode: str = "hedge_best_quote_maker_volume_v1"
    required_position_mode: str = "hedge"

    @property
    def scope(self) -> str:
        return f"{self.server_id}/{self.symbol}/{self.strategy_profile}"

    def as_payload(self) -> dict[str, Any]:
        return {
            "server_id": self.server_id,
            "symbol": self.symbol,
            "strategy_profile": self.strategy_profile,
            "strategy_mode": self.strategy_mode,
            "required_position_mode": self.required_position_mode,
            "ssh_host": self.ssh_host,
            "control_path": self.control_path,
            "runner_wrapper": self.runner_wrapper,
            "scope": {
                "server_id": self.server_id,
                "symbol": self.symbol,
                "strategy_profile": self.strategy_profile,
            },
            "common_params": list(self.common_params),
            "profile_params": list(self.profile_params),
            "global_safety_params": list(self.global_safety_params),
            "forbidden_params": list(self.forbidden_params),
        }

    def __getitem__(self, key: str) -> Any:
        return self.as_payload()[key]

    def scope_payload(self) -> dict[str, Any]:
        return {
            "server_id": self.server_id,
            "symbol": self.symbol,
            "strategy_profile": self.strategy_profile,
            "strategy_mode": self.strategy_mode,
            "required_position_mode": self.required_position_mode,
            "scope": self.scope,
        }


def _sorted_tuple(values: frozenset[str]) -> tuple[str, ...]:
    return tuple(sorted(values))


CENTRAL_STRATEGY_TARGETS: tuple[CentralStrategyTarget, ...] = (
    CentralStrategyTarget(
        server_id="114",
        symbol=PHAROS_SYMBOL,
        strategy_profile=PHAROS_PROFILE,
        ssh_host="srv-43-155-163-114",
        control_path="/home/ubuntu/wangge/output/pharosusdt_loop_runner_control.json",
        runner_wrapper="/usr/local/bin/grid-saved-runner",
        common_params=_sorted_tuple(COMMON_EXECUTION_PARAMS),
        profile_params=_sorted_tuple(PHAROS_PROFILE_PARAMS),
        global_safety_params=_sorted_tuple(GLOBAL_SAFETY_PARAMS),
        forbidden_params=_sorted_tuple(PHAROS_FORBIDDEN_PARAMS),
    ),
    CentralStrategyTarget(
        server_id="150",
        symbol=PHAROS_SYMBOL,
        strategy_profile=PHAROS_PROFILE,
        ssh_host="srv-43-131-232-150",
        control_path="/home/ubuntu/wangge_api2/output/pharosusdt_loop_runner_control.json",
        runner_wrapper="/usr/local/bin/grid-saved-runner-api2",
        common_params=_sorted_tuple(COMMON_EXECUTION_PARAMS),
        profile_params=_sorted_tuple(PHAROS_PROFILE_PARAMS),
        global_safety_params=_sorted_tuple(GLOBAL_SAFETY_PARAMS),
        forbidden_params=_sorted_tuple(PHAROS_FORBIDDEN_PARAMS),
    ),
)


def _target_key(server_id: Any, symbol: Any, strategy_profile: Any) -> tuple[str, str, str]:
    return (
        str(server_id or "").strip(),
        str(symbol or "").strip().upper(),
        str(strategy_profile or "").strip().lower(),
    )


def list_central_strategy_targets() -> list[CentralStrategyTarget]:
    return list(CENTRAL_STRATEGY_TARGETS)


def get_central_strategy_target(server_id: Any, symbol: Any, strategy_profile: Any) -> CentralStrategyTarget:
    requested = _target_key(server_id, symbol, strategy_profile)
    for target in CENTRAL_STRATEGY_TARGETS:
        if _target_key(target.server_id, target.symbol, target.strategy_profile) == requested:
            return target
    raise KeyError(f"unknown central strategy target: {requested[0]}/{requested[1]}/{requested[2]}")


def _jsonable(value: Any) -> Any:
    try:
        json.dumps(value)
    except (TypeError, ValueError):
        return str(value)
    return value


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _as_int(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _is_active_value(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        return value.strip() != ""
    if isinstance(value, (list, tuple, set, dict)):
        return bool(value)
    return True


def _section_item(key: str, value: Any, owner: str) -> dict[str, Any]:
    return {
        "key": key,
        "value": _jsonable(value),
        "active": _is_active_value(value),
        "owner": owner,
        "editable": owner in {"common", "profile", "global_safety"},
    }


def _empty_sections() -> dict[str, dict[str, dict[str, Any]]]:
    return {
        "common": {},
        "profile": {},
        "global_safety": {},
        "forbidden": {},
        "unknown": {},
    }


def _schema_report_for_config(config: dict[str, Any]) -> dict[str, Any]:
    schema_config = dict(config)
    schema_config.setdefault("symbol", PHAROS_SYMBOL)
    schema_config.setdefault("strategy_profile", schema_config.get("profile", PHAROS_PROFILE))
    schema_config.setdefault("strategy_mode", schema_config.get("mode", "hedge_best_quote_maker_volume_v1"))
    schema_config.setdefault("required_position_mode", "hedge")
    _, report = apply_strategy_profile_schema(argparse.Namespace(**schema_config), enabled=True)
    return report


def _normalize_config_for_target(target: CentralStrategyTarget, config: dict[str, Any], *, action: str) -> dict[str, Any]:
    if not isinstance(config, dict):
        raise ValueError("config must be object")
    normalized: dict[str, Any] = {}
    stripped_params: list[str] = []
    for key, value in config.items():
        key_str = str(key)
        if key_str.startswith("_"):
            stripped_params.append(key_str)
            continue
        if key_str in {"symbol", "strategy_profile", "strategy_mode", "required_position_mode"}:
            normalized[key_str] = value
            continue
        if _workbench_owner_for_key(target, key_str) in {"common", "profile", "global_safety"}:
            normalized[key_str] = value
            continue
        stripped_params.append(key_str)
    normalized["symbol"] = str(normalized.get("symbol") or target.symbol).upper().strip()
    normalized["strategy_profile"] = str(normalized.get("strategy_profile") or target.strategy_profile).strip()
    normalized["strategy_mode"] = str(normalized.get("strategy_mode") or target.strategy_mode).strip()
    normalized["required_position_mode"] = str(
        normalized.get("required_position_mode") or target.required_position_mode
    ).strip()
    if normalized["symbol"] != target.symbol:
        raise ValueError(f"symbol mismatch for target {target.scope}: {normalized['symbol']}")
    if normalized["strategy_profile"] != target.strategy_profile:
        raise ValueError(f"strategy_profile mismatch for target {target.scope}: {normalized['strategy_profile']}")
    if normalized["strategy_mode"] != target.strategy_mode:
        raise ValueError(f"strategy_mode mismatch for target {target.scope}: {normalized['strategy_mode']}")
    if normalized["required_position_mode"] != target.required_position_mode:
        raise ValueError(
            f"required_position_mode mismatch for target {target.scope}: {normalized['required_position_mode']}"
        )
    schema_report = _schema_report_for_config(normalized)
    if not bool(schema_report.get("strict_ok", False)):
        unknown_params = ", ".join(schema_report.get("unknown_params") or [])
        raise ValueError(f"config has active unknown params: {unknown_params}")
    forbidden_active = (schema_report.get("profile_boundary") or {}).get("forbidden_active_params") or []
    if forbidden_active:
        raise ValueError("config has active forbidden params: " + ", ".join(forbidden_active))
    normalized["_central_workbench_action"] = action
    normalized["_central_workbench_saved_at"] = datetime.now(timezone.utc).isoformat()
    normalized["_central_workbench_scope"] = target.scope
    normalized["_central_workbench_stripped_params"] = sorted(set(stripped_params))
    return normalized


def _starts_with_any(key: str, prefixes: frozenset[str]) -> bool:
    return any(key.startswith(prefix) for prefix in prefixes)


def _workbench_owner_for_key(target: CentralStrategyTarget, key: str) -> str:
    common_params = set(target.common_params)
    profile_params = set(target.profile_params)
    global_safety_params = set(target.global_safety_params)
    forbidden_params = set(target.forbidden_params)
    if key in forbidden_params:
        return "forbidden"
    if _starts_with_any(key, PHAROS_FORBIDDEN_PARAM_PREFIXES):
        return "forbidden"
    if (
        key in common_params
        or key in PHAROS_COMMON_EXECUTION_EXACT_PARAMS
        or _starts_with_any(key, PHAROS_COMMON_EXECUTION_PARAM_PREFIXES)
    ):
        return "common"
    if (
        key in global_safety_params
        or key in PHAROS_GLOBAL_SAFETY_EXACT_PARAMS
        or _starts_with_any(key, PHAROS_GLOBAL_SAFETY_PARAM_PREFIXES)
    ):
        return "global_safety"
    if (
        key in profile_params
        or key in PHAROS_PROFILE_EXACT_PARAMS
        or _starts_with_any(key, PHAROS_PROFILE_PARAM_PREFIXES)
    ):
        return "profile"
    return "unknown"


def build_pharos_runtime_summary(
    events: list[dict[str, Any]] | None,
    config: dict[str, Any],
    *,
    error: str = "",
) -> dict[str, Any]:
    valid_events = [event for event in events or [] if isinstance(event, dict)]
    if not valid_events:
        return {
            "ok": False,
            "state": "unknown",
            "state_label": "运行数据不足",
            "reason_codes": ["no_runtime_events"],
            "event_count": 0,
            "error": error,
            "latest": {},
            "window_gross_notional": 0.0,
        }

    first = valid_events[0]
    latest = valid_events[-1]
    threshold = _as_float(
        latest.get("threshold_position_notional"),
        _as_float(config.get("threshold_position_notional"), 0.0),
    )
    net_notional = abs(_as_float(latest.get("actual_net_notional")))
    unrealized_pnl = _as_float(latest.get("unrealized_pnl"), _as_float(latest.get("unrealized_loss")) * -1.0)
    rolling_loss_per_10k = _as_float(latest.get("rolling_hourly_loss_per_10k"))
    rolling_gross = _as_float(latest.get("rolling_hourly_gross_notional"))
    cumulative = _as_float(latest.get("cumulative_gross_notional"))
    first_cumulative = _as_float(first.get("cumulative_gross_notional"), cumulative)
    window_gross = max(cumulative - first_cumulative, 0.0)
    open_orders = _as_int(latest.get("open_order_count"))
    stale_orders = _as_int(latest.get("stale_order_count"))
    missing_orders = _as_int(latest.get("missing_order_count"))
    volatility_paused = bool(latest.get("volatility_entry_pause_active"))
    max_cumulative = _as_float(latest.get("max_cumulative_notional"), _as_float(config.get("max_cumulative_notional")))
    remaining_cumulative = max(max_cumulative - cumulative, 0.0) if max_cumulative > 0 else 0.0

    reason_codes: list[str] = []
    if threshold > 0 and net_notional >= threshold:
        reason_codes.append("net_inventory_over_soft_threshold")
    if volatility_paused:
        reason_codes.append("volatility_entry_pause_active")
    if stale_orders > 0:
        reason_codes.append("stale_orders_present")
    if missing_orders > 0:
        reason_codes.append("missing_orders_present")
    if max_cumulative > 0 and remaining_cumulative <= max(max_cumulative * 0.02, 1000.0):
        reason_codes.append("near_cumulative_volume_limit")

    if "net_inventory_over_soft_threshold" in reason_codes:
        state = "repair"
        state_label = "修仓状态：净敞口已超过 soft 阈值"
    elif volatility_paused:
        state = "paused"
        state_label = "暂停刷量：波动保护 active"
    else:
        state = "brush"
        state_label = "刷量状态：normal/fast 可用"

    return {
        "ok": True,
        "state": state,
        "state_label": state_label,
        "reason_codes": reason_codes,
        "event_count": len(valid_events),
        "first_ts": first.get("ts") or "",
        "latest_ts": latest.get("ts") or "",
        "latest_cycle": latest.get("cycle"),
        "window_gross_notional": window_gross,
        "latest": {
            "mid_price": _as_float(latest.get("mid_price")),
            "center_price": _as_float(latest.get("center_price")),
            "current_long_notional": _as_float(latest.get("current_long_notional")),
            "current_short_notional": _as_float(latest.get("current_short_notional")),
            "actual_net_notional": _as_float(latest.get("actual_net_notional")),
            "unrealized_pnl": unrealized_pnl,
            "rolling_hourly_gross_notional": rolling_gross,
            "rolling_hourly_loss": _as_float(latest.get("rolling_hourly_loss")),
            "rolling_hourly_loss_per_10k": rolling_loss_per_10k,
            "cumulative_gross_notional": cumulative,
            "max_cumulative_notional": max_cumulative,
            "remaining_cumulative_notional": remaining_cumulative,
            "open_order_count": open_orders,
            "stale_order_count": stale_orders,
            "missing_order_count": missing_orders,
            "volatility_entry_pause_active": volatility_paused,
            "adaptive_regime_router_regime": latest.get("adaptive_regime_router_regime") or "",
        },
        "error": error,
    }


def build_pharos_recommendation_presets(
    config: dict[str, Any],
    runtime_summary: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    per_order = max(_as_float(config.get("per_order_notional"), 10.0), 1.0)
    max_total = max(_as_float(config.get("max_total_notional"), 1450.0), 1.0)
    cumulative = max(_as_float(config.get("max_cumulative_notional"), 200000.0), 1.0)
    state = (runtime_summary or {}).get("state") or "unknown"
    loss_per_10k = _as_float(((runtime_summary or {}).get("latest") or {}).get("rolling_hourly_loss_per_10k"))
    runtime_note = "当前可刷量" if state == "brush" and loss_per_10k <= 8 else "当前需要结合预检和敞口判断"

    return [
        {
            "preset_id": "conservative_profit",
            "name": "保守盈利",
            "scenario": "适合交易赛前几天、盘口深度一般、优先降低磨损。",
            "risk": "成交速度较慢，小时刷量目标不要设太高。",
            "runtime_note": runtime_note,
            "patch": {
                "per_order_notional": min(per_order, 10.0),
                "max_new_orders": min(_as_int(config.get("max_new_orders"), 5), 3),
                "max_total_notional": min(max_total, 1000.0),
                "best_quote_maker_volume_cycle_budget_notional": min(
                    _as_float(config.get("best_quote_maker_volume_cycle_budget_notional"), 40.0),
                    25.0,
                ),
                "rolling_hourly_loss_limit": min(_as_float(config.get("rolling_hourly_loss_limit"), 8.0), 5.0),
                "anti_chase_entry_guard_enabled": True,
                "volatility_entry_pause_enabled": True,
                "hard_loss_forced_reduce_enabled": True,
            },
        },
        {
            "preset_id": "balanced_volume",
            "name": "均衡刷量",
            "scenario": "适合常规刷量，兼顾速度、磨损和回到 normal/fast 的能力。",
            "risk": "单边行情下仍可能进入 repair/safe，需要看运行数据。",
            "runtime_note": runtime_note,
            "patch": {
                "per_order_notional": min(max(per_order, 10.0), 15.0),
                "max_new_orders": 5,
                "max_total_notional": max(max_total, 1450.0),
                "best_quote_maker_volume_cycle_budget_notional": max(
                    _as_float(config.get("best_quote_maker_volume_cycle_budget_notional"), 40.0),
                    40.0,
                ),
                "rolling_hourly_loss_limit": max(_as_float(config.get("rolling_hourly_loss_limit"), 8.0), 8.0),
                "anti_chase_entry_guard_enabled": True,
                "volatility_entry_pause_enabled": True,
            },
        },
        {
            "preset_id": "final_day_volume",
            "name": "最后一天冲量",
            "scenario": "适合交易赛最后一天、市场量大、盘口深、有明确 20万/50万 小时目标。",
            "risk": "磨损和浮亏扩大更快；必须确认累计刷量上限、小时亏损阈值和盘口承接。",
            "runtime_note": runtime_note,
            "patch": {
                "per_order_notional": max(per_order * 2.0, 20.0),
                "max_new_orders": max(_as_int(config.get("max_new_orders"), 5), 8),
                "max_total_notional": max(max_total, 2500.0),
                "max_mid_drift_steps": max(_as_int(config.get("max_mid_drift_steps"), 20), 30),
                "best_quote_maker_volume_cycle_budget_notional": max(
                    _as_float(config.get("best_quote_maker_volume_cycle_budget_notional"), 40.0),
                    120.0,
                ),
                "rolling_hourly_loss_limit": max(_as_float(config.get("rolling_hourly_loss_limit"), 8.0), 15.0),
                "max_cumulative_notional": max(cumulative, 500000.0),
                "anti_chase_entry_guard_enabled": True,
                "volatility_entry_pause_enabled": True,
            },
        },
    ]


def build_workbench_payload(
    server_id: Any,
    symbol: Any,
    strategy_profile: Any,
    config: dict[str, Any],
    *,
    history: list[dict[str, Any]] | None = None,
    history_error: str = "",
    runtime_events: list[dict[str, Any]] | None = None,
    runtime_error: str = "",
) -> dict[str, Any]:
    target = get_central_strategy_target(server_id, symbol, strategy_profile)
    config_dict = dict(config or {})
    sections = _empty_sections()

    for key in sorted(config_dict):
        if str(key).startswith("_"):
            continue
        value = config_dict[key]
        owner = _workbench_owner_for_key(target, str(key))
        sections[owner][key] = _section_item(key, value, owner)

    schema_report = _schema_report_for_config(config_dict)
    runtime_summary = build_pharos_runtime_summary(runtime_events or [], config_dict, error=runtime_error)
    recommendation_presets = build_pharos_recommendation_presets(config_dict, runtime_summary)
    return {
        "ok": True,
        "scope": target.scope_payload(),
        "target": {
            "server_id": target.server_id,
            "ssh_host": target.ssh_host,
            "control_path": target.control_path,
        },
        "sections": sections,
        "schema_report": schema_report,
        "profile_family": schema_report.get("profile_family"),
        "strategy_intent": schema_report.get("strategy_intent"),
        "required_position_mode": schema_report.get("required_position_mode"),
        "profile_boundary": schema_report.get("profile_boundary") or {},
        "startup_preflight": schema_report.get("startup_preflight") or {},
        "global_safety_preflight": schema_report.get("global_safety_preflight") or {},
        "ignored_params": list(schema_report.get("ignored_params") or []),
        "unknown_params": list(schema_report.get("unknown_params") or []),
        "history": list(history or []),
        "history_error": history_error,
        "can_rollback": len(history or []) >= 2,
        "runtime_summary": runtime_summary,
        "recommendation_presets": recommendation_presets,
        "raw_config": {str(key): _jsonable(value) for key, value in config_dict.items()},
        "source": f"ssh:{target.ssh_host}:{target.control_path}",
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


def read_remote_target_config(target: CentralStrategyTarget) -> dict[str, Any]:
    completed = subprocess.run(
        ["ssh", target.ssh_host, "cat", target.control_path],
        capture_output=True,
        check=False,
        text=True,
        timeout=REMOTE_READ_TIMEOUT_SECONDS,
    )
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout or "").strip()
        raise RuntimeError(f"ssh read failed for {target.scope}: {detail or completed.returncode}")

    loaded = json.loads(completed.stdout)
    if isinstance(loaded, dict) and isinstance(loaded.get("config"), dict):
        return dict(loaded["config"])
    if not isinstance(loaded, dict):
        raise ValueError(f"remote control file for {target.scope} is not a JSON object")
    return dict(loaded)


def _history_path_for_target(target: CentralStrategyTarget) -> str:
    return f"{target.control_path}.central_workbench_history.jsonl"


REMOTE_WRITE_CONTROL_SCRIPT = """
import json
from datetime import datetime, timezone
import os
import sys
import tempfile
import uuid

path = sys.argv[1]
history_path = sys.argv[2]
data = json.load(sys.stdin)
directory = os.path.dirname(path) or "."
os.makedirs(directory, exist_ok=True)
history_directory = os.path.dirname(history_path) or "."
os.makedirs(history_directory, exist_ok=True)

def load_current_config():
    try:
        with open(path, "r", encoding="utf-8") as handle:
            loaded = json.load(handle)
    except FileNotFoundError:
        return {}
    if isinstance(loaded, dict) and isinstance(loaded.get("config"), dict):
        return dict(loaded["config"])
    if isinstance(loaded, dict):
        return dict(loaded)
    return {}

def public_config(config):
    return {str(key): value for key, value in dict(config or {}).items() if not str(key).startswith("_")}

def changed_params(before, after):
    before_public = public_config(before)
    after_public = public_config(after)
    return sorted(
        key
        for key in set(before_public) | set(after_public)
        if before_public.get(key) != after_public.get(key)
    )

before = load_current_config()
version_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]
entry = {
    "version_id": version_id,
    "saved_at": data.get("_central_workbench_saved_at") or datetime.now(timezone.utc).isoformat(),
    "action": data.get("_central_workbench_action") or "save",
    "scope": data.get("_central_workbench_scope") or "",
    "changed_params": changed_params(before, data),
    "stripped_params": sorted(set(data.get("_central_workbench_stripped_params") or [])),
    "before_config": before,
    "after_config": data,
}
fd, tmp_path = tempfile.mkstemp(prefix=os.path.basename(path) + ".", suffix=".tmp", dir=directory)
try:
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        json.dump(data, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\\n")
    os.replace(tmp_path, path)
    with open(history_path, "a", encoding="utf-8") as history:
        history.write(json.dumps(entry, ensure_ascii=False, sort_keys=True) + "\\n")
finally:
    if os.path.exists(tmp_path):
        os.unlink(tmp_path)
print(json.dumps({
    "path": path,
    "history_path": history_path,
    "version_id": version_id,
    "changed_params": entry["changed_params"],
    "stripped_params": entry["stripped_params"],
    "history_entry": {
        "version_id": version_id,
        "saved_at": entry["saved_at"],
        "action": entry["action"],
        "scope": entry["scope"],
        "changed_params": entry["changed_params"],
        "stripped_params": entry["stripped_params"],
    },
}, ensure_ascii=False, sort_keys=True))
""".strip()


REMOTE_HISTORY_CONTROL_SCRIPT = """
import json
import sys

history_path = sys.argv[1]
scope = sys.argv[2]
limit = int(sys.argv[3]) if len(sys.argv) > 3 else 8

def summary(entry):
    return {
        "version_id": entry.get("version_id") or "",
        "saved_at": entry.get("saved_at") or "",
        "action": entry.get("action") or "",
        "scope": entry.get("scope") or "",
        "changed_params": list(entry.get("changed_params") or []),
        "stripped_params": list(entry.get("stripped_params") or []),
        "rollback_of": entry.get("rollback_of") or "",
        "restored_version_id": entry.get("restored_version_id") or "",
    }

entries = []
try:
    with open(history_path, "r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                continue
            if scope and entry.get("scope") != scope:
                continue
            entries.append(summary(entry))
except FileNotFoundError:
    entries = []
print(json.dumps(entries[-limit:], ensure_ascii=False, sort_keys=True))
""".strip()


REMOTE_RUNTIME_EVENTS_SCRIPT = """
import json
import os
import subprocess
import sys

control_path = sys.argv[1]
limit = int(sys.argv[2]) if len(sys.argv) > 2 else 120
selected_keys = {
    "ts",
    "cycle",
    "symbol",
    "strategy_profile",
    "mid_price",
    "center_price",
    "current_long_notional",
    "current_short_notional",
    "actual_net_notional",
    "unrealized_pnl",
    "unrealized_loss",
    "open_order_count",
    "kept_order_count",
    "missing_order_count",
    "stale_order_count",
    "planned_buy_notional",
    "planned_short_notional",
    "rolling_hourly_loss",
    "rolling_hourly_loss_limit",
    "rolling_hourly_gross_notional",
    "rolling_hourly_loss_per_10k",
    "cumulative_gross_notional",
    "max_cumulative_notional",
    "max_actual_net_notional",
    "threshold_position_notional",
    "volatility_entry_pause_active",
    "volatility_entry_pause_reason",
    "adaptive_regime_router_regime",
    "adaptive_regime_router_candidate_regime",
}

try:
    with open(control_path, "r", encoding="utf-8") as handle:
        control = json.load(handle)
except FileNotFoundError:
    print(json.dumps({"events": [], "error": "control file missing"}, ensure_ascii=False, sort_keys=True))
    sys.exit(0)

summary_path = control.get("summary_jsonl") or "output/pharosusdt_hedge_bq_events.jsonl"
if not os.path.isabs(summary_path):
    root = os.path.dirname(os.path.dirname(control_path))
    summary_path = os.path.join(root, summary_path)

if not os.path.exists(summary_path):
    print(json.dumps({"events": [], "error": "summary_jsonl missing: " + summary_path}, ensure_ascii=False, sort_keys=True))
    sys.exit(0)

try:
    raw = subprocess.check_output(["tail", "-n", str(max(limit, 1)), summary_path], text=True, errors="replace")
except Exception as exc:
    print(json.dumps({"events": [], "error": type(exc).__name__ + ": " + str(exc)}, ensure_ascii=False, sort_keys=True))
    sys.exit(0)

events = []
for line in raw.splitlines():
    line = line.strip()
    if not line:
        continue
    try:
        event = json.loads(line)
    except json.JSONDecodeError:
        continue
    if not isinstance(event, dict):
        continue
    events.append({key: event.get(key) for key in selected_keys if key in event})

print(json.dumps({"events": events, "error": ""}, ensure_ascii=False, sort_keys=True))
""".strip()


REMOTE_ROLLBACK_CONTROL_SCRIPT = """
from datetime import datetime, timezone
import json
import os
import sys
import tempfile
import uuid

path = sys.argv[1]
history_path = sys.argv[2]
scope = sys.argv[3]

def load_json_file(file_path):
    try:
        with open(file_path, "r", encoding="utf-8") as handle:
            loaded = json.load(handle)
    except FileNotFoundError:
        return {}
    if isinstance(loaded, dict) and isinstance(loaded.get("config"), dict):
        return dict(loaded["config"])
    if isinstance(loaded, dict):
        return dict(loaded)
    return {}

def public_config(config):
    return {str(key): value for key, value in dict(config or {}).items() if not str(key).startswith("_")}

def changed_params(before, after):
    before_public = public_config(before)
    after_public = public_config(after)
    return sorted(
        key
        for key in set(before_public) | set(after_public)
        if before_public.get(key) != after_public.get(key)
    )

entries = []
try:
    with open(history_path, "r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                continue
            if entry.get("scope") != scope:
                continue
            if isinstance(entry.get("after_config"), dict):
                entries.append(entry)
except FileNotFoundError:
    entries = []

if len(entries) < 2:
    print("no previous central workbench saved version", file=sys.stderr)
    sys.exit(2)

current_entry = entries[-1]
restore_entry = entries[-2]
before = load_json_file(path)
after = dict(restore_entry["after_config"])
version_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]
entry = {
    "version_id": version_id,
    "saved_at": datetime.now(timezone.utc).isoformat(),
    "action": "rollback",
    "scope": scope,
    "rollback_of": current_entry.get("version_id") or "",
    "restored_version_id": restore_entry.get("version_id") or "",
    "changed_params": changed_params(before, after),
    "stripped_params": [],
    "before_config": before,
    "after_config": after,
}

directory = os.path.dirname(path) or "."
os.makedirs(directory, exist_ok=True)
fd, tmp_path = tempfile.mkstemp(prefix=os.path.basename(path) + ".", suffix=".tmp", dir=directory)
try:
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        json.dump(after, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\\n")
    os.replace(tmp_path, path)
    with open(history_path, "a", encoding="utf-8") as history:
        history.write(json.dumps(entry, ensure_ascii=False, sort_keys=True) + "\\n")
finally:
    if os.path.exists(tmp_path):
        os.unlink(tmp_path)

print(json.dumps({
    "rolled_back": True,
    "rollback_of": entry["rollback_of"],
    "restored_version_id": entry["restored_version_id"],
    "changed_params": entry["changed_params"],
    "history_entry": {
        "version_id": version_id,
        "saved_at": entry["saved_at"],
        "action": "rollback",
        "scope": scope,
        "rollback_of": entry["rollback_of"],
        "restored_version_id": entry["restored_version_id"],
        "changed_params": entry["changed_params"],
        "stripped_params": [],
    },
}, ensure_ascii=False, sort_keys=True))
""".strip()


def _write_remote_target_config(target: CentralStrategyTarget, config: dict[str, Any]) -> dict[str, Any]:
    history_path = _history_path_for_target(target)
    remote_command = " ".join(
        [
            "python3",
            "-c",
            shlex.quote(REMOTE_WRITE_CONTROL_SCRIPT),
            shlex.quote(target.control_path),
            shlex.quote(history_path),
        ]
    )
    completed = subprocess.run(
        ["ssh", target.ssh_host, remote_command],
        capture_output=True,
        check=False,
        input=json.dumps(config, ensure_ascii=False, sort_keys=True),
        text=True,
        timeout=REMOTE_WRITE_TIMEOUT_SECONDS,
    )
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout or "").strip()
        raise RuntimeError(f"ssh write failed for {target.scope}: {detail or completed.returncode}")
    stdout = (completed.stdout or "").strip()
    parsed: dict[str, Any] = {}
    if stdout:
        try:
            parsed = json.loads(stdout)
        except json.JSONDecodeError:
            parsed = {"stdout": stdout}
    return {
        **parsed,
        "stdout": stdout,
        "stderr": (completed.stderr or "").strip(),
    }


def read_remote_target_history(target: CentralStrategyTarget, *, limit: int = 8) -> list[dict[str, Any]]:
    history_path = _history_path_for_target(target)
    remote_command = " ".join(
        [
            "python3",
            "-c",
            shlex.quote(REMOTE_HISTORY_CONTROL_SCRIPT),
            shlex.quote(history_path),
            shlex.quote(target.scope),
            str(int(limit)),
        ]
    )
    completed = subprocess.run(
        ["ssh", target.ssh_host, remote_command],
        capture_output=True,
        check=False,
        text=True,
        timeout=REMOTE_HISTORY_TIMEOUT_SECONDS,
    )
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout or "").strip()
        raise RuntimeError(f"ssh history read failed for {target.scope}: {detail or completed.returncode}")
    loaded = json.loads(completed.stdout or "[]")
    if not isinstance(loaded, list):
        return []
    return [entry for entry in loaded if isinstance(entry, dict)]


def read_remote_runtime_events(target: CentralStrategyTarget, *, limit: int = 120) -> tuple[list[dict[str, Any]], str]:
    remote_command = " ".join(
        [
            "python3",
            "-c",
            shlex.quote(REMOTE_RUNTIME_EVENTS_SCRIPT),
            shlex.quote(target.control_path),
            str(int(limit)),
        ]
    )
    completed = subprocess.run(
        ["ssh", target.ssh_host, remote_command],
        capture_output=True,
        check=False,
        text=True,
        timeout=REMOTE_RUNTIME_TIMEOUT_SECONDS,
    )
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout or "").strip()
        raise RuntimeError(f"ssh runtime read failed for {target.scope}: {detail or completed.returncode}")
    loaded = json.loads(completed.stdout or "{}")
    if not isinstance(loaded, dict):
        return [], "runtime response is not an object"
    events = loaded.get("events") if isinstance(loaded.get("events"), list) else []
    return [entry for entry in events if isinstance(entry, dict)], str(loaded.get("error") or "")


def save_remote_workbench_config(
    server_id: Any,
    symbol: Any,
    strategy_profile: Any,
    config: dict[str, Any],
    *,
    action: str = "save",
) -> dict[str, Any]:
    target = get_central_strategy_target(server_id, symbol, strategy_profile)
    normalized = _normalize_config_for_target(target, config, action=action)
    write_result = _write_remote_target_config(target, normalized)
    history_entry = write_result.get("history_entry") if isinstance(write_result.get("history_entry"), dict) else None
    history: list[dict[str, Any]] = [history_entry] if history_entry else []
    history_error = ""
    try:
        history = read_remote_target_history(target)
    except Exception as exc:
        history_error = f"{type(exc).__name__}: {exc}"
    payload = build_workbench_payload(
        target.server_id,
        target.symbol,
        target.strategy_profile,
        normalized,
        history=history,
        history_error=history_error,
    )
    return {
        **payload,
        "saved": True,
        "stripped_params": list(normalized.get("_central_workbench_stripped_params") or []),
        "changed_params": list(write_result.get("changed_params") or []),
        "history_entry": history_entry or {},
        "remote_write": write_result,
    }


def _restart_remote_runner(target: CentralStrategyTarget) -> dict[str, Any]:
    completed = subprocess.run(
        ["ssh", target.ssh_host, target.runner_wrapper, "restart", target.symbol],
        capture_output=True,
        check=False,
        text=True,
        timeout=REMOTE_RUNNER_TIMEOUT_SECONDS,
    )
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout or "").strip()
        raise RuntimeError(f"runner restart failed for {target.scope}: {detail or completed.returncode}")
    return {
        "stdout": (completed.stdout or "").strip(),
        "stderr": (completed.stderr or "").strip(),
    }


def apply_remote_workbench_config(
    server_id: Any,
    symbol: Any,
    strategy_profile: Any,
    config: dict[str, Any],
) -> dict[str, Any]:
    target = get_central_strategy_target(server_id, symbol, strategy_profile)
    saved = save_remote_workbench_config(
        target.server_id,
        target.symbol,
        target.strategy_profile,
        config,
        action="apply",
    )
    restart_result = _restart_remote_runner(target)
    return {
        **saved,
        "started": True,
        "remote_runner": restart_result,
    }


def _rollback_remote_target_config(target: CentralStrategyTarget) -> dict[str, Any]:
    history_path = _history_path_for_target(target)
    remote_command = " ".join(
        [
            "python3",
            "-c",
            shlex.quote(REMOTE_ROLLBACK_CONTROL_SCRIPT),
            shlex.quote(target.control_path),
            shlex.quote(history_path),
            shlex.quote(target.scope),
        ]
    )
    completed = subprocess.run(
        ["ssh", target.ssh_host, remote_command],
        capture_output=True,
        check=False,
        text=True,
        timeout=REMOTE_ROLLBACK_TIMEOUT_SECONDS,
    )
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout or "").strip()
        raise RuntimeError(f"rollback failed for {target.scope}: {detail or completed.returncode}")
    stdout = (completed.stdout or "").strip()
    parsed = json.loads(stdout or "{}")
    if not isinstance(parsed, dict):
        raise ValueError(f"rollback response for {target.scope} is not an object")
    return {
        **parsed,
        "stdout": stdout,
        "stderr": (completed.stderr or "").strip(),
    }


def rollback_remote_workbench_config(
    server_id: Any,
    symbol: Any,
    strategy_profile: Any,
) -> dict[str, Any]:
    target = get_central_strategy_target(server_id, symbol, strategy_profile)
    rollback_result = _rollback_remote_target_config(target)
    return {
        "ok": True,
        "scope": target.scope_payload(),
        "rolled_back": bool(rollback_result.get("rolled_back")),
        "rollback_of": rollback_result.get("rollback_of") or "",
        "restored_version_id": rollback_result.get("restored_version_id") or "",
        "changed_params": list(rollback_result.get("changed_params") or []),
        "history_entry": rollback_result.get("history_entry") or {},
        "remote_rollback": rollback_result,
    }


def build_remote_workbench_status(server_id: Any, symbol: Any, strategy_profile: Any) -> dict[str, Any]:
    try:
        target = get_central_strategy_target(server_id, symbol, strategy_profile)
    except Exception as exc:
        normalized_server_id, normalized_symbol, normalized_profile = _target_key(
            server_id,
            symbol,
            strategy_profile,
        )
        return {
            "ok": False,
            "scope": {
                "server_id": normalized_server_id,
                "symbol": normalized_symbol,
                "strategy_profile": normalized_profile,
            },
            "sections": _empty_sections(),
            "error": f"{type(exc).__name__}: {exc}",
        }

    try:
        config = read_remote_target_config(target)
        history: list[dict[str, Any]] = []
        history_error = ""
        runtime_events: list[dict[str, Any]] = []
        runtime_error = ""
        try:
            history = read_remote_target_history(target)
        except Exception as history_exc:
            history_error = f"{type(history_exc).__name__}: {history_exc}"
        try:
            runtime_events, runtime_error = read_remote_runtime_events(target)
        except Exception as runtime_exc:
            runtime_error = f"{type(runtime_exc).__name__}: {runtime_exc}"
        return build_workbench_payload(
            target.server_id,
            target.symbol,
            target.strategy_profile,
            config,
            history=history,
            history_error=history_error,
            runtime_events=runtime_events,
            runtime_error=runtime_error,
        )
    except Exception as exc:
        return {
            "ok": False,
            "scope": target.scope_payload(),
            "target": {
                "server_id": target.server_id,
                "ssh_host": target.ssh_host,
                "control_path": target.control_path,
            },
            "sections": _empty_sections(),
            "error": f"{type(exc).__name__}: {exc}",
        }
