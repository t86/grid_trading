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


def build_workbench_payload(
    server_id: Any,
    symbol: Any,
    strategy_profile: Any,
    config: dict[str, Any],
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


REMOTE_WRITE_CONTROL_SCRIPT = """
import json
import os
import sys
import tempfile

path = sys.argv[1]
data = json.load(sys.stdin)
directory = os.path.dirname(path) or "."
os.makedirs(directory, exist_ok=True)
fd, tmp_path = tempfile.mkstemp(prefix=os.path.basename(path) + ".", suffix=".tmp", dir=directory)
try:
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        json.dump(data, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\\n")
    os.replace(tmp_path, path)
finally:
    if os.path.exists(tmp_path):
        os.unlink(tmp_path)
print(path)
""".strip()


def _write_remote_target_config(target: CentralStrategyTarget, config: dict[str, Any]) -> dict[str, Any]:
    remote_command = " ".join(
        [
            "python3",
            "-c",
            shlex.quote(REMOTE_WRITE_CONTROL_SCRIPT),
            shlex.quote(target.control_path),
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
    return {
        "stdout": (completed.stdout or "").strip(),
        "stderr": (completed.stderr or "").strip(),
    }


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
    payload = build_workbench_payload(target.server_id, target.symbol, target.strategy_profile, normalized)
    return {
        **payload,
        "saved": True,
        "stripped_params": list(normalized.get("_central_workbench_stripped_params") or []),
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
        return build_workbench_payload(target.server_id, target.symbol, target.strategy_profile, config)
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
