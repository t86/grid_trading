"""Read-only liveness watchdog for recovery-coordinator owned symbols.

It intentionally has no runner, order, or control-document actuator.  Its
only responsibility is to make a failed coordinator round visible quickly
after systemd restart ownership has been removed from a managed runner.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping

from .futures_recovery_store import (
    RecoveryStateStoreError,
    decode_recovery_control_state,
    recovery_coordinator_registered,
)
from .futures_run_lifecycle import (
    resolve_authoritative_run_contract,
    run_contract_snapshot_from_config,
)
from .futures_terminal_ownership import (
    TerminalIntentValidationError,
    validate_terminal_intent,
)
from .notifications import alert_source_label, load_alert_notifier_config, send_alert_email


HEARTBEAT_KEY = "futures_recovery_guard_heartbeat"
HEARTBEAT_SCHEMA = "futures_recovery_guard_heartbeat_v1"
WATCHDOG_HEARTBEAT_KEY = "recovery_coordinator_watchdog_heartbeat"
WATCHDOG_HEARTBEAT_SCHEMA = "recovery_coordinator_watchdog_heartbeat_v1"
TARGET_GATE_HEARTBEAT_SCHEMA = "futures_target_gate_heartbeat_v1"
DEFAULT_TARGET_GATE_MAX_HEARTBEAT_AGE_SECONDS = 600.0


def _read_json_with_readability(path: Path) -> tuple[dict[str, Any], bool]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}, True
    except (json.JSONDecodeError, OSError):
        return {}, False
    return (payload, True) if isinstance(payload, dict) else ({}, False)


def _read_json(path: Path) -> dict[str, Any]:
    return _read_json_with_readability(path)[0]


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _parse_time(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(timezone.utc)


def _assess_target_gate_liveness(
    *,
    symbol: str,
    control: Mapping[str, Any],
    heartbeat: Mapping[str, Any] | None,
    heartbeat_readable: bool,
    now: datetime,
    max_heartbeat_age_seconds: float,
) -> dict[str, Any] | None:
    """Require a fresh gate only while a bounded target window is live."""

    if max_heartbeat_age_seconds <= 0:
        raise ValueError("target_gate_max_heartbeat_age_seconds must be positive")
    try:
        raw_snapshot = run_contract_snapshot_from_config(control)
    except (TypeError, ValueError):
        return None
    raw_target = raw_snapshot.get("max_cumulative_notional")
    if (
        not isinstance(raw_target, (int, float))
        or isinstance(raw_target, bool)
        or raw_target <= 0
        or raw_snapshot.get("run_end_time") is None
    ):
        return None
    try:
        snapshot, run_contract_id = resolve_authoritative_run_contract(
            control,
            expected_symbol=symbol,
        )
    except (TypeError, ValueError) as exc:
        return {
            "healthy": False,
            "reason": "target_gate_run_contract_invalid",
            "detail": str(exc),
        }
    target = snapshot.get("max_cumulative_notional")
    window_start = _parse_time(snapshot.get("runtime_guard_stats_start_time"))
    window_end = _parse_time(snapshot.get("run_end_time"))
    if not isinstance(target, (int, float)) or isinstance(target, bool) or target <= 0:
        return None
    if window_start is None or window_end is None:
        return {
            "healthy": False,
            "reason": "target_gate_run_contract_invalid",
        }
    checked_now = now.astimezone(timezone.utc)
    if (
        checked_now < window_start
        or checked_now > window_end + timedelta(seconds=max_heartbeat_age_seconds)
    ):
        return None
    if not heartbeat_readable:
        return {"healthy": False, "reason": "target_gate_heartbeat_unreadable"}
    if not isinstance(heartbeat, Mapping):
        return {"healthy": False, "reason": "target_gate_heartbeat_missing"}
    if heartbeat.get("schema") != TARGET_GATE_HEARTBEAT_SCHEMA:
        return {"healthy": False, "reason": "target_gate_heartbeat_invalid"}
    if heartbeat.get("symbol") != str(symbol).upper().strip():
        return {"healthy": False, "reason": "target_gate_heartbeat_symbol_mismatch"}
    if heartbeat.get("run_contract_id") != run_contract_id:
        return {"healthy": False, "reason": "target_gate_heartbeat_contract_mismatch"}
    checked_at = _parse_time(heartbeat.get("checked_at"))
    if checked_at is None:
        return {"healthy": False, "reason": "target_gate_heartbeat_timestamp_invalid"}
    age_seconds = (checked_now - checked_at).total_seconds()
    if age_seconds < -60 or age_seconds > max_heartbeat_age_seconds:
        return {
            "healthy": False,
            "reason": "target_gate_heartbeat_stale",
            "age_seconds": age_seconds,
        }
    return {
        "healthy": True,
        "reason": "target_gate_heartbeat_fresh",
        "checked_at": checked_at.isoformat(),
        "age_seconds": age_seconds,
    }


def _assess_deadline_terminal_intent(
    *,
    symbol: str,
    control: Mapping[str, Any],
    intent: Mapping[str, Any] | None,
    intent_readable: bool,
    now: datetime,
    grace_seconds: float,
) -> dict[str, Any] | None:
    """Require the frozen run's terminal owner once its target window closes."""

    if grace_seconds <= 0:
        raise ValueError("target_gate_max_heartbeat_age_seconds must be positive")
    try:
        raw_snapshot = run_contract_snapshot_from_config(control)
    except (TypeError, ValueError):
        return None
    raw_target = raw_snapshot.get("max_cumulative_notional")
    if (
        not isinstance(raw_target, (int, float))
        or isinstance(raw_target, bool)
        or raw_target <= 0
        or raw_snapshot.get("run_end_time") is None
    ):
        return None
    try:
        snapshot, run_contract_id = resolve_authoritative_run_contract(
            control,
            expected_symbol=symbol,
        )
    except (TypeError, ValueError) as exc:
        return {
            "healthy": False,
            "reason": "deadline_terminal_run_contract_invalid",
            "detail": str(exc),
        }
    window_end = _parse_time(snapshot.get("run_end_time"))
    if window_end is None:
        return {
            "healthy": False,
            "reason": "deadline_terminal_run_contract_invalid",
        }
    if now.astimezone(timezone.utc) < window_end + timedelta(seconds=grace_seconds):
        return None
    if not intent_readable:
        return {"healthy": False, "reason": "deadline_terminal_intent_unreadable"}
    if not isinstance(intent, Mapping):
        return {"healthy": False, "reason": "deadline_terminal_intent_missing"}
    try:
        validated = validate_terminal_intent(intent, expected_symbol=symbol)
    except TerminalIntentValidationError as exc:
        return {
            "healthy": False,
            "reason": "deadline_terminal_intent_invalid",
            "detail": str(exc),
        }
    if validated.run_contract_id != run_contract_id:
        return {
            "healthy": False,
            "reason": "deadline_terminal_intent_contract_mismatch",
        }
    return {
        "healthy": True,
        "reason": "deadline_terminal_intent_present",
        "status": validated.status,
        "intent_id": validated.intent_id,
    }


def assess_coordinator_liveness(
    *,
    symbol: str,
    control: Mapping[str, Any],
    guard_state: Mapping[str, Any],
    now: datetime,
    max_heartbeat_age_seconds: float,
    force_reason: str | None = None,
) -> dict[str, Any]:
    """Classify coordinator liveness without changing any trading state."""

    normalized = str(symbol).upper().strip()
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("now must be timezone-aware")
    if max_heartbeat_age_seconds <= 0:
        raise ValueError("max_heartbeat_age_seconds must be positive")
    registered = recovery_coordinator_registered(control)
    if force_reason and not registered:
        return {
            "symbol": normalized,
            "tracked": True,
            "healthy": False,
            "reason": force_reason,
        }
    if not registered:
        return {
            "symbol": normalized,
            "tracked": False,
            "healthy": True,
            "reason": "unregistered",
        }
    try:
        decode_recovery_control_state(control, expected_symbol=normalized)
    except (TypeError, ValueError, RecoveryStateStoreError) as exc:
        return {
            "symbol": normalized,
            "tracked": True,
            "healthy": False,
            "reason": "registered_control_invalid",
            "detail": str(exc),
        }
    if force_reason:
        return {
            "symbol": normalized,
            "tracked": True,
            "healthy": False,
            "reason": force_reason,
        }
    heartbeat = guard_state.get(HEARTBEAT_KEY)
    if not isinstance(heartbeat, Mapping) or heartbeat.get("schema") != HEARTBEAT_SCHEMA:
        return {
            "symbol": normalized,
            "tracked": True,
            "healthy": False,
            "reason": "coordinator_heartbeat_missing_or_invalid",
        }
    checked_at = _parse_time(heartbeat.get("checked_at"))
    if checked_at is None:
        return {
            "symbol": normalized,
            "tracked": True,
            "healthy": False,
            "reason": "coordinator_heartbeat_timestamp_invalid",
        }
    age_seconds = (now.astimezone(timezone.utc) - checked_at).total_seconds()
    if age_seconds < -60 or age_seconds > max_heartbeat_age_seconds:
        return {
            "symbol": normalized,
            "tracked": True,
            "healthy": False,
            "reason": "coordinator_heartbeat_stale",
            "age_seconds": age_seconds,
        }
    symbols = heartbeat.get("symbols")
    symbol_status = symbols.get(normalized) if isinstance(symbols, Mapping) else None
    if not isinstance(symbol_status, Mapping):
        return {
            "symbol": normalized,
            "tracked": True,
            "healthy": False,
            "reason": "coordinator_round_failed",
            "action": None,
        }
    # The coordinator executes one isolated action per symbol.  A blocked
    # neighbor must not hide this symbol's healthy round, while this symbol's
    # own visible block must remain alertable instead of looking like progress.
    if symbol_status.get("liveness_status") == "blocked":
        return {
            "symbol": normalized,
            "tracked": True,
            "healthy": False,
            "reason": "coordinator_symbol_blocked",
            "action": symbol_status.get("action"),
        }
    if symbol_status.get("healthy") is not True:
        return {
            "symbol": normalized,
            "tracked": True,
            "healthy": False,
            "reason": "coordinator_symbol_round_unhealthy",
            "action": symbol_status.get("action"),
        }
    return {
        "symbol": normalized,
        "tracked": True,
        "healthy": True,
        "reason": "coordinator_heartbeat_fresh",
        "checked_at": checked_at.isoformat(),
        "age_seconds": age_seconds,
        "action": symbol_status.get("action"),
    }


def update_watchdog_state(
    *,
    assessment: Mapping[str, Any],
    state: dict[str, Any],
    now: datetime,
    alert_threshold: int,
    force_alert: bool = False,
) -> tuple[dict[str, Any], bool]:
    """Persist a deduplicated alert episode for one observed symbol."""

    if alert_threshold <= 0:
        raise ValueError("alert_threshold must be positive")
    symbol = str(assessment["symbol"])
    if not assessment.get("tracked"):
        state[symbol] = {
            "status": "unregistered",
            "checked_at": now.isoformat(),
            "assessment": dict(assessment),
            "alert_sent": False,
        }
        return state, False
    if assessment.get("healthy"):
        state[symbol] = {
            "status": "healthy",
            "checked_at": now.isoformat(),
            "assessment": dict(assessment),
            "consecutive_failures": 0,
            "alert_sent": False,
        }
        return state, False

    previous = state.get(symbol)
    previous = previous if isinstance(previous, Mapping) else {}
    reason = str(assessment.get("reason") or "unknown")
    same_episode = previous.get("status") == "unhealthy" and previous.get("reason") == reason
    failures = int(previous.get("consecutive_failures") or 0) + 1 if same_episode else 1
    alert_sent = bool(previous.get("alert_sent")) if same_episode else False
    should_alert = (force_alert or failures >= alert_threshold) and not alert_sent
    state[symbol] = {
        "status": "unhealthy",
        "reason": reason,
        "checked_at": now.isoformat(),
        "first_failed_at": previous.get("first_failed_at") if same_episode else now.isoformat(),
        "consecutive_failures": failures,
        "alert_sent": alert_sent or should_alert,
        "assessment": dict(assessment),
    }
    if should_alert:
        state[symbol]["last_alert_at"] = now.isoformat()
    return state, should_alert


def _format_alert_body(assessment: Mapping[str, Any], state: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            f"结论: {assessment['symbol']} 的恢复协调器不可用，受管 runner 保持 Restart=no。",
            f"原因: {assessment.get('reason')}",
            f"连续失败: {state.get('consecutive_failures')}",
            f"最近协调动作: {assessment.get('action')}",
            f"心跳年龄秒: {assessment.get('age_seconds')}",
            "",
            "该 watchdog 不会重启 runner、修改控制、撤单或下单；请检查协调器服务日志和状态文件。",
        ]
    )


def _record_watchdog_heartbeat(
    state: dict[str, Any], *, now: datetime, results: list[Mapping[str, Any]]
) -> None:
    tracked = {
        str(result["symbol"]): bool(result["assessment"].get("healthy"))
        for result in results
        if result.get("assessment", {}).get("tracked") is True
    }
    state[WATCHDOG_HEARTBEAT_KEY] = {
        "schema": WATCHDOG_HEARTBEAT_SCHEMA,
        "checked_at": now.isoformat(),
        "ok": all(tracked.values()),
        "symbols": tracked,
    }


def check_symbol(
    *,
    symbol: str,
    output_dir: Path,
    guard_state: Mapping[str, Any],
    state: dict[str, Any],
    now: datetime,
    max_heartbeat_age_seconds: float,
    target_gate_max_heartbeat_age_seconds: float = (
        DEFAULT_TARGET_GATE_MAX_HEARTBEAT_AGE_SECONDS
    ),
    alert_threshold: int,
    alert_config_path: Path | None,
    force_reason: str | None = None,
) -> dict[str, Any]:
    normalized = str(symbol).upper().strip()
    control_path = output_dir / f"{normalized.lower()}_loop_runner_control.json"
    control, control_readable = _read_json_with_readability(control_path)
    assessment = assess_coordinator_liveness(
        symbol=normalized,
        control=control,
        guard_state=guard_state,
        now=now,
        max_heartbeat_age_seconds=max_heartbeat_age_seconds,
        force_reason=force_reason or (
            "recovery_control_unreadable" if not control_readable else None
        ),
    )
    target_gate_assessment = None
    deadline_terminal_assessment = None
    if assessment.get("healthy"):
        heartbeat_path = output_dir / f"{normalized.lower()}_target_gate_heartbeat.json"
        heartbeat, heartbeat_readable = _read_json_with_readability(heartbeat_path)
        target_gate_assessment = _assess_target_gate_liveness(
            symbol=normalized,
            control=control,
            heartbeat=heartbeat if heartbeat else None,
            heartbeat_readable=heartbeat_readable,
            now=now,
            max_heartbeat_age_seconds=target_gate_max_heartbeat_age_seconds,
        )
        if target_gate_assessment is not None and not target_gate_assessment["healthy"]:
            assessment = {
                **assessment,
                "healthy": False,
                "reason": str(target_gate_assessment["reason"]),
                "target_gate": target_gate_assessment,
            }
    if assessment.get("healthy"):
        intent_path = output_dir / f"{normalized.lower()}_terminal_intent.json"
        intent, intent_readable = _read_json_with_readability(intent_path)
        deadline_terminal_assessment = _assess_deadline_terminal_intent(
            symbol=normalized,
            control=control,
            intent=intent if intent else None,
            intent_readable=intent_readable,
            now=now,
            grace_seconds=target_gate_max_heartbeat_age_seconds,
        )
        if (
            deadline_terminal_assessment is not None
            and not deadline_terminal_assessment["healthy"]
        ):
            assessment = {
                **assessment,
                "healthy": False,
                "reason": str(deadline_terminal_assessment["reason"]),
                "deadline_terminal": deadline_terminal_assessment,
            }
    alert_config = load_alert_notifier_config(alert_config_path)
    if assessment.get("tracked") and not alert_config.get("enabled"):
        assessment = {
            **assessment,
            "healthy": False,
            "reason": "alert_delivery_not_configured",
        }
    state, should_alert = update_watchdog_state(
        assessment=assessment,
        state=state,
        now=now,
        alert_threshold=alert_threshold,
        force_alert=bool(force_reason),
    )
    result: dict[str, Any] = {
        "symbol": normalized,
        "assessment": assessment,
        "target_gate": target_gate_assessment,
        "deadline_terminal": deadline_terminal_assessment,
        "alert": None,
    }
    if should_alert:
        result["alert"] = send_alert_email(
            subject=f"[grid][{alert_source_label()}] {normalized} recovery coordinator unhealthy",
            body=_format_alert_body(assessment, state[normalized]),
            config_path=alert_config_path,
        )
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Alert on stale recovery-coordinator heartbeats.")
    parser.add_argument("--symbols", required=True, help="Comma-separated symbols")
    parser.add_argument("--output-dir", default="output")
    parser.add_argument("--guard-state-path", default="output/bq_volume_recovery_guard_state.json")
    parser.add_argument("--state-path", default="output/recovery_coordinator_watchdog_state.json")
    parser.add_argument("--alert-config-path", default="output/alert_notifier_config.json")
    parser.add_argument("--max-heartbeat-age-seconds", type=float, default=150.0)
    parser.add_argument(
        "--target-gate-max-heartbeat-age-seconds",
        type=float,
        default=DEFAULT_TARGET_GATE_MAX_HEARTBEAT_AGE_SECONDS,
    )
    parser.add_argument("--alert-threshold", type=int, default=2)
    parser.add_argument("--force-reason", default=None)
    args = parser.parse_args(argv)

    now = datetime.now(timezone.utc)
    state_path = Path(args.state_path)
    state = _read_json(state_path)
    guard_state = _read_json(Path(args.guard_state_path))
    symbols = [item.strip().upper() for item in args.symbols.split(",") if item.strip()]
    results = [
        check_symbol(
            symbol=symbol,
            output_dir=Path(args.output_dir),
            guard_state=guard_state,
            state=state,
            now=now,
            max_heartbeat_age_seconds=float(args.max_heartbeat_age_seconds),
            target_gate_max_heartbeat_age_seconds=float(
                args.target_gate_max_heartbeat_age_seconds
            ),
            alert_threshold=int(args.alert_threshold),
            alert_config_path=Path(args.alert_config_path) if args.alert_config_path else None,
            force_reason=(str(args.force_reason).strip() if args.force_reason else None),
        )
        for symbol in symbols
    ]
    _record_watchdog_heartbeat(state, now=now, results=results)
    _write_json(state_path, state)
    print(json.dumps({"ok": True, "checked_at": now.isoformat(), "results": results}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
