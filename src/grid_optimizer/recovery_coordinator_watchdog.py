"""Read-only liveness watchdog for recovery-coordinator owned symbols.

It intentionally has no runner, order, or control-document actuator.  Its
only responsibility is to make a failed coordinator round visible quickly
after systemd restart ownership has been removed from a managed runner.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from .futures_recovery_store import (
    RecoveryStateStoreError,
    decode_recovery_control_state,
    recovery_coordinator_registered,
)
from .notifications import alert_source_label, load_alert_notifier_config, send_alert_email


HEARTBEAT_KEY = "futures_recovery_guard_heartbeat"
HEARTBEAT_SCHEMA = "futures_recovery_guard_heartbeat_v1"
WATCHDOG_HEARTBEAT_KEY = "recovery_coordinator_watchdog_heartbeat"
WATCHDOG_HEARTBEAT_SCHEMA = "recovery_coordinator_watchdog_heartbeat_v1"


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}
    return payload if isinstance(payload, dict) else {}


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
    if not recovery_coordinator_registered(control):
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
    alert_threshold: int,
    alert_config_path: Path | None,
    force_reason: str | None = None,
) -> dict[str, Any]:
    normalized = str(symbol).upper().strip()
    assessment = assess_coordinator_liveness(
        symbol=normalized,
        control=_read_json(output_dir / f"{normalized.lower()}_loop_runner_control.json"),
        guard_state=guard_state,
        now=now,
        max_heartbeat_age_seconds=max_heartbeat_age_seconds,
        force_reason=force_reason,
    )
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
    result: dict[str, Any] = {"symbol": normalized, "assessment": assessment, "alert": None}
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
