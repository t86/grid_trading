from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any


def _parse_datetime(value: Any, field_name: str) -> datetime | None:
    if value in {"", None}:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        dt = datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
    if dt.tzinfo is None:
        raise ValueError(f"{field_name} must include timezone information")
    return dt.astimezone(timezone.utc)


def _parse_positive_float(value: Any, field_name: str) -> float | None:
    if value in {"", None}:
        return None
    parsed = float(value)
    if parsed <= 0:
        raise ValueError(f"{field_name} must be > 0")
    return parsed


def _event_net_pnl(item: dict[str, Any]) -> float:
    if "net_pnl" in item:
        return float(item.get("net_pnl") or 0.0)
    realized = float(item.get("realized_pnl") or 0.0)
    funding = float(item.get("funding_fee") or item.get("income") or 0.0)
    commission = float(item.get("commission_quote") or 0.0)
    recycle_loss = float(item.get("recycle_loss_abs") or 0.0)
    return realized + funding - commission - recycle_loss


@dataclass(frozen=True)
class RuntimeGuardConfig:
    run_start_time: datetime | None
    run_end_time: datetime | None
    rolling_hourly_loss_limit: float | None
    max_cumulative_notional: float | None


@dataclass(frozen=True)
class RuntimeGuardResult:
    tradable: bool
    stop_triggered: bool
    runtime_status: str
    primary_reason: str | None
    matched_reasons: list[str]
    triggered_at: str | None
    rolling_hourly_loss: float
    cumulative_gross_notional: float


def normalize_runtime_guard_config(raw: dict[str, Any]) -> RuntimeGuardConfig:
    config = RuntimeGuardConfig(
        run_start_time=_parse_datetime(raw.get("run_start_time"), "run_start_time"),
        run_end_time=_parse_datetime(raw.get("run_end_time"), "run_end_time"),
        rolling_hourly_loss_limit=_parse_positive_float(
            raw.get("rolling_hourly_loss_limit"),
            "rolling_hourly_loss_limit",
        ),
        max_cumulative_notional=_parse_positive_float(
            raw.get("max_cumulative_notional"),
            "max_cumulative_notional",
        ),
    )
    if config.run_start_time and config.run_end_time and config.run_start_time >= config.run_end_time:
        raise ValueError("run_start_time must be earlier than run_end_time")
    return config


def normalize_runtime_guard_payload(raw: dict[str, Any]) -> dict[str, Any]:
    config = normalize_runtime_guard_config(raw)
    return {
        "run_start_time": config.run_start_time.isoformat() if config.run_start_time else None,
        "run_end_time": config.run_end_time.isoformat() if config.run_end_time else None,
        "rolling_hourly_loss_limit": config.rolling_hourly_loss_limit,
        "max_cumulative_notional": config.max_cumulative_notional,
    }


def evaluate_runtime_guards(
    *,
    config: RuntimeGuardConfig,
    now: datetime,
    cumulative_gross_notional: float,
    pnl_events: list[dict[str, Any]],
) -> RuntimeGuardResult:
    current = now.astimezone(timezone.utc)
    reasons: list[str] = []

    window_start = current - timedelta(minutes=60)
    window_net_pnl = 0.0
    for event in pnl_events:
        event_ts = _parse_datetime(event.get("ts"), "ts")
        if event_ts is None or event_ts < window_start or event_ts > current:
            continue
        window_net_pnl += _event_net_pnl(event)
    rolling_loss = max(0.0, -window_net_pnl)

    if config.run_start_time and current < config.run_start_time:
        return RuntimeGuardResult(
            tradable=False,
            stop_triggered=False,
            runtime_status="waiting",
            primary_reason="before_start_window",
            matched_reasons=["before_start_window"],
            triggered_at=None,
            rolling_hourly_loss=rolling_loss,
            cumulative_gross_notional=float(cumulative_gross_notional),
        )

    if config.run_end_time and current >= config.run_end_time:
        reasons.append("after_end_window")
    if config.rolling_hourly_loss_limit is not None and rolling_loss >= config.rolling_hourly_loss_limit:
        reasons.append("rolling_hourly_loss_limit_hit")
    if config.max_cumulative_notional is not None and float(cumulative_gross_notional) >= config.max_cumulative_notional:
        reasons.append("max_cumulative_notional_hit")

    return RuntimeGuardResult(
        tradable=not reasons,
        stop_triggered=bool(reasons),
        runtime_status="stopped" if reasons else "running",
        primary_reason=reasons[0] if reasons else None,
        matched_reasons=reasons,
        triggered_at=current.isoformat() if reasons else None,
        rolling_hourly_loss=rolling_loss,
        cumulative_gross_notional=float(cumulative_gross_notional),
    )
