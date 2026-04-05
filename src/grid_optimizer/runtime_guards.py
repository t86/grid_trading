from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .audit import build_audit_paths, income_row_time_ms, read_jsonl, trade_row_time_ms
from .competition_board import resolve_active_competition_board


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
    max_actual_net_notional: float | None
    max_synthetic_drift_notional: float | None
    runtime_guard_stats_start_time: datetime | None = None


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
    actual_net_notional_abs: float
    synthetic_drift_notional: float


def resolve_runtime_guard_stats_start_time(
    *,
    runtime_guard_stats_start_time: Any = None,
    symbol: str | None = None,
    market: str = "futures",
    now: datetime | None = None,
) -> datetime | None:
    try:
        explicit_start = _parse_datetime(runtime_guard_stats_start_time, "runtime_guard_stats_start_time")
    except ValueError:
        explicit_start = None
    normalized_symbol = str(symbol or "").upper().strip()
    normalized_market = str(market or "").strip().lower()
    if not normalized_symbol or not normalized_market:
        return explicit_start
    try:
        board = resolve_active_competition_board(normalized_symbol, normalized_market, now=now)
    except Exception:
        return explicit_start
    if not isinstance(board, dict):
        return explicit_start
    try:
        board_start = _parse_datetime(board.get("activity_start_at"), "activity_start_at")
    except ValueError:
        return explicit_start
    if explicit_start is None:
        return board_start
    if board_start is None:
        return explicit_start
    return max(explicit_start, board_start)


def summarize_futures_runtime_guard_inputs(
    summary_path: Path,
    *,
    runtime_guard_stats_start_time: Any = None,
    symbol: str | None = None,
    now: datetime | None = None,
) -> tuple[float, list[dict[str, Any]], datetime | None]:
    audit_paths = build_audit_paths(summary_path)
    trade_rows = read_jsonl(audit_paths["trade_audit"], limit=0)
    income_rows = read_jsonl(audit_paths["income_audit"], limit=0)
    metrics_start_time = resolve_runtime_guard_stats_start_time(
        runtime_guard_stats_start_time=runtime_guard_stats_start_time,
        symbol=symbol,
        market="futures",
        now=now,
    )
    cumulative_gross_notional = 0.0
    pnl_events: list[dict[str, Any]] = []
    stable_assets = {"USDT", "USDC", "FDUSD", "BUSD"}

    def _as_float(value: Any) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return 0.0

    for row in trade_rows:
        trade_time_ms = trade_row_time_ms(row)
        trade_ts: datetime | None = None
        if trade_time_ms > 0:
            trade_ts = datetime.fromtimestamp(trade_time_ms / 1000.0, tz=timezone.utc)
        if metrics_start_time is not None:
            if trade_ts is None or trade_ts < metrics_start_time:
                continue
        price = _as_float(row.get("price"))
        qty = abs(_as_float(row.get("qty")))
        cumulative_gross_notional += price * qty
        if trade_ts is None:
            continue
        realized_pnl = _as_float(row.get("realizedPnl"))
        commission = _as_float(row.get("commission"))
        commission_asset = str(row.get("commissionAsset", "")).upper().strip()
        net_pnl = realized_pnl - (commission if commission_asset in stable_assets else 0.0)
        pnl_events.append(
            {
                "ts": trade_ts.isoformat(),
                "net_pnl": net_pnl,
            }
        )

    for row in income_rows:
        income_time_ms = income_row_time_ms(row)
        if income_time_ms <= 0:
            continue
        income_ts = datetime.fromtimestamp(income_time_ms / 1000.0, tz=timezone.utc)
        if metrics_start_time is not None and income_ts < metrics_start_time:
            continue
        pnl_events.append(
            {
                "ts": income_ts.isoformat(),
                "net_pnl": _as_float(row.get("income")),
            }
        )

    return cumulative_gross_notional, pnl_events, metrics_start_time


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
        max_actual_net_notional=_parse_positive_float(
            raw.get("max_actual_net_notional"),
            "max_actual_net_notional",
        ),
        max_synthetic_drift_notional=_parse_positive_float(
            raw.get("max_synthetic_drift_notional"),
            "max_synthetic_drift_notional",
        ),
        runtime_guard_stats_start_time=_parse_datetime(
            raw.get("runtime_guard_stats_start_time"),
            "runtime_guard_stats_start_time",
        ),
    )
    if config.run_start_time and config.run_end_time and config.run_start_time >= config.run_end_time:
        raise ValueError("run_start_time must be earlier than run_end_time")
    return config


def normalize_runtime_guard_payload(
    raw: dict[str, Any],
    *,
    symbol: str | None = None,
    market: str = "futures",
    now: datetime | None = None,
) -> dict[str, Any]:
    config = normalize_runtime_guard_config(raw)
    normalized_symbol = str(symbol or raw.get("symbol") or "").upper().strip() or None
    resolved_stats_start_time = resolve_runtime_guard_stats_start_time(
        runtime_guard_stats_start_time=config.runtime_guard_stats_start_time,
        symbol=normalized_symbol,
        market=market,
        now=now,
    )
    return {
        "run_start_time": config.run_start_time.isoformat() if config.run_start_time else None,
        "run_end_time": config.run_end_time.isoformat() if config.run_end_time else None,
        "rolling_hourly_loss_limit": config.rolling_hourly_loss_limit,
        "max_cumulative_notional": config.max_cumulative_notional,
        "max_actual_net_notional": config.max_actual_net_notional,
        "max_synthetic_drift_notional": config.max_synthetic_drift_notional,
        "runtime_guard_stats_start_time": resolved_stats_start_time.isoformat() if resolved_stats_start_time else None,
    }


def evaluate_runtime_guards(
    *,
    config: RuntimeGuardConfig,
    now: datetime,
    cumulative_gross_notional: float,
    pnl_events: list[dict[str, Any]],
    actual_net_notional: float | None = None,
    synthetic_drift_notional: float | None = None,
) -> RuntimeGuardResult:
    current = now.astimezone(timezone.utc)
    reasons: list[str] = []
    actual_net_notional_abs = abs(float(actual_net_notional or 0.0))
    safe_synthetic_drift_notional = max(float(synthetic_drift_notional or 0.0), 0.0)

    window_start = current - timedelta(minutes=60)
    window_net_pnl = 0.0
    for event in pnl_events:
        event_ts = _parse_datetime(event.get("ts"), "ts")
        if event_ts is None or event_ts < window_start or event_ts > current:
            continue
        if config.runtime_guard_stats_start_time and event_ts < config.runtime_guard_stats_start_time:
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
            actual_net_notional_abs=actual_net_notional_abs,
            synthetic_drift_notional=safe_synthetic_drift_notional,
        )

    if config.run_end_time and current >= config.run_end_time:
        reasons.append("after_end_window")
    if config.rolling_hourly_loss_limit is not None and rolling_loss >= config.rolling_hourly_loss_limit:
        reasons.append("rolling_hourly_loss_limit_hit")
    if config.max_cumulative_notional is not None and float(cumulative_gross_notional) >= config.max_cumulative_notional:
        reasons.append("max_cumulative_notional_hit")
    if config.max_actual_net_notional is not None and actual_net_notional_abs >= config.max_actual_net_notional:
        reasons.append("max_actual_net_notional_hit")
    if (
        config.max_synthetic_drift_notional is not None
        and safe_synthetic_drift_notional >= config.max_synthetic_drift_notional
    ):
        reasons.append("max_synthetic_drift_notional_hit")

    return RuntimeGuardResult(
        tradable=not reasons,
        stop_triggered=bool(reasons),
        runtime_status="stopped" if reasons else "running",
        primary_reason=reasons[0] if reasons else None,
        matched_reasons=reasons,
        triggered_at=current.isoformat() if reasons else None,
        rolling_hourly_loss=rolling_loss,
        cumulative_gross_notional=float(cumulative_gross_notional),
        actual_net_notional_abs=actual_net_notional_abs,
        synthetic_drift_notional=safe_synthetic_drift_notional,
    )
