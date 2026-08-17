from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .audit import (
    build_audit_paths,
    income_row_key,
    income_row_time_ms,
    read_jsonl,
    read_income_audit_rows_with_integrity,
    read_trade_audit_rows_with_integrity,
    trade_row_time_ms,
)
from .competition_board import resolve_active_competition_board


_BEIJING_TZ = timezone(timedelta(hours=8))
_BEIJING_DAILY_8_STATS_TOKENS = {
    "beijing_08_daily",
    "beijing_8_daily",
    "beijing_daily_08",
    "beijing_daily_8",
    "asia_shanghai_08_daily",
}


def best_quote_order_ref_audit_path(state_path: Path) -> Path:
    """Return the append-only book-classification ledger for submitted orders."""

    path = Path(state_path)
    return path.with_name(f"{path.stem}_order_refs.jsonl")


def _beijing_daily_8_start(now: datetime | None = None) -> datetime:
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        raise ValueError("now must include timezone information")
    local = current.astimezone(_BEIJING_TZ)
    start = local.replace(hour=8, minute=0, second=0, microsecond=0)
    if local < start:
        start -= timedelta(days=1)
    return start.astimezone(timezone.utc)


def _parse_datetime(value: Any, field_name: str, *, now: datetime | None = None) -> datetime | None:
    if value in {"", None}:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        text = str(value).strip()
        if field_name == "runtime_guard_stats_start_time" and text.lower() in _BEIJING_DAILY_8_STATS_TOKENS:
            return _beijing_daily_8_start(now=now)
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        raise ValueError(f"{field_name} must include timezone information")
    return dt.astimezone(timezone.utc)


def _parse_positive_float(value: Any, field_name: str) -> float | None:
    if value in {"", None}:
        return None
    parsed = float(value)
    if not math.isfinite(parsed) or parsed <= 0:
        raise ValueError(f"{field_name} must be a finite number > 0")
    return parsed


def _parse_nonnegative_float(value: Any, field_name: str) -> float | None:
    if value in {"", None}:
        return None
    parsed = float(value)
    if not math.isfinite(parsed) or parsed < 0:
        raise ValueError(f"{field_name} must be non-negative")
    return parsed


def _event_net_pnl(item: dict[str, Any]) -> float:
    if "net_pnl" in item:
        return float(item.get("net_pnl") or 0.0)
    realized = float(item.get("realized_pnl") or 0.0)
    funding = float(item.get("funding_fee") or item.get("income") or 0.0)
    commission = float(item.get("commission_quote") or 0.0)
    recycle_loss = float(item.get("recycle_loss_abs") or 0.0)
    return realized + funding + commission - recycle_loss


def summarize_runtime_total_pnl(
    pnl_events: list[dict[str, Any]],
    *,
    start_time: datetime | None,
    now: datetime,
    unrealized_pnl: float | None,
) -> float | None:
    """Return run-window realized/funding/fees plus current ordinary uPnL."""

    try:
        safe_unrealized_pnl = float(unrealized_pnl) if unrealized_pnl is not None else None
    except (TypeError, ValueError):
        return None
    if safe_unrealized_pnl is None or not math.isfinite(safe_unrealized_pnl):
        return None

    current = now.astimezone(timezone.utc)
    realized = 0.0
    for event in pnl_events:
        event_ts = _parse_datetime(event.get("ts"), "ts")
        if event_ts is None or event_ts > current:
            continue
        if start_time is not None and event_ts < start_time:
            continue
        if event.get("pnl_observation_available") is False:
            return None
        try:
            event_pnl = _event_net_pnl(event)
        except (TypeError, ValueError):
            return None
        if not math.isfinite(event_pnl):
            return None
        realized += event_pnl
        if not math.isfinite(realized):
            return None
    total = realized + safe_unrealized_pnl
    return total if math.isfinite(total) else None


def _trade_order_identity(row: dict[str, Any]) -> str:
    for key in ("orderId", "order_id", "i"):
        value = row.get(key)
        if value not in {"", None}:
            return str(value)
    client_id = row.get("clientOrderId") or row.get("client_order_id") or row.get("origClientOrderId")
    if client_id not in {"", None}:
        return f"client:{client_id}"
    trade_id = row.get("tradeId") or row.get("trade_id") or row.get("t")
    if trade_id not in {"", None}:
        return f"trade:{trade_id}"
    return ""


def _trade_notional(row: dict[str, Any]) -> float:
    def _as_float(value: Any) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return 0.0

    for key in ("quoteQty", "quote_qty", "quote_asset_qty"):
        quote_qty = abs(_as_float(row.get(key)))
        if quote_qty > 0:
            return quote_qty
    return abs(_as_float(row.get("price")) * _as_float(row.get("qty")))


@dataclass(frozen=True)
class RuntimeGuardConfig:
    run_start_time: datetime | None
    run_end_time: datetime | None
    rolling_hourly_loss_limit: float | None
    rolling_hourly_loss_per_10k_limit: float | None
    rolling_hourly_loss_per_10k_min_notional: float
    max_cumulative_notional: float | None
    max_actual_net_notional: float | None
    max_synthetic_drift_notional: float | None
    max_unrealized_loss: float | None = None
    runtime_guard_stats_start_time: datetime | None = None
    target_min_total_pnl: float | None = None


@dataclass(frozen=True)
class RuntimeGuardResult:
    tradable: bool
    stop_triggered: bool
    runtime_status: str
    primary_reason: str | None
    matched_reasons: list[str]
    triggered_at: str | None
    rolling_hourly_loss: float
    rolling_hourly_gross_notional: float
    rolling_hourly_loss_per_10k: float
    rolling_hourly_loss_per_10k_active: bool
    cumulative_gross_notional: float
    actual_net_notional_abs: float
    synthetic_drift_notional: float
    unrealized_loss: float = 0.0
    target_total_pnl: float | None = None
    target_profit_gate_active: bool = False
    target_profit_satisfied: bool = False


def resolve_runtime_guard_stats_start_time(
    *,
    runtime_guard_stats_start_time: Any = None,
    symbol: str | None = None,
    market: str = "futures",
    now: datetime | None = None,
) -> datetime | None:
    try:
        explicit_start = _parse_datetime(
            runtime_guard_stats_start_time,
            "runtime_guard_stats_start_time",
            now=now,
        )
    except ValueError:
        explicit_start = None
    # An explicit start belongs to the run contract.  Competition-board
    # metadata is only a legacy/default fallback when no explicit boundary was
    # supplied; it must never move a running contract's accounting window.
    if explicit_start is not None:
        return explicit_start
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
        board_start = _parse_datetime(board.get("activity_start_at"), "activity_start_at", now=now)
    except ValueError:
        return explicit_start
    return board_start


def summarize_futures_runtime_guard_inputs(
    summary_path: Path,
    *,
    runtime_guard_stats_start_time: Any = None,
    symbol: str | None = None,
    now: datetime | None = None,
    bq_order_refs_path: Path | None = None,
    bq_book_scope: str | None = None,
    immutable_window: bool = False,
    runtime_guard_stats_end_time: Any = None,
) -> tuple[float, list[dict[str, Any]], datetime | None]:
    audit_paths = build_audit_paths(summary_path)
    trade_rows, trade_audit_complete = read_trade_audit_rows_with_integrity(
        audit_paths["trade_audit"]
    )
    income_rows, income_audit_complete = read_income_audit_rows_with_integrity(
        audit_paths["income_audit"]
    )
    metrics_start_time = (
        _parse_datetime(
            runtime_guard_stats_start_time,
            "runtime_guard_stats_start_time",
            now=now,
        )
        if immutable_window
        else resolve_runtime_guard_stats_start_time(
            runtime_guard_stats_start_time=runtime_guard_stats_start_time,
            symbol=symbol,
            market="futures",
            now=now,
        )
    )
    metrics_end_time = _parse_datetime(
        runtime_guard_stats_end_time,
        "runtime_guard_stats_end_time",
        now=now,
    )
    audit_state: dict[str, Any] = {}
    if audit_paths["audit_state"].exists():
        try:
            raw_audit_state = json.loads(
                audit_paths["audit_state"].read_text(encoding="utf-8")
            )
        except (OSError, json.JSONDecodeError):
            raw_audit_state = None
        if isinstance(raw_audit_state, dict):
            audit_state = raw_audit_state
    try:
        income_last_time_ms = int(audit_state.get("income_last_time_ms") or 0)
    except (TypeError, ValueError):
        income_last_time_ms = 0
        income_audit_complete = False
    if income_last_time_ms > 0:
        local_income_last_time_ms = max(
            (income_row_time_ms(row) for row in income_rows),
            default=0,
        )
        expected_income_keys = {
            str(key)
            for key in audit_state.get("income_last_keys_at_time", [])
            if str(key).strip()
        }
        local_income_keys_at_watermark = {
            income_row_key(row)
            for row in income_rows
            if income_row_time_ms(row) == income_last_time_ms
        }
        if (
            local_income_last_time_ms < income_last_time_ms
            or not expected_income_keys.issubset(local_income_keys_at_watermark)
        ):
            income_audit_complete = False
    cumulative_gross_notional = 0.0
    pnl_events: list[dict[str, Any]] = []
    stable_assets = {"USDT", "USDC", "FDUSD", "BUSD"}
    normalized_symbol = str(symbol or "").upper().strip()
    quote_asset = next(
        (asset for asset in stable_assets if normalized_symbol.endswith(asset)),
        "",
    )
    seen_order_notional_keys: set[str] = set()
    normalized_bq_book_scope = str(bq_book_scope or "").lower().strip()
    bq_order_books: dict[str, str] = {}
    if bq_order_refs_path is not None:
        for archived_ref in read_jsonl(
            best_quote_order_ref_audit_path(bq_order_refs_path),
            limit=0,
        ):
            order_id = str(archived_ref.get("order_id") or "").strip()
            book = str(archived_ref.get("book") or "unknown").lower().strip()
            if order_id and book in {"normal_bq", "frozen_bq"}:
                bq_order_books[order_id] = book
        try:
            raw_state = json.loads(bq_order_refs_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            raw_state = {}
        raw_refs = raw_state.get("best_quote_volume_order_refs") if isinstance(raw_state, dict) else {}
        if isinstance(raw_refs, dict):
            for order_id, ref in raw_refs.items():
                if not isinstance(ref, dict):
                    continue
                bq_order_books[str(order_id)] = str(ref.get("book") or "unknown").lower().strip() or "unknown"

    def _historical_bq_book(row: dict[str, Any]) -> str:
        order_id = str(row.get("orderId") or row.get("order_id") or "").strip()
        referenced_book = bq_order_books.get(order_id, "unknown")
        if referenced_book != "unknown":
            return referenced_book
        client_order_id = str(
            row.get("clientOrderId")
            or row.get("client_order_id")
            or row.get("origClientOrderId")
            or ""
        ).lower().strip()
        compact_symbol = str(symbol or "").lower().strip().replace("usdt", "u")
        expected_prefix = f"gx-{compact_symbol}-" if compact_symbol else ""
        if not expected_prefix or not client_order_id.startswith(expected_prefix):
            return "unknown"
        parts = client_order_id.split("-")
        role_token = parts[2] if len(parts) >= 3 else ""
        if role_token in {"bestquot", "hardloss", "inventor", "rc", "tlr"}:
            return "normal_bq"
        if role_token in {"frozenin", "frozenpl", "frpl", "frps"}:
            return "frozen_bq"
        return "unknown"

    def _is_frozen_pair_release_trade(row: dict[str, Any]) -> bool:
        client_order_id = str(
            row.get("clientOrderId")
            or row.get("client_order_id")
            or row.get("origClientOrderId")
            or ""
        ).lower()
        if "frozenin" in client_order_id:
            return True
        order_id = str(row.get("orderId") or row.get("order_id") or "").strip()
        return bool(order_id and bq_order_books.get(order_id) == "frozen_bq")

    def _as_float(value: Any) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return 0.0

    def _finite_float(value: Any) -> float | None:
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            return None
        return parsed if math.isfinite(parsed) else None

    proof_ts = (
        metrics_start_time
        or (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    ).isoformat()

    def _mark_pnl_proof_unavailable(reason: str) -> None:
        pnl_events.append(
            {
                "ts": proof_ts,
                "net_pnl": 0.0,
                "pnl_event_type": "audit_integrity",
                "pnl_observation_available": False,
                "pnl_unavailable_reason": reason,
                "gross_notional": 0.0,
                "client_order_id": "",
                "order_id": None,
                "side": "",
                "position_side": "",
            }
        )

    if not trade_audit_complete:
        _mark_pnl_proof_unavailable("trade_audit_incomplete")
    if not income_audit_complete:
        _mark_pnl_proof_unavailable("income_audit_incomplete")

    for row in trade_rows:
        trade_time_ms = trade_row_time_ms(row)
        trade_ts: datetime | None = None
        if trade_time_ms > 0:
            trade_ts = datetime.fromtimestamp(trade_time_ms / 1000.0, tz=timezone.utc)
        if trade_ts is None:
            if normalized_bq_book_scope:
                historical_book = _historical_bq_book(row)
                if historical_book != normalized_bq_book_scope:
                    if historical_book == "unknown":
                        _mark_pnl_proof_unavailable("trade_book_unknown")
                    continue
            if not _is_frozen_pair_release_trade(row):
                _mark_pnl_proof_unavailable("trade_time_invalid")
            continue
        if metrics_start_time is not None:
            if trade_ts < metrics_start_time:
                continue
        if metrics_end_time is not None:
            if trade_ts >= metrics_end_time:
                continue
        if normalized_bq_book_scope:
            historical_book = _historical_bq_book(row)
            if historical_book != normalized_bq_book_scope:
                if historical_book == "unknown":
                    _mark_pnl_proof_unavailable("trade_book_unknown")
                continue
        price = _as_float(row.get("price"))
        qty = abs(_as_float(row.get("qty")))
        notional = _trade_notional(row)
        order_identity = _trade_order_identity(row)
        if order_identity:
            if order_identity not in seen_order_notional_keys:
                cumulative_gross_notional += notional
                seen_order_notional_keys.add(order_identity)
        else:
            cumulative_gross_notional += notional
        if _is_frozen_pair_release_trade(row):
            continue
        observed_realized_pnl = _finite_float(row.get("realizedPnl"))
        observed_commission = _finite_float(row.get("commission"))
        realized_pnl = observed_realized_pnl or 0.0
        commission = observed_commission or 0.0
        commission_asset = str(row.get("commissionAsset", "")).upper().strip()
        pnl_observation_available = bool(
            observed_realized_pnl is not None
            and observed_commission is not None
            and (commission == 0.0 or commission_asset == quote_asset)
        )
        net_pnl = realized_pnl - (commission if commission_asset == quote_asset else 0.0)
        pnl_events.append(
            {
                "ts": trade_ts.isoformat(),
                "net_pnl": net_pnl,
                "pnl_observation_available": pnl_observation_available,
                "gross_notional": notional,
                "client_order_id": str(
                    row.get("clientOrderId")
                    or row.get("client_order_id")
                    or row.get("origClientOrderId")
                    or ""
                ),
                "order_id": row.get("orderId") or row.get("order_id"),
                "side": str(row.get("side") or ""),
                "position_side": str(row.get("positionSide") or row.get("position_side") or ""),
            }
        )

    for row in income_rows:
        income_time_ms = income_row_time_ms(row)
        if income_time_ms <= 0:
            _mark_pnl_proof_unavailable("income_time_invalid")
            continue
        income_ts = datetime.fromtimestamp(income_time_ms / 1000.0, tz=timezone.utc)
        if metrics_start_time is not None and income_ts < metrics_start_time:
            continue
        if metrics_end_time is not None and income_ts >= metrics_end_time:
            continue
        observed_income = _finite_float(row.get("income"))
        pnl_events.append(
            {
                "ts": income_ts.isoformat(),
                "net_pnl": observed_income or 0.0,
                "pnl_event_type": "income",
                "pnl_observation_available": observed_income is not None,
                "gross_notional": 0.0,
            }
        )

    return cumulative_gross_notional, pnl_events, metrics_start_time


def normalize_runtime_guard_config(raw: dict[str, Any], *, now: datetime | None = None) -> RuntimeGuardConfig:
    config = RuntimeGuardConfig(
        run_start_time=_parse_datetime(raw.get("run_start_time"), "run_start_time"),
        run_end_time=_parse_datetime(raw.get("run_end_time"), "run_end_time"),
        rolling_hourly_loss_limit=_parse_positive_float(
            raw.get("rolling_hourly_loss_limit"),
            "rolling_hourly_loss_limit",
        ),
        rolling_hourly_loss_per_10k_limit=_parse_positive_float(
            raw.get("rolling_hourly_loss_per_10k_limit"),
            "rolling_hourly_loss_per_10k_limit",
        ),
        rolling_hourly_loss_per_10k_min_notional=(
            _parse_positive_float(
                raw.get("rolling_hourly_loss_per_10k_min_notional"),
                "rolling_hourly_loss_per_10k_min_notional",
            )
            or 10000.0
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
        max_unrealized_loss=_parse_positive_float(
            raw.get("max_unrealized_loss"),
            "max_unrealized_loss",
        ),
        runtime_guard_stats_start_time=_parse_datetime(
            raw.get("runtime_guard_stats_start_time"),
            "runtime_guard_stats_start_time",
            now=now,
        ),
        target_min_total_pnl=_parse_nonnegative_float(
            raw.get("target_min_total_pnl"),
            "target_min_total_pnl",
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
    config = normalize_runtime_guard_config(raw, now=now)
    normalized_symbol = str(symbol or raw.get("symbol") or "").upper().strip() or None
    resolved_stats_start_time = (
        config.runtime_guard_stats_start_time
        if config.max_cumulative_notional is not None
        else resolve_runtime_guard_stats_start_time(
            runtime_guard_stats_start_time=config.runtime_guard_stats_start_time,
            symbol=normalized_symbol,
            market=market,
            now=now,
        )
    )
    return {
        "run_start_time": config.run_start_time.isoformat() if config.run_start_time else None,
        "run_end_time": config.run_end_time.isoformat() if config.run_end_time else None,
        "rolling_hourly_loss_limit": config.rolling_hourly_loss_limit,
        "rolling_hourly_loss_per_10k_limit": config.rolling_hourly_loss_per_10k_limit,
        "rolling_hourly_loss_per_10k_min_notional": config.rolling_hourly_loss_per_10k_min_notional,
        "max_cumulative_notional": config.max_cumulative_notional,
        "max_actual_net_notional": config.max_actual_net_notional,
        "max_synthetic_drift_notional": config.max_synthetic_drift_notional,
        "max_unrealized_loss": config.max_unrealized_loss,
        "target_min_total_pnl": config.target_min_total_pnl,
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
    unrealized_pnl: float | None = None,
    target_total_pnl: float | None = None,
) -> RuntimeGuardResult:
    current = now.astimezone(timezone.utc)
    reasons: list[str] = []
    actual_net_notional_abs = abs(float(actual_net_notional or 0.0))
    safe_synthetic_drift_notional = max(float(synthetic_drift_notional or 0.0), 0.0)
    unrealized_loss = max(0.0, -float(unrealized_pnl or 0.0))
    safe_target_total_pnl = (
        float(target_total_pnl)
        if target_total_pnl is not None and math.isfinite(float(target_total_pnl))
        else None
    )
    target_profit_gate_active = config.target_min_total_pnl is not None
    target_profit_satisfied = bool(
        target_profit_gate_active
        and safe_target_total_pnl is not None
        and safe_target_total_pnl + 1e-12 >= float(config.target_min_total_pnl)
    )

    window_start = current - timedelta(minutes=60)
    window_net_pnl = 0.0
    window_gross_notional = 0.0
    for event in pnl_events:
        event_ts = _parse_datetime(event.get("ts"), "ts")
        if event_ts is None or event_ts < window_start or event_ts > current:
            continue
        if config.runtime_guard_stats_start_time and event_ts < config.runtime_guard_stats_start_time:
            continue
        window_net_pnl += _event_net_pnl(event)
        try:
            window_gross_notional += max(float(event.get("gross_notional") or 0.0), 0.0)
        except (TypeError, ValueError):
            pass
    rolling_loss = max(0.0, -window_net_pnl)
    rolling_loss_per_10k = rolling_loss / (window_gross_notional / 10000.0) if window_gross_notional > 0 else 0.0
    rolling_loss_per_10k_active = window_gross_notional >= config.rolling_hourly_loss_per_10k_min_notional

    if config.run_start_time and current < config.run_start_time:
        return RuntimeGuardResult(
            tradable=False,
            stop_triggered=False,
            runtime_status="waiting",
            primary_reason="before_start_window",
            matched_reasons=["before_start_window"],
            triggered_at=None,
            rolling_hourly_loss=rolling_loss,
            rolling_hourly_gross_notional=window_gross_notional,
            rolling_hourly_loss_per_10k=rolling_loss_per_10k,
            rolling_hourly_loss_per_10k_active=rolling_loss_per_10k_active,
            cumulative_gross_notional=float(cumulative_gross_notional),
            actual_net_notional_abs=actual_net_notional_abs,
            synthetic_drift_notional=safe_synthetic_drift_notional,
            unrealized_loss=unrealized_loss,
        )

    if config.run_end_time and current >= config.run_end_time:
        reasons.append("after_end_window")
    if config.rolling_hourly_loss_limit is not None and rolling_loss >= config.rolling_hourly_loss_limit:
        reasons.append("rolling_hourly_loss_limit_hit")
    if (
        config.rolling_hourly_loss_per_10k_limit is not None
        and rolling_loss_per_10k_active
        and rolling_loss_per_10k >= config.rolling_hourly_loss_per_10k_limit
    ):
        reasons.append("rolling_hourly_loss_per_10k_limit_hit")
    if (
        config.max_cumulative_notional is not None
        and float(cumulative_gross_notional) >= config.max_cumulative_notional
        and (not target_profit_gate_active or target_profit_satisfied)
    ):
        reasons.append("max_cumulative_notional_hit")
    if config.max_actual_net_notional is not None and actual_net_notional_abs >= config.max_actual_net_notional:
        reasons.append("max_actual_net_notional_hit")
    if (
        config.max_synthetic_drift_notional is not None
        and safe_synthetic_drift_notional >= config.max_synthetic_drift_notional
    ):
        reasons.append("max_synthetic_drift_notional_hit")
    if config.max_unrealized_loss is not None and unrealized_loss >= config.max_unrealized_loss:
        reasons.append("max_unrealized_loss_hit")

    return RuntimeGuardResult(
        tradable=not reasons,
        stop_triggered=bool(reasons),
        runtime_status="stopped" if reasons else "running",
        primary_reason=reasons[0] if reasons else None,
        matched_reasons=reasons,
        triggered_at=current.isoformat() if reasons else None,
        rolling_hourly_loss=rolling_loss,
        rolling_hourly_gross_notional=window_gross_notional,
        rolling_hourly_loss_per_10k=rolling_loss_per_10k,
        rolling_hourly_loss_per_10k_active=rolling_loss_per_10k_active,
        cumulative_gross_notional=float(cumulative_gross_notional),
        actual_net_notional_abs=actual_net_notional_abs,
        synthetic_drift_notional=safe_synthetic_drift_notional,
        unrealized_loss=unrealized_loss,
        target_total_pnl=safe_target_total_pnl,
        target_profit_gate_active=target_profit_gate_active,
        target_profit_satisfied=target_profit_satisfied,
    )
