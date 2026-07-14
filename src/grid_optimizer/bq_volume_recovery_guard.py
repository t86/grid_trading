from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from math import ceil
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping

from .recovery_control_ownership import (
    exclusive_control_lock,
    mark_recovery_owned,
    write_control_json_atomically,
)


RestartRunner = Callable[[str], object]
UserTradesPageFetcher = Callable[..., list[dict[str, Any]]]
CorruptStateExchangeFetcher = Callable[[str], dict[str, Any]]


@dataclass(frozen=True)
class RecoveryParameters:
    """Resolved recovery parameters; action branches must not re-derive them."""

    per_order_notional: float
    configured_min_cycle_budget_notional: float
    static_cycle_budget_floor_notional: float
    effective_cycle_budget_floor_notional: float
    loss_reduce_cycle_budget_cap_notional: float
    cycle_budget_increment_notional: float


def _resolve_recovery_parameters(
    *,
    control: dict[str, Any],
    static_cycle_budget_floor_notional: float,
    effective_cycle_budget_floor_notional: float | None = None,
    cycle_budget_increment_notional: float = 0.0,
) -> RecoveryParameters:
    """Resolve runner control > guard configuration > derived fallback once."""
    per_order = max(
        _safe_float(control.get("per_order_notional")),
        _safe_float(control.get("maker_order_notional")),
    )
    configured_min = max(
        _safe_float(control.get("best_quote_maker_volume_min_cycle_budget_notional")),
        0.0,
    )
    static_floor = max(float(static_cycle_budget_floor_notional), 0.0)
    # per_order*4 is only a legacy fallback when neither authoritative source
    # provides a minimum. It must never compete with an explicit runner value.
    authoritative_floor = configured_min or static_floor or per_order * 4.0
    effective_floor = max(
        authoritative_floor,
        max(float(effective_cycle_budget_floor_notional or 0.0), 0.0),
    )
    return RecoveryParameters(
        per_order_notional=per_order,
        configured_min_cycle_budget_notional=configured_min,
        static_cycle_budget_floor_notional=static_floor,
        effective_cycle_budget_floor_notional=effective_floor,
        loss_reduce_cycle_budget_cap_notional=authoritative_floor,
        cycle_budget_increment_notional=max(float(cycle_budget_increment_notional), 0.0),
    )


_RECOVERY_CONTROL_KEYS = (
    "best_quote_maker_volume_allow_loss_reduce_only",
    "best_quote_maker_volume_active_pair_reduce_enabled",
    "best_quote_maker_volume_active_pair_reduce_order_notional",
    "best_quote_maker_volume_active_pair_reduce_max_notional_per_side",
    "best_quote_maker_volume_inventory_cost_gate_enabled",
    "best_quote_maker_volume_inventory_bias_min_notional_gap",
    "best_quote_maker_volume_inventory_bias_reduce_share",
    "best_quote_maker_volume_inventory_soft_ratio",
    "best_quote_maker_volume_dynamic_control_trend_entry_guard_enabled",
    "best_quote_maker_volume_dynamic_control_trend_inventory_guard_enabled",
    "best_quote_maker_volume_dynamic_control_trend_loss_reduce_guard_enabled",
    "best_quote_maker_volume_inventory_bias_enabled",
    "best_quote_maker_volume_same_side_entry_price_guard_report_only",
    "best_quote_maker_volume_cycle_budget_notional",
    "best_quote_maker_volume_quote_offset_ticks",
    "best_quote_maker_volume_same_side_entry_price_guard_min_notional",
    "best_quote_maker_volume_suppress_short_reduce_enabled",
    "best_quote_maker_volume_directional_net_guard",
    "max_actual_net_notional",
    "max_position_notional",
    "max_short_position_notional",
    "maker_max_long_notional",
    "maker_max_short_notional",
    "best_quote_maker_volume_max_long_notional",
    "best_quote_maker_volume_max_short_notional",
    "best_quote_maker_volume_min_cycle_budget_notional",
    "pause_buy_position_notional",
    "pause_short_position_notional",
    "sticky_entry_price_tolerance_steps",
    "sticky_entry_preserve_less_aggressive",
    "sticky_exit_price_tolerance_steps",
)


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    write_control_json_atomically(path, payload)


def _append_jsonl(path: Path, row: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")


def _target_gate_done_marker(output_dir: Path, symbol: str, now: datetime) -> Path:
    return output_dir / f"{symbol.lower()}_target_gate_done_{now.strftime('%Y%m%d')}.flag"


def _fetch_corrupt_state_exchange_snapshot(symbol: str) -> dict[str, Any]:
    from .data import fetch_futures_open_orders, fetch_futures_position_risk_v3

    api_key = os.environ["BINANCE_API_KEY"]
    api_secret = os.environ["BINANCE_API_SECRET"]
    positions = fetch_futures_position_risk_v3(api_key, api_secret, symbol)
    return {
        "open_order_count": len(
            fetch_futures_open_orders(symbol, api_key, api_secret, use_cache=False)
        ),
        "long_notional": sum(
            abs(_safe_float(row.get("notional")))
            for row in positions
            if str(row.get("positionSide", "")).upper() == "LONG"
        ),
        "short_notional": sum(
            abs(_safe_float(row.get("notional")))
            for row in positions
            if str(row.get("positionSide", "")).upper() == "SHORT"
        ),
    }


def _fetch_arx_strategy_order_snapshot(symbol: str) -> dict[str, Any]:
    """Return the exchange truth for ARX runner-owned orders only."""
    from .data import fetch_futures_open_orders

    api_key = os.environ["BINANCE_API_KEY"]
    api_secret = os.environ["BINANCE_API_SECRET"]
    prefix = "gx-arxu"
    orders = fetch_futures_open_orders(
        symbol, api_key, api_secret, use_cache=False
    )
    strategy_orders = [
        order
        for order in orders
        if str(order.get("clientOrderId") or "").lower().startswith(prefix)
    ]
    return {
        "strategy_open_order_count": len(strategy_orders),
        "strategy_order_ids": [str(order.get("orderId") or "") for order in strategy_orders],
    }


def _salvage_truncated_order_refs_state(
    *,
    state_path: Path,
    plan: dict[str, Any],
    state_mtime: float,
    exchange_snapshot_fetcher: CorruptStateExchangeFetcher,
    symbol: str,
) -> tuple[dict[str, Any] | None, list[str], dict[str, Any]]:
    reasons: list[str] = []
    details: dict[str, Any] = {}
    try:
        raw = state_path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as exc:
        return None, ["corrupt_state_unreadable"], {"salvage_error": str(exc)[:240]}
    marker = '\n  "best_quote_volume_order_refs": {'
    marker_at = raw.rfind(marker)
    if marker_at < 0:
        return None, ["corruption_not_in_terminal_order_refs"], details
    prefix = raw[:marker_at].rstrip()
    if prefix.endswith(","):
        prefix = prefix[:-1]
    try:
        salvaged = json.loads(prefix + "\n}\n")
    except json.JSONDecodeError as exc:
        return None, ["state_prefix_not_salvageable"], {"salvage_error": str(exc)[:240]}
    if not isinstance(salvaged, dict) or not isinstance(
        salvaged.get("best_quote_volume_ledger"), dict
    ):
        reasons.append("salvaged_state_missing_active_ledger")

    best_quote = plan.get("best_quote_maker_volume")
    reduce_freeze = (
        best_quote.get("reduce_freeze") if isinstance(best_quote, dict) else None
    )
    frozen_ledger = reduce_freeze.get("ledger") if isinstance(reduce_freeze, dict) else None
    if not isinstance(frozen_ledger, dict):
        reasons.append("pre_corruption_snapshot_missing_frozen_ledger")

    generated_at = _parse_time(plan.get("generated_at"))
    snapshot_delta_seconds = (
        abs(state_mtime - generated_at.timestamp()) if generated_at is not None else float("inf")
    )
    details["pre_corruption_snapshot_delta_seconds"] = round(snapshot_delta_seconds, 3)
    if snapshot_delta_seconds > 30.0:
        reasons.append("pre_corruption_snapshot_not_adjacent")

    try:
        exchange = exchange_snapshot_fetcher(symbol)
    except Exception as exc:
        exchange = {}
        reasons.append("exchange_snapshot_error")
        details["exchange_snapshot_error"] = str(exc)[:240]
    open_order_count = _safe_int(exchange.get("open_order_count"), -1)
    details["exchange_open_order_count"] = open_order_count
    if open_order_count != 0:
        reasons.append("exchange_open_orders_present")

    planned_long = _safe_float(
        reduce_freeze.get("actual_long_notional") if isinstance(reduce_freeze, dict) else None
    )
    planned_short = _safe_float(
        reduce_freeze.get("actual_short_notional") if isinstance(reduce_freeze, dict) else None
    )
    exchange_long = _safe_float(exchange.get("long_notional"))
    exchange_short = _safe_float(exchange.get("short_notional"))
    position_drift = abs(exchange_long - planned_long) + abs(exchange_short - planned_short)
    drift_threshold = max(
        _safe_float(plan.get("hedge_bq_position_drift_tolerance_notional")),
        100.0,
    )
    details.update(
        {
            "exchange_long_notional": exchange_long,
            "exchange_short_notional": exchange_short,
            "snapshot_long_notional": planned_long,
            "snapshot_short_notional": planned_short,
            "position_drift_notional": position_drift,
            "position_drift_threshold_notional": drift_threshold,
        }
    )
    if position_drift > drift_threshold:
        reasons.append("exchange_position_drift_too_large")

    if reasons:
        return None, reasons, details
    salvaged["best_quote_volume_order_refs"] = {}
    salvaged["best_quote_frozen_inventory"] = frozen_ledger
    salvaged["updated_by"] = "bq_volume_recovery_guard_order_refs_salvage"
    return salvaged, [], details


def _salvage_truncated_applied_trade_keys_state(
    *,
    state_path: Path,
    plan: dict[str, Any],
    state_mtime: float,
    exchange_snapshot_fetcher: CorruptStateExchangeFetcher,
    symbol: str,
) -> tuple[dict[str, Any] | None, list[str], dict[str, Any]]:
    details: dict[str, Any] = {}
    try:
        raw = state_path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as exc:
        return None, ["corrupt_state_unreadable"], {"salvage_error": str(exc)[:240]}
    marker = '\n    "applied_trade_fill_keys": ['
    marker_at = raw.rfind(marker)
    if marker_at < 0:
        return None, ["corruption_not_in_terminal_applied_trade_keys"], details
    tail_match = re.search(
        r'\n      "(\d+:(?:BUY|SELL):(?:LONG|SHORT):\d+:[0-9.]+:[0-9.]+)$',
        raw,
    )
    if tail_match is None:
        return None, ["terminal_applied_trade_key_incomplete"], details
    repaired_raw = raw + '"\n    ]\n  }\n}\n'
    try:
        salvaged = json.loads(repaired_raw)
    except json.JSONDecodeError as exc:
        return None, ["applied_trade_keys_state_not_salvageable"], {
            "salvage_error": str(exc)[:240]
        }
    ledger = salvaged.get("best_quote_volume_ledger")
    frozen = salvaged.get("best_quote_frozen_inventory")
    fill_keys = ledger.get("applied_trade_fill_keys") if isinstance(ledger, dict) else None
    reasons: list[str] = []
    if not isinstance(ledger, dict):
        reasons.append("salvaged_state_missing_active_ledger")
    if not isinstance(frozen, dict):
        reasons.append("salvaged_state_missing_frozen_ledger")
    if not isinstance(fill_keys, list) or not fill_keys:
        reasons.append("salvaged_state_missing_applied_trade_keys")
    elif fill_keys[-1] != tail_match.group(1):
        reasons.append("salvaged_terminal_trade_key_mismatch")

    best_quote = plan.get("best_quote_maker_volume")
    reduce_freeze = (
        best_quote.get("reduce_freeze") if isinstance(best_quote, dict) else None
    )
    generated_at = _parse_time(plan.get("generated_at"))
    snapshot_delta_seconds = (
        abs(state_mtime - generated_at.timestamp())
        if generated_at is not None
        else float("inf")
    )
    details["pre_corruption_snapshot_delta_seconds"] = round(snapshot_delta_seconds, 3)
    if snapshot_delta_seconds > 30.0:
        reasons.append("pre_corruption_snapshot_not_adjacent")

    try:
        exchange = exchange_snapshot_fetcher(symbol)
    except Exception as exc:
        exchange = {}
        reasons.append("exchange_snapshot_error")
        details["exchange_snapshot_error"] = str(exc)[:240]
    open_order_count = _safe_int(exchange.get("open_order_count"), -1)
    details["exchange_open_order_count"] = open_order_count
    if open_order_count != 0:
        reasons.append("exchange_open_orders_present")

    planned_long = _safe_float(
        reduce_freeze.get("actual_long_notional") if isinstance(reduce_freeze, dict) else None
    )
    planned_short = _safe_float(
        reduce_freeze.get("actual_short_notional") if isinstance(reduce_freeze, dict) else None
    )
    exchange_long = _safe_float(exchange.get("long_notional"))
    exchange_short = _safe_float(exchange.get("short_notional"))
    position_drift = abs(exchange_long - planned_long) + abs(exchange_short - planned_short)
    drift_threshold = max(
        _safe_float(plan.get("hedge_bq_position_drift_tolerance_notional")),
        100.0,
    )
    details.update(
        {
            "terminal_trade_key": tail_match.group(1),
            "exchange_long_notional": exchange_long,
            "exchange_short_notional": exchange_short,
            "snapshot_long_notional": planned_long,
            "snapshot_short_notional": planned_short,
            "position_drift_notional": position_drift,
            "position_drift_threshold_notional": drift_threshold,
        }
    )
    if position_drift > drift_threshold:
        reasons.append("exchange_position_drift_too_large")
    if reasons:
        return None, reasons, details
    salvaged["updated_by"] = "bq_volume_recovery_guard_applied_trade_keys_salvage"
    return salvaged, [], details


def recover_corrupt_loop_state(
    *,
    symbol: str,
    output_dir: Path,
    now: datetime,
    max_backup_age_seconds: float,
    min_corrupt_age_seconds: float,
    max_snapshot_age_seconds: float,
    dry_run: bool,
    exchange_snapshot_fetcher: CorruptStateExchangeFetcher | None = None,
) -> dict[str, Any] | None:
    normalized_symbol = symbol.upper().strip()
    slug = normalized_symbol.lower()
    state_path = output_dir / f"{slug}_loop_state.json"
    if not state_path.exists():
        return None
    try:
        payload = json.loads(state_path.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            return None
        decode_error = "loop state is not a JSON object"
    except (json.JSONDecodeError, OSError, UnicodeDecodeError) as exc:
        decode_error = str(exc)

    try:
        state_mtime = state_path.stat().st_mtime
        corrupt_age_seconds = max(now.timestamp() - state_mtime, 0.0)
    except OSError:
        state_mtime = 0.0
        corrupt_age_seconds = 0.0
    plan = _read_json(_plan_path(output_dir, normalized_symbol))
    gate = _inactive_restart_gate(
        control=_read_json(_control_path(output_dir, normalized_symbol)),
        plan=plan,
        submit=_read_json(_submit_path(output_dir, normalized_symbol)),
        now=now,
        max_snapshot_age_seconds=max_snapshot_age_seconds,
    )
    blocking_reasons = [
        reason
        for reason in gate["reasons"]
        if reason not in {"latest_plan_stale", "latest_submit_stale"}
    ]
    if corrupt_age_seconds < max(float(min_corrupt_age_seconds), 0.0):
        blocking_reasons.append("corrupt_state_not_persistent")

    backup_path: Path | None = None
    backup_payload: dict[str, Any] | None = None
    backup_candidates = list(
        output_dir.glob(f"{slug}_loop_state.json.bak_autorealign_*")
    ) + list(output_dir.glob(f"{slug}_loop_state.json.bak_bq_recovery_restart"))
    for candidate in sorted(
        backup_candidates,
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    ):
        try:
            backup_age = max(now.timestamp() - candidate.stat().st_mtime, 0.0)
        except OSError:
            continue
        if backup_age > max(float(max_backup_age_seconds), 0.0):
            continue
        candidate_payload = _read_json(candidate)
        if candidate_payload:
            backup_path = candidate
            backup_payload = candidate_payload
            break
    order_refs_payload, order_refs_reasons, order_refs_details = _salvage_truncated_order_refs_state(
        state_path=state_path,
        plan=plan,
        state_mtime=state_mtime,
        exchange_snapshot_fetcher=(
            exchange_snapshot_fetcher or _fetch_corrupt_state_exchange_snapshot
        ),
        symbol=normalized_symbol,
    )
    applied_keys_payload, applied_keys_reasons, applied_keys_details = (
        _salvage_truncated_applied_trade_keys_state(
            state_path=state_path,
            plan=plan,
            state_mtime=state_mtime,
            exchange_snapshot_fetcher=(
                exchange_snapshot_fetcher or _fetch_corrupt_state_exchange_snapshot
            ),
            symbol=normalized_symbol,
        )
    )
    salvage_payload = order_refs_payload or applied_keys_payload
    salvage_kind = (
        "order_refs" if order_refs_payload is not None else "applied_trade_fill_keys"
    )
    if backup_payload is None and salvage_payload is None:
        blocking_reasons.append("no_recent_valid_autorealign_backup")
        blocking_reasons.extend(order_refs_reasons)
        blocking_reasons.extend(applied_keys_reasons)

    result: dict[str, Any] = {
        "symbol": normalized_symbol,
        "changed_keys": [],
        "backup_path": str(backup_path) if backup_path else None,
        "dry_run": dry_run,
        "restart_failed": None,
        "state_corruption": {
            "path": str(state_path),
            "error": decode_error[:240],
            "age_seconds": round(corrupt_age_seconds, 3),
            "blocking_reasons": blocking_reasons,
            "snapshot_gate": gate,
            "order_refs_salvage": order_refs_details,
            "applied_trade_fill_keys_salvage": applied_keys_details,
        },
    }
    if blocking_reasons:
        result["action"] = "skip_corrupt_state_recovery_safety_gate"
        return result
    if dry_run:
        result["action"] = (
            f"dry_run_salvage_corrupt_state_{salvage_kind}"
            if salvage_payload is not None
            else "dry_run_restore_corrupt_state_from_autorealign_backup"
        )
        return result

    stamp = now.strftime("%Y%m%dT%H%M%SZ")
    corrupt_archive = state_path.with_name(state_path.name + f".corrupt_bq_recovery_{stamp}")
    shutil.copy2(state_path, corrupt_archive)
    _write_json(state_path, salvage_payload or backup_payload or {})
    plan_path = _plan_path(output_dir, normalized_symbol)
    archived_plan: str | None = None
    if plan_path.exists():
        stale_plan_path = plan_path.with_name(plan_path.name + f".stale_state_recovery_{stamp}")
        plan_path.replace(stale_plan_path)
        archived_plan = str(stale_plan_path)
    result["action"] = (
        f"salvage_corrupt_state_{salvage_kind}"
        if salvage_payload is not None
        else "restore_corrupt_state_from_autorealign_backup"
    )
    result["corrupt_archive_path"] = str(corrupt_archive)
    result["archived_plan_path"] = archived_plan
    result["safe_restart_after_salvage"] = salvage_payload is not None
    return result


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _is_high_recovery_wear(volume_summary: dict[str, Any]) -> bool:
    trailing_5m_wear = _safe_float(
        volume_summary.get("trailing_5m_realized_wear_per_10k")
    )
    trailing_15m_wear = _safe_float(
        volume_summary.get("trailing_15m_realized_wear_per_10k")
    )
    return (
        _safe_float(volume_summary.get("trailing_15m_gross_notional")) >= 100.0
        and (trailing_15m_wear > 3.0 or trailing_5m_wear > 80.0)
    )


def _parse_time(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _exchange_trade_fetch_cooldown_seconds(exc: Exception) -> float | None:
    message = str(exc)
    if "Binance API local cooldown active for futures" not in message:
        return None
    match = re.search(r"retry after\s+([0-9]+(?:\.[0-9]+)?)s", message)
    if match is None:
        return 60.0
    try:
        return max(float(match.group(1)), 0.0)
    except ValueError:
        return 60.0


def _trade_time(row: dict[str, Any]) -> datetime | None:
    time_ms = _safe_float(row.get("time"))
    if time_ms > 0:
        return datetime.fromtimestamp(time_ms / 1000.0, tz=timezone.utc)
    return _parse_time(row.get("audit_synced_at") or row.get("ts") or row.get("transactTime"))


def _iter_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return rows
    for line in lines:
        text = line.strip()
        if not text:
            continue
        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            rows.append(payload)
    return rows


def fetch_recent_user_trades(
    *,
    fetch_page: UserTradesPageFetcher,
    now: datetime,
    window_seconds: float,
    max_pages: int = 90,
) -> list[dict[str, Any]]:
    start_time_ms = int((now.timestamp() - max(float(window_seconds), 1.0)) * 1000)
    end_time_ms = int(now.timestamp() * 1000)
    cursor = start_time_ms
    rows: list[dict[str, Any]] = []
    seen_trade_ids: set[str] = set()
    for _ in range(max(int(max_pages), 1)):
        batch = fetch_page(start_time_ms=cursor, limit=1000)
        if not batch:
            break
        new_rows = 0
        for row in batch:
            trade_time = _safe_int(row.get("time"))
            if trade_time < start_time_ms or trade_time > end_time_ms:
                continue
            trade_id = row.get("id")
            if trade_id is not None:
                normalized_id = str(trade_id)
                if normalized_id in seen_trade_ids:
                    continue
                seen_trade_ids.add(normalized_id)
            rows.append(row)
            new_rows += 1
        if len(batch) < 1000 or new_rows == 0:
            break
        next_cursor = max(_safe_int(item.get("time")) for item in batch)
        if next_cursor < cursor:
            break
        cursor = next_cursor
    return rows


def _fetch_exchange_user_trades(*, symbol: str, now: datetime, window_seconds: float) -> list[dict[str, Any]]:
    from .data import fetch_futures_user_trades

    api_key = os.environ["BINANCE_API_KEY"]
    api_secret = os.environ["BINANCE_API_SECRET"]
    return fetch_recent_user_trades(
        fetch_page=lambda **kwargs: fetch_futures_user_trades(
            symbol=symbol,
            api_key=api_key,
            api_secret=api_secret,
            **kwargs,
        ),
        now=now,
        window_seconds=window_seconds,
    )


def summarize_recent_volume(*, rows: list[dict[str, Any]], now: datetime, window_seconds: float) -> dict[str, Any]:
    window_start = now.timestamp() - max(float(window_seconds), 1.0)
    gross_notional = 0.0
    trade_count = 0
    latest_trade_at: datetime | None = None
    seen_trade_ids: set[str] = set()
    for row in rows:
        trade_id = row.get("id")
        if trade_id is not None:
            normalized_id = str(trade_id)
            if normalized_id in seen_trade_ids:
                continue
            seen_trade_ids.add(normalized_id)
        trade_at = _trade_time(row)
        if trade_at is None or trade_at.timestamp() < window_start or trade_at > now:
            continue
        notional = _safe_float(row.get("quoteQty") or row.get("quote_qty") or row.get("notional"))
        if notional <= 0:
            notional = _safe_float(row.get("price")) * _safe_float(row.get("qty") or row.get("quantity"))
        if notional <= 0:
            continue
        gross_notional += abs(notional)
        trade_count += 1
        latest_trade_at = trade_at if latest_trade_at is None or trade_at > latest_trade_at else latest_trade_at
    return {
        "gross_notional": gross_notional,
        "trade_count": trade_count,
        "window_seconds": max(float(window_seconds), 1.0),
        "latest_trade_at": latest_trade_at.isoformat() if latest_trade_at else None,
    }


def summarize_recent_realized_pnl(
    *, rows: list[dict[str, Any]], now: datetime, window_seconds: float
) -> float:
    window_start = now.timestamp() - max(float(window_seconds), 1.0)
    realized_pnl = 0.0
    seen_trade_ids: set[str] = set()
    for row in rows:
        trade_id = row.get("id")
        if trade_id is not None:
            normalized_id = str(trade_id)
            if normalized_id in seen_trade_ids:
                continue
            seen_trade_ids.add(normalized_id)
        trade_at = _trade_time(row)
        if trade_at is None or trade_at.timestamp() < window_start or trade_at > now:
            continue
        realized_pnl += _safe_float(row.get("realizedPnl") or row.get("realized_pnl"))
    return realized_pnl


def apply_daily_target_pace_floor(
    *,
    volume_summary: dict[str, Any],
    rows: list[dict[str, Any]],
    now: datetime,
    window_seconds: float,
    min_volume_notional: float,
    daily_target_notional: float | None,
    target_pace_fraction: float,
    target_pace_max_multiplier: float,
    target_completion_buffer_seconds: float = 0.0,
) -> float:
    static_floor = max(float(min_volume_notional), 0.0)
    target = max(float(daily_target_notional or 0.0), 0.0)
    if target <= 0:
        volume_summary["effective_min_volume_notional"] = static_floor
        return static_floor

    day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    day_end = day_start + timedelta(days=1)
    completion_buffer = max(float(target_completion_buffer_seconds), 0.0)
    target_deadline = day_end - timedelta(seconds=completion_buffer)
    effective_target_deadline = target_deadline if now < target_deadline else day_end
    elapsed_seconds = max((now - day_start).total_seconds(), 1.0)
    remaining_seconds = max((effective_target_deadline - now).total_seconds(), 1.0)
    day_summary = summarize_recent_volume(rows=rows, now=now, window_seconds=elapsed_seconds)
    day_gross = _safe_float(day_summary.get("gross_notional"))
    remaining_target = max(target - day_gross, 0.0)
    required_hourly = remaining_target * 3600.0 / remaining_seconds
    target_floor = required_hourly * max(float(window_seconds), 1.0) / 3600.0
    target_floor *= max(float(target_pace_fraction), 0.0)
    max_multiplier = max(float(target_pace_max_multiplier), 1.0)
    if static_floor > 0:
        target_floor = min(target_floor, static_floor * max_multiplier)
    effective_floor = max(static_floor, target_floor)
    volume_summary.update(
        {
            "daily_target_notional": target,
            "daily_gross_notional": day_gross,
            "remaining_target_notional": remaining_target,
            "remaining_target_seconds": remaining_seconds,
            "target_completion_buffer_seconds": completion_buffer,
            "target_deadline": target_deadline.isoformat(),
            "effective_target_deadline": effective_target_deadline.isoformat(),
            "completion_buffer_expired": now >= target_deadline,
            "required_hourly_notional": required_hourly,
            "target_pace_floor_notional": target_floor,
            "effective_min_volume_notional": effective_floor,
        }
    )
    return effective_floor


def apply_target_pace_cycle_budget_floor(
    *,
    volume_summary: dict[str, Any],
    rows: list[dict[str, Any]],
    now: datetime,
    control: dict[str, Any],
    assessment: dict[str, Any],
    static_floor_notional: float,
    target_pace_fraction: float,
    cycle_budget_increment: float,
) -> float:
    static_floor = max(float(static_floor_notional), 0.0)
    current_budget = max(_safe_float(control.get("best_quote_maker_volume_cycle_budget_notional")), 0.0)
    required_hourly = max(_safe_float(volume_summary.get("required_hourly_notional")), 0.0)
    day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    trailing_seconds = min(max((now - day_start).total_seconds(), 1.0), 3600.0)
    trailing_summary = summarize_recent_volume(rows=rows, now=now, window_seconds=trailing_seconds)
    trailing_gross = max(_safe_float(trailing_summary.get("gross_notional")), 0.0)
    trailing_hourly = trailing_gross * 3600.0 / trailing_seconds
    trailing_realized_pnl = summarize_recent_realized_pnl(
        rows=rows,
        now=now,
        window_seconds=trailing_seconds,
    )
    trailing_realized_wear_per_10k = (
        -trailing_realized_pnl / trailing_gross * 10_000.0 if trailing_gross > 0 else 0.0
    )
    short_wear_metrics: dict[str, float] = {}
    for label, seconds in (("5m", 300.0), ("15m", 900.0)):
        short_summary = summarize_recent_volume(rows=rows, now=now, window_seconds=seconds)
        short_gross = max(_safe_float(short_summary.get("gross_notional")), 0.0)
        short_realized = summarize_recent_realized_pnl(
            rows=rows,
            now=now,
            window_seconds=seconds,
        )
        short_wear_metrics[f"trailing_{label}_gross_notional"] = short_gross
        short_wear_metrics[f"trailing_{label}_realized_pnl"] = short_realized
        short_wear_metrics[f"trailing_{label}_realized_wear_per_10k"] = (
            -short_realized / short_gross * 10_000.0 if short_gross > 0 else 0.0
        )
    desired_hourly = required_hourly * max(float(target_pace_fraction), 0.0)
    pace_ratio = desired_hourly / trailing_hourly if trailing_hourly > 0 else 4.0

    volume_summary.update(
        {
            "trailing_60m_gross_notional": trailing_gross,
            "trailing_60m_hourly_notional": trailing_hourly,
            "trailing_60m_realized_pnl": trailing_realized_pnl,
            "trailing_60m_realized_wear_per_10k": trailing_realized_wear_per_10k,
            **short_wear_metrics,
            "target_cycle_budget_pace_ratio": pace_ratio,
            "static_cycle_budget_floor_notional": static_floor,
        }
    )
    if static_floor <= 0 or required_hourly <= 0 or trailing_hourly >= desired_hourly:
        volume_summary["target_cycle_budget_floor_notional"] = static_floor
        return static_floor

    per_order = max(
        _safe_float(control.get("per_order_notional")),
        _safe_float(control.get("maker_order_notional")),
    )
    buy_levels = max(_safe_int(control.get("buy_levels")), 0)
    sell_levels = max(_safe_int(control.get("sell_levels")), 0)
    ladder_levels = min(value for value in (buy_levels, sell_levels) if value > 0) if buy_levels and sell_levels else 0
    if current_budget <= 0 or per_order <= 0 or ladder_levels <= 0:
        volume_summary["target_cycle_budget_floor_notional"] = static_floor
        return static_floor

    increment = max(float(cycle_budget_increment), 1.0)
    raw_target = static_floor * max(pace_ratio, 1.0)
    rounded_target = ceil(raw_target / increment) * increment
    ladder_capacity = per_order * ladder_levels * 2.0
    # Target pacing is allowed to advance only by the configured recovery
    # increment.  A percentage-based jump can turn a low-volume recovery into
    # an abrupt exposure and wear increase after a restart.
    one_step_capacity = current_budget + increment

    frozen_total = max(_safe_float(assessment.get("frozen_total_notional")), 0.0)
    use_actual = (
        frozen_total <= 0
        and assessment.get("actual_long_notional") is not None
        and assessment.get("actual_short_notional") is not None
    )
    current_long = _safe_float(
        assessment.get("actual_long_notional") if use_actual else assessment.get("current_long_notional")
    )
    current_short = _safe_float(
        assessment.get("actual_short_notional") if use_actual else assessment.get("current_short_notional")
    )
    long_headroom = max(_safe_float(assessment.get("max_long_notional")) - current_long, 0.0)
    short_headroom = max(_safe_float(assessment.get("max_short_notional")) - current_short, 0.0)
    headroom_capacity = min(long_headroom, short_headroom) * 2.0

    upper_bound = min(ladder_capacity, one_step_capacity)
    if headroom_capacity > 0:
        upper_bound = min(upper_bound, headroom_capacity)
    upper_bound = max(upper_bound, current_budget, static_floor)
    target_floor = max(static_floor, min(rounded_target, upper_bound))
    volume_summary.update(
        {
            "target_cycle_budget_floor_notional": target_floor,
            "target_cycle_budget_ladder_capacity": ladder_capacity,
            "target_cycle_budget_headroom_capacity": headroom_capacity,
            "target_cycle_budget_one_step_capacity": one_step_capacity,
        }
    )
    return target_floor


def _control_path(output_dir: Path, symbol: str) -> Path:
    return output_dir / f"{symbol.lower()}_loop_runner_control.json"


def _plan_path(output_dir: Path, symbol: str) -> Path:
    return output_dir / f"{symbol.lower()}_loop_latest_plan.json"


def _submit_path(output_dir: Path, symbol: str) -> Path:
    return output_dir / f"{symbol.lower()}_loop_latest_submit.json"


def _trade_path(output_dir: Path, symbol: str) -> Path:
    return output_dir / f"{symbol.lower()}_loop_trade_audit.jsonl"


def _events_path(output_dir: Path, symbol: str) -> Path:
    return output_dir / f"{symbol.lower()}_loop_events.jsonl"


def _latest_runtime_stop_event(output_dir: Path, symbol: str) -> dict[str, Any]:
    for event in reversed(_iter_jsonl(_events_path(output_dir, symbol))):
        reason = str(event.get("stop_reason") or "").strip()
        if reason and str(event.get("runtime_status") or "").strip().lower() == "stopped":
            return event
    return {}


def _latest_runner_error_event(output_dir: Path, symbol: str) -> dict[str, Any]:
    for event in reversed(_iter_jsonl(_events_path(output_dir, symbol))):
        if str(event.get("error_type") or "").strip():
            return event
        # A successful cycle after an error proves the runner has recovered;
        # do not restart it just because an older error remains in the log.
        if "cycle" in event and not str(event.get("error_message") or "").strip():
            return {}
    return {}


def _symbol_state(state: dict[str, Any], symbol: str) -> dict[str, Any]:
    symbols = state.setdefault("symbols", {})
    if not isinstance(symbols, dict):
        symbols = {}
        state["symbols"] = symbols
    item = symbols.setdefault(symbol, {})
    if not isinstance(item, dict):
        item = {}
        symbols[symbol] = item
    return item


def _remember_recovery_controls(item: dict[str, Any], control: dict[str, Any], keys: tuple[str, ...]) -> None:
    original = item.get("guard_original_controls")
    originals = dict(original) if isinstance(original, dict) else {}
    for key in keys:
        if key in _RECOVERY_CONTROL_KEYS and key not in originals:
            originals[key] = control.get(key)
    item["guard_original_controls"] = originals


def _remember_recovery_updates(item: dict[str, Any], updates: dict[str, Any]) -> None:
    existing = item.get("guard_recovery_controls")
    expected = dict(existing) if isinstance(existing, dict) else {}
    for key, value in updates.items():
        if key in _RECOVERY_CONTROL_KEYS:
            expected[key] = value
    item["guard_recovery_controls"] = expected


def _restore_recovery_controls(
    item: dict[str, Any],
    control: dict[str, Any] | None = None,
    cycle_budget_floor_notional: float = 0.0,
) -> dict[str, Any]:
    original = item.get("guard_original_controls")
    updates = {"best_quote_maker_volume_allow_loss_reduce_only": False}
    if isinstance(original, dict):
        for key, value in original.items():
            if key in _RECOVERY_CONTROL_KEYS:
                updates[key] = value
    budget_key = "best_quote_maker_volume_cycle_budget_notional"
    budget_floor = max(float(cycle_budget_floor_notional), 0.0)
    current_budget = updates.get(budget_key)
    if current_budget is None and isinstance(control, dict):
        current_budget = control.get(budget_key)
    if budget_floor > 0 and current_budget is not None and _safe_float(current_budget) < budget_floor:
        updates[budget_key] = budget_floor
    updates["best_quote_maker_volume_net_loss_reduce_enabled"] = False
    return updates


def _loss_reduce_recovery_updates(
    *,
    control: dict[str, Any],
    assessment: dict[str, Any],
    static_cycle_budget_floor_notional: float = 0.0,
    quote_offset_extra_ticks: int = 0,
    pause_baseline_long_notional: float = 0.0,
    pause_baseline_short_notional: float = 0.0,
    parameters: RecoveryParameters | None = None,
) -> dict[str, Any]:
    updates: dict[str, Any] = {
        "best_quote_maker_volume_net_loss_reduce_enabled": False,
        "best_quote_maker_volume_allow_loss_reduce_only": True,
    }
    if not bool(control.get("best_quote_maker_volume_allow_loss_reduce_only")):
        extra_ticks = max(int(quote_offset_extra_ticks), 0)
        if (
            extra_ticks > 0
            and not (
                bool(assessment.get("dynamic_quote_offset_applied"))
                and _safe_int(assessment.get("planned_entry_order_count")) > 0
            )
        ):
            updates["best_quote_maker_volume_quote_offset_ticks"] = (
                max(_safe_int(control.get("best_quote_maker_volume_quote_offset_ticks")), 0) + extra_ticks
            )
    resolved = parameters or _resolve_recovery_parameters(
        control=control,
        static_cycle_budget_floor_notional=static_cycle_budget_floor_notional,
    )
    per_order = resolved.per_order_notional
    loss_reduce_budget_cap = resolved.loss_reduce_cycle_budget_cap_notional
    loss_reduce_budget_floor = (
        min(
            max(
                resolved.configured_min_cycle_budget_notional,
                per_order,
            ),
            loss_reduce_budget_cap,
        )
        if loss_reduce_budget_cap > 0
        else 0.0
    )
    current_budget = _safe_float(control.get("best_quote_maker_volume_cycle_budget_notional"))
    if loss_reduce_budget_floor > 0 and current_budget < loss_reduce_budget_floor:
        updates["best_quote_maker_volume_cycle_budget_notional"] = loss_reduce_budget_floor
    elif loss_reduce_budget_cap > 0 and current_budget > loss_reduce_budget_cap:
        updates["best_quote_maker_volume_cycle_budget_notional"] = loss_reduce_budget_cap
    effective_budget = current_budget
    if loss_reduce_budget_cap > 0:
        effective_budget = min(effective_budget, loss_reduce_budget_cap)
    pair_order_notional = max(
        per_order,
        _safe_float(control.get("best_quote_maker_volume_active_pair_reduce_order_notional")),
    )
    pair_max_notional = max(
        pair_order_notional,
        effective_budget * 0.5,
        _safe_float(
            control.get("best_quote_maker_volume_active_pair_reduce_max_notional_per_side")
        ),
    )
    if (
        str(assessment.get("symbol") or "").upper() == "ARXUSDT"
        and bool(assessment.get("target_pace_behind"))
    ):
        # While ARX trails its daily target, a 20–32U near-market pair reducer
        # cannot drain a 2.6k soft-cap inventory quickly enough to reopen flow.
        # Use the already bounded recovery-side cap as the single maker order.
        pair_order_notional = pair_max_notional
    if pair_order_notional > 0:
        updates.update(
            {
                "best_quote_maker_volume_active_pair_reduce_enabled": True,
                "best_quote_maker_volume_active_pair_reduce_order_notional": pair_order_notional,
                "best_quote_maker_volume_active_pair_reduce_max_notional_per_side": pair_max_notional,
            }
        )
    soft_ratio_candidates: list[float] = []
    for baseline, max_key in (
        (pause_baseline_long_notional, "max_long_notional"),
        (pause_baseline_short_notional, "max_short_notional"),
    ):
        max_notional = max(_safe_float(assessment.get(max_key)), 0.0)
        if baseline > 0 and max_notional > 0:
            soft_ratio_candidates.append(min(float(baseline) / max_notional, 1.0))
    if soft_ratio_candidates:
        current_soft_ratio = max(
            _safe_float(control.get("best_quote_maker_volume_inventory_soft_ratio")),
            0.0,
        )
        target_soft_ratio = min(soft_ratio_candidates)
        if current_soft_ratio <= 0 or target_soft_ratio < current_soft_ratio:
            updates["best_quote_maker_volume_inventory_soft_ratio"] = target_soft_ratio
    buffer_notional = resolved.loss_reduce_cycle_budget_cap_notional
    if buffer_notional <= 0:
        return updates
    current_long = max(_safe_float(assessment.get("current_long_notional")), 0.0)
    current_short = max(_safe_float(assessment.get("current_short_notional")), 0.0)
    if current_long > current_short + buffer_notional:
        reducible_sides = {"LONG"}
    elif current_short > current_long + buffer_notional:
        reducible_sides = {"SHORT"}
    else:
        reducible_sides = {"LONG", "SHORT"}
    for side, current_key, pause_key, baseline in (
        ("LONG", "current_long_notional", "pause_buy_position_notional", pause_baseline_long_notional),
        ("SHORT", "current_short_notional", "pause_short_position_notional", pause_baseline_short_notional),
    ):
        if pause_key not in control:
            continue
        if side not in reducible_sides:
            if baseline > 0 and _safe_float(control.get(pause_key)) < baseline:
                updates[pause_key] = baseline
            continue
        current_notional = max(_safe_float(assessment.get(current_key)), 0.0)
        recovery_pause = max(current_notional - buffer_notional, 1.0)
        if current_notional > 0 and _safe_float(control.get(pause_key)) > recovery_pause:
            updates[pause_key] = recovery_pause
    return updates


def _loss_reduce_flow_control_updates(
    control: dict[str, Any], desired: dict[str, Any]
) -> dict[str, Any]:
    keys = (
        "best_quote_maker_volume_active_pair_reduce_enabled",
        "best_quote_maker_volume_active_pair_reduce_order_notional",
        "best_quote_maker_volume_active_pair_reduce_max_notional_per_side",
        "best_quote_maker_volume_inventory_soft_ratio",
    )
    return {key: desired[key] for key in keys if key in desired and control.get(key) != desired[key]}


def _normal_entry_wear_backoff_confirmed(
    volume_summary: dict[str, Any], *, target_pace_behind: bool
) -> bool:
    wear_5m = _safe_float(volume_summary.get("trailing_5m_realized_wear_per_10k"))
    wear_15m = _safe_float(volume_summary.get("trailing_15m_realized_wear_per_10k"))
    volume_5m = _safe_float(volume_summary.get("trailing_5m_gross_notional"))
    volume_15m = _safe_float(volume_summary.get("trailing_15m_gross_notional"))
    if volume_15m < 100.0 or max(wear_5m, wear_15m) <= 3.0:
        return False
    if not target_pace_behind:
        return True
    # While badly behind target, stale loss-reduce wear in the 15m window must
    # not immediately throttle newly restored normal entry flow.
    return volume_5m >= 100.0 and wear_5m > 3.0


def _recovery_inventory_buffer_ok(
    assessment: dict[str, Any],
    *,
    recover_cap_ratio: float,
    original_controls: dict[str, Any] | None = None,
    entry_reserve_notional: float = 0.0,
) -> bool:
    originals = original_controls if isinstance(original_controls, dict) else {}
    soft_ratio = max(_safe_float(assessment.get("inventory_soft_ratio")), 0.0)
    recovery_ratio = min(max(float(recover_cap_ratio), 0.0), 1.0)

    def threshold(max_key: str, soft_key: str, pause_key: str) -> float:
        cap_limit = _safe_float(assessment.get(max_key)) * recovery_ratio
        max_notional = max(_safe_float(assessment.get(max_key)), 0.0)
        ratio_limit = max_notional * soft_ratio if max_notional > 0 and soft_ratio > 0 else 0.0
        original_pause = max(_safe_float(originals.get(pause_key)), 0.0)
        baseline_candidates = [value for value in (original_pause, ratio_limit) if value > 0]
        soft_limit = (
            min(baseline_candidates)
            if baseline_candidates
            else _safe_float(assessment.get(soft_key))
        )
        # Apply the recovery ratio to the soft boundary too. Otherwise a soft limit
        # below the hard-cap buffer wins the min() and recovery closes immediately
        # below soft, causing repeated loss-reduce cycles around the same boundary.
        buffered_soft_limit = soft_limit * recovery_ratio
        candidates = [value for value in (cap_limit, buffered_soft_limit) if value > 0]
        boundary = min(candidates) if candidates else 0.0
        # Keep enough room for one normal entry cycle after loss-reduce closes.
        # Without this reserve, larger cycle budgets immediately push inventory
        # back over soft and cause allow-loss on/off oscillation.
        return max(boundary - max(float(entry_reserve_notional), 0.0), 0.0)

    long_limit = threshold("max_long_notional", "long_soft_limit_notional", "pause_buy_position_notional")
    short_limit = threshold("max_short_notional", "short_soft_limit_notional", "pause_short_position_notional")
    frozen_total = max(_safe_float(assessment.get("frozen_total_notional")), 0.0)
    actual_long = assessment.get("actual_long_notional")
    actual_short = assessment.get("actual_short_notional")
    use_actual = frozen_total <= 0 and actual_long is not None and actual_short is not None
    current_long = _safe_float(actual_long) if use_actual else _safe_float(assessment.get("current_long_notional"))
    current_short = _safe_float(actual_short) if use_actual else _safe_float(assessment.get("current_short_notional"))
    return (
        long_limit > 0
        and short_limit > 0
        and current_long < long_limit
        and current_short < short_limit
    )


def _actual_inventory_below_soft_limits(
    assessment: dict[str, Any],
    *,
    pause_baseline_long_notional: float,
    pause_baseline_short_notional: float,
) -> bool:
    has_frozen = _safe_float(assessment.get("frozen_total_notional")) > 0
    if has_frozen:
        inventory_long = assessment.get("current_long_notional")
        inventory_short = assessment.get("current_short_notional")
    else:
        inventory_long = assessment.get("actual_long_notional")
        inventory_short = assessment.get("actual_short_notional")
    if inventory_long is None or inventory_short is None:
        return False
    long_limit = max(
        float(pause_baseline_long_notional),
        _safe_float(assessment.get("long_soft_limit_notional")),
    )
    short_limit = max(
        float(pause_baseline_short_notional),
        _safe_float(assessment.get("short_soft_limit_notional")),
    )
    return (
        long_limit > 0
        and short_limit > 0
        and _safe_float(inventory_long) < long_limit
        and _safe_float(inventory_short) < short_limit
    )


def _plan_time_is_fresh(payload: dict[str, Any], *, now: datetime, max_age_seconds: float) -> bool:
    generated_at = _parse_time(payload.get("generated_at"))
    if generated_at is None:
        return False
    return (now - generated_at).total_seconds() <= max(float(max_age_seconds), 1.0)


def _live_book(plan: dict[str, Any], submit: dict[str, Any]) -> tuple[float, float]:
    submit_book = submit.get("live_book") if isinstance(submit.get("live_book"), dict) else {}
    bid = _safe_float(plan.get("best_bid") or plan.get("bid_price") or submit_book.get("bid_price"))
    ask = _safe_float(plan.get("best_ask") or plan.get("ask_price") or submit_book.get("ask_price"))
    return bid, ask


def _tick_size(plan: dict[str, Any], control: dict[str, Any]) -> float:
    info = plan.get("symbol_info") if isinstance(plan.get("symbol_info"), dict) else {}
    return _safe_float(info.get("tick_size") or control.get("step_price"), 0.0)


def _planned_orders(plan: dict[str, Any], submit: dict[str, Any]) -> list[dict[str, Any]]:
    orders: list[dict[str, Any]] = []
    for key in ("buy_orders", "sell_orders", "forced_reduce_orders"):
        for item in list(plan.get(key) or []):
            if isinstance(item, dict):
                orders.append(item)
    actions = submit.get("validation", {}).get("actions", {}) if isinstance(submit.get("validation"), dict) else {}
    if isinstance(actions, dict):
        for item in list(actions.get("place_orders") or []):
            if isinstance(item, dict):
                orders.append(item)
    return orders


def _order_is_near_market(order: dict[str, Any], *, bid: float, ask: float, tick_size: float, far_ticks: int) -> bool:
    if bid <= 0 or ask <= 0 or tick_size <= 0:
        return False
    side = str(order.get("side") or "").upper().strip()
    price = _safe_float(order.get("price"))
    if price <= 0:
        return False
    allowed_gap = max(int(far_ticks), 0) * tick_size
    if side == "BUY":
        return price >= bid - allowed_gap
    if side == "SELL":
        return price <= ask + allowed_gap
    return False


def _order_market_distance_ticks(
    order: dict[str, Any], *, bid: float, ask: float, tick_size: float
) -> float | None:
    side = str(order.get("side") or "").upper().strip()
    price = _safe_float(order.get("price"))
    if price <= 0 or tick_size <= 0:
        return None
    if side == "BUY":
        return max((bid - price) / tick_size, 0.0)
    if side == "SELL":
        return max((price - ask) / tick_size, 0.0)
    return None


def _order_is_reduce_only(order: dict[str, Any]) -> bool:
    if bool(order.get("reduce_only") or order.get("reduceOnly") or order.get("force_reduce_only")):
        return True
    role = str(order.get("role") or order.get("execution_type") or "").lower()
    return "reduce" in role


def _order_reduce_position_side(order: dict[str, Any]) -> str:
    if not _order_is_reduce_only(order):
        return ""
    position_side = str(order.get("position_side") or order.get("positionSide") or "").upper().strip()
    if position_side in {"LONG", "SHORT"}:
        return position_side
    role = str(order.get("role") or order.get("execution_type") or "").lower()
    if "reduce_long" in role:
        return "LONG"
    if "reduce_short" in role:
        return "SHORT"
    return ""


def _active_order_count(plan: dict[str, Any], submit: dict[str, Any]) -> int:
    observed = (
        submit.get("observed_strategy_open_order_state", {})
        if isinstance(submit.get("observed_strategy_open_order_state"), dict)
        else {}
    )
    plan_summary = submit.get("plan_summary", {}) if isinstance(submit.get("plan_summary"), dict) else {}
    return max(
        _safe_int(plan.get("open_order_count")),
        _safe_int(plan.get("total_open_order_count")),
        _safe_int(observed.get("active_order_count")),
        _safe_int(plan_summary.get("open_order_count")),
    )


def _position_caps(plan: dict[str, Any], control: dict[str, Any]) -> tuple[float, float]:
    long_cap = _safe_float(
        plan.get("effective_max_position_notional")
        or control.get("best_quote_maker_volume_max_long_notional")
        or control.get("max_position_notional")
    )
    short_cap = _safe_float(
        plan.get("effective_max_short_position_notional")
        or control.get("best_quote_maker_volume_max_short_notional")
        or control.get("max_short_position_notional")
    )
    return long_cap, short_cap


def _position_soft_limits(
    plan: dict[str, Any],
    control: dict[str, Any],
    *,
    long_cap: float,
    short_cap: float,
) -> tuple[float, float]:
    soft_ratio = _safe_float(control.get("best_quote_maker_volume_inventory_soft_ratio"))
    ratio_long = long_cap * soft_ratio if long_cap > 0 and soft_ratio > 0 else 0.0
    ratio_short = short_cap * soft_ratio if short_cap > 0 and soft_ratio > 0 else 0.0
    configured_long = _safe_float(
        control.get("pause_buy_position_notional") or plan.get("effective_pause_buy_position_notional")
    )
    configured_short = _safe_float(
        control.get("pause_short_position_notional") or plan.get("effective_pause_short_position_notional")
    )

    def choose(configured: float, ratio_limit: float) -> float:
        candidates = [value for value in (configured, ratio_limit) if value > 0]
        return min(candidates) if candidates else 0.0

    return choose(configured_long, ratio_long), choose(configured_short, ratio_short)


def _cost_gate_blocked(plan: dict[str, Any]) -> bool:
    best_quote = plan.get("best_quote_maker_volume") if isinstance(plan.get("best_quote_maker_volume"), dict) else {}
    gate = best_quote.get("inventory_cost_gate") if isinstance(best_quote.get("inventory_cost_gate"), dict) else {}
    return _safe_int(gate.get("blocked_buy_orders")) > 0 or _safe_int(gate.get("blocked_sell_orders")) > 0


def assess_symbol(
    *,
    symbol: str,
    control: dict[str, Any],
    plan: dict[str, Any],
    submit: dict[str, Any],
    volume_summary: dict[str, Any],
    now: datetime,
    min_volume_notional: float,
    near_cap_ratio: float,
    far_ticks: int,
    plan_stale_seconds: float,
) -> dict[str, Any]:
    reasons: list[str] = []
    if not control:
        reasons.append("missing_control")
    if not _plan_time_is_fresh(plan, now=now, max_age_seconds=plan_stale_seconds):
        reasons.append("latest_plan_stale")
    if not _plan_time_is_fresh(submit, now=now, max_age_seconds=plan_stale_seconds):
        reasons.append("latest_submit_stale")

    gross = _safe_float(volume_summary.get("gross_notional"))
    bid, ask = _live_book(plan, submit)
    tick = _tick_size(plan, control)
    orders = _planned_orders(plan, submit)
    reduce_only_orders = [item for item in orders if _order_is_reduce_only(item)]
    entry_orders = [item for item in orders if not _order_is_reduce_only(item)]
    reduce_only_order_count = len(reduce_only_orders)
    reduce_long_order_count = sum(
        1 for item in reduce_only_orders if _order_reduce_position_side(item) == "LONG"
    )
    reduce_short_order_count = sum(
        1 for item in reduce_only_orders if _order_reduce_position_side(item) == "SHORT"
    )
    entry_order_count = len(entry_orders)
    entry_buy_order_count = sum(
        1 for item in entry_orders if str(item.get("side") or "").upper().strip() == "BUY"
    )
    entry_sell_order_count = sum(
        1 for item in entry_orders if str(item.get("side") or "").upper().strip() == "SELL"
    )
    stale_order_count = len(list(plan.get("stale_orders") or []))
    missing_order_count = len(list(plan.get("missing_orders") or []))
    reduce_only_only = bool(orders) and entry_order_count == 0
    near_orders = [
        item
        for item in orders
        if _order_is_near_market(item, bid=bid, ask=ask, tick_size=tick, far_ticks=far_ticks)
    ]
    near_entry_orders = [
        item
        for item in entry_orders
        if _order_is_near_market(item, bid=bid, ask=ask, tick_size=tick, far_ticks=far_ticks)
    ]
    near_reduce_only_orders = [
        item
        for item in reduce_only_orders
        if _order_is_near_market(item, bid=bid, ask=ask, tick_size=tick, far_ticks=far_ticks)
    ]
    reduce_only_distance_ticks = [
        distance
        for item in reduce_only_orders
        if (distance := _order_market_distance_ticks(item, bid=bid, ask=ask, tick_size=tick))
        is not None
    ]
    active_count = _active_order_count(plan, submit)
    low_volume = gross < max(float(min_volume_notional), 0.0) or active_count <= 0
    if low_volume:
        reasons.append("low_volume")
    all_orders_far = bool(orders) and not near_orders
    ineffective_orders = active_count <= 0 or all_orders_far
    if active_count <= 0:
        reasons.append("no_active_orders")
    if all_orders_far:
        reasons.append("planned_orders_far_from_market")

    long_cap, short_cap = _position_caps(plan, control)
    current_long = _safe_float(plan.get("current_long_notional"))
    current_short = _safe_float(plan.get("current_short_notional"))
    best_quote = plan.get("best_quote_maker_volume") if isinstance(plan.get("best_quote_maker_volume"), dict) else {}
    dynamic_offsets = (
        best_quote.get("dynamic_offsets")
        if isinstance(best_quote.get("dynamic_offsets"), dict)
        else {}
    )
    reduce_freeze = best_quote.get("reduce_freeze") if isinstance(best_quote.get("reduce_freeze"), dict) else {}
    frozen_v2 = best_quote.get("frozen_v2") if isinstance(best_quote.get("frozen_v2"), dict) else {}
    actual_long = (
        _safe_float(reduce_freeze.get("actual_long_notional"))
        if "actual_long_notional" in reduce_freeze
        else None
    )
    actual_short = (
        _safe_float(reduce_freeze.get("actual_short_notional"))
        if "actual_short_notional" in reduce_freeze
        else None
    )
    frozen_long = max(_safe_float(reduce_freeze.get("frozen_long_notional")), 0.0)
    frozen_short = max(_safe_float(reduce_freeze.get("frozen_short_notional")), 0.0)
    frozen_total = max(
        frozen_long + frozen_short,
        _safe_float(frozen_v2.get("frozen_total_notional")),
        0.0,
    )
    near_long_cap = long_cap > 0 and current_long >= long_cap * max(float(near_cap_ratio), 0.0)
    near_short_cap = short_cap > 0 and current_short >= short_cap * max(float(near_cap_ratio), 0.0)
    if near_long_cap:
        reasons.append("long_near_cap")
    if near_short_cap:
        reasons.append("short_near_cap")
    long_soft, short_soft = _position_soft_limits(
        plan,
        control,
        long_cap=long_cap,
        short_cap=short_cap,
    )
    near_long_soft = long_soft > 0 and current_long >= long_soft
    near_short_soft = short_soft > 0 and current_short >= short_soft
    inventory_notional_gap = abs(current_long - current_short)
    balancing_entry_only = (
        current_short > current_long
        and entry_buy_order_count > 0
        and entry_sell_order_count == 0
    ) or (
        current_long > current_short
        and entry_sell_order_count > 0
        and entry_buy_order_count == 0
    )
    balancing_entry_requote_safe = (
        balancing_entry_only
        and reduce_only_order_count == 0
    )
    balancing_budget_raise_safe = balancing_entry_only and _safe_float(
        control.get("sticky_entry_price_tolerance_steps")
    ) > 1.0
    budget_raise_inventory_buffer_blocked = (
        (long_soft > 0 and current_long >= long_soft * 0.85)
        or (short_soft > 0 and current_short >= short_soft * 0.85)
        or (
            min(long_soft, short_soft) > 0
            and inventory_notional_gap >= min(long_soft, short_soft) * 0.25
        )
    )
    if near_long_soft and not near_long_cap:
        reasons.append("long_near_soft_limit")
    if near_short_soft and not near_short_cap:
        reasons.append("short_near_soft_limit")
    buy_paused = bool(plan.get("buy_paused"))
    short_paused = bool(plan.get("short_paused"))
    pause_reasons = {
        str(reason).strip()
        for reason in list(plan.get("pause_reasons") or []) + list(plan.get("short_pause_reasons") or [])
        if str(reason).strip()
    }
    one_sided_inventory_bias = (
        entry_order_count > 0
        and buy_paused != short_paused
        and "inventory_bias" in pause_reasons
    )
    volatility_entry_pause = plan.get("volatility_entry_pause")
    volatility_entry_pause_active = bool(
        volatility_entry_pause.get("active") if isinstance(volatility_entry_pause, dict) else False
    )
    volatility_inventory_gate_active = bool(
        volatility_entry_pause.get("inventory_gate_active")
        if isinstance(volatility_entry_pause, dict)
        else False
    )
    volatility_inventory_reduce_deadlock = (
        volatility_entry_pause_active
        and volatility_inventory_gate_active
        and active_count <= 0
        and max(current_long, current_short) > 0
    )
    active_pair_reduce = (
        plan.get("best_quote_active_pair_reduce")
        if isinstance(plan.get("best_quote_active_pair_reduce"), dict)
        else {}
    )
    active_pair_reduce_suppression_deadlock = (
        bool(active_pair_reduce.get("enabled"))
        and bool(active_pair_reduce.get("active"))
        and str(active_pair_reduce.get("reason") or "") == "no_valid_reduce_order"
        and _safe_int(active_pair_reduce.get("order_count")) <= 0
        and _safe_int(active_pair_reduce.get("suppressed_entry_order_count")) > 0
        and active_count <= 0
    )
    active_pair_reduce_below_soft_deadlock = (
        bool(active_pair_reduce.get("enabled"))
        and bool(active_pair_reduce.get("active"))
        and str(active_pair_reduce.get("reason") or "") == "soft_pair_reduce"
        and bool(active_pair_reduce.get("normal_entry_suppressed"))
        and _safe_int(active_pair_reduce.get("suppressed_entry_order_count")) > 0
        and not bool(active_pair_reduce.get("completed"))
        and low_volume
        and actual_long is not None
        and actual_short is not None
        and long_soft > 0
        and short_soft > 0
        and actual_long < long_soft
        and actual_short < short_soft
        and frozen_total <= 0
    )
    if active_pair_reduce_suppression_deadlock or active_pair_reduce_below_soft_deadlock:
        reasons.append("active_pair_reduce_suppression_deadlock")

    return {
        "symbol": symbol,
        "reasons": reasons,
        "low_volume": low_volume,
        "gross_notional": gross,
        "active_order_count": active_count,
        "planned_order_count": len(orders),
        "planned_entry_order_count": entry_order_count,
        "planned_entry_buy_order_count": entry_buy_order_count,
        "planned_entry_sell_order_count": entry_sell_order_count,
        "stale_order_count": stale_order_count,
        "missing_order_count": missing_order_count,
        "balancing_entry_only": balancing_entry_only,
        "balancing_entry_requote_safe": balancing_entry_requote_safe,
        "balancing_budget_raise_safe": balancing_budget_raise_safe,
        "planned_reduce_only_order_count": reduce_only_order_count,
        "planned_reduce_long_order_count": reduce_long_order_count,
        "planned_reduce_short_order_count": reduce_short_order_count,
        "planned_reduce_only_only": reduce_only_only,
        "near_market_order_count": len(near_orders),
        "near_market_entry_order_count": len(near_entry_orders),
        "near_market_reduce_only_order_count": len(near_reduce_only_orders),
        "nearest_reduce_only_distance_ticks": (
            min(reduce_only_distance_ticks) if reduce_only_distance_ticks else None
        ),
        "all_entry_orders_far": bool(entry_orders) and not near_entry_orders,
        "dynamic_quote_offset_applied": bool(dynamic_offsets.get("dynamic_quote_offset_applied")),
        "dynamic_quote_offset_ticks": _safe_int(dynamic_offsets.get("quote_offset_ticks")),
        "ineffective_orders": ineffective_orders,
        "all_orders_far": all_orders_far,
        "near_cap": near_long_cap or near_short_cap,
        "inventory_soft_pressure": near_long_soft or near_short_soft,
        "long_near_cap": near_long_cap,
        "short_near_cap": near_short_cap,
        "long_near_soft": near_long_soft,
        "short_near_soft": near_short_soft,
        "inventory_notional_gap": inventory_notional_gap,
        "budget_raise_inventory_buffer_blocked": budget_raise_inventory_buffer_blocked,
        "long_soft_limit_notional": long_soft,
        "short_soft_limit_notional": short_soft,
        "inventory_soft_ratio": _safe_float(control.get("best_quote_maker_volume_inventory_soft_ratio")),
        "buy_paused": buy_paused,
        "short_paused": short_paused,
        "pause_reasons": sorted(pause_reasons),
        "one_sided_inventory_bias": one_sided_inventory_bias,
        "volatility_entry_pause_active": volatility_entry_pause_active,
        "volatility_inventory_gate_active": volatility_inventory_gate_active,
        "volatility_inventory_reduce_deadlock": volatility_inventory_reduce_deadlock,
        "active_pair_reduce_suppression_deadlock": active_pair_reduce_suppression_deadlock,
        "active_pair_reduce_below_soft_deadlock": active_pair_reduce_below_soft_deadlock,
        "current_long_notional": current_long,
        "current_short_notional": current_short,
        "actual_long_notional": actual_long,
        "actual_short_notional": actual_short,
        "frozen_long_notional": frozen_long,
        "frozen_short_notional": frozen_short,
        "frozen_total_notional": frozen_total,
        "max_long_notional": long_cap,
        "max_short_notional": short_cap,
        "cost_gate_blocked": _cost_gate_blocked(plan),
        "bid_price": bid,
        "ask_price": ask,
        "tick_size": tick,
    }


def _backup_control(control_path: Path, control: dict[str, Any], now: datetime) -> Path:
    stamp = now.strftime("%Y%m%dT%H%M%SZ")
    backup_path = control_path.with_name(control_path.name + f".bak_bq_volume_recovery_{stamp}")
    backup_path.write_text(
        json.dumps(control, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return backup_path


def _backup_valid_loop_state_before_restart(
    *, symbol: str, control_path: Path, now: datetime
) -> Path | None:
    state_path = control_path.parent / f"{symbol.lower()}_loop_state.json"
    payload = _read_json(state_path)
    if not payload:
        return None
    backup_path = state_path.with_name(state_path.name + ".bak_bq_recovery_restart")
    _write_json(backup_path, payload)
    os.utime(backup_path, (now.timestamp(), now.timestamp()))
    return backup_path


def _apply_control_update(
    *,
    symbol: str,
    control_path: Path,
    control: dict[str, Any],
    updates: dict[str, Any],
    now: datetime,
    dry_run: bool,
    restart_runner: RestartRunner,
) -> tuple[list[str], str | None]:
    if dry_run:
        changed = [key for key, value in updates.items() if control.get(key) != value]
        return changed, None
    # The decision was made from ``control`` earlier in this cycle.  Re-read
    # under the symbol lock so a concurrent observer/writer cannot be erased
    # by a stale whole-document write, then keep the lock through restart.
    with exclusive_control_lock(control_path):
        current = _read_json(control_path) or dict(control)
        changed = [key for key, value in updates.items() if current.get(key) != value]
        if not changed:
            return [], None
        updated = dict(current)
        updated.update(updates)
        mark_recovery_owned(updated)
        updated["recovery_control_updated_at"] = now.isoformat()
        backup_path = _backup_control(control_path, current, now)
        _backup_valid_loop_state_before_restart(
            symbol=symbol,
            control_path=control_path,
            now=now,
        )
        _write_json(control_path, updated)
        restart_runner(symbol)
        return changed, str(backup_path)


def _arx_independent_freeze_policy_updates(
    *,
    symbol: str,
    control: dict[str, Any],
    temporary_anti_chase_relief: bool = False,
) -> dict[str, Any]:
    if symbol.upper().strip() != "ARXUSDT":
        return {}
    freeze_was_enabled = bool(
        control.get("best_quote_maker_volume_reduce_freeze_enabled")
    )
    anti_chase_min_key = "best_quote_maker_volume_same_side_entry_price_guard_min_notional"
    anti_chase_gap_key = "best_quote_maker_volume_same_side_entry_price_guard_gap_ticks"
    # Retain a bounded recovery relaxation while it is active. A migration from
    # freeze-off is different: it must start from the canonical v3 guard.
    configured_anti_chase_min = control.get(anti_chase_min_key) if freeze_was_enabled else 200.0
    if configured_anti_chase_min is None:
        configured_anti_chase_min = 200.0
    configured_anti_chase_gap = control.get(anti_chase_gap_key) if freeze_was_enabled else 1
    if configured_anti_chase_gap is None:
        configured_anti_chase_gap = 1
    expected = {
        "best_quote_maker_volume_reduce_freeze_enabled": True,
        "best_quote_maker_volume_reduce_freeze_loss_ratio": 0.01,
        "best_quote_maker_volume_reduce_freeze_band_budget_enabled": True,
        "best_quote_maker_volume_reduce_freeze_band_budget_price_ratio": 0.01,
        "best_quote_maker_volume_reduce_freeze_band_budget_base_notional": 100.0,
        "best_quote_maker_volume_frozen_total_cap_notional": 800.0,
        "best_quote_maker_volume_frozen_long_cap_notional": 800.0,
        "best_quote_maker_volume_frozen_short_cap_notional": 800.0,
        "best_quote_maker_volume_reduce_freeze_quality_gate_enabled": True,
        "best_quote_maker_volume_reduce_freeze_quality_max_loss_ratio": 0.03,
        "best_quote_maker_volume_reduce_freeze_quality_release_profit_ratio": 0.002,
        "best_quote_maker_volume_reduce_freeze_quality_max_atr_multiple": 3.0,
        "best_quote_maker_volume_reduce_freeze_quality_atr_floor_ratio": 0.004,
        "best_quote_maker_volume_reduce_freeze_quality_easy_bucket_notional": 100.0,
        "best_quote_maker_volume_reduce_freeze_quality_medium_bucket_notional": 50.0,
        "best_quote_maker_volume_reduce_freeze_quality_hard_bucket_notional": 25.0,
        "best_quote_maker_volume_reduce_freeze_profitable_pair_gate_enabled": False,
        "best_quote_maker_volume_frozen_pair_release_enabled": False,
        "best_quote_maker_volume_frozen_single_leg_take_profit_enabled": True,
        "best_quote_maker_volume_frozen_pair_release_min_profit_ratio": 0.01,
        "best_quote_maker_volume_frozen_pair_release_allow_loss": False,
        "best_quote_maker_volume_same_side_entry_price_guard_enabled": True,
        "best_quote_maker_volume_same_side_entry_price_guard_report_only": False,
        anti_chase_min_key: configured_anti_chase_min,
        anti_chase_gap_key: configured_anti_chase_gap,
        "best_quote_maker_volume_net_loss_reduce_enabled": False,
        "adverse_reduce_enabled": False,
        "hard_loss_forced_reduce_enabled": False,
    }
    if temporary_anti_chase_relief:
        expected.pop(anti_chase_min_key)
        expected.pop(anti_chase_gap_key)
    return {key: value for key, value in expected.items() if control.get(key) != value}


def _default_restart_runner(symbol: str, *, runner_wrapper: str) -> object:
    command = [runner_wrapper, "restart", symbol]
    try:
        result = subprocess.run(
            command,
            check=True,
            timeout=120,
            capture_output=True,
            text=True,
        )
    except subprocess.TimeoutExpired as exc:
        raise subprocess.CalledProcessError(
            124, command, output=exc.output, stderr=exc.stderr
        ) from exc
    if not _runner_is_active(symbol):
        raise subprocess.CalledProcessError(
            1,
            command,
            output=result.stdout,
            stderr=f"runner restart completed but {symbol} is not active",
        )
    return result


def _default_stop_runner(symbol: str, *, runner_wrapper: str) -> object:
    return subprocess.run([runner_wrapper, "stop", symbol], check=True)


def recover_arx_exchange_order_drift(
    *,
    symbol: str,
    local_active_order_count: int,
    submit: dict[str, Any] | None = None,
    state: dict[str, Any],
    now: datetime,
    cooldown_seconds: float,
    dry_run: bool,
    runner_wrapper: str,
    restart_runner: RestartRunner | None = None,
    exchange_snapshot_fetcher: CorruptStateExchangeFetcher | None = None,
) -> dict[str, Any] | None:
    """Restart ARX only when Binance confirms the runner book is empty.

    The runner's websocket/cache view can preserve a filled or cancelled order
    briefly.  That must not suppress recovery while the exchange has no
    runner-owned maker order at all.
    """
    normalized_symbol = symbol.upper().strip()
    if normalized_symbol != "ARXUSDT" or int(local_active_order_count) <= 0:
        return None
    try:
        exchange = (exchange_snapshot_fetcher or _fetch_arx_strategy_order_snapshot)(
            normalized_symbol
        )
    except Exception as exc:
        return {
            "symbol": normalized_symbol,
            "action": "skip_arx_exchange_order_drift_snapshot_failed",
            "changed_keys": [],
            "backup_path": None,
            "dry_run": dry_run,
            "restart_failed": None,
            "local_active_order_count": int(local_active_order_count),
            "exchange_order_snapshot_error": str(exc)[:240],
        }
    exchange_open_order_count = _safe_int(exchange.get("strategy_open_order_count"), -1)
    if not should_restart_arx_for_exchange_order_drift(
        symbol=normalized_symbol,
        local_active_order_count=local_active_order_count,
        exchange_open_order_count=exchange_open_order_count,
    ):
        return None
    if has_recent_arx_submit_activity(
        submit=submit or {}, now=now, max_age_seconds=cooldown_seconds
    ):
        return {
            "symbol": normalized_symbol,
            "action": "hold_arx_exchange_order_drift_after_recent_activity",
            "changed_keys": [],
            "backup_path": None,
            "dry_run": dry_run,
            "restart_failed": None,
            "local_active_order_count": int(local_active_order_count),
            "exchange_open_order_count": exchange_open_order_count,
            "exchange_strategy_order_ids": exchange.get("strategy_order_ids") or [],
        }
    item = _symbol_state(state, normalized_symbol)
    last_restart_at = _parse_time(item.get("last_exchange_order_drift_restart_at"))
    cooldown_active = (
        last_restart_at is not None
        and (now - last_restart_at).total_seconds() < max(float(cooldown_seconds), 0.0)
    )
    result = {
        "symbol": normalized_symbol,
        "changed_keys": [],
        "backup_path": None,
        "dry_run": dry_run,
        "restart_failed": None,
        "local_active_order_count": int(local_active_order_count),
        "exchange_open_order_count": exchange_open_order_count,
        "exchange_strategy_order_ids": exchange.get("strategy_order_ids") or [],
    }
    if cooldown_active:
        result["action"] = "hold_arx_exchange_order_drift_restart_cooldown"
        return result
    if dry_run:
        result["action"] = "dry_run_restart_arx_exchange_order_drift"
        return result
    restart = restart_runner or (
        lambda item_symbol: _default_restart_runner(item_symbol, runner_wrapper=runner_wrapper)
    )
    try:
        restart(normalized_symbol)
    except subprocess.CalledProcessError as exc:
        result["action"] = "restart_arx_exchange_order_drift_failed"
        result["restart_failed"] = str(exc)
        return result
    item.update(
        {
            "last_exchange_order_drift_restart_at": now.isoformat(),
            "last_recovery_action_at": now.isoformat(),
            "last_recovery_action": "restart_arx_exchange_order_drift",
            "status": "exchange_order_drift_recovery_active",
        }
    )
    result["action"] = "restart_arx_exchange_order_drift"
    return result


def recover_arx_runner_error_loop(
    *,
    output_dir: Path,
    symbol: str,
    state: dict[str, Any],
    now: datetime,
    cooldown_seconds: float,
    dry_run: bool,
    runner_wrapper: str,
    restart_runner: RestartRunner | None = None,
) -> dict[str, Any] | None:
    """Break a live ARX runner error loop before it exhausts its retries."""
    normalized_symbol = symbol.upper().strip()
    if normalized_symbol != "ARXUSDT":
        return None
    event = _latest_runner_error_event(Path(output_dir), normalized_symbol)
    consecutive_errors = _safe_int(event.get("consecutive_errors"))
    if consecutive_errors < 3:
        return None
    item = _symbol_state(state, normalized_symbol)
    last_restart_at = _parse_time(item.get("last_runner_error_restart_at"))
    if (
        last_restart_at is not None
        and (now - last_restart_at).total_seconds() < max(float(cooldown_seconds), 0.0)
    ):
        return {
            "symbol": normalized_symbol,
            "action": "hold_arx_runner_error_restart_cooldown",
            "changed_keys": [],
            "backup_path": None,
            "dry_run": dry_run,
            "restart_failed": None,
            "runner_error_type": event.get("error_type"),
            "runner_consecutive_errors": consecutive_errors,
        }
    result = {
        "symbol": normalized_symbol,
        "changed_keys": [],
        "backup_path": None,
        "dry_run": dry_run,
        "restart_failed": None,
        "runner_error_type": event.get("error_type"),
        "runner_consecutive_errors": consecutive_errors,
    }
    if dry_run:
        result["action"] = "dry_run_restart_arx_runner_error_loop"
        return result
    restart = restart_runner or (
        lambda item_symbol: _default_restart_runner(item_symbol, runner_wrapper=runner_wrapper)
    )
    try:
        restart(normalized_symbol)
    except subprocess.CalledProcessError as exc:
        result["action"] = "restart_arx_runner_error_loop_failed"
        result["restart_failed"] = str(exc)
        return result
    item.update(
        {
            "last_runner_error_restart_at": now.isoformat(),
            "last_recovery_action_at": now.isoformat(),
            "last_recovery_action": "restart_arx_runner_error_loop",
            "status": "runner_error_recovery_active",
        }
    )
    result["action"] = "restart_arx_runner_error_loop"
    return result


def recover_arx_effective_control_drift(
    *,
    output_dir: Path,
    symbol: str,
    state: dict[str, Any],
    now: datetime,
    cooldown_seconds: float,
    dry_run: bool,
    runner_wrapper: str,
    restart_runner: RestartRunner | None = None,
) -> dict[str, Any] | None:
    """Restart when a recovery control was not consumed by the live runner."""
    normalized_symbol = symbol.upper().strip()
    if normalized_symbol != "ARXUSDT":
        return None
    control = _read_json(_control_path(Path(output_dir), normalized_symbol))
    plan = _read_json(_plan_path(Path(output_dir), normalized_symbol))
    if control.get("sticky_entry_preserve_less_aggressive") is not False:
        return None
    if plan.get("effective_sticky_entry_preserve_less_aggressive") is False:
        return None
    item = _symbol_state(state, normalized_symbol)
    last_restart_at = _parse_time(item.get("last_effective_control_drift_restart_at"))
    if (
        last_restart_at is not None
        and (now - last_restart_at).total_seconds() < max(float(cooldown_seconds), 0.0)
    ):
        return {
            "symbol": normalized_symbol,
            "action": "hold_arx_effective_control_drift_restart_cooldown",
            "changed_keys": [],
            "backup_path": None,
            "dry_run": dry_run,
            "restart_failed": None,
            "control_sticky_entry_preserve_less_aggressive": False,
            "plan_effective_sticky_entry_preserve_less_aggressive": plan.get(
                "effective_sticky_entry_preserve_less_aggressive"
            ),
        }
    result = {
        "symbol": normalized_symbol,
        "changed_keys": [],
        "backup_path": None,
        "dry_run": dry_run,
        "restart_failed": None,
        "control_sticky_entry_preserve_less_aggressive": False,
        "plan_effective_sticky_entry_preserve_less_aggressive": plan.get(
            "effective_sticky_entry_preserve_less_aggressive"
        ),
    }
    if dry_run:
        result["action"] = "dry_run_restart_arx_effective_control_drift"
        return result
    restart = restart_runner or (
        lambda item_symbol: _default_restart_runner(item_symbol, runner_wrapper=runner_wrapper)
    )
    try:
        restart(normalized_symbol)
    except subprocess.CalledProcessError as exc:
        result["action"] = "restart_arx_effective_control_drift_failed"
        result["restart_failed"] = str(exc)
        return result
    item.update(
        {
            "last_effective_control_drift_restart_at": now.isoformat(),
            "last_recovery_action_at": now.isoformat(),
            "last_recovery_action": "restart_arx_effective_control_drift",
            "status": "effective_control_drift_recovery_active",
        }
    )
    result["action"] = "restart_arx_effective_control_drift"
    return result


def should_enter_loss_reduce(
    *,
    low_volume: bool,
    effective_inventory_soft_pressure: bool,
    sla_recovery_due: bool,
    target_pace_behind: bool,
    inventory_soft_pressure: bool,
    active_order_count: int,
    planned_order_count: int,
    planned_reduce_only_only: bool,
    no_fill_seconds: float,
    trigger_seconds: float,
    recovery_hold_satisfied: bool,
    high_recovery_wear: bool,
    confirmed_loss_reduce_wear: bool,
    recovery_reapply_debounced: bool,
    post_restore_budget_cooldown_active: bool,
    recovery_expected_allow_loss: bool,
    require_soft_pressure_for_allow_loss: bool,
    effective_near_market_flow: bool,
) -> bool:
    """Pure entry decision for the loss-reduce recovery profile.

    The invariant is intentionally at the profile boundary: a live,
    near-market maker book wins over a low-volume sample.  That prevents the
    next guard minute from replacing usable flow with an opposing profile.
    """
    recovery_signal = effective_inventory_soft_pressure or (
        sla_recovery_due
        and target_pace_behind
        and inventory_soft_pressure
        and (effective_inventory_soft_pressure or not require_soft_pressure_for_allow_loss)
    ) or (
        sla_recovery_due
        and inventory_soft_pressure
        and (
            (active_order_count <= 0 and planned_order_count <= 0)
            or (planned_reduce_only_only and no_fill_seconds >= max(float(trigger_seconds), 0.0))
        )
    )
    return (
        low_volume
        and recovery_signal
        and not effective_near_market_flow
        and recovery_hold_satisfied
        and not high_recovery_wear
        and not confirmed_loss_reduce_wear
        and not recovery_reapply_debounced
        and not post_restore_budget_cooldown_active
        and not recovery_expected_allow_loss
    )


def evaluate_action_verification(
    *,
    action_age_seconds: float,
    plan_is_fresh: bool,
    open_order_drift: int,
    prior_failures: int,
    execution_progress: bool = False,
) -> tuple[str, int]:
    """Classify one post-actuation feedback cycle without side effects."""
    # A fast GTX maker can be filled before the next stream snapshot.  In
    # that case its absence from open orders is execution progress, not a
    # failed recovery action.
    if plan_is_fresh and (
        abs(int(open_order_drift)) == 0 or bool(execution_progress)
    ):
        return "confirmed", 0
    failures = max(int(prior_failures), 0) + 1
    if failures >= 2 or action_age_seconds >= 120.0:
        return "failed", failures
    return "pending", failures


def is_arx_severe_volume_priority_recovery(
    *,
    symbol: str,
    target_pace_behind: bool,
    pace_ratio: float,
    planned_reduce_only_order_count: int,
) -> bool:
    return (
        symbol.upper().strip() == "ARXUSDT"
        and bool(target_pace_behind)
        and float(pace_ratio) < 1.0
        and int(planned_reduce_only_order_count) > 0
    )


def is_target_pace_ahead(
    *,
    symbol: str,
    required_hourly_notional: float,
    trailing_60m_notional: float,
    trailing_15m_notional: float,
    trailing_5m_notional: float,
    trailing_3m_notional: float,
    target_pace_fraction: float,
) -> bool:
    required_hourly = max(float(required_hourly_notional), 0.0)
    fraction = max(float(target_pace_fraction), 0.0)
    if required_hourly <= 0:
        return False
    is_arx = symbol.upper().strip() == "ARXUSDT"
    five_minute_scale = 1.0 if is_arx else 0.5
    return (
        float(trailing_60m_notional) >= required_hourly * fraction
        and float(trailing_15m_notional) >= required_hourly * 0.25 * fraction
        and float(trailing_5m_notional) >= required_hourly / 12.0 * fraction * five_minute_scale
        and (
            not is_arx
            or float(trailing_3m_notional) >= required_hourly / 20.0 * fraction
        )
    )


def should_close_loss_reduce_when_target_pace_ahead(
    *,
    symbol: str,
    target_pace_ahead: bool,
    allow_loss_reduce_only: bool,
    current_long_notional: float,
    current_short_notional: float,
    max_long_notional: float,
    max_short_notional: float,
) -> bool:
    """Close temporary ARX loss-reduce as soon as maker pace recovers."""
    if not bool(target_pace_ahead) or not bool(allow_loss_reduce_only):
        return False
    if symbol.upper().strip() == "ARXUSDT":
        return True
    return (
        _safe_float(current_long_notional) < _safe_float(max_long_notional)
        and _safe_float(current_short_notional) < _safe_float(max_short_notional)
    )


def should_force_arx_severe_near_maker_entry(
    *,
    symbol: str,
    target_pace_behind: bool,
    pace_ratio: float,
    planned_entry_order_count: int,
    effective_inventory_soft_pressure: bool,
    allow_loss_reduce_only: bool,
) -> bool:
    return (
        symbol.upper().strip() == "ARXUSDT"
        and bool(target_pace_behind)
        and float(pace_ratio) < 0.30
        and int(planned_entry_order_count) > 0
        and not bool(effective_inventory_soft_pressure)
        and not bool(allow_loss_reduce_only)
    )


def should_relax_arx_zero_order_volume_blockers(
    *,
    symbol: str,
    target_pace_behind: bool,
    pace_ratio: float,
    active_order_count: int,
    planned_order_count: int,
    near_cap: bool,
    volatility_entry_pause_active: bool,
    pause_reasons: list[str] | set[str] | tuple[str, ...],
    same_side_entry_blocked: bool = False,
) -> bool:
    """Permit a bounded ARX escape when soft guards leave the book empty.

    The runner's hard position caps and volatility pause remain authoritative.
    This only gives the recovery guard a reversible way to restore maker
    participation after trend/bias protections have jointly produced no orders.
    """
    soft_blockers = {"trend_entry_guard", "trend_loss_reduce_guard", "inventory_bias"}
    return (
        symbol.upper().strip() == "ARXUSDT"
        and bool(target_pace_behind)
        and float(pace_ratio) < 0.30
        and int(active_order_count) <= 0
        # One unplaced plan leg is operationally equivalent to an empty book:
        # it cannot restore two-sided maker flow by itself.
        and int(planned_order_count) <= 1
        and not bool(near_cap)
        and not bool(volatility_entry_pause_active)
        and (
            bool(soft_blockers.intersection(str(reason) for reason in pause_reasons))
            or bool(same_side_entry_blocked)
        )
    )


def should_restart_arx_for_exchange_order_drift(
    *,
    symbol: str,
    local_active_order_count: int,
    exchange_open_order_count: int,
) -> bool:
    """A stream-only order count must not suppress ARX recovery."""
    return (
        symbol.upper().strip() == "ARXUSDT"
        and int(local_active_order_count) > 0
        and int(exchange_open_order_count) == 0
    )


def has_recent_arx_submit_activity(
    *, submit: dict[str, Any], now: datetime, max_age_seconds: float
) -> bool:
    """A fresh maker event is not an empty-book runner failure.

    REST can briefly observe no order between a maker cancellation and the
    replacement.  A new or filled strategy order in that interval proves the
    runner is alive and re-quoting; only a quiet stream may justify restart.
    """
    events = submit.get("observed_execution_events") or []
    cutoff_ms = (now - timedelta(seconds=max(float(max_age_seconds), 0.0))).timestamp() * 1000
    return any(
        isinstance(event, dict)
        and str(event.get("kind") or "").upper()
        in {"ORDER_NEW", "ORDER_FILLED", "ORDER_PARTIALLY_FILLED"}
        and _safe_float(event.get("event_time")) >= cutoff_ms
        for event in events
    )


def should_hold_arx_volume_priority_release(
    *,
    symbol: str,
    target_pace_behind: bool,
    pace_ratio: float,
    allow_loss_reduce_only: bool,
    planned_reduce_only_order_count: int,
) -> bool:
    # A recovery timeout is not evidence that pace recovered.  While ARX is
    # still below 30% of its required pace and has an executable reduce leg,
    # retain the recovery profile even when that profile has already disabled
    # its temporary loss-reduce switch.
    return is_arx_severe_volume_priority_recovery(
        symbol=symbol,
        target_pace_behind=target_pace_behind,
        pace_ratio=pace_ratio,
        planned_reduce_only_order_count=planned_reduce_only_order_count,
    )


def should_hold_arx_recovery_capacity_for_net_limit(
    *,
    symbol: str,
    actual_net_notional: float,
    original_max_actual_net_notional: float,
) -> bool:
    """Do not restore ARX's baseline net cap while it would stop the runner."""
    return (
        symbol.upper().strip() == "ARXUSDT"
        and _safe_float(original_max_actual_net_notional) > 0
        and abs(_safe_float(actual_net_notional))
        > _safe_float(original_max_actual_net_notional)
    )


def should_restore_near_market_recovery(
    *,
    symbol: str,
    effective_near_market_flow: bool,
    target_pace_behind: bool,
) -> bool:
    return bool(effective_near_market_flow) and not (
        symbol.upper().strip() == "ARXUSDT" and bool(target_pace_behind)
    )


def should_enable_arx_sticky_requote(
    *,
    symbol: str,
    target_pace_behind: bool,
    pace_ratio: float,
    near_cap: bool,
    volatility_entry_pause_active: bool,
) -> bool:
    """Permit a best-quote replacement while ARX still materially trails pace."""
    return (
        symbol.upper().strip() == "ARXUSDT"
        and bool(target_pace_behind)
        and float(pace_ratio) < 0.75
        and not bool(near_cap)
        and not bool(volatility_entry_pause_active)
    )


def should_bypass_arx_submit_failure_recovery_gate(
    *,
    symbol: str,
    target_pace_behind: bool,
    low_volume: bool,
    active_order_count: int,
    volatility_entry_pause_active: bool,
    ledger_position_drift_blocked: bool,
    recovery_gate_reasons: Iterable[str],
    validation_errors: Iterable[str],
) -> bool:
    """Let ARX repair a stale submit cap without opening other validation bypasses."""
    reasons = {str(item).strip() for item in recovery_gate_reasons if str(item).strip()}
    errors = [str(item).strip() for item in validation_errors if str(item).strip()]
    return (
        symbol.upper().strip() == "ARXUSDT"
        and bool(target_pace_behind)
        and int(active_order_count) <= 1
        and not bool(volatility_entry_pause_active)
        and not bool(ledger_position_drift_blocked)
        and reasons
        and reasons <= {"latest_submit_error", "latest_validation_failed"}
        and bool(errors)
        and all("above max_total_notional=" in item for item in errors)
    )


def should_repair_arx_loss_reduce_flow(
    *,
    symbol: str,
    target_pace_behind: bool,
    allow_loss_reduce_only: bool,
    has_flow_updates: bool,
    high_recovery_wear: bool,
    confirmed_loss_reduce_wear: bool,
    volatility_entry_pause_active: bool,
) -> bool:
    return (
        symbol.upper().strip() == "ARXUSDT"
        and bool(target_pace_behind)
        and bool(allow_loss_reduce_only)
        and bool(has_flow_updates)
        and not bool(high_recovery_wear)
        and not bool(confirmed_loss_reduce_wear)
        and not bool(volatility_entry_pause_active)
    )


def arx_lopsided_entry_recovery_updates(
    *, control: dict[str, Any], assessment: dict[str, Any]
) -> dict[str, Any]:
    """Restore the empty opposite maker leg when ARX is trapped on one side."""
    current_long = max(_safe_float(assessment.get("current_long_notional")), 0.0)
    current_short = max(_safe_float(assessment.get("current_short_notional")), 0.0)
    max_long = max(_safe_float(assessment.get("max_long_notional")), 0.0)
    max_short = max(_safe_float(assessment.get("max_short_notional")), 0.0)
    if (
        str(assessment.get("symbol") or "").upper() != "ARXUSDT"
        or not bool(assessment.get("target_pace_behind"))
        or not bool(assessment.get("low_volume"))
        or bool(assessment.get("volatility_entry_pause_active"))
        or _safe_int(assessment.get("planned_entry_order_count")) > 0
        or max_long <= 0
        or max_short <= 0
        or current_long < max_long * 0.9
        or current_short > max_short * 0.5
    ):
        return {}
    updates = {
        key: False
        for key in (
            "best_quote_maker_volume_dynamic_control_trend_entry_guard_enabled",
            "best_quote_maker_volume_dynamic_control_trend_inventory_guard_enabled",
            "best_quote_maker_volume_dynamic_control_trend_loss_reduce_guard_enabled",
        )
        if bool(control.get(key))
    }
    if _safe_float(control.get("best_quote_maker_volume_inventory_soft_ratio")) < 1.2:
        updates["best_quote_maker_volume_inventory_soft_ratio"] = 1.2
    return updates


def should_bypass_arx_recovery_drift_debounce(
    *,
    symbol: str,
    target_pace_behind: bool,
    no_fill_seconds: float,
    fast_sla_seconds: float,
    volatility_entry_pause_active: bool,
) -> bool:
    """Reapply a lost ARX capacity recovery when the fast SLA is already missed."""
    return (
        symbol.upper().strip() == "ARXUSDT"
        and bool(target_pace_behind)
        and float(no_fill_seconds) >= max(float(fast_sla_seconds), 0.0)
        and not bool(volatility_entry_pause_active)
    )


def arx_single_side_cap_updates(control: dict[str, Any]) -> dict[str, Any]:
    """Keep all ARX entry paths within the operational single-side hard cap."""
    targets = {
        "pause_buy_position_notional": 1800.0,
        "pause_short_position_notional": 1800.0,
        "max_position_notional": 2000.0,
        "max_short_position_notional": 2000.0,
        "maker_max_long_notional": 2000.0,
        "maker_max_short_notional": 2000.0,
        "best_quote_maker_volume_max_long_notional": 2000.0,
        "best_quote_maker_volume_max_short_notional": 2000.0,
    }
    return {
        key: value for key, value in targets.items() if _safe_float(control.get(key)) != value
    }


def arx_frozen_inventory_headroom_updates(
    *,
    control: dict[str, Any],
    frozen_long_notional: float,
    frozen_short_notional: float,
) -> dict[str, Any]:
    """Reserve frozen ARX inventory inside the real exchange-side limits."""
    frozen_long = max(_safe_float(frozen_long_notional), 0.0)
    frozen_short = max(_safe_float(frozen_short_notional), 0.0)
    if frozen_long <= 0.0 and frozen_short <= 0.0:
        return {}
    long_soft = max(1800.0 - frozen_long, 1.0)
    short_soft = max(1800.0 - frozen_short, 1.0)
    long_hard = max(2000.0 - frozen_long, 1.0)
    short_hard = max(2000.0 - frozen_short, 1.0)
    targets = {
        "pause_buy_position_notional": long_soft,
        "pause_short_position_notional": short_soft,
        "max_position_notional": long_hard,
        "max_short_position_notional": short_hard,
        "maker_max_long_notional": long_hard,
        "maker_max_short_notional": short_hard,
        "best_quote_maker_volume_max_long_notional": long_hard,
        "best_quote_maker_volume_max_short_notional": short_hard,
    }
    return {
        key: value for key, value in targets.items() if _safe_float(control.get(key)) != value
    }


def arx_directional_unwind_quote_updates(control: dict[str, Any]) -> dict[str, Any]:
    """Keep an active ARX directional unwind at the best maker quote."""
    if str(control.get("best_quote_maker_volume_directional_net_guard") or "off").lower() not in {
        "net_long",
        "net_short",
    }:
        return {}
    targets = {
        "best_quote_maker_volume_active_pair_reduce_enabled": False,
        "best_quote_maker_volume_quote_offset_ticks": 0,
        "best_quote_maker_volume_dynamic_control_trend_inventory_guard_reduce_extra_ticks": 0,
        "best_quote_maker_volume_dynamic_control_trend_loss_reduce_guard_reduce_extra_ticks": 0,
        "best_quote_maker_volume_dynamic_control_trend_loss_reduce_guard_recent_loss_extra_ticks": 0,
    }
    return {key: value for key, value in targets.items() if control.get(key) != value}


def arx_side_cap_unwind_updates(
    *,
    control: dict[str, Any],
    actual_long_notional: float,
    actual_short_notional: float,
) -> dict[str, Any]:
    """Route maker-only recovery to the side that remains above the hard cap."""
    long_excess = max(_safe_float(actual_long_notional) - 2000.0, 0.0)
    short_excess = max(_safe_float(actual_short_notional) - 2000.0, 0.0)
    long_soft_excess = max(_safe_float(actual_long_notional) - 1800.0, 0.0)
    short_soft_excess = max(_safe_float(actual_short_notional) - 1800.0, 0.0)
    net_limit = max(_safe_float(control.get("max_actual_net_notional")), 0.0) or 1000.0
    net_notional = _safe_float(actual_long_notional) - _safe_float(actual_short_notional)
    current_direction = str(
        control.get("best_quote_maker_volume_directional_net_guard") or "off"
    ).lower()
    if long_excess > 0.0 or short_excess > 0.0:
        direction: str | None = "net_long" if long_excess >= short_excess else "net_short"
    elif abs(net_notional) > net_limit:
        direction = "net_long" if net_notional > 0.0 else "net_short"
    elif long_soft_excess > 0.0 or short_soft_excess > 0.0:
        direction = "net_long" if long_soft_excess >= short_soft_excess else "net_short"
    elif (
        current_direction == "net_long"
        and _safe_float(actual_long_notional) > 1800.0
    ):
        direction = "net_long"
    elif (
        current_direction == "net_short"
        and _safe_float(actual_short_notional) > 1800.0
    ):
        direction = "net_short"
    else:
        direction = None
    if direction is None:
        if (
            current_direction == "off"
            and not bool(control.get("best_quote_maker_volume_allow_loss_reduce_only"))
            and not bool(control.get("best_quote_maker_volume_net_loss_reduce_enabled"))
        ):
            return {}
        targets = {
            "best_quote_maker_volume_directional_net_guard": "off",
            "best_quote_maker_volume_allow_loss_reduce_only": False,
            "best_quote_maker_volume_net_loss_reduce_enabled": False,
            "best_quote_maker_volume_active_pair_reduce_enabled": False,
        }
    else:
        targets = {
            "best_quote_maker_volume_directional_net_guard": direction,
            "best_quote_maker_volume_allow_loss_reduce_only": True,
            "best_quote_maker_volume_net_loss_reduce_enabled": False,
            "best_quote_maker_volume_active_pair_reduce_enabled": False,
            "loss_inventory_no_cross_small_entry_notional": 200.0,
            "best_quote_maker_volume_quote_offset_ticks": 0,
            "best_quote_maker_volume_dynamic_control_trend_inventory_guard_reduce_extra_ticks": 0,
            "best_quote_maker_volume_dynamic_control_trend_loss_reduce_guard_reduce_extra_ticks": 0,
            "best_quote_maker_volume_dynamic_control_trend_loss_reduce_guard_recent_loss_extra_ticks": 0,
        }
    return {key: value for key, value in targets.items() if control.get(key) != value}


def should_bypass_arx_side_unwind_recovery_gate(
    *,
    symbol: str,
    updates: Mapping[str, Any],
) -> bool:
    """A live ARX direction flip must not wait behind a stale submit report."""
    return (
        symbol.upper().strip() == "ARXUSDT"
        and str(updates.get("best_quote_maker_volume_directional_net_guard") or "").lower()
        in {"net_long", "net_short"}
    )


def arx_severe_pace_capacity_updates(
    *,
    control: dict[str, Any],
    target_pace_behind: bool,
    pace_ratio: float,
    near_cap: bool,
    volatility_entry_pause_active: bool,
    frozen_total_notional: float,
    frozen_long_notional: float = 0.0,
    frozen_short_notional: float = 0.0,
    actual_long_notional: float = 0.0,
    actual_short_notional: float = 0.0,
) -> dict[str, Any]:
    """Temporarily recover ARX flow without exceeding the per-side hard cap."""
    del actual_long_notional, actual_short_notional
    target_max_notional = 2000.0
    side_cap_updates = (
        arx_frozen_inventory_headroom_updates(
            control=control,
            frozen_long_notional=frozen_long_notional,
            frozen_short_notional=frozen_short_notional,
        )
        or arx_single_side_cap_updates(control)
    )
    capacity_already_raised = not side_cap_updates and all(
        _safe_float(control.get(key)) >= 480.0
        for key in (
            "best_quote_maker_volume_active_pair_reduce_order_notional",
            "best_quote_maker_volume_active_pair_reduce_max_notional_per_side",
        )
    )
    if (
        not bool(target_pace_behind)
        or (bool(near_cap) and capacity_already_raised)
        or bool(volatility_entry_pause_active)
        or float(frozen_total_notional) <= 0.0
    ):
        return {}
    targets = {
        "best_quote_maker_volume_inventory_soft_ratio": 0.9,
        "best_quote_maker_volume_min_cycle_budget_notional": 960.0,
        "best_quote_maker_volume_cycle_budget_notional": 1600.0,
        "best_quote_maker_volume_active_pair_reduce_order_notional": 480.0,
        "best_quote_maker_volume_active_pair_reduce_max_notional_per_side": 480.0,
        "max_total_notional": target_max_notional * 2.0,
    }
    updates = dict(side_cap_updates)
    updates.update({
        key: value
        for key, value in targets.items()
        if _safe_float(control.get(key)) < value
    })
    if bool(control.get("best_quote_maker_volume_inventory_bias_enabled")):
        updates["best_quote_maker_volume_inventory_bias_enabled"] = False
    if (
        bool(control.get("best_quote_maker_volume_same_side_entry_price_guard_enabled"))
        and not bool(
            control.get("best_quote_maker_volume_same_side_entry_price_guard_report_only")
        )
    ):
        updates["best_quote_maker_volume_same_side_entry_price_guard_report_only"] = True
    if _safe_int(control.get("best_quote_maker_volume_quote_offset_ticks")) > 0:
        updates["best_quote_maker_volume_quote_offset_ticks"] = 0
    if bool(control.get("sticky_entry_preserve_less_aggressive")):
        updates["sticky_entry_preserve_less_aggressive"] = False
    return updates


def arx_balanced_fast_sla_capacity_updates(
    *,
    control: dict[str, Any],
    assessment: dict[str, Any],
    target_pace_behind: bool,
    no_fill_seconds: float,
    fast_sla_seconds: float,
    high_recovery_wear: bool,
) -> dict[str, Any]:
    """Restore two-sided ARX maker flow before using loss-reduce on balanced stock."""
    current_long = max(_safe_float(assessment.get("current_long_notional")), 0.0)
    current_short = max(_safe_float(assessment.get("current_short_notional")), 0.0)
    actual_long = max(_safe_float(assessment.get("actual_long_notional")), current_long)
    actual_short = max(_safe_float(assessment.get("actual_short_notional")), current_short)
    # Frozen inventory can make the exchange side larger than the runner's
    # ledger side.  A fixed 3,200 cap then turns a balanced recovery into a
    # one-sided reduce-only book.  Give exactly one 600U maker batch of room,
    # bounded at 5,000U, so the recovery guard can restore two-sided flow.
    actual_side_max = max(actual_long, actual_short)
    target_max = min(5000.0, max(3200.0, ceil((actual_side_max + 600.0) / 200.0) * 200.0))
    extended_for_actual_inventory = target_max > 3200.0
    if (
        not bool(target_pace_behind)
        or float(no_fill_seconds) < max(float(fast_sla_seconds), 0.0)
        or bool(high_recovery_wear)
        or bool(assessment.get("volatility_entry_pause_active"))
        or min(current_long, current_short) < 1300.0
        or abs(current_long - current_short) > 650.0
        or _safe_float(control.get("max_position_notional")) >= target_max
        or _safe_float(control.get("max_short_position_notional")) >= target_max
    ):
        return {}
    targets = {
        "pause_buy_position_notional": target_max if extended_for_actual_inventory else 2880.0,
        "pause_short_position_notional": target_max if extended_for_actual_inventory else 2880.0,
        "max_position_notional": target_max,
        "max_short_position_notional": target_max,
        "maker_max_long_notional": target_max,
        "maker_max_short_notional": target_max,
        "best_quote_maker_volume_max_long_notional": target_max,
        "best_quote_maker_volume_max_short_notional": target_max,
        "best_quote_maker_volume_inventory_soft_ratio": 1.0 if extended_for_actual_inventory else 0.9,
        "best_quote_maker_volume_min_cycle_budget_notional": 1200.0,
        "best_quote_maker_volume_cycle_budget_notional": 2000.0,
        "best_quote_maker_volume_active_pair_reduce_order_notional": 600.0,
        "best_quote_maker_volume_active_pair_reduce_max_notional_per_side": 600.0,
        "max_total_notional": target_max * 2.0 if extended_for_actual_inventory else 6000.0,
        "best_quote_maker_volume_allow_loss_reduce_only": False,
        "best_quote_maker_volume_net_loss_reduce_enabled": False,
        "best_quote_maker_volume_quote_offset_ticks": 0,
    }
    return {key: value for key, value in targets.items() if control.get(key) != value}


def arx_soft_recovery_extension_updates(
    *,
    control: dict[str, Any],
    assessment: dict[str, Any],
    volume_summary: dict[str, Any],
) -> dict[str, Any]:
    """Make a timed ARX soft-recovery extension materially restore maker flow."""
    wear_5m = _safe_float(volume_summary.get("trailing_5m_realized_wear_per_10k"))
    wear_15m = _safe_float(volume_summary.get("trailing_15m_realized_wear_per_10k"))
    if (
        str(assessment.get("symbol") or "").upper() != "ARXUSDT"
        or not bool(assessment.get("target_pace_behind"))
        or not bool(assessment.get("effective_inventory_soft_pressure"))
        or bool(assessment.get("volatility_entry_pause_active"))
        or _safe_float(volume_summary.get("trailing_15m_gross_notional")) < 100.0
        or max(wear_5m, wear_15m) > 3.0
    ):
        return {}
    targets = {
        "best_quote_maker_volume_cycle_budget_notional": 1600.0,
        # Active-pair reduction deliberately suppresses ordinary entries.  It
        # is therefore the wrong recovery path when ARX needs sustained maker
        # flow rather than a one-sided inventory exit.
        "best_quote_maker_volume_active_pair_reduce_enabled": False,
        "best_quote_maker_volume_active_pair_reduce_order_notional": 600.0,
        "best_quote_maker_volume_active_pair_reduce_max_notional_per_side": 600.0,
        "best_quote_maker_volume_quote_offset_ticks": 0,
    }
    return {
        key: value
        for key, value in targets.items()
        if (
            key == "best_quote_maker_volume_active_pair_reduce_enabled"
            and bool(control.get(key)) != bool(value)
        )
        or _safe_float(control.get(key)) < value
        or (
            key == "best_quote_maker_volume_quote_offset_ticks"
            and _safe_int(control.get(key)) > 0
        )
    }


def _runner_is_active(symbol: str) -> bool:
    service = f"grid-loop@{symbol}.service"
    return (
        subprocess.run(
            ["systemctl", "is-active", service],
            capture_output=True,
            text=True,
        ).stdout.strip()
        == "active"
    )


def _clear_inactive_observation(item: dict[str, Any]) -> None:
    for key in ("first_inactive_at", "last_inactive_at", "inactive_gate_reasons"):
        item.pop(key, None)


def _inactive_restart_gate(
    *,
    control: dict[str, Any],
    plan: dict[str, Any],
    submit: dict[str, Any],
    now: datetime,
    max_snapshot_age_seconds: float,
) -> dict[str, Any]:
    reasons: list[str] = []
    if not control:
        reasons.append("missing_control")

    run_start = _parse_time(control.get("run_start_time"))
    run_end = _parse_time(control.get("run_end_time"))
    if run_start is not None and now < run_start:
        reasons.append("before_run_window")
    if run_end is not None and now >= run_end:
        reasons.append("after_run_window")

    if not _plan_time_is_fresh(plan, now=now, max_age_seconds=max_snapshot_age_seconds):
        reasons.append("latest_plan_stale")
    if not _plan_time_is_fresh(submit, now=now, max_age_seconds=max_snapshot_age_seconds):
        reasons.append("latest_submit_stale")

    stop_reason = str(plan.get("stop_reason") or submit.get("stop_reason") or "").strip()
    runtime_status = str(plan.get("runtime_status") or submit.get("runtime_status") or "").strip().lower()
    if stop_reason:
        reasons.append("explicit_stop_reason")
    if runtime_status in {"stopped", "complete", "completed", "after_end_window", "target_reached"}:
        reasons.append(f"runtime_status_{runtime_status}")
    if submit.get("error"):
        reasons.append("latest_submit_error")

    validation = submit.get("validation") if isinstance(submit.get("validation"), dict) else {}
    if validation.get("ok") is False or list(validation.get("errors") or []):
        reasons.append("latest_validation_failed")

    observed = (
        submit.get("observed_strategy_open_order_state", {})
        if isinstance(submit.get("observed_strategy_open_order_state"), dict)
        else {}
    )
    plan_summary = submit.get("plan_summary", {}) if isinstance(submit.get("plan_summary"), dict) else {}
    planned_open = max(
        _safe_int(plan.get("open_order_count")),
        _safe_int(plan.get("total_open_order_count")),
        _safe_int(plan_summary.get("open_order_count")),
    )
    observed_open = _safe_int(observed.get("active_order_count"))
    open_order_drift = abs(observed_open - planned_open)
    if open_order_drift > 10:
        reasons.append("open_order_reconcile_drift_gt_10")

    return {
        "ok": not reasons,
        "reasons": reasons,
        "run_start_time": run_start.isoformat() if run_start else None,
        "run_end_time": run_end.isoformat() if run_end else None,
        "planned_open_order_count": planned_open,
        "observed_open_order_count": observed_open,
        "open_order_drift": open_order_drift,
    }


def _net_guard_stop_recovery(
    *,
    control: dict[str, Any],
    plan: dict[str, Any],
    exchange: dict[str, Any],
    recovery_baseline_notional: float = 0.0,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Permit one bounded restart from a verified directional max-net stop.

    A max-net stop leaves the runner inactive and its last plan stale, so it
    cannot pass the generic restart gate. This exception is deliberately
    narrow: exchange positions must match the stop snapshot and no strategy
    order may remain. A net-long restart suppresses the BUY/SHORT reduce leg;
    a net-short restart must explicitly leave that leg enabled so it can reduce
    the short imbalance.
    """
    best_quote = plan.get("best_quote_maker_volume")
    reduce_freeze = (
        best_quote.get("reduce_freeze") if isinstance(best_quote, dict) else {}
    )
    snapshot_long = _safe_float(reduce_freeze.get("actual_long_notional"))
    snapshot_short = _safe_float(reduce_freeze.get("actual_short_notional"))
    exchange_long = _safe_float(exchange.get("long_notional"))
    exchange_short = _safe_float(exchange.get("short_notional"))
    configured_max_net = _safe_float(control.get("max_actual_net_notional"))
    max_net = _safe_float(recovery_baseline_notional) or configured_max_net
    per_order = max(
        _safe_float(control.get("per_order_notional")),
        _safe_float(control.get("maker_order_notional")),
        _safe_float(control.get("best_quote_maker_volume_min_cycle_budget_notional")),
        20.0,
    )
    position_drift = abs(exchange_long - snapshot_long) + abs(exchange_short - snapshot_short)
    drift_threshold = max(
        _safe_float(plan.get("hedge_bq_position_drift_tolerance_notional")),
        per_order * 2.0,
        100.0,
    )
    net_notional = exchange_long - exchange_short
    net_abs = abs(net_notional)
    details = {
        "ok": False,
        "exchange_open_order_count": _safe_int(exchange.get("open_order_count"), -1),
        "exchange_long_notional": exchange_long,
        "exchange_short_notional": exchange_short,
        "snapshot_long_notional": snapshot_long,
        "snapshot_short_notional": snapshot_short,
        "position_drift_notional": position_drift,
        "position_drift_threshold_notional": drift_threshold,
        "configured_max_actual_net_notional": configured_max_net,
        "recovery_baseline_notional": _safe_float(recovery_baseline_notional),
        "effective_guard_baseline_notional": max_net,
        "net_notional": net_notional,
        "net_notional_abs": net_abs,
    }
    if max_net <= 0:
        details["reason"] = "max_actual_net_notional_not_configured"
        return {}, details
    if _safe_int(exchange.get("open_order_count"), -1) != 0:
        details["reason"] = "exchange_open_orders_present"
        return {}, details
    if position_drift > drift_threshold:
        details["reason"] = "exchange_position_drift_too_large"
        return {}, details
    if net_abs < max_net:
        details["reason"] = "exchange_inside_net_guard"
        return {}, details

    direction = "net_long" if net_notional >= 0 else "net_short"
    temporary_limit = max(configured_max_net, net_abs + per_order)
    details.update(
        {
            "ok": True,
            "temporary_max_actual_net_notional": temporary_limit,
            "direction": direction,
        }
    )
    return {
        "max_actual_net_notional": temporary_limit,
        "best_quote_maker_volume_suppress_short_reduce_enabled": False,
        "best_quote_maker_volume_directional_net_guard": direction,
        "best_quote_maker_volume_allow_loss_reduce_only": True,
        "best_quote_maker_volume_net_loss_reduce_enabled": False,
    }, details


def recover_inactive_runner(
    *,
    symbol: str,
    output_dir: Path,
    state: dict[str, Any],
    now: datetime,
    trigger_seconds: float,
    restart_cooldown_seconds: float,
    max_snapshot_age_seconds: float,
    runner_wrapper: str,
    dry_run: bool,
    restart_runner: RestartRunner | None = None,
    exchange_snapshot_fetcher: CorruptStateExchangeFetcher | None = None,
) -> dict[str, Any]:
    normalized_symbol = symbol.upper().strip()
    output_dir = Path(output_dir)
    item = _symbol_state(state, normalized_symbol)
    first_inactive = _parse_time(item.get("first_inactive_at")) or now
    elapsed = max((now - first_inactive).total_seconds(), 0.0)
    item.update(
        {
            "first_inactive_at": first_inactive.isoformat(),
            "last_inactive_at": now.isoformat(),
        }
    )
    control_path = _control_path(output_dir, normalized_symbol)
    control = _read_json(control_path)
    arx_policy_updates = _arx_independent_freeze_policy_updates(
        symbol=normalized_symbol,
        control=control,
    )
    if arx_policy_updates:
        restart = restart_runner or (
            lambda item_symbol: _default_restart_runner(
                item_symbol, runner_wrapper=runner_wrapper
            )
        )
        if dry_run:
            changed = list(arx_policy_updates)
            backup_path = None
            action = "dry_run_enforce_arx_maker_only_policy_before_inactive_restart"
        else:
            try:
                changed, backup_path = _apply_control_update(
                    symbol=normalized_symbol,
                    control_path=control_path,
                    control=control,
                    updates=arx_policy_updates,
                    now=now,
                    dry_run=False,
                    restart_runner=restart,
                )
                action = "enforce_arx_maker_only_policy_before_inactive_restart"
            except subprocess.CalledProcessError as exc:
                changed, backup_path = list(arx_policy_updates), None
                action = "enforce_arx_maker_only_policy_before_inactive_restart_failed"
                return {
                    "symbol": normalized_symbol,
                    "action": action,
                    "changed_keys": changed,
                    "backup_path": backup_path,
                    "dry_run": dry_run,
                    "restart_failed": str(exc),
                    "inactive_seconds": elapsed,
                }
        item.update(
            {
                "status": "runner_restart_requested",
                "last_recovery_action_at": now.isoformat(),
                "last_recovery_action": action,
            }
        )
        return {
            "symbol": normalized_symbol,
            "action": action,
            "changed_keys": changed,
            "backup_path": backup_path,
            "dry_run": dry_run,
            "restart_failed": None,
            "inactive_seconds": elapsed,
        }
    plan = _read_json(_plan_path(output_dir, normalized_symbol))
    submit = _read_json(_submit_path(output_dir, normalized_symbol))
    gate = _inactive_restart_gate(
        control=control,
        plan=plan,
        submit=submit,
        now=now,
        max_snapshot_age_seconds=max_snapshot_age_seconds,
    )
    item["inactive_gate_reasons"] = list(gate["reasons"])
    action = "wait_runner_inactive_confirmation"
    restart_failed: str | None = None

    last_restart_at = _parse_time(item.get("last_inactive_restart_at"))
    restart_cooldown_active = (
        last_restart_at is not None
        and (now - last_restart_at).total_seconds() < max(float(restart_cooldown_seconds), 0.0)
    )
    net_guard_recovery_active = _safe_float(item.get("net_guard_recovery_baseline_notional")) > 0
    stop_reason = str(plan.get("stop_reason") or submit.get("stop_reason") or "").strip()
    stop_reason_source = "plan_or_submit" if stop_reason else ""
    runtime_stop_event = _latest_runtime_stop_event(output_dir, normalized_symbol)
    if not stop_reason:
        stop_reason = str(runtime_stop_event.get("stop_reason") or "").strip()
        stop_reason_source = "runner_event" if stop_reason else ""
    runtime_stop_at = _parse_time(runtime_stop_event.get("ts"))
    newer_runtime_guard_stop = (
        stop_reason == "max_actual_net_notional_hit"
        and runtime_stop_at is not None
        and (last_restart_at is None or runtime_stop_at >= last_restart_at)
    )
    net_guard_live_gate: dict[str, Any] | None = None
    if (
        stop_reason == "max_actual_net_notional_hit"
        and elapsed >= max(float(trigger_seconds), 0.0)
        and (
            not restart_cooldown_active
            or not net_guard_recovery_active
            or newer_runtime_guard_stop
        )
    ):
        try:
            exchange = (exchange_snapshot_fetcher or _fetch_corrupt_state_exchange_snapshot)(
                normalized_symbol
            )
            net_guard_updates, net_guard_live_gate = _net_guard_stop_recovery(
                control=control,
                plan=plan,
                exchange=exchange,
                recovery_baseline_notional=_safe_float(
                    item.get("net_guard_recovery_baseline_notional")
                ),
            )
        except Exception as exc:
            net_guard_updates = {}
            net_guard_live_gate = {"ok": False, "error": str(exc)[:240]}
        if net_guard_updates:
            restart = restart_runner or (
                lambda item_symbol: _default_restart_runner(
                    item_symbol, runner_wrapper=runner_wrapper
                )
            )
            _remember_recovery_controls(item, control, tuple(net_guard_updates))
            _remember_recovery_updates(item, net_guard_updates)
            original_max_actual_net_notional = _safe_float(
                (item.get("guard_original_controls") or {}).get("max_actual_net_notional")
            )
            if dry_run:
                changed = [
                    key for key, value in net_guard_updates.items() if control.get(key) != value
                ]
                backup_path = None
                direction = str(net_guard_live_gate.get("direction") or "net")
                action = f"dry_run_recover_{direction}_guard_stop"
            else:
                try:
                    changed, backup_path = _apply_control_update(
                        symbol=normalized_symbol,
                        control_path=control_path,
                        control=control,
                        updates=net_guard_updates,
                        now=now,
                        dry_run=False,
                        restart_runner=restart,
                    )
                    if not changed:
                        restart(normalized_symbol)
                    direction = str(net_guard_live_gate.get("direction") or "net")
                    action = f"recover_{direction}_guard_stop"
                    item["last_inactive_restart_at"] = now.isoformat()
                    item["status"] = "net_guard_recovery_active"
                except subprocess.CalledProcessError as exc:
                    changed, backup_path = list(net_guard_updates), None
                    direction = str(net_guard_live_gate.get("direction") or "net")
                    action = f"recover_{direction}_guard_stop_restart_failed"
                    restart_failed = str(exc)
            item.update(
                {
                    "net_guard_recovery_baseline_notional": _safe_float(
                        original_max_actual_net_notional
                        if original_max_actual_net_notional > 0
                        else control.get("max_actual_net_notional")
                    ),
                    "net_guard_recovery_direction": str(
                        net_guard_live_gate.get("direction") or "net"
                    ),
                    "recovery_started_at": now.isoformat(),
                    "recovery_owned": True,
                    "last_recovery_action_at": now.isoformat(),
                    "last_recovery_action": action,
                }
            )
            return {
                "symbol": normalized_symbol,
                "action": action,
                "changed_keys": changed,
                "backup_path": backup_path,
                "dry_run": dry_run,
                "restart_failed": restart_failed,
                "inactive_seconds": elapsed,
                "inactive_restart_gate": gate,
                "net_guard_live_gate": net_guard_live_gate,
                "net_guard_stop_reason_source": stop_reason_source,
                "net_guard_stop_event_at": runtime_stop_at.isoformat() if runtime_stop_at else None,
            }
        if net_guard_live_gate is not None and not bool(net_guard_live_gate.get("ok")):
            stale_only = set(gate["reasons"]).issubset(
                {"latest_plan_stale", "latest_submit_stale"}
            )
            # A stopped runner necessarily has stale snapshots.  If a prior
            # max-net stop is now back inside its limit and the exchange still
            # matches that snapshot with no strategy orders, it is safe to
            # resume the normal inactive-runner path.  Treating this as a
            # permanent net-guard failure creates an unrecoverable deadlock.
            if stale_only and net_guard_live_gate.get("reason") == "exchange_inside_net_guard":
                gate = dict(gate)
                gate["ok"] = True
                gate["reasons"] = []
                net_guard_live_gate = dict(net_guard_live_gate)
                net_guard_live_gate["stale_restart_safe"] = True
            else:
                item["status"] = "net_guard_live_safety_hold"
                return {
                    "symbol": normalized_symbol,
                    "action": "skip_net_guard_live_safety_gate",
                    "changed_keys": [],
                    "backup_path": None,
                    "dry_run": dry_run,
                    "restart_failed": None,
                    "inactive_seconds": elapsed,
                    "inactive_restart_gate": gate,
                    "net_guard_live_gate": net_guard_live_gate,
                    "net_guard_stop_reason_source": stop_reason_source,
                    "net_guard_stop_event_at": runtime_stop_at.isoformat() if runtime_stop_at else None,
                }

    original_controls = item.get("guard_original_controls")
    invalid_pause_updates: dict[str, Any] = {}
    if bool(item.get("recovery_owned")) and isinstance(original_controls, dict):
        for key in ("pause_buy_position_notional", "pause_short_position_notional"):
            baseline = _safe_float(original_controls.get(key))
            if _safe_float(control.get(key)) <= 0 and baseline > 0:
                invalid_pause_updates[key] = baseline
    stale_only = set(gate["reasons"]).issubset(
        {"latest_plan_stale", "latest_submit_stale"}
    )
    invalid_control_live_gate: dict[str, Any] | None = None
    if invalid_pause_updates and stale_only:
        try:
            exchange = (exchange_snapshot_fetcher or _fetch_corrupt_state_exchange_snapshot)(
                normalized_symbol
            )
        except Exception as exc:
            exchange = {}
            invalid_control_live_gate = {"ok": False, "error": str(exc)[:240]}
        if invalid_control_live_gate is None:
            best_quote = plan.get("best_quote_maker_volume")
            reduce_freeze = (
                best_quote.get("reduce_freeze") if isinstance(best_quote, dict) else {}
            )
            expected_long = _safe_float(reduce_freeze.get("actual_long_notional"))
            expected_short = _safe_float(reduce_freeze.get("actual_short_notional"))
            position_drift = abs(
                _safe_float(exchange.get("long_notional")) - expected_long
            ) + abs(_safe_float(exchange.get("short_notional")) - expected_short)
            drift_threshold = max(
                _safe_float(
                    (item.get("last_assessment") or {}).get(
                        "ledger_position_drift_threshold_notional"
                    )
                ),
                100.0,
            )
            invalid_control_live_gate = {
                "ok": _safe_int(exchange.get("open_order_count"), -1) == 0
                and position_drift <= drift_threshold,
                "open_order_count": _safe_int(exchange.get("open_order_count"), -1),
                "position_drift_notional": position_drift,
                "position_drift_threshold_notional": drift_threshold,
            }
    if invalid_pause_updates and invalid_control_live_gate and invalid_control_live_gate["ok"]:
        restart = restart_runner or (
            lambda item_symbol: _default_restart_runner(
                item_symbol, runner_wrapper=runner_wrapper
            )
        )
        if dry_run:
            action = "dry_run_repair_invalid_recovery_pause_and_restart"
            changed = list(invalid_pause_updates)
            backup_path = None
        else:
            try:
                changed, backup_path = _apply_control_update(
                    symbol=normalized_symbol,
                    control_path=control_path,
                    control=control,
                    updates=invalid_pause_updates,
                    now=now,
                    dry_run=False,
                    restart_runner=restart,
                )
                action = "repair_invalid_recovery_pause_and_restart"
                item["last_inactive_restart_at"] = now.isoformat()
                item["status"] = "runner_restart_requested"
            except subprocess.CalledProcessError as exc:
                changed, backup_path = list(invalid_pause_updates), None
                action = "repair_invalid_recovery_pause_restart_failed"
                restart_failed = str(exc)
        return {
            "symbol": normalized_symbol,
            "action": action,
            "changed_keys": changed,
            "backup_path": backup_path,
            "dry_run": dry_run,
            "restart_failed": restart_failed,
            "inactive_seconds": elapsed,
            "inactive_restart_gate": gate,
            "invalid_control_live_gate": invalid_control_live_gate,
        }

    if not gate["ok"]:
        action = "skip_runner_inactive_safety_gate"
    elif restart_cooldown_active:
        action = "runner_inactive_restart_cooldown"
    elif elapsed >= max(float(trigger_seconds), 0.0):
        if dry_run:
            action = "dry_run_restart_runner_inactive"
        else:
            restart = restart_runner or (
                lambda item_symbol: _default_restart_runner(item_symbol, runner_wrapper=runner_wrapper)
            )
            try:
                restart(normalized_symbol)
                action = "restart_runner_inactive"
                item["last_inactive_restart_at"] = now.isoformat()
                item["status"] = "runner_restart_requested"
            except subprocess.CalledProcessError as exc:
                action = "restart_runner_inactive_failed"
                restart_failed = str(exc)

    return {
        "symbol": normalized_symbol,
        "action": action,
        "changed_keys": [],
        "backup_path": None,
        "dry_run": dry_run,
        "restart_failed": restart_failed,
        "inactive_seconds": elapsed,
        "inactive_restart_gate": gate,
        "net_guard_live_gate": net_guard_live_gate,
        "net_guard_stop_reason_source": stop_reason_source,
        "net_guard_stop_event_at": runtime_stop_at.isoformat() if runtime_stop_at else None,
    }


def _set_post_restore_cooldown(
    item: dict[str, Any],
    *,
    now: datetime,
    cooldown_seconds: float,
) -> None:
    seconds = max(float(cooldown_seconds), 0.0)
    item.update({"last_normal_at": now.isoformat()})
    item.update({"status": "normal"})
    if seconds > 0:
        cooldown_until = (now + timedelta(seconds=seconds)).isoformat()
        item["cooldown_until"] = cooldown_until
        item["post_restore_budget_cooldown_until"] = cooldown_until
    else:
        item.pop("cooldown_until", None)
        item.pop("post_restore_budget_cooldown_until", None)


def _normalize_symbols(items: list[str]) -> list[str]:
    symbols: list[str] = []
    for item in items:
        for part in str(item).split(","):
            symbol = part.strip().upper()
            if symbol and symbol not in symbols:
                symbols.append(symbol)
    return symbols


def _parse_symbol_notionals(items: list[str]) -> dict[str, float]:
    values: dict[str, float] = {}
    for item in items:
        for part in str(item).split(","):
            symbol, separator, raw_value = part.strip().partition("=")
            normalized_symbol = symbol.strip().upper()
            if not separator or not normalized_symbol:
                continue
            value = _safe_float(raw_value, -1.0)
            if value > 0:
                values[normalized_symbol] = value
    return values


def _parse_symbol_pause_baselines(items: list[str]) -> dict[str, tuple[float, float]]:
    values: dict[str, tuple[float, float]] = {}
    for item in items:
        for token in str(item).split(","):
            raw_symbol, separator, raw_pair = token.strip().partition("=")
            normalized_symbol = raw_symbol.upper().strip()
            if not separator or not normalized_symbol:
                continue
            raw_long, pair_separator, raw_short = raw_pair.partition(":")
            long_value = _safe_float(raw_long, -1.0)
            short_value = _safe_float(raw_short if pair_separator else raw_long, -1.0)
            if long_value > 0 and short_value > 0:
                values[normalized_symbol] = (long_value, short_value)
    return values


def check_symbol(
    *,
    symbol: str,
    output_dir: Path,
    state: dict[str, Any],
    now: datetime,
    window_seconds: float,
    min_volume_notional: float,
    trigger_seconds: float,
    daily_target_notional: float | None = None,
    target_pace_fraction: float = 0.9,
    target_pace_max_multiplier: float = 2.0,
    target_completion_buffer_seconds: float = 0.0,
    recover_min_volume_notional: float | None = None,
    near_cap_ratio: float = 0.95,
    recover_cap_ratio: float = 0.96,
    far_ticks: int = 8,
    plan_stale_seconds: float = 300.0,
    allow_inventory_cost_gate_disable: bool = True,
    max_recovery_seconds: float = 300.0,
    cooldown_seconds: float = 600.0,
    recovery_min_hold_seconds: float = 120.0,
    recovery_reapply_min_seconds: float = 180.0,
    post_restore_cooldown_seconds: float = 300.0,
    max_soft_recovery_extensions: int = 1,
    inventory_bias_relief_notional_margin: float = 24.0,
    volume_recovery_cycle_budget_increment: float = 12.0,
    cycle_budget_floor_notional: float = 0.0,
    pause_baseline_long_notional: float = 0.0,
    pause_baseline_short_notional: float = 0.0,
    loss_reduce_quote_offset_extra_ticks: int = 0,
    require_soft_pressure_for_allow_loss: bool = False,
    trade_rows: list[dict[str, Any]] | None = None,
    volume_source: str = "local_audit",
    dry_run: bool = False,
    runner_wrapper: str = "/usr/local/bin/grid-saved-runner",
    restart_runner: RestartRunner | None = None,
) -> dict[str, Any]:
    normalized_symbol = symbol.upper().strip()
    output_dir = Path(output_dir)
    control_path = _control_path(output_dir, normalized_symbol)
    control = _read_json(control_path)
    plan = _read_json(_plan_path(output_dir, normalized_symbol))
    submit = _read_json(_submit_path(output_dir, normalized_symbol))
    rows = trade_rows if trade_rows is not None else _iter_jsonl(_trade_path(output_dir, normalized_symbol))
    volume_summary = summarize_recent_volume(
        rows=rows,
        now=now,
        window_seconds=window_seconds,
    )
    volume_summary["source"] = volume_source
    effective_min_volume_notional = apply_daily_target_pace_floor(
        volume_summary=volume_summary,
        rows=rows,
        now=now,
        window_seconds=window_seconds,
        min_volume_notional=min_volume_notional,
        daily_target_notional=daily_target_notional,
        target_pace_fraction=target_pace_fraction,
        target_pace_max_multiplier=target_pace_max_multiplier,
        target_completion_buffer_seconds=target_completion_buffer_seconds,
    )
    assessment = assess_symbol(
        symbol=normalized_symbol,
        control=control,
        plan=plan,
        submit=submit,
        volume_summary=volume_summary,
        now=now,
        min_volume_notional=effective_min_volume_notional,
        near_cap_ratio=near_cap_ratio,
        far_ticks=far_ticks,
        plan_stale_seconds=plan_stale_seconds,
    )
    actual_inventory_below_soft = _actual_inventory_below_soft_limits(
        assessment,
        pause_baseline_long_notional=pause_baseline_long_notional,
        pause_baseline_short_notional=pause_baseline_short_notional,
    )
    effective_inventory_soft_pressure = bool(assessment.get("inventory_soft_pressure")) and not (
        actual_inventory_below_soft
    )
    assessment["actual_inventory_below_soft"] = actual_inventory_below_soft
    assessment["effective_inventory_soft_pressure"] = effective_inventory_soft_pressure
    recovery_entry_reserve_notional = max(
        _safe_float(control.get("per_order_notional")),
        _safe_float(control.get("maker_order_notional")),
        _safe_float(control.get("best_quote_maker_volume_cycle_budget_notional")) * 0.5,
    )
    assessment["recovery_entry_reserve_notional"] = recovery_entry_reserve_notional
    max_actual_net_notional = _safe_float(control.get("max_actual_net_notional"))
    actual_long_for_net = assessment.get("actual_long_notional")
    actual_short_for_net = assessment.get("actual_short_notional")
    if actual_long_for_net is None or actual_short_for_net is None:
        actual_long_for_net = assessment.get("current_long_notional")
        actual_short_for_net = assessment.get("current_short_notional")
    actual_net_for_entry = abs(
        _safe_float(actual_long_for_net) - _safe_float(actual_short_for_net)
    )
    net_guard_entry_buffer_ok = (
        max_actual_net_notional <= 0
        or actual_net_for_entry + recovery_entry_reserve_notional
        <= max_actual_net_notional * 0.95
    )
    assessment["actual_net_notional_for_entry"] = actual_net_for_entry
    assessment["net_guard_entry_buffer_ok"] = net_guard_entry_buffer_ok
    ledger_drift_threshold = max(
        100.0,
        max(
            _safe_float(assessment.get("max_long_notional")),
            _safe_float(assessment.get("max_short_notional")),
        )
        * 0.2,
    )
    ledger_position_drift_notional = 0.0
    if (
        _safe_float(assessment.get("frozen_total_notional")) <= 0
        and assessment.get("actual_long_notional") is not None
        and assessment.get("actual_short_notional") is not None
    ):
        ledger_position_drift_notional = max(
            abs(
                _safe_float(assessment.get("current_long_notional"))
                - _safe_float(assessment.get("actual_long_notional"))
            ),
            abs(
                _safe_float(assessment.get("current_short_notional"))
                - _safe_float(assessment.get("actual_short_notional"))
            ),
        )
    assessment["ledger_position_drift_notional"] = ledger_position_drift_notional
    assessment["ledger_position_drift_threshold_notional"] = ledger_drift_threshold
    assessment["ledger_position_drift_blocked"] = (
        ledger_position_drift_notional > ledger_drift_threshold
    )
    static_cycle_budget_floor_notional = max(float(cycle_budget_floor_notional), 0.0)
    cycle_budget_floor_notional = apply_target_pace_cycle_budget_floor(
        volume_summary=volume_summary,
        rows=rows,
        now=now,
        control=control,
        assessment=assessment,
        static_floor_notional=cycle_budget_floor_notional,
        target_pace_fraction=target_pace_fraction,
        cycle_budget_increment=volume_recovery_cycle_budget_increment,
    )
    parameters = _resolve_recovery_parameters(
        control=control,
        static_cycle_budget_floor_notional=static_cycle_budget_floor_notional,
        effective_cycle_budget_floor_notional=cycle_budget_floor_notional,
        cycle_budget_increment_notional=volume_recovery_cycle_budget_increment,
    )
    # All downstream actions consume the same resolved floor. In particular an
    # explicit runner min-cycle value cannot be lowered by a guard CLI default.
    cycle_budget_floor_notional = parameters.effective_cycle_budget_floor_notional
    item = _symbol_state(state, normalized_symbol)
    original_controls = item.get("guard_original_controls")
    if isinstance(original_controls, dict):
        for key, baseline in (
            ("pause_buy_position_notional", pause_baseline_long_notional),
            ("pause_short_position_notional", pause_baseline_short_notional),
        ):
            if baseline > 0 and _safe_float(original_controls.get(key)) < baseline:
                original_controls[key] = baseline
        item["guard_original_controls"] = original_controls
    action = "none"
    backup_path: str | None = None
    changed: list[str] = []
    restart_failed: str | None = None
    restart = restart_runner or (lambda item_symbol: _default_restart_runner(item_symbol, runner_wrapper=runner_wrapper))
    net_guard_baseline = _safe_float(item.get("net_guard_recovery_baseline_notional"))
    if net_guard_baseline > 0:
        actual_long = assessment.get("actual_long_notional")
        actual_short = assessment.get("actual_short_notional")
        actual_net = (
            abs(_safe_float(actual_long) - _safe_float(actual_short))
            if actual_long is not None and actual_short is not None
            else None
        )
        expected_controls = item.get("guard_recovery_controls")
        expected_controls = (
            {
                key: value
                for key, value in expected_controls.items()
                if key in _RECOVERY_CONTROL_KEYS
            }
            if isinstance(expected_controls, dict)
            else {}
        )
        original_guard_limit = _safe_float(
            (item.get("guard_original_controls") or {}).get("max_actual_net_notional")
        )
        restore_baseline = (
            original_guard_limit if original_guard_limit > 0 else net_guard_baseline
        )
        restore_threshold = max(
            restore_baseline * 0.95 - recovery_entry_reserve_notional,
            0.0,
        )
        assessment["net_guard_restore_threshold_notional"] = restore_threshold
        if actual_net is not None and actual_net <= restore_threshold:
            updates = _restore_recovery_controls(
                item,
                control,
                cycle_budget_floor_notional,
            )
            changed, backup_path = _apply_control_update(
                symbol=normalized_symbol,
                control_path=control_path,
                control=control,
                updates=updates,
                now=now,
                dry_run=dry_run,
                restart_runner=restart,
            )
            action = (
                "dry_run_restore_net_guard_after_rebalance"
                if dry_run
                else "restore_net_guard_after_rebalance"
            )
            _set_post_restore_cooldown(
                item,
                now=now,
                cooldown_seconds=post_restore_cooldown_seconds,
            )
            for key in (
                "net_guard_recovery_baseline_notional",
                "net_guard_recovery_direction",
                "guard_original_controls",
                "guard_recovery_controls",
                "recovery_started_at",
                "recovery_owned",
            ):
                item.pop(key, None)
        else:
            drifted_updates = {
                key: value for key, value in expected_controls.items() if control.get(key) != value
            }
            if drifted_updates:
                changed, backup_path = _apply_control_update(
                    symbol=normalized_symbol,
                    control_path=control_path,
                    control=control,
                    updates=drifted_updates,
                    now=now,
                    dry_run=dry_run,
                    restart_runner=restart,
                )
                action = (
                    "dry_run_reapply_net_guard_recovery_controls"
                    if dry_run
                    else "reapply_net_guard_recovery_controls"
                )
            else:
                action = "hold_net_guard_rebalance"
                direction = str(item.get("net_guard_recovery_direction") or "").strip().lower()
                expected_role = {
                    "net_long": "best_quote_reduce_long",
                    "net_short": "best_quote_reduce_short",
                }.get(direction, "")
                planned_roles = {
                    str(order.get("role") or "").strip().lower()
                    for key in ("buy_orders", "sell_orders")
                    for order in list(plan.get(key) or [])
                    if isinstance(order, dict)
                }
                plan_generated_at = _parse_time(plan.get("generated_at"))
                plan_is_fresh = (
                    plan_generated_at is not None
                    and (now - plan_generated_at).total_seconds() <= max(float(plan_stale_seconds), 0.0)
                )
                last_directional_restart_at = _parse_time(
                    item.get("last_directional_net_guard_restart_at")
                )
                directional_restart_due = (
                    bool(expected_role)
                    and expected_role not in planned_roles
                    and plan_is_fresh
                    and (
                        last_directional_restart_at is None
                        or (now - last_directional_restart_at).total_seconds()
                        >= max(float(recovery_reapply_min_seconds), 60.0)
                    )
                )
                if directional_restart_due:
                    item["last_directional_net_guard_restart_at"] = now.isoformat()
                    if dry_run:
                        action = "dry_run_restart_net_guard_missing_directional_reduce"
                    else:
                        try:
                            restart(normalized_symbol)
                            action = "restart_net_guard_missing_directional_reduce"
                        except Exception as exc:  # pragma: no cover - exercised by host wrapper failures
                            restart_failed = str(exc)
                            action = "restart_net_guard_missing_directional_reduce_failed"
            item.update(
                {
                    "status": "net_guard_recovery_active",
                    "last_recovery_check_at": now.isoformat(),
                    "last_recovery_action": action,
                }
            )
        result = {
            "symbol": normalized_symbol,
            "action": action,
            "changed_keys": changed,
            "backup_path": backup_path,
            "dry_run": dry_run,
            "restart_failed": restart_failed,
            "volume_summary": volume_summary,
            "assessment": assessment,
            "net_guard_recovery_baseline_notional": net_guard_baseline,
            "net_guard_restore_baseline_notional": restore_baseline,
            "net_guard_actual_notional": actual_net,
        }
        _append_jsonl(
            output_dir / "bq_volume_recovery_guard_events.jsonl",
            {"ts": now.isoformat(), **result},
        )
        return result
    actual_long = assessment.get("actual_long_notional")
    actual_short = assessment.get("actual_short_notional")
    directional_long = _safe_float(
        actual_long if actual_long is not None else assessment.get("current_long_notional")
    )
    directional_short = _safe_float(
        actual_short if actual_short is not None else assessment.get("current_short_notional")
    )
    directional_buffer = max(parameters.per_order_notional, 1.0)
    if (
        bool(control.get("best_quote_maker_volume_suppress_short_reduce_enabled"))
        and directional_long <= directional_short + directional_buffer
    ):
        updates = {
            "best_quote_maker_volume_suppress_short_reduce_enabled": False,
            "best_quote_maker_volume_net_loss_reduce_enabled": False,
        }
        changed, backup_path = _apply_control_update(
            symbol=normalized_symbol,
            control_path=control_path,
            control=control,
            updates=updates,
            now=now,
            dry_run=dry_run,
            restart_runner=restart,
        )
        action = (
            "dry_run_clear_stale_short_reduce_suppression"
            if dry_run
            else "clear_stale_short_reduce_suppression"
        )
        item.update(
            {
                "status": "recovery_active",
                "last_recovery_action_at": now.isoformat(),
                "last_recovery_action": action,
            }
        )
        result = {
            "symbol": normalized_symbol,
            "action": action,
            "changed_keys": changed,
            "backup_path": backup_path,
            "dry_run": dry_run,
            "restart_failed": restart_failed,
            "volume_summary": volume_summary,
            "assessment": assessment,
        }
        _append_jsonl(
            output_dir / "bq_volume_recovery_guard_events.jsonl",
            {"ts": now.isoformat(), **result},
        )
        return result
    recover_floor = (
        float(recover_min_volume_notional)
        if recover_min_volume_notional is not None
        else max(float(effective_min_volume_notional), 0.0)
    )
    recover_floor = max(recover_floor, _safe_float(volume_summary.get("target_pace_floor_notional")))

    status_reasons = set(assessment.get("reasons") or [])
    stale_or_missing = bool({"missing_control", "latest_plan_stale", "latest_submit_stale"} & status_reasons)
    recovery_gate = _inactive_restart_gate(
        control=control,
        plan=plan,
        submit=submit,
        now=now,
        max_snapshot_age_seconds=plan_stale_seconds,
    )
    cooldown_until = _parse_time(item.get("cooldown_until"))
    post_restore_budget_cooldown_until = _parse_time(item.get("post_restore_budget_cooldown_until"))
    post_restore_budget_cooldown_active = (
        post_restore_budget_cooldown_until is not None
        and now < post_restore_budget_cooldown_until
    )
    effective_near_market_flow = (
        _safe_float(volume_summary.get("gross_notional")) > 0
        and _safe_float(volume_summary.get("gross_notional")) >= recover_floor
        and _safe_int(assessment.get("active_order_count")) > 0
        and _safe_int(assessment.get("near_market_order_count")) > 0
        and not bool(assessment.get("ineffective_orders"))
    )
    severe_one_sided_stall = (
        bool(assessment.get("one_sided_inventory_bias"))
        and _safe_float(volume_summary.get("gross_notional"))
        <= max(_safe_float(volume_summary.get("effective_min_volume_notional")), 0.0) * 0.5
    )
    recovery_original_controls = item.get("guard_original_controls")
    recovery_has_original_controls = isinstance(recovery_original_controls, dict) and bool(recovery_original_controls)
    recovery_expected_controls = item.get("guard_recovery_controls")
    recovery_expected_controls = (
        {
            key: value
            for key, value in recovery_expected_controls.items()
            if key in _RECOVERY_CONTROL_KEYS
        }
        if isinstance(recovery_expected_controls, dict)
        else {}
    )
    recovery_started_at = _parse_time(item.get("recovery_started_at"))
    recovery_timed_out = (
        recovery_started_at is not None
        and (now - recovery_started_at).total_seconds() >= max(float(max_recovery_seconds), 0.0)
    )
    recovery_timeout_required = recovery_timed_out and (
        recovery_has_original_controls or bool(control.get("best_quote_maker_volume_allow_loss_reduce_only"))
    )
    arx_recovery_baseline_net_limit_blocked = should_hold_arx_recovery_capacity_for_net_limit(
        symbol=normalized_symbol,
        actual_net_notional=_safe_float(assessment.get("actual_net_notional_for_entry")),
        original_max_actual_net_notional=_safe_float(
            recovery_original_controls.get("max_actual_net_notional")
            if isinstance(recovery_original_controls, dict)
            else 0.0
        ),
    )
    recovery_hold_satisfied = (
        recovery_started_at is None
        or (now - recovery_started_at).total_seconds() >= max(float(recovery_min_hold_seconds), 0.0)
    )
    last_recovery_action_at = _parse_time(item.get("last_recovery_action_at"))
    recovery_reapply_debounced = (
        last_recovery_action_at is not None
        and (now - last_recovery_action_at).total_seconds() < max(float(recovery_reapply_min_seconds), 0.0)
    )
    pause_baseline_updates: dict[str, Any] = {}
    if not bool(control.get("best_quote_maker_volume_allow_loss_reduce_only")):
        for key, baseline in (
            ("pause_buy_position_notional", pause_baseline_long_notional),
            ("pause_short_position_notional", pause_baseline_short_notional),
        ):
            if baseline > 0 and _safe_float(control.get(key)) < baseline:
                pause_baseline_updates[key] = baseline
    directional_loss_reduce_pause_updates: dict[str, Any] = {}
    if bool(control.get("best_quote_maker_volume_allow_loss_reduce_only")):
        current_long = max(_safe_float(assessment.get("current_long_notional")), 0.0)
        current_short = max(_safe_float(assessment.get("current_short_notional")), 0.0)
        direction_buffer = max(
            _safe_float(control.get("best_quote_maker_volume_min_cycle_budget_notional")),
            1.0,
        )
        if current_long > current_short + direction_buffer:
            if (
                pause_baseline_short_notional > 0
                and _safe_float(control.get("pause_short_position_notional"))
                < pause_baseline_short_notional
            ):
                directional_loss_reduce_pause_updates["pause_short_position_notional"] = (
                    pause_baseline_short_notional
                )
        elif current_short > current_long + direction_buffer:
            if (
                pause_baseline_long_notional > 0
                and _safe_float(control.get("pause_buy_position_notional"))
                < pause_baseline_long_notional
            ):
                directional_loss_reduce_pause_updates["pause_buy_position_notional"] = (
                    pause_baseline_long_notional
                )
    loss_reduce_cycle_budget_cap = parameters.loss_reduce_cycle_budget_cap_notional
    required_hourly_notional = max(_safe_float(volume_summary.get("required_hourly_notional")), 0.0)
    trailing_hourly_notional = max(_safe_float(volume_summary.get("trailing_60m_hourly_notional")), 0.0)
    trailing_15m_notional = max(
        _safe_float(volume_summary.get("trailing_15m_gross_notional")), 0.0
    )
    trailing_5m_notional = max(
        _safe_float(volume_summary.get("trailing_5m_gross_notional")), 0.0
    )
    trailing_3m_notional = max(
        _safe_float(volume_summary.get("gross_notional")), 0.0
    )
    pace_fraction = max(float(target_pace_fraction), 0.0)
    target_pace_ahead = is_target_pace_ahead(
        symbol=normalized_symbol,
        required_hourly_notional=required_hourly_notional,
        trailing_60m_notional=trailing_hourly_notional,
        trailing_15m_notional=trailing_15m_notional,
        trailing_5m_notional=trailing_5m_notional,
        trailing_3m_notional=trailing_3m_notional,
        target_pace_fraction=pace_fraction,
    )
    target_pace_behind = (
        required_hourly_notional > 0
        and not target_pace_ahead
    )
    arx_submit_failure_recovery_bypass = should_bypass_arx_submit_failure_recovery_gate(
        symbol=normalized_symbol,
        target_pace_behind=target_pace_behind,
        low_volume=bool(assessment.get("low_volume")),
        active_order_count=_safe_int(assessment.get("active_order_count")),
        volatility_entry_pause_active=bool(assessment.get("volatility_entry_pause_active")),
        ledger_position_drift_blocked=bool(assessment.get("ledger_position_drift_blocked")),
        recovery_gate_reasons=tuple(recovery_gate.get("reasons") or ()),
        validation_errors=tuple(
            (submit.get("validation") or {}).get("errors") or ()
            if isinstance(submit.get("validation"), dict)
            else ()
        ),
    )
    recovery_low_volume = bool(assessment.get("low_volume")) or target_pace_behind
    assessment["target_pace_behind"] = target_pace_behind
    trailing_realized_wear_per_10k = _safe_float(
        volume_summary.get("trailing_60m_realized_wear_per_10k")
    )
    trailing_5m_realized_wear_per_10k = _safe_float(
        volume_summary.get("trailing_5m_realized_wear_per_10k")
    )
    trailing_15m_realized_wear_per_10k = _safe_float(
        volume_summary.get("trailing_15m_realized_wear_per_10k")
    )
    high_recovery_wear = _is_high_recovery_wear(volume_summary)
    trailing_5m_gross_notional = _safe_float(
        volume_summary.get("trailing_5m_gross_notional")
    )
    confirmed_loss_reduce_wear = (
        trailing_5m_realized_wear_per_10k > 80.0
        or (
            trailing_5m_gross_notional >= 100.0
            and trailing_5m_realized_wear_per_10k > 3.0
        )
    )
    active_pair_reduce_deadlock = bool(
        assessment.get("active_pair_reduce_suppression_deadlock")
    ) or bool(assessment.get("active_pair_reduce_below_soft_deadlock"))
    assessment["high_recovery_wear"] = high_recovery_wear
    assessment["confirmed_loss_reduce_wear"] = confirmed_loss_reduce_wear
    two_sided_stale_no_fill = (
        _safe_float(volume_summary.get("gross_notional")) <= 0
        and _safe_int(assessment.get("planned_entry_buy_order_count")) > 0
        and _safe_int(assessment.get("planned_entry_sell_order_count")) > 0
        and _safe_int(assessment.get("stale_order_count")) > 0
        and _safe_int(assessment.get("missing_order_count")) > 0
        and _safe_int(assessment.get("active_order_count")) > 0
        and not bool(assessment.get("inventory_soft_pressure"))
    )
    assessment["two_sided_stale_no_fill"] = two_sided_stale_no_fill
    gross_in_recovery_window = _safe_float(volume_summary.get("gross_notional"))
    no_fill_since = _parse_time(item.get("no_fill_since"))
    if gross_in_recovery_window > 0:
        item.pop("no_fill_since", None)
        no_fill_since = None
    elif no_fill_since is None:
        observed_trade_times = [
            trade_at
            for row in rows
            if (trade_at := _trade_time(row)) is not None and trade_at <= now
        ]
        no_fill_since = max(observed_trade_times, default=now)
        item["no_fill_since"] = no_fill_since.isoformat()
    no_fill_seconds = max((now - no_fill_since).total_seconds(), 0.0) if no_fill_since else 0.0

    planned_entry_buy_count = _safe_int(
        assessment.get("planned_entry_buy_order_count")
    )
    planned_entry_sell_count = _safe_int(
        assessment.get("planned_entry_sell_order_count")
    )
    planned_entry_one_sided = (planned_entry_buy_count > 0) != (
        planned_entry_sell_count > 0
    )
    one_sided_entry_unprotected = (
        planned_entry_one_sided
        and not effective_inventory_soft_pressure
        and not bool(assessment.get("near_cap"))
        and not bool(assessment.get("volatility_entry_pause_active"))
        and not bool(assessment.get("volatility_inventory_gate_active"))
    )
    one_sided_entry_since = _parse_time(item.get("one_sided_entry_since"))
    if not one_sided_entry_unprotected:
        item.pop("one_sided_entry_since", None)
        one_sided_entry_since = None
    elif one_sided_entry_since is None:
        one_sided_entry_since = now
        item["one_sided_entry_since"] = now.isoformat()
    one_sided_entry_seconds = (
        max((now - one_sided_entry_since).total_seconds(), 0.0)
        if one_sided_entry_since
        else 0.0
    )
    fast_sla_seconds = min(max(float(trigger_seconds), 120.0), 300.0)
    severe_one_sided_stall = severe_one_sided_stall or (
        one_sided_entry_unprotected
        and one_sided_entry_seconds >= fast_sla_seconds
        and target_pace_behind
    )

    pace_ratio = (
        trailing_hourly_notional / required_hourly_notional
        if required_hourly_notional > 0
        else 1.0
    )
    arx_severe_volume_priority_recovery = is_arx_severe_volume_priority_recovery(
        symbol=normalized_symbol,
        target_pace_behind=target_pace_behind,
        pace_ratio=pace_ratio,
        planned_reduce_only_order_count=_safe_int(
            assessment.get("planned_reduce_only_order_count")
        ),
    )
    critical_arx_inventory_pace_override = (
        normalized_symbol == "ARXUSDT"
        and bool(volume_summary.get("completion_buffer_expired"))
        and _safe_float(volume_summary.get("remaining_target_notional")) > 0
        and pace_ratio < 0.65
        and effective_inventory_soft_pressure
        and _safe_int(assessment.get("planned_reduce_only_order_count")) > 0
        and _safe_int(assessment.get("near_market_reduce_only_order_count")) > 0
    )
    assessment["critical_arx_inventory_pace_override"] = (
        critical_arx_inventory_pace_override
    )
    critical_ousdt_reduce_budget_ramp = (
        normalized_symbol == "OUSDT"
        and target_pace_behind
        and pace_ratio < 0.25
        and not high_recovery_wear
        and not confirmed_loss_reduce_wear
        and bool(control.get("best_quote_maker_volume_allow_loss_reduce_only"))
        and _safe_int(assessment.get("near_market_reduce_only_order_count")) > 0
    )
    assessment["critical_ousdt_reduce_budget_ramp"] = (
        critical_ousdt_reduce_budget_ramp
    )
    low_pace_since = _parse_time(item.get("low_pace_since"))
    if pace_ratio >= 0.75:
        item.pop("low_pace_since", None)
        low_pace_since = None
    elif low_pace_since is None:
        low_pace_since = now
        item["low_pace_since"] = now.isoformat()
    low_pace_seconds = max((now - low_pace_since).total_seconds(), 0.0) if low_pace_since else 0.0
    last_sla_action_at = _parse_time(item.get("last_sla_action_at"))
    sla_action_debounced = bool(
        last_sla_action_at is not None
        and (now - last_sla_action_at).total_seconds() < 120.0
    )
    sla_recovery_due = (
        no_fill_seconds >= fast_sla_seconds
        or one_sided_entry_seconds >= fast_sla_seconds
        or (required_hourly_notional > 0 and pace_ratio < 0.75 and low_pace_seconds >= 120.0)
    ) and not sla_action_debounced
    assessment.update(
        {
            "no_fill_seconds": no_fill_seconds,
            "planned_entry_one_sided": planned_entry_one_sided,
            "one_sided_entry_unprotected": one_sided_entry_unprotected,
            "one_sided_entry_seconds": one_sided_entry_seconds,
            "fast_sla_seconds": fast_sla_seconds,
            "pace_ratio": pace_ratio,
            "arx_severe_volume_priority_recovery": arx_severe_volume_priority_recovery,
            "low_pace_seconds": low_pace_seconds,
            "sla_recovery_due": sla_recovery_due,
            "sla_action_debounced": sla_action_debounced,
        }
    )
    arx_severe_near_maker_entry = should_force_arx_severe_near_maker_entry(
        symbol=normalized_symbol,
        target_pace_behind=target_pace_behind,
        pace_ratio=pace_ratio,
        planned_entry_order_count=_safe_int(assessment.get("planned_entry_order_count")),
        effective_inventory_soft_pressure=effective_inventory_soft_pressure,
        allow_loss_reduce_only=bool(
            control.get("best_quote_maker_volume_allow_loss_reduce_only")
        ),
    )
    assessment["arx_severe_near_maker_entry"] = arx_severe_near_maker_entry
    preflight_best_quote = plan.get("best_quote_maker_volume")
    preflight_metrics = (
        preflight_best_quote.get("metrics")
        if isinstance(preflight_best_quote, dict)
        else {}
    )
    preflight_same_side_guard = (
        preflight_metrics.get("same_side_entry_price_guard")
        if isinstance(preflight_metrics, dict)
        else {}
    )
    arx_zero_order_volume_updates: dict[str, Any] = {}
    if should_relax_arx_zero_order_volume_blockers(
        symbol=normalized_symbol,
        target_pace_behind=target_pace_behind,
        pace_ratio=pace_ratio,
        active_order_count=_safe_int(assessment.get("active_order_count")),
        planned_order_count=_safe_int(assessment.get("planned_order_count")),
        near_cap=bool(assessment.get("near_cap")),
        volatility_entry_pause_active=bool(assessment.get("volatility_entry_pause_active")),
        pause_reasons=tuple(assessment.get("pause_reasons") or ()),
        same_side_entry_blocked=bool(
            preflight_same_side_guard.get("blocked_long_entry")
            or preflight_same_side_guard.get("blocked_short_entry")
        )
        if isinstance(preflight_same_side_guard, dict)
        else False,
    ):
        for key in (
            "best_quote_maker_volume_dynamic_control_trend_entry_guard_enabled",
            "best_quote_maker_volume_dynamic_control_trend_loss_reduce_guard_enabled",
            "best_quote_maker_volume_inventory_bias_enabled",
        ):
            if bool(control.get(key)):
                arx_zero_order_volume_updates[key] = False
        if _safe_int(control.get("best_quote_maker_volume_quote_offset_ticks")) > 0:
            arx_zero_order_volume_updates["best_quote_maker_volume_quote_offset_ticks"] = 0
        if _safe_float(control.get("sticky_entry_price_tolerance_steps")) > 1.0:
            arx_zero_order_volume_updates["sticky_entry_price_tolerance_steps"] = 1.0
        if (
            bool(control.get("best_quote_maker_volume_same_side_entry_price_guard_enabled"))
            and not bool(
                control.get("best_quote_maker_volume_same_side_entry_price_guard_report_only")
            )
            and isinstance(preflight_same_side_guard, dict)
            and (
                bool(preflight_same_side_guard.get("blocked_long_entry"))
                or bool(preflight_same_side_guard.get("blocked_short_entry"))
            )
        ):
            arx_zero_order_volume_updates[
                "best_quote_maker_volume_same_side_entry_price_guard_report_only"
            ] = True
    arx_lopsided_entry_updates = arx_lopsided_entry_recovery_updates(
        control=control,
        assessment=assessment,
    )
    arx_sticky_requote_updates: dict[str, Any] = {}
    arx_severe_pace_capacity = arx_severe_pace_capacity_updates(
        control=control,
        target_pace_behind=target_pace_behind,
        pace_ratio=pace_ratio,
        near_cap=bool(assessment.get("near_cap")),
        volatility_entry_pause_active=bool(assessment.get("volatility_entry_pause_active")),
        frozen_total_notional=_safe_float(assessment.get("frozen_total_notional")),
        frozen_long_notional=_safe_float(assessment.get("frozen_long_notional")),
        frozen_short_notional=_safe_float(assessment.get("frozen_short_notional")),
        actual_long_notional=_safe_float(assessment.get("actual_long_notional")),
        actual_short_notional=_safe_float(assessment.get("actual_short_notional")),
    ) if normalized_symbol == "ARXUSDT" else {}
    arx_frozen_inventory_headroom = arx_frozen_inventory_headroom_updates(
        control=control,
        frozen_long_notional=_safe_float(assessment.get("frozen_long_notional")),
        frozen_short_notional=_safe_float(assessment.get("frozen_short_notional")),
    ) if normalized_symbol == "ARXUSDT" else {}
    arx_side_cap_unwind = {
        **arx_frozen_inventory_headroom,
        **arx_side_cap_unwind_updates(
        control=control,
        actual_long_notional=_safe_float(assessment.get("actual_long_notional")),
        actual_short_notional=_safe_float(assessment.get("actual_short_notional")),
        ),
    } if normalized_symbol == "ARXUSDT" else {}
    arx_fast_sla_capacity_reapply = should_bypass_arx_recovery_drift_debounce(
        symbol=normalized_symbol,
        target_pace_behind=target_pace_behind,
        no_fill_seconds=no_fill_seconds,
        fast_sla_seconds=fast_sla_seconds,
        volatility_entry_pause_active=bool(assessment.get("volatility_entry_pause_active")),
    )
    if (
        should_enable_arx_sticky_requote(
            symbol=normalized_symbol,
            target_pace_behind=target_pace_behind,
            pace_ratio=pace_ratio,
            near_cap=bool(assessment.get("near_cap")),
            volatility_entry_pause_active=bool(assessment.get("volatility_entry_pause_active")),
        )
        and bool(control.get("sticky_entry_preserve_less_aggressive"))
    ):
        # A stale, less-aggressive entry must not survive a severe pace
        # recovery merely for queue priority: it blocks the replacement maker
        # quote that the recovery guard is trying to restore.
        arx_sticky_requote_updates["sticky_entry_preserve_less_aggressive"] = False
    wear_backoff_floor = max(
        parameters.effective_cycle_budget_floor_notional,
        static_cycle_budget_floor_notional,
        max(
            _safe_float(control.get("per_order_notional")),
            _safe_float(control.get("maker_order_notional")),
        )
        * 4.0,
    )
    arx_freeze_policy_updates = _arx_independent_freeze_policy_updates(
        symbol=normalized_symbol,
        control=control,
        temporary_anti_chase_relief=(
            _safe_float(
                recovery_expected_controls.get(
                    "best_quote_maker_volume_same_side_entry_price_guard_min_notional"
                )
            )
            > 200.0
        ),
    )
    bq_volume_plan = plan.get("best_quote_maker_volume")
    bq_volume_metrics = (
        bq_volume_plan.get("metrics")
        if isinstance(bq_volume_plan, dict)
        else {}
    )
    same_side_entry_guard = (
        bq_volume_metrics.get("same_side_entry_price_guard")
        if isinstance(bq_volume_metrics, dict)
        else {}
    )
    same_side_entry_guard = (
        same_side_entry_guard if isinstance(same_side_entry_guard, dict) else {}
    )
    anti_chase_blocks_missing_entry_leg = bool(
        same_side_entry_guard.get("enabled")
    ) and not bool(same_side_entry_guard.get("report_only")) and (
        (
            _safe_int(assessment.get("planned_entry_buy_order_count")) == 0
            and _safe_int(assessment.get("planned_entry_sell_order_count")) > 0
            and bool(same_side_entry_guard.get("blocked_long_entry"))
        )
        or (
            _safe_int(assessment.get("planned_entry_sell_order_count")) == 0
            and _safe_int(assessment.get("planned_entry_buy_order_count")) > 0
            and bool(same_side_entry_guard.get("blocked_short_entry"))
        )
        or (
            _safe_int(assessment.get("planned_entry_order_count")) == 0
            and (
                bool(same_side_entry_guard.get("blocked_long_entry"))
                or bool(same_side_entry_guard.get("blocked_short_entry"))
            )
        )
    )
    arx_missing_entry_anti_chase_recovery_due = (
        normalized_symbol == "ARXUSDT"
        and sla_recovery_due
        and target_pace_behind
        and (
            no_fill_seconds >= 180.0
            or (pace_ratio < 0.5 and low_pace_seconds >= fast_sla_seconds)
        )
        and (
            not confirmed_loss_reduce_wear
            or (
                _safe_float(assessment.get("inventory_notional_gap")) >= 150.0
                and not bool(control.get("best_quote_maker_volume_allow_loss_reduce_only"))
            )
        )
        and not bool(assessment.get("inventory_soft_pressure"))
        and not bool(assessment.get("near_cap"))
        and not (
            bool(control.get("best_quote_maker_volume_allow_loss_reduce_only"))
            and actual_inventory_below_soft
        )
        and anti_chase_blocks_missing_entry_leg
    )
    loss_reduce_flow_updates: dict[str, Any] = {}
    if (
        bool(control.get("best_quote_maker_volume_allow_loss_reduce_only"))
        and effective_inventory_soft_pressure
    ):
        desired_loss_reduce_updates = _loss_reduce_recovery_updates(
            control=control,
            assessment=assessment,
            static_cycle_budget_floor_notional=static_cycle_budget_floor_notional,
            quote_offset_extra_ticks=loss_reduce_quote_offset_extra_ticks,
            pause_baseline_long_notional=pause_baseline_long_notional,
            pause_baseline_short_notional=pause_baseline_short_notional,
        )
        loss_reduce_flow_updates = _loss_reduce_flow_control_updates(
            control,
            desired_loss_reduce_updates,
        )

    action_verification: str | None = None
    control_updated_at = _parse_time(control.get("recovery_control_updated_at"))
    if (
        control_updated_at is not None
        and control_updated_at <= now
        and item.get("last_verified_control_update_at") != control_updated_at.isoformat()
    ):
        submit_generated_at = _parse_time(
            submit.get("submit_generated_at") or submit.get("generated_at")
        )
        execution_progress = bool(
            submit.get("executed")
            and submit_generated_at is not None
            and submit_generated_at >= control_updated_at
            and (
                submit.get("placed_orders")
                or any(
                    str(event.get("kind") or "").upper()
                    in {"ORDER_FILLED", "ORDER_PARTIALLY_FILLED"}
                    for event in (submit.get("observed_execution_events") or [])
                    if isinstance(event, dict)
                )
            )
        )
        action_verification, failures = evaluate_action_verification(
            action_age_seconds=max((now - control_updated_at).total_seconds(), 0.0),
            plan_is_fresh=_plan_time_is_fresh(plan, now=now, max_age_seconds=120.0),
            open_order_drift=_safe_int(recovery_gate.get("open_order_drift")),
            prior_failures=_safe_int(item.get("action_verification_failures")),
            execution_progress=execution_progress,
        )
        item["action_verification_failures"] = failures
        item["last_action_verification"] = action_verification
        if action_verification == "confirmed":
            item["last_verified_control_update_at"] = control_updated_at.isoformat()

    try:
        if stale_or_missing and not recovery_timeout_required:
            action = "skip_stale_or_missing_inputs"
        elif (
            not bool(recovery_gate.get("ok"))
            and not arx_submit_failure_recovery_bypass
            and not should_bypass_arx_side_unwind_recovery_gate(
                symbol=normalized_symbol,
                updates=arx_side_cap_unwind,
            )
            and not recovery_timeout_required
        ):
            action = "skip_recovery_safety_gate"
        elif action_verification == "pending":
            action = "hold_recovery_action_verification_pending"
        elif action_verification == "failed":
            action = "recovery_action_verification_failed_hold"
            item["status"] = "recovery_verification_failed"
        elif arx_side_cap_unwind:
            _remember_recovery_controls(item, control, tuple(arx_side_cap_unwind))
            _remember_recovery_updates(item, arx_side_cap_unwind)
            action = (
                "dry_run_enforce_arx_single_side_cap_unwind"
                if dry_run
                else "enforce_arx_single_side_cap_unwind"
            )
            item.update(
                {
                    "status": "recovery_active",
                    "recovery_started_at": now.isoformat(),
                    "recovery_owned": True,
                    "last_recovery_action_at": now.isoformat(),
                    "last_recovery_action": action,
                }
            )
            changed, backup_path = _apply_control_update(
                symbol=normalized_symbol,
                control_path=control_path,
                control=control,
                updates=arx_side_cap_unwind,
                now=now,
                dry_run=dry_run,
                restart_runner=restart,
            )
        elif arx_severe_pace_capacity:
            _remember_recovery_controls(
                item, control, tuple(arx_severe_pace_capacity)
            )
            _remember_recovery_updates(item, arx_severe_pace_capacity)
            action = (
                "dry_run_raise_arx_severe_pace_capacity"
                if dry_run
                else "raise_arx_severe_pace_capacity"
            )
            item.update(
                {
                    "status": "recovery_active",
                    "recovery_started_at": now.isoformat(),
                    "recovery_owned": True,
                    "last_recovery_action_at": now.isoformat(),
                    "last_recovery_action": action,
                    "last_sla_action_at": now.isoformat(),
                }
            )
            changed, backup_path = _apply_control_update(
                symbol=normalized_symbol,
                control_path=control_path,
                control=control,
                updates=arx_severe_pace_capacity,
                now=now,
                dry_run=dry_run,
                restart_runner=restart,
            )
        elif arx_sticky_requote_updates:
            _remember_recovery_controls(
                item, control, tuple(arx_sticky_requote_updates)
            )
            _remember_recovery_updates(item, arx_sticky_requote_updates)
            action = "dry_run_enable_arx_sticky_requote" if dry_run else "enable_arx_sticky_requote"
            item.update(
                {
                    "status": "recovery_active",
                    "recovery_started_at": now.isoformat(),
                    "recovery_owned": True,
                    "last_recovery_action_at": now.isoformat(),
                    "last_recovery_action": action,
                }
            )
            changed, backup_path = _apply_control_update(
                symbol=normalized_symbol,
                control_path=control_path,
                control=control,
                updates=arx_sticky_requote_updates,
                now=now,
                dry_run=dry_run,
                restart_runner=restart,
            )
        elif arx_zero_order_volume_updates:
            _remember_recovery_controls(
                item,
                control,
                tuple(arx_zero_order_volume_updates),
            )
            _remember_recovery_updates(item, arx_zero_order_volume_updates)
            action = (
                "dry_run_relax_arx_zero_order_soft_blockers"
                if dry_run
                else "relax_arx_zero_order_soft_blockers"
            )
            item.update(
                {
                    "status": "recovery_active",
                    "recovery_started_at": now.isoformat(),
                    "recovery_owned": True,
                    "last_recovery_action_at": now.isoformat(),
                    "last_recovery_action": action,
                    "last_sla_action_at": now.isoformat(),
                }
            )
            changed, backup_path = _apply_control_update(
                symbol=normalized_symbol,
                control_path=control_path,
                control=control,
                updates=arx_zero_order_volume_updates,
                now=now,
                dry_run=dry_run,
                restart_runner=restart,
            )
        elif arx_lopsided_entry_updates:
            _remember_recovery_controls(
                item,
                control,
                tuple(arx_lopsided_entry_updates),
            )
            _remember_recovery_updates(item, arx_lopsided_entry_updates)
            action = (
                "dry_run_relax_arx_lopsided_entry_blockers_for_pace"
                if dry_run
                else "relax_arx_lopsided_entry_blockers_for_pace"
            )
            item.update(
                {
                    "status": "recovery_active",
                    "recovery_started_at": now.isoformat(),
                    "recovery_owned": True,
                    "last_recovery_action_at": now.isoformat(),
                    "last_recovery_action": action,
                    "last_sla_action_at": now.isoformat(),
                }
            )
            changed, backup_path = _apply_control_update(
                symbol=normalized_symbol,
                control_path=control_path,
                control=control,
                updates=arx_lopsided_entry_updates,
                now=now,
                dry_run=dry_run,
                restart_runner=restart,
            )
        elif (
            str(control.get("best_quote_maker_volume_directional_net_guard") or "off")
            .lower()
            .strip()
            == "off"
            and should_repair_arx_loss_reduce_flow(
            symbol=normalized_symbol,
            target_pace_behind=target_pace_behind,
            allow_loss_reduce_only=bool(
                control.get("best_quote_maker_volume_allow_loss_reduce_only")
            ),
            has_flow_updates=bool(loss_reduce_flow_updates),
            high_recovery_wear=high_recovery_wear,
            confirmed_loss_reduce_wear=confirmed_loss_reduce_wear,
            volatility_entry_pause_active=bool(
                assessment.get("volatility_entry_pause_active")
            ),
            )
        ):
            _remember_recovery_controls(
                item,
                control,
                tuple(loss_reduce_flow_updates),
            )
            _remember_recovery_updates(item, loss_reduce_flow_updates)
            action = (
                "dry_run_repair_arx_loss_reduce_flow"
                if dry_run
                else "repair_arx_loss_reduce_flow"
            )
            item.update(
                {
                    "status": "recovery_active",
                    "recovery_started_at": item.get("recovery_started_at") or now.isoformat(),
                    "recovery_owned": True,
                    "last_recovery_action_at": now.isoformat(),
                    "last_recovery_action": action,
                }
            )
            changed, backup_path = _apply_control_update(
                symbol=normalized_symbol,
                control_path=control_path,
                control=control,
                updates=loss_reduce_flow_updates,
                now=now,
                dry_run=dry_run,
                restart_runner=restart,
            )
        elif arx_freeze_policy_updates:
            action = (
                "dry_run_enable_arx_independent_freeze_first"
                if dry_run
                else "enable_arx_independent_freeze_first"
            )
            changed, backup_path = _apply_control_update(
                symbol=normalized_symbol,
                control_path=control_path,
                control=control,
                updates=arx_freeze_policy_updates,
                now=now,
                dry_run=dry_run,
                restart_runner=restart,
            )
        elif (
            normalized_symbol == "OUSDT"
            and target_pace_behind
            and bool(assessment.get("low_volume"))
            and not bool(control.get("best_quote_maker_volume_allow_loss_reduce_only"))
            and _safe_int(assessment.get("active_order_count")) <= 0
            and _safe_int(assessment.get("planned_order_count")) <= 0
            and "budget_below_minimum" in set(assessment.get("pause_reasons") or [])
        ):
            updates = _loss_reduce_recovery_updates(
                control=control,
                assessment=assessment,
                static_cycle_budget_floor_notional=static_cycle_budget_floor_notional,
                quote_offset_extra_ticks=loss_reduce_quote_offset_extra_ticks,
                pause_baseline_long_notional=pause_baseline_long_notional,
                pause_baseline_short_notional=pause_baseline_short_notional,
            )
            _remember_recovery_controls(
                item,
                control,
                tuple(
                    key
                    for key in updates
                    if key != "best_quote_maker_volume_net_loss_reduce_enabled"
                ),
            )
            _remember_recovery_updates(item, updates)
            action = (
                "dry_run_enable_ousdt_budget_deadlock_loss_reduce"
                if dry_run
                else "enable_ousdt_budget_deadlock_loss_reduce"
            )
            item.update(
                {
                    "status": "recovery_active",
                    "recovery_started_at": now.isoformat(),
                    "recovery_owned": True,
                    "last_recovery_action_at": now.isoformat(),
                    "last_recovery_action": action,
                }
            )
            changed, backup_path = _apply_control_update(
                symbol=normalized_symbol,
                control_path=control_path,
                control=control,
                updates=updates,
                now=now,
                dry_run=dry_run,
                restart_runner=restart,
            )
        elif (
            target_pace_behind
            and bool(assessment.get("low_volume"))
            and not bool(control.get("best_quote_maker_volume_allow_loss_reduce_only"))
            and _safe_int(assessment.get("active_order_count")) <= 0
            and _safe_int(assessment.get("planned_order_count")) <= 0
            and "budget_below_minimum" in set(assessment.get("pause_reasons") or [])
            and _safe_float(control.get("best_quote_maker_volume_cycle_budget_notional"))
            < wear_backoff_floor
        ):
            updates = {
                "best_quote_maker_volume_allow_loss_reduce_only": False,
                "best_quote_maker_volume_net_loss_reduce_enabled": False,
                "best_quote_maker_volume_cycle_budget_notional": wear_backoff_floor,
            }
            action = (
                "dry_run_restore_runnable_budget_after_wear_backoff"
                if dry_run
                else "restore_runnable_budget_after_wear_backoff"
            )
            item.update(
                {
                    "status": "normal",
                    "last_recovery_action_at": now.isoformat(),
                    "last_recovery_action": action,
                }
            )
            _set_post_restore_cooldown(
                item,
                now=now,
                cooldown_seconds=post_restore_cooldown_seconds,
            )
            changed, backup_path = _apply_control_update(
                symbol=normalized_symbol,
                control_path=control_path,
                control=control,
                updates=updates,
                now=now,
                dry_run=dry_run,
                restart_runner=restart,
            )
            item.pop("guard_original_controls", None)
            item.pop("guard_recovery_controls", None)
            item.pop("recovery_started_at", None)
            item.pop("recovery_owned", None)
        elif (
            arx_severe_volume_priority_recovery
            and bool(assessment.get("inventory_soft_pressure"))
            and not critical_arx_inventory_pace_override
            and not bool(control.get("best_quote_maker_volume_allow_loss_reduce_only"))
        ):
            updates = _loss_reduce_recovery_updates(
                control=control,
                assessment=assessment,
                static_cycle_budget_floor_notional=max(
                    cycle_budget_floor_notional,
                    _safe_float(control.get("best_quote_maker_volume_cycle_budget_notional")),
                ),
                quote_offset_extra_ticks=loss_reduce_quote_offset_extra_ticks,
                pause_baseline_long_notional=pause_baseline_long_notional,
                pause_baseline_short_notional=pause_baseline_short_notional,
            )
            updates["best_quote_maker_volume_active_pair_reduce_enabled"] = False
            updates["sticky_entry_preserve_less_aggressive"] = False
            _remember_recovery_controls(
                item,
                control,
                tuple(
                    key
                    for key in updates
                    if key != "best_quote_maker_volume_net_loss_reduce_enabled"
                ),
            )
            _remember_recovery_updates(item, updates)
            action = (
                "dry_run_enable_arx_volume_priority_maker_release"
                if dry_run
                else "enable_arx_volume_priority_maker_release"
            )
            item.update(
                {
                    "status": "recovery_active",
                    "recovery_started_at": item.get("recovery_started_at") or now.isoformat(),
                    "recovery_owned": True,
                    "last_recovery_action_at": now.isoformat(),
                    "last_recovery_action": action,
                }
            )
            changed, backup_path = _apply_control_update(
                symbol=normalized_symbol,
                control_path=control_path,
                control=control,
                updates=updates,
                now=now,
                dry_run=dry_run,
                restart_runner=restart,
            )
        elif (
            (high_recovery_wear or confirmed_loss_reduce_wear)
            and bool(control.get("best_quote_maker_volume_allow_loss_reduce_only"))
            and not recovery_hold_satisfied
            and not active_pair_reduce_deadlock
        ):
            action = "hold_loss_reduce_min_duration_before_wear_judgement"
            item.update(
                {
                    "status": "recovery_active",
                    "last_recovery_check_at": now.isoformat(),
                }
            )
        elif (
            high_recovery_wear
            and bool(control.get("best_quote_maker_volume_allow_loss_reduce_only"))
            and not confirmed_loss_reduce_wear
            and not active_pair_reduce_deadlock
        ):
            action = "hold_loss_reduce_until_fresh_wear_confirmation"
            item.update(
                {
                    "status": "recovery_active",
                    "last_recovery_check_at": now.isoformat(),
                }
            )
        elif (
            confirmed_loss_reduce_wear
            and bool(control.get("best_quote_maker_volume_allow_loss_reduce_only"))
        ):
            current_budget = _safe_float(
                control.get("best_quote_maker_volume_cycle_budget_notional")
            )
            updates = _restore_recovery_controls(
                item,
                control,
                wear_backoff_floor,
            )
            updates.update(
                {
                    "best_quote_maker_volume_allow_loss_reduce_only": False,
                    "best_quote_maker_volume_net_loss_reduce_enabled": False,
                    "best_quote_maker_volume_active_pair_reduce_enabled": False,
                    "best_quote_maker_volume_cycle_budget_notional": max(
                        wear_backoff_floor,
                        current_budget
                        - max(float(volume_recovery_cycle_budget_increment), 0.0),
                    ),
                    "best_quote_maker_volume_quote_offset_ticks": max(
                        _safe_int(control.get("best_quote_maker_volume_quote_offset_ticks")),
                        1,
                    ),
                }
            )
            action = (
                "dry_run_disable_loss_reduce_for_high_wear"
                if dry_run
                else "disable_loss_reduce_for_high_wear"
            )
            item.update(
                {
                    "last_recovery_action_at": now.isoformat(),
                    "last_recovery_action": action,
                }
            )
            _set_post_restore_cooldown(
                item,
                now=now,
                cooldown_seconds=post_restore_cooldown_seconds,
            )
            changed, backup_path = _apply_control_update(
                symbol=normalized_symbol,
                control_path=control_path,
                control=control,
                updates=updates,
                now=now,
                dry_run=dry_run,
                restart_runner=restart,
            )
            item.pop("guard_original_controls", None)
            item.pop("guard_recovery_controls", None)
            item.pop("recovery_started_at", None)
            item.pop("recovery_owned", None)
        elif (
            high_recovery_wear
            and confirmed_loss_reduce_wear
            and not bool(control.get("best_quote_maker_volume_allow_loss_reduce_only"))
            and bool(control.get("best_quote_maker_volume_active_pair_reduce_enabled"))
            and _safe_int(assessment.get("planned_reduce_only_order_count")) > 0
        ):
            current_budget = _safe_float(
                control.get("best_quote_maker_volume_cycle_budget_notional")
            )
            updates = {
                "best_quote_maker_volume_allow_loss_reduce_only": False,
                "best_quote_maker_volume_net_loss_reduce_enabled": False,
                "best_quote_maker_volume_active_pair_reduce_enabled": False,
                "best_quote_maker_volume_cycle_budget_notional": max(
                    wear_backoff_floor,
                    current_budget
                    - max(float(volume_recovery_cycle_budget_increment), 0.0),
                ),
                "best_quote_maker_volume_quote_offset_ticks": max(
                    _safe_int(control.get("best_quote_maker_volume_quote_offset_ticks")),
                    1,
                ),
            }
            _remember_recovery_controls(
                item,
                control,
                (
                    "best_quote_maker_volume_active_pair_reduce_enabled",
                    "best_quote_maker_volume_cycle_budget_notional",
                    "best_quote_maker_volume_quote_offset_ticks",
                ),
            )
            _remember_recovery_updates(item, updates)
            action = (
                "dry_run_suppress_active_pair_reduce_for_high_wear"
                if dry_run
                else "suppress_active_pair_reduce_for_high_wear"
            )
            item.update(
                {
                    "status": "recovery_active",
                    "last_recovery_action_at": now.isoformat(),
                    "last_recovery_action": action,
                }
            )
            changed, backup_path = _apply_control_update(
                symbol=normalized_symbol,
                control_path=control_path,
                control=control,
                updates=updates,
                now=now,
                dry_run=dry_run,
                restart_runner=restart,
            )
        elif (
            high_recovery_wear
            and target_pace_behind
            and not bool(control.get("best_quote_maker_volume_allow_loss_reduce_only"))
            and not bool(assessment.get("inventory_soft_pressure"))
            and not bool(assessment.get("near_cap"))
            and _safe_int(assessment.get("planned_entry_order_count")) >= 2
            and _safe_int(assessment.get("planned_reduce_only_order_count")) == 0
            and _safe_float(control.get("best_quote_maker_volume_cycle_budget_notional"))
            < cycle_budget_floor_notional
        ):
            updates = {
                "best_quote_maker_volume_allow_loss_reduce_only": False,
                "best_quote_maker_volume_net_loss_reduce_enabled": False,
                "best_quote_maker_volume_cycle_budget_notional": cycle_budget_floor_notional,
                "best_quote_maker_volume_quote_offset_ticks": max(
                    _safe_int(control.get("best_quote_maker_volume_quote_offset_ticks")) - 1,
                    1,
                ),
            }
            _remember_recovery_controls(
                item,
                control,
                (
                    "best_quote_maker_volume_cycle_budget_notional",
                    "best_quote_maker_volume_quote_offset_ticks",
                ),
            )
            _remember_recovery_updates(item, updates)
            action = (
                "dry_run_restore_normal_entry_pace_after_high_wear"
                if dry_run
                else "restore_normal_entry_pace_after_high_wear"
            )
            item.update(
                {
                    "status": "recovery_active",
                    "recovery_owned": True,
                    "last_recovery_action_at": now.isoformat(),
                    "last_recovery_action": action,
                    "cooldown_until": (
                        now
                        + timedelta(seconds=max(float(post_restore_cooldown_seconds), 0.0))
                    ).isoformat(),
                    "post_restore_budget_cooldown_until": (
                        now
                        + timedelta(seconds=max(float(post_restore_cooldown_seconds), 0.0))
                    ).isoformat(),
                }
            )
            changed, backup_path = _apply_control_update(
                symbol=normalized_symbol,
                control_path=control_path,
                control=control,
                updates=updates,
                now=now,
                dry_run=dry_run,
                restart_runner=restart,
            )
        elif (
            bool(assessment.get("ledger_position_drift_blocked"))
            and _safe_int(assessment.get("active_order_count")) > 0
        ):
            action = "hold_ledger_position_drift_safety_gate"
        elif active_pair_reduce_deadlock:
            updates = {
                "best_quote_maker_volume_active_pair_reduce_enabled": False,
                "best_quote_maker_volume_net_loss_reduce_enabled": False,
            }
            _remember_recovery_controls(
                item,
                control,
                ("best_quote_maker_volume_active_pair_reduce_enabled",),
            )
            _remember_recovery_updates(item, updates)
            action = (
                "dry_run_disable_stalled_active_pair_reduce_suppression"
                if dry_run
                else "disable_stalled_active_pair_reduce_suppression"
            )
            item.update(
                {
                    "status": "recovery_active",
                    "recovery_started_at": item.get("recovery_started_at") or now.isoformat(),
                    "recovery_owned": True,
                    "last_recovery_action_at": now.isoformat(),
                    "last_recovery_action": action,
                }
            )
            changed, backup_path = _apply_control_update(
                symbol=normalized_symbol,
                control_path=control_path,
                control=control,
                updates=updates,
                now=now,
                dry_run=dry_run,
                restart_runner=restart,
            )
        elif pause_baseline_updates:
            updates = {
                **pause_baseline_updates,
                "best_quote_maker_volume_net_loss_reduce_enabled": False,
            }
            action = (
                "dry_run_restore_pause_baseline_after_recovery_drift"
                if dry_run
                else "restore_pause_baseline_after_recovery_drift"
            )
            changed, backup_path = _apply_control_update(
                symbol=normalized_symbol,
                control_path=control_path,
                control=control,
                updates=updates,
                now=now,
                dry_run=dry_run,
                restart_runner=restart,
            )
        elif directional_loss_reduce_pause_updates:
            updates = {
                **directional_loss_reduce_pause_updates,
                "best_quote_maker_volume_net_loss_reduce_enabled": False,
            }
            action = (
                "dry_run_correct_loss_reduce_to_dominant_side"
                if dry_run
                else "correct_loss_reduce_to_dominant_side"
            )
            changed, backup_path = _apply_control_update(
                symbol=normalized_symbol,
                control_path=control_path,
                control=control,
                updates=updates,
                now=now,
                dry_run=dry_run,
                restart_runner=restart,
            )
        elif (
            bool(control.get("best_quote_maker_volume_allow_loss_reduce_only"))
            and recovery_low_volume
            and effective_inventory_soft_pressure
            and max(
                _safe_float(assessment.get("current_long_notional")),
                _safe_float(assessment.get("current_short_notional")),
            )
            > min(
                _safe_float(assessment.get("current_long_notional")),
                _safe_float(assessment.get("current_short_notional")),
            )
            * 2.0
            and (
                (
                    _safe_float(assessment.get("current_long_notional"))
                    > _safe_float(assessment.get("long_soft_limit_notional"))
                    and _safe_int(assessment.get("planned_reduce_long_order_count")) <= 0
                )
                or (
                    _safe_float(assessment.get("current_short_notional"))
                    > _safe_float(assessment.get("short_soft_limit_notional"))
                    and _safe_int(assessment.get("planned_reduce_short_order_count")) <= 0
                )
            )
            and "best_quote_maker_volume_inventory_bias_reduce_share" in control
            and _safe_float(control.get("best_quote_maker_volume_inventory_bias_reduce_share")) < 0.25
            and not bool(assessment.get("volatility_entry_pause_active"))
        ):
            updates = {
                "best_quote_maker_volume_net_loss_reduce_enabled": False,
                "best_quote_maker_volume_inventory_bias_reduce_share": 0.25,
            }
            expected_recovery_budget = max(
                _safe_float(
                    recovery_expected_controls.get(
                        "best_quote_maker_volume_cycle_budget_notional"
                    )
                ),
                0.0,
            )
            if expected_recovery_budget > _safe_float(
                control.get("best_quote_maker_volume_cycle_budget_notional")
            ):
                updates["best_quote_maker_volume_cycle_budget_notional"] = (
                    expected_recovery_budget
                )
            if (
                loss_reduce_quote_offset_extra_ticks > 0
                and not bool(assessment.get("dynamic_quote_offset_applied"))
                and _safe_int(control.get("best_quote_maker_volume_quote_offset_ticks"))
                < int(loss_reduce_quote_offset_extra_ticks)
            ):
                updates["best_quote_maker_volume_quote_offset_ticks"] = int(
                    loss_reduce_quote_offset_extra_ticks
                )
            _remember_recovery_controls(item, control, tuple(updates))
            _remember_recovery_updates(item, updates)
            action = (
                "dry_run_enable_dominant_leg_reduce_share_for_wrong_way_recovery"
                if dry_run
                else "enable_dominant_leg_reduce_share_for_wrong_way_recovery"
            )
            item.update(
                {
                    "status": "recovery_active",
                    "recovery_started_at": item.get("recovery_started_at") or now.isoformat(),
                    "recovery_owned": True,
                    "last_recovery_action_at": now.isoformat(),
                    "last_recovery_action": action,
                }
            )
            changed, backup_path = _apply_control_update(
                symbol=normalized_symbol,
                control_path=control_path,
                control=control,
                updates=updates,
                now=now,
                dry_run=dry_run,
                restart_runner=restart,
            )
        elif (
            sla_recovery_due
            and not high_recovery_wear
            and bool(control.get("best_quote_maker_volume_allow_loss_reduce_only"))
            and _safe_int(assessment.get("active_order_count")) <= 0
            and _safe_int(assessment.get("planned_order_count")) <= 0
            and bool(assessment.get("inventory_soft_pressure"))
            and _safe_float(control.get("best_quote_maker_volume_cycle_budget_notional"))
            < max(
                _safe_float(control.get("best_quote_maker_volume_min_cycle_budget_notional")),
                _safe_float(control.get("per_order_notional")),
                _safe_float(control.get("maker_order_notional")),
            )
        ):
            minimum_budget = max(
                _safe_float(control.get("best_quote_maker_volume_min_cycle_budget_notional")),
                _safe_float(control.get("per_order_notional")),
                _safe_float(control.get("maker_order_notional")),
            )
            updates = {
                "best_quote_maker_volume_allow_loss_reduce_only": True,
                "best_quote_maker_volume_net_loss_reduce_enabled": False,
                "best_quote_maker_volume_cycle_budget_notional": minimum_budget,
                "best_quote_maker_volume_quote_offset_ticks": 0,
            }
            _remember_recovery_controls(
                item,
                control,
                (
                    "best_quote_maker_volume_cycle_budget_notional",
                    "best_quote_maker_volume_quote_offset_ticks",
                ),
            )
            _remember_recovery_updates(item, updates)
            action = (
                "dry_run_fund_zero_order_loss_recovery"
                if dry_run
                else "fund_zero_order_loss_recovery"
            )
            item.update(
                {
                    "status": "recovery_active",
                    "recovery_owned": True,
                    "last_recovery_action_at": now.isoformat(),
                    "last_recovery_action": action,
                    "last_sla_action_at": now.isoformat(),
                }
            )
            changed, backup_path = _apply_control_update(
                symbol=normalized_symbol,
                control_path=control_path,
                control=control,
                updates=updates,
                now=now,
                dry_run=dry_run,
                restart_runner=restart,
            )
        elif (
            bool(control.get("best_quote_maker_volume_allow_loss_reduce_only"))
            and effective_inventory_soft_pressure
            and not sla_recovery_due
            and not sla_action_debounced
            and loss_reduce_quote_offset_extra_ticks > 0
            and not bool(assessment.get("dynamic_quote_offset_applied"))
            and not (
                normalized_symbol == "ARXUSDT" and target_pace_behind
            )
            and _safe_int(control.get("best_quote_maker_volume_quote_offset_ticks"))
            < int(loss_reduce_quote_offset_extra_ticks)
        ):
            updates = {
                "best_quote_maker_volume_net_loss_reduce_enabled": False,
                "best_quote_maker_volume_quote_offset_ticks": int(
                    loss_reduce_quote_offset_extra_ticks
                ),
            }
            _remember_recovery_controls(
                item,
                control,
                ("best_quote_maker_volume_quote_offset_ticks",),
            )
            _remember_recovery_updates(item, updates)
            action = (
                "dry_run_add_loss_reduce_offset_for_wear"
                if dry_run
                else "add_loss_reduce_offset_for_wear"
            )
            item.update(
                {
                    "status": "recovery_active",
                    "recovery_started_at": item.get("recovery_started_at") or now.isoformat(),
                    "recovery_owned": True,
                    "last_recovery_action_at": now.isoformat(),
                    "last_recovery_action": action,
                }
            )
            changed, backup_path = _apply_control_update(
                symbol=normalized_symbol,
                control_path=control_path,
                control=control,
                updates=updates,
                now=now,
                dry_run=dry_run,
                restart_runner=restart,
            )
        elif (
            sla_recovery_due
            and target_pace_behind
            and (
                no_fill_seconds >= 180.0
                or one_sided_entry_seconds >= fast_sla_seconds
            )
            and not high_recovery_wear
            and bool(control.get("best_quote_maker_volume_allow_loss_reduce_only"))
            and effective_inventory_soft_pressure
            and _safe_int(assessment.get("active_order_count")) > 0
            and _safe_int(assessment.get("planned_reduce_only_order_count")) > 0
            and not bool(assessment.get("all_entry_orders_far"))
            and (
                _safe_int(control.get("best_quote_maker_volume_quote_offset_ticks")) > 0
                or _safe_float(control.get("sticky_entry_price_tolerance_steps")) > 1.0
                or _safe_float(control.get("best_quote_maker_volume_cycle_budget_notional"))
                != loss_reduce_cycle_budget_cap
            )
        ):
            updates = {
                "best_quote_maker_volume_allow_loss_reduce_only": True,
                "best_quote_maker_volume_net_loss_reduce_enabled": False,
                "best_quote_maker_volume_cycle_budget_notional": loss_reduce_cycle_budget_cap,
                "best_quote_maker_volume_quote_offset_ticks": 0,
                "sticky_entry_price_tolerance_steps": 1.0,
            }
            updates.update(loss_reduce_flow_updates)
            _remember_recovery_controls(
                item,
                control,
                tuple(
                    key
                    for key in updates
                    if key != "best_quote_maker_volume_net_loss_reduce_enabled"
                ),
            )
            _remember_recovery_updates(item, updates)
            action = (
                "dry_run_accelerate_loss_reduce_for_no_fill_sla"
                if dry_run
                else "accelerate_loss_reduce_for_no_fill_sla"
            )
            item.update(
                {
                    "status": "recovery_active",
                    "recovery_started_at": item.get("recovery_started_at") or now.isoformat(),
                    "recovery_owned": True,
                    "last_recovery_action_at": now.isoformat(),
                    "last_recovery_action": action,
                    "last_sla_action_at": now.isoformat(),
                }
            )
            changed, backup_path = _apply_control_update(
                symbol=normalized_symbol,
                control_path=control_path,
                control=control,
                updates=updates,
                now=now,
                dry_run=dry_run,
                restart_runner=restart,
            )
        elif (
            bool(control.get("best_quote_maker_volume_allow_loss_reduce_only"))
            and bool(assessment.get("dynamic_quote_offset_applied"))
            and _safe_int(assessment.get("planned_entry_order_count")) > 0
            and _safe_int(assessment.get("dynamic_quote_offset_ticks"))
            >= _safe_int(control.get("best_quote_maker_volume_quote_offset_ticks")) + 4
            and _safe_int(control.get("best_quote_maker_volume_quote_offset_ticks")) > 0
        ):
            updates = {
                "best_quote_maker_volume_net_loss_reduce_enabled": False,
                "best_quote_maker_volume_quote_offset_ticks": max(
                    _safe_int(control.get("best_quote_maker_volume_quote_offset_ticks")) - 1,
                    0,
                ),
            }
            _remember_recovery_controls(
                item,
                control,
                ("best_quote_maker_volume_quote_offset_ticks",),
            )
            _remember_recovery_updates(item, updates)
            action = (
                "dry_run_remove_loss_reduce_offset_under_dynamic_widening"
                if dry_run
                else "remove_loss_reduce_offset_under_dynamic_widening"
            )
            item.update(
                {
                    "status": "recovery_active",
                    "recovery_started_at": item.get("recovery_started_at") or now.isoformat(),
                    "recovery_owned": True,
                    "last_recovery_action_at": now.isoformat(),
                    "last_recovery_action": action,
                }
            )
            changed, backup_path = _apply_control_update(
                symbol=normalized_symbol,
                control_path=control_path,
                control=control,
                updates=updates,
                now=now,
                dry_run=dry_run,
                restart_runner=restart,
            )
        elif (
            target_pace_behind
            and not recovery_reapply_debounced
            and bool(control.get("best_quote_maker_volume_allow_loss_reduce_only"))
            and _safe_int(assessment.get("planned_entry_order_count")) > 0
            and bool(assessment.get("all_entry_orders_far"))
            and _safe_int(assessment.get("near_market_reduce_only_order_count")) > 0
            and _safe_float(volume_summary.get("trailing_15m_gross_notional")) >= 100.0
            and trailing_5m_realized_wear_per_10k <= 3.0
            and trailing_15m_realized_wear_per_10k <= 3.0
            and _safe_int(control.get("best_quote_maker_volume_quote_offset_ticks")) > 1
        ):
            updates = {
                "best_quote_maker_volume_net_loss_reduce_enabled": False,
                "best_quote_maker_volume_quote_offset_ticks": max(
                    _safe_int(control.get("best_quote_maker_volume_quote_offset_ticks")) - 1,
                    1,
                ),
            }
            _remember_recovery_controls(
                item,
                control,
                ("best_quote_maker_volume_quote_offset_ticks",),
            )
            _remember_recovery_updates(item, updates)
            action = (
                "dry_run_pull_directional_entry_one_tick_closer"
                if dry_run
                else "pull_directional_entry_one_tick_closer"
            )
            item.update(
                {
                    "status": "recovery_active",
                    "recovery_started_at": now.isoformat(),
                    "recovery_owned": True,
                    "last_recovery_action_at": now.isoformat(),
                    "last_recovery_action": action,
                }
            )
            changed, backup_path = _apply_control_update(
                symbol=normalized_symbol,
                control_path=control_path,
                control=control,
                updates=updates,
                now=now,
                dry_run=dry_run,
                restart_runner=restart,
            )
        elif (
            target_pace_behind
            and not recovery_reapply_debounced
            and not bool(control.get("best_quote_maker_volume_allow_loss_reduce_only"))
            and _safe_int(assessment.get("planned_entry_order_count")) > 0
            and _safe_float(assessment.get("inventory_notional_gap"))
            >= min(
                _safe_float(assessment.get("long_soft_limit_notional")),
                _safe_float(assessment.get("short_soft_limit_notional")),
            )
            * 0.25
            and required_hourly_notional > 0
            and trailing_hourly_notional < required_hourly_notional * 0.75
            and _safe_float(volume_summary.get("trailing_15m_gross_notional")) >= 100.0
            and (
                bool(assessment.get("balancing_entry_requote_safe"))
                or (
                    trailing_5m_realized_wear_per_10k <= 3.0
                    and trailing_15m_realized_wear_per_10k <= 3.0
                )
            )
            and _safe_int(control.get("best_quote_maker_volume_quote_offset_ticks")) > 0
        ):
            updates = {
                "best_quote_maker_volume_net_loss_reduce_enabled": False,
                "best_quote_maker_volume_quote_offset_ticks": max(
                    _safe_int(control.get("best_quote_maker_volume_quote_offset_ticks")) - 1,
                    0,
                ),
            }
            _remember_recovery_controls(
                item,
                control,
                ("best_quote_maker_volume_quote_offset_ticks",),
            )
            _remember_recovery_updates(item, updates)
            action = (
                "dry_run_pull_imbalanced_entry_one_tick_closer_for_pace"
                if dry_run
                else "pull_imbalanced_entry_one_tick_closer_for_pace"
            )
            item.update(
                {
                    "status": "recovery_active",
                    "recovery_started_at": now.isoformat(),
                    "recovery_owned": True,
                    "last_recovery_action_at": now.isoformat(),
                    "last_recovery_action": action,
                }
            )
            changed, backup_path = _apply_control_update(
                symbol=normalized_symbol,
                control_path=control_path,
                control=control,
                updates=updates,
                now=now,
                dry_run=dry_run,
                restart_runner=restart,
            )
        elif should_close_loss_reduce_when_target_pace_ahead(
            symbol=normalized_symbol,
            target_pace_ahead=target_pace_ahead,
            allow_loss_reduce_only=bool(
                control.get("best_quote_maker_volume_allow_loss_reduce_only")
            ),
            current_long_notional=_safe_float(assessment.get("current_long_notional")),
            current_short_notional=_safe_float(assessment.get("current_short_notional")),
            max_long_notional=_safe_float(assessment.get("max_long_notional")),
            max_short_notional=_safe_float(assessment.get("max_short_notional")),
        ):
            updates = _restore_recovery_controls(item, control, cycle_budget_floor_notional)
            if normalized_symbol == "ARXUSDT":
                updates.update(
                    arx_frozen_inventory_headroom_updates(
                        control=control,
                        frozen_long_notional=_safe_float(
                            assessment.get("frozen_long_notional")
                        ),
                        frozen_short_notional=_safe_float(
                            assessment.get("frozen_short_notional")
                        ),
                    )
                )
            changed, backup_path = _apply_control_update(
                symbol=normalized_symbol,
                control_path=control_path,
                control=control,
                updates=updates,
                now=now,
                dry_run=dry_run,
                restart_runner=restart,
            )
            action = (
                "dry_run_restore_loss_reduce_when_target_pace_ahead"
                if dry_run
                else "restore_loss_reduce_when_target_pace_ahead"
            )
            _set_post_restore_cooldown(
                item,
                now=now,
                cooldown_seconds=post_restore_cooldown_seconds,
            )
            item.pop("guard_original_controls", None)
            item.pop("guard_recovery_controls", None)
            item.pop("recovery_started_at", None)
            item.pop("recovery_owned", None)
        elif (
            target_pace_ahead
            and not recovery_has_original_controls
            and not bool(control.get("best_quote_maker_volume_allow_loss_reduce_only"))
            and effective_inventory_soft_pressure
            and _safe_int(assessment.get("active_order_count")) > 0
        ):
            action = "hold_loss_reduce_while_target_pace_ahead"
            item.update(
                {
                    "status": "normal",
                    "last_recovery_check_at": now.isoformat(),
                    "last_normal_at": now.isoformat(),
                }
            )
        elif (
            _normal_entry_wear_backoff_confirmed(
                volume_summary,
                target_pace_behind=target_pace_behind,
            )
            and not recovery_reapply_debounced
            and not post_restore_budget_cooldown_active
            and not bool(control.get("best_quote_maker_volume_allow_loss_reduce_only"))
            and not effective_inventory_soft_pressure
            and not bool(assessment.get("inventory_soft_pressure"))
            and not bool(assessment.get("balancing_entry_requote_safe"))
            and (
                _safe_float(control.get("best_quote_maker_volume_cycle_budget_notional"))
                > wear_backoff_floor
                or _safe_int(control.get("best_quote_maker_volume_quote_offset_ticks")) < 1
            )
        ):
            current_budget = _safe_float(control.get("best_quote_maker_volume_cycle_budget_notional"))
            updates = {
                "best_quote_maker_volume_net_loss_reduce_enabled": False,
                "best_quote_maker_volume_cycle_budget_notional": max(
                    wear_backoff_floor,
                    current_budget - max(float(volume_recovery_cycle_budget_increment), 0.0),
                ),
                "best_quote_maker_volume_quote_offset_ticks": max(
                    _safe_int(control.get("best_quote_maker_volume_quote_offset_ticks")),
                    1,
                ),
            }
            _remember_recovery_controls(
                item,
                control,
                (
                    "best_quote_maker_volume_cycle_budget_notional",
                    "best_quote_maker_volume_quote_offset_ticks",
                ),
            )
            _remember_recovery_updates(item, updates)
            action = "dry_run_backoff_cycle_budget_for_wear" if dry_run else "backoff_cycle_budget_for_wear"
            item.update(
                {
                    "status": "recovery_active",
                    "recovery_started_at": now.isoformat(),
                    "recovery_owned": True,
                    "last_recovery_action_at": now.isoformat(),
                    "last_recovery_action": action,
                }
            )
            changed, backup_path = _apply_control_update(
                symbol=normalized_symbol,
                control_path=control_path,
                control=control,
                updates=updates,
                now=now,
                dry_run=dry_run,
                restart_runner=restart,
            )
        elif (
            target_pace_behind
            and not recovery_reapply_debounced
            and bool(control.get("best_quote_maker_volume_allow_loss_reduce_only"))
            and effective_inventory_soft_pressure
            and _safe_int(assessment.get("planned_entry_order_count")) > 0
            and _safe_int(assessment.get("planned_reduce_only_order_count")) > 0
            and _safe_float(volume_summary.get("trailing_15m_gross_notional")) >= 100.0
            and trailing_5m_realized_wear_per_10k <= 3.0
            and trailing_15m_realized_wear_per_10k <= 3.0
            and static_cycle_budget_floor_notional > 0
            and _safe_float(control.get("best_quote_maker_volume_cycle_budget_notional"))
            < static_cycle_budget_floor_notional
        ):
            current_budget = _safe_float(control.get("best_quote_maker_volume_cycle_budget_notional"))
            severe_target_pace_deficit = (
                required_hourly_notional > 0
                and trailing_hourly_notional < required_hourly_notional * 0.5
            )
            target_recovery_budget = (
                static_cycle_budget_floor_notional
                if severe_target_pace_deficit
                else min(
                    static_cycle_budget_floor_notional,
                    current_budget + max(float(volume_recovery_cycle_budget_increment), 0.0),
                )
            )
            updates = {
                "best_quote_maker_volume_net_loss_reduce_enabled": False,
                "best_quote_maker_volume_cycle_budget_notional": target_recovery_budget,
            }
            volume_summary["severe_target_pace_deficit"] = severe_target_pace_deficit
            volume_summary["directional_recovery_target_budget_notional"] = target_recovery_budget
            _remember_recovery_controls(
                item,
                control,
                ("best_quote_maker_volume_cycle_budget_notional",),
            )
            _remember_recovery_updates(item, updates)
            action = (
                "dry_run_raise_directional_recovery_budget_to_floor_for_pace"
                if dry_run and severe_target_pace_deficit
                else "raise_directional_recovery_budget_to_floor_for_pace"
                if severe_target_pace_deficit
                else "dry_run_raise_directional_recovery_budget_for_pace"
                if dry_run
                else "raise_directional_recovery_budget_for_pace"
            )
            item.update(
                {
                    "status": "recovery_active",
                    "recovery_started_at": now.isoformat(),
                    "recovery_owned": True,
                    "last_recovery_action_at": now.isoformat(),
                    "last_recovery_action": action,
                }
            )
            changed, backup_path = _apply_control_update(
                symbol=normalized_symbol,
                control_path=control_path,
                control=control,
                updates=updates,
                now=now,
                dry_run=dry_run,
                restart_runner=restart,
            )
        elif (
            recovery_low_volume
            and not post_restore_budget_cooldown_active
            and not recovery_reapply_debounced
            and max(float(cycle_budget_floor_notional), 0.0)
            > _safe_float(control.get("best_quote_maker_volume_cycle_budget_notional"))
            and _safe_float(control.get("best_quote_maker_volume_cycle_budget_notional")) > 0
            and not bool(control.get("best_quote_maker_volume_allow_loss_reduce_only"))
            and not effective_inventory_soft_pressure
            and not bool(assessment.get("inventory_soft_pressure"))
            and bool(assessment.get("budget_raise_inventory_buffer_blocked"))
            and not bool(assessment.get("balancing_budget_raise_safe"))
            and not arx_missing_entry_anti_chase_recovery_due
            and not (
                sla_recovery_due
                and _safe_int(assessment.get("active_order_count")) > 0
                and _safe_int(assessment.get("planned_entry_order_count")) > 0
                and _safe_int(control.get("best_quote_maker_volume_quote_offset_ticks")) > 0
                and net_guard_entry_buffer_ok
            )
        ):
            action = "hold_budget_raise_for_inventory_imbalance"
        elif (
            recovery_low_volume
            and not post_restore_budget_cooldown_active
            and not recovery_reapply_debounced
            and max(float(cycle_budget_floor_notional), 0.0)
            > _safe_float(control.get("best_quote_maker_volume_cycle_budget_notional"))
            and _safe_float(control.get("best_quote_maker_volume_cycle_budget_notional")) > 0
            and not bool(control.get("best_quote_maker_volume_allow_loss_reduce_only"))
            and not effective_inventory_soft_pressure
            and not bool(assessment.get("inventory_soft_pressure"))
            and not bool(assessment.get("near_cap"))
            and not bool(assessment.get("ineffective_orders"))
            and not bool(assessment.get("volatility_entry_pause_active"))
            and (
                not bool(assessment.get("budget_raise_inventory_buffer_blocked"))
                or bool(assessment.get("balancing_budget_raise_safe"))
            )
            and (
                "inventory_bias" not in set(assessment.get("pause_reasons") or [])
                or bool(assessment.get("balancing_budget_raise_safe"))
            )
        ):
            budget_floor = max(float(cycle_budget_floor_notional), 0.0)
            updates = {
                "best_quote_maker_volume_net_loss_reduce_enabled": False,
                "best_quote_maker_volume_cycle_budget_notional": budget_floor,
            }
            _remember_recovery_controls(
                item,
                control,
                tuple(
                    key
                    for key in updates
                    if key
                    not in {
                        "best_quote_maker_volume_allow_loss_reduce_only",
                        "best_quote_maker_volume_net_loss_reduce_enabled",
                    }
                ),
            )
            _remember_recovery_updates(
                item,
                {"best_quote_maker_volume_cycle_budget_notional": budget_floor},
            )
            action = "dry_run_raise_cycle_budget_for_volume" if dry_run else "raise_cycle_budget_for_volume"
            item.update(
                {
                    "status": "recovery_active",
                    "recovery_started_at": now.isoformat(),
                    "recovery_owned": True,
                    "last_recovery_action_at": now.isoformat(),
                    "last_recovery_action": action,
                }
            )
            changed, backup_path = _apply_control_update(
                symbol=normalized_symbol,
                control_path=control_path,
                control=control,
                updates=updates,
                now=now,
                dry_run=dry_run,
                restart_runner=restart,
            )
        elif arx_soft_recovery_extension_updates(
            control=control,
            assessment=assessment,
            volume_summary=volume_summary,
        ):
            updates = arx_soft_recovery_extension_updates(
                control=control,
                assessment=assessment,
                volume_summary=volume_summary,
            )
            _remember_recovery_controls(item, control, tuple(updates))
            _remember_recovery_updates(item, updates)
            action = (
                "dry_run_raise_arx_recovery_maker_flow"
                if dry_run
                else "raise_arx_recovery_maker_flow"
            )
            item.update(
                {
                    "status": "recovery_active",
                    "recovery_started_at": item.get("recovery_started_at") or now.isoformat(),
                    "recovery_owned": True,
                    "last_recovery_action_at": now.isoformat(),
                    "last_recovery_action": action,
                }
            )
            changed, backup_path = _apply_control_update(
                symbol=normalized_symbol,
                control_path=control_path,
                control=control,
                updates=updates,
                now=now,
                dry_run=dry_run,
                restart_runner=restart,
            )
        elif (
            bool(control.get("best_quote_maker_volume_allow_loss_reduce_only"))
            and loss_reduce_cycle_budget_cap > 0
            and _safe_float(control.get("best_quote_maker_volume_cycle_budget_notional"))
            > loss_reduce_cycle_budget_cap
            and not (normalized_symbol == "ARXUSDT" and target_pace_behind)
            and not (
                recovery_has_original_controls
                and recovery_hold_satisfied
                and _recovery_inventory_buffer_ok(
                    assessment,
                    recover_cap_ratio=recover_cap_ratio,
                    original_controls=recovery_original_controls,
                    entry_reserve_notional=recovery_entry_reserve_notional,
                )
            )
        ):
            updates = _loss_reduce_recovery_updates(
                control=control,
                assessment=assessment,
                static_cycle_budget_floor_notional=static_cycle_budget_floor_notional,
                quote_offset_extra_ticks=loss_reduce_quote_offset_extra_ticks,
                pause_baseline_long_notional=pause_baseline_long_notional,
                pause_baseline_short_notional=pause_baseline_short_notional,
            )
            _remember_recovery_controls(
                item,
                control,
                ("best_quote_maker_volume_cycle_budget_notional",),
            )
            _remember_recovery_updates(
                item,
                updates,
            )
            action = (
                "dry_run_cap_loss_reduce_budget_for_wear"
                if dry_run
                else "cap_loss_reduce_budget_for_wear"
            )
            item.update(
                {
                    "status": "recovery_active",
                    "recovery_started_at": item.get("recovery_started_at") or now.isoformat(),
                    "recovery_owned": True,
                    "last_recovery_action_at": now.isoformat(),
                    "last_recovery_action": action,
                }
            )
            changed, backup_path = _apply_control_update(
                symbol=normalized_symbol,
                control_path=control_path,
                control=control,
                updates=updates,
                now=now,
                dry_run=dry_run,
                restart_runner=restart,
            )
        elif (
            two_sided_stale_no_fill
            and (not recovery_reapply_debounced or sla_recovery_due)
            and _safe_float(control.get("sticky_entry_price_tolerance_steps")) > 1.0
        ):
            updates = {
                "best_quote_maker_volume_net_loss_reduce_enabled": False,
                "sticky_entry_price_tolerance_steps": 1.0,
            }
            _remember_recovery_controls(
                item,
                control,
                ("sticky_entry_price_tolerance_steps",),
            )
            _remember_recovery_updates(item, updates)
            action = (
                "dry_run_refresh_stale_two_sided_entries"
                if dry_run
                else "refresh_stale_two_sided_entries"
            )
            item.update(
                {
                    "status": "recovery_active",
                    "recovery_started_at": item.get("recovery_started_at") or now.isoformat(),
                    "recovery_owned": True,
                    "last_recovery_action_at": now.isoformat(),
                    "last_recovery_action": action,
                }
            )
            changed, backup_path = _apply_control_update(
                symbol=normalized_symbol,
                control_path=control_path,
                control=control,
                updates=updates,
                now=now,
                dry_run=dry_run,
                restart_runner=restart,
            )
            item["last_sla_action_at"] = now.isoformat()
        elif arx_missing_entry_anti_chase_recovery_due:
            current_long = max(_safe_float(assessment.get("current_long_notional")), 0.0)
            current_short = max(_safe_float(assessment.get("current_short_notional")), 0.0)
            lighter_inventory = min(current_long, current_short)
            heavier_inventory = max(current_long, current_short)
            order_notional = max(
                _safe_float(control.get("per_order_notional")),
                _safe_float(control.get("maker_order_notional")),
                25.0,
            )
            key = "best_quote_maker_volume_same_side_entry_price_guard_min_notional"
            current_gate = max(_safe_float(control.get(key)), 200.0)
            recovery_gate_already_raised = (
                key in recovery_expected_controls
                and current_gate
                > _safe_float(recovery_original_controls.get(key))
            )
            if recovery_gate_already_raised:
                target_gate = float(ceil(max(heavier_inventory - 1.0, current_gate)))
            elif no_fill_seconds >= 300.0 and pace_ratio < 0.1:
                max_inventory = max(
                    _safe_float(assessment.get("max_long_notional")),
                    _safe_float(assessment.get("max_short_notional")),
                )
                target_gate = float(
                    ceil(
                        min(
                            heavier_inventory + order_notional,
                            max(max_inventory - 1.0, current_gate),
                        )
                    )
                )
            else:
                target_gate = float(
                    ceil(
                        min(
                            max(lighter_inventory + order_notional, current_gate + order_notional),
                            max(heavier_inventory - 1.0, current_gate),
                        )
                    )
                )
            if target_gate > current_gate:
                gap_key = "best_quote_maker_volume_same_side_entry_price_guard_gap_ticks"
                updates = {
                    "best_quote_maker_volume_net_loss_reduce_enabled": False,
                    key: target_gate,
                    gap_key: 0,
                }
                current_budget = _safe_float(
                    control.get("best_quote_maker_volume_cycle_budget_notional")
                )
                target_budget_floor = max(
                    _safe_float(volume_summary.get("target_cycle_budget_floor_notional")),
                    float(cycle_budget_floor_notional),
                    _safe_float(
                        control.get("best_quote_maker_volume_min_cycle_budget_notional")
                    ),
                )
                target_budget = min(
                    target_budget_floor,
                    current_budget
                    + max(float(volume_recovery_cycle_budget_increment), 0.0),
                )
                if target_budget > current_budget:
                    updates["best_quote_maker_volume_cycle_budget_notional"] = target_budget
                if _safe_float(control.get("sticky_entry_price_tolerance_steps")) > 1.0:
                    updates["sticky_entry_price_tolerance_steps"] = 1.0
                _remember_recovery_controls(
                    item,
                    control,
                    tuple(
                        update_key
                        for update_key in updates
                        if update_key != "best_quote_maker_volume_net_loss_reduce_enabled"
                    ),
                )
                _remember_recovery_updates(item, updates)
                action = (
                    "dry_run_relax_arx_v3_anti_chase_for_missing_entry_leg"
                    if dry_run
                    else "relax_arx_v3_anti_chase_for_missing_entry_leg"
                )
                item.update(
                    {
                        "status": "recovery_active",
                        "recovery_started_at": now.isoformat(),
                        "recovery_owned": True,
                        "last_recovery_action_at": now.isoformat(),
                        "last_recovery_action": action,
                        "last_sla_action_at": now.isoformat(),
                    }
                )
                changed, backup_path = _apply_control_update(
                    symbol=normalized_symbol,
                    control_path=control_path,
                    control=control,
                    updates=updates,
                    now=now,
                    dry_run=dry_run,
                    restart_runner=restart,
                )
            else:
                action = "hold_arx_v3_anti_chase_heavier_leg_boundary"
        elif (
            sla_recovery_due
            and target_pace_behind
            and (not high_recovery_wear or arx_severe_near_maker_entry)
            and not bool(control.get("best_quote_maker_volume_allow_loss_reduce_only"))
            and not bool(assessment.get("inventory_soft_pressure"))
            and not bool(assessment.get("near_cap"))
            and _safe_int(assessment.get("active_order_count")) > 0
            and _safe_int(assessment.get("planned_entry_order_count")) > 0
            and not (severe_one_sided_stall and one_sided_entry_unprotected)
            and not (
                severe_one_sided_stall
                and bool(assessment.get("one_sided_inventory_bias"))
                and bool(assessment.get("balancing_entry_requote_safe"))
                and _safe_int(control.get("best_quote_maker_volume_quote_offset_ticks")) <= 0
            )
        ):
            current_budget = _safe_float(
                control.get("best_quote_maker_volume_cycle_budget_notional")
            )
            ladder_capacity = max(
                _safe_float(volume_summary.get("target_cycle_budget_ladder_capacity")),
                cycle_budget_floor_notional,
            )
            target_budget = min(
                ladder_capacity,
                max(
                    cycle_budget_floor_notional,
                    current_budget + max(float(volume_recovery_cycle_budget_increment), 0.0),
                ),
            )
            current_offset = _safe_int(
                control.get("best_quote_maker_volume_quote_offset_ticks")
            )
            updates: dict[str, Any] = {
                "best_quote_maker_volume_allow_loss_reduce_only": False,
                "best_quote_maker_volume_net_loss_reduce_enabled": False,
            }
            if (
                target_budget > current_budget
                and (
                    not bool(assessment.get("budget_raise_inventory_buffer_blocked"))
                    or bool(assessment.get("balancing_budget_raise_safe"))
                )
            ):
                updates["best_quote_maker_volume_cycle_budget_notional"] = target_budget
            if current_offset > 0:
                updates["best_quote_maker_volume_quote_offset_ticks"] = (
                    0 if arx_severe_near_maker_entry else current_offset - 1
                )
            if len(updates) == 2 and _safe_float(
                control.get("sticky_entry_price_tolerance_steps")
            ) > 1.0:
                updates["sticky_entry_price_tolerance_steps"] = 1.0
            _remember_recovery_controls(
                item,
                control,
                tuple(
                    key
                    for key in updates
                    if key
                    not in {
                        "best_quote_maker_volume_allow_loss_reduce_only",
                        "best_quote_maker_volume_net_loss_reduce_enabled",
                    }
                ),
            )
            _remember_recovery_updates(item, updates)
            action = (
                "dry_run_escalate_normal_entry_for_sla"
                if dry_run
                else "escalate_normal_entry_for_sla"
            )
            item.update(
                {
                    "status": "recovery_active",
                    "recovery_owned": True,
                    "last_recovery_action_at": now.isoformat(),
                    "last_recovery_action": action,
                    "last_sla_action_at": now.isoformat(),
                }
            )
            changed, backup_path = _apply_control_update(
                symbol=normalized_symbol,
                control_path=control_path,
                control=control,
                updates=updates,
                now=now,
                dry_run=dry_run,
                restart_runner=restart,
            )
        elif (
            cooldown_until is not None
            and now < cooldown_until
            and not arx_severe_volume_priority_recovery
            and not severe_one_sided_stall
            and not (recovery_low_volume and effective_inventory_soft_pressure)
            and not (
                recovery_low_volume
                and actual_inventory_below_soft
                and required_hourly_notional > 0
                and not target_pace_ahead
            )
        ):
            action = "cooldown"
        elif recovery_has_original_controls and not bool(control.get("best_quote_maker_volume_allow_loss_reduce_only")):
            cap_buffer_ok = _recovery_inventory_buffer_ok(
                assessment,
                recover_cap_ratio=recover_cap_ratio,
                original_controls=recovery_original_controls,
                entry_reserve_notional=recovery_entry_reserve_notional,
            )
            restore_ready = (
                _safe_float(volume_summary.get("gross_notional")) >= recover_floor
                and _safe_int(assessment.get("active_order_count")) > 0
                and _safe_int(assessment.get("near_market_order_count")) > 0
                and cap_buffer_ok
                and not target_pace_behind
            )
            drifted_updates = {
                key: value
                for key, value in recovery_expected_controls.items()
                if control.get(key) != value
            }
            arx_balanced_capacity_updates = arx_balanced_fast_sla_capacity_updates(
                control=control,
                assessment=assessment,
                target_pace_behind=target_pace_behind,
                no_fill_seconds=no_fill_seconds,
                fast_sla_seconds=fast_sla_seconds,
                high_recovery_wear=high_recovery_wear,
            ) if normalized_symbol == "ARXUSDT" else {}
            if arx_balanced_capacity_updates:
                _remember_recovery_controls(
                    item, control, tuple(arx_balanced_capacity_updates)
                )
                _remember_recovery_updates(item, arx_balanced_capacity_updates)
                changed, backup_path = _apply_control_update(
                    symbol=normalized_symbol,
                    control_path=control_path,
                    control=control,
                    updates=arx_balanced_capacity_updates,
                    now=now,
                    dry_run=dry_run,
                    restart_runner=restart,
                )
                action = (
                    "dry_run_raise_arx_balanced_fast_sla_capacity"
                    if dry_run
                    else "raise_arx_balanced_fast_sla_capacity"
                )
                item.update(
                    {
                        "status": "recovery_active",
                        "recovery_started_at": now.isoformat(),
                        "recovery_owned": True,
                        "last_recovery_action_at": now.isoformat(),
                        "last_recovery_action": action,
                    }
                )
            elif should_enter_loss_reduce(
                low_volume=bool(assessment.get("low_volume")),
                effective_inventory_soft_pressure=effective_inventory_soft_pressure,
                sla_recovery_due=sla_recovery_due,
                target_pace_behind=target_pace_behind,
                inventory_soft_pressure=bool(assessment.get("inventory_soft_pressure")),
                active_order_count=_safe_int(assessment.get("active_order_count")),
                planned_order_count=_safe_int(assessment.get("planned_order_count")),
                planned_reduce_only_only=bool(assessment.get("planned_reduce_only_only")),
                no_fill_seconds=no_fill_seconds,
                trigger_seconds=trigger_seconds,
                recovery_hold_satisfied=recovery_hold_satisfied,
                high_recovery_wear=high_recovery_wear,
                confirmed_loss_reduce_wear=confirmed_loss_reduce_wear,
                recovery_reapply_debounced=recovery_reapply_debounced,
                post_restore_budget_cooldown_active=post_restore_budget_cooldown_active,
                recovery_expected_allow_loss=bool(
                    recovery_expected_controls.get("best_quote_maker_volume_allow_loss_reduce_only")
                ),
                require_soft_pressure_for_allow_loss=require_soft_pressure_for_allow_loss,
                effective_near_market_flow=effective_near_market_flow,
            ):
                loss_updates = _loss_reduce_recovery_updates(
                    control=control,
                    assessment=assessment,
                    static_cycle_budget_floor_notional=static_cycle_budget_floor_notional,
                    quote_offset_extra_ticks=loss_reduce_quote_offset_extra_ticks,
                    pause_baseline_long_notional=pause_baseline_long_notional,
                    pause_baseline_short_notional=pause_baseline_short_notional,
                )
                if (
                    bool(control.get("best_quote_maker_volume_suppress_short_reduce_enabled"))
                    and _safe_float(item.get("net_guard_recovery_baseline_notional")) <= 0
                ):
                    # A directional net-long rescue may have left this switch
                    # behind.  Ordinary soft recovery needs both reduce legs;
                    # do not persist the stale directional suppression.
                    loss_updates["best_quote_maker_volume_suppress_short_reduce_enabled"] = False
                _remember_recovery_controls(
                    item,
                    control,
                    tuple(
                        key
                        for key in loss_updates
                        if key
                        not in {
                            "best_quote_maker_volume_net_loss_reduce_enabled",
                            "best_quote_maker_volume_suppress_short_reduce_enabled",
                        }
                    ),
                )
                updates = _restore_recovery_controls(item, control, cycle_budget_floor_notional)
                updates.update(loss_updates)
                item["guard_recovery_controls"] = {}
                _remember_recovery_updates(item, updates)
                changed, backup_path = _apply_control_update(
                    symbol=normalized_symbol,
                    control_path=control_path,
                    control=control,
                    updates=updates,
                    now=now,
                    dry_run=dry_run,
                    restart_runner=restart,
                )
                action = (
                    "dry_run_switch_to_soft_inventory_loss_reduce"
                    if dry_run
                    else "switch_to_soft_inventory_loss_reduce"
                )
                item.update(
                    {
                        "status": "recovery_active",
                        "recovery_started_at": now.isoformat(),
                        "soft_recovery_extension_count": 0,
                        "recovery_owned": True,
                        "last_recovery_action_at": now.isoformat(),
                        "last_recovery_action": action,
                    }
                )
            elif (
                severe_one_sided_stall
                and recovery_hold_satisfied
                and not recovery_reapply_debounced
                and _safe_float(control.get("sticky_entry_price_tolerance_steps")) > 1.0
            ):
                _remember_recovery_controls(
                    item,
                    control,
                    ("sticky_entry_price_tolerance_steps",),
                )
                updates = {
                    "best_quote_maker_volume_net_loss_reduce_enabled": False,
                    "sticky_entry_price_tolerance_steps": 1.0,
                }
                _remember_recovery_updates(item, updates)
                changed, backup_path = _apply_control_update(
                    symbol=normalized_symbol,
                    control_path=control_path,
                    control=control,
                    updates=updates,
                    now=now,
                    dry_run=dry_run,
                    restart_runner=restart,
                )
                action = (
                    "dry_run_switch_to_one_sided_sticky_requote"
                    if dry_run
                    else "switch_to_one_sided_sticky_requote"
                )
                item.update(
                    {
                        "status": "recovery_active",
                        "recovery_started_at": now.isoformat(),
                        "recovery_owned": True,
                        "last_recovery_action_at": now.isoformat(),
                        "last_recovery_action": action,
                    }
                )
            elif (
                severe_one_sided_stall
                and recovery_hold_satisfied
                and not recovery_reapply_debounced
                and _safe_float(control.get("sticky_entry_price_tolerance_steps")) <= 1.0
            ):
                current_gap = abs(
                    _safe_float(assessment.get("current_long_notional"))
                    - _safe_float(assessment.get("current_short_notional"))
                )
                configured_gap = _safe_float(
                    control.get("best_quote_maker_volume_inventory_bias_min_notional_gap")
                )
                relief_gap = current_gap + max(float(inventory_bias_relief_notional_margin), 0.0)
                if current_gap >= configured_gap and relief_gap > configured_gap:
                    updates = {
                        "best_quote_maker_volume_inventory_bias_min_notional_gap": relief_gap,
                    }
                    _remember_recovery_controls(item, control, tuple(updates))
                    _remember_recovery_updates(item, updates)
                    changed, backup_path = _apply_control_update(
                        symbol=normalized_symbol,
                        control_path=control_path,
                        control=control,
                        updates=updates,
                        now=now,
                        dry_run=dry_run,
                        restart_runner=restart,
                    )
                    action = (
                        "dry_run_relax_inventory_bias_after_sticky_stall"
                        if dry_run
                        else "relax_inventory_bias_after_sticky_stall"
                    )
                    item.update(
                        {
                            "status": "recovery_active",
                            "recovery_started_at": now.isoformat(),
                            "last_recovery_action_at": now.isoformat(),
                            "last_recovery_action": action,
                        }
                    )
                else:
                    action = "hold_tight_sticky_inventory_bias_relief"
                    item.update({"status": "recovery_active", "last_recovery_check_at": now.isoformat()})
            elif (
                bool(assessment.get("low_volume"))
                and recovery_hold_satisfied
                and not recovery_reapply_debounced
                and not recovery_timed_out
                and not drifted_updates
                and any(
                    key in recovery_expected_controls
                    for key in (
                        "sticky_entry_price_tolerance_steps",
                        "best_quote_maker_volume_inventory_bias_min_notional_gap",
                    )
                )
                and not effective_inventory_soft_pressure
                and not bool(assessment.get("one_sided_inventory_bias"))
                and not bool(assessment.get("near_cap"))
                and not bool(assessment.get("ineffective_orders"))
                and not bool(assessment.get("planned_reduce_only_only"))
            ):
                current_budget = _safe_float(control.get("best_quote_maker_volume_cycle_budget_notional"))
                increment = max(float(volume_recovery_cycle_budget_increment), 0.0)
                if current_budget > 0 and increment > 0:
                    _remember_recovery_controls(
                        item,
                        control,
                        ("best_quote_maker_volume_cycle_budget_notional",),
                    )
                    updates = _restore_recovery_controls(item, control, cycle_budget_floor_notional)
                    updates["best_quote_maker_volume_cycle_budget_notional"] = max(
                        current_budget + increment,
                        max(float(cycle_budget_floor_notional), 0.0),
                    )
                    item["guard_recovery_controls"] = {}
                    _remember_recovery_updates(item, updates)
                    changed, backup_path = _apply_control_update(
                        symbol=normalized_symbol,
                        control_path=control_path,
                        control=control,
                        updates=updates,
                        now=now,
                        dry_run=dry_run,
                        restart_runner=restart,
                    )
                    action = (
                        "dry_run_switch_to_cycle_budget_after_inventory_relief"
                        if dry_run
                        else "switch_to_cycle_budget_after_inventory_relief"
                    )
                    item.update(
                        {
                            "status": "recovery_active",
                            "recovery_started_at": now.isoformat(),
                            "last_recovery_action_at": now.isoformat(),
                            "last_recovery_action": action,
                        }
                    )
                else:
                    action = "hold_inventory_relief_without_budget"
                    item.update({"status": "recovery_active", "last_recovery_check_at": now.isoformat()})
            elif (
                arx_severe_volume_priority_recovery
                and bool(assessment.get("inventory_soft_pressure"))
                and not bool(control.get("best_quote_maker_volume_allow_loss_reduce_only"))
                and not confirmed_loss_reduce_wear
            ):
                updates = _loss_reduce_recovery_updates(
                    control=control,
                    assessment=assessment,
                    static_cycle_budget_floor_notional=max(
                        cycle_budget_floor_notional,
                        _safe_float(
                            control.get("best_quote_maker_volume_cycle_budget_notional")
                        ),
                    ),
                    quote_offset_extra_ticks=loss_reduce_quote_offset_extra_ticks,
                    pause_baseline_long_notional=pause_baseline_long_notional,
                    pause_baseline_short_notional=pause_baseline_short_notional,
                )
                _remember_recovery_controls(
                    item,
                    control,
                    tuple(
                        key
                        for key in updates
                        if key != "best_quote_maker_volume_net_loss_reduce_enabled"
                    ),
                )
                _remember_recovery_updates(item, updates)
                changed, backup_path = _apply_control_update(
                    symbol=normalized_symbol,
                    control_path=control_path,
                    control=control,
                    updates=updates,
                    now=now,
                    dry_run=dry_run,
                    restart_runner=restart,
                )
                action = (
                    "dry_run_enable_arx_severe_inventory_loss_recovery"
                    if dry_run
                    else "enable_arx_severe_inventory_loss_recovery"
                )
                item.update(
                    {
                        "status": "recovery_active",
                        "recovery_started_at": item.get("recovery_started_at") or now.isoformat(),
                        "recovery_owned": True,
                        "last_recovery_action_at": now.isoformat(),
                        "last_recovery_action": action,
                    }
                )
            elif (
                recovery_timed_out
                and not arx_recovery_baseline_net_limit_blocked
                and not should_hold_arx_volume_priority_release(
                    symbol=normalized_symbol,
                    target_pace_behind=target_pace_behind,
                    pace_ratio=pace_ratio,
                    allow_loss_reduce_only=bool(
                        control.get("best_quote_maker_volume_allow_loss_reduce_only")
                    ),
                    planned_reduce_only_order_count=_safe_int(
                        assessment.get("planned_reduce_only_order_count")
                    ),
                )
            ):
                updates = _restore_recovery_controls(item, control, cycle_budget_floor_notional)
                changed, backup_path = _apply_control_update(
                    symbol=normalized_symbol,
                    control_path=control_path,
                    control=control,
                    updates=updates,
                    now=now,
                    dry_run=dry_run,
                    restart_runner=restart,
                )
                action = "dry_run_recovery_timeout_cooldown" if dry_run else "recovery_timeout_cooldown"
                item.update(
                    {
                        "status": "cooldown",
                        "cooldown_until": (now + timedelta(seconds=max(float(cooldown_seconds), 0.0))).isoformat(),
                        "last_recovery_action_at": now.isoformat(),
                        "last_recovery_action": action,
                    }
                )
                item.pop("guard_original_controls", None)
                item.pop("guard_recovery_controls", None)
                item.pop("recovery_started_at", None)
                item.pop("recovery_owned", None)
            elif (
                drifted_updates
                and effective_near_market_flow
                and recovery_hold_satisfied
                and bool(recovery_expected_controls.get("best_quote_maker_volume_allow_loss_reduce_only"))
                and not bool(control.get("best_quote_maker_volume_allow_loss_reduce_only"))
            ):
                updates = _restore_recovery_controls(item, control, cycle_budget_floor_notional)
                changed, backup_path = _apply_control_update(
                    symbol=normalized_symbol,
                    control_path=control_path,
                    control=control,
                    updates=updates,
                    now=now,
                    dry_run=dry_run,
                    restart_runner=restart,
                )
                action = (
                    "dry_run_abandon_loss_reduce_for_effective_flow"
                    if dry_run
                    else "abandon_loss_reduce_for_effective_flow"
                )
                _set_post_restore_cooldown(
                    item,
                    now=now,
                    cooldown_seconds=post_restore_cooldown_seconds,
                )
                item.pop("guard_original_controls", None)
                item.pop("guard_recovery_controls", None)
                item.pop("recovery_started_at", None)
                item.pop("recovery_owned", None)
            elif (
                drifted_updates
                and not restore_ready
                and recovery_reapply_debounced
                and not arx_severe_volume_priority_recovery
                and not arx_fast_sla_capacity_reapply
            ):
                action = "hold_recovery_control_drift_debounce"
                item.update({"status": "recovery_active", "last_recovery_check_at": now.isoformat()})
            elif drifted_updates and not restore_ready:
                changed, backup_path = _apply_control_update(
                    symbol=normalized_symbol,
                    control_path=control_path,
                    control=control,
                    updates=drifted_updates,
                    now=now,
                    dry_run=dry_run,
                    restart_runner=restart,
                )
                action = (
                    "dry_run_reapply_recovery_controls_after_drift"
                    if dry_run
                    else "reapply_recovery_controls_after_drift"
                )
                item.update(
                    {
                        "status": "recovery_active",
                        "last_recovery_check_at": now.isoformat(),
                        "last_recovery_action_at": now.isoformat(),
                        "last_recovery_action": action,
                    }
                )
            elif restore_ready and not recovery_hold_satisfied:
                action = "hold_recovery_min_duration"
                item.update({"status": "recovery_active", "last_recovery_check_at": now.isoformat()})
            elif restore_ready:
                updates = _restore_recovery_controls(item, control, cycle_budget_floor_notional)
                changed, backup_path = _apply_control_update(
                    symbol=normalized_symbol,
                    control_path=control_path,
                    control=control,
                    updates=updates,
                    now=now,
                    dry_run=dry_run,
                    restart_runner=restart,
                )
                action = "dry_run_restore_recovery_controls" if dry_run else "restore_recovery_controls"
                _set_post_restore_cooldown(
                    item,
                    now=now,
                    cooldown_seconds=post_restore_cooldown_seconds,
                )
                item.pop("guard_original_controls", None)
                item.pop("guard_recovery_controls", None)
                item.pop("recovery_started_at", None)
                item.pop("recovery_owned", None)
            else:
                if should_force_arx_severe_near_maker_entry(
                    symbol=normalized_symbol,
                    target_pace_behind=target_pace_behind,
                    pace_ratio=pace_ratio,
                    planned_entry_order_count=_safe_int(
                        assessment.get("planned_entry_order_count")
                    ),
                    effective_inventory_soft_pressure=effective_inventory_soft_pressure,
                    allow_loss_reduce_only=bool(
                        control.get("best_quote_maker_volume_allow_loss_reduce_only")
                    ),
                ):
                    updates: dict[str, Any] = {
                        "best_quote_maker_volume_allow_loss_reduce_only": False,
                        "best_quote_maker_volume_net_loss_reduce_enabled": False,
                        "best_quote_maker_volume_quote_offset_ticks": 0,
                    }
                    if _safe_float(control.get("sticky_entry_price_tolerance_steps")) > 1.0:
                        updates["sticky_entry_price_tolerance_steps"] = 1.0
                    _remember_recovery_controls(
                        item,
                        control,
                        tuple(
                            key
                            for key in updates
                            if key
                            not in {
                                "best_quote_maker_volume_allow_loss_reduce_only",
                                "best_quote_maker_volume_net_loss_reduce_enabled",
                            }
                        ),
                    )
                    _remember_recovery_updates(item, updates)
                    changed, backup_path = _apply_control_update(
                        symbol=normalized_symbol,
                        control_path=control_path,
                        control=control,
                        updates=updates,
                        now=now,
                        dry_run=dry_run,
                        restart_runner=restart,
                    )
                    action = (
                        "dry_run_force_arx_near_maker_entry_after_inventory_wait"
                        if dry_run
                        else "force_arx_near_maker_entry_after_inventory_wait"
                    )
                    item.update(
                        {
                            "status": "recovery_active",
                            "recovery_owned": True,
                            "last_recovery_action_at": now.isoformat(),
                            "last_recovery_action": action,
                        }
                    )
                else:
                    action = "hold_inventory_bias_relief"
                    item.update({"status": "recovery_active", "last_recovery_check_at": now.isoformat()})
        elif bool(control.get("best_quote_maker_volume_allow_loss_reduce_only")):
            if (
                bool(control.get("best_quote_maker_volume_suppress_short_reduce_enabled"))
                and _safe_float(item.get("net_guard_recovery_baseline_notional")) <= 0
            ):
                for key in ("guard_original_controls", "guard_recovery_controls"):
                    values = item.get(key)
                    if isinstance(values, dict):
                        values.pop("best_quote_maker_volume_suppress_short_reduce_enabled", None)
                        item[key] = values
                updates = {
                    "best_quote_maker_volume_suppress_short_reduce_enabled": False,
                    "best_quote_maker_volume_net_loss_reduce_enabled": False,
                }
                changed, backup_path = _apply_control_update(
                    symbol=normalized_symbol,
                    control_path=control_path,
                    control=control,
                    updates=updates,
                    now=now,
                    dry_run=dry_run,
                    restart_runner=restart,
                )
                action = (
                    "dry_run_clear_stale_short_reduce_suppression"
                    if dry_run
                    else "clear_stale_short_reduce_suppression"
                )
                item.update(
                    {
                        "status": "recovery_active",
                        "last_recovery_action_at": now.isoformat(),
                        "last_recovery_action": action,
                    }
                )
                result = {
                    "symbol": normalized_symbol,
                    "action": action,
                    "changed_keys": changed,
                    "backup_path": backup_path,
                    "dry_run": dry_run,
                    "restart_failed": restart_failed,
                    "volume_summary": volume_summary,
                    "assessment": assessment,
                }
                _append_jsonl(
                    output_dir / "bq_volume_recovery_guard_events.jsonl",
                    {"ts": now.isoformat(), **result},
                )
                return result
            if bool(control.get("best_quote_maker_volume_net_loss_reduce_enabled")):
                updates = {"best_quote_maker_volume_net_loss_reduce_enabled": False}
                changed, backup_path = _apply_control_update(
                    symbol=normalized_symbol,
                    control_path=control_path,
                    control=control,
                    updates=updates,
                    now=now,
                    dry_run=dry_run,
                    restart_runner=restart,
                )
                action = "dry_run_disable_net_loss_reduce" if dry_run else "disable_net_loss_reduce"
                item.update({"status": "recovery_active", "last_recovery_check_at": now.isoformat()})
                result = {
                    "symbol": normalized_symbol,
                    "action": action,
                    "changed_keys": changed,
                    "backup_path": backup_path,
                    "dry_run": dry_run,
                    "restart_failed": restart_failed,
                    "volume_summary": volume_summary,
                    "assessment": assessment,
                }
                _append_jsonl(
                    output_dir / "bq_volume_recovery_guard_events.jsonl",
                    {"ts": now.isoformat(), **result},
                )
                return result
            recovery_started_at = _parse_time(item.get("recovery_started_at"))
            recovery_stage_timed_out = (
                recovery_started_at is not None
                and (now - recovery_started_at).total_seconds() >= max(float(max_recovery_seconds), 0.0)
            )
            extension_count = _safe_int(item.get("soft_recovery_extension_count"))
            cap_buffer_ok = _recovery_inventory_buffer_ok(
                assessment,
                recover_cap_ratio=recover_cap_ratio,
                original_controls=recovery_original_controls,
                entry_reserve_notional=recovery_entry_reserve_notional,
            )
            if bool(item.get("recovery_owned")) and cap_buffer_ok and recovery_hold_satisfied:
                updates = _restore_recovery_controls(item, control, cycle_budget_floor_notional)
                changed, backup_path = _apply_control_update(
                    symbol=normalized_symbol,
                    control_path=control_path,
                    control=control,
                    updates=updates,
                    now=now,
                    dry_run=dry_run,
                    restart_runner=restart,
                )
                action = (
                    "dry_run_restore_after_inventory_below_soft"
                    if dry_run
                    else "restore_after_inventory_below_soft"
                )
                _set_post_restore_cooldown(
                    item,
                    now=now,
                    cooldown_seconds=post_restore_cooldown_seconds,
                )
                item.pop("guard_original_controls", None)
                item.pop("guard_recovery_controls", None)
                item.pop("recovery_started_at", None)
                item.pop("recovery_owned", None)
                result = {
                    "symbol": normalized_symbol,
                    "action": action,
                    "changed_keys": changed,
                    "backup_path": backup_path,
                    "dry_run": dry_run,
                    "restart_failed": restart_failed,
                    "volume_summary": volume_summary,
                    "assessment": assessment,
                }
                _append_jsonl(
                    output_dir / "bq_volume_recovery_guard_events.jsonl",
                    {"ts": now.isoformat(), **result},
                )
                return result
            current_long = max(_safe_float(assessment.get("current_long_notional")), 0.0)
            current_short = max(_safe_float(assessment.get("current_short_notional")), 0.0)
            heavier_inventory = max(current_long, current_short)
            lighter_inventory = min(current_long, current_short)
            buffered_long_soft = max(
                float(pause_baseline_long_notional),
                _safe_float(assessment.get("long_soft_limit_notional")),
            ) * min(max(float(recover_cap_ratio), 0.0), 1.0)
            buffered_short_soft = max(
                float(pause_baseline_short_notional),
                _safe_float(assessment.get("short_soft_limit_notional")),
            ) * min(max(float(recover_cap_ratio), 0.0), 1.0)
            soft_hysteresis_bridge_updates: dict[str, Any] = {}
            if (
                target_pace_behind
                and recovery_hold_satisfied
                and not high_recovery_wear
                and (not recovery_reapply_debounced or no_fill_seconds >= 300.0)
                and actual_inventory_below_soft
                and not cap_buffer_ok
                and _safe_int(assessment.get("planned_reduce_only_order_count")) <= 0
                and not bool(assessment.get("volatility_entry_pause_active"))
            ):
                if (
                    buffered_long_soft > 0
                    and current_long >= buffered_long_soft
                    and current_long > current_short
                    and _safe_float(control.get("pause_buy_position_notional"))
                    > buffered_long_soft
                ):
                    soft_hysteresis_bridge_updates["pause_buy_position_notional"] = (
                        buffered_long_soft
                    )
                elif (
                    buffered_short_soft > 0
                    and current_short >= buffered_short_soft
                    and current_short > current_long
                    and _safe_float(control.get("pause_short_position_notional"))
                    > buffered_short_soft
                ):
                    soft_hysteresis_bridge_updates["pause_short_position_notional"] = (
                        buffered_short_soft
                    )
            if soft_hysteresis_bridge_updates:
                updates = {
                    **soft_hysteresis_bridge_updates,
                    "best_quote_maker_volume_net_loss_reduce_enabled": False,
                }
                _remember_recovery_controls(item, control, tuple(soft_hysteresis_bridge_updates))
                _remember_recovery_updates(item, updates)
                changed, backup_path = _apply_control_update(
                    symbol=normalized_symbol,
                    control_path=control_path,
                    control=control,
                    updates=updates,
                    now=now,
                    dry_run=dry_run,
                    restart_runner=restart,
                )
                action = (
                    "dry_run_bridge_soft_hysteresis_gap_for_recovery"
                    if dry_run
                    else "bridge_soft_hysteresis_gap_for_recovery"
                )
                item.update(
                    {
                        "status": "recovery_active",
                        "last_recovery_check_at": now.isoformat(),
                        "last_recovery_action_at": now.isoformat(),
                        "last_recovery_action": action,
                    }
                )
                result = {
                    "symbol": normalized_symbol,
                    "action": action,
                    "changed_keys": changed,
                    "backup_path": backup_path,
                    "dry_run": dry_run,
                    "restart_failed": restart_failed,
                    "volume_summary": volume_summary,
                    "assessment": assessment,
                }
                _append_jsonl(
                    output_dir / "bq_volume_recovery_guard_events.jsonl",
                    {"ts": now.isoformat(), **result},
                )
                return result
            pair_reduce_min_side = max(
                _safe_float(control.get("best_quote_maker_volume_active_pair_reduce_min_side_notional")),
                0.0,
            )
            current_bias_reduce_share = max(
                _safe_float(control.get("best_quote_maker_volume_inventory_bias_reduce_share")),
                0.0,
            )
            stalled_directional_loss_reduce_plan = (
                bool(assessment.get("low_volume"))
                and effective_inventory_soft_pressure
                and _safe_int(assessment.get("planned_reduce_only_order_count")) <= 0
                and current_long > 0
                and current_short > 0
                and lighter_inventory < heavier_inventory * 0.5
                and not bool(assessment.get("volatility_entry_pause_active"))
                and "best_quote_maker_volume_inventory_bias_reduce_share" in control
                and current_bias_reduce_share < 0.25
                and extension_count >= max(int(max_soft_recovery_extensions), 0)
                and not cap_buffer_ok
                and recovery_hold_satisfied
                and last_recovery_action_at is not None
                and not recovery_reapply_debounced
            )
            if stalled_directional_loss_reduce_plan:
                updates = {"best_quote_maker_volume_inventory_bias_reduce_share": 0.25}
                _remember_recovery_controls(item, control, tuple(updates))
                _remember_recovery_updates(item, updates)
                changed, backup_path = _apply_control_update(
                    symbol=normalized_symbol,
                    control_path=control_path,
                    control=control,
                    updates=updates,
                    now=now,
                    dry_run=dry_run,
                    restart_runner=restart,
                )
                action = (
                    "dry_run_enable_bounded_dominant_leg_reduce_for_stalled_loss_recovery"
                    if dry_run
                    else "enable_bounded_dominant_leg_reduce_for_stalled_loss_recovery"
                )
                item.update(
                    {
                        "status": "recovery_active",
                        "last_recovery_check_at": now.isoformat(),
                        "last_recovery_action_at": now.isoformat(),
                        "last_recovery_action": action,
                    }
                )
                result = {
                    "symbol": normalized_symbol,
                    "action": action,
                    "changed_keys": changed,
                    "backup_path": backup_path,
                    "dry_run": dry_run,
                    "restart_failed": restart_failed,
                    "volume_summary": volume_summary,
                    "assessment": assessment,
                }
                _append_jsonl(
                    output_dir / "bq_volume_recovery_guard_events.jsonl",
                    {"ts": now.isoformat(), **result},
                )
                return result
            stalled_loss_reduce_plan = (
                bool(assessment.get("low_volume"))
                and effective_inventory_soft_pressure
                and _safe_int(assessment.get("planned_reduce_only_order_count")) <= 0
                and current_long > 0
                and current_short > 0
                and lighter_inventory >= pair_reduce_min_side
                and lighter_inventory >= heavier_inventory * 0.5
                and not bool(assessment.get("volatility_entry_pause_active"))
                and not bool(control.get("best_quote_maker_volume_active_pair_reduce_enabled"))
                and extension_count >= max(int(max_soft_recovery_extensions), 0)
                and not cap_buffer_ok
                and recovery_hold_satisfied
                and last_recovery_action_at is not None
                and not recovery_reapply_debounced
            )
            if stalled_loss_reduce_plan:
                updates = {"best_quote_maker_volume_active_pair_reduce_enabled": True}
                _remember_recovery_controls(item, control, tuple(updates))
                _remember_recovery_updates(item, updates)
                changed, backup_path = _apply_control_update(
                    symbol=normalized_symbol,
                    control_path=control_path,
                    control=control,
                    updates=updates,
                    now=now,
                    dry_run=dry_run,
                    restart_runner=restart,
                )
                action = (
                    "dry_run_enable_bounded_pair_reduce_for_stalled_loss_recovery"
                    if dry_run
                    else "enable_bounded_pair_reduce_for_stalled_loss_recovery"
                )
                item.update(
                    {
                        "status": "recovery_active",
                        "last_recovery_check_at": now.isoformat(),
                        "last_recovery_action_at": now.isoformat(),
                        "last_recovery_action": action,
                    }
                )
                result = {
                    "symbol": normalized_symbol,
                    "action": action,
                    "changed_keys": changed,
                    "backup_path": backup_path,
                    "dry_run": dry_run,
                    "restart_failed": restart_failed,
                    "volume_summary": volume_summary,
                    "assessment": assessment,
                }
                _append_jsonl(
                    output_dir / "bq_volume_recovery_guard_events.jsonl",
                    {"ts": now.isoformat(), **result},
                )
                return result
            can_extend_soft_recovery = (
                recovery_stage_timed_out
                and effective_inventory_soft_pressure
                and _safe_int(assessment.get("active_order_count")) > 0
                and _safe_int(assessment.get("near_market_order_count")) > 0
                and not bool(assessment.get("ineffective_orders"))
                and not cap_buffer_ok
                and extension_count < max(int(max_soft_recovery_extensions), 0)
            )
            if can_extend_soft_recovery:
                action = "extend_soft_inventory_loss_recovery"
                extension_updates = dict(loss_reduce_flow_updates)
                extension_updates.update(
                    arx_soft_recovery_extension_updates(
                        control=control,
                        assessment=assessment,
                        volume_summary=volume_summary,
                    )
                )
                if extension_updates:
                    _remember_recovery_controls(
                        item,
                        control,
                        tuple(extension_updates),
                    )
                    _remember_recovery_updates(item, extension_updates)
                    changed, backup_path = _apply_control_update(
                        symbol=normalized_symbol,
                        control_path=control_path,
                        control=control,
                        updates=extension_updates,
                        now=now,
                        dry_run=dry_run,
                        restart_runner=restart,
                    )
                item.update(
                    {
                        "status": "recovery_active",
                        "recovery_started_at": now.isoformat(),
                        "soft_recovery_extension_count": extension_count + 1,
                        "last_recovery_check_at": now.isoformat(),
                        "last_recovery_action_at": now.isoformat(),
                        "last_recovery_action": action,
                    }
                )
                result = {
                    "symbol": normalized_symbol,
                    "action": action,
                    "changed_keys": changed,
                    "backup_path": backup_path,
                    "dry_run": dry_run,
                    "restart_failed": restart_failed,
                    "volume_summary": volume_summary,
                    "assessment": assessment,
                }
                _append_jsonl(
                    output_dir / "bq_volume_recovery_guard_events.jsonl",
                    {"ts": now.isoformat(), **result},
                )
                return result
            hold_exhausted_soft_recovery = (
                recovery_stage_timed_out
                and effective_inventory_soft_pressure
                and _safe_int(assessment.get("active_order_count")) > 0
                and _safe_int(assessment.get("near_market_order_count")) > 0
                and not bool(assessment.get("ineffective_orders"))
                and not cap_buffer_ok
            )
            if hold_exhausted_soft_recovery:
                current_budget = _safe_float(
                    control.get("best_quote_maker_volume_cycle_budget_notional")
                )
                target_budget_floor = max(
                    _safe_float(volume_summary.get("target_cycle_budget_floor_notional")),
                    float(cycle_budget_floor_notional),
                    _safe_float(
                        control.get("best_quote_maker_volume_min_cycle_budget_notional")
                    ),
                )
                target_budget = min(
                    target_budget_floor,
                    current_budget
                    + max(float(volume_recovery_cycle_budget_increment), 0.0),
                )
                dominant_side_reduce_present = (
                    current_long > current_short
                    and _safe_int(assessment.get("planned_reduce_long_order_count")) > 0
                ) or (
                    current_short > current_long
                    and _safe_int(assessment.get("planned_reduce_short_order_count")) > 0
                )
                exhausted_balancing_flow = (
                    bool(assessment.get("balancing_entry_only"))
                    and dominant_side_reduce_present
                    and _safe_int(assessment.get("near_market_entry_order_count")) > 0
                    and _safe_int(assessment.get("near_market_reduce_only_order_count")) > 0
                )
                can_raise_exhausted_recovery_budget = (
                    target_pace_behind
                    and not confirmed_loss_reduce_wear
                    and not recovery_reapply_debounced
                    and (
                        bool(assessment.get("planned_reduce_only_only"))
                        or exhausted_balancing_flow
                    )
                    and _safe_int(assessment.get("near_market_reduce_only_order_count")) > 0
                    and target_budget > current_budget
                )
                if can_raise_exhausted_recovery_budget:
                    updates = {
                        "best_quote_maker_volume_net_loss_reduce_enabled": False,
                        "best_quote_maker_volume_cycle_budget_notional": target_budget,
                    }
                    _remember_recovery_controls(
                        item,
                        control,
                        ("best_quote_maker_volume_cycle_budget_notional",),
                    )
                    _remember_recovery_updates(item, updates)
                    changed, backup_path = _apply_control_update(
                        symbol=normalized_symbol,
                        control_path=control_path,
                        control=control,
                        updates=updates,
                        now=now,
                        dry_run=dry_run,
                        restart_runner=restart,
                    )
                    action = (
                        "dry_run_raise_exhausted_soft_recovery_budget_for_pace"
                        if dry_run
                        else "raise_exhausted_soft_recovery_budget_for_pace"
                    )
                    item.update(
                        {
                            "status": "recovery_active",
                            "recovery_started_at": now.isoformat(),
                            "last_recovery_check_at": now.isoformat(),
                            "last_recovery_action_at": now.isoformat(),
                            "last_recovery_action": action,
                        }
                    )
                    result = {
                        "symbol": normalized_symbol,
                        "action": action,
                        "changed_keys": changed,
                        "backup_path": backup_path,
                        "dry_run": dry_run,
                        "restart_failed": restart_failed,
                        "volume_summary": volume_summary,
                        "assessment": assessment,
                    }
                    _append_jsonl(
                        output_dir / "bq_volume_recovery_guard_events.jsonl",
                        {"ts": now.isoformat(), **result},
                    )
                    return result
                can_tighten_exhausted_recovery = (
                    target_pace_behind
                    and (sla_recovery_due or no_fill_seconds >= 300.0)
                    and not confirmed_loss_reduce_wear
                    and not recovery_reapply_debounced
                    and _safe_int(assessment.get("planned_reduce_only_order_count")) > 0
                    and _safe_int(control.get("best_quote_maker_volume_quote_offset_ticks")) > 1
                )
                if can_tighten_exhausted_recovery:
                    updates = {
                        "best_quote_maker_volume_net_loss_reduce_enabled": False,
                        "best_quote_maker_volume_quote_offset_ticks": max(
                            _safe_int(control.get("best_quote_maker_volume_quote_offset_ticks")) - 1,
                            1,
                        ),
                    }
                    _remember_recovery_controls(
                        item,
                        control,
                        ("best_quote_maker_volume_quote_offset_ticks",),
                    )
                    _remember_recovery_updates(item, updates)
                    changed, backup_path = _apply_control_update(
                        symbol=normalized_symbol,
                        control_path=control_path,
                        control=control,
                        updates=updates,
                        now=now,
                        dry_run=dry_run,
                        restart_runner=restart,
                    )
                    action = (
                        "dry_run_tighten_exhausted_soft_recovery_one_tick"
                        if dry_run
                        else "tighten_exhausted_soft_recovery_one_tick"
                    )
                    item.update(
                        {
                            "status": "recovery_active",
                            "recovery_started_at": now.isoformat(),
                            "last_recovery_check_at": now.isoformat(),
                            "last_recovery_action_at": now.isoformat(),
                            "last_recovery_action": action,
                        }
                    )
                    result = {
                        "symbol": normalized_symbol,
                        "action": action,
                        "changed_keys": changed,
                        "backup_path": backup_path,
                        "dry_run": dry_run,
                        "restart_failed": restart_failed,
                        "volume_summary": volume_summary,
                        "assessment": assessment,
                    }
                    _append_jsonl(
                        output_dir / "bq_volume_recovery_guard_events.jsonl",
                        {"ts": now.isoformat(), **result},
                    )
                    return result
                action = "hold_soft_inventory_loss_recovery_until_both_below"
                item.update(
                    {
                        "status": "recovery_active",
                        "last_recovery_check_at": now.isoformat(),
                        "last_recovery_action": action,
                    }
                )
                result = {
                    "symbol": normalized_symbol,
                    "action": action,
                    "changed_keys": changed,
                    "backup_path": backup_path,
                    "dry_run": dry_run,
                    "restart_failed": restart_failed,
                    "volume_summary": volume_summary,
                    "assessment": assessment,
                }
                _append_jsonl(
                    output_dir / "bq_volume_recovery_guard_events.jsonl",
                    {"ts": now.isoformat(), **result},
                )
                return result
            hold_soft_hysteresis_bridge = (
                recovery_stage_timed_out
                and target_pace_behind
                and not high_recovery_wear
                and actual_inventory_below_soft
                and not cap_buffer_ok
                and (
                    _safe_float(control.get("pause_buy_position_notional"))
                    < pause_baseline_long_notional
                    or _safe_float(control.get("pause_short_position_notional"))
                    < pause_baseline_short_notional
                )
            )
            if hold_soft_hysteresis_bridge:
                updates = {"best_quote_maker_volume_net_loss_reduce_enabled": False}
                current_budget = _safe_float(
                    control.get("best_quote_maker_volume_cycle_budget_notional")
                )
                configured_budget_floor = max(
                    _safe_float(
                        control.get("best_quote_maker_volume_min_cycle_budget_notional")
                    ),
                    _safe_float(volume_summary.get("target_cycle_budget_floor_notional")),
                    float(cycle_budget_floor_notional),
                )
                target_budget = min(
                    configured_budget_floor,
                    current_budget
                    + max(float(volume_recovery_cycle_budget_increment), 0.0),
                )
                if (
                    _safe_int(assessment.get("planned_entry_order_count")) <= 0
                    and _safe_int(assessment.get("near_market_reduce_only_order_count")) > 0
                    and target_budget > current_budget
                ):
                    updates["best_quote_maker_volume_cycle_budget_notional"] = target_budget
                    _remember_recovery_controls(
                        item,
                        control,
                        ("best_quote_maker_volume_cycle_budget_notional",),
                    )
                if (
                    (
                        bool(assessment.get("balancing_entry_only"))
                        or critical_ousdt_reduce_budget_ramp
                    )
                    and _safe_float(control.get("sticky_entry_price_tolerance_steps")) > 1.0
                ):
                    updates["sticky_entry_price_tolerance_steps"] = 1.0
                    _remember_recovery_controls(
                        item,
                        control,
                        ("sticky_entry_price_tolerance_steps",),
                    )
                _remember_recovery_updates(item, updates)
                changed, backup_path = _apply_control_update(
                    symbol=normalized_symbol,
                    control_path=control_path,
                    control=control,
                    updates=updates,
                    now=now,
                    dry_run=dry_run,
                    restart_runner=restart,
                )
                action = (
                    "dry_run_raise_budget_while_holding_soft_hysteresis_bridge"
                    if dry_run
                    and "best_quote_maker_volume_cycle_budget_notional" in updates
                    else "raise_budget_while_holding_soft_hysteresis_bridge"
                    if "best_quote_maker_volume_cycle_budget_notional" in updates
                    else "dry_run_hold_soft_hysteresis_bridge_until_buffer"
                    if dry_run
                    else "hold_soft_hysteresis_bridge_until_buffer"
                )
                item.update(
                    {
                        "status": "recovery_active",
                        "recovery_started_at": now.isoformat(),
                        "last_recovery_check_at": now.isoformat(),
                        "last_recovery_action_at": now.isoformat(),
                        "last_recovery_action": action,
                    }
                )
                return_result = {
                    "symbol": normalized_symbol,
                    "action": action,
                    "changed_keys": changed,
                    "backup_path": backup_path,
                    "dry_run": dry_run,
                    "restart_failed": restart_failed,
                    "volume_summary": volume_summary,
                    "assessment": assessment,
                }
                _append_jsonl(
                    output_dir / "bq_volume_recovery_guard_events.jsonl",
                    {"ts": now.isoformat(), **return_result},
                )
                return return_result
            if (
                recovery_stage_timed_out
                and not arx_recovery_baseline_net_limit_blocked
                and not should_hold_arx_volume_priority_release(
                    symbol=normalized_symbol,
                    target_pace_behind=target_pace_behind,
                    pace_ratio=pace_ratio,
                    allow_loss_reduce_only=bool(
                        control.get("best_quote_maker_volume_allow_loss_reduce_only")
                    ),
                    planned_reduce_only_order_count=_safe_int(
                        assessment.get("planned_reduce_only_order_count")
                    ),
                )
            ):
                updates = _restore_recovery_controls(item, control, cycle_budget_floor_notional)
                changed, backup_path = _apply_control_update(
                    symbol=normalized_symbol,
                    control_path=control_path,
                    control=control,
                    updates=updates,
                    now=now,
                    dry_run=dry_run,
                    restart_runner=restart,
                )
                action = "dry_run_recovery_timeout_cooldown" if dry_run else "recovery_timeout_cooldown"
                item.update(
                    {
                        "status": "cooldown",
                        "cooldown_until": (now + timedelta(seconds=max(float(cooldown_seconds), 0.0))).isoformat(),
                        "last_recovery_action_at": now.isoformat(),
                        "last_recovery_action": action,
                    }
                )
                item.pop("guard_original_controls", None)
                item.pop("guard_recovery_controls", None)
                item.pop("recovery_started_at", None)
                item.pop("recovery_owned", None)
                return_result = {
                    "symbol": normalized_symbol,
                    "action": action,
                    "changed_keys": changed,
                    "backup_path": backup_path,
                    "dry_run": dry_run,
                    "restart_failed": restart_failed,
                    "volume_summary": volume_summary,
                    "assessment": assessment,
                }
                _append_jsonl(output_dir / "bq_volume_recovery_guard_events.jsonl", {"ts": now.isoformat(), **return_result})
                return return_result
            restore_ready = should_restore_near_market_recovery(
                symbol=normalized_symbol,
                effective_near_market_flow=effective_near_market_flow,
                target_pace_behind=target_pace_behind,
            )
            if restore_ready and not recovery_hold_satisfied:
                action = "hold_recovery_min_duration"
                item.update({"status": "recovery_active", "last_recovery_check_at": now.isoformat()})
            elif restore_ready:
                has_original_controls = bool(item.get("guard_original_controls"))
                updates = _restore_recovery_controls(item, control, cycle_budget_floor_notional)
                changed, backup_path = _apply_control_update(
                    symbol=normalized_symbol,
                    control_path=control_path,
                    control=control,
                    updates=updates,
                    now=now,
                    dry_run=dry_run,
                    restart_runner=restart,
                )
                if has_original_controls:
                    action = "dry_run_restore_recovery_controls" if dry_run else "restore_recovery_controls"
                else:
                    action = "dry_run_disable_allow_loss_after_recovery" if dry_run else "disable_allow_loss_after_recovery"
                _set_post_restore_cooldown(
                    item,
                    now=now,
                    cooldown_seconds=post_restore_cooldown_seconds,
                )
                item.pop("guard_original_controls", None)
                item.pop("guard_recovery_controls", None)
                item.pop("recovery_started_at", None)
                item.pop("recovery_owned", None)
            elif not cap_buffer_ok:
                current_budget = _safe_float(
                    control.get("best_quote_maker_volume_cycle_budget_notional")
                )
                target_budget_floor = max(
                    _safe_float(volume_summary.get("target_cycle_budget_floor_notional")),
                    float(cycle_budget_floor_notional),
                    _safe_float(
                        control.get("best_quote_maker_volume_min_cycle_budget_notional")
                    ),
                )
                target_budget = min(
                    target_budget_floor,
                    current_budget + max(float(volume_recovery_cycle_budget_increment), 0.0),
                )
                current_long = _safe_float(assessment.get("current_long_notional"))
                current_short = _safe_float(assessment.get("current_short_notional"))
                dominant_side_reduce_present = (
                    current_long > current_short
                    and _safe_int(assessment.get("planned_reduce_long_order_count")) > 0
                ) or (
                    current_short > current_long
                    and _safe_int(assessment.get("planned_reduce_short_order_count")) > 0
                )
                cap_pressure_balancing_flow = (
                    bool(assessment.get("balancing_entry_only"))
                    and dominant_side_reduce_present
                    and _safe_int(assessment.get("near_market_entry_order_count")) > 0
                    and _safe_int(assessment.get("near_market_reduce_only_order_count")) > 0
                )
                if (
                    target_pace_behind
                    and (
                        not recovery_reapply_debounced
                        or critical_ousdt_reduce_budget_ramp
                    )
                    and not high_recovery_wear
                    and bool(control.get("best_quote_maker_volume_allow_loss_reduce_only"))
                    and (
                        (
                            bool(assessment.get("planned_reduce_only_only"))
                            and _safe_int(
                                assessment.get("near_market_reduce_only_order_count")
                            )
                            > 0
                        )
                        or cap_pressure_balancing_flow
                    )
                    and target_budget > current_budget
                ):
                    updates = {
                        "best_quote_maker_volume_net_loss_reduce_enabled": False,
                        "best_quote_maker_volume_cycle_budget_notional": target_budget,
                    }
                    _remember_recovery_controls(
                        item,
                        control,
                        ("best_quote_maker_volume_cycle_budget_notional",),
                    )
                    _remember_recovery_updates(item, updates)
                    action = (
                        "dry_run_raise_cap_pressure_balancing_budget_for_pace"
                        if dry_run and cap_pressure_balancing_flow
                        else "raise_cap_pressure_balancing_budget_for_pace"
                        if cap_pressure_balancing_flow
                        else "dry_run_raise_cap_pressure_reduce_budget_for_pace"
                        if dry_run
                        else "raise_cap_pressure_reduce_budget_for_pace"
                    )
                    item.update(
                        {
                            "status": "recovery_active",
                            "last_recovery_check_at": now.isoformat(),
                            "last_recovery_action_at": now.isoformat(),
                            "last_recovery_action": action,
                        }
                    )
                    changed, backup_path = _apply_control_update(
                        symbol=normalized_symbol,
                        control_path=control_path,
                        control=control,
                        updates=updates,
                        now=now,
                        dry_run=dry_run,
                        restart_runner=restart,
                    )
                else:
                    arx_extension_updates = arx_soft_recovery_extension_updates(
                        control=control,
                        assessment=assessment,
                        volume_summary=volume_summary,
                    )
                    if arx_extension_updates:
                        _remember_recovery_controls(
                            item,
                            control,
                            tuple(arx_extension_updates),
                        )
                        _remember_recovery_updates(item, arx_extension_updates)
                        action = (
                            "dry_run_raise_arx_cap_pressure_maker_flow"
                            if dry_run
                            else "raise_arx_cap_pressure_maker_flow"
                        )
                        item.update(
                            {
                                "status": "recovery_active",
                                "last_recovery_check_at": now.isoformat(),
                                "last_recovery_action_at": now.isoformat(),
                                "last_recovery_action": action,
                            }
                        )
                        changed, backup_path = _apply_control_update(
                            symbol=normalized_symbol,
                            control_path=control_path,
                            control=control,
                            updates=arx_extension_updates,
                            now=now,
                            dry_run=dry_run,
                            restart_runner=restart,
                        )
                    else:
                        action = "hold_recovery_until_cap_buffer"
                        item.update(
                            {"status": "recovery_active", "last_recovery_check_at": now.isoformat()}
                        )
            else:
                action = "hold_recovery_until_volume_or_orders"
                item.update({"status": "recovery_active", "last_recovery_check_at": now.isoformat()})
        elif (
            normalized_symbol == "OUSDT"
            and target_pace_behind
            and pace_ratio < 0.75
            and low_pace_seconds >= max(float(trigger_seconds), 120.0)
            and bool(assessment.get("low_volume"))
            and bool(assessment.get("planned_reduce_only_only"))
            and _safe_int(assessment.get("planned_reduce_only_order_count")) > 0
            and not bool(control.get("best_quote_maker_volume_allow_loss_reduce_only"))
            and _safe_int(control.get("best_quote_maker_volume_quote_offset_ticks")) <= 1
            and _safe_float(control.get("sticky_exit_price_tolerance_steps")) > 1.0
            and _safe_float(assessment.get("nearest_reduce_only_distance_ticks")) > 1.0
            and _safe_float(volume_summary.get("gross_notional"))
            <= _safe_float(volume_summary.get("effective_min_volume_notional")) * 0.25
        ):
            updates = {
                "best_quote_maker_volume_net_loss_reduce_enabled": False,
                "best_quote_maker_volume_quote_offset_ticks": 0,
                "sticky_exit_price_tolerance_steps": 1.0,
            }
            _remember_recovery_controls(
                item,
                control,
                (
                    "best_quote_maker_volume_quote_offset_ticks",
                    "sticky_exit_price_tolerance_steps",
                ),
            )
            _remember_recovery_updates(item, updates)
            action = (
                "dry_run_tighten_sticky_exit_for_low_pace_reduce_only_flow"
                if dry_run
                else "tighten_sticky_exit_for_low_pace_reduce_only_flow"
            )
            item.update(
                {
                    "status": "recovery_active",
                    "recovery_started_at": now.isoformat(),
                    "recovery_owned": True,
                    "last_recovery_action_at": now.isoformat(),
                    "last_recovery_action": action,
                }
            )
            changed, backup_path = _apply_control_update(
                symbol=normalized_symbol,
                control_path=control_path,
                control=control,
                updates=updates,
                now=now,
                dry_run=dry_run,
                restart_runner=restart,
            )
        elif (
            target_pace_behind
            and pace_ratio < 0.75
            and not recovery_reapply_debounced
            and not post_restore_budget_cooldown_active
            and (
                critical_arx_inventory_pace_override
                or (not high_recovery_wear and not confirmed_loss_reduce_wear)
            )
            and effective_inventory_soft_pressure
            and not bool(control.get("best_quote_maker_volume_allow_loss_reduce_only"))
            and _safe_int(assessment.get("planned_reduce_only_order_count")) > 0
            and _safe_int(assessment.get("near_market_reduce_only_order_count")) > 0
        ):
            current_budget = _safe_float(
                control.get("best_quote_maker_volume_cycle_budget_notional")
            )
            updates = _loss_reduce_recovery_updates(
                control=control,
                assessment=assessment,
                static_cycle_budget_floor_notional=max(
                    float(cycle_budget_floor_notional),
                    current_budget,
                ),
                quote_offset_extra_ticks=loss_reduce_quote_offset_extra_ticks,
                pause_baseline_long_notional=pause_baseline_long_notional,
                pause_baseline_short_notional=pause_baseline_short_notional,
            )
            _remember_recovery_controls(
                item,
                control,
                tuple(
                    key
                    for key in updates
                    if key != "best_quote_maker_volume_net_loss_reduce_enabled"
                ),
            )
            _remember_recovery_updates(item, updates)
            action_name = (
                "enable_critical_arx_inventory_loss_reduce_for_pace"
                if critical_arx_inventory_pace_override
                else "enable_cap_pressure_loss_reduce_for_pace"
            )
            action = f"dry_run_{action_name}" if dry_run else action_name
            item.update(
                {
                    "status": "recovery_active",
                    "recovery_started_at": now.isoformat(),
                    "soft_recovery_extension_count": 0,
                    "recovery_owned": True,
                    "last_recovery_check_at": now.isoformat(),
                    "last_recovery_action_at": now.isoformat(),
                    "last_recovery_action": action,
                }
            )
            changed, backup_path = _apply_control_update(
                symbol=normalized_symbol,
                control_path=control_path,
                control=control,
                updates=updates,
                now=now,
                dry_run=dry_run,
                restart_runner=restart,
            )
        elif not recovery_low_volume:
            action = "volume_normal"
            item.clear()
            item.update({"status": "normal", "last_normal_at": now.isoformat(), "last_assessment": assessment})
        else:
            first_low = _parse_time(item.get("first_low_volume_at")) or now
            elapsed = (now - first_low).total_seconds()
            item.update(
                {
                    "status": "low_volume",
                    "first_low_volume_at": first_low.isoformat(),
                    "last_low_volume_at": now.isoformat(),
                    "elapsed_seconds": elapsed,
                    "last_assessment": assessment,
                }
            )
            if elapsed < max(float(trigger_seconds), 0.0):
                action = "wait_low_volume_confirmation"
            elif (
                recovery_reapply_debounced
                and not effective_inventory_soft_pressure
                and not arx_severe_volume_priority_recovery
            ):
                action = "hold_non_safety_recovery_debounce"
            elif (
                severe_one_sided_stall
                and not recovery_reapply_debounced
                and _safe_float(control.get("sticky_entry_price_tolerance_steps")) > 1.0
            ):
                _remember_recovery_controls(
                    item,
                    control,
                    ("sticky_entry_price_tolerance_steps",),
                )
                updates = {
                    "best_quote_maker_volume_net_loss_reduce_enabled": False,
                    "sticky_entry_price_tolerance_steps": 1.0,
                }
                _remember_recovery_updates(item, updates)
                action = "dry_run_tighten_sticky_for_one_sided_stall" if dry_run else "tighten_sticky_for_one_sided_stall"
                item.update(
                    {
                        "status": "recovery_active",
                        "recovery_started_at": item.get("recovery_started_at") or now.isoformat(),
                        "recovery_owned": True,
                        "last_recovery_action_at": now.isoformat(),
                        "last_recovery_action": action,
                    }
                )
                changed, backup_path = _apply_control_update(
                    symbol=normalized_symbol,
                    control_path=control_path,
                    control=control,
                    updates=updates,
                    now=now,
                    dry_run=dry_run,
                    restart_runner=restart,
                )
            elif (
                effective_inventory_soft_pressure
                and not effective_near_market_flow
                and not high_recovery_wear
                and not confirmed_loss_reduce_wear
                and not recovery_reapply_debounced
                and not post_restore_budget_cooldown_active
            ):
                updates = _loss_reduce_recovery_updates(
                    control=control,
                    assessment=assessment,
                    static_cycle_budget_floor_notional=(
                        max(
                            cycle_budget_floor_notional,
                            _safe_float(
                                control.get(
                                    "best_quote_maker_volume_cycle_budget_notional"
                                )
                            ),
                        )
                        if target_pace_behind
                        else static_cycle_budget_floor_notional
                    ),
                    quote_offset_extra_ticks=loss_reduce_quote_offset_extra_ticks,
                    pause_baseline_long_notional=pause_baseline_long_notional,
                    pause_baseline_short_notional=pause_baseline_short_notional,
                )
                _remember_recovery_controls(
                    item,
                    control,
                    tuple(key for key in updates if key != "best_quote_maker_volume_net_loss_reduce_enabled"),
                )
                _remember_recovery_updates(item, updates)
                action = "dry_run_enable_soft_inventory_loss_reduce" if dry_run else "enable_soft_inventory_loss_reduce"
                item.update(
                    {
                        "status": "recovery_active",
                        "recovery_started_at": item.get("recovery_started_at") or now.isoformat(),
                        "soft_recovery_extension_count": 0,
                        "recovery_owned": True,
                        "last_recovery_action_at": now.isoformat(),
                        "last_recovery_action": action,
                    }
                )
                changed, backup_path = _apply_control_update(
                    symbol=normalized_symbol,
                    control_path=control_path,
                    control=control,
                    updates=updates,
                    now=now,
                    dry_run=dry_run,
                    restart_runner=restart,
                )
            elif (
                actual_inventory_below_soft
                and not bool(assessment.get("volatility_inventory_reduce_deadlock"))
                and (required_hourly_notional <= 0 or target_pace_ahead)
            ):
                action = "hold_loss_reduce_when_actual_inventory_below_soft"
                item.update(
                    {
                        "status": "low_volume",
                        "last_recovery_check_at": now.isoformat(),
                        "last_recovery_action": action,
                    }
                )
            elif (
                normalized_symbol == "ARXUSDT"
                and target_pace_behind
                and sla_recovery_due
                and no_fill_seconds >= fast_sla_seconds
                and bool(assessment.get("planned_reduce_only_only"))
                and _safe_int(assessment.get("near_market_order_count")) > 0
                and bool(control.get("best_quote_maker_volume_same_side_entry_price_guard_enabled"))
                and not bool(control.get("best_quote_maker_volume_same_side_entry_price_guard_report_only"))
                and not bool(assessment.get("near_cap"))
                and not high_recovery_wear
                and not recovery_reapply_debounced
            ):
                current_long = max(_safe_float(assessment.get("current_long_notional")), 0.0)
                current_short = max(_safe_float(assessment.get("current_short_notional")), 0.0)
                lighter_inventory = min(current_long, current_short)
                heavier_inventory = max(current_long, current_short)
                order_notional = max(
                    _safe_float(control.get("per_order_notional")),
                    _safe_float(control.get("maker_order_notional")),
                    25.0,
                )
                current_gate = max(
                    _safe_float(
                        control.get("best_quote_maker_volume_same_side_entry_price_guard_min_notional")
                    ),
                    200.0,
                )
                target_gate = min(
                    max(lighter_inventory + order_notional, current_gate + order_notional),
                    max(heavier_inventory - 1.0, current_gate),
                )
                target_gate = float(ceil(target_gate))
                if target_gate > current_gate:
                    key = "best_quote_maker_volume_same_side_entry_price_guard_min_notional"
                    gap_key = "best_quote_maker_volume_same_side_entry_price_guard_gap_ticks"
                    updates = {
                        "best_quote_maker_volume_net_loss_reduce_enabled": False,
                        key: target_gate,
                        gap_key: 0,
                    }
                    _remember_recovery_controls(item, control, (key, gap_key))
                    _remember_recovery_updates(item, updates)
                    action = (
                        "dry_run_relax_arx_v3_anti_chase_for_no_fill_sla"
                        if dry_run
                        else "relax_arx_v3_anti_chase_for_no_fill_sla"
                    )
                    item.update(
                        {
                            "status": "recovery_active",
                            "recovery_started_at": item.get("recovery_started_at") or now.isoformat(),
                            "recovery_owned": True,
                            "last_recovery_action_at": now.isoformat(),
                            "last_recovery_action": action,
                        }
                    )
                    changed, backup_path = _apply_control_update(
                        symbol=normalized_symbol,
                        control_path=control_path,
                        control=control,
                        updates=updates,
                        now=now,
                        dry_run=dry_run,
                        restart_runner=restart,
                    )
                else:
                    action = "hold_arx_v3_anti_chase_heavier_leg_boundary"
                    item.update({"status": "low_volume", "last_recovery_check_at": now.isoformat()})
            elif (
                bool(assessment.get("planned_reduce_only_only"))
                and _safe_int(assessment.get("near_market_order_count")) > 0
            ):
                current_budget = _safe_float(control.get("best_quote_maker_volume_cycle_budget_notional"))
                budget_floor = max(float(cycle_budget_floor_notional), 0.0)
                if (
                    target_pace_behind
                    and no_fill_seconds >= max(float(trigger_seconds), 0.0)
                    and bool(assessment.get("inventory_soft_pressure"))
                    and not confirmed_loss_reduce_wear
                    and (
                        not post_restore_budget_cooldown_active
                        or arx_severe_volume_priority_recovery
                    )
                    and not bool(control.get("best_quote_maker_volume_allow_loss_reduce_only"))
                ):
                    updates = {
                        "best_quote_maker_volume_allow_loss_reduce_only": True,
                        "best_quote_maker_volume_net_loss_reduce_enabled": False,
                        "best_quote_maker_volume_quote_offset_ticks": max(
                            _safe_int(control.get("best_quote_maker_volume_quote_offset_ticks")) - 1,
                            0,
                        ),
                    }
                    _remember_recovery_controls(
                        item,
                        control,
                        ("best_quote_maker_volume_quote_offset_ticks",),
                    )
                    _remember_recovery_updates(item, updates)
                    action = (
                        "dry_run_enable_stalled_reduce_only_loss_recovery"
                        if dry_run
                        else "enable_stalled_reduce_only_loss_recovery"
                    )
                    item.update(
                        {
                            "status": "recovery_active",
                            "recovery_started_at": now.isoformat(),
                            "recovery_owned": True,
                            "last_recovery_action_at": now.isoformat(),
                            "last_recovery_action": action,
                        }
                    )
                    changed, backup_path = _apply_control_update(
                        symbol=normalized_symbol,
                        control_path=control_path,
                        control=control,
                        updates=updates,
                        now=now,
                        dry_run=dry_run,
                        restart_runner=restart,
                    )
                elif current_budget > 0 and budget_floor > current_budget:
                    _remember_recovery_controls(
                        item,
                        control,
                        ("best_quote_maker_volume_cycle_budget_notional",),
                    )
                    updates = {
                        "best_quote_maker_volume_net_loss_reduce_enabled": False,
                        "best_quote_maker_volume_cycle_budget_notional": budget_floor,
                    }
                    _remember_recovery_updates(
                        item,
                        {"best_quote_maker_volume_cycle_budget_notional": budget_floor},
                    )
                    action = (
                        "dry_run_raise_cycle_budget_floor_for_reduce_only_flow"
                        if dry_run
                        else "raise_cycle_budget_floor_for_reduce_only_flow"
                    )
                    item.update(
                        {
                            "status": "recovery_active",
                            "recovery_started_at": now.isoformat(),
                            "recovery_owned": True,
                            "last_recovery_action_at": now.isoformat(),
                            "last_recovery_action": action,
                        }
                    )
                    changed, backup_path = _apply_control_update(
                        symbol=normalized_symbol,
                        control_path=control_path,
                        control=control,
                        updates=updates,
                        now=now,
                        dry_run=dry_run,
                        restart_runner=restart,
                    )
                else:
                    action = "hold_near_market_reduce_only_flow"
                    item.update({"status": "low_volume", "last_recovery_check_at": now.isoformat()})
            elif (
                not bool(assessment.get("near_cap"))
                and not bool(assessment.get("inventory_soft_pressure"))
                and not bool(assessment.get("ineffective_orders"))
                and "inventory_bias" not in set(assessment.get("pause_reasons") or [])
                and not post_restore_budget_cooldown_active
                and not recovery_reapply_debounced
            ):
                current_budget = _safe_float(control.get("best_quote_maker_volume_cycle_budget_notional"))
                increment = max(float(volume_recovery_cycle_budget_increment), 0.0)
                if current_budget > 0 and increment > 0:
                    _remember_recovery_controls(
                        item,
                        control,
                        ("best_quote_maker_volume_cycle_budget_notional",),
                    )
                    target_budget = max(
                        current_budget + increment,
                        max(float(cycle_budget_floor_notional), 0.0),
                    )
                    updates = {
                        "best_quote_maker_volume_net_loss_reduce_enabled": False,
                        "best_quote_maker_volume_cycle_budget_notional": target_budget,
                    }
                    _remember_recovery_updates(
                        item,
                        {"best_quote_maker_volume_cycle_budget_notional": target_budget},
                    )
                    action = "dry_run_raise_cycle_budget_for_volume" if dry_run else "raise_cycle_budget_for_volume"
                    item.update(
                        {
                            "status": "recovery_active",
                            "recovery_started_at": now.isoformat(),
                            "recovery_owned": True,
                            "last_recovery_action_at": now.isoformat(),
                            "last_recovery_action": action,
                        }
                    )
                    changed, backup_path = _apply_control_update(
                        symbol=normalized_symbol,
                        control_path=control_path,
                        control=control,
                        updates=updates,
                        now=now,
                        dry_run=dry_run,
                        restart_runner=restart,
                    )
                else:
                    action = "low_volume_not_near_cap"
            elif (
                not bool(assessment.get("near_cap"))
                and _safe_float(assessment.get("current_long_notional")) <= 0
                and _safe_float(assessment.get("current_short_notional")) <= 0
            ):
                action = "low_volume_not_near_cap"
            elif not bool(assessment.get("ineffective_orders")):
                current_gap = abs(
                    _safe_float(assessment.get("current_long_notional"))
                    - _safe_float(assessment.get("current_short_notional"))
                )
                configured_gap = _safe_float(control.get("best_quote_maker_volume_inventory_bias_min_notional_gap"))
                relief_gap = current_gap + max(float(inventory_bias_relief_notional_margin), 0.0)
                if (
                    current_gap >= configured_gap
                    and relief_gap > configured_gap
                    and net_guard_entry_buffer_ok
                    and not recovery_reapply_debounced
                ):
                    _remember_recovery_controls(
                        item,
                        control,
                        ("best_quote_maker_volume_inventory_bias_min_notional_gap",),
                    )
                    updates = {
                        "best_quote_maker_volume_net_loss_reduce_enabled": False,
                        "best_quote_maker_volume_inventory_bias_min_notional_gap": relief_gap,
                    }
                    _remember_recovery_updates(
                        item,
                        {"best_quote_maker_volume_inventory_bias_min_notional_gap": relief_gap},
                    )
                    action = "dry_run_relax_inventory_bias_for_volume" if dry_run else "relax_inventory_bias_for_volume"
                    item.update(
                        {
                            "status": "recovery_active",
                            "recovery_started_at": item.get("recovery_started_at") or now.isoformat(),
                            "recovery_owned": True,
                            "last_recovery_action_at": now.isoformat(),
                            "last_recovery_action": action,
                        }
                    )
                    changed, backup_path = _apply_control_update(
                        symbol=normalized_symbol,
                        control_path=control_path,
                        control=control,
                        updates=updates,
                        now=now,
                        dry_run=dry_run,
                        restart_runner=restart,
                    )
                elif effective_near_market_flow:
                    action = "hold_effective_near_market_flow"
                elif require_soft_pressure_for_allow_loss and not effective_inventory_soft_pressure:
                    action = "hold_low_volume_without_soft_pressure"
                elif confirmed_loss_reduce_wear:
                    action = "hold_confirmed_wear_without_loss_reduce"
                elif recovery_reapply_debounced:
                    action = "hold_loss_reduce_reapply_debounce"
                elif post_restore_budget_cooldown_active:
                    action = "hold_loss_reduce_post_restore_cooldown"
                else:
                    updates = _loss_reduce_recovery_updates(
                        control=control,
                        assessment=assessment,
                        static_cycle_budget_floor_notional=static_cycle_budget_floor_notional,
                        quote_offset_extra_ticks=loss_reduce_quote_offset_extra_ticks,
                        pause_baseline_long_notional=pause_baseline_long_notional,
                        pause_baseline_short_notional=pause_baseline_short_notional,
                    )
                    _remember_recovery_controls(
                        item,
                        control,
                        tuple(key for key in updates if key != "best_quote_maker_volume_net_loss_reduce_enabled"),
                    )
                    _remember_recovery_updates(item, updates)
                    action = "dry_run_enable_allow_loss_reduce_only" if dry_run else "enable_allow_loss_reduce_only"
                    item.update(
                        {
                            "status": "recovery_active",
                            "recovery_started_at": item.get("recovery_started_at") or now.isoformat(),
                            "recovery_owned": True,
                            "last_recovery_action_at": now.isoformat(),
                            "last_recovery_action": action,
                        }
                    )
                    changed, backup_path = _apply_control_update(
                        symbol=normalized_symbol,
                        control_path=control_path,
                        control=control,
                        updates=updates,
                        now=now,
                        dry_run=dry_run,
                        restart_runner=restart,
                    )
            else:
                updates = {"best_quote_maker_volume_net_loss_reduce_enabled": False}
                chosen_action: str | None = None
                if (
                    allow_inventory_cost_gate_disable
                    and bool(assessment.get("cost_gate_blocked"))
                    and bool(control.get("best_quote_maker_volume_inventory_cost_gate_enabled"))
                ):
                    _remember_recovery_controls(item, control, ("best_quote_maker_volume_inventory_cost_gate_enabled",))
                    updates["best_quote_maker_volume_inventory_cost_gate_enabled"] = False
                    _remember_recovery_updates(
                        item,
                        {"best_quote_maker_volume_inventory_cost_gate_enabled": False},
                    )
                    chosen_action = "disable_inventory_cost_gate"
                elif (
                    not confirmed_loss_reduce_wear
                    and not recovery_reapply_debounced
                    and not post_restore_budget_cooldown_active
                    and (
                    effective_inventory_soft_pressure
                    or bool(assessment.get("volatility_inventory_reduce_deadlock"))
                    or not require_soft_pressure_for_allow_loss
                    )
                ):
                    updates = _loss_reduce_recovery_updates(
                        control=control,
                        assessment=assessment,
                        static_cycle_budget_floor_notional=static_cycle_budget_floor_notional,
                        quote_offset_extra_ticks=loss_reduce_quote_offset_extra_ticks,
                        pause_baseline_long_notional=pause_baseline_long_notional,
                        pause_baseline_short_notional=pause_baseline_short_notional,
                    )
                    _remember_recovery_controls(
                        item,
                        control,
                        tuple(key for key in updates if key != "best_quote_maker_volume_net_loss_reduce_enabled"),
                    )
                    _remember_recovery_updates(item, updates)
                    chosen_action = (
                        "enable_volatility_inventory_reduce_only"
                        if bool(assessment.get("volatility_inventory_reduce_deadlock"))
                        else "enable_allow_loss_reduce_only"
                    )
                if chosen_action is None:
                    action = "hold_ineffective_orders_without_soft_pressure"
                else:
                    action = f"dry_run_{chosen_action}" if dry_run else chosen_action
                    item.update(
                        {
                            "status": "recovery_active",
                            "recovery_started_at": item.get("recovery_started_at") or now.isoformat(),
                            "soft_recovery_extension_count": 0,
                            "recovery_owned": True,
                            "last_recovery_action_at": now.isoformat(),
                            "last_recovery_action": action,
                        }
                    )
                    changed, backup_path = _apply_control_update(
                        symbol=normalized_symbol,
                        control_path=control_path,
                        control=control,
                        updates=updates,
                        now=now,
                        dry_run=dry_run,
                        restart_runner=restart,
                    )
    except subprocess.CalledProcessError as exc:
        restart_failed = str(exc)
        action = "restart_failed"

    result = {
        "symbol": normalized_symbol,
        "action": action,
        "changed_keys": changed,
        "backup_path": backup_path,
        "dry_run": dry_run,
        "restart_failed": restart_failed,
        "recovery_gate": recovery_gate,
        "volume_summary": volume_summary,
        "assessment": assessment,
    }
    _append_jsonl(
        output_dir / "bq_volume_recovery_guard_events.jsonl",
        {"ts": now.isoformat(), **result},
    )
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Recover stalled best-quote maker-volume saved runners.")
    parser.add_argument("--app-dir", default="/home/ubuntu/wangge")
    parser.add_argument("--output-dir", default=None)
    parser.add_argument("--state-path", default=None)
    parser.add_argument("--runner-wrapper", default="/usr/local/bin/grid-saved-runner")
    parser.add_argument("--symbols", nargs="+", default=["REUSDT"])
    parser.add_argument("--window-seconds", type=float, default=180.0)
    parser.add_argument("--min-volume-notional", type=float, default=10.0)
    parser.add_argument("--trigger-seconds", type=float, default=180.0)
    parser.add_argument("--recover-min-volume-notional", type=float, default=None)
    parser.add_argument("--daily-targets", nargs="*", default=[])
    parser.add_argument("--target-pace-fraction", type=float, default=0.9)
    parser.add_argument("--target-pace-max-multiplier", type=float, default=2.0)
    parser.add_argument("--target-completion-buffer-seconds", type=float, default=0.0)
    parser.add_argument("--near-cap-ratio", type=float, default=0.95)
    parser.add_argument("--recover-cap-ratio", type=float, default=0.96)
    parser.add_argument("--far-ticks", type=int, default=8)
    parser.add_argument("--plan-stale-seconds", type=float, default=300.0)
    parser.add_argument("--allow-inventory-cost-gate-disable", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--max-recovery-seconds", type=float, default=300.0)
    parser.add_argument("--cooldown-seconds", type=float, default=600.0)
    parser.add_argument("--recovery-min-hold-seconds", type=float, default=120.0)
    parser.add_argument("--recovery-reapply-min-seconds", type=float, default=180.0)
    parser.add_argument("--post-restore-cooldown-seconds", type=float, default=300.0)
    parser.add_argument("--max-soft-recovery-extensions", type=int, default=1)
    parser.add_argument("--inactive-trigger-seconds", type=float, default=120.0)
    parser.add_argument("--inactive-restart-cooldown-seconds", type=float, default=600.0)
    parser.add_argument("--inactive-max-snapshot-age-seconds", type=float, default=300.0)
    parser.add_argument("--corrupt-state-max-backup-age-seconds", type=float, default=3600.0)
    parser.add_argument("--corrupt-state-min-age-seconds", type=float, default=30.0)
    parser.add_argument(
        "--stop-persistent-corrupt-runner",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    parser.add_argument("--inventory-bias-relief-notional-margin", type=float, default=24.0)
    parser.add_argument("--volume-recovery-cycle-budget-increment", type=float, default=12.0)
    parser.add_argument("--cycle-budget-floors", nargs="*", default=[])
    parser.add_argument("--pause-baselines", nargs="*", default=[])
    parser.add_argument("--loss-reduce-quote-offset-extra-ticks", nargs="*", default=[])
    parser.add_argument("--require-soft-pressure-for-allow-loss-symbols", nargs="*", default=[])
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)

    app_dir = Path(args.app_dir).resolve()
    output_dir = Path(args.output_dir).resolve() if args.output_dir else app_dir / "output"
    state_path = Path(args.state_path).resolve() if args.state_path else output_dir / "bq_volume_recovery_guard_state.json"
    state = _read_json(state_path)
    now = datetime.now(timezone.utc)
    daily_targets = _parse_symbol_notionals(args.daily_targets)
    cycle_budget_floors = _parse_symbol_notionals(args.cycle_budget_floors)
    pause_baselines = _parse_symbol_pause_baselines(args.pause_baselines)
    loss_reduce_quote_offset_extra_ticks = _parse_symbol_notionals(
        args.loss_reduce_quote_offset_extra_ticks
    )
    require_soft_pressure_for_allow_loss_symbols = set(
        _normalize_symbols(args.require_soft_pressure_for_allow_loss_symbols)
    )
    results = []
    exit_code = 0
    for symbol in _normalize_symbols(args.symbols):
        target_done_marker = _target_gate_done_marker(output_dir, symbol, now)
        if target_done_marker.exists():
            runner_active = _runner_is_active(symbol)
            stop_failed: str | None = None
            if runner_active and args.dry_run:
                action = "dry_run_stop_runner_target_gate_done"
            elif runner_active:
                try:
                    _default_stop_runner(symbol, runner_wrapper=args.runner_wrapper)
                    action = "stop_runner_target_gate_done"
                except subprocess.CalledProcessError as exc:
                    action = "stop_runner_target_gate_done_failed"
                    stop_failed = str(exc)
                    exit_code = 1
            else:
                action = "skip_target_gate_done_terminal"
            result = {
                "symbol": symbol,
                "action": action,
                "changed_keys": [],
                "backup_path": None,
                "dry_run": args.dry_run,
                "restart_failed": stop_failed,
                "target_gate_done_marker": str(target_done_marker),
            }
            _append_jsonl(
                output_dir / "bq_volume_recovery_guard_events.jsonl",
                {"ts": now.isoformat(), **result},
            )
            results.append(result)
            continue
        corrupt_state_result = recover_corrupt_loop_state(
            symbol=symbol,
            output_dir=output_dir,
            now=now,
            max_backup_age_seconds=args.corrupt_state_max_backup_age_seconds,
            min_corrupt_age_seconds=args.corrupt_state_min_age_seconds,
            max_snapshot_age_seconds=args.inactive_max_snapshot_age_seconds,
            dry_run=args.dry_run,
        )
        if corrupt_state_result is not None:
            corruption = corrupt_state_result.get("state_corruption")
            blocking_reasons = (
                corruption.get("blocking_reasons") if isinstance(corruption, dict) else []
            )
            persistent_corruption = (
                corrupt_state_result.get("action") == "skip_corrupt_state_recovery_safety_gate"
                and "corrupt_state_not_persistent" not in blocking_reasons
            )
            if (
                args.stop_persistent_corrupt_runner
                and persistent_corruption
                and _runner_is_active(symbol)
            ):
                if args.dry_run:
                    corrupt_state_result["action"] = (
                        "dry_run_stop_runner_persistent_corrupt_state"
                    )
                else:
                    try:
                        _default_stop_runner(symbol, runner_wrapper=args.runner_wrapper)
                        corrupt_state_result["action"] = (
                            "stop_runner_persistent_corrupt_state"
                        )
                    except subprocess.CalledProcessError as exc:
                        corrupt_state_result["action"] = (
                            "stop_runner_persistent_corrupt_state_failed"
                        )
                        corrupt_state_result["restart_failed"] = str(exc)
                        exit_code = 1
            elif (
                bool(corrupt_state_result.get("safe_restart_after_salvage"))
                and not args.dry_run
                and not _runner_is_active(symbol)
            ):
                try:
                    _default_restart_runner(symbol, runner_wrapper=args.runner_wrapper)
                    corrupt_state_result["action"] = (
                        str(corrupt_state_result.get("action")) + "_and_restart_runner"
                    )
                except subprocess.CalledProcessError as exc:
                    corrupt_state_result["action"] = (
                        str(corrupt_state_result.get("action")) + "_restart_failed"
                    )
                    corrupt_state_result["restart_failed"] = str(exc)
                    exit_code = 1
            _append_jsonl(
                output_dir / "bq_volume_recovery_guard_events.jsonl",
                {"ts": now.isoformat(), **corrupt_state_result},
            )
            results.append(corrupt_state_result)
            continue
        if not _runner_is_active(symbol):
            result = recover_inactive_runner(
                symbol=symbol,
                output_dir=output_dir,
                state=state,
                now=now,
                trigger_seconds=args.inactive_trigger_seconds,
                restart_cooldown_seconds=args.inactive_restart_cooldown_seconds,
                max_snapshot_age_seconds=args.inactive_max_snapshot_age_seconds,
                runner_wrapper=args.runner_wrapper,
                dry_run=args.dry_run,
            )
            _append_jsonl(
                output_dir / "bq_volume_recovery_guard_events.jsonl",
                {"ts": now.isoformat(), **result},
            )
            results.append(result)
            if result.get("restart_failed"):
                exit_code = 1
            continue
        effective_control_drift_result = recover_arx_effective_control_drift(
            output_dir=output_dir,
            symbol=symbol,
            state=state,
            now=now,
            cooldown_seconds=max(min(float(args.trigger_seconds), 90.0), 60.0),
            dry_run=args.dry_run,
            runner_wrapper=args.runner_wrapper,
        )
        if effective_control_drift_result is not None:
            _append_jsonl(
                output_dir / "bq_volume_recovery_guard_events.jsonl",
                {"ts": now.isoformat(), **effective_control_drift_result},
            )
            results.append(effective_control_drift_result)
            if effective_control_drift_result.get("restart_failed"):
                exit_code = 1
            continue
        runner_error_result = recover_arx_runner_error_loop(
            output_dir=output_dir,
            symbol=symbol,
            state=state,
            now=now,
            cooldown_seconds=max(min(float(args.trigger_seconds), 90.0), 60.0),
            dry_run=args.dry_run,
            runner_wrapper=args.runner_wrapper,
        )
        if runner_error_result is not None:
            _append_jsonl(
                output_dir / "bq_volume_recovery_guard_events.jsonl",
                {"ts": now.isoformat(), **runner_error_result},
            )
            results.append(runner_error_result)
            if runner_error_result.get("restart_failed"):
                exit_code = 1
            continue
        item = _symbol_state(state, symbol)
        _clear_inactive_observation(item)
        fetch_deferred_until = _parse_time(item.get("exchange_trade_fetch_deferred_until"))
        if fetch_deferred_until is not None and now < fetch_deferred_until:
            result = {
                "symbol": symbol,
                "action": "defer_exchange_trade_fetch_cooldown",
                "changed_keys": [],
                "backup_path": None,
                "dry_run": args.dry_run,
                "restart_failed": None,
                "retry_after_seconds": round((fetch_deferred_until - now).total_seconds(), 3),
            }
            _append_jsonl(
                output_dir / "bq_volume_recovery_guard_events.jsonl",
                {"ts": now.isoformat(), **result},
            )
            results.append(result)
            continue
        try:
            fetch_window_seconds = args.window_seconds
            if symbol in daily_targets:
                day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
                fetch_window_seconds = max(fetch_window_seconds, (now - day_start).total_seconds())
            trade_rows = _fetch_exchange_user_trades(
                symbol=symbol,
                now=now,
                window_seconds=fetch_window_seconds,
            )
        except (KeyError, OSError, RuntimeError, ValueError) as exc:
            retry_after_seconds = _exchange_trade_fetch_cooldown_seconds(exc)
            if retry_after_seconds is not None:
                item["exchange_trade_fetch_deferred_until"] = (
                    now + timedelta(seconds=retry_after_seconds)
                ).isoformat()
                result = {
                    "symbol": symbol,
                    "action": "defer_exchange_trade_fetch_cooldown",
                    "changed_keys": [],
                    "backup_path": None,
                    "dry_run": args.dry_run,
                    "restart_failed": None,
                    "retry_after_seconds": retry_after_seconds,
                }
                _append_jsonl(
                    output_dir / "bq_volume_recovery_guard_events.jsonl",
                    {"ts": now.isoformat(), **result},
                )
                results.append(result)
                continue
            result = {
                "symbol": symbol,
                "action": "skip_exchange_trade_fetch_failed",
                "changed_keys": [],
                "backup_path": None,
                "dry_run": args.dry_run,
                "restart_failed": None,
                "exchange_trade_fetch_failed": str(exc),
            }
            _append_jsonl(output_dir / "bq_volume_recovery_guard_events.jsonl", {"ts": now.isoformat(), **result})
            results.append(result)
            exit_code = 1
            continue
        item.pop("exchange_trade_fetch_deferred_until", None)
        submit = _read_json(_submit_path(output_dir, symbol))
        local_active_order_count = _safe_int(
            (submit.get("observed_strategy_open_order_state") or {}).get(
                "active_order_count"
            )
        )
        control_for_drift_recovery = _read_json(_control_path(output_dir, symbol))
        arx_single_side_cap_pending = symbol.upper() == "ARXUSDT" and bool(
            arx_single_side_cap_updates(control_for_drift_recovery)
            or arx_directional_unwind_quote_updates(control_for_drift_recovery)
        )
        exchange_order_drift_result = recover_arx_exchange_order_drift(
            symbol=symbol,
            local_active_order_count=local_active_order_count,
            submit=submit,
            state=state,
            now=now,
            # A runner needs one complete fresh cycle after restart, but an
            # exchange-empty ARX book must not remain stuck for the normal
            # multi-minute recovery cooldown.
            cooldown_seconds=max(min(float(args.trigger_seconds), 90.0), 60.0),
            dry_run=args.dry_run,
            runner_wrapper=args.runner_wrapper,
        )
        if exchange_order_drift_result is not None:
            hold_for_recent_activity = (
                exchange_order_drift_result.get("action")
                == "hold_arx_exchange_order_drift_after_recent_activity"
            )
            if not (arx_single_side_cap_pending and hold_for_recent_activity):
                _append_jsonl(
                    output_dir / "bq_volume_recovery_guard_events.jsonl",
                    {"ts": now.isoformat(), **exchange_order_drift_result},
                )
                results.append(exchange_order_drift_result)
                if exchange_order_drift_result.get("restart_failed"):
                    exit_code = 1
                continue
        result = check_symbol(
            symbol=symbol,
            output_dir=output_dir,
            state=state,
            now=now,
            window_seconds=args.window_seconds,
            min_volume_notional=args.min_volume_notional,
            trigger_seconds=args.trigger_seconds,
            daily_target_notional=daily_targets.get(symbol),
            target_pace_fraction=args.target_pace_fraction,
            target_pace_max_multiplier=args.target_pace_max_multiplier,
            target_completion_buffer_seconds=args.target_completion_buffer_seconds,
            recover_min_volume_notional=args.recover_min_volume_notional,
            near_cap_ratio=args.near_cap_ratio,
            recover_cap_ratio=args.recover_cap_ratio,
            far_ticks=args.far_ticks,
            plan_stale_seconds=args.plan_stale_seconds,
            allow_inventory_cost_gate_disable=args.allow_inventory_cost_gate_disable,
            max_recovery_seconds=args.max_recovery_seconds,
            cooldown_seconds=args.cooldown_seconds,
            recovery_min_hold_seconds=args.recovery_min_hold_seconds,
            recovery_reapply_min_seconds=args.recovery_reapply_min_seconds,
            post_restore_cooldown_seconds=args.post_restore_cooldown_seconds,
            max_soft_recovery_extensions=args.max_soft_recovery_extensions,
            inventory_bias_relief_notional_margin=args.inventory_bias_relief_notional_margin,
            volume_recovery_cycle_budget_increment=args.volume_recovery_cycle_budget_increment,
            cycle_budget_floor_notional=cycle_budget_floors.get(symbol, 0.0),
            pause_baseline_long_notional=pause_baselines.get(symbol, (0.0, 0.0))[0],
            pause_baseline_short_notional=pause_baselines.get(symbol, (0.0, 0.0))[1],
            loss_reduce_quote_offset_extra_ticks=int(
                loss_reduce_quote_offset_extra_ticks.get(symbol, 0.0)
            ),
            require_soft_pressure_for_allow_loss=(
                symbol in require_soft_pressure_for_allow_loss_symbols
            ),
            trade_rows=trade_rows,
            volume_source="exchange_user_trades",
            dry_run=args.dry_run,
            runner_wrapper=args.runner_wrapper,
        )
        if result.get("restart_failed"):
            exit_code = 1
        results.append(result)

    if not args.dry_run:
        _write_json(state_path, state)
    print(json.dumps({"ok": exit_code == 0, "checked_at": now.isoformat(), "results": results}, ensure_ascii=False))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
