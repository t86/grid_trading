from __future__ import annotations

import argparse
import json
import os
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable


RestartRunner = Callable[[str], object]
UserTradesPageFetcher = Callable[..., list[dict[str, Any]]]


_RECOVERY_CONTROL_KEYS = (
    "best_quote_maker_volume_allow_loss_reduce_only",
    "best_quote_maker_volume_inventory_cost_gate_enabled",
    "best_quote_maker_volume_inventory_bias_min_notional_gap",
    "best_quote_maker_volume_cycle_budget_notional",
)


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    tmp_path.replace(path)


def _append_jsonl(path: Path, row: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")


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


def _control_path(output_dir: Path, symbol: str) -> Path:
    return output_dir / f"{symbol.lower()}_loop_runner_control.json"


def _plan_path(output_dir: Path, symbol: str) -> Path:
    return output_dir / f"{symbol.lower()}_loop_latest_plan.json"


def _submit_path(output_dir: Path, symbol: str) -> Path:
    return output_dir / f"{symbol.lower()}_loop_latest_submit.json"


def _trade_path(output_dir: Path, symbol: str) -> Path:
    return output_dir / f"{symbol.lower()}_loop_trade_audit.jsonl"


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


def _restore_recovery_controls(item: dict[str, Any]) -> dict[str, Any]:
    original = item.get("guard_original_controls")
    updates = {"best_quote_maker_volume_allow_loss_reduce_only": False}
    if isinstance(original, dict):
        for key, value in original.items():
            if key in _RECOVERY_CONTROL_KEYS:
                updates[key] = value
    updates["best_quote_maker_volume_net_loss_reduce_enabled"] = False
    return updates


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
    low_volume = gross < max(float(min_volume_notional), 0.0)
    if low_volume:
        reasons.append("low_volume")

    bid, ask = _live_book(plan, submit)
    tick = _tick_size(plan, control)
    orders = _planned_orders(plan, submit)
    near_orders = [
        item
        for item in orders
        if _order_is_near_market(item, bid=bid, ask=ask, tick_size=tick, far_ticks=far_ticks)
    ]
    active_count = _active_order_count(plan, submit)
    all_orders_far = bool(orders) and not near_orders
    ineffective_orders = active_count <= 0 or all_orders_far
    if active_count <= 0:
        reasons.append("no_active_orders")
    if all_orders_far:
        reasons.append("planned_orders_far_from_market")

    long_cap, short_cap = _position_caps(plan, control)
    current_long = _safe_float(plan.get("current_long_notional"))
    current_short = _safe_float(plan.get("current_short_notional"))
    near_long_cap = long_cap > 0 and current_long >= long_cap * max(float(near_cap_ratio), 0.0)
    near_short_cap = short_cap > 0 and current_short >= short_cap * max(float(near_cap_ratio), 0.0)
    if near_long_cap:
        reasons.append("long_near_cap")
    if near_short_cap:
        reasons.append("short_near_cap")

    return {
        "symbol": symbol,
        "reasons": reasons,
        "low_volume": low_volume,
        "gross_notional": gross,
        "active_order_count": active_count,
        "planned_order_count": len(orders),
        "near_market_order_count": len(near_orders),
        "ineffective_orders": ineffective_orders,
        "all_orders_far": all_orders_far,
        "near_cap": near_long_cap or near_short_cap,
        "long_near_cap": near_long_cap,
        "short_near_cap": near_short_cap,
        "current_long_notional": current_long,
        "current_short_notional": current_short,
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
    changed = [key for key, value in updates.items() if control.get(key) != value]
    if not changed:
        return [], None
    if dry_run:
        return changed, None
    updated = dict(control)
    updated.update(updates)
    backup_path = _backup_control(control_path, control, now)
    _write_json(control_path, updated)
    restart_runner(symbol)
    return changed, str(backup_path)


def _default_restart_runner(symbol: str, *, runner_wrapper: str) -> object:
    return subprocess.run([runner_wrapper, "restart", symbol], check=True)


def _normalize_symbols(items: list[str]) -> list[str]:
    symbols: list[str] = []
    for item in items:
        for part in str(item).split(","):
            symbol = part.strip().upper()
            if symbol and symbol not in symbols:
                symbols.append(symbol)
    return symbols


def check_symbol(
    *,
    symbol: str,
    output_dir: Path,
    state: dict[str, Any],
    now: datetime,
    window_seconds: float,
    min_volume_notional: float,
    trigger_seconds: float,
    recover_min_volume_notional: float | None = None,
    near_cap_ratio: float = 0.95,
    recover_cap_ratio: float = 0.96,
    far_ticks: int = 8,
    plan_stale_seconds: float = 300.0,
    allow_inventory_cost_gate_disable: bool = True,
    max_recovery_seconds: float = 300.0,
    cooldown_seconds: float = 600.0,
    inventory_bias_relief_notional_margin: float = 24.0,
    volume_recovery_cycle_budget_increment: float = 12.0,
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
    volume_summary = summarize_recent_volume(
        rows=trade_rows if trade_rows is not None else _iter_jsonl(_trade_path(output_dir, normalized_symbol)),
        now=now,
        window_seconds=window_seconds,
    )
    volume_summary["source"] = volume_source
    assessment = assess_symbol(
        symbol=normalized_symbol,
        control=control,
        plan=plan,
        submit=submit,
        volume_summary=volume_summary,
        now=now,
        min_volume_notional=min_volume_notional,
        near_cap_ratio=near_cap_ratio,
        far_ticks=far_ticks,
        plan_stale_seconds=plan_stale_seconds,
    )
    item = _symbol_state(state, normalized_symbol)
    restart = restart_runner or (lambda item_symbol: _default_restart_runner(item_symbol, runner_wrapper=runner_wrapper))
    recover_floor = (
        float(recover_min_volume_notional)
        if recover_min_volume_notional is not None
        else max(float(min_volume_notional), 0.0)
    )

    action = "none"
    backup_path: str | None = None
    changed: list[str] = []
    restart_failed: str | None = None
    status_reasons = set(assessment.get("reasons") or [])
    stale_or_missing = bool({"missing_control", "latest_plan_stale", "latest_submit_stale"} & status_reasons)
    cooldown_until = _parse_time(item.get("cooldown_until"))
    recovery_original_controls = item.get("guard_original_controls")
    recovery_has_original_controls = isinstance(recovery_original_controls, dict) and bool(recovery_original_controls)

    try:
        if stale_or_missing:
            action = "skip_stale_or_missing_inputs"
        elif cooldown_until is not None and now < cooldown_until:
            action = "cooldown"
        elif recovery_has_original_controls and not bool(control.get("best_quote_maker_volume_allow_loss_reduce_only")):
            cap_buffer_ok = (
                _safe_float(assessment.get("current_long_notional"))
                < _safe_float(assessment.get("max_long_notional")) * max(float(recover_cap_ratio), 0.0)
                and _safe_float(assessment.get("current_short_notional"))
                < _safe_float(assessment.get("max_short_notional")) * max(float(recover_cap_ratio), 0.0)
            )
            restore_ready = (
                _safe_float(volume_summary.get("gross_notional")) >= recover_floor
                and _safe_int(assessment.get("active_order_count")) > 0
                and _safe_int(assessment.get("near_market_order_count")) > 0
                and cap_buffer_ok
            )
            if restore_ready:
                updates = _restore_recovery_controls(item)
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
                item.update({"status": "normal", "last_normal_at": now.isoformat()})
                item.pop("guard_original_controls", None)
                item.pop("recovery_started_at", None)
                item.pop("recovery_owned", None)
            else:
                action = "hold_inventory_bias_relief"
                item.update({"status": "recovery_active", "last_recovery_check_at": now.isoformat()})
        elif bool(control.get("best_quote_maker_volume_allow_loss_reduce_only")):
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
            if (
                recovery_started_at is not None
                and (now - recovery_started_at).total_seconds() >= max(float(max_recovery_seconds), 0.0)
            ):
                updates = _restore_recovery_controls(item)
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
            cap_buffer_ok = (
                _safe_float(assessment.get("current_long_notional"))
                < _safe_float(assessment.get("max_long_notional")) * max(float(recover_cap_ratio), 0.0)
                and _safe_float(assessment.get("current_short_notional"))
                < _safe_float(assessment.get("max_short_notional")) * max(float(recover_cap_ratio), 0.0)
            )
            restore_ready = (
                _safe_float(volume_summary.get("gross_notional")) >= recover_floor
                and _safe_int(assessment.get("active_order_count")) > 0
                and _safe_int(assessment.get("near_market_order_count")) > 0
                and cap_buffer_ok
            )
            if restore_ready:
                has_original_controls = bool(item.get("guard_original_controls"))
                updates = _restore_recovery_controls(item)
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
                item.update({"status": "normal", "last_normal_at": now.isoformat()})
                item.pop("guard_original_controls", None)
                item.pop("recovery_started_at", None)
                item.pop("recovery_owned", None)
            elif not cap_buffer_ok:
                action = "hold_recovery_until_cap_buffer"
                item.update({"status": "recovery_active", "last_recovery_check_at": now.isoformat()})
            else:
                action = "hold_recovery_until_volume_or_orders"
                item.update({"status": "recovery_active", "last_recovery_check_at": now.isoformat()})
        elif not bool(assessment.get("low_volume")):
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
            elif not bool(assessment.get("near_cap")) and not bool(assessment.get("ineffective_orders")):
                current_budget = _safe_float(control.get("best_quote_maker_volume_cycle_budget_notional"))
                increment = max(float(volume_recovery_cycle_budget_increment), 0.0)
                if current_budget > 0 and increment > 0:
                    _remember_recovery_controls(
                        item,
                        control,
                        ("best_quote_maker_volume_cycle_budget_notional",),
                    )
                    updates = {
                        "best_quote_maker_volume_net_loss_reduce_enabled": False,
                        "best_quote_maker_volume_cycle_budget_notional": current_budget + increment,
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
                    action = "dry_run_raise_cycle_budget_for_volume" if dry_run else "raise_cycle_budget_for_volume"
                    item.update(
                        {
                            "status": "recovery_active",
                            "recovery_started_at": item.get("recovery_started_at") or now.isoformat(),
                            "recovery_owned": True,
                            "last_recovery_action_at": now.isoformat(),
                            "last_recovery_action": action,
                        }
                    )
                else:
                    action = "low_volume_not_near_cap"
            elif not bool(assessment.get("near_cap")):
                action = "low_volume_not_near_cap"
            elif not bool(assessment.get("ineffective_orders")):
                current_gap = abs(
                    _safe_float(assessment.get("current_long_notional"))
                    - _safe_float(assessment.get("current_short_notional"))
                )
                configured_gap = _safe_float(control.get("best_quote_maker_volume_inventory_bias_min_notional_gap"))
                relief_gap = current_gap + max(float(inventory_bias_relief_notional_margin), 0.0)
                if relief_gap > configured_gap:
                    _remember_recovery_controls(
                        item,
                        control,
                        ("best_quote_maker_volume_inventory_bias_min_notional_gap",),
                    )
                    updates = {
                        "best_quote_maker_volume_net_loss_reduce_enabled": False,
                        "best_quote_maker_volume_inventory_bias_min_notional_gap": relief_gap,
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
                else:
                    _remember_recovery_controls(
                        item,
                        control,
                        ("best_quote_maker_volume_allow_loss_reduce_only",),
                    )
                    updates = {
                        "best_quote_maker_volume_net_loss_reduce_enabled": False,
                        "best_quote_maker_volume_allow_loss_reduce_only": True,
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
            else:
                updates = {"best_quote_maker_volume_net_loss_reduce_enabled": False}
                if (
                    allow_inventory_cost_gate_disable
                    and bool(assessment.get("cost_gate_blocked"))
                    and bool(control.get("best_quote_maker_volume_inventory_cost_gate_enabled"))
                ):
                    _remember_recovery_controls(item, control, ("best_quote_maker_volume_inventory_cost_gate_enabled",))
                    updates["best_quote_maker_volume_inventory_cost_gate_enabled"] = False
                    chosen_action = "disable_inventory_cost_gate"
                else:
                    _remember_recovery_controls(item, control, ("best_quote_maker_volume_allow_loss_reduce_only",))
                    updates["best_quote_maker_volume_allow_loss_reduce_only"] = True
                    chosen_action = "enable_allow_loss_reduce_only"
                changed, backup_path = _apply_control_update(
                    symbol=normalized_symbol,
                    control_path=control_path,
                    control=control,
                    updates=updates,
                    now=now,
                    dry_run=dry_run,
                    restart_runner=restart,
                )
                action = f"dry_run_{chosen_action}" if dry_run else chosen_action
                item.update(
                    {
                        "status": "recovery_active",
                        "recovery_started_at": item.get("recovery_started_at") or now.isoformat(),
                        "recovery_owned": True,
                        "last_recovery_action_at": now.isoformat(),
                        "last_recovery_action": action,
                    }
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
    parser.add_argument("--near-cap-ratio", type=float, default=0.95)
    parser.add_argument("--recover-cap-ratio", type=float, default=0.96)
    parser.add_argument("--far-ticks", type=int, default=8)
    parser.add_argument("--plan-stale-seconds", type=float, default=300.0)
    parser.add_argument("--allow-inventory-cost-gate-disable", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--max-recovery-seconds", type=float, default=300.0)
    parser.add_argument("--cooldown-seconds", type=float, default=600.0)
    parser.add_argument("--inventory-bias-relief-notional-margin", type=float, default=24.0)
    parser.add_argument("--volume-recovery-cycle-budget-increment", type=float, default=12.0)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)

    app_dir = Path(args.app_dir).resolve()
    output_dir = Path(args.output_dir).resolve() if args.output_dir else app_dir / "output"
    state_path = Path(args.state_path).resolve() if args.state_path else output_dir / "bq_volume_recovery_guard_state.json"
    state = _read_json(state_path)
    now = datetime.now(timezone.utc)
    results = []
    exit_code = 0
    for symbol in _normalize_symbols(args.symbols):
        try:
            trade_rows = _fetch_exchange_user_trades(
                symbol=symbol,
                now=now,
                window_seconds=args.window_seconds,
            )
        except (KeyError, OSError, RuntimeError, ValueError) as exc:
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
        result = check_symbol(
            symbol=symbol,
            output_dir=output_dir,
            state=state,
            now=now,
            window_seconds=args.window_seconds,
            min_volume_notional=args.min_volume_notional,
            trigger_seconds=args.trigger_seconds,
            recover_min_volume_notional=args.recover_min_volume_notional,
            near_cap_ratio=args.near_cap_ratio,
            recover_cap_ratio=args.recover_cap_ratio,
            far_ticks=args.far_ticks,
            plan_stale_seconds=args.plan_stale_seconds,
            allow_inventory_cost_gate_disable=args.allow_inventory_cost_gate_disable,
            max_recovery_seconds=args.max_recovery_seconds,
            cooldown_seconds=args.cooldown_seconds,
            inventory_bias_relief_notional_margin=args.inventory_bias_relief_notional_margin,
            volume_recovery_cycle_budget_increment=args.volume_recovery_cycle_budget_increment,
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
