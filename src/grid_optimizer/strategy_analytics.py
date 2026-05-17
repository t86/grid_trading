from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .audit import build_audit_paths, read_json, read_jsonl, read_trade_audit_rows, trade_row_time_ms
from .monitor import summarize_income, summarize_user_trades


PARAM_FINGERPRINT_KEYS = (
    "strategy_profile",
    "strategy_mode",
    "grid_level_mode",
    "allocation_mode",
    "strategy_direction",
    "step_price",
    "step_ratio",
    "grid_band_ratio",
    "buy_levels",
    "sell_levels",
    "total_grid_notional",
    "per_order_notional",
    "threshold_position_notional",
    "threshold_reduce_target_ratio",
    "adverse_reduce_enabled",
    "hard_loss_forced_reduce_enabled",
    "volatility_trigger_enabled",
    "volume_trigger_enabled",
)

GRID_ROLES = {
    "entry",
    "entry_long",
    "entry_short",
    "grid_entry",
    "grid_entry_long",
    "grid_entry_short",
    "grid_exit",
    "take_profit",
    "take_profit_long",
    "take_profit_short",
    "bootstrap",
    "bootstrap_entry",
    "bootstrap_long",
    "bootstrap_short",
    "inventory_build",
    "defense_buy",
    "defense_sell",
}

REDUCE_ROLES = {
    "forced_reduce",
    "tail_cleanup",
    "active_delever_long",
    "active_delever_short",
    "soft_delever_long",
    "soft_delever_short",
    "hard_delever_long",
    "hard_delever_short",
    "adverse_reduce_long",
    "adverse_reduce_short",
    "hard_loss_forced_reduce_long",
    "hard_loss_forced_reduce_short",
    "maker_reduce_long",
    "maker_reduce_short",
    "best_quote_reduce_long",
    "best_quote_reduce_short",
    "recycle_sell",
    "taker_exit",
    "fast_stop_exit",
    "flatten",
}

PROTECTION_ROLE_TOKENS = (
    "guard",
    "protect",
    "delever",
    "forced_reduce",
    "adverse_reduce",
    "hard_loss",
    "stop",
    "flatten",
    "recycle",
    "tail_cleanup",
)


def _safe_float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _safe_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _iso_from_ms(value: int) -> str | None:
    if value <= 0:
        return None
    return datetime.fromtimestamp(value / 1000.0, tz=timezone.utc).isoformat()


def _read_json_dict(path: Path) -> dict[str, Any]:
    payload = read_json(path)
    return payload if isinstance(payload, dict) else {}


def _events_path_symbol(path: Path) -> str:
    stem = path.stem
    if stem.endswith("_loop_events"):
        return stem[: -len("_loop_events")].upper()
    if stem.endswith("_spot_loop_events"):
        return stem[: -len("_spot_loop_events")].upper()
    if stem.endswith("_events"):
        return stem[: -len("_events")].upper()
    return stem.upper()


def _control_path_for_events(events_path: Path) -> Path:
    stem = events_path.stem
    if stem.endswith("_spot_loop_events"):
        return events_path.with_name(f"{stem[: -len('_spot_loop_events')]}_spot_loop_runner_control.json")
    if stem.endswith("_loop_events"):
        return events_path.with_name(f"{stem[: -len('_loop_events')]}_loop_runner_control.json")
    return events_path.with_name(f"{stem}_runner_control.json")


def _load_runner_config(events_path: Path) -> dict[str, Any]:
    control = _read_json_dict(_control_path_for_events(events_path))
    config = control.get("config") if isinstance(control.get("config"), dict) else {}
    if config:
        return dict(config)
    return control


def _market_type_for_path(events_path: Path, config: dict[str, Any]) -> str:
    raw = str(config.get("market_type") or "").lower().strip()
    if raw in {"spot", "futures", "margin"}:
        return raw
    return "spot" if "_spot_" in events_path.name else "futures"


def _strategy_mode(config: dict[str, Any]) -> str:
    return str(
        config.get("strategy_profile")
        or config.get("strategy_mode")
        or config.get("mode")
        or "unknown"
    ).strip() or "unknown"


def _param_fingerprint(config: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    params = {
        key: config.get(key)
        for key in PARAM_FINGERPRINT_KEYS
        if key in config and config.get(key) not in (None, "")
    }
    encoded = json.dumps(params, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha1(encoded.encode("utf-8")).hexdigest()[:12], params


def infer_trade_role(row: dict[str, Any]) -> str:
    explicit = str(row.get("role") or row.get("order_role") or "").lower().strip()
    if explicit:
        return explicit
    client_order_id = str(row.get("clientOrderId") or row.get("client_order_id") or "").lower().strip()
    parts = client_order_id.split("-")
    if len(parts) >= 3 and parts[0] == "gx":
        compact = parts[2]
        side = str(row.get("side") or "").upper().strip()
        if compact == "entrylon":
            return "entry_long"
        if compact == "entrysho":
            return "entry_short"
        if compact == "bootstra":
            return "bootstrap_long" if side == "BUY" else "bootstrap_short"
        if compact == "takeprof":
            return "take_profit_short" if side == "BUY" else "take_profit_long"
        if compact == "activede":
            return "active_delever_short" if side == "BUY" else "active_delever_long"
        if compact == "softdele":
            return "soft_delever_short" if side == "BUY" else "soft_delever_long"
        if compact == "harddele":
            return "hard_delever_short" if side == "BUY" else "hard_delever_long"
    return ""


def classify_trade(row: dict[str, Any]) -> str:
    role = infer_trade_role(row)
    if role in REDUCE_ROLES:
        return "reduce"
    if role in GRID_ROLES:
        return "grid"
    if role and any(token in role for token in PROTECTION_ROLE_TOKENS):
        return "protection"
    if bool(row.get("reduceOnly") or row.get("reduce_only")):
        return "reduce"
    return "other"


def _bucket_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    summary = summarize_user_trades(rows)
    commission = _safe_float(summary.get("commission"))
    realized = _safe_float(summary.get("realized_pnl"))
    return {
        "trade_count": int(summary.get("trade_count") or 0),
        "gross_notional": _safe_float(summary.get("gross_notional")),
        "buy_notional": _safe_float(summary.get("buy_notional")),
        "sell_notional": _safe_float(summary.get("sell_notional")),
        "realized_pnl": realized,
        "commission": commission,
        "net_realized_pnl": realized - commission,
    }


def _build_points(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    points: list[dict[str, Any]] = []
    cumulative_notional = 0.0
    cumulative_net = 0.0
    for row in sorted(rows, key=lambda item: (trade_row_time_ms(item), str(item.get("id") or ""))):
        notional = _safe_float(row.get("price")) * _safe_float(row.get("qty"))
        net = _safe_float(row.get("realizedPnl")) - abs(_safe_float(row.get("commission")))
        cumulative_notional += notional
        cumulative_net += net
        points.append(
            {
                "ts": _iso_from_ms(trade_row_time_ms(row)),
                "cumulative_notional": cumulative_notional,
                "cumulative_net_realized_pnl": cumulative_net,
            }
        )
    return points[-240:]


def _event_stats(events_path: Path) -> dict[str, Any]:
    count = 0
    first_ms = 0
    last_ms = 0
    guard_hits: defaultdict[str, int] = defaultdict(int)
    if not events_path.exists():
        return {"event_count": 0, "first_event_at": None, "last_event_at": None, "guard_hits": {}}
    try:
        with events_path.open("r", encoding="utf-8") as handle:
            for raw in handle:
                raw = raw.strip()
                if not raw:
                    continue
                try:
                    event = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                if not isinstance(event, dict):
                    continue
                count += 1
                ts_ms = _safe_int(event.get("time_ms") or event.get("timestamp_ms"))
                if ts_ms <= 0:
                    ts = str(event.get("ts") or "").strip()
                    try:
                        ts_ms = int(datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp() * 1000) if ts else 0
                    except ValueError:
                        ts_ms = 0
                if ts_ms > 0:
                    first_ms = ts_ms if first_ms <= 0 else min(first_ms, ts_ms)
                    last_ms = max(last_ms, ts_ms)
                for key, value in event.items():
                    if not value:
                        continue
                    normalized_key = str(key).lower()
                    if any(token in normalized_key for token in ("guard", "reduce", "protect", "stop_trigger")):
                        guard_hits[str(key)] += 1
    except OSError:
        pass
    return {
        "event_count": count,
        "first_event_at": _iso_from_ms(first_ms),
        "last_event_at": _iso_from_ms(last_ms),
        "guard_hits": dict(sorted(guard_hits.items())),
    }


def build_strategy_analytics(
    *,
    output_dir: str | Path = "output",
    symbols: list[str] | None = None,
    market_type: str | None = None,
    limit_per_file: int = 0,
) -> dict[str, Any]:
    base = Path(output_dir)
    allowed_symbols = {str(item).upper().strip() for item in (symbols or []) if str(item).strip()}
    normalized_market = str(market_type or "").lower().strip()
    rows: list[dict[str, Any]] = []

    event_paths = sorted(
        {
            *base.glob("*_loop_events.jsonl"),
            *base.glob("*_spot_loop_events.jsonl"),
        }
    )
    for events_path in event_paths:
        symbol = _events_path_symbol(events_path)
        if allowed_symbols and symbol not in allowed_symbols:
            continue
        config = _load_runner_config(events_path)
        item_market_type = _market_type_for_path(events_path, config)
        if normalized_market and item_market_type != normalized_market:
            continue
        audit_paths = build_audit_paths(events_path)
        trade_rows = read_trade_audit_rows(audit_paths["trade_audit"], limit=max(int(limit_per_file), 0))
        income_rows = read_jsonl(audit_paths["income_audit"], limit=0)
        first_trade_ms = min((trade_row_time_ms(row) for row in trade_rows if trade_row_time_ms(row) > 0), default=0)
        last_trade_ms = max((trade_row_time_ms(row) for row in trade_rows if trade_row_time_ms(row) > 0), default=0)
        categories: dict[str, list[dict[str, Any]]] = {"grid": [], "reduce": [], "protection": [], "other": []}
        roles: defaultdict[str, int] = defaultdict(int)
        for trade in trade_rows:
            category = classify_trade(trade)
            categories.setdefault(category, []).append(trade)
            role = infer_trade_role(trade) or "unknown"
            roles[role] += 1
        overall = _bucket_summary(trade_rows)
        income = summarize_income(income_rows)
        funding_fee = _safe_float(income.get("funding_fee"))
        fingerprint, params = _param_fingerprint(config)
        rows.append(
            {
                "symbol": symbol,
                "market_type": item_market_type,
                "strategy_mode": _strategy_mode(config),
                "param_fingerprint": fingerprint,
                "params": params,
                "events_path": str(events_path),
                "trade_audit_path": str(audit_paths["trade_audit"]),
                "trade_count": overall["trade_count"],
                "gross_notional": overall["gross_notional"],
                "realized_pnl": overall["realized_pnl"],
                "commission": overall["commission"],
                "funding_fee": funding_fee,
                "net_pnl": overall["net_realized_pnl"] + funding_fee,
                "first_trade_at": _iso_from_ms(first_trade_ms),
                "last_trade_at": _iso_from_ms(last_trade_ms),
                "categories": {key: _bucket_summary(value) for key, value in categories.items()},
                "roles": dict(sorted(roles.items())),
                "events": _event_stats(events_path),
                "points": _build_points(trade_rows),
            }
        )

    summary = {
        "symbol_count": len({row["symbol"] for row in rows}),
        "run_count": len(rows),
        "trade_count": sum(int(row["trade_count"]) for row in rows),
        "gross_notional": sum(float(row["gross_notional"]) for row in rows),
        "realized_pnl": sum(float(row["realized_pnl"]) for row in rows),
        "commission": sum(float(row["commission"]) for row in rows),
        "funding_fee": sum(float(row["funding_fee"]) for row in rows),
        "net_pnl": sum(float(row["net_pnl"]) for row in rows),
    }
    mode_summary: dict[str, dict[str, Any]] = {}
    for row in rows:
        key = str(row["strategy_mode"])
        target = mode_summary.setdefault(
            key,
            {"strategy_mode": key, "run_count": 0, "trade_count": 0, "gross_notional": 0.0, "net_pnl": 0.0},
        )
        target["run_count"] += 1
        target["trade_count"] += int(row["trade_count"])
        target["gross_notional"] += float(row["gross_notional"])
        target["net_pnl"] += float(row["net_pnl"])
    return {
        "ok": True,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "summary": summary,
        "mode_summary": sorted(mode_summary.values(), key=lambda item: float(item["gross_notional"]), reverse=True),
        "rows": sorted(rows, key=lambda item: (str(item["symbol"]), str(item["strategy_mode"]), str(item["param_fingerprint"]))),
    }
