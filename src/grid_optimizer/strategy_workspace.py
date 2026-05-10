from __future__ import annotations

from urllib.parse import urlparse
from typing import Any


_METRIC_FIELDS = (
    "total_volume",
    "recent_hour_volume",
    "total_pnl",
    "trade_pnl",
    "unrealized_pnl",
    "fees",
    "funding_fee",
    "open_order_count",
)
_STATUS_PRIORITY = {"running": 0, "saved_idle": 1, "idle": 2, "offline": 3}


def _normalized_text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _normalized_symbol(value: Any) -> str | None:
    text = str(value or "").upper().strip()
    return text or None


def _safe_float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _server_id(server: dict[str, Any]) -> str | None:
    return _normalized_text(server.get("server_id") or server.get("id"))


def _server_label(server: dict[str, Any]) -> str | None:
    return _normalized_text(server.get("server_label") or server.get("label"))


def _server_base_url(server: dict[str, Any]) -> str | None:
    return _normalized_text(server.get("server_base_url") or server.get("base_url") or server.get("url"))


def _server_host(server: dict[str, Any]) -> str | None:
    base_url = _server_base_url(server)
    if not base_url:
        return None
    parsed = urlparse(base_url if "://" in base_url else f"http://{base_url}")
    return _normalized_text(parsed.netloc or parsed.path)


def _server_source(server: dict[str, Any]) -> str:
    parts = [
        part
        for part in (_server_label(server), _server_id(server), _server_host(server))
        if part
    ]
    return " / ".join(parts) or "--"


def _card_status(card: dict[str, Any], default_status: str) -> str:
    if bool(card.get("is_running") or card.get("running")):
        return "running"
    return default_status


def _iter_server_cards(server: dict[str, Any]) -> list[tuple[dict[str, Any], str]]:
    groups = server.get("groups") if isinstance(server.get("groups"), dict) else {}
    cards: list[tuple[dict[str, Any], str]] = []
    for item in groups.get("running") or []:
        if isinstance(item, dict):
            cards.append((item, "running"))
    for item in groups.get("saved_idle") or []:
        if isinstance(item, dict):
            cards.append((item, "saved_idle"))
    if cards:
        return cards
    return [
        (item, "idle")
        for item in server.get("symbols") or []
        if isinstance(item, dict)
    ]


def _normalize_cell(server: dict[str, Any], card: dict[str, Any], default_status: str) -> dict[str, Any]:
    status = _card_status(card, default_status)
    ok = bool(server.get("ok", True))
    fees = card.get("fees") if card.get("fees") is not None else card.get("total_fees")
    return {
        "server_id": _server_id(server),
        "server_label": _server_label(server),
        "server_base_url": _server_base_url(server),
        "server_host": _server_host(server),
        "server_source": _server_source(server),
        "ok": ok,
        "status": "offline" if not ok else status,
        "is_running": bool(card.get("is_running") or card.get("running")),
        "strategy_profile": card.get("strategy_profile"),
        "strategy_name": card.get("strategy_name"),
        "strategy_mode": card.get("strategy_mode"),
        "config": card.get("config"),
        "target_url": card.get("target_url"),
        "total_volume": _safe_float(card.get("total_volume")),
        "recent_hour_volume": _safe_float(card.get("recent_hour_volume")),
        "total_pnl": _safe_float(card.get("total_pnl")),
        "trade_pnl": _safe_float(card.get("trade_pnl")),
        "unrealized_pnl": _safe_float(card.get("unrealized_pnl")),
        "fees": _safe_float(fees),
        "funding_fee": _safe_float(card.get("funding_fee")),
        "open_order_count": _safe_float(card.get("open_order_count")),
        "position_summary": card.get("position_summary") or card.get("current_position_display"),
        "updated_at": card.get("updated_at"),
        "raw": card,
    }


def _group_status(cells: list[dict[str, Any]]) -> str:
    if any(bool(cell.get("is_running")) or cell.get("status") == "running" for cell in cells):
        return "running"
    if any(cell.get("status") == "saved_idle" for cell in cells):
        return "saved_idle"
    if cells and all(not bool(cell.get("ok")) for cell in cells):
        return "offline"
    return "idle"


def _group_totals(cells: list[dict[str, Any]]) -> dict[str, float]:
    ok_cells = [cell for cell in cells if cell.get("ok")]
    return {
        field: sum(_safe_float(cell.get(field)) for cell in ok_cells)
        for field in _METRIC_FIELDS
    }


def _server_error(server: dict[str, Any]) -> dict[str, Any] | None:
    if bool(server.get("ok", True)):
        return None
    return {
        "server_id": _server_id(server),
        "server_label": _server_label(server),
        "server_base_url": _server_base_url(server),
        "url": _normalized_text(server.get("url")),
        "error": _normalized_text(server.get("error")),
    }


def build_strategy_workspace_payload(running_status_payload: dict[str, Any]) -> dict[str, Any]:
    """Build a pure symbol-first workspace payload from running status data."""
    source = running_status_payload if isinstance(running_status_payload, dict) else {}
    groups: dict[str, list[dict[str, Any]]] = {}
    server_errors: list[dict[str, Any]] = []

    for server in source.get("servers") or []:
        if not isinstance(server, dict):
            continue
        error = _server_error(server)
        if error is not None:
            server_errors.append(error)
        for card, default_status in _iter_server_cards(server):
            symbol = _normalized_symbol(card.get("symbol"))
            if symbol is None:
                continue
            groups.setdefault(symbol, []).append(_normalize_cell(server, card, default_status))

    symbol_groups = [
        {
            "symbol": symbol,
            "status": _group_status(cells),
            "servers": cells,
            "totals": _group_totals(cells),
        }
        for symbol, cells in groups.items()
    ]
    symbol_groups.sort(key=lambda item: (_STATUS_PRIORITY.get(str(item["status"]), 99), item["symbol"]))

    result: dict[str, Any] = {
        "ok": bool(source.get("ok", True)),
        "view_mode": source.get("view_mode") or source.get("scope"),
        "summary": source.get("summary") if isinstance(source.get("summary"), dict) else {},
        "symbols": [str(item["symbol"]) for item in symbol_groups],
        "symbols_by_symbol": symbol_groups,
        "server_errors": server_errors,
    }
    if "ts" in source:
        result["ts"] = source.get("ts")
    return result
