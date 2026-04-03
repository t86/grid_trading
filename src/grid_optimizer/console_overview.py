from __future__ import annotations

import base64
import copy
import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Any

import requests


_KNOWN_LINK_PATHS = {
    "/monitor": "monitor",
    "/spot_runner": "spot_runner",
    "/strategies": "strategies",
    "/spot_strategies": "spot_strategies",
    "/competition_board": "competition_board",
    "/basis": "basis",
}
_REMOTE_TIMEOUT_SECONDS = 4
_READ_TIMEOUT_RETRIES = 1
_OVERVIEW_CACHE_TTL_SECONDS = 10
_OVERVIEW_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
_OVERVIEW_CACHE_LOCK = threading.Lock()


def build_console_overview(registry: dict[str, Any], account_id: str) -> dict[str, Any]:
    accounts_by_id = registry.get("accounts_by_id") or {}
    accounts = registry.get("accounts") or []
    account = accounts_by_id.get(account_id)
    if account is None:
        account = next((item for item in accounts if item.get("id") == account_id), None)
    if account is None:
        raise ValueError(f"Unknown account_id: {account_id}")

    servers_by_id = registry.get("servers_by_id") or {}
    server = servers_by_id.get(account.get("server_id"))
    if server is None:
        raise ValueError(f"Unknown server_id for account {account_id}: {account.get('server_id')}")

    cache_key = _overview_cache_key(registry, account, server)
    cached = _get_cached_console_overview(cache_key)
    if cached is not None:
        return cached

    warnings: list[str] = []
    with ThreadPoolExecutor(max_workers=4) as executor:
        future_health = executor.submit(_run_overview_section, _fetch_health, server)
        future_futures = executor.submit(_run_overview_section, _fetch_market_overview, server, account, market="futures")
        future_spot = executor.submit(_run_overview_section, _fetch_market_overview, server, account, market="spot")
        future_competitions = executor.submit(_run_overview_section, _fetch_competitions, registry, account)

        health, health_warnings = future_health.result()
        futures, futures_warnings = future_futures.result()
        spot, spot_warnings = future_spot.result()
        competitions, competitions_warnings = future_competitions.result()

    warnings.extend(health_warnings)
    warnings.extend(futures_warnings)
    warnings.extend(spot_warnings)
    warnings.extend(competitions_warnings)
    links = build_console_links(server, account)

    overview = {
        "ok": True,
        "account": account,
        "server": server,
        "health": health,
        "summary": _build_summary(account, health, futures, spot, competitions, warnings),
        "futures": futures,
        "spot": spot,
        "competitions": competitions,
        "links": links,
        "warnings": warnings,
        "fetched_at": datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds"),
    }
    _set_cached_console_overview(cache_key, overview)
    return copy.deepcopy(overview)


def build_console_links(server: dict[str, Any], account: dict[str, Any]) -> dict[str, str]:
    base_url = str(server.get("base_url", "")).strip().rstrip("/")
    links: dict[str, str] = {}
    for path in account.get("pages", []):
        key = _KNOWN_LINK_PATHS.get(path)
        if key:
            links[key] = f"{base_url}{path}"
    return links


def _fetch_remote_json(
    server: dict[str, Any],
    path: str,
    params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    server_id = str(server.get("id", "")).strip().upper()
    username_key = f"GRID_NODE_{server_id}_USERNAME"
    password_key = f"GRID_NODE_{server_id}_PASSWORD"
    username = os.getenv(username_key)
    password = os.getenv(password_key)
    if not username or not password:
        raise RuntimeError(f"Missing Basic Auth credentials for server_id {server_id}")
    token = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")
    base_url = str(server.get("base_url", "")).strip().rstrip("/")
    url = f"{base_url}{path}"
    last_error: Exception | None = None
    for attempt in range(_READ_TIMEOUT_RETRIES + 1):
        try:
            response = requests.get(
                url,
                headers={"Authorization": f"Basic {token}", "Accept": "application/json"},
                params=params or None,
                timeout=_REMOTE_TIMEOUT_SECONDS,
            )
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict):
                raise RuntimeError(f"Unexpected JSON payload from {url}")
            return payload
        except requests.ReadTimeout as exc:
            last_error = exc
            if attempt >= _READ_TIMEOUT_RETRIES:
                raise
    if last_error is not None:
        raise last_error
    raise RuntimeError(f"Unreachable remote fetch state for {url}")


def _run_overview_section(func: Any, *args: Any, **kwargs: Any) -> tuple[Any, list[str]]:
    section_warnings: list[str] = []
    result = func(*args, section_warnings, **kwargs)
    return result, section_warnings


def _fetch_health(server: dict[str, Any], warnings: list[str]) -> dict[str, Any]:
    try:
        payload = _fetch_remote_json(server, "/api/health")
    except Exception as exc:
        warnings.append(f"health unavailable for {server.get('id')}: {exc}")
        return {"ok": False, "status": "offline", "error": f"{type(exc).__name__}: {exc}"}
    return {
        "ok": bool(payload.get("ok", True)),
        "status": "online" if payload.get("ok", True) else "offline",
        "error": None if payload.get("ok", True) else str(payload.get("error", "")) or "health check failed",
    }


def _fetch_market_overview(
    server: dict[str, Any],
    account: dict[str, Any],
    warnings: list[str],
    *,
    market: str,
) -> list[dict[str, Any]]:
    if not _account_implies_market(account, market):
        return []

    symbols = [
        symbol
        for symbol in (account.get("default_symbols") or [])
        if isinstance(symbol, str) and symbol.strip()
    ]
    if not symbols:
        return []

    path = "/api/loop_monitor" if market == "futures" else "/api/spot_runner/status"
    overview: list[dict[str, Any]] = []
    for symbol in symbols:
        try:
            payload = _fetch_remote_json(server, path, params={"symbol": symbol})
        except Exception as exc:
            warnings.append(f"{market} unavailable for {symbol}: {exc}")
            continue
        overview.append(_normalize_market_snapshot(symbol, payload))
    return overview


def _fetch_competitions(
    registry: dict[str, Any],
    account: dict[str, Any],
    warnings: list[str],
) -> list[dict[str, Any]]:
    wanted = {
        str(symbol).strip().upper()
        for symbol in (account.get("competition_symbols") or [])
        if isinstance(symbol, str) and symbol.strip()
    }
    if not wanted:
        return []

    competition_source = registry.get("competition_source") or {}
    servers_by_id = registry.get("servers_by_id") or {}
    source_server = servers_by_id.get(competition_source.get("server_id"))
    if source_server is None:
        warnings.append("competition source is unavailable")
        return []
    try:
        payload = _fetch_remote_json(source_server, str(competition_source.get("path", "/api/competition_board")))
    except Exception as exc:
        warnings.append(f"competition unavailable for {source_server.get('id')}: {exc}")
        return []

    snapshot = payload.get("snapshot") if isinstance(payload, dict) else None
    boards = []
    if isinstance(snapshot, dict):
        candidate = snapshot.get("boards", [])
        if isinstance(candidate, list):
            boards = candidate
    elif isinstance(payload, list):
        boards = payload

    competitions: list[dict[str, Any]] = []
    for board in boards:
        if not isinstance(board, dict):
            continue
        symbol = _board_symbol(board)
        if symbol not in wanted:
            continue
        competitions.append(
            {
                "symbol": symbol,
                "market": board.get("market"),
                "label": board.get("label") or board.get("tabLabel") or symbol,
                "url": board.get("url"),
                "activity_end_at": board.get("activity_end_at"),
                "current_floor": board.get("current_floor_value_text") or board.get("current_floor_value"),
            }
        )
    return competitions


def _board_symbol(board: dict[str, Any]) -> str:
    for key in ("symbol", "rewardSymbol", "reward_unit", "rewardUnit", "asset"):
        value = board.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip().upper()
    return ""


def _account_implies_market(account: dict[str, Any], market: str) -> bool:
    kind = str(account.get("kind", "")).strip().lower()
    pages = {str(path).strip() for path in (account.get("pages") or []) if isinstance(path, str)}
    if market == "futures":
        return kind in {"futures", "mixed"} and bool(pages & {"/monitor", "/strategies"})
    if market == "spot":
        return kind in {"spot", "mixed"} and bool(pages & {"/spot_runner", "/spot_strategies"})
    return False


def _normalize_market_snapshot(symbol: str, payload: dict[str, Any]) -> dict[str, Any]:
    snapshot = payload.get("snapshot") if isinstance(payload, dict) else None
    if not isinstance(snapshot, dict):
        snapshot = {}
    status = snapshot.get("runner_status") or snapshot.get("status") or ("online" if payload.get("ok", True) else "offline")
    return {
        "symbol": symbol,
        "ok": bool(payload.get("ok", True)),
        "status": status,
        "snapshot": snapshot,
    }


def _build_summary(
    account: dict[str, Any],
    health: dict[str, Any],
    futures: list[dict[str, Any]],
    spot: list[dict[str, Any]],
    competitions: list[dict[str, Any]],
    warnings: list[str],
) -> dict[str, Any]:
    return {
        "account_kind": account.get("kind"),
        "health_status": health.get("status"),
        "futures_count": len(futures),
        "spot_count": len(spot),
        "competition_count": len(competitions),
        "warning_count": len(warnings),
        "primary_status": "degraded" if warnings else "healthy",
    }


def _overview_cache_key(
    registry: dict[str, Any],
    account: dict[str, Any],
    server: dict[str, Any],
) -> str:
    competition_source = registry.get("competition_source") or {}
    key_payload = {
        "account": {
            "id": account.get("id"),
            "server_id": account.get("server_id"),
            "kind": account.get("kind"),
            "default_symbols": account.get("default_symbols") or [],
            "competition_symbols": account.get("competition_symbols") or [],
            "pages": account.get("pages") or [],
        },
        "server": {
            "id": server.get("id"),
            "base_url": server.get("base_url"),
        },
        "competition_source": {
            "server_id": competition_source.get("server_id"),
            "path": competition_source.get("path"),
        },
    }
    return json.dumps(key_payload, sort_keys=True, ensure_ascii=False)


def _get_cached_console_overview(cache_key: str) -> dict[str, Any] | None:
    now = time.monotonic()
    with _OVERVIEW_CACHE_LOCK:
        cached = _OVERVIEW_CACHE.get(cache_key)
        if cached is None:
            return None
        cached_at, payload = cached
        if now - cached_at > _OVERVIEW_CACHE_TTL_SECONDS:
            _OVERVIEW_CACHE.pop(cache_key, None)
            return None
        return copy.deepcopy(payload)


def _set_cached_console_overview(cache_key: str, overview: dict[str, Any]) -> None:
    with _OVERVIEW_CACHE_LOCK:
        _OVERVIEW_CACHE[cache_key] = (time.monotonic(), copy.deepcopy(overview))


def _clear_console_overview_cache() -> None:
    with _OVERVIEW_CACHE_LOCK:
        _OVERVIEW_CACHE.clear()
