from __future__ import annotations

import argparse
import base64
import binascii
from concurrent.futures import ThreadPoolExecutor
import hmac
import json
import math
import os
from pathlib import Path
import re
import threading
import time
from datetime import datetime, timedelta, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable
from urllib.parse import parse_qs, urlparse

from . import alpha_volume_alert as alert
from .alpha_market import AlphaMarketClient
from .alpha_competition_discovery import CompetitionDiscoveryCache, CompetitionDiscoveryService
from .alpha_competition_history import CompetitionHistoryStore, DEFAULT_HISTORY_PATH
from .alpha_competition_reward import fetch_ended_reward
from .alpha_competition_metrics import (
    BinanceCompetitionRuleProvider,
    CompetitionMetricsService,
    CompetitionRuleCache,
    CompetitionVolumeProvider,
)
from .volume_spike_monitor import send_bark_alert


DEFAULT_HOST = os.environ.get("ALPHA_DASHBOARD_HOST", "0.0.0.0")
DEFAULT_PORT = int(os.environ.get("ALPHA_DASHBOARD_PORT", "8796"))
DEFAULT_SYMBOLS = ("QUID", "GRVT", "O", "PRL", "CAP")
DEFAULT_RULE_CACHE = "/home/ubuntu/.cache/binance-alpha-volume-alert/competition_rules.json"
DEFAULT_DISCOVERY_CACHE = "/home/ubuntu/.cache/binance-alpha-volume-alert/competition_discovery.json"
DEFAULT_HISTORY_CACHE = str(DEFAULT_HISTORY_PATH)
DEFAULT_HOURLY_ALERT_STATE = "/home/ubuntu/.cache/binance-alpha-volume-alert/hourly_volume_alert_state.json"
DEFAULT_HOURLY_BARK_CONFIG = "/home/ubuntu/.config/binance-alpha-bark.json"

_COMPETITION_SERVICE: CompetitionMetricsService | None = None
_COMPETITION_SERVICE_LOCK = threading.Lock()
_DISCOVERY_SERVICE: CompetitionDiscoveryService | None = None
_DISCOVERY_SERVICE_LOCK = threading.Lock()
_HISTORY_STORE: CompetitionHistoryStore | None = None
_HISTORY_STORE_LOCK = threading.Lock()
_HISTORY_REFERENCE_LOCK = threading.Lock()
_HISTORY_REWARD_LOCK = threading.Lock()
_ALERT_CHECK_LOCK = threading.Lock()
_HOURLY_ALERT_LOCK = threading.Lock()
_SNAPSHOT_SYMBOL_RE = re.compile(r"[A-Z0-9_]{1,32}")
_MAX_SNAPSHOT_SYMBOLS = 32
_ALLOWED_METHODS = {
    "/": "GET",
    "/api/snapshot": "GET",
    "/api/competition": "GET",
    "/api/history": "GET, POST",
    "/api/check": "POST",
}


class AuthConfigurationError(RuntimeError):
    """Dashboard Basic Auth credentials are missing or incomplete."""


def _configured_auth_credentials() -> tuple[str, str]:
    alpha_username = os.environ.get("ALPHA_DASHBOARD_USERNAME")
    alpha_password = os.environ.get("ALPHA_DASHBOARD_PASSWORD")
    if alpha_username is not None or alpha_password is not None:
        if not alpha_username or not alpha_password:
            raise AuthConfigurationError("dashboard credentials are incomplete")
        return alpha_username, alpha_password

    grid_username = os.environ.get("GRID_WEB_USERNAME")
    grid_password = os.environ.get("GRID_WEB_PASSWORD")
    if not grid_username or not grid_password:
        raise AuthConfigurationError("dashboard credentials are missing or incomplete")
    return grid_username, grid_password


def _env_truthy(name: str, default: str = "0") -> bool:
    return os.environ.get(name, default).strip().lower() in {"1", "true", "yes", "on"}


def _fallback_symbols_from_env() -> list[str]:
    raw = os.environ.get("ALPHA_SYMBOLS", ",".join(DEFAULT_SYMBOLS))
    return [symbol.strip().upper() for symbol in raw.split(",") if symbol.strip()]


def _symbols_from_env() -> list[str]:
    fallback = _fallback_symbols_from_env()
    if not _env_truthy("ALPHA_AUTO_SYMBOLS", "0"):
        return fallback
    try:
        service = competition_service()
        active = service.active_recent_symbols()
    except Exception:
        return fallback
    return active or fallback


def _symbols_from_query(query: str) -> list[str]:
    values = parse_qs(query, keep_blank_values=True).get("symbols")
    if values is None:
        return _symbols_from_env()
    if len(values) != 1:
        raise ValueError("invalid symbols query")
    raw_symbols = values[0].split(",")
    if not 1 <= len(raw_symbols) <= _MAX_SNAPSHOT_SYMBOLS:
        raise ValueError("invalid symbols query")
    symbols: list[str] = []
    seen: set[str] = set()
    for symbol in raw_symbols:
        if _SNAPSHOT_SYMBOL_RE.fullmatch(symbol) is None:
            raise ValueError("invalid symbols query")
        if symbol not in seen:
            seen.add(symbol)
            symbols.append(symbol)
    return symbols


def _finite_float(value: Any, default: float = 0.0) -> float:
    fallback = alert._safe_float(default)
    if not math.isfinite(fallback):
        fallback = 0.0
    parsed = alert._safe_float(value, fallback)
    return parsed if math.isfinite(parsed) else fallback


def _finite_sum(values: list[float]) -> float:
    total = sum(values)
    return total if math.isfinite(total) else 0.0


def _hourly_alert_threshold() -> float:
    threshold = _finite_float(os.environ.get("ALPHA_HOURLY_ALERT_THRESHOLD", "10000000"), 10000000.0)
    return threshold if threshold > 0 else 10000000.0


def _hourly_alert_state_path() -> Path:
    return Path(os.environ.get("ALPHA_HOURLY_ALERT_STATE_FILE", DEFAULT_HOURLY_ALERT_STATE))


def _hourly_alert_bark_config_path() -> Path | None:
    raw = os.environ.get("ALPHA_HOURLY_BARK_CONFIG_PATH", DEFAULT_HOURLY_BARK_CONFIG).strip()
    return Path(raw) if raw else None


def _load_hourly_alert_state(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {"version": 1, "symbols": {}}
    if not isinstance(payload, dict):
        return {"version": 1, "symbols": {}}
    symbols = payload.get("symbols")
    if not isinstance(symbols, dict):
        payload["symbols"] = {}
    payload.setdefault("version", 1)
    return payload


def _save_hourly_alert_state(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(json.dumps(state, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    temporary.replace(path)


def _beijing_date_key(now: datetime) -> str:
    return now.astimezone(timezone(timedelta(hours=8))).date().isoformat()


def _format_hourly_volume(value: float) -> str:
    return f"{value:,.2f} USDT"


def _format_hourly_alert_body(row: dict[str, Any], threshold: float, now: datetime) -> str:
    return "\n".join(
        [
            f"{row.get('symbol', '')} 最近 1 小时成交额突破 {_format_hourly_volume(threshold)}。",
            "",
            f"最近1小时成交额: {_format_hourly_volume(_finite_float(row.get('latest1hQuoteVolume')))}",
            f"最近1分钟成交额: {_format_hourly_volume(_finite_float(row.get('latest1mQuoteVolume')))}",
            f"24小时成交额: {_format_hourly_volume(_finite_float(row.get('tickerQuoteVolume24h')))}",
            f"最新价格: {_finite_float(row.get('lastPrice')):.10g}",
            f"Alpha ID: {row.get('alphaId', '')}",
            f"交易对: {row.get('pair', '')}",
            f"检查时间UTC: {now.isoformat(timespec='seconds')}",
        ]
    )


def _send_hourly_volume_alert(row: dict[str, Any], threshold: float, now: datetime) -> dict[str, Any]:
    symbol = str(row.get("symbol") or "").upper()
    subject = f"Alpha小时成交额突破1000万: {symbol}"
    body = _format_hourly_alert_body(row, threshold, now)
    result: dict[str, Any] = {"symbol": symbol, "email": {"sent": False}, "bark": {"sent": False}}
    try:
        alert.send_email(subject, body)
        result["email"] = {"sent": True}
    except Exception as exc:
        result["email"] = {"sent": False, "error": f"{type(exc).__name__}: {exc}"}
    try:
        result["bark"] = send_bark_alert(title=subject, body=body, config_path=_hourly_alert_bark_config_path())
    except Exception as exc:
        result["bark"] = {"sent": False, "error": f"{type(exc).__name__}: {exc}"}
    return result


def check_hourly_volume_breakouts(rows: list[dict[str, Any]]) -> dict[str, Any]:
    enabled = _env_truthy("ALPHA_HOURLY_ALERT_ENABLED", "0")
    threshold = _hourly_alert_threshold()
    result: dict[str, Any] = {
        "enabled": enabled,
        "threshold": threshold,
        "triggered": [],
        "checked": 0,
    }
    if not enabled:
        return result

    now = datetime.now(timezone.utc)
    date_key = _beijing_date_key(now)
    state_path = _hourly_alert_state_path()
    with _HOURLY_ALERT_LOCK:
        state = _load_hourly_alert_state(state_path)
        symbols_state = state.setdefault("symbols", {})
        if not isinstance(symbols_state, dict):
            symbols_state = {}
            state["symbols"] = symbols_state

        for row in rows:
            symbol = str(row.get("symbol") or "").upper()
            if not symbol:
                continue
            volume = _finite_float(row.get("latest1hQuoteVolume"))
            above = volume >= threshold
            previous = symbols_state.get(symbol)
            if not isinstance(previous, dict):
                previous = {}
            had_previous = symbol in symbols_state
            was_above = bool(previous.get("above")) if had_previous else above
            already_triggered_today = previous.get("lastTriggerDate") == date_key
            send_now = had_previous and (not was_above) and above and not already_triggered_today
            send_result = None
            if send_now:
                send_result = _send_hourly_volume_alert(row, threshold, now)
                result["triggered"].append(send_result)
            symbols_state[symbol] = {
                **previous,
                "above": above,
                "lastVolume": volume,
                "lastCheckedAtUtc": now.isoformat(timespec="seconds"),
                "lastThreshold": threshold,
            }
            if send_now:
                symbols_state[symbol]["lastTriggerDate"] = date_key
                symbols_state[symbol]["lastTriggerAtUtc"] = now.isoformat(timespec="seconds")
                symbols_state[symbol]["lastSendResult"] = send_result
            result["checked"] += 1

        state["updatedAtUtc"] = now.isoformat(timespec="seconds")
        try:
            _save_hourly_alert_state(state_path, state)
        except OSError as exc:
            result["stateError"] = f"{type(exc).__name__}: {exc}"
    return result


_LEADERBOARD_ENDPOINTS = (
    "/bapi/defi/v1/private/wallet-direct/buw/competition/rank/list",
    "/bapi/defi/v1/private/wallet-direct/buw/wallet/competition/rank/list",
    "/bapi/defi/v1/private/wallet-direct/buw/activity/competition/rank/list",
    "/bapi/defi/v1/private/wallet-direct/buw/wallet/activity/competition/rank/list",
)
_LEADERBOARD_SOURCE = "binance_private_api"
_LEADERBOARD_SUCCESS_TTL_SECONDS = 300.0
_LEADERBOARD_FAILURE_TTL_SECONDS = 60.0


def _leaderboard_result(
    row: dict[str, Any],
    *,
    threshold: float | None = None,
    note: str,
    updated_at: str | None = None,
    winner_count: int | None = None,
) -> dict[str, Any]:
    if winner_count is None:
        parsed_winner_count = _exact_integer(row.get("winnerCount"))
        winner_count = parsed_winner_count if parsed_winner_count is not None and parsed_winner_count > 0 else None
    return {
        "leaderboardThreshold": threshold,
        "leaderboardThresholdRank": winner_count,
        "leaderboardThresholdUpdatedAt": updated_at,
        "leaderboardThresholdUpdatedAtLabel": "" if updated_at else None,
        "leaderboardThresholdSource": _LEADERBOARD_SOURCE,
        "leaderboardThresholdNote": note,
    }


def _leaderboard_identity(row: dict[str, Any]) -> tuple[str, str] | None:
    symbol = str(row.get("symbol") or "").strip().upper()
    article_url = str(row.get("articleUrl") or "").strip()
    article_code = urlparse(article_url).path.rstrip("/").split("/")[-1]
    if not symbol or not article_code:
        return None
    return symbol, article_code


class _BoundedSubmitter:
    def __init__(self, *, max_workers: int, max_pending: int) -> None:
        self._executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="alpha-leaderboard")
        self._capacity = threading.BoundedSemaphore(max_pending)

    def submit(self, task: Callable[[], None]) -> bool:
        if not self._capacity.acquire(blocking=False):
            return False

        def run() -> None:
            try:
                task()
            finally:
                self._capacity.release()

        try:
            self._executor.submit(run)
        except RuntimeError:
            self._capacity.release()
            return False
        return True


class _LeaderboardThresholdRefresher:
    def __init__(
        self,
        *,
        fetcher: Callable[[dict[str, Any]], dict[str, Any]],
        submitter: Any,
        clock: Callable[[], float] = time.monotonic,
        success_ttl: float = _LEADERBOARD_SUCCESS_TTL_SECONDS,
        failure_ttl: float = _LEADERBOARD_FAILURE_TTL_SECONDS,
    ) -> None:
        self._fetcher = fetcher
        self._submitter = submitter
        self._clock = clock
        self._success_ttl = success_ttl
        self._failure_ttl = failure_ttl
        self._lock = threading.Lock()
        self._cache: dict[tuple[str, str], tuple[float, int, dict[str, Any]]] = {}
        self._inflight: set[tuple[str, str]] = set()

    def get(self, row: dict[str, Any]) -> dict[str, Any]:
        identity = _leaderboard_identity(row)
        winner_count = _exact_integer(row.get("winnerCount"))
        if identity is None or winner_count is None or winner_count <= 0:
            return _leaderboard_result(row, note="leaderboard API unavailable")

        now = self._clock()
        with self._lock:
            cached = self._cache.get(identity)
            if cached is not None and cached[0] > now and cached[1] == winner_count:
                return dict(cached[2])
            if identity in self._inflight:
                return _leaderboard_result(row, note="leaderboard refresh in progress")
            self._inflight.add(identity)

        task_row = dict(row)
        accepted = self._submitter.submit(lambda: self._refresh(identity, winner_count, task_row))
        if accepted is False:
            with self._lock:
                self._inflight.discard(identity)
            return _leaderboard_result(row, note="leaderboard refresh capacity unavailable")
        return _leaderboard_result(row, note="leaderboard refresh in progress")

    def _refresh(self, identity: tuple[str, str], winner_count: int, row: dict[str, Any]) -> None:
        try:
            result = self._fetcher(row)
        except (Exception, SystemExit):
            result = _leaderboard_result(row, note="leaderboard API unavailable")
        ttl = self._success_ttl if result.get("leaderboardThreshold") is not None else self._failure_ttl
        with self._lock:
            self._cache[identity] = (self._clock() + ttl, winner_count, dict(result))
            self._inflight.discard(identity)


def _binance_web_cookie() -> str:
    cookie = os.environ.get("BINANCE_WEB_COOKIE") or os.environ.get("ALPHA_BINANCE_WEB_COOKIE") or ""
    cookie_file = os.environ.get("BINANCE_WEB_COOKIE_FILE") or os.environ.get("ALPHA_BINANCE_WEB_COOKIE_FILE")
    if not cookie and cookie_file:
        try:
            cookie = Path(cookie_file).read_text(encoding="utf-8").strip()
        except OSError:
            cookie = ""
    return cookie.strip()


def _find_numeric_field(value: Any, keys: set[str]) -> float | None:
    if not isinstance(value, dict):
        return None
    for key, item in value.items():
        key_text = str(key).replace("_", "").lower()
        if key_text in keys:
            return _non_negative_number(item)
    return None


def _non_negative_number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        parsed = float(value)
    elif isinstance(value, str):
        if not re.fullmatch(r"(?:0|[1-9]\d*)(?:\.\d+)?", value):
            return None
        parsed = float(value)
    else:
        return None
    return parsed if math.isfinite(parsed) and parsed >= 0 else None


def _exact_integer(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value) if math.isfinite(value) and value.is_integer() else None
    if isinstance(value, str):
        if not re.fullmatch(r"0|[1-9]\d*", value):
            return None
        return int(value)
    return None


def _find_ranked_rows(value: Any) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if isinstance(value, dict):
        rank_value = _exact_integer(value.get("rank"))
        volume_value = _find_numeric_field(value, {"volume", "amount", "threshold", "quotevolume"})
        if rank_value is not None and volume_value is not None:
            rows.append(value)
        for item in value.values():
            rows.extend(_find_ranked_rows(item))
    elif isinstance(value, list):
        for item in value:
            rows.extend(_find_ranked_rows(item))
    return rows


_LEADERBOARD_LIST_KEYS = {"rows", "ranklist", "leaderboard", "list"}


def _direct_text_field(value: dict[str, Any], keys: set[str]) -> str | None:
    found: set[str] = set()
    for key, item in value.items():
        if str(key).replace("_", "").lower() in keys and isinstance(item, (str, int)) and not isinstance(item, bool):
            text = str(item).strip()
            if text:
                found.add(text)
    return next(iter(found)) if len(found) == 1 else None


def _matching_competition_rank_trees(value: Any, identity: tuple[str, str]) -> list[Any]:
    trees: list[Any] = []
    if isinstance(value, dict):
        symbol = _direct_text_field(value, {"symbol", "tokensymbol"})
        article_code = _direct_text_field(value, {"articlecode", "articleid"})
        if symbol is not None and article_code is not None and (symbol.upper(), article_code) == identity:
            for key, item in value.items():
                if str(key).replace("_", "").lower() in _LEADERBOARD_LIST_KEYS:
                    trees.append(item)
        for item in value.values():
            trees.extend(_matching_competition_rank_trees(item, identity))
    elif isinstance(value, list):
        for item in value:
            trees.extend(_matching_competition_rank_trees(item, identity))
    return trees


def _fetch_leaderboard_threshold(
    row: dict[str, Any],
    *,
    session_factory: Callable[[], Any] | None = None,
) -> dict[str, Any]:
    cookie = _binance_web_cookie()
    if not cookie:
        return _leaderboard_result(row, note="BINANCE_WEB_COOKIE is not configured")

    import requests

    identity = _leaderboard_identity(row)
    winner_count = _exact_integer(row.get("winnerCount"))
    if identity is None or winner_count is None or winner_count <= 0:
        return _leaderboard_result(row, note="leaderboard API unavailable", winner_count=None)
    symbol, article_code = identity
    page_size = 20
    page_no = max(1, (winner_count + page_size - 1) // page_size)
    params_variants = (
        {"symbol": symbol, "pageNo": page_no, "pageSize": page_size},
        {"tokenSymbol": symbol, "pageNo": page_no, "pageSize": page_size},
        {"articleCode": article_code, "symbol": symbol, "pageNo": page_no, "pageSize": page_size},
    )
    headers = {
        "Accept": "application/json, text/plain, */*",
        "User-Agent": "Mozilla/5.0",
        "clienttype": "web",
        "Cookie": cookie,
        "Referer": "https://www.binance.com/en/activity",
    }
    make_session = requests.Session if session_factory is None else session_factory
    with make_session() as session:
        for endpoint in _LEADERBOARD_ENDPOINTS:
            for params in params_variants:
                params = {key: value for key, value in params.items() if value not in (None, "")}
                try:
                    response = session.get(f"https://www.binance.com{endpoint}", params=params, headers=headers, timeout=12)
                    payload = response.json()
                except (Exception, SystemExit):
                    continue
                if isinstance(payload, dict) and str(payload.get("code")) == "100001005":
                    return _leaderboard_result(row, note="Binance login cookie is invalid or expired")
                for rank_tree in _matching_competition_rank_trees(payload, identity):
                    for item in _find_ranked_rows(rank_tree):
                        rank = _exact_integer(item.get("rank"))
                        volume = _find_numeric_field(item, {"volume", "amount", "threshold", "quotevolume"})
                        if rank != winner_count or volume is None:
                            continue
                        return _leaderboard_result(
                            row,
                            threshold=volume,
                            note="available",
                            updated_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
                            winner_count=winner_count,
                        )
    return _leaderboard_result(row, note="leaderboard API unavailable", winner_count=winner_count)


_LEADERBOARD_REFRESHER = _LeaderboardThresholdRefresher(
    fetcher=_fetch_leaderboard_threshold,
    submitter=_BoundedSubmitter(max_workers=2, max_pending=4),
)


def _apply_leaderboard_thresholds(
    payload: dict[str, Any],
    *,
    refresher: _LeaderboardThresholdRefresher = _LEADERBOARD_REFRESHER,
) -> dict[str, Any]:
    for row in payload.get("rows", []):
        if not isinstance(row, dict):
            continue
        row.setdefault("leaderboardThreshold", None)
        row.setdefault("leaderboardThresholdRank", None)
        row.setdefault("leaderboardThresholdUpdatedAt", None)
        row.setdefault("leaderboardThresholdUpdatedAtLabel", None)
        row.setdefault("leaderboardThresholdSource", None)
        row.setdefault("leaderboardThresholdNote", None)
        if row.get("status") == "active":
            row.update(refresher.get(row))
    return payload


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return alert._safe_int(value, default)
    except OverflowError:
        return default


def _ms_to_iso(ms: int) -> str:
    if ms <= 0:
        return ""
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat(timespec="seconds")


def _closed_1m_rows(pair: str, market: Any) -> list[list[Any]]:
    rows = market.fetch_klines(pair, interval="1m", limit=63)
    return rows[:-1] if len(rows) >= 2 else rows


def _snapshot_row(token: Any, client: Any) -> dict[str, Any]:
    closed_rows = _closed_1m_rows(token.pair, client)
    latest = closed_rows[-1] if closed_rows else None
    previous = closed_rows[-2] if len(closed_rows) >= 2 else None
    baseline_rows = closed_rows[-21:-1]
    baseline_values = [
        _finite_float(row[7])
        for row in baseline_rows
        if _finite_float(row[7]) > 0
    ]
    if not baseline_values:
        baseline_values = [_finite_float(row[7]) for row in baseline_rows]
    baseline = _finite_sum(baseline_values) / max(1, len(baseline_values))
    latest_1m = _finite_float(latest[7]) if latest else 0.0
    previous_1m = _finite_float(previous[7]) if previous else 0.0
    delta_1m = _finite_float(latest_1m - previous_1m)
    latest_1h = _finite_sum([_finite_float(row[7]) for row in closed_rows[-60:]])
    raw_multiple = latest_1m / baseline if baseline > 0 else (999999.0 if latest_1m > 0 else 0.0)
    multiple = _finite_float(raw_multiple, 999999.0 if latest_1m > 0 else 0.0)
    ticker = client.fetch_ticker(token.pair)
    return {
        "symbol": token.symbol,
        "name": token.name,
        "alphaId": token.alpha_id,
        "pair": token.pair,
        "chain": token.chain_name,
        "lastPrice": _finite_float(ticker.get("lastPrice"), _finite_float(token.price)),
        "latest1mQuoteVolume": latest_1m,
        "previous1mQuoteVolume": previous_1m,
        "delta1mQuoteVolume": delta_1m,
        "latest1hQuoteVolume": latest_1h,
        "baselineQuoteVolume": baseline,
        "multiple": multiple,
        "trades": _safe_int(latest[8]) if latest else 0,
        "closedUtc": _ms_to_iso(_safe_int(latest[6]) if latest else 0),
        "tickerQuoteVolume24h": _finite_float(ticker.get("quoteVolume"), _finite_float(token.volume_24h)),
        "priceChangePercent24h": _finite_float(ticker.get("priceChangePercent")),
    }


def collect_snapshot(symbols: list[str], market: Any | None = None) -> dict[str, Any]:
    client = market or AlphaMarketClient()
    tokens = client.fetch_tokens()
    rows: list[dict[str, Any]] = []
    errors: list[str] = []
    for symbol in symbols:
        token = tokens.get(symbol.upper())
        if token is None:
            errors.append(f"{symbol}: not found in Binance Alpha token list")
            continue
        try:
            rows.append(_snapshot_row(token, client))
        except Exception:
            errors.append(f"{symbol}: market data unavailable")
    rows.sort(key=lambda row: row["multiple"], reverse=True)
    hourly_alert = check_hourly_volume_breakouts(rows)
    return {
        "generatedAtUtc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "symbols": symbols,
        "rows": rows,
        "errors": errors,
        "alerts": {
            "hourlyVolume": hourly_alert,
        },
        "config": {
            "autoSymbols": _env_truthy("ALPHA_AUTO_SYMBOLS", "0"),
            "spikeMultiple": _finite_float(os.environ.get("ALPHA_SPIKE_MULTIPLE", "3"), 3.0),
            "minQuoteVolume": _finite_float(os.environ.get("ALPHA_MIN_QUOTE_VOLUME", "1000"), 1000.0),
            "absoluteMinQuoteVolume": _finite_float(
                os.environ.get("ALPHA_ABSOLUTE_MIN_QUOTE_VOLUME", "60000"),
                60000.0,
            ),
            "cooldownMinutes": _safe_int(os.environ.get("ALPHA_COOLDOWN_MINUTES"), 30),
        },
    }


def check_alert_once() -> dict[str, Any]:
    with _ALERT_CHECK_LOCK:
        started = time.time()
        args = [
            "--symbols", ",".join(_symbols_from_env()),
            "--state-file", os.environ.get("ALPHA_STATE_FILE", "/tmp/binance_alpha_volume_alert_state.json"),
        ]
        if os.environ.get("ALPHA_DASHBOARD_DRY_RUN", "0") == "1":
            args.append("--dry-run")
        parsed = alert.parse_args_from(args) if hasattr(alert, "parse_args_from") else None
        if parsed is None:
            raise RuntimeError("alert parser helper is unavailable")
        return_code = alert.run(parsed)
        return {
            "ok": return_code == 0,
            "returnCode": return_code,
            "elapsedMs": int((time.time() - started) * 1000),
            "dryRun": os.environ.get("ALPHA_DASHBOARD_DRY_RUN", "0") == "1",
        }


def competition_service() -> CompetitionMetricsService:
    global _COMPETITION_SERVICE
    with _COMPETITION_SERVICE_LOCK:
        if _COMPETITION_SERVICE is None:
            market = AlphaMarketClient()
            cache_path = Path(os.environ.get("ALPHA_COMPETITION_RULE_CACHE", DEFAULT_RULE_CACHE))
            _COMPETITION_SERVICE = CompetitionMetricsService(
                rule_provider=BinanceCompetitionRuleProvider(),
                rule_cache=CompetitionRuleCache(cache_path),
                volume_provider=CompetitionVolumeProvider(market=market),
            )
        return _COMPETITION_SERVICE


def discovery_service() -> CompetitionDiscoveryService:
    global _DISCOVERY_SERVICE
    with _DISCOVERY_SERVICE_LOCK:
        if _DISCOVERY_SERVICE is None:
            cache_path = Path(
                os.environ.get("ALPHA_COMPETITION_DISCOVERY_CACHE", DEFAULT_DISCOVERY_CACHE)
            )
            _DISCOVERY_SERVICE = CompetitionDiscoveryService(
                provider=BinanceCompetitionRuleProvider(),
                cache=CompetitionDiscoveryCache(cache_path),
            )
        return _DISCOVERY_SERVICE


def history_store() -> CompetitionHistoryStore:
    global _HISTORY_STORE
    with _HISTORY_STORE_LOCK:
        if _HISTORY_STORE is None:
            path = Path(os.environ.get("ALPHA_COMPETITION_HISTORY_CACHE", DEFAULT_HISTORY_CACHE))
            store = CompetitionHistoryStore(path)
            cache_path = Path(os.environ.get("ALPHA_COMPETITION_DISCOVERY_CACHE", DEFAULT_DISCOVERY_CACHE))
            try:
                store.archive_rules(list(CompetitionDiscoveryCache(cache_path).load().rules), now=datetime.now(timezone.utc))
            except (OSError, ValueError):
                pass
            _HISTORY_STORE = store
        return _HISTORY_STORE


def complete_history_reference(store: CompetitionHistoryStore, *, now: datetime, provider: Any = None) -> None:
    if not _HISTORY_REFERENCE_LOCK.acquire(blocking=False):
        return
    try:
        pending = store.pending_reference(now=now)
        if pending is None:
            return
        identity, rule, round_ = pending
        volume_provider = provider or CompetitionVolumeProvider(market=AlphaMarketClient())
        try:
            volume = volume_provider.fetch(rule, round_, now, require_complete=True)
        except Exception:
            store.mark_reference_unavailable(identity, now=now)
        else:
            store.save_reference(identity, weighted_volume=volume.weighted_volume, source=volume.source, now=now)
    finally:
        _HISTORY_REFERENCE_LOCK.release()


def complete_history_reward(store: CompetitionHistoryStore, *, now: datetime) -> None:
    if not _HISTORY_REWARD_LOCK.acquire(blocking=False):
        return
    try:
        pending = store.pending_reward(now=now)
        if pending is None:
            return
        try:
            reward = fetch_ended_reward(pending)
        except Exception:
            store.mark_reward_unavailable(pending["id"], now=now)
        else:
            store.save_reward(pending["id"], reward, now=now)
    finally:
        _HISTORY_REWARD_LOCK.release()


INDEX_HTML = r"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Binance Alpha Monitor</title>
  <style>
    :root {
      color-scheme: light;
      --bg: #f4f6f8;
      --panel: #ffffff;
      --text: #17212b;
      --muted: #667085;
      --line: #d8dee8;
      --line-soft: #edf0f4;
      --accent: #0f766e;
      --accent-soft: #e8f5f2;
      --blue: #175cd3;
      --blue-soft: #eaf2ff;
      --warn: #b45309;
      --warn-soft: #fff4e5;
      --danger: #b42318;
      --danger-soft: #ffebe9;
      --good: #047857;
      --good-soft: #e7f6ec;
    }
    * { box-sizing: border-box; }
    html, body { max-width: 100%; overflow-x: hidden; }
    body {
      margin: 0;
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      background: var(--bg);
      color: var(--text);
      letter-spacing: 0;
    }
    .shell { width: 100%; max-width: 1320px; margin: 0 auto; padding: 24px 20px 40px; }
    header {
      display: flex;
      align-items: flex-end;
      justify-content: space-between;
      gap: 20px;
      margin-bottom: 22px;
    }
    h1 { margin: 0; font-size: 25px; line-height: 1.15; font-weight: 760; }
    h2 { margin: 0; font-size: 18px; line-height: 1.3; font-weight: 760; }
    .sub, .section-copy { margin: 6px 0 0; color: var(--muted); font-size: 13px; line-height: 1.55; }
    .actions { display: flex; align-items: center; gap: 8px; flex-wrap: wrap; justify-content: flex-end; }
    button {
      appearance: none;
      border: 1px solid var(--line);
      background: var(--panel);
      color: var(--text);
      border-radius: 8px;
      min-height: 36px;
      padding: 0 14px;
      font-weight: 680;
      cursor: pointer;
    }
    button.primary { background: var(--accent); border-color: var(--accent); color: white; }
    button:disabled { opacity: .55; cursor: wait; }
    button:focus-visible, a:focus-visible { outline: 3px solid rgba(23, 92, 211, .28); outline-offset: 2px; }
    .section-block + .section-block { margin-top: 26px; }
    .section-heading {
      display: flex;
      align-items: flex-end;
      justify-content: space-between;
      gap: 18px;
      margin-bottom: 12px;
    }
    .source-time { flex: 0 0 auto; color: var(--muted); font-size: 12px; line-height: 1.5; text-align: right; }
    .kpis {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 10px;
      margin: 12px 0;
    }
    .kpi {
      min-width: 0;
      min-height: 82px;
      padding: 12px;
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 9px;
    }
    .kpi .label { color: var(--muted); font-size: 12px; }
    .kpi .value { margin-top: 8px; font-size: 21px; font-weight: 760; overflow-wrap: anywhere; }
    .table-wrap {
      width: 100%;
      max-width: 100%;
      overflow: auto;
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 10px;
      -webkit-overflow-scrolling: touch;
    }
    table { width: 100%; border-collapse: collapse; }
    th, td {
      padding: 12px 11px;
      border-bottom: 1px solid var(--line-soft);
      text-align: right;
      font-size: 13px;
      white-space: nowrap;
      font-variant-numeric: tabular-nums;
      vertical-align: middle;
    }
    th {
      position: sticky;
      top: 0;
      z-index: 1;
      color: var(--muted);
      background: #fafbfc;
      font-size: 12px;
      font-weight: 720;
      letter-spacing: .01em;
    }
    td:first-child, th:first-child { text-align: left; }
    tr:last-child td { border-bottom: 0; }
    .market-table { min-width: 1050px; }
    .competition-table { min-width: 1280px; }
    .competition-table th:nth-child(1) { min-width: 130px; }
    .competition-table th:nth-child(2) { min-width: 180px; }
    .competition-table th:last-child { min-width: 220px; }
    .sym { font-weight: 780; letter-spacing: .01em; }
    .name { margin-top: 3px; color: var(--muted); font-size: 12px; font-weight: 450; white-space: normal; }
    .metric-primary { font-weight: 720; }
    .metric-secondary { margin-top: 4px; color: var(--muted); font-size: 11px; line-height: 1.45; white-space: normal; }
    .pill, .status-chip, .source-badge {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      border-radius: 999px;
      padding: 3px 8px;
      font-size: 12px;
      font-weight: 760;
      line-height: 1.35;
    }
    .pill { min-width: 58px; background: var(--accent-soft); color: var(--accent); }
    .hot { background: var(--warn-soft); color: var(--warn); }
    .very-hot { background: var(--danger-soft); color: var(--danger); }
    .status-chip { background: #f0f2f5; color: #475467; }
    .status-chip.active { background: var(--good-soft); color: var(--good); }
    .status-chip.unavailable { background: var(--danger-soft); color: var(--danger); }
    .source-badges { display: flex; justify-content: flex-end; align-items: center; gap: 5px; flex-wrap: wrap; }
    .source-badge { background: var(--blue-soft); color: var(--blue); }
    .source-badge.official { background: var(--good-soft); color: var(--good); }
    .source-badge.estimate { background: var(--blue-soft); color: var(--blue); }
    .source-badge.stale { background: var(--warn-soft); color: var(--warn); }
    .source-details { margin-top: 6px; color: var(--muted); font-size: 11px; line-height: 1.55; white-space: normal; overflow-wrap: anywhere; }
    .source-details a { color: var(--blue); text-decoration-thickness: 1px; text-underline-offset: 2px; }
    .threshold-watch { color: var(--blue); font-weight: 680; }
    .threshold-reference { color: var(--warn); font-weight: 760; }
    .threshold-safe { color: var(--good); font-weight: 760; }
    .positive { color: var(--good); }
    .negative { color: var(--danger); }
    .status { min-height: 18px; margin-top: 9px; color: var(--muted); font-size: 12px; line-height: 1.5; overflow-wrap: anywhere; }
    .errors {
      min-height: 0;
      margin-top: 8px;
      color: var(--danger);
      font-size: 12px;
      line-height: 1.55;
      white-space: pre-wrap;
      overflow-wrap: anywhere;
    }
    .discovery-status {
      margin: 0 0 8px;
      padding: 7px 10px;
      color: var(--warn);
      background: var(--warn-soft);
      border-radius: 8px;
      font-size: 12px;
      line-height: 1.5;
      overflow-wrap: anywhere;
    }
    .history-controls { display: flex; align-items: center; gap: 8px; flex-wrap: wrap; }
    .history-controls select, .history-form input, .history-form textarea {
      border: 1px solid var(--line); border-radius: 6px; background: #fff; color: var(--text);
      font: inherit; font-size: 13px; min-height: 36px; padding: 7px 9px;
    }
    .history-summary { display: flex; gap: 22px; flex-wrap: wrap; margin: 10px 0 14px; font-size: 13px; }
    .history-summary strong { margin-left: 6px; font-variant-numeric: tabular-nums; }
    .history-table { min-width: 1100px; }
    .history-table th:first-child { min-width: 140px; }
    .history-table td:last-child, .history-table th:last-child { text-align: center; }
    .history-table button { min-height: 30px; padding: 0 9px; font-size: 12px; }
    .history-empty { padding: 18px 11px; color: var(--muted); font-size: 13px; }
    .history-dialog { width: min(440px, calc(100vw - 24px)); max-height: min(86vh, 750px);
      overflow: auto; border: 1px solid var(--line); border-radius: 8px; padding: 20px; color: var(--text); }
    .history-dialog::backdrop { background: rgba(16, 24, 40, .45); }
    .history-dialog h2 { margin-bottom: 6px; }
    .history-form { display: grid; gap: 12px; margin-top: 16px; }
    .history-form label { display: grid; gap: 5px; color: var(--muted); font-size: 12px; font-weight: 650; }
    .history-form input, .history-form textarea { width: 100%; color: var(--text); font-weight: 450; }
    .history-form textarea { min-height: 64px; resize: vertical; }
    .history-form [hidden] { display: none; }
    .history-form fieldset { border: 0; padding: 0; margin: 0; display: grid; gap: 12px; min-width: 0; }
    .history-form-actions { display: flex; gap: 8px; justify-content: flex-end; margin-top: 3px; }
    .history-form-actions .delete { margin-right: auto; color: var(--danger); }
    @media (max-width: 760px) {
      .shell { padding: 17px 11px 28px; }
      header { align-items: stretch; flex-direction: column; gap: 14px; margin-bottom: 20px; }
      h1 { font-size: 22px; }
      .actions { justify-content: stretch; }
      .actions button { flex: 1 1 0; min-width: 0; }
      .section-block + .section-block { margin-top: 24px; }
      .section-heading { align-items: flex-start; flex-direction: column; gap: 7px; }
      .source-time { max-width: 100%; text-align: left; overflow-wrap: anywhere; }
      .kpis { grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 8px; }
      .kpi { min-height: 76px; padding: 10px; }
      .kpi .value { font-size: 18px; }
      .competition-wrap { overflow: visible; background: transparent; border: 0; }
      .competition-table { min-width: 0; display: block; }
      .competition-table thead { display: none; }
      .competition-table tbody { display: grid; width: 100%; gap: 10px; }
      .competition-table tbody tr {
        display: block;
        width: 100%;
        padding: 5px 12px;
        overflow: hidden;
        background: var(--panel);
        border: 1px solid var(--line);
        border-radius: 10px;
      }
      .competition-table tbody td {
        display: grid;
        grid-template-columns: 98px minmax(0, 1fr);
        gap: 10px;
        align-items: start;
        width: 100%;
        padding: 9px 0;
        text-align: right;
        white-space: normal;
        overflow-wrap: anywhere;
      }
      .competition-table tbody td:last-child { border-bottom: 0; }
      .competition-table tbody td::before {
        content: attr(data-label);
        color: var(--muted);
        text-align: left;
        font-size: 12px;
        font-weight: 720;
        line-height: 1.45;
      }
      .competition-table .cell-value { min-width: 0; text-align: right; }
      .competition-table .source-badges { justify-content: flex-end; }
      .competition-table .name { text-align: right; }
      .history-controls { width: 100%; }
      .history-controls select { flex: 1 1 120px; }
      .history-controls button { flex: 1 1 120px; }
      .history-summary { gap: 8px 18px; }
      .history-table { min-width: 0; display: block; }
      .history-table thead { display: none; }
      .history-table tbody { display: grid; gap: 8px; }
      .history-table tbody tr { display: block; background: var(--panel); border: 1px solid var(--line); border-radius: 8px; padding: 6px 11px; }
      .history-table tbody td { display: grid; grid-template-columns: 98px minmax(0, 1fr); gap: 8px;
        width: 100%; padding: 7px 0; white-space: normal; text-align: right; overflow-wrap: anywhere; }
      .history-table tbody td::before { content: attr(data-label); text-align: left; color: var(--muted); font-size: 12px; font-weight: 700; }
      .history-table tbody td:last-child { text-align: right; }
      .history-table tbody td:last-child button { justify-self: end; }
    }
    @media (prefers-reduced-motion: reduce) {
      *, *::before, *::after {
        scroll-behavior: auto !important;
        animation-duration: .01ms !important;
        animation-iteration-count: 1 !important;
        transition-duration: .01ms !important;
      }
    }
  </style>
</head>
<body>
  <div class="shell">
    <header>
      <div>
        <h1>Binance Alpha Monitor</h1>
        <p class="sub">交易赛门槛与实时行情分区展示，每 30 秒自动刷新。</p>
      </div>
      <div class="actions">
        <button id="refreshBtn" class="primary">Refresh</button>
        <button id="checkBtn">Check Alert</button>
      </div>
    </header>

    <section id="competitionSection" class="section-block" aria-labelledby="competitionTitle">
      <div class="section-heading">
        <div>
          <h2 id="competitionTitle">Alpha 交易赛门槛</h2>
          <p class="section-copy">按当前轮累计加权交易量估算；观察线 0.4、参考线 0.6、安全线 1.0，不含新锐交易者个人 1.2x 加成。</p>
        </div>
        <div id="competitionGenerated" class="source-time" role="status" aria-live="polite">等待交易赛数据…</div>
      </div>
      <div id="competitionDiscoveryStatus" class="discovery-status" role="alert" aria-live="polite" hidden></div>
      <div class="table-wrap competition-wrap">
        <table class="competition-table">
          <thead>
            <tr>
              <th>币种</th>
              <th>轮次 / Day / 倒计时</th>
              <th>当前倍速</th>
              <th>加权总量</th>
              <th>获奖人数</th>
              <th>实际榜单门槛</th>
              <th>平均量</th>
              <th>观察线 0.4</th>
              <th>参考线 0.6</th>
              <th>安全线 1.0</th>
              <th>来源 / 更新时间 / 公告</th>
            </tr>
          </thead>
          <tbody id="competitionRows"></tbody>
        </table>
      </div>
      <div id="competitionErrors" class="errors" role="alert" aria-live="polite"></div>
    </section>

    <section id="historySection" class="section-block" aria-labelledby="historyTitle">
      <div class="section-heading">
        <div>
          <h2 id="historyTitle">已结束竞赛参照</h2>
          <p class="section-copy">参考线 = 该轮加权总量 ÷ 获奖人数 × 0.6；奖励价值 = 每人奖励代币数 × 结束时价格。</p>
        </div>
        <div class="history-controls">
          <select id="historyRound" aria-label="筛选轮次"><option value="">全部轮次</option><option value="1">第 1 轮</option><option value="2">第 2 轮</option></select>
          <select id="historyWinners" aria-label="筛选获奖人数"><option value="">全部获奖人数</option></select>
          <button id="addHistoryBtn" type="button">录入旧赛</button>
        </div>
      </div>
      <div id="historySummary" class="history-summary" aria-live="polite"></div>
      <div class="table-wrap competition-wrap">
        <table class="history-table">
          <thead><tr><th>币种 / 轮次</th><th>结束时间</th><th>获奖人数</th><th>最终进榜线</th><th>最终参考线</th><th>真实 / 参考</th><th>结束时奖励价值</th><th>进榜线 / 奖励</th><th>公告</th><th>操作</th></tr></thead>
          <tbody id="historyRows"></tbody>
        </table>
        <div id="historyEmpty" class="history-empty" hidden>暂无已结束竞赛。可录入旧赛的最终分数线。</div>
      </div>
      <div id="historyStatus" class="errors" role="status" aria-live="polite"></div>
    </section>

    <section id="marketSection" class="section-block" aria-labelledby="marketTitle">
      <div class="section-heading">
        <div>
          <h2 id="marketTitle">实时行情监控</h2>
          <p class="section-copy">保留原有 1 分钟放量、1 小时成交量与告警检查。</p>
        </div>
        <div id="marketGenerated" class="source-time" role="status" aria-live="polite">等待行情数据…</div>
      </div>
      <div class="kpis">
        <div class="kpi"><div class="label">Symbols</div><div class="value" id="kpiSymbols">-</div></div>
        <div class="kpi"><div class="label">Max Multiple</div><div class="value" id="kpiMultiple">-</div></div>
        <div class="kpi"><div class="label">Top 1h Volume</div><div class="value" id="kpiVolume">-</div></div>
        <div class="kpi"><div class="label">Spike Rule</div><div class="value" id="kpiRule">-</div></div>
      </div>
      <div class="table-wrap market-wrap">
        <table class="market-table">
          <thead>
            <tr>
              <th>Symbol</th>
              <th>Price</th>
              <th>1m Vol</th>
              <th>1m Δ</th>
              <th>1h Vol</th>
              <th>20m Avg</th>
              <th>Multiple</th>
              <th>Trades</th>
              <th>24h Vol</th>
              <th>24h %</th>
              <th>Closed UTC</th>
            </tr>
          </thead>
          <tbody id="rows"></tbody>
        </table>
      </div>
      <div class="status" id="status" role="status" aria-live="polite"></div>
      <div class="status" id="alertStatus" role="status" aria-live="polite"></div>
      <div class="errors" id="errors" role="alert" aria-live="polite"></div>
    </section>
  </div>

  <dialog id="historyDialog" class="history-dialog" aria-labelledby="historyDialogTitle">
    <h2 id="historyDialogTitle">录入最终进榜线</h2>
    <div id="historyDialogContext" class="sub"></div>
    <form id="historyForm" class="history-form">
      <fieldset id="manualHistoryFields">
        <label>币种<input name="symbol" maxlength="32" placeholder="例如 AEON" required></label>
        <label>项目名称<input name="name" maxlength="100" placeholder="可选"></label>
        <label>轮次<input name="round" type="number" min="1" max="20" step="1" value="1" required></label>
        <label>结束时间（本地时间）<input name="endUtc" type="datetime-local" required></label>
        <label>获奖人数<input name="winnerCount" type="number" min="1" step="1" required></label>
        <label>官方公告链接<input name="articleUrl" type="url" placeholder="可选，Binance 公告"></label>
      </fieldset>
      <label>最终进榜分数线（U）<input name="finalThreshold" type="number" min="0.01" step="any" required></label>
      <label>该轮最终加权交易量（U）<input name="weightedVolume" type="number" min="0.01" step="any" placeholder="可选"></label>
      <label>备注<textarea name="note" maxlength="500" placeholder="可选"></textarea></label>
      <div id="historyFormStatus" class="errors" role="alert"></div>
      <div class="history-form-actions">
        <button id="deleteHistoryBtn" class="delete" type="button" hidden>删除记录</button>
        <button id="cancelHistoryBtn" type="button">取消</button>
        <button class="primary" type="submit">保存</button>
      </div>
    </form>
  </dialog>

  <script>
    const fmt = new Intl.NumberFormat('en-US', { maximumFractionDigits: 2 });
    const integerFmt = new Intl.NumberFormat('en-US', { maximumFractionDigits: 0 });
    const priceFmt = new Intl.NumberFormat('en-US', { maximumFractionDigits: 10 });
    const statusLabels = {
      'upcoming': '尚未开始',
      'active': '进行中',
      'between_rounds': '轮次间隔',
      'ended': '交易赛已结束',
      'rule_unavailable': '规则暂不可用',
      'volume_unavailable': '交易量暂不可用',
    };
    const rowsEl = document.getElementById('rows');
    const competitionRowsEl = document.getElementById('competitionRows');
    const historyRowsEl = document.getElementById('historyRows');
    const historyDialog = document.getElementById('historyDialog');
    const historyForm = document.getElementById('historyForm');
    let historyRows = [];
    let editingHistoryId = null;
    const statusEl = document.getElementById('status');
    const alertStatusEl = document.getElementById('alertStatus');
    const errorsEl = document.getElementById('errors');
    const competitionErrorsEl = document.getElementById('competitionErrors');
    const competitionDiscoveryStatusEl = document.getElementById('competitionDiscoveryStatus');
    const refreshBtn = document.getElementById('refreshBtn');
    const checkBtn = document.getElementById('checkBtn');
    let refreshGeneration = 0;

    function escapeHtml(value) {
      return String(value ?? '').replace(/[&<>"']/g, character => ({
        '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
      })[character]);
    }

    function safeArticleUrl(value) {
      if (typeof value !== 'string' || !/^https?:\/\//i.test(value.trim())) return null;
      try {
        const url = new URL(value.trim());
        return (url.protocol === 'http:' || url.protocol === 'https:') ? url.href : null;
      } catch (_) {
        return null;
      }
    }

    function finiteNumber(value) {
      return typeof value === 'number' && Number.isFinite(value) ? value : null;
    }

    function money(value) {
      const number = finiteNumber(value);
      return number === null ? '—' : fmt.format(number);
    }

    function formatU(value) {
      if (typeof value !== 'number' || !Number.isFinite(value)) return '—';
      return `${fmt.format(value)} U`;
    }

    function formatInteger(value) {
      const number = finiteNumber(value);
      return number === null ? '—' : integerFmt.format(number);
    }

    function formatUtc(value) {
      if (typeof value !== 'string' || !value) return '时间未知';
      const timestamp = Date.parse(value);
      if (!Number.isFinite(timestamp)) return '时间未知';
      return `${new Date(timestamp).toISOString().replace('T', ' ').replace('.000Z', ' UTC')}`;
    }

    function formatCountdown(endUtc, nowMs = Date.now()) {
      if (typeof endUtc !== 'string' || !endUtc) return '结束时间未知';
      const endMs = Date.parse(endUtc);
      if (!Number.isFinite(endMs)) return '结束时间未知';
      const remainingMs = endMs - nowMs;
      if (remainingMs <= 0) return '已结束';
      const totalMinutes = Math.ceil(remainingMs / 60000);
      const days = Math.floor(totalMinutes / 1440);
      const hours = Math.floor((totalMinutes % 1440) / 60);
      const minutes = totalMinutes % 60;
      const parts = [];
      if (days) parts.push(`${days}天`);
      if (hours || days) parts.push(`${hours}小时`);
      parts.push(`${minutes}分钟`);
      return `剩余 ${parts.join(' ')}`;
    }

    function formatStartCountdown(startUtc, nowMs = Date.now()) {
      if (typeof startUtc !== 'string' || !startUtc) return '开始时间未知';
      const startMs = Date.parse(startUtc);
      if (!Number.isFinite(startMs)) return '开始时间未知';
      const remainingMs = startMs - nowMs;
      if (remainingMs <= 0) return '即将开始';
      const totalMinutes = Math.ceil(remainingMs / 60000);
      const days = Math.floor(totalMinutes / 1440);
      const hours = Math.floor((totalMinutes % 1440) / 60);
      const minutes = totalMinutes % 60;
      const parts = [];
      if (days) parts.push(`${days}天`);
      if (hours || days) parts.push(`${hours}小时`);
      parts.push(`${minutes}分钟`);
      return `距离开始 ${parts.join(' ')}`;
    }

    function multipleClass(value) {
      if (value >= 5) return 'pill very-hot';
      if (value >= 3) return 'pill hot';
      return 'pill';
    }

    function sourceContent(row) {
      const badges = [];
      if (row.volumeSource === 'official') {
        badges.push('<span class="source-badge official">官方</span>');
      } else if (row.volumeSource === 'alpha_kline_estimate') {
        badges.push('<span class="source-badge estimate">Alpha K线估算</span>');
      }
      if (row.stale) badges.push('<span class="source-badge stale">数据已过期</span>');
      const articleUrl = safeArticleUrl(row.articleUrl);
      const details = [];
      if (row.volumeUpdatedAtUtc) details.push(`数据 ${escapeHtml(formatUtc(row.volumeUpdatedAtUtc))}`);
      if (articleUrl) details.push(`<a href="${escapeHtml(articleUrl)}" target="_blank" rel="noopener noreferrer">官方公告</a>`);
      if (row.error) details.push(escapeHtml(row.error));
      if (!badges.length) badges.push(`<span class="status-chip">${escapeHtml(statusLabels[row.status] || '来源暂不可用')}</span>`);
      return `<div class="cell-value source-cell"><div class="source-badges">${badges.join('')}</div>${details.length ? `<div class="source-details">${details.join(' · ')}</div>` : ''}</div>`;
    }

    function roundContent(row) {
      const state = statusLabels[row.status] || '状态未知';
      if (row.status === 'upcoming' || row.status === 'between_rounds') {
        const pendingLabel = row.status === 'upcoming' ? '未开始' : '轮间等待';
        return `<div class="cell-value"><div class="metric-primary">第 ${escapeHtml(row.round)} 轮 · ${escapeHtml(formatUtc(row.roundStartUtc))} → ${escapeHtml(formatUtc(row.roundEndUtc))}</div>
          <div class="metric-secondary"><span class="status-chip">${escapeHtml(pendingLabel)} · ${escapeHtml(formatStartCountdown(row.roundStartUtc))}</span></div></div>`;
      }
      if (row.status !== 'active') {
        const unavailable = row.status === 'rule_unavailable' || row.status === 'volume_unavailable';
        return `<div class="cell-value"><span class="status-chip${unavailable ? ' unavailable' : ''}">${escapeHtml(state)}</span></div>`;
      }
      return `<div class="cell-value"><div class="metric-primary">第 ${escapeHtml(row.round)} 轮 · Day ${escapeHtml(row.day)}</div>
        <div class="metric-secondary"><span class="status-chip active">进行中</span> · ${escapeHtml(formatCountdown(row.roundEndUtc))}</div></div>`;
    }

    function leaderboardThresholdContent(row) {
      const threshold = finiteNumber(row.leaderboardThreshold);
      if (threshold === null) return '<div class="cell-value">—</div>';
      const details = [];
      if (row.leaderboardThresholdUpdatedAtLabel) {
        details.push(escapeHtml(row.leaderboardThresholdUpdatedAtLabel));
      } else if (row.leaderboardThresholdUpdatedAt) {
        details.push(escapeHtml(formatUtc(row.leaderboardThresholdUpdatedAt)));
      }
      if (row.leaderboardThresholdRank) details.push(`第 ${escapeHtml(row.leaderboardThresholdRank)} 名`);
      if (row.leaderboardThresholdSource) details.push(escapeHtml(row.leaderboardThresholdSource));
      if (row.leaderboardThresholdNote) details.push(escapeHtml(row.leaderboardThresholdNote));
      return `<div class="cell-value"><div class="metric-primary">${escapeHtml(formatU(threshold))}</div>${details.length ? `<div class="metric-secondary">${details.join(' · ')}</div>` : ''}</div>`;
    }

    function renderCompetitionRow(row) {
      const pending = row.status === 'upcoming' || row.status === 'between_rounds';
      const multiplier = finiteNumber(row.currentMultiplier);
      return `<tr>
        <td data-label="币种"><div class="cell-value token-cell"><div class="sym">${escapeHtml(row.symbol)}</div><div class="name">${escapeHtml(row.name)}</div></div></td>
        <td data-label="轮次 / Day">${roundContent(row)}</td>
        <td data-label="当前倍速">${pending ? '—' : multiplier === null ? '—' : `<span class="${multipleClass(multiplier)}">${escapeHtml(multiplier.toFixed(1))}x</span>`}</td>
        <td data-label="加权总量" class="metric-primary">${pending ? '—' : escapeHtml(formatU(row.weightedVolume))}</td>
        <td data-label="获奖人数">${escapeHtml(formatInteger(row.winnerCount))}</td>
        <td data-label="实际榜单门槛">${pending ? '—' : leaderboardThresholdContent(row)}</td>
        <td data-label="平均量">${pending ? '—' : escapeHtml(formatU(row.averageVolume))}</td>
        <td data-label="观察线 0.4" class="threshold-watch">${pending ? '—' : escapeHtml(formatU(row.watchThreshold))}</td>
        <td data-label="参考线 0.6" class="threshold-reference">${pending ? '—' : escapeHtml(formatU(row.referenceThreshold))}</td>
        <td data-label="安全线 1.0" class="threshold-safe">${pending ? '—' : escapeHtml(formatU(row.safeThreshold))}</td>
        <td data-label="来源 / 更新时间 / 公告">${sourceContent(row)}</td>
      </tr>`;
    }

    function validatePayload(data, label) {
      if (
        data === null || typeof data !== 'object' || Array.isArray(data) ||
        !Array.isArray(data.rows) ||
        !data.rows.every(row => row !== null && typeof row === 'object' && !Array.isArray(row))
      ) {
        throw new Error(`${label}数据格式无效`);
      }
      return data;
    }

    function competitionSymbols(data) {
      const rows = validatePayload(data, '交易赛').rows;
      const symbols = [];
      const seen = new Set();
      for (const row of rows) {
        const symbol = row.symbol;
        if (typeof symbol !== 'string' || !/^[A-Z0-9_]{1,32}$/.test(symbol)) continue;
        if (seen.has(symbol)) continue;
        seen.add(symbol);
        symbols.push(symbol);
      }
      return symbols;
    }

    function renderCompetition(data) {
      const payload = validatePayload(data, '交易赛');
      const rows = payload.rows;
      const isDiscoveryError = error => String(error).toLowerCase().includes('discovery');
      const discoveryErrors = (Array.isArray(payload.errors) ? payload.errors : []).filter(isDiscoveryError);
      const discoveryMessages = discoveryErrors.map(String);
      if (payload.discoveryStale === true) discoveryMessages.push('公告发现数据已过期，正在保留最近一次成功名单。');
      competitionDiscoveryStatusEl.innerHTML = discoveryMessages.map(message => escapeHtml(message)).join('<br>');
      competitionDiscoveryStatusEl.hidden = discoveryMessages.length === 0;
      document.getElementById('competitionGenerated').textContent = `交易赛更新 ${payload.generatedAtUtc || '时间未知'}`;
      competitionRowsEl.innerHTML = rows.map(renderCompetitionRow).join('');
      competitionErrorsEl.textContent = (Array.isArray(payload.errors) ? payload.errors : []).map(String).join('\n');
    }

    function median(numbers) {
      if (!numbers.length) return null;
      const sorted = [...numbers].sort((a, b) => a - b);
      const middle = Math.floor(sorted.length / 2);
      return sorted.length % 2 ? sorted[middle] : (sorted[middle - 1] + sorted[middle]) / 2;
    }

    function renderHistory() {
      const round = document.getElementById('historyRound').value;
      const winners = document.getElementById('historyWinners').value;
      const filtered = historyRows.filter(row =>
        (!round || String(row.round) === round) && (!winners || String(row.winnerCount) === winners));
      const completed = filtered.filter(row => finiteNumber(row.finalThreshold) !== null);
      const cutoffs = completed.map(row => row.finalThreshold);
      const ratios = completed.map(row => {
        const reference = finiteNumber(row.referenceThreshold);
        return reference > 0 ? row.finalThreshold / reference : null;
      }).filter(value => value !== null);
      const rewardRatios = completed.filter(row => finiteNumber(row.rewardValueU) > 0)
        .map(row => row.finalThreshold / row.rewardValueU);
      document.getElementById('historySummary').innerHTML = [
        `已录入 <strong>${completed.length} / ${filtered.length}</strong>`,
        `中位进榜线 <strong>${escapeHtml(formatU(median(cutoffs)))}</strong>`,
        `范围 <strong>${cutoffs.length ? `${escapeHtml(formatU(Math.min(...cutoffs)))}–${escapeHtml(formatU(Math.max(...cutoffs)))}` : '—'}</strong>`,
        `真实 / 参考中位 <strong>${ratios.length ? `${median(ratios).toFixed(2)}x` : '—'}</strong>`,
        `进榜线 / 奖励中位 <strong>${rewardRatios.length ? `${fmt.format(median(rewardRatios))}x` : '—'}</strong>`,
      ].map(value => `<span>${value}</span>`).join('');
      historyRowsEl.innerHTML = filtered.map(row => {
        const reference = finiteNumber(row.referenceThreshold);
        const final = finiteNumber(row.finalThreshold);
        const reward = finiteNumber(row.rewardValueU);
        const referenceSource = {alpha_kline_estimate: 'Alpha K线估算', official: '官方总量', manual_volume: '手工加权总量'}[row.referenceSource];
        const articleUrl = safeArticleUrl(row.articleUrl);
        return `<tr>
          <td data-label="币种 / 轮次"><div><div><strong>${escapeHtml(row.symbol)}</strong> · 第 ${escapeHtml(row.round)} 轮</div><div class="name">${escapeHtml(row.name || '')}</div></div></td>
          <td data-label="结束时间">${escapeHtml(formatUtc(row.endUtc))}</td>
          <td data-label="获奖人数">${escapeHtml(formatInteger(row.winnerCount))}</td>
          <td data-label="最终进榜线" class="metric-primary">${escapeHtml(formatU(final))}</td>
          <td data-label="最终参考线"><div><div>${escapeHtml(formatU(reference))}</div><div class="metric-secondary">${referenceSource ? `${escapeHtml(referenceSource)} · 总量 ${escapeHtml(formatU(row.finalWeightedVolume))}` : escapeHtml(row.referenceError || '待补充轮次总量')}</div></div></td>
          <td data-label="真实 / 参考">${final !== null && reference > 0 ? `${(final / reference).toFixed(2)}x` : '—'}</td>
          <td data-label="结束时奖励价值"><div><div>${escapeHtml(formatU(reward))}</div><div class="metric-secondary">${row.rewardSource === 'official_reward_alpha_end_close' ? `${escapeHtml(fmt.format(row.rewardTokensPerWinner))} ${escapeHtml(row.symbol)} × ${escapeHtml(row.rewardEndPriceU)} U` : escapeHtml(row.rewardError || '待获取公告奖励及结束时价格')}</div></div></td>
          <td data-label="进榜线 / 奖励">${finiteNumber(row.thresholdRewardRatio) > 0 ? `${fmt.format(row.thresholdRewardRatio)}x` : '—'}</td>
          <td data-label="公告">${articleUrl ? `<a href="${escapeHtml(articleUrl)}" target="_blank" rel="noopener noreferrer">查看公告</a>` : '—'}</td>
          <td data-label="操作"><button type="button" data-history-id="${escapeHtml(row.id)}">${final === null ? '录入' : '修改'}</button></td>
        </tr>`;
      }).join('');
      document.getElementById('historyEmpty').hidden = filtered.length > 0;
    }

    async function refreshHistory() {
      try {
        const payload = await fetchJson('api/history');
        if (!Array.isArray(payload.rows)) throw new Error('历史数据格式无效');
        historyRows = payload.rows;
        const roundSelect = document.getElementById('historyRound');
        const selectedRound = roundSelect.value;
        const rounds = [...new Set(historyRows.map(row => row.round).filter(Number.isInteger))].sort((a, b) => a - b);
        roundSelect.innerHTML = '<option value="">全部轮次</option>' + rounds.map(value => `<option value="${value}">第 ${value} 轮</option>`).join('');
        roundSelect.value = rounds.some(value => String(value) === selectedRound) ? selectedRound : '';
        const winnersSelect = document.getElementById('historyWinners');
        const selected = winnersSelect.value;
        const counts = [...new Set(historyRows.map(row => row.winnerCount).filter(Number.isInteger))].sort((a, b) => a - b);
        winnersSelect.innerHTML = '<option value="">全部获奖人数</option>' + counts.map(count => `<option value="${count}">${formatInteger(count)} 人</option>`).join('');
        winnersSelect.value = counts.some(count => String(count) === selected) ? selected : '';
        renderHistory();
        document.getElementById('historyStatus').textContent = '';
      } catch (error) {
        document.getElementById('historyStatus').textContent = `历史读取失败：${error}`;
      }
    }

    function openHistoryDialog(row = null) {
      editingHistoryId = row?.id || null;
      historyForm.reset();
      document.getElementById('manualHistoryFields').hidden = Boolean(row);
      document.getElementById('manualHistoryFields').disabled = Boolean(row);
      document.getElementById('deleteHistoryBtn').hidden = !row || row.finalThreshold === null;
      document.getElementById('historyDialogContext').textContent = row
        ? `${row.symbol} · 第 ${row.round} 轮 · ${formatUtc(row.endUtc)}` : '补录已结束交易赛';
      historyForm.elements.namedItem('finalThreshold').value = row?.finalThreshold ?? '';
      historyForm.elements.namedItem('weightedVolume').value = row?.referenceSource === 'manual_volume' ? row.finalWeightedVolume : '';
      historyForm.elements.namedItem('weightedVolume').placeholder = row?.finalWeightedVolume != null
        ? `已记录 ${formatU(row.finalWeightedVolume)}` : '可选';
      historyForm.elements.namedItem('note').value = row?.note || '';
      document.getElementById('historyFormStatus').textContent = '';
      historyDialog.showModal();
    }

    async function submitHistory(action = 'save') {
      const field = name => historyForm.elements.namedItem(name).value;
      const data = action === 'delete' ? { action, id: editingHistoryId } : {
        finalThreshold: Number(field('finalThreshold')),
        note: field('note').trim(),
      };
      if (action !== 'delete') {
        if (field('weightedVolume')) data.weightedVolume = Number(field('weightedVolume'));
        if (editingHistoryId) data.id = editingHistoryId;
        else Object.assign(data, {
          symbol: field('symbol').trim().toUpperCase(),
          name: field('name').trim(),
          round: Number(field('round')),
          endUtc: new Date(field('endUtc')).toISOString(),
          winnerCount: Number(field('winnerCount')),
          articleUrl: field('articleUrl').trim(),
        });
      }
      const response = await fetch('api/history', {
        method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(data),
      });
      const payload = await response.json();
      if (!response.ok) throw new Error(payload.error || `HTTP ${response.status}`);
      historyDialog.close();
      await refreshHistory();
    }

    document.getElementById('addHistoryBtn').addEventListener('click', () => openHistoryDialog());
    document.getElementById('cancelHistoryBtn').addEventListener('click', () => historyDialog.close());
    document.getElementById('historyRound').addEventListener('change', renderHistory);
    document.getElementById('historyWinners').addEventListener('change', renderHistory);
    historyRowsEl.addEventListener('click', event => {
      const button = event.target.closest('button[data-history-id]');
      if (button) openHistoryDialog(historyRows.find(row => row.id === button.dataset.historyId));
    });
    historyForm.addEventListener('submit', async event => {
      event.preventDefault();
      try { await submitHistory(); }
      catch (error) { document.getElementById('historyFormStatus').textContent = String(error); }
    });
    document.getElementById('deleteHistoryBtn').addEventListener('click', async () => {
      if (!confirm('删除这条最终分数线？')) return;
      try { await submitHistory('delete'); }
      catch (error) { document.getElementById('historyFormStatus').textContent = String(error); }
    });

    function renderMarket(data) {
      const payload = validatePayload(data, '行情');
      const rows = payload.rows;
      document.getElementById('marketGenerated').textContent = `行情更新 ${payload.generatedAtUtc || '时间未知'}`;
      document.getElementById('kpiSymbols').textContent = rows.length;
      const maxMult = rows.length ? finiteNumber(rows[0].multiple) : null;
      document.getElementById('kpiMultiple').textContent = maxMult === null ? '—' : `${maxMult.toFixed(2)}x`;
      const topVol = rows.reduce((maximum, row) => Math.max(maximum, finiteNumber(row.latest1hQuoteVolume) || 0), 0);
      document.getElementById('kpiVolume').textContent = formatU(topVol);
      const absoluteMinimum = payload.config && typeof payload.config === 'object'
        ? payload.config.absoluteMinQuoteVolume : null;
      document.getElementById('kpiRule').textContent = `1m >= ${formatU(absoluteMinimum)}`;
      rowsEl.innerHTML = rows.map(row => {
        const delta = finiteNumber(row.delta1mQuoteVolume);
        const percent = finiteNumber(row.priceChangePercent24h);
        const multiple = finiteNumber(row.multiple);
        const pctClass = (percent || 0) >= 0 ? 'positive' : 'negative';
        return `<tr>
          <td><div class="sym">${escapeHtml(row.symbol)}</div><div class="name">${escapeHtml(row.name)} · ${escapeHtml(row.alphaId)}</div></td>
          <td>${escapeHtml(finiteNumber(row.lastPrice) === null ? '—' : priceFmt.format(row.lastPrice))}</td>
          <td>${escapeHtml(money(row.latest1mQuoteVolume))}</td>
          <td class="${(delta || 0) >= 0 ? 'positive' : 'negative'}">${delta !== null && delta >= 0 ? '+' : ''}${escapeHtml(money(delta))}</td>
          <td>${escapeHtml(money(row.latest1hQuoteVolume))}</td>
          <td>${escapeHtml(money(row.baselineQuoteVolume))}</td>
          <td>${multiple === null ? '—' : `<span class="${multipleClass(multiple)}">${escapeHtml(multiple.toFixed(2))}x</span>`}</td>
          <td>${escapeHtml(formatInteger(row.trades))}</td>
          <td>${escapeHtml(money(row.tickerQuoteVolume24h))}</td>
          <td class="${pctClass}">${percent === null ? '—' : `${escapeHtml(percent.toFixed(2))}%`}</td>
          <td>${escapeHtml(row.closedUtc)}</td>
        </tr>`;
      }).join('');
      errorsEl.textContent = (Array.isArray(payload.errors) ? payload.errors : []).map(String).join('\n');
    }

    async function fetchJson(path, options = {}) {
      const separator = path.includes('?') ? '&' : '?';
      const response = await fetch(`${path}${separator}t=${Date.now()}`, { cache: 'no-store', ...options });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      return response.json();
    }

    async function refresh() {
      const generation = ++refreshGeneration;
      refreshBtn.disabled = true;
      try {
        let competitionResult;
        try {
          competitionResult = { status: 'fulfilled', value: await fetchJson('api/competition') };
        } catch (reason) {
          competitionResult = { status: 'rejected', reason };
        }
        if (generation !== refreshGeneration) return;
        let marketResult = null;
        if (competitionResult.status === 'fulfilled') {
          let symbols;
          try {
            symbols = competitionSymbols(competitionResult.value);
          } catch (reason) {
            competitionResult = { status: 'rejected', reason };
            symbols = [];
          }
          if (symbols.length) {
            try {
              const query = encodeURIComponent(symbols.join(','));
              marketResult = { status: 'fulfilled', value: await fetchJson(`api/snapshot?symbols=${query}`) };
            } catch (reason) {
              marketResult = { status: 'rejected', reason };
            }
          } else {
            marketResult = { status: 'fulfilled', value: { rows: [], errors: [], symbols: [] } };
          }
        }
        if (generation !== refreshGeneration) return;
        if (marketResult === null) {
          statusEl.textContent = '行情未刷新：交易赛名单刷新失败。';
        } else if (marketResult.status === 'fulfilled') {
          try {
            renderMarket(marketResult.value);
            statusEl.textContent = '实时行情已更新。';
          } catch (error) {
            statusEl.textContent = `行情刷新失败：${error}`;
          }
        } else {
          statusEl.textContent = `行情刷新失败：${marketResult.reason}`;
        }
        if (competitionResult.status === 'fulfilled') {
          try {
            renderCompetition(competitionResult.value);
          } catch (error) {
            competitionErrorsEl.textContent = `交易赛刷新失败：${error}`;
          }
        } else {
          competitionErrorsEl.textContent = `交易赛刷新失败：${competitionResult.reason}`;
        }
        await refreshHistory();
      } finally {
        if (generation === refreshGeneration) refreshBtn.disabled = false;
      }
    }

    async function checkAlert() {
      checkBtn.disabled = true;
      alertStatusEl.textContent = '正在检查告警规则…';
      try {
        const data = await fetchJson('api/check', { method: 'POST' });
        alertStatusEl.textContent = `告警检查完成，耗时 ${formatInteger(data.elapsedMs)} ms${data.dryRun ? '（dry-run）' : ''}。`;
        await refresh();
      } catch (err) {
        alertStatusEl.textContent = `告警检查失败：${err}`;
      } finally {
        checkBtn.disabled = false;
      }
    }

    refreshBtn.addEventListener('click', refresh);
    checkBtn.addEventListener('click', checkAlert);
    refresh();
    setInterval(refresh, 30000);
  </script>
</body>
</html>
"""


class Handler(BaseHTTPRequestHandler):
    def _history(self) -> CompetitionHistoryStore:
        return getattr(self.server, "history_store", None) or history_store()

    def _discovery(self, current: datetime) -> Any:
        service = getattr(self.server, "discovery_service", None) or discovery_service()
        return service.discover(now=current)

    @staticmethod
    def _add_discovery_metadata(payload: dict[str, Any], snapshot: Any) -> dict[str, Any]:
        discovered_at = snapshot.discovered_at_utc
        payload["discoveredAtUtc"] = (
            None if discovered_at is None else discovered_at.isoformat(timespec="seconds")
        )
        payload["discoveryStale"] = snapshot.stale
        payload["errors"] = [*snapshot.errors, *payload.get("errors", [])]
        return payload

    def _auth_credentials(self) -> tuple[str, str] | None:
        try:
            return _configured_auth_credentials()
        except AuthConfigurationError:
            return None

    def _is_authorized(self) -> bool:
        credentials = self._auth_credentials()
        if credentials is None:
            return False
        header = self.headers.get("Authorization", "")
        scheme, separator, encoded = header.partition(" ")
        if not separator or scheme.casefold() != "basic" or not encoded or encoded != encoded.strip():
            return False
        try:
            decoded = base64.b64decode(encoded, validate=True)
        except (binascii.Error, UnicodeEncodeError, ValueError):
            return False
        username, password = credentials
        expected = f"{username}:{password}".encode("utf-8")
        return hmac.compare_digest(decoded, expected)

    def _require_auth(self) -> bool:
        if self._is_authorized():
            return False
        raw = b"Authentication required"
        self.send_response(HTTPStatus.UNAUTHORIZED)
        self.send_header("WWW-Authenticate", 'Basic realm="Binance Alpha Monitor"')
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Cache-Control", "no-store, max-age=0")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)
        return True

    def _send_json(
        self,
        payload: dict[str, Any],
        status: HTTPStatus = HTTPStatus.OK,
        *,
        headers: dict[str, str] | None = None,
    ) -> None:
        raw = json.dumps(payload, ensure_ascii=False, allow_nan=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store, max-age=0")
        for name, value in (headers or {}).items():
            self.send_header(name, value)
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def _send_internal_error(self, exc: BaseException) -> None:
        self.log_error("API request failed: %r", exc)
        self._send_json(
            {"ok": False, "error": "internal server error"},
            HTTPStatus.INTERNAL_SERVER_ERROR,
        )

    def _send_bad_request(self) -> None:
        self._send_json(
            {"ok": False, "error": "invalid symbols query"},
            HTTPStatus.BAD_REQUEST,
        )

    def _send_method_not_allowed(self, allowed: str) -> None:
        self._send_json(
            {"ok": False, "error": "method not allowed"},
            HTTPStatus.METHOD_NOT_ALLOWED,
            headers={"Allow": allowed},
        )

    def _send_html(self, html: str) -> None:
        raw = html.encode("utf-8")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Cache-Control", "no-store, max-age=0")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self) -> None:
        if self._require_auth():
            return
        parsed = urlparse(self.path)
        if parsed.path == "/":
            self._send_html(INDEX_HTML)
            return
        if parsed.path == "/api/snapshot":
            try:
                if parse_qs(parsed.query, keep_blank_values=True).get("symbols") is not None:
                    symbols = _symbols_from_query(parsed.query)
                else:
                    symbols = None
            except ValueError:
                self._send_bad_request()
                return
            try:
                if symbols is None:
                    current = datetime.now(timezone.utc)
                    snapshot = self._discovery(current)
                    symbols = [rule.symbol for rule in snapshot.rules]
                market = getattr(self.server, "market", None)
                self._send_json(collect_snapshot(symbols, market=market))
            except (Exception, SystemExit) as exc:
                self._send_internal_error(exc)
            return
        if parsed.path == "/api/competition":
            try:
                current = datetime.now(timezone.utc)
                history = self._history()
                snapshot = self._discovery(current)
                service = getattr(self.server, "competition_service", None) or competition_service()
                payload = service.collect_rules(list(snapshot.rules), now=current)
                self._add_discovery_metadata(payload, snapshot)
                payload = _apply_leaderboard_thresholds(payload)
                try:
                    history.archive_rules(list(snapshot.rules), now=current)
                    history.capture_rows(payload["rows"], now=current)
                except (OSError, ValueError) as exc:
                    self.log_error("History capture failed: %r", exc)
                self._send_json(payload)
            except (Exception, SystemExit) as exc:
                self._send_internal_error(exc)
            return
        if parsed.path == "/api/history":
            try:
                current = datetime.now(timezone.utc)
                store = self._history()
                complete_history_reference(store, now=current, provider=getattr(self.server, "history_volume_provider", None))
                complete_history_reward(store, now=current)
                self._send_json(store.snapshot(now=current))
            except (Exception, SystemExit) as exc:
                self._send_internal_error(exc)
            return
        if parsed.path == "/api/check":
            self._send_method_not_allowed("POST")
            return
        self.send_error(HTTPStatus.NOT_FOUND)

    def do_POST(self) -> None:
        if self._require_auth():
            return
        parsed = urlparse(self.path)
        if parsed.path == "/api/history":
            origin = self.headers.get("Origin")
            if origin is not None:
                parsed_origin = urlparse(origin)
                if parsed_origin.scheme not in {"http", "https"} or parsed_origin.netloc.casefold() != self.headers.get("Host", "").casefold():
                    self._send_json({"ok": False, "error": "cross-origin request rejected"}, HTTPStatus.FORBIDDEN)
                    return
            if self.headers.get("Content-Type", "").split(";", 1)[0].strip().casefold() != "application/json":
                self._send_json({"ok": False, "error": "JSON content required"}, HTTPStatus.UNSUPPORTED_MEDIA_TYPE)
                return
            try:
                length = int(self.headers.get("Content-Length", "0"))
                if not 1 <= length <= 4096:
                    raise ValueError("request body is too large or empty")
                payload = json.loads(self.rfile.read(length))
                if not isinstance(payload, dict):
                    raise ValueError("history entry must be an object")
                store = self._history()
                if payload.get("action") == "delete":
                    store.delete(payload.get("id"))
                    self._send_json({"ok": True})
                else:
                    row = store.save_final(payload, now=datetime.now(timezone.utc))
                    self._send_json({"ok": True, "row": row})
            except (ValueError, TypeError, json.JSONDecodeError) as exc:
                self._send_json({"ok": False, "error": str(exc)}, HTTPStatus.BAD_REQUEST)
            except (Exception, SystemExit) as exc:
                self._send_internal_error(exc)
            return
        if parsed.path == "/api/check":
            try:
                self._send_json(check_alert_once())
            except (Exception, SystemExit) as exc:
                self._send_internal_error(exc)
            return
        if parsed.path in {"/", "/api/snapshot", "/api/competition"}:
            self._send_method_not_allowed("GET")
            return
        self.send_error(HTTPStatus.NOT_FOUND)

    def _do_unsupported_method(self) -> None:
        if self._require_auth():
            return
        allowed = _ALLOWED_METHODS.get(urlparse(self.path).path)
        if allowed is None:
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        self._send_method_not_allowed(allowed)

    do_DELETE = _do_unsupported_method
    do_CONNECT = _do_unsupported_method
    do_HEAD = _do_unsupported_method
    do_OPTIONS = _do_unsupported_method
    do_PATCH = _do_unsupported_method
    do_PUT = _do_unsupported_method
    do_TRACE = _do_unsupported_method

    def log_message(self, fmt: str, *args: Any) -> None:
        print(f"{datetime.now().isoformat(timespec='seconds')} {self.client_address[0]} {fmt % args}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Serve a Binance Alpha volume dashboard.")
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    return parser.parse_args()


def main() -> int:
    try:
        _configured_auth_credentials()
    except AuthConfigurationError as exc:
        raise SystemExit(str(exc)) from None
    args = parse_args()
    server = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"Serving Binance Alpha dashboard on http://{args.host}:{args.port}")
    server.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
