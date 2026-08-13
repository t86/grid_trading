from __future__ import annotations

import base64
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass
from http.client import HTTPConnection
from http.server import ThreadingHTTPServer
import math
from pathlib import Path
import threading
import time
from typing import Any
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import pytest

from grid_optimizer.alpha_market import AlphaToken
from grid_optimizer import alpha_competition_dashboard as dashboard


class FakeMarket:
    def __init__(self) -> None:
        self.fetch_token_calls = 0
        self.kline_calls: list[tuple[str, str, int]] = []

    def fetch_tokens(self) -> dict[str, AlphaToken]:
        self.fetch_token_calls += 1
        return {
            "QUID": AlphaToken("QUID", "ALPHA_1075", "Squid", "Solana", 0.1, 1_000, 10, "ALPHA_1075USDC"),
            "GRVT": AlphaToken("GRVT", "ALPHA_1076", "Grvt", "BSC", 0.2, 2_000, 20, "ALPHA_1076USDT"),
        }

    def fetch_klines(
        self,
        pair: str,
        *,
        interval: str,
        limit: int,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
    ) -> list[list[Any]]:
        assert start_time_ms is None
        assert end_time_ms is None
        self.kline_calls.append((pair, interval, limit))
        latest = 80.0 if pair.endswith("USDT") else 40.0
        return [
            _kline(1, 10.0, 1),
            _kline(2, 20.0, 2),
            _kline(3, latest, 3),
            _kline(4, 99_999.0, 4),
        ]

    def fetch_ticker(self, pair: str) -> dict[str, str]:
        return {
            "lastPrice": "0.25",
            "quoteVolume": "123456.75",
            "priceChangePercent": "4.5" if pair.endswith("USDT") else "-1.25",
        }


def _kline(close_time: float | int, quote_volume: float, trades: float | int) -> list[Any]:
    row: list[Any] = [0] * 9
    row[6] = close_time * 1_000
    row[7] = str(quote_volume)
    row[8] = trades
    return row


def _basic_auth(username: str, password: str, *, scheme: str = "Basic", suffix: str = "") -> str:
    token = base64.b64encode(f"{username}:{password}".encode()).decode("ascii")
    return f"{scheme} {token}{suffix}"


def _clear_auth_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in (
        "ALPHA_DASHBOARD_USERNAME",
        "ALPHA_DASHBOARD_PASSWORD",
        "GRID_WEB_USERNAME",
        "GRID_WEB_PASSWORD",
    ):
        monkeypatch.delenv(name, raising=False)


class FakeCompetitionService:
    def __init__(self) -> None:
        self.calls: list[list[str]] = []

    def collect(self, symbols: list[str]) -> dict[str, Any]:
        self.calls.append(symbols)
        return {
            "generatedAtUtc": "2026-08-07T00:00:00+00:00",
            "rows": [{"symbol": symbol, "name": "章鱼"} for symbol in symbols],
            "errors": [],
        }


class DeferredSubmitter:
    def __init__(self) -> None:
        self.tasks: list[Any] = []

    def submit(self, task: Any) -> None:
        self.tasks.append(task)

    def run_next(self) -> None:
        task = self.tasks.pop(0)
        task()


class FakeLeaderboardResponse:
    def __init__(self, payload: Any) -> None:
        self.payload = payload

    def json(self) -> Any:
        return self.payload


class FakeLeaderboardSession:
    def __init__(self, payload: Any = None, *, error: Exception | None = None) -> None:
        self.payload = payload
        self.error = error
        self.calls: list[tuple[str, dict[str, Any], dict[str, str], int]] = []

    def __enter__(self) -> FakeLeaderboardSession:
        return self

    def __exit__(self, *args: Any) -> None:
        return None

    def get(
        self,
        url: str,
        *,
        params: dict[str, Any],
        headers: dict[str, str],
        timeout: int,
    ) -> FakeLeaderboardResponse:
        self.calls.append((url, params, headers, timeout))
        if self.error is not None:
            raise self.error
        return FakeLeaderboardResponse(self.payload)


def _leaderboard_row() -> dict[str, Any]:
    return {
        "symbol": "QUID",
        "status": "active",
        "winnerCount": 2_500,
        "articleUrl": "https://www.binance.com/en/support/announcement/article-quid",
    }


@dataclass(frozen=True)
class HttpResult:
    status_code: int
    headers: Any
    body: bytes

    def json(self) -> Any:
        import json

        return json.loads(self.body)


class HttpServerHarness:
    def __init__(self, server: ThreadingHTTPServer) -> None:
        self.server = server
        host, port = server.server_address
        self.base_url = f"http://{host}:{port}"

    def request(
        self,
        method: str,
        path: str,
        *,
        auth: bool,
        authorization: str | None = None,
    ) -> HttpResult:
        headers: dict[str, str] = {}
        if authorization is not None:
            headers["Authorization"] = authorization
        elif auth:
            token = base64.b64encode(b"alpha-user:alpha-password").decode("ascii")
            headers["Authorization"] = f"Basic {token}"
        request = Request(
            f"{self.base_url}{path}",
            data=b"" if method == "POST" else None,
            headers=headers,
            method=method,
        )
        if method == "CONNECT":
            host, port = self.server.server_address
            connection = HTTPConnection(host, port, timeout=2)
            try:
                connection.request(method, path, headers=headers)
                response = connection.getresponse()
                return HttpResult(response.status, response.headers, response.read())
            finally:
                connection.close()
        try:
            response = urlopen(request, timeout=2)
        except HTTPError as exc:
            return HttpResult(exc.code, exc.headers, exc.read())
        with response:
            return HttpResult(response.status, response.headers, response.read())

    def get(self, path: str, *, auth: bool = True) -> HttpResult:
        return self.request("GET", path, auth=auth)


@contextmanager
def running_server(
    *,
    market: Any | None = None,
    competition_service: Any | None = None,
) -> Any:
    server = ThreadingHTTPServer(("127.0.0.1", 0), dashboard.Handler)
    if market is not None:
        server.market = market  # type: ignore[attr-defined]
    if competition_service is not None:
        server.competition_service = competition_service  # type: ignore[attr-defined]
    thread = threading.Thread(target=server.serve_forever, name="test-alpha-dashboard")
    thread.start()
    try:
        yield HttpServerHarness(server)
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
        assert not thread.is_alive()
        assert server.socket.fileno() == -1


@pytest.fixture(autouse=True)
def reset_competition_service() -> Any:
    dashboard._COMPETITION_SERVICE = None
    yield
    dashboard._COMPETITION_SERVICE = None


@pytest.fixture
def fake_market() -> FakeMarket:
    return FakeMarket()


@pytest.fixture
def fake_competition_service() -> FakeCompetitionService:
    return FakeCompetitionService()


@pytest.fixture
def http_server(
    monkeypatch: pytest.MonkeyPatch,
    fake_market: FakeMarket,
    fake_competition_service: FakeCompetitionService,
) -> Any:
    monkeypatch.setenv("ALPHA_DASHBOARD_USERNAME", "alpha-user")
    monkeypatch.setenv("ALPHA_DASHBOARD_PASSWORD", "alpha-password")
    monkeypatch.setenv("ALPHA_SYMBOLS", "QUID,GRVT,O,PRL,CAP")
    monkeypatch.setattr(
        dashboard,
        "check_alert_once",
        lambda: {"ok": True, "returnCode": 0, "elapsedMs": 1, "dryRun": True},
    )
    with running_server(market=fake_market, competition_service=fake_competition_service) as server:
        yield server


def test_symbols_from_env_preserves_configuration_order(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ALPHA_SYMBOLS", " grvt, QUID, o ,,PRL,cap ")

    assert dashboard._symbols_from_env() == ["GRVT", "QUID", "O", "PRL", "CAP"]


def test_symbols_from_env_uses_current_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ALPHA_SYMBOLS", raising=False)

    assert dashboard._symbols_from_env() == ["QUID", "GRVT", "O", "PRL", "CAP"]


def test_snapshot_preserves_production_fields(fake_market: FakeMarket) -> None:
    row = dashboard.collect_snapshot(["QUID"], market=fake_market)["rows"][0]

    assert set(row) >= {
        "symbol",
        "name",
        "alphaId",
        "pair",
        "lastPrice",
        "latest1mQuoteVolume",
        "previous1mQuoteVolume",
        "delta1mQuoteVolume",
        "latest1hQuoteVolume",
        "baselineQuoteVolume",
        "multiple",
        "trades",
        "closedUtc",
        "tickerQuoteVolume24h",
        "priceChangePercent24h",
    }
    assert row["latest1mQuoteVolume"] == 40.0
    assert row["closedUtc"] == "1970-01-01T00:00:03+00:00"
    assert fake_market.kline_calls == [("ALPHA_1075USDC", "1m", 63)]


def test_snapshot_preserves_multiple_sort_and_symbol_errors(fake_market: FakeMarket) -> None:
    payload = dashboard.collect_snapshot(["QUID", "MISSING", "GRVT"], market=fake_market)

    assert [row["symbol"] for row in payload["rows"]] == ["GRVT", "QUID"]
    assert payload["symbols"] == ["QUID", "MISSING", "GRVT"]
    assert payload["errors"] == ["MISSING: not found in Binance Alpha token list"]


def test_check_alert_delegates_to_versioned_alert_module() -> None:
    with patch("grid_optimizer.alpha_competition_dashboard.alert.run", return_value=0) as run:
        result = dashboard.check_alert_once()

    assert result["ok"] is True
    run.assert_called_once()


def test_competition_service_is_lazy_thread_safe_and_uses_configured_cache(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    created: list[dict[str, Any]] = []
    market = object()
    provider = object()
    cache = object()
    volume_provider = object()
    service = object()
    cache_path = tmp_path / "rules.json"
    monkeypatch.setenv("ALPHA_COMPETITION_RULE_CACHE", str(cache_path))
    monkeypatch.setattr(dashboard, "AlphaMarketClient", lambda: market)
    monkeypatch.setattr(dashboard, "BinanceCompetitionRuleProvider", lambda: provider)
    monkeypatch.setattr(
        dashboard,
        "CompetitionRuleCache",
        lambda path: created.append({"cache_path": path}) or cache,
    )
    monkeypatch.setattr(
        dashboard,
        "CompetitionVolumeProvider",
        lambda *, market: created.append({"volume_market": market}) or volume_provider,
    )
    monkeypatch.setattr(
        dashboard,
        "CompetitionMetricsService",
        lambda **kwargs: created.append(kwargs) or service,
    )

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(lambda _: dashboard.competition_service(), range(32)))

    assert results == [service] * 32
    assert created == [
        {"cache_path": cache_path},
        {"volume_market": market},
        {"rule_provider": provider, "rule_cache": cache, "volume_provider": volume_provider},
    ]


def test_competition_service_uses_official_default_cache_path(monkeypatch: pytest.MonkeyPatch) -> None:
    paths: list[Path] = []
    monkeypatch.delenv("ALPHA_COMPETITION_RULE_CACHE", raising=False)
    monkeypatch.setattr(dashboard, "AlphaMarketClient", lambda: object())
    monkeypatch.setattr(dashboard, "BinanceCompetitionRuleProvider", lambda: object())
    monkeypatch.setattr(
        dashboard,
        "CompetitionRuleCache",
        lambda path: paths.append(path) or object(),
    )
    monkeypatch.setattr(dashboard, "CompetitionVolumeProvider", lambda *, market: object())
    monkeypatch.setattr(dashboard, "CompetitionMetricsService", lambda **kwargs: object())

    dashboard.competition_service()

    assert paths == [Path("/home/ubuntu/.cache/binance-alpha-volume-alert/competition_rules.json")]


def test_all_routes_require_basic_auth(http_server: HttpServerHarness) -> None:
    for method, path in [
        ("GET", "/"),
        ("GET", "/api/snapshot"),
        ("GET", "/api/competition"),
        ("POST", "/api/check"),
    ]:
        response = http_server.request(method, path, auth=False)
        assert response.status_code == 401
        assert response.headers["WWW-Authenticate"] == 'Basic realm="Binance Alpha Monitor"'


def test_auth_fails_closed_without_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_auth_environment(monkeypatch)

    with running_server() as server:
        response = server.request(
            "GET",
            "/",
            auth=False,
            authorization=_basic_auth("alpha-user", "alpha-password"),
        )

    assert response.status_code == 401


@pytest.mark.parametrize(
    "environment",
    [
        {"ALPHA_DASHBOARD_USERNAME": "alpha-user"},
        {"ALPHA_DASHBOARD_PASSWORD": "alpha-password"},
        {
            "ALPHA_DASHBOARD_USERNAME": "alpha-user",
            "GRID_WEB_PASSWORD": "alpha-password",
        },
        {
            "ALPHA_DASHBOARD_USERNAME": "alpha-user",
            "GRID_WEB_USERNAME": "grid-user",
            "GRID_WEB_PASSWORD": "grid-password",
        },
    ],
)
def test_auth_fails_closed_for_incomplete_or_mixed_credentials(
    monkeypatch: pytest.MonkeyPatch,
    environment: dict[str, str],
) -> None:
    _clear_auth_environment(monkeypatch)
    for name, value in environment.items():
        monkeypatch.setenv(name, value)

    with running_server() as server:
        response = server.request("GET", "/", auth=False, authorization=_basic_auth("alpha-user", "alpha-password"))

    assert response.status_code == 401


def test_auth_uses_complete_grid_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_auth_environment(monkeypatch)
    monkeypatch.setenv("GRID_WEB_USERNAME", "grid-user")
    monkeypatch.setenv("GRID_WEB_PASSWORD", "grid-password")

    with running_server() as server:
        response = server.request(
            "GET",
            "/",
            auth=False,
            authorization=_basic_auth("grid-user", "grid-password"),
        )

    assert response.status_code == 200


def test_basic_auth_scheme_is_case_insensitive_and_base64_is_strict(
    http_server: HttpServerHarness,
) -> None:
    accepted = http_server.request(
        "GET",
        "/",
        auth=False,
        authorization=_basic_auth("alpha-user", "alpha-password", scheme="bAsIc"),
    )
    rejected = http_server.request(
        "GET",
        "/",
        auth=False,
        authorization=_basic_auth("alpha-user", "alpha-password", suffix="!"),
    )

    assert accepted.status_code == 200
    assert rejected.status_code == 401


@pytest.mark.parametrize(
    "environment",
    [
        {},
        {"ALPHA_DASHBOARD_USERNAME": "alpha-user"},
        {"GRID_WEB_PASSWORD": "grid-password"},
    ],
)
def test_main_rejects_missing_or_incomplete_auth_before_binding(
    monkeypatch: pytest.MonkeyPatch,
    environment: dict[str, str],
) -> None:
    _clear_auth_environment(monkeypatch)
    for name, value in environment.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setattr(dashboard, "parse_args", lambda: object())
    bound: list[object] = []
    monkeypatch.setattr(dashboard, "ThreadingHTTPServer", lambda *args: bound.append(args))

    with pytest.raises(SystemExit, match="credentials"):
        dashboard.main()

    assert bound == []


def test_root_preserves_existing_dashboard_html(http_server: HttpServerHarness) -> None:
    response = http_server.get("/")

    assert response.status_code == 200
    assert response.headers["Content-Type"] == "text/html; charset=utf-8"
    assert response.headers["Cache-Control"] == "no-store, max-age=0"
    text = response.body.decode("utf-8")
    assert "<title>Binance Alpha Monitor</title>" in text
    assert "<th>1m Vol</th>" in text
    assert "Check Alert" in text


def test_api_snapshot_uses_injected_market_and_json_headers(
    http_server: HttpServerHarness,
) -> None:
    response = http_server.get("/api/snapshot?symbols=QUID")

    assert response.status_code == 200
    assert response.headers["Content-Type"] == "application/json; charset=utf-8"
    assert response.headers["Cache-Control"] == "no-store, max-age=0"
    assert [row["symbol"] for row in response.json()["rows"]] == ["QUID"]


@pytest.mark.parametrize("failure_stage", ["klines", "ticker"])
def test_api_snapshot_keeps_healthy_symbols_when_one_market_lookup_fails(
    http_server: HttpServerHarness,
    failure_stage: str,
) -> None:
    secret = "private-token /private/market/cache.json"

    class PartiallyRaisingMarket(FakeMarket):
        def fetch_klines(self, pair: str, **kwargs: Any) -> list[list[Any]]:
            if failure_stage == "klines" and pair == "ALPHA_1075USDC":
                raise RuntimeError(secret)
            return super().fetch_klines(pair, **kwargs)

        def fetch_ticker(self, pair: str) -> dict[str, str]:
            if failure_stage == "ticker" and pair == "ALPHA_1075USDC":
                raise RuntimeError(secret)
            return super().fetch_ticker(pair)

    http_server.server.market = PartiallyRaisingMarket()  # type: ignore[attr-defined]

    response = http_server.get("/api/snapshot?symbols=QUID,GRVT")

    assert response.status_code == 200
    assert [row["symbol"] for row in response.json()["rows"]] == ["GRVT"]
    assert response.json()["errors"] == ["QUID: market data unavailable"]
    assert secret.encode() not in response.body


def test_api_competition_uses_configured_symbol_order_and_utf8_json(
    http_server: HttpServerHarness,
    fake_competition_service: FakeCompetitionService,
) -> None:
    response = http_server.get("/api/competition")

    assert response.status_code == 200
    assert response.headers["Content-Type"] == "application/json; charset=utf-8"
    assert response.headers["Cache-Control"] == "no-store, max-age=0"
    assert [row["symbol"] for row in response.json()["rows"]] == ["QUID", "GRVT", "O", "PRL", "CAP"]
    assert b"\xe7\xab\xa0\xe9\xb1\xbc" in response.body
    assert fake_competition_service.calls == [["QUID", "GRVT", "O", "PRL", "CAP"]]


def test_apply_leaderboard_thresholds_returns_immediately_and_refreshes_single_flight() -> None:
    submitter = DeferredSubmitter()
    fetch_calls: list[str] = []

    def fetch(row: dict[str, Any]) -> dict[str, Any]:
        fetch_calls.append(row["symbol"])
        return {
            "leaderboardThreshold": 12_345.0,
            "leaderboardThresholdRank": 2_500,
            "leaderboardThresholdUpdatedAt": "2026-08-13T03:00:00+00:00",
            "leaderboardThresholdUpdatedAtLabel": "",
            "leaderboardThresholdSource": "binance_private_api",
            "leaderboardThresholdNote": "available",
        }

    refresher = dashboard._LeaderboardThresholdRefresher(
        fetcher=fetch,
        submitter=submitter,
        clock=lambda: 100.0,
    )
    payload = {"rows": [_leaderboard_row()]}

    first = dashboard._apply_leaderboard_thresholds(payload, refresher=refresher)
    with ThreadPoolExecutor(max_workers=8) as pool:
        concurrent = list(
            pool.map(
                lambda _: dashboard._apply_leaderboard_thresholds(
                    {"rows": [_leaderboard_row()]},
                    refresher=refresher,
                ),
                range(16),
            )
        )

    assert fetch_calls == []
    assert len(submitter.tasks) == 1
    assert first["rows"][0]["leaderboardThreshold"] is None
    assert first["rows"][0]["leaderboardThresholdNote"] == "leaderboard refresh in progress"
    assert all(
        result["rows"][0]["leaderboardThresholdNote"] == "leaderboard refresh in progress"
        for result in concurrent
    )

    submitter.run_next()

    refreshed = dashboard._apply_leaderboard_thresholds({"rows": [_leaderboard_row()]}, refresher=refresher)
    assert fetch_calls == ["QUID"]
    assert refreshed["rows"][0]["leaderboardThreshold"] == 12_345.0
    assert len(submitter.tasks) == 0


def test_leaderboard_refresh_failure_is_redacted_and_cached_until_retry_ttl() -> None:
    submitter = DeferredSubmitter()
    now = [100.0]
    secret = "private-cookie /private/cookie.txt"
    fetch_calls = 0

    def fetch(row: dict[str, Any]) -> dict[str, Any]:
        nonlocal fetch_calls
        fetch_calls += 1
        raise RuntimeError(secret)

    refresher = dashboard._LeaderboardThresholdRefresher(
        fetcher=fetch,
        submitter=submitter,
        clock=lambda: now[0],
        failure_ttl=15.0,
    )

    refresher.get(_leaderboard_row())
    submitter.run_next()
    failed = refresher.get(_leaderboard_row())
    again = refresher.get(_leaderboard_row())

    assert fetch_calls == 1
    assert len(submitter.tasks) == 0
    assert failed == again
    assert failed["leaderboardThreshold"] is None
    assert failed["leaderboardThresholdNote"] == "leaderboard API unavailable"
    assert secret not in str(failed)

    now[0] += 16.0
    refresher.get(_leaderboard_row())
    assert len(submitter.tasks) == 1


@pytest.mark.parametrize(
    "payload",
    [
        {
            "symbol": "QUID",
            "articleCode": "article-quid",
            "rows": [{"rank": 2_499, "volume": "101"}],
        },
        {
            "symbol": "QUID",
            "articleCode": "article-quid",
            "rows": [{"rank": 2_500.9, "volume": "101"}],
        },
        {
            "symbol": "QUID",
            "articleCode": "article-quid",
            "rows": [{"rank": "2500.9", "volume": "101"}],
        },
        {
            "symbol": "QUID",
            "articleCode": "article-quid",
            "rows": [{"rank": "2500.0", "volume": "101"}],
        },
        {
            "symbol": "QUID",
            "articleCode": "article-quid",
            "rows": [{"rank": float("nan"), "volume": "101"}],
        },
        {
            "symbol": "QUID",
            "articleCode": "article-quid",
            "rows": [{"rank": float("inf"), "volume": "101"}],
        },
        {
            "symbol": "GRVT",
            "articleCode": "article-quid",
            "rows": [{"rank": 2_500, "volume": "102"}],
        },
        {
            "symbol": "QUID",
            "articleCode": "article-grvt",
            "rows": [{"rank": 2_500, "volume": "103"}],
        },
        {
            "symbol": "QUID",
            "articleCode": "article-quid",
            "rows": [{"ranking": 2_500, "volume": "104"}],
        },
        {
            "symbol": "QUID",
            "articleCode": "article-quid",
            "rows": [{"rank": 2_500}, {"volume": "104"}],
        },
        {"rows": [{"rank": 2_500, "volume": "104"}]},
    ],
    ids=[
        "wrong-rank",
        "fractional-numeric-rank",
        "fractional-string-rank",
        "decimal-string-rank",
        "nan-rank",
        "infinite-rank",
        "wrong-symbol",
        "wrong-article",
        "missing-exact-rank",
        "split-rank-volume",
        "missing-identity",
    ],
)
def test_fetch_leaderboard_threshold_rejects_unverified_competition_or_rank(
    monkeypatch: pytest.MonkeyPatch,
    payload: dict[str, Any],
) -> None:
    monkeypatch.setenv("BINANCE_WEB_COOKIE", "private-cookie")
    session = FakeLeaderboardSession(payload)

    result = dashboard._fetch_leaderboard_threshold(_leaderboard_row(), session_factory=lambda: session)

    assert result["leaderboardThreshold"] is None
    assert result["leaderboardThresholdRank"] == 2_500
    assert result["leaderboardThresholdNote"] == "leaderboard API unavailable"


@pytest.mark.parametrize(
    "payload",
    [
        {
            "data": [
                {"symbol": "QUID"},
                {"articleCode": "article-quid"},
                {"rows": [{"rank": 2_500, "volume": "12345.5"}]},
            ]
        },
        {
            "competition": {"symbol": "QUID", "articleCode": "article-quid"},
            "rows": [{"rank": 2_500, "volume": "12345.5"}],
        },
    ],
    ids=["identity-split-across-objects", "identity-and-ranks-unrelated-siblings"],
)
def test_fetch_leaderboard_threshold_rejects_unbound_identity_and_ranks(
    monkeypatch: pytest.MonkeyPatch,
    payload: dict[str, Any],
) -> None:
    monkeypatch.setenv("BINANCE_WEB_COOKIE", "private-cookie")
    session = FakeLeaderboardSession(payload)

    result = dashboard._fetch_leaderboard_threshold(_leaderboard_row(), session_factory=lambda: session)

    assert result["leaderboardThreshold"] is None
    assert result["leaderboardThresholdNote"] == "leaderboard API unavailable"


def test_fetch_leaderboard_threshold_accepts_rank_from_bound_competition_container(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("BINANCE_WEB_COOKIE", "private-cookie")
    session = FakeLeaderboardSession(
        {
            "data": {
                "competitions": [
                    {
                        "symbol": "GRVT",
                        "articleCode": "article-grvt",
                        "rows": [{"rank": 2_500, "volume": "99999"}],
                    },
                    {
                        "symbol": "QUID",
                        "articleCode": "article-quid",
                        "rankList": [{"rank": 2_500, "volume": "12345.5"}],
                    },
                ]
            }
        }
    )

    result = dashboard._fetch_leaderboard_threshold(_leaderboard_row(), session_factory=lambda: session)

    assert result["leaderboardThreshold"] == 12_345.5
    assert result["leaderboardThresholdRank"] == 2_500


@pytest.mark.parametrize(
    "volume",
    [True, False, float("nan"), float("inf"), -1, "-1"],
    ids=["true", "false", "nan", "infinite", "negative-number", "negative-string"],
)
def test_fetch_leaderboard_threshold_rejects_invalid_volume(
    monkeypatch: pytest.MonkeyPatch,
    volume: Any,
) -> None:
    monkeypatch.setenv("BINANCE_WEB_COOKIE", "private-cookie")
    session = FakeLeaderboardSession(
        {
            "symbol": "QUID",
            "articleCode": "article-quid",
            "rows": [{"rank": 2_500, "volume": volume}],
        }
    )

    result = dashboard._fetch_leaderboard_threshold(_leaderboard_row(), session_factory=lambda: session)

    assert result["leaderboardThreshold"] is None
    assert result["leaderboardThresholdNote"] == "leaderboard API unavailable"


@pytest.mark.parametrize(
    "winner_count",
    [True, False, 2_500.9, "2500.9", float("nan"), float("inf")],
    ids=["true", "false", "fractional-number", "fractional-string", "nan", "infinite"],
)
def test_fetch_leaderboard_threshold_rejects_invalid_winner_count(
    monkeypatch: pytest.MonkeyPatch,
    winner_count: Any,
) -> None:
    monkeypatch.setenv("BINANCE_WEB_COOKIE", "private-cookie")
    row = {**_leaderboard_row(), "winnerCount": winner_count}
    session = FakeLeaderboardSession(
        {
            "symbol": "QUID",
            "articleCode": "article-quid",
            "rows": [{"rank": 2_500, "volume": "12345.5"}],
        }
    )

    result = dashboard._fetch_leaderboard_threshold(row, session_factory=lambda: session)

    assert result["leaderboardThreshold"] is None
    assert result["leaderboardThresholdRank"] is None
    assert session.calls == []


@pytest.mark.parametrize("rank", [2_500, "2500"], ids=["integer", "canonical-string"])
@pytest.mark.parametrize("winner_count", [2_500, "2500"], ids=["integer-winners", "string-winners"])
def test_fetch_leaderboard_threshold_accepts_exact_identity_rank_and_winner_count(
    monkeypatch: pytest.MonkeyPatch,
    rank: Any,
    winner_count: Any,
) -> None:
    monkeypatch.setenv("BINANCE_WEB_COOKIE", "private-cookie")
    session = FakeLeaderboardSession(
        {
            "symbol": "quid",
            "articleCode": "article-quid",
            "rows": [{"rank": rank, "volume": "12345.5"}],
        }
    )

    row = {**_leaderboard_row(), "winnerCount": winner_count}
    result = dashboard._fetch_leaderboard_threshold(row, session_factory=lambda: session)

    assert result["leaderboardThreshold"] == 12_345.5
    assert result["leaderboardThresholdRank"] == 2_500
    assert result["leaderboardThresholdNote"] == "available"
    assert all(call[0].startswith("https://www.binance.com/") for call in session.calls)


def test_fetch_leaderboard_threshold_reports_invalid_cookie_without_echoing_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    secret = "private-cookie /private/cookie.txt"
    monkeypatch.setenv("BINANCE_WEB_COOKIE", secret)
    session = FakeLeaderboardSession({"code": "100001005", "message": secret})

    result = dashboard._fetch_leaderboard_threshold(_leaderboard_row(), session_factory=lambda: session)

    assert result["leaderboardThreshold"] is None
    assert result["leaderboardThresholdNote"] == "Binance login cookie is invalid or expired"
    assert secret not in str(result)


def test_api_check_preserves_post_behavior_and_json_headers(http_server: HttpServerHarness) -> None:
    response = http_server.request("POST", "/api/check", auth=True)

    assert response.status_code == 200
    assert response.headers["Content-Type"] == "application/json; charset=utf-8"
    assert response.headers["Cache-Control"] == "no-store, max-age=0"
    assert response.json()["ok"] is True


@pytest.mark.parametrize(
    ("method", "path", "target"),
    [
        ("GET", "/api/snapshot", "market"),
        ("GET", "/api/competition", "competition"),
        ("POST", "/api/check", "check"),
    ],
)
def test_api_internal_errors_are_stable_json_and_redacted(
    http_server: HttpServerHarness,
    monkeypatch: pytest.MonkeyPatch,
    method: str,
    path: str,
    target: str,
) -> None:
    secret = "secret-token /private/cache/rules.json"

    class RaisingMarket:
        def fetch_tokens(self) -> Any:
            raise RuntimeError(secret)

    class RaisingCompetitionService:
        def collect(self, symbols: list[str]) -> Any:
            raise RuntimeError(secret)

    if target == "market":
        http_server.server.market = RaisingMarket()  # type: ignore[attr-defined]
    elif target == "competition":
        http_server.server.competition_service = RaisingCompetitionService()  # type: ignore[attr-defined]
    else:
        def raise_system_exit() -> Any:
            raise SystemExit(secret)

        monkeypatch.setattr(dashboard, "check_alert_once", raise_system_exit)

    response = http_server.request(method, path, auth=True)

    assert response.status_code == 500
    assert response.headers["Content-Type"] == "application/json; charset=utf-8"
    assert response.headers["Cache-Control"] == "no-store, max-age=0"
    assert response.json() == {"ok": False, "error": "internal server error"}
    assert secret.encode() not in response.body


def test_concurrent_api_checks_are_serialized(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_auth_environment(monkeypatch)
    monkeypatch.setenv("ALPHA_DASHBOARD_USERNAME", "alpha-user")
    monkeypatch.setenv("ALPHA_DASHBOARD_PASSWORD", "alpha-password")
    worker_count = 8
    start = threading.Barrier(worker_count)
    counter_lock = threading.Lock()
    active = 0
    maximum_active = 0
    total_calls = 0

    def fake_run(args: Any) -> int:
        nonlocal active, maximum_active, total_calls
        with counter_lock:
            active += 1
            total_calls += 1
            maximum_active = max(maximum_active, active)
        time.sleep(0.03)
        with counter_lock:
            active -= 1
        return 0

    monkeypatch.setattr(dashboard.alert, "run", fake_run)
    with running_server() as server:
        def request_check(_: int) -> HttpResult:
            start.wait(timeout=2)
            return server.request("POST", "/api/check", auth=True)

        with ThreadPoolExecutor(max_workers=worker_count) as pool:
            responses = list(pool.map(request_check, range(worker_count)))

    assert [response.status_code for response in responses] == [200] * worker_count
    assert total_calls == worker_count
    assert maximum_active == 1


def test_snapshot_api_normalizes_non_finite_market_and_config_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_auth_environment(monkeypatch)
    monkeypatch.setenv("ALPHA_DASHBOARD_USERNAME", "alpha-user")
    monkeypatch.setenv("ALPHA_DASHBOARD_PASSWORD", "alpha-password")
    monkeypatch.setenv("ALPHA_SYMBOLS", "QUID")
    monkeypatch.setenv("ALPHA_SPIKE_MULTIPLE", "NaN")
    monkeypatch.setenv("ALPHA_MIN_QUOTE_VOLUME", "Infinity")
    monkeypatch.setenv("ALPHA_ABSOLUTE_MIN_QUOTE_VOLUME", "-Infinity")
    monkeypatch.setenv("ALPHA_COOLDOWN_MINUTES", "Infinity")

    class NonFiniteMarket(FakeMarket):
        def fetch_tokens(self) -> dict[str, AlphaToken]:
            self.fetch_token_calls += 1
            return {
                "QUID": AlphaToken(
                    "QUID",
                    "ALPHA_1075",
                    "Squid",
                    "Solana",
                    math.inf,
                    math.nan,
                    10,
                    "ALPHA_1075USDC",
                )
            }

        def fetch_klines(self, pair: str, **kwargs: Any) -> list[list[Any]]:
            return [
                _kline(1, math.nan, 1),
                _kline(2, math.inf, 2),
                _kline(math.inf, -math.inf, math.inf),
                _kline(4, math.nan, 4),
            ]

        def fetch_ticker(self, pair: str) -> dict[str, str]:
            return {
                "lastPrice": "NaN",
                "quoteVolume": "Infinity",
                "priceChangePercent": "-Infinity",
            }

    with running_server(market=NonFiniteMarket()) as server:
        response = server.get("/api/snapshot")

    assert response.status_code == 200
    payload = response.json()
    assert all(
        math.isfinite(value)
        for value in (
            payload["rows"][0]["lastPrice"],
            payload["rows"][0]["latest1mQuoteVolume"],
            payload["rows"][0]["previous1mQuoteVolume"],
            payload["rows"][0]["delta1mQuoteVolume"],
            payload["rows"][0]["latest1hQuoteVolume"],
            payload["rows"][0]["baselineQuoteVolume"],
            payload["rows"][0]["multiple"],
            payload["rows"][0]["tickerQuoteVolume24h"],
            payload["rows"][0]["priceChangePercent24h"],
            payload["config"]["spikeMultiple"],
            payload["config"]["minQuoteVolume"],
            payload["config"]["absoluteMinQuoteVolume"],
        )
    )
    assert payload["rows"][0]["trades"] == 0
    assert payload["rows"][0]["closedUtc"] == ""
    assert payload["config"]["cooldownMinutes"] == 30
    assert b"NaN" not in response.body
    assert b"Infinity" not in response.body


@pytest.mark.parametrize(
    "query",
    [
        "quid",
        "QUID,,O",
        f"{'A' * 33}",
        ",".join(f"S{index}" for index in range(33)),
    ],
)
def test_snapshot_query_rejects_invalid_or_excessive_symbols_without_market_call(
    http_server: HttpServerHarness,
    fake_market: FakeMarket,
    query: str,
) -> None:
    response = http_server.get(f"/api/snapshot?symbols={query}")

    assert response.status_code == 400
    assert response.headers["Content-Type"] == "application/json; charset=utf-8"
    assert response.headers["Cache-Control"] == "no-store, max-age=0"
    assert fake_market.fetch_token_calls == 0


def test_snapshot_query_deduplicates_symbols_without_changing_order(
    http_server: HttpServerHarness,
) -> None:
    response = http_server.get("/api/snapshot?symbols=GRVT,QUID,GRVT,QUID")

    assert response.status_code == 200
    assert response.json()["symbols"] == ["GRVT", "QUID"]


def test_unknown_routes_and_wrong_api_methods_are_not_found(http_server: HttpServerHarness) -> None:
    assert http_server.get("/missing").status_code == 404
    get_check = http_server.get("/api/check")
    post_snapshot = http_server.request("POST", "/api/snapshot", auth=True)
    assert get_check.status_code == 405
    assert get_check.headers["Allow"] == "POST"
    assert post_snapshot.status_code == 405
    assert post_snapshot.headers["Allow"] == "GET"


def test_unsupported_methods_authenticate_before_method_rejection(http_server: HttpServerHarness) -> None:
    assert http_server.request("PUT", "/api/snapshot", auth=False).status_code == 401
    response = http_server.request("PUT", "/api/snapshot", auth=True)
    assert response.status_code == 405
    assert response.headers["Allow"] == "GET"
    assert response.headers["Content-Type"] == "application/json; charset=utf-8"
    assert response.headers["Cache-Control"] == "no-store, max-age=0"


@pytest.mark.parametrize("method", ["TRACE", "CONNECT"])
def test_trace_and_connect_authenticate_before_method_rejection(
    http_server: HttpServerHarness,
    method: str,
) -> None:
    assert http_server.request(method, "/api/snapshot", auth=False).status_code == 401

    response = http_server.request(method, "/api/snapshot", auth=True)

    assert response.status_code == 405
    assert response.headers["Allow"] == "GET"
    assert response.headers["Content-Type"] == "application/json; charset=utf-8"
    assert response.headers["Cache-Control"] == "no-store, max-age=0"


def test_dashboard_production_bind_defaults() -> None:
    assert dashboard.DEFAULT_HOST == "0.0.0.0"
    assert dashboard.DEFAULT_PORT == 8796


def test_page_places_competition_thresholds_above_existing_market_monitor() -> None:
    html = dashboard.INDEX_HTML

    assert html.index('id="competitionSection"') < html.index('id="marketSection"')
    for marker in (
        'id="competitionRows"',
        'class="competition-table"',
        'data-label="观察线 0.4"',
        'data-label="参考线 0.6"',
        'data-label="安全线 1.0"',
        'id="rows"',
        '<th>1m Vol</th>',
        'id="checkBtn"',
        '不含新锐交易者个人 1.2x 加成',
    ):
        assert marker in html


def test_page_has_responsive_table_and_mobile_card_contracts() -> None:
    html = dashboard.INDEX_HTML

    for marker in (
        ".competition-table { min-width:",
        "@media (max-width: 760px)",
        ".competition-table thead { display: none; }",
        ".competition-table tbody tr {",
        ".competition-table tbody td::before",
        "content: attr(data-label)",
        "overflow-wrap: anywhere",
        "@media (prefers-reduced-motion: reduce)",
    ):
        assert marker in html


def test_mobile_multiline_values_stay_inside_the_value_column() -> None:
    html = dashboard.INDEX_HTML

    assert 'class="cell-value"' in html
    assert 'class="cell-value source-cell"' in html
    assert 'class="cell-value token-cell"' in html
    assert ".competition-table .cell-value { min-width: 0; text-align: right; }" in html


def test_page_renders_all_competition_states_without_coercing_missing_values() -> None:
    html = dashboard.INDEX_HTML

    for marker in (
        "function formatU(value)",
        "if (typeof value !== 'number' || !Number.isFinite(value)) return '—'",
        "function formatCountdown(endUtc",
        "'upcoming': '尚未开始'",
        "'active': '进行中'",
        "'between_rounds': '轮次间隔'",
        "'ended': '交易赛已结束'",
        "'rule_unavailable': '规则暂不可用'",
        "'volume_unavailable': '交易量暂不可用'",
        "volumeUpdatedAtUtc",
    ):
        assert marker in html


def test_page_escapes_api_content_and_allows_only_web_article_links() -> None:
    html = dashboard.INDEX_HTML

    for marker in (
        "function escapeHtml(value)",
        "function safeArticleUrl(value)",
        "url.protocol === 'http:' || url.protocol === 'https:'",
        "escapeHtml(row.symbol)",
        "escapeHtml(row.name)",
        "escapeHtml(row.alphaId)",
        "escapeHtml(row.error)",
        'rel="noopener noreferrer"',
    ):
        assert marker in html


def test_page_refreshes_market_and_competition_independently() -> None:
    html = dashboard.INDEX_HTML

    assert "Promise.allSettled" in html
    assert "fetchJson('api/snapshot')" in html
    assert "fetchJson('api/competition')" in html
    assert "if (!response.ok)" in html
    assert "renderMarket(marketResult.value)" in html
    assert "renderCompetition(competitionResult.value)" in html
    assert "competitionErrorsEl.textContent" in html
    assert 'id="alertStatus"' in html
    assert "await refresh()" in html
    assert "alertStatusEl.textContent" in html


def test_page_ignores_stale_refresh_generations_and_latest_request_owns_button() -> None:
    html = dashboard.INDEX_HTML

    for marker in (
        "let refreshGeneration = 0",
        "const generation = ++refreshGeneration",
        "if (generation !== refreshGeneration) return",
        "finally",
        "if (generation === refreshGeneration) refreshBtn.disabled = false",
    ):
        assert marker in html


def test_page_validates_api_payloads_and_isolates_renderer_failures() -> None:
    html = dashboard.INDEX_HTML

    for marker in (
        "function validatePayload(data, label)",
        "data === null",
        "typeof data !== 'object'",
        "Array.isArray(data)",
        "!Array.isArray(data.rows)",
        "data.rows.every",
    ):
        assert marker in html
    market_branch = html.index("if (marketResult.status === 'fulfilled')")
    market_render = html.index("renderMarket(marketResult.value)", market_branch)
    market_catch = html.index("} catch (error)", market_render)
    competition_branch = html.index("if (competitionResult.status === 'fulfilled')")
    competition_render = html.index("renderCompetition(competitionResult.value)", competition_branch)
    competition_catch = html.index("} catch (error)", competition_render)
    assert market_branch < market_render < market_catch
    assert competition_branch < competition_render < competition_catch


def test_page_exposes_accessible_live_regions_and_text_source_badges() -> None:
    html = dashboard.INDEX_HTML

    assert html.count('role="status" aria-live="polite"') >= 3
    assert html.count('role="alert" aria-live="polite"') >= 2
    for marker in (
        'source-badge official',
        'source-badge estimate',
        'source-badge stale',
        'Alpha K线估算',
        '>官方<',
        '>数据已过期<',
    ):
        assert marker in html
