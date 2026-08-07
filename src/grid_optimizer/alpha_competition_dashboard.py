from __future__ import annotations

import argparse
import base64
import binascii
import hmac
import json
import math
import os
from pathlib import Path
import re
import threading
import time
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

from . import alpha_volume_alert as alert
from .alpha_market import AlphaMarketClient
from .alpha_competition_metrics import (
    BinanceCompetitionRuleProvider,
    CompetitionMetricsService,
    CompetitionRuleCache,
    CompetitionVolumeProvider,
)


DEFAULT_HOST = os.environ.get("ALPHA_DASHBOARD_HOST", "0.0.0.0")
DEFAULT_PORT = int(os.environ.get("ALPHA_DASHBOARD_PORT", "8796"))
DEFAULT_SYMBOLS = ("QUID", "GRVT", "O", "PRL", "CAP")
DEFAULT_RULE_CACHE = "/home/ubuntu/.cache/binance-alpha-volume-alert/competition_rules.json"

_COMPETITION_SERVICE: CompetitionMetricsService | None = None
_COMPETITION_SERVICE_LOCK = threading.Lock()
_ALERT_CHECK_LOCK = threading.Lock()
_SNAPSHOT_SYMBOL_RE = re.compile(r"[A-Z0-9]{1,32}")
_MAX_SNAPSHOT_SYMBOLS = 32
_ALLOWED_METHODS = {
    "/": "GET",
    "/api/snapshot": "GET",
    "/api/competition": "GET",
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


def _symbols_from_env() -> list[str]:
    raw = os.environ.get("ALPHA_SYMBOLS", ",".join(DEFAULT_SYMBOLS))
    return [symbol.strip().upper() for symbol in raw.split(",") if symbol.strip()]


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
    return {
        "generatedAtUtc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "symbols": symbols,
        "rows": rows,
        "errors": errors,
        "config": {
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


INDEX_HTML = r"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Binance Alpha Monitor</title>
  <style>
    :root {
      color-scheme: light;
      --bg: #f6f7f9;
      --panel: #ffffff;
      --text: #18202a;
      --muted: #667085;
      --line: #d9dee7;
      --accent: #0f766e;
      --accent-soft: #e6f3f1;
      --warn: #b45309;
      --danger: #b42318;
      --good: #047857;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      background: var(--bg);
      color: var(--text);
      letter-spacing: 0;
    }
    .shell { max-width: 1280px; margin: 0 auto; padding: 22px 18px 32px; }
    header {
      display: flex;
      align-items: flex-end;
      justify-content: space-between;
      gap: 16px;
      margin-bottom: 16px;
    }
    h1 { margin: 0; font-size: 24px; line-height: 1.15; font-weight: 720; }
    .sub { margin-top: 6px; color: var(--muted); font-size: 13px; }
    .actions { display: flex; align-items: center; gap: 8px; flex-wrap: wrap; justify-content: flex-end; }
    button {
      appearance: none;
      border: 1px solid var(--line);
      background: var(--panel);
      color: var(--text);
      border-radius: 7px;
      height: 34px;
      padding: 0 12px;
      font-weight: 650;
      cursor: pointer;
    }
    button.primary { background: var(--accent); border-color: var(--accent); color: white; }
    button:disabled { opacity: .55; cursor: wait; }
    .kpis {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 10px;
      margin: 14px 0;
    }
    .kpi {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 12px;
      min-height: 82px;
    }
    .kpi .label { color: var(--muted); font-size: 12px; }
    .kpi .value { margin-top: 8px; font-size: 21px; font-weight: 760; }
    .table-wrap {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      overflow: auto;
    }
    table { width: 100%; border-collapse: collapse; min-width: 1050px; }
    th, td { padding: 11px 12px; border-bottom: 1px solid var(--line); text-align: right; font-size: 13px; white-space: nowrap; }
    th { color: var(--muted); font-size: 12px; font-weight: 700; background: #fbfcfd; position: sticky; top: 0; }
    td:first-child, th:first-child { text-align: left; }
    tr:last-child td { border-bottom: 0; }
    .sym { font-weight: 760; }
    .name { color: var(--muted); font-size: 12px; margin-top: 2px; }
    .pill {
      display: inline-flex;
      align-items: center;
      min-width: 58px;
      justify-content: center;
      border-radius: 999px;
      padding: 3px 8px;
      background: var(--accent-soft);
      color: var(--accent);
      font-weight: 760;
    }
    .hot { background: #fff4e5; color: var(--warn); }
    .very-hot { background: #ffe8e5; color: var(--danger); }
    .positive { color: var(--good); }
    .negative { color: var(--danger); }
    .status { color: var(--muted); font-size: 12px; margin-top: 10px; min-height: 18px; }
    .errors { margin-top: 10px; color: var(--danger); font-size: 12px; line-height: 1.5; }
    @media (max-width: 760px) {
      header { align-items: flex-start; flex-direction: column; }
      .actions { justify-content: flex-start; }
      .kpis { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .shell { padding: 16px 10px 24px; }
    }
  </style>
</head>
<body>
  <div class="shell">
    <header>
      <div>
        <h1>Binance Alpha Monitor</h1>
        <div class="sub" id="generated">Loading...</div>
      </div>
      <div class="actions">
        <button id="refreshBtn" class="primary">Refresh</button>
        <button id="checkBtn">Check Alert</button>
      </div>
    </header>

    <section class="kpis">
      <div class="kpi"><div class="label">Symbols</div><div class="value" id="kpiSymbols">-</div></div>
      <div class="kpi"><div class="label">Max Multiple</div><div class="value" id="kpiMultiple">-</div></div>
      <div class="kpi"><div class="label">Top 1h Volume</div><div class="value" id="kpiVolume">-</div></div>
      <div class="kpi"><div class="label">Spike Rule</div><div class="value" id="kpiRule">-</div></div>
    </section>

    <div class="table-wrap">
      <table>
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
    <div class="status" id="status"></div>
    <div class="errors" id="errors"></div>
  </div>

  <script>
    const fmt = new Intl.NumberFormat('en-US', { maximumFractionDigits: 2 });
    const priceFmt = new Intl.NumberFormat('en-US', { maximumFractionDigits: 10 });
    const rowsEl = document.getElementById('rows');
    const statusEl = document.getElementById('status');
    const errorsEl = document.getElementById('errors');
    const refreshBtn = document.getElementById('refreshBtn');
    const checkBtn = document.getElementById('checkBtn');

    function money(v) { return fmt.format(v || 0); }
    function multipleClass(v) {
      if (v >= 5) return 'pill very-hot';
      if (v >= 3) return 'pill hot';
      return 'pill';
    }
    function render(data) {
      document.getElementById('generated').textContent = `Updated ${data.generatedAtUtc}`;
      document.getElementById('kpiSymbols').textContent = data.rows.length;
      const maxMult = data.rows.length ? data.rows[0].multiple : 0;
      document.getElementById('kpiMultiple').textContent = `${maxMult.toFixed(2)}x`;
      const topVol = data.rows.reduce((m, r) => Math.max(m, r.latest1hQuoteVolume || 0), 0);
      document.getElementById('kpiVolume').textContent = `${money(topVol)} U`;
      document.getElementById('kpiRule').textContent = `1m >= ${money(data.config.absoluteMinQuoteVolume)} U`;
      rowsEl.innerHTML = data.rows.map(row => {
        const pctClass = row.priceChangePercent24h >= 0 ? 'positive' : 'negative';
        return `<tr>
          <td><div class="sym">${row.symbol}</div><div class="name">${row.name} · ${row.alphaId}</div></td>
          <td>${priceFmt.format(row.lastPrice)}</td>
          <td>${money(row.latest1mQuoteVolume)}</td>
          <td class="${row.delta1mQuoteVolume >= 0 ? 'positive' : 'negative'}">${row.delta1mQuoteVolume >= 0 ? '+' : ''}${money(row.delta1mQuoteVolume)}</td>
          <td>${money(row.latest1hQuoteVolume)}</td>
          <td>${money(row.baselineQuoteVolume)}</td>
          <td><span class="${multipleClass(row.multiple)}">${row.multiple.toFixed(2)}x</span></td>
          <td>${fmt.format(row.trades || 0)}</td>
          <td>${money(row.tickerQuoteVolume24h)}</td>
          <td class="${pctClass}">${row.priceChangePercent24h.toFixed(2)}%</td>
          <td>${row.closedUtc}</td>
        </tr>`;
      }).join('');
      errorsEl.textContent = (data.errors || []).join('\n');
    }
    async function refresh() {
      refreshBtn.disabled = true;
      statusEl.textContent = 'Refreshing...';
      try {
        const res = await fetch(`api/snapshot?t=${Date.now()}`, { cache: 'no-store' });
        const data = await res.json();
        render(data);
        statusEl.textContent = 'Live data loaded.';
      } catch (err) {
        statusEl.textContent = `Refresh failed: ${err}`;
      } finally {
        refreshBtn.disabled = false;
      }
    }
    async function checkAlert() {
      checkBtn.disabled = true;
      statusEl.textContent = 'Checking alert rule...';
      try {
        const res = await fetch(`api/check?t=${Date.now()}`, { method: 'POST', cache: 'no-store' });
        const data = await res.json();
        statusEl.textContent = `Alert check finished in ${data.elapsedMs} ms${data.dryRun ? ' (dry-run)' : ''}.`;
        await refresh();
      } catch (err) {
        statusEl.textContent = `Alert check failed: ${err}`;
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
                symbols = _symbols_from_query(parsed.query)
            except ValueError:
                self._send_bad_request()
                return
            try:
                market = getattr(self.server, "market", None)
                self._send_json(collect_snapshot(symbols, market=market))
            except (Exception, SystemExit) as exc:
                self._send_internal_error(exc)
            return
        if parsed.path == "/api/competition":
            try:
                service = getattr(self.server, "competition_service", None) or competition_service()
                self._send_json(service.collect(_symbols_from_env()))
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
