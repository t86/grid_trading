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
    .competition-table { min-width: 1180px; }
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
      <div class="table-wrap competition-wrap">
        <table class="competition-table">
          <thead>
            <tr>
              <th>币种</th>
              <th>轮次 / Day / 倒计时</th>
              <th>当前倍速</th>
              <th>加权总量</th>
              <th>获奖人数</th>
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
    const statusEl = document.getElementById('status');
    const alertStatusEl = document.getElementById('alertStatus');
    const errorsEl = document.getElementById('errors');
    const competitionErrorsEl = document.getElementById('competitionErrors');
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
      if (row.status !== 'active') {
        const unavailable = row.status === 'rule_unavailable' || row.status === 'volume_unavailable';
        return `<div class="cell-value"><span class="status-chip${unavailable ? ' unavailable' : ''}">${escapeHtml(state)}</span></div>`;
      }
      return `<div class="cell-value"><div class="metric-primary">第 ${escapeHtml(row.round)} 轮 · Day ${escapeHtml(row.day)}</div>
        <div class="metric-secondary"><span class="status-chip active">进行中</span> · ${escapeHtml(formatCountdown(row.roundEndUtc))}</div></div>`;
    }

    function renderCompetitionRow(row) {
      const multiplier = finiteNumber(row.currentMultiplier);
      return `<tr>
        <td data-label="币种"><div class="cell-value token-cell"><div class="sym">${escapeHtml(row.symbol)}</div><div class="name">${escapeHtml(row.name)}</div></div></td>
        <td data-label="轮次 / Day">${roundContent(row)}</td>
        <td data-label="当前倍速">${multiplier === null ? '—' : `<span class="${multipleClass(multiplier)}">${escapeHtml(multiplier.toFixed(1))}x</span>`}</td>
        <td data-label="加权总量" class="metric-primary">${escapeHtml(formatU(row.weightedVolume))}</td>
        <td data-label="获奖人数">${escapeHtml(formatInteger(row.winnerCount))}</td>
        <td data-label="平均量">${escapeHtml(formatU(row.averageVolume))}</td>
        <td data-label="观察线 0.4" class="threshold-watch">${escapeHtml(formatU(row.watchThreshold))}</td>
        <td data-label="参考线 0.6" class="threshold-reference">${escapeHtml(formatU(row.referenceThreshold))}</td>
        <td data-label="安全线 1.0" class="threshold-safe">${escapeHtml(formatU(row.safeThreshold))}</td>
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

    function renderCompetition(data) {
      const payload = validatePayload(data, '交易赛');
      const rows = payload.rows;
      document.getElementById('competitionGenerated').textContent = `交易赛更新 ${payload.generatedAtUtc || '时间未知'}`;
      competitionRowsEl.innerHTML = rows.map(renderCompetitionRow).join('');
      competitionErrorsEl.textContent = (Array.isArray(payload.errors) ? payload.errors : []).map(String).join('\n');
    }

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
        const [marketResult, competitionResult] = await Promise.allSettled([
          fetchJson('api/snapshot'),
          fetchJson('api/competition'),
        ]);
        if (generation !== refreshGeneration) return;
        if (marketResult.status === 'fulfilled') {
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
