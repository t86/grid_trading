from __future__ import annotations

import argparse
import json
import math
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from .data import cache_file_path, load_or_fetch_candles
from .optimize import min_step_ratio_for_cost, optimize_grid_count

HTML_PAGE = """<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>网格策略测算器</title>
  <style>
    :root {
      --bg: #f8f7f4;
      --panel: #ffffff;
      --text: #171717;
      --muted: #6a6a6a;
      --line: #e5e1d8;
      --brand: #0f766e;
      --brand-soft: #e6f6f4;
      --danger: #b42318;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: "Avenir Next", "PingFang SC", "Hiragino Sans GB", "Microsoft YaHei", sans-serif;
      background: radial-gradient(circle at 10% 0%, #fffef8 0%, var(--bg) 45%, #f1efe8 100%);
      color: var(--text);
    }
    .wrap {
      max-width: 1200px;
      margin: 24px auto 48px;
      padding: 0 16px;
      display: grid;
      gap: 16px;
    }
    .card {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 14px;
      padding: 16px;
      box-shadow: 0 8px 30px rgba(13, 18, 30, 0.05);
    }
    .header h1 {
      margin: 0 0 6px;
      font-size: 26px;
      letter-spacing: 0.02em;
    }
    .header p {
      margin: 0;
      color: var(--muted);
      font-size: 14px;
    }
    form.grid {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 12px;
    }
    .field {
      display: flex;
      flex-direction: column;
      gap: 6px;
    }
    .field label {
      color: var(--muted);
      font-size: 13px;
    }
    .field input, .field select {
      height: 38px;
      border-radius: 10px;
      border: 1px solid var(--line);
      padding: 0 10px;
      font-size: 14px;
      background: #fff;
    }
    .field input:focus, .field select:focus {
      outline: none;
      border-color: var(--brand);
      box-shadow: 0 0 0 3px rgba(15, 118, 110, 0.12);
    }
    .actions {
      grid-column: 1 / -1;
      display: flex;
      align-items: center;
      gap: 10px;
      margin-top: 4px;
    }
    button {
      height: 40px;
      border-radius: 10px;
      border: none;
      background: var(--brand);
      color: #fff;
      font-size: 14px;
      font-weight: 600;
      padding: 0 14px;
      cursor: pointer;
    }
    button:disabled {
      cursor: not-allowed;
      opacity: 0.7;
    }
    .hint {
      color: var(--muted);
      font-size: 13px;
    }
    .msg {
      font-size: 13px;
      color: var(--muted);
    }
    .msg.error {
      color: var(--danger);
      font-weight: 600;
    }
    .summary {
      display: grid;
      grid-template-columns: repeat(5, minmax(0, 1fr));
      gap: 10px;
    }
    .kpi {
      border: 1px solid var(--line);
      border-radius: 10px;
      padding: 10px;
      background: var(--brand-soft);
    }
    .kpi .k {
      font-size: 12px;
      color: var(--muted);
      margin-bottom: 6px;
    }
    .kpi .v {
      font-size: 18px;
      font-weight: 700;
      color: #0c4b46;
    }
    .table-wrap {
      overflow: auto;
      border: 1px solid var(--line);
      border-radius: 10px;
    }
    table {
      width: 100%;
      border-collapse: collapse;
      font-size: 13px;
      min-width: 740px;
    }
    th, td {
      padding: 8px 10px;
      border-bottom: 1px solid var(--line);
      text-align: right;
      white-space: nowrap;
    }
    .candidate-row {
      cursor: pointer;
    }
    .candidate-row:hover {
      background: #f7f7f2;
    }
    .candidate-row.active {
      background: #e9f6f4;
    }
    th:first-child, td:first-child { text-align: left; }
    thead th {
      background: #faf8f2;
      font-weight: 700;
      color: #3f3f3f;
      position: sticky;
      top: 0;
      z-index: 1;
    }
    @media (max-width: 980px) {
      form.grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .summary { grid-template-columns: repeat(2, minmax(0, 1fr)); }
    }
    @media (max-width: 600px) {
      form.grid { grid-template-columns: 1fr; }
      .summary { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <div class="wrap">
    <section class="card header">
      <h1>BTC / ETH 合约网格策略测算器</h1>
      <p>等差做多网格 · 双边手续费万二（0.02%） · 支持 equal / linear 分配优化</p>
    </section>

    <section class="card">
      <form id="form" class="grid">
        <div class="field">
          <label>交易对</label>
          <select id="symbol">
            <option value="BTCUSDT">BTCUSDT</option>
            <option value="ETHUSDT">ETHUSDT</option>
          </select>
        </div>
        <div class="field">
          <label>最低价 min_price</label>
          <input id="min_price" type="number" step="0.0001" value="50000" />
        </div>
        <div class="field">
          <label>最高价 max_price</label>
          <input id="max_price" type="number" step="0.0001" value="130000" />
        </div>
        <div class="field">
          <label>到最低价总买入量（名义）</label>
          <input id="total_buy_notional" type="number" step="0.0001" value="10000" />
        </div>

        <div class="field">
          <label>回看天数</label>
          <input id="lookback_days" type="number" step="1" value="365" />
        </div>
        <div class="field">
          <label>K线周期</label>
          <select id="interval">
            <option value="1h">1h</option>
            <option value="4h">4h</option>
            <option value="1m">1m</option>
          </select>
        </div>
        <div class="field">
          <label>N最小值</label>
          <input id="n_min" type="number" step="1" value="5" />
        </div>
        <div class="field">
          <label>N最大值</label>
          <input id="n_max" type="number" step="1" value="200" />
        </div>

        <div class="field">
          <label>手续费（单边）</label>
          <input id="fee_rate" type="number" step="0.0001" value="0.0002" />
        </div>
        <div class="field">
          <label>滑点（单边）</label>
          <input id="slippage" type="number" step="0.0001" value="0" />
        </div>
        <div class="field">
          <label>分配模式（逗号）</label>
          <input id="allocation_modes" type="text" value="equal,linear" />
        </div>
        <div class="field">
          <label>目标函数</label>
          <select id="objective">
            <option value="calmar">calmar（收益/回撤）</option>
            <option value="net_profit">net_profit（净收益）</option>
            <option value="total_return">total_return（总收益率）</option>
            <option value="annualized_return">annualized_return（年化）</option>
          </select>
        </div>
        <div class="field">
          <label>最小成交数</label>
          <input id="min_trade_count" type="number" step="1" value="0" />
        </div>
        <div class="field">
          <label>最小平均资金占用(0-1)</label>
          <input id="min_avg_capital_usage" type="number" step="0.01" value="0" />
        </div>
        <div class="field">
          <label>Top候选数量</label>
          <input id="top_k" type="number" step="1" value="5" />
        </div>

        <div class="actions">
          <button id="run_btn" type="submit">开始测算</button>
          <button id="csv_btn" type="button" disabled>下载当前买入计划 CSV</button>
          <span id="status" class="msg">等待输入参数。</span>
        </div>
      </form>
      <p class="hint">提示：若数据缓存已存在会更快；勾选分钟级会明显增加计算时间。</p>
    </section>

    <section class="card">
      <h3>当前方案（可在Top候选点击切换）</h3>
      <div id="summary" class="summary"></div>
    </section>

    <section class="card">
      <h3>Top 候选（点击行切换到该方案）</h3>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>N</th>
              <th>Mode</th>
              <th>Score</th>
              <th>净收益</th>
              <th>总收益率</th>
              <th>年化</th>
              <th>最大回撤</th>
              <th>成交数</th>
              <th>手续费</th>
            </tr>
          </thead>
          <tbody id="top_tbody"></tbody>
        </table>
      </div>
    </section>

    <section class="card">
      <h3>每格买入计划</h3>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>格子</th>
              <th>买入价</th>
              <th>卖出价</th>
              <th>买入名义</th>
              <th>买入数量</th>
            </tr>
          </thead>
          <tbody id="plan_tbody"></tbody>
        </table>
      </div>
    </section>
  </div>

  <script>
    const form = document.getElementById("form");
    const statusEl = document.getElementById("status");
    const runBtn = document.getElementById("run_btn");
    const csvBtn = document.getElementById("csv_btn");
    const summaryEl = document.getElementById("summary");
    const topBody = document.getElementById("top_tbody");
    const planBody = document.getElementById("plan_tbody");

    let latestPlanRows = [];
    let latestTopCandidates = [];
    let latestCandleCount = 0;
    let selectedTopIndex = 0;

    function fmtNum(v, digits = 4) {
      if (v === null || v === undefined || Number.isNaN(v)) return "-";
      return Number(v).toLocaleString(undefined, { maximumFractionDigits: digits, minimumFractionDigits: digits });
    }

    function fmtPct(v) {
      if (v === null || v === undefined || Number.isNaN(v)) return "-";
      return `${(Number(v) * 100).toFixed(2)}%`;
    }

    function setStatus(text, isError = false) {
      statusEl.textContent = text;
      statusEl.className = isError ? "msg error" : "msg";
    }

    function readForm() {
      return {
        symbol: document.getElementById("symbol").value.trim(),
        min_price: Number(document.getElementById("min_price").value),
        max_price: Number(document.getElementById("max_price").value),
        total_buy_notional: Number(document.getElementById("total_buy_notional").value),
        lookback_days: Number(document.getElementById("lookback_days").value),
        interval: document.getElementById("interval").value.trim(),
        n_min: Number(document.getElementById("n_min").value),
        n_max: Number(document.getElementById("n_max").value),
        fee_rate: Number(document.getElementById("fee_rate").value),
        slippage: Number(document.getElementById("slippage").value),
        allocation_modes: document.getElementById("allocation_modes").value.trim(),
        objective: document.getElementById("objective").value.trim(),
        min_trade_count: Number(document.getElementById("min_trade_count").value),
        min_avg_capital_usage: Number(document.getElementById("min_avg_capital_usage").value),
        top_k: Number(document.getElementById("top_k").value)
      };
    }

    function renderSummary(best, candleCount) {
      const items = [
        ["当前 N", best.n],
        ["当前分配模式", best.allocation_mode],
        ["净收益", fmtNum(best.net_profit)],
        ["总收益率", fmtPct(best.total_return)],
        ["最大回撤", fmtPct(best.max_drawdown)],
        ["年化收益", fmtPct(best.annualized_return)],
        ["成交数", best.trade_count],
        ["手续费", fmtNum(best.total_fees)],
        ["平均资金占用", fmtPct(best.avg_capital_usage)],
        ["K线数量", candleCount]
      ];
      summaryEl.innerHTML = items.map(([k, v]) => (
        `<div class="kpi"><div class="k">${k}</div><div class="v">${v}</div></div>`
      )).join("");
    }

    function renderTop(rows, activeIndex) {
      topBody.innerHTML = rows.map((x, idx) => `
        <tr class="candidate-row ${idx === activeIndex ? "active" : ""}" data-idx="${idx}">
          <td>${x.n}</td>
          <td>${x.allocation_mode}</td>
          <td>${fmtNum(x.score, 4)}</td>
          <td>${fmtNum(x.net_profit)}</td>
          <td>${fmtPct(x.total_return)}</td>
          <td>${fmtPct(x.annualized_return)}</td>
          <td>${fmtPct(x.max_drawdown)}</td>
          <td>${x.trade_count}</td>
          <td>${fmtNum(x.total_fees)}</td>
        </tr>
      `).join("");
    }

    function renderPlan(rows) {
      latestPlanRows = rows;
      csvBtn.disabled = rows.length === 0;
      planBody.innerHTML = rows.map((x) => `
        <tr>
          <td>${x.idx}</td>
          <td>${fmtNum(x.buy_price)}</td>
          <td>${fmtNum(x.sell_price)}</td>
          <td>${fmtNum(x.buy_notional)}</td>
          <td>${fmtNum(x.qty, 6)}</td>
        </tr>
      `).join("");
    }

    function downloadPlanCsv() {
      if (!latestPlanRows.length) return;
      const lines = ["idx,buy_price,sell_price,buy_notional,qty"];
      for (const row of latestPlanRows) {
        lines.push([row.idx, row.buy_price, row.sell_price, row.buy_notional, row.qty].join(","));
      }
      const blob = new Blob([lines.join("\\n")], { type: "text/csv;charset=utf-8;" });
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = "grid_plan.csv";
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      URL.revokeObjectURL(url);
    }

    csvBtn.addEventListener("click", downloadPlanCsv);

    function selectTopCandidate(index) {
      if (!latestTopCandidates.length) return;
      if (index < 0 || index >= latestTopCandidates.length) return;
      selectedTopIndex = index;
      const selected = latestTopCandidates[index];
      renderSummary(selected, latestCandleCount);
      renderPlan(selected.plan || []);
      renderTop(latestTopCandidates, selectedTopIndex);
      setStatus(`已切换候选：N=${selected.n}，模式=${selected.allocation_mode}。`);
    }

    topBody.addEventListener("click", (e) => {
      const row = e.target.closest("tr.candidate-row");
      if (!row) return;
      const idx = Number(row.dataset.idx);
      if (Number.isNaN(idx)) return;
      selectTopCandidate(idx);
    });

    form.addEventListener("submit", async (e) => {
      e.preventDefault();
      const payload = readForm();
      runBtn.disabled = true;
      csvBtn.disabled = true;
      setStatus("正在测算，请稍等...");
      try {
        const resp = await fetch("/api/optimize", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload)
        });
        const data = await resp.json();
        if (!resp.ok || !data.ok) {
          throw new Error(data.error || `请求失败(${resp.status})`);
        }
        if (!data.best) {
          setStatus("未找到可用方案，请调整区间或N范围。", true);
          summaryEl.innerHTML = "";
          topBody.innerHTML = "";
          planBody.innerHTML = "";
          latestTopCandidates = [];
          latestPlanRows = [];
          return;
        }
        latestTopCandidates = data.top || [];
        latestCandleCount = data.data.candles;
        if (!latestTopCandidates.length) {
          latestTopCandidates = [data.best];
        }
        selectTopCandidate(0);
        const cacheLabel = data.data.cache_file ? ` 缓存：${data.data.cache_file}` : "";
        setStatus(`完成：默认显示最优候选。${cacheLabel}`);
      } catch (err) {
        setStatus(`测算失败：${err.message}`, true);
      } finally {
        runBtn.disabled = false;
      }
    });
  </script>
</body>
</html>
"""


def _safe_float(value: Any, name: str) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a number") from exc
    return number


def _safe_int(value: Any, name: str) -> int:
    try:
        number = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be an integer") from exc
    return number


def _safe_metric(value: float) -> float | None:
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def _normalize_payload(payload: dict[str, Any]) -> dict[str, Any]:
    symbol = str(payload.get("symbol", "BTCUSDT")).upper().strip()
    min_price = _safe_float(payload.get("min_price"), "min_price")
    max_price = _safe_float(payload.get("max_price"), "max_price")
    total_buy_notional = _safe_float(payload.get("total_buy_notional"), "total_buy_notional")
    lookback_days = _safe_int(payload.get("lookback_days", 365), "lookback_days")
    interval = str(payload.get("interval", "1h")).strip()
    n_min = _safe_int(payload.get("n_min", 5), "n_min")
    n_max = _safe_int(payload.get("n_max", 200), "n_max")
    fee_rate = _safe_float(payload.get("fee_rate", 0.0002), "fee_rate")
    slippage = _safe_float(payload.get("slippage", 0.0), "slippage")
    funding_buffer = _safe_float(payload.get("funding_buffer", 0.0), "funding_buffer")
    top_k = _safe_int(payload.get("top_k", 5), "top_k")
    refresh = bool(payload.get("refresh", False))

    raw_modes = payload.get("allocation_modes", "equal,linear")
    if isinstance(raw_modes, str):
        allocation_modes = [x.strip().lower() for x in raw_modes.split(",") if x.strip()]
    elif isinstance(raw_modes, list):
        allocation_modes = [str(x).strip().lower() for x in raw_modes if str(x).strip()]
    else:
        raise ValueError("allocation_modes must be string or list")

    objective = str(payload.get("objective", "calmar")).strip().lower()
    if objective not in {"calmar", "net_profit", "total_return", "annualized_return"}:
        raise ValueError("unsupported objective")
    min_trade_count = _safe_int(payload.get("min_trade_count", 0), "min_trade_count")
    min_avg_capital_usage = _safe_float(
        payload.get("min_avg_capital_usage", 0.0), "min_avg_capital_usage"
    )

    if min_price >= max_price:
        raise ValueError("min_price must be less than max_price")
    if total_buy_notional <= 0:
        raise ValueError("total_buy_notional must be > 0")
    if lookback_days <= 0:
        raise ValueError("lookback_days must be > 0")
    if n_min <= 0 or n_max <= 0 or n_min > n_max:
        raise ValueError("invalid n range")
    if fee_rate < 0 or slippage < 0 or funding_buffer < 0:
        raise ValueError("fee/slippage/funding_buffer must be >= 0")
    if not allocation_modes:
        raise ValueError("allocation_modes cannot be empty")
    if top_k <= 0:
        raise ValueError("top_k must be > 0")
    if min_trade_count < 0:
        raise ValueError("min_trade_count must be >= 0")
    if min_avg_capital_usage < 0 or min_avg_capital_usage > 1:
        raise ValueError("min_avg_capital_usage must be in [0,1]")

    return {
        "symbol": symbol,
        "min_price": min_price,
        "max_price": max_price,
        "total_buy_notional": total_buy_notional,
        "lookback_days": lookback_days,
        "interval": interval,
        "n_min": n_min,
        "n_max": n_max,
        "fee_rate": fee_rate,
        "slippage": slippage,
        "funding_buffer": funding_buffer,
        "allocation_modes": allocation_modes,
        "objective": objective,
        "min_trade_count": min_trade_count,
        "min_avg_capital_usage": min_avg_capital_usage,
        "top_k": top_k,
        "refresh": refresh,
    }


def _run_optimizer(params: dict[str, Any]) -> dict[str, Any]:
    cache_path = str(cache_file_path(params["symbol"], params["interval"], cache_dir="data"))
    candles = load_or_fetch_candles(
        symbol=params["symbol"],
        interval=params["interval"],
        lookback_days=params["lookback_days"],
        cache_dir="data",
        refresh=params["refresh"],
    )

    optimization = optimize_grid_count(
        candles=candles,
        min_price=params["min_price"],
        max_price=params["max_price"],
        total_buy_notional=params["total_buy_notional"],
        n_min=params["n_min"],
        n_max=params["n_max"],
        fee_rate=params["fee_rate"],
        slippage=params["slippage"],
        funding_buffer=params["funding_buffer"],
        allocation_modes=params["allocation_modes"],
        objective=params["objective"],
        min_trade_count=params["min_trade_count"],
        min_avg_capital_usage=params["min_avg_capital_usage"],
        top_k=params["top_k"],
    )

    if optimization.best is None:
        return {
            "ok": True,
            "best": None,
            "top": [],
            "plan": [],
            "data": {"candles": len(candles), "cache_file": cache_path},
            "search": {
                "tested": optimization.tested,
                "skipped_by_cost": optimization.skipped_by_cost,
                "objective": params["objective"],
                "min_trade_count": params["min_trade_count"],
                "min_avg_capital_usage": params["min_avg_capital_usage"],
                "min_step_ratio_for_cost": min_step_ratio_for_cost(
                    fee_rate=params["fee_rate"],
                    slippage=params["slippage"],
                    funding_buffer=params["funding_buffer"],
                ),
            },
        }

    def _candidate_payload(item) -> dict[str, Any]:
        plan_rows = []
        for idx in range(item.n):
            plan_rows.append(
                {
                    "idx": idx + 1,
                    "buy_price": item.grid_levels[idx],
                    "sell_price": item.grid_levels[idx + 1],
                    "buy_notional": item.per_grid_notionals[idx],
                    "qty": item.per_grid_qty[idx],
                }
            )
        return {
            "n": item.n,
            "allocation_mode": item.allocation_mode,
            "score": _safe_metric(item.score),
            "net_profit": item.net_profit,
            "total_return": item.total_return,
            "annualized_return": item.annualized_return,
            "max_drawdown": item.max_drawdown,
            "total_fees": item.total_fees,
            "trade_count": item.trade_count,
            "win_rate": item.win_rate,
            "avg_capital_usage": item.avg_capital_usage,
            "max_capital_usage": item.max_capital_usage,
            "plan": plan_rows,
        }

    best = _candidate_payload(optimization.best)
    top = [_candidate_payload(x) for x in optimization.top_results]

    return {
        "ok": True,
        "best": best,
        "top": top,
        "plan": best["plan"],
        "data": {"candles": len(candles), "cache_file": cache_path},
        "search": {
            "tested": optimization.tested,
            "skipped_by_cost": optimization.skipped_by_cost,
            "objective": params["objective"],
            "min_trade_count": params["min_trade_count"],
            "min_avg_capital_usage": params["min_avg_capital_usage"],
            "min_step_ratio_for_cost": min_step_ratio_for_cost(
                fee_rate=params["fee_rate"],
                slippage=params["slippage"],
                funding_buffer=params["funding_buffer"],
            ),
        },
    }


class _Handler(BaseHTTPRequestHandler):
    server_version = "grid-web/0.1"

    def _send_json(self, payload: dict[str, Any], status: int = 200) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_html(self, body: str, status: int = 200) -> None:
        data = body.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self) -> None:  # noqa: N802
        if self.path in {"/", "/index.html"}:
            self._send_html(HTML_PAGE, status=HTTPStatus.OK)
            return
        if self.path == "/api/health":
            self._send_json({"ok": True}, status=HTTPStatus.OK)
            return
        self._send_json({"ok": False, "error": "Not Found"}, status=HTTPStatus.NOT_FOUND)

    def do_HEAD(self) -> None:  # noqa: N802
        if self.path in {"/", "/index.html"}:
            body = HTML_PAGE.encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            return
        if self.path == "/api/health":
            body = json.dumps({"ok": True}, ensure_ascii=False).encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            return
        self.send_response(HTTPStatus.NOT_FOUND)
        self.end_headers()

    def do_POST(self) -> None:  # noqa: N802
        if self.path != "/api/optimize":
            self._send_json({"ok": False, "error": "Not Found"}, status=HTTPStatus.NOT_FOUND)
            return

        try:
            content_len = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            self._send_json({"ok": False, "error": "Invalid Content-Length"}, status=400)
            return
        if content_len <= 0 or content_len > 1024 * 1024:
            self._send_json({"ok": False, "error": "Invalid payload size"}, status=400)
            return

        raw = self.rfile.read(content_len)
        try:
            payload = json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError:
            self._send_json({"ok": False, "error": "Invalid JSON"}, status=400)
            return
        if not isinstance(payload, dict):
            self._send_json({"ok": False, "error": "JSON body must be object"}, status=400)
            return

        try:
            params = _normalize_payload(payload)
            result = _run_optimizer(params)
        except ValueError as exc:
            self._send_json({"ok": False, "error": str(exc)}, status=400)
            return
        except Exception as exc:  # pragma: no cover
            self._send_json({"ok": False, "error": f"{type(exc).__name__}: {exc}"}, status=500)
            return

        self._send_json(result, status=200)

    def log_message(self, fmt: str, *args: Any) -> None:
        return


def main() -> None:
    parser = argparse.ArgumentParser(description="Web UI for grid optimizer.")
    parser.add_argument("--host", type=str, default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8787)
    args = parser.parse_args()

    server = ThreadingHTTPServer((args.host, args.port), _Handler)
    print(f"Grid Web UI running at http://{args.host}:{args.port}")
    print("Press Ctrl+C to stop.")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
