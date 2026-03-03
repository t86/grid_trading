from __future__ import annotations

import argparse
import json
import math
import threading
import time
import uuid
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from collections.abc import Callable
from statistics import mean
from typing import Any

from .backtest import run_backtest, supported_allocation_modes
from .data import cache_file_path, list_futures_symbols, load_or_fetch_candles
from .optimize import min_step_ratio_for_cost, objective_value, optimize_grid_count

JOBS: dict[str, dict[str, Any]] = {}
JOBS_LOCK = threading.Lock()
SYMBOLS_LOCK = threading.Lock()
SYMBOLS_TTL_SECONDS = 900
DEFAULT_SYMBOLS = [
    "BTCUSDT",
    "ETHUSDT",
    "SOLUSDT",
    "BNBUSDT",
    "XRPUSDT",
    "DOGEUSDT",
    "ADAUSDT",
    "LTCUSDT",
    "LINKUSDT",
    "TRXUSDT",
    "AVAXUSDT",
    "DOTUSDT",
]
SYMBOLS_CACHE: dict[str, Any] = {
    "symbols": list(DEFAULT_SYMBOLS),
    "updated_at": 0.0,
}

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
    .is-hidden {
      display: none;
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
    .mode-group {
      border: 1px solid var(--line);
      border-radius: 10px;
      padding: 8px 10px;
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 6px 8px;
      background: #fff;
      min-height: 38px;
      align-content: center;
    }
    .mode-item {
      display: flex;
      align-items: center;
      gap: 6px;
      font-size: 12px;
      color: #303030;
      white-space: nowrap;
    }
    .mode-item input {
      width: 14px;
      height: 14px;
      margin: 0;
      padding: 0;
    }
    .mode-desc {
      color: var(--muted);
      font-size: 12px;
      line-height: 1.5;
      margin-top: 6px;
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
    .formula {
      margin: 12px 0 0;
      color: #2f463f;
      font-size: 13px;
      line-height: 1.5;
    }
    .msg {
      font-size: 13px;
      color: var(--muted);
    }
    .msg.error {
      color: var(--danger);
      font-weight: 600;
    }
    .progress-box {
      grid-column: 1 / -1;
      display: grid;
      gap: 6px;
    }
    .progress-track {
      width: 100%;
      height: 10px;
      background: #ebe8de;
      border-radius: 999px;
      overflow: hidden;
      border: 1px solid var(--line);
    }
    .progress-fill {
      width: 0%;
      height: 100%;
      background: linear-gradient(90deg, #0f766e 0%, #1d9a90 100%);
      transition: width 0.25s ease;
    }
    .progress-text {
      color: var(--muted);
      font-size: 12px;
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
    .layer-grid {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 12px;
      margin-bottom: 10px;
    }
    .layer-actions {
      grid-column: 1 / -1;
      display: flex;
      align-items: center;
      gap: 10px;
      margin-top: 2px;
    }
    .delta-better {
      color: #0b6b44;
      font-weight: 700;
    }
    .delta-worse {
      color: #b42318;
      font-weight: 700;
    }
    .delta-neutral {
      color: var(--muted);
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
      .layer-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
    }
    @media (max-width: 600px) {
      form.grid { grid-template-columns: 1fr; }
      .summary { grid-template-columns: 1fr; }
      .layer-grid { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <div class="wrap">
    <section class="card header">
      <h1>币安合约交易赛网格策略回测器</h1>
      <p>目标：在指定时间区间内尽可能提高成交量，同时把回撤和损失控制到更低。</p>
    </section>

    <section class="card">
      <form id="form" class="grid">
        <div class="field">
          <label>交易对</label>
          <select id="symbol">
            <option value="BTCUSDT">BTCUSDT</option>
            <option value="ETHUSDT">ETHUSDT</option>
            <option value="SOLUSDT">SOLUSDT</option>
            <option value="BNBUSDT">BNBUSDT</option>
            <option value="XRPUSDT">XRPUSDT</option>
            <option value="DOGEUSDT">DOGEUSDT</option>
            <option value="ADAUSDT">ADAUSDT</option>
            <option value="LTCUSDT">LTCUSDT</option>
            <option value="LINKUSDT">LINKUSDT</option>
            <option value="TRXUSDT">TRXUSDT</option>
            <option value="AVAXUSDT">AVAXUSDT</option>
            <option value="DOTUSDT">DOTUSDT</option>
          </select>
        </div>
        <div class="field">
          <label>测算模式</label>
          <select id="calc_mode">
            <option value="optimize">优化模式（自动找最优N）</option>
            <option value="fixed">固定参数模式（你指定N和每格金额）</option>
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
        <div class="field optimize-only">
          <label>最大投入金额（上限）</label>
          <input id="total_buy_notional" type="number" step="0.0001" value="10000" />
        </div>
        <div class="field optimize-only">
          <label>目标交易量（成交额）</label>
          <input id="target_trade_volume" type="number" step="0.0001" value="500000" />
        </div>
        <div class="field fixed-only">
          <label>固定格子数 N</label>
          <input id="fixed_n" type="number" step="1" value="20" />
        </div>
        <div class="field fixed-only">
          <label>每格买入金额</label>
          <input id="fixed_per_grid_notional" type="number" step="0.0001" value="500" />
        </div>

        <div class="field">
          <label>开始时间</label>
          <input id="start_time" type="datetime-local" />
        </div>
        <div class="field">
          <label>结束时间</label>
          <input id="end_time" type="datetime-local" />
        </div>
        <div class="field">
          <label>回看天数（未填时间时生效）</label>
          <input id="lookback_days" type="number" step="1" value="7" />
        </div>
        <div class="field">
          <label>K线周期</label>
          <select id="interval">
            <option value="1h">1h</option>
            <option value="4h">4h</option>
            <option value="1m">1m</option>
            <option value="1s">1s</option>
          </select>
        </div>
        <div class="field optimize-only">
          <label>N最小值</label>
          <input id="n_min" type="number" step="1" value="5" />
        </div>
        <div class="field optimize-only">
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
        <div class="field optimize-only">
          <label>分配模式（多选）</label>
          <div id="allocation_modes" class="mode-group">
            <label class="mode-item"><input type="checkbox" name="allocation_mode" value="equal" checked />equal</label>
            <label class="mode-item"><input type="checkbox" name="allocation_mode" value="equal_qty" />equal_qty</label>
            <label class="mode-item"><input type="checkbox" name="allocation_mode" value="linear" checked />linear</label>
            <label class="mode-item"><input type="checkbox" name="allocation_mode" value="linear_reverse" />linear_reverse</label>
            <label class="mode-item"><input type="checkbox" name="allocation_mode" value="quadratic" />quadratic</label>
            <label class="mode-item"><input type="checkbox" name="allocation_mode" value="quadratic_reverse" />quadratic_reverse</label>
            <label class="mode-item"><input type="checkbox" name="allocation_mode" value="geometric" />geometric</label>
            <label class="mode-item"><input type="checkbox" name="allocation_mode" value="geometric_reverse" />geometric_reverse</label>
            <label class="mode-item"><input type="checkbox" name="allocation_mode" value="center_heavy" />center_heavy</label>
            <label class="mode-item"><input type="checkbox" name="allocation_mode" value="edge_heavy" />edge_heavy</label>
          </div>
          <div class="mode-desc">
            equal: 每格等额；equal_qty: 每格等数量（币安网格更贴近）；linear: 越低价越多；linear_reverse: 越高价越多；
            quadratic/geometric: 低价加仓更激进；对应 reverse 为高价更激进；
            center_heavy: 中间格子更多；edge_heavy: 两端格子更多。
          </div>
        </div>
        <div class="field">
          <label>目标函数</label>
          <select id="objective">
            <option value="competition_volume">competition_volume（交易赛推荐）</option>
            <option value="calmar">calmar（收益/回撤）</option>
            <option value="net_profit">net_profit（净收益）</option>
            <option value="total_return">total_return（总收益率）</option>
            <option value="annualized_return">annualized_return（年化）</option>
          </select>
        </div>
        <div class="field optimize-only">
          <label>最小成交数</label>
          <input id="min_trade_count" type="number" step="1" value="0" />
        </div>
        <div class="field optimize-only">
          <label>最小平均资金占用(0-1)</label>
          <input id="min_avg_capital_usage" type="number" step="0.01" value="0" />
        </div>
        <div class="field optimize-only">
          <label>Top候选数量</label>
          <input id="top_k" type="number" step="1" value="5" />
        </div>

        <div class="actions">
          <button id="run_btn" type="submit">开始测算</button>
          <button id="suggest_btn" type="button" class="optimize-only">智能建议 min/max</button>
          <button id="csv_btn" type="button" disabled>下载当前买入计划 CSV</button>
          <span id="status" class="msg">等待输入参数。</span>
        </div>
        <div class="actions optimize-only">
          <span id="suggest_status" class="msg">可先点击“智能建议 min/max”，再进行正式测算。</span>
        </div>
        <div id="progress_box" class="progress-box">
          <div class="progress-track">
            <div id="progress_fill" class="progress-fill"></div>
          </div>
          <div id="progress_text" class="progress-text">进度 0.0% · ETA --</div>
        </div>
      </form>
      <p class="hint">提示：优先使用“交易赛推荐”目标函数；支持 `1s` 级别数据（由成交明细聚合并本地缓存）。</p>
    </section>

    <section class="card optimize-only">
      <h3>智能区间建议（交易赛）</h3>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>#</th>
              <th>建议 min_price</th>
              <th>建议 max_price</th>
              <th>推荐 N</th>
              <th>推荐模式</th>
              <th>单格价差</th>
              <th>每格买入额(均/最小/最大)</th>
              <th>成交额</th>
              <th>目标达成率</th>
              <th>净收益</th>
              <th>最大回撤</th>
              <th>成交数</th>
              <th>说明</th>
              <th>操作</th>
            </tr>
          </thead>
          <tbody id="suggest_tbody"></tbody>
        </table>
      </div>
    </section>

    <section class="card">
      <h3>当前方案（可在Top候选点击切换）</h3>
      <div id="summary" class="summary"></div>
      <p id="pnl_formula" class="formula"></p>
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
              <th>成交额</th>
              <th>目标达成率</th>
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

    <section class="card">
      <h3>一键分层参数生成器（多机器人近似线性加仓）</h3>
      <div class="layer-grid">
        <div class="field">
          <label>分层数量</label>
          <input id="layer_count" type="number" step="1" value="5" min="2" max="20" />
        </div>
        <div class="field">
          <label>N建议模式</label>
          <select id="layer_n_mode">
            <option value="fit_selected_plan">拟合当前方案（最接近最佳策略）</option>
            <option value="fit_nested_cover_qty">递减覆盖（币安数量模式）</option>
            <option value="budget_equal_step">总N对齐（按区间宽度，推荐）</option>
            <option value="budget_by_weight">总N对齐（按资金权重）</option>
            <option value="target_step">独立步长模式（每层单独算N）</option>
          </select>
        </div>
        <div class="field layer-manual-field">
          <label>低价层加权方式</label>
          <select id="layer_weight_mode">
            <option value="linear">linear（默认）</option>
            <option value="quadratic">quadratic（更激进）</option>
            <option value="geometric">geometric（更陡峭）</option>
            <option value="equal">equal（均匀）</option>
          </select>
        </div>
        <div class="field layer-budget-field">
          <label>总格子预算（所有层合计）</label>
          <input id="layer_total_n_budget" type="number" step="1" value="20" min="2" max="500" />
        </div>
        <div class="field layer-target-step-field">
          <label>目标单格价差(%)</label>
          <input id="layer_target_step_pct" type="number" step="0.1" value="0.5" min="0.1" />
        </div>
        <div class="field layer-target-step-field">
          <label>每层建议N上限</label>
          <input id="layer_n_cap" type="number" step="1" value="80" min="5" max="300" />
        </div>
        <div class="layer-actions">
          <button id="layer_btn" type="button">一键生成分层参数</button>
          <button id="layer_compare_btn" type="button" disabled>分层组合回测对比</button>
          <button id="layer_csv_btn" type="button" disabled>下载分层参数 CSV</button>
          <span id="layer_status" class="msg">使用当前输入参数点击生成。</span>
        </div>
      </div>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>层级(低->高)</th>
              <th>覆盖格子</th>
              <th>区间下限</th>
              <th>区间上限</th>
              <th>资金占比</th>
              <th>层总投入</th>
              <th>建议N</th>
              <th>单格价差(%)</th>
              <th>建议每格买入额</th>
              <th>建议每格买入数量</th>
              <th>建议</th>
            </tr>
          </thead>
          <tbody id="layer_tbody"></tbody>
        </table>
      </div>
      <p class="hint">说明：若交易所按“每格固定数量”下单，优先用“递减覆盖（币安数量模式）”；第一层覆盖全部格子，后续层逐步下调最高价以增强低价区买入。</p>
    </section>

    <section class="card">
      <h3>分层组合回测 vs 当前方案</h3>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>指标</th>
              <th>当前方案</th>
              <th>分层组合</th>
              <th>差值(分层-当前)</th>
            </tr>
          </thead>
          <tbody id="compare_tbody"></tbody>
        </table>
      </div>
      <p id="compare_status" class="hint">先生成分层参数，再点击“分层组合回测对比”。</p>
    </section>
  </div>

  <script>
    const form = document.getElementById("form");
    const statusEl = document.getElementById("status");
    const runBtn = document.getElementById("run_btn");
    const suggestBtn = document.getElementById("suggest_btn");
    const csvBtn = document.getElementById("csv_btn");
    const suggestStatusEl = document.getElementById("suggest_status");
    const suggestBody = document.getElementById("suggest_tbody");
    const summaryEl = document.getElementById("summary");
    const formulaEl = document.getElementById("pnl_formula");
    const topBody = document.getElementById("top_tbody");
    const planBody = document.getElementById("plan_tbody");
    const layerBody = document.getElementById("layer_tbody");
    const layerBtn = document.getElementById("layer_btn");
    const layerCompareBtn = document.getElementById("layer_compare_btn");
    const layerCsvBtn = document.getElementById("layer_csv_btn");
    const layerStatusEl = document.getElementById("layer_status");
    const compareBody = document.getElementById("compare_tbody");
    const compareStatusEl = document.getElementById("compare_status");
    const layerCountEl = document.getElementById("layer_count");
    const layerNModeEl = document.getElementById("layer_n_mode");
    const layerWeightModeEl = document.getElementById("layer_weight_mode");
    const layerTotalNEl = document.getElementById("layer_total_n_budget");
    const layerTargetStepPctEl = document.getElementById("layer_target_step_pct");
    const layerNCapEl = document.getElementById("layer_n_cap");
    const startTimeEl = document.getElementById("start_time");
    const endTimeEl = document.getElementById("end_time");
    const symbolEl = document.getElementById("symbol");
    const layerManualFields = Array.from(document.querySelectorAll(".layer-manual-field"));
    const layerBudgetFields = Array.from(document.querySelectorAll(".layer-budget-field"));
    const layerTargetStepFields = Array.from(document.querySelectorAll(".layer-target-step-field"));
    const calcModeEl = document.getElementById("calc_mode");
    const progressBoxEl = document.getElementById("progress_box");
    const progressFillEl = document.getElementById("progress_fill");
    const progressTextEl = document.getElementById("progress_text");
    const optimizeOnlyFields = Array.from(document.querySelectorAll(".optimize-only"));
    const fixedOnlyFields = Array.from(document.querySelectorAll(".fixed-only"));

    let latestPlanRows = [];
    let latestTopCandidates = [];
    let latestCandleCount = 0;
    let latestRangeSuggestions = [];
    let latestLayerRows = [];
    let latestComparison = null;
    let selectedTopIndex = 0;
    let currentPollToken = 0;

    function fmtNum(v, digits = 4) {
      if (v === null || v === undefined || Number.isNaN(v)) return "-";
      return Number(v).toLocaleString(undefined, { maximumFractionDigits: digits, minimumFractionDigits: digits });
    }

    function fmtPct(v) {
      if (v === null || v === undefined || Number.isNaN(v)) return "-";
      return `${(Number(v) * 100).toFixed(2)}%`;
    }

    function fmtDateTime(v) {
      if (!v) return "-";
      const d = new Date(v);
      if (Number.isNaN(d.getTime())) return String(v);
      return d.toLocaleString();
    }

    function setStatus(text, isError = false) {
      statusEl.textContent = text;
      statusEl.className = isError ? "msg error" : "msg";
    }

    function setSuggestStatus(text, isError = false) {
      suggestStatusEl.textContent = text;
      suggestStatusEl.className = isError ? "msg error" : "msg";
    }

    function setLayerStatus(text, isError = false) {
      layerStatusEl.textContent = text;
      layerStatusEl.className = isError ? "msg error" : "msg";
    }

    function setCompareStatus(text, isError = false) {
      compareStatusEl.textContent = text;
      compareStatusEl.className = isError ? "hint msg error" : "hint";
    }

    function signedNum(v, digits = 4) {
      if (v === null || v === undefined || Number.isNaN(v)) return "-";
      const n = Number(v);
      if (Math.abs(n) < 1e-12) return "0";
      const sign = n > 0 ? "+" : "";
      return `${sign}${n.toLocaleString(undefined, { maximumFractionDigits: digits, minimumFractionDigits: digits })}`;
    }

    function signedPct(v) {
      if (v === null || v === undefined || Number.isNaN(v)) return "-";
      const n = Number(v);
      if (Math.abs(n) < 1e-12) return "0.00%";
      const sign = n > 0 ? "+" : "";
      return `${sign}${(n * 100).toFixed(2)}%`;
    }

    function deltaClass(delta, preferLower = false) {
      if (delta === null || delta === undefined || Number.isNaN(delta)) return "delta-neutral";
      if (Math.abs(Number(delta)) < 1e-12) return "delta-neutral";
      const better = preferLower ? Number(delta) < 0 : Number(delta) > 0;
      return better ? "delta-better" : "delta-worse";
    }

    function clearComparison(message = "先生成分层参数，再点击“分层组合回测对比”。") {
      latestComparison = null;
      compareBody.innerHTML = "";
      setCompareStatus(message, false);
    }

    function formatEta(seconds) {
      if (seconds === null || seconds === undefined) return "--";
      const s = Number(seconds);
      if (Number.isNaN(s) || s < 0) return "--";
      const h = Math.floor(s / 3600);
      const m = Math.floor((s % 3600) / 60);
      const sec = Math.floor(s % 60);
      if (h > 0) return `${h}h ${m}m ${sec}s`;
      if (m > 0) return `${m}m ${sec}s`;
      return `${sec}s`;
    }

    function setProgress(progress, etaSeconds, message = "") {
      const pct = Math.max(0, Math.min(100, Number(progress || 0) * 100));
      progressFillEl.style.width = `${pct.toFixed(2)}%`;
      const etaText = formatEta(etaSeconds);
      progressTextEl.textContent = `进度 ${pct.toFixed(1)}% · ETA ${etaText}${message ? ` · ${message}` : ""}`;
    }

    function setGroupVisible(nodes, visible) {
      for (const node of nodes) {
        node.classList.toggle("is-hidden", !visible);
        for (const input of node.querySelectorAll("input,select")) {
          input.disabled = !visible;
        }
      }
    }

    function applyCalcModeUI() {
      const mode = calcModeEl.value;
      const isOptimize = mode === "optimize";
      setGroupVisible(optimizeOnlyFields, isOptimize);
      setGroupVisible(fixedOnlyFields, !isOptimize);
    }

    function toLocalDatetimeValue(dateObj) {
      const offsetMs = dateObj.getTimezoneOffset() * 60 * 1000;
      const local = new Date(dateObj.getTime() - offsetMs);
      return local.toISOString().slice(0, 16);
    }

    function isoFromInput(inputEl) {
      const raw = String(inputEl && inputEl.value ? inputEl.value : "").trim();
      if (!raw) return "";
      const parsed = new Date(raw);
      if (Number.isNaN(parsed.getTime())) return "";
      return parsed.toISOString();
    }

    function initDateRangeDefaults() {
      if (!startTimeEl || !endTimeEl) return;
      if (String(startTimeEl.value || "").trim() && String(endTimeEl.value || "").trim()) return;
      const end = new Date();
      const start = new Date(end.getTime() - 3 * 24 * 3600 * 1000);
      startTimeEl.value = toLocalDatetimeValue(start);
      endTimeEl.value = toLocalDatetimeValue(end);
    }

    function applySymbolOptions(symbols) {
      if (!symbolEl || !Array.isArray(symbols) || !symbols.length) return;
      const previous = String(symbolEl.value || "").trim().toUpperCase();
      symbolEl.innerHTML = symbols.map((s) => `<option value="${s}">${s}</option>`).join("");
      if (previous && symbols.includes(previous)) {
        symbolEl.value = previous;
      } else if (symbols.includes("BTCUSDT")) {
        symbolEl.value = "BTCUSDT";
      } else {
        symbolEl.value = symbols[0];
      }
    }

    async function loadSymbols() {
      try {
        const resp = await fetch("/api/symbols");
        const data = await resp.json();
        if (!resp.ok || !data.ok) {
          throw new Error(data.error || `请求失败(${resp.status})`);
        }
        const symbols = Array.isArray(data.symbols) ? data.symbols : [];
        if (symbols.length) {
          applySymbolOptions(symbols);
          setStatus(`已加载 ${symbols.length} 个可交易合约币种。`);
        }
      } catch (_) {
        // Keep default static options when symbol API is unavailable.
      }
    }

    function readForm() {
      const checkedModes = Array.from(
        document.querySelectorAll('input[name="allocation_mode"]:checked')
      ).map((x) => x.value);
      return {
        calc_mode: document.getElementById("calc_mode").value.trim(),
        symbol: document.getElementById("symbol").value.trim(),
        min_price: Number(document.getElementById("min_price").value),
        max_price: Number(document.getElementById("max_price").value),
        total_buy_notional: Number(document.getElementById("total_buy_notional").value),
        target_trade_volume: Number(document.getElementById("target_trade_volume").value),
        fixed_n: Number(document.getElementById("fixed_n").value),
        fixed_per_grid_notional: Number(document.getElementById("fixed_per_grid_notional").value),
        start_time: isoFromInput(startTimeEl),
        end_time: isoFromInput(endTimeEl),
        lookback_days: Number(document.getElementById("lookback_days").value),
        interval: document.getElementById("interval").value.trim(),
        n_min: Number(document.getElementById("n_min").value),
        n_max: Number(document.getElementById("n_max").value),
        fee_rate: Number(document.getElementById("fee_rate").value),
        slippage: Number(document.getElementById("slippage").value),
        allocation_modes: checkedModes,
        objective: document.getElementById("objective").value.trim(),
        min_trade_count: Number(document.getElementById("min_trade_count").value),
        min_avg_capital_usage: Number(document.getElementById("min_avg_capital_usage").value),
        top_k: Number(document.getElementById("top_k").value)
      };
    }

    function buildLayerWeights(layerCount, mode) {
      const m = String(mode || "").trim().toLowerCase();
      if (m === "equal") {
        return Array.from({ length: layerCount }, () => 1);
      }
      if (m === "quadratic") {
        return Array.from({ length: layerCount }, (_, i) => (layerCount - i) ** 2);
      }
      if (m === "geometric") {
        const ratio = 1.25;
        return Array.from({ length: layerCount }, (_, i) => ratio ** (layerCount - i - 1));
      }
      return Array.from({ length: layerCount }, (_, i) => layerCount - i);
    }

    function applyLayerModeUI() {
      const mode = layerNModeEl.value;
      const isFitMode = mode === "fit_selected_plan" || mode === "fit_nested_cover_qty";
      const showBudget = !isFitMode && mode.startsWith("budget_");
      const showTargetStep = !isFitMode && mode === "target_step";
      setGroupVisible(layerManualFields, !isFitMode);
      setGroupVisible(layerBudgetFields, showBudget);
      setGroupVisible(layerTargetStepFields, showTargetStep);
    }

    function allocateIntegerByScore(total, scores, minEach = 1) {
      const n = scores.length;
      if (n === 0) return [];
      const minTotal = minEach * n;
      if (total < minTotal) {
        throw new Error(`总N预算至少为 ${minTotal}`);
      }
      const scoreSum = scores.reduce((a, b) => a + Math.max(0, b), 0);
      if (scoreSum <= 0) {
        const base = Array.from({ length: n }, () => minEach);
        let remain = total - minTotal;
        let idx = 0;
        while (remain > 0) {
          base[idx % n] += 1;
          idx += 1;
          remain -= 1;
        }
        return base;
      }

      const normalized = scores.map((x) => Math.max(0, x) / scoreSum);
      const base = Array.from({ length: n }, () => minEach);
      let remain = total - minTotal;
      const extras = normalized.map((r) => r * remain);
      const floors = extras.map((x) => Math.floor(x));
      for (let i = 0; i < n; i += 1) {
        base[i] += floors[i];
      }
      let used = floors.reduce((a, b) => a + b, 0);
      let left = remain - used;
      const remainders = extras.map((x, i) => ({ i, frac: x - floors[i] }));
      remainders.sort((a, b) => b.frac - a.frac);
      for (let k = 0; k < remainders.length && left > 0; k += 1) {
        base[remainders[k].i] += 1;
        left -= 1;
      }
      return base;
    }

    function buildFittedLayersFromPlan(layerCount) {
      if (!latestPlanRows.length) {
        throw new Error("请先完成一次测算并选中候选方案，再做拟合分层");
      }
      const n = latestPlanRows.length;
      if (n <= 0) {
        throw new Error("当前方案没有可用格子");
      }
      const m = Math.max(1, Math.min(layerCount, n));
      const targets = latestPlanRows.map((row) => Number(row.buy_notional));
      const totalNotional = targets.reduce((a, b) => a + b, 0);

      const prefix1 = [0];
      const prefix2 = [0];
      for (const t of targets) {
        prefix1.push(prefix1[prefix1.length - 1] + t);
        prefix2.push(prefix2[prefix2.length - 1] + t * t);
      }

      function segCost(i, j) {
        const len = j - i + 1;
        const sum = prefix1[j + 1] - prefix1[i];
        const sum2 = prefix2[j + 1] - prefix2[i];
        return sum2 - (sum * sum) / len;
      }

      const dp = Array.from({ length: m + 1 }, () => Array(n).fill(Number.POSITIVE_INFINITY));
      const prev = Array.from({ length: m + 1 }, () => Array(n).fill(-1));

      for (let j = 0; j < n; j += 1) {
        dp[1][j] = segCost(0, j);
      }

      for (let k = 2; k <= m; k += 1) {
        for (let j = k - 1; j < n; j += 1) {
          let best = Number.POSITIVE_INFINITY;
          let bestI = -1;
          for (let i = k - 2; i <= j - 1; i += 1) {
            const val = dp[k - 1][i] + segCost(i + 1, j);
            if (val < best) {
              best = val;
              bestI = i;
            }
          }
          dp[k][j] = best;
          prev[k][j] = bestI;
        }
      }

      const segments = [];
      let k = m;
      let j = n - 1;
      while (k >= 1 && j >= 0) {
        const i = prev[k][j];
        const start = i + 1;
        segments.push([start, j]);
        j = i;
        k -= 1;
      }
      segments.reverse();

      const rows = [];
      let absErrSum = 0;
      let absPctSum = 0;
      for (let sIdx = 0; sIdx < segments.length; sIdx += 1) {
        const [start, end] = segments[sIdx];
        const len = end - start + 1;
        const sum = prefix1[end + 1] - prefix1[start];
        const mean = sum / len;

        let layerAbsPct = 0;
        let layerCountPct = 0;
        for (let i = start; i <= end; i += 1) {
          const err = Math.abs(targets[i] - mean);
          absErrSum += err;
          if (targets[i] > 0) {
            const pct = err / targets[i];
            absPctSum += pct;
            layerAbsPct += pct;
            layerCountPct += 1;
          }
        }

        const low = Number(latestPlanRows[start].buy_price);
        const high = Number(latestPlanRows[end].sell_price);
        const mid = (low + high) / 2;
        const stepPct = mid > 0 ? ((high - low) / len) / mid : 0;
        const notional = mean * len;
        const weightRatio = totalNotional > 0 ? notional / totalNotional : 0;
        const layerMape = layerCountPct > 0 ? layerAbsPct / layerCountPct : 0;
        rows.push({
          layer: sIdx + 1,
          grid_span: `${start + 1}-${end + 1}`,
          min_price: low,
          max_price: high,
          weight_ratio: weightRatio,
          notional,
          suggest_n: len,
          step_pct: stepPct,
          per_grid_notional: mean,
          per_grid_qty: null,
          layer_mode: "equal",
          hint: `拟合层；层内MAPE=${(layerMape * 100).toFixed(2)}%`,
        });
      }

      const fitMae = n > 0 ? absErrSum / n : 0;
      const fitMape = n > 0 ? absPctSum / n : 0;
      return {
        rows,
        totalNotional,
        mode: "fit_selected_plan",
        nMode: "fit_selected_plan",
        layerCount: m,
        requestedLayerCount: layerCount,
        targetN: n,
        fitMae,
        fitMape,
      };
    }

    function buildNestedCoverageQtyFromPlan(layerCount) {
      if (!latestPlanRows.length) {
        throw new Error("请先完成一次测算并选中候选方案，再生成递减覆盖方案");
      }
      const n = latestPlanRows.length;
      if (n <= 0) {
        throw new Error("当前方案没有可用格子");
      }
      const m = Math.max(1, Math.min(layerCount, n));
      const prices = latestPlanRows.map((row) => Number(row.buy_price));
      const targetQty = latestPlanRows.map((row) => Number(row.qty));
      const totalNotional = latestPlanRows.reduce((acc, row) => acc + Number(row.buy_notional), 0);

      const coverNs = [];
      for (let i = 0; i < m; i += 1) {
        coverNs.push(Math.max(1, Math.ceil(n * (m - i) / m)));
      }

      const segMeans = [];
      for (let k = 0; k < m; k += 1) {
        const upper = coverNs[k];
        const lower = (k + 1 < m) ? coverNs[k + 1] : 0;
        let sum = 0;
        let cnt = 0;
        for (let idx = lower; idx < upper; idx += 1) {
          sum += targetQty[idx];
          cnt += 1;
        }
        const mean = cnt > 0 ? (sum / cnt) : targetQty[n - 1];
        segMeans.push(mean);
      }

      // Enforce non-decreasing cumulative qty from high->low, so each layer qty stays >= 0.
      for (let i = 1; i < segMeans.length; i += 1) {
        if (segMeans[i] < segMeans[i - 1]) {
          segMeans[i] = segMeans[i - 1];
        }
      }

      const layerQty = [];
      for (let i = 0; i < segMeans.length; i += 1) {
        if (i === 0) {
          layerQty.push(Math.max(0, segMeans[i]));
        } else {
          layerQty.push(Math.max(0, segMeans[i] - segMeans[i - 1]));
        }
      }

      const predicted = Array.from({ length: n }, () => 0);
      for (let i = 0; i < m; i += 1) {
        const q = layerQty[i];
        const coverN = coverNs[i];
        for (let idx = 0; idx < coverN; idx += 1) {
          predicted[idx] += q;
        }
      }

      let absErrSum = 0;
      let absPctSum = 0;
      for (let idx = 0; idx < n; idx += 1) {
        const err = Math.abs(predicted[idx] - targetQty[idx]);
        absErrSum += err;
        if (targetQty[idx] > 0) {
          absPctSum += err / targetQty[idx];
        }
      }

      const rows = [];
      let layeredTotalNotional = 0;
      for (let i = 0; i < m; i += 1) {
        const coverN = coverNs[i];
        const q = layerQty[i];
        const maxPrice = Number(latestPlanRows[coverN - 1].sell_price);
        const minPrice = Number(latestPlanRows[0].buy_price);
        const mid = (minPrice + maxPrice) / 2;
        const stepPct = mid > 0 ? ((maxPrice - minPrice) / coverN) / mid : 0;
        let layerNotional = 0;
        for (let idx = 0; idx < coverN; idx += 1) {
          layerNotional += prices[idx] * q;
        }
        layeredTotalNotional += layerNotional;
        const avgPerGridNotional = coverN > 0 ? layerNotional / coverN : 0;
        rows.push({
          layer: i + 1,
          grid_span: `1-${coverN}`,
          min_price: minPrice,
          max_price: maxPrice,
          weight_ratio: 0,
          notional: layerNotional,
          suggest_n: coverN,
          step_pct: stepPct,
          per_grid_notional: avgPerGridNotional,
          per_grid_qty: q,
          layer_mode: "equal_qty",
          hint: "递减覆盖层；每格同数量，覆盖低价区更深",
        });
      }
      for (const row of rows) {
        row.weight_ratio = layeredTotalNotional > 0 ? row.notional / layeredTotalNotional : 0;
      }

      const fitMae = n > 0 ? absErrSum / n : 0;
      const fitMape = n > 0 ? absPctSum / n : 0;
      return {
        rows,
        totalNotional: layeredTotalNotional,
        mode: "fit_nested_cover_qty",
        nMode: "fit_nested_cover_qty",
        layerCount: m,
        requestedLayerCount: layerCount,
        targetN: n,
        fitMae,
        fitMape,
      };
    }

    function suggestLayerN(low, high, targetStepPct, nCap, feeRate, slippage) {
      const mid = (low + high) / 2;
      if (mid <= 0 || high <= low) return 6;
      const widthRatio = (high - low) / mid;
      const targetStepRatio = Math.max(
        Math.max(0.0001, targetStepPct / 100),
        (2 * Math.max(0, feeRate) + 2 * Math.max(0, slippage)) * 1.2
      );
      const rawN = Math.floor(widthRatio / targetStepRatio);
      const cap = Math.max(5, Math.floor(nCap));
      return Math.max(5, Math.min(cap, rawN));
    }

    function renderLayerRows(rows) {
      latestLayerRows = rows;
      layerCsvBtn.disabled = rows.length === 0;
      layerCompareBtn.disabled = rows.length === 0;
      layerBody.innerHTML = rows.map((x) => `
        <tr>
          <td>${x.layer}</td>
          <td>${x.grid_span || "-"}</td>
          <td>${fmtNum(x.min_price)}</td>
          <td>${fmtNum(x.max_price)}</td>
          <td>${fmtPct(x.weight_ratio)}</td>
          <td>${fmtNum(x.notional)}</td>
          <td>${x.suggest_n}</td>
          <td>${fmtPct(x.step_pct)}</td>
          <td>${fmtNum(x.per_grid_notional)}</td>
          <td>${x.per_grid_qty === null || x.per_grid_qty === undefined ? "-" : fmtNum(x.per_grid_qty, 6)}</td>
          <td>${x.hint}</td>
        </tr>
      `).join("");
    }

    function buildLayeredParams() {
      const payload = readForm();
      const layerCount = Math.floor(Number(layerCountEl.value));
      const targetStepPct = Number(layerTargetStepPctEl.value);
      const nCap = Math.floor(Number(layerNCapEl.value));
      const nMode = layerNModeEl.value;
      const totalNBudget = Math.floor(Number(layerTotalNEl.value));
      const mode = layerWeightModeEl.value;

      if (nMode === "fit_selected_plan") {
        if (!(layerCount >= 1 && layerCount <= 50)) {
          throw new Error("分层数量需在 1~50 之间");
        }
        return buildFittedLayersFromPlan(layerCount);
      }
      if (nMode === "fit_nested_cover_qty") {
        if (!(layerCount >= 1 && layerCount <= 50)) {
          throw new Error("分层数量需在 1~50 之间");
        }
        return buildNestedCoverageQtyFromPlan(layerCount);
      }

      const minPrice = Number(payload.min_price);
      const maxPrice = Number(payload.max_price);
      const totalNotional = payload.calc_mode === "fixed"
        ? Number(payload.fixed_n) * Number(payload.fixed_per_grid_notional)
        : Number(payload.total_buy_notional);

      if (!(layerCount >= 2 && layerCount <= 50)) {
        throw new Error("分层数量需在 2~50 之间");
      }
      if (!(minPrice > 0 && maxPrice > minPrice)) {
        throw new Error("价格区间无效，请检查最低价和最高价");
      }
      if (!(totalNotional > 0)) {
        throw new Error("总投入必须大于 0");
      }
      if (!(targetStepPct > 0)) {
        if (nMode === "target_step") {
          throw new Error("目标单格价差(%) 必须大于 0");
        }
      }
      if (!(nCap >= 5)) {
        if (nMode === "target_step") {
          throw new Error("每层建议N上限至少为 5");
        }
      }
      if (nMode.startsWith("budget_")) {
        if (!(totalNBudget >= layerCount)) {
          throw new Error(`总格子预算至少为分层数量（>=${layerCount}）`);
        }
        if (totalNBudget > 5000) {
          throw new Error("总格子预算过大（>5000），请检查是否误填。");
        }
        if (latestTopCandidates.length) {
          const current = latestTopCandidates[selectedTopIndex] || latestTopCandidates[0];
          const refN = Number(current && current.n ? current.n : 0);
          if (refN > 0) {
            const softCap = Math.max(layerCount, refN * 20);
            if (totalNBudget > softCap) {
              throw new Error(
                `总格子预算=${totalNBudget} 远大于当前方案N=${refN}。` +
                `建议先用 ${refN}（对齐）或 ${Math.min(softCap, refN * 3)}（适度加密）。`
              );
            }
          }
        }
      }

      const width = (maxPrice - minPrice) / layerCount;
      const weights = buildLayerWeights(layerCount, mode);
      const totalWeight = weights.reduce((a, b) => a + b, 0);
      const widths = Array.from({ length: layerCount }, () => width);
      let nPlan = [];
      if (nMode === "budget_equal_step") {
        nPlan = allocateIntegerByScore(totalNBudget, widths, 1);
      } else if (nMode === "budget_by_weight") {
        nPlan = allocateIntegerByScore(totalNBudget, weights, 1);
      }
      const rows = [];

      for (let i = 0; i < layerCount; i += 1) {
        const low = minPrice + i * width;
        const high = (i === layerCount - 1) ? maxPrice : (minPrice + (i + 1) * width);
        const ratio = weights[i] / totalWeight;
        const layerNotional = totalNotional * ratio;
        let suggestN = nPlan[i];
        if (nMode === "target_step") {
          suggestN = suggestLayerN(
            low,
            high,
            targetStepPct,
            nCap,
            payload.fee_rate,
            payload.slippage
          );
        }
        const mid = (low + high) / 2;
        const stepPct = mid > 0 ? ((high - low) / suggestN) / mid : 0;
        const perGridNotional = layerNotional / suggestN;
        let hint = "中间过渡层";
        if (i === 0) hint = "最低价层，资金更重";
        if (i === layerCount - 1) hint = "高价层，资金更轻";
        if (nMode === "budget_equal_step") hint += "；N按区间宽度分配";
        if (nMode === "budget_by_weight") hint += "；N按资金权重分配";
        if (nMode === "target_step") hint += "；N按目标步长独立计算";
        rows.push({
          layer: i + 1,
          grid_span: "-",
          min_price: low,
          max_price: high,
          weight_ratio: ratio,
          notional: layerNotional,
          suggest_n: suggestN,
          step_pct: stepPct,
          per_grid_notional: perGridNotional,
          per_grid_qty: null,
          layer_mode: "equal",
          hint,
        });
      }
      return { rows, totalNotional, mode, layerCount, nMode };
    }

    function renderComparison(baseline, layered) {
      const specs = [
        { key: "total_buy_notional", label: "总投入基准", type: "num", preferLower: false, neutral: true },
        { key: "net_profit", label: "净收益", type: "num", preferLower: false },
        { key: "total_return", label: "总收益率", type: "pct", preferLower: false },
        { key: "annualized_return", label: "年化收益", type: "pct", preferLower: false },
        { key: "max_drawdown", label: "最大回撤", type: "pct", preferLower: true },
        { key: "calmar", label: "Calmar", type: "num", preferLower: false },
        { key: "trade_count", label: "成交数", type: "int", preferLower: false, neutral: true },
        { key: "trade_volume", label: "成交额", type: "num", preferLower: false },
        { key: "total_fees", label: "手续费", type: "num", preferLower: true },
        { key: "avg_capital_usage", label: "平均资金占用", type: "pct", preferLower: false, neutral: true },
        { key: "realized_pnl", label: "已实现收益", type: "num", preferLower: false },
        { key: "unrealized_pnl", label: "期末浮盈", type: "num", preferLower: false },
      ];

      function fmtByType(value, type) {
        if (value === null || value === undefined || Number.isNaN(value)) return "-";
        if (type === "pct") return fmtPct(value);
        if (type === "int") return String(Math.round(Number(value)));
        return fmtNum(value);
      }

      function fmtDeltaByType(value, type) {
        if (value === null || value === undefined || Number.isNaN(value)) return "-";
        if (type === "pct") return signedPct(value);
        if (type === "int") {
          const n = Math.round(Number(value));
          if (n === 0) return "0";
          return `${n > 0 ? "+" : ""}${n}`;
        }
        return signedNum(value);
      }

      compareBody.innerHTML = specs.map((spec) => {
        const rawBase = baseline[spec.key];
        const rawLay = layered[spec.key];
        const base = Number(rawBase);
        const lay = Number(rawLay);
        const hasBase = rawBase !== null && rawBase !== undefined && Number.isFinite(base);
        const hasLay = rawLay !== null && rawLay !== undefined && Number.isFinite(lay);
        const delta = hasBase && hasLay ? (lay - base) : null;
        const cls = spec.neutral ? "delta-neutral" : deltaClass(delta, !!spec.preferLower);
        return `
          <tr>
            <td>${spec.label}</td>
            <td>${hasBase ? fmtByType(base, spec.type) : "-"}</td>
            <td>${hasLay ? fmtByType(lay, spec.type) : "-"}</td>
            <td class="${cls}">${fmtDeltaByType(delta, spec.type)}</td>
          </tr>
        `;
      }).join("");
    }

    function renderSummary(best, candleCount) {
      const items = [
        ["当前 N", best.n],
        ["当前分配模式", best.allocation_mode],
        ["净收益", fmtNum(best.net_profit)],
        ["总投入基准", fmtNum(best.total_buy_notional)],
        ["已实现收益", fmtNum(best.realized_pnl)],
        ["期末浮盈", fmtNum(best.unrealized_pnl)],
        ["手续费", fmtNum(best.total_fees)],
        ["总收益率", fmtPct(best.total_return)],
        ["标的区间涨跌", fmtPct(best.underlying_return)],
        ["起始价格", fmtNum(best.start_price)],
        ["终止价格", fmtNum(best.end_price)],
        ["最大回撤", fmtPct(best.max_drawdown)],
        ["年化收益", fmtPct(best.annualized_return)],
        ["成交数", best.trade_count],
        ["成交额", fmtNum(best.trade_volume)],
        ["目标成交额", fmtNum(best.target_trade_volume)],
        ["目标达成率", fmtPct(best.volume_coverage)],
        ["平均资金占用", fmtPct(best.avg_capital_usage)],
        ["K线数量", candleCount]
      ];
      summaryEl.innerHTML = items.map(([k, v]) => (
        `<div class="kpi"><div class="k">${k}</div><div class="v">${v}</div></div>`
      )).join("");
      formulaEl.textContent =
        `净收益 = 已实现收益(${fmtNum(best.realized_pnl)}) + 期末浮盈(${fmtNum(best.unrealized_pnl)}) - 手续费(${fmtNum(best.total_fees)}) = ${fmtNum(best.net_profit)}。` +
        ` 总收益率 = 净收益(${fmtNum(best.net_profit)}) / 总投入基准(${fmtNum(best.total_buy_notional)}) = ${fmtPct(best.total_return)}。` +
        ` 回测区间：${fmtDateTime(best.start_time)} @ ${fmtNum(best.start_price)} -> ${fmtDateTime(best.end_time)} @ ${fmtNum(best.end_price)}。`;
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
          <td>${fmtNum(x.trade_volume)}</td>
          <td>${fmtPct(x.volume_coverage)}</td>
          <td>${fmtNum(x.total_fees)}</td>
        </tr>
      `).join("");
    }

    function renderRangeSuggestions(rows) {
      latestRangeSuggestions = rows;
      suggestBody.innerHTML = rows.map((x, idx) => `
        <tr>
          <td>${idx + 1}</td>
          <td>${fmtNum(x.min_price)}</td>
          <td>${fmtNum(x.max_price)}</td>
          <td>${x.recommended_n}</td>
          <td>${x.recommended_mode}</td>
          <td>${fmtPct(x.step_pct)}</td>
          <td>${fmtNum(x.avg_per_grid_notional)} / ${fmtNum(x.min_per_grid_notional)} / ${fmtNum(x.max_per_grid_notional)}</td>
          <td>${fmtNum(x.trade_volume)}</td>
          <td>${fmtPct(x.volume_coverage)}</td>
          <td>${fmtNum(x.net_profit)}</td>
          <td>${fmtPct(x.max_drawdown)}</td>
          <td>${x.trade_count}</td>
          <td>${x.reason}</td>
          <td><button type="button" data-suggest-idx="${idx}">应用</button></td>
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

    function downloadLayerCsv() {
      if (!latestLayerRows.length) return;
      const lines = [
        "layer,grid_span,min_price,max_price,weight_ratio,notional,suggest_n,step_pct,per_grid_notional,per_grid_qty,layer_mode,hint"
      ];
      for (const row of latestLayerRows) {
        lines.push(
          [
            row.layer,
            row.grid_span,
            row.min_price,
            row.max_price,
            row.weight_ratio,
            row.notional,
            row.suggest_n,
            row.step_pct,
            row.per_grid_notional,
            row.per_grid_qty,
            row.layer_mode,
            row.hint,
          ].join(",")
        );
      }
      const blob = new Blob([lines.join("\\n")], { type: "text/csv;charset=utf-8;" });
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = "layered_grid_params.csv";
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      URL.revokeObjectURL(url);
    }

    layerBtn.addEventListener("click", () => {
      try {
        const out = buildLayeredParams();
        renderLayerRows(out.rows);
        clearComparison("分层参数已更新，点击“分层组合回测对比”查看与当前方案差异。");
        const nSum = out.rows.reduce((acc, row) => acc + row.suggest_n, 0);
        if (out.nMode === "fit_selected_plan" || out.nMode === "fit_nested_cover_qty") {
          const extra = out.requestedLayerCount !== out.layerCount
            ? `（请求${out.requestedLayerCount}层，实际${out.layerCount}层）`
            : "";
          const modeText = out.nMode === "fit_nested_cover_qty"
            ? "递减覆盖拟合"
            : "分段拟合";
          setLayerStatus(
            `已完成${modeText}${extra}：目标N=${out.targetN}，压缩后层数=${out.layerCount}，合计N=${nSum}，MAPE=${fmtPct(out.fitMape)}，MAE=${fmtNum(out.fitMae)}。`
          );
        } else {
          setLayerStatus(
            `已生成 ${out.layerCount} 层，资金模式=${out.mode}，N模式=${out.nMode}，合计N=${nSum}，总投入=${fmtNum(out.totalNotional)}。`
          );
        }
      } catch (err) {
        renderLayerRows([]);
        clearComparison();
        setLayerStatus(`生成失败：${err.message}`, true);
      }
    });
    layerCsvBtn.addEventListener("click", downloadLayerCsv);
    layerNModeEl.addEventListener("change", applyLayerModeUI);
    applyLayerModeUI();
    clearComparison();

    layerCompareBtn.addEventListener("click", async () => {
      if (!latestLayerRows.length) {
        setCompareStatus("请先生成分层参数。", true);
        return;
      }
      if (!latestTopCandidates.length) {
        setCompareStatus("请先完成测算并选中当前方案。", true);
        return;
      }
      const baseline = latestTopCandidates[selectedTopIndex];
      const payload = readForm();
      const req = {
        symbol: payload.symbol,
        start_time: payload.start_time,
        end_time: payload.end_time,
        lookback_days: payload.lookback_days,
        interval: payload.interval,
        fee_rate: payload.fee_rate,
        slippage: payload.slippage,
        refresh: false,
        layers: latestLayerRows
          .map((x) => ({
            min_price: Number(x.min_price),
            max_price: Number(x.max_price),
            n: Number(x.suggest_n),
            notional: Number(x.notional),
            grid_span: x.grid_span || "-",
            layer_mode: x.layer_mode || "equal",
          }))
          .filter((x) => x.n > 0 && x.notional > 0),
        baseline: {
          n: baseline.n,
          allocation_mode: baseline.allocation_mode,
          total_buy_notional: baseline.total_buy_notional,
          net_profit: baseline.net_profit,
          total_return: baseline.total_return,
          annualized_return: baseline.annualized_return,
          max_drawdown: baseline.max_drawdown,
          calmar: baseline.calmar,
          trade_count: baseline.trade_count,
          trade_volume: baseline.trade_volume,
          total_fees: baseline.total_fees,
          avg_capital_usage: baseline.avg_capital_usage,
          realized_pnl: baseline.realized_pnl,
          unrealized_pnl: baseline.unrealized_pnl,
        },
      };
      if (!req.layers.length) {
        setCompareStatus("当前分层参数没有可回测层（可能每层资金过低）。", true);
        return;
      }

      layerCompareBtn.disabled = true;
      setCompareStatus("正在回测分层组合，请稍候...");
      try {
        const resp = await fetch("/api/layer_compare", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(req)
        });
        const data = await resp.json();
        if (!resp.ok || !data.ok) {
          throw new Error(data.error || `请求失败(${resp.status})`);
        }
        latestComparison = data;
        renderComparison(data.baseline, data.layered);
        const cacheLabel = data.data.cache_file ? ` 缓存：${data.data.cache_file}` : "";
        setCompareStatus(
          `对比完成：分层=${data.layered.layer_count}层，合计N=${data.layered.layer_total_n}，K线=${data.data.candles}。${cacheLabel}`
        );
      } catch (err) {
        compareBody.innerHTML = "";
        setCompareStatus(`对比失败：${err.message}`, true);
      } finally {
        layerCompareBtn.disabled = latestLayerRows.length === 0;
      }
    });

    function selectTopCandidate(index) {
      if (!latestTopCandidates.length) return;
      if (index < 0 || index >= latestTopCandidates.length) return;
      selectedTopIndex = index;
      const selected = latestTopCandidates[index];
      if (layerTotalNEl && Number.isFinite(Number(selected.n)) && selected.n > 0) {
        layerTotalNEl.value = String(selected.n);
      }
      renderSummary(selected, latestCandleCount);
      renderPlan(selected.plan || []);
      clearComparison("当前方案已切换，请重新点击“分层组合回测对比”。");
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
    calcModeEl.addEventListener("change", applyCalcModeUI);
    applyCalcModeUI();
    initDateRangeDefaults();
    loadSymbols();
    renderRangeSuggestions([]);

    suggestBody.addEventListener("click", (e) => {
      const btn = e.target.closest("button[data-suggest-idx]");
      if (!btn) return;
      const idx = Number(btn.dataset.suggestIdx);
      if (Number.isNaN(idx) || idx < 0 || idx >= latestRangeSuggestions.length) return;
      const picked = latestRangeSuggestions[idx];
      document.getElementById("min_price").value = String(Number(picked.min_price.toFixed(8)));
      document.getElementById("max_price").value = String(Number(picked.max_price.toFixed(8)));
      document.getElementById("n_min").value = String(picked.recommended_n);
      document.getElementById("n_max").value = String(picked.recommended_n);
      for (const checkbox of document.querySelectorAll('input[name="allocation_mode"]')) {
        checkbox.checked = checkbox.value === picked.recommended_mode;
      }
      setStatus(
        `已应用智能建议：min=${fmtNum(picked.min_price)}，max=${fmtNum(picked.max_price)}，N=${picked.recommended_n}，mode=${picked.recommended_mode}。点击“开始测算”可查看每格买入计划。`
      );
    });

    suggestBtn.addEventListener("click", async () => {
      const payload = readForm();
      if (payload.calc_mode !== "optimize") {
        setSuggestStatus("固定参数模式不需要区间建议，请切换到优化模式。", true);
        return;
      }
      suggestBtn.disabled = true;
      setSuggestStatus("正在计算区间建议，请稍候...");
      try {
        const resp = await fetch("/api/suggest_range", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload)
        });
        const data = await resp.json();
        if (!resp.ok || !data.ok) {
          throw new Error(data.error || `请求失败(${resp.status})`);
        }
        const suggestions = data.suggestions || [];
        renderRangeSuggestions(suggestions);
        if (!suggestions.length) {
          setSuggestStatus("未生成可用建议，请放宽参数（例如降低目标成交额或放宽N范围）。", true);
          return;
        }
        const best = suggestions[0];
        setSuggestStatus(
          `已生成 ${suggestions.length} 组建议。首选达成率=${fmtPct(best.volume_coverage)}，成交额=${fmtNum(best.trade_volume)}。`
        );
      } catch (err) {
        renderRangeSuggestions([]);
        setSuggestStatus(`区间建议失败：${err.message}`, true);
      } finally {
        suggestBtn.disabled = false;
      }
    });

    async function pollJob(jobId, token) {
      while (token === currentPollToken) {
        const resp = await fetch(`/api/job/${jobId}`);
        const data = await resp.json();
        if (!resp.ok || !data.ok) {
          throw new Error(data.error || `任务查询失败(${resp.status})`);
        }

        setProgress(data.progress, data.eta_seconds, data.message || "");

        if (data.status === "done") {
          return data.result;
        }
        if (data.status === "failed") {
          throw new Error(data.error || data.message || "任务失败");
        }
        await new Promise((resolve) => setTimeout(resolve, 1000));
      }
      throw new Error("任务已被新的测算请求替换");
    }

    form.addEventListener("submit", async (e) => {
      e.preventDefault();
      const payload = readForm();
      if (payload.start_time && payload.end_time) {
        const startMs = new Date(payload.start_time).getTime();
        const endMs = new Date(payload.end_time).getTime();
        if (Number.isFinite(startMs) && Number.isFinite(endMs) && startMs >= endMs) {
          setStatus("测算失败：开始时间必须早于结束时间。", true);
          return;
        }
      }
      currentPollToken += 1;
      const token = currentPollToken;
      runBtn.disabled = true;
      csvBtn.disabled = true;
      setStatus("正在提交测算任务...");
      setProgress(0, null, "任务初始化");
      try {
        const submitResp = await fetch("/api/optimize", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload)
        });
        const submitData = await submitResp.json();
        if (!submitResp.ok || !submitData.ok) {
          throw new Error(submitData.error || `请求失败(${submitResp.status})`);
        }
        setStatus(`任务已启动：${submitData.job_id.slice(0, 8)}...`);
        const data = await pollJob(submitData.job_id, token);
        if (!data.best) {
          const msg = data.error ? `未找到可用方案：${data.error}` : "未找到可用方案，请调整参数。";
          setStatus(msg, true);
          summaryEl.innerHTML = "";
          formulaEl.textContent = "";
          topBody.innerHTML = "";
          planBody.innerHTML = "";
          latestTopCandidates = [];
          latestPlanRows = [];
          clearComparison();
          return;
        }
        latestTopCandidates = data.top || [];
        latestCandleCount = data.data.candles;
        if (!latestTopCandidates.length) {
          latestTopCandidates = [data.best];
        }
        selectTopCandidate(0);
        const cacheLabel = data.data.cache_file ? ` 缓存：${data.data.cache_file}` : "";
        if (payload.calc_mode === "fixed") {
          setStatus(`完成：固定参数回测已完成。${cacheLabel}`);
        } else if (payload.objective === "competition_volume") {
          const best = latestTopCandidates[0];
          setStatus(
            `完成：已按交易赛目标排序，Top1达成率=${fmtPct(best.volume_coverage)}，成交额=${fmtNum(best.trade_volume)}。${cacheLabel}`
          );
        } else {
          setStatus(`完成：默认显示最优候选。${cacheLabel}`);
        }
        setProgress(1, 0, "完成");
      } catch (err) {
        if (token === currentPollToken) {
          setStatus(`测算失败：${err.message}`, true);
          clearComparison();
        }
      } finally {
        if (token === currentPollToken) {
          runBtn.disabled = false;
        }
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


def _parse_optional_datetime(value: Any, name: str) -> datetime | None:
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    cleaned = raw.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(cleaned)
    except ValueError as exc:
        raise ValueError(f"{name} must be an ISO datetime string") from exc
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _safe_metric(value: float) -> float | None:
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def _series_max_drawdown(nav: list[float]) -> float:
    if not nav:
        return 0.0
    peak = nav[0]
    max_dd = 0.0
    for value in nav:
        if value > peak:
            peak = value
        if peak <= 0:
            continue
        dd = (peak - value) / peak
        if dd > max_dd:
            max_dd = dd
    return max_dd


def _load_symbol_list(refresh: bool = False) -> tuple[list[str], str]:
    now = time.time()
    with SYMBOLS_LOCK:
        cached = SYMBOLS_CACHE.get("symbols") or []
        updated_at = float(SYMBOLS_CACHE.get("updated_at", 0.0))
        if not refresh and cached and (now - updated_at) <= SYMBOLS_TTL_SECONDS:
            return list(cached), "cache"

    try:
        symbols = list_futures_symbols(
            quote_asset="USDT",
            contract_type="PERPETUAL",
            only_trading=True,
        )
        if not symbols:
            raise RuntimeError("No symbols returned from Binance futures exchangeInfo")
        with SYMBOLS_LOCK:
            SYMBOLS_CACHE["symbols"] = list(symbols)
            SYMBOLS_CACHE["updated_at"] = now
        return symbols, "binance"
    except Exception:
        with SYMBOLS_LOCK:
            cached = SYMBOLS_CACHE.get("symbols") or []
        if cached:
            return list(cached), "stale_cache"
        return list(DEFAULT_SYMBOLS), "fallback"


def _percentile(sorted_values: list[float], p: float) -> float:
    if not sorted_values:
        raise ValueError("sorted_values cannot be empty")
    if p <= 0:
        return sorted_values[0]
    if p >= 1:
        return sorted_values[-1]
    idx = p * (len(sorted_values) - 1)
    low = int(math.floor(idx))
    high = int(math.ceil(idx))
    if low == high:
        return sorted_values[low]
    frac = idx - low
    return sorted_values[low] * (1.0 - frac) + sorted_values[high] * frac


def _sample_n_values(n_min: int, n_max: int, max_count: int = 16) -> list[int]:
    if n_min <= 0 or n_max <= 0 or n_min > n_max:
        raise ValueError("invalid n range")
    span = n_max - n_min + 1
    if span <= max_count:
        return list(range(n_min, n_max + 1))
    values = {n_min, n_max}
    steps = max_count - 2
    for i in range(1, steps + 1):
        ratio = i / (steps + 1)
        n = int(round(n_min + ratio * (n_max - n_min)))
        values.add(max(n_min, min(n_max, n)))
    return sorted(values)


def _build_candidate_ranges(candles: list[Any]) -> list[tuple[float, float, str]]:
    low_prices = sorted(float(x.low) for x in candles)
    high_prices = sorted(float(x.high) for x in candles)
    close_prices = sorted(float(x.close) for x in candles)
    last_close = float(candles[-1].close)
    mid_close = _percentile(close_prices, 0.5)

    raw_ranges: list[tuple[float, float, str]] = []
    quantile_pairs = [
        (0.01, 0.99),
        (0.03, 0.97),
        (0.05, 0.95),
        (0.10, 0.90),
        (0.15, 0.85),
        (0.20, 0.80),
        (0.25, 0.75),
    ]
    for low_q, high_q in quantile_pairs:
        mn = _percentile(low_prices, low_q)
        mx = _percentile(high_prices, high_q)
        raw_ranges.append((mn, mx, f"quantile_{int(low_q * 100)}_{int(high_q * 100)}"))

    widths = [0.02, 0.03, 0.05, 0.08, 0.12, 0.18]
    for width in widths:
        raw_ranges.append(
            (
                max(0.0000001, last_close * (1.0 - width)),
                last_close * (1.0 + width),
                f"last_close_{int(width * 100)}pct",
            )
        )
        raw_ranges.append(
            (
                max(0.0000001, mid_close * (1.0 - width)),
                mid_close * (1.0 + width),
                f"mid_close_{int(width * 100)}pct",
            )
        )

    dedup: dict[tuple[float, float], tuple[float, float, str]] = {}
    for mn, mx, tag in raw_ranges:
        if mn <= 0 or mx <= mn:
            continue
        if (mx - mn) / ((mx + mn) / 2.0) < 0.004:
            continue
        key = (round(mn, 6), round(mx, 6))
        dedup[key] = (mn, mx, tag)

    ranges = sorted(dedup.values(), key=lambda x: x[1] - x[0])
    return ranges[:16]


def _competition_sort_key(
    trade_volume: float,
    target_trade_volume: float,
    net_profit: float,
    max_drawdown: float,
) -> tuple[float, float, float, float, float, float]:
    if target_trade_volume > 0:
        meets_target = 1.0 if trade_volume >= target_trade_volume else 0.0
        coverage = min(1.0, trade_volume / target_trade_volume)
    else:
        meets_target = 1.0
        coverage = 1.0
    loss = max(0.0, -net_profit)
    profit = max(0.0, net_profit)
    return (
        meets_target,
        coverage,
        -loss,
        profit,
        trade_volume,
        -max_drawdown,
    )


def _mode_preference_score(mode: str) -> float:
    # Competition-style volume seeking with uncertain direction:
    # prefer more direction-neutral distribution modes.
    order = {
        "equal_qty": 6.0,
        "equal": 5.5,
        "center_heavy": 5.0,
        "edge_heavy": 4.6,
        "linear_reverse": 4.0,
        "linear": 3.8,
        "geometric_reverse": 3.4,
        "geometric": 3.2,
        "quadratic_reverse": 3.0,
        "quadratic": 2.8,
    }
    return order.get(mode.strip().lower(), 0.0)


def _ordered_suggestion_modes(modes: list[str]) -> list[str]:
    dedup: list[str] = []
    for mode in modes:
        cleaned = mode.strip().lower()
        if cleaned and cleaned not in dedup:
            dedup.append(cleaned)
    if not dedup:
        return dedup
    return sorted(
        dedup,
        key=lambda x: (_mode_preference_score(x), x),
        reverse=True,
    )


def _normalize_suggest_payload(payload: dict[str, Any]) -> dict[str, Any]:
    symbol = str(payload.get("symbol", "BTCUSDT")).upper().strip()
    lookback_days = _safe_int(payload.get("lookback_days", 7), "lookback_days")
    start_time = _parse_optional_datetime(payload.get("start_time"), "start_time")
    end_time = _parse_optional_datetime(payload.get("end_time"), "end_time")
    interval = str(payload.get("interval", "1m")).strip()
    total_buy_notional = _safe_float(payload.get("total_buy_notional", 0), "total_buy_notional")
    target_trade_volume = _safe_float(payload.get("target_trade_volume", 0.0), "target_trade_volume")
    n_min = _safe_int(payload.get("n_min", 5), "n_min")
    n_max = _safe_int(payload.get("n_max", 200), "n_max")
    fee_rate = _safe_float(payload.get("fee_rate", 0.0002), "fee_rate")
    slippage = _safe_float(payload.get("slippage", 0.0), "slippage")
    funding_buffer = _safe_float(payload.get("funding_buffer", 0.0), "funding_buffer")
    min_trade_count = _safe_int(payload.get("min_trade_count", 0), "min_trade_count")
    min_avg_capital_usage = _safe_float(
        payload.get("min_avg_capital_usage", 0.0), "min_avg_capital_usage"
    )
    top_k = _safe_int(payload.get("top_k", 5), "top_k")
    refresh = bool(payload.get("refresh", False))

    raw_modes = payload.get("allocation_modes", "equal,linear")
    if isinstance(raw_modes, str):
        allocation_modes = [x.strip().lower() for x in raw_modes.split(",") if x.strip()]
    elif isinstance(raw_modes, list):
        allocation_modes = [str(x).strip().lower() for x in raw_modes if str(x).strip()]
    else:
        raise ValueError("allocation_modes must be string or list")

    if total_buy_notional <= 0:
        raise ValueError("total_buy_notional must be > 0")
    if target_trade_volume < 0:
        raise ValueError("target_trade_volume must be >= 0")
    if start_time is not None and end_time is not None and start_time >= end_time:
        raise ValueError("start_time must be earlier than end_time")
    if start_time is None and end_time is None and lookback_days <= 0:
        raise ValueError("lookback_days must be > 0")
    if (start_time is None or end_time is None) and lookback_days <= 0:
        raise ValueError("lookback_days must be > 0 when only one boundary time is provided")
    if n_min <= 0 or n_max <= 0 or n_min > n_max:
        raise ValueError("invalid n range")
    if fee_rate < 0 or slippage < 0 or funding_buffer < 0:
        raise ValueError("fee/slippage/funding_buffer must be >= 0")
    if min_trade_count < 0:
        raise ValueError("min_trade_count must be >= 0")
    if min_avg_capital_usage < 0 or min_avg_capital_usage > 1:
        raise ValueError("min_avg_capital_usage must be in [0,1]")
    if top_k <= 0:
        raise ValueError("top_k must be > 0")
    if not allocation_modes:
        raise ValueError("allocation_modes cannot be empty")
    supported = set(supported_allocation_modes())
    unknown = [x for x in allocation_modes if x not in supported]
    if unknown:
        raise ValueError(
            f"unsupported allocation_modes: {','.join(unknown)}; "
            f"supported: {','.join(sorted(supported))}"
        )

    return {
        "symbol": symbol,
        "lookback_days": lookback_days,
        "start_time": start_time,
        "end_time": end_time,
        "interval": interval,
        "total_buy_notional": total_buy_notional,
        "target_trade_volume": target_trade_volume,
        "n_min": n_min,
        "n_max": n_max,
        "fee_rate": fee_rate,
        "slippage": slippage,
        "funding_buffer": funding_buffer,
        "min_trade_count": min_trade_count,
        "min_avg_capital_usage": min_avg_capital_usage,
        "allocation_modes": _ordered_suggestion_modes(allocation_modes),
        "top_k": min(10, top_k),
        "refresh": refresh,
    }


def _range_reason(coverage: float, net_profit: float, mode: str) -> str:
    mode_hint = "中性分配" if _mode_preference_score(mode) >= 5.0 else "方向性分配"
    if coverage >= 1.0 and net_profit >= 0:
        return f"达成目标且净收益为正（{mode_hint}）"
    if coverage >= 1.0 and net_profit < 0:
        return f"达成目标，优先控制损失（{mode_hint}）"
    if coverage < 1.0 and net_profit >= 0:
        return f"未达目标但净收益为正，可继续加密（{mode_hint}）"
    return f"优先提升成交额，亏损可控（{mode_hint}）"


def _run_range_suggestion(params: dict[str, Any]) -> dict[str, Any]:
    cache_path = str(cache_file_path(params["symbol"], params["interval"], cache_dir="data"))
    candles = load_or_fetch_candles(
        symbol=params["symbol"],
        interval=params["interval"],
        lookback_days=params["lookback_days"],
        cache_dir="data",
        refresh=params["refresh"],
        start_time=params["start_time"],
        end_time=params["end_time"],
    )
    if len(candles) < 10:
        raise ValueError("Not enough candle data for range suggestion")

    range_candidates = _build_candidate_ranges(candles)
    if not range_candidates:
        raise ValueError("Unable to build candidate ranges from current data")
    n_values = _sample_n_values(params["n_min"], params["n_max"], max_count=16)

    suggestions: list[dict[str, Any]] = []
    for mn, mx, tag in range_candidates:
        optimization = optimize_grid_count(
            candles=candles,
            min_price=mn,
            max_price=mx,
            total_buy_notional=params["total_buy_notional"],
            n_min=n_values[0],
            n_max=n_values[-1],
            n_values=n_values,
            fee_rate=params["fee_rate"],
            slippage=params["slippage"],
            funding_buffer=params["funding_buffer"],
            allocation_modes=params["allocation_modes"],
            objective="competition_volume",
            target_trade_volume=params["target_trade_volume"],
            min_trade_count=params["min_trade_count"],
            min_avg_capital_usage=params["min_avg_capital_usage"],
            top_k=max(10, len(params["allocation_modes"])),
        )
        if not optimization.top_results:
            continue

        ranked_candidates = sorted(
            optimization.top_results,
            key=lambda item: (
                _competition_sort_key(
                    trade_volume=float(item.trade_volume),
                    target_trade_volume=float(params["target_trade_volume"]),
                    net_profit=float(item.net_profit),
                    max_drawdown=float(item.max_drawdown),
                ),
                _mode_preference_score(item.allocation_mode),
            ),
            reverse=True,
        )
        best = ranked_candidates[0]
        if params["target_trade_volume"] > 0:
            coverage = best.trade_volume / params["target_trade_volume"]
        else:
            coverage = 1.0
        width_pct = (mx - mn) / ((mx + mn) / 2.0) if (mx + mn) > 0 else 0.0
        mid = (mn + mx) / 2.0
        step_pct = ((mx - mn) / best.n) / mid if mid > 0 and best.n > 0 else 0.0
        min_grid_notional = min(best.per_grid_notionals) if best.per_grid_notionals else 0.0
        max_grid_notional = max(best.per_grid_notionals) if best.per_grid_notionals else 0.0
        avg_grid_notional = (
            sum(best.per_grid_notionals) / len(best.per_grid_notionals)
            if best.per_grid_notionals
            else 0.0
        )
        suggestions.append(
            {
                "min_price": mn,
                "max_price": mx,
                "range_width_pct": width_pct,
                "recommended_n": best.n,
                "recommended_mode": best.allocation_mode,
                "step_pct": step_pct,
                "avg_per_grid_notional": avg_grid_notional,
                "min_per_grid_notional": min_grid_notional,
                "max_per_grid_notional": max_grid_notional,
                "trade_volume": best.trade_volume,
                "volume_coverage": coverage,
                "net_profit": best.net_profit,
                "max_drawdown": best.max_drawdown,
                "trade_count": best.trade_count,
                "total_fees": best.total_fees,
                "source": tag,
                "reason": _range_reason(
                    coverage=coverage,
                    net_profit=best.net_profit,
                    mode=best.allocation_mode,
                ),
            }
        )

    suggestions = sorted(
        suggestions,
        key=lambda x: _competition_sort_key(
            trade_volume=float(x["trade_volume"]),
            target_trade_volume=float(params["target_trade_volume"]),
            net_profit=float(x["net_profit"]),
            max_drawdown=float(x["max_drawdown"]),
        ),
        reverse=True,
    )[: params["top_k"]]

    return {
        "ok": True,
        "suggestions": suggestions,
        "data": {
            "candles": len(candles),
            "cache_file": cache_path,
            "symbol": params["symbol"],
            "interval": params["interval"],
            "start_time": candles[0].open_time.isoformat(),
            "end_time": candles[-1].close_time.isoformat(),
            "n_samples": n_values,
            "range_candidates": len(range_candidates),
        },
    }


def _normalize_compare_payload(payload: dict[str, Any]) -> dict[str, Any]:
    symbol = str(payload.get("symbol", "BTCUSDT")).upper().strip()
    lookback_days = _safe_int(payload.get("lookback_days", 365), "lookback_days")
    start_time = _parse_optional_datetime(payload.get("start_time"), "start_time")
    end_time = _parse_optional_datetime(payload.get("end_time"), "end_time")
    interval = str(payload.get("interval", "1h")).strip()
    fee_rate = _safe_float(payload.get("fee_rate", 0.0002), "fee_rate")
    slippage = _safe_float(payload.get("slippage", 0.0), "slippage")
    refresh = bool(payload.get("refresh", False))

    if start_time is not None and end_time is not None and start_time >= end_time:
        raise ValueError("start_time must be earlier than end_time")
    if start_time is None and end_time is None and lookback_days <= 0:
        raise ValueError("lookback_days must be > 0")
    if (start_time is None or end_time is None) and lookback_days <= 0:
        raise ValueError("lookback_days must be > 0 when only one boundary time is provided")
    if fee_rate < 0 or slippage < 0:
        raise ValueError("fee_rate/slippage must be >= 0")

    raw_layers = payload.get("layers")
    if not isinstance(raw_layers, list) or not raw_layers:
        raise ValueError("layers must be a non-empty list")

    layers = []
    supported_modes = set(supported_allocation_modes())
    for i, item in enumerate(raw_layers):
        if not isinstance(item, dict):
            raise ValueError(f"layers[{i}] must be an object")
        min_price = _safe_float(item.get("min_price"), f"layers[{i}].min_price")
        max_price = _safe_float(item.get("max_price"), f"layers[{i}].max_price")
        n = _safe_int(item.get("n", item.get("suggest_n")), f"layers[{i}].n")
        notional = _safe_float(item.get("notional"), f"layers[{i}].notional")
        grid_span = str(item.get("grid_span", "-"))
        layer_mode = str(item.get("layer_mode", "equal")).strip().lower()
        if min_price <= 0 or max_price <= 0 or min_price >= max_price:
            raise ValueError(f"layers[{i}] has invalid price range")
        if n <= 0:
            raise ValueError(f"layers[{i}].n must be > 0")
        if notional <= 0:
            raise ValueError(f"layers[{i}].notional must be > 0")
        if layer_mode not in supported_modes:
            raise ValueError(
                f"layers[{i}].layer_mode unsupported: {layer_mode}; "
                f"supported: {','.join(sorted(supported_modes))}"
            )
        layers.append(
            {
                "min_price": min_price,
                "max_price": max_price,
                "n": n,
                "notional": notional,
                "grid_span": grid_span,
                "layer_mode": layer_mode,
            }
        )

    baseline_raw = payload.get("baseline", {})
    if baseline_raw is None:
        baseline_raw = {}
    if not isinstance(baseline_raw, dict):
        raise ValueError("baseline must be object")

    baseline: dict[str, Any] = {
        "n": baseline_raw.get("n"),
        "allocation_mode": baseline_raw.get("allocation_mode"),
    }
    float_keys = [
        "total_buy_notional",
        "net_profit",
        "total_return",
        "annualized_return",
        "max_drawdown",
        "calmar",
        "trade_volume",
        "total_fees",
        "avg_capital_usage",
        "realized_pnl",
        "unrealized_pnl",
    ]
    int_keys = ["trade_count"]
    for key in float_keys:
        if key in baseline_raw and baseline_raw[key] is not None:
            baseline[key] = _safe_float(baseline_raw.get(key), f"baseline.{key}")
    for key in int_keys:
        if key in baseline_raw and baseline_raw[key] is not None:
            baseline[key] = _safe_int(baseline_raw.get(key), f"baseline.{key}")

    return {
        "symbol": symbol,
        "lookback_days": lookback_days,
        "start_time": start_time,
        "end_time": end_time,
        "interval": interval,
        "fee_rate": fee_rate,
        "slippage": slippage,
        "refresh": refresh,
        "layers": layers,
        "baseline": baseline,
    }


def _run_layer_compare(params: dict[str, Any]) -> dict[str, Any]:
    cache_path = str(cache_file_path(params["symbol"], params["interval"], cache_dir="data"))
    candles = load_or_fetch_candles(
        symbol=params["symbol"],
        interval=params["interval"],
        lookback_days=params["lookback_days"],
        cache_dir="data",
        refresh=params["refresh"],
        start_time=params["start_time"],
        end_time=params["end_time"],
    )
    if not candles:
        raise ValueError("No candle data")

    combined_equity = [0.0] * len(candles)
    combined_capital = [0.0] * len(candles)

    total_buy_notional = 0.0
    net_profit = 0.0
    total_fees = 0.0
    trade_count = 0
    trade_volume = 0.0
    realized_pnl = 0.0
    unrealized_pnl = 0.0
    layer_total_n = 0

    for layer in params["layers"]:
        result = run_backtest(
            candles=candles,
            min_price=layer["min_price"],
            max_price=layer["max_price"],
            n=layer["n"],
            total_buy_notional=layer["notional"],
            allocation_mode=layer["layer_mode"],
            fee_rate=params["fee_rate"],
            slippage=params["slippage"],
            capture_trades=False,
            capture_curves=True,
        )
        total_buy_notional += layer["notional"]
        net_profit += result.net_profit
        total_fees += result.total_fees
        trade_count += result.trade_count
        trade_volume += result.trade_volume
        realized_pnl += result.realized_pnl
        unrealized_pnl += result.unrealized_pnl
        layer_total_n += layer["n"]

        equity_curve = result.equity_curve or []
        capital_curve = result.capital_usage_curve or []
        if len(equity_curve) != len(candles) or len(capital_curve) != len(candles):
            raise RuntimeError("Layer curve length mismatch")
        for i, value in enumerate(equity_curve):
            combined_equity[i] += value
        for i, value in enumerate(capital_curve):
            combined_capital[i] += value

    if total_buy_notional <= 0:
        raise ValueError("total layered notional must be > 0")

    total_return = net_profit / total_buy_notional
    backtest_days = (candles[-1].close_time - candles[0].open_time).total_seconds() / 86_400
    if backtest_days > 0 and total_return > -1.0:
        annualized = (1.0 + total_return) ** (365.0 / backtest_days) - 1.0
    else:
        annualized = -1.0 if total_return <= -1.0 else 0.0

    nav = [total_buy_notional + x for x in combined_equity]
    max_drawdown = _series_max_drawdown(nav)
    if max_drawdown > 0:
        calmar = annualized / max_drawdown
    else:
        calmar = math.inf if annualized > 0 else 0.0

    avg_capital_usage = mean(combined_capital) / total_buy_notional if combined_capital else 0.0
    max_capital_usage = max(combined_capital) / total_buy_notional if combined_capital else 0.0

    start_price = candles[0].open
    end_price = candles[-1].close
    underlying_return = (end_price / start_price - 1.0) if start_price > 0 else 0.0

    layered = {
        "layer_count": len(params["layers"]),
        "layer_total_n": layer_total_n,
        "total_buy_notional": total_buy_notional,
        "net_profit": net_profit,
        "total_return": total_return,
        "annualized_return": annualized,
        "max_drawdown": max_drawdown,
        "calmar": _safe_metric(calmar),
        "trade_count": trade_count,
        "trade_volume": trade_volume,
        "total_fees": total_fees,
        "avg_capital_usage": avg_capital_usage,
        "max_capital_usage": max_capital_usage,
        "realized_pnl": realized_pnl,
        "unrealized_pnl": unrealized_pnl,
        "start_time": candles[0].open_time.isoformat(),
        "end_time": candles[-1].close_time.isoformat(),
        "start_price": start_price,
        "end_price": end_price,
        "underlying_return": underlying_return,
    }

    baseline = dict(params.get("baseline", {}))
    comparison: dict[str, Any] = {}
    compare_keys = [
        "net_profit",
        "total_return",
        "annualized_return",
        "max_drawdown",
        "calmar",
        "trade_count",
        "trade_volume",
        "total_fees",
        "avg_capital_usage",
        "realized_pnl",
        "unrealized_pnl",
    ]
    for key in compare_keys:
        base = baseline.get(key)
        lay = layered.get(key)
        if base is None or lay is None:
            comparison[f"{key}_delta"] = None
            continue
        try:
            comparison[f"{key}_delta"] = float(lay) - float(base)
        except (TypeError, ValueError):
            comparison[f"{key}_delta"] = None

    return {
        "ok": True,
        "baseline": baseline,
        "layered": layered,
        "comparison": comparison,
        "data": {
            "candles": len(candles),
            "cache_file": cache_path,
            "symbol": params["symbol"],
            "interval": params["interval"],
            "lookback_days": params["lookback_days"],
            "start_time": candles[0].open_time.isoformat(),
            "end_time": candles[-1].close_time.isoformat(),
        },
    }


def _create_job(params: dict[str, Any]) -> str:
    job_id = uuid.uuid4().hex
    now = time.time()
    with JOBS_LOCK:
        # Keep job store bounded.
        expired_ids = []
        for jid, job in JOBS.items():
            age = now - float(job.get("updated_at", now))
            if age > 6 * 3600 and job.get("status") in {"done", "failed"}:
                expired_ids.append(jid)
        for jid in expired_ids:
            JOBS.pop(jid, None)

        JOBS[job_id] = {
            "job_id": job_id,
            "status": "queued",
            "created_at": now,
            "updated_at": now,
            "progress": 0.0,
            "processed": 0,
            "total": 0,
            "eta_seconds": None,
            "message": "任务排队中",
            "params": params,
            "result": None,
            "error": None,
        }
    return job_id


def _update_job(job_id: str, **updates: Any) -> None:
    with JOBS_LOCK:
        current = JOBS.get(job_id)
        if not current:
            return
        current.update(updates)
        current["updated_at"] = time.time()


def _get_job_snapshot(job_id: str) -> dict[str, Any] | None:
    with JOBS_LOCK:
        job = JOBS.get(job_id)
        if not job:
            return None
        # shallow copy is enough for response rendering
        return dict(job)


def _normalize_payload(payload: dict[str, Any]) -> dict[str, Any]:
    calc_mode = str(payload.get("calc_mode", "optimize")).strip().lower()
    if calc_mode not in {"optimize", "fixed"}:
        raise ValueError("calc_mode must be optimize or fixed")
    symbol = str(payload.get("symbol", "BTCUSDT")).upper().strip()
    min_price = _safe_float(payload.get("min_price"), "min_price")
    max_price = _safe_float(payload.get("max_price"), "max_price")
    total_buy_notional = _safe_float(payload.get("total_buy_notional", 0), "total_buy_notional")
    fixed_n = _safe_int(payload.get("fixed_n", 0), "fixed_n")
    fixed_per_grid_notional = _safe_float(
        payload.get("fixed_per_grid_notional", 0), "fixed_per_grid_notional"
    )
    lookback_days = _safe_int(payload.get("lookback_days", 365), "lookback_days")
    start_time = _parse_optional_datetime(payload.get("start_time"), "start_time")
    end_time = _parse_optional_datetime(payload.get("end_time"), "end_time")
    interval = str(payload.get("interval", "1h")).strip()
    n_min = _safe_int(payload.get("n_min", 5), "n_min")
    n_max = _safe_int(payload.get("n_max", 200), "n_max")
    fee_rate = _safe_float(payload.get("fee_rate", 0.0002), "fee_rate")
    slippage = _safe_float(payload.get("slippage", 0.0), "slippage")
    funding_buffer = _safe_float(payload.get("funding_buffer", 0.0), "funding_buffer")
    target_trade_volume = _safe_float(payload.get("target_trade_volume", 0.0), "target_trade_volume")
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
    if objective not in {
        "calmar",
        "net_profit",
        "total_return",
        "annualized_return",
        "competition_volume",
    }:
        raise ValueError("unsupported objective")
    min_trade_count = _safe_int(payload.get("min_trade_count", 0), "min_trade_count")
    min_avg_capital_usage = _safe_float(
        payload.get("min_avg_capital_usage", 0.0), "min_avg_capital_usage"
    )

    if min_price >= max_price:
        raise ValueError("min_price must be less than max_price")
    if start_time is not None and end_time is not None and start_time >= end_time:
        raise ValueError("start_time must be earlier than end_time")
    if start_time is None and end_time is None and lookback_days <= 0:
        raise ValueError("lookback_days must be > 0")
    if (start_time is None or end_time is None) and lookback_days <= 0:
        raise ValueError("lookback_days must be > 0 when only one boundary time is provided")
    if fee_rate < 0 or slippage < 0 or funding_buffer < 0:
        raise ValueError("fee/slippage/funding_buffer must be >= 0")
    if min_trade_count < 0:
        raise ValueError("min_trade_count must be >= 0")
    if min_avg_capital_usage < 0 or min_avg_capital_usage > 1:
        raise ValueError("min_avg_capital_usage must be in [0,1]")
    if target_trade_volume < 0:
        raise ValueError("target_trade_volume must be >= 0")

    if calc_mode == "optimize":
        if total_buy_notional <= 0:
            raise ValueError("total_buy_notional must be > 0")
        if n_min <= 0 or n_max <= 0 or n_min > n_max:
            raise ValueError("invalid n range")
        if not allocation_modes:
            raise ValueError("allocation_modes cannot be empty")
        supported = set(supported_allocation_modes())
        unknown = [x for x in allocation_modes if x not in supported]
        if unknown:
            raise ValueError(
                f"unsupported allocation_modes: {','.join(unknown)}; "
                f"supported: {','.join(sorted(supported))}"
            )
        if top_k <= 0:
            raise ValueError("top_k must be > 0")
    else:
        if fixed_n <= 0:
            raise ValueError("fixed_n must be > 0")
        if fixed_per_grid_notional <= 0:
            raise ValueError("fixed_per_grid_notional must be > 0")
        total_buy_notional = fixed_n * fixed_per_grid_notional
        n_min = fixed_n
        n_max = fixed_n
        allocation_modes = ["equal"]
        if top_k <= 0:
            top_k = 1

    return {
        "calc_mode": calc_mode,
        "symbol": symbol,
        "min_price": min_price,
        "max_price": max_price,
        "total_buy_notional": total_buy_notional,
        "fixed_n": fixed_n,
        "fixed_per_grid_notional": fixed_per_grid_notional,
        "lookback_days": lookback_days,
        "start_time": start_time,
        "end_time": end_time,
        "interval": interval,
        "n_min": n_min,
        "n_max": n_max,
        "fee_rate": fee_rate,
        "slippage": slippage,
        "funding_buffer": funding_buffer,
        "target_trade_volume": target_trade_volume,
        "allocation_modes": allocation_modes,
        "objective": objective,
        "min_trade_count": min_trade_count,
        "min_avg_capital_usage": min_avg_capital_usage,
        "top_k": top_k,
        "refresh": refresh,
    }


def _run_optimizer(
    params: dict[str, Any],
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    cache_path = str(cache_file_path(params["symbol"], params["interval"], cache_dir="data"))
    candles = load_or_fetch_candles(
        symbol=params["symbol"],
        interval=params["interval"],
        lookback_days=params["lookback_days"],
        cache_dir="data",
        refresh=params["refresh"],
        start_time=params["start_time"],
        end_time=params["end_time"],
    )

    def _candidate_payload(item) -> dict[str, Any]:
        target_trade_volume = params["target_trade_volume"]
        if target_trade_volume > 0:
            volume_coverage = item.trade_volume / target_trade_volume
        else:
            volume_coverage = 1.0
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
            "calmar": _safe_metric(item.calmar),
            "total_fees": item.total_fees,
            "trade_count": item.trade_count,
            "trade_volume": item.trade_volume,
            "target_trade_volume": target_trade_volume,
            "volume_coverage": volume_coverage,
            "win_rate": item.win_rate,
            "avg_capital_usage": item.avg_capital_usage,
            "max_capital_usage": item.max_capital_usage,
            "total_buy_notional": sum(item.per_grid_notionals),
            "realized_pnl": item.realized_pnl,
            "unrealized_pnl": item.unrealized_pnl,
            "start_time": item.start_time.isoformat(),
            "end_time": item.end_time.isoformat(),
            "start_price": item.start_price,
            "end_price": item.end_price,
            "underlying_return": item.underlying_return,
            "plan": plan_rows,
        }

    if params["calc_mode"] == "fixed":
        if progress_callback:
            progress_callback({"processed": 0, "total": 1, "status": "started", "n": params["fixed_n"], "mode": "equal"})
        fixed_result = run_backtest(
            candles=candles,
            min_price=params["min_price"],
            max_price=params["max_price"],
            n=params["fixed_n"],
            total_buy_notional=params["total_buy_notional"],
            allocation_mode="equal",
            fee_rate=params["fee_rate"],
            slippage=params["slippage"],
            capture_trades=False,
        )
        if progress_callback:
            progress_callback(
                {"processed": 1, "total": 1, "status": "tested", "n": params["fixed_n"], "mode": "equal"}
            )
        fixed_result.score = objective_value(
            fixed_result,
            params["objective"],
            target_trade_volume=params["target_trade_volume"],
        )

        if fixed_result.trade_count < params["min_trade_count"]:
            return {
                "ok": True,
                "best": None,
                "top": [],
                "plan": [],
                "data": {"candles": len(candles), "cache_file": cache_path},
                "search": {
                    "mode": "fixed",
                    "tested": 1,
                    "skipped_by_cost": 0,
                    "objective": params["objective"],
                    "min_trade_count": params["min_trade_count"],
                    "min_avg_capital_usage": params["min_avg_capital_usage"],
                    "target_trade_volume": params["target_trade_volume"],
                    "min_step_ratio_for_cost": min_step_ratio_for_cost(
                        fee_rate=params["fee_rate"],
                        slippage=params["slippage"],
                        funding_buffer=params["funding_buffer"],
                    ),
                },
                "error": "Fixed result filtered by min_trade_count",
            }

        if fixed_result.avg_capital_usage < params["min_avg_capital_usage"]:
            return {
                "ok": True,
                "best": None,
                "top": [],
                "plan": [],
                "data": {"candles": len(candles), "cache_file": cache_path},
                "search": {
                    "mode": "fixed",
                    "tested": 1,
                    "skipped_by_cost": 0,
                    "objective": params["objective"],
                    "min_trade_count": params["min_trade_count"],
                    "min_avg_capital_usage": params["min_avg_capital_usage"],
                    "target_trade_volume": params["target_trade_volume"],
                    "min_step_ratio_for_cost": min_step_ratio_for_cost(
                        fee_rate=params["fee_rate"],
                        slippage=params["slippage"],
                        funding_buffer=params["funding_buffer"],
                    ),
                },
                "error": "Fixed result filtered by min_avg_capital_usage",
            }

        best = _candidate_payload(fixed_result)
        top = [best]
        tested = 1
        skipped_by_cost = 0
    else:
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
            target_trade_volume=params["target_trade_volume"],
            min_trade_count=params["min_trade_count"],
            min_avg_capital_usage=params["min_avg_capital_usage"],
            top_k=params["top_k"],
            progress_callback=progress_callback,
        )

        if optimization.best is None:
            return {
                "ok": True,
                "best": None,
                "top": [],
                "plan": [],
                "data": {"candles": len(candles), "cache_file": cache_path},
                "search": {
                    "mode": "optimize",
                    "tested": optimization.tested,
                    "skipped_by_cost": optimization.skipped_by_cost,
                    "objective": params["objective"],
                    "min_trade_count": params["min_trade_count"],
                    "min_avg_capital_usage": params["min_avg_capital_usage"],
                    "target_trade_volume": params["target_trade_volume"],
                    "min_step_ratio_for_cost": min_step_ratio_for_cost(
                        fee_rate=params["fee_rate"],
                        slippage=params["slippage"],
                        funding_buffer=params["funding_buffer"],
                    ),
                },
            }

        best = _candidate_payload(optimization.best)
        top = [_candidate_payload(x) for x in optimization.top_results]
        tested = optimization.tested
        skipped_by_cost = optimization.skipped_by_cost

    return {
        "ok": True,
        "best": best,
        "top": top,
        "plan": best["plan"],
        "data": {
            "candles": len(candles),
            "cache_file": cache_path,
            "calc_mode": params["calc_mode"],
            "total_buy_notional": params["total_buy_notional"],
            "start_time": candles[0].open_time.isoformat(),
            "end_time": candles[-1].close_time.isoformat(),
        },
        "search": {
            "mode": params["calc_mode"],
            "tested": tested,
            "skipped_by_cost": skipped_by_cost,
            "objective": params["objective"],
            "min_trade_count": params["min_trade_count"],
            "min_avg_capital_usage": params["min_avg_capital_usage"],
            "target_trade_volume": params["target_trade_volume"],
            "min_step_ratio_for_cost": min_step_ratio_for_cost(
                fee_rate=params["fee_rate"],
                slippage=params["slippage"],
                funding_buffer=params["funding_buffer"],
            ),
        },
    }


def _run_job_worker(job_id: str) -> None:
    snapshot = _get_job_snapshot(job_id)
    if not snapshot:
        return
    params = snapshot["params"]
    start_ts = time.time()
    backtest_start_ts = start_ts

    def _progress(event: dict[str, Any]) -> None:
        nonlocal backtest_start_ts
        processed = int(event.get("processed", 0))
        total = int(event.get("total", 0))
        mode = event.get("mode")
        n = event.get("n")
        status = event.get("status", "")

        if status == "started":
            backtest_start_ts = time.time()

        ratio = 0.0
        if total > 0:
            ratio = min(1.0, max(0.0, processed / total))

        elapsed = max(0.001, time.time() - backtest_start_ts)
        eta_seconds: int | None = None
        if ratio > 0.0001 and ratio < 1.0:
            eta_seconds = int(elapsed * (1.0 - ratio) / ratio)

        progress_value = 0.15 + 0.8 * ratio
        message = f"回测中 {processed}/{total}"
        if n is not None:
            message += f" (N={n}"
            if mode:
                message += f", mode={mode}"
            message += ")"

        _update_job(
            job_id,
            status="running",
            progress=progress_value,
            processed=processed,
            total=total,
            eta_seconds=eta_seconds,
            message=message,
        )

    try:
        _update_job(job_id, status="running", progress=0.05, message="加载K线数据...")
        result = _run_optimizer(params, progress_callback=_progress)
        total_elapsed = int(time.time() - start_ts)
        _update_job(
            job_id,
            status="done",
            progress=1.0,
            processed=result.get("search", {}).get("tested", 0),
            total=result.get("search", {}).get("tested", 0),
            eta_seconds=0,
            message=f"测算完成，用时 {total_elapsed}s",
            result=result,
            error=None,
        )
    except Exception as exc:  # pragma: no cover
        _update_job(
            job_id,
            status="failed",
            progress=1.0,
            eta_seconds=None,
            message="测算失败",
            error=f"{type(exc).__name__}: {exc}",
        )


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
        if self.path == "/api/symbols":
            symbols, source = _load_symbol_list(refresh=False)
            self._send_json(
                {
                    "ok": True,
                    "symbols": symbols,
                    "count": len(symbols),
                    "source": source,
                },
                status=HTTPStatus.OK,
            )
            return
        if self.path.startswith("/api/job/"):
            job_id = self.path.split("/api/job/", 1)[1].strip()
            if not job_id:
                self._send_json({"ok": False, "error": "Missing job id"}, status=400)
                return
            snapshot = _get_job_snapshot(job_id)
            if not snapshot:
                self._send_json({"ok": False, "error": "Job not found"}, status=404)
                return
            payload = {
                "ok": True,
                "job_id": snapshot["job_id"],
                "status": snapshot["status"],
                "progress": snapshot.get("progress", 0.0),
                "processed": snapshot.get("processed", 0),
                "total": snapshot.get("total", 0),
                "eta_seconds": snapshot.get("eta_seconds"),
                "message": snapshot.get("message"),
                "error": snapshot.get("error"),
                "result": snapshot.get("result") if snapshot.get("status") == "done" else None,
            }
            self._send_json(payload, status=HTTPStatus.OK)
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
        if self.path == "/api/symbols":
            body = json.dumps({"ok": True}, ensure_ascii=False).encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            return
        self.send_response(HTTPStatus.NOT_FOUND)
        self.end_headers()

    def do_POST(self) -> None:  # noqa: N802
        if self.path not in {"/api/optimize", "/api/layer_compare", "/api/suggest_range"}:
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

        if self.path == "/api/optimize":
            try:
                params = _normalize_payload(payload)
            except ValueError as exc:
                self._send_json({"ok": False, "error": str(exc)}, status=400)
                return
            job_id = _create_job(params)
            worker = threading.Thread(target=_run_job_worker, args=(job_id,), daemon=True)
            worker.start()
            self._send_json({"ok": True, "job_id": job_id}, status=202)
            return

        if self.path == "/api/layer_compare":
            try:
                params = _normalize_compare_payload(payload)
            except ValueError as exc:
                self._send_json({"ok": False, "error": str(exc)}, status=400)
                return
            try:
                result = _run_layer_compare(params)
            except ValueError as exc:
                self._send_json({"ok": False, "error": str(exc)}, status=400)
                return
            except Exception as exc:  # pragma: no cover
                self._send_json({"ok": False, "error": f"{type(exc).__name__}: {exc}"}, status=500)
                return
            self._send_json(result, status=200)
            return

        if self.path == "/api/suggest_range":
            try:
                params = _normalize_suggest_payload(payload)
            except ValueError as exc:
                self._send_json({"ok": False, "error": str(exc)}, status=400)
                return
            try:
                result = _run_range_suggestion(params)
            except ValueError as exc:
                self._send_json({"ok": False, "error": str(exc)}, status=400)
                return
            except Exception as exc:  # pragma: no cover
                self._send_json({"ok": False, "error": f"{type(exc).__name__}: {exc}"}, status=500)
                return
            self._send_json(result, status=200)
            return

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
