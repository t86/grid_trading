from __future__ import annotations

import json
import math
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests

from .audit import build_audit_paths, read_trade_audit_rows, trade_row_time_ms
from .competition_board import _fetch_symbol_close_price_usdt
from .monitor import runner_control_path_for_symbol, summarize_user_trades

UTC_PLUS_8 = timezone(timedelta(hours=8))
MASTER_SPRINT_CACHE_PATH = Path("output/master_sprint_snapshot.json")
MASTER_SPRINT_REFRESH_HOUR_CST = 15
MASTER_SPRINT_API_CACHE: dict[str, tuple[float, bytes]] = {}
MASTER_SPRINT_API_CACHE_LOCK = threading.Lock()
MASTER_SPRINT_MEMORY_CACHE: dict[str, Any] = {"loaded_at": 0.0, "data": None}
MASTER_SPRINT_MEMORY_CACHE_LOCK = threading.Lock()


@dataclass(frozen=True)
class SprintBoardConfig:
    slug: str
    label: str
    phase_label: str
    resource_id: int | None
    referer: str
    start_at: str
    end_at: str
    entry_threshold: float
    threshold_unit: str
    leaderboard_unit: str
    symbols: tuple[str, ...]
    reward_pools: tuple[dict[str, Any], ...] = ()


# Week-2 resource ids are kept here so we can adjust them in one place if Binance
# changes the page wiring again.
SPRINT_BOARD_CONFIGS: tuple[SprintBoardConfig, ...] = (
    SprintBoardConfig(
        slug="tradfi_week2",
        label="TradFi 大师赛",
        phase_label="第 2 期",
        resource_id=int(os.environ.get("GRID_MASTER_SPRINT_TRADFI_RESOURCE_ID") or 52051),
        referer="https://www.binance.com/zh-CN/activity/trading-competition/tradfisprint-2026wk2-new?ref=YEK2JZJT",
        start_at="2026-04-28T08:00:00+08:00",
        end_at="2026-05-05T07:59:00+08:00",
        entry_threshold=500.0,
        threshold_unit="USDT",
        leaderboard_unit="USDT",
        symbols=("BZUSDT", "CLUSDT"),
        reward_pools=(
            {
                "label": "低于 70 亿合并交易量",
                "value": 120000.0,
                "unit": "USDT",
                "note": "TradFi 周奖池低档",
            },
            {
                "label": "达到 70 亿合并交易量",
                "value": 270000.0,
                "unit": "USDT",
                "note": "TradFi 周奖池高档",
            },
        ),
    ),
    SprintBoardConfig(
        slug="um_week2",
        label="UM 大师赛",
        phase_label="第 2 期",
        resource_id=int(os.environ.get("GRID_MASTER_SPRINT_UM_RESOURCE_ID") or 52057),
        referer="https://www.binance.com/zh-CN/activity/trading-competition/umsprint-2026wk2-new?ref=YEK2JZJT",
        start_at="2026-04-28T08:00:00+08:00",
        end_at="2026-05-05T07:59:00+08:00",
        entry_threshold=500.0,
        threshold_unit="USDT",
        leaderboard_unit="USDT",
        symbols=("ETHUSDC", "BTCUSDC"),
        reward_pools=(
            {
                "label": "UM 周奖池",
                "value": 330.0,
                "unit": "BNB",
                "note": "按当前 BNBUSDT 价格折算",
            },
        ),
    ),
    SprintBoardConfig(
        slug="altcoins_week2",
        label="Altcoins 大师赛",
        phase_label="第 2 期",
        resource_id=int(os.environ.get("GRID_MASTER_SPRINT_ALTCOINS_RESOURCE_ID") or 52057),
        referer="https://www.binance.com/zh-CN/activity/trading-competition/altcoinsprint-2026wk2",
        start_at="2026-04-28T08:00:00+08:00",
        end_at="2026-05-05T07:59:00+08:00",
        entry_threshold=500.0,
        threshold_unit="USDT",
        leaderboard_unit="USDT",
        symbols=("TRUMPUSDC", "ORDIUSDC", "IPUSDC"),
        reward_pools=(
            {
                "label": "PUMP 周奖池",
                "value": 50000000.0,
                "unit": "PUMP",
                "note": "需要 PUMPUSDT 价格后才能折 U",
            },
            {
                "label": "BANK 周奖池",
                "value": 2500000.0,
                "unit": "BANK",
                "note": "需要 BANKUSDT 价格后才能折 U",
            },
        ),
    ),
)

MASTER_SPRINT_PAGE = r"""
<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>大师赛追踪看板</title>
  <style>
    :root {
      color-scheme: light;
      --bg: #f4efe6;
      --panel: rgba(255,255,255,0.92);
      --panel-strong: #fffdfa;
      --line: #d9cfbf;
      --text: #241b14;
      --muted: #6e6256;
      --accent: #a54f2b;
      --accent-soft: #f0d7c6;
      --good: #1c7c54;
      --warn: #9b6a18;
      --bad: #a3372c;
      --shadow: 0 18px 44px rgba(83, 54, 29, 0.08);
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: "SF Pro Text", "PingFang SC", "Hiragino Sans GB", "Microsoft YaHei", sans-serif;
      color: var(--text);
      background:
        radial-gradient(circle at top left, rgba(232, 180, 127, 0.26), transparent 34%),
        linear-gradient(180deg, #f8f3eb 0%, var(--bg) 48%, #efe5d7 100%);
    }
    .wrap {
      max-width: 1380px;
      margin: 0 auto;
      padding: 28px 20px 36px;
    }
    .hero,
    .board {
      background: var(--panel);
      border: 1px solid rgba(217, 207, 191, 0.9);
      border-radius: 8px;
      box-shadow: var(--shadow);
      backdrop-filter: blur(8px);
    }
    .hero {
      padding: 22px 24px;
      margin-bottom: 18px;
    }
    .hero-head {
      display: flex;
      justify-content: space-between;
      gap: 16px;
      align-items: flex-start;
      flex-wrap: wrap;
    }
    .hero h1 {
      margin: 0;
      font-size: 30px;
      line-height: 1.1;
    }
    .hero p {
      margin: 8px 0 0;
      color: var(--muted);
      line-height: 1.65;
      max-width: 860px;
      font-size: 14px;
    }
    .toolbar {
      display: flex;
      gap: 10px;
      align-items: center;
      flex-wrap: wrap;
    }
    .toolbar button,
    .toolbar a {
      appearance: none;
      border: 1px solid var(--line);
      background: var(--panel-strong);
      color: var(--text);
      border-radius: 8px;
      padding: 10px 14px;
      text-decoration: none;
      font-size: 13px;
      font-weight: 700;
      cursor: pointer;
    }
    .toolbar .primary {
      background: var(--accent);
      border-color: var(--accent);
      color: #fff;
    }
    .meta {
      margin-top: 14px;
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 12px;
    }
    .meta-card {
      padding: 14px 16px;
      border-radius: 8px;
      background: rgba(255,255,255,0.74);
      border: 1px solid rgba(217, 207, 191, 0.9);
    }
    .meta-card .label {
      color: var(--muted);
      font-size: 12px;
    }
    .meta-card .value {
      margin-top: 6px;
      font-size: 22px;
      font-weight: 700;
    }
    .boards {
      display: grid;
      gap: 16px;
    }
    .board {
      padding: 20px 22px;
    }
    .board-head {
      display: flex;
      justify-content: space-between;
      gap: 16px;
      flex-wrap: wrap;
      align-items: flex-start;
    }
    .board-title h2 {
      margin: 0;
      font-size: 22px;
    }
    .board-title p {
      margin: 6px 0 0;
      color: var(--muted);
      font-size: 13px;
    }
    .status {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      border-radius: 999px;
      padding: 7px 11px;
      font-size: 12px;
      font-weight: 700;
      background: var(--accent-soft);
      color: #6d351b;
    }
    .status.good { background: rgba(28,124,84,0.12); color: var(--good); }
    .status.warn { background: rgba(155,106,24,0.12); color: var(--warn); }
    .status.bad { background: rgba(163,55,44,0.12); color: var(--bad); }
    .summary-grid {
      margin-top: 16px;
      display: grid;
      grid-template-columns: repeat(5, minmax(0, 1fr));
      gap: 12px;
    }
    .summary-card {
      border: 1px solid rgba(217, 207, 191, 0.9);
      border-radius: 8px;
      padding: 14px 16px;
      background: rgba(255,255,255,0.82);
    }
    .summary-card .label {
      color: var(--muted);
      font-size: 12px;
    }
    .summary-card .value {
      margin-top: 7px;
      font-size: 24px;
      font-weight: 700;
    }
    .summary-card .sub {
      margin-top: 7px;
      color: var(--muted);
      font-size: 12px;
      line-height: 1.55;
    }
    .detail-grid {
      margin-top: 16px;
      display: grid;
      grid-template-columns: 1.15fr 0.85fr;
      gap: 16px;
    }
    table {
      width: 100%;
      border-collapse: collapse;
      font-size: 13px;
    }
    th, td {
      padding: 10px 8px;
      border-bottom: 1px solid rgba(217, 207, 191, 0.9);
      text-align: left;
      vertical-align: top;
    }
    th {
      color: var(--muted);
      width: 168px;
      font-weight: 700;
    }
    .note {
      margin-top: 10px;
      color: var(--muted);
      font-size: 12px;
      line-height: 1.6;
    }
    .empty {
      padding: 32px;
      text-align: center;
      color: var(--muted);
      border: 1px dashed var(--line);
      border-radius: 8px;
      background: rgba(255,255,255,0.7);
    }
    @media (max-width: 1080px) {
      .meta,
      .summary-grid,
      .detail-grid {
        grid-template-columns: 1fr 1fr;
      }
    }
    @media (max-width: 720px) {
      .meta,
      .summary-grid,
      .detail-grid {
        grid-template-columns: 1fr;
      }
      .hero h1 {
        font-size: 26px;
      }
    }
  </style>
</head>
<body>
  <div class="wrap">
    <section class="hero">
      <div class="hero-head">
        <div>
          <h1>大师赛追踪看板</h1>
          <p>单独追踪 UM、Altcoins、TradFi 三个分赛的官方门槛、前 500 榜单强度，以及 111 本机对应策略在比赛时间窗内的累计成交额。页面默认读取缓存，到了北京时间 15:00 之后会自动补当天刷新。</p>
        </div>
        <div class="toolbar">
          <button id="refresh_btn" class="primary">立即刷新</button>
          <a href="/monitor">监控页</a>
          <a href="/strategies">策略总览</a>
        </div>
      </div>
      <div id="meta" class="meta"></div>
    </section>
    <div id="boards" class="boards">
      <div class="empty">正在加载数据...</div>
    </div>
  </div>

  <script>
    const metaEl = document.getElementById("meta");
    const boardsEl = document.getElementById("boards");
    const refreshBtn = document.getElementById("refresh_btn");

    function escapeHtml(value) {
      return String(value ?? "")
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/\"/g, "&quot;");
    }

    function fmtNum(value, digits = 2) {
      const num = Number(value);
      if (!Number.isFinite(num)) return "--";
      return num.toLocaleString("zh-CN", {
        minimumFractionDigits: digits,
        maximumFractionDigits: digits,
      });
    }

    function fmtTs(value) {
      if (!value) return "--";
      const dt = new Date(value);
      return Number.isNaN(dt.getTime())
        ? String(value)
        : dt.toLocaleString("zh-CN", { hour12: false });
    }

    function statusClass(status) {
      if (status === "ok") return "good";
      if (status === "partial") return "warn";
      return "bad";
    }

    function validRewardEstimates(estimates) {
      return (Array.isArray(estimates) ? estimates : [])
        .filter((item) => Number.isFinite(Number(item.reward_per_10k_volume_usdt)));
    }

    function formatBestRewardPer10k(estimates) {
      const rows = validRewardEstimates(estimates);
      if (!rows.length) return "--";
      const total = rows.reduce((acc, item) => acc + Number(item.reward_per_10k_volume_usdt || 0), 0);
      return `${fmtNum(total, 4)} U`;
    }

    function formatRewardEstimateSub(estimates) {
      const rows = Array.isArray(estimates) ? estimates : [];
      const valid = validRewardEstimates(rows);
      if (valid.length > 1) {
        return valid.map((item) => `${item.label}: ${fmtNum(item.reward_per_10k_volume_usdt, 4)}U / 万U`).join(" · ");
      }
      if (valid.length === 1) {
        const item = valid[0];
        const price = Number.isFinite(Number(item.reward_price_usdt))
          ? `现价 ${fmtNum(item.reward_price_usdt, 4)}U`
          : "";
        return [item.pool_text || item.label || "奖池", price].filter(Boolean).join(" · ");
      }
      return rows.length ? "缺少奖励币价格或官方前 500 总量，暂无法折 U。" : "未配置奖池。";
    }

    function formatRewardEstimateLines(estimates) {
      const rows = Array.isArray(estimates) ? estimates : [];
      if (!rows.length) return "--";
      return rows.map((item) => {
        const per10k = Number.isFinite(Number(item.reward_per_10k_volume_usdt))
          ? `${fmtNum(item.reward_per_10k_volume_usdt, 4)} U / 万U`
          : "暂无法折 U";
        const price = Number.isFinite(Number(item.reward_price_usdt))
          ? `，价格 ${fmtNum(item.reward_price_usdt, 4)}U`
          : "";
        const note = item.note ? `，${escapeHtml(item.note)}` : "";
        return `<div>${escapeHtml(item.label || "奖池")}：${escapeHtml(item.pool_text || "--")} => ${escapeHtml(per10k)}${escapeHtml(price)}${note}</div>`;
      }).join("");
    }

    function renderMeta(snapshot) {
      const items = [
        { label: "刷新状态", value: snapshot.refresh_status_text || "--" },
        { label: "缓存时间", value: snapshot.refreshed_at_cst || "--" },
        { label: "官方抓取", value: `${snapshot.official_success_count || 0} / ${snapshot.competition_count || 0}` },
        { label: "下一次目标刷新", value: snapshot.next_refresh_target_cst || "--" },
      ];
      metaEl.innerHTML = items.map((item) => `
        <div class="meta-card">
          <div class="label">${escapeHtml(item.label)}</div>
          <div class="value">${escapeHtml(item.value)}</div>
        </div>
      `).join("");
    }

    function renderSymbolTable(symbols) {
      const rows = Array.isArray(symbols) ? symbols : [];
      if (!rows.length) {
        return '<div class="empty">当前没有读到本地成交记录。</div>';
      }
      return `
        <table>
          <thead>
            <tr>
              <th>交易对</th>
              <th>累计成交额</th>
              <th>成交笔数</th>
              <th>最近成交</th>
            </tr>
          </thead>
          <tbody>
            ${rows.map((row) => `
              <tr>
                <td>${escapeHtml(row.symbol || "--")}</td>
                <td>${escapeHtml(fmtNum(row.volume, 2))}</td>
                <td>${escapeHtml(fmtNum(row.trade_count, 0))}</td>
                <td>${escapeHtml(row.last_trade_time_cst || "--")}</td>
              </tr>
            `).join("")}
          </tbody>
        </table>
      `;
    }

    function renderBoard(item) {
      const official = item.official || {};
      const local = item.local || {};
      const officialError = official.error
        ? `<div class="note">官方榜单抓取失败：${escapeHtml(official.error)}</div>`
        : "";
      const assumption = item.resource_hint ? `<div class="note">${escapeHtml(item.resource_hint)}</div>` : "";
      return `
        <section class="board">
          <div class="board-head">
            <div class="board-title">
              <h2>${escapeHtml(item.label || item.slug || "--")}</h2>
              <p>${escapeHtml(item.phase_label || "--")} · ${escapeHtml(item.window_text || "--")} · 统计符号：${escapeHtml((item.symbols || []).join(" + "))}</p>
            </div>
            <div class="status ${statusClass(item.status)}">${escapeHtml(item.status_text || "--")}</div>
          </div>
          <div class="summary-grid">
            <div class="summary-card">
              <div class="label">入门门槛</div>
              <div class="value">${escapeHtml(fmtNum(item.entry_threshold, 0))}</div>
              <div class="sub">${escapeHtml(item.threshold_unit || "")}</div>
            </div>
            <div class="summary-card">
              <div class="label">前 500 名累计成交额</div>
              <div class="value">${escapeHtml(fmtNum(official.top500_total_volume, 2))}</div>
              <div class="sub">资源 ID ${escapeHtml(String(item.resource_id || "--"))} · 官方更新时间 ${escapeHtml(official.updated_at_cst || "--")}</div>
            </div>
            <div class="summary-card">
              <div class="label">第 500 名门槛</div>
              <div class="value">${escapeHtml(fmtNum(official.rank_500_value, 2))}</div>
              <div class="sub">${escapeHtml(item.leaderboard_unit || "")}</div>
            </div>
            <div class="summary-card">
              <div class="label">本机当前累计成交额</div>
              <div class="value">${escapeHtml(fmtNum(local.total_volume, 2))}</div>
              <div class="sub">距离入门门槛 ${escapeHtml(fmtNum(local.distance_to_entry_threshold, 2))} ${escapeHtml(item.threshold_unit || "")}</div>
            </div>
            <div class="summary-card">
              <div class="label">每万 U 预估奖励</div>
              <div class="value">${escapeHtml(formatBestRewardPer10k(item.reward_estimates || []))}</div>
              <div class="sub">${escapeHtml(formatRewardEstimateSub(item.reward_estimates || []))}</div>
            </div>
          </div>
          <div class="detail-grid">
            <div class="summary-card">
              <div class="label">本机符号拆分</div>
              <div class="sub">只统计比赛时间窗内本地审计日志里的成交额。</div>
              <div style="margin-top:10px">${renderSymbolTable(local.symbols || [])}</div>
            </div>
            <div class="summary-card">
              <table>
                <tbody>
                  <tr><th>比赛开始</th><td>${escapeHtml(item.start_at_cst || "--")}</td></tr>
                  <tr><th>比赛结束</th><td>${escapeHtml(item.end_at_cst || "--")}</td></tr>
                  <tr><th>官方榜单人数</th><td>${escapeHtml(fmtNum(official.total_ranked_users, 0))}</td></tr>
                  <tr><th>拉取到的榜单条数</th><td>${escapeHtml(fmtNum(official.rows_fetched, 0))}</td></tr>
                  <tr><th>最近本机成交</th><td>${escapeHtml(local.last_trade_time_cst || "--")}</td></tr>
                  <tr><th>本机总成交笔数</th><td>${escapeHtml(fmtNum(local.trade_count, 0))}</td></tr>
                  <tr><th>奖池估算</th><td>${formatRewardEstimateLines(item.reward_estimates || [])}</td></tr>
                </tbody>
              </table>
              ${officialError}
              ${assumption}
            </div>
          </div>
        </section>
      `;
    }

    async function loadSnapshot(force = false) {
      boardsEl.innerHTML = '<div class="empty">正在加载数据...</div>';
      try {
        const resp = await fetch(`/api/master_sprint_board${force ? "?refresh=1" : ""}`);
        const data = await resp.json();
        if (!resp.ok || !data.ok) {
          throw new Error(data.error || `HTTP ${resp.status}`);
        }
        renderMeta(data.snapshot || {});
        const competitions = Array.isArray((data.snapshot || {}).competitions) ? data.snapshot.competitions : [];
        boardsEl.innerHTML = competitions.length
          ? competitions.map(renderBoard).join("")
          : '<div class="empty">当前没有可展示的分赛配置。</div>';
      } catch (err) {
        boardsEl.innerHTML = `<div class="empty">加载失败：${escapeHtml(err)}</div>`;
      }
    }

    refreshBtn.addEventListener("click", () => loadSnapshot(true));
    loadSnapshot(false);
  </script>
</body>
</html>
"""


def _read_json_file(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return default
    return payload


def _write_json_file(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _parse_iso_datetime(value: Any) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None


def _format_number(value: float | int | None, digits: int = 2) -> str:
    if value is None:
        return "--"
    return f"{float(value):,.{digits}f}"


def _format_cst_label(value: datetime | None) -> str:
    if value is None:
        return ""
    return value.astimezone(UTC_PLUS_8).strftime("%Y-%m-%d %H:%M")


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip().replace(",", "")
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _reward_price_usdt(unit: str, at_time: datetime) -> float | None:
    normalized = str(unit or "").strip().upper()
    if not normalized or "/" in normalized:
        return None
    if normalized in {"USD", "USDT", "USDC"}:
        return 1.0
    return _fetch_symbol_close_price_usdt(normalized, at_time)


def _reward_estimates(config: SprintBoardConfig, official: dict[str, Any], now: datetime) -> list[dict[str, Any]]:
    top500_volume = _safe_float(official.get("top500_total_volume"))
    estimates: list[dict[str, Any]] = []
    for pool in config.reward_pools:
        if not isinstance(pool, dict):
            continue
        value = _safe_float(pool.get("value"))
        unit = str(pool.get("unit") or "").strip().upper()
        price_usdt = _reward_price_usdt(unit, now) if value is not None else None
        reward_value_usdt = value * price_usdt if value is not None and price_usdt is not None else None
        reward_per_10k_volume_usdt = (
            reward_value_usdt / top500_volume * 10000.0
            if reward_value_usdt is not None and top500_volume is not None and top500_volume > 0
            else None
        )
        estimates.append(
            {
                "label": str(pool.get("label") or unit or "奖池").strip(),
                "pool_value": value,
                "pool_unit": unit,
                "pool_text": (
                    f"{_format_number(value, 2)} {unit}".strip()
                    if value is not None
                    else str(pool.get("text") or "").strip()
                ),
                "reward_price_usdt": price_usdt,
                "reward_value_usdt": reward_value_usdt,
                "top500_total_volume": top500_volume,
                "reward_per_10k_volume_usdt": reward_per_10k_volume_usdt,
                "note": str(pool.get("note") or "").strip(),
            }
        )
    return estimates


def _refresh_target_for(now: datetime) -> datetime:
    cst_now = now.astimezone(UTC_PLUS_8)
    return cst_now.replace(
        hour=MASTER_SPRINT_REFRESH_HOUR_CST,
        minute=0,
        second=0,
        microsecond=0,
    )


def _next_refresh_target_for(now: datetime) -> datetime:
    current_target = _refresh_target_for(now)
    if now.astimezone(UTC_PLUS_8) < current_target:
        return current_target
    return current_target + timedelta(days=1)


def _cached_snapshot_due(snapshot: dict[str, Any] | None, now: datetime) -> bool:
    if not isinstance(snapshot, dict):
        return True
    refreshed_at = _parse_iso_datetime(snapshot.get("refreshed_at_utc"))
    if refreshed_at is None:
        return True
    current_target = _refresh_target_for(now)
    if now.astimezone(UTC_PLUS_8) >= current_target:
        return refreshed_at.astimezone(UTC_PLUS_8) < current_target
    previous_target = current_target - timedelta(days=1)
    return refreshed_at.astimezone(UTC_PLUS_8) < previous_target


def _slugify_symbol(symbol: str) -> str:
    out = "".join(char.lower() if char.isalnum() else "_" for char in str(symbol or "").strip())
    return out.strip("_") or "symbol"


def _symbol_events_path(symbol: str) -> Path:
    control_path = runner_control_path_for_symbol(symbol)
    control_payload = _read_json_file(control_path, {})
    if isinstance(control_payload, dict):
        summary_jsonl = str(control_payload.get("summary_jsonl") or "").strip()
        if summary_jsonl:
            return Path(summary_jsonl)
    return Path(f"output/{_slugify_symbol(symbol)}_loop_events.jsonl")


def _trade_notional_rows_for_symbol(
    symbol: str,
    *,
    start_ms: int,
    end_ms: int,
) -> list[dict[str, Any]]:
    audit_paths = build_audit_paths(_symbol_events_path(symbol))
    return read_trade_audit_rows(
        audit_paths["trade_audit"],
        limit=0,
        predicate=lambda row: start_ms <= trade_row_time_ms(row) < end_ms,
    )


def _local_symbol_metrics(symbol: str, *, start_at: datetime, end_at: datetime) -> dict[str, Any]:
    start_ms = int(start_at.astimezone(timezone.utc).timestamp() * 1000)
    end_ms = int(end_at.astimezone(timezone.utc).timestamp() * 1000)
    rows = _trade_notional_rows_for_symbol(symbol, start_ms=start_ms, end_ms=end_ms)
    summary = summarize_user_trades(rows)
    last_trade_ms = max((trade_row_time_ms(row) for row in rows), default=0)
    last_trade_dt = datetime.fromtimestamp(last_trade_ms / 1000.0, tz=timezone.utc) if last_trade_ms > 0 else None
    return {
        "symbol": symbol,
        "volume": float(summary.get("gross_notional") or 0.0),
        "trade_count": len(rows),
        "last_trade_time_utc": last_trade_dt.isoformat() if last_trade_dt is not None else "",
        "last_trade_time_cst": _format_cst_label(last_trade_dt),
    }


def _fetch_board_rows(resource_id: int, referer: str, *, max_rows: int = 500) -> dict[str, Any]:
    headers = {
        "Content-Type": "application/json",
        "Origin": "https://www.binance.com",
        "Referer": referer,
        "User-Agent": os.environ.get(
            "GRID_COMPETITION_BOARD_UA",
            (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36"
            ),
        ),
    }
    endpoint = "https://www.binance.com/bapi/growth/v1/friendly/growth-paas/resource/summary/list"
    page_size = 100
    page_count = max(1, math.ceil(max_rows / page_size))

    def fetch_page(page_index: int) -> dict[str, Any]:
        response = requests.post(
            endpoint,
            headers=headers,
            data=json.dumps(
                {
                    "resourceId": int(resource_id),
                    "leaderboardType": "USER",
                    "pageIndex": int(page_index),
                    "pageSize": int(page_size),
                }
            ),
            timeout=20,
        )
        response.raise_for_status()
        payload = response.json()
        if str(payload.get("code")) != "000000":
            raise RuntimeError(
                f"leaderboard fetch failed for resource {resource_id}: "
                f"{payload.get('code')} {payload.get('message')}"
            )
        return payload

    payloads: dict[int, dict[str, Any]] = {}
    with ThreadPoolExecutor(max_workers=min(5, page_count)) as executor:
        future_map = {executor.submit(fetch_page, page): page for page in range(1, page_count + 1)}
        for future, page in ((future, future_map[future]) for future in future_map):
            payloads[page] = future.result()

    rows: list[dict[str, Any]] = []
    updated_time_ms = 0
    total_ranked_users = 0
    for page in range(1, page_count + 1):
        payload = payloads[page]
        data = payload.get("data") if isinstance(payload.get("data"), dict) else {}
        summary = data.get("resourceSummaryList") if isinstance(data.get("resourceSummaryList"), dict) else {}
        if not updated_time_ms:
            updated_time_ms = int(data.get("updatedTime") or 0)
        if not total_ranked_users:
            total_ranked_users = int(summary.get("total") or 0)
        for row in list(summary.get("data") or []):
            if not isinstance(row, dict):
                continue
            rows.append(
                {
                    "rank": int(row.get("sequence") or 0),
                    "value": float(row.get("grade") or 0.0),
                    "name": str(row.get("nickName") or "").strip(),
                    "trading_volume": float(row.get("tradingVolume") or 0.0),
                }
            )
            if len(rows) >= max_rows:
                break
        if len(rows) >= max_rows:
            break

    rows.sort(key=lambda item: item["rank"])
    rank_500 = next((row for row in rows if int(row.get("rank") or 0) == 500), None)
    updated_dt = datetime.fromtimestamp(updated_time_ms / 1000.0, tz=timezone.utc) if updated_time_ms > 0 else None
    return {
        "resource_id": resource_id,
        "rows_fetched": len(rows),
        "total_ranked_users": total_ranked_users,
        "top500_total_volume": sum(float(row.get("value") or 0.0) for row in rows[:500]),
        "rank_500_value": float(rank_500.get("value") or 0.0) if rank_500 is not None else None,
        "updated_at_utc": updated_dt.isoformat() if updated_dt is not None else "",
        "updated_at_cst": _format_cst_label(updated_dt),
    }


def _competition_snapshot(config: SprintBoardConfig, now: datetime) -> dict[str, Any]:
    start_at = _parse_iso_datetime(config.start_at)
    end_at = _parse_iso_datetime(config.end_at)
    if start_at is None or end_at is None:
        raise RuntimeError(f"invalid sprint config window for {config.slug}")

    symbol_rows = [_local_symbol_metrics(symbol, start_at=start_at, end_at=end_at) for symbol in config.symbols]
    total_volume = sum(float(item.get("volume") or 0.0) for item in symbol_rows)
    total_trades = sum(int(item.get("trade_count") or 0) for item in symbol_rows)
    last_trade = max(
        (str(item.get("last_trade_time_utc") or "").strip() for item in symbol_rows if str(item.get("last_trade_time_utc") or "").strip()),
        default="",
    )
    last_trade_dt = _parse_iso_datetime(last_trade)

    official: dict[str, Any]
    status = "ok"
    status_text = "官方榜单与本机成交额已更新"
    if config.resource_id is None:
        official = {
            "resource_id": None,
            "rows_fetched": 0,
            "total_ranked_users": None,
            "top500_total_volume": None,
            "rank_500_value": None,
            "updated_at_utc": "",
            "updated_at_cst": "",
            "error": "resource id not configured",
        }
        status = "partial"
        status_text = "官方榜单资源未配置，仅展示本机成交额"
    else:
        try:
            official = _fetch_board_rows(config.resource_id, config.referer, max_rows=500)
            official["error"] = ""
        except Exception as exc:
            official = {
                "resource_id": config.resource_id,
                "rows_fetched": 0,
                "total_ranked_users": None,
                "top500_total_volume": None,
                "rank_500_value": None,
                "updated_at_utc": "",
                "updated_at_cst": "",
                "error": f"{type(exc).__name__}: {exc}",
            }
            status = "partial"
            status_text = "本机成交额已更新，官方榜单抓取失败"

    reward_estimates = _reward_estimates(config, official, now)

    return {
        "slug": config.slug,
        "label": config.label,
        "phase_label": config.phase_label,
        "resource_id": config.resource_id,
        "resource_hint": "如 Binance 后续更换第 2 期资源 ID，可直接改本页配置常量或环境变量覆盖。",
        "symbols": list(config.symbols),
        "entry_threshold": config.entry_threshold,
        "threshold_unit": config.threshold_unit,
        "leaderboard_unit": config.leaderboard_unit,
        "start_at_utc": start_at.astimezone(timezone.utc).isoformat(),
        "end_at_utc": end_at.astimezone(timezone.utc).isoformat(),
        "start_at_cst": _format_cst_label(start_at),
        "end_at_cst": _format_cst_label(end_at),
        "window_text": f"{_format_cst_label(start_at)} - {_format_cst_label(end_at)}",
        "status": status,
        "status_text": status_text,
        "official": official,
        "reward_estimates": reward_estimates,
        "local": {
            "total_volume": total_volume,
            "trade_count": total_trades,
            "last_trade_time_utc": last_trade_dt.astimezone(timezone.utc).isoformat() if last_trade_dt is not None else "",
            "last_trade_time_cst": _format_cst_label(last_trade_dt),
            "distance_to_entry_threshold": total_volume - float(config.entry_threshold),
            "symbols": symbol_rows,
        },
    }


def _build_snapshot_payload(now: datetime) -> dict[str, Any]:
    competitions = [_competition_snapshot(config, now) for config in SPRINT_BOARD_CONFIGS]
    refreshed_target = _refresh_target_for(now)
    next_refresh_target = _next_refresh_target_for(now)
    return {
        "ok": True,
        "generated_at_utc": now.astimezone(timezone.utc).isoformat(),
        "refreshed_at_utc": now.astimezone(timezone.utc).isoformat(),
        "refreshed_at_cst": _format_cst_label(now),
        "refresh_status_text": (
            f"北京时间 {MASTER_SPRINT_REFRESH_HOUR_CST:02d}:00 后自动补当天刷新"
        ),
        "refresh_target_cst": _format_cst_label(refreshed_target),
        "next_refresh_target_cst": _format_cst_label(next_refresh_target),
        "competition_count": len(competitions),
        "official_success_count": sum(1 for item in competitions if not str((item.get("official") or {}).get("error") or "").strip()),
        "competitions": competitions,
    }


def build_master_sprint_snapshot(*, refresh: bool = False, now: datetime | None = None) -> dict[str, Any]:
    current_time = now.astimezone(timezone.utc) if now is not None else datetime.now(timezone.utc)
    cached = None
    with MASTER_SPRINT_MEMORY_CACHE_LOCK:
        if isinstance(MASTER_SPRINT_MEMORY_CACHE.get("data"), dict):
            cached = MASTER_SPRINT_MEMORY_CACHE.get("data")
    if cached is None:
        payload = _read_json_file(MASTER_SPRINT_CACHE_PATH, {})
        cached = payload if isinstance(payload, dict) else None
    if not refresh and not _cached_snapshot_due(cached, current_time) and isinstance(cached, dict):
        return cached
    snapshot = _build_snapshot_payload(current_time)
    _write_json_file(MASTER_SPRINT_CACHE_PATH, snapshot)
    with MASTER_SPRINT_MEMORY_CACHE_LOCK:
        MASTER_SPRINT_MEMORY_CACHE["loaded_at"] = current_time.timestamp()
        MASTER_SPRINT_MEMORY_CACHE["data"] = snapshot
    return snapshot
