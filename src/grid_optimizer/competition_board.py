from __future__ import annotations

import json
import math
import os
import re
import subprocess
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from collections.abc import Callable
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import requests

CACHE_PATH = Path("output/competition_board_cache.json")
ENTRIES_PATH = Path("output/competition_board_entries.json")
HISTORY_DIR_PATH = Path("output/competition_board_history")
HISTORY_INDEX_PATH = Path("output/competition_board_history_index.json")
CACHE_TTL_SECONDS = 1800

_CACHE_LOCK = threading.Lock()
_REFRESH_LOCK = threading.Lock()
_MEMORY_CACHE: dict[str, Any] = {"loaded_at": 0.0, "data": None}
_HISTORY_SCHEDULER_LOCK = threading.Lock()
_HISTORY_SCHEDULER_STARTED = False


@dataclass(frozen=True)
class CompetitionSource:
    slug: str
    symbol: str
    market: str
    label: str
    url: str


COMPETITION_SOURCES: tuple[CompetitionSource, ...] = (
    CompetitionSource(
        slug="spot_kat",
        symbol="KAT",
        market="spot",
        label="KAT 新现货上市活动",
        url="https://www.binance.com/zh-CN/activity/trading-competition/spot-KAT-listing-campaign-1",
    ),
    CompetitionSource(
        slug="spot_night",
        symbol="NIGHT",
        market="spot",
        label="NIGHT 新现货上市活动",
        url="https://www.binance.com/zh-CN/activity/trading-competition/spot-NIGHT-listing-campaign",
    ),
    CompetitionSource(
        slug="spot_cfg",
        symbol="CFG",
        market="spot",
        label="CFG 现货交易竞赛",
        url="https://www.binance.com/zh-CN/activity/trading-competition/spot-altcoin-festival-wave-CFG",
    ),
    CompetitionSource(
        slug="futures_opn",
        symbol="OPN",
        market="futures",
        label="OPN 合约交易挑战赛",
        url="https://www.binance.com/zh-CN/activity/trading-competition/futures-opn-challenge?ref=YEK2JZJT",
    ),
    CompetitionSource(
        slug="futures_kat",
        symbol="KAT",
        market="futures",
        label="KAT 合约交易挑战赛",
        url="https://www.binance.com/zh-CN/activity/trading-competition/futures-kat-challenge?ref=YEK2JZJT",
    ),
    CompetitionSource(
        slug="futures_robo",
        symbol="ROBO",
        market="futures",
        label="ROBO 合约交易挑战赛",
        url="https://www.binance.com/zh-CN/activity/trading-competition/futures-robo-challenge?ref=YEK2JZJT",
    ),
    CompetitionSource(
        slug="futures_night",
        symbol="NIGHT",
        market="futures",
        label="NIGHT 合约交易挑战赛",
        url="https://www.binance.com/zh-CN/activity/trading-competition/futures-night-challenge-2?ref=YEK2JZJT",
    ),
    CompetitionSource(
        slug="futures_bard",
        symbol="BARD",
        market="futures",
        label="BARD 合约交易挑战赛",
        url="https://www.binance.com/zh-CN/activity/trading-competition/futures-bard-challenge2?ref=YEK2JZJT",
    ),
)

STATIC_BOARD_HINTS: dict[str, dict[str, Any]] = {
    "spot_kat": {
        "boards": [
            {
                "tabLabel": "默认",
                "resourceId": 46380,
                "metricField": "tradingVolume",
                "metricLabel": "交易量 (USD)",
                "rewardUnit": "KAT",
                "leaderboardUnit": "USD",
                "leaderboardUnitTitle": "交易量",
                "rankingType": "CUSTOMIZED",
                "competitionType": "SPOT",
                "activityPeriodText": "2026/03/19 09:00 - 2026/04/02 09:00",
                "activityEndAt": "2026-04-02T09:00:00+08:00",
                "maxRows": 5000,
                "bodyExcerpt": """
累计现货交易量至少 1,000 USD，方可参与排行榜奖励。
最终奖励将根据用户的累计交易量占合格用户总累计交易量的比例进行分配。
总奖池 3,200,000 KAT
单人奖励上限为 80,000 KAT
""",
            }
        ]
    },
    "spot_night": {
        "boards": [
            {
                "tabLabel": "默认",
                "resourceId": 46151,
                "metricField": "tradingVolume",
                "metricLabel": "交易量 (USD)",
                "rewardUnit": "NIGHT",
                "leaderboardUnit": "USD",
                "leaderboardUnitTitle": "交易量",
                "rankingType": "CUSTOMIZED",
                "competitionType": "SPOT",
                "activityPeriodText": "2026/03/13 09:00 - 2026/04/03 09:00",
                "activityEndAt": "2026-04-03T09:00:00+08:00",
                "maxRows": 5000,
                "bodyExcerpt": """
累计现货交易量至少 1,000 USD，方可参与排行榜奖励。
最终奖励将根据用户的累计交易量占合格用户总累计交易量的比例进行分配。
总奖池 72,000,000 NIGHT
单人奖励上限为 80,000 NIGHT
""",
            }
        ]
    },
    "spot_cfg": {
        "boards": [
            {
                "tabLabel": "默认",
                "resourceId": 47017,
                "metricField": "tradingVolume",
                "metricLabel": "交易量 (USD)",
                "rewardUnit": "CFG",
                "leaderboardUnit": "USD",
                "leaderboardUnitTitle": "交易量",
                "rankingType": "CUSTOMIZED",
                "competitionType": "SPOT",
                "activityPeriodText": "2026/03/20 10:00 - 2026/03/27 10:00",
                "activityEndAt": "2026-03-27T10:00:00+08:00",
                "maxRows": 5000,
                "bodyExcerpt": """
累计现货交易量至少 500 USD，方可参与排行榜奖励。
奖励结构
第 1 - 200 名
平分 334,000 CFG
第 201 - 5000 名
平分 501,000 CFG
""",
            }
        ]
    },
    "futures_opn": {
        "boards": [
            {
                "tabLabel": "交易量挑战赛",
                "resourceId": 44814,
                "metricField": "grade",
                "metricLabel": "交易量 (USDT)",
                "rewardUnit": "OPN",
                "leaderboardUnit": "USDT",
                "leaderboardUnitTitle": "交易量",
                "rankingType": "CUSTOMIZED",
                "competitionType": "FUTURES",
                "activityPeriodText": "2026/03/05 13:00 - 2026/03/25 23:59",
                "activityEndAt": "2026-03-25T23:59:00+08:00",
                "maxRows": 200,
                "bodyExcerpt": """
累计合约交易量至少 500 USDT，方可参与排行榜奖励。
第 1 名
80,000 OPN
第 2 名
64,000 OPN
第 3 名
48,000 OPN
第 4 名
32,000 OPN
第 5 名
16,000 OPN
第 6 - 20 名
平分 120,000 OPN
第 21 - 50 名
平分 160,000 OPN
第 51 - 200 名
平分 280,000 OPN
""",
            }
        ]
    },
    "futures_kat": {
        "boards": [
            {
                "tabLabel": "交易量挑战赛 - 第一阶段",
                "resourceId": 46949,
                "metricField": "grade",
                "metricLabel": "交易量 (USDT)",
                "rewardUnit": "KAT",
                "leaderboardUnit": "USDT",
                "leaderboardUnitTitle": "交易量",
                "rankingType": "CUSTOMIZED",
                "competitionType": "FUTURES",
                "activityPeriodText": "2026/03/19 17:00 - 2026/03/29 07:59",
                "activityEndAt": "2026-03-29T07:59:00+08:00",
                "maxRows": 200,
                "bodyExcerpt": """
活动时间：2026/03/19 17:00 - 2026/03/29 07:59
累计合约交易量至少 500 USDT，方可参与排行榜奖励。
第 1 名
562,500 KAT
第 2 名
450,000 KAT
第 3 名
262,500 KAT
第 4 名
150,000 KAT
第 5 名
75,000 KAT
第 6 - 20 名
平分 562,500 KAT
第 21 - 50 名
平分 562,500 KAT
第 51 - 200 名
平分 1,125,000 KAT
""",
            },
            {
                "tabLabel": "交易量挑战赛 - 第二阶段",
                "metricField": "grade",
                "metricLabel": "交易量 (USDT)",
                "rewardUnit": "KAT",
                "leaderboardUnit": "USDT",
                "leaderboardUnitTitle": "交易量",
                "rankingType": "CUSTOMIZED",
                "competitionType": "FUTURES",
                "activityPeriodText": "2026/03/29 08:00 - 2026/04/08 07:59",
                "activityEndAt": "2026-04-08T07:59:00+08:00",
                "maxRows": 200,
                "bodyExcerpt": """
活动时间：2026/03/29 08:00 - 2026/04/08 07:59
累计合约交易量至少 500 USDT，方可参与排行榜奖励。
第 1 名
562,500 KAT
第 2 名
450,000 KAT
第 3 名
262,500 KAT
第 4 名
150,000 KAT
第 5 名
75,000 KAT
第 6 - 20 名
平分 562,500 KAT
第 21 - 50 名
平分 562,500 KAT
第 51 - 200 名
平分 1,125,000 KAT
""",
            },
        ]
    },
    "futures_robo": {
        "boards": [
            {
                "tabLabel": "交易量挑战赛",
                "resourceId": 45636,
                "metricField": "grade",
                "metricLabel": "交易量 (USDT)",
                "rewardUnit": "ROBO",
                "leaderboardUnit": "USDT",
                "leaderboardUnitTitle": "交易量",
                "rankingType": "CUSTOMIZED",
                "competitionType": "FUTURES",
                "activityPeriodText": "2026/03/06 13:00 - 2026/03/26 23:59",
                "activityEndAt": "2026-03-26T23:59:00+08:00",
                "maxRows": 200,
                "bodyExcerpt": """
累计合约交易量至少 500 USDT，方可参与排行榜奖励。
第 1 名
800,000 ROBO
第 2 名
640,000 ROBO
第 3 名
480,000 ROBO
第 4 名
320,000 ROBO
第 5 名
160,000 ROBO
第 6 - 20 名
平分 1,200,000 ROBO
第 21 - 50 名
平分 1,600,000 ROBO
第 51 - 200 名
平分 2,800,000 ROBO
""",
            }
        ]
    },
    "futures_night": {
        "boards": [
            {
                "tabLabel": "交易量挑战赛",
                "resourceId": 46144,
                "metricField": "grade",
                "metricLabel": "交易量 (USDT)",
                "rewardUnit": "NIGHT",
                "leaderboardUnit": "USDT",
                "leaderboardUnitTitle": "交易量",
                "rankingType": "CUSTOMIZED",
                "competitionType": "FUTURES",
                "activityPeriodText": "2026/03/13 10:00 - 2026/04/02 23:59",
                "activityEndAt": "2026-04-02T23:59:00+08:00",
                "maxRows": 500,
                "bodyExcerpt": """
累计合约交易量至少 500 USDT，方可参与排行榜奖励。
第 1 名
1,000,000 NIGHT
第 2 名
800,000 NIGHT
第 3 名
600,000 NIGHT
第 4 名
400,000 NIGHT
第 5 名
200,000 NIGHT
第 6 - 20 名
平分 1,000,000 NIGHT
第 21 - 50 名
平分 1,000,000 NIGHT
第 51 - 200 名
平分 3,000,000 NIGHT
第 201 - 500 名
平分 2,000,000 NIGHT
""",
            }
        ]
    },
    "futures_bard": {
        "boards": [
            {
                "tabLabel": "交易量挑战赛 - 第一阶段",
                "resourceId": 47457,
                "metricField": "grade",
                "metricLabel": "交易量 (USDT)",
                "rewardUnit": "BARD",
                "leaderboardUnit": "USDT",
                "leaderboardUnitTitle": "交易量",
                "rankingType": "CUSTOMIZED",
                "competitionType": "FUTURES",
                "activityPeriodText": "2026/03/26 18:00 - 2026/04/05 07:59",
                "activityEndAt": "2026-04-05T07:59:00+08:00",
                "maxRows": 200,
                "bodyExcerpt": """
活动时间：2026/03/26 18:00 - 2026/04/05 07:59
累计 BARD U 本位合约交易量至少 500 USDT，方可参与排行榜奖励。
本阶段奖池 500,000 BARD
活动页当前展示为按交易量排名参与榜单分配，具体名次奖励结构以 Binance 活动页实时说明为准。
""",
            },
            {
                "tabLabel": "交易量挑战赛 - 第二阶段",
                "metricField": "grade",
                "metricLabel": "交易量 (USDT)",
                "rewardUnit": "BARD",
                "leaderboardUnit": "USDT",
                "leaderboardUnitTitle": "交易量",
                "rankingType": "CUSTOMIZED",
                "competitionType": "FUTURES",
                "activityPeriodText": "2026/04/05 08:00 - 2026/04/15 07:59",
                "activityEndAt": "2026-04-15T07:59:00+08:00",
                "maxRows": 200,
                "bodyExcerpt": """
活动时间：2026/04/05 08:00 - 2026/04/15 07:59
累计 BARD U 本位合约交易量至少 500 USDT，方可参与排行榜奖励。
本阶段奖池 500,000 BARD
活动页当前展示为按交易量排名参与榜单分配，具体名次奖励结构以 Binance 活动页实时说明为准。
""",
            },
        ]
    },
}

_PLAYWRIGHT_EXTRACT_SCRIPT = r"""
async () => {
  function sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  function appMeta() {
    const node = document.getElementById("__APP_DATA");
    if (!node) {
      return {};
    }
    try {
      const data = JSON.parse(node.textContent || "{}");
      const route = Object.values((data.appState && data.appState.loader && data.appState.loader.dataByRouteId) || {})
        .find((item) => item && item.activityGroup);
      const activityGroup = route && route.activityGroup ? route.activityGroup : {};
      const homepage = activityGroup.i18nContent && activityGroup.i18nContent.homepage
        ? activityGroup.i18nContent.homepage
        : {};
      return {
        code: activityGroup.code || "",
        activityId: activityGroup.id || null,
        seoTitle: (homepage.seoContent && homepage.seoContent.title) || document.title || "",
        seoDescription: (homepage.seoContent && homepage.seoContent.description) || "",
        publishedTime: activityGroup.publishedTime || null,
        taskExpiredTime: activityGroup.taskExpiredTime || null,
      };
    } catch (err) {
      return { appError: String(err || "") };
    }
  }

  function reactFiber(el) {
    return Object.values(el || {}).find(
      (value) => value && typeof value === "object" && "memoizedProps" in value && "return" in value
    ) || null;
  }

  function findLeaderboardState() {
    const section = document.getElementById("leaderboard-section") || document.body || document.documentElement;
    if (!section) {
      return null;
    }
    let best = null;
    for (const el of section.querySelectorAll("*")) {
      let fiber = reactFiber(el);
      while (fiber) {
        const rawType = fiber.elementType || fiber.type;
        const name = typeof rawType === "string"
          ? rawType
          : (rawType && (rawType.displayName || rawType.name)) || String(rawType || "");
        const props = fiber.memoizedProps || {};
        const columns = Array.isArray(props.columns)
          ? props.columns.map((item) => ({
              title: item && item.title ? String(item.title) : "",
              dataIndex: item && item.dataIndex ? String(item.dataIndex) : "",
            }))
          : [];
        const list = Array.isArray(props.list)
          ? props.list.map((item) => ({
              resourceId: item && item.resourceId ? Number(item.resourceId) : null,
              sequence: item && item.sequence ? Number(item.sequence) : null,
              nickName: item && item.nickName ? String(item.nickName) : "",
              tradingVolume: Number(item && item.tradingVolume ? item.tradingVolume : 0),
              grade: Number(item && item.grade ? item.grade : 0),
              rewardCount: item && item.rewardCount !== null && item.rewardCount !== undefined
                ? Number(item.rewardCount)
                : null,
            }))
          : [];
        const resourceIds = new Set(list.map((item) => item.resourceId).filter(Boolean));
        const score = resourceIds.size * 1000 + list.length * 10 + columns.length;
        if (score > 0) {
          const candidate = {
            page: Number(props.page || 1),
            total: Number(props.total || 0),
            pageSize: Number(props.pageSize || 10),
            rankingType: props.rankingType || "",
            competitionType: props.competitionType || "",
            rewardUnit: props.rewardUnit || "",
            leaderboardUnit: props.leaderboardUnit || "",
            leaderboardUnitTitle: props.leaderboardUnitTitle || "",
            columns,
            list,
          };
          if (!best || score > best.score) {
            best = { score, state: candidate };
          }
        }
        fiber = fiber.return;
      }
    }
    return best ? best.state : null;
  }

  function currentTabLabel() {
    const active = Array.from(document.querySelectorAll(".bn-tab.active"))
      .map((el) => (el.textContent || "").trim())
      .find((text) => text);
    return active || "";
  }

  async function clickTab(label) {
    const normalized = String(label || "").trim();
    if (!normalized) {
      return false;
    }
    const button = Array.from(document.querySelectorAll(".bn-tab"))
      .find((el) => (el.textContent || "").trim() === normalized);
    if (!button) {
      return false;
    }
    button.click();
    const startedAt = Date.now();
    while (Date.now() - startedAt < 12000) {
      if (currentTabLabel() === normalized) {
        await sleep(1200);
        return true;
      }
      await sleep(150);
    }
    return false;
  }

  async function waitForLeaderboard() {
    const startedAt = Date.now();
    while (Date.now() - startedAt < 15000) {
      const state = findLeaderboardState();
      if (state && state.list && state.list.length) {
        return state;
      }
      await sleep(250);
    }
    return findLeaderboardState();
  }

  async function collectBoard(label) {
    const state = await waitForLeaderboard();
    if (!state || !state.list || !state.list.length) {
      return null;
    }
    const metricColumn = state.columns && state.columns.length ? state.columns[state.columns.length - 1] : null;
    const bodyText = document.body && document.body.innerText ? document.body.innerText : "";
    const updatedMatch = bodyText.match(/数据上次更新时间：([^\n]+)/);
    return {
      tabLabel: label || currentTabLabel() || "默认",
      url: location.href,
      title: document.title || "",
      bodyExcerpt: bodyText.slice(0, 60000),
      resourceId: state.list[0].resourceId,
      metricField: metricColumn && metricColumn.dataIndex ? metricColumn.dataIndex : "tradingVolume",
      metricLabel: metricColumn && metricColumn.title ? metricColumn.title : "",
      rewardUnit: state.rewardUnit || "",
      leaderboardUnit: state.leaderboardUnit || "",
      leaderboardUnitTitle: state.leaderboardUnitTitle || "",
      rankingType: state.rankingType || "",
      competitionType: state.competitionType || "",
      updatedText: updatedMatch ? updatedMatch[1].trim() : "",
    };
  }

  const meta = appMeta();
  const labels = Array.from(document.querySelectorAll(".bn-tab"))
    .map((el) => (el.textContent || "").trim())
    .filter((text, index, arr) => text && arr.indexOf(text) === index);
  const targets = labels.filter((text) => !/活动主页/.test(text));
  const boards = [];

  if (!targets.length) {
    const board = await collectBoard(currentTabLabel() || "默认");
    if (board) {
      boards.push(board);
    }
  } else {
    for (const label of targets) {
      await clickTab(label);
      const board = await collectBoard(label);
      if (board) {
        boards.push(board);
      }
    }
  }

  return {
    meta,
    boards,
  };
}
"""

_SEGMENT_LINE_RE = re.compile(r"第\s*([0-9,]+)\s*(?:[-–—~至]\s*([0-9,]+))?\s*名")
_THRESHOLD_RE = re.compile(
    r"累计[^。\n]{0,80}?(?:至少|不低于)\s*([0-9,]+(?:\.[0-9]+)?)\s*(美元|USDT|USD)",
    re.IGNORECASE,
)
_CAP_RE = re.compile(r"上限(?:为)?\s*([0-9,]+(?:\.[0-9]+)?)\s*([A-Z]+)")
_PRIZE_POOL_RE = re.compile(r"(?:瓜分|总奖池|奖池)\s*([0-9,]+(?:\.[0-9]+)?)\s*([A-Z]+)")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _empty_snapshot(*, error: str | None = None) -> dict[str, Any]:
    payload = {
        "generated_at_utc": "",
        "boards": [],
        "markets": {"spot": [], "futures": []},
        "entries": [],
        "errors": [],
    }
    if error:
        payload["errors"] = [error]
    return payload


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


def _safe_int(value: Any) -> int | None:
    number = _safe_float(value)
    if number is None:
        return None
    return int(number)


def _sanitize_key(raw: str) -> str:
    text = re.sub(r"\s+", "_", str(raw or "").strip())
    text = re.sub(r"[^0-9A-Za-z_\u4e00-\u9fff-]+", "", text)
    return text or "default"


def _normalize_lines(text: str) -> list[str]:
    return [line.strip() for line in str(text).splitlines() if line.strip()]


def _extract_playwright_result(stdout: str) -> Any:
    marker = "### Result"
    idx = stdout.find(marker)
    if idx < 0:
        raise RuntimeError(f"playwright result not found: {stdout[-400:]}")
    payload = stdout[idx + len(marker) :].strip()
    next_marker = payload.find("\n### ")
    if next_marker >= 0:
        payload = payload[:next_marker].strip()
    return json.loads(payload)


def _playwright_cli_command(*args: str) -> list[str]:
    return ["npx", "--yes", "--package", "@playwright/cli", "playwright-cli", *args]


def _run_playwright_extract(url: str) -> dict[str, Any]:
    last_error: Exception | None = None
    for attempt in range(3):
        session = f"competition-board-{uuid.uuid4().hex[:10]}"
        try:
            subprocess.run(
                _playwright_cli_command("--session", session, "open", url),
                check=True,
                capture_output=True,
                text=True,
                timeout=90,
                start_new_session=True,
            )
            eval_proc = subprocess.run(
                _playwright_cli_command("--session", session, "eval", _PLAYWRIGHT_EXTRACT_SCRIPT),
                check=True,
                capture_output=True,
                text=True,
                timeout=180,
                start_new_session=True,
            )
            return _extract_playwright_result(eval_proc.stdout)
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
            details = ""
            if isinstance(exc, subprocess.CalledProcessError):
                stdout = str(exc.stdout or "").strip()
                stderr = str(exc.stderr or "").strip()
                details = f" stdout={stdout[-300:]} stderr={stderr[-300:]}"
            last_error = RuntimeError(
                f"playwright extract failed for {url} on attempt {attempt + 1}: {type(exc).__name__}:{details}"
            )
            time.sleep(1.5 * (attempt + 1))
        finally:
            subprocess.run(
                _playwright_cli_command("--session", session, "close"),
                capture_output=True,
                text=True,
                timeout=30,
                check=False,
                start_new_session=True,
            )
    if last_error is not None:
        raise last_error
    raise RuntimeError(f"playwright extract failed for {url}")


def _fetch_leaderboard_rows(resource_id: int, referer: str, *, max_rows: int | None = None) -> dict[str, Any]:
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

    def _request(page_index: int) -> dict[str, Any]:
        response = requests.post(
            endpoint,
            data=json.dumps(
                {
                    "resourceId": int(resource_id),
                    "leaderboardType": "USER",
                    "pageIndex": int(page_index),
                    "pageSize": 100,
                }
            ),
            headers=headers,
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        if str(payload.get("code")) != "000000":
            raise RuntimeError(
                f"leaderboard fetch failed for resource {resource_id}: "
                f"{payload.get('code')} {payload.get('message')}"
            )
        return payload

    first_payload = _request(1)
    data = first_payload.get("data", {}) if isinstance(first_payload, dict) else {}
    summary_list = data.get("resourceSummaryList", {}) if isinstance(data, dict) else {}
    total = int(summary_list.get("total", 0) or 0)
    row_limit = total
    if max_rows is not None and max_rows > 0:
        row_limit = min(total, int(max_rows))
    pages = max(1, math.ceil(row_limit / 100)) if row_limit > 0 else 1

    page_payloads: dict[int, dict[str, Any]] = {1: first_payload}
    if pages > 1:
        with ThreadPoolExecutor(max_workers=min(8, pages - 1)) as executor:
            future_map = {
                executor.submit(_request, page_index): page_index for page_index in range(2, pages + 1)
            }
            for future, page_index in ((future, future_map[future]) for future in future_map):
                page_payloads[page_index] = future.result()

    rows: list[dict[str, Any]] = []
    for page_index in range(1, pages + 1):
        payload = page_payloads[page_index]
        page_data = payload.get("data", {}) if isinstance(payload, dict) else {}
        page_summary = page_data.get("resourceSummaryList", {}) if isinstance(page_data, dict) else {}
        for item in page_summary.get("data", []) or []:
            if not isinstance(item, dict):
                continue
            rows.append(item)
            if len(rows) >= row_limit > 0:
                break
        if len(rows) >= row_limit > 0:
            break

    return {
        "resource_id": resource_id,
        "eligible_user_count": int(data.get("eligibleUserCount", total) or total),
        "eligible_metric_total": float(data.get("eligibleTradingVolume", 0.0) or 0.0),
        "updated_time_ms": int(data.get("updatedTime", 0) or 0),
        "rows_truncated": row_limit < total,
        "last_rank_fetched": int(rows[-1].get("sequence", 0) or 0) if rows else 0,
        "rows": rows,
    }


def _parse_threshold(text: str) -> dict[str, Any]:
    match = _THRESHOLD_RE.search(text)
    if not match:
        return {"threshold_value": None, "threshold_unit": ""}
    return {
        "threshold_value": _safe_float(match.group(1)),
        "threshold_unit": match.group(2).upper(),
    }


def _parse_cap(text: str) -> dict[str, Any]:
    match = _CAP_RE.search(text)
    if not match:
        return {"cap_value": None, "cap_unit": ""}
    return {
        "cap_value": _safe_float(match.group(1)),
        "cap_unit": match.group(2).upper(),
    }


def _parse_prize_pool(text: str) -> dict[str, Any]:
    match = _PRIZE_POOL_RE.search(text)
    if not match:
        return {"prize_pool_value": None, "prize_pool_unit": "", "prize_pool_text": ""}
    return {
        "prize_pool_value": _safe_float(match.group(1)),
        "prize_pool_unit": match.group(2).upper(),
        "prize_pool_text": match.group(0).strip(),
    }


def _parse_segments(text: str, total_rows: int) -> list[dict[str, Any]]:
    lines = _normalize_lines(text)
    segments: list[dict[str, Any]] = []
    seen: set[tuple[int, int, str]] = set()

    for index, line in enumerate(lines):
        matched = _SEGMENT_LINE_RE.search(line)
        if not matched:
            continue
        start_rank = int(matched.group(1).replace(",", ""))
        end_rank = int((matched.group(2) or matched.group(1)).replace(",", ""))
        reward_text = ""
        for probe in lines[index + 1 : index + 5]:
            if _SEGMENT_LINE_RE.search(probe):
                break
            if (
                "奖池" in probe
                or "平分" in probe
                or "均分" in probe
                or re.search(r"[A-Z]{2,}", probe)
            ):
                reward_text = probe
                break
        if not reward_text:
            continue
        dedupe_key = (start_rank, end_rank, reward_text)
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        segments.append(
            {
                "start_rank": start_rank,
                "end_rank": end_rank,
                "rank_label": line,
                "reward_text": reward_text,
            }
        )

    if segments:
        return sorted(segments, key=lambda item: (item["start_rank"], item["end_rank"]))

    if "最终奖励" in text and "总奖池" in text:
        cap = _parse_cap(text)
        reward_text = "按交易量占比瓜分总奖池"
        if cap["cap_value"] is not None and cap["cap_unit"]:
            reward_text += f"（单人上限 {cap['cap_value']:,.0f} {cap['cap_unit']}）"
        return [
            {
                "start_rank": 1,
                "end_rank": total_rows if total_rows > 0 else None,
                "rank_label": "全部合格用户",
                "reward_text": reward_text,
            }
        ]
    return []


def _per_user_reward_from_segment(
    reward_text: str,
    *,
    start_rank: int,
    end_rank: int,
    prize_pool_value: float | None,
) -> float | None:
    segment_size = max(1, end_rank - start_rank + 1)
    direct = re.search(r"([0-9,]+(?:\.[0-9]+)?)\s*([A-Z]+)", reward_text)
    if direct and not any(token in reward_text for token in ("平分", "均分", "奖池的")):
        return _safe_float(direct.group(1))
    shared = re.search(r"(?:平分|均分)\s*([0-9,]+(?:\.[0-9]+)?)\s*[A-Z]+", reward_text)
    if shared:
        total_amount = _safe_float(shared.group(1))
        return None if total_amount is None else total_amount / segment_size
    percent = re.search(r"奖池的\s*([0-9,]+(?:\.[0-9]+)?)\s*%", reward_text)
    if percent and prize_pool_value is not None:
        pct = _safe_float(percent.group(1))
        return None if pct is None else prize_pool_value * pct / 100.0
    return None


def _compose_board(
    source: CompetitionSource,
    board_meta: dict[str, Any],
    leaderboard_payload: dict[str, Any],
    extracted_meta: dict[str, Any],
) -> dict[str, Any]:
    metric_field = str(board_meta.get("metricField", "tradingVolume")).strip() or "tradingVolume"
    raw_rows = leaderboard_payload.get("rows", [])
    rows: list[dict[str, Any]] = []
    for item in raw_rows:
        if not isinstance(item, dict):
            continue
        value = _safe_float(item.get(metric_field))
        if value is None:
            value = _safe_float(item.get("tradingVolume")) or _safe_float(item.get("grade")) or 0.0
        rows.append(
            {
                "rank": int(item.get("sequence", 0) or 0),
                "name": str(item.get("nickName", "")).strip() or "-",
                "value": value,
                "value_text": f"{value:,.2f}",
                "reward_count": _safe_float(item.get("rewardCount")),
            }
        )
    rows.sort(key=lambda item: item["rank"])

    body_excerpt = str(board_meta.get("bodyExcerpt", ""))
    threshold = _parse_threshold(body_excerpt)
    cap = _parse_cap(body_excerpt)
    prize_pool = _parse_prize_pool(body_excerpt or str(extracted_meta.get("seoDescription", "")))
    total_rows = int(leaderboard_payload.get("eligible_user_count", len(rows)) or len(rows))
    segments = _parse_segments(body_excerpt, total_rows)
    value_by_rank = {item["rank"]: item["value"] for item in rows}

    current_segments: list[dict[str, Any]] = []
    for segment in segments:
        end_rank = segment.get("end_rank")
        cutoff_value = value_by_rank.get(int(end_rank)) if isinstance(end_rank, int) else None
        per_user_reward = None
        if isinstance(end_rank, int):
            per_user_reward = _per_user_reward_from_segment(
                str(segment["reward_text"]),
                start_rank=int(segment["start_rank"]),
                end_rank=end_rank,
                prize_pool_value=prize_pool["prize_pool_value"],
            )
        current_segments.append(
            {
                **segment,
                "cutoff_value": cutoff_value,
                "cutoff_value_text": f"{cutoff_value:,.2f}" if cutoff_value is not None else "-",
                "per_user_reward": per_user_reward,
                "per_user_reward_text": (
                    f"{per_user_reward:,.2f} {board_meta.get('rewardUnit', '')}".strip()
                    if per_user_reward is not None
                    else ""
                ),
            }
        )

    tab_label = str(board_meta.get("tabLabel", "默认")).strip() or "默认"
    board_key = f"{source.slug}:{_sanitize_key(tab_label)}"
    updated_time_ms = int(leaderboard_payload.get("updated_time_ms", 0) or 0)
    updated_at_utc = (
        datetime.fromtimestamp(updated_time_ms / 1000.0, tz=timezone.utc).isoformat()
        if updated_time_ms > 0
        else None
    )
    lowest_entry = rows[-1]["value"] if rows else None
    rows_truncated = bool(leaderboard_payload.get("rows_truncated"))

    return {
        "board_key": board_key,
        "source_slug": source.slug,
        "symbol": source.symbol,
        "market": source.market,
        "title": str(board_meta.get("title", source.label)).strip() or source.label,
        "label": f"{source.symbol} · {tab_label}" if tab_label != "默认" else source.label,
        "base_label": source.label,
        "tab_label": tab_label,
        "url": str(board_meta.get("url", source.url)).strip() or source.url,
        "resource_id": int(board_meta.get("resourceId", 0) or 0),
        "metric_field": metric_field,
        "metric_label": str(board_meta.get("metricLabel", "当前数值")).strip() or "当前数值",
        "reward_unit": str(board_meta.get("rewardUnit", "")).strip(),
        "leaderboard_unit": str(board_meta.get("leaderboardUnit", "")).strip(),
        "leaderboard_unit_title": str(board_meta.get("leaderboardUnitTitle", "")).strip(),
        "competition_type": str(board_meta.get("competitionType", "")).strip(),
        "ranking_type": str(board_meta.get("rankingType", "")).strip(),
        "seo_description": str(extracted_meta.get("seoDescription", "")).strip(),
        "activity_code": str(extracted_meta.get("code", "")).strip(),
        "activity_id": extracted_meta.get("activityId"),
        "published_time": extracted_meta.get("publishedTime"),
        "task_expired_time": extracted_meta.get("taskExpiredTime"),
        "updated_text": str(board_meta.get("updatedText", "")).strip(),
        "updated_at_utc": updated_at_utc,
        "activity_period_text": str(board_meta.get("activityPeriodText", "")).strip(),
        "activity_end_at": str(board_meta.get("activityEndAt", "")).strip(),
        "threshold_value": threshold["threshold_value"],
        "threshold_unit": threshold["threshold_unit"],
        "cap_value": cap["cap_value"],
        "cap_unit": cap["cap_unit"],
        "prize_pool_value": prize_pool["prize_pool_value"],
        "prize_pool_unit": prize_pool["prize_pool_unit"] or str(board_meta.get("rewardUnit", "")).strip(),
        "prize_pool_text": prize_pool["prize_pool_text"] or str(extracted_meta.get("seoDescription", "")).strip(),
        "eligible_user_count": total_rows,
        "eligible_metric_total": float(leaderboard_payload.get("eligible_metric_total", 0.0) or 0.0),
        "eligible_metric_total_text": f"{float(leaderboard_payload.get('eligible_metric_total', 0.0) or 0.0):,.2f}",
        "current_floor_value": lowest_entry,
        "current_floor_value_text": f"{lowest_entry:,.2f}" if lowest_entry is not None else "-",
        "rows_truncated": rows_truncated,
        "last_rank_fetched": int(leaderboard_payload.get("last_rank_fetched", len(rows)) or len(rows)),
        "segments": current_segments,
        "top_rows": rows[:10],
        "tail_rows": rows[-10:],
        "rows": rows,
        "body_excerpt": body_excerpt,
    }


def _hinted_boards_for_source(source: CompetitionSource) -> tuple[dict[str, Any], list[dict[str, Any]]] | None:
    hint = STATIC_BOARD_HINTS.get(source.slug)
    if not isinstance(hint, dict):
        return None
    meta = dict(hint.get("meta", {}) or {})
    boards = hint.get("boards", [])
    if not isinstance(boards, list) or not boards:
        return None
    normalized_boards: list[dict[str, Any]] = []
    for board in boards:
        if not isinstance(board, dict):
            continue
        normalized = dict(board)
        normalized.setdefault("url", source.url)
        normalized.setdefault("title", source.label)
        normalized_boards.append(normalized)
    if not normalized_boards:
        return None
    return meta, normalized_boards


def _read_json_file(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return default
    except json.JSONDecodeError:
        return default


def _write_json_file(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _competition_board_tz() -> timezone | ZoneInfo:
    tz_name = str(os.environ.get("GRID_COMPETITION_BOARD_TZ", "Asia/Shanghai")).strip() or "Asia/Shanghai"
    try:
        return ZoneInfo(tz_name)
    except ZoneInfoNotFoundError:
        return timezone.utc


def _competition_board_local_now() -> datetime:
    return datetime.now(_competition_board_tz())


def _competition_board_local_date_text(*, when: datetime | None = None) -> str:
    probe = when.astimezone(_competition_board_tz()) if when is not None else _competition_board_local_now()
    return probe.date().isoformat()


def _parse_iso_datetime(value: str) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _local_datetime_text(value: str) -> str:
    probe = _parse_iso_datetime(value)
    if probe is None:
        return ""
    localized = probe.astimezone(_competition_board_tz())
    return f"{localized.year}/{localized.month}/{localized.day} {localized.strftime('%H:%M:%S')}"


def _board_history_granularity(board: dict[str, Any]) -> str:
    explicit = str(board.get("history_granularity", "")).strip().lower()
    if explicit in {"daily", "hourly"}:
        return explicit
    updated_at = _parse_iso_datetime(str(board.get("updated_at_utc", "")).strip())
    return "hourly" if updated_at is not None else "daily"


def _board_history_capture_key(board: dict[str, Any]) -> str:
    granularity = _board_history_granularity(board)
    updated_at = _parse_iso_datetime(str(board.get("updated_at_utc", "")).strip())
    if updated_at is not None:
        localized = updated_at.astimezone(_competition_board_tz())
        if granularity == "hourly":
            return localized.strftime("%Y-%m-%d %H:00")
        return localized.strftime("%Y-%m-%d")
    return _competition_board_local_date_text()


def _history_file_name(board_key: str, capture_key: str) -> str:
    return f"{_sanitize_key(capture_key)}__{_sanitize_key(board_key)}.json"


def _history_file_path(board_key: str, capture_key: str) -> Path:
    return HISTORY_DIR_PATH / _history_file_name(board_key, capture_key)


def _load_history_index() -> dict[str, Any]:
    payload = _read_json_file(HISTORY_INDEX_PATH, {})
    if not isinstance(payload, dict):
        return {"updated_at_utc": "", "boards": {}}
    boards = payload.get("boards", {})
    if not isinstance(boards, dict):
        boards = {}
    normalized_boards: dict[str, list[dict[str, Any]]] = {}
    for board_key, records in boards.items():
        if not isinstance(records, list):
            continue
        clean_records: list[dict[str, Any]] = []
        for item in records:
            if not isinstance(item, dict):
                continue
            capture_key = str(item.get("capture_key", item.get("capture_date", ""))).strip()
            relative_path = str(item.get("path", "")).strip()
            if not capture_key or not relative_path:
                continue
            clean_records.append(
                {
                    "capture_key": capture_key,
                    "capture_label": str(item.get("capture_label", capture_key)).strip() or capture_key,
                    "capture_date": str(item.get("capture_date", capture_key.split(" ", 1)[0])).strip(),
                    "capture_granularity": str(item.get("capture_granularity", "daily")).strip() or "daily",
                    "captured_at_utc": str(item.get("captured_at_utc", "")).strip(),
                    "path": relative_path,
                    "label": str(item.get("label", "")).strip(),
                    "updated_text": str(item.get("updated_text", "")).strip(),
                    "updated_at_utc": str(item.get("updated_at_utc", "")).strip(),
                    "eligible_user_count": _safe_int(item.get("eligible_user_count")) or 0,
                    "current_floor_value_text": str(item.get("current_floor_value_text", "")).strip(),
                }
            )
        clean_records.sort(key=lambda item: item["capture_key"], reverse=True)
        normalized_boards[str(board_key)] = clean_records
    return {
        "updated_at_utc": str(payload.get("updated_at_utc", "")).strip(),
        "boards": normalized_boards,
    }


def _save_history_index(payload: dict[str, Any]) -> None:
    payload = dict(payload)
    payload["updated_at_utc"] = _now_iso()
    boards = payload.get("boards", {})
    if not isinstance(boards, dict):
        boards = {}
    normalized_boards: dict[str, list[dict[str, Any]]] = {}
    for board_key, records in boards.items():
        if not isinstance(records, list):
            continue
        normalized_records = [item for item in records if isinstance(item, dict)]
        normalized_records.sort(key=lambda item: str(item.get("capture_key", item.get("capture_date", ""))), reverse=True)
        normalized_boards[str(board_key)] = normalized_records
    payload["boards"] = normalized_boards
    _write_json_file(HISTORY_INDEX_PATH, payload)


def _expected_board_keys() -> list[str]:
    board_keys: list[str] = []
    for source in COMPETITION_SOURCES:
        hinted = _hinted_boards_for_source(source)
        if hinted is None:
            board_keys.append(f"{source.slug}:default")
            continue
        _, board_metas = hinted
        for board_meta in board_metas:
            tab_label = str(board_meta.get("tabLabel", "默认")).strip() or "默认"
            board_keys.append(f"{source.slug}:{_sanitize_key(tab_label)}")
    return sorted(set(board_keys))


def _first_segment_cutoff(board: dict[str, Any]) -> float | None:
    segments = board.get("segments", [])
    if not isinstance(segments, list) or not segments:
        return None
    first = segments[0]
    if not isinstance(first, dict):
        return None
    return _safe_float(first.get("cutoff_value"))


def _load_history_board_from_record(record: dict[str, Any]) -> dict[str, Any] | None:
    if not isinstance(record, dict):
        return None
    payload = _read_json_file(Path(str(record.get("path", "")).strip()), {})
    if not isinstance(payload, dict):
        return None
    board = payload.get("board")
    return board if isinstance(board, dict) else None


def _history_compare_meta_for_board(board: dict[str, Any], records: list[dict[str, Any]]) -> dict[str, Any]:
    if not isinstance(board, dict) or not isinstance(records, list):
        return {}
    current_capture_key = _board_history_capture_key(board)
    baseline_record = next(
        (
            item
            for item in records
            if str(item.get("capture_key", item.get("capture_date", ""))).strip() != current_capture_key
        ),
        None,
    )
    if baseline_record is None:
        return {}
    baseline_board = _load_history_board_from_record(baseline_record)
    if baseline_board is None:
        return {}
    granularity = _board_history_granularity(board)
    floor_current = _safe_float(board.get("current_floor_value"))
    floor_previous = _safe_float(baseline_board.get("current_floor_value"))
    eligible_current = _safe_int(board.get("eligible_user_count"))
    eligible_previous = _safe_int(baseline_board.get("eligible_user_count"))
    segment_current = _first_segment_cutoff(board)
    segment_previous = _first_segment_cutoff(baseline_board)
    current_updated_text = str(board.get("updated_text", "")).strip() or _local_datetime_text(str(board.get("updated_at_utc", "")).strip())
    previous_updated_text = (
        str(baseline_board.get("updated_text", "")).strip()
        or _local_datetime_text(str(baseline_board.get("updated_at_utc", "")).strip())
        or str(baseline_record.get("updated_text", "")).strip()
    )
    return {
        "title": "上一档 vs 当前" if granularity == "hourly" else "前一日 vs 当前日",
        "granularity": granularity,
        "current_capture_key": current_capture_key,
        "previous_capture_key": str(baseline_record.get("capture_key", baseline_record.get("capture_date", ""))).strip(),
        "current_updated_text": current_updated_text,
        "previous_updated_text": previous_updated_text,
        "floor_current": floor_current,
        "floor_previous": floor_previous,
        "floor_current_text": str(board.get("current_floor_value_text", "")).strip() or "-",
        "floor_previous_text": str(baseline_board.get("current_floor_value_text", "")).strip() or "-",
        "floor_delta": None if floor_current is None or floor_previous is None else floor_current - floor_previous,
        "eligible_current": eligible_current,
        "eligible_previous": eligible_previous,
        "eligible_delta": (
            None if eligible_current is None or eligible_previous is None else eligible_current - eligible_previous
        ),
        "segment_current": segment_current,
        "segment_previous": segment_previous,
        "segment_current_text": (
            str((board.get("segments") or [{}])[0].get("cutoff_value_text", "")).strip()
            if isinstance(board.get("segments"), list) and board.get("segments")
            else "-"
        ),
        "segment_previous_text": (
            str((baseline_board.get("segments") or [{}])[0].get("cutoff_value_text", "")).strip()
            if isinstance(baseline_board.get("segments"), list) and baseline_board.get("segments")
            else "-"
        ),
        "segment_delta": (
            None if segment_current is None or segment_previous is None else segment_current - segment_previous
        ),
    }


def _board_history_meta_map() -> dict[str, dict[str, Any]]:
    index = _load_history_index()
    meta_by_board: dict[str, dict[str, Any]] = {}
    for board_key, records in (index.get("boards") or {}).items():
        if not isinstance(records, list):
            continue
        capture_dates = [
            str(item.get("capture_key", item.get("capture_date", ""))).strip()
            for item in records
            if str(item.get("capture_key", item.get("capture_date", ""))).strip()
        ]
        latest = records[0] if records else None
        meta_by_board[str(board_key)] = {
            "history_dates": capture_dates,
            "history_capture_count": len(capture_dates),
            "latest_history_date": capture_dates[0] if capture_dates else "",
            "latest_history_updated_text": str((latest or {}).get("updated_text", "")).strip(),
            "latest_history_captured_at_utc": str((latest or {}).get("captured_at_utc", "")).strip(),
        }
    return meta_by_board


def _attach_history_meta(snapshot: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(snapshot, dict):
        return snapshot
    boards = snapshot.get("boards", [])
    if not isinstance(boards, list):
        return snapshot
    history_index = _load_history_index()
    history_boards = history_index.get("boards", {}) if isinstance(history_index, dict) else {}
    meta_by_board = _board_history_meta_map()
    for board in boards:
        if not isinstance(board, dict):
            continue
        history_meta = meta_by_board.get(str(board.get("board_key", "")).strip(), {})
        board["history_dates"] = list(history_meta.get("history_dates", []))
        board["history_capture_count"] = int(history_meta.get("history_capture_count", 0) or 0)
        board["latest_history_date"] = str(history_meta.get("latest_history_date", "")).strip()
        board["latest_history_updated_text"] = str(history_meta.get("latest_history_updated_text", "")).strip()
        board["latest_history_captured_at_utc"] = str(history_meta.get("latest_history_captured_at_utc", "")).strip()
        records = list((history_boards.get(str(board.get("board_key", "")).strip(), []) if isinstance(history_boards, dict) else []))
        board["history_compare"] = _history_compare_meta_for_board(board, records)
    return snapshot


def archive_competition_board_history(
    snapshot: dict[str, Any],
    *,
    capture_date: str | None = None,
    capture_keys_by_board: dict[str, str] | None = None,
    board_keys: list[str] | None = None,
) -> dict[str, Any]:
    if not isinstance(snapshot, dict):
        raise ValueError("snapshot must be a dict")
    boards = snapshot.get("boards", [])
    if not isinstance(boards, list):
        raise ValueError("snapshot.boards must be a list")
    normalized_capture_date = str(capture_date or "").strip()
    if normalized_capture_date and not re.fullmatch(r"\d{4}-\d{2}-\d{2}(?: \d{2}:00)?", normalized_capture_date):
        raise ValueError("capture_date must be YYYY-MM-DD or YYYY-MM-DD HH:00")
    wanted_keys = {str(item).strip() for item in (board_keys or []) if str(item).strip()}
    per_board_capture_keys = {
        str(key).strip(): str(value).strip()
        for key, value in (capture_keys_by_board or {}).items()
        if str(key).strip() and str(value).strip()
    }
    index = _load_history_index()
    boards_index = index.setdefault("boards", {})
    created: list[str] = []
    skipped: list[str] = []
    captured_at_utc = _now_iso()
    for board in boards:
        if not isinstance(board, dict):
            continue
        board_key = str(board.get("board_key", "")).strip()
        if not board_key:
            continue
        if wanted_keys and board_key not in wanted_keys:
            continue
        board_capture_key = per_board_capture_keys.get(board_key) or normalized_capture_date or _board_history_capture_key(board)
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}(?: \d{2}:00)?", board_capture_key):
            skipped.append(board_key)
            continue
        capture_granularity = "hourly" if " " in board_capture_key else "daily"
        capture_label = board_capture_key
        existing_records = boards_index.setdefault(board_key, [])
        if any(str(item.get("capture_key", item.get("capture_date", ""))).strip() == board_capture_key for item in existing_records):
            skipped.append(board_key)
            continue
        history_path = _history_file_path(board_key, board_capture_key)
        record_payload = {
            "board_key": board_key,
            "capture_key": board_capture_key,
            "capture_label": capture_label,
            "capture_date": board_capture_key.split(" ", 1)[0],
            "capture_granularity": capture_granularity,
            "captured_at_utc": captured_at_utc,
            "snapshot_generated_at_utc": str(snapshot.get("generated_at_utc", "")).strip(),
            "board": board,
        }
        _write_json_file(history_path, record_payload)
        relative_path = history_path.as_posix()
        existing_records = [
            item
            for item in existing_records
            if str(item.get("capture_key", item.get("capture_date", ""))).strip() != board_capture_key
        ]
        existing_records.append(
            {
                "capture_key": board_capture_key,
                "capture_label": capture_label,
                "capture_date": board_capture_key.split(" ", 1)[0],
                "capture_granularity": capture_granularity,
                "captured_at_utc": captured_at_utc,
                "path": relative_path,
                "label": str(board.get("label", "")).strip(),
                "updated_text": str(board.get("updated_text", "")).strip(),
                "updated_at_utc": str(board.get("updated_at_utc", "")).strip(),
                "eligible_user_count": int(board.get("eligible_user_count", 0) or 0),
                "current_floor_value_text": str(board.get("current_floor_value_text", "")).strip(),
            }
        )
        existing_records.sort(key=lambda item: str(item.get("capture_key", item.get("capture_date", ""))), reverse=True)
        boards_index[board_key] = existing_records
        created.append(board_key)
    _save_history_index(index)
    _attach_history_meta(snapshot)
    with _CACHE_LOCK:
        cached = _MEMORY_CACHE.get("data")
        if isinstance(cached, dict):
            _attach_history_meta(cached)
    return {
        "capture_date": normalized_capture_date,
        "created_board_keys": created,
        "skipped_board_keys": skipped,
        "capture_keys_by_board": per_board_capture_keys,
    }


def load_competition_board_history(board_key: str, *, capture_date: str | None = None) -> dict[str, Any]:
    normalized_key = str(board_key or "").strip()
    if not normalized_key:
        raise ValueError("board_key is required")
    index = _load_history_index()
    records = list((index.get("boards") or {}).get(normalized_key, []))
    available_dates = [
        str(item.get("capture_key", item.get("capture_date", ""))).strip()
        for item in records
        if str(item.get("capture_key", item.get("capture_date", ""))).strip()
    ]
    selected_date = str(capture_date or "").strip()
    if not selected_date and available_dates:
        selected_date = available_dates[0]
    selected_record = next(
        (
            item
            for item in records
            if str(item.get("capture_key", item.get("capture_date", ""))).strip() == selected_date
        ),
        None,
    )
    history_payload = None
    if selected_record is not None:
        history_payload = _read_json_file(Path(str(selected_record.get("path", "")).strip()), {})
        if not isinstance(history_payload, dict):
            history_payload = None
    return {
        "board_key": normalized_key,
        "available_dates": available_dates,
        "selected_date": selected_date,
        "record": selected_record,
        "history": history_payload,
    }


def _rank_cutoff_snapshot(board: dict[str, Any], targets: list[int]) -> dict[str, dict[str, Any]]:
    rows = board.get("rows", [])
    if not isinstance(rows, list):
        return {}
    row_by_rank = {
        int(item.get("rank", 0) or 0): item
        for item in rows
        if isinstance(item, dict) and int(item.get("rank", 0) or 0) > 0
    }
    cutoffs: dict[str, dict[str, Any]] = {}
    for rank in targets:
        row = row_by_rank.get(int(rank))
        if not isinstance(row, dict):
            continue
        value = _safe_float(row.get("value"))
        cutoffs[str(rank)] = {
            "rank": int(rank),
            "value": value,
            "value_text": str(row.get("value_text", "")).strip() or (f"{value:,.2f}" if value is not None else "-"),
            "name": str(row.get("name", "")).strip() or "-",
        }
    return cutoffs


def load_competition_board_trend(board_key: str, *, granularity: str = "daily") -> dict[str, Any]:
    normalized_key = str(board_key or "").strip()
    normalized_granularity = str(granularity or "daily").strip().lower() or "daily"
    if not normalized_key:
        raise ValueError("board_key is required")
    if normalized_granularity != "daily":
        raise ValueError("granularity must be daily")

    index = _load_history_index()
    records = list((index.get("boards") or {}).get(normalized_key, []))
    grouped_records: dict[str, dict[str, Any]] = {}
    for record in records:
        if not isinstance(record, dict):
            continue
        capture_date = str(record.get("capture_date", "")).strip() or str(record.get("capture_key", "")).strip().split(" ", 1)[0]
        if not capture_date:
            continue
        existing = grouped_records.get(capture_date)
        current_key = str(record.get("capture_key", record.get("capture_date", ""))).strip()
        existing_key = str((existing or {}).get("capture_key", (existing or {}).get("capture_date", ""))).strip()
        if existing is None or current_key > existing_key:
            grouped_records[capture_date] = record

    rank_targets = [1, 5, 20, 50, 200]
    points: list[dict[str, Any]] = []
    board_label = ""
    metric_label = ""
    for capture_date in sorted(grouped_records):
        record = grouped_records[capture_date]
        board = _load_history_board_from_record(record)
        if not isinstance(board, dict):
            continue
        if not board_label:
            board_label = str(board.get("label", "")).strip()
        if not metric_label:
            metric_label = str(board.get("metric_label", "")).strip() or "当前数值"
        eligible_metric_total = _safe_float(board.get("eligible_metric_total"))
        current_floor_value = _safe_float(board.get("current_floor_value"))
        updated_text = (
            str(board.get("updated_text", "")).strip()
            or _local_datetime_text(str(board.get("updated_at_utc", "")).strip())
            or str(record.get("updated_text", "")).strip()
        )
        points.append(
            {
                "date": capture_date,
                "capture_key": str(record.get("capture_key", capture_date)).strip() or capture_date,
                "updated_text": updated_text,
                "eligible_metric_total": eligible_metric_total,
                "eligible_metric_total_text": (
                    str(board.get("eligible_metric_total_text", "")).strip()
                    or (f"{eligible_metric_total:,.2f}" if eligible_metric_total is not None else "-")
                ),
                "eligible_user_count": _safe_int(board.get("eligible_user_count")) or 0,
                "current_floor_value": current_floor_value,
                "current_floor_value_text": str(board.get("current_floor_value_text", "")).strip() or "-",
                "rank_cutoffs": _rank_cutoff_snapshot(board, rank_targets),
            }
        )

    return {
        "board_key": normalized_key,
        "label": board_label or str((records[0] if records else {}).get("label", "")).strip() or normalized_key,
        "metric_label": metric_label or "当前数值",
        "granularity": normalized_granularity,
        "rank_targets": rank_targets,
        "points": points,
    }


def _trend_point_delta(points: list[dict[str, Any]], getter: Callable[[dict[str, Any]], float | None]) -> tuple[float, float]:
    if not points:
        return 0.0, 1.0
    latest = getter(points[-1])
    previous = getter(points[-2]) if len(points) >= 2 else latest
    earlier = getter(points[-3]) if len(points) >= 3 else previous
    latest_val = float(latest or 0.0)
    previous_val = float(previous or 0.0)
    earlier_val = float(earlier or previous_val)
    delta_1 = latest_val - previous_val
    delta_2 = previous_val - earlier_val
    weighted = 0.65 * delta_1 + 0.35 * delta_2
    baseline = abs(delta_2) if abs(delta_2) > 1e-9 else abs(delta_1) if abs(delta_1) > 1e-9 else 1.0
    acceleration = max(0.5, min(3.0, abs(delta_1) / baseline))
    return weighted, acceleration


def _forecast_stage_multiplier(days_remaining: float | None, acceleration: float, scenario: str) -> float:
    if days_remaining is None:
        stage_factor = 1.0
    elif days_remaining <= 1.0:
        stage_factor = max(1.35, min(2.40, acceleration * 1.15))
    elif days_remaining <= 2.0:
        stage_factor = max(1.10, min(1.60, 1.0 + (acceleration - 1.0) * 0.50))
    else:
        stage_factor = 1.0
    scenario_factor = {
        "conservative": 0.85,
        "base": 1.00,
        "aggressive": 1.20,
    }.get(str(scenario).strip().lower(), 1.0)
    return stage_factor * scenario_factor


def _forecast_days_remaining(board: dict[str, Any], *, now: datetime | None = None) -> float | None:
    end_at = _parse_iso_datetime(str(board.get("activity_end_at", "")).strip())
    if end_at is None:
        return None
    probe = now or datetime.now(timezone.utc)
    if probe.tzinfo is None:
        probe = probe.replace(tzinfo=timezone.utc)
    return max(0.0, (end_at.astimezone(timezone.utc) - probe.astimezone(timezone.utc)).total_seconds() / 86400.0)


def _enforce_cutoff_monotonicity(cutoffs: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    ordered = [1, 5, 20, 50, 200]
    previous_value: float | None = None
    for rank in ordered:
        item = cutoffs.get(str(rank))
        if not isinstance(item, dict):
            continue
        value = _safe_float(item.get("value"))
        if value is None:
            continue
        if previous_value is not None and value > previous_value:
            value = previous_value
            item["value"] = value
            item["value_text"] = f"{value:,.2f}"
        previous_value = value
    return cutoffs


def _interpolate_rank_delta(
    rank: int,
    rank_delta_map: dict[int, float],
    *,
    floor_rank: int,
    floor_delta: float,
) -> float:
    anchors: list[tuple[int, float]] = sorted(rank_delta_map.items())
    if floor_rank > 0:
        anchors.append((floor_rank, floor_delta))
    anchors = sorted({int(anchor_rank): float(anchor_delta) for anchor_rank, anchor_delta in anchors}.items())
    if not anchors:
        return 0.0
    if rank <= anchors[0][0]:
        return anchors[0][1]
    for index in range(len(anchors) - 1):
        left_rank, left_delta = anchors[index]
        right_rank, right_delta = anchors[index + 1]
        if left_rank <= rank <= right_rank:
            if right_rank == left_rank:
                return left_delta
            weight = (rank - left_rank) / float(right_rank - left_rank)
            return left_delta + (right_delta - left_delta) * weight
    return anchors[-1][1]


def _predicted_board_for_projection(
    board: dict[str, Any],
    scenario_cutoffs: dict[str, dict[str, Any]],
    total_delta: float,
) -> dict[str, Any]:
    predicted = dict(board)
    current_rows = board.get("rows", [])
    if not isinstance(current_rows, list):
        current_rows = []
    rank_delta_map: dict[int, float] = {}
    for key, item in scenario_cutoffs.items():
        if not isinstance(item, dict):
            continue
        rank_value = _safe_int(key)
        delta = _safe_float(item.get("delta"))
        if rank_value is None or delta is None:
            continue
        rank_delta_map[int(rank_value)] = float(delta)
    floor_rank = int(board.get("last_rank_fetched", len(current_rows)) or len(current_rows) or 200)
    floor_delta = _safe_float((scenario_cutoffs.get("floor") or {}).get("delta")) or 0.0
    predicted_rows: list[dict[str, Any]] = []
    for row in current_rows:
        if not isinstance(row, dict):
            continue
        rank = int(row.get("rank", 0) or 0)
        current_value = _safe_float(row.get("value")) or 0.0
        delta = _interpolate_rank_delta(rank, rank_delta_map, floor_rank=floor_rank, floor_delta=floor_delta)
        next_value = max(0.0, current_value + delta)
        predicted_rows.append(
            {
                **row,
                "value": next_value,
                "value_text": f"{next_value:,.2f}",
            }
        )
    predicted["rows"] = predicted_rows
    predicted["top_rows"] = predicted_rows[:10]
    predicted["tail_rows"] = predicted_rows[-10:]
    predicted_total = max(0.0, (_safe_float(board.get("eligible_metric_total")) or 0.0) + total_delta)
    predicted["eligible_metric_total"] = predicted_total
    predicted["eligible_metric_total_text"] = f"{predicted_total:,.2f}"
    floor_item = scenario_cutoffs.get("floor") or {}
    floor_value = _safe_float(floor_item.get("value"))
    if floor_value is not None:
        predicted["current_floor_value"] = floor_value
        predicted["current_floor_value_text"] = f"{floor_value:,.2f}"
    return predicted


def _find_entry_for_board(
    board_key: str,
    *,
    entry_id: str | None = None,
    name: str | None = None,
    entries: list[dict[str, Any]] | None = None,
) -> dict[str, Any] | None:
    normalized_entries = _normalize_entries(entries or _load_entries())
    normalized_board_key = str(board_key or "").strip()
    normalized_entry_id = str(entry_id or "").strip()
    normalized_name = str(name or "").strip().casefold()
    if normalized_entry_id:
        return next((item for item in normalized_entries if item.get("id") == normalized_entry_id), None)
    if normalized_name:
        return next(
            (
                item
                for item in normalized_entries
                if item.get("board_key") == normalized_board_key and str(item.get("name", "")).strip().casefold() == normalized_name
            ),
            None,
        )
    return None


def _forecast_board_with_entry(
    board: dict[str, Any],
    trend: dict[str, Any],
    *,
    next_day_volume: float,
    entry: dict[str, Any] | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    points = list(trend.get("points", [])) if isinstance(trend, dict) else []
    total_delta, total_acceleration = _trend_point_delta(points, lambda item: _safe_float(item.get("eligible_metric_total")))
    floor_delta, floor_acceleration = _trend_point_delta(points, lambda item: _safe_float(item.get("current_floor_value")))
    days_remaining = _forecast_days_remaining(board, now=now)
    rank_targets = [1, 5, 20, 50, 200]
    rank_base_deltas: dict[int, float] = {}
    rank_accelerations: dict[int, float] = {}
    for rank in rank_targets:
        rank_delta, rank_acc = _trend_point_delta(
            points,
            lambda item, key=str(rank): _safe_float(((item.get("rank_cutoffs") or {}).get(key) or {}).get("value")),
        )
        rank_base_deltas[rank] = rank_delta
        rank_accelerations[rank] = rank_acc

    predicted_entry = None
    if entry is not None:
        current_value = _safe_float(entry.get("value")) or 0.0
        predicted_entry = {
            **entry,
            "current_value": current_value,
            "predicted_value": current_value + float(next_day_volume),
            "next_day_volume": float(next_day_volume),
        }

    scenarios: dict[str, Any] = {}
    for scenario in ("conservative", "base", "aggressive"):
        total_multiplier = _forecast_stage_multiplier(days_remaining, total_acceleration, scenario)
        scenario_total_delta = max(0.0, total_delta * total_multiplier)
        scenario_cutoffs: dict[str, dict[str, Any]] = {}
        for rank in rank_targets:
            current_cutoff = _safe_float(next((row.get("value") for row in (board.get("rows") or []) if int(row.get("rank", 0) or 0) == rank), None))
            if current_cutoff is None:
                current_cutoff = _safe_float((((points[-1].get("rank_cutoffs") or {}) if points else {}).get(str(rank), {}) or {}).get("value")) or 0.0
            rank_multiplier = _forecast_stage_multiplier(days_remaining, rank_accelerations.get(rank, 1.0), scenario)
            predicted_value = max(0.0, current_cutoff + rank_base_deltas.get(rank, 0.0) * rank_multiplier)
            scenario_cutoffs[str(rank)] = {
                "rank": rank,
                "value": predicted_value,
                "value_text": f"{predicted_value:,.2f}",
                "delta": predicted_value - current_cutoff,
            }
        current_floor = _safe_float(board.get("current_floor_value")) or 0.0
        floor_multiplier = _forecast_stage_multiplier(days_remaining, floor_acceleration, scenario)
        predicted_floor = max(0.0, current_floor + floor_delta * floor_multiplier)
        scenario_cutoffs["floor"] = {
            "rank": int(board.get("last_rank_fetched", len(board.get("rows", []))) or len(board.get("rows", [])) or 200),
            "value": predicted_floor,
            "value_text": f"{predicted_floor:,.2f}",
            "delta": predicted_floor - current_floor,
        }
        _enforce_cutoff_monotonicity(scenario_cutoffs)
        predicted_board = _predicted_board_for_projection(board, scenario_cutoffs, scenario_total_delta)
        scenario_payload: dict[str, Any] = {
            "predicted_total": predicted_board["eligible_metric_total"],
            "predicted_total_text": predicted_board["eligible_metric_total_text"],
            "cutoffs": scenario_cutoffs,
        }
        if predicted_entry is not None:
            scenario_payload["projected_entry"] = _entry_projection(
                {
                    "id": str(predicted_entry.get("id", "")).strip() or "forecast_entry",
                    "board_key": str(board.get("board_key", "")).strip(),
                    "name": str(predicted_entry.get("name", "用户")).strip() or "用户",
                    "value": float(predicted_entry["predicted_value"]),
                    "note": str(predicted_entry.get("note", "")).strip(),
                    "updated_at_utc": _now_iso(),
                },
                predicted_board,
            )
        scenarios[scenario] = scenario_payload

    return {
        "board_key": str(board.get("board_key", "")).strip(),
        "board_label": str(board.get("label", "")).strip(),
        "metric_label": str(board.get("metric_label", "")).strip() or "当前数值",
        "days_remaining": days_remaining,
        "next_day_volume": float(next_day_volume),
        "entry": predicted_entry,
        "scenarios": scenarios,
        "notes": [
            "预测基于最近两天榜单增量加权外推，并叠加比赛阶段尾盘因子。",
            "最后一天或倒数第二天会自动放大预测，适合 OPN / ROBO 这类尾盘冲量明显的合约赛。",
        ],
    }


def build_competition_board_forecast(
    board_key: str,
    *,
    next_day_volume: float,
    entry_id: str | None = None,
    name: str | None = None,
    refresh: bool = False,
    now: datetime | None = None,
) -> dict[str, Any]:
    normalized_key = str(board_key or "").strip()
    if not normalized_key:
        raise ValueError("board_key is required")
    volume = _safe_float(next_day_volume)
    if volume is None or volume < 0:
        raise ValueError("next_day_volume must be a non-negative number")
    snapshot = build_competition_board_snapshot(refresh=refresh)
    boards = snapshot.get("boards", [])
    board = next((item for item in boards if isinstance(item, dict) and str(item.get("board_key", "")).strip() == normalized_key), None)
    if board is None:
        raise ValueError("board_key not found")
    trend = load_competition_board_trend(normalized_key, granularity="daily")
    entry = _find_entry_for_board(normalized_key, entry_id=entry_id, name=name)
    return _forecast_board_with_entry(board, trend, next_day_volume=volume, entry=entry, now=now)


def capture_daily_competition_board_history(*, capture_date: str | None = None) -> dict[str, Any]:
    normalized_capture_date = str(capture_date or _competition_board_local_date_text()).strip()
    existing_index = _load_history_index()
    boards_index = existing_index.get("boards", {}) if isinstance(existing_index, dict) else {}
    missing_board_keys = [
        board_key
        for board_key in _expected_board_keys()
        if not any(
            str(item.get("capture_date", "")).strip() == normalized_capture_date
            for item in (boards_index.get(board_key, []) if isinstance(boards_index, dict) else [])
        )
    ]
    if not missing_board_keys:
        return {
            "capture_date": normalized_capture_date,
            "status": "skipped_already_captured",
            "created_board_keys": [],
            "missing_board_keys": [],
        }
    snapshot = build_competition_board_snapshot(refresh=True)
    archive_result = archive_competition_board_history(
        snapshot,
        capture_date=normalized_capture_date,
        board_keys=missing_board_keys,
    )
    remaining_board_keys = [
        board_key
        for board_key in missing_board_keys
        if board_key not in set(archive_result["created_board_keys"])
    ]
    return {
        "capture_date": normalized_capture_date,
        "status": "captured" if archive_result["created_board_keys"] else "no_new_boards",
        "created_board_keys": archive_result["created_board_keys"],
        "skipped_board_keys": archive_result["skipped_board_keys"],
        "missing_board_keys": remaining_board_keys,
        "generated_at_utc": str(snapshot.get("generated_at_utc", "")).strip(),
    }


def capture_due_competition_board_history() -> dict[str, Any]:
    snapshot = build_competition_board_snapshot(refresh=True)
    index = _load_history_index()
    boards_index = index.get("boards", {}) if isinstance(index, dict) else {}
    board_capture_keys: dict[str, str] = {}
    missing_board_keys: list[str] = []
    for board in snapshot.get("boards", []):
        if not isinstance(board, dict):
            continue
        board_key = str(board.get("board_key", "")).strip()
        if not board_key:
            continue
        capture_key = _board_history_capture_key(board)
        board_capture_keys[board_key] = capture_key
        records = boards_index.get(board_key, []) if isinstance(boards_index, dict) else []
        exists = any(
            str(item.get("capture_key", item.get("capture_date", ""))).strip() == capture_key
            for item in records
            if isinstance(item, dict)
        )
        if not exists:
            missing_board_keys.append(board_key)
    if not missing_board_keys:
        return {
            "status": "skipped_no_new_update_slot",
            "created_board_keys": [],
            "skipped_board_keys": sorted(board_capture_keys),
            "missing_board_keys": [],
            "board_capture_keys": board_capture_keys,
            "generated_at_utc": str(snapshot.get("generated_at_utc", "")).strip(),
        }
    archive_result = archive_competition_board_history(
        snapshot,
        board_keys=missing_board_keys,
        capture_keys_by_board={key: board_capture_keys[key] for key in missing_board_keys},
    )
    remaining_board_keys = [
        board_key
        for board_key in missing_board_keys
        if board_key not in set(archive_result["created_board_keys"])
    ]
    return {
        "status": "captured" if archive_result["created_board_keys"] else "no_new_boards",
        "created_board_keys": archive_result["created_board_keys"],
        "skipped_board_keys": archive_result["skipped_board_keys"],
        "missing_board_keys": remaining_board_keys,
        "board_capture_keys": board_capture_keys,
        "generated_at_utc": str(snapshot.get("generated_at_utc", "")).strip(),
    }


def _competition_history_check_interval_seconds() -> int:
    raw = str(os.environ.get("GRID_COMPETITION_HISTORY_INTERVAL_SECONDS", "600")).strip()
    try:
        value = int(raw)
    except ValueError:
        value = 600
    return max(60, value)


def _competition_history_scheduler_enabled() -> bool:
    raw = str(os.environ.get("GRID_COMPETITION_HISTORY_SCHEDULER", "1")).strip().lower()
    return raw not in {"0", "false", "no", "off"}


def _competition_history_scheduler_loop() -> None:
    last_attempt_bucket = ""
    while True:
        try:
            now = _competition_board_local_now()
            bucket = now.strftime("%Y-%m-%d %H:%M")
            bucket = bucket[:-1] + "0"
            if bucket != last_attempt_bucket:
                last_attempt_bucket = bucket
                result = capture_due_competition_board_history()
                print(
                    "[competition_board_history_scheduler]",
                    f"bucket={bucket}",
                    f"status={result.get('status')}",
                    f"created={len(result.get('created_board_keys', []))}",
                )
        except Exception as exc:  # pragma: no cover
            print("[competition_board_history_scheduler]", f"error={type(exc).__name__}: {exc}")
        time.sleep(_competition_history_check_interval_seconds())


def start_competition_board_history_scheduler() -> bool:
    global _HISTORY_SCHEDULER_STARTED
    if not _competition_history_scheduler_enabled():
        return False
    with _HISTORY_SCHEDULER_LOCK:
        if _HISTORY_SCHEDULER_STARTED:
            return False
        worker = threading.Thread(target=_competition_history_scheduler_loop, daemon=True)
        worker.start()
        _HISTORY_SCHEDULER_STARTED = True
        return True


def _load_entries() -> list[dict[str, Any]]:
    payload = _read_json_file(ENTRIES_PATH, [])
    if not isinstance(payload, list):
        return []
    out: list[dict[str, Any]] = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        board_key = str(item.get("board_key", "")).strip()
        name = str(item.get("name", "")).strip()
        if not board_key or not name:
            continue
        out.append(
            {
                "id": str(item.get("id", uuid.uuid4().hex)).strip() or uuid.uuid4().hex,
                "board_key": board_key,
                "name": name,
                "value": _safe_float(item.get("value")) or 0.0,
                "note": str(item.get("note", "")).strip(),
                "updated_at_utc": str(item.get("updated_at_utc", _now_iso())).strip() or _now_iso(),
            }
        )
    return out


def _save_entries(entries: list[dict[str, Any]]) -> None:
    _write_json_file(ENTRIES_PATH, entries)


def _entry_identity(entry: dict[str, Any]) -> tuple[str, str]:
    return (
        str(entry.get("board_key", "")).strip(),
        str(entry.get("name", "")).strip().casefold(),
    )


def _normalize_entries(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    best_by_identity: dict[tuple[str, str], dict[str, Any]] = {}
    for entry in entries:
        identity = _entry_identity(entry)
        if not identity[0] or not identity[1]:
            continue
        existing = best_by_identity.get(identity)
        if existing is None or str(entry.get("updated_at_utc", "")) >= str(existing.get("updated_at_utc", "")):
            best_by_identity[identity] = entry
    normalized = list(best_by_identity.values())
    normalized.sort(key=lambda item: (item["board_key"], item["name"], item["updated_at_utc"]))
    return normalized


def _project_entries_for_boards(boards: list[dict[str, Any]]) -> list[dict[str, Any]]:
    entries = _normalize_entries(_load_entries())
    board_map = {item["board_key"]: item for item in boards}
    projected_entries: list[dict[str, Any]] = []
    for entry in entries:
        board = board_map.get(entry["board_key"])
        if board is None:
            continue
        projected = _entry_projection(entry, board)
        projected["board_label"] = board["label"]
        projected["metric_label"] = board["metric_label"]
        projected_entries.append(projected)
    projected_entries.sort(key=lambda item: (item["board_label"], item["projected_rank"], item["name"]))
    return projected_entries


def upsert_competition_entry(payload: dict[str, Any]) -> dict[str, Any]:
    board_key = str(payload.get("board_key", "")).strip()
    name = str(payload.get("name", "")).strip()
    if not board_key:
        raise ValueError("board_key is required")
    if not name:
        raise ValueError("name is required")
    value = _safe_float(payload.get("value"))
    if value is None or value < 0:
        raise ValueError("value must be a non-negative number")

    entries = _normalize_entries(_load_entries())
    entry_id = str(payload.get("id", "")).strip() or uuid.uuid4().hex
    note = str(payload.get("note", "")).strip()
    updated = False
    for item in entries:
        if item["id"] == entry_id:
            item.update(
                {
                    "board_key": board_key,
                    "name": name,
                    "value": value,
                    "note": note,
                    "updated_at_utc": _now_iso(),
                }
            )
            updated = True
            break
    if not updated and not str(payload.get("id", "")).strip():
        for item in entries:
            if item["board_key"] == board_key and item["name"].casefold() == name.casefold():
                item.update(
                    {
                        "name": name,
                        "value": value,
                        "note": note,
                        "updated_at_utc": _now_iso(),
                    }
                )
                entry_id = item["id"]
                updated = True
                break
    if not updated:
        entries.append(
            {
                "id": entry_id,
                "board_key": board_key,
                "name": name,
                "value": value,
                "note": note,
                "updated_at_utc": _now_iso(),
            }
        )
    entries = _normalize_entries(entries)
    _save_entries(entries)
    with _CACHE_LOCK:
        cached = _MEMORY_CACHE.get("data")
        if isinstance(cached, dict) and isinstance(cached.get("boards"), list):
            cached["entries"] = _project_entries_for_boards(cached["boards"])
    return {"entry_id": entry_id}


def delete_competition_entry(entry_id: str) -> dict[str, Any]:
    normalized_id = str(entry_id).strip()
    if not normalized_id:
        raise ValueError("entry_id is required")
    entries = [item for item in _normalize_entries(_load_entries()) if item["id"] != normalized_id]
    _save_entries(entries)
    with _CACHE_LOCK:
        cached = _MEMORY_CACHE.get("data")
        if isinstance(cached, dict) and isinstance(cached.get("boards"), list):
            cached["entries"] = _project_entries_for_boards(cached["boards"])
    return {"entry_id": normalized_id}


def _entry_projection(entry: dict[str, Any], board: dict[str, Any]) -> dict[str, Any]:
    value = float(entry["value"])
    rows = board.get("rows", [])
    higher = sum(1 for item in rows if float(item["value"]) > value)
    projected_rank = higher + 1
    projected_rank_text = str(projected_rank)
    if bool(board.get("rows_truncated")) and rows and value < float(rows[-1]["value"]):
        projected_rank = int(board.get("last_rank_fetched", len(rows)) or len(rows)) + 1
        projected_rank_text = f">{int(board.get('last_rank_fetched', len(rows)) or len(rows))}"
    reward_segment = next(
        (
            segment
            for segment in board.get("segments", [])
            if isinstance(segment.get("start_rank"), int)
            and (segment.get("end_rank") is None or projected_rank <= int(segment["end_rank"]))
            and projected_rank >= int(segment["start_rank"])
        ),
        None,
    )
    threshold_value = board.get("threshold_value")
    eligible = threshold_value is None or value >= float(threshold_value)
    projected_reward = ""
    if reward_segment and eligible:
        projected_reward = str(reward_segment.get("reward_text", ""))
        per_user_reward = reward_segment.get("per_user_reward")
        if per_user_reward is not None and board.get("reward_unit"):
            projected_reward = f"{per_user_reward:,.2f} {board['reward_unit']}".strip()
    gap_to_next = None
    if projected_rank > 1 and rows and projected_rank - 2 < len(rows):
        previous_row = rows[projected_rank - 2]
        gap_to_next = max(0.0, float(previous_row["value"]) - value)
    return {
        "id": entry["id"],
        "board_key": entry["board_key"],
        "name": entry["name"],
        "value": value,
        "value_text": f"{value:,.2f}",
        "note": entry["note"],
        "updated_at_utc": entry["updated_at_utc"],
        "projected_rank": projected_rank,
        "projected_rank_text": projected_rank_text,
        "eligible": eligible,
        "projected_reward": projected_reward,
        "segment_label": reward_segment.get("rank_label") if reward_segment else "",
        "gap_to_next": gap_to_next,
        "gap_to_next_text": f"{gap_to_next:,.2f}" if gap_to_next is not None else "",
    }


def _refresh_competition_data() -> dict[str, Any]:
    boards: list[dict[str, Any]] = []
    scrape_errors: list[str] = []

    for source in COMPETITION_SOURCES:
        hinted = _hinted_boards_for_source(source)
        if hinted is not None:
            meta, board_metas = hinted
        else:
            try:
                extracted = _run_playwright_extract(source.url)
            except Exception as exc:
                scrape_errors.append(f"{source.slug}: playwright extract failed: {type(exc).__name__}: {exc}")
                continue
            meta = extracted.get("meta", {}) if isinstance(extracted, dict) else {}
            board_metas = extracted.get("boards", []) if isinstance(extracted, dict) else []
            if not board_metas:
                scrape_errors.append(f"{source.slug}: no boards discovered")
                continue

        for board_meta in board_metas:
            resource_id = _safe_int(board_meta.get("resourceId"))
            try:
                leaderboard = (
                    _fetch_leaderboard_rows(
                        resource_id,
                        str(board_meta.get("url", source.url)),
                        max_rows=_safe_int(board_meta.get("maxRows")),
                    )
                    if resource_id is not None and resource_id > 0
                    else {
                        "resource_id": 0,
                        "eligible_user_count": 0,
                        "eligible_metric_total": 0.0,
                        "updated_time_ms": 0,
                        "rows_truncated": False,
                        "last_rank_fetched": 0,
                        "rows": [],
                    }
                )
                boards.append(_compose_board(source, board_meta, leaderboard, meta))
            except Exception as exc:
                scrape_errors.append(
                    f"{source.slug}:{board_meta.get('tabLabel', 'default')}: "
                    f"{type(exc).__name__}: {exc}"
                )

    generated_at = _now_iso()
    grouped: dict[str, list[dict[str, Any]]] = {"spot": [], "futures": []}
    for board in boards:
        grouped.setdefault(str(board["market"]), []).append(board)
    for market_rows in grouped.values():
        market_rows.sort(key=lambda item: (item["symbol"], item["tab_label"]))

    payload = {
        "generated_at_utc": generated_at,
        "boards": boards,
        "markets": grouped,
        "entries": _project_entries_for_boards(boards),
        "errors": scrape_errors,
    }
    _attach_history_meta(payload)
    _write_json_file(CACHE_PATH, payload)
    return payload


def _remember_snapshot(snapshot: dict[str, Any]) -> dict[str, Any]:
    _attach_history_meta(snapshot)
    with _CACHE_LOCK:
        _MEMORY_CACHE["loaded_at"] = time.time()
        _MEMORY_CACHE["data"] = snapshot
    return snapshot


def _load_cached_snapshot(*, max_age_seconds: float | None) -> dict[str, Any] | None:
    now = time.time()

    with _CACHE_LOCK:
        cached = _MEMORY_CACHE.get("data")
        loaded_at = float(_MEMORY_CACHE.get("loaded_at", 0.0) or 0.0)
        if cached is not None and (max_age_seconds is None or now - loaded_at <= max_age_seconds):
            return cached

    file_payload = _read_json_file(CACHE_PATH, {})
    generated = str(file_payload.get("generated_at_utc", "")).strip()
    if not generated:
        return None
    try:
        generated_ts = datetime.fromisoformat(generated).timestamp()
    except ValueError:
        generated_ts = 0.0
    if max_age_seconds is not None and now - generated_ts > max_age_seconds:
        return None

    if isinstance(file_payload.get("boards"), list):
        file_payload["entries"] = _project_entries_for_boards(file_payload["boards"])
    _attach_history_meta(file_payload)
    return _remember_snapshot(file_payload)


def build_competition_board_snapshot(*, refresh: bool = False) -> dict[str, Any]:
    if not refresh:
        cached = _load_cached_snapshot(max_age_seconds=CACHE_TTL_SECONDS)
        if cached is not None:
            return cached

        stale = _load_cached_snapshot(max_age_seconds=None)
        if stale is not None:
            return stale

    if refresh:
        if not _REFRESH_LOCK.acquire(blocking=False):
            stale = _load_cached_snapshot(max_age_seconds=None)
            return stale if stale is not None else _empty_snapshot(error="competition board refresh already in progress")
        try:
            fresh = _refresh_competition_data()
            return _remember_snapshot(fresh)
        finally:
            _REFRESH_LOCK.release()

    with _REFRESH_LOCK:
        cached = _load_cached_snapshot(max_age_seconds=CACHE_TTL_SECONDS)
        if cached is not None:
            return cached

        stale = _load_cached_snapshot(max_age_seconds=None)
        if stale is not None:
            return stale

        fresh = _refresh_competition_data()
        return _remember_snapshot(fresh)


COMPETITION_BOARD_PAGE = """<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>交易赛实时榜单</title>
  <style>
    :root {
      --bg: #f6f3ed;
      --panel: #ffffff;
      --line: #e4ddd0;
      --text: #171717;
      --muted: #6f6b62;
      --brand: #0b6f68;
      --brand-soft: #e6f6f4;
      --warn: #b76e00;
      --bad: #b42318;
      --good: #0f7b45;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: "Avenir Next", "PingFang SC", "Hiragino Sans GB", "Microsoft YaHei", sans-serif;
      color: var(--text);
      background: radial-gradient(circle at top left, #fffef8 0%, var(--bg) 48%, #ece7dc 100%);
      background-attachment: fixed;
      overflow-x: hidden;
    }
    body::before, body::after {
      content: "";
      position: fixed;
      inset: auto;
      width: 34vw;
      height: 34vw;
      border-radius: 999px;
      filter: blur(48px);
      opacity: 0.22;
      pointer-events: none;
      z-index: 0;
    }
    body::before {
      top: -8vw;
      right: -10vw;
      background: radial-gradient(circle, rgba(11, 111, 104, 0.30) 0%, rgba(11, 111, 104, 0) 72%);
    }
    body::after {
      bottom: -10vw;
      left: -8vw;
      background: radial-gradient(circle, rgba(183, 110, 0, 0.22) 0%, rgba(183, 110, 0, 0) 72%);
    }
    .wrap { max-width: 1480px; margin: 24px auto 48px; padding: 0 16px; display: grid; gap: 16px; }
    .card {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 16px;
      padding: 16px;
      box-shadow: 0 10px 32px rgba(17, 24, 39, 0.05);
      min-width: 0;
      position: relative;
      z-index: 1;
    }
    .header h1 { margin: 0 0 6px; font-size: 28px; }
    .header p { margin: 0; color: var(--muted); font-size: 14px; line-height: 1.6; }
    .toolbar, .header-links { margin-top: 12px; display: flex; gap: 10px; flex-wrap: wrap; }
    .toolbar input, .toolbar select {
      height: 38px;
      border-radius: 10px;
      border: 1px solid var(--line);
      padding: 0 10px;
      background: #fff;
      color: var(--text);
      font-size: 14px;
    }
    .toolbar label { display: flex; flex-direction: column; gap: 6px; font-size: 12px; color: var(--muted); min-width: 180px; }
    .header-links a, button {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      height: 38px;
      padding: 0 14px;
      border-radius: 10px;
      border: 1px solid var(--line);
      background: var(--brand-soft);
      color: #0f423f;
      text-decoration: none;
      font-size: 13px;
      font-weight: 700;
      cursor: pointer;
    }
    button.primary {
      background: var(--brand);
      border-color: var(--brand);
      color: #fff;
    }
    button.secondary {
      background: #fff;
      border-color: var(--line);
      color: var(--text);
    }
    button.danger {
      background: #fff2f2;
      border-color: #efc6c6;
      color: var(--bad);
    }
    .entry-actions { display: flex; gap: 8px; align-items: center; }
    .entry-editing-meta { margin-top: 8px; color: var(--warn); font-weight: 700; }
    .row-actions { display: flex; gap: 8px; justify-content: flex-end; }
    .cutline-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(120px, 1fr));
      gap: 10px;
      margin: 12px 0 14px;
    }
    .cutline-chip {
      border: 1px solid var(--line);
      border-radius: 14px;
      background: linear-gradient(180deg, rgba(255,255,255,0.95), rgba(248,244,236,0.95));
      padding: 10px 12px;
      min-width: 0;
    }
    .cutline-chip .label {
      font-size: 12px;
      color: var(--muted);
      margin-bottom: 6px;
    }
    .cutline-chip .value {
      font-size: 18px;
      font-weight: 800;
      line-height: 1.2;
      word-break: break-word;
    }
    .meta, .msg { font-size: 13px; color: var(--muted); }
    .msg.error { color: var(--bad); font-weight: 700; }
    .kpi-grid {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 12px;
    }
    .kpi {
      border: 1px solid var(--line);
      border-radius: 14px;
      padding: 14px;
      background: linear-gradient(180deg, #fffefc 0%, #faf8f2 100%);
    }
    .kpi .label { font-size: 12px; color: var(--muted); margin-bottom: 8px; }
    .kpi .value { font-size: 24px; font-weight: 800; letter-spacing: -0.02em; }
    .board-grid {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 16px;
      align-items: start;
    }
    .board-card {
      border: 1px solid var(--line);
      border-radius: 14px;
      padding: 14px;
      background: linear-gradient(180deg, #fff 0%, #faf8f2 100%);
      display: grid;
      gap: 12px;
      min-width: 0;
      overflow: hidden;
      box-shadow: 0 18px 40px rgba(17, 24, 39, 0.06);
      transition: transform 180ms ease, box-shadow 180ms ease, border-color 180ms ease;
      cursor: pointer;
    }
    .board-card:hover {
      transform: translateY(-3px);
      box-shadow: 0 22px 48px rgba(17, 24, 39, 0.10);
      border-color: #d5ccb9;
    }
    .board-card:focus-visible {
      outline: 2px solid rgba(11, 111, 104, 0.35);
      outline-offset: 2px;
    }
    .board-head {
      display: grid;
      grid-template-columns: minmax(0, 1fr) auto;
      gap: 12px;
      align-items: flex-start;
      min-width: 0;
    }
    .board-head > div { min-width: 0; }
    .board-head h3 { margin: 0; font-size: 18px; }
    .board-head p { margin: 4px 0 0; color: var(--muted); font-size: 12px; }
    .board-head a { word-break: break-all; }
    .event-strip {
      display: grid;
      grid-template-columns: minmax(0, 1.25fr) minmax(0, 1fr);
      gap: 12px;
      align-items: stretch;
    }
    .event-panel, .countdown-panel {
      border-radius: 14px;
      border: 1px solid var(--line);
      min-width: 0;
    }
    .event-panel {
      padding: 12px 14px;
      background:
        linear-gradient(135deg, rgba(11, 111, 104, 0.10), rgba(11, 111, 104, 0.02)),
        #fff;
    }
    .countdown-panel {
      padding: 12px 14px;
      background:
        linear-gradient(135deg, rgba(183, 110, 0, 0.12), rgba(183, 110, 0, 0.03)),
        #fff;
    }
    .event-panel .label, .countdown-panel .label {
      font-size: 11px;
      color: var(--muted);
      margin-bottom: 6px;
      letter-spacing: 0.04em;
      text-transform: uppercase;
    }
    .event-panel .value, .countdown-panel .value {
      font-size: 17px;
      font-weight: 800;
      line-height: 1.35;
    }
    .event-panel .sub, .countdown-panel .sub {
      margin-top: 6px;
      font-size: 12px;
      color: var(--muted);
      line-height: 1.5;
    }
    .countdown-value {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      padding: 10px 12px;
      border-radius: 12px;
      background: rgba(255, 255, 255, 0.72);
      box-shadow: inset 0 0 0 1px rgba(215, 199, 170, 0.7);
    }
    .countdown-value.warn {
      box-shadow: inset 0 0 0 1px rgba(245, 173, 86, 0.9);
      color: var(--warn);
    }
    .countdown-value.bad {
      box-shadow: inset 0 0 0 1px rgba(234, 120, 120, 0.9);
      color: var(--bad);
    }
    .countdown-digits {
      font-variant-numeric: tabular-nums;
      letter-spacing: 0.04em;
      white-space: nowrap;
    }
    .pill {
      display: inline-flex;
      align-items: center;
      min-height: 24px;
      padding: 0 10px;
      border-radius: 999px;
      font-size: 12px;
      font-weight: 700;
      border: 1px solid transparent;
      background: #f0ece3;
      color: #574c36;
      justify-self: end;
      white-space: nowrap;
    }
    .pill.good { background: #e7f7ef; color: var(--good); border-color: #c4ead4; }
    .pill.warn { background: #fff5e8; color: var(--warn); border-color: #f5d3a4; }
    .pill.bad { background: #fceaea; color: var(--bad); border-color: #f2c6c3; }
    .board-submeta {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      align-items: center;
    }
    .history-pill {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      min-height: 26px;
      padding: 0 10px;
      border-radius: 999px;
      background: #f6f1e6;
      color: #6e5832;
      border: 1px solid #e7d9bc;
      font-size: 12px;
      font-weight: 700;
      white-space: nowrap;
    }
    .history-hint {
      font-size: 12px;
      color: var(--muted);
    }
    .compare-inline {
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 10px;
    }
    .compare-inline-card {
      border: 1px solid var(--line);
      border-radius: 12px;
      padding: 10px 12px;
      background: #fff;
      min-width: 0;
    }
    .compare-inline-head {
      display: flex;
      justify-content: space-between;
      gap: 8px;
      align-items: center;
      margin-bottom: 6px;
    }
    .compare-inline-title {
      font-size: 11px;
      color: var(--muted);
    }
    .compare-inline-badge {
      font-size: 11px;
      font-weight: 700;
      color: #6e5832;
      background: #f6f1e6;
      border: 1px solid #e7d9bc;
      border-radius: 999px;
      padding: 2px 8px;
      white-space: nowrap;
    }
    .compare-inline-value {
      font-size: 15px;
      font-weight: 800;
      line-height: 1.35;
    }
    .compare-inline-delta {
      margin-top: 6px;
      font-size: 12px;
      font-weight: 700;
    }
    .compare-inline-delta.up { color: var(--good); }
    .compare-inline-delta.down { color: var(--bad); }
    .compare-inline-delta.flat { color: var(--muted); }
    .board-meta {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 10px;
    }
    .mini {
      border: 1px solid var(--line);
      border-radius: 12px;
      padding: 10px;
      background: #fff;
      min-width: 0;
    }
    .mini .label { font-size: 11px; color: var(--muted); margin-bottom: 4px; }
    .mini .value { font-size: 16px; font-weight: 700; }
    .table-wrap {
      width: 100%;
      max-width: 100%;
      overflow-x: auto;
      overflow-y: hidden;
      border: 1px solid var(--line);
      border-radius: 12px;
    }
    table { width: 100%; border-collapse: collapse; font-size: 13px; min-width: 0; table-layout: fixed; }
    .entries-table { min-width: 980px; table-layout: auto; }
    th, td { padding: 10px 8px; border-bottom: 1px solid var(--line); text-align: left; vertical-align: top; word-break: break-word; }
    th { font-size: 12px; color: var(--muted); font-weight: 700; background: #faf8f2; }
    td.num, th.num { text-align: right; white-space: nowrap; word-break: normal; }
    tbody tr:hover { background: #faf8f3; }
    .two-cols {
      display: grid;
      grid-template-columns: minmax(0, 1.15fr) minmax(0, 0.85fr);
      gap: 16px;
      align-items: start;
    }
    .two-cols > div { min-width: 0; }
    .empty {
      padding: 18px;
      border: 1px dashed var(--line);
      border-radius: 12px;
      color: var(--muted);
      font-size: 13px;
      text-align: center;
    }
    .history-modal {
      position: fixed;
      inset: 0;
      display: none;
      align-items: center;
      justify-content: center;
      padding: 24px;
      background: rgba(17, 24, 39, 0.44);
      z-index: 40;
    }
    .history-modal.open { display: flex; }
    .history-dialog {
      width: min(1180px, calc(100vw - 32px));
      max-height: calc(100vh - 48px);
      overflow: auto;
      border-radius: 18px;
      border: 1px solid var(--line);
      background: linear-gradient(180deg, #fffefb 0%, #f8f4ea 100%);
      box-shadow: 0 30px 90px rgba(17, 24, 39, 0.22);
      padding: 18px;
      display: grid;
      gap: 14px;
    }
    .history-head {
      display: flex;
      gap: 12px;
      align-items: flex-start;
      justify-content: space-between;
    }
    .history-head h3 {
      margin: 0;
      font-size: 22px;
    }
    .history-head p {
      margin: 6px 0 0;
      color: var(--muted);
      font-size: 13px;
      line-height: 1.6;
    }
    .history-date-list {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
    }
    .history-date-btn {
      height: 34px;
      padding: 0 12px;
      border-radius: 999px;
      border: 1px solid var(--line);
      background: #fff;
      color: #5c564a;
      font-size: 12px;
      font-weight: 700;
    }
    .history-date-btn.active {
      background: var(--brand);
      border-color: var(--brand);
      color: #fff;
    }
    .history-grid {
      display: grid;
      gap: 12px;
    }
    .compare-grid {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 10px;
    }
    .compare-card {
      border: 1px solid var(--line);
      border-radius: 12px;
      padding: 12px;
      background: #fff;
    }
    .compare-card .label {
      font-size: 11px;
      color: var(--muted);
      margin-bottom: 6px;
    }
    .compare-card .value {
      font-size: 16px;
      font-weight: 800;
      line-height: 1.35;
    }
    .compare-card .delta {
      margin-top: 6px;
      font-size: 12px;
      font-weight: 700;
    }
    .delta.up { color: var(--good); }
    .delta.down { color: var(--bad); }
    .delta.flat { color: var(--muted); }
    .trend-grid {
      display: grid;
      gap: 14px;
    }
    .trend-card {
      border: 1px solid var(--line);
      border-radius: 14px;
      background: #fff;
      padding: 14px;
      min-width: 0;
    }
    .trend-card h4 {
      margin: 0 0 6px;
      font-size: 16px;
    }
    .trend-card p {
      margin: 0 0 10px;
      color: var(--muted);
      font-size: 12px;
      line-height: 1.5;
    }
    .trend-legend {
      display: flex;
      flex-wrap: wrap;
      gap: 10px 14px;
      margin-bottom: 8px;
      font-size: 12px;
      color: var(--muted);
    }
    .trend-legend span {
      display: inline-flex;
      align-items: center;
      gap: 6px;
    }
    .trend-dot {
      width: 10px;
      height: 10px;
      border-radius: 999px;
      flex: 0 0 auto;
    }
    .trend-chart {
      width: 100%;
      min-height: 280px;
      overflow-x: auto;
    }
    .trend-chart svg {
      width: 100%;
      min-width: 760px;
      height: 280px;
      display: block;
    }
    .trend-axis-label {
      font-size: 11px;
      fill: #857b69;
    }
    .trend-grid-line {
      stroke: #e9e1d2;
      stroke-width: 1;
    }
    .trend-series-line {
      fill: none;
      stroke-width: 2.5;
      stroke-linecap: round;
      stroke-linejoin: round;
    }
    .trend-series-point {
      stroke: #fff;
      stroke-width: 2;
    }
    .trend-table-wrap {
      width: 100%;
      overflow-x: auto;
      border: 1px solid var(--line);
      border-radius: 12px;
    }
    .trend-table {
      min-width: 880px;
      table-layout: auto;
    }
    @media (max-width: 1180px) {
      .kpi-grid, .board-meta, .compare-grid, .compare-inline { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .board-grid, .two-cols { grid-template-columns: 1fr; }
      .event-strip { grid-template-columns: 1fr; }
    }
    @media (max-width: 680px) {
      .kpi-grid, .board-meta, .compare-grid, .compare-inline { grid-template-columns: 1fr; }
      .toolbar label { min-width: 100%; }
      .board-head { grid-template-columns: 1fr; }
      .pill { justify-self: start; }
    }
  </style>
</head>
<body>
  <div class="wrap">
    <section class="card header">
      <h1>交易赛实时榜单</h1>
      <p>聚合 Binance 现货 / 合约交易赛公开排行榜，展示奖励段、当前入围门槛，并支持录入参赛人员当前数值后实时估算排名。</p>
      <div class="header-links">
        <a href="/">返回策略测算页</a>
        <a href="/monitor">实盘监控</a>
        <a href="/spot_runner">现货执行台</a>
      </div>
      <div class="toolbar">
        <button id="refresh_btn" class="primary">刷新榜单数据</button>
        <span id="status" class="msg">等待加载。</span>
      </div>
      <div id="meta" class="meta"></div>
    </section>

    <section class="card">
      <div class="kpi-grid" id="summary"></div>
    </section>

    <section class="card">
      <h2 style="margin:0 0 12px;">参赛人员录入</h2>
      <div class="toolbar">
        <label>榜单
          <select id="entry_board"></select>
        </label>
        <label>姓名 / 备注名
          <input id="entry_name" type="text" placeholder="例如 小王 / OPN 1号号" />
        </label>
        <label>当前数值
          <input id="entry_value" type="number" step="0.01" min="0" placeholder="当前榜单数值" />
        </label>
        <label>备注
          <input id="entry_note" type="text" placeholder="例如 主号 / 子号 / API号" />
        </label>
        <div class="entry-actions">
          <button id="entry_save_btn" class="primary">保存录入</button>
          <button id="entry_cancel_btn" class="secondary" type="button" hidden>取消编辑</button>
        </div>
      </div>
      <div id="entry_editing_meta" class="entry-editing-meta" hidden></div>
      <p class="meta">现货页按“交易量 (USD)”估算；合约页按榜单实际统计字段估算，例如“交易量 (USDT)”。</p>
      <div class="table-wrap">
        <table class="entries-table">
          <thead>
            <tr>
              <th>榜单</th>
              <th>姓名</th>
              <th class="num">当前数值</th>
              <th class="num">预计排名</th>
              <th>奖励段</th>
              <th>预计奖励</th>
              <th class="num">追上前一名还差</th>
              <th>备注</th>
              <th></th>
            </tr>
          </thead>
          <tbody id="entries_body"></tbody>
        </table>
      </div>
    </section>

    <section class="card">
      <h2 style="margin:0 0 12px;">次日榜单预测</h2>
      <div class="toolbar">
        <label>榜单
          <select id="forecast_board"></select>
        </label>
        <label>关联录入用户（可选）
          <select id="forecast_entry"></select>
        </label>
        <label>预计下一天交易量
          <input id="forecast_next_volume" type="number" step="0.01" min="0" placeholder="例如 500000" />
        </label>
        <div class="entry-actions">
          <button id="forecast_run_btn" class="primary">预测</button>
        </div>
      </div>
      <p class="meta">基于最近两天榜单总量和关键名次门槛的变化做情景预测。最后一天会自动放大增量，适合 OPN / ROBO 这类尾盘冲量明显的比赛。</p>
      <div id="forecast_body">
        <div class="empty">先选择榜单并输入预计下一天交易量。</div>
      </div>
    </section>

    <section class="card">
      <h2 style="margin:0 0 12px;">现货交易赛</h2>
      <div id="spot_boards" class="board-grid"></div>
    </section>

    <section class="card">
      <h2 style="margin:0 0 12px;">合约交易赛</h2>
      <div id="futures_boards" class="board-grid"></div>
    </section>

    <section class="card">
      <h2 style="margin:0 0 12px;">已结束交易赛</h2>
      <div id="ended_boards" class="board-grid"></div>
    </section>
  </div>

  <div id="history_modal" class="history-modal" aria-hidden="true">
    <div class="history-dialog">
      <div class="history-head">
        <div>
          <h3 id="history_title">日榜历史</h3>
          <p id="history_meta">点击日期查看该日抓取到的排行榜快照。</p>
        </div>
        <button id="history_close_btn" type="button">关闭</button>
      </div>
      <div id="history_dates" class="history-date-list"></div>
      <div id="history_body" class="history-grid"></div>
    </div>
  </div>

  <div id="trend_modal" class="history-modal" aria-hidden="true">
    <div class="history-dialog">
      <div class="history-head">
        <div>
          <h3 id="trend_title">按天趋势</h3>
          <p id="trend_meta">按天查看交易赛总量和关键排名分数线的变化。</p>
        </div>
        <button id="trend_close_btn" type="button">关闭</button>
      </div>
      <div id="trend_body" class="trend-grid"></div>
    </div>
  </div>

  <script>
    const refreshBtn = document.getElementById("refresh_btn");
    const statusEl = document.getElementById("status");
    const metaEl = document.getElementById("meta");
    const summaryEl = document.getElementById("summary");
    const spotBoardsEl = document.getElementById("spot_boards");
    const futuresBoardsEl = document.getElementById("futures_boards");
    const endedBoardsEl = document.getElementById("ended_boards");
    const entryBoardEl = document.getElementById("entry_board");
    const entryNameEl = document.getElementById("entry_name");
    const entryValueEl = document.getElementById("entry_value");
    const entryNoteEl = document.getElementById("entry_note");
    const entrySaveBtn = document.getElementById("entry_save_btn");
    const entryCancelBtn = document.getElementById("entry_cancel_btn");
    const entryEditingMetaEl = document.getElementById("entry_editing_meta");
    const entriesBody = document.getElementById("entries_body");
    const forecastBoardEl = document.getElementById("forecast_board");
    const forecastEntryEl = document.getElementById("forecast_entry");
    const forecastNextVolumeEl = document.getElementById("forecast_next_volume");
    const forecastRunBtn = document.getElementById("forecast_run_btn");
    const forecastBodyEl = document.getElementById("forecast_body");
    const historyModalEl = document.getElementById("history_modal");
    const historyTitleEl = document.getElementById("history_title");
    const historyMetaEl = document.getElementById("history_meta");
    const historyDatesEl = document.getElementById("history_dates");
    const historyBodyEl = document.getElementById("history_body");
    const historyCloseBtn = document.getElementById("history_close_btn");
    const trendModalEl = document.getElementById("trend_modal");
    const trendTitleEl = document.getElementById("trend_title");
    const trendMetaEl = document.getElementById("trend_meta");
    const trendBodyEl = document.getElementById("trend_body");
    const trendCloseBtn = document.getElementById("trend_close_btn");
    const apiBase = `${window.location.protocol}//${window.location.host}`;

    let snapshot = null;
    let historyState = { boardKey: "", selectedDate: "", loading: false };
    let trendState = { boardKey: "", loading: false };
    let editingEntryId = "";

    function escapeHtml(value) {
      return String(value ?? "")
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#39;");
    }

    function fmtNum(value, digits = 2) {
      if (value === null || value === undefined || Number.isNaN(Number(value))) return "-";
      return Number(value).toLocaleString(undefined, {
        minimumFractionDigits: digits,
        maximumFractionDigits: digits,
      });
    }

    function fmtDate(value) {
      if (!value) return "-";
      const d = new Date(value);
      if (Number.isNaN(d.getTime())) return String(value);
      return d.toLocaleString();
    }

    function countdownState(value) {
      if (!value) {
        return { text: "未配置", sub: "等待补充结束时间", level: "" };
      }
      const end = new Date(value);
      if (Number.isNaN(end.getTime())) {
        return { text: String(value), sub: "结束时间格式异常", level: "" };
      }
      const diff = end.getTime() - Date.now();
      if (diff <= 0) {
        return { text: "已结束", sub: `结束于 ${fmtDate(value)}`, level: "bad" };
      }
      const totalSeconds = Math.floor(diff / 1000);
      const days = Math.floor(totalSeconds / 86400);
      const hours = Math.floor((totalSeconds % 86400) / 3600);
      const minutes = Math.floor((totalSeconds % 3600) / 60);
      const seconds = totalSeconds % 60;
      const level = diff <= 6 * 3600 * 1000 ? "bad" : diff <= 24 * 3600 * 1000 ? "warn" : "";
      return {
        text: `${days}天 ${String(hours).padStart(2, "0")}时 ${String(minutes).padStart(2, "0")}分 ${String(seconds).padStart(2, "0")}秒`,
        sub: `结束时间 ${fmtDate(value)}`,
        level,
      };
    }

    function boardHasEnded(board) {
      const value = board && board.activity_end_at ? String(board.activity_end_at) : "";
      if (!value) return false;
      const end = new Date(value);
      if (Number.isNaN(end.getTime())) return false;
      return end.getTime() <= Date.now();
    }

    function countdownMarkup(board) {
      const countdown = countdownState(board.activity_end_at);
      return `
        <div class="countdown-panel" data-countdown-end="${escapeHtml(board.activity_end_at || "")}">
          <div class="label">结束倒计时</div>
          <div class="countdown-value ${escapeHtml(countdown.level)}">
            <span class="countdown-digits">${escapeHtml(countdown.text)}</span>
          </div>
          <div class="sub">${escapeHtml(countdown.sub)}</div>
        </div>
      `;
    }

    function historyPillMarkup(board) {
      if (Array.isArray(board.history_dates) && board.history_dates.length) {
        return `<span class="history-pill">历史已归档 ${escapeHtml(board.latest_history_date || board.history_dates[0])}</span>`;
      }
      return `<span class="history-pill">待归档日榜</span>`;
    }

    function updateCountdownPanels() {
      document.querySelectorAll(".countdown-panel[data-countdown-end]").forEach((panel) => {
        const state = countdownState(panel.dataset.countdownEnd || "");
        const valueEl = panel.querySelector(".countdown-value");
        const digitsEl = panel.querySelector(".countdown-digits");
        const subEl = panel.querySelector(".sub");
        if (valueEl) {
          valueEl.className = `countdown-value ${state.level}`.trim();
        }
        if (digitsEl) {
          digitsEl.textContent = state.text;
        }
        if (subEl) {
          subEl.textContent = state.sub;
        }
      });
    }

    function setStatus(text, isError = false) {
      statusEl.textContent = text;
      statusEl.className = isError ? "msg error" : "msg";
    }

    function boardPill(board) {
      const threshold = board.threshold_value !== null && board.threshold_value !== undefined
        ? `官方门槛 ${fmtNum(board.threshold_value, 0)} ${escapeHtml(board.threshold_unit || board.leaderboard_unit || "")}`
        : "未解析出官方门槛";
      return `<span class="pill ${board.market === "spot" ? "good" : "warn"}">${threshold}</span>`;
    }

    function compareDeltaText(value, digits = 2, suffix = "") {
      const number = Number(value);
      if (!Number.isFinite(number)) return { text: "缺少对比样本", cls: "flat" };
      if (number === 0) return { text: `0${suffix}`, cls: "flat" };
      return {
        text: `${number > 0 ? "+" : ""}${fmtNum(number, digits)}${suffix}`,
        cls: number > 0 ? "up" : "down",
      };
    }

    function boardCompareMarkup(board) {
      const compare = board && board.history_compare ? board.history_compare : null;
      if (!compare || !compare.previous_capture_key) return "";
      const previousBadge = compare.previous_updated_text || compare.previous_capture_key || "-";
      const floorDelta = compareDeltaText(compare.floor_delta);
      const eligibleDelta = compareDeltaText(compare.eligible_delta, 0, " 人");
      const segmentDelta = compareDeltaText(compare.segment_delta);
      return `
        <div>
          <div class="meta" style="margin-bottom:8px;">${escapeHtml(compare.title || "上一档对比")}</div>
          <div class="compare-inline">
            <div class="compare-inline-card">
              <div class="compare-inline-head">
                <div class="compare-inline-title">最低上榜值</div>
                <span class="compare-inline-badge">${escapeHtml(previousBadge)}</span>
              </div>
              <div class="compare-inline-value">${escapeHtml(compare.floor_previous_text || "-")} → ${escapeHtml(compare.floor_current_text || "-")}</div>
              <div class="compare-inline-delta ${escapeHtml(floorDelta.cls)}">${escapeHtml(floorDelta.text)}</div>
            </div>
            <div class="compare-inline-card">
              <div class="compare-inline-head">
                <div class="compare-inline-title">合格人数</div>
                <span class="compare-inline-badge">${escapeHtml(previousBadge)}</span>
              </div>
              <div class="compare-inline-value">${escapeHtml(Number(compare.eligible_previous || 0).toLocaleString())} → ${escapeHtml(Number(compare.eligible_current || 0).toLocaleString())}</div>
              <div class="compare-inline-delta ${escapeHtml(eligibleDelta.cls)}">${escapeHtml(eligibleDelta.text)}</div>
            </div>
            <div class="compare-inline-card">
              <div class="compare-inline-head">
                <div class="compare-inline-title">首档门槛</div>
                <span class="compare-inline-badge">${escapeHtml(previousBadge)}</span>
              </div>
              <div class="compare-inline-value">${escapeHtml(compare.segment_previous_text || "-")} → ${escapeHtml(compare.segment_current_text || "-")}</div>
              <div class="compare-inline-delta ${escapeHtml(segmentDelta.cls)}">${escapeHtml(segmentDelta.text)}</div>
            </div>
          </div>
        </div>
      `;
    }

    function renderSummary(data) {
      const boards = Array.isArray(data.boards) ? data.boards : [];
      const entryCount = Array.isArray(data.entries) ? data.entries.length : 0;
      const totalBoards = boards.length;
      const totalUsers = boards.reduce((acc, item) => acc + Number(item.eligible_user_count || 0), 0);
      const endedCount = boards.filter((item) => boardHasEnded(item)).length;
      const rows = [
        { label: "榜单数", value: totalBoards },
        { label: "参赛样本总数", value: totalUsers.toLocaleString() },
        { label: "录入人数", value: entryCount },
        { label: "已结束榜单", value: endedCount },
        { label: "最近生成时间", value: fmtDate(data.generated_at_utc) },
      ];
      summaryEl.innerHTML = rows.map((item) => `
        <div class="kpi">
          <div class="label">${escapeHtml(item.label)}</div>
          <div class="value">${escapeHtml(item.value)}</div>
        </div>
      `).join("");
    }

    function segmentRows(board) {
      const rows = Array.isArray(board.segments) ? board.segments : [];
      if (!rows.length) {
        return `<div class="empty">未解析出官方奖励段，已保留原始规则文本。</div>`;
      }
      return `
        <div class="table-wrap">
          <table class="segment-table">
            <thead>
              <tr>
                <th>榜段</th>
                <th>奖励</th>
                <th class="num">当前门槛</th>
              </tr>
            </thead>
            <tbody>
              ${rows.map((item) => `
                <tr>
                  <td>${escapeHtml(item.rank_label || "-")}</td>
                  <td>${escapeHtml(item.reward_text || "-")}</td>
                  <td class="num">${escapeHtml(item.cutoff_value_text || "-")}</td>
                </tr>
              `).join("")}
            </tbody>
          </table>
        </div>
      `;
    }

    function topRows(board) {
      const rows = Array.isArray(board.top_rows) ? board.top_rows : [];
      if (!rows.length) {
        return `<div class="empty">没有榜单数据。</div>`;
      }
      return `
        <div class="table-wrap">
          <table class="top-table">
            <thead>
              <tr>
                <th class="num">排名</th>
                <th>姓名</th>
                <th class="num">${escapeHtml(board.metric_label || "当前数值")}</th>
              </tr>
            </thead>
            <tbody>
              ${rows.map((item) => `
                <tr>
                  <td class="num">${escapeHtml(item.rank)}</td>
                  <td>${escapeHtml(item.name)}</td>
                  <td class="num">${escapeHtml(item.value_text)}</td>
                </tr>
              `).join("")}
            </tbody>
          </table>
        </div>
      `;
    }

    function keyCutlinesMarkup(board) {
      const rows = Array.isArray(board.rows) ? board.rows : [];
      if (!rows.length) return "";
      const targets = [1, 2, 3, 4, 5, 20, 50, 200];
      const rowByRank = new Map(rows.map((item) => [Number(item.rank), item]));
      const items = targets
        .map((rank) => {
          const row = rowByRank.get(rank);
          if (!row) return "";
          return `
            <div class="cutline-chip">
              <div class="label">第 ${escapeHtml(rank)} 名分数线</div>
              <div class="value">${escapeHtml(row.value_text || "-")}</div>
            </div>
          `;
        })
        .filter(Boolean);
      if (!items.length) return "";
      return `
        <div>
          <div class="meta" style="margin-bottom:8px;">关键分数线</div>
          <div class="cutline-grid">${items.join("")}</div>
        </div>
      `;
    }

    function renderTrendChart(points, series, { logScale = false } = {}) {
      const width = 920;
      const height = 280;
      const pad = { top: 18, right: 20, bottom: 46, left: 66 };
      const chartWidth = width - pad.left - pad.right;
      const chartHeight = height - pad.top - pad.bottom;
      const validSeries = series
        .map((item) => ({
          ...item,
          values: points.map((point) => Number(item.pick(point))),
        }))
        .filter((item) => item.values.some((value) => Number.isFinite(value) && value > (logScale ? 0 : -Infinity)));
      if (!validSeries.length) {
        return `<div class="empty">历史归档还不够，暂时画不出趋势图。</div>`;
      }
      const allValues = validSeries
        .flatMap((item) => item.values)
        .filter((value) => Number.isFinite(value) && value > (logScale ? 0 : -Infinity));
      if (!allValues.length) {
        return `<div class="empty">缺少有效数值。</div>`;
      }
      const transformValue = (value) => {
        if (!Number.isFinite(value)) return NaN;
        if (logScale) {
          return value > 0 ? Math.log10(value) : NaN;
        }
        return value;
      };
      const minRaw = Math.min(...allValues);
      const maxRaw = Math.max(...allValues);
      const minValue = transformValue(logScale ? Math.max(minRaw, 1) : 0);
      const maxValue = transformValue(maxRaw);
      const span = maxValue - minValue || 1;
      const x = (index) => {
        if (points.length <= 1) return pad.left + chartWidth / 2;
        return pad.left + (chartWidth * index) / (points.length - 1);
      };
      const y = (value) => {
        const transformed = transformValue(value);
        if (!Number.isFinite(transformed)) return NaN;
        return pad.top + chartHeight - ((transformed - minValue) / span) * chartHeight;
      };
      const gridLines = Array.from({ length: 4 }, (_, index) => {
        const ratio = index / 3;
        const yPos = pad.top + chartHeight * ratio;
        const value = maxValue - span * ratio;
        const labelRaw = logScale ? 10 ** value : value;
        return `
          <line class="trend-grid-line" x1="${pad.left}" y1="${yPos}" x2="${width - pad.right}" y2="${yPos}" />
          <text class="trend-axis-label" x="${pad.left - 10}" y="${yPos + 4}" text-anchor="end">${escapeHtml(fmtNum(labelRaw, 0))}</text>
        `;
      }).join("");
      const xLabels = points.map((point, index) => `
        <text class="trend-axis-label" x="${x(index)}" y="${height - 14}" text-anchor="middle">${escapeHtml(point.date.slice(5))}</text>
      `).join("");
      const lines = validSeries.map((item) => {
        const coords = item.values
          .map((value, index) => {
            const yPos = y(value);
            return Number.isFinite(yPos) ? `${x(index)},${yPos}` : "";
          })
          .filter(Boolean)
          .join(" ");
        const circles = item.values.map((value, index) => {
          const yPos = y(value);
          if (!Number.isFinite(yPos)) return "";
          return `<circle class="trend-series-point" cx="${x(index)}" cy="${yPos}" r="4" fill="${item.color}" />`;
        }).join("");
        return `
          <polyline class="trend-series-line" points="${coords}" stroke="${item.color}" />
          ${circles}
        `;
      }).join("");
      return `
        <div class="trend-legend">
          ${validSeries.map((item) => `<span><i class="trend-dot" style="background:${escapeHtml(item.color)};"></i>${escapeHtml(item.name)}</span>`).join("")}
        </div>
        <div class="trend-chart">
          <svg viewBox="0 0 ${width} ${height}" preserveAspectRatio="none" aria-hidden="true">
            ${gridLines}
            ${lines}
            ${xLabels}
          </svg>
        </div>
      `;
    }

    function renderTrendTable(trend) {
      const points = Array.isArray(trend && trend.points) ? trend.points : [];
      if (!points.length) {
        return `<div class="empty">还没有足够的按天归档数据。</div>`;
      }
      return `
        <div class="trend-table-wrap">
          <table class="trend-table">
            <thead>
              <tr>
                <th>日期</th>
                <th class="num">总交易量</th>
                <th class="num">1名</th>
                <th class="num">5名</th>
                <th class="num">20名</th>
                <th class="num">50名</th>
                <th class="num">200名</th>
              </tr>
            </thead>
            <tbody>
              ${points.map((point) => `
                <tr>
                  <td>${escapeHtml(point.date)}<br /><span class="meta">${escapeHtml(point.updated_text || "-")}</span></td>
                  <td class="num">${escapeHtml(point.eligible_metric_total_text || "-")}</td>
                  <td class="num">${escapeHtml(((point.rank_cutoffs || {})["1"] || {}).value_text || "-")}</td>
                  <td class="num">${escapeHtml(((point.rank_cutoffs || {})["5"] || {}).value_text || "-")}</td>
                  <td class="num">${escapeHtml(((point.rank_cutoffs || {})["20"] || {}).value_text || "-")}</td>
                  <td class="num">${escapeHtml(((point.rank_cutoffs || {})["50"] || {}).value_text || "-")}</td>
                  <td class="num">${escapeHtml(((point.rank_cutoffs || {})["200"] || {}).value_text || "-")}</td>
                </tr>
              `).join("")}
            </tbody>
          </table>
        </div>
      `;
    }

    function renderTrendContent(trend) {
      const points = Array.isArray(trend && trend.points) ? trend.points : [];
      if (!points.length) {
        trendBodyEl.innerHTML = `<div class="empty">还没有足够的日级归档数据。</div>`;
        return;
      }
      const totalSeries = [
        { name: trend.metric_label || "总量", color: "#0b6f68", pick: (point) => point.eligible_metric_total },
      ];
      const cutoffSeries = [
        { name: "1名", color: "#b42318", pick: (point) => ((point.rank_cutoffs || {})["1"] || {}).value },
        { name: "5名", color: "#d97706", pick: (point) => ((point.rank_cutoffs || {})["5"] || {}).value },
        { name: "20名", color: "#0f7b45", pick: (point) => ((point.rank_cutoffs || {})["20"] || {}).value },
        { name: "50名", color: "#2563eb", pick: (point) => ((point.rank_cutoffs || {})["50"] || {}).value },
        { name: "200名", color: "#7c3aed", pick: (point) => ((point.rank_cutoffs || {})["200"] || {}).value },
      ];
      trendBodyEl.innerHTML = `
        <div class="trend-card">
          <h4>总交易量按天变化</h4>
          <p>按每天最后一次归档快照汇总，便于看整场比赛热度和增量变化。</p>
          ${renderTrendChart(points, totalSeries)}
        </div>
        <div class="trend-card">
          <h4>关键排名分数线按天变化</h4>
          <p>展示第 1 / 5 / 20 / 50 / 200 名的门槛变化。为了同时容纳高位和低位，图表使用对数尺度。</p>
          ${renderTrendChart(points, cutoffSeries, { logScale: true })}
        </div>
        <div class="trend-card">
          <h4>每日明细</h4>
          <p>表格里可以直接查看每天的总交易量和关键排名分数线。</p>
          ${renderTrendTable(trend)}
        </div>
      `;
    }

    function renderBoard(board) {
      return `
        <article class="board-card" data-board-key="${escapeHtml(board.board_key)}" tabindex="0" role="button" aria-label="查看 ${escapeHtml(board.label)} 的历史榜单">
          <div class="board-head">
            <div>
              <h3>${escapeHtml(board.label)}</h3>
              <p><a href="${escapeHtml(board.url)}" target="_blank" rel="noreferrer">${escapeHtml(board.url)}</a></p>
            </div>
            ${boardPill(board)}
          </div>
          <div class="board-submeta">
            ${historyPillMarkup(board)}
            <span class="history-hint">点击卡片查看按日期归档的日榜数据</span>
            <button type="button" class="secondary" data-trend-board-key="${escapeHtml(board.board_key)}">按天趋势</button>
          </div>
          ${boardCompareMarkup(board)}
          <div class="event-strip">
            <div class="event-panel">
              <div class="label">活动周期</div>
              <div class="value">${escapeHtml(board.activity_period_text || "未提供")}</div>
              <div class="sub">结束时间会随卡片倒计时实时提示。</div>
            </div>
            ${countdownMarkup(board)}
          </div>
          ${keyCutlinesMarkup(board)}
          <div class="board-meta">
            <div class="mini">
              <div class="label">榜单更新时间</div>
              <div class="value">${escapeHtml(board.updated_text || fmtDate(board.updated_at_utc))}</div>
            </div>
            <div class="mini">
              <div class="label">合格人数</div>
              <div class="value">${escapeHtml(Number(board.eligible_user_count || 0).toLocaleString())}</div>
            </div>
            <div class="mini">
              <div class="label">${board.rows_truncated ? "当前奖励区末位值" : "当前最低上榜值"}</div>
              <div class="value">${escapeHtml(board.current_floor_value_text || "-")}</div>
            </div>
            <div class="mini">
              <div class="label">榜单字段</div>
              <div class="value">${escapeHtml(board.metric_label || "-")}</div>
            </div>
          </div>
          <div class="two-cols">
            <div>
              <div class="meta" style="margin-bottom:8px;">奖励段与当前门槛</div>
              ${segmentRows(board)}
            </div>
            <div>
              <div class="meta" style="margin-bottom:8px;">当前前十</div>
              ${topRows(board)}
            </div>
          </div>
          <div class="meta">奖池：${escapeHtml(board.prize_pool_text || "-")} · 官方门槛：${board.threshold_value !== null && board.threshold_value !== undefined ? `${fmtNum(board.threshold_value, 0)} ${escapeHtml(board.threshold_unit || "")}` : "未识别"} · 当前总量：${escapeHtml(board.eligible_metric_total_text || "-")}</div>
        </article>
      `;
    }

    function renderBoards() {
      const boards = Array.isArray(snapshot && snapshot.boards) ? snapshot.boards : [];
      const activeBoards = boards.filter((item) => !boardHasEnded(item));
      const endedBoards = boards.filter((item) => boardHasEnded(item));
      const spot = activeBoards.filter((item) => item.market === "spot");
      const futures = activeBoards.filter((item) => item.market === "futures");
      spotBoardsEl.innerHTML = spot.length ? spot.map(renderBoard).join("") : `<div class="empty">暂无现货榜单。</div>`;
      futuresBoardsEl.innerHTML = futures.length ? futures.map(renderBoard).join("") : `<div class="empty">暂无合约榜单。</div>`;
      endedBoardsEl.innerHTML = endedBoards.length ? endedBoards.map(renderBoard).join("") : `<div class="empty">当前没有已结束榜单。</div>`;
      updateCountdownPanels();
    }

    function renderEntryBoardOptions() {
      const boards = Array.isArray(snapshot && snapshot.boards) ? snapshot.boards : [];
      const currentValue = entryBoardEl.value.trim();
      entryBoardEl.innerHTML = boards.map((item) => `
        <option value="${escapeHtml(item.board_key)}">${escapeHtml(item.label)} · ${escapeHtml(item.metric_label || "当前数值")}</option>
      `).join("");
      if (currentValue && boards.some((item) => item.board_key === currentValue)) {
        entryBoardEl.value = currentValue;
      }
    }

    function renderForecastBoardOptions() {
      const boards = Array.isArray(snapshot && snapshot.boards) ? snapshot.boards : [];
      const currentValue = forecastBoardEl.value.trim();
      forecastBoardEl.innerHTML = boards.map((item) => `
        <option value="${escapeHtml(item.board_key)}">${escapeHtml(item.label)} · ${escapeHtml(item.metric_label || "当前数值")}</option>
      `).join("");
      if (currentValue && boards.some((item) => item.board_key === currentValue)) {
        forecastBoardEl.value = currentValue;
      }
    }

    function renderForecastEntryOptions() {
      const entries = Array.isArray(snapshot && snapshot.entries) ? snapshot.entries : [];
      const boardKey = forecastBoardEl.value.trim();
      const currentValue = forecastEntryEl.value.trim();
      const boardEntries = entries.filter((item) => String(item.board_key || "") === boardKey);
      forecastEntryEl.innerHTML = `
        <option value="">不关联录入用户</option>
        ${boardEntries.map((item) => `
          <option value="${escapeHtml(item.id || "")}">${escapeHtml(item.name || "-")} · 当前 ${escapeHtml(item.value_text || "-")}</option>
        `).join("")}
      `;
      if (currentValue && boardEntries.some((item) => String(item.id || "") === currentValue)) {
        forecastEntryEl.value = currentValue;
      }
    }

    function renderForecastScenario(title, payload, metricLabel, nextDayVolume) {
      const cutoffs = payload && payload.cutoffs ? payload.cutoffs : {};
      const projectedEntry = payload && payload.projected_entry ? payload.projected_entry : null;
      return `
        <div class="card" style="padding:14px;">
          <div class="board-head">
            <div>
              <h3 style="margin:0;">${escapeHtml(title)}</h3>
              <p>预测次日总量 ${escapeHtml(payload.predicted_total_text || "-")} ${escapeHtml(metricLabel || "")}</p>
            </div>
          </div>
          <div class="cutline-grid">
            ${["1", "5", "20", "50", "200"].map((rank) => {
              const item = cutoffs[rank] || {};
              const delta = Number(item.delta || 0);
              const cls = delta > 0 ? "up" : delta < 0 ? "down" : "flat";
              const prefix = delta > 0 ? "+" : "";
              return `
                <div class="cutline-chip">
                  <div class="label">第 ${escapeHtml(rank)} 名门槛</div>
                  <div class="value">${escapeHtml(item.value_text || "-")}</div>
                  <div class="compare-inline-delta ${escapeHtml(cls)}">${escapeHtml(prefix + fmtNum(delta, 2))}</div>
                </div>
              `;
            }).join("")}
          </div>
          ${projectedEntry ? `
            <div class="compare-inline">
              <div class="compare-inline-card">
                <div class="compare-inline-head">
                  <div class="compare-inline-title">预测累计量</div>
                  <span class="compare-inline-badge">${escapeHtml(projectedEntry.name || "用户")}</span>
                </div>
                <div class="compare-inline-value">${escapeHtml(projectedEntry.value_text || "-")}</div>
                <div class="compare-inline-delta flat">次日新增 ${escapeHtml(fmtNum(nextDayVolume, 2))}</div>
              </div>
              <div class="compare-inline-card">
                <div class="compare-inline-head">
                  <div class="compare-inline-title">预测排名</div>
                  <span class="compare-inline-badge">${escapeHtml(projectedEntry.segment_label || "-")}</span>
                </div>
                <div class="compare-inline-value">${escapeHtml(projectedEntry.projected_rank_text || projectedEntry.projected_rank || "-")}</div>
                <div class="compare-inline-delta flat">差前一名 ${escapeHtml(projectedEntry.gap_to_next_text || "-")}</div>
              </div>
              <div class="compare-inline-card">
                <div class="compare-inline-head">
                  <div class="compare-inline-title">预测奖励</div>
                  <span class="compare-inline-badge">${escapeHtml(projectedEntry.board_label || "-")}</span>
                </div>
                <div class="compare-inline-value">${escapeHtml(projectedEntry.projected_reward || "-")}</div>
                <div class="compare-inline-delta flat">${escapeHtml(projectedEntry.note || "按当前录入推演")}</div>
              </div>
            </div>
          ` : `<div class="meta">未关联录入用户，本情景仅展示榜单总量和关键门槛预测。</div>`}
        </div>
      `;
    }

    function renderForecastResult(result) {
      const scenarios = result && result.scenarios ? result.scenarios : {};
      const notes = Array.isArray(result && result.notes) ? result.notes : [];
      const entry = result && result.entry ? result.entry : null;
      forecastBodyEl.innerHTML = `
        <div class="card" style="padding:14px; margin-bottom:12px;">
          <div class="board-head">
            <div>
              <h3 style="margin:0;">${escapeHtml(result.board_label || result.board_key || "次日榜单预测")}</h3>
              <p>预计下一天交易量 ${escapeHtml(fmtNum(result.next_day_volume, 2))} ${escapeHtml(result.metric_label || "")} · 剩余 ${result.days_remaining === null || result.days_remaining === undefined ? "-" : escapeHtml(fmtNum(result.days_remaining, 2))} 天</p>
            </div>
            ${entry ? `<span class="history-pill">关联用户：${escapeHtml(entry.name || "-")}</span>` : `<span class="history-pill">仅门槛预测</span>`}
          </div>
          ${notes.length ? `<div class="meta">${notes.map((item) => escapeHtml(item)).join(" · ")}</div>` : ""}
        </div>
        <div class="board-grid">
          ${renderForecastScenario("保守情景", scenarios.conservative || {}, result.metric_label || "", result.next_day_volume || 0)}
          ${renderForecastScenario("基准情景", scenarios.base || {}, result.metric_label || "", result.next_day_volume || 0)}
        </div>
        <div class="board-grid" style="margin-top:12px;">
          ${renderForecastScenario("激进情景", scenarios.aggressive || {}, result.metric_label || "", result.next_day_volume || 0)}
        </div>
      `;
    }

    function setEntryEditingState(entry = null) {
      editingEntryId = entry && entry.id ? String(entry.id) : "";
      const isEditing = Boolean(editingEntryId);
      entrySaveBtn.textContent = isEditing ? "更新录入" : "保存录入";
      entryCancelBtn.hidden = !isEditing;
      if (isEditing) {
        entryEditingMetaEl.hidden = false;
        entryEditingMetaEl.textContent = `正在编辑：${entry.board_label || "-"} / ${entry.name || "-"}`;
      } else {
        entryEditingMetaEl.hidden = true;
        entryEditingMetaEl.textContent = "";
      }
    }

    function resetEntryForm() {
      const firstBoardKey = entryBoardEl.options.length ? String(entryBoardEl.options[0].value || "") : "";
      setEntryEditingState(null);
      entryBoardEl.value = firstBoardKey;
      entryNameEl.value = "";
      entryValueEl.value = "";
      entryNoteEl.value = "";
    }

    function startEditingEntry(entryId) {
      const entries = Array.isArray(snapshot && snapshot.entries) ? snapshot.entries : [];
      const entry = entries.find((item) => String(item.id) === String(entryId));
      if (!entry) {
        setStatus("未找到要编辑的录入记录。", true);
        return;
      }
      renderEntryBoardOptions();
      entryBoardEl.value = entry.board_key || "";
      entryNameEl.value = entry.name || "";
      entryValueEl.value = Number.isFinite(Number(entry.value)) ? String(entry.value) : "";
      entryNoteEl.value = entry.note || "";
      setEntryEditingState(entry);
      entryNameEl.focus();
    }

    function renderEntries() {
      const entries = Array.isArray(snapshot && snapshot.entries) ? snapshot.entries : [];
      if (!entries.length) {
        entriesBody.innerHTML = `
          <tr>
            <td colspan="9">
              <div class="empty">还没有录入任何参赛人员。</div>
            </td>
          </tr>
        `;
        return;
      }
      entriesBody.innerHTML = entries.map((item) => `
        <tr>
          <td>${escapeHtml(item.board_label || "-")}</td>
          <td>${escapeHtml(item.name)}</td>
          <td class="num">${escapeHtml(item.value_text || "-")}</td>
          <td class="num">${escapeHtml(item.projected_rank_text || item.projected_rank || "-")}</td>
          <td>${escapeHtml(item.segment_label || "-")}</td>
          <td>${escapeHtml(item.projected_reward || "-")}</td>
          <td class="num">${escapeHtml(item.gap_to_next_text || "-")}</td>
          <td>${escapeHtml(item.note || "-")}</td>
          <td>
            <div class="row-actions">
              <button class="secondary" data-edit-id="${escapeHtml(item.id)}">编辑</button>
              <button class="danger" data-delete-id="${escapeHtml(item.id)}">删除</button>
            </div>
          </td>
        </tr>
      `).join("");
      entriesBody.querySelectorAll("button[data-edit-id]").forEach((btn) => {
        btn.addEventListener("click", () => {
          startEditingEntry(btn.dataset.editId || "");
        });
      });
      entriesBody.querySelectorAll("button[data-delete-id]").forEach((btn) => {
        btn.addEventListener("click", async () => {
          await saveEntry({ action: "delete", entry_id: btn.dataset.deleteId });
          if ((btn.dataset.deleteId || "") === editingEntryId) {
            resetEntryForm();
          }
          setStatus("录入已删除。");
        });
      });
    }

    function deltaMarkup(currentValue, previousValue, digits = 2, suffix = "") {
      const currentNum = Number(currentValue);
      const previousNum = Number(previousValue);
      if (!Number.isFinite(currentNum) || !Number.isFinite(previousNum)) {
        return `<div class="delta flat">缺少前一日数据</div>`;
      }
      const diff = currentNum - previousNum;
      const level = diff > 0 ? "up" : diff < 0 ? "down" : "flat";
      const prefix = diff > 0 ? "+" : "";
      return `<div class="delta ${level}">${prefix}${fmtNum(diff, digits)}${suffix}</div>`;
    }

    function renderHistoryCompare(board, previousBoard, meta = {}, previousMeta = {}) {
      if (!board) return "";
      if (!previousBoard) {
        return `
          <div class="card" style="padding:14px;">
            <div class="meta">前一日 vs 当前日</div>
            <div class="empty">当前日期之前还没有归档历史，暂时无法对比。</div>
          </div>
        `;
      }
      return `
        <div class="card" style="padding:14px;">
          <div class="board-head">
            <div>
              <h3 style="margin:0;">前一日 vs 当前日</h3>
              <p>${escapeHtml(previousMeta.capture_date || "-")} 对比 ${escapeHtml(meta.capture_date || "-")}</p>
            </div>
          </div>
          <div class="compare-grid">
            <div class="compare-card">
              <div class="label">最低上榜值变化</div>
              <div class="value">${escapeHtml(previousBoard.current_floor_value_text || "-")} → ${escapeHtml(board.current_floor_value_text || "-")}</div>
              ${deltaMarkup(board.current_floor_value, previousBoard.current_floor_value)}
            </div>
            <div class="compare-card">
              <div class="label">合格人数变化</div>
              <div class="value">${escapeHtml(Number(previousBoard.eligible_user_count || 0).toLocaleString())} → ${escapeHtml(Number(board.eligible_user_count || 0).toLocaleString())}</div>
              ${deltaMarkup(board.eligible_user_count, previousBoard.eligible_user_count, 0, " 人")}
            </div>
            <div class="compare-card">
              <div class="label">门槛榜段末位变化</div>
              <div class="value">${escapeHtml((previousBoard.segments && previousBoard.segments[0] && previousBoard.segments[0].cutoff_value_text) || "-")} → ${escapeHtml((board.segments && board.segments[0] && board.segments[0].cutoff_value_text) || "-")}</div>
              ${deltaMarkup(
                board.segments && board.segments[0] ? board.segments[0].cutoff_value : NaN,
                previousBoard.segments && previousBoard.segments[0] ? previousBoard.segments[0].cutoff_value : NaN
              )}
            </div>
            <div class="compare-card">
              <div class="label">榜单更新时间变化</div>
              <div class="value">${escapeHtml(previousBoard.updated_text || "-")} → ${escapeHtml(board.updated_text || "-")}</div>
              <div class="delta flat">用于判断官方榜单是否已推进</div>
            </div>
          </div>
        </div>
      `;
    }

    function renderHistoryBoard(board, meta = {}, previousBoard = null, previousMeta = {}) {
      if (!board) {
        historyBodyEl.innerHTML = `<div class="empty">该日期没有归档数据。</div>`;
        return;
      }
      const capturedAt = meta.captured_at_utc ? fmtDate(meta.captured_at_utc) : "-";
      historyBodyEl.innerHTML = `
        ${renderHistoryCompare(board, previousBoard, meta, previousMeta)}
        <div class="card" style="padding:14px;">
          <div class="board-head">
            <div>
              <h3 style="margin:0;">${escapeHtml(board.label || board.base_label || "历史日榜")}</h3>
              <p>归档日期：${escapeHtml(meta.capture_date || "-")} · 抓取时间：${escapeHtml(capturedAt)}</p>
            </div>
            ${boardPill(board)}
          </div>
          <div class="board-meta">
            <div class="mini">
              <div class="label">榜单更新时间</div>
              <div class="value">${escapeHtml(board.updated_text || fmtDate(board.updated_at_utc))}</div>
            </div>
            <div class="mini">
              <div class="label">合格人数</div>
              <div class="value">${escapeHtml(Number(board.eligible_user_count || 0).toLocaleString())}</div>
            </div>
            <div class="mini">
              <div class="label">${board.rows_truncated ? "当日奖励区末位值" : "当日最低上榜值"}</div>
              <div class="value">${escapeHtml(board.current_floor_value_text || "-")}</div>
            </div>
            <div class="mini">
              <div class="label">榜单字段</div>
              <div class="value">${escapeHtml(board.metric_label || "-")}</div>
            </div>
          </div>
          <div class="two-cols">
            <div>
              <div class="meta" style="margin-bottom:8px;">当日奖励段与门槛</div>
              ${segmentRows(board)}
            </div>
            <div>
              <div class="meta" style="margin-bottom:8px;">当日前十</div>
              ${topRows(board)}
            </div>
          </div>
        </div>
      `;
    }

    function closeHistoryModal() {
      historyModalEl.classList.remove("open");
      historyModalEl.setAttribute("aria-hidden", "true");
      historyBodyEl.innerHTML = "";
      historyDatesEl.innerHTML = "";
      historyState = { boardKey: "", selectedDate: "", loading: false };
    }

    function closeTrendModal() {
      trendModalEl.classList.remove("open");
      trendModalEl.setAttribute("aria-hidden", "true");
      trendBodyEl.innerHTML = "";
      trendState = { boardKey: "", loading: false };
    }

    async function openBoardHistory(boardKey, captureDate = "") {
      if (!boardKey) return;
      historyState = { boardKey, selectedDate: captureDate, loading: true };
      historyModalEl.classList.add("open");
      historyModalEl.setAttribute("aria-hidden", "false");
      historyTitleEl.textContent = "日榜历史";
      historyMetaEl.textContent = "正在加载归档数据...";
      historyDatesEl.innerHTML = "";
      historyBodyEl.innerHTML = `<div class="empty">正在加载该交易赛的历史快照...</div>`;
      try {
        const url = captureDate
          ? `${apiBase}/api/competition_board_history?board_key=${encodeURIComponent(boardKey)}&date=${encodeURIComponent(captureDate)}`
          : `${apiBase}/api/competition_board_history?board_key=${encodeURIComponent(boardKey)}`;
        const resp = await fetch(url);
        const data = await resp.json();
        if (!resp.ok || !data.ok) {
          throw new Error(data.error || `请求失败(${resp.status})`);
        }
        const payload = data.history || {};
        const board = payload.history && payload.history.board ? payload.history.board : null;
        const meta = payload.record || {};
        const dateList = Array.isArray(payload.available_dates) ? payload.available_dates : [];
        const currentIndex = dateList.indexOf(payload.selected_date);
        const previousDate = currentIndex >= 0 ? dateList[currentIndex + 1] || "" : "";
        let previousBoard = null;
        let previousMeta = {};
        if (previousDate) {
          const previousResp = await fetch(`${apiBase}/api/competition_board_history?board_key=${encodeURIComponent(boardKey)}&date=${encodeURIComponent(previousDate)}`);
          const previousData = await previousResp.json();
          if (previousResp.ok && previousData.ok) {
            const previousPayload = previousData.history || {};
            previousBoard = previousPayload.history && previousPayload.history.board ? previousPayload.history.board : null;
            previousMeta = previousPayload.record || {};
          }
        }
        historyState = { boardKey, selectedDate: payload.selected_date || "", loading: false };
        historyTitleEl.textContent = board && board.label ? `${board.label} 日榜历史` : "日榜历史";
        historyMetaEl.textContent = dateList && dateList.length
          ? `已归档 ${dateList.length} 天，点击日期可切换查看每日榜单快照，并对比前一日门槛变化。`
          : "还没有归档到历史日榜。";
        historyDatesEl.innerHTML = dateList.map((dateText) => `
          <button type="button" class="history-date-btn ${dateText === payload.selected_date ? "active" : ""}" data-history-date="${escapeHtml(dateText)}">
            ${escapeHtml(dateText)}
          </button>
        `).join("");
        historyDatesEl.querySelectorAll("button[data-history-date]").forEach((btn) => {
          btn.addEventListener("click", () => openBoardHistory(boardKey, btn.dataset.historyDate || ""));
        });
        renderHistoryBoard(board, meta, previousBoard, previousMeta);
      } catch (err) {
        historyMetaEl.textContent = "历史数据加载失败。";
        historyBodyEl.innerHTML = `<div class="empty">${escapeHtml(err.message || "加载失败")}</div>`;
      }
    }

    async function openBoardTrend(boardKey) {
      if (!boardKey) return;
      trendState = { boardKey, loading: true };
      trendModalEl.classList.add("open");
      trendModalEl.setAttribute("aria-hidden", "false");
      trendTitleEl.textContent = "按天趋势";
      trendMetaEl.textContent = "正在加载按天趋势数据...";
      trendBodyEl.innerHTML = `<div class="empty">正在整理该交易赛的按天趋势...</div>`;
      try {
        const resp = await fetch(`${apiBase}/api/competition_board_trend?board_key=${encodeURIComponent(boardKey)}&granularity=daily`);
        const data = await resp.json();
        if (!resp.ok || !data.ok) {
          throw new Error(data.error || `请求失败(${resp.status})`);
        }
        const trend = data.trend || {};
        trendState = { boardKey, loading: false };
        trendTitleEl.textContent = `${trend.label || boardKey} 按天趋势`;
        trendMetaEl.textContent = `按日聚合每个自然日最后一次归档快照，展示总交易量与关键排名门槛变化。`;
        renderTrendContent(trend);
      } catch (err) {
        trendMetaEl.textContent = "趋势数据加载失败。";
        trendBodyEl.innerHTML = `<div class="empty">${escapeHtml(err.message || "加载失败")}</div>`;
      }
    }

    function updateSnapshotMeta() {
      const errors = Array.isArray(snapshot && snapshot.errors) ? snapshot.errors : [];
      metaEl.textContent = errors.length
        ? `生成时间：${fmtDate(snapshot.generated_at_utc)} · 部分抓取异常：${errors.join(" | ")}`
        : `生成时间：${fmtDate(snapshot.generated_at_utc)}`;
    }

    async function loadSnapshot(refresh = false) {
      refreshBtn.disabled = true;
      setStatus(refresh ? "正在刷新榜单数据..." : "正在加载榜单数据...");
      try {
        const url = refresh ? `${apiBase}/api/competition_board?refresh=1` : `${apiBase}/api/competition_board`;
        const resp = await fetch(url);
        const data = await resp.json();
        if (!resp.ok || !data.ok) {
          throw new Error(data.error || `请求失败(${resp.status})`);
        }
        snapshot = data.snapshot;
        renderSummary(snapshot);
        renderBoards();
        renderEntryBoardOptions();
        renderForecastBoardOptions();
        renderForecastEntryOptions();
        renderEntries();
        updateSnapshotMeta();
        setStatus(`完成：现货 ${snapshot.markets.spot.length} 个榜单，合约 ${snapshot.markets.futures.length} 个榜单。`);
      } catch (err) {
        setStatus(`加载失败：${err.message}`, true);
      } finally {
        refreshBtn.disabled = false;
      }
    }

    async function saveEntry(payload) {
      try {
        const resp = await fetch(`${apiBase}/api/competition_entries`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        });
        const data = await resp.json();
        if (!resp.ok || !data.ok) {
          throw new Error(data.error || `请求失败(${resp.status})`);
        }
        if (data.snapshot) {
          snapshot = data.snapshot;
          renderSummary(snapshot);
          renderForecastBoardOptions();
          renderForecastEntryOptions();
          renderEntries();
          updateSnapshotMeta();
        } else {
          await loadSnapshot(false);
        }
      } catch (err) {
        setStatus(`保存失败：${err.message}`, true);
        throw err;
      }
    }

    refreshBtn.addEventListener("click", () => loadSnapshot(true));
    historyCloseBtn.addEventListener("click", closeHistoryModal);
    trendCloseBtn.addEventListener("click", closeTrendModal);
    historyModalEl.addEventListener("click", (event) => {
      if (event.target === historyModalEl) {
        closeHistoryModal();
      }
    });
    trendModalEl.addEventListener("click", (event) => {
      if (event.target === trendModalEl) {
        closeTrendModal();
      }
    });
    document.addEventListener("keydown", (event) => {
      if (event.key === "Escape" && historyModalEl.classList.contains("open")) {
        closeHistoryModal();
      }
      if (event.key === "Escape" && trendModalEl.classList.contains("open")) {
        closeTrendModal();
      }
    });
    function bindBoardHistoryEvents(container) {
      container.addEventListener("click", (event) => {
        const card = event.target.closest(".board-card[data-board-key]");
        if (!card || event.target.closest("a, button")) return;
        openBoardHistory(card.dataset.boardKey || "");
      });
      container.addEventListener("keydown", (event) => {
        const card = event.target.closest(".board-card[data-board-key]");
        if (!card) return;
        if (event.key === "Enter" || event.key === " ") {
          event.preventDefault();
          openBoardHistory(card.dataset.boardKey || "");
        }
      });
    }
    function bindBoardTrendButtons(container) {
      container.addEventListener("click", (event) => {
        const button = event.target.closest("button[data-trend-board-key]");
        if (!button) return;
        event.preventDefault();
        event.stopPropagation();
        openBoardTrend(button.dataset.trendBoardKey || "");
      });
    }
    bindBoardHistoryEvents(spotBoardsEl);
    bindBoardHistoryEvents(futuresBoardsEl);
    bindBoardHistoryEvents(endedBoardsEl);
    bindBoardTrendButtons(spotBoardsEl);
    bindBoardTrendButtons(futuresBoardsEl);
    bindBoardTrendButtons(endedBoardsEl);
    entrySaveBtn.addEventListener("click", async () => {
      const boardKey = entryBoardEl.value.trim();
      const name = entryNameEl.value.trim();
      const value = Number(entryValueEl.value);
      const note = entryNoteEl.value.trim();
      const isEditing = Boolean(editingEntryId);
      if (!boardKey || !name || !Number.isFinite(value) || value < 0) {
        setStatus("请完整填写榜单、姓名和当前数值。", true);
        return;
      }
      entrySaveBtn.disabled = true;
      entryCancelBtn.disabled = true;
      setStatus(isEditing ? "正在更新录入..." : "正在保存录入...");
      try {
        await saveEntry({ action: "upsert", id: editingEntryId || undefined, board_key: boardKey, name, value, note });
        resetEntryForm();
        setStatus(isEditing ? "录入已更新。" : "录入已保存。");
      } finally {
        entrySaveBtn.disabled = false;
        entryCancelBtn.disabled = false;
      }
    });
    entryCancelBtn.addEventListener("click", () => {
      resetEntryForm();
      setStatus("已取消编辑。");
    });
    forecastBoardEl.addEventListener("change", () => {
      renderForecastEntryOptions();
    });
    forecastRunBtn.addEventListener("click", async () => {
      const boardKey = forecastBoardEl.value.trim();
      const nextDayVolume = Number(forecastNextVolumeEl.value);
      const entryId = forecastEntryEl.value.trim();
      if (!boardKey || !Number.isFinite(nextDayVolume) || nextDayVolume < 0) {
        setStatus("请先选择榜单并填写预计下一天交易量。", true);
        return;
      }
      forecastRunBtn.disabled = true;
      forecastBodyEl.innerHTML = `<div class="empty">正在预测次日榜单变化...</div>`;
      try {
        const resp = await fetch(`${apiBase}/api/competition_forecast`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            board_key: boardKey,
            next_day_volume: nextDayVolume,
            entry_id: entryId || undefined,
          }),
        });
        const data = await resp.json();
        if (!resp.ok || !data.ok) {
          throw new Error(data.error || `请求失败(${resp.status})`);
        }
        renderForecastResult(data.forecast || {});
        setStatus("次日榜单预测已更新。");
      } catch (err) {
        forecastBodyEl.innerHTML = `<div class="empty">${escapeHtml(err.message || "预测失败")}</div>`;
        setStatus(`预测失败：${err.message}`, true);
      } finally {
        forecastRunBtn.disabled = false;
      }
    });

    window.setInterval(updateCountdownPanels, 1000);

    loadSnapshot(false);
  </script>
</body>
</html>
"""
