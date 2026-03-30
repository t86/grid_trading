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
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import requests

CACHE_PATH = Path("output/competition_board_cache.json")
ENTRIES_PATH = Path("output/competition_board_entries.json")
HISTORY_INDEX_PATH = Path("output/competition_board_history_index.json")
HISTORY_DIR_PATH = Path("output/competition_board_history")
REWARD_PRICE_CACHE_PATH = Path("output/competition_reward_price_cache.json")
CACHE_TTL_SECONDS = 1800
UTC_PLUS_8 = timezone(timedelta(hours=8))

_CACHE_LOCK = threading.Lock()
_REFRESH_LOCK = threading.Lock()
_PRICE_CACHE_LOCK = threading.Lock()
_MEMORY_CACHE: dict[str, Any] = {"loaded_at": 0.0, "data": None}

ENDED_LAST_DAY_MARKET_VOLUME: dict[str, float] = {
    "futures_opn:交易量挑战赛": 176_000_000.0,
    "futures_robo:交易量挑战赛": 274_000_000.0,
}

STATIC_ENDED_REWARD_SUMMARIES: dict[str, dict[str, Any]] = {
    "futures_enso:交易量挑战赛": {
        "symbol": "ENSO",
        "label": "ENSO 合约交易挑战赛",
        "rows": [
            {"rank_label": "第 1 名", "reward_value_usdt": 20058.30, "cutoff_value": 86691314.25, "ratio": 0.000231},
            {"rank_label": "第 2 名", "reward_value_usdt": 16046.64, "cutoff_value": 55725507.86, "ratio": 0.000288},
            {"rank_label": "第 3 名", "reward_value_usdt": 12034.98, "cutoff_value": 55261143.23, "ratio": 0.000218},
            {"rank_label": "第 4 名", "reward_value_usdt": 8023.32, "cutoff_value": 46482395.32, "ratio": 0.000173},
            {"rank_label": "第 5 名", "reward_value_usdt": 4011.66, "cutoff_value": 43734447.96, "ratio": 0.000092},
            {"rank_label": "第 6 - 20 名", "reward_value_usdt": 2005.83, "cutoff_value": 6351679.10, "ratio": 0.000316},
            {"rank_label": "第 21 - 50 名", "reward_value_usdt": 1337.22, "cutoff_value": 4370469.55, "ratio": 0.000306},
            {"rank_label": "第 51 - 200 名", "reward_value_usdt": 468.03, "cutoff_value": 1205606.16, "ratio": 0.000388},
        ],
    },
    "futures_zama:交易量挑战赛": {
        "symbol": "ZAMA",
        "label": "ZAMA 合约交易挑战赛",
        "rows": [
            {"rank_label": "第 1 名", "reward_value_usdt": 14512.00, "cutoff_value": 44205967.06, "ratio": 0.000328},
            {"rank_label": "第 2 名", "reward_value_usdt": 11609.60, "cutoff_value": 35675435.18, "ratio": 0.000325},
            {"rank_label": "第 3 名", "reward_value_usdt": 8707.20, "cutoff_value": 20625290.73, "ratio": 0.000422},
            {"rank_label": "第 4 名", "reward_value_usdt": 5804.80, "cutoff_value": 14375690.16, "ratio": 0.000404},
            {"rank_label": "第 5 名", "reward_value_usdt": 2902.40, "cutoff_value": 5158472.41, "ratio": 0.000563},
            {"rank_label": "第 6 - 20 名", "reward_value_usdt": 1451.20, "cutoff_value": 2735692.31, "ratio": 0.000530},
            {"rank_label": "第 21 - 50 名", "reward_value_usdt": 967.47, "cutoff_value": 1902993.71, "ratio": 0.000508},
            {"rank_label": "第 51 - 200 名", "reward_value_usdt": 338.61, "cutoff_value": 389458.32, "ratio": 0.000869},
        ],
    },
    "futures_birb:交易量挑战赛": {
        "symbol": "BIRB",
        "label": "BIRB 合约交易挑战赛",
        "rows": [
            {"rank_label": "第 1 名", "reward_value_usdt": None, "cutoff_value": 87361530.24, "ratio": None},
            {"rank_label": "第 2 名", "reward_value_usdt": None, "cutoff_value": 63132367.43, "ratio": None},
            {"rank_label": "第 3 名", "reward_value_usdt": None, "cutoff_value": 60967628.55, "ratio": None},
            {"rank_label": "第 4 名", "reward_value_usdt": None, "cutoff_value": 39749252.10, "ratio": None},
            {"rank_label": "第 5 名", "reward_value_usdt": None, "cutoff_value": 27215865.19, "ratio": None},
            {"rank_label": "第 6 - 20 名", "reward_value_usdt": None, "cutoff_value": 6126050.03, "ratio": None},
            {"rank_label": "第 21 - 50 名", "reward_value_usdt": None, "cutoff_value": 3267159.91, "ratio": None},
            {"rank_label": "第 51 - 200 名", "reward_value_usdt": None, "cutoff_value": 1125142.20, "ratio": None},
        ],
    },
    "futures_elsa:交易量挑战赛": {
        "symbol": "ELSA",
        "label": "ELSA 合约交易挑战赛",
        "rows": [
            {"rank_label": "第 1 名", "reward_value_usdt": None, "cutoff_value": 16985052.80, "ratio": None},
            {"rank_label": "第 2 名", "reward_value_usdt": None, "cutoff_value": 8497432.23, "ratio": None},
            {"rank_label": "第 3 名", "reward_value_usdt": None, "cutoff_value": 8000133.96, "ratio": None},
            {"rank_label": "第 4 名", "reward_value_usdt": None, "cutoff_value": 6915946.94, "ratio": None},
            {"rank_label": "第 5 名", "reward_value_usdt": None, "cutoff_value": 3087567.46, "ratio": None},
            {"rank_label": "第 6 - 20 名", "reward_value_usdt": None, "cutoff_value": 1114832.19, "ratio": None},
            {"rank_label": "第 21 - 50 名", "reward_value_usdt": None, "cutoff_value": 591614.56, "ratio": None},
            {"rank_label": "第 51 - 200 名", "reward_value_usdt": None, "cutoff_value": 151414.31, "ratio": None},
        ],
    },
    "futures_sent:交易量挑战赛": {
        "symbol": "SENT",
        "label": "SENT 合约交易挑战赛",
        "rows": [
            {"rank_label": "第 1 名", "reward_value_usdt": 27748.00, "cutoff_value": 72338496.81, "ratio": 0.000384},
            {"rank_label": "第 2 名", "reward_value_usdt": 22198.40, "cutoff_value": 58723131.40, "ratio": 0.000378},
            {"rank_label": "第 3 名", "reward_value_usdt": 16648.80, "cutoff_value": 48870930.84, "ratio": 0.000341},
            {"rank_label": "第 4 名", "reward_value_usdt": 11099.20, "cutoff_value": 48522939.04, "ratio": 0.000229},
            {"rank_label": "第 5 名", "reward_value_usdt": 5549.60, "cutoff_value": 39331514.09, "ratio": 0.000141},
            {"rank_label": "第 6 - 20 名", "reward_value_usdt": 2774.80, "cutoff_value": 4962123.23, "ratio": 0.000559},
            {"rank_label": "第 21 - 50 名", "reward_value_usdt": 1849.87, "cutoff_value": 3006484.49, "ratio": 0.000615},
            {"rank_label": "第 51 - 200 名", "reward_value_usdt": 647.45, "cutoff_value": 973170.35, "ratio": 0.000665},
        ],
    },
    "futures_fogo:交易量挑战赛": {
        "symbol": "FOGO",
        "label": "FOGO 合约交易挑战赛",
        "rows": [
            {"rank_label": "第 1 名", "reward_value_usdt": 13468.00, "cutoff_value": 60941911.92, "ratio": 0.000221},
            {"rank_label": "第 2 名", "reward_value_usdt": 10774.40, "cutoff_value": 59100487.35, "ratio": 0.000182},
            {"rank_label": "第 3 名", "reward_value_usdt": 8080.80, "cutoff_value": 17544927.15, "ratio": 0.000461},
            {"rank_label": "第 4 名", "reward_value_usdt": 5387.20, "cutoff_value": 12379863.94, "ratio": 0.000435},
            {"rank_label": "第 5 名", "reward_value_usdt": 2693.60, "cutoff_value": 8775521.29, "ratio": 0.000307},
            {"rank_label": "第 6 - 20 名", "reward_value_usdt": 1346.80, "cutoff_value": 3361731.01, "ratio": 0.000401},
            {"rank_label": "第 21 - 50 名", "reward_value_usdt": 897.87, "cutoff_value": 1467693.55, "ratio": 0.000612},
            {"rank_label": "第 51 - 200 名", "reward_value_usdt": 314.25, "cutoff_value": 560508.17, "ratio": 0.000561},
        ],
    },
    "futures_zkp:交易量挑战赛": {
        "symbol": "ZKP",
        "label": "ZKP 合约交易挑战赛",
        "rows": [
            {"rank_label": "第 1 名", "reward_value_usdt": 11850.00, "cutoff_value": 41095197.63, "ratio": 0.000288},
            {"rank_label": "第 2 名", "reward_value_usdt": 9480.00, "cutoff_value": 39149212.48, "ratio": 0.000242},
            {"rank_label": "第 3 名", "reward_value_usdt": 7110.00, "cutoff_value": 21332413.59, "ratio": 0.000333},
            {"rank_label": "第 4 名", "reward_value_usdt": 4740.00, "cutoff_value": 20779558.24, "ratio": 0.000228},
            {"rank_label": "第 5 名", "reward_value_usdt": 2370.00, "cutoff_value": 17755937.84, "ratio": 0.000133},
            {"rank_label": "第 6 - 20 名", "reward_value_usdt": 1185.00, "cutoff_value": 2724287.59, "ratio": 0.000435},
            {"rank_label": "第 21 - 50 名", "reward_value_usdt": 790.00, "cutoff_value": 1813332.41, "ratio": 0.000436},
            {"rank_label": "第 51 - 200 名", "reward_value_usdt": 276.50, "cutoff_value": 421419.02, "ratio": 0.000656},
        ],
    },
    "futures_magma:交易量挑战赛": {
        "symbol": "MAGMA",
        "label": "MAGMA 合约交易挑战赛",
        "rows": [
            {"rank_label": "第 1 名", "reward_value_usdt": None, "cutoff_value": 21832348.11, "ratio": None},
            {"rank_label": "第 2 名", "reward_value_usdt": None, "cutoff_value": 15274016.29, "ratio": None},
            {"rank_label": "第 3 名", "reward_value_usdt": None, "cutoff_value": 14569706.52, "ratio": None},
            {"rank_label": "第 4 名", "reward_value_usdt": None, "cutoff_value": 12910864.67, "ratio": None},
            {"rank_label": "第 5 名", "reward_value_usdt": None, "cutoff_value": 11223176.48, "ratio": None},
            {"rank_label": "第 6 - 20 名", "reward_value_usdt": None, "cutoff_value": 1814327.81, "ratio": None},
            {"rank_label": "第 21 - 50 名", "reward_value_usdt": None, "cutoff_value": 678787.74, "ratio": None},
            {"rank_label": "第 51 - 200 名", "reward_value_usdt": None, "cutoff_value": 157063.05, "ratio": None},
        ],
    },
    "futures_collect:交易量挑战赛": {
        "symbol": "COLLECT",
        "label": "COLLECT 合约交易挑战赛",
        "rows": [
            {"rank_label": "第 1 名", "reward_value_usdt": None, "cutoff_value": 32315389.24, "ratio": None},
            {"rank_label": "第 2 名", "reward_value_usdt": None, "cutoff_value": 29088269.90, "ratio": None},
            {"rank_label": "第 3 名", "reward_value_usdt": None, "cutoff_value": 24826234.93, "ratio": None},
            {"rank_label": "第 4 名", "reward_value_usdt": None, "cutoff_value": 15329938.67, "ratio": None},
            {"rank_label": "第 5 名", "reward_value_usdt": None, "cutoff_value": 9365065.51, "ratio": None},
            {"rank_label": "第 6 - 20 名", "reward_value_usdt": None, "cutoff_value": 2515592.70, "ratio": None},
            {"rank_label": "第 21 - 50 名", "reward_value_usdt": None, "cutoff_value": 1582278.87, "ratio": None},
            {"rank_label": "第 51 - 200 名", "reward_value_usdt": None, "cutoff_value": 413872.59, "ratio": None},
        ],
    },
    "futures_brev:交易量挑战赛": {
        "symbol": "BREV",
        "label": "BREV 合约交易挑战赛",
        "rows": [
            {"rank_label": "第 1 名", "reward_value_usdt": 15570.00, "cutoff_value": 153588397.60, "ratio": 0.000101},
            {"rank_label": "第 2 名", "reward_value_usdt": 12456.00, "cutoff_value": 102192670.35, "ratio": 0.000122},
            {"rank_label": "第 3 名", "reward_value_usdt": 9342.00, "cutoff_value": 85586624.48, "ratio": 0.000109},
            {"rank_label": "第 4 名", "reward_value_usdt": 6228.00, "cutoff_value": 61430452.38, "ratio": 0.000101},
            {"rank_label": "第 5 名", "reward_value_usdt": 3114.00, "cutoff_value": 31753045.35, "ratio": 0.000098},
            {"rank_label": "第 6 - 20 名", "reward_value_usdt": 1557.00, "cutoff_value": 8274285.33, "ratio": 0.000188},
            {"rank_label": "第 21 - 50 名", "reward_value_usdt": 1038.00, "cutoff_value": 6032188.17, "ratio": 0.000172},
            {"rank_label": "第 51 - 200 名", "reward_value_usdt": 363.30, "cutoff_value": 1239024.78, "ratio": 0.000293},
        ],
    },
}


@dataclass(frozen=True)
class CompetitionSource:
    slug: str
    symbol: str
    market: str
    label: str
    url: str


COMPETITION_SOURCES: tuple[CompetitionSource, ...] = (
    CompetitionSource(
        slug="spot_sahara",
        symbol="SAHARA",
        market="spot",
        label="SAHARA 现货交易竞赛",
        url="https://www.binance.com/zh-CN/activity/trading-competition/spot-altcoin-festival-wave-SAHARA",
    ),
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
        slug="spot_bard",
        symbol="BARD",
        market="spot",
        label="BARD 现货交易竞赛",
        url="https://www.bmwweb.systems/zh-CN/activity/trading-competition/spot-altcoin-festival-wave-BARD?ref=YEK2JZJT",
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
        url="https://www.bmwweb.solutions/zh-CN/activity/trading-competition/futures-bard-challenge2?ref=YEK2JZJT",
    ),
    CompetitionSource(
        slug="futures_enso",
        symbol="ENSO",
        market="futures",
        label="ENSO 合约交易挑战赛",
        url="https://www.bmwweb.systems/zh-CN/activity/trading-competition/futures-enso-challenge?ref=YEK2JZJT",
    ),
    CompetitionSource(
        slug="futures_zama",
        symbol="ZAMA",
        market="futures",
        label="ZAMA 合约交易挑战赛",
        url="https://www.bmwweb.systems/zh-CN/activity/trading-competition/futures-zama-challenge?ref=YEK2JZJT",
    ),
    CompetitionSource(
        slug="futures_birb",
        symbol="BIRB",
        market="futures",
        label="BIRB 合约交易挑战赛",
        url="https://www.bmwweb.systems/zh-CN/activity/trading-competition/futures-birb-challenge?ref=YEK2JZJT",
    ),
    CompetitionSource(
        slug="futures_elsa",
        symbol="ELSA",
        market="futures",
        label="ELSA 合约交易挑战赛",
        url="https://www.bmwweb.systems/zh-CN/activity/trading-competition/futures-elsa-challenge?ref=YEK2JZJT",
    ),
    CompetitionSource(
        slug="futures_sent",
        symbol="SENT",
        market="futures",
        label="SENT 合约交易挑战赛",
        url="https://www.bmwweb.systems/zh-CN/activity/trading-competition/futures-sent-challenge?ref=YEK2JZJT",
    ),
    CompetitionSource(
        slug="futures_fogo",
        symbol="FOGO",
        market="futures",
        label="FOGO 合约交易挑战赛",
        url="https://www.bmwweb.systems/zh-CN/activity/trading-competition/futures-fogo-challenge?ref=YEK2JZJT",
    ),
    CompetitionSource(
        slug="futures_zkp",
        symbol="ZKP",
        market="futures",
        label="ZKP 合约交易挑战赛",
        url="https://www.bmwweb.systems/zh-CN/activity/trading-competition/futures-zkp-challenge?ref=YEK2JZJT",
    ),
    CompetitionSource(
        slug="futures_magma",
        symbol="MAGMA",
        market="futures",
        label="MAGMA 合约交易挑战赛",
        url="https://www.bmwweb.systems/zh-CN/activity/trading-competition/futures-magma-challenge?ref=YEK2JZJT",
    ),
    CompetitionSource(
        slug="futures_collect",
        symbol="COLLECT",
        market="futures",
        label="COLLECT 合约交易挑战赛",
        url="https://www.bmwweb.systems/zh-CN/activity/trading-competition/futures-collect-challenge?ref=YEK2JZJT",
    ),
    CompetitionSource(
        slug="futures_brev",
        symbol="BREV",
        market="futures",
        label="BREV 合约交易挑战赛",
        url="https://www.bmwweb.systems/zh-CN/activity/trading-competition/futures-brev-challenge?ref=YEK2JZJT",
    ),
)

STATIC_BOARD_HINTS: dict[str, dict[str, Any]] = {
    "spot_sahara": {
        "boards": [
            {
                "tabLabel": "默认",
                "resourceId": 46372,
                "metricField": "tradingVolume",
                "metricLabel": "交易量 (USD)",
                "rewardUnit": "SAHARA",
                "leaderboardUnit": "USD",
                "leaderboardUnitTitle": "交易量",
                "rankingType": "CUSTOMIZED",
                "competitionType": "SPOT",
                "activityPeriodText": "2026/03/18 07:00 - 2026/03/25 07:00",
                "activityEndAt": "2026-03-25T07:00:00+08:00",
                "maxRows": 5000,
                "bodyExcerpt": """
累计现货交易量至少 500 USD，方可参与排行榜奖励。
奖励结构
第 1 - 200 名
平分 1,600,000 SAHARA
第 201 - 5000 名
平分 2,400,000 SAHARA
""",
            }
        ]
    },
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
    "spot_bard": {
        "boards": [
            {
                "tabLabel": "默认",
                "resourceId": 47247,
                "metricField": "tradingVolume",
                "metricLabel": "交易量 (USD)",
                "rewardUnit": "BARD",
                "leaderboardUnit": "USD",
                "leaderboardUnitTitle": "交易量",
                "rankingType": "CUSTOMIZED",
                "competitionType": "SPOT",
                "activityPeriodText": "2026/03/26 17:00 - 2026/04/16 17:00",
                "activityEndAt": "2026-04-16T17:00:00+08:00",
                "maxRows": 5000,
                "bodyExcerpt": """
总奖池 3,000,000 BARD
活动期间，在 BARD/USDT、BARD/USDC 任一符合条件的现货交易对累计交易量至少 500 USD，方可参与排行榜奖励。
奖励结构
第1名
150,000 BARD
第2名
120,000 BARD
第3名
90,000 BARD
第4名
60,000 BARD
第5名
30,000 BARD
第6 - 20名
平分300,000 BARD奖池
第21 - 50名
平分450,000 BARD奖池
第51 - 200名
平分450,000 BARD奖池
第201 - 1,000名
平分450,000 BARD奖池
第1001名及之后
平分900,000 BARD奖池，每位用户的奖励上限为25 BARD
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
                "resourceId": 46948,
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
第 1 名
75,000 BARD
第 2 名
60,000 BARD
第 3 名
35,000 BARD
第 4 名
20,000 BARD
第 5 名
10,000 BARD
第 6 - 20 名
平分 75,000 BARD
第 21 - 50 名
平分 75,000 BARD
第 51 - 200 名
平分 150,000 BARD
""",
            }
        ]
    },
    "futures_enso": {
        "boards": [
            {
                "tabLabel": "交易量挑战赛",
                "resourceId": 43343,
                "metricField": "grade",
                "metricLabel": "交易量 (USDT)",
                "rewardUnit": "BNB",
                "leaderboardUnit": "USDT",
                "leaderboardUnitTitle": "交易量",
                "rankingType": "CUSTOMIZED",
                "competitionType": "FUTURES",
                "activityEndAt": "2026-03-18T07:59:00+08:00",
                "maxRows": 200,
                "bodyExcerpt": """
累计 ENSO U 本位合约交易量至少 500 USDT，方可参与排行榜奖励。
总奖池 300 BNB
第 1 名
30 BNB
第 2 名
24 BNB
第 3 名
18 BNB
第 4 名
12 BNB
第 5 名
6 BNB
第 6 - 20 名
平分 45 BNB
第 21 - 50 名
平分 60 BNB
第 51 - 200 名
平分 105 BNB
""",
            }
        ]
    },
    "futures_zama": {
        "boards": [
            {
                "tabLabel": "交易量挑战赛",
                "resourceId": 41118,
                "metricField": "grade",
                "metricLabel": "交易量 (USDT)",
                "rewardUnit": "ZAMA",
                "leaderboardUnit": "USDT",
                "leaderboardUnitTitle": "交易量",
                "rankingType": "CUSTOMIZED",
                "competitionType": "FUTURES",
                "activityEndAt": "2026-02-13T07:59:00+08:00",
                "maxRows": 200,
                "bodyExcerpt": """
累计 ZAMA U 本位合约交易量至少 500 USDT，方可参与排行榜奖励。
总奖池 8,000,000 ZAMA
第 1 名
800,000 ZAMA
第 2 名
640,000 ZAMA
第 3 名
480,000 ZAMA
第 4 名
320,000 ZAMA
第 5 名
160,000 ZAMA
第 6 - 20 名
平分 1,200,000 ZAMA
第 21 - 50 名
平分 1,600,000 ZAMA
第 51 - 200 名
平分 2,800,000 ZAMA
""",
            }
        ]
    },
    "futures_birb": {
        "boards": [
            {
                "tabLabel": "交易量挑战赛",
                "resourceId": 40507,
                "metricField": "grade",
                "metricLabel": "交易量 (USDT)",
                "rewardUnit": "BIRB",
                "leaderboardUnit": "USDT",
                "leaderboardUnitTitle": "交易量",
                "rankingType": "CUSTOMIZED",
                "competitionType": "FUTURES",
                "activityEndAt": "2026-02-08T07:59:00+08:00",
                "maxRows": 200,
                "bodyExcerpt": """
累计 BIRB U 本位合约交易量至少 500 USDT，方可参与排行榜奖励。
总奖池 1,200,000 BIRB
第 1 名
120,000 BIRB
第 2 名
96,000 BIRB
第 3 名
72,000 BIRB
第 4 名
48,000 BIRB
第 5 名
24,000 BIRB
第 6 - 20 名
平分 180,000 BIRB
第 21 - 50 名
平分 240,000 BIRB
第 51 - 200 名
平分 420,000 BIRB
""",
            }
        ]
    },
    "futures_elsa": {
        "boards": [
            {
                "tabLabel": "交易量挑战赛",
                "resourceId": 39559,
                "metricField": "grade",
                "metricLabel": "交易量 (USDT)",
                "rewardUnit": "ELSA",
                "leaderboardUnit": "USDT",
                "leaderboardUnitTitle": "交易量",
                "rankingType": "CUSTOMIZED",
                "competitionType": "FUTURES",
                "activityEndAt": "2026-02-02T07:59:00+08:00",
                "maxRows": 200,
                "bodyExcerpt": """
累计 ELSA U 本位合约交易量至少 500 USDT，方可参与排行榜奖励。
总奖池 400,000 ELSA
第 1 名
40,000 ELSA
第 2 名
32,000 ELSA
第 3 名
24,000 ELSA
第 4 名
16,000 ELSA
第 5 名
8,000 ELSA
第 6 - 20 名
平分 60,000 ELSA
第 21 - 50 名
平分 80,000 ELSA
第 51 - 200 名
平分 140,000 ELSA
""",
            }
        ]
    },
    "futures_sent": {
        "boards": [
            {
                "tabLabel": "交易量挑战赛",
                "resourceId": 39509,
                "metricField": "grade",
                "metricLabel": "交易量 (USDT)",
                "rewardUnit": "SENT",
                "leaderboardUnit": "USDT",
                "leaderboardUnitTitle": "交易量",
                "rankingType": "CUSTOMIZED",
                "competitionType": "FUTURES",
                "activityEndAt": "2026-02-02T07:59:00+08:00",
                "maxRows": 200,
                "bodyExcerpt": """
累计 SENT U 本位合约交易量至少 500 USDT，方可参与排行榜奖励。
总奖池 7,000,000 SENT
第 1 名
700,000 SENT
第 2 名
560,000 SENT
第 3 名
420,000 SENT
第 4 名
280,000 SENT
第 5 名
140,000 SENT
第 6 - 20 名
平分 1,050,000 SENT
第 21 - 50 名
平分 1,400,000 SENT
第 51 - 200 名
平分 2,450,000 SENT
""",
            }
        ]
    },
    "futures_fogo": {
        "boards": [
            {
                "tabLabel": "交易量挑战赛",
                "resourceId": 38183,
                "metricField": "grade",
                "metricLabel": "交易量 (USDT)",
                "rewardUnit": "FOGO",
                "leaderboardUnit": "USDT",
                "leaderboardUnitTitle": "交易量",
                "rankingType": "CUSTOMIZED",
                "competitionType": "FUTURES",
                "activityEndAt": "2026-01-25T07:59:00+08:00",
                "maxRows": 200,
                "bodyExcerpt": """
累计 FOGO U 本位合约交易量至少 500 USDT，方可参与排行榜奖励。
总奖池 3,500,000 FOGO
第 1 名
350,000 FOGO
第 2 名
280,000 FOGO
第 3 名
210,000 FOGO
第 4 名
140,000 FOGO
第 5 名
70,000 FOGO
第 6 - 20 名
平分 525,000 FOGO
第 21 - 50 名
平分 700,000 FOGO
第 51 - 200 名
平分 1,225,000 FOGO
""",
            }
        ]
    },
    "futures_zkp": {
        "boards": [
            {
                "tabLabel": "交易量挑战赛",
                "resourceId": 38070,
                "metricField": "grade",
                "metricLabel": "交易量 (USDT)",
                "rewardUnit": "ZKP",
                "leaderboardUnit": "USDT",
                "leaderboardUnitTitle": "交易量",
                "rankingType": "CUSTOMIZED",
                "competitionType": "FUTURES",
                "activityEndAt": "2026-01-24T07:59:00+08:00",
                "maxRows": 200,
                "bodyExcerpt": """
累计 ZKP U 本位合约交易量至少 500 USDT，方可参与排行榜奖励。
总奖池 1,000,000 ZKP
第 1 名
100,000 ZKP
第 2 名
80,000 ZKP
第 3 名
60,000 ZKP
第 4 名
40,000 ZKP
第 5 名
20,000 ZKP
第 6 - 20 名
平分 150,000 ZKP
第 21 - 50 名
平分 200,000 ZKP
第 51 - 200 名
平分 350,000 ZKP
""",
            }
        ]
    },
    "futures_magma": {
        "boards": [
            {
                "tabLabel": "交易量挑战赛",
                "resourceId": 37323,
                "metricField": "grade",
                "metricLabel": "交易量 (USDT)",
                "rewardUnit": "MAGMA",
                "leaderboardUnit": "USDT",
                "leaderboardUnitTitle": "交易量",
                "rankingType": "CUSTOMIZED",
                "competitionType": "FUTURES",
                "activityEndAt": "2026-01-17T07:59:00+08:00",
                "maxRows": 200,
                "bodyExcerpt": """
累计 MAGMA U 本位合约交易量至少 500 USDT，方可参与排行榜奖励。
总奖池 500,000 MAGMA
第 1 名
50,000 MAGMA
第 2 名
40,000 MAGMA
第 3 名
30,000 MAGMA
第 4 名
20,000 MAGMA
第 5 名
10,000 MAGMA
第 6 - 20 名
平分 75,000 MAGMA
第 21 - 50 名
平分 100,000 MAGMA
第 51 - 200 名
平分 175,000 MAGMA
""",
            }
        ]
    },
    "futures_collect": {
        "boards": [
            {
                "tabLabel": "交易量挑战赛",
                "resourceId": 37348,
                "metricField": "grade",
                "metricLabel": "交易量 (USDT)",
                "rewardUnit": "COLLECT",
                "leaderboardUnit": "USDT",
                "leaderboardUnitTitle": "交易量",
                "rankingType": "CUSTOMIZED",
                "competitionType": "FUTURES",
                "activityEndAt": "2026-01-17T07:59:00+08:00",
                "maxRows": 200,
                "bodyExcerpt": """
累计 COLLECT U 本位合约交易量至少 500 USDT，方可参与排行榜奖励。
总奖池 1,200,000 COLLECT
第 1 名
120,000 COLLECT
第 2 名
96,000 COLLECT
第 3 名
72,000 COLLECT
第 4 名
48,000 COLLECT
第 5 名
24,000 COLLECT
第 6 - 20 名
平分 180,000 COLLECT
第 21 - 50 名
平分 240,000 COLLECT
第 51 - 200 名
平分 420,000 COLLECT
""",
            }
        ]
    },
    "futures_brev": {
        "boards": [
            {
                "tabLabel": "交易量挑战赛",
                "resourceId": 37158,
                "metricField": "grade",
                "metricLabel": "交易量 (USDT)",
                "rewardUnit": "BREV",
                "leaderboardUnit": "USDT",
                "leaderboardUnitTitle": "交易量",
                "rankingType": "CUSTOMIZED",
                "competitionType": "FUTURES",
                "activityEndAt": "2026-01-16T07:59:00+08:00",
                "maxRows": 200,
                "bodyExcerpt": """
累计 BREV U 本位合约交易量至少 500 USDT，方可参与排行榜奖励。
总奖池 500,000 BREV
第 1 名
50,000 BREV
第 2 名
40,000 BREV
第 3 名
30,000 BREV
第 4 名
20,000 BREV
第 5 名
10,000 BREV
第 6 - 20 名
平分 75,000 BREV
第 21 - 50 名
平分 100,000 BREV
第 51 - 200 名
平分 175,000 BREV
""",
            }
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
_ACTIVITY_PERIOD_RE = re.compile(
    r"(?P<start>\d{4}/\d{2}/\d{2}\s+\d{2}:\d{2})\s*-\s*(?P<end>\d{4}/\d{2}/\d{2}\s+\d{2}:\d{2})"
)


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
    referer_text = str(referer or "https://www.binance.com").strip() or "https://www.binance.com"
    referer_parts = urlsplit(referer_text)
    origin = f"{referer_parts.scheme}://{referer_parts.netloc}" if referer_parts.scheme and referer_parts.netloc else "https://www.binance.com"
    headers = {
        "Content-Type": "application/json",
        "Origin": origin,
        "Referer": referer_text,
        "User-Agent": os.environ.get(
            "GRID_COMPETITION_BOARD_UA",
            (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36"
            ),
        ),
    }
    endpoint = f"{origin}/bapi/growth/v1/friendly/growth-paas/resource/summary/list"

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


def _parse_activity_period_bounds(text: str) -> tuple[str | None, str | None]:
    raw = str(text or "").strip()
    if not raw:
        return None, None
    match = _ACTIVITY_PERIOD_RE.search(raw)
    if not match:
        return None, None
    try:
        start = datetime.strptime(match.group("start"), "%Y/%m/%d %H:%M").replace(tzinfo=UTC_PLUS_8)
        end = datetime.strptime(match.group("end"), "%Y/%m/%d %H:%M").replace(tzinfo=UTC_PLUS_8)
    except ValueError:
        return None, None
    return start.isoformat(), end.isoformat()


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
    activity_period_text = str(board_meta.get("activityPeriodText", "")).strip()
    activity_start_at, activity_end_from_period = _parse_activity_period_bounds(activity_period_text)
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
        "activity_period_text": activity_period_text,
        "activity_start_at": activity_start_at,
        "activity_end_at": str(board_meta.get("activityEndAt", "")).strip() or activity_end_from_period,
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


def _parse_iso_datetime(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text or text.lower() == "none":
      return None
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _history_entry_datetime(entry: dict[str, Any]) -> datetime | None:
    for key in ("updated_at_utc", "captured_at_utc"):
        parsed = _parse_iso_datetime(entry.get(key))
        if parsed is not None:
            return parsed
    return None


def _normalize_history_index_payload(boards: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    if not isinstance(boards, dict):
        return {}
    normalized: dict[str, list[dict[str, Any]]] = {}
    for board_key, items in boards.items():
        if not isinstance(items, list):
            continue
        deduped: dict[str, dict[str, Any]] = {}
        for item in items:
            if not isinstance(item, dict):
                continue
            dedupe_key = str(item.get("path") or item.get("capture_key") or item.get("updated_at_utc") or uuid.uuid4().hex)
            deduped[dedupe_key] = item
        valid_items = list(deduped.values())
        valid_items.sort(
            key=lambda item: _history_entry_datetime(item) or datetime.min.replace(tzinfo=timezone.utc),
            reverse=True,
        )
        normalized[str(board_key)] = valid_items
    return normalized


def _history_storage_name(board: dict[str, Any]) -> str:
    symbol = re.sub(r"[^0-9A-Za-z._-]+", "", str(board.get("symbol", "")).strip().lower()) or "board"
    tab_label = str(board.get("tab_label") or board.get("label") or "default").strip()
    normalized_tab = re.sub(r"\s+", "", tab_label)
    normalized_tab = re.sub(r"[/:]+", "_", normalized_tab)
    normalized_tab = normalized_tab.replace("·", "_")
    normalized_tab = normalized_tab or "default"
    return f"{str(board.get('market', '')).strip().lower()}_{symbol}{normalized_tab}"


def _capture_info_for_board(board: dict[str, Any], *, captured_at_utc: str | None = None) -> dict[str, Any]:
    updated_dt = _parse_iso_datetime(board.get("updated_at_utc"))
    captured_dt = _parse_iso_datetime(captured_at_utc) if captured_at_utc else None
    anchor_dt = updated_dt or captured_dt or datetime.now(timezone.utc)
    capture_dt = anchor_dt.astimezone(UTC_PLUS_8).replace(minute=0, second=0, microsecond=0)
    return {
        "capture_key": capture_dt.strftime("%Y-%m-%d %H:00"),
        "capture_label": capture_dt.strftime("%Y-%m-%d %H:00"),
        "capture_date": capture_dt.strftime("%Y-%m-%d"),
        "capture_granularity": "hourly",
    }


def _history_entry_from_payload(payload: dict[str, Any], path: Path) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    board = payload.get("board")
    if not isinstance(board, dict):
        return None
    board_key = str(payload.get("board_key") or board.get("board_key") or "").strip()
    if not board_key:
        return None
    capture_info = _capture_info_for_board(board, captured_at_utc=str(payload.get("captured_at_utc", "")).strip() or None)
    return {
        "capture_key": str(payload.get("capture_key") or capture_info["capture_key"]).strip(),
        "capture_label": str(payload.get("capture_label") or capture_info["capture_label"]).strip(),
        "capture_date": str(payload.get("capture_date") or capture_info["capture_date"]).strip(),
        "capture_granularity": str(payload.get("capture_granularity") or capture_info["capture_granularity"]).strip(),
        "captured_at_utc": str(payload.get("captured_at_utc", "")).strip(),
        "path": str(path),
        "label": str(payload.get("label") or board.get("label") or board_key).strip(),
        "updated_text": str(payload.get("updated_text") or board.get("updated_text") or "").strip(),
        "updated_at_utc": str(payload.get("updated_at_utc") or board.get("updated_at_utc") or "").strip(),
        "eligible_user_count": _safe_int(payload.get("eligible_user_count") or board.get("eligible_user_count")),
        "current_floor_value_text": str(payload.get("current_floor_value_text") or board.get("current_floor_value_text") or "").strip(),
    }


def _scan_history_index_from_disk() -> dict[str, list[dict[str, Any]]]:
    boards: dict[str, list[dict[str, Any]]] = {}
    if not HISTORY_DIR_PATH.exists():
        return boards
    for path in sorted(HISTORY_DIR_PATH.glob("*.json")):
        payload = _read_json_file(path, {})
        entry = _history_entry_from_payload(payload, path)
        if entry is None:
            continue
        board = payload.get("board") if isinstance(payload, dict) else None
        board_key = str(payload.get("board_key") or (board or {}).get("board_key") or "").strip()
        if not board_key:
            continue
        boards.setdefault(board_key, []).append(entry)
    return _normalize_history_index_payload(boards)


def _save_history_index(boards: dict[str, list[dict[str, Any]]]) -> None:
    payload = {
        "updated_at_utc": datetime.now(timezone.utc).isoformat(),
        "boards": _normalize_history_index_payload(boards),
    }
    _write_json_file(HISTORY_INDEX_PATH, payload)


def _load_history_index() -> dict[str, list[dict[str, Any]]]:
    payload = _read_json_file(HISTORY_INDEX_PATH, {})
    indexed = _normalize_history_index_payload(payload.get("boards", {}) if isinstance(payload, dict) else {})
    scanned = _scan_history_index_from_disk()
    merged: dict[str, list[dict[str, Any]]] = {}
    for board_key in set(indexed) | set(scanned):
        merged[board_key] = list(indexed.get(board_key, [])) + list(scanned.get(board_key, []))
    normalized = _normalize_history_index_payload(merged)
    if normalized != indexed:
        _save_history_index(normalized)
    return normalized


def _persist_boards_to_history(boards: list[dict[str, Any]], *, snapshot_generated_at_utc: str) -> None:
    if not isinstance(boards, list):
        return
    history_index = _load_history_index()
    for board in boards:
        if not isinstance(board, dict):
            continue
        board_key = str(board.get("board_key", "")).strip()
        if not board_key:
            continue
        capture_info = _capture_info_for_board(board)
        file_name = f"{capture_info['capture_date']}_{capture_info['capture_label'][-5:].replace(':', '')}__{_history_storage_name(board)}.json"
        path = HISTORY_DIR_PATH / file_name
        payload = {
            "board_key": board_key,
            **capture_info,
            "captured_at_utc": datetime.now(timezone.utc).isoformat(),
            "snapshot_generated_at_utc": snapshot_generated_at_utc,
            "board": board,
        }
        _write_json_file(path, payload)
        entry = _history_entry_from_payload(payload, path)
        if entry is None:
            continue
        history_index.setdefault(board_key, [])
        history_index[board_key] = [item for item in history_index[board_key] if str(item.get("path")) != str(path)]
        history_index[board_key].append(entry)
    _save_history_index(history_index)


def _load_history_board(entry: dict[str, Any]) -> dict[str, Any] | None:
    relative_path = str(entry.get("path", "")).strip()
    if not relative_path:
        return None
    payload = _read_json_file(Path(relative_path), {})
    if not isinstance(payload, dict):
        return None
    board = payload.get("board")
    return board if isinstance(board, dict) else None


def _values_by_rank(board: dict[str, Any]) -> dict[int, float]:
    out: dict[int, float] = {}
    for item in board.get("rows", []):
        if not isinstance(item, dict):
            continue
        rank = _safe_int(item.get("rank"))
        value = _safe_float(item.get("value"))
        if rank is None or value is None:
            continue
        out[int(rank)] = float(value)
    return out


def _select_previous_day_entry(entries: list[dict[str, Any]], final_dt: datetime) -> dict[str, Any] | None:
    if not entries:
        return None
    target = final_dt - timedelta(hours=24)
    older = [item for item in entries if (_history_entry_datetime(item) or final_dt) <= target]
    if older:
        return max(older, key=lambda item: _history_entry_datetime(item) or datetime.min.replace(tzinfo=timezone.utc))
    candidates = [item for item in entries if (_history_entry_datetime(item) or final_dt) < final_dt]
    if candidates:
        return max(candidates, key=lambda item: _history_entry_datetime(item) or datetime.min.replace(tzinfo=timezone.utc))
    return None


def _load_reward_price_cache() -> dict[str, float]:
    payload = _read_json_file(REWARD_PRICE_CACHE_PATH, {})
    if not isinstance(payload, dict):
        return {}
    out: dict[str, float] = {}
    for key, value in payload.items():
        number = _safe_float(value)
        if number is not None and number > 0:
            out[str(key)] = float(number)
    return out


def _save_reward_price_cache(cache: dict[str, float]) -> None:
    _write_json_file(REWARD_PRICE_CACHE_PATH, cache)


def _fetch_symbol_close_price_usdt(symbol: str, end_at: datetime) -> float | None:
    normalized = str(symbol or "").strip().upper()
    if not normalized:
        return None
    if normalized == "USDT":
        return 1.0
    cache_key = f"{normalized}|{end_at.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M')}"
    with _PRICE_CACHE_LOCK:
        cached = _load_reward_price_cache()
        if cache_key in cached:
            return cached[cache_key]
    params = {
        "symbol": f"{normalized}USDT",
        "interval": "1m",
        "limit": 1,
        "endTime": int(end_at.astimezone(timezone.utc).timestamp() * 1000),
    }
    try:
        resp = requests.get("https://api.binance.com/api/v3/klines", params=params, timeout=10)
        resp.raise_for_status()
        payload = resp.json()
    except Exception:
        return None
    if not isinstance(payload, list) or not payload:
        return None
    row = payload[-1]
    if not isinstance(row, list) or len(row) < 5:
        return None
    price = _safe_float(row[4])
    if price is None or price <= 0:
        return None
    with _PRICE_CACHE_LOCK:
        cached = _load_reward_price_cache()
        cached[cache_key] = float(price)
        _save_reward_price_cache(cached)
    return float(price)


def _build_ended_boards_analytics(boards: list[dict[str, Any]]) -> dict[str, Any]:
    history_index = _load_history_index()
    now = datetime.now(timezone.utc)
    delta_rows: list[dict[str, Any]] = []
    reward_rows: list[dict[str, Any]] = []
    tracked_ranks = (20, 30, 50, 100, 150, 200)
    known_reward_keys: set[str] = set()

    ended_boards = [
        board for board in boards
        if isinstance(board, dict)
        and _parse_iso_datetime(board.get("activity_end_at")) is not None
        and _parse_iso_datetime(board.get("activity_end_at")).astimezone(timezone.utc) <= now
    ]
    ended_boards.sort(
        key=lambda item: _parse_iso_datetime(item.get("activity_end_at")) or datetime.min.replace(tzinfo=timezone.utc),
        reverse=True,
    )

    for board in ended_boards:
        board_key = str(board.get("board_key", "")).strip()
        entries = history_index.get(board_key, [])
        if len(entries) < 2:
            continue
        final_entry = next((item for item in entries if _history_entry_datetime(item) is not None), None)
        if final_entry is None:
            continue
        final_dt = _history_entry_datetime(final_entry)
        if final_dt is None:
            continue
        previous_entry = _select_previous_day_entry(entries, final_dt)
        if previous_entry is None:
            continue
        final_board = _load_history_board(final_entry)
        previous_board = _load_history_board(previous_entry)
        if not isinstance(final_board, dict) or not isinstance(previous_board, dict):
            continue
        final_values = _values_by_rank(final_board)
        previous_values = _values_by_rank(previous_board)
        delta_rows.append(
            {
                "board_key": board_key,
                "label": str(final_board.get("label") or board.get("label") or board_key),
                "symbol": str(final_board.get("symbol") or board.get("symbol") or ""),
                "market": str(final_board.get("market") or board.get("market") or ""),
                "final_capture": str(final_entry.get("capture_label", "")),
                "previous_capture": str(previous_entry.get("capture_label", "")),
                "last_day_market_volume": ENDED_LAST_DAY_MARKET_VOLUME.get(board_key),
                "deltas": {
                    str(rank): round(max(0.0, final_values.get(rank, 0.0) - previous_values.get(rank, 0.0)), 8)
                    for rank in tracked_ranks
                },
            }
        )

        end_at = _parse_iso_datetime(final_board.get("activity_end_at") or board.get("activity_end_at"))
        reward_unit = str(final_board.get("reward_unit") or board.get("reward_unit") or "").strip().upper()
        reward_price = _fetch_symbol_close_price_usdt(reward_unit, end_at) if end_at is not None and reward_unit else None
        for segment in final_board.get("segments", []):
            if not isinstance(segment, dict):
                continue
            per_user_reward = _safe_float(segment.get("per_user_reward"))
            cutoff_value = _safe_float(segment.get("cutoff_value"))
            if per_user_reward is None or cutoff_value is None or cutoff_value <= 0:
                continue
            reward_usdt = float(per_user_reward) * float(reward_price) if reward_price is not None else None
            ratio = reward_usdt / float(cutoff_value) if reward_usdt is not None and cutoff_value > 0 else None
            reward_rows.append(
                {
                    "board_key": board_key,
                    "label": str(final_board.get("label") or board.get("label") or board_key),
                    "symbol": str(final_board.get("symbol") or board.get("symbol") or ""),
                    "market": str(final_board.get("market") or board.get("market") or ""),
                    "rank_label": str(segment.get("rank_label", "")).strip() or "-",
                    "reward_text": str(segment.get("per_user_reward_text") or segment.get("reward_text") or "").strip() or "-",
                    "reward_unit": reward_unit or str(final_board.get("reward_unit") or "").strip(),
                    "reward_price_usdt": reward_price,
                    "reward_value_usdt": reward_usdt,
                    "cutoff_value": float(cutoff_value),
                    "cutoff_value_text": f"{float(cutoff_value):,.2f}",
                    "ratio": ratio,
                }
            )
            known_reward_keys.add(board_key)

    for board_key, summary in STATIC_ENDED_REWARD_SUMMARIES.items():
        if board_key in known_reward_keys:
            continue
        for row in summary.get("rows", []):
            if not isinstance(row, dict):
                continue
            cutoff_value = _safe_float(row.get("cutoff_value"))
            if cutoff_value is None or cutoff_value <= 0:
                continue
            reward_rows.append(
                {
                    "board_key": board_key,
                    "label": str(summary.get("label") or board_key),
                    "symbol": str(summary.get("symbol") or ""),
                    "market": "futures",
                    "rank_label": str(row.get("rank_label") or "-"),
                    "reward_text": "-",
                    "reward_unit": "",
                    "reward_price_usdt": None,
                    "reward_value_usdt": _safe_float(row.get("reward_value_usdt")),
                    "cutoff_value": float(cutoff_value),
                    "cutoff_value_text": f"{float(cutoff_value):,.2f}",
                    "ratio": _safe_float(row.get("ratio")),
                }
            )

    return {
        "delta_rows": delta_rows,
        "reward_rows": reward_rows,
    }


def _build_ongoing_boards_analytics(boards: list[dict[str, Any]]) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    history_index = _load_history_index()
    tracked_ranks = (20, 30, 50, 100, 150, 200)
    ongoing_rows: list[dict[str, Any]] = []

    for board in boards:
        if not isinstance(board, dict):
            continue
        end_at = _parse_iso_datetime(board.get("activity_end_at"))
        if end_at is None or end_at.astimezone(timezone.utc) <= now:
            continue
        if str(board.get("market", "")).strip() != "futures":
            continue
        board_key = str(board.get("board_key", "")).strip()
        values = _values_by_rank(board)
        latest_day_volume = None
        previous_capture = ""
        current_dt = _parse_iso_datetime(board.get("updated_at_utc")) or now
        entries = history_index.get(board_key, [])
        previous_entry = _select_previous_day_entry(entries, current_dt)
        previous_board = _load_history_board(previous_entry) if previous_entry is not None else None
        current_total = _safe_float(board.get("eligible_metric_total"))
        previous_total = _safe_float(previous_board.get("eligible_metric_total")) if isinstance(previous_board, dict) else None
        if current_total is not None and previous_total is not None:
            latest_day_volume = max(0.0, float(current_total) - float(previous_total))
        if previous_entry is not None:
            previous_capture = str(previous_entry.get("capture_label", "")).strip()
        reward_unit = str(board.get("reward_unit") or "").strip().upper()
        reward_price = _fetch_symbol_close_price_usdt(reward_unit, now) if reward_unit else None
        reward_rows: list[dict[str, Any]] = []
        reward_rows_message = ""
        for segment in board.get("segments", []):
            if not isinstance(segment, dict):
                continue
            per_user_reward = _safe_float(segment.get("per_user_reward"))
            cutoff_value = _safe_float(segment.get("cutoff_value"))
            if per_user_reward is None or cutoff_value is None or cutoff_value <= 0:
                continue
            reward_usdt = float(per_user_reward) * float(reward_price) if reward_price is not None else None
            ratio = reward_usdt / float(cutoff_value) if reward_usdt is not None and cutoff_value > 0 else None
            reward_rows.append(
                {
                    "rank_label": str(segment.get("rank_label", "")).strip() or "-",
                    "reward_value_usdt": reward_usdt,
                    "current_cutoff_value": float(cutoff_value),
                    "forecast_rank": _safe_int(segment.get("end_rank")),
                    "ratio": ratio,
                }
            )
        if not reward_rows:
            if not board.get("segments"):
                reward_rows_message = "未解析出奖励段，暂时无法计算奖励 / 门槛比值。"
            elif not board.get("rows"):
                reward_rows_message = "官方公开榜单暂未返回有效门槛数据，所以奖励 / 门槛比值暂时无法计算。"
            else:
                reward_rows_message = "奖励段已识别，但缺少可用门槛值，暂时无法计算奖励 / 门槛比值。"
        ongoing_rows.append(
            {
                "board_key": board_key,
                "label": str(board.get("label", "")).strip() or str(board.get("title", "")).strip(),
                "symbol": str(board.get("symbol", "")).strip(),
                "updated_at_utc": str(board.get("updated_at_utc", "")).strip(),
                "latest_day_volume": latest_day_volume,
                "latest_day_volume_text": f"{latest_day_volume:,.0f}" if latest_day_volume is not None else "",
                "previous_capture": previous_capture,
                "current_values": {
                    str(rank): values.get(rank)
                    for rank in tracked_ranks
                },
                "reward_rows": reward_rows,
                "reward_rows_message": reward_rows_message,
            }
        )

    ongoing_rows.sort(key=lambda item: item["symbol"])
    return {"board_rows": ongoing_rows}


def resolve_active_competition_board(
    symbol: str,
    market: str,
    *,
    snapshot: dict[str, Any] | None = None,
    now: datetime | None = None,
) -> dict[str, Any] | None:
    normalized_symbol = str(symbol or "").upper().strip()
    normalized_market = str(market or "").strip().lower()
    if not normalized_symbol or not normalized_market:
        return None
    if normalized_symbol.endswith("USDT"):
        normalized_symbol = normalized_symbol[:-4]
    current = now.astimezone(timezone.utc) if now is not None else datetime.now(timezone.utc)
    boards = snapshot.get("boards", []) if isinstance(snapshot, dict) else None
    if not isinstance(boards, list):
        boards = build_competition_board_snapshot(refresh=False).get("boards", [])
    candidates: list[dict[str, Any]] = []
    for board in boards:
        if not isinstance(board, dict):
            continue
        if str(board.get("market", "")).strip().lower() != normalized_market:
            continue
        if str(board.get("symbol", "")).upper().strip() != normalized_symbol:
            continue
        start_raw = str(board.get("activity_start_at", "")).strip()
        end_raw = str(board.get("activity_end_at", "")).strip()
        if not start_raw or not end_raw:
            parsed_start, parsed_end = _parse_activity_period_bounds(str(board.get("activity_period_text", "")).strip())
            start_raw = start_raw or str(parsed_start or "")
            end_raw = end_raw or str(parsed_end or "")
        start_at = _parse_iso_datetime(start_raw)
        end_at = _parse_iso_datetime(end_raw)
        if start_at is None or end_at is None:
            continue
        start_utc = start_at.astimezone(timezone.utc)
        end_utc = end_at.astimezone(timezone.utc)
        if start_utc <= current <= end_utc:
            candidates.append(
                {
                    **board,
                    "activity_start_at": start_at.isoformat(),
                    "activity_end_at": end_at.isoformat(),
                }
            )
    if not candidates:
        return None
    candidates.sort(
        key=lambda item: _parse_iso_datetime(item.get("activity_start_at")) or datetime.min.replace(tzinfo=timezone.utc),
        reverse=True,
    )
    return candidates[0]


def _attach_snapshot_analytics(snapshot: dict[str, Any]) -> dict[str, Any]:
    boards = snapshot.get("boards", []) if isinstance(snapshot, dict) else []
    if not isinstance(boards, list):
        boards = []
    snapshot["ended_analytics"] = _build_ended_boards_analytics(boards)
    snapshot["ongoing_analytics"] = _build_ongoing_boards_analytics(boards)
    return snapshot


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
    _persist_boards_to_history(boards, snapshot_generated_at_utc=generated_at)
    payload = _attach_snapshot_analytics(payload)
    _write_json_file(CACHE_PATH, payload)
    return payload


def _remember_snapshot(snapshot: dict[str, Any]) -> dict[str, Any]:
    snapshot = _attach_snapshot_analytics(snapshot)
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
    button.danger {
      background: #fff2f2;
      border-color: #efc6c6;
      color: var(--bad);
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
    }
    .board-card:hover {
      transform: translateY(-3px);
      box-shadow: 0 22px 48px rgba(17, 24, 39, 0.10);
      border-color: #d5ccb9;
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
    .predict-grid {
      display: grid;
      grid-template-columns: minmax(280px, 360px) minmax(0, 1fr);
      gap: 16px;
      align-items: start;
    }
    .predict-panel {
      border: 1px solid var(--line);
      border-radius: 14px;
      padding: 14px;
      background: linear-gradient(180deg, #fff 0%, #faf8f2 100%);
      display: grid;
      gap: 12px;
    }
    .predict-panel h3 {
      margin: 0;
      font-size: 16px;
    }
    .predict-panel .subtle {
      font-size: 12px;
      color: var(--muted);
      line-height: 1.6;
    }
    .predict-kpis {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 10px;
    }
    .predict-table { min-width: 760px; table-layout: auto; }
    .predict-table td:last-child,
    .predict-table th:last-child {
      white-space: normal;
    }
    .analytics-table {
      min-width: 0;
      table-layout: fixed;
    }
    .ended-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(560px, 1fr));
      gap: 16px;
      align-items: start;
    }
    .ended-card {
      border: 1px solid var(--line);
      border-radius: 14px;
      padding: 14px;
      background: linear-gradient(180deg, #fff 0%, #faf8f2 100%);
      display: grid;
      gap: 12px;
      min-width: 0;
      box-shadow: 0 18px 40px rgba(17, 24, 39, 0.06);
    }
    .ended-card.ended {
      background: linear-gradient(180deg, #fffdf9 0%, #faf6ed 100%);
      border-color: #e2d3b8;
    }
    .ended-card.ongoing {
      background: linear-gradient(180deg, #f7fffb 0%, #eef9f4 100%);
      border-color: #cfe7d7;
    }
    .ended-card h3 {
      margin: 0;
      font-size: 18px;
    }
    .ended-card .meta-row {
      display: flex;
      flex-wrap: wrap;
      gap: 10px 14px;
      font-size: 12px;
      color: var(--muted);
    }
    .ended-card .analytics-table th:first-child,
    .ended-card .analytics-table td:first-child {
      width: 26%;
    }
    .ended-card .analytics-table th:not(:first-child),
    .ended-card .analytics-table td:not(:first-child) {
      width: auto;
    }
    .volume-inline {
      display: grid;
      grid-template-columns: minmax(180px, 260px) auto;
      gap: 12px;
      align-items: end;
    }
    .volume-inline input {
      height: 38px;
      border-radius: 10px;
      border: 1px solid var(--line);
      padding: 0 10px;
      background: #fff;
      color: var(--text);
      font-size: 14px;
      width: 100%;
    }
    .empty {
      padding: 18px;
      border: 1px dashed var(--line);
      border-radius: 12px;
      color: var(--muted);
      font-size: 13px;
      text-align: center;
    }
    @media (max-width: 1480px) {
      .ended-grid { grid-template-columns: 1fr; }
    }
    @media (max-width: 1180px) {
      .kpi-grid, .board-meta, .predict-kpis { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .board-grid, .two-cols, .predict-grid { grid-template-columns: 1fr; }
      .ended-grid { grid-template-columns: 1fr; }
      .event-strip { grid-template-columns: 1fr; }
    }
    @media (max-width: 680px) {
      .kpi-grid, .board-meta { grid-template-columns: 1fr; }
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
      <h2 style="margin:0 0 12px;">进行中比赛最终门槛预测</h2>
      <p class="meta">仅对合约交易赛展示。直接填你假定的“最后一天全市场成交量”，页面就把它当作收官日量来推算最终门槛。</p>
      <div id="ongoing_cards" class="ended-grid"></div>
    </section>

    <section class="card">
      <h2 style="margin:0 0 12px;">已结束比赛最后一天增量对照</h2>
      <p class="meta">口径：最终榜单快照相对 24 小时前最近一档快照的门槛增量；奖励/门槛比值按比赛结束时奖励币种折算成 USDT 后计算。</p>
      <div id="ended_cards" class="ended-grid"></div>
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
        <button id="entry_save_btn" class="primary">保存录入</button>
      </div>
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
      <h2 style="margin:0 0 12px;">现货交易赛</h2>
      <div id="spot_boards" class="board-grid"></div>
    </section>

    <section class="card">
      <h2 style="margin:0 0 12px;">合约交易赛</h2>
      <div id="futures_boards" class="board-grid"></div>
    </section>
  </div>

  <script>
    const refreshBtn = document.getElementById("refresh_btn");
    const statusEl = document.getElementById("status");
    const metaEl = document.getElementById("meta");
    const summaryEl = document.getElementById("summary");
    const spotBoardsEl = document.getElementById("spot_boards");
    const futuresBoardsEl = document.getElementById("futures_boards");
    const entryBoardEl = document.getElementById("entry_board");
    const entryNameEl = document.getElementById("entry_name");
    const entryValueEl = document.getElementById("entry_value");
    const entryNoteEl = document.getElementById("entry_note");
    const entrySaveBtn = document.getElementById("entry_save_btn");
    const entriesBody = document.getElementById("entries_body");
    const ongoingCardsEl = document.getElementById("ongoing_cards");
    const endedCardsEl = document.getElementById("ended_cards");
    const apiBase = `${window.location.protocol}//${window.location.host}`;
    const FORECAST_COEFFICIENTS = {
      20: 0.012575,
      30: 0.010699,
      50: 0.009418,
      100: 0.002558,
      150: 0.002720,
      200: 0.002116,
    };
    const FORECAST_RANKS = [20, 30, 50, 100, 150, 200];
    const ONGOING_VOLUME_STORAGE_KEY = "competition_board_ongoing_volume_inputs_v1";

    let snapshot = null;

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

    function loadOngoingVolumeState() {
      try {
        const raw = window.localStorage.getItem(ONGOING_VOLUME_STORAGE_KEY);
        if (!raw) return {};
        const parsed = JSON.parse(raw);
        return parsed && typeof parsed === "object" ? parsed : {};
      } catch (err) {
        return {};
      }
    }

    function saveOngoingVolumeState(state) {
      try {
        window.localStorage.setItem(ONGOING_VOLUME_STORAGE_KEY, JSON.stringify(state || {}));
      } catch (err) {
      }
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

    function renderSummary(data) {
      const boards = Array.isArray(data.boards) ? data.boards : [];
      const entryCount = Array.isArray(data.entries) ? data.entries.length : 0;
      const totalBoards = boards.length;
      const totalUsers = boards.reduce((acc, item) => acc + Number(item.eligible_user_count || 0), 0);
      const rows = [
        { label: "榜单数", value: totalBoards },
        { label: "参赛样本总数", value: totalUsers.toLocaleString() },
        { label: "录入人数", value: entryCount },
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

    function renderBoard(board) {
      return `
        <article class="board-card">
          <div class="board-head">
            <div>
              <h3>${escapeHtml(board.label)}</h3>
              <p><a href="${escapeHtml(board.url)}" target="_blank" rel="noreferrer">${escapeHtml(board.url)}</a></p>
            </div>
            ${boardPill(board)}
          </div>
          <div class="event-strip">
            <div class="event-panel">
              <div class="label">活动周期</div>
              <div class="value">${escapeHtml(board.activity_period_text || "未提供")}</div>
              <div class="sub">结束时间会随卡片倒计时实时提示。</div>
            </div>
            ${countdownMarkup(board)}
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
      const spot = boards.filter((item) => item.market === "spot");
      const futures = boards.filter((item) => item.market === "futures");
      spotBoardsEl.innerHTML = spot.length ? spot.map(renderBoard).join("") : `<div class="empty">暂无现货榜单。</div>`;
      futuresBoardsEl.innerHTML = futures.length ? futures.map(renderBoard).join("") : `<div class="empty">暂无合约榜单。</div>`;
      updateCountdownPanels();
    }

    function renderEntryBoardOptions() {
      const boards = Array.isArray(snapshot && snapshot.boards) ? snapshot.boards : [];
      entryBoardEl.innerHTML = boards.map((item) => `
        <option value="${escapeHtml(item.board_key)}">${escapeHtml(item.label)} · ${escapeHtml(item.metric_label || "当前数值")}</option>
      `).join("");
    }

    function renderEndedAnalytics() {
      const analytics = snapshot && snapshot.ended_analytics ? snapshot.ended_analytics : {};
      const deltaRows = Array.isArray(analytics.delta_rows) ? analytics.delta_rows : [];
      const rewardRows = Array.isArray(analytics.reward_rows) ? analytics.reward_rows : [];
      if (!deltaRows.length && !rewardRows.length) {
        endedCardsEl.innerHTML = `<div class="empty">当前没有可用的已结束比赛历史快照。</div>`;
        return;
      }
      const grouped = new Map();
      deltaRows.forEach((item) => {
        const key = item.board_key || item.symbol || item.label;
        const existing = grouped.get(key) || { delta: null, rewards: [] };
        existing.delta = item;
        grouped.set(key, existing);
      });
      rewardRows.forEach((item) => {
        const key = item.board_key || item.symbol || item.label;
        const existing = grouped.get(key) || { delta: null, rewards: [] };
        existing.rewards.push(item);
        grouped.set(key, existing);
      });
      endedCardsEl.innerHTML = Array.from(grouped.entries()).map(([_, group]) => {
        const delta = group.delta;
        const rewards = Array.isArray(group.rewards) ? group.rewards : [];
        const label = (delta && delta.label) || (rewards[0] && rewards[0].label) || "-";
        const symbol = (delta && delta.symbol) || (rewards[0] && rewards[0].symbol) || "-";
        const metaRow = delta ? `
          <div class="meta-row">
            <span>最终快照：${escapeHtml(delta.final_capture || "-")}</span>
            <span>对比快照：${escapeHtml(delta.previous_capture || "-")}</span>
            <span>最后一天全市场量：${delta.last_day_market_volume === null || delta.last_day_market_volume === undefined ? "-" : fmtNum(delta.last_day_market_volume, 0)}</span>
          </div>
        ` : `<div class="meta-row"><span>未找到对应的历史增量快照。</span></div>`;
        const deltas = delta && delta.deltas ? delta.deltas : {};
        const deltaTable = delta ? `
          <div>
            <div class="meta" style="margin-bottom:8px;">最后一天门槛增量</div>
            <div class="table-wrap">
              <table class="analytics-table">
                <thead>
                  <tr>
                    <th class="num">前20</th>
                    <th class="num">前30</th>
                    <th class="num">前50</th>
                    <th class="num">前100</th>
                    <th class="num">前150</th>
                    <th class="num">前200</th>
                  </tr>
                </thead>
                <tbody>
                  <tr>
                    <td class="num">${fmtNum(deltas["20"], 0)}</td>
                    <td class="num">${fmtNum(deltas["30"], 0)}</td>
                    <td class="num">${fmtNum(deltas["50"], 0)}</td>
                    <td class="num">${fmtNum(deltas["100"], 0)}</td>
                    <td class="num">${fmtNum(deltas["150"], 0)}</td>
                    <td class="num">${fmtNum(deltas["200"], 0)}</td>
                  </tr>
                </tbody>
              </table>
            </div>
          </div>
        ` : `<div class="empty">没有可用的最后一天增量数据。</div>`;
        const rewardTable = rewards.length ? `
          <div>
            <div class="meta" style="margin-bottom:8px;">奖励 / 门槛交易量比值</div>
            <div class="table-wrap">
              <table class="analytics-table">
                <thead>
                  <tr>
                    <th>榜段</th>
                    <th class="num">奖励折 USDT</th>
                    <th class="num">最终门槛</th>
                    <th class="num">奖励 / 门槛</th>
                  </tr>
                </thead>
                <tbody>
                  ${rewards.map((item) => `
                    <tr>
                      <td>${escapeHtml(item.rank_label || "-")}</td>
                      <td class="num">${item.reward_value_usdt === null || item.reward_value_usdt === undefined ? "-" : fmtNum(item.reward_value_usdt, 2)}</td>
                      <td class="num">${fmtNum(item.cutoff_value, 2)}</td>
                      <td class="num">${item.ratio === null || item.ratio === undefined ? "-" : fmtNum(item.ratio, 6)}</td>
                    </tr>
                  `).join("")}
                </tbody>
              </table>
            </div>
          </div>
        ` : `<div class="empty">没有可用于计算奖励 / 门槛比值的奖励段数据。</div>`;
        return `
          <article class="ended-card">
            <div>
              <h3>${escapeHtml(symbol)} · ${escapeHtml(label)}</h3>
              ${metaRow}
            </div>
            ${deltaTable}
            ${rewardTable}
          </article>
        `;
      }).join("");
    }

    function renderOngoingAnalytics() {
      const analytics = snapshot && snapshot.ongoing_analytics ? snapshot.ongoing_analytics : {};
      const rows = Array.isArray(analytics.board_rows) ? analytics.board_rows : [];
      const savedVolumes = loadOngoingVolumeState();
      if (!rows.length) {
        ongoingCardsEl.innerHTML = `<div class="empty">当前没有进行中的合约交易赛可用于预测。</div>`;
        return;
      }
      ongoingCardsEl.innerHTML = rows.map((item) => `
        <article class="ended-card ongoing" data-ongoing-board="${escapeHtml(item.board_key || "")}">
          <div>
            <h3>${escapeHtml(item.symbol || "-")} · ${escapeHtml(item.label || "-")}</h3>
            <div class="meta-row">
              <span>当前快照：${escapeHtml(fmtDate(item.updated_at_utc) || "-")}</span>
              <span>最新一天交易量：${item.latest_day_volume === null || item.latest_day_volume === undefined ? "-" : fmtNum(item.latest_day_volume, 0)}</span>
            </div>
          </div>
          <div>
            <div class="meta" style="margin-bottom:8px;">假定收官日全市场成交量</div>
            <div class="volume-inline">
              <label>
                <input class="ongoing-volume-input" type="number" step="0.01" min="0" placeholder="直接填最后一天，例如 176000000" />
              </label>
              <div class="meta">这里输入的值会直接当作最后一天全市场成交量；留空时默认使用最近一天交易量${item.previous_capture ? `（对比快照：${escapeHtml(item.previous_capture)}）` : ""}。</div>
            </div>
          </div>
          <div>
            <div class="meta" style="margin-bottom:8px;">最终门槛</div>
            <div class="table-wrap">
              <table class="analytics-table">
                <thead>
                  <tr>
                    <th class="num">排名</th>
                    <th class="num">最终门槛</th>
                  </tr>
                </thead>
                <tbody class="ongoing-threshold-body">
                  ${FORECAST_RANKS.map((rank) => `
                    <tr data-rank="${rank}">
                      <td class="num">${rank}</td>
                      <td class="num">-</td>
                    </tr>
                  `).join("")}
                </tbody>
              </table>
            </div>
          </div>
          <div>
            <div class="meta" style="margin-bottom:8px;">奖励 / 最终门槛交易量比值</div>
            ${item.reward_rows.length ? `
              <div class="table-wrap">
                <table class="analytics-table">
                  <thead>
                    <tr>
                      <th>榜段</th>
                      <th class="num">奖励折 USDT</th>
                      <th class="num">最终门槛</th>
                      <th class="num">奖励 / 门槛</th>
                    </tr>
                  </thead>
                  <tbody class="ongoing-reward-body">
                    ${item.reward_rows.map((reward) => `
                      <tr data-forecast-rank="${reward.forecast_rank === null || reward.forecast_rank === undefined ? "" : reward.forecast_rank}">
                        <td>${escapeHtml(reward.rank_label || "-")}</td>
                        <td class="num">${reward.reward_value_usdt === null || reward.reward_value_usdt === undefined ? "-" : fmtNum(reward.reward_value_usdt, 2)}</td>
                        <td class="num">${reward.current_cutoff_value === null || reward.current_cutoff_value === undefined ? "-" : fmtNum(reward.current_cutoff_value, 2)}</td>
                        <td class="num">${reward.ratio === null || reward.ratio === undefined ? "-" : fmtNum(reward.ratio, 6)}</td>
                      </tr>
                    `).join("")}
                  </tbody>
                </table>
              </div>
            ` : `<div class="empty">${escapeHtml(item.reward_rows_message || "当前没有可用于计算奖励 / 门槛比值的数据。")}</div>`}
          </div>
        </article>
      `).join("");

      ongoingCardsEl.querySelectorAll(".ended-card.ongoing").forEach((card) => {
        const input = card.querySelector(".ongoing-volume-input");
        const body = card.querySelector(".ongoing-threshold-body");
        const rewardBody = card.querySelector(".ongoing-reward-body");
        if (!input || !body) return;
        const boardKey = card.dataset.ongoingBoard || "";
        const board = rows.find((item) => item.board_key === boardKey);
        if (!board) return;
        const currentValues = board.current_values || {};
        const defaultVolume = Number(board.latest_day_volume);
        if (savedVolumes && savedVolumes[boardKey] !== undefined && savedVolumes[boardKey] !== null) {
          input.value = String(savedVolumes[boardKey]);
        }
        const updateTable = () => {
          const hasManualValue = input.value.trim() !== "";
          const assumedVolume = Number(hasManualValue ? input.value : defaultVolume);
          const safeVolume = Number.isFinite(assumedVolume) && assumedVolume >= 0 ? assumedVolume : 0;
          const nextState = loadOngoingVolumeState();
          if (hasManualValue) {
            nextState[boardKey] = input.value.trim();
          } else {
            delete nextState[boardKey];
          }
          saveOngoingVolumeState(nextState);
          const predictedValues = {};
          body.querySelectorAll("tr[data-rank]").forEach((row) => {
            const rank = row.dataset.rank || "";
            const coefficient = FORECAST_COEFFICIENTS[rank];
            const currentValue = currentValues[rank];
            const delta = coefficient ? safeVolume * coefficient : null;
            const predicted = (currentValue !== null && currentValue !== undefined && delta !== null)
              ? Number(currentValue) + Number(delta)
              : (currentValue !== null && currentValue !== undefined ? Number(currentValue) : null);
            predictedValues[rank] = predicted;
            const cells = row.querySelectorAll("td");
            if (cells.length < 2) return;
            cells[1].textContent = predicted === null ? "-" : fmtNum(predicted, 0);
          });
          if (rewardBody) {
            rewardBody.querySelectorAll("tr").forEach((row) => {
              const rank = row.dataset.forecastRank || "";
              const forecastValue = Object.prototype.hasOwnProperty.call(predictedValues, rank)
                ? predictedValues[rank]
                : null;
              const reward = board.reward_rows.find((item) => String(item.forecast_rank ?? "") === rank) || null;
              if (!reward) return;
              const finalCutoff = forecastValue !== null && forecastValue !== undefined
                ? Number(forecastValue)
                : (reward.current_cutoff_value !== null && reward.current_cutoff_value !== undefined ? Number(reward.current_cutoff_value) : null);
              const ratio = reward.reward_value_usdt !== null && reward.reward_value_usdt !== undefined && finalCutoff && finalCutoff > 0
                ? Number(reward.reward_value_usdt) / Number(finalCutoff)
                : null;
              const cells = row.querySelectorAll("td");
              if (cells.length < 4) return;
              cells[2].textContent = finalCutoff === null ? "-" : fmtNum(finalCutoff, 2);
              cells[3].textContent = ratio === null ? "-" : fmtNum(ratio, 6);
            });
          }
        };
        input.addEventListener("input", updateTable);
        updateTable();
      });
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
          <td><button class="danger" data-delete-id="${escapeHtml(item.id)}">删除</button></td>
        </tr>
      `).join("");
      entriesBody.querySelectorAll("button[data-delete-id]").forEach((btn) => {
        btn.addEventListener("click", async () => {
          await saveEntry({ action: "delete", entry_id: btn.dataset.deleteId });
        });
      });
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
        renderOngoingAnalytics();
        renderEndedAnalytics();
        renderBoards();
        renderEntryBoardOptions();
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
          renderEndedAnalytics();
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
    entrySaveBtn.addEventListener("click", async () => {
      const boardKey = entryBoardEl.value.trim();
      const name = entryNameEl.value.trim();
      const value = Number(entryValueEl.value);
      const note = entryNoteEl.value.trim();
      if (!boardKey || !name || !Number.isFinite(value) || value < 0) {
        setStatus("请完整填写榜单、姓名和当前数值。", true);
        return;
      }
      entrySaveBtn.disabled = true;
      setStatus("正在保存录入...");
      try {
        await saveEntry({ action: "upsert", board_key: boardKey, name, value, note });
        entryNameEl.value = "";
        entryValueEl.value = "";
        entryNoteEl.value = "";
        setStatus("录入已保存。");
      } finally {
        entrySaveBtn.disabled = false;
      }
    });

    window.setInterval(updateCountdownPanels, 1000);

    loadSnapshot(false);
  </script>
</body>
</html>
"""
