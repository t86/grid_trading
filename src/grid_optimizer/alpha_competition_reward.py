from __future__ import annotations

import math
import re
from datetime import datetime
from typing import Any

from .alpha_competition_metrics import (
    BinanceCompetitionRuleProvider, _CMS_DETAIL_PATH, _body_blocks, parse_competition_rule,
)
from .alpha_market import AlphaMarketClient


def reward_tokens_per_winner(detail: dict[str, Any], *, symbol: str, winner_count: int) -> float:
    number = r"[0-9][0-9,]*(?:\.[0-9]+)?"
    pattern = re.compile(
        rf"\btop\s+(?P<winners>[0-9,]+)\s+users\b.*?\bshare\s+"
        rf"(?P<pool>{number})\s+{re.escape(symbol)}\s+tokens\s+equally"
        rf"(?:\s*\(\s*=\s*(?P<each>{number})\s+{re.escape(symbol)}\s+per\s+user\s*\))?",
        re.IGNORECASE,
    )
    amounts: set[float] = set()
    for block in _body_blocks(detail):
        for match in pattern.finditer(block.text):
            if int(match['winners'].replace(',', '')) != winner_count:
                raise ValueError("reward winner count differs from the competition")
            amount = float(match['pool'].replace(',', '')) / winner_count
            if match['each'] and not math.isclose(amount, float(match['each'].replace(',', '')), rel_tol=1e-6):
                raise ValueError("reward pool and per-user reward conflict")
            if not math.isfinite(amount) or amount <= 0:
                raise ValueError("reward amount is invalid")
            amounts.add(amount)
    if len(amounts) != 1:
        raise ValueError("unambiguous equal per-user token reward is required")
    return amounts.pop()


def fetch_ended_reward(row: dict[str, Any], *, market: Any = None, rules: Any = None) -> dict[str, Any]:
    provider = rules or BinanceCompetitionRuleProvider()
    detail = provider._get_data(_CMS_DETAIL_PATH, {"articleCode": row['articleCode']})
    if detail.get('code') != row['articleCode']:
        raise ValueError("reward announcement identity differs")
    rule = parse_competition_rule({"data": detail}, expected_symbol=row['symbol'])
    end = datetime.fromisoformat(row['endUtc'])
    if rule.winner_count != row['winnerCount'] or not any(
        round_.number == row['round'] and round_.end_utc == end for round_ in rule.rounds
    ):
        raise ValueError("reward announcement round differs")
    amount = reward_tokens_per_winner(detail, symbol=rule.symbol, winner_count=rule.winner_count)
    client = market or AlphaMarketClient()
    token = client.fetch_tokens().get(rule.symbol)
    if token is None or not token.pair.endswith('USDT'):
        raise ValueError("USDT reward price pair is unavailable")
    end_ms = int(end.timestamp() * 1000)
    candles = client.fetch_klines(
        token.pair, interval='1m', limit=1,
        start_time_ms=end_ms - 60_000, end_time_ms=end_ms - 1,
    )
    if len(candles) != 1 or len(candles[0]) < 7:
        raise ValueError("competition-end price candle is unavailable")
    candle = candles[0]
    if int(candle[0]) != end_ms - 60_000 or int(candle[6]) != end_ms - 1:
        raise ValueError("price candle does not end at the competition boundary")
    price = float(candle[4])
    value = amount * price
    if not math.isfinite(price) or price <= 0 or not math.isfinite(value) or value <= 0:
        raise ValueError("competition-end reward price is invalid")
    return {
        'rewardValueU': value,
        'rewardTokensPerWinner': amount,
        'rewardEndPriceU': price,
        'rewardPricePair': token.pair,
        'rewardPriceAtUtc': end.isoformat(),
        'rewardSource': 'official_reward_alpha_end_close',
    }
