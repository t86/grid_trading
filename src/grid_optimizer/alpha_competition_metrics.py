from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import html
import json
import math
import os
from pathlib import Path
import re
import tempfile
import threading
from typing import Any

import requests


_UTC = timezone.utc
_TITLE_PREFIX = "Binance Alpha Trading Competition: Trade "
_COMPETITION_TITLE_SYMBOL_RE = re.compile(
    r"^Binance Alpha Trading Competition:\s*Trade\s+.+?\(([A-Z0-9_]+)\)\s+and\b",
    re.IGNORECASE,
)
_CMS_BASE_URL = "https://www.binance.com"
_CMS_LIST_PATH = "/bapi/composite/v1/public/cms/article/list/query"
_CMS_DETAIL_PATH = "/bapi/composite/v1/public/cms/article/detail/query"
_PERIOD_RE = re.compile(
    r"\b(?P<number>\d+)(?P<suffix>st|nd|rd|th)\s+(?P<symbol>[A-Za-z0-9_]+)\s+"
    r"Trading\s+Competition\s+Promotion\s+Period\s*:\s*"
    r"(?P<start>\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2})\s+\(UTC\)\s+to\s+"
    r"(?P<end>\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2})\s+\(UTC\)",
)
_WINNER_RE = re.compile(r"\bThe\s+top\s+([0-9][0-9,]*)\s+users\b")
_DAY_ROW_START_RE = re.compile(r"^Day\s+(?P<day>\d+)\b(?!\s+(?:through|to)\b)", re.IGNORECASE)
_DAY_MARKER_RE = re.compile(r"\bDay\s+(?P<day>\d+)\b(?!\s+(?:through|to)\b)", re.IGNORECASE)
_MULTIPLIER_RE = re.compile(r"(?<![\w.])([+-]?(?:\d+(?:\.\d*)?|\.\d+))\s*x\b")
_RISING_TRADER_RE = re.compile(r"\bRising\s+Trader\b", re.IGNORECASE)
_BLOCK_NODES = {"paragraph", "p", "row", "heading", "listItem", "list-item"}
_ELEMENT_BLOCK_TAGS = {"p", "li", "h1", "h2", "h3", "h4", "h5", "h6"}


class RuleParseError(ValueError):
    """The official competition article did not contain a usable rule set."""


@dataclass(frozen=True)
class _ArticleBlock:
    text: str
    multiplier_row: bool = False


@dataclass(frozen=True)
class CompetitionRound:
    number: int
    start_utc: datetime
    end_utc: datetime


@dataclass(frozen=True)
class CompetitionRule:
    symbol: str
    name: str
    article_code: str
    title: str
    article_url: str
    published_at_utc: datetime
    winner_count: int
    rounds: tuple[CompetitionRound, ...]
    multipliers: tuple[float, ...]


@dataclass(frozen=True)
class CachedRuleResult:
    rule: CompetitionRule
    stale: bool


@dataclass(frozen=True)
class Thresholds:
    average: float
    watch: float
    reference: float
    safe: float


@dataclass(frozen=True)
class OfficialVolumeSnapshot:
    weighted_volume: float
    updated_at_utc: datetime


@dataclass(frozen=True)
class VolumeSnapshot:
    weighted_volume: float
    source: str
    updated_at_utc: datetime


@dataclass(frozen=True)
class RoundSelection:
    status: str
    round: CompetitionRound | None
    day: int | None
    multiplier: float | None


@dataclass(frozen=True)
class _CachedRule:
    fetched_at: datetime
    rule: CompetitionRule


def _normalize_text(value: str) -> str:
    return " ".join(html.unescape(value).split())


def _text_descendants(node: object) -> list[str]:
    if isinstance(node, list):
        return [text for child in node for text in _text_descendants(child)]
    if not isinstance(node, Mapping):
        return []
    if node.get("node") == "text" and isinstance(node.get("text"), str):
        return [node["text"]]
    return [text for value in node.values() if isinstance(value, (Mapping, list)) for text in _text_descendants(value)]


def _collect_blocks(node: object) -> list[_ArticleBlock]:
    if isinstance(node, list):
        return [block for child in node for block in _collect_blocks(child)]
    if not isinstance(node, Mapping):
        return []
    if node.get("node") == "text" and isinstance(node.get("text"), str):
        block = _normalize_text(node["text"])
        return [_ArticleBlock(block)] if block else []
    node_type = node.get("node")
    tag = node.get("tag")
    table_row = node_type == "row" or node_type == "element" and isinstance(tag, str) and tag.lower() == "tr"
    semantic_block = node_type in _BLOCK_NODES or (
        node_type == "element" and isinstance(tag, str) and tag.lower() in _ELEMENT_BLOCK_TAGS
    )
    if table_row or semantic_block:
        block = _normalize_text(" ".join(_text_descendants(node)))
        return [_ArticleBlock(block, multiplier_row=table_row)] if block else []
    return [block for value in node.values() if isinstance(value, (Mapping, list)) for block in _collect_blocks(value)]


def _article_data(article: object) -> Mapping[str, Any]:
    if not isinstance(article, Mapping):
        raise RuleParseError("article data is missing")
    data = article.get("data")
    if not isinstance(data, Mapping):
        raise RuleParseError("article data is missing")
    return data


def _required_text(data: Mapping[str, Any], key: str) -> str:
    value = data.get(key)
    if not isinstance(value, str) or not value.strip():
        raise RuleParseError(f"article {key} is missing")
    return value.strip()


def _published_at_utc(data: Mapping[str, Any]) -> datetime:
    value = data.get("publishDate")
    if isinstance(value, bool):
        raise RuleParseError("article publishDate is missing or invalid")
    if isinstance(value, int):
        milliseconds = value
    elif isinstance(value, str) and re.fullmatch(r"\d+", value):
        milliseconds = int(value)
    else:
        raise RuleParseError("article publishDate is missing or invalid")
    try:
        return datetime.fromtimestamp(milliseconds / 1000, tz=_UTC)
    except (OverflowError, OSError, ValueError) as exc:
        raise RuleParseError("article publishDate is missing or invalid") from exc


def _body_blocks(data: Mapping[str, Any]) -> tuple[_ArticleBlock, ...]:
    body = data.get("body")
    if not isinstance(body, str):
        raise RuleParseError("article body is missing")
    try:
        tree = json.loads(body)
    except json.JSONDecodeError as exc:
        raise RuleParseError("article body is malformed") from exc
    return tuple(_collect_blocks(tree))


def _parse_title(title: str, symbol: str) -> str:
    if not title.startswith(_TITLE_PREFIX):
        raise RuleParseError("article title does not match the competition")
    remainder = title[len(_TITLE_PREFIX):]
    identity_start = remainder.find(" (")
    expected_identity = f" ({symbol})"
    if identity_start < 0 or not remainder.startswith(expected_identity, identity_start):
        raise RuleParseError("article title does not match the competition")
    suffix = remainder[identity_start + len(expected_identity):]
    if re.match(r"\s+and\b", suffix) is None:
        raise RuleParseError("article title does not match the competition")
    return remainder[:identity_start].strip() or symbol


def _ordinal_suffix(number: int) -> str:
    if 10 <= number % 100 <= 20:
        return "th"
    return {1: "st", 2: "nd", 3: "rd"}.get(number % 10, "th")


def _parse_datetime(value: str) -> datetime:
    try:
        return datetime.strptime(value, "%Y-%m-%d %H:%M").replace(tzinfo=_UTC)
    except ValueError as exc:
        raise RuleParseError("promotion round has an invalid date") from exc


def _legacy_multiplier_block(block: _ArticleBlock) -> bool:
    if block.multiplier_row or _DAY_ROW_START_RE.match(block.text) is None:
        return False
    return [int(marker.group("day")) for marker in _DAY_MARKER_RE.finditer(block.text)] == list(range(1, 8))


def _multiplier_blocks(blocks: tuple[_ArticleBlock, ...]) -> tuple[_ArticleBlock, ...]:
    explicit = tuple(
        block for block in blocks if block.multiplier_row and _DAY_ROW_START_RE.match(block.text) is not None
    )
    if explicit:
        return explicit
    return tuple(block for block in blocks if _legacy_multiplier_block(block))


def _parse_rounds(blocks: tuple[_ArticleBlock, ...], expected_symbol: str) -> tuple[CompetitionRound, ...]:
    multiplier_blocks = _multiplier_blocks(blocks)
    first_day = next(
        (index for index, block in enumerate(blocks) if block in multiplier_blocks),
        len(blocks),
    )
    rounds_by_number: dict[int, CompetitionRound] = {}
    for block in blocks[:first_day]:
        for match in _PERIOD_RE.finditer(block.text):
            number = int(match.group("number"))
            if match.group("symbol") != expected_symbol:
                raise RuleParseError("promotion round symbol does not match article")
            if number <= 0 or match.group("suffix") != _ordinal_suffix(number):
                raise RuleParseError("promotion round number is invalid")
            round_ = CompetitionRound(number, _parse_datetime(match.group("start")), _parse_datetime(match.group("end")))
            if round_.start_utc >= round_.end_utc or round_.end_utc - round_.start_utc != timedelta(days=7):
                raise RuleParseError("promotion round must last exactly seven days")
            existing = rounds_by_number.get(number)
            if existing is not None and existing != round_:
                raise RuleParseError("promotion round descriptions conflict")
            rounds_by_number[number] = round_
    if not rounds_by_number:
        raise RuleParseError("at least one full promotion round is required")
    return tuple(rounds_by_number[number] for number in sorted(rounds_by_number))


def _parse_winner_count(blocks: tuple[_ArticleBlock, ...]) -> int:
    match = _WINNER_RE.search(" ".join(block.text for block in blocks))
    if match is None:
        raise RuleParseError("winner count is missing")
    raw = match.group(1)
    if "," in raw:
        valid = re.fullmatch(r"[0-9]{1,3}(?:,[0-9]{3})+", raw) is not None
    else:
        valid = re.fullmatch(r"[0-9]+", raw) is not None
    if not valid:
        raise RuleParseError("winner count is invalid")
    count = int(raw.replace(",", ""))
    if count <= 0:
        raise RuleParseError("winner count must be positive")
    return count


def _parse_multipliers(blocks: tuple[_ArticleBlock, ...]) -> tuple[float, ...]:
    multipliers: dict[int, float] = {}
    for block in _multiplier_blocks(blocks):
        markers = list(_DAY_MARKER_RE.finditer(block.text))
        for index, marker in enumerate(markers):
            day = int(marker.group("day"))
            if day in multipliers:
                raise RuleParseError("Day multiplier is duplicated")
            end = markers[index + 1].start() if index + 1 < len(markers) else len(block.text)
            rising_trader = _RISING_TRADER_RE.search(block.text, marker.end(), end)
            if rising_trader is not None:
                end = rising_trader.start()
            multiplier_match = _MULTIPLIER_RE.search(block.text, marker.end(), end)
            if multiplier_match is None:
                raise RuleParseError("Day multiplier is missing")
            multiplier = float(multiplier_match.group(1))
            if multiplier <= 0 or not math.isfinite(multiplier):
                raise RuleParseError("Day multiplier must be positive and finite")
            multipliers[day] = multiplier
    if set(multipliers) != set(range(1, 8)):
        raise RuleParseError("Day multipliers must contain Days 1 through 7 exactly once")
    return tuple(multipliers[day] for day in range(1, 8))


def parse_competition_rule(article: object, expected_symbol: object) -> CompetitionRule:
    """Parse a Binance Alpha competition CMS article without performing I/O."""
    if not isinstance(expected_symbol, str) or not expected_symbol.strip():
        raise RuleParseError("expected symbol is missing")
    symbol = expected_symbol.strip().upper()
    data = _article_data(article)
    code = _required_text(data, "code")
    title = _required_text(data, "title")
    blocks = _body_blocks(data)
    return CompetitionRule(
        symbol=symbol,
        name=_parse_title(title, symbol),
        article_code=code,
        title=title,
        article_url=f"https://www.binance.com/en/support/announcement/detail/{code}",
        published_at_utc=_published_at_utc(data),
        winner_count=_parse_winner_count(blocks),
        rounds=_parse_rounds(blocks, symbol),
        multipliers=_parse_multipliers(blocks),
    )


def _normalized_symbol(value: object) -> str:
    if not isinstance(value, str) or not value.strip():
        raise RuleParseError("expected symbol is missing")
    return value.strip().upper()


def _require_utc_aware(value: datetime, name: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError(f"{name} must be UTC-aware")
    return value.astimezone(_UTC)


def _article_release_ms(item: Mapping[str, Any]) -> int:
    value = item.get("releaseDate")
    if isinstance(value, bool):
        raise RuleParseError("Binance CMS article release date is invalid")
    if isinstance(value, int):
        return value
    if isinstance(value, str) and re.fullmatch(r"\d+", value):
        return int(value)
    raise RuleParseError("Binance CMS article release date is invalid")


def _list_article_code(item: Mapping[str, Any]) -> str:
    value = item.get("code")
    if not isinstance(value, str) or not value.strip():
        raise RuleParseError("Binance CMS article code is invalid")
    return value.strip()


def _list_articles(data: Mapping[str, Any]) -> list[object]:
    if "articles" in data:
        articles = data.get("articles")
        if not isinstance(articles, list):
            raise RuleParseError("Binance CMS article list is invalid")
        return articles

    catalogs = data.get("catalogs")
    if not isinstance(catalogs, list):
        raise RuleParseError("Binance CMS article list is invalid")
    matches: list[Mapping[str, Any]] = []
    pending: list[object] = list(catalogs)
    while pending:
        catalog = pending.pop()
        if not isinstance(catalog, Mapping):
            raise RuleParseError("Binance CMS article list is invalid")
        children = catalog.get("catalogs", [])
        if not isinstance(children, list):
            raise RuleParseError("Binance CMS article list is invalid")
        pending.extend(children)
        catalog_id = catalog.get("catalogId")
        if isinstance(catalog_id, int) and not isinstance(catalog_id, bool) and catalog_id == 93:
            matches.append(catalog)
    if len(matches) != 1:
        raise RuleParseError("Binance CMS article list is invalid")
    articles = matches[0].get("articles")
    if not isinstance(articles, list):
        raise RuleParseError("Binance CMS article list is invalid")
    return articles


def _competition_symbol_from_title(item: Mapping[str, Any]) -> str | None:
    title = item.get("title")
    if not isinstance(title, str):
        raise RuleParseError("Binance CMS article title is invalid")
    match = _COMPETITION_TITLE_SYMBOL_RE.search(title.strip())
    if match is None:
        return None
    return match.group(1).upper()


def _title_matches_symbol(item: Mapping[str, Any], symbol: str) -> bool:
    title = item.get("title")
    if not isinstance(title, str):
        raise RuleParseError("Binance CMS article title is invalid")
    try:
        _parse_title(title.strip(), symbol)
    except RuleParseError:
        return False
    return True


class BinanceCompetitionRuleProvider:
    def __init__(self, session: requests.Session | None = None) -> None:
        self.session = session or requests.Session()

    def _get_data(self, path: str, params: dict[str, object]) -> Mapping[str, Any]:
        try:
            response = self.session.get(f"{_CMS_BASE_URL}{path}", params=params, timeout=20)
            response.raise_for_status()
            payload = response.json()
        except requests.RequestException:
            raise RuleParseError("Binance CMS request failed") from None
        except ValueError:
            raise RuleParseError("Binance CMS response is malformed") from None
        if not isinstance(payload, Mapping) or payload.get("code") != "000000":
            raise RuleParseError("Binance CMS response was rejected")
        data = payload.get("data")
        if not isinstance(data, Mapping):
            raise RuleParseError("Binance CMS response data is invalid")
        return data

    def fetch_recent_symbols(self, *, now: datetime | None = None, days: int = 60) -> list[str]:
        current = datetime.now(_UTC) if now is None else _require_utc_aware(now, "now")
        cutoff_ms = int((current - timedelta(days=days)).timestamp() * 1000)
        symbols: list[str] = []
        seen: set[str] = set()
        for page_no in range(1, 21):
            data = self._get_data(
                _CMS_LIST_PATH,
                {"type": 1, "catalogId": 93, "pageNo": page_no, "pageSize": 50},
            )
            raw_articles = _list_articles(data)
            articles: list[Mapping[str, Any]] = []
            for item in raw_articles:
                if not isinstance(item, Mapping):
                    raise RuleParseError("Binance CMS article list is invalid")
                _list_article_code(item)
                _article_release_ms(item)
                articles.append(item)
            for item in articles:
                if _article_release_ms(item) < cutoff_ms:
                    continue
                symbol = _competition_symbol_from_title(item)
                if symbol is None or symbol in seen:
                    continue
                seen.add(symbol)
                symbols.append(symbol)
            page_is_old = bool(articles) and all(_article_release_ms(item) < cutoff_ms for item in articles)
            if not articles or page_is_old:
                break
        return symbols

    def fetch_rule(self, symbol: str, *, now: datetime | None = None) -> CompetitionRule:
        target = _normalized_symbol(symbol)
        current = datetime.now(_UTC) if now is None else _require_utc_aware(now, "now")
        cutoff_ms = int((current - timedelta(days=60)).timestamp() * 1000)
        candidates: list[Mapping[str, Any]] = []
        for page_no in range(1, 21):
            data = self._get_data(
                _CMS_LIST_PATH,
                {"type": 1, "catalogId": 93, "pageNo": page_no, "pageSize": 50},
            )
            raw_articles = _list_articles(data)
            articles: list[Mapping[str, Any]] = []
            for item in raw_articles:
                if not isinstance(item, Mapping):
                    raise RuleParseError("Binance CMS article list is invalid")
                _list_article_code(item)
                _article_release_ms(item)
                articles.append(item)
            candidates.extend(
                item
                for item in articles
                if _article_release_ms(item) >= cutoff_ms and _title_matches_symbol(item, target)
            )
            page_is_old = bool(articles) and all(_article_release_ms(item) < cutoff_ms for item in articles)
            if candidates or not articles or page_is_old:
                break
        if not candidates:
            raise RuleParseError(f"no recent competition announcement for {target}")
        latest = max(candidates, key=_article_release_ms)
        code = _list_article_code(latest)
        detail = self._get_data(_CMS_DETAIL_PATH, {"articleCode": code})
        rule = parse_competition_rule({"data": detail}, expected_symbol=target)
        if rule.article_code != code:
            raise RuleParseError("article detail code does not match article list")
        return _validate_rule(rule, expected_symbol=target)


def _encode_datetime(value: datetime, name: str) -> str:
    return _require_utc_aware(value, name).isoformat()


def _decode_datetime(value: object, name: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError(f"{name} is invalid")
    parsed = datetime.fromisoformat(value)
    return _require_utc_aware(parsed, name)


def _positive_finite_multiplier(value: object) -> float:
    try:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise TypeError
        multiplier = float(value)
    except (OverflowError, TypeError, ValueError):
        raise RuleParseError("competition rule multiplier is invalid") from None
    if multiplier <= 0 or not math.isfinite(multiplier):
        raise RuleParseError("competition rule multiplier is invalid")
    return multiplier


def _validate_rule(rule: object, *, expected_symbol: str | None = None) -> CompetitionRule:
    if not isinstance(rule, CompetitionRule):
        raise RuleParseError("competition rule is invalid")
    try:
        symbol = _normalized_symbol(rule.symbol)
        if symbol != rule.symbol or re.fullmatch(r"[A-Z0-9_]+", symbol) is None:
            raise ValueError
        if expected_symbol is not None and symbol != expected_symbol:
            raise ValueError
        if not rule.name.strip() or not rule.article_code.strip() or not rule.title.strip() or not rule.article_url.strip():
            raise ValueError
        if isinstance(rule.winner_count, bool) or not isinstance(rule.winner_count, int) or rule.winner_count <= 0:
            raise ValueError
        _require_utc_aware(rule.published_at_utc, "published_at_utc")
        if not rule.rounds:
            raise ValueError
        numbers: list[int] = []
        timeline: list[tuple[datetime, datetime, int]] = []
        for round_ in rule.rounds:
            if (
                not isinstance(round_, CompetitionRound)
                or isinstance(round_.number, bool)
                or not isinstance(round_.number, int)
                or round_.number <= 0
            ):
                raise ValueError
            start = _require_utc_aware(round_.start_utc, "round start_utc")
            end = _require_utc_aware(round_.end_utc, "round end_utc")
            if start >= end or end - start != timedelta(days=7):
                raise ValueError
            numbers.append(round_.number)
            timeline.append((start, end, round_.number))
        if numbers != list(range(1, len(numbers) + 1)):
            raise ValueError
        ordered_timeline = sorted(timeline)
        if [number for _start, _end, number in ordered_timeline] != numbers:
            raise ValueError
        if any(left[1] > right[0] for left, right in zip(ordered_timeline, ordered_timeline[1:])):
            raise ValueError
        if len(rule.multipliers) != 7:
            raise ValueError
        for multiplier in rule.multipliers:
            _positive_finite_multiplier(multiplier)
    except (AttributeError, OverflowError, TypeError, ValueError, RuleParseError):
        raise RuleParseError("competition rule is invalid") from None
    return rule


def select_round(rule: CompetitionRule, now: datetime) -> RoundSelection:
    valid_rule = _validate_rule(rule)
    current = _require_utc_aware(now, "now")
    ordered = sorted(valid_rule.rounds, key=lambda item: item.start_utc)
    if current < ordered[0].start_utc:
        return RoundSelection("upcoming", None, None, None)
    for round_ in ordered:
        if round_.start_utc <= current < round_.end_utc:
            day = int((current - round_.start_utc).total_seconds() // 86_400) + 1
            return RoundSelection("active", round_, day, valid_rule.multipliers[day - 1])
    for left, right in zip(ordered, ordered[1:]):
        if left.end_utc <= current < right.start_utc:
            return RoundSelection("between_rounds", None, None, None)
    return RoundSelection("ended", None, None, None)


def _validate_round_window(round_: object) -> CompetitionRound:
    if not isinstance(round_, CompetitionRound):
        raise ValueError("competition round is invalid")
    start = _require_utc_aware(round_.start_utc, "round start_utc")
    end = _require_utc_aware(round_.end_utc, "round end_utc")
    if start >= end or end - start != timedelta(days=7):
        raise ValueError("competition round is invalid")
    return round_


def _kline_open_time_ms(value: object) -> int:
    if isinstance(value, bool):
        raise ValueError("kline open time is invalid")
    if isinstance(value, int):
        open_ms = value
    elif isinstance(value, str) and re.fullmatch(r"\d+", value):
        open_ms = int(value)
    else:
        raise ValueError("kline open time is invalid")
    if open_ms < 0:
        raise ValueError("kline open time is invalid")
    return open_ms


def _kline_quote_volume(value: object) -> float:
    if isinstance(value, bool):
        raise ValueError("kline quote volume is invalid")
    try:
        volume = float(value)
    except (OverflowError, TypeError, ValueError):
        raise ValueError("kline quote volume is invalid") from None
    if volume < 0 or not math.isfinite(volume):
        raise ValueError("kline quote volume is invalid")
    return volume


def _official_weighted_volume(value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("official weighted volume is invalid")
    try:
        volume = float(value)
    except (OverflowError, TypeError, ValueError):
        raise ValueError("official weighted volume is invalid") from None
    if volume < 0 or not math.isfinite(volume):
        raise ValueError("official weighted volume is invalid")
    return volume


def weight_kline_volume(
    round_: CompetitionRound,
    multipliers: tuple[float, ...],
    rows: list[list[Any]],
) -> float:
    valid_round = _validate_round_window(round_)
    if not isinstance(multipliers, tuple) or len(multipliers) != 7:
        raise ValueError("early-bird multipliers must contain seven days")
    valid_multipliers = tuple(_positive_finite_multiplier(value) for value in multipliers)
    if not isinstance(rows, list):
        raise ValueError("kline rows are invalid")

    seen: set[int] = set()
    total = 0.0
    for row in rows:
        if not isinstance(row, list) or len(row) <= 7:
            raise ValueError("kline row is invalid")
        open_ms = _kline_open_time_ms(row[0])
        quote_volume = _kline_quote_volume(row[7])
        if open_ms in seen:
            continue
        seen.add(open_ms)
        try:
            opened = datetime.fromtimestamp(open_ms / 1000, tz=_UTC)
        except (OverflowError, OSError, ValueError):
            raise ValueError("kline open time is invalid") from None
        if not valid_round.start_utc <= opened < valid_round.end_utc:
            continue
        day_index = int((opened - valid_round.start_utc).total_seconds() // 86_400)
        total += quote_volume * valid_multipliers[day_index]
    if not math.isfinite(total):
        raise ValueError("weighted kline volume is invalid")
    return total


def calculate_thresholds(*, weighted_volume: float, winner_count: int) -> Thresholds:
    if isinstance(weighted_volume, bool) or not isinstance(weighted_volume, (int, float)):
        raise ValueError("weighted volume is invalid")
    try:
        total = float(weighted_volume)
    except (OverflowError, TypeError, ValueError):
        raise ValueError("weighted volume is invalid") from None
    if total < 0 or not math.isfinite(total):
        raise ValueError("weighted volume is invalid")
    if isinstance(winner_count, bool) or not isinstance(winner_count, int) or winner_count <= 0:
        raise ValueError("winner count is invalid")
    average = total / winner_count
    return Thresholds(
        average=average,
        watch=average * 0.4,
        reference=average * 0.6,
        safe=average,
    )


class CompetitionVolumeProvider:
    def __init__(
        self,
        *,
        market: Any,
        official_fetcher: Callable[
            [CompetitionRule, CompetitionRound, datetime], OfficialVolumeSnapshot | None
        ]
        | None = None,
    ) -> None:
        self.market = market
        self.official_fetcher = official_fetcher

    def fetch(
        self,
        rule: CompetitionRule,
        round_: CompetitionRound,
        now: datetime,
    ) -> VolumeSnapshot:
        valid_rule = _validate_rule(rule)
        valid_round = _validate_round_window(round_)
        if valid_round not in valid_rule.rounds:
            raise ValueError("competition round does not belong to rule")
        current = _require_utc_aware(now, "now")
        if current < valid_round.start_utc:
            raise ValueError("now must not precede round start")

        if self.official_fetcher is not None:
            official = self.official_fetcher(valid_rule, valid_round, current)
            if official is not None:
                if not isinstance(official, OfficialVolumeSnapshot):
                    raise ValueError("official volume snapshot is invalid")
                try:
                    official_total = _official_weighted_volume(official.weighted_volume)
                    official_updated = _require_utc_aware(official.updated_at_utc, "official updated_at_utc")
                except ValueError:
                    raise ValueError("official volume snapshot is invalid") from None
                if not valid_round.start_utc <= official_updated <= current:
                    raise ValueError("official volume snapshot is invalid")
                return VolumeSnapshot(official_total, "official", official_updated)

        tokens = self.market.fetch_tokens()
        if not isinstance(tokens, Mapping):
            raise ValueError("Alpha token pair response is invalid")
        token = tokens.get(valid_rule.symbol)
        pair = getattr(token, "pair", None)
        if not isinstance(pair, str) or not pair.strip():
            raise ValueError(f"Alpha trading pair is missing for {valid_rule.symbol}")
        end = min(current, valid_round.end_utc)
        rows = self.market.fetch_klines(
            pair.strip(),
            interval="1h",
            limit=200,
            start_time_ms=int(valid_round.start_utc.timestamp() * 1000),
            end_time_ms=int(end.timestamp() * 1000),
        )
        return VolumeSnapshot(
            weighted_volume=weight_kline_volume(valid_round, valid_rule.multipliers, rows),
            source="alpha_kline_estimate",
            updated_at_utc=current,
        )


def _encode_rule(rule: CompetitionRule) -> dict[str, object]:
    _validate_rule(rule)
    return {
        "symbol": rule.symbol,
        "name": rule.name,
        "article_code": rule.article_code,
        "title": rule.title,
        "article_url": rule.article_url,
        "published_at_utc": _encode_datetime(rule.published_at_utc, "published_at_utc"),
        "winner_count": rule.winner_count,
        "rounds": [
            {
                "number": round_.number,
                "start_utc": _encode_datetime(round_.start_utc, "round start_utc"),
                "end_utc": _encode_datetime(round_.end_utc, "round end_utc"),
            }
            for round_ in rule.rounds
        ],
        "multipliers": list(rule.multipliers),
    }


def _cached_text(data: Mapping[str, object], key: str) -> str:
    value = data.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"cached rule {key} is invalid")
    return value


def _decode_rule(value: object) -> CompetitionRule:
    if not isinstance(value, Mapping):
        raise ValueError("cached rule is invalid")
    symbol = _normalized_symbol(value.get("symbol"))
    winner_count = value.get("winner_count")
    if isinstance(winner_count, bool) or not isinstance(winner_count, int) or winner_count <= 0:
        raise ValueError("cached rule winner_count is invalid")
    raw_rounds = value.get("rounds")
    if not isinstance(raw_rounds, list):
        raise ValueError("cached rule rounds are invalid")
    rounds: list[CompetitionRound] = []
    for raw_round in raw_rounds:
        if not isinstance(raw_round, Mapping):
            raise ValueError("cached rule round is invalid")
        number = raw_round.get("number")
        if isinstance(number, bool) or not isinstance(number, int) or number <= 0:
            raise ValueError("cached rule round number is invalid")
        rounds.append(
            CompetitionRound(
                number=number,
                start_utc=_decode_datetime(raw_round.get("start_utc"), "round start_utc"),
                end_utc=_decode_datetime(raw_round.get("end_utc"), "round end_utc"),
            )
        )
    raw_multipliers = value.get("multipliers")
    if not isinstance(raw_multipliers, list):
        raise ValueError("cached rule multipliers are invalid")
    multipliers: list[float] = []
    for raw_multiplier in raw_multipliers:
        multipliers.append(_positive_finite_multiplier(raw_multiplier))
    rule = CompetitionRule(
        symbol=symbol,
        name=_cached_text(value, "name"),
        article_code=_cached_text(value, "article_code"),
        title=_cached_text(value, "title"),
        article_url=_cached_text(value, "article_url"),
        published_at_utc=_decode_datetime(value.get("published_at_utc"), "published_at_utc"),
        winner_count=winner_count,
        rounds=tuple(rounds),
        multipliers=tuple(multipliers),
    )
    return _validate_rule(rule)


class CompetitionRuleCache:
    def __init__(self, path: Path, *, ttl: timedelta = timedelta(hours=6)) -> None:
        if ttl <= timedelta(0):
            raise ValueError("ttl must be positive")
        self.path = Path(path)
        self.ttl = ttl
        self._lock = threading.RLock()

    def _load(self) -> dict[str, _CachedRule]:
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, TypeError, ValueError, json.JSONDecodeError):
            return {}
        if not isinstance(payload, Mapping) or payload.get("version") != 1:
            return {}
        raw_rules = payload.get("rules")
        if not isinstance(raw_rules, Mapping):
            return {}
        state: dict[str, _CachedRule] = {}
        for raw_symbol, raw_entry in raw_rules.items():
            try:
                if not isinstance(raw_symbol, str) or not isinstance(raw_entry, Mapping):
                    raise ValueError("cached rule entry is invalid")
                symbol = _normalized_symbol(raw_symbol)
                rule = _decode_rule(raw_entry.get("rule"))
                if rule.symbol != symbol:
                    raise ValueError("cached rule symbol is inconsistent")
                state[symbol] = _CachedRule(
                    fetched_at=_decode_datetime(raw_entry.get("fetched_at"), "fetched_at"),
                    rule=rule,
                )
            except (AttributeError, OverflowError, TypeError, ValueError, RuleParseError):
                continue
        return state

    def _save(self, state: Mapping[str, _CachedRule]) -> None:
        payload = {
            "version": 1,
            "rules": {
                symbol: {
                    "fetched_at": _encode_datetime(entry.fetched_at, "fetched_at"),
                    "rule": _encode_rule(entry.rule),
                }
                for symbol, entry in sorted(state.items())
            },
        }
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temp_path: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                "w",
                encoding="utf-8",
                dir=self.path.parent,
                prefix=f".{self.path.name}.",
                suffix=".tmp",
                delete=False,
            ) as handle:
                temp_path = Path(handle.name)
                json.dump(payload, handle, ensure_ascii=False, separators=(",", ":"))
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temp_path, self.path)
            temp_path = None
        finally:
            if temp_path is not None:
                temp_path.unlink(missing_ok=True)

    def get(
        self,
        symbol: str,
        *,
        now: datetime,
        loader: Callable[[str], CompetitionRule],
    ) -> CachedRuleResult:
        with self._lock:
            target = _normalized_symbol(symbol)
            current = _require_utc_aware(now, "now")
            state = self._load()
            cached = state.get(target)
            if cached is not None:
                age = current - cached.fetched_at
                if timedelta(0) <= age < self.ttl:
                    return CachedRuleResult(cached.rule, False)
            try:
                rule = _validate_rule(loader(target), expected_symbol=target)
            except Exception:
                if cached is None:
                    raise
                return CachedRuleResult(cached.rule, True)
            state[target] = _CachedRule(current, rule)
            self._save(state)
            return CachedRuleResult(rule, False)

    def store(self, rule: CompetitionRule, *, fetched_at: datetime) -> None:
        with self._lock:
            valid_rule = _validate_rule(rule)
            fetched = _require_utc_aware(fetched_at, "fetched_at")
            state = self._load()
            state[valid_rule.symbol] = _CachedRule(fetched, valid_rule)
            self._save(state)


def _metrics_row(symbol: str) -> dict[str, object]:
    return {
        "symbol": symbol,
        "name": None,
        "round": None,
        "day": None,
        "roundStartUtc": None,
        "roundEndUtc": None,
        "currentMultiplier": None,
        "weightedVolume": None,
        "volumeSource": None,
        "volumeUpdatedAtUtc": None,
        "winnerCount": None,
        "averageVolume": None,
        "watchThreshold": None,
        "referenceThreshold": None,
        "safeThreshold": None,
        "articleUrl": None,
        "stale": False,
        "status": "rule_unavailable",
        "error": None,
    }


def _iso_seconds(value: datetime, name: str) -> str:
    return _require_utc_aware(value, name).isoformat(timespec="seconds")


def _validated_volume_snapshot(
    value: object,
    *,
    round_: CompetitionRound,
    current: datetime,
) -> VolumeSnapshot:
    if not isinstance(value, VolumeSnapshot):
        raise ValueError("competition volume snapshot is invalid")
    weighted_volume = _official_weighted_volume(value.weighted_volume)
    if not isinstance(value.source, str) or not value.source.strip():
        raise ValueError("competition volume snapshot is invalid")
    updated_at = _require_utc_aware(value.updated_at_utc, "volume updated_at_utc")
    if updated_at < round_.start_utc or updated_at > current:
        raise ValueError("competition volume snapshot is invalid")
    return VolumeSnapshot(weighted_volume, value.source.strip(), updated_at)


class _VolumeUnavailableError(RuntimeError):
    pass


def _normalized_service_symbol(value: object) -> str:
    if not isinstance(value, str):
        raise RuleParseError("expected symbol is missing")
    symbol = value.strip().upper()
    if re.fullmatch(r"[A-Z0-9]{1,32}", symbol) is None:
        raise RuleParseError("expected symbol is invalid")
    return symbol


class CompetitionMetricsService:
    def __init__(
        self,
        *,
        rule_provider: Any,
        rule_cache: Any,
        volume_provider: Any,
        volume_ttl: timedelta = timedelta(seconds=60),
    ) -> None:
        if not isinstance(volume_ttl, timedelta) or volume_ttl <= timedelta(0):
            raise ValueError("volume_ttl must be positive")
        self.rule_provider = rule_provider
        self.rule_cache = rule_cache
        self.volume_provider = volume_provider
        self.volume_ttl = volume_ttl
        self._volume_cache: dict[tuple[str, str, int], tuple[datetime, VolumeSnapshot]] = {}
        self._volume_failures: dict[tuple[str, str, int], tuple[datetime, str]] = {}
        self._lock = threading.RLock()

    @staticmethod
    def _error(symbol: str, kind: str) -> str:
        return f"{symbol or 'UNKNOWN'}: competition {kind} unavailable"

    def _rule_result(self, symbol: str, current: datetime) -> CachedRuleResult:
        result = self.rule_cache.get(
            symbol,
            now=current,
            loader=lambda target: self.rule_provider.fetch_rule(target, now=current),
        )
        if not isinstance(result, CachedRuleResult) or not isinstance(result.stale, bool):
            raise RuleParseError("competition rule cache result is invalid")
        return CachedRuleResult(
            _validate_rule(result.rule, expected_symbol=symbol),
            result.stale,
        )

    def _volume(
        self,
        rule: CompetitionRule,
        round_: CompetitionRound,
        current: datetime,
    ) -> tuple[VolumeSnapshot, bool, str | None]:
        key = (rule.symbol, rule.article_code, round_.number)
        for old_key in list(self._volume_cache):
            if old_key[0] == rule.symbol and old_key[1] != rule.article_code:
                del self._volume_cache[old_key]
        for old_key in list(self._volume_failures):
            if old_key[0] == rule.symbol and old_key[1] != rule.article_code:
                del self._volume_failures[old_key]

        cached = self._volume_cache.get(key)
        cached_age = current - cached[0] if cached is not None else None
        fallback = cached if cached_age is not None and cached_age >= timedelta(0) else None
        if cached is not None:
            if cached_age is not None and timedelta(0) <= cached_age < self.volume_ttl:
                return cached[1], False, None

        failure = self._volume_failures.get(key)
        if failure is not None:
            failure_age = current - failure[0]
            if timedelta(0) <= failure_age < self.volume_ttl:
                if fallback is None:
                    raise _VolumeUnavailableError(failure[1])
                return fallback[1], True, failure[1]
            del self._volume_failures[key]
        try:
            snapshot = _validated_volume_snapshot(
                self.volume_provider.fetch(rule, round_, current),
                round_=round_,
                current=current,
            )
        except Exception:
            error = self._error(rule.symbol, "volume")
            self._volume_failures[key] = (current, error)
            if fallback is None:
                raise _VolumeUnavailableError(error) from None
            return fallback[1], True, error
        self._volume_cache[key] = (current, snapshot)
        self._volume_failures.pop(key, None)
        return snapshot, False, None

    def _build_row(
        self,
        cached_rule: CachedRuleResult,
        current: datetime,
    ) -> dict[str, object]:
        rule = cached_rule.rule
        selection = select_round(rule, current)
        row = _metrics_row(rule.symbol)
        row.update(
            {
                "name": rule.name,
                "winnerCount": rule.winner_count,
                "articleUrl": rule.article_url,
                "stale": cached_rule.stale,
                "status": selection.status,
            }
        )
        if selection.status != "active" or selection.round is None:
            return row

        round_ = selection.round
        row.update(
            {
                "round": round_.number,
                "day": selection.day,
                "roundStartUtc": _iso_seconds(round_.start_utc, "round start_utc"),
                "roundEndUtc": _iso_seconds(round_.end_utc, "round end_utc"),
                "currentMultiplier": selection.multiplier,
            }
        )
        try:
            snapshot, volume_stale, error = self._volume(rule, round_, current)
        except _VolumeUnavailableError:
            row["status"] = "volume_unavailable"
            row["error"] = self._error(rule.symbol, "volume")
            return row

        thresholds = calculate_thresholds(
            weighted_volume=snapshot.weighted_volume,
            winner_count=rule.winner_count,
        )
        row.update(
            {
                "weightedVolume": snapshot.weighted_volume,
                "volumeSource": snapshot.source,
                "volumeUpdatedAtUtc": _iso_seconds(snapshot.updated_at_utc, "volume updated_at_utc"),
                "averageVolume": thresholds.average,
                "watchThreshold": thresholds.watch,
                "referenceThreshold": thresholds.reference,
                "safeThreshold": thresholds.safe,
                "stale": cached_rule.stale or volume_stale,
                "error": error,
            }
        )
        return row

    def active_recent_symbols(self, *, now: datetime | None = None) -> list[str]:
        current = datetime.now(_UTC) if now is None else _require_utc_aware(now, "now")
        fetch_recent = getattr(self.rule_provider, "fetch_recent_symbols", None)
        if not callable(fetch_recent):
            return []
        active: list[str] = []
        with self._lock:
            for raw_symbol in fetch_recent(now=current):
                try:
                    symbol = _normalized_service_symbol(raw_symbol)
                    cached_rule = self._rule_result(symbol, current)
                    if select_round(cached_rule.rule, current).status == "active":
                        active.append(symbol)
                except Exception:
                    continue
        return active

    def collect(self, symbols: list[str], *, now: datetime | None = None) -> dict[str, Any]:
        current = datetime.now(_UTC) if now is None else _require_utc_aware(now, "now")
        rows: list[dict[str, object]] = []
        errors: list[str] = []
        with self._lock:
            for raw_symbol in symbols:
                try:
                    symbol = _normalized_service_symbol(raw_symbol)
                except RuleParseError:
                    symbol = ""
                    error = self._error(symbol, "rule")
                    row = _metrics_row(symbol)
                    row["error"] = error
                    rows.append(row)
                    errors.append(error)
                    continue
                try:
                    cached_rule = self._rule_result(symbol, current)
                except Exception:
                    error = self._error(symbol, "rule")
                    row = _metrics_row(symbol)
                    row["error"] = error
                else:
                    row = self._build_row(cached_rule, current)
                rows.append(row)
                if isinstance(row["error"], str):
                    errors.append(row["error"])
        return {
            "generatedAtUtc": current.isoformat(timespec="seconds"),
            "rows": rows,
            "errors": errors,
        }
