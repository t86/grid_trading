from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import html
import json
import math
import re
from typing import Any


_UTC = timezone.utc
_TITLE_PREFIX = "Binance Alpha Trading Competition: Trade "
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


class RuleParseError(ValueError):
    """The official competition article did not contain a usable rule set."""


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


def _collect_blocks(node: object) -> list[str]:
    if isinstance(node, list):
        return [block for child in node for block in _collect_blocks(child)]
    if not isinstance(node, Mapping):
        return []
    if node.get("node") == "text" and isinstance(node.get("text"), str):
        block = _normalize_text(node["text"])
        return [block] if block else []
    if node.get("node") in _BLOCK_NODES:
        block = _normalize_text(" ".join(_text_descendants(node)))
        return [block] if block else []
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


def _body_blocks(data: Mapping[str, Any]) -> tuple[str, ...]:
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


def _parse_rounds(blocks: tuple[str, ...], expected_symbol: str) -> tuple[CompetitionRound, ...]:
    first_day = next((index for index, block in enumerate(blocks) if _DAY_ROW_START_RE.match(block)), len(blocks))
    rounds_by_number: dict[int, CompetitionRound] = {}
    for block in blocks[:first_day]:
        for match in _PERIOD_RE.finditer(block):
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


def _parse_winner_count(blocks: tuple[str, ...]) -> int:
    match = _WINNER_RE.search(" ".join(blocks))
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


def _parse_multipliers(blocks: tuple[str, ...]) -> tuple[float, ...]:
    multipliers: dict[int, float] = {}
    for block in blocks:
        if _DAY_ROW_START_RE.match(block) is None:
            continue
        markers = list(_DAY_MARKER_RE.finditer(block))
        for index, marker in enumerate(markers):
            day = int(marker.group("day"))
            if day in multipliers:
                raise RuleParseError("Day multiplier is duplicated")
            end = markers[index + 1].start() if index + 1 < len(markers) else len(block)
            rising_trader = _RISING_TRADER_RE.search(block, marker.end(), end)
            if rising_trader is not None:
                end = rising_trader.start()
            multiplier_match = _MULTIPLIER_RE.search(block, marker.end(), end)
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
