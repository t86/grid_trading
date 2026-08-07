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


@dataclass(frozen=True)
class CachedRuleResult:
    rule: CompetitionRule
    stale: bool


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
            raw_articles = data.get("articles")
            if not isinstance(raw_articles, list):
                raise RuleParseError("Binance CMS article list is invalid")
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
        if numbers != list(range(1, len(numbers) + 1)):
            raise ValueError
        if len(rule.multipliers) != 7:
            raise ValueError
        for multiplier in rule.multipliers:
            _positive_finite_multiplier(multiplier)
    except (AttributeError, OverflowError, TypeError, ValueError, RuleParseError):
        raise RuleParseError("competition rule is invalid") from None
    return rule


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
