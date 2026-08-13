from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
import json
import logging
import os
from pathlib import Path
import re
import tempfile
import threading
from typing import Any

from grid_optimizer.alpha_competition_metrics import (
    CompetitionAnnouncement,
    CompetitionRule,
    decode_competition_rule,
    encode_competition_rule,
)


_UTC = timezone.utc
_DISCOVERY_ERROR = "competition announcement discovery unavailable"
_CACHE_ERROR = "competition discovery cache unavailable"
_REFRESH_BUDGET_ERROR = "competition announcement refresh budget exhausted"
_MAX_RULE_REQUESTS_PER_REFRESH = 8
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DiscoveredCompetition:
    announcement: CompetitionAnnouncement
    rule: CompetitionRule
    validated_at_utc: datetime


@dataclass(frozen=True)
class DiscoverySnapshot:
    discovered_at_utc: datetime | None
    competitions: tuple[DiscoveredCompetition, ...]
    stale: bool
    errors: tuple[str, ...]

    @property
    def rules(self) -> tuple[CompetitionRule, ...]:
        return tuple(item.rule for item in self.competitions)


def _empty_snapshot() -> DiscoverySnapshot:
    return DiscoverySnapshot(None, (), True, ())


def _require_utc(value: object, name: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError(f"{name} must be UTC-aware")
    return value.astimezone(_UTC)


def _decode_datetime(value: object, name: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError(f"{name} is invalid")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        raise ValueError(f"{name} is invalid") from None
    return _require_utc(parsed, name)


def _require_text(value: object, name: str) -> str:
    if not isinstance(value, str) or not value.strip() or value != value.strip():
        raise ValueError(f"{name} is invalid")
    return value


def _validate_announcement(value: object) -> CompetitionAnnouncement:
    if not isinstance(value, CompetitionAnnouncement):
        raise TypeError("competition announcement is invalid")
    symbol = _require_text(value.symbol, "announcement symbol")
    if symbol != symbol.upper() or re.fullmatch(r"[A-Z0-9_]{1,32}", symbol) is None:
        raise ValueError("announcement symbol is invalid")
    _require_text(value.article_code, "announcement article code")
    _require_text(value.title, "announcement title")
    _require_utc(value.released_at_utc, "announcement released_at_utc")
    return value


def _validate_discovered(value: object) -> DiscoveredCompetition:
    if not isinstance(value, DiscoveredCompetition):
        raise TypeError("discovered competition is invalid")
    announcement = _validate_announcement(value.announcement)
    rule = decode_competition_rule(encode_competition_rule(value.rule))
    _require_utc(value.validated_at_utc, "validated_at_utc")
    if (
        announcement.article_code != rule.article_code
        or announcement.symbol != rule.symbol
        or announcement.title != rule.title
    ):
        raise ValueError("competition announcement and rule identities differ")
    return value


def _validate_snapshot(value: object) -> DiscoverySnapshot:
    if not isinstance(value, DiscoverySnapshot):
        raise TypeError("discovery snapshot is invalid")
    if value.discovered_at_utc is not None:
        _require_utc(value.discovered_at_utc, "discovered_at_utc")
    if not isinstance(value.competitions, tuple):
        raise TypeError("discovery competitions are invalid")
    seen: set[str] = set()
    for competition in value.competitions:
        valid = _validate_discovered(competition)
        if valid.announcement.article_code in seen:
            raise ValueError("competition article code is duplicated")
        seen.add(valid.announcement.article_code)
    if not isinstance(value.stale, bool):
        raise TypeError("discovery stale flag is invalid")
    if not isinstance(value.errors, tuple) or any(
        not isinstance(error, str) or not error for error in value.errors
    ):
        raise TypeError("discovery errors are invalid")
    return value


def _encode_announcement(value: CompetitionAnnouncement) -> dict[str, str]:
    announcement = _validate_announcement(value)
    return {
        "symbol": announcement.symbol,
        "article_code": announcement.article_code,
        "title": announcement.title,
        "released_at_utc": announcement.released_at_utc.isoformat(),
    }


def _decode_announcement(value: object) -> CompetitionAnnouncement:
    if not isinstance(value, Mapping):
        raise ValueError("cached announcement is invalid")
    return _validate_announcement(
        CompetitionAnnouncement(
            symbol=_require_text(value.get("symbol"), "announcement symbol"),
            article_code=_require_text(value.get("article_code"), "announcement article code"),
            title=_require_text(value.get("title"), "announcement title"),
            released_at_utc=_decode_datetime(
                value.get("released_at_utc"), "announcement released_at_utc"
            ),
        )
    )


class CompetitionDiscoveryCache:
    VERSION = 1

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self._lock = threading.RLock()

    def load(self) -> DiscoverySnapshot:
        with self._lock:
            try:
                payload = json.loads(self.path.read_text(encoding="utf-8"))
                return self._decode_payload(payload)
            except (AttributeError, OSError, TypeError, ValueError, json.JSONDecodeError):
                return _empty_snapshot()

    def _decode_payload(self, payload: object) -> DiscoverySnapshot:
        if (
            not isinstance(payload, Mapping)
            or isinstance(payload.get("version"), bool)
            or payload.get("version") != self.VERSION
        ):
            raise ValueError("discovery cache root is invalid")
        raw_discovered_at = payload.get("discovered_at_utc")
        discovered_at = (
            None
            if raw_discovered_at is None
            else _decode_datetime(raw_discovered_at, "discovered_at_utc")
        )
        raw_entries = payload.get("rules_by_article_code")
        if not isinstance(raw_entries, Mapping):
            raise ValueError("discovery cache entries are invalid")
        competitions: list[DiscoveredCompetition] = []
        for raw_code, raw_entry in raw_entries.items():
            try:
                code = _require_text(raw_code, "cached article code")
                if not isinstance(raw_entry, Mapping):
                    raise ValueError("cached competition entry is invalid")
                competition = _validate_discovered(
                    DiscoveredCompetition(
                        announcement=_decode_announcement(raw_entry.get("announcement")),
                        rule=decode_competition_rule(raw_entry.get("rule")),
                        validated_at_utc=_decode_datetime(
                            raw_entry.get("validated_at_utc"), "validated_at_utc"
                        ),
                    )
                )
                if competition.announcement.article_code != code:
                    raise ValueError("cached article code is inconsistent")
                competitions.append(competition)
            except (AttributeError, OverflowError, TypeError, ValueError):
                continue
        return DiscoverySnapshot(discovered_at, tuple(competitions), False, ())

    def store(self, snapshot: DiscoverySnapshot) -> None:
        with self._lock:
            valid = _validate_snapshot(snapshot)
            payload = {
                "version": self.VERSION,
                "discovered_at_utc": (
                    None
                    if valid.discovered_at_utc is None
                    else valid.discovered_at_utc.isoformat()
                ),
                "rules_by_article_code": {
                    item.announcement.article_code: {
                        "announcement": _encode_announcement(item.announcement),
                        "rule": encode_competition_rule(item.rule),
                        "validated_at_utc": item.validated_at_utc.isoformat(),
                    }
                    for item in valid.competitions
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


class CompetitionDiscoveryService:
    def __init__(
        self,
        provider: Any,
        cache: CompetitionDiscoveryCache,
        *,
        ttl: timedelta = timedelta(minutes=5),
        rule_ttl: timedelta = timedelta(hours=6),
    ) -> None:
        if not isinstance(ttl, timedelta) or ttl <= timedelta(0):
            raise ValueError("ttl must be a positive timedelta")
        if not isinstance(rule_ttl, timedelta) or rule_ttl <= timedelta(0):
            raise ValueError("rule_ttl must be a positive timedelta")
        if not isinstance(cache, CompetitionDiscoveryCache):
            raise TypeError("cache must be a CompetitionDiscoveryCache")
        self.provider = provider
        self.cache = cache
        self.ttl = ttl
        self.rule_ttl = rule_ttl
        self._refresh_lock = threading.Lock()
        self._memory = cache.load()
        self._last_attempt_utc: datetime | None = None
        self._detail_cursor_article_code: str | None = None

    @staticmethod
    def _age_is_fresh(timestamp: datetime, current: datetime, ttl: timedelta) -> bool:
        age = current - timestamp
        return timedelta(0) <= age < ttl

    def _is_fresh(self, snapshot: DiscoverySnapshot, current: datetime) -> bool:
        return (
            not snapshot.stale
            and snapshot.discovered_at_utc is not None
            and self._age_is_fresh(snapshot.discovered_at_utc, current, self.ttl)
        )

    def _retry_is_cooled_down(self, current: datetime) -> bool:
        return (
            self._last_attempt_utc is not None
            and self._age_is_fresh(self._last_attempt_utc, current, self.ttl)
        )

    @staticmethod
    def _is_ended(competition: DiscoveredCompetition, current: datetime) -> bool:
        return max(round_.end_utc for round_ in competition.rule.rounds) <= current

    def _view(self, snapshot: DiscoverySnapshot, current: datetime) -> DiscoverySnapshot:
        visible = [
            competition
            for competition in snapshot.competitions
            if not self._is_ended(competition, current)
        ]
        winners: dict[str, DiscoveredCompetition] = {}
        for competition in visible:
            existing = winners.get(competition.rule.symbol)
            if existing is None or competition.rule.published_at_utc > existing.rule.published_at_utc:
                winners[competition.rule.symbol] = competition
        selected = tuple(
            competition
            for competition in visible
            if winners.get(competition.rule.symbol) is competition
        )
        return replace(snapshot, competitions=selected)

    def discover(self, *, now: datetime) -> DiscoverySnapshot:
        current = _require_utc(now, "now")
        snapshot = self._memory
        if self._is_fresh(snapshot, current):
            return self._view(snapshot, current)
        if self._retry_is_cooled_down(current):
            return self._view(snapshot, current)
        if snapshot.competitions:
            if not self._refresh_lock.acquire(blocking=False):
                return replace(self._view(snapshot, current), stale=True)
            try:
                if self._retry_is_cooled_down(current):
                    return self._view(self._memory, current)
                if self._memory is not snapshot:
                    return self._view(self._memory, current)
                return self._attempt_refresh(snapshot, current)
            finally:
                self._refresh_lock.release()
        with self._refresh_lock:
            if self._retry_is_cooled_down(current):
                return self._view(self._memory, current)
            if self._memory is not snapshot:
                return self._view(self._memory, current)
            return self._attempt_refresh(snapshot, current)

    def _attempt_refresh(
        self,
        previous: DiscoverySnapshot,
        current: datetime,
    ) -> DiscoverySnapshot:
        try:
            return self._refresh(previous, current)
        finally:
            self._last_attempt_utc = current

    def _refresh(self, previous: DiscoverySnapshot, current: datetime) -> DiscoverySnapshot:
        try:
            raw_announcements = self.provider.fetch_recent_announcements(now=current)
            announcements = self._announcements(raw_announcements)
        except Exception:
            logger.warning(_DISCOVERY_ERROR)
            failed = replace(
                previous,
                stale=True,
                errors=(_DISCOVERY_ERROR,),
            )
            self._memory = failed
            return self._view(failed, current)

        cached_by_code = {
            item.announcement.article_code: item for item in previous.competitions
        }
        refreshed_by_code: dict[str, DiscoveredCompetition] = {}
        errors: list[str] = []
        seen_codes: set[str] = set()
        detail_candidates: list[CompetitionAnnouncement] = []
        for announcement in announcements:
            code = announcement.article_code
            if code in seen_codes:
                continue
            seen_codes.add(code)
            cached = cached_by_code.get(code)
            if self._can_reuse(cached, announcement, current):
                refreshed_by_code[code] = cached
                continue
            detail_candidates.append(announcement)

        candidate_codes = [item.article_code for item in detail_candidates]
        try:
            start = candidate_codes.index(self._detail_cursor_article_code)
        except ValueError:
            start = 0
        rotated = detail_candidates[start:] + detail_candidates[:start]
        attempted = rotated[:_MAX_RULE_REQUESTS_PER_REFRESH]
        deferred = rotated[_MAX_RULE_REQUESTS_PER_REFRESH:]
        self._detail_cursor_article_code = (
            deferred[0].article_code if deferred else None
        )

        for announcement in attempted:
            code = announcement.article_code
            cached = cached_by_code.get(code)
            try:
                rule = self.provider.fetch_announcement_rule(announcement, now=current)
                discovered = _validate_discovered(
                    DiscoveredCompetition(announcement, rule, current)
                )
            except Exception:
                error = f"{announcement.symbol}: competition announcement rule unavailable"
                logger.warning(error)
                errors.append(error)
                if cached is not None and not self._is_ended(cached, current):
                    refreshed_by_code[code] = cached
                continue
            if not self._is_ended(discovered, current):
                refreshed_by_code[code] = discovered

        if deferred:
            logger.warning(_REFRESH_BUDGET_ERROR)
            errors.append(_REFRESH_BUDGET_ERROR)
            for announcement in deferred:
                cached = cached_by_code.get(announcement.article_code)
                if cached is not None and not self._is_ended(cached, current):
                    refreshed_by_code[announcement.article_code] = cached

        refreshed = [
            refreshed_by_code[item.article_code]
            for item in announcements
            if item.article_code in refreshed_by_code
        ]

        extras = sorted(
            (
                item
                for code, item in cached_by_code.items()
                if code not in seen_codes and not self._is_ended(item, current)
            ),
            key=lambda item: item.announcement.article_code,
        )
        refreshed.extend(extras)
        state = DiscoverySnapshot(
            previous.discovered_at_utc if errors else current,
            tuple(refreshed),
            bool(errors),
            tuple(errors),
        )
        if not errors:
            try:
                self.cache.store(state)
            except OSError:
                logger.warning(_CACHE_ERROR)
                state = replace(state, stale=True, errors=(_CACHE_ERROR,))
        self._memory = state
        return self._view(state, current)

    @staticmethod
    def _announcements(values: object) -> tuple[CompetitionAnnouncement, ...]:
        if not isinstance(values, (list, tuple)):
            raise TypeError("competition announcements are invalid")
        result: list[CompetitionAnnouncement] = []
        metadata_by_code: dict[str, CompetitionAnnouncement] = {}
        for raw in values:
            announcement = _validate_announcement(raw)
            existing = metadata_by_code.get(announcement.article_code)
            if existing is not None:
                if existing != announcement:
                    raise ValueError("competition announcement metadata conflict")
                continue
            metadata_by_code[announcement.article_code] = announcement
            result.append(announcement)
        return tuple(result)

    def _can_reuse(
        self,
        cached: DiscoveredCompetition | None,
        announcement: CompetitionAnnouncement,
        current: datetime,
    ) -> bool:
        return (
            cached is not None
            and cached.announcement == announcement
            and self._age_is_fresh(cached.validated_at_utc, current, self.rule_ttl)
        )
