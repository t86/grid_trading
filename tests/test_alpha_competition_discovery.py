from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
import json
import logging
from pathlib import Path
import threading

import pytest

from grid_optimizer import alpha_competition_discovery as discovery
from grid_optimizer.alpha_competition_metrics import (
    CompetitionAnnouncement,
    CompetitionRound,
    CompetitionRule,
    decode_competition_rule,
    encode_competition_rule,
)


UTC = timezone.utc
NOW = datetime(2026, 8, 13, 2, tzinfo=UTC)


def _announcement(
    symbol: str,
    *,
    code: str | None = None,
    released_at: datetime = NOW - timedelta(days=1),
) -> CompetitionAnnouncement:
    article_code = code or f"{symbol.lower()}-article"
    return CompetitionAnnouncement(
        symbol=symbol,
        article_code=article_code,
        title=f"Binance Alpha Trading Competition: Trade Project ({symbol}) and Win",
        released_at_utc=released_at,
    )


def _rule(
    announcement: CompetitionAnnouncement,
    *,
    first_start: datetime,
    round_count: int = 1,
    gap: timedelta = timedelta(0),
    published_at: datetime | None = None,
) -> CompetitionRule:
    rounds = tuple(
        CompetitionRound(
            number=index + 1,
            start_utc=first_start + index * (timedelta(days=7) + gap),
            end_utc=first_start + index * (timedelta(days=7) + gap) + timedelta(days=7),
        )
        for index in range(round_count)
    )
    return CompetitionRule(
        symbol=announcement.symbol,
        name="Project",
        article_code=announcement.article_code,
        title=announcement.title,
        article_url=(
            "https://www.binance.com/en/support/announcement/detail/"
            f"{announcement.article_code}"
        ),
        published_at_utc=published_at or announcement.released_at_utc,
        winner_count=1_000,
        rounds=rounds,
        multipliers=(3.5, 3.0, 2.5, 2.0, 1.8, 1.3, 1.0),
    )


class _Provider:
    def __init__(
        self,
        announcements: list[CompetitionAnnouncement],
        rules: dict[str, CompetitionRule],
    ) -> None:
        self.announcements = announcements
        self.rules = rules
        self.list_calls = 0
        self.detail_calls: list[str] = []
        self.detail_nows: list[datetime] = []

    def fetch_recent_announcements(self, *, now: datetime) -> list[CompetitionAnnouncement]:
        self.list_calls += 1
        return list(self.announcements)

    def fetch_announcement_rule(
        self,
        announcement: CompetitionAnnouncement,
        *,
        now: datetime,
    ) -> CompetitionRule:
        self.detail_calls.append(announcement.article_code)
        self.detail_nows.append(now)
        return self.rules[announcement.article_code]


def _service(
    tmp_path: Path,
    announcements: list[CompetitionAnnouncement],
    rules: dict[str, CompetitionRule],
    *,
    ttl: timedelta = timedelta(minutes=5),
    rule_ttl: timedelta = timedelta(hours=6),
) -> tuple[discovery.CompetitionDiscoveryService, _Provider]:
    provider = _Provider(announcements, rules)
    service = discovery.CompetitionDiscoveryService(
        provider=provider,
        cache=discovery.CompetitionDiscoveryCache(tmp_path / "discovery.json"),
        ttl=ttl,
        rule_ttl=rule_ttl,
    )
    return service, provider


def _discovered(
    announcement: CompetitionAnnouncement,
    rule: CompetitionRule,
    *,
    validated_at: datetime = NOW,
) -> discovery.DiscoveredCompetition:
    return discovery.DiscoveredCompetition(announcement, rule, validated_at)


def _snapshot(
    *competitions: discovery.DiscoveredCompetition,
    discovered_at: datetime | None = NOW,
    stale: bool = False,
    errors: tuple[str, ...] = (),
) -> discovery.DiscoverySnapshot:
    return discovery.DiscoverySnapshot(discovered_at, competitions, stale, errors)


def test_discovery_shows_upcoming_and_active_but_removes_ended(tmp_path: Path) -> None:
    dos = _announcement("DOS")
    power = _announcement("POWER")
    prl = _announcement("PRL")
    rules = {
        dos.article_code: _rule(dos, first_start=NOW + timedelta(hours=1)),
        power.article_code: _rule(power, first_start=NOW - timedelta(days=1)),
        prl.article_code: _rule(prl, first_start=NOW - timedelta(days=8)),
    }
    service, _provider = _service(tmp_path, [dos, power, prl], rules)

    snapshot = service.discover(now=NOW)

    assert [rule.symbol for rule in snapshot.rules] == ["DOS", "POWER"]
    assert snapshot.stale is False
    assert snapshot.errors == ()
    assert _provider.detail_nows == [NOW, NOW, NOW]
    payload = json.loads((tmp_path / "discovery.json").read_text(encoding="utf-8"))
    assert list(payload["rules_by_article_code"]) == [dos.article_code, power.article_code]


@pytest.mark.parametrize(
    ("offset", "expected"),
    [
        (timedelta(microseconds=-1), ["DOS"]),
        (timedelta(0), ["DOS"]),
        (timedelta(days=7, microseconds=-1), ["DOS"]),
        (timedelta(days=7), []),
    ],
)
def test_discovery_uses_half_open_start_and_end_boundaries(
    tmp_path: Path,
    offset: timedelta,
    expected: list[str],
) -> None:
    dos = _announcement("DOS")
    start = NOW + timedelta(hours=1)
    rule = _rule(dos, first_start=start)
    service, _provider = _service(tmp_path, [dos], {dos.article_code: rule})

    assert [item.symbol for item in service.discover(now=start + offset).rules] == expected


def test_between_rounds_competition_remains_visible(tmp_path: Path) -> None:
    dos = _announcement("DOS")
    rule = _rule(
        dos,
        first_start=NOW - timedelta(days=8),
        round_count=2,
        gap=timedelta(days=2),
    )
    service, _provider = _service(tmp_path, [dos], {dos.article_code: rule})

    assert [item.symbol for item in service.discover(now=NOW).rules] == ["DOS"]


def test_same_symbol_uses_rule_with_latest_published_time(tmp_path: Path) -> None:
    old = _announcement("DOS", code="old", released_at=NOW - timedelta(days=2))
    new = _announcement("DOS", code="new", released_at=NOW - timedelta(days=1))
    rules = {
        old.article_code: _rule(
            old,
            first_start=NOW - timedelta(days=1),
            published_at=NOW - timedelta(days=2),
        ),
        new.article_code: _rule(
            new,
            first_start=NOW - timedelta(days=1),
            published_at=NOW - timedelta(days=1),
        ),
    }
    service, _provider = _service(tmp_path, [old, new], rules)

    assert [item.announcement.article_code for item in service.discover(now=NOW).competitions] == [
        "new"
    ]


def test_scanned_order_is_preserved_after_code_and_symbol_deduplication(tmp_path: Path) -> None:
    power = _announcement("POWER")
    dos = _announcement("DOS", code="dos-a")
    dos_duplicate = replace(dos)
    dos_tie = _announcement("DOS", code="dos-b")
    published = NOW - timedelta(days=1)
    rules = {
        power.article_code: _rule(power, first_start=NOW - timedelta(days=1)),
        dos.article_code: _rule(dos, first_start=NOW - timedelta(days=1), published_at=published),
        dos_tie.article_code: _rule(
            dos_tie,
            first_start=NOW - timedelta(days=1),
            published_at=published,
        ),
    }
    service, provider = _service(tmp_path, [power, dos, dos_duplicate, dos_tie], rules)

    result = service.discover(now=NOW)

    assert [item.announcement.article_code for item in result.competitions] == ["power-article", "dos-a"]
    assert provider.detail_calls == ["power-article", "dos-a", "dos-b"]


def test_cache_json_is_keyed_by_article_code_and_rule_codec_is_public(tmp_path: Path) -> None:
    dos = _announcement("DOS")
    rule = _rule(dos, first_start=NOW + timedelta(hours=1))
    cache_path = tmp_path / "nested" / "discovery.json"
    cache = discovery.CompetitionDiscoveryCache(cache_path)

    cache.store(_snapshot(_discovered(dos, rule)))

    payload = json.loads(cache_path.read_text(encoding="utf-8"))
    assert payload["version"] == 1
    assert list(payload["rules_by_article_code"]) == [dos.article_code]
    assert payload["rules_by_article_code"][dos.article_code]["announcement"]["symbol"] == "DOS"
    assert decode_competition_rule(encode_competition_rule(rule)) == rule


@pytest.mark.parametrize(
    "payload",
    [
        "not json",
        json.dumps([]),
        json.dumps({"version": 2, "discovered_at_utc": None, "rules_by_article_code": {}}),
        json.dumps({"version": 1, "discovered_at_utc": "2026-08-13T02:00:00", "rules_by_article_code": {}}),
    ],
)
def test_corrupt_or_incompatible_cache_root_loads_as_empty(tmp_path: Path, payload: str) -> None:
    path = tmp_path / "discovery.json"
    path.write_text(payload, encoding="utf-8")

    snapshot = discovery.CompetitionDiscoveryCache(path).load()

    assert snapshot.discovered_at_utc is None
    assert snapshot.competitions == ()
    assert snapshot.stale is True
    assert snapshot.errors == ()


def test_one_corrupt_cache_entry_does_not_hide_healthy_entry(tmp_path: Path) -> None:
    dos = _announcement("DOS")
    power = _announcement("POWER")
    cache_path = tmp_path / "discovery.json"
    cache = discovery.CompetitionDiscoveryCache(cache_path)
    cache.store(
        _snapshot(
            _discovered(dos, _rule(dos, first_start=NOW - timedelta(days=1))),
            _discovered(power, _rule(power, first_start=NOW - timedelta(days=1))),
        )
    )
    payload = json.loads(cache_path.read_text(encoding="utf-8"))
    payload["rules_by_article_code"][dos.article_code]["announcement"]["symbol"] = "OTHER"
    cache_path.write_text(json.dumps(payload), encoding="utf-8")

    loaded = cache.load()

    assert [item.rule.symbol for item in loaded.competitions] == ["POWER"]


def test_cache_rejects_entry_when_key_or_announcement_identity_disagrees_with_rule(
    tmp_path: Path,
) -> None:
    dos = _announcement("DOS")
    cache_path = tmp_path / "discovery.json"
    cache = discovery.CompetitionDiscoveryCache(cache_path)
    cache.store(_snapshot(_discovered(dos, _rule(dos, first_start=NOW))))
    payload = json.loads(cache_path.read_text(encoding="utf-8"))
    entry = payload["rules_by_article_code"].pop(dos.article_code)
    entry["announcement"]["title"] = entry["announcement"]["title"].replace("DOS", "OTHER")
    payload["rules_by_article_code"]["wrong-code"] = entry
    cache_path.write_text(json.dumps(payload), encoding="utf-8")

    assert cache.load().competitions == ()


def test_service_recovers_fresh_snapshot_after_restart_without_provider_call(tmp_path: Path) -> None:
    dos = _announcement("DOS")
    rule = _rule(dos, first_start=NOW - timedelta(days=1))
    cache = discovery.CompetitionDiscoveryCache(tmp_path / "discovery.json")
    cache.store(_snapshot(_discovered(dos, rule)))
    provider = _Provider([], {})
    restarted = discovery.CompetitionDiscoveryService(provider=provider, cache=cache)

    result = restarted.discover(now=NOW + timedelta(minutes=1))

    assert result.rules == (rule,)
    assert provider.list_calls == 0


def test_cache_atomic_replace_failure_keeps_old_file_and_cleans_temp(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dos = _announcement("DOS")
    power = _announcement("POWER")
    cache_path = tmp_path / "discovery.json"
    cache = discovery.CompetitionDiscoveryCache(cache_path)
    cache.store(_snapshot(_discovered(dos, _rule(dos, first_start=NOW))))
    original = cache_path.read_bytes()

    def fail_replace(_source: object, _target: object) -> None:
        raise OSError("disk unavailable")

    monkeypatch.setattr(discovery.os, "replace", fail_replace)

    with pytest.raises(OSError, match="disk unavailable"):
        cache.store(_snapshot(_discovered(power, _rule(power, first_start=NOW))))

    assert cache_path.read_bytes() == original
    assert list(tmp_path.glob(".discovery.json.*.tmp")) == []


def test_future_discovery_and_validation_timestamps_are_not_fresh(tmp_path: Path) -> None:
    dos = _announcement("DOS")
    cached_rule = _rule(dos, first_start=NOW - timedelta(days=1))
    cache = discovery.CompetitionDiscoveryCache(tmp_path / "discovery.json")
    cache.store(
        _snapshot(
            _discovered(dos, cached_rule, validated_at=NOW + timedelta(minutes=1)),
            discovered_at=NOW + timedelta(minutes=1),
        )
    )
    replacement = replace(cached_rule, winner_count=2_000)
    provider = _Provider([dos], {dos.article_code: replacement})
    service = discovery.CompetitionDiscoveryService(provider=provider, cache=cache)

    result = service.discover(now=NOW)

    assert result.rules == (replacement,)
    assert provider.list_calls == 1
    assert provider.detail_calls == [dos.article_code]


@pytest.mark.parametrize("ttl", [timedelta(0), timedelta(seconds=-1), 1, None])
def test_service_rejects_invalid_ttls(tmp_path: Path, ttl: object) -> None:
    provider = _Provider([], {})
    cache = discovery.CompetitionDiscoveryCache(tmp_path / "discovery.json")

    with pytest.raises((TypeError, ValueError)):
        discovery.CompetitionDiscoveryService(provider=provider, cache=cache, ttl=ttl)  # type: ignore[arg-type]

    with pytest.raises((TypeError, ValueError)):
        discovery.CompetitionDiscoveryService(provider=provider, cache=cache, rule_ttl=ttl)  # type: ignore[arg-type]


def test_discover_rejects_naive_non_utc_and_non_datetime_values(tmp_path: Path) -> None:
    service, _provider = _service(tmp_path, [], {})

    for invalid in (
        datetime(2026, 8, 13, 2),
        datetime(2026, 8, 13, 2, tzinfo=timezone(timedelta(hours=8))),
        "2026-08-13T02:00:00+00:00",
    ):
        with pytest.raises((TypeError, ValueError), match="UTC-aware"):
            service.discover(now=invalid)  # type: ignore[arg-type]


def test_cache_store_validates_snapshot_and_duplicate_article_codes(tmp_path: Path) -> None:
    dos = _announcement("DOS")
    other_dos = _announcement("DOS", code=dos.article_code)
    rule = _rule(dos, first_start=NOW)
    cache = discovery.CompetitionDiscoveryCache(tmp_path / "discovery.json")

    with pytest.raises((TypeError, ValueError)):
        cache.store("not a snapshot")  # type: ignore[arg-type]
    with pytest.raises((TypeError, ValueError)):
        cache.store(_snapshot(_discovered(dos, rule), _discovered(other_dos, rule)))
    with pytest.raises((TypeError, ValueError)):
        cache.store(replace(_snapshot(_discovered(dos, rule)), errors=(1,)))  # type: ignore[arg-type]


class _FailingListProvider:
    def __init__(self) -> None:
        self.list_calls = 0

    def fetch_recent_announcements(self, *, now: datetime) -> list[CompetitionAnnouncement]:
        self.list_calls += 1
        raise RuntimeError("secret upstream failure")


def test_list_failure_uses_last_known_good_and_marks_stale(tmp_path: Path) -> None:
    dos = _announcement("DOS")
    power = _announcement("POWER")
    cache = discovery.CompetitionDiscoveryCache(tmp_path / "discovery.json")
    cache.store(
        _snapshot(
            _discovered(dos, _rule(dos, first_start=NOW + timedelta(hours=1))),
            _discovered(power, _rule(power, first_start=NOW - timedelta(days=1))),
            discovered_at=NOW,
        )
    )
    service = discovery.CompetitionDiscoveryService(
        provider=_FailingListProvider(),
        cache=cache,
    )

    result = service.discover(now=NOW + timedelta(minutes=6))

    assert [rule.symbol for rule in result.rules] == ["DOS", "POWER"]
    assert result.stale is True
    assert result.errors == ("competition announcement discovery unavailable",)


def test_list_failure_logs_only_the_fixed_redacted_warning(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    service = discovery.CompetitionDiscoveryService(
        provider=_FailingListProvider(),
        cache=discovery.CompetitionDiscoveryCache(tmp_path / "discovery.json"),
    )

    with caplog.at_level(logging.WARNING, logger=discovery.__name__):
        result = service.discover(now=NOW)

    assert result.errors == ("competition announcement discovery unavailable",)
    assert [record.getMessage() for record in caplog.records] == [
        "competition announcement discovery unavailable"
    ]
    logs = caplog.text
    assert "secret" not in logs
    assert "private" not in logs
    assert "/" not in logs


def test_cached_ended_rule_is_removed_even_when_list_request_fails(tmp_path: Path) -> None:
    prl = _announcement("PRL")
    cache = discovery.CompetitionDiscoveryCache(tmp_path / "discovery.json")
    cache.store(
        _snapshot(
            _discovered(prl, _rule(prl, first_start=NOW - timedelta(days=7))),
            discovered_at=NOW - timedelta(minutes=6),
        )
    )
    service = discovery.CompetitionDiscoveryService(
        provider=_FailingListProvider(),
        cache=cache,
    )

    result = service.discover(now=NOW)

    assert result.rules == ()
    assert result.stale is True
    assert result.errors == ("competition announcement discovery unavailable",)


def test_new_bad_article_is_visible_without_hiding_healthy_rules(tmp_path: Path) -> None:
    dos = _announcement("DOS")
    power = _announcement("POWER")
    service, provider = _service(
        tmp_path,
        [dos, power],
        {power.article_code: _rule(power, first_start=NOW - timedelta(days=1))},
    )

    result = service.discover(now=NOW)

    assert [rule.symbol for rule in result.rules] == ["POWER"]
    assert result.stale is True
    assert result.discovered_at_utc is None
    assert result.errors == ("DOS: competition announcement rule unavailable",)
    assert provider.detail_calls == [dos.article_code, power.article_code]


def test_detail_failure_logs_only_the_validated_symbol_and_fixed_warning(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    dos = _announcement("DOS")

    class SecretDetailProvider(_Provider):
        def fetch_announcement_rule(
            self,
            announcement: CompetitionAnnouncement,
            *,
            now: datetime,
        ) -> CompetitionRule:
            raise RuntimeError("secret private /tmp/provider-token")

    service = discovery.CompetitionDiscoveryService(
        provider=SecretDetailProvider([dos], {}),
        cache=discovery.CompetitionDiscoveryCache(tmp_path / "discovery.json"),
    )

    with caplog.at_level(logging.WARNING, logger=discovery.__name__):
        result = service.discover(now=NOW)

    assert result.errors == ("DOS: competition announcement rule unavailable",)
    assert [record.getMessage() for record in caplog.records] == [
        "DOS: competition announcement rule unavailable"
    ]
    logs = caplog.text
    assert "secret" not in logs
    assert "private" not in logs
    assert "/tmp" not in logs


def test_unsafe_announcement_symbol_never_reaches_detail_or_logs(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    unsafe = _announcement("SECRET/PATH")
    provider = _Provider([unsafe], {})
    service = discovery.CompetitionDiscoveryService(
        provider=provider,
        cache=discovery.CompetitionDiscoveryCache(tmp_path / "discovery.json"),
    )

    with caplog.at_level(logging.WARNING, logger=discovery.__name__):
        result = service.discover(now=NOW)

    assert provider.detail_calls == []
    assert result.errors == ("competition announcement discovery unavailable",)
    assert [record.getMessage() for record in caplog.records] == [
        "competition announcement discovery unavailable"
    ]
    assert "SECRET/PATH" not in caplog.text


def test_cached_bad_detail_retains_old_rule_until_known_final_end(tmp_path: Path) -> None:
    dos = _announcement("DOS")
    old_rule = _rule(dos, first_start=NOW - timedelta(days=1))
    cache = discovery.CompetitionDiscoveryCache(tmp_path / "discovery.json")
    cache.store(
        _snapshot(
            _discovered(dos, old_rule, validated_at=NOW - timedelta(hours=7)),
            discovered_at=NOW - timedelta(minutes=6),
        )
    )
    provider = _Provider([dos], {})
    service = discovery.CompetitionDiscoveryService(provider=provider, cache=cache)

    result = service.discover(now=NOW)

    assert result.rules == (old_rule,)
    assert result.stale is True
    assert result.discovered_at_utc == NOW - timedelta(minutes=6)
    assert result.errors == ("DOS: competition announcement rule unavailable",)


def test_cached_nonended_article_absent_from_scan_is_preserved_deterministically(
    tmp_path: Path,
) -> None:
    zed = _announcement("ZED", code="z-code")
    alpha = _announcement("ALPHA", code="a-code")
    power = _announcement("POWER", code="power-code")
    cache = discovery.CompetitionDiscoveryCache(tmp_path / "discovery.json")
    cache.store(
        _snapshot(
            _discovered(zed, _rule(zed, first_start=NOW - timedelta(days=1))),
            _discovered(alpha, _rule(alpha, first_start=NOW - timedelta(days=1))),
            discovered_at=NOW - timedelta(minutes=6),
        )
    )
    provider = _Provider(
        [power],
        {power.article_code: _rule(power, first_start=NOW - timedelta(days=1))},
    )
    service = discovery.CompetitionDiscoveryService(provider=provider, cache=cache)

    result = service.discover(now=NOW)

    assert [item.announcement.article_code for item in result.competitions] == [
        "power-code",
        "a-code",
        "z-code",
    ]
    assert result.stale is False


def test_first_boot_list_failure_returns_empty_error_without_configured_fallback(
    tmp_path: Path,
) -> None:
    provider = _FailingListProvider()
    service = discovery.CompetitionDiscoveryService(
        provider=provider,
        cache=discovery.CompetitionDiscoveryCache(tmp_path / "discovery.json"),
    )

    result = service.discover(now=NOW)

    assert result.rules == ()
    assert result.discovered_at_utc is None
    assert result.stale is True
    assert result.errors == ("competition announcement discovery unavailable",)
    assert provider.list_calls == 1


def test_failed_refresh_is_cooled_down_until_ttl_and_retries_at_boundary(
    tmp_path: Path,
) -> None:
    provider = _FailingListProvider()
    service = discovery.CompetitionDiscoveryService(
        provider=provider,
        cache=discovery.CompetitionDiscoveryCache(tmp_path / "discovery.json"),
    )

    first = service.discover(now=NOW)
    during_cooldown = service.discover(now=NOW + timedelta(minutes=4, seconds=59))
    retried = service.discover(now=NOW + timedelta(minutes=5))

    assert during_cooldown == first
    assert retried == first
    assert provider.list_calls == 2


def test_partial_and_cache_store_failures_are_cooled_down(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dos = _announcement("DOS")
    partial, partial_provider = _service(tmp_path / "partial", [dos], {})

    partial.discover(now=NOW)
    partial.discover(now=NOW + timedelta(minutes=1))

    assert partial_provider.list_calls == 1

    rule = _rule(dos, first_start=NOW - timedelta(days=1))
    failed_store, store_provider = _service(
        tmp_path / "store", [dos], {dos.article_code: rule}
    )

    def fail_store(_snapshot: discovery.DiscoverySnapshot) -> None:
        raise OSError("disk unavailable")

    monkeypatch.setattr(failed_store.cache, "store", fail_store)
    failed_store.discover(now=NOW)
    failed_store.discover(now=NOW + timedelta(minutes=1))

    assert store_provider.list_calls == 1


def test_refresh_limits_new_rule_requests_and_resumes_after_cooldown(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    announcements = [
        _announcement(
            f"P{index:02d}",
            released_at=NOW - timedelta(minutes=index),
        )
        for index in range(12)
    ]
    rules = {
        item.article_code: _rule(item, first_start=NOW - timedelta(days=1))
        for item in announcements
    }
    cache_path = tmp_path / "discovery.json"
    provider = _Provider(announcements, rules)
    service = discovery.CompetitionDiscoveryService(
        provider=provider,
        cache=discovery.CompetitionDiscoveryCache(cache_path),
    )

    with caplog.at_level(logging.WARNING, logger=discovery.__name__):
        first = service.discover(now=NOW)

    assert provider.detail_calls == [item.article_code for item in announcements[:8]]
    assert [rule.symbol for rule in first.rules] == [item.symbol for item in announcements[:8]]
    assert first.discovered_at_utc is None
    assert first.stale is True
    assert first.errors == ("competition announcement refresh budget exhausted",)
    assert [record.getMessage() for record in caplog.records] == [
        "competition announcement refresh budget exhausted"
    ]
    assert not cache_path.exists()

    second = service.discover(now=NOW + timedelta(minutes=5))

    assert provider.detail_calls[8:] == [item.article_code for item in announcements[8:]]
    assert len(provider.detail_calls[:8]) <= 8
    assert len(provider.detail_calls[8:]) <= 8
    assert len(provider.detail_calls) == 12
    assert [rule.symbol for rule in second.rules] == [item.symbol for item in announcements]
    assert second.discovered_at_utc == NOW + timedelta(minutes=5)
    assert second.stale is False
    assert second.errors == ()


def test_failed_rules_do_not_starve_a_later_valid_announcement(tmp_path: Path) -> None:
    failing = [_announcement(f"F{index}") for index in range(8)]
    valid = _announcement("VALID")
    announcements = [*failing, valid]
    provider = _Provider(
        announcements,
        {valid.article_code: _rule(valid, first_start=NOW - timedelta(days=1))},
    )
    service = discovery.CompetitionDiscoveryService(
        provider=provider,
        cache=discovery.CompetitionDiscoveryCache(tmp_path / "discovery.json"),
    )

    first = service.discover(now=NOW)
    first_calls = list(provider.detail_calls)
    second = service.discover(now=NOW + timedelta(minutes=5))
    second_calls = provider.detail_calls[len(first_calls):]

    assert first_calls == [item.article_code for item in failing]
    assert len(first_calls) <= 8
    assert second_calls[0] == valid.article_code
    assert len(second_calls) <= 8
    assert first.rules == ()
    assert [rule.symbol for rule in second.rules] == ["VALID"]
    assert second.stale is True


def test_reverse_clock_does_not_apply_refresh_attempt_cooldown(tmp_path: Path) -> None:
    provider = _FailingListProvider()
    service = discovery.CompetitionDiscoveryService(
        provider=provider,
        cache=discovery.CompetitionDiscoveryCache(tmp_path / "discovery.json"),
    )

    service.discover(now=NOW)
    service.discover(now=NOW - timedelta(seconds=1))

    assert provider.list_calls == 2


def test_cache_write_failure_keeps_new_memory_result_but_marks_it_stale(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dos = _announcement("DOS")
    service, _provider = _service(
        tmp_path,
        [dos],
        {dos.article_code: _rule(dos, first_start=NOW - timedelta(days=1))},
    )

    def fail_store(_snapshot: discovery.DiscoverySnapshot) -> None:
        raise OSError("secret path")

    monkeypatch.setattr(service.cache, "store", fail_store)

    result = service.discover(now=NOW)

    assert [rule.symbol for rule in result.rules] == ["DOS"]
    assert result.stale is True
    assert result.errors == ("competition discovery cache unavailable",)
    assert "secret" not in " ".join(result.errors)


def test_cache_write_failure_logs_only_the_fixed_redacted_warning(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    dos = _announcement("DOS")
    service, _provider = _service(
        tmp_path,
        [dos],
        {dos.article_code: _rule(dos, first_start=NOW - timedelta(days=1))},
    )

    def fail_store(_snapshot: discovery.DiscoverySnapshot) -> None:
        raise OSError("secret private /tmp/discovery.json")

    monkeypatch.setattr(service.cache, "store", fail_store)

    with caplog.at_level(logging.WARNING, logger=discovery.__name__):
        result = service.discover(now=NOW)

    assert result.errors == ("competition discovery cache unavailable",)
    assert [record.getMessage() for record in caplog.records] == [
        "competition discovery cache unavailable"
    ]
    logs = caplog.text
    assert "secret" not in logs
    assert "private" not in logs
    assert "/tmp" not in logs


class _BlockingProvider(_Provider):
    def __init__(
        self,
        announcements: list[CompetitionAnnouncement],
        rules: dict[str, CompetitionRule],
    ) -> None:
        super().__init__(announcements, rules)
        self.list_entered = threading.Event()
        self.list_release = threading.Event()

    def fetch_recent_announcements(self, *, now: datetime) -> list[CompetitionAnnouncement]:
        self.list_calls += 1
        self.list_entered.set()
        if not self.list_release.wait(timeout=5):
            raise TimeoutError("test did not release provider")
        return list(self.announcements)


def _run_concurrent_discovery(
    service: discovery.CompetitionDiscoveryService,
    *,
    now: datetime,
    count: int = 8,
) -> tuple[
    list[discovery.DiscoverySnapshot],
    list[BaseException],
    list[threading.Thread],
    threading.Condition,
]:
    barrier = threading.Barrier(count)
    condition = threading.Condition()
    results: list[discovery.DiscoverySnapshot] = []
    errors: list[BaseException] = []

    def worker() -> None:
        try:
            barrier.wait(timeout=5)
            result = service.discover(now=now)
            with condition:
                results.append(result)
                condition.notify_all()
        except BaseException as exc:
            with condition:
                errors.append(exc)
                condition.notify_all()

    threads = [threading.Thread(target=worker, daemon=True) for _ in range(count)]
    for thread in threads:
        thread.start()
    return results, errors, threads, condition


def test_stale_snapshot_refresh_is_single_flight_and_other_callers_return_old_rules(
    tmp_path: Path,
) -> None:
    dos = _announcement("DOS")
    old_rule = _rule(dos, first_start=NOW - timedelta(days=1))
    new_rule = replace(old_rule, winner_count=2_000)
    cache = discovery.CompetitionDiscoveryCache(tmp_path / "discovery.json")
    cache.store(
        _snapshot(
            _discovered(dos, old_rule, validated_at=NOW - timedelta(hours=7)),
            discovered_at=NOW - timedelta(minutes=6),
        )
    )
    provider = _BlockingProvider([dos], {dos.article_code: new_rule})
    service = discovery.CompetitionDiscoveryService(provider=provider, cache=cache)

    results, errors, threads, condition = _run_concurrent_discovery(service, now=NOW)

    assert provider.list_entered.wait(timeout=2)
    with condition:
        assert condition.wait_for(lambda: len(results) + len(errors) >= 7, timeout=2)
    assert errors == []
    assert provider.list_calls == 1
    assert len(results) == 7
    assert all(result.rules == (old_rule,) and result.stale for result in results)

    provider.list_release.set()
    for thread in threads:
        thread.join(timeout=2)
    assert all(not thread.is_alive() for thread in threads)
    assert errors == []
    assert len(results) == 8
    assert sum(result.rules == (new_rule,) for result in results) == 1
    assert service.discover(now=NOW).rules == (new_rule,)
    assert provider.list_calls == 1


def test_first_boot_concurrent_callers_wait_for_one_refresh_and_reuse_result(
    tmp_path: Path,
) -> None:
    dos = _announcement("DOS")
    rule = _rule(dos, first_start=NOW - timedelta(days=1))
    provider = _BlockingProvider([dos], {dos.article_code: rule})
    service = discovery.CompetitionDiscoveryService(
        provider=provider,
        cache=discovery.CompetitionDiscoveryCache(tmp_path / "discovery.json"),
    )

    results, errors, threads, condition = _run_concurrent_discovery(service, now=NOW)

    assert provider.list_entered.wait(timeout=2)
    with condition:
        assert not condition.wait_for(lambda: bool(results or errors), timeout=0.2)
    assert provider.list_calls == 1

    provider.list_release.set()
    for thread in threads:
        thread.join(timeout=2)
    assert all(not thread.is_alive() for thread in threads)
    assert errors == []
    assert len(results) == 8
    assert all(result.rules == (rule,) and not result.stale for result in results)
    assert provider.list_calls == 1


def test_service_cache_write_failure_preserves_disk_lkg_and_publishes_new_memory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dos = _announcement("DOS")
    old_rule = _rule(dos, first_start=NOW - timedelta(days=1))
    new_rule = replace(old_rule, winner_count=2_000)
    cache = discovery.CompetitionDiscoveryCache(tmp_path / "discovery.json")
    cache.store(
        _snapshot(
            _discovered(dos, old_rule, validated_at=NOW - timedelta(hours=7)),
            discovered_at=NOW - timedelta(minutes=6),
        )
    )
    provider = _Provider([dos], {dos.article_code: new_rule})
    service = discovery.CompetitionDiscoveryService(provider=provider, cache=cache)

    def fail_replace(_source: object, _target: object) -> None:
        raise OSError("disk unavailable")

    monkeypatch.setattr(discovery.os, "replace", fail_replace)

    result = service.discover(now=NOW)

    assert result.rules == (new_rule,)
    assert result.stale is True
    assert result.errors == ("competition discovery cache unavailable",)
    assert cache.load().rules == (old_rule,)
    assert list(tmp_path.glob(".discovery.json.*.tmp")) == []
