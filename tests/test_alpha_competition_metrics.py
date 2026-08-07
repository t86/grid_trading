from __future__ import annotations

import copy
from dataclasses import replace
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
import threading

import pytest
import requests

import grid_optimizer.alpha_competition_metrics as metrics
from grid_optimizer.alpha_competition_metrics import RuleParseError, parse_competition_rule


FIXTURES = json.loads((Path(__file__).parent / "fixtures" / "alpha_competition_articles.json").read_text())
UTC = timezone.utc
NOW = datetime(2026, 8, 7, 12, 0, tzinfo=UTC)


class _FakeResponse:
    def __init__(
        self,
        payload: object,
        *,
        status_error: Exception | None = None,
        json_error: Exception | None = None,
        text: str = "",
    ) -> None:
        self.payload = payload
        self.status_error = status_error
        self.json_error = json_error
        self.text = text

    def raise_for_status(self) -> None:
        if self.status_error is not None:
            raise self.status_error

    def json(self) -> object:
        if self.json_error is not None:
            raise self.json_error
        return self.payload


class _FakeSession:
    def __init__(self, responses: list[_FakeResponse | Exception]) -> None:
        self.responses = list(responses)
        self.calls: list[tuple[str, dict[str, object], float]] = []

    def get(self, url: str, *, params: dict[str, object], timeout: float) -> _FakeResponse:
        self.calls.append((url, params, timeout))
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


def _official(data: object, *, code: str = "000000") -> _FakeResponse:
    return _FakeResponse({"code": code, "message": None, "messageDetail": None, "data": data, "success": code == "000000"})


def _list_item(article: dict[str, object], *, code: str | None = None, release_date: int | None = None, title: str | None = None) -> dict[str, object]:
    data = article["data"]
    assert isinstance(data, dict)
    return {
        "code": code or data["code"],
        "title": title or data["title"],
        "releaseDate": release_date if release_date is not None else data["publishDate"],
    }


def _tree(article: dict[str, object]) -> dict[str, object]:
    data = article["data"]
    assert isinstance(data, dict)
    return json.loads(data["body"])


def _set_tree(article: dict[str, object], tree: dict[str, object]) -> None:
    data = article["data"]
    assert isinstance(data, dict)
    data["body"] = json.dumps(tree)


def _append_block(article: dict[str, object], text: str) -> None:
    tree = _tree(article)
    children = tree["children"]
    assert isinstance(children, list)
    children.append({"node": "paragraph", "children": [{"node": "text", "text": text}]})
    _set_tree(article, tree)


@pytest.mark.parametrize(
    ("symbol", "name", "code", "published", "winner", "rounds", "multipliers"),
    [
        ("QUID", "Squid", "18d7255a59f74b3d90139c755cc806dd", 1785927611774, 2500, (("2026-08-05 13:00", "2026-08-12 13:00"), ("2026-08-12 13:00", "2026-08-19 13:00")), (3.5, 3.0, 2.5, 2.0, 1.8, 1.3, 1.0)),
        ("GRVT", "Grvt", "7344e0bd9d244ff89c1a1642800da931", 1785841206003, 2500, (("2026-08-04 13:00", "2026-08-11 13:00"), ("2026-08-11 13:00", "2026-08-18 13:00")), (3.0, 3.0, 2.5, 2.0, 1.8, 1.3, 1.0)),
        ("CAP", "Cap", "b935934fe980452fa96722ecaf30abde", 1785411007454, 2000, (("2026-07-30 13:00", "2026-08-06 13:00"), ("2026-08-06 13:00", "2026-08-13 13:00")), (2.0, 2.0, 1.8, 1.8, 1.5, 1.5, 1.0)),
        ("PRL", "Perle", "21f8a7399c2542e68452f266ecb0a8ef", 1785319516682, 2000, (("2026-07-29 11:00", "2026-08-05 11:00"), ("2026-08-05 11:00", "2026-08-12 11:00")), (2.0, 2.0, 1.8, 1.8, 1.5, 1.5, 1.0)),
        ("O", "o1.exchange", "cc164ab36d114ea2a123309c9bbed74f", 1784804407255, 2160, (("2026-07-23 13:00", "2026-07-30 13:00"), ("2026-07-30 13:00", "2026-08-06 13:00")), (3.0, 3.0, 2.5, 2.0, 1.8, 1.3, 1.0)),
    ],
)
def test_parses_all_official_competition_articles(symbol, name, code, published, winner, rounds, multipliers) -> None:
    rule = parse_competition_rule(FIXTURES[symbol], symbol)

    assert (rule.symbol, rule.name, rule.article_code, rule.published_at_utc) == (symbol, name, code, datetime.fromtimestamp(published / 1000, tz=UTC))
    assert rule.winner_count == winner
    assert tuple((round_.start_utc.strftime("%Y-%m-%d %H:%M"), round_.end_utc.strftime("%Y-%m-%d %H:%M")) for round_ in rule.rounds) == rounds
    assert rule.multipliers == multipliers


@pytest.mark.parametrize("bad_title", [
    "Binance Alpha Trading Competition:Trade o1.exchange (O) and Earn Rewards",
    "Binance Alpha Trading Competition:\tTrade o1.exchange (O) and Earn Rewards",
    "Binance Alpha Trading Competition:\nTrade o1.exchange (O) and Earn Rewards",
    "Binance Alpha Trading Competition:  Trade o1.exchange (O) and Earn Rewards",
    "Binance Alpha Trading Competition: Trade OTHER (OTHER) and Earn; Trade o1.exchange (O) and Earn More",
])
def test_title_requires_exact_literal_prefix_and_first_identity(bad_title: str) -> None:
    article = copy.deepcopy(FIXTURES["O"])
    article["data"]["title"] = bad_title

    with pytest.raises(RuleParseError, match="title"):
        parse_competition_rule(article, "O")


@pytest.mark.parametrize("article, symbol", [(None, "QUID"), ({}, "QUID"), ({"data": None}, "QUID"), (FIXTURES["QUID"], None), (FIXTURES["QUID"], ""), (FIXTURES["QUID"], 3)])
def test_malformed_root_data_or_symbol_is_controlled_error(article: object, symbol: object) -> None:
    with pytest.raises(RuleParseError):
        parse_competition_rule(article, symbol)


@pytest.mark.parametrize("body", [None, "{bad json"])
def test_missing_or_malformed_body_is_controlled_error(body: object) -> None:
    article = copy.deepcopy(FIXTURES["QUID"])
    article["data"]["body"] = body

    with pytest.raises(RuleParseError, match="body"):
        parse_competition_rule(article, "QUID")


@pytest.mark.parametrize("publish_date", [True, 1785927611774.0, "1785927611774.0", " 1785927611774"])
def test_publish_date_requires_integer_or_pure_digit_string(publish_date: object) -> None:
    article = copy.deepcopy(FIXTURES["QUID"])
    article["data"]["publishDate"] = publish_date

    with pytest.raises(RuleParseError, match="publishDate"):
        parse_competition_rule(article, "QUID")


@pytest.mark.parametrize("winner", ["The top 2,50 users win.", "The top 0 users win.", ""])
def test_winner_is_strictly_grouped_and_positive(winner: str) -> None:
    article = copy.deepcopy(FIXTURES["QUID"])
    tree = _tree(article)
    text = json.dumps(tree).replace("The\\u00a0top 2,500 users win.", winner)
    article["data"]["body"] = text

    with pytest.raises(RuleParseError, match="winner"):
        parse_competition_rule(article, "QUID")


def test_later_unrelated_valid_looking_period_does_not_affect_canonical_rounds() -> None:
    article = copy.deepcopy(FIXTURES["QUID"])
    _append_block(article, "3rd QUID Trading Competition Promotion Period: 2026-08-19 13:00 (UTC) to 2026-08-26 13:00 (UTC)")

    assert [round_.number for round_ in parse_competition_rule(article, "QUID").rounds] == [1, 2]


def test_duplicate_identical_canonical_round_is_tolerated() -> None:
    article = copy.deepcopy(FIXTURES["QUID"])
    tree = _tree(article)
    children = tree["children"]
    assert isinstance(children, list)
    children.insert(1, copy.deepcopy(children[0]))
    _set_tree(article, tree)

    assert len(parse_competition_rule(article, "QUID").rounds) == 2


def test_conflicting_canonical_round_is_controlled_error() -> None:
    article = copy.deepcopy(FIXTURES["QUID"])
    tree = _tree(article)
    children = tree["children"]
    assert isinstance(children, list)
    children.insert(1, {"node": "paragraph", "children": [{"node": "text", "text": "1st QUID Trading Competition Promotion Period: 2026-08-06 13:00 (UTC) to 2026-08-13 13:00 (UTC)"}]})
    _set_tree(article, tree)

    with pytest.raises(RuleParseError, match="round"):
        parse_competition_rule(article, "QUID")


@pytest.mark.parametrize("replacement", [
    "Day 7 qualifying trades",
    "Day 7 qualifying trades 1.0x Day 7 qualifying trades 1.0x",
    "Day 1 qualifying trades 0x",
    "Day 1 qualifying trades nanx",
    "Day 1 qualifying trades " + "9" * 400 + "x",
])
def test_invalid_day_rows_are_controlled_errors(replacement: str) -> None:
    article = copy.deepcopy(FIXTURES["QUID"])
    tree = _tree(article)
    text = json.dumps(tree)
    if replacement.startswith("Day 7 qualifying trades"):
        text = text.replace("Day 7 qualifying trades 1.0x", replacement)
        article["data"]["body"] = text
    else:
        row = tree["children"][3]
        assert isinstance(row, dict)
        row["children"] = [{"node": "text", "text": replacement}]
        _set_tree(article, tree)

    with pytest.raises(RuleParseError, match="Day|multiplier"):
        parse_competition_rule(article, "QUID")


@pytest.mark.parametrize("replacement", [
    "1st QUID Trading Competition Promotion Period: 2026-08-05 13:00 (UTC) to 2026-08-11 13:00 (UTC)",
    "1st QUID Trading Competition Promotion Period: 2026-08-12 13:00 (UTC) to 2026-08-05 13:00 (UTC)",
    "1th QUID Trading Competition Promotion Period: 2026-08-05 13:00 (UTC) to 2026-08-12 13:00 (UTC)",
    "1st O Trading Competition Promotion Period: 2026-08-05 13:00 (UTC) to 2026-08-12 13:00 (UTC)",
])
def test_invalid_canonical_rounds_are_controlled_errors(replacement: str) -> None:
    article = copy.deepcopy(FIXTURES["QUID"])
    tree = _tree(article)
    first = tree["children"][0]
    assert isinstance(first, dict)
    first["children"] = [{"node": "text", "text": replacement}]
    _set_tree(article, tree)

    with pytest.raises(RuleParseError, match="round"):
        parse_competition_rule(article, "QUID")


def test_missing_day_row_is_controlled_error() -> None:
    article = copy.deepcopy(FIXTURES["QUID"])
    tree = _tree(article)
    children = tree["children"]
    assert isinstance(children, list)
    children.pop(9)
    _set_tree(article, tree)

    with pytest.raises(RuleParseError, match="Day"):
        parse_competition_rule(article, "QUID")


def test_prose_days_and_unrelated_multiplier_are_not_day_rows() -> None:
    article = copy.deepcopy(FIXTURES["QUID"])
    _append_block(article, "Day 1 through Day 7 is explanatory prose with an unrelated 1.2x reward.")

    assert parse_competition_rule(article, "QUID").multipliers == (3.5, 3.0, 2.5, 2.0, 1.8, 1.3, 1.0)


def test_day_row_split_across_text_nodes_is_collected() -> None:
    article = copy.deepcopy(FIXTURES["QUID"])
    tree = _tree(article)
    row = tree["children"][4]
    assert isinstance(row, dict)
    row["children"] = [{"node": "text", "text": "Day 2 qualifying "}, {"node": "text", "text": "trades 3.0x"}]
    _set_tree(article, tree)

    assert parse_competition_rule(article, "QUID").multipliers[1] == 3.0


def test_nested_text_nodes_decode_html_entities_and_unicode_whitespace() -> None:
    article = copy.deepcopy(FIXTURES["QUID"])
    tree = _tree(article)
    winner = tree["children"][2]
    assert isinstance(winner, dict)
    winner["children"] = [{"node": "text", "text": "The&nbsp;top\u2009 2,500 users win."}]
    _set_tree(article, tree)

    assert parse_competition_rule(article, "QUID").winner_count == 2500


def test_missing_day_seven_multiplier_does_not_use_rising_trader_multiplier() -> None:
    article = copy.deepcopy(FIXTURES["QUID"])
    tree = _tree(article)
    text = json.dumps(tree).replace("Day 7 qualifying trades 1.0x", "Day 7 qualifying trades")
    article["data"]["body"] = text

    with pytest.raises(RuleParseError, match="Day multiplier"):
        parse_competition_rule(article, "QUID")


def test_article_url_is_exact() -> None:
    assert parse_competition_rule(FIXTURES["QUID"], "QUID").article_url == "https://www.binance.com/en/support/announcement/detail/18d7255a59f74b3d90139c755cc806dd"


def test_provider_selects_latest_exact_symbol_article() -> None:
    quid_data = FIXTURES["QUID"]["data"]
    assert isinstance(quid_data, dict)
    recent = int((NOW - timedelta(days=1)).timestamp() * 1000)
    items = [
        _list_item(FIXTURES["O"], release_date=recent + 4),
        _list_item(FIXTURES["QUID"], release_date=recent + 3),
        _list_item(FIXTURES["QUID"], code="older", release_date=recent + 2),
        _list_item(
            FIXTURES["QUID"],
            code="incidental",
            release_date=recent + 5,
            title="Binance Alpha Trading Competition: Trade Other (OTHER) and Earn; Trade Squid (QUID) and Earn More",
        ),
    ]
    session = _FakeSession([_official({"articles": items}), _official(quid_data)])
    provider = metrics.BinanceCompetitionRuleProvider(session=session)

    rule = provider.fetch_rule(" quid ", now=NOW)

    assert rule.article_code == quid_data["code"]
    assert session.calls[-1][1] == {"articleCode": quid_data["code"]}
    assert all(call[2] == 20 for call in session.calls)


def test_provider_pages_when_first_page_has_no_exact_symbol() -> None:
    quid_data = FIXTURES["QUID"]["data"]
    assert isinstance(quid_data, dict)
    session = _FakeSession([
        _official({"articles": [_list_item(FIXTURES["O"], release_date=int(NOW.timestamp() * 1000))]}),
        _official({"articles": [_list_item(FIXTURES["QUID"], release_date=int((NOW - timedelta(days=1)).timestamp() * 1000))]}),
        _official(quid_data),
    ])
    provider = metrics.BinanceCompetitionRuleProvider(session=session)

    assert provider.fetch_rule("QUID", now=NOW).symbol == "QUID"
    assert [call[1]["pageNo"] for call in session.calls[:2]] == [1, 2]


def test_provider_keeps_paging_when_only_one_item_is_older_than_cutoff() -> None:
    quid_data = FIXTURES["QUID"]["data"]
    assert isinstance(quid_data, dict)
    session = _FakeSession([
        _official({"articles": [
            _list_item(FIXTURES["O"], release_date=int((NOW - timedelta(days=61)).timestamp() * 1000)),
            _list_item(FIXTURES["GRVT"], release_date=int((NOW - timedelta(days=1)).timestamp() * 1000)),
        ]}),
        _official({"articles": [_list_item(FIXTURES["QUID"], release_date=int((NOW - timedelta(days=2)).timestamp() * 1000))]}),
        _official(quid_data),
    ])
    provider = metrics.BinanceCompetitionRuleProvider(session=session)

    assert provider.fetch_rule("QUID", now=NOW).symbol == "QUID"
    assert [call[1]["pageNo"] for call in session.calls[:2]] == [1, 2]


def test_provider_stops_when_page_is_older_than_sixty_days() -> None:
    old_release = int((NOW - timedelta(days=61)).timestamp() * 1000)
    session = _FakeSession([_official({"articles": [_list_item(FIXTURES["O"], release_date=old_release)]})])
    provider = metrics.BinanceCompetitionRuleProvider(session=session)

    with pytest.raises(RuleParseError, match="no recent.*QUID"):
        provider.fetch_rule("QUID", now=NOW)

    assert len(session.calls) == 1


def test_provider_stops_after_twenty_recent_pages() -> None:
    recent_release = int((NOW - timedelta(days=1)).timestamp() * 1000)
    response = lambda: _official({"articles": [_list_item(FIXTURES["O"], release_date=recent_release)]})
    session = _FakeSession([response() for _ in range(20)])
    provider = metrics.BinanceCompetitionRuleProvider(session=session)

    with pytest.raises(RuleParseError, match="no recent.*QUID"):
        provider.fetch_rule("QUID", now=NOW)

    assert len(session.calls) == 20


@pytest.mark.parametrize(
    "payload",
    [
        {"code": "100001", "message": "private upstream detail", "data": {}},
        {"code": "000000", "data": None},
        ["not", "an", "object"],
    ],
)
def test_provider_rejects_invalid_official_response_without_leaking_body(payload: object) -> None:
    session = _FakeSession([_FakeResponse(payload, text="SECRET RESPONSE BODY")])
    provider = metrics.BinanceCompetitionRuleProvider(session=session)

    with pytest.raises(RuleParseError) as error:
        provider.fetch_rule("QUID", now=NOW)

    assert "SECRET" not in str(error.value)
    assert "private upstream detail" not in str(error.value)


def test_provider_sanitizes_network_errors_and_uses_twenty_second_timeout() -> None:
    session = _FakeSession([requests.Timeout("SECRET network detail")])
    provider = metrics.BinanceCompetitionRuleProvider(session=session)

    with pytest.raises(RuleParseError, match="CMS request failed") as error:
        provider.fetch_rule("QUID", now=NOW)

    assert "SECRET" not in str(error.value)
    assert error.value.__cause__ is None
    assert error.value.__suppress_context__ is True
    assert session.calls[0][2] == 20


def test_provider_sanitizes_json_decode_error_cause() -> None:
    session = _FakeSession([_FakeResponse(None, json_error=ValueError("SECRET JSON content"))])
    provider = metrics.BinanceCompetitionRuleProvider(session=session)

    with pytest.raises(RuleParseError, match="response is malformed") as error:
        provider.fetch_rule("QUID", now=NOW)

    assert "SECRET" not in str(error.value)
    assert error.value.__cause__ is None
    assert error.value.__suppress_context__ is True


def test_provider_requires_detail_code_to_match_selected_list_item() -> None:
    quid_data = copy.deepcopy(FIXTURES["QUID"]["data"])
    assert isinstance(quid_data, dict)
    session = _FakeSession([
        _official({"articles": [_list_item(FIXTURES["QUID"], code="listed-code", release_date=int(NOW.timestamp() * 1000))]}),
        _official(quid_data),
    ])
    provider = metrics.BinanceCompetitionRuleProvider(session=session)

    with pytest.raises(RuleParseError, match="code"):
        provider.fetch_rule("QUID", now=NOW)


def test_provider_requires_utc_aware_now() -> None:
    provider = metrics.BinanceCompetitionRuleProvider(session=_FakeSession([]))

    with pytest.raises(ValueError, match="UTC-aware"):
        provider.fetch_rule("QUID", now=datetime(2026, 8, 7, 12, 0))


def test_rule_cache_returns_fresh_rule_without_loading(tmp_path: Path) -> None:
    quid_rule = parse_competition_rule(FIXTURES["QUID"], "QUID")
    cache = metrics.CompetitionRuleCache(tmp_path / "rules.json")
    cache.store(quid_rule, fetched_at=NOW)

    result = cache.get(" quid ", now=NOW + timedelta(hours=5), loader=lambda symbol: pytest.fail(f"unexpected load for {symbol}"))

    assert result == metrics.CachedRuleResult(rule=quid_rule, stale=False)
    assert json.loads((tmp_path / "rules.json").read_text())["version"] == 1


def test_rule_cache_loads_new_symbol_even_when_another_symbol_is_fresh(tmp_path: Path) -> None:
    quid_rule = parse_competition_rule(FIXTURES["QUID"], "QUID")
    grvt_rule = parse_competition_rule(FIXTURES["GRVT"], "GRVT")
    loaded: list[str] = []
    cache = metrics.CompetitionRuleCache(tmp_path / "rules.json")
    cache.store(quid_rule, fetched_at=NOW)

    result = cache.get(" grvt ", now=NOW + timedelta(minutes=1), loader=lambda symbol: loaded.append(symbol) or grvt_rule)

    assert result == metrics.CachedRuleResult(rule=grvt_rule, stale=False)
    assert loaded == ["GRVT"]


def test_rule_cache_returns_stale_last_success_after_loader_failure(tmp_path: Path) -> None:
    quid_rule = parse_competition_rule(FIXTURES["QUID"], "QUID")
    cache = metrics.CompetitionRuleCache(tmp_path / "rules.json", ttl=timedelta(hours=6))
    cache.store(quid_rule, fetched_at=NOW)

    def fail(_symbol: str) -> metrics.CompetitionRule:
        raise RuntimeError("network unavailable")

    result = cache.get("QUID", now=NOW + timedelta(hours=7), loader=fail)

    assert result == metrics.CachedRuleResult(rule=quid_rule, stale=True)


def test_rule_cache_propagates_loader_failure_without_cached_rule(tmp_path: Path) -> None:
    expected = RuntimeError("network unavailable")
    cache = metrics.CompetitionRuleCache(tmp_path / "rules.json")

    def fail(_symbol: str) -> metrics.CompetitionRule:
        raise expected

    with pytest.raises(RuntimeError) as error:
        cache.get("QUID", now=NOW, loader=fail)

    assert error.value is expected


@pytest.mark.parametrize("contents", ["{broken", '{"version":2,"rules":{}}'])
def test_rule_cache_ignores_corrupt_or_incompatible_state_and_refetches(tmp_path: Path, contents: str) -> None:
    path = tmp_path / "rules.json"
    path.write_text(contents)
    quid_rule = parse_competition_rule(FIXTURES["QUID"], "QUID")
    loaded: list[str] = []
    cache = metrics.CompetitionRuleCache(path)

    result = cache.get("QUID", now=NOW, loader=lambda symbol: loaded.append(symbol) or quid_rule)

    assert result == metrics.CachedRuleResult(rule=quid_rule, stale=False)
    assert loaded == ["QUID"]
    assert json.loads(path.read_text())["version"] == 1


def test_rule_cache_round_trips_explicit_rule_dataclasses(tmp_path: Path) -> None:
    quid_rule = parse_competition_rule(FIXTURES["QUID"], "QUID")
    path = tmp_path / "rules.json"
    metrics.CompetitionRuleCache(path).store(quid_rule, fetched_at=NOW)

    result = metrics.CompetitionRuleCache(path).get("QUID", now=NOW + timedelta(hours=1), loader=lambda _: pytest.fail("unexpected load"))

    assert result.rule == quid_rule
    assert result.rule.rounds == quid_rule.rounds


def test_rule_cache_ignores_semantically_invalid_fresh_entry_and_refetches(tmp_path: Path) -> None:
    quid_rule = parse_competition_rule(FIXTURES["QUID"], "QUID")
    path = tmp_path / "rules.json"
    metrics.CompetitionRuleCache(path).store(quid_rule, fetched_at=NOW)
    payload = json.loads(path.read_text())
    payload["rules"]["QUID"]["rule"]["rounds"] = []
    path.write_text(json.dumps(payload))
    loaded: list[str] = []

    result = metrics.CompetitionRuleCache(path).get(
        "QUID",
        now=NOW + timedelta(hours=1),
        loader=lambda symbol: loaded.append(symbol) or quid_rule,
    )

    assert result == metrics.CachedRuleResult(quid_rule, False)
    assert loaded == ["QUID"]


def test_rule_cache_rejects_invalid_store_and_loader_rules(tmp_path: Path) -> None:
    quid_rule = parse_competition_rule(FIXTURES["QUID"], "QUID")
    first_round = quid_rule.rounds[0]
    invalid_rules = [
        replace(quid_rule, symbol="quid"),
        replace(quid_rule, name=""),
        replace(quid_rule, article_code=""),
        replace(quid_rule, winner_count=0),
        replace(quid_rule, published_at_utc=quid_rule.published_at_utc.replace(tzinfo=None)),
        replace(quid_rule, rounds=()),
        replace(quid_rule, rounds=(first_round, first_round)),
        replace(quid_rule, rounds=(replace(first_round, number=1.0),)),
        replace(quid_rule, rounds=(replace(first_round, end_utc=first_round.end_utc - timedelta(days=1)),)),
        replace(quid_rule, multipliers=()),
        replace(quid_rule, multipliers=(float("inf"),) + quid_rule.multipliers[1:]),
    ]

    for index, invalid_rule in enumerate(invalid_rules):
        cache = metrics.CompetitionRuleCache(tmp_path / f"store-{index}.json")
        with pytest.raises(RuleParseError):
            cache.store(invalid_rule, fetched_at=NOW)

    invalid_loader_rule = replace(quid_rule, multipliers=())
    with pytest.raises(RuleParseError):
        metrics.CompetitionRuleCache(tmp_path / "loader.json").get(
            "QUID", now=NOW, loader=lambda _symbol: invalid_loader_rule
        )


def test_rule_cache_skips_only_corrupt_symbol_entry(tmp_path: Path) -> None:
    quid_rule = parse_competition_rule(FIXTURES["QUID"], "QUID")
    path = tmp_path / "rules.json"
    metrics.CompetitionRuleCache(path).store(quid_rule, fetched_at=NOW)
    payload = json.loads(path.read_text())
    payload["rules"]["GRVT"] = {"fetched_at": NOW.isoformat(), "rule": {"symbol": "GRVT"}}
    path.write_text(json.dumps(payload))

    def fail(_symbol: str) -> metrics.CompetitionRule:
        raise RuntimeError("network unavailable")

    result = metrics.CompetitionRuleCache(path).get("QUID", now=NOW + timedelta(hours=7), loader=fail)

    assert result == metrics.CachedRuleResult(quid_rule, True)


def test_rule_cache_isolates_huge_multiplier_in_other_symbol_entry(tmp_path: Path) -> None:
    quid_rule = parse_competition_rule(FIXTURES["QUID"], "QUID")
    path = tmp_path / "rules.json"
    metrics.CompetitionRuleCache(path).store(quid_rule, fetched_at=NOW)
    payload = json.loads(path.read_text())
    corrupt_grvt = copy.deepcopy(payload["rules"]["QUID"])
    corrupt_grvt["rule"]["symbol"] = "GRVT"
    corrupt_grvt["rule"]["multipliers"][0] = 10**400
    payload["rules"]["GRVT"] = corrupt_grvt
    path.write_text(json.dumps(payload))

    def fail(_symbol: str) -> metrics.CompetitionRule:
        raise RuntimeError("network unavailable")

    result = metrics.CompetitionRuleCache(path).get("QUID", now=NOW + timedelta(hours=7), loader=fail)

    assert result == metrics.CachedRuleResult(quid_rule, True)


def test_rule_cache_rejects_huge_multiplier_from_store(tmp_path: Path) -> None:
    quid_rule = parse_competition_rule(FIXTURES["QUID"], "QUID")
    invalid_rule = replace(quid_rule, multipliers=(10**400,) + quid_rule.multipliers[1:])

    with pytest.raises(RuleParseError):
        metrics.CompetitionRuleCache(tmp_path / "store-huge.json").store(invalid_rule, fetched_at=NOW)


def test_rule_cache_rejects_huge_multiplier_from_loader(tmp_path: Path) -> None:
    quid_rule = parse_competition_rule(FIXTURES["QUID"], "QUID")
    invalid_rule = replace(quid_rule, multipliers=(10**400,) + quid_rule.multipliers[1:])

    with pytest.raises(RuleParseError):
        metrics.CompetitionRuleCache(tmp_path / "loader-huge.json").get(
            "QUID", now=NOW, loader=lambda _symbol: invalid_rule
        )


def test_rule_cache_does_not_treat_future_fetch_as_fresh(tmp_path: Path) -> None:
    quid_rule = parse_competition_rule(FIXTURES["QUID"], "QUID")
    loaded: list[str] = []
    cache = metrics.CompetitionRuleCache(tmp_path / "rules.json")
    cache.store(quid_rule, fetched_at=NOW + timedelta(hours=1))

    result = cache.get("QUID", now=NOW, loader=lambda symbol: loaded.append(symbol) or quid_rule)

    assert result == metrics.CachedRuleResult(quid_rule, False)
    assert loaded == ["QUID"]


def test_rule_cache_serializes_concurrent_symbol_transactions(tmp_path: Path) -> None:
    path = tmp_path / "rules.json"
    cache = metrics.CompetitionRuleCache(path)
    rules = {
        "QUID": parse_competition_rule(FIXTURES["QUID"], "QUID"),
        "GRVT": parse_competition_rule(FIXTURES["GRVT"], "GRVT"),
    }
    barrier = threading.Barrier(2)
    errors: list[BaseException] = []

    def worker(symbol: str) -> None:
        def load(_target: str) -> metrics.CompetitionRule:
            try:
                barrier.wait(timeout=0.2)
            except threading.BrokenBarrierError:
                pass
            return rules[symbol]

        try:
            cache.get(symbol, now=NOW, loader=load)
        except BaseException as error:
            errors.append(error)

    threads = [threading.Thread(target=worker, args=(symbol,)) for symbol in rules]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=2)

    assert all(not thread.is_alive() for thread in threads)
    assert errors == []
    assert set(json.loads(path.read_text())["rules"]) == {"QUID", "GRVT"}


def test_rule_cache_removes_temp_file_when_atomic_replace_fails(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    quid_rule = parse_competition_rule(FIXTURES["QUID"], "QUID")
    cache = metrics.CompetitionRuleCache(tmp_path / "rules.json")

    def fail_replace(_source: object, _destination: object) -> None:
        raise OSError("disk failure")

    monkeypatch.setattr(metrics.os, "replace", fail_replace)
    with pytest.raises(OSError, match="disk failure"):
        cache.store(quid_rule, fetched_at=NOW)

    assert list(tmp_path.iterdir()) == []


def test_rule_cache_requires_utc_aware_times(tmp_path: Path) -> None:
    quid_rule = parse_competition_rule(FIXTURES["QUID"], "QUID")
    cache = metrics.CompetitionRuleCache(tmp_path / "rules.json")

    with pytest.raises(ValueError, match="UTC-aware"):
        cache.store(quid_rule, fetched_at=datetime(2026, 8, 7, 12, 0))
    with pytest.raises(ValueError, match="UTC-aware"):
        cache.get("QUID", now=datetime(2026, 8, 7, 12, 0), loader=lambda _: quid_rule)
