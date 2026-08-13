from __future__ import annotations

import copy
from dataclasses import replace
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
import threading

import pytest
import requests

import grid_optimizer.alpha_competition_metrics as metrics
from grid_optimizer.alpha_competition_metrics import RuleParseError, parse_competition_rule


FIXTURES = json.loads((Path(__file__).parent / "fixtures" / "alpha_competition_articles.json").read_text())
UTC = timezone.utc
NOW = datetime(2026, 8, 7, 12, 0, tzinfo=UTC)
DISCOVERY_NOW = datetime(2026, 8, 13, 12, 0, tzinfo=UTC)


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


def _replace_period_label(article: dict[str, object], *, round_number: int, label: str) -> None:
    tree = _tree(article)
    children = tree["children"]
    assert isinstance(children, list)
    block = children[round_number - 1]
    assert isinstance(block, dict)
    text = json.dumps(block).replace("DAPPOS Trading Competition", f"{label} Trading Competition")
    children[round_number - 1] = json.loads(text)
    _set_tree(article, tree)


def _announcement(
    article: dict[str, object],
    *,
    symbol: str,
    released_at_utc: datetime = DISCOVERY_NOW,
) -> metrics.CompetitionAnnouncement:
    data = article["data"]
    assert isinstance(data, dict)
    return metrics.CompetitionAnnouncement(
        symbol=symbol,
        article_code=str(data["code"]),
        title=str(data["title"]),
        released_at_utc=released_at_utc,
    )


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


def test_dos_accepts_consistent_project_name_period_labels() -> None:
    rule = parse_competition_rule(FIXTURES["DOS"], "DOS")

    assert (rule.symbol, rule.name, rule.article_code) == (
        "DOS", "DAPPOS", "2e19d56645a2472fa3dbf1b8bf2c7efe",
    )
    assert [round_.number for round_ in rule.rounds] == [1, 2]


def test_period_labels_must_still_be_consistent_within_one_article() -> None:
    article = copy.deepcopy(FIXTURES["DOS"])
    _replace_period_label(article, round_number=2, label="OTHER")

    with pytest.raises(RuleParseError, match="promotion round labels conflict"):
        parse_competition_rule(article, "DOS")


def test_period_labels_are_compared_case_insensitively() -> None:
    article = copy.deepcopy(FIXTURES["DOS"])
    _replace_period_label(article, round_number=2, label="dappos")

    assert [round_.number for round_ in parse_competition_rule(article, "DOS").rounds] == [1, 2]


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


def test_parser_collects_current_element_table_rows_without_treating_prose_as_rows() -> None:
    article = copy.deepcopy(FIXTURES["QUID"])
    data = article["data"]
    assert isinstance(data, dict)
    multipliers = (3.5, 3.0, 2.5, 2.0, 1.8, 1.3, 1.0)
    rows = []
    for day, multiplier in enumerate(multipliers, start=1):
        rows.append({
            "node": "element",
            "tag": "tr",
            "children": [
                {"node": "element", "tag": "td", "children": [
                    {"node": "element", "tag": "span", "children": [{"node": "text", "text": f"Day {day}"}]},
                ]},
                {"node": "element", "tag": "td", "children": [
                    {"node": "text", "text": "2026-08-05 13:00 (UTC) to 2026-08-06 13:00 (UTC)"},
                ]},
                {"node": "element", "tag": "td", "children": [
                    {"node": "element", "tag": "strong", "children": [{"node": "text", "text": f"{multiplier}x"}]},
                ]},
            ],
        })
    data["body"] = json.dumps({
        "node": "element",
        "tag": "article",
        "children": [
            {"node": "element", "tag": "p", "children": [{"node": "text", "text": "1st QUID Trading Competition Promotion Period: 2026-08-05 13:00 (UTC) to 2026-08-12 13:00 (UTC)"}]},
            {"node": "element", "tag": "p", "children": [{"node": "text", "text": "2nd QUID Trading Competition Promotion Period: 2026-08-12 13:00 (UTC) to 2026-08-19 13:00 (UTC)"}]},
            {"node": "element", "tag": "p", "children": [{"node": "text", "text": "The top 2,500 users win."}]},
            {"node": "element", "tag": "p", "children": [{"node": "text", "text": "Day 1 is mentioned in ordinary prose with an unrelated 9.9x reward."}]},
            {"node": "element", "tag": "table", "children": [
                {"node": "element", "tag": "tbody", "children": rows},
            ]},
        ],
    })

    assert parse_competition_rule(article, "QUID").multipliers == multipliers


@pytest.mark.parametrize("position", ["before", "after"])
def test_transparent_element_day_prose_is_not_a_multiplier_row(position: str) -> None:
    article = copy.deepcopy(FIXTURES["QUID"])
    tree = _tree(article)
    children = tree["children"]
    assert isinstance(children, list)
    rows = children[3:10]
    prose = {
        "node": "element",
        "tag": "div",
        "children": [{
            "node": "element",
            "tag": "span",
            "children": [{"node": "text", "text": "Day 1 is ordinary prose with an unrelated 9.9x reward."}],
        }],
    }
    table = {
        "node": "element",
        "tag": "table",
        "children": [{
            "node": "element",
            "tag": "tbody",
            "children": [
                {"node": "element", "tag": "tr", "children": [
                    {"node": "element", "tag": "td", "children": row["children"]},
                ]}
                for row in rows
            ],
        }],
    }
    competition_section = [prose, table] if position == "before" else [table, prose]
    tree["children"] = children[:3] + competition_section + children[10:]
    _set_tree(article, tree)
    data = article["data"]
    assert isinstance(data, dict)
    prose_blocks = [block for block in metrics._body_blocks(data) if "ordinary prose" in block.text]

    assert len(prose_blocks) == 1
    assert prose_blocks[0].multiplier_row is False
    assert parse_competition_rule(article, "QUID").multipliers == (3.5, 3.0, 2.5, 2.0, 1.8, 1.3, 1.0)


def test_provider_enumerates_recent_competition_announcements_by_article_code() -> None:
    release = int((DISCOVERY_NOW - timedelta(days=1)).timestamp() * 1000)
    power = {
        "code": "8bd4e92286d8474fa440091eea5672ff",
        "title": "Binance Alpha Trading Competition: Trade Power Protocol (POWER) and Share Rewards",
        "releaseDate": release - 1,
    }
    noise = {
        "code": "noise-code",
        "title": "Binance Adds a New Alpha Token",
        "releaseDate": release - 2,
    }
    session = _FakeSession([
        _official({"articles": [_list_item(FIXTURES["DOS"], release_date=release), power, noise]}),
        _official({"articles": []}),
    ])
    provider = metrics.BinanceCompetitionRuleProvider(session=session)

    result = provider.fetch_recent_announcements(now=DISCOVERY_NOW)

    assert [(item.symbol, item.article_code) for item in result] == [
        ("DOS", "2e19d56645a2472fa3dbf1b8bf2c7efe"),
        ("POWER", "8bd4e92286d8474fa440091eea5672ff"),
    ]
    assert result[0] == metrics.CompetitionAnnouncement(
        symbol="DOS",
        article_code="2e19d56645a2472fa3dbf1b8bf2c7efe",
        title=FIXTURES["DOS"]["data"]["title"],
        released_at_utc=datetime.fromtimestamp(release / 1000, tz=UTC),
    )
    assert [call[1]["pageNo"] for call in session.calls] == [1, 2]


def test_provider_deduplicates_exact_announcement_metadata_by_article_code() -> None:
    item = _list_item(
        FIXTURES["DOS"],
        release_date=int((DISCOVERY_NOW - timedelta(days=1)).timestamp() * 1000),
    )
    session = _FakeSession([_official({"articles": [item, copy.deepcopy(item)]}), _official({"articles": []})])

    result = metrics.BinanceCompetitionRuleProvider(session=session).fetch_recent_announcements(
        now=DISCOVERY_NOW
    )

    assert len(result) == 1
    assert result[0].article_code == item["code"]


def test_provider_does_not_treat_a_recent_duplicate_only_page_as_old() -> None:
    item = _list_item(
        FIXTURES["DOS"],
        release_date=int((DISCOVERY_NOW - timedelta(days=1)).timestamp() * 1000),
    )
    session = _FakeSession([
        _official({"articles": [item]}),
        _official({"articles": [copy.deepcopy(item)]}),
        _official({"articles": []}),
    ])

    result = metrics.BinanceCompetitionRuleProvider(session=session).fetch_recent_announcements(
        now=DISCOVERY_NOW
    )

    assert len(result) == 1
    assert [call[1]["pageNo"] for call in session.calls] == [1, 2, 3]


def test_provider_rejects_conflicting_announcement_metadata_for_one_article_code() -> None:
    release = int((DISCOVERY_NOW - timedelta(days=1)).timestamp() * 1000)
    first = _list_item(FIXTURES["DOS"], release_date=release)
    conflict = copy.deepcopy(first)
    conflict["title"] = "Binance Alpha Trading Competition: Trade Other (OTHER) and Share Rewards"
    provider = metrics.BinanceCompetitionRuleProvider(
        session=_FakeSession([_official({"articles": [first, conflict]})])
    )

    with pytest.raises(RuleParseError, match="metadata conflict"):
        provider.fetch_recent_announcements(now=DISCOVERY_NOW)


@pytest.mark.parametrize(
    "item",
    [
        None,
        {"code": "", "title": "Binance Adds a New Alpha Token", "releaseDate": 1},
        {"code": "noise", "title": "Binance Adds a New Alpha Token", "releaseDate": True},
        {"code": "noise", "title": None, "releaseDate": 1},
    ],
)
def test_provider_rejects_malformed_items_even_when_the_title_is_not_a_competition(item: object) -> None:
    provider = metrics.BinanceCompetitionRuleProvider(
        session=_FakeSession([_official({"articles": [item]})])
    )

    with pytest.raises(RuleParseError):
        provider.fetch_recent_announcements(now=DISCOVERY_NOW)


@pytest.mark.parametrize(
    "title",
    [
        "binance Alpha Trading Competition: Trade Project (O) and Earn Rewards",
        "Binance Alpha Trading Competition: trade Project (O) and Earn Rewards",
        "Binance Alpha Trading Competition: Trade Project (o) and Earn Rewards",
        "Binance Alpha Trading Competition: Trade Project(O) and Earn Rewards",
        "Binance Alpha Trading Competition: Trade Other (OTHER) and Earn; Trade o1.exchange (O) and Earn More",
        "Binance Alpha Trading Competition: Trade Project (BAD-SYMBOL) and Earn Rewards",
        f"Binance Alpha Trading Competition: Trade Project ({'A' * 33}) and Earn Rewards",
        "Binance Alpha Trading Competition: Trade Project (O) anderson Rewards",
        "Binance Alpha Trading Competition:Trade Project (O) and Earn Rewards",
    ],
)
def test_announcement_title_parser_does_not_find_an_incidental_o_identity(title: str) -> None:
    release = int((DISCOVERY_NOW - timedelta(days=1)).timestamp() * 1000)
    session = _FakeSession([
        _official({"articles": [{"code": "candidate", "title": title, "releaseDate": release}]}),
        _official({"articles": []}),
    ])

    announcements = metrics.BinanceCompetitionRuleProvider(
        session=session
    ).fetch_recent_announcements(now=DISCOVERY_NOW)

    assert all(item.symbol != "O" for item in announcements)


@pytest.mark.parametrize("name", ["  ", "\t "])
def test_announcement_title_parser_rejects_whitespace_only_project_name(name: str) -> None:
    release = int((DISCOVERY_NOW - timedelta(days=1)).timestamp() * 1000)
    session = _FakeSession([
        _official({"articles": [{
            "code": "missing-project-name",
            "title": f"Binance Alpha Trading Competition: Trade {name}(DOS) and Rewards",
            "releaseDate": release,
        }]}),
        _official({"articles": []}),
    ])

    announcements = metrics.BinanceCompetitionRuleProvider(
        session=session
    ).fetch_recent_announcements(now=DISCOVERY_NOW)

    assert announcements == []


def test_announcement_title_parser_accepts_parentheses_inside_the_project_name() -> None:
    release = int((DISCOVERY_NOW - timedelta(days=1)).timestamp() * 1000)
    session = _FakeSession([
        _official({"articles": [{
            "code": "parenthesized-project",
            "title": "Binance Alpha Trading Competition: Trade Project (Labs) (DOS) and Earn Rewards",
            "releaseDate": release,
        }]}),
        _official({"articles": []}),
    ])

    announcements = metrics.BinanceCompetitionRuleProvider(
        session=session
    ).fetch_recent_announcements(now=DISCOVERY_NOW)

    assert [(item.symbol, item.article_code) for item in announcements] == [
        ("DOS", "parenthesized-project"),
    ]


def test_rule_parser_accepts_parentheses_inside_the_project_name() -> None:
    article = copy.deepcopy(FIXTURES["DOS"])
    data = article["data"]
    assert isinstance(data, dict)
    data["title"] = "Binance Alpha Trading Competition: Trade Project (Labs) (DOS) and Earn Rewards"

    rule = parse_competition_rule(article, "DOS")

    assert (rule.symbol, rule.name) == ("DOS", "Project (Labs)")


def test_provider_fetches_parenthesized_project_announcement_rule_end_to_end() -> None:
    detail = copy.deepcopy(FIXTURES["DOS"]["data"])
    assert isinstance(detail, dict)
    detail["code"] = "parenthesized-project"
    detail["title"] = "Binance Alpha Trading Competition: Trade Project (Labs) (DOS) and Earn Rewards"
    item = {
        "code": detail["code"],
        "title": detail["title"],
        "releaseDate": int((DISCOVERY_NOW - timedelta(days=1)).timestamp() * 1000),
    }
    provider = metrics.BinanceCompetitionRuleProvider(session=_FakeSession([
        _official({"articles": [item]}),
        _official({"articles": []}),
        _official(detail),
    ]))

    announcements = provider.fetch_recent_announcements(now=DISCOVERY_NOW)
    rule = provider.fetch_announcement_rule(announcements[0])

    assert (rule.symbol, rule.name, rule.article_code) == (
        "DOS", "Project (Labs)", "parenthesized-project",
    )


def test_provider_includes_cutoff_boundary_and_stops_on_an_entirely_old_page() -> None:
    cutoff = DISCOVERY_NOW - timedelta(days=60)
    page_one = [
        _list_item(FIXTURES["DOS"], release_date=int((DISCOVERY_NOW - timedelta(days=1)).timestamp() * 1000)),
        _list_item(FIXTURES["O"], release_date=int((cutoff - timedelta(milliseconds=1)).timestamp() * 1000)),
    ]
    page_two = [
        _list_item(FIXTURES["QUID"], release_date=int(cutoff.timestamp() * 1000)),
    ]
    page_three = [
        _list_item(FIXTURES["GRVT"], release_date=int((cutoff - timedelta(days=1)).timestamp() * 1000)),
    ]
    session = _FakeSession([
        _official({"articles": page_one}),
        _official({"articles": page_two}),
        _official({"articles": page_three}),
    ])

    result = metrics.BinanceCompetitionRuleProvider(session=session).fetch_recent_announcements(
        now=DISCOVERY_NOW
    )

    assert [item.symbol for item in result] == ["DOS", "QUID"]
    assert [call[1]["pageNo"] for call in session.calls] == [1, 2, 3]


def test_provider_fetches_rule_for_an_announcement_and_validates_identity() -> None:
    announcement = _announcement(FIXTURES["DOS"], symbol="DOS")
    detail = FIXTURES["DOS"]["data"]
    assert isinstance(detail, dict)
    session = _FakeSession([_official(detail)])

    rule = metrics.BinanceCompetitionRuleProvider(session=session).fetch_announcement_rule(announcement)

    assert (rule.symbol, rule.article_code, rule.title) == (
        announcement.symbol,
        announcement.article_code,
        announcement.title,
    )
    assert session.calls[0][1] == {"articleCode": announcement.article_code}


def test_provider_rejects_announcement_detail_code_mismatch() -> None:
    announcement = _announcement(FIXTURES["DOS"], symbol="DOS")
    detail = copy.deepcopy(FIXTURES["DOS"]["data"])
    assert isinstance(detail, dict)
    detail["code"] = "different-code"

    with pytest.raises(RuleParseError, match="code"):
        metrics.BinanceCompetitionRuleProvider(
            session=_FakeSession([_official(detail)])
        ).fetch_announcement_rule(announcement)


@pytest.mark.parametrize(
    "title",
    [
        "Binance Alpha Trading Competition: Trade DAPPOS (DOS) and Earn Different Rewards",
        "Binance Alpha Trading Competition: Trade DAPPOS (OTHER) and Share Rewards",
    ],
)
def test_provider_rejects_announcement_detail_title_or_symbol_mismatch(title: str) -> None:
    announcement = _announcement(FIXTURES["DOS"], symbol="DOS")
    detail = copy.deepcopy(FIXTURES["DOS"]["data"])
    assert isinstance(detail, dict)
    detail["title"] = title

    with pytest.raises(RuleParseError, match="identity"):
        metrics.BinanceCompetitionRuleProvider(
            session=_FakeSession([_official(detail)])
        ).fetch_announcement_rule(announcement)


def test_recent_announcement_provider_requires_utc_aware_now() -> None:
    provider = metrics.BinanceCompetitionRuleProvider(session=_FakeSession([]))

    with pytest.raises(ValueError, match="UTC-aware"):
        provider.fetch_recent_announcements(now=datetime(2026, 8, 13, 12, 0))


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


def test_provider_selects_unique_catalog_93_from_current_list_envelope() -> None:
    quid_data = FIXTURES["QUID"]["data"]
    assert isinstance(quid_data, dict)
    item = _list_item(FIXTURES["QUID"], release_date=int((NOW - timedelta(days=1)).timestamp() * 1000))
    session = _FakeSession([
        _official({"catalogs": [
            {"catalogId": 48, "articles": [_list_item(FIXTURES["O"])], "catalogs": []},
            {"catalogId": 93, "articles": [item], "catalogs": []},
        ]}),
        _official(quid_data),
    ])
    provider = metrics.BinanceCompetitionRuleProvider(session=session)

    rule = provider.fetch_rule("QUID", now=NOW)

    assert rule.article_code == quid_data["code"]
    assert session.calls[-1][1] == {"articleCode": quid_data["code"]}


def test_provider_prefers_legacy_direct_articles_when_catalogs_are_also_present() -> None:
    quid_data = FIXTURES["QUID"]["data"]
    assert isinstance(quid_data, dict)
    item = _list_item(FIXTURES["QUID"], release_date=int((NOW - timedelta(days=1)).timestamp() * 1000))
    session = _FakeSession([
        _official({"articles": [item], "catalogs": "ignored because direct articles take priority"}),
        _official(quid_data),
    ])
    provider = metrics.BinanceCompetitionRuleProvider(session=session)

    assert provider.fetch_rule("QUID", now=NOW).article_code == quid_data["code"]


@pytest.mark.parametrize("data", [
    {},
    {"catalogs": []},
    {"catalogs": "invalid"},
    {"catalogs": [None]},
    {"catalogs": [{"catalogId": 93, "articles": "invalid", "catalogs": []}]},
    {"catalogs": [
        {"catalogId": 93, "articles": [], "catalogs": []},
        {"catalogId": 93, "articles": [], "catalogs": []},
    ]},
    {"catalogs": [{"catalogId": 48, "articles": [], "catalogs": "invalid"}]},
    {"articles": None, "catalogs": [{"catalogId": 93, "articles": [], "catalogs": []}]},
])
def test_provider_rejects_missing_ambiguous_or_malformed_catalog_envelopes(data: object) -> None:
    provider = metrics.BinanceCompetitionRuleProvider(session=_FakeSession([_official(data)]))

    with pytest.raises(RuleParseError, match="article list"):
        provider.fetch_rule("QUID", now=NOW)


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


def _kline(opened: str, *, quote_volume: object) -> list[object]:
    open_ms = int(datetime.strptime(opened, "%Y-%m-%d %H:%M").replace(tzinfo=UTC).timestamp() * 1000)
    return [open_ms, "1", "1", "1", "1", "1", open_ms + 3_599_999, quote_volume, 1, "1", "1", "0"]


class _FakeMarketClient:
    def __init__(self, rows: list[list[object]], *, pair: str = "ALPHA_1075USDC") -> None:
        self.rows = rows
        self.pair = pair
        self.calls: list[dict[str, object]] = []
        self.token_fetches = 0

    def fetch_tokens(self) -> dict[str, object]:
        self.token_fetches += 1
        return {"QUID": SimpleNamespace(pair=self.pair)}

    def fetch_klines(
        self,
        pair: str,
        *,
        interval: str,
        limit: int,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
    ) -> list[list[object]]:
        self.calls.append(
            {
                "pair": pair,
                "interval": interval,
                "limit": limit,
                "start_time_ms": start_time_ms,
                "end_time_ms": end_time_ms,
            }
        )
        return self.rows


def test_select_round_uses_half_open_boundaries_and_reports_all_inactive_states() -> None:
    rule = parse_competition_rule(FIXTURES["QUID"], "QUID")
    first = rule.rounds[0]
    second = rule.rounds[1]
    with_gap = replace(
        rule,
        rounds=(
            first,
            replace(second, start_utc=second.start_utc + timedelta(hours=2), end_utc=second.end_utc + timedelta(hours=2)),
        ),
    )

    upcoming = metrics.select_round(with_gap, first.start_utc - timedelta(microseconds=1))
    first_start = metrics.select_round(with_gap, first.start_utc)
    first_last = metrics.select_round(with_gap, first.end_utc - timedelta(microseconds=1))
    between = metrics.select_round(with_gap, first.end_utc)
    second_start = metrics.select_round(with_gap, with_gap.rounds[1].start_utc)
    ended = metrics.select_round(with_gap, with_gap.rounds[1].end_utc)
    contiguous_second = metrics.select_round(rule, rule.rounds[0].end_utc)

    assert upcoming == metrics.RoundSelection("upcoming", None, None, None)
    assert (first_start.status, first_start.round, first_start.day, first_start.multiplier) == (
        "active", first, 1, 3.5,
    )
    assert (first_last.status, first_last.round, first_last.day, first_last.multiplier) == (
        "active", first, 7, 1.0,
    )
    assert between == metrics.RoundSelection("between_rounds", None, None, None)
    assert (second_start.status, second_start.round, second_start.day, second_start.multiplier) == (
        "active", with_gap.rounds[1], 1, 3.5,
    )
    assert ended == metrics.RoundSelection("ended", None, None, None)
    assert (contiguous_second.status, contiguous_second.round, contiguous_second.day) == (
        "active", rule.rounds[1], 1,
    )


@pytest.mark.parametrize("invalid_timeline", ["partial_overlap", "same_window", "reversed_numbers"])
def test_select_round_rejects_ambiguous_or_reversed_rule_timelines(invalid_timeline: str) -> None:
    rule = parse_competition_rule(FIXTURES["QUID"], "QUID")
    first, second = rule.rounds
    if invalid_timeline == "partial_overlap":
        second = replace(
            second,
            start_utc=first.end_utc - timedelta(hours=1),
            end_utc=second.end_utc - timedelta(hours=1),
        )
    elif invalid_timeline == "same_window":
        second = replace(second, start_utc=first.start_utc, end_utc=first.end_utc)
    else:
        first = replace(
            first,
            start_utc=second.end_utc + timedelta(days=1),
            end_utc=second.end_utc + timedelta(days=8),
        )
    invalid_rule = replace(rule, rounds=(first, second))

    with pytest.raises(RuleParseError, match="rule is invalid"):
        metrics.select_round(invalid_rule, NOW)


def test_rule_cache_uses_shared_validation_for_overlapping_rounds(tmp_path: Path) -> None:
    rule = parse_competition_rule(FIXTURES["QUID"], "QUID")
    first, second = rule.rounds
    overlapping = replace(
        rule,
        rounds=(
            first,
            replace(
                second,
                start_utc=first.end_utc - timedelta(hours=1),
                end_utc=second.end_utc - timedelta(hours=1),
            ),
        ),
    )

    with pytest.raises(RuleParseError, match="rule is invalid"):
        metrics.CompetitionRuleCache(tmp_path / "rules.json").store(overlapping, fetched_at=NOW)


@pytest.mark.parametrize(
    ("symbol", "before_boundary", "at_boundary", "expected"),
    [
        ("PRL", "2026-08-06 10:00", "2026-08-06 11:00", 100 * 2.0 + 200 * 2.0),
        ("QUID", "2026-08-06 12:00", "2026-08-06 13:00", 100 * 3.5 + 200 * 3.0),
    ],
)
def test_weight_kline_volume_uses_round_start_for_non_natural_utc_days(
    symbol: str, before_boundary: str, at_boundary: str, expected: float,
) -> None:
    rule = parse_competition_rule(FIXTURES[symbol], symbol)

    result = metrics.weight_kline_volume(
        rule.rounds[1 if symbol == "PRL" else 0],
        rule.multipliers,
        [_kline(before_boundary, quote_volume="100"), _kline(at_boundary, quote_volume="200")],
    )

    assert result == pytest.approx(expected)


def test_weight_kline_volume_deduplicates_open_time_and_ignores_round_outside_rows() -> None:
    rule = parse_competition_rule(FIXTURES["QUID"], "QUID")
    round_ = rule.rounds[0]
    rows = [
        _kline("2026-08-05 12:00", quote_volume="999"),
        _kline("2026-08-05 13:00", quote_volume="100"),
        _kline("2026-08-05 13:00", quote_volume="999"),
        _kline("2026-08-12 13:00", quote_volume="999"),
    ]

    assert metrics.weight_kline_volume(round_, rule.multipliers, rows) == pytest.approx(350)


@pytest.mark.parametrize(
    "rows",
    [
        [[]],
        [[True, "1", "1", "1", "1", "1", 0, "10"]],
        [["not-ms", "1", "1", "1", "1", "1", 0, "10"]],
        [_kline("2026-08-05 13:00", quote_volume=True)],
        [_kline("2026-08-05 13:00", quote_volume="nan")],
        [_kline("2026-08-05 13:00", quote_volume="inf")],
        [_kline("2026-08-05 13:00", quote_volume="-1")],
    ],
)
def test_weight_kline_volume_rejects_malformed_or_non_finite_rows(rows: list[list[object]]) -> None:
    rule = parse_competition_rule(FIXTURES["QUID"], "QUID")

    with pytest.raises(ValueError, match="kline"):
        metrics.weight_kline_volume(rule.rounds[0], rule.multipliers, rows)


def test_weight_kline_volume_validates_duplicate_rows_before_deduplicating() -> None:
    rule = parse_competition_rule(FIXTURES["QUID"], "QUID")
    valid = _kline("2026-08-05 13:00", quote_volume="100")
    invalid_duplicate = _kline("2026-08-05 13:00", quote_volume="nan")

    with pytest.raises(ValueError, match="kline"):
        metrics.weight_kline_volume(rule.rounds[0], rule.multipliers, [valid, invalid_duplicate])


def test_thresholds_use_winner_average_and_exclude_personal_rising_trader_multiplier() -> None:
    result = metrics.calculate_thresholds(weighted_volume=4_800_000, winner_count=2500)

    assert (result.average, result.watch, result.reference, result.safe) == pytest.approx(
        (1920, 768, 1152, 1920)
    )


@pytest.mark.parametrize(
    ("weighted_volume", "winner_count"),
    [(-1, 2500), (float("nan"), 2500), (float("inf"), 2500), (True, 2500), (100, 0), (100, -1), (100, 1.5), (100, True)],
)
def test_thresholds_reject_invalid_inputs(weighted_volume: object, winner_count: object) -> None:
    with pytest.raises(ValueError, match="weighted volume|winner count"):
        metrics.calculate_thresholds(weighted_volume=weighted_volume, winner_count=winner_count)


def test_volume_provider_uses_actual_pair_start_end_and_limit_200() -> None:
    rule = parse_competition_rule(FIXTURES["QUID"], "QUID")
    round_ = rule.rounds[0]
    market = _FakeMarketClient([_kline("2026-08-05 13:00", quote_volume="100")])

    result = metrics.CompetitionVolumeProvider(market=market).fetch(rule, round_, NOW)

    assert result.source == "alpha_kline_estimate"
    assert result.weighted_volume == pytest.approx(350)
    assert result.updated_at_utc == NOW
    assert market.calls == [
        {
            "pair": "ALPHA_1075USDC",
            "interval": "1h",
            "limit": 200,
            "start_time_ms": int(round_.start_utc.timestamp() * 1000),
            "end_time_ms": int(NOW.timestamp() * 1000),
        }
    ]


def test_volume_provider_uses_verified_official_total_without_market_requests() -> None:
    rule = parse_competition_rule(FIXTURES["QUID"], "QUID")
    official_updated = NOW - timedelta(seconds=5)
    market = _FakeMarketClient([])

    result = metrics.CompetitionVolumeProvider(
        market=market,
        official_fetcher=lambda rule_, round_, now: metrics.OfficialVolumeSnapshot(9_000_000, official_updated),
    ).fetch(rule, rule.rounds[0], NOW)

    assert result == metrics.VolumeSnapshot(9_000_000, "official", official_updated)
    assert market.token_fetches == 0
    assert market.calls == []


def test_volume_provider_falls_back_only_when_verified_official_total_is_none() -> None:
    rule = parse_competition_rule(FIXTURES["QUID"], "QUID")
    official_calls: list[tuple[metrics.CompetitionRule, metrics.CompetitionRound, datetime]] = []
    market = _FakeMarketClient([_kline("2026-08-05 13:00", quote_volume="10")])

    result = metrics.CompetitionVolumeProvider(
        market=market,
        official_fetcher=lambda rule_, round_, now: official_calls.append((rule_, round_, now)) or None,
    ).fetch(rule, rule.rounds[0], NOW)

    assert official_calls == [(rule, rule.rounds[0], NOW)]
    assert (result.source, result.weighted_volume, market.token_fetches, len(market.calls)) == (
        "alpha_kline_estimate", 35, 1, 1,
    )


def test_volume_provider_clamps_kline_end_to_round_end() -> None:
    rule = parse_competition_rule(FIXTURES["QUID"], "QUID")
    round_ = rule.rounds[0]
    market = _FakeMarketClient([])

    metrics.CompetitionVolumeProvider(market=market).fetch(rule, round_, round_.end_utc + timedelta(days=1))

    assert market.calls[0]["end_time_ms"] == int(round_.end_utc.timestamp() * 1000)


def test_volume_provider_counts_the_current_forming_hour_by_open_time() -> None:
    rule = parse_competition_rule(FIXTURES["QUID"], "QUID")
    round_ = rule.rounds[0]
    current = round_.start_utc + timedelta(minutes=30)
    market = _FakeMarketClient([_kline("2026-08-05 13:00", quote_volume="100")])

    result = metrics.CompetitionVolumeProvider(market=market).fetch(rule, round_, current)

    assert result.weighted_volume == pytest.approx(350)
    assert market.calls[0]["end_time_ms"] == int(current.timestamp() * 1000)


@pytest.mark.parametrize(
    ("weighted_volume", "updated_at"),
    [
        ("9000000", NOW),
        (True, NOW),
        (-1, NOW),
        (float("nan"), NOW),
        (float("inf"), NOW),
        (9_000_000, datetime(2026, 8, 7, 12, 0)),
        (9_000_000, datetime(2026, 8, 5, 12, 59, 59, 999999, tzinfo=UTC)),
        (9_000_000, NOW + timedelta(microseconds=1)),
    ],
)
def test_volume_provider_rejects_invalid_official_snapshot_instead_of_guessing_fallback(
    weighted_volume: object, updated_at: datetime,
) -> None:
    rule = parse_competition_rule(FIXTURES["QUID"], "QUID")
    market = _FakeMarketClient([])
    provider = metrics.CompetitionVolumeProvider(
        market=market,
        official_fetcher=lambda rule_, round_, now: metrics.OfficialVolumeSnapshot(weighted_volume, updated_at),
    )

    with pytest.raises(ValueError, match="official"):
        provider.fetch(rule, rule.rounds[0], NOW)

    assert market.token_fetches == 0
    assert market.calls == []


@pytest.mark.parametrize("updated_at", [datetime(2026, 8, 5, 13, 0, tzinfo=UTC), NOW])
def test_volume_provider_accepts_official_snapshot_time_boundaries(updated_at: datetime) -> None:
    rule = parse_competition_rule(FIXTURES["QUID"], "QUID")
    market = _FakeMarketClient([])

    result = metrics.CompetitionVolumeProvider(
        market=market,
        official_fetcher=lambda rule_, round_, now: metrics.OfficialVolumeSnapshot(9_000_000.5, updated_at),
    ).fetch(rule, rule.rounds[0], NOW)

    assert result == metrics.VolumeSnapshot(9_000_000.5, "official", updated_at)
    assert market.token_fetches == 0
    assert market.calls == []


def test_volume_provider_does_not_guess_pair_when_symbol_is_missing() -> None:
    rule = parse_competition_rule(FIXTURES["QUID"], "QUID")
    market = _FakeMarketClient([])
    market.fetch_tokens = lambda: {}

    with pytest.raises(ValueError, match="pair"):
        metrics.CompetitionVolumeProvider(market=market).fetch(rule, rule.rounds[0], NOW)

    assert market.calls == []


def test_volume_provider_rejects_time_before_round_start() -> None:
    rule = parse_competition_rule(FIXTURES["QUID"], "QUID")
    market = _FakeMarketClient([])

    with pytest.raises(ValueError, match="round start"):
        metrics.CompetitionVolumeProvider(market=market).fetch(
            rule, rule.rounds[0], rule.rounds[0].start_utc - timedelta(microseconds=1)
        )

    assert market.token_fetches == 0
    assert market.calls == []


def test_calculation_apis_require_utc_aware_datetimes() -> None:
    rule = parse_competition_rule(FIXTURES["QUID"], "QUID")
    naive_now = datetime(2026, 8, 7, 12, 0)

    with pytest.raises(ValueError, match="UTC-aware"):
        metrics.select_round(rule, naive_now)
    with pytest.raises(ValueError, match="UTC-aware"):
        metrics.CompetitionVolumeProvider(market=_FakeMarketClient([])).fetch(
            rule, rule.rounds[0], naive_now
        )


_SERVICE_ROW_KEYS = {
    "symbol",
    "name",
    "round",
    "day",
    "roundStartUtc",
    "roundEndUtc",
    "currentMultiplier",
    "weightedVolume",
    "volumeSource",
    "volumeUpdatedAtUtc",
    "winnerCount",
    "averageVolume",
    "watchThreshold",
    "referenceThreshold",
    "safeThreshold",
    "articleUrl",
    "stale",
    "status",
    "error",
}


class _ServiceRuleProvider:
    def __init__(self, rules: dict[str, metrics.CompetitionRule | Exception]) -> None:
        self.rules = rules
        self.calls: list[tuple[str, datetime]] = []

    def fetch_rule(self, symbol: str, *, now: datetime) -> metrics.CompetitionRule:
        self.calls.append((symbol, now))
        result = self.rules[symbol]
        if isinstance(result, Exception):
            raise result
        return result


class _ServiceRuleCache:
    def __init__(
        self,
        *,
        stale_symbols: set[str] | None = None,
        failures: dict[str, Exception] | None = None,
    ) -> None:
        self.stale_symbols = stale_symbols or set()
        self.failures = failures or {}
        self.calls: list[tuple[str, datetime]] = []

    def get(self, symbol: str, *, now: datetime, loader) -> metrics.CachedRuleResult:
        self.calls.append((symbol, now))
        failure = self.failures.get(symbol)
        if failure is not None:
            raise failure
        return metrics.CachedRuleResult(loader(symbol), symbol in self.stale_symbols)


class _ServiceVolumeProvider:
    def __init__(self, results: dict[str, list[metrics.VolumeSnapshot | Exception]]) -> None:
        self.results = results
        self.calls: list[tuple[str, int, datetime]] = []

    def fetch(
        self,
        rule: metrics.CompetitionRule,
        round_: metrics.CompetitionRound,
        now: datetime,
    ) -> metrics.VolumeSnapshot:
        self.calls.append((rule.symbol, round_.number, now))
        values = self.results[rule.symbol]
        result = values.pop(0) if len(values) > 1 else values[0]
        if isinstance(result, Exception):
            raise result
        return result


def _service(
    *,
    rules: dict[str, metrics.CompetitionRule | Exception],
    volumes: dict[str, list[metrics.VolumeSnapshot | Exception]] | None = None,
    stale_symbols: set[str] | None = None,
    cache_failures: dict[str, Exception] | None = None,
) -> tuple[
    metrics.CompetitionMetricsService,
    _ServiceRuleProvider,
    _ServiceRuleCache,
    _ServiceVolumeProvider,
]:
    rule_provider = _ServiceRuleProvider(rules)
    rule_cache = _ServiceRuleCache(stale_symbols=stale_symbols, failures=cache_failures)
    volume_provider = _ServiceVolumeProvider(volumes or {})
    return (
        metrics.CompetitionMetricsService(
            rule_provider=rule_provider,
            rule_cache=rule_cache,
            volume_provider=volume_provider,
        ),
        rule_provider,
        rule_cache,
        volume_provider,
    )


def test_service_maps_active_metrics_to_fixed_json_friendly_fields() -> None:
    rule = parse_competition_rule(FIXTURES["QUID"], "QUID")
    updated_at = NOW - timedelta(seconds=1)
    service, provider, cache, volume = _service(
        rules={"QUID": rule},
        volumes={"QUID": [metrics.VolumeSnapshot(4_800_000, "alpha_kline_estimate", updated_at)]},
    )

    payload = service.collect([" quid "], now=NOW)

    assert set(payload) == {"generatedAtUtc", "rows", "errors"}
    assert payload["generatedAtUtc"] == "2026-08-07T12:00:00+00:00"
    assert payload["errors"] == []
    assert payload["rows"] == [
        {
            "symbol": "QUID",
            "name": "Squid",
            "round": 1,
            "day": 2,
            "roundStartUtc": "2026-08-05T13:00:00+00:00",
            "roundEndUtc": "2026-08-12T13:00:00+00:00",
            "currentMultiplier": 3.0,
            "weightedVolume": 4_800_000.0,
            "volumeSource": "alpha_kline_estimate",
            "volumeUpdatedAtUtc": "2026-08-07T11:59:59+00:00",
            "winnerCount": 2500,
            "averageVolume": 1920.0,
            "watchThreshold": 768.0,
            "referenceThreshold": 1152.0,
            "safeThreshold": 1920.0,
            "articleUrl": rule.article_url,
            "stale": False,
            "status": "active",
            "error": None,
        }
    ]
    assert provider.calls == [("QUID", NOW)]
    assert cache.calls == [("QUID", NOW)]
    assert volume.calls == [("QUID", 1, NOW)]
    json.dumps(payload, allow_nan=False)


def test_service_keeps_order_and_isolates_rule_and_cache_failures_without_leaking_details() -> None:
    quid = parse_competition_rule(FIXTURES["QUID"], "QUID")
    grvt = parse_competition_rule(FIXTURES["GRVT"], "GRVT")
    o_rule = parse_competition_rule(FIXTURES["O"], "O")
    service, _provider, _cache, volume = _service(
        rules={
            "QUID": RuntimeError("SECRET provider payload {'huge': [...]}"),
            "GRVT": grvt,
            "O": o_rule,
        },
        volumes={"GRVT": [metrics.VolumeSnapshot(6_100_000, "official", NOW)]},
        cache_failures={"GRVT": RuntimeError("SECRET cache path /private/cache")},
    )

    payload = service.collect([" quid ", "grvt", "O"], now=NOW)

    assert [row["symbol"] for row in payload["rows"]] == ["QUID", "GRVT", "O"]
    assert [row["status"] for row in payload["rows"]] == ["rule_unavailable", "rule_unavailable", "ended"]
    assert all(set(row) == _SERVICE_ROW_KEYS for row in payload["rows"])
    assert payload["rows"][0]["weightedVolume"] is None
    assert payload["rows"][1]["weightedVolume"] is None
    assert payload["rows"][2]["winnerCount"] == o_rule.winner_count
    assert volume.calls == []
    assert payload["errors"] == [payload["rows"][0]["error"], payload["rows"][1]["error"]]
    assert all(error and "rule unavailable" in error for error in payload["errors"])
    assert "SECRET" not in json.dumps(payload)
    assert "private" not in json.dumps(payload)


@pytest.mark.parametrize(
    ("status", "current", "rule_factory"),
    [
        ("upcoming", datetime(2026, 8, 5, 12, 59, 59, tzinfo=UTC), lambda rule: rule),
        (
            "between_rounds",
            datetime(2026, 8, 12, 13, 30, tzinfo=UTC),
            lambda rule: replace(
                rule,
                rounds=(
                    rule.rounds[0],
                    replace(
                        rule.rounds[1],
                        start_utc=rule.rounds[1].start_utc + timedelta(hours=1),
                        end_utc=rule.rounds[1].end_utc + timedelta(hours=1),
                    ),
                ),
            ),
        ),
        ("ended", datetime(2026, 8, 19, 13, 0, tzinfo=UTC), lambda rule: rule),
    ],
)
def test_service_inactive_statuses_keep_rule_metadata_without_fetching_volume(
    status: str,
    current: datetime,
    rule_factory,
) -> None:
    rule = rule_factory(parse_competition_rule(FIXTURES["QUID"], "QUID"))
    service, _provider, _cache, volume = _service(rules={"QUID": rule})

    row = service.collect(["QUID"], now=current)["rows"][0]

    assert set(row) == _SERVICE_ROW_KEYS
    assert row["status"] == status
    assert (row["name"], row["winnerCount"], row["articleUrl"]) == (
        rule.name,
        rule.winner_count,
        rule.article_url,
    )
    for key in (
        "round",
        "day",
        "roundStartUtc",
        "roundEndUtc",
        "currentMultiplier",
        "weightedVolume",
        "volumeSource",
        "volumeUpdatedAtUtc",
        "averageVolume",
        "watchThreshold",
        "referenceThreshold",
        "safeThreshold",
    ):
        assert row[key] is None
    assert row["stale"] is False
    assert row["error"] is None
    assert volume.calls == []


def test_service_propagates_stale_rule_flag_to_active_row() -> None:
    rule = parse_competition_rule(FIXTURES["QUID"], "QUID")
    service, _provider, _cache, _volume = _service(
        rules={"QUID": rule},
        volumes={"QUID": [metrics.VolumeSnapshot(10, "official", NOW)]},
        stale_symbols={"QUID"},
    )

    row = service.collect(["QUID"], now=NOW)["rows"][0]

    assert row["status"] == "active"
    assert row["stale"] is True


def test_service_volume_cache_is_fresh_before_sixty_seconds_and_refreshes_at_boundary() -> None:
    rule = parse_competition_rule(FIXTURES["QUID"], "QUID")
    service, _provider, _cache, volume = _service(
        rules={"QUID": rule},
        volumes={
            "QUID": [
                metrics.VolumeSnapshot(100, "official", NOW),
                metrics.VolumeSnapshot(200, "official", NOW + timedelta(seconds=60)),
            ]
        },
    )

    first = service.collect(["QUID"], now=NOW)["rows"][0]
    cached = service.collect(["QUID"], now=NOW + timedelta(seconds=59, microseconds=999999))["rows"][0]
    refreshed = service.collect(["QUID"], now=NOW + timedelta(seconds=60))["rows"][0]

    assert [first["weightedVolume"], cached["weightedVolume"], refreshed["weightedVolume"]] == [100, 100, 200]
    assert len(volume.calls) == 2


def test_service_volume_cache_does_not_reuse_future_entry() -> None:
    rule = parse_competition_rule(FIXTURES["QUID"], "QUID")
    service, _provider, _cache, volume = _service(
        rules={"QUID": rule},
        volumes={
            "QUID": [
                metrics.VolumeSnapshot(100, "official", NOW + timedelta(seconds=10)),
                metrics.VolumeSnapshot(200, "official", NOW),
            ]
        },
    )

    service.collect(["QUID"], now=NOW + timedelta(seconds=10))
    row = service.collect(["QUID"], now=NOW)["rows"][0]

    assert row["weightedVolume"] == 200
    assert len(volume.calls) == 2


def test_service_volume_cache_is_keyed_by_symbol_and_round_number() -> None:
    rule = parse_competition_rule(FIXTURES["QUID"], "QUID")
    first_now = rule.rounds[0].end_utc - timedelta(seconds=30)
    second_now = rule.rounds[1].start_utc
    service, _provider, _cache, volume = _service(
        rules={"QUID": rule},
        volumes={
            "QUID": [
                metrics.VolumeSnapshot(100, "official", first_now),
                metrics.VolumeSnapshot(200, "official", second_now),
            ]
        },
    )

    first = service.collect(["QUID"], now=first_now)["rows"][0]
    second = service.collect(["QUID"], now=second_now)["rows"][0]

    assert (first["round"], first["weightedVolume"]) == (1, 100)
    assert (second["round"], second["weightedVolume"]) == (2, 200)
    assert [(symbol, number) for symbol, number, _now in volume.calls] == [("QUID", 1), ("QUID", 2)]


def test_service_volume_refresh_failure_uses_last_known_good_and_reports_stale_error() -> None:
    rule = parse_competition_rule(FIXTURES["QUID"], "QUID")
    service, _provider, _cache, volume = _service(
        rules={"QUID": rule},
        volumes={
            "QUID": [
                metrics.VolumeSnapshot(4_800_000, "official", NOW),
                RuntimeError("SECRET upstream volume payload"),
            ]
        },
    )
    service.collect(["QUID"], now=NOW)

    payload = service.collect(["QUID"], now=NOW + timedelta(seconds=60))
    row = payload["rows"][0]

    assert set(row) == _SERVICE_ROW_KEYS
    assert (row["status"], row["weightedVolume"], row["volumeSource"]) == (
        "active",
        4_800_000,
        "official",
    )
    assert row["stale"] is True
    assert row["error"] == "QUID: competition volume unavailable"
    assert payload["errors"] == [row["error"]]
    assert "SECRET" not in json.dumps(payload)
    assert len(volume.calls) == 2


def test_service_volume_failure_without_cache_is_unavailable_not_zero_and_does_not_stop_next_symbol() -> None:
    quid = parse_competition_rule(FIXTURES["QUID"], "QUID")
    grvt = parse_competition_rule(FIXTURES["GRVT"], "GRVT")
    service, _provider, _cache, volume = _service(
        rules={"QUID": quid, "GRVT": grvt},
        volumes={
            "QUID": [RuntimeError("SECRET no volume")],
            "GRVT": [metrics.VolumeSnapshot(6_100_000, "official", NOW)],
        },
    )

    payload = service.collect(["QUID", "GRVT"], now=NOW)
    unavailable, active = payload["rows"]

    assert [unavailable["status"], active["status"]] == ["volume_unavailable", "active"]
    assert all(set(row) == _SERVICE_ROW_KEYS for row in payload["rows"])
    assert unavailable["currentMultiplier"] == 3.0
    assert unavailable["winnerCount"] == quid.winner_count
    for key in (
        "weightedVolume",
        "volumeSource",
        "volumeUpdatedAtUtc",
        "averageVolume",
        "watchThreshold",
        "referenceThreshold",
        "safeThreshold",
    ):
        assert unavailable[key] is None
    assert active["weightedVolume"] == 6_100_000
    assert payload["errors"] == ["QUID: competition volume unavailable"]
    assert [call[0] for call in volume.calls] == ["QUID", "GRVT"]


def test_service_requires_utc_aware_now_before_calling_dependencies() -> None:
    rule = parse_competition_rule(FIXTURES["QUID"], "QUID")
    service, provider, cache, volume = _service(rules={"QUID": rule})

    with pytest.raises(ValueError, match="UTC-aware"):
        service.collect(["QUID"], now=datetime(2026, 8, 7, 12, 0))

    assert provider.calls == []
    assert cache.calls == []
    assert volume.calls == []


def test_service_serializes_concurrent_volume_cache_misses() -> None:
    class BlockingVolumeProvider:
        def __init__(self) -> None:
            self.calls = 0
            self.entered = threading.Event()
            self.release = threading.Event()

        def fetch(self, _rule, _round, now: datetime) -> metrics.VolumeSnapshot:
            self.calls += 1
            self.entered.set()
            assert self.release.wait(timeout=2)
            return metrics.VolumeSnapshot(4_800_000, "official", now)

    rule = parse_competition_rule(FIXTURES["QUID"], "QUID")
    provider = _ServiceRuleProvider({"QUID": rule})
    cache = _ServiceRuleCache()
    volume = BlockingVolumeProvider()
    service = metrics.CompetitionMetricsService(
        rule_provider=provider,
        rule_cache=cache,
        volume_provider=volume,
    )
    rows: list[dict[str, object]] = []

    def collect() -> None:
        rows.append(service.collect(["QUID"], now=NOW)["rows"][0])

    first = threading.Thread(target=collect)
    second = threading.Thread(target=collect)
    first.start()
    assert volume.entered.wait(timeout=2)
    second.start()
    volume.release.set()
    first.join(timeout=2)
    second.join(timeout=2)

    assert not first.is_alive()
    assert not second.is_alive()
    assert volume.calls == 1
    assert [row["weightedVolume"] for row in rows] == [4_800_000, 4_800_000]


@pytest.mark.parametrize(
    "updated_at",
    [
        datetime(2026, 8, 5, 12, 59, 59, tzinfo=UTC),
        datetime(2026, 8, 7, 12, 0),
        NOW + timedelta(microseconds=1),
    ],
)
def test_service_rejects_volume_snapshot_outside_round_or_collect_time(updated_at: datetime) -> None:
    rule = parse_competition_rule(FIXTURES["QUID"], "QUID")
    service, _provider, _cache, volume = _service(
        rules={"QUID": rule},
        volumes={"QUID": [metrics.VolumeSnapshot(4_800_000, "official", updated_at)]},
    )

    row = service.collect(["QUID"], now=NOW)["rows"][0]

    assert row["status"] == "volume_unavailable"
    assert row["weightedVolume"] is None
    assert volume.calls == [("QUID", 1, NOW)]


def test_service_accepts_volume_snapshot_at_collect_time_boundary() -> None:
    rule = parse_competition_rule(FIXTURES["QUID"], "QUID")
    service, _provider, _cache, _volume = _service(
        rules={"QUID": rule},
        volumes={"QUID": [metrics.VolumeSnapshot(4_800_000, "official", NOW)]},
    )

    row = service.collect(["QUID"], now=NOW)["rows"][0]

    assert row["status"] == "active"
    assert row["volumeUpdatedAtUtc"] == "2026-08-07T12:00:00+00:00"


def test_service_rejects_future_snapshot_crossing_active_round_end() -> None:
    rule = parse_competition_rule(FIXTURES["QUID"], "QUID")
    current = rule.rounds[0].end_utc - timedelta(microseconds=1)
    service, _provider, _cache, _volume = _service(
        rules={"QUID": rule},
        volumes={
            "QUID": [metrics.VolumeSnapshot(4_800_000, "official", rule.rounds[0].end_utc)]
        },
    )

    row = service.collect(["QUID"], now=current)["rows"][0]

    assert row["status"] == "volume_unavailable"
    assert row["weightedVolume"] is None


def test_service_does_not_fallback_to_future_cache_when_refresh_fails() -> None:
    rule = parse_competition_rule(FIXTURES["QUID"], "QUID")
    future = NOW + timedelta(hours=1)
    service, _provider, _cache, volume = _service(
        rules={"QUID": rule},
        volumes={
            "QUID": [
                metrics.VolumeSnapshot(4_800_000, "official", future),
                RuntimeError("refresh failed"),
            ]
        },
    )
    assert service.collect(["QUID"], now=future)["rows"][0]["status"] == "active"

    row = service.collect(["QUID"], now=NOW)["rows"][0]

    assert row["status"] == "volume_unavailable"
    assert row["weightedVolume"] is None
    assert len(volume.calls) == 2


def test_service_backs_off_failed_stale_volume_refresh_until_ttl_boundary() -> None:
    rule = parse_competition_rule(FIXTURES["QUID"], "QUID")
    service, _provider, _cache, volume = _service(
        rules={"QUID": rule},
        volumes={
            "QUID": [
                metrics.VolumeSnapshot(4_800_000, "official", NOW),
                RuntimeError("first refresh failed"),
                RuntimeError("boundary retry failed"),
            ]
        },
    )
    service.collect(["QUID"], now=NOW)

    first_failure = service.collect(["QUID"], now=NOW + timedelta(seconds=60))["rows"][0]
    backed_off = service.collect(["QUID"], now=NOW + timedelta(seconds=119, microseconds=999999))["rows"][0]
    assert len(volume.calls) == 2
    boundary_retry = service.collect(["QUID"], now=NOW + timedelta(seconds=120))["rows"][0]

    assert [first_failure["status"], backed_off["status"], boundary_retry["status"]] == [
        "active",
        "active",
        "active",
    ]
    assert all(row["stale"] is True for row in (first_failure, backed_off, boundary_retry))
    assert all(row["error"] == "QUID: competition volume unavailable" for row in (first_failure, backed_off, boundary_retry))
    assert len(volume.calls) == 3


def test_service_deduplicates_concurrent_failed_refreshes_with_stale_volume() -> None:
    class BlockingRefreshVolumeProvider:
        def __init__(self) -> None:
            self.calls = 0
            self.entered = threading.Event()
            self.release = threading.Event()

        def fetch(self, _rule, _round, now: datetime) -> metrics.VolumeSnapshot:
            self.calls += 1
            if self.calls == 1:
                return metrics.VolumeSnapshot(4_800_000, "official", now)
            self.entered.set()
            assert self.release.wait(timeout=2)
            raise RuntimeError("refresh failed")

    rule = parse_competition_rule(FIXTURES["QUID"], "QUID")
    provider = _ServiceRuleProvider({"QUID": rule})
    cache = _ServiceRuleCache()
    volume = BlockingRefreshVolumeProvider()
    service = metrics.CompetitionMetricsService(
        rule_provider=provider,
        rule_cache=cache,
        volume_provider=volume,
    )
    service.collect(["QUID"], now=NOW)
    rows: list[dict[str, object]] = []
    ready = threading.Barrier(3)
    collecting = [threading.Event(), threading.Event()]

    def collect_expired(index: int) -> None:
        ready.wait(timeout=2)
        collecting[index].set()
        rows.append(service.collect(["QUID"], now=NOW + timedelta(seconds=60))["rows"][0])

    first = threading.Thread(target=collect_expired, args=(0,))
    second = threading.Thread(target=collect_expired, args=(1,))
    first.start()
    second.start()
    ready.wait(timeout=2)
    assert all(event.wait(timeout=2) for event in collecting)
    assert volume.entered.wait(timeout=2)
    volume.release.set()
    first.join(timeout=2)
    second.join(timeout=2)

    assert not first.is_alive()
    assert not second.is_alive()
    assert volume.calls == 2
    assert len(rows) == 2
    assert all(row["weightedVolume"] == 4_800_000 for row in rows)
    assert all(row["stale"] is True for row in rows)


def test_service_backs_off_concurrent_initial_volume_failures_without_last_known_good() -> None:
    class BlockingInitialFailureVolumeProvider:
        def __init__(self) -> None:
            self.calls = 0
            self.entered = threading.Event()
            self.release = threading.Event()

        def fetch(self, _rule, _round, _now: datetime) -> metrics.VolumeSnapshot:
            self.calls += 1
            self.entered.set()
            assert self.release.wait(timeout=2)
            raise RuntimeError("initial fetch failed")

    rule = parse_competition_rule(FIXTURES["QUID"], "QUID")
    provider = _ServiceRuleProvider({"QUID": rule})
    cache = _ServiceRuleCache()
    volume = BlockingInitialFailureVolumeProvider()
    service = metrics.CompetitionMetricsService(
        rule_provider=provider,
        rule_cache=cache,
        volume_provider=volume,
    )
    ready = threading.Barrier(3)
    collecting = [threading.Event(), threading.Event()]
    rows: list[dict[str, object]] = []

    def collect_initial(index: int) -> None:
        ready.wait(timeout=2)
        collecting[index].set()
        rows.append(service.collect(["QUID"], now=NOW)["rows"][0])

    first = threading.Thread(target=collect_initial, args=(0,))
    second = threading.Thread(target=collect_initial, args=(1,))
    first.start()
    second.start()
    ready.wait(timeout=2)
    assert all(event.wait(timeout=2) for event in collecting)
    assert volume.entered.wait(timeout=2)
    volume.release.set()
    first.join(timeout=2)
    second.join(timeout=2)

    assert not first.is_alive()
    assert not second.is_alive()
    assert volume.calls == 1
    assert len(rows) == 2
    assert all(row["status"] == "volume_unavailable" for row in rows)
    assert all(row["weightedVolume"] is None for row in rows)

    boundary = service.collect(["QUID"], now=NOW + timedelta(seconds=60))["rows"][0]
    assert boundary["status"] == "volume_unavailable"
    assert volume.calls == 2


def test_service_volume_cache_identity_includes_article_code() -> None:
    first_rule = parse_competition_rule(FIXTURES["QUID"], "QUID")
    second_rule = replace(
        first_rule,
        article_code="new-competition-code",
        article_url="https://www.binance.com/en/support/announcement/detail/new-competition-code",
    )
    service, provider, _cache, volume = _service(
        rules={"QUID": first_rule},
        volumes={
            "QUID": [
                metrics.VolumeSnapshot(100, "official", NOW),
                metrics.VolumeSnapshot(200, "official", NOW + timedelta(seconds=1)),
            ]
        },
    )
    first = service.collect(["QUID"], now=NOW)["rows"][0]
    provider.rules["QUID"] = second_rule

    second = service.collect(["QUID"], now=NOW + timedelta(seconds=1))["rows"][0]

    assert (first["weightedVolume"], second["weightedVolume"]) == (100, 200)
    assert second["articleUrl"].endswith("/new-competition-code")
    assert len(volume.calls) == 2


@pytest.mark.parametrize("symbol", [None, True, 3, "", " ", "Q_UID", "QUID-USDT", "A" * 33])
def test_service_rejects_unsafe_symbol_before_external_io(symbol: object) -> None:
    service, provider, cache, volume = _service(rules={})

    row = service.collect([symbol], now=NOW)["rows"][0]

    assert row["status"] == "rule_unavailable"
    assert provider.calls == []
    assert cache.calls == []
    assert volume.calls == []


def test_service_preserves_duplicate_symbols_and_input_order() -> None:
    quid = parse_competition_rule(FIXTURES["QUID"], "QUID")
    grvt = parse_competition_rule(FIXTURES["GRVT"], "GRVT")
    service, provider, _cache, volume = _service(
        rules={"QUID": quid, "GRVT": grvt},
        volumes={
            "QUID": [metrics.VolumeSnapshot(100, "official", NOW)],
            "GRVT": [metrics.VolumeSnapshot(200, "official", NOW)],
        },
    )

    payload = service.collect([" quid ", "GRVT", "quid"], now=NOW)

    assert [row["symbol"] for row in payload["rows"]] == ["QUID", "GRVT", "QUID"]
    assert [symbol for symbol, _now in provider.calls] == ["QUID", "GRVT", "QUID"]
    assert [symbol for symbol, _round, _now in volume.calls] == ["QUID", "GRVT"]


def test_service_does_not_mask_internal_row_builder_defect_as_rule_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    rule = parse_competition_rule(FIXTURES["QUID"], "QUID")
    service, _provider, _cache, _volume = _service(rules={"QUID": rule})

    def fail_build(*_args) -> dict[str, object]:
        raise AssertionError("programming defect")

    monkeypatch.setattr(service, "_build_row", fail_build)

    with pytest.raises(AssertionError, match="programming defect"):
        service.collect(["QUID"], now=NOW)
