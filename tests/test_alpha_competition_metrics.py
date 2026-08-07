from __future__ import annotations

import copy
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from grid_optimizer.alpha_competition_metrics import RuleParseError, parse_competition_rule


FIXTURES = json.loads((Path(__file__).parent / "fixtures" / "alpha_competition_articles.json").read_text())
UTC = timezone.utc


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
