from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from grid_optimizer.alpha_competition_history import CompetitionHistoryStore
from grid_optimizer.alpha_competition_metrics import CompetitionRound, CompetitionRule


UTC = timezone.utc
NOW = datetime(2026, 9, 30, 12, 0, tzinfo=UTC)


def _rule() -> CompetitionRule:
    return CompetitionRule(
        symbol="AEON",
        name="AEON",
        article_code="aeon-article",
        title="Binance Alpha Trading Competition: Trade AEON (AEON) and Win",
        article_url="https://www.binance.com/en/support/announcement/detail/aeon-article",
        published_at_utc=NOW - timedelta(days=17),
        winner_count=2500,
        rounds=(
            CompetitionRound(1, NOW - timedelta(days=15), NOW - timedelta(days=8)),
            CompetitionRound(2, NOW - timedelta(days=8), NOW - timedelta(days=1)),
        ),
        multipliers=(3.5, 3.0, 2.5, 2.0, 1.8, 1.3, 1.0),
    )


def test_ended_rounds_survive_refresh_and_have_independent_cutoffs(tmp_path: Path) -> None:
    path = tmp_path / "history.json"
    store = CompetitionHistoryStore(path)
    store.archive_rules([_rule()], now=NOW - timedelta(days=9))
    store.capture_rows(
        [{
            "articleUrl": _rule().article_url,
            "round": 1,
            "status": "active",
            "weightedVolume": 4_000_000.0,
            "referenceThreshold": 960.0,
            "safeThreshold": 1600.0,
            "leaderboardThreshold": 1800.0,
            "volumeSource": "alpha_kline_estimate",
        }],
        now=NOW - timedelta(days=9),
    )

    rows = store.snapshot(now=NOW)["rows"]
    assert {row["round"] for row in rows} == {1, 2}
    first = next(row for row in rows if row["round"] == 1)
    assert first["lastObservation"]["referenceThreshold"] == 960.0

    store.save_final({"id": "aeon-article:1", "finalThreshold": 2300.0}, now=NOW)
    store.save_final({"id": "aeon-article:2", "finalThreshold": 3100.0}, now=NOW)
    store.archive_rules([_rule()], now=NOW)

    restored = CompetitionHistoryStore(path).snapshot(now=NOW)["rows"]
    assert {row["round"]: row["finalThreshold"] for row in restored} == {1: 2300.0, 2: 3100.0}
    assert next(row for row in restored if row["round"] == 1)["lastObservation"]["referenceThreshold"] == 960.0


def test_manual_old_round_can_be_added_edited_and_removed(tmp_path: Path) -> None:
    store = CompetitionHistoryStore(tmp_path / "history.json")
    saved = store.save_final(
        {
            "symbol": "CAP",
            "round": 1,
            "endUtc": "2026-08-08T13:00:00+00:00",
            "winnerCount": 2500,
            "finalThreshold": 1900.0,
            "articleUrl": "https://www.binance.com/en/support/announcement/detail/cap-article",
        },
        now=NOW,
    )
    assert saved["id"] == "cap-article:1"
    assert saved["source"] == "manual"

    store.save_final({"id": saved["id"], "finalThreshold": 2100.0}, now=NOW)
    assert store.snapshot(now=NOW)["rows"][0]["finalThreshold"] == 2100.0

    store.delete(saved["id"])
    assert store.snapshot(now=NOW)["rows"] == []


@pytest.mark.parametrize("pre_archived", [False, True])
def test_official_archive_links_matching_manual_round_without_losing_cutoff(tmp_path: Path, pre_archived: bool) -> None:
    store = CompetitionHistoryStore(tmp_path / "history.json")
    rule = _rule()
    if pre_archived:
        store.archive_rules([rule], now=NOW)
    manual = store.save_final({
        "symbol": rule.symbol, "round": 1, "endUtc": rule.rounds[0].end_utc.isoformat(),
        "winnerCount": rule.winner_count, "finalThreshold": 303574, "note": "verified by user",
    }, now=NOW)
    store.archive_rules([rule], now=NOW)
    rows = store.snapshot(now=NOW)["rows"]
    assert len(rows) == 2
    assert not any(row["id"] == manual["id"] for row in rows)
    first = next(row for row in rows if row["round"] == 1)
    assert first["articleCode"] == rule.article_code
    assert first["finalThreshold"] == 303574
    assert first["note"] == "verified by user"
    assert store.pending_reference(now=NOW) is not None
    assert store.pending_reward(now=NOW)["id"] == first["id"]


def test_official_archive_does_not_merge_manual_round_with_wrong_winners(tmp_path: Path) -> None:
    store = CompetitionHistoryStore(tmp_path / "history.json")
    rule = _rule()
    store.save_final({
        "symbol": rule.symbol, "round": 1, "endUtc": rule.rounds[0].end_utc.isoformat(),
        "winnerCount": rule.winner_count + 1, "finalThreshold": 123,
    }, now=NOW)
    store.archive_rules([rule], now=NOW)
    assert len(store.snapshot(now=NOW)["rows"]) == 3


def test_official_archive_preserves_conflicting_manual_cutoffs(tmp_path: Path) -> None:
    store = CompetitionHistoryStore(tmp_path / "history.json")
    rule = _rule()
    store.archive_rules([rule], now=NOW)
    store.save_final({"id": "aeon-article:1", "finalThreshold": 111}, now=NOW)
    manual = store.save_final({
        "symbol": rule.symbol, "round": 1, "endUtc": rule.rounds[0].end_utc.isoformat(),
        "winnerCount": rule.winner_count, "finalThreshold": 222,
    }, now=NOW)
    store.archive_rules([rule], now=NOW)
    rows = store.snapshot(now=NOW)["rows"]
    assert next(row for row in rows if row["id"] == manual["id"])["finalThreshold"] == 222
    assert next(row for row in rows if row["id"] == "aeon-article:1")["finalThreshold"] == 111


@pytest.mark.parametrize("value", [0, -1, float("nan"), "oops"])
def test_final_cutoff_must_be_positive_finite(tmp_path: Path, value: object) -> None:
    store = CompetitionHistoryStore(tmp_path / "history.json")
    store.archive_rules([_rule()], now=NOW)
    with pytest.raises(ValueError):
        store.save_final({"id": "aeon-article:1", "finalThreshold": value}, now=NOW)


def test_future_round_cannot_be_marked_final(tmp_path: Path) -> None:
    store = CompetitionHistoryStore(tmp_path / "history.json")
    with pytest.raises(ValueError):
        store.save_final(
            {
                "symbol": "XDP",
                "round": 2,
                "endUtc": "2026-10-13T13:00:00+00:00",
                "winnerCount": 2500,
                "finalThreshold": 2200.0,
            },
            now=NOW,
        )


def test_finished_rule_can_store_final_kline_reference_without_changing_cutoff(tmp_path: Path) -> None:
    store = CompetitionHistoryStore(tmp_path / "history.json")
    store.archive_rules([_rule()], now=NOW)
    store.save_final({"id": "aeon-article:1", "finalThreshold": 180_554.0}, now=NOW)

    pending = store.pending_reference(now=NOW)
    assert pending is not None
    identity, rule, round_ = pending
    assert (identity, rule.symbol, round_.number) == ("aeon-article:1", "AEON", 1)

    store.save_reference(
        identity,
        weighted_volume=461_489_123.52,
        source="alpha_kline_estimate",
        now=NOW,
    )
    row = next(row for row in CompetitionHistoryStore(store.path).snapshot(now=NOW)["rows"] if row["id"] == identity)
    assert row["finalThreshold"] == 180_554.0
    assert row["finalWeightedVolume"] == pytest.approx(461_489_123.52)
    assert row["referenceThreshold"] == pytest.approx(461_489_123.52 / 2500 * 0.6)
    assert row["referenceSource"] == "alpha_kline_estimate"
    assert "rule" not in row


def test_manual_weighted_volume_and_per_winner_reward_are_saved(tmp_path: Path) -> None:
    store = CompetitionHistoryStore(tmp_path / "history.json")
    row = store.save_final(
        {
            "symbol": "CAP",
            "round": 1,
            "endUtc": "2026-08-08T13:00:00+00:00",
            "winnerCount": 2500,
            "finalThreshold": 180_554.0,
            "weightedVolume": 460_000_000.0,
            "rewardValueU": 120.0,
        },
        now=NOW,
    )

    assert row["referenceThreshold"] == pytest.approx(460_000_000 / 2500 * 0.6)
    assert row["referenceSource"] == "manual_volume"
    assert row["rewardValueU"] == 120.0
    assert row["finalThreshold"] / row["rewardValueU"] == pytest.approx(1504.6166666666666)


def test_failed_reference_calculation_is_cooled_down(tmp_path: Path) -> None:
    store = CompetitionHistoryStore(tmp_path / "history.json")
    store.archive_rules([_rule()], now=NOW)
    store.save_reference("aeon-article:2", weighted_volume=1000.0, source="alpha_kline_estimate", now=NOW)
    store.mark_reference_unavailable("aeon-article:1", now=NOW)

    assert store.pending_reference(now=NOW + timedelta(minutes=5)) is None
    assert store.pending_reference(now=NOW + timedelta(minutes=11))[0] == "aeon-article:1"


def test_reward_is_persisted_and_ratio_follows_cutoff_edits(tmp_path: Path) -> None:
    store = CompetitionHistoryStore(tmp_path / "history.json")
    store.archive_rules([_rule()], now=NOW)
    store.save_final({"id": "aeon-article:1", "finalThreshold": 180_554.0}, now=NOW)
    assert store.pending_reward(now=NOW)["id"] == "aeon-article:1"
    store.save_reward("aeon-article:1", {
        "rewardValueU": 112.294,
        "rewardTokensPerWinner": 910,
        "rewardEndPriceU": 0.1234,
        "rewardSource": "official_reward_alpha_end_close",
    }, now=NOW)
    assert store.pending_reward(now=NOW)["id"] == "aeon-article:2"
    store.save_final({"id": "aeon-article:1", "finalThreshold": 190_000.0}, now=NOW)
    row = next(row for row in CompetitionHistoryStore(store.path).snapshot(now=NOW)["rows"] if row["round"] == 1)
    assert row["rewardValueU"] == 112.294
    assert row["thresholdRewardRatio"] == pytest.approx(190_000 / 112.294)


def test_reward_failure_is_retried_after_cooldown(tmp_path: Path) -> None:
    store = CompetitionHistoryStore(tmp_path / "history.json")
    store.archive_rules([_rule()], now=NOW)
    store.mark_reward_unavailable("aeon-article:1", now=NOW)
    store.mark_reward_unavailable("aeon-article:2", now=NOW)
    assert store.pending_reward(now=NOW + timedelta(minutes=5)) is None
    assert store.pending_reward(now=NOW + timedelta(minutes=11))["id"] == "aeon-article:1"
