from __future__ import annotations

import copy
import json
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from grid_optimizer.alpha_competition_reward import fetch_ended_reward, reward_tokens_per_winner


def _detail(text: str) -> dict:
    return {"body": json.dumps({"node": "paragraph", "children": [{"node": "text", "text": text}]})}


def test_per_winner_reward_comes_from_token_pool_and_checks_explicit_amount() -> None:
    text = (
        "The top 2,000 users by purchase volume of AEON tokens during the Promotion Period "
        "will share 1,820,000 AEON tokens equally (= 910 AEON per user)."
    )
    assert reward_tokens_per_winner(_detail(text), symbol="AEON", winner_count=2000) == 910
    with pytest.raises(ValueError, match="conflict"):
        reward_tokens_per_winner(_detail(text.replace("910", "920")), symbol="AEON", winner_count=2000)
    with pytest.raises(ValueError, match="winner count"):
        reward_tokens_per_winner(_detail(text), symbol="AEON", winner_count=2500)


def test_reward_without_explicit_amount_is_divided_by_winners() -> None:
    detail = _detail("The top 2,000 users will share 1,820,000 AEON tokens equally.")
    assert reward_tokens_per_winner(detail, symbol="AEON", winner_count=2000) == 910
    with pytest.raises(ValueError):
        reward_tokens_per_winner(detail, symbol="OTHER", winner_count=2000)


@pytest.mark.parametrize("bad_candle", [False, True])
def test_end_valuation_uses_exact_completed_minute_not_current_price(bad_candle: bool) -> None:
    detail = copy.deepcopy(json.loads(
        (Path(__file__).parent / "fixtures/alpha_competition_articles.json").read_text()
    )["CAP"]["data"])
    tree = json.loads(detail["body"])
    tree["children"].append({
        "node": "paragraph",
        "children": [{"node": "text", "text": "The top 2,000 users will share 1,820,000 CAP tokens equally (= 910 CAP per user)."}],
    })
    detail["body"] = json.dumps(tree)
    end = datetime(2026, 8, 6, 13, tzinfo=timezone.utc)
    end_ms = int(end.timestamp() * 1000)

    class Rules:
        def _get_data(self, path, params):
            assert params == {"articleCode": detail["code"]}
            return detail

    class Market:
        def fetch_tokens(self):
            return {"CAP": SimpleNamespace(pair="ALPHA_1USDT", price=999)}

        def fetch_klines(self, pair, **kwargs):
            assert kwargs == {
                "interval": "1m", "limit": 1,
                "start_time_ms": end_ms - 60000, "end_time_ms": end_ms - 1,
            }
            return [[end_ms - (0 if bad_candle else 60000), "0.1", "0.2", "0.1", "0.1234", "10", end_ms - 1]]

    row = {"symbol": "CAP", "articleCode": detail["code"], "winnerCount": 2000, "round": 1, "endUtc": end.isoformat()}
    if bad_candle:
        with pytest.raises(ValueError, match="boundary"):
            fetch_ended_reward(row, market=Market(), rules=Rules())
    else:
        reward = fetch_ended_reward(row, market=Market(), rules=Rules())
        assert reward["rewardValueU"] == pytest.approx(910 * 0.1234)
        assert reward["rewardEndPriceU"] == 0.1234
        assert reward["rewardTokensPerWinner"] == 910
