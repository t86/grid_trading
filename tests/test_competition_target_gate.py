"""Tests for the underwater-side handling of the competition target gate."""
from grid_optimizer.competition_target_gate import evaluate_triggers, resolve_flatten_action


def test_resolve_skip_when_nothing_to_close():
    assert resolve_flatten_action("LONG", 0, 0.55, 0.56) == "skip"
    assert resolve_flatten_action("SHORT", -3, 0.55, 0.54) == "skip"


def test_resolve_falls_back_to_market_without_cost_or_mark():
    # unknown ledger cost or unknown mark -> old market-flatten behaviour
    assert resolve_flatten_action("LONG", 100, 0.55, 0.0) == "market"
    assert resolve_flatten_action("SHORT", 100, 0.0, 0.54) == "market"


def test_resolve_long_side():
    assert resolve_flatten_action("LONG", 100, 0.56, 0.55) == "market"   # in profit
    assert resolve_flatten_action("LONG", 100, 0.55, 0.55) == "market"   # breakeven
    assert resolve_flatten_action("LONG", 100, 0.54, 0.55) == "rest_tp"  # underwater


def test_resolve_short_side():
    assert resolve_flatten_action("SHORT", 100, 0.54, 0.55) == "market"   # in profit
    assert resolve_flatten_action("SHORT", 100, 0.55, 0.55) == "market"   # breakeven
    assert resolve_flatten_action("SHORT", 100, 0.56, 0.55) == "rest_tp"  # underwater


def test_evaluate_triggers_zero_target_never_fires():
    target_ok, hit_target, hit_wear = evaluate_triggers(50000, 1.0, 0.0, 75000, 2.0)
    assert not target_ok and not hit_target and not hit_wear


def test_evaluate_triggers_target_hit():
    _, hit_target, _ = evaluate_triggers(60001, 1.0, 60000, 999999999, 999999)
    assert hit_target
