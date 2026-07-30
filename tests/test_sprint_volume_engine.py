from __future__ import annotations

import pytest

from grid_optimizer.sprint_volume_engine import SprintConfig, resolve_sprint_action


def decide(**overrides):
    values = {
        "config": SprintConfig(
            enabled=True,
            target_total_volume=4_000.0,
            depth_fraction=0.25,
            max_order_notional=100.0,
            min_order_notional=10.0,
            abnormal_spread_bps=40.0,
            max_take_spread_bps=0.05,
        ),
        "current_volume": 0.0,
        "mid_price": 100_000.0,
        "spread_bps": 0.01,
        "top_bid_notional": 1_000.0,
        "top_ask_notional": 1_000.0,
    }
    values.update(overrides)
    return resolve_sprint_action(**values)


def test_roundtrip_respects_order_cap():
    result = decide()

    assert result["action"] == "roundtrip"
    assert result["roundtrip_notional"] == pytest.approx(100.0)
    assert result["reason"] == "burst"


def test_roundtrip_respects_depth_fraction():
    result = decide(top_bid_notional=200.0, top_ask_notional=80.0)

    assert result["action"] == "roundtrip"
    assert result["roundtrip_notional"] == pytest.approx(20.0)


def test_roundtrip_does_not_overshoot_remaining_volume():
    result = decide(current_volume=3_950.0)

    assert result["action"] == "roundtrip"
    assert result["roundtrip_notional"] == pytest.approx(25.0)


def test_target_and_near_target_stop():
    reached = decide(current_volume=4_000.0)
    near = decide(current_volume=3_990.0)

    assert (reached["action"], reached["reason"]) == ("stop", "target_reached")
    assert (near["action"], near["reason"]) == ("stop", "near_target")


def test_tight_spread_gate_pauses():
    result = decide(spread_bps=0.06)

    assert (result["action"], result["reason"]) == (
        "pause",
        "spread_above_take_threshold",
    )


def test_abnormal_spread_and_missing_price_pause():
    abnormal = decide(spread_bps=41.0)
    missing = decide(mid_price=0.0)

    assert (abnormal["action"], abnormal["reason"]) == ("pause", "abnormal_spread")
    assert (missing["action"], missing["reason"]) == ("pause", "no_price")


def test_thin_depth_pauses():
    result = decide(top_bid_notional=20.0, top_ask_notional=20.0)

    assert (result["action"], result["reason"]) == ("pause", "depth_too_thin")


def test_disabled_configuration_does_not_trade():
    result = decide(config=SprintConfig(enabled=False))

    assert (result["action"], result["reason"]) == ("disabled", "disabled")
