from grid_optimizer.dual_leg_volume_engine import (
    DualLegConfig,
    decide_entry,
    decide_pending,
    maker_buy_price,
    should_switch_to_taker,
)


def _entry(config: DualLegConfig, **overrides):
    values = {
        "config": config,
        "current_volume": 0.0,
        "mid_price": 100.0,
        "spread_bps": 3.0,
        "top_bid_notional": 10_000.0,
        "top_ask_notional": 10_000.0,
    }
    values.update(overrides)
    return decide_entry(**values)


def test_decide_entry_prefers_maker_when_spread_is_wide_enough():
    result = _entry(
        DualLegConfig(
            enabled=True,
            target_total_volume=1_000.0,
            max_order_notional=100.0,
            min_order_notional=10.0,
            min_maker_spread_bps=2.5,
        )
    )

    assert result["action"] == "post_maker"
    assert result["mode"] == "maker_first"
    assert result["leg_notional"] == 100.0


def test_decide_entry_falls_back_to_taker_for_tight_spread():
    result = _entry(
        DualLegConfig(
            enabled=True,
            target_total_volume=1_000.0,
            max_order_notional=100.0,
            min_order_notional=10.0,
            min_maker_spread_bps=2.5,
        ),
        spread_bps=0.1,
    )

    assert result["action"] == "taker_roundtrip"
    assert result["reason"] == "spread_too_tight_for_maker"


def test_decide_entry_honors_forced_taker_and_depth_cap():
    result = _entry(
        DualLegConfig(
            enabled=True,
            target_total_volume=1_000.0,
            depth_fraction=0.25,
            max_order_notional=200.0,
            min_order_notional=10.0,
            min_maker_spread_bps=0.0,
        ),
        top_bid_notional=400.0,
        top_ask_notional=600.0,
        prefer_taker=True,
    )

    assert result["action"] == "taker_roundtrip"
    assert result["reason"] == "prefer_taker_deadline"
    assert result["leg_notional"] == 100.0


def test_decide_entry_stops_near_target_below_minimum_leg():
    result = _entry(
        DualLegConfig(
            enabled=True,
            target_total_volume=1_000.0,
            max_order_notional=100.0,
            min_order_notional=10.0,
        ),
        current_volume=985.0,
    )

    assert result["action"] == "stop"
    assert result["reason"] == "near_target"


def test_decide_entry_pauses_on_abnormal_spread():
    result = _entry(
        DualLegConfig(
            enabled=True,
            target_total_volume=1_000.0,
            abnormal_spread_bps=20.0,
        ),
        spread_bps=25.0,
    )

    assert result["action"] == "pause"
    assert result["reason"] == "abnormal_spread"


def test_decide_pending_distinguishes_fill_partial_and_timeout():
    config = DualLegConfig(enabled=True, maker_wait_seconds=5.0, min_order_notional=10.0)

    assert decide_pending(
        config=config,
        filled_notional=99.95,
        order_notional=100.0,
        elapsed_seconds=1.0,
    )["action"] == "complete"
    assert decide_pending(
        config=config,
        filled_notional=20.0,
        order_notional=100.0,
        elapsed_seconds=5.0,
    )["action"] == "cancel_and_complete"
    assert decide_pending(
        config=config,
        filled_notional=0.0,
        order_notional=100.0,
        elapsed_seconds=5.0,
    )["action"] == "cancel_unfilled"
    assert decide_pending(
        config=config,
        filled_notional=0.0,
        order_notional=100.0,
        elapsed_seconds=4.0,
    )["action"] == "wait"


def test_maker_buy_price_never_crosses_ask():
    assert maker_buy_price(bid=100.0, ask=100.03, tick_size=0.01, improve_ticks=2) == 100.02
    assert maker_buy_price(bid=100.0, ask=100.01, tick_size=0.01, improve_ticks=1) == 100.0


def test_price_wear_switch_waits_for_sample_and_requires_strict_breach():
    assert not should_switch_to_taker(
        total_spread_loss=0.05,
        done_volume=500.0,
        threshold_per10k=1.0,
        min_volume=1_000.0,
    )
    assert not should_switch_to_taker(
        total_spread_loss=0.1,
        done_volume=1_000.0,
        threshold_per10k=1.0,
        min_volume=1_000.0,
    )
    assert should_switch_to_taker(
        total_spread_loss=0.101,
        done_volume=1_000.0,
        threshold_per10k=1.0,
        min_volume=1_000.0,
    )
