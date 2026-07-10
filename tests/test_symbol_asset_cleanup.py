from grid_optimizer.symbol_asset_cleanup import build_futures_close_requests, round_down


def test_build_futures_close_requests_handles_one_way_short() -> None:
    requests = build_futures_close_requests(
        positions=[{"symbol": "GRAMUSDT", "positionAmt": "-300", "positionSide": "BOTH"}],
        symbol="GRAMUSDT",
        step_size=0.01,
    )

    assert requests == [
        {
            "side": "BUY",
            "quantity": 300.0,
            "position_side": None,
            "reduce_only": True,
        }
    ]


def test_build_futures_close_requests_handles_hedge_mode_both_sides() -> None:
    requests = build_futures_close_requests(
        positions=[
            {"symbol": "GRAMUSDT", "positionAmt": "2.5", "positionSide": "LONG"},
            {"symbol": "GRAMUSDT", "positionAmt": "-3.25", "positionSide": "SHORT"},
        ],
        symbol="GRAMUSDT",
        step_size=0.01,
    )

    assert requests == [
        {
            "side": "SELL",
            "quantity": 2.5,
            "position_side": "LONG",
            "reduce_only": None,
        },
        {
            "side": "BUY",
            "quantity": 3.25,
            "position_side": "SHORT",
            "reduce_only": None,
        },
    ]


def test_round_down_respects_exchange_step() -> None:
    assert round_down(300.019, 0.01) == 300.01
