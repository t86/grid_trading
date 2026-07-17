from __future__ import annotations

import json
from argparse import Namespace
from datetime import datetime, timedelta, timezone

import pytest

from grid_optimizer.loop_runner import (
    _classify_current_frozen_inventory_open_orders,
    _current_frozen_inventory_order_authorization,
    _filter_futures_normal_strategy_orders,
    apply_best_quote_frozen_inventory_manual_reduce,
    apply_best_quote_frozen_inventory_pair_release,
)


NOW = datetime(2026, 7, 17, 2, 0, tzinfo=timezone.utc)


def _manual_state(
    *,
    request_id: str = "manual-long-1",
    expires_at: datetime | None = None,
) -> dict[str, object]:
    return {
        "best_quote_frozen_inventory": {
            "long_qty": 0.4,
            "long_lots": [{"qty": 0.4, "entry_price": 200.0}],
            "short_qty": 0.0,
            "short_lots": [],
        },
        "best_quote_frozen_inventory_manual_reduce": {
            "long": {
                "requested": True,
                "request_id": request_id,
                "requested_qty": 0.2,
                "requested_at": (NOW - timedelta(seconds=10)).isoformat(),
                "expires_at": (expires_at or NOW + timedelta(minutes=5)).isoformat(),
            }
        },
    }


def _manual_order(*, request_id: str = "manual-long-1") -> dict[str, object]:
    return {
        "book": "frozen_bq",
        "role": "frozen_inventory_manual_reduce_long",
        "side": "SELL",
        "position_side": "LONG",
        "qty": 0.2,
        "price": 201.0,
        "force_reduce_only": True,
        "execution_type": "maker",
        "time_in_force": "GTX",
        "post_only": True,
        "manual_frozen_inventory_reduce": True,
        "frozen_inventory_request_id": request_id,
    }


def _pair_state(*, request_id: str = "pair-1") -> dict[str, object]:
    return {
        "best_quote_frozen_inventory": {
            "long_qty": 0.4,
            "long_lots": [{"qty": 0.4, "entry_price": 200.0}],
            "short_qty": 0.4,
            "short_lots": [{"qty": 0.4, "entry_price": 202.0}],
        },
        "best_quote_frozen_inventory_pair_release": {
            "requested": True,
            "request_id": request_id,
            "requested_qty": 0.2,
            "requested_at": NOW.isoformat(),
            "expires_at": (NOW + timedelta(minutes=5)).isoformat(),
            "filled_long_qty": 0.0,
            "filled_short_qty": 0.0,
            "paired_released_qty": 0.0,
        },
    }


def _pair_order(*, request_id: str = "pair-1") -> dict[str, object]:
    return {
        "book": "frozen_bq",
        "role": "frozen_inventory_pair_release_long",
        "side": "SELL",
        "position_side": "LONG",
        "qty": 0.2,
        "price": 201.0,
        "force_reduce_only": True,
        "execution_type": "maker",
        "time_in_force": "GTX",
        "post_only": True,
        "frozen_inventory_pair_release": True,
        "frozen_inventory_request_id": request_id,
    }


def _write_state(tmp_path, state: dict[str, object]) -> Namespace:
    path = tmp_path / "state.json"
    path.write_text(json.dumps(state), encoding="utf-8")
    return Namespace(state_path=str(path))


def test_current_frozen_authorization_binds_exact_live_directive(tmp_path) -> None:
    args = _write_state(tmp_path, _manual_state())

    authorized, reason = _current_frozen_inventory_order_authorization(
        args=args,
        order=_manual_order(),
        now=NOW,
    )

    assert reason["reason"] == "authorized"
    assert authorized is not None
    assert authorized["frozen_inventory_authorization_validated"] is True
    assert authorized["frozen_inventory_request_id"] == "manual-long-1"


INVALID_DIRECTIVE_NUMBERS = [
    pytest.param(float("nan"), id="nan"),
    pytest.param(float("inf"), id="positive-inf"),
    pytest.param(float("-inf"), id="negative-inf"),
    pytest.param(True, id="bool"),
    pytest.param(-0.1, id="negative"),
]


@pytest.mark.parametrize("invalid", INVALID_DIRECTIVE_NUMBERS)
def test_manual_reduce_generator_fails_closed_for_invalid_requested_qty(invalid) -> None:
    state = _manual_state(expires_at=datetime(2099, 1, 1, tzinfo=timezone.utc))
    state["best_quote_frozen_inventory_manual_reduce"]["long"]["requested_qty"] = invalid
    plan = {"buy_orders": [], "sell_orders": []}

    report = apply_best_quote_frozen_inventory_manual_reduce(
        plan=plan,
        state=state,
        report={},
        bid_price=199.0,
        ask_price=201.0,
        tick_size=0.1,
        step_size=0.1,
        min_qty=0.1,
        min_notional=5.0,
        hedge_mode=True,
    )

    assert report["active"] is False
    assert report["orders"] == []
    assert plan["sell_orders"] == []


@pytest.mark.parametrize("invalid", INVALID_DIRECTIVE_NUMBERS)
def test_current_manual_authorization_fails_closed_for_invalid_requested_qty(
    tmp_path,
    invalid,
) -> None:
    state = _manual_state()
    state["best_quote_frozen_inventory_manual_reduce"]["long"]["requested_qty"] = invalid
    args = _write_state(tmp_path, state)

    authorized, _ = _current_frozen_inventory_order_authorization(
        args=args,
        order=_manual_order(),
        now=NOW,
    )

    assert authorized is None


@pytest.mark.parametrize(
    "field",
    ["requested_qty", "filled_long_qty", "filled_short_qty", "paired_released_qty"],
)
@pytest.mark.parametrize("invalid", INVALID_DIRECTIVE_NUMBERS)
def test_pair_release_generator_fails_closed_for_invalid_directive_quantity(
    field: str,
    invalid,
) -> None:
    directive_quantities = {
        "requested_qty": 0.2,
        "filled_long_qty": 0.0,
        "filled_short_qty": 0.0,
        "paired_released_qty": 0.0,
    }
    directive_quantities[field] = invalid
    plan = {"buy_orders": [], "sell_orders": []}

    report = apply_best_quote_frozen_inventory_pair_release(
        plan=plan,
        report={
            "frozen_long_qty": 0.4,
            "frozen_short_qty": 0.4,
            "ledger": {
                "long_lots": [{"qty": 0.4, "entry_price": 200.0}],
                "short_lots": [{"qty": 0.4, "entry_price": 202.0}],
            },
        },
        bid_price=199.0,
        ask_price=201.0,
        tick_size=0.1,
        step_size=0.1,
        min_qty=0.1,
        min_notional=5.0,
        hedge_mode=True,
        enabled=True,
        stable_allowed=True,
        max_notional=100.0,
        min_side_notional=0.0,
        min_profit_ratio=0.0,
        max_slippage_ticks=0,
        allow_loss=True,
        request_id="pair-invalid",
        **directive_quantities,
    )

    assert report["active"] is False
    assert report["orders"] == []
    assert plan["sell_orders"] == []
    assert plan["buy_orders"] == []


@pytest.mark.parametrize(
    "field",
    ["requested_qty", "filled_long_qty", "filled_short_qty", "paired_released_qty"],
)
@pytest.mark.parametrize("invalid", INVALID_DIRECTIVE_NUMBERS)
def test_current_pair_authorization_fails_closed_for_invalid_directive_quantity(
    tmp_path,
    field: str,
    invalid,
) -> None:
    state = _pair_state()
    state["best_quote_frozen_inventory_pair_release"][field] = invalid
    args = _write_state(tmp_path, state)

    authorized, _ = _current_frozen_inventory_order_authorization(
        args=args,
        order=_pair_order(),
        now=NOW,
    )

    assert authorized is None


@pytest.mark.parametrize(
    ("mutate", "expected_reason"),
    [
        (
            lambda state, order: order.update(
                {"frozen_inventory_request_id": "forged-request"}
            ),
            "frozen_inventory_directive_request_mismatch",
        ),
        (
            lambda state, order: state[
                "best_quote_frozen_inventory_manual_reduce"
            ]["long"].update({"consumed": True}),
            "authorization_consumed",
        ),
        (
            lambda state, order: order.update(
                {"execution_type": "aggressive", "time_in_force": "IOC", "post_only": False}
            ),
            "frozen_inventory_gtx_maker_required",
        ),
        (
            lambda state, order: order.update({"qty": 0.3}),
            "frozen_inventory_quantity_exceeds_directive",
        ),
    ],
)
def test_current_frozen_authorization_rejects_stale_or_unsafe_plan(
    tmp_path,
    mutate,
    expected_reason: str,
) -> None:
    state = _manual_state()
    order = _manual_order()
    mutate(state, order)
    args = _write_state(tmp_path, state)

    authorized, reason = _current_frozen_inventory_order_authorization(
        args=args,
        order=order,
        now=NOW,
    )

    assert authorized is None
    assert reason["reason"] == expected_reason


def test_current_frozen_authorization_rechecks_absolute_expiry(tmp_path) -> None:
    args = _write_state(
        tmp_path,
        _manual_state(expires_at=NOW - timedelta(microseconds=1)),
    )

    authorized, reason = _current_frozen_inventory_order_authorization(
        args=args,
        order=_manual_order(),
        now=NOW,
    )

    assert authorized is None
    assert reason["reason"] == "authorization_expired"


def test_exchange_frozen_prefix_without_durable_ref_gets_no_ownership(tmp_path) -> None:
    args = _write_state(tmp_path, _manual_state())
    exchange_row = {
        **_manual_order(),
        "orderId": 71,
        "clientOrderId": "gx-bchu-frozenin-1-12345678",
        "origQty": "0.2",
        "executedQty": "0",
    }

    authorized, reason = _current_frozen_inventory_order_authorization(
        args=args,
        order=exchange_row,
        operation="cancel",
        now=NOW,
    )

    assert authorized is None
    assert reason["reason"] == "frozen_inventory_order_ref_missing"


def test_raw_exchange_order_uses_durable_ref_for_frozen_classification(tmp_path) -> None:
    state = _manual_state()
    state["best_quote_volume_order_refs"] = {
        "71": {
            "book": "frozen_bq",
            "role": "frozen_inventory_manual_reduce_long",
            "side": "SELL",
            "position_side": "LONG",
            "client_order_id": "gx-bchu-frml-1-12345678",
            "frozen_inventory_request_id": "manual-long-1",
        }
    }
    state["best_quote_frozen_inventory_manual_reduce"]["long"]["submitted"] = True
    args = _write_state(tmp_path, state)

    classified = _classify_current_frozen_inventory_open_orders(
        args=args,
        orders=[
            {
                "orderId": 71,
                "clientOrderId": "gx-bchu-frml-1-12345678",
                "side": "SELL",
                "positionSide": "LONG",
                "origQty": "0.2",
                "executedQty": "0",
                "price": "201",
            }
        ],
        now=NOW,
    )

    assert classified[0]["role"] == "frozen_inventory_manual_reduce_long"
    assert classified[0]["frozen_inventory_authorization_validated"] is True


def test_durable_frozen_ref_keeps_ownership_when_directive_is_missing(tmp_path) -> None:
    state = _manual_state()
    state.pop("best_quote_frozen_inventory_manual_reduce")
    state["best_quote_volume_order_refs"] = {
        "71": {
            "book": "frozen_bq",
            "role": "frozen_inventory_manual_reduce_long",
            "side": "SELL",
            "position_side": "LONG",
            "client_order_id": "gx-bchu-frml-1-12345678",
            "frozen_inventory_request_id": "manual-long-1",
        }
    }
    args = _write_state(tmp_path, state)

    classified = _classify_current_frozen_inventory_open_orders(
        args=args,
        orders=[
            {
                "orderId": 71,
                "clientOrderId": "gx-bchu-frml-1-12345678",
                "side": "SELL",
                "positionSide": "LONG",
                "origQty": "0.2",
                "executedQty": "0",
                "price": "201",
            }
        ],
        now=NOW,
    )

    assert classified[0]["book"] == "frozen_bq"
    assert classified[0]["role"] == "frozen_inventory_manual_reduce_long"
    assert classified[0]["frozen_inventory_ownership_validated"] is True
    assert classified[0].get("frozen_inventory_authorization_validated") is not True


def test_raw_exchange_frozen_prefix_without_ref_remains_ordinary(tmp_path) -> None:
    args = _write_state(tmp_path, _manual_state())

    classified = _classify_current_frozen_inventory_open_orders(
        args=args,
        orders=[
            {
                "orderId": 72,
                "clientOrderId": "gx-bchu-frml-spoof-12345678",
                "side": "SELL",
                "positionSide": "LONG",
                "origQty": "0.2",
                "executedQty": "0",
                "price": "201",
            }
        ],
        now=NOW,
    )

    assert classified[0].get("frozen_inventory_authorization_validated") is not True
    assert "role" not in classified[0]


def test_normal_order_filter_trusts_durable_ref_not_frozen_prefix(tmp_path) -> None:
    state = _manual_state()
    state["best_quote_volume_order_refs"] = {
        "71": {
            "book": "frozen_bq",
            "role": "frozen_inventory_manual_reduce_long",
            "side": "SELL",
            "position_side": "LONG",
            "client_order_id": "gx-bchu-frml-1-12345678",
            "frozen_inventory_request_id": "manual-long-1",
        }
    }
    state["best_quote_frozen_inventory_manual_reduce"]["long"]["submitted"] = True
    args = _write_state(tmp_path, state)
    open_orders = [
        {
            "orderId": 71,
            "clientOrderId": "gx-bchu-frml-1-12345678",
            "side": "SELL",
            "positionSide": "LONG",
            "origQty": "0.2",
            "executedQty": "0",
        },
        {
            "orderId": 72,
            "clientOrderId": "gx-bchu-frozen-spoof-12345678",
            "side": "BUY",
            "positionSide": "LONG",
            "origQty": "0.1",
            "executedQty": "0",
        },
    ]

    normal = _filter_futures_normal_strategy_orders(
        open_orders,
        "BCHUSDT",
        args=args,
        now=NOW,
    )

    assert [row["orderId"] for row in normal] == [72]


def test_authorization_is_rechecked_after_plan_was_previously_valid(tmp_path) -> None:
    state = _manual_state()
    args = _write_state(tmp_path, state)
    order = _manual_order()
    first, _ = _current_frozen_inventory_order_authorization(
        args=args,
        order=order,
        now=NOW,
    )
    assert first is not None

    state["best_quote_frozen_inventory_manual_reduce"]["long"]["consumed"] = True
    _write_state(tmp_path, state)
    second, reason = _current_frozen_inventory_order_authorization(
        args=args,
        order=first,
        now=NOW + timedelta(seconds=1),
    )

    assert second is None
    assert reason["reason"] == "authorization_consumed"


def test_manual_order_cannot_be_placed_again_while_previous_submit_is_pending(
    tmp_path,
) -> None:
    state = _manual_state()
    state["best_quote_frozen_inventory_manual_reduce"]["long"]["submitted"] = True
    args = _write_state(tmp_path, state)

    authorized, reason = _current_frozen_inventory_order_authorization(
        args=args,
        order=_manual_order(),
        operation="place",
        now=NOW,
    )

    assert authorized is None
    assert reason["reason"] == "frozen_inventory_directive_already_submitted"


def test_pair_initial_orders_cannot_be_placed_again_while_fill_reconcile_is_pending(
    tmp_path,
) -> None:
    state = {
        "best_quote_frozen_inventory": {
            "long_qty": 0.4,
            "short_qty": 0.4,
        },
        "best_quote_frozen_inventory_pair_release": {
            "requested": True,
            "request_id": "pair-pending-1",
            "requested_qty": 0.2,
            "requested_at": NOW.isoformat(),
            "expires_at": (NOW + timedelta(minutes=5)).isoformat(),
            "awaiting_fill_confirmation": True,
        },
    }
    args = _write_state(tmp_path, state)
    stale_initial_leg = {
        "book": "frozen_bq",
        "role": "frozen_inventory_pair_release_long",
        "side": "SELL",
        "position_side": "LONG",
        "qty": 0.2,
        "price": 201.0,
        "force_reduce_only": True,
        "execution_type": "maker",
        "time_in_force": "GTX",
        "post_only": True,
        "frozen_inventory_pair_release": True,
        "frozen_inventory_request_id": "pair-pending-1",
    }

    authorized, reason = _current_frozen_inventory_order_authorization(
        args=args,
        order=stale_initial_leg,
        operation="place",
        now=NOW,
    )

    assert authorized is None
    assert reason["reason"] == "frozen_inventory_pair_awaiting_reconcile"


def test_pair_repair_requires_current_missing_side_and_explicit_marker(tmp_path) -> None:
    state = {
        "best_quote_frozen_inventory": {
            "long_qty": 0.3,
            "long_lots": [{"qty": 0.3, "entry_price": 200.0}],
            "short_qty": 0.4,
            "short_lots": [{"qty": 0.4, "entry_price": 201.0}],
        },
        "best_quote_frozen_inventory_pair_release": {
            "requested": True,
            "request_id": "pair-repair-1",
            "requested_qty": 0.2,
            "requested_at": NOW.isoformat(),
            "expires_at": (NOW + timedelta(minutes=5)).isoformat(),
            "filled_long_qty": 0.1,
            "filled_short_qty": 0.0,
            "paired_released_qty": 0.0,
            "repair_side": "short",
        },
    }
    args = _write_state(tmp_path, state)
    repair = {
        "book": "frozen_bq",
        "role": "frozen_inventory_pair_release_short",
        "side": "BUY",
        "position_side": "SHORT",
        "qty": 0.1,
        "price": 200.0,
        "force_reduce_only": True,
        "execution_type": "maker",
        "time_in_force": "GTX",
        "post_only": True,
        "frozen_inventory_pair_release": True,
        "frozen_inventory_request_id": "pair-repair-1",
        "frozen_inventory_pair_repair_side": "SHORT",
    }

    authorized, reason = _current_frozen_inventory_order_authorization(
        args=args,
        order=repair,
        now=NOW,
    )

    assert authorized is not None
    assert reason["reason"] == "authorized"


def test_pair_repair_rejects_accidental_single_leg_without_repair_marker(tmp_path) -> None:
    state = {
        "best_quote_frozen_inventory": {
            "long_qty": 0.3,
            "short_qty": 0.4,
        },
        "best_quote_frozen_inventory_pair_release": {
            "requested": True,
            "request_id": "pair-repair-2",
            "requested_qty": 0.2,
            "expires_at": (NOW + timedelta(minutes=5)).isoformat(),
            "filled_long_qty": 0.1,
            "filled_short_qty": 0.0,
            "repair_side": "short",
        },
    }
    args = _write_state(tmp_path, state)
    accidental_leg = {
        "role": "frozen_inventory_pair_release_short",
        "side": "BUY",
        "position_side": "SHORT",
        "qty": 0.1,
        "force_reduce_only": True,
        "execution_type": "maker",
        "time_in_force": "GTX",
        "post_only": True,
        "frozen_inventory_pair_release": True,
        "frozen_inventory_request_id": "pair-repair-2",
    }

    authorized, reason = _current_frozen_inventory_order_authorization(
        args=args,
        order=accidental_leg,
        now=NOW,
    )

    assert authorized is None
    assert reason["reason"] == "frozen_inventory_pair_repair_mismatch"
