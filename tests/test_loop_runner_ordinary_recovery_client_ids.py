from __future__ import annotations

from argparse import Namespace
from dataclasses import replace
from datetime import datetime, timezone
from unittest.mock import patch

import pytest

from grid_optimizer.futures_managed_order_manifest import (
    build_managed_order_client_order_id,
    ordinary_recovery_client_order_prefix,
)
from grid_optimizer.futures_recovery_coordinator import (
    ActionId,
    FlowBlockerAssessment,
    FuturesRecoveryDecisionEngine,
    OrderRole,
    RecoveryState,
    Side,
    SymbolSnapshot,
)
from grid_optimizer.loop_runner import (
    _build_recovery_aware_client_order_id,
    _guard_recovery_profile_order_before_submit,
    _load_recovery_order_profile_gate,
    _strategy_client_order_prefix,
)


NOW = datetime(2026, 7, 16, 5, 0, tzinfo=timezone.utc)
BASELINE = {
    "best_quote_maker_volume_allow_loss_reduce_only": False,
    "best_quote_maker_volume_net_loss_reduce_enabled": False,
    "hard_loss_forced_reduce_enabled": False,
    "volatility_entry_pause_enabled": True,
}


def _active_state(action: ActionId, side: Side) -> RecoveryState:
    if action is ActionId.INVENTORY_RECOVER:
        assessment = FlowBlockerAssessment(inventory_reduce_sides=(side,))
    elif action is ActionId.MAKER_FLOW_RECOVER:
        assessment = FlowBlockerAssessment(missing_entry_sides=(side,))
    elif action is ActionId.BASELINE_TUNE:
        initial = replace(
            RecoveryState.initial("ARXUSDT", BASELINE, now=NOW).with_exhausted_attempt(
                ActionId.MAKER_FLOW_RECOVER,
                side,
                OrderRole.ENTRY,
                now=NOW,
            ),
            active_episode_fingerprint=f"baseline-{side.value}",
        )
        return FuturesRecoveryDecisionEngine().plan_round(
            snapshot=SymbolSnapshot(
                symbol=initial.symbol,
                captured_at=NOW,
                assessment=FlowBlockerAssessment(
                    missing_entry_sides=(side,),
                    episode_fingerprint=f"baseline-{side.value}",
                ),
            ),
            state=initial,
            now=NOW,
            round_id=f"enter-{action.value}-{side.value.lower()}",
        ).next_state
    else:
        raise AssertionError(action)
    initial = RecoveryState.initial("ARXUSDT", BASELINE, now=NOW)
    return FuturesRecoveryDecisionEngine().plan_round(
        snapshot=SymbolSnapshot(
            symbol=initial.symbol,
            captured_at=NOW,
            assessment=assessment,
        ),
        state=initial,
        now=NOW,
        round_id=f"enter-{action.value}-{side.value.lower()}",
    ).next_state


def _args(state: RecoveryState, tmp_path) -> Namespace:
    args = Namespace(
        symbol=state.symbol,
        recovery_control_path=str(tmp_path / "control.json"),
        recovery_generation=state.generation,
    )
    for key, value in state.desired_profile.fields.items():
        if not key.startswith("recovery_"):
            setattr(args, key, value)
    return args


def _allowed_order(state: RecoveryState) -> dict[str, object]:
    if state.order_role is OrderRole.ENTRY:
        role = (
            "best_quote_entry_long"
            if state.side is Side.BUY
            else "best_quote_entry_short"
        )
    else:
        role = (
            "best_quote_reduce_long"
            if state.side is Side.SELL
            else "best_quote_reduce_short"
        )
    return {
        "role": role,
        "side": state.side.value,
        "execution_type": "post_only",
        "order_type": "LIMIT",
        "time_in_force": "GTX",
        "qty": 10.0,
        "price": 1.0,
    }


def test_ordinary_recovery_prefix_binds_full_identity_and_is_not_symbol_wide() -> None:
    binding = {
        "symbol": "ARXUSDT",
        "generation": 17,
        "decision_id": "ARXUSDT:decision-17",
        "profile_digest": "a" * 64,
        "action_id": ActionId.BASELINE_TUNE.value,
        "side": Side.SELL.value,
        "order_role": OrderRole.REDUCE_ONLY.value,
    }
    prefix = ordinary_recovery_client_order_prefix(**binding)

    assert prefix.startswith(_strategy_client_order_prefix("ARXUSDT"))
    assert prefix != _strategy_client_order_prefix("ARXUSDT")
    assert len(prefix) <= 30
    for key, replacement in {
        "symbol": "OUSDT",
        "generation": 18,
        "decision_id": "ARXUSDT:decision-18",
        "profile_digest": "b" * 64,
        "action_id": ActionId.INVENTORY_RECOVER.value,
        "side": Side.BUY.value,
        "order_role": OrderRole.ENTRY.value,
    }.items():
        changed = {**binding, key: replacement}
        assert ordinary_recovery_client_order_prefix(**changed) != prefix, key

    first = build_managed_order_client_order_id(
        prefix=prefix,
        order_index=1,
        nonce_ns=101,
    )
    second = build_managed_order_client_order_id(
        prefix=prefix,
        order_index=1,
        nonce_ns=102,
    )
    assert first.startswith(prefix)
    assert second.startswith(prefix)
    assert first != second
    assert len(first) <= 36
    assert len(second) <= 36
    with pytest.raises(ValueError):
        build_managed_order_client_order_id(
            prefix=_strategy_client_order_prefix("ARXUSDT"),
            order_index=1,
            nonce_ns=103,
        )
    with pytest.raises(ValueError):
        ordinary_recovery_client_order_prefix(
            **{
                **binding,
                "symbol": "A" * 25 + "USDT",
            }
        )


@pytest.mark.parametrize(
    ("action", "side"),
    (
        (ActionId.INVENTORY_RECOVER, Side.SELL),
        (ActionId.MAKER_FLOW_RECOVER, Side.BUY),
        (ActionId.BASELINE_TUNE, Side.SELL),
    ),
)
def test_submit_refence_injects_bound_prefix_only_for_active_ordinary_recovery(
    tmp_path,
    action: ActionId,
    side: Side,
) -> None:
    state = _active_state(action, side)
    args = _args(state, tmp_path)
    order = _allowed_order(state)
    with patch("grid_optimizer.loop_runner.JsonRecoveryStore.read", return_value=state):
        planned = _load_recovery_order_profile_gate(args, now=NOW)
        guarded, reason = _guard_recovery_profile_order_before_submit(
            args=args,
            order=order,
            expected_gate=planned.as_dict(),
            now=NOW,
        )

    assert reason is None
    assert guarded is not None
    prefix = ordinary_recovery_client_order_prefix(
        symbol=state.symbol,
        generation=state.generation,
        decision_id=str(state.decision_id),
        profile_digest=state.desired_profile.digest,
        action_id=state.active_action.value,
        side=state.side.value,
        order_role=state.order_role.value,
    )
    assert guarded["_ordinary_recovery_client_order_prefix"] == prefix
    assert guarded["order_type"] == "LIMIT"
    assert guarded["time_in_force"] == "GTX"
    assert guarded["execution_type"] == "post_only"
    client_order_id = _build_recovery_aware_client_order_id(
        symbol=state.symbol,
        role=str(guarded["role"]),
        index=1,
        order=guarded,
        nonce_ns=103,
    )
    assert client_order_id.startswith(prefix)
    assert len(client_order_id) <= 36


def test_unregistered_and_registered_stable_orders_keep_default_identity(
    tmp_path,
) -> None:
    order = {
        "role": "best_quote_entry_long",
        "side": "BUY",
        "execution_type": "post_only",
        "order_type": "LIMIT",
        "time_in_force": "GTX",
    }
    unmanaged, reason = _guard_recovery_profile_order_before_submit(
        args=Namespace(symbol="ARXUSDT"),
        order=order,
        expected_gate=None,
        now=NOW,
    )
    assert reason is None
    assert unmanaged == order
    assert "_ordinary_recovery_client_order_prefix" not in unmanaged

    stable = RecoveryState.initial("ARXUSDT", BASELINE, now=NOW)
    args = _args(stable, tmp_path)
    with patch("grid_optimizer.loop_runner.JsonRecoveryStore.read", return_value=stable):
        gate = _load_recovery_order_profile_gate(args, now=NOW)
        registered, reason = _guard_recovery_profile_order_before_submit(
            args=args,
            order=order,
            expected_gate=gate.as_dict(),
            now=NOW,
        )
    assert reason is None
    assert registered == order
    assert "_ordinary_recovery_client_order_prefix" not in registered

    with patch(
        "grid_optimizer.loop_runner._build_client_order_id",
        return_value="legacy-client-order-id",
    ) as legacy_builder:
        client_order_id = _build_recovery_aware_client_order_id(
            symbol="ARXUSDT",
            role="best_quote_entry_long",
            index=3,
            order=registered,
            nonce_ns=999,
        )
    assert client_order_id == "legacy-client-order-id"
    legacy_builder.assert_called_once_with(
        symbol="ARXUSDT",
        role="best_quote_entry_long",
        index=3,
    )


def test_changed_decision_is_rejected_before_bound_prefix_is_injected(tmp_path) -> None:
    state = _active_state(ActionId.MAKER_FLOW_RECOVER, Side.BUY)
    args = _args(state, tmp_path)
    with patch("grid_optimizer.loop_runner.JsonRecoveryStore.read", return_value=state):
        planned = _load_recovery_order_profile_gate(args, now=NOW)
    changed = replace(state, decision_id=f"{state.decision_id}:changed")

    with patch("grid_optimizer.loop_runner.JsonRecoveryStore.read", return_value=changed):
        guarded, reason = _guard_recovery_profile_order_before_submit(
            args=args,
            order=_allowed_order(state),
            expected_gate=planned.as_dict(),
            now=NOW,
        )

    assert guarded is None
    assert reason is not None
    assert reason["reason"] == "recovery_profile_not_authorized"


def test_registration_after_unmanaged_plan_is_rejected_at_submit_refence(
    tmp_path,
) -> None:
    state = _active_state(ActionId.INVENTORY_RECOVER, Side.SELL)
    args = _args(state, tmp_path)

    with patch("grid_optimizer.loop_runner.JsonRecoveryStore.read", return_value=state):
        guarded, reason = _guard_recovery_profile_order_before_submit(
            args=args,
            order=_allowed_order(state),
            expected_gate=None,
            now=NOW,
        )

    assert guarded is None
    assert reason is not None
    assert reason["reason"] == "recovery_profile_not_authorized"
    assert reason["gate_reason"] == "recovery_registered_after_plan"
