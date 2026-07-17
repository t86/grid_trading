from __future__ import annotations

from argparse import Namespace
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
from unittest.mock import patch

import pytest

from grid_optimizer.futures_managed_order_manifest import (
    build_managed_order_client_order_id,
    ordinary_recovery_client_order_prefix,
)
from grid_optimizer.futures_recovery_coordinator import (
    ActionLease,
    ActionId,
    CleanupObligation,
    FlowBlockerAssessment,
    FuturesRecoveryDecisionEngine,
    ManagedOrderIdentity,
    ManagedOrderManifest,
    OrderRole,
    RecoveryPolicy,
    RecoveryState,
    SafetyLease,
    Side,
    SymbolSnapshot,
)
from grid_optimizer.futures_recovery_runtime_adapter import (
    InventoryRecoveryReceiptJournalError,
    OrdinaryRecoveryRuntimeEvidence,
    load_inventory_recovery_runtime_evidence,
    load_ordinary_recovery_runtime_evidence,
    ordinary_recovery_fill_progress_receipts,
)
from grid_optimizer.loop_runner import (
    INVENTORY_RECOVERY_RUNNER_RECEIPTS_KEY,
    _apply_recovery_profile_gate_to_actions,
    _guard_recovery_profile_order_before_submit,
    _load_recovery_order_profile_gate,
    _maintain_normal_recovery_profile_gate,
    _record_ordinary_recovery_progress,
    _record_inventory_recovery_progress,
    apply_recovery_profile_gate_to_plan,
)


NOW = datetime(2026, 7, 16, 3, 0, tzinfo=timezone.utc)
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
    else:
        raise AssertionError(action)
    state = RecoveryState.initial("ARXUSDT", BASELINE, now=NOW)
    return FuturesRecoveryDecisionEngine(
        policy=RecoveryPolicy(
            action_progress_timeout=timedelta(seconds=30),
            action_hard_ttl=timedelta(seconds=60),
        )
    ).plan_round(
        snapshot=SymbolSnapshot(
            symbol=state.symbol,
            captured_at=NOW,
            assessment=assessment,
        ),
        state=state,
        now=NOW,
        round_id=f"enter-{action.value}-{side.value.lower()}",
    ).next_state


def _baseline_tune_entry_state(side: Side) -> RecoveryState:
    initial = replace(
        RecoveryState.initial("ARXUSDT", BASELINE, now=NOW).with_exhausted_attempt(
            ActionId.MAKER_FLOW_RECOVER,
            side,
            OrderRole.ENTRY,
            now=NOW,
        ),
        active_episode_fingerprint=f"baseline-entry-{side.value}",
    )
    return FuturesRecoveryDecisionEngine().plan_round(
        snapshot=SymbolSnapshot(
            symbol=initial.symbol,
            captured_at=NOW,
            assessment=FlowBlockerAssessment(
                missing_entry_sides=(side,),
                episode_fingerprint=f"baseline-entry-{side.value}",
            ),
        ),
        state=initial,
        now=NOW,
        round_id=f"baseline-entry-{side.value.lower()}",
    ).next_state


def _args(state: RecoveryState, tmp_path) -> Namespace:
    args = Namespace(
        symbol=state.symbol,
        recovery_control_path=str(tmp_path / "control.json"),
        recovery_generation=state.generation,
        state_path=str(tmp_path / "runner-state.json"),
        best_quote_maker_volume_allow_loss_reduce_only=False,
        best_quote_maker_volume_net_loss_reduce_enabled=False,
        hard_loss_forced_reduce_enabled=False,
        volatility_entry_pause_enabled=True,
    )
    for key, value in state.desired_profile.fields.items():
        if not key.startswith("recovery_"):
            setattr(args, key, value)
    return args


def _install_frozen_short_request(
    args: Namespace,
    *,
    request_id: str,
    order_id: int = 88,
) -> tuple[dict[str, object], dict[str, object]]:
    client_order_id = f"gx-arxu-frozenin-1-{order_id}"
    frozen_place: dict[str, object] = {
        "book": "frozen_bq",
        "role": "frozen_inventory_manual_reduce_short",
        "side": "BUY",
        "position_side": "SHORT",
        "qty": 1.0,
        "price": 0.99,
        "force_reduce_only": True,
        "execution_type": "maker",
        "time_in_force": "GTX",
        "post_only": True,
        "manual_frozen_inventory_reduce": True,
        "frozen_inventory_request_id": request_id,
        "frozen_inventory_authorization_validated": True,
    }
    frozen_cancel: dict[str, object] = {
        **frozen_place,
        "orderId": order_id,
        "clientOrderId": client_order_id,
        "origQty": "1.0",
        "executedQty": "0",
    }
    Path(args.state_path).write_text(
        json.dumps(
            {
                "best_quote_frozen_inventory": {
                    "long_qty": 0.0,
                    "long_lots": [],
                    "short_qty": 2.0,
                    "short_lots": [{"qty": 2.0, "entry_price": 1.0}],
                },
                "best_quote_frozen_inventory_manual_reduce": {
                    "short": {
                        "requested": True,
                        "request_id": request_id,
                        "requested_qty": 1.0,
                        "requested_at": NOW.isoformat(),
                        "expires_at": (NOW + timedelta(minutes=5)).isoformat(),
                    }
                },
                "best_quote_volume_order_refs": {
                    str(order_id): {
                        "book": "frozen_bq",
                        "role": "frozen_inventory_manual_reduce_short",
                        "side": "BUY",
                        "position_side": "SHORT",
                        "client_order_id": client_order_id,
                        "frozen_inventory_request_id": request_id,
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    return frozen_place, frozen_cancel


def _ordinary_client_order_id(
    state: RecoveryState,
    *,
    order_index: int,
    nonce_ns: int,
) -> str:
    assert state.decision_id is not None
    assert state.side is not None
    assert state.order_role is not None
    return build_managed_order_client_order_id(
        prefix=ordinary_recovery_client_order_prefix(
            symbol=state.symbol,
            generation=state.generation,
            decision_id=state.decision_id,
            profile_digest=state.desired_profile.digest,
            action_id=state.active_action.value,
            side=state.side.value,
            order_role=state.order_role.value,
        ),
        order_index=order_index,
        nonce_ns=nonce_ns,
    )


def _orders() -> list[dict[str, object]]:
    return [
        {
            "role": "best_quote_reduce_long",
            "side": "SELL",
            "execution_type": "post_only",
            "qty": 10.0,
            "price": 1.01,
        },
        {
            "role": "best_quote_reduce_short",
            "side": "BUY",
            "execution_type": "post_only",
            "qty": 10.0,
            "price": 0.99,
        },
        {
            "role": "best_quote_entry_long",
            "side": "BUY",
            "execution_type": "post_only",
            "qty": 10.0,
            "price": 0.99,
        },
        {
            "role": "best_quote_entry_short",
            "side": "SELL",
            "execution_type": "post_only",
            "qty": 10.0,
            "price": 1.01,
        },
    ]


def test_inventory_profile_gate_keeps_only_selected_normal_bq_reduce(tmp_path) -> None:
    state = _active_state(ActionId.INVENTORY_RECOVER, Side.SELL)
    args = _args(state, tmp_path)
    with patch("grid_optimizer.loop_runner.JsonRecoveryStore.read", return_value=state):
        gate = _load_recovery_order_profile_gate(args, now=NOW + timedelta(seconds=1))
    plan = {
        "bootstrap_orders": [],
        "buy_orders": [dict(_orders()[1]), dict(_orders()[2])],
        "sell_orders": [dict(_orders()[0]), dict(_orders()[3])],
    }

    report = apply_recovery_profile_gate_to_plan(plan=plan, gate=gate)

    assert report["ready"] is True
    assert report["active_action"] == "inventory_recover"
    assert report["allowed_roles"] == ["best_quote_reduce_long"]
    assert [row["role"] for row in plan["sell_orders"]] == [
        "best_quote_reduce_long"
    ]
    assert plan["buy_orders"] == []
    assert report["dropped_order_count"] == 3


def test_inventory_profile_gate_preserves_authorized_frozen_order_outside_ordinary_scope(
    tmp_path,
) -> None:
    state = _active_state(ActionId.INVENTORY_RECOVER, Side.SELL)
    args = _args(state, tmp_path)
    frozen_order = {
        "book": "frozen_bq",
        "role": "frozen_inventory_manual_reduce_short",
        "side": "BUY",
        "position_side": "SHORT",
        "qty": 1.0,
        "force_reduce_only": True,
        "execution_type": "maker",
        "time_in_force": "GTX",
        "post_only": True,
        "manual_frozen_inventory_reduce": True,
        "frozen_inventory_request_id": "frozen-request-1",
        "frozen_inventory_authorization_validated": True,
    }
    with patch("grid_optimizer.loop_runner.JsonRecoveryStore.read", return_value=state):
        gate = _load_recovery_order_profile_gate(args, now=NOW + timedelta(seconds=1))
    plan = {
        "bootstrap_orders": [],
        "buy_orders": [dict(_orders()[1]), frozen_order],
        "sell_orders": [dict(_orders()[0])],
    }

    report = apply_recovery_profile_gate_to_plan(plan=plan, gate=gate)

    assert plan["buy_orders"] == [frozen_order]
    assert plan["sell_orders"] == [dict(_orders()[0])]
    assert report["preserved_frozen_order_count"] == 1


def test_maker_flow_profile_gate_keeps_only_selected_entry_side(tmp_path) -> None:
    state = _active_state(ActionId.MAKER_FLOW_RECOVER, Side.BUY)
    args = _args(state, tmp_path)
    with patch("grid_optimizer.loop_runner.JsonRecoveryStore.read", return_value=state):
        gate = _load_recovery_order_profile_gate(args, now=NOW + timedelta(seconds=1))
    plan = {
        "bootstrap_orders": [],
        "buy_orders": [dict(_orders()[1]), dict(_orders()[2])],
        "sell_orders": [dict(_orders()[0]), dict(_orders()[3])],
    }

    report = apply_recovery_profile_gate_to_plan(plan=plan, gate=gate)

    assert report["active_action"] == "maker_flow_recover"
    assert report["allowed_roles"] == ["best_quote_entry_long"]
    assert [row["role"] for row in plan["buy_orders"]] == [
        "best_quote_entry_long"
    ]
    assert plan["sell_orders"] == []


def test_loss_budget_baseline_tune_gate_keeps_only_no_loss_gtx_reduce(
    tmp_path,
) -> None:
    assessment = FlowBlockerAssessment(
        inventory_reduce_sides=(Side.SELL,),
        loss_only_blocked_sides=(Side.SELL,),
        episode_fingerprint="baseline-tune-gate",
    )
    initial = RecoveryState.initial("ARXUSDT", BASELINE, now=NOW)
    initial = initial.with_exhausted_attempt(
        ActionId.INVENTORY_RECOVER,
        Side.SELL,
        OrderRole.REDUCE_ONLY,
        now=NOW,
    ).with_temporary_loss_lease_use("baseline-tune-gate", count=1)
    state = FuturesRecoveryDecisionEngine(
        policy=RecoveryPolicy(max_temporary_loss_leases_per_episode=1)
    ).plan_round(
        snapshot=SymbolSnapshot(
            symbol=initial.symbol,
            captured_at=NOW,
            assessment=assessment,
        ),
        state=initial,
        now=NOW,
        round_id="baseline-tune-gate-enter",
    ).next_state
    args = _args(state, tmp_path)
    plan = {
        "bootstrap_orders": [],
        "buy_orders": [dict(_orders()[1]), dict(_orders()[2])],
        "sell_orders": [dict(_orders()[0]), dict(_orders()[3])],
    }

    with patch("grid_optimizer.loop_runner.JsonRecoveryStore.read", return_value=state):
        gate = _load_recovery_order_profile_gate(
            args,
            now=NOW + timedelta(seconds=1),
        )
    report = apply_recovery_profile_gate_to_plan(plan=plan, gate=gate)

    assert report["ready"] is True
    assert report["active_action"] == "baseline_tune"
    assert report["allowed_roles"] == ["best_quote_reduce_long"]
    assert [row["role"] for row in plan["sell_orders"]] == [
        "best_quote_reduce_long"
    ]
    assert plan["buy_orders"] == []
    assert state.desired_profile.fields[
        "best_quote_maker_volume_allow_loss_reduce_only"
    ] is False


def test_old_generation_is_fail_closed_before_normal_plan(tmp_path) -> None:
    state = _active_state(ActionId.INVENTORY_RECOVER, Side.SELL)
    args = _args(state, tmp_path)
    args.recovery_generation -= 1
    with patch("grid_optimizer.loop_runner.JsonRecoveryStore.read", return_value=state):
        maintenance = _maintain_normal_recovery_profile_gate(
            args,
            state_path=tmp_path / "runner-state.json",
            now=NOW + timedelta(seconds=1),
        )

    assert maintenance is not None
    assert maintenance["skip_normal_cycle"] is True
    assert maintenance["reason"] == "recovery_generation_mismatch"


def test_current_inventory_gtx_acceptance_writes_idempotent_progress(tmp_path) -> None:
    state = _active_state(ActionId.INVENTORY_RECOVER, Side.SELL)
    client_order_id = _ordinary_client_order_id(
        state,
        order_index=1,
        nonce_ns=801,
    )
    args = _args(state, tmp_path)
    state_path = tmp_path / "runner-state.json"
    state_path.write_text("{}", encoding="utf-8")
    with patch("grid_optimizer.loop_runner.JsonRecoveryStore.read", return_value=state):
        gate = _load_recovery_order_profile_gate(args, now=NOW + timedelta(seconds=1))
        plan = {
            "bootstrap_orders": [],
            "buy_orders": [],
            "sell_orders": [dict(_orders()[0])],
        }
        planned_gate = apply_recovery_profile_gate_to_plan(plan=plan, gate=gate)
        submit_report = {
            "placed_orders": [
                {
                    "request": {
                        "role": "best_quote_reduce_long",
                        "side": "SELL",
                        "order_type": "LIMIT",
                        "time_in_force": "GTX",
                    },
                    "response": {
                        "orderId": 801,
                        "clientOrderId": client_order_id,
                        "status": "NEW",
                    },
                }
            ]
        }
        first = _record_inventory_recovery_progress(
            args,
            state_path=state_path,
            expected_gate=planned_gate,
            submit_report=submit_report,
            now=NOW + timedelta(seconds=2),
        )
        second = _record_inventory_recovery_progress(
            args,
            state_path=state_path,
            expected_gate=planned_gate,
            submit_report=submit_report,
            now=NOW + timedelta(seconds=3),
        )

    assert first is not None
    assert second is not None
    assert second["receipt_id"] == first["receipt_id"]
    document = json.loads(state_path.read_text(encoding="utf-8"))
    journal = document[INVENTORY_RECOVERY_RUNNER_RECEIPTS_KEY]
    assert len(journal["events"]) == 1
    evidence = load_inventory_recovery_runtime_evidence(
        state_path=state_path,
        recovery_state=state,
        captured_at=NOW + timedelta(seconds=4),
    )
    # Exchange acceptance proves a submit/manifest only.  It is not a
    # userTrades fill and therefore cannot complete the recovery action.
    assert evidence.progress_receipts == ()
    assert evidence.accepted_order_ids == ("801",)
    assert evidence.accepted_client_order_ids == (client_order_id,)
    assert evidence.accepted_order_pairs == (("801", client_order_id),)


def test_ordinary_progress_rejects_non_decision_client_order_id(tmp_path) -> None:
    state = _active_state(ActionId.INVENTORY_RECOVER, Side.SELL)
    args = _args(state, tmp_path)
    state_path = tmp_path / "runner-state.json"
    state_path.write_text("{}", encoding="utf-8")

    with patch("grid_optimizer.loop_runner.JsonRecoveryStore.read", return_value=state):
        gate = _load_recovery_order_profile_gate(
            args,
            now=NOW + timedelta(seconds=1),
        )
        with pytest.raises(RuntimeError, match="outside its decision prefix"):
            _record_inventory_recovery_progress(
                args,
                state_path=state_path,
                expected_gate=gate.as_dict(),
                submit_report={
                    "placed_orders": [
                        {
                            "request": {
                                "role": "best_quote_reduce_long",
                                "side": "SELL",
                                "order_type": "LIMIT",
                                "time_in_force": "GTX",
                            },
                            "response": {
                                "orderId": 899,
                                "clientOrderId": "manual-same-side-gtx",
                                "status": "NEW",
                            },
                        }
                    ]
                },
                now=NOW + timedelta(seconds=2),
            )

    assert INVENTORY_RECOVERY_RUNNER_RECEIPTS_KEY not in json.loads(
        state_path.read_text(encoding="utf-8")
    )


@pytest.mark.parametrize(
    ("state", "order_request", "order_id"),
    (
        (
            _active_state(ActionId.MAKER_FLOW_RECOVER, Side.BUY),
            {
                "role": "best_quote_entry_long",
                "side": "BUY",
                "time_in_force": "GTX",
                "type": "LIMIT",
            },
            811,
        ),
        (
            _baseline_tune_entry_state(Side.SELL),
            {
                "role": "best_quote_entry_short",
                "side": "SELL",
                "time_in_force": "GTX",
                "type": "LIMIT",
            },
            812,
        ),
    ),
)
def test_ordinary_entry_progress_requires_current_exchange_accepted_limit_gtx(
    tmp_path,
    state: RecoveryState,
    order_request: dict[str, object],
    order_id: int,
) -> None:
    args = _args(state, tmp_path)
    for key, value in state.desired_profile.fields.items():
        if not key.startswith("recovery_"):
            setattr(args, key, value)
    state_path = tmp_path / "runner-state.json"
    state_path.write_text("{}", encoding="utf-8")
    client_order_id = _ordinary_client_order_id(
        state,
        order_index=1,
        nonce_ns=order_id,
    )
    with patch("grid_optimizer.loop_runner.JsonRecoveryStore.read", return_value=state):
        gate = _load_recovery_order_profile_gate(args, now=NOW + timedelta(seconds=1))
        receipt = _record_ordinary_recovery_progress(
            args,
            state_path=state_path,
            expected_gate=gate.as_dict(),
            submit_report={
                "placed_orders": [
                    {
                        "request": order_request,
                        "response": {
                            "orderId": order_id,
                            "clientOrderId": client_order_id,
                            "status": "NEW",
                        },
                    }
                ]
            },
            now=NOW + timedelta(seconds=2),
        )

    assert receipt is not None
    assert receipt["action_id"] == state.active_action.value
    evidence = load_ordinary_recovery_runtime_evidence(
        state_path=state_path,
        recovery_state=state,
        captured_at=NOW + timedelta(seconds=3),
    )
    assert evidence.progress_receipts == ()
    assert evidence.accepted_order_ids == (str(order_id),)
    assert evidence.accepted_order_pairs == (
        (str(order_id), client_order_id),
    )


def test_ordinary_fill_progress_requires_current_exact_manifest_pair() -> None:
    state = _active_state(ActionId.MAKER_FLOW_RECOVER, Side.BUY)
    client_order_id = _ordinary_client_order_id(
        state,
        order_index=1,
        nonce_ns=811,
    )
    evidence = OrdinaryRecoveryRuntimeEvidence(
        accepted_order_ids=("811",),
        accepted_client_order_ids=(client_order_id,),
        accepted_order_pairs=(("811", client_order_id),),
    )
    filled_at = NOW + timedelta(seconds=3)

    rejected = ordinary_recovery_fill_progress_receipts(
        recovery_state=state,
        runtime_evidence=evidence,
        trade_rows=(
            {
                "id": 9000,
                "orderId": 811,
                "clientOrderId": "wrong-owner",
                "symbol": "ARXUSDT",
                "side": "BUY",
                "positionSide": "LONG",
                "time": int(filled_at.timestamp() * 1000),
                "qty": "1",
            },
        ),
        captured_at=filled_at,
    )
    assert rejected == ()

    receipts = ordinary_recovery_fill_progress_receipts(
        recovery_state=state,
        runtime_evidence=evidence,
        trade_rows=(
            {
                "id": 9001,
                "orderId": 811,
                "clientOrderId": client_order_id,
                "symbol": "ARXUSDT",
                "side": "BUY",
                "positionSide": "LONG",
                "time": int(filled_at.timestamp() * 1000),
                "qty": "1",
            },
        ),
        captured_at=filled_at,
    )

    assert len(receipts) == 1
    receipt = receipts[0]
    assert receipt.decision_id == state.decision_id
    assert receipt.generation == state.generation
    assert receipt.side is Side.BUY
    assert receipt.order_role is OrderRole.ENTRY
    assert receipt.observation_seq == 9001
    assert receipt.observed_at == filled_at

    assert state.progress_deadline_at is not None
    late = state.progress_deadline_at
    assert ordinary_recovery_fill_progress_receipts(
        recovery_state=state,
        runtime_evidence=evidence,
        trade_rows=(
            {
                "id": 9002,
                "orderId": 811,
                "clientOrderId": client_order_id,
                "symbol": "ARXUSDT",
                "side": "BUY",
                "positionSide": "LONG",
                "time": int(late.timestamp() * 1000),
                "qty": "1",
            },
        ),
        captured_at=late,
    ) == ()


def test_ordinary_manifest_preserves_order_to_client_pairing(tmp_path) -> None:
    state = _active_state(ActionId.MAKER_FLOW_RECOVER, Side.BUY)
    args = _args(state, tmp_path)
    state_path = tmp_path / "runner-state.json"
    state_path.write_text("{}", encoding="utf-8")
    order_pairs = (
        (2, _ordinary_client_order_id(state, order_index=1, nonce_ns=2)),
        (10, _ordinary_client_order_id(state, order_index=2, nonce_ns=10)),
    )
    with patch("grid_optimizer.loop_runner.JsonRecoveryStore.read", return_value=state):
        gate = _load_recovery_order_profile_gate(
            args,
            now=NOW + timedelta(seconds=1),
        )
        _record_ordinary_recovery_progress(
            args,
            state_path=state_path,
            expected_gate=gate.as_dict(),
            submit_report={
                "placed_orders": [
                    {
                        "request": {
                            "role": "best_quote_entry_long",
                            "side": "BUY",
                            "order_type": "LIMIT",
                            "time_in_force": "GTX",
                        },
                        "response": {
                            "orderId": order_id,
                            "clientOrderId": client_order_id,
                            "status": "NEW",
                        },
                    }
                    for order_id, client_order_id in order_pairs
                ]
            },
            now=NOW + timedelta(seconds=2),
        )

    evidence = load_ordinary_recovery_runtime_evidence(
        state_path=state_path,
        recovery_state=state,
        captured_at=NOW + timedelta(seconds=3),
    )

    assert evidence.accepted_order_pairs == (
        ("10", order_pairs[1][1]),
        ("2", order_pairs[0][1]),
    )


def test_plan_admission_or_non_limit_order_cannot_fake_maker_progress(tmp_path) -> None:
    state = _active_state(ActionId.MAKER_FLOW_RECOVER, Side.BUY)
    args = _args(state, tmp_path)
    for key, value in state.desired_profile.fields.items():
        if not key.startswith("recovery_"):
            setattr(args, key, value)
    state_path = tmp_path / "runner-state.json"
    state_path.write_text("{}", encoding="utf-8")
    with patch("grid_optimizer.loop_runner.JsonRecoveryStore.read", return_value=state):
        gate = _load_recovery_order_profile_gate(args, now=NOW + timedelta(seconds=1))
        no_submit = _record_ordinary_recovery_progress(
            args,
            state_path=state_path,
            expected_gate=gate.as_dict(),
            submit_report={
                "recovery_profile_execution": {"authorized": True},
                "placed_orders": [],
            },
            now=NOW + timedelta(seconds=2),
        )
        market_submit = _record_ordinary_recovery_progress(
            args,
            state_path=state_path,
            expected_gate=gate.as_dict(),
            submit_report={
                "placed_orders": [
                    {
                        "request": {
                            "role": "best_quote_entry_long",
                            "side": "BUY",
                            "time_in_force": "GTX",
                            "type": "MARKET",
                        },
                        "response": {
                            "orderId": 813,
                            "clientOrderId": "ordinary-813",
                            "status": "FILLED",
                        },
                    }
                ]
            },
            now=NOW + timedelta(seconds=3),
        )

    assert no_submit is None
    assert market_submit is None
    assert INVENTORY_RECOVERY_RUNNER_RECEIPTS_KEY not in json.loads(
        state_path.read_text(encoding="utf-8")
    )


def test_ordinary_adapter_rejects_action_fence_tampering(tmp_path) -> None:
    state = _active_state(ActionId.MAKER_FLOW_RECOVER, Side.BUY)
    args = _args(state, tmp_path)
    state_path = tmp_path / "runner-state.json"
    state_path.write_text("{}", encoding="utf-8")
    client_order_id = _ordinary_client_order_id(
        state,
        order_index=1,
        nonce_ns=814,
    )
    with patch("grid_optimizer.loop_runner.JsonRecoveryStore.read", return_value=state):
        gate = _load_recovery_order_profile_gate(args, now=NOW + timedelta(seconds=1))
        receipt = _record_ordinary_recovery_progress(
            args,
            state_path=state_path,
            expected_gate=gate.as_dict(),
            submit_report={
                "placed_orders": [
                    {
                        "request": {
                            "role": "best_quote_entry_long",
                            "side": "BUY",
                            "order_type": "LIMIT",
                            "time_in_force": "GTX",
                        },
                        "response": {
                            "orderId": 814,
                            "clientOrderId": client_order_id,
                            "status": "NEW",
                        },
                    }
                ]
            },
            now=NOW + timedelta(seconds=2),
        )
    assert receipt is not None
    document = json.loads(state_path.read_text(encoding="utf-8"))
    document[INVENTORY_RECOVERY_RUNNER_RECEIPTS_KEY]["events"][0][
        "action_id"
    ] = ActionId.BASELINE_TUNE.value
    state_path.write_text(json.dumps(document), encoding="utf-8")

    with pytest.raises(InventoryRecoveryReceiptJournalError):
        load_ordinary_recovery_runtime_evidence(
            state_path=state_path,
            recovery_state=state,
            captured_at=NOW + timedelta(seconds=3),
        )


def test_no_accepted_gtx_never_creates_synthetic_progress_or_exhausted(tmp_path) -> None:
    state = _active_state(ActionId.INVENTORY_RECOVER, Side.BUY)
    args = _args(state, tmp_path)
    state_path = tmp_path / "runner-state.json"
    state_path.write_text("{}", encoding="utf-8")
    with patch("grid_optimizer.loop_runner.JsonRecoveryStore.read", return_value=state):
        gate = _load_recovery_order_profile_gate(args, now=NOW + timedelta(seconds=1))
        receipt = _record_inventory_recovery_progress(
            args,
            state_path=state_path,
            expected_gate=gate.as_dict(),
            submit_report={"placed_orders": []},
            now=NOW + timedelta(seconds=2),
        )
        maintenance = _maintain_normal_recovery_profile_gate(
            args,
            state_path=state_path,
            now=state.progress_deadline_at,
        )

    assert receipt is None
    assert maintenance is not None
    assert maintenance["action"] == "inventory_recovery_deadline_wait"
    assert maintenance["skip_normal_cycle"] is True
    assert INVENTORY_RECOVERY_RUNNER_RECEIPTS_KEY not in json.loads(
        state_path.read_text(encoding="utf-8")
    )
    assert state.attempt_exhausted(
        ActionId.INVENTORY_RECOVER,
        Side.BUY,
        OrderRole.REDUCE_ONLY,
    ) is False


def test_submit_rechecks_current_generation_and_rejects_old_plan(tmp_path) -> None:
    state = _active_state(ActionId.INVENTORY_RECOVER, Side.SELL)
    args = _args(state, tmp_path)
    with patch("grid_optimizer.loop_runner.JsonRecoveryStore.read", return_value=state):
        planned = _load_recovery_order_profile_gate(
            args,
            now=NOW + timedelta(seconds=1),
        )
    args.recovery_generation += 1
    with patch("grid_optimizer.loop_runner.JsonRecoveryStore.read", return_value=state):
        guarded, reason = _guard_recovery_profile_order_before_submit(
            args=args,
            order=dict(_orders()[0]),
            expected_gate=planned.as_dict(),
            now=NOW + timedelta(seconds=2),
        )

    assert guarded is None
    assert reason["reason"] == "recovery_profile_not_authorized"


def test_execution_gate_drops_all_mutations_when_plan_generation_is_stale(
    tmp_path,
) -> None:
    state = _active_state(ActionId.INVENTORY_RECOVER, Side.SELL)
    args = _args(state, tmp_path)
    with patch("grid_optimizer.loop_runner.JsonRecoveryStore.read", return_value=state):
        planned = _load_recovery_order_profile_gate(
            args,
            now=NOW + timedelta(seconds=1),
        )
    args.recovery_generation += 1
    actions = {
        "place_orders": [dict(_orders()[0])],
        "place_count": 1,
        "place_notional": 10.0,
        "cancel_orders": [{"orderId": 99}],
        "cancel_count": 1,
    }

    with patch("grid_optimizer.loop_runner.JsonRecoveryStore.read", return_value=state):
        filtered, report = _apply_recovery_profile_gate_to_actions(
            args=args,
            actions=actions,
            expected_gate=planned.as_dict(),
            now=NOW + timedelta(seconds=2),
        )

    assert filtered["place_orders"] == []
    assert filtered["cancel_orders"] == []
    assert report["authorized"] is False
    assert report["reason"] == "recovery_generation_mismatch"


def test_execution_gate_preserves_authorized_frozen_mutations_when_ordinary_generation_is_stale(
    tmp_path,
) -> None:
    state = _active_state(ActionId.INVENTORY_RECOVER, Side.SELL)
    args = _args(state, tmp_path)
    with patch("grid_optimizer.loop_runner.JsonRecoveryStore.read", return_value=state):
        planned = _load_recovery_order_profile_gate(
            args,
            now=NOW + timedelta(seconds=1),
        )
    args.recovery_generation += 1
    frozen_place, frozen_cancel = _install_frozen_short_request(
        args,
        request_id="frozen-request-2",
    )
    actions = {
        "place_orders": [dict(_orders()[0]), frozen_place],
        "place_count": 2,
        "cancel_orders": [{"orderId": 99}, frozen_cancel],
        "cancel_count": 2,
    }

    with patch("grid_optimizer.loop_runner.JsonRecoveryStore.read", return_value=state):
        filtered, report = _apply_recovery_profile_gate_to_actions(
            args=args,
            actions=actions,
            expected_gate=planned.as_dict(),
            now=NOW + timedelta(seconds=2),
        )

    assert [row["role"] for row in filtered["place_orders"]] == [
        "frozen_inventory_manual_reduce_short"
    ]
    assert [row["orderId"] for row in filtered["cancel_orders"]] == [88]
    assert report["authorized"] is False
    assert report["preserved_frozen_place_count"] == 1
    assert report["preserved_frozen_cancel_count"] == 1


def test_submit_fence_preserves_authorized_frozen_order_when_ordinary_owner_changed(
    tmp_path,
) -> None:
    state = _active_state(ActionId.INVENTORY_RECOVER, Side.SELL)
    args = _args(state, tmp_path)
    frozen_order, _ = _install_frozen_short_request(
        args,
        request_id="frozen-request-3",
    )

    with patch("grid_optimizer.loop_runner.JsonRecoveryStore.read", return_value=state):
        guarded, reason = _guard_recovery_profile_order_before_submit(
            args=args,
            order=frozen_order,
            expected_gate=None,
            now=NOW + timedelta(seconds=2),
        )

    assert guarded is not None
    assert guarded["frozen_inventory_request_id"] == "frozen-request-3"
    assert guarded["frozen_inventory_authorization_validated"] is True
    assert reason is None


def test_non_gtx_response_cannot_create_inventory_progress(tmp_path) -> None:
    state = _active_state(ActionId.INVENTORY_RECOVER, Side.SELL)
    args = _args(state, tmp_path)
    state_path = tmp_path / "runner-state.json"
    state_path.write_text("{}", encoding="utf-8")
    with patch("grid_optimizer.loop_runner.JsonRecoveryStore.read", return_value=state):
        gate = _load_recovery_order_profile_gate(args, now=NOW + timedelta(seconds=1))
        receipt = _record_inventory_recovery_progress(
            args,
            state_path=state_path,
            expected_gate=gate.as_dict(),
            submit_report={
                "placed_orders": [
                    {
                        "request": {
                            "role": "best_quote_reduce_long",
                            "side": "SELL",
                            "time_in_force": "GTC",
                        },
                        "response": {
                            "orderId": 802,
                            "clientOrderId": "inventory-802",
                            "status": "NEW",
                        },
                    }
                ]
            },
            now=NOW + timedelta(seconds=2),
        )

    assert receipt is None
    assert INVENTORY_RECOVERY_RUNNER_RECEIPTS_KEY not in json.loads(
        state_path.read_text(encoding="utf-8")
    )


def test_cooldown_allows_normal_trading_on_confirmed_baseline(tmp_path) -> None:
    active = _active_state(ActionId.INVENTORY_RECOVER, Side.SELL)
    cooldown = replace(
        active,
        phase=active.phase.COOLDOWN,
        desired_profile=active.baseline_profile,
        progress_deadline_at=None,
        hard_expires_at=None,
        cooldown_until=NOW + timedelta(seconds=90),
    )
    args = _args(cooldown, tmp_path)
    plan = {
        "bootstrap_orders": [],
        "buy_orders": [dict(_orders()[2])],
        "sell_orders": [dict(_orders()[3])],
    }

    with patch(
        "grid_optimizer.loop_runner.JsonRecoveryStore.read",
        return_value=cooldown,
    ):
        gate = _load_recovery_order_profile_gate(
            args,
            now=NOW + timedelta(seconds=31),
        )
    report = apply_recovery_profile_gate_to_plan(plan=plan, gate=gate)

    assert gate.ready is True
    assert report["active_action"] == "noop"
    assert report["dropped_order_count"] == 0
    assert [row["role"] for row in plan["buy_orders"]] == [
        "best_quote_entry_long"
    ]
    assert [row["role"] for row in plan["sell_orders"]] == [
        "best_quote_entry_short"
    ]


def test_safe_restoring_baseline_immediately_allows_normal_trading(
    tmp_path,
) -> None:
    active = _active_state(ActionId.INVENTORY_RECOVER, Side.SELL)
    restoring = replace(
        active,
        phase=active.phase.RESTORING,
        desired_profile=active.baseline_profile,
        desired_runner_state="running",
        action_lease=None,
        cleanup_obligation=None,
        safety_lease=None,
        hard_expires_at=None,
        progress_deadline_at=NOW + timedelta(seconds=60),
    )
    args = _args(restoring, tmp_path)
    plan = {
        "bootstrap_orders": [],
        "buy_orders": [dict(_orders()[2])],
        "sell_orders": [dict(_orders()[3])],
    }

    with patch(
        "grid_optimizer.loop_runner.JsonRecoveryStore.read",
        return_value=restoring,
    ):
        gate = _load_recovery_order_profile_gate(
            args,
            now=NOW + timedelta(seconds=31),
        )
    report = apply_recovery_profile_gate_to_plan(plan=plan, gate=gate)

    assert gate.ready is True
    assert report["active_action"] == "noop"
    assert report["dropped_order_count"] == 0


def test_restoring_with_any_unresolved_risk_state_remains_fail_closed(
    tmp_path,
) -> None:
    active = _active_state(ActionId.INVENTORY_RECOVER, Side.SELL)
    safe = replace(
        active,
        phase=active.phase.RESTORING,
        desired_profile=active.baseline_profile,
        desired_runner_state="running",
        action_lease=None,
        cleanup_obligation=None,
        safety_lease=None,
        hard_expires_at=None,
        progress_deadline_at=NOW + timedelta(seconds=60),
    )
    variants = {
        "profile": replace(safe, desired_profile=active.desired_profile),
        "runner": replace(safe, desired_runner_state="stopped"),
        "action_lease": replace(
            safe,
            action_lease=ActionLease(
                epoch=1,
                action_id=ActionId.TEMPORARY_LOSS_RELIEF,
                side=Side.SELL,
                order_role=OrderRole.REDUCE_ONLY,
                started_at=NOW,
                hard_expires_at=NOW + timedelta(seconds=60),
            ),
        ),
        "cleanup": replace(
            safe,
            cleanup_obligation=CleanupObligation(
                managed_order_manifest=ManagedOrderManifest(
                    symbol="ARXUSDT",
                    generation=active.generation,
                    decision_id="old-loss",
                    profile_digest=active.desired_profile.digest,
                    action_id=ActionId.TEMPORARY_LOSS_RELIEF,
                    side=Side.SELL,
                    order_role=OrderRole.REDUCE_ONLY,
                    issued_at=NOW,
                    orders=(ManagedOrderIdentity("91", "gx-old-loss-91"),),
                    action_lease_epoch=1,
                    hard_expires_at=NOW + timedelta(seconds=60),
                ),
                needs_manifest_rebuild=False,
                created_at=NOW,
                attempt_count=0,
                next_retry_at=NOW,
            ),
        ),
        "safety_lease": replace(
            safe,
            safety_lease=SafetyLease(
                epoch=1,
                started_at=NOW,
                hard_expires_at=NOW + timedelta(seconds=60),
                evidence_fingerprint="safety-risk",
                observation_seq=1,
                observed_at=NOW,
            ),
        ),
    }
    args = _args(safe, tmp_path)

    for label, state in variants.items():
        with patch(
            "grid_optimizer.loop_runner.JsonRecoveryStore.read",
            return_value=state,
        ):
            gate = _load_recovery_order_profile_gate(
                args,
                now=NOW + timedelta(seconds=31),
            )
        assert gate.ready is False, label
        assert gate.reason == "recovery_phase_restoring_not_tradable", label


def test_inventory_adapter_ignores_receipt_from_old_generation(tmp_path) -> None:
    state = _active_state(ActionId.INVENTORY_RECOVER, Side.SELL)
    client_order_id = _ordinary_client_order_id(
        state,
        order_index=1,
        nonce_ns=901,
    )
    args = _args(state, tmp_path)
    state_path = tmp_path / "runner-state.json"
    state_path.write_text("{}", encoding="utf-8")
    with patch("grid_optimizer.loop_runner.JsonRecoveryStore.read", return_value=state):
        gate = _load_recovery_order_profile_gate(args, now=NOW + timedelta(seconds=1))
        _record_inventory_recovery_progress(
            args,
            state_path=state_path,
            expected_gate=gate.as_dict(),
            submit_report={
                "placed_orders": [
                    {
                        "request": {
                            "role": "best_quote_reduce_long",
                            "side": "SELL",
                            "order_type": "LIMIT",
                            "time_in_force": "GTX",
                        },
                        "response": {
                            "orderId": 901,
                            "clientOrderId": client_order_id,
                            "status": "NEW",
                        },
                    }
                ]
            },
            now=NOW + timedelta(seconds=2),
        )
    new_state = replace(
        state,
        generation=state.generation + 1,
        decision_id="ARXUSDT:new-inventory-generation",
        issued_at=NOW + timedelta(seconds=5),
        progress_deadline_at=NOW + timedelta(seconds=35),
        hard_expires_at=NOW + timedelta(seconds=65),
    )

    evidence = load_inventory_recovery_runtime_evidence(
        state_path=state_path,
        recovery_state=new_state,
        captured_at=NOW + timedelta(seconds=6),
    )

    assert evidence.progress_receipts == ()
    assert evidence.accepted_order_ids == ()


def test_inventory_adapter_rejects_non_string_exchange_order_identity(
    tmp_path,
) -> None:
    state = _active_state(ActionId.INVENTORY_RECOVER, Side.SELL)
    client_order_id = _ordinary_client_order_id(
        state,
        order_index=1,
        nonce_ns=902,
    )
    args = _args(state, tmp_path)
    state_path = tmp_path / "runner-state.json"
    state_path.write_text("{}", encoding="utf-8")
    with patch("grid_optimizer.loop_runner.JsonRecoveryStore.read", return_value=state):
        gate = _load_recovery_order_profile_gate(args, now=NOW + timedelta(seconds=1))
        _record_inventory_recovery_progress(
            args,
            state_path=state_path,
            expected_gate=gate.as_dict(),
            submit_report={
                "placed_orders": [
                    {
                        "request": {
                            "role": "best_quote_reduce_long",
                            "side": "SELL",
                            "order_type": "LIMIT",
                            "time_in_force": "GTX",
                        },
                        "response": {
                            "orderId": 902,
                            "clientOrderId": client_order_id,
                            "status": "NEW",
                        },
                    }
                ]
            },
            now=NOW + timedelta(seconds=2),
        )
    document = json.loads(state_path.read_text(encoding="utf-8"))
    document[INVENTORY_RECOVERY_RUNNER_RECEIPTS_KEY]["events"][0][
        "accepted_order_ids"
    ] = [902]
    state_path.write_text(json.dumps(document), encoding="utf-8")

    with pytest.raises(InventoryRecoveryReceiptJournalError):
        load_inventory_recovery_runtime_evidence(
            state_path=state_path,
            recovery_state=state,
            captured_at=NOW + timedelta(seconds=3),
        )
