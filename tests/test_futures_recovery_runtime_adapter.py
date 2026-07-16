from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from grid_optimizer.futures_recovery_coordinator import (
    ActionId,
    ActivationReceipt,
    CleanupProof,
    EffectReceipt,
    EffectStage,
    FlowBlockerAssessment,
    FuturesRecoveryCoordinator,
    FuturesRecoveryDecisionEngine,
    ManagedOrderIdentity,
    ManagedOrderManifest,
    OrderRole,
    InMemoryRecoveryStore,
    RecoveryPhase,
    RecoveryPolicy,
    RecoveryState,
    Side,
    SymbolSnapshot,
    temporary_loss_authorized,
)
from grid_optimizer.futures_recovery_runtime_adapter import (
    RecoveryAdmissionEvidenceError,
    RegisteredSymbolRecoveryOrchestrator,
    TemporaryLossReceiptJournalError,
    load_recovery_admission_evidence,
    load_temporary_loss_runtime_evidence,
    reconcile_temporary_loss_cleanup,
    temporary_loss_fill_progress_receipts,
    temporary_loss_client_order_prefix,
)


NOW = datetime(2026, 7, 16, 2, 0, tzinfo=timezone.utc)
JOURNAL_KEY = "_futures_recovery_runner_receipts"
SCHEMA = "temporary_loss_runner_receipts_v1"
BASELINE = {
    "best_quote_maker_volume_allow_loss_reduce_only": False,
    "best_quote_maker_volume_net_loss_reduce_enabled": False,
    "hard_loss_forced_reduce_enabled": False,
    "volatility_entry_pause_enabled": True,
}


def _normal_recovery_gate(state: RecoveryState) -> dict[str, object]:
    return {
        "managed": True,
        "ready": True,
        "reason": None,
        "symbol": state.symbol,
        "generation": state.generation,
        "decision_id": state.decision_id,
        "profile_digest": state.desired_profile.digest,
        "active_action": "noop",
        "side": None,
        "order_role": None,
        "ledger_class": None,
        "allowed_orders": [],
        "allowed_roles": [],
        "progress_deadline_at": None,
        "hard_expires_at": None,
    }


def _admission_documents(
    state: RecoveryState,
    *,
    plan_at: datetime,
    submit_at: datetime,
) -> tuple[dict[str, object], dict[str, object]]:
    gate = _normal_recovery_gate(state)
    plan = {
        "generated_at": plan_at.isoformat(),
        "recovery_profile_gate": {
            **gate,
            "dropped_order_count": 0,
            "dropped_orders": [],
        },
    }
    submit = {
        "submit_generated_at": submit_at.isoformat(),
        "recovery_profile_execution": {
            "managed": True,
            "authorized": True,
            "reason": None,
            "current_gate": gate,
            "dropped_place_count": 0,
            "dropped_cancel_count": 0,
            "dropped_orders": [],
        },
        "validation": {
            "actions": {
                "reduce_only_no_loss_guard": {
                    "enabled": True,
                    "dropped_order_count": 2,
                    "dropped_orders": [
                        {
                            "role": "best_quote_reduce_long",
                            "side": "SELL",
                            "reduce_only_no_loss_drop_reason": (
                                "long_reduce_below_no_loss_floor"
                            ),
                        },
                        {
                            "role": "best_quote_reduce_short",
                            "side": "BUY",
                            "reduce_only_no_loss_drop_reason": (
                                "short_reduce_above_no_loss_ceiling"
                            ),
                        },
                        {
                            "role": "best_quote_reduce_long",
                            "side": "SELL",
                            "reduce_only_no_loss_drop_reason": (
                                "long_reduce_below_no_loss_floor_near_miss"
                            ),
                        },
                    ],
                }
            }
        },
    }
    return plan, submit


def test_admission_evidence_requires_two_current_fences_and_exact_loss_reasons() -> (
    None
):
    state = RecoveryState.initial("BCHUSDT", BASELINE, now=NOW)
    plan, submit = _admission_documents(
        state,
        plan_at=NOW - timedelta(seconds=2),
        submit_at=NOW - timedelta(seconds=1),
    )

    evidence = load_recovery_admission_evidence(
        plan=plan,
        submit=submit,
        recovery_state=state,
        captured_at=NOW,
        max_age_seconds=30,
    )

    assert evidence.plan_observed_at == NOW - timedelta(seconds=2)
    assert evidence.submit_observed_at == NOW - timedelta(seconds=1)
    assert evidence.loss_only_blocked_sides == (Side.BUY, Side.SELL)


@pytest.mark.parametrize(
    ("mutate", "error"),
    [
        (
            lambda plan, _submit: plan["recovery_profile_gate"].__setitem__(
                "generation", 9
            ),
            "plan recovery gate does not match current state",
        ),
        (
            lambda _plan, submit: submit["recovery_profile_execution"][
                "current_gate"
            ].__setitem__("profile_digest", "stale"),
            "submit recovery gate does not match plan",
        ),
        (
            lambda _plan, submit: submit.__setitem__(
                "submit_generated_at", (NOW + timedelta(seconds=1)).isoformat()
            ),
            "after its snapshot",
        ),
    ],
)
def test_admission_evidence_rejects_unfenced_or_noncausal_reports(
    mutate,
    error: str,
) -> None:
    state = RecoveryState.initial("BCHUSDT", BASELINE, now=NOW)
    plan, submit = _admission_documents(
        state,
        plan_at=NOW - timedelta(seconds=2),
        submit_at=NOW - timedelta(seconds=1),
    )
    mutate(plan, submit)

    with pytest.raises(RecoveryAdmissionEvidenceError, match=error):
        load_recovery_admission_evidence(
            plan=plan,
            submit=submit,
            recovery_state=state,
            captured_at=NOW,
            max_age_seconds=30,
        )


def test_admission_evidence_rejects_stale_plan_even_when_legacy_window_is_long() -> (
    None
):
    state = RecoveryState.initial("BCHUSDT", BASELINE, now=NOW)
    plan, submit = _admission_documents(
        state,
        plan_at=NOW - timedelta(seconds=31),
        submit_at=NOW - timedelta(seconds=1),
    )

    with pytest.raises(RecoveryAdmissionEvidenceError, match="plan is stale"):
        load_recovery_admission_evidence(
            plan=plan,
            submit=submit,
            recovery_state=state,
            captured_at=NOW,
            max_age_seconds=300,
        )


def test_admission_evidence_accepts_bounded_baseline_tune_reduce_gate() -> None:
    assessment = FlowBlockerAssessment(
        inventory_reduce_sides=(Side.SELL,),
        loss_only_blocked_sides=(Side.SELL,),
        episode_fingerprint="adapter-baseline-tune",
    )
    initial = RecoveryState.initial("BCHUSDT", BASELINE, now=NOW)
    initial = initial.with_exhausted_attempt(
        ActionId.INVENTORY_RECOVER,
        Side.SELL,
        OrderRole.REDUCE_ONLY,
        now=NOW,
    ).with_temporary_loss_lease_use("adapter-baseline-tune", count=1)
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
        round_id="adapter-baseline-tune-enter",
    ).next_state
    gate = {
        "managed": True,
        "ready": True,
        "reason": None,
        "symbol": state.symbol,
        "generation": state.generation,
        "decision_id": state.decision_id,
        "profile_digest": state.desired_profile.digest,
        "active_action": "baseline_tune",
        "side": "SELL",
        "order_role": "reduce_only",
        "ledger_class": "normal_bq",
        "allowed_orders": [
            {"role": "best_quote_reduce_long", "side": "SELL"}
        ],
        "allowed_roles": ["best_quote_reduce_long"],
        "progress_deadline_at": state.progress_deadline_at.isoformat(),
        "hard_expires_at": state.hard_expires_at.isoformat(),
    }
    plan, submit = _admission_documents(
        state,
        plan_at=NOW + timedelta(seconds=1),
        submit_at=NOW + timedelta(seconds=2),
    )
    plan["recovery_profile_gate"] = {
        **gate,
        "dropped_order_count": 0,
        "dropped_orders": [],
    }
    submit["recovery_profile_execution"]["current_gate"] = gate

    evidence = load_recovery_admission_evidence(
        plan=plan,
        submit=submit,
        recovery_state=state,
        captured_at=NOW + timedelta(seconds=3),
        max_age_seconds=30,
    )

    assert Side.SELL in evidence.loss_only_blocked_sides


def _states() -> tuple[RecoveryState, RecoveryState]:
    engine = FuturesRecoveryDecisionEngine()
    assessment = FlowBlockerAssessment(
        inventory_reduce_sides=(Side.SELL,),
        loss_only_blocked_sides=(Side.SELL,),
        episode_fingerprint="runtime-adapter-test",
    )
    state = RecoveryState.initial("ARXUSDT", BASELINE, now=NOW)
    state = state.with_exhausted_attempt(
        ActionId.INVENTORY_RECOVER,
        Side.SELL,
        OrderRole.REDUCE_ONLY,
        now=NOW,
    )
    settling = engine.plan_round(
        snapshot=SymbolSnapshot(
            symbol="ARXUSDT",
            captured_at=NOW,
            assessment=assessment,
        ),
        state=state,
        now=NOW,
        round_id="enter",
    ).next_state
    assert settling.action_lease is not None
    activated_at = NOW + timedelta(seconds=1)
    active = engine.plan_round(
        snapshot=SymbolSnapshot(
            symbol="ARXUSDT",
            captured_at=activated_at,
            assessment=assessment,
            activation_receipt=ActivationReceipt(
                decision_id=str(settling.decision_id),
                generation=settling.generation,
                profile_digest=settling.desired_profile.digest,
                action_lease_epoch=settling.action_lease.epoch,
                observed_at=activated_at,
            ),
        ),
        state=settling,
        now=activated_at,
        round_id="activate",
    ).next_state
    return settling, active


def _temporary_loss_test_client_id(
    state: RecoveryState,
    order_id: str,
) -> str:
    prefix = temporary_loss_client_order_prefix(state)
    suffix_length = 36 - len(prefix)
    suffix = hashlib.sha256(
        f"test-order:{order_id}".encode("utf-8")
    ).hexdigest()[:suffix_length]
    return prefix + suffix


def _managed_manifest(
    state: RecoveryState,
    pairs: tuple[tuple[str, str], ...],
) -> ManagedOrderManifest:
    assert state.decision_id is not None
    assert state.issued_at is not None
    assert state.side is not None
    assert state.order_role is not None
    lease = state.action_lease
    is_temporary_loss = state.active_action is ActionId.TEMPORARY_LOSS_RELIEF
    if is_temporary_loss:
        assert lease is not None
    return ManagedOrderManifest(
        symbol=state.symbol,
        generation=state.generation,
        decision_id=state.decision_id,
        profile_digest=state.desired_profile.digest,
        action_id=state.active_action,
        side=state.side,
        order_role=state.order_role,
        issued_at=state.issued_at,
        orders=tuple(
            sorted(
                ManagedOrderIdentity(
                    order_id=order_id,
                    client_order_id=client_order_id,
                )
                for order_id, client_order_id in pairs
            )
        ),
        action_lease_epoch=(lease.epoch if is_temporary_loss else None),
        hard_expires_at=(lease.hard_expires_at if is_temporary_loss else None),
    )


def _cleaning_state(*, open_order_ids: tuple[str, ...]) -> RecoveryState:
    _settling, active = _states()
    assert active.action_lease is not None
    assessment = FlowBlockerAssessment(
        inventory_reduce_sides=(Side.SELL,),
        loss_only_blocked_sides=(Side.SELL,),
        episode_fingerprint="runtime-adapter-test",
    )
    cleaning = FuturesRecoveryDecisionEngine().plan_round(
        snapshot=SymbolSnapshot(
            symbol="ARXUSDT",
            captured_at=active.action_lease.hard_expires_at,
            assessment=assessment,
            managed_order_manifest=_managed_manifest(
                active,
                tuple(
                    (
                        order_id,
                        _temporary_loss_test_client_id(active, order_id),
                    )
                    for order_id in open_order_ids
                ),
            ),
        ),
        state=active,
        now=active.action_lease.hard_expires_at,
        round_id="expire",
    ).next_state
    assert cleaning.phase is RecoveryPhase.CLEANING
    return cleaning


def _restoring_state() -> RecoveryState:
    cleaning = _cleaning_state(open_order_ids=())
    obligation = cleaning.cleanup_obligation
    assert obligation is not None
    observed_at = obligation.created_at + timedelta(seconds=1)
    restoring = FuturesRecoveryDecisionEngine().plan_round(
        snapshot=SymbolSnapshot(
            symbol="ARXUSDT",
            captured_at=observed_at,
            assessment=FlowBlockerAssessment(),
            cleanup_proof=CleanupProof(
                source_decision_id=obligation.source_decision_id,
                source_generation=(
                    obligation.managed_order_manifest.generation
                ),
                manifest_digest=obligation.managed_order_manifest.digest,
                remaining_order_ids=(),
                user_trades_watermark=0,
                observed_at=observed_at,
            ),
        ),
        state=cleaning,
        now=observed_at,
        round_id="cleanup-proof",
    ).next_state
    assert restoring.phase is RecoveryPhase.RESTORING
    return restoring


def _binding(state: RecoveryState) -> dict[str, object]:
    assert state.action_lease is not None
    return {
        "symbol": state.symbol,
        "generation": state.generation,
        "decision_id": state.decision_id,
        "profile_digest": state.desired_profile.digest,
        "action_lease_epoch": state.action_lease.epoch,
        "side": state.action_lease.side.value,
        "role": "best_quote_reduce_long",
        "order_role": "reduce_only",
        "hard_expires_at": state.action_lease.hard_expires_at.isoformat(),
    }


def _receipt_id(
    kind: str,
    binding: dict[str, object],
    identity: dict[str, object] | None = None,
) -> str:
    encoded = json.dumps(
        {"kind": kind, "binding": binding, "identity": identity or {}},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode()
    return "tlr-" + hashlib.sha256(encoded).hexdigest()[:32]


def _event(
    state: RecoveryState,
    *,
    kind: str,
    seq: int,
    observed_at: datetime,
    order_ids: list[str] | None = None,
    client_order_ids: list[str] | None = None,
    accepted_statuses: list[str] | None = None,
) -> dict[str, object]:
    binding = _binding(state)
    identity: dict[str, object] = {}
    if kind == "progress":
        identity = {
            "accepted_order_ids": order_ids or [],
            "accepted_client_order_ids": client_order_ids or [],
        }
        details: dict[str, object] = {
            **identity,
            "accepted_statuses": accepted_statuses or ["NEW"],
            "allow_loss_effective": True,
            "progress_stage": "gtx_order_accepted",
        }
    elif kind == "activation":
        details = {"allow_loss_effective": False, "config_applied": True}
    elif kind == "expiry":
        assert state.action_lease is not None
        details = {
            "allow_loss_effective": False,
            "expired_at": state.action_lease.hard_expires_at.isoformat(),
            "reason": "temporary_loss_action_lease_hard_expired",
        }
    elif kind == "cleanup_needed":
        details = {
            "allow_loss_effective": False,
            "accepted_order_ids": order_ids or [],
            "accepted_client_order_ids": client_order_ids or [],
            "exchange_observation_required": True,
            "reason": "temporary_loss_lease_orders_must_reconcile",
        }
    else:
        raise AssertionError(kind)
    return {
        "receipt_id": _receipt_id(kind, binding, identity),
        "kind": kind,
        **binding,
        "observed_at": observed_at.isoformat(),
        "observation_seq": seq,
        **details,
    }


def _restoration_event(
    state: RecoveryState,
    *,
    seq: int,
    observed_at: datetime,
) -> dict[str, object]:
    binding: dict[str, object] = {
        "symbol": state.symbol,
        "generation": state.generation,
        "decision_id": state.decision_id,
        "profile_digest": state.desired_profile.digest,
    }
    return {
        "receipt_id": _receipt_id("restoration_applied", binding),
        "kind": "restoration_applied",
        **binding,
        "observed_at": observed_at.isoformat(),
        "observation_seq": seq,
        "allow_loss_effective": False,
        "config_applied": True,
    }


def _write_journal(
    tmp_path,
    events: list[dict[str, object]],
    *,
    symbol: str = "ARXUSDT",
):
    latest = {str(event["kind"]): str(event["receipt_id"]) for event in events}
    path = tmp_path / f"{symbol.lower()}_loop_state.json"
    path.write_text(
        json.dumps(
            {
                JOURNAL_KEY: {
                    "schema": SCHEMA,
                    "symbol": symbol,
                    "next_observation_seq": (
                        int(events[-1]["observation_seq"]) + 1 if events else 0
                    ),
                    "events": events,
                    "latest": latest,
                }
            }
        ),
        encoding="utf-8",
    )
    return path


def test_strict_activation_receipt_maps_to_coordinator_type(tmp_path) -> None:
    settling, _active = _states()
    event = _event(
        settling,
        kind="activation",
        seq=0,
        observed_at=NOW + timedelta(milliseconds=100),
    )
    evidence = load_temporary_loss_runtime_evidence(
        state_path=_write_journal(tmp_path, [event]),
        recovery_state=settling,
        captured_at=NOW + timedelta(seconds=1),
    )

    assert evidence.activation_receipt == ActivationReceipt(
        decision_id=str(settling.decision_id),
        generation=settling.generation,
        profile_digest=settling.desired_profile.digest,
        action_lease_epoch=settling.action_lease.epoch,
        observed_at=NOW + timedelta(milliseconds=100),
    )
    assert evidence.progress_receipts == ()


def test_registered_orchestrator_enforces_one_snapshot_one_cas_one_effect() -> None:
    store = InMemoryRecoveryStore()
    store.register_symbol("ARXUSDT", BASELINE, now=NOW)
    effects: list[object] = []
    orchestrator = RegisteredSymbolRecoveryOrchestrator(
        store=store,
        effect_executor=lambda _symbol, command: effects.append(command),
    )

    result = orchestrator.reconcile(
        snapshot=SymbolSnapshot(
            symbol="ARXUSDT",
            captured_at=NOW,
            assessment=FlowBlockerAssessment(runner_faults=("runner_inactive",)),
        ),
        now=NOW,
        round_id="registered-one-round",
    )

    assert result.snapshot_provider_calls == 1
    assert result.control_cas_count == 1
    assert result.effect_count == 1
    assert len(effects) == 1
    assert result.plan.action_id is ActionId.RUNNER_RECOVER
    assert result.plan.effect_stage is EffectStage.RUNNER_RESTART


def test_progress_and_cleanup_are_fenced_to_exact_typed_lease(tmp_path) -> None:
    _settling, active = _states()
    client_order_id = _temporary_loss_test_client_id(active, "701")
    progress = _event(
        active,
        kind="progress",
        seq=4,
        observed_at=NOW + timedelta(seconds=2),
        order_ids=["701"],
        client_order_ids=[client_order_id],
    )
    expiry = _event(
        active,
        kind="expiry",
        seq=5,
        observed_at=active.action_lease.hard_expires_at,
    )
    cleanup = _event(
        active,
        kind="cleanup_needed",
        seq=6,
        observed_at=active.action_lease.hard_expires_at,
        order_ids=["701"],
        client_order_ids=[client_order_id],
    )
    evidence = load_temporary_loss_runtime_evidence(
        state_path=_write_journal(tmp_path, [progress, expiry, cleanup]),
        recovery_state=active,
        captured_at=active.action_lease.hard_expires_at + timedelta(seconds=1),
    )

    assert evidence.progress_receipts == ()
    assert evidence.accepted_order_ids == ("701",)
    assert evidence.accepted_client_order_ids == (client_order_id,)
    assert evidence.accepted_order_pairs == (("701", client_order_id),)
    assert evidence.cleanup_requested is True
    assert evidence.cleanup_order_ids == ("701",)
    assert evidence.cleanup_client_order_ids == (client_order_id,)


def test_authenticated_temporary_loss_journal_rejects_non_decision_prefix(
    tmp_path,
) -> None:
    _settling, active = _states()
    event = _event(
        active,
        kind="progress",
        seq=0,
        observed_at=NOW + timedelta(seconds=2),
        order_ids=["701"],
        # _event recomputes the receipt id over this identity, so the failure
        # proves hash consistency alone cannot widen decision ownership.
        client_order_ids=["typed-loss-701"],
    )

    with pytest.raises(
        TemporaryLossReceiptJournalError,
        match="decision-bound client order prefix",
    ):
        load_temporary_loss_runtime_evidence(
            state_path=_write_journal(tmp_path, [event]),
            recovery_state=active,
            captured_at=NOW + timedelta(seconds=3),
        )


def test_cleanup_manifest_may_restore_progress_trimmed_from_bounded_journal(
    tmp_path,
) -> None:
    _settling, active = _states()
    cleanup_at = active.action_lease.hard_expires_at
    client_700 = _temporary_loss_test_client_id(active, "700")
    client_701 = _temporary_loss_test_client_id(active, "701")
    retained_progress = _event(
        active,
        kind="progress",
        seq=127,
        observed_at=NOW + timedelta(seconds=2),
        order_ids=["701"],
        client_order_ids=[client_701],
    )
    cleanup = _event(
        active,
        kind="cleanup_needed",
        seq=128,
        observed_at=cleanup_at,
        order_ids=["700", "701"],
        client_order_ids=[client_700, client_701],
    )

    evidence = load_temporary_loss_runtime_evidence(
        state_path=_write_journal(tmp_path, [retained_progress, cleanup]),
        recovery_state=active,
        captured_at=cleanup_at + timedelta(seconds=1),
    )

    assert evidence.accepted_order_pairs == (
        ("700", client_700),
        ("701", client_701),
    )
    assert evidence.cleanup_order_ids == ("700", "701")


def test_temporary_loss_fill_progress_requires_exact_pair_and_action_window(
    tmp_path,
) -> None:
    _settling, active = _states()
    filled_at = NOW + timedelta(seconds=2)
    client_order_id = _temporary_loss_test_client_id(active, "701")
    event = _event(
        active,
        kind="progress",
        seq=4,
        observed_at=filled_at,
        order_ids=["701"],
        client_order_ids=[client_order_id],
    )
    evidence = load_temporary_loss_runtime_evidence(
        state_path=_write_journal(tmp_path, [event]),
        recovery_state=active,
        captured_at=filled_at,
    )

    receipts = temporary_loss_fill_progress_receipts(
        recovery_state=active,
        runtime_evidence=evidence,
        trade_rows=(
            {
                "id": 1701,
                "orderId": 701,
                "clientOrderId": client_order_id,
                "symbol": "ARXUSDT",
                "side": "SELL",
                "positionSide": "LONG",
                "time": int(filled_at.timestamp() * 1000),
                "qty": "1",
            },
        ),
        captured_at=filled_at,
    )
    assert len(receipts) == 1
    assert receipts[0].decision_id == active.decision_id
    assert receipts[0].generation == active.generation
    assert receipts[0].side is Side.SELL
    assert receipts[0].order_role is OrderRole.REDUCE_ONLY
    assert receipts[0].observation_seq == 1701

    missing_trade_id = temporary_loss_fill_progress_receipts(
        recovery_state=active,
        runtime_evidence=evidence,
        trade_rows=(
            {
                "orderId": 701,
                "clientOrderId": client_order_id,
                "symbol": "ARXUSDT",
                "side": "SELL",
                "positionSide": "LONG",
                "time": int(filled_at.timestamp() * 1000),
                "qty": "1",
            },
        ),
        captured_at=filled_at,
    )
    assert missing_trade_id == ()

    assert active.action_lease is not None
    assert active.action_lease.activated_at is not None
    before_activation = active.action_lease.activated_at - timedelta(
        milliseconds=1
    )
    assert temporary_loss_fill_progress_receipts(
        recovery_state=active,
        runtime_evidence=evidence,
        trade_rows=(
            {
                "id": 1699,
                "orderId": 701,
                "clientOrderId": client_order_id,
                "symbol": "ARXUSDT",
                "side": "SELL",
                "positionSide": "LONG",
                "time": int(before_activation.timestamp() * 1000),
                "qty": "1",
            },
            {
                "id": 1700,
                "orderId": 701,
                "clientOrderId": client_order_id,
                "symbol": "ARXUSDT",
                "side": "SELL",
                "positionSide": "LONG",
                "time": int(filled_at.timestamp() * 1000),
                "qty": "inf",
            },
        ),
        captured_at=filled_at,
    ) == ()

    assert active.progress_deadline_at is not None
    late = active.progress_deadline_at
    late_receipts = temporary_loss_fill_progress_receipts(
        recovery_state=active,
        runtime_evidence=evidence,
        trade_rows=(
            {
                "id": 1702,
                "orderId": 701,
                "clientOrderId": client_order_id,
                "symbol": "ARXUSDT",
                "side": "SELL",
                "positionSide": "LONG",
                "time": int(late.timestamp() * 1000),
                "qty": "1",
            },
        ),
        captured_at=late,
    )
    assert late_receipts == ()


def test_restoration_applied_maps_to_coordinator_profile_confirmation(tmp_path) -> None:
    restoring = _restoring_state()
    observed_at = NOW + timedelta(minutes=6)
    event = _restoration_event(restoring, seq=8, observed_at=observed_at)

    evidence = load_temporary_loss_runtime_evidence(
        state_path=_write_journal(tmp_path, [event]),
        recovery_state=restoring,
        captured_at=observed_at + timedelta(seconds=1),
    )

    assert evidence.applied_generation == restoring.generation
    assert evidence.applied_profile_digest == restoring.desired_profile.digest
    assert evidence.activation_receipt is None
    assert evidence.progress_receipts == ()


def test_restoration_applied_fails_closed_on_profile_drift(tmp_path) -> None:
    restoring = _restoring_state()
    observed_at = NOW + timedelta(minutes=6)
    event = _restoration_event(restoring, seq=0, observed_at=observed_at)
    event["profile_digest"] = "wrong"

    with pytest.raises(TemporaryLossReceiptJournalError):
        load_temporary_loss_runtime_evidence(
            state_path=_write_journal(tmp_path, [event]),
            recovery_state=restoring,
            captured_at=observed_at + timedelta(seconds=1),
        )


@pytest.mark.parametrize(
    "mutate",
    [
        lambda event: event.update(profile_digest="wrong"),
        lambda event: event.update(action_lease_epoch=999),
        lambda event: event.update(side="BUY"),
        lambda event: event.update(role="best_quote_reduce_short"),
        lambda event: event.update(hard_expires_at="2026-07-17T00:00:00+00:00"),
        lambda event: event.update(receipt_id="tlr-forged"),
        lambda event: event.update(allow_loss_effective=False),
    ],
    ids=("digest", "epoch", "side", "role", "expiry", "id", "allow_loss"),
)
def test_relevant_progress_receipt_fails_closed_on_any_fence_drift(
    tmp_path,
    mutate,
) -> None:
    _settling, active = _states()
    event = _event(
        active,
        kind="progress",
        seq=0,
        observed_at=NOW + timedelta(seconds=2),
        order_ids=["701"],
        client_order_ids=[_temporary_loss_test_client_id(active, "701")],
    )
    mutate(event)
    with pytest.raises(TemporaryLossReceiptJournalError):
        load_temporary_loss_runtime_evidence(
            state_path=_write_journal(tmp_path, [event]),
            recovery_state=active,
            captured_at=NOW + timedelta(seconds=3),
        )


def test_cleanup_manifest_must_equal_accumulated_progress_manifest(tmp_path) -> None:
    _settling, active = _states()
    cleanup_at = active.action_lease.hard_expires_at
    events = [
        _event(
            active,
            kind="progress",
            seq=0,
            observed_at=NOW + timedelta(seconds=2),
            order_ids=["701"],
            client_order_ids=[_temporary_loss_test_client_id(active, "701")],
        ),
        _event(
            active,
            kind="cleanup_needed",
            seq=1,
            observed_at=cleanup_at,
            order_ids=["999"],
            client_order_ids=[_temporary_loss_test_client_id(active, "999")],
        ),
    ]
    with pytest.raises(
        TemporaryLossReceiptJournalError,
        match="cleanup manifest does not match progress manifest",
    ):
        load_temporary_loss_runtime_evidence(
            state_path=_write_journal(tmp_path, events),
            recovery_state=active,
            captured_at=cleanup_at + timedelta(seconds=1),
        )


def test_future_or_non_monotonic_receipts_are_rejected(tmp_path) -> None:
    settling, _active = _states()
    events = [
        _event(
            settling,
            kind="activation",
            seq=1,
            observed_at=NOW + timedelta(seconds=3),
        ),
        _event(
            settling,
            kind="activation",
            seq=1,
            observed_at=NOW + timedelta(seconds=4),
        ),
    ]
    with pytest.raises(TemporaryLossReceiptJournalError):
        load_temporary_loss_runtime_evidence(
            state_path=_write_journal(tmp_path, events),
            recovery_state=settling,
            captured_at=NOW + timedelta(seconds=2),
        )


def _owned_open_order(
    order_id: str = "701",
    *,
    tif: str = "GTX",
    client_order_id: str | None = None,
) -> dict[str, object]:
    return {
        "symbol": "ARXUSDT",
        "orderId": int(order_id),
        "clientOrderId": client_order_id or f"typed-loss-{order_id}",
        "type": "LIMIT",
        "timeInForce": tif,
        "side": "SELL",
        "positionSide": "LONG",
        "status": "NEW",
    }


def test_cleanup_cancels_only_manifest_gtx_and_builds_exact_proof() -> None:
    cleaning = _cleaning_state(open_order_ids=("701",))
    owned = _owned_open_order(
        client_order_id=_temporary_loss_test_client_id(cleaning, "701")
    )
    snapshots = [
        [owned, _owned_open_order("888")],
        [_owned_open_order("888")],
    ]
    canceled: list[str] = []

    result = reconcile_temporary_loss_cleanup(
        recovery_state=cleaning,
        observed_at=cleaning.cleanup_obligation.created_at + timedelta(seconds=1),
        fetch_open_orders=lambda _symbol: snapshots.pop(0),
        cancel_order=lambda _symbol, order_id: (
            canceled.append(order_id)
            or {"symbol": "ARXUSDT", "orderId": int(order_id), "status": "CANCELED"}
        ),
        fetch_user_trades_page=lambda _symbol, start_ms, end_ms, _limit: [
            {"id": 991, "orderId": 701, "time": start_ms}
        ],
    )

    assert canceled == ["701"]
    assert result.canceled_order_ids == ("701",)
    assert result.remaining_order_ids == ()
    assert result.blocked_reason is None
    assert result.effect_receipt is not None
    assert result.effect_receipt.decision_id == cleaning.decision_id
    assert result.effect_receipt.effect_epoch == cleaning.pending_effect_epoch
    assert result.cleanup_proof is not None
    assert result.cleanup_proof.source_decision_id == cleaning.decision_id
    assert result.cleanup_proof.source_generation == (
        cleaning.cleanup_obligation.managed_order_manifest.generation
    )
    assert result.cleanup_proof.manifest_digest == (
        cleaning.cleanup_obligation.managed_order_manifest.digest
    )
    assert result.cleanup_proof.remaining_order_ids == ()
    assert result.cleanup_proof.user_trades_watermark == 991


@pytest.mark.parametrize("preemptor", ["safety", "runner"])
@pytest.mark.parametrize(
    "open_order_ids",
    [("701",), ()],
    ids=["managed-gtx-cancel", "local-state-repair"],
)
def test_cleanup_finishes_old_temporary_loss_lease_after_higher_priority_preemption(
    preemptor: str,
    open_order_ids: tuple[str, ...],
) -> None:
    _settling, active = _states()
    assert active.action_lease is not None
    old_decision_id = str(active.decision_id)
    observed_at = NOW + timedelta(seconds=2)
    if preemptor == "safety":
        assessment = FlowBlockerAssessment(
            safety_reasons=("volatility_entry_pause",),
            safety_evidence_fingerprint="fresh-safety-observation",
            safety_observation_seq=7,
            safety_observed_at=observed_at,
        )
        expected_action = ActionId.SAFETY_CONVERGE
    else:
        assessment = FlowBlockerAssessment(runner_faults=("runner_inactive",))
        expected_action = ActionId.RUNNER_RECOVER

    cleaning = FuturesRecoveryDecisionEngine().plan_round(
        snapshot=SymbolSnapshot(
            symbol=active.symbol,
            captured_at=observed_at,
            assessment=assessment,
            managed_order_manifest=_managed_manifest(
                active,
                tuple(
                    (
                        order_id,
                        _temporary_loss_test_client_id(active, order_id),
                    )
                    for order_id in open_order_ids
                ),
            ),
        ),
        state=active,
        now=observed_at,
        round_id=f"preempt-{preemptor}",
    ).next_state

    assert cleaning.phase is RecoveryPhase.CLEANING
    assert cleaning.active_action is expected_action
    assert cleaning.decision_id != old_decision_id
    assert cleaning.cleanup_obligation is not None
    assert cleaning.cleanup_obligation.source_action_id is ActionId.TEMPORARY_LOSS_RELIEF
    assert cleaning.cleanup_obligation.source_decision_id == old_decision_id
    unrelated = _owned_open_order("888")
    reconstructed = _owned_open_order("701")
    reconstructed["clientOrderId"] = (
        f"{temporary_loss_client_order_prefix(cleaning)}pre001"
    )
    snapshots = [
        (
            [
                _owned_open_order(
                    "701",
                    client_order_id=_temporary_loss_test_client_id(
                        cleaning,
                        "701",
                    ),
                ),
                unrelated,
            ]
            if open_order_ids
            else [reconstructed, unrelated]
        ),
        [unrelated],
    ]
    canceled: list[str] = []

    result = reconcile_temporary_loss_cleanup(
        recovery_state=cleaning,
        observed_at=observed_at + timedelta(seconds=1),
        fetch_open_orders=lambda _symbol: snapshots.pop(0),
        cancel_order=lambda _symbol, order_id: (
            canceled.append(order_id)
            or {
                "symbol": active.symbol,
                "orderId": int(order_id),
                "status": "CANCELED",
            }
        ),
        fetch_user_trades_page=lambda _symbol, start_ms, _end_ms, _limit: [
            {"id": 993, "orderId": 701, "time": start_ms}
        ],
    )

    assert result.effect_receipt is not None
    assert result.effect_receipt.decision_id == cleaning.decision_id
    assert result.cleanup_proof is not None
    assert result.cleanup_proof.source_decision_id == old_decision_id
    assert canceled == ["701"]


def test_cleanup_refuses_non_gtx_manifest_order_before_any_cancel() -> None:
    cleaning = _cleaning_state(open_order_ids=("701",))
    canceled: list[str] = []

    with pytest.raises(
        TemporaryLossReceiptJournalError,
        match="maker-only GTX scope mismatch",
    ):
        reconcile_temporary_loss_cleanup(
            recovery_state=cleaning,
            observed_at=cleaning.cleanup_obligation.created_at + timedelta(seconds=1),
            fetch_open_orders=lambda _symbol: [
                _owned_open_order(
                    tif="GTC",
                    client_order_id=_temporary_loss_test_client_id(
                        cleaning,
                        "701",
                    ),
                )
            ],
            cancel_order=lambda _symbol, order_id: canceled.append(order_id),
            fetch_user_trades_page=lambda _symbol, _start_ms, _end_ms, _limit: [],
        )

    assert canceled == []


def test_cleanup_without_manifest_observes_exchange_but_cancels_nothing() -> None:
    cleaning = _cleaning_state(open_order_ids=())
    snapshots = [[_owned_open_order("888")], [_owned_open_order("888")]]
    canceled: list[str] = []

    result = reconcile_temporary_loss_cleanup(
        recovery_state=cleaning,
        observed_at=cleaning.cleanup_obligation.created_at + timedelta(seconds=1),
        fetch_open_orders=lambda _symbol: snapshots.pop(0),
        cancel_order=lambda _symbol, order_id: canceled.append(order_id),
        fetch_user_trades_page=lambda _symbol, _start_ms, _end_ms, _limit: [],
    )

    assert canceled == []
    assert result.cleanup_proof is not None
    assert result.cleanup_proof.user_trades_watermark == 0


def test_local_state_repair_rebuilds_and_cancels_typed_lease_order() -> None:
    cleaning = _cleaning_state(open_order_ids=())
    prefix = temporary_loss_client_order_prefix(cleaning)
    owned = _owned_open_order("701")
    owned["clientOrderId"] = f"{prefix}a1b2c3"
    unrelated = _owned_open_order("888")
    snapshots = [[owned, unrelated], [unrelated]]
    canceled: list[str] = []

    result = reconcile_temporary_loss_cleanup(
        recovery_state=cleaning,
        observed_at=cleaning.cleanup_obligation.created_at + timedelta(seconds=1),
        fetch_open_orders=lambda _symbol: snapshots.pop(0),
        cancel_order=lambda _symbol, order_id: (
            canceled.append(order_id)
            or {
                "symbol": "ARXUSDT",
                "orderId": int(order_id),
                "status": "CANCELED",
            }
        ),
        fetch_user_trades_page=lambda _symbol, start_ms, _end_ms, _limit: [
            {"id": 994, "orderId": 701, "time": start_ms}
        ],
    )

    assert canceled == ["701"]
    assert result.cleanup_proof is not None
    assert result.cleanup_proof.user_trades_watermark == 994


def test_local_state_repair_refuses_rebuilt_non_gtx_order_before_cancel() -> None:
    cleaning = _cleaning_state(open_order_ids=())
    owned = _owned_open_order("701", tif="GTC")
    owned["clientOrderId"] = f"{temporary_loss_client_order_prefix(cleaning)}badgtx"
    canceled: list[str] = []

    with pytest.raises(
        TemporaryLossReceiptJournalError,
        match="maker-only GTX scope mismatch",
    ):
        reconcile_temporary_loss_cleanup(
            recovery_state=cleaning,
            observed_at=cleaning.cleanup_obligation.created_at + timedelta(seconds=1),
            fetch_open_orders=lambda _symbol: [owned],
            cancel_order=lambda _symbol, order_id: canceled.append(order_id),
            fetch_user_trades_page=lambda _symbol, _start_ms, _end_ms, _limit: [],
        )

    assert canceled == []


def test_cleanup_does_not_claim_proof_while_owned_order_remains_open() -> None:
    cleaning = _cleaning_state(open_order_ids=("701",))
    owned = _owned_open_order(
        client_order_id=_temporary_loss_test_client_id(cleaning, "701")
    )
    snapshots = [[owned], [owned]]

    result = reconcile_temporary_loss_cleanup(
        recovery_state=cleaning,
        observed_at=cleaning.cleanup_obligation.created_at + timedelta(seconds=1),
        fetch_open_orders=lambda _symbol: snapshots.pop(0),
        cancel_order=lambda _symbol, order_id: {
            "symbol": "ARXUSDT",
            "orderId": int(order_id),
            "status": "CANCELED",
        },
        fetch_user_trades_page=lambda _symbol, _start_ms, _end_ms, _limit: [],
    )

    assert result.remaining_order_ids == ("701",)
    assert result.blocked_reason == "owned_gtx_orders_still_open"
    assert result.effect_receipt is None
    assert result.cleanup_proof is None


def test_cleanup_cancel_unknown_race_requires_fresh_exchange_and_trade_proof() -> None:
    cleaning = _cleaning_state(open_order_ids=("701",))
    owned = _owned_open_order(
        client_order_id=_temporary_loss_test_client_id(cleaning, "701")
    )
    snapshots = [[owned], []]
    trade_page_calls: list[tuple[int, int, int]] = []

    def cancel_raced(_symbol: str, _order_id: str):
        raise RuntimeError("Binance -2011: Unknown order sent.")

    def fetch_trade_page(
        _symbol: str,
        start_ms: int,
        end_ms: int,
        limit: int,
    ) -> list[dict[str, object]]:
        trade_page_calls.append((start_ms, end_ms, limit))
        return [{"id": 992, "orderId": 701, "time": start_ms}]

    result = reconcile_temporary_loss_cleanup(
        recovery_state=cleaning,
        observed_at=cleaning.cleanup_obligation.created_at + timedelta(seconds=1),
        fetch_open_orders=lambda _symbol: snapshots.pop(0),
        cancel_order=cancel_raced,
        fetch_user_trades_page=fetch_trade_page,
    )

    assert result.canceled_order_ids == ()
    assert result.already_terminal_order_ids == ("701",)
    assert result.remaining_order_ids == ()
    assert trade_page_calls
    assert result.cleanup_proof is not None
    assert result.cleanup_proof.user_trades_watermark == 992


def test_cleanup_cancel_unknown_race_still_blocks_if_order_remains_open() -> None:
    cleaning = _cleaning_state(open_order_ids=("701",))
    owned = _owned_open_order(
        client_order_id=_temporary_loss_test_client_id(cleaning, "701")
    )
    snapshots = [[owned], [owned]]

    def cancel_raced(_symbol: str, _order_id: str):
        raise RuntimeError("Binance -2011: Unknown order sent.")

    result = reconcile_temporary_loss_cleanup(
        recovery_state=cleaning,
        observed_at=cleaning.cleanup_obligation.created_at + timedelta(seconds=1),
        fetch_open_orders=lambda _symbol: snapshots.pop(0),
        cancel_order=cancel_raced,
        fetch_user_trades_page=lambda _symbol, _start, _end, _limit: [],
    )

    assert result.already_terminal_order_ids == ("701",)
    assert result.remaining_order_ids == ("701",)
    assert result.cleanup_proof is None


def test_cleanup_fails_closed_when_trade_window_request_budget_is_exhausted() -> None:
    cleaning = _cleaning_state(open_order_ids=())
    snapshots = [[], []]

    with pytest.raises(
        TemporaryLossReceiptJournalError,
        match="request limit exhausted",
    ):
        reconcile_temporary_loss_cleanup(
            recovery_state=cleaning,
            observed_at=cleaning.cleanup_obligation.created_at + timedelta(seconds=1),
            fetch_open_orders=lambda _symbol: snapshots.pop(0),
            cancel_order=lambda _symbol, _order_id: None,
            fetch_user_trades_page=lambda _symbol, start_ms, _end_ms, limit: [
                {"id": f"{start_ms}-a", "time": start_ms},
                {"id": f"{start_ms}-b", "time": start_ms},
            ][:limit],
            trade_page_limit=2,
            max_trade_page_requests=3,
        )


def test_cleanup_fails_closed_on_full_single_millisecond_trade_page() -> None:
    cleaning = _cleaning_state(open_order_ids=())
    snapshots = [[], []]

    with pytest.raises(
        TemporaryLossReceiptJournalError,
        match="density exceeds timestamp resolution",
    ):
        reconcile_temporary_loss_cleanup(
            recovery_state=cleaning,
            observed_at=cleaning.cleanup_obligation.created_at + timedelta(seconds=1),
            fetch_open_orders=lambda _symbol: snapshots.pop(0),
            cancel_order=lambda _symbol, _order_id: None,
            fetch_user_trades_page=lambda _symbol, start_ms, _end_ms, limit: [
                {"id": f"{start_ms}-{index}", "time": start_ms}
                for index in range(limit)
            ],
            trade_page_limit=2,
            max_trade_page_requests=80,
        )


@pytest.mark.parametrize(
    ("accepted_status", "owned_order_still_open", "expected_canceled"),
    [
        ("NEW", True, ("701",)),
        ("FILLED", False, ()),
    ],
)
def test_temporary_loss_progress_manifest_reaches_exact_cleanup_for_open_or_filled_order(
    tmp_path,
    accepted_status: str,
    owned_order_still_open: bool,
    expected_canceled: tuple[str, ...],
) -> None:
    state_path = tmp_path / "arxusdt_loop_state.json"
    _settling, active = _states()
    client_order_id = _temporary_loss_test_client_id(active, "701")
    progress_at = NOW + timedelta(seconds=2)
    _write_journal(
        tmp_path,
        [
            _event(
                active,
                kind="progress",
                seq=0,
                observed_at=progress_at,
                order_ids=["701"],
                client_order_ids=[client_order_id],
                accepted_statuses=[accepted_status],
            )
        ],
    )
    evidence = load_temporary_loss_runtime_evidence(
        state_path=state_path,
        recovery_state=active,
        captured_at=progress_at,
    )
    assert evidence.cleanup_requested is False
    assert evidence.accepted_order_ids == ("701",)
    assert evidence.accepted_order_pairs == (("701", client_order_id),)

    held = FuturesRecoveryDecisionEngine().plan_round(
        snapshot=SymbolSnapshot(
            symbol=active.symbol,
            captured_at=progress_at,
            assessment=FlowBlockerAssessment(
                inventory_reduce_sides=(Side.SELL,),
                loss_only_blocked_sides=(Side.SELL,),
            ),
            progress_receipts=evidence.progress_receipts,
            managed_order_manifest=_managed_manifest(
                active,
                evidence.accepted_order_pairs,
            ),
        ),
        state=active,
        now=progress_at,
        round_id=f"progress-{accepted_status.lower()}",
    )
    assert held.next_state.phase is RecoveryPhase.ACTIVE
    deadline_at = active.action_lease.hard_expires_at
    cleaning_plan = FuturesRecoveryDecisionEngine().plan_round(
        snapshot=SymbolSnapshot(
            symbol=active.symbol,
            captured_at=deadline_at,
            assessment=FlowBlockerAssessment(
                inventory_reduce_sides=(Side.SELL,),
                loss_only_blocked_sides=(Side.SELL,),
            ),
            managed_order_manifest=_managed_manifest(
                held.next_state,
                evidence.accepted_order_pairs,
            ),
        ),
        state=held.next_state,
        now=deadline_at,
        round_id=f"deadline-{accepted_status.lower()}",
    )
    cleaning = cleaning_plan.next_state
    assert cleaning.phase is RecoveryPhase.CLEANING
    assert cleaning_plan.effect_stage is EffectStage.MANAGED_GTX_CANCEL
    assert cleaning.cleanup_obligation is not None
    assert cleaning.cleanup_obligation.open_order_ids == ("701",)
    assert cleaning.cleanup_obligation.needs_manifest_rebuild is False

    unrelated = _owned_open_order("888")
    first = [unrelated]
    if owned_order_still_open:
        first = [
            _owned_open_order(
                "701",
                client_order_id=client_order_id,
            ),
            unrelated,
        ]
    snapshots = [first, [unrelated]]
    canceled: list[str] = []
    cleanup = reconcile_temporary_loss_cleanup(
        recovery_state=cleaning,
        observed_at=deadline_at + timedelta(seconds=1),
        fetch_open_orders=lambda _symbol: snapshots.pop(0),
        cancel_order=lambda _symbol, order_id: (
            canceled.append(order_id)
            or {
                "symbol": active.symbol,
                "orderId": int(order_id),
                "status": "CANCELED",
            }
        ),
        fetch_user_trades_page=lambda _symbol, start_ms, _end_ms, _limit: [
            {"id": 1201, "orderId": 701, "time": start_ms + 1}
        ],
    )
    assert tuple(canceled) == expected_canceled
    assert cleanup.cleanup_proof is not None
    assert cleanup.cleanup_proof.remaining_order_ids == ()
    restored = FuturesRecoveryDecisionEngine().plan_round(
        snapshot=SymbolSnapshot(
            symbol=cleaning.symbol,
            captured_at=deadline_at + timedelta(seconds=1),
            assessment=FlowBlockerAssessment(
                inventory_reduce_sides=(Side.SELL,),
                loss_only_blocked_sides=(Side.SELL,),
            ),
            effect_receipt=cleanup.effect_receipt,
            cleanup_proof=cleanup.cleanup_proof,
        ),
        state=cleaning,
        now=deadline_at + timedelta(seconds=1),
        round_id=f"cleanup-{accepted_status.lower()}",
    ).next_state
    assert restored.phase is RecoveryPhase.RESTORING


def test_fenced_inventory_deadline_escalates_to_typed_loss_and_returns_baseline(
    tmp_path,
) -> None:
    engine = FuturesRecoveryDecisionEngine()
    state = RecoveryState.initial("BCHUSDT", BASELINE, now=NOW)
    plan_report, submit_report = _admission_documents(
        state,
        plan_at=NOW - timedelta(seconds=2),
        submit_at=NOW - timedelta(seconds=1),
    )
    admission = load_recovery_admission_evidence(
        plan=plan_report,
        submit=submit_report,
        recovery_state=state,
        captured_at=NOW,
        max_age_seconds=30,
    )
    assert Side.SELL in admission.loss_only_blocked_sides
    blocked = FlowBlockerAssessment(
        inventory_reduce_sides=(Side.SELL,),
        loss_only_blocked_sides=(Side.SELL,),
        episode_fingerprint="bch-inventory-loss-episode",
    )

    inventory = engine.plan_round(
        snapshot=SymbolSnapshot(
            symbol=state.symbol,
            captured_at=NOW,
            assessment=blocked,
        ),
        state=state,
        now=NOW,
        round_id="inventory-enter",
    )
    state = inventory.next_state
    assert state.active_action is ActionId.INVENTORY_RECOVER
    assert state.phase is RecoveryPhase.ACTIVE
    assert inventory.effect_stage is EffectStage.RUNNER_RESTART
    assert state.progress_deadline_at is not None

    inventory_deadline = state.progress_deadline_at
    inventory_timeout = engine.plan_round(
        snapshot=SymbolSnapshot(
            symbol=state.symbol,
            captured_at=inventory_deadline,
            assessment=blocked,
            effect_receipt=EffectReceipt(
                decision_id=str(state.decision_id),
                stage=EffectStage.RUNNER_RESTART,
                effect_epoch=int(inventory.effect_epoch),
                observed_at=NOW + timedelta(seconds=1),
            ),
        ),
        state=state,
        now=inventory_deadline,
        round_id="inventory-deadline",
    )
    state = inventory_timeout.next_state
    assert state.phase is RecoveryPhase.CLEANING
    assert state.attempt_exhausted(
        ActionId.INVENTORY_RECOVER,
        Side.SELL,
        OrderRole.REDUCE_ONLY,
    )
    assert inventory_timeout.effect_stage is EffectStage.LOCAL_STATE_REPAIR
    obligation = state.cleanup_obligation
    assert obligation is not None
    cleanup_at = inventory_deadline + timedelta(seconds=1)
    inventory_cleaned = engine.plan_round(
        snapshot=SymbolSnapshot(
            symbol=state.symbol,
            captured_at=cleanup_at,
            assessment=blocked,
            cleanup_proof=CleanupProof(
                source_decision_id=obligation.source_decision_id,
                source_generation=obligation.managed_order_manifest.generation,
                manifest_digest=obligation.managed_order_manifest.digest,
                remaining_order_ids=(),
                user_trades_watermark=0,
                observed_at=cleanup_at,
            ),
        ),
        state=state,
        now=cleanup_at,
        round_id="inventory-cleaned",
    )
    state = inventory_cleaned.next_state
    assert state.phase is RecoveryPhase.RESTORING
    assert inventory_cleaned.effect_stage is EffectStage.RUNNER_RESTART

    inventory_applied_at = inventory_deadline + timedelta(seconds=2)
    inventory_restored = engine.plan_round(
        snapshot=SymbolSnapshot(
            symbol=state.symbol,
            captured_at=inventory_applied_at,
            assessment=blocked,
            applied_generation=state.generation,
            applied_profile_digest=state.desired_profile.digest,
        ),
        state=state,
        now=inventory_applied_at,
        round_id="inventory-restored",
    )
    state = inventory_restored.next_state
    assert state.phase is RecoveryPhase.COOLDOWN
    assert state.active_episode_fingerprint == "bch-inventory-loss-episode"
    assert state.cooldown_until is not None

    cooldown_finished = engine.plan_round(
        snapshot=SymbolSnapshot(
            symbol=state.symbol,
            captured_at=state.cooldown_until,
            assessment=blocked,
        ),
        state=state,
        now=state.cooldown_until,
        round_id="inventory-cooldown-finished",
    )
    state = cooldown_finished.next_state
    assert state.phase is RecoveryPhase.STABLE
    assert state.attempt_exhausted(
        ActionId.INVENTORY_RECOVER,
        Side.SELL,
        OrderRole.REDUCE_ONLY,
    )

    loss_entered_at = cooldown_finished.next_state.cooldown_until or (
        inventory_applied_at + timedelta(minutes=2)
    )
    loss_entered_at = max(
        inventory_applied_at + timedelta(minutes=1, seconds=1),
        loss_entered_at,
    )
    loss = engine.plan_round(
        snapshot=SymbolSnapshot(
            symbol=state.symbol,
            captured_at=loss_entered_at,
            assessment=blocked,
        ),
        state=state,
        now=loss_entered_at,
        round_id="typed-loss-enter",
    )
    state = loss.next_state
    assert state.phase is RecoveryPhase.SETTLING
    assert state.active_action is ActionId.TEMPORARY_LOSS_RELIEF
    assert state.action_lease is not None
    assert state.desired_profile.fields[
        "best_quote_maker_volume_allow_loss_reduce_only"
    ] is False

    activated_at = loss_entered_at + timedelta(seconds=1)
    activation = engine.plan_round(
        snapshot=SymbolSnapshot(
            symbol=state.symbol,
            captured_at=activated_at,
            assessment=blocked,
            activation_receipt=ActivationReceipt(
                decision_id=str(state.decision_id),
                generation=state.generation,
                profile_digest=state.desired_profile.digest,
                action_lease_epoch=state.action_lease.epoch,
                observed_at=activated_at,
            ),
        ),
        state=state,
        now=activated_at,
        round_id="typed-loss-activate",
    )
    state = activation.next_state
    assert temporary_loss_authorized(state, now=activated_at)

    progress_at = activated_at + timedelta(seconds=1)
    client_order_id = _temporary_loss_test_client_id(state, "901")
    _write_journal(
        tmp_path,
        [
            _event(
                state,
                kind="progress",
                seq=0,
                observed_at=progress_at,
                order_ids=["901"],
                client_order_ids=[client_order_id],
                accepted_statuses=["FILLED"],
            )
        ],
        symbol=state.symbol,
    )
    progress = load_temporary_loss_runtime_evidence(
        state_path=tmp_path / "bchusdt_loop_state.json",
        recovery_state=state,
        captured_at=progress_at,
    )
    held = engine.plan_round(
        snapshot=SymbolSnapshot(
            symbol=state.symbol,
            captured_at=progress_at,
            assessment=blocked,
            progress_receipts=progress.progress_receipts,
            managed_order_manifest=_managed_manifest(
                state,
                progress.accepted_order_pairs,
            ),
        ),
        state=state,
        now=progress_at,
        round_id="typed-loss-progress",
    )
    assert held.next_state.phase is RecoveryPhase.ACTIVE
    deadline_at = state.action_lease.hard_expires_at
    cleaning_plan = engine.plan_round(
        snapshot=SymbolSnapshot(
            symbol=state.symbol,
            captured_at=deadline_at,
            assessment=blocked,
            managed_order_manifest=_managed_manifest(
                held.next_state,
                progress.accepted_order_pairs,
            ),
        ),
        state=held.next_state,
        now=deadline_at,
        round_id="typed-loss-deadline",
    )
    state = cleaning_plan.next_state
    assert state.phase is RecoveryPhase.CLEANING
    assert state.cleanup_obligation is not None
    assert state.cleanup_obligation.open_order_ids == ("901",)
    assert state.cleanup_obligation.needs_manifest_rebuild is False

    cleanup_at = deadline_at + timedelta(seconds=1)
    cleanup = reconcile_temporary_loss_cleanup(
        recovery_state=state,
        observed_at=cleanup_at,
        fetch_open_orders=lambda _symbol: [],
        cancel_order=lambda _symbol, _order_id: (_ for _ in ()).throw(
            AssertionError("FILLED typed order was canceled")
        ),
        fetch_user_trades_page=lambda _symbol, start_ms, _end_ms, _limit: [
            {"id": 1901, "orderId": 901, "time": start_ms + 1}
        ],
    )
    restored = engine.plan_round(
        snapshot=SymbolSnapshot(
            symbol=state.symbol,
            captured_at=cleanup_at,
            assessment=blocked,
            effect_receipt=cleanup.effect_receipt,
            cleanup_proof=cleanup.cleanup_proof,
        ),
        state=state,
        now=cleanup_at,
        round_id="typed-loss-cleanup",
    )
    state = restored.next_state
    assert state.phase is RecoveryPhase.RESTORING
    assert state.action_lease is None
    assert state.desired_profile.fields[
        "best_quote_maker_volume_allow_loss_reduce_only"
    ] is False

    baseline_applied_at = cleanup_at + timedelta(seconds=1)
    baseline_applied = engine.plan_round(
        snapshot=SymbolSnapshot(
            symbol=state.symbol,
            captured_at=baseline_applied_at,
            assessment=FlowBlockerAssessment(),
            applied_generation=state.generation,
            applied_profile_digest=state.desired_profile.digest,
        ),
        state=state,
        now=baseline_applied_at,
        round_id="typed-loss-baseline-applied",
    )
    state = baseline_applied.next_state
    assert state.phase is RecoveryPhase.COOLDOWN
    assert state.active_episode_fingerprint is None
    assert state.cooldown_until is not None

    final = engine.plan_round(
        snapshot=SymbolSnapshot(
            symbol=state.symbol,
            captured_at=state.cooldown_until,
            assessment=FlowBlockerAssessment(),
        ),
        state=state,
        now=state.cooldown_until,
        round_id="typed-loss-finished",
    ).next_state
    assert final.phase is RecoveryPhase.STABLE
    assert final.active_action is ActionId.NOOP
    assert final.exhausted_attempts == ()
    assert final.desired_profile.digest == final.baseline_profile.digest
    assert final.desired_profile.fields[
        "best_quote_maker_volume_allow_loss_reduce_only"
    ] is False
    assert final.desired_profile.fields["volatility_entry_pause_enabled"] is True
    assert final.desired_profile.execution_policy.time_in_force == "GTX"


def test_temporary_loss_runtime_adapter_completes_one_action_state_machine(
    tmp_path,
) -> None:
    state_path = tmp_path / "arxusdt_loop_state.json"
    store = InMemoryRecoveryStore()
    initial = store.register_symbol("ARXUSDT", BASELINE, now=NOW)
    seeded = initial.with_exhausted_attempt(
        ActionId.INVENTORY_RECOVER,
        Side.SELL,
        OrderRole.REDUCE_ONLY,
        now=NOW,
    )
    store.compare_and_swap(
        "ARXUSDT",
        expected_revision=0,
        next_state=replace(seeded, document_revision=1),
    )

    journal_events: list[dict[str, object]] = []
    current_now = NOW
    latest_effect_receipt: EffectReceipt | None = None
    cleanup_result = None
    effect_stages: list[EffectStage] = []
    canceled_order_ids: list[str] = []
    open_order_snapshots: list[list[dict[str, object]]] = []

    active_assessment = FlowBlockerAssessment(
        inventory_reduce_sides=(Side.SELL,),
        loss_only_blocked_sides=(Side.SELL,),
        episode_fingerprint="end-to-end-lease",
    )

    def snapshot_provider(
        symbol: str,
        now: datetime,
        state: RecoveryState,
    ) -> SymbolSnapshot:
        nonlocal latest_effect_receipt
        evidence = None
        if state.phase in {
            RecoveryPhase.SETTLING,
            RecoveryPhase.ACTIVE,
            RecoveryPhase.RESTORING,
        }:
            evidence = load_temporary_loss_runtime_evidence(
                state_path=state_path,
                recovery_state=state,
                captured_at=now,
            )
        return SymbolSnapshot(
            symbol=symbol,
            captured_at=now,
            assessment=(
                active_assessment
                if state.phase in {RecoveryPhase.STABLE, RecoveryPhase.SETTLING, RecoveryPhase.ACTIVE}
                else FlowBlockerAssessment()
            ),
            activation_receipt=(
                evidence.activation_receipt if evidence is not None else None
            ),
            progress_receipts=(
                evidence.progress_receipts if evidence is not None else ()
            ),
            managed_order_manifest=(
                _managed_manifest(state, evidence.accepted_order_pairs)
                if evidence is not None
                and state.phase is RecoveryPhase.ACTIVE
                else None
            ),
            effect_receipt=(
                cleanup_result.effect_receipt
                if state.phase is RecoveryPhase.CLEANING
                and cleanup_result is not None
                else latest_effect_receipt
            ),
            cleanup_proof=(
                cleanup_result.cleanup_proof
                if state.phase is RecoveryPhase.CLEANING
                and cleanup_result is not None
                else None
            ),
            applied_generation=(
                evidence.applied_generation if evidence is not None else None
            ),
            applied_profile_digest=(
                evidence.applied_profile_digest if evidence is not None else None
            ),
        )

    def execute_effect(symbol: str, command) -> None:
        nonlocal latest_effect_receipt, cleanup_result
        effect_stages.append(command.stage)
        if command.stage is EffectStage.RUNNER_RESTART:
            latest_effect_receipt = EffectReceipt(
                decision_id=command.decision_id,
                stage=command.stage,
                effect_epoch=command.effect_epoch,
                observed_at=current_now,
            )
            return
        assert command.stage is EffectStage.MANAGED_GTX_CANCEL
        cleanup_result = reconcile_temporary_loss_cleanup(
            recovery_state=store.read(symbol),
            observed_at=current_now,
            fetch_open_orders=lambda _symbol: open_order_snapshots.pop(0),
            cancel_order=lambda _symbol, order_id: (
                canceled_order_ids.append(order_id)
                or {
                    "symbol": symbol,
                    "orderId": int(order_id),
                    "status": "CANCELED",
                }
            ),
            fetch_user_trades_page=lambda _symbol, start_ms, _end_ms, _limit: [
                {"id": 1201, "orderId": 701, "time": start_ms + 1}
            ],
        )

    coordinator = FuturesRecoveryCoordinator(
        store=store,
        snapshot_provider=snapshot_provider,
        effect_executor=execute_effect,
    )

    def reconcile_once(round_id: str, at: datetime):
        nonlocal current_now
        current_now = at
        commits_before = store.commit_count("ARXUSDT")
        effects_before = len(effect_stages)
        plan = coordinator.reconcile_symbol(
            "ARXUSDT",
            now=at,
            round_id=round_id,
        )
        assert store.commit_count("ARXUSDT") == commits_before + 1
        assert len(effect_stages) - effects_before <= 1
        return plan

    entered = reconcile_once("e2e-enter", NOW)
    settling = entered.next_state
    assert settling.phase is RecoveryPhase.SETTLING
    assert settling.desired_profile.fields[
        "best_quote_maker_volume_allow_loss_reduce_only"
    ] is False

    activation_at = NOW + timedelta(seconds=1)
    journal_events.append(
        _event(
            settling,
            kind="activation",
            seq=0,
            observed_at=activation_at,
        )
    )
    _write_journal(tmp_path, journal_events)
    activated = reconcile_once("e2e-activate", activation_at)
    active = activated.next_state
    assert active.phase is RecoveryPhase.ACTIVE
    assert temporary_loss_authorized(active, now=activation_at)
    client_order_id = _temporary_loss_test_client_id(active, "701")
    unrelated = _owned_open_order("888")
    open_order_snapshots.extend(
        (
            [
                _owned_open_order(
                    "701",
                    client_order_id=client_order_id,
                ),
                unrelated,
            ],
            [unrelated],
        )
    )

    progress_at = activation_at + timedelta(seconds=1)
    journal_events.append(
        _event(
            active,
            kind="progress",
            seq=1,
            observed_at=progress_at,
            order_ids=["701"],
            client_order_ids=[client_order_id],
        )
    )
    assert active.action_lease is not None
    expiry_at = active.action_lease.hard_expires_at
    journal_events.extend(
        (
            _event(active, kind="expiry", seq=2, observed_at=expiry_at),
            _event(
                active,
                kind="cleanup_needed",
                seq=3,
                observed_at=expiry_at,
                order_ids=["701"],
                client_order_ids=[client_order_id],
            ),
        )
    )
    _write_journal(tmp_path, journal_events)
    expired = reconcile_once("e2e-expire", expiry_at)
    cleaning = expired.next_state
    assert cleaning.phase is RecoveryPhase.CLEANING
    assert cleaning.cleanup_obligation is not None
    assert cleaning.cleanup_obligation.open_order_ids == ("701",)
    assert canceled_order_ids == ["701"]
    assert cleanup_result is not None
    assert cleanup_result.cleanup_proof is not None

    cleanup_at = expiry_at + timedelta(seconds=1)
    cleaned = reconcile_once("e2e-cleanup-proof", cleanup_at)
    restoring = cleaned.next_state
    assert restoring.phase is RecoveryPhase.RESTORING
    assert restoring.action_lease is None
    assert restoring.desired_profile.fields[
        "best_quote_maker_volume_allow_loss_reduce_only"
    ] is False

    restoration_at = cleanup_at + timedelta(seconds=1)
    journal_events.append(
        _restoration_event(
            restoring,
            seq=4,
            observed_at=restoration_at,
        )
    )
    _write_journal(tmp_path, journal_events)
    restored = reconcile_once("e2e-restored", restoration_at)
    cooldown = restored.next_state
    assert cooldown.phase is RecoveryPhase.COOLDOWN
    assert cooldown.cooldown_until is not None

    stable = reconcile_once("e2e-stable", cooldown.cooldown_until).next_state
    assert stable.phase is RecoveryPhase.STABLE
    assert stable.active_action is ActionId.NOOP
    assert stable.action_lease is None
    assert stable.desired_profile.fields[
        "best_quote_maker_volume_allow_loss_reduce_only"
    ] is False
    assert stable.desired_profile.fields["volatility_entry_pause_enabled"] is True
    assert stable.desired_profile.execution_policy.time_in_force == "GTX"
    assert effect_stages == [
        EffectStage.RUNNER_RESTART,
        EffectStage.RUNNER_RESTART,
        EffectStage.MANAGED_GTX_CANCEL,
        EffectStage.RUNNER_RESTART,
    ]
