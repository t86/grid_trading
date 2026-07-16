from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from grid_optimizer import futures_managed_order_manifest as manifest_module
from grid_optimizer import futures_recovery_runtime_adapter as runtime_module
from grid_optimizer.futures_managed_order_manifest import (
    build_managed_order_client_order_id,
    ordinary_recovery_client_order_prefix,
)
from grid_optimizer.futures_recovery_coordinator import (
    ActionId,
    CleanupProof,
    EffectStage,
    FlowBlockerAssessment,
    FuturesRecoveryDecisionEngine,
    OrderRole,
    ProgressReceipt,
    RecoveryPhase,
    RecoveryState,
    Side,
    SymbolSnapshot,
)
from grid_optimizer.futures_recovery_store import JsonRecoveryStore


NOW = datetime(2026, 7, 16, 3, 0, tzinfo=timezone.utc)
BASELINE = {
    "best_quote_maker_volume_allow_loss_reduce_only": False,
    "best_quote_maker_volume_net_loss_reduce_enabled": False,
    "hard_loss_forced_reduce_enabled": False,
    "volatility_entry_pause_enabled": True,
}


def _managed_types():
    identity_type = getattr(manifest_module, "ManagedOrderIdentity", None)
    manifest_type = getattr(manifest_module, "ManagedOrderManifest", None)
    assert identity_type is not None, "ManagedOrderIdentity must be implemented"
    assert manifest_type is not None, "ManagedOrderManifest must be implemented"
    return identity_type, manifest_type


def _enter_maker_flow():
    engine = FuturesRecoveryDecisionEngine()
    assessment = FlowBlockerAssessment(
        missing_entry_sides=(Side.BUY,),
        episode_fingerprint="managed-cleanup-maker-buy",
    )
    entered = engine.plan_round(
        snapshot=SymbolSnapshot(
            symbol="ARXUSDT",
            captured_at=NOW,
            assessment=assessment,
        ),
        state=RecoveryState.initial("ARXUSDT", BASELINE, now=NOW),
        now=NOW,
        round_id="managed-cleanup-enter",
    )
    state = entered.next_state
    assert state.phase is RecoveryPhase.ACTIVE
    assert state.active_action is ActionId.MAKER_FLOW_RECOVER
    assert state.side is Side.BUY
    assert state.order_role is OrderRole.ENTRY
    return engine, assessment, state


def _manifest_for(state: RecoveryState, *pairs: tuple[str, str]):
    identity_type, manifest_type = _managed_types()
    assert state.decision_id is not None
    assert state.issued_at is not None
    return manifest_type(
        symbol=state.symbol,
        generation=state.generation,
        decision_id=state.decision_id,
        profile_digest=state.desired_profile.digest,
        action_id=state.active_action,
        side=state.side,
        order_role=state.order_role,
        issued_at=state.issued_at,
        orders=tuple(
            identity_type(order_id=order_id, client_order_id=client_order_id)
            for order_id, client_order_id in pairs
        ),
        action_lease_epoch=(
            state.action_lease.epoch if state.action_lease is not None else None
        ),
        hard_expires_at=(
            state.action_lease.hard_expires_at
            if state.action_lease is not None
            else None
        ),
    )


def _managed_client_order_id(state: RecoveryState, *, nonce_ns: int = 1) -> str:
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
        order_index=1,
        nonce_ns=nonce_ns,
    )


def _fill_receipt(state: RecoveryState, *, observed_at: datetime) -> ProgressReceipt:
    assert state.decision_id is not None
    return ProgressReceipt(
        decision_id=state.decision_id,
        generation=state.generation,
        side=state.side,
        order_role=state.order_role,
        observation_seq=801,
        observed_at=observed_at,
    )


def _exit_with_fill(*, exchange_observation_available: bool = True):
    engine, assessment, active = _enter_maker_flow()
    client_order_id = _managed_client_order_id(active)
    manifest = _manifest_for(active, ("801", client_order_id))
    filled_at = NOW + timedelta(seconds=1)
    exited = engine.plan_round(
        snapshot=SymbolSnapshot(
            symbol=active.symbol,
            captured_at=filled_at,
            assessment=assessment,
            progress_receipts=(_fill_receipt(active, observed_at=filled_at),),
            managed_order_manifest=manifest,
            exchange_observation_available=exchange_observation_available,
        ),
        state=active,
        now=filled_at,
        round_id="managed-cleanup-fill",
    )
    return active, manifest, exited


def _deadline_without_journal():
    engine, assessment, active = _enter_maker_flow()
    assert active.progress_deadline_at is not None
    deadline = active.progress_deadline_at
    exited = engine.plan_round(
        snapshot=SymbolSnapshot(
            symbol=active.symbol,
            captured_at=deadline,
            assessment=assessment,
            exchange_observation_available=False,
        ),
        state=active,
        now=deadline,
        round_id="managed-cleanup-no-journal",
    )
    return active, exited


def _owned_order(
    *,
    order_id: str,
    client_order_id: str,
    order_type: str = "LIMIT",
    time_in_force: str = "GTX",
    side: str = "BUY",
    position_side: str = "LONG",
) -> dict[str, object]:
    return {
        "symbol": "ARXUSDT",
        "orderId": int(order_id),
        "clientOrderId": client_order_id,
        "type": order_type,
        "timeInForce": time_in_force,
        "side": side,
        "positionSide": position_side,
        "status": "NEW",
    }


def test_ordinary_fill_enters_cleaning_before_runner_restart() -> None:
    _active, manifest, exited = _exit_with_fill()

    assert exited.next_state.phase is RecoveryPhase.CLEANING
    assert exited.effect_stage is EffectStage.MANAGED_GTX_CANCEL
    assert exited.effect_stage is not EffectStage.RUNNER_RESTART
    obligation = exited.next_state.cleanup_obligation
    assert obligation is not None
    assert obligation.managed_order_manifest == manifest
    assert obligation.needs_manifest_rebuild is False
    assert manifest.orders[0].order_id == "801"
    assert manifest.orders[0].client_order_id
    assert len(manifest.digest) == 64


def test_ordinary_missing_journal_preserves_binding_for_local_repair() -> None:
    source, exited = _deadline_without_journal()

    assert exited.next_state.phase is RecoveryPhase.CLEANING
    assert exited.effect_stage is EffectStage.LOCAL_STATE_REPAIR
    obligation = exited.next_state.cleanup_obligation
    assert obligation is not None
    assert obligation.needs_manifest_rebuild is True
    binding = obligation.managed_order_manifest
    assert binding is not None
    assert binding.orders == ()
    assert binding.symbol == source.symbol
    assert binding.generation == source.generation
    assert binding.decision_id == source.decision_id
    assert binding.profile_digest == source.desired_profile.digest
    assert binding.action_id is source.active_action
    assert binding.side is source.side
    assert binding.order_role is source.order_role


def test_nonempty_authenticated_manifest_uses_managed_cancel_when_preflight_failed() -> (
    None
):
    _active, manifest, exited = _exit_with_fill(
        exchange_observation_available=False
    )

    assert exited.next_state.phase is RecoveryPhase.CLEANING
    assert exited.effect_stage is EffectStage.MANAGED_GTX_CANCEL
    obligation = exited.next_state.cleanup_obligation
    assert obligation is not None
    assert obligation.managed_order_manifest == manifest
    assert obligation.needs_manifest_rebuild is False


@pytest.mark.parametrize(
    ("assessment", "expected_action", "expected_effect"),
    [
        (
            FlowBlockerAssessment(
                safety_reasons=("volatility",),
                safety_evidence_fingerprint="managed-cleanup-safety-2",
                safety_observation_seq=2,
                safety_observed_at=NOW + timedelta(seconds=1),
            ),
            ActionId.SAFETY_CONVERGE,
            EffectStage.MANAGED_GTX_CANCEL,
        ),
        (
            FlowBlockerAssessment(runner_faults=("runner_inactive",)),
            ActionId.RUNNER_RECOVER,
            EffectStage.MANAGED_GTX_CANCEL,
        ),
        (
            FlowBlockerAssessment(
                terminal_reason="trusted_lifecycle_terminal_handoff",
                terminal_lifecycle_handoff=True,
            ),
            ActionId.TERMINAL_HANDOFF,
            EffectStage.MANAGED_GTX_CANCEL,
        ),
        (
            FlowBlockerAssessment(
                terminal_reason="competition_target_confirmed",
                terminal_clean_flat_proof=True,
            ),
            ActionId.TERMINAL_STOP,
            EffectStage.RUNNER_STOP,
        ),
    ],
)
def test_ordinary_preemption_preserves_manifest_in_cleaning_before_successor(
    assessment: FlowBlockerAssessment,
    expected_action: ActionId,
    expected_effect: EffectStage,
) -> None:
    engine, _ordinary_assessment, active = _enter_maker_flow()
    manifest = _manifest_for(active, ("801", _managed_client_order_id(active)))
    preempted_at = NOW + timedelta(seconds=1)

    preempted = engine.plan_round(
        snapshot=SymbolSnapshot(
            symbol=active.symbol,
            captured_at=preempted_at,
            assessment=assessment,
            managed_order_manifest=manifest,
        ),
        state=active,
        now=preempted_at,
        round_id=f"managed-cleanup-preempt-{expected_action.value}",
    )

    assert preempted.action_id is expected_action
    assert preempted.next_state.phase is RecoveryPhase.CLEANING
    assert preempted.effect_stage is expected_effect
    obligation = preempted.next_state.cleanup_obligation
    assert obligation is not None
    assert obligation.managed_order_manifest == manifest
    assert preempted.next_state.cleanup_successor_action is expected_action


def test_generic_cleanup_cancels_only_exact_owned_limit_gtx_identity() -> None:
    _active, manifest, exited = _exit_with_fill()
    reconcile = getattr(runtime_module, "reconcile_managed_gtx_cleanup", None)
    assert reconcile is not None, "reconcile_managed_gtx_cleanup must be implemented"
    owned = _owned_order(
        order_id="801",
        client_order_id=manifest.orders[0].client_order_id,
    )
    unrelated = [
        _owned_order(order_id="802", client_order_id="manual-802"),
        _owned_order(
            order_id="803", client_order_id="wrong-type", order_type="MARKET"
        ),
        _owned_order(
            order_id="804", client_order_id="wrong-tif", time_in_force="GTC"
        ),
        _owned_order(
            order_id="805", client_order_id="wrong-side", side="SELL"
        ),
        _owned_order(
            order_id="806", client_order_id="wrong-position", position_side="SHORT"
        ),
    ]
    snapshots = [[owned, *unrelated], unrelated]
    cancel_calls: list[tuple[str, str]] = []

    def cancel_order(symbol: str, order_id: str):
        cancel_calls.append((symbol, order_id))
        return {"symbol": symbol, "orderId": int(order_id), "status": "CANCELED"}

    result = reconcile(
        recovery_state=exited.next_state,
        observed_at=NOW + timedelta(seconds=2),
        fetch_open_orders=lambda _symbol: snapshots.pop(0),
        cancel_order=cancel_order,
        fetch_user_trades_page=lambda _symbol, _start, _end, _limit: [],
    )

    assert cancel_calls == [("ARXUSDT", "801")]
    assert result.canceled_order_ids == ("801",)
    assert result.remaining_order_ids == ()
    assert result.cleanup_proof is not None


def test_generic_cleanup_client_id_mismatch_is_fail_closed_before_any_cancel() -> (
    None
):
    _active, _manifest, exited = _exit_with_fill()
    reconcile = getattr(runtime_module, "reconcile_managed_gtx_cleanup", None)
    assert reconcile is not None, "reconcile_managed_gtx_cleanup must be implemented"
    cancel_calls: list[tuple[str, str]] = []

    with pytest.raises(RuntimeError, match="client.*(?:mismatch|match)|clientOrderId"):
        reconcile(
            recovery_state=exited.next_state,
            observed_at=NOW + timedelta(seconds=2),
            fetch_open_orders=lambda _symbol: [
                _owned_order(order_id="801", client_order_id="mismatched-client")
            ],
            cancel_order=lambda symbol, order_id: cancel_calls.append(
                (symbol, order_id)
            ),
            fetch_user_trades_page=lambda _symbol, _start, _end, _limit: [],
        )

    assert cancel_calls == []


def test_cleanup_supplements_a_truncated_128_order_manifest_from_decision_prefix() -> (
    None
):
    engine, assessment, active = _enter_maker_flow()
    # The persisted receipt journal retains only its newest 128 accepted orders.
    # Order 1000 models the still-open order that fell out of that bounded ring.
    retained_pairs = tuple(
        (
            str(order_id),
            _managed_client_order_id(active, nonce_ns=order_id),
        )
        for order_id in range(1001, 1129)
    )
    manifest = _manifest_for(active, *retained_pairs)
    filled_at = NOW + timedelta(seconds=1)
    exited = engine.plan_round(
        snapshot=SymbolSnapshot(
            symbol=active.symbol,
            captured_at=filled_at,
            assessment=assessment,
            progress_receipts=(_fill_receipt(active, observed_at=filled_at),),
            managed_order_manifest=manifest,
        ),
        state=active,
        now=filled_at,
        round_id="managed-cleanup-truncated-manifest",
    )
    assert exited.next_state.phase is RecoveryPhase.CLEANING
    assert len(manifest.orders) == 128

    omitted_pair = (
        "1000",
        _managed_client_order_id(active, nonce_ns=1000),
    )
    before = [
        _owned_order(order_id=order_id, client_order_id=client_order_id)
        for order_id, client_order_id in (omitted_pair, *retained_pairs)
    ]
    snapshots = [before, []]
    cancel_calls: list[tuple[str, str]] = []

    def cancel_order(symbol: str, order_id: str):
        cancel_calls.append((symbol, order_id))
        return {"symbol": symbol, "orderId": int(order_id), "status": "CANCELED"}

    result = runtime_module.reconcile_managed_gtx_cleanup(
        recovery_state=exited.next_state,
        observed_at=NOW + timedelta(seconds=2),
        fetch_open_orders=lambda _symbol: snapshots.pop(0),
        cancel_order=cancel_order,
        fetch_user_trades_page=lambda _symbol, _start, _end, _limit: [],
    )

    expected_order_ids = tuple(str(order_id) for order_id in range(1000, 1129))
    assert tuple(order_id for _symbol, order_id in cancel_calls) == expected_order_ids
    assert result.canceled_order_ids == expected_order_ids
    assert result.remaining_order_ids == ()
    assert result.cleanup_proof is not None


@pytest.mark.parametrize(
    "invalid_open_order",
    [
        {"order_id": "801", "client_order_id": "alternate_owned_client"},
        {"order_type": "MARKET"},
        {"time_in_force": "GTC"},
        {"side": "SELL"},
        {"position_side": "SHORT"},
    ],
)
def test_cleanup_rejects_an_invalid_prefix_supplement_before_any_cancel(
    invalid_open_order: dict[str, str],
) -> None:
    _active, manifest, exited = _exit_with_fill()
    omitted_client_order_id = _managed_client_order_id(_active, nonce_ns=800)
    invalid_fields = dict(invalid_open_order)
    order_id = invalid_fields.pop("order_id", "800")
    client_order_id = invalid_fields.pop(
        "client_order_id",
        omitted_client_order_id,
    )
    if client_order_id == "alternate_owned_client":
        client_order_id = _managed_client_order_id(_active, nonce_ns=802)
    invalid = _owned_order(
        order_id=order_id,
        client_order_id=client_order_id,
        **invalid_fields,
    )
    retained = _owned_order(
        order_id="801",
        client_order_id=manifest.orders[0].client_order_id,
    )
    open_orders = [invalid]
    if order_id != "801":
        open_orders.append(retained)
    cancel_calls: list[tuple[str, str]] = []

    with pytest.raises((RuntimeError, ValueError), match="owned order|client"):
        runtime_module.reconcile_managed_gtx_cleanup(
            recovery_state=exited.next_state,
            observed_at=NOW + timedelta(seconds=2),
            fetch_open_orders=lambda _symbol: open_orders,
            cancel_order=lambda symbol, order_id: cancel_calls.append(
                (symbol, order_id)
            ),
            fetch_user_trades_page=lambda _symbol, _start, _end, _limit: [],
        )

    assert cancel_calls == []


def test_cleanup_rejects_persisted_manifest_outside_exact_decision_prefix() -> None:
    engine, assessment, active = _enter_maker_flow()
    invalid_manifest = _manifest_for(active, ("801", "manual-same-side-gtx"))
    filled_at = NOW + timedelta(seconds=1)
    cleaning = engine.plan_round(
        snapshot=SymbolSnapshot(
            symbol=active.symbol,
            captured_at=filled_at,
            assessment=assessment,
            progress_receipts=(_fill_receipt(active, observed_at=filled_at),),
            managed_order_manifest=invalid_manifest,
        ),
        state=active,
        now=filled_at,
        round_id="managed-cleanup-invalid-persisted-prefix",
    ).next_state
    cancel_calls: list[tuple[str, str]] = []

    with pytest.raises(
        (RuntimeError, ValueError),
        match="decision-bound client order prefix",
    ):
        runtime_module.reconcile_managed_gtx_cleanup(
            recovery_state=cleaning,
            observed_at=NOW + timedelta(seconds=2),
            fetch_open_orders=lambda _symbol: [
                _owned_order(
                    order_id="801",
                    client_order_id="manual-same-side-gtx",
                )
            ],
            cancel_order=lambda symbol, order_id: cancel_calls.append(
                (symbol, order_id)
            ),
            fetch_user_trades_page=lambda _symbol, _start, _end, _limit: [],
        )

    assert cancel_calls == []


def test_local_repair_rebuilds_only_the_exact_decision_bound_prefix() -> None:
    source, exited = _deadline_without_journal()
    reconcile = getattr(runtime_module, "reconcile_managed_gtx_cleanup", None)
    assert reconcile is not None, "reconcile_managed_gtx_cleanup must be implemented"
    exact_client_id = _managed_client_order_id(source, nonce_ns=2)
    same_symbol_unrelated = "gx-arxu-rc-deadbeef0000-unrelated"
    exact = _owned_order(order_id="901", client_order_id=exact_client_id)
    unrelated = _owned_order(
        order_id="902", client_order_id=same_symbol_unrelated
    )
    snapshots = [[exact, unrelated], [unrelated]]
    cancel_calls: list[tuple[str, str]] = []

    def cancel_order(symbol: str, order_id: str):
        cancel_calls.append((symbol, order_id))
        return {"symbol": symbol, "orderId": int(order_id), "status": "CANCELED"}

    result = reconcile(
        recovery_state=exited.next_state,
        observed_at=source.progress_deadline_at + timedelta(seconds=1),
        fetch_open_orders=lambda _symbol: snapshots.pop(0),
        cancel_order=cancel_order,
        fetch_user_trades_page=lambda _symbol, _start, _end, _limit: [],
    )

    assert cancel_calls == [("ARXUSDT", "901")]
    assert result.canceled_order_ids == ("901",)
    assert result.remaining_order_ids == ()
    assert result.cleanup_proof is not None


def test_delayed_ordinary_to_temporary_loss_cleanup_expires_into_restoring(
    tmp_path,
) -> None:
    engine, _ordinary_assessment, active = _enter_maker_flow()
    active = active.with_exhausted_attempt(
        ActionId.INVENTORY_RECOVER,
        Side.SELL,
        OrderRole.REDUCE_ONLY,
        now=NOW,
    )
    manifest = _manifest_for(active, ("801", _managed_client_order_id(active)))
    loss_assessment = FlowBlockerAssessment(
        inventory_reduce_sides=(Side.SELL,),
        loss_only_blocked_sides=(Side.SELL,),
        episode_fingerprint="managed-cleanup-delayed-loss-successor",
    )
    preempted_at = NOW + timedelta(seconds=1)
    preempted = engine.plan_round(
        snapshot=SymbolSnapshot(
            symbol=active.symbol,
            captured_at=preempted_at,
            assessment=loss_assessment,
            managed_order_manifest=manifest,
        ),
        state=active,
        now=preempted_at,
        round_id="managed-cleanup-delayed-loss-preempt",
    )

    assert preempted.action_id is ActionId.TEMPORARY_LOSS_RELIEF
    assert preempted.next_state.phase is RecoveryPhase.CLEANING
    assert (
        preempted.next_state.cleanup_successor_action
        is ActionId.TEMPORARY_LOSS_RELIEF
    )
    assert preempted.next_state.action_lease is not None
    assert (
        preempted.next_state.desired_profile.fields[
            "best_quote_maker_volume_allow_loss_reduce_only"
        ]
        is False
    )

    obligation = preempted.next_state.cleanup_obligation
    deadline = preempted.next_state.progress_deadline_at
    assert obligation is not None
    assert deadline is not None
    completed = engine.plan_round(
        snapshot=SymbolSnapshot(
            symbol=active.symbol,
            captured_at=deadline,
            assessment=loss_assessment,
            cleanup_proof=CleanupProof(
                source_decision_id=obligation.source_decision_id,
                source_generation=obligation.managed_order_manifest.generation,
                manifest_digest=obligation.managed_order_manifest.digest,
                remaining_order_ids=(),
                user_trades_watermark=0,
                observed_at=deadline,
            ),
        ),
        state=preempted.next_state,
        now=deadline,
        round_id="managed-cleanup-delayed-loss-proof-at-deadline",
    )

    assert completed.next_state.phase is RecoveryPhase.RESTORING
    assert completed.next_state.action_lease is None
    assert completed.next_state.desired_profile == completed.next_state.baseline_profile
    assert (
        completed.next_state.desired_profile.fields[
            "best_quote_maker_volume_allow_loss_reduce_only"
        ]
        is False
    )
    assert (
        completed.next_state.desired_profile.fields[
            "volatility_entry_pause_enabled"
        ]
        is True
    )

    store = JsonRecoveryStore(tmp_path / "arxusdt_loop_runner_control.json")
    store.register_symbol(active.symbol, BASELINE, now=NOW)
    store.compare_and_swap(
        active.symbol,
        expected_revision=0,
        next_state=active,
    )
    store.compare_and_swap(
        active.symbol,
        expected_revision=active.document_revision,
        next_state=preempted.next_state,
    )
    store.compare_and_swap(
        active.symbol,
        expected_revision=preempted.next_state.document_revision,
        next_state=completed.next_state,
    )
    persisted = store.read(active.symbol)
    assert persisted.phase is RecoveryPhase.RESTORING
    assert persisted.action_lease is None
