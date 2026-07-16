from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from types import MappingProxyType

import pytest

from grid_optimizer.bq_volume_recovery_guard import _restore_recovery_controls
from grid_optimizer.futures_recovery_coordinator import (
    ACTION_DEFINITIONS,
    ACTION_PRIORITY,
    ActivationReceipt,
    ActionId,
    ActionMode,
    CleanupProof,
    EffectCommand,
    EffectReceipt,
    EffectStage,
    FlowBlockerAssessment,
    FlowObservation,
    FlowRoleKey,
    FuturesRecoveryCoordinator,
    FuturesRecoveryDecisionEngine,
    InMemoryRecoveryStore,
    LedgerClass,
    ManagedOrderIdentity,
    ManagedOrderManifest,
    OrderRole,
    ProgressReceipt,
    RecoveryPhase,
    RecoveryPolicy,
    RecoveryState,
    Side,
    SymbolSnapshot,
    materialize_profile,
    temporary_loss_authorized,
)
from grid_optimizer.futures_managed_order_manifest import (
    build_managed_order_client_order_id,
    ordinary_recovery_client_order_prefix,
)
from grid_optimizer.futures_recovery_runtime_adapter import (
    temporary_loss_client_order_prefix_for_binding,
)


NOW = datetime(2026, 7, 15, 1, 0, tzinfo=timezone.utc)
BASELINE = {
    "best_quote_maker_volume_allow_loss_reduce_only": False,
    "best_quote_maker_volume_cycle_budget_notional": 360.0,
    "best_quote_maker_volume_directional_net_guard": "off",
    "best_quote_maker_volume_net_loss_reduce_enabled": False,
    "hard_loss_forced_reduce_enabled": False,
    "pause_buy_position_notional": 1800.0,
    "pause_short_position_notional": 1800.0,
    "volatility_entry_pause_enabled": True,
}


def _snapshot(
    *,
    symbol: str = "ARXUSDT",
    now: datetime = NOW,
    assessment: FlowBlockerAssessment | None = None,
    **kwargs: object,
) -> SymbolSnapshot:
    return SymbolSnapshot(
        symbol=symbol,
        captured_at=now,
        assessment=assessment or FlowBlockerAssessment(),
        **kwargs,
    )


def _managed_manifest(
    state: RecoveryState,
    *order_ids: str,
) -> ManagedOrderManifest:
    assert state.decision_id is not None
    assert state.issued_at is not None
    assert state.side is not None
    assert state.order_role is not None
    lease = state.action_lease
    if state.active_action is ActionId.TEMPORARY_LOSS_RELIEF:
        assert lease is not None
        prefix = temporary_loss_client_order_prefix_for_binding(
            symbol=state.symbol,
            source_decision_id=state.decision_id,
            action_lease_epoch=lease.epoch,
            side=state.side,
            hard_expires_at=lease.hard_expires_at,
        )
        client_order_ids = tuple(
            f"{prefix}{index:06x}" for index, _ in enumerate(order_ids, start=1)
        )
    else:
        prefix = ordinary_recovery_client_order_prefix(
            symbol=state.symbol,
            generation=state.generation,
            decision_id=state.decision_id,
            profile_digest=state.desired_profile.digest,
            action_id=state.active_action.value,
            side=state.side.value,
            order_role=state.order_role.value,
        )
        client_order_ids = tuple(
            build_managed_order_client_order_id(
                prefix=prefix,
                order_index=index,
                nonce_ns=index,
            )
            for index, _ in enumerate(order_ids, start=1)
        )
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
                for order_id, client_order_id in zip(
                    order_ids,
                    client_order_ids,
                    strict=True,
                )
            )
        ),
        action_lease_epoch=(lease.epoch if lease is not None else None),
        hard_expires_at=(lease.hard_expires_at if lease is not None else None),
    )


def _cleanup_proof(
    obligation,
    *,
    observed_at: datetime,
    user_trades_watermark: int,
) -> CleanupProof:
    return CleanupProof(
        source_decision_id=obligation.source_decision_id,
        source_generation=obligation.managed_order_manifest.generation,
        manifest_digest=obligation.managed_order_manifest.digest,
        remaining_order_ids=(),
        user_trades_watermark=user_trades_watermark,
        observed_at=observed_at,
    )


def _complete_cleanup(
    engine: FuturesRecoveryDecisionEngine,
    cleaning,
    *,
    now: datetime,
    round_id: str,
    assessment: FlowBlockerAssessment | None = None,
):
    assert cleaning.next_state.phase is RecoveryPhase.CLEANING
    obligation = cleaning.next_state.cleanup_obligation
    assert obligation is not None
    return engine.plan_round(
        snapshot=_snapshot(
            now=now,
            assessment=assessment,
            cleanup_proof=_cleanup_proof(
                obligation,
                observed_at=now,
                user_trades_watermark=0,
            ),
        ),
        state=cleaning.next_state,
        now=now,
        round_id=round_id,
    )


def _activate_temporary_loss(
    engine: FuturesRecoveryDecisionEngine,
    entered,
    assessment: FlowBlockerAssessment,
    *,
    now: datetime,
    round_id: str,
):
    plan = entered
    state = plan.next_state
    lease = state.action_lease
    assert lease is not None
    activated = engine.plan_round(
        snapshot=_snapshot(
            now=now,
            assessment=assessment,
            activation_receipt=ActivationReceipt(
                decision_id=state.decision_id,
                generation=state.generation,
                profile_digest=state.desired_profile.digest,
                action_lease_epoch=lease.epoch,
                observed_at=now,
            ),
        ),
        state=state,
        now=now,
        round_id=round_id,
    )
    assert activated.effect_stage is EffectStage.RUNNER_RESTART
    assert activated.effect_epoch is not None
    acknowledged_at = now + timedelta(microseconds=1)
    return engine.plan_round(
        snapshot=_snapshot(
            now=acknowledged_at,
            assessment=assessment,
            effect_receipt=EffectReceipt(
                decision_id=activated.next_state.decision_id,
                stage=EffectStage.RUNNER_RESTART,
                effect_epoch=activated.effect_epoch,
                observed_at=acknowledged_at,
            ),
        ),
        state=activated.next_state,
        now=acknowledged_at,
        round_id=f"{round_id}-restart-ack",
    )


def test_action_registry_is_a_closed_unique_total_order() -> None:
    assert ACTION_PRIORITY == (
        ActionId.TERMINAL_HANDOFF,
        ActionId.TERMINAL_STOP,
        ActionId.SAFETY_CONVERGE,
        ActionId.RUNNER_RECOVER,
        ActionId.FROZEN_LEDGER_REPAIR,
        ActionId.INVENTORY_RECOVER,
        ActionId.TEMPORARY_LOSS_RELIEF,
        ActionId.MAKER_FLOW_RECOVER,
        ActionId.BASELINE_REBASE,
        ActionId.BASELINE_TUNE,
        ActionId.NOOP,
    )
    assert len(ACTION_PRIORITY) == len(set(ACTION_PRIORITY)) == len(ActionId)
    assert tuple(item.action_id for item in ACTION_DEFINITIONS) == ACTION_PRIORITY
    assert all(callable(item.evaluate) for item in ACTION_DEFINITIONS)


def test_unregistered_terminal_reason_cannot_create_a_terminal_latch() -> None:
    engine = FuturesRecoveryDecisionEngine()
    state = RecoveryState.initial("ARXUSDT", BASELINE, now=NOW)

    rejected = engine.plan_round(
        snapshot=_snapshot(
            assessment=FlowBlockerAssessment(
                terminal_reason="low_volume_timeout",
                missing_entry_sides=(Side.BUY,),
            )
        ),
        state=state,
        now=NOW,
        round_id="reject-untrusted-terminal",
    )

    assert rejected.action_id is ActionId.MAKER_FLOW_RECOVER
    assert rejected.desired_runner_state == "running"
    assert rejected.next_state.phase is RecoveryPhase.ACTIVE


def test_registered_terminal_reason_without_clean_flat_proof_cannot_stop_runner() -> None:
    engine = FuturesRecoveryDecisionEngine()
    state = RecoveryState.initial("ARXUSDT", BASELINE, now=NOW)

    blocked = engine.plan_round(
        snapshot=_snapshot(
            assessment=FlowBlockerAssessment(
                terminal_reason="trusted_competition_window_end",
            )
        ),
        state=state,
        now=NOW,
        round_id="terminal-without-flat-proof",
    )

    assert blocked.action_id is ActionId.NOOP
    assert blocked.desired_runner_state == "running"
    assert blocked.next_state.phase is RecoveryPhase.STABLE
    assert "terminal_clean_flat_proof_required" in blocked.reasons


def test_one_symbol_selects_one_action_commits_once_and_executes_one_effect() -> None:
    store = InMemoryRecoveryStore()
    store.register_symbol("ARXUSDT", BASELINE, now=NOW)
    effects: list[tuple[str, EffectCommand]] = []

    def snapshot_provider(
        symbol: str, now: datetime, state: RecoveryState
    ) -> SymbolSnapshot:
        return _snapshot(
            symbol=symbol,
            now=now,
            assessment=FlowBlockerAssessment(
                runner_faults=("runner_inactive", "quiet_exchange_order_drift"),
                inventory_reduce_sides=(Side.SELL,),
                missing_entry_sides=(Side.BUY,),
            ),
        )

    coordinator = FuturesRecoveryCoordinator(
        store=store,
        snapshot_provider=snapshot_provider,
        effect_executor=lambda symbol, command: effects.append((symbol, command)),
    )

    outcome = coordinator.reconcile_symbol("ARXUSDT", now=NOW, round_id="round-arx-1")

    assert outcome.action_id is ActionId.RUNNER_RECOVER
    assert outcome.mode is ActionMode.ENTER
    assert outcome.effect_stage is EffectStage.RUNNER_RESTART
    assert outcome.suppressed_actions == (
        ActionId.INVENTORY_RECOVER,
        ActionId.MAKER_FLOW_RECOVER,
    )
    assert store.commit_count("ARXUSDT") == 1
    assert len(effects) == 1
    assert effects[0][0] == "ARXUSDT"
    assert effects[0][1].stage is EffectStage.RUNNER_RESTART
    assert effects[0][1].effect_epoch == outcome.effect_epoch


def test_same_round_is_not_committed_or_executed_again_after_coordinator_restart() -> (
    None
):
    store = InMemoryRecoveryStore()
    store.register_symbol("ARXUSDT", BASELINE, now=NOW)
    effects: list[EffectCommand] = []

    def snapshot_provider(
        symbol: str, now: datetime, state: RecoveryState
    ) -> SymbolSnapshot:
        return _snapshot(
            symbol=symbol,
            now=now,
            assessment=FlowBlockerAssessment(runner_faults=("runner_inactive",)),
        )

    first_coordinator = FuturesRecoveryCoordinator(
        store=store,
        snapshot_provider=snapshot_provider,
        effect_executor=lambda _symbol, command: effects.append(command),
    )
    first = first_coordinator.reconcile_symbol(
        "ARXUSDT", now=NOW, round_id="durable-round-1"
    )
    restarted_coordinator = FuturesRecoveryCoordinator(
        store=store,
        snapshot_provider=snapshot_provider,
        effect_executor=lambda _symbol, command: effects.append(command),
    )
    replay = restarted_coordinator.reconcile_symbol(
        "ARXUSDT",
        now=NOW + timedelta(milliseconds=100),
        round_id="durable-round-1",
    )

    assert first.effect_stage is EffectStage.RUNNER_RESTART
    assert replay.effect_stage is EffectStage.NONE
    assert replay.next_state.document_revision == first.next_state.document_revision
    assert store.commit_count("ARXUSDT") == 1
    assert len(effects) == 1


def test_late_round_replay_is_blocked_by_bounded_durable_history() -> None:
    store = InMemoryRecoveryStore()
    store.register_symbol("ARXUSDT", BASELINE, now=NOW)
    effects: list[EffectCommand] = []

    def snapshot_provider(
        symbol: str, now: datetime, state: RecoveryState
    ) -> SymbolSnapshot:
        return _snapshot(
            symbol=symbol,
            now=now,
            assessment=FlowBlockerAssessment(runner_faults=("runner_inactive",)),
        )

    coordinator = FuturesRecoveryCoordinator(
        store=store,
        snapshot_provider=snapshot_provider,
        effect_executor=lambda _symbol, command: effects.append(command),
    )
    coordinator.reconcile_symbol("ARXUSDT", now=NOW, round_id="late-r1")
    coordinator.reconcile_symbol(
        "ARXUSDT",
        now=NOW + timedelta(seconds=1),
        round_id="late-r2",
    )
    revision_after_r2 = store.read("ARXUSDT").document_revision
    restarted = FuturesRecoveryCoordinator(
        store=store,
        snapshot_provider=snapshot_provider,
        effect_executor=lambda _symbol, command: effects.append(command),
    )

    replay = restarted.reconcile_symbol(
        "ARXUSDT",
        now=NOW + timedelta(seconds=2),
        round_id="late-r1",
    )

    assert replay.effect_stage is EffectStage.NONE
    assert store.read("ARXUSDT").document_revision == revision_after_r2
    assert store.commit_count("ARXUSDT") == 2
    assert len(effects) == 2


def test_each_symbol_has_an_independent_single_action_round() -> None:
    store = InMemoryRecoveryStore()
    store.register_symbol("ARXUSDT", BASELINE, now=NOW)
    store.register_symbol("OUSDT", BASELINE, now=NOW)

    def snapshot_provider(
        symbol: str, now: datetime, state: RecoveryState
    ) -> SymbolSnapshot:
        assessment = (
            FlowBlockerAssessment(runner_faults=("runner_inactive",))
            if symbol == "ARXUSDT"
            else FlowBlockerAssessment(inventory_reduce_sides=(Side.BUY,))
        )
        return _snapshot(symbol=symbol, now=now, assessment=assessment)

    coordinator = FuturesRecoveryCoordinator(
        store=store,
        snapshot_provider=snapshot_provider,
    )

    arx = coordinator.reconcile_symbol("ARXUSDT", now=NOW, round_id="arx-1")
    ousdt = coordinator.reconcile_symbol("OUSDT", now=NOW, round_id="o-1")

    assert arx.action_id is ActionId.RUNNER_RECOVER
    assert ousdt.action_id is ActionId.INVENTORY_RECOVER
    assert ousdt.side is Side.BUY
    assert store.commit_count("ARXUSDT") == 1
    assert store.commit_count("OUSDT") == 1


def test_higher_priority_safety_preempts_an_active_maker_action() -> None:
    engine = FuturesRecoveryDecisionEngine()
    state = RecoveryState.initial("ARXUSDT", BASELINE, now=NOW)
    maker = engine.plan_round(
        snapshot=_snapshot(
            assessment=FlowBlockerAssessment(missing_entry_sides=(Side.BUY,))
        ),
        state=state,
        now=NOW,
        round_id="maker-enter",
    )
    assert maker.action_id is ActionId.MAKER_FLOW_RECOVER

    safety = engine.plan_round(
        snapshot=_snapshot(
            now=NOW + timedelta(seconds=1),
            assessment=FlowBlockerAssessment(
                safety_reasons=("volatility",),
                safety_evidence_fingerprint="volatility-seq-2",
                safety_observation_seq=2,
                safety_observed_at=NOW + timedelta(seconds=1),
                runner_faults=("runner_inactive",),
                missing_entry_sides=(Side.BUY,),
            ),
        ),
        state=maker.next_state,
        now=NOW + timedelta(seconds=1),
        round_id="safety-preempt",
    )

    assert safety.action_id is ActionId.SAFETY_CONVERGE
    assert safety.mode is ActionMode.ENTER
    assert safety.next_state.active_action is ActionId.SAFETY_CONVERGE
    assert safety.suppressed_actions == (
        ActionId.RUNNER_RECOVER,
        ActionId.MAKER_FLOW_RECOVER,
    )
    assert safety.desired_profile.fields["volatility_entry_pause_enabled"] is True


def test_stale_progress_receipt_cannot_complete_a_new_action() -> None:
    engine = FuturesRecoveryDecisionEngine()
    assessment = FlowBlockerAssessment(missing_entry_sides=(Side.BUY,))
    entered = engine.plan_round(
        snapshot=_snapshot(assessment=assessment),
        state=RecoveryState.initial("ARXUSDT", BASELINE, now=NOW),
        now=NOW,
        round_id="progress-enter",
    )

    stale = engine.plan_round(
        snapshot=_snapshot(
            now=NOW + timedelta(seconds=1),
            assessment=assessment,
            progress_receipts=(
                ProgressReceipt(
                    decision_id="previous-decision",
                    generation=entered.next_state.generation,
                    side=Side.BUY,
                    order_role=OrderRole.ENTRY,
                    observation_seq=10,
                    observed_at=NOW + timedelta(seconds=1),
                ),
            ),
        ),
        state=entered.next_state,
        now=NOW + timedelta(seconds=1),
        round_id="progress-stale",
    )
    assert stale.mode is ActionMode.ADVANCE
    assert stale.effect_stage is EffectStage.RUNNER_RESTART
    assert stale.next_state.phase is RecoveryPhase.ACTIVE

    current = engine.plan_round(
        snapshot=_snapshot(
            now=NOW + timedelta(seconds=2),
            assessment=assessment,
            progress_receipts=(
                ProgressReceipt(
                    decision_id=stale.next_state.decision_id,
                    generation=stale.next_state.generation,
                    side=Side.BUY,
                    order_role=OrderRole.ENTRY,
                    observation_seq=11,
                    observed_at=NOW + timedelta(seconds=2),
                ),
            ),
        ),
        state=stale.next_state,
        now=NOW + timedelta(seconds=2),
        round_id="progress-current",
    )
    assert current.mode is ActionMode.EXIT
    assert current.next_state.phase is RecoveryPhase.CLEANING
    restored = _complete_cleanup(
        engine,
        current,
        now=NOW + timedelta(seconds=2),
        round_id="progress-current-cleaned",
        assessment=assessment,
    )
    assert restored.next_state.phase is RecoveryPhase.RESTORING


def test_normal_entry_fill_clocks_trigger_one_side_scoped_maker_recovery() -> None:
    engine = FuturesRecoveryDecisionEngine(
        policy=RecoveryPolicy(action_progress_timeout=timedelta(seconds=120))
    )
    state = RecoveryState.initial("ARXUSDT", BASELINE, now=NOW)
    buy_key = FlowRoleKey(LedgerClass.NORMAL_BQ, Side.BUY, OrderRole.ENTRY)
    sell_key = FlowRoleKey(LedgerClass.NORMAL_BQ, Side.SELL, OrderRole.ENTRY)
    no_fill = {
        buy_key: FlowObservation(
            obligation_active=True,
            qualified_quote_present=True,
            submitted_gtx_present=True,
        ),
        sell_key: FlowObservation(
            obligation_active=True,
            qualified_quote_present=True,
            submitted_gtx_present=True,
        ),
    }

    armed = engine.plan_round(
        snapshot=_snapshot(flow_observations=no_fill),
        state=state,
        now=NOW,
        round_id="flow-clock-arm",
    )

    assert armed.action_id is ActionId.NOOP
    assert armed.next_state.flow_clocks[buy_key].fill_missing_since == NOW
    assert armed.next_state.flow_clocks[sell_key].fill_missing_since == NOW

    one_side_filled = {
        buy_key: FlowObservation(
            obligation_active=True,
            qualified_quote_present=True,
            submitted_gtx_present=True,
            latest_fill_at=NOW + timedelta(seconds=121),
        ),
        sell_key: no_fill[sell_key],
    }
    recovered = engine.plan_round(
        snapshot=_snapshot(
            now=NOW + timedelta(seconds=121),
            flow_observations=one_side_filled,
        ),
        state=armed.next_state,
        now=NOW + timedelta(seconds=121),
        round_id="flow-clock-one-side-expired",
    )

    assert recovered.action_id is ActionId.MAKER_FLOW_RECOVER
    assert recovered.side is Side.SELL
    assert recovered.next_state.flow_clocks[buy_key].fill_missing_since is None
    assert recovered.next_state.flow_clocks[sell_key].fill_missing_since == NOW
    assert recovered.suppressed_actions == ()


def test_inactive_flow_obligation_resets_clock_before_reactivation() -> None:
    engine = FuturesRecoveryDecisionEngine(
        policy=RecoveryPolicy(action_progress_timeout=timedelta(seconds=120))
    )
    state = RecoveryState.initial("ARXUSDT", BASELINE, now=NOW)
    key = FlowRoleKey(LedgerClass.NORMAL_BQ, Side.BUY, OrderRole.ENTRY)
    active = FlowObservation(
        obligation_active=True,
        qualified_quote_present=True,
        submitted_gtx_present=True,
    )

    armed = engine.plan_round(
        snapshot=_snapshot(flow_observations={key: active}),
        state=state,
        now=NOW,
        round_id="flow-clock-before-pause",
    )
    paused = engine.plan_round(
        snapshot=_snapshot(
            now=NOW + timedelta(seconds=119),
            flow_observations={
                key: FlowObservation(
                    obligation_active=False,
                    qualified_quote_present=False,
                    submitted_gtx_present=False,
                )
            },
        ),
        state=armed.next_state,
        now=NOW + timedelta(seconds=119),
        round_id="flow-clock-paused",
    )

    cleared = paused.next_state.flow_clocks[key]
    assert cleared.quote_missing_since is None
    assert cleared.submit_missing_since is None
    assert cleared.fill_missing_since is None

    resumed_at = NOW + timedelta(seconds=240)
    resumed = engine.plan_round(
        snapshot=_snapshot(
            now=resumed_at,
            flow_observations={key: active},
        ),
        state=paused.next_state,
        now=resumed_at,
        round_id="flow-clock-resumed",
    )

    assert resumed.action_id is ActionId.NOOP
    assert resumed.next_state.flow_clocks[key].fill_missing_since == resumed_at


def test_first_flow_observation_does_not_backfill_a_stale_fill_timestamp() -> None:
    engine = FuturesRecoveryDecisionEngine(
        policy=RecoveryPolicy(action_progress_timeout=timedelta(seconds=120))
    )
    state = RecoveryState.initial("ARXUSDT", BASELINE, now=NOW)
    key = FlowRoleKey(LedgerClass.NORMAL_BQ, Side.SELL, OrderRole.ENTRY)

    armed = engine.plan_round(
        snapshot=_snapshot(
            flow_observations={
                key: FlowObservation(
                    obligation_active=True,
                    qualified_quote_present=True,
                    submitted_gtx_present=True,
                    latest_fill_at=NOW - timedelta(hours=1),
                )
            }
        ),
        state=state,
        now=NOW,
        round_id="flow-clock-stale-first-fill",
    )

    assert armed.action_id is ActionId.NOOP
    assert armed.next_state.flow_clocks[key].fill_missing_since == NOW


def test_flow_clock_future_timestamp_is_rebased_after_clock_rollback() -> None:
    engine = FuturesRecoveryDecisionEngine(
        policy=RecoveryPolicy(action_progress_timeout=timedelta(seconds=120))
    )
    key = FlowRoleKey(LedgerClass.NORMAL_BQ, Side.BUY, OrderRole.ENTRY)
    state = RecoveryState.initial("ARXUSDT", BASELINE, now=NOW).with_flow_clock(
        key,
        quote_missing_since=NOW + timedelta(hours=1),
        submit_missing_since=NOW + timedelta(hours=1),
        fill_missing_since=NOW + timedelta(hours=1),
    )

    rebased = engine.plan_round(
        snapshot=_snapshot(
            flow_observations={
                key: FlowObservation(
                    obligation_active=True,
                    qualified_quote_present=False,
                    submitted_gtx_present=False,
                )
            }
        ),
        state=state,
        now=NOW,
        round_id="flow-clock-rollback",
    )

    clock = rebased.next_state.flow_clocks[key]
    assert rebased.action_id is ActionId.NOOP
    assert clock.quote_missing_since == NOW
    assert clock.submit_missing_since == NOW
    assert clock.fill_missing_since == NOW


def test_runner_recovery_clears_entry_flow_debt_before_fresh_restore() -> None:
    engine = FuturesRecoveryDecisionEngine(
        policy=RecoveryPolicy(
            action_progress_timeout=timedelta(seconds=120),
            cooldown=timedelta(seconds=5),
        )
    )
    buy_key = FlowRoleKey(LedgerClass.NORMAL_BQ, Side.BUY, OrderRole.ENTRY)
    sell_key = FlowRoleKey(LedgerClass.NORMAL_BQ, Side.SELL, OrderRole.ENTRY)
    old = NOW - timedelta(minutes=10)
    state = RecoveryState.initial("ARXUSDT", BASELINE, now=NOW)
    state = state.with_flow_clock(
        buy_key,
        quote_missing_since=None,
        submit_missing_since=None,
        fill_missing_since=old,
    )
    state = state.with_flow_clock(
        sell_key,
        quote_missing_since=None,
        submit_missing_since=None,
        fill_missing_since=old,
    )

    entered = engine.plan_round(
        snapshot=_snapshot(
            assessment=FlowBlockerAssessment(
                runner_faults=("recovery_admission_observation_unavailable",)
            )
        ),
        state=state,
        now=NOW,
        round_id="outage-runner-enter",
    )
    assert entered.action_id is ActionId.RUNNER_RECOVER
    assert entered.next_state.flow_clocks[buy_key].fill_missing_since is None
    assert entered.next_state.flow_clocks[sell_key].fill_missing_since is None

    exited = engine.plan_round(
        snapshot=_snapshot(
            now=NOW + timedelta(seconds=1),
            effect_receipt=EffectReceipt(
                decision_id=str(entered.next_state.decision_id),
                stage=EffectStage.RUNNER_RESTART,
                effect_epoch=int(entered.effect_epoch),
                observed_at=NOW + timedelta(seconds=1),
            ),
        ),
        state=entered.next_state,
        now=NOW + timedelta(seconds=1),
        round_id="outage-runner-exit",
    )
    assert exited.next_state.phase is RecoveryPhase.COOLDOWN
    assert exited.next_state.cooldown_until is not None
    stable = engine.plan_round(
        snapshot=_snapshot(now=exited.next_state.cooldown_until),
        state=exited.next_state,
        now=exited.next_state.cooldown_until,
        round_id="outage-runner-stable",
    )
    resumed_at = exited.next_state.cooldown_until + timedelta(seconds=1)
    resumed = engine.plan_round(
        snapshot=_snapshot(
            now=resumed_at,
            flow_observations={
                buy_key: FlowObservation(
                    obligation_active=True,
                    qualified_quote_present=True,
                    submitted_gtx_present=True,
                ),
                sell_key: FlowObservation(
                    obligation_active=True,
                    qualified_quote_present=True,
                    submitted_gtx_present=True,
                ),
            },
        ),
        state=stable.next_state,
        now=resumed_at,
        round_id="outage-flow-fresh",
    )

    assert resumed.action_id is ActionId.NOOP
    assert resumed.next_state.flow_clocks[buy_key].fill_missing_since == resumed_at
    assert resumed.next_state.flow_clocks[sell_key].fill_missing_since == resumed_at


def test_failed_effect_is_retried_under_the_same_decision_next_round() -> None:
    store = InMemoryRecoveryStore()
    store.register_symbol("ARXUSDT", BASELINE, now=NOW)
    calls: list[tuple[str, EffectCommand]] = []

    def snapshot_provider(
        symbol: str, now: datetime, state: RecoveryState
    ) -> SymbolSnapshot:
        return _snapshot(
            symbol=symbol,
            now=now,
            assessment=FlowBlockerAssessment(runner_faults=("runner_inactive",)),
        )

    def flaky_effect(symbol: str, command: EffectCommand) -> None:
        calls.append((symbol, command))
        if len(calls) == 1:
            raise RuntimeError("restart transport failed")

    coordinator = FuturesRecoveryCoordinator(
        store=store,
        snapshot_provider=snapshot_provider,
        effect_executor=flaky_effect,
    )

    first = coordinator.reconcile_symbol("ARXUSDT", now=NOW, round_id="runner-effect-1")
    first_decision_id = store.read("ARXUSDT").decision_id
    second = coordinator.reconcile_symbol(
        "ARXUSDT",
        now=NOW + timedelta(seconds=1),
        round_id="runner-effect-2",
    )

    assert first.effect_error == "restart transport failed"
    assert second.effect_error is None
    assert second.action_id is ActionId.RUNNER_RECOVER
    assert second.mode is ActionMode.ADVANCE
    assert store.read("ARXUSDT").decision_id == first_decision_id
    assert store.commit_count("ARXUSDT") == 2
    assert len(calls) == 2
    assert calls[0][1].decision_id == calls[1][1].decision_id == first_decision_id
    assert calls[0][1].stage is calls[1][1].stage is EffectStage.RUNNER_RESTART
    assert calls[0][1].effect_epoch == calls[1][1].effect_epoch


def test_missing_effect_receipt_cannot_bypass_absolute_action_deadline() -> None:
    policy = RecoveryPolicy(
        action_progress_timeout=timedelta(seconds=5),
        action_hard_ttl=timedelta(seconds=10),
    )
    engine = FuturesRecoveryDecisionEngine(policy=policy)
    assessment = FlowBlockerAssessment(runner_faults=("runner_inactive",))
    entered = engine.plan_round(
        snapshot=_snapshot(assessment=assessment),
        state=RecoveryState.initial("ARXUSDT", BASELINE, now=NOW),
        now=NOW,
        round_id="effect-deadline-enter",
    )

    timed_out = engine.plan_round(
        snapshot=_snapshot(
            now=NOW + timedelta(seconds=5),
            assessment=assessment,
        ),
        state=entered.next_state,
        now=NOW + timedelta(seconds=5),
        round_id="effect-deadline-expired",
    )

    assert timed_out.mode is ActionMode.EXIT
    assert timed_out.next_state.phase is RecoveryPhase.RESTORING
    assert timed_out.effect_stage is EffectStage.RUNNER_RESTART
    assert timed_out.next_state.pending_effect_stage is EffectStage.RUNNER_RESTART
    assert timed_out.next_state.pending_effect_epoch == timed_out.effect_epoch
    assert timed_out.next_state.desired_runner_state == "running"


def test_fresh_safety_condition_cannot_restore_baseline_when_effect_receipt_times_out() -> (
    None
):
    policy = RecoveryPolicy(
        action_progress_timeout=timedelta(seconds=5),
        action_hard_ttl=timedelta(seconds=10),
        safety_hard_ttl=timedelta(seconds=30),
    )
    engine = FuturesRecoveryDecisionEngine(policy=policy)
    assessment = FlowBlockerAssessment(
        safety_reasons=("volatility_entry_pause",),
        safety_evidence_fingerprint="safety-effect-deadline",
        safety_observation_seq=1,
        safety_observed_at=NOW,
    )
    entered = engine.plan_round(
        snapshot=_snapshot(assessment=assessment),
        state=RecoveryState.initial("ARXUSDT", BASELINE, now=NOW),
        now=NOW,
        round_id="safety-effect-deadline-enter",
    )
    decision_id = entered.next_state.decision_id
    profile_digest = entered.next_state.desired_profile.digest
    effect_epoch = entered.effect_epoch

    timed_out = engine.plan_round(
        snapshot=_snapshot(
            now=NOW + timedelta(seconds=5),
            assessment=assessment,
        ),
        state=entered.next_state,
        now=NOW + timedelta(seconds=5),
        round_id="safety-effect-deadline-expired",
    )

    assert timed_out.action_id is ActionId.SAFETY_CONVERGE
    assert timed_out.mode is ActionMode.ADVANCE
    assert timed_out.next_state.phase is RecoveryPhase.ACTIVE
    assert timed_out.next_state.decision_id == decision_id
    assert timed_out.next_state.desired_profile.digest == profile_digest
    assert timed_out.next_state.desired_profile.digest != (
        timed_out.next_state.baseline_profile.digest
    )
    assert timed_out.effect_stage is EffectStage.RUNNER_RESTART
    assert timed_out.effect_epoch == effect_epoch
    assert timed_out.next_state.pending_effect_epoch == effect_epoch
    assert timed_out.liveness_status == "blocked"


def test_confirmed_runner_recovery_clears_directly_without_second_restart() -> None:
    engine = FuturesRecoveryDecisionEngine(
        policy=RecoveryPolicy(cooldown=timedelta(seconds=5))
    )
    entered = engine.plan_round(
        snapshot=_snapshot(
            assessment=FlowBlockerAssessment(runner_faults=("runner_inactive",))
        ),
        state=RecoveryState.initial("ARXUSDT", BASELINE, now=NOW),
        now=NOW,
        round_id="runner-direct-enter",
    )
    generation = entered.next_state.generation

    cleared = engine.plan_round(
        snapshot=_snapshot(
            now=NOW + timedelta(seconds=1),
            assessment=FlowBlockerAssessment(),
            effect_receipt=EffectReceipt(
                decision_id=entered.next_state.decision_id,
                stage=EffectStage.RUNNER_RESTART,
                effect_epoch=entered.effect_epoch,
                observed_at=NOW + timedelta(seconds=1),
            ),
        ),
        state=entered.next_state,
        now=NOW + timedelta(seconds=1),
        round_id="runner-direct-clear",
    )

    assert cleared.next_state.phase is RecoveryPhase.COOLDOWN
    assert cleared.next_state.generation == generation
    assert cleared.effect_stage is EffectStage.NONE
    assert cleared.effect_epoch is None
    assert (
        cleared.next_state.desired_profile.digest
        == cleared.next_state.baseline_profile.digest
    )


def test_restore_confirmation_timeout_becomes_one_explicit_runner_action() -> None:
    policy = RecoveryPolicy(
        action_progress_timeout=timedelta(seconds=5),
        action_hard_ttl=timedelta(seconds=10),
        restore_timeout=timedelta(seconds=3),
    )
    engine = FuturesRecoveryDecisionEngine(policy=policy)
    maker = engine.plan_round(
        snapshot=_snapshot(
            assessment=FlowBlockerAssessment(missing_entry_sides=(Side.BUY,))
        ),
        state=RecoveryState.initial("ARXUSDT", BASELINE, now=NOW),
        now=NOW,
        round_id="restore-maker-enter",
    )
    cleaning = engine.plan_round(
        snapshot=_snapshot(
            now=NOW + timedelta(seconds=1),
            assessment=FlowBlockerAssessment(),
            effect_receipt=EffectReceipt(
                decision_id=maker.next_state.decision_id,
                stage=EffectStage.RUNNER_RESTART,
                effect_epoch=maker.effect_epoch,
                observed_at=NOW + timedelta(seconds=1),
            ),
        ),
        state=maker.next_state,
        now=NOW + timedelta(seconds=1),
        round_id="restore-maker-exit",
    )
    assert cleaning.next_state.phase is RecoveryPhase.CLEANING
    restoring = _complete_cleanup(
        engine,
        cleaning,
        now=NOW + timedelta(seconds=1),
        round_id="restore-maker-cleaned",
    )
    assert restoring.next_state.phase is RecoveryPhase.RESTORING

    escalated = engine.plan_round(
        snapshot=_snapshot(now=NOW + timedelta(seconds=4)),
        state=restoring.next_state,
        now=NOW + timedelta(seconds=4),
        round_id="restore-timeout-runner",
    )

    assert escalated.action_id is ActionId.RUNNER_RECOVER
    assert escalated.mode is ActionMode.ENTER
    assert escalated.next_state.phase is RecoveryPhase.ACTIVE
    assert escalated.effect_stage is EffectStage.RUNNER_RESTART
    assert escalated.reasons == ("restore_confirmation_timeout",)


def test_materializer_rebuilds_once_and_protected_fields_win() -> None:
    profile = materialize_profile(
        baseline=BASELINE,
        action_id=ActionId.MAKER_FLOW_RECOVER,
        action_updates={
            "best_quote_maker_volume_allow_loss_reduce_only": True,
            "best_quote_maker_volume_cycle_budget_notional": 720.0,
            "best_quote_maker_volume_directional_net_guard": "net_long",
            "best_quote_maker_volume_net_loss_reduce_enabled": True,
            "hard_loss_forced_reduce_enabled": True,
            "volatility_entry_pause_enabled": False,
        },
    )

    assert profile.fields["best_quote_maker_volume_cycle_budget_notional"] == 720.0
    assert profile.fields["best_quote_maker_volume_allow_loss_reduce_only"] is False
    assert profile.fields["best_quote_maker_volume_net_loss_reduce_enabled"] is False
    assert profile.fields["hard_loss_forced_reduce_enabled"] is False
    assert profile.fields["volatility_entry_pause_enabled"] is True
    assert profile.execution_policy.order_type == "LIMIT"
    assert profile.execution_policy.time_in_force == "GTX"
    assert profile.execution_policy.post_only is True
    with pytest.raises(TypeError):
        profile.fields["volatility_entry_pause_enabled"] = False  # type: ignore[index]


def test_materialized_profile_is_deeply_immutable_and_detached_from_inputs() -> None:
    baseline = dict(BASELINE)
    baseline["recovery_nested_policy"] = {
        "allowed_sides": ["BUY"],
        "limits": {"max_orders": 2},
    }
    profile = materialize_profile(
        baseline=baseline,
        action_id=ActionId.NOOP,
    )
    baseline["recovery_nested_policy"]["allowed_sides"].append("SELL")  # type: ignore[index,union-attr]

    nested = profile.fields["recovery_nested_policy"]
    assert nested["allowed_sides"] == ("BUY",)
    assert nested["limits"]["max_orders"] == 2
    with pytest.raises(TypeError):
        nested["limits"]["max_orders"] = 3


def test_restore_never_reopens_historical_allow_loss() -> None:
    updates = _restore_recovery_controls(
        {
            "guard_original_controls": {
                "best_quote_maker_volume_allow_loss_reduce_only": True,
                "best_quote_maker_volume_cycle_budget_notional": 48.0,
            }
        },
        control=BASELINE,
        cycle_budget_floor_notional=60.0,
    )

    assert updates["best_quote_maker_volume_allow_loss_reduce_only"] is False
    assert updates["best_quote_maker_volume_net_loss_reduce_enabled"] is False
    assert updates["best_quote_maker_volume_cycle_budget_notional"] == 60.0


@pytest.mark.parametrize(
    ("assessment", "exhausted", "expected_action", "expected_side", "expected_role"),
    (
        (
            FlowBlockerAssessment(runner_faults=("generation_mismatch",)),
            (),
            ActionId.RUNNER_RECOVER,
            None,
            None,
        ),
        (
            FlowBlockerAssessment(inventory_reduce_sides=(Side.SELL,)),
            (),
            ActionId.INVENTORY_RECOVER,
            Side.SELL,
            OrderRole.REDUCE_ONLY,
        ),
        (
            FlowBlockerAssessment(
                inventory_reduce_sides=(Side.SELL,),
                loss_only_blocked_sides=(Side.SELL,),
            ),
            ((ActionId.INVENTORY_RECOVER, Side.SELL, OrderRole.REDUCE_ONLY),),
            ActionId.TEMPORARY_LOSS_RELIEF,
            Side.SELL,
            OrderRole.REDUCE_ONLY,
        ),
        (
            FlowBlockerAssessment(missing_entry_sides=(Side.BUY,)),
            (),
            ActionId.MAKER_FLOW_RECOVER,
            Side.BUY,
            OrderRole.ENTRY,
        ),
    ),
)
def test_blocker_assessment_routes_to_one_directional_action(
    assessment: FlowBlockerAssessment,
    exhausted: tuple[tuple[ActionId, Side, OrderRole], ...],
    expected_action: ActionId,
    expected_side: Side | None,
    expected_role: OrderRole | None,
) -> None:
    engine = FuturesRecoveryDecisionEngine()
    state = RecoveryState.initial("ARXUSDT", BASELINE, now=NOW)
    for action_id, side, role in exhausted:
        state = state.with_exhausted_attempt(action_id, side, role, now=NOW)

    plan = engine.plan_round(
        snapshot=_snapshot(assessment=assessment),
        state=state,
        now=NOW,
        round_id="route-1",
    )

    assert plan.action_id is expected_action
    assert plan.side is expected_side
    assert plan.order_role is expected_role
    assert plan.desired_profile.execution_policy.time_in_force == "GTX"
    assert plan.desired_profile.execution_policy.post_only is True
    assert (
        plan.desired_profile.fields["best_quote_maker_volume_allow_loss_reduce_only"]
        is False
    )
    if expected_action is ActionId.TEMPORARY_LOSS_RELIEF:
        assert plan.next_state.phase is RecoveryPhase.SETTLING
        assert plan.desired_profile.fields["recovery_action_lease_active"] is False


def test_persistent_bilateral_blocker_rotates_direction_across_recovery_rounds() -> (
    None
):
    engine = FuturesRecoveryDecisionEngine(policy=RecoveryPolicy(cooldown=timedelta(0)))
    assessment = FlowBlockerAssessment(
        missing_entry_sides=(Side.BUY, Side.SELL),
        episode_fingerprint="bilateral-maker-flow",
    )
    state = RecoveryState.initial("ARXUSDT", BASELINE, now=NOW)

    first = engine.plan_round(
        snapshot=_snapshot(assessment=assessment),
        state=state,
        now=NOW,
        round_id="bilateral-first",
    )
    assert first.action_id is ActionId.MAKER_FLOW_RECOVER
    assert first.side is Side.BUY
    assert first.effect_stage is EffectStage.RUNNER_RESTART
    assert (
        first.next_state.fairness_last_selected_sides["maker_flow_recover:entry"]
        is Side.BUY
    )

    progressed = engine.plan_round(
        snapshot=_snapshot(
            now=NOW + timedelta(seconds=1),
            assessment=assessment,
            progress_receipts=(
                ProgressReceipt(
                    decision_id=first.next_state.decision_id,
                    generation=first.next_state.generation,
                    side=Side.BUY,
                    order_role=OrderRole.ENTRY,
                    observation_seq=1,
                    observed_at=NOW + timedelta(seconds=1),
                ),
            ),
        ),
        state=first.next_state,
        now=NOW + timedelta(seconds=1),
        round_id="bilateral-first-progress",
    )
    cleaned = _complete_cleanup(
        engine,
        progressed,
        now=NOW + timedelta(seconds=1),
        round_id="bilateral-first-cleaned",
        assessment=assessment,
    )
    restored = engine.plan_round(
        snapshot=_snapshot(
            now=NOW + timedelta(seconds=2),
            assessment=assessment,
            applied_generation=cleaned.next_state.generation,
            applied_profile_digest=cleaned.next_state.desired_profile.digest,
        ),
        state=cleaned.next_state,
        now=NOW + timedelta(seconds=2),
        round_id="bilateral-first-restored",
    )
    stable = engine.plan_round(
        snapshot=_snapshot(
            now=NOW + timedelta(seconds=2),
            assessment=assessment,
        ),
        state=restored.next_state,
        now=NOW + timedelta(seconds=2),
        round_id="bilateral-first-stable",
    )
    second = engine.plan_round(
        snapshot=_snapshot(
            now=NOW + timedelta(seconds=3),
            assessment=assessment,
        ),
        state=stable.next_state,
        now=NOW + timedelta(seconds=3),
        round_id="bilateral-second",
    )

    assert second.action_id is ActionId.MAKER_FLOW_RECOVER
    assert second.side is Side.SELL
    assert second.effect_stage is EffectStage.RUNNER_RESTART
    assert (
        second.next_state.fairness_last_selected_sides["maker_flow_recover:entry"]
        is Side.SELL
    )
    for plan in (first, second):
        assert plan.desired_profile.execution_policy.time_in_force == "GTX"
        assert plan.desired_profile.execution_policy.post_only is True
        assert plan.desired_profile.fields["volatility_entry_pause_enabled"] is True
        assert (
            plan.desired_profile.fields[
                "best_quote_maker_volume_allow_loss_reduce_only"
            ]
            is False
        )


def test_exhausted_directional_tuple_yields_for_the_rest_of_the_episode() -> None:
    engine = FuturesRecoveryDecisionEngine()
    state = RecoveryState.initial("ARXUSDT", BASELINE, now=NOW)
    state = replace(
        state.with_exhausted_attempt(
            ActionId.MAKER_FLOW_RECOVER,
            Side.BUY,
            OrderRole.ENTRY,
            now=NOW,
        ),
        active_episode_fingerprint="bilateral-maker-flow",
    )

    plan = engine.plan_round(
        snapshot=_snapshot(
            assessment=FlowBlockerAssessment(
                missing_entry_sides=(Side.BUY, Side.SELL),
                episode_fingerprint="bilateral-maker-flow",
            )
        ),
        state=state,
        now=NOW,
        round_id="bilateral-exhausted-buy",
    )

    assert plan.action_id is ActionId.MAKER_FLOW_RECOVER
    assert plan.side is Side.SELL
    assert {
        (item.action_id, item.side, item.order_role)
        for item in plan.next_state.exhausted_attempts
    } == {
        (ActionId.MAKER_FLOW_RECOVER, Side.BUY, OrderRole.ENTRY),
    }


def test_bilateral_inventory_timeouts_preserve_each_exact_exhausted_tuple() -> None:
    policy = RecoveryPolicy(
        action_progress_timeout=timedelta(seconds=5),
        action_hard_ttl=timedelta(seconds=10),
    )
    engine = FuturesRecoveryDecisionEngine(policy=policy)
    assessment = FlowBlockerAssessment(
        inventory_reduce_sides=(Side.BUY, Side.SELL),
        loss_only_blocked_sides=(Side.BUY, Side.SELL),
        episode_fingerprint="bilateral-inventory-timeout",
    )
    state = replace(
        RecoveryState.initial("ARXUSDT", BASELINE, now=NOW).with_exhausted_attempt(
            ActionId.INVENTORY_RECOVER,
            Side.BUY,
            OrderRole.REDUCE_ONLY,
            now=NOW,
        ),
        active_episode_fingerprint="bilateral-inventory-timeout",
    )

    entered = engine.plan_round(
        snapshot=_snapshot(assessment=assessment),
        state=state,
        now=NOW,
        round_id="bilateral-inventory-sell-enter",
    )
    assert entered.action_id is ActionId.INVENTORY_RECOVER
    assert entered.side is Side.SELL

    effect_confirmed = engine.plan_round(
        snapshot=_snapshot(
            now=NOW + timedelta(seconds=1),
            assessment=assessment,
            effect_receipt=EffectReceipt(
                decision_id=entered.next_state.decision_id,
                stage=EffectStage.RUNNER_RESTART,
                effect_epoch=entered.effect_epoch,
                observed_at=NOW + timedelta(seconds=1),
            ),
        ),
        state=entered.next_state,
        now=NOW + timedelta(seconds=1),
        round_id="bilateral-inventory-sell-effect",
    )
    timed_out = engine.plan_round(
        snapshot=_snapshot(
            now=NOW + timedelta(seconds=5),
            assessment=assessment,
        ),
        state=effect_confirmed.next_state,
        now=NOW + timedelta(seconds=5),
        round_id="bilateral-inventory-sell-timeout",
    )

    assert {
        (item.action_id, item.side, item.order_role)
        for item in timed_out.next_state.exhausted_attempts
    } == {
        (ActionId.INVENTORY_RECOVER, Side.BUY, OrderRole.REDUCE_ONLY),
        (ActionId.INVENTORY_RECOVER, Side.SELL, OrderRole.REDUCE_ONLY),
    }


def test_bilateral_exhausted_inventory_advances_to_baseline_tune_not_retry() -> None:
    engine = FuturesRecoveryDecisionEngine()
    state = RecoveryState.initial("ARXUSDT", BASELINE, now=NOW)
    state = replace(
        state.with_exhausted_attempt(
            ActionId.INVENTORY_RECOVER,
            Side.BUY,
            OrderRole.REDUCE_ONLY,
            now=NOW,
        ).with_exhausted_attempt(
            ActionId.INVENTORY_RECOVER,
            Side.SELL,
            OrderRole.REDUCE_ONLY,
            now=NOW,
        ),
        active_episode_fingerprint="bilateral-inventory-exhausted",
    )

    successor = engine.plan_round(
        snapshot=_snapshot(
            assessment=FlowBlockerAssessment(
                inventory_reduce_sides=(Side.BUY, Side.SELL),
                episode_fingerprint="bilateral-inventory-exhausted",
            )
        ),
        state=state,
        now=NOW + timedelta(seconds=1),
        round_id="bilateral-inventory-baseline-successor",
    )

    assert successor.action_id is ActionId.BASELINE_TUNE
    assert successor.order_role is OrderRole.REDUCE_ONLY
    assert successor.effect_stage is EffectStage.RUNNER_RESTART
    assert {
        (item.action_id, item.side, item.order_role)
        for item in successor.next_state.exhausted_attempts
    } == {
        (ActionId.INVENTORY_RECOVER, Side.BUY, OrderRole.REDUCE_ONLY),
        (ActionId.INVENTORY_RECOVER, Side.SELL, OrderRole.REDUCE_ONLY),
    }


def test_exhausted_bilateral_baseline_successor_does_not_restart_inventory_cycle() -> (
    None
):
    engine = FuturesRecoveryDecisionEngine()
    state = RecoveryState.initial("ARXUSDT", BASELINE, now=NOW)
    for action_id in (ActionId.INVENTORY_RECOVER, ActionId.BASELINE_TUNE):
        for side in (Side.BUY, Side.SELL):
            state = state.with_exhausted_attempt(
                action_id,
                side,
                OrderRole.REDUCE_ONLY,
                now=NOW,
            )
    state = replace(
        state,
        active_episode_fingerprint="bilateral-baseline-exhausted",
    )

    blocked = engine.plan_round(
        snapshot=_snapshot(
            assessment=FlowBlockerAssessment(
                inventory_reduce_sides=(Side.BUY, Side.SELL),
                episode_fingerprint="bilateral-baseline-exhausted",
            )
        ),
        state=state,
        now=NOW + timedelta(seconds=1),
        round_id="bilateral-baseline-exhausted",
    )

    assert blocked.action_id is ActionId.NOOP
    assert blocked.liveness_status == "blocked"
    assert blocked.effect_stage is EffectStage.NONE
    assert blocked.next_state.exhausted_attempts == state.exhausted_attempts


def test_all_exhausted_bilateral_maker_tuples_advance_to_baseline_tune() -> None:
    engine = FuturesRecoveryDecisionEngine()
    state = RecoveryState.initial("ARXUSDT", BASELINE, now=NOW)
    state = state.with_exhausted_attempt(
        ActionId.MAKER_FLOW_RECOVER,
        Side.BUY,
        OrderRole.ENTRY,
        now=NOW,
    ).with_exhausted_attempt(
        ActionId.MAKER_FLOW_RECOVER,
        Side.SELL,
        OrderRole.ENTRY,
        now=NOW,
    )
    state = replace(
        state,
        active_episode_fingerprint="bilateral-maker-flow",
        fairness_last_selected_sides=MappingProxyType(
            {"maker_flow_recover:entry": Side.SELL}
        ),
    )

    plan = engine.plan_round(
        snapshot=_snapshot(
            assessment=FlowBlockerAssessment(
                missing_entry_sides=(Side.BUY, Side.SELL),
                episode_fingerprint="bilateral-maker-flow",
            )
        ),
        state=state,
        now=NOW + timedelta(seconds=1),
        round_id="bilateral-new-fair-cycle",
    )

    assert plan.action_id is ActionId.BASELINE_TUNE
    assert plan.side is Side.BUY
    assert plan.order_role is OrderRole.ENTRY
    assert plan.liveness_status == "recovering"


def test_maker_flow_overlay_is_bounded_and_only_relaxes_same_side_price_guard() -> None:
    baseline = {
        **BASELINE,
        "best_quote_maker_volume_same_side_entry_price_guard_enabled": True,
        "best_quote_maker_volume_same_side_entry_price_guard_report_only": False,
        "near_market_entry_max_center_distance_steps": 1.5,
        "near_market_reentry_confirm_cycles": 3,
    }
    plan = FuturesRecoveryDecisionEngine().plan_round(
        snapshot=_snapshot(
            assessment=FlowBlockerAssessment(
                missing_entry_sides=(Side.BUY,),
                episode_fingerprint="maker-overlay",
            )
        ),
        state=RecoveryState.initial("ARXUSDT", baseline, now=NOW),
        now=NOW,
        round_id="maker-overlay-enter",
    )

    assert plan.action_id is ActionId.MAKER_FLOW_RECOVER
    assert plan.side is Side.BUY
    assert plan.order_role is OrderRole.ENTRY
    assert (
        plan.desired_profile.fields[
            "best_quote_maker_volume_same_side_entry_price_guard_report_only"
        ]
        is True
    )
    assert plan.desired_profile.fields["near_market_entry_max_center_distance_steps"] == 1.5
    assert plan.desired_profile.fields["near_market_reentry_confirm_cycles"] == 3
    assert plan.desired_profile.fields["volatility_entry_pause_enabled"] is True
    assert plan.desired_profile.fields["best_quote_maker_volume_allow_loss_reduce_only"] is False


def test_exhausted_maker_flow_upgrades_once_to_numeric_baseline_tune() -> None:
    baseline = {
        **BASELINE,
        "best_quote_maker_volume_same_side_entry_price_guard_enabled": True,
        "best_quote_maker_volume_same_side_entry_price_guard_report_only": False,
        "near_market_entry_max_center_distance_steps": 1.5,
        "near_market_reentry_confirm_cycles": 3,
    }
    assessment = FlowBlockerAssessment(
        missing_entry_sides=(Side.BUY,),
        episode_fingerprint="maker-baseline-successor",
    )
    exhausted = replace(
        RecoveryState.initial("ARXUSDT", baseline, now=NOW).with_exhausted_attempt(
            ActionId.MAKER_FLOW_RECOVER,
            Side.BUY,
            OrderRole.ENTRY,
            now=NOW,
        ),
        active_episode_fingerprint="maker-baseline-successor",
    )

    tuned = FuturesRecoveryDecisionEngine().plan_round(
        snapshot=_snapshot(assessment=assessment),
        state=exhausted,
        now=NOW,
        round_id="maker-baseline-successor",
    )

    assert tuned.action_id is ActionId.BASELINE_TUNE
    assert tuned.side is Side.BUY
    assert tuned.order_role is OrderRole.ENTRY
    assert tuned.desired_profile.fields["near_market_entry_max_center_distance_steps"] == 999.0
    assert tuned.desired_profile.fields["near_market_reentry_confirm_cycles"] == 1
    assert tuned.desired_profile.fields["volatility_entry_pause_enabled"] is True
    assert tuned.desired_profile.fields["best_quote_maker_volume_allow_loss_reduce_only"] is False

    all_exhausted = tuned.next_state.with_exhausted_attempt(
        ActionId.BASELINE_TUNE,
        Side.BUY,
        OrderRole.ENTRY,
        now=NOW + timedelta(minutes=1),
    )
    blocked = FuturesRecoveryDecisionEngine().plan_round(
        snapshot=_snapshot(
            now=NOW + timedelta(minutes=2),
            assessment=assessment,
        ),
        state=replace(
            all_exhausted,
            phase=RecoveryPhase.STABLE,
            active_action=ActionId.NOOP,
            decision_id=None,
            side=None,
            order_role=None,
            reasons=(),
            issued_at=None,
            progress_deadline_at=None,
            hard_expires_at=None,
            pending_effect_stage=EffectStage.NONE,
            pending_effect_epoch=None,
            desired_profile=all_exhausted.baseline_profile,
        ),
        now=NOW + timedelta(minutes=2),
        round_id="maker-baseline-exhausted",
    )

    assert blocked.action_id is ActionId.NOOP
    assert blocked.liveness_status == "blocked"
    assert "action_attempt_exhausted:baseline_tune:BUY:entry" in blocked.reasons


def test_maker_and_baseline_timeouts_end_in_visible_exhausted_noop() -> None:
    policy = RecoveryPolicy(
        action_progress_timeout=timedelta(seconds=5),
        action_hard_ttl=timedelta(seconds=10),
        restore_timeout=timedelta(seconds=5),
        cooldown=timedelta(0),
    )
    engine = FuturesRecoveryDecisionEngine(policy=policy)
    assessment = FlowBlockerAssessment(
        missing_entry_sides=(Side.BUY,),
        episode_fingerprint="maker-timeout-ladder",
    )
    maker = engine.plan_round(
        snapshot=_snapshot(assessment=assessment),
        state=RecoveryState.initial("ARXUSDT", BASELINE, now=NOW),
        now=NOW,
        round_id="maker-timeout-enter",
    )
    maker_effect_ack = engine.plan_round(
        snapshot=_snapshot(
            now=NOW + timedelta(seconds=1),
            assessment=assessment,
            effect_receipt=EffectReceipt(
                decision_id=str(maker.next_state.decision_id),
                stage=EffectStage.RUNNER_RESTART,
                effect_epoch=int(maker.effect_epoch),
                observed_at=NOW + timedelta(seconds=1),
            ),
        ),
        state=maker.next_state,
        now=NOW + timedelta(seconds=1),
        round_id="maker-timeout-effect-ack",
    )
    maker_timeout = engine.plan_round(
        snapshot=_snapshot(
            now=NOW + timedelta(seconds=5),
            assessment=assessment,
        ),
        state=maker_effect_ack.next_state,
        now=NOW + timedelta(seconds=5),
        round_id="maker-timeout",
    )
    maker_cleaned = _complete_cleanup(
        engine,
        maker_timeout,
        now=NOW + timedelta(seconds=5),
        round_id="maker-timeout-cleaned",
        assessment=assessment,
    )
    maker_restored = engine.plan_round(
        snapshot=_snapshot(
            now=NOW + timedelta(seconds=6),
            assessment=assessment,
            applied_generation=maker_cleaned.next_state.generation,
            applied_profile_digest=maker_cleaned.next_state.desired_profile.digest,
        ),
        state=maker_cleaned.next_state,
        now=NOW + timedelta(seconds=6),
        round_id="maker-timeout-restored",
    )
    maker_stable = engine.plan_round(
        snapshot=_snapshot(
            now=NOW + timedelta(seconds=6),
            assessment=assessment,
        ),
        state=maker_restored.next_state,
        now=NOW + timedelta(seconds=6),
        round_id="maker-timeout-stable",
    )
    baseline = engine.plan_round(
        snapshot=_snapshot(
            now=NOW + timedelta(seconds=7),
            assessment=assessment,
        ),
        state=maker_stable.next_state,
        now=NOW + timedelta(seconds=7),
        round_id="baseline-timeout-enter",
    )
    assert baseline.action_id is ActionId.BASELINE_TUNE

    baseline_effect_ack = engine.plan_round(
        snapshot=_snapshot(
            now=NOW + timedelta(seconds=8),
            assessment=assessment,
            effect_receipt=EffectReceipt(
                decision_id=str(baseline.next_state.decision_id),
                stage=EffectStage.RUNNER_RESTART,
                effect_epoch=int(baseline.effect_epoch),
                observed_at=NOW + timedelta(seconds=8),
            ),
        ),
        state=baseline.next_state,
        now=NOW + timedelta(seconds=8),
        round_id="baseline-timeout-effect-ack",
    )
    baseline_timeout = engine.plan_round(
        snapshot=_snapshot(
            now=NOW + timedelta(seconds=12),
            assessment=assessment,
        ),
        state=baseline_effect_ack.next_state,
        now=NOW + timedelta(seconds=12),
        round_id="baseline-timeout",
    )
    baseline_cleaned = _complete_cleanup(
        engine,
        baseline_timeout,
        now=NOW + timedelta(seconds=12),
        round_id="baseline-timeout-cleaned",
        assessment=assessment,
    )
    baseline_restored = engine.plan_round(
        snapshot=_snapshot(
            now=NOW + timedelta(seconds=13),
            assessment=assessment,
            applied_generation=baseline_cleaned.next_state.generation,
            applied_profile_digest=baseline_cleaned.next_state.desired_profile.digest,
        ),
        state=baseline_cleaned.next_state,
        now=NOW + timedelta(seconds=13),
        round_id="baseline-timeout-restored",
    )
    baseline_stable = engine.plan_round(
        snapshot=_snapshot(
            now=NOW + timedelta(seconds=13),
            assessment=assessment,
        ),
        state=baseline_restored.next_state,
        now=NOW + timedelta(seconds=13),
        round_id="baseline-timeout-stable",
    )
    blocked = engine.plan_round(
        snapshot=_snapshot(
            now=NOW + timedelta(seconds=14),
            assessment=assessment,
        ),
        state=baseline_stable.next_state,
        now=NOW + timedelta(seconds=14),
        round_id="baseline-timeout-blocked",
    )

    assert blocked.action_id is ActionId.NOOP
    assert blocked.liveness_status == "blocked"
    assert "action_attempt_exhausted:maker_flow_recover:BUY:entry" in blocked.reasons
    assert "action_attempt_exhausted:baseline_tune:BUY:entry" in blocked.reasons


def test_exhausted_ordinary_actions_rearm_after_bounded_backoff_without_resetting_episode() -> None:
    flow_key = FlowRoleKey(
        LedgerClass.NORMAL_BQ,
        Side.BUY,
        OrderRole.ENTRY,
    )
    state = RecoveryState.initial("ARXUSDT", BASELINE, now=NOW)
    state = state.with_flow_clock(
        flow_key,
        quote_missing_since=NOW - timedelta(minutes=4),
        submit_missing_since=NOW - timedelta(minutes=3),
        fill_missing_since=NOW - timedelta(minutes=2),
    ).with_temporary_loss_lease_use("ordinary-backoff", count=2)
    state = replace(
        state.with_exhausted_attempt(
            ActionId.MAKER_FLOW_RECOVER,
            Side.BUY,
            OrderRole.ENTRY,
            now=NOW,
        ).with_exhausted_attempt(
            ActionId.BASELINE_TUNE,
            Side.BUY,
            OrderRole.ENTRY,
            now=NOW,
        ),
        active_episode_fingerprint="ordinary-backoff",
    )
    assessment = FlowBlockerAssessment(
        missing_entry_sides=(Side.BUY,),
        episode_fingerprint="ordinary-backoff",
    )
    engine = FuturesRecoveryDecisionEngine(
        policy=RecoveryPolicy(exhausted_retry_backoff=timedelta(seconds=30))
    )

    waiting = engine.plan_round(
        snapshot=_snapshot(
            now=NOW + timedelta(seconds=29),
            assessment=assessment,
        ),
        state=state,
        now=NOW + timedelta(seconds=29),
        round_id="ordinary-backoff-wait",
    )
    retried = engine.plan_round(
        snapshot=_snapshot(
            now=NOW + timedelta(seconds=30),
            assessment=assessment,
        ),
        state=waiting.next_state,
        now=NOW + timedelta(seconds=30),
        round_id="ordinary-backoff-retry",
    )

    assert waiting.action_id is ActionId.NOOP
    assert waiting.liveness_status == "blocked"
    assert retried.action_id is ActionId.MAKER_FLOW_RECOVER
    assert retried.side is Side.BUY
    assert retried.next_state.flow_clocks == state.flow_clocks
    assert retried.next_state.active_episode_fingerprint == "ordinary-backoff"
    assert retried.next_state.temporary_loss_lease_uses == {"ordinary-backoff": 2}
    assert retried.next_state.action_lease is None
    assert retried.desired_profile.fields["best_quote_maker_volume_allow_loss_reduce_only"] is False


def test_terminal_deadline_preempts_an_expired_ordinary_retry_backoff() -> None:
    state = RecoveryState.initial("ARXUSDT", BASELINE, now=NOW)
    state = replace(
        state.with_exhausted_attempt(
            ActionId.MAKER_FLOW_RECOVER,
            Side.BUY,
            OrderRole.ENTRY,
            now=NOW,
        ).with_exhausted_attempt(
            ActionId.BASELINE_TUNE,
            Side.BUY,
            OrderRole.ENTRY,
            now=NOW,
        ),
        active_episode_fingerprint="finite-run-deadline",
    )
    engine = FuturesRecoveryDecisionEngine(
        policy=RecoveryPolicy(exhausted_retry_backoff=timedelta(seconds=30))
    )

    deadline = engine.plan_round(
        snapshot=_snapshot(
            now=NOW + timedelta(seconds=30),
            assessment=FlowBlockerAssessment(
                terminal_reason="trusted_lifecycle_terminal_handoff",
                terminal_lifecycle_handoff=True,
                missing_entry_sides=(Side.BUY,),
                episode_fingerprint="finite-run-deadline",
            ),
        ),
        state=state,
        now=NOW + timedelta(seconds=30),
        round_id="finite-run-deadline",
    )

    assert deadline.action_id is ActionId.TERMINAL_HANDOFF
    assert deadline.liveness_status == "terminal_handoff"
    assert deadline.next_state.active_action is ActionId.NOOP


def test_temporary_allow_loss_expires_and_cleans_without_exchange_observation() -> None:
    policy = RecoveryPolicy(
        action_progress_timeout=timedelta(seconds=30),
        action_hard_ttl=timedelta(seconds=60),
        cooldown=timedelta(seconds=5),
    )
    engine = FuturesRecoveryDecisionEngine(policy=policy)
    state = RecoveryState.initial("ARXUSDT", BASELINE, now=NOW)
    state = state.with_exhausted_attempt(
        ActionId.INVENTORY_RECOVER,
        Side.SELL,
        OrderRole.REDUCE_ONLY,
        now=NOW,
    )
    assessment = FlowBlockerAssessment(
        inventory_reduce_sides=(Side.SELL,),
        loss_only_blocked_sides=(Side.SELL,),
    )

    entered = engine.plan_round(
        snapshot=_snapshot(assessment=assessment),
        state=state,
        now=NOW,
        round_id="tlr-enter",
    )
    assert entered.action_id is ActionId.TEMPORARY_LOSS_RELIEF
    assert entered.next_state.action_lease is not None
    assert entered.next_state.action_lease.hard_expires_at == NOW + timedelta(
        seconds=60
    )
    assert (
        entered.desired_profile.fields["best_quote_maker_volume_allow_loss_reduce_only"]
        is False
    )
    assert entered.next_state.phase is RecoveryPhase.SETTLING
    assert entered.next_state.temporary_loss_lease_uses == {}

    activated = _activate_temporary_loss(
        engine,
        entered,
        assessment,
        now=NOW + timedelta(seconds=1),
        round_id="tlr-activate",
    )
    assert activated.next_state.phase is RecoveryPhase.ACTIVE
    assert (
        activated.desired_profile.fields[
            "best_quote_maker_volume_allow_loss_reduce_only"
        ]
        is True
    )
    assert activated.next_state.action_lease.activated_at == NOW + timedelta(seconds=1)
    assert activated.next_state.action_lease.hard_expires_at == NOW + timedelta(
        seconds=60
    )
    assert sum(activated.next_state.temporary_loss_lease_uses.values()) == 1

    expired = engine.plan_round(
        snapshot=_snapshot(
            now=NOW + timedelta(seconds=61),
            assessment=assessment,
            exchange_observation_available=False,
            managed_order_manifest=_managed_manifest(activated.next_state, "7001"),
        ),
        state=activated.next_state,
        now=NOW + timedelta(seconds=61),
        round_id="tlr-expire",
    )

    assert expired.action_id is ActionId.TEMPORARY_LOSS_RELIEF
    assert expired.mode is ActionMode.EXIT
    assert expired.next_state.phase is RecoveryPhase.CLEANING
    assert expired.effect_stage is EffectStage.MANAGED_GTX_CANCEL
    assert (
        expired.desired_profile.fields["best_quote_maker_volume_allow_loss_reduce_only"]
        is False
    )
    assert expired.next_state.cleanup_obligation is not None
    assert expired.next_state.cleanup_obligation.open_order_ids == ("7001",)
    assert expired.next_state.action_lease.hard_expires_at == NOW + timedelta(
        seconds=60
    )


def test_temporary_loss_activation_requires_exact_receipt_and_deadline_wins() -> None:
    policy = RecoveryPolicy(
        action_progress_timeout=timedelta(seconds=10),
        action_hard_ttl=timedelta(seconds=20),
    )
    engine = FuturesRecoveryDecisionEngine(policy=policy)
    assessment = FlowBlockerAssessment(
        inventory_reduce_sides=(Side.SELL,),
        loss_only_blocked_sides=(Side.SELL,),
        episode_fingerprint="activation-gate-1",
    )
    state = RecoveryState.initial("ARXUSDT", BASELINE, now=NOW)
    state = state.with_exhausted_attempt(
        ActionId.INVENTORY_RECOVER,
        Side.SELL,
        OrderRole.REDUCE_ONLY,
        now=NOW,
    )
    entered = engine.plan_round(
        snapshot=_snapshot(assessment=assessment),
        state=state,
        now=NOW,
        round_id="activation-enter",
    )
    lease = entered.next_state.action_lease
    assert lease is not None

    mismatched = engine.plan_round(
        snapshot=_snapshot(
            now=NOW + timedelta(seconds=1),
            assessment=assessment,
            activation_receipt=ActivationReceipt(
                decision_id=entered.next_state.decision_id,
                generation=entered.next_state.generation,
                profile_digest="wrong-profile-digest",
                action_lease_epoch=lease.epoch,
                observed_at=NOW + timedelta(seconds=1),
            ),
        ),
        state=entered.next_state,
        now=NOW + timedelta(seconds=1),
        round_id="activation-mismatch",
    )
    assert mismatched.mode is ActionMode.ADVANCE
    assert mismatched.effect_stage is EffectStage.RUNNER_RESTART
    assert mismatched.next_state.phase is RecoveryPhase.SETTLING
    assert (
        mismatched.desired_profile.fields[
            "best_quote_maker_volume_allow_loss_reduce_only"
        ]
        is False
    )
    assert mismatched.next_state.temporary_loss_lease_uses == {}

    at_deadline = engine.plan_round(
        snapshot=_snapshot(
            now=NOW + timedelta(seconds=10),
            assessment=assessment,
            activation_receipt=ActivationReceipt(
                decision_id=mismatched.next_state.decision_id,
                generation=mismatched.next_state.generation,
                profile_digest=mismatched.next_state.desired_profile.digest,
                action_lease_epoch=lease.epoch,
                observed_at=NOW + timedelta(seconds=10),
            ),
        ),
        state=mismatched.next_state,
        now=NOW + timedelta(seconds=10),
        round_id="activation-deadline",
    )
    assert at_deadline.mode is ActionMode.EXIT
    assert at_deadline.next_state.phase is RecoveryPhase.CLEANING
    assert (
        at_deadline.desired_profile.fields[
            "best_quote_maker_volume_allow_loss_reduce_only"
        ]
        is False
    )
    assert at_deadline.next_state.temporary_loss_lease_uses == {}


def test_temporary_loss_activation_fences_new_generation_with_runner_restart() -> None:
    engine = FuturesRecoveryDecisionEngine()
    assessment = FlowBlockerAssessment(
        inventory_reduce_sides=(Side.SELL,),
        loss_only_blocked_sides=(Side.SELL,),
        episode_fingerprint="activation-restart-fence",
    )
    state = RecoveryState.initial("ARXUSDT", BASELINE, now=NOW)
    state = state.with_exhausted_attempt(
        ActionId.INVENTORY_RECOVER,
        Side.SELL,
        OrderRole.REDUCE_ONLY,
        now=NOW,
    )
    entered = engine.plan_round(
        snapshot=_snapshot(assessment=assessment),
        state=state,
        now=NOW,
        round_id="activation-restart-enter",
    )
    settling = entered.next_state
    lease = settling.action_lease
    assert lease is not None

    activated_at = NOW + timedelta(seconds=1)
    activated = engine.plan_round(
        snapshot=_snapshot(
            now=activated_at,
            assessment=assessment,
            activation_receipt=ActivationReceipt(
                decision_id=settling.decision_id,
                generation=settling.generation,
                profile_digest=settling.desired_profile.digest,
                action_lease_epoch=lease.epoch,
                observed_at=activated_at,
            ),
        ),
        state=settling,
        now=activated_at,
        round_id="activation-restart-activate",
    )

    assert activated.next_state.generation == settling.generation + 1
    assert activated.next_state.phase is RecoveryPhase.ACTIVE
    assert activated.effect_stage is EffectStage.RUNNER_RESTART
    assert activated.effect_epoch is not None
    assert activated.next_state.pending_effect_stage is EffectStage.RUNNER_RESTART
    assert activated.next_state.pending_effect_epoch == activated.effect_epoch

    acknowledged_at = activated_at + timedelta(milliseconds=1)
    acknowledged = engine.plan_round(
        snapshot=_snapshot(
            now=acknowledged_at,
            assessment=assessment,
            effect_receipt=EffectReceipt(
                decision_id=activated.next_state.decision_id,
                stage=EffectStage.RUNNER_RESTART,
                effect_epoch=activated.effect_epoch,
                observed_at=acknowledged_at,
            ),
        ),
        state=activated.next_state,
        now=acknowledged_at,
        round_id="activation-restart-ack",
    )

    assert acknowledged.next_state.phase is RecoveryPhase.ACTIVE
    assert acknowledged.next_state.pending_effect_stage is EffectStage.NONE
    assert acknowledged.next_state.pending_effect_epoch is None
    assert acknowledged.effect_stage is EffectStage.NONE


def test_action_lease_hard_expiry_cannot_be_bypassed_by_missing_state_deadlines() -> (
    None
):
    policy = RecoveryPolicy(
        action_progress_timeout=timedelta(seconds=10),
        action_hard_ttl=timedelta(seconds=20),
    )
    engine = FuturesRecoveryDecisionEngine(policy=policy)
    assessment = FlowBlockerAssessment(
        inventory_reduce_sides=(Side.SELL,),
        loss_only_blocked_sides=(Side.SELL,),
        episode_fingerprint="lease-deadline-source-of-truth",
    )
    state = RecoveryState.initial("ARXUSDT", BASELINE, now=NOW)
    state = state.with_exhausted_attempt(
        ActionId.INVENTORY_RECOVER,
        Side.SELL,
        OrderRole.REDUCE_ONLY,
        now=NOW,
    )
    entered = engine.plan_round(
        snapshot=_snapshot(assessment=assessment),
        state=state,
        now=NOW,
        round_id="lease-deadline-enter",
    )
    activated = _activate_temporary_loss(
        engine,
        entered,
        assessment,
        now=NOW + timedelta(seconds=1),
        round_id="lease-deadline-active",
    )
    corrupted_deadlines = replace(
        activated.next_state,
        progress_deadline_at=None,
        hard_expires_at=None,
    )

    expired = engine.plan_round(
        snapshot=_snapshot(
            now=NOW + timedelta(seconds=21),
            assessment=assessment,
            managed_order_manifest=_managed_manifest(corrupted_deadlines, "7002"),
        ),
        state=corrupted_deadlines,
        now=NOW + timedelta(seconds=21),
        round_id="lease-deadline-expired",
    )

    assert expired.mode is ActionMode.EXIT
    assert expired.next_state.phase is RecoveryPhase.CLEANING
    assert (
        expired.desired_profile.fields["best_quote_maker_volume_allow_loss_reduce_only"]
        is False
    )


def test_runner_side_authorization_reclaims_loss_at_absolute_lease_expiry() -> None:
    engine = FuturesRecoveryDecisionEngine(
        policy=RecoveryPolicy(
            action_progress_timeout=timedelta(seconds=10),
            action_hard_ttl=timedelta(seconds=20),
        )
    )
    assessment = FlowBlockerAssessment(
        inventory_reduce_sides=(Side.SELL,),
        loss_only_blocked_sides=(Side.SELL,),
        episode_fingerprint="runner-independent-expiry",
    )
    state = RecoveryState.initial("ARXUSDT", BASELINE, now=NOW)
    state = state.with_exhausted_attempt(
        ActionId.INVENTORY_RECOVER,
        Side.SELL,
        OrderRole.REDUCE_ONLY,
        now=NOW,
    )
    entered = engine.plan_round(
        snapshot=_snapshot(assessment=assessment),
        state=state,
        now=NOW,
        round_id="runner-expiry-enter",
    )
    activated = _activate_temporary_loss(
        engine,
        entered,
        assessment,
        now=NOW + timedelta(seconds=1),
        round_id="runner-expiry-active",
    ).next_state

    assert (
        temporary_loss_authorized(
            activated,
            now=NOW + timedelta(seconds=19),
        )
        is True
    )
    assert (
        temporary_loss_authorized(
            activated,
            now=NOW + timedelta(seconds=20),
        )
        is False
    )
    assert (
        temporary_loss_authorized(
            activated,
            now=NOW + timedelta(days=1),
        )
        is False
    )


def test_unknown_lease_manifest_cannot_skip_cleaning_and_proof_is_bound() -> None:
    policy = RecoveryPolicy(
        action_progress_timeout=timedelta(seconds=10),
        action_hard_ttl=timedelta(seconds=20),
        cleanup_retry_interval=timedelta(seconds=5),
    )
    engine = FuturesRecoveryDecisionEngine(policy=policy)
    state = RecoveryState.initial("ARXUSDT", BASELINE, now=NOW)
    state = state.with_exhausted_attempt(
        ActionId.INVENTORY_RECOVER,
        Side.SELL,
        OrderRole.REDUCE_ONLY,
        now=NOW,
    )
    assessment = FlowBlockerAssessment(
        inventory_reduce_sides=(Side.SELL,),
        loss_only_blocked_sides=(Side.SELL,),
        episode_fingerprint="episode-cleanup-1",
    )
    entered = engine.plan_round(
        snapshot=_snapshot(assessment=assessment),
        state=state,
        now=NOW,
        round_id="cleanup-enter",
    )
    activated = _activate_temporary_loss(
        engine,
        entered,
        assessment,
        now=NOW + timedelta(seconds=1),
        round_id="cleanup-activate",
    )
    expired_at = NOW + timedelta(seconds=21)
    expired = engine.plan_round(
        snapshot=_snapshot(
            now=expired_at,
            assessment=assessment,
            exchange_observation_available=False,
        ),
        state=activated.next_state,
        now=expired_at,
        round_id="cleanup-expire",
    )

    obligation = expired.next_state.cleanup_obligation
    assert expired.next_state.phase is RecoveryPhase.CLEANING
    assert expired.effect_stage is EffectStage.LOCAL_STATE_REPAIR
    assert obligation is not None
    assert obligation.needs_manifest_rebuild is True
    assert obligation.open_order_ids == ()

    wrong = engine.plan_round(
        snapshot=_snapshot(
            now=expired_at + timedelta(seconds=1),
            assessment=assessment,
            cleanup_proof=replace(
                _cleanup_proof(
                    obligation,
                    observed_at=expired_at + timedelta(seconds=1),
                    user_trades_watermark=10,
                ),
                source_decision_id="different-decision",
            ),
        ),
        state=expired.next_state,
        now=expired_at + timedelta(seconds=1),
        round_id="cleanup-wrong-proof",
    )
    assert wrong.next_state.phase is RecoveryPhase.CLEANING

    future = engine.plan_round(
        snapshot=_snapshot(
            now=expired_at + timedelta(seconds=1, milliseconds=500),
            assessment=assessment,
            cleanup_proof=_cleanup_proof(
                obligation,
                observed_at=expired_at + timedelta(minutes=5),
                user_trades_watermark=10,
            ),
        ),
        state=wrong.next_state,
        now=expired_at + timedelta(seconds=1, milliseconds=500),
        round_id="cleanup-future-proof",
    )
    assert future.next_state.phase is RecoveryPhase.CLEANING

    complete = engine.plan_round(
        snapshot=_snapshot(
            now=expired_at + timedelta(seconds=2),
            assessment=assessment,
            cleanup_proof=_cleanup_proof(
                obligation,
                observed_at=expired_at + timedelta(seconds=2),
                user_trades_watermark=11,
            ),
        ),
        state=future.next_state,
        now=expired_at + timedelta(seconds=2),
        round_id="cleanup-valid-proof",
    )
    assert complete.next_state.phase is RecoveryPhase.RESTORING
    assert complete.next_state.cleanup_obligation is None


def test_safety_and_terminal_preemption_preserve_lease_cleanup_obligation() -> None:
    policy = RecoveryPolicy(
        action_progress_timeout=timedelta(seconds=10),
        action_hard_ttl=timedelta(seconds=30),
        cleanup_retry_interval=timedelta(seconds=5),
    )
    engine = FuturesRecoveryDecisionEngine(policy=policy)
    loss_assessment = FlowBlockerAssessment(
        inventory_reduce_sides=(Side.SELL,),
        loss_only_blocked_sides=(Side.SELL,),
        episode_fingerprint="preempt-cleanup-1",
    )
    state = RecoveryState.initial("ARXUSDT", BASELINE, now=NOW)
    state = state.with_exhausted_attempt(
        ActionId.INVENTORY_RECOVER,
        Side.SELL,
        OrderRole.REDUCE_ONLY,
        now=NOW,
    )
    entered = engine.plan_round(
        snapshot=_snapshot(assessment=loss_assessment),
        state=state,
        now=NOW,
        round_id="preempt-loss-enter",
    )
    activated = _activate_temporary_loss(
        engine,
        entered,
        loss_assessment,
        now=NOW + timedelta(seconds=1),
        round_id="preempt-loss-activate",
    )
    source_decision_id = activated.next_state.decision_id

    safety = engine.plan_round(
        snapshot=_snapshot(
            now=NOW + timedelta(seconds=2),
            assessment=FlowBlockerAssessment(
                safety_reasons=("volatility",),
                safety_evidence_fingerprint="safety-preempt-1",
                safety_observation_seq=1,
                safety_observed_at=NOW + timedelta(seconds=2),
            ),
            managed_order_manifest=_managed_manifest(activated.next_state, "7003"),
        ),
        state=activated.next_state,
        now=NOW + timedelta(seconds=2),
        round_id="preempt-safety",
    )
    obligation = safety.next_state.cleanup_obligation
    assert safety.action_id is ActionId.SAFETY_CONVERGE
    assert safety.mode is ActionMode.ENTER
    assert safety.next_state.phase is RecoveryPhase.CLEANING
    assert safety.effect_stage is EffectStage.MANAGED_GTX_CANCEL
    assert (
        safety.desired_profile.fields["best_quote_maker_volume_allow_loss_reduce_only"]
        is False
    )
    assert obligation is not None
    assert obligation.source_decision_id == source_decision_id
    assert obligation.open_order_ids == ("7003",)

    terminal = engine.plan_round(
        snapshot=_snapshot(
            now=NOW + timedelta(seconds=3),
            assessment=FlowBlockerAssessment(
                terminal_reason="trusted_competition_window_end",
                terminal_clean_flat_proof=True,
            ),
        ),
        state=safety.next_state,
        now=NOW + timedelta(seconds=3),
        round_id="preempt-terminal",
    )
    assert terminal.action_id is ActionId.TERMINAL_STOP
    assert terminal.next_state.phase is RecoveryPhase.CLEANING
    assert terminal.next_state.cleanup_obligation == obligation
    assert terminal.effect_stage is EffectStage.RUNNER_STOP
    assert terminal.next_state.terminal_stop_confirmed is False

    stop_confirmed = engine.plan_round(
        snapshot=_snapshot(
            now=NOW + timedelta(seconds=4),
            assessment=FlowBlockerAssessment(
                terminal_reason="trusted_competition_window_end",
                terminal_clean_flat_proof=True,
            ),
            effect_receipt=EffectReceipt(
                decision_id=terminal.next_state.decision_id,
                stage=EffectStage.RUNNER_STOP,
                effect_epoch=terminal.effect_epoch,
                observed_at=NOW + timedelta(seconds=4),
            ),
        ),
        state=terminal.next_state,
        now=NOW + timedelta(seconds=4),
        round_id="terminal-stop-confirmed",
    )
    assert stop_confirmed.next_state.phase is RecoveryPhase.CLEANING
    assert stop_confirmed.next_state.terminal_stop_confirmed is True
    assert stop_confirmed.effect_stage is EffectStage.NONE

    cleaned = engine.plan_round(
        snapshot=_snapshot(
            now=NOW + timedelta(seconds=5),
            assessment=FlowBlockerAssessment(
                terminal_reason="trusted_competition_window_end",
                terminal_clean_flat_proof=True,
            ),
            cleanup_proof=_cleanup_proof(
                obligation,
                observed_at=NOW + timedelta(seconds=5),
                user_trades_watermark=12,
            ),
        ),
        state=stop_confirmed.next_state,
        now=NOW + timedelta(seconds=5),
        round_id="terminal-cleaned",
    )
    assert cleaned.next_state.phase is RecoveryPhase.STOPPED
    assert cleaned.next_state.cleanup_obligation is None
    assert cleaned.next_state.action_lease is None
    assert cleaned.next_state.desired_runner_state == "stopped"

    handoff = engine.plan_round(
        snapshot=_snapshot(
            now=NOW + timedelta(seconds=6),
            assessment=FlowBlockerAssessment(
                terminal_reason="trusted_lifecycle_terminal_handoff",
                terminal_lifecycle_handoff=True,
            ),
        ),
        state=cleaned.next_state,
        now=NOW + timedelta(seconds=6),
        round_id="terminal-stopped-handoff",
    )
    assert handoff.action_id is ActionId.TERMINAL_HANDOFF
    assert handoff.mode is ActionMode.EXIT
    assert handoff.effect_stage is EffectStage.NONE
    assert handoff.next_state.phase is RecoveryPhase.STABLE
    assert handoff.next_state.desired_runner_state == "running"


def test_terminal_lifecycle_handoff_cleans_active_loss_lease_then_returns_stable() -> None:
    engine = FuturesRecoveryDecisionEngine(
        policy=RecoveryPolicy(cleanup_retry_interval=timedelta(seconds=5))
    )
    loss_assessment = FlowBlockerAssessment(
        inventory_reduce_sides=(Side.SELL,),
        loss_only_blocked_sides=(Side.SELL,),
        episode_fingerprint="terminal-handoff-loss",
    )
    state = RecoveryState.initial("ARXUSDT", BASELINE, now=NOW).with_exhausted_attempt(
        ActionId.INVENTORY_RECOVER,
        Side.SELL,
        OrderRole.REDUCE_ONLY,
        now=NOW,
    )
    entered = engine.plan_round(
        snapshot=_snapshot(assessment=loss_assessment),
        state=state,
        now=NOW,
        round_id="terminal-handoff-enter",
    )
    active = _activate_temporary_loss(
        engine,
        entered,
        loss_assessment,
        now=NOW + timedelta(seconds=1),
        round_id="terminal-handoff-activate",
    ).next_state
    assert active.action_lease is not None
    source_decision_id = str(active.decision_id)

    handoff = engine.plan_round(
        snapshot=_snapshot(
            now=NOW + timedelta(seconds=2),
            assessment=FlowBlockerAssessment(
                terminal_reason="trusted_competition_window_end",
                terminal_lifecycle_handoff=True,
            ),
            managed_order_manifest=_managed_manifest(active, "7004"),
        ),
        state=active,
        now=NOW + timedelta(seconds=2),
        round_id="terminal-handoff-preempt",
    )

    assert handoff.action_id is ActionId.TERMINAL_HANDOFF
    assert handoff.next_state.phase is RecoveryPhase.CLEANING
    assert handoff.next_state.desired_runner_state == "running"
    assert handoff.effect_stage is EffectStage.MANAGED_GTX_CANCEL
    assert (
        handoff.next_state.desired_profile.fields[
            "best_quote_maker_volume_allow_loss_reduce_only"
        ]
        is False
    )
    obligation = handoff.next_state.cleanup_obligation
    assert obligation is not None
    assert obligation.source_decision_id == source_decision_id
    assert obligation.open_order_ids == ("7004",)
    assert handoff.next_state.action_lease is active.action_lease

    stable = engine.plan_round(
        snapshot=_snapshot(
            now=NOW + timedelta(seconds=3),
            assessment=FlowBlockerAssessment(
                terminal_reason="trusted_competition_window_end",
                terminal_lifecycle_handoff=True,
            ),
            cleanup_proof=_cleanup_proof(
                obligation,
                observed_at=NOW + timedelta(seconds=3),
                user_trades_watermark=99,
            ),
        ),
        state=handoff.next_state,
        now=NOW + timedelta(seconds=3),
        round_id="terminal-handoff-clean",
    )

    assert stable.action_id is ActionId.TERMINAL_HANDOFF
    assert stable.next_state.phase is RecoveryPhase.STABLE
    assert stable.next_state.active_action is ActionId.NOOP
    assert stable.next_state.cleanup_obligation is None
    assert stable.next_state.action_lease is None
    assert stable.next_state.desired_profile.digest == stable.next_state.baseline_profile.digest
    assert stable.next_state.pending_effect_stage is EffectStage.NONE


def test_delayed_terminal_handoff_cleanup_cannot_expire_into_runner_restart() -> None:
    engine = FuturesRecoveryDecisionEngine(
        policy=RecoveryPolicy(
            action_progress_timeout=timedelta(seconds=5),
            action_hard_ttl=timedelta(seconds=10),
            cleanup_retry_interval=timedelta(seconds=1),
        )
    )
    loss_assessment = FlowBlockerAssessment(
        inventory_reduce_sides=(Side.SELL,),
        loss_only_blocked_sides=(Side.SELL,),
        episode_fingerprint="delayed-terminal-handoff",
    )
    state = RecoveryState.initial("ARXUSDT", BASELINE, now=NOW)
    state = state.with_exhausted_attempt(
        ActionId.INVENTORY_RECOVER,
        Side.SELL,
        OrderRole.REDUCE_ONLY,
        now=NOW,
    )
    entered = engine.plan_round(
        snapshot=_snapshot(assessment=loss_assessment),
        state=state,
        now=NOW,
        round_id="delayed-terminal-handoff-enter",
    )
    active = _activate_temporary_loss(
        engine,
        entered,
        loss_assessment,
        now=NOW + timedelta(seconds=1),
        round_id="delayed-terminal-handoff-activate",
    ).next_state
    handoff = engine.plan_round(
        snapshot=_snapshot(
            now=NOW + timedelta(seconds=2),
            assessment=FlowBlockerAssessment(
                terminal_reason="trusted_lifecycle_terminal_handoff",
                terminal_lifecycle_handoff=True,
            ),
            managed_order_manifest=_managed_manifest(active, "7014"),
        ),
        state=active,
        now=NOW + timedelta(seconds=2),
        round_id="delayed-terminal-handoff-preempt",
    )
    obligation = handoff.next_state.cleanup_obligation
    old_action_deadline = handoff.next_state.progress_deadline_at
    assert obligation is not None
    assert old_action_deadline is not None
    assert handoff.next_state.cleanup_successor_action is ActionId.TERMINAL_HANDOFF

    proof_at = old_action_deadline + timedelta(seconds=1)
    finished = engine.plan_round(
        snapshot=_snapshot(
            now=proof_at,
            assessment=FlowBlockerAssessment(
                terminal_reason="trusted_lifecycle_terminal_handoff",
                terminal_lifecycle_handoff=True,
            ),
            cleanup_proof=_cleanup_proof(
                obligation,
                observed_at=proof_at,
                user_trades_watermark=101,
            ),
        ),
        state=handoff.next_state,
        now=proof_at,
        round_id="delayed-terminal-handoff-clean",
    )

    assert finished.action_id is ActionId.TERMINAL_HANDOFF
    assert finished.effect_stage is EffectStage.NONE
    assert finished.next_state.phase is RecoveryPhase.STABLE
    assert finished.next_state.active_action is ActionId.NOOP
    assert finished.next_state.desired_runner_state == "running"
    assert finished.next_state.action_lease is None
    assert finished.next_state.cleanup_obligation is None
    assert finished.next_state.pending_effect_stage is EffectStage.NONE
    assert finished.next_state.desired_profile == finished.next_state.baseline_profile


@pytest.mark.parametrize("preemptor", ["safety", "runner"])
def test_terminal_lifecycle_handoff_reissues_existing_cleanup_under_its_own_fence(
    preemptor: str,
) -> None:
    engine = FuturesRecoveryDecisionEngine(
        policy=RecoveryPolicy(cleanup_retry_interval=timedelta(seconds=5))
    )
    loss_assessment = FlowBlockerAssessment(
        inventory_reduce_sides=(Side.SELL,),
        loss_only_blocked_sides=(Side.SELL,),
        episode_fingerprint=f"terminal-after-{preemptor}",
    )
    state = RecoveryState.initial("ARXUSDT", BASELINE, now=NOW).with_exhausted_attempt(
        ActionId.INVENTORY_RECOVER,
        Side.SELL,
        OrderRole.REDUCE_ONLY,
        now=NOW,
    )
    entered = engine.plan_round(
        snapshot=_snapshot(assessment=loss_assessment),
        state=state,
        now=NOW,
        round_id=f"terminal-after-{preemptor}-enter",
    )
    active = _activate_temporary_loss(
        engine,
        entered,
        loss_assessment,
        now=NOW + timedelta(seconds=1),
        round_id=f"terminal-after-{preemptor}-activate",
    ).next_state
    if preemptor == "safety":
        preempt_assessment = FlowBlockerAssessment(
            safety_reasons=("volatility",),
            safety_evidence_fingerprint="terminal-cleanup-safety",
            safety_observation_seq=1,
            safety_observed_at=NOW + timedelta(seconds=2),
        )
    else:
        preempt_assessment = FlowBlockerAssessment(
            runner_faults=("runner_inactive",),
        )
    first_cleanup = engine.plan_round(
        snapshot=_snapshot(
            now=NOW + timedelta(seconds=2),
            assessment=preempt_assessment,
            managed_order_manifest=_managed_manifest(active, "7005"),
        ),
        state=active,
        now=NOW + timedelta(seconds=2),
        round_id=f"terminal-after-{preemptor}-cleanup",
    )
    obligation = first_cleanup.next_state.cleanup_obligation
    assert obligation is not None
    assert first_cleanup.effect_stage is EffectStage.MANAGED_GTX_CANCEL

    handoff = engine.plan_round(
        snapshot=_snapshot(
            now=NOW + timedelta(seconds=3),
            assessment=FlowBlockerAssessment(
                terminal_reason="trusted_lifecycle_terminal_handoff",
                terminal_lifecycle_handoff=True,
            ),
        ),
        state=first_cleanup.next_state,
        now=NOW + timedelta(seconds=3),
        round_id=f"terminal-after-{preemptor}-handoff",
    )

    assert handoff.action_id is ActionId.TERMINAL_HANDOFF
    assert handoff.next_state.phase is RecoveryPhase.CLEANING
    assert handoff.next_state.cleanup_obligation is not None
    assert handoff.next_state.cleanup_obligation.source_decision_id == (
        obligation.source_decision_id
    )
    assert handoff.next_state.cleanup_obligation.open_order_ids == (
        obligation.open_order_ids
    )
    assert handoff.effect_stage is EffectStage.MANAGED_GTX_CANCEL
    assert handoff.effect_epoch is not None
    assert handoff.effect_epoch > int(first_cleanup.effect_epoch or -1)
    assert handoff.next_state.pending_effect_stage is EffectStage.MANAGED_GTX_CANCEL
    assert handoff.next_state.desired_runner_state == "running"
    assert (
        handoff.next_state.desired_profile.fields[
            "best_quote_maker_volume_allow_loss_reduce_only"
        ]
        is False
    )


def test_settling_preemption_preserves_known_lease_order_for_cleanup() -> None:
    engine = FuturesRecoveryDecisionEngine()
    loss_assessment = FlowBlockerAssessment(
        inventory_reduce_sides=(Side.SELL,),
        loss_only_blocked_sides=(Side.SELL,),
        episode_fingerprint="settling-orphan-order",
    )
    state = RecoveryState.initial("ARXUSDT", BASELINE, now=NOW)
    state = state.with_exhausted_attempt(
        ActionId.INVENTORY_RECOVER,
        Side.SELL,
        OrderRole.REDUCE_ONLY,
        now=NOW,
    )
    settling = engine.plan_round(
        snapshot=_snapshot(assessment=loss_assessment),
        state=state,
        now=NOW,
        round_id="settling-orphan-enter",
    )
    source_decision_id = settling.next_state.decision_id

    safety = engine.plan_round(
        snapshot=_snapshot(
            now=NOW + timedelta(seconds=1),
            assessment=FlowBlockerAssessment(
                safety_reasons=("volatility",),
                safety_evidence_fingerprint="settling-safety-1",
                safety_observation_seq=1,
                safety_observed_at=NOW + timedelta(seconds=1),
            ),
            managed_order_manifest=_managed_manifest(settling.next_state, "7006"),
        ),
        state=settling.next_state,
        now=NOW + timedelta(seconds=1),
        round_id="settling-orphan-safety",
    )

    obligation = safety.next_state.cleanup_obligation
    assert safety.action_id is ActionId.SAFETY_CONVERGE
    assert safety.next_state.phase is RecoveryPhase.CLEANING
    assert (
        safety.desired_profile.fields["best_quote_maker_volume_allow_loss_reduce_only"]
        is False
    )
    assert obligation is not None
    assert obligation.source_decision_id == source_decision_id
    assert obligation.open_order_ids == ("7006",)


def test_old_cleanup_receipt_cannot_acknowledge_new_runner_restart_stage() -> None:
    engine = FuturesRecoveryDecisionEngine(
        policy=RecoveryPolicy(cleanup_retry_interval=timedelta(seconds=1))
    )
    loss_assessment = FlowBlockerAssessment(
        inventory_reduce_sides=(Side.SELL,),
        loss_only_blocked_sides=(Side.SELL,),
        episode_fingerprint="typed-effect-receipt",
    )
    state = RecoveryState.initial("ARXUSDT", BASELINE, now=NOW)
    state = state.with_exhausted_attempt(
        ActionId.INVENTORY_RECOVER,
        Side.SELL,
        OrderRole.REDUCE_ONLY,
        now=NOW,
    )
    entered = engine.plan_round(
        snapshot=_snapshot(assessment=loss_assessment),
        state=state,
        now=NOW,
        round_id="typed-loss-enter",
    )
    activated = _activate_temporary_loss(
        engine,
        entered,
        loss_assessment,
        now=NOW + timedelta(seconds=1),
        round_id="typed-loss-active",
    )
    runner_preempt = engine.plan_round(
        snapshot=_snapshot(
            now=NOW + timedelta(seconds=2),
            assessment=FlowBlockerAssessment(runner_faults=("runner_inactive",)),
            managed_order_manifest=_managed_manifest(activated.next_state, "7007"),
        ),
        state=activated.next_state,
        now=NOW + timedelta(seconds=2),
        round_id="typed-runner-preempt",
    )
    obligation = runner_preempt.next_state.cleanup_obligation
    cancel_epoch = runner_preempt.effect_epoch
    assert obligation is not None
    assert runner_preempt.effect_stage is EffectStage.MANAGED_GTX_CANCEL
    assert cancel_epoch is not None

    cleanup_complete = engine.plan_round(
        snapshot=_snapshot(
            now=NOW + timedelta(seconds=3),
            assessment=FlowBlockerAssessment(runner_faults=("runner_inactive",)),
            cleanup_proof=_cleanup_proof(
                obligation,
                observed_at=NOW + timedelta(seconds=3),
                user_trades_watermark=30,
            ),
        ),
        state=runner_preempt.next_state,
        now=NOW + timedelta(seconds=3),
        round_id="typed-cleanup-complete",
    )
    restart_epoch = cleanup_complete.effect_epoch
    assert cleanup_complete.effect_stage is EffectStage.RUNNER_RESTART
    assert restart_epoch is not None and restart_epoch != cancel_epoch

    stale_receipt = engine.plan_round(
        snapshot=_snapshot(
            now=NOW + timedelta(seconds=4),
            assessment=FlowBlockerAssessment(runner_faults=("runner_inactive",)),
            effect_receipt=EffectReceipt(
                decision_id=cleanup_complete.next_state.decision_id,
                stage=EffectStage.MANAGED_GTX_CANCEL,
                effect_epoch=cancel_epoch,
                observed_at=NOW + timedelta(seconds=4),
            ),
        ),
        state=cleanup_complete.next_state,
        now=NOW + timedelta(seconds=4),
        round_id="typed-stale-receipt",
    )
    assert stale_receipt.effect_stage is EffectStage.RUNNER_RESTART
    assert stale_receipt.effect_epoch == restart_epoch
    assert stale_receipt.next_state.pending_effect_stage is EffectStage.RUNNER_RESTART


def test_delayed_cleanup_cannot_activate_an_expired_successor_effect() -> None:
    policy = RecoveryPolicy(
        action_progress_timeout=timedelta(seconds=5),
        action_hard_ttl=timedelta(seconds=10),
        cleanup_retry_interval=timedelta(seconds=1),
    )
    engine = FuturesRecoveryDecisionEngine(policy=policy)
    loss_assessment = FlowBlockerAssessment(
        inventory_reduce_sides=(Side.SELL,),
        loss_only_blocked_sides=(Side.SELL,),
        episode_fingerprint="delayed-cleanup-successor",
    )
    state = RecoveryState.initial("ARXUSDT", BASELINE, now=NOW)
    state = state.with_exhausted_attempt(
        ActionId.INVENTORY_RECOVER,
        Side.SELL,
        OrderRole.REDUCE_ONLY,
        now=NOW,
    )
    entered = engine.plan_round(
        snapshot=_snapshot(assessment=loss_assessment),
        state=state,
        now=NOW,
        round_id="delayed-loss-enter",
    )
    activated = _activate_temporary_loss(
        engine,
        entered,
        loss_assessment,
        now=NOW + timedelta(seconds=1),
        round_id="delayed-loss-active",
    )
    runner_preempt = engine.plan_round(
        snapshot=_snapshot(
            now=NOW + timedelta(seconds=2),
            assessment=FlowBlockerAssessment(runner_faults=("runner_inactive",)),
            managed_order_manifest=_managed_manifest(activated.next_state, "7008"),
        ),
        state=activated.next_state,
        now=NOW + timedelta(seconds=2),
        round_id="delayed-runner-preempt",
    )
    obligation = runner_preempt.next_state.cleanup_obligation
    assert obligation is not None

    too_late = engine.plan_round(
        snapshot=_snapshot(
            now=NOW + timedelta(seconds=13),
            assessment=FlowBlockerAssessment(runner_faults=("runner_inactive",)),
            cleanup_proof=_cleanup_proof(
                obligation,
                observed_at=NOW + timedelta(seconds=13),
                user_trades_watermark=31,
            ),
        ),
        state=runner_preempt.next_state,
        now=NOW + timedelta(seconds=13),
        round_id="delayed-cleanup-proof",
    )

    assert too_late.next_state.phase is RecoveryPhase.RESTORING
    assert too_late.effect_stage is EffectStage.RUNNER_RESTART
    assert too_late.effect_epoch is not None
    assert too_late.next_state.pending_effect_stage is EffectStage.RUNNER_RESTART
    assert too_late.next_state.action_lease is None
    assert (
        too_late.desired_profile.fields[
            "best_quote_maker_volume_allow_loss_reduce_only"
        ]
        is False
    )


def test_delayed_cleanup_rechecks_live_safety_before_restoring_baseline() -> None:
    policy = RecoveryPolicy(
        action_progress_timeout=timedelta(seconds=2),
        action_hard_ttl=timedelta(seconds=10),
        safety_hard_ttl=timedelta(seconds=2),
    )
    engine = FuturesRecoveryDecisionEngine(policy=policy)
    loss_assessment = FlowBlockerAssessment(
        inventory_reduce_sides=(Side.SELL,),
        loss_only_blocked_sides=(Side.SELL,),
        episode_fingerprint="delayed-safety-cleanup",
    )
    state = RecoveryState.initial("ARXUSDT", BASELINE, now=NOW)
    state = state.with_exhausted_attempt(
        ActionId.INVENTORY_RECOVER,
        Side.SELL,
        OrderRole.REDUCE_ONLY,
        now=NOW,
    )
    entered = engine.plan_round(
        snapshot=_snapshot(assessment=loss_assessment),
        state=state,
        now=NOW,
        round_id="delayed-safety-loss-enter",
    )
    activated = _activate_temporary_loss(
        engine,
        entered,
        loss_assessment,
        now=NOW + timedelta(seconds=1),
        round_id="delayed-safety-loss-active",
    )
    safety = engine.plan_round(
        snapshot=_snapshot(
            now=NOW + timedelta(seconds=2),
            assessment=FlowBlockerAssessment(
                safety_reasons=("volatility",),
                safety_evidence_fingerprint="delayed-safety-1",
                safety_observation_seq=1,
                safety_observed_at=NOW + timedelta(seconds=2),
            ),
            managed_order_manifest=_managed_manifest(activated.next_state, "7009"),
        ),
        state=activated.next_state,
        now=NOW + timedelta(seconds=2),
        round_id="delayed-safety-preempt",
    )
    obligation = safety.next_state.cleanup_obligation
    assert obligation is not None

    still_unsafe = engine.plan_round(
        snapshot=_snapshot(
            now=NOW + timedelta(seconds=5),
            assessment=FlowBlockerAssessment(
                safety_reasons=("volatility",),
                safety_evidence_fingerprint="delayed-safety-2",
                safety_observation_seq=2,
                safety_observed_at=NOW + timedelta(seconds=5),
            ),
            cleanup_proof=_cleanup_proof(
                obligation,
                observed_at=NOW + timedelta(seconds=5),
                user_trades_watermark=32,
            ),
        ),
        state=safety.next_state,
        now=NOW + timedelta(seconds=5),
        round_id="delayed-safety-proof",
    )

    assert still_unsafe.next_state.phase is RecoveryPhase.ACTIVE
    assert still_unsafe.action_id is ActionId.SAFETY_CONVERGE
    assert still_unsafe.next_state.safety_lease.epoch == 2
    assert still_unsafe.next_state.cleanup_obligation is None
    assert still_unsafe.desired_profile.fields["recovery_entry_blocked_sides"] == (
        "BUY",
        "SELL",
    )
    assert (
        still_unsafe.desired_profile.fields[
            "best_quote_maker_volume_allow_loss_reduce_only"
        ]
        is False
    )


def test_same_episode_temporary_loss_budget_cannot_reopen_allow_loss() -> None:
    policy = RecoveryPolicy(max_temporary_loss_leases_per_episode=1)
    engine = FuturesRecoveryDecisionEngine(policy=policy)
    assessment = FlowBlockerAssessment(
        inventory_reduce_sides=(Side.SELL,),
        loss_only_blocked_sides=(Side.SELL,),
        episode_fingerprint="episode-budget-1",
    )
    state = RecoveryState.initial("ARXUSDT", BASELINE, now=NOW)
    state = state.with_exhausted_attempt(
        ActionId.INVENTORY_RECOVER,
        Side.SELL,
        OrderRole.REDUCE_ONLY,
        now=NOW,
    ).with_temporary_loss_lease_use("episode-budget-1", count=1)

    successor = engine.plan_round(
        snapshot=_snapshot(assessment=assessment),
        state=state,
        now=NOW,
        round_id="budget-blocked",
    )

    assert successor.action_id is ActionId.BASELINE_TUNE
    assert successor.order_role is OrderRole.REDUCE_ONLY
    assert successor.liveness_status == "recovering"
    assert (
        successor.desired_profile.fields[
            "best_quote_maker_volume_allow_loss_reduce_only"
        ]
        is False
    )

    new_episode = engine.plan_round(
        snapshot=_snapshot(
            now=NOW + timedelta(seconds=1),
            assessment=replace(
                assessment,
                episode_fingerprint="episode-budget-2",
            ),
        ),
        state=state,
        now=NOW + timedelta(seconds=1),
        round_id="budget-new-episode",
    )
    assert new_episode.action_id is ActionId.TEMPORARY_LOSS_RELIEF


def test_temporary_loss_budget_exhaustion_enters_bounded_baseline_tune() -> None:
    policy = RecoveryPolicy(
        action_progress_timeout=timedelta(seconds=30),
        action_hard_ttl=timedelta(seconds=60),
        max_temporary_loss_leases_per_episode=1,
    )
    engine = FuturesRecoveryDecisionEngine(policy=policy)
    assessment = FlowBlockerAssessment(
        inventory_reduce_sides=(Side.SELL,),
        loss_only_blocked_sides=(Side.SELL,),
        episode_fingerprint="loss-budget-baseline-successor",
    )
    state = RecoveryState.initial("ARXUSDT", BASELINE, now=NOW)
    state = state.with_exhausted_attempt(
        ActionId.INVENTORY_RECOVER,
        Side.SELL,
        OrderRole.REDUCE_ONLY,
        now=NOW,
    ).with_temporary_loss_lease_use(
        "loss-budget-baseline-successor",
        count=1,
    )

    successor = engine.plan_round(
        snapshot=_snapshot(assessment=assessment),
        state=state,
        now=NOW,
        round_id="loss-budget-baseline-successor",
    )

    assert successor.action_id is ActionId.BASELINE_TUNE
    assert successor.side is Side.SELL
    assert successor.order_role is OrderRole.REDUCE_ONLY
    assert successor.next_state.phase is RecoveryPhase.ACTIVE
    assert successor.effect_stage is EffectStage.RUNNER_RESTART
    assert successor.next_state.progress_deadline_at == NOW + timedelta(seconds=30)
    assert successor.next_state.hard_expires_at == NOW + timedelta(seconds=60)
    assert successor.desired_profile.execution_policy.time_in_force == "GTX"
    assert successor.desired_profile.execution_policy.post_only is True
    assert (
        successor.desired_profile.fields[
            "best_quote_maker_volume_allow_loss_reduce_only"
        ]
        is False
    )
    assert (
        successor.desired_profile.fields[
            "best_quote_maker_volume_net_loss_reduce_enabled"
        ]
        is False
    )
    assert successor.desired_profile.fields["hard_loss_forced_reduce_enabled"] is False


def test_cleared_inventory_blocker_ends_episode_and_reclaims_loss_budget() -> None:
    policy = RecoveryPolicy(max_temporary_loss_leases_per_episode=1)
    engine = FuturesRecoveryDecisionEngine(policy=policy)
    assessment = FlowBlockerAssessment(
        inventory_reduce_sides=(Side.SELL,),
        loss_only_blocked_sides=(Side.SELL,),
        episode_fingerprint="same-shape-episode",
    )
    state = RecoveryState.initial("ARXUSDT", BASELINE, now=NOW)
    state = state.with_exhausted_attempt(
        ActionId.INVENTORY_RECOVER,
        Side.SELL,
        OrderRole.REDUCE_ONLY,
        now=NOW,
    ).with_temporary_loss_lease_use("same-shape-episode", count=1)
    successor = engine.plan_round(
        snapshot=_snapshot(assessment=assessment),
        state=state,
        now=NOW,
        round_id="episode-old-blocked",
    )
    assert successor.action_id is ActionId.BASELINE_TUNE

    exiting = engine.plan_round(
        snapshot=_snapshot(
            now=NOW + timedelta(seconds=1),
            assessment=FlowBlockerAssessment(),
            effect_receipt=EffectReceipt(
                decision_id=successor.next_state.decision_id,
                stage=EffectStage.RUNNER_RESTART,
                effect_epoch=successor.effect_epoch,
                observed_at=NOW + timedelta(seconds=1),
            ),
        ),
        state=successor.next_state,
        now=NOW + timedelta(seconds=1),
        round_id="episode-cleared",
    )
    assert exiting.next_state.phase is RecoveryPhase.CLEANING
    cleaned = _complete_cleanup(
        engine,
        exiting,
        now=NOW + timedelta(seconds=1),
        round_id="episode-cleared-cleaned",
    )
    assert cleaned.next_state.phase is RecoveryPhase.RESTORING
    cleared = engine.plan_round(
        snapshot=_snapshot(
            now=NOW + timedelta(seconds=2),
            assessment=FlowBlockerAssessment(),
            applied_generation=cleaned.next_state.generation,
            applied_profile_digest=cleaned.next_state.desired_profile.digest,
        ),
        state=cleaned.next_state,
        now=NOW + timedelta(seconds=2),
        round_id="episode-cleared-restored",
    )
    assert cleared.liveness_status == "recovering"
    assert cleared.next_state.phase is RecoveryPhase.COOLDOWN
    assert cleared.next_state.exhausted_attempts == ()
    assert cleared.next_state.temporary_loss_lease_uses == {}
    stable = engine.plan_round(
        snapshot=_snapshot(
            now=cleared.next_state.cooldown_until,
            assessment=FlowBlockerAssessment(),
        ),
        state=cleared.next_state,
        now=cleared.next_state.cooldown_until,
        round_id="episode-cleared-cooldown",
    )
    assert stable.next_state.phase is RecoveryPhase.STABLE

    reappeared = engine.plan_round(
        snapshot=_snapshot(
            now=NOW + timedelta(seconds=2),
            assessment=assessment,
        ),
        state=stable.next_state.with_exhausted_attempt(
            ActionId.INVENTORY_RECOVER,
            Side.SELL,
            OrderRole.REDUCE_ONLY,
            now=NOW + timedelta(seconds=2),
        ),
        now=NOW + timedelta(seconds=2),
        round_id="episode-new-same-shape",
    )
    assert reappeared.action_id is ActionId.TEMPORARY_LOSS_RELIEF
    assert (
        reappeared.desired_profile.fields[
            "best_quote_maker_volume_allow_loss_reduce_only"
        ]
        is False
    )


def test_episode_is_reclaimed_when_blocker_clears_during_cooldown() -> None:
    state = RecoveryState.initial("ARXUSDT", BASELINE, now=NOW)
    state = state.with_exhausted_attempt(
        ActionId.INVENTORY_RECOVER,
        Side.SELL,
        OrderRole.REDUCE_ONLY,
        now=NOW,
    ).with_temporary_loss_lease_use("cooldown-episode", count=1)
    state = replace(
        state,
        phase=RecoveryPhase.COOLDOWN,
        active_action=ActionId.TEMPORARY_LOSS_RELIEF,
        decision_id="cooldown-episode-decision",
        side=Side.SELL,
        order_role=OrderRole.REDUCE_ONLY,
        cooldown_until=NOW + timedelta(seconds=10),
        active_episode_fingerprint="cooldown-episode",
    )

    cleared = FuturesRecoveryDecisionEngine().plan_round(
        snapshot=_snapshot(
            now=NOW + timedelta(seconds=1),
            assessment=FlowBlockerAssessment(),
        ),
        state=state,
        now=NOW + timedelta(seconds=1),
        round_id="cooldown-episode-cleared",
    )

    assert cleared.next_state.phase is RecoveryPhase.COOLDOWN
    assert cleared.next_state.exhausted_attempts == ()
    assert cleared.next_state.temporary_loss_lease_uses == {}
    assert cleared.next_state.active_episode_fingerprint is None


def test_nonterminal_loss_recovery_converges_to_running_stable_without_cleanup_event() -> (
    None
):
    policy = RecoveryPolicy(
        action_progress_timeout=timedelta(seconds=10),
        action_hard_ttl=timedelta(seconds=20),
        cooldown=timedelta(seconds=2),
    )
    engine = FuturesRecoveryDecisionEngine(policy=policy)
    assessment = FlowBlockerAssessment(
        inventory_reduce_sides=(Side.SELL,),
        loss_only_blocked_sides=(Side.SELL,),
        episode_fingerprint="liveness-converges-1",
    )
    state = RecoveryState.initial("ARXUSDT", BASELINE, now=NOW)
    state = state.with_exhausted_attempt(
        ActionId.INVENTORY_RECOVER,
        Side.SELL,
        OrderRole.REDUCE_ONLY,
        now=NOW,
    )
    entered = engine.plan_round(
        snapshot=_snapshot(assessment=assessment),
        state=state,
        now=NOW,
        round_id="liveness-enter",
    )
    activated = _activate_temporary_loss(
        engine,
        entered,
        assessment,
        now=NOW + timedelta(seconds=1),
        round_id="liveness-activate",
    )

    exited = engine.plan_round(
        snapshot=_snapshot(
            now=NOW + timedelta(seconds=2),
            assessment=FlowBlockerAssessment(),
            managed_order_manifest=_managed_manifest(activated.next_state, "7010"),
        ),
        state=activated.next_state,
        now=NOW + timedelta(seconds=2),
        round_id="liveness-blocker-cleared",
    )
    obligation = exited.next_state.cleanup_obligation
    assert obligation is not None
    assert exited.next_state.phase is RecoveryPhase.CLEANING
    assert (
        exited.desired_profile.fields["best_quote_maker_volume_allow_loss_reduce_only"]
        is False
    )

    cleaned = engine.plan_round(
        snapshot=_snapshot(
            now=NOW + timedelta(seconds=3),
            cleanup_proof=_cleanup_proof(
                obligation,
                observed_at=NOW + timedelta(seconds=3),
                user_trades_watermark=20,
            ),
        ),
        state=exited.next_state,
        now=NOW + timedelta(seconds=3),
        round_id="liveness-cleaned",
    )
    assert cleaned.next_state.phase is RecoveryPhase.RESTORING

    restored = engine.plan_round(
        snapshot=_snapshot(
            now=NOW + timedelta(seconds=4),
            applied_generation=cleaned.next_state.generation,
            applied_profile_digest=cleaned.next_state.desired_profile.digest,
        ),
        state=cleaned.next_state,
        now=NOW + timedelta(seconds=4),
        round_id="liveness-restored",
    )
    stable = engine.plan_round(
        snapshot=_snapshot(now=NOW + timedelta(seconds=7)),
        state=restored.next_state,
        now=NOW + timedelta(seconds=7),
        round_id="liveness-stable",
    )

    assert stable.next_state.phase is RecoveryPhase.STABLE
    assert stable.next_state.desired_runner_state == "running"
    assert stable.next_state.action_lease is None
    assert stable.next_state.cleanup_obligation is None
    assert (
        stable.next_state.desired_profile.fields[
            "best_quote_maker_volume_allow_loss_reduce_only"
        ]
        is False
    )


def test_nonterminal_pause_exits_without_cleanup_event_and_preserves_flow_clock() -> (
    None
):
    policy = RecoveryPolicy(cooldown=timedelta(seconds=5))
    engine = FuturesRecoveryDecisionEngine(policy=policy)
    flow_key = FlowRoleKey(
        ledger_class=LedgerClass.NORMAL_BQ,
        side=Side.SELL,
        order_role=OrderRole.REDUCE_ONLY,
    )
    state = RecoveryState.initial("ARXUSDT", BASELINE, now=NOW).with_flow_clock(
        flow_key,
        quote_missing_since=NOW - timedelta(minutes=10),
        submit_missing_since=NOW - timedelta(minutes=9),
        fill_missing_since=NOW - timedelta(minutes=8),
    )
    entered = engine.plan_round(
        snapshot=_snapshot(
            assessment=FlowBlockerAssessment(
                safety_reasons=("volatility",),
                safety_evidence_fingerprint="safety-observation-1",
                safety_observation_seq=1,
                safety_observed_at=NOW,
            )
        ),
        state=state,
        now=NOW,
        round_id="safety-enter",
    )
    assert entered.desired_profile.fields["recovery_safety_lease_epoch"] == 1
    assert (
        entered.desired_profile.fields["recovery_safety_lease_hard_expires_at"]
        == entered.next_state.safety_lease.hard_expires_at.isoformat()
    )

    exited = engine.plan_round(
        snapshot=_snapshot(
            now=NOW + timedelta(seconds=10),
            assessment=FlowBlockerAssessment(),
            effect_receipt=EffectReceipt(
                decision_id=entered.next_state.decision_id,
                stage=EffectStage.RUNNER_RESTART,
                effect_epoch=entered.effect_epoch,
                observed_at=NOW + timedelta(seconds=10),
            ),
        ),
        state=entered.next_state,
        now=NOW + timedelta(seconds=10),
        round_id="safety-clear",
    )
    assert exited.action_id is ActionId.SAFETY_CONVERGE
    assert exited.mode is ActionMode.EXIT
    assert exited.next_state.phase is RecoveryPhase.RESTORING

    restored = engine.plan_round(
        snapshot=_snapshot(
            now=NOW + timedelta(seconds=11),
            applied_generation=exited.next_state.generation,
            applied_profile_digest=exited.next_state.desired_profile.digest,
        ),
        state=exited.next_state,
        now=NOW + timedelta(seconds=11),
        round_id="safety-restored",
    )
    assert restored.next_state.phase is RecoveryPhase.COOLDOWN

    stable = engine.plan_round(
        snapshot=_snapshot(now=NOW + timedelta(seconds=17)),
        state=restored.next_state,
        now=NOW + timedelta(seconds=17),
        round_id="safety-stable",
    )
    assert stable.next_state.phase is RecoveryPhase.STABLE
    assert stable.next_state.desired_runner_state == "running"
    assert stable.next_state.flow_clocks[flow_key] == state.flow_clocks[flow_key]


@pytest.mark.parametrize("phase", (RecoveryPhase.RESTORING, RecoveryPhase.COOLDOWN))
def test_fresh_safety_evidence_reenters_during_restore_or_cooldown(
    phase: RecoveryPhase,
) -> None:
    engine = FuturesRecoveryDecisionEngine(
        policy=RecoveryPolicy(cooldown=timedelta(seconds=30))
    )
    first = engine.plan_round(
        snapshot=_snapshot(
            assessment=FlowBlockerAssessment(
                safety_reasons=("volatility",),
                safety_evidence_fingerprint="safety-first",
                safety_observation_seq=1,
                safety_observed_at=NOW,
            )
        ),
        state=RecoveryState.initial("ARXUSDT", BASELINE, now=NOW),
        now=NOW,
        round_id=f"safety-{phase.value}-enter",
    )
    restoring = engine.plan_round(
        snapshot=_snapshot(
            now=NOW + timedelta(seconds=1),
            assessment=FlowBlockerAssessment(),
            effect_receipt=EffectReceipt(
                decision_id=first.next_state.decision_id,
                stage=EffectStage.RUNNER_RESTART,
                effect_epoch=first.effect_epoch,
                observed_at=NOW + timedelta(seconds=1),
            ),
        ),
        state=first.next_state,
        now=NOW + timedelta(seconds=1),
        round_id=f"safety-{phase.value}-clear",
    )
    state = restoring.next_state
    if phase is RecoveryPhase.COOLDOWN:
        state = engine.plan_round(
            snapshot=_snapshot(
                now=NOW + timedelta(seconds=2),
                applied_generation=state.generation,
                applied_profile_digest=state.desired_profile.digest,
            ),
            state=state,
            now=NOW + timedelta(seconds=2),
            round_id="safety-cooldown-confirm",
        ).next_state
    assert state.phase is phase

    reentered = engine.plan_round(
        snapshot=_snapshot(
            now=NOW + timedelta(seconds=3),
            assessment=FlowBlockerAssessment(
                safety_reasons=("volatility",),
                safety_evidence_fingerprint="safety-new",
                safety_observation_seq=2,
                safety_observed_at=NOW + timedelta(seconds=3),
            ),
        ),
        state=state,
        now=NOW + timedelta(seconds=3),
        round_id=f"safety-{phase.value}-reenter",
    )

    assert reentered.action_id is ActionId.SAFETY_CONVERGE
    assert reentered.mode is ActionMode.ENTER
    assert reentered.next_state.phase is RecoveryPhase.ACTIVE
    assert reentered.next_state.safety_lease.epoch == 2
    assert reentered.desired_profile.fields["volatility_entry_pause_enabled"] is True


def test_safety_lease_cannot_be_extended_by_the_same_evidence() -> None:
    policy = RecoveryPolicy(safety_hard_ttl=timedelta(seconds=30))
    engine = FuturesRecoveryDecisionEngine(policy=policy)
    initial = RecoveryState.initial("ARXUSDT", BASELINE, now=NOW)
    assessment = FlowBlockerAssessment(
        safety_reasons=("volatility",),
        safety_evidence_fingerprint="same-observation",
        safety_observation_seq=0,
        safety_observed_at=NOW,
    )
    entered = engine.plan_round(
        snapshot=_snapshot(assessment=assessment),
        state=initial,
        now=NOW,
        round_id="safety-1",
    )
    original_expiry = entered.next_state.safety_lease.hard_expires_at

    held = engine.plan_round(
        snapshot=_snapshot(
            now=NOW + timedelta(seconds=31),
            assessment=assessment,
            effect_receipt=EffectReceipt(
                decision_id=entered.next_state.decision_id,
                stage=EffectStage.RUNNER_RESTART,
                effect_epoch=entered.effect_epoch,
                observed_at=NOW + timedelta(seconds=1),
            ),
        ),
        state=entered.next_state,
        now=NOW + timedelta(seconds=31),
        round_id="safety-2",
    )
    assert held.mode is ActionMode.HOLD
    assert held.liveness_status == "blocked"
    assert held.next_state.safety_lease.hard_expires_at == original_expiry

    cycled = engine.plan_round(
        snapshot=_snapshot(
            now=NOW + timedelta(seconds=31, milliseconds=500),
            assessment=replace(
                assessment,
                safety_evidence_fingerprint="different-but-stale-observation",
                safety_observation_seq=0,
                safety_observed_at=NOW,
            ),
        ),
        state=held.next_state,
        now=NOW + timedelta(seconds=31, milliseconds=500),
        round_id="safety-stale-cycle",
    )
    assert cycled.mode is ActionMode.HOLD
    assert cycled.next_state.safety_lease.hard_expires_at == original_expiry

    renewed = engine.plan_round(
        snapshot=_snapshot(
            now=NOW + timedelta(seconds=32),
            assessment=replace(
                assessment,
                safety_evidence_fingerprint="new-observation",
                safety_observation_seq=1,
                safety_observed_at=NOW + timedelta(seconds=32),
            ),
        ),
        state=cycled.next_state,
        now=NOW + timedelta(seconds=32),
        round_id="safety-3",
    )
    assert renewed.mode is ActionMode.ADVANCE
    assert renewed.next_state.safety_lease.epoch == 2
    assert renewed.next_state.safety_lease.hard_expires_at == NOW + timedelta(
        seconds=62
    )
    assert renewed.desired_profile.fields["recovery_safety_lease_epoch"] == 2
    assert renewed.desired_profile.digest != entered.desired_profile.digest
